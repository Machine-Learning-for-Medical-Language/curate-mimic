#!/usr/bin/env python3

import csv
import sys
import argparse
import requests
import gzip
from json import dumps
from time import sleep
from os.path import join,exists
from tqdm import tqdm
import pandas as pd

from threading import Thread
from queue import Queue

import mongo_handler

JSON = 'json'
XMI = 'xmi'
JSON_LITE = 'json-lite'
FHIR = 'fhir'
MONGO = 'mongo'
FILTERED = 'filtered'
MIMIC = 'mimic'
I2B2 = 'i2b2'

STOP_JOB = '-STOP-'
NUM_TRIES = 5

file_extensions = {JSON:JSON, XMI:XMI, JSON_LITE:JSON, FHIR:FHIR}
output_formats = [JSON, XMI, JSON_LITE, FHIR, MONGO]
input_formats = [MIMIC, I2B2]
text_fields = { MIMIC: 'TEXT', I2B2: 'OBSERVATION_BLOB'}
id_fields = { MIMIC: 'ROW_ID', I2B2: 'INSTANCE_NUM'}

# One worker type just reads notes and metadata from a queue, calls a REST server, and
# puts the output into the output queue
class InputWorker(Thread):
    def __init__(self, job_queue, output_queue, rest_url):
        super().__init__()
        self.in_queue = job_queue
        self.out_queue = output_queue
        self.rest_url = rest_url

    def run(self):
        while True:
            job = self.in_queue.get()
            if job == STOP_JOB:
                self.in_queue.task_done()
                break

            (text, params, metadata) = job
            tries = 0
            while tries < NUM_TRIES:
                tries += 1
                try:
                    r = requests.post(self.rest_url, data=text, params=params)
                    if r is not None and r.status_code == 200:
                        json = r.json()
                        output_json = {'nlp':json, 'metadata': metadata}
                        self.out_queue.put(output_json)
                        self.in_queue.task_done()
                        break
                    else:
                        if tries == NUM_TRIES:
                            sys.stderr.write('Could not process row with metadata %s\n' % (str(metadata)))
                except:
                    sys.stderr.write('Error on post call with metadata %s\n' % (str(metadata)))


# The other worker type reads the output from the NLP and stores it, either in a filesystem
# or a database.
class OutputWorker(Thread):
    def __init__(self, output_queue, args):
        super().__init__()
        self.queue = output_queue
        self.args = args
        if self.args.output_format == MONGO:
            self.mongo_db = mongo_handler.get_db(self.args.input_format)

    def run(self):
        while True:
            job = self.queue.get()
            if job == STOP_JOB:
                self.queue.task_done()
                break
            else:
                json = job

                if self.args.output_format == JSON or self.args.output_format == MONGO:
                    json['nlp'] = json['nlp']['_views']['_InitialView']

                output = json

                if self.args.output_format == FHIR:
                    # TODO call to Bin's library once it's pip installable
                    raise NotImplementedError('FHIR file output not implemented yet.')

                if self.args.output_format in [JSON, XMI, JSON_LITE, FHIR]:
                    of_name = join(self.args.output_dir, '%s.%s' % (output['metadata']['ROW_ID'], file_extensions[self.args.output_format]))
                    with open(of_name, 'wt') as of:
                        of.write(dumps(output))
                elif self.args.output_format == MONGO:
                    self.mongo_db.note_nlp.insert_one(json)

                self.queue.task_done()

def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_file', help='Path to NOTEEVENTS.csv[.gz] file')
    parser.add_argument('--input-format', choices=input_formats, default=MIMIC)
    parser.add_argument('--output-format', choices=output_formats, default=JSON)
    parser.add_argument('--rest-url', help='Path to cTAKES REST URL')
    parser.add_argument('--max-notes', type=int, help='Max number of notes to process (for testing)', default=-1)
    parser.add_argument('--output-dir', help='Output dir (for file-based output formats', default=None)
    parser.add_argument('--num-threads', type=int, default=1, help='Number of workers to run (does not need to equal the number of containers (default=1)')
    parser.add_argument('--resume', action="store_true", help='Whether to resume processing (i.e. check for existing files before processing')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    compression = None
    # Trying to transparently handle the gzipped or non-gzipped case
    if args.input_file.endswith('.csv.gz'):
        compression = 'gzip'
    elif args.input_file.endswith('.csv'):
        compression = None
    else:
        raise Exception('Input file must end with .csv[.gz]')

    text_field = text_fields[args.input_format]
    id_field = id_fields[args.input_format]
 
    params = {}
    if args.output_format == JSON:
        # this filters out syntax but keeps all the semantic types
        params['format'] = FILTERED
    elif args.output_format == XMI:
        params['format'] = XMI
    elif args.output_format == MONGO:
        params['format'] = FILTERED
        assert mongo_handler.check_mongo_connection(), "Could not connect to MongoDB"

    assert args.output_format == MONGO or not args.output_dir is None, 'If output format is a file-based method then output-dir must be defined.'

    # Set the queue max size to 100 -- 100 is plenty for the workers to work with, and no need to read all of mimic into memory while we're waiting for the queue to be processed
    job_queue = Queue(maxsize = 100)
    output_queue = Queue()

    # start up all the workers
    workers = []
    for ind in range(args.num_threads):
        worker = InputWorker(job_queue, output_queue, args.rest_url)
        worker.start()
        workers.append(worker)

    writer = OutputWorker(output_queue, args)
    writer.start()

    reader = pd.read_csv(args.input_file, chunksize=1000, compression=compression)
    num_rows = 0
    row_id = -1

    for chunk in tqdm(reader):
        for row in chunk.iterrows():
            row = row[1]
            #for row_ind, row in enumerate(tqdm(csvreader)):
            # check whether the user specified for an early exit (usually for testing purposes)
            if args.max_notes > 0 and num_rows >= args.max_notes:
                print('Exiting after %d notes due to input argument' % (args.max_notes))
                break

            while job_queue.full():
                # no need to put all this data into memory if we already have 100 notes queued up.
                sleep(1)

            text = row.pop(text_field)
            if type(text) != str or len(text) == 0:
                print("Error processing row %d, after instance %d, either text wasn't a string or was length 0" % (num_rows, row_id))
                continue

            if args.resume:
                row_id = row[id_field]
                if args.output_format in [JSON, XMI, JSON_LITE, FHIR]:
                    of_name = join(args.output_dir, '%s.%s' % (row_id, file_extensions[args.output_format]))
                    if exists(of_name):
                        continue
            
            job_queue.put( (text, params, dict(row)) )
            num_rows += 1

        if args.max_notes > 0 and num_rows >= args.max_notes:
            break

    # after we've pushed all the jobs to the workers add the stop job so they know when to exit.
    for worker_ind, worker in enumerate(workers):
        job_queue.put( STOP_JOB )

    for worker in workers:
        worker.join()

    # all workers are done, put a STOP job in the output queue too
    output_queue.put(STOP_JOB)
    job_queue.join()
    output_queue.join()

    # Wait for writer to process all the output jobs and quit:
    writer.join()

    print("Processing complete and all threads shut down...")




if __name__ == '__main__':
    main()
