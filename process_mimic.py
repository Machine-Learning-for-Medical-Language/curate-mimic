#!/usr/bin/env python3

import csv
import sys
import argparse
import requests
import gzip
from json import dumps
from time import sleep
from os.path import join

from threading import Thread
from queue import Queue

JSON = 'json'
XMI = 'xmi'
JSON_LITE = 'json-lite'
FHIR = 'fhir'
MONGO = 'mongo'

STOP_JOB = '-STOP-'

file_extensions = {JSON:JSON, XMI:XMI, JSON_LITE:JSON, FHIR:FHIR}
output_formats = [JSON, XMI, JSON_LITE, FHIR, MONGO]

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
            r = requests.post(self.rest_url, data=text, params=params)
            json = r.json()
            output_json = {'nlp':json, 'metadata': metadata}
            self.out_queue.put(output_json)
            self.in_queue.task_done()

# The other worker type reads the output from the NLP and stores it, either in a filesystem
# or a database.
class OutputWorker(Thread):
    def __init__(self, output_queue, args):
        super().__init__()
        self.queue = output_queue
        self.args = args

    def run(self):
        while True:
            job = self.queue.get()
            if job == STOP_JOB:
                self.queue.task_done()
                break
            else:
                json = job

                if self.args.output_format == 'json':
                    json['nlp'] = json['nlp']['_views']['_InitialView']

                output = json

                if self.args.output_format == 'fhir':
                    # TODO call to Bin's library once it's pip installable
                    raise NotImplementedError('FHIR file output not implemented yet.')

                if self.args.output_format in ['json', 'xmi', 'json-lite', 'fhir']:
                    of_name = join(self.args.output_dir, '%s.%s' % (output['metadata']['ROW_ID'], file_extensions[self.args.output_format]))
                    with open(of_name, 'wt') as of:
                        of.write(dumps(output))
                self.queue.task_done()

def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_file', help='Path to NOTEEVENTS.csv[.gz] file')
    parser.add_argument('--output-format', choices=output_formats, default='json')
    parser.add_argument('--rest-url', help='Path to cTAKES REST URL')
    parser.add_argument('--max-notes', type=int, help='Max number of notes to process (for testing)', default=-1)
    parser.add_argument('--output-dir', help='Output dir (for file-based output formats', default=None)
    parser.add_argument('--num-threads', type=int, default=1, help='Number of workers to run (does not need to equal the number of containers (default=1)')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    # Trying to transparently handle the gzipped or non-gzipped case
    if args.input_file.endswith('.csv.gz'):
        f = gzip.open(args.input_file, 'rt')
    elif args.input_file.endswith('.csv'):
        f = open(args.input_file, 'rt')
    else:
        raise Exception('Input file must end with .csv[.gz]')

    params = {}
    if args.output_format == 'json':
        # this filters out syntax but keeps all the semantic types
        params['format'] = 'filtered'
    elif args.output_format == 'xmi':
        params['format'] = 'xmi'

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

    with f:
        csvreader = csv.DictReader(f)
        for row_ind, row in enumerate(csvreader):
            # check whether the user specified for an early exit (usually for testing purposes)
            if args.max_notes > 0 and row_ind >= args.max_notes:
                print('Exiting after %d notes due to input argument' % (args.max_notes))
                break

            while job_queue.full():
                # no need to put all this data into memory if we already have 100 notes queued up.
                sleep(1)


            text = row.pop('TEXT')
            job_queue.put( (text, params, row) )

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
