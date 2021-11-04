#!/usr/bin/env python3

import csv
import sys
import argparse
import requests


JSON = 'json'
XMI = 'xmi'
JSON_LITE = 'json-lite'
FHIR = 'fhir'
file_extensions = {JSON:JSON, XMI:XMI, JSON_LITE:JSON, FHIR:FHIR}

def main(args):
    if args.input_file.endswith('.csv.gz'):
        f = gzip.open(args.input_file, 'rb')
    elif args.input_file.endswith('.csv'):
        f = open(args.input_file, 'rt')
    else:
        raise Exception('Input file must end with .csv[.gz]')

    params = {}
    if args.output_format == 'json':
        params['format'] = 'full'
    elif args.output_format == 'xmi':
        params['format'] = 'xmi'
    

    with f:
        csvreader = csv.DictReader(f)
        for row in csvreader:
            text = row.pop('TEXT')
            params['metadata'] = row
            r = requests.post(args.rest-url, data=text, params=params)

            if args.output_format == 'json':
                output = r.json()['_views']['_InitialView']
            else:
                output = r.json()
            
            if args.output_format == 'fhir':
                # TODO call to Bin's library once it's pip installable
                raise NotImplementedError('FHIR file output not implemented yet.')
            
            if args.output_format in ['json', 'xmi', 'json-lite', 'fhir']:
                with open(join(args.output_dir, '%s.%s' % (row['ROW_ID'], file_extensions[args.output_format]))) as of:
                    of.write(output)
            


output_formats = [JSON, XMI, JSON_LITE, FHIR]
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_arguments('input-file', help='Path to NOTEEVENTS.csv[.gz] file')
parser.add_arguments('--output-format', choices=output_formats)
parser.add_arguments('--rest-url', help='Path to cTAKES REST URL')
parser.add_arguments('--max-notes', type=int, help='Max number of notes to process (for testing)', default=-1)
parser.add_arguments('--output-args')

if __name__ == '__main__':
    args = parser.parse_args
    main(args)
