import sys, os
from os.path import join, exists

import csv
import json
import requests
from time import time
import logging

from PyRuSH import RuSH
from nltk.tokenize import wordpunct_tokenize as tokenize
from nltk.tokenize.util import align_tokens

#from cnlpt.thyme_eval import fix_simple_tokenize

def fix_simple_tokenize(tokens):
    new_tokens = []
    ind = 0
    while ind < len(tokens):
        if tokens[ind] == "'" and ind+1 < len(tokens) and tokens[ind+1] == 's':
            new_tokens.append("'s")
            ind += 2
        else:
            new_tokens.append(tokens[ind])
            ind += 1

    return new_tokens

def main(args):
    if len(args) < 2:
        sys.stderr.write('Required argument(s): <path to NOTEEVENTS.csv> <output dir>\n')
        sys.exit(-1)

    url = 'http://nlp-gpu:8000/temporal/process'
    rush = RuSH('conf/rush_rules.tsv')
    doc_limit = -10

    start_time = time()

    with open(args[0], 'rt') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for doc_num,row in enumerate(csvreader):
            row_id = int(row['ROW_ID'])
            try:
                text = row['TEXT'].strip()
                temporal_info = {}
                if len(text) == 0:
                    continue
                ofname = join(args[1], 'mimic_%d.json' % (row_id,))
                if exists(ofname):
                    continue
                
                sentences = rush.segToSentenceSpans(text)
                if len(sentences) == 0:
                    sys.stderr.write('Skipping row %d with no sentences found\n' % (row_id,))
                    continue

                sent_tokens = []
                for sentence_ind,sentence in enumerate(sentences):
                    sent_txt = text[sentence.begin:sentence.end]
                    tokens = tokenize(sent_txt)
                    # This causes problems later when we go to align, because it's too stupid to handle edge cases like where there is a typo
                    # like "EEG' s"
                    #tokens = fix_simple_tokenize(tokens)
                    if text[sentence.end-1] == '\n':
                        tokens.append('<cr>')
                    if len(tokens) > 0:
                        sent_tokens.append(tokens)
                
                if len(sent_tokens) == 0:
                    sys.stderr.write('Skipping row %d with no tokenized sentences found\n' % (row_id,))
                    continue

                r = requests.post(url, json={'sent_tokens': sent_tokens, 'metadata': 'MIMIC_ROWID=%s' % (str(row_id))})
                if r.status_code != 200:
                    raise Exception('Problem processing row id %d' % (row_id))

                json_out = r.json()

                timexes = {}
                events = {} 
                relations = []
                for sentence_ind, sentence in enumerate(sentences):
                    sent_txt = text[sentence.begin:sentence.end]
                    sent_events = json_out['events'][sentence_ind]
                    sent_timexes = json_out['timexes'][sentence_ind]
                    sent_rels = json_out['relations'][sentence_ind]
                    #try:
                    token_spans = align_tokens(sent_tokens[sentence_ind], sent_txt)
                    #except:

                    timex_ids = []
                    event_ids = []

                    for sent_timex in sent_timexes:
                        timex_start_offset = token_spans[sent_timex['begin']][0] + sentence.begin
                        timex_end_offset = token_spans[sent_timex['end']][1] + sentence.begin
                        timex_text = text[timex_start_offset:timex_end_offset]
                            
                        timex_id = 'Timex_Row-%d_Sent-%d_Ind-%d' % (row_id,sentence_ind, len(timex_ids))
                        timex_ids.append(timex_id)

                        timexes[timex_id] = {'row_id':row_id, 'sent_index':sentence_ind, 'begin': timex_start_offset, 'end': timex_end_offset, 'text':timex_text, 'timeClass':sent_timex['timeClass']}

                    for sent_event in sent_events:
                        event_start_offset = token_spans[sent_event['begin']][0] + sentence.begin
                        event_end_offset = token_spans[sent_event['end']][1] + sentence.begin
                        event_text = text[event_start_offset:event_end_offset]
                        event_id = 'Event_Row-%d_Sent-%d_Ind-%d' % (row_id,sentence_ind, len(event_ids))
                        event_ids.append(event_id)

                        events[event_id] = {'row_id':row_id, 'sent_index':sentence_ind, 'begin': event_start_offset, 'end': event_end_offset, 'text':event_text, 'dtr':sent_event['dtr']}

                    for rel in sent_rels:
                        if rel['arg1'] is None or rel['arg2'] is None:
                            #logging.warning('Skipping relation in %d that could not be aligned to event/timex arguments.' % (row_id) )
                            continue

                        arg1_type, arg1_ind = rel['arg1'].split('-')
                        arg2_type, arg2_ind = rel['arg2'].split('-')
                        if arg1_type == 'EVENT':
                            arg1 = event_ids[int(arg1_ind)]
                        elif arg1_type == 'TIMEX':
                            arg1 = timex_ids[int(arg1_ind)]
                        if arg1 == -1:
                            continue

                        if arg2_type == 'EVENT':
                            arg2 = event_ids[int(arg2_ind)]
                        elif arg2_type == 'TIMEX':
                            arg2 = timex_ids[int(arg2_ind)]
                        if arg2 == -1:
                            continue

                        relations.append({'row_id':row_id, 'sent_index':sentence_ind, 'arg1':arg1, 'arg2':arg2, 'category':rel['category']}) 
                temporal_info['timexes'] = timexes
                temporal_info['events'] = events
                temporal_info['relations'] = relations

                with open(ofname, 'wt') as of:
                    #of.write(json.dumps(r.json()))
                    of.write(json.dumps(temporal_info))

                if doc_limit > 0 and doc_num >= doc_limit:
                    sys.stderr.write('Exiting early due to doc_limit parameter\n')
                    break
                
                if doc_num % 10000 == 0:
                    print("Processing %d documents took %d s" % (doc_num, time() - start_time))
            except Exception as e:
                sys.stderr.write('Caught exception in row id %d\n' % (row_id) )
                continue

    end_time = time()
    runtime = end_time - start_time
    print('Processing %d documents took %d s' % (doc_limit, runtime))
            
if __name__ == '__main__':
    main(sys.argv[1:])

