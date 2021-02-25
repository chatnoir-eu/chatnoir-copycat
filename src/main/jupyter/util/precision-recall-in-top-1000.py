import json
import pandas as pd
import multiprocessing as mp
from trectools import TrecQrel
from os import listdir

THRESHOLD=0.82
DIR='/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/'
qrels = None

TRACK_TO_QRELS={
    '18':  TrecQrel(DIR + 'qrel-files/qrels-web-2009.txt'),
    '19':  TrecQrel(DIR + 'qrel-files/qrels-web-2010.txt'),
    '20':  TrecQrel(DIR + 'qrel-files/qrels-web-2011.txt'),
    '21':  TrecQrel(DIR + 'qrel-files/qrels-web-2012.txt'),
    '22':  TrecQrel(DIR + 'qrel-files/qrels-web-2013.txt'),
    '23':  TrecQrel(DIR + 'qrel-files/qrels-web-2014.txt'),
}

def analyze_line(line):
    dedup_data = json.loads(line)
    topic = dedup_data['topic']
    judged_docs = qrels.get_document_names_for_topic(int(topic))
            
    for sim in dedup_data['similarities']:
        is_judged = sim['firstId'] in judged_docs or sim['secondId'] in judged_docs
        is_relevant = False
        is_irrelevant = False
                
        if is_judged:
            judgment_a = qrels.get_judgement(sim['firstId'], int(topic))
            judgment_b = qrels.get_judgement(sim['secondId'], int(topic))
            is_irrelevant = judgment_a == 0 or judgment_b == 0
            is_relevant = not is_irrelevant
                
        return {
            'topic': topic,
            'judged': is_judged,
            'relevant': is_relevant,
            'irrelevant': is_irrelevant,
            'near-duplicate': sim['similarities']['s3'] >=  THRESHOLD,
            'simhash(3+5-grams)': sim['similarities']['simhash(3+5-grams)'] > 0.94,
            'simhash(1-grams)': sim['similarities']['simhash(1-grams)'] > 0.94,
            'url': sim['similarities']['url'] > 0.5,
            'text-profile': sim['similarities']['text-profile'] > 0.5,
            'md5': sim['similarities']['md5'] > 0.5,
            'copy-cat': ((sim['similarities']['simhash(3+5-grams)'] > 0.94) or ((sim['similarities']['url'] > 0.5) and (sim['similarities']['simhash(1-grams)'] > 0.94))),
            'copy-cat-tp': ((sim['similarities']['simhash(3+5-grams)'] > 0.94) or (sim['similarities']['text-profile'] > 0.5) or ((sim['similarities']['url'] > 0.5) and (sim['similarities']['simhash(1-grams)'] > 0.94)))
        }

def eval_with_threshold(run_file_name, web_track):
    with open(run_file_name) as f:
        print('Process ' + run_file_name)
        return mp.Pool(10).map(analyze_line, [i for i in f])

def list_pretty_run_files(run_file_dir):
    return [i.replace('.gz', '').replace('input.', '') for i in listdir(run_file_dir)]


web_tracks = range(18,24) # the web tracks took place between TREC 18 and TREC 23
EXPERIMENT_DIR='/mnt/ceph/storage/data-in-progress/data-research/web-search/'
for web_track in web_tracks:
    rows = []
    run_file_dir=EXPERIMENT_DIR + 'web-search-trec/trec-system-runs/trec' + str(web_track) + '/web.adhoc/'
    qrels = TRACK_TO_QRELS[str(web_track)]

    for run_file in list_pretty_run_files(run_file_dir):
        run_file = '/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/trec' + str(web_track) + '-web.adhoc-top1000/' + run_file + '.jsonl'
        rows += [eval_with_threshold(run_file, web_track)]

    df = pd.DataFrame(rows)
    df.to_json(DIR + 'metadata/web-track-' + str(web_track) + '-precision-recall.jsonl', lines=True, orient='records')

