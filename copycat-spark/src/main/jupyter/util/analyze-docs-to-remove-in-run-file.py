import json
from os import listdir
import pandas as pd
import multiprocessing as mp

THRESHOLD=0.82
DIR='/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/'

def analyze_jsonl_line(line):
    dedup_data = json.loads(line)
    docs_to_remove = []
            
    for sim in dedup_data['similarities']:
        if sim['similarities']['s3'] >=  THRESHOLD:
            docs_to_remove += [sim['secondId']]
    
    return {
        'topic': dedup_data['topic'],
        'duplicates': len(set(docs_to_remove)),
        'docs': dedup_data['docs'],
    }

def analyze_all_jsonl_lines(file_name):
    with open(file_name) as f:
        print('Process ' + file_name)
        return mp.Pool(50).map(analyze_jsonl_line, [i for i in f])

def analyze_all_runs(run_files):
    rows = []
    for r in run_files:
        rows += [i for i in analyze_all_jsonl_lines(r)]
    
    return pd.DataFrame(rows)

def analyze_runs_in_dir(dir_name, min_docs, max_docs, doc_number):
    all_runs = [dir_name + i for i in listdir(dir_name) if '.jsonl' in i]

    df = analyze_all_runs(all_runs)
    df['redundancy'] = df['duplicates']/df['docs']
    df = df[(df['docs'] >= min_docs) & (df['docs'] < max_docs)]
    df['docs'] = doc_number

    return df


for web_track in [18]:
    for docs in [10, 100, 1000]:
        directory = DIR + '/trec' + str(web_track) + '-web.adhoc-top' + str(docs) + '/'
        analyze_runs_in_dir(directory, docs-(docs/10), docs+(docs/10), docs).to_json('redundant-in-runs-track-' + str(web_track) + '-docs-'+ str(docs) + '.jsonl', lines=True, orient='records')

