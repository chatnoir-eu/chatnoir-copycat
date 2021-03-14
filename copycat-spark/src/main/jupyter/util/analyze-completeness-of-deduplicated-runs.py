from statistics import mean
from datetime import datetime
import multiprocessing as mp
from os import listdir
import json


def analyze_jsonl_line(line):
    try:
        j = json.loads(line)
        return {
            'topic': j['topic'],
            'docs': j['docs'],
        }
    except:
        return None


def analyze_all_jsonl_lines(file_name):
    with open(file_name) as f:
        return mp.Pool(10).map(analyze_jsonl_line, [i for i in f])


def load_deduplicated_run_file(track, run):
    ret = {
        'run': run,
        'track': track
    }

    for size in [10, 100, 1000]:
        try:
            file_name = '/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/trec' + str(track) + '-web.adhoc-top' + str(size) + '/' + run + '.jsonl'
            topics = []
            docs_per_topic = []

            for i in analyze_all_jsonl_lines(file_name):
                if i:
                    topics += [i['topic']]
                    docs_per_topic += [i['docs']]

            ret['topics (' + str(size) + ')'] = len(topics)
            ret['mean_docs_per_topic (' + str(size) + ')'] = mean(docs_per_topic)
        except:
            ret['topics (' + str(size) + ')'] = -1
            ret['mean_docs_per_topic (' + str(size) + ')'] = -1

    return json.dumps(ret)


def list_pretty_run_files(run_file_dir):
    return [i.replace('.gz', '').replace('input.', '') for i in listdir(run_file_dir)]


EXPERIMENT_DIR='/mnt/ceph/storage/data-in-progress/data-research/web-search/'
web_tracks = range(18,23) # the web tracks took place between TREC 18 and TREC 23

for web_track in web_tracks:
    run_file_dir=EXPERIMENT_DIR + 'web-search-trec/trec-system-runs/trec' + str(web_track) + '/web.adhoc/'

    for run_file in list_pretty_run_files(run_file_dir):
        print(load_deduplicated_run_file(web_track, run_file))

