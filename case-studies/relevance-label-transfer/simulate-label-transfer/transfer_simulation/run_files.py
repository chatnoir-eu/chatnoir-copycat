import glob
from trectools import TrecRun, TrecQrel
import pandas as pd
import numpy as np


def list_run_files_of_qrel_file(qrel_file_name):
    run_file_dir = run_files_of_qrel_file(qrel_file_name)

    return glob.glob(run_file_dir + '*.gz')


def run_files_of_qrel_file(qrel_file_name):
    if qrel_file_name.startswith('qrels-web-2009') or '.web.1-50' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec18/web.adhoc/'
    if qrel_file_name.startswith('qrels-web-2010') or '.web.51-100' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec19/web.adhoc/'
    if qrel_file_name.startswith('qrels-web-2011') or '.web.101-150' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec20/web.adhoc/'
    if qrel_file_name.startswith('qrels-web-2012') or '.web.151-200' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec21/web.adhoc/'
    if '.web.201-250' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec22/web.adhoc/'
    if '.web.251-300' in qrel_file_name:
        return '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec23/web.adhoc/'

    raise ValueError('Could not handle: ' + str(qrel_file_name))


if __name__ == '__main__':
    label_transfers = ['', 'cw12-url', 'wayback-cw12-url']

    for track in ['qrels-web-2009', 'qrels-web-2010', 'qrels-web-2011', 'qrels-web-2012']:
        track_display_name = track.replace('qrels-', '')
        qrels = {
            'orig': TrecQrel('data/' + track),
            'cw12-url': TrecQrel('data/' + track + '-cw12-url'),
            'wayback-cw12-url': TrecQrel('data/' + track + '-wayback-cw12-url'),
        }

        track_eval_data = []

        for run_file in list_run_files_of_qrel_file(track):
            run = TrecRun(run_file)

            run_file_eval = {
                'run': run.get_runid(),
                'track': track_display_name
            }

            print('Process: ' + track_display_name + '(' + run.get_runid() + ')')

            for qrel in qrels.keys():
                run_file_eval[qrel + '-mean-qrel-coverage-10'] = run.get_mean_coverage(qrels[qrel], 10)
                run_file_eval[qrel + '-mean-qrel-coverage-100'] = run.get_mean_coverage(qrels[qrel], 100)
                run_file_eval[qrel + '-mean-qrel-coverage-1000'] = run.get_mean_coverage(qrels[qrel], 1000)

            track_eval_data += [run_file_eval]

        df_name = '/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/evaluations/' \
                  + track_display_name + '-fairness.jsonl'
        pd.DataFrame(track_eval_data).to_json(df_name, lines=True, orient='records')


def identify_judgments_to_remove_for_leave_one_out(trec_run_files):
    df = pd.concat([i.run_data for i in trec_run_files])
    df['key'] = np.str(df['query']) + df['docid']
    df = df.sort_values('rank', ascending=True).groupby('key', as_index=False).first()
    df = df[['query', 'rank', 'system', 'docid']]

    return df
