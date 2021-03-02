import argparse
import glob
import json
from trectools import TrecRun, TrecQrel, TrecEval

QREL_DIR = '../src/main/resources/artificial-qrels/'

RUN_FILE_DIR_TO_QRELS = {
    '/topics-1-50-cw09': QREL_DIR + 'qrels.inofficial.duplicate-free.web.1-50.txt',
    '/topics-1-50-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.txt',
    '/topics-1-50-cw12wb12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12wb12.1-50.txt',

    '/topics-51-100-cw09': QREL_DIR + 'qrels.inofficial.duplicate-free.web.51-100.txt',
    '/topics-51-100-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.txt',
    '/topics-51-100-cw12wb12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12wb12.51-100.txt',

    '/topics-101-150-cw09': QREL_DIR + 'qrels.inofficial.duplicate-free.web.101-150.txt',
    '/topics-101-150-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.txt',
    '/topics-101-150-cw12wb12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12wb12.101-150.txt',

    '/topics-151-200-cw09': QREL_DIR + 'qrels.inofficial.duplicate-free.web.151-200.txt',
    '/topics-151-200-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.txt',
    '/topics-151-200-cw12wb12': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cw12wb12.151-200.txt',

    '/topics-201-250-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.web.201-250.txt',
    '/topics-251-300-cw12': QREL_DIR + 'qrels.inofficial.duplicate-free.web.251-300.txt',

    '/topics-1-50-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.txt',
    '/topics-51-100-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.txt',
    '/topics-101-150-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.txt',
    '/topics-151-200-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.txt',
    '/topics-201-250-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.txt',
    '/topics-251-300-cc15': QREL_DIR + 'qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.txt',
}


def parse_args():
    parser = argparse.ArgumentParser(description="TBD :)")
    parser.add_argument("--inputDir", type=str, required=True)
    parser.add_argument("--outputFile", type=str, required=True)

    return parser.parse_args()


def extract_corpus(run_file_name):
    return (run_file_name.split('/final-rankings'))[0].split('-')[-1]


def extract_topics(run_file_name):
    return 'topics-' + (run_file_name.split('-' + extract_corpus(run_file_name)))[0].split('topics-')[-1]


def report_run(qrels, run_file_name, remove_docs_with_zero_score=False):
    run = TrecRun(run_file_name)
    system = run.run_data['system'][0]
    if remove_docs_with_zero_score:
        run.run_data = run.run_data[run.run_data['score'] > 0]

    trec_eval = TrecEval(run, qrels)

    ret = {
        'corpus': extract_corpus(run_file_name),
        'topics': extract_topics(run_file_name),
        'tag': system,
        "bpref": trec_eval.getBpref(),
        "pseudoNDCG@10": trec_eval.getNDCG(depth=10),
        "pseudoNDCG": trec_eval.getNDCG()
    }

    return json.dumps(ret)


def report_run_per_query(qrels, run_file_name, remove_docs_with_zero_score=False):
    run = TrecRun(run_file_name)
    system = run.run_data['system'][0]
    if remove_docs_with_zero_score:
        run.run_data = run.run_data[run.run_data['score'] > 0]

    trec_eval = TrecEval(run, qrels)

    bpref = trec_eval.getBpref(per_query=True)
    ndcg_10 = trec_eval.getNDCG(depth=10, per_query='query')
    ndcg = trec_eval.getNDCG(per_query='query')

    ret = bpref.join(ndcg_10, on='query')
    ret = ret.join(ndcg, on='query')

    for query, r in ret.iterrows():
        yield json.dumps({
            'corpus': extract_corpus(run_file_name),
            'topic': query,
            'tag': system,
            "bpref": r['Bpref@1000'],
            "pseudoNDCG@10":  r['NDCG@10'],
            "pseudoNDCG":  r['NDCG@1000']
        })


if __name__ == '__main__':
    args = parse_args()
    with open(args.outputFile + '-per-query-zero-scores-removed.jsonl', 'w+') as f:
        for run_file_dir in RUN_FILE_DIR_TO_QRELS:
            trec_qrel = TrecQrel(RUN_FILE_DIR_TO_QRELS[run_file_dir])
            for run_file in glob.glob(args.inputDir + run_file_dir + '/final-rankings/*.txt', recursive=True):
                print('Evaluate ' + run_file)
                for report_line in report_run_per_query(trec_qrel, run_file, remove_docs_with_zero_score=True):
                    f.write(report_line + '\n')

    with open(args.outputFile + '-zero-scores-removed.jsonl', 'w+') as f:
        for run_file_dir in RUN_FILE_DIR_TO_QRELS:
            trec_qrel = TrecQrel(RUN_FILE_DIR_TO_QRELS[run_file_dir])
            for run_file in glob.glob(args.inputDir + run_file_dir + '/final-rankings/*.txt', recursive=True):
                print('Evaluate ' + run_file)
                f.write(report_run(trec_qrel, run_file, remove_docs_with_zero_score=True) + '\n')

    with open(args.outputFile + '-per-query.jsonl', 'w+') as f:
        for run_file_dir in RUN_FILE_DIR_TO_QRELS:
            trec_qrel = TrecQrel(RUN_FILE_DIR_TO_QRELS[run_file_dir])
            for run_file in glob.glob(args.inputDir + run_file_dir + '/final-rankings/*.txt', recursive=True):
                print('Evaluate ' + run_file)
                for report_line in report_run_per_query(trec_qrel, run_file):
                    f.write(report_line + '\n')

    with open(args.outputFile + '.jsonl', 'w+') as f:
        for run_file_dir in RUN_FILE_DIR_TO_QRELS:
            trec_qrel = TrecQrel(RUN_FILE_DIR_TO_QRELS[run_file_dir])
            for run_file in glob.glob(args.inputDir + run_file_dir + '/final-rankings/*.txt', recursive=True):
                print('Evaluate ' + run_file)
                f.write(report_run(trec_qrel, run_file) + '\n')

