import json
import argparse
from run_files import list_run_files_of_qrel_file
from trectools import TrecRun, TrecQrel, TrecEval
from evaluation_for_reproduciblity_analysis import QREL_DIR


def parse_args():
    parser = argparse.ArgumentParser(description="TBD :)")
    parser.add_argument("--outputFile", type=str, required=True)

    return parser.parse_args()


def report_run(qrels, corpus, topics, run_file_name):
    run = TrecRun(run_file_name)
    trec_eval = TrecEval(run, qrels)

    ret = {
        'corpus': corpus,
        'topics': topics,
        'tag': run.run_data['system'][0],
        "bpref": trec_eval.getBpref(),
        "pseudoNDCG@10": trec_eval.getNDCG(depth=10, removeUnjudged=True),
        "pseudoNDCG": trec_eval.getNDCG(removeUnjudged=True),
    }

    return json.dumps(ret)


def qrel_files(topics):
    transferred_prefix = QREL_DIR + '/qrels-with-only-transferred-original-labels/qrels.inofficial.duplicate-free.'

    ret = [
        (QREL_DIR + 'qrels.inofficial.duplicate-free.web.' + topics + '.txt', 'cw09'),
        (transferred_prefix + 'transferred-to-cc15.' + topics + '.txt', 'cc15')
    ]

    if topics in ['1-50', '51-100', '101-150', '151-200']:
        ret += [
            (transferred_prefix + 'transferred-to-cw12.' + topics + '.txt', 'cw12'),
            (transferred_prefix + 'transferred-to-cw12wb12.' + topics + '.txt', 'cw12wb12')
        ]

    return ret


if __name__ == '__main__':
    args = parse_args()
    with open(args.outputFile, 'w+') as f:
        for topics in ['1-50', '51-100', '101-150', '151-200', '201-250', '251-300']:
            qrel_files_for_topic = qrel_files(topics)
            for qrel_file, corpus in qrel_files_for_topic:
                trec_qrel = TrecQrel(qrel_file)
                for run_file in list_run_files_of_qrel_file(qrel_files_for_topic[0][0]):
                    print(corpus + ': evaluate ' + run_file)
                    f.write(report_run(trec_qrel, corpus, topics, run_file) + '\n')
