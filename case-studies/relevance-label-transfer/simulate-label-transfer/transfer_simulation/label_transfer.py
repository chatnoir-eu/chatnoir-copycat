import pandas as pd
from trectools import TrecQrel


class BaseLabelTransfer:
    def keep_doc(self, doc_id):
        return doc_id in self.ids_to_transfer


class CW12UrlLabelTransfer(BaseLabelTransfer):
    def __init__(self, input_files):
        df = load_input_files_to_dataframe(input_files)
        df = df[(df['cw12Matches'] > 0)]
        self.ids_to_transfer = df_to_ids_to_transfer(df)
        self.name = 'cw12-url'


class CW12WaybackUrlLabelTransfer(BaseLabelTransfer):
    def __init__(self, input_files):
        df = load_input_files_to_dataframe(input_files)
        df = df[(df['matches'] > 0) | (df['non200Matches'] > 0) | (df['cw12Matches'] > 0)]
        self.ids_to_transfer = df_to_ids_to_transfer(df)
        self.name = 'wayback-cw12-url'


def df_to_ids_to_transfer(df):
    return {str(i) for i in df['document']}


def load_input_files_to_dataframe(input_files):
    dfs = []
    for input_file in input_files:
        dfs += [pd.read_json(input_file, lines=True)]
    return pd.concat(dfs)


def qrels_as_str(input_file, labels_to_keep=None):
    return parse_qrels(input_file, labels_to_keep).qrels_data[["query", "q0", "docid", "rel"]].to_csv(
        sep=" ", header=False, index=False)


def parse_qrels(input_file, labels_to_keep=None):
    ret = TrecQrel(input_file)

    if labels_to_keep is None:
        return ret
    else:
        ret.filename = None

        ret.qrels_data['tmp_delete_me'] = ret.qrels_data['docid'].apply(lambda i: labels_to_keep.keep_doc(i))
        ret.qrels_data = ret.qrels_data[ret.qrels_data['tmp_delete_me']]
        ret.qrels_data = ret.qrels_data.drop(['tmp_delete_me'], axis=1)

        return ret


if __name__ == '__main__':
    files = [
        '/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/web-2009.jsonl',
        '/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/web-2010.jsonl',
        '/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/web-2011.jsonl',
        '/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/web-2012.jsonl'
    ]
    label_transfers = [
        CW12UrlLabelTransfer(files),
        CW12WaybackUrlLabelTransfer(files)
    ]
    base_dir = 'data/'

    for trec_run_file in ['qrels-web-2009', 'qrels-web-2010', 'qrels-web-2011', 'qrels-web-2012']:
        for label_transfer in label_transfers:
            run_file = base_dir + trec_run_file
            print('Process ' + str(run_file))

            tmp = qrels_as_str(run_file, label_transfer)

            with open(run_file + '-' + label_transfer.name, 'w+') as f:
                f.write(tmp)
