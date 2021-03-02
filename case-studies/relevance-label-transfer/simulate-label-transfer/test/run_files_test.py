import unittest

from transfer_simulation.run_files import run_files_of_qrel_file, list_run_files_of_qrel_file, \
    identify_judgments_to_remove_for_leave_one_out
from approvaltests import verify, verify_as_json
from trectools import TrecRun


class LabelTransferTest(unittest.TestCase):

    def test_web_2009_dir(self):
        expected = '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec18/web.adhoc/'
        actual = run_files_of_qrel_file('qrels-web-2009')

        self.assertEquals(expected, actual)

    def test_web_2009_run_file_count(self):
        expected = 71
        actual = list_run_files_of_qrel_file('qrels-web-2009')

        self.assertEquals(expected, len(actual))

    def test_web_2010_dir(self):
        expected = '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec19/web.adhoc/'
        actual = run_files_of_qrel_file('qrels-web-2010-cw12-url')

        self.assertEquals(expected, actual)

    def test_web_2010_run_file_count(self):
        expected = 56
        actual = list_run_files_of_qrel_file('qrels-web-2010')

        self.assertEquals(expected, len(actual))

    def test_web_2011_dir(self):
        expected = '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec20/web.adhoc/'
        actual = run_files_of_qrel_file('qrels-web-2011-cw12-url')

        self.assertEquals(expected, actual)

    def test_web_2011_run_file_count(self):
        expected = 37
        actual = list_run_files_of_qrel_file('qrels-web-2011')

        self.assertEquals(expected, len(actual))

    def test_web_2012_dir(self):
        expected = '/mnt/ceph/storage/data-in-progress/trec-system-runs/trec21/web.adhoc/'
        actual = run_files_of_qrel_file('qrels-web-2012-foo-bar')

        self.assertEquals(expected, actual)

    @classmethod
    def test_web_2012_run_file_count(cls):
        actual = list_run_files_of_qrel_file('qrels-web-2012')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_1_50_run_file_count(cls):
        actual = list_run_files_of_qrel_file('resources/artificial-qrels/qrels.inofficial.duplicate-free.web.1-50.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_51_100_run_file_count(cls):
        actual = list_run_files_of_qrel_file('artificial-qrels/qrels.inofficial.duplicate-free.web.51-100.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_101_150_run_file_count(cls):
        actual = list_run_files_of_qrel_file('artificial-qrels/qrels.inofficial.duplicate-free.web.101-150.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_151_200_run_file_count(cls):
        actual = list_run_files_of_qrel_file('artificial-qrels/qrels.inofficial.duplicate-free.web.151-200.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_201_250_run_file_count(cls):
        actual = list_run_files_of_qrel_file('artificial-qrels/qrels.inofficial.duplicate-free.web.201-250.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_web_qrel_251_300_run_file_count(cls):
        actual = list_run_files_of_qrel_file('artificial-qrels/qrels.inofficial.duplicate-free.web.251-300.txt')

        verify_as_json(sorted(actual))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_single_run(cls):
        run = TrecRun('test/resources/sample-run-file-01')
        actual = identify_judgments_to_remove_for_leave_one_out([run])

        verify(actual.to_csv(header=False))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_multiple_runs(cls):
        run_01 = TrecRun('test/resources/sample-run-file-01')
        run_02 = TrecRun('test/resources/sample-run-file-02')
        actual = identify_judgments_to_remove_for_leave_one_out([run_01, run_02])

        verify(actual.to_csv(header=False))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_multiple_runs_reverse(cls):
        run_01 = TrecRun('test/resources/sample-run-file-01')
        run_02 = TrecRun('test/resources/sample-run-file-02')
        actual = identify_judgments_to_remove_for_leave_one_out([run_02, run_01])

        verify(actual.to_csv(header=False))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_multiple_runs_01(cls):
        run_01 = TrecRun('test/resources/sample-run-file-01')
        run_02 = TrecRun('test/resources/sample-run-file-02')
        run_03 = TrecRun('test/resources/sample-run-file-03')
        actual = identify_judgments_to_remove_for_leave_one_out([run_02, run_03, run_01])

        verify(actual.to_csv(header=False))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_multiple_runs_02(cls):
        run_01 = TrecRun('test/resources/sample-run-file-01')
        run_02 = TrecRun('test/resources/sample-run-file-02')
        run_03 = TrecRun('test/resources/sample-run-file-03')
        actual = identify_judgments_to_remove_for_leave_one_out([run_03, run_02, run_01])

        verify(actual.to_csv(header=False))

    @classmethod
    def test_documents_to_remove_for_leave_one_out_with_single_topic_and_multiple_runs_03(cls):
        run_01 = TrecRun('test/resources/sample-run-file-01')
        run_02 = TrecRun('test/resources/sample-run-file-02')
        run_03 = TrecRun('test/resources/sample-run-file-03')
        actual = identify_judgments_to_remove_for_leave_one_out([run_01, run_02, run_03,])

        verify(actual.to_csv(header=False))
