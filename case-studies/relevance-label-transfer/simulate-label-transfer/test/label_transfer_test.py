import unittest

from os import path
from transfer_simulation.label_transfer import CW12UrlLabelTransfer, CW12WaybackUrlLabelTransfer, qrels_as_str
from approvaltests.approvals import verify, verify_as_json


class LabelTransferTest(unittest.TestCase):

    def test_that_ceph_is_mounted(self):
        self.assertTrue(
            path.exists('/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/'),
            'Ceph must be mounted.'
        )

    @classmethod
    def test_cw12url_for_2009_sample(cls):
        input_files = ['test/resources/small-web-2009-sample.jsonl']
        actual = sorted(CW12UrlLabelTransfer(input_files).ids_to_transfer)

        verify_as_json(actual)

    @classmethod
    def test_cw12url_for_2012_sample(cls):
        input_files = ['test/resources/small-web-2012-sample.jsonl']
        actual = sorted(CW12UrlLabelTransfer(input_files).ids_to_transfer)

        verify_as_json(actual)

    @classmethod
    def test_cw12waybackurl_for_2009_sample(cls):
        input_files = ['test/resources/small-web-2009-sample.jsonl']
        actual = sorted(CW12WaybackUrlLabelTransfer(input_files).ids_to_transfer)

        verify_as_json(actual)

    @classmethod
    def test_cw12waybackurl_for_2009_and_2012_sample(cls):
        input_files = ['test/resources/small-web-2009-sample.jsonl', 'test/resources/small-web-2012-sample.jsonl']
        actual = sorted(CW12WaybackUrlLabelTransfer(input_files).ids_to_transfer)

        verify_as_json(actual)

    def test_name_of_cw12waybackurl(self):
        expected = 'wayback-cw12-url'
        input_files = ['test/resources/small-web-2009-sample.jsonl']
        actual = CW12WaybackUrlLabelTransfer(input_files).name

        self.assertEquals(expected, actual)

    def test_name_of_cw12url(self):
        expected = 'cw12-url'
        input_files = ['test/resources/small-web-2009-sample.jsonl']
        actual = CW12UrlLabelTransfer(input_files).name

        self.assertEquals(expected, actual)

    @classmethod
    def test_parsing_of_qrel(cls):
        actual = qrels_as_str('test/resources/sample-qrel-file.txt')

        verify(actual)

    @classmethod
    def test_parsing_of_qrels_with_filtering(cls):
        input_files = ['test/resources/artificial-sample-for-qrel-filtering.jsonl']
        labels_to_keep = CW12WaybackUrlLabelTransfer(input_files)
        actual = qrels_as_str('test/resources/sample-qrel-file.txt', labels_to_keep)

        verify(actual)

