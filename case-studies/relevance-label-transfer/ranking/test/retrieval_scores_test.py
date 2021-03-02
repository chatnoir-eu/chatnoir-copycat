import unittest

from main import query_handler
from asyncio import get_event_loop


class RetrievalScoresTest(unittest.TestCase):

    def testScoreOfSampleCW12DocumentForTopicForearmPainOnCW12(self):
        expected = {'qid': -1, 'doc': 'clueweb12-0303wb-20-27036', 'rank': 0, 'score': 20.415102}
        actual = self.query(
            index='webis_warc_clueweb12_011',
            query='forearm pain',
            trec_id='clueweb12-0303wb-20-27036'
        )

        self.assertEquals(expected, actual)

    def testScoreOfSampleCW12DocumentForTopicForearmPainOnAliasCW12AndWB12(self):
        # Using the alias works fine: The score has decreased slightly by 0.002016
        expected = {'qid': -1, 'doc': 'clueweb12-0303wb-20-27036', 'rank': 0, 'score': 20.413086}
        actual = self.query(
            index='clueweb12-and-wayback12',
            query='forearm pain',
            trec_id='clueweb12-0303wb-20-27036'
        )

        self.assertEquals(expected, actual)

    def testScoreOfSampleWB12DocumentForTopicForearmPainOnWB12(self):
        expected = {'qid': -1, 'doc': '<urn:uuid:ade83896-125e-485c-86ea-a51e4ef0bf4f>', 'rank': 0, 'score': 13.013705}
        actual = self.query(
            index='clueweb09-in-wayback12',
            query='forearm pain',
            trec_id='<urn:uuid:ade83896-125e-485c-86ea-a51e4ef0bf4f>'
        )

        self.assertEquals(expected, actual)

    def testScoreOfSampleWB12DocumentForTopicForearmPainOnAliasCW12AndWB12(self):
        # Using the alias works fine: The score has increased by 3.12618
        expected = {'qid': -1, 'doc': '<urn:uuid:ade83896-125e-485c-86ea-a51e4ef0bf4f>', 'rank': 0, 'score': 16.139885}
        actual = self.query(
            index='clueweb12-and-wayback12',
            query='forearm pain',
            trec_id='<urn:uuid:ade83896-125e-485c-86ea-a51e4ef0bf4f>'
        )

        self.assertEquals(expected, actual)

    @classmethod
    def query(cls, index: str, query: str, trec_id: str):
        return get_event_loop().run_until_complete(query_handler(
            index=[index],
            topic_num= -1,
            query=query,
            fields=["body_lang.en^1", "title_lang.en^0", "meta_desc_lang.en^0"],
            trec_id=trec_id
        ))
