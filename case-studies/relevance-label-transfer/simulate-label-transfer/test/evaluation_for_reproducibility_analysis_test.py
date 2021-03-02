import unittest

from transfer_simulation.evaluation_for_reproduciblity_analysis import extract_corpus, extract_topics


class EvaluationForReproducibilityAnalysisTest(unittest.TestCase):
    def test_cw09_corpus_is_extracted(self):
        run_file_name = 'my-test-rankings/topics-1-50-cw09/final-rankings/'
        expected = 'cw09'
        actual = extract_corpus(run_file_name)

        self.assertEquals(expected, actual)

    def test_topics_1_to_50_is_extracted(self):
        run_file_name = 'my-test-rankings/topics-1-50-cw09/final-rankings/'
        expected = 'topics-1-50'
        actual = extract_topics(run_file_name)

        self.assertEquals(expected, actual)

    def test_cw12_corpus_is_extracted(self):
        run_file_name = 'bla-cw12/final-rankings/'
        expected = 'cw12'
        actual = extract_corpus(run_file_name)

        self.assertEquals(expected, actual)

    def test_topics_212_to_50_is_extracted(self):
        run_file_name = 'my-test-rankings/topics-212-50-cw09/final-rankings/'
        expected = 'topics-212-50'
        actual = extract_topics(run_file_name)

        self.assertEquals(expected, actual)