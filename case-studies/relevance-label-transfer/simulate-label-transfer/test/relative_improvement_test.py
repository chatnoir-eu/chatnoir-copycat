import unittest

import pandas as pd
from transfer_simulation.repro_measures import avg_score, relative_improvement, delta_relative_improvement


class DeltaRelativeImprovementTest(unittest.TestCase):
    def test_avg_score(self):
        expected = 11/3
        self.assertEquals(3.6666666666666665, expected)
        method = 'baseline'
        actual = avg_score(self.df(), method, True)

        self.assertEquals(expected, actual)

    def test_avg_score_2(self):
        expected = (3+3.1+25)/3
        self.assertEquals(10.366666666666667, expected)
        method = 'improved'
        actual = avg_score(self.df(), method, False)

        self.assertEquals(expected, actual)

    def test_relative_improvement(self):
        expected = (((2+0.1+11)/3) - ((1+0+10)/3))/((1+0+10)/3)
        self.assertEquals(0.19090909090909083, expected)
        advanced = 'improved'
        baseline = 'baseline'
        actual = relative_improvement(self.df(), baseline=baseline, advanced=advanced, on_old_collection=True)

        self.assertEquals(expected, actual)

    def test_relative_improvement_2(self):
        expected = (((3+3.1+25)/3)-((2+3+20)/3))/((2+3+20)/3)
        self.assertEquals(0.24399999999999997, expected)
        advanced = 'improved'
        baseline = 'baseline'
        actual = relative_improvement(self.df(), baseline=baseline, advanced=advanced, on_old_collection=False)

        self.assertEquals(expected, actual)

    def test_delta_relative_improvement(self):
        expected = 0.19090909090909083-0.2439999999999999
        self.assertEquals(-0.05309090909090908, expected)
        advanced = 'improved'
        baseline = 'baseline'
        actual = delta_relative_improvement(self.df(), baseline=baseline, advanced=advanced)

        self.assertEquals(-0.05309090909090913, actual)

    def test_delta_relative_improvement_2(self):
        advanced = 'baseline'
        baseline = 'improved'
        actual = delta_relative_improvement(self.df(), baseline=baseline, advanced=advanced)

        self.assertEquals(0.035836135588228096, actual)

    def test_delta_relative_improvement_3(self):
        advanced = 'improved'
        baseline = 'baseline'
        actual = delta_relative_improvement(self.df_not_reproduced(), baseline=baseline, advanced=advanced)

        self.assertEquals(0.37937062937062915, actual)

    def test_delta_relative_improvement_4(self):
        advanced = 'baseline'
        baseline = 'improved'
        actual = delta_relative_improvement(self.df_not_reproduced(), baseline=baseline, advanced=advanced)

        self.assertEquals(-0.3925328316631089, actual)

    @classmethod
    def df(cls):
        return pd.DataFrame([
            {'collection': 'source', 'topic': 1, 'measure': 1, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 1, 'measure': 2, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 1, 'measure': 2, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 1, 'measure': 3, 'tag': 'improved'},

            {'collection': 'source', 'topic': 2, 'measure': 0, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 2, 'measure': 3, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 2, 'measure': 0.1, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 2, 'measure': 3.1, 'tag': 'improved'},

            {'collection': 'source', 'topic': 3, 'measure': 10, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 3, 'measure': 20, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 3, 'measure': 11, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 3, 'measure': 25, 'tag': 'improved'},
        ])

    @classmethod
    def df_not_reproduced(cls):
        return pd.DataFrame([
            {'collection': 'source', 'topic': 1, 'measure': 1, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 1, 'measure': 3, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 1, 'measure': 2, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 1, 'measure': 3, 'tag': 'improved'},

            {'collection': 'source', 'topic': 2, 'measure': 0, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 2, 'measure': 3, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 2, 'measure': 0.1, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 2, 'measure': 3.1, 'tag': 'improved'},

            {'collection': 'source', 'topic': 3, 'measure': 10, 'tag': 'baseline'},
            {'collection': 'cw12', 'topic': 3, 'measure': 20, 'tag': 'baseline'},
            {'collection': 'source', 'topic': 3, 'measure': 11, 'tag': 'improved'},
            {'collection': 'cw12', 'topic': 3, 'measure': 15, 'tag': 'improved'},
        ])