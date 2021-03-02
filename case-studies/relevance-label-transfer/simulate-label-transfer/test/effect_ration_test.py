import unittest

import pandas as pd
from transfer_simulation.repro_measures import effect_ratio


class LabelTransferTest(unittest.TestCase):

    def test_example_with_positive_effect_ratio(self):
        expected = (((3-2) + (3.1-3) + (25 - 20))/3) / (((2-1) + (0.1-0) + (11-10))/3)
        self.assertEquals(2.904761904761904, expected)
        advanced = 'improved'
        baseline = 'baseline'

        actual = effect_ratio(
            df=self.df(),
            advanced=advanced,
            baseline=baseline
        )

        self.assertEquals(
            expected,
            actual
        )

    def test_example_with_negative_effect_ratio_2(self):
        expected = (((2-3) + (3-3.1) + (20-25))/3)/(((1-2) + (0-0.1) + (10-11))/3)
        self.assertEquals(2.904761904761904, expected)
        advanced = 'baseline'
        baseline = 'improved'

        actual = effect_ratio(
            df=self.df(),
            advanced=advanced,
            baseline=baseline
        )

        self.assertEquals(
            expected,
            actual
        )

    def test_example_with_negative_effect_ratio(self):
        expected = (((3-3) + (3.1-3) + (15-20))/3)/(((2-1) + (0.1-0) + (11-10))/3)
        self.assertEquals(-2.3333333333333335, expected)
        advanced = 'improved'
        baseline = 'baseline'

        actual = effect_ratio(
            df=self.df_not_reproduced(),
            advanced=advanced,
            baseline=baseline
        )

        self.assertEquals(
            expected,
            actual
        )

    def test_example_with_negative_effect_ratio_on_not_reproduced_df_2(self):
        expected = (((3-3) + (3 - 3.1) + (20-15)) / 3) / (((1-2) + (0-0.1) + (10-11)) / 3)
        self.assertEquals(-2.3333333333333335, expected)
        advanced = 'baseline'
        baseline = 'improved'

        actual = effect_ratio(
            df=self.df_not_reproduced(),
            advanced=advanced,
            baseline=baseline
        )

        self.assertEquals(
            expected,
            actual
        )

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
