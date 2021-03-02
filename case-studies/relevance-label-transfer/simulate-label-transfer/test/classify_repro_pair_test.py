import unittest

import pandas as pd
from transfer_simulation.repro_measures import classify_repro_pair, classify_repro_pairs


class ClassifyReproPairTest(unittest.TestCase):

    def test_classification1(self):
        repro_pair = self.df1().iloc[0]
        expected = {
            'effect-size-success-absolute-scores-failure': 1,
            'effect-size-success-absolute-scores-success': 0,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 0,
        }
        actual = classify_repro_pair(repro_pair)

        self.assertEquals(expected, actual)

    def test_classification2(self):
        repro_pair = {"firstTag": "0.40.20.0", "sourceFirstPos": 1, "sourceFirstScore": 0.3703536772,
                      "sourceSecondPost": 49, "sourceSecondTag": "0.80.80.4", "sourceSecondScore": 0.3490109373,
                      "effectRatio": -0.4962933215, "delta_relative_improvement": 0.0371788357
                      }
        expected = {
            'effect-size-success-absolute-scores-failure': 0,
            'effect-size-success-absolute-scores-success': 0,
            'effect-size-failure-absolute-scores-failure': 1,
            'effect-size-failure-absolute-scores-success': 0,
        }
        actual = classify_repro_pair(repro_pair)

        self.assertEquals(expected, actual)

    def test_classification3(self):
        repro_pair = {"firstTag": "0.40.20.0", "sourceFirstPos": 1, "sourceFirstScore": 0.3703536772,
                      "sourceSecondPost": 49, "sourceSecondTag": "0.80.80.4", "sourceSecondScore": 0.3490109373,
                      "effectRatio": 0.4962933215, "delta_relative_improvement": -0.0371788357
                      }
        expected = {
            'effect-size-success-absolute-scores-failure': 0,
            'effect-size-success-absolute-scores-success': 1,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 0,
        }
        actual = classify_repro_pair(repro_pair)

        self.assertEquals(expected, actual)

    def test_classification4(self):
        repro_pair = {"firstTag": "0.40.20.0", "sourceFirstPos": 1, "sourceFirstScore": 0.3703536772,
                      "sourceSecondPost": 49, "sourceSecondTag": "0.80.80.4", "sourceSecondScore": 0.3490109373,
                      "effectRatio": -0.4962933215, "delta_relative_improvement": -0.0371788357
                      }
        expected = {
            'effect-size-success-absolute-scores-failure': 0,
            'effect-size-success-absolute-scores-success': 0,
            'effect-size-failure-absolute-scores-failure': 0,
            'effect-size-failure-absolute-scores-success': 1,
        }
        actual = classify_repro_pair(repro_pair)

        self.assertEquals(expected, actual)

    def test_summation1(self):
        expected = {
            'effect-size-success-absolute-scores-failure': 3
        }
        actual = classify_repro_pairs(self.df1())

        self.assertEquals(expected, actual)

    def test_summation2(self):
        expected = {
            'effect-size-success-absolute-scores-failure': 3,
            'effect-size-failure-absolute-scores-failure': 2,
            'effect-size-failure-absolute-scores-success': 1,
        }
        actual = classify_repro_pairs(self.df2())

        self.assertEquals(expected, actual)

    @classmethod
    def df1(cls):
        return pd.DataFrame([
            {"firstTag": "0.40.20.0", "sourceFirstPos": 1, "sourceFirstScore": 0.3703536772, "sourceSecondPost": 49,
             "sourceSecondTag": "0.80.80.4", "sourceSecondScore": 0.3490109373, "effectRatio": 0.4962933215,
             "delta_relative_improvement": 0.0371788357},
            {"firstTag": "0.60.20.0", "sourceFirstPos": 3, "sourceFirstScore": 0.369782335, "sourceSecondPost": 72,
             "sourceSecondTag": "0.40.80.4", "sourceSecondScore": 0.3440355996, "effectRatio": 0.2672481477,
             "delta_relative_improvement": 0.0590357807},
            {"firstTag": "1.00.60.0", "sourceFirstPos": 4, "sourceFirstScore": 0.3697568702, "sourceSecondPost": 50,
             "sourceSecondTag": "0.40.40.2", "sourceSecondScore": 0.3490109373, "effectRatio": 0.6699748386,
             "delta_relative_improvement": 0.0279357363},
        ])

    @classmethod
    def df2(cls):
        return pd.DataFrame([
            {"firstTag": "0.40.20.0", "sourceFirstPos": 1, "sourceFirstScore": 0.3703536772, "sourceSecondPost": 49,
             "sourceSecondTag": "0.80.80.4", "sourceSecondScore": 0.3490109373, "effectRatio": 0.4962933215,
             "delta_relative_improvement": 0.0371788357},
            {"firstTag": "0.60.20.0", "sourceFirstPos": 3, "sourceFirstScore": 0.369782335, "sourceSecondPost": 72,
             "sourceSecondTag": "0.40.80.4", "sourceSecondScore": 0.3440355996, "effectRatio": 0.2672481477,
             "delta_relative_improvement": 0.0590357807},
            {"firstTag": "1.00.60.0", "sourceFirstPos": 4, "sourceFirstScore": 0.3697568702, "sourceSecondPost": 50,
             "sourceSecondTag": "0.40.40.2", "sourceSecondScore": 0.3490109373, "effectRatio": 0.6699748386,
             "delta_relative_improvement": 0.0279357363},
            {"firstTag": "1.00.60.0", "sourceFirstPos": 4, "sourceFirstScore": 0.3697568702, "sourceSecondPost": 50,
             "sourceSecondTag": "0.40.40.2", "sourceSecondScore": 0.3490109373, "effectRatio": -0.6699748386,
             "delta_relative_improvement": 0.0279357363},
            {"firstTag": "1.00.60.0", "sourceFirstPos": 4, "sourceFirstScore": 0.3697568702, "sourceSecondPost": 50,
             "sourceSecondTag": "0.40.40.2", "sourceSecondScore": 0.3490109373, "effectRatio": -0.6699748386,
             "delta_relative_improvement": 0.0279357363},
            {"firstTag": "1.00.60.0", "sourceFirstPos": 4, "sourceFirstScore": 0.3697568702, "sourceSecondPost": 50,
             "sourceSecondTag": "0.40.40.2", "sourceSecondScore": 0.3490109373, "effectRatio": -0.6699748386,
             "delta_relative_improvement": -0.0279357363},
        ])
