import numpy as np


IsAnomaly = "isAnomaly"
AnomalyId = "id"
AnomalyScore = "score"
Value = "value"
Timestamp = "timestamp"
Mag = "mag"
ExpectedValue = "expectedValue"
UpperBoundary = "upperBoundary"
LowerBoundary = "lowerBoundary"

MAX_RATIO = 0.25
EPS = 1e-8
THRESHOLD = 0.3
MAG_WINDOW = 3
SCORE_WINDOW = 40


def average_filter(values, n=3):
    if n >= len(values):
        n = len(values)

    res = np.cumsum(values, dtype=float)
    res[n:] = res[n:] - res[:-n]
    res[n:] = res[n:] / n

    for i in range(1, n):
        res[i] /= (i + 1)

    return res