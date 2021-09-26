import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose, STL


def generate_period_trend_ts(period=100, repeat=10):
    x = np.arange(period * repeat) * (2 * np.pi / period)
    noise = np.random.rand(period * repeat) / 3
    ts = np.sin(x) + noise
    ts = ts + np.arange(period * repeat) / (period * repeat)
    return ts


def n_sigma(data):
    from statsmodels.datasets import co2
    data = co2.load(True).data
    print(data.head())
    data_len = len(data)
    data = data.resample('M').mean().ffill()
    res = STL(data).fit()
    print(type(data))
    print(len(data), len(res.resid), len(res.trend), len(res.seasonal))
    res.plot()
    plt.show()


if __name__ == '__main__':
    data = generate_period_trend_ts()
    n_sigma(data)