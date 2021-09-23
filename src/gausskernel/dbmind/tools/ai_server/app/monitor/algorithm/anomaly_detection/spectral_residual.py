# SR.py
import numpy as np
import scipy as sc
import pandas as pd
import matplotlib.pyplot as plt
from gs_aiops.detector.algorithm.anomal_detect_algorithm.utils import *
from scipy.fftpack import fft, ifft
from gs_aiops.tools import generate_anomal_data
from gs_aiops.detector.algorithm.anomal_detect_algorithm.utils import load_data


class SR:
    '''
    This module realises a spectral residual method for anomaly detection.
    The input data suppose list,np.ndarray and pd.Series
    '''

    def __init__(self, X=np.array([]), slice_window=3, map_window=3, tresh=1):
        self.slice_window = slice_window
        self.X = getData(X)
        self.map_window = map_window
        self.thresh = tresh
        # raise NotImplementedError

    def run(self):
        Smap = self.getSalienceMap(self.X)
        result = np.array([1 if i > self.thresh else 0 for i in Smap])
        return result, Smap

    def setdata(self, data):
        self.X = getData(data)

    def setslicewindow(self, thresh):
        self.slice_window = thresh

    def plot(self):
        raise NotImplementedError

    def getSR(self, X):
        '''
        傅里叶变化、残差谱、反傅里叶变化
        '''
        X = getData(X)

        # spectral_residual_transform
        yy = fft(X)
        A = yy.real
        P = yy.imag
        V = np.sqrt(A ** 2 + P ** 2)
        eps_index = np.where(V <= EPS)[0]
        V[eps_index] = EPS
        L = np.log(V)
        L[eps_index] = 0
        residual = np.exp(L - average_filter(L, self.map_window))
        yy.imag = residual * P / V
        yy.real = residual * A / V
        yy.imag[eps_index] = 0
        yy.real[eps_index] = 0
        result = ifft(yy)
        S = np.sqrt(result.real ** 2 + result.imag ** 2)
        # guass filter
        return S

    def getSalienceMap(self, X):
        Map = self.getSR(self.extendseries(X))[:len(X)]
        ave_mag = average_filter(Map, n=self.slice_window)
        ave_mag[np.where(ave_mag <= EPS)] = EPS

        return abs(Map - ave_mag) / ave_mag

    def estimate(self, X):
        '''
            get k estimated points which is equal to x(n+1)
            x(n+1)=x(n-m+1)+m*g
            g=sum(g(x(n),x(n-i)))/m
        '''
        n = len(X)
        gradients = [(X[-1] - v) / (n - 1 - i) for i, v in enumerate(X[:-1])]
        # g=np.sum(gradients)/m
        return X[1] + np.sum(gradients)

    def extendseries(self, X, k=5):
        '''
        use k to extend oringe serie;
        '''
        print(X[-k - 2:-1])
        X = np.append(X, self.estimate(X[-k - 2:-1]).repeat(k))
        return X



if __name__ == '__main__':
    # data = generate_anomal_data.generate_period_trend_ts()
    data = load_data('../../../../anomal_data/art_daily_flatmiddle.csv')
    sr = SR(data, tresh=1.5)
    res, ma = sr.run()
    print(res)
    print(len(res))
    plt.subplot(211)
    plt.plot(data)
    for index, value in enumerate(data):
        if res[index] == 1:
            plt.scatter(index, value, c='r')
    plt.subplot(212)
    plt.plot(ma)
    plt.show()
    # data = np.array([1, 2, 3])
    # print(SR().extendseries(data))