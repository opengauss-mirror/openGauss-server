import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from gs_aiops.detector.algorithm.anomal_detect_algorithm.utils import *


class SpectralResidual:
    def __init__(self, series, threshold, mag_window, score_window, batch_size):
        self.__series__ = series
        self.__values__ = self.__series__['value'].tolist()
        self.__threshold__ = threshold
        self.__mag_window = mag_window
        self.__score_window = score_window
        self.__anomaly_frame = None
        self.__batch_size = batch_size
        if self.__batch_size <= 0:
            self.__batch_size = len(series)

        self.__batch_size = max(12, self.__batch_size)
        self.__batch_size = min(len(series), self.__batch_size)

    def detect(self):
        if self.__anomaly_frame is None:
            self.__anomaly_frame = self.__detect()

        return self.__anomaly_frame

    def __detect(self):
        # 将原数据拆成batch，在每个batch大小的数据上进行异常检测
        anomaly_frames = []
        for i in range(0, len(self.__series__), self.__batch_size):
            start = i
            end = i + self.__batch_size
            end = min(end, len(self.__series__))
            if end - start >= 12:
                anomaly_frames.append(self.__detect_core(self.__series__[start:end]))
            else:
                ext_start = max(0, end - self.__batch_size)
                ext_frame = self.__detect_core(self.__series__[ext_start:end])
                anomaly_frames.append(ext_frame[start-ext_start:])

        return pd.concat(anomaly_frames, axis=0, ignore_index=True)

    def __detect_core(self, series):
        # 普残差核心检测单元函数
        values = series['value'].values
        # sr为了达到更好的预测效果，需要将目标监测点放置在窗口中心，因此需要对每个检测的序列后面增加估计点
        extended_series = SpectralResidual.extend_series(values)
        # 基于傅里叶算法得到
        mags = self.spectral_residual_transform(extended_series)
        anomaly_scores = self.generate_spectral_score(mags)
        anomaly_frame = pd.DataFrame({Timestamp: series['timestamp'].values,
                                      Value: values,
                                      Mag: mags[:len(values)],
                                      AnomalyScore: anomaly_scores[:len(values)]})
        anomaly_frame[IsAnomaly] = np.where(anomaly_frame[AnomalyScore] > self.__threshold__, True, False)

        return anomaly_frame

    def generate_spectral_score(self, mags):
        ave_mag = average_filter(mags, n=self.__score_window)
        safeDivisors = np.clip(ave_mag, EPS, ave_mag.max())

        raw_scores = np.abs(mags - ave_mag) / safeDivisors
        scores = np.clip(raw_scores / 10.0, 0, 1.0)

        return scores

    def spectral_residual_transform(self, values):
        trans = np.fft.fft(values)
        A = np.abs(trans)
        P = np.angle(trans)
        L = np.log(A)
        AL = average_filter(L, self.__mag_window)
        R = np.exp(L - AL)
        trans.imag = P
        trans.real = R
        # trans.real = [0 for i in range(len(A))]
        S_ = np.exp(trans)
        res = np.fft.ifft(S_)
        res = np.sqrt(res.real ** 2 + res.imag ** 2)
        return res

        # yy = np.fft.fft(values)
        # A = yy.real
        # P = yy.imag
        # V = np.sqrt(A ** 2 + P ** 2)
        # eps_index = np.where(V <= EPS)[0]
        # V[eps_index] = EPS
        # L = np.log(V)
        # L[eps_index] = 0
        # residual = np.exp(L - average_filter(L, self.__mag_window))
        # yy.imag = residual * P / V
        # yy.real = residual * A / V
        # yy.imag[eps_index] = 0
        # yy.real[eps_index] = 0
        # result = np.fft.ifft(yy)
        # S = np.sqrt(result.real ** 2 + result.imag ** 2)
        # return S

        # trans = np.fft.fft(values)
        # mag = np.sqrt(trans.real ** 2 + trans.imag ** 2)
        # eps_index = np.where(mag <= EPS)[0]
        # mag[eps_index] = EPS
        #
        # mag_log = np.log(mag)
        # mag_log[eps_index] = 0
        #
        # spectral = np.exp(mag_log - average_filter(mag_log, n=self.__mag_window))
        #
        # trans.real = trans.real * spectral / mag
        # trans.imag = trans.imag * spectral / mag
        # trans.real[eps_index] = 0
        # trans.imag[eps_index] = 0
        #
        # wave_r = np.fft.ifft(trans)
        # mag = np.sqrt(wave_r.real ** 2 + wave_r.imag ** 2)
        # return mag

    @staticmethod
    def predict_next(values):
        if len(values) <= 1:
            raise ValueError(f'data should contain at least 2 numbers')

        v_last = values[-1]
        n = len(values)

        slopes = [(v_last - v) / (n - 1 - i) for i, v in enumerate(values[:-1])]

        return values[1] + sum(slopes)

    @staticmethod
    def extend_series(values, extend_num=5, look_ahead=5):
        if look_ahead < 1:
            raise ValueError('look_ahead must be at least 1')

        extension = [SpectralResidual.predict_next(values[-look_ahead - 2:-1])] * extend_num
        return np.concatenate((values, extension), axis=0)


if __name__ == '__main__':
    series = pd.read_csv('./dataset/sample.csv')
    a_detector = SpectralResidual(series[:1000], THRESHOLD, MAG_WINDOW, SCORE_WINDOW, 1000)
    res = a_detector.detect()
    print(res)
    # res['mag'].plot()
    res['value'].plot()
    plt.show()
