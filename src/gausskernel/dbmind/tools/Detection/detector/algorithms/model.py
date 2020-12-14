from abc import abstractmethod


class AlgModel(object):
    def __init__(self):
        pass

    @abstractmethod
    def fit(self, timeseries):
        pass

    @abstractmethod
    def forecast(self, forecast_periods):
        pass

    def save(self, model_path):
        pass

    def load(self, model_path):
        pass
