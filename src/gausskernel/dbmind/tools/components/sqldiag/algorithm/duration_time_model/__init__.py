import abc


class AbstractModel:
    def __init__(self, params):
        self.params = params

    @abc.abstractmethod
    def fit(self, data):
        pass

    @abc.abstractmethod
    def transform(self, data):
        pass

    @abc.abstractmethod
    def load(self, filepath):
        pass

    @abc.abstractmethod
    def save(self, filepath):
        pass
