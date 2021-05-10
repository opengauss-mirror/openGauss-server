from gensim.models import word2vec

from preprocessing import templatize_sql


class Sentence(object):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        for sql, _ in self.data:
            yield templatize_sql(sql).split()


class Word2Vector(object):
    def __init__(self, max_len=150, **kwargs):
        self.model = None
        self.params = kwargs
        self.max_len = max_len

    def fit(self, sentence):
        sentence = Sentence(sentence)
        self.model = word2vec.Word2Vec(sentence, **self.params)

    def update(self, sentence):
        sentence = Sentence(sentence)
        self.model.build_vocab(sentence, update=True)
        self.model.train(sentence, total_examples=self.model.corpus_count, epochs=self.model.iter)

    def str2vec(self, string):
        vector = list()
        string = templatize_sql(string)
        for item in string.strip().split():
            if item in self.model:
                vector.extend(self.model[item])
            else:
                vector.extend([0.0] * self.params.get('size'))

        if len(vector) >= self.max_len:
            del vector[self.max_len:]
        else:
            vector.extend([0.0] * (self.max_len - len(vector)))

        return vector

    def save(self, filepath):
        self.model.save(filepath)

    def load(self, filepath):
        self.model = word2vec.Word2Vec.load(filepath)
