"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
from gensim.models import word2vec

from .sql_processor import sql_filter


class MySentence(object):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        for _, sql in self.data:
            yield sql_filter(sql).split()


class Word2Vector(object):
    def __init__(self, max_len=150, **kwargs):
        self.model = None
        self.params = kwargs
        self.max_len = max_len

    def train(self, sentence):
        sentence = MySentence(sentence)
        self.model = word2vec.Word2Vec(sentence, **self.params)

    def update(self, sentence):
        sentence = MySentence(sentence)
        self.model.build_vocab(sentence, update=True)
        self.model.train(sentence, total_examples=self.model.corpus_count, epochs=self.model.iter)

    def str2vec(self, string):
        str_vec = list()
        for item in string.strip().split():
            if item in self.model:
                str_vec.extend(self.model[item])
            else:
                str_vec.extend([0.0] * self.params.get('size'))
        if len(str_vec) >= self.max_len:
            del str_vec[self.max_len:]
        else:
            str_vec.extend([0.0] * (self.max_len - len(str_vec)))
        return str_vec

    def save_model(self, model_path):
        self.model.save(model_path)

    def load_model(self, model_path):
        self.model = word2vec.Word2Vec.load(model_path)
