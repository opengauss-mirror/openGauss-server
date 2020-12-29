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
import json
import os
import pickle
import random
from collections import defaultdict
from functools import lru_cache, reduce

from sklearn.decomposition import PCA

from .sql_processor import sql_similarity, get_sql_template

TIME_LIST_SIZE = 10
KNN_NUMBER = 3
SAMPLE_NUMBER = 5
HISTORY_VECTORS_LIMIT = 3000
POINT_NUMBER = 3


class SqlTemplate:
    def __init__(self, template_path, word2vec_model):
        self._template_path = template_path
        self._w2v_model = word2vec_model
        self._hash_table = dict(INSERT=dict(), UPDATE=dict(), DELETE=dict(), SELECT=dict(), OTHER=dict())
        self._pca_model = None

    @property
    def template(self):
        return self._hash_table

    @template.setter
    def template(self, data):
        history_vectors = list()
        db_template_path = os.path.join(self._template_path, 'template.json')
        pca_model_path = os.path.join(self._template_path, 'decomposition.pickle')
        history_sql_vecs_path = os.path.join(self._template_path, 'history_sql_vectors')
        if os.path.exists(db_template_path):
            self.load_template(db_template_path)
        if os.path.exists(history_sql_vecs_path):
            history_vectors = self.load_history_vectors(history_sql_vecs_path)
        sql_number = list(data)
        if len(sql_number) <= 1:
            self._pca_model = None
        else:
            self._pca_model = PCA(n_components=2)
        self.worker(data, history_vectors)
        self.save_template(db_template_path)
        self.save_decompose_model(pca_model_path)
        if len(history_vectors) > HISTORY_VECTORS_LIMIT:
            del history_vectors[:-HISTORY_VECTORS_LIMIT]
        self.save_history_vectors(history_sql_vecs_path, history_vectors)
        self.predict_exec_time.cache_clear()

    def worker(self, db_data, history_vectors):
        for exec_time, sql in db_data:
            filtered_sql, sql_template = get_sql_template(sql)
            history_vectors.append(self._w2v_model.str2vec(filtered_sql))
            sql_prefix = filtered_sql.split()[0]
            if sql_prefix not in self._hash_table.keys():
                sql_prefix = 'OTHER'
            if sql_template in self._hash_table[sql_prefix]:
                if filtered_sql in self._hash_table[sql_prefix][sql_template]['sql_info']:
                    self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time_list'].append(exec_time)
                else:
                    self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql] = dict()
                    self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time_list'] = []
                    self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time_list'].append(exec_time)
            else:
                self._hash_table[sql_prefix][sql_template] = dict()
                self._hash_table[sql_prefix][sql_template]['sql_info'] = dict()
                self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql] = dict()
                self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time_list'] = []
                self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time_list'].append(exec_time)

        if self._pca_model:
            self._pca_model.fit(history_vectors)

        template_id_index = 0
        for sql_prefix, sql_prefix_info in self._hash_table.items():
            for sql_template, sql_template_info in sql_prefix_info.items():
                decomposition_str_vecs = []
                sql_template_info['template_id'] = template_id_index
                template_id_index += 1
                for filtered_sql, filtered_sql_info in sql_template_info['sql_info'].items():
                    del filtered_sql_info['time_list'][:-TIME_LIST_SIZE]
                    filtered_sql_info['time'] = reduce(lambda x, y: (x + y) / 2, filtered_sql_info['time_list'])
                    filtered_sql_info['point'] = [0.0, 0.0] if self._pca_model is None else \
                        self._pca_model.transform([self._w2v_model.str2vec(filtered_sql)]).tolist()[0]
                    decomposition_str_vecs.append([filtered_sql_info['point']])
                    filtered_sql_info['mean_time'] = sum(filtered_sql_info['time_list']) / \
                                                     len(filtered_sql_info['time_list'])
                sql_template_info['center'] = [sum([item[0][0] for item in decomposition_str_vecs]) / \
                                               len(sql_template_info['sql_info']),
                                               sum([item[0][1] for item in decomposition_str_vecs]) / \
                                               len(sql_template_info['sql_info'])]
                sql_template_info['mean_time'] = sum(
                    [value['mean_time'] for key, value in sql_template_info['sql_info'].items()]) / \
                                                 len(sql_template_info['sql_info'])

    @lru_cache(maxsize=10240)
    def predict_exec_time(self, sql):
        if not sql.strip():
            exec_time = 0.0
            template_id = -1
            point = [-100, -100]
            return exec_time, template_id, point
        else:
            filtered_sql, sql_template = get_sql_template(sql)
            sql_prefix = filtered_sql.split()[0]
            if sql_prefix not in self._hash_table.keys():
                sql_prefix = "OTHER"
            if not self._hash_table[sql_prefix]:
                exec_time = 0.0
                template_id = -1
                point = [0.0, 0.0] if self._pca_model is None else \
                    self._pca_model.transform([self._w2v_model.str2vec(filtered_sql)]).tolist()[0]
                return exec_time, template_id, point
            elif sql_template in self._hash_table[sql_prefix]:
                if filtered_sql in self._hash_table[sql_prefix][sql_template]['sql_info']:
                    exec_time = self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['time']
                    template_id = self._hash_table[sql_prefix][sql_template]['template_id']
                    point = self._hash_table[sql_prefix][sql_template]['sql_info'][filtered_sql]['point']
                    return exec_time, template_id, point
                else:
                    similarity_result = defaultdict(list)
                    for sql_info in self._hash_table[sql_prefix][sql_template]['sql_info']:
                        exec_time = self._hash_table[sql_prefix][sql_template]['sql_info'][sql_info]['time']
                        similarity_of_sql = sql_similarity(filtered_sql, sql_info)
                        similarity_result[sql_info].extend([similarity_of_sql, exec_time])
                    topn_sql_info = sorted(similarity_result.items(), key=lambda x: x[1][0], reverse=True)[:KNN_NUMBER]
                    topn_exec_times = [item[1][1] for item in topn_sql_info]
                    similarity_of_sqls = [item[1][0] for item in topn_sql_info]
                    similarity_proportions = [item / sum(similarity_of_sqls) for item in similarity_of_sqls]
                    exec_time = reduce(lambda x, y: x + y,
                                       map(lambda x, y: x * y, similarity_proportions, topn_exec_times))
                    template_id = self._hash_table[sql_prefix][sql_template]['template_id']
                    point = [0.0, 0.0] if self._pca_model is None else \
                        self._pca_model.transform([self._w2v_model.str2vec(filtered_sql)]).tolist()[0]
                    return exec_time, template_id, point
            else:
                most_similar_template = ''
                similarity_value = -1
                for template in self._hash_table[sql_prefix]:
                    if sql_similarity(sql_template, template) > similarity_value:
                        most_similar_template = template
                exec_time = self._hash_table[sql_prefix][most_similar_template]['mean_time']
                template_id = self._hash_table[sql_prefix][most_similar_template]['template_id']
                point = [0.0, 0.0] if self._pca_model is None else \
                    self._pca_model.transform([self._w2v_model.str2vec(filtered_sql)]).tolist()[0]
                return exec_time, template_id, point

    def predict_batch_exec_time(self, sqls):
        result = self._init_result()
        db_template_path = os.path.join(self._template_path, 'template.json')
        pca_model_path = os.path.join(self._template_path, 'decomposition.pickle')
        if not os.path.exists(db_template_path):
            result['data']['background'] = dict()
            return result
        else:
            self.load_template(db_template_path)
        if not sqls:
            result['data']['background'] = self.get_template_background()
            return result
        if os.path.exists(pca_model_path):
            self.load_decompose_model(pca_model_path)
        else:
            self._pca_model = None
        for sql in sqls:
            exec_time, template_id, point = self.predict_exec_time(sql)
            result['data']['time'].append(exec_time)
            result['data']['cluster'].append(str(template_id))
            result['data']['points'].append(point)
        result['data']['background'] = self.get_template_background()
        return result

    def get_template_background(self):
        background = dict()
        for sql_prefix, sql_prefix_info in self._hash_table.items():
            for sql_template, sql_template_info in sql_prefix_info.items():
                background[sql_template_info['template_id']] = dict()
                if len(sql_template_info['sql_info']) > SAMPLE_NUMBER:
                    background[sql_template_info['template_id']]['stmts'] = \
                        random.sample(sql_template_info['sql_info'].keys(), SAMPLE_NUMBER)
                else:
                    background[sql_template_info['template_id']]['stmts'] = list(sql_template_info['sql_info'].keys())
                background[sql_template_info['template_id']]['center'] = sql_template_info['center']
                background[sql_template_info['template_id']]['avg_time'] = sql_template_info['mean_time']
                background[sql_template_info['template_id']]['points'] = \
                    [sql_template_info['sql_info'][sql]['point'] \
                     for sql in background[sql_template_info['template_id']]['stmts']]
        return background

    @staticmethod
    def _init_result():
        result = dict()
        result['status'] = 'success'
        result['data'] = dict()
        result['data']['time'] = list()
        result['data']['points'] = list()
        result['data']['cluster'] = list()
        return result

    def save_template(self, template_path):
        with open(template_path, mode='w') as f:
            json.dump(self._hash_table, f, indent=4)

    def load_template(self, template_path):
        if os.path.exists(template_path):
            with open(template_path, mode='r') as f:
                self._hash_table = json.load(f)

    def save_decompose_model(self, pca_path):
        with open(pca_path, mode='wb') as f:
            pickle.dump(self._pca_model, f)

    def load_decompose_model(self, pca_path):
        if os.path.exists(pca_path):
            with open(pca_path, mode='rb') as f:
                self._pca_model = pickle.load(f)

    @staticmethod
    def save_history_vectors(history_vectors_path, history_vectors):
        with open(history_vectors_path, mode='wb') as f:
            pickle.dump(history_vectors, f)

    @staticmethod
    def load_history_vectors(history_vectors_path):
        with open(history_vectors_path, mode='rb') as f:
            return pickle.load(f)
