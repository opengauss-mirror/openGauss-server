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
import heapq
import json
import logging
import os
import stat
from functools import reduce
from collections import defaultdict

from algorithm.sql_similarity import calc_sql_distance
from preprocessing import get_sql_template, templatize_sql
from utils import check_illegal_sql, LRUCache
from . import AbstractModel


class TemplateModel(AbstractModel):
    def __init__(self, params):
        super().__init__(params)
        self.bias = 1e-5
        self.__hash_table = dict(INSERT=dict(), UPDATE=dict(), DELETE=dict(), SELECT=dict(),
                                 OTHER=dict())
        self.time_list_size = params.time_list_size
        self.knn_number = params.knn_number
        self.similarity_algorithm = calc_sql_distance(params.similarity_algorithm)

    # training method for template model
    def fit(self, data):
        for sql, duration_time in data:
            if check_illegal_sql(sql):
                continue
            sql_template = templatize_sql(sql)
            sql_prefix = sql_template.split()[0]
            if sql_prefix not in self.__hash_table:
                sql_prefix = 'OTHER'
            if sql_template not in self.__hash_table[sql_prefix]:
                self.__hash_table[sql_prefix][sql_template] = dict(time_list=[], count=0, mean_time=0.0, iter_time=0.0)
            self.__hash_table[sql_prefix][sql_template]['count'] += 1
            self.__hash_table[sql_prefix][sql_template]['time_list'].append(duration_time)

        for sql_prefix, sql_prefix_info in self.__hash_table.items():
            for sql_template, sql_template_info in sql_prefix_info.items():
                del sql_template_info['time_list'][:-self.time_list_size]
                sql_template_info['mean_time'] = sum(sql_template_info['time_list']) / len(sql_template_info['time_list'])
                sql_template_info['iter_time'] = reduce(lambda x, y: (x+y)/2, sql_template_info['time_list'])


    def transform(self, data):
        predict_result_dict = defaultdict(list)
        for sql in data:
            sql_, status, predict_time, top_similarity_sql = self.predict_duration_time(sql)
            predict_result_dict[status].append([sql_, predict_time, top_similarity_sql])
        for key, value in predict_result_dict.items():
            if value:
                value.sort(key=lambda item: item[1], reverse=True)
        return predict_result_dict

    @LRUCache(max_size=1024)
    def predict_duration_time(self, sql):
        top_similarity_sql = None
        if check_illegal_sql(sql):
            predict_time = -1
            status = 'Suspect illegal sql'
            return sql, status, predict_time, top_similarity_sql

        sql_template = templatize_sql(sql)
        # get 'sql_template' of SQL
        sql_prefix = sql_template.strip().split()[0]
        if sql_prefix not in self.__hash_table:
            sql_prefix = 'OTHER'
        if not self.__hash_table[sql_prefix]:
            status = 'No SQL information'
            predict_time = -1
        elif sql_template not in self.__hash_table[sql_prefix]:
            similarity_info = []
            """
            if the template does not exist in the hash table,
            then calculate the possible execution time based on template
            similarity and KNN algorithm in all other templates
            """
            status = 'No SQL template found'
            for local_sql_template, local_sql_template_info in self.__hash_table[sql_prefix].items():
                similarity_info.append(
                    (self.similarity_algorithm(sql_template, local_sql_template),
                     local_sql_template_info['mean_time'], local_sql_template))
            topn_similarity_info = heapq.nlargest(self.knn_number, similarity_info)
            sum_similarity_scores = sum(item[0] for item in topn_similarity_info)
            if not sum_similarity_scores:
                sum_similarity_scores = self.bias
            top_similarity_sql = '\n'.join([item[2] for item in topn_similarity_info])
            similarity_proportions = [item[0] / sum_similarity_scores for item in
                                      topn_similarity_info]
            topn_duration_time = [item[1] for item in topn_similarity_info]
            predict_time = reduce(lambda x, y: x + y,
                                  map(lambda x, y: x * y, similarity_proportions,
                                      topn_duration_time))

        else:
            status = 'Fine match'
            predict_time = self.__hash_table[sql_prefix][sql_template]['iter_time']
            top_similarity_sql = sql_template

        return sql, status, predict_time, top_similarity_sql

    def load(self, filepath):
        realpath = os.path.realpath(filepath)
        if os.path.exists(realpath):
            template_path = os.path.join(realpath, 'template.json')
            with open(template_path, mode='r') as f:
                self.__hash_table = json.load(f)
        else:
            logging.error("{} not exist.".format(realpath))

    def save(self, filepath):
        realpath = os.path.realpath(filepath)
        if not os.path.exists(realpath):
            os.makedirs(realpath, mode=0o700)
        if oct(os.stat(realpath).st_mode)[-3:] != '700':
            os.chmod(realpath, stat.S_IRWXU)
        template_path = os.path.join(realpath, 'template.json')
        with open(template_path, mode='w') as f:
            json.dump(self.__hash_table, f, indent=4)
        print("Template model is stored in '{}'".format(realpath))

