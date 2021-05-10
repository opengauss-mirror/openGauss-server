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

from algorithm.sql_similarity import calc_sql_distance
from preprocessing import get_sql_template
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
            # get 'fine_template' and 'rough_template' of SQL
            fine_template, rough_template = get_sql_template(sql)
            sql_prefix = fine_template.split()[0]
            # if prefix of SQL is not in 'update', 'delete', 'select' and 'insert',
            # then convert prefix to 'other'
            if sql_prefix not in self.__hash_table:
                sql_prefix = 'OTHER'
            if rough_template not in self.__hash_table[sql_prefix]:
                self.__hash_table[sql_prefix][rough_template] = dict()
                self.__hash_table[sql_prefix][rough_template]['info'] = dict()
            if fine_template not in self.__hash_table[sql_prefix][rough_template]['info']:
                self.__hash_table[sql_prefix][rough_template]['info'][fine_template] = \
                    dict(time_list=[], count=0, mean_time=0.0, iter_time=0.0)
            # count the number of occurrences of fine template
            self.__hash_table[sql_prefix][rough_template]['info'][fine_template]['count'] += 1
            # store the execution time of the matched template in the corresponding list
            self.__hash_table[sql_prefix][rough_template]['info'][fine_template][
                'time_list'].append(duration_time)
            # iterative calculation of execution time based on historical data
            if not self.__hash_table[sql_prefix][rough_template]['info'][fine_template][
                'iter_time']:
                self.__hash_table[sql_prefix][rough_template]['info'][fine_template][
                    'iter_time'] = duration_time
            else:
                self.__hash_table[sql_prefix][rough_template]['info'][fine_template]['iter_time'] = \
                    (self.__hash_table[sql_prefix][rough_template]['info'][fine_template][
                         'iter_time'] + duration_time) / 2
        # calculate the average execution time of each template
        for sql_prefix, sql_prefix_info in self.__hash_table.items():
            for rough_template, rough_template_info in sql_prefix_info.items():
                for _, fine_template_info in rough_template_info['info'].items():
                    del fine_template_info['time_list'][:-self.time_list_size]
                    fine_template_info['mean_time'] = \
                        sum(fine_template_info['time_list']) / len(
                            fine_template_info['time_list'])
                rough_template_info['count'] = len(rough_template_info['info'])
                rough_template_info['mean_time'] = sum(
                    [value['mean_time'] for key, value in
                     rough_template_info['info'].items()]) / len(
                    rough_template_info['info'])

    def transform(self, data):
        predict_time_list = []
        for sql in data:
            predict_time = self.predict_duration_time(sql)
            predict_time_list.append([sql, predict_time])
        return predict_time_list

    @LRUCache(max_size=1024)
    def predict_duration_time(self, sql):
        if check_illegal_sql(sql):
            return -1
        sql_prefix = sql.strip().split()[0]
        # get 'fine_template' and 'rough_template' of SQL
        fine_template, rough_template = get_sql_template(sql)
        if sql_prefix not in self.__hash_table:
            sql_prefix = 'OTHER'
        if not self.__hash_table[sql_prefix]:
            logging.warning("'{}' not in the templates.".format(sql))
            predict_time = -1
        elif rough_template not in self.__hash_table[sql_prefix] or fine_template not in \
                self.__hash_table[sql_prefix][rough_template]['info']:
            similarity_info = []
            """ 
            if the template does not exist in the hash table,
            then calculate the possible execution time based on template
            similarity and KNN algorithm in all other templates
            """
            if rough_template not in self.__hash_table[sql_prefix]:
                for local_rough_template, local_rough_template_info in self.__hash_table[sql_prefix].items():
                    similarity_info.append(
                        (self.similarity_algorithm(rough_template, local_rough_template),
                         local_rough_template_info['mean_time']))
            else:
                for local_fine_template, local_fine_template_info in \
                        self.__hash_table[sql_prefix][rough_template]['info'].items():
                    similarity_info.append(
                        (self.similarity_algorithm(fine_template, local_fine_template),
                         local_fine_template_info['iter_time']))
            topn_similarity_info = heapq.nlargest(self.knn_number, similarity_info)
            sum_similarity_scores = sum(item[0] for item in topn_similarity_info) + self.bias
            similarity_proportions = (item[0] / sum_similarity_scores for item in
                                      topn_similarity_info)
            topn_duration_time = (item[1] for item in topn_similarity_info)
            predict_time = reduce(lambda x, y: x + y,
                                  map(lambda x, y: x * y, similarity_proportions,
                                      topn_duration_time))
        else:
            predict_time = self.__hash_table[sql_prefix][rough_template]['info'][fine_template]['iter_time']

        return predict_time

    def load(self, filepath):
        realpath = os.path.realpath(filepath)
        if os.path.exists(realpath):
            template_path = os.path.join(realpath, 'template.json')
            with open(template_path, mode='r') as f:
                self.__hash_table = json.load(f)
            logging.info("template model '{}' is loaded.".format(template_path))
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
        logging.info("template model is stored in '{}'".format(realpath))
