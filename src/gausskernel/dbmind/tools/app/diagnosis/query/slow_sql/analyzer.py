# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import logging
import re
from collections import defaultdict
from typing import List, Dict

import numpy as np

from dbmind.common.types.misc import SlowQuery
from dbmind.common.types.root_cause import RootCause
from .featurelib import load_feature_lib, get_feature_mapper
from .query_feature import QueryFeature
from ..slow_sql import query_info_source

_system_table_keywords = ('PG_', 'GS_')
_shield_keywords = ('CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK')

FEATURE_LIB = load_feature_lib()
FEATURES_CAUSE_MAPPER = get_feature_mapper()
FEATURES, CAUSES, WEIGHT_MATRIX = FEATURE_LIB['features'], FEATURE_LIB['labels'], FEATURE_LIB['weight_matrix']


def _vector_distance(vector1: List, vector2: List, label: int, weight_matrix: List[List]) -> float:
    """
    Calculate the distance between vectors based on the improved Hamming algorithm
    :param vector1: input vector
    :param vector2: input vector
    :param label: input label value of vector2
    :param weight_matrix: weight matrix
    :return: distance of two vectors
    """
    if len(vector1) != len(vector2):
        raise ValueError('not equal.')
    distance = 0.0
    for index in range(len(vector1)):
        if vector1[index] == vector2[index] and vector1[index]:
            distance += weight_matrix[label - 1][index] * vector1[index]
    return distance


def _euclid_distance(vector1: List, vector2: List) -> float:
    """
    Calculate the distance between vectors based on the euclid algorithm
    :param vector1: input vector
    :param vector2: input vector
    :return: distance of two vectors
    """
    dist = np.sqrt(np.sum(np.square([item1 - item2 for item1, item2 in zip(vector1, vector2)])))
    return round(dist, 4)


def _calculate_nearest_feature(sql_feature: List, topk: int = 3) -> List:
    """
    Return the topk feature that is most similar to the input vector
    :param sql_feature: input vector
    :param topk: The number of most similar features output
    :return: The most similar feature and its index information
    """
    indexes = []
    for feature, cause in zip(FEATURES, CAUSES):
        dis = _vector_distance(feature, sql_feature, cause, WEIGHT_MATRIX)
        indexes.append((dis, cause))
    indexes = sorted(indexes, key=lambda x: x[1], reverse=True)
    indexes = list(filter(lambda x: x[0], indexes))
    filter_indexes = []
    _id = 0
    while _id < len(indexes):
        max_dis = 0
        while _id + 1 < len(indexes) and indexes[_id][1] == indexes[_id + 1][1]:
            max_dis = max(indexes[_id][0], indexes[_id + 1][0])
            _id += 1
        filter_indexes.append([max(max_dis, indexes[_id][0]), indexes[_id][1]])
        _id += 1
    probability_sum = sum([item[0] for item in filter_indexes[:topk]])
    filter_indexes = [[item[0] / probability_sum, item[1]] for item in filter_indexes[:topk]]
    filter_indexes = sorted(filter_indexes, key=lambda x: x[0], reverse=True)
    return filter_indexes


class SlowSQLAnalyzer:
    """
    Classes for diagnosing slow SQL
    """

    def __init__(self, topk: int = 3, buffer_capacity: int = 500):
        """
        :param topk: The number of output root causes
        :param buffer_capacity: The length of slow SQL buffer queue
        """
        self.topk = topk
        self.sql_buffers = []
        self.buffer_capacity = buffer_capacity

    def run(self, slow_query_instance: SlowQuery) -> [SlowQuery, None]:
        """
        API for slow SQL diagnostic calls
        :param slow_query_instance: The instance of slow query
        :return: The result of slow query, if the slow query has been diagnosed, then return None
        """
        if self._sql_judge(slow_query_instance):
            return
        data_factory = query_info_source.QueryContext(slow_query_instance)
        pg_class = data_factory.acquire_pg_class()
        schema_info = pg_class.get(slow_query_instance.db_name, {})
        self._analyze(slow_query_instance, data_factory, schema_info)
        return slow_query_instance

    def _sql_judge(self, slow_sql_instance: SlowQuery) -> bool:
        """Determine if the SQL has been diagnosed, return True if it has been diagnosed, else return False"""
        diagnosed_flag = "%s-%s-%s" % (
            slow_sql_instance.start_at, slow_sql_instance.start_at + slow_sql_instance.duration_time,
            slow_sql_instance.template_id)
        if diagnosed_flag in self.sql_buffers:
            return True
        if len(self.sql_buffers) >= self.buffer_capacity:
            self.sql_buffers.pop()
        self.sql_buffers.append(diagnosed_flag)
        return False

    def _analyze(self, slow_sql_instance: SlowQuery, data_factory: query_info_source.QueryContext,
                 schema_infos: Dict) -> [SlowQuery,
                                         None]:
        """Slow SQL diagnosis main process"""
        logging.debug(f"[SLOW QUERY] Diagnosing SQL: {slow_sql_instance.query}")
        exist_tables = defaultdict(list)
        if slow_sql_instance.query.upper() == 'COMMIT' or slow_sql_instance.query.upper().startswith('SET'):
            title = FEATURES_CAUSE_MAPPER.get('C_UNKNOWN')
            root_cause = RootCause.get(title)
            slow_sql_instance.add_cause(root_cause)
            return
        if sum(item in slow_sql_instance.query.upper() for item in _system_table_keywords):
            root_cause = RootCause.get(FEATURES_CAUSE_MAPPER.get('C_VIEW'))
            slow_sql_instance.add_cause(root_cause)
            return
        if sum(item in slow_sql_instance.query.upper() for item in _shield_keywords):
            root_cause = RootCause.get(FEATURES_CAUSE_MAPPER.get('C_SQL'))
            slow_sql_instance.add_cause(root_cause)
            return
        if schema_infos:
            query = slow_sql_instance.query
            regex_result = re.findall(r"([\w\d_]+)\.([\w\d_]+)", slow_sql_instance.query)
            if regex_result:
                for schema, table in regex_result:
                    exist_tables[schema].append(table)
                    query.replace("%s.%s" % (schema, table), ' ')
            for table in schema_infos[slow_sql_instance.schema_name]:
                if table in query:
                    exist_tables[slow_sql_instance.schema_name].append(table)
        slow_sql_instance.tables_name = exist_tables
        feature_generator = QueryFeature(slow_sql_instance, data_factory)
        feature_generator.initialize_metrics()
        feature, details = feature_generator()
        logging.debug("[SLOW QUERY] Feature vector: %s, detail: %s", feature, details)
        topk_root_cause = _calculate_nearest_feature(feature, topk=self.topk)
        logging.debug("[SLOW QUERY] Topk root cause: %s", topk_root_cause)
        if topk_root_cause:
            for probability, index in topk_root_cause:
                cause_key = f"C{index}"
                title = FEATURES_CAUSE_MAPPER.get(cause_key, 'C_UNKNOWN')
                root_cause = RootCause.get(title)
                root_cause.set_probability(probability)
                root_cause.format(**details)
                slow_sql_instance.add_cause(root_cause)
        if not slow_sql_instance.root_causes:
            title = FEATURES_CAUSE_MAPPER.get('C_UNKNOWN')
            root_cause = RootCause.get(title)
            slow_sql_instance.add_cause(root_cause)
