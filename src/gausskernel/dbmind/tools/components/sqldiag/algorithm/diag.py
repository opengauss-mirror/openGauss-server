# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import sys

from ..preprocessing import LoadData
from .duration_time_model.dnn import DnnModel
from .duration_time_model.template import TemplateModel

W2V_SUFFIX = 'word2vector'


def check_template_algorithm(param):
    if param and param not in ["list", "levenshtein", "parse_tree", "cosine_distance"]:
        raise ValueError("The similarity algorithm '%s' is invaild, "
                         "please choose from ['list', 'levenshtein', 'parse_tree', 'cosine_distance']" % param)


class ModelConfig(object):
    def __init__(self):
        pass

    def init_from(self, config):
        pass

    @classmethod
    def init_from_config_parser(cls, config):
        config_instance = cls()
        config_instance.init_from(config)
        return config_instance


class DnnConfig(ModelConfig):
    def __init__(self):
        super().__init__()
        self.epoch = 300

    def init_from(self, config):
        self.epoch = config.get("dnn", "epoch") if \
            config.get("dnn", "epoch") else self.epoch
        self.epoch = int(self.epoch)

class TemplateConfig(ModelConfig):
    def __init__(self):
        super().__init__()
        self.similarity_algorithm = "list"
        self.time_list_size = 10
        self.knn_number = 3

    def init_from(self, config):
        self.similarity_algorithm = config.get("template", "similarity_algorithm") if \
            config.get("template", "similarity_algorithm") else self.similarity_algorithm
        check_template_algorithm(self.similarity_algorithm)
        self.time_list_size = config.get("template", "time_list_size") if \
            config.get("template", "time_list_size") else self.time_list_size
        self.knn_number = config.get("template", "knn_number", ) if \
            config.get("template", "knn_number") else self.knn_number
        self.time_list_size = int(self.time_list_size)
        self.knn_number = int(self.knn_number)

SUPPORTED_ALGORITHM = {'dnn': lambda config: DnnModel(DnnConfig.init_from_config_parser(config)),
                       'template': lambda config: TemplateModel(
                           TemplateConfig.init_from_config_parser(config))}


class SQLDiag:
    def __init__(self, model_algorithm, params):
        if model_algorithm not in SUPPORTED_ALGORITHM:
            raise NotImplementedError("do not support {}".format(model_algorithm))
        try:
            self._model = SUPPORTED_ALGORITHM.get(model_algorithm)(params)
        except ValueError as e:
            logging.error(e, exc_info=True)
            sys.exit(1)

    def fit(self, data):
        self._model.fit(data)

    def transform(self, data):
        return self._model.transform(data)

    def fine_tune(self, filepath, data):
        self._model.load(filepath)
        self._model.fit(data)

    def load(self, filepath):
        self._model.load(filepath)

    def save(self, filepath):
        self._model.save(filepath)
