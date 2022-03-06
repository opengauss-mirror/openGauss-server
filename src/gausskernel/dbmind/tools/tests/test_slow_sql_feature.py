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
import csv
import os

import numpy as np

from dbmind.app.diagnosis.query.slow_sql.featurelib.feature_model import calculate_weight, build_model

FEATURES_DIMENSION = 5
LABEL_DIMENSION = 1
FEATURES_NUMBER = 3
FEATURE_PATH = os.path.join(os.path.dirname(__file__), 'features')


def remove_file(file_path):
    if os.path.exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)


def check_file(file_path):
    return os.path.exists(file_path)


def test_calculate_weight():
    features, labels = np.zeros((FEATURES_NUMBER, FEATURES_DIMENSION)), np.zeros(FEATURES_NUMBER)
    print(FEATURE_PATH)
    with open(FEATURE_PATH, mode='r') as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            line = [int(item) for item in line]
            features[csv_reader.line_num - 1] = line[:-1]
            labels[csv_reader.line_num - 1] = line[-1]
    weight_matrix = calculate_weight(features, labels)
    assert len(weight_matrix) == 3
    assert len(weight_matrix[0]) == 5


def test_build_model():
    feature_lib_path = 'test_feature_lib.npz'
    remove_file(feature_lib_path)
    build_model(feature_path=FEATURE_PATH, feature_number=FEATURES_NUMBER, feature_dimension=FEATURES_DIMENSION,
                save_path=feature_lib_path)
    assert check_file(feature_lib_path)
    remove_file(feature_lib_path)
