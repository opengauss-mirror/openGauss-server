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
import os


def get_train_dataset(filepath):
    """
    return all message from train file onetime
    the content must be separated by '\t'
    :return numpy array
    """

    def line_processor(line):
        first_delimater_pos = line.index(',')
        exec_time = float(line[:first_delimater_pos])
        sql = line[first_delimater_pos + 1:].strip().strip('"')
        if sql in ('NULL',) or not sql:
            return
        return exec_time, sql

    if not os.path.exists(filepath):
        raise FileNotFoundError('%s not exists.' % filepath)
    with open(filepath, mode='r') as f:
        contents = f.readlines()
        contents = tuple(filter(lambda item: item, list(map(line_processor, contents))))
        return contents


def get_test_dataset(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError('%s not exists.' % filepath)
    with open(filepath, mode='r') as f:
        contents = f.readlines()
        contents = tuple(map(lambda item: item.strip().strip('"'), contents))
        return contents
