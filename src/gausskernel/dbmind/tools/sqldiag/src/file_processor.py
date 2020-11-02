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
import numpy as np
from sql_template import derive_template_of_sql


# output onetime
def get_train_dataset(filepath) -> np.ndarray:
    """
    return all message from train file onetime
    the content must be separated by '\t'
    :return numpy array
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError('file not exists: %s.' % filepath)

    data = np.loadtxt(filepath, dtype=str, delimiter='\t|\t', skiprows=1, usecols=(0, 1, 2, 3, 4))
    for i in range(len(data)):
        data[i, 4] = derive_template_of_sql(data[i, 4])
        data[i, 1] = float(data[i, 1]) - float(data[i, 0])
    data = np.delete(data, obj=0, axis=1)
    return data


# generator of data
def generator_of_train_dataset(filepath):
    """
    return generator for message from train file
    the content must be separated by '\t'
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError('file not exists: %s.' % filepath)

    with open(filepath, encoding='utf-8', mode='r') as f:
        line = f.readline()
        col_name = line.strip().split('\t')
        try:
            time_start_index = col_name.index('TIMER_START')
            time_end_index = col_name.index('TIMER_END')
            lock_time_index = col_name.index('LOCK_TIME')
            time_wait_index = col_name.index('TIMER_WAIT')
            query_index = col_name.index('DIGEST_TEXT')
        except Exception as e:
            raise e

        while True:
            line = f.readline()
            if line.strip() == '':
                break

            line_split = line.strip().split('\t')
            content = list()
            execution_time = float(line_split[time_end_index]) - float(line_split[time_start_index])
            content.append(execution_time)
            content.append(line_split[time_wait_index])
            content.append(line_split[lock_time_index])
            query = line_split[query_index]
            query_template = derive_template_of_sql(query)
            content.append(query_template)
            yield content


def get_test_dataset(filepath) -> list:
    """
    return all message from test file onetime
    return format: list
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError('file not exists: %s.' % filepath)
    queries = list()
    with open(filepath, encoding='utf-8', mode='r') as f:
        while True:
            line = f.readline()
            if line.strip() == '':
                break
            query = line.strip()
            query_template = derive_template_of_sql(query)
            queries.append(query_template)
        return queries


def generator_of_test_dataset(filepath):
    """
    return generator for message from test file
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError('file not exists: %s.' % filepath)

    with open(filepath, encoding='utf-8', mode='r') as f:
        while True:
            line = f.readline()
            if line.strip() == '':
                break
            query = line.strip()
            query_template = derive_template_of_sql(query)
            yield query_template
