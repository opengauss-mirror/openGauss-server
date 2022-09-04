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
import os
import threading
import time

import dbmind.common.utils.cli
from dbmind.common import utils
from dbmind.constants import METRIC_MAP_CONFIG, MISC_PATH

CURR_DIR = os.path.realpath(os.path.dirname(__file__))


def test_read_simple_conf_file():
    conf = utils.read_simple_config_file(
        os.path.join(MISC_PATH, METRIC_MAP_CONFIG)
    )

    assert len(conf) > 0
    for name, value in conf.items():
        assert not name.startswith('#')


def test_write_to_terminal():
    dbmind.common.utils.cli.write_to_terminal(1111)
    dbmind.common.utils.cli.write_to_terminal(111, level='error', color='red')
    dbmind.common.utils.cli.write_to_terminal('hello world', color='yellow')


__results = []


@utils.ttl_cache(0.1, 4)
def need_cache_func(a, b):
    __results.append((a, b))
    return a * b


def test_ttl_cache():
    __inner_results = []
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))
    time.sleep(0.1)
    for i in range(4):
        __inner_results.append(need_cache_func(1, i))
        __inner_results.append(need_cache_func(1, i))

    assert __inner_results == [0, 0, 1, 1, 2, 2, 3, 3, 0, 0, 1, 1, 2, 2, 3, 3, 0, 0, 1, 1, 2, 2, 3, 3]
    assert __results == [(1, 0), (1, 1), (1, 2), (1, 3), (1, 0), (1, 1), (1, 2), (1, 3)]


def test_ttl_ordered_dict():
    d = utils.TTLOrderedDict(0.1)
    d['a'] = 1
    d['b'] = 2
    time.sleep(0.2)
    assert 'a' not in d
    assert d.get('b') is None
    assert len(d) == 0
    d['a'] = 2
    assert d['a'] == 2

    def update(start):
        for i in range(10):
            d[start + i] = i

    t1 = threading.Thread(target=update, args=(0,))
    t2 = threading.Thread(target=update, args=(5,))
    t1.start()
    t2.start()
    assert len(d) == 16
    t1.join()
    t2.join()

    time.sleep(0.5)
    assert len(d) == 0


def test_naive_queue():
    q = utils.NaiveQueue(8)
    for i in range(10):
        q.put(i)

    for i in range(8):
        assert q.get() == 2 + i

    def insert1():
        for i_ in range(4):
            q.put(i_)
        for i_ in q:
            assert i_ >= 0

    def insert2():
        for i_ in range(4):
            q.put(i_)

    t1 = threading.Thread(target=insert1)
    t2 = threading.Thread(target=insert2)
    t1.start()
    t2.start()

    assert len(q) == 8

    assert list(q) == [0, 1, 2, 3, 0, 1, 2, 3]
