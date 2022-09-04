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

import time

from dbmind.common.dispatcher.task_worker import ProcessWorker


def square(v):
    return v * v


def sleepy_square(v):
    time.sleep(0.2)
    return v * v


process_worker = ProcessWorker(10)


def test_process_worker():
    assert process_worker.apply(square, True, (2,)) == 4
    assert process_worker.parallel_execute(square, ((0,), (1,), (2,), (3,), (4,))) == [0, 1, 4, 9, 16]

    # Should execute in parallel, only waiting for the slowest one.
    start_time = time.time()
    process_worker.parallel_execute(sleepy_square, [(v,) for v in range(10)])
    end_time = time.time()
    assert 0.2 < (end_time - start_time) < 0.3


def test_blocking_task():
    start_time = time.time()
    process_worker._parallel_execute(sleepy_square, [[1], [1], [1], [1], [1]])
    end_time = time.time()

    interval = end_time - start_time
    assert 0.2 < interval < 0.3
