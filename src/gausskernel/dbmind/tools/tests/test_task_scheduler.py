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

from dbmind.common.dispatcher import task_scheduler


def test_repeated_timer():
    count = 0

    def increase():
        nonlocal count
        count += 1

    timer = task_scheduler.RepeatedTimer(0.1, increase)
    timer.start()
    time.sleep(0.5)
    assert count >= 5
    # Cancel method should work.
    timer.cancel()
    time.sleep(0.5)
    assert 5 <= count <= 6

    # RepeatedTimer can add into set.
    _set = set()
    timer2 = task_scheduler.RepeatedTimer(0.1, increase)
    _set.add(timer)
    _set.add(timer2)
    assert len(_set) == 1
    timer3 = task_scheduler.RepeatedTimer(0.2, increase)
    _set.add(timer3)
    assert len(_set) == 2


VAR1 = VAR2 = 0


@task_scheduler.timer(0.1)
def outer_increase1():
    global VAR1
    VAR1 += 1


@task_scheduler.timer(0.2)
def outer_increase2():
    global VAR2
    VAR2 += 2


def test_timer_task_mgr():
    tasks = ('RepeatedTimer(outer_increase1, 0.1)', 'RepeatedTimer(outer_increase2, 0.2)')
    for t in tasks:
        assert t in str(task_scheduler.TimedTaskManager.timers)
    assert len(task_scheduler.TimedTaskManager.timers) >= 2
    task_scheduler.TimedTaskManager.start()
    time.sleep(1)
    assert VAR1 <= VAR2
    assert VAR1 >= 10
    task_scheduler.TimedTaskManager.stop()
