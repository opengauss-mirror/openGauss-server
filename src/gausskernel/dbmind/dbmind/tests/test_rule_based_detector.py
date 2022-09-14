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
from dbmind.app.monitoring import detect_future
from dbmind.common.types import Sequence


def test_disk_spill():
    values = [x / 100 for x in range(100)]
    timestamps = list(range(100))
    full_sequence = Sequence(timestamps=timestamps, values=values, name="os_disk_usage")
    latest_sequences = (full_sequence[0, 79],)
    future_sequences = (full_sequence[80, 100],)
    alarm_list = detect_future("10.244.44.157", ("os_disk_usage",), latest_sequences, future_sequences)
    assert len(alarm_list) >= 1
    for alarm in alarm_list:
        assert alarm.host == "10.244.44.157"
        assert alarm.alarm_content
