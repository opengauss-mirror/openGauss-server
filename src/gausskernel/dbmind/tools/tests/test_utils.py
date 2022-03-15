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

import os

from dbmind.constants import METRIC_MAP_CONFIG, MISC_PATH
from dbmind.common import utils

CURR_DIR = os.path.realpath(os.path.dirname(__file__))


def test_read_simple_conf_file():
    conf = utils.read_simple_config_file(
        os.path.join(MISC_PATH, METRIC_MAP_CONFIG)
    )

    assert len(conf) > 0
    for name, value in conf.items():
        assert not name.startswith('#')


def test_write_to_terminal():
    utils.write_to_terminal(1111)
    utils.write_to_terminal(111, level='error', color='red')
    utils.write_to_terminal('hello world', color='yellow')
