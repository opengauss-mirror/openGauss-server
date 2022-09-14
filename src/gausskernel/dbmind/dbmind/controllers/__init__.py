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

from ..common.utils import where_am_i


def get_dbmind_controller():
    _, _, files = next(
        os.walk(os.path.realpath(os.path.dirname(__file__)), topdown=True)
    )
    base = where_am_i(globals())
    return ['%s.%s' % (base, c.rsplit('.')[0]) for c in filter(lambda x: x.endswith('.py'), files)]
