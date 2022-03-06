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
import logging

from dbmind.common.types.root_cause import RootCause
from .slow_sql.analyzer import SlowSQLAnalyzer

_analyzer = SlowSQLAnalyzer()


def diagnose_query(slow_query):
    try:
        _analyzer.run(slow_query)
    except Exception as e:
        slow_query.add_cause(RootCause.get('LACK_INFORMATION'))
        logging.exception(e)
    return slow_query
