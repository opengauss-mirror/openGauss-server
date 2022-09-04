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

from dbmind.common.platform import LINUX
from dbmind.common.types.root_cause import RootCause
from .slow_sql.analyzer import SlowSQLAnalyzer

if LINUX:
    from dbmind.common.dispatcher.task_worker import get_mp_sync_manager

    shared_sql_buffer = get_mp_sync_manager().list()
else:
    shared_sql_buffer = None
_analyzer = SlowSQLAnalyzer(buffer=shared_sql_buffer)


def diagnose_query(query_context):
    try:
        _analyzer.run(query_context)
    except Exception as e:
        query_context.slow_sql_instance.add_cause(RootCause.get('LACK_INFORMATION'))
        logging.exception(e)
    return query_context.slow_sql_instance
