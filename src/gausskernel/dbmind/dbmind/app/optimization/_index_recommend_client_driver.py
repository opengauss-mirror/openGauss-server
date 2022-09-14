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

from contextlib import contextmanager
from typing import List

import sqlparse

from dbmind import global_vars
from dbmind.components.index_advisor.executors.common import BaseExecutor


class RpcExecutor(BaseExecutor):

    def execute_sqls(self, sqls) -> List[str]:
        results = []
        sqls = ['set current_schema = %s' % self.get_schema()] + sqls
        set_sqls = []
        for sql in sqls:
            if sql.strip().upper().startswith('SET'):
                set_sqls.append(sql)
            else:
                session_sql = ';'.join(set_sqls + [sql])
                # such as Select or With or Explain
                sql_type = sqlparse.parse(sql)[0].tokens[0].value.upper()
                res = global_vars.agent_rpc_client.call('query_in_database',
                                                        session_sql,
                                                        self.dbname,
                                                        return_tuples=True)
                results.extend([(sql_type,)] + res if res else [('ERROR',)])
        return results

    @contextmanager
    def session(self):
        yield
