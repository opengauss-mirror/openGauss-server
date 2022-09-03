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
from sqlalchemy import text

from ..business_db import get_session, session_clz


def truncate_table(table_name):
    with get_session() as session:
        if session_clz.get('db_type') == 'sqlite':
            sql_prefix = 'DELETE FROM '
        else:
            sql_prefix = 'TRUNCATE TABLE '
        session.execute(text(sql_prefix + table_name))
        session.commit()
