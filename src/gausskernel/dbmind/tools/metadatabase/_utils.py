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
from urllib import parse

DB_TYPES = {'sqlite', 'opengauss', 'postgresql', 'mysql'}


def create_dsn(
        db_type,
        database,
        host=None,
        port=None,
        username=None,
        password=None
):
    """Generate a DSN (Data Source Name) according to the user's given parameters.
    Meanwhile, DBMind will adapt some interfaces to SQLAlchemy, such as openGauss."""
    if db_type not in DB_TYPES:
        raise ValueError("Not supported database type '%s'." % db_type)
    if db_type == 'opengauss':
        db_type = 'postgresql'
        # DBMind has to override the following method.
        # Otherwise, SQLAlchemy will raise an exception about unknown version.
        from sqlalchemy.dialects.postgresql.base import PGDialect
        PGDialect._get_server_version_info = lambda *args: (9, 2)
    if db_type == 'sqlite':
        dsn = '{}:///{}?check_same_thread=False'.format(db_type, database)
    else:
        username = parse.quote(username)
        password = parse.quote(password)
        host = parse.quote(host)
        database = parse.quote(database)
        dsn = '{}://{}:{}@{}:{}/{}'.\
            format(db_type, username, password, host, port, database)
    return dsn
