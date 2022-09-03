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
import contextlib

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

from dbmind import global_vars
from ._utils import create_dsn

session_clz = dict()


def update_session_clz_from_configs():
    db_type = global_vars.configs.get('METADATABASE', 'dbtype')
    database = global_vars.configs.get('METADATABASE', 'database')
    host = global_vars.configs.get('METADATABASE', 'host')
    port = global_vars.configs.get('METADATABASE', 'port')
    username = global_vars.configs.get('METADATABASE', 'username')
    password = global_vars.configs.get('METADATABASE', 'password')
    if db_type in ('opengauss', 'postgres'):
        valid_port = port.strip() != '' and port is not None
        valid_host = host.strip() != '' and host is not None
        if not valid_port:
            raise ValueError('Invalid port for metadatabase %s: %s.' % (db_type, port))
        if not valid_host:
            raise ValueError('Invalid host for metadatabase %s: %s.' % (db_type, host))

    dsn = create_dsn(db_type, database, host, port, username, password)
    postgres_dsn = create_dsn(db_type, 'postgres', host, port, username, password)
    if db_type == 'sqlite':
        engine = create_engine(dsn, pool_pre_ping=True, encoding='utf-8')
    else:
        engine = create_engine(dsn, pool_pre_ping=True, encoding='utf-8',
                               pool_size=10, max_overflow=20, pool_recycle=25,
                               connect_args={'connect_timeout': 20, 'application_name': 'DBMind-Service'})

    session_maker = sessionmaker(bind=engine)
    session_clz.update(
        postgres_dsn=postgres_dsn,
        dsn=dsn,
        engine=engine,
        session_maker=session_maker,
        db_type=db_type,
        db_name=database
    )


@contextlib.contextmanager
def get_session():
    if len(session_clz) == 0:
        update_session_clz_from_configs()

    session = session_clz['session_maker']()
    try:
        yield session
        session.commit()
    except Exception as exception:
        session.rollback()
        raise exception
    finally:
        session.close()
