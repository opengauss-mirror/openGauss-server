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

import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ProgrammingError

from .base import Base, DynamicConfig
from .schema import load_all_schema_models
from ..common.exceptions import SQLExecutionError


def create_metadatabase_schema(check_first=True):
    from .business_db import update_session_clz_from_configs
    from .business_db import session_clz

    update_session_clz_from_configs()
    load_all_schema_models()
    try:
        Base.metadata.create_all(
            session_clz.get('engine'),
            checkfirst=check_first
        )
    except Exception as e:
        raise SQLExecutionError(e)


def destroy_metadatabase():
    from .business_db import update_session_clz_from_configs
    from .business_db import session_clz

    update_session_clz_from_configs()
    load_all_schema_models()
    try:
        Base.metadata.drop_all(
            session_clz.get('engine')
        )
    except Exception as e:
        raise SQLExecutionError(e)


def create_dynamic_config_schema():
    from sqlalchemy.orm import sessionmaker
    from dbmind.constants import DYNAMIC_CONFIG
    from ._utils import create_dsn
    from .dao.dynamic_config import table_mapper

    load_all_schema_models()
    engine = create_engine(create_dsn('sqlite', DYNAMIC_CONFIG), encoding='utf-8')
    DynamicConfig.metadata.create_all(engine)

    # Batch insert default values into config tables.
    with sessionmaker(engine, autocommit=True, autoflush=True)() as session:
        for table_name, table in table_mapper.items():
            try:
                session.bulk_save_objects(table.default_values())
            except sqlalchemy.exc.IntegrityError:
                # May be duplicate, ignore it.
                pass
