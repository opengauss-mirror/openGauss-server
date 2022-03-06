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

from dbmind.constants import DYNAMIC_CONFIG
from ._utils import create_dsn


_session_maker = None


@contextlib.contextmanager
def get_session():
    global _session_maker

    if not _session_maker:
        # Notice: Current working directory is the confpath, so we can
        # use the relative path directly.
        dsn = create_dsn('sqlite', DYNAMIC_CONFIG)
        engine = create_engine(dsn)
        _session_maker = sessionmaker(engine)

    session = _session_maker()
    try:
        yield session
        session.commit()
    except Exception as exception:
        session.rollback()
        raise exception
    finally:
        session.close()


