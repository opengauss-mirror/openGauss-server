# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
import time
from threading import local

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras


class Driver:
    def __init__(self):
        self._url = None
        self.parsed_dsn = None
        self.initialized = False
        self._conn = local()

    def initialize(self, url):
        self._url = url
        self.parsed_dsn = psycopg2.extensions.parse_dsn(self._url)
        self.initialized = True
        try:
            dsn = self._url
            conn = psycopg2.connect(dsn)
            conn.cursor().execute('select 1;')
            conn.close()
        except Exception:
            raise ConnectionError()

    @property
    def address(self):
        return '%s:%s' % (self.parsed_dsn['host'], self.parsed_dsn['port'])

    @property
    def dbname(self):
        return self.parsed_dsn['dbname']

    def query(self, stmt, timeout=0, force_connection_db=None):
        if not self.initialized:
            raise AssertionError()

        try:
            conn = self.get_conn(force_connection_db)
            with conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                try:
                    start = time.monotonic()
                    if timeout > 0:
                        cursor.execute('SET statement_timeout = %d;' % (timeout * 1000))
                    cursor.execute(stmt)
                    result = cursor.fetchall()
                except psycopg2.extensions.QueryCanceledError as e:
                    logging.error('%s: %s.' % (e.pgerror, stmt))
                    logging.info(
                        'Time elapsed during execution is %fs '
                        'but threshold is %fs.' % (time.monotonic() - start, timeout)
                    )
                    result = []
            conn.commit()
        except psycopg2.InternalError as e:
            logging.error("Cannot execute '%s' due to internal error: %s." % (stmt, e.pgerror))
            result = []
        except Exception as e:
            logging.exception(e)
            result = []

        return result

    def get_conn(self, force_connection_db=None):
        """Cache the connection in the thread so that the thread can
        reuse this connection next time, thereby avoiding repeated creation.
        By this way, we can realize thread-safe database query,
        and at the same time, it can also have an ability similar to a connection pool. """
        # If query wants to connect to other database by force, we can generate and cache
        # the new connection as the following.
        if force_connection_db:
            db_name = force_connection_db
            parsed_dsn = self.parsed_dsn.copy()
            parsed_dsn['dbname'] = force_connection_db
            dsn = ' '.join(['{}={}'.format(k, v) for k, v in parsed_dsn.items()])
        else:
            db_name = self.dbname
            dsn = self._url

        if not hasattr(self._conn, db_name) or getattr(self._conn, db_name).closed:
            setattr(self._conn, db_name, psycopg2.connect(dsn))
        # Check whether the connection is timeout or invalid.
        try:
            getattr(self._conn, db_name).cursor().execute('select 1;')
        except (
                psycopg2.InternalError,
                psycopg2.InterfaceError,
                psycopg2.errors.AdminShutdown,
                psycopg2.OperationalError
        ) as e:
            logging.warning(
                'Cached database connection to openGauss has been timeout due to %s, reconnecting.' % e
            )
            setattr(self._conn, db_name, psycopg2.connect(dsn))
        except Exception as e:
            logging.error('Failed to connect to openGauss with cached connection (%s), try to reconnect.' % e)
            setattr(self._conn, db_name, psycopg2.connect(dsn))
        return getattr(self._conn, db_name)

