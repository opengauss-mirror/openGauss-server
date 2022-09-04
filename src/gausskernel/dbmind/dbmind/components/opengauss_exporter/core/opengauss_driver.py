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
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras

from dbmind.common.utils import dbmind_assert
from dbmind.common.utils.exporter import warn_logging_and_terminal


class Driver:
    def __init__(self):
        self._url = None
        self.parsed_dsn = None
        self.initialized = False
        self._conn = local()

    def initialize(self, url):
        try:
            # Specify default schema is public.
            conn = psycopg2.connect(
                url, options="-c search_path=public",
                application_name='DBMind-openGauss-exporter'
            )
            conn.cursor().execute('select 1;')
            conn.close()
            self._url = url
            self.parsed_dsn = psycopg2.extensions.parse_dsn(url)
            self.initialized = True
        except Exception as e:
            raise ConnectionError(e)

    @property
    def address(self):
        return '%s:%s' % (self.parsed_dsn['host'], self.parsed_dsn['port'])

    @property
    def dbname(self):
        return self.parsed_dsn['dbname']

    @property
    def username(self):
        return self.parsed_dsn['user']

    @property
    def pwd(self):
        return self.parsed_dsn['password']

    def query(self, stmt, timeout=0, force_connection_db=None, return_tuples=False):
        if not self.initialized:
            raise AssertionError()
        cursor_dict = {}
        if not return_tuples:
            cursor_dict['cursor_factory'] = psycopg2.extras.RealDictCursor
        try:
            conn = self.get_conn(force_connection_db)
            with conn.cursor(
                    **cursor_dict
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
            setattr(self._conn, db_name, psycopg2.connect(
                dsn, application_name='DBMind-openGauss-exporter'))
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
            setattr(self._conn, db_name, psycopg2.connect(
                dsn, application_name='DBMind-openGauss-exporter'))
        except Exception as e:
            logging.error('Failed to connect to openGauss with cached connection (%s), try to reconnect.' % e)
            setattr(self._conn, db_name, psycopg2.connect(
                dsn, application_name='DBMind-openGauss-exporter'))
        return getattr(self._conn, db_name)


class DriverBundle:
    __main_db_name__ = 'postgres'

    _thread_pool_executor = ThreadPoolExecutor(
        thread_name_prefix='DriverBundleWorker'
    )

    def __init__(
            self, url,
            include_db_list=None,
            exclude_db_list=None
    ):
        self.main_driver = Driver()
        self.main_driver.initialize(url)  # If cannot access, raise a ConnectionError.
        self._bundle = {self.main_driver.dbname: self.main_driver}
        if self.main_dbname != DriverBundle.__main_db_name__:
            warn_logging_and_terminal(
                'The default connection database of the exporter is not postgres, '
                'so it is possible that some database metric information '
                'cannot be collected, such as slow SQL queries.'
            )
        if not self.is_monitor_admin():
            warn_logging_and_terminal(
                'The current user does not have the MonitorAdmin permission, '
                'which will cause many metrics to fail to obtain. '
                'Please consider granting this permission to the connecting user.'
            )

        for dbname in self._discover_databases(include_db_list, exclude_db_list):
            if dbname in self._bundle:
                continue

            try:
                driver = Driver()
                driver.initialize(self._splice_url_for_other_db(dbname))

                self._bundle[dbname] = driver  # Ensure that each driver can access corresponding database.
            except ConnectionError:
                logging.warning(
                    'Cannot connect to the database %s by using the given user.', dbname
                )

    def _discover_databases(self, include_dbs, exclude_dbs):
        if not include_dbs:
            include_dbs = {}
        if not exclude_dbs:
            exclude_dbs = {}
        include_dbs = set(include_dbs)
        exclude_dbs = set(exclude_dbs)

        # We cannot allow to both set these two arguments.
        dbmind_assert(not (include_dbs and exclude_dbs))

        all_db_list = self.main_driver.query(
            'SELECT datname FROM pg_catalog.pg_database;',
            return_tuples=True
        )
        discovered = set()
        for dbname in all_db_list:
            if dbname in ('template0', 'template1'):  # Skip these useless databases.
                continue
            discovered.add(dbname[0])

        if include_dbs:
            return discovered.intersection(include_dbs)

        return discovered - exclude_dbs

    def _splice_url_for_other_db(self, dbname):
        parsed_dsn = self.main_driver.parsed_dsn.copy()
        parsed_dsn['dbname'] = dbname
        return ' '.join(['{}={}'.format(k, v) for (k, v) in parsed_dsn.items()])

    def query(self, stmt, timeout=0, force_connection_db=None, return_tuples=False):
        """A decorator for Driver.query. If the caller sets
        the parameter `force_connection_db`, this method only returns
        the query result from this specified database.
        Otherwise, the method will return the
        union set of each database's execution result.

        This method need to guaranteed thread safety.
        """
        if force_connection_db is not None:
            if force_connection_db not in self._bundle:
                return []
            return self._bundle[force_connection_db].query(stmt, timeout, None, return_tuples)

        # Use multiple threads to query.
        futures = []
        for dbname in self._bundle:
            driver = self._bundle[dbname]
            futures.append(
                DriverBundle._thread_pool_executor.submit(
                    driver.query, stmt, timeout, None, return_tuples
                )
            )

        union_set = set()
        for future in as_completed(futures):
            try:
                # Because the driver's execution result is a 2-dimension array,
                # we need to take the array apart then combine them.
                result = future.result()
                for row in result:
                    if return_tuples:
                        union_set.add(tuple(row))
                    else:
                        union_set.add(tuple(row.items()))
            except Exception as e:
                logging.exception(e)
        if return_tuples:
            return list(union_set)
        else:
            # Get back to the dict-based format.
            ret = []
            for row in union_set:
                dict_based_row = {}
                for k, v in row:
                    dict_based_row[k] = v
                ret.append(dict_based_row)
            return ret

    @property
    def address(self):
        return self.main_driver.address

    @property
    def main_dbname(self):
        return self.main_driver.dbname

    def is_monitor_admin(self):
        r = self.main_driver.query(
            'select rolmonitoradmin from pg_roles where rolname = CURRENT_USER;',
            return_tuples=True
        )
        return r[0][0]

