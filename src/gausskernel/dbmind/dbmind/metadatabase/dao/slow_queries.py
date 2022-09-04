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
"""This file involves all slow query tables. They are:
slow_queries: tb_slow_queries
slow_queries_journal: tb_slow_queries_journal
slow_queries_killed: tb_slow_queries_killed

This file offers some common operations for each slow query table.
Meanwhile, supply some basic analysis functionalities.
"""
import time

from sqlalchemy import func
from sqlalchemy.orm import load_only
from sqlalchemy.sql import text, desc, and_

from dbmind.common.parser import sql_parsing

from ._common import truncate_table
from ..business_db import get_session
from ..schema import SlowQueries
from ..schema import SlowQueriesJournal
from ..schema import SlowQueriesKilled


def key_value_format(query):
    rv = {}
    for row in query:
        v1, v2 = row[0], row[1]
        rv[v1] = v2
    return rv


def _recognize_query_type(query):
    q = query.upper()
    if q.startswith('SELECT'):
        return 's'
    elif q.startswith('DELETE'):
        return 'd'
    elif q.startswith('UPDATE'):
        return 'u'
    elif q.startswith('INSERT'):
        return 'i'
    else:
        return None  # e.g., the query stats with 'create' or comments.


def insert_slow_query(
        address, schema_name, db_name, query, hashcode1, hashcode2=None,
        template_id=None, hit_rate=None, fetch_rate=None, plan_time=None,
        parse_time=None, db_time=None, cpu_time=None, data_io_time=None,
        root_cause=None, suggestion=None
):
    query_type = _recognize_query_type(query)
    with get_session() as session:
        session.add(
            SlowQueries(
                address=address,
                schema_name=schema_name,
                db_name=db_name,
                query=query,
                query_type=query_type,
                involving_systable='PG_' in query.upper(),
                template_id=template_id,
                hashcode1=hashcode1,
                hashcode2=hashcode2,
                insert_at=int(time.time() * 1000),
                hit_rate=hit_rate,
                fetch_rate=fetch_rate,
                cpu_time=cpu_time,
                data_io_time=data_io_time,
                plan_time=plan_time,
                parse_time=parse_time,
                db_time=db_time,
                root_cause=root_cause,
                suggestion=suggestion
            )
        )


def insert_slow_query_journal(slow_query_id, start_at, duration_time):
    with get_session() as session:
        session.merge(
            SlowQueriesJournal(
                slow_query_id=slow_query_id,
                start_at=start_at,
                round_start_at=int(start_at / 1000) * 1000,
                duration_time=duration_time,
            )
        )


def select_slow_query_id_by_hashcode(hashcode1, hashcode2):
    with get_session() as session:
        result = session.query(SlowQueries.slow_query_id).filter(
            and_(SlowQueries.hashcode1 == hashcode1,
                 SlowQueries.hashcode2 == hashcode2)
        ).order_by(desc(SlowQueries.insert_at)).limit(1)
        return result


def select_slow_queries(target_list=(), query=None, start_time=None, end_time=None, limit=50, group: bool = False):
    with get_session() as session:
        if group:
            tb_journal = session.query(
                SlowQueriesJournal.slow_query_id, func.count(SlowQueriesJournal.slow_query_id).label('count')
            ).group_by(
                SlowQueriesJournal.slow_query_id
            ).order_by(text('count DESC')).subquery()

            result = session.query(
                SlowQueries.db_name,
                SlowQueries.query,
                SlowQueries.root_cause,
                SlowQueries.suggestion,
                tb_journal.c.count
            ).join(
                tb_journal, and_(
                    tb_journal.c.slow_query_id == SlowQueries.slow_query_id
                )
            )
        else:
            if len(target_list) > 0:
                attr_targets = []
                for column_name in target_list:
                    if hasattr(SlowQueries, column_name):
                        attr_targets.append(getattr(SlowQueries, column_name))
                    else:
                        attr_targets.append(getattr(SlowQueriesJournal, column_name))
                result = session.query(
                    *attr_targets
                )
            else:
                result = session.query(
                    SlowQueries.address,
                    SlowQueries.schema_name,
                    SlowQueries.db_name,
                    SlowQueries.query,
                    SlowQueries.template_id,
                    SlowQueries.hit_rate,
                    SlowQueries.fetch_rate,
                    SlowQueries.cpu_time,
                    SlowQueries.data_io_time,
                    SlowQueries.parse_time,
                    SlowQueries.plan_time,
                    SlowQueries.db_time,
                    SlowQueries.root_cause,
                    SlowQueries.suggestion,
                    SlowQueriesJournal.start_at,
                    SlowQueriesJournal.duration_time
                )
            result = result.select_from(SlowQueriesJournal).join(
                SlowQueries,
                SlowQueriesJournal.slow_query_id == SlowQueries.slow_query_id
            )

            if query:
                result = result.filter(
                    SlowQueries.query.like('%{}%'.format(query))
                )
            if start_time:
                result = result.filter(
                    SlowQueriesJournal.start_at >= start_time
                )
            if end_time:
                result = result.filter(
                    SlowQueriesJournal.start_at <= end_time
                )
            result = result.order_by(desc(SlowQueriesJournal.start_at))

        if limit is None:
            return result

        return result.limit(limit)


def count_slow_queries(distinct=False):
    with get_session() as session:
        if distinct:
            return session.query(SlowQueries.slow_query_id).count()
        return session.query(SlowQueriesJournal.slow_query_id).count()


def group_by_dbname():
    with get_session() as session:
        query = session.query(SlowQueries.db_name, func.count(1)).group_by(SlowQueries.db_name)
        return key_value_format(query)


def group_by_schema():
    with get_session() as session:
        query = session.query(SlowQueries.schema_name, func.count(1)).group_by(SlowQueries.schema_name)
        return key_value_format(query)


def execute_on_the_table(sql):
    with get_session() as session:
        return session.execute(text(sql)).all()


def count_systable():
    stmt = """
    with systable(n) as (select count(1) from tb_slow_queries where involving_systable),
     bussiness(n) as (select count(1) from tb_slow_queries where not involving_systable)
      select systable.n, bussiness.n from systable, bussiness;
"""
    result = execute_on_the_table(stmt)
    if len(result) > 0:
        systable, busstable = result[0]
        return {'system_table': systable, 'business_table': busstable}

    return {}


def slow_query_trend():
    stmt = """
        select round_start_at as time, count(1) 
        from tb_slow_queries_journal group by time order by time limit 100;
        """
    result = execute_on_the_table(stmt)
    if len(result) > 0:
        timestamps = []
        values = []
        for row in result:
            timestamp, value = row
            timestamps.append(timestamp)
            values.append(value)
        return {'timestamps': timestamps, 'values': values}

    return {'timestamps': [], 'values': []}


def slow_query_distribution():
    stmt = """
    SELECT (SELECT Count(1)
        FROM   tb_slow_queries
        WHERE  query_type = 's'),
       (SELECT Count(1)
        FROM   tb_slow_queries
        WHERE  query_type = 'd'),
       (SELECT Count(1)
        FROM   tb_slow_queries
        WHERE  query_type = 'i'),
       (SELECT Count(1)
        FROM   tb_slow_queries
        WHERE  query_type = 'u'); 
    """
    result = execute_on_the_table(stmt)
    if len(result) > 0:
        select, delete, insert, update = result[0]
        return {
            'select': select,
            'delete': delete,
            'insert': insert,
            'update': update
        }

    return {'select': 0, 'delete': 0, 'insert': 0, 'update': 0}


def mean_cpu_time():
    with get_session() as session:
        avg = session.query(func.avg(SlowQueries.cpu_time) / 1000 / 1000).all()[0][0]
        if avg is None:
            return -1
        return avg


def mean_io_time():
    with get_session() as session:
        avg = session.query(func.avg(SlowQueries.data_io_time) / 1000 / 1000).all()[0][0]
        if avg is None:
            return -1
        return avg


def mean_fetch_rate():
    with get_session() as session:
        avg = session.query(func.avg(SlowQueries.fetch_rate) * 100).all()[0][0]
        if avg is None:
            return -1
        return avg


def mean_buffer_hit_rate():
    with get_session() as session:
        avg = session.query(func.avg(SlowQueries.hit_rate) * 100).all()[0][0]
        if avg is None:
            return -1
        return avg


def delete_slow_queries(retention_start_time):
    """To prevent the table from over-expanding."""
    with get_session() as session:
        session.query(SlowQueries).filter(
            SlowQueries.insert_at <= retention_start_time
        ).delete()
        session.query(SlowQueriesJournal).filter(
            SlowQueriesJournal.start_at <= retention_start_time
        )


def truncate_slow_queries():
    truncate_table(SlowQueries.__tablename__)
    truncate_table(SlowQueriesJournal.__tablename__)


def slow_query_template():
    stmt = """
    SELECT t1.template_id,
           t1.count,
           t2.query
    FROM
      (SELECT template_id,
              Count(1) AS COUNT
       FROM tb_slow_queries GROUP  BY template_id) t1
    INNER JOIN
      (SELECT template_id,
              query,
              ROW_NUMBER() OVER (PARTITION BY template_id
                                 ORDER BY insert_at DESC) AS rn
       FROM tb_slow_queries) t2 
       ON t1.template_id = t2.template_id
    WHERE t2.rn = 1
      ORDER  BY t1.count DESC
    LIMIT 50;
    """
    return execute_on_the_table(stmt)


def insert_killed_slow_queries(db_name, query, killed, username, elapsed_time, killed_time):
    with get_session() as session:
        session.add(
            SlowQueriesKilled(
                db_name=db_name.lower(),
                query=sql_parsing.standardize_sql(query),
                killed=killed,
                username=username,
                elapsed_time=elapsed_time,
                killed_time=killed_time,
            )
        )


def select_killed_slow_queries(query=None, start_time=None, end_time=None, limit=None):
    with get_session() as session:
        result = session.query(SlowQueriesKilled)
        if query:
            result = result.filter(
                SlowQueriesKilled.query.like('%{}%'.format(query))
            )
        if start_time:
            result = result.filter(
                SlowQueriesKilled.killed_time >= start_time
            )
        if end_time:
            result = result.filter(
                SlowQueriesKilled.killed_time <= end_time
            )

        result = result.order_by(SlowQueriesKilled.killed_time)
        if limit is None:
            return result

        return result.limit(limit)


def delete_killed_slow_queries(retention_start_time):
    """To prevent the table from over-expanding."""
    with get_session() as session:
        session.query(SlowQueriesKilled).filter(
            SlowQueriesKilled.killed_time <= retention_start_time
        ).delete()


def truncate_killed_slow_queries():
    truncate_table(SlowQueriesKilled.__tablename__)
