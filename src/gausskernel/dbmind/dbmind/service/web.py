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
"""This module is merely for web service
and does not disturb internal operations in DBMind."""
import datetime
import getpass
import json
import logging
import os
import sys
import time
from collections import defaultdict
from itertools import groupby

from dbmind import global_vars
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.types import ALARM_TYPES
from dbmind.common.utils import ttl_cache
from dbmind.components.sql_rewriter import get_all_involved_tables, SQLRewriter
from dbmind.metadatabase import dao
from dbmind.service.utils import SequenceUtils, get_master_instance_address
from . import dai


def get_metric_sequence_internal(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    """Timestamps are microsecond level."""
    if to_timestamp is None:
        to_timestamp = int(time.time() * 1000)
    if from_timestamp is None:
        from_timestamp = to_timestamp - 0.5 * 60 * 60 * 1000  # It defaults to show a half of hour.
    from_datetime = datetime.datetime.fromtimestamp(from_timestamp / 1000)
    to_datetime = datetime.datetime.fromtimestamp(to_timestamp / 1000)
    fetcher = dai.get_metric_sequence(metric_name, from_datetime, to_datetime, step)
    return fetcher


def get_metric_sequence(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    fetcher = get_metric_sequence_internal(metric_name, from_timestamp, to_timestamp, step)
    result = fetcher.fetchall()
    result.sort(key=lambda s: str(s.labels))  # Sorted by labels.
    return list(map(lambda s: s.jsonify(), result))


def get_forecast_sequence_info(metric_name):
    return sqlalchemy_query_jsonify(dao.forecasting_metrics.aggregate_forecasting_metric(metric_name))


def get_stored_forecast_sequence(metric_name, start_at=None, limit=None):
    r = dao.forecasting_metrics.select_forecasting_metric(metric_name, min_metric_time=start_at)
    metric_forecast_result = dict()
    for row in r:
        metric_time = row.metric_time
        metric_value = row.metric_value
        labels = row.labels
        if labels not in metric_forecast_result:
            metric_forecast_result[labels] = {'timestamps': [], 'values': []}

        # skip duplication
        if len(metric_forecast_result[labels]['timestamps']) > 0 and \
                metric_time <= metric_forecast_result[labels]['timestamps'][-1]:
            continue
        metric_forecast_result[labels]['timestamps'].append(metric_time)
        metric_forecast_result[labels]['values'].append(float(metric_value))
    rv = []
    for labels in metric_forecast_result:
        forecast_entity = metric_forecast_result[labels]
        forecast_entity['labels'] = json.loads(labels)
        forecast_entity['name'] = metric_name
        if limit is not None:
            forecast_entity['timestamps'] = forecast_entity['timestamps'][-limit:]
            forecast_entity['values'] = forecast_entity['values'][-limit:]
        rv.append(forecast_entity)
    # Bring into correspondence with the function get_metric_sequence().
    rv.sort(key=lambda v: str(v['labels']))
    return rv


@ttl_cache(10)
def get_metric_forecast_sequence(metric_name, from_timestamp=None, to_timestamp=None, step=None):
    fetcher = get_metric_sequence_internal(metric_name, from_timestamp, to_timestamp, step)
    sequences = fetcher.fetchall()
    if len(sequences) == 0:
        return []

    forecast_length_factor = 0.33  # 1 / 3
    if from_timestamp is None or to_timestamp is None:
        forecast_minutes = (sequences[0].timestamps[-1] - sequences[0].timestamps[0]) * \
                           forecast_length_factor / 60 / 1000
    else:
        forecast_minutes = (to_timestamp - from_timestamp) * forecast_length_factor / 60 / 1000

    try:
        metric_value_range = global_vars.metric_value_range_map.get(metric_name)
        lower, upper = map(float, metric_value_range.split(','))
    except Exception:
        lower, upper = 0, float("inf")
    # Sorted by labels to bring into correspondence with get_metric_sequence().
    sequences.sort(key=lambda s: str(s.labels))
    future_sequences = global_vars.worker.parallel_execute(
        quickly_forecast, ((sequence, forecast_minutes, lower, upper)
                           for sequence in sequences)
    ) or []

    return list(map(lambda s: s.jsonify() if s else {}, future_sequences))


@ttl_cache(seconds=60)
def get_xact_status():
    committed = dai.get_latest_metric_value('pg_db_xact_commit').fetchall()
    aborted = dai.get_latest_metric_value('pg_db_xact_rollback').fetchall()

    rv = defaultdict(lambda: defaultdict(dict))
    for seq in committed:
        from_instance = SequenceUtils.from_server(seq)
        datname = seq.labels['datname']
        value = seq.values[0]
        rv[from_instance][datname]['commit'] = value
    for seq in aborted:
        from_instance = SequenceUtils.from_server(seq)
        datname = seq.labels['datname']
        value = seq.values[0]
        rv[from_instance][datname]['abort'] = value

    return rv


@ttl_cache(seconds=24 * 60 * 60)
def get_cluster_node_status():
    node_list = []
    topo = {'root': [], 'leaf': []}
    sequences = dai.get_latest_metric_value('pg_node_info_uptime').fetchall()
    for seq in sequences:
        node_info = {
            'node_name': seq.labels['node_name'],
            'address': SequenceUtils.from_server(seq),
            'is_slave': seq.labels['is_slave'],
            'installation_path': seq.labels['installpath'],
            'data_path': seq.labels['datapath'],
            'uptime': seq.values[0],
            'version': seq.labels['version'],
            'datname': seq.labels.get('datname', 'unknown')
        }
        node_list.append(node_info)
        if node_info['node_name'].startswith('cn_'):
            topo['root'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'cn'})
        elif node_info['node_name'].startswith('dn_'):
            topo['leaf'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'dn'})
        else:
            if node_info['is_slave'] == 'Y':
                topo['leaf'].append({'address': node_info['address'], 'datname': node_info['datname'], 'type': 'slave'})
            else:
                topo['root'].append(
                    {'address': node_info['address'], 'datname': node_info['datname'], 'type': 'master'})

    return {'topo': topo, 'node_list': node_list}


def stat_object_proportion(obj_read_metric, obj_hit_metric):
    obj_read = dai.get_latest_metric_value(obj_read_metric).fetchall()
    obj_read_tbl = defaultdict(dict)
    for s in obj_read:
        host = SequenceUtils.from_server(s)
        datname = s.labels['datname']
        value = s.values[0]
        obj_read_tbl[host][datname] = value
    obj_hit = dai.get_latest_metric_value(obj_hit_metric).fetchall()
    obj_hit_tbl = defaultdict(dict)
    for s in obj_hit:
        host = SequenceUtils.from_server(s)
        datname = s.labels['datname']
        value = s.values[0]
        obj_hit_tbl[host][datname] = value
    buff_hit_tbl = defaultdict(dict)
    for host in obj_read_tbl.keys():
        for datname in obj_read_tbl[host].keys():
            read_value = obj_read_tbl[host][datname]
            hit_value = obj_hit_tbl[host][datname]
            buff_hit_tbl[host][datname] = hit_value / (hit_value + read_value + 0.0001)

    return buff_hit_tbl


def stat_buffer_hit():
    return stat_object_proportion('pg_db_blks_read', 'pg_db_blks_hit')


def stat_idx_hit():
    return stat_object_proportion('pg_index_idx_blks_read', 'pg_index_idx_blks_hit')


def stat_xact_successful():
    return stat_object_proportion('pg_db_xact_rollback', 'pg_db_xact_commit')


def stat_group_by_host(to_agg_tbl):
    each_db = to_agg_tbl
    return {
        host: sum(each_db[host].values()) / len(each_db[host])
        for host in each_db
    }


@ttl_cache(seconds=60)
def get_running_status():
    buffer_pool = stat_group_by_host(stat_buffer_hit())
    index = stat_group_by_host(stat_idx_hit())
    transaction = stat_group_by_host(stat_xact_successful())

    hosts = set(buffer_pool.keys())
    hosts = hosts.intersection(index.keys(), transaction.keys())
    rv = defaultdict(dict)
    for host in hosts:
        rv[host]['buffer_pool'] = buffer_pool[host]
        rv[host]['index'] = index[host]
        rv[host]['transaction'] = transaction[host]

        try:
            host_ip = host.split(':')[0]
        except IndexError:
            logging.error('Cannot split the host string: %s.', host)
            host_ip = host
        try:
            rv[host]['cpu'] = 1 - dai.get_latest_metric_value('os_cpu_usage') \
                .from_server(host_ip).fetchone().values[0]
            rv[host]['mem'] = 1 - dai.get_latest_metric_value('os_mem_usage') \
                .from_server(host_ip).fetchone().values[0]
            rv[host]['io'] = 1 - dai.get_latest_metric_value('os_disk_usage') \
                .from_server(host_ip).fetchone().values[0]
        except IndexError as e:
            logging.warning('Cannot fetch value from sequence with given fields. '
                            'Maybe relative metrics are not stored in the time-series database or '
                            'there are multiple exporters connecting with a same instance.', exc_info=e)
    return rv


@ttl_cache(seconds=60)
def get_host_status():
    rv = defaultdict(dict)
    for seq in dai.get_latest_metric_value('os_cpu_usage').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['os_cpu_usage'] = seq.values[0]
    for seq in dai.get_latest_metric_value('os_mem_usage').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['os_mem_usage'] = seq.values[0]
    for seq in dai.get_latest_metric_value('os_disk_usage').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['os_disk_usage'] = seq.values[0]
    for seq in dai.get_latest_metric_value('os_cpu_processor_number').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['cpu_cores'] = seq.values[0]
    for seq in dai.get_latest_metric_value('node_uname_info').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['host_name'] = seq.labels['nodename']
        rv[host]['release'] = seq.labels['release']
    for seq in dai.get_latest_metric_value('node_time_seconds').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['node_timestamp'] = seq.values[0]
    for seq in dai.get_latest_metric_value('node_boot_time_seconds').fetchall():
        host = SequenceUtils.from_server(seq)
        rv[host]['boot_time'] = rv[host]['node_timestamp'] - seq.values[0]

    return rv


@ttl_cache(seconds=60)
def get_database_list():
    sequences = dai.get_latest_metric_value('pg_database_is_template').fetchall()
    rv = set()
    for seq in sequences:
        if seq.values[0] != 1:
            rv.add(seq.labels['datname'])
    return list(rv)


def get_latest_alert():
    alerts = list(global_vars.self_driving_records)
    alerts.reverse()  # Bring the latest events to the front
    return alerts


@ttl_cache(seconds=24 * 60 * 60)
def get_cluster_summary():
    node_status = get_cluster_node_status()['node_list']
    nb_node = len(node_status)
    nb_cn = 0
    nb_dn = 0
    nb_not_slave = 0
    nb_slave = 0
    for node in node_status:
        node_name = node['node_name']
        is_slave = False if node['is_slave'] == 'N' else True

        if node_name.startswith('cn'):
            nb_cn += 1
        if node_name.startswith('dn'):
            nb_dn += 1
        if is_slave:
            nb_slave += 1
        else:
            nb_not_slave += 1

    version = node_status[0]['version'] if nb_node > 0 else 'unknown due to configuration error'
    if nb_cn > 0:
        form = 'GaussDB (for openGauss, %d CN and %d DN)' % (nb_cn, nb_dn)
    elif nb_node == 1 and nb_not_slave == 1:
        form = 'openGauss (Single Node)'
    elif nb_slave > 0:
        form = 'openGauss (Single Node, Master-Standby Replication)'
    else:
        form = 'unknown'
    return {
        'cluster_summary': {
            'deployment_form': form,
            'version': version,
            'exporters': nb_node,
            'master': nb_node - nb_slave,
            'slave': nb_slave,
            'cn': nb_cn,
            'dn': nb_dn
        },
        'runtime': {
            'python_version': sys.version,
            'python_file_path': sys.executable,
            'python_path': os.environ.get('PYTHONPATH', '(None)'),
            'deployment_user': getpass.getuser(),
            'path': os.environ.get('PATH', '(None)'),
            'ld_library_path': os.environ.get('LD_LIBRARY_PATH', '(None)')
        }
    }


def toolkit_recommend_knobs_by_metrics():
    return {
        "metric_snapshot": sqlalchemy_query_jsonify(dao.knob_recommendation.select_metric_snapshot(),
                                                    field_names=('host', 'metric', 'value')),
        "warnings": sqlalchemy_query_jsonify(dao.knob_recommendation.select_warnings(),
                                             field_names=('host', 'level', 'comment')),
        "details": sqlalchemy_query_jsonify(dao.knob_recommendation.select_details(),
                                            field_names=('host', 'name', 'current', 'recommend', 'min', 'max'))
    }


def get_db_schema_table_count():
    db_set = set()
    schema_set = set()
    table_set = set()
    results = dai.get_latest_metric_value('pg_class_relsize').fetchall()
    for res in results:
        db_name, schema_name, table_name = res.labels['datname'], res.labels['nspname'], res.labels['relname']
        db_set.add(db_name)
        schema_set.add((db_name, schema_name))
        table_set.add((db_name, schema_name, table_name))
    return len(db_set), len(schema_set), len(table_set)


def get_latest_indexes_stat():
    latest_indexes_stat = defaultdict(int)
    latest_recommendation_stat = dao.index_recommendation.get_latest_recommendation_stat()
    for res in latest_recommendation_stat:
        latest_indexes_stat['suggestions'] += res.recommend_index_count
        latest_indexes_stat['redundant_indexes'] += res.redundant_index_count
        latest_indexes_stat['invalid_indexes'] += res.invalid_index_count
        latest_indexes_stat['stmt_count'] += res.stmt_count
        latest_indexes_stat['positive_sql_count'] += res.positive_stmt_count
    latest_indexes_stat['valid_index'] = (
            len(list(dao.index_recommendation.get_existing_indexes())) -
            latest_indexes_stat['redundant_indexes'] -
            latest_indexes_stat['invalid_indexes']
    )
    return latest_indexes_stat


def get_index_change():
    timestamps = []
    index_count_change = dict()
    recommendation_stat = dao.index_recommendation.get_recommendation_stat()
    for res in recommendation_stat:
        timestamp = res.occurrence_time
        if timestamp not in timestamps:
            timestamps.append(timestamp)
        if timestamp not in index_count_change:
            index_count_change[timestamp] = defaultdict(int)
        index_count_change[timestamp]['suggestions'] += res.recommend_index_count
        index_count_change[timestamp]['redundant_indexes'] += res.redundant_index_count
        index_count_change[timestamp]['invalid_indexes'] += res.invalid_index_count
    suggestions_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['suggestions'] for timestamp in timestamps]
    }
    redundant_indexes_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['redundant_indexes'] for timestamp in
                   timestamps]
    }
    invalid_indexes_change = {
        'timestamps': timestamps,
        'values': [index_count_change[timestamp]['invalid_indexes'] for timestamp in timestamps]
    }
    return {'suggestions': suggestions_change, 'redundant_indexes': redundant_indexes_change,
            'invalid_indexes': invalid_indexes_change}


def get_index_details():
    advised_indexes = dict()
    _advised_indexes = dao.index_recommendation.get_advised_index()
    advised_indexes['header'] = ['schema', 'database', 'table', 'advised_indexes', 'number_of_indexes', 'select',
                                 'update',
                                 'delete', 'insert', 'workload_improvement_rate']
    advised_indexes['rows'] = []
    group_func = lambda x: (x.schema_name, x.db_name, x.tb_name)
    for (schema_name, db_name, tb_name), group in groupby(sorted(_advised_indexes, key=group_func),
                                                          key=group_func):
        group_result = list(group)
        row = [schema_name, db_name, tb_name,
               ';'.join([x.columns for x in group_result]),
               len(group_result),
               int(group_result[0].select_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].update_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].delete_ratio * group_result[0].stmt_count / 100),
               int(group_result[0].insert_ratio * group_result[0].stmt_count / 100),
               sum(float(x.optimized) / len(group_result) for x in group_result)]
        advised_indexes['rows'].append(row)
    positive_sql = dict()
    positive_sql['header'] = ['schema', 'database', 'table', 'template', 'typical_sql_stmt',
                              'number_of_sql_statement']
    positive_sql['rows'] = []
    for positive_sql_result in dao.index_recommendation.get_advised_index_details():
        row = [positive_sql_result[2].schema_name, positive_sql_result[2].db_name,
               positive_sql_result[2].tb_name, positive_sql_result[1].template,
               positive_sql_result[0].stmt,
               positive_sql_result[0].stmt_count]
        positive_sql['rows'].append(row)

    return {'advised_indexes': advised_indexes, 'positive_sql': positive_sql}


def get_existing_indexes():
    filenames = ['db_name', 'tb_name', 'columns', 'index_stmt']
    return sqlalchemy_query_jsonify(dao.index_recommendation.get_existing_indexes(), filenames)


@ttl_cache(seconds=10 * 60)
def get_index_advisor_summary():
    db_count, schema_count, table_count = get_db_schema_table_count()
    index_advisor_summary = {'db': db_count,
                             'schema': schema_count,
                             'table': table_count}
    latest_indexes_stat = get_latest_indexes_stat()
    index_advisor_summary.update(latest_indexes_stat)
    index_advisor_summary.update(get_index_change())
    index_details = get_index_details()
    index_advisor_summary.update(index_details)
    index_advisor_summary.update(
        {'improvement_rate': 100 * float(
            latest_indexes_stat['positive_sql_count'] / latest_indexes_stat['stmt_count']) if latest_indexes_stat[
            'stmt_count'] else 0})
    index_advisor_summary.update({'existing_indexes': get_existing_indexes()})
    return index_advisor_summary


def get_all_metrics():
    return list(global_vars.metric_map.keys())


def sqlalchemy_query_jsonify(query, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if not field_names:
        field_names = query.statement.columns.keys()  # in order keys.
    rv['header'] = field_names
    for result in query:
        if hasattr(result, '__iter__'):
            row = list(result)
        else:
            row = [getattr(result, field) for field in field_names]
        rv['rows'].append(row)
    return rv


def psycopg2_dict_jsonify(realdict, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if len(realdict) == 0:
        return rv

    if not field_names:
        rv['header'] = list(realdict[0].keys())
    for obj in realdict:
        row = []
        for field in rv['header']:
            row.append(obj[field])
        rv['rows'].append(row)
    return rv


def get_history_alarms(host=None, alarm_type=None, alarm_level=None, group: bool = False):
    return sqlalchemy_query_jsonify(dao.alarms.select_history_alarm(host, alarm_type, alarm_level, group=group))


def get_future_alarms(metric_name=None, host=None, start_at=None, group: bool = False):
    return sqlalchemy_query_jsonify(dao.alarms.select_future_alarm(metric_name, host, start_at, group=group))


def get_security_alarms(host=None):
    return get_history_alarms(host, alarm_type=ALARM_TYPES.SECURITY)


def get_healing_info(action=None, success=None, min_occurrence=None):
    return sqlalchemy_query_jsonify(
        dao.healing_records.select_healing_records(action, success, min_occurrence)
    )


@ttl_cache(600)
def get_slow_queries(query=None, start_time=None, end_time=None, limit=None, group: bool = False):
    query = dao.slow_queries.select_slow_queries([], query, start_time, end_time, limit, group=group)
    return sqlalchemy_query_jsonify(query)


@ttl_cache(10)
def get_killed_slow_queries(query=None, start_time=None, end_time=None, limit=None):
    query = dao.slow_queries.select_killed_slow_queries(query, start_time, end_time, limit)
    return sqlalchemy_query_jsonify(query)


@ttl_cache(60)
def get_slow_query_summary():
    # Maybe multiple nodes, but we don't need to care.
    # Because that is an abnormal scenario.
    threshold = dai.get_latest_metric_value('pg_settings_setting') \
        .filter(name='log_min_duration_statement') \
        .fetchone().values[0]
    return {
        'nb_unique_slow_queries': dao.slow_queries.count_slow_queries(),
        'slow_query_threshold': threshold,
        'main_slow_queries': dao.slow_queries.count_slow_queries(distinct=True),
        'statistics_for_database': dao.slow_queries.group_by_dbname(),
        'statistics_for_schema': dao.slow_queries.group_by_schema(),
        'systable': dao.slow_queries.count_systable(),
        'slow_query_count': dao.slow_queries.slow_query_trend(),
        'distribution': dao.slow_queries.slow_query_distribution(),
        'mean_cpu_time': dao.slow_queries.mean_cpu_time(),
        'mean_io_time': dao.slow_queries.mean_io_time(),
        'mean_buffer_hit_rate': dao.slow_queries.mean_buffer_hit_rate(),
        'mean_fetch_rate': dao.slow_queries.mean_fetch_rate(),
        'slow_query_template': sqlalchemy_query_jsonify(dao.slow_queries.slow_query_template(),
                                                        ['template_id', 'count', 'query']),
        'table_of_slow_query': get_slow_queries(limit=20)
    }


@ttl_cache(60)
def get_top_queries(username, password):
    stmt = """\
    SELECT user_name,
           unique_sql_id,
           query,
           n_calls,
           min_elapse_time,
           max_elapse_time,
           total_elapse_time / ( n_calls + 0.001 ) AS avg_elapse_time,
           n_returned_rows,
           db_time,
           cpu_time,
           execution_time,
           parse_time,
           last_updated,
           sort_spill_count,
           hash_spill_count
    FROM   dbe_perf.statement
    ORDER  BY n_calls,
              avg_elapse_time DESC
    LIMIT  10; 
    """
    res = global_vars.agent_rpc_client.call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    sorted_fields = [
        'user_name',
        'unique_sql_id',
        'query',
        'n_calls',
        'min_elapse_time',
        'max_elapse_time',
        'avg_elapse_time',
        'n_returned_rows',
        'db_time',
        'cpu_time',
        'execution_time',
        'parse_time',
        'last_updated',
        'sort_spill_count',
        'hash_spill_count'
    ]
    return psycopg2_dict_jsonify(res, sorted_fields)


def get_active_query(username, password):
    stmt = """\
    SELECT datname,
           usename,
           application_name,
           client_addr,
           query_start,
           waiting,
           state,
           query,
           connection_info
    FROM   pg_stat_activity; 
    """
    res = global_vars.agent_rpc_client.call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    sorted_fields = [
        'datname',
        'usename',
        'application_name',
        'client_addr',
        'query_start',
        'waiting',
        'state',
        'query',
        'connection_info'
    ]
    return psycopg2_dict_jsonify(res, sorted_fields)


def get_holding_lock_query(username, password):
    stmt = """\
    SELECT c.relkind,
       d.datname,
       c.relname,
       l.mode,
       s.query,
       extract(epoch
               FROM pg_catalog.now() - s.xact_start) AS holding_time
    FROM pg_locks AS l
    INNER JOIN pg_database AS d ON l.database = d.oid
    INNER JOIN pg_class AS c ON l.relation = c.oid
    INNER JOIN pg_stat_activity AS s ON l.pid = s.pid
    WHERE s.pid != pg_catalog.pg_backend_pid();
    """
    res = global_vars.agent_rpc_client.call_with_another_credential(
        username, password, 'query_in_postgres', stmt
    )
    return psycopg2_dict_jsonify(res)


def check_credential(username, password):
    return global_vars.agent_rpc_client.handshake(username, password, receive_exception=True)


def toolkit_index_advise(database, sqls):
    for i, sql in enumerate(sqls):
        advise_stmt = sql.replace("'", "''").strip()
        advise_stmt = "select * from gs_index_advise('%s')" % advise_stmt
        sqls[i] = advise_stmt

    stmt = ' union '.join(sqls) + ';'
    res = global_vars.agent_rpc_client.call('query_in_database', stmt, database, return_tuples=False)
    return psycopg2_dict_jsonify(res)


def toolkit_rewrite_sql(database, sqls, rewritten_flags=None, if_format=True):
    rewritten_sqls = []
    if rewritten_flags is None:
        rewritten_flags = []
    schemas_results = global_vars.agent_rpc_client.call('query_in_database',
                                                        'select distinct(table_schema) from information_schema.tables;',
                                                        database, return_tuples=True)
    schemas = ','.join([res[0] for res in schemas_results]) if schemas_results else 'public'
    for _sql in sqls.split(';'):
        if not _sql.strip():
            continue
        sql = _sql + ';'
        sql_checking_stmt = 'set current_schema=%s;explain %s' % (schemas, sql)
        if not global_vars.agent_rpc_client.call('query_in_database', sql_checking_stmt, database, return_tuples=False):
            rewritten_sqls.append(sql)
            rewritten_flags.append(False)
            continue
        table2columns_mapper = dict()
        table_exists_primary = dict()
        involved_tables = get_all_involved_tables(sql)
        for table_name in involved_tables:
            search_table_stmt = "select column_name, ordinal_position " \
                                "from information_schema.columns where table_name='%s';" % table_name
            results = sorted(global_vars.agent_rpc_client.call('query_in_database', search_table_stmt,
                                                               database,
                                                               return_tuples=True), key=lambda x: x[1])
            table2columns_mapper[table_name] = [res[0] for res in results]
            exists_primary_stmt = "SELECT count(*)  FROM information_schema.table_constraints WHERE " \
                                  "constraint_type in ('PRIMARY KEY', 'UNIQUE') AND table_name = '%s'" % table_name
            table_exists_primary[table_name] = \
                global_vars.agent_rpc_client.call('query_in_database', exists_primary_stmt, database,
                                                  return_tuples=True)[0][0]
            rewritten_flag, output_sql = SQLRewriter().rewrite(sql, table2columns_mapper, table_exists_primary,
                                                               if_format)
            rewritten_flags.append(rewritten_flag)
            rewritten_sqls.append(output_sql)
    return '\n'.join(rewritten_sqls)


def toolkit_slow_sql_rca(sql, database, schema=None, start_time=None, end_time=None, wdr=None, skip_search=False):
    from dbmind.metadatabase.dao import slow_queries
    root_causes, suggestions = [], []
    if sql is None or database is None:
        return root_causes, suggestions
    # Default interval of searching time is 10S
    default_interval = 120 * 1000
    start_timestamp, end_timestamp = 0, 0
    if start_time is not None:
        start_timestamp = int(start_time)
    if end_time is not None:
        end_timestamp = int(end_time)
    if not skip_search:
        # If it can be found in the meta-database, return it directly.
        field_names = (
            'root_cause', 'suggestion'
        )
        # Maximum number of output lines.
        nb_rows = 3
        result = slow_queries.select_slow_queries(field_names, sql, start_time, end_time, limit=nb_rows)
        for slow_query in result:
            root_causes.append(getattr(slow_query, 'root_cause').split('\n'))
            suggestions.append(getattr(slow_query, 'suggestion').split('\n'))
        if root_causes:
            return root_causes, suggestions
    if not start_timestamp and not end_timestamp:
        end_timestamp = int(datetime.datetime.now().timestamp()) * 1000
        start_timestamp = end_timestamp - default_interval
    elif not start_timestamp:
        start_timestamp = end_timestamp - default_interval
    elif not end_timestamp:
        end_timestamp = start_timestamp + default_interval

    from dbmind.common.types import SlowQuery
    from dbmind.app.diagnosis.query.slow_sql.query_info_source import QueryContextFromTSDB
    from dbmind.app.diagnosis.query.slow_sql.analyzer import SlowSQLAnalyzer

    host, port = get_master_instance_address()
    slow_sql_instance = SlowQuery(db_host=host,
                                  db_port=port,
                                  query=sql,
                                  db_name=database,
                                  schema_name=schema,
                                  start_timestamp=start_timestamp,
                                  duration_time=end_timestamp - start_timestamp
                                  )
    try:
        query_analyzer = SlowSQLAnalyzer()
        query_context = QueryContextFromTSDB
        slow_sql_instance = query_analyzer.run(query_context(slow_sql_instance))
        root_causes.append(slow_sql_instance.root_causes.split('\n'))
        suggestions.append(slow_sql_instance.suggestions.split('\n'))
    except Exception as e:
        logging.exception(e)
        return [], []
    return root_causes, suggestions


def get_metric_statistic():
    return sqlalchemy_query_jsonify(
        dao.statistical_metric.select_knob_statistic_records()
    )


def get_regular_inspections(inspection_type):
    if inspection_type not in ('daily check', 'weekly check', 'monthly check'):
        return
    return sqlalchemy_query_jsonify(
        dao.regular_inspections.select_knob_regular_inspections(inspection_type, limit=1)
    )
