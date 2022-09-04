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
import logging
import threading
import time
from collections import defaultdict
from datetime import timedelta, datetime

import numpy as np

import dbmind.service.utils
from dbmind import constants
from dbmind import global_vars
from dbmind.app.diagnosis import diagnose_for_alarm_logs
from dbmind.app.diagnosis.query import diagnose_query
from dbmind.app.diagnosis.query.slow_sql.query_info_source import QueryContextFromTSDB
from dbmind.app.healing import (get_repair_toolkit,
                                get_correspondence_repair_methods,
                                HealingAction,
                                get_action_name)
from dbmind.app.monitoring import detect_future, MUST_BE_DETECTED_METRICS
from dbmind.app.monitoring import detect_history, group_sequences_together, regular_inspection
from dbmind.app.optimization import (need_recommend_index,
                                     do_index_recomm,
                                     recommend_knobs,
                                     TemplateArgs,
                                     get_database_schemas)
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.dispatcher import timer
from dbmind.common.types.sequence import EMPTY_SEQUENCE
from dbmind.common.utils import cast_to_int_or_float
from dbmind.service import dai

index_template_args = TemplateArgs(
    global_vars.configs.getint('SELF-OPTIMIZATION', 'max_reserved_period'),
    global_vars.configs.getint('SELF-OPTIMIZATION', 'max_template_num')
)
alarms_need_to_repair = []
alarms_repair_flag = False
alarms_repair_condition = threading.Condition()

detection_interval = global_vars.configs.getint(
    'SELF-MONITORING', 'detection_interval'
)

last_detection_minutes = global_vars.configs.getint(
    'SELF-MONITORING', 'last_detection_time'
) / 60

how_long_to_forecast_minutes = global_vars.configs.getint(
    'SELF-MONITORING', 'forecasting_future_time'
) / 60

result_storage_retention = global_vars.configs.getint(
    'SELF-MONITORING', 'result_storage_retention'
)

optimization_interval = global_vars.configs.getint(
    'SELF-OPTIMIZATION', 'optimization_interval'
)

kill_slow_query = global_vars.configs.getboolean(
    'SELF-OPTIMIZATION', 'kill_slow_query'
)

enable_self_healing = False

# unit: second
updating_statistic_interval = 600
updating_param_interval = 10 * 60
daily_inspection_interval = 24 * 60 * 60
weekly_inspection_interval = 7 * daily_inspection_interval

templates = defaultdict(dict)

"""The Four Golden Signals:
https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals
"""
golden_kpi = set(map(str.strip, global_vars.configs.get('SELF-MONITORING', 'golden_kpi').split(',')))
golden_kpi |= MUST_BE_DETECTED_METRICS.BUILTIN_GOLDEN_KPI

wrapped_golden_kpi = set((kpi,) for kpi in golden_kpi)
to_be_detected_metrics_for_history = wrapped_golden_kpi | MUST_BE_DETECTED_METRICS.HISTORY
to_be_detected_metrics_for_future = wrapped_golden_kpi | MUST_BE_DETECTED_METRICS.future()


@timer(detection_interval)
def self_monitoring_and_diagnosis():
    global alarms_repair_flag

    # Should not monitor while repairing.
    with alarms_repair_condition:
        history_alarms = list()
        # diagnose for time-series data
        if constants.ANOMALY_DETECTION_NAME in global_vars.backend_timed_task:
            end = datetime.now()
            start = end - timedelta(minutes=last_detection_minutes)

            for metrics in to_be_detected_metrics_for_history:
                sequences_list = []
                for metric in metrics:
                    latest_sequences = dai.get_metric_sequence(metric, start, end).fetchall()
                    logging.debug('The length of latest_sequences is %d and metric name is %s.',
                                  len(latest_sequences), metric)

                    sequences_list.append(latest_sequences)

                group_list = group_sequences_together(sequences_list, metrics)

                alarms = global_vars.worker.parallel_execute(
                    detect_history, ((sequences,) for sequences in group_list)
                ) or []

                logging.debug('The length of detected alarms is %d.', len(alarms))
                history_alarms.extend(alarms)

            global_vars.self_driving_records.put(
                {
                    'catalog': 'monitoring',
                    'msg': 'Completed anomaly detection for KPIs and found %d anomalies.' % len(history_alarms),
                    'time': int(time.time() * 1000)
                }
            )

        # diagnose for logs
        if constants.ALARM_LOG_DIAGNOSIS_NAME in global_vars.backend_timed_task:
            alarm_log_collection = dai.get_all_last_monitoring_alarm_logs(last_detection_minutes)
            history_alarms.extend(
                global_vars.worker.parallel_execute(
                    diagnose_for_alarm_logs, alarm_log_collection
                ) or []
            )
            logging.debug('The length of alarm_log_collection is %d.', len(alarm_log_collection))
            global_vars.self_driving_records.put(
                {
                    'catalog': 'monitoring',
                    'msg': 'Completed detection for database logs and found %d anomalies.'
                           % len(alarm_log_collection),
                    'time': int(time.time() * 1000)
                }
            )

        # save history alarms
        for alarms in history_alarms:
            if not alarms:
                continue
            dai.save_history_alarms(alarms)
            alarms_need_to_repair.extend(alarms)

        # diagnose for slow queries
        if constants.SLOW_QUERY_DIAGNOSIS_NAME in global_vars.backend_timed_task:
            slow_query_collection = dai.get_all_slow_queries(last_detection_minutes)
            logging.debug('The length of slow_query_collection is %d.', len(slow_query_collection))
            global_vars.self_driving_records.put(
                {
                    'catalog': 'monitoring',
                    'msg': 'Completed detection for slow queries and diagnosed %d slow queries.'
                           % len(slow_query_collection),
                    'time': int(time.time() * 1000)
                }
            )
            query_context = QueryContextFromTSDB
            slow_queries = global_vars.worker.parallel_execute(
                diagnose_query, ((query_context(slow_query),) for slow_query in slow_query_collection)
            ) or []
            dai.save_slow_queries(slow_queries)
            alarms_need_to_repair.extend(slow_queries)

        alarms_repair_flag = True
        alarms_repair_condition.notify_all()


@timer(optimization_interval)
def self_optimization():
    if constants.KNOB_OPTIMIZATION_NAME in global_vars.backend_timed_task:
        recommend_knobs_result = recommend_knobs()
        dai.save_knob_recomm(recommend_knobs_result)
        global_vars.self_driving_records.put(
            {
                'catalog': 'optimization',
                'msg': 'Completed knob recommendation.',
                'time': int(time.time() * 1000)
            }
        )

    if (constants.INDEX_OPTIMIZATION_NAME in global_vars.backend_timed_task
            and need_recommend_index()):
        database_schemas = get_database_schemas()
        results = global_vars.worker.parallel_execute(
            do_index_recomm,
            ((index_template_args, db_name, ','.join(schemas), templates[db_name], optimization_interval) for
             db_name, schemas in
             database_schemas.items())) or []
        index_infos = []
        for result in results:
            if result is None:
                continue
            index_info, database_templates = result
            if index_info and database_templates:
                index_infos.append(index_info)
                templates.update(database_templates)
        dai.save_index_recomm(index_infos)
        global_vars.self_driving_records.put(
            {
                'catalog': 'optimization',
                'msg': 'Completed index recommendation and generated report.',
                'time': int(time.time() * 1000)
            }
        )


@timer(detection_interval)
def self_healing():
    global alarms_repair_flag

    if not enable_self_healing:
        return

    with alarms_repair_condition:
        alarms_repair_condition.wait_for(predicate=lambda: alarms_repair_flag)

        toolkit_impl = 'om'
        toolkit = get_repair_toolkit(toolkit_impl)
        actions = {}
        for alarm in alarms_need_to_repair:
            # Alarm's type is Alarm or SlowQuery. They both
            # have alarm_cause field.
            if alarm is None:
                continue
            for cause in alarm.alarm_cause:
                methods = get_correspondence_repair_methods(cause.title)
                if not methods:
                    continue

                for method in methods:
                    if method not in actions:
                        func = getattr(toolkit, method)
                        actions[method] = HealingAction(
                            action_name=get_action_name(func),
                            callback=func
                        )
                    actions[method].attach_alarm(alarm)
        nb_success = 0
        nb_failure = 0
        for method_name in actions:
            action = actions[method_name]
            logging.info('Starting to repair the found problem(s) by using %s.', method_name)
            action.perform()
            result = action.result
            if result.success:
                nb_success += 1
            else:
                nb_failure += 1
            dai.save_healing_record(result)

        global_vars.self_driving_records.put(
            {
                'catalog': 'healing',
                'msg': 'Auto fix %d alarm(s) by performing %d action(s) including %d success(es) and %d failure(s).' % (
                    len(alarms_need_to_repair), len(actions), nb_success, nb_failure
                ),
                'time': int(time.time() * 1000)
            }
        )
        alarms_need_to_repair.clear()
        alarms_repair_flag = False


@timer(seconds=10)
def slow_query_killer():
    if not kill_slow_query:
        return

    max_elapsed_time = cast_to_int_or_float(
        global_vars.dynamic_configs.get('slow_sql_threshold', 'max_elapsed_time')
    )
    if max_elapsed_time is None or max_elapsed_time < 0:
        logging.warning("Can not actively kill slow SQL, because the "
                        "configuration value 'max_elapsed_time' is invalid.")
        return
    stmt = """
    SELECT datname AS db_name,
           query,
           pg_cancel_backend(pid) AS killed,
           usename AS username,
           extract(epoch
                   FROM now() - xact_start) AS elapsed_time,
           (extract(epoch from now()) * 1000)::bigint AS killed_time
    FROM pg_stat_activity
    WHERE query_id > 0
      AND query IS NOT NULL
      AND length(trim(query)) > 0
      AND elapsed_time >= {};
    """.format(max_elapsed_time)
    results = global_vars.agent_rpc_client.call('query_in_postgres', stmt)
    if len(results) > 0:
        dai.save_killed_slow_queries(results)
        global_vars.self_driving_records.put(
            {
                'catalog': 'optimization',
                'msg': 'Automatically killed %d slow queries.' % len(results),
                'time': int(time.time() * 1000)
            }
        )


@timer(how_long_to_forecast_minutes * 60)
def forecast_kpi():
    if constants.FORECAST_NAME not in global_vars.backend_timed_task:
        return
    # The general training length is at least three times the forecasting length.
    expansion_factor = 5
    enough_history_minutes = how_long_to_forecast_minutes * expansion_factor
    if enough_history_minutes <= 0:
        logging.error(
            'The value of enough_history_minutes is less than or equal to 0 '
            'and DBMind has ignored it.'
        )
        return

    start = datetime.now() - timedelta(minutes=enough_history_minutes)
    end = datetime.now()
    alarms = list()
    for metrics in to_be_detected_metrics_for_future:
        sequences_list = []
        latest_to_future = {}
        for metric in metrics:
            latest_sequences = dai.get_metric_sequence(metric, start, end).fetchall()
            try:
                metric_value_range = global_vars.metric_value_range_map.get(metric)
                lower, upper = map(float, metric_value_range.split(','))
            except Exception:
                lower, upper = 0, float("inf")

            future_sequences = global_vars.worker.parallel_execute(
                quickly_forecast, ((sequence, how_long_to_forecast_minutes, lower, upper)
                                   for sequence in latest_sequences)
            )

            if future_sequences:
                for i in range(len(latest_sequences)):
                    if future_sequences[i]:
                        latest_to_future[latest_sequences[i]] = future_sequences[i]
                        # Save the forecast future KPIs for users browsing.
                        metric = latest_sequences[i].name
                        host = dbmind.service.utils.SequenceUtils.from_server(latest_sequences[i])
                        dai.save_forecast_sequence(metric, host, future_sequences[i])
                    else:
                        latest_to_future[latest_sequences[i]] = EMPTY_SEQUENCE

            sequences_list.append(latest_sequences)

        group_list = group_sequences_together(sequences_list, metrics)

        detect_materials = list()
        for latest_sequences in group_list:
            future_sequences = []
            for latest_sequence in latest_sequences:
                if latest_to_future.get(latest_sequence):
                    future_sequences.append(latest_to_future.get(latest_sequence))
                else:
                    future_sequences.append(EMPTY_SEQUENCE)

            host = dbmind.service.utils.SequenceUtils.from_server(latest_sequences[0])
            detect_materials.append((host, metrics, latest_sequences, future_sequences))

        alarms.extend(global_vars.worker.parallel_execute(detect_future, detect_materials) or [])

    global_vars.self_driving_records.put(
        {
            'catalog': 'monitoring',
            'msg': 'Completed forecast for KPIs and found %d anomalies in future.'
                   % len(alarms),
            'time': int(time.time() * 1000)
        }
    )
    for alarm in alarms:
        dai.save_future_alarms(alarm)


@timer(max(result_storage_retention // 10, 60))
def discard_expired_results():
    """Periodic cleanup of not useful diagnostics or predictions"""
    logging.info('Starting to clean up older diagnostics and predictions.')
    try:
        dai.delete_older_result(int(time.time()), result_storage_retention)
        global_vars.self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Automatically clean up discarded diagnosis results.',
                'time': int(time.time() * 1000)
            }
        )
    except Exception as e:
        logging.exception(e)
        global_vars.self_driving_records.put(
            {
                'catalog': 'vacuum',
                'msg': 'Failed to clean up discarded diagnosis results due to %s.' % e,
                'time': int(time.time() * 1000)
            }
        )


statistical_metrics = {}


@timer(seconds=updating_statistic_interval)
def update_statistical_metrics():
    logging.info('Starting to save statistic value of key metrics.')
    end = datetime.now()
    start = end - timedelta(seconds=updating_statistic_interval)  # Polish later: check more.
    results = []
    handled_metrics = []
    for metric in golden_kpi:
        if metric in handled_metrics:
            continue
        handled_metrics.append(metric)
        latest_sequences = dai.get_metric_sequence(metric, start, end).fetchall()
        logging.debug('The length of latest_sequences is %d and metric name is %s.',
                      len(latest_sequences), metric)
        for sequence in latest_sequences:
            if not sequence.values:
                continue
            host = dbmind.service.utils.SequenceUtils.from_server(sequence)
            date = int(time.time() * 1000)
            metric_name = metric
            unique_metric_name = '%s-%s' % (metric, host)
            if 'mountpoint' in sequence.labels:
                unique_metric_name += sequence.labels['mountpoint']
                metric_name = '%s(%s)' % (metric_name, sequence.labels['mountpoint'])
            if unique_metric_name not in statistical_metrics:
                statistical_metrics[unique_metric_name] = {'avg_val': {}}
            prev_length = statistical_metrics[unique_metric_name]['avg_val'].get('length', 0)
            prev_sum_value = statistical_metrics[unique_metric_name]['avg_val'].get('value', 0) * prev_length
            avg_val = \
                round((prev_sum_value + sum(sequence.values)) / (prev_length + len(sequence.values)), 4)
            statistical_metrics[unique_metric_name]['avg_val'] = {'value': avg_val,
                                                                  'length': prev_length + len(
                                                                      sequence.values)}
            min_val = round(min(min(sequence.values), statistical_metrics[unique_metric_name].get('min_val', 0)), 4)
            max_val = round(max(max(sequence.values), statistical_metrics[unique_metric_name].get('max_val', 0)), 4)
            the_95_quantile = round(np.nanpercentile(sequence.values, 95), 4)
            results.append({'metric_name': metric_name,
                            'host': host,
                            'date': date,
                            'avg_val': avg_val,
                            'min_val': min_val,
                            'max_val': max_val,
                            'the_95_quantile': the_95_quantile})
    if results:
        dai.save_statistical_metric_records(results)
    global_vars.self_driving_records.put(
        {
            'catalog': 'monitoring',
            'msg': 'Updated statistical metrics.',
            'time': int(time.time() * 1000)
        }
    )


@timer(seconds=daily_inspection_interval)
def daily_inspection():
    logging.info('Starting to inspect.')
    results = []
    host, port = dbmind.service.utils.get_master_instance_address()
    end = datetime.now()
    start = end - timedelta(seconds=daily_inspection_interval)

    regular_inspector = regular_inspection.Inspection(host=host, port=port, start=start, end=end)
    report = regular_inspector.inspect()
    conclusion = regular_inspector.conclusion()
    results.append({'inspection_type': 'daily check',
                    'start': int(start.timestamp() * 1000),
                    'end': int(end.timestamp()) * 1000,
                    'report': report,
                    'conclusion': conclusion})
    dai.save_regular_inspection_results(results)
    global_vars.self_driving_records.put(
        {
            'catalog': 'diagnosis',
            'msg': 'Updated daily inspection report.',
            'time': int(time.time() * 1000)
        }
    )


@timer(seconds=updating_param_interval)
def update_detection_param():
    logging.info('Start to update detection params.')
    host, port = dbmind.service.utils.get_master_instance_address()
    end = datetime.now()
    start = end - timedelta(seconds=daily_inspection_interval)
    regular_inspection.update_detection_param(host, port, start, end)
