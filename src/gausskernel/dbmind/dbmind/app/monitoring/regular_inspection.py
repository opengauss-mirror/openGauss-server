# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import re
from collections import namedtuple, defaultdict

from dbmind import global_vars
from dbmind.common.algorithm.data_statistic import get_statistic_data, box_plot
from dbmind.metadatabase import dao
from dbmind.service import dai
from dbmind.app.monitoring import get_param


CHANGE_THRESHOLD = 0.2
DISK_DEVICE_PATTERN = re.compile(r"device: ([\w/-]+), mountpoint: ([\w/-]+)")
METRIC_ATTR = namedtuple('METRIC_ATTR',
                         ['name', 'category', 'fetch_method', 'related_metric', 'label'])


def _get_root_cause(root_causes):
    if root_causes is None:
        return []
    return re.findall(r'\d{1,2}\. ([A-Z_]+): \(', root_causes)


def _get_query_type(query):
    query = query.upper()
    if 'SELECT' in query:
        return 'SELECT'
    elif 'UPDATE' in query:
        return 'UPDATE'
    elif 'DELETE' in query:
        return 'DELETE'
    elif 'INSERT' in query:
        return 'INSERT'
    else:
        return 'OTHER'


_param_metric_mapper = {'tps_threshold': METRIC_ATTR(name='qps', category='database', fetch_method='one',
                                                     related_metric='gaussdb_qps_by_instance', label=''),
                        'p80_threshold': METRIC_ATTR(name='p80', category='database', fetch_method='one',
                                                     related_metric='statement_responsetime_percentile_p80',
                                                     label=''),
                        'io_capacity_threshold': METRIC_ATTR(name='io_capacity', category='system',
                                                             fetch_method='one',
                                                             related_metric='os_disk_iocapacity ',
                                                             label=''),
                        'io_delay_threshold': METRIC_ATTR(name='io_delay', category='system',
                                                          fetch_method='all',
                                                          related_metric='io_write_delay_time',
                                                          label='device'),
                        'iops_threshold': METRIC_ATTR(name='iops', category='system', fetch_method='one',
                                                      related_metric='os_disk_iops',
                                                      label=''),
                        'replication_write_diff_threshold': METRIC_ATTR(name='replication_write_diff',
                                                                        category='database',
                                                                        fetch_method='all',
                                                                        related_metric='pg_replication_write_diff',
                                                                        label=''),
                        'replication_sent_diff_threshold': METRIC_ATTR(name='replication_sent_diff',
                                                                       category='database',
                                                                       fetch_method='all',
                                                                       related_metric='pg_replication_sent_diff',
                                                                       label=''),
                        'replication_replay_diff_threshold': METRIC_ATTR(name='replication_replay_diff',
                                                                         category='database', fetch_method='all',
                                                                         related_metric='pg_replication_replay_diff',
                                                                         label=''),
                        'data_file_wait_threshold': METRIC_ATTR(name='io_write_delay', category='database',
                                                                fetch_method='one',
                                                                related_metric='gaussdb_data_file_write_time',
                                                                label='')}


class Inspection:
    _metric_list = [
        METRIC_ATTR(name='cpu_usage', category='system', fetch_method='one',
                    related_metric='os_cpu_usage', label=''),
        METRIC_ATTR(name='disk_usage', category='system', fetch_method='all',
                    related_metric='os_disk_usage', label='device'),
        METRIC_ATTR(name='mem_usage', category='system', fetch_method='one',
                    related_metric='os_mem_usage', label=''),
        METRIC_ATTR(name='io_read_delay', category='system', fetch_method='all',
                    related_metric='io_read_delay_time', label='device'),
        METRIC_ATTR(name='io_write_delay', category='system', fetch_method='all',
                    related_metric='io_write_delay_time', label='device'),
        METRIC_ATTR(name='network_receive_drop', category='system', fetch_method='one',
                    related_metric='node_network_receive_drop', label=''),
        METRIC_ATTR(name='network_transmit_drop', category='system', fetch_method='one',
                    related_metric='node_network_transmit_drop', label=''),
        METRIC_ATTR(name='fds', category='system', fetch_method='one',
                    related_metric='node_process_fds_rate', label=''),
        METRIC_ATTR(name='select', category='database', fetch_method='one',
                    related_metric='gs_sql_count_select', label=''),
        METRIC_ATTR(name='delete', category='database', fetch_method='one',
                    related_metric='gs_sql_count_delete', label=''),
        METRIC_ATTR(name='insert', category='database', fetch_method='one',
                    related_metric='gs_sql_count_insert', label=''),
        METRIC_ATTR(name='update', category='database', fetch_method='one',
                    related_metric='gs_sql_count_update', label=''),
        METRIC_ATTR(name='thread_pool', category='database', fetch_method='one',
                    related_metric='thread_occupy', label=''),
        METRIC_ATTR(name='connection_pool', category='database', fetch_method='one',
                    related_metric='gaussdb_connections_used_ratio', label=''),
        METRIC_ATTR(name='qps', category='database', fetch_method='one',
                    related_metric='gaussdb_qps_by_instance', label=''),
        METRIC_ATTR(name='p80', category='database', fetch_method='one',
                    related_metric='statement_responsetime_percentile_p80', label=''),
        METRIC_ATTR(name='p95', category='database', fetch_method='one',
                    related_metric='statement_responsetime_percentile_p95', label='')
    ]

    def __init__(self, host, port, start, end):
        self.host = host
        self.port = port
        self.start = start
        self.end = end
        self.statistic_data = {}
        self.alarm_content_statistic = {}
        self.slow_sql_rca_statistics = {}
        self.slow_sql_db_statistics = {}
        self.slow_sql_query_statistics = {}
        self.detection_interval = global_vars.configs.getint('SELF-MONITORING', 'detection_interval')

    def statistic(self):
        for metric in Inspection._metric_list:
            if metric.category not in ('system', 'database'):
                continue
            server = self.host
            if metric.category == 'database':
                server = "%s:%s" % (self.host, self.port)
            if metric.fetch_method == 'one':
                sequence = dai.get_metric_sequence(metric.related_metric, self.start, self.end).from_server(
                    server).fetchone()
                avg_val, max_val, min_val, the_95th_val = get_statistic_data(sequence.values)
                self.statistic_data[metric.name] = {'average': avg_val, 'minimal': min_val, 'maximum': max_val,
                                                    'the 95th percentage': the_95th_val}
            if metric.fetch_method == 'all':
                sequences = dai.get_metric_sequence(metric.related_metric, self.start, self.end).from_server(
                    server).fetchall()
                for sequence in sequences:
                    distinct_name = "%s(%s)" % (metric.name, sequence.labels[metric.label])
                    avg_val, max_val, min_val, the_95th_val = get_statistic_data(sequence.values)
                    self.statistic_data[distinct_name] = {'average': avg_val, 'minimal': min_val, 'maximum': max_val,
                                                          'the 95th percentage': the_95th_val}

    def alarm(self):

        result = dao.alarms.select_history_alarm(host=self.host,
                                                 start_occurrence_time=dai.datetime_to_timestamp(self.start),
                                                 end_occurrence_time=dai.datetime_to_timestamp(self.end),
                                                 group=True)
        for alarm_ in result:
            alarm_ = list(alarm_)
            alarm_content = alarm_[1]
            count_ = alarm_[-1]
            if alarm_content in self.alarm_content_statistic:
                self.alarm_content_statistic[alarm_content] += count_
            else:
                self.alarm_content_statistic[alarm_content] = count_

    def slow_sql(self):
        """\
        Returns information such as the number of slow SQL at diagnosis time,
        root cause distribution, and slow SQL distribution in each database.
        """
        field_names = ('query', 'db_name', 'root_cause')
        result = dao.slow_queries.select_slow_queries(field_names,
                                                      start_time=dai.datetime_to_timestamp(self.start),
                                                      end_time=dai.datetime_to_timestamp(self.end))
        for slow_query in result:
            query = getattr(slow_query, 'query')
            db_name = getattr(slow_query, 'db_name')
            root_causes = getattr(slow_query, 'root_cause')
            if db_name in self.slow_sql_db_statistics:
                self.slow_sql_db_statistics[db_name] += 1
            else:
                self.slow_sql_db_statistics[db_name] = 1
            query_type = _get_query_type(query)
            if query_type in self.slow_sql_query_statistics:
                self.slow_sql_query_statistics[query_type] += 1
            else:
                self.slow_sql_query_statistics[query_type] = 1
            parse_root_causes = _get_root_cause(root_causes)
            for root_cause_ in parse_root_causes:
                if root_cause_ in self.slow_sql_rca_statistics:
                    self.slow_sql_rca_statistics[root_cause_] += 1
                else:
                    self.slow_sql_rca_statistics[root_cause_] = 1

    def index_recommend(self):
        # Only an interface, not implemented.
        pass

    def conclusion(self):
        RELATED_ALARM_THRESHOLD = 0.2

        alarm_content_statistic = sorted(self.alarm_content_statistic.items(), key=lambda x: x[1], reverse=True)
        alarm_count = sum(self.alarm_content_statistic.values())
        system_resource_related, security_related, performance_related = [], [], []
        disk_related = defaultdict(list)
        disk_related_alarm = ('The disk usage has exceeded the warning level',
                              'The disk usage increased too fast',
                              'The IO_CAPACITY has exceeded the warning level',
                              'The DISK_IO_UTILS has exceeded the warning level',
                              )
        for alarm_content, count_ in alarm_content_statistic:
            if sum(alarm_ in alarm_content for alarm_ in disk_related_alarm):
                result = DISK_DEVICE_PATTERN.findall(alarm_content)
                if result:
                    device = "%s[mp:%s]" % (result[0][0], result[0][1])
                    disk_related[device].append(alarm_content)
            elif 'The cpu usage has exceeded the warning level' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                system_resource_related.append('The CPU usage frequently exceeds the alarm threshold, '
                                               'please consider whether to expand the capacity.\n')
            elif 'The memory usage has exceeded the warning level' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                system_resource_related.append('The Memory usage frequently exceeds the alarm threshold, '
                                               'please consider whether to expand the capacity.\n')
            elif 'The connection usage has exceeded the warning level' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                performance_related.append('The database connection pool occupancy rate frequently exceeds the '
                                           'threshold, please consider modifying the configuration or '
                                           'other processing methods')
            elif 'Find suspicious abnormal brute force logins' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                security_related.append('The database may be operating brute force login, please deal with it in time')
            elif 'Found anomalies for gaussdb_errors_rate' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                security_related.append('The database log reports frequent errors, please deal with them in time')
            elif 'The 80% SQL response time of openGauss has exceeded the warning level' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                performance_related.append('Database performance is degraded, considering the '
                                           'impact of system resources')
            elif 'The usage of handle has exceeded the warning level' in alarm_content and \
                    count_ / alarm_count > RELATED_ALARM_THRESHOLD:
                system_resource_related.append('The handle occupancy rate frequently exceeds the threshold, '
                                               'considering that the program is not released in time')
        for device, alarm_contents in disk_related.items():
            device_alarms = 'device(%s):  ' % device
            for alarm_content in alarm_contents:
                if 'The disk usage has exceeded the warning level' in alarm_content:
                    device_alarms += 'a. The disk usage exceeded the threshold, ' \
                                     'please consider whether to expand the capacity.\n'
                if 'The DISK_IO_UTILS has exceeded the warning level' in alarm_content:
                    device_alarms += 'b. The io_utils exceeded the threshold, ' \
                                     'please consider whether to expand the capacity.'
            system_resource_related.append(device_alarms)
        conclusion = '[SYSTEM RESOURCE]\n'
        for index, content in enumerate(system_resource_related, start=1):
            conclusion += '  %s. %s\n' % (index, content)
        conclusion += '[SECURITY]\n'
        for index, content in enumerate(security_related, start=1):
            conclusion += '  %s. %s' % (index, content)
        conclusion += '[PERFORMANCE]\n'
        for index, content in enumerate(performance_related, start=1):
            conclusion += '  %s. %s' % (index, content)
        slow_sql_count = sum(self.slow_sql_db_statistics.values())

        conclusion += '[SLOW SQL]\n'
        index = 1
        for root_cause_, count_ in self.slow_sql_rca_statistics.items():
            if root_cause_ == 'LACK_STATISTIC_INFO' and \
                    count_ / slow_sql_count > RELATED_ALARM_THRESHOLD:
                conclusion += '  %s. The statistical data is not updated in time, please modify the relevant ' \
                              'configuration or update the statistical information in time\n' % index
                index += 1
            elif root_cause_ == 'LARGE_FETCHED_TUPLES' and \
                    count_ / slow_sql_count > RELATED_ALARM_THRESHOLD:
                conclusion += ' %s. There are frequent large scans, it is recommended to modify the business ' \
                              'interface to avoid such operations\n' % index
                index += 1
            elif root_cause_ == 'SYSTEM_RESOURCE' and \
                    count_ / slow_sql_count > RELATED_ALARM_THRESHOLD:
                conclusion += '  %s. System resources are tight during the business period, consider ' \
                              'capacity expansion\n' % index
                index += 1
        return conclusion

    def inspect(self):
        report = '[METRIC STATISTIC INFO]\n'
        self.statistic()
        self.alarm()
        self.slow_sql()
        for metric, data in self.statistic_data.items():
            statistic = ', '.join(['%s: %s' % (key, value) for key, value in data.items()])
            report += "  %s: %s\n" % (metric.upper(), statistic)
        report += "\n[ALARM STATISTICS INFO]\n"
        for alarm, count in self.alarm_content_statistic.items():
            report += "  ALARM_ CONTENT: %s, COUNT: %s\n" % (alarm, count)
        report += "\n[SLOW SQL INFO]\n"
        report += "\n  [DATABASE DISTRIBUTION]\n"
        for dbname, count in self.slow_sql_db_statistics.items():
            report += "    DATABASE: %s, COUNT: %s\n" % (dbname, count)
        report += "\n  [RCA DISTRIBUTION]\n"
        for root_cause, count in self.slow_sql_rca_statistics.items():
            report += "    ROOT CAUSE: %s, COUNT: %s\n" % (root_cause, count)
        report += "\n  [QUERY DISTRIBUTION]\n"
        report += "    SELECT: %s, UPDATE: %s, DELETE: %s, INSERT: %s, OTHER: %s\n" % (
            self.slow_sql_query_statistics.get('SELECT', 0),
            self.slow_sql_query_statistics.get('UPDATE', 0),
            self.slow_sql_query_statistics.get('DELETE', 0),
            self.slow_sql_query_statistics.get('INSERT', 0),
            self.slow_sql_query_statistics.get('OTHER', 0))
        return report


def update_detection_param(host, port, start, end):
    for param, metric in _param_metric_mapper.items():
        if metric.category not in ('system', 'database'):
            continue
        upper = 0
        original_value = get_param(param)
        server = host
        if metric.category == 'database':
            server = "%s:%s" % (host, port)
        if metric.fetch_method == 'one':
            sequence = dai.get_metric_sequence(metric.related_metric, start, end).from_server(
                server).fetchone()
            upper, _ = box_plot(sequence.values, n=3)
        if metric.fetch_method == 'all':
            sequences = dai.get_metric_sequence(metric.related_metric, start, end).from_server(
                server).fetchall()
            for sequence in sequences:
                upper = max(upper, box_plot(sequence.values, n=3)[0])
        if original_value != 0 and upper != 0 and abs(original_value - upper) / original_value > CHANGE_THRESHOLD:
            global_vars.dynamic_configs.set('detection_params', param, upper)
            logging.info("Update detection parameter '%s' from %s to %s.", param, original_value, upper)

