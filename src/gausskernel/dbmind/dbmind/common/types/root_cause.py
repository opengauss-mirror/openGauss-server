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
from dbmind.common.utils import dbmind_assert
from .enums import ALARM_LEVEL


class _Define:
    def __init__(self, category,
                 detail=None,
                 suggestion='',
                 level=ALARM_LEVEL.WARNING):
        self.category = category
        self.detail = detail
        self.level = level
        self.suggestion = suggestion
        self.title = ''


class RootCause:
    SYSTEM_ERROR = _Define('[SYSTEM]', 'System error')
    DISK_SPILL = _Define('[SYSTEM][DISK]', 'Disk already spills')

    # system level
    SYSTEM_ANOMALY = _Define('[SYSTEM][UNKNOWN]', 'Monitoring metric has anomaly.')
    WORKING_CPU_CONTENTION = _Define('[SYSTEM][CPU]', 'Workloads compete to use CPU resources.',
                                     'Reduce CPU intensive concurrent clients.')
    WORKING_IO_CONTENTION = _Define('[SYSTEM][IO]', 'Workloads compete to use IO resources.',
                                    'Reduce IO intensive concurrent clients.')
    LARGE_IO_CAPACITY = _Define('[SYSTEM][IO]', 'Current IO CAPACITY is too large.',
                                'Reduce IO intensives.')
    WORKING_MEM_CONTENTION = _Define('[SYSTEM][MEMORY]', 'Workloads compete to use memory resources.',
                                     'Reduce memory intensive concurrent clients.')
    LOCK_CONTENTION = _Define('[SYSTEM][LOCK]', 'Query waits for locks.',
                              'Reduce insert/update concurrent clients.')
    SMALL_SHARED_BUFFER = _Define('[SYSTEM][BUFFER]', 'shared buffer is small.',
                                  'Tune the shared_buffer parameter larger.')
    TABLE_EXPANSION = _Define('[SYSTEM][EXPANSION]', 'too many dirty tuples exist.',
                              'Vacuum the database.')
    VACUUM = _Define('[SYSTEM][VACUUM]', 'An vacuum operation is being performed during SQL execution.',
                     'Adjust the freqency of vacuum.')
    BGWRITER_CHECKPOINT = _Define('[SYSTEM][CHECKPOINT]', 'background checkpoint.',
                                  'Adjust the frequency of checkpoint.')
    ANALYZE = _Define('[SYSTEM][ANALYZE]', 'An analyze operation is being performed during SQL execution.',
                      'Adjust the frequency of analyze.')
    WALWRITER = _Define('[SYSTEM][WAL]', 'writing WAL.',
                        'Reduce insert/update concurrent clients.')
    FULL_CONNECTIONS = _Define('[SYSTEM][CONNECTIONS]', 'too many connections.',
                               'Reduce number of clients.')
    FAST_CONNECTIONS_INCREASE = _Define('[SYSTEM][CONNECTIONS]', 'connections grows too fast.',
                                        'Mentions the impact of business.')

    COMPLEX_SLOW_QUERY = _Define('[SYSTEM][SLOWQUERY]', 'slow queries exist.',
                                 'Check the slow query diagnosis for more details.')
    REPLICATION_SYNC = _Define('[SYSTEM][REPLICATION]', 'replication delay.',
                               'Repair the states of stand-by servers.')
    LOW_NETWORK_BANDWIDTH = _Define('[SYSTEM][NETWORK]', 'network is busy.')
    LOW_IO_BANDWIDTH = _Define('[SYSTEM][IO]', 'IO is busy.')
    LOW_CPU_IDLE = _Define('[SYSTEM][CPU]', 'CPU is busy.')
    HIGH_CPU_USAGE = _Define('[SYSTEM][CPU]', 'CPU usage is too high.',
                             'Suggestions:\na). Reduce the workloads.\nb). Increase CPU core numbers.')
    DISK_WILL_SPILL = _Define('[SYSTEM][DISK]', 'Disk will spill.', 'Properly expand the disk capacity.')
    DISK_BURST_INCREASE = _Define('[SYSTEM][DISK]', 'Disk usage increase too fast.',
                                  'a) Check for business impact and expand disk capacity.')
    MEMORY_USAGE_BURST_INCREASE = _Define('[SYSTEM][MEMORY]', 'Memory usage grows faster', 'Check for business impact.')
    HIGH_MEMORY_USAGE = _Define('[SYSTEM][MEMORY]', 'Memory usage is too high', 'Check for business impact.')
    QPS_VIOLENT_INCREASE = _Define('[DB][QPS]', 'Database QPS rises sharply.', 'NONE')
    POOR_SQL_PERFORMANCE = _Define('[DB][SQL]', 'Database has poor performance of SQL.', 'NONE')
    # slow query
    LOCK_CONTENTION_SQL = _Define('[SLOW QUERY][LOCK]',
                                  'There is lock competition during statement execution, and SQL is blocked, '
                                  'detail: {lock_info}.',
                                  'Please adjust the business reasonably to avoid lock blocking.')
    LARGE_DEAD_RATE = _Define('[SLOW QUERY][TABLE EXPANSION]',
                              'The dead tuples in the related table of the SQL are relatively large, '
                              'which affects the execution performance, '
                              'detail: table info({large_table}), dead_rate({dead_rate}).',
                              'Perform the analysis operation in time after a large number of '
                              'insert and update operations on the table.')
    LARGE_FETCHED_TUPLES = _Define('[SLOW QUERY][FETCHED TUPLES]',
                                   'The SQL scans a large number of tuples, '
                                   'detail: fetched_tuples({fetched_tuples}), '
                                   'fetched_tuples_rate({fetched_tuples_rate}), '
                                   'returned_rows({returned_rows}), ',
                                   'Check whether the field has an index;'
                                   'Avoid operations such as select count(*);'
                                   'Whether syntax problems cause the statement index to fail, the general '
                                   'index failure cases include: '
                                   '1). The range is too large; '
                                   '2). There is an implicit conversion; '
                                   '3). Use fuzzy query, etc;')
    SMALL_SHARED_BUFFER_SQL = _Define('[SLOW SQL][SHARED BUFFER]',
                                      'The database shared_buffers parameter setting may be too small, '
                                      'resulting in a small cache hit rate,'
                                      'detail: hit_rate({hit_rate})',
                                      'It is recommended to adjust the shared_buffers parameter reasonably')
    UPDATED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                      'There are redundant indexes in UPDATED related tables,'
                                      'detail: {redundant_index}.',
                                      'Deleting redundant indexes can effectively improve the efficiency '
                                      'of update statements.')
    INSERTED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                       'There are redundant indexes in INSERTED related tables,'
                                       'detail: {redundant_index}.',
                                       'Deleting redundant indexes can effectively improve the efficiency '
                                       'of insert statements.')
    DELETED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                      'There are redundant indexes in DELETED related tables,'
                                      'detail: {redundant_index}.',
                                      'Deleting redundant indexes can effectively improve the efficiency '
                                      'of delete statements.')
    LARGE_UPDATED_TUPLES = _Define('[SLOW SQL][UPDATED TUPLES]',
                                   'The UPDATE operation has a large number of update tuples, '
                                   'resulting in slow SQL performance,'
                                   'detail: updated_tuples({updated_tuples}), '
                                   'updated_tuples_rate({updated_tuples_rate})',
                                   'It is recommended to plan the business reasonably and '
                                   'execute it at different peaks.')
    LARGE_INSERTED_TUPLES = _Define('[SLOW SQL][INSERTED TUPLES]',
                                    'The INSERT operation has a large number of insert tuples, '
                                    'resulting in slow SQL performance,'
                                    'detail: inserted_tuples({inserted_tuples}), '
                                    'It is recommended to plan the business reasonably and '
                                    'execute it at different peaks.')
    LARGE_DELETED_TUPLES = _Define('[SLOW SQL][DELETED TUPLES]',
                                   'The INSERT operation has a large number of delete tuples, '
                                   'resulting in slow SQL performance,'
                                   'detail: deleted_tuples({deleted_tuples}), '
                                   'deleted_tuples_rate({deleted_tuples_rate})',
                                   'It is recommended to plan the business reasonably and '
                                   'execute it at different peaks..')
    INSERTED_INDEX_NUMBER = _Define('[SLOW SQL][INDEX NUMBER]',
                                    'INSERT involves too many indexes in the table, '
                                    'which affects insert performance, '
                                    'detail: {index}',
                                    'The more indexes there are, the greater the maintenance cost for insert operations'
                                    ' and the slower the speed of inserting a piece of data. Therefore, design business'
                                    ' indexes reasonably.')
    EXTERNAL_SORT = _Define('[SLOW SQL][EXTERNAL SORT]',
                            'External sort is suspected during SQL execution, '
                            'resulting in slow SQL performance, '
                            'detail: {external_sort}',
                            'Reasonably increase the work_mem parameter according to business needs.')
    VACUUM_CONFLICT = _Define('[SLOW SQL][VACUUM]',
                              'During SQL execution, related tables are executing VACUUM tasks, '
                              'resulting in slow queries,'
                              'detail: {autovacuum}',
                              '')
    ANALYZE_CONFLICT = _Define('[SLOW SQL][ANALYZE]',
                               'During SQL execution, related tables are executing ANALYZE tasks, '
                               'resulting in slow queries,'
                               'detail: {autoanalyze}',
                               '')
    LOAD_CONCENTRATION = _Define('[SLOW SQL][LOAD]',
                                 'During SQL execution, the database load is concentrated, '
                                 'resulting in poor performance,'
                                 'detail: tps({tps})',
                                 'When the database business volume is large, expansion can be considered.')

    SYSTEM_RESOURCE = _Define('[SLOW SQL][SYSTEM]',
                              'During SQL execution, some system resources are shortage,'
                              'detail: {system_cause}',
                              'System resources are tight, check whether preemption occurs and '
                              'consider expanding if necessary')
    ABNORMAL_THREAD_POOL = _Define('[SLOW SQL][THREAD]',
                                   'Abnormal thread pool: {thread_pool}.',
                                   'Adjust the thread pool size according to the business')
    LARGE_CONNECTION_OCCUPY_RATE = _Define('[SLOW SQL][CONNECTION]',
                                           'Database connection pool usage is high: {connection_rate}.',
                                           'Adjust the connection pool size according to the business')
    ABNORMAL_WAIT_EVENT = _Define('[SLOW SQL][DATABASE]',
                                  'Abnormal wait event: {wait_event}',
                                  '')
    LACK_STATISTIC_INFO = _Define('[SLOW SQL][DATABASE]',
                                  'Database statistics are not updated in time, '
                                  'detail: {update_statistics}',
                                  'Perform the analysis operation in time after a large number of '
                                  'insert and update operations on the table.')
    LOW_CHECKPOINT_EFFICIENT = _Define('[SLOW SQL][CHECKPOINT]',
                                       'Checkpoint is less efficient, '
                                       'detail: the backend process writes a large number of buffers, '
                                       'it is {bgwriter_rate} times the write volume of bgwriter and checkpoint.',
                                       '')
    LOW_REPLICATION_EFFICIENT = _Define('[SLOW SQL][REPLICATION]',
                                        'Primary-standby synchronization performance is poor, '
                                        'detail: {replication}.',
                                        '')
    ABNORMAL_SEQSCAN_OPERATOR = _Define('[SLOW SQL][OPERATOR]',
                                        'Abnormal seqscan operator, analysis conclusion: {seqscan}.',
                                        '{seqscan}.')
    ABNORMAL_NESTLOOP_OPERATOR = _Define('[SLOW SQL][OPERATOR]',
                                         'Abnormal nestloop operator.',
                                         '{nestloop}.')
    ABNORMAL_HASHJOIN_OPERATOR = _Define('[SLOW SQL][OPERATOR]',
                                         'Abnormal hashjoin operator.',
                                         '{hashjoin}.')
    ABNORMAL_GROUPAGG_OPERATOR = _Define('[SLOW SQL][OPERATOR]',
                                         'Abnormal groupagg operator.'
                                         '{groupagg}.')
    ABNORMAL_SQL_STRUCTURE = _Define('[SLOW SQL][STRUCTURE]',
                                     'SQL structure needs to be optimized. ',
                                     'rewritten SQL: {rewritten_sql}')
    TIMED_TASK_CONFLICT = _Define('[SLOW SQL][TASK]',
                                  'Timing task conflict with SQL execution'
                                  'detail: {timed_task}')
    ABNORMAL_PLAN_TIME = _Define('[SLOW SQL][PLAN]',
                                 'Plan generation takes too long')
    DATABASE_VIEW = _Define('[SLOW SQL][VIEW]',
                            'Poor performance of database views',
                            'System table query service, no suggestion.')
    ILLEGAL_SQL = _Define('[SLOW SQL][SQL]',
                          'Only support UPDATE, DELETE, INSERT, SELECT',
                          '')
    LACK_INFORMATION = _Define('[SLOW SQL][UNKNOWN]',
                               'Cannot diagnose due to lack of information.',
                               '')
    UNKNOWN = _Define('[SLOW SQL][UNKNOWN]',
                      'UNKNOWN',
                      '')

    # security
    TOO_MANY_ERRORS = _Define(
        '[SECURITY][RISK]',
        'The database has produced too many execution errors in a short period of time,'
        ' a scanning or penetration attack may have occurred.',
        ' Please check whether the access interface exposed to the user is secure and for no vulnerable application.'
    )
    TOO_MANY_INVALID_LOGINS = _Define(
        '[SECURITY][RISK]',
        'Too many invalid logins to the database is a short in a short period of time,'
        ' a brute-force attack may have occurred.',
        ' Please check whether the access interface exposed to the user is secure.'
    )

    # ...
    # Define more root causes *above*.

    @staticmethod
    def has(title):
        return isinstance(title, str) and hasattr(RootCause, title.upper())

    @staticmethod
    def get(title):
        """Generate dynamic ``RootCause`` object."""
        defined = getattr(RootCause, title.upper())
        dbmind_assert(isinstance(defined, _Define))
        return RootCause(title.upper(), 1., defined)

    def format(self, *args, **kwargs):
        self.detail = self.detail.format(*args, **kwargs)
        return self

    def format_suggestion(self, *args, **kwargs):
        self.suggestion = self.suggestion.format(*args, **kwargs)
        return self

    def __init__(self, title, probability, defined):
        self.title = title
        self.probability = probability
        self.category = defined.category
        self.detail = defined.detail
        self.level = defined.level
        self.suggestion = defined.suggestion

    def set_probability(self, probability):
        self.probability = probability
        return self

    def set_detail(self, detail):
        self.detail = detail
        return self

    def set_level(self, level):
        self.level = level
        return self

    def __repr__(self):
        return 'RootCause{title=%s, category=%s, level=%s, detail=%s, prob=%f}' % (
            self.title, self.category, self.level, self.detail, self.probability
        )


# Set the title for each _Define object.
for attr in dir(RootCause):
    if attr.isupper() and not attr.startswith('_'):
        root_cause = getattr(RootCause, attr)
        setattr(root_cause, 'title', attr)
