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
from .enumerations import ALARM_LEVEL


class _Define:
    def __init__(self, category,
                 detail=None,
                 suggestion='',
                 level=ALARM_LEVEL.ERROR):
        self.category = category
        self.detail = detail
        self.level = level
        self.suggestion = suggestion


class RootCause:
    # Format:
    # title = _Define('category', 'default_detail', default_level=INFO)
    # demos:
    SYSTEM_ERROR = _Define('[SYSTEM]', 'System error')
    DISK_SPILL = _Define('[SYSTEM][DISK]', 'Disk already spills')

    # system level
    WORKING_CPU_CONTENTION = _Define('[SYSTEM][CPU]', 'Workloads compete to use CPU resources.')
    NONWORKING_CPU_CONTENTION = _Define('[SYSTEM][CPU]', 'Non-business tasks consume CPU resources.')
    WORKING_IO_CONTENTION = _Define('[SYSTEM][IO]', 'Workloads compete to use IO resources.')
    NONWORKING_IO_CONTENTION = _Define('[SYSTEM][IO]', 'Non-business tasks consume IO resources.')
    WORKING_MEM_CONTENTION = _Define('[SYSTEM][MEMORY]', 'Workloads compete to use memory resources.')
    NONWORKING_MEM_CONTENTION = _Define('[SYSTEM][MEMORY]', 'Non-business tasks consume memory resources.')
    LOCK_CONTENTION = _Define('[SYSTEM][LOCK]', 'Query waits for locks.')
    SMALL_SHARED_BUFFER = _Define('[SYSTEM][BUFFER]', 'shared buffer is small.')
    TABLE_EXPANSION = _Define('[SYSTEM][EXPANSION]', 'too many dirty tuples exist.')
    VACUUM = _Define('[SYSTEM][VACUUM]', 'doing vacuum.')
    BGWRITER_CHECKPOINT = _Define('[SYSTEM][CHECKPOINT]', 'background checkpoint.')
    ANALYZE = _Define('[SYSTEM][ANALYZE]', 'analyzing tables.')
    WALWRITER = _Define('[SYSTEM][WAL]', 'writing WAL.')
    FULL_CONNECTIONS = _Define('[SYSTEM][CONNECTIONS]', 'too many connections.')
    COMPLEX_SLOW_QUERY = _Define('[SYSTEM][SLOWQUERY]', 'slow queries exist.')
    LOW_NETWORK_BANDWIDTH = _Define('[SYSTEM][NETWORK]', 'network is busy.')
    LOW_IO_BANDWIDTH = _Define('[SYSTEM][IO]', 'IO is busy.')
    LOW_CPU_IDLE = _Define('[SYSTEM][IO]', 'CPU is busy.')
    DISK_WILL_SPILL = _Define('[SYSTEM][DISK]', 'Disk will spill.')
    DISK_BURST_INCREASE = _Define('[SYSTEM][DISK]', 'Disk usage suddenly increase.')
    # slow query
    LOCK_CONTENTION_SQL = _Define('[SLOW QUERY][LOCK]',
                                  'There is lock competition during statement execution, and SQL is blocked, '
                                  'detail: {lock_info}.',
                                  'Adjust the business.')
    LARGE_DEAD_RATE = _Define('[SLOW QUERY][TABLE EXPANSION]',
                              'The dead tuples in the related table of the SQL are relatively large, '
                              'which affects the execution performance, '
                              'detail: {large_table}, {dead_rate}.',
                              'Reasonably adjust autovacuum_related parameters to ensure that dead_rate are in '
                              'a reasonable range.')
    LARGE_FETCHED_TUPLES = _Define('[SLOW QUERY][FETCHED TUPLES]',
                                   'The SQL scans a large number of tuples, '
                                   'detail: fetched_tuples({fetched_tuples}), fetched_tuples_rate({fetched_tuples_rate})',
                                   'Check whether the field has an index;'
                                   'Avoid operations such as select count(*);'
                                   'Whether syntax problems cause the statement index to fail, the general '
                                   'index failure cases include: '
                                   '1). The range is too large; 2). There is an implicit conversion; '
                                   '3). Use fuzzy query, etc;')
    LARGE_RETURNED_ROWS = _Define('[SLOW QUERY][FETCHED ROWS]',
                                  'The SQL return a large number of tuples, '
                                  'detail: returned_rows({returned_rows}), returned_rows_rate({returned_rows_rate})',
                                  'Optimize business statements and try to avoid similar operations.')
    SMALL_SHARED_BUFFER_SQL = _Define('[SLOW SQL][SHARED BUFFER]',
                                      'The database shared_buffers parameter setting may be too small, '
                                      'resulting in a small cache hit rate,'
                                      'detail: hit_rate({hit_rate})',
                                      'It is recommended to adjust the shared_buffers parameter reasonably')
    UPDATED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                      'There are redundant indexes in UPDATED related tables,'
                                      'detail: {redundant_index}.',
                                      'Delete irrelevant redundant indexes.')
    LARGE_UPDATED_TUPLES = _Define('[SLOW SQL][UPDATED TUPLES]',
                                   'The UPDATE operation has a large number of update tuples, '
                                   'resulting in slow SQL performance,'
                                   'detail: updated_tuples({updated_tuples}), updated_tuples_rate({updated_tuples_rate})',
                                   'It is recommended to plan business reasonably and perform staggered peaks.')
    INSERTED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                       'There are redundant indexes in INSERTED related tables,'
                                       'detail: {redundant_index}.',
                                       'Delete irrelevant redundant indexes.')
    INSERTED_INDEX_NUMBER = _Define('[SLOW SQL][INDEX NUMBER]',
                                    'INSERT involves too many indexes in the table, '
                                    'which affects insert performance, '
                                    'detail: {index}',
                                    'The more indexes there are, the greater the maintenance cost for insert operations'
                                    ' and the slower the speed of inserting a piece of data. Therefore, design business'
                                    ' indexes reasonably.')
    LARGE_INSERTED_TUPLES = _Define('[SLOW SQL][INSERTED TUPLES]',
                                    'The INSERT operation has a large number of insert tuples, '
                                    'resulting in slow SQL performance,'
                                    'detail: inserted_tuples({inserted_tuples}), inserted_tuples_rate({inserted_tuples_rate})',
                                    'It is recommended to plan business reasonably and perform staggered peaks.')
    DELETED_REDUNDANT_INDEX = _Define('[SLOW SQL][REDUNDANT INDEX]',
                                      'There are redundant indexes in DELETED related tables,'
                                      'detail: {redundant_index}.',
                                      'Delete irrelevant redundant indexes.')
    LARGE_DELETED_TUPLES = _Define('[SLOW SQL][DELETED TUPLES]',
                                   'The INSERT operation has a large number of delete tuples, '
                                   'resulting in slow SQL performance,'
                                   'detail: deleted_tuples({deleted_tuples}), deleted_tuples_rate({deleted_tuples_rate})',
                                   'It is recommended to plan business reasonably and perform staggered peaks.')
    EXTERNAL_SORT = _Define('[SLOW SQL][EXTERNAL SORT]',
                            'External sort is suspected during SQL execution, '
                            'resulting in slow SQL performance, '
                            'detail: {external_sort}',
                            'Reasonably increase the work_mem parameter according to business needs')
    LARGE_TPS = _Define('[SLOW SQL][LOAD]',
                        'During SQL execution, the database load is concentrated, resulting in poor performance,'
                        'detail: tps({tps})',
                        '')
    VACUUM_SQL = _Define('[SLOW SQL][VACUUM]',
                         'During SQL execution, related tables are executing VACUUM tasks, '
                         'resulting in slow queries,'
                         'detail: {autovacuum}',
                         '')
    ANALYZE_SQL = _Define('[SLOW SQL][ANALYZE]',
                          'During SQL execution, related tables are executing ANALYZE tasks, '
                          'resulting in slow queries,'
                          'detail: {autoanalyze}',
                          '')
    SYSTEM_SQL = _Define('[SLOW SQL][SYSTEM]',
                         'During SQL execution, system resources are shortage,'
                         'detail: {system_cause}',
                         '')
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
    # ...
    # Define more root causes *above*.

    @staticmethod
    def has(title):
        return isinstance(title, str) and hasattr(RootCause, title.upper())

    @staticmethod
    def get(title):
        """Generate dynamic ``RootCause`` object."""
        defined = getattr(RootCause, title.upper())
        if not isinstance(defined, _Define):
            raise TypeError('Wrong ROOTCAUSE definition.')
        return RootCause(title.upper(), 1., defined)

    def format(self, *args, **kwargs):
        self.detail = self.detail.format(*args, **kwargs)
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

