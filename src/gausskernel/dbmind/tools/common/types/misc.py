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
from .root_cause import RootCause


class SlowQuery:
    def __init__(self, db_host, db_port, db_name, schema_name, query, start_timestamp, duration_time,
                 hit_rate=None, fetch_rate=None, cpu_time=None, data_io_time=None, template_id=None, sort_count=None,
                 sort_mem_used=None, sort_spill_count=None, hash_count=None, hash_mem_used=None, hash_spill_count=None,
                 lock_wait_count=None, lwlock_wait_count=None, n_returned_rows=None, n_tuples_returned=None,
                 n_tuples_fetched=None, n_tuples_inserted=None, n_tuples_updated=None, n_tuples_deleted=None, **kwargs):
        self.db_host = db_host
        self.db_port = db_port
        self.schema_name = schema_name
        self.db_name = db_name
        self.tables_name = None
        self.query = query
        self.start_at = start_timestamp
        self.duration_time = duration_time
        self.hit_rate = hit_rate
        self.fetch_rate = fetch_rate
        self.cpu_time = cpu_time
        self.data_io_time = data_io_time
        self.template_id = template_id
        self.sort_count = sort_count
        self.sort_mem_used = sort_mem_used
        self.sort_spill_count = sort_spill_count
        self.hash_count = hash_count
        self.hash_mem_used = hash_mem_used
        self.hash_spill_count = hash_spill_count
        self.lwlock_wait_count = lwlock_wait_count
        self.lock_wait_count = lock_wait_count
        self.n_returned_rows = n_returned_rows
        self.n_tuples_returned = n_tuples_returned
        self.n_tuples_fetched = n_tuples_fetched
        self.n_tuples_inserted = n_tuples_inserted
        self.n_tuples_updated = n_tuples_updated
        self.n_tuples_deleted = n_tuples_deleted
        self.alarm_cause = list()

        self.kwargs = kwargs

    def add_cause(self, root_cause: RootCause):
        self.alarm_cause.append(root_cause)
        return self

    @property
    def root_causes(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s: (%.2f) %s' % (index, c.title, c.probability, c.detail)
            )
            index += 1
        return '\n'.join(lines)

    @property
    def suggestions(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s' % (index, c.suggestion if c.suggestion else 'No suggestions.')
            )
            index += 1
        return '\n'.join(lines)

