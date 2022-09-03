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

from collections import defaultdict
from datetime import datetime, timedelta

from dbmind import global_vars
from dbmind.app.optimization._index_recommend_client_driver import RpcExecutor
from dbmind.common.parser.sql_parsing import fill_value, standardize_sql
from dbmind.components.extract_log import get_workload_template
from dbmind.components.index_advisor import index_advisor_workload
from dbmind.service import dai


class TemplateArgs:
    def __init__(self, max_reserved_period, max_template_num):
        self.max_reserved_period = max_reserved_period
        self.max_template_num = max_template_num


def need_recommend_index():
    return True


def get_database_schemas():
    database_schemas = defaultdict(list)
    results = dai.get_latest_metric_value('pg_class_relsize').fetchall()
    for res in results:
        db_name, schema_name = res.labels['datname'], res.labels['nspname']
        if schema_name not in database_schemas[db_name]:
            database_schemas[db_name].append(schema_name)
    return database_schemas


def do_index_recomm(templatization_args, db_name, schemas, database_templates, optimization_interval):
    index_advisor_workload.MAX_INDEX_STORAGE = global_vars.configs.getint('SELF-OPTIMIZATION', 'max_index_storage')
    executor = RpcExecutor(db_name, None, None, None, None, schemas)
    queries = (
        standardize_sql(fill_value(pg_sql_statement_full_count.labels['query']))
        for pg_sql_statement_full_count in (
            dai.get_metric_sequence('pg_sql_statement_full_count',
                                    datetime.now() - timedelta(seconds=optimization_interval),
                                    datetime.now()).fetchall()
        ) if pg_sql_statement_full_count.labels['datname'] == db_name
    )
    get_workload_template(database_templates, queries, templatization_args)
    index_advisor_workload.MAX_INDEX_NUM = global_vars.configs.getint('SELF-OPTIMIZATION', 'max_index_num')
    index_advisor_workload.MAX_INDEX_STORAGE = global_vars.configs.getint('SELF-OPTIMIZATION', 'max_index_storage')
    index_advisor_workload.print = lambda *args, **kwargs: None

    detail_info = index_advisor_workload.index_advisor_workload({'historyIndexes': {}}, executor, database_templates,
                                                                multi_iter_mode=False, show_detail=True)
    detail_info['db_name'] = db_name
    detail_info['host'] = dai.get_latest_metric_value('pg_class_relsize').fetchone().labels['from_instance']
    return detail_info, {db_name: database_templates}
