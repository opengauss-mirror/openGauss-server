"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
import ast
import time
from collections import Counter

import numpy as np

import config
import global_vars
from utils.plan_parsing import Plan
from utils.sql_parsing import get_indexes
from utils.sql_parsing import sql_parse

MAX_CPU_RATE = 30.0
TIME_SCALE = 600
QPS_GROWN_RATE = 30
PLAN_CAUSE = {
    'Scan': {
        'FuzzyQuery': 'FuzzyQuery: SQL statement uses the keyword "like" causes fuzzy query',
        'IsNotNULL': 'IndexFailure: SQL statement uses the keyword "is not null" causes indexes failure',
        'UnEqual': 'IndexFailure: SQL statement uses the keyword "!=" causes indexes failure',
        'Function': 'IndexFailure: Function operations are used in the WHERE '
                    'clause causes the SQL engine to abandon '
                    ' indexes and use the full table scan',
        'OR': 'NeedIndex: The SQL or condition contains columns which are not '
              'all created indexes',
        'FullScan': 'FullScan: Select all columns from the table, which causes the full scan',
        'Update': 'FullScan: The UPDATE statement updates all columns',
        'ExprInWhere': 'IndexFailure: Expression manipulation of the WHERE '
                       'clause causes the SQL engine to abandon '
                       'the use of indexes in favor of full table scans',
        'NotIn': 'FullScan: SQL statement uses the keyword "IN" or "NOT IN" causes the full scan',
        'RangeTooLarge': 'RangeTooLarge: Condition range is too large',

    },
    'Sort': {
        'ExternalSorting': 'ExternalSorting: The cost of query statement sorting is too high, '
                           'resulting in slow SQL',
    },
    'Join': {
        'NestLoop': 'NestLoop: The slow execution of "NestLoop" operator during JOIN '
                    'operation of a large table, resulting in slow SQL',
    },
    'Aggregate': {
        'SortAggregate': 'SortAggregate: For the aggregate operation of the large result set, '
                         '"Sort Aggregate" operator has poor performance'
    },
    'Redistribute': {
        'DataSkew': 'DataSkew: Data redistribution during the query execution'
    },
    'Insert': {
        'DataRes': 'Database Resources: A large number of data or indexes are involved'
    },
    'Delete': {
        'DataRes': 'Database Resources: A large number of data or indexes are involved'
    }
}

NON_PLAN_CAUSE = {
    'LoadRequestCrowded': 'External Resources: Database request crowded',
    'RedundantIndex': 'External Resources: There are a large number of redundant '
                      'indexes in related columns, resulting in slow insert performance',
    'ResourceShortage': 'External Resources: External processes occupy a large '
                        'number of system resources, resulting in a database resource shortage',
}

PLAN_SUGGESTION = {
    'Scan': {
        'FuzzyQuery': 'Avoid fuzzy queries or do not use full fuzzy queries',
        'IsNotNULL': 'Do not use "is not null", otherwise, the index will be invalid',
        'UnEqual': 'Change the unequal sign to "or"',
        'Function': 'Avoid using function operations in the where clause',
        'OR': 'Create indexes on all related columns',
        'FullScan': 'Please stop this operation if it is not necessary',
        'Update': 'If only one or two columns are changed, do not update all columns, '
                  'otherwise, frequent calls will cause significant performance consumption',
        'ExprInWhere': 'Change the WHERE clause, do not perform expression operations on the columns',
        'NotIn': 'For continuity values, you can use the keyword "between" instead',
        'RangeTooLarge': 'Please reduce query range',

    },
    'Sort': {
        'ExternalSorting': 'Adjust the size of work_mem',
    },
    'Join': {
        'NestLoop': 'Turn off NestLoop by setting the GUC parameter "enable_nestloop" to off, '
                    'and let the optimizer choose other join methods',
    },
    'Aggregate': {
        'SortAggregate': 'By setting the GUC parameter enable_sort to off, let the optimizer '
                         'select the HashAgg operator'
    },
    'Redistribute': {
        'DataSkew': 'It is recommended to use the distribution key recommending tool '
                    'to recommend appropriate distribution keys to avoid data skew'
    },
    'Insert': {
        'DataRes': 'You can use "copy" instead of "insert"'
    },
    'Delete': {
        'DataRes': 'You may launch batch deletions or remove and recreate indexes'
    }
}

NON_PLAN_SUGGESTION = {
    'LoadRequestCrowded': 'It is recommended to change the free time to execute',
    'RedundantIndex': 'Delete the duplicate index before insert',
    'ResourceShortage': 'Stop unnecessary large processes',
}


def is_abnormal(opt, opts, n_sigma=3):
    opts_cost = list(map(lambda o: o.exec_cost, opts))
    mean = np.mean(opts_cost)
    std = np.std(opts_cost)
    three_sigma = mean + n_sigma * std
    if opt.exec_cost >= three_sigma:
        return True
    else:
        return False


def linear_fitting(datas):
    datax = np.arange(1, 1 + len(datas), 1)
    datay = np.array(datas, dtype='float')
    growth_rate, intercept = np.polyfit(datax, datay, deg=1)
    growth_rate = round(growth_rate, 4)
    intercept = round(intercept, 4)
    return growth_rate, intercept


def do_resource_check(dbagent, sql, timestamp, rca, suggestion):
    """
    Get RCA of system resource.
    :param dbagent: obj, interface for sqlite3.
    :param sql: str, query.
    :param timestamp: int, timestamp.
    :param rca: list, store of rca.
    :param suggestion: store of rca's suggestion.
    """
    count = Counter(get_indexes(dbagent, sql, timestamp))
    if len(count):
        if (max(count.values())) > 3:
            rca.append(NON_PLAN_CAUSE['RedundantIndex'])
            suggestion.append(NON_PLAN_SUGGESTION['RedundantIndex'])

    cpu_res = dbagent.fetch_all_result(
        'SELECT process from database_exporter where timestamp == "{timestamp}"'.format(timestamp=timestamp))[0][0]
    cpu_mem_res = ast.literal_eval(cpu_res).values()
    cpu_res = [float(x.split(':')[0]) for x in list(cpu_mem_res)]
    if sum(cpu_res) > MAX_CPU_RATE:
        rca.append(NON_PLAN_CAUSE['ResourceShortage'])
        suggestion.append(NON_PLAN_SUGGESTION['ResourceShortage'])
    source_timer_interval = config.get('agent', 'source_timer_interval')  # 10S -> 300S
    time_value, time_unit = int(source_timer_interval[:-1]) * 12, source_timer_interval[-1]
    qps_list = dbagent.select_timeseries_by_timestamp('database_exporter',
                                                      'qps', str(time_value) + time_unit, timestamp)
    qps = [int(item[1]) for item in qps_list]
    growth_rate, _ = linear_fitting(qps)
    if growth_rate >= QPS_GROWN_RATE:
        rca.append(NON_PLAN_CAUSE['LoadRequestCrowded'])
        suggestion.append(NON_PLAN_SUGGESTION['LoadRequestCrowded'])


def analyze_scan(dbagent, sql_stmt, timestamp, rca, suggestion):
    case = sql_parse(dbagent, sql_stmt, timestamp)
    if case is not None:
        rca.append(PLAN_CAUSE['Scan'][case])
        suggestion.append(PLAN_SUGGESTION['Scan'][case])


def analyze_sort(dbagent, start_time, finish_time, heaviest_opt, rca, suggestion):
    start_time = int(time.mktime(time.strptime(start_time, global_vars.DATE_FORMAT)))
    finish_time = int(time.mktime(time.strptime(finish_time, global_vars.DATE_FORMAT)))
    res = dbagent.fetch_all_result('SELECT temp_file from database_exporter '
                                   'where timestamp between "{start_time}" and "{finish_time}"'
                                   .format(start_time=start_time, finish_time=finish_time))
    res = [item[0] for item in res]
    if heaviest_opt.name == 'Sort' and 't' in res:
        rca.append(PLAN_CAUSE['Sort']['ExternalSorting'])
        suggestion.append(PLAN_SUGGESTION['Sort']['ExternalSorting'])


def analyze_join(heaviest_opt, rca, suggestion):
    if heaviest_opt.name == 'Nested Loop':
        rca.append(PLAN_CAUSE['Join']['NestLoop'])
        suggestion.append(PLAN_SUGGESTION['Join']['NestLoop'])


def analyze_agg(heaviest_opt, rca, suggestion):
    if heaviest_opt.name == 'SortAggregate':
        rca.append(PLAN_CAUSE['Aggregate']['SortAggregate'])
        suggestion.append(PLAN_SUGGESTION['Aggregate']['SortAggregate'])


def diagnose_auto(dbagent, query, start_time):
    """
    Get RCA of system resource.
    :param dbagent: obj, interface for sqlite3.
    :param query: str, query.
    :param start_time: int, start_time.
    :return
    """
    rca = []
    suggestion = []
    plan = Plan()
    timestamp, start_time, finish_time, explain = dbagent.fetch_all_result('select timestamp, start_time, finish_time,'
                                                                           'explain from wdr where start_time == "{start_time}"'
                                                                           .format(start_time=start_time))[0]
    plan.parse(explain)
    operators = plan.sorted_operators
    if not len(operators):
        return []
    heaviest_opt = operators[0]

    for operator in operators:
        if str.startswith(operator.name, 'Vector Streaming(type: REDISTRIBUTE)'):
            rca.append(PLAN_CAUSE['Redistribute']['DataSkew'])
            suggestion.append(PLAN_SUGGESTION['Redistribute']['DataSkew'])
            return [[x_rca, x_sug] for x_rca, x_sug in zip(rca, suggestion)]

    if heaviest_opt.type == 'Scan':
        analyze_scan(dbagent, query, timestamp, rca, suggestion)
    elif heaviest_opt.type == 'Sort':
        analyze_sort(dbagent, start_time, finish_time, heaviest_opt, rca, suggestion)
    elif heaviest_opt.type == 'Other':
        if str.startswith(heaviest_opt.name, 'Update'):
            analyze_scan(dbagent, query, timestamp, rca, suggestion)
        elif str.startswith(heaviest_opt.name, 'Insert'):
            rca.append(PLAN_CAUSE['Insert']['DataRes'])
            suggestion.append(PLAN_SUGGESTION['Insert']['DataRes'])
        elif str.startswith(heaviest_opt.name, 'Delete'):
            rca.append(PLAN_CAUSE['Delete']['DataRes'])
            suggestion.append(PLAN_SUGGESTION['Delete']['DataRes'])
        analyze_join(heaviest_opt, rca, suggestion)
    elif heaviest_opt.type == 'Aggregate':
        analyze_agg(heaviest_opt, rca, suggestion)
    do_resource_check(dbagent, query, timestamp, rca, suggestion)

    zip_rca = [[x_rca, x_sug] for x_rca, x_sug in zip(rca, suggestion)]
    return zip_rca


def diagnose_user(dbagent, query, start_time):
    """
    Get RCA of system resource.
    :param dbagent: obj, interface for sqlite3.
    :param query: str, query.
    :param start_time: int, start_time.
    :return
    """
    rca = []
    suggestion = []
    plan = Plan()
    timestamp, start_time, finish_time, explain = dbagent.fetch_all_result('select timestamp, start_time, finish_time, '
                                                                           ' explain from wdr where start_time == "{start_time}"'
                                                                           .format(start_time=start_time))[0]
    plan.parse(explain)
    operators = plan.sorted_operators
    if not len(operators):
        return []
    heaviest_opt = operators[0]

    for operator in operators:
        if str.startswith(operator.name, 'Vector Streaming(type: REDISTRIBUTE)'):
            rca.append(PLAN_CAUSE['Redistribute']['DataSkew'])
            suggestion.append(PLAN_SUGGESTION['Redistribute']['DataSkew'])
            zip_rca = [[x_rca, x_sug] for x_rca, x_sug in zip(rca, suggestion)]
            zip_rca.insert(0, finish_time)
            zip_rca.insert(0, start_time)
            return zip_rca

    if heaviest_opt.type == 'Scan':
        analyze_scan(dbagent, query, timestamp, rca, suggestion)
    elif heaviest_opt.type == 'Sort':
        analyze_sort(dbagent, start_time, finish_time, heaviest_opt, rca, suggestion)
    elif heaviest_opt.type == 'Other':
        if str.startswith(heaviest_opt.name, 'Update'):
            analyze_scan(dbagent, query, timestamp, rca, suggestion)
        elif str.startswith(heaviest_opt.name, 'Insert'):
            rca.append(PLAN_CAUSE['Insert']['DataRes'])
            suggestion.append(PLAN_SUGGESTION['Insert']['DataRes'])
        elif str.startswith(heaviest_opt.name, 'Delete'):
            rca.append(PLAN_CAUSE['Delete']['DataRes'])
            suggestion.append(PLAN_SUGGESTION['Delete']['DataRes'])
        analyze_join(heaviest_opt, rca, suggestion)

    elif heaviest_opt.type == 'Aggregate':
        analyze_agg(heaviest_opt, rca, suggestion)
    do_resource_check(dbagent, query, timestamp, rca, suggestion)

    zip_rca = [[x_rca, x_sug] for x_rca, x_sug in zip(rca, suggestion)]
    zip_rca.insert(0, finish_time)
    zip_rca.insert(0, start_time)
    return zip_rca

