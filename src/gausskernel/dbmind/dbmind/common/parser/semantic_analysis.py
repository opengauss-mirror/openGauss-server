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

from dataclasses import dataclass, field
from functools import reduce
from typing import Any, List
import operator as op_module
from itertools import groupby
import logging

import intervals as intervals_module
from mo_sql_parsing import parse


@dataclass
class Interval:
    interval: Any
    column: str = None

    def __sub__(self, other):
        if self.column == other.column:
            return Interval(self.interval - other.interval, self.column) if not (
                    self.interval - other.interval).is_empty() else False
        return True

    def __or__(self, other):
        return Interval(self.interval | other.interval, self.column)


def groupby_column(intervals: List[Interval]):
    intervals.sort(key=lambda x: x.column)
    for column, group in groupby(intervals, key=lambda x: x.column):
        yield column, group


@dataclass
class Intervals:
    intervals: List[Interval] = field(default_factory=lambda: [])

    def __and__(self, other):
        results = Intervals()
        all_intervals = self.intervals + other.intervals
        if False in self.intervals:
            intervals = Intervals()
            intervals.append_interval(False)
            return intervals
        for interval in all_intervals[:]:
            if interval is True:
                all_intervals.remove(interval)

        for column, group in groupby_column(all_intervals):
            intervals = list(group)
            cur_interval = intervals[0]
            for next_interval in intervals[1:]:
                cur_interval = Interval(op_module.and_(cur_interval.interval, next_interval.interval), column)
            results.append_interval(cur_interval if not cur_interval.interval.is_empty() else False)
        if False in results.intervals:
            intervals = Intervals()
            intervals.append_interval(False)
            return intervals
        return results

    def append_interval(self, interval):
        self.intervals.append(interval)

    def is_empty(self):
        if False in self.intervals:
            return True
        return False


def _gte(left, right):
    """ Generate intervals for '>=' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, (float, int)):
        interval = Interval(intervals_module.closed(right, intervals_module.inf), left)
        intervals.append_interval(interval)
    elif isinstance(right, str) and isinstance(left, (float, int)):
        interval = Interval(intervals_module.closed(-intervals_module.inf, left), right)
        intervals.append_interval(interval)
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals
    return intervals


def _gt(left, right):
    """ Generate intervals for '>' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, (float, int)):
        interval = Interval(intervals_module.open(right, intervals_module.inf), left)
        intervals.append_interval(interval)
    elif isinstance(right, str) and isinstance(left, (float, int)):
        interval = Interval(intervals_module.open(-intervals_module.inf, left), right)
        intervals.append_interval(interval)
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals
    return intervals


def _lte(left, right):
    """ Generate intervals for '<=' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, (float, int)):
        interval = Interval(intervals_module.closed(-intervals_module.inf, right), left)
        intervals.append_interval(interval)
    elif isinstance(right, str) and isinstance(left, (float, int)):
        interval = Interval(intervals_module.closed(left, intervals_module.inf), right)
        intervals.append_interval(interval)
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals
    return intervals


def _lt(left, right):
    """ Generate intervals for '<' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, (float, int)):
        interval = Interval(intervals_module.open(-intervals_module.inf, right), left)
        intervals.append_interval(interval)
    elif isinstance(right, str) and isinstance(left, (float, int)):
        interval = Interval(intervals_module.open(left, intervals_module.inf), right)
        intervals.append_interval(interval)
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals
    return intervals


def _eq(left, right):
    """ Generate intervals for '=' operator. """
    intervals = Intervals()
    left, right = sorted([left, right], key=lambda x: 0 if isinstance(x, str) else 1)
    if isinstance(left, str) and isinstance(right, (float, int)):
        intervals.append_interval(Interval(intervals_module.singleton(right), left))
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals
    return intervals


def _between(column, left, right):
    """ Generate intervals for 'between' operator. """
    if isinstance(column, str) and isinstance(left, (float, int)) and isinstance(right, (float, int)):
        left_intervals = _gte(column, left)
        right_intervals = _lte(column, right)
        return left_intervals & right_intervals
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals


def _in(left, right):
    """ Generate intervals for 'in' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, list):
        if all(isinstance(x, (float, int)) for x in right):
            interval = _eq(left, right[0]).intervals[0]
            for x in right[1:]:
                interval = interval | _eq(left, x).intervals[0]
            intervals.append_interval(interval)
            return intervals
        else:
            intervals.append_interval(True)
            return intervals
    intervals = Intervals()
    intervals.append_interval(True)
    return intervals


def _nin(left, right):
    """ Generate intervals for 'not in' operator. """
    intervals = Intervals()
    if isinstance(left, str) and isinstance(right, list):
        interval = Interval(intervals_module.open(-intervals_module.inf, intervals_module.inf), left)
        for x in right:
            if isinstance(x, (float, int)):
                interval = interval - _eq(left, x).intervals[0]
            else:
                intervals = Intervals()
                intervals.append_interval(True)
                return intervals
        intervals.append_interval(interval)
    return intervals


def to_intervals(data):
    leaf_op_func = {'gt': _gt, 'gte': _gte, 'lt': _lt, 'lte': _lte, 'eq': _eq, 'in': _in, 'nin': _nin,
                    'between': _between}
    for op in leaf_op_func:
        if op in data:
            return leaf_op_func[op](*data[op])
    if 'and' in data:
        intervals_list = []
        for subdata in data['and']:
            intervals_list.append(to_intervals(subdata))
        intervals = reduce(op_module.and_, intervals_list)
        return intervals
    else:
        intervals = Intervals()
        intervals.append_interval(True)
        return intervals


def combine(iterable, cur_element, total_list):
    """ Append subsequences of elements from iterable to total_list,
    e.g., ['AB', 'CD'] -> [['A','C'], ['A', 'D'], ['B', 'C'], ['B', 'D']].
    """
    if not iterable:
        return total_list
    for subdata in iterable[0]:
        cur_element.append(subdata)
        if len(iterable) == 1:
            total_list.append(cur_element[:])
        combine(iterable[1:], cur_element, total_list)
        cur_element.pop()


def split_parsed_sql(where_clause):
    """Split predicates of WHERE clause, e.g., (a OR b) AND c -> [a AND b, a AND c] """
    if isinstance(where_clause, dict) and 'and' in where_clause:
        total_list = []
        cur_element = []
        combine([split_parsed_sql(subdata) for subdata in where_clause['and']], cur_element, total_list)
        return [{'and': subdata} for subdata in total_list]
    elif isinstance(where_clause, dict) and 'or' in where_clause:
        results = []
        for subdata in where_clause['or']:
            results.extend(split_parsed_sql(subdata))
        return results
    else:
        return [where_clause]


def has_valid_intervals(sql: str):
    try:
        parsed_sql = parse(sql)
    except Exception as e:
        logging.error(e)
        return None
    if 'where' not in parsed_sql:
        return None
    else:
        intervals_list = []
        for _sub_parsed_sql in split_parsed_sql(parsed_sql['where']):
            intervals = to_intervals(_sub_parsed_sql)
            intervals_list.append(intervals)
        return any(not intervals.is_empty() for intervals in intervals_list)
