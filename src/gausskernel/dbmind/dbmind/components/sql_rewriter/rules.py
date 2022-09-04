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
import abc
import operator as op_module
from collections import defaultdict
from copy import deepcopy
from itertools import groupby

try:
    from utils import get_table_names, get_columns
except ImportError:
    from .utils import get_table_names, get_columns

OPERATOR = {
    'gt': op_module.gt,
    'gte': op_module.ge,
    'lt': op_module.lt,
    'lte': op_module.le,
    'sub': op_module.sub,
    'div': op_module.truediv,
    'add': op_module.add,
    'mul': op_module.mul
}

OPERATOR_PAIR = {'div': 'mul', 'mul': 'div', 'sub': 'add',
                 'add': 'sub'}

COMPARE_PAIR = {'gt': 'lt', 'gte': 'lte', 'lt': 'gt', 'lte': 'gte', 'eq': 'eq', 'neq': 'neq'}

COMMUTATIVE_OP = {'add', 'mul'}

SIGN_CONVERSION_OP = {'mul', 'div'}


class Rule:
    def __init__(self):
        self.table2columns = dict()
        self.table_exists_primary = dict()

    @abc.abstractmethod
    def check_and_format(self, parsed_sql, table2columns, table_exists_primary) -> str:
        pass


class Delete2Truncate(Rule):
    """It is recommended that the DELETE
    without the WHERE condition be changed to TRUNCATE."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        if len(parsed_sql) == 1 and 'delete' in parsed_sql:
            parsed_sql['truncate'] = parsed_sql.pop('delete')
            return self.__class__.__name__
        return ''


class Star2Columns(Rule):
    """SELECT * type is not advised."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        self.table2columns = table2columns
        star_count = self.find_star(parsed_sql, 0)
        if star_count:
            return self.__class__.__name__
        return ''

    def find_star(self, parsed_sql, star_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                star_count = self.find_star(sub_parsed_sql, star_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_star(list(parsed_sql.values())[0], star_count)
            if parsed_sql.get('select') == '*' or parsed_sql.get('select_distinct') == '*':
                select_values = get_columns(self.table2columns, parsed_sql)
                if select_values:
                    if parsed_sql.get('select'):
                        parsed_sql['select'] = select_values
                    else:
                        parsed_sql['select_distinct'] = select_values
                    star_count += 1
            # For select query in from clause
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                star_count = self.find_star(parsed_sql['from'], star_count)
        return star_count


class Having2Where(Rule):
    """ Having clause is not advised. """
    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        having_count = self.find_having(parsed_sql, 0)
        if having_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def merge_where(where_clause, having_clause):
        if where_clause is None:
            return having_clause
        if 'and' in where_clause:
            where_clause = [where_clause['and']]
        else:
            where_clause = [where_clause]
        if 'and' in having_clause:
            having_clause = [having_clause['and']]
        else:
            having_clause = [having_clause]
        return where_clause + having_clause

    def find_having(self, parsed_sql, having_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                having_count = self.find_having(sub_parsed_sql, having_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                having_count = self.find_having(list(parsed_sql.values())[0], having_count)
            if 'having' in parsed_sql:
                having_count += 1
                parsed_sql['where'] = self.merge_where(parsed_sql.get('where'), parsed_sql['having'])
                parsed_sql.pop('having')
        return having_count


class AlwaysTrue(Rule):
    """Remove useless where clause."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        true_count = self.find_true(parsed_sql, 0)
        if true_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def rm_true_expr(where_clause, index, true_count):
        if isinstance(where_clause[index], (int, float, bool)):
            if (where_clause[index] != 0) and (where_clause[index] is not False):
                where_clause.pop(index)
                true_count += 1
                return true_count
        elif isinstance(where_clause[index], dict):
            if 'eq' in where_clause[index]:
                if isinstance(where_clause[index]['eq'][0], (int, float, bool)) and \
                        isinstance(where_clause[index]['eq'][1], (int, float, bool)):
                    if op_module.eq(where_clause[index]['eq'][0], where_clause[index]['eq'][1]):
                        where_clause.pop(index)
                        true_count += 1
                elif isinstance(where_clause[index]['eq'][0], dict) \
                        and isinstance(where_clause[index]['eq'][1], dict):
                    if 'literal' in where_clause[index]['eq'][0] and \
                            where_clause[index]['eq'][0].get('literal') == where_clause[index]['eq'][1].get('literal'):
                        where_clause.pop(index)
                        true_count += 1
            elif 'neq' in where_clause[index]:
                if isinstance(where_clause[index]['neq'][0], (int, float, bool)) and \
                        isinstance(where_clause[index]['neq'][1], (int, float, bool)):
                    if op_module.ne(where_clause[index]['neq'][0], where_clause[index]['neq'][1]):
                        where_clause.pop(index)
                        true_count += 1
                elif isinstance(where_clause[index]['neq'][0], dict) and isinstance(where_clause[index]['neq'][1],
                                                                                    dict):
                    if 'literal' in where_clause[index]['neq'][0] \
                            and where_clause[index]['neq'][0].get('literal') \
                            != where_clause[index]['neq'][1].get('literal'):
                        where_clause.pop(index)
                        true_count += 1
            elif any(operator in where_clause[index] for operator in OPERATOR) \
                    and isinstance(list(where_clause[index].values())[0][0], (int, float)) \
                    and isinstance(list(where_clause[index].values())[0][1], (int, float)):
                operator = list(where_clause[index].keys())[0]
                res = OPERATOR[operator](where_clause[index][operator][0], where_clause[index][operator][1])
                if res != 0:
                    where_clause.pop(index)
                    true_count += 1
            elif 'and' in where_clause[index]:
                length = len(where_clause[index]['and'])
                for cur_idx in range(length):
                    true_count = AlwaysTrue.rm_true_expr(where_clause[index]['and'], length - 1 - cur_idx, true_count)
                # If all is True, then pop.
                if not where_clause[index]['and']:
                    where_clause.pop(index)
            elif 'or' in where_clause[index]:
                length = len(where_clause[index]['or'])
                for cur_idx in range(length):
                    true_count = AlwaysTrue.rm_true_expr(where_clause[index]['or'], length - 1 - cur_idx, true_count)
                # If exists True, then pop.
                if len(where_clause[index]['or']) < length:
                    where_clause.pop(index)
        return true_count

    def find_true(self, parsed_sql, true_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                true_count = self.find_true(sub_parsed_sql, true_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_true(list(parsed_sql.values())[0], true_count)
            if 'where' in parsed_sql:
                true_count += AlwaysTrue.rm_true_expr(parsed_sql, 'where', 0)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                true_count = self.find_true(parsed_sql['from'], true_count)

        return true_count


class DistinctStar(Rule):
    """Distinct * is not meaningful for primary keys."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        self.table_exists_primary = table_exists_primary
        distinctstar_count = self.find_distinctstar(parsed_sql, 0)
        if distinctstar_count:
            return self.__class__.__name__
        return ''

    def find_distinctstar(self, parsed_sql, distinctstar_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                distinctstar_count = self.find_distinctstar(sub_parsed_sql, distinctstar_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_distinctstar(list(parsed_sql.values())[0], distinctstar_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                distinctstar_count = self.find_distinctstar(parsed_sql['from'], distinctstar_count)
            if parsed_sql.get('select_distinct') == '*':
                table_names = get_table_names(parsed_sql['from'])
                for table_name in table_names:
                    if self.table_exists_primary.get(table_name):
                        parsed_sql['select'] = parsed_sql.pop('select_distinct')
                        distinctstar_count += 1
                        break

        return distinctstar_count


class UnionAll(Rule):
    """Change Union to Union All."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        union_count = self.find_union(parsed_sql, 0)
        if union_count:
            return self.__class__.__name__
        return ''

    def find_union(self, parsed_sql, union_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                union_count = self.find_union(sub_parsed_sql, union_count)
        elif isinstance(parsed_sql, dict):
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                union_count = self.find_union(parsed_sql['from'], union_count)
            if 'union' in parsed_sql:
                union_count += 1
                parsed_sql['union_all'] = parsed_sql['union']
                parsed_sql.pop('union')
            elif isinstance(parsed_sql.get('from'), dict) and 'union' in parsed_sql.get('from'):
                parsed_sql['from']['union_all'] = parsed_sql['from'].pop('union')
        return union_count


class OrderbyConst(Rule):
    """Transform constant in ORDER BY or GROUP BY to column name.
    Example: "select id from test where id=1 order by 1.
    """

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        self.table2columns = table2columns
        orderby_count = self.find_orderbyconst(parsed_sql, 0)
        if orderby_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def replace_const_by_column(parsed_sql, index, columns, checked=False):
        if isinstance(parsed_sql[index], dict) and isinstance(parsed_sql[index].get('value'), int):
            parsed_sql[index]['value'] = columns[parsed_sql[index]['value'] - 1]
            checked = True
            return checked
        if isinstance(parsed_sql[index], list):
            for secondary_index in range(len(parsed_sql[index])):
                checked += OrderbyConst.replace_const_by_column(parsed_sql[index], secondary_index, columns, checked)
        return checked

    def find_orderbyconst(self, parsed_sql, orderby_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                orderby_count = self.find_orderbyconst(sub_parsed_sql, orderby_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_orderbyconst(list(parsed_sql.values())[0], orderby_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                orderby_count = self.find_orderbyconst(parsed_sql['from'], orderby_count)
            if 'select' in parsed_sql:
                select_key = 'select'
            elif 'select_distinct' in parsed_sql:
                select_key = 'select_distinct'
            else:
                return orderby_count
            columns = []
            if not isinstance(parsed_sql[select_key], list):
                parsed_sql[select_key] = [parsed_sql[select_key]]
            for _column in parsed_sql[select_key]:
                if isinstance(_column, dict) and 'value' in _column:
                    columns.append(_column.get('name', _column['value']))
                else:
                    columns = []
                    break
            if not columns:
                return orderby_count
            if 'groupby' in parsed_sql:
                orderby_count += self.replace_const_by_column(parsed_sql, 'groupby', columns)
            if 'orderby' in parsed_sql:
                orderby_count += self.replace_const_by_column(parsed_sql, 'orderby', columns)
            return orderby_count

        return orderby_count


class Or2In(Rule):
    """Transform the OR query with different conditions in the same column to the IN query."""

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary) -> str:
        or_count = self.find_or(parsed_sql, 0)
        if or_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def find_or(parsed_sql, or_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                or_count = Or2In.find_or(sub_parsed_sql, or_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return Or2In.find_or(list(parsed_sql.values())[0], or_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                or_count = Or2In.find_or(parsed_sql['from'], or_count)
            if 'where' in parsed_sql:
                or_count = Or2In.find_or(parsed_sql['where'], or_count)
            elif 'or' in parsed_sql:
                or_count += Or2In.or2in(parsed_sql)
            elif 'and' in parsed_sql:
                or_count = Or2In.find_or(parsed_sql['and'], or_count)
        return or_count

    @staticmethod
    def or2in(parsed_sql):
        column_eq_expr = []
        other_expr = []
        or_count = 0
        for sub_parsed_sql in parsed_sql['or']:
            if isinstance(sub_parsed_sql, dict) and 'eq' in sub_parsed_sql:
                sub_parsed_sql['eq'].sort(key=lambda x: 0 if isinstance(x, str) else 1)
                if isinstance(sub_parsed_sql['eq'][0], str):
                    column_eq_expr.append(sub_parsed_sql['eq'])
                else:
                    other_expr.append(sub_parsed_sql)
            elif isinstance(sub_parsed_sql, dict) and 'and' in sub_parsed_sql:
                or_count = Or2In.find_or(sub_parsed_sql['and'], 0) 
                other_expr.append(sub_parsed_sql)
            else:
                other_expr.append(sub_parsed_sql)
        in_list = []
        for column, group in groupby(sorted(column_eq_expr, key=lambda x: x[0]), key=lambda x: x[0]):
            values = []
            for sub_column, sub_value in group:
                values.append(sub_value)
            in_list.append({'in': [column, values]})
        if not in_list:
            return False if not or_count else True
        if len(in_list) + len(other_expr) == 1:
            parsed_sql['in'] = in_list[0]['in']
            parsed_sql.pop('or')
        else:
            parsed_sql['or'] = in_list + other_expr
        return True


class OrderbyConstColumns(Rule):
    """Delete useless conditions in ORDER BY or GROUP BY.
    Example: "select id from test where id=1 order by id
    """

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        orderby_count = self.find_orderbyconstcolumns(parsed_sql, 0)
        if orderby_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def get_columns(whereclause, columns=None):
        if columns is None:
            columns = []
        if not isinstance(whereclause, dict):
            return []
        if 'and' in whereclause:
            for sub_clause in whereclause['and']:
                OrderbyConstColumns.get_columns(sub_clause, columns)
        elif 'eq' in whereclause:
            whereclause['eq'].sort(key=lambda x: 0 if isinstance(x, str) else 1)
            if isinstance(whereclause['eq'][0], str) and (isinstance(whereclause['eq'][1], (int, float)) or (
                    isinstance(whereclause['eq'][1], dict) and 'literal' in whereclause['eq'][1])):
                columns.append(whereclause['eq'][0])
        return columns

    @staticmethod
    def filter_columns(parsed_sql, index, columns):
        if isinstance(parsed_sql[index], dict):
            if 'value' in parsed_sql[index] and isinstance(parsed_sql[index]['value'], str):
                if parsed_sql[index]['value'] in columns:
                    parsed_sql.pop(index)
                    return True
        elif isinstance(parsed_sql[index], list):
            checked = False
            length = len(parsed_sql[index])
            for secondary_idx in range(length - 1, -1, -1):
                if isinstance(parsed_sql[index][secondary_idx], dict) and isinstance(
                        parsed_sql[index][secondary_idx].get('value'), str):
                    if parsed_sql[index][secondary_idx]['value'] in columns:
                        parsed_sql[index].pop(secondary_idx)
            if len(parsed_sql[index]) != length:
                checked = True
            if not parsed_sql[index]:
                parsed_sql.pop(index)
            return checked
        return False

    def find_orderbyconstcolumns(self, parsed_sql, orderby_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                orderby_count = self.find_orderbyconstcolumns(sub_parsed_sql, orderby_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_orderbyconstcolumns(list(parsed_sql.values())[0], orderby_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                orderby_count = self.find_orderbyconstcolumns(parsed_sql['from'], orderby_count)
            if isinstance(parsed_sql.get('where'), dict):
                columns = OrderbyConstColumns.get_columns(parsed_sql.get('where'))
                if isinstance(parsed_sql.get('orderby'), (dict, list)):
                    orderby_count += self.filter_columns(parsed_sql, 'orderby', columns)
                if isinstance(parsed_sql.get('groupby'), (dict, list)):
                    orderby_count += self.filter_columns(parsed_sql, 'groupby', columns)
        return orderby_count


class ImplicitConversion(Rule):
    """Expression transformation.
    SQL: select * from table1 where col1 + 1 < 2 -> select * from table1 where col1 < 1
    """

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        implicit_count = self.find_implicit(parsed_sql, 0)
        if implicit_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def _exists_int_or_float(left_right):
        """Check if there exists a column with int or float."""
        left, right = left_right
        if isinstance(left, str) and isinstance(right, (int, float)):
            return True
        if isinstance(right, str) and isinstance(left, (int, float)):
            return True
        return False

    @staticmethod
    def _check_implicit(where_clause):
        """Return checked bool, compare_op, operator"""
        checked = False, None, None
        for operator in COMPARE_PAIR.keys():
            if operator in where_clause:
                left, right = where_clause[operator]
                # If right is dict then swap locations and change the compare_operator
                if isinstance(right, dict) and list(right.keys())[0] in OPERATOR_PAIR \
                        and isinstance(left, (int, float)) \
                        and ImplicitConversion._exists_int_or_float(list(right.values())[0]):
                    right, left = left, right
                    compare_op = COMPARE_PAIR[operator]
                    where_clause.pop(operator)
                    # Change to right compare_operator
                    where_clause[compare_op] = [left, right]
                    checked = True, compare_op, list(left.keys())[0]
                elif isinstance(left, dict) \
                        and list(left.keys())[0] in OPERATOR_PAIR \
                        and isinstance(right, (int, float)) \
                        and ImplicitConversion._exists_int_or_float(list(left.values())[0]):
                    checked = True, operator, list(left.keys())[0]
        return checked

    @staticmethod
    def _format_implicit(where_clause, index, implicit_count):
        if isinstance(where_clause[index], dict):
            checked, compare_op, operator = ImplicitConversion._check_implicit(where_clause[index])
            if checked:
                column_index = 0 if isinstance(where_clause[index][compare_op][0][operator][0], str) else 1
                column = where_clause[index][compare_op][0][operator][column_index]
                left_value = where_clause[index][compare_op][0][operator][1 - column_index]
                right_value = where_clause[index][compare_op][1]
                if operator == 'div' and column_index == 0 and (left_value == 0 or left_value == 0.0):
                    raise ZeroDivisionError
                implicit_count += 1
                if operator in COMMUTATIVE_OP or column_index == 0:
                    pair_op = OPERATOR_PAIR[operator]
                else:
                    pair_op = operator
                if operator in SIGN_CONVERSION_OP and left_value < 0:
                    compare_op_result = COMPARE_PAIR[compare_op]
                else:
                    compare_op_result = compare_op
                where_clause[index][compare_op_result] = where_clause[index].pop(compare_op)
                try:
                    right_value = OPERATOR[pair_op](right_value,
                                                    left_value) if operator in COMMUTATIVE_OP or column_index == 0 else \
                        OPERATOR[pair_op](left_value, right_value)
                except ZeroDivisionError:
                    if operator == 'mul':
                        if OPERATOR[compare_op](0, right_value) > 0:
                            where_clause[index] = True
                        else:
                            where_clause[index] = False
                    else:
                        if OPERATOR[compare_op](left_value, 0) > 0:
                            where_clause[index] = True
                        else:
                            where_clause[index] = False
                    return implicit_count
                where_clause[index][compare_op_result] = [column, right_value]
                return implicit_count
            if 'and' in where_clause[index] or 'or' in where_clause[index]:
                if 'and' in where_clause[index]:
                    key = 'and'
                else:
                    key = 'or'
                for subindex, value in enumerate(where_clause[index][key]):
                    implicit_count = ImplicitConversion._format_implicit(where_clause[index][key], subindex,
                                                                         implicit_count)

        return implicit_count

    def find_implicit(self, parsed_sql, implicit_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                implicit_count = self.find_implicit(sub_parsed_sql, implicit_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_implicit(list(parsed_sql.values())[0], implicit_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                implicit_count = self.find_implicit(parsed_sql['from'], implicit_count)
            if 'where' in parsed_sql:
                implicit_count += ImplicitConversion._format_implicit(parsed_sql, 'where', 0)

        return implicit_count


class SelfJoin(Rule):
    """Transform Self join to Union all.
    Original SQL: select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id.
    Rewritten SQL: SELECT * FROM
            (SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b
                 WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20) AND a.c_id > b.c_id
            UNION ALL
            SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b
                WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20 + 1) AND a.c_id - b.c_id <= 20);'
    """

    def check_and_format(self, parsed_sql, table2columns, table_exists_primary):
        self.table2columns = table2columns
        selfjoin_count = self.find_selfjoin(parsed_sql, 0)
        if selfjoin_count:
            return self.__class__.__name__
        return ''

    @staticmethod
    def is_selfjoin(from_clause):
        if isinstance(from_clause, list) and len(from_clause) == 2:
            return 'value' in from_clause[0] and 'value' in from_clause[1] and from_clause[0]['value'] == \
                   from_clause[1]['value']
        return False

    @staticmethod
    def format_gt_lt(cond):
        op_ = list(cond.keys())[0]
        if op_ in {'gte', 'gt', 'lte', 'lt'}:
            left, right = cond[op_]
            # Table columns in the same side like: table1.col1 - table2.col1 < 10 or 10 > table1.col1 - table2.col1
            if (isinstance(right, (int, float)) and isinstance(left, dict) and 'sub' in left) or (
                    isinstance(left, (int, float)) and isinstance(right, dict) and 'sub' in right):
                if [left, right] != sorted([left, right], key=lambda x: 0 if isinstance(x, dict) else 1):
                    left, right = [right, left]
                    op_ = COMPARE_PAIR[op_]
                if (not isinstance(left['sub'][0], str)) or (not isinstance(left['sub'][1], str)):
                    return False, None
            # Table columns not in same side like: table1.col1 < table2.col1 - 1 or table2.col1 - 1 > table1.col1.
            elif (isinstance(right, str) and isinstance(left, dict) and 'sub' in left) or (
                    isinstance(left, str) and isinstance(right, dict) and 'sub' in right):
                if [left, right] != sorted([left, right], key=lambda x: 0 if isinstance(x, dict) else 1):
                    left, right = [right, left]
                    op_ = COMPARE_PAIR[op_]
                if (not isinstance(left['sub'][0], str)) and (not isinstance(left['sub'][1], (int, float))):
                    return False, None
                # Move right side column to the left.
                left['sub'][1], right = right, left['sub'][1]
            # Table columns not in same side like: table1.col1 < table2.col1 + 1 or table2.col1 + 1 > table1.col1
            elif (isinstance(right, str) and isinstance(left, dict) and 'add' in left) or (
                    isinstance(left, str) and isinstance(right, dict) and 'add' in right):
                if [left, right] != sorted([left, right], key=lambda x: 0 if isinstance(x, str) else 1):
                    left, right = [right, left]
                    op_ = COMPARE_PAIR[op_]
                right['add'].sort(key=lambda x: 0 if isinstance(x, str) else 1)
                if (not isinstance(right['add'][0], str)) and (not isinstance(right['add'][1], (int, float))):
                    return False, None
                # Change add to sub like table1.col1 < table2.col1 + 1 -> table1.col1 - table2.col1 < 1
                temp_left = {'sub': [left, right['add'][0]]}
                temp_right = right['add'][1]
                left, right = temp_left, temp_right
            # Table columns compare like table1.col1 < table2.col1 -> table1.col1 - table2.col1 < 0
            elif isinstance(left, str) and isinstance(right, str):
                sub_left = {'sub': [left, right]}
                right = 0
                left = sub_left
            else:
                return False, None
            op_, right = SelfJoin._sort_left_right(op_, left, right)
            # Two different tables
            if '.' in left['sub'][0] and '.' in left['sub'][1] and left['sub'][0].split('.')[0] != \
                    left['sub'][1].split('.')[0]:
                return True, {op_: [left, right]}
        return False, None

    @staticmethod
    def _sort_left_right(op, left, right):
        """Sort the left_column and right_column for consistent order of all conditions."""

        left_column, right_column = left['sub']
        left['sub'].sort()
        if [left_column, right_column] != left['sub']:
            op = COMPARE_PAIR[op]
            right = -right
        return op, right

    @staticmethod
    def _generate_subselect_conditions(conds):
        left_cond, right_cond = conds
        left_op = list(left_cond.keys())[0]
        right_op = list(right_cond.keys())[0]
        left_value = left_cond[left_op][1]
        right_value = right_cond[right_op][1]
        final_right_value = right_value - left_value
        left_column, right_column = left_cond[left_op][0]['sub']
        final_left_cond = {left_op: [left_column, right_column]} if left_value == 0 or left_value == 0.0 else left_cond
        conds1 = [
            {'eq': [{'trunc': {
                'div': [{'add': [left_column] if left_value == 0 or left_value == 0.0 else [left_column, -left_value]},
                        final_right_value]}},
                {'trunc': {'div': [right_column, final_right_value]}}]}, final_left_cond]
        conds2 = [{'eq': [{'trunc': {
            'div': [{'add': [left_column] if left_value == 0 or left_value == 0.0 else [left_column, -left_value]},
                    final_right_value]}},
            {'trunc': {'add': [{'div': [right_column, final_right_value]}, 1]}}]}, right_cond]
        return conds1, conds2

    @staticmethod
    def _check_region_conds(new_conds, indexes):
        """Check if there exists a region column like 1 < table1.col1 - table2.col1 < 10."""

        column_region = defaultdict(dict)
        for cond, index in zip(new_conds, indexes):
            op = list(cond.keys())[0]
            left_column, right_column = cond[op][0]['sub']
            if op in ('lt', 'lte'):
                if ('lt' not in column_region[(left_column, right_column)]) and (
                        not 'lte' in column_region[(left_column, right_column)]):
                    column_region[(left_column, right_column)][op] = (cond, index)
            else:
                if ('gt' not in column_region[(left_column, right_column)]) and (
                        not 'gte' in column_region[(left_column, right_column)]):
                    column_region[(left_column, right_column)][op] = (cond, index)
        # Check the conditions.
        for key, value in column_region.items():
            if len(value.keys()) == 2:
                region_conds = [None, None]
                related_indexes = [None, None]
                left_value = right_value = None
                for cond, index in value.values():
                    op = list(cond.keys())[0]
                    if op in ('lt', 'lte'):
                        right_value = cond[op][1]
                        region_conds[1] = cond
                        related_indexes[1] = index
                    else:
                        left_value = cond[op][1]
                        region_conds[0] = cond
                        related_indexes[0] = index
                if right_value > left_value:
                    return True, region_conds, related_indexes

        return False, None, None

    def selfjoin2unionall(self, parsed_sql):
        if 'and' not in parsed_sql['where']:
            where_clause = deepcopy(parsed_sql['where'])
            parsed_sql['where'] = {}
            parsed_sql['where']['and'] = [where_clause]
        new_conds = []
        indexes = []
        for index, cond in enumerate(parsed_sql['where']['and']):
            if not isinstance(cond, dict):
                continue
            checked, new_cond = SelfJoin.format_gt_lt(cond)
            if checked:
                new_conds.append(new_cond)
                indexes.append(index)
        region_checked, region_conds, related_indexes = SelfJoin._check_region_conds(new_conds, indexes)
        if region_checked:
            select_key = 'select' if 'select' in parsed_sql else 'select_distinct'
            if isinstance(parsed_sql[select_key], dict):
                parsed_sql[select_key] = [parsed_sql[select_key]]
            table_prefix_columns_alias = [column['name'] if 'name' in column else column['value'] for column in
                                          parsed_sql[select_key]]
            table_prefix_columns = [column['value'] for column in parsed_sql[select_key]]
            if 'orderby' in parsed_sql:
                if not self._column_to_index(parsed_sql, table_prefix_columns, table_prefix_columns_alias, 'orderby'):
                    return False
            if 'groupby' in parsed_sql:
                if not self._column_to_index(parsed_sql, table_prefix_columns, table_prefix_columns_alias, 'groupby'):
                    return False
            conds1, conds2 = SelfJoin._generate_subselect_conditions(region_conds)
            where_values = deepcopy(parsed_sql['where']['and'])
            for index in sorted(related_indexes, reverse=True):
                where_values.pop(index)
            where1 = where_values + conds1
            where2 = where_values + conds2
            old_select = deepcopy(parsed_sql[select_key])
            old_from = deepcopy(parsed_sql['from'])
            parsed_sql.pop(select_key)
            parsed_sql['select'] = '*'
            parsed_sql.pop('where')
            left_select = {select_key: old_select, 'from': old_from, 'where': {'and': where1}}
            right_select = {select_key: old_select, 'from': old_from, 'where': {'and': where2}}
            parsed_sql['from'] = {'union_all': [left_select, right_select]}
            return True
        return False

    @staticmethod
    def _column_to_index(parsed_sql, table_prefix_columns, table_prefix_columns_alias, type):
        columns = [column.split('.')[-1] for column in table_prefix_columns]
        if isinstance(parsed_sql[type], dict):
            parsed_sql[type] = [parsed_sql[type]]
        for _column in parsed_sql[type]:
            if isinstance(_column['value'], int):
                continue
            elif isinstance(_column['value'], str):
                index = 0
                if _column['value'] in columns:
                    index = columns.index(_column['value']) + 1
                    _column['value'] = index
                elif _column['value'] in table_prefix_columns_alias:
                    index = table_prefix_columns_alias.index(_column['value']) + 1
                    _column['value'] = index
                elif _column['value'] in table_prefix_columns:
                    index = table_prefix_columns.index(_column['value']) + 1
                    _column['value'] = index
                if index == 0:
                    return False
        return True

    @staticmethod
    def _check_feasibility(parsed_sql):
        required_fields1 = {'select', 'where', 'from'}
        required_fields2 = {'select_distinct', 'where', 'from'}
        useful_fields = {'select', 'where', 'from', 'orderby', 'groupby', 'select_distinct'}
        if not ((required_fields1 - set(parsed_sql.keys())) or required_fields2 - set(parsed_sql.keys())) or (
                set(parsed_sql.keys()) - useful_fields):
            return False
        return True

    def find_selfjoin(self, parsed_sql, selfjoin_count):
        if isinstance(parsed_sql, list):
            for sub_parsed_sql in parsed_sql:
                selfjoin_count = self.find_selfjoin(sub_parsed_sql, selfjoin_count)
        elif isinstance(parsed_sql, dict):
            if 'union' in parsed_sql or 'union_all' in parsed_sql:
                return self.find_selfjoin(list(parsed_sql.values())[0], selfjoin_count)
            if 'from' in parsed_sql:
                if not isinstance(parsed_sql['from'], list):
                    parsed_sql['from'] = [parsed_sql['from']]
                selfjoin_count = self.find_selfjoin(parsed_sql['from'], selfjoin_count)
            if not self._check_feasibility(parsed_sql):
                return selfjoin_count
            if 'from' in parsed_sql:
                if not SelfJoin.is_selfjoin(parsed_sql['from']):
                    return self.find_selfjoin(parsed_sql['from'], selfjoin_count)
            if 'where' in parsed_sql and set(parsed_sql.keys()):
                selfjoin_count += self.selfjoin2unionall(parsed_sql)

        return selfjoin_count
