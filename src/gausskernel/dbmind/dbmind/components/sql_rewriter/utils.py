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
from collections import OrderedDict

JOIN_KEYWORDS = ['join', 'left join', 'right join', 'inner join', 'full join']


def get_table_names(from_clause, table_names=None):
    if table_names is None:
        table_names = OrderedDict()
    if isinstance(from_clause, str):
        table_names[from_clause] = table_names.get(from_clause, []) + [from_clause]
    elif isinstance(from_clause, dict) and 'value' in from_clause:
        table_names[from_clause['value']] = table_names.get(from_clause['value'], []) + [from_clause['name']]
    elif isinstance(from_clause, dict):
        isjoin = False
        for join_keyword in JOIN_KEYWORDS:
            if join_keyword in from_clause:
                get_table_names(from_clause[join_keyword], table_names)
                isjoin = True
        if not isjoin:
            return dict()
    elif isinstance(from_clause, list):
        for sub_from_clause in from_clause:
            get_table_names(sub_from_clause, table_names)
    return table_names


def get_columns(table2columns, parsed_sql):
    select_values = []
    table_names = get_table_names(parsed_sql['from'])
    for table_name, alias_names in table_names.items():
        columns = table2columns.get(table_name, [])
        for alias_name in alias_names:
            for column in columns:
                select_values.append(
                    {'value': column if len(table_names.keys()) == 1 and all(
                        len(alias_names) == 1 for alias_names in
                        table_names.values()) else alias_name + '.' + column})
    return select_values
