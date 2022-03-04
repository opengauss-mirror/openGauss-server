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

import re


class IndexInfo:
    def __init__(self, schema, table, indexname, columns, indexdef):
        self.schema = schema
        self.table = table
        self.indexname = indexname
        self.columns = columns
        self.indexdef = indexdef
        self.primary_key = False
        self.redundant_obj = []


class ExecuteFactory:
    def __init__(self, dbname, user, password, host, port, schema, multi_node, max_index_storage):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.max_index_storage = max_index_storage
        self.multi_node = multi_node

    @staticmethod
    def record_redundant_indexes(cur_table_indexes, redundant_indexes):
        cur_table_indexes = sorted(cur_table_indexes,
                                   key=lambda index_obj: len(index_obj.columns.split(',')))
        # record redundant indexes
        for pos, index in enumerate(cur_table_indexes[:-1]):
            is_redundant = False
            for candidate_index in cur_table_indexes[pos + 1:]:
                existed_index1 = list(map(str.strip, index.columns.split(',')))
                existed_index2 = list(map(str.strip, candidate_index.columns.split(',')))
                if existed_index1 == existed_index2[0: len(existed_index1)]:
                    is_redundant = True
                    index.redundant_obj.append(candidate_index)
            if is_redundant:
                redundant_indexes.append(index)

    @staticmethod
    def match_table_name(table_name, query_index_dict):
        for elem in query_index_dict.keys():
            item_tmp = '_'.join(elem.split('.'))
            if table_name == item_tmp:
                table_name = elem
                break
            elif 'public_' + table_name == item_tmp:
                table_name = 'public.' + table_name
                break
        else:
            return False, table_name
        return True, table_name

    @staticmethod
    def get_valid_indexes(record, hypoid_table_column, valid_indexes):
        tokens = record.split(' ')
        for token in tokens:
            if 'btree' in token:
                if 'btree_global_' in token:
                    index_type = 'global'
                elif 'btree_local_' in token:
                    index_type = 'local'
                else:
                    index_type = ''
                hypo_index_id = re.search(
                    r'\d+', token.split('_', 1)[0]).group()
                table_columns = hypoid_table_column.get(hypo_index_id)
                if not table_columns:
                    continue
                table_name, columns = table_columns.split(':')
                if table_name not in valid_indexes.keys():
                    valid_indexes[table_name] = []
                if columns not in valid_indexes[table_name]:
                    valid_indexes[table_name].append((columns, index_type))

    @staticmethod
    def record_ineffective_negative_sql(candidate_index, obj, ind):
        cur_table = candidate_index.table
        if cur_table not in obj.statement.lower() and \
                not re.search(r'((\A|[\s\(,])%s[\s\),])' % cur_table.split('.')[-1], obj.statement.lower()):
            return

        if any(re.match(r'(insert\s+into\s+%s\s)' % table, obj.statement.lower())
               for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.insert_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        elif any(re.match(r'(delete\s+from\s+%s\s)' % table, obj.statement.lower())
                 or re.match(r'(delete\s+%s\s)' % table, obj.statement.lower())
                 for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.delete_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        elif any(re.match(r'(update\s+%s\s)' % table, obj.statement.lower())
                 for table in [cur_table, cur_table.split('.')[-1]]):
            candidate_index.update_sql_num += obj.frequency
            # the index column appears in the UPDATE set condition, the statement is negative
            if any(column in obj.statement.lower().split('where ', 1)[0] for column in
                   candidate_index.columns.split(',')):
                candidate_index.negative_pos.append(ind)
            else:
                candidate_index.ineffective_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency
        else:
            candidate_index.select_sql_num += obj.frequency
            # SELECT scenes to filter out positive
            if ind not in candidate_index.positive_pos and \
                    any(re.search(r'\b%s\b' % column, obj.statement.lower())
                        for column in candidate_index.columns.split(', ')):
                candidate_index.ineffective_pos.append(ind)
            candidate_index.total_sql_num += obj.frequency

    @staticmethod
    def match_last_result(table_name, index_column, history_indexes, history_invalid_indexes):
        for column in history_indexes.get(table_name, dict()):
            # if the historical index matches the existed index successfully,
            # then set the historical index to invalid
            history_index_column = list(map(str.strip, column[0].split(',')))
            existed_index_column = list(map(str.strip, index_column[0].split(',')))
            if len(history_index_column) > len(existed_index_column):
                continue
            if history_index_column == existed_index_column[0:len(history_index_column)]:
                history_indexes[table_name].remove(column)
                history_invalid_indexes[table_name] = history_invalid_indexes.get(
                    table_name, list())
                history_invalid_indexes[table_name].append(column)
                if not history_indexes[table_name]:
                    del history_indexes[table_name]

    @staticmethod
    def make_single_advisor_sql(ori_sql):
        sql = 'select gs_index_advise(\''
        for elem in ori_sql:
            if elem == '\'':
                sql += '\''
            sql += elem
        sql += '\');'
        return sql
