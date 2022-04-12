"""
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.

This file contains test cases for workload index advisor.

"""

import os
import io
import shlex
import sys
import json
import unittest
from unittest.mock import patch, mock_open
from collections.abc import Iterable
from collections import defaultdict

import index_advisor_workload as iaw
import mcts


def hash_any(obj):
    try:
        return hash(obj)
    except:
        h = 0
        for item in obj:
            h = 31 * h + (hash_any(item) & 255)
        return h


def list_equal(list1, list2):
    def is_iterable(L):
        return isinstance(L, Iterable) and not isinstance(L, str)

    def is_nested_list(L):
        if not is_iterable(L):
            return False
        return len(L) > 0 and is_iterable(L[0])

    assert is_iterable(list1)

    list1_copy = sorted(list1, key=hash_any)
    list2_copy = sorted(list2, key=hash_any)
    for a, b in zip(list1_copy, list2_copy):
        if is_nested_list(a):
            return list_equal(a, b)
        if a != b:
            print("False is ", a, b)
            return False

    return True


class Case:
    expected_sql_frequency = {"select * from student_range_part where credit=1 and stu_id='a_1' and stu_name='b__1'": 1,
                              "select * from student_range_part1 where student_range_part1.stu_id = '12' "
                              "and student_range_part1.stu_name='b__1' and credit=1": 3,
                              "select * from student_range_part where stu_id='w_1'": 1,
                              "select * from student_range_part where stu_name='q__1' and stu_id='w_1'": 1,
                              'select * from student_range_part1 where credit=1': 1}
    sql_content = """select * from student_range_part where credit=1 and stu_id='a_1' and stu_name='b__1';   
select * from student_range_part1 where student_range_part1.stu_id = '12' and student_range_part1.stu_name='b__1' and credit=1;                                                                                          
select * from student_range_part1 where student_range_part1.stu_id = '12' and student_range_part1.stu_name='b__1' and credit=1;                                                                                          
select * from student_range_part1 where student_range_part1.stu_id = '12' and student_range_part1.stu_name='b__1' and credit=1;                                                                                          
select * from student_range_part where stu_id='w_1';
select * from student_range_part where stu_name='q__1' and stu_id='w_1';
select * from student_range_part1 where credit=1;
        """
    config1 = [iaw.IndexItem(tbl='test1', cols='a,b', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='c', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='d', index_type='local'), ]

    config2 = [iaw.IndexItem(tbl='test1', cols='b', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='c', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='d', index_type='local'), ]

    config3 = [iaw.IndexItem(tbl='test1', cols='a,b', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='c', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='d', index_type='local'),
               iaw.IndexItem(tbl='test1', cols='e', index_type='local')]

    config4 = [iaw.IndexItem(tbl='test1', cols='a,b', index_type='local'),
               iaw.IndexItem(tbl='test1', cols='c', index_type='global'),
               iaw.IndexItem(tbl='test1', cols='d', index_type='local'), ]

    class QueryItem:
        pass

    class IndexItem:
        pass

    indexlist = [{'atomic_pos': 0,
                  'benefit': 0,
                  'columns': 'stu_name, stu_id, credit',
                  'delete_sql_num': 0,
                  'index_type': 'local',
                  'ineffective_pos': [],
                  'insert_sql_num': 0,
                  'negative_pos': [],
                  'positive_pos': None,
                  'select_sql_num': 0,
                  'storage': 0,
                  'table': 'public.student_range_part',
                  'total_sql_num': 0,
                  'update_sql_num': 0},
                 {'atomic_pos': 0,
                  'benefit': 0,
                  'columns': 'stu_name, credit, stu_id',
                  'delete_sql_num': 0,
                  'index_type': 'local',
                  'ineffective_pos': [],
                  'insert_sql_num': 0,
                  'negative_pos': [],
                  'positive_pos': None,
                  'select_sql_num': 0,
                  'storage': 0,
                  'table': 'public.student_range_part1',
                  'total_sql_num': 0,
                  'update_sql_num': 0},
                 {'atomic_pos': 0,
                  'benefit': 0,
                  'columns': 'stu_id',
                  'delete_sql_num': 0,
                  'index_type': 'global',
                  'ineffective_pos': [],
                  'insert_sql_num': 0,
                  'negative_pos': [],
                  'positive_pos': None,
                  'select_sql_num': 0,
                  'storage': 0,
                  'table': 'public.student_range_part',
                  'total_sql_num': 0,
                  'update_sql_num': 0},
                 {'atomic_pos': 0,
                  'benefit': 0,
                  'columns': 'stu_id, stu_name',
                  'delete_sql_num': 0,
                  'index_type': 'global',
                  'ineffective_pos': [],
                  'insert_sql_num': 0,
                  'negative_pos': [],
                  'positive_pos': None,
                  'select_sql_num': 0,
                  'storage': 0,
                  'table': 'public.student_range_part',
                  'total_sql_num': 0,
                  'update_sql_num': 0},
                 {'atomic_pos': 3,
                  'benefit': 0,
                  'columns': 'stu_id',
                  'delete_sql_num': 0,
                  'index_type': 'global',
                  'ineffective_pos': [0, 3],
                  'insert_sql_num': 0,
                  'negative_pos': [],
                  'positive_pos': [2],
                  'select_sql_num': 3.0,
                  'storage': 0,
                  'table': 'public.student_range_part',
                  'total_sql_num': 3.0,
                  'update_sql_num': 0}]
    queryitemlist = [{'cost_list': [36.0, 8.27, 36.0, 8.27, 8.27],
                      'frequency': 1.0,
                      'statement': 'select * from student_range_part where credit=1 and '
                      "stu_id='a_1' and stu_name='b__1'"},
                     {'cost_list': [24.81, 24.81, 24.81, 24.81, 24.81],
                      'frequency': 3.0,
                      'statement': 'select * from student_range_part1 where '
                      "student_range_part1.stu_id = '12' and "
                      "student_range_part1.stu_name='b__1' and credit=1", },
                     {'cost_list': [41.0, 41.0, 41.0, 8.27, 8.27],
                      'frequency': 1.0,
                      'statement': "select * from student_range_part where stu_id='w_1'"},
                     {'cost_list': [46.0, 8.27, 46.0, 8.27, 8.27],
                      'frequency': 1.0,
                      'statement': "select * from student_range_part where stu_name='q__1' and "
                      "stu_id='w_1'", },
                     {'cost_list': [8.27, 8.27, 8.27, 8.27, 8.27],
                      'frequency': 1.0,
                      'statement': 'select * from student_range_part1 where credit=1'}]
    costlist = [156.08, 90.61999999999999, 156.08,
                57.889999999999986, 57.889999999999986]
    display_info = {'recommendIndexes': [], 'workloadCount': 7}
    workload = []
    candidate_indexes = []
    candidate_index_list = [{'atomic_pos': 3,
                             'benefit': 0,
                             'columns': 'stu_id',
                             'delete_sql_num': 0,
                             'index_type': 'global',
                             'ineffective_pos': [0, 3],
                             'insert_sql_num': 0,
                             'negative_pos': [],
                             'positive_pos': [2],
                             'select_sql_num': 3.0,
                             'storage': 0,
                             'table': 'public.student_range_part',
                             'total_sql_num': 3.0,
                             'update_sql_num': 0}]
    for index in candidate_index_list:
        candidate_index = IndexItem()
        for attr, value in index.items():
            setattr(candidate_index, attr, value)
        candidate_indexes.append(candidate_index)

    for query, index in zip(queryitemlist, indexlist):
        queryitem = QueryItem()
        indexitem = IndexItem()
        for attr, value in query.items():
            setattr(queryitem, attr, value)
        for attr, value in index.items():
            setattr(indexitem, attr, value)
        queryitem.index_list = [indexitem]
        workload.append(queryitem)


class IndexAdvisorTester(unittest.TestCase):

    def test_mcts(self):
        storage_threshold = 12
        index1 = iaw.IndexItem('public.a', 'col1', index_type='global')
        index2 = iaw.IndexItem('public.b', 'col1', index_type='global')
        index3 = iaw.IndexItem('public.c', 'col1', index_type='global')
        index4 = iaw.IndexItem('public.d', 'col1', index_type='global')

        atomic_index1 = iaw.IndexItem('public.a', 'col1', index_type='global')
        atomic_index2 = iaw.IndexItem('public.b', 'col1', index_type='global')
        atomic_index3 = iaw.IndexItem('public.c', 'col1', index_type='global')
        atomic_index4 = iaw.IndexItem('public.d', 'col1', index_type='global')

        atomic_index1.storage = 10
        atomic_index2.storage = 4
        atomic_index3.storage = 7
        available_choices = [index1, index2, index3, index4]
        atomic_choices = [[], [atomic_index2], [atomic_index1], [atomic_index3],
                          [atomic_index2, atomic_index3], [atomic_index4]]
        query = iaw.QueryItem('select * from gia_01', 1)
        query.cost_list = [10, 7, 5, 9, 4, 11]
        workload_info = [query]

        results = mcts.MCTS(workload_info, atomic_choices, available_choices, storage_threshold, 2)
        self.assertLessEqual([index1.atomic_pos, index2.atomic_pos, index3.atomic_pos], [2, 1, 3])
        self.assertSetEqual({results[0].table, results[1].table}, {'public.b', 'public.c'})

    def test_get_indexable_columns(self):
        tables = 'table1 table2 table2 table3 table3 table3'.split()
        columns = 'col1,col2 col2 col3 col1,col2 col2,col3 col2,col5'.split()
        index_types = 'global local global global local local'.split()
        table_index_dict = defaultdict(list)
        for table, column, index_type in zip(tables, columns, index_types):
            table_index_dict[table].append((column, index_type))
        expected_query_indexable_columns = {'table1': [('col1', 'global'), ('col2', 'global')],
                                            'table2': [('col2', 'local'), ('col3', 'global')],
                                            'table3': [('col1', 'global'), ('col2', 'global'), ('col2', 'local'),
                                                       ('col3', 'local'), ('col5', 'local')]
                                            }
        query_indexable_columns = iaw.get_indexable_columns(table_index_dict)
        self.maxDiff = None
        for table in expected_query_indexable_columns:
            self.assertTrue(list_equal(
                expected_query_indexable_columns[table], query_indexable_columns[table]))

    def test_generate_atomic_config(self):
        queryitem1 = iaw.QueryItem('test', 0)
        queryitem2 = iaw.QueryItem('test', 0)
        queryitem3 = iaw.QueryItem('test', 0)
        queryitem4 = iaw.QueryItem('test', 0)
        queryitem1.valid_index_list = [iaw.IndexItem('table1', 'col1,col2', index_type='local'),
                                       iaw.IndexItem(
                                           'table2', 'col1,col3', index_type='global'),
                                       iaw.IndexItem(
                                           'table3', 'col1,col3', index_type='global')
                                       ]
        queryitem2.valid_index_list = [iaw.IndexItem('table1', 'col1,col2', index_type='local'),
                                       iaw.IndexItem(
                                           'table1', 'col2,col3', index_type='global'),
                                       ]
        queryitem3.valid_index_list = [iaw.IndexItem('table4', 'col1,col2', index_type=''),
                                       iaw.IndexItem(
                                           'table4', 'col3', index_type=''),
                                       ]
        queryitem4.valid_index_list = []
        atomic_config_total = iaw.generate_atomic_config(
            [queryitem1, queryitem2, queryitem3, queryitem4])
        table_combinations_list = []
        cols_combinations_list = []
        index_type_combinations_list = []
        for combinations in atomic_config_total:
            table_combination = []
            cols_combination = []
            index_type_combination = []
            for indexitem in combinations:
                table_combination.append(indexitem.table)
                cols_combination.append(indexitem.columns)
                index_type_combination.append(indexitem.index_type)
            table_combinations_list.append(table_combination)
            cols_combinations_list.append(cols_combination)
            index_type_combinations_list.append(index_type_combination)
        expected_table_comination_list = [[], ['table1'], ['table2'], ['table3'], ['table1', 'table2'],
                                          ['table1', 'table3'], ['table2', 'table3'], [
                                              'table1', 'table2', 'table3'],
                                          ['table1'], ['table1', 'table1'], [
                                              'table4'], ['table4'], ['table4', 'table4']
                                          ]
        expected_cols_combinations_list = [[], ['col1,col2'], ['col1,col3'], ['col1,col3'], ['col1,col2', 'col1,col3'],
                                           ['col1,col2', 'col1,col3'], [
                                               'col1,col3', 'col1,col3'],
                                           ['col1,col2', 'col1,col3', 'col1,col3'], [
                                               'col2,col3'], ['col1,col2', 'col2,col3'],
                                           ['col1,col2'], ['col3'], [
                                               'col1,col2', 'col3']
                                           ]
        expected_index_type_combinations_list = [[], ['local'], ['global'], ['global'], ['local', 'global'], ['local', 'global'],
                                                 ['global', 'global'], [
                                                     'local', 'global', 'global'], ['global'],
                                                 ['local', 'global'], [
                                                     ''], [''], ['', '']
                                                 ]
        self.maxDiff = None
        self.assertTrue(list_equal(table_combinations_list,
                        expected_table_comination_list))
        self.assertTrue(list_equal(cols_combinations_list,
                        expected_cols_combinations_list))
        self.assertTrue(list_equal(index_type_combinations_list,
                        expected_index_type_combinations_list))

    def test_find_subsets_num(self):
        # not contain new index config
        with self.assertRaises(ValueError):
            iaw.find_subsets_num(Case.config1, [[], Case.config1])
        atomic_subsets_num, cur_index_atomic_pos = iaw.find_subsets_num([Case.config1[0]],
                                                                        [[Case.config1[0]], Case.config1, Case.config2, Case.config4])
        expected_atomic_subsets_num = [0]
        expected_cur_index_atomic_pos = 0
        self.assertEqual((expected_atomic_subsets_num, expected_cur_index_atomic_pos),
                         (atomic_subsets_num, cur_index_atomic_pos))
        atomic_subsets_num, cur_index_atomic_pos = iaw.find_subsets_num(Case.config3,
                                                                        [[Case.config3[-1]], Case.config1, Case.config3])
        expected_atomic_subsets_num = [0, 2]
        expected_cur_index_atomic_pos = 0
        self.assertEqual((expected_atomic_subsets_num, expected_cur_index_atomic_pos),
                         (atomic_subsets_num, cur_index_atomic_pos))
        # Case.config3 and Case.config4 containing same index with different index_type
        atomic_subsets_num, cur_index_atomic_pos = \
            iaw.find_subsets_num(
                Case.config3, [[Case.config3[-1]], Case.config1, Case.config3, Case.config4])
        expected_atomic_subsets_num = [0, 2]
        expected_cur_index_atomic_pos = 0
        self.assertEqual((expected_atomic_subsets_num, expected_cur_index_atomic_pos),
                         (atomic_subsets_num, cur_index_atomic_pos))

    def test_is_same_config(self):
        self.assertTrue(iaw.is_same_config(Case.config1, Case.config1))
        self.assertFalse(iaw.is_same_config(Case.config1, Case.config2))
        self.assertFalse(iaw.is_same_config(Case.config1, Case.config3))
        self.assertFalse(iaw.is_same_config(Case.config1, Case.config4))

    def test_load_workload(self):
        with patch('index_advisor_workload.open',  mock_open(read_data=Case.sql_content)) as m:
            workload = iaw.load_workload('testfile')
            sql_frequency = []
            for item in workload:
                sql_frequency.append((item.statement, item.frequency))
        self.maxDiff = True
        self.assertDictEqual(dict(sql_frequency), Case.expected_sql_frequency)

    def test_get_workload_template(self):
        workload = []
        for sql, frequency in Case.expected_sql_frequency.items():
            workload.append(iaw.QueryItem(sql, frequency))
        expected_templates = {'select * from student_range_part where credi@@@ and stu_id@@@ and stu_name@@@':
                              {'cnt': 1, 'samples': ["select * from student_range_part where credit=1 "
                                                     "and stu_id='a_1' and stu_name='b__1'"]},

                              'select * from student_range_part1 where student_range_part1.stu_id =@@@ '
                              'and student_range_part1.stu_name@@@ and credi@@@':
                              {'cnt': 3, 'samples': ["select * from student_range_part1 where "
                                                     "student_range_part1.stu_id = '12' and student_range_part1.stu_name='b__1' and credit=1"]},
                              'select * from student_range_part where stu_id@@@':
                              {'cnt': 1, 'samples': [
                                  "select * from student_range_part where stu_id='w_1'"]},
                              'select * from student_range_part where stu_name@@@ and stu_id@@@':
                              {'cnt': 1, 'samples': ["select * from student_range_part where stu_name='q__1' "
                                                     "and stu_id='w_1'"]},
                              'select * from student_range_part1 where credi@@@':
                              {'cnt': 1, 'samples': ['select * from student_range_part1 where credit=1']}}
        templates = iaw.get_workload_template(workload)
        self.assertDictEqual(templates, expected_templates)

    def test_workload_compression(self):
        with patch('index_advisor_workload.open',  mock_open(read_data=Case.sql_content)) as m:
            compressed_workload, total_num = iaw.workload_compression('test')
        expected_total_num = 7
        self.assertEqual(total_num, expected_total_num)

    def test_parse_plan_cost(self):
        planlist = ['Partition Iterator  (cost=0.00..36.00 rows=1 width=19)',
                    'Index Scan using student_range_part1_credit_tableoid_idx on student_range_part1 '
                    ' (cost=0.00..8.27 rows=1 width=19)',
                    'Index Scan using <142374>btree_global_student_range_part_stu_id on student_range_part'
                    '  (cost=0.00..8.27 rows=1 width=19)',
                    'Partition Iterator  (cost=0.00..36.00 rows=1 width=19)',
                    'Partition Iterator  (cost=0.00..8.27 rows=1 width=19)']
        expected_costlist = [36, 8.27, 8.27, 36.00, 8.27]
        for subplan, expected_cost in zip(planlist, expected_costlist):
            self.assertEqual(
                expected_cost, iaw.GSqlExecute.parse_plan_cost(subplan))

    def test_record_info(self):
        expected_recommend = {'columns': 'stu_id',
                              'deleteRatio': 0.0,
                              'dmlCount': 3,
                              'index_type': 'global',
                              'insertRatio': 0.0,
                              'schemaName': 'public',
                              'selectRatio': 100.0,
                              'sqlDetails': [{'correlationType': 0,
                                              'sql': 'select * from student_range_part where credit=1 and '
                                              "stu_id='a_1' and stu_name='b__1'",
                                              'sqlCount': 2,
                                              'sqlTemplate': 'select * from student_range_part where '
                                              'credit? and stu_id=? and stu_name=?'},
                                             {'correlationType': 0,
                                              'sql': 'select * from student_range_part where '
                                              "stu_name='q__1' and stu_id='w_1'",
                                              'sqlCount': 2,
                                              'sqlTemplate': 'select * from student_range_part where '
                                              'stu_name=? and stu_id=?'},
                                             {'correlationType': 1,
                                              'optimized': '3.958',
                                              'sql': "select * from student_range_part where stu_id='w_1'",
                                              'sqlCount': 1,
                                              'sqlTemplate': 'select * from student_range_part where '
                                              'stu_id=?'}],
                              'statement': 'CREATE INDEX idx_student_range_part_global_stu_id ON '
                              'public.student_range_part(stu_id) global;',
                              'tbName': 'student_range_part',
                              'updateRatio': 0.0,
                              'workloadOptimized': '62.91'}
        sql_info = dict()
        advisor = iaw.IndexAdvisor('db', Case.workload, False)
        advisor.display_detail_info = dict()
        advisor.display_detail_info['recommendIndexes'] = []
        advisor.index_cost_total = Case.costlist
        advisor.record_info(Case.candidate_indexes[0],
                            sql_info, 3, 'student_range_part',
                            'CREATE INDEX idx_student_range_part_global_stu_id ON '
                            'public.student_range_part(stu_id) global;')

        for key, value in advisor.display_detail_info['recommendIndexes'][0].items():
            self.assertEqual(expected_recommend[key], value)

    def test_remote(self):
        if not os.path.exists('remote.json'):
            print("Not found remote.json file so not tested for remote.")
            return
        with open('remote.json') as f:
            config = json.load(f)
            cmd = config['cmd']
            pwd = config['pwd']
            sys.stdin = io.IOBase()
            mock_r, mock_w = os.pipe()
            os.write(mock_w, 'mock text'.encode())
            sys.stdin.fileno = lambda: mock_r
            sys.stdin.readable = lambda: True
            sys.stdin.read = lambda: pwd
            sys.argv[1:] = shlex.split(cmd)
            ret = iaw.main()
            if '--driver' in cmd:
                sys.argv[1:] = shlex.split(cmd.replace('--driver', ''))
            else:
                sys.argv[1:] = shlex.split(cmd + '--driver')
            ret = iaw.main()


if __name__ == '__main__':
    unittest.main()
