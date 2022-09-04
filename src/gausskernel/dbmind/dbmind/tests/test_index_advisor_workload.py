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

import unittest
from collections import defaultdict
from unittest.mock import patch, mock_open
from collections.abc import Iterable
import os
import sys
import json
import io
import shlex

import dbmind.components.index_advisor.utils
from dbmind.components.index_advisor.sql_output_parser import parse_table_sql_results, get_checked_indexes, \
    parse_single_advisor_results, parse_existing_indexes_results, parse_explain_plan, ExistingIndex, IndexItemFactory
from dbmind.components.index_advisor.sql_generator import get_existing_index_sql, get_index_check_sqls, get_single_advisor_sql, \
    get_workload_cost_sqls
from dbmind.components.index_advisor import index_advisor_workload
from dbmind.components.index_advisor.index_advisor_workload import generate_sorted_atomic_config, add_more_column_index, \
    filter_redundant_indexes_with_same_type
from dbmind.components.index_advisor.utils import WorkLoad, QueryItem
from dbmind.components.index_advisor import mcts


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
    config1 = (IndexItemFactory().get_index('test1', 'a,b', index_type='global'),
               IndexItemFactory().get_index('test1', 'c', index_type='global'),
               IndexItemFactory().get_index('test1', 'd', index_type='local'),)

    config2 = (IndexItemFactory().get_index('test1', 'b', index_type='global'),
               IndexItemFactory().get_index('test1', 'c', index_type='global'),
               IndexItemFactory().get_index('test1', 'd', index_type='local'),)

    config3 = (IndexItemFactory().get_index('test1', 'a,b', index_type='global'),
               IndexItemFactory().get_index('test1', 'c', index_type='global'),
               IndexItemFactory().get_index('test1', 'd', index_type='local'),
               IndexItemFactory().get_index('test1', 'e', index_type='local'))

    config4 = (IndexItemFactory().get_index('test1', 'a,b', index_type='local'),
               IndexItemFactory().get_index('test1', 'c', index_type='global'),
               IndexItemFactory().get_index('test1', 'd', index_type='local'),)

    index1 = IndexItemFactory().get_index("public.store_sales", "ss_item_sk, ss_sold_date_sk", "")
    index2 = IndexItemFactory().get_index("public.item", "i_manufact_id", "global")

    indexes = [IndexItemFactory().get_index("public.item", "i_manufact_id, i_brand_id", "global"),
               IndexItemFactory().get_index("public.store_sales", "ss_item_sk", ""),
               IndexItemFactory().get_index("public.item", "i_manufact_id", "global"),
               IndexItemFactory().get_index("public.item", "i_manufact_id", "local"),
               IndexItemFactory().get_index("public.store_sales", "ss_item_sk, ss_sold_date_sk", ""),
               IndexItemFactory().get_index("public.store_sales", "ss_sold_date_sk", "")
               ]
    benefits = [1000, 1000, 2000, 200, 2000, 1000]
    for index, benefit in zip(indexes, benefits):
        index.benefit = benefit

    @staticmethod
    def get_index_advisor():
        queries = [QueryItem('select * from store_sales ;select * from item ', 1) for _ in range(9)]
        workload = WorkLoad(queries)
        workload.add_indexes(None, [19933.29, 266350.65, 92390.78, 1734959.92, 0.0, 105765.53, 114289.41, 131445.85,
                                    1445666.07], set())
        workload.add_indexes((Case.index1,),
                             [19933.29, 266350.65, 14841.35, 1734959.92, 0.0, 105765.53, 114289.41, 131445.85,
                              1445666.07], {'<171278>btree_store_sales_ss_item_sk_ss_sold_date_sk'})
        workload.add_indexes((Case.index2,),
                             [19933.29, 266350.65, 90653.52, 1734959.92, 0.0, 105765.53, 114289.41, 131445.85,
                              1445666.07], {'<171324>btree_global_item_i_manufact_id'})
        index_advisor = index_advisor_workload.IndexAdvisor(executor=None, workload=workload, multi_iter_mode=False)
        index_advisor.display_detail_info['recommendIndexes'] = []
        return index_advisor


class SqlOutPutParserTester(unittest.TestCase):

    def test_parse_table_sql_results(self):
        ori_inputs = ['SELECT 25', 'dbgen_version', 'customer_address', 'customer_demographics', 'date_dim', 'warehouse',
                  'ship_mode', 'time_dim', 'reason', 'income_band', 'item', 'store', 'customer', 'web_site',
                  'store_returns', 'household_demographics', 'web_page', 'promotion', 'catalog_page', 'inventory',
                  'catalog_returns', 'web_returns', 'web_sales', 'catalog_sales', 'store_sales', 'temptable']
        inputs = [(input,) for input in ori_inputs]
        expected = ori_inputs[1:]
        self.assertEqual(parse_table_sql_results(inputs), expected)

    def test_parse_explain_plan(self):
        query_number = 6
        ori_explain_results = ['SELECT 1', '(99920,<99920>btree_web_sales_ws_item_sk)',
                           'EXPLAIN',
                           'Limit  (cost=19932.04..19933.29 rows=100 width=17)', '  CTE customer_total_return',
                           '    ->  HashAggregate  (cost=11966.66..12475.61 rows=50895 width=46)',
                           '          Group By Key: store_returns.sr_customer_sk, store_returns.sr_store_sk',
                           '          ->  Hash Join  (cost=2322.68..11584.94 rows=50895 width=14)',
                           '                Hash Cond: (store_returns.sr_returned_date_sk = date_dim.d_date_sk)',
                           '                ->  Seq Scan on store_returns  (cost=0.00..7675.14 rows=287514 width=18)',
                           '                ->  Hash  (cost=2318.11..2318.11 rows=365 width=4)',
                           '                      ->  Partition Iterator  (cost=0.00..2318.11 rows=365 width=4)',
                           '                            Iterations: 6',
                           '                            ->  Partitioned Seq Scan on date_dim  (cost=0.00..2318.11 rows=365 width=4)',
                           '                                  Filter: (d_year = 2000)',
                           '                                  Selected Partitions:  1..6',
                           '  ->  Sort  (cost=7456.43..7458.98 rows=1018 width=17)',
                           '        Sort Key: customer.c_customer_id',
                           '        ->  Hash Join  (cost=2659.39..7405.58 rows=1018 width=17)',
                           '              Hash Cond: (customer.c_customer_sk = ctr1.ctr_customer_sk)',
                           '              ->  Partition Iterator  (cost=0.00..3861.00 rows=100000 width=21)',
                           '                    Iterations: 16',
                           '                    ->  Partitioned Seq Scan on customer  (cost=0.00..3861.00 rows=100000 width=21)',
                           '                          Selected Partitions:  1..16',
                           '              ->  Hash  (cost=2646.67..2646.67 rows=1018 width=4)',
                           '                    ->  Hash Join  (cost=1409.91..2646.67 rows=1018 width=4)',
                           '                          Hash Cond: (ctr1.ctr_store_sk = subquery."?column?")',
                           '                          Join Filter: (ctr1.ctr_total_return > (subquery."?column?" * 1.2))',
                           '                          ->  Hash Join  (cost=2.30..1181.80 rows=3054 width=44)',
                           '                                Hash Cond: (ctr1.ctr_store_sk = store.s_store_sk)',
                           '                                ->  CTE Scan on customer_total_return ctr1  (cost=0.00..1017.90 rows=50895 width=40)',
                           '                                ->  Hash  (cost=2.15..2.15 rows=12 width=4)',
                           '                                      ->  Partition Iterator  (cost=0.00..2.15 rows=12 width=4)',
                           '                                            Iterations: 16',
                           '                                            ->  Partitioned Seq Scan on store  (cost=0.00..2.15 rows=12 width=4)',
                           "                                                  Filter: (s_state = 'TN'::bpchar)",
                           '                                                  Selected Partitions:  1..16',
                           '                          ->  Hash  (cost=1405.11..1405.11 rows=200 width=36)',
                           '                                ->  Subquery Scan on subquery  (cost=1399.61..1405.11 rows=200 width=36)',
                           '                                      ->  HashAggregate  (cost=1399.61..1403.11 rows=200 width=100)',
                           '                                            Group By Key: ctr2.ctr_store_sk',
                           '                                            ->  CTE Scan on customer_total_return ctr2  (cost=0.00..1017.90 rows=50895 width=36)',
                           'EXPLAIN', 'Merge Join  (cost=241750.91..266350.65 rows=562348 width=452)',
                           '  Merge Cond: (public.date_dim.d_week_seq = wswscs.d_week_seq)', '  CTE wscs',
                           '    ->  Result  (cost=0.00..77967.65 rows=2160865 width=10)',
                           '          ->  Append  (cost=0.00..77967.65 rows=2160865 width=10)',
                           '                ->  Seq Scan on web_sales  (cost=0.00..26029.84 rows=719384 width=10)',
                           '                ->  Seq Scan on catalog_sales  (cost=0.00..51937.81 rows=1441481 width=10)',
                           '  CTE wswscs', '    ->  HashAggregate  (cost=157010.23..157114.54 rows=10431 width=252)',
                           '          Group By Key: public.date_dim.d_week_seq',
                           '          ->  Hash Join  (cost=3048.60..75977.80 rows=2160865 width=28)',
                           '                Hash Cond: (wscs.sold_date_sk = public.date_dim.d_date_sk)',
                           '                ->  CTE Scan on wscs  (cost=0.00..43217.30 rows=2160865 width=18)',
                           '                ->  Hash  (cost=2135.49..2135.49 rows=73049 width=18)',
                           '                      ->  Partition Iterator  (cost=0.00..2135.49 rows=73049 width=18)',
                           '                            Iterations: 6',
                           '                            ->  Partitioned Seq Scan on date_dim  (cost=0.00..2135.49 rows=73049 width=18)',
                           '                                  Selected Partitions:  1..6',
                           '  ->  Merge Join  (cost=5763.90..5955.32 rows=10782 width=232)',
                           '        Merge Cond: (public.date_dim.d_week_seq = ((wswscs.d_week_seq - 53)))',
                           '        ->  Sort  (cost=2333.65..2334.56 rows=365 width=4)',
                           '              Sort Key: public.date_dim.d_week_seq',
                           '              ->  Partition Iterator  (cost=0.00..2318.11 rows=365 width=4)',
                           '                    Iterations: 6',
                           '                    ->  Partitioned Seq Scan on date_dim  (cost=0.00..2318.11 rows=365 width=4)',
                           '                          Filter: (d_year = 2001)',
                           '                          Selected Partitions:  1..6',
                           '        ->  Sort  (cost=3430.25..3456.77 rows=10605 width=228)',
                           '              Sort Key: ((wswscs.d_week_seq - 53))',
                           '              ->  Hash Join  (cost=2322.68..2721.18 rows=10605 width=228)',
                           '                    Hash Cond: (wswscs.d_week_seq = public.date_dim.d_week_seq)',
                           '                    ->  CTE Scan on wswscs  (cost=0.00..208.62 rows=10431 width=228)',
                           '                    ->  Hash  (cost=2318.11..2318.11 rows=365 width=4)',
                           '                          ->  Partition Iterator  (cost=0.00..2318.11 rows=365 width=4)',
                           '                                Iterations: 6',
                           '                                ->  Partitioned Seq Scan on date_dim  (cost=0.00..2318.11 rows=365 width=4)',
                           '                                      Filter: (d_year = 2002)',
                           '                                      Selected Partitions:  1..6',
                           '  ->  Sort  (cost=904.82..930.89 rows=10431 width=228)',
                           '        Sort Key: wswscs.d_week_seq',
                           '        ->  CTE Scan on wswscs  (cost=0.00..208.62 rows=10431 width=228)',
                           'EXPLAIN',
                           'Limit  (cost=92391.70..92392.95 rows=100 width=91)',
                           '  ->  Sort  (cost=92391.70..92392.37 rows=269 width=97)',
                           '        Sort Key: dt.d_year, (sum(store_sales.ss_ext_sales_price)) DESC, item.i_brand_id',
                           '        ->  HashAggregate  (cost=92378.16..92380.85 rows=269 width=97)',
                           '              Group By Key: dt.d_year, item.i_brand, item.i_brand_id',
                           '              ->  Hash Join  (cost=4196.05..92350.39 rows=2777 width=65)',
                           '                    Hash Cond: (store_sales.ss_sold_date_sk = dt.d_date_sk)',
                           '                    ->  Hash Join  (cost=1801.20..89917.32 rows=2786 width=65)',
                           '                          Hash Cond: (store_sales.ss_item_sk = item.i_item_sk)',
                           '                          ->  Seq Scan on store_sales  (cost=0.00..80598.76 rows=2880576 width=14)',
                           '                          ->  Hash  (cost=1801.00..1801.00 rows=16 width=59)',
                           '                                ->  Partition Iterator  (cost=0.00..1801.00 rows=16 width=59)',
                           '                                      Iterations: 16',
                           '                                      ->  Partitioned Seq Scan on item  (cost=0.00..1801.00 rows=16 width=59)',
                           '                                            Filter: (i_manufact_id = 436)',
                           '                                            Selected Partitions:  1..16',
                           '                    ->  Hash  (cost=2318.11..2318.11 rows=6139 width=8)',
                           '                          ->  Partition Iterator  (cost=0.00..2318.11 rows=6139 width=8)',
                           '                                Iterations: 6',
                           '                                ->  Partitioned Seq Scan on date_dim dt  (cost=0.00..2318.11 rows=6139 width=8)',
                           '                                      Filter: (d_moy = 12)',
                           '                                      Selected Partitions:  1..6',
                           'EXPLAIN',
                           'Limit  (cost=1735014.23..1735014.24 rows=1 width=480)', '  CTE year_total',
                           '    ->  Result  (cost=393928.45..982224.47 rows=4779284 width=255)',
                           '          ->  Append  (cost=393928.45..982224.47 rows=4779284 width=255)',
                           '                ->  HashAggregate  (cost=393928.45..515875.92 rows=2633947 width=245)',
                           '                      Group By Key: public.customer.c_customer_id, public.customer.c_first_name, public.customer.c_last_name, public.customer.c_preferred_cust_flag, public.customer.c_birth_country, public.customer.c_login, public.customer.c_email_address, public.date_dim.d_year',
                           '                      ->  Hash Join  (cost=8159.60..163766.50 rows=2633947 width=213)',
                           '                            Hash Cond: (store_sales.ss_customer_sk = public.customer.c_customer_sk)',
                           '                            ->  Hash Join  (cost=3048.60..121988.79 rows=2753927 width=30)',
                           '                                  Hash Cond: (store_sales.ss_sold_date_sk = public.date_dim.d_date_sk)',
                           '                                  ->  Seq Scan on store_sales  (cost=0.00..80598.76 rows=2880576 width=30)',
                           '                                  ->  Hash  (cost=2135.49..2135.49 rows=73049 width=8)',
                           '                                        ->  Partition Iterator  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                              Iterations: 6',
                           '                                              ->  Partitioned Seq Scan on date_dim  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                                    Selected Partitions:  1..6',
                           '                            ->  Hash  (cost=3861.00..3861.00 rows=100000 width=191)',
                           '                                  ->  Partition Iterator  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                        Iterations: 16',
                           '                                        ->  Partitioned Seq Scan on customer  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                              Selected Partitions:  1..16',
                           '                ->  HashAggregate  (cost=219065.76..281762.65 rows=1426289 width=248)',
                           '                      Group By Key: public.customer.c_customer_id, public.customer.c_first_name, public.customer.c_last_name, public.customer.c_preferred_cust_flag, public.customer.c_birth_country, public.customer.c_login, public.customer.c_email_address, public.date_dim.d_year',
                           '                      ->  Hash Join  (cost=8159.60..99479.86 rows=1426289 width=216)',
                           '                            Hash Cond: (catalog_sales.cs_bill_customer_sk = public.customer.c_customer_sk)',
                           '                            ->  Hash Join  (cost=3048.60..74729.42 rows=1433745 width=33)',
                           '                                  Hash Cond: (catalog_sales.cs_sold_date_sk = public.date_dim.d_date_sk)',
                           '                                  ->  Seq Scan on catalog_sales  (cost=0.00..51937.81 rows=1441481 width=33)',
                           '                                  ->  Hash  (cost=2135.49..2135.49 rows=73049 width=8)',
                           '                                        ->  Partition Iterator  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                              Iterations: 6',
                           '                                              ->  Partitioned Seq Scan on date_dim  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                                    Selected Partitions:  1..6',
                           '                            ->  Hash  (cost=3861.00..3861.00 rows=100000 width=191)',
                           '                                  ->  Partition Iterator  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                        Iterations: 16',
                           '                                        ->  Partitioned Seq Scan on customer  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                              Selected Partitions:  1..16',
                           '                ->  HashAggregate  (cost=108795.58..136793.06 rows=719048 width=248)',
                           '                      Group By Key: public.customer.c_customer_id, public.customer.c_first_name, public.customer.c_last_name, public.customer.c_preferred_cust_flag, public.customer.c_birth_country, public.customer.c_login, public.customer.c_email_address, public.date_dim.d_year',
                           '                      ->  Hash Join  (cost=8159.60..53966.83 rows=719048 width=216)',
                           '                            Hash Cond: (web_sales.ws_sold_date_sk = public.date_dim.d_date_sk)',
                           '                            ->  Hash Join  (cost=5111.00..41030.69 rows=719216 width=216)',
                           '                                  Hash Cond: (web_sales.ws_bill_customer_sk = public.customer.c_customer_sk)',
                           '                                  ->  Seq Scan on web_sales  (cost=0.00..26029.84 rows=719384 width=33)',
                           '                                  ->  Hash  (cost=3861.00..3861.00 rows=100000 width=191)',
                           '                                        ->  Partition Iterator  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                              Iterations: 16',
                           '                                              ->  Partitioned Seq Scan on customer  (cost=0.00..3861.00 rows=100000 width=191)',
                           '                                                    Selected Partitions:  1..16',
                           '                            ->  Hash  (cost=2135.49..2135.49 rows=73049 width=8)',
                           '                                  ->  Partition Iterator  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                        Iterations: 6',
                           '                                        ->  Partitioned Seq Scan on date_dim  (cost=0.00..2135.49 rows=73049 width=8)',
                           '                                              Selected Partitions:  1..6',
                           '  ->  Sort  (cost=752789.76..752789.76 rows=1 width=480)',
                           '        Sort Key: t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name, t_s_secyear.customer_email_address',
                           '        ->  Nested Loop  (cost=0.00..752789.74 rows=1 width=480)',
                           '              Join Filter: ((t_s_secyear.customer_id = t_w_secyear.customer_id) AND (CASE WHEN (t_c_firstyear.year_total > 0::numeric) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE NULL::numeric END > CASE WHEN (t_w_firstyear.year_total > 0::numeric) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE NULL::numeric END))',
                           '              ->  Nested Loop  (cost=0.00..633304.67 rows=1 width=848)',
                           '                    Join Filter: ((t_s_secyear.customer_id = t_c_secyear.customer_id) AND (CASE WHEN (t_c_firstyear.year_total > 0::numeric) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE NULL::numeric END > CASE WHEN (t_s_firstyear.year_total > 0::numeric) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE NULL::numeric END))',
                           '                    ->  Nested Loop  (cost=0.00..513819.60 rows=1 width=812)',
                           '                          Join Filter: (t_s_firstyear.customer_id = t_s_secyear.customer_id)',
                           '                          ->  Nested Loop  (cost=0.00..394333.33 rows=2 width=300)',
                           '                                Join Filter: (t_s_firstyear.customer_id = t_w_firstyear.customer_id)',
                           '                                ->  Nested Loop  (cost=0.00..262896.22 rows=8 width=200)',
                           '                                      Join Filter: (t_s_firstyear.customer_id = t_c_firstyear.customer_id)',
                           '                                      ->  CTE Scan on year_total t_s_firstyear  (cost=0.00..131430.31 rows=40 width=100)',
                           "                                            Filter: ((year_total > 0::numeric) AND (sale_type = 's'::text) AND (dyear = 2001))",
                           '                                      ->  CTE Scan on year_total t_c_firstyear  (cost=0.00..131430.31 rows=40 width=100)',
                           "                                            Filter: ((year_total > 0::numeric) AND (sale_type = 'c'::text) AND (dyear = 2001))",
                           '                                ->  CTE Scan on year_total t_w_firstyear  (cost=0.00..131430.31 rows=40 width=100)',
                           "                                      Filter: ((year_total > 0::numeric) AND (sale_type = 'w'::text) AND (dyear = 2001))",
                           '                          ->  CTE Scan on year_total t_s_secyear  (cost=0.00..119482.10 rows=119 width=512)',
                           "                                Filter: ((sale_type = 's'::text) AND (dyear = 2002))",
                           '                    ->  CTE Scan on year_total t_c_secyear  (cost=0.00..119482.10 rows=119 width=100)',
                           "                          Filter: ((sale_type = 'c'::text) AND (dyear = 2002))",
                           '              ->  CTE Scan on year_total t_w_secyear  (cost=0.00..119482.10 rows=119 width=100)',
                           "                    Filter: ((sale_type = 'w'::text) AND (dyear = 2002))",
                           'ERROR',
                           'EXPLAIN', 'Limit  (cost=105767.16..105767.80 rows=51 width=11)',
                           '  ->  Sort  (cost=105767.16..105767.29 rows=51 width=19)',
                           '        Sort Key: (count(*)), a.ca_state', '        InitPlan 1 (returns $2)',
                           '          ->  HashAggregate  (cost=2500.80..2500.82 rows=1 width=4)',
                           '                Group By Key: date_dim.d_month_seq',
                           '                ->  Partition Iterator  (cost=0.00..2500.73 rows=28 width=4)',
                           '                      Iterations: 6',
                           '                      ->  Partitioned Seq Scan on date_dim  (cost=0.00..2500.73 rows=28 width=4)',
                           '                            Filter: ((d_year = 2000) AND (d_moy = 2))',
                           '                            Selected Partitions:  1..6',
                           '        ->  HashAggregate  (cost=103264.26..103264.90 rows=51 width=19)',
                           '              Group By Key: a.ca_state', '              Filter: (count(*) >= 10)',
                           '              ->  Hash Join  (cost=13643.14..103156.16 rows=14414 width=3)',
                           '                    Hash Cond: (c.c_current_addr_sk = a.ca_address_sk)',
                           '                    ->  Hash Join  (cost=11422.14..100736.97 rows=14414 width=4)',
                           '                          Hash Cond: (s.ss_customer_sk = c.c_customer_sk)',
                           '                          ->  Hash Join  (cost=6311.14..95425.31 rows=15071 width=4)',
                           '                                Hash Cond: (s.ss_item_sk = i.i_item_sk)',
                           '                                ->  Hash Join  (cost=2318.49..91112.00 rows=45320 width=8)',
                           '                                      Hash Cond: (s.ss_sold_date_sk = d.d_date_sk)',
                           '                                      ->  Seq Scan on store_sales s  (cost=0.00..80598.76 rows=2880576 width=12)',
                           '                                      ->  Hash  (cost=2318.11..2318.11 rows=30 width=4)',
                           '                                            ->  Partition Iterator  (cost=0.00..2318.11 rows=30 width=4)',
                           '                                                  Iterations: 6',
                           '                                                  ->  Partitioned Seq Scan on date_dim d  (cost=0.00..2318.11 rows=30 width=4)',
                           '                                                        Filter: (d_month_seq = $2)',
                           '                                                        Selected Partitions:  1..6',
                           '                                ->  Hash  (cost=3917.83..3917.83 rows=5986 width=4)',
                           '                                      ->  Hash Join  (cost=1846.35..3917.83 rows=5986 width=4)',
                           '                                            Hash Cond: (i.i_category = subquery."?column?")',
                           '                                            Join Filter: (i.i_current_price > (1.2 * subquery.avg))',
                           '                                            ->  Partition Iterator  (cost=0.00..1756.00 rows=18000 width=61)',
                           '                                                  Iterations: 16',
                           '                                                  ->  Partitioned Seq Scan on item i  (cost=0.00..1756.00 rows=18000 width=61)',
                           '                                                        Selected Partitions:  1..16',
                           '                                            ->  Hash  (cost=1846.22..1846.22 rows=10 width=83)',
                           '                                                  ->  Subquery Scan on subquery  (cost=1846.00..1846.22 rows=10 width=83)',
                           '                                                        ->  HashAggregate  (cost=1846.00..1846.12 rows=10 width=89)',
                           '                                                              Group By Key: j.i_category',
                           '                                                              ->  Partition Iterator  (cost=0.00..1756.00 rows=18000 width=57)',
                           '                                                                    Iterations: 16',
                           '                                                                    ->  Partitioned Seq Scan on item j  (cost=0.00..1756.00 rows=18000 width=57)',
                           '                                                                          Selected Partitions:  1..16',
                           '                          ->  Hash  (cost=3861.00..3861.00 rows=100000 width=8)',
                           '                                ->  Partition Iterator  (cost=0.00..3861.00 rows=100000 width=8)',
                           '                                      Iterations: 16',
                           '                                      ->  Partitioned Seq Scan on customer c  (cost=0.00..3861.00 rows=100000 width=8)',
                           '                                            Selected Partitions:  1..16',
                           '                    ->  Hash  (cost=1596.00..1596.00 rows=50000 width=7)',
                           '                          ->  Partition Iterator  (cost=0.00..1596.00 rows=50000 width=7)',
                           '                                Iterations: 17',
                           '                                ->  Partitioned Seq Scan on customer_address a  (cost=0.00..1596.00 rows=50000 width=7)',
                           '                                      Selected Partitions:  1..17', ]
        explain_results = [(result, ) for result in ori_explain_results]
        expected_costs = [19933.29, 266350.65, 92392.95, 1735014.24, 0.0, 105767.8]
        expected_indexes_names = set()
        expected_index_ids = ['99920']
        self.assertEqual((expected_costs, expected_indexes_names, expected_index_ids),
                         parse_explain_plan(explain_results, query_number))

    def test_parse_single_advisor_result(self):
        ori_inputs = [' (public,date_dim,d_year,global)', ' (public,store_sales,"ss_sold_date_sk,ss_item_sk","")']
        inputs = [(_input, ) for _input in ori_inputs]
        expected_results = [IndexItemFactory().get_index('public.date_dim', 'd_year', 'global'),
                            IndexItemFactory().get_index('public.store_sales', 'ss_sold_date_sk,ss_item_sk', '')]
        self.assertEqual(parse_single_advisor_results(inputs), expected_results)

    def test_get_checked_indexes(self):
        ori_inputs = ['SELECT 1', '(171194,<171194>btree_global_item_i_manufact_id)', 'SELECT 1',
                  '(171195,<171195>btree_store_sales_ss_item_sk)', 'SELECT 1',
                  '(171196,<171196>btree_store_sales_ss_item_sk_ss_sold_date_sk)', 'SELECT 3',
                  '(<171194>btree_global_item_i_manufact_id,171194,item,"(i_manufact_id)")',
                  '(<171195>btree_store_sales_ss_item_sk,171195,store_sales,"(ss_item_sk)")',
                  '(<171196>btree_store_sales_ss_item_sk_ss_sold_date_sk,171196,store_sales,"(ss_item_sk, ss_sold_date_sk)")',
                  'EXPLAIN', 'Limit  (cost=13107.63..13108.88 rows=100 width=91)',
                  '  ->  Sort  (cost=13107.63..13108.30 rows=269 width=97)',
                  '        Sort Key: dt.d_year, (sum(store_sales.ss_ext_sales_price)) DESC, item.i_brand_id',
                  '        ->  HashAggregate  (cost=13094.08..13096.77 rows=269 width=97)',
                  '              Group By Key: dt.d_year, item.i_brand, item.i_brand_id',
                  '              ->  Hash Join  (cost=2400.45..13066.31 rows=2777 width=65)',
                  '                    Hash Cond: (store_sales.ss_sold_date_sk = dt.d_date_sk)',
                  '                    ->  Nested Loop  (cost=5.60..10633.25 rows=2786 width=65)',
                  '                          ->  Index Scan using <171194>btree_global_item_i_manufact_id on item  (cost=0.00..68.53 rows=16 width=59)',
                  '                                Index Cond: (i_manufact_id = 436)',
                  '                          ->  Bitmap Heap Scan on store_sales  (cost=5.60..658.55 rows=174 width=14)',
                  '                                Recheck Cond: (ss_item_sk = item.i_item_sk)',
                  '                                ->  Bitmap Index Scan on <171196>btree_store_sales_ss_item_sk_ss_sold_date_sk  (cost=0.00..5.55 rows=174 width=0)',
                  '                                      Index Cond: (ss_item_sk = item.i_item_sk)',
                  '                    ->  Hash  (cost=2318.11..2318.11 rows=6139 width=8)',
                  '                          ->  Partition Iterator  (cost=0.00..2318.11 rows=6139 width=8)',
                  '                                Iterations: 6',
                  '                                ->  Partitioned Seq Scan on date_dim dt  (cost=0.00..2318.11 rows=6139 width=8)',
                  '                                      Filter: (d_moy = 12)',
                  '                                      Selected Partitions:  1..6', 'SELECT 1', '']
        inputs = [(_input, ) for _input in ori_inputs]
        expected_indexes = [IndexItemFactory().get_index('public.item', 'i_manufact_id', 'global'),
                            IndexItemFactory().get_index('public.store_sales', 'ss_item_sk, ss_sold_date_sk', '')]
        self.assertEqual(get_checked_indexes(inputs, {'public.item', 'public.store_sales'}), expected_indexes)

    def test_parse_existed_indexes_results(self):
        ori_inputs = ['SELECT 6',
                  'ship_mode|ship_mode_pkey|CREATE UNIQUE INDEX ship_mode_pkey ON ship_mode USING btree (sm_ship_mode_sk) LOCAL(PARTITION p_list_15_sm_ship_mode_sk_idx, PARTITION p_list_14_sm_ship_mode_sk_idx, PARTITION p_list_13_sm_ship_mode_sk_idx, PARTITION p_list_12_sm_ship_mode_sk_idx, PARTITION p_list_11_sm_ship_mode_sk_idx, PARTITION p_list_10_sm_ship_mode_sk_idx, PARTITION p_list_9_sm_ship_mode_sk_idx, PARTITION p_list_8_sm_ship_mode_sk_idx, PARTITION p_list_7_sm_ship_mode_sk_idx, PARTITION p_list_6_sm_ship_mode_sk_idx, PARTITION p_list_5_sm_ship_mode_sk_idx, PARTITION p_list_4_sm_ship_mode_sk_idx, PARTITION p_list_3_sm_ship_mode_sk_idx, PARTITION p_list_2_sm_ship_mode_sk_idx, PARTITION p_list_1_sm_ship_mode_sk_idx)  TABLESPACE pg_default|p',
                  'temptable|temptable_int2_int3_int4_idx|CREATE INDEX temptable_int2_int3_int4_idx ON temptable USING btree (int2, int3, int4) TABLESPACE pg_default|None',
                  'temptable|temptable_int2_int3_idx|CREATE INDEX temptable_int2_int3_idx ON temptable USING btree (int2, int3) TABLESPACE pg_default|None',
                  'temptable|temptable_int1_int2_int3_idx|CREATE INDEX temptable_int1_int2_int3_idx ON temptable USING btree (int1, int2, int3) TABLESPACE pg_default|None',
                  'temptable|temptable_int1_int2_idx|CREATE INDEX temptable_int1_int2_idx ON temptable USING btree (int1, int2) TABLESPACE pg_default|None',
                  'temptable|temptable_int1_idx|CREATE INDEX temptable_int1_idx ON temptable USING btree (int1) TABLESPACE pg_default|None']
        schema = 'public'
        inputs = [tuple(_input.split('|')) for _input in ori_inputs]
        expected_indexes = [ExistingIndex("public", "ship_mode", "ship_mode_pkey", "sm_ship_mode_sk",
                                         "CREATE UNIQUE INDEX ship_mode_pkey ON ship_mode USING btree (sm_ship_mode_sk) LOCAL(PARTITION p_list_15_sm_ship_mode_sk_idx, PARTITION p_list_14_sm_ship_mode_sk_idx, PARTITION p_list_13_sm_ship_mode_sk_idx, PARTITION p_list_12_sm_ship_mode_sk_idx, PARTITION p_list_11_sm_ship_mode_sk_idx, PARTITION p_list_10_sm_ship_mode_sk_idx, PARTITION p_list_9_sm_ship_mode_sk_idx, PARTITION p_list_8_sm_ship_mode_sk_idx, PARTITION p_list_7_sm_ship_mode_sk_idx, PARTITION p_list_6_sm_ship_mode_sk_idx, PARTITION p_list_5_sm_ship_mode_sk_idx, PARTITION p_list_4_sm_ship_mode_sk_idx, PARTITION p_list_3_sm_ship_mode_sk_idx, PARTITION p_list_2_sm_ship_mode_sk_idx, PARTITION p_list_1_sm_ship_mode_sk_idx)  TABLESPACE pg_default"),
                            ExistingIndex("public", "temptable", "temptable_int2_int3_int4_idx", "int2, int3, int4",
                                         "CREATE INDEX temptable_int2_int3_int4_idx ON temptable USING btree (int2, int3, int4) TABLESPACE pg_default"),
                            ExistingIndex("public", "temptable", "temptable_int2_int3_idx", "int2, int3",
                                         "CREATE INDEX temptable_int2_int3_idx ON temptable USING btree (int2, int3) TABLESPACE pg_default"),
                            ExistingIndex("public", "temptable", "temptable_int1_int2_int3_idx", "int1, int2, int3",
                                         "CREATE INDEX temptable_int1_int2_int3_idx ON temptable USING btree (int1, int2, int3) TABLESPACE pg_default"),
                            ExistingIndex("public", "temptable", "temptable_int1_int2_idx", "int1, int2",
                                         "CREATE INDEX temptable_int1_int2_idx ON temptable USING btree (int1, int2) TABLESPACE pg_default"),
                            ExistingIndex("public", "temptable", "temptable_int1_idx", "int1",
                                         "CREATE INDEX temptable_int1_idx ON temptable USING btree (int1) TABLESPACE pg_default")]
        indexes = parse_existing_indexes_results(inputs, schema)
        for index, expected_indexes in zip(indexes, expected_indexes):
            self.assertEqual(
                (index.get_schema_table(), index.get_indexname(), index.get_columns(), index.get_indexdef()),
                (expected_indexes.get_schema_table(), expected_indexes.get_indexname(), expected_indexes.get_columns(),
                 expected_indexes.get_indexdef())
            )
        self.assertEqual(indexes[0].is_primary_key(), True)


class SqlGeneratorTester(unittest.TestCase):

    def test_existed_index_sql(self):
        schema = 'public'
        tables = ['table1', 'table2']
        expected = "SELECT c.relname AS tablename, i.relname AS indexname, pg_catalog.pg_get_indexdef(i.oid) " \
                   "AS indexdef, p.contype AS pkey from pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN " \
                   "pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n ON n.oid = c.relnamespace " \
                   "LEFT JOIN pg_constraint p ON (i.oid = p.conindid AND p.contype = 'p') WHERE (c.relkind = " \
                   "ANY (ARRAY['r'::\"char\", 'm'::\"char\"])) AND (i.relkind = ANY (ARRAY['i'::\"char\", 'I'::\"char\"])) " \
                   "AND n.nspname = 'public' AND c.relname in ('table1','table2') order by c.relname;"
        self.assertEqual(get_existing_index_sql(schema, tables), expected)

    def test_workload_cost_sqls(self):
        statements = ['select 1', 'select * from bmsql_customer limit 1']
        indexes = [IndexItemFactory().get_index('public.date_dim', 'd_year', 'global')]
        is_multi_node = True
        expected = ['SET enable_hypo_index = on;\n',
                    "SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON public.date_dim(d_year) global');",
                    'set enable_fast_query_shipping = off;', 'set enable_stream_operator = on; ',
                    "set explain_perf_mode = 'normal'; ", 'EXPLAIN select 1;',
                    'EXPLAIN select * from bmsql_customer limit 1;']

        self.assertEqual(get_workload_cost_sqls(statements, indexes, is_multi_node), expected)

    def test_single_advisor_sql(self):
        statement = 'select * from bmsql_customer where c_w_id=0'
        expected = "select pg_catalog.gs_index_advise('select * from bmsql_customer where c_w_id=0');"
        self.assertEqual(get_single_advisor_sql(statement), expected)

    def test_index_check_sqls(self):
        statement = 'select * from bmsql_customer where c_d_id=1'
        indexes = [IndexItemFactory().get_index('public.date_dim', 'd_year', 'global')]
        is_multi_node = True
        expected = ['SET enable_hypo_index = on;', 'SET enable_fast_query_shipping = off;',
                    'SET enable_stream_operator = on;',
                    "SELECT pg_catalog.hypopg_create_index('CREATE INDEX ON public.date_dim(d_year) global')",
                    'SELECT pg_catalog.hypopg_display_index()', "SET explain_perf_mode = 'normal';",
                    'explain select * from bmsql_customer where c_d_id=1', 'SELECT pg_catalog.hypopg_reset_index()']
        self.assertEqual(get_index_check_sqls(statement, indexes, is_multi_node), expected)


class IndexAdvisorTester(unittest.TestCase):

    def test_get_workload_template(self):
        workload = []
        for sql, frequency in Case.expected_sql_frequency.items():
            workload.append(index_advisor_workload.QueryItem(sql, frequency))
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
        templates = index_advisor_workload.get_workload_template(workload)
        self.assertDictEqual(templates, expected_templates)

    def test_workload_compression(self):
        with patch('dbmind.components.index_advisor.index_advisor_workload.open',
                   mock_open(read_data=Case.sql_content)) as m:
            queries = index_advisor_workload.workload_compression('test')
        workloadCount = 7
        self.assertEqual(int(sum(query.get_frequency() for query in queries)), workloadCount)

    def test_generate_sorted_atomic_config(self):
        queryitem1 = index_advisor_workload.QueryItem('test', 0)
        queryitem2 = index_advisor_workload.QueryItem('test', 0)
        queryitem3 = index_advisor_workload.QueryItem('test', 0)
        queryitem4 = index_advisor_workload.QueryItem('test', 0)
        queryitem1.add_index(IndexItemFactory().get_index('table1', 'col1, col2', index_type='local'))
        queryitem1.add_index(IndexItemFactory().get_index('table3', 'col1, col3', index_type='global'))
        queryitem1.add_index(IndexItemFactory().get_index('table2', 'col1, col3', index_type='global'))
        queryitem2.add_index(IndexItemFactory().get_index('table1', 'col1, col2', index_type='local'))
        queryitem2.add_index(IndexItemFactory().get_index('table1', 'col2, col3', index_type='global'))
        queryitem2.add_index(IndexItemFactory().get_index('table2', 'col1, col3', index_type='global'))
        queryitem3.add_index(IndexItemFactory().get_index('table4', 'col1, col2', index_type=''))
        queryitem3.add_index(IndexItemFactory().get_index('table4', 'col3', index_type=''))
        atomic_config_total = generate_sorted_atomic_config([queryitem1, queryitem2, queryitem3, queryitem4], [])
        expected_config_total = [(), (IndexItemFactory().get_index("table1", "col1, col2", "local"),),
                                 (IndexItemFactory().get_index("table2", "col1, col3", "global"),),
                                 (IndexItemFactory().get_index("table1", "col1, col2", "local"),
                                  IndexItemFactory().get_index("table2", "col1, col3", "global")),
                                 (IndexItemFactory().get_index("table1", "col2, col3", "global"),),
                                 (IndexItemFactory().get_index("table1", "col1, col2", "local"),
                                  IndexItemFactory().get_index("table1", "col2, col3", "global")),
                                 (IndexItemFactory().get_index("table1", "col2, col3", "global"),
                                  IndexItemFactory().get_index("table2", "col1, col3", "global")),
                                 (IndexItemFactory().get_index("table1", "col1, col2", "local"),
                                  IndexItemFactory().get_index("table1", "col2, col3", "global"),
                                  IndexItemFactory().get_index("table2", "col1, col3", "global")),
                                 (IndexItemFactory().get_index("table4", "col1, col2", ""),),
                                 (IndexItemFactory().get_index("table4", "col3", ""),),
                                 (IndexItemFactory().get_index("table4", "col1, col2", ""),
                                  IndexItemFactory().get_index("table4", "col3", ""))
                                 ]

        self.assertEqual(atomic_config_total, expected_config_total)

    def test_add_more_column_index(self):
        indexes = [IndexItemFactory().get_index("public.item", "i_manufact_id", "global"),
                   IndexItemFactory().get_index("public.store_sales", "ss_item_sk", "")]
        table = 'public.store_sales'
        columns_info = ('ss_item_sk', '')
        single_col_info = ('ss_sold_date_sk', '')
        expected_indexes = [IndexItemFactory().get_index("public.item", "i_manufact_id", "global"),
                            IndexItemFactory().get_index("public.store_sales", "ss_item_sk", ""),
                            IndexItemFactory().get_index("public.store_sales", "ss_item_sk, ss_sold_date_sk", "")]
        add_more_column_index(indexes, table, columns_info, single_col_info)
        self.assertEqual(indexes, expected_indexes)

    def test_filter_redundant_indexes_with_same_type(self):
        filtered_indexes = filter_redundant_indexes_with_same_type(Case.indexes)
        expected_indexes = [IndexItemFactory().get_index("public.item", "i_manufact_id, i_brand_id", "global"),
                            IndexItemFactory().get_index("public.item", "i_manufact_id", "local"),
                            IndexItemFactory().get_index("public.store_sales", "ss_item_sk, ss_sold_date_sk", ""),
                            IndexItemFactory().get_index("public.store_sales", "ss_sold_date_sk", "")]
        self.assertEqual(filtered_indexes, expected_indexes)

    def test_filter_redundant_with_diff_types(self):
        indexes = Case.indexes[:]
        index_advisor_workload.IndexAdvisor.filter_redundant_indexes_with_diff_types(indexes)
        expected_indexes = [IndexItemFactory().get_index('public.item', 'i_manufact_id', 'global'),
                            IndexItemFactory().get_index("public.store_sales", "ss_item_sk, ss_sold_date_sk", ""),
                            IndexItemFactory().get_index("public.store_sales", "ss_sold_date_sk", "")]
        self.assertEqual(indexes, expected_indexes)

    def test_filter_low_benefit_index(self):
        index_advisor = Case.get_index_advisor()
        index_advisor.filter_low_benefit_index([Case.index1, Case.index2])
        expected_indexes = [Case.index1]
        self.assertEqual(index_advisor.determine_indexes, expected_indexes)

    def test_record_info(self):
        index_advisor = Case.get_index_advisor()
        sql_info = dict()
        index_advisor.record_info(Case.index1, sql_info, 'store_sales',
                                  'CREATE INDEX idx_store_sales_ss_item_sk_ss_sold_date_sk on '
                                  'public.store_sales(ss_item_sk, ss_sold_date_sk);')
        expected = {'associationIndex': defaultdict(list),
                    'workloadOptimized': '1.00', 'schemaName': 'public', 'tbName': 'store_sales',
                    'columns': 'ss_item_sk, ss_sold_date_sk', 'index_type': '',
                    'statement': 'CREATE INDEX idx_store_sales_ss_item_sk_ss_sold_date_sk on '
                                 'public.store_sales(ss_item_sk, ss_sold_date_sk);',
                    'dmlCount': 9, 'selectRatio': 100.0,
                    'insertRatio': 0.0, 'deleteRatio': 0.0, 'updateRatio': 0.0}
        self.assertEqual(sql_info, expected)

    def test_get_positive_sql_count(self):
        indexes = [Case.index1, Case.index2]
        index_advisor = Case.get_index_advisor()
        self.assertEqual(index_advisor_workload.get_positive_sql_count(indexes, index_advisor.workload), 1)

    def test_get_redundant_created_indexes(self):
        indexes = [ExistingIndex('public', 'table1', 'idx1', 'col1, col2', 'create index on table1(col1,col2)'),
                   ExistingIndex('public', 'table1', 'idx1', 'col1', 'create index on table1(col1)'),
                   ExistingIndex('public', 'table1', 'idx1', 'col2, col3', 'create index on table1(col2,col3)'),
                   ExistingIndex('public', 'table1', 'idx1', 'col2, col3, col4',
                                'create index on table1(col2,col3,col4)'),
                   ExistingIndex('public', 'table1', 'idx1', 'col1, col2, col3',
                                'create index on table1(col1,col2,col3)'),
                   ExistingIndex('public', 'table1', 'idx1', 'col1, col2, col3, col4',
                                'create index on table1(col1,col2,col3,col4)'),
                   ]
        unused_indexes = indexes[2:]
        redundant_indexes = index_advisor_workload.get_redundant_created_indexes(indexes, unused_indexes)
        expected_indexes = [indexes[1], indexes[2], indexes[4]]
        self.assertEqual(redundant_indexes, expected_indexes)

    def test_generate_single_column_indexes(self):
        indexes = [IndexItemFactory().get_index('table1', 'col1, col2', 'local'),
                   IndexItemFactory().get_index('table1', 'col1', 'global'),
                   IndexItemFactory().get_index('table1', 'col2', 'local'),
                   IndexItemFactory().get_index('table2', 'col1, col2', 'local'),
                   IndexItemFactory().get_index('table2', 'col1', 'global'),
                   IndexItemFactory().get_index('table2', 'col2', 'local'),
                   IndexItemFactory().get_index('table3', 'col3', ''),
                   IndexItemFactory().get_index('table3', 'col2, col3', '')]
        expected_indexes = [IndexItemFactory().get_index('table1', 'col1', 'local'),
                            IndexItemFactory().get_index('table1', 'col2', 'local'),
                            IndexItemFactory().get_index('table1', 'col1', 'global'),
                            IndexItemFactory().get_index('table2', 'col1', 'local'),
                            IndexItemFactory().get_index('table2', 'col2', 'local'),
                            IndexItemFactory().get_index('table2', 'col1', 'global'),
                            IndexItemFactory().get_index('table3', 'col3', ''),
                            IndexItemFactory().get_index('table3', 'col2', '')
                            ]
        single_column_indexes = index_advisor_workload.generate_single_column_indexes(indexes)
        self.assertEqual(single_column_indexes, expected_indexes)

    def test_find_subsets_configs(self):
        cur_config = [IndexItemFactory().get_index('table2', 'col1', 'global'),
                      IndexItemFactory().get_index('table3', 'col3', ''),
                      IndexItemFactory().get_index('table3', 'col2, col3, col4', '')
                      ]
        atomic_configs = [(IndexItemFactory().get_index('table2', 'col1', 'global'),),
                          (IndexItemFactory().get_index('table2', 'col2', 'local'),),
                          (IndexItemFactory().get_index('table3', 'col3', ''),),
                          (IndexItemFactory().get_index('table3', 'col2, col3, col4', ''),),
                          (IndexItemFactory().get_index('table2', 'col1', 'global'),
                           IndexItemFactory().get_index('table2', 'col2', 'local'),),
                          (IndexItemFactory().get_index('table3', 'col3', ''),
                           IndexItemFactory().get_index('table3', 'col2', ''),),
                          (IndexItemFactory().get_index('table3', 'col3', ''),
                           IndexItemFactory().get_index('table3', 'col2, col3', ''),),
                          (IndexItemFactory().get_index('table2', 'col1', 'global'),
                           IndexItemFactory().get_index('table3', 'col2, col3', ''),),
                          ]
        self.assertEqual(dbmind.components.index_advisor.utils.lookfor_subsets_configs(cur_config, atomic_configs),
                         atomic_configs[-2:])

    def test_mcts(self):
        index1 = IndexItemFactory().get_index('public.a', 'col1', index_type='global')
        index2 = IndexItemFactory().get_index('public.b', 'col1', index_type='global')
        index3 = IndexItemFactory().get_index('public.c', 'col1', index_type='global')
        index4 = IndexItemFactory().get_index('public.d', 'col1', index_type='global')

        atomic_index1 = index1
        atomic_index2 = index2
        atomic_index3 = index3
        atomic_index4 = index4

        atomic_index1.set_storage(10)
        atomic_index2.set_storage(4)
        atomic_index3.set_storage(7)
        atomic_index4.set_storage(8)
        available_choices = [index1, index2, index3, index4]
        atomic_choices = [(), (atomic_index2,), (atomic_index1,), (atomic_index3,),
                          (atomic_index2, atomic_index3), (atomic_index4,)]
        query = QueryItem('select * from gia_01', 1)
        workload = WorkLoad([query])

        storage_threshold = 12
        costs = [10, 7, 5, 9, 4, 11]
        for cost, indexes in zip(costs, atomic_choices):
            workload.add_indexes(indexes, [cost], 'None')
        results = mcts.MCTS(workload, atomic_choices, available_choices, storage_threshold, 2)
        self.assertEqual(set(results), {index2, index3})

        mcts.CANDIDATE_SUBSET_BENEFIT = defaultdict(list)
        mcts.CANDIDATE_SUBSET = defaultdict(list)
        index2.association, index3.association = defaultdict(list), defaultdict(list)
        workload = WorkLoad([query])
        storage_threshold = 20
        costs = [10, 8, 6, 9, 4, 6]
        for cost, indexes in zip(costs, atomic_choices):
            workload.add_indexes(indexes, [cost], 'None')
        results = mcts.MCTS(workload, atomic_choices, available_choices, storage_threshold, 3)
        self.assertSetEqual(set(results), {index2, index3, index4})

        mcts.CANDIDATE_SUBSET_BENEFIT = defaultdict(list)
        mcts.CANDIDATE_SUBSET = defaultdict(list)
        index2.association, index3.association = defaultdict(list), defaultdict(list)
        print('===============')
        results = mcts.MCTS(workload, atomic_choices, available_choices, storage_threshold, 2)
        # this result is not stable
        print(results)
        self.assertSetEqual(set(results), {index1, index4})
        self.assertIn(index2.association,
                      [{},
                       {'public.b col1 global,public.c col1 global': [('select * from gia_01', 0.3)]},
                       {'public.c col1 global,public.b col1 global': [('select * from gia_01', 0.3)]}
                       ]
                      )

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
            ret = index_advisor_workload.main()
            if '--driver' in cmd:
                sys.argv[1:] = shlex.split(cmd.replace('--driver', ''))
            else:
                sys.argv[1:] = shlex.split(cmd + '--driver')
            ret = index_advisor_workload.main()

    if __name__ == '__main__':
        unittest.main()
