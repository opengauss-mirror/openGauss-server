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
import unittest

from dbmind.components.sql_rewriter import SQLRewriter, get_offline_rewriter

mapper = {'DistinctStar': {
        'select distinct * from bmsql_config join bmsql_district b on True;':
            'SELECT bmsql_config.cfg_name, bmsql_config.cfg_value, b.d_w_id, b.d_id, b.d_ytd, b.d_tax, '
            'b.d_next_o_id, b.d_name, b.d_street_1, b.d_street_2, b.d_city, b.d_state, b.d_zip '
            'FROM bmsql_config JOIN bmsql_district AS b ON TRUE;',
        },
    'Star2Columns': {
        'select * from bmsql_config a, bmsql_config b;':
            'SELECT a.cfg_name, a.cfg_value, b.cfg_name, b.cfg_value FROM bmsql_config AS a, bmsql_config AS b;',
        'select * from (select * from bmsql_config a, bmsql_config b);':
            'SELECT * FROM (SELECT a.cfg_name, a.cfg_value, b.cfg_name, b.cfg_value FROM bmsql_config AS a, bmsql_config AS b);'
        },
    'Having2Where': {
        "select cfg_name from bmsql_config group by 1 having cfg_name='1';":
            "SELECT cfg_name FROM bmsql_config WHERE cfg_name = '1';"},
    'ImplicitConversion': {
        'select * from bmsql_oorder where o_w_id +1  >3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id > 2;',
        'select * from bmsql_oorder where o_w_id +1 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id < 2;',
        'select * from bmsql_oorder where o_w_id -1  >3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id > 4;',
        'select * from bmsql_oorder where o_w_id -1 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id < 4;',
        'select * from bmsql_oorder where o_w_id * 0 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder;',
        'select * from bmsql_oorder where o_w_id * 2 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id < 1.5;',
        'select * from bmsql_oorder where o_w_id * -2 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id > -1.5;',
        'select * from bmsql_oorder where o_w_id / -2 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id > -6;',
        'select * from bmsql_oorder where o_w_id / 2 < 3;':
            'SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d FROM bmsql_oorder WHERE o_w_id < 6;',
        'select * from bmsql_oorder where o_w_id /0 >3;':
            'select * from bmsql_oorder where o_w_id /0 >3;'},
    'OrderbyConst': {
        'select cfg_name from bmsql_config order by 1;': 'SELECT cfg_name FROM bmsql_config ORDER BY cfg_name;',
        'select cfg_name from bmsql_config group by 1;': 'SELECT cfg_name FROM bmsql_config GROUP BY cfg_name;'},
    'OrderbyConstColumns': {
        "select cfg_name from bmsql_config where cfg_name='2' group by cfg_name order by cfg_name, cfg_value;":
            "SELECT cfg_name FROM bmsql_config WHERE cfg_name = '2' ORDER BY cfg_value;"},
    'AlwaysTrue': {'select * from bmsql_config where 1=1 and 2=2;': 'SELECT cfg_name, cfg_value FROM bmsql_config;'},
    'UnionAll': {
        'select * from bmsql_config union select * from bmsql_config;':
            'SELECT cfg_name, cfg_value FROM bmsql_config UNION ALL SELECT cfg_name, cfg_value FROM bmsql_config;'},
    'Delete2Truncate': {'delete from bmsql_config;':
                            'TRUNCATE TABLE bmsql_config;'},
    'Or2In': {
        "select * from bmsql_stock where  s_w_id=10 or  s_w_id=1 or s_w_id=100 or  s_i_id=1 or s_i_id=10":
            '''SELECT s_w_id,
       s_i_id,
       s_quantity,
       s_ytd,
       s_order_cnt,
       s_remote_cnt,
       s_data,
       s_dist_01,
       s_dist_02,
       s_dist_03,
       s_dist_04,
       s_dist_05,
       s_dist_06,
       s_dist_07,
       s_dist_08,
       s_dist_09,
       s_dist_10
FROM bmsql_stock
WHERE s_i_id IN (1,
                 10)
  OR s_w_id IN (10,
                1,
                100);'''},
    'SelfJoin': {
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20) AND a.c_id > b.c_id '
            'UNION ALL SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20 + 1) AND a.c_id - b.c_id <= 20);',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20);',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by 1;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by a.c_id;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
        'select distinct a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by a.c_id;':
            'SELECT * FROM '
            '(SELECT DISTINCT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT DISTINCT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
    }
}

offline_mapper = {
    'ImplicitConversion': {
        'select o_w_id from bmsql_oorder where o_w_id +1  >3;':
            'SELECT o_w_id FROM bmsql_oorder WHERE o_w_id > 2;',
        'select * from bmsql_oorder where o_w_id +1  >3;':
            'SELECT * FROM bmsql_oorder WHERE o_w_id > 2;',
    },
    'OrderbyConst': {
        'select cfg_name from bmsql_config order by 1;': 'SELECT cfg_name FROM bmsql_config ORDER BY cfg_name;',
        'select cfg_name from bmsql_config group by 1;': 'SELECT cfg_name FROM bmsql_config GROUP BY cfg_name;'},
    'OrderbyConstColumns': {
        "select cfg_name from bmsql_config where cfg_name='2' group by cfg_name order by cfg_name, cfg_value;":
            "SELECT cfg_name FROM bmsql_config WHERE cfg_name = '2' ORDER BY cfg_value;"},
    'AlwaysTrue': {'select cfg_name from bmsql_config where 1=1 and 2=2;': 'SELECT cfg_name FROM bmsql_config;'},
    'UnionAll': {
        'select cfg_name, cfg_value from bmsql_config union select cfg_name, cfg_value from bmsql_config;':
            'SELECT cfg_name, cfg_value FROM bmsql_config UNION ALL SELECT cfg_name, cfg_value FROM bmsql_config;'},
    'Delete2Truncate': {'delete from bmsql_config;':
                            'TRUNCATE TABLE bmsql_config;'},
    'Or2In': {
        "select s_w_id from bmsql_stock where  s_w_id=10 or  s_w_id=1 or s_w_id=100 or  s_i_id=1 or s_i_id=10":
            '''SELECT s_w_id
FROM bmsql_stock
WHERE s_i_id IN (1,
                 10)
  OR s_w_id IN (10,
                1,
                100);'''},
    'SelfJoin': {
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20) AND a.c_id > b.c_id '
            'UNION ALL SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id) / 20) = TRUNC(b.c_id / 20 + 1) AND a.c_id - b.c_id <= 20);',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20);',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by 1;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
        'select a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by a.c_id;':
            'SELECT * FROM '
            '(SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
        'select distinct a.c_id from bmsql_customer a, bmsql_customer b where a.c_id - b.c_id <= 20 and a.c_id > b.c_id + 1 order by a.c_id;':
            'SELECT * FROM '
            '(SELECT DISTINCT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19) AND a.c_id - b.c_id > 1 '
            'UNION ALL '
            'SELECT DISTINCT a.c_id FROM bmsql_customer AS a, bmsql_customer AS b WHERE TRUNC((a.c_id + -1) / 19) = TRUNC(b.c_id / 19 + 1) AND a.c_id - b.c_id <= 20) '
            'ORDER BY 1;',
    }
}

table2columns_mapper = {
    'bmsql_oorder': ['o_w_id', 'o_d_id', 'o_id', 'o_c_id', 'o_carrier_id', 'o_ol_cnt', 'o_all_local', 'o_entry_d'],
    'bmsql_customer': ['c_w_id', 'c_d_id', 'c_id', 'c_discount', 'c_credit', 'c_last', 'c_first', 'c_credit_lim',
                       'c_balance', 'c_ytd_payment', 'c_payment_cnt', 'c_delivery_cnt', 'c_street_1', 'c_street_2',
                       'c_city', 'c_state', 'c_zip', 'c_phone', 'c_since', 'c_middle', 'c_data'],
    'bmsql_stock': ['s_w_id', 's_i_id', 's_quantity', 's_ytd', 's_order_cnt', 's_remote_cnt', 's_data', 's_dist_01',
                    's_dist_02', 's_dist_03', 's_dist_04', 's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08',
                    's_dist_09', 's_dist_10'],
    'bmsql_config': ['cfg_name', 'cfg_value'],
    'bmsql_district': ['d_w_id', 'd_id', 'd_ytd', 'd_tax', 'd_next_o_id', 'd_name', 'd_street_1', 'd_street_2',
                       'd_city', 'd_state', 'd_zip']}

table_exists_primary = {'bmsql_config': True,
                        'bmsql_customer': True,
                        'bmsql_oorder': True,
                        'bmsql_district': True}

offline_rewriter = get_offline_rewriter()


class RewriteTester(unittest.TestCase):
    def __test_rule(self, rule):
        for input_sql, expected_output_sql in mapper.get(rule).items():
            _, output_sql = SQLRewriter().rewrite(input_sql, table2columns_mapper, table_exists_primary)
            self.assertEqual(re.sub(r'\s+', ' ', output_sql), re.sub(r'\s+', ' ', expected_output_sql))

    def test_DistinctStar(self):
        self.__test_rule('DistinctStar')

    def test_Star2Columns(self):
        self.__test_rule('Star2Columns')

    def test_ImplicitConversion(self):
        self.__test_rule('ImplicitConversion')

    def test_OrderbyConst(self):
        self.__test_rule('OrderbyConst')

    def test_OrderbyConstColumns(self):
        self.__test_rule('OrderbyConstColumns')

    def test_AlwaysTrue(self):
        self.__test_rule('AlwaysTrue')

    def test_UnionAll(self):
        self.__test_rule('UnionAll')

    def test_Delete2Truncate(self):
        self.__test_rule('Delete2Truncate')

    def test_Or2In(self):
        self.__test_rule('Or2In')

    def test_SelfJoin(self):
        self.__test_rule('SelfJoin')

    def __test_rule_offline(self, rule):
        for input_sql, expected_output_sql in offline_mapper.get(rule).items():
            _, output_sql = offline_rewriter.rewrite(input_sql, table2columns_mapper, table_exists_primary)
            self.assertEqual(re.sub(r'\s+', ' ', output_sql), re.sub(r'\s+', ' ', expected_output_sql))

    def test_ImplicitConversion_offline(self):
        self.__test_rule_offline('ImplicitConversion')

    def test_OrderbyConstColumns_offline(self):
        self.__test_rule_offline('OrderbyConstColumns')

    def test_AlwaysTrue_offline(self):
        self.__test_rule_offline('AlwaysTrue')

    def test_UnionAll_offline(self):
        self.__test_rule_offline('UnionAll')

    def test_Delete2Truncate_offline(self):
        self.__test_rule_offline('Delete2Truncate')

    def test_Or2In_offline(self):
        self.__test_rule_offline('Or2In')

    def test_SelfJoin_offline(self):
        self.__test_rule_offline('SelfJoin')


if __name__ == '__main__':
    unittest.main()

