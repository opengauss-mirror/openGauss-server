/*
################################################################################
# TESTCASE NAME : skew_statistic_data_type_01.py 
# COMPONENT(S)  : 测试单列分布键的倾斜场景
# PREREQUISITE  : skew_hint_setup.py
# PLATFORM      : all
# DESCRIPTION   : skew hint
# TAG           : hint
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
set current_schema to skew_join;
--S1.skew_option=normal
set skew_option=normal;
--S2.设置计划格式
set explain_perf_mode=normal;
--S3.设置计划生成参数
set enable_broadcast = off;
--S4.设置计划生成参数
set enable_mergejoin = off;
--S5.设置计划生成参数
set enable_nestloop = off;
set query_dop = 1002;

--I2.单列重分布，单值倾斜
--S1.倾斜列分布键为单列表达式
explain (costs off) select count(*) from skew_scol s, test_scol t where sqrt(s.a) = t.a;
--S2.倾斜列分布键为多列表达式
explain (costs off) select count(*) from skew_scol s, test_scol t where s.a + s.b = t.a;
--S3.倾斜列分布键为多列表达式
explain (costs off) select count(*) from skew_scol s, test_scol t where s.a * s.b = t.a;
--S4.倾斜列分布键为agg结果
explain (costs off) select count(*) from test_scol t, (select count(*) from skew_scol group by a, b)tmp(a) where tmp.a = t.a;
--S5.倾斜列对侧分布键为agg结果
explain (costs off) select count(*) from skew_scol s, (select count(*) from test_scol group by a, b)tmp(a) where tmp.a = s.b;

set query_dop = 2002;
