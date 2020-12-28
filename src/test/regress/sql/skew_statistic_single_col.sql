/*
################################################################################
# TESTCASE NAME : skew_statistic_single_col.py 
# COMPONENT(S)  : 测试单列分布键的倾斜场景
# PREREQUISITE  : skew_hint_setup.py
# PLATFORM      : all
# DESCRIPTION   : single col's skew
# TAG           : single col
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
--S1.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b;
--S2.执行查询
select count(*) from skew_scol s, test_scol t where s.b = t.b;

--I3.单列重分布，基表有NULL值
--S1.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.c = t.c;
--S2.执行查询
select count(*) from skew_scol s, test_scol t where s.c = t.c;

--I4.单列重分布，倾斜列带有过滤条件
--S1.执行计划，简单表达式，倾斜值被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b > 1;
--S2.执行计划，简单表达式，倾斜值被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b != 1;
--S3.执行计划，简单表达式，倾斜值不被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b < 100;
--S4.执行计划，简单表达式，倾斜值不被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b + 50 > 10;
--S5.执行计划，复杂表达式，倾斜值被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and sqrt(s.b) > 1;
--S6.执行计划，复杂表达式，倾斜值不被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and sqrt(s.b) < 10;
--S7.执行计划，多列表达式，倾斜值不一定被过滤
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and  s.a + s.b > 1;

--I5.单列重分布，其他列带有过滤条件
--S1.设置保守策略，其他列带有简单表达式
set skew_option = lazy;
--S2.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c > 1;
--S3.设置激进策略，其他列带有简单表达式
set skew_option = normal;
--S4.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c > 1;
--S5.设置保守策略，其他列带有复杂表达式
set skew_option = lazy;
--S6.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and sqrt(s.c) < 10;
--S7.设置激进策略，其他列带有简单表达式
set skew_option = normal;
--S8.执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and sqrt(s.c) < 10;

--I6.单列重分布，双侧倾斜
--S1.双侧倾斜值不同。执行计划
explain (costs off) select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.c;
--S2.双侧倾斜值不同，执行查询
select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.c;
--S1.双侧倾斜值相同。执行计划
explain (costs off) select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.b;
--S2.双侧倾斜值相同，执行查询
select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.b;

--I7.删除表数据
drop schema skew_join cascade;
set query_dop = 2002;
