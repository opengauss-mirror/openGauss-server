/*
################################################################################
# TESTCASE NAME : skew_statistic_multi_col.py 
# COMPONENT(S)  : 测试多列分布键的倾斜场景
# PREREQUISITE  : setup.py
# PLATFORM      : all
# DESCRIPTION   : multi columns' skew
# TAG           : multi columns
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

--I2.多列重分布，无多列统计信息
--S1.多列重分布，无多列统计信息，多值倾斜，带有NULL值，执行计划
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c;
--S2.多列重分布，无多列统计信息，多值倾斜，带有NULL值，执行查询
select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c;
--S3.多列重分布，无多列统计信息，单值双侧倾斜，倾斜值不同，执行计划
explain (costs off) select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.b and s.c = s1.c and s.c is not null;


--I3.多列重分布，无多列统计信息，倾斜列带有过滤条件
--S1.倾斜列的某一列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.b > 1;
--S2.倾斜列的某一列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.c is null;
--S3.倾斜列的某一列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.c is not null;
--S4.倾斜列的某几列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.b = 1 and s.c = 1;
--S5.倾斜列的某几列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.a != 1 and s.b = 1 and s.c = 1;
--S6.倾斜列的某几列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b + s.c > 0;
--S7.倾斜列的某几列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b + s.c < 1;

--I4.收集多列统计信息
--S1.设置参数
set default_statistics_target = -2;
--S2.收集统计信息
analyze skew_scol(b, c);
analyze skew_scol1(b, c);

--I5.多列重分布，带多列统计信息，多值倾斜，带有NULL值
--S1.执行计划，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c;
--S2.执行查询
select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c;

explain (costs off) select count(*) from skew_scol s, skew_scol1 s1 where s.b = s1.b and s.c = s1.c;

--I6.多列重分布，带多列统计信息，倾斜列带有过滤条件
--S1.倾斜列的某一列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.b > 1;
--S2.倾斜列的某一列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.c is null;
--S3.倾斜列的某一列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.c is not null;
--S4.倾斜列的某几列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.b = 1 and s.c = 1;
--S5.倾斜列的某几列上带有简单表达式，不生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.c = t.c and s.a != 1 and s.b = 1 and s.c = 1;
--S6.倾斜列的某几列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b + s.c > 0;
--S7.倾斜列的某几列上带有简单表达式，生成倾斜优化
explain (costs off) select count(*) from skew_scol s, test_scol t where s.b = t.b and s.b + s.c < 1;

set query_dop = 2002;
