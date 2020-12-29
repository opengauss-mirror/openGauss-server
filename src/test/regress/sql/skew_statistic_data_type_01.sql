/*
################################################################################
# TESTCASE NAME : skew_statistic_data_type_01.py 
# COMPONENT(S)  : 测试各种类型使用单列统计信息
# PREREQUISITE  : setup.py
# PLATFORM      : all
# DESCRIPTION   : skew type
# TAG           : type
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

--I2.倾斜值为int1
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int1 = ut_int1;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int1 = ut_int1;

--I3.倾斜值为int2
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int2 = ut_int2;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int2 = ut_int2;

--I4.倾斜值为int4
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int4 = ut_int4;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int4 = ut_int4;

--I5.倾斜值为int8
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int8 = ut_int8;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int8 = ut_int8;

--I6.倾斜值为numeric
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_num = ut_num;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_num = ut_num;

--I7.倾斜值为char
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_char = ut_char;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_char = ut_char;

--I8.倾斜值为varchar
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_varchar = ut_varchar;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_varchar = ut_varchar;

--I9.倾斜值为text
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_text = ut_text;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_text = ut_text;

set query_dop = 2002;
