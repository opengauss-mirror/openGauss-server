/*
################################################################################
# TESTCASE NAME : skew_statistic_data_type_01.py 
# COMPONENT(S)  : 测试各种类型使用单列统计信息
# PREREQUISITE  : skew_hint_setup.py
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

--I2.倾斜值为date
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_date = ut_date;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_date = ut_date;

--I3.倾斜值为time
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_time = ut_time;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_time = ut_time;

--I4.倾斜值为time with time zone
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_timez = ut_timez;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_timez = ut_timez;

--I5.倾斜值为timestamp
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_timestamp = ut_timestamp;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_timestamp = ut_timestamp;

--I6.倾斜值为timestamp with time zone
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_timestampz = ut_timestampz;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_timestampz = ut_timestampz;

--I7.倾斜值为smalldatetime
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_smalldatetime = ut_smalldatetime;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_smalldatetime = ut_smalldatetime;

--I8.倾斜值为interval
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_interval = ut_interval;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_interval = ut_interval;

set query_dop = 2002;
