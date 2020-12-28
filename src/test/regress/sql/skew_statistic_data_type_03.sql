/*
################################################################################
# TESTCASE NAME : skew_statistic_data_type_03.py 
# COMPONENT(S)  : 测试多分布列使用单列统计信息
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

--I2.倾斜值为int1, int2
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int1 = ut_int1 and st_int2 = ut_int2;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int1 = ut_int1 and st_int2 = ut_int2;

--I3.倾斜值为int4, int8
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_int4 = ut_int4 and st_int8 = ut_int8;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_int4 = ut_int4 and st_int8 = ut_int8;

--I4.倾斜值为numeric, char
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_num = ut_num and st_char = ut_char;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_num = ut_num and st_char = ut_char;

--I5.倾斜值为varchar, text
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_varchar = ut_varchar and st_text = ut_text;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_varchar = ut_varchar and st_text = ut_text;

--I6.倾斜值为date, time
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_date = ut_date and st_time = ut_time;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_date = ut_date and st_time = ut_time;

--I7.倾斜值为time with time zone, timestamp
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_timez = ut_timez and st_timestamp = ut_timestamp;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_timez = ut_timez and st_timestamp = ut_timestamp;

--I8.倾斜值为timestamp with time zone, smalldatetime
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_timestampz = ut_timestampz and st_smalldatetime = ut_smalldatetime;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_timestampz = ut_timestampz and st_smalldatetime = ut_smalldatetime;

--I9.倾斜值为interval, smalldatetime
--S1.执行计划
explain (costs off) select count(*) from skew_type, unskew_type where st_interval = ut_interval and st_interval = ut_interval;
--S2.执行查询
select count(*) from skew_type, unskew_type where st_interval = ut_interval and st_interval = ut_interval;

set query_dop = 2002;
