/*
################################################################################
# TESTCASE NAME : skew_hint_06.py 
# COMPONENT(S)  : skew hint功能测试: 重分布类型测试
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : skew hint
# TAG           : hint
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
--S1.设置schema
set current_schema = skew_hint;
--S2.关闭sort agg
set enable_sort = off;
set enable_nestloop = off;
--S3.关闭query下推
--S4.关闭enable_broadcast
set enable_broadcast = off;
--S5.设置计划格式
set explain_perf_mode=normal;
--S6.设置query_dop使得explain中倾斜优化生效
set query_dop = 1002;
--S7.设置倾斜优化策略
set skew_option=normal;

--I2. INT8OID
explain(verbose on, costs off) select t.col_bigint from typetest t, skew_t1 s where t.col_bigint = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_bigint) (1)) */ t.col_bigint from typetest t, skew_t1 s where t.col_bigint = s.c;

--I3. INT1OID
explain(verbose on, costs off) select t.col_tinyint from typetest t, skew_t1 s where t.col_tinyint = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_tinyint) (1)) */ t.col_tinyint from typetest t, skew_t1 s where t.col_tinyint = s.c;

--I4. INT2OID
explain(verbose on, costs off) select t.col_smallint from typetest t, skew_t1 s where t.col_smallint = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_smallint) (1)) */ t.col_smallint from typetest t, skew_t1 s where t.col_smallint = s.c;

--I5. INT4OID
explain(verbose on, costs off) select s.a from typetest t, skew_t1 s where t.col_integer = s.a;
explain(verbose on, costs off) select /*+ skew(s (a) (1)) */ s.a from typetest t, skew_t1 s where t.col_integer = s.a;

--I6. NUMERICOID
explain(verbose on, costs off) select t.col_numeric from typetest t, skew_t1 s where t.col_numeric = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_numeric) (564.322815379585)) */ t.col_numeric from typetest t, skew_t1 s where t.col_numeric = s.c;

--I7. CHAROID
explain(verbose on, costs off) select t.col_char2 from typetest t, char_t s where t.col_char2 = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_char2) ('F')) */ t.col_char2 from typetest t, char_t s where t.col_char2 = s.c;

--I8. BPCHAROID
explain(verbose on, costs off) select t.col_char from typetest t, bpchar_t s where t.col_char = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_char) ('test_agg_1')) */ t.col_char from typetest t, bpchar_t s where t.col_char = s.c;
--null字符串
explain(verbose on, costs off) select /*+ skew(t (col_char) ('null')) */ t.col_char from typetest t, bpchar_t s where t.col_char = s.c;

--I9. VARCHAROID
explain(verbose on, costs off) select t.col_varchar from typetest t, varchar_t s where t.col_varchar = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_varchar) ('vector_agg_3')) */ t.col_varchar from typetest t, varchar_t s where t.col_varchar = s.c;

--I10. NVARCHAR2OID
explain(verbose on, costs off) select t.col_varchar from typetest t, varchar_t s where t.col_char = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_varchar) ('vector_agg_3')) */ t.col_varchar from typetest t, varchar_t s where t.col_varchar = s.c;

--I11. DATEOID
explain(verbose on, costs off) select t.col_date from typetest t, date_t s where t.col_date = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_date) ('2005-02-14')) */ t.col_date from typetest t, date_t s where t.col_date = s.c;

--I12. TIMEOID
explain(verbose on, costs off) select t.col_time_without_time_zone from typetest t, time_t s where t.col_time_without_time_zone = s.c;
explain(verbose on, costs off) select /*+ skew(t (t.col_time_without_time_zone) ('08:20:12'))*/ t.col_time_without_time_zone from typetest t, time_t s where t.col_time_without_time_zone = s.c;

--I13. TIMESTAMPOID
explain(verbose on, costs off) select t.col_timestamp_without_timezone from typetest t, timestamp_t s where t.col_timestamp_without_timezone = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_timestamp_without_timezone) ('1997-02-10 17:32:01.4'))*/ t.col_timestamp_without_timezone from typetest t, timestamp_t s where t.col_timestamp_without_timezone = s.c;

--I14. TIMESTAMPTZOID
explain(verbose on, costs off) select t.col_timestamp_with_timezone from typetest t, timestampz_t s where t.col_timestamp_with_timezone = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_timestamp_with_timezone) ('1971-03-23 11:14:05+08'))*/ t.col_timestamp_with_timezone from typetest t, timestampz_t s where t.col_timestamp_with_timezone = s.c;

--I15. INTERVALOID
explain(verbose on, costs off) select t.col_interval from typetest t, interval_t s where t.col_interval = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_interval) ('@ 1 minute')) */ t.col_interval from typetest t, interval_t s where t.col_interval = s.c;

--I16. TIMETZOID
explain(verbose on, costs off) select t.col_time_with_time_zone from typetest t, timez_t s where t.col_time_with_time_zone = s.c;
explain(verbose on, costs off) select /*+ skew(t (t.col_time_with_time_zone) ('06:26:42+08'))*/ t.col_time_with_time_zone from typetest t, timez_t s where t.col_time_with_time_zone = s.c;

--I17. SMALLDATETIMEOID
explain(verbose on, costs off) select t.col_smalldatetime from typetest t, smalldatetime_t s where t.col_smalldatetime = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_smalldatetime) ('1997-02-02 03:04:00'))*/ t.col_smalldatetime  from typetest t, smalldatetime_t s where t.col_smalldatetime = s.c;

--I18. TEXTOID
explain(verbose on, costs off) select t.col_text from typetest t, text_t s where t.col_text = s.c;
-- string must use single_quote
explain(verbose on, costs off) select /*+ skew(t (col_text) (597b5b23f4aadf9513306bcd59afb6e4c9_3))*/ t.col_text from typetest t, text_t s where t.col_text = s.c;
explain(verbose on, costs off) select /*+ skew(t (col_text) ('597b5b23f4aadf9513306bcd59afb6e4c9_3'))*/ t.col_text from typetest t, text_t s where t.col_text = s.c;

--I19. NULL值 under DBCOMPATIBILITY 'ORA'
explain(verbose on, costs off) select t.col_integer from typetest t, null_t s where t.col_integer = s.c;
explain(verbose on, costs off) select /*+ skew(s (c) (''))*/ t.col_integer from typetest t, null_t s where t.col_integer = s.c;
explain(verbose on, costs off) select /*+ skew(s (c) (null))*/ t.col_integer from typetest t, null_t s where t.col_integer = s.c;
explain(verbose on, costs off) select /*+ skew(s (c) (NULL))*/ t.col_integer from typetest t, null_t s where t.col_integer = s.c;

--I20. Unicode for table identifier
create table U&"\0441\043B\043E\043D"(a int, b text) distribute by hash(a);
insert into U&"\0441\043B\043E\043D" values(generate_series(1,100),'乐观');
insert into U&"\0441\043B\043E\043D" values(generate_series(1,10),'坚强'); 
insert into U&"\0441\043B\043E\043D" values(generate_series(1,10),'勇敢');
analyze U&"\0441\043B\043E\043D";
analyze text_t;

explain(verbose on, costs off) select /*+ skew(U&"\0441\043B\043E\043D"(b)('乐观'))*/ * from U&"\0441\043B\043E\043D" ,text_t where text_t.c=U&"\0441\043B\043E\043D".b; 

--I20. Unicode for value string
delete from U&"\0441\043B\043E\043D";
insert into U&"\0441\043B\043E\043D" values(generate_series(1,10),U&'\0441\043B\043E\043D');
insert into U&"\0441\043B\043E\043D" values(generate_series(1,10),'слон');
analyze U&"\0441\043B\043E\043D";
select * from U&"\0441\043B\043E\043D" order by 1;
select * from слон order by 1;

explain(verbose on, costs off) select /*+ skew(U&"\0441\043B\043E\043D"(b)(U&'\0441\043B\043E\043D'))*/ * from U&"\0441\043B\043E\043D" ,text_t where text_t.c=U&"\0441\043B\043E\043D".b;
explain(verbose on, costs off) select /*+ skew(U&"\0441\043B\043E\043D"(b)('слон'))*/ * from U&"\0441\043B\043E\043D" ,text_t where text_t.c=U&"\0441\043B\043E\043D".b; 

--I21.修复指定hint时行数估算为负值
set enable_broadcast = off;
set explain_perf_mode = pretty;
drop table if exists t2,t3;
create table t2(c1 int, c2 char, c3 date)with(orientation=column);
create table t3(c1 int, c2 char, c3 date)with(orientation=column);
insert into t2 values(generate_series(1,1000),'a',date'2019-01-01');
insert into t3 values(generate_series(1,100),'a',date'2019-01-01');
analyze t2;
analyze t3;

explain select /*+ skew(t2(c3) ('2019-01-01'))*/ t3.c1, t2.c1 from t3 join t2 on t3.c3 = t2.c3;

drop table if exists t2,t3;

--I22.还原设置
--S1.还原query_dop
set query_dop = 2002;
