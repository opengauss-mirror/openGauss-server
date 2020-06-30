create schema stats;
set current_schema=stats;
create table fetch_stat_within_new_xid(f1 int, f2 float, f3 text);
insert into fetch_stat_within_new_xid select generate_series(1,1000), 10.0, repeat('Gauss Database,Niubility!!',2);
analyze fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
vacuum fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
-- test the results of two VACUUM FULL actions
vacuum full fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
vacuum full fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
-- test the results of two VACUUM FULL ANALYZE actions
vacuum full analyze fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
vacuum full analyze fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';
-- test the results of VACUUM FULL and VACUUM FULL ANALYZE
vacuum full fetch_stat_within_new_xid;
vacuum full analyze fetch_stat_within_new_xid;
select relname, relpages, reltuples, relfilenode > 0 from pg_class where relname='fetch_stat_within_new_xid';

set datestyle = 'iso, mdy';

create table string_eq_int(a int, b text, c varchar(3), d char(3), e char(10));
insert into string_eq_int values(generate_series(1, 1000), '100', '100', '100', '100');

analyze string_eq_int;
set explain_perf_mode = pretty;

explain select * from string_eq_int where b = 100;
explain select * from string_eq_int where c = 100;
explain select * from string_eq_int where d = 100;
explain select * from string_eq_int where e = 100;

create table string_eq_timestamp(a int, b text, c varchar(30), d char(30), e date);
insert into string_eq_timestamp values(generate_series(1, 500), '2012-12-16 10:11:15' , '2012-12-16 10:11:15', '2012-12-16 10:11:15', '2012-12-16');
insert into string_eq_timestamp values(generate_series(501, 1000), NULL, NULL, NULL, NULL);
analyze string_eq_timestamp;
explain select * from string_eq_timestamp where b = '2012-12-16 10:11:15'::timestamp;
explain select * from string_eq_timestamp where b <> '2012-12-16 10:11:15'::timestamp;
explain select * from string_eq_timestamp where c = '2012-12-16 10:11:15'::timestamp;
explain select * from string_eq_timestamp where '2012-12-16 10:11:15'::timestamp <> c;
explain select * from string_eq_timestamp where d = '2012-12-16 10:11:15'::timestamp;
set datestyle = 'Postgres, MDY';
explain select * from string_eq_timestamp where c = '2012-12-16 10:11:15'::timestamp;

explain (costs off, nodes off)
select * from string_eq_timestamp where e=2012-12-16
union
select * from string_eq_timestamp where e=2012-12-16
union
select * from string_eq_timestamp where e=2012-12-16
union
select * from string_eq_timestamp where e=2012-12-16
union
select * from string_eq_timestamp where e=2012-12-16
union
select * from string_eq_timestamp where e=2012-12-16;

reset datestyle;
reset explain_perf_mode;

--test for ANALYZE for table in new nodegroup
create node group analyze_group with (datanode1, datanode2);
create table test_analyze_group(a int) distribute by hash(a) to group analyze_group;
insert into test_analyze_group values(1);
create node group "$a" with (datanode1, datanode2);
create table test6 (val int) distribute by hash(val) to group "$a";
insert into test6 values(1);
analyze test_analyze_group;
analyze test6;
set default_statistics_target=-2;
analyze test_analyze_group;
analyze test6;
reset default_statistics_target;
drop table test_analyze_group;
drop table test6;
drop node group analyze_group;
drop node group "$a";

-- test the results of ANALYZE replication temp table
create table replication_temp_test(id int);
insert into replication_temp_test select generate_series(1,100);
create temp table replication_temp_table distribute by replication as select * from replication_temp_test;
select count(*) from pg_stats where tablename = 'replication_temp_table';
analyze replication_temp_table;
select count(*) from pg_stats where tablename = 'replication_temp_table';

-- test the results of VACUUM ANALYZE temp table
create temp table temp_table(a int, b int);
insert into temp_table values(generate_series(1,100),generate_series(1,100));
select count(*) from pg_stats where tablename = 'temp_table';
vacuum analyze temp_table;
select count(*) from pg_stats where tablename = 'temp_table';
drop table temp_table;
set default_statistics_target = -10;
create temp table temp_table(a int, b int);
insert into temp_table values(generate_series(1,100),generate_series(1,100));
select count(*) from pg_stats where tablename = 'temp_table';
vacuum analyze temp_table;
select count(*) from pg_stats where tablename = 'temp_table';
reset default_statistics_target;

-- extrapolation stats
set enable_extrapolation_stats=on;
create table src_table(a int);
insert into src_table values(1);
-- all mcv
create table ex_table(a int, b date);
insert into ex_table select generate_series(1,20), '2018-03-06'::date+generate_series(1,20) from src_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
analyze ex_table;
explain select count(*) from ex_table where b='2018-03-25';
explain select count(*) from ex_table where b='2019-03-26';
explain select count(*) from ex_table where b>='2019-03-26';
explain select count(*) from ex_table where b>'2019-03-26';
drop table ex_table;

-- all histogram
create table ex_table(a int, b date);
insert into ex_table select generate_series(1,1000), '2018-03-06'::date+generate_series(1,1000) from src_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
analyze ex_table;
explain select count(*) from ex_table where b='2018-03-25';
explain select count(*) from ex_table where b='2039-03-26';
explain select count(*) from ex_table where b>='2039-03-26';
explain select count(*) from ex_table where b>'2039-03-26';
drop table ex_table;

-- some mcv, some histogram
create table ex_table(a int, b date);
insert into ex_table select generate_series(1,20), '2018-03-06'::date+generate_series(1,20) from src_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select generate_series(1,1000), '2019-03-26'::date+generate_series(1,1000) from src_table;
analyze ex_table;
explain select count(*) from ex_table where b='2018-03-25';
explain select count(*) from ex_table where b='2039-03-26';
explain select count(*) from ex_table where b>='2039-03-26';
explain select count(*) from ex_table where b>'2039-03-26';
drop table ex_table;

-- other data type
create table ex_table(a int, c timestamp, d timestamptz, e time, f smalldatetime);
insert into ex_table select generate_series(1,20), '2018-03-06'::date+generate_series(1,20),
 '2018-03-06'::date+generate_series(1,20), '2018-03-06'::date+generate_series(1,20),
 '2018-03-06'::date+generate_series(1,20) from src_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
insert into ex_table select * from ex_table;
analyze ex_table;
explain select count(*) from ex_table where c='2018-03-25';
explain select count(*) from ex_table where c='2019-03-26';
explain select count(*) from ex_table where c>'2019-03-26';
explain select count(*) from ex_table where d='2018-03-25';
explain select count(*) from ex_table where d='2019-03-26';
explain select count(*) from ex_table where d>'2019-03-26';
explain select count(*) from ex_table where e='00:00:00';
explain select count(*) from ex_table where e='08:00:00';
explain select count(*) from ex_table where e>'08:00:00';
explain select count(*) from ex_table where f='2018-03-25';
explain select count(*) from ex_table where f='2019-03-26';
explain select count(*) from ex_table where f>'2019-03-26';
drop table ex_table;

create table test(a int,b text);
insert into test values(1,rpad('数据倾斜测试',800,'数据倾斜优化要优化'));
analyze test;
select * from pg_stats where tablename = 'test';
drop table test;

reset enable_extrapolation_stats;
drop schema stats cascade;
