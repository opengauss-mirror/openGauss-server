--
-- This is a test plan for Extension Connector.
-- Contents:
-- 
-- 	* Part-1: Abnormal scene
-- 	* Part-2: Data Type of MPPDB
-- 	* Part-3: Query Plan
-- 	* Part-4: Query with local and remote tables
-- 	* Part-5: Test SQL: DDL/DML/DQL/TEXT
--

----
--- Prepare Public Objects: User, Data Source, Table, etc.
----

-- create a user to be connected
create user ploken identified by 'Gs@123456';

-- create a data source for connection
create data source myself options (dsn 'myself');

-- create a table for test
create table test_tbl (c1 int);
insert into test_tbl values (119);
insert into test_tbl values (119);
insert into test_tbl values (911);

-- grant
grant select on table test_tbl to ploken;

----
--- Part-1: Test Abnormal scene 
----

-- create test table
create table s1_tbl_001 (c1 int);
create table s1_tbl_002 (c1 bool, c2 int, c3 float8, c4 text, c5 numeric(15,5), c6 varchar2(20));
create table s1_tbl_003 (c1 int, c2 blob, c3 bytea);
insert into s1_tbl_002 values (true, 119, 1234.56, '@ploken@', 987654321.01234567, '##ploken##'); 
insert into s1_tbl_002 values (false, 119, 1234.56, '@ploken@', 987654321.01234567, '##ploken##'); 
grant select on table s1_tbl_002 to ploken;
grant select on table s1_tbl_003 to ploken;

-- create test data source
create data source myself1 options (dsn 'myself', username '', password '');
create data source myself2 type 'MPPDB' version 'V100R007C10' options (dsn 'myself', username 'ploken', password 'Gs@123456', encoding 'utf8');
create data source myself3 options (dsn 'myself', encoding 'utf99');

-- Data Source missing
select * from exec_on_extension('', 'select * from test_tbl') as (c1 int);

-- DSN missing
select * from exec_hadoop_sql('', 'select * from test_tbl', '') as (c1 int);

-- SQL missing
select * from exec_on_extension('myself', '') as (c1 int);
select * from exec_hadoop_sql('myself', '', '') as (c1 int);

-- Data Source not found
select * from exec_on_extension('IAmInvalidDataSource', 'select * from test_tbl') as (c1 int);

-- Target Tbl missing
select * from exec_on_extension('myself', 'select * from test_tbl');

-- No Privileges on the table
select * from exec_on_extension('myself', 'select * from s1_tbl_001') as (c1 int);

-- Invalid SQL
select * from exec_on_extension('myself', 'select * from ') as (c1 int);

-- Number(Col_Target_Tbl) > Number(Col_RETSET)
select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int, c2 int);

-- Unsupported Data Type in Target Tbl
select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 blob);

-- Unsupported Data Type in RETSET
select * from exec_on_extension('myself', 'select * from s1_tbl_003') as (c1 int, c2 text, c3 text);

-- Unsupported Encoding Method
select * from exec_on_extension('myself3', 'select * from test_tbl') as (c1 int);

-- Read Full Options
select * from exec_on_extension('myself2', 'select * from test_tbl') as (c1 int);
select * from exec_hadoop_sql('myself', 'select * from test_tbl', 'utf8') as (c1 int);

-- Read Empty Options
select * from exec_on_extension('myself1', 'select * from test_tbl') as (c1 int);
select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int);

-- Done
revoke select on table s1_tbl_002 from ploken;
revoke select on table s1_tbl_003 from ploken;
drop table s1_tbl_001;
drop table s1_tbl_002;
drop table s1_tbl_003;
drop data source myself1;
drop data source myself2;
drop data source myself3;

----
--- Part-2: Test Data Types of MPPDB
----

-- create numeric table
create table t1_row (c1 tinyint, c2 smallint, c3 integer, c4 bigint, c5 float4, c6 float8, c7 numeric(38,25), c8 numeric(38), c9 boolean); 
insert into t1_row values (255, 32767, 2147483647, 9223372036854775807, 123456789.123456789, 12345678901234567890.1234567890123456789, 1234567890123.1234567890123456789012345, 12345678901234567890123456789012345678, true);
insert into t1_row values (0, -32768, -2147483648, -9223372036854775808, -123456789.123456789, -12345678901234567890.1234567890123456789, -1234567890123.1234567890123456789012345, -12345678901234567890123456789012345678, false);
create table t1_col with(orientation=column) as select * from t1_row;

-- create char table
create table t2_row (c1 char, c2 char(20), c3 varchar, c4 varchar(20), c5 varchar2, c6 varchar2(20), c7 nchar, c8 nchar(20), c9 nvarchar2, c10 nvarchar2(20), c11 text);
insert into t2_row values ('@', '+ploken+ char', 'S', '+ploken+ varchar', 'H', '+ploken+ varchar2', 'E', '+ploken+ nchar', 'N', '+ploken+ nvarchar2', '+ploken+ text');
insert into t2_row values (':', '+ploken+ char', '#', '+ploken+ varchar', '?', '+ploken+ varchar2', '&', '+ploken+ nchar', '%', '+ploken+ nvarchar2', '+ploken+ text');
create table t2_col with(orientation=column) as select * from t2_row;

-- create date table
create table t3_row (c1 date, c2 timestamp(6), c3 timestamp(6) with time zone, c5 interval year(6), c6 interval month(6), c7 interval day(6), c8 interval hour(6), c9 interval minute(6), c10 interval second(6), c11 interval day to hour, c12 interval day to minute, c13 interval day to second(6), c14 interval hour to minute, c15 interval hour to second(6), c16 interval minute to second(6)); 
insert into t3_row values (date '2012-12-12', timestamp '2012-12-12 12:12:12.999999', timestamp '2012-12-12 12:12:12.999999 pst', interval '12' year, interval '12' month, interval '12' day, interval '12' hour, interval '12' minute, interval '12' second, interval '3 12' day to hour, interval '3 12:12' day to minute, interval '3 12:12:12.999999' day to second, interval '3:12' hour to minute, interval '3:12:12.999999' hour to second, interval '3:12.999999' minute to second);
insert into t3_row select * from t3_row;
insert into t3_row select * from t3_row;
create table t3_col with(orientation=column) as select * from t3_row;

-- grant select on table
grant select on table t1_row to ploken;
grant select on table t2_row to ploken;
grant select on table t3_row to ploken;
grant select on table t1_col to ploken;
grant select on table t2_col to ploken;
grant select on table t3_col to ploken;

-- exec_on_extension
select * from pgxc_wlm_ec_operator_statistics;
select * from pgxc_wlm_ec_operator_history;
select * from pgxc_wlm_ec_operator_info;
SET resource_track_cost TO 1;
SET resource_track_level TO 'operator';
SET resource_track_duration TO '0s';
select * from exec_on_extension('myself', 'select * from t1_row') as (c1 tinyint, c2 smallint, c3 integer, c4 bigint, c5 float4, c6 float8, c7 numeric(38,25), c8 numeric(38), c9 boolean);
select plan_node_id,tuple_processed,ec_status,ec_dsn,ec_query,ec_libodbc_type from pgxc_wlm_ec_operator_history;
SET resource_track_cost TO 100000;
SET resource_track_level TO 'query';
SET resource_track_duration TO '1min';
select * from exec_on_extension('myself', 'select * from t2_row') as (c1 char, c2 char(20), c3 varchar, c4 varchar(20), c5 varchar2, c6 varchar2(20), c7 nchar, c8 nchar(20), c9 nvarchar2, c10 nvarchar2(20), c11 text);
select * from exec_on_extension('myself', 'select * from t2_row') as (c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text, c10 text, c11 text);
select * from exec_on_extension('myself', 'select * from t3_row') as (c1 date, c2 timestamp(6), c3 timestamp(6) with time zone, c5 interval year(6), c6 interval month(6), c7 interval day(6), c8 interval hour(6), c9 interval minute(6), c10 interval second(6), c11 interval day to hour, c12 interval day to minute, c13 interval day to second(6), c14 interval hour to minute, c15 interval hour to second(6), c16 interval minute to second(6));
 
select * from exec_on_extension('myself', 'select * from t1_col') as (c1 tinyint, c2 smallint, c3 integer, c4 bigint, c5 float4, c6 float8, c7 numeric(38,25), c8 numeric(38), c9 boolean);
select * from exec_on_extension('myself', 'select * from t2_col') as (c1 char, c2 char(20), c3 varchar, c4 varchar(20), c5 varchar2, c6 varchar2(20), c7 nchar, c8 nchar(20), c9 nvarchar2, c10 nvarchar2(20), c11 text);
select * from exec_on_extension('myself', 'select * from t2_col') as (c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text, c10 text, c11 text);
select * from exec_on_extension('myself', 'select * from t3_col') as (c1 date, c2 timestamp(6), c3 timestamp(6) with time zone, c5 interval year(6), c6 interval month(6), c7 interval day(6), c8 interval hour(6), c9 interval minute(6), c10 interval second(6), c11 interval day to hour, c12 interval day to minute, c13 interval day to second(6), c14 interval hour to minute, c15 interval hour to second(6), c16 interval minute to second(6)); 

-- exec_hadoop_sql
select * from exec_hadoop_sql('myself', 'select * from t1_row', '') as (c1 tinyint, c2 smallint, c3 integer, c4 bigint, c5 float4, c6 float8, c7 numeric(38,25), c8 numeric(38), c9 boolean);
select * from exec_hadoop_sql('myself', 'select * from t2_row', '') as (c1 char, c2 char(20), c3 varchar, c4 varchar(20), c5 varchar2, c6 varchar2(20), c7 nchar, c8 nchar(20), c9 nvarchar2, c10 nvarchar2(20), c11 text);
select * from exec_hadoop_sql('myself', 'select * from t2_row', '') as (c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text, c10 text, c11 text);
select * from exec_hadoop_sql('myself', 'select * from t3_row', '') as (c1 date, c2 timestamp(6), c3 timestamp(6) with time zone, c5 interval year(6), c6 interval month(6), c7 interval day(6), c8 interval hour(6), c9 interval minute(6), c10 interval second(6), c11 interval day to hour, c12 interval day to minute, c13 interval day to second(6), c14 interval hour to minute, c15 interval hour to second(6), c16 interval minute to second(6));
 
select * from exec_hadoop_sql('myself', 'select * from t1_col', '') as (c1 tinyint, c2 smallint, c3 integer, c4 bigint, c5 float4, c6 float8, c7 numeric(38,25), c8 numeric(38), c9 boolean);
select * from exec_hadoop_sql('myself', 'select * from t2_col', '') as (c1 char, c2 char(20), c3 varchar, c4 varchar(20), c5 varchar2, c6 varchar2(20), c7 nchar, c8 nchar(20), c9 nvarchar2, c10 nvarchar2(20), c11 text);
select * from exec_hadoop_sql('myself', 'select * from t2_col', '') as (c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text, c10 text, c11 text);
select * from exec_hadoop_sql('myself', 'select * from t3_col', '') as (c1 date, c2 timestamp(6), c3 timestamp(6) with time zone, c5 interval year(6), c6 interval month(6), c7 interval day(6), c8 interval hour(6), c9 interval minute(6), c10 interval second(6), c11 interval day to hour, c12 interval day to minute, c13 interval day to second(6), c14 interval hour to minute, c15 interval hour to second(6), c16 interval minute to second(6)); 

-- Done
revoke select on table t1_row from ploken;
revoke select on table t2_row from ploken;
revoke select on table t3_row from ploken;
revoke select on table t1_col from ploken;
revoke select on table t2_col from ploken;
revoke select on table t3_col from ploken;
drop table t1_row;
drop table t2_row;
drop table t3_row;
drop table t1_col;
drop table t2_col;
drop table t3_col;

----
--- Part-3: Test Plan
----

-- explain select
explain (costs off) select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int);
explain (costs off) select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int);

-- explain create table
explain (analyze, costs off, timing off) create table s3_tbl_001 as select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int);
explain (analyze, costs off, timing off) create table s3_tbl_002 as select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int);
explain (analyze, costs off, timing off) create table s3_tbl_003 with (orientation=column) as select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int);
explain (analyze, costs off, timing off) create table s3_tbl_004 with (orientation=column) as select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int);

-- explain insert into
create table s3_tbl_005 (c1 int);
explain (costs off) insert into s3_tbl_005 select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int);
explain (costs off) insert into s3_tbl_005 select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int);

-- explain local_row + remote
explain (costs off) select * from test_tbl inner join (select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b on test_tbl.c1=b.c1;
explain (costs off) select * from test_tbl inner join (select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b on test_tbl.c1=b.c1;

-- explain local_col + remote
create table test_tbl_col with (orientation=column) as select * from test_tbl;
explain (costs off) select * from test_tbl_col inner join (select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b on test_tbl_col.c1=b.c1;
explain (costs off) select * from test_tbl_col inner join (select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b on test_tbl_col.c1=b.c1;

-- two exec_on_extension
explain (costs off) select * from 
	test_tbl, 
	(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int));
-- two exec_hadoop_sql
explain (costs off) select * from 
	test_tbl, 
	(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int));
-- exec_on_extension + exec_hadoop_sql
explain (costs off) select * from 
	test_tbl, 
	(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int));
-- exec_hadoop_sql + exec_on_extension
explain (costs off) select * from 
	test_tbl, 
	(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int));

-- Done
drop table s3_tbl_001, s3_tbl_002, s3_tbl_003, s3_tbl_004, s3_tbl_005;

----
--- Part-4: Test Query with local and remote tables
----

-- local_row + remote
select * from test_tbl inner join (select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b on test_tbl.c1=b.c1 order by 1,2;
select * from test_tbl inner join (select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b on test_tbl.c1=b.c1 order by 1,2;

-- local_col + remote
create table  s4_tbl_001 with (orientation=column) as select * from test_tbl;
select * from s4_tbl_001 inner join (select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b on s4_tbl_001.c1=b.c1 order by 1,2;
select * from s4_tbl_001 inner join (select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b on s4_tbl_001.c1=b.c1 order by 1,2;

-- two exec_on_extension
select * from 
	test_tbl, 
	(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) order by 1,2;
-- two exec_hadoop_sql
select * from 
	test_tbl, 
	(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) order by 1,2;
-- exec_on_extension + exec_hadoop_sql
select * from 
	test_tbl, 
	(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) order by 1,2;
-- exec_hadoop_sql + exec_on_extension
select * from 
	test_tbl, 
	(select * from exec_hadoop_sql('myself', 'select * from test_tbl', '') as (c1 int)) b 
where
	test_tbl.c1 = b.c1 and
	b.c1 in
		(select * from exec_on_extension('myself', 'select * from test_tbl') as (c1 int)) order by 1,2;
-- complex query
create table s4_tbl_002 (c1 int, c2 text, c3 timestamp(6), c4 bool, c5 float8);
insert into s4_tbl_002 values (1, 'ploken_line_1',  '2012-12-10 12:12:11.123456', true,  1234561.1234561);
insert into s4_tbl_002 values (2, 'ploken_line_1',  '2012-12-11 12:12:12.123456', false, 1234562.1234562);
insert into s4_tbl_002 values (3, 'ploken_line_2',  '2012-12-12 12:12:13.123456', true,  1234563.1234563);
insert into s4_tbl_002 values (4, 'ploken_line_2',  '2012-12-14 12:12:14.123456', false, 1234564.1234564);
insert into s4_tbl_002 values (5, 'ploken_line_3',  '2012-12-15 12:12:15.123456', true,  1234565.1234565);
insert into s4_tbl_002 values (6, 'ploken_line_3',  '2012-12-16 12:12:16.123456', false, 1234566.1234566);
insert into s4_tbl_002 values (1, 'ploken_line_1',  '2012-12-10 12:12:11.123456', true,  1234561.1234561);
insert into s4_tbl_002 values (2, 'ploken_line_1',  '2012-12-11 12:12:12.123456', false, 1234562.1234562);
insert into s4_tbl_002 values (3, 'ploken_line_2',  '2012-12-12 12:12:13.123456', true,  1234563.1234563);
insert into s4_tbl_002 values (4, 'ploken_line_2',  '2012-12-14 12:12:14.123456', false, 1234564.1234564);
insert into s4_tbl_002 values (5, 'ploken_line_3',  '2012-12-15 12:12:15.123456', true,  1234565.1234565);
insert into s4_tbl_002 values (6, 'ploken_line_3',  '2012-12-16 12:12:16.123456', false, 1234566.1234566);
grant select on table s4_tbl_002 to ploken;

select 
	a.c2, avg(b.c5) as avg5
from 
	s4_tbl_002 a,
	(select * from exec_on_extension('myself', 'select * from s4_tbl_002') as (c1 int, c2 text, c3 timestamp(6), c4 bool, c5 float8)) b
where
	a.c1 = b.c1 
	and b.c4 = true
	and exists (
		select t.c3 from exec_on_extension('myself', 'select c3 from s4_tbl_002') as t(c3 timestamp(6)) 
		where t.c3 > '2012-12-12 12:12:12.199999'::timestamp(6) and t.c3 > a.c3
	)
group by 
	a.c2
order by 
	avg5 desc;

-- Done
revoke select on table s4_tbl_002 from ploken;
drop table s4_tbl_001, s4_tbl_002;

----
--- Part-5: Test DDL/DML/DQL/TEXT
----

-- exec_on_extension
select * from exec_on_extension('myself', 'create table ploken_new_tbl (c1 int, c2 text)') as (c1 text);
select * from exec_on_extension('myself', 'select * from ploken_new_tbl') as (c1 int, c2 text);

select * from exec_on_extension('myself', 'insert into ploken_new_tbl values (911, ''exec_on_extension_old'')') as (c1 text);
select * from exec_on_extension('myself', 'select * from ploken_new_tbl') as (c1 int, c2 text);

select * from exec_on_extension('myself', 'update ploken_new_tbl set c2=''exec_on_extension_new'' where c1>100') as (c1 text);
select * from exec_on_extension('myself', 'select * from ploken_new_tbl') as (c1 int, c2 text);

select * from exec_on_extension('myself', 'drop table ploken_new_tbl') as (c1 text);
select * from exec_on_extension('myself', 'select * from ploken_new_tbl') as (c1 int, c2 text);

select * from exec_on_extension('myself', 'show listen_addresses') as (c1 text);

-- exec_hadoop_sql
select * from exec_hadoop_sql('myself', 'create table ploken_new_tbl (c1 int, c2 text)', '') as (c1 text);
select * from exec_hadoop_sql('myself', 'select * from ploken_new_tbl', '') as (c1 int, c2 text);

select * from exec_hadoop_sql('myself', 'insert into ploken_new_tbl values (911, ''exec_hadoop_sql_old'')', '') as (c1 text);
select * from exec_hadoop_sql('myself', 'select * from ploken_new_tbl', '') as (c1 int, c2 text);

select * from exec_hadoop_sql('myself', 'update ploken_new_tbl set c2=''exec_hadoop_sql_new'' where c1>100', '') as (c1 text);
select * from exec_hadoop_sql('myself', 'select * from ploken_new_tbl', '') as (c1 int, c2 text);

select * from exec_hadoop_sql('myself', 'drop table ploken_new_tbl', '') as (c1 text);
select * from exec_hadoop_sql('myself', 'select * from ploken_new_tbl', '') as (c1 int, c2 text);

select * from exec_hadoop_sql('myself', 'show listen_addresses', '') as (c1 text);

----
--- End: Drop public objects
----
revoke select on table test_tbl from ploken;
revoke usage on data source myself from ploken;
drop table test_tbl;
