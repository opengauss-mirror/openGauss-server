/*
 * This file is used to test the function of ExecVecAggregation()---(1)
 */
----
--- Create Table and Insert Data
----
create schema vector_agg_engine;
set current_schema=vector_agg_engine;
set time zone prc;
set time zone prc;
set datestyle to iso;

\parallel on  5
create table vector_agg_engine.ROW_AGG_TABLE_01
( 
   col_smallint smallint null
  ,col_integer	integer default 23423
  ,col_bigint 	bigint default 923423432
  ,col_oid		Oid
  ,col_real 	real
  ,col_numeric 	numeric(18,12) null
  ,col_numeric2 numeric null
  ,col_double_precision double precision
  ,col_decimal 			decimal(19) default 923423423
  ,col_char 	char(57) null
  ,col_char2 	char default '0'
  ,col_varchar 	varchar(19)
  ,col_text text 	null
  ,col_varchar2 	varchar2(20)
  ,col_time_without_time_zone	time without time zone null
  ,col_time_with_time_zone 		time with time zone
  ,col_timestamp_without_timezone	timestamp
  ,col_timestamp_with_timezone 	timestamptz
  ,col_smalldatetime	smalldatetime
  ,col_money			money
  ,col_date date
) distribute by hash(col_integer);

create table vector_agg_engine.VECTOR_AGG_TABLE_01
( 
   col_smallint smallint null
  ,col_integer	integer default 23423
  ,col_bigint 	bigint default 923423432
  ,col_oid		Oid
  ,col_real 	real
  ,col_numeric 	numeric(18,12) null
  ,col_numeric2 numeric null
  ,col_double_precision double precision
  ,col_decimal 			decimal(19) default 923423423
  ,col_char 		char(57) null
  ,col_char2 		char default '0'
  ,col_varchar 		varchar(19)
  ,col_text text 	null
  ,col_varchar2 	varchar2(25)
  ,col_time_without_time_zone		time without time zone null
  ,col_time_with_time_zone 			time with time zone
  ,col_timestamp_without_timezone	timestamp
  ,col_timestamp_with_timezone 		timestamptz
  ,col_smalldatetime	smalldatetime
  ,col_money			money
  ,col_date date
) with (orientation=column) distribute by hash(col_integer);

create table vector_agg_engine.VECTOR_AGG_TABLE_02
(  col_int  int
  ,col_num  numeric
  ,col_bint bigint
)with(orientation = column) distribute by hash(col_int);

create table vector_agg_engine.ROW_AGG_TABLE_03
( 
   col_smallint smallint null
  ,col_integer  integer default 23423
  ,col_bigint   bigint default 923423432
  ,col_real   real
  ,col_numeric  numeric(18,12) null
  ,col_serial int 
  ,col_double_precision double precision
  ,col_decimal      decimal(19) default 923423423
  ,col_char     char(57) null
  ,col_char2    char default '0'
  ,col_varchar    varchar(19)
  ,col_text text  null
  ,col_varchar2   varchar2(25)
  ,col_time_without_time_zone   time without time zone null
  ,col_time_with_time_zone      time with time zone
  ,col_timestamp_without_timezone timestamp
  ,col_timestamp_with_timezone    timestamptz
  ,col_smalldatetime  smalldatetime
  ,col_money      money
  ,col_date date
) with (orientation=column) distribute by hash(col_integer)
partition by range(col_numeric, col_date, col_integer)(
partition partition_p1 values less than(3,'2002-02-04 00:01:00',20),
partition partition_p2 values less than(21,'2005-03-26 00:00:00',1061) ,
partition partition_p3 values less than(121,'2005-06-01 20:00:00',1600),
partition partition_p4 values less than(121,'2006-08-01 20:00:00',1987) ,
partition partition_p5 values less than(1456,'2007-12-03 10:00:00',2567),
partition partition_p6 values less than(2678,'2008-02-03 11:01:34',2800),
partition partition_p7 values less than(3601,'2008-02-13 01:01:34',3801),
partition partition_p8 values less than(3601,'2012-04-18 23:01:44',4560),
partition partition_p9 values less than(4500,'2012-06-18 23:01:44',4900),
partition partition_p10 values less than(9845,'2016-06-28 23:21:44',6200)) ;

create table vector_agg_engine.VECTOR_AGG_TABLE_03
( 
   col_smallint smallint null
  ,col_integer  integer default 23423
  ,col_bigint   bigint default 923423432
  ,col_real   real
  ,col_numeric  numeric(18,12) null
  ,col_numeric2 numeric null
  ,col_double_precision double precision
  ,col_decimal      decimal(19) default 923423423
  ,col_char     char(57) null
  ,col_char2    char default '0'
  ,col_varchar    varchar(19)
  ,col_text text  null
  ,col_varchar2   varchar2(25)
  ,col_time_without_time_zone   time without time zone null
  ,col_time_with_time_zone      time with time zone
  ,col_timestamp_without_timezone timestamp
  ,col_timestamp_with_timezone    timestamptz
  ,col_smalldatetime  smalldatetime
  ,col_money      money
  ,col_date date
) with (orientation=column) distribute by hash(col_integer)
partition by range(col_numeric, col_date, col_integer)(
partition partition_p1 values less than(3,'2002-02-04 00:01:00',20),
partition partition_p2 values less than(21,'2005-03-26 00:00:00',1061) ,
partition partition_p3 values less than(121,'2005-06-01 20:00:00',1600),
partition partition_p4 values less than(121,'2006-08-01 20:00:00',1987) ,
partition partition_p5 values less than(1456,'2007-12-03 10:00:00',2567),
partition partition_p6 values less than(2678,'2008-02-03 11:01:34',2800),
partition partition_p7 values less than(3601,'2008-02-13 01:01:34',3801),
partition partition_p8 values less than(3601,'2012-04-18 23:01:44',4560),
partition partition_p9 values less than(4500,'2012-06-18 23:01:44',4900),
partition partition_p10 values less than(9845,'2016-06-28 23:21:44',6200)) ;

\parallel off


--CREATE OR REPLACE PROCEDURE func_insert_tbl_agg_01()
--AS  
--BEGIN  
--	FOR I IN 1..6000 LOOP
--		if I < 2000 then
--			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 111111, 23, i + 10.001, 561.322815379585 + i, 20857435000485996218310931968699256001981133222933850071857221402.725428507532497489821939560364033880 + i * 2,8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-03-03 11:04', 56 + i, '2005-02-14');
--		elsif i > 1999 AND i < 2488 then
--			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 121111, 45, i + 9.08, 27.25684426652 + i * 0.001, 20857434798277339938397404472048722532796412222119506033298219314.596941867590737379779439339277062225 + i, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'T', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1996-06-12 03:06', 56 + i, '2008-02-14');
--		else
--			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 111131, 2345, i + 2.047, 39.2456977995 + i * 0.3, 20857434796839002905636223150710041116810786801730952028511523795.100678976382813790191855282491921088 + i * 1.5, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1992-02-06 03:08', 56 + i, '2015-02-14');
--		end if;
--	END LOOP; 
--END;
--/
--CALL func_insert_tbl_agg_01();

create table src(a int);
insert into src values(1);

create table t1(a int);
insert into t1 select generate_series(1,1999) from src;
insert into vector_agg_engine.row_agg_table_01 select 1, i.a, 111111, 23, i.a + 10.001, 561.322815379585 + i.a, 20857435000485996218310931968699256001981133222933850071857221402.725428507532497489821939560364033880 + i.a * 2,8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-03-03 11:04', 56 + i.a, '2005-02-14' from t1 i;

create table t2(a int);
insert into t2 select generate_series(2000,2487) from src;
insert into vector_agg_engine.row_agg_table_01 select  1, i.a, 121111, 45, i.a + 9.08, 27.25684426652 + i.a * 0.001, 20857434798277339938397404472048722532796412222119506033298219314.596941867590737379779439339277062225 + i.a, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'T', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1996-06-12 03:06', 56 + i.a, '2008-02-14' from t2 i;

create table t3(a int);
insert into t3 select generate_series(2488, 6000) from src;
insert into vector_agg_engine.row_agg_table_01 select 1, i.a, 111131, 2345, i.a + 2.047, 39.2456977995 + i.a * 0.3, 20857434796839002905636223150710041116810786801730952028511523795.100678976382813790191855282491921088 + i.a * 1.5, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1992-02-06 03:08', 56 + i.a, '2015-02-14' from t3 i;

drop table t1;
drop table t2;
drop table t3;

insert into  vector_agg_table_01 select * from row_agg_table_01;


COPY VECTOR_AGG_TABLE_02(col_int, col_num, col_bint) FROM stdin;
1	1.2	111111
4	2.1	111111
8	3.6	121111
2	2.4	111131
9	4.2	111111
3	1.6	111131
5	21.7	121111
3	5.6	111111
\.

--CREATE OR REPLACE PROCEDURE func_insert_tbl_agg_03()
--AS  
--BEGIN  
--	FOR I IN 1..1000 LOOP
--		if I < 200 then
--			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(1, i, 111111, i + 10.001, 561.322815379585 + i, 1112 + i * 3, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2005-02-14');
--		elsif i > 199 AND i < 248 then
--			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(4, i + 2000, 121111, i + 2009.08, 27.25684426652 + i * 0.001, 25467 + i * 2, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2008-02-14');
--		else
--			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(7, i + 4000, 111131, i + 4002.047, 39.2456977995 + i * 0.3, 3658742 + i, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2015-02-14');
--		end if;
--	END LOOP; 
--END;
--/
--CALL func_insert_tbl_agg_03();

create table t1(a int);
insert into t1 select generate_series(1,199) from src;
insert into vector_agg_engine.row_agg_table_03 select 1, i.a, 111111, i.a + 10.001, 561.322815379585 + i.a, 1112 + i.a * 3, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i.a, '2005-02-14' from t1 i;

create table t2(a int);
insert into t2 select generate_series(200,247) from src;
insert into vector_agg_engine.row_agg_table_03 select 4, i.a + 2000, 121111, i.a + 2009.08, 27.25684426652 + i.a * 0.001, 25467 + i.a * 2, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i.a, '2008-02-14' from t2 i;

create table t3(a int);
insert into t3 select generate_series(248,1000) from src;
insert into vector_agg_engine.row_agg_table_03 select 7, i.a + 4000, 111131, i.a + 4002.047, 39.2456977995 + i.a * 0.3, 3658742 + i.a, 8885.169 - i.a * 0.125, 61032419811910588 + i.a, 'test_agg_'||i.a, 'F', 'vector_agg_'||i.a, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i.a, 'beijing_agg'||i.a, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i.a, '2015-02-14' from t3 i;

insert into VECTOR_AGG_TABLE_03 select * from ROW_AGG_TABLE_03;

drop table t1;
drop table t2;
drop table t3;
drop table src;
 
analyze vector_agg_table_01;
analyze vector_agg_table_02;
analyze vector_agg_table_03;


--test max/min optimization
explain (costs off)
select
max(col_smallint), min(col_smallint), 
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric)
from vector_agg_engine.VECTOR_AGG_TABLE_01;

select
max(col_smallint), min(col_smallint), 
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric)
from vector_agg_engine.VECTOR_AGG_TABLE_01;

explain (costs off)
select
max(col_smallint), min(col_smallint), 
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric)
from vector_agg_engine.VECTOR_AGG_TABLE_03;

select
max(col_smallint), min(col_smallint), 
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric)
from vector_agg_engine.VECTOR_AGG_TABLE_03;

drop table if exists minmax_test;
create table minmax_test
(
 l_tint tinyint default 255,
 l_num1 numeric(18,0)
)with(orientation=column,compression=low) distribute by hash(l_tint);

insert into minmax_test values(generate_series(1,2));
select min(l_num1) from minmax_test;

----
--- case 1: Basic Test Without NULL (HashAgg || SortAgg)
----
--explain (verbose on, costs off) select count(*), sum(col_integer), avg(col_integer), sum(col_integer)::float8/count(*), col_bigint from vector_agg_table_01 group by col_bigint order by col_bigint;

select count(*), sum(col_integer), avg(col_integer), sum(col_integer)::float8/count(*), col_bigint from vector_agg_table_01 group by col_bigint order by col_bigint;
select count(*), sum(A.col_integer * B.col_int), avg(A.col_integer * B.col_int), sum(A.col_integer * B.col_int)::float8/count(*), A.col_bigint, B.col_bint from vector_agg_table_01 A full outer join vector_agg_table_02 B on A.col_bigint = B.col_bint group by A.col_bigint, B.col_bint order by A.col_bigint, B.col_bint;
select sum(y) from (select sum(col_integer) y, col_bigint%2 x from vector_agg_table_01 group by col_bigint) q1 group by x order by x;
select col_bigint - 1, col_bigint + 1, sum(col_integer), col_bigint - 3, avg(col_integer),  col_bigint%2, min(col_integer) from vector_agg_table_01 group by col_bigint order by col_bigint;
select col_integer from vector_agg_table_01 group by col_integer order by col_integer limit 10;
select col_integer + col_numeric from vector_agg_table_01 group by col_integer + col_numeric order by col_integer + col_numeric limit 10 offset 100;
select A.col_integer + B.col_bint, A.col_integer, B.col_bint from vector_agg_table_01 A, vector_agg_table_02 B where A.col_bigint = B.col_bint group by A.col_integer, B.col_bint order by 1, 2 limit 30;
select A.col_integer + B.col_bint from vector_agg_table_01 A, vector_agg_table_02 B where A.col_bigint = B.col_bint group by A.col_integer + B.col_bint order by 1 limit 30;
select count(*) + sum(col_integer) + avg(col_integer), col_bigint from vector_agg_table_01 group by col_bigint order by col_bigint;
select col_numeric, avg(col_numeric) from vector_agg_table_01 group by col_numeric order by col_numeric limit 50;
select col_double_precision, avg(col_double_precision) from vector_agg_table_01 group by col_double_precision order by col_double_precision limit 50;
select col_decimal, avg(col_decimal) from vector_agg_table_01 group by col_decimal order by col_decimal limit 50;
select sum(col_money), min(col_money),max(col_money) from vector_agg_table_01 limit 50;
select sum(col_money), min(col_money),max(col_money) from vector_agg_table_01 group by col_time_without_time_zone order by 1;
select A.col_money + B.col_money from vector_agg_table_01 A, vector_agg_table_03 B order by 1 limit 10;
select min(col_oid), max(col_oid) from vector_agg_table_01;
select min(col_smalldatetime), max(col_smalldatetime) from vector_agg_table_01;
select min(col_smalldatetime), max(col_smalldatetime) from vector_agg_table_01 group by col_smalldatetime order by 1, 2;
select avg(col_bigint) + sum(col_numeric / 2.4) from vector_agg_table_01 group by col_integer, col_char2 order by 1 limit 10;
select sum(col_bint), avg(col_num - 0.99999999999999999999999), avg(col_num * 1.1) from vector_agg_table_02 group by col_int order by 1, 2, 3 limit 10;
select sum(col_bint), count(*) from vector_agg_table_02 group by col_int order by 1, 2 limit 10;
select sum((col_bigint + 8::bigint) * 2::bigint - 7::bigint), col_integer from vector_agg_table_03 where col_real > 4890.00 group by col_integer order by col_integer;
set enable_codegen=off;
select sum((col_bigint + 8::bigint) * 2::bigint - 7::bigint), col_integer from vector_agg_table_03 where col_real > 4890.00 group by col_integer order by col_integer;
set enable_codegen=on;

----
--- case 2: Basic Test with NULL
----
delete from vector_agg_table_01;

INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, 12, NULL, 23, 12.021, 515.343815379585, NULL, 8875.15, 61032419811910575, 'test_agg_0', 'T', 'vector_agg_0', '597b5b23f4abcf9513306bcd59afb6e4c9_0', 'beijing_agg_0', '17:25:28', NULL, NULL, '1971-03-23 11:14:05', '1997-02-02 03:04', 79.3, '2005-02-25');
INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(3, NULL, 114531, NULL,  2.047, NULL, 20857434796839002905636223150710041116810786801730952028511513795.100678976382813790191855282491921068, 8885.169, NULL, 'test_agg_7002', 'T', 'vector_agg_7002', '597b5b23f4aadf9473306bcd59afb6e4c9', 'beijing_agg_7002', '16:19:25', NULL, 'Mon Jan 15 17:32:01.4 1997 PST', NULL, '1997-02-02 03:04', 21.6, '2005-06-12');
INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, 15, 152161, 2355, NULL, 227.25684426652, 18857434798277339938397404472048722532796412222119506033298219325.596941867590737379779439339277062225, NULL, 61032419811910588, 'test_agg_0', 'F', 'vector_agg_0', '597b5b23f4aadf9513306bcd59afb6e4c9_0', 'beijing_agg_0', NULL, '14:32:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', NULL, 56, NULL);
INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(NULL, 12, 121111, 25, 9.08, 27.25684426652, 20857434798277339938397404472048722532796412222119506033298219314.596941867590737379779439339277062225, 8885.169, 61032419811910588, NULL, 'T', NULL, NULL, 'beijing_agg_8010', '17:09:24', NULL, 'Mon Feb 10 17:32:01.4 1997 PST', NULL, '1997-02-02 03:04', 56, '2005-02-14');
INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
insert into vector_agg_table_01 select * from row_agg_table_01;

select avg(col_integer), sum(col_integer), count(*), col_bigint from vector_agg_table_01 group by col_bigint order by col_bigint;
select col_integer,  sum(col_integer),sum(col_bigint), col_integer from vector_agg_table_01  group by 1,4 order by 1,2,3,4 limit 5;
select col_char2 from vector_agg_table_01 group by col_char2 order by col_char2;
select col_char2, count(1) from vector_agg_table_01 group by col_char2 order by col_char2;
select col_char2, count(col_char2) from vector_agg_table_01 group by col_char2 order by col_char2;
select count(*) from vector_agg_table_01 where col_char2 is null group by col_char2;
select max(col_smallint), min(col_smallint) from vector_agg_table_01 where col_smallint is not null;
select min(col_numeric), max(col_numeric), sum(col_numeric), avg(col_numeric) from vector_agg_table_01 group by col_smallint order by col_smallint;
select min(col_decimal), max(col_decimal), sum(col_decimal), avg(col_decimal) from vector_agg_table_01 group by col_date order by col_date;
select max(col_time_without_time_zone), min(col_time_without_time_zone) from vector_agg_table_01 where col_time_without_time_zone is not null;
select max(col_time_with_time_zone), min(col_time_with_time_zone) from vector_agg_table_01 where col_time_with_time_zone is not null;
select max(col_timestamp_without_timezone), min(col_timestamp_without_timezone) from vector_agg_table_01 where col_timestamp_without_timezone is not null;
select max(col_timestamp_with_timezone), min(col_timestamp_with_timezone) from vector_agg_table_01 where col_timestamp_with_timezone is not null;
select sum(col_time_without_time_zone), avg(col_time_without_time_zone) from vector_agg_table_01;
select sum(col_time_without_time_zone), avg(col_time_without_time_zone) from vector_agg_table_01 group by col_bigint order by 1, 2;
select sum(col_time_without_time_zone), avg(col_time_without_time_zone) from vector_agg_table_01 group by col_time_without_time_zone order by 1, 2;

----
--- case 3: Basic Test with Partition
----
select count(*), sum(length(col_varchar2)), avg(col_numeric) from vector_agg_table_03 where 'DAsdf;redis' = 'DAsdf;redis' and col_smallint != 4 or col_integer%148=1 and col_varchar != 'vector_agg_500' or col_numeric < 1239.5 and col_smallint in (1,5,7) order by 1,2 limit 5;
select count(*), null as "hnull", sum(col_integer) as c_int_sum, avg(col_numeric) as c_num_avg, sum(col_real) as c_real_sum, avg(col_double_precision) as c_dp_avg, sum(col_decimal) as c_dec_num from vector_agg_table_03 where col_integer <= 201 and col_smallint >= 1 or col_bigint < 10200 and col_smallint != 7 and col_smallint != 4;

delete from vector_agg_engine.VECTOR_AGG_TABLE_03 where col_smallint = 4;
select
max(col_smallint), min(col_smallint),
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric)
from vector_agg_engine.VECTOR_AGG_TABLE_03;

select
max(col_smallint), min(col_smallint), 10 as col1,
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric), 200 as col2
from vector_agg_engine.VECTOR_AGG_TABLE_03;

delete from vector_agg_engine.VECTOR_AGG_TABLE_03;
select
max(col_smallint), min(col_smallint), 10 as col1,
max(col_timestamp_without_timezone), min(col_timestamp_without_timezone),
max(col_numeric), min(col_numeric), 200 as col2
from vector_agg_engine.VECTOR_AGG_TABLE_03;

----
--- Clean Resource and Tables
----
drop schema vector_agg_engine cascade;
