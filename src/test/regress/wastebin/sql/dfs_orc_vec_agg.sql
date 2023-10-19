set enable_global_stats = true;
/*
 * This file is used to test the function of ExecVecAggregation()
 */
----
--- Create Table and Insert Data
----
create schema vector_agg_engine;
set current_schema=vector_agg_engine;
set time zone prc;
set time zone prc;
set datestyle to iso;

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
) 
distribute by hash(col_integer);

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
) with (orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_integer);

CREATE OR REPLACE PROCEDURE func_insert_tbl_agg_01()
AS  
BEGIN  
	FOR I IN 1..6000 LOOP
		if I < 2000 then
			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 111111, 23, i + 10.001, 561.322815379585 + i, 20857435000485996218310931968699256001981133222933850071857221402.725428507532497489821939560364033880 + i * 2,8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-03-03 11:04', 56 + i, '2005-02-14');
		elsif i > 1999 AND i < 2488 then
			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 121111, 45, i + 9.08, 27.25684426652 + i * 0.001, 20857434798277339938397404472048722532796412222119506033298219314.596941867590737379779439339277062225 + i, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'T', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1996-06-12 03:06', 56 + i, '2008-02-14');
		else
			INSERT INTO vector_agg_engine.row_agg_table_01 VALUES(1, i, 111131, 2345, i + 2.047, 39.2456977995 + i * 0.3, 20857434796839002905636223150710041116810786801730952028511523795.100678976382813790191855282491921088 + i * 1.5, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1992-02-06 03:08', 56 + i, '2015-02-14');
		end if;
	END LOOP; 
END;
/
CALL func_insert_tbl_agg_01();

insert into vector_agg_table_01 select * from row_agg_table_01;

create table vector_agg_engine.VECTOR_AGG_TABLE_02
(  col_int	int
  ,col_num	numeric
  ,col_bint	bigint
)with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int);

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

create table vector_agg_engine.ROW_AGG_TABLE_03
( 
   col_smallint smallint null
  ,col_integer	integer default 23423
  ,col_bigint 	bigint default 923423432
  ,col_real 	real
  ,col_numeric 	numeric(18,12) null
  ,col_serial	bigint
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
)
distribute by hash(col_integer)
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

CREATE OR REPLACE PROCEDURE func_insert_tbl_agg_03()
AS  
BEGIN  
	FOR I IN 1..1000 LOOP
		if I < 200 then
			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(1, i, 111111, i + 10.001, 561.322815379585 + i, 1112 + i * 3, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '08:20:12', '06:26:42+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2005-02-14');
		elsif i > 199 AND i < 248 then
			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(4, i + 2000, 121111, i + 2009.08, 27.25684426652 + i * 0.001, 25467 + i * 2, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '14:21:56', '15:12:22+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2008-02-14');
		else
			INSERT INTO vector_agg_engine.row_agg_table_03 VALUES(7, i + 4000, 111131, i + 4002.047, 39.2456977995 + i * 0.3, 3658742 + i, 8885.169 - i * 0.125, 61032419811910588 + i, 'test_agg_'||i, 'F', 'vector_agg_'||i, '597b5b23f4aadf9513306bcd59afb6e4c9_'||i, 'beijing_agg'||i, '19:07:24', '22:32:36+08', 'Mon Feb 10 17:32:01.4 1997 PST', '1971-03-23 11:14:05', '1997-02-02 03:04', 56 + i, '2015-02-14');
		end if;
	END LOOP; 
END;
/
CALL func_insert_tbl_agg_03();

create table vector_agg_engine.VECTOR_AGG_TABLE_03
( 
   col_smallint smallint null
  ,col_integer	integer default 23423
  ,col_bigint 	bigint default 923423432
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
) with (orientation = column, max_batchrow = 10000) 
distribute by hash(col_integer)
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

insert into VECTOR_AGG_TABLE_03 select * from ROW_AGG_TABLE_03;

create table vector_agg_engine.VECTOR_AGG_TABLE_04
(
   col_id	integer
  ,col_place	varchar
)with (orientation = orc)
tablespace hdfs_ts;

COPY VECTOR_AGG_TABLE_04(col_id, col_place) FROM stdin;
12	xian
\N	xian
\N	tianshui
\N	tianshui
6	beijing
6	beijing
4	beijing
8	beijing
\.

create table vector_agg_engine.VECTOR_AGG_TABLE_05
(
   col_id	integer
  ,col_place	varchar
)with (orientation = orc)
tablespace hdfs_ts;

COPY VECTOR_AGG_TABLE_05(col_id, col_place) FROM stdin;
12	tian
\N	tian
\N	xian
\N	tshui
6	beijing
6	beijing
4	beijing
8	beijing
\.

create table vector_agg_engine.VECTOR_AGG_TABLE_06
(
   col_int int
  ,col_int2 int8
  ,col_char char(20)
  ,col_varchar varchar(30)
  ,col_date date
  ,col_num numeric(10,2)
  ,col_num2 numeric(10,4)
  ,col_float float4
  ,col_float2 float8
)WITH (orientation = orc) 
tablespace hdfs_ts
distribute by hash (col_int);

analyze vector_agg_table_01;
analyze vector_agg_table_02;
analyze vector_agg_table_03;
analyze vector_agg_table_04;
analyze vector_agg_table_05;
analyze vector_agg_table_06;

----
--- case 1: Basic Test Without NULL (HashAgg || SortAgg)
----
explain (verbose on, costs off) select count(*), sum(col_integer), avg(col_integer), sum(col_integer)::float8/count(*), col_bigint from vector_agg_table_01 group by col_bigint order by col_bigint;

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


----
--- cas4: Test Agg With NULL Table
----
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06;
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06 group by col_num;

select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06;
select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06 group by col_date;

select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06;
select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06 group by col_int;

select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06;
select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06 group by col_num2;

select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06;
select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06 group by col_num2;


select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06;
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06 group by col_num;

select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06;
select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06 group by col_date;

select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06;
select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06 group by col_int;

select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06;
select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06 group by col_num2;

select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06;
select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06 group by col_num2;


---
---
explain select count(distinct vector_agg_table_04.*) from vector_agg_table_04;
select count(distinct vector_agg_table_04.*) from vector_agg_table_04; --A.*
select count(vector_agg_engine.vector_agg_table_04.*) from vector_agg_engine.vector_agg_table_04; --A.B.*
select count(regression.vector_agg_engine.vector_agg_table_04.*) from regression.vector_agg_engine.vector_agg_table_04; --A.B.C.*
select count(distinct AA.*) from (select a.*, b.* from vector_agg_table_04 a, vector_agg_table_05 b where a.col_id = b.col_id) AA;

----
--- depend on lineitem_vec
----
select sum(L_QUANTITY) a ,l_returnflag from vector_engine.lineitem_vec group by L_returnflag order by a;
select avg(L_QUANTITY) a, sum(l_quantity) b , l_returnflag from vector_engine.lineitem_vec group by L_returnflag order by a;

explain (verbose on, costs off) 
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	sum_qty
;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	sum_qty
;

explain (verbose on, costs off) 
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;

---
---
explain (verbose on, costs off)  
select col_smallint ,'',col_numeric2,0,'',0,1,0,0,'' ,col_varchar2 from vector_agg_table_01  where col_integer=2 group by 1,2,3,4,5,8,9,10,11;
select col_smallint ,'',col_numeric2,0,'',0,1,0,0,'' ,col_varchar2 from vector_agg_table_01  where col_integer=2 group by 1,2,3,4,5,8,9,10,11;

select col_smallint,'','' from vector_agg_table_01  where col_integer=2 group by 1,2,3;
explain (verbose on, costs off)
select col_smallint,col_smallint from vector_agg_table_01  where col_integer=2 group by 1,2;
select col_smallint,col_smallint from vector_agg_table_01  where col_integer=2 group by 1,2;
explain (verbose on, costs off)
select col_smallint,'',col_smallint from vector_agg_table_01  where col_integer=2 group by 1,3;
select col_smallint,'',col_smallint from vector_agg_table_01  where col_integer=2 group by 1,3;
----
--- Clean Resource and Tables
----
drop schema vector_agg_engine cascade;

set enable_hashagg = off;
create schema vec;
set current_schema to vec; 
--text sort agg and plain agg
create table aggt_col(a numeric, b int, c varchar(10))with(orientation = orc) tablespace hdfs_ts;
insert into aggt_col values(1, 1, 'abc'), (1, 1, 'abc'), (1, 1, 'abc'),(1, 2, 'efg'), (1, 3, 'hij');
select * from aggt_col order by 1, 2, 3;
select count(distinct a) from aggt_col;
select count(distinct b) from aggt_col;
select count(distinct c) from aggt_col;

select avg(distinct a) from aggt_col;
select avg(distinct b) from aggt_col;

select a, sum(a), avg(distinct a),count(distinct b), sum(b)  from aggt_col group by a;
select a, count(distinct b), max(c) from aggt_col group by a;
select a, count(distinct b), sum(b)  from aggt_col group by a;

delete from aggt_col;

create table aggt_row(a numeric, b int, c varchar(10));
insert into aggt_row values(1, generate_series(1, 2200), 'agg'||generate_series(1, 2200));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
select a, count(distinct b),  count(distinct c) from aggt_col group by a;
insert into aggt_col values(1, 2200, 'agg2200');
select a, count(distinct b),  count(distinct c) from aggt_col group by a;
insert into aggt_col values(2, 2200, 'agg2200');
select a, count(distinct b),  count(distinct c) from aggt_col group by a  order by 1;

delete from aggt_col;

create table aggt_row(a numeric, b int, c varchar(10));
insert into aggt_row values(generate_series(1, 2200), generate_series(1, 2200), 'agg'||generate_series(1, 2200));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
select a, count(distinct b),  count(distinct c) from aggt_col group by a order by 1;
insert into aggt_col values(1, 2201, 'agg2201'), (1, 2202, 'agg2202'), (1, 2203, 'agg2203');
select a, count(distinct b),  count(distinct c) from aggt_col group by a order by 1;
insert into aggt_col values(2, 2, 'agg2'), (2, 2, 'agg2'), (2, 2, 'agg2');
select a, count(distinct b),  count(distinct c) from aggt_col group by a  order by 1;


drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation = orc) tablespace hdfs_ts;

create table aggt_row(a numeric, b int, c varchar(10), d int);
insert into aggt_row values(1, 1, 'agg1', generate_series(1, 1000));
insert into aggt_row values(1, 2, 'agg2', generate_series(1, 500));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c), sum(a), sum(b), max(c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c), sum(a), sum(b), max(c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b), sum(a), sum(b)  from  aggt_col group by c order by 1;

drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation = orc) tablespace hdfs_ts;

create table aggt_row(a numeric, b int, c varchar(10), d int);
insert into aggt_row values(1, 1, 'agg1', generate_series(1, 1500));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
select sum(a) as sum_value, d from aggt_col group by d  having sum_value < 0;

drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation = orc) tablespace hdfs_ts;

create table aggt_row(a numeric, b int, c varchar(10), d int);
insert into aggt_row values(1, 1, 'agg1', generate_series(1, 2000));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
insert into aggt_col values(1, 2, 'agg2');
insert into aggt_col values(1, 3, 'agg3');
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
insert into aggt_col values(10, 20, 'agg30');
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;

delete from aggt_col;
create table aggt_row(a numeric, b int, c varchar(10), d int);
insert into aggt_row values(1, generate_series(1, 500), 'agg'||generate_series(1, 500));
insert into aggt_row values(2, generate_series(1, 1500), 'agg'||generate_series(1, 1500));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
analyze aggt_col;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c), max(b), max(c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c), max(b), max(c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b), max(b), max(c) from  aggt_col group by c order by 1;

delete from aggt_col;
create table aggt_row(a numeric, b int, c varchar(10), d int);
insert into aggt_row values(2, generate_series(1, 1010), 'agg'||generate_series(1, 1010));
insert into aggt_row values(3, generate_series(1, 800), 'agg'||generate_series(1, 800));
insert into aggt_col select * from aggt_row;
drop table aggt_row;
--insert into aggt_col values(4, generate_series(1, 800), 'agg'||generate_series(1, 800));
--insert into aggt_col values(5, generate_series(1, 800), 'agg'||generate_series(1, 800));
--insert into aggt_col values(6, generate_series(1, 800), 'agg'||generate_series(1, 800));
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;


create table aggt_col_numeric(a int, b numeric(10,0), c varchar(10), d int)with(orientation = orc) tablespace hdfs_ts;

create table aggt_row_numeric(a int, b numeric(10,0), c varchar(10), d int);
insert into aggt_row_numeric values(1, 100, 'aggtest', generate_series(1, 2200));
insert into aggt_col_numeric select * from aggt_row_numeric;
drop table aggt_row_numeric;
select count(distinct b) from aggt_col_numeric;
select count(distinct c) from aggt_col_numeric;
select count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
select sum(b), avg(b), count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
explain (costs off) select sum(b), avg(b), count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
drop table aggt_col;
drop table aggt_col_numeric;

create table t1_agg_col(a int, b timetz, c tinterval, d interval, e int)with(orientation = orc) tablespace hdfs_ts;
create table t1_agg_row(a int, b timetz, c tinterval, d interval, e int);
insert into t1_agg_row values(1, '10:11:12', '["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]', 1, generate_series(1, 1000));
insert into t1_agg_col select * from t1_agg_row;
drop table t1_agg_row;
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(1, '10:11:12', '["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]', 1);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(1, '10:11:13', '["Feb 10, 1957 23:59:12" "Jan 14, 1973 03:14:21"]', 2);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(2, '10:11:13', '["Feb 10, 1957 23:59:12" "Jan 14, 1973 03:14:21"]', 2);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a order by 1;
select count(distinct b), 1 as col1, 'plain agg' as col2 from t1_agg_col order by 1;;
drop table t1_agg_col;
drop schema vec;
reset enable_hashagg;
