set enable_global_stats = true;
/*
 * This file is used to test the function of ExecVecResult()
 */
----
--- Create Table and Insert Data
----
create schema vector_result_engine;
set current_schema=vector_result_engine;

create table vector_result_engine.ROW_RESULT_TABLE_01
(
    col_int		int
   ,col_bint	bigint
   ,col_serial	bigint
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
   ,col_tinterval	tinterval
)distribute by hash(col_int);

create table vector_result_engine.VECTOR_RESULT_TABLE_01
(
    col_int		int
   ,col_bint	bigint
   ,col_serial	bigint
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
   ,col_tinterval	tinterval
)with(orientation = orc) tablespace hdfs_ts distribute by hash(col_int);

CREATE OR REPLACE PROCEDURE func_insert_tbl_result_01()
AS  
BEGIN  
	FOR I IN 1..10000 LOOP
		IF i > 0 AND i < 2000 then
			INSERT INTO vector_result_engine.row_result_table_01 VALUES(i, i * 2, i*10, 'aa','result_bb','result_MPPDBBJ',i+0.001,i+1.12,i+10.123,'1986-12-24','11:20:00','11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
		ELSIF i > 1999 AND i < 6000 then
			INSERT INTO vector_result_engine.row_result_table_01 VALUES(i, i * 3, i*5, 'dd','result_bb','result_MPPDBXA',i+0.111,i+2.34,i+10.008,'1996-06-08 10:11:15','02:15:01','02:15:01+04','4 day 13:24:56','["Sep 6, 1981 23:59:12" "Oct4, 1983 23:59:12"]');
		ELSE
			INSERT INTO vector_result_engine.row_result_table_01 VALUES(i, i * 1, i*12, 'hg','result_bb','result_MPPDBSZ',i+0.222,i+5.67,i+6.789,'2015-06-02','08:12:36','08:12:36+08','1 day 11:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1996 23:28:12"]');
		END IF;
	END LOOP; 
END;
/
CALL func_insert_tbl_result_01();

insert into vector_result_table_01 select * from row_result_table_01;

CREATE TABLE vector_result_engine.VECTOR_RESULT_TABLE_02(
    a1 character varying(1000),
    a2 integer,
    a3 character varying(1000),
    a4 integer,
    a5 character varying(1000),
    a6 integer,
    a7 character varying(1000),
    a8 integer,
    a9 character varying(1000),
    a10 integer
)
WITH (orientation = column)
DISTRIBUTE BY HASH (a4)
PARTITION BY RANGE (a2)
(
    PARTITION p1 VALUES LESS THAN (1),
    PARTITION p50001 VALUES LESS THAN (50001)
);

create table vector_result_engine.VECTOR_RESULT_TABLE_03
(
   a int
  ,b varchar(23)
)with(orientation = orc) tablespace hdfs_ts;

create table vector_result_engine.VECTOR_RESULT_TABLE_04
(
   a int
  ,b text
)with(orientation = orc) tablespace hdfs_ts;

insert into VECTOR_RESULT_TABLE_03 values(1,'tianjian');

create table vector_result_engine.ROW_RESULT_TABLE_05 
(
   c_int integer
  ,c_smallint smallint
  ,c_bigint bigint
  ,c_decimal decimal
  ,c_numeric numeric
  ,c_real real
  ,c_double double precision
  ,c_serial bigint
  ,c_bigserial bigint
  ,c_money money
  ,c_character_varying character varying(1123)
  ,c_varchar varchar(16678)
  ,c_char char(14675)
  ,c_text text
  ,c_bytea bytea
  ,c_timestamp_without  timestamp without time zone
  ,c_timestamp_with timestamp with time zone
  ,c_boolean boolean
  ,c_cidr cidr
  ,c_inet inet 
  ,c_bit bit(20)
  ,c_bit_varying bit varying(20)
  ,c_oid oid
  ,c_character character(10)
  ,c_interval interval
  ,c_date date
  ,c_time_without time without time zone
  ,c_time_with time with time zone
  ,c_binary_integer binary_integer
  ,c_binary_double binary_double
  ,c_dec dec(18,9)
  ,c_numeric_1 numeric(19,9)
  ,c_varchar2 varchar2
)distribute by replication;

create table vector_result_engine.VECTOR_RESULT_TABLE_05 with(orientation = orc) tablespace hdfs_ts distribute by hash(c_int) as select * from vector_result_engine.ROW_RESULT_TABLE_05;

analyze vector_result_table_01;
analyze vector_result_table_02;
analyze vector_result_table_03;
analyze vector_result_table_04;

----
--- case 1: Basic Case
----
explain (verbose on, costs off) select col_serial from vector_result_table_01 where current_date>'2015-02-14' order by  1 limit 10;
select col_serial from vector_result_table_01 where current_date>'2015-02-14' order by  1 limit 10;
select col_time+'00:00:20' from vector_result_table_01 where current_date < '2010-02-14' order by  1 limit 5;
select 'aa'  from vector_result_table_01  where current_date > '2015-02-14' and col_timetz = '08:12:36+08' limit 5;
select ctid, * from vector_result_table_01 where col_text is NULL;
select  col_timetz, ctid from vector_result_table_01 where current_date > '2015-02-14' and col_int < 5 order by 1, 2;
execute direct on(datanode1) 'select 2 from vector_result_engine.vector_result_table_01 where col_int > 500 limit 10';

----
--- case 2: With NULL
----
INSERT INTO vector_result_engine.row_result_table_01 VALUES(25, 252, 2525, NULL,'result_bb','result_MPPDBSZ',0.222,5.67,6.789,'2015-09-02',NULL,'08:12:36+08','1 day 11:24:56',NULL);
INSERT INTO vector_result_engine.row_result_table_01 VALUES(NULL,NULL,212525,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
delete from vector_result_table_01;
insert into vector_result_table_01 select * from row_result_table_01;
select *,ctid from vector_result_table_01 where col_vchar is NULL order by 1, 2, 3;
select col_date from vector_result_table_01 where current_date>'2015-02-14' and col_char is NULL order by 1;

----
--- case 3: Function Case
----
CREATE FUNCTION vec_result_func(int, bigint) RETURNS bigint
    AS 'select count(*) from vector_result_table_01 where col_int<$1 and col_bint<$2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
select * from vec_result_func(5,500);
drop function vec_result_func;

CREATE FUNCTION vec_result_func(int, bigint) RETURNS bigint
    AS 'select count(*) from vector_result_table_01 where col_int<$1'
    LANGUAGE SQL
    IMMUTABLE
	RETURNS NULL ON NULL INPUT;;
select * from vec_result_func(5,500);
drop function vec_result_func;
----
--- case 4: With Partition
----
SELECT a1, a2 FROM vector_result_table_02 WHERE a9='da' AND a9=' l' AND current_date>'2015-02-14'  ORDER BY a1, a2;

----
--- case 5: coerce transformation
----
insert into vector_result_table_04 select * from vector_result_table_03;
select * from vector_result_table_03;
select * from vector_result_table_04;

----
--- case 6: Test Vtimstamp_part
----
explain (costs off, verbose on) select distinct date_trunc('microsecon',col_date), date_trunc('millisecon',col_date) from vector_result_table_01 order by 1, 2;
select distinct date_trunc('microsecon',col_date), date_trunc('millisecon',col_date) from vector_result_table_01 order by 1, 2;
--select distinct date_part('seconds',col_timetz), date_part('min',col_timetz), date_part('hours',col_timetz) from vector_result_table_01 order by 1,2,3;
select distinct date_trunc('months',col_date), date_trunc('qtr',col_date), date_trunc('days',col_date) from vector_result_table_01 order by 1,2 ,3;
select distinct date_trunc('decades',col_date), date_trunc('weeks',col_date), date_trunc('years',col_date) from vector_result_table_01 order by 1,2,3;
select distinct date_trunc('millennia',col_date),date_trunc('centuries',col_date) from vector_result_table_01 order by 1,2;
select distinct date_trunc('hours',col_time), date_trunc('minute',col_interval) from vector_result_table_01 order by 1, 2;
--select distinct date_part('timezone_h',col_timetz) from vector_result_table_01 order by 1;
--select distinct date_part('timezone_m',col_timetz) from vector_result_table_01 order by 1;
--select distinct date_part('timezone',col_timetz) from vector_result_table_01 order by 1;
select date_trunc('microsecon',col_date), date_trunc('millisecon',col_date) from vector_result_table_01 where col_num > 1998 and col_float < 2015 order by 1,2;
--select distinct date_part('timezone_h',col_timetz) from vector_result_table_01 where col_char > 'aa' and col_timetz < '11:20:22+06' order by 1;

----
--- Test table_skewness function
----
create table test(id int);
select table_skewness('test');
drop table test;

----
--- Clean Table and Resource
----
drop schema vector_result_engine cascade;
