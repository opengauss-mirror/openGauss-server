set enable_global_stats = true;
/*
 * This file is used to test the function of ExecVecSort()
 */
----
--- Create Table and Insert Data
----
create schema vector_sort_engine;
set current_schema=vector_sort_engine;

create table vector_sort_engine.VECTOR_SORT_TABLE_01
(
   col_int		int
  ,col_bint		bigint
  ,col_real		real
  ,col_float	float8
  ,col_dp		double precision
  ,col_num		numeric(5,2)
  ,col_decimal	decimal(15,2)
  ,col_char		char
  ,col_char2	char(10)
  ,col_vchar	varchar(45)
  ,col_time		time
  ,col_date		date
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tv		tinterval
) with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int);

COPY VECTOR_SORT_TABLE_01(col_int, col_bint, col_real, col_float, col_dp, col_num, col_decimal, col_char, col_char2, col_vchar, col_time, col_date, col_interval, col_timetz, col_tv) FROM stdin;
1	155894	2.28	2.5	3.65	3.24	0.02	N	TOMO	DELIVER IN PERSON	17:00:05	1996-03-22	2 day 13:34:56	1984-2-6 01:00:30+8	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]
21	2875	\N	0.2	1.8	3.26	0.12	F	TFGO	TAKE BACK RETURN	\N	1997-01-21	1 day 14:45:26	1986-2-6 03:00:30+8	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
15	17895	5.14	3.2	0.23	1.27	0.31	T	HGBF	AIR IN ROAD	08:02:30	1986-02-20	1 day 13:34:56	1987-2-6 08:00:30+8	["epoch" "Mon May  1 00:30:30 1995"]
21	25642	2.48	3.6	1.78	0.05	1.02	C	LKGJD	MAIL	12:00:12	1987-08-12	18 day 14:34:56	1989-2-6 06:00:30+8	["Feb 15 1990 12:15:03" "2001-09-23 11:12:13"]
18	\N	2.1	0.62	5.12	0.8	1.01	\N	KLDJA	TRAIN	15:00:21	1997-08-12	18 day 15:34:56	1990-2-6 12:00:30+8	\N
64	251	2.64	2.1	3.654	1.25	0.05	G	KIdja	MAIL	15:00:30	1986-12-24	7 day 16:34:56	2002-2-6 00:00:30+8	["-infinity" "infinity"]
\N	546	6.34	1.8	3.64	0.2	0.01	T	Pirm	DELIVER IN PERSON	16:21:12	1985-05-25	22 day 13:34:56	1984-2-6 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
48	128795	1.36	2.5	0.36	1.87	0.01	F	lomme	RAIL	18:00:20	1987-05-02	\N	1984-02-07 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
42	15	0.28	1.52	2.36	0.1	0.36	F	\N	MAIL	09:00:20	1985-02-25	21 day 13:34:56	\N	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.

create table vector_sort_engine.VECTOR_SORT_TABLE_02
(
    L_ORDERKEY    BIGINT NOT NULL
  , L_PARTKEY     BIGINT NOT NULL
  , L_SUPPKEY     BIGINT NOT NULL
  , L_LINENUMBER  BIGINT NOT NULL
  , L_QUANTITY    DECIMAL(15,2) NOT NULL
  , L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL
  , L_DISCOUNT    DECIMAL(15,2) NOT NULL
  , L_TAX         DECIMAL(15,2) NOT NULL
  , L_RETURNFLAG  CHAR(1) NOT NULL
  , L_LINESTATUS  CHAR(1) NOT NULL
  , L_SHIPDATE    DATE NOT NULL
  , L_COMMITDATE  DATE NOT NULL
  , L_RECEIPTDATE DATE NOT NULL
  , L_SHIPINSTRUCT CHAR(25) NOT NULL
  , L_SHIPMODE     CHAR(10) NOT NULL
  , L_COMMENT      VARCHAR(44) NOT NULL
) with (orientation = orc) 
tablespace hdfs_ts
distribute by hash(L_ORDERKEY);

insert into vector_sort_table_02 select * from vector_engine.lineitem_vec;
insert into vector_sort_table_02 select * from vector_sort_table_02 where L_ORDERKEY < 4962;

analyze vector_sort_table_01;
analyze vector_sort_table_02;

----
--- test 1: Basic Test
----
select col_char, col_time, col_interval from VECTOR_SORT_TABLE_01 order by 1, 2, 3;

select col_int, col_real, col_num, col_vchar, col_timetz from VECTOR_SORT_TABLE_01 order by 1, 2, 3;

select col_decimal, col_char2, col_timetz, col_tv from VECTOR_SORT_TABLE_01 order by 1, 2, 3;

select col_bint, col_float, ctid from VECTOR_SORT_TABLE_01 order by 1, 2, 3;

select * from VECTOR_SORT_TABLE_01 order by col_interval;

select *, ctid from VECTOR_SORT_TABLE_01 order by col_timetz, col_int;

select * from VECTOR_SORT_TABLE_01 where col_tv is not null order by col_tv, col_char, col_int;

----
--- test 2: Vec Sort With Null Value And Big Amount
---- 
SET enable_vector_engine=on;

explain (verbose on, costs off) select * from vector_engine.lineitem_vec order by L_TAX limit 100;

select * from vector_engine.lineitem_vec order by L_EXTENDEDPRICE,L_PARTKEY limit 100;

select * from vector_engine.lineitem_vec order by L_EXTENDEDPRICE desc, L_PARTKEY limit 100;

select * from vector_engine.lineitem_vec order by L_SHIPDATE,L_PARTKEY limit 100;

select * from vector_engine.lineitem_vec order by L_SHIPDATE desc, L_PARTKEY limit 100;

select * from vector_engine.lineitem_vec order by L_COMMENT, L_PARTKEY limit 100;

select * from vector_engine.lineitem_vec order by L_COMMENT desc, L_PARTKEY limit 100;

----
--- test 3: Sort Method Stat Info
----
explain analyze select * from VECTOR_SORT_TABLE_01 order by col_interval;

explain analyze select * from vector_engine.lineitem_vec order by L_COMMENT, L_PARTKEY limit 10;

set work_mem = 64;
explain analyze select * from vector_engine.lineitem_vec order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
reset work_mem;

----
--- test 4: test the Fetch batch from DN 
--- Here each tape can not fetch batch with only one time 
----
select L_ORDERKEY, L_QUANTITY from vector_sort_table_02 order by L_ORDERKEY, L_QUANTITY limit 1000 offset 5500;

----
--- test 5: Vec Sort On Disk
----
insert into vector_sort_table_02 select * from vector_sort_table_02;
set work_mem = 64;
select * from vector_sort_table_02 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
reset work_mem;

----
--- Clean Resource and Tables
----
drop schema vector_sort_engine cascade;


