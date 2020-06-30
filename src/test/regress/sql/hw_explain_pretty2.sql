----
--- CREATE TABLE
----
set codegen_cost_threshold=0;
create schema explain_pretty;
set current_schema=explain_pretty;

create table explain_pretty.EXPLAIN_PRETTY_TABLE_01
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column);

COPY EXPLAIN_PRETTY_TABLE_01(col_int, col_int2, col_num, col_char, col_varchar, col_date, col_interval) FROM stdin;
11	21	1.25	T	beijing	2005-02-14	2 day 13:24:56
12	12	2.25	F	tianjing	2016-02-15	2 day 13:24:56
13	23	1.25	T	beijing	2006-02-14	4 day 13:25:25
12	12	1.25	T	xian	2005-02-14	4 day 13:25:25
15	25	2.25	F	beijing	2006-02-14	2 day 13:24:56
17	27	2.27	C	xian	2006-02-14	2 day 13:24:56
18	28	2.25	C	tianjing	2008-02-14	8 day 13:28:56
12	22	2.25	F	tianjing	2016-02-15	2 day 13:24:56
18	27	2.25	F	xian	2008-02-14	2 day 13:24:56
16	26	2.36	C	beijing	2006-05-08	8 day 13:28:56
16	16	2.36	C	beijing	2006-05-08	8 day 13:28:56
18	28	2.25	T	xian	2008-02-14	4 day 13:25:25
\.

create table explain_pretty.EXPLAIN_PRETTY_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column);

COPY EXPLAIN_PRETTY_TABLE_02(col_int, col_int2, col_num, col_char, col_varchar, col_date, col_interval) FROM stdin;
11	13	1.25	T	beijing	2005-02-14	5 day 13:24:56
12	14	2.25	F	tianjing	2006-02-15	2 day 13:24:56
13	14	1.25	T	beijing	2006-02-14	4 day 13:25:25
12	15	1.25	T	xian	2005-02-14	5 day 13:25:25
15	15	2.25	F	beijing	2006-02-14	2 day 13:24:56
\.

analyze EXPLAIN_PRETTY_TABLE_01;
analyze EXPLAIN_PRETTY_TABLE_02;

create table row_append_table_01 as select * from EXPLAIN_PRETTY_TABLE_01;
create table row_append_table_02 as select * from EXPLAIN_PRETTY_TABLE_02;

analyze row_append_table_01;
analyze row_append_table_02;

explain (verbose on, costs off, analyze on, cpu on)
(select col_interval from EXPLAIN_PRETTY_TABLE_01 where col_int > 11) union (select col_interval from EXPLAIN_PRETTY_TABLE_02 where col_int > 12) order by col_interval;
explain (verbose on, costs off, analyze on, cpu on)
(select col_interval from row_append_table_01 where col_int > 11) union (select col_interval from row_append_table_02 where col_int > 12) order by col_interval;

explain (analyze on, costs off, timing off) select * from row_append_table_01 join row_append_table_02 on row_append_table_01.col_int=row_append_table_02.col_int and row_append_table_01.col_int + row_append_table_02.col_int > 100;
--test sort,hashagg, cpu buffer
set explain_perf_mode = run;
\o hw_explain_pretty_result.txt
explain performance select * from row_append_table_01 where col_int > 15;
explain (analyze on, timing off) select * from row_append_table_01 join row_append_table_02 on row_append_table_01.col_int=row_append_table_02.col_int and row_append_table_01.col_int + row_append_table_02.col_int > 100;
explain performance
 (select distinct
col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2<23 order by col_char) 
union
(select distinct col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2>=23)
union
(select distinct col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2 = 26) order by col_char, col_varchar;

explain (analyze on, cpu on, buffers on)
 (select distinct
col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2<23 order by col_char) 
union
(select distinct col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2>=23)
union
(select distinct col_char, col_varchar from EXPLAIN_PRETTY_TABLE_01 where col_int2 = 26) order by col_char, col_varchar;

--test setop
explain select t1.col_int from  EXPLAIN_PRETTY_TABLE_01  t1 minus select t2.col_int from  EXPLAIN_PRETTY_TABLE_02 t2;

--test subquery
explain performance
select t1.col_int from  EXPLAIN_PRETTY_TABLE_01  t1 intersect all (select t2.col_int from  EXPLAIN_PRETTY_TABLE_02 t2) order by 1;

explain performance
select t1.col_int from  row_append_table_01  t1 intersect all (select t2.col_int from  row_append_table_02 t2) order by 1;

--test hashjoin
set enable_nestloop=off;
set enable_mergejoin=off;
explain (analyze on, cpu on, buffers on)
 select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;
explain performance
 select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;
--test left join
explain performance
select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 left join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;

set enable_vector_engine=off;
explain performance
 select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23 order by 1;
reset enable_vector_engine;

--test nestloop
set enable_hashjoin=off;
set enable_mergejoin=off;
set enable_nestloop=on;

explain verbose select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;
explain performance
select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 left join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;

\o
\! rm hw_explain_pretty_result.txt
--error condition
explain (format xml, verbose on) select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;

--test multi query
set enable_nestloop=off;
set enable_mergejoin=off;
set enable_hashjoin=on;
select 1\; explain analyze select count(*) from  EXPLAIN_PRETTY_TABLE_01  t1 join EXPLAIN_PRETTY_TABLE_01 t2 on t1.col_int2 = t2.col_int2  where t1.col_int2<23;


---
create table explain_pretty.row_EXPLAIN_PRETTY_TABLE_01
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
);

create table explain_pretty.row_EXPLAIN_PRETTY_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
);

\o hw_explain_pretty_result.txt
--test subplans
explain analyze  insert into explain_pretty.row_EXPLAIN_PRETTY_TABLE_01 select * from explain_pretty.EXPLAIN_PRETTY_TABLE_01;

--test bitmap

create table test_bitmap (a int, b int, c int);
insert into test_bitmap values (null, generate_series(1, 20), null);
insert into test_bitmap values (null, null, generate_series(1, 20));
create index idx_b on test_bitmap (b);
create index idx_c on test_bitmap (c);
analyze test_bitmap;
set enable_seqscan = off;
set enable_indexscan = off;

explain performance select * from test_bitmap where b<10 and c<5;
explain (analyze on, cpu on, buffers on) select * from test_bitmap where b<10 and c<5;
explain performance select * from test_bitmap where b<10 or c<5;
explain (analyze on, cpu on, buffers on) select * from test_bitmap where b<10 or c<5;

\o
\! rm hw_explain_pretty_result.txt


--test partition table
CREATE TABLE EXPLAIN_PRETTY_TABLE_05(
    a1 character varying(1000),
    a2 integer
)
WITH (orientation=column)
PARTITION BY RANGE (a2)
(
    PARTITION p1 VALUES LESS THAN (5),
    PARTITION p2 VALUES LESS THAN (10)
);
insert into EXPLAIN_PRETTY_TABLE_05 values('a',1);
insert into EXPLAIN_PRETTY_TABLE_05 values('a',5);
\o hw_explain_pretty_result.txt
explain select * from EXPLAIN_PRETTY_TABLE_05 order by 2;
\o
\! rm hw_explain_pretty_result.txt


CREATE TABLE EXPLAIN_PRETTY_TABLE_06
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
  --, primary key (L_ORDERKEY, L_LINENUMBER)
)
with (orientation = column)
PARTITION BY RANGE(L_SHIPDATE)
(
    PARTITION L_SHIPDATE_1 VALUES LESS THAN('1993-01-01 00:00:00'),
    PARTITION L_SHIPDATE_2 VALUES LESS THAN('1994-01-01 00:00:00'),
    PARTITION L_SHIPDATE_3 VALUES LESS THAN('1995-01-01 00:00:00'),
	PARTITION L_SHIPDATE_4 VALUES LESS THAN('1996-01-01 00:00:00'),
	PARTITION L_SHIPDATE_5 VALUES LESS THAN('1997-01-01 00:00:00'),
	PARTITION L_SHIPDATE_6 VALUES LESS THAN('1998-01-01 00:00:00'),
	PARTITION L_SHIPDATE_7 VALUES LESS THAN('1999-01-01 00:00:00')
)
;

create index lineitem_index on EXPLAIN_PRETTY_TABLE_06(l_orderkey) local;
set enable_seqscan=off;
set enable_tidscan=off;
set enable_indexscan = on;
set enable_bitmapscan=off;
explain (costs off) delete from EXPLAIN_PRETTY_TABLE_06 where l_orderkey<1;

-- test subplan executed on CN
create table store_sales_extend_min_1t
(
	ss_item_sk int,
	ss_sold_date_sk smallint,
	ss_ticket_number bigint,
	ss_date date,
	ss_time time,
	ss_timestamp timestamp,
	ss_list_price decimal(7,2)
)
with (orientation = column);

create table store_sales_extend_max_1t
(ss_item_sk int,
	ss_sold_date_sk smallint,
	ss_ticket_number bigint,
	ss_date date,
	ss_time time,
	ss_timestamp timestamp,
	ss_list_price decimal(7,2)
)
with (orientation = column);

\o hw_explain_pretty_result.txt
explain performance select min((select max(ss_date) from store_sales_extend_min_1t) + (select min(ss_time) from store_sales_extend_max_1t)) from store_sales_extend_min_1t;
\o
\! rm hw_explain_pretty_result.txt

-- test declare curosr
set explain_perf_mode=normal;
start transaction;
explain (costs off) cursor cursor_1 for select count(*) from store_sales_extend_min_1t;
explain analyze cursor cursor_1 for select count(*) from store_sales_extend_min_1t;
end;

set explain_perf_mode=pretty;
start transaction;
explain (costs off) cursor cursor_1 for select count(*) from store_sales_extend_min_1t;
explain analyze cursor cursor_1 for select count(*) from store_sales_extend_min_1t;
end;


--test rows Removed by Filter for partition table and nestloop when nloops larger than 1
set explain_perf_mode=normal; 
create table tbl_part(a int) partition by range(a)
(
partition P1 values less than (10),
partition P2 values less than (20),
partition P3 values less than (30),
partition P4 values less than (40),
partition P5 values less than (MAXVALUE)
);
insert into tbl_part values(generate_series(1,50));
select count(*) from tbl_part;
select count(*) from tbl_part where a%2=0;
explain analyze select count(*) from tbl_part where a%2=0;
Drop table tbl_part cascade;

set enable_mergejoin=off;
set enable_hashjoin=off;
create table tb1_nestloop(a int,b int);
create table tb2_nestloop (a int,d int);
insert into tb1_nestloop values(generate_series(1,10), generate_series(1,10));
insert into tb2_nestloop values(generate_series(1,20), generate_series(1,20));
analyze tb1_nestloop;
analyze tb2_nestloop;
explain analyze select * from tb1_nestloop ,tb2_nestloop where tb1_nestloop.a= tb2_nestloop.d and tb1_nestloop.a%2=0 and tb2_nestloop.a%2=0;
set enable_mergejoin=on;
set enable_hashjoin=on;
drop table tb1_nestloop cascade;
drop table tb2_nestloop cascade;

drop schema explain_pretty cascade;
