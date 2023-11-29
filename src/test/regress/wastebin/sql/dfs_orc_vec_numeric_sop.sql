set enable_global_stats = true;
/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine;
set current_schema=vector_sop_engine;

create table vector_sop_engine.VECTOR_SOP_TABLE_01
(
   col_int2	int2
  ,col_int4	int4
  ,col_int8	int8
  ,col_char	char(10)
  ,col_vchar	varchar(12)
  ,col_num	numeric(10,2)
  ,col_interval	interval
  ,col_timetz	timetz
) with (orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int8);

COPY VECTOR_SOP_TABLE_01(col_int2, col_int4, col_int8, col_char, col_vchar, col_num, col_interval, col_timetz) FROM stdin; 
1	2	3	abc	cdffef	11.23	1 day 12:34:56	1984-2-6 00:00:30+8
1	2	3	abc	cdffef	11.23	1 day 12:34:56	1985-2-6 00:00:30+8
1	2	3	abc	cdffef	11.23	2 days 12:34:56	1986-2-6 00:00:30+8
3	4	3	abc	cdffef	11.25	4 days 12:34:56	1987-2-6 00:00:30+8
1	2	3	agc	cdffef	11.23	1 day 12:34:56	1988-2-6 00:00:30+8
1	2	3	amc	cdffef	11.23	1 day 12:34:56	1989-2-6 00:00:30+8
1	2	3	abc	cdffef	11.23	1 day 12:34:56	\N
3	4	3	abc	cdffef	11.25	3 days 12:34:56	\N
\N	\N	\N	\N	\N	\N	5 days 12:34:56	1984-2-6 00:00:30+8
\N	\N	\N	\N	\N	\N	6 days 12:34:56	\N
3	4	3	agc	cdffef	11.25	\N	1990-2-6 00:00:30+8
1	2	3	\N	cdffef	11.23	\N	1991-2-6 00:00:30+8
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_text	text
) with (orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int);

COPY VECTOR_SOP_TABLE_02(col_int, col_int2, col_text) FROM stdin;
1	2	replication_t1
2	3	replication_t1
1	2	t
2	3	t
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_03
(
   col_tz	timetz
  ,col_int	int
) with (orientation = column, max_batchrow = 10000)
partition by range (col_int) 
(
        partition timetz_partp1 values less than (10),
        partition timetz_partp2 values less than (20),
        partition timetz_partp3 values less than (30)
);

COPY VECTOR_SOP_TABLE_03(col_tz, col_int) FROM stdin;
1984-2-6 00:00:30+8	1
1985-2-6 00:00:30+8	9
1986-2-6 00:00:30+8	10
1987-2-6 00:00:30+8	19
1988-2-6 00:00:30+8	20
1989-2-6 00:00:30+8	29
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_04
(
    col_int	int
   ,col_num	numeric
)with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int);

COPY VECTOR_SOP_TABLE_04(col_int, col_num) FROM stdin;
1	2.568
2	11.23
3	11.25
\N	3.65
6	2.789
52	11.23
14	\N
7	36.5
10	2.57
2	NaN
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_05
(
    col_int	int
   ,col_num	numeric
)with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(col_int);

COPY VECTOR_SOP_TABLE_05(col_int, col_num) FROM stdin;
2	2.568
3	\N
65	11.23
12	NaN
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_06
(
    a	int
   ,b	varchar
   ,c	text
)with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(a);

COPY VECTOR_SOP_TABLE_06(a, b, c) FROM stdin;
1	abc	abc
2	15ab	ab15
3	uim	cmcc
5	llvm	llvm
8	abc	llvm
9	123vm	beijing
12	abc	him
65	\N	xian
32	beijing	\N
\.

create table vector_sop_engine.VECTOR_SOP_TABLE_07
(
    a	int
   ,b	varchar
   ,c	text
)with(orientation = orc) 
tablespace hdfs_ts
distribute by hash(a);


COPY VECTOR_SOP_TABLE_07(a, b, c) FROM stdin;
12	ac	abc
32	beijing	xian
65	llvm	llvm
9	123vm	xian
12	abc	him
3	\N	cmcc
4	hik	\N
\.

analyze vector_sop_table_01;
analyze vector_sop_table_02;
analyze vector_sop_table_03;
analyze vector_sop_table_04;
analyze vector_sop_table_05;
analyze vector_sop_table_06;
analyze vector_sop_table_07;

----
--- test 1: Simple Operation for numeric 
----
select * from vector_sop_table_01 where col_num=11.25 order by col_char;
select * from vector_sop_table_01 where col_num != 11.23 order by col_char;
select col_num + 2.2 from vector_sop_table_01 where col_num < 11.25;
select col_num-2.3  from vector_sop_table_01 where col_num > 11.23;
select col_num*1.1 from vector_sop_table_01 where col_num > 11.23;
select col_num/1.1 from vector_sop_table_01 where col_num > 11.23;
select * from vector_sop_table_01 where col_num >= 11.25 order by col_char;
select col_int8 from vector_sop_table_01 where col_num <= 11.23;
select * from vector_sop_table_04 where col_num <> 11.25 order by 1, 2;
select * from vector_sop_table_04 where col_num <> 'NaN' order by 1, 2;
select * from vector_sop_table_04 where 11.23 <> col_num order by 1, 2; 
select * from vector_sop_table_04 where 'NaN' <> col_num order by 1, 2;
select * from vector_sop_table_04 A inner join vector_sop_table_05 B on A.col_num <> B.col_num order by 1, 2, 3, 4; 
select * from vector_sop_table_04 where col_num <> 11.25 and col_int > 5 order by 1, 2;
select * from vector_sop_table_04 where col_num <> 'NaN' and col_int < 75 order by 1, 2;
select * from vector_sop_table_04 where 11.23 <> col_num and col_int > 5 order by 1, 2; 
select * from vector_sop_table_04 where 'NaN' <> col_num and col_num > 2.5 order by 1, 2;
select * from vector_sop_table_04 A inner join vector_sop_table_05 B on A.col_num <> B.col_num and A.col_int <> B.col_int order by 1, 2, 3, 4; 
select * from vector_sop_table_06 A where A.b <> 'abc' order by 1, 2, 3;
select * from vector_sop_table_06 A where 'llvm' <> A.c order by 1, 2, 3;
select * from vector_sop_table_06 A inner join vector_sop_table_07 B on A.b <> B.b order by 1, 2, 3, 4, 5, 6;
select * from vector_sop_table_06 A where A.b <> 'abc' or A.c <> 'cmcc' order by 1, 2, 3;
select * from vector_sop_table_06 A where 'llvm' <>A.b and A.c <> 'xian' order by 1, 2, 3;
select * from vector_sop_table_06 A inner join vector_sop_table_07 B on A.b <> B.b and A.c <> B.c order by 1, 2, 3, 4, 5, 6;

----
--- test 2 : Simple Operation for interval/timetz
----
select col_interval from vector_sop_table_01 order by 1;
select * from vector_sop_table_01 where col_interval='1 day 12:34:56' order by 1;
select * from vector_sop_table_01 where col_interval is NULL order by 1;
select col_interval + interval '1 day' from vector_sop_table_01 order by 1;
select col_interval + interval '1 hour' from vector_sop_table_01 order by 1;
select col_interval + interval '1 minute' from vector_sop_table_01 order by 1;
select col_interval + interval '1 second' from vector_sop_table_01 order by 1;

select col_timetz from vector_sop_table_01 order by 1;
select col_timetz from vector_sop_table_01 where col_timetz is not NULL order by 1;
select col_timetz from vector_sop_table_01 where col_timetz is NULL order by 1;
select col_timetz + interval '1 hour' from vector_sop_table_01 order by 1; 
select col_timetz + interval '1 minute' from vector_sop_table_01 order by 1; 
select col_timetz + interval '1 second' from vector_sop_table_01 order by 1; 

----
--- test 3: Simple Operation for substr
----
select substring(col_text,1,2) from vector_sop_table_02 order by 1;
select substring(col_text,10,1) from vector_sop_table_02 order by 1;
select substring(col_text,10,1) is Not NULL from vector_sop_table_02 order by 1;
select substring(col_text,10,1) is NULL from vector_sop_table_02 order by 1;
select substring('a',col_text) is NULL from vector_sop_table_02 order by 1;
select substring(col_text,10) is Not NULL from vector_sop_table_02 order by 1;

----
--- test 4: Operation with Partition
----
select * from vector_sop_table_03 order by 1;
select * from vector_sop_table_03 where col_tz is NULl order by 1;
select * from vector_sop_table_03 where col_tz is Not NULL  order by 1;

----
--- Clean Resource and Tables
----
drop schema vector_sop_engine cascade;
