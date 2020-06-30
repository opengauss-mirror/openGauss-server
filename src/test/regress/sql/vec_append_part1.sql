/*
 * This file is used to test the function of ExecVecAppend
 */
----
--- Create Table and Insert Data
----
create schema vector_append_engine_part1;
set current_schema=vector_append_engine_part1;

create table vector_append_engine_part1.VECTOR_APPEND_TABLE_01
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column)  ;

COPY VECTOR_APPEND_TABLE_01(col_int, col_int2, col_num, col_char, col_varchar, col_date, col_interval) FROM stdin;
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

create table vector_append_engine_part1.VECTOR_APPEND_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column)  ;

COPY VECTOR_APPEND_TABLE_02(col_int, col_int2, col_num, col_char, col_varchar, col_date, col_interval) FROM stdin;
11	13	1.25	T	beijing	2005-02-14	5 day 13:24:56
12	14	2.25	F	tianjing	2006-02-15	2 day 13:24:56
13	14	1.25	T	beijing	2006-02-14	4 day 13:25:25
12	15	1.25	T	xian	2005-02-14	5 day 13:25:25
15	15	2.25	F	beijing	2006-02-14	2 day 13:24:56
\.

analyze vector_append_table_01;
analyze vector_append_table_02;

----
--- test 1: Basic Test
----  
explain (verbose on, costs off) 
(select col_interval from vector_append_table_01 where col_int > 11) union (select col_interval from vector_append_table_02 where col_int > 12) order by col_interval;

(select col_interval from vector_append_table_01 where col_int > 11) union (select col_interval from vector_append_table_02 where col_int > 12) order by col_interval;
(select col_int, col_int2, col_num from vector_append_table_01 where col_int > 11) union (select col_int, col_int2, col_num from vector_append_table_02 where col_int > 12) order by 1, 2, 3;
(select col_int, col_int2, col_num from vector_append_table_01 where col_date > '2005-02-14') union (select col_int, col_int2, col_num from vector_append_table_02 where col_num < 2.25) order by 1, 2, 3;

----
--- Clean Resource and Tables
----
drop schema vector_append_engine_part1 cascade;
