/*
 * This file is used to test the function of ExecVecAppend
 */
----
--- Create Table and Insert Data
----
create schema vector_append_engine;
set current_schema=vector_append_engine;

create table vector_append_engine.VECTOR_APPEND_TABLE_01
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column) distribute by hash(col_int);

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

create table vector_append_engine.VECTOR_APPEND_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_num	numeric(10,4)
  ,col_char	char
  ,col_varchar	varchar(20)
  ,col_date	date
  ,col_interval	interval
) with(orientation=column) distribute by hash(col_int);

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
--- test 2: cased summarizd from user requirements(With Union and Using agg or unique node)
----
explain (verbose, costs off) 
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int<13) 
union
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int>=13);

explain (verbose, costs off) 
(select distinct col_int2, col_num from vector_append_table_01 where col_int2<23) 
union
(select distinct col_int2, col_num from vector_append_table_01 where col_int2>=23);

explain (verbose, costs off) 
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int<13 order by col_int) 
union
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int>=13);

explain (verbose, costs off) 
(select distinct col_int2, col_num from vector_append_table_01 where col_int2<23 order by col_int2) 
union
(select distinct col_int2, col_num from vector_append_table_01 where col_int2>=23);

explain (verbose, costs off) 
(select distinct col_char, col_varchar from vector_append_table_01 where col_int2<23 order by col_char) 
union
(select distinct col_char, col_varchar from vector_append_table_01 where col_int2>=23)
union
(select distinct col_char, col_varchar from vector_append_table_01 where col_int2 = 26) order by col_char, col_varchar;

((select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int<13) 
union
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int>=13)) order by col_int, col_int2, col_num;

((select distinct col_int2, col_num from vector_append_table_01 where col_int2<23) 
union
(select distinct col_int2, col_num from vector_append_table_01 where col_int2>=23)) order by col_int2, col_num;

((select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int<13 order by col_int) 
union
(select distinct col_int, col_int2, col_num from vector_append_table_01 where col_int>=13)) order by col_int, col_int2, col_num;

((select distinct col_int2, col_num from vector_append_table_01 where col_int2<23 order by col_int2) 
union
(select distinct col_int2, col_num from vector_append_table_01 where col_int2>=23)) order by col_int2, col_num;

(select distinct col_char, col_varchar from vector_append_table_01 where col_int2<23 order by col_char) 
union
(select distinct col_char, col_varchar from vector_append_table_01 where col_int2>=23)
union
(select distinct col_char, col_varchar from vector_append_table_01 where col_int2 = 26) order by col_char, col_varchar;

----
--- test 3: Intersect
----
explain (verbose, costs off)
select col_int2 from vector_append_table_01 INTERSECT select col_int from vector_append_table_01 order by 1;

--add for llt
\o hw_explain_open_group.txt
explain (analyze on, format YAML) 
select col_int2 from vector_append_table_01 INTERSECT select col_int from vector_append_table_01 order by 1;
\o
\! rm hw_explain_open_group.txt

select col_int2 from vector_append_table_01 INTERSECT select col_int from vector_append_table_01 order by 1;

select col_int2 from vector_append_table_01 INTERSECT ALL select col_int from vector_append_table_01 order by 1;

select col_interval from vector_append_table_01 INTERSECT select col_interval from vector_append_table_01 order by 1;

select col_int2 from vector_append_table_01 EXCEPT select col_int from vector_append_table_01 order by 1;

select col_int2 from vector_append_table_01 EXCEPT ALL select col_int from vector_append_table_01 order by 1;

select col_int * 3 as int1 from vector_append_table_01 EXCEPT select col_int2 * 2 from vector_append_table_01 order by 1;

select col_char, col_num from vector_append_table_01 INTERSECT select col_varchar, col_num from vector_append_table_01 order by col_num;

SELECT col_int2-1 as q2, col_int FROM vector_append_table_01 INTERSECT ALL SELECT col_int-1, col_int FROM vector_append_table_01 ORDER BY 1;

----
--- Clean Resource and Tables
----
drop schema vector_append_engine cascade;