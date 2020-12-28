 /*
 * This file is used to test the function of ExecVecAppend
 */
----
--- Create Table and Insert Data
----
create schema vector_append_engine_part2;
set current_schema=vector_append_engine_part2;

create table vector_append_engine_part2.VECTOR_APPEND_TABLE_01
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

analyze vector_append_table_01;

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
--- Clean Resource and Tables
----
drop schema vector_append_engine_part2 cascade;
