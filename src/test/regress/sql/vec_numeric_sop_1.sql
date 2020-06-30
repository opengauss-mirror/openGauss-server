/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine_1;
set current_schema=vector_sop_engine_1;

create table vector_sop_engine_1.VECTOR_SOP_TABLE_01
(
   col_int2	int2
  ,col_int4	int4
  ,col_int8	int8
  ,col_char	char(10)
  ,col_vchar	varchar(12)
  ,col_num	numeric(10,2)
  ,col_interval	interval
  ,col_timetz	timetz
) with (orientation=column)  ;

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

analyze vector_sop_table_01;
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
--- Clean Resource and Tables
----
drop schema vector_sop_engine_1 cascade;
