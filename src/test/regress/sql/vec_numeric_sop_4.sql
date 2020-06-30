/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine_4;
set current_schema=vector_sop_engine_4;

create table vector_sop_engine_4.VECTOR_SOP_TABLE_04
(
    col_int	int
   ,col_num	numeric
)with(orientation = column)  ;

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

create table vector_sop_engine_4.VECTOR_SOP_TABLE_05
(
    col_int	int
   ,col_num	numeric
)with(orientation = column)  ;

COPY VECTOR_SOP_TABLE_05(col_int, col_num) FROM stdin;
2	2.568
3	\N
65	11.23
12	NaN
\.

analyze vector_sop_table_04;
analyze vector_sop_table_05;

----
--- test 1: Simple Operation for numeric 
----
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

----
--- Clean Resource and Tables
----
drop schema vector_sop_engine_4 cascade;
