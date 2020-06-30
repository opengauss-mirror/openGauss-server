/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine_5;
set current_schema=vector_sop_engine_5;

create table vector_sop_engine_5.VECTOR_SOP_TABLE_06
(
    a	int
   ,b	varchar
   ,c	text
)with(orientation = column)  ;

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

create table vector_sop_engine_5.VECTOR_SOP_TABLE_07
(
    a	int
   ,b	varchar
   ,c	text
)with(orientation = column)  ;


COPY VECTOR_SOP_TABLE_07(a, b, c) FROM stdin;
12	ac	abc
32	beijing	xian
65	llvm	llvm
9	123vm	xian
12	abc	him
3	\N	cmcc
4	hik	\N
\.

analyze vector_sop_table_06;
analyze vector_sop_table_07;

----
--- test 1: Simple Operation for numeric 
----
select * from vector_sop_table_06 A where A.b <> 'abc' order by 1, 2, 3;
select * from vector_sop_table_06 A where 'llvm' <> A.c order by 1, 2, 3;
select * from vector_sop_table_06 A inner join vector_sop_table_07 B on A.b <> B.b order by 1, 2, 3, 4, 5, 6;
select * from vector_sop_table_06 A where A.b <> 'abc' or A.c <> 'cmcc' order by 1, 2, 3;
select * from vector_sop_table_06 A where 'llvm' <>A.b and A.c <> 'xian' order by 1, 2, 3;
select * from vector_sop_table_06 A inner join vector_sop_table_07 B on A.b <> B.b and A.c <> B.c order by 1, 2, 3, 4, 5, 6;

----
--- Clean Resource and Tables
----
drop schema vector_sop_engine_5 cascade;
