/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine_2;
set current_schema=vector_sop_engine_2;

create table vector_sop_engine_2.VECTOR_SOP_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_text	text
) with (orientation=column)  ;

COPY VECTOR_SOP_TABLE_02(col_int, col_int2, col_text) FROM stdin;
1	2	replication_t1
2	3	replication_t1
1	2	t
2	3	t
\.

analyze vector_sop_table_02;

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
--- Clean Resource and Tables
----
drop schema vector_sop_engine_2 cascade;
