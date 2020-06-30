/*
 * This file is used to test the simple operation for different data-type
 */
----
--- Create Table and Insert Data
----
create schema vector_sop_engine_3;
set current_schema=vector_sop_engine_3;

create table vector_sop_engine_3.VECTOR_SOP_TABLE_03
(
   col_tz	timetz
  ,col_int	int
) with (orientation = column)
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

analyze vector_sop_table_03;
----
--- test 4: Operation with Partition
----
select * from vector_sop_table_03 order by 1, 2;
select * from vector_sop_table_03 where col_tz is NULl order by 1, 2;
select * from vector_sop_table_03 where col_tz is Not NULL  order by 1, 2;

----
--- Clean Resource and Tables
----
drop schema vector_sop_engine_3 cascade;
