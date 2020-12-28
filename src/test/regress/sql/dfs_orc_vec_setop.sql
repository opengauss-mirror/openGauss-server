set enable_global_stats = true;

/*
 * This file is used to test the function of ExecVecSetOp
 */
----
--- Create Table and Insert Data
----
create schema vector_setop_engine;
set current_schema=vector_setop_engine;

create table vector_setop_engine.VECTOR_SETOP_TABLE_01
(
   col_inta	int
  ,col_intb	int
  ,col_num	numeric
  ,col_char	char
  ,col_varchar	varchar
  ,col_text	text
  ,col_time	time
  ,col_interval	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_inta);

COPY VECTOR_SETOP_TABLE_01(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
1	2	1.2	T	hash_t1	hash_t1	11:18:00	2 day 13:24:56
2	3	2.3	T	hash_t1	hash_t1	11:18:00	2 day 13:24:56
1	2	1.2	F	t	t	11:28:00	2 day 13:25:56
2	3	2.3	F	t	t	11:28:00	2 day 13:25:56
\.

create table vector_setop_engine.VECTOR_SETOP_TABLE_02
(
   col_inta	int
  ,col_intb	int
  ,col_num	numeric
  ,col_char	char
  ,col_varchar	varchar
  ,col_text	text
  ,col_time	time
  ,col_interval	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_inta);

COPY VECTOR_SETOP_TABLE_02(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
1	2	1.2	T	hash_t2	hash_t2	11:18:00	2 day 13:24:56
2	3	2.3	T	hash_t2	hash_t2	11:18:00	2 day 13:24:56
3	4	3.4	T	hash_t2	hash_t2	11:18:00	2 day 13:24:56
1	2	1.2	F	t	t	11:28:00	2 day 13:25:56
2	3	2.3	F	t	t	11:28:00	2 day 13:25:56
3	4	3.4	F	t	t	11:28:00	2 day 13:25:56
\.

create table vector_setop_engine.VECTOR_SETOP_TABLE_03
(
   col_inta	int
  ,col_intb	int
  ,col_num	numeric
  ,col_char	char
  ,col_varchar	varchar
  ,col_text	text
  ,col_time	time
  ,col_interval	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_intb);

COPY VECTOR_SETOP_TABLE_03(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
1	2	1.2	T	hash_t3	hash_t3	11:18:00	2 day 13:24:56
2	3	2.3	T	hash_t3	hash_t3	11:18:00	2 day 13:24:56
3	4	3.4	T	hash_t3	hash_t3	11:18:00	2 day 13:24:56
4	5	4.5	T	hash_t3	hash_t3	11:18:00	2 day 13:24:56
1	2	1.2	F	t	t	11:28:00	2 day 13:25:56
2	3	2.3	F	t	t	11:28:00	2 day 13:25:56
3	4	3.4	F	t	t	11:28:00	2 day 13:25:56
4	5	4.5	F	t	t	11:28:00	2 day 13:25:56
\.

create table vector_setop_engine.VECTOR_SETOP_TABLE_04
(
   col_inta	smallint
  ,col_intb	bigint
  ,col_num	numeric
  ,col_char	char
  ,col_varchar	varchar
  ,col_text	text
  ,col_time	time
  ,col_interval	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_inta);

COPY VECTOR_SETOP_TABLE_04(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
1	2	1.2	T	hash_t4	hash_t4	11:18:00	2 day 13:24:56
2	3	2.3	T	hash_t4	hash_t4	11:18:00	2 day 13:24:56
3	4	3.4	T	hash_t4	hash_t4	11:18:00	2 day 13:24:56
4	5	4.5	T	hash_t4	hash_t4	11:18:00	2 day 13:24:56
5	6	4.5	T	hash_t4	hash_t4	11:18:00	2 day 13:24:56
1	2	1.2	F	t	t	11:28:00	2 day 13:25:56
2	3	2.3	F	t	t	11:28:00	2 day 13:25:56
3	4	3.4	F	t	t	11:28:00	2 day 13:25:56
4	5	4.5	F	t	t	11:28:00	2 day 13:25:56
5	6	4.5	F	t	t	11:28:00	2 day 13:25:56
\.

analyze vector_setop_table_01;
analyze vector_setop_table_02;
analyze vector_setop_table_03;
analyze vector_setop_table_04;

create view setop_12 as select vector_setop_table_01.col_inta as ta1, vector_setop_table_01.col_intb as tb1, vector_setop_table_02.col_inta as ta2, vector_setop_table_01.col_intb as tb2 from vector_setop_table_01 inner join vector_setop_table_02 on vector_setop_table_01.col_inta = vector_setop_table_02.col_inta; 

create view setop_23 as select vector_setop_table_02.col_inta as ta2, vector_setop_table_02.col_intb as tb2, vector_setop_table_03.col_inta as ta3, vector_setop_table_03.col_intb as tb3 from vector_setop_table_02 inner join vector_setop_table_03 on vector_setop_table_02.col_inta = vector_setop_table_03.col_inta; 

create view setop_31 as select vector_setop_table_03.col_inta as ta3, vector_setop_table_03.col_intb as tb3, vector_setop_table_01.col_inta as ta1, vector_setop_table_01.col_intb as tb1 from vector_setop_table_03 inner join vector_setop_table_01 on vector_setop_table_03.col_inta = vector_setop_table_01.col_inta; 


----
--- test 1: Basic Test: INTERSECT ALL
----
-- hash + hash + same distributeKey + Append executes on all DNs
explain (verbose on, costs off)
select * from vector_setop_table_01 intersect all select * from vector_setop_table_02 order by 1, 2, 3;

select * from vector_setop_table_01 intersect all select * from vector_setop_table_02 order by 1, 2, 3;
select * from vector_setop_table_01 where col_inta = 1 intersect all select * from vector_setop_table_02 where col_intb = 1 order by 1, 2, 3;
select col_num, col_time from vector_setop_table_01 intersect all select col_num, col_time from vector_setop_table_03 order by 1, 2;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect all select col_intb, col_inta from vector_setop_table_03 where col_inta = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 intersect all select * from vector_setop_table_02 where col_inta = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect all select col_intb, col_inta from vector_setop_table_03 where col_intb = 1 order by 1, 2;
select col_time, col_interval from vector_setop_table_01 where col_inta = 1 intersect all select col_time, col_interval from vector_setop_table_03 where col_intb = 1 order by 1, 2; 

-- hash + hash + different distributeKey + Append executes on all DNs
select * from vector_setop_table_01 intersect all select * from vector_setop_table_03 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 intersect all select col_intb, col_inta from vector_setop_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 intersect all select * from vector_setop_table_03 where col_intb = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect all select col_intb, col_inta from vector_setop_table_02 where col_inta = 1 order by 1, 2;

-- hash + hash + type cast
select * from vector_setop_table_01 intersect all select * from vector_setop_table_04 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 intersect all select col_intb, col_inta from vector_setop_table_04 order by 1, 2;

-- execute on cn + hash
select 1 from pg_auth_members intersect all select col_intb from vector_setop_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select col_inta from vector_setop_table_01 intersect all select col_intb from vector_setop_table_02 order by 1;
select col_intb from vector_setop_table_01 intersect all select col_intb from vector_setop_table_02 order by 1;
select col_interval from vector_setop_table_01 intersect all select col_interval from vector_setop_table_02 order by 1;

select * from setop_12 intersect all select * from setop_23 order by 1, 2, 3;

SELECT 1 AS one intersect all SELECT 1.1::float8 order by 1;

(select * from vector_setop_table_01) minus (select * from vector_setop_table_01);

--Since column table does not support replication, the following tests should be fixed later
-- hash + replication  + Append executes on special DN
--select * from vector_setop_table_01 intersect all select * from replication_t1 order by 1, 2, 3;
-- replication + replication
--select * from replication_t1 intersect all select * from replication_t2 order by 1, 2, 3;

----
--- test 2: Basic Test: INTERSECT
----
-- hash + hash + same distributeKey + Append executes on all DNs
select * from vector_setop_table_01 intersect select * from vector_setop_table_02 order by 1, 2, 3;
select col_varchar, col_time from vector_setop_table_01 intersect select col_varchar, col_time from vector_setop_table_03 order by 1, 2;
select * from vector_setop_table_01 where col_inta = 1 intersect select * from vector_setop_table_02 where col_intb = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect all select col_intb, col_inta from vector_setop_table_03 where col_inta = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 intersect select * from vector_setop_table_02 where col_inta = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect select col_intb, col_inta from vector_setop_table_03 where col_intb = 1 order by 1, 2;
select col_time, col_interval from vector_setop_table_01 where col_inta = 1 intersect select col_time, col_interval from vector_setop_table_03 where col_intb = 1 order by 1, 2; 

-- hash + hash + different distributeKey + Append executes on all DNs
select * from vector_setop_table_01 intersect select * from vector_setop_table_03 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 intersect select col_intb, col_inta from vector_setop_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 intersect select * from vector_setop_table_03 where col_intb = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 intersect select col_intb, col_inta from vector_setop_table_02 where col_inta = 1 order by 1, 2;

-- hash + hash + type cast
select * from vector_setop_table_01 intersect select * from vector_setop_table_04 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 intersect select col_intb, col_inta from vector_setop_table_04 order by 1, 2;

-- execute on cn + hash
select 1 from pg_auth_members intersect all select col_intb from vector_setop_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select col_inta from vector_setop_table_01 intersect select col_intb from vector_setop_table_02 order by 1;
select col_intb from vector_setop_table_01 intersect select col_intb from vector_setop_table_02 order by 1;
select col_interval from vector_setop_table_01 intersect select col_interval from vector_setop_table_02 order by 1;

select * from setop_12 intersect select * from setop_23 order by 1, 2, 3;

SELECT 1 AS one intersect SELECT 1.1::float8 order by 1;

--Since column table does not support replication, the following tests should be fixed later
-- hash + replication  + Append executes on special DN
--select * from hash_t1 intersect select * from replication_t1 order by 1, 2;
-- replication + replication
--select * from replication_t1 intersect select * from replication_t2 order by 1, 2;


----
--- test 3: Basic Test: EXCEPT ALL 
----
select * from vector_setop_table_01 except all select * from vector_setop_table_02 order by 1, 2, 3;
select col_varchar, col_time from vector_setop_table_01 except all select col_varchar, col_time from vector_setop_table_03 order by 1, 2;
select * from vector_setop_table_01 where col_inta = 1 except all select * from vector_setop_table_02 where col_intb = 1 order by 1, 2, 3, 4;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except all select col_intb, col_inta from vector_setop_table_03 where col_inta = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 except all select * from vector_setop_table_02 where col_inta = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except all select col_intb, col_inta from vector_setop_table_03 where col_intb = 1 order by 1, 2;
select col_time, col_interval from vector_setop_table_01 where col_inta = 1 except all select col_time, col_interval from vector_setop_table_03 where col_intb = 1 order by 1, 2; 

-- hash + hash + different distributeKey + Append executes on all DNs
select * from vector_setop_table_01 except all select * from vector_setop_table_03 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 except all select col_intb, col_inta from vector_setop_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 except all select * from vector_setop_table_03 where col_intb = 1 order by 1, 2, 3, 4;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except all select col_intb, col_inta from vector_setop_table_02 where col_inta = 1 order by 1, 2;

-- hash + hash + type cast
select * from vector_setop_table_01 except all select * from vector_setop_table_04 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 except all select col_intb, col_inta from vector_setop_table_04 order by 1, 2;

-- execute on cn + hash
select 1 from pg_auth_members except all select col_intb from vector_setop_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select col_inta from vector_setop_table_01 except all select col_intb from vector_setop_table_02 order by 1;
select col_intb from vector_setop_table_01 except all select col_intb from vector_setop_table_02 order by 1;
select col_interval from vector_setop_table_01 except all select col_interval from vector_setop_table_02 order by 1;

select * from setop_12 except all select * from setop_23 order by 1, 2, 3;

SELECT 1 AS one except all SELECT 1.1::float8 order by 1;

--Since column table does not support replication, the following tests should be fixed later
-- hash + replication  + Append executes on special DN
--select * from hash_t1 except all select * from replication_t1 order by 1, 2, 3;
-- replication + replication
--select * from replication_t1 except all select * from replication_t2 order by 1, 2, 3;

----
--- test 4: Basic Test: EXCEPT 
----
select * from vector_setop_table_01 except select * from vector_setop_table_02 order by 1, 2, 3;
select col_varchar, col_time from vector_setop_table_01 except select col_varchar, col_time from vector_setop_table_03 order by 1, 2;
select * from vector_setop_table_01 where col_inta = 1 except select * from vector_setop_table_02 where col_intb = 1 order by 1, 2, 3, 4;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except all select col_intb, col_inta from vector_setop_table_03 where col_inta = 1 order by 1, 2;

-- hash + hash + same distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 except all select * from vector_setop_table_02 where col_inta = 1 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except select col_intb, col_inta from vector_setop_table_03 where col_intb = 1 order by 1, 2;
select col_time, col_interval from vector_setop_table_01 where col_inta = 1 except select col_time, col_interval from vector_setop_table_03 where col_intb = 1 order by 1, 2; 

-- hash + hash + different distributeKey + Append executes on all DNs
select * from vector_setop_table_01 except select * from vector_setop_table_03 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 except select col_intb, col_inta from vector_setop_table_02 order by 1, 2;

-- hash + hash + different distributeKey + Append executes on special DN
select * from vector_setop_table_01 where col_inta = 1 except all select * from vector_setop_table_03 where col_intb = 1 order by 1, 2, 3, 4;
select col_inta, col_intb from vector_setop_table_01 where col_inta = 1 except all select col_intb, col_inta from vector_setop_table_02 where col_inta = 1 order by 1, 2;

-- hash + hash + type cast
select * from vector_setop_table_01 except select * from vector_setop_table_04 order by 1, 2, 3;
select col_inta, col_intb from vector_setop_table_01 except select col_intb, col_inta from vector_setop_table_04 order by 1, 2;

-- execute on cn + hash
select 1 from pg_auth_members except all select col_intb from vector_setop_table_02 order by 1;

-- targetlist dosenot contains distributeKey
select col_inta from vector_setop_table_01 except select col_intb from vector_setop_table_02 order by 1;
select col_intb from vector_setop_table_01 except select col_intb from vector_setop_table_02 order by 1;
select col_interval from vector_setop_table_01 except select col_interval from vector_setop_table_02 order by 1;

select * from setop_12 except select * from setop_23 order by 1, 2, 3;

SELECT 1 AS one except SELECT 1.1::float8 order by 1;

--Since column table does not support replication, the following tests should be fixed later
-- hash + replication  + Append executes on special DN
--select * from hash_t1 except select * from replication_t1 order by 1, 2, 3;
-- replication + replication
--select * from replication_t1 except select * from replication_t2 order by 1, 2, 3;

----
--- test 5: SetOp with RunSort
----
set enable_hashagg=off;

explain (verbose, costs off) select * from vector_setop_table_01 intersect all select * from vector_setop_table_02 order by 1, 2, 3;
select * from vector_setop_table_01 intersect all select * from vector_setop_table_02 order by 1, 2, 3;

explain (verbose, costs off) select * from vector_setop_table_01 intersect select * from vector_setop_table_02 order by 1, 2, 3;
select * from vector_setop_table_01 intersect select * from vector_setop_table_02 order by 1, 2, 3;

explain (verbose, costs off) select * from vector_setop_table_01 except all select * from vector_setop_table_02 order by 1, 2, 3;
select * from vector_setop_table_01 except all select * from vector_setop_table_02 order by 1, 2, 3;

explain (verbose, costs off) select * from vector_setop_table_01 except select * from vector_setop_table_02 order by 1, 2, 3;
select * from vector_setop_table_01 except select * from vector_setop_table_02 order by 1, 2, 3;

explain (verbose, costs off) select col_interval from vector_setop_table_01 intersect select col_interval from vector_setop_table_02 order by 1;
select col_interval from vector_setop_table_01 intersect select col_interval from vector_setop_table_02 order by 1;

explain (verbose, costs off) select 1 from (select * from vector_setop_table_01 union all select * from vector_setop_table_01) order by 1;
select 1 from (select * from vector_setop_table_01 union all select * from vector_setop_table_01) order by 1;

reset enable_hashagg;

-- Setop with size large than 1000
select * from lineitem_mergesort except all select * from lineitem order by 1, 2, 3, 4 limit 5;

----
--- test 6: SetOp With Null Element
----
COPY VECTOR_SETOP_TABLE_01(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
\N	3	2.3	T	hash_t1	hash_t1	\N	2 day 13:24:56
\N	\N	\N	\N	\N	\N	\N	\N
\.

select * from vector_setop_table_01 intersect all select * from vector_setop_table_01 order by 1, 2, 3, 4;

----
--- test 7: Test append_merge_exec_nodes of SetOp
----
explain (verbose, costs off) select * from vector_setop_table_01 where col_inta = 2 union all select * from vector_setop_table_03 where col_intb = 5 order by 1, 2, 4;
select * from vector_setop_table_01 where col_inta = 2 union all select * from vector_setop_table_03 where col_intb = 5 order by 1, 2, 4;

----
--- Clean Resources and Tables
----
drop schema vector_setop_engine cascade;
