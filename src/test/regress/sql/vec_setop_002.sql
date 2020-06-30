/*
 * This file is used to test the function of ExecVecSetOp
 */
set current_schema=vector_setop_engine;

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
