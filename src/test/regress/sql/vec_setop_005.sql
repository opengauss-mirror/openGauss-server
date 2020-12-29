/*
 * This file is used to test the function of ExecVecSetOp
 */
set current_schema=vector_setop_engine;

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

COPY VECTOR_SETOP_TABLE_05(col_inta, col_intb, col_num, col_char, col_varchar, col_text, col_time, col_interval) FROM stdin;
\N	3	2.3	T	hash_t1	hash_t1	\N	2 day 13:24:56
\N	\N	\N	\N	\N	\N	\N	\N
\.

select * from vector_setop_table_05 intersect all select * from vector_setop_table_05 order by 1, 2, 3, 4;

----
--- test 7: Test append_merge_exec_nodes of SetOp
----
explain (verbose, costs off) select * from vector_setop_table_05 where col_inta = 2 union all select * from vector_setop_table_03 where col_intb = 5 order by 1, 2, 4;
select * from vector_setop_table_05 where col_inta = 2 union all select * from vector_setop_table_03 where col_intb = 5 order by 1, 2, 4;
