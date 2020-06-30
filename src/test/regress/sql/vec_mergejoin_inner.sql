/*
 * This file is used to test the function of ExecVecMergeJoin(): part 1: inner join
 */
set enable_hashjoin=off;
set enable_nestloop=off;
---
-- case 1: MergeJoin Inner Join
---
explain (verbose on, costs off) select A.col_int, B.col_int from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int order by 1, 2 limit 100;
select A.col_int, B.col_int from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int order by 1, 2 limit 100;
select A.col_int, B.col_int from vector_mergejoin_table_01 A join vector_mergejoin_table_02 B on(A.col_int = B.col_int) and A.col_int is null order by 1, 2;
select A.col_int, B.col_int from vector_mergejoin_table_01 A join vector_mergejoin_table_02 B on(A.col_int = B.col_int) and B.col_int is not null order by 1, 2 limit 100;
select A.col_int, B.col_int, A.col_char,B.col_char from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_char = B.col_char and A.col_vchar=B.col_vchar order by 1, 2 limit 100;
select A.col_int, sum(A.col_int) from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int group by A.col_int order by 1 limit 100;
select count(*) from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_num = B.col_num;
select A.col_timetz, B.col_timetz from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_timetz = B.col_timetz order by 1, 2 limit 10;
select A.col_interval, B.col_interval from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_interval = B.col_interval order by 1, 2 limit 10;
select A.col_tinterval, B.col_tinterval from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_tinterval = B.col_tinterval order by 1, 2 limit 10;

explain (costs off)select t1.* from VECTOR_MERGEJOIN_TABLE_08 t1 join vector_mergejoin_table_01 t2 on t1.col_int=t2.col_int;

explain (analyze on, detail on, costs off, timing off, format json) select t1.* from VECTOR_MERGEJOIN_TABLE_08 t1 join vector_mergejoin_table_01 t2 on t1.col_int=t2.col_int;
\o sort_info.text
set explain_perf_mode = pretty;
explain performance select t1.* from VECTOR_MERGEJOIN_TABLE_08 t1 join vector_mergejoin_table_01 t2 on t1.col_int=t2.col_int;
reset explain_perf_mode;
\o
\! rm sort_info.text
