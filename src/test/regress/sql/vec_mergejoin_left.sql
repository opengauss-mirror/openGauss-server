/*
 * This file is used to test the function of ExecVecMergeJoin(): part 2: left join
 */
set enable_hashjoin=off;
set enable_nestloop=off;
---
-- case 2: MergeJoin Left Join
---
explain (verbose on, costs off) select * from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;

select * from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;
select count(*) from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int;
select * from vector_mergejoin_table_02 A left join vector_mergejoin_table_01 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;
select count(*) from vector_mergejoin_table_02 A left join vector_mergejoin_table_01 B using(col_int);

