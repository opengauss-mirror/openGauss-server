/*
 * This file is used to test the function of ExecVecMergeJoin(): part 4: anti join
 */
set enable_hashjoin=off;
set enable_nestloop=off;
----
--- case 4: MergeJoin Full Join
----
--explain (verbose on, costs off) select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;

--select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B using(col_int) order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select * from vector_mergejoin_table_02 A full join vector_mergejoin_table_01 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select count(*) from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int;

----
--- case 5: MergeJoin Anti Join
----
SET ENABLE_SEQSCAN=ON;

explain (verbose on, costs off) select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_03 A, vector_mergejoin_engine.vector_mergejoin_table_04 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_04) order by 1, 2;

select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_03 A, vector_mergejoin_engine.vector_mergejoin_table_04 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_04) order by 1, 2;
select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_04 A, vector_mergejoin_engine.vector_mergejoin_table_03 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_03) order by 1, 2;

