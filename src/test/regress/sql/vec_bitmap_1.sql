/*
 * This file is used to test the function of ExecCstoreIndexAnd()/ExecCstoreIndexOr()--vec_bitmap, for ordinary table
 */
----
--- Create Table and Insert Data
----
set current_schema=vector_bitmap_engine;
set enable_indexscan=off;
set enable_seqscan=off;

\parallel on 2
analyze vector_bitmap_table_01;
analyze vector_bitmap_table_02;
\parallel off

----
--- case 1: Ctid + Heap + And + Or
----
explain (verbose on, costs off)
select count(1) from vector_bitmap_table_01; 
select count(1) from vector_bitmap_table_01; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

----
--- case 3 : Rescan Ctid + Heap + And + Or
----
set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_material = off;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 and t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 and t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
 
explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 or t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 or t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;