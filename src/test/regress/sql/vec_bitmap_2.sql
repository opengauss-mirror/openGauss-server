/*
 * This file is used to test the function of ExecCstoreIndexAnd()/ExecCstoreIndexOr()--vec_bitmap, only for partition table
 */
----
--- Create Table and Insert Data
----
set current_schema=vector_bitmap_engine;
set enable_indexscan=off;
set enable_seqscan=off;

\parallel on 4
analyze vector_bitmap_table_02;
analyze vector_bitmap_table_03;
analyze vector_bitmap_table_04;
analyze vector_bitmap_table_05;
\parallel off

----
--- case 2: Partitioned Ctid + Heap + And + Or
----
explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

----
--- case 3 : Rescan Ctid + Heap + And + Or
----
set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_material = off;

explain (verbose on, costs off)
select t1.c_city from vector_bitmap_engine.vector_bitmap_table_04 t1 where t1.c_city like 'a%' and t1.c_city in (select t2.wd_varchar from vector_bitmap_engine.vector_bitmap_table_05 t2 where t2.wd_int < 1000 and t2.wd_char like '%a%');
select t1.c_city from vector_bitmap_engine.vector_bitmap_table_04 t1 where t1.c_city like 'a%' and t1.c_city in (select t2.wd_varchar from vector_bitmap_engine.vector_bitmap_table_05 t2 where t2.wd_int < 1000 and t2.wd_char like '%a%') order by 1;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int2=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int2=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int1=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int1=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

----
--- case 4 : Plus Expr
----
explain (verbose on, costs off)
select * from vector_bitmap_table_03 where (col_int1 in (2, 9, 21) and col_num in (1.5, 2.3) and length(col_txt) < 10) and col_int2 < 10 order by 1, 2, 3, 4;
select * from vector_bitmap_table_03 where (col_int1 in (2, 9, 21) and col_num in (1.5, 2.3) and length(col_txt) < 10) and col_int2 < 10 order by 1, 2, 3, 4;
