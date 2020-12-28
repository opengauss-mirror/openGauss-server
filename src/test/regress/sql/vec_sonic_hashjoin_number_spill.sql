-- To guarantee that all the tests (except for cound(*))
-- are spilt or respilt, we had better to execute explain performance first.
-- However, this will cost much longer time.
-- Thus, we constructed those spill cases offline to avoid explain performance here.
-- When any memory related modification committed,
-- please make sure all the cases are still spilt or respilt
-- by playing explain performance per case.

set current_schema = sonic_hashjoin_test_number;

set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set query_mem = 0;

-- test spill
set work_mem = '5MB';

select * from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.c_int = t2.c_int and t2.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint where t1.a is not null order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 limit 1000;

select * from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.c_int = t2.c_int and t1.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint and t1.a = t2.a and t1.b = t2.b and t1.c = t2.c and t1.d = t2.d and t1.e = t2.e and t1.f = t2.f and t1.g = t2.g and t1.h = t2.h and t1.i = t2.i and t1.j = t2.j where t2.d is not null order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 limit 500;

-- test total rows
select count(*) from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.c_int = t2.c_int and t1.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint and t1.a = t2.a and t1.b = t2.b and t1.c = t2.c and t1.d = t2.d and t1.e = t2.e and t1.f = t2.f and t1.g = t2.g and t1.h = t2.h and t1.i = t2.i and t1.j = t2.j where t2.d is not null limit 500;

-- test complicate join key
select * from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.a + 5 = t2.a + 5 and t1.d - 3 = t2.d - 3 and t1.f * 5 = t2.f * 5 and t1.g + 1.23 = t2.g + 1.23 and t1.h = t2.h where t2.j is not null order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 limit 500;

-- test data number of single DN > 16k.
-- build table is bigger
explain (verbose on, costs off) select /*+ leading ((t2 t1))*/ * from VEC_HASHJOIN_TABLE_02_NUM t1 join VEC_HASHJOIN_TABLE_03_NUM t2 on t1.c_int = t2.c_int limit 100;
select /*+ leading ((t2 t1))*/ t1.* from VEC_HASHJOIN_TABLE_02_NUM t1 join VEC_HASHJOIN_TABLE_03_NUM t2 on t1.c_int = t2.c_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13 limit 100;
select /*+ leading ((t2 t1))*/ count(*) from VEC_HASHJOIN_TABLE_02_NUM t1 join VEC_HASHJOIN_TABLE_03_NUM t2 on t1.c_int = t2.c_int;

-- probe table is bigger
explain (verbose on, costs off) select /*+ leading ((t1 t2))*/ * from VEC_HASHJOIN_TABLE_02_NUM t1 join VEC_HASHJOIN_TABLE_03_NUM t2 on t1.c_int = t2.c_int limit 100;
select /*+ leading ((t1 t2))*/ count(*) from VEC_HASHJOIN_TABLE_02_NUM t1 join VEC_HASHJOIN_TABLE_03_NUM t2 on t1.c_int = t2.c_int limit 100;

-- test respill
set work_mem = 1320;

select * from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.c_int = t2.c_int and t2.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint where t1.a is not null order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 limit 1000;

select * from VEC_HASHJOIN_TABLE_01_NUM t1 join VEC_HASHJOIN_TABLE_01_NUM t2 on t1.c_int = t2.c_int and t1.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint and t1.a = t2.a and t1.b = t2.b and t1.c = t2.c and t1.d = t2.d and t1.e = t2.e and t1.f = t2.f and t1.g = t2.g and t1.h = t2.h and t1.i = t2.i and t1.j = t2.j where t2.e is not null order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 limit 1000;

reset query_mem;
reset work_mem;
