set current_schema = sonic_hashjoin_test_date;

set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set analysis_options='on(HASH_CONFLICT)';
set work_mem = '4GB';

show enable_sonic_hashjoin;

select * from VEC_HASHJOIN_TABLE_01_DATE t1 join VEC_HASHJOIN_TABLE_01_DATE t2 on t1.c_int = t2.c_int and t2.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint where t1.a is not null order by 1,2,3,4,5,6,7,8,9,10,11 limit 100;

select * from VEC_HASHJOIN_TABLE_01_DATE t1 join VEC_HASHJOIN_TABLE_01_DATE t2 on t1.c_int = t2.c_int and t1.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint and t1.a = t2.a and t1.b = t2.b and t1.c = t2.c and t1.d = t2.d and t1.e = t2.e and t1.f = t2.f and t1.g = t2.g and t1.h = t2.h where t2.d is not null order by 1,2,3,4,5,6,7,8,9,10,11 limit 100; 
-- test total rows
select count(*) from VEC_HASHJOIN_TABLE_01_DATE t1 join VEC_HASHJOIN_TABLE_01_DATE t2 on t1.c_int = t2.c_int and t1.c_bigint=t2.c_bigint and t1.c_smallint = t2.c_smallint and t1.a = t2.a and t1.b = t2.b and t1.c = t2.c and t1.d = t2.d and t1.e = t2.e and t1.f = t2.f and t1.g = t2.g and t1.h = t2.h where t2.d is not null limit 100;
-- test complicate join key
select * from VEC_HASHJOIN_TABLE_01_DATE t1 join VEC_HASHJOIN_TABLE_01_DATE t2 on t1.a + 5 = t2.a + 5 and t1.d + INTERVAL '1 hour' = t2.d + INTERVAL '1 hour' and abstime(t1.h) = abstime(t2.h) where t2.e is not null and t1.c_int > 9000 order by 1,2,3,4,5,6,7,8,9,10,11 limit 100;

-- test data number of single DN > 16k.
-- build table is bigger
explain (verbose on, costs off) select /*+ leading ((t2 t1))*/ * from VEC_HASHJOIN_TABLE_02_DATE t1 join VEC_HASHJOIN_TABLE_04_DATE t2 on t1.c_int = t2.c_int limit 100;
select /*+ leading ((t2 t1))*/ t1.* from VEC_HASHJOIN_TABLE_02_DATE t1 join VEC_HASHJOIN_TABLE_04_DATE t2 on t1.c_int = t2.c_int order by 1,2,3,4,5,6,7,8,9,10,11 limit 100;
select /*+ leading ((t2 t1))*/ count(*) from VEC_HASHJOIN_TABLE_02_DATE t1 join VEC_HASHJOIN_TABLE_04_DATE t2 on t1.c_int = t2.c_int;

-- probe table is bigger
explain (verbose on, costs off) select /*+ leading ((t1 t2))*/ * from VEC_HASHJOIN_TABLE_02_DATE t1 join VEC_HASHJOIN_TABLE_04_DATE t2 on t1.c_int = t2.c_int limit 100;
select /*+ leading ((t1 t2))*/ count(*) from VEC_HASHJOIN_TABLE_02_DATE t1 join VEC_HASHJOIN_TABLE_04_DATE t2 on t1.c_int = t2.c_int limit 100;

-- test join on different data type
START TRANSACTION;
set timezone = PRC;
INSERT INTO VEC_HASHJOIN_TABLE_01_DATE VALUES(1, 2, 3, '2018-01-01', '2018-01-01', '2018-01-01', '2018-01-01', '0930'::TIMETZ, '0930'::INTERVAL, '["2018-01-02" "2018-01-01"]', '2018-01-01'::SMALLDATETIME);
INSERT INTO VEC_HASHJOIN_TABLE_01_DATE VALUES(2, 3, 4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT * FROM VEC_HASHJOIN_TABLE_01_DATE A INNER JOIN VEC_HASHJOIN_TABLE_01_DATE B ON A.A = B.B AND A.A = B.C AND A.A = B.D AND A.A = B.H;
SELECT * FROM VEC_HASHJOIN_TABLE_01_DATE A INNER JOIN VEC_HASHJOIN_TABLE_01_DATE B ON A.B = B.C AND A.B = B.D AND A.B = B.H;
SELECT * FROM VEC_HASHJOIN_TABLE_01_DATE A INNER JOIN VEC_HASHJOIN_TABLE_01_DATE B ON A.C = B.D AND A.C = B.H;
SELECT * FROM VEC_HASHJOIN_TABLE_01_DATE A INNER JOIN VEC_HASHJOIN_TABLE_01_DATE B ON A.D = B.H;
reset timezone;
ROLLBACK;

reset analysis_options;
