set current_schema = sonic_hashjoin_test_number;

set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set analysis_options='on(HASH_CONFLICT)';
set work_mem = '4GB';

show enable_sonic_hashjoin;

CREATE TABLE sonic_hashjoin_test_number.VEC_HASHJOIN_TABLE_01_NUM(
C_INT INT,
C_BIGINT BIGINT,
C_SMALLINT SMALLINT,
a FLOAT,
b FLOAT4,
c FLOAT8,
d numeric,
e numeric(20,2),
f decimal,
g decimal(40,2),
h real,
i double precision,
j boolean
) with(orientation = column) 
partition by range (C_INT)
(
partition location_1 values less than (0),
partition location_2 values less than (1000),
partition location_3 values less than (2000),
partition location_4 values less than (5000),
partition location_5 values less than (7000),
partition location_7 values less than (maxvalue)
);

CREATE TABLE sonic_hashjoin_test_number.VEC_HASHJOIN_TABLE_02_NUM(
C_INT INT,
C_BIGINT BIGINT,
C_SMALLINT SMALLINT,
a FLOAT,
b FLOAT4,
c FLOAT8,
d numeric,
e numeric(20,2),
f decimal,
g decimal(40,2),
h real,
i double precision,
j boolean
) with(orientation = column) ;

CREATE TABLE sonic_hashjoin_test_number.VEC_HASHJOIN_TABLE_03_NUM(
C_INT INT
) with(orientation = column) ;

CREATE TABLE sonic_hashjoin_test_number.VEC_HASHJOIN_TABLE_04_NUM(
C_INT INT,
C_BIGINT BIGINT,
C_SMALLINT SMALLINT,
a FLOAT,
b FLOAT4,
c FLOAT8,
d numeric,
e numeric(20,2),
f decimal,
g decimal(40,2),
h real,
i double precision,
j boolean
) with(orientation = column) ;

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

-- test different join key type
select * from VEC_HASHJOIN_TABLE_03_NUM t1 join VEC_HASHJOIN_TABLE_04_NUM t2 on t1.c_int = t2.c_bigint and t1.c_int = t2.c_smallint and t1.c_int = t2.a and t1.c_int = t2.b and t1.c_int = t2.c and t1.c_int = t2.d and t1.c_int = t2.e and t1.c_int = t2.f and t1.c_int = t2.g and t1.c_int = t2.h and t1.c_int = t2.i and t1.c_int::boolean = t2.j order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

select * from VEC_HASHJOIN_TABLE_04_NUM t1 join VEC_HASHJOIN_TABLE_04_NUM t2 on t1.a = t2.d and t1.a = t2.e and t1.d = t2.g and t1.d = t2.h and t1.d = t2.j and t1.h = t2.j and t1.e = t2.g and t1.g = t2.i order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

reset analysis_options;
