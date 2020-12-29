set current_schema = sonic_hashjoin_test_date;
set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set enable_sonic_hashagg to on;
set analysis_options='on(HASH_CONFLICT)';

set work_mem = 64;
select sum(t1.c_int),avg(t1.c_bigint) from VEC_HASHJOIN_TABLE_01_DATE t1 where t1.a is not null group by t1.c_smallint order by 1,2 limit 10;

-- test data number of single DN > 16k.
select sum(t1.c_int),avg(t1.c_bigint) from VEC_HASHJOIN_TABLE_02_DATE t1 where t1.a is not null group by t1.c_smallint, t1.d order by 1,2 limit 10;

reset work_mem;
reset analysis_options;

set current_schema = sonic_hashjoin_test_number;
set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set enable_sonic_hashagg to on;
set analysis_options='on(HASH_CONFLICT)';
set query_mem=0;
set work_mem = 64;
select sum(t1.c_int),avg(t1.c_bigint) from VEC_HASHJOIN_TABLE_01_NUM t1 where t1.a is not null group by t1.c_smallint order by 1,2 limit 10;

create table VEC_HASHAGG_TABLE_03_NUM (
C_BIGINT BIGINT,
C_SMALLINT SMALLINT,
d numeric) with (orientation = column);

insert into VEC_HASHAGG_TABLE_03_NUM select C_BIGINT,C_SMALLINT,d from VEC_HASHJOIN_TABLE_02_NUM;
insert into VEC_HASHAGG_TABLE_03_NUM select C_BIGINT,C_SMALLINT,d+1000000 from VEC_HASHJOIN_TABLE_02_NUM;
insert into VEC_HASHAGG_TABLE_03_NUM select C_BIGINT,C_SMALLINT,d+3000000 from VEC_HASHJOIN_TABLE_02_NUM;
explain performance
select max(t1.c_bigint) from VEC_HASHAGG_TABLE_03_NUM t1 group by t1.c_smallint, t1.d order by 1 limit 10;
select max(t1.c_bigint) from VEC_HASHAGG_TABLE_03_NUM t1 group by t1.c_smallint, t1.d order by 1 limit 10;
reset work_mem;
reset analysis_options;
drop table VEC_HASHAGG_TABLE_03_NUM;
