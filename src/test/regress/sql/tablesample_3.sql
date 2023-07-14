create schema tablesample_schema4;
set current_schema = tablesample_schema4;

create table test_tablesample(id int, name text, salary numeric);
create table test_tablesample2(id int, name text, salary numeric);

insert into test_tablesample select generate_series(1, 3000), 'row'|| generate_series(1,3000), generate_series(1, 3000);
insert into test_tablesample2 select * from test_tablesample;
analyze test_tablesample;
analyze test_tablesample2;

-- union same query, should return only 1 row
select count(*) from ((select * from test_tablesample tablesample BERNOULLI(5) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample BERNOULLI(5) REPEATABLE (200) limit 1));
select count(*) from ((select * from test_tablesample tablesample SYSTEM(20) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample SYSTEM(20) REPEATABLE (200) limit 1));

explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_hashjoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_mergejoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_material to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

explain (costs off) select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

drop table test_tablesample;
drop table test_tablesample2;
create table test_tablesample(id int, name text, salary numeric) with (orientation=column);
create table test_tablesample2(id int, name text, salary numeric) with (orientation=column);

insert into test_tablesample values(generate_series(1, 50), 'row'|| generate_series(1,50), generate_series(1, 50));
insert into test_tablesample2 values(generate_series(1, 50), 'row'|| generate_series(1,50), generate_series(1, 50));
analyze test_tablesample;
analyze test_tablesample2;

-- union same query, should return only 1 row
select count(*) from ((select * from test_tablesample tablesample BERNOULLI(10) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample BERNOULLI(10) REPEATABLE (200) limit 1));
select count(*) from ((select * from test_tablesample tablesample SYSTEM(30) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample SYSTEM(30) REPEATABLE (200) limit 1));

explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_hashjoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_mergejoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_material to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

explain (costs off) select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

drop table test_tablesample;
drop table test_tablesample2;
create table test_tablesample(id int, name text, salary numeric) with (storage_type=ustore);
create table test_tablesample2(id int, name text, salary numeric) with (storage_type=ustore);

insert into test_tablesample select generate_series(1, 500), 'row'|| generate_series(1,500), generate_series(1, 500);
insert into test_tablesample2 select * from test_tablesample;
analyze test_tablesample;
analyze test_tablesample2;

-- union same query, should return only 1 row
select count(*) from ((select * from test_tablesample tablesample BERNOULLI(10) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample BERNOULLI(10) REPEATABLE (200) limit 1));
select count(*) from ((select * from test_tablesample tablesample SYSTEM(70) REPEATABLE (200) limit 1) union (select * from test_tablesample tablesample SYSTEM(70) REPEATABLE (200) limit 1));

explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_hashjoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_mergejoin to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

set enable_material to off;
explain (costs off) select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample BERNOULLI(50) REPEATABLE (200) left join test_tablesample2 tablesample BERNOULLI(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

explain (costs off) select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;
select * from test_tablesample tablesample SYSTEM(50) REPEATABLE (200) left join test_tablesample2 tablesample SYSTEM(50) REPEATABLE (200) on test_tablesample.id=test_tablesample2.id where test_tablesample2.id is NULL;

reset search_path;
drop schema  tablesample_schema4 cascade;
