set enable_nestloop=off;
set enable_mergejoin=off;

create schema tablesample_schema;
set current_schema = tablesample_schema;
create table test_tablesample_row(id int, name text, salary numeric);
create table test_tablesample_row_rep(id int, name text, salary numeric) ;
create table test_tablesample_row_part(id int, name text, salary numeric)
partition by range(id) 
(
partition p1 values less than (100), 
partition p2 values less than (200), 
partition p3 values less than (300), 
partition p4 values less than (400),
partition p5 values less than (500),
partition p6 values less than (600),
partition p7 values less than (700),
partition p8 values less than (800),
partition p9 values less than (900),
partition p10 values less than (maxvalue)
);

insert into test_tablesample_row select generate_series(1, 1000), 'row'|| generate_series(1,1000), generate_series(1, 1000);
insert into test_tablesample_row_rep select * from test_tablesample_row;
insert into test_tablesample_row_part select * from test_tablesample_row;

analyze test_tablesample_row;
analyze test_tablesample_row_rep;
analyze test_tablesample_row_part;


select count(*) from test_tablesample_row tablesample system (0);
select count(*) from test_tablesample_row tablesample system (50) repeatable (500);
select count(*) from test_tablesample_row tablesample system (50) repeatable (0);
-- repeatable is random
select count(*) from test_tablesample_row tablesample system (50) repeatable (-200);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row tablesample system (100);
select count(*) from test_tablesample_row tablesample system (100) repeatable (3);
select count(*) from test_tablesample_row tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_row tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_row where random() < 0.5;
select count(*) from test_tablesample_row tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row tablesample bernoulli (100);
select count(*) from test_tablesample_row tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_row tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_row tablesample hybrid (50, 50) repeatable (5);
explain (verbose on, costs off) 
  select id from test_tablesample_row tablesample system (50) repeatable (2);


select count(*) from test_tablesample_row_rep tablesample system (50) repeatable (500);
select count(*) from test_tablesample_row_rep tablesample system (50) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row_rep tablesample system (100);
select count(*) from test_tablesample_row_rep tablesample system (100) repeatable (3);
select count(*) from test_tablesample_row_rep tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_row_rep tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_row_rep tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row_rep tablesample bernoulli (100);
select count(*) from test_tablesample_row_rep tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_row_rep tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_row_rep tablesample hybrid (100, 50) repeatable (5);
explain (verbose on, costs off) 
  select id from test_tablesample_row_rep tablesample bernoulli (50) repeatable (2);


select count(*) from test_tablesample_row_part tablesample system (50) repeatable (500);
select count(*) from test_tablesample_row_part tablesample system (50) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row_part tablesample system (100);
select count(*) from test_tablesample_row_part tablesample system (100) repeatable (3);
select count(*) from test_tablesample_row_part tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_row_part tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_row_part tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_row_part tablesample bernoulli (100);
select count(*) from test_tablesample_row_part tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_row_part tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_row_part tablesample hybrid (100, 100) repeatable (5);
explain (verbose on, costs off) 
  select id from test_tablesample_row_part tablesample hybrid (100, 100) repeatable (5) where id > 300;

-- create view for tablesample
create view test_tablesample_v1 as
  select * from test_tablesample_row tablesample system (60) repeatable (500);
create view test_tablesample_v2 as
  select * from test_tablesample_row tablesample system (99);
\d+ test_tablesample_v1
\d+ test_tablesample_v2

-- check that collations get assigned within the tablesample arguments
select count(*) from test_tablesample_row tablesample bernoulli (('1'::text < '0'::text)::int);

-- errors
select id from test_tablesample_row tablesample foobar (1);

select id from test_tablesample_row tablesample system (null);
select id from test_tablesample_row tablesample system (50) repeatable (null);

select id from test_tablesample_row tablesample bernoulli (-1);
select id from test_tablesample_row tablesample bernoulli (200);
select id from test_tablesample_row tablesample system (-1);
select id from test_tablesample_row tablesample system (200);
select id from test_tablesample_row tablesample hybrid (200, -1);
select id from test_tablesample_row tablesample hybrid (100);


with query_select as (select * from test_tablesample_row)
select * from query_select tablesample bernoulli (5.5) repeatable (1);

select q.* from (select * from test_tablesample_row) as q tablesample bernoulli (5);
select a.*  from test_tablesample_row_part partition(p1) a tablesample bernoulli (5);

explain verbose select * from test_tablesample_row tablesample system (50) repeatable (500);

-- hdfs table and foreign table unsupport tablesample

reset search_path;
drop schema  tablesample_schema cascade;
