set enable_nestloop=off;
set enable_mergejoin=off;

create schema schema_sample_heap_tbl;
set current_schema = schema_sample_heap_tbl;
create table heap_tablesample_row(id int, name text, salary numeric);
create table heap_tablesample_row_part(id int, name text, salary numeric)
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

insert into heap_tablesample_row select v, 'row'|| v, v from generate_series(1, 1000) as v;

insert into heap_tablesample_row_part select * from heap_tablesample_row;

analyze heap_tablesample_row;
-- analyze heap_tablesample_row_part;


select count(*) from heap_tablesample_row tablesample system (0);
select count(*) from heap_tablesample_row tablesample system (50) repeatable (500);
select count(*) from heap_tablesample_row tablesample system (50) repeatable (2);

-- repeatable is random
select count(*) from heap_tablesample_row tablesample system (50) repeatable (-200);

-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from heap_tablesample_row tablesample system (100);
select count(*) from heap_tablesample_row tablesample system (100) repeatable (3);
select count(*) from heap_tablesample_row tablesample system (100) repeatable (0.4);
select count(*) from heap_tablesample_row tablesample bernoulli (50) repeatable (200);


select count(*) from heap_tablesample_row tablesample bernoulli (5.5) repeatable (1);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from heap_tablesample_row tablesample bernoulli (100);
select count(*) from heap_tablesample_row tablesample bernoulli (100) repeatable (0);
select count(*) from heap_tablesample_row tablesample bernoulli (100) repeatable (2.3);
select count(*) from heap_tablesample_row tablesample hybrid (50, 50) repeatable (5);
explain (verbose on, costs off) 
  select id from heap_tablesample_row tablesample system (50) repeatable (2);

explain (verbose on, costs off) 
  select id from heap_tablesample_row tablesample system (50) repeatable (2);
  
select count(*) from heap_tablesample_row_part tablesample system (50) repeatable (500);
select count(*) from heap_tablesample_row_part tablesample system (50) repeatable (1);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from heap_tablesample_row_part tablesample system (100);
select count(*) from heap_tablesample_row_part tablesample system (100) repeatable (3);
select count(*) from heap_tablesample_row_part tablesample system (100) repeatable (0.4);
select count(*) from heap_tablesample_row_part tablesample bernoulli (50) repeatable (200);
select count(*) from heap_tablesample_row_part tablesample bernoulli (5.5) repeatable (-1);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from heap_tablesample_row_part tablesample bernoulli (100);
select count(*) from heap_tablesample_row_part tablesample bernoulli (100) repeatable (0);
select count(*) from heap_tablesample_row_part tablesample bernoulli (100) repeatable (2.3);
select count(*) from heap_tablesample_row_part tablesample hybrid (100, 100) repeatable (5);
explain (verbose on, costs off) 
  select id from heap_tablesample_row_part tablesample hybrid (100, 100) repeatable (5) where id > 300;

-- create view for tablesample
create view test_tablesample_v1 as
  select * from heap_tablesample_row tablesample system (60) repeatable (500);
create view test_tablesample_v2 as
  select * from heap_tablesample_row tablesample system (99);
\d+ test_tablesample_v1
\d+ test_tablesample_v2

-- check that collations get assigned within the tablesample arguments
select count(*) from heap_tablesample_row tablesample bernoulli (('1'::text < '0'::text)::int);

-- errors
select id from heap_tablesample_row tablesample foobar (1);

select id from heap_tablesample_row tablesample system (null);
select id from heap_tablesample_row tablesample system (50) repeatable (null);

select id from heap_tablesample_row tablesample bernoulli (-1);
select id from heap_tablesample_row tablesample bernoulli (200);
select id from heap_tablesample_row tablesample system (-1);
select id from heap_tablesample_row tablesample system (200);
select id from heap_tablesample_row tablesample hybrid (200, -1);
select id from heap_tablesample_row tablesample hybrid (100);


with query_select as (select * from heap_tablesample_row)
select * from query_select tablesample bernoulli (5.5) repeatable (1);

select q.* from (select * from heap_tablesample_row) as q tablesample bernoulli (5);
select a.*  from heap_tablesample_row_part partition(p1) a tablesample bernoulli (5);

set enable_fast_query_shipping=on;
explain verbose select * from heap_tablesample_row tablesample system (50) repeatable (500);

reset search_path;
drop schema schema_sample_heap_tbl cascade;
