set enable_nestloop=off;
set enable_mergejoin=off;

create schema tablesample_schema_2;
set current_schema = tablesample_schema_2;

create table test_tablesample_col(name text, id int, salary numeric) with (orientation=column) ;
create table test_tablesample_col_rep(id int, name text, salary numeric) with (orientation=column) ;
create table test_tablesample_col_part(name text, id int, salary numeric)
with (orientation=column) ;
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

insert into test_tablesample_col select 'col'|| generate_series(1,1000), generate_series(1, 1000), generate_series(1, 1000);
insert into test_tablesample_col_rep select generate_series(1, 1000), 'row'|| generate_series(1,1000), generate_series(1, 1000);
vacuum full test_tablesample_col_rep;
insert into test_tablesample_col_part select * from test_tablesample_col;

analyze test_tablesample_col;
analyze test_tablesample_col_rep;
analyze test_tablesample_col_part;

select count(*) from test_tablesample_col tablesample system (0);
select count(*) from test_tablesample_col tablesample system (50) repeatable (500);
select count(*) from test_tablesample_col tablesample system (50) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col tablesample system (100);
select count(*) from test_tablesample_col tablesample system (100) repeatable (3);
select count(*) from test_tablesample_col tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_col tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_col tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col tablesample bernoulli (100);
select count(*) from test_tablesample_col tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_col tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_col tablesample hybrid (50, 50) repeatable (50);
explain (verbose on, costs off) 
  select id from test_tablesample_col tablesample bernoulli (50) repeatable (2);


select count(*) from test_tablesample_col_rep tablesample system (50) repeatable (500);
select count(*) from test_tablesample_col_rep tablesample system (50) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col_rep tablesample system (100);
select count(*) from test_tablesample_col_rep tablesample system (100) repeatable (3);
select count(*) from test_tablesample_col_rep tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_col_rep tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_col_rep tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col_rep tablesample bernoulli (100);
select count(*) from test_tablesample_col_rep tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_col_rep tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_col_rep tablesample hybrid (100, 50) repeatable (50);
explain (verbose on, costs off) 
  select id from test_tablesample_col_rep tablesample bernoulli (50) repeatable (2);


select count(*) from test_tablesample_col_part tablesample system (50) repeatable (500);
select count(*) from test_tablesample_col_part tablesample system (50) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col_part tablesample system (100);
select count(*) from test_tablesample_col_part tablesample system (100) repeatable (3);
select count(*) from test_tablesample_col_part tablesample system (100) repeatable (0.4);
select count(*) from test_tablesample_col_part tablesample bernoulli (50) repeatable (200);
select count(*) from test_tablesample_col_part tablesample bernoulli (5.5) repeatable (0);
-- 100% should give repeatable count results (ie, all rows) in any case
select count(*) from test_tablesample_col_part tablesample bernoulli (100);
select count(*) from test_tablesample_col_part tablesample bernoulli (100) repeatable (0);
select count(*) from test_tablesample_col_part tablesample bernoulli (100) repeatable (2.3);
select count(*) from test_tablesample_col_part tablesample hybrid (100, 50) repeatable (50);
explain (verbose on, costs off) 
  select id from test_tablesample_col_part tablesample bernoulli (50) repeatable (2);

reset search_path;
drop schema  tablesample_schema_2 cascade;
