set enable_nestloop=off;
set enable_mergejoin=off;

create schema tablesample_schema_3;
set current_schema = tablesample_schema_3;

-- test for null/distinct and other data characteristic
create table test_tablesample_01(c1 int, c2 numeric) ;
create table test_tablesample_02(c1 bigint, c2 int) ;
create table test_tablesample_target(c1 int, c2 int) ;
insert into test_tablesample_01 select generate_series(1,1000)%8, generate_series(1,1000)%7;
insert into test_tablesample_02 select generate_series(1,1000)%10, generate_series(1,1000)%12;
update test_tablesample_02 set c1 = null where c1 between 4 and 7;

analyze test_tablesample_01;
analyze test_tablesample_02;

select distinct c1 from test_tablesample_01 tablesample system (100) repeatable (100) order by 1;
update test_tablesample_01 set c2 = null where c1 between 4 and 7;

select distinct c1 from (select * from test_tablesample_01 tablesample bernoulli (100)) order by 1;

explain (verbose on, costs off) 
with query_select_01 as (select * from test_tablesample_01 tablesample bernoulli (100) where c1 > 5),
     query_select_02 as (select * from test_tablesample_02 tablesample bernoulli (100) where c1 > 5)
select q1.c1, q2.c2 from query_select_01 q1, query_select_02 q2 where q1.c1 = q2.c1;

explain (verbose on, costs off) 
with query_select_01 as (select * from test_tablesample_01 tablesample bernoulli (100) where c1 > 2),
     query_select_02 as (select * from test_tablesample_02 tablesample bernoulli (100) where c1 < 7)
select q1.c1, q2.c2 from query_select_01 q1 left join query_select_02 q2 on q1.c1 = q2.c1 and q1.c2 = q2.c2;

explain (verbose on, costs off)
with query_select_01 as (select * from test_tablesample_01 tablesample bernoulli (100) where c1 > 2),
     query_select_02 as (select * from test_tablesample_02 tablesample bernoulli (100) where c1 < 7)
insert into test_tablesample_target select q1.c1, q2.c2 from query_select_01 q1 join query_select_02 q2 on q1.c1 = q2.c1 and q1.c2 = q2.c2
 union all select q1.c1, q2.c2 from query_select_01 q1 join query_select_02 q2 on q1.c1 = q2.c2 and q1.c2 = q2.c1;

with query_select_01 as (select * from test_tablesample_01 tablesample bernoulli (100) where c1 > 2),
     query_select_02 as (select * from test_tablesample_02 tablesample bernoulli (100) where c1 < 7)
insert into test_tablesample_target select q1.c1, q2.c2 from query_select_01 q1 join query_select_02 q2 on q1.c1 = q2.c1 and q1.c2 = q2.c2
 union all select q1.c1, q2.c2 from query_select_01 q1 join query_select_02 q2 on q1.c1 = q2.c2 and q1.c2 = q2.c1;

explain (verbose on, costs off, nodes on)
update test_tablesample_target t1 set c1 = c2+1 where exists (select 1 from test_tablesample_01 t2 tablesample bernoulli (100) where t2.c1<10 and t1.c1=t2.c1 and t1.c2=t2.c2);

explain (verbose on, costs off)
select c1, c2, count(*), sum(c1), count(c2), max(c1), c2 from
(select c1, c2 from test_tablesample_01 tablesample bernoulli (100) group by c1, c2 limit 10) group by c1, c2;

reset search_path;
drop schema  tablesample_schema_3 cascade;
