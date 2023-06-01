---------------------------
-- prepare data
---------------------------
create schema upserttest;
set current_schema to upserttest;
create table data1(a int);
create table data2(a int, b int);
create table aa (a1 int primary key, a2 int, a3 int not NULL, a4 int unique);
create table ff (f1 int primary key, f2 int references aa, f3 int);
create table gg (g1 int primary key, g2 int, g3 int);
create table hh (h1 int primary key, h2 int, h3 int);
insert into data1 values(1),(2),(3),(4),(1),(2),(3),(4),(5);
insert into data2 values(1,2),(2,3),(3,4),(4,5),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6);
insert into aa values (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4);
insert into ff values (1,1),(2,2),(3,3),(4,4);
insert into gg values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5);
insert into hh select generate_series(1,100000), generate_series(2,100001), generate_series(3,100002);
insert into hh select h1 + 100000, h2, h3 from hh;
analyze data1;
analyze data2;
analyze aa;
analyze ff;
analyze gg;
analyze hh;

---------------------------
-- basic test
---------------------------
insert into aa values(1,1,1,1) on duplicate key update a2 = (select a from data1 where a > 2 limit 1);   -- suc
explain (costs off, verbose) insert into aa values(1,1) on duplicate key update a2 = (select a from data1 where a > 2 limit 1);
insert into aa values(2,2,2,2) on duplicate key update a2 = (select * from data1 order by a ASC limit 1) + (select * from data1 order by a DESC limit 1); -- suc
explain (costs off, verbose) insert into aa values(2,2) on duplicate key update a2 = (select * from data1 order by a ASC limit 1) + (select * from data1 order by a DESC limit 1);
insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data1);                        -- err, muilt row
insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data2 limit 1);                -- err, muilt column
insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data2 limit 1).b;              -- err, not support, reference follow
update aa set a2 = ((select * from data2 limit 1).b) where a1 = 1;                                        -- err
insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data1 where a > 5);            -- suc, 0 row means NULL
explain (costs off, verbose) insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data1 where a > 5);
insert into aa values(3,3,3,3) on duplicate key update a2 = (select * from data2 where a > 5);            -- err, muilt column, even 0 row
insert into aa values(4,4,4,4) on duplicate key update a2 = (select * from data1 where a > 5) + 1;        -- suc, 0 row + 1 means NULL
select * from aa order by a1;

insert into ff values(1,1,1) on duplicate key update f2 = (select a + 10 from data1 where a > 2 limit 1); -- err, invalid fk
insert into aa values(1,1,1,1) on duplicate key update a3 = (select a from data1 where a > 10 limit 1);   -- err, a3 is not null
insert into aa values(1,1,1,1) on duplicate key update a4 = (select a from data1 where a = 3 limit 1);    -- err, a4 is unique key
select * from ff order by f1;
select * from aa order by a1;

---------------------------
-- muilt-set
---------------------------
insert into gg values(1,1,1) on duplicate key update (g2, g3) = select * from data2 where a = 5;           -- err, invalid syntax
insert into gg values(1,1,1) on duplicate key update (g2, g3) = (select * from data2 where a = 5 limit 1); -- err, there is some syntax limitations for muilt-set, limit is not allowed;
insert into gg values(1,1,1) on duplicate key update (g2, g3) = (with tmptb(c1, c2) as (select * from data2) select * from tmptb limit 1); -- err, there is some syntax limitations;
insert into gg values(2,2,2) on duplicate key update (g2, g3) = (select a, b from data2 where a = 5);      -- suc
explain (costs off, verbose) insert into gg values(2,2,2) on duplicate key update (g2, g3) = (select a, b from data2 where a = 5);
insert into gg values(3,3,3) on duplicate key update (g2, g3) = (select a, a, b from data2 where a = 5);   -- err, columns not match
update gg set (g2) = (select * from data2 where a = 5) where g1 = 3;                                       -- suc, maybe, it could be a bug, we only update one column, and * means two.
insert into gg values(4,4,4) on duplicate key update (g2) = (select * from data2 where a = 5);             -- suc, only g2 be updated
insert into gg values(5,5,5) on duplicate key update (g2) = (select * from data2 where a = 7);             -- suc, only g2 be updated to NULL
explain (costs off, verbose) insert into gg values(5,5,5) on duplicate key update (g2) = (select * from data2 where a = 7);
select * from gg order by g1;

---------------------------
-- complex subquery
---------------------------
insert into aa values(1,1,1,1) on duplicate key update a2 = (select max(a) from data1);                            -- suc
insert into aa values(2,2,2,2) on duplicate key update a2 = (select max(b) from data2 group by a limit 1);         -- suc
insert into aa values(3,3,3,3) on duplicate key update a2 = (select a from data2 except select a from data2) + 5;  -- suc
insert into aa values(4,4,4,4) on duplicate key update a2 = (with tmptb(c1) as (select * from data1) select data2.b from data2 where data2.a not in (select c1 from tmptb));  -- suc
explain (costs off, verbose) insert into aa values(1,1,1,1) on duplicate key update a2 = (select max(a) from data1);
explain (costs off, verbose) insert into aa values(2,2,2,2) on duplicate key update a2 = (select max(b) from data2 group by a limit 1);
explain (costs off, verbose) insert into aa values(3,3,3,3) on duplicate key update a2 = (select a from data2 except select a from data1) + 5;
explain (costs off, verbose) insert into aa values(4,4,4,4) on duplicate key update a2 = (with tmptb(c1) as (select * from data1) select data2.b from data2 where data2.a not in (select c1 from tmptb));
select * from aa order by a1;

insert into aa values(1,1,1,1) on duplicate key update a2 = (1 in (1,2,3));                                  -- suc, true means 1
insert into aa values(2,2,2,2) on duplicate key update a2 = (5 not in (select * from data1));                -- suc, false means 0
insert into aa values(3,3,3,3) on duplicate key update a2 = (select a from data1 except select a1 from aa);  -- suc, only one row
insert into aa values(4,4,4,4) on duplicate key update a2 = (select min (data2.a) from data1 left join data2 on data1.a = data2.a - 1 where data1.a in (select a1 from aa));  -- suc
explain (costs off, verbose) insert into aa values(1,1,1,1) on duplicate key update a2 = (1 not in (1,2,3));
explain (costs off, verbose) insert into aa values(2,2,2,2) on duplicate key update a2 = (5 not in (select * from data1));
explain (costs off, verbose) insert into aa values(3,3,3,3) on duplicate key update a2 = (select a from data1 except select a1 from aa);
explain (costs off, verbose) insert into aa values(4,4,4,4) on duplicate key update a2 = (select min (data2.a) from data1 left join data2 on data1.a = data2.a - 1 where data1.a in (select a1 from aa));
select * from aa order by a1;

------------------------------
-- pbe
------------------------------
prepare sub1 as select a from data1 where a = $1 limit 1;
insert into aa values(1,3,3,3) on duplicate key update a2 = (select sub(3));   -- err, invalid syntax
insert into aa values(1,3,3,3) on duplicate key update a2 = (execute sub(3));  -- err, invalid syntax

prepare sub2 as insert into aa values(1,1,1,1) on duplicate key update a2 =(select max(a) from data1);
execute sub2;                                                                  -- suc
explain (costs off, verbose) execute sub2;

prepare sub3 as insert into aa values($1,0,0,0) on duplicate key update a2 =( select count(*)+ excluded.a1 from hh);
execute sub3(2);                                                               -- suc
explain (costs off, verbose) execute sub3(2);

select * from aa order by a1;

------------------------------
-- hint
------------------------------
explain select /*+tablescan(hh) */ h1 from hh where h1 = 2000;                                                                       -- should be a seqscan plan
insert into aa values(1,3,3,3) on duplicate key update a2 = (select /*+tablescan(hh) */ h1 from hh where h1 = 2000);                 -- suc
explain (costs off, verbose) insert into aa values(1,3,3,3) on duplicate key update a2 = (select /*+tablescan(hh) */ h1 from hh where h1 = 2000);
explain (costs off, verbose) insert into aa values(1,3,3,3) on duplicate key update a2 = (select  h1 from hh where h1 = 2000);
select * from aa order by a1;

------------------------------
-- bypass and smp
------------------------------
explain (costs off, verbose) select h1 from hh where h1 = 20;                                                                        -- should be a bypass query
insert into aa values(2,2,2,2) on duplicate key update a2 = (select h1 from hh where h1 = 20);                                       -- suc
explain (costs off, verbose) insert into aa values(2,3,3,3) on duplicate key update a2 = (select h1 from hh where h1 = 20);          -- upsert not support bypass, so subquery is not a bypass, too.
set query_dop = 2;
explain (costs off, verbose) select count(*) from hh;                                                                                -- should be a smp query
set enable_seqscan_dopcost = off;
explain (costs off, verbose) select count(*) from hh;                                                                                -- should not be a smp query
insert into aa values(3,3,3,3) on duplicate key update a2 = (select count(*) from hh);                                               -- suc
explain (costs off, verbose) insert into aa values(3,3,3,3) on duplicate key update a2 = (select count(*) from hh);                  -- subquery is not execute by smp, but there still is a two-level agg.

explain (costs off, verbose) select 4,count(*), 4, 4 from hh;                                                                        -- should not be a smp query
set enable_seqscan_dopcost = on;
explain (costs off, verbose) select 4,count(*), 4, 4 from hh;                                                                        -- should be a smp query
insert into aa select 4,count(*), 4, 4 from hh on duplicate key update a2 = (select count(*) from hh);                               -- suc
explain (costs off, verbose) insert into aa select 4,count(*), 4, 4 from hh on duplicate key update a2 = (select count(*) from hh);  -- insert-part is a smp plan, but update-part not
set enable_seqscan_dopcost = off;
explain (costs off, verbose) insert into aa select 4,count(*), 4, 4 from hh on duplicate key update a2 = (select count(*) from hh);  -- neither insert-part is a smp plan, nor update-part

prepare sub4 as insert into aa select $1, count(*), 4, 4 from hh on duplicate key update a2 =(select count(*) + $1 from hh);
execute sub4(1);                                                                                                                     -- suc
explain (costs off, verbose) execute sub4(1);                                                                                        -- insert-part is a smp plan, but update-part not
set enable_seqscan_dopcost = on;
explain (costs off, verbose) execute sub4(1);                                                                                        -- insert-part is a smp plan, but update-part not

set query_dop = 1;
select * from aa order by a1;

execute sub4(2);                                                                                                                     -- suc
explain (costs off, verbose) execute sub4(2);                                                                                        -- the plan was rebuilt, not a smp any more.
select * from aa order by a1;

------------------------------
-- subquery has excluded
------------------------------
insert into gg values(1,1) on duplicate key update g2 = (select max(a) from data1 where a > excluded.g2);                  -- suc
insert into gg values(2,2) on duplicate key update g2 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2); -- suc
insert into gg values(4,3,1) on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));  -- suc
explain (costs off, verbose) insert into gg values(1,1) on duplicate key update g2 = (select max(a) from data1 where a > excluded.g2); 
explain (costs off, verbose) insert into gg values(2,2) on duplicate key update g2 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2);
explain (costs off, verbose) insert into gg values(4,3,1) on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));
select * from gg order by g1;

insert into gg select *,1 from data2 on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1)); -- suc
explain (costs off, verbose) insert into gg select *,1 from data2 on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));
select * from gg order by g1;

insert into gg select * from data2 on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));  -- suc
explain (costs off, verbose) insert into gg select * from data2 on duplicate key update (g2,g3) = (select min (data2.a), max(data2.b) - excluded.g3 from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));
select * from gg order by g1;

insert into gg values(2,2) on duplicate key update g2 = (excluded.g2 not in (select * from data1)); -- suc
explain (costs off, verbose) insert into gg values(2,2) on duplicate key update g2 = (excluded.g2 not in (select * from data1));
select * from gg order by g1;

set query_dop = 2;
prepare sub5 as insert into aa select $1, count(*), 4, 4 from hh on duplicate key update a2 =(select count(*) * excluded.a1 + $1 from hh);
execute sub5(1);                                                                                                                     -- suc
explain (costs off, verbose) execute sub5(1);                                                                                        -- insert-part is a smp plan, but update-part not
set query_dop = 1;
execute sub5(2);                                                                                                                     -- suc
explain (costs off, verbose) execute sub5(2);                                                                                        -- the plan was rebuilt, not a smp any more.
select * from aa order by a1;

------------------------------
-- column-table, partition table, mtview
------------------------------
create table bb (b1 int primary key, b2 int) with (orientation=column);
insert into bb values(1,1),(2,2),(3,3),(4,4);
insert into bb values(1,1),(2,2) on duplicate key update b2 = (select max(a) from data1 where a > excluded.g2);

create table dd (d1 int primary key, d2 int, d3 int) partition by range(d1) (partition dd1 values less than (10), partition dd2 values less than (20),partition dd3 values less than (maxvalue));
insert into dd values (1,1),(2,2),(3,3),(4,4),(11,11),(12,12),(13,13),(14,14),(21,21),(22,22),(33,33),(44,44);
insert into dd values(1,1) on duplicate key update d2 = (select max(a) from data1 where a > excluded.g2);
explain (costs off, verbose) insert into gg values(1,1) on duplicate key update g2 = (select max(a) from data1 where a > excluded.g2);
insert into dd values(2,2) on duplicate key update d3 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2);
explain (costs off, verbose) insert into gg values(2,2) on duplicate key update g2 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2); 
insert into dd values(3,3) on duplicate key update d3 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));
explain (costs off, verbose) insert into gg values(3,3) on duplicate key update g2 = (select max(data2.b) from data1 left join data2 on data1.a = data2.a - excluded.g2 where data1.a in (1,2, excluded.g1));
select * from dd;


------------------------------
-- transaction
------------------------------
begin;
insert into gg values(1,1) on duplicate key update g2 = (select max(a) + 1 from data1 where a > excluded.g2);
rollback;
select * from gg order by g1;

------------------------------
-- priviliege
------------------------------
CREATE USER upsert_subquery_tester PASSWORD '123456@cc';
SET SESSION SESSION AUTHORIZATION upsert_subquery_tester PASSWORD '123456@cc';
select * from data1;                                                                                        -- must have no permission
create table gg (g1 int primary key, g2 int, g3 int);
insert into gg values (1,1),(2,2),(3,3),(4,4);
insert into gg values(1,1) on duplicate key update g2 = (select max(a) from data1 where a > excluded.g2);   -- err
insert into gg values(1,1) on duplicate key update g2 = (select 1 from data1 limit 1);                      -- err

------------------------------
-- clean up
------------------------------
\c
drop user upsert_subquery_tester cascade;
drop schema upserttest cascade;
