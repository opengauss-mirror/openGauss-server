create schema mulcolpk;
set current_schema to mulcolpk;
create table mulcolpk (a int, b int);
insert into mulcolpk values (generate_series(0, 0), generate_series(1, 90));
insert into mulcolpk values (generate_series(1, 10), generate_series(0, 0));
analyze mulcolpk;

--we just test row estimate in index path.
set enable_seqscan = off;
set enable_bitmapscan = off;
set plan_cache_mode = force_generic_plan;

--1. create index 
create index mulcolpk_idx on mulcolpk(a, b);
explain select * from mulcolpk where a = 0 and b = 0;

--2. create unique index
drop index mulcolpk_idx;
create unique index mulcolpk_idx on mulcolpk(a, b);
explain select * from mulcolpk where a = 0 and b = 0;

reset plan_cache_mode;
reset enable_bitmapscan;
reset enable_seqscan;

drop schema mulcolpk cascade;
