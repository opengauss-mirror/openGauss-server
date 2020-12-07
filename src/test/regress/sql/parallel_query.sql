set enable_material to off;
create table parallel_t1(a int);
insert into parallel_t1 values(generate_series(1,100000));
analyze parallel_t1;
--normal plan for seq scan
explain (costs off) select count(*) from parallel_t1;
explain (costs off) select count(*) from parallel_t1 where a = 5000;
explain (costs off) select count(*) from parallel_t1 where a > 5000;
explain (costs off) select count(*) from parallel_t1 where a < 5000;
explain (costs off) select count(*) from parallel_t1 where a <> 5000;
select count(*) from parallel_t1;
select count(*) from parallel_t1 where a = 5000;
select count(*) from parallel_t1 where a > 5000;
select count(*) from parallel_t1 where a < 5000;
select count(*) from parallel_t1 where a <> 5000;

--set parallel parameter
set force_parallel_mode=on;
set parallel_setup_cost=0;
set parallel_tuple_cost=0.000005;
set max_parallel_workers_per_gather=2;
set min_parallel_table_scan_size=0;
set parallel_leader_participation=on;

--parallel plan for seq scan
explain (costs off) select count(*) from parallel_t1;
explain (costs off) select count(*) from parallel_t1 where a = 5000;
explain (costs off) select count(*) from parallel_t1 where a > 5000;
explain (costs off) select count(*) from parallel_t1 where a < 5000;
explain (costs off) select count(*) from parallel_t1 where a <> 5000;
select count(*) from parallel_t1;
select count(*) from parallel_t1 where a = 5000;
select count(*) from parallel_t1 where a > 5000;
select count(*) from parallel_t1 where a < 5000;
select count(*) from parallel_t1 where a <> 5000;
explain (costs off,analyse on,verbose on) select count(*) from parallel_t1;

--clean up
reset force_parallel_mode;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset max_parallel_workers_per_gather;
reset min_parallel_table_scan_size;
reset parallel_leader_participation;

--test parallel index scan
create table parallel_t2(a int, b int);
insert into parallel_t2 values(generate_series(1,100000), generate_series(1,100000));
create index t2_idx on parallel_t2 using btree(a);
analyze parallel_t2;

--normal plan for merge join
set enable_hashjoin to off;
set enable_nestloop to off;
set enable_indexscan to off;
explain (costs off) select count(*) from parallel_t1,parallel_t2 where parallel_t1.a=parallel_t2.a;
select count(*) from parallel_t1,parallel_t2 where parallel_t1.a=parallel_t2.a;
reset enable_hashjoin;
reset enable_nestloop;
reset enable_indexscan;

--set index scan parameter
set enable_seqscan to off;
set enable_bitmapscan to off;

--normal plan for index scan
explain (costs off) select count(b) from parallel_t2 where a > 5000;
explain (costs off) select count(b) from parallel_t2 where a < 5000;
select count(b) from parallel_t2 where a > 5000;
select count(b) from parallel_t2 where a < 5000;

--normal plan for index only scan
explain (costs off) select count(*) from parallel_t2;
explain (costs off) select count(*) from parallel_t2 where a > 5000;
explain (costs off) select count(*) from parallel_t2 where a < 5000;
select count(*) from parallel_t2;
select count(*) from parallel_t2 where a > 5000;
select count(*) from parallel_t2 where a < 5000;

--set parallel parameter
set force_parallel_mode=on;
set parallel_setup_cost=0;
set parallel_tuple_cost=0.000005;
set max_parallel_workers_per_gather=2;
set min_parallel_table_scan_size=0;
set min_parallel_index_scan_size=0;
set parallel_leader_participation=on;

--parallel plan for index scan
explain (costs off) select count(b) from parallel_t2 where a > 5000;
explain (costs off) select count(b) from parallel_t2 where a < 5000;
select count(b) from parallel_t2 where a > 5000;
select count(b) from parallel_t2 where a < 5000;

--parallel plan for index only scan
explain (costs off) select count(a) from parallel_t2;
explain (costs off) select count(a) from parallel_t2 where a > 5000;
explain (costs off) select count(a) from parallel_t2 where a < 5000;
select count(a) from parallel_t2;
select count(a) from parallel_t2 where a > 5000;
select count(a) from parallel_t2 where a < 5000;

--set parallel_workers for parallel_t2
alter table parallel_t2 set (parallel_workers = 1);

--parallel plan for index scan
explain (costs off) select count(b) from parallel_t2 where a > 5000;
explain (costs off) select count(b) from parallel_t2 where a < 5000;
select count(b) from parallel_t2 where a > 5000;
select count(b) from parallel_t2 where a < 5000;

--parallel plan for merge join
reset enable_seqscan;
reset enable_bitmapscan;
set enable_hashjoin to off;
set enable_nestloop to off;
set enable_indexscan to off;
explain (costs off) select count(*) from parallel_t1,parallel_t2 where parallel_t1.a=parallel_t2.a;
select count(*) from parallel_t1,parallel_t2 where parallel_t1.a=parallel_t2.a;
reset enable_hashjoin;
reset enable_nestloop;
reset enable_indexscan;

--clean up
drop table parallel_t1;
drop table parallel_t2;
reset enable_seqscan;
reset enable_bitmapscan;
reset force_parallel_mode;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset max_parallel_workers_per_gather;
reset min_parallel_table_scan_size;
reset parallel_leader_participation;
reset min_parallel_index_scan_size;

-- nestloop
set enable_hashjoin=off;
set enable_mergejoin=off;
explain (costs off, analyse on) select schemaname, tablename from pg_tables where tablename like 'sql%' order by tablename;
--set parallel parameter
set force_parallel_mode=on;
set parallel_setup_cost=0;
set parallel_tuple_cost=0.000005;
set max_parallel_workers_per_gather=2;
set min_parallel_table_scan_size=0;
set parallel_leader_participation=on;
-- nestloop
explain (costs off, analyse on) select schemaname, tablename from pg_tables where tablename like 'sql%' order by tablename;
--clean up
reset force_parallel_mode;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset max_parallel_workers_per_gather;
reset min_parallel_table_scan_size;
reset parallel_leader_participation;
reset enable_hashjoin;
reset enable_mergejoin;