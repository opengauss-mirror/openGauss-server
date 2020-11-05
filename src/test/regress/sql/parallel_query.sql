create table parallel_t1(a int);
insert into parallel_t1 values(generate_series(1,100000));
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

--clean up
drop table parallel_t1;
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

--set index scan parameter
set enable_seqscan to off;
set enable_bitmapscan to off;

--normal plan for index scan
explain (costs off) select count(b) from parallel_t2 where a > 5000;
explain (costs off) select count(b) from parallel_t2 where a < 5000;
select count(b) from parallel_t2 where a > 5000;
select count(b) from parallel_t2 where a < 5000;

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

--set parallel_workers for parallel_t2
alter table parallel_t2 set (parallel_workers = 1);

--parallel plan for index scan
explain (costs off) select count(b) from parallel_t2 where a > 5000;
explain (costs off) select count(b) from parallel_t2 where a < 5000;
select count(b) from parallel_t2 where a > 5000;
select count(b) from parallel_t2 where a < 5000;

--clean up
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