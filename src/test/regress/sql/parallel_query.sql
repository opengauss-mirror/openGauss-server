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

--normal plan for bitmapscan
create index idx_parallel_t1 on parallel_t1(a);
analyze parallel_t1;
set enable_seqscan to off;
set enable_indexscan to off;
explain (costs off) select count(*) from parallel_t1 where a > 5000;
explain (costs off) select count(*) from parallel_t1 where a < 5000;
explain (costs off) select count(*) from parallel_t1 where a <> 5000;
select count(*) from parallel_t1 where a > 5000;
select count(*) from parallel_t1 where a < 5000;
select count(*) from parallel_t1 where a <> 5000;
reset enable_seqscan;
reset enable_indexscan;

--set parallel parameter
set force_parallel_mode=on;
set parallel_setup_cost=0;
set parallel_tuple_cost=0.000005;
set max_parallel_workers_per_gather=2;
set min_parallel_table_scan_size=0;
set min_parallel_index_scan_size=0;
set parallel_leader_participation=on;

--rescan case
create table test_with_rescan(dm int, sj_dm int, name text);
insert into test_with_rescan values(1,0,'universe');
insert into test_with_rescan values(2,1,'galaxy');
insert into test_with_rescan values(3,2,'sun');
insert into test_with_rescan values(4,3,'earth');
insert into test_with_rescan values(5,4,'asia');
insert into test_with_rescan values(6,5,'China');
insert into test_with_rescan values(7,6,'shaanxi');
insert into test_with_rescan values(8,7,'xian');
insert into test_with_rescan values(9,8,'huawei');
insert into test_with_rescan values(10,9,'v10');
insert into test_with_rescan values(11,10,'v10-3L');
insert into test_with_rescan values(12,11,'gauss');
insert into test_with_rescan values(13,12,'test');
insert into test_with_rescan values(14,13,'test');
insert into test_with_rescan values(15,14,'test');
insert into test_with_rescan values(16,15,'test');
insert into test_with_rescan values(17,16,'test');
insert into test_with_rescan values(18,17,'test');
insert into test_with_rescan values(19,18,'test');
insert into test_with_rescan values(20,19,'test');
create index on test_with_rescan(dm);
create index on test_with_rescan(sj_dm);
create index on test_with_rescan(name);
analyze test_with_rescan;
explain (costs off)
WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1 
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t;

WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t order by 1,2,3,4;

--increate cpu_tuple_cost, disable seqscan, test parallel index scan
set enable_seqscan=off;
set cpu_tuple_cost=1000;
explain (costs off)
WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1 
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t;

WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t order by 1,2,3,4;

--disable indexscan, test parallel bitmap scan
set enable_indexscan to off;
explain (costs off)
WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1 
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t;

WITH recursive t_result AS (
select * from(
SELECT dm,sj_dm,name,1 as level
FROM test_with_rescan
WHERE sj_dm < 10 order by dm limit 6 offset 2)
UNION all
SELECT t2.dm,t2.sj_dm,t2.name||' > '||t1.name,t1.level+1
FROM t_result t1
JOIN test_with_rescan t2 ON t2.sj_dm = t1.dm
)
SELECT *
FROM t_result t order by 1,2,3,4;

drop table test_with_rescan;
reset enable_seqscan;
reset enable_indexscan;
reset cpu_tuple_cost;

--parallel plan for seq scan
set enable_bitmapscan to off;
set enable_indexscan to off;
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
reset enable_indexscan;
reset enable_bitmapscan;

--parallel plan for bitmap scan
--onepage case
CREATE TABLE onepage1 (val int, val2 int);
ALTER TABLE onepage1 ADD PRIMARY KEY(val, val2);
insert into onepage1 (select * from generate_series(1, 5) a, generate_series(1, 5) b);
CREATE TABLE onepage2 as select * from onepage1;
explain select * from onepage2 natural join onepage1 where onepage2.val > 2 and onepage1.val < 4;
select * from onepage2 natural join onepage1 where onepage2.val > 2 and onepage1.val < 4 order by 1,2;
drop table onepage1;
drop table onepage2;
set enable_seqscan to off;
set enable_indexscan to off;
explain (costs off) select count(*) from parallel_t1 where a > 5000;
explain (costs off) select count(*) from parallel_t1 where a < 5000;
explain (costs off) select count(*) from parallel_t1 where a <> 5000;
select count(*) from parallel_t1 where a > 5000;
select count(*) from parallel_t1 where a < 5000;
select count(*) from parallel_t1 where a <> 5000;
reset enable_seqscan;
reset enable_indexscan;

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