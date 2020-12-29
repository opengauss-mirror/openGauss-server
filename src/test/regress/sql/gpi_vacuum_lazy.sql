--drop table and index
drop index if exists global_index_test_b;
drop index if exists local_index_test_c;
drop index if exists global_index_test_d;
drop table if exists test_vacuum_lazy;

--prepare table and index
create table test_vacuum_lazy(a int, b int, c int, d int) partition by range(a) (partition p1 values less than (10000), partition p2 values  less than (20000), partition p3 values less than (30001));
create unique index global_index_test_a on test_vacuum_lazy (a) global;
create index global_index_test_b on test_vacuum_lazy (b) global;
create index local_index_test_c on test_vacuum_lazy (c) local (partition c_index1, partition c_index2, partition c_index3);
create index global_index_test_d on test_vacuum_lazy(d) global;

--one thread
insert into test_vacuum_lazy select r,r,r,10000 from generate_series(0,9999) as r;
insert into test_vacuum_lazy select r,r,r,20000 from generate_series(10000,19999) as r;
insert into test_vacuum_lazy select r,r,r,30000 from generate_series(20000,29999) as r;
select count(*) from test_vacuum_lazy;
vacuum analyze test_vacuum_lazy;
explain (costs off) select count(*) from test_vacuum_lazy where d = 20000;
select count(*) from test_vacuum_lazy where d = 20000;

set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select count(*) from test_vacuum_lazy where c < 20000;
select count(*) from test_vacuum_lazy where c < 20000;
set enable_bitmapscan=on;
set enable_seqscan=on;

delete from test_vacuum_lazy where d = 10000;

explain (costs off) select count(*) from test_vacuum_lazy where d = 20000;
select count(*) from test_vacuum_lazy where d = 20000;

set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select count(*) from test_vacuum_lazy where c < 20000;
select count(*) from test_vacuum_lazy where c < 20000;
set enable_bitmapscan=on;
set enable_seqscan=on;

start transaction;
alter table test_vacuum_lazy add partition p6 values less than (40001);
insert into test_vacuum_lazy select r,r,r,100 from generate_series(30000,39999) as r;
abort;
vacuum analyze test_vacuum_lazy;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select count(*) from test_vacuum_lazy where d = 20000;
select count(*) from test_vacuum_lazy where d = 20000;
explain (costs off) select count(*) from test_vacuum_lazy where c < 20000;
select count(*) from test_vacuum_lazy where c < 20000;
set enable_bitmapscan=on;
set enable_seqscan=on;

select count(*) from test_vacuum_lazy;
analyze test_vacuum_lazy;
delete from test_vacuum_lazy where d = 10000;
vacuum test_vacuum_lazy;
delete from test_vacuum_lazy where d = 30000;
analyze test_vacuum_lazy;
select count(*) from test_vacuum_lazy;

--multi thread
\parallel on
insert into test_vacuum_lazy select r,r,r,5000 from generate_series(0,4999) as r;
vacuum analyze test_vacuum_lazy;
insert into test_vacuum_lazy select r,r,r,10000 from generate_series(5000,9999) as r;
vacuum test_vacuum_lazy;
insert into test_vacuum_lazy select r,r,r,15000 from generate_series(10000,14999) as r;
analyze test_vacuum_lazy;
insert into test_vacuum_lazy select r,r,r,20000 from generate_series(15000,19999) as r;
vacuum analyze test_vacuum_lazy;
insert into test_vacuum_lazy select r,r,r,25000 from generate_series(20000,24999) as r;
vacuum test_vacuum_lazy;
insert into test_vacuum_lazy select r,r,r,30000 from generate_series(25000,29999) as r;
analyze test_vacuum_lazy;
\parallel off
select count(*) from test_vacuum_lazy;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select count(*) from test_vacuum_lazy where d = 20000;
select count(*) from test_vacuum_lazy where d = 20000;
set enable_bitmapscan=on;
set enable_seqscan=on;

\parallel on
delete from test_vacuum_lazy where d = 5000;
vacuum analyze test_vacuum_lazy;
delete from test_vacuum_lazy where d = 10000;
vacuum test_vacuum_lazy;
delete from test_vacuum_lazy where d = 15000;
analyze test_vacuum_lazy;
delete from test_vacuum_lazy where d = 20000;
vacuum analyze test_vacuum_lazy;
delete from test_vacuum_lazy where d = 25000;
vacuum test_vacuum_lazy;
delete from test_vacuum_lazy where d = 30000;
analyze test_vacuum_lazy;
\parallel off
select * from test_vacuum_lazy;

\parallel on
insert into test_vacuum_lazy values (0, 0, 0, 999);
insert into test_vacuum_lazy select r,r,r,5000 from generate_series(1,4998) as r;
insert into test_vacuum_lazy values (4999, 4999, 4999, 999);
vacuum analyze test_vacuum_lazy;
insert into test_vacuum_lazy values (5000, 5000, 5000, 999);
insert into test_vacuum_lazy select r,r,r,10000 from generate_series(5001,9998) as r;
insert into test_vacuum_lazy values (9999, 9999, 9999, 999);
vacuum test_vacuum_lazy;
insert into test_vacuum_lazy values (10000, 10000, 10000, 999);
insert into test_vacuum_lazy select r,r,r,15000 from generate_series(10001,14998) as r;
insert into test_vacuum_lazy values (14999, 14999, 14999, 999);
analyze test_vacuum_lazy;
insert into test_vacuum_lazy values (15000, 15000, 15000, 999);
insert into test_vacuum_lazy select r,r,r,20000 from generate_series(15001,19998) as r;
insert into test_vacuum_lazy values (19999, 19999, 19999, 999);
vacuum analyze test_vacuum_lazy;
insert into test_vacuum_lazy values (20000, 20000, 20000, 999);
insert into test_vacuum_lazy select r,r,r,25000 from generate_series(20001,24998) as r;
insert into test_vacuum_lazy values (24999, 24999, 24999, 999);
vacuum test_vacuum_lazy;
insert into test_vacuum_lazy values (25000, 25000, 25000, 999);
insert into test_vacuum_lazy select r,r,r,30000 from generate_series(25001,29998) as r;
insert into test_vacuum_lazy values (29999, 29999, 29999, 999);
analyze test_vacuum_lazy;
\parallel off
select count(*) from test_vacuum_lazy;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select count(*) from test_vacuum_lazy where b > 10 and b < 20000;
select count(*) from test_vacuum_lazy where b > 10 and b < 20000;
explain (costs off) select count(*) from test_vacuum_lazy where d = 999 order by 1;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
set enable_bitmapscan=on;
set enable_seqscan=on;

\parallel on
select count(*) from test_vacuum_lazy where d = 999 order by 1;
vacuum analyze test_vacuum_lazy;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
vacuum test_vacuum_lazy;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
analyze test_vacuum_lazy;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
vacuum analyze test_vacuum_lazy;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
vacuum test_vacuum_lazy;
select count(*) from test_vacuum_lazy where d = 999 order by 1;
analyze test_vacuum_lazy;
\parallel off

alter table test_vacuum_lazy disable row movement;
\parallel on
delete from test_vacuum_lazy where b % 7 = 0 and d <> 999;
analyze test_vacuum_lazy;
delete from test_vacuum_lazy where b % 5 = 0 and d <> 999;
vacuum analyze test_vacuum_lazy;
delete from test_vacuum_lazy where b % 3 = 0 and d <> 999;
vacuum test_vacuum_lazy;
\parallel off

set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select * from test_vacuum_lazy where d = 999 order by 1;
select * from test_vacuum_lazy where d = 999 order by 1;
set enable_bitmapscan=on;
set enable_seqscan=on;

\parallel on
delete from test_vacuum_lazy where b % 19 = 0 and d <> 999;
analyze test_vacuum_lazy;
delete from test_vacuum_lazy where b % 17 = 0 and d <> 999;
vacuum analyze test_vacuum_lazy;
delete from test_vacuum_lazy where b % 13 = 0 and d <> 999;
vacuum test_vacuum_lazy;
\parallel off
alter table test_vacuum_lazy enable row movement;
set enable_bitmapscan=off;
set enable_seqscan=off;
explain (costs off) select * from test_vacuum_lazy where d = 999 order by 1;
select * from test_vacuum_lazy where d = 999 order by 1;
set enable_bitmapscan=on;
set enable_seqscan=on;
set enable_indexscan=off;
explain (costs off) select * from test_vacuum_lazy where d = 999 order by 1;
select * from test_vacuum_lazy where d = 999 order by 1;
set enable_indexscan=on;
set enable_bitmapscan=off;
set enable_indexscan=off;
explain (costs off) select * from test_vacuum_lazy where d = 999 order by 1;
select * from test_vacuum_lazy where d = 999 order by 1;
set enable_indexscan=on;
set enable_bitmapscan=on;

--clean table and index
drop index if exists global_index_test_b;
drop index if exists local_index_test_c;
drop index if exists global_index_test_d;
drop table if exists test_vacuum_lazy;

