CREATE SCHEMA test_ustore_index;
SET current_schema = test_ustore_index;
set ustore_attr='ustore_verify_level=slow;ustore_verify_module=all';
set enable_seqscan to false;
set enable_indexonlyscan to true;
set enable_indexscan to true;
set enable_bitmapscan to false;

-- Test create index
drop table if exists t1;
create table t1(c1 int, c2 int, c3 char(8)) with (storage_type=USTORE);
create index t1i1 on t1(c1, c3);
insert into t1 values(1, 2, '12345678');
insert into t1 values(1, 3, '123');
insert into t1 values(2, 1, '1234');
drop table if exists t2;
create table t2(c1 int, c2 int) with (storage_type=USTORE);
create unique index t2i1 on t2(c1);
insert into t2 values(3, 4);
insert into t2 values(5, 2);
insert into t2 values(1, 6);

-- Test IndexOnlyScan
explain select c1, c3 from t1 where c1 = 2;
select c1, c3 from t1 where c1 = 2;
explain select c1 from t2 order by c1;
select c1 from t2 order by c1;

-- Test IndexScan
explain select * from t1 order by c1;
select * from t1 order by c1;
explain select c2 from t2 order by c1;
select c2 from t2 order by c1;

-- Test Insert
create table t3(c1 int, c2 int, c3 int) with (storage_type=USTORE);
insert into t3 values(1, 10, 100);
insert into t3 values(2, 10, 100);
create unique index t3i1 on t3(c1, c3);

insert into t3 values(3, 10, 100);

-- Test duplicate key
insert into t3 values(1, 10, 100);
insert into t3 values(3, 10, 100);

drop index t3i1;
drop table t3;

-- Test Update
explain update t1 set c1 =4 where c3='12345678';
update t1 set c1 =4 where c3='12345678';
select * from t1 order by c1;

explain update t2 set c1=4 where c1 = 1;
update t2 set c1=4 where c1 = 1;
select * from t2 order by c1;

-- Test multiple inserts and updates
create table t3 (c1 int, c2 int, c3 int) with (storage_type=USTORE);
create unique index t3i1 on t3(c1,c3);
insert into t3 values(1,10,100);
insert into t3 values(2,10,200);
insert into t3 values(3,10,300);

-- updated through opfusion layer, single row affected
explain update t3 set c1=4 where c3=100;
update t3 set c1=4 where c3=100;
update t3 set c1=5 where c3=300;
update t3 set c1=6 where c3=200;
update t3 set c2=20 where c3=200;
update t3 set c2=20 where c3=300;

insert into t3 values(1,10,100);
insert into t3 values(2,10,100);
insert into t3 values(3,10,100);

-- violate uniqueness constraint
update t3 set c1=1 where c3=100;

-- updated through executor layer
update t3 set c1=1 where c2=20;

-- update a row affected by the executor layer update
update t3 set c2 =30 where c3=300;
select * from t3 order by c1;

drop index t1i1;
drop table t1;
drop index t2i1;
drop table t2;
drop index t3i1;
drop table t3;

-- test multi-version index --
set enable_opfusion to false;
create table t(a int, b double precision) with (storage_type=USTORE);
create index ti1 on t(a);
create index ti2 on t(b);

-- insert data --
insert into t values(generate_series(1, 1000), generate_series(1, 1000));

-- record initial size --
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- inplace update a: t and ti2 should not grow bigger --
explain (costs off) update t set a = a + 1;
update t set a = a + 1;
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- inplace update b: t and ti1 should not grow bigger --
explain (costs off) update t set b = b + 1.0;
update t set b = b + 1.0;
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- test delete --
explain (costs off) delete from t where a > 50;
delete from t where a > 50;

-- test index only scan: shouldn't see deleted tuple nor fetch uheap --
explain (costs off) select count(*) from t where a > 0 and a < 100;
select count(*) from t where a > 0 and a < 100;
explain (costs off) select count(*) from t where b = 1.0;
select count(*) from t where b = 1.0;

drop index ti1;
drop index ti2;
drop table t;

-- test multi-version index with opfusion --
set enable_opfusion to true;
create table t(a int, b double precision) with (storage_type=USTORE);
create index ti1 on t(a);
create index ti2 on t(b);

-- insert data --
insert into t values(generate_series(1, 1000), generate_series(1, 1000));

-- record initial size --
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- inplace update a: t and ti2 should not grow bigger --
explain (costs off) update t set a = a + 1 where a >= 1 and a <= 1000;
update t set a = a + 1 where a >= 1 and a <= 1000;
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- inplace update b: t and ti1 should not grow bigger --
explain (costs off) update t set b = b + 1.0 where b <= 1000;
update t set b = b + 1.0 where b <= 1000;
select pg_size_pretty(pg_relation_size('t')), pg_size_pretty(pg_relation_size('ti1')), pg_size_pretty(pg_relation_size('ti2'));

-- test delete --
explain (costs off) delete from t where a > 50;
delete from t where a > 50;

-- test index only scan: shouldn't see deleted tuple nor fetch uheap --
explain (costs off) select count(*) from t where a > 0 and a < 100;
select count(*) from t where a > 0 and a < 100;
explain (costs off) select a from t where a >= 48 and a <= 52;
select a from t where a >= 48 and a <= 52;
explain (costs off) select b from t where b = 1.0;
select b from t where b = 1.0;

drop index ti1;
drop index ti2;
drop table t;

-- test build index --

-- test cid check support --
create table t(a int, b int) with(storage_type=ustore);
create index on t(a);
insert into t values(generate_series(1, 10), 2);

set enable_bitmapscan = off;
set enable_seqscan = off;
set enable_indexscan = off;
set enable_indexonlyscan = off;

begin;
update t set a = 2;
set enable_bitmapscan = on;
explain select a from t where a < 10;
select a from t where a < 10;
set enable_bitmapscan = off;

set enable_indexscan = on;
explain select a from t where a < 10;
select a from t where a < 10;
set enable_indexscan = off;

set enable_indexonlyscan = on;
explain (costs off) select a from t where a < 10;
select a from t where a < 10;
set enable_indexonlyscan = off;
rollback;

-- test cursor support --
set enable_indexscan = on;
begin;
declare c1 cursor for select a from t where a < 10;
update t set a = 2;
declare c2 cursor for select a from t where a < 10;
update t set a = 3;
declare c3 cursor for select a from t where a < 10;
fetch next from c1;
fetch next from c1;
fetch next from c2;
fetch next from c3;
rollback;

set enable_indexscan = off;
set enable_indexonlyscan = on;
begin;
declare c1 cursor for select a from t where a < 10;
update t set a = 2;
fetch next from c1;
fetch next from c1;
fetch next from c1;
rollback;

drop table t;

-- test unique index first for ustore table --

create table t1(c1 int not null, c2 int not null, c3 text, constraint ustore_index primary key (c1)) with (storage_type=USTORE);

insert into t1 values(generate_series(1, 10), generate_series(1, 10));

analyze t1;

set enable_seqscan = on;
set enable_indexscan = on;

set sql_beta_feature = no_unique_index_first;
show sql_beta_feature;

explain SELECT c1 FROM t1 WHERE c1 = 50;
explain SELECT /*+ tablescan(t1) */c1 FROM t1 WHERE c1 = 50;
explain SELECT /*+ indexonlyscan(t1) */c1 FROM t1 WHERE c1 = 50;

reset sql_beta_feature;
show sql_beta_feature;

explain SELECT c1 FROM t1 WHERE c1 = 50;
explain SELECT /*+ tablescan(t1) */c1 FROM t1 WHERE c1 = 50;
explain SELECT /*+ indexonlyscan(t1) */c1 FROM t1 WHERE c1 = 50;

drop table t1;

-- test ustore index trace
create table t(a int, b int) with(storage_type=USTORE);
create index on t(a, b);
insert into t values(generate_series(1, 10), generate_series(1, 10));
delete from t where a % 2 = 0;

set ustore_attr="index_trace_level=all;enable_log_tuple=on";

select /*+ indexscan(t) */ * from t where a = 1;

set ustore_attr="index_trace_level=no;enable_log_tuple=off";
drop table t;

-- test ustore querying with bitmapindexscan when updated in the same transaction
set enable_indexscan to off;
set enable_indexonlyscan to off;
set enable_seqscan to off;
set enable_bitmapscan to on;

drop table if exists test;
create table test(a int);
create index test_idx on test(a);
insert into test values(2);
insert into test values(2);
insert into test values(1);
insert into test values(1);

begin;
declare c1 cursor for select a from test where a = 2;
update test set a = 2;
fetch next from c1;
fetch next from c1;
fetch next from c1;
fetch next from c1;
rollback;

drop table if exists test;
reset enable_indexscan;
reset enable_indexonlyscan;
reset enable_seqscan;
reset enable_bitmapscan;
-- end
DROP SCHEMA test_ustore_index cascade;