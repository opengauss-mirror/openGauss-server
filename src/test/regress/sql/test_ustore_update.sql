-- test update
CREATE SCHEMA test_ustore_update;
SET current_schema = test_ustore_update;
drop table if exists t1;
create table t1(c1 integer, c2 integer default 200) with (storage_type=USTORE);
create index idx1 on t1(c1);
start transaction;
insert into t1 values(10, 20);
insert into t1 values(20, 30);
insert into t1 values(30, 40);
insert into t1 values(40, 50);
insert into t1 values(50, 60);
insert into t1 values(60, 70);
insert into t1 values(70, 80);
insert into t1 values(80, 90);
update t1 set c2 = 100 where c1 > 60;
commit;
select * from t1;
select /*+ indexonlyscan(t1) */ sum(c1), count(*) from t1 union select /*+ tablescan(t1) */ sum(c1),count(*) from t1;

start transaction;
update t1 set c2 = 100 where c1 > 60;
commit;

select * from t1 order by c1;
select /*+ indexonlyscan(t1) */ sum(c1), count(*) from t1 union select /*+ tablescan(t1) */ sum(c1),count(*) from t1;

-- Rollback of inplace update
drop table if exists t3;
create table t3(c1 integer, c2 integer) with (storage_type=USTORE);
start transaction;
insert into t3 values(1, 2);
insert into t3 values(3, 4);
insert into t3 values(5, 6);
insert into t3 values(7, 8);
insert into t3 values(9, 10);
commit;
create index idx3 on t3(c1);

select * from t3 order by c1;
select /*+ indexonlyscan(t3) */ sum(c1), count(*) from t3 union select /*+ tablescan(t3) */ sum(c1),count(*) from t3;

start transaction;
update t3 set c2 = 20 where c1 = 5;
update t3 set c2 = 30 where c1 = 7;
select * from t3 order by c1;
rollback;

select * from t3 order by c1;
select /*+ indexonlyscan(t3) */ sum(c1), count(*) from t3 union select /*+ tablescan(t3) */ sum(c1),count(*) from t3;

-- Rollback of non-inplace update
-- Note that the first update statement causes 
-- page to be pruned and re-arranged so the update is done inplace.
-- The second update is done as a pure non-inplace update because 
-- we do not prune a data page with open transaction on a tuple. 
drop table if exists t4;
create table t4(c1 integer primary key, c2 varchar(128)) with (storage_type=USTORE);
start transaction;
insert into t4 values(1, 'abc');
insert into t4 values(2, 'bcd');
insert into t4 values(3, 'cde');
insert into t4 values(generate_series(4, 500), 'defdewjhdlsahdlsa');
update t4 set c2 = 'aaaabbbb' where c1 < 10;
update t4 set c2 = 'aaaabbbbhjhjjhjhjhjhjhjhjhj' where c1 < 10;
commit;
create index idx4 on t4(c1);

select * from t4 order by c1 limit 10;
select /*+ indexonlyscan(t4) */ sum(c1), count(*) from t4 union select /*+ tablescan(t4) */ sum(c1),count(*) from t4;

start transaction;
update t4 set c2 = 'aaaabbbbccccdddd' where c1 = 3;
update t4 set c2 = 'aa';
update t4 set c2 = 'aaaaa';
update t4 set c2 = 'aaaaaaaaaaaaaaaahdkjdsd';
select /*+ indexonlyscan(t4) */ sum(c1), count(*) from t4 union select /*+ tablescan(t4) */ sum(c1),count(*) from t4;
rollback;

select * from t4 order by c1 limit 10;
select /*+ indexonlyscan(t4) */ sum(c1), count(*) from t4 union select /*+ tablescan(t4) */ sum(c1),count(*) from t4;

start transaction;
update t4 set c2 = 'aa';
update t4 set c2 = 'aaaaa';
update t4 set c2 = 'aaaaaaaaaaaaaaaahdkjdsd';
commit;

select * from t4 order by c1 limit 10;
select /*+ indexonlyscan(t4) */ sum(c1), count(*) from t4 union select /*+ tablescan(t4) */ sum(c1),count(*) from t4;

-- Test updates involving mixed table types
drop table if exists t5;
drop table if exists t6;
create table t5(a integer, b integer) with (orientation = column);
create table t6(a integer, b integer) with (storage_type=USTORE);
insert into t5 values(generate_series(1,10,1) , generate_series(1,10,1));
insert into t6 values(generate_series(1,10,1) , generate_series(1,10,1));
update t5 set b = 999 from t6 where t5.a=t6.a;
select count(*) from t5 where b = 999;

drop table t1;
drop table t3;
drop table t4;
drop table t5;
drop table t6;
-- end
DROP SCHEMA test_ustore_update cascade;