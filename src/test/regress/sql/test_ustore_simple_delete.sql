-- test delete
drop table if exists t1;
create table t1 (c1 int) with (storage_type=USTORE);
start transaction;
insert into t1(c1) values(1);
insert into t1(c1) values(2);
commit;
select * from t1;

start transaction;
delete from t1 where c1 = 1;
commit;
select * from t1;

drop table if exists t2;
create table t2 (c1 int, c2 char(10), c3 decimal, c4 text, c5 varchar(10)) with (storage_type=USTORE);
start transaction;
insert into t2 (c1, c2, c3, c4, c5) values (1, 'bbb', 1.1, 'ddd', 'eee');
insert into t2 (c1, c2, c3, c4, c5) values (2, 'ccc', 2.2, 'eee', 'fff');
commit;
select * from t2;

start transaction;
delete from t2 where c4 = 'ddd';
commit;
select * from t2;

drop table t1;
drop table t2;
