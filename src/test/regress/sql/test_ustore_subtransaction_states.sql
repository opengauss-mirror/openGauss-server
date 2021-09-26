
\set AUTOCOMMIT off

-- Test1: commit sub transaction
begin;
create table table_subtran (c1 int) with (storage_type=ustore);
insert into table_subtran values (1);
commit;

begin;
savepoint s1;
update table_subtran set c1 = 2;
rollback to s1;
select * from table_subtran;

savepoint s2;
insert into table_subtran values (2);
select * from table_subtran;

savepoint s3;
delete from table_subtran;
rollback to s2;
select * from table_subtran;
savepoint s4;
update table_subtran set c1 = 2;
commit;

begin;
select * from table_subtran;
drop table table_subtran;
commit;


-- Test2: Rollback sub transaction
begin;
create table table_subtran (c1 int) with (storage_type=ustore);
insert into table_subtran values (1);
commit;

begin;
savepoint s1;
update table_subtran set c1 = 2;
rollback to s1;
select * from table_subtran;

savepoint s2;
insert into table_subtran values (2);
select * from table_subtran;

savepoint s3;
delete from table_subtran;
rollback to s2;
select * from table_subtran;
savepoint s4;
update table_subtran set c1 = 2;
rollback;

begin;
select * from table_subtran;
drop table table_subtran;
commit;

-- Test3: Rollback sub transaction in progress in by raising an error.
begin;
create table table_subtran (c1 int) with (storage_type=ustore);
insert into table_subtran values (1);
commit;

begin;
insert into table_subtran values (1);
insert into table_subtran values (2);
savepoint s1;
insert into table_subtran values (3);
RAISE EXCEPTION '';
rollback;

begin;
select * from table_subtran;
drop table table_subtran;
commit;

