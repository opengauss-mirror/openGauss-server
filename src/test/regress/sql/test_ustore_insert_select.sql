-- Test for Insert-Select functionality. It tests the following scenarios:
   --  ustore->ustore
   --  heap->ustore
   --  ustore->heap
   --  ustore->column store
   --  column store->ustore

-- test insert-select from ustore to ustore table
drop table if exists t1;
drop table if exists t2;
create table t1 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
create table t2 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
insert into t1 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t1 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t1 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t1 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t1 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t1;

insert into t2 select * from t1;
select * from t2;
-- Verify there is no difference between the two tables
select * from t1 except select * from t2;

-- test insert-select from heap to ustore table
drop table if exists t3;
drop table if exists t4;
create table t3 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL);
create table t4 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
insert into t3 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t3 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t3 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t3 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t3 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t3;

insert into t4 select * from t3;
select * from t4;
-- Verify there is no difference between the two tables
select * from t3 except select * from t4;

-- test insert-select from ustore to heap table
drop table if exists t5;
drop table if exists t6;
create table t5 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
create table t6 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL);
insert into t5 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t5 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t5 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t5 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t5 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t5;

insert into t6 select * from t5;
select * from t6;
-- Verify there is no difference between the two tables
select * from t5 except select * from t6;

-- test insert-select from ustore to column table
drop table if exists t7;
drop table if exists t8;
create table t7 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
create table t8 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (orientation=column);
insert into t7 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t7 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t7 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t7 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t7 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t7;

insert into t8 select * from t7;
select * from t8;
-- Verify there is no difference between the two tables
select * from t7 except select * from t8;

-- test insert-select from column to ustore table
drop table if exists t9;
drop table if exists t10;
create table t9 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (orientation=column);
create table t10 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
insert into t9 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t9 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t9 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t9 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t9 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t9;

insert into t10 select * from t9;
select * from t9;
-- Verify there is no difference between the two tables
select * from t9 except select * from t10;

-- Clean up
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;
drop table t7;
drop table t8;
drop table t9;
drop table t10;
