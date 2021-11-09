drop table if exists t0;
drop table if exists t1;
drop table if exists part_t1;

-- Row table t0 is used for cstore insertion.
create table t0 (a int, b int, c int, d int);
insert into t0 values (generate_series(1, 800, 1), generate_series(1, 800, 1), generate_series(1, 800, 1), generate_series(1, 800, 1));

create table t1 (a int, b int, c int, d int, primary key (a, b), unique (c)) with (orientation=column);
\d t1
drop table t1;

create table t1 (a int, b int, c int, d int) with (orientation=column);
alter table t1 add primary key (a, b);
alter table t1 add unique (c);
\d t1
alter table t1 drop constraint t1_pkey;
alter table t1 drop constraint t1_c_key;

-- Fail. Unique index on cstore table can only be cbtree.
create unique index on t1 (a, b, c);
-- Success
create unique index on t1 using cbtree (a, b, c);
drop table t1;

create table t1 (a int primary key, b int, c int, d int) with (orientation=column);
insert into t1 select * from t0 where a <= 100;
------------------- Fail. -----------------
insert into t1 values (1, 2, 3, 4);
update t1 set a = 10 where a = 1;
insert into t1 values (null, 1, 1, 1);

------------------- Success. -----------------------------
insert into t1 select * from t0 where a > 100 and a < 200;
-- (1, 1, 1, 1) -> (300, 1, 1, 1)
update t1 set a = 300 where a = 1;
insert into t1 values (1, 2, 3, 4);
alter table t1 drop constraint t1_pkey;
insert into t1 values (1, 3, 4, 5);
drop table t1;

-- Fail. Primary key must contain partition key.
CREATE TABLE part_t1(a int, b int, c int, d int, primary key (b, c)) with (orientation = column)
PARTITION BY RANGE (a)
(
    PARTITION P1 VALUES LESS THAN(200),
    PARTITION P2 VALUES LESS THAN(400),
    PARTITION P3 VALUES LESS THAN(600),
    PARTITION P4 VALUES LESS THAN(800)
);

CREATE TABLE part_t1(a int, b int, c int, d int, primary key (a, b)) with (orientation = column)
PARTITION BY RANGE (a)
(
    PARTITION P1 VALUES LESS THAN(200),
    PARTITION P2 VALUES LESS THAN(400),
    PARTITION P3 VALUES LESS THAN(600),
    PARTITION P4 VALUES LESS THAN(800)
);
-- Fail. Unqiue index on cstore table can only be cbtree.
create unique index on part_t1 (b, c);
-- Fail. Unique index on cstore_table can only be local index.
create unique index on part_t1 using cbtree (a, c);
create unique index on part_t1 using cbtree (a, c) local;
\d part_t1
drop table part_t1;

CREATE TABLE part_t1(a int, b int, c int, d int) with (orientation = column)
PARTITION BY RANGE (a)
(
    PARTITION P1 VALUES LESS THAN(200),
    PARTITION P2 VALUES LESS THAN(400),
    PARTITION P3 VALUES LESS THAN(600),
    PARTITION P4 VALUES LESS THAN(800)
);
insert into part_t1 select * from t0 where a < 400;
insert into part_t1 values (1, 1, 2, 3);
-- Fail. Duplicate tuples: (1, 1, 1, 1), (1, 1, 2, 3)
alter table part_t1 add primary key (a, b);

delete from part_t1 where a = 1 and b = 1 and c = 2 and d = 3;

insert into part_t1 values (1, null, 3, 4);
-- Fail. Primary key must be not null.
alter table part_t1 add primary key (a, b);

delete from part_t1 where b is null;

-- Success.
alter table part_t1 add primary key (a, b);

------------------------ Fail. --------------------------------
insert into part_t1 values (1, null, 3, 4);
update part_t1 set a = 1, b = 1 where a = 300 and b = 300;
insert into part_t1 select * from t0 where a > 100 and a < 300;
---------------------------------------------------------------

-- Success.
insert into part_t1 select * from t0 where a > 400 and a < 500;

drop table part_t1;

drop table t0;

-- CStore does not supoort DEFERABLE/INITIALLY DEFERRED on primary/unique constraint
-- Fail on column-level and table-level constraint
create table col_t1_defer (a int primary key DEFERRABLE INITIALLY DEFERRED, b int) with (orientation = column);
create table col_t1_defer (a int, b int, primary key (a) DEFERRABLE INITIALLY DEFERRED) with (orientation = column);

drop table if exists col_t1_defer;

-- CStore supports NOT DEFERABLE/INITIALLY IMMEDIATE on primary/unique constraint
-- Scuccess on column-level and table-level constraint
create table col_t1_not_defer (a int primary key NOT DEFERRABLE INITIALLY IMMEDIATE, b int) with (orientation = column);
create table col_t2_not_defer (a int, b int, primary key (a) NOT DEFERRABLE INITIALLY IMMEDIATE) with (orientation = column);

drop table col_t1_not_defer;
drop table col_t2_not_defer;

-- CStore supports add primary key/unique column
create table col_t1 (a int, b int) with (orientation = column);
alter table col_t1 add column c int unique;
\d col_t1
-- Duplicate key violation
insert into col_t1 values (1, 1, 1), (2, 2, 1);

drop table col_t1;

-- Test adding and merging cstore partition
create table col_part_t1 (a int primary key, b int) with (orientation = column)
partition by range (a)
(
partition p1 values less than (100),
partition p2 values less than (200),
partition p3 values less than (300)
);
alter table col_part_t1 add partition p4 values less than(400);
insert into col_part_t1 values (105, 105), (205, 205), (305, 305);
alter table col_part_t1 merge partitions p2, p3, p4 into partition p234;
insert into col_part_t1 values (106, 106), (206, 206), (306, 306);
-- Duplicate key violation
insert into col_part_t1 values (105, 105), (205, 205), (305, 305);

drop table col_part_t1;

create schema schema_column_pk_unique;
create table schema_column_pk_unique.tab012_01(col01 smallint primary key, col02 integer unique, col03 bigint) with(orientation=column);
alter table schema_column_pk_unique.tab012_01 alter col01 type int;
alter table schema_column_pk_unique.tab012_01 alter col02 type smallint;
alter table schema_column_pk_unique.tab012_01 alter col03 type smallint;
drop schema schema_column_pk_unique cascade;