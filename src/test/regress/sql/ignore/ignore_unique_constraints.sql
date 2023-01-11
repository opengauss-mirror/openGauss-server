create database sql_ignore_unique_test dbcompatibility 'B';
\c sql_ignore_unique_test;
drop table if exists t_ignore;
create table t_ignore(col1 int, col2 int unique, col3 int unique);

-- sqlbypass
set enable_opfusion = on;
insert into t_ignore values(1,1,1);
insert into t_ignore values(2,2,2);
update t_ignore set col2 = 1 where col1 = 2;
insert /*+ ignore_error */ into t_ignore values (2,1,2);
select * from t_ignore;
update /*+ ignore_error */ t_ignore set col2 = 1 where col1 = 2;
select * from t_ignore;

-- insert ignore from other tables with duplicate keys
create table t_from (col1 int, col2 int, col3 int);
insert into t_from values(9,9,9);
insert into t_from values(1,1,1);
insert into t_from values(2,2,2);
insert into t_ignore select * from t_from;
select * from t_ignore;

-- no sqlbypass
set enable_opfusion = off;
insert into t_ignore values(2,1,2);
update t_ignore set col2 = 1 where col1 = 2;
insert /*+ ignore_error */ into t_ignore values(2,1,2);
update /*+ ignore_error */ t_ignore set col2 = 1 where col1 = 2;
select * from t_ignore;

-- insert ignore from other tables with duplicate keys
insert into t_ignore select * from t_from;
select * from t_ignore;

drop table t_ignore;
set enable_opfusion = on;
drop table t_from;

-- test for integrity of UPSERT
drop table if exists t_unique_upsert;
create table t_unique_upsert(id int unique, cont text);
set enable_opfusion = on;
insert into t_unique_upsert values(1, 'a');
insert into t_unique_upsert values(2, 'b');
insert /*+ ignore_error */ into t_unique_upsert values(1, 'x') on duplicate key update cont = 'on dup';
select * from t_unique_upsert;

delete from t_unique_upsert;
set enable_opfusion = off;
insert into t_unique_upsert values(1, 'a');
insert into t_unique_upsert values(2, 'b');
insert /*+ ignore_error */ into t_unique_upsert values(1, 'x') on duplicate key update cont = 'on dup';
select * from t_unique_upsert;

-- test for partition table with unique key
-- opfusion: on
set enable_opfusion = on;
set enable_partition_opfusion = on;
drop table if exists t_unique_key_partition;
CREATE TABLE t_unique_key_partition
(
    num     integer UNIQUE NOT NULL,
    ca_city character varying(60)
) PARTITION BY RANGE (num)
(
    PARTITION P1 VALUES LESS THAN(5000),
    PARTITION P2 VALUES LESS THAN(10000),
    PARTITION P3 VALUES LESS THAN(15000),
    PARTITION P4 VALUES LESS THAN(20000),
    PARTITION P5 VALUES LESS THAN(25000),
    PARTITION P6 VALUES LESS THAN(30000),
    PARTITION P7 VALUES LESS THAN(40000)
);
insert into t_unique_key_partition values(1, 'shenzhen');
insert into t_unique_key_partition values(2, 'beijing');
select * from t_unique_key_partition;
explain(costs off) insert /*+ ignore_error */ into  t_unique_key_partition values (1);
insert /*+ ignore_error */ into  t_unique_key_partition values (1);
select * from t_unique_key_partition;
update /*+ ignore_error */ t_unique_key_partition set num = 1 where num = 2;
select * from t_unique_key_partition;
-- opfusion: off
set enable_opfusion = off;
set enable_partition_opfusion = off;
explain(costs off) insert /*+ ignore_error */ into  t_unique_key_partition values (1);
insert /*+ ignore_error */ into  t_unique_key_partition values (1);
select * from t_unique_key_partition;
update /*+ ignore_error */ t_unique_key_partition set num = 1 where num = 2;
select * from t_unique_key_partition;

-- test for subpartition table
drop table if exists ignore_range_range;
CREATE TABLE ignore_range_range
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) UNIQUE ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
    PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201901' )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201901_b VALUES LESS THAN( '3' )
  ),
  PARTITION p_201902 VALUES LESS THAN( '201902' )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN( '2' ),
    SUBPARTITION p_201902_b VALUES LESS THAN( '3' )
  )
);
set enable_opfusion = on;
set enable_partition_opfusion = on;
insert /*+ ignore_error */ into ignore_range_range values('201901', '1', '1', 1);
insert /*+ ignore_error */  into ignore_range_range values('201901', '1', '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', '2', '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = '1' where dept_code = '2';
select * from ignore_range_range;

delete from ignore_range_range;
set enable_opfusion = off;
set enable_partition_opfusion = off;
insert /*+ ignore_error */  into ignore_range_range values('201901', '1', '1', 1);
insert /*+ ignore_error */  into ignore_range_range values('201901', '1', '1', 1);
select * from ignore_range_range;
insert into ignore_range_range values('201901', '2', '1', 1);
update /*+ ignore_error */ ignore_range_range set dept_code = '1' where dept_code = '2';
select * from ignore_range_range;

-- test for ustore table
drop table if exists t_ignore;
create table t_ignore(num int unique) with(storage_type=ustore);

-- test for ustore table, opfusion: on
set enable_opfusion = on;
insert into t_ignore values(1);
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1);
select * from t_ignore;
insert into t_ignore values(2);
update /*+ ignore_error */ t_ignore set num = 1 where num = 2;
select * from t_ignore;

-- test for ustore table, opfusion: off
set enable_opfusion = off;
delete from t_ignore;
insert into t_ignore values(1);
explain(costs off) insert /*+ ignore_error */ into t_ignore values(1);
insert /*+ ignore_error */ into t_ignore values(1);
select * from t_ignore;
insert into t_ignore values(2);
update /*+ ignore_error */ t_ignore set num = 1 where num = 2;
select * from t_ignore;

-- test for segment table
drop table if exists t_ignore;
create table t_ignore(num int unique) with(segment = on);

-- test for segment table, opfusion: on
set enable_opfusion = on;
insert into t_ignore values(1);
insert /*+ ignore_error */ into t_ignore values(1);
select * from t_ignore;
insert into t_ignore values(2);
update /*+ ignore_error */ t_ignore set num = 1 where num = 2;
select * from t_ignore;

-- test for segment table, opfusion: off
delete from t_ignore;
set enable_opfusion = off;
insert into t_ignore values(1);
insert /*+ ignore_error */ into t_ignore values(1);
select * from t_ignore;
insert into t_ignore values(2);
update /*+ ignore_error */ t_ignore set num = 1 where num = 2;
select * from t_ignore;

-- test for insert table with primary key
drop table if exists t_pri_key_insert;
create table t_pri_key_insert(id int primary key);
insert /*+ ignore_error */ into t_pri_key_insert values(null);
insert /*+ ignore_error */ into t_pri_key_insert values(1);
insert /*+ ignore_error */ into t_pri_key_insert values(1);
select * from t_pri_key_insert;
drop table t_pri_key_insert;

-- test for update table with primary key
drop table if exists t_pri_key_update;
create table t_pri_key_update(c1 int primary key, c2 int);
insert into t_pri_key_update values(1, 101), (2, 201);
update /*+ ignore_error */ t_pri_key_update set c2 = 999 where c1 = 2;
update /*+ ignore_error */ t_pri_key_update set c1 = null where c1 = 2;
select * from t_pri_key_update;
drop table t_pri_key_update;

create table t_pri_key_update_partition (c1 int primary key, c2 int)
    partition by list(c2)
(
partition p1 values (1),
partition p2 values (10),
partition p3 values (100),
partition p4 values (500)
);
insert into t_pri_key_update_partition values (1, 1), (10, 10), (100, 100);
select * from t_pri_key_update_partition;
update /*+ ignore_error */ t_pri_key_update_partition set c2 = 500 where c1 = 10;
select * from t_pri_key_update_partition;

-- test for column orientation table: not supported
drop table if exists t_column_orien cascade;
create table t_column_orien(c1 int primary key) with (orientation=column);
insert into t_column_orien values(1), (2);
insert /*+ ignore_error */ into t_column_orien values(1);
insert /*+ ignore_error */ into t_column_orien values(null);
update /*+ ignore_error */ t_column_orien set c1 = null where c1 = 2;
update /*+ ignore_error */ t_column_orien set c1 = 1 where c1 = 2;

\c postgres
drop database if exists sql_ignore_unique_test;