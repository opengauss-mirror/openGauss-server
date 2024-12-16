create schema test_view_table_depend;
set current_schema to 'test_view_table_depend';

create table t1 (c1 int);
create view v1 as select * from t1;
drop table t1;
\d+ v1;
create table t1 (c1 int);
\d+ v1;
select valid = true from pg_class join pg_object on oid = object_oid where relname = 'v1';

drop table t1 cascade;
create table t1 (c1 int);
create view v1 as select * from t1;
drop table t1;
\d v1;
\d+ v1;
select * from v1;
create table t2 (c1 int);
select * from v1;
drop table t2;
create table t1 (c2 int);
select * from v1;
drop table t1;
create table t1 (c1 int);
insert into t1 values(1);
select * from v1;
\d v1;
\d+ v1;
drop table t1;
create table t1 (c1 varchar);
insert into t1 values('a');
select * from v1;
\d v1;
\d+ v1;

drop table t1 cascade;
create table t1 (c1 int);
create view v1 as select * from t1;
insert into t1 values(1);
select * from v1;
\d v1;
\d+ v1;
drop table t1;
create table t1 (c2 int, c3 int, c1 int);
insert into t1 values(2, 3, 1);
select * from v1;
\d v1;
\d+ v1;

drop table t1 cascade;
create table t1 (c1 int, c2 int, c3 int);
create view v1 as select * from t1;
insert into t1 values(1, 2, 3);
select * from v1;
\d v1;
\d+ v1;
drop table t1;
create table t1 (c2 int);
insert into t1 values(2);
select * from v1;
\d v1;
\d+ v1;

drop view v1;
drop table t1 cascade;
create table t1 (c1 int);
create view v1 as select * from t1;
create view v2 as select * from v1;
insert into t1 values(1);
select * from v1;
select * from v2;
\d v1;
\d v2;
\d+ v1;
\d+ v2;
drop table t1;
\d v1;
\d v2;
\d+ v1;
\d+ v2;
select * from v1;
select * from v2;
create table t1 (c1 varchar);
insert into t1 values('a');
select * from v1;
select * from v2;
\d v1;
\d v2;
\d+ v1;
\d+ v2;

drop table t1 cascade;
create table t1 (c1 int);
create view v1 as select c1 from t1;
create view v2 as select c1 + 1 from v1;
insert into t1 values(1);
select * from v1;
select * from v2;
\d v1;
\d v2;
\d+ v1;
\d+ v2;
drop table t1;
\d v1;
\d v2;
\d+ v1;
\d+ v2;
select * from v1;
select * from v2;
create table t1 (c1 int);
insert into t1 values(1);
select * from v1;
select * from v2;
\d v1;
\d v2;
\d+ v1;
\d+ v2;

drop table t1 cascade;
create table t1 (c1 int);
create view v1 as select c1 + 1 from t1;
insert into t1 values(1);
select * from v1;
\d v1;
\d+ v1;
drop table t1;
\d v1;
\d+ v1;
select * from v1;
create table t1 (c1 int);
insert into t1 values(1);
select * from v1;
\d v1;
\d+ v1;

drop table t1 cascade;
create table t1 (c1 int);
create table t2 (c2 int);
create view v1 as select c1 + c2 from t1, t2;
insert into t1 values(1);
insert into t2 values(2);
select * from v1;
\d v1;
\d+ v1;
drop table t1;
\d v1;
\d+ v1;
select * from v1;
create table t1 (c1 int);
insert into t1 values(1);
select * from v1;
\d v1;
\d+ v1;
drop table t1 cascade;
drop table t2 cascade;

-- partition table
create table partition_t1 (a int, b int)
partition by range(a)
(
partition p1 values less than (100),
partition p2 values less than (200),
partition p3 values less than (MAXVALUE)
);
insert into partition_t1 values(99,1),(180,2),(300,4);
create view partition_v1 as select * from partition_t1 partition(p1);
select * from partition_v1;
\d+ partition_v1
drop table partition_t1;
select * from partition_v1;
\d+ partition_v1
create table partition_t1 (a int, b int)
partition by range(a)
(
partition p1 values less than (100),
partition p2 values less than (200),
partition p3 values less than (MAXVALUE)
);
select * from partition_v1;
\d+ partition_v1

-- less partition and different partition type
drop table partition_t1;
create table partition_t1 (a char(20), b char(20))
partition by list(a)
(
partition p1 values ('100'),
partition p2 values ('200')
);
insert into partition_t1 values('100','1'),('200','2'),('200','3');
select * from partition_v1;
\d+ partition_v1
drop table partition_t1 cascade;

-- secondary partition
CREATE TABLE partition_t1
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201901' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201902' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into partition_t1 values('201901', '1', '1', 1);
insert into partition_t1 values('201901', '2', '1', 1);
insert into partition_t1 values('201902', '1', '1', 1);
insert into partition_t1 values('201902', '2', '1', 1);

create view partition_v1 as select * from partition_t1 partition (p_201901);
create view partition_v2 as select * from partition_t1 partition (p_201902);
create view partition_v3 as select * from partition_t1 partition for ('201901');
create view partition_v4 as select * from partition_t1 subpartition (p_201902_a);
create view partition_v5 as select * from partition_t1 subpartition for ('201902', '2');
select * from partition_v1;
\d+ partition_v1
select * from partition_v2;
\d+ partition_v2
select * from partition_v3;
\d+ partition_v3
select * from partition_v4;
\d+ partition_v4
select * from partition_v5;
\d+ partition_v5
drop table partition_t1;
select * from partition_v1;
\d+ partition_v1
select * from partition_v2;
\d+ partition_v2
select * from partition_v3;
\d+ partition_v3
select * from partition_v4;
\d+ partition_v4
select * from partition_v5;
\d+ partition_v5
CREATE TABLE partition_t1
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201901' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201902' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into partition_t1 values('201901', '1', '1', 1);
insert into partition_t1 values('201901', '2', '1', 1);
insert into partition_t1 values('201902', '1', '1', 1);
insert into partition_t1 values('201902', '2', '1', 1);
select * from partition_v1;
\d+ partition_v1
select * from partition_v2;
\d+ partition_v2
select * from partition_v3;
\d+ partition_v3
select * from partition_v4;
\d+ partition_v4
select * from partition_v5;
\d+ partition_v5

-- less subpartition and different partition type
drop table partition_t1;
CREATE TABLE partition_t1
(
    month_code int  NOT NULL ,
    dept_code  int  NOT NULL ,
    user_no    int  NOT NULL ,
    sales_amt  varchar( 30 )
)
PARTITION BY RANGE (month_code) SUBPARTITION BY RANGE (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN ( 2019 )
  (
    SUBPARTITION p_201901_a VALUES LESS THAN ( 2 )
  ),
  PARTITION p_201902 VALUES LESS THAN ( 2020 )
  (
    SUBPARTITION p_201902_a VALUES LESS THAN ( 3 )
  )
);
insert into partition_t1 values(2018, 1, 1, 'a');
insert into partition_t1 values(2019, 2, 1, 'b');

select * from partition_v1;
\d+ partition_v1
select * from partition_v2;
\d+ partition_v2
select * from partition_v3;
\d+ partition_v3
select * from partition_v4;
\d+ partition_v4
select * from partition_v5;
\d+ partition_v5

drop table partition_t1 cascade;

create table t1 (c1 int, c2 int);
create view v1 as select * from t1;
drop table t1;
create table t1 (c2 int, c3 int, c4 int, c1 int);
insert into t1 values (2, 3, 4, 1);
select * from v1;

drop table t1 cascade;

--test cte
create table t1 (a int);
create view v1 as with tmp as (select * from t1) select * from tmp;
drop table t1;
select * from v1;
\d+ v1

create table t1 (a int);
\d+ v1
select * from v1;

drop table t1 cascade;

-- test setop
create table t1 (a int);
create view v1 as select 1 union select a from t1;
drop table t1;
select * from v1;
\d+ v1

create table t1 (a int);
\d+ v1
select * from v1;

drop table t1 cascade;

-- test subquery
create table t1 (a int);
create table t2 (a int);
insert into t1 values (1);
insert into t2 values (1);
create view v1 as select * from (select * from t1);
create view v2 as select * from t1 where exists (select * from t1 inner join t2 on t1.a = t2.a);
drop table t1;
select * from v1;
\d+ v1
select * from v2;
\d+ v2

create table t1 (a int);
insert into t1 values (1);
\d+ v1
select * from v1;
\d+ v2
select * from v2;

drop table t2;
\d+ v2
select * from v2;

create table t2 (b int, a int);
insert into t2 values (2, 1);
select * from v2;

drop view v1;
drop view v2;
drop table t1;
drop table t2;

drop schema test_view_table_depend cascade;
reset current_schema;
