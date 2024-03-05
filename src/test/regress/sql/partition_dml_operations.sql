DROP SCHEMA partition_dml_operations CASCADE;
CREATE SCHEMA partition_dml_operations;
SET CURRENT_SCHEMA TO partition_dml_operations;

--select
create table tsource(ld int not null,sd int not null,jname varchar2) partition by range(ld)
(
 partition ts1 values less than(6),
 partition ts2 values less than(36)
);
insert into tsource values (5),(15);
select * from tsource partition (ts1);
select * from tsource partition for(5);
select * from tsource subpartition (ts1);
select * from tsource subpartition for(3,6);
drop table tsource;
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);


select * from range_list partition (p_201901);
select * from range_list subpartition (p_201901_a);
select * from range_list partition for ('201902');
select * from range_list subpartition for ('201902','1');

drop table range_list;
--insert
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into test_range_pt partition (p1) values(1);
insert into test_range_pt partition (p2) values(1);
insert into test_range_pt partition (p3) values(1);

insert into test_range_pt partition for (1) values(1);
insert into test_range_pt partition for (2001) values(1);
insert into test_range_pt partition for (3001) values(1);

drop table test_range_pt;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into range_list partition (p_201901) values('201902', '1', '1', 1);
insert into range_list partition (p_201902) values('201902', '1', '1', 1);
insert into range_list partition (p_201902_a) values('201902', '1', '1', 1);
insert into range_list partition (p_201902_c) values('201902', '1', '1', 1);

insert into range_list subpartition (p_201901_a) values('201902', '1', '1', 1);
insert into range_list subpartition (p_201901_b) values('201902', '1', '1', 1);
insert into range_list subpartition (p_201902_a) values('201902', '1', '1', 1);
insert into range_list subpartition (p_201902_b) values('201902', '1', '1', 1);

insert into range_list subpartition (p_201901) values('201902', '1', '1', 1);
insert into range_list subpartition (p_201902) values('201902', '1', '1', 1);
insert into range_list subpartition (p_201903) values('201902', '1', '1', 1);

insert into range_list partition for ('201902') values('201902', '1', '1', 1);
insert into range_list partition for ('201903') values('201902', '1', '1', 1);
insert into range_list partition for ('201910') values('201902', '1', '1', 1);

insert into range_list subpartition for ('201902','1') values('201902', '1', '1', 1);
insert into range_list subpartition for ('201902','2') values('201902', '1', '1', 1);
insert into range_list subpartition for ('201903','1') values('201902', '1', '1', 1);
insert into range_list subpartition for ('201903','2') values('201902', '1', '1', 1);

insert into range_list subpartition for ('201902') values('201902', '1', '1', 1);
insert into range_list subpartition for ('201910','1') values('201902', '1', '1', 1);

drop table range_list;

--update
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001, 1, 1);
insert into test_range_pt values(3001, 1, 1);

update test_range_pt partition (p1) set b = 2;
select * from test_range_pt;
update test_range_pt partition (p1) set a = 2;
select * from test_range_pt;

update test_range_pt partition for (1) set b = 3;
select * from test_range_pt;
update test_range_pt partition for (1) set a = 3;
select * from test_range_pt;

drop table test_range_pt;


CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select *from range_list;

update range_list partition (p_201901) set user_no = '2';
select *from range_list;
update range_list subpartition (p_201901_a) set user_no = '3';
select *from range_list;

update range_list partition for ('201902') set user_no = '4';
select *from range_list;
update range_list subpartition for ('201902','2') set user_no = '5';
select *from range_list;

drop table range_list;

--delete
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001, 1, 1);
insert into test_range_pt values(3001, 1, 1);

delete from test_range_pt partition (p1);
select * from test_range_pt;

delete from test_range_pt partition for (2001);
select * from test_range_pt;

drop table test_range_pt;


CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select *from range_list;

delete from range_list partition (p_201901);
select *from range_list;
delete from range_list partition for ('201903');
select *from range_list;
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201902', '2', '1', 1);
insert into range_list values('201902', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
insert into range_list values('201903', '1', '1', 1);
insert into range_list values('201903', '2', '1', 1);
select *from range_list;
delete from range_list subpartition (p_201901_a);
select *from range_list;
delete from range_list subpartition for ('201903','2');
select *from range_list;

--error when db_compatibility is A
delete from range_list partition (p_201901_a);
delete from range_list as T partition (p_201901_a);
drop table range_list;

--upsert
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) PRIMARY KEY  ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

create index idx1 on range_list(month_code,dept_code) local;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

create index idx2 on range_list(month_code) global;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

drop table range_list;


CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) PRIMARY KEY ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL  ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

create index idx1 on range_list(month_code,dept_code) local;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

create index idx2 on range_list(month_code) global;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
select * from range_list;
delete from range_list;

drop table range_list;


CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) PRIMARY KEY ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL  ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into range_list partition (p_201901)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list partition (p_201901)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list partition (p_201902)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 10;
select * from range_list;

insert into range_list subpartition (p_201901_a)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 10;
insert into range_list subpartition (p_201901_b)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 20;
select * from range_list;

insert into range_list partition for ('201902')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 30;
insert into range_list partition for ('201903')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 40;
select * from range_list;

insert into range_list subpartition for ('201902','1')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 40;
insert into range_list subpartition for ('201902','2')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 50;
select * from range_list;

drop table range_list;

CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL  ,
    sales_amt  int,
	PRIMARY KEY(month_code, dept_code)
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into range_list partition (p_201901)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list partition (p_201901)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 5;
insert into range_list partition (p_201902)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 10;
select * from range_list;

insert into range_list subpartition (p_201901_a)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 10;
insert into range_list subpartition (p_201901_b)  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 20;
select * from range_list;

insert into range_list partition for ('201902')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 30;
insert into range_list partition for ('201903')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 40;
select * from range_list;

insert into range_list subpartition for ('201902','1')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 40;
insert into range_list subpartition for ('201902','2')  values('201902', '1', '1', 1)  ON DUPLICATE KEY UPDATE sales_amt = 50;
select * from range_list;

drop table range_list;

drop table test_range_pt;
create table test_range_pt (a int, b int primary key, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into test_range_pt values (1,1),(2001,2001);
insert into test_range_pt partition (p1) values(1,2001)  ON DUPLICATE KEY UPDATE a = 5;
drop table test_range_pt;

drop table if exists tsource cascade;
create table tsource(ld int ,sd int not null,code int primary key)
partition by range(ld) subpartition by range(sd)
(
  partition ts1 values less than(16)(
  subpartition ts11 values less than(16),
  subpartition ts12 values less than(66)
  ),
  partition ts2 values less than(66)(
  subpartition ts21 values less than(16),
  subpartition ts22 values less than(66)
  )
);
insert into tsource values(10,1,1),(60,1,2);
insert into tsource partition (ts1) values(10,1,2) on duplicate key update sd=3;
insert into tsource values(10,60,3);
insert into tsource subpartition (ts11) values(10,1,3) on duplicate key update sd=4;
drop table if exists tsource cascade;

--Merge into
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001,1 ,1);

create table newtest_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

insert into newtest_range_pt values(1,2,2);
insert into newtest_range_pt values(2,2,2);
insert into newtest_range_pt values(2001,2,2);

MERGE INTO test_range_pt p
USING newtest_range_pt np
ON p.a= np.a
WHEN MATCHED THEN
  UPDATE SET b = np.b, c = np.c
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.a, np.b, np.c);
  
select * from test_range_pt;
select * from newtest_range_pt;

delete from test_range_pt;
delete from newtest_range_pt;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001,1 ,1);
insert into newtest_range_pt values(1,2,2);
insert into newtest_range_pt values(2,2,2);
insert into newtest_range_pt values(2001,2,2);
insert into newtest_range_pt values(2002,2,2);

MERGE INTO test_range_pt partition (p1) p
USING newtest_range_pt partition (p1) np
ON p.a= np.a
WHEN MATCHED THEN
  UPDATE SET b = np.b, c = np.c
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.a, np.b, np.c);

select * from test_range_pt;
select * from newtest_range_pt;

delete from test_range_pt;
delete from newtest_range_pt;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001,1 ,1);
insert into newtest_range_pt values(1,2,2);
insert into newtest_range_pt values(2,2,2);
insert into newtest_range_pt values(2001,2,2);
insert into newtest_range_pt values(2002,2,2);

MERGE INTO test_range_pt partition for (1) p
USING newtest_range_pt partition for (1) np
ON p.a= np.a
WHEN MATCHED THEN
  UPDATE SET b = np.b, c = np.c
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.a, np.b, np.c);

select * from test_range_pt;
select * from newtest_range_pt;

drop table test_range_pt;
drop table newtest_range_pt;



CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);

CREATE TABLE newrange_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201910' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2')
  )
);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list p
USING newrange_list np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = np.dept_code, user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);
  
select *from range_list;
select *from newrange_list;

delete from range_list;
delete from newrange_list;

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list partition (p_201901) p
USING newrange_list partition (p_201901) np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = np.dept_code, user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);

select *from range_list;
select *from newrange_list;

delete from range_list;
delete from newrange_list;

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list partition for ('201901') p
USING newrange_list partition for ('201901') np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = np.dept_code, user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);

select *from range_list;
select *from newrange_list;

delete from range_list;
delete from newrange_list;

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list subpartition (p_201901_a) p
USING newrange_list subpartition (p_201901_a) np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = np.dept_code, user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);
  
select *from range_list;
select *from newrange_list;

delete from range_list;
delete from newrange_list;

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list subpartition for ('201901', '1') p
USING newrange_list subpartition for ('201901', '1') np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = np.dept_code, user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);
  
select *from range_list;
select *from newrange_list;

delete from range_list;
delete from newrange_list;

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201902', '2', '1', 2);
insert into newrange_list values('201902', '1', '1', 1);
insert into newrange_list values('201903', '1', '1', 2);

MERGE INTO range_list  p
USING newrange_list  np
ON p.month_code= np.month_code
WHEN MATCHED THEN
  UPDATE SET dept_code = '3', user_no = np.user_no, sales_amt = np.sales_amt
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.month_code, np.dept_code, np.user_no, np.sales_amt);
  
select *from range_list;
select *from newrange_list;

drop table range_list;
drop table newrange_list;


create table lm_list_range (id int,sd int,name varchar2) 
partition by list(id) subpartition by range(sd)(
	partition ts1 values(1,2,3,4,5)
		(subpartition ts11 values less than(5),subpartition ts12 values less than(10),subpartition ts13 values less than(20)),
	partition ts2 values(6,7,8,9,10),
	partition ts3 values(11,12,13,14,15)
		(subpartition ts31 values less than(5),subpartition ts32 values less than(10),subpartition ts33 values less than(20)));

select * from lm_list_range partition for(5,34);
drop table lm_list_range;

drop table if exists tsource;
create table  if not exists tsource(fd int,sd int,ttime date)partition by range (ttime)interval('1 day')
(partition p1 values less than ('2022-02-01 00:00:00'),
 partition p2 values less than ('2022-02-02 00:00:00'));
create index pidx on tsource(fd);
create index tidx on tsource(ttime);
insert into tsource values(1,2,'2022-02-01 20:00:00'),(2,3,'2022-02-02 20:00:00');
insert into tsource partition(p2) values(2,2,'2022-02-01 20:30:00');
insert into tsource partition for('2022-02-01 11:30:00') values(2,3,'2022-02-01 20:31:00');
insert into tsource partition for('2022-02-01 11:30:00'::date) values(2,3,'2022-02-01 20:31:00'); 
drop table if exists tsource;

DROP SCHEMA partition_dml_operations CASCADE;
RESET CURRENT_SCHEMA; 

--delete multi partitions in B db compatibility
drop database if exists test_part_delete_B_db;
create database test_part_delete_B_db dbcompatibility 'B';
\c test_part_delete_B_db

--part table delete
create table test_range_pt (a int, b int, c int)
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
) ENABLE ROW MOVEMENT;

insert into test_range_pt values(1, 1, 1);
insert into test_range_pt values(2001, 1, 1);
insert into test_range_pt values(3001, 1, 1);
insert into test_range_pt values(4001, 1, 1);
insert into test_range_pt values(5001, 1, 1);

delete from test_range_pt partition (p1);
select * from test_range_pt;
delete from test_range_pt t partition ("p2");
select * from test_range_pt;
delete from test_range_pt as t partition (p5, p3);
select * from test_range_pt;
delete test_range_pt partition (p5, p3);
select * from test_range_pt;
--syntax error
delete from test_range_pt partition (p5, p3) as t;
delete from test_range_pt t partition (p5 p3);
delete from test_range_pt t partition (3);

drop table test_range_pt;

--part column store table delete
create table test_range_pt_cstore (a int, b int, c int)
WITH (ORIENTATION=COLUMN )
partition by range(a)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (maxvalue)
) ENABLE ROW MOVEMENT;

insert into test_range_pt_cstore values(1, 1, 1);
insert into test_range_pt_cstore values(2001, 1, 1);
insert into test_range_pt_cstore values(3001, 1, 1);
insert into test_range_pt_cstore values(4001, 1, 1);
insert into test_range_pt_cstore values(5001, 1, 1);

delete from test_range_pt_cstore partition (p1);
select * from test_range_pt_cstore;
delete from test_range_pt_cstore t partition ("p2");
select * from test_range_pt_cstore;
delete from test_range_pt_cstore as t partition (p5, p3);
select * from test_range_pt_cstore;

drop table test_range_pt_cstore;

--subpart table delete
CREATE TABLE range_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY RANGE (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES LESS THAN( '201902' )
  (
    SUBPARTITION p_201901_a values ('1'),
    SUBPARTITION p_201901_b values ('2'),
    SUBPARTITION p_201901_c values ('3')
  ),
  PARTITION p_201902 VALUES LESS THAN( '201903' )
  (
    SUBPARTITION p_201902_a values ('1'),
    SUBPARTITION p_201902_b values ('2'),
    SUBPARTITION p_201902_c values ('3')
  ),
  PARTITION p_201903 VALUES LESS THAN( '201904' )
  (
    SUBPARTITION p_201903_a values ('1'),
    SUBPARTITION p_201903_b values ('2'),
    SUBPARTITION p_201903_c values ('3')
  ),
  PARTITION p_201904 VALUES LESS THAN( MAXVALUE )
  (
    SUBPARTITION p_201904_a values ('1'),
    SUBPARTITION p_201904_b values ('2'),
    SUBPARTITION p_201904_c values ('3')
  )
);

insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201901', '2', '1', 2);
insert into range_list values('201901', '3', '1', 3);
insert into range_list values('201902', '1', '1', 4);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 6);
insert into range_list values('201903', '1', '1', 7);
insert into range_list values('201903', '2', '1', 8);
insert into range_list values('201903', '3', '1', 9);
insert into range_list values('201904', '1', '1', 10);
insert into range_list values('201904', '2', '1', 11);
insert into range_list values('201904', '3', '1', 12);
--single part
select * from range_list order by sales_amt;
delete from range_list partition(p_201902_a);
select * from range_list order by sales_amt;
delete from range_list t partition(p_201901);
select * from range_list order by sales_amt;
delete from range_list as t partition("p_201903_b");
select * from range_list order by sales_amt;

delete range_list;
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201901', '2', '1', 2);
insert into range_list values('201901', '3', '1', 3);
insert into range_list values('201902', '1', '1', 4);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 6);
insert into range_list values('201903', '1', '1', 7);
insert into range_list values('201903', '2', '1', 8);
insert into range_list values('201903', '3', '1', 9);
insert into range_list values('201904', '1', '1', 10);
insert into range_list values('201904', '2', '1', 11);
insert into range_list values('201904', '3', '1', 12);
--multi parts
delete from range_list as t partition(p_201901, p_201902);
select * from range_list order by sales_amt;
delete from range_list t partition(p_201903_a, "p_201904_b", p_201903_b);
select * from range_list order by sales_amt;

delete range_list;
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201901', '2', '1', 2);
insert into range_list values('201901', '3', '1', 3);
insert into range_list values('201902', '1', '1', 4);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 6);
insert into range_list values('201903', '1', '1', 7);
insert into range_list values('201903', '2', '1', 8);
insert into range_list values('201903', '3', '1', 9);
insert into range_list values('201904', '1', '1', 10);
insert into range_list values('201904', '2', '1', 11);
insert into range_list values('201904', '3', '1', 12);

delete from range_list partition(p_201902_b, p_201901);
select * from range_list order by sales_amt;
delete from range_list partition(p_201904, p_201903_a,p_201903_b);
select * from range_list order by sales_amt;

delete range_list;
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201901', '2', '1', 2);
insert into range_list values('201901', '3', '1', 3);
insert into range_list values('201902', '1', '1', 4);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 6);
insert into range_list values('201903', '1', '1', 7);
insert into range_list values('201903', '2', '1', 8);
insert into range_list values('201903', '3', '1', 9);
insert into range_list values('201904', '1', '1', 10);
insert into range_list values('201904', '2', '1', 11);
insert into range_list values('201904', '3', '1', 12);

delete from range_list as t partition(p_201902_b, p_201902);
select * from range_list order by sales_amt;
delete from range_list as t partition(p_201901, p_201901_c);
select * from range_list order by sales_amt;

delete range_list;
insert into range_list values('201901', '1', '1', 1);
insert into range_list values('201901', '2', '1', 2);
insert into range_list values('201901', '3', '1', 3);
insert into range_list values('201902', '1', '1', 4);
insert into range_list values('201902', '2', '1', 5);
insert into range_list values('201902', '3', '1', 6);
insert into range_list values('201903', '1', '1', 7);
insert into range_list values('201903', '2', '1', 8);
insert into range_list values('201903', '3', '1', 9);
insert into range_list values('201904', '1', '1', 10);
insert into range_list values('201904', '2', '1', 11);
insert into range_list values('201904', '3', '1', 12);

explain delete from range_list as t partition(p_201904_c, p_201901_b, p_201902_a, p_201902, p_201903, p_201903_b);
delete from range_list as t partition(p_201904_c, p_201901_b, p_201902_a, p_201902, p_201903, p_201903_b);
select * from range_list order by sales_amt;

--delete part table with local index (single column)
set enable_seqscan=false;
set enable_bitmapscan=false;

CREATE INDEX idx_range_list_part ON
range_list(month_code) LOCAL
(
PARTITION idx_range_list_part1 (SUBPARTITION idx_range_list_subpart11, SUBPARTITION idx_range_list_subpart12, SUBPARTITION idx_range_list_subpart13),
PARTITION idx_range_list_part2 (SUBPARTITION idx_range_list_subpart21, SUBPARTITION idx_range_list_subpart22, SUBPARTITION idx_range_list_subpart23),
PARTITION idx_range_list_part3 (SUBPARTITION idx_range_list_subpart31, SUBPARTITION idx_range_list_subpart32, SUBPARTITION idx_range_list_subpart33),
PARTITION idx_range_list_part4 (SUBPARTITION idx_range_list_subpart41, SUBPARTITION idx_range_list_subpart42, SUBPARTITION idx_range_list_subpart43)
);


EXPLAIN DELETE FROM range_list PARTITION(p_201901, p_201902) WHERE month_code='201905';
EXPLAIN DELETE FROM range_list T PARTITION(p_201904_a, p_201904_C) WHERE month_code='201905';
EXPLAIN DELETE FROM range_list AS T PARTITION(p_201904_A, p_201901_B, p_201902_A, p_201902, p_201903, p_201903_B) WHERE month_code='201905';

DELETE FROM range_list T PARTITION(p_201904_a, p_201904_C) WHERE month_code='201904';
select * from range_list order by sales_amt;
DROP INDEX idx_range_list_part;

--delete part table with local index (multi column)
CREATE INDEX idx_range_list_part ON
range_list(user_no, sales_amt) LOCAL
(
PARTITION idx_range_list_part1 (SUBPARTITION idx_range_list_subpart11, SUBPARTITION idx_range_list_subpart12, SUBPARTITION idx_range_list_subpart13),
PARTITION idx_range_list_part2 (SUBPARTITION idx_range_list_subpart21, SUBPARTITION idx_range_list_subpart22, SUBPARTITION idx_range_list_subpart23),
PARTITION idx_range_list_part3 (SUBPARTITION idx_range_list_subpart31, SUBPARTITION idx_range_list_subpart32, SUBPARTITION idx_range_list_subpart33),
PARTITION idx_range_list_part4 (SUBPARTITION idx_range_list_subpart41, SUBPARTITION idx_range_list_subpart42, SUBPARTITION idx_range_list_subpart43)
);

EXPLAIN DELETE FROM range_list PARTITION(p_201901, p_201902) WHERE user_no=1 and sales_amt=1;
EXPLAIN DELETE FROM range_list T PARTITION(p_201904_a, p_201904_B) WHERE user_no=1 and sales_amt=11;
EXPLAIN DELETE FROM range_list AS T PARTITION(p_201904_A, p_201901_B, p_201902_A, p_201902, p_201903, p_201903_B) WHERE user_no=1 and sales_amt in (10,11,12);

DELETE FROM range_list T PARTITION(p_201904_a, p_201904_B) WHERE user_no=1 and sales_amt=11;
select * from range_list order by sales_amt;
DROP INDEX idx_range_list_part;

--delete part table with global index
CREATE INDEX idx_range_list_global ON range_list(month_code) GLOBAL;

EXPLAIN DELETE FROM range_list WHERE month_code='201905';
EXPLAIN DELETE FROM range_list PARTITION(p_201901, p_201901) WHERE month_code='201905';
EXPLAIN DELETE FROM range_list T PARTITION(p_201904_a, p_201904_C, p_201904_b) WHERE month_code='201905';
EXPLAIN DELETE FROM range_list AS T PARTITION(p_201904_A, p_201901_B, p_201902_A, p_201902, p_201903_B) WHERE month_code='201905';
EXPLAIN PLAN SET statement_id='DELETE_MULTI_PARTS' FOR DELETE FROM range_list AS T PARTITION(p_201904_A, p_201901_B, p_201902_A, p_201902, p_201903_B) WHERE month_code='201905';
SELECT statement_id,id,operation,options,object_name,object_type,projection FROM PLAN_TABLE WHERE statement_id='DELETE_MULTI_PARTS' ORDER BY id;
DELETE FROM PLAN_TABLE WHERE statement_id='DELETE_MULTI_PARTS';

DROP INDEX idx_range_list_global;
set enable_bitmapscan=true;
set enable_seqscan=true;
drop table range_list;

-- test multi partitions in order
-- range partitions with local index, single column
drop table if exists t_multi_parts_order;
create table t_multi_parts_order (
    col int
)
partition by range(col) 
(
    partition p1 values less than(10),
    partition pd values less than(maxvalue) 
)enable row movement;

create index tbl_idx_t_multi_parts_order on t_multi_parts_order(col) local;

insert into t_multi_parts_order select generate_series(1,40);
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;

begin;
delete from t_multi_parts_order partition(p1, pd) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

begin;
delete from t_multi_parts_order partition(pd, p1) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

explain (costs off) delete from t_multi_parts_order partition(pd, p1) order by col limit 10;

begin;
delete from t_multi_parts_order partition(p1, pd) order by col desc limit 10;
select * from t_multi_parts_order partition (p1) order by col desc limit 10;
select * from t_multi_parts_order partition (pd) order by col desc limit 10;
rollback;

begin;
delete from t_multi_parts_order partition(pd, p1) order by col desc limit 10;
select * from t_multi_parts_order partition (p1) order by col desc limit 10;
select * from t_multi_parts_order partition (pd) order by col desc limit 10;
rollback;

explain (costs off) delete from t_multi_parts_order partition(pd, p1) order by col desc limit 10;



drop table if exists t_multi_parts_order;

-- range partitions with global index, single column
create table t_multi_parts_order (
    col int
)
partition by range(col) 
(
    partition p1 values less than(10),
    partition pd values less than(maxvalue) 
)enable row movement;

create index tbl_idx_t_multi_parts_order on t_multi_parts_order(col) global;

insert into t_multi_parts_order select generate_series(1,40);
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;

begin;
delete from t_multi_parts_order partition(p1, pd) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

begin;
delete from t_multi_parts_order partition(pd, p1) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

explain delete from t_multi_parts_order partition(pd, p1) order by col limit 10;
drop table if exists t_multi_parts_order;

-- hash partitions with local index, single column
create table t_multi_parts_order (
    col int
)
partition by hash(col) 
(
    partition p1,
    partition pd
)enable row movement;

create index tbl_idx_t_multi_parts_order on t_multi_parts_order(col) local;

insert into t_multi_parts_order select generate_series(1,40);
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;

begin;
delete from t_multi_parts_order partition(p1, pd) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

begin;
delete from t_multi_parts_order partition(pd, p1) order by col limit 10;
select * from t_multi_parts_order partition (p1) order by col limit 10;
select * from t_multi_parts_order partition (pd) order by col limit 10;
rollback;

explain delete from t_multi_parts_order partition(pd, p1) order by col limit 10;
drop table if exists t_multi_parts_order;

-- list partitions with local index, single column
create table t_multi_parts_order (
    col int
)
partition by list(col) 
(
    partition p1 values(1),
    partition pd values(2)
)enable row movement;

create index tbl_idx_t_multi_parts_order on t_multi_parts_order(col) local;

insert into t_multi_parts_order select generate_series(1,2);
select * from t_multi_parts_order partition (p1) order by col limit 1;
select * from t_multi_parts_order partition (pd) order by col limit 1;

begin;
delete from t_multi_parts_order partition(p1, pd) order by col limit 1;
select * from t_multi_parts_order partition (p1) order by col limit 1;
select * from t_multi_parts_order partition (pd) order by col limit 1;
rollback;

begin;
delete from t_multi_parts_order partition(pd, p1) order by col limit 1;
select * from t_multi_parts_order partition (p1) order by col limit 1;
select * from t_multi_parts_order partition (pd) order by col limit 1;
rollback;

explain delete from t_multi_parts_order partition(pd, p1) order by col limit 10;

drop table if exists t_multi_parts_order;

-- range partitions with local index, multi columns
create table t_multi_parts_order (
    col int,
    col1 int
)
partition by range(col) 
(
    partition p1 values less than(10),
    partition p2 values less than(20),
    partition p3 values less than(30),
    partition p4 values less than(maxvalue) 
)enable row movement;

create index tbl_idx_t_multi_parts_order on t_multi_parts_order(col1) local;

insert into t_multi_parts_order select generate_series(0,39), generate_series(0,39);
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);

begin;
delete from t_multi_parts_order partition(p1, p4) order by col1 limit 10;
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);
rollback;
select count(*) from t_multi_parts_order partition (p1);

begin;
delete from t_multi_parts_order partition(p4, p1) order by col1 limit 10;
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);
rollback;
select count(*) from t_multi_parts_order partition (p1);

begin;
delete from t_multi_parts_order partition(p3, p2, p4) order by col1 limit 10;
select count(*) from t_multi_parts_order partition (p2);
select count(*) from t_multi_parts_order partition (p3);
select count(*) from t_multi_parts_order partition (p4);
rollback;
select count(*) from t_multi_parts_order partition (p2);

explain delete from t_multi_parts_order partition(p3, p2, p4) order by col1 limit 10;

drop table if exists t_multi_parts_order;

-- range partitions without index
create table t_multi_parts_order (
    col int
)
partition by range(col) 
(
    partition p1 values less than(10),
    partition p2 values less than(20),
    partition p3 values less than(30),
    partition p4 values less than(maxvalue) 
);
insert into t_multi_parts_order select generate_series(0,39);
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);

begin;
delete from t_multi_parts_order partition(p1, p4) limit 10;
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);
rollback;
explain delete from t_multi_parts_order partition(p1, p4) limit 10;

begin;
delete from t_multi_parts_order partition(p4, p1) limit 10;
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p4);
rollback;
explain delete from t_multi_parts_order partition(p4, p1) limit 10;


ALTER TABLE t_multi_parts_order MERGE PARTITIONS p1,p2  INTO PARTITION p0;
begin;
delete from t_multi_parts_order partition(p3, p0, p4) limit 20;
select count(*) from t_multi_parts_order partition (p0);
select count(*) from t_multi_parts_order partition (p3);
select count(*) from t_multi_parts_order partition (p4);
rollback;
explain delete from t_multi_parts_order partition(p3, p0, p4) limit 20;

ALTER TABLE t_multi_parts_order SPLIT PARTITION p0 AT ( 10 ) INTO ( PARTITION p1 , PARTITION p2);

begin;
delete from t_multi_parts_order partition(p3, p2, p4) limit 10;
select count(*) from t_multi_parts_order partition (p2);
select count(*) from t_multi_parts_order partition (p3);
select count(*) from t_multi_parts_order partition (p4);
rollback;

explain delete from t_multi_parts_order partition(p3, p2, p4) limit 10;

ALTER TABLE t_multi_parts_order drop partition p4;
ALTER TABLE t_multi_parts_order add partition p5 values less than(50);
ALTER TABLE t_multi_parts_order add partition p6 values less than(60);
ALTER TABLE t_multi_parts_order add partition pmax values less than(maxvalue);
insert into t_multi_parts_order select generate_series(40,69);

begin;
delete from t_multi_parts_order partition(pmax, p5, p6, p1) limit 20;
select count(*) from t_multi_parts_order partition (p1);
select count(*) from t_multi_parts_order partition (p5);
select count(*) from t_multi_parts_order partition (p6);
select count(*) from t_multi_parts_order partition (pmax);
rollback;

explain delete from t_multi_parts_order partition(pmax, p5, p6, p1) limit 10;

drop table if exists t_multi_parts_order;

-- range subpartitions with local index, multi columns
create table t_multi_parts_order (
    col int,
    col1 int
)
partition by range(col) SUBPARTITION BY range(col1)
(
    partition p1 values less than(8)
    (
        SUBPARTITION subp_10_0 values less than(1),
        SUBPARTITION subp_10_1 values less than(2),
        SUBPARTITION subp_10_2 values less than(3),
        SUBPARTITION subp_10_3 values less than(maxvalue)
    ),
    partition p4 values less than(maxvalue)
    (
        SUBPARTITION subp_max_0 values less than(1),
        SUBPARTITION subp_max_1 values less than(2),
        SUBPARTITION subp_max_2 values less than(3),
        SUBPARTITION subp_max_3 values less than(maxvalue)
    )
);
create index tbl_idx_t_multi_parts_order0 on t_multi_parts_order(col) local;
create index tbl_idx_t_multi_parts_order1 on t_multi_parts_order(col1) local;
create index tbl_idx_t_multi_parts_order2 on t_multi_parts_order(col,col1) local;
create index tbl_idx_t_multi_parts_order3 on t_multi_parts_order(col1,col) local;
insert into t_multi_parts_order select generate_series(0,31), generate_series(0,31) % 4;

-- Local index can eliminate order by when equivalence conditions exist
explain delete from t_multi_parts_order where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(p1) as t where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order as t partition(p1) where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col=1 and col1=1  order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col=1 and col1=1  order by col desc,col1 desc  limit 10;

select count(*) from t_multi_parts_order subpartition(subp_10_1);
begin;
delete from t_multi_parts_order partition(subp_max_1, subp_10_3) where col=1 and col1=1  order by col,col1 limit 10;
delete from t_multi_parts_order partition(subp_max_1, p1) where col=1 and col1=1  order by col,col1 limit 10;
select count(*) from t_multi_parts_order subpartition(subp_10_1);
rollback;

set enable_bitmapscan=false;
-- Partkey can eliminate order by when equivalence conditions exist
explain delete from t_multi_parts_order where col=1 order by col limit 10;
explain delete from t_multi_parts_order partition(p1) as t where col=1 order by col limit 10;
explain delete from t_multi_parts_order as t partition(p1) where col=1 order by col limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) where col=1 order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) where col=1 order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col=1 order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col=1 order by col desc limit 10;
-- Subpartkey can eliminate order by when equivalence conditions exist
explain delete from t_multi_parts_order where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order partition(p1) as t where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order as t partition(p1) where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col1=1 order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) where col1=1 order by col1 desc limit 10;

-- Local index cannot eliminate order by without conditions
explain delete from t_multi_parts_order order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(p1) as t order by col,col1 limit 10;
explain delete from t_multi_parts_order as t partition(p1) order by col,col1 limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col,col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col desc,col1 desc limit 10;

explain delete from t_multi_parts_order order by col limit 10;
explain delete from t_multi_parts_order partition(p1) as t order by col limit 10;
explain delete from t_multi_parts_order as t partition(p1) order by col limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col desc limit 10;

explain delete from t_multi_parts_order order by col1 limit 10;
explain delete from t_multi_parts_order partition(p1) as t order by col1 limit 10;
explain delete from t_multi_parts_order as t partition(p1) order by col1 limit 10;
explain delete from t_multi_parts_order subpartition(subp_max_1) order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1) order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col1 limit 10;
explain delete from t_multi_parts_order partition(subp_max_1, p1) order by col1 desc limit 10;
set enable_bitmapscan=true;

begin;
delete from t_multi_parts_order partition(p4) order by col limit 6;
select count(*) from t_multi_parts_order partition (p4) as t;
rollback;
explain delete from t_multi_parts_order partition(p4) order by col limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_0) order by col1 limit 6;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_0) order by col1 limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_0, subp_max_3) order by col1 limit 6;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_0, subp_max_3) order by col1 limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_max_0) order by col1 limit 6;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_3, subp_max_0) order by col1 limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) order by col,col1 limit 6;
select count(*) from t_multi_parts_order subpartition (subp_10_1) as t;
select count(*) from t_multi_parts_order subpartition (subp_10_2) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) order by col,col1 limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) order by col1,col limit 6;
select count(*) from t_multi_parts_order subpartition (subp_10_1) as t;
select count(*) from t_multi_parts_order subpartition (subp_10_2) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) order by col1,col limit 6;

drop table if exists t_multi_parts_order;

-- range subpartitions without index, multi columns
create table t_multi_parts_order (
    col int,
    col1 int
)
partition by range(col) SUBPARTITION BY range(col1)
(
    partition p1 values less than(8)
    (
        SUBPARTITION subp_10_0 values less than(1),
        SUBPARTITION subp_10_1 values less than(2),
        SUBPARTITION subp_10_2 values less than(3),
        SUBPARTITION subp_10_3 values less than(maxvalue)
    ),
    partition p4 values less than(maxvalue)
    (
        SUBPARTITION subp_max_0 values less than(1),
        SUBPARTITION subp_max_1 values less than(2),
        SUBPARTITION subp_max_2 values less than(3),
        SUBPARTITION subp_max_3 values less than(maxvalue)
    )
);

insert into t_multi_parts_order select generate_series(0,31), generate_series(0,31) % 4;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;

begin;
delete from t_multi_parts_order partition(subp_max_0, subp_max_3) limit 6;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_max_0) limit 6;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;

explain delete from t_multi_parts_order partition(subp_max_3, subp_max_0) limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) limit 6;
select count(*) from t_multi_parts_order subpartition (subp_10_1) as t;
select count(*) from t_multi_parts_order subpartition (subp_10_2) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_3, subp_max_0, subp_10_1, subp_10_2) limit 6;

begin;
delete from t_multi_parts_order partition(subp_max_3, subp_10_1, p4) limit 6;
select count(*) from t_multi_parts_order subpartition (subp_10_1) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_0) as t;
select count(*) from t_multi_parts_order subpartition (subp_max_3) as t;
rollback;
explain delete from t_multi_parts_order partition(subp_max_3, subp_10_1, p4) limit 6;

drop table if exists t_multi_parts_order;

\c regression
drop database test_part_delete_B_db;