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
