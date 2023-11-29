create table pt_tbl_test(id integer, name varchar(20), gender boolean);
insert into pt_tbl_test (id, name, gender) values (1, 'zhangsan', true);
insert into pt_tbl_test (id, name, gender) values (2, 'lisi', false);
insert into pt_tbl_test (id, name, gender) values (3, 'wangwu', true);
select * from pt_tbl_test where id = 1;
delete from pt_tbl_test where id = 1;
select * from pt_tbl_test where id = 1;
drop table pt_tbl_test;

CREATE TABLE fq_tbl_test
(
 gid INTEGER,
 gvalue INTEGER,
 grange DECIMAL(7,2)
)
PARTITION BY RANGE (gvalue)
(
 PARTITION P1 VALUES LESS THAN(10),
 PARTITION P2 VALUES LESS THAN(20),
 PARTITION P3 VALUES LESS THAN(30),
 PARTITION P4 VALUES LESS THAN(40),
 PARTITION P5 VALUES LESS THAN(50),
 PARTITION P6 VALUES LESS THAN(60),
 PARTITION P7 VALUES LESS THAN(70),
 PARTITION P8 VALUES LESS THAN(MAXVALUE)
);

insert into fq_tbl_test select v, v, v/3.1415926 from generate_series(1, 100) as v;
select * from fq_tbl_test where gid = 1;
delete from fq_tbl_test where gid = 1;
select * from fq_tbl_test where gid = 1;
drop table fq_tbl_test;

create table up_pt_tbl_test(id integer, name varchar(20), gender boolean);

insert into up_pt_tbl_test (id, name, gender) values (1, 'zhangsan', true);
insert into up_pt_tbl_test (id, name, gender) values (2, 'lisi', false);
insert into up_pt_tbl_test (id, name, gender) values (3, 'wangwu', true);

select * from up_pt_tbl_test where id = 1;
update up_pt_tbl_test set name = 'likui' where id =  1;
select * from up_pt_tbl_test where id = 1;

update up_pt_tbl_test set id = 22 where name = 'lisi';
drop table up_pt_tbl_test;



CREATE TABLE up_fq_tbl_test
(
 gid INTEGER,
 gvalue INTEGER,
 grange DECIMAL(7,2)
)
PARTITION BY RANGE (gvalue)
(
 PARTITION P1 VALUES LESS THAN(10),
 PARTITION P2 VALUES LESS THAN(20),
 PARTITION P3 VALUES LESS THAN(30),
 PARTITION P4 VALUES LESS THAN(40),
 PARTITION P5 VALUES LESS THAN(50),
 PARTITION P6 VALUES LESS THAN(60),
 PARTITION P7 VALUES LESS THAN(70),
 PARTITION P8 VALUES LESS THAN(MAXVALUE)
);

insert into up_fq_tbl_test select v, v, v/3.1415926 from generate_series(1, 100) as v;

select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);

--modify partition key
update up_fq_tbl_test set gvalue = 555 where gid = 5;

select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);

--modify non partition key
update up_fq_tbl_test set grange = 10000 where gid = 5;
select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);
drop table up_fq_tbl_test;

----the partition relation enable row movement
CREATE TABLE up_fq_tbl_test
(
 gid INTEGER,
 gvalue INTEGER,
 grange DECIMAL(7,2)
)
PARTITION BY RANGE (gvalue)
(
 PARTITION P1 VALUES LESS THAN(10),
 PARTITION P2 VALUES LESS THAN(20),
 PARTITION P3 VALUES LESS THAN(30),
 PARTITION P4 VALUES LESS THAN(40),
 PARTITION P5 VALUES LESS THAN(50),
 PARTITION P6 VALUES LESS THAN(60),
 PARTITION P7 VALUES LESS THAN(70),
 PARTITION P8 VALUES LESS THAN(MAXVALUE)
)enable row movement;

insert into up_fq_tbl_test select v, v, v/3.1415926 from generate_series(1, 100) as v;

select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);

--modify partition key
update up_fq_tbl_test set gvalue = 555 where gid = 5 ;

select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);

--modify non partition key
update up_fq_tbl_test set grange = 10000 where gid = 5;
select count(*) from up_fq_tbl_test partition(P1);
select count(*) from up_fq_tbl_test partition(P8);
drop table up_fq_tbl_test;

--Add some testcase to test update the index column twice or more times and query results with index scan
--check the result is the latest one or not
---heap relation
create table twice_up_tbl_test(id integer, name varchar(20), gender boolean);
create index twice_up_tbl_test_idx on twice_up_tbl_test(name);

insert into twice_up_tbl_test (id, name, gender) values (1, 'zhangsan', true);
insert into twice_up_tbl_test (id, name, gender) values (2, 'lisi', false);
insert into twice_up_tbl_test (id, name, gender) values (3, 'wangwu', true);

select * from twice_up_tbl_test where name = 'zhangsan';
update twice_up_tbl_test set name = 'likui' where name = 'zhangsan';
select * from twice_up_tbl_test where name = 'zhangsan';
select * from twice_up_tbl_test where name = 'likui';

update twice_up_tbl_test set name = 'xxx' where name = 'likui';
select * from twice_up_tbl_test where name = 'likui';
select * from twice_up_tbl_test where name = 'xxx';

update twice_up_tbl_test set name = 'ppp' where name = 'xxx';
select * from twice_up_tbl_test where name = 'xxx';
select * from twice_up_tbl_test where name = 'ppp';
update twice_up_tbl_test set name = 'qqq' where name = 'ppp';
select * from twice_up_tbl_test order by id;

drop table twice_up_tbl_test;

----range partiton relation
CREATE TABLE twice_up_goo
(
 gid INTEGER,
 gvalue INTEGER,
 grange DECIMAL(7,2)
)
PARTITION BY RANGE (gvalue)
(
 PARTITION P1 VALUES LESS THAN(10),
 PARTITION P2 VALUES LESS THAN(20),
 PARTITION P3 VALUES LESS THAN(30),
 PARTITION P4 VALUES LESS THAN(40),
 PARTITION P5 VALUES LESS THAN(50),
 PARTITION P6 VALUES LESS THAN(60),
 PARTITION P7 VALUES LESS THAN(70),
 PARTITION P8 VALUES LESS THAN(MAXVALUE)
)enable row movement;
create index twice_up_tbl_test_idx on twice_up_goo(gvalue) LOCAL;
insert into twice_up_goo select v, v, v/3.1415926 from generate_series(1, 100) as v;

select * from twice_up_goo where gvalue = 1;
update twice_up_goo set gvalue = 12345 where gvalue = 1;
select * from twice_up_goo where gvalue = 1;
select * from twice_up_goo where gvalue = 12345;
update twice_up_goo set gvalue = 23456 where gvalue = 12345;
select * from twice_up_goo where gvalue = 12345;
select * from twice_up_goo where gvalue = 23456;

drop table twice_up_goo;

---core problem
CREATE TYPE heapmood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE heap_array_tab 
(a int, 
 b heapmood[],
 c int[]
 )
PARTITION BY RANGE (a) 
(
 partition p1 VALUES less than(100),
 partition p2 VALUES less than(maxvalue)
 ) enable row movement;

INSERT INTO heap_array_tab(a,b[0],c[0]) VALUES(1,'happy',1);
INSERT INTO heap_array_tab(a,b[1], c[1]) VALUES(2,'sad',2);
ANALYZE heap_array_tab;
SELECT * FROM heap_array_tab ORDER BY 1,2,3;
UPDATE heap_array_tab set b[1]='ok' where b[1]='sad';
SELECT * FROM heap_array_tab ORDER BY 1,2,3;
drop table heap_array_tab;
drop type heapmood;

--heap table
CREATE TYPE heapmood1 AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE heap_array_tab1 
(a int, 
 b heapmood1[],
 c int[]
 );

INSERT INTO heap_array_tab1(a,b[0],c[0]) VALUES(1,'happy',1);
INSERT INTO heap_array_tab1(a,b[1], c[1]) VALUES(2,'sad',2);
ANALYZE heap_array_tab1;
SELECT * FROM heap_array_tab1 ORDER BY 1,2,3;
UPDATE heap_array_tab1 set b[1]='ok' where b[1]='sad';
SELECT * FROM heap_array_tab1 ORDER BY 1,2,3;
drop table heap_array_tab1;
drop type heapmood1;

--test the index column is the order by column
----range partition
CREATE TABLE heap_range_order_by_tbl (
  GID INTEGER, 
  GVALUE INTEGER, 
  GAAA INTEGER,
  GDDD VARCHAR(20),
  GRANGE DECIMAL(7, 2)
) PARTITION BY RANGE (GVALUE) (
  PARTITION P1 VALUES LESS THAN(10), 
  PARTITION P2 VALUES LESS THAN(20), 
  PARTITION P3 VALUES LESS THAN(30), 
  PARTITION P4 VALUES LESS THAN(40), 
  PARTITION P5 VALUES LESS THAN(50), 
  PARTITION P6 VALUES LESS THAN(60), 
  PARTITION P7 VALUES LESS THAN(70), 
  PARTITION P8 VALUES LESS THAN(MAXVALUE)
);
CREATE INDEX idx_heap_range_order_by_tbl ON heap_range_order_by_tbl (GID) LOCAL;
INSERT INTO heap_range_order_by_tbl SELECT V, V, V,'P00421579', V FROM GENERATE_SERIES(1, 100) AS V;

----heap relation
CREATE TABLE heap_order_by_tbl(ID INT, VAL VARCHAR(200));

CREATE INDEX idx_heap_order_by_tbl ON heap_order_by_tbl (ID);
INSERT INTO heap_order_by_tbl SELECT V, 'CHAR-'||V FROM GENERATE_SERIES(1, 100) AS V;

set enable_seqscan=off;
set enable_indexscan = on;
set enable_hashjoin=off;
set enable_bitmapscan = off;

explain select * from heap_range_order_by_tbl A INNER JOIN heap_range_order_by_tbl B ON (A.GID = B.GID) ORDER BY A.GID;

explain select * from heap_range_order_by_tbl A INNER JOIN heap_range_order_by_tbl B ON (A.GID = B.GID) ORDER BY A.GID DESC;

explain select * from heap_order_by_tbl A INNER JOIN heap_order_by_tbl B ON (A.ID = B.ID) ORDER BY A.ID;

explain select * from heap_order_by_tbl A INNER JOIN heap_order_by_tbl B ON (A.ID = B.ID) ORDER BY A.ID DESC;

select * from heap_order_by_tbl order by ID limit 10;

select * from heap_range_order_by_tbl order by GID limit 20;

drop table heap_range_order_by_tbl;
drop table heap_order_by_tbl;

--test merge join
CREATE TABLE partition_heap_test_t1 (A INT4, B INT)
PARTITION BY RANGE (A)
(
	PARTITION p1_partition_heap_test_t1 VALUES LESS THAN (10),
	PARTITION p2_partition_heap_test_t1 VALUES LESS THAN (20),
	PARTITION p3_partition_heap_test_t1 VALUES LESS THAN (100),
	PARTITION p4_partition_heap_test_t1 VALUES LESS THAN (110)
);

CREATE TABLE partition_heap_test_t2 (A INT4, B INT)
PARTITION BY RANGE (A)
(
	PARTITION p1_partition_heap_test_t2 VALUES LESS THAN (10),
	PARTITION p2_partition_heap_test_t2 VALUES LESS THAN (20),
	PARTITION p3_partition_heap_test_t2 VALUES LESS THAN (100),
	PARTITION p4_partition_heap_test_t2 VALUES LESS THAN (110)
);

CREATE INDEX INDEX_ON_TEST_T1 ON partition_heap_test_t1 (A) LOCAL;

CREATE INDEX INDEX_ON_TEST_T2 ON partition_heap_test_t2 (A) LOCAL;

CREATE INDEX INDEX_ON_TEST_T1_1 ON partition_heap_test_t1 (B) LOCAL;

CREATE INDEX INDEX_ON_TEST_T2_1 ON partition_heap_test_t2 (B) LOCAL;

INSERT INTO partition_heap_test_t1  select generate_series(1, 100),generate_series(1, 100);
INSERT INTO partition_heap_test_t2  select generate_series(1, 100),generate_series(1, 100);

SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

EXPLAIN (COSTS OFF) SELECT partition_heap_test_t1.*, partition_heap_test_t2.B FROM partition_heap_test_t1 INNER JOIN partition_heap_test_t2 ON (partition_heap_test_t1.A = partition_heap_test_t2.A) ORDER BY 1,2,3;
SELECT partition_heap_test_t1.*, partition_heap_test_t2.B FROM partition_heap_test_t1 INNER JOIN partition_heap_test_t2 ON (partition_heap_test_t1.A = partition_heap_test_t2.A) order by 1, 2, 3 limit 10;

drop table partition_heap_test_t1;
drop table partition_heap_test_t2;

-----test heap relation merge join 
CREATE TABLE partition_heap_test_t1 (A INT4, B INT);
CREATE TABLE partition_heap_test_t2 (A INT4, B INT);

CREATE INDEX INDEX_ON_TEST_T1 ON partition_heap_test_t1 (A);

CREATE INDEX INDEX_ON_TEST_T2 ON partition_heap_test_t2 (A);

CREATE INDEX INDEX_ON_TEST_T1_1 ON partition_heap_test_t1 (B);

CREATE INDEX INDEX_ON_TEST_T2_1 ON partition_heap_test_t2 (B);

INSERT INTO partition_heap_test_t1  select generate_series(1, 10),generate_series(1, 10);
INSERT INTO partition_heap_test_t2  select generate_series(1, 10),generate_series(1, 10);

SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

EXPLAIN (COSTS OFF) SELECT partition_heap_test_t1.*, partition_heap_test_t2.B FROM partition_heap_test_t1 INNER JOIN partition_heap_test_t2 ON (partition_heap_test_t1.A = partition_heap_test_t2.A) ORDER BY 1,2,3;
SELECT partition_heap_test_t1.*, partition_heap_test_t2.B FROM partition_heap_test_t1 INNER JOIN partition_heap_test_t2 ON (partition_heap_test_t1.A = partition_heap_test_t2.A) order by 1, 2, 3;

drop table partition_heap_test_t1;
drop table partition_heap_test_t2;
