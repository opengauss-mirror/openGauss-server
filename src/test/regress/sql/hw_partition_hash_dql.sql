
--
---- test partition for (null)
--

-- 1. test ordinary
	-- 1.1 range partitioned table
	-- 1.2 interval partitioned table
-- 2. test data column of partition key value
	-- 2.1 text
	-- 2.2 timestamp
-- 3. MAXVALUE
	-- 3.1 MAXVALUE is first column
	-- 3.2 MAXVALUE is second column

CREATE schema FVT_COMPRESS_QWER;
set search_path to FVT_COMPRESS_QWER;


-- 1. test ordinary
---- 1.1 range partitioned table
create table test_partition_for_null_hash (a int, b int, c int, d int) 
partition by hash (a) 
(
	partition test_partition_for_null_hash_p1,
	partition test_partition_for_null_hash_p2,
	partition test_partition_for_null_hash_p3
);

insert into test_partition_for_null_hash values (0, 0, 0, 0);
insert into test_partition_for_null_hash values (1, 1, 1, 1);
insert into test_partition_for_null_hash values (5, 5, 5, 5);

-- success
insert into test_partition_for_null_hash values (null, null, null, null);
-- success
insert into test_partition_for_null_hash values (0, null, null, null);


-- success
select * from test_partition_for_null_hash partition for (null) order by 1, 2, 3, 4;
-- success
select * from test_partition_for_null_hash partition for (0) order by 1, 2, 3, 4;


-- success
alter table test_partition_for_null_hash rename partition for (null) to test_partition_for_null_hash_part1;
-- failed, same partition name test_partition_for_null_hash_part1 is already exists
alter table test_partition_for_null_hash rename partition for (0) to test_partition_for_null_hash_part1;
-- success
select * from test_partition_for_null_hash partition (test_partition_for_null_hash_part1) order by 1, 2, 3, 4;

alter table test_partition_for_null_hash drop partition for (NULL);
alter table test_partition_for_null_hash drop partition for (0);

CREATE TABLE select_hash_partition_table_000_3(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(20),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(20),
 C_INT INTEGER,
 C_BIGINT BIGINT,
 C_SMALLINT SMALLINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(10,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
 partition by hash (C_INT)
( 
     partition select_hash_partition_000_3_1,
     partition select_hash_partition_000_3_2
);

create index select_list_partition_table_index_000_3 ON select_hash_partition_table_000_3(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) local(partition select_list_partition_000_3_1, partition select_list_partition_000_3_3);
create view select_list_partition_table_view_000_3 as select * from select_hash_partition_table_000_3;

INSERT INTO select_hash_partition_table_000_3 VALUES('A','ABC','ABCDEFG','a','abc','abcdefg',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO select_hash_partition_table_000_3 VALUES('B','BCD','BCDEFGH','b','bcd','bcdefgh',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO select_hash_partition_table_000_3 VALUES('C','CDE','CDEFGHI','c','cde','cdefghi',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO select_hash_partition_table_000_3 VALUES('D','DEF','DEFGHIJ','d','def','defghij',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO select_hash_partition_table_000_3 VALUES('E','EFG','EFGHIJK','e','efg','efghijk',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO select_hash_partition_table_000_3 VALUES('F','FGH','FGHIJKL','f','fgh','fghijkl',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO select_hash_partition_table_000_3 VALUES('G','GHI','GHIJKLM','g','ghi','ghijklm',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO select_hash_partition_table_000_3 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_hash_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_hash_partition_table_000_3 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_hash_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_hash_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',1100,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_hash_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',1600,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');

select * from select_hash_partition_table_000_3 partition for (NULL) order by C_INT;

alter table select_hash_partition_table_000_3 rename partition for (NULL) to select_hash_partition_table_000_3_p1;

alter table select_hash_partition_table_000_3 drop partition for (NULL);


CREATE TABLE partition_wise_join_table_001_1 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision,RANK SMALLINT) 
partition  by  hash(ID) 
( 
	partition partition_wise_join_table_001_1_1,
	partition partition_wise_join_table_001_1_2
) ;

INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(1,9),'PARTITION WIASE JOIN 1-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,10000,13 );
INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(41,49),'PARTITION WIASE JOIN 1-3-' || generate_series(40,60),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,15000,15 );

create index idx_partition_wise_join_table_001_1_1 on partition_wise_join_table_001_1(ID) LOCAL;
create index idx_partition_wise_join_table_001_1_2 on partition_wise_join_table_001_1(ID,NAME) LOCAL;
create index idx_partition_wise_join_table_001_1_3 on partition_wise_join_table_001_1(RANK) LOCAL;
create index idx_partition_wise_join_table_001_1_4 on partition_wise_join_table_001_1(RANK,SALARY,NAME) LOCAL;

CREATE TABLE partition_wise_join_table_001_2 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision ) 
partition by hash(ID)
( 
	partition partition_wise_join_table_001_1_1, 
	partition partition_wise_join_table_001_1_2
);

INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(1,9),'PARTITION WIASE JOIN 2-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No 66# Science 4 Street  of Xi'an  of China $$,10000);
INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(71,79),'PARTITION WIASE JOIN 2-3-' || generate_series(70,80),90 + random() * 10,'1990-8-8',$$No 77# Science 4 Street  of Xi'an  of China $$,15000);

CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_1 ON PARTITION_WISE_JOIN_TABLE_001_2(ID) LOCAL;
CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_2 ON PARTITION_WISE_JOIN_TABLE_001_2(ID,NAME) LOCAL;
CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_3 ON PARTITION_WISE_JOIN_TABLE_001_2(SALARY,NAME) LOCAL;

SELECT A.ID,B.ID,A.RANK,B.SALARY,A.SALARY,A.ADDRESS,B.BIRTHDAY FROM PARTITION_WISE_JOIN_TABLE_001_1 A,PARTITION_WISE_JOIN_TABLE_001_2 B WHERE A.ID = B.ID AND  A.ID < 100 OR A.ID >400 order by 1, 2;

ANALYZE PARTITION_WISE_JOIN_TABLE_001_1;
ANALYZE PARTITION_WISE_JOIN_TABLE_001_2;

SELECT A.ID,B.ID,A.RANK,B.SALARY,A.SALARY,A.ADDRESS,B.BIRTHDAY FROM PARTITION_WISE_JOIN_TABLE_001_1 A,PARTITION_WISE_JOIN_TABLE_001_2 B WHERE A.ID = B.ID AND  A.ID < 100 OR A.ID >400 order by 1, 2;

CREATE TABLE HW_PARTITION_SELECT_RT (A INT, B INT)
PARTITION BY hash(A)
(
	PARTITION HW_PARTITION_SELECT_RT_P1,
	PARTITION HW_PARTITION_SELECT_RT_P2,
	PARTITION HW_PARTITION_SELECT_RT_P3
);
EXPLAIN (COSTS OFF) SELECT B FROM (SELECT B FROM HW_PARTITION_SELECT_RT LIMIT 100) ORDER BY B;

CREATE TABLE TESTTABLE_TEST1(A INT) PARTITION BY HASH (A)(PARTITION TESTTABLE_TEST1_P1);
CREATE TABLE TESTTABLE_TEST2(A INT);
SELECT * FROM TESTTABLE_TEST1 UNION ALL SELECT * FROM TESTTABLE_TEST2 order by 1;

CREATE TABLE select_partition_table_000_3(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(102400),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	partition by hash (C_INT)
( 
     partition select_partition_000_3_1,
     partition select_partition_000_3_2
);

create index select_partition_table_index_000_3 ON select_partition_table_000_3(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) local(partition select_partition_000_3_1, partition select_partition_000_3_3);
create view select_partition_table_view_000_3 as select * from select_partition_table_000_3;

INSERT INTO select_partition_table_000_3 VALUES('A','ABC','ABCDEFG','a','abc','abcdefg',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO select_partition_table_000_3 VALUES('B','BCD','BCDEFGH','b','bcd','bcdefgh',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO select_partition_table_000_3 VALUES('C','CDE','CDEFGHI','c','cde','cdefghi',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO select_partition_table_000_3 VALUES('D','DEF','DEFGHIJ','d','def','defghij',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO select_partition_table_000_3 VALUES('E','EFG','EFGHIJK','e','efg','efghijk',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO select_partition_table_000_3 VALUES('F','FGH','FGHIJKL','f','fgh','fghijkl',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO select_partition_table_000_3 VALUES('G','GHI','GHIJKLM','g','ghi','ghijklm',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO select_partition_table_000_3 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_3 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',1100,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_3 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',1600,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');

explain (costs off, verbose on) select lower(C_CHAR_3), initcap(C_VARCHAR_3), sqrt(C_INT), C_NUMERIC- 1 + 2*6/3, rank() over w from select_partition_table_000_3 where C_INT > 600 or C_BIGINT < 444444 window w as (partition by C_TS_WITHOUT) order by 1,2,3,4,5;

select lower(C_CHAR_3), initcap(C_VARCHAR_3), sqrt(C_INT), C_NUMERIC- 1 + 2*6/3, rank() over w from select_partition_table_000_3 where C_INT > 600 or C_BIGINT < 444444 window w as (partition by C_TS_WITHOUT) order by 1,2,3,4,5;

create table hw_partition_select_rt5 (a int, b int, c int)
partition by hash(c)
(
partition hw_partition_select_rt5_p1
);

alter table hw_partition_select_rt5 drop column b;

update hw_partition_select_rt5 set c=0 where c=-1;

drop schema FVT_COMPRESS_QWER cascade;

--begin: these test are related to explain output change about partition table.
--      major change is as below
	--1.
	--Selected Partitions:  1  2  6  7  8  9
	--                          \|/
	--Selected Partitions:  1..2,6..9
	--2.
	--Selected Partitions:  1  3  5  7  9
	--                          \|/
	--Selected Partitions:  1,3,5,7,9
CREATE schema FVT_COMPRESS;
set search_path to FVT_COMPRESS;


create table test_explain_format_on_part_table (id int) 
partition by hash(id) 
(
partition p1,
partition p2,
partition p3,
partition p4,
partition p5,
partition p6,
partition p7,
partition p8,
partition p9
);
-- two continous segments, text formast
explain (verbose on, costs off) 
	select * from test_explain_format_on_part_table where id <15 or id >51;
-- no continous segment, text formast
explain (verbose on, costs off) 
	select * from test_explain_format_on_part_table where id =5 or id =25 or id=45 or id = 65 or id = 85;
-- two continous segments, non-text formast
explain (verbose on, costs off, FORMAT JSON) 
	select * from test_explain_format_on_part_table where id <15 or id >51;
-- no continous segment, non-text formast
explain (verbose on, costs off, FORMAT JSON) 
	select * from test_explain_format_on_part_table where id =5 or id =25 or id=45 or id = 65 or id = 85;

drop table test_explain_format_on_part_table;
--end: these test are related to explain output change about partition table.

create table hw_partition_select_parttable (
   c1 int,
   c2 int,
   c3 text)
partition by hash(c1)
(partition hw_partition_select_parttable_p1,
 partition hw_partition_select_parttable_p2,
 partition hw_partition_select_parttable_p3);
 
 insert into hw_partition_select_parttable values (10,40,'abc');
 insert into hw_partition_select_parttable(c1,c2) values (100,20);
 insert into hw_partition_select_parttable values(300,200);
 
select * from hw_partition_select_parttable order by 1, 2, 3;

select c1 from hw_partition_select_parttable order by 1;

select c1,c2 from hw_partition_select_parttable order by 1, 2;

select c2 from hw_partition_select_parttable order by 1;

select c1,c2,c3 from hw_partition_select_parttable order by 1, 2, 3;

select c1 from hw_partition_select_parttable where c1>50 and c1<300 order by 1;

select * from hw_partition_select_parttable where c2>100 order by 1, 2, 3;

create table t_select_datatype_int32(c1 int,c2 int,c3 int,c4 text)
partition by hash(c1)
(partition t_select_datatype_int32_p1,
 partition t_select_datatype_int32_p2,
 partition t_select_datatype_int32_p3,
 partition t_select_datatype_int32_p4);
 
insert into t_select_datatype_int32 values(-100,20,20,'a'), (100,300,300,'bb'), (150,75,500,NULL), (200,500,50,'ccc'), (250,50,50,NULL), (300,700,125,''), (450,35,150,'dddd');

--partition select for int32
--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1=50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1=250 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1=500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1=550 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<=50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<150 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<=200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<=500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<700 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<=700 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>150 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=150 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 AND t_select_datatype_int32.c1<250 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 AND t_select_datatype_int32.c1>0 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 AND t_select_datatype_int32.c1>100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>50 AND t_select_datatype_int32.c1>=150 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>100 AND t_select_datatype_int32.c1>=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 AND t_select_datatype_int32.c1=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 AND t_select_datatype_int32.c1<300 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 AND t_select_datatype_int32.c1<550 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>100 AND t_select_datatype_int32.c1<=500 AND t_select_datatype_int32.c1>=100 AND t_select_datatype_int32.c1<500 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>250 AND t_select_datatype_int32.c1<50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>50 AND t_select_datatype_int32.c1>100 AND t_select_datatype_int32.c1>=100 AND t_select_datatype_int32.c1<250 AND t_select_datatype_int32.c1<=250 AND t_select_datatype_int32.c1=200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 OR t_select_datatype_int32.c1<250 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 OR t_select_datatype_int32.c1>0 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 OR t_select_datatype_int32.c1>100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>50 OR t_select_datatype_int32.c1>=150 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>100 OR t_select_datatype_int32.c1>=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 OR t_select_datatype_int32.c1=100 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=100 OR t_select_datatype_int32.c1<200 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>100 OR t_select_datatype_int32.c1<=300 OR t_select_datatype_int32.c1>=100 OR t_select_datatype_int32.c1<300 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>250 OR t_select_datatype_int32.c1<50 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<170  AND ( t_select_datatype_int32.c1>600 OR t_select_datatype_int32.c1<150) order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where (t_select_datatype_int32.c1<170 OR t_select_datatype_int32.c1<250)  AND ( t_select_datatype_int32.c1>600 OR t_select_datatype_int32.c1<150) order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1<50 OR t_select_datatype_int32.c1>250 AND t_select_datatype_int32.c1<400 order by 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=-100 AND t_select_datatype_int32.c1<50 OR t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<700 order by 1, 2, 3, 4; 

--success
select * from t_select_datatype_int32 where t_select_datatype_int32.c1>=-100 AND t_select_datatype_int32.c1<=100 OR t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<700 order by 1, 2, 3, 4; 

--IS NULL
--success
select * from t_select_datatype_int32 where 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<t_select_datatype_int32.c2) AND 
	(t_select_datatype_int32.c2<t_select_datatype_int32.c3 OR t_select_datatype_int32.c2>100) OR 
	t_select_datatype_int32.c4 IS NULL
	ORDER BY 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<t_select_datatype_int32.c2) AND 
	(t_select_datatype_int32.c2<t_select_datatype_int32.c3 OR t_select_datatype_int32.c2>100) AND 
	t_select_datatype_int32.c4 IS NULL
	ORDER BY 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where 
	t_select_datatype_int32.c4 IS NULL AND 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<t_select_datatype_int32.c2) AND 
	(t_select_datatype_int32.c2<t_select_datatype_int32.c3 OR t_select_datatype_int32.c2>100)
	ORDER BY 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where 
	t_select_datatype_int32.c4 IS NULL OR 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<t_select_datatype_int32.c2) AND 
	(t_select_datatype_int32.c2<t_select_datatype_int32.c3 OR t_select_datatype_int32.c2>100)
	ORDER BY 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c4 IS NULL) AND 
	(t_select_datatype_int32.c2<t_select_datatype_int32.c3 OR t_select_datatype_int32.c2>100)
	ORDER BY 1, 2, 3, 4;

--success
select * from t_select_datatype_int32 where 
	(t_select_datatype_int32.c1>500 OR t_select_datatype_int32.c1<250) AND 
	(t_select_datatype_int32.c1>300 AND t_select_datatype_int32.c1<t_select_datatype_int32.c2) AND 
	(t_select_datatype_int32.c4 IS NULL OR t_select_datatype_int32.c2>100)
	ORDER BY 1, 2, 3, 4;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- check select contarins partition

--
---- check select from range partition
--

create table hw_partition_select_ordinary_table (a int, b int);

create table test_select_hash_partition (a int, b int) 
partition by hash(a) 
(
	partition test_select_hash_partition_p1, 
	partition test_select_hash_partition_p2,
	partition test_select_hash_partition_p3
);

insert into test_select_hash_partition values(2);

--success
select * from test_select_hash_partition partition (test_select_hash_partition_p1) order by 1, 2;

--success
select * from test_select_hash_partition partition (test_select_hash_partition_p2) order by 1, 2;

--success
select * from test_select_hash_partition partition (test_select_hash_partition_p3) order by 1, 2;

--success
select * from test_select_hash_partition partition (test_select_hash_partition_p4) order by 1, 2;

--success
select a from test_select_hash_partition partition (test_select_hash_partition_p2) order by 1;

--success
select a from test_select_hash_partition partition for (0) order by 1;

--success
select a from test_select_hash_partition partition for (1) order by 1;

--success
select a from test_select_hash_partition partition for (2) order by 1;

--success
select a from test_select_hash_partition partition for (5) order by 1;

--success
select a from test_select_hash_partition partition for (8) order by 1;

-- fail: table is not partitioned table
select a from hw_partition_select_ordinary_table partition (test_select_hash_partition_p2);

-- fail: table is not partitioned table
select a from hw_partition_select_ordinary_table partition for (2);

--
--
CREATE TABLE hw_partition_select_test(C_INT INTEGER)
 partition by hash (C_INT)
( 
     partition hw_partition_select_test_part_1,
     partition hw_partition_select_test_part_2,
     partition hw_partition_select_test_part_3
);
insert  into hw_partition_select_test values(111);
insert  into hw_partition_select_test values(555);
insert  into hw_partition_select_test values(888);

select a.*  from hw_partition_select_test partition(hw_partition_select_test_part_1) a;

create table  hash_partitioned_table (a int)
partition by hash(a)
(
	partition hash_partitioned_table_p1,
	partition hash_partitioned_table_p2,
	partition hash_partitioned_table_p3
);

insert into hash_partitioned_table values (1);
insert into hash_partitioned_table values (2);
insert into hash_partitioned_table values (5);
insert into hash_partitioned_table values (6);

with tmp1 as (select a from hash_partitioned_table partition for (2)) select a from tmp1 order by 1;

drop schema FVT_COMPRESS cascade;





