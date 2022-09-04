CREATE schema FVT_COMPRESS_QWER;
set search_path to FVT_COMPRESS_QWER;
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
insert into test_partition_for_null_hash select * from test_partition_for_null_hash;
select * from test_partition_for_null_hash order by a;
-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_hash values (null, null, null, null);
-- success
insert into test_partition_for_null_hash values (0, null, null, null);

CREATE TABLE select_hash_partition_table_000_3(
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
select count(*) from select_hash_partition_table_000_3;

CREATE TABLE partition_wise_join_table_001_1 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision,RANK SMALLINT) 
partition  by  hash(ID) 
( 
	partition partition_wise_join_table_001_1_1,
	partition partition_wise_join_table_001_1_2
) ;

INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(1,9),'PARTITION WIASE JOIN 1-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,10000,13 );
INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(41,49),'PARTITION WIASE JOIN 1-3-' || generate_series(40,60),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,15000,15 );
select count(*) from partition_wise_join_table_001_1;

CREATE TABLE partition_wise_join_table_001_2 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision ) 
partition by hash(ID)
( 
	partition partition_wise_join_table_001_1_1, 
	partition partition_wise_join_table_001_1_2
);

INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(1,9),'PARTITION WIASE JOIN 2-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No 66# Science 4 Street  of Xi'an  of China $$,10000);
INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(71,79),'PARTITION WIASE JOIN 2-3-' || generate_series(70,80),90 + random() * 10,'1990-8-8',$$No 77# Science 4 Street  of Xi'an  of China $$,15000);
select count(*) from partition_wise_join_table_001_2;

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
select count(*) from select_partition_table_000_3;

create table test_select_hash_partition (a int, b int) 
partition by hash(a) 
(
	partition test_select_hash_partition_p1, 
	partition test_select_hash_partition_p2,
	partition test_select_hash_partition_p3
);

insert into test_select_hash_partition values(1,1);
insert into test_select_hash_partition values(2,2);
insert into test_select_hash_partition values(0,0);
insert into test_select_hash_partition values(3,3);
insert into test_select_hash_partition values(7,5);
insert into test_select_hash_partition values(7,6);
select * from test_select_hash_partition order by a;

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
insert  into hw_partition_select_test values(100);
select count(*) from hw_partition_select_test;
drop schema FVT_COMPRESS_QWER cascade;