
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
create table test_partition_for_null_range (a int, b int, c int, d int) 
partition by range (a, b) 
(
	partition test_partition_for_null_range_p1 values less than (1, 1),
	partition test_partition_for_null_range_p2 values less than (4, 4),
	partition test_partition_for_null_range_p3 values less than (7, 7)
);

insert into test_partition_for_null_range values (0, 0, 0, 0);
insert into test_partition_for_null_range values (1, 1, 1, 1);
insert into test_partition_for_null_range values (5, 5, 5, 5);

-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range values (null, null, null, null);
-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range values (null, 0, null, null);
-- success
insert into test_partition_for_null_range values (0, null, null, null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range partition for (null, null) order by 1, 2, 3, 4;
-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range partition for (null, 0) order by 1, 2, 3, 4;
-- success
select * from test_partition_for_null_range partition for (0, null) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range rename partition for (null, null) to test_partition_for_null_range_part1;
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range rename partition for (null, 0) to test_partition_for_null_range_part1;
-- success
alter table test_partition_for_null_range rename partition for (0, null) to test_partition_for_null_range_part1;
-- success
select * from test_partition_for_null_range partition (test_partition_for_null_range_part1) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range drop partition for (null, null);
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range drop partition for (null, 0);
-- success
alter table test_partition_for_null_range drop partition for (0, null);
-- failed
select * from test_partition_for_null_range partition (test_partition_for_null_range_part1) order by 1, 2, 3, 4;

---- 1.2 interval partitioned table
create table test_partition_for_null_interval (a int, b int) 
partition by range (a)
(
	partition test_partition_for_null_interval_p1 values less than (1),
	partition test_partition_for_null_interval_p2 values less than (4),
	partition test_partition_for_null_interval_p3 values less than (40)
);

insert into test_partition_for_null_interval values (0, 0);
insert into test_partition_for_null_interval values (1, 1);
insert into test_partition_for_null_interval values (5, 5);
insert into test_partition_for_null_interval values (7, 7);
insert into test_partition_for_null_interval values (20, 20);
insert into test_partition_for_null_interval values (30, 30);

-- failed: inserted partition key does not map to any partition
--         inserted partition key cannot be NULL for interval-partitioned table
insert into test_partition_for_null_interval values (null, null);
-- failed: inserted partition key does not map to any partition
--         inserted partition key cannot be NULL for interval-partitioned table
insert into test_partition_for_null_interval values (null, 0);
-- success
insert into test_partition_for_null_interval values (0, null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_interval partition for (null) order by 1, 2;

-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_interval rename partition for (null) to test_partition_for_null_interval_part1;

-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_interval drop partition for (null);

-- 2. test data column of partition key value
---- 2.1 text
create table test_partition_for_null_range_text (a text, b text, c text, d text) 
partition by range (a, b) 
(
	partition test_partition_for_null_range_text_p1 values less than ('B', 'B'),
	partition test_partition_for_null_range_text_p2 values less than ('E', 'E'),
	partition test_partition_for_null_range_text_p3 values less than ('H', 'H')
);

insert into test_partition_for_null_range_text values ('A', 'A', 'A', 'A');
insert into test_partition_for_null_range_text values ('B', 'B', 'B', 'B');
insert into test_partition_for_null_range_text values ('F', 'F', 'F', 'F');


-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_text values (null, null, null, null);
-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_text values (null, 'A', null, null);
-- success
insert into test_partition_for_null_range_text values ('A', null, null, null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_text partition for (null, null) order by 1, 2, 3, 4;
-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_text partition for (null, 'A') order by 1, 2, 3, 4;
-- success
select * from test_partition_for_null_range_text partition for ('A', null) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_text rename partition for (null, null) to test_partition_for_null_range_text_part1;
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_text rename partition for (null, 'A') to test_partition_for_null_range_text_part1;
-- success
alter table test_partition_for_null_range_text rename partition for ('A', null) to test_partition_for_null_range_text_part1;
-- success
select * from test_partition_for_null_range_text partition (test_partition_for_null_range_text_part1) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_text drop partition for (null, null);
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_text drop partition for (null, 'A');
-- success
alter table test_partition_for_null_range_text drop partition for ('A', null);
-- failed
select * from test_partition_for_null_range_text partition (test_partition_for_null_range_text_part1) order by 1, 2, 3, 4;

---- 2.2 timestamp
--a. range partitioned table
create table test_partition_for_null_range_timestamp
(
	a timestamp without time zone,
	b timestamp with time zone,
	c int,
	d int) 
partition by range (a, b) 
(
	partition test_partition_for_null_range_timestamp_p1 values less than ('2000-01-01 01:01:01', '2000-01-01 01:01:01+01'),
	partition test_partition_for_null_range_timestamp_p2 values less than ('2000-02-02 02:02:02', '2000-02-02 02:02:02+02'),
	partition test_partition_for_null_range_timestamp_p3 values less than ('2000-03-03 03:03:03', '2000-03-03 03:03:03+03')
);

insert into test_partition_for_null_range_timestamp values ('2000-01-01 00:00:00', '2000-01-01 00:00:00+00', 1, 1);
insert into test_partition_for_null_range_timestamp values ('2000-01-01 01:01:01', '2000-01-01 01:01:01+01', 2, 2);
insert into test_partition_for_null_range_timestamp values ('2000-02-03 02:02:02', '2000-02-03 02:02:02+02', 3, 3);


-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_timestamp values (null, null, null, null);
-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_timestamp values (null, '2000-01-01 01:01:01+01', null, null);
-- success
insert into test_partition_for_null_range_timestamp values ('2000-01-01 01:01:01', null, null, null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_timestamp partition for (null, null) order by 1, 2, 3, 4;
-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_timestamp partition for (null, '2000-01-01 01:01:01+01') order by 1, 2, 3, 4;
-- success
select * from test_partition_for_null_range_timestamp partition for ('2000-01-01 01:01:01', null) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_timestamp rename partition for (null, null) to test_partition_for_null_range_timestamp_part1;
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_timestamp rename partition for (null, '2000-01-01 01:01:01+01') to test_partition_for_null_range_timestamp_part1;
-- success
alter table test_partition_for_null_range_timestamp rename partition for ('2000-01-01 01:01:01', null) to test_partition_for_null_range_timestamp_part1;
-- success
select * from test_partition_for_null_range_timestamp partition (test_partition_for_null_range_timestamp_part1) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_timestamp drop partition for (null, null);
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_timestamp drop partition for (null, '2000-01-01 01:01:01+01');
-- success
alter table test_partition_for_null_range_timestamp drop partition for ('2000-01-01 01:01:01', null);
-- failed
select * from test_partition_for_null_range_timestamp partition (test_partition_for_null_range_timestamp_part1) order by 1, 2, 3, 4;

-- b. interval partitioned table
create table test_partition_for_null_interval_timestamp (a timestamp without time zone, b int) 
partition by range (a)
(
	partition test_partition_for_null_interval_timestamp_p1 values less than ('2000-01-01'),
	partition test_partition_for_null_interval_timestamp_p2 values less than ('2000-04-01'),
	partition test_partition_for_null_interval_timestamp_p3 values less than ('2001-07-01')
);

insert into test_partition_for_null_interval_timestamp values ('2000-01-01 00:00:00', 1);
insert into test_partition_for_null_interval_timestamp values ('2000-04-01 00:00:00', 2);
insert into test_partition_for_null_interval_timestamp values ('2000-05-01 00:00:00', 3);
insert into test_partition_for_null_interval_timestamp values ('2000-10-01 00:00:00', 3);


-- failed: inserted partition key does not map to any partition
--         inserted partition key cannot be NULL for interval-partitioned table
insert into test_partition_for_null_interval_timestamp values (null, null);
-- failed: inserted partition key does not map to any partition
--         inserted partition key cannot be NULL for interval-partitioned table
insert into test_partition_for_null_interval_timestamp values (null, 0);
-- success
insert into test_partition_for_null_interval_timestamp values ('2000-01-01 00:00:00', null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_interval_timestamp partition for (null) order by 1, 2;

-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_interval_timestamp rename partition for (null) to test_partition_for_null_interval_timestamp_part1;

-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_interval_timestamp drop partition for (null);

-- 3. MAXVALUE
-- 3.1 MAXVALUE is first column
create table test_partition_for_null_range_and_maxvalue_1 (a int, b int, c int, d int) 
partition by range (a, b) 
(
	partition test_partition_for_null_range_and_maxvalue_1_p1 values less than (1, 1),
	partition test_partition_for_null_range_and_maxvalue_1_p2 values less than (4, 4),
	partition test_partition_for_null_range_and_maxvalue_1_p3 values less than (7, 7), 
	partition test_partition_for_null_range_and_maxvalue_1_p4 values less than (maxvalue, 10)
);

insert into test_partition_for_null_range_and_maxvalue_1 values (0, 0, 0, 0);
insert into test_partition_for_null_range_and_maxvalue_1 values (1, 1, 1, 1);
insert into test_partition_for_null_range_and_maxvalue_1 values (5, 5, 5, 5);
insert into test_partition_for_null_range_and_maxvalue_1 values (8, 8, 8, 8);


-- success: insert into partition p4
insert into test_partition_for_null_range_and_maxvalue_1 values (null, null, null, null);
-- success: insert into partition p4
insert into test_partition_for_null_range_and_maxvalue_1 values (null, 0, null, null);
-- success: insert into partition p1
insert into test_partition_for_null_range_and_maxvalue_1 values (0, null, null, null);


-- success: select partition p4
select * from test_partition_for_null_range_and_maxvalue_1 partition for (null, null) order by 1, 2, 3, 4;
-- success: select partition p4
select * from test_partition_for_null_range_and_maxvalue_1 partition for (null, 0) order by 1, 2, 3, 4;
-- success: select partition p1
select * from test_partition_for_null_range_and_maxvalue_1 partition for (0, null) order by 1, 2, 3, 4;


-- success: rename partition p4
alter table test_partition_for_null_range_and_maxvalue_1 rename partition for (null, null) to test_partition_for_null_range_and_maxvalue_1_part4;
-- failed: partition "PART4" of relation "TEST_PARTITION_FOR_NULL_RANGE_AND_MAXVALUE_1" already exists
alter table test_partition_for_null_range_and_maxvalue_1 rename partition for (null, 0) to test_partition_for_null_range_and_maxvalue_1_p4;
-- success
alter table test_partition_for_null_range_and_maxvalue_1 rename partition for (0, null) to test_partition_for_null_range_and_maxvalue_1_part1;
-- success
select * from test_partition_for_null_range_and_maxvalue_1 partition (test_partition_for_null_range_and_maxvalue_1_part1) order by 1, 2, 3, 4;
select * from test_partition_for_null_range_and_maxvalue_1 partition (test_partition_for_null_range_and_maxvalue_1_p4) order by 1, 2, 3, 4;


-- success: drop partition p4
alter table test_partition_for_null_range_and_maxvalue_1 drop partition for (null, null);
-- failed: The partition number is invalid or out-of-range (partition p4 has dropped)
alter table test_partition_for_null_range_and_maxvalue_1 drop partition for (null, 0);
-- success: drop partition p1
alter table test_partition_for_null_range_and_maxvalue_1 drop partition for (0, null);
-- failed
select * from test_partition_for_null_range_and_maxvalue_1 partition (test_partition_for_null_range_and_maxvalue_1_part1) order by 1, 2, 3, 4;
select * from test_partition_for_null_range_and_maxvalue_1 partition (test_partition_for_null_range_and_maxvalue_1_part4) order by 1, 2, 3, 4;

-- 3.2 MAXVALUE is second column
create table test_partition_for_null_range_and_maxvalue_2 (a int, b int, c int, d int) 
partition by range (a, b) 
(
	partition test_partition_for_null_range_and_maxvalue_2_p1 values less than (1, 1),
	partition test_partition_for_null_range_and_maxvalue_2_p2 values less than (4, 4),
	partition test_partition_for_null_range_and_maxvalue_2_p3 values less than (7, 7), 
	partition test_partition_for_null_range_and_maxvalue_2_p4 values less than (10, maxvalue)
);

insert into test_partition_for_null_range_and_maxvalue_2 values (0, 0, 0, 0);
insert into test_partition_for_null_range_and_maxvalue_2 values (1, 1, 1, 1);
insert into test_partition_for_null_range_and_maxvalue_2 values (5, 5, 5, 5);
insert into test_partition_for_null_range_and_maxvalue_2 values (8, 8, 8, 8);


-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_and_maxvalue_2 values (null, null, null, null);
-- failed: inserted partition key does not map to any table partition
insert into test_partition_for_null_range_and_maxvalue_2 values (null, 0, null, null);
-- success: insert into partition p1
insert into test_partition_for_null_range_and_maxvalue_2 values (0, null, null, null);


-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_and_maxvalue_2 partition for (null, null) order by 1, 2, 3, 4;
-- failed: The partition number is invalid or out-of-range
select * from test_partition_for_null_range_and_maxvalue_2 partition for (null, 0) order by 1, 2, 3, 4;
-- success: select partition p1
select * from test_partition_for_null_range_and_maxvalue_2 partition for (0, null) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_and_maxvalue_2 rename partition for (null, null) to test_partition_for_null_range_and_maxvalue_2_part4;
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_and_maxvalue_2 rename partition for (null, 0) to test_partition_for_null_range_and_maxvalue_2_part4;
-- success
alter table test_partition_for_null_range_and_maxvalue_2 rename partition for (0, null) to test_partition_for_null_range_and_maxvalue_2_part1;
-- success
select * from test_partition_for_null_range_and_maxvalue_2 partition (test_partition_for_null_range_and_maxvalue_2_part1) order by 1, 2, 3, 4;


-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_and_maxvalue_2 drop partition for (null, null);
-- failed: The partition number is invalid or out-of-range
alter table test_partition_for_null_range_and_maxvalue_2 drop partition for (null, 0);
-- success: drop partition p1
alter table test_partition_for_null_range_and_maxvalue_2 drop partition for (0, null);
-- failed
select * from test_partition_for_null_range_and_maxvalue_2 partition (test_partition_for_null_range_and_maxvalue_2_part1) order by 1, 2, 3, 4;

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
 partition by range (C_INT)
( 
     partition select_partition_000_3_1 values less than (500),
     partition select_partition_000_3_3 values less than (2000)
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

select * from select_partition_table_000_3 partition for (NULL) order by C_INT;

alter table select_partition_table_000_3 rename partition for (NULL) to select_partition_table_000_3_p1;

alter table select_partition_table_000_3 drop partition for (NULL);

CREATE TABLE select_partition_table_000_4(
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
 partition by range (C_TS_WITHOUT)
( 
     partition select_partition_000_4_1 values less than ('2000-04-01'),
     partition select_partition_000_4_2 values less than ('2000-07-01'),
     partition select_partition_000_4_3 values less than ('2001-10-01')
);

create index select_partition_table_index_000_4 ON select_partition_table_000_4(C_CHAR_3,C_VARCHAR_3,C_INT) local(PARTITION select_partition_000_4_1, PARTITION select_partition_000_4_2, PARTITION select_partition_000_4_3);
create view select_partition_table_view_000_4 as select * from select_partition_table_000_4;

INSERT INTO select_partition_table_000_4 VALUES('A','ABC','ABCDEFG','a','abc','abcdefg',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO select_partition_table_000_4 VALUES('B','BCD','BCDEFGH','b','bcd','bcdefgh',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO select_partition_table_000_4 VALUES('C','CDE','CDEFGHI','c','cde','cdefghi',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO select_partition_table_000_4 VALUES('D','DEF','DEFGHIJ','d','def','defghij',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO select_partition_table_000_4 VALUES('E','EFG','EFGHIJK','e','efg','efghijk',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO select_partition_table_000_4 VALUES('F','FGH','FGHIJKL','f','fgh','fghijkl',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO select_partition_table_000_4 VALUES('G','GHI','GHIJKLM','g','ghi','ghijklm',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO select_partition_table_000_4 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_4 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_4 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_4 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_4 VALUES('J','IJK','IJKLMNO','j','jkl','jklmnop',1111,111111,111,11.1,11.11,11.111,'2000-09-09','2000-10-02 09:09:09','2000-10-02 09:09:09+09');
INSERT INTO select_partition_table_000_4 VALUES('K','JKl','IJKLMNO','j','jkl','jklmnop',1111,111111,111,11.1,11.11,11.111,'2000-09-09','2000-10-06 09:09:09','2000-10-06 09:09:09+09');

select C_INT,C_TS_WITHOUT from select_partition_table_000_4 partition for (null) order by 1, 2;

alter table select_partition_table_000_4 rename partition for (null) to select_partition_table_000_4_p1;

alter table select_partition_table_000_4 drop partition for (null);

CREATE TABLE partition_wise_join_table_001_1 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision,RANK SMALLINT) 
partition  by  range(ID) 
( 
	partition partition_wise_join_table_001_1_1  values less than (100), 
	partition partition_wise_join_table_001_1_2  values less than (1000) 
) ;

INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(1,10),'PARTITION WIASE JOIN 1-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,10000,13 );
INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(101,150),'PARTITION WIASE JOIN 1-2-' || generate_series(101,150),90 + random() * 10,'1989-8-8',$$No.99# Science 6 Street  of Xi'an  of China $$,12000 ,15);
INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(40,60),'PARTITION WIASE JOIN 1-3-' || generate_series(40,60),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,15000,15 );
INSERT INTO partition_wise_join_table_001_1 VALUES (generate_series(700,800),'PARTITION WIASE JOIN 1-4-' || generate_series(700,800),90 + random() * 10,'1990-8-8',$$No.88# Science 6 Street  of Xi'an  of China $$,18000,17 );

create index idx_partition_wise_join_table_001_1_1 on partition_wise_join_table_001_1(ID) LOCAL;
create index idx_partition_wise_join_table_001_1_2 on partition_wise_join_table_001_1(ID,NAME) LOCAL;
create index idx_partition_wise_join_table_001_1_3 on partition_wise_join_table_001_1(RANK) LOCAL;
create index idx_partition_wise_join_table_001_1_4 on partition_wise_join_table_001_1(RANK,SALARY,NAME) LOCAL;

CREATE TABLE partition_wise_join_table_001_2 (ID INT NOT NULL,NAME VARCHAR(50) NOT NULL,SCORE NUMERIC(4,1),BIRTHDAY TIMESTAMP WITHOUT TIME ZONE,ADDRESS TEXT,SALARY double precision ) 
partition by range(ID)
( 
	partition partition_wise_join_table_001_1_1  values less than (100), 
	partition partition_wise_join_table_001_1_2  values less than (300),
	partition partition_wise_join_table_001_1_3  values less than (1000)
);

INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(1,10),'PARTITION WIASE JOIN 2-1-' || generate_series(1,10),90 + random() * 10,'1990-8-8',$$No 66# Science 4 Street  of Xi'an  of China $$,10000);
INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(500,600),'PARTITION WIASE JOIN 2-2-' || generate_series(500,600),90 + random() * 10,'1990-8-8',$$No 77# Science 4 Street  of Xi'an  of China $$,12000);
INSERT INTO partition_wise_join_table_001_2 VALUES (generate_series(70,80),'PARTITION WIASE JOIN 2-3-' || generate_series(70,80),90 + random() * 10,'1990-8-8',$$No 77# Science 4 Street  of Xi'an  of China $$,15000);

CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_1 ON PARTITION_WISE_JOIN_TABLE_001_2(ID) LOCAL;
CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_2 ON PARTITION_WISE_JOIN_TABLE_001_2(ID,NAME) LOCAL;
CREATE INDEX IDX_PARTITION_WISE_JOIN_TABLE_001_2_3 ON PARTITION_WISE_JOIN_TABLE_001_2(SALARY,NAME) LOCAL;

SELECT A.ID,B.ID,A.NAME,B.NAME,A.RANK,B.SALARY,A.SALARY,A.ADDRESS,B.BIRTHDAY FROM PARTITION_WISE_JOIN_TABLE_001_1 A,PARTITION_WISE_JOIN_TABLE_001_2 B WHERE A.ID = B.ID AND  A.ID < 100 OR A.ID >400 order by 1, 2;

ANALYZE PARTITION_WISE_JOIN_TABLE_001_1;
ANALYZE PARTITION_WISE_JOIN_TABLE_001_2;

SELECT A.ID,B.ID,A.NAME,B.NAME,A.RANK,B.SALARY,A.SALARY,A.ADDRESS,B.BIRTHDAY FROM PARTITION_WISE_JOIN_TABLE_001_1 A,PARTITION_WISE_JOIN_TABLE_001_2 B WHERE A.ID = B.ID AND  A.ID < 100 OR A.ID >400 order by 1, 2;

CREATE TABLE HW_PARTITION_SELECT_RT (A INT, B INT)
PARTITION BY RANGE (A)
(
	PARTITION HW_PARTITION_SELECT_RT_P1 VALUES LESS THAN (1),
	PARTITION HW_PARTITION_SELECT_RT_P2 VALUES LESS THAN (4),
	PARTITION HW_PARTITION_SELECT_RT_P3 VALUES LESS THAN (7)
);
EXPLAIN (COSTS OFF, NODES OFF) SELECT B FROM (SELECT B FROM HW_PARTITION_SELECT_RT LIMIT 100) ORDER BY B;

--CREATE TABLE HW_PARTITION_SELECT_PTEST(A INT, B INT)
--PARTITION BY RANGE(A)
--INTERVAL(1000)
--(
--	PARTITION HW_PARTITION_SELECT_PTEST_P0 VALUES LESS THAN(0)
--);

--CREATE INDEX INDEX_ON_PTEST ON HW_PARTITION_SELECT_PTEST(A) LOCAL;
--INSERT INTO HW_PARTITION_SELECT_PTEST VALUES (GENERATE_SERIES(0,4999), GENERATE_SERIES(0,4999));
--ANALYZE HW_PARTITION_SELECT_PTEST;
--EXPLAIN(COSTS OFF, NODES OFF) SELECT * FROM HW_PARTITION_SELECT_PTEST; --SEQSCAN
--EXPLAIN(COSTS OFF, NODES OFF) SELECT * FROM HW_PARTITION_SELECT_PTEST WHERE A = 5000 AND B > 20;  --INDEXSCAN
--EXPLAIN(COSTS OFF, NODES OFF) SELECT * FROM HW_PARTITION_SELECT_PTEST WHERE A = 500 OR A = 3000; --BITMAPSCAN
--EXPLAIN(COSTS OFF, NODES OFF) SELECT A FROM HW_PARTITION_SELECT_PTEST WHERE A > 5000; -- INDEXONLYSCAN

CREATE TABLE TESTTABLE_TEST1(A INT) PARTITION BY RANGE (A)(PARTITION TESTTABLE_TEST1_P1 VALUES LESS THAN (10));
CREATE TABLE TESTTABLE_TEST2(A INT);
SELECT * FROM TESTTABLE_TEST1 UNION ALL SELECT * FROM TESTTABLE_TEST2 order by 1;


create table hw_partition_select_rangetab( C_CHAR_3 CHAR(102400), C_INT INTEGER)
partition by range (C_CHAR_3)
(
partition RANGE_PARTITION_000_1 values less than ('DDDDD'),
partition RANGE_PARTITION_000_2 values less than ('ZZZZZ')
);
select * from  hw_partition_select_rangetab where length(C_CHAR_3)>5 order by 1, 2;

create table hw_partition_select_rangetab( C_CHAR_3 text, C_INT INTEGER)
partition by range (C_CHAR_3)
(
partition RANGE_PARTITION_000_1 values less than ('DDDDD'),
partition RANGE_PARTITION_000_2 values less than ('ZZZZZ')
);
select * from  hw_partition_select_rangetab where length(C_CHAR_3)>5 order by 1, 2;


--create range partition table
CREATE TABLE select_partition_table_000_2(
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
	partition by range (C_TS_WITHOUT,C_NUMERIC)
( 
     partition select_partition_000_2_1 values less than ('2000-05-01', 5),
     partition select_partition_000_2_3 values less than ('2000-10-01', 10)
);

INSERT INTO select_partition_table_000_2 VALUES('A','ABC','ABCDEFG','a','abc','abcdefg',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO select_partition_table_000_2 VALUES('B','BCD','BCDEFGH','b','bcd','bcdefgh',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO select_partition_table_000_2 VALUES('C','CDE','CDEFGHI','c','cde','cdefghi',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO select_partition_table_000_2 VALUES('D','DEF','DEFGHIJ','d','def','defghij',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO select_partition_table_000_2 VALUES('E','EFG','EFGHIJK','e','efg','efghijk',555,555555,55,4.5,4.55,5.555,'2000-05-01','2000-05-01 ','2000-05-05 05:05:05+05');
INSERT INTO select_partition_table_000_2 VALUES('E','EFG','EFGHIJK','e','efg','efghijk',555,555555,55,5.5,5.55,5.555,'2000-05-01','2000-05-01 ','2000-05-05 05:05:05+05');
INSERT INTO select_partition_table_000_2 VALUES('F','FGH','FGHIJKL','f','fgh','fghijkl',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO select_partition_table_000_2 VALUES('G','GHI','GHIJKLM','g','ghi','ghijklm',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO select_partition_table_000_2 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_2 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO select_partition_table_000_2 VALUES('H','HIJ','HIJKLMN','h','hij','hijklmn',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO select_partition_table_000_2 VALUES('I','IJK','IJKLMNO','i','ijk','ijklmno',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');

create index select_partition_table_index_000_2 ON select_partition_table_000_2(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) local(PARTITION select_partition_000_2_1, PARTITION select_partition_000_2_3);
create view select_partition_table_view_000_2 as select * from select_partition_table_000_2;

explain (costs off, verbose on) select lower(C_CHAR_3), initcap(C_VARCHAR_3), sqrt(C_INT), C_NUMERIC- 1 + 2*6/3, rank() over w from select_partition_table_000_2 where C_INT > 600 or C_BIGINT < 444444 window w as (partition by C_TS_WITHOUT) order by 1,2,3,4,5;

select lower(C_CHAR_3), initcap(C_VARCHAR_3), sqrt(C_INT), C_NUMERIC- 1 + 2*6/3, rank() over w from select_partition_table_000_2 where C_INT > 600 or C_BIGINT < 444444 window w as (partition by C_TS_WITHOUT) order by 1,2,3,4,5;

--create interval partition table
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
	partition by range (C_INT)
( 
     partition select_partition_000_3_1 values less than (500),
     partition select_partition_000_3_3 values less than (2000)
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

create table hw_partition_select_rt1 (a int, b int, c int, d int) 
partition by range (a, b, c, d) 
(
partition pone values less than (2, 2, 2, 2),
partition ptwo values less than (2, 3, 2, 2)
);

select * from hw_partition_select_rt1 where a=2 and b>2 and c=2 and d=2;

create table hw_partition_select_rt2 (a int, b int, c int, d int) 
partition by range (a, b, c, d) 
(
partition hw_partition_select_rt2_p1 values less than (2, 2, MAXVALUE, 2),
partition hw_partition_select_rt2_p2 values less than (2, 3, 2, MAXVALUE)
);

select * from hw_partition_select_rt2 where a=2 and b>2 and c=2 and d=2;

create table hw_partition_select_rt3 (a int, b int, c int, d int) 
partition by range (a, b, c, d) 
(
partition hw_partition_select_rt3_p1 values less than (2, 2, 3, 2),
partition hw_partition_select_rt3_p2 values less than (2, 3, 2, MAXVALUE)
);

select * from hw_partition_select_rt3 where a=2 and b>2 and c=2 and d=2;

create table hw_partition_select_rt4 (a int, b int, c int, d int) 
partition by range (a, b, c, d) 
(
partition hw_partition_select_rt4_p1 values less than (2, 2, 3, 2),
partition hw_partition_select_rt4_p2 values less than (2, 4, 2, MAXVALUE)
);

select * from hw_partition_select_rt4 where a=2 and b>2 and c=2 and d=2;

create table hw_partition_select_rt5 (a int, b int, c int)
partition by range(c)
(
partition hw_partition_select_rt5_p1 values less than (1)
);

alter table hw_partition_select_rt5 drop column b;

update hw_partition_select_rt5 set c=0 where c=-1;

drop schema FVT_COMPRESS_QWER cascade;


