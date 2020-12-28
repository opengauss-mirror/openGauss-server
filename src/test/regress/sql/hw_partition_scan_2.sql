
-- Enables the planner's use of partitionwisejoin plans
RESET ENABLE_PARTITIONWISE;
RESET ENABLE_SEQSCAN;
RESET ENABLE_INDEXSCAN;
RESET ENABLE_INDEXONLYSCAN;
RESET ENABLE_BITMAPSCAN;
RESET ENABLE_TIDSCAN;
RESET ENABLE_SORT;
RESET ENABLE_MATERIAL;
RESET ENABLE_NESTLOOP;
RESET ENABLE_MERGEJOIN;
RESET ENABLE_HASHJOIN;

--CREATE RANGE PARTITION TABLE
CREATE SCHEMA fvt_obj_scan_define;
CREATE TABLE fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1(
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
	C_NUMERIC NUMERIC(10,5),
	C_DP DOUBLE PRECISION,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	PARTITION BY RANGE (C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT)
( 
     PARTITION SCAN_PARTITION_000_1_1 VALUES LESS THAN ('D', 'D', 400, '2000-04-01'),
     PARTITION SCAN_PARTITION_000_1_2 VALUES LESS THAN ('G', 'G', 700, '2000-07-01'),
     PARTITION SCAN_PARTITION_000_1_3 VALUES LESS THAN ('K', 'K',1000, '2000-10-01')
);
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('A','ABC','ABCDEFG','A','ABC','ABCDEFG',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('B','BCD','BCDEFGH','B','BCD','BCDEFGH',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('C','CDE','CDEFGHI','C','CDE','CDEFGHI',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('D','DEF','DEFGHIJ','D','DEF','DEFGHIJ',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('E','EFG','EFGHIJK','E','EFG','EFGHIJK',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('F','FGH','FGHIJKL','F','FGH','FGHIJKL',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('G','GHI','GHIJKLM','G','GHI','GHIJKLM',699,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('G','GHI','GHIJKLM','G','GHI','GHIJKLM',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',300,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',600,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','D','H','HIJ','D',200,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','D','H','HIJ','D',400,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','G','H','HIJ','G',200,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','G','H','HIJ','G',500,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','G','H','HIJ','G',700,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 VALUES('H','HIJ','K','H','HIJ','G',700,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');

CREATE INDEX SCAN_PARTITION_TABLE_INDEX_000_1 ON fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) LOCAL(PARTITION SCAN_PARTITION_000_1_1, PARTITION SCAN_PARTITION_000_1_2, PARTITION SCAN_PARTITION_000_1_3);
CREATE VIEW fvt_obj_scan_define.SCAN_PARTITION_TABLE_VIEW_000_1 AS SELECT * FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1;

--CREATE RANGE PARTITION TABLE
CREATE TABLE fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2(
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
	C_NUMERIC NUMERIC(10,5),
	C_DP DOUBLE PRECISION,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	PARTITION BY RANGE (C_TS_WITHOUT,C_NUMERIC)
( 
     PARTITION SCAN_PARTITION_000_2_1 VALUES LESS THAN ('2000-05-01', 5),
     PARTITION SCAN_PARTITION_000_2_3 VALUES LESS THAN ('2000-10-01', 10)
);

INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('A','ABC','ABCDEFG','A','ABC','ABCDEFG',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('B','BCD','BCDEFGH','B','BCD','BCDEFGH',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('C','CDE','CDEFGHI','C','CDE','CDEFGHI',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('D','DEF','DEFGHIJ','D','DEF','DEFGHIJ',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('E','EFG','EFGHIJK','E','EFG','EFGHIJK',555,555555,55,4.5,4.55,5.555,'2000-05-01','2000-05-01 ','2000-05-05 05:05:05+05');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('E','EFG','EFGHIJK','E','EFG','EFGHIJK',555,555555,55,5.5,5.55,5.555,'2000-05-01','2000-05-01 ','2000-05-05 05:05:05+05');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('F','FGH','FGHIJKL','F','FGH','FGHIJKL',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('G','GHI','GHIJKLM','G','GHI','GHIJKLM',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');

CREATE INDEX SCAN_PARTITION_TABLE_INDEX_000_2 ON fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) LOCAL(PARTITION SCAN_PARTITION_000_2_1, PARTITION SCAN_PARTITION_000_2_3);
CREATE VIEW fvt_obj_scan_define.SCAN_PARTITION_TABLE_VIEW_000_2 AS SELECT * FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_2;

--CREATE RANGE PARTITION TABLE
CREATE TABLE fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3(
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
	C_NUMERIC NUMERIC(10,5),
	C_DP DOUBLE PRECISION,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	PARTITION BY RANGE (C_INT)
( 
     PARTITION SCAN_PARTITION_000_3_1 VALUES LESS THAN (500),
     PARTITION SCAN_PARTITION_000_3_2 VALUES LESS THAN (1000),
	 PARTITION SCAN_PARTITION_000_3_3 VALUES LESS THAN (1100),
	 PARTITION SCAN_PARTITION_000_3_4 VALUES LESS THAN (1200),
	 PARTITION SCAN_PARTITION_000_3_5 VALUES LESS THAN (1300),
	 PARTITION SCAN_PARTITION_000_3_6 VALUES LESS THAN (1400),
	 PARTITION SCAN_PARTITION_000_3_7 VALUES LESS THAN (1500),
	 PARTITION SCAN_PARTITION_000_3_8 VALUES LESS THAN (1600),
	 PARTITION SCAN_PARTITION_000_3_9 VALUES LESS THAN (1700)
);

CREATE INDEX SCAN_PARTITION_TABLE_INDEX_000_3 ON fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3(C_CHAR_3,C_VARCHAR_3,C_INT,C_TS_WITHOUT) LOCAL;
CREATE VIEW fvt_obj_scan_define.SCAN_PARTITION_TABLE_VIEW_000_3 AS SELECT * FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3;

INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('A','ABC','ABCDEFG','A','ABC','ABCDEFG',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('B','BCD','BCDEFGH','B','BCD','BCDEFGH',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('C','CDE','CDEFGHI','C','CDE','CDEFGHI',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('D','DEF','DEFGHIJ','D','DEF','DEFGHIJ',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('E','EFG','EFGHIJK','E','EFG','EFGHIJK',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('F','FGH','FGHIJKL','F','FGH','FGHIJKL',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('G','GHI','GHIJKLM','G','GHI','GHIJKLM',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',1100,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',1600,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');

--CREATE RANGE PARTITION TABLE
CREATE TABLE fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4(
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
	C_NUMERIC NUMERIC(10,5),
	C_DP DOUBLE PRECISION,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE ) 
	PARTITION BY RANGE (C_TS_WITHOUT)
( 
     PARTITION SCAN_PARTITION_000_4_1 VALUES LESS THAN ('2000-04-01'),
     PARTITION SCAN_PARTITION_000_4_2 VALUES LESS THAN ('2000-07-01'),
     PARTITION SCAN_PARTITION_000_4_3 VALUES LESS THAN ('2000-10-01'),
     PARTITION SCAN_PARTITION_000_4_4 VALUES LESS THAN ('2000-12-31')
);

CREATE INDEX SELECT_PARTITION_TABLE_INDEX_000_4 ON fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4(C_CHAR_3,C_VARCHAR_3,C_INT) LOCAL;
CREATE VIEW fvt_obj_scan_define.SCAN_PARTITION_TABLE_VIEW_000_4 AS SELECT * FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4;

INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('A','ABC','ABCDEFG','A','ABC','ABCDEFG',111,111111,11,1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('B','BCD','BCDEFGH','B','BCD','BCDEFGH',222,222222,22,2.2,2.22,2.222,'2000-02-02','2000-02-02 02:02:02','2000-02-02 02:02:02+02');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('C','CDE','CDEFGHI','C','CDE','CDEFGHI',333,333333,33,3.3,3.33,3.333,'2000-03-03','2000-03-03 03:03:03','2000-03-03 03:03:03+03');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('D','DEF','DEFGHIJ','D','DEF','DEFGHIJ',444,444444,44,4.4,4.44,4.444,'2000-04-04','2000-04-04 04:04:04','2000-04-04 04:04:04+04');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('E','EFG','EFGHIJK','E','EFG','EFGHIJK',555,555555,55,5.5,5.55,5.555,'2000-05-05','2000-05-05 05:05:05','2000-05-05 05:05:05+05');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('F','FGH','FGHIJKL','F','FGH','FGHIJKL',666,666666,66,6.6,6.66,6.666,'2000-06-06','2000-06-06 06:06:06','2000-06-06 06:06:06+06');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('G','GHI','GHIJKLM','G','GHI','GHIJKLM',777,777777,77,7.7,7.77,7.777,'2000-07-07','2000-07-07 07:07:07','2000-07-07 07:07:07+07');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('H','HIJ','HIJKLMN','H','HIJ','HIJKLMN',888,888888,88,8.8,8.88,8.888,'2000-08-08','2000-08-08 08:08:08','2000-08-08 08:08:08+08');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('I','IJK','IJKLMNO','I','IJK','IJKLMNO',999,999999,99,9.9,9.99,9.999,'2000-09-09','2000-09-09 09:09:09','2000-09-09 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('J','IJK','IJKLMNO','J','JKL','JKLMNOP',1111,111111,111,11.1,11.11,11.111,'2000-09-09','2000-10-02 09:09:09','2000-10-02 09:09:09+09');
INSERT INTO fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_4 VALUES('K','JKL','IJKLMNO','J','JKL','JKLMNOP',1111,111111,111,11.1,11.11,11.111,'2000-09-09','2000-10-06 09:09:09','2000-10-06 09:09:09+09');

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT AND C.C_INT = D.C_INT AND A.C_TS_WITHOUT < '2000-06-01' ORDER BY 1,2,3,4,5,6,7,8;

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT AND C.C_INT = D.C_INT AND A.C_TS_WITHOUT < '2000-06-01' ORDER BY 1,2,3,4,5,6,7,8;

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT AND C.C_INT = D.C_INT AND A.C_TS_WITHOUT < '2000-06-01' ORDER BY A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT;

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT AND C.C_INT = D.C_INT AND A.C_TS_WITHOUT < '2000-06-01' ORDER BY 1;

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT AND C.C_INT = D.C_INT ORDER BY 1; 

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE A.C_INT = B.C_INT AND B.C_INT = C.C_INT ORDER BY 1, 7;

SELECT A.C_INT, A.C_TS_WITHOUT, B.C_INT, B.C_TS_WITHOUT, C.C_INT, C.C_TS_WITHOUT, D.C_INT, D.C_TS_WITHOUT FROM fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 A, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 B, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_1 C, fvt_obj_scan_define.SCAN_PARTITION_TABLE_000_3 D WHERE  C.C_INT = D.C_INT ORDER BY 1, 3, 5;

DROP SCHEMA fvt_obj_scan_define CASCADE;

CREATE SCHEMA fvt_data_partition_scan;
CREATE TABLE fvt_data_partition_scan.performance_select_index_tbl_000  ( 
    StartTime            timestamp without time  zone NOT NULL,
    StartTimeDstOffset   smallint NOT NULL,
    SvrStartTime         timestamp without time  zone NOT NULL,
    STSvrDstOffset       smallint NOT NULL,
    EndTime              timestamp without time  zone NOT NULL,
    EndTimeDstOffset     smallint NOT NULL,
    SvrEndTime           timestamp without time  zone NOT NULL,
    ETSvrDstOffset       smallint NOT NULL,
    InsertTime           timestamp without time  zone NULL,
    TimezoneOffset       smallint NOT NULL,
    ObjectNo             int NOT NULL,
    GranulityPeriod      smallint NULL,
    ResultReliablityFlag  smallint NULL,
    Day                  smallint NULL,
    Counter_50331654     decimal(13,3) NULL,
    Counter_50331655     decimal(13,3) NULL,
    Counter_50331656     decimal(13,3) NULL,
    Counter_50331657     decimal(13,3) NULL,
    Counter_50331658     decimal(13,3) NULL,
    Counter_50331659     decimal(13,3) NULL,
    Counter_50331660     decimal(13,3) NULL,
    Counter_50331661     decimal(13,3) NULL,
    Counter_50331662     decimal(13,3) NULL,
    Counter_50331663     decimal(13,3) NULL,
    Counter_50331664     decimal(13,3) NULL,
    Counter_50331665     decimal(13,3) NULL,
    Counter_50331666     decimal(13,3) NULL,
    Counter_50331667     decimal(13,3) NULL,
    Counter_50331668     decimal(13,3) NULL,
    Counter_50331669     decimal(13,3) NULL,
    Counter_50331670     decimal(13,3) NULL,
    Counter_50331671     decimal(13,3) NULL,
    Counter_50331672     decimal(13,3) NULL,
    Counter_50331673     decimal(13,3) NULL,
    Counter_50331674     decimal(13,3) NULL,
    Counter_50331675     decimal(13,3) NULL,
    Counter_50331676     decimal(13,3) NULL,
    Counter_50331677     decimal(13,3) NULL,
    Counter_50331678     decimal(13,3) NULL,
    Counter_50331679     decimal(13,3) NULL
    )

partition by range (Day) 
    (
    partition Day_0         VALUES LESS THAN (0)  ,
    partition Day_1         VALUES LESS THAN (1)  ,
    partition Day_2         VALUES LESS THAN (2)  ,
    partition Day_3         VALUES LESS THAN (3)  ,
    partition Day_4         VALUES LESS THAN (4)  ,
    partition Day_5         VALUES LESS THAN (5)  ,
    partition Day_6         VALUES LESS THAN (6)  ,
    partition Day_7         VALUES LESS THAN (7)  ,
    partition Day_8         VALUES LESS THAN (8)  ,
    partition Day_9         VALUES LESS THAN (9)  ,
    partition Day_10     VALUES LESS THAN (10)  ,
    partition Day_11     VALUES LESS THAN (11)  ,
    partition Day_12     VALUES LESS THAN (12)  ,
    partition Day_13     VALUES LESS THAN (13)  ,
    partition Day_14     VALUES LESS THAN (14)  ,
    partition Day_15     VALUES LESS THAN (15)  ,
    partition Day_16     VALUES LESS THAN (16)  ,
    partition Day_17     VALUES LESS THAN (17)  ,
    partition Day_18     VALUES LESS THAN (18)  ,
    partition Day_19     VALUES LESS THAN (19)  ,
    partition Day_20     VALUES LESS THAN (20)  ,
    partition Day_21     VALUES LESS THAN (21)  ,
    partition Day_22     VALUES LESS THAN (22)  ,
    partition Day_23     VALUES LESS THAN (23)  ,
    partition Day_24     VALUES LESS THAN (24)  ,
    partition Day_25     VALUES LESS THAN (25)  ,
    partition Day_26     VALUES LESS THAN (26)  ,
    partition Day_27     VALUES LESS THAN (27)  ,
    partition Day_28     VALUES LESS THAN (28)  ,
    partition Day_29     VALUES LESS THAN (29)  ,
    partition Day_30     VALUES LESS THAN (30)  ,
    partition Day_31     VALUES LESS THAN (31)  ,
    partition Day_32     VALUES LESS THAN (32)  ,
    partition Day_33     VALUES LESS THAN (33)  ,
    partition Day_34     VALUES LESS THAN (34)  ,
    partition Day_35     VALUES LESS THAN (35)  ,
    partition Day_36     VALUES LESS THAN (36)  ,
    partition Day_37     VALUES LESS THAN (37)  ,
    partition Day_38     VALUES LESS THAN (38)  ,
    partition Day_39     VALUES LESS THAN (39)  ,
    partition Day_40     VALUES LESS THAN (40)  ,
    partition Day_41     VALUES LESS THAN (41)  ,
    partition Day_42     VALUES LESS THAN (42)  ,
    partition Day_43     VALUES LESS THAN (43)  ,
    partition Day_44     VALUES LESS THAN (44)  ,
    partition Day_45     VALUES LESS THAN (45)  ,
    partition Day_46     VALUES LESS THAN (46)  ,
    partition Day_47     VALUES LESS THAN (47)  ,
    partition Day_48     VALUES LESS THAN (48)  ,
    partition Day_49     VALUES LESS THAN (49)  ,
    partition Day_50     VALUES LESS THAN (50)  ,
    partition Day_51     VALUES LESS THAN (51)  ,
    partition Day_52     VALUES LESS THAN (52)  ,
    partition Day_53     VALUES LESS THAN (53)  ,
    partition Day_54     VALUES LESS THAN (54)  ,
    partition Day_55     VALUES LESS THAN (55)  ,
    partition Day_56     VALUES LESS THAN (56)  ,
    partition Day_57     VALUES LESS THAN (57)  ,
    partition Day_58     VALUES LESS THAN (58)  ,
    partition Day_59     VALUES LESS THAN (59)  ,
    partition Day_60     VALUES LESS THAN (60)  ,
    partition Day_61     VALUES LESS THAN (61)  ,
    partition Day_62     VALUES LESS THAN (62)  ,
    partition Day_63     VALUES LESS THAN (63)  ,
    partition Day_64     VALUES LESS THAN (64)  ,
    partition Day_65     VALUES LESS THAN (65)  ,
    partition Day_66     VALUES LESS THAN (66)  ,
    partition Day_67     VALUES LESS THAN (67)  ,
    partition Day_68     VALUES LESS THAN (68)  ,
    partition Day_69     VALUES LESS THAN (69)  ,
    partition Day_70     VALUES LESS THAN (70)  ,
    partition Day_71     VALUES LESS THAN (71)  ,
    partition Day_72     VALUES LESS THAN (72)  ,
    partition Day_73     VALUES LESS THAN (73)  ,
    partition Day_74     VALUES LESS THAN (74)  ,
    partition Day_75     VALUES LESS THAN (75)  ,
    partition Day_76     VALUES LESS THAN (76)  ,
    partition Day_77     VALUES LESS THAN (77)  ,
    partition Day_78     VALUES LESS THAN (78)  ,
    partition Day_79     VALUES LESS THAN (79)  ,
    partition Day_80     VALUES LESS THAN (80)  ,
    partition Day_81     VALUES LESS THAN (81)  ,
    partition Day_82     VALUES LESS THAN (82)  ,
    partition Day_83     VALUES LESS THAN (83)  ,
    partition Day_84     VALUES LESS THAN (84)  ,
    partition Day_85     VALUES LESS THAN (85)  ,
    partition Day_86     VALUES LESS THAN (86)  ,
    partition Day_87     VALUES LESS THAN (87)  ,
    partition Day_88     VALUES LESS THAN (88)  ,
    partition Day_89     VALUES LESS THAN (89)  ,
    partition Day_90     VALUES LESS THAN (90)  ,
    partition Day_91     VALUES LESS THAN (91)  ,
    partition Day_92     VALUES LESS THAN (92)  ,
    partition Day_93     VALUES LESS THAN (93)  ,
    partition Day_94     VALUES LESS THAN (94)  ,
    partition Day_95     VALUES LESS THAN (95)  ,
    partition Day_96     VALUES LESS THAN (96)  ,
    partition Day_97     VALUES LESS THAN (97)  ,
    partition Day_98     VALUES LESS THAN (98)  ,
    partition Day_99     VALUES LESS THAN (99)
 );
CREATE INDEX performance_select_index_index_000 ON fvt_data_partition_scan.performance_select_index_tbl_000(Day) LOCAL;
SET ENABLE_SEQSCAN = FALSE;
\! /data2/moyq/mppdb_svn/GAUSS200_OLAP_TRUNK/Code/src/test/regress/./tmp_check/install//data2/moyq/install/GAUSS200_OLAP_TRUNK/bin/gs_guc reload -D /data2/moyq/mppdb_svn/GAUSS200_OLAP_TRUNK/Code/src/test/regress/tmp_check/coordinator1 -Z coordinator -c "debug_print_plan=on" >/dev/null
EXPLAIN (COSTS OFF, NODES OFF) SELECT * FROM fvt_data_partition_scan.PERFORMANCE_SELECT_INDEX_TBL_000 WHERE DAY =1 OR  DAY =2 ;
\! /data2/moyq/mppdb_svn/GAUSS200_OLAP_TRUNK/Code/src/test/regress/./tmp_check/install//data2/moyq/install/GAUSS200_OLAP_TRUNK/bin/gs_guc reload -D /data2/moyq/mppdb_svn/GAUSS200_OLAP_TRUNK/Code/src/test/regress/tmp_check/coordinator1 -Z coordinator -c "debug_print_plan=off" >/dev/null
SELECT * FROM fvt_data_partition_scan.PERFORMANCE_SELECT_INDEX_TBL_000 WHERE DAY =1 OR  DAY =2 ;
EXPLAIN (COSTS OFF, NODES OFF) SELECT COUNT(*) FROM fvt_data_partition_scan.PERFORMANCE_SELECT_INDEX_TBL_000 WHERE DAY =1 OR  DAY =2 ;
SELECT COUNT(*) FROM fvt_data_partition_scan.PERFORMANCE_SELECT_INDEX_TBL_000 WHERE DAY =1 OR  DAY =2 ;
DROP SCHEMA fvt_data_partition_scan CASCADE;

-- Test for TidScan in cursor
CREATE TABLE TEST_TIDSCAN_PARTITION(SCORE INTEGER)
PARTITION BY RANGE (SCORE)
(
	PARTITION A1 VALUES LESS THAN (10),
	PARTITION A2 VALUES LESS THAN (20),
	PARTITION A3 VALUES LESS THAN (30),
	PARTITION A4 VALUES LESS THAN (40),
	PARTITION A5 VALUES LESS THAN (50)
) enable row movement;

INSERT INTO TEST_TIDSCAN_PARTITION VALUES(1),(11),(21),(31),(41);
SELECT * FROM TEST_TIDSCAN_PARTITION ORDER BY 1;

START TRANSACTION;
	CURSOR CUR FOR SELECT SCORE  FROM TEST_TIDSCAN_PARTITION WHERE SCORE  > 0;
	MOVE CUR;
	EXPLAIN (COSTS OFF, NODES OFF) UPDATE TEST_TIDSCAN_PARTITION SET SCORE =SCORE+1 WHERE CURRENT OF CUR;
	UPDATE TEST_TIDSCAN_PARTITION SET SCORE =SCORE+1 WHERE CURRENT OF CUR;
	CLOSE CUR;
END;
SELECT * FROM TEST_TIDSCAN_PARTITION ORDER BY 1;

START TRANSACTION;
	CURSOR CUR FOR SELECT SCORE  FROM TEST_TIDSCAN_PARTITION WHERE SCORE  > 0;
	MOVE CUR;
	EXPLAIN (COSTS OFF, NODES OFF) DELETE FROM TEST_TIDSCAN_PARTITION WHERE CURRENT OF CUR;
	DELETE FROM TEST_TIDSCAN_PARTITION WHERE CURRENT OF CUR;
	CLOSE CUR;
END;
SELECT * FROM TEST_TIDSCAN_PARTITION ORDER BY 1;

DROP TABLE TEST_TIDSCAN_PARTITION;

-- SCHEMA TPCC
DROP SCHEMA TPCC CASCADE;
CREATE SCHEMA TPCC;

-- TABLE TPCC.CUSTOMER
CREATE TABLE TPCC.CUSTOMER
(
    C_ID INTEGER NOT NULL,
    C_D_ID INTEGER NOT NULL,
    C_W_ID INTEGER NOT NULL,
    C_FIRST CHARACTER VARYING(16) NOT NULL,
    C_MIDDLE CHARACTER(2) NOT NULL,
    C_LAST CHARACTER VARYING(16) NOT NULL,
    C_STREET_1 CHARACTER VARYING(20) NOT NULL,
    C_STREET_2 CHARACTER VARYING(20) NOT NULL,
    C_CITY CHARACTER VARYING(20) NOT NULL,
    C_STATE CHARACTER(2) NOT NULL,
    C_ZIP CHARACTER(9) NOT NULL,
    C_PHONE CHARACTER(16) NOT NULL,
    C_SINCE TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    C_CREDIT CHARACTER(2) NOT NULL,
    C_CREDIT_LIM NUMERIC(12,2) NOT NULL,
    C_DISCOUNT NUMERIC(4,4) NOT NULL,
    C_BALANCE NUMERIC(12,2) NOT NULL,
    C_YTD_PAYMENT NUMERIC(12,2) NOT NULL,
    C_PAYMENT_CNT INTEGER NOT NULL,
    C_DELIVERY_CNT INTEGER NOT NULL,
    C_DATA CHARACTER VARYING(500) NOT NULL
);
ALTER TABLE TPCC.CUSTOMER ADD CONSTRAINT PK_CUSTOMER PRIMARY KEY (C_W_ID, C_D_ID, C_ID);
CREATE INDEX NDX_CUSTOMER_NAME ON TPCC.CUSTOMER USING BTREE (C_W_ID, C_D_ID, C_LAST, C_FIRST);
INSERT INTO TPCC.CUSTOMER VALUES (1, 1, 1, 'iscmvlstpn', 'OE', 'BARBARBAR', 'bkilipzfcxcle', 'pmbwodmpvhvpafbj', 'dyfaoptppzjcgjrvyqa', 'uq', '480211111', '9400872216162535', '2013-01-04 11:26:41', 'GC', 50000.00, .4361, -10.00, 10.00, 1, 0, 'qvldetanrbrburbmzqujshoqnggsmnteccipriirdhirwiynpfzcsykxxyscdsfqafhatdokmjogfgslucunvwbtbfsqzjeclbacpjqdhjchvgbnrkjrgjrycsgppsocnevautzfeosviaxbvobffnjuqhlvnwuqhtgjqsbfacwjpbvpgthpyxcpmnutcjxrbxxbmrmwwxcepwiixvvleyajautcesljhrsfsmsnmzjcxvcuxdwmyijbwywiirsgocwktedbbokhynznceaesuifkgoaafagugetfhbcylksrjukvbufqcvbffaxnzssyquidvwefktknrchyxfphunqktwnipnsrvqswsymocnoexbabwnpmnxsvshdsjhazcauvqjgvqjfkjjgqrceyjmbumkapmcbxeashybpgekjkfezthnjbhfqiwbutbxtkjkndyylrvrhsazhijvmkmhdgvuyvyayiavdmypqomgobo');
INSERT INTO TPCC.CUSTOMER VALUES (2, 1, 1, 'phmencktqrps', 'OE', 'BARBAROUGHT', 'kytapfsuywamkucwpcxy', 'nbkqsbefbmgbasmiadp', 'qkhfsdlifokhfptsfpf', 'rb', '256711111', '8313671875670172', '2013-01-04 11:26:41', 'GC', 50000.00, .3875, -10.00, 10.00, 1, 0, 'oqhmhetvorihjqaqpyybpnibobqixkuebyprxzjpvkqecckhdvpxttjdixbwqpldjnblfziozxwwjabpwcrzjguiykbpfnfmqzcfxtzuzloklbtxbejdotibxbyozuamrgidiadygpiogtunzlewwwugbunzaxnyhxbhmaahbbwgpxrinyymvssaveladazwxwmnvcchvcetewnzymfxfofunrcwuigldzudzgonxeozwmbjhkqyyanwatxftargojhquknqdhcgfuzkphlnmtklrgcpormhkirqeurcrbimhrppqrmutgagrettbauxcfdpxneiyrfkcysfxdcgdchebavbawovrtlevxyjkdnwnkdkapopmyj');
INSERT INTO TPCC.CUSTOMER VALUES (3, 1, 1, 'qccnmddepil', 'OE', 'BARBARABLE', 'lawvuhqcck', 'aoafgbhpvhflhz', 'ihooljmkazu', 'qk', '905411111', '2276693691956320', '2013-01-04 11:26:41', 'GC', 50000.00, .0976, -10.00, 10.00, 1, 0, 'vbcjdfjqdxuxmjfsiqgjcdfjwsgpusxgjxhbmvgziyhobblidilcybjlgvjkgdqlarjjnxrrhjwdpzrkxhzshkignbblvoehpimhfzgovwjugstktgtsvijwuxflojewnjvevngcjmioxrgqtsthkijdepwihvhvibwslhnpiveelekgjmcustxgduqzdeuomaiplnutkzpsmkixxdjnssavpayngpareewomuzbosuqadyzkzklautxpgmzzvumqhgqbqzlbmpyasfunexfxjlpwyaabqellegbzfvxyilrjsbruhgkipjgwgwsunkanewjuikhghiq');

-- TABLE TPCC.WAREHOUSE
CREATE TABLE TPCC.WAREHOUSE
(
    W_ID INTEGER NOT NULL,
    W_NAME CHARACTER VARYING(10) NOT NULL,
    W_STREET_1 CHARACTER VARYING(20) NOT NULL,
    W_STREET_2 CHARACTER VARYING(20) NOT NULL,
    W_CITY CHARACTER VARYING(20) NOT NULL,
    W_STATE CHARACTER(2) NOT NULL,
    W_ZIP CHARACTER(9) NOT NULL,
    W_TAX NUMERIC(4,4) NOT NULL,
    W_YTD NUMERIC(12,2) NOT NULL
);
ALTER TABLE TPCC.WAREHOUSE ADD CONSTRAINT PK_WAREHOUSE PRIMARY KEY (W_ID);
INSERT INTO TPCC.WAREHOUSE VALUES (1, 'hvrjzuvnqg', 'pjoeqzpqqw', 'hibcbzhgnucvbl', 'nxosqqbmhwbmjzuulhk', 'xf', '669911111', .1712, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (2, 'jaqspofube', 'owgdhnucqyqbbfk', 'dtupkutecuap', 'mxhemmhyhneqdvvu', 'yd', '496611111', .0587, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (3, 'qcscbhkkql', 'uzeznxwryyxnnafma', 'rdkibeupaubqjbidkys', 'qqtfijqygynlimldu', 'rx', '545511111', .0755, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (4, 'dmkczswa', 'qnyjknerugc', 'ouwdesbzmapbaacdokxw', 'nmtycfewakbtwk', 'pj', '522411111', .0292, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (5, 'vsfcguexuf', 'otgxyehwdfeuijpq', 'etabccwdpyrlhjx', 'fdvorawcqkevk', 'ge', '329711111', .1093, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (6, 'escpbk', 'pgldmwzurmpcsgkngae', 'smzypxptzjeptmbjgqec', 'fxikuujtuiwmszaoavt', 'cz', '784411111', .0920, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (7, 'jcanwmh', 'zvcqkejsttimeijkd', 'qwfwmvnqdn', 'pjabtrkivtrdymedt', 'kk', '950211111', .1957, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (8, 'wzdnxwhm', 'dxnwdkomunescoop', 'uusdlsbpnn', 'tcokvfcfyvgmpxgr', 'nk', '979511111', .1409, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (9, 'ydcuynmyud', 'fwrhyecfujfveira', 'ddptyswaplhortppwoa', 'zrnqelqgykbywthrfwt', 'im', '684011111', .1686, 300000.00);
INSERT INTO TPCC.WAREHOUSE VALUES (10, 'wfnlmpcw', 'wyebgagycgclfvz', 'hgswhwxldejumvgcjhwt', 'cvaxmlubplw', 'wp', '760511111', .0192, 300000.00);

-- SCHEMA fvt_data_partition_scan
DROP SCHEMA fvt_data_partition_scan CASCADE;
CREATE SCHEMA fvt_data_partition_scan;

-- TABLE fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000
CREATE TABLE fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000
(
    D_ID INTEGER NOT NULL,
    D_W_ID INTEGER NOT NULL,
    D_NAME CHARACTER VARYING(10) NOT NULL,
    D_STREET_1 CHARACTER VARYING(20) NOT NULL,
    D_STREET_2 CHARACTER VARYING(20) NOT NULL,
    D_CITY CHARACTER VARYING(20) NOT NULL
)
PARTITION BY RANGE (D_ID)
(
    PARTITION UPDATE_PARTITION_PICT_TABLE_FROM_000_1 VALUES LESS THAN (500),
    PARTITION UPDATE_PARTITION_PICT_TABLE_FROM_000_2 VALUES LESS THAN (1500)
);
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (1, 1, 'swotbb', 'ogsxoekiohenrovqcr', 'utwvubqsuhikkpefz', 'vpwhmiiwxjvzqad');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (2, 1, 'ntqarolc', 'ksnfrqaeubqgaodliopy', 'aenneibwcgejkwpa', 'txiuglqcgsunehor');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (3, 1, 'dmvecgmor', 'tuvhniprofrbvtmsyvw', 'eddapvbccowszjdx', 'mgfwgtycyxwui');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (4, 1, 'cayzjsdtio', 'wiojkmfcmp', 'wesurkukrxoii', 'rmxcijrbrmtewnp');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (5, 1, 'zskgwbq', 'dywdmnvyci', 'dvcsdvloau', 'nqwcundkxfuikagfyybn');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (6, 1, 'ugwscdmy', 'hfgiduaqisbmkinhi', 'jzmogdnmdoo', 'gotjbatmirk');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (7, 1, 'vlsywc', 'mtmgjffkmsatzl', 'rfspzqlurgduazqt', 'uthvwjrinzaxuzq');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (8, 1, 'hlysyik', 'uslotvsjfagtix', 'okzjalhyrzcxtsbvhdqn', 'whhxarkdvqseugecwb');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (9, 1, 'ddcaijlkq', 'kqualmlmiihcbgg', 'qjiusmbhvkufegpzb', 'anyvweicbzoc');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (10, 1, 'osmgkxgssu', 'qjrlstdsqgwbirqefi', 'zpsflfeeldlzfxx', 'rwokxjeczcu');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (1, 2, 'hqpwuvosy', 'qaobjrprypislzgkhnz', 'tktsebeuitpau', 'cioejsreouallmrwhjb');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (2, 2, 'lddvtz', 'ogtxorjjvsrtw', 'knhogjcjxyoxkqcn', 'cteadumukr');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (3, 2, 'qmnagzh', 'dpywygfwcrysss', 'qsmokswjfwb', 'lcrlvihudqkjl');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (4, 2, 'faelfr', 'kydmzwgnsnrdtpvuztm', 'dprhhewqsjcdsve', 'shkspklvjungqai');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (5, 2, 'vzglpg', 'qnsvqaarnaayxotrqcm', 'hgffdorrndnsfszfkoa', 'oihafkcckgndx');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (6, 2, 'extclapdr', 'hhrneiwtlzvan', 'dqmntscldng', 'mvgljxfnbfpiffhq');
INSERT INTO fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 VALUES (7, 2, 'tetatfq', 'dalquhejntt', 'ttbsvjxbvhfsu', 'xkqxgqrtizeorobri');

-- TABLE fvt_data_partition_scan.WITH_UPDATE_TABLE_001
CREATE TABLE fvt_data_partition_scan.WITH_UPDATE_TABLE_001
(
    W_ID INTEGER,
    W_NAME CHARACTER VARYING(10),
    W_STREET_1 CHARACTER VARYING(20),
    W_STREET_2 CHARACTER VARYING(20),
    W_CITY CHARACTER VARYING(20)
);
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (1, 'hvrjzuvnqg', 'pjoeqzpqqw', 'hibcbzhgnucvbl', 'nxosqqbmhwbmjzuulhk');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (1, 'hvrjzuvnqg', 'pjoeqzpqqw', 'hibcbzhgnucvbl', 'nxosqqbmhwbmjzuulhk');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (2, 'jaqspofube', 'owgdhnucqyqbbfk', 'dtupkutecuap', 'mxhemmhyhneqdvvu');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (3, 'qcscbhkkql', 'uzeznxwryyxnnafma', 'rdkibeupaubqjbidkys', 'qqtfijqygynlimldu');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (4, 'dmkczswa', 'qnyjknerugc', 'ouwdesbzmapbaacdokxw', 'nmtycfewakbtwk');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (5, 'vsfcguexuf', 'otgxyehwdfeuijpq', 'etabccwdpyrlhjx', 'fdvorawcqkevk');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (6, 'escpbk', 'pgldmwzurmpcsgkngae', 'smzypxptzjeptmbjgqec', 'fxikuujtuiwmszaoavt');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (7, 'jcanwmh', 'zvcqkejsttimeijkd', 'qwfwmvnqdn', 'pjabtrkivtrdymedt');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (8, 'wzdnxwhm', 'dxnwdkomunescoop', 'uusdlsbpnn', 'tcokvfcfyvgmpxgr');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (9, 'ydcuynmyud', 'fwrhyecfujfveira', 'ddptyswaplhortppwoa', 'zrnqelqgykbywthrfwt');
INSERT INTO fvt_data_partition_scan.WITH_UPDATE_TABLE_001 VALUES (10, 'wfnlmpcw', 'wyebgagycgclfvz', 'hgswhwxldejumvgcjhwt', 'cvaxmlubplw');

-- Begin test
CREATE TABLE  fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059
(
	C_ID INT NOT NULL,
	C_D_ID INT NOT NULL,
	C_W_ID INT NOT NULL,
	C_FIRST VARCHAR(16),
	C_MIDDLE CHAR(2) NOT NULL,
	C_LAST VARCHAR(16) NOT NULL,
	C_STREET_1 VARCHAR(20) NOT NULL,
	C_STREET_2 VARCHAR(20) NOT NULL,
	C_CITY VARCHAR(20) NOT NULL
)
distribute by hash (C_CITY)
PARTITION BY RANGE (C_ID)
(
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_1  VALUES LESS THAN (500) ,
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_2  VALUES LESS THAN (1500),
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_3  VALUES LESS THAN (2000),
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_4  VALUES LESS THAN (2500),
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_5  VALUES LESS THAN (3000),
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_6  VALUES LESS THAN (3500),
	PARTITION UPDATE_PARTITION_PICT_TABLE_059_7  VALUES LESS THAN (4000)
)
ENABLE  ROW MOVEMENT; 
CREATE INDEX UPDATE_PARTITION_PICT_TABLE_059_INDEX_001 ON fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059(C_ID) LOCAL;
CREATE INDEX UPDATE_PARTITION_PICT_TABLE_059_INDEX_002 ON fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059(C_ID) LOCAL;
CREATE INDEX UPDATE_PARTITION_PICT_TABLE_059_INDEX_003 ON fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059(C_ID , C_D_ID) LOCAL;
CREATE INDEX UPDATE_PARTITION_PICT_TABLE_059_INDEX_004 ON fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059(C_D_ID) LOCAL;
INSERT INTO  fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059  SELECT C_ID  , C_D_ID , C_W_ID , C_FIRST , C_MIDDLE , C_LAST , C_STREET_1 , C_STREET_2 , C_CITY FROM TPCC.CUSTOMER WHERE C_ID<2000 AND C_D_ID=6 AND C_W_ID =3;

UPDATE  fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059 UPDATE_059 SET
	UPDATE_059.C_ID=UPDATE_059.C_ID+2000,
	UPDATE_059.C_D_ID=FROM_DD.D_ID+WITH_CC.W_ID,
	UPDATE_059.C_W_ID=UPDATE_059.C_W_ID+FROM_DD.D_W_ID+WITH_CC.W_ID,
	UPDATE_059.C_FIRST=WITH_CC.W_NAME,
	(C_STREET_1 , C_STREET_2 , C_CITY )=(SELECT W_STREET_1 , W_STREET_2 , W_CITY FROM TPCC.WAREHOUSE WHERE W_ID=8)
FROM fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_FROM_000 FROM_DD , fvt_data_partition_scan.WITH_UPDATE_TABLE_001 WITH_CC
WHERE UPDATE_059.C_ID BETWEEN (SELECT W_ID*80 FROM TPCC.WAREHOUSE  WHERE W_ID=10 ) AND 1000;
SELECT C_ID , C_D_ID , C_W_ID , C_FIRST , C_MIDDLE , C_LAST , C_STREET_1 , C_STREET_2 , C_CITY FROM  fvt_data_partition_scan.UPDATE_PARTITION_PICT_TABLE_059  ORDER BY C_ID , C_D_ID , C_W_ID , C_FIRST , C_MIDDLE;

-- Clean up
DROP SCHEMA fvt_data_partition_scan CASCADE;
DROP SCHEMA TPCC CASCADE;

CREATE TABLE hw_partition_scan_tenk3 (
    a integer,
    b timestamp without time zone
)
DISTRIBUTE BY HASH (a)
PARTITION BY RANGE (a)
(
    PARTITION p0 VALUES LESS THAN (100),
    PARTITION p1 VALUES LESS THAN (200),
    PARTITION p2 VALUES LESS THAN (300)
);

COPY hw_partition_scan_tenk3 (a, b) FROM stdin;
10	2015-04-25 18:41:30.483823
20	2015-04-25 18:41:30.489231
30	2015-04-30 18:41:30.482444
40	2015-04-22 18:41:30.487192
50	2015-04-27 18:41:30.48514
60	2015-04-22 18:41:30.480243
\.
;

CREATE INDEX hw_partition_scan_tenk3_idx_1 ON hw_partition_scan_tenk3 USING btree (b) LOCAL(PARTITION p0_b_idx, PARTITION p1_b_idx, PARTITION p2_b_idx) ;

ANALYZE hw_partition_scan_tenk3;

SET ENABLE_FAST_QUERY_SHIPPING = OFF;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;

SELECT * FROM HW_PARTITION_SCAN_TENK3 WHERE B > TO_DATE('2015-04-22 00:00:00','YYYY-MM-DD HH24:MI:SS') ORDER BY 1;

DROP TABLE HW_PARTITION_SCAN_TENK3;
