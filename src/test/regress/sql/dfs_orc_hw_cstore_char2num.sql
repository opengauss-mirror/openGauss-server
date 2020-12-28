set enable_global_stats = true;

CREATE TABLE char_to_num_row (
    id int,
    a char(30),
    b varchar(30),
	c bpchar
);

CREATE TABLE char_to_num_row1 (
    id int,
    a char(30),
    b varchar(30),
	c bpchar
);

CREATE TABLE char_to_num_col (
    id int,
    a char(30),
    b varchar(30),
	c bpchar
)with ( orientation = orc ) tablespace hdfs_ts;

--ok
INSERT INTO char_to_num_row VALUES (1, '123', '456', '789');
INSERT INTO char_to_num_row VALUES (2, '123  ', '456', '789');
INSERT INTO char_to_num_row VALUES (3, NULL, NULL, '789');
INSERT INTO char_to_num_row VALUES (3, '1230', '4560', '789');
INSERT INTO char_to_num_row VALUES (3, '', '', '789');

INSERT INTO char_to_num_col select * from char_to_num_row;

--error
INSERT INTO char_to_num_col VALUES (4, 'abc', 'abc', '789');
INSERT INTO char_to_num_col VALUES (5, '0123', '0120', '789');
INSERT INTO char_to_num_col VALUES (6, ' 123', ' 120', '789');
INSERT INTO char_to_num_col VALUES (7, 'a123', '+123', '789');
INSERT INTO char_to_num_col VALUES (8, '1234567890123456789012', '1234567890123456789012', '789');
INSERT INTO char_to_num_col VALUES (8, '12345678901234567890123456', '12345678901234567890123456', '789');
INSERT INTO char_to_num_col VALUES (9, '123  12', '123  ', '789');
INSERT INTO char_to_num_col VALUES (9, ' ', ' ', '789');

INSERT INTO char_to_num_row1 VALUES (10, '123', '456', '789');
INSERT INTO char_to_num_row1 VALUES (11, '123.', '45 6', '789');

INSERT INTO char_to_num_col select * from char_to_num_row1;


SELECT * FROM char_to_num_col ORDER BY id;

-- delete a row with a TOASTed value
--DELETE FROM char_to_num_col;

DROP TABLE char_to_num_row;
DROP TABLE char_to_num_row1;
DROP TABLE char_to_num_col;

-- empty string test
CREATE TABLE char_to_num_00 ( a int, b varchar(10) ) with ( orientation = orc ) tablespace hdfs_ts;
COPY char_to_num_00 FROM stdin with ( NULL 'null' );
1	
1	null
1	''
1	''
1	''
1	''
1	'1'
1	'2'
1	'33'
1	'44'
1	
1	''
\.
SELECT * FROM char_to_num_00 ORDER BY 2;
DROP TABLE char_to_num_00;

-- need to update source buffer size when decompression
CREATE TABLE char_to_num_01 ( a int , b char(11) );
INSERT INTO char_to_num_01 VALUES(1, '2'), (1, '187040'), (1, '3'), (1, '4'), (1, '5'), (1, '6'), (1, '7'), (1, '8'), (1, '9');
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
INSERT INTO char_to_num_01 SELECT * FROM char_to_num_01;
CREATE TABLE char_to_num_02 ( a int , b char(11) ) with ( orientation = orc ) tablespace hdfs_ts;
INSERT INTO char_to_num_02 SELECT * FROM char_to_num_01 ORDER BY 2;
\o char_to_num_02.dat
select * from char_to_num_02 order by 2;
\! rm char_to_num_02.dat
\o
select COUNT(*) from char_to_num_02;
DROP TABLE char_to_num_02, char_to_num_01;

-- assert failed
CREATE TABLE char_to_num_03 (id int, a char(22) ) with (orientation = orc ) tablespace hdfs_ts;
COPY char_to_num_03 FROM STDIN;
1	19953730129049907919
1	199537301290499079199
1	19953730129049907919
1	19953730129049907919
1	19953730129049907919
1	19953730129049907918
1	19953730129049907917
\.
SELECT * FROM char_to_num_03 ORDER BY 2;
DROP TABLE char_to_num_03;
-- uint64 overflow
CREATE TABLE char_to_num_04 (id int, a varchar(22) ) with (orientation = orc ) tablespace hdfs_ts;
COPY char_to_num_04 FROM STDIN;
1	19953730129049907919
1	199537301290499079199
1	19953730129049907919
1	19953730129049907919
1	19953730129049907919
1	19953730129049907918
1	19953730129049907917
\.
SELECT * FROM char_to_num_04 ORDER BY 2;
DROP TABLE char_to_num_04;
