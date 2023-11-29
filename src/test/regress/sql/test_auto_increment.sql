-- only supported in b database
CREATE TABLE test_create_autoinc_err(id int auto_increment unique key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err(id int, name varchar(200),a int) AUTO_INCREMENT=100;
ALTER TABLE test_create_autoinc_err AUTO_INCREMENT=100;
-- create b db
create database autoinc_b_db with dbcompatibility = 'B';
\c autoinc_b_db
-- test CREATE TABLE with AUTO_INCREMENT
-- test create table
CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT INDEX,
    b varchar(32)
); -- ERROR

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT KEY,
    b varchar(32)
); -- ERROR

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT PRIMARY KEY,
    b varchar(32)
);
\d test_create_autoinc;
DROP TABLE test_create_autoinc;

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT UNIQUE KEY,
    b varchar(32)
);
DROP TABLE test_create_autoinc;

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT UNIQUE,
    b varchar(32)
);
\d test_create_autoinc;
DROP TABLE test_create_autoinc;

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT,
    b varchar(32),
    UNIQUE ((b||'1'),a)
); -- ERROR

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT,
    b varchar(32),
    UNIQUE (a,b)
);
\d test_create_autoinc;
DROP TABLE test_create_autoinc;

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT,
    b varchar(32),
    PRIMARY KEY ((b||'1'),a)
); -- ERROR

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT,
    b varchar(32),
    PRIMARY KEY (a,b)
);
\d test_create_autoinc;
DROP TABLE test_create_autoinc;

CREATE TABLE test_create_autoinc(
    a int AUTO_INCREMENT,
    b varchar(32),
    KEY (b,a)
); -- ERROR

-- constraint error
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key CHECK (id < 500), name varchar(200),a int);
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int CHECK ((id + a) < 500));
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key DEFAULT 100, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key GENERATED ALWAYS AS (a+1) STORED, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int GENERATED ALWAYS AS (id+1) STORED);
CREATE TABLE test_create_autoinc_err(id int GENERATED ALWAYS AS (a+1) STORED, name varchar(200),a int auto_increment primary key);

--auto_increment value error
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=-1;
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=0; 
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=170141183460469231731687303715884105728;
CREATE TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=1.1;
CREATE TEMPORARY TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=-1;
CREATE TEMPORARY TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=0; 
CREATE TEMPORARY TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=170141183460469231731687303715884105728;
CREATE TEMPORARY TABLE test_create_autoinc_err(id int auto_increment primary key, name varchar(200),a int) auto_increment=1.1;
-- datatype error
CREATE TABLE test_create_autoinc_err1(id SERIAL auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id DECIMAL(10,4) auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id NUMERIC(10,4) auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id text auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id oid auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id int[] auto_increment primary key, name varchar(200),a int);
CREATE TABLE test_create_autoinc_err1(id int16 auto_increment, name varchar(200),a int, unique(id)) auto_increment=170141183460469231731687303715884105727;
-- table type error
CREATE TABLE test_create_autoinc_err(id INTEGER auto_increment, name varchar(200),a int, primary key(id)) with (ORIENTATION=column);
CREATE TABLE test_create_autoinc_err(id INTEGER auto_increment, name varchar(200),a int, primary key(id)) with (ORIENTATION=orc);
CREATE TABLE test_create_autoinc_err(id INTEGER auto_increment, name varchar(200),a int, primary key(id)) with (ORIENTATION=timeseries);
CREATE FOREIGN TABLE t_table_0020 (
  col01 integer AUTO_INCREMENT NOT NULL,
  col02 float,
  col03 int,
  PRIMARY KEY (col01,col02,col03)
) server gsmpp_server;
--test different type with auto_increment start value
CREATE TABLE test_create_autoinc(id bool auto_increment primary key, name varchar(200),a int) auto_increment=1;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id TINYINT primary key auto_increment, name varchar(200),a int) auto_increment=255;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id SMALLINT auto_increment unique, name varchar(200),a int) auto_increment=32767;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id INTEGER auto_increment, name varchar(200),a int, primary key(id)) auto_increment=2147483647;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id BIGINT auto_increment, name varchar(200),a int, primary key(id, name)) auto_increment=9223372036854775807;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id REAL auto_increment, name varchar(200),a int, unique(id, name)) auto_increment=9223372036854775807;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id DOUBLE PRECISION auto_increment, name varchar(200),a int, unique(id)) auto_increment=9223372036854775807;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;
CREATE TABLE test_create_autoinc(id FLOAT(50) auto_increment, name varchar(200),a int, unique(id)) auto_increment=9223372036854775807;
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc VALUES(DEFAULT,'Gauss',0);
SELECT pg_catalog.pg_get_tabledef('test_create_autoinc');
DROP TABLE test_create_autoinc;

--test CREATE TABLE LIKE with AUTO_INCREMENT
CREATE TABLE test_create_autoinc_source(id int auto_increment primary key) AUTO_INCREMENT = 100;
INSERT INTO test_create_autoinc_source VALUES(DEFAULT);
INSERT INTO test_create_autoinc_source VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_source ORDER BY 1;
--error
CREATE TABLE test_create_autoinc_like_err(LIKE test_create_autoinc_source);
CREATE TABLE test_create_autoinc_like_err(LIKE test_create_autoinc_source INCLUDING INDEXES, a int auto_increment);
CREATE TABLE test_create_autoinc_like_err(a int auto_increment, LIKE test_create_autoinc_source INCLUDING INDEXES);
CREATE TABLE test_create_autoinc_like_err(a int auto_increment primary key, LIKE test_create_autoinc_source);
CREATE TABLE test_create_autoinc_like_err(LIKE test_create_autoinc_source INCLUDING INDEXES) with (ORIENTATION=column);
CREATE TABLE test_create_autoinc_like_err(LIKE test_create_autoinc_source INCLUDING INDEXES, a int GENERATED ALWAYS AS (id+1) STORED);
--row table
CREATE TABLE test_create_autoinc_like(LIKE test_create_autoinc_source INCLUDING INDEXES);
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
ALTER TABLE test_create_autoinc_like AUTO_INCREMENT=200;
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like ORDER BY 1;
TRUNCATE TABLE test_create_autoinc_like;
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like ORDER BY 1;
DROP TABLE test_create_autoinc_like;
CREATE TABLE test_create_autoinc_like(LIKE test_create_autoinc_source INCLUDING INDEXES) AUTO_INCREMENT=100;
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
ALTER TABLE test_create_autoinc_like AUTO_INCREMENT=200;
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like ORDER BY 1;
TRUNCATE TABLE test_create_autoinc_like;
INSERT INTO test_create_autoinc_like VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like ORDER BY 1;
DROP TABLE test_create_autoinc_like;
--local temp table
CREATE TEMPORARY TABLE test_create_autoinc_like_temp(LIKE test_create_autoinc_source INCLUDING INDEXES);
INSERT INTO test_create_autoinc_like_temp VALUES(0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
ALTER TABLE test_create_autoinc_like_temp AUTO_INCREMENT=200;
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like_temp ORDER BY 1;
TRUNCATE TABLE test_create_autoinc_like_temp;
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like_temp ORDER BY 1;
DROP TABLE test_create_autoinc_like_temp;
CREATE TEMPORARY TABLE test_create_autoinc_like_temp(LIKE test_create_autoinc_source INCLUDING INDEXES) AUTO_INCREMENT=100;
INSERT INTO test_create_autoinc_like_temp VALUES(0);
SELECT LAST_INSERT_ID();
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
ALTER TABLE test_create_autoinc_like_temp AUTO_INCREMENT=200;
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like_temp ORDER BY 1;
TRUNCATE TABLE test_create_autoinc_like_temp;
INSERT INTO test_create_autoinc_like_temp VALUES(DEFAULT);
SELECT id FROM test_create_autoinc_like_temp ORDER BY 1;
DROP TABLE test_create_autoinc_like_temp;

DROP TABLE test_create_autoinc_source;

--test ALTER TABLE ADD COLUMN AUTO_INCREMENT
--cstore is not supported
CREATE TABLE test_alter_autoinc_col(col int) with (ORIENTATION=column);
INSERT INTO test_alter_autoinc_col VALUES(1);
ALTER TABLE test_alter_autoinc_col ADD COLUMN id int AUTO_INCREMENT primary key;
DROP TABLE test_alter_autoinc_col;
-- auto_increment and generated column
CREATE TABLE test_alter_autoinc(col int);
ALTER TABLE test_alter_autoinc ADD COLUMN a int GENERATED ALWAYS AS (b+1), ADD COLUMN b int auto_increment primary key;
ALTER TABLE test_alter_autoinc ADD COLUMN a int auto_increment primary key, ADD COLUMN b int GENERATED ALWAYS AS (a+1);
DROP TABLE test_alter_autoinc;
--astore with data
CREATE TABLE test_alter_autoinc(col int);
INSERT INTO test_alter_autoinc VALUES(1);
INSERT INTO test_alter_autoinc VALUES(2);
ALTER TABLE test_alter_autoinc ADD COLUMN id int AUTO_INCREMENT; -- ERROR
ALTER TABLE test_alter_autoinc ADD COLUMN id int AUTO_INCREMENT primary key;
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
insert into test_alter_autoinc(col) values (3),(4),(5);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
insert into test_alter_autoinc values (1, 1);
insert into test_alter_autoinc values (6, 0);
select last_insert_id();
ALTER TABLE test_alter_autoinc AUTO_INCREMENT = 1000;
INSERT INTO test_alter_autoinc VALUES(3,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
TRUNCATE TABLE test_alter_autoinc;
INSERT INTO test_alter_autoinc VALUES(4,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc DROP COLUMN id;
SELECT col FROM test_alter_autoinc ORDER BY 1;

ALTER TABLE test_alter_autoinc ADD id int AUTO_INCREMENT, ADD primary key(id), AUTO_INCREMENT = 100;
INSERT INTO test_alter_autoinc VALUES(5,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc DROP COLUMN id, ADD id int AUTO_INCREMENT, ADD primary key(id), AUTO_INCREMENT = 200;
INSERT INTO test_alter_autoinc VALUES(6,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
SELECT pg_catalog.pg_get_tabledef('test_alter_autoinc');
-- error
ALTER TABLE test_alter_autoinc ADD COLUMN new_id int AUTO_INCREMENT;
ALTER TABLE test_alter_autoinc ADD COLUMN new_id int AUTO_INCREMENT UNIQUE;
ALTER TABLE test_alter_autoinc ALTER COLUMN id DROP DEFAULT;
ALTER TABLE test_alter_autoinc DROP COLUMN id, ADD new_id NUMERIC(10,4) AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE test_alter_autoinc DROP CONSTRAINT test_alter_autoinc_pkey;
ALTER TABLE test_alter_autoinc auto_increment=-1;
ALTER TABLE test_alter_autoinc auto_increment=1701411834604692317316873037158841057278;
ALTER TABLE test_alter_autoinc auto_increment=1.1;
ALTER LARGE SEQUENCE test_alter_autoinc_id_seq1 RESTART;
ALTER LARGE SEQUENCE test_alter_autoinc_id_seq1 maxvalue 90;
ALTER LARGE SEQUENCE test_alter_autoinc_id_seq1 OWNED BY test_alter_autoinc.col;
ALTER LARGE SEQUENCE test_alter_autoinc_id_seq1 AUTO_INCREMENT = 100;
select nextval('test_alter_autoinc_id_seq1');
select nextval('test_alter_autoinc_id_seq1'::regclass);
select setval('test_alter_autoinc_id_seq1'::regclass, 1000);
select setval('test_alter_autoinc_id_seq1'::regclass, 1000, true);
DROP SEQUENCE test_alter_autoinc_id_seq1;
DROP LARGE SEQUENCE test_alter_autoinc_id_seq1 CASCADE;
-- supplementary tests
ALTER TABLE test_alter_autoinc AUTO_INCREMENT = 1000;
INSERT INTO test_alter_autoinc VALUES(7,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc DROP COLUMN id, ADD id int AUTO_INCREMENT UNIQUE;
INSERT INTO test_alter_autoinc VALUES(8,DEFAULT);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
DROP TABLE test_alter_autoinc;

--test alter table add AUTO_INCREMENT NULL UNIQUE
CREATE TABLE test_alter_autoinc(col int);
INSERT INTO test_alter_autoinc VALUES(1);
INSERT INTO test_alter_autoinc VALUES(2);
ALTER TABLE test_alter_autoinc ADD COLUMN id int AUTO_INCREMENT NULL UNIQUE;
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
INSERT INTO test_alter_autoinc VALUES(3,NULL);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
INSERT INTO test_alter_autoinc VALUES(4,0);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
DROP TABLE test_alter_autoinc;

--test alter table add NULL AUTO_INCREMENT UNIQUE
CREATE TABLE test_alter_autoinc(col int);
INSERT INTO test_alter_autoinc VALUES(1);
INSERT INTO test_alter_autoinc VALUES(2);
ALTER TABLE test_alter_autoinc ADD COLUMN id int NULL AUTO_INCREMENT UNIQUE;
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
INSERT INTO test_alter_autoinc VALUES(3,NULL);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
INSERT INTO test_alter_autoinc VALUES(4,0);
SELECT id, col FROM test_alter_autoinc ORDER BY 1, 2;
DROP TABLE test_alter_autoinc;

--local temp table with data
CREATE TEMPORARY TABLE test_alter_autoinc_ltemp(col int);
INSERT INTO test_alter_autoinc_ltemp VALUES(1);
INSERT INTO test_alter_autoinc_ltemp VALUES(2);
ALTER TABLE test_alter_autoinc_ltemp ADD id int AUTO_INCREMENT, ADD primary key(id), AUTO_INCREMENT = 100;
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
INSERT INTO test_alter_autoinc_ltemp VALUES(3,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO test_alter_autoinc_ltemp VALUES(4,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp DROP COLUMN id, ADD id1 int AUTO_INCREMENT, ADD primary key(id1), AUTO_INCREMENT = 200;
INSERT INTO test_alter_autoinc_ltemp VALUES(5,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id1, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp DROP COLUMN id1, ADD id int AUTO_INCREMENT UNIQUE;
INSERT INTO test_alter_autoinc_ltemp VALUES(6,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp ADD CONSTRAINT test_alter_autoinc_ltemp_u1 UNIQUE(id);
ALTER TABLE test_alter_autoinc_ltemp ADD CONSTRAINT test_alter_autoinc_ltemp_u2 UNIQUE(id);
ALTER TABLE test_alter_autoinc_ltemp DROP CONSTRAINT test_alter_autoinc_ltemp_id_key;
INSERT INTO test_alter_autoinc_ltemp VALUES(7,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp DROP CONSTRAINT test_alter_autoinc_ltemp_u2;
INSERT INTO test_alter_autoinc_ltemp VALUES(8,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp MODIFY col NUMERIC(10,4);
INSERT INTO test_alter_autoinc_ltemp VALUES(9,DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id, col FROM test_alter_autoinc_ltemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_ltemp DROP COLUMN col;
INSERT INTO test_alter_autoinc_ltemp VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT id FROM test_alter_autoinc_ltemp ORDER BY 1;
ALTER TABLE test_alter_autoinc_ltemp AUTO_INCREMENT=100;
INSERT INTO test_alter_autoinc_ltemp VALUES(DEFAULT);
SELECT id FROM test_alter_autoinc_ltemp ORDER BY 1;
TRUNCATE TABLE test_alter_autoinc_ltemp;
INSERT INTO test_alter_autoinc_ltemp VALUES(DEFAULT);
SELECT id FROM test_alter_autoinc_ltemp ORDER BY 1;
-- error
ALTER TABLE test_alter_autoinc_ltemp ALTER COLUMN id DROP DEFAULT;
ALTER TABLE test_alter_autoinc_ltemp DROP COLUMN id, ADD new_id NUMERIC(10,4) AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE test_alter_autoinc_ltemp DROP CONSTRAINT test_alter_autoinc_ltemp_u1;
ALTER TABLE test_alter_autoinc_ltemp auto_increment=-1;
ALTER TABLE test_alter_autoinc_ltemp auto_increment=1701411834604692317316873037158841057278;
ALTER TABLE test_alter_autoinc_ltemp auto_increment=1.1;
DROP TABLE test_alter_autoinc_ltemp;
--global temp table with data
CREATE GLOBAL TEMPORARY TABLE test_alter_autoinc_gtemp(col int);
INSERT INTO test_alter_autoinc_gtemp VALUES(1);
INSERT INTO test_alter_autoinc_gtemp VALUES(2);
ALTER TABLE test_alter_autoinc_gtemp ADD id int AUTO_INCREMENT, ADD primary key(id), AUTO_INCREMENT = 100;
SELECT id, col FROM test_alter_autoinc_gtemp ORDER BY 1, 2;
ALTER TABLE test_alter_autoinc_gtemp AUTO_INCREMENT = 1000;
INSERT INTO test_alter_autoinc_gtemp VALUES(3,DEFAULT);
SELECT id, col FROM test_alter_autoinc_gtemp ORDER BY 1, 2;
TRUNCATE TABLE test_alter_autoinc_gtemp;
INSERT INTO test_alter_autoinc_gtemp VALUES(4,DEFAULT);
SELECT id, col FROM test_alter_autoinc_gtemp ORDER BY 1, 2;
DROP TABLE test_alter_autoinc_gtemp;
--part table with data
CREATE TABLE test_alter_partition_autoinc (
    col2 INT NOT NULL,
    col3 INT NOT NULL
) PARTITION BY RANGE (col2) SUBPARTITION BY HASH (col3) (
    PARTITION col1_less_1000 VALUES LESS THAN(1000)
    (
        SUBPARTITION p1_col2_hash1,
        SUBPARTITION p1_col2_hash2
    ),
    PARTITION col1_mid_1000 VALUES LESS THAN(2000)
    (
        SUBPARTITION p2_col2_hash1,
        SUBPARTITION p2_col2_hash2
    ),
    PARTITION col1_greater_2000 VALUES LESS THAN (MAXVALUE)
    (
        SUBPARTITION p3_col2_hash1,
        SUBPARTITION p3_col2_hash2
    )
);
INSERT INTO test_alter_partition_autoinc VALUES(1,1);
INSERT INTO test_alter_partition_autoinc VALUES(2001,2001);
ALTER TABLE test_alter_partition_autoinc ADD col1 int AUTO_INCREMENT, ADD UNIQUE(col1, col2, col3), AUTO_INCREMENT = 100;
SELECT col1, col2, col3 FROM test_alter_partition_autoinc ORDER BY 1, 2;
ALTER TABLE test_alter_partition_autoinc AUTO_INCREMENT = 1000;
INSERT INTO test_alter_partition_autoinc VALUES(3001,3001,DEFAULT);
SELECT col1, col2, col3 FROM test_alter_partition_autoinc ORDER BY 1, 2;
TRUNCATE TABLE test_alter_partition_autoinc;
INSERT INTO test_alter_partition_autoinc VALUES(1,1,DEFAULT);
SELECT col1, col2, col3 FROM test_alter_partition_autoinc ORDER BY 1, 2;
DROP TABLE test_alter_partition_autoinc;

-- test alter table add column, add/drop index
CREATE TABLE test_alter_autoinc(
    a int,
    b varchar(32)
);
ALTER TABLE test_alter_autoinc ADD COLUMN seq int AUTO_INCREMENT, ADD CONSTRAINT test_alter_autoinc_uk UNIQUE (a, seq); -- ERROR
ALTER TABLE test_alter_autoinc ADD COLUMN seq int AUTO_INCREMENT, ADD CONSTRAINT test_alter_autoinc_uk UNIQUE (seq);
CREATE INDEX test_alter_autoinc_idx1 ON test_alter_autoinc (seq,a);
SELECT pg_get_tabledef('test_alter_autoinc'::regclass);
ALTER TABLE test_alter_autoinc DROP CONSTRAINT test_alter_autoinc_uk; -- ERROR
DROP INDEX test_alter_autoinc_idx1;
ALTER TABLE test_alter_autoinc DROP CONSTRAINT test_alter_autoinc_uk, ADD CONSTRAINT test_alter_autoinc_pk PRIMARY KEY (seq);
ALTER TABLE test_alter_autoinc DROP CONSTRAINT test_alter_autoinc_pk; -- ERROR
DROP TABLE test_alter_autoinc;

-- auto_increment in table with single column PRIMARY KEY
CREATE TABLE single_autoinc_pk(col int auto_increment PRIMARY KEY) AUTO_INCREMENT = 10;
INSERT INTO single_autoinc_pk VALUES(NULL);
SELECT LAST_INSERT_ID();
INSERT INTO single_autoinc_pk VALUES(1 - 1);
SELECT LAST_INSERT_ID();
INSERT INTO single_autoinc_pk VALUES(100);
SELECT LAST_INSERT_ID();
INSERT INTO single_autoinc_pk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
ALTER TABLE single_autoinc_pk AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO single_autoinc_pk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_pk ORDER BY 1;
UPDATE single_autoinc_pk SET col=NULL WHERE col=10;
UPDATE single_autoinc_pk SET col=0 WHERE col=11;
SELECT col FROM single_autoinc_pk ORDER BY 1;
UPDATE single_autoinc_pk SET col=DEFAULT WHERE col=100;
UPDATE single_autoinc_pk SET col=3000 WHERE col=0;
SELECT LAST_INSERT_ID();
INSERT INTO single_autoinc_pk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_pk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_pk');
ALTER TABLE single_autoinc_pk ADD CONSTRAINT test_alter_single_autoinc_pk_u1 UNIQUE(col);
ALTER TABLE single_autoinc_pk ADD CONSTRAINT test_alter_single_autoinc_pk_u2 UNIQUE(col);
SELECT pg_catalog.pg_get_tabledef('single_autoinc_pk');
ALTER TABLE single_autoinc_pk DROP CONSTRAINT test_alter_single_autoinc_pk_u2;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_pk');
DROP TABLE single_autoinc_pk;

-- auto_increment in table with single column NULL UNIQUE
CREATE TABLE single_autoinc_uk(col int auto_increment NULL UNIQUE) AUTO_INCREMENT = 10;
INSERT INTO single_autoinc_uk VALUES(NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(1 - 1);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(100);
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
ALTER TABLE single_autoinc_uk AUTO_INCREMENT = 1000;
INSERT INTO single_autoinc_uk VALUES(0);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=NULL WHERE col=11;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=0 WHERE col=100;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=DEFAULT WHERE col=1000;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=3000 WHERE col=0;
INSERT INTO single_autoinc_uk VALUES(0);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_uk');
DROP TABLE single_autoinc_uk;

-- auto_increment in table with single column NULL auto_increment UNIQUE
CREATE TABLE single_autoinc_uk(col int NULL auto_increment UNIQUE KEY) AUTO_INCREMENT = 10;
INSERT INTO single_autoinc_uk VALUES(NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(1 - 1);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(100);
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_uk');
DROP TABLE single_autoinc_uk;

-- auto_increment in table with single column auto_increment UNIQUE
CREATE TABLE single_autoinc_uk(col int auto_increment UNIQUE KEY) AUTO_INCREMENT = 10;
INSERT INTO single_autoinc_uk VALUES(NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(1 - 1);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(100);
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_uk');
DROP TABLE single_autoinc_uk;

-- test auto_increment with rollback
CREATE TABLE single_autoinc_rollback(col int auto_increment PRIMARY KEY) AUTO_INCREMENT = 10;

begin transaction;
INSERT INTO single_autoinc_rollback VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_rollback ORDER BY 1;
rollback;
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_rollback ORDER BY 1;

begin transaction;
INSERT INTO single_autoinc_rollback VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_rollback ORDER BY 1;
commit;

begin transaction;
INSERT INTO single_autoinc_rollback VALUES(4000);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_rollback ORDER BY 1;
rollback;
SELECT col FROM single_autoinc_rollback ORDER BY 1;

begin transaction;
INSERT INTO single_autoinc_rollback VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_rollback ORDER BY 1;
commit;

DROP TABLE single_autoinc_rollback;

--test auto_increment if duplcate key error
create table test_autoinc_duplicate_err
(
    id     int auto_increment primary key,
    gender int         null,
    name   varchar(50) null,
    unique (gender,name)
);
insert into test_autoinc_duplicate_err (gender,name) values (1,'Gauss');
SELECT LAST_INSERT_ID();
insert into test_autoinc_duplicate_err (gender,name) values (1,'Gauss');
SELECT LAST_INSERT_ID();
insert into test_autoinc_duplicate_err (gender,name) values (1,'Euler');
SELECT LAST_INSERT_ID();
select * from test_autoinc_duplicate_err order by id;
insert into test_autoinc_duplicate_err (id,gender,name) values (10,1,'Gauss');
select * from test_autoinc_duplicate_err order by id;
insert into test_autoinc_duplicate_err (gender,name) values (1,'Newton');
select * from test_autoinc_duplicate_err order by id;

drop table test_autoinc_duplicate_err;

--test auto_increment with check error
CREATE TABLE test_autoinc_with_check(
    col1 int auto_increment primary key, col2 int, CHECK (col2 >0)
);
INSERT INTO test_autoinc_with_check VALUES(DEFAULT, 1);
INSERT INTO test_autoinc_with_check VALUES(DEFAULT, -1);
INSERT INTO test_autoinc_with_check VALUES(DEFAULT, 2);
SELECT col1,col2 FROM test_autoinc_with_check ORDER BY 1;
DROP TABLE test_autoinc_with_check;

--test auto_increment with trigger
create table test_autoinc_trigger
(
    id     int auto_increment primary key,
    gender int         null,
    name   varchar(50) null,
    unique (gender,name)
);
insert into test_autoinc_trigger (gender,name) values (1,'Gauss');
insert into test_autoinc_trigger (gender,name) values (1,'Euler');
select * from test_autoinc_trigger order by id;

CREATE OR REPLACE FUNCTION tg_bf_insert_autoinc_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    update test_autoinc_trigger set id= 3 where name='Gauss';
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_bf_insert_autoinc
BEFORE INSERT ON test_autoinc_trigger
FOR EACH ROW
EXECUTE PROCEDURE tg_bf_insert_autoinc_func();

insert into test_autoinc_trigger (gender, name) values (1,'Newton');
select * from test_autoinc_trigger order by id;
drop trigger tg_bf_insert_autoinc ON test_autoinc_trigger;
drop FUNCTION tg_bf_insert_autoinc_func;


create table tmp_tg_table(tmp_gender int UNIQUE);

CREATE OR REPLACE FUNCTION tg_bf_insert_autoinc_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    insert into test_autoinc_trigger (gender,name) values (new.tmp_gender,'Fields');
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_bf_insert_autoinc
BEFORE INSERT ON tmp_tg_table
FOR EACH ROW
EXECUTE PROCEDURE tg_bf_insert_autoinc_func();

insert into tmp_tg_table values(1);

SELECT LAST_INSERT_ID();
select * from test_autoinc_trigger order by id;
insert into test_autoinc_trigger (gender,name) values (1,'Turing');
SELECT LAST_INSERT_ID();
select * from test_autoinc_trigger order by id;

drop trigger tg_bf_insert_autoinc ON tmp_tg_table;
drop FUNCTION tg_bf_insert_autoinc_func;

CREATE OR REPLACE FUNCTION tg_af_insert_autoinc_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
    insert into tmp_tg_table values (1);
    RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER tg_af_insert_autoinc
AFTER INSERT ON test_autoinc_trigger
FOR EACH ROW
EXECUTE PROCEDURE tg_af_insert_autoinc_func();

insert into test_autoinc_trigger (id,gender,name) values (100,1,'Shannon');
select * from test_autoinc_trigger order by id;
drop trigger tg_af_insert_autoinc ON test_autoinc_trigger;
drop FUNCTION tg_af_insert_autoinc_func;

insert into test_autoinc_trigger (gender,name) values (1,'Fermat');
select * from test_autoinc_trigger order by id;

drop table tmp_tg_table;
drop table test_autoinc_trigger;

-- auto_increment in ustore table PRIMARY KEY
CREATE TABLE ustore_single_autoinc(col int auto_increment PRIMARY KEY) WITH (STORAGE_TYPE=USTORE);
INSERT INTO ustore_single_autoinc VALUES(NULL);
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(1 - 1);
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(100);
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
ALTER TABLE ustore_single_autoinc AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
INSERT INTO ustore_single_autoinc select col-col from ustore_single_autoinc;
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
INSERT INTO ustore_single_autoinc VALUES(500);
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(-1);
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(2100), (NULL), (2000), (DEFAULT), (2200), (NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
UPDATE ustore_single_autoinc SET col=NULL WHERE col=2201;
SELECT col FROM ustore_single_autoinc ORDER BY 1;
UPDATE ustore_single_autoinc SET col=0 WHERE col=2200;
SELECT col FROM ustore_single_autoinc ORDER BY 1;
UPDATE ustore_single_autoinc SET col=DEFAULT WHERE col=2101;
UPDATE ustore_single_autoinc SET col=3000 WHERE col=-1;
SELECT LAST_INSERT_ID();
INSERT INTO ustore_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
COPY ustore_single_autoinc (col) from stdin;
3003
3002
3004
\.
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('ustore_single_autoinc');
TRUNCATE TABLE ustore_single_autoinc;
INSERT INTO ustore_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ustore_single_autoinc ORDER BY 1;
DROP TABLE ustore_single_autoinc;

-- auto_increment in local temp table PRIMARY KEY
CREATE TEMPORARY TABLE ltemp_single_autoinc(col int auto_increment PRIMARY KEY);
INSERT INTO ltemp_single_autoinc VALUES(NULL);
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(1 - 1);
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(100);
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
ALTER TABLE ltemp_single_autoinc AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
INSERT INTO ltemp_single_autoinc select col-col from ltemp_single_autoinc;
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
INSERT INTO ltemp_single_autoinc VALUES(500);
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(-1);
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(2100), (NULL), (2000), (DEFAULT), (2200), (NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
UPDATE ltemp_single_autoinc SET col=NULL WHERE col=2201;
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
UPDATE ltemp_single_autoinc SET col=0 WHERE col=2200;
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
UPDATE ltemp_single_autoinc SET col=DEFAULT WHERE col=2101;
UPDATE ltemp_single_autoinc SET col=3000 WHERE col=-1;
SELECT LAST_INSERT_ID();
INSERT INTO ltemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
COPY ltemp_single_autoinc (col) from stdin;
3003
3002
3004
\.
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('ltemp_single_autoinc');
TRUNCATE TABLE ltemp_single_autoinc;
INSERT INTO ltemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM ltemp_single_autoinc ORDER BY 1;
DROP TABLE ltemp_single_autoinc;

-- auto_increment in local temp table NULL UNIQUE
CREATE TEMPORARY TABLE single_autoinc_uk(col int auto_increment NULL UNIQUE) AUTO_INCREMENT = 10;
INSERT INTO single_autoinc_uk VALUES(NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(1 - 1);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(100);
SELECT col FROM single_autoinc_uk ORDER BY 1;
INSERT INTO single_autoinc_uk VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
ALTER TABLE single_autoinc_uk AUTO_INCREMENT = 1000;
INSERT INTO single_autoinc_uk VALUES(0);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=NULL WHERE col=11;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=0 WHERE col=100;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=DEFAULT WHERE col=1000;
SELECT col FROM single_autoinc_uk ORDER BY 1;
UPDATE single_autoinc_uk SET col=3000 WHERE col=0;
INSERT INTO single_autoinc_uk VALUES(0);
SELECT LAST_INSERT_ID();
SELECT col FROM single_autoinc_uk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('single_autoinc_uk');
DROP TABLE single_autoinc_uk;

--auto_increment in global temp table
CREATE GLOBAL TEMPORARY TABLE gtemp_single_autoinc(col int auto_increment PRIMARY KEY);
INSERT INTO gtemp_single_autoinc VALUES(NULL);
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(1 - 1);
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(100);
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
ALTER TABLE gtemp_single_autoinc AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
INSERT INTO gtemp_single_autoinc select col-col from gtemp_single_autoinc;
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
INSERT INTO gtemp_single_autoinc VALUES(500);
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(-1);
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(2100), (NULL), (2000), (DEFAULT), (2200), (NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
UPDATE gtemp_single_autoinc SET col=NULL WHERE col=2201;
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
UPDATE gtemp_single_autoinc SET col=0 WHERE col=2200;
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
UPDATE gtemp_single_autoinc SET col=DEFAULT WHERE col=2101;
UPDATE gtemp_single_autoinc SET col=3000 WHERE col=-1;
SELECT LAST_INSERT_ID();
INSERT INTO gtemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
COPY gtemp_single_autoinc (col) from stdin;
3003
3002
3004
\.
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('gtemp_single_autoinc');
TRUNCATE TABLE gtemp_single_autoinc;
INSERT INTO gtemp_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM gtemp_single_autoinc ORDER BY 1;
DROP TABLE gtemp_single_autoinc;

--auto_increment in unlogged table
CREATE UNLOGGED TABLE unlog_single_autoinc(col int auto_increment PRIMARY KEY);
INSERT INTO unlog_single_autoinc VALUES(NULL);
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(1 - 1);
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(100);
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
ALTER TABLE unlog_single_autoinc AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
INSERT INTO unlog_single_autoinc select col-col from unlog_single_autoinc;
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
INSERT INTO unlog_single_autoinc VALUES(500);
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(-1);
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(2100), (NULL), (2000), (DEFAULT), (2200), (NULL);
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
UPDATE unlog_single_autoinc SET col=NULL WHERE col=2201;
SELECT col FROM unlog_single_autoinc ORDER BY 1;
UPDATE unlog_single_autoinc SET col=0 WHERE col=2200;
SELECT col FROM unlog_single_autoinc ORDER BY 1;
UPDATE unlog_single_autoinc SET col=DEFAULT WHERE col=2101;
UPDATE unlog_single_autoinc SET col=3000 WHERE col=-1;
SELECT LAST_INSERT_ID();
INSERT INTO unlog_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
COPY unlog_single_autoinc (col) from stdin;
3003
3002
3004
\.
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('unlog_single_autoinc');
TRUNCATE TABLE unlog_single_autoinc;
INSERT INTO unlog_single_autoinc VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
SELECT col FROM unlog_single_autoinc ORDER BY 1;
DROP TABLE unlog_single_autoinc;

--auto_increment column as partkey primary key
CREATE TABLE test_part_autoinc_pk (
	col1 INT AUTO_INCREMENT,
    col2 INT NOT NULL,
    col3 INT NOT NULL,
    PRIMARY KEY(col1, col2)
) PARTITION BY RANGE (col1) SUBPARTITION BY HASH (col2) (
    PARTITION col1_less_1000 VALUES LESS THAN(1000)
    (
        SUBPARTITION p1_col2_hash1,
        SUBPARTITION p1_col2_hash2
    ),
    PARTITION col1_mid_1000 VALUES LESS THAN(2000)
    (
        SUBPARTITION p2_col2_hash1,
        SUBPARTITION p2_col2_hash2
    ),
    PARTITION col1_greater_2000 VALUES LESS THAN (MAXVALUE)
    (
        SUBPARTITION p3_col2_hash1,
        SUBPARTITION p3_col2_hash2
    )
);
INSERT INTO test_part_autoinc_pk VALUES(NULL, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(1 - 1, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(100, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(DEFAULT, 1, 1);
SELECT LAST_INSERT_ID();
ALTER TABLE test_part_autoinc_pk AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(DEFAULT, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
INSERT INTO test_part_autoinc_pk select col1-col1, col2, col3 from test_part_autoinc_pk;
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
INSERT INTO test_part_autoinc_pk VALUES(500, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(-1, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(2100, 1, 1), (NULL, 1, 1), (2000, 1, 1), (DEFAULT, 1, 1), (2200, 1, 1), (NULL, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
UPDATE test_part_autoinc_pk SET col1=NULL WHERE col1=2201;
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
UPDATE test_part_autoinc_pk SET col1=0 WHERE col1=2200;
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
UPDATE test_part_autoinc_pk SET col1=DEFAULT WHERE col1=2101;
UPDATE test_part_autoinc_pk SET col1=3000 WHERE col1=-1;
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_pk VALUES(DEFAULT, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
COPY test_part_autoinc_pk (col1,col2,col3) from stdin;
3003	1	1
999	1	1
1999	1	1
3004	1	1
\.
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('test_part_autoinc_pk');
TRUNCATE TABLE test_part_autoinc_pk;
INSERT INTO test_part_autoinc_pk VALUES(DEFAULT, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_pk ORDER BY 1;
DROP TABLE test_part_autoinc_pk;

--auto_increment column as unique key not partkey
CREATE TABLE test_part_autoinc_unique (
	col1 INT AUTO_INCREMENT NULL,
    col2 INT NOT NULL,
    col3 INT NOT NULL,
    UNIQUE(col1, col2, col3)
) PARTITION BY RANGE (col2) SUBPARTITION BY HASH (col3) (
    PARTITION col1_less_1000 VALUES LESS THAN(1000)
    (
        SUBPARTITION p1_col2_hash1,
        SUBPARTITION p1_col2_hash2
    ),
    PARTITION col1_mid_1000 VALUES LESS THAN(2000)
    (
        SUBPARTITION p2_col2_hash1,
        SUBPARTITION p2_col2_hash2
    ),
    PARTITION col1_greater_2000 VALUES LESS THAN (MAXVALUE)
    (
        SUBPARTITION p3_col2_hash1,
        SUBPARTITION p3_col2_hash2
    )
);
INSERT INTO test_part_autoinc_unique VALUES(NULL, 1, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(1 - 1, 1001, 1001);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(100, 2001, 2001);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique(col2,col3) VALUES(2001, 2001);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
ALTER TABLE test_part_autoinc_unique AUTO_INCREMENT = 1000;
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(0, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
INSERT INTO test_part_autoinc_unique select col1-col1, col2, col3 from test_part_autoinc_unique order by 1 NULLS FIRST;
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
INSERT INTO test_part_autoinc_unique VALUES(500, 1001, 1001);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(-1, 2001, 2001);
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(2100, 1, 1), (0, 1001, 1001), (2000, 2001, 2001), (NULL, 1, 1), (2200, 1001, 1001), (0, 2001, 2001);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
UPDATE test_part_autoinc_unique SET col1=NULL WHERE col1=2201;
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
UPDATE test_part_autoinc_unique SET col1=0 WHERE col1=2200;
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
UPDATE test_part_autoinc_unique SET col1=DEFAULT WHERE col1=2101;
SELECT LAST_INSERT_ID();
INSERT INTO test_part_autoinc_unique VALUES(0, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
COPY test_part_autoinc_unique (col1,col2,col3) from stdin;
3003	1001	1001
999	2001	2001
1999	1	1
3004	1001	1001
\.
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
SELECT pg_catalog.pg_get_tabledef('test_part_autoinc_unique');
TRUNCATE TABLE test_part_autoinc_unique;
INSERT INTO test_part_autoinc_unique VALUES(DEFAULT, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
INSERT INTO test_part_autoinc_unique VALUES(0, 1, 1);
SELECT LAST_INSERT_ID();
SELECT col1 FROM test_part_autoinc_unique ORDER BY 1;
DROP TABLE test_part_autoinc_unique;

--test insert into on duplicate AND merge into
CREATE TABLE test_autoinc_upsert(col int auto_increment UNIQUE, col1 INT);
CREATE TABLE test_autoinc_merge(col int auto_increment UNIQUE, col1 INT);
--insert test_autoinc_upsert
INSERT INTO test_autoinc_upsert VALUES(DEFAULT,0) ON DUPLICATE KEY UPDATE col1=col;
INSERT INTO test_autoinc_upsert VALUES(1,0) ON DUPLICATE KEY UPDATE col1=col;
INSERT INTO test_autoinc_upsert VALUES(2,0) ON DUPLICATE KEY UPDATE col1=col;
INSERT INTO test_autoinc_upsert VALUES(100,0) ON DUPLICATE KEY UPDATE col1=col;
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_upsert ORDER BY 1;
--all insert test_autoinc_merge
MERGE INTO test_autoinc_merge m USING test_autoinc_upsert u 
  ON m.col = u.col
WHEN MATCHED THEN
  UPDATE SET m.col1=u.col
WHEN NOT MATCHED THEN  
  INSERT VALUES (u.col, u.col1);
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;
--all update test_autoinc_merge
MERGE INTO test_autoinc_merge m USING test_autoinc_upsert u 
  ON m.col = u.col
WHEN MATCHED THEN
  UPDATE SET m.col1=u.col
WHEN NOT MATCHED THEN  
  INSERT VALUES (u.col, u.col1);
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;
--insert test_autoinc_upsert 
INSERT INTO test_autoinc_upsert VALUES(100,NULL) ON DUPLICATE KEY UPDATE col1=col;
INSERT INTO test_autoinc_upsert VALUES(DEFAULT,NULL) ON DUPLICATE KEY UPDATE col1=col;
INSERT INTO test_autoinc_upsert VALUES(50,NULL) ON DUPLICATE KEY UPDATE col1=col;
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_upsert ORDER BY 1;
--insert and update test_autoinc_merge
MERGE INTO test_autoinc_merge m USING test_autoinc_upsert u 
  ON m.col = u.col
WHEN MATCHED THEN
  UPDATE SET m.col1=u.col1
WHEN NOT MATCHED THEN  
  INSERT VALUES (u.col, u.col1);
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;
--insert null/0 to test_autoinc_merge.col
MERGE INTO test_autoinc_merge m USING test_autoinc_upsert u 
  ON m.col = u.col1
WHEN MATCHED THEN
  UPDATE SET m.col1=u.col1
WHEN NOT MATCHED THEN  
  INSERT VALUES (u.col1, u.col1);
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;
--iinsert default to test_autoinc_merge.col
MERGE INTO test_autoinc_merge m USING test_autoinc_upsert u 
  ON m.col = u.col1
WHEN MATCHED THEN
  UPDATE SET m.col1=u.col1
WHEN NOT MATCHED THEN  
  INSERT VALUES (default, u.col1);
SELECT LAST_INSERT_ID();
SELECT col,col1 FROM test_autoinc_merge ORDER BY 1;

DROP TABLE test_autoinc_upsert;
DROP TABLE test_autoinc_merge;

--ERROR test
CREATE TABLE test_autoinc_err (
    col1 int auto_increment primary key,
    col2 int unique
);
INSERT INTO test_autoinc_err VALUES(NULL, 1);
SELECT LAST_INSERT_ID();
INSERT INTO test_autoinc_err VALUES(DEFAULT, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_err ORDER BY 1;
INSERT INTO test_autoinc_err VALUES(DEFAULT, 3);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_err ORDER BY 1;
INSERT INTO test_autoinc_err VALUES(0,1), (100,5);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_err ORDER BY 1;
INSERT INTO test_autoinc_err VALUES(0,5), (6,6), (0,1), (7,7);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_err ORDER BY 1;
INSERT INTO test_autoinc_err VALUES(DEFAULT, 5);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_err ORDER BY 1;

DROP TABLE test_autoinc_err;

-- last_insert_id test
CREATE TABLE test_last_insert_id(col int auto_increment PRIMARY KEY) AUTO_INCREMENT = 10;
INSERT INTO test_last_insert_id VALUES(DEFAULT);
SELECT LAST_INSERT_ID();
INSERT INTO test_last_insert_id VALUES(0), (NULL), (50), (DEFAULT);
SELECT LAST_INSERT_ID();
SELECT LAST_INSERT_ID(100 + 100);
SELECT LAST_INSERT_ID();
SELECT LAST_INSERT_ID(NULL);
SELECT LAST_INSERT_ID();
SELECT LAST_INSERT_ID(1.1);
SELECT LAST_INSERT_ID();
DROP TABLE test_last_insert_id;

-- test auto_increment insert multi values
create table test_autoinc_batch_insert (
    col1 int auto_increment primary key,
    col2 int 
);

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1), (DEFAULT, 1), (DEFAULT, 1), (DEFAULT, 1), (DEFAULT, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(5, 1), (8, 1), (7, 1), (9, 1), (20, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(5, 1), (NULL, 1), (NULL, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(10, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (15, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(20, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (NULL, 1), (NULL, 1), (29, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
DELETE FROM test_autoinc_batch_insert;


TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(30, 1);
INSERT INTO test_autoinc_batch_insert VALUES(1, 1), (NULL, 1), (NULL, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(40, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (2, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(50, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (NULL, 1), (NULL, 1), (3, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;


TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(60, 1);
INSERT INTO test_autoinc_batch_insert VALUES(62, 1), (NULL, 1), (NULL, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(70, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (74, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(80, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (85, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;


TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(90, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (91, 1), (NULL, 1), (NULL, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(100, 1);
INSERT INTO test_autoinc_batch_insert VALUES(NULL, 1), (NULL, 1), (102, 1), (NULL, 1), (109, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_insert;
INSERT INTO test_autoinc_batch_insert VALUES(110, 1);
INSERT INTO test_autoinc_batch_insert VALUES(120, 1), (NULL, 1), (NULL, 1), (NULL, 1), (123, 1);
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;
INSERT INTO test_autoinc_batch_insert VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_insert ORDER BY 1;

drop table test_autoinc_batch_insert;

-- test auto_increment insert select
create table test_autoinc_insert_select (
    col1 int auto_increment primary key,
    col2 int 
);
create table test_autoinc_source (
    col1 int,
    col2 int
);

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(DEFAULT, 1), (DEFAULT, 2), (DEFAULT, 3), (DEFAULT, 4), (DEFAULT, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(5, 1), (8, 2), (7, 3), (9, 4), (20, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(5, 1), (NULL, 2), (NULL, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(5, 1), (NULL, 2), (NULL, 3), (4, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(5, 1), (NULL, 2), (NULL, 3), (10, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(5, 1), (NULL, 2), (NULL, 3), (16, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (15, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(10, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (NULL, 3), (NULL, 4), (29, 5);
INSERT INTO test_autoinc_insert_select VALUES(20, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(1, 1), (NULL, 2), (NULL, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(30, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (2, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(40, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (NULL, 3), (NULL, 4), (3, 5);
INSERT INTO test_autoinc_insert_select VALUES(50, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;


TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(62, 1), (NULL, 2), (NULL, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(60, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (74, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(70, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (85, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(80, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (91, 3), (NULL, 4), (NULL, 5);
INSERT INTO test_autoinc_insert_select VALUES(90, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(NULL, 1), (NULL, 2), (102, 3), (NULL, 4), (109, 5);
INSERT INTO test_autoinc_insert_select VALUES(100, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 2);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

TRUNCATE TABLE test_autoinc_source;
TRUNCATE TABLE test_autoinc_insert_select;
INSERT INTO test_autoinc_source VALUES(120, 1), (NULL, 2), (NULL, 3), (NULL, 4), (123, 5);
INSERT INTO test_autoinc_insert_select VALUES(110, 1);
INSERT INTO test_autoinc_insert_select SELECT * FROM test_autoinc_source order by col2;
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;
INSERT INTO test_autoinc_insert_select VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_insert_select ORDER BY 1;

drop table test_autoinc_source;
drop table test_autoinc_insert_select;

-- test copy from
create table test_autoinc_batch_copy (
    col1 int auto_increment primary key,
    col2 int 
);

TRUNCATE TABLE test_autoinc_batch_copy;
COPY test_autoinc_batch_copy (col2) from stdin;
1
1
1
1
1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_copy;
COPY test_autoinc_batch_copy (col1,col2) from stdin;
5	1
4	1
8	1
9	1
1	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_copy;
COPY test_autoinc_batch_copy (col1,col2) from stdin;
5	1
0	1
0	1
0	1
0	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(30, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
1	1
0	1
0	1
0	1
0	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
-- error
TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(60, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
62	1
0	1
0	1
0	1
0	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
-- error
TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(70, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
0	1
0	1
74	1
0	1
0	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
-- error
TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(90, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
0	1
0	1
91	1
0	1
0	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
-- error
TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(100, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
0	1
0	1
102	1
0	1
109	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;

TRUNCATE TABLE test_autoinc_batch_copy;
INSERT INTO test_autoinc_batch_copy VALUES(110, 1);
COPY test_autoinc_batch_copy (col1,col2) from stdin;
120	1
0	1
0	1
0	1
123	1
\.
SELECT LAST_INSERT_ID();
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;
INSERT INTO test_autoinc_batch_copy VALUES(DEFAULT, 1);
SELECT col1,col2 FROM test_autoinc_batch_copy ORDER BY 1;

drop table test_autoinc_batch_copy;

-- upsert not auto_increment column
CREATE TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 1 ), ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE action = 'update';
SELECT LAST_INSERT_ID(); -- 3
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3 4
drop table test_autoinc_upsert;
-- upsert auto_increment 3
CREATE TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE id = '3';
SELECT LAST_INSERT_ID(); -- 4
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 3 4
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 3 4 5
drop table test_autoinc_upsert;
-- upsert auto_increment 2
CREATE TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE id = '2';
SELECT LAST_INSERT_ID(); -- 3
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3 4
drop table test_autoinc_upsert;
-- upsert auto_increment 5
CREATE TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 ), ( 4 )
ON DUPLICATE KEY UPDATE id = '5';
SELECT LAST_INSERT_ID(); -- 6
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 5 6 7
INSERT INTO test_autoinc_upsert (val ) VALUES ( 5 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 5 6 7 8
drop table test_autoinc_upsert;
-- temp table upsert not auto_increment column
CREATE TEMPORARY TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 1 ), ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE action = 'update';
SELECT LAST_INSERT_ID(); -- 3
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3 4
drop table test_autoinc_upsert;
-- upsert auto_increment 3
CREATE TEMPORARY TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE id = '3';
SELECT LAST_INSERT_ID(); -- 4
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 3 4
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 3 4 5
drop table test_autoinc_upsert;
-- upsert auto_increment 2
CREATE TEMPORARY TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 )
ON DUPLICATE KEY UPDATE id = '2';
SELECT LAST_INSERT_ID(); -- 3
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3
INSERT INTO test_autoinc_upsert (val ) VALUES ( 4 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 2 3 4
drop table test_autoinc_upsert;
-- upsert auto_increment 5
CREATE TEMPORARY TABLE test_autoinc_upsert ( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY( ID ),
val INT NOT NULL, UNIQUE( val ),
test_autoinc_upsert INT DEFAULT 1,
action varchar( 10 ) DEFAULT 'insert',
comment varchar( 30 ) );
INSERT INTO test_autoinc_upsert ( val ) VALUES ( 1 ), ( 2 ) ON DUPLICATE KEY UPDATE action = 'update';
SELECT * FROM test_autoinc_upsert ORDER BY 2;
INSERT INTO test_autoinc_upsert (val )
VALUES ( 2 ), ( 3 ), ( 4 )
ON DUPLICATE KEY UPDATE id = '5';
SELECT LAST_INSERT_ID(); -- 6
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 5 6 7
INSERT INTO test_autoinc_upsert (val ) VALUES ( 5 );
SELECT * FROM test_autoinc_upsert ORDER BY 2; -- 1 5 6 7 8
drop table test_autoinc_upsert;

\c regression
clean connection to all force for database autoinc_b_db;
drop database if exists autoinc_b_db;