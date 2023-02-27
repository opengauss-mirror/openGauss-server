create database atbdb_ltt WITH ENCODING 'UTF-8' dbcompatibility 'B';
\c atbdb_ltt
CREATE SCHEMA atbdb_ltt_schema;
SET CURRENT_SCHEMA TO atbdb_ltt_schema;

-- test modify column without data
CREATE LOCAL TEMPORARY TABLE test_at_modify(
    a int,
    b int NOT NULL
);
ALTER TABLE test_at_modify MODIFY b varchar(8) NULL;
ALTER TABLE test_at_modify MODIFY b varchar(8) DEFAULT '0';
ALTER TABLE test_at_modify MODIFY b int AUTO_INCREMENT PRIMARY KEY INITIALLY DEFERRED;
ALTER TABLE test_at_modify MODIFY b varchar(8) UNIQUE DEFERRABLE;
ALTER TABLE test_at_modify MODIFY b varchar(8) CHECK (b < 'a');
ALTER TABLE test_at_modify MODIFY b varchar(8) COLLATE "POSIX";
ALTER TABLE test_at_modify MODIFY b varchar(8) GENERATED ALWAYS AS (a+1) STORED;
ALTER TABLE test_at_modify MODIFY b int NOT NULL;
INSERT INTO test_at_modify VALUES(1,1);
DROP TABLE test_at_modify;

-- test modify column datatype
CREATE LOCAL TEMPORARY TABLE test_at_modify_type(
    a int,
    b int NOT NULL
);
INSERT INTO test_at_modify_type VALUES(1,1);
INSERT INTO test_at_modify_type VALUES(2,2);
INSERT INTO test_at_modify_type VALUES(3,3);
ALTER TABLE test_at_modify_type MODIFY COLUMN b varchar(8);
SELECT * FROM test_at_modify_type where b = '3';
ALTER TABLE test_at_modify_type MODIFY COLUMN b DATE; -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b RAW;
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;
CREATE LOCAL TEMPORARY TABLE test_at_modify_type(
    a int,
    b text
);
INSERT INTO test_at_modify_type VALUES(1,'beijing');
INSERT INTO test_at_modify_type VALUES(2,'shanghai');
INSERT INTO test_at_modify_type VALUES(3,'guangzhou');
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','nanjing','wuhan'); -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','nanjing','guangzhou');
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','guangzhou','wuhan'); -- ERROR
DROP TABLE test_at_modify_type;
CREATE LOCAL TEMPORARY TABLE test_at_modify_type(
    a int,
    b varchar(32)
);
INSERT INTO test_at_modify_type VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_modify_type VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_modify_type VALUES(3,'2022-11-24 12:00:00');
ALTER TABLE test_at_modify_type MODIFY COLUMN b varchar(10); -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b DATE;
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;
CREATE LOCAL TEMPORARY TABLE test_at_modify_type(
    a int,
    b int[] NOT NULL
);
INSERT INTO test_at_modify_type VALUES(1,ARRAY[1,1]);
INSERT INTO test_at_modify_type VALUES(2,ARRAY[2,2]);
INSERT INTO test_at_modify_type VALUES(3,ARRAY[3,3]);
ALTER TABLE test_at_modify_type MODIFY COLUMN b float4[];
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;

-- test modify column constraint
CREATE LOCAL TEMPORARY TABLE test_at_modify_constr(
    a int,
    b int NOT NULL
);
INSERT INTO test_at_modify_constr VALUES(1,1);
INSERT INTO test_at_modify_constr VALUES(2,2);
INSERT INTO test_at_modify_constr VALUES(3,3);
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) NOT NULL NULL; -- ERROR
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) UNIQUE KEY NULL;
INSERT INTO test_at_modify_constr VALUES(3,3); -- ERROR
INSERT INTO test_at_modify_constr VALUES(4,NULL);
ALTER TABLE test_at_modify_constr MODIFY b int NULL PRIMARY KEY; -- ERROR
DELETE FROM test_at_modify_constr WHERE b IS NULL;
ALTER TABLE test_at_modify_constr MODIFY b int NULL PRIMARY KEY;
INSERT INTO test_at_modify_constr VALUES(4,NULL); -- ERROR
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) CONSTRAINT t_at_m_check CHECK (b < 3); -- ERROR
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) CONSTRAINT t_at_m_check CHECK (b < 5);
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) CONSTRAINT t_at_m_check CHECK (b = a); -- ERROR
ALTER TABLE test_at_modify_constr MODIFY b varchar(8) CONSTRAINT t_at_m_check_1 CHECK (b = a);
INSERT INTO test_at_modify_constr VALUES(4,4);
INSERT INTO test_at_modify_constr VALUES(5,5); -- ERROR
INSERT INTO test_at_modify_constr VALUES(6,'a'); -- ERROR
INSERT INTO test_at_modify_constr VALUES(0,'a');
ALTER TABLE test_at_modify_constr MODIFY b int NOT NULL PRIMARY KEY; -- ERROR
ALTER TABLE test_at_modify_constr MODIFY b int NOT NULL;
INSERT INTO test_at_modify_constr VALUES(5,5); -- ERROR
SELECT b FROM test_at_modify_constr ORDER BY 1;
ALTER TABLE test_at_modify_constr MODIFY b int NOT NULL REFERENCES test_at_ref (a); -- ERROR
DROP TABLE test_at_modify_constr;

-- test modify column default
CREATE LOCAL TEMPORARY TABLE test_at_modify_default(
    a int,
    b int DEFAULT NULL
);
INSERT INTO test_at_modify_default VALUES(1,1);
INSERT INTO test_at_modify_default VALUES(2,2);
INSERT INTO test_at_modify_default VALUES(3,3);
ALTER TABLE test_at_modify_default MODIFY b bigint DEFAULT (a+1); -- ERROR
ALTER TABLE test_at_modify_default MODIFY b bigint DEFAULT NULL;
ALTER TABLE test_at_modify_default MODIFY b varchar(8) DEFAULT 'a' GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_modify_default MODIFY b varchar(8) DEFAULT 'a';
INSERT INTO test_at_modify_default VALUES(0,DEFAULT);
SELECT b FROM test_at_modify_default ORDER BY 1;
ALTER TABLE test_at_modify_default MODIFY b int DEFAULT 4 AUTO_INCREMENT; -- ERROR
ALTER TABLE test_at_modify_default MODIFY b int DEFAULT 4;
INSERT INTO test_at_modify_default VALUES(4,DEFAULT);
SELECT b FROM test_at_modify_default ORDER BY 1;
ALTER TABLE test_at_modify_default MODIFY b varchar(8) GENERATED ALWAYS AS (a+1) STORED;
SELECT a,b FROM test_at_modify_default ORDER BY 1,2;
ALTER TABLE test_at_modify_default MODIFY a varchar(8) DEFAULT 'a';
INSERT INTO test_at_modify_default VALUES(DEFAULT,DEFAULT);
SELECT a,b FROM test_at_modify_default ORDER BY 1,2;
DROP TABLE test_at_modify_default;

-- test modify column depended by generated column
CREATE LOCAL TEMPORARY TABLE test_at_modify_generated(
    a int,
    b varchar(32),
    c varchar(32) GENERATED ALWAYS AS (b) STORED
);
INSERT INTO test_at_modify_generated(a,b) VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_modify_generated(a,b) VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_modify_generated(a,b) VALUES(3,'2022-11-24 12:00:00');
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b DATE;
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b varchar(32);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
DROP TABLE test_at_modify_generated;


-- test modify column AUTO_INCREMENT
CREATE LOCAL TEMPORARY TABLE test_at_modify_autoinc(
    a int,
    b int
);
INSERT INTO test_at_modify_autoinc VALUES(1,NULL);
INSERT INTO test_at_modify_autoinc VALUES(2,0);
ALTER TABLE test_at_modify_autoinc MODIFY b int2 AUTO_INCREMENT; -- ERROR
ALTER TABLE test_at_modify_autoinc MODIFY b DECIMAL(4,2) AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_modify_autoinc MODIFY b serial AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_modify_autoinc MODIFY b int2 AUTO_INCREMENT NULL UNIQUE KEY;
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
INSERT INTO test_at_modify_autoinc VALUES(3,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc MODIFY COLUMN b int;
INSERT INTO test_at_modify_autoinc VALUES(4,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc MODIFY b int AUTO_INCREMENT PRIMARY KEY, AUTO_INCREMENT=100;
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
INSERT INTO test_at_modify_autoinc VALUES(5,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc AUTO_INCREMENT=1000;
ALTER TABLE test_at_modify_autoinc MODIFY b int2 AUTO_INCREMENT;
INSERT INTO test_at_modify_autoinc VALUES(6,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE, MODIFY  b int2 AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_modify_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE; -- ERROR
ALTER TABLE test_at_modify_autoinc MODIFY COLUMN b int;
ALTER TABLE test_at_modify_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE;
INSERT INTO test_at_modify_autoinc VALUES(7,0,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc DROP COLUMN c , MODIFY b int2 AUTO_INCREMENT UNIQUE KEY;
INSERT INTO test_at_modify_autoinc VALUES(8,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
ALTER TABLE test_at_modify_autoinc MODIFY b float4; -- ALTER TYPE ONLY, KEEP AUTO_INCREMENT
INSERT INTO test_at_modify_autoinc VALUES(9,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 1,2;
DROP TABLE test_at_modify_autoinc;

-- ------------------------------------------------------ test ALTER TABLE CHANGE
-- test change column without data
CREATE LOCAL TEMPORARY TABLE test_at_change(
    a int,
    b int NOT NULL
);
ALTER TABLE test_at_change CHANGE b b1 varchar(8) NULL;
ALTER TABLE test_at_change CHANGE b1 b varchar(8) DEFAULT '0';
ALTER TABLE test_at_change CHANGE b b1 int AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE test_at_change CHANGE b1 b varchar(8) UNIQUE;
ALTER TABLE test_at_change CHANGE b b1 varchar(8) CHECK (b1 < 'a');
ALTER TABLE test_at_change CHANGE b1 b varchar(8) COLLATE "POSIX";
ALTER TABLE test_at_change CHANGE b b1 varchar(8) GENERATED ALWAYS AS (a+1) STORED;
ALTER TABLE test_at_change CHANGE b1 b int NOT NULL;
INSERT INTO test_at_change VALUES(1,1);
DROP TABLE test_at_change;

-- test change column datatype
CREATE LOCAL TEMPORARY TABLE test_at_change_type(
    a int,
    b int NOT NULL
);
INSERT INTO test_at_change_type VALUES(1,1);
INSERT INTO test_at_change_type VALUES(2,2);
INSERT INTO test_at_change_type VALUES(3,3);
ALTER TABLE test_at_change_type CHANGE b b1 varchar(8);
SELECT * FROM test_at_change_type where b1 = '3';
ALTER TABLE test_at_change_type CHANGE b1 b DATE; -- ERROR
ALTER TABLE test_at_change_type CHANGE b1 b RAW;
SELECT * FROM test_at_change_type ORDER BY 1,2;
DROP TABLE test_at_change_type;
CREATE LOCAL TEMPORARY TABLE test_at_change_type(
    a int,
    b text
);
INSERT INTO test_at_change_type VALUES(1,'beijing');
INSERT INTO test_at_change_type VALUES(2,'shanghai');
INSERT INTO test_at_change_type VALUES(3,'guangzhou');
ALTER TABLE test_at_change_type CHANGE b b1 SET('beijing','shanghai','nanjing','wuhan'); -- ERROR
ALTER TABLE test_at_change_type CHANGE b b1 SET('beijing','shanghai','nanjing','guangzhou');
ALTER TABLE test_at_change_type CHANGE b1 b SET('beijing','shanghai','guangzhou','wuhan'); -- ERROR
DROP TABLE test_at_change_type;
CREATE LOCAL TEMPORARY TABLE test_at_change_type(
    a int,
    b varchar(32)
);
INSERT INTO test_at_change_type VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_change_type VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_change_type VALUES(3,'2022-11-24 12:00:00');
ALTER TABLE test_at_change_type CHANGE b b1 varchar(10); -- ERROR
ALTER TABLE test_at_change_type CHANGE b b1 DATE;
SELECT * FROM test_at_change_type ORDER BY 1,2;
DROP TABLE test_at_change_type;

-- test change column constraint
CREATE LOCAL TEMPORARY TABLE test_at_change_constr(
    a int,
    b int NOT NULL
);
INSERT INTO test_at_change_constr VALUES(1,1);
INSERT INTO test_at_change_constr VALUES(2,2);
INSERT INTO test_at_change_constr VALUES(3,3);
ALTER TABLE test_at_change_constr CHANGE b b1 varchar(8) NOT NULL NULL; -- ERROR
ALTER TABLE test_at_change_constr CHANGE b b1 varchar(8) UNIQUE KEY NULL;
INSERT INTO test_at_change_constr VALUES(3,3); -- ERROR
INSERT INTO test_at_change_constr VALUES(4,NULL);
ALTER TABLE test_at_change_constr CHANGE b1 b int NULL PRIMARY KEY; -- ERROR
DELETE FROM test_at_change_constr WHERE b1 IS NULL;
ALTER TABLE test_at_change_constr CHANGE b1 b int NULL PRIMARY KEY;
INSERT INTO test_at_change_constr VALUES(4,NULL); -- ERROR
ALTER TABLE test_at_change_constr CHANGE b b1 varchar(8) CONSTRAINT t_at_m_check CHECK (b1 < 3); -- ERROR
ALTER TABLE test_at_change_constr CHANGE b b1 varchar(8) CONSTRAINT t_at_m_check CHECK (b1 < 5);
ALTER TABLE test_at_change_constr CHANGE b1 b varchar(8) CONSTRAINT t_at_m_check CHECK (b = a); -- ERROR
ALTER TABLE test_at_change_constr CHANGE b1 b varchar(8) CONSTRAINT t_at_m_check_1 CHECK (b = a);
INSERT INTO test_at_change_constr VALUES(4,4);
INSERT INTO test_at_change_constr VALUES(5,5); -- ERROR
INSERT INTO test_at_change_constr VALUES(6,'a'); -- ERROR
INSERT INTO test_at_change_constr VALUES(0,'a');
ALTER TABLE test_at_change_constr CHANGE b b1 int NOT NULL PRIMARY KEY; -- ERROR
ALTER TABLE test_at_change_constr CHANGE b b1 int NOT NULL;
INSERT INTO test_at_change_constr VALUES(5,5); -- ERROR
SELECT b1 FROM test_at_change_constr ORDER BY 1;
ALTER TABLE test_at_change_constr CHANGE b1 b int NOT NULL REFERENCES test_at_ref (a); -- ERROR
DROP TABLE test_at_change_constr;

-- test change column default
CREATE LOCAL TEMPORARY TABLE test_at_change_default(
    a int,
    b int DEFAULT NULL
);
INSERT INTO test_at_change_default VALUES(1,1);
INSERT INTO test_at_change_default VALUES(2,2);
INSERT INTO test_at_change_default VALUES(3,3);
ALTER TABLE test_at_change_default CHANGE b b1 bigint DEFAULT (a+1); -- ERROR
ALTER TABLE test_at_change_default CHANGE b b1 bigint DEFAULT NULL;
ALTER TABLE test_at_change_default CHANGE b1 b varchar(8) DEFAULT 'a' GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_change_default CHANGE b1 b varchar(8) DEFAULT 'a';
INSERT INTO test_at_change_default VALUES(0,DEFAULT);
SELECT b FROM test_at_change_default ORDER BY 1;
ALTER TABLE test_at_change_default CHANGE b b1 int DEFAULT 4 AUTO_INCREMENT; -- ERROR
ALTER TABLE test_at_change_default CHANGE b b1 int DEFAULT 4;
INSERT INTO test_at_change_default VALUES(4,DEFAULT);
SELECT b1 FROM test_at_change_default ORDER BY 1;
ALTER TABLE test_at_change_default CHANGE b1 b varchar(8) GENERATED ALWAYS AS (a+1) STORED;
SELECT a,b FROM test_at_change_default ORDER BY 1,2;
ALTER TABLE test_at_change_default CHANGE a a1 varchar(8) DEFAULT 'a';
INSERT INTO test_at_change_default VALUES(DEFAULT,DEFAULT);
SELECT * FROM test_at_change_default ORDER BY 1,2;
DROP TABLE test_at_change_default;

-- test change column depended by generated column
CREATE LOCAL TEMPORARY TABLE test_at_change_generated(
    a int,
    b varchar(32),
    c varchar(32) GENERATED ALWAYS AS (b) STORED
);
INSERT INTO test_at_change_generated(a,b) VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_change_generated(a,b) VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_change_generated(a,b) VALUES(3,'2022-11-24 12:00:00');
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN b b1 DATE;
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN b1 b varchar(32);
SELECT * FROM test_at_change_generated ORDER BY 1,2;
DROP TABLE test_at_change_generated;

-- test change column AUTO_INCREMENT
CREATE LOCAL TEMPORARY TABLE test_at_change_autoinc(
    a int,
    b int
);
INSERT INTO test_at_change_autoinc VALUES(1,NULL);
INSERT INTO test_at_change_autoinc VALUES(2,0);
ALTER TABLE test_at_change_autoinc CHANGE b b1 int2 AUTO_INCREMENT; -- ERROR
ALTER TABLE test_at_change_autoinc CHANGE b b1 DECIMAL(4,2) AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_change_autoinc CHANGE b b1 serial AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_change_autoinc CHANGE b b1 int2 AUTO_INCREMENT NULL UNIQUE KEY;
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
INSERT INTO test_at_change_autoinc VALUES(3,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
ALTER TABLE test_at_change_autoinc CHANGE b1 b int;
INSERT INTO test_at_change_autoinc VALUES(4,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
ALTER TABLE test_at_change_autoinc CHANGE b b1 int AUTO_INCREMENT PRIMARY KEY, AUTO_INCREMENT=100;
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
INSERT INTO test_at_change_autoinc VALUES(5,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
ALTER TABLE test_at_change_autoinc AUTO_INCREMENT=1000;
ALTER TABLE test_at_change_autoinc CHANGE b1 b int2 AUTO_INCREMENT;
INSERT INTO test_at_change_autoinc VALUES(6,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
ALTER TABLE test_at_change_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE, CHANGE b b1 int2 AUTO_INCREMENT UNIQUE KEY; -- ERROR
ALTER TABLE test_at_change_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE; -- ERROR
ALTER TABLE test_at_change_autoinc CHANGE b b1 int;
ALTER TABLE test_at_change_autoinc ADD COLUMN c int AUTO_INCREMENT UNIQUE;
INSERT INTO test_at_change_autoinc VALUES(7,0,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
ALTER TABLE test_at_change_autoinc DROP COLUMN c , CHANGE b1 b int2 AUTO_INCREMENT UNIQUE KEY;
INSERT INTO test_at_change_autoinc VALUES(8,0);
SELECT * FROM test_at_change_autoinc ORDER BY 1,2;
DROP TABLE test_at_change_autoinc;

-- TEMPORARY view
CREATE LOCAL TEMPORARY TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique);
INSERT INTO test_at_modify_view_column VALUES(1, '1', '1', 1);
CREATE TEMPORARY VIEW test_modify_view_star AS select * from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int FIRST;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;

-- END
RESET CURRENT_SCHEMA;
DROP SCHEMA atbdb_ltt_schema CASCADE;
\c regression
clean connection to all force for database atbdb_ltt;
drop database if exists atbdb_ltt;