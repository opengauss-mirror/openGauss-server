create database atbdb_ustore WITH ENCODING 'UTF-8' dbcompatibility 'B';
\c atbdb_ustore
CREATE SCHEMA atbdb_ustore_schema;
SET CURRENT_SCHEMA TO atbdb_ustore_schema;

-- test modify column without data
CREATE TABLE test_at_modify(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
ALTER TABLE test_at_modify MODIFY b varchar(8) NULL;
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b varchar(8) DEFAULT '0';
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b int AUTO_INCREMENT PRIMARY KEY;
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b varchar(8) UNIQUE;
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b varchar(8) CHECK (b < 'a');
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b varchar(8) COLLATE "POSIX";
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b varchar(8) GENERATED ALWAYS AS (a+1) STORED;
\d+ test_at_modify;
ALTER TABLE test_at_modify MODIFY b int NOT NULL;
\d+ test_at_modify;
select pg_get_tabledef('test_at_modify'::regclass);
INSERT INTO test_at_modify VALUES(1,1);
DROP TABLE test_at_modify;

-- test modify column datatype
CREATE TABLE test_at_modify_type(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_type VALUES(1,1);
INSERT INTO test_at_modify_type VALUES(2,2);
INSERT INTO test_at_modify_type VALUES(3,3);
ALTER TABLE test_at_modify_type MODIFY COLUMN b varchar(8);
SELECT * FROM test_at_modify_type where b = '3';
ALTER TABLE test_at_modify_type MODIFY COLUMN b DATE; -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b RAW;
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;
CREATE TABLE test_at_modify_type(
    a int,
    b serial NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_type VALUES(1,1);
INSERT INTO test_at_modify_type VALUES(2,2);
INSERT INTO test_at_modify_type VALUES(3,3);
ALTER TABLE test_at_modify_type MODIFY COLUMN b int;
ALTER TABLE test_at_modify_type MODIFY COLUMN b int[]; -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b int16;
ALTER TABLE test_at_modify_type MODIFY COLUMN b serial; -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b DECIMAL(4,2);
SELECT * FROM test_at_modify_type where b = 3;
ALTER TABLE test_at_modify_type MODIFY COLUMN b BOOLEAN;
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;
CREATE TABLE test_at_modify_type(
    a int,
    b text
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_type VALUES(1,'beijing');
INSERT INTO test_at_modify_type VALUES(2,'shanghai');
INSERT INTO test_at_modify_type VALUES(3,'guangzhou');
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','nanjing','wuhan'); -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','nanjing','guangzhou');
ALTER TABLE test_at_modify_type MODIFY COLUMN b SET('beijing','shanghai','guangzhou','wuhan'); -- ERROR
select pg_get_tabledef('test_at_modify_type'::regclass);
DROP TABLE test_at_modify_type;
CREATE TABLE test_at_modify_type(
    a int,
    b varchar(32)
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_type VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_modify_type VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_modify_type VALUES(3,'2022-11-24 12:00:00');
ALTER TABLE test_at_modify_type MODIFY COLUMN b varchar(10); -- ERROR
ALTER TABLE test_at_modify_type MODIFY COLUMN b DATE;
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;
CREATE TABLE test_at_modify_type(
    a int,
    b int[] NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_type VALUES(1,ARRAY[1,1]);
INSERT INTO test_at_modify_type VALUES(2,ARRAY[2,2]);
INSERT INTO test_at_modify_type VALUES(3,ARRAY[3,3]);
ALTER TABLE test_at_modify_type MODIFY COLUMN b float4[];
SELECT * FROM test_at_modify_type ORDER BY 1,2;
DROP TABLE test_at_modify_type;

-- test modify column constraint
CREATE TABLE test_at_modify_constr(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
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
select pg_get_tabledef('test_at_modify_constr'::regclass);
ALTER TABLE test_at_modify_constr MODIFY b int NOT NULL REFERENCES test_at_ref (a); -- ERROR
DROP TABLE test_at_modify_constr;

-- test modify column default
CREATE TABLE test_at_modify_default(
    a int,
    b int DEFAULT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_default VALUES(1,1);
INSERT INTO test_at_modify_default VALUES(2,2);
INSERT INTO test_at_modify_default VALUES(3,3);
ALTER TABLE test_at_modify_default MODIFY b bigint DEFAULT (a+1); -- ERROR
ALTER TABLE test_at_modify_default MODIFY b bigint DEFAULT NULL;
\d+ test_at_modify_default;
ALTER TABLE test_at_modify_default MODIFY b varchar(8) DEFAULT 'a' GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_modify_default MODIFY b varchar(8) DEFAULT 'a';
\d+ test_at_modify_default;
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
\d+ test_at_modify_default;
DROP TABLE test_at_modify_default;

-- test modify column depended by generated column
CREATE TABLE test_at_modify_generated(
    a int,
    b varchar(32),
    c varchar(32) GENERATED ALWAYS AS (b) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_generated(a,b) VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_modify_generated(a,b) VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_modify_generated(a,b) VALUES(3,'2022-11-24 12:00:00');
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b DATE;
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
\d+ test_at_modify_generated
ALTER TABLE test_at_modify_generated MODIFY COLUMN b varchar(32);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
DROP TABLE test_at_modify_generated;
CREATE TABLE test_at_modify_generated(
    a int,
    b int GENERATED ALWAYS AS (a+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_generated(a,b) VALUES(-1,DEFAULT);
INSERT INTO test_at_modify_generated(a,b) VALUES(0,DEFAULT);
INSERT INTO test_at_modify_generated(a,b) VALUES(1,DEFAULT);
INSERT INTO test_at_modify_generated(a,b) VALUES(2,DEFAULT);
ALTER TABLE test_at_modify_generated MODIFY COLUMN b varchar(8) GENERATED ALWAYS AS (a) STORED FIRST, MODIFY COLUMN a varchar(8) AFTER b;
INSERT INTO test_at_modify_generated(a,b) VALUES(3,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY b::int,a::int;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a int AFTER b, MODIFY COLUMN b int GENERATED ALWAYS AS (a+1) STORED FIRST;
INSERT INTO test_at_modify_generated(a,b) VALUES(4,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b varchar(8) AFTER a, MODIFY COLUMN a varchar(8) AFTER b;
INSERT INTO test_at_modify_generated(a,b) VALUES(5,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b int GENERATED ALWAYS AS (a) STORED;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a int FIRST, MODIFY COLUMN b int FIRST;
INSERT INTO test_at_modify_generated(a,b) VALUES(6,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
DROP TABLE test_at_modify_generated;
CREATE TABLE test_at_modify_generated(
    a int,
    b int GENERATED ALWAYS AS (a+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_generated(a,b) VALUES(-1,DEFAULT);
INSERT INTO test_at_modify_generated(a,b) VALUES(0,DEFAULT);
INSERT INTO test_at_modify_generated(a,b) VALUES(1,DEFAULT);
ALTER TABLE test_at_modify_generated MODIFY COLUMN a bool;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a int;
INSERT INTO test_at_modify_generated(a,b) VALUES(100,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a bool, MODIFY COLUMN b varchar(32);
\d test_at_modify_generated
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b int GENERATED ALWAYS AS (a+1) STORED, MODIFY COLUMN a int;
\d test_at_modify_generated
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
INSERT INTO test_at_modify_generated(a,b) VALUES(100,DEFAULT);
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN b bool GENERATED ALWAYS AS (a) STORED;
\d test_at_modify_generated
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a bool;
\d test_at_modify_generated
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
ALTER TABLE test_at_modify_generated MODIFY COLUMN a int;
INSERT INTO test_at_modify_generated(a,b) VALUES(100,DEFAULT);
\d test_at_modify_generated
SELECT * FROM test_at_modify_generated ORDER BY 1,2;
DROP TABLE test_at_modify_generated;

-- error generated column reference generated column
CREATE TABLE test_at_modify_generated(
    a int,
    b int,
    c int GENERATED ALWAYS AS (b+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_generated(a,b) VALUES(1,1);
ALTER TABLE test_at_modify_generated MODIFY b float4 GENERATED ALWAYS AS (a+1000) STORED; -- ERROR
ALTER TABLE test_at_modify_generated MODIFY b float4 GENERATED ALWAYS AS (c+1000) STORED; -- ERROR
ALTER TABLE test_at_modify_generated MODIFY a float4 GENERATED ALWAYS AS (b+1000) STORED, MODIFY c float4 GENERATED ALWAYS AS (a+1000) STORED; -- ERROR
ALTER TABLE test_at_modify_generated MODIFY COLUMN c float4, MODIFY b float4 GENERATED ALWAYS AS (c+1000) STORED;
DROP TABLE test_at_modify_generated;

-- test modify column AUTO_INCREMENT
CREATE TABLE test_at_modify_autoinc(
    a int,
    b int
) WITH(STORAGE_TYPE=USTORE);
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
ALTER TABLE test_at_modify_autoinc DROP COLUMN c , MODIFY b int2 AUTO_INCREMENT UNIQUE KEY FIRST;
INSERT INTO test_at_modify_autoinc(a,b) VALUES(8,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 2,1;
ALTER TABLE test_at_modify_autoinc MODIFY b float4; -- ALTER TYPE ONLY, KEEP AUTO_INCREMENT
INSERT INTO test_at_modify_autoinc(a,b) VALUES(9,0);
SELECT * FROM test_at_modify_autoinc ORDER BY 2,1;
DROP TABLE test_at_modify_autoinc;

-- test generated column reference auto_increment column
CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,0);
INSERT INTO test_at_modify_fa VALUES(21,22,0);
ALTER TABLE test_at_modify_fa MODIFY COLUMN c int AUTO_INCREMENT PRIMARY KEY, MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED; -- ERROR
ALTER TABLE test_at_modify_fa MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED, MODIFY COLUMN c int AUTO_INCREMENT PRIMARY KEY; -- ERROR
ALTER TABLE test_at_modify_fa MODIFY COLUMN c bigint AUTO_INCREMENT PRIMARY KEY, MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED; -- ERROR
ALTER TABLE test_at_modify_fa MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED, MODIFY COLUMN c bigint AUTO_INCREMENT PRIMARY KEY; -- ERROR
DROP TABLE test_at_modify_fa;

-- test modify column depended by other objects
CREATE TABLE test_at_modify_depend(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_depend VALUES(1,1);
INSERT INTO test_at_modify_depend VALUES(2,2);
INSERT INTO test_at_modify_depend VALUES(3,3);
-- --PROCEDURE contains column
CREATE OR REPLACE PROCEDURE test_at_modify_proc(IN p_in int)
    AS
    BEGIN
        INSERT INTO test_at_modify_depend(a,b) VALUES(p_in, p_in);
    END;
/
ALTER TABLE test_at_modify_depend MODIFY b varchar(8) NOT NULL;
CALL test_at_modify_proc(2);
DROP PROCEDURE test_at_modify_proc;

-- --TRIGGER contains and depends column
CREATE OR REPLACE FUNCTION tg_bf_test_at_modify_func() RETURNS TRIGGER AS
$$
    DECLARE
    BEGIN
        UPDATE test_at_modify_depend SET b = NULL WHERE a < NEW.a;
        RETURN NEW;
    END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER tg_bf_test_at_modify
    AFTER UPDATE ON test_at_modify_depend
    FOR EACH ROW WHEN ( NEW.b IS NULL AND OLD.b = OLD.a)
    EXECUTE PROCEDURE tg_bf_test_at_modify_func();
ALTER TABLE test_at_modify_depend MODIFY b int NULL DEFAULT 0;
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
UPDATE test_at_modify_depend SET b = NULL WHERE a = 2;
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
DROP TRIGGER tg_bf_test_at_modify ON test_at_modify_depend;

-- --TRIGGER contains but does not depend column
CREATE TRIGGER tg_bf_test_at_modify
    BEFORE INSERT ON test_at_modify_depend
    FOR EACH ROW
    EXECUTE PROCEDURE tg_bf_test_at_modify_func();
ALTER TABLE test_at_modify_depend MODIFY b varchar(8) NULL;
INSERT INTO test_at_modify_depend VALUES (4, 4);
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
DROP TRIGGER tg_bf_test_at_modify ON test_at_modify_depend;
DROP PROCEDURE tg_bf_test_at_modify_func;

-- --VIEW depends column
CREATE VIEW test_at_modify_view AS SELECT b FROM test_at_modify_depend;
ALTER TABLE test_at_modify_depend MODIFY b bigint NULL; -- ERROR
ALTER TABLE test_at_modify_depend MODIFY b int NULL; -- ERROR
ALTER TABLE test_at_modify_depend MODIFY b varchar(8) NULL;
SELECT * FROM test_at_modify_view ORDER BY 1;
DROP VIEW test_at_modify_view;
CREATE VIEW test_at_modify_view AS SELECT a FROM test_at_modify_depend where b > 0;
CREATE VIEW test_at_modify_view1 AS SELECT * FROM test_at_modify_view;
ALTER TABLE test_at_modify_depend MODIFY b bigint NULL GENERATED ALWAYS AS (a+1);
ALTER TABLE test_at_modify_depend MODIFY b varchar(8) NULL;
ALTER TABLE test_at_modify_depend MODIFY b int NULL;
DROP VIEW test_at_modify_view1;
DROP VIEW test_at_modify_view;
CREATE materialized VIEW test_at_modify_view AS SELECT b FROM test_at_modify_depend; --ERROR

-- --TABLE reference column.
DELETE FROM test_at_modify_depend;
ALTER TABLE test_at_modify_depend MODIFY b INT PRIMARY KEY;
CREATE TABLE test_at_modify_ref(
    a int,
    b int,
    FOREIGN KEY (b) REFERENCES test_at_modify_depend(b) ON DELETE SET NULL
);
ALTER TABLE test_at_modify_depend MODIFY COLUMN b varchar(8);
INSERT INTO test_at_modify_ref VALUES(0,0); -- ERROR
INSERT INTO test_at_modify_depend VALUES(0,0);
INSERT INTO test_at_modify_ref VALUES(0,0);
ALTER TABLE test_at_modify_ref MODIFY COLUMN b varchar(8) GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_modify_ref MODIFY COLUMN b varchar(8);
\d+ test_at_modify_ref
DROP TABLE test_at_modify_ref;

-- --TABLE reference self column.
CREATE TABLE test_at_modify_ref(
    a int PRIMARY KEY,
    b int,
    FOREIGN KEY (b) REFERENCES test_at_modify_ref(a) ON DELETE SET NULL
);
INSERT INTO test_at_modify_ref VALUES(0,0);
ALTER TABLE test_at_modify_ref MODIFY COLUMN b varchar(8) GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_modify_ref MODIFY COLUMN b varchar(8);
ALTER TABLE test_at_modify_ref MODIFY COLUMN a varchar(8);
INSERT INTO test_at_modify_ref VALUES('a','a');
DROP TABLE test_at_modify_ref;

-- --RULE reference column.
CREATE RULE test_at_modify_rule AS ON INSERT TO test_at_modify_depend WHERE (b is null) DO INSTEAD UPDATE test_at_modify_depend SET b=0;
ALTER TABLE test_at_modify_depend MODIFY COLUMN b int not null; -- ERROR
DROP RULE test_at_modify_rule ON test_at_modify_depend;

-- --RLSPOLICY reference column.
DROP TABLE test_at_modify_depend;
CREATE ROLE at_modify_role_ustore PASSWORD 'Gauss@123';
CREATE TABLE test_at_modify_depend(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_depend VALUES(0,0);
GRANT USAGE ON SCHEMA atbdb_ustore_schema TO at_modify_role_ustore;
GRANT SELECT ON test_at_modify_depend TO at_modify_role_ustore;
ALTER TABLE test_at_modify_depend ENABLE ROW LEVEL SECURITY;
CREATE ROW LEVEL SECURITY POLICY test_at_modify_rls ON test_at_modify_depend AS RESTRICTIVE FOR SELECT TO at_modify_role_ustore USING(b >= 20);
ALTER TABLE test_at_modify_depend MODIFY COLUMN b int not null;
INSERT INTO test_at_modify_depend VALUES(21,21);
SET ROLE at_modify_role_ustore PASSWORD 'Gauss@123';
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
RESET ROLE;
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
ALTER TABLE test_at_modify_depend MODIFY COLUMN b bool not null;
ALTER TABLE test_at_modify_depend MODIFY COLUMN b int not null;
INSERT INTO test_at_modify_depend VALUES(22,22);
SET ROLE at_modify_role_ustore PASSWORD 'Gauss@123';
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
RESET ROLE;
SELECT * FROM test_at_modify_depend ORDER BY 1,2;
DROP TABLE test_at_modify_depend;
REVOKE ALL PRIVILEGES ON SCHEMA atbdb_ustore_schema FROM at_modify_role_ustore;
DROP ROLE at_modify_role_ustore;

-- ------------------------------------------------------ test ALTER TABLE CHANGE
-- test change column without data
CREATE TABLE test_at_change(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
ALTER TABLE test_at_change CHANGE b b1 varchar(8) NULL;
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b1 b varchar(8) DEFAULT '0';
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b b1 int AUTO_INCREMENT PRIMARY KEY;
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b1 b varchar(8) UNIQUE;
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b b1 varchar(8) CHECK (b1 < 'a');
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b1 b varchar(8) COLLATE "POSIX";
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b b1 varchar(8) GENERATED ALWAYS AS (a+1) STORED;
\d+ test_at_change;
ALTER TABLE test_at_change CHANGE b1 b int NOT NULL;
\d+ test_at_change;
select pg_get_tabledef('test_at_change'::regclass);
INSERT INTO test_at_change VALUES(1,1);
DROP TABLE test_at_change;

-- test change column datatype
CREATE TABLE test_at_change_type(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_type VALUES(1,1);
INSERT INTO test_at_change_type VALUES(2,2);
INSERT INTO test_at_change_type VALUES(3,3);
ALTER TABLE test_at_change_type CHANGE b b1 varchar(8);
SELECT * FROM test_at_change_type where b1 = '3';
ALTER TABLE test_at_change_type CHANGE b1 b DATE; -- ERROR
ALTER TABLE test_at_change_type CHANGE b1 b RAW;
SELECT * FROM test_at_change_type ORDER BY 1,2;
DROP TABLE test_at_change_type;
CREATE TABLE test_at_change_type(
    a int,
    b serial NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_type VALUES(1,1);
INSERT INTO test_at_change_type VALUES(2,2);
INSERT INTO test_at_change_type VALUES(3,3);
ALTER TABLE test_at_change_type CHANGE b b1 int;
ALTER TABLE test_at_change_type CHANGE b1 b serial; -- ERROR
ALTER TABLE test_at_change_type CHANGE b1 b DECIMAL(4,2);
SELECT * FROM test_at_change_type where b = 3;
ALTER TABLE test_at_change_type CHANGE b b1 BOOLEAN;
SELECT * FROM test_at_change_type ORDER BY 1,2;
DROP TABLE test_at_change_type;
CREATE TABLE test_at_change_type(
    a int,
    b text
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_type VALUES(1,'beijing');
INSERT INTO test_at_change_type VALUES(2,'shanghai');
INSERT INTO test_at_change_type VALUES(3,'guangzhou');
ALTER TABLE test_at_change_type CHANGE b b1 SET('beijing','shanghai','nanjing','wuhan'); -- ERROR
ALTER TABLE test_at_change_type CHANGE b b1 SET('beijing','shanghai','nanjing','guangzhou');
ALTER TABLE test_at_change_type CHANGE b1 b SET('beijing','shanghai','guangzhou','wuhan'); -- ERROR
select pg_get_tabledef('test_at_change_type'::regclass);
DROP TABLE test_at_change_type;
CREATE TABLE test_at_change_type(
    a int,
    b varchar(32)
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_type VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_change_type VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_change_type VALUES(3,'2022-11-24 12:00:00');
ALTER TABLE test_at_change_type CHANGE b b1 varchar(10); -- ERROR
ALTER TABLE test_at_change_type CHANGE b b1 DATE;
SELECT * FROM test_at_change_type ORDER BY 1,2;
DROP TABLE test_at_change_type;

-- test change column constraint
CREATE TABLE test_at_change_constr(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
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
select pg_get_tabledef('test_at_change_constr'::regclass);
ALTER TABLE test_at_change_constr CHANGE b1 b int NOT NULL REFERENCES test_at_ref (a); -- ERROR
DROP TABLE test_at_change_constr;

-- test change column default
CREATE TABLE test_at_change_default(
    a int,
    b int DEFAULT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_default VALUES(1,1);
INSERT INTO test_at_change_default VALUES(2,2);
INSERT INTO test_at_change_default VALUES(3,3);
ALTER TABLE test_at_change_default CHANGE b b1 bigint DEFAULT (a+1); -- ERROR
ALTER TABLE test_at_change_default CHANGE b b1 bigint DEFAULT NULL;
\d+ test_at_change_default;
ALTER TABLE test_at_change_default CHANGE b1 b varchar(8) DEFAULT 'a' GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_change_default CHANGE b1 b varchar(8) DEFAULT 'a';
\d+ test_at_change_default;
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
\d+ test_at_change_default;
DROP TABLE test_at_change_default;

-- test change column depended by generated column
CREATE TABLE test_at_change_generated(
    a int,
    b varchar(32),
    c varchar(32) GENERATED ALWAYS AS (b) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_generated(a,b) VALUES(1,'2022-11-22 12:00:00');
INSERT INTO test_at_change_generated(a,b) VALUES(2,'2022-11-23 12:00:00');
INSERT INTO test_at_change_generated(a,b) VALUES(3,'2022-11-24 12:00:00');
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN b b1 DATE;
SELECT * FROM test_at_change_generated ORDER BY 1,2;
\d+ test_at_change_generated
ALTER TABLE test_at_change_generated CHANGE COLUMN b1 b varchar(32) AFTER c;
SELECT * FROM test_at_change_generated ORDER BY 1,2;
DROP TABLE test_at_change_generated;
CREATE TABLE test_at_change_generated(
    a int,
    b int GENERATED ALWAYS AS (a+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_generated(a,b) VALUES(-1,DEFAULT);
INSERT INTO test_at_change_generated(a,b) VALUES(0,DEFAULT);
INSERT INTO test_at_change_generated(a,b) VALUES(1,DEFAULT);
ALTER TABLE test_at_change_generated CHANGE COLUMN a a1 bool;
ALTER TABLE test_at_change_generated CHANGE COLUMN a1 a int;
INSERT INTO test_at_change_generated(a,b) VALUES(100,DEFAULT);
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN a a1 bool, MODIFY COLUMN b varchar(32);
\d test_at_change_generated
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated MODIFY COLUMN b int GENERATED ALWAYS AS (a+1) STORED, CHANGE COLUMN a1 a int;
\d test_at_change_generated
SELECT * FROM test_at_change_generated ORDER BY 1,2;
INSERT INTO test_at_change_generated(a,b) VALUES(100,DEFAULT);
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated MODIFY COLUMN b bool GENERATED ALWAYS AS (a) STORED;
\d test_at_change_generated
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN a a1 bool;
\d test_at_change_generated
SELECT * FROM test_at_change_generated ORDER BY 1,2;
ALTER TABLE test_at_change_generated CHANGE COLUMN a1 a int;
INSERT INTO test_at_change_generated(a,b) VALUES(100,DEFAULT);
\d test_at_change_generated
SELECT * FROM test_at_change_generated ORDER BY 1,2;
DROP TABLE test_at_change_generated;

-- test change column AUTO_INCREMENT
CREATE TABLE test_at_change_autoinc(
    a int,
    b int
) WITH(STORAGE_TYPE=USTORE);
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
ALTER TABLE test_at_change_autoinc DROP COLUMN c , CHANGE b1 b int2 AUTO_INCREMENT UNIQUE KEY FIRST;
INSERT INTO test_at_change_autoinc(a,b) VALUES(8,0);
SELECT * FROM test_at_change_autoinc ORDER BY 2,1;
DROP TABLE test_at_change_autoinc;

-- test change column depended by other objects
CREATE TABLE test_at_change_depend(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_depend VALUES(1,1);
INSERT INTO test_at_change_depend VALUES(2,2);
INSERT INTO test_at_change_depend VALUES(3,3);
-- --PROCEDURE contains column
CREATE OR REPLACE PROCEDURE test_at_change_proc(IN p_in int)
    AS
    BEGIN
        INSERT INTO test_at_change_depend(a,b) VALUES(p_in, p_in);
    END;
/
ALTER TABLE test_at_change_depend CHANGE b b1 varchar(8) NOT NULL;
CALL test_at_change_proc(2); -- ERROR
DROP PROCEDURE test_at_change_proc;

-- --TRIGGER contains and depends column
CREATE OR REPLACE FUNCTION tg_bf_test_at_change_func() RETURNS TRIGGER AS
$$
    DECLARE
    BEGIN
        UPDATE test_at_change_depend SET b1 = NULL WHERE a < NEW.a;
        RETURN NEW;
    END
$$ LANGUAGE PLPGSQL;
CREATE TRIGGER tg_bf_test_at_change
    AFTER UPDATE ON test_at_change_depend
    FOR EACH ROW WHEN ( NEW.b1 IS NULL AND OLD.b1 = OLD.a)
    EXECUTE PROCEDURE tg_bf_test_at_change_func();
ALTER TABLE test_at_change_depend CHANGE b1 b varchar(8) NULL DEFAULT '0';
UPDATE test_at_change_depend SET b = NULL WHERE a = 2; -- ERROR
DROP TRIGGER tg_bf_test_at_change ON test_at_change_depend;
DROP FUNCTION tg_bf_test_at_change_func;

-- --VIEW depends column
CREATE VIEW test_at_change_view AS SELECT b FROM test_at_change_depend;
ALTER TABLE test_at_change_depend CHANGE b b1 bigint NULL; -- ERROR
ALTER TABLE test_at_change_depend CHANGE b b1 int NULL; -- ERROR
ALTER TABLE test_at_change_depend CHANGE b b1 varchar(8) NULL;
SELECT b FROM test_at_change_view ORDER BY 1;
DROP VIEW test_at_change_view;
CREATE VIEW test_at_change_view AS SELECT a FROM test_at_change_depend where b1 > 0;
CREATE VIEW test_at_change_view1 AS SELECT * FROM test_at_change_view;
ALTER TABLE test_at_change_depend CHANGE b1 b bigint NULL GENERATED ALWAYS AS (a+1);
ALTER TABLE test_at_change_depend CHANGE b b1 varchar(8) NULL;
ALTER TABLE test_at_change_depend CHANGE b1 b int NULL;
SELECT * FROM test_at_change_view1 ORDER BY 1;
DROP VIEW test_at_change_view1;
DROP VIEW test_at_change_view;
CREATE materialized VIEW test_at_change_view AS SELECT b FROM test_at_change_depend; -- ERROR

-- --TABLE reference column.
DELETE FROM test_at_change_depend;
ALTER TABLE test_at_change_depend CHANGE b b INT PRIMARY KEY;
CREATE TABLE test_at_change_ref(
    a int,
    b int,
    FOREIGN KEY (b) REFERENCES test_at_change_depend(b) ON DELETE SET NULL
) WITH(STORAGE_TYPE=USTORE);
ALTER TABLE test_at_change_depend CHANGE COLUMN b b1 varchar(8);
INSERT INTO test_at_change_ref VALUES(0,0); -- ERROR
INSERT INTO test_at_change_depend VALUES(0,0);
INSERT INTO test_at_change_ref VALUES(0,0);
ALTER TABLE test_at_change_ref CHANGE COLUMN b b1 varchar(8) GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_change_ref CHANGE COLUMN b b1 varchar(8);
\d+ test_at_change_ref
DROP TABLE test_at_change_ref;

-- --TABLE reference self column.
CREATE TABLE test_at_change_ref(
    a int PRIMARY KEY,
    b int,
    FOREIGN KEY (b) REFERENCES test_at_change_ref(a) ON DELETE SET NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_ref VALUES(0,0);
ALTER TABLE test_at_change_ref CHANGE COLUMN b b1 varchar(8) GENERATED ALWAYS AS (a+1) STORED; -- ERROR
ALTER TABLE test_at_change_ref CHANGE COLUMN b b1 varchar(8);
ALTER TABLE test_at_change_ref CHANGE COLUMN a a1 varchar(8);
INSERT INTO test_at_change_ref VALUES('a','a');
DROP TABLE test_at_change_ref;

-- --RULE reference column.
CREATE RULE test_at_change_rule AS ON INSERT TO test_at_change_depend WHERE (b1 is null) DO INSTEAD UPDATE test_at_change_depend SET b1=0;
ALTER TABLE test_at_change_depend CHANGE COLUMN b1 b bigint not null; -- ERROR
DROP RULE test_at_change_rule ON test_at_change_depend;

-- --RLSPOLICY reference column.
DROP TABLE test_at_change_depend;
CREATE ROLE at_change_ustore_role PASSWORD 'Gauss@123';
CREATE TABLE test_at_change_depend(
    a int,
    b int NOT NULL
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_change_depend VALUES(0,0);
GRANT USAGE ON SCHEMA atbdb_ustore_schema TO at_change_ustore_role;
GRANT SELECT ON test_at_change_depend TO at_change_ustore_role;
ALTER TABLE test_at_change_depend ENABLE ROW LEVEL SECURITY;
CREATE ROW LEVEL SECURITY POLICY test_at_change_rls ON test_at_change_depend AS RESTRICTIVE FOR SELECT TO at_change_ustore_role USING(b >= 20);
ALTER TABLE test_at_change_depend CHANGE COLUMN b b1 int not null;
INSERT INTO test_at_change_depend VALUES(21,21);
SET ROLE at_change_ustore_role PASSWORD 'Gauss@123';
SELECT * FROM test_at_change_depend ORDER BY 1,2;
RESET ROLE;
SELECT * FROM test_at_change_depend ORDER BY 1,2;
ALTER TABLE test_at_change_depend CHANGE COLUMN b1 b2 bool not null;
ALTER TABLE test_at_change_depend CHANGE COLUMN b2 b3 int not null;
INSERT INTO test_at_change_depend VALUES(22,22);
SET ROLE at_change_ustore_role PASSWORD 'Gauss@123';
SELECT * FROM test_at_change_depend ORDER BY 1,2;
RESET ROLE;
SELECT * FROM test_at_change_depend ORDER BY 1,2;
DROP TABLE test_at_change_depend;
REVOKE ALL PRIVILEGES ON SCHEMA atbdb_ustore_schema FROM at_change_ustore_role;
DROP ROLE at_change_ustore_role;

-- test alter command order
CREATE TABLE test_at_pass(
    a int,
    b int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_pass VALUES(1,0);
ALTER TABLE test_at_pass ADD COLUMN c int, DROP COLUMN c; -- ERROR
ALTER TABLE test_at_pass ADD COLUMN c int DEFAULT 0, MODIFY COLUMN c bigint; -- ERROR
ALTER TABLE test_at_pass ADD COLUMN c int DEFAULT 0, CHANGE COLUMN c c1 bigint; -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN b bigint, MODIFY COLUMN b float4; -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN b bigint, CHANGE COLUMN b b1 float4; -- ERROR
ALTER TABLE test_at_pass CHANGE COLUMN b b1 bigint, CHANGE COLUMN b1 b2 bigint; -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN b bigint, DROP COLUMN b; -- ERROR
ALTER TABLE test_at_pass CHANGE COLUMN b b1 bigint, DROP COLUMN b; -- ERROR
ALTER TABLE test_at_pass CHANGE COLUMN b b1 bigint, DROP COLUMN b1; -- ERROR
ALTER TABLE test_at_pass MODIFY a bigint, MODIFY COLUMN a VARCHAR(8); -- ERROR
ALTER TABLE test_at_pass CHANGE COLUMN b a bigint, CHANGE COLUMN a b VARCHAR(8); -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN b bigint, ALTER COLUMN b SET DEFAULT 100;
\d test_at_pass
ALTER TABLE test_at_pass MODIFY COLUMN b bigint DEFAULT 100, ALTER COLUMN b DROP DEFAULT;
\d test_at_pass
ALTER TABLE test_at_pass CHANGE COLUMN b b1 bigint, ALTER COLUMN b1 SET DEFAULT 100;
\d test_at_pass
ALTER TABLE test_at_pass CHANGE COLUMN b1 b bigint DEFAULT 100, ALTER COLUMN b DROP DEFAULT;
\d test_at_pass
ALTER TABLE test_at_pass CHANGE COLUMN b b1 bigint DEFAULT 100, ALTER COLUMN b DROP DEFAULT; -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN a bigint CONSTRAINT atpass_pk PRIMARY KEY, DROP CONSTRAINT atpass_pk; -- ERROR
ALTER TABLE test_at_pass MODIFY COLUMN a bigint CONSTRAINT atpass_pk PRIMARY KEY, ADD CONSTRAINT atpass_pk PRIMARY KEY(a); -- ERROR
DROP TABLE test_at_pass;

-- test complex commands combined
CREATE TABLE test_at_complex(
    a int,
    b int GENERATED ALWAYS AS (a+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_complex VALUES(0,DEFAULT);
INSERT INTO test_at_complex VALUES(1,DEFAULT);
INSERT INTO test_at_complex VALUES(2,DEFAULT);
INSERT INTO test_at_complex VALUES(-1,DEFAULT);
ALTER TABLE test_at_complex MODIFY COLUMN a varchar(8), MODIFY COLUMN b int AUTO_INCREMENT UNIQUE;
INSERT INTO test_at_complex VALUES(3,DEFAULT);
SELECT * FROM test_at_complex ORDER BY a::int,b::int;
DROP TABLE test_at_complex;

CREATE TABLE test_at_complex(
    a int,
    b int GENERATED ALWAYS AS (a+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_complex VALUES(0,DEFAULT);
INSERT INTO test_at_complex VALUES(1,DEFAULT);
INSERT INTO test_at_complex VALUES(2,DEFAULT);
INSERT INTO test_at_complex VALUES(-1,DEFAULT);
ALTER TABLE test_at_complex MODIFY COLUMN b int AUTO_INCREMENT UNIQUE, MODIFY COLUMN a varchar(8);
INSERT INTO test_at_complex VALUES(3,DEFAULT);
SELECT * FROM test_at_complex ORDER BY a::int,b::int;
DROP TABLE test_at_complex;

-- test modify partitioned table column without data
CREATE TABLE pt_at_modify (a int, b int NOT NULL, PRIMARY KEY(b,a)) WITH(STORAGE_TYPE=USTORE)
PARTITION BY RANGE (a)
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (1000),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);
ALTER TABLE pt_at_modify MODIFY a int8 DEFAULT 0; -- ERROR
ALTER TABLE pt_at_modify MODIFY a int DEFAULT 0;
ALTER TABLE pt_at_modify MODIFY a int GENERATED ALWAYS AS (b+1) STORED; -- ERROR
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int8 NULL;
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int8 DEFAULT 0;
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int AUTO_INCREMENT;
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int2 UNIQUE;
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int CHECK (b < 10000);
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b varchar(8) COLLATE "POSIX";
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b int8 GENERATED ALWAYS AS (a+1) STORED;
\d+ pt_at_modify;
ALTER TABLE pt_at_modify MODIFY b varchar(8) NOT NULL;
\d+ pt_at_modify;
select pg_get_tabledef('pt_at_modify'::regclass);
INSERT INTO pt_at_modify VALUES(1,1);
DROP TABLE pt_at_modify;

-- test alter modify first after
CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,13);
INSERT INTO test_at_modify_fa VALUES(21,22,23);
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (a+100) STORED AFTER a, MODIFY c float4 FIRST;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,13);
INSERT INTO test_at_modify_fa VALUES(21,22,23);
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (a+100) STORED FIRST, MODIFY c float4 GENERATED ALWAYS AS (b+100) STORED AFTER a;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,0);
INSERT INTO test_at_modify_fa VALUES(21,22,0);
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (a+100) STORED FIRST, MODIFY c bigint AUTO_INCREMENT PRIMARY KEY AFTER a;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
INSERT INTO test_at_modify_fa(a,b,c) VALUES(31,32,NULL);
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int GENERATED ALWAYS AS (b+1) STORED
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,DEFAULT);
INSERT INTO test_at_modify_fa VALUES(11,12,DEFAULT);
INSERT INTO test_at_modify_fa VALUES(21,22,DEFAULT);
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (a+100) STORED AFTER a, MODIFY b float4 GENERATED ALWAYS AS (a+1000) STORED FIRST; -- ERROR
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (b+100) STORED AFTER a, MODIFY a float4 GENERATED ALWAYS AS (b+1000) STORED FIRST;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,13);
INSERT INTO test_at_modify_fa VALUES(21,22,23);
SELECT a,b,c FROM test_at_modify_fa ORDER BY 1,2,3;
ALTER TABLE test_at_modify_fa ADD COLUMN d int GENERATED ALWAYS AS (a+100) STORED AFTER a, ADD COLUMN e int GENERATED ALWAYS AS (b+100) STORED FIRST;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,13);
INSERT INTO test_at_modify_fa VALUES(21,22,23);
SELECT a,b,c FROM test_at_modify_fa ORDER BY 1,2,3;
ALTER TABLE test_at_modify_fa ADD COLUMN d bigint AUTO_INCREMENT PRIMARY KEY AFTER a, ADD COLUMN e int GENERATED ALWAYS AS (b+100) STORED FIRST;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
INSERT INTO test_at_modify_fa(a,b,c) VALUES(31,32,33);
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3,4;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,13);
INSERT INTO test_at_modify_fa VALUES(21,22,23);
ALTER TABLE test_at_modify_fa MODIFY COLUMN c float4 GENERATED ALWAYS AS (a+100) STORED FIRST, MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED AFTER c; -- ERROR
ALTER TABLE test_at_modify_fa MODIFY COLUMN c float4 GENERATED ALWAYS AS (a+100) STORED FIRST, MODIFY COLUMN b int GENERATED ALWAYS AS (a+100) STORED AFTER c;
SELECT * FROM test_at_modify_fa ORDER BY 1,2,3;
DROP TABLE test_at_modify_fa;

CREATE TABLE test_at_modify_fa(
    a int,
    b int,
    c int
) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_fa VALUES(1,2,3);
INSERT INTO test_at_modify_fa VALUES(11,12,0);
INSERT INTO test_at_modify_fa VALUES(21,22,0);
ALTER TABLE test_at_modify_fa MODIFY COLUMN c bigint AUTO_INCREMENT PRIMARY KEY FIRST, MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED AFTER c; -- ERROR
ALTER TABLE test_at_modify_fa MODIFY COLUMN b int GENERATED ALWAYS AS (c+100) STORED AFTER c, MODIFY COLUMN c bigint AUTO_INCREMENT PRIMARY KEY FIRST; -- ERROR
DROP TABLE test_at_modify_fa;

-- primary key should be not null after modify 
create table test11(f11 int, f12 varchar(20), f13 bool, CONSTRAINT pk_test11_f11 primary key (f11)) WITH(STORAGE_TYPE=USTORE);
\d test11
ALTER TABLE test11 MODIFY COLUMN f11 int;
\d test11
ALTER TABLE test11 MODIFY COLUMN f11 int AFTER f13;
\d test11
ALTER TABLE test11 DROP CONSTRAINT pk_test11_f11, MODIFY COLUMN f11 int NULL;
\d test11
ALTER TABLE test11 ADD CONSTRAINT pk_test11_f11 primary key (f11), MODIFY COLUMN f11 int NULL;
\d test11
insert into test11(f11,f12,f13) values(NULL,'1',true); --ERROR
drop table test11;
-- primary keys should be not null after modify 
create table test11(f11 int, f12 varchar(20), f13 bool, CONSTRAINT pk_test11_f11 primary key (f11,f12)) WITH(STORAGE_TYPE=USTORE);
\d test11
ALTER TABLE test11 MODIFY COLUMN f11 int;
\d test11
ALTER TABLE test11 MODIFY f11 int AFTER f13;
\d test11
ALTER TABLE test11 DROP CONSTRAINT pk_test11_f11, MODIFY COLUMN f11 int NULL;
\d test11
ALTER TABLE test11 ADD CONSTRAINT pk_test11_f11 primary key (f11), MODIFY COLUMN f11 int NULL;
\d test11
insert into test11(f11,f12,f13) values(NULL,'1',true); --ERROR
drop table test11;
-- primary keys in partition table should be not null after modify 
create table range_range(id int, gender varchar not null, birthday date not null) WITH(STORAGE_TYPE=USTORE)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
ALTER TABLE range_range ADD CONSTRAINT range_range_pkey primary KEY USING ubtree  (id, birthday);
\d+ range_range
ALTER TABLE range_range MODIFY COLUMN id int AFTER birthday;
\d+ range_range
drop table if exists range_range cascade;
-- primary key in partition table should be not null after modify 
create table range_range(id int, gender varchar not null, birthday date not null) WITH(STORAGE_TYPE=USTORE)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
ALTER TABLE range_range ADD CONSTRAINT range_range_pkey primary KEY USING ubtree  (id);
\d+ range_range
ALTER TABLE range_range MODIFY COLUMN id int AFTER birthday;
\d+ range_range
drop table if exists range_range cascade;
-- primary key in partition table should be not null after modify 
create table range_range(id int, gender varchar not null, birthday date not null) WITH(STORAGE_TYPE=USTORE)
partition by range (id) subpartition by range (birthday)
(
        partition p_1 values less than(100)
        (
                subpartition p_1_a values less than('2022-01-01'),
                subpartition p_1_b values less than(MAXVALUE)
        ),
        partition p_2 values less than(200)
        (
                subpartition p_2_a values less than('2022-01-01'),
                subpartition p_2_b values less than(MAXVALUE)
        ),
        partition p_3 values less than(MAXVALUE)
        (
                subpartition p_3_a values less than('2022-01-01'),
                subpartition p_3_b values less than(MAXVALUE)
        )
);
ALTER TABLE range_range ADD CONSTRAINT range_range_pkey primary KEY USING ubtree  (gender);
\d+ range_range
ALTER TABLE range_range MODIFY COLUMN gender varchar AFTER birthday;
\d+ range_range
drop table if exists range_range cascade;
-- primary keys in multi range keys partition table should be not null after modify 
create table multi_keys_range(f1 int, f2 int, f3 int) WITH(STORAGE_TYPE=USTORE)
partition by range(f1, f2)
(
        partition multi_keys_range_p0 values less than (10, 0),
        partition multi_keys_range_p1 values less than (20, 0),
        partition multi_keys_range_p2 values less than (30, 0)
);
-- primary key should be LOCAL INDEX
alter table multi_keys_range modify f1 int after f3, ADD CONSTRAINT multi_keys_range_pkey PRIMARY KEY USING ubtree (f1,f2);
\d+ multi_keys_range
alter table multi_keys_range modify f2 int after f3;
\d+ multi_keys_range
drop table if exists multi_keys_range cascade;
-- primary keys in multi list keys partition table should be not null after modify 
create table multi_keys_list(f1 int, f2 int, f3 int) WITH(STORAGE_TYPE=USTORE)
partition by list(f1, f2)
(
        partition multi_keys_list_p0 values ((10, 0)),
        partition multi_keys_list_p1 values ((20, 0)),
        partition multi_keys_list_p2 values (DEFAULT)
);
-- primary key should be LOCAL INDEX
alter table multi_keys_list modify f1 int after f3, ADD CONSTRAINT multi_keys_list_pkey PRIMARY KEY USING ubtree (f1,f2);
\d+ multi_keys_list
alter table multi_keys_list modify f2 int after f3;
\d+ multi_keys_list
drop table if exists multi_keys_list cascade;

-- test moidfy/change VIEW depends column
-- --modify
-- -- --test select *
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_star AS select * from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int FIRST;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test select * with add column
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_star AS select * from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20), ADD COLUMN f0 int;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int, ADD COLUMN f0 int;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int FIRST, ADD COLUMN f5 int FIRST;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test select * special
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
create view test_modify_view_star(col1,col2) as
SELECT * FROM
(
    SELECT
        CAST(f1/10000 AS DECIMAL(18,2)),
        CAST(CAST(f4 AS DECIMAL(18,4))/f1*100 AS DECIMAL(18,2))
    FROM test_at_modify_view_column
);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20);
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column MODIFY column f1 int FIRST; -- ERROR
ALTER TABLE test_at_modify_view_column ADD COLUMN f5 int FIRST; -- ERROR
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test modify view column
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_f1f2 WITH(security_barrier=TRUE) AS select F1,F2 from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int;
SELECT pg_get_viewdef('test_modify_view_f1f2'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int FIRST;
SELECT pg_get_viewdef('test_modify_view_f1f2'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test view and column name
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW "test_modify_view_f1f2F3" AS select F1,F2,F3 AS "F3" from test_at_modify_view_column where f4 > 0;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f1 int;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f4 varchar(20);
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  MODIFY column f4 int AFTER f1;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test drop column
CREATE TABLE test_at_modify_view_column (f5 int, f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(5, 4, '3', '2', 1);
CREATE VIEW "test_modify_view_f1f2F3" AS select F1,F2,F3 AS "F3" from test_at_modify_view_column where f4 > 0;
ALTER TABLE test_at_modify_view_column  MODIFY column f1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  MODIFY column f4 varchar(20), DROP COLUMN f5;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
SELECT * FROM "test_modify_view_f1f2F3";
ALTER TABLE test_at_modify_view_column  MODIFY column f4 int AFTER f1, DROP COLUMN f5;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
SELECT * FROM "test_modify_view_f1f2F3";
DROP VIEW "test_modify_view_f1f2F3";
DROP TABLE test_at_modify_view_column CASCADE;
-- --change
-- -- --test select *
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_star AS select * from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 int;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
ALTER TABLE test_at_modify_view_column  CHANGE column c1 f1 int FIRST;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test select * with add column
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_star AS select * from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20), ADD COLUMN f0 int; -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 int, ADD COLUMN f0 int;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column  CHANGE column c1 f1 int FIRST, ADD COLUMN f5 int FIRST;
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test select * special
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
create view test_modify_view_star(col1,col2) as
SELECT * FROM
(
    SELECT
        CAST(f1/10000 AS DECIMAL(18,2)),
        CAST(CAST(f4 AS DECIMAL(18,4))/f1*100 AS DECIMAL(18,2))
    FROM test_at_modify_view_column
);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20);
SELECT pg_get_viewdef('test_modify_view_star'::regclass);
SELECT * FROM test_modify_view_star;
ALTER TABLE test_at_modify_view_column CHANGE column c1 f1 int FIRST; -- ERROR
ALTER TABLE test_at_modify_view_column ADD COLUMN f5 int FIRST; -- ERROR
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test CHANGE view column
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW test_modify_view_f1f2 WITH(security_barrier=TRUE) AS select F1,F2 from test_at_modify_view_column;
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 int;
SELECT pg_get_viewdef('test_modify_view_f1f2'::regclass);
ALTER TABLE test_at_modify_view_column  CHANGE column c1 f1 int FIRST;
SELECT pg_get_viewdef('test_modify_view_f1f2'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test view and column name
CREATE TABLE test_at_modify_view_column (f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(4, '3', '2', 1);
CREATE VIEW "test_modify_view_f1f2F3" AS select F1,F2,F3 AS "F3" from test_at_modify_view_column where f4 > 0;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 int;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  CHANGE column f4 c4 varchar(20);
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
ALTER TABLE test_at_modify_view_column  CHANGE column c4 f4 int AFTER c1;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
DROP TABLE test_at_modify_view_column CASCADE;
-- -- --test drop column
CREATE TABLE test_at_modify_view_column (f5 int, f4 int primary key, f3 text, f2 text, f1 int unique) WITH(STORAGE_TYPE=USTORE);
INSERT INTO test_at_modify_view_column VALUES(5, 4, '3', '2', 1);
CREATE VIEW "test_modify_view_f1f2F3" AS select F1,F2,F3 AS "F3" from test_at_modify_view_column where f4 > 0;
ALTER TABLE test_at_modify_view_column  CHANGE column f1 c1 varchar(20); -- ERROR
ALTER TABLE test_at_modify_view_column  CHANGE column f4 c4 varchar(20), DROP COLUMN f5;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
SELECT * FROM "test_modify_view_f1f2F3";
ALTER TABLE test_at_modify_view_column  CHANGE column c4 f4 int AFTER f1, DROP COLUMN f5;
SELECT pg_get_viewdef('"test_modify_view_f1f2F3"'::regclass);
SELECT * FROM "test_modify_view_f1f2F3";
DROP VIEW "test_modify_view_f1f2F3";
DROP TABLE test_at_modify_view_column CASCADE;

-- END
RESET CURRENT_SCHEMA;
DROP SCHEMA atbdb_ustore_schema CASCADE;
\c regression
clean connection to all force for database atbdb_ustore;
drop database if exists atbdb_ustore;
