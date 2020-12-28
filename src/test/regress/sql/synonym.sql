--
-- Tests for SYNONYMS
--

CREATE SCHEMA syn_test;
CREATE SCHEMA pablic;
grant all on schema pablic to public;
SET CURRENT_SCHEMA = syn_test;
-- 0. Precondition, create referenced objects including table, view, funciton and procedure.
CREATE SCHEMA syn_ot;

CREATE TABLE syn_ot.t1_row(c1 int, c2 int);
CREATE TABLE syn_ot.t2_col(c1 int, c2 varchar2(10)) WITH(orientation = column) DISTRIBUTE BY HASH(c1);
CREATE TABLE syn_ot.t3_part(c1 int, c2 int) PARTITION BY range(c2)
(
	PARTITION p1 VALUES LESS THAN (5),
	PARTITION p2 VALUES LESS THAN (10),
	PARTITION p3 VALUES LESS THAN (maxvalue)
);

CREATE VIEW syn_ot.v_t1 AS SELECT * FROM syn_ot.t1_row;
CREATE VIEW syn_ot.v_t2 AS SELECT * FROM syn_ot.t2_col ORDER BY c1;
CREATE VIEW syn_ot.v_t3 AS SELECT c1, max(c2) FROM syn_ot.t3_part GROUP BY c1;

CREATE PROCEDURE syn_ot.insert_data(v_a integer, v_b integer)
SECURITY INVOKER
AS
BEGIN
	INSERT INTO syn_ot.t1_row VALUES(v_a, v_b);
END;
/

CREATE OR REPLACE FUNCTION syn_ot.add(a integer, b integer) RETURNS integer AS
$$
SELECT $1 + $2
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION syn_ot.add(a decimal(5,2), b decimal(5,2)) RETURNS decimal(5,2) AS
$$
SELECT $1 + $2
$$
LANGUAGE sql;

-- Case 1. SYNONYM FOR TABLE OBJECT
-- 1.1 row-store table
CREATE SYNONYM t1 FOR syn_ot.t1_row;
SELECT * FROM t1 ORDER BY 1;
INSERT INTO t1 VALUES(1,1),(2,2);
SELECT * FROM t1 ORDER BY 1;
UPDATE t1 SET c2 = c2 * 3 WHERE c2 = 1;
SELECT * FROM t1 ORDER BY 1;
DELETE FROM t1 WHERE c2 > 5;
SELECT * FROM t1 ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

-- 1.2 col-store table
CREATE SYNONYM t2 FOR syn_ot.t2_col;
SELECT * FROM t2 ORDER BY 1;
INSERT INTO t2 VALUES(1, 'aa'),(2, 'bb');
SELECT * FROM t2 ORDER BY 1;
UPDATE t2 SET c2 = upper(c2) WHERE c1 = 2;
SELECT * FROM t2 ORDER BY 1;
DELETE FROM t2 WHERE c1 = 2;
SELECT * FROM t2 ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

-- 1.3 partition table
CREATE SYNONYM t3 FOR syn_ot.t3_part;
SELECT * FROM t3 ORDER BY 1;
INSERT INTO t3 VALUES(1,2),(2,6),(3,12);
SELECT * FROM t3 ORDER BY 1;
UPDATE t3 SET c2 = 10 WHERE c1 = 3;
SELECT * FROM t3 ORDER BY 1;
DELETE FROM t3 WHERE c2 < 5;
SELECT * FROM t3 ORDER BY 1;
SELECT * FROM t3 PARTITION(p1) ORDER BY 1;
SELECT * FROM t3 PARTITION(p3) ORDER BY 1;

CREATE TABLE pt3 (LIKE syn_ot.t3_part INCLUDING PARTITION);
CREATE TABLE pst3 (LIKE t3 INCLUDING PARTITION);

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

DROP SYNONYM t1;
SELECT count(*) FROM pg_synonym;
DROP SYNONYM t2;
SELECT count(*) FROM pg_synonym;
DROP SYNONYM t3;
SELECT count(*) FROM pg_synonym;

SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

-- Case 2. SYNONYM FOR VIEW OBJECT
CREATE SYNONYM v1 FOR syn_ot.v_t1;
SELECT * FROM v1 ORDER BY 1;
CREATE SYNONYM v2 FOR syn_ot.v_t2;
SELECT * FROM v2 ORDER BY 1;
CREATE SYNONYM v3 FOR syn_ot.v_t3;
SELECT * FROM v3 ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

DROP SYNONYM v1;
DROP SYNONYM v2;

SELECT count(*) FROM pg_synonym;

-- Case 3. SYNONYM FOR FUNCTION OBJECT
SELECT syn_ot.add(1,1);
SELECT syn_ot.add(1.1, 1.2);

CREATE SYNONYM add FOR syn_ot.add;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

SELECT add(1,2);
SELECT add(1.2, 2.3);

SELECT count(*) FROM pg_synonym;

DROP SYNONYM add;

SELECT count(*) FROM pg_synonym;

-- Case 4. SYNONYM FOR PROCEDURE OBJECT
CREATE SYNONYM insert_data FOR syn_ot.insert_data;
CALL insert_data(3,3);
SELECT * FROM syn_ot.t1_row ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

DROP SYNONYM IF EXISTS insert_data;
SELECT count(*) FROM pg_synonym;

-- Case 5. synonym object has the same name with database objects, choose db object.
CREATE SYNONYM t1 FOR syn_ot.t1_row;
CREATE TABLE t1(a int, b int, c int);

-- columns of table t1, not table syn_ot.t1_row, [a, b, c]
SELECT * FROM t1 ORDER BY 1;
-- ok to insert
INSERT INTO t1 VALUES(0, 0, 0); 

-- Case 6. The essence of synonyms is a mapping between names.
-- if referenced object is not existed, failed to do select.
CREATE SYNONYM tt FOR syn_ot.tt_row;
SELECT * FROM tt;
INSERT INTO tt VALUES(1,1,1);
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

CREATE SYNONYM ww FOR syn_ww.tt_row;
SELECT * FROM tt;
CREATE SYNONYM pp FOR pp_row;
SELECT * FROM pp;
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

-- if the referenced object is created, okay to do select.
CREATE TABLE syn_ot.tt_row(id int, name varchar2(5), birth date);
INSERT INTO syn_ot.tt_row VALUES(1, 'July', '2000-01-01');
SELECT * FROM tt ORDER BY 1; 
INSERT INTO tt VALUES(2, 'Mia', '2008-08-08');
SELECT * FROM tt ORDER BY 1;

CREATE SCHEMA syn_ww;
CREATE TABLE syn_ww.tt_row(a int);
CREATE TABLE pp_row(a int);

SELECT * FROM ww ORDER BY 1;
SELECT * FROM pp ORDER BY 1;

-- for table in node group, also ok to create synonym and do something.
CREATE NODE GROUP grp1 WITH(datanode1, datanode2);
CREATE TABLE syn_ot.t1_grp1( id int, name varchar2(5)) TO GROUP grp1;
INSERT INTO syn_ot.t1_grp1 VALUES(1, 'AA');

CREATE SYNONYM gt1 FOR syn_ot.t1_grp1;
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
INSERT INTO gt1 VALUES(2, 'bb');
SELECT * FROM gt1;

CREATE TABLE ggtt1 (LIKE syn_ot.t1_grp1 INCLUDING ALL);
CREATE TABLE ggtt2 (LIKE gt1 INCLUDING ALL);

-- if referencing other unsupported objects, such as type, sequence, another synonym, failed.
CREATE OR REPLACE SYNONYM ww for syn_ww.sw1;
CREATE TYPE syn_ww.sw1 AS (a int, b int);
---- use synonym to create table, report error.
CREATE TABLE mm (a int, b ww);
SELECT * FROM ww;
DROP TABLE IF EXISTS mm;
DROP TYPE syn_ww.sw1;

---- use synonym to select sequence, report error.
CREATE SEQUENCE syn_ww.sw1 START 101 CACHE 20;
SELECT nextval('syn_ww.sw1');
SELECT nextval('ww');

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

---- synonym reference another synonym, report error.
CREATE OR REPLACE SYNONYM ww for syn_test.pp;
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT * FROM ww;

-- Case 7. name too long and truncate
CREATE SYNONYM tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttoolong FOR syn_ot.t1_row;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
DROP SYNONYM IF EXISTS tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttoo;

-- Case 8. the type of referenced object changed and then mapping automatically.
CREATE SYNONYM rtt1 FOR syn_ot.tt_row;
SELECT * FROM rtt1 ORDER BY 1;
ALTER TABLE syn_ot.tt_row ADD avg_score NUMBER(1);
SELECT * FROM rtt1 ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

CREATE TABLE syn_ot.t1_ori (c1 int, c2 int);
CREATE SYNONYM rtt2 FOR syn_ot.t1_ori;
INSERT INTO rtt2 VALUES(1,1);
SELECT * FROM rtt2 ORDER BY 1;
SELECT synname, synobjschema, synobjname FROM pg_synonym WHERE synname = 'rtt2';

DROP TABLE syn_ot.t1_ori;
SELECT * FROM rtt2 ORDER BY 1;

CREATE VIEW syn_ot.t1_ori AS SELECT * FROM syn_ot.t1_row ORDER BY 1;
SELECT * FROM rtt2 ORDER BY 1;
SELECT synname, synobjschema, synobjname FROM pg_synonym WHERE synname = 'rtt2';

-- Case 9. about privilege
CREATE USER u_tmp PASSWORD 'utmp@123';
GRANT CREATE ON DATABASE regression to u_tmp;
SET SESSION AUTHORIZATION u_tmp PASSWORD 'utmp@123';

CREATE SCHEMA rt_priv;
REVOKE CREATE ON SCHEMA rt_priv FROM u_tmp;
CREATE SYNONYM rt_priv.t1 FOR syn_ot.t1_row;

GRANT CREATE ON SCHEMA rt_priv TO u_tmp;
CREATE SYNONYM rt_priv.tt1 FOR syn_ot.t1_row;
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;

REVOKE CREATE ON SCHEMA rt_priv FROM u_tmp;
DROP SYNONYM IF EXISTS rt_priv.t1;
DROP SYNONYM rt_priv.tt1;

GRANT CREATE ON SCHEMA rt_priv TO u_tmp;
DROP SYNONYM rt_priv.tt1;
DROP SYNONYM IF EXISTS rt_priv.t1;
DROP SCHEMA rt_priv CASCADE;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;

-- Case 10. about privilege of two user.
\c -
CREATE USER user1 PASSWORD 'user@1111';
CREATE USER user2 PASSWORD 'user@2222';

GRANT CREATE ON DATABASE regression to user1, user2;
SET SESSION AUTHORIZATION user1 PASSWORD 'user@1111';

CREATE SCHEMA su1;
CREATE TABLE ut1(a int);
CREATE TABLE su1.ut1(a int);

CREATE SYNONYM pablic.uut1 for ut1;
CREATE SYNONYM pablic.uutt1 for su1.ut1;

COMMENT ON COLUMN pablic.uut1.a is 'puut1_a';
CREATE INDEX idx_a ON pablic.uutt1(a);

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

---- test for xxx_objects modification.
SELECT object_name, object_type FROM USER_OBJECTS WHERE object_type = 'synonym' ORDER BY 1,2;
SELECT object_name, object_type FROM DBA_OBJECTS WHERE object_type = 'synonym' ORDER BY 1,2;
SELECT object_name, object_type FROM ALL_OBJECTS WHERE object_type = 'synonym' ORDER BY 1,2;

\c -
SET SESSION AUTHORIZATION user2 PASSWORD 'user@2222';
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

SELECT * FROM pablic.uut1;
SELECT * FROM pablic.uutt1;

CREATE SYNONYM su1.uut2 for su1.ut1;
DROP SYNONYM pablic.uut1;

SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

\c -
SET SESSION AUTHORIZATION user1 PASSWORD 'user@1111';
DROP SCHEMA su1 CASCADE;

\c -
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

-- Case 11. other supported ddl or dml, such as explain query, merge into.
CREATE TABLE syn_ot.product
(
	product_id INTEGER,
	product_name VARCHAR2(60),
	category VARCHAR2(60)
);
CREATE SYNONYM p FOR syn_ot.product;

CREATE TABLE syn_ot.newproduct AS SELECT * FROM p;
CREATE SYNONYM np FOR syn_ot.newproduct;

INSERT INTO p VALUES (1501, 'VIVITAR 35MM', 'ELECTRNCS');
INSERT INTO p VALUES (1502, 'OLYMPUS IS50', 'ELECTRNCS');
INSERT INTO p VALUES (1600, 'PLAY GYM', 'TOYS');
INSERT INTO p VALUES (1601, 'LAMAZE', 'TOYS');
INSERT INTO p VALUES (1666, 'HARRY POTTER', 'DVD');

INSERT INTO np VALUES (1502, 'OLYMPUS CAMERA', 'ELECTRNCS');
INSERT INTO np VALUES (1601, 'LAMAZE', 'TOYS');
INSERT INTO np VALUES (1666, 'HARRY POTTER', 'TOYS');
INSERT INTO np VALUES (1700, 'WAIT INTERFACE', 'BOOKS');

explain (costs off, verbose off)
SELECT p.product_id, p.product_name, p.category FROM p inner join np on p.product_id = np.product_id ORDER BY 1;

MERGE INTO p USING np ON (p.product_id = np.product_id)
WHEN matched THEN
UPDATE SET p.product_name = np.product_name, p.category=np.category;

explain (costs off, verbose off)
UPDATE p SET p.category=np.category FROM np WHERE p.product_id = np.product_id;

-- Case 12. other sanity check about search_path.
SET current_schema = mt;
CREATE SYNONYM mm for syn_ot.t1;
SET current_schema = mt, syn_test;
CREATE SYNONYM mm1 for syn_ot.t1;
CREATE SYNONYM mm2 for mmt2;

CREATE TABLE ttt1(LIKE syn_ot.t1_row);
INSERT INTO ttt1 SELECT * FROM syn_ot.t1_row;
SELECT * FROM ttt1 ORDER BY 1,2;

CREATE TABLE ttt2(LIKE syn_ot.t3_part INCLUDING ALL);
SELECT * FROM ttt2 p1 WHERE p1.c1 NOT IN (SELECT p2.c1 FROM syn_ot.t3_part p2);

-- Case 13. create trigger on synonym object or use synonym in procedure.
CREATE FUNCTION dummy_trigger() returns trigger as $$
BEGIN
RETURN NULL;
END $$
LANGUAGE plpgsql;

CREATE TRIGGER trig_test BEFORE INSERT OR UPDATE OR DELETE
ON t1
FOR EACH STATEMENT
EXECUTE PROCEDURE dummy_trigger();

DROP TRIGGER IF EXISTS trig_test ON t1;

CREATE OR REPLACE PROCEDURE syn_pro_01(s_id integer) IS
s_name tt.name%TYPE;
BEGIN
SELECT name INTO s_name FROM tt WHERE tt.id = s_id;
END;
/
CALL syn_pro_01(1);

CREATE OR REPLACE PROCEDURE syn_pro_02(s_id integer) IS
s_name syn_test.tt.name%TYPE;
BEGIN
SELECT name INTO s_name FROM tt WHERE tt.id = s_id;
END;
/
CALL syn_pro_02(1);

CREATE OR REPLACE PROCEDURE syn_pro_03
AS
c1 tt%ROWTYPE;
BEGIN
SELECT 1 INTO c1 FROM dual;
END;
/
CALL syn_pro_03();

CREATE OR REPLACE PROCEDURE syn_pro_04
AS
c1 syn_test.tt%ROWTYPE;
BEGIN
SELECT 2 INTO c1 FROM dual;
end;
/
CALL syn_pro_04();

-- case 14. function or procedure including synonym objects, which defined using language plpsql, drop and recompile.
CREATE OR REPLACE SYNONYM ss1 FOR syn_ot.t2_col;

CREATE OR REPLACE PROCEDURE pp1 AS
BEGIN
INSERT INTO ss1 VALUES(3,'SP1');
END;
/

CREATE OR REPLACE FUNCTION ff0 RETURNS BIGINT AS
$$
SELECT count(*) FROM ss1;
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION ff1 RETURNS void AS
$$
BEGIN
UPDATE ss1 SET c2 = upper(c2) WHERE c1 = 1;
END;
$$
LANGUAGE plpgsql;

CALL pp1();
CALL ff0();
CALL ff1();
SELECT * FROM ss1 order by 1, 2;

DROP SYNONYM ss1;

CALL pp1();
CALL ff0();
CALL ff1();

RESET CURRENT_SCHEMA;

PREPARE update_p(VARCHAR2(60)) AS UPDATE p SET p.category=np.category FROM np WHERE p.product_id = np.product_id AND p.category = $1;
EXECUTE update_p('ELECTRNCS');
DROP SYNONYM p;

EXECUTE update_p('ELECTRNCS');

-- case 15. For define view using some synonym, dependency must be record before view decoupling is implemented, which is a little different from A db behavior.
INSERT INTO syn_test.t1 VALUES(1,1,2),(1,2,3),(2,3,1),(2,1,5);
CREATE SYNONYM st1 FOR syn_test.t1;
CREATE VIEW v1 AS SELECT * FROM st1;
SELECT * FROM v1 ORDER BY 1,2,3;

CREATE SYNONYM sv FOR v1;
CREATE VIEW v2 AS SELECT max(a) FROM sv;
SELECT * FROM v2;

CREATE OR REPLACE FUNCTION max_a RETURNS INT AS
$$
SELECT max(a) FROM syn_test.t1;
$$
LANGUAGE SQL;

CREATE SYNONYM m FOR max_a;
CREATE VIEW v3 AS SELECT * FROM m();

SELECT count(*) FROM st1 AS t, m() WHERE t.a < m;

DROP SYNONYM sv;
DROP SYNONYM m;
DROP SYNONYM st1;

DROP VIEW v3;
DROP SYNONYM m;
DROP SYNONYM st1 CASCADE;

-- clean up.
DROP SCHEMA syn_ot CASCADE;
DROP SCHEMA syn_test CASCADE;
DROP SCHEMA syn_ww CASCADE;
DROP SCHEMA pablic CASCADE;
DROP NODE GROUP grp1;
REVOKE CREATE ON DATABASE regression FROM u_tmp, user1, user2;
DROP USER u_tmp, user1, user2 CASCADE;

