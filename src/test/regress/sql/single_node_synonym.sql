--
-- Tests for SYNONYMS, single node.
--

CREATE SCHEMA syn_test;
SET CURRENT_SCHEMA = syn_test;
-- 0. Precondition, create referenced objects including table, view, funciton and procedure.
CREATE SCHEMA syn_ot;

CREATE TABLE syn_ot.t1_row(c1 int, c2 int);
CREATE TABLE syn_ot.t2_part(c1 int, c2 int) PARTITION BY range(c2)
(
	PARTITION p1 VALUES LESS THAN (5),
	PARTITION p2 VALUES LESS THAN (10),
	PARTITION p3 VALUES LESS THAN (maxvalue)
);

CREATE VIEW syn_ot.v_t1 AS SELECT * FROM syn_ot.t1_row;
CREATE VIEW syn_ot.v_t2 AS SELECT c1, max(c2) FROM syn_ot.t2_part GROUP BY c1;

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

-- 1.2 partition table
CREATE SYNONYM t2 FOR syn_ot.t2_part;
SELECT * FROM t2 ORDER BY 1;
INSERT INTO t2 VALUES(1,2),(2,6),(3,12);
SELECT * FROM t2 ORDER BY 1;
UPDATE t2 SET c2 = 10 WHERE c1 = 3;
SELECT * FROM t2 ORDER BY 1;
DELETE FROM t2 WHERE c2 < 5;
SELECT * FROM t2 ORDER BY 1;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;

DROP SYNONYM t1;
SELECT count(*) FROM pg_synonym;
DROP SYNONYM t2;
SELECT count(*) FROM pg_synonym;

SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

-- Case 2. SYNONYM FOR VIEW OBJECT
CREATE SYNONYM v1 FOR syn_ot.v_t1;
SELECT * FROM v1 ORDER BY 1;
CREATE SYNONYM v2 FOR syn_ot.v_t2;
SELECT * FROM v2 ORDER BY 1;

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

-- if the referenced object is created, okay to do select.
CREATE TABLE syn_ot.tt_row(id int, name varchar2(5), birth date);
INSERT INTO syn_ot.tt_row VALUES(1, 'July', '2000-01-01');
SELECT * FROM tt ORDER BY 1; 
INSERT INTO tt VALUES(2, 'Mia', '2008-08-08');
SELECT * FROM tt ORDER BY 1;

-- Case 7. name too long and truncate
CREATE SYNONYM tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttoolong FOR syn_ot.t1_row;

SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
DROP SYNONYM IF EXISTS tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttoo;

-- Case 8. the type of referenced object changed and then mapping automatically.
CREATE SYNONYM rtt1 FOR syn_ot.tt_row;
SELECT * FROM rtt1 ORDER BY 1;
ALTER TABLE syn_ot.rtt_row ADD avg_score NUMBER(1);
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

\c -
SELECT synname, synobjschema, synobjname FROM pg_synonym ORDER BY 1, 2, 3;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM USER_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM DBA_SYNONYMS ORDER BY 1,2;
SELECT schema_name, synonym_name, table_schema_name, table_name FROM ALL_SYNONYMS ORDER BY 1,2;

-- Case 10. other supported ddl or dml, such as explain query, merge into.
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

-- Case 11. create trigger on synonym object.
CREATE FUNCTION dummy_trigger() returns trigger as $$
BEGIN
RETURN NULL;
END $$
LANGUAGE plpgsql;

CREATE TRIGGER trig_test BEFORE INSERT OR UPDATE OR DELETE
ON syn_test.tt
FOR EACH STATEMENT
EXECUTE PROCEDURE dummy_trigger();

DROP TRIGGER IF EXISTS trig_test ON tt;

SET CURRENT_SCHEMA = syn_test;
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
CREATE OR REPLACE SYNONYM ss1 FOR syn_ot.tt_row;

CREATE OR REPLACE PROCEDURE pp1 AS
BEGIN
INSERT INTO ss1(id, name) VALUES(3,'SP1');
END;
/

CALL pp1();
SELECT * FROM ss1 order by 1, 2;
DROP SYNONYM ss1;

CALL pp1();

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

CREATE OR REPLACE FUNCTION max_a RETURNS INT AS
$$
SELECT max(a) FROM syn_test.t1;
$$
LANGUAGE SQL;
CREATE SYNONYM m FOR max_a;
CREATE VIEW v2 AS SELECT * FROM m();

DROP SYNONYM m;
DROP SYNONYM st1;

DROP SYNONYM st1 CASCADE;

-- clean up.
DROP SCHEMA syn_ot CASCADE;
DROP SCHEMA syn_test CASCADE;
REVOKE CREATE ON DATABASE regression FROM u_tmp;
DROP USER u_tmp;


