CREATE DATABASE test_inlineview;
\c test_inlineview

CREATE TABLE dept (
  deptno integer PRIMARY KEY,
  dname VARCHAR(50),
  loc VARCHAR(50)
);

CREATE TABLE emp (
  empno integer PRIMARY KEY,
  ename VARCHAR(50),
  job VARCHAR(50),
  mgr integer default 0,
  hiredate DATE,
  sal numeric(6,2),
  comm integer,
  deptno integer REFERENCES dept(deptno)
);

INSERT INTO dept (deptno, dname, loc) VALUES (10, 'ACCOUNTING', 'NEW YORK');
INSERT INTO dept (deptno, dname, loc) VALUES (20, 'RESEARCH', 'DALLAS');
INSERT INTO dept (deptno, dname, loc) VALUES (30, 'SALES', 'CHICAGO');

INSERT INTO emp (empno, ename, sal, comm, deptno) VALUES (1, 'EMPLOYEE_1', 1000, NULL, 10);
INSERT INTO emp (empno, ename, sal, comm, deptno) VALUES (2, 'EMPLOYEE_2', 2000, NULL, 10);
INSERT INTO emp (empno, ename, sal, comm, deptno) VALUES (3, 'EMPLOYEE_3', 3000, NULL, 10);
INSERT INTO emp (empno, ename, sal, comm, deptno) VALUES (4, 'EMPLOYEE_4', 4000, 500, 20);
INSERT INTO emp (empno, ename, sal, comm, deptno) VALUES (5, 'EMPLOYEE_5', 5000, NULL, 30);

-- UPDATE
BEGIN;
SELECT * FROM emp ORDER BY empno ASC;
UPDATE (SELECT o.comm,o.sal
          FROM emp o
         WHERE o.comm IS NULL AND o.deptno IN (SELECT deptno FROM (SELECT deptno, COUNT(*) tot FROM (SELECT * FROM emp WHERE comm IS NULL) GROUP BY deptno) WHERE tot > 2)) empcomm
   SET empcomm.comm = empcomm.sal * 0.2;
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;

-- DELETE
BEGIN;
SELECT * FROM emp ORDER BY empno ASC;
DELETE FROM (SELECT o.comm,o.sal
          FROM emp o
         WHERE o.comm IS NULL AND o.deptno IN (SELECT deptno FROM (SELECT   deptno, COUNT (*) tot FROM (SELECT * FROM emp WHERE comm IS NULL) GROUP BY deptno ) WHERE tot > 1)) empcomm;
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;

BEGIN;
SELECT * FROM emp WHERE sal < 3000 ORDER BY empno ASC;
DELETE FROM (SELECT sal FROM emp) vemp(colsal) WHERE colsal < 3000;
SELECT * FROM emp WHERE sal < 3000 ORDER BY empno ASC;
ROLLBACK;

BEGIN;
UPDATE (SELECT emp.*, dept.dname FROM emp LEFT JOIN dept ON emp.deptno = dept.deptno) SET ename='ABCD' WHERE empno=1;
SELECT * FROM emp ORDER BY empno ASC;
DELETE FROM (SELECT emp.*, dept.dname FROM emp LEFT JOIN dept ON emp.deptno = dept.deptno);
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;

DELETE (SELECT * FROM (VALUES(1)) AS tmp(a));

-- INSERT
BEGIN;
SELECT * FROM emp ORDER BY empno ASC;
INSERT INTO (SELECT empno, ename, deptno, mgr FROM emp WHERE deptno = (SELECT deptno FROM dept WHERE loc = 'CHICAGO') WITH CHECK OPTION) empc
  SELECT 6, 'EMPLOYEE_6', d.deptno, 6000 FROM dept d WHERE d.loc = 'CHICAGO';
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;

BEGIN;
INSERT INTO (TABLE emp) vemp VALUES 
    (6, 'EMPLOYEE_6', NULL, 0, NULL, 6000, NULL, 30),
    (7, 'EMPLOYEE_7', NULL, 0, NULL, 7000, NULL, 20);
ROLLBACK;

BEGIN;
INSERT INTO (SELECT empno, ename, sal, comm, deptno FROM emp) empcomm(empno, ename, deptno) VALUES (6, 'EMPLOYEE_6', 30);
ROLLBACK;

-- PREPARE
PREPARE insert_emp(integer, varchar(50), numeric(6, 2), integer) AS
  INSERT INTO (SELECT empno, ename, sal, deptno FROM emp ) e VALUES ($1, $2, $3, $4);
BEGIN;
EXECUTE insert_emp(6, 'EMPLOYEE_6', 6000, 30);
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;
DEALLOCATE insert_emp;

PREPARE update_emp(integer, varchar(50)) AS
  UPDATE (SELECT * FROM emp WHERE empno = $1) e SET ename = $2;
BEGIN;
EXECUTE update_emp(1, 'ABCD');
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;
DEALLOCATE update_emp;

PREPARE delete_emp(integer) AS
  DELETE FROM (SELECT empno FROM emp WHERE empno = $1);
BEGIN;
EXECUTE delete_emp(1);
SELECT * FROM emp ORDER BY empno ASC;
ROLLBACK;
DEALLOCATE delete_emp;

-- with check option
CREATE VIEW v1 AS SELECT * FROM emp e WHERE sal > 2000;

UPDATE (SELECT * FROM v1 WHERE sal > 1000 WITH CASCADED CHECK OPTION) SET sal = 1500;
UPDATE (SELECT * FROM v1 WHERE sal > 1000 WITH LOCAL CHECK OPTION) SET sal = 500;
BEGIN;
UPDATE (SELECT * FROM v1 WHERE sal > 1000 WITH LOCAL CHECK OPTION) SET sal = 1500;
ROLLBACK;

INSERT INTO (SELECT * FROM v1 WHERE sal > 1000 WITH CASCADED CHECK OPTION) v SELECT 6, 'EMPLOYEE_6', NULL, 0, NULL, 1500, NULL, 30;
INSERT INTO (SELECT * FROM v1 WHERE sal > 1000 WITH LOCAL CHECK OPTION) v SELECT 6, 'EMPLOYEE_6', NULL, 0, NULL, 500, NULL, 30;
BEGIN;
INSERT INTO (SELECT * FROM v1 WHERE sal > 1000 WITH LOCAL CHECK OPTION) v SELECT 6, 'EMPLOYEE_6', NULL, 0, NULL, 1500, NULL, 30;
ROLLBACK;

-- query plan
CREATE VIEW v2 AS SELECT * FROM emp NATURAL JOIN dept;
EXPLAIN DELETE FROM (SELECT * FROM emp NATURAL JOIN dept);
EXPLAIN DELETE FROM v2;

EXPLAIN UPDATE (SELECT * FROM emp NATURAL JOIN dept) SET dname = 'ABCD' WHERE empno = 1;
EXPLAIN UPDATE v2 SET dname = 'ABCD' WHERE empno = 1;

EXPLAIN INSERT INTO (SELECT * FROM emp e WHERE sal > 2000) v VALUES (6, 'EMPLOYEE_6', NULL, 0, NULL, 6000, NULL, 30);
EXPLAIN INSERT INTO v1 VALUES (6, 'EMPLOYEE_6', NULL, 0, NULL, 6000, NULL, 30);

-- constraint violation
UPDATE (TABLE emp) SET empno = 4 WHERE empno = 5;

-- target column not found
INSERT INTO (SELECT empno, ename, sal, comm, deptno FROM emp) empcomm(empno, ename, dname) VALUES (6, 'EMPLOYEE_6', 'CHICAGO');
UPDATE (SELECT * FROM emp) SET dname = 'ABCD' WHERE empno = 1;
UPDATE (SELECT * FROM emp) SET emp.ename = 'ABCD' WHERE empno = 1;

-- type mismatch
INSERT INTO (SELECT empno, ename, sal, comm FROM emp) empcomm(empno, ename) VALUES ('6aa', 'EMPLOYEE_6');
UPDATE (TABLE emp) SET hiredate = 'EMPLOYEE_5' WHERE empno = 5;

-- not allowed to insert into more than one table
INSERT INTO
    (SELECT empno FROM emp JOIN dept ON emp.deptno = dept.deptno WHERE dname = 'SALES') vemp
VALUES (6);

-- default values not allowed
INSERT INTO (SELECT mgr FROM emp) e DEFAULT VALUES;
UPDATE (SELECT mgr FROM emp) e SET mgr = DEFAULT;

-- ambiguous column reference
BEGIN;
SELECT count(*) FROM emp JOIN dept ON emp.deptno = dept.deptno;
DELETE FROM (SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno);
SELECT count(*) FROM emp JOIN dept ON emp.deptno = dept.deptno;
ROLLBACK;

DELETE FROM (SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno) WHERE deptno = 10;

BEGIN;
SELECT ename FROM emp JOIN dept ON emp.deptno = dept.deptno WHERE dname = 'SALES';
UPDATE (SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno) SET ename = 'ABCD' WHERE dname = 'SALES';
SELECT ename FROM emp JOIN dept ON emp.deptno = dept.deptno WHERE dname = 'SALES';
ROLLBACK;

UPDATE (SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno) SET ename = 'ABCD' WHERE deptno = 10;
UPDATE (SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno) SET deptno = 40 WHERE dname = 'SALES';

INSERT INTO (SELECT e.deptno, d.deptno FROM emp e JOIN dept d ON e.deptno = d.deptno) v VALUES (50, 50);

create type newtype as(a int, b int);
create table test(a newtype,b int);
insert into (table test) t values(ROW(1,2),3);
update (table test) a set a.a=12;
update (table test) a set a.b=22;
select * from test;
update (table test) a set a.a=ROW(13,23);
update (table test) a set a.c=10;
update (table test) b set b.c=10;
update (table test) a set a.a.a=12;
drop table test;
drop type newtype;

-- non-updatable views
UPDATE (SELECT current_database as dat from current_catalog) SET dat = 'postgres';
DELETE FROM (SELECT current_timestamp);
INSERT INTO (VALUES (1, 1, 1), (2, 2, 2)) v1 VALUES (3, 3, 3), (4, 4, 4);

DELETE FROM (SELECT DISTINCT sal FROM emp) vemp(colsal) WHERE colsal < 3000;
UPDATE (SELECT deptno FROM emp GROUP BY deptno) SET deptno = 10;
INSERT INTO (SELECT 1 AS res FROM emp) vemp VALUES (2);

DELETE FROM (SELECT * FROM emp CROSS JOIN dept);

-- Invalid target relation
DELETE FROM (SELECT * FROM pg_authid) a;
DELETE FROM (SELECT * FROM gs_global_chain);

insert into (select * from pg_auth_history) ah values (pg_current_userid(), current_timestamp, NULL);

create schema ledger with blockchain;
create table ledger.tb1(col1 int, col2 text);
insert into (select * from blockchain.ledger_tb1_hist) h values (0, NULL, NULL, NULL);
insert into ledger.tb1 values (1, 'a');
update (select hash from ledger.tb1) set hash = NULL;

drop schema ledger cascade;

create materialized view mv1 as select * from emp;
insert into (select * from mv1) v values (7, 'EMPLOYEE_7', NULL, 0, NULL, 7000, NULL, 20);
drop materialized view mv1;

update (select ctid from emp) set ctid = NULL;

-- authorization
DROP USER IF EXISTS usr1;
CREATE USER usr1 PASSWORD '1234@abcd';
ALTER SESSION SET SESSION AUTHORIZATION usr1 PASSWORD '1234@abcd';

DELETE FROM (SELECT sal FROM emp) WHERE sal < 3000;

\c -
GRANT DELETE, SELECT ON emp TO usr1;
ALTER SESSION SET SESSION AUTHORIZATION usr1 PASSWORD '1234@abcd';

BEGIN;
SELECT * FROM emp WHERE sal < 3000 ORDER BY empno ASC;
DELETE FROM (SELECT sal FROM emp) WHERE sal < 3000;
SELECT * FROM emp WHERE sal < 3000 ORDER BY empno ASC;
ROLLBACK;

\c -
REVOKE ALL PRIVILEGES ON emp FROM usr1;
DROP USER usr1;

-- update_multi_base_table_view.sql
DROP SCHEMA IF EXISTS update_multi_base_table_view CASCADE;
CREATE SCHEMA update_multi_base_table_view;
SET CURRENT_SCHEMA TO update_multi_base_table_view;

CREATE TABLE dept(
    deptno INT NOT NULL, 
    dname VARCHAR(14),
    loc VARCHAR(13),
    CONSTRAINT pk_dept PRIMARY KEY(deptno)
);

INSERT INTO dept VALUES (10,'ACCOUNTING','NEW YORK'); 
INSERT INTO dept VALUES (20,'RESEARCH','DALLAS'); 
INSERT INTO dept VALUES (30,'SALES','CHICAGO');  
INSERT INTO dept VALUES (40,'OPERATIONS','BOSTON');

CREATE TABLE emp (
    empno int NOT NULL PRIMARY KEY,
    ename VARCHAR(10),  
    job VARCHAR(9),  
    deptno int,
    CONSTRAINT fk_deptno FOREIGN KEY(deptno) REFERENCES dept(deptno)
);

INSERT INTO emp VALUES (7369,'SMITH','CLERK',20); 
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',30); 
INSERT INTO emp VALUES (7566,'JONES','MANAGER',20); 
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',30); 
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',30); 
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',10); 
INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',20); 
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',10); 
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',30); 
INSERT INTO emp VALUES (7876,'ADAMS','CLERK',20); 
INSERT INTO emp VALUES (7900,'JAMES','CLERK',30); 
INSERT INTO emp VALUES (7902,'FORD','ANALYST',20); 
INSERT INTO emp VALUES (7934,'MILLER','CLERK',10);

CREATE TABLE salgrade (  
    grade int PRIMARY KEY, 
    losal int,  
    hisal int
); 

INSERT INTO SALGRADE VALUES (1,700,1200); 
INSERT INTO SALGRADE VALUES (2,1201,1400); 
INSERT INTO SALGRADE VALUES (3,1401,2000); 
INSERT INTO SALGRADE VALUES (4,2001,3000); 
INSERT INTO SALGRADE VALUES (5,3001,9999);

CREATE TABLE emp_sal (
    empno int,
    grade int,
    CONSTRAINT fk_empno FOREIGN KEY(empno) REFERENCES emp(empno),
    CONSTRAINT fk_grade FOREIGN KEY(grade) REFERENCES salgrade(grade)
);

INSERT INTO emp_sal VALUES(7369, 1);
INSERT INTO emp_sal VALUES(7499, 1);
INSERT INTO emp_sal VALUES(7521, 1);
INSERT INTO emp_sal VALUES(7566, 2);
INSERT INTO emp_sal VALUES(7654, 2);
INSERT INTO emp_sal VALUES(7698, 2);
INSERT INTO emp_sal VALUES(7782, 3);
INSERT INTO emp_sal VALUES(7788, 3);
INSERT INTO emp_sal VALUES(7839, 3);
INSERT INTO emp_sal VALUES(7844, 4);
INSERT INTO emp_sal VALUES(7876, 4);
INSERT INTO emp_sal VALUES(7900, 4);
INSERT INTO emp_sal VALUES(7902, 5);
INSERT INTO emp_sal VALUES(7934, 5);

    -- view based on multi tables
CREATE VIEW v_empdept_update AS 
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, dept.deptno 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno;

    -- view based on multi tables, with generated columns
CREATE VIEW v_empdept_gencol_update AS 
    SELECT emp.empno+1 as empno, emp.ename, emp.job, dept.dname, dept.loc 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno;

CREATE VIEW v_empdeptsal_join_update AS
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, salgrade.losal, salgrade.hisal
    FROM emp natural join dept natural join emp_sal natural join salgrade; 

    -- view with subquery as base table
CREATE VIEW v_subqry_update AS
    SELECT emp.empno, emp.ename, emp.job, sub.deptno, sub.dname, sub.loc
    FROM emp, (SELECT dname, loc, deptno, empno 
               FROM v_empdept_update ) AS sub
    WHERE emp.deptno = sub.deptno and emp.empno = sub.empno;

CREATE VIEW v_sidejoin_update AS
    SELECT emp.*, emp_sal.grade
    FROM emp left join emp_sal
    ON emp.empno = emp_sal.empno;

BEGIN;
SELECT * FROM v_empdept_update WHERE EMPNO=7369;  
UPDATE (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, dept.deptno 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno
) SET ENAME='ABCD', JOB='SALESMAN' WHERE EMPNO=7369;
SELECT * FROM v_empdept_update WHERE EMPNO=7369;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdept_gencol_update WHERE EMPNO=7370;
UPDATE (
    SELECT emp.empno+1 as empno, emp.ename, emp.job, dept.dname, dept.loc 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno
) SET DNAME='ENGINEERING' WHERE EMPNO=7370;
SELECT * FROM v_empdept_gencol_update WHERE EMPNO=7370;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
UPDATE (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, salgrade.losal, salgrade.hisal
    FROM emp natural join dept natural join emp_sal natural join salgrade
) SET hisal=1300 WHERE EMPNO=7654;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
ROLLBACK;

BEGIN;
SELECT * FROM v_subqry_update WHERE EMPNO=7499;
UPDATE (
    SELECT emp.empno, emp.ename, emp.job, sub.deptno, sub.dname, sub.loc
    FROM emp, (SELECT dname, loc, deptno, empno 
               FROM v_empdept_update ) AS sub
    WHERE emp.deptno = sub.deptno and emp.empno = sub.empno
) SET DNAME='ABCD' WHERE EMPNO=7499;
SELECT * FROM v_subqry_update WHERE EMPNO=7499;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
DELETE FROM (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, salgrade.losal, salgrade.hisal
    FROM emp natural join dept natural join emp_sal natural join salgrade
) WHERE EMPNO=7654;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
ROLLBACK;

BEGIN;
TRUNCATE TABLE emp_sal;
SELECT * FROM v_sidejoin_update WHERE empno=7369;
UPDATE (
    SELECT emp.*, emp_sal.grade
    FROM emp left join emp_sal
    ON emp.empno = emp_sal.empno
) SET ename='ABCD' WHERE empno=7369;
SELECT * FROM v_sidejoin_update WHERE empno=7369;
DELETE FROM (
    SELECT emp.*, emp_sal.grade
    FROM emp left join emp_sal
    ON emp.empno = emp_sal.empno
) WHERE empno=7369;
SELECT * FROM v_sidejoin_update WHERE empno=7369;
ROLLBACK;

UPDATE (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, dept.deptno 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno
) SET DNAME='ENGINEERING', ENAME='ABCD' WHERE EMPNO=7369;
UPDATE (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc 
    FROM dept left join emp on emp.deptno = dept.deptno
) SET DEPTNO=20 WHERE EMPNO=7900;
UPDATE (
    SELECT emp.empno+1 as empno, emp.ename, emp.job, dept.dname, dept.loc 
    FROM dept, emp 
    WHERE dept.deptno = emp.deptno
) SET EMPNO=7369 WHERE EMPNO=7370;
DELETE FROM (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc
    FROM emp cross join dept
);
DELETE FROM (
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc
    FROM emp full join dept on emp.deptno = emp.deptno
);

drop table if exists t_ViewUpdate_Case0015_1 cascade;
drop table if exists t_ViewUpdate_Case0015_2 cascade;
drop table if exists t_ViewUpdate_Case0015_3 cascade;
drop view if exists v_ViewUpdate_Case0015 cascade;
create table t_ViewUpdate_Case0015_1(col1 int ,col2 varchar(100) not null);
insert into t_ViewUpdate_Case0015_1 values(1,'HaErBin');
insert into t_ViewUpdate_Case0015_1 values(2,'ChangChun');
insert into t_ViewUpdate_Case0015_1 values(3,'TieLing');
create table t_ViewUpdate_Case0015_2(col3 int ,col4 varchar(100) not null);
insert into t_ViewUpdate_Case0015_2 values(3,'TieLing');
create table t_ViewUpdate_Case0015_3 as select * from t_ViewUpdate_Case0015_1;
create or replace view v_ViewUpdate_Case0015 as 
    select col1 as 序号,col2 as 省会 from t_ViewUpdate_Case0015_1 right join t_ViewUpdate_Case0015_2 on col1=col3;
select * from v_ViewUpdate_Case0015;
update (select col1 as 序号,col2 as 省会 from t_ViewUpdate_Case0015_1 right join t_ViewUpdate_Case0015_2 on col1=col3)
set 省会='ShenYang' where 序号=3;
select * from v_ViewUpdate_Case0015;
drop table if exists t_ViewUpdate_Case0015_1 cascade;
drop table if exists t_ViewUpdate_Case0015_2 cascade;
drop table if exists t_ViewUpdate_Case0015_3 cascade;
drop view if exists v_ViewUpdate_Case0015 cascade;

reset CURRENT_SCHEMA;
DROP SCHEMA IF EXISTS update_multi_base_table_view CASCADE;

-- updatable_views.sql with check option
CREATE TABLE base_tbl (a int, b int DEFAULT 10);
INSERT INTO base_tbl VALUES (1,2), (2,3), (1,-1);

INSERT INTO (SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r VALUES(3,4); -- ok
INSERT INTO (SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r VALUES(4,3); -- should fail
INSERT INTO (SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r VALUES(5,null); -- should fail
UPDATE (SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r SET b = 5 WHERE a = 3; -- ok
UPDATE (SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r SET b = -5 WHERE a = 3; -- should fail
INSERT INTO (SELECT a FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r VALUES (9); -- ok
INSERT INTO (SELECT a FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION) r VALUES (10); -- should fail
SELECT * FROM base_tbl;

DROP TABLE base_tbl CASCADE;
CREATE TABLE base_tbl (a int);
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a > 0;

INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH CHECK OPTION) r VALUES (-5); -- should fail
INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH CHECK OPTION) r VALUES (5); -- ok
INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH CHECK OPTION) r VALUES (15); -- should fail
SELECT * FROM base_tbl;

UPDATE (SELECT * FROM rw_view1 WHERE a < 10 WITH CHECK OPTION) r SET a = a - 10; -- should fail
UPDATE (SELECT * FROM rw_view1 WHERE a < 10 WITH CHECK OPTION) r SET a = a + 10; -- should fail

INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (-10); -- ok, but not in view
INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (20); -- should fail
SELECT * FROM base_tbl;

INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (-10); -- ok, but not in view
INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (20); -- should fail

ALTER VIEW rw_view1 SET (check_option=local);

INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (-20); -- should fail
INSERT INTO (SELECT * FROM rw_view1 WHERE a < 10 WITH LOCAL CHECK OPTION) r VALUES (30); -- should fail

DROP TABLE base_tbl CASCADE;
CREATE TABLE base_tbl (a int);
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WITH CHECK OPTION;
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a > 0;

INSERT INTO (SELECT * FROM base_tbl WITH CHECK OPTION) r  VALUES (-1); -- ok
INSERT INTO (SELECT * FROM base_tbl WITH CHECK OPTION) r  VALUES (1); -- ok
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0) r VALUES (-2); -- ok, but not in view
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0) r VALUES (2); -- ok
INSERT INTO (SELECT * FROM rw_view2 WITH CHECK OPTION) r VALUES (-3); -- should fail
INSERT INTO (SELECT * FROM rw_view2 WITH CHECK OPTION) r VALUES (3); -- ok

DROP TABLE base_tbl CASCADE;
CREATE TABLE base_tbl (a int);
CREATE TABLE ref_tbl (a int PRIMARY KEY);
INSERT INTO ref_tbl SELECT * FROM generate_series(1,10);

INSERT INTO (SELECT * FROM base_tbl b
  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a)
  WITH CHECK OPTION) r VALUES (5); -- ok
INSERT INTO (SELECT * FROM base_tbl b
  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a)
  WITH CHECK OPTION) r VALUES (15); -- should fail

UPDATE (SELECT * FROM base_tbl b
  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a)
  WITH CHECK OPTION) r SET a = a + 5; -- ok
UPDATE (SELECT * FROM base_tbl b
  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a)
  WITH CHECK OPTION) r SET a = a + 5; -- should fail

DROP TABLE base_tbl, ref_tbl CASCADE;

CREATE TABLE base_tbl (a int, b int);

CREATE FUNCTION base_tbl_trig_fn()
RETURNS trigger AS
$$
BEGIN
  NEW.b := 10;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER base_tbl_trig BEFORE INSERT OR UPDATE ON base_tbl
  FOR EACH ROW EXECUTE PROCEDURE base_tbl_trig_fn();

INSERT INTO (SELECT * FROM base_tbl WHERE a < b WITH CHECK OPTION) r VALUES (5,0); -- ok
INSERT INTO (SELECT * FROM base_tbl WHERE a < b WITH CHECK OPTION) r VALUES (15, 20); -- should fail
UPDATE (SELECT * FROM base_tbl WHERE a < b WITH CHECK OPTION) r SET a = 20, b = 30; -- should fail

DROP TABLE base_tbl CASCADE;
DROP FUNCTION base_tbl_trig_fn();
CREATE TABLE base_tbl (a int, b int);
CREATE VIEW rw_view1 AS SELECT a FROM base_tbl WHERE a < b;

CREATE FUNCTION rw_view1_trig_fn()
RETURNS trigger AS
$$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO base_tbl VALUES (NEW.a, 10);
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    UPDATE base_tbl SET a=NEW.a WHERE a=OLD.a;
    RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    DELETE FROM base_tbl WHERE a=OLD.a;
    RETURN OLD;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER rw_view1_trig
  INSTEAD OF INSERT OR UPDATE OR DELETE ON rw_view1
  FOR EACH ROW EXECUTE PROCEDURE rw_view1_trig_fn();

INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH LOCAL CHECK OPTION) r VALUES (-5); -- should fail
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH LOCAL CHECK OPTION) r VALUES (5); -- ok
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH LOCAL CHECK OPTION) r VALUES (50); -- ok, but not in view
UPDATE (SELECT * FROM rw_view1 WHERE a > 0 WITH LOCAL CHECK OPTION) r SET a = a - 10; -- should fail
SELECT * FROM base_tbl;
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r VALUES (100); -- ok, but not in view (doesn't fail rw_view1's check)
UPDATE (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r SET a = 200 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view1's check)
SELECT * FROM base_tbl;
DROP TRIGGER rw_view1_trig ON rw_view1;
CREATE RULE rw_view1_ins_rule AS ON INSERT TO rw_view1
  DO INSTEAD INSERT INTO base_tbl VALUES (NEW.a, 10);
CREATE RULE rw_view1_upd_rule AS ON UPDATE TO rw_view1
  DO INSTEAD UPDATE base_tbl SET a=NEW.a WHERE a=OLD.a;
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r VALUES (-10); -- ok, but not in view (doesn't fail rw_view2's check)
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r VALUES (5); -- ok
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r VALUES (20); -- ok, but not in view (doesn't fail rw_view1's check)
UPDATE (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r SET a = 30 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view1's check)
INSERT INTO (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r VALUES (5); -- ok
UPDATE (SELECT * FROM rw_view1 WHERE a > 0 WITH CASCADED CHECK OPTION) r SET a = -5 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view2's check)
SELECT * FROM base_tbl;

DROP TABLE base_tbl CASCADE;
DROP FUNCTION rw_view1_trig_fn();

CREATE TABLE base_tbl (a int);
CREATE VIEW rw_view1 AS SELECT a,10 AS b FROM base_tbl;
CREATE RULE rw_view1_ins_rule AS ON INSERT TO rw_view1
  DO INSTEAD INSERT INTO base_tbl VALUES (NEW.a);

INSERT INTO (SELECT * FROM rw_view1 WHERE a > b WITH LOCAL CHECK OPTION) r VALUES (2,3); -- ok, but not in view (doesn't fail rw_view2's check)
DROP TABLE base_tbl CASCADE;

CREATE TABLE base_tbl1(id int PRIMARY KEY, num int);
CREATE TABLE base_tbl2(id int PRIMARY KEY, num int);
INSERT INTO base_tbl1 VALUES (1, 1), (2, 2);
INSERT INTO base_tbl2 VALUES (1, 3), (2, 4);

insert into (select *, (select num from base_tbl2 where base_tbl2.id = d.id limit 1) c
from base_tbl1 d where exists (select * from base_tbl1 a right join base_tbl2 b on a.id = b.id where b.id = d.id)
with local check option) r(id, num) values (4, 4); -- fail
insert into base_tbl2 VALUES (4, 5);
insert into (select *, (select num from base_tbl2 where base_tbl2.id = d.id limit 1) c
from base_tbl1 d where exists (select * from base_tbl1 a right join base_tbl2 b on a.id = b.id where b.id = d.id)
with local check option) r(id, num) values (4, 4);

drop table base_tbl1 cascade;
drop table base_tbl2 cascade;



--- B_Format compatibility
CREATE DATABASE test_inlineview_b DBCOMPATIBILITY 'B';
\c test_inlineview_b

create table t_col(c1 int, c2 int default NULL) with (orientation = column);
insert into t_col values (1, 2);
create view v1 as select * from t_col;
delete from v1 returning *;
update /*+ ignore_error */ v1 set c1 = null where c1 = 2;
insert /*+ ignore_error */ into t_col values (1);
INSERT INTO (SELECT * FROM t_col WHERE c1 < 10) t VALUES (3, 2) RETURNING *;
update (SELECT * FROM t_col WHERE c1 < 10) SET c2 = 3 where c1 = 1 returning *;
drop view v1;
drop table t_col;

CREATE TEMPORARY TABLE t0 ( c54 INT , c9 INT ) ;
INSERT INTO (TABLE t0) t VALUES ( 25 , -8 ) , ( -88 , -77 ) ;
SELECT * FROM t0;
DROP TABLE t0;

drop table if exists t_t_mutil_t1;
drop table if exists t_t_mutil_t2;
drop table if exists t_t_mutil_t3;
create table t_t_mutil_t1(col1 int,col2 int);
create table t_t_mutil_t2(col1 int,col2 int);
create table t_t_mutil_t3(col1 int,col2 int);
insert into t_t_mutil_t1 values(1,1),(1,1);
insert into t_t_mutil_t2 values(1,1),(1,2);
insert into t_t_mutil_t3 values(1,1),(1,3);

-- multi delete
create view v1 as select * from t_t_mutil_t1;
begin;
delete from (select * from v1) a,(select * from t_t_mutil_t2) b where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;

delete from (table t_t_mutil_t1 union table t_t_mutil_t2);
delete b from (select * from t_t_mutil_t2) b;

-- multi update 
begin;
update t_t_mutil_t1 a, (select * from t_t_mutil_t2) b set a.col2=7,b.col2=8 where a.col1=b.col1;
select * from t_t_mutil_t1;
select * from t_t_mutil_t2;
rollback;

begin;
update (select * from t_t_mutil_t1) col1, (select * from t_t_mutil_t1) col2 set col1.col1 = 7;
select * from t_t_mutil_t1;
rollback;

update t_t_mutil_t1 a, (select * from t_t_mutil_t2 b join t_t_mutil_t3 c on b.col1=c.col1) bc set a.col2=7, bc.col2=8;
update t_t_mutil_t1 a, (select * from t_t_mutil_t2) b set a.col2=7,b.col2=8 where a.col1=b.col1 order by a.col2;
update t_t_mutil_t1 a, (select * from t_t_mutil_t2) b set a.col2=7,b.col2=8 where a.col1=b.col1 limit 1;
update t_t_mutil_t1 a, (select * from t_t_mutil_t2) b set a.col2=7,b.col2=8 where a.col1=b.col1 returning *;

create table base_tbl1 (id int, num int);
create view rw_view1 as select * from base_tbl1 where id > 0;

insert into (select * from rw_view1 where id < 10 with local check option) r values (0); -- ok
insert into (select * from rw_view1 where id < 10 with local check option) r values (10); -- fail
insert into (select * from rw_view1 where id < 10 with cascaded check option) r values (0); -- fail
insert into (select * from rw_view1 where id < 10 with cascaded check option) r values (10); -- fail
CREATE TABLE base_tbl (a int primary key, b int DEFAULT 10);
INSERT INTO base_tbl VALUES (1,2), (2,3);
INSERT INTO (SELECT * FROM base_tbl WHERE a < 10 WITH CHECK OPTION) r VALUES(3,4);
UPDATE (SELECT * FROM base_tbl WHERE a < 10 WITH CHECK OPTION) r SET b = 5 WHERE a = 3;
drop table if exists base_tbl cascade;
CREATE TABLE base_tbl (a int primary key, b int DEFAULT 10);
INSERT INTO base_tbl VALUES (1,2), (8,2), (9,0);
UPDATE (SELECT * FROM base_tbl WHERE a < 10 WITH CHECK OPTION) r SET a = a + b;

\c regression
DROP DATABASE test_inlineview;
DROP DATABASE test_inlineview_b;