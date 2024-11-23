DROP SCHEMA IF EXISTS update_multi_base_table_view CASCADE;
CREATE SCHEMA update_multi_base_table_view;
SET CURRENT_SCHEMA TO update_multi_base_table_view;

-- 1. INITIATE BASE TABLES

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

-- 2. CREATE VIEWS

    -- view based on a single table
CREATE VIEW v_emp_update AS 
    SELECT emp.empno, emp.ename, emp.job 
    FROM emp 
    WHERE emp.deptno=10;

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

    -- view based on multi tables joined with each other 
CREATE VIEW v_empdept_join_update AS 
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc 
    FROM dept left join emp on emp.deptno = dept.deptno;

CREATE VIEW v_empdeptsal_join_update AS
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc, salgrade.losal, salgrade.hisal
    FROM emp natural join dept natural join emp_sal natural join salgrade; 

    -- view with subquery as base table
CREATE VIEW v_subqry_update AS
    SELECT emp.empno, emp.ename, emp.job, sub.deptno, sub.dname, sub.loc
    FROM emp, (SELECT dname, loc, deptno, empno 
               FROM v_empdept_update ) AS sub
    WHERE emp.deptno = sub.deptno and emp.empno = sub.empno;

CREATE VIEW v_sublink_update AS
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc
    FROM dept, emp
    WHERE dept.deptno = emp.deptno AND emp.empno in (
        SELECT empno
        FROM emp_sal
        WHERE grade > 3
    );

    -- view based on full join/cross join, not allowed for deletes, but ok for updates
CREATE VIEW v_empdept_crossjoin_update AS
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc
    FROM emp cross join dept;
CREATE VIEW v_empdept_fulljoin_update AS
    SELECT emp.empno, emp.ename, emp.job, dept.dname, dept.loc
    FROM emp full join dept on emp.deptno = emp.deptno;

-- 3. UPDATE/DELETE FROM VIEWS

BEGIN;
SELECT * FROM v_emp_update WHERE EMPNO=7782;
UPDATE v_emp_update SET ENAME='ABC' WHERE EMPNO=7782;
SELECT * FROM v_emp_update WHERE EMPNO=7782;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdept_update WHERE EMPNO=7369;  
UPDATE v_empdept_update SET ENAME='ABCD', JOB='SALESMAN' WHERE EMPNO=7369;
SELECT * FROM v_empdept_update WHERE EMPNO=7369;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdept_gencol_update WHERE EMPNO=7370;
UPDATE v_empdept_gencol_update SET DNAME='ENGINEERING' WHERE EMPNO=7370;
SELECT * FROM v_empdept_gencol_update WHERE EMPNO=7370;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
UPDATE v_empdeptsal_join_update SET hisal=1300 WHERE EMPNO=7654;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
ROLLBACK;

BEGIN;
SELECT * FROM v_subqry_update WHERE EMPNO=7499;
UPDATE v_subqry_update SET DNAME='ABCD' WHERE EMPNO=7499;
SELECT * FROM v_subqry_update WHERE EMPNO=7499;
ROLLBACK;

BEGIN;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
DELETE FROM v_empdeptsal_join_update WHERE EMPNO=7654;
SELECT * FROM v_empdeptsal_join_update WHERE EMPNO=7654;
ROLLBACK;

-- 4. ERROR SITUATION

    -- update columns from multiple tables at the same time
UPDATE v_empdept_update SET DNAME='ENGINEERING', ENAME='ABCD' WHERE EMPNO=7369;

    -- update columns that don't exist on the view;
UPDATE v_empdeptsal_join_update SET DEPTNO=20 WHERE EMPNO=7900;

    -- update generated columns
UPDATE v_empdept_gencol_update SET EMPNO=7369 WHERE EMPNO=7370;

    -- delete from a view that contains full join or cross join
DELETE FROM v_empdept_crossjoin_update;
DELETE FROM v_empdept_fulljoin_update;

DROP SCHEMA IF EXISTS update_multi_base_table_view CASCADE;

-- 5. B compatibility, update multiple views at the same time
DROP DATABASE IF EXISTS mutli_tblv_bdat;
CREATE DATABASE mutli_tblv_bdat DBCOMPATIBILITY='B';

\c mutli_tblv_bdat

CREATE TABLE t1(a int, b int PRIMARY KEY);
CREATE TABLE t2(a int, b int PRIMARY KEY);
CREATE TABLE t3(a int, b int PRIMARY KEY);

INSERT INTO t1 VALUES (1, 1), (1, 2), (1, 3);
INSERT INTO t2 VALUES (2, 1), (2, 2), (2, 3);
INSERT INTO t3 VALUES (3, 1), (3, 2), (3, 3);

CREATE VIEW tv12 AS SELECT t1.a, t2.b FROM t1,t2 WHERE t1.b=t2.b;
CREATE VIEW tv23 AS SELECT t2.a, t3.b FROM t2 JOIN t3 ON t2.b=t3.b;
CREATE VIEW tv123 AS SELECT t1.a, sub.b FROM t1, (SELECT t3.a, t2.b FROM t3, t2 WHERE t3.b=t2.b) AS sub WHERE t1.b=sub.b;

CREATE VIEW v1 AS SELECT * FROM (SELECT a FROM t1);

BEGIN;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
UPDATE tv12, tv23 SET tv12.a=4, tv23.a=7;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
ROLLBACK;

BEGIN;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
DELETE FROM tv12, tv23 WHERE tv12.b=tv23.b;
SELECT * FROM t1;
SELECT * FROM t2;
SELECT * FROM t3;
ROLLBACK;

-- update on a table in a view's subquery not supported in B-format database
BEGIN;
UPDATE tv12, tv123 SET tv12.a=4, tv123.b=7 WHERE tv12.b=tv123.b AND tv123.b=1;
ROLLBACK;

-- delete on a table in a view's subquery not supported in B-format database
BEGIN;
DELETE FROM tv12, tv123 WHERE tv12.b=tv123.b;
ROLLBACK;

\c postgres
DROP DATABASE mutli_tblv_bdat;
