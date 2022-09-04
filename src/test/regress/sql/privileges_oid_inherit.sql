--
--FOR BLACKLIST FEATURE: WITH OIDS、INHERITS is not supported.
--
GRANT CREATE ON SCHEMA public TO PUBLIC;
--
-- Test access privileges
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'warning';

DROP ROLE IF EXISTS regressgroup1;
DROP ROLE IF EXISTS regressgroup2;

DROP ROLE IF EXISTS regressuser1;
DROP ROLE IF EXISTS regressuser2;
DROP ROLE IF EXISTS regressuser3;
DROP ROLE IF EXISTS regressuser4;
DROP ROLE IF EXISTS regressuser5;
DROP ROLE IF EXISTS regressuser6;

SELECT lo_unlink(oid) FROM pg_largeobject_metadata;

RESET client_min_messages;

-- test proper begins here

CREATE USER regressuser1 PASSWORD 'gauss@123';
CREATE USER regressuser2 PASSWORD 'gauss@123';
CREATE USER regressuser3 PASSWORD 'gauss@123';
CREATE USER regressuser4 PASSWORD 'gauss@123';
CREATE USER regressuser5 PASSWORD 'gauss@123';
CREATE USER regressuser5 PASSWORD 'gauss@123';	-- duplicate

CREATE GROUP regressgroup1 PASSWORD 'gauss@123';
CREATE GROUP regressgroup2 WITH USER regressuser1, regressuser2 PASSWORD 'gauss@123';

ALTER GROUP regressgroup1 ADD USER regressuser4;

ALTER GROUP regressgroup2 ADD USER regressuser2;	-- duplicate
ALTER GROUP regressgroup2 DROP USER regressuser2;
GRANT regressgroup2 TO regressuser4 WITH ADMIN OPTION;

-- test owner privileges

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
SET search_path to public;
SELECT session_user, current_user;

CREATE TABLE atest1 ( a int, b text );
SELECT * FROM atest1;
INSERT INTO atest1 VALUES (1, 'one');
DELETE FROM atest1;
UPDATE atest1 SET a = 1 WHERE b = 'blech';
TRUNCATE atest1;
START TRANSACTION;
LOCK atest1 IN ACCESS EXCLUSIVE MODE;
COMMIT;

REVOKE ALL ON atest1 FROM PUBLIC;
SELECT * FROM atest1;

GRANT ALL ON atest1 TO regressuser2;
GRANT SELECT ON atest1 TO regressuser3, regressuser4;
SELECT * FROM atest1;

CREATE TABLE atest2 (col1 varchar(10), col2 boolean);
GRANT SELECT ON atest2 TO regressuser2;
GRANT UPDATE ON atest2 TO regressuser3;
GRANT INSERT ON atest2 TO regressuser4;
GRANT TRUNCATE ON atest2 TO regressuser5;


SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SELECT session_user, current_user;

-- try various combinations of queries on atest1 and atest2

SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- ok
INSERT INTO atest1 VALUES (2, 'two'); -- ok
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- ok
UPDATE atest1 SET a = 1 WHERE a = 2; -- ok
UPDATE atest2 SET col2 = NOT col2; -- fail
--SELECT * FROM atest1 ORDER BY 1 FOR UPDATE; -- ok
--SELECT * FROM atest2 ORDER BY 1 FOR UPDATE; -- fail
DELETE FROM atest2; -- fail
TRUNCATE atest2; -- fail
START TRANSACTION;
LOCK atest2 IN ACCESS EXCLUSIVE MODE; -- fail
COMMIT;
COPY atest2 FROM stdin; -- fail
GRANT ALL ON atest1 TO PUBLIC; -- fail

-- checks in subquery, both ok
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) );
SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );


SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
SELECT session_user, current_user;

SELECT * FROM atest1 ORDER BY 1; -- ok
SELECT * FROM atest2; -- fail
INSERT INTO atest1 VALUES (2, 'two'); -- fail
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- fail
UPDATE atest1 SET a = 1 WHERE a = 2; -- fail
UPDATE atest2 SET col2 = NULL; -- ok
UPDATE atest2 SET col2 = NOT col2; -- fails; requires SELECT on atest2
UPDATE atest2 SET col2 = true FROM atest1 WHERE atest1.a = 5; -- ok
SELECT * FROM atest1 FOR UPDATE; -- fail
SELECT * FROM atest2 FOR UPDATE; -- fail
DELETE FROM atest2; -- fail
TRUNCATE atest2; -- fail
START TRANSACTION;
LOCK atest2 IN ACCESS EXCLUSIVE MODE; -- ok
COMMIT;
COPY atest2 FROM stdin; -- fail

-- checks in subquery, both fail
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) );
SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
COPY atest2 FROM stdin; -- ok
bar	true
\.
SELECT * FROM atest1 ORDER BY 1; -- ok


-- groups

SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
CREATE TABLE atest3 (one int, two int, three int);
GRANT DELETE ON atest3 TO GROUP regressgroup2;

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';

SELECT * FROM atest3; -- fail
DELETE FROM atest3; -- ok


-- views

SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';

CREATE VIEW atestv1 AS SELECT * FROM atest1; -- ok
/* The next *should* fail, but it's not implemented that way yet. */
CREATE VIEW atestv2 AS SELECT * FROM atest2;
CREATE VIEW atestv3 AS SELECT * FROM atest3; -- ok

SELECT * FROM atestv1 order by 1, 2; -- ok
SELECT * FROM atestv2; -- fail
GRANT SELECT ON atestv1, atestv3 TO regressuser4;
GRANT SELECT ON atestv2 TO regressuser2;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';

SELECT * FROM atestv1 order by 1, 2; -- ok
SELECT * FROM atestv2; -- fail
SELECT * FROM atestv3; -- fail due to issue 3520503, see above

CREATE VIEW atestv4 AS SELECT * FROM atestv3; -- nested view
SELECT * FROM atestv4; -- fail due to issue 3520503, see above
GRANT SELECT ON atestv4 TO regressuser2;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';

-- Two complex cases:

SELECT * FROM atestv3; -- fail
-- fail due to issue 3520503, see above
SELECT * FROM atestv4; -- ok (even though regressuser2 cannot access underlying atestv3)

SELECT * FROM atest2; -- ok
SELECT * FROM atestv2; -- fail (even though regressuser2 can access underlying atest2)

-- Test column level permissions

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
CREATE TABLE atest5 (one int, two int, three int);
CREATE TABLE atest6 (one int, two int, blue int);
GRANT SELECT (one), INSERT (two), UPDATE (three) ON atest5 TO regressuser4;
GRANT ALL (one) ON atest5 TO regressuser3;

INSERT INTO atest5 VALUES (1,2,3);

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT * FROM atest5; -- fail
SELECT one FROM atest5; -- ok
COPY atest5 (one) TO stdout; -- ok
SELECT two FROM atest5; -- fail
COPY atest5 (two) TO stdout; -- fail
SELECT atest5 FROM atest5; -- fail
COPY atest5 (one,two) TO stdout; -- fail
SELECT 1 FROM atest5; -- ok
SELECT 1 FROM atest5 a JOIN atest5 b USING (one); -- ok 
SELECT 1 FROM atest5 a JOIN atest5 b USING (two); -- fail
SELECT 1 FROM atest5 a NATURAL JOIN atest5 b; -- fail
SELECT (j.*) IS NULL FROM (atest5 a JOIN atest5 b USING (one)) j; -- fail
SELECT 1 FROM atest5 WHERE two = 2; -- fail
SELECT * FROM atest1, atest5; -- fail
SELECT atest1.* FROM atest1, atest5 order by 1, 2; -- ok
SELECT atest1.*,atest5.one FROM atest1, atest5 order by 1, 2, 3; -- ok 
SELECT atest1.*,atest5.one FROM atest1 JOIN atest5 ON (atest1.a = atest5.two); -- fail
SELECT atest1.*,atest5.one FROM atest1 JOIN atest5 ON (atest1.a = atest5.one); -- ok 
SELECT one, two FROM atest5; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
GRANT SELECT (one,two) ON atest6 TO regressuser4;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT one, two FROM atest5 NATURAL JOIN atest6; -- fail still

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
GRANT SELECT (two) ON atest5 TO regressuser4;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT one, two FROM atest5 NATURAL JOIN atest6; -- ok now 

-- test column-level privileges for INSERT and UPDATE
INSERT INTO atest5 (two) VALUES (3); -- fail due to issue 3520503, see above
COPY atest5 FROM stdin; -- fail
COPY atest5 (two) FROM stdin; -- ok
1
\.
INSERT INTO atest5 (three) VALUES (4); -- fail
INSERT INTO atest5 VALUES (5,5,5); -- fail
UPDATE atest5 SET three = 10; -- ok
UPDATE atest5 SET one = 8; -- fail
UPDATE atest5 SET three = 5, one = 2; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
REVOKE ALL (one) ON atest5 FROM regressuser4;
GRANT SELECT (one,two,blue) ON atest6 TO regressuser4;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT one FROM atest5; -- fail
UPDATE atest5 SET one = 1; -- fail
SELECT atest6 FROM atest6; -- ok
COPY atest6 TO stdout; -- ok

-- check error reporting with column privs
SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
CREATE TABLE t1 (c1 int, c2 int, c3 int check (c3 < 5), primary key (c1, c2));
GRANT SELECT (c1) ON t1 TO regressuser2;
GRANT INSERT (c1, c2, c3) ON t1 TO regressuser2;
GRANT UPDATE (c1, c2, c3) ON t1 TO regressuser2;

-- seed data
INSERT INTO t1 VALUES (1, 1, 1);
INSERT INTO t1 VALUES (1, 2, 1);
INSERT INTO t1 VALUES (2, 1, 2);
INSERT INTO t1 VALUES (2, 2, 2);
INSERT INTO t1 VALUES (3, 1, 3);

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
INSERT INTO t1 (c1, c2) VALUES (1, 1); -- fail, but row not shown
UPDATE t1 SET c2 = 1; -- fail, but row not shown
INSERT INTO t1 (c1, c2) VALUES (null, null); -- fail, but see columns being inserted
INSERT INTO t1 (c3) VALUES (null); -- fail, but see columns being inserted or have SELECT
INSERT INTO t1 (c1) VALUES (5); -- fail, but see columns being inserted or have SELECT
UPDATE t1 SET c3 = 10 where c1 = 1; -- fail, but see columns with SELECT rights, or being modified

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
DROP TABLE t1;

-- test column-level privileges when involved with DELETE
SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
ALTER TABLE atest6 ADD COLUMN three integer;
GRANT DELETE ON atest5 TO regressuser3;
GRANT SELECT (two) ON atest5 TO regressuser3;
REVOKE ALL (one) ON atest5 FROM regressuser3;
GRANT SELECT (one) ON atest5 TO regressuser4;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT atest6 FROM atest6; -- fail
SELECT one FROM atest5 NATURAL JOIN atest6; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
ALTER TABLE atest6 DROP COLUMN three;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT atest6 FROM atest6; -- ok
SELECT one FROM atest5 NATURAL JOIN atest6; -- ok 

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
ALTER TABLE atest6 DROP COLUMN two;
REVOKE SELECT (one,blue) ON atest6 FROM regressuser4;

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT * FROM atest6; -- fail
SELECT 1 FROM atest6; -- fail

SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
DELETE FROM atest5 WHERE one = 1; -- fail
DELETE FROM atest5 WHERE two = 2; -- ok

-- check inheritance cases
SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
CREATE TABLE atestp1 (f1 int, f2 int) WITH OIDS;
CREATE TABLE atestp2 (fx int, fy int) WITH OIDS;
CREATE TABLE atestc (fz int) INHERITS (atestp1, atestp2);
GRANT SELECT(fx,fy,oid) ON atestp2 TO regressuser2;
GRANT SELECT(fx) ON atestc TO regressuser2;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SELECT fx FROM atestp2; -- ok
SELECT fy FROM atestp2; -- fail due to issue 3520503, see above
SELECT atestp2 FROM atestp2; -- fail due to issue 3520503, see above
SELECT oid FROM atestp2; -- fail due to issue 3520503, see above
SELECT fy FROM atestc; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
GRANT SELECT(fy,oid) ON atestc TO regressuser2;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SELECT fx FROM atestp2; -- still ok
SELECT fy FROM atestp2; -- ok
SELECT atestp2 FROM atestp2; -- fail due to issue 3520503, see above
SELECT oid FROM atestp2; -- ok

-- privileges on functions, languages

-- switch to superuser
\c -

REVOKE ALL PRIVILEGES ON LANGUAGE sql FROM PUBLIC;
GRANT USAGE ON LANGUAGE sql TO regressuser1; -- ok
GRANT USAGE ON LANGUAGE c TO PUBLIC; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
SET search_path TO public;
GRANT USAGE ON LANGUAGE sql TO regressuser2; -- fail
CREATE FUNCTION testfunc1(int) RETURNS int AS 'select 2 * $1;' LANGUAGE sql;
CREATE FUNCTION testfunc2(int) RETURNS int AS 'select 3 * $1;' LANGUAGE sql;

REVOKE ALL ON FUNCTION testfunc1(int), testfunc2(int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION testfunc1(int), testfunc2(int) TO regressuser2;
GRANT USAGE ON FUNCTION testfunc1(int) TO regressuser3; -- semantic error
GRANT ALL PRIVILEGES ON FUNCTION testfunc1(int) TO regressuser4;
GRANT ALL PRIVILEGES ON FUNCTION testfunc_nosuch(int) TO regressuser4;

CREATE FUNCTION testfunc4(boolean) RETURNS text
  AS 'select col1 from atest2 where col2 = $1;'
  LANGUAGE sql SECURITY DEFINER;
GRANT EXECUTE ON FUNCTION testfunc4(boolean) TO regressuser3;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SELECT testfunc1(5), testfunc2(5); -- ok
CREATE FUNCTION testfunc3(int) RETURNS int AS 'select 2 * $1;' LANGUAGE sql; -- fail

SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
SELECT testfunc1(5); -- fail
SELECT col1 FROM atest2 WHERE col2 = true; -- fail
SELECT testfunc4(true); -- fail due to issue 3520503, see above

SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SELECT testfunc1(5); -- ok

DROP FUNCTION testfunc1(int); -- fail

\c -

DROP FUNCTION testfunc1(int); -- ok
-- restore to sanity
GRANT ALL PRIVILEGES ON LANGUAGE sql TO PUBLIC;


-- clean up

\c
DROP FUNCTION testfunc2(int);
DROP FUNCTION testfunc4(boolean);

DROP VIEW atestv1;
DROP VIEW atestv2;
-- this should cascade to drop atestv4
DROP VIEW atestv3 CASCADE;
-- this should complain "does not exist"
DROP VIEW atestv4;

DROP TABLE atest1;
DROP TABLE atest2;
DROP TABLE atest3;
DROP TABLE atest4;
DROP TABLE atest5;
DROP TABLE atest6;
DROP TABLE atestc;
DROP TABLE atestp1;
DROP TABLE atestp2;

SELECT lo_unlink(oid) FROM pg_largeobject_metadata;

DROP GROUP regressgroup1;
DROP GROUP regressgroup2;

-- these are needed to clean up permissions
REVOKE USAGE ON LANGUAGE sql FROM regressuser1;
DROP OWNED BY regressuser1;

DROP USER regressuser1;
DROP USER regressuser2;
DROP USER regressuser3;
DROP USER regressuser4;
DROP USER regressuser5;
DROP USER regressuser6;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;