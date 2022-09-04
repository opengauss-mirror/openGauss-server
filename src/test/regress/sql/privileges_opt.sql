--
--FOR BLACKLIST FEATURE: CREATE TYPE is not supported.
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

CREATE TABLE atest1 ( a int, b text );
CREATE TABLE atest2 (col1 varchar(10), col2 boolean);
CREATE TABLE atest3 (one int, two int, three int);

-- privileges on types

-- switch to superuser
\c -

CREATE TYPE testtype1 AS (a int, b text);
REVOKE USAGE ON TYPE testtype1 FROM PUBLIC;
GRANT USAGE ON TYPE testtype1 TO regressuser2;
GRANT USAGE ON TYPE _testtype1 TO regressuser2; -- fail
GRANT USAGE ON DOMAIN testtype1 TO regressuser2; -- fail

CREATE DOMAIN testdomain1 AS int;
REVOKE USAGE on DOMAIN testdomain1 FROM PUBLIC;
GRANT USAGE ON DOMAIN testdomain1 TO regressuser2;
GRANT USAGE ON TYPE testdomain1 TO regressuser2; -- ok

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
SET search_path TO public;

-- commands that should fail

CREATE AGGREGATE testagg1a(testdomain1) (sfunc = int4_sum, stype = bigint);

CREATE DOMAIN testdomain2a AS testdomain1;

CREATE DOMAIN testdomain3a AS int;
CREATE FUNCTION castfunc(int) RETURNS testdomain3a AS $$ SELECT $1::testdomain3a $$ LANGUAGE SQL;
CREATE CAST (testdomain1 AS testdomain3a) WITH FUNCTION castfunc(int);
DROP FUNCTION castfunc(int) CASCADE;
DROP DOMAIN testdomain3a;

CREATE FUNCTION testfunc5a(a testdomain1) RETURNS int LANGUAGE SQL AS $$ SELECT $1 $$;
CREATE FUNCTION testfunc6a(b int) RETURNS testdomain1 LANGUAGE SQL AS $$ SELECT $1::testdomain1 $$;

CREATE OPERATOR !+! (PROCEDURE = int4pl, LEFTARG = testdomain1, RIGHTARG = testdomain1);

CREATE TABLE test5a (a int, b testdomain1);
CREATE TABLE test6a OF testtype1;
CREATE TABLE test10a (a int[], b testtype1[]);

CREATE TABLE test9a (a int, b int);
ALTER TABLE test9a ADD COLUMN c testdomain1;
ALTER TABLE test9a ALTER COLUMN b TYPE testdomain1;

CREATE TYPE test7a AS (a int, b testdomain1);

CREATE TYPE test8a AS (a int, b int);
ALTER TYPE test8a ADD ATTRIBUTE c testdomain1;
ALTER TYPE test8a ALTER ATTRIBUTE b TYPE testdomain1;

CREATE TABLE test11a AS (SELECT 1::testdomain1 AS a);

REVOKE ALL ON TYPE testtype1 FROM PUBLIC;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';

-- commands that should succeed

CREATE AGGREGATE testagg1b(testdomain1) (sfunc = int4_sum, stype = bigint);

CREATE DOMAIN testdomain2b AS testdomain1;

CREATE DOMAIN testdomain3b AS int;
CREATE FUNCTION castfunc(int) RETURNS testdomain3b AS $$ SELECT $1::testdomain3b $$ LANGUAGE SQL;
CREATE CAST (testdomain1 AS testdomain3b) WITH FUNCTION castfunc(int);

CREATE FUNCTION testfunc5b(a testdomain1) RETURNS int LANGUAGE SQL AS $$ SELECT $1 $$;
CREATE FUNCTION testfunc6b(b int) RETURNS testdomain1 LANGUAGE SQL AS $$ SELECT $1::testdomain1 $$;

CREATE OPERATOR !! (PROCEDURE = testfunc5b, RIGHTARG = testdomain1);

CREATE TABLE test5b (a int, b testdomain1);
CREATE TABLE test6b OF testtype1;
CREATE TABLE test10b (a int[], b testtype1[]);

CREATE TABLE test9b (a int, b int);
ALTER TABLE test9b ADD COLUMN c testdomain1;
ALTER TABLE test9b ALTER COLUMN b TYPE testdomain1;

CREATE TYPE test7b AS (a int, b testdomain1);

CREATE TYPE test8b AS (a int, b int);
ALTER TYPE test8b ADD ATTRIBUTE c testdomain1;
ALTER TYPE test8b ALTER ATTRIBUTE b TYPE testdomain1;

CREATE TABLE test11b AS (SELECT 1::testdomain1 AS a);

REVOKE ALL ON TYPE testtype1 FROM PUBLIC;

\c -
DROP AGGREGATE testagg1b(testdomain1);
DROP DOMAIN testdomain2b;
DROP OPERATOR !! (NONE, testdomain1);
DROP FUNCTION testfunc5b(a testdomain1);
DROP FUNCTION testfunc6b(b int);
DROP TABLE test5b;
DROP TABLE test6b;
DROP TABLE test9b;
DROP TABLE test10b;
DROP TYPE test7b;
DROP TYPE test8b;
DROP CAST (testdomain1 AS testdomain3b);
DROP FUNCTION castfunc(int) CASCADE;
DROP DOMAIN testdomain3b;
DROP TABLE test11b;

DROP TYPE testtype1; -- ok
DROP DOMAIN testdomain1; -- ok


-- truncate
SET SESSION AUTHORIZATION regressuser5 PASSWORD 'gauss@123';
SET search_path TO public;
TRUNCATE atest2; -- ok
TRUNCATE atest3; -- fail

-- has_table_privilege function

-- bad-input checks
select has_table_privilege(NULL,'pg_authid','select');
select has_table_privilege('pg_shad','select');
select has_table_privilege('nosuchuser','pg_authid','select');
select has_table_privilege('pg_authid','sel');
select has_table_privilege(-999999,'pg_authid','update');
select has_table_privilege(1,'select');

-- superuser
\c -

select has_table_privilege(current_user,'pg_authid','select');
select has_table_privilege(current_user,'pg_authid','insert');

select has_table_privilege(t2.oid,'pg_authid','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'pg_authid','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

-- 'rule' privilege no longer exists, but for backwards compatibility
-- has_table_privilege still recognizes the keyword and says FALSE
select has_table_privilege(current_user,t1.oid,'rule')
from (select oid from pg_class where relname = 'pg_authid') as t1;
select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'pg_authid') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'pg_authid') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'pg_authid') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('pg_authid','update');
select has_table_privilege('pg_authid','delete');
select has_table_privilege('pg_authid','truncate');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'pg_authid') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'pg_authid') as t1;

-- non-superuser
SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
SET search_path TO public;

select has_table_privilege(current_user,'pg_class','select');
select has_table_privilege(current_user,'pg_class','insert');

select has_table_privilege(t2.oid,'pg_class','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'pg_class','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'pg_class') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'pg_class') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'pg_class') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('pg_class','update');
select has_table_privilege('pg_class','delete');
select has_table_privilege('pg_class','truncate');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'pg_class') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'pg_class') as t1;

select has_table_privilege(current_user,'atest1','select');
select has_table_privilege(current_user,'atest1','insert');

select has_table_privilege(t2.oid,'atest1','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'atest1','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'atest1') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'atest1') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'atest1') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('atest1','update');
select has_table_privilege('atest1','delete');
select has_table_privilege('atest1','truncate');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'atest1') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'atest1') as t1;


-- Grant options

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';

CREATE TABLE atest4 (a int);

GRANT SELECT ON atest4 TO regressuser2 WITH GRANT OPTION;
GRANT UPDATE ON atest4 TO regressuser2;
GRANT SELECT ON atest4 TO GROUP regressgroup1 WITH GRANT OPTION;

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';

GRANT SELECT ON atest4 TO regressuser3;
GRANT UPDATE ON atest4 TO regressuser3; -- fail

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';

REVOKE SELECT ON atest4 FROM regressuser3; -- does nothing
SELECT has_table_privilege('regressuser3', 'atest4', 'SELECT'); -- true
REVOKE SELECT ON atest4 FROM regressuser2; -- fail
REVOKE GRANT OPTION FOR SELECT ON atest4 FROM regressuser2 CASCADE; -- ok
SELECT has_table_privilege('regressuser2', 'atest4', 'SELECT'); -- true
SELECT has_table_privilege('regressuser3', 'atest4', 'SELECT'); -- false

SELECT has_table_privilege('regressuser1', 'atest4', 'SELECT WITH GRANT OPTION'); -- true

-- has_sequence_privilege tests
\c -

CREATE SEQUENCE x_seq;

GRANT USAGE on x_seq to regressuser2;

SELECT has_sequence_privilege('regressuser1', 'atest1', 'SELECT');
SELECT has_sequence_privilege('regressuser1', 'x_seq', 'INSERT');
SELECT has_sequence_privilege('regressuser1', 'x_seq', 'SELECT');

SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT has_sequence_privilege('x_seq', 'USAGE');

-- largeobject privilege tests
\c -
SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT lo_create(1001);
SELECT lo_create(1002);
SELECT lo_create(1003);
SELECT lo_create(1004);
SELECT lo_create(1005);

GRANT ALL ON LARGE OBJECT 1001 TO PUBLIC;
GRANT SELECT ON LARGE OBJECT 1003 TO regressuser2;
GRANT SELECT,UPDATE ON LARGE OBJECT 1004 TO regressuser2;
GRANT ALL ON LARGE OBJECT 1005 TO regressuser2;
GRANT SELECT ON LARGE OBJECT 1005 TO regressuser2 WITH GRANT OPTION;

GRANT SELECT, INSERT ON LARGE OBJECT 1001 TO PUBLIC;	-- to be failed
GRANT SELECT, UPDATE ON LARGE OBJECT 1001 TO nosuchuser;	-- to be failed
GRANT SELECT, UPDATE ON LARGE OBJECT  999 TO PUBLIC;	-- to be failed

\c -
SET SESSION AUTHORIZATION regressuser2 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT lo_create(2001);
SELECT lo_create(2002);

SELECT loread(lo_open(1001, x'40000'::int), 32);
SELECT loread(lo_open(1002, x'40000'::int), 32);	-- to be denied
SELECT loread(lo_open(1003, x'40000'::int), 32);
SELECT loread(lo_open(1004, x'40000'::int), 32);

SELECT lowrite(lo_open(1001, x'20000'::int), 'abcd');
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');	-- to be denied
SELECT lowrite(lo_open(1003, x'20000'::int), 'abcd');	-- to be denied
SELECT lowrite(lo_open(1004, x'20000'::int), 'abcd');

GRANT SELECT ON LARGE OBJECT 1005 TO regressuser3;
GRANT UPDATE ON LARGE OBJECT 1006 TO regressuser3;	-- to be denied
REVOKE ALL ON LARGE OBJECT 2001, 2002 FROM PUBLIC;
GRANT ALL ON LARGE OBJECT 2001 TO regressuser3;

SELECT lo_unlink(1001);		-- to be denied
SELECT lo_unlink(2002);

\c -
-- confirm ACL setting
SELECT oid, pg_get_userbyid(lomowner) ownername, lomacl FROM pg_largeobject_metadata;

SET SESSION AUTHORIZATION regressuser3 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT loread(lo_open(1001, x'40000'::int), 32);
SELECT loread(lo_open(1003, x'40000'::int), 32);	-- to be denied
SELECT loread(lo_open(1005, x'40000'::int), 32);

SELECT lo_truncate(lo_open(1005, x'20000'::int), 10);	-- to be denied
SELECT lo_truncate(lo_open(2001, x'20000'::int), 10);

-- compatibility mode in largeobject permission
\c -
SET lo_compat_privileges = false;	-- default setting
SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT loread(lo_open(1002, x'40000'::int), 32);	-- to be denied
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');	-- to be denied
SELECT lo_truncate(lo_open(1002, x'20000'::int), 10);	-- to be denied
SELECT lo_unlink(1002);					-- to be denied
SELECT lo_export(1001, '/dev/null');			-- to be denied

\c -
SET lo_compat_privileges = true;	-- compatibility mode
SET SESSION AUTHORIZATION regressuser4 PASSWORD 'gauss@123';
SET search_path TO public;

SELECT loread(lo_open(1002, x'40000'::int), 32);
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');
SELECT lo_truncate(lo_open(1002, x'20000'::int), 10);
SELECT lo_unlink(1002);
SELECT lo_export(1001, '/dev/null');			-- to be denied

-- don't allow unpriv users to access pg_largeobject contents
\c -
SELECT * FROM pg_largeobject LIMIT 0;

SET SESSION AUTHORIZATION regressuser1 PASSWORD 'gauss@123';
SET search_path TO public;
SELECT * FROM pg_largeobject LIMIT 0;			-- to be denied

-- test default ACLs
\c -

CREATE SCHEMA testns;
GRANT ALL ON SCHEMA testns TO regressuser1;

CREATE TABLE testns.acltest1 (x int);
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'SELECT'); -- no
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'INSERT'); -- no

ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT SELECT ON TABLES TO public;

SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'SELECT'); -- no
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'INSERT'); -- no

DROP TABLE testns.acltest1;
CREATE TABLE testns.acltest1 (x int);

SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'INSERT'); -- no

ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT INSERT ON TABLES TO regressuser1;

DROP TABLE testns.acltest1;
CREATE TABLE testns.acltest1 (x int);

SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'INSERT'); -- yes

ALTER DEFAULT PRIVILEGES IN SCHEMA testns REVOKE INSERT ON TABLES FROM regressuser1;

DROP TABLE testns.acltest1;
CREATE TABLE testns.acltest1 (x int);

SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regressuser1', 'testns.acltest1', 'INSERT'); -- no

ALTER DEFAULT PRIVILEGES FOR ROLE regressuser1 REVOKE EXECUTE ON FUNCTIONS FROM public;

SET ROLE regressuser1 PASSWORD 'gauss@123';

CREATE FUNCTION testns.foo() RETURNS int AS 'select 1' LANGUAGE sql;

SELECT has_function_privilege('regressuser2', 'testns.foo()', 'EXECUTE'); -- no

ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT EXECUTE ON FUNCTIONS to public;

DROP FUNCTION testns.foo();
CREATE FUNCTION testns.foo() RETURNS int AS 'select 1' LANGUAGE sql;

SELECT has_function_privilege('regressuser2', 'testns.foo()', 'EXECUTE'); -- yes

DROP FUNCTION testns.foo();

ALTER DEFAULT PRIVILEGES FOR ROLE regressuser1 REVOKE USAGE ON TYPES FROM public;

CREATE DOMAIN testns.testdomain1 AS int;

SELECT has_type_privilege('regressuser2', 'testns.testdomain1', 'USAGE'); -- no

ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT USAGE ON TYPES to public;

DROP DOMAIN testns.testdomain1;
CREATE DOMAIN testns.testdomain1 AS int;

SELECT has_type_privilege('regressuser2', 'testns.testdomain1', 'USAGE'); -- yes

DROP DOMAIN testns.testdomain1;

RESET ROLE;

SELECT count(*)
  FROM pg_default_acl d LEFT JOIN pg_namespace n ON defaclnamespace = n.oid
  WHERE nspname = 'testns';

DROP SCHEMA testns CASCADE;

SELECT d.*     -- check that entries went away
  FROM pg_default_acl d LEFT JOIN pg_namespace n ON defaclnamespace = n.oid
  WHERE nspname IS NULL AND defaclnamespace != 0;


-- Grant on all objects of given type in a schema
\c -

CREATE SCHEMA testns;
CREATE TABLE testns.t1 (f1 int);
CREATE TABLE testns.t2 (f1 int);

SELECT has_table_privilege('regressuser1', 'testns.t1', 'SELECT'); -- false

GRANT ALL ON ALL TABLES IN SCHEMA testns TO regressuser1;

SELECT has_table_privilege('regressuser1', 'testns.t1', 'SELECT'); -- true
SELECT has_table_privilege('regressuser1', 'testns.t2', 'SELECT'); -- true

REVOKE ALL ON ALL TABLES IN SCHEMA testns FROM regressuser1;

SELECT has_table_privilege('regressuser1', 'testns.t1', 'SELECT'); -- false
SELECT has_table_privilege('regressuser1', 'testns.t2', 'SELECT'); -- false

CREATE FUNCTION testns.testfunc(int) RETURNS int AS 'select 3 * $1;' LANGUAGE sql;

SELECT has_function_privilege('regressuser1', 'testns.testfunc(int)', 'EXECUTE'); -- true by default

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA testns FROM PUBLIC;

SELECT has_function_privilege('regressuser1', 'testns.testfunc(int)', 'EXECUTE'); -- false

SET client_min_messages TO 'warning';
DROP SCHEMA testns CASCADE;
RESET client_min_messages;


-- test that dependent privileges are revoked (or not) properly
\c -

set session role regressuser1 PASSWORD 'gauss@123';
set search_path to public; 
create table dep_priv_test (a int);
grant select on dep_priv_test to regressuser2 with grant option;
grant select on dep_priv_test to regressuser3 with grant option;
set session role regressuser2 PASSWORD 'gauss@123';
set search_path to public;
grant select on dep_priv_test to regressuser4 with grant option;
set session role regressuser3 PASSWORD 'gauss@123';
set search_path to public;
grant select on dep_priv_test to regressuser4 with grant option;
set session role regressuser4 PASSWORD 'gauss@123';
set search_path to public;
grant select on dep_priv_test to regressuser5;
\dp dep_priv_test
set session role regressuser2 PASSWORD 'gauss@123';
set search_path to public;
revoke select on dep_priv_test from regressuser4 cascade;
\dp dep_priv_test
set session role regressuser3 PASSWORD 'gauss@123';
set search_path to public;
revoke select on dep_priv_test from regressuser4 cascade;
\dp dep_priv_test
set session role regressuser1 PASSWORD 'gauss@123';
set search_path to public;
drop table dep_priv_test;


-- clean up

\c

drop sequence x_seq;

DROP TABLE atest1;
DROP TABLE atest2;
DROP TABLE atest3;
DROP TABLE atest4;

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