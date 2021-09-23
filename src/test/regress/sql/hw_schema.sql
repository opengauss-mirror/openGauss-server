--
--create the same name schema as the user while creating the user
--
--test
SET CHECK_FUNCTION_BODIES = ON;
DROP USER if exists test_a CASCADE;
DROP USER if exists test_b CASCADE;
CREATE USER test_a PASSWORD 'AAAaaa111';
CREATE USER test_b PASSWORD 'BBBbbb222';

--
--invoke procedures in other function
--

CREATE OR REPLACE FUNCTION multiply_p(integer, integer) RETURNS integer AS $$
    BEGIN
        return $1 * $2;
    END;
$$ LANGUAGE plpgsql;

SET SESSION AUTHORIZATION TEST_A PASSWORD 'AAAaaa111';
CREATE TABLE TEST_A_TABLE(A INT, B INT);
INSERT INTO TEST_A_TABLE VALUES(1, 1);

CREATE OR REPLACE FUNCTION add_a(integer, integer) RETURNS integer AS $$
    BEGIN
        return $1 + $2;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_a_func() RETURNS integer AS $$
DECLARE result integer;
    BEGIN
        select add_a(1, 1) into result;
        return result;
    END;
$$ LANGUAGE plpgsql;

GRANT ALL ON SCHEMA TEST_A TO TEST_B;

SET SESSION AUTHORIZATION test_b PASSWORD 'BBBbbb222';
CREATE TABLE TEST_B_TABLE(A INT, B INT);
INSERT INTO TEST_B_TABLE VALUES(2, 2);

CREATE OR REPLACE FUNCTION minus_b(integer, integer) RETURNS integer AS $$
    BEGIN
        return $1 - $2;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_b_func() RETURNS integer AS $$
DECLARE result integer;
    BEGIN
        select minus_b(1, 1) into result;
        return result;
    END;
$$ LANGUAGE plpgsql;

GRANT ALL ON SCHEMA TEST_B TO TEST_A;

RESET SESSION AUTHORIZATION;
SELECT current_schema;

SET SESSION AUTHORIZATION test_a PASSWORD 'AAAaaa111';
SELECT current_schema;

SET SESSION AUTHORIZATION test_b PASSWORD 'BBBbbb222';
SELECT current_schema;

RESET SESSION AUTHORIZATION;
SELECT current_schema;
SELECT multiply_p(1, 2);
SELECT test_a_func();
SELECT test_b_func();
SELECT test_a.test_a_func();
SELECT test_b.test_b_func();

SET SESSION AUTHORIZATION test_a PASSWORD 'AAAaaa111';
SELECT current_schema;
SELECT multiply_p(1, 2);
SELECT test_a_func();
SELECT test_b_func();
SELECT test_a.test_a_func();
SELECT test_b.test_b_func();

SET SESSION AUTHORIZATION test_b PASSWORD 'BBBbbb222';
SELECT current_schema;
SELECT multiply_p(1, 2);
SELECT test_a_func();
SELECT test_b_func();
SELECT test_a.test_a_func();
SELECT test_b.test_b_func();

RESET SESSION AUTHORIZATION;
RESET search_path;
DROP USER test_a,test_b CASCADE;
DROP FUNCTION multiply_p(integer, integer) CASCADE;

CREATE OR REPLACE FUNCTION TEST_DROP_FUNCTION_001(i integer) RETURNS integer AS $$
BEGIN
CREATE USER fvt_obj_define_function_use_00004  password 'TESTDB@123';
DROP USER FVT_OBJ_DEFINE_FUNCTION_USE_00004;
RETURN i + 1;
END;
$$ LANGUAGE plpgsql;
select TEST_DROP_FUNCTION_001(2);
select 1 from pg_user where USENAME = 'fvt_obj_define_function_use_00004';
CREATE USER FVT_OBJ_DEFINE_FUNCTION_USE_00004  password 'TESTDB@123';
DROP USER FVT_OBJ_DEFINE_FUNCTION_USE_00004;
select 1 from pg_user where USENAME = 'fvt_obj_define_function_use_00004';
CREATE OR REPLACE FUNCTION TEST_DROP_FUNCTION_001(i integer) RETURNS integer AS $$
BEGIN
CREATE USER FVT_OBJ_DEFINE_FUNCTION_USE_00004  IDENTIFIED BY 'TESTDB@123';
DROP USER FVT_OBJ_DEFINE_FUNCTION_USE_00004 cascade;
 RETURN i + 1;
END;
$$ LANGUAGE plpgsql;
select TEST_DROP_FUNCTION_001(2);
select 1 from pg_user where USENAME = 'fvt_obj_define_function_use_00004';

-- test on schema data type passing between CO and DN
--      watch for temp namespace, non-default namespace and default tablespace
--
--create schema myschema;
--create type myschema.char as enum('ax', 'bx');
--create type myschema.bpchar as enum('good', 'one');
--create table mctable(i int, c1 myschema.char, c2 myschema.bpchar, c3 "char", c4 char);
--insert into mctable values (1, 'ax', 'good', 'm', 'm');      -- good
--insert into mctable values (2, 'bx', 'one', 'o', 'p');       -- good
--insert into mctable values (3, 'cx', 'good', 'e', 'f');      -- bad
--insert into mctable values (4, 'ax', 'bad', 'e', 'f');       -- bad
--insert into mctable values (5, 'ax', 'one', 'e', 'ff');      -- bad
--insert into mctable values (6, 'ax', 'one', '?longlong', 'f'); -- good
--select c3, i, c2, c1, count(*), max(i) from mctable group by i, c1, c2, c3, c4 order by 1;
--
--create temp table fullname (first text, last text);
--create temp table people (fn fullname, bd date, c myschema.bpchar, c3 "char");
--insert into people values ('(Joe,Blow)', '1984-01-10', 'good', 'h');
--select * from people order by 1;
--
---- clean up: have to drop table separately as cascade only remove related columns
--drop schema myschema cascade;
--select * from mctable order by 1;
--select * from people order by 1;
--drop table mctable;

