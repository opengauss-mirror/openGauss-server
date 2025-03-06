create database gms_assert_testdb;
\c gms_assert_testdb
create schema gms_assert_test;
set search_path=gms_assert_test;
create extension gms_assert;
create extension gms_output;
select gms_output.enable();

-- NOOP
SELECT gms_assert.noop(NULL);
SELECT gms_assert.noop('');
SELECT gms_assert.noop('\s\t\n');
SELECT gms_assert.noop(E'\s\t\n');
SELECT gms_assert.noop(4.1);
SELECT gms_assert.noop('A line. ');

-- ENQUOTE_LITERAL
SELECT gms_assert.enquote_literal('\s\t\n');
SELECT gms_assert.enquote_literal(E'\s\t\n');
SELECT gms_assert.enquote_literal(4.1);

SELECT gms_assert.enquote_literal(NULL); -- ''
SELECT gms_assert.enquote_literal(''); -- ''
SELECT gms_assert.enquote_literal('AbC'); -- 'AbC'
SELECT gms_assert.enquote_literal('A''''bC'); -- 'A''bC'
SELECT gms_assert.enquote_literal('A''bC'); -- ERROR
SELECT gms_assert.enquote_literal('''AbC'); -- ERROR
SELECT gms_assert.enquote_literal('''AbC'''); -- 'AbC'
SELECT gms_assert.enquote_literal('''A''''bC'''); -- 'A''bC'
SELECT gms_assert.enquote_literal(E'\'A\'\'bC\''); -- 'A''bC'
SELECT gms_assert.enquote_literal('''A''bC'''); -- ERROR
SELECT gms_assert.enquote_literal('\n\s\t&%\'); -- '\n\s\t&%\'
SELECT gms_assert.enquote_literal('''\n\s\t&%\'''); -- '\n\s\t&%\'
SELECT gms_assert.enquote_literal(E'\'\s\t&%\''); -- 's      &%'
SELECT gms_assert.enquote_literal(E'O\'\'hello'); -- 'O''hello'
SELECT gms_assert.enquote_literal(E'\'O\'\'hello\''); -- 'O''hello'
SELECT gms_assert.enquote_literal(10.01); -- '10.01'

-- SIMPLE_SQL_NAME
SELECT gms_assert.simple_sql_name('a\s\t\n');
SELECT gms_assert.simple_sql_name(E'as\t\n');
SELECT gms_assert.simple_sql_name(E'as\t\n\?');
SELECT gms_assert.simple_sql_name(4.1);

SELECT gms_assert.simple_sql_name(NULL); -- ERROR
SELECT gms_assert.simple_sql_name(''); -- ERROR
SELECT gms_assert.simple_sql_name('a_B'); -- a_B
SELECT gms_assert.simple_sql_name('_B'); -- _B
SELECT gms_assert.simple_sql_name('€9'); -- €9
SELECT gms_assert.simple_sql_name('          a_1B$# '); -- a_1B$#
SELECT gms_assert.simple_sql_name('          a_  1B$# '); -- ERROR
SELECT gms_assert.simple_sql_name('a_*B'); -- ERROR
SELECT gms_assert.simple_sql_name('a_""B'); -- ERROR
SELECT gms_assert.simple_sql_name('"a_B"'); -- "a_B"
SELECT gms_assert.simple_sql_name('    "a_ *B" '); -- "a_ *B"
SELECT gms_assert.simple_sql_name('"a_""B"'); -- "a_""B"
SELECT gms_assert.simple_sql_name('"a_B'); -- ERROR

-- ENQUOTE_NAME
SELECT gms_assert.enquote_name(NULL);
SELECT gms_assert.enquote_name('');
SELECT gms_assert.enquote_name('a\s\t\n');
SELECT gms_assert.enquote_name(E'a\s\t\n');
SELECT gms_assert.enquote_name(4.1);

SELECT gms_assert.enquote_name('Ab_c'); -- "AB_C"
SELECT gms_assert.enquote_name('A''b_c'); -- "A'B_C"
SELECT gms_assert.enquote_name('A""b_c'); -- "A""B_C"
SELECT gms_assert.enquote_name('A"ss"b_c'); -- ERROR

SELECT gms_assert.enquote_name('"Ab _c"'); -- "Ab _c"
SELECT gms_assert.enquote_name('   "Ab _c" '); -- ERROR

SELECT gms_assert.enquote_name('Ab_c', true); -- "AB_C"
SELECT gms_assert.enquote_name('Ab_c', false); -- "Ab_c"
SELECT gms_assert.enquote_name('Ab_c', NULL); -- "Ab_c"
SELECT gms_assert.enquote_name('"Ab_c"', true); -- "Ab_c"

-- QUALIFIED_SQL_NAME
SELECT gms_assert.qualified_sql_name(NULL); -- ERROR
SELECT gms_assert.qualified_sql_name(''); -- ERROR
SELECT gms_assert.qualified_sql_name('a\s\t\n'); -- ERROR
SELECT gms_assert.qualified_sql_name(E'a\s\t\n');
SELECT gms_assert.qualified_sql_name(E'a\s\t\n\?'); -- ERROR
SELECT gms_assert.qualified_sql_name(4.1); -- ERROR

SELECT gms_assert.qualified_sql_name('a_$bc');
SELECT gms_assert.qualified_sql_name('  a_$bc ');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e');
SELECT gms_assert.qualified_sql_name('"A*bc"."d#E"');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e@fg');
SELECT gms_assert.qualified_sql_name('ƒ1.€9@abc™');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e@fg@hi');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e@fg@hi.jk');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e@fg@hi@jk');
SELECT gms_assert.qualified_sql_name('a_$bc.d#e@"fg@hi"@jk');

-- SCHEMA_NAME
SELECT gms_assert.schema_name(NULL);
SELECT gms_assert.schema_name('');
SELECT gms_assert.schema_name(E'public\t\n');

SELECT gms_assert.schema_name('public');
SELECT gms_assert.schema_name('Public');

DROP SCHEMA IF EXISTS schem1;
SELECT gms_assert.schema_name('schem1');
CREATE SCHEMA schem1;
SELECT gms_assert.schema_name('schem1');

-- SQL_OBJECT_NAME
SELECT gms_assert.sql_object_name('tbl@abcd');

CREATE TABLE "Tab1"(id int);
CREATE TABLE "Tab1@remote"(id int);
CREATE TABLE "tab2"(id int);
CREATE TABLE "a\b"(id int);
SELECT gms_assert.sql_object_name('"Tab1"');
SELECT gms_assert.sql_object_name('"Tab1@remote"');
SELECT gms_assert.sql_object_name('tab1'); -- ERROR
SELECT gms_assert.sql_object_name('tab2');
SELECT gms_assert.sql_object_name(E'tab2\t\n');
SELECT gms_assert.sql_object_name(E'tab2\t\n\?');  -- ERROR
SELECT gms_assert.sql_object_name('"tab2"'); -- "tab2"
SELECT gms_assert.sql_object_name('"TAB2"'); -- ERROR
SELECT gms_assert.sql_object_name(E'"a\\b"');
DROP TABLE "Tab1";
DROP TABLE "Tab1@remote";
DROP TABLE "tab2";
DROP TABLE "a\b";
SELECT gms_assert.sql_object_name('"Tab1@remote"');
SELECT gms_assert.sql_object_name('Tab1@remote');

CREATE TABLE schem1.tb(id int);
CREATE INDEX tbidx ON schem1.tb(id);
CREATE VIEW schem1.tbv AS SELECT * FROM schem1.tb;
CREATE SYNONYM syn1 FOR schem1.tb;
CREATE SYNONYM syn2 FOR tb;

SELECT gms_assert.sql_object_name('tb'); -- ERROR
SELECT gms_assert.sql_object_name('schem1.tb'); -- schem1.tb
SELECT gms_assert.sql_object_name('Schem1.TB'); -- Schem1.TB
SELECT gms_assert.sql_object_name(CURRENT_CATALOG || '.schem1.tb');
SELECT gms_assert.sql_object_name('schem1.tbidx');
SELECT gms_assert.sql_object_name('schem1.tbv');
SELECT gms_assert.sql_object_name('syn1');
SELECT gms_assert.sql_object_name('syn2'); -- ERROR

DROP SCHEMA IF EXISTS schem2;
CREATE SCHEMA schem2;
CREATE OR REPLACE PACKAGE schem2.pkg1 AS
    FUNCTION access_func (funcname varchar2) RETURN varchar2;
    FUNCTION public_func1(a INTEGER) RETURN INTEGER;
END pkg1;
/

CREATE OR REPLACE PACKAGE BODY schem2.pkg1 AS 
    FUNCTION access_func (funcname varchar2) RETURN varchar2 IS
    BEGIN
        RETURN gms_assert.sql_object_name(funcname);
    END access_func;

    FUNCTION private_func1(a INTEGER) RETURN INTEGER IS
    BEGIN
        RETURN a;
    END private_func1;

    FUNCTION public_func1(a INTEGER) RETURN INTEGER IS
    BEGIN
        RETURN private_func1(a);
    END public_func1;
END pkg1;
/

SELECT gms_assert.sql_object_name('schem2.pkg1');
SELECT gms_assert.sql_object_name('schem2.pkg1.public_func1');
SELECT gms_assert.sql_object_name(CURRENT_CATALOG || '.schem2.pkg1.public_func1');
SELECT gms_assert.sql_object_name('schem2.pkg1.private_func1');
SELECT schem2.pkg1.access_func('private_func1'); -- ERROR
SELECT schem2.pkg1.access_func('pkg1.private_func1'); -- pkg1.private_func1
SELECT schem2.pkg1.access_func('pkg1.private_func'); -- pkg1.private_func

create or replace package "pac_GmsAssert_Case0006_!@#$%_中文字符" 
is
a int;
procedure proc1;
end "pac_GmsAssert_Case0006_!@#$%_中文字符"; 
/
--期望通过
begin
gms_output.put_line(gms_assert.sql_object_name('"pac_GmsAssert_Case0006_!@#$%_中文字符".proc'));
end;
/
--期望通过
begin
gms_output.put_line(gms_assert.sql_object_name('"pac_GmsAssert_Case0006_!@#$%_中文字符".proc1'));
end;
/
--期望报错
begin
gms_output.put_line(gms_assert.sql_object_name('"pac_GmsAssert_Case0006_!@#$%_中文字符1".proc1'));
end;
/
DROP PACKAGE "pac_GmsAssert_Case0006_!@#$%_中文字符";

DROP USER IF EXISTS usr1 cascade;
CREATE USER usr1 WITH password '1234@abcd';
SET SESSION AUTHORIZATION usr1 password '1234@abcd';
SELECT gms_assert.sql_object_name('schem1.tb'); -- ERROR
\c -
GRANT USAGE ON SCHEMA schem1 TO usr1;
SET SESSION AUTHORIZATION usr1 password '1234@abcd';
SELECT gms_assert.sql_object_name('schem1.tb');
SELECT gms_assert.sql_object_name('schem1.tbidx');
SELECT gms_assert.sql_object_name('schem1.tbv'); 
SELECT gms_assert.sql_object_name('syn1');

\c -
GRANT USAGE ON SCHEMA schem2 TO usr1;
SET SESSION AUTHORIZATION usr1 password '1234@abcd';
SELECT gms_assert.sql_object_name('schem2.pkg1');
SELECT gms_assert.sql_object_name('schem2.pkg1.public_func1');

\c -
DROP USER IF EXISTS usr1 CASCADE;
DROP SCHEMA schem1 CASCADE;
DROP PACKAGE schem2.pkg1;
DROP SCHEMA schem2 CASCADE;

DROP EXTENSION gms_assert;

\c postgres
drop database gms_assert_testdb;
