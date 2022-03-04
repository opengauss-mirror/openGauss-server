create role gs_developper password 'Dev@9999'sysadmin;
set role gs_developper password 'Dev@9999';
create schema gs_source;
set current_schema = gs_source;
set plsql_show_all_error = on;
truncate DBE_PLDEVELOPER.gs_source;
truncate DBE_PLDEVELOPER.gs_errors;
--
-- basic cases
--
-- function normal
CREATE OR REPLACE PROCEDURE gourav88(a inout int, b out int, c out int, d in int, e inout int)
PACKAGE
AS DECLARE V1 INT;
param1 INT;
param2 INT;
BEGIN
param1 := 10;
param2 := 20;
V1 := param1 + param2;
a:=10;
b:=100;
c:=1000;
create table if not exists gourav.gourav888 (a integer, b integer, c integer);
insert into gourav.gourav888 values(a,b,c);
END;
/
create or replace function func1() returns boolean as $$ declare
sql_temp text;
begin
    sql_temp := 'create table test(a int);';
    execute immediate sql_temp;
    return true;
end;
$$ language plpgsql;
-- function fail
create or replace function func2() returns boolean as $$ declare
sql_temp text;
begin
    sql_temp1 := 'create table test(a int);';
    execute immediate sql_temp;
    return true;
end;
$$ language plpgsql;
-- procedure fail/procedure does not replace function with the same name
create or replace procedure func1
is
begin
insert into fasd af asd asdf;
end;
/
-- package
CREATE OR REPLACE PACKAGE emp_bonus9 AS
da int;
PROCEDURE aa(gg int,kk varchar2);
PROCEDURE aa(kk int,gg int);
END emp_bonus9;
/
-- package body with failure
CREATE OR REPLACE PACKAGE BODY emp_bonus9 AS 
dd int;
PROCEDURE aa(gg int,kk varchar2)
IS
BEGIN
insert int aa aa;
END;
PROCEDURE aa(kk int,gg int)
IS
BEGIN
insert into test1 values(77);
END;
END emp_bonus9;
/
select rolname, name, status, type, src from DBE_PLDEVELOPER.gs_source s join pg_authid a on s.owner = a.oid order by name;
-- check if id is valid
select distinct p.pkgname, s.name from gs_package p, DBE_PLDEVELOPER.gs_source s where s.id = p.oid order by name;
select distinct p.proname, s.name from pg_proc p, DBE_PLDEVELOPER.gs_source s where s.id = p.oid order by name;
select s.name, count(*) from DBE_PLDEVELOPER.gs_errors E, DBE_PLDEVELOPER.gs_source s where s.id = e.id group by s.name order by name;
truncate DBE_PLDEVELOPER.gs_source;
--
-- extended cases
--
-- sensitive information masking
create or replace procedure mask
is
begin
create role phil password 'Phil@123';
end;
/
-- change of owner
create role jackson_src password 'Jackson#456' sysadmin;
set role jackson_src password 'Jackson#456';
create or replace procedure func1
is
begin
insert into fasd af asd asdf;
end;
/
set role gs_developper password 'Dev@9999';
set behavior_compat_options='allow_procedure_compile_check';
-- [no log] trigger func
CREATE TABLE table_stats (
    table_name text primary key,
    num_insert_query int DEFAULT 0);
CREATE FUNCTION count_insert_query() RETURNS TRIGGER AS $_$
BEGIN
    UPDATE table_stats SET num_insert_query = num_insert_query + 1  WHERE table_name = TG_TABLE_NAME;
    RETURN NEW;
END $_$ LANGUAGE 'plpgsql';
-- [no log] sql function
create function func0(integer,integer) RETURNS integer
AS 'SELECT $1 + $2;'
LANGUAGE SQL
IMMUTABLE SHIPPABLE
RETURNS NULL ON NULL INPUT;
-- [no log] duplicate function definition without replace mark
create function func1() returns boolean as $$ declare
sql_temp text;
begin
    sql_temp := 'create table test(a int);';
    execute immediate sql_temp;
    return true;
end;
$$ language plpgsql;
create or replace procedure p1(a varchar2(10))
is
begin
    CREATE ROW LEVEL SECURITY POLICY p02 ON document_row AS WHATEVER
    USING (dlevel <= (SELECT aid FROM account_row WHERE aname = current_user));
    insert int asd asd;
    insert into test1 values(1);
    insert int asd asd;
end;
/
select name,type,line,src from DBE_PLDEVELOPER.gs_errors order by name;
create or replace package pkg1
is
procedure proc1(c ff%F);
end pkg1;
/
select name,type,line,src from DBE_PLDEVELOPER.gs_errors order by name;
create or replace package pkg2
is
a inv%d;
procedure proc1(c ff%F);
end pkg2;
/
create or replace package pkg3
is
a int;
end pkg3;
/
create or replace package body pkg3
is
a b c d;
procedure proc1()
is
begin
insert int asd asd;
end;
end pkg3;
/
select name,type,line,src from DBE_PLDEVELOPER.gs_errors;

create or replace procedure pro70 is
begin
savepoint save_a;
commit;
savepoint save_a;
end;
/
create table t1 (id int);
create or replace procedure pro71 is
cursor c1 for select pro70() from t1;
val int;
begin
end;
/
create or replace function bulk_f_039_1() returns int[]
LANGUAGE plpgsql AS 
$$ 
declare
var1 int[];
CURSOR C1 IS select id,id from t1 order by 1 desc;
begin
return var1;
end;
$$;
create or replace procedure pro25
as
type tpc1 is ref cursor;
--v_cur tpc1;
begin
open v_cur for select c1,c2 from tab1;
end;
/

CREATE OR REPLACE PACKAGE error2 IS 
a int;b int;
FUNCTION func1(a in int, b inout int, c out int) return int;
FUNCTION func2(a in int, b inout int, c out int) return int;
END error2;
/
CREATE OR REPLACE PACKAGE BODY error2 IS
FUNCTION func1 (a in int, b inout int c out int) return int
IS
a1 NUMBER;
BEGIN
a1 :=10;
RETURN(a1 + a + b);
END;
aaa;
FUNCTION func2 (a in int, b inout int c out int) return int
IS
a1 NUMBER;
BEGIN
a1 :=10;
RETURN(a1 + a + b);
END;
END error2;
/

select name,type,line,src from DBE_PLDEVELOPER.gs_errors order by name;
-- [no log] guc off
set plsql_show_all_error = off;
create or replace procedure func00
is
begin
create role yyy password 'Gauss@123';
end;
/
set plsql_show_all_error=on;
create or replace procedure proc4
is
begin
insert int a;
end;
/
create or replace package pkg4
is
a a;
end pkg4;
/
select name,type,src,line from DBE_PLDEVELOPER.gs_errors order by name;
select name,type,status,src from DBE_PLDEVELOPER.gs_source order by name;
set plsql_show_all_error=off;
create or replace procedure proc4
is
b int;
c int;
begin
insert int a;
end;
/
create or replace package pkg4
is
a a;
end pkg4;
/
select name,type,status,src from DBE_PLDEVELOPER.gs_source order by name;
create or replace procedure proc5
is
b int;
c int;
begin
insert int a;
end;
/
create or replace package pkg5
is
a a;
end pkg5;
/
create or replace package pack3 is
array_v1 pack1.array_type1;
procedure pp1();
end pack3;
/
set behavior_compat_options='skip_insert_gs_source';
create or replace procedure SkipInsertGsSource
is
begin
null;
end;
/
set plsql_show_all_error to on;
select name,type,status,src from DBE_PLDEVELOPER.gs_source order by name;
select name,type,line,src from DBE_PLDEVELOPER.gs_errors order by name;
drop package if exists pkg4;
drop package if exists pkg5;
drop function if exists proc4;
drop function if exists proc5;
drop package pkg1;
drop package pkg2;
drop package pkg3;
drop package emp_bonus9;
select rolname, name, status, type, src from DBE_PLDEVELOPER.gs_source s join pg_authid a on s.owner = a.oid order by name;
truncate DBE_PLDEVELOPER.gs_source;
truncate DBE_PLDEVELOPER.gs_errors;
reset role;
reset behavior_compat_options;
drop schema gs_source cascade;
drop role jackson_src;
drop role gs_developper;
