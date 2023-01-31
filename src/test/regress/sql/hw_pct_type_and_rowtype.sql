--For Composite Type with %TYPE and %ROWTYPE

-- check compatibility --
show sql_compatibility; -- expect ORA --
-- create new schema --
drop schema if exists sql_compositetype;
create schema sql_compositetype;
set current_schema = sql_compositetype;

-- initialize table and type--
create type ctype1 as (a int, b int);
create table foo(a int, b ctype1);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

--general create type with %TYPE and %ROWTYPE
create type ctype2 as (a foo.a%TYPE, b foo.b%TYPE);
create type ctype3 as (a foo%ROWTYPE);

--create type with schema of table
create type ctype4 as (a sql_compositetype.foo.a%TYPE, b sql_compositetype.foo.b%TYPE);
create type ctype5 as (a sql_compositetype.foo%ROWTYPE);

--create type with database and schema of table
create type ctype6 as (a regression.sql_compositetype.foo.a%TYPE, b regression.sql_compositetype.foo.b%TYPE);
create type ctype7 as (a regression.sql_compositetype.foo%ROWTYPE);

--ERROR: %TYPE with table is not allowed
create type ctype8 as (a foo%TYPE);
create type ctype9 as (a sql_compositetype.foo%TYPE);
create type ctype10 as (a regression.sql_compositetype.foo%TYPE);

--ERROR: %ROWTYPE with attribute is not allowed
create type ctype11 as (a foo.a%ROWTYPE, b foo.b%ROWTYPE);
create type ctype12 as (a sql_compositetype.foo.a%ROWTYPE, b sql_compositetype.foo.b%ROWTYPE);
create type ctype13 as (a regression.sql_compositetype.foo.a%ROWTYPE, b regression.sql_compositetype.foo.b%ROWTYPE);

--ERROR: %TYPE and %ROWTYPE with type is not allowed
create type ctype14 as (a ctype1%TYPE);
create type ctype15 as (a ctype1%ROWTYPE);

--ERROR: %ROWTYPE with incorrect database or schema is not allowed
create type ctype16 as (a postgres.sql_compositetype.foo%ROWTYPE, b postgres.sql_compositetype.foo%ROWTYPE);
create type ctype16 as (a regression.sql.foo%ROWTYPE, b regression.sql.foo%ROWTYPE);
create type ctype16 as (a sql.foo%ROWTYPE, b sql.foo%ROWTYPE);

--ERROR: %ROWTYPE with more than 4 dots is not allowed
create type ctype16 as (a regression.sql_compositetype.foo.a%ROWTYPE, b regression.sql_compositetype.foo.b%ROWTYPE);
create type ctype16 as (a postgres.regression.sql_compositetype.foo.a%ROWTYPE, b postgres.regression.sql_compositetype.foo.b%ROWTYPE);

--test select stmt for %TYPE and %ROWTYPE
create table t1(a int , b int);
create table t2(a int, b t1);
insert into t2 values(1,(2,3));
drop function if exists get_t2;
create or replace function get_t2() RETURNS record as $$ 
declare
    v_a record;
begin 
    select * into v_a from t2;
    return v_a;
end;
$$ language plpgsql;
select t.b from get_t2() as t (a t2.a%TYPE, b t1%ROWtype);

--test update stmt for %TYPE and %ROWTYPE
update t2 SET a = t.a + 1 from get_t2() as t (a t2.a%TYPE, b t1%ROWtype);
select * from t2;

--test alter type for %TYPE and %ROWTYPE
ALTER TYPE ctype2 ADD ATTRIBUTE c foo.a%TYPE;
ALTER TYPE ctype2 ADD ATTRIBUTE d foo%ROWTYPE;

--test drop function for %TYPE and %ROWTYPE
create or replace function get_int(i int) RETURNS int as $$ 
begin 
   return i.a;
end;
$$ language plpgsql;
drop function get_int(i foo.b%TYPE); --should fail
drop function get_int(i foo%ROWTYPE); --should fail
drop function get_int(foo.a%TYPE); --should success

create or replace function get_int(i foo%ROWTYPE) RETURNS int as $$ 
begin 
   return i.a;
end;
$$ language plpgsql;
drop function get_int(i foo.b%TYPE); --should fail
drop function get_int(i t2%ROWTYPE); --should fail
drop function get_int(i foo%ROWTYPE); --should success

--test typmod whether reversed by %TYPE and %ROWTYPE
create table v1(a int, b varchar(2));
create type ctype17 as (a v1.b%TYPE[],b v1%ROWTYPE);
create table v2(a int, b ctype17);
insert into v2 values(1, (array['aa','bb'], (2,'cc')));
select * from v2;
insert into v2 values(1, (array['aaa','bb'], (2,'cc')));
insert into v2 values(1, (array['aa','bb'], (2,'ccc')));

--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------

drop table if exists v2;
drop type if exists ctype17;
drop table if exists v1;
drop function if exists get_t2;
drop table if exists t2;
drop table if exists t1;
drop type if exists ctype16;
drop type if exists ctype15;
drop type if exists ctype14;
drop type if exists ctype13;
drop type if exists ctype12;
drop type if exists ctype11;
drop type if exists ctype10;
drop type if exists ctype9;
drop type if exists ctype8;
drop type if exists ctype7;
drop type if exists ctype6;
drop type if exists ctype5;
drop type if exists ctype4;
drop type if exists ctype3;
drop type if exists ctype2;
drop table if exists foo;
drop type if exists ctype1;

-- clean up --
drop schema if exists sql_compositetype cascade;















--For Function Return Type with %TYPE, %ROWTYPE and []

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists sql_functionreturn;
create schema sql_functionreturn;
set current_schema = sql_functionreturn;

-- initialize table and type--
create type ctype1 as (a int, b int);
create table foo(a int, b ctype1);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

--test function return type for %TYPE with composite type
create or replace function get_ctype1 RETURNS foo.b%TYPE as $$ 
declare
    v_a ctype1;
    begin
    v_a := (1,2);
    return v_a;
end;
$$ language plpgsql;
select get_ctype1();

--test function return type for %TYPE with simple type
create or replace function get_int RETURNS foo.a%TYPE as $$ 
declare
    v_a int;
begin 
    v_a := 1;
    return v_a;
end;
$$ language plpgsql;
select get_int();

--test function return type for %TYPE[] with simple type
create or replace function get_intarray RETURNS foo.a%TYPE[] as $$ 
declare
    type arr is VARRAY(10) of int; 
    v_a arr := arr();
begin 
    v_a.extend(1);
    v_a(1) := 1;
    v_a.extend(1);
    v_a(2) := 2;
    return v_a;
end;
$$ language plpgsql;
select get_intarray();

--test function return type for %TYPE[] with composite type
create or replace function get_ctype1array RETURNS foo.b%TYPE[] as $$ 
declare
    type arr is VARRAY(10) of ctype1; 
    v_a arr := arr();
begin 
    v_a.extend(1);
    v_a(1) := (1,2);
    v_a.extend(1);
    v_a(2) := (3,4);
    return v_a;
end;
$$ language plpgsql;
select get_ctype1array();

--test function return type for %ROWTYPE
create or replace function get_foo RETURNS foo%ROWTYPE as $$ 
declare
    v_a foo;
begin 
    v_a := (1,(2,3));
return v_a;
end;
$$ language plpgsql;
select get_foo();

--test function return type for %ROWTYPE[]
create or replace function get_fooarray RETURNS foo%ROWTYPE[] as $$ 
declare
    type arr is VARRAY(10) of foo; 
    v_a arr := arr();
begin 
    v_a.extend(1);
    v_a(1) := (1,(2,3));
    v_a.extend(1);
    v_a(2) := (4,(5,6));
    return v_a;
end;
$$ language plpgsql;
select get_fooarray();

--test function return type for SETOF %TYPE[] with simple type
create or replace function get_set_intarray RETURNS SETOF foo.a%TYPE[] as $$ 
declare
    type arr is VARRAY(10) of int; 
    v_a arr := arr();
begin 
    v_a.extend(1);
    v_a(1) := 1;
	RETURN NEXT v_a;
    v_a.extend(1);
    v_a(2) := 2;
	RETURN NEXT v_a;
    return;
end;
$$ language plpgsql;
select get_set_intarray();

--test %TYPE for variable
create or replace function f1(ss in int) return int as
  va foo%ROWTYPE;
  vb va.b%TYPE;
  vc va.a%TYPE;
begin
  va := (1, (2, 3));
  vb := (4, 5);
  vc := 6;
  raise info '% % %',va , vb, vc;
  vb.a := vc;
  va.b := vb;
  va.a := vc;
  raise info '% % %',va , vb, vc;
  return va.a;
end;
/
select f1(1);

--ERROR: test %TYPE for variable, not existed field 
create or replace function f1(ss in int) return int as
  va foo%ROWTYPE;
  vb va.b%TYPE;
  vc va.c%TYPE;
begin
  va := (1, (2, 3));
  vb := (4, 5);
  vc := 6;
  raise info '% % %',va , vb, vc;
  vb.a := vc;
  va.b := vb;
  va.a := vc;
  raise info '% % %',va , vb, vc;
  return va.a;
end;
/
DROP function f1();

--test synonym type
DROP SCHEMA IF EXISTS sql_compositetype_test;
CREATE SCHEMA sql_compositetype_test;

CREATE TABLE sql_compositetype_test.tabfoo(a int, b int);
CREATE TYPE sql_compositetype_test.compfoo AS (f1 int, f2 text);

CREATE OR REPLACE SYNONYM tabfoo for sql_compositetype_test.tabfoo;
CREATE OR REPLACE SYNONYM compfoo for sql_compositetype_test.compfoo;

create table t1 (a int, b compfoo);
CREATE OR REPLACE PROCEDURE pro_test_tab (in_tabfoo tabfoo%rowtype)
AS
BEGIN
END;
/

--防止权限问题，将用户建成sysadmin
create user synonym_user_1 password 'hauwei@123' sysadmin;
create user synonym_user_2 password 'hauwei@123' sysadmin;
--授予public权限
grant all privileges on schema sql_functionreturn to synonym_user_1;
grant all privileges on schema sql_functionreturn to synonym_user_2;
--创建测试表
drop table if exists synonym_user_1.tb_test;
create table synonym_user_1.tb_test(col1 int,col2 int);
--创建同义词
create or replace synonym sql_functionreturn.tb_test for synonym_user_1.tb_test;
create or replace synonym synonym_user_2.tb_test for synonym_user_1.tb_test;
--创建测试package
create or replace package synonym_user_2.pckg_test as
v_a tb_test.col1%type;
v_b tb_test%rowtype;
procedure proc_test(i_col1 in tb_test.col1%type,o_ret out tb_test.col1%type);
function func_test(i_col1 in int) return tb_test.col1%type;
function func_test1(i_col1 in int) return tb_test%rowtype;
end pckg_test;
/
create or replace package body synonym_user_2.pckg_test as
procedure proc_test(i_col1 in tb_test.col1%type,o_ret out tb_test.col1%type)as
begin
select col1 into o_ret from tb_test where col1=i_col1;
end;
function func_test(i_col1 in int) return tb_test.col1%type as
begin
select col1 into v_a from tb_test where col1=i_col1;
return v_a;
end;
function func_test1(i_col1 in int) return tb_test%rowtype as
begin
for rec in (select col1,col2 from tb_test where col1=i_col1) loop
v_b.col1:=rec.col1;
v_b.col2:=rec.col2;
end loop;
return v_b;
end;
end pckg_test;
/
DROP PACKAGE synonym_user_2.pckg_test;
DROP USER synonym_user_2 CASCADE;
DROP USER synonym_user_1 CASCADE;

--test public synonym in PLpgSQL
set behavior_compat_options= 'bind_procedure_searchpath';
--防止权限问题，将用户建成sysadmin
create user synonym_user_1 password 'Gauss_234' sysadmin;
create user synonym_user_2 password 'Gauss_234' sysadmin;
--授予public权限
grant all privileges on schema public to synonym_user_1;
grant all privileges on schema public to synonym_user_2;
--创建测试表
drop table if exists synonym_user_1.tb_test;
create table synonym_user_1.tb_test(col1 int,col2 int);
--创建同义词
create or replace synonym public.tb_test for synonym_user_1.tb_test;
create or replace package synonym_user_2.pckg_test as
v_a tb_test.col1%type;
v_b tb_test%rowtype;
procedure proc_test(i_col1 in tb_test.col1%type,o_ret out tb_test.col1%type);
function func_test(i_col1 in int) return tb_test.col1%type;
function func_test1(i_col1 in int) return tb_test%rowtype;
end pckg_test;
/
create or replace package body synonym_user_2.pckg_test as
procedure proc_test(i_col1 in tb_test.col1%type,o_ret out tb_test.col1%type)as
begin
select col1 into o_ret from tb_test where col1=i_col1;
end;
function func_test(i_col1 in int) return tb_test.col1%type as
begin
select col1 into v_a from tb_test where col1=i_col1;
return v_a;
end;
function func_test1(i_col1 in int) return tb_test%rowtype as
begin
for rec in (select col1,col2 from tb_test where col1=i_col1) loop
v_b.col1:=rec.col1;
v_b.col2:=rec.col2;
end loop;
return v_b;
end;
end pckg_test;
/
DROP PACKAGE synonym_user_2.pckg_test;
DROP USER synonym_user_2 CASCADE;
DROP USER synonym_user_1 CASCADE;
reset behavior_compat_options;

--test 同义词引用存储过程和package
set behavior_compat_options= 'bind_procedure_searchpath';
--防止权限问题，将用户建成sysadmin
create user synonym_user_1 password 'Gauss_234' sysadmin;
create user synonym_user_2 password 'Gauss_234' sysadmin;
--授予public权限
grant all privileges on schema public to synonym_user_1;
grant all privileges on schema public to synonym_user_2;
set current_schema = public;
--创建测试存储过程
create or replace procedure synonym_user_1.proc_test()as
begin
raise info 'test procedure';
end;
/
--创建测试package
create or replace package synonym_user_1.pckg_test1 as
procedure proc_test2();
end pckg_test1;
/
create or replace package body synonym_user_1.pckg_test1 as
procedure proc_test2()as
begin
raise info 'test package procedure';
end;
end pckg_test1;
/
--创建同义词
create or replace synonym public.proc_test for synonym_user_1.proc_test;
create or replace synonym public.pckg_test1 for synonym_user_1.pckg_test1;
show search_path;
--创建测试package
create or replace package synonym_user_2.pckg_test as
procedure proc_test1();
end pckg_test;
/
create or replace package body synonym_user_2.pckg_test as
procedure proc_test1()as

begin
proc_test();
pckg_test1.proc_test2();
end;
end pckg_test;
/

call synonym_user_2.pckg_test.proc_test1();
DROP PACKAGE synonym_user_2.pckg_test;
DROP PACKAGE synonym_user_1.pckg_test1;
DROP PROCEDURE synonym_user_1.proc_test();
DROP USER synonym_user_2 CASCADE;
DROP USER synonym_user_1 CASCADE;
reset behavior_compat_options;

--test Package return record.col%TYPE
create schema synonym_schema1;
create schema synonym_schema2;

set search_path = synonym_schema1;

--测试packgae内部引用
create or replace package p_test1 as
    type t1 is record(c1 varchar2, c2 int);
    function f1(ss in t1) return t1.c2%TYPE;
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in t1) return t1.c2%TYPE as
    begin
    return ss.c2;
    end;
end p_test1;
/

select p_test1.f1(('aa',5));

--测试跨packgae引用
create or replace package p_test2 as
    function ff1(ss in p_test1.t1) return p_test1.t1.c2%TYPE;
    va p_test1.t1.c2%TYPE;
end p_test2;
/
create or replace package body p_test2 as
    vb p_test1.t1.c2%TYPE;
    function ff1(ss in p_test1.t1) return p_test1.t1.c2%TYPE as
    begin
    return ss.c2;
    end;
end p_test2;
/

select p_test2.ff1(('aa',55));

--测试跨schema packgae引用
set search_path = synonym_schema2;

create or replace package p_test2 as
   function fff1(ss in synonym_schema1.p_test1.t1) return synonym_schema1.p_test1.t1.c2%TYPE;
end p_test2;
/
create or replace package body p_test2 as
    function fff1(ss in synonym_schema1.p_test1.t1) return synonym_schema1.p_test1.t1.c2%TYPE as
    begin
    return ss.c2;
    end;
end p_test2;
/

select p_test2.fff1(('aa',555));

DROP PACKAGE p_test2;
DROP PACKAGE synonym_schema1.p_test2;
DROP PACKAGE synonym_schema1.p_test1;

DROP SCHEMA synonym_schema2 CASCADE;
DROP SCHEMA synonym_schema1 CASCADE;

set current_schema = sql_functionreturn;

--test pkg.val%TYPE
create schema synonym_schema1;
create schema synonym_schema2;

set search_path = synonym_schema1;

create or replace package pck1 is
va int;
end pck1;
/
create or replace package body pck1 as
    function f1(ss in int) return int as
    begin
    return ss;
    end;
end pck1;
/

--测试跨packgae引用
create or replace package p_test2 as
    va pck1.va%TYPE;
    procedure p1 (a pck1.va%TYPE);
end p_test2;
/
create or replace package body p_test2 as
    procedure p1 (a pck1.va%TYPE) as
    begin
    NULL;
    end;
end p_test2;
/

--测试跨schema packgae引用
set search_path = synonym_schema2;
create or replace package p_test2 as
    va synonym_schema1.pck1.va%TYPE;
    procedure p1 (a synonym_schema1.pck1.va%TYPE);
end p_test2;
/
create or replace package body p_test2 as
    procedure p1 (a synonym_schema1.pck1.va%TYPE) as
    begin
    NULL;
    end;
end p_test2;
/

DROP PACKAGE p_test2;
DROP PACKAGE synonym_schema1.p_test2;
DROP PACKAGE synonym_schema1.pck1;

DROP SCHEMA synonym_schema2 CASCADE;
DROP SCHEMA synonym_schema1 CASCADE;

set current_schema = sql_functionreturn;

--test keyword table name used by keyword.col%TYPE
DROP TABLE if EXISTS type;
CREATE TABLE type(a int, b int);
create or replace package p_test1 as
    type r1 is record(c1 type%ROWTYPE, c2 int);
	procedure p1 (a in type%ROWTYPE);
end p_test1;
/
create or replace procedure func1 as
	va type%rowtype;
	type r1 is record(c1 type%rowtype,c2 varchar2(20));
begin
  va := 'a';
  raise info '%',va;
  va := 'b';
  raise info '%',va;
end;
/

DROP PROCEDURE func1;
DROP PACKAGE p_test1;
DROP TABLE type;

--test row type default value
DROP SYNONYM tb_test;
CREATE TABLE tb_test(a int, b int);
create or replace package pckg_test as
procedure proc_test(i_col1 in tb_test);
end pckg_test;
/
create or replace package body pckg_test as
procedure proc_test(i_col1 in tb_test)as
v_idx tb_test%rowtype:=i_col1;
v_idx2 tb_test%rowtype;
begin
raise info '%', v_idx;
raise info '%', v_idx2;
end;
end pckg_test;
/
call pckg_test.proc_test((11,22));

drop package pckg_test;
drop table tb_test;

-- test record type default value
create or replace procedure p1 is
type r1 is record (a int :=1,b int :=2);
va r1 := (2,3);
begin
raise info '%', va;
end;
/
call p1();
create or replace procedure p1 is
type r1 is record (a int :=1,b int :=2);
va r1 := NULL;
begin
raise info '%', va;
end;
/
call p1();

DROP procedure p1;

--test record type default value in package
create or replace package pck1 is
type t1 is record(c1 int := 1, c2 int := 2);
va t1 := (4,5);
end pck1;
/

declare
begin
raise info '%',pck1.va;
end;
/

DROP package pck1;

-- test rowVar%TYPE
-- (1) in procedure
create table test1 (a int , b int);
create or replace procedure p1() is
va test1%ROWTYPE;
vb va%ROWTYPE;
begin
vb := (1,2);
raise info '%',vb;
end;
/
call p1();
-- (1) record var%TYPE, should error
create or replace procedure p1() is
TYPE r1 is record (a int, b int);
va r1;
vb va%ROWTYPE;
begin
vb := (1,2);
raise info '%',vb;
end;
/

-- (2) in package
create or replace package pck1 is
va test1%ROWTYPE;
vb va%ROWTYPE;
end pck1;
/
drop package pck1;
-- (2) record var%TYPE, should error
create or replace package pck1 is
TYPE r1 is record (a varchar(10), b int);
va r1;
vb va%ROWTYPE;
end pck1;
/
drop package pck1;
-- (3) across package
create or replace package pck1 is
va test1%ROWTYPE;
end pck1;
/

create or replace package pck2 is
va pck1.va%ROWTYPE := (2,3);
end pck2;
/

declare
begin
raise info '%', pck2.va;
end;
/
drop package pck2;
drop package pck1;
-- (3) record var%TYPE, should error
create or replace package pck1 is
TYPE r1 is record (a varchar(10), b int);
va r1;
end pck1;
/

create or replace package pck2 is
va pck1.va%ROWTYPE := (2,3);
end pck2;
/
DROP PACKAGE pck2;
DROP PACKAGE pck1;
DROP PROCEDURE p1();
DROP TABLE test1;
-- test array var%TYPE
create or replace procedure p1() is
type r1 is varray(10) of int;
va r1;
vb va%TYPE;
begin
vb(1) := 1;
raise info '%',vb;
end;
/
call p1();

create or replace procedure p1() is
type r1 is table of int index by varchar2(10);
va r1;
vb va%TYPE;
begin
vb('aaa') := 1;
raise info '%',vb;
end;
/

call p1();

DROP PROCEDURE p1;


--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------

drop procedure if exists pro_test_tab;
drop table if exists t1;
drop synonym if exists compfoo;
drop synonym if exists tabfoo;
drop type if exists sql_compositetype_test.compfoo;
drop table if exists sql_compositetype_test.tabfoo;
drop function if exists get_set_intarray;
drop function if exists get_fooarray;
drop function if exists get_foo;
drop function if exists get_ctype1array;
drop function if exists get_intarray;
drop function if exists get_int;
drop function if exists get_ctype1;
drop table if exists foo;
drop type if exists ctype1;

-- clean up --
drop schema if exists sql_compositetype_test cascade;
drop schema if exists sql_functionreturn cascade;
