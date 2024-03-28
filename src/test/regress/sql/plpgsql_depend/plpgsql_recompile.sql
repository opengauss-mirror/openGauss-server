drop schema if exists plpgsql_recompile cascade;
create schema plpgsql_recompile;
set current_schema = plpgsql_recompile;

create type s_type as (
    id integer,
    name varchar,
    addr text
);

--- 不设置 plpgsql_dependency
set behavior_compat_options = '';
--- test 0
create or replace procedure proc_no_guc(a s_type)
is
begin
RAISE INFO 'call a: %', a;
end;
/
call proc_no_guc(((1,'zhang','shanghai')));

alter type s_type ADD attribute a int;
--- 修改后，预期新增的参数值不打印
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc_no_guc' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call proc_no_guc((1,'zhang','shanghai',100));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc_no_guc' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

alter type s_type DROP attribute a;
--- 修改后，预期valid字段无变化
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc_no_guc' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call proc_no_guc((1,'zhang','shanghai'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='proc_no_guc' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
--- drop procedure
drop procedure proc_no_guc;

--- 设置 plpgsql_dependency
set behavior_compat_options = 'plpgsql_dependency';
--- test 1
create or replace procedure type_alter(a s_type)
is
begin    
    RAISE INFO 'call a: %', a;
end;
/
select valid from pg_object where object_type='P' and object_oid 
in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = 
(select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai'));

alter function type_alter compile;
alter procedure type_alter(s_type) compile;

alter type s_type ADD attribute a int;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai',100));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

alter type s_type DROP attribute a;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

alter type s_type ALTER attribute id type float;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
alter procedure type_alter compile;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1.0,'zhang','shanghai'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

-- report error
alter type s_type RENAME to s_type_rename;
ALTER type s_type RENAME ATTRIBUTE name TO new_name;
call type_alter((1,'zhang','shanghai'));

-- test 2
drop table if exists stu;
create table stu(sno int, name varchar, sex varchar, cno int);
create type r1 as (a int, c stu%RowType);

drop package if exists pkg;
create or replace package pkg
is    
procedure proc1(p_in r1);
end pkg;
/
create or replace package body pkg
is
declare
v1 r1;
v2 stu%RowType;
procedure proc1(p_in r1) as
begin        
RAISE INFO 'call p_in: %', p_in;
end;
end pkg;
/

call pkg.proc1((1,(1,'zhang','M',1)));
alter package pkg compile;
alter package pkg compile specification;
alter package pkg compile body;
alter package pkg compile package;

alter table stu ADD column b int;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1,2)));


alter table stu DROP column b;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1)));

alter table stu RENAME to stu_re;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1)));

alter type r1 ALTER attribute a type float;
alter package pkg compile;
call pkg.proc1((1.0,(1,'zhang','M',1)));

--compile error
create or replace package body pkg
is
declare
v1 r1%RowType;
procedure proc1(p_in r1) as
begin
select into v1 values (1,ROW(1,'zhang','M',1));     
RAISE INFO 'call p_in: %', p_in;
end;
end pkg;
/
alter package pkg compile;

-- test 3
create procedure test_subtype_proc is
xx s_type;
begin
xx := (1,'aaa','bbb');
raise notice '%',xx;
end;
/

call test_subtype_proc();
alter type s_type ADD attribute a int;
--修改后，预期调用成功
call test_subtype_proc();
--手动触发编译， 调用成功
alter procedure test_subtype_proc compile;
call test_subtype_proc();

-- test 4
create or replace package pkg
is    
procedure proc1();
end pkg;
/
create or replace package body pkg
is
xxx s_type;
procedure proc1() as
yyy s_type;
begin        
xxx:= (1,'aaa','aaaaa');
yyy:= (2,'bbb','bbbbb');
RAISE INFO 'call xxx: %, yyy: %', xxx, yyy;
end;
end pkg;
/

call pkg.proc1();
alter type s_type ADD attribute b int;
--修改后，预期调用成功
call pkg.proc1();
--手动触发编译， 调用成功
alter package pkg compile;
call pkg.proc1();

-- test 5
create or replace package pkg
is    
procedure proc1(p_in s_type);
end pkg;
/
create or replace package body pkg
is
xxx s_type;
procedure proc1(p_in s_type) as
yyy s_type;
begin        
xxx:= (1,'aaa','aaaaa');
yyy:= (2,'bbb','bbbbb');
RAISE INFO 'call xxx: %, yyy: %, p_in: %', xxx, yyy, p_in;
end;
end pkg;
/

call pkg.proc1((1,'zhang','M',1,2));
alter type s_type ADD attribute c int;
--修改后，预期调用成功
call pkg.proc1((1,'zhang','M',1,2,3));
--手动触发编译， 调用成功
alter package pkg compile;
call pkg.proc1((1,'zhang','M',1,2,3));

-- test 6
drop type if exists r1;
drop table if exists stu;
create table stu(sno int, name varchar, sex varchar, cno int);
create type r1 as (a int, c stu%RowType);
create schema plpgsql_recompile_new;

create or replace procedure test_proc(p_in r1)
is
declare
v1 stu%RowType;
begin        
RAISE INFO 'call p_in: %', p_in;
end;
/
call test_proc((1,(1,'zhang','M',1)));

-- rename report error
ALTER TYPE r1 RENAME to r1_rename;
ALTER TYPE r1 RENAME ATTRIBUTE a TO new_a;
ALTER TABLE stu RENAME  TO stu_rename;
ALTER TABLE stu RENAME COLUMN cno TO new_cno;
ALTER PROCEDURE test_proc(r1) RENAME TO test_proc_rename;
ALTER SCHEMA plpgsql_recompile RENAME TO plpgsql_recompile_rename;
call test_proc((1.0,(1,'zhang','M',1)));

-- set schema report error
ALTER TYPE r1 SET SCHEMA plpgsql_recompile_new;
ALTER TABLE stu SET SCHEMA plpgsql_recompile_new;
ALTER PROCEDURE test_proc(r1) SET SCHEMA plpgsql_recompile_new;
call test_proc((1.0,(1,'zhang','M',1)));

-- drop and recreate
drop type r1;
drop table stu;
drop procedure test_proc;

create table stu(sno int, name varchar, sex varchar, cno int);
create type r1 as (a int, c stu%RowType);
create or replace procedure test_proc(p_in r1)
is
declare
v1 stu%RowType;
begin        
RAISE INFO 'call p_in: %', p_in;
end;
/
call test_proc((1,(1,'zhang','M',1)));

drop type r1;
drop table stu;
drop procedure test_proc;

-- test 7
set behavior_compat_options = 'plpgsql_dependency';
create or replace package pkg1 is
type tttt is record (col1 int, col2 text);
procedure p1(param pkg2.tqqq);
end pkg1;
/
create or replace package pkg2 is
type tqqq is record (col1 int, col2 text, col3 varchar);
procedure p1(param pkg1.tttt);
end pkg2;
/
create or replace package body pkg2 is
procedure p1(param pkg1.tttt) is
begin
RAISE INFO 'call param: %', param;
end;
end pkg2;
/

call pkg2.p1((1,'a'));
set behavior_compat_options ='';
drop package pkg1;
set behavior_compat_options = 'plpgsql_dependency';
drop package pkg2;

-- clean
drop schema plpgsql_recompile_new cascade;
drop schema plpgsql_recompile cascade;
reset behavior_compat_options;
