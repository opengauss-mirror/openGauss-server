-- test create type table of 
-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists plpgsql_nest_compile;
create schema plpgsql_nest_compile;
set current_schema = plpgsql_nest_compile;

-- test insert into select array
create table tb1 (a varchar(10), b varchar(10), c varchar(10));

create or replace procedure proc1()
as
declare 
type arra is varray(10) of varchar;
a arra;
b arra;
begin
a(1) := 'a';
a(2) := 'b';
a(3) := 'c';
a(4) := 'd';
a(5) := 'e';
a(6) := 'f';
insert into tb1 select a(1),a(2),a(3);
insert into tb1 select a(4),a(5),a(6);
end;
/

call proc1();

select * from tb1;

create or replace procedure proc2()
as
declare 
type arra is table of varchar;
a arra;
b arra;
begin
a(1) := 'a1';
a(2) := 'b1';
a(3) := 'c1';
a(4) := 'd1';
a(5) := 'e1';
a(6) := 'f1';
insert into tb1 select a(1),a(2),a(3);
insert into tb1 select a(4),a(5),a(6);
end;
/

call proc2();

select * from tb1;

create or replace package pck1 as
procedure proc1(c out varchar2);
end pck1;
/

create or replace package body pck1 as
procedure proc1(c out varchar2)
as 
declare 
type arra is table of varchar;
a arra;
b arra;
begin
a(1) := 'a2';
a(2) := 'b2';
a(3) := 'c2';
a(4) := 'd2';
a(5) := 'e2';
a(6) := 'f2';
insert into tb1 select a(1),a(2),a(3);
insert into tb1 select a(4),a(5),a(6);
end;
end pck1;
/

select * from pck1.proc1();

select * from tb1;

drop package pck1;

-- test for loop
create table emp(
deptno smallint,
ename char(100),
salary int
);

create table emp_back(
deptno smallint,
ename char(100),
salary int
);

insert into emp values (10,'CLARK',7000),(10,'KING',8000),(10,'MILLER',12000),(20,'ADAMS',5000),(20,'FORD',4000);

create or replace PROCEDURE test_forloop_001()
As
begin
for data in delete from emp returning * loop
insert into emp_back values(data.deptno,data.ename,data.salary);
end loop;
end;
/

call test_forloop_001();
select * from emp;
select * from emp_back;
create or replace package pack0 is
    function f1(ss in int) return int;
end pack0;
/
create or replace package body pack0 is
function f1(ss in int) return int as
    va int;
begin
    va := ss;
    return va;
end;
end pack0;
/
create or replace package pack01 is
procedure main();
end pack01;
/
create or replace package body pack01 is
xx number:=dbe_sql.register_context();
yy1 int:=pack0.f1(1);
procedure main() is
yy int;
begin
yy :=pack0.f1(1);
end;
end pack01;
/

create or replace package body pack01 is
xx number:=dbe_sql.register_context();
yy1 int:=pack0.f1(1);
procedure main() is
yy int;
begin
yy :=pack0.f1(1);
end;
end pack01;
/
drop package pack01;
drop package pack0;

drop schema if exists plpgsql_nest_compile cascade;
