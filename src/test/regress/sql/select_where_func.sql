-- check compatibility --
show sql_compatibility; -- expect A --
drop schema if exists sch2;
create schema sch2;
set current_schema = sch2;

create table tbl_1 (a int, b int);
insert into tbl_1 values (1,1);
insert into tbl_1 values (2,2);
insert into tbl_1 values (3,3);
insert into tbl_1 values (4,4);
insert into tbl_1 values (5,5);

create or replace function func_1() returns integer AS $Summary$
declare
begin
return 1;
end;
$Summary$ language plpgsql;

create or replace function func_1(a int) returns integer AS $Summary$
declare
begin
return a;
end;
$Summary$ language plpgsql;

drop schema if exists sch1;
create schema sch1;
create or replace function sch1.func_1() returns integer AS $Summary$
declare
begin
return 4;
end;
$Summary$ language plpgsql;

-- package
create or replace package aa
is
function func_2() return integer;
end aa;
/

create or replace package body aa
is
function func_2 return integer 
is
begin
    return 3;
end;
end aa;
/

select aa.func_2();

create or replace package sch1.aa
is
function func_2() return integer;
end aa;
/

create or replace package body sch1.aa
is
function func_2 return integer 
is
begin
    return 5;
end;
end aa;
/

select sch1.aa.func_2();

select * from tbl_1 where a = func_1();
select * from tbl_1 where a = func_1(2);
select * from tbl_1 where a = func_1;
-- pkg.fun
select * from tbl_1 where a = aa.func_2();
select * from tbl_1 where a = aa.func_2;
-- schma.fun
select * from tbl_1 where a = sch1.func_1();
select * from tbl_1 where a = sch1.func_1;
-- pkg.schma.fun
select * from tbl_1 where a = sch1.aa.func_2();
select * from tbl_1 where a = sch1.aa.func_2;

create or replace package a2
as
v integer := 1;
function func_11() return integer;
end a2;
/

create or replace package body a2
is
function func_11 return integer 
is
  b integer;
begin
    return 1;
end;
end a2;
/

create or replace package a3
is
function func_111() return integer;
end a3;
/

create or replace package body a3
is
function func_111 return integer 
is
	cur sys_refcursor;
    temp integer;
begin
    open cur for
	select a from tbl_1 t where (t.a = a2.v or t.b = 3);
    fetch cur into temp;
	RAISE INFO '%' , temp;
    fetch cur into temp;
	RAISE INFO '%' , temp;
    return 3;
end;
end a3;
/

select a3.func_111();

create table t1(c1 int);
insert into t1 values(1),(2),(3),(4),(5),(6);

create or replace package call_test as
function func1() return int;
function func2(c1 out int) return int;
function func3(c1 int default 3) return int;
procedure proc1();
procedure proc2(c1 out int);
procedure proc3(c1 int default 3, c2 out int);
end call_test;
/
create or replace package body call_test as
function func1() return int as
begin
  return 1;
end;
function func2(c1 out int) return int as
begin
  return 2;
end;
function func3(c1 int default 3) return int as
begin
  return 3;
end;
procedure proc1() as
begin
  raise info 'proc1';
end;
procedure proc2(c1 out int) as
begin
  c1 := 5;
end;
procedure proc3(c1 int default 3, c2 out int) as
begin
  c2 := 6;
end;
end call_test;
/

begin
call_test.proc1;
end;
/

select * from t1 where c1 = call_test.func1;
select * from t1 where c1 = call_test.func2;
select * from t1 where c1 = call_test.func3;
select * from t1 where c1 = call_test.proc1;
select * from t1 where c1 = call_test.proc2;
select * from t1 where c1 = call_test.proc3;

declare
var int;
begin
call_test.func2;
end;
/

declare
var int;
begin
select c1 into var from t1 where c1 = call_test.func1;
raise info 'var is %',var;
select c1 into var from t1 where c1 = call_test.func2;
raise info 'var is %',var;
select c1 into var from t1 where c1 = call_test.func3;
raise info 'var is %',var;
select c1 into var from t1 where c1 = call_test.proc2;
raise info 'var is %',var;
select c1 into var from t1 where c1 = call_test.proc3;
raise info 'var is %',var;
--call call_test.proc1;
end;
/

drop package call_test;
drop package sch1.aa;
drop package aa;
drop package a2;
drop package a3;
drop table tbl_1;
drop function func_1;
drop schema if exists sch1 cascade;
drop schema if exists sch2 cascade;
