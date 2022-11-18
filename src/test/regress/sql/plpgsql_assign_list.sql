-- FOR PL/pgSQL ARRAY of RECORD TYPE scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_assignlist;
create schema plpgsql_assignlist;
set current_schema = plpgsql_assignlist;

-- initialize table and type--
create type o1 as (o1a int, o1b int);
create type o2 as (o2a o1, o2b int);
create type o3 as (o3a o2, o3b int);
create type o4 as (o4a o3, o4b int);
create type o5 as (o5a o2[], o5b int);
create type o6 as (o6a o5, o6b int);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- test assign list without array: nested record
create or replace function get_age RETURNS integer as $$ 
declare
    type r1 is record  (r1a int, r1b int);
    type r2 is record  (r2a r1, r2b int);
    type r3 is record  (r3a r2, r3b int);
	va r3;
begin
	va.r3a.r2a.r1a := 123;
    raise info '%', va;
    va := (((4,3),2),1);
    raise info '%', va;
    va.r3a.r2a.r1a := 456;
    raise info '%', va;
    return va.r3a.r2a.r1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list without array: nested composite type
create or replace function get_age RETURNS integer as $$ 
declare
	va o4;
begin
    va.o4a.o3a.o2a.o1a := 123;
    raise info '%', va;
    va.o4a.o3a.o2a := (456, 789);
    raise info '%', va;
    return va.o4a.o3a.o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
    TYPE o3_arr is VARRAY(10) of o3;
	va o3_arr;
begin
    va(1).o3a.o2a.o1a := 123;
    raise info '%', va;
    va(2).o3a.o2a := (456, 789);
    raise info '%', va;
    va(3).o3a := ((123, 456),789);
    raise info '%', va;
    return va(2).o3a.o2a.o1b;
end;
$$ language plpgsql;
select get_age();


-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
	va o5;
begin
    va.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.o5a(2).o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
	va o6;
begin
    va.o6a.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.o6a.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.o6a.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.o6a.o5a(2).o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: with record nested
create or replace function get_age RETURNS integer as $$ 
declare
	TYPE r1 is RECORD  (r1a int, r1b int);
    TYPE r1_arr is VARRAY(10) of r1;
    TYPE r2 is RECORD (r2a r1_arr);
    va r2;
begin
    va.r2a(1).r1a := 123;
    raise info '%', va.r2a(1).r1a;
    va.r2a(2) := (456, 789);
    raise info '%', va;
    va.r2a(2).r1b := 999;
    raise info '%', va;
    return va.r2a(2).r1b;
end;
$$ language plpgsql;
select get_age();

-- test assign list with table: with record nested
create or replace function get_age RETURNS integer as $$ 
declare
	TYPE r1 is RECORD  (r1a int, r1b int);
    TYPE r1_arr is table of r1 index by varchar2(10);
    TYPE r2 is RECORD (r2a r1_arr);
    va r2;
begin
    va.r2a('a').r1a := 123;
    raise info '%', va.r2a('a').r1a;
    va.r2a('aa') := (456, 789);
    raise info '%', va;
    va.r2a('aa').r1b := 999;
    raise info '%', va;
    return va.r2a('aa').r1b;
end;
$$ language plpgsql;
select get_age();

--test assign list with array: array not in first three word
create or replace function get_age RETURNS integer as $$ 
declare
    TYPE r1 is RECORD (r1a o6, r1b int);
	va r1;
begin
    va.r1a.o6a.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.r1a.o6a.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.r1a.o6a.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.r1a.o6a.o5a[2].o2a.o1a;
end;
$$ language plpgsql;
select get_age();

--test o1.col1.col2 ref
create type ct as (num int,info text);
create type ct1 as (num int,info ct);
create or replace package autonomous_pkg_a IS
count_public ct1 := (1,(1,'a')::ct)::ct1;
function autonomous_f_public(num1 int) return int;
end autonomous_pkg_a;
/
create or replace package body autonomous_pkg_a as
count_private ct1 :=(2,(2,'b')::ct)::ct1;
function autonomous_f_public(num1 int) return int
is
declare
re_int int;
begin
count_public.num = num1 + count_public.num;
count_private.num = num1 + count_private.num;
raise info 'count_public.info.num: %', count_public.info.num;
count_public.info.num = count_public.info.num + num1;
raise info 'count_public.info.num: %', count_public.info.num;
count_private.info.num = count_private.info.num + num1;
re_int = count_public.num +count_private.num;
return re_int;
end;
end autonomous_pkg_a;
/

select autonomous_pkg_a.autonomous_f_public(10);
drop package autonomous_pkg_a;
drop type ct1;
drop type ct;

-- test select into syntax error
declare
va int;
vb varchar2;
vc varchar2;
begin
select 1,'a','b' into va,vb vc;
end;
/


-- test nest cursor loop exception bug
drop table tb1;
create table tb1 (c1 int, c2 varchar(30)) with (storage_type = 'ustore');
insert into tb1 values(1, 'aaa');
insert into tb1 values(2, 'bbb');
insert into tb1 values(3, '123');
insert into tb1 values(4, 'VC00F7101700020D08');
insert into tb1 values(5, '123');
insert into tb1 values(6, 'dqd');

create or replace procedure p2 as
cursor c1 is select * from tb1;
cursor c2 is select * from tb1;
va tb1%rowtype;
vb int;
begin
for rec in c1 loop
  begin
   for rec2 in c2 loop
    commit;
    vb := 1/(rec2.c1 - 3);
   end loop;
  exception when division_by_zero then
   rollback;
  end;
  commit;
end loop;

end;
/

call p2();
drop procedure p2();
drop table tb1;
--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop function if exists get_age();
drop type if exists o6;
drop type if exists o5;
drop type if exists o4;
drop type if exists o3;
drop type if exists o2;
drop type if exists o1;

-- clean up --
drop schema if exists plpgsql_assignlist cascade;
