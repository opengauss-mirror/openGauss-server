-- FOR PL/pgSQL ARRAY of RECORD TYPE scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_packagetype1;
create schema plpgsql_packagetype1;
drop schema if exists plpgsql_packagetype2;
create schema plpgsql_packagetype2;


-- initialize table and type--



----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- test package type internal use
set search_path=plpgsql_packagetype1;
create or replace package p_test1 as
    type t1 is record(c1 varchar2, c2 int);
    type t2 is table of t1;
    function f1(ss in t1) return t1;
    function f2(ss in t2) return t2;
    procedure p1(aa in t1,bb in t2,cc out t1,dd out t2);
	procedure p2(aa in int);
end p_test1;
/
create or replace package p_test1 as
    type t1 is record(c1 varchar2, c2 int);
    type t2 is table of t1;
    function f1(ss in t1) return t1;
    function f2(ss in t2) return t2;
    procedure p1(aa in t1,bb in t2,cc out t1,dd out t2);
        procedure p2(aa in int);
end p_test1;
/
create or replace package body p_test1 as
    vb char;
    function f1(ss in t1) return t1 as
        va t1;
	begin
        va := ss;
        plpgsql_packagetype1.p_test1.vb := '';
        raise info '%',va;
        return va;
	end;

    function f2(ss in t2) return t2 as
        vb t2;
	begin
        vb := ss;
        return vb;
	end;
    
    procedure p1(aa in t1,bb in t2,cc out t1,dd out t2) as
	begin
        cc := aa;
        dd := bb;
        raise info '% %', cc,dd;
    end;
	
    procedure p2(aa in int) as
	    aa t1;
	    bb t2;
	    cc t1;
	    dd t2;
	begin
	    aa := ('a',1);
		bb := array[('b',2),('c',3)];
		p1(aa,bb,cc,dd);
	end;
		
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in t1) return t1 as
        va t1;
        begin
        va := ss;
        raise info '%',va;
        return va;
        end;

    function f2(ss in t2) return t2 as
        vb t2;
        begin
        vb := ss;
        return vb;
        end;

        procedure p1(aa in t1,bb in t2,cc out t1,dd out t2) as
        begin
        cc := aa;
        dd := bb;
        raise info '% %', cc,dd;
    end;

    procedure p2(aa in int) as
            aa t1;
            bb t2;
            cc t1;
            dd t2;
        begin
            aa := ('a',1);
                bb := array[('b',2),('c',3)];
                p1(aa,bb,cc,dd);
        end;

end p_test1;
/

select p_test1.f1(('a',3));
select p_test1.f2(array[('a',1)::p_test1.t1,('b',2)::p_test1.t1,('c',4)::p_test1.t1]);

-- test package type used in another package
create or replace package p_test2 as
    var1 p_test1.t1;
    type t21 is record(c1 p_test1.t1);
    type t22 is table of p_test1.t1;
    function ff1(ss in p_test1.t1) return p_test1.t1;
    procedure pp1(aa in p_test1.t1,bb in p_test1.t2,cc out p_test1.t1,dd out p_test1.t2);
    procedure pp2(aa in int);
end p_test2;
/
create or replace package body p_test2 as
    function ff1(ss in p_test1.t1) return p_test1.t1 as
        va p_test1.t1;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;


	procedure pp1(aa in p_test1.t1,bb in p_test1.t2,cc out p_test1.t1,dd out p_test1.t2) as
	begin
        cc := aa;
        dd := bb;
        raise info '% %', cc,dd;
    end;
	
    procedure pp2(aa in int) as
	    aa p_test1.t1;
	    bb p_test1.t2;
	    cc p_test1.t1;
	    dd p_test1.t2;
	begin
	    aa := ('a',1);
		bb := array[('b',2),('c',3)];
		pp1(aa,bb,cc,dd);
	end;
		
end p_test2;
/

select p_test2.ff1(('a',3));

-- test package type used in another schema package
set search_path=plpgsql_packagetype2;
create or replace package p_test2 as
    var1 plpgsql_packagetype1.p_test1.t1;
    type t21 is record(c1 plpgsql_packagetype1.p_test1.t1);
    type t22 is table of plpgsql_packagetype1.p_test1.t1;
    function ff1(ss in plpgsql_packagetype1.p_test1.t1) return plpgsql_packagetype1.p_test1.t1;
    procedure pp1(aa in plpgsql_packagetype1.p_test1.t1,bb in plpgsql_packagetype1.p_test1.t2,cc out plpgsql_packagetype1.p_test1.t1,dd out plpgsql_packagetype1.p_test1.t2);
end p_test2;
/
create or replace package body p_test2 as
    function ff1(ss in plpgsql_packagetype1.p_test1.t1) return plpgsql_packagetype1.p_test1.t1 as
        va plpgsql_packagetype1.p_test1.t1;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;


	procedure pp1(aa in plpgsql_packagetype1.p_test1.t1,bb in plpgsql_packagetype1.p_test1.t2,cc out plpgsql_packagetype1.p_test1.t1,dd out plpgsql_packagetype1.p_test1.t2) as
	begin
        cc := aa;
        dd := bb;
        raise info '% %', cc,dd;
    end;
	
    procedure pp2(aa in int) as
	    aa plpgsql_packagetype1.p_test1.t1;
	    bb plpgsql_packagetype1.p_test1.t2;
	    cc plpgsql_packagetype1.p_test1.t1;
	    dd plpgsql_packagetype1.p_test1.t2;
	begin
	    aa := ('a',1);
		bb := array[('b',2),('c',3)];
		pp1(aa,bb,cc,dd);
	end;
		
end p_test2;
/

select p_test2.ff1(('a',3));

drop package p_test2;
drop package plpgsql_packagetype1.p_test2;
drop package plpgsql_packagetype1.p_test1;

--test ref cursortype
create or replace package test_cur
IS
type ref_cur is ref cursor;
end test_cur;
/
create or replace package body test_cur
IS
a int;
end test_cur;
/
create or replace package test_cur2
IS
procedure proc1(cur1 test_cur.ref_cur);
end test_cur2;
/
create or replace package body test_cur2
IS
procedure proc1(cur1 test_cur.ref_cur)
is
BEGIN
cur1.a.a:=2;
end;
end test_cur2;
/
create or replace package test_cur3
IS
procedure proc11(cur1 test_cur.ref_cur);
function func11() return test_cur.ref_cur;
end test_cur3;
/
create or replace package body test_cur3
IS
procedure proc11(cur1 test_cur.ref_cur)
is
BEGIN
cur1.a.a:=2;
end;
function func11() return test_cur.ref_cur
is
BEGIN
return 1;
end;
end test_cur3;
/
create or replace package test_cur4
IS
procedure proc111(cur1 test_cur.ref_cur);
function func111 return test_cur.ref_cur;
end test_cur4;
/
create or replace package body test_cur4
IS
procedure proc111(cur1 test_cur.ref_cur)
is
BEGIN
cur1.a.a:=2;
cur.a.a:=2;
end;
function func111() return test_cur.ref_cur
is
BEGIN
return 1;
end;
end test_cur4;
/
drop package if exists test_cur;
drop package if exists test_cur2;
drop package if exists test_cur3;
drop package if exists test_cur4;

create or replace package pck1 is
type t1 is table of varchar2(10);
procedure pp11 (t1 in varchar2(10));
procedure pp22 (t1 out varchar2(10));
end pck1;
/

create or replace package body pck1 is
procedure pp11 (t1 in varchar2(10)) is
begin
raise info '%', t1;
end;
procedure pp22 (t1 out varchar2(10)) is
begin
t1 := 'bb';
raise info '%', t1;
end;
end pck1;
/
call pck1.pp11('aa');
call pck1.pp22('cc');

DROP PACKAGE pck1;

--test package, type with same name
create or replace package plpgsql_packagetype1.pck2 is
va int;
end pck2;
/
create type plpgsql_packagetype1.pck2 as (a int, b int);

DROP table if exists t1;
CREATE table t1 (a plpgsql_packagetype1.pck2);
insert into t1 values((1,2));
select * from t1;

create or replace package pck3 is
va plpgsql_packagetype1.pck2;
end pck3;
/

DROP package pck3;
DROP package plpgsql_packagetype1.pck2;
DROP table t1;
DROP type plpgsql_packagetype1.pck2;

--test synonym name same with procedure itself
create procedure proc1 as
begin
null;
end;
/
create synonym proc1 for proc1;

call proc1();

DROP procedure proc1();

--test package type form record
create or replace package pck1 is
type t1 is record (a int, b int);
type ta is varray(10) of varchar2(100);
type tb is varray(10) of int;
type tc is varray(10) of t1;
type td is table of varchar2(100);
type te is table of int;
type tf is table of t1;
end pck1;
/

create or replace package pck2 is
type tb is record (col1 pck1.ta, col2 pck1.tb, col3 pck1.tc, col4 pck1.td, col5 pck1.te, col6 pck1.tf);
end pck2;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- test not support nested type
-- 1. array nest array
create or replace package pck1 as
type t1 is varray(10) of int;
type t2 is varray(10) of t1;
end pck1;
/

create or replace function func1() return int as
type t1 is varray(10) of int;
type t2 is varray(10) of t1;
begin
return 0;
end;
/
-- 2. table nest array
create or replace package pck1 as
type t1 is varray(10) of int;
type t2 is table of t1;
end pck1;
/

create or replace function func1() return int as
type t1 is varray(10) of int;
type t2 is table of t1;
begin
return 0;
end;
/
-- 3. array nest table
create or replace package pck1 as
type t1 is table of int;
type t2 is varray(10) of t1;
end pck1;
/

create or replace function func1() return int as
type t1 is table of int;
type t2 is varray(10) of t1;
begin
return 0;
end;
/
-- 4. table nest table, will be supported soon
-- create or replace package pck1 as
-- type t1 is table of int;
-- type t2 is table of t1;
-- end pck1;
-- /

-- create or replace function func1() return int as
-- type t1 is table of int;
-- type t2 is table of t1;
-- begin
-- return 0;
-- end;
-- /
-- 5. record nest ref cursor
drop package if exists pck1;
create or replace package pck1 as
type t1 is ref cursor;
type t2 is record(c1 t1,c2 int);
end pck1;
/

create or replace function func1() return int as
type t1 is ref cursor;
type t2 is record(c1 t1,c2 int);
begin
return 0;
end;
/
-- 6. table nest ref cursor
create or replace package pck1 as
type t1 is ref cursor;
type t2 is table of t1;
end pck1;
/

create or replace function func1() return int as
type t1 is ref cursor;
type t2 is table of t1;
begin
return 0;
end;
/
-- 7. varray nest ref cursor
create or replace package pck1 as
type t1 is ref cursor;
type t2 is varray(10) of t1;
end pck1;
/

create or replace function func1() return int as
type t1 is ref cursor;
type t2 is varray(10) of t1;
begin
return 0;
end;
/
DROP package pck1;
DROP function func1();
-- 8.package nest

create or replace package pck1 as
type t1 is table of int;
type t2 is varray(10) of int;
type t3 is ref cursor;
end pck1;
/
create or replace package pck2 as
type t1 is varray(10) of pck1.t2;
v1 t1;
function func1() return int;
end pck2;
/

-- 9.package nested
create or replace package pck2 as
type t1 is table of pck1.t1;
v1 t1;
function func1() return int;
end pck2;
/

-- 10.package nested
create or replace package pck2 as
type t1 is table of pck1.t3;
v1 t1;
function func1() return int;
end pck2;
/

-- 10.package nested
create or replace package pck2 as
type t1 is record(a pck1.t3);
v1 t1;
function func1() return int;
end pck2;
/

DROP package pck2;
DROP package pck1;

-- test type nested by private type
create or replace package pck1 as
type t1 is varray(10) of int;
type t2 is record (a int, b int);
end pck1;
/
create or replace package body pck1 as
type t3 is varray(10) of int;
type t4 is record (a t3, b int);
procedure p2 (a pck1.t2) is
type t5 is varray(10) of t4;
type t6 is varray(10) of t2;
begin
null;
end;
end pck1;
/

DROP PACKAGE pck1;

-- test replace body, do not influence head 
create or replace package pck1 as
type t1 is varray(10) of int;
type t2 is record (a int, b int);
end pck1;
/

create or replace package pck2 as
procedure p1 (a pck1.t1);
end pck2;
/
create or replace package body pck2 as
procedure p1 (a pck1.t1) is
begin
null;
end;
end pck2;
/

create or replace package body pck1 as
procedure p1 (a int) is
begin
null;
end;
end pck1;
/

call pck2.p1(array[1,2,4]);

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- test package table of type
create or replace package pck1 is
type t1 is table of int index by varchar2(10);
end pck1;
/

create or replace package  pck2 is
va pck1.t1;
procedure p1;
end pck2;
/
create or replace package body pck2 is
procedure p1 is
begin
va('aaa') := 1;
va('a') := 2;
raise info '%', va;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- test package record type as procedure in out param
-- (1) in param
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a in r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
va r1;
begin
va := (1,2);
p2(va);
va := (4,5);
p2(va);
end;
procedure p2 (a in r1) as
begin
raise info '%', a;
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;
-- (2) out param
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a out r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
va r1;
begin
raise info '%', va;
p2(va);
raise info '%', va;
end;
procedure p2 (a out r1) as
begin
a.a := 11;
a.b := 22;
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;
-- (3) inout param
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a inout r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
va r1;
begin
va := (11,22);
raise info '%', va;
p2(va);
raise info '%', va;
end;
procedure p2 (a inout r1) as
begin
a.a := a.a + 1;
a.b := a.b + 1;
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;
-- test more column number
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a out r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
type r2 is record (a int, b int, c int);
va r1;
vb r2;
begin
va := (1,2);
vb := (1,2,3);
raise info '%',va;
p2(vb);
raise info '%',va;
end;
procedure p2 (a out r1) as
begin
raise info '%', a;
a := (4,5);
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;
-- test less column number
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a out r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
type r2 is record (a int);
va r1;
vb r2;
begin
va := (1,2);
vb.a := 1;
raise info '%',va;
p2(vb);
raise info '%',va;
end;
procedure p2 (a out r1) as
begin
raise info '%', a;
a := (4,5);
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;
-- test wrong column type
create or replace package pck1 as
type r1 is record (a int, b int);
procedure p1;
procedure p2(a out r1);
end pck1;
/
create or replace package body pck1 as
procedure p1 as
type r2 is record (a int, b varchar2(10));
va r1;
vb r2;
begin
va := (1,2);
vb := (1,'aa');
raise info '%',va;
p2(vb);
raise info '%',va;
end;
procedure p2 (a out r1) as
begin
raise info '%', a;
a := (4,5);
end;
end pck1;
/
call pck1.p1();
DROP PACKAGE pck1;

-- test package type alter
create or replace package pck1 is
type r1 is record (a int, b int);
type r2 is table of int index by varchar(10);
type r3 is varray(10) of int;
end pck1;
/

-- (1) grant or revoke
grant drop on type pck1.r1 to public;
grant alter on type pck1.r2 to public;
grant alter on type pck1.r3 to public;
revoke drop on type pck1.r1 from public;
revoke drop on type pck1.r2 from public;
revoke drop on type pck1.r3 from public;

-- (2) drop
DROP TYPE pck1.r1 cascade;
DROP TYPE pck1.r2 cascade;
DROP TYPE pck1.r3 cascade;

-- (3) alter: rename
ALTER TYPE pck1.r1 RENAME TO o1;
ALTER TYPE pck1.r2 RENAME TO o1;
ALTER TYPE pck1.r3 RENAME TO o1;

-- (4) alter: owner
ALTER TYPE pck1.r1 OWNER TO CURRENT_USER;
ALTER TYPE pck1.r2 OWNER TO CURRENT_USER;
ALTER TYPE pck1.r3 OWNER TO CURRENT_USER;

-- (5) alter: set schema
ALTER TYPE pck1.r1 SET SCHEMA public;
ALTER TYPE pck1.r2 SET SCHEMA public;
ALTER TYPE pck1.r3 SET SCHEMA public;

DROP PACKAGE pck1;

-- test package type as table of type column
create or replace package pck1 is
type r1 is record (a int, b int);
type r2 is table of int index by varchar(10);
type r3 is varray(10) of int;
end pck1;
/

-- (1) as table
create table t1(a pck1.r1);
create table t1(a pck1.r2);
create table t1(a pck1.r3);

-- (2) as type
create type o1 as (a pck1.r1);
create type o1 as (a pck1.r2);
create type o1 as (a pck1.r3);

-- (3) in procedure
create or replace procedure p1 as
begin
create table t1(a pck1.r1);
end;
/
call p1();

create or replace procedure p1 as
begin
create table t1(a pck1.r2);
end;
/
call p1();

create or replace procedure p1 as
begin
create table t1(a pck1.r3);
end;
/
call p1();

create or replace procedure p1 as
begin
create type o1 as (a pck1.r1);
end;
/
call p1();

create or replace procedure p1 as
begin
create type o1 as (a pck1.r2);
end;
/
call p1();

create or replace procedure p1 as
begin
create type o1 as (a pck1.r3);
end;
/
call p1();

DROP procedure p1;
DROP package pck1;

-- test package-depended type clean

create or replace package pck4 is
va int;
end pck4;
/

create or replace package body pck4 is
type test_pck4_recordtype is record (a int, b int);
type test_pck4_arraytype is varray(10) of int;
type test_pck4_tableoftype is table of int index by varchar2(10);
type test_pck4_refcursor is ref cursor;
end pck4;
/

select count(*) from pg_type where typname like '%.test_pck4%';
select count(*) from PG_SYNONYM where synname like '%.test_pck4%';

create or replace package body pck4 is
vb int;
end pck4;
/

select count(*) from pg_type where typname like '%.test_pck4%';
select count(*) from PG_SYNONYM where synname like '%.test_pck4%';

DROP PACKAGE pck4;

-- test table of record index by varchar
create or replace package pkg045
is
type type000 is record(c1 number(7,3),c2 varchar2(30));
type type001 is table of type000 index by varchar2(30);
function proc045_2(col1 int) return int;
end pkg045;
/

create or replace package body pkg045
is
function proc045_2(col1 int) return int
is
b2 pkg045.type001;
begin
b2('1').c1:=1.000;
b2('1').c2:='aaa';
raise info '%,%', b2('1').c1, b2('1').c2;
return 1;
end;
end pkg045;
/

call pkg045.proc045_2(1);
drop package pkg045;

-- test alter package/function owner
-- (a) alter package
reset search_path;
create user alt_user_1 PASSWORD 'gauss@123';
create user alt_user_2 PASSWORD 'gauss@123';

SET SESSION AUTHORIZATION alt_user_1 password 'gauss@123';
drop package if exists pck1;

create or replace package pck1 as
type tt1 is record(c1 int,c2 int);
type tt2 is table of tt1;
type tt3 is varray(10) of tt1;
function func1()return int;
procedure proc1();
end pck1;
/

create or replace package body pck1 as
function func1()return int as
type tt5 is record(c1 tt1,c2 int);
type tt6 is table of pck1.tt1;
type tt7 is varray(10) of pck1.tt1;
type tt8 is ref cursor;
type tt9 is table of tt5;
begin
return 0;
end;
procedure proc1() as
type tta is record(c1 tt1,c2 int);
type ttb is table of pck1.tt1;
type ttc is varray(10) of pck1.tt1;
type ttd is ref cursor;
type tte is table of tta;
begin
null;
end;
end pck1;
/

reset session AUTHORIZATION;
alter package alt_user_1.pck1 owner to alt_user_2;
------usename user2
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.tt1%' limit 1);
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.tt2%' limit 1);
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.tt3%' limit 1);
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.tt5%' limit 1);
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.tta%' limit 1);

drop package alt_user_1.pck1;

-- (b) alter function
SET SESSION AUTHORIZATION alt_user_1 password 'gauss@123';
create or replace function func1()return int as
type ttt5 is record(c1 int,c2 int);
type ttt6 is table of int;
type ttt7 is varray(10) of int;
type ttt8 is ref cursor;
type ttt9 is table of ttt5;
begin
return 0;
end;
/
reset session AUTHORIZATION;
alter function alt_user_1.func1() owner to alt_user_2;
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.ttt5%' limit 1);
select usename from pg_user where usesysid = (select typowner from pg_type where typname like '%.ttt6%' limit 1);

drop function alt_user_1.func1();
drop user alt_user_1 cascade;
drop user alt_user_2 cascade;


--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop package p_test2;
drop package plpgsql_packagetype1.p_test2;
drop package plpgsql_packagetype1.p_test1;

-- clean up --
drop schema if exists plpgsql_packagetype2 cascade;
drop schema if exists plpgsql_packagetype1 cascade;
