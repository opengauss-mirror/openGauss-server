drop schema if exists pkg_val_1 cascade;
drop schema if exists pkg_val_2 cascade;

create schema pkg_val_1;
create schema pkg_val_2;

set current_schema = pkg_val_2;
set behavior_compat_options='allow_procedure_compile_check';

--test package val assign
create or replace package pck1 is
type r1 is record (a int, b int);
type r2 is varray(10) of int;
type r3 is table of int;
type r4 is record (a r2);
va r1;
vb r2;
vc r3;
vd int;
vf r4;
end pck1;
/
create or replace package body pck1 is
end pck1;
/

create or replace package pck2 is
ve int;
procedure p1;
end pck2;
/
create or replace package body pck2  is
procedure p1 as 
begin
pck1.va := (1,2);
pck1.va := (3,4);
pck1.va.a := 5;
pck1.va.a := pck1.va.a + 1;
pck1.vb(1) := 1;
pck1.vb(2) := 2 + pck1.vb(1);
pck1.vc(1) := 1;
pck1.vc(2) := 2 + pck1.vc(1);
pck1.vd := 4;
pck1.vd := 5;
pck1.vf.a(1) := 1;
pck1.vf.a(2) := 1 + pck1.vf.a(1);
pck2.ve := 6;
pck2.ve := 7;
raise info '%, %, %, %, %, %', pck1.va, pck1.vb, pck1.vc, pck1.vd, pck1.vf, ve;
end;
end pck2;
/
call pck2.p1();

create or replace package body pck2  is
procedure p1 as 
begin
select 11,22 into pck1.va;
select 33 into pck1.va.a;
select 11 into pck1.vb(1);
select 22 into pck1.vb(2);
select 33 into pck1.vc(1);
select 44 into pck1.vc(2);
select 55 into pck1.vd;
select 66 into pck1.vd;
select 77 into pck1.vf.a(1);
select 77 into pck2.ve;
select 88 into pck2.ve;
raise info '%, %, %, %, %,%', pck1.va, pck1.vb, pck1.vc, pck1.vd, pck1.vf, ve;
end;
end pck2;
/
call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;

--test 跨schema.pkg.val 
create or replace package pkg_val_1.pck1 is
type r1 is record (a int, b int);
type r2 is varray(10) of int;
type r3 is table of int;
type r4 is record (a r2);
va r1;
vb r2;
vc r3;
vd int;
vf r4;
end pck1;
/
create or replace package body pkg_val_1.pck1 is
end pck1;
/

create or replace package pck2 is
ve int;
procedure p1;
end pck2;
/
create or replace package body pck2  is
procedure p1 as 
begin
pkg_val_1.pck1.va := (1,2);
pkg_val_1.pck1.va := (3,4);
pkg_val_1.pck1.va.a := 5;
pkg_val_1.pck1.vb(1) := 1;
pkg_val_1.pck1.vb(2) := pkg_val_1.pck1.vb(1) + 11;
pkg_val_1.pck1.vc(1) := 1;
pkg_val_1.pck1.vc(2) := 2 + pkg_val_1.pck1.vc(1);
pkg_val_1.pck1.vd := 4;
pkg_val_1.pck1.vd := 5;
pkg_val_1.pck1.vf.a(1) := 11;
pkg_val_2.pck2.ve := 6;
pkg_val_2.pck2.ve := 7;
raise info '%, %, %, %, %, %', pkg_val_1.pck1.va, pkg_val_1.pck1.vb, pkg_val_1.pck1.vc, pkg_val_1.pck1.vd, pkg_val_1.pck1.vf, ve;
end;
end pck2;
/
call pck2.p1();

create or replace package body pck2  is
procedure p1 as 
begin
select 11,22 into pkg_val_1.pck1.va;
--select 33 into pkg_val_1.pck1.va.a; not support now
select 11 into pkg_val_1.pck1.vb(1);
select 22 into pkg_val_1.pck1.vb(2);
select 33 into pkg_val_1.pck1.vc(1);
select 44 into pkg_val_1.pck1.vc(2);
select 55 into pkg_val_1.pck1.vd;
select 66 into pkg_val_1.pck1.vd;
select 77 into pkg_val_2.pck2.ve;
select 88 into pkg_val_2.pck2.ve;
raise info '%, %, %, %, %', pkg_val_1.pck1.va, pkg_val_1.pck1.vb, pkg_val_1.pck1.vc, pkg_val_1.pck1.vd, ve;
end;
end pck2;
/
call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pkg_val_1.pck1;

--test pkg.array.extend
create or replace package pck1 is
type ta is varray(10) of varchar(100);
tb ta;
end pck1;
/

create or replace package pck2 is
procedure proc1;
end pck2;
/
create or replace package body pck2 is
procedure proc1() is
begin
pck1.tb.delete;
end;
end pck2;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

--test 跨packgae cursor
DROP TABLE if exists test_1;
create table test_1(a int, b int);
insert into test_1 values(11,22);

create or replace package pck1 is
cursor c1 is select * from test_1;
end pck1;
/

create or replace package pck2 is
procedure p1;
end pck2;
/
create or replace package body pck2 is
procedure p1 as
type r1 is record (a int, b int);
va r1;
begin
open pck1.c1;
fetch pck1.c1 into va;
raise info '%',va;
end;
end pck2;
/
call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;
DROP TABLE test_1;

--test pkg.row.col引用
create or replace package pck1 is
type r1 is record  (a int, b int);
va r1;
end pck1;
/

create or replace package pck2 is
procedure p1;
end pck2;
/
create or replace package body pck2  is
procedure p1 as 
begin
pck1.va.a := 1;
pck1.va := (1,2);
pck1.va.a := pck1.va.b + 1;
pck1.va.a := pck1.va.a + 1;
raise info '%,', pck1.va;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;

--test table var index by varchar2
create or replace package pck1_zjc is
    TYPE SalTabTyp is TABLE OF integer index by varchar(10);
	aa SalTabTyp;
end pck1_zjc;
/
declare 
	a integer;
begin
	pck1_zjc.aa('a') = 1;
	pck1_zjc.aa('b') = 2;
	pck1_zjc.aa('c') = pck1_zjc.aa('a') + pck1_zjc.aa('b');
	RAISE INFO '%', pck1_zjc.aa;   
end;
/

DROP PACKAGE pck1_zjc;

--test table var index by varchar2 with different schema 
create or replace package pkg_val_1.pck1_zjc is
    TYPE SalTabTyp is TABLE OF integer index by varchar(10);
	aa SalTabTyp;
end pck1_zjc;
/
declare 
	a integer;
begin
	pkg_val_1.pck1_zjc.aa('a') = 1;
	pkg_val_1.pck1_zjc.aa('b') = 2;
	pkg_val_1.pck1_zjc.aa('c') = pkg_val_1.pck1_zjc.aa('a') + pkg_val_1.pck1_zjc.aa('b');
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;   
end;
/

DROP PACKAGE pkg_val_1.pck1_zjc;

--test for table of multiset
create or replace package pck1_zjc is
    TYPE SalTabTyp is TABLE OF integer;
	aa SalTabTyp;
	bb SalTabTyp;
end pck1_zjc;
/

declare 
	a integer;
begin
	pck1_zjc.aa(0) = 1;
	pck1_zjc.aa(2) = 2;
	pck1_zjc.bb(0) = 2;
	pck1_zjc.bb(1) = NULL;
	pck1_zjc.aa = pck1_zjc.aa multiset union pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa = pck1_zjc.aa multiset union distinct pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa = pck1_zjc.aa multiset intersect pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa = pck1_zjc.aa multiset intersect distinct pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa = pck1_zjc.aa multiset except pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa = pck1_zjc.aa multiset except distinct pck1_zjc.bb;
	RAISE INFO '%', pck1_zjc.aa;   
end;
/

DROP package pck1_zjc;

--test for table of multiset：record of table
create or replace package pck1_zjc is
    TYPE SalTabTyp is TABLE OF integer;
    TYPE r1 is record (a SalTabTyp);
	aa r1;
	bb r1;
end pck1_zjc;
/

declare 
	a integer;
 begin
	pck1_zjc.aa.a(0) = 1;
	pck1_zjc.aa.a(2) = 2;
	pck1_zjc.bb.a(0) = 2;
	pck1_zjc.bb.a(1) = NULL;
	pck1_zjc.aa.a = pck1_zjc.aa.a multiset union pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa;
    pck1_zjc.aa.a = pck1_zjc.aa.a multiset union distinct pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa.a;
    pck1_zjc.aa.a = pck1_zjc.aa.a multiset intersect pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa.a;
    pck1_zjc.aa.a = pck1_zjc.aa.a multiset intersect distinct pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa.a;
    pck1_zjc.aa.a = pck1_zjc.aa.a multiset except pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa.a;
    pck1_zjc.aa.a = pck1_zjc.aa.a multiset except distinct pck1_zjc.bb.a;
	RAISE INFO '%', pck1_zjc.aa.a; 
end;
/

DROP package pck1_zjc;

--test for table of multiset：跨schema
create or replace package pkg_val_1.pck1_zjc is
    TYPE SalTabTyp is TABLE OF integer;
	aa SalTabTyp;
	bb SalTabTyp;
end pck1_zjc;
/

declare 
	a integer;
 begin
	pkg_val_1.pck1_zjc.aa(0) = 1;
	pkg_val_1.pck1_zjc.aa(2) = 2;
	pkg_val_1.pck1_zjc.bb(0) = 2;
	pkg_val_1.pck1_zjc.bb(1) = NULL;
	pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset union pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;
    pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset union distinct pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;
    pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset intersect pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;
    pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset intersect distinct pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;
    pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset except pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;
    pkg_val_1.pck1_zjc.aa = pkg_val_1.pck1_zjc.aa multiset except distinct pkg_val_1.pck1_zjc.bb;
	RAISE INFO '%', pkg_val_1.pck1_zjc.aa;   
end;
/

DROP package pkg_val_1.pck1_zjc;

--test record of table
declare 
	TYPE SalTabTyp is TABLE OF integer;
    TYPE r1 is record (a SalTabTyp);
	aa r1;
	bb r1;
 begin
	aa.a(0) = 1;
	aa.a(2) = 2;
	bb.a(0) = 2;
	bb.a(1) = NULL;
	aa.a = aa.a multiset union bb.a;
	RAISE INFO '%', aa;
    aa.a = aa.a multiset union distinct bb.a;
	RAISE INFO '%', aa.a;
    aa.a = aa.a multiset intersect bb.a;
	RAISE INFO '%', aa.a;
    aa.a = aa.a multiset intersect distinct bb.a;
	RAISE INFO '%', aa.a;
    aa.a = aa.a multiset except bb.a;
	RAISE INFO '%', aa.a;
    aa.a = aa.a multiset except distinct bb.a;
	RAISE INFO '%', aa.a; 
end;
/

--test record of record of table : not support yet
-- create or replace procedure pro1 is
-- 	TYPE SalTabTyp is TABLE OF integer;
--     TYPE r1 is record (a SalTabTyp);
--     TYPE r2 is record (a r1);
-- 	aa r2;
-- 	bb r2;
--  begin
-- 	aa.a.a(0) = 1;
-- 	aa.a.a(2) = 2;
-- 	bb.a.a(0) = 2;
-- 	bb.a.a(1) = NULL;
-- 	aa.a.a = aa.a.a multiset union bb.a.a;
-- 	RAISE INFO '%', aa.a.a;
--     aa.a.a = aa.a.a multiset union distinct bb.a.a;
-- 	RAISE INFO '%', aa.a.a;
--     aa.a.a = aa.a.a multiset intersect bb.a.a;
-- 	RAISE INFO '%', aa.a.a;
--     aa.a.a = aa.a.a multiset intersect distinct bb.a.a;
-- 	RAISE INFO '%', aa.a.a;
--     aa.a.a = aa.a.a multiset except bb.a.a;
-- 	RAISE INFO '%', aa.a.a;
--     aa.a.a = aa.a.a multiset except distinct bb.a.a;
-- 	RAISE INFO '%', aa.a.a; 
-- end;
-- /

--test package constant variable
create or replace package pck1 is
    va constant int := 1;
end pck1;
/

declare
vb int;
begin
vb := 2;
pck1.va := vb;
end;
/
DROP package pck1;

--test error message when not found variable
create or replace package pck1 is
    va constant int := 1;
end pck1;
/

declare
vb int;
begin
vb := 2;
pck1.vb := vb;
end;
/

declare
vb int;
begin
vb := 2;
pck2.vb := vb;
end;
/

DROP package pck1;

--test 嵌套引用package变量
create or replace package pck1 is
type a is record(a1 varchar2);
type b is record(b1 a,b2 varchar2);
vb b;
end pck1;
/
--2.跨包record类型嵌套
create or replace package pck2 is
procedure proc1();
end pck2;
/
create or replace package body pck2 is
procedure proc1() as
P1 varchar2;
begin
pck1.vb.b1.a1 :='abc';
P1 :=pck1.vb.b1.a1;
raise info '%', P1;
end;
end pck2;
/

call pck2.proc1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;
--test procedure param duplicate with package
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(r1 int, r2 int, r3 int, r4 int, ve int);
end pck1;
/
DROP package pck1;

--test procedure var duplicate with package public
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int);
	procedure p2(a int);
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is 
	va int;
	vb int;
	vc int;
	vd int;
	ve int;
	vf int;
	r1 int;
	r2 int;
	r3 int;
	r4 int;
	begin
	va := a;
	vb := va + 1;
	vc := vb + 1;
	vd := vc + 1;
	ve := vd + 1;
	vf := ve + 1;
	r1 := ve + 1;
	r2 := r1 + 1;
	r3 := r2 + 1;
	r4 := r3 + 1;
    raise info '%, %, %, %, %, %, %, %, %, %', va,vb,vc,vd,ve,vf,r1,r2,r3,r4;
	end;

	procedure p2(a int) is
	val1 r1;
	val2 r2;
	val3 r3;
	begin
	va := (1 , 2);
	vb := array[3,4,5];
	vc := array[7,8,9];
	val1 := va;
	val2 := vb;
	val3 := vc;
	raise info '%, %, %, %, %, %', va,vb,vc,val1,val2,val3;
	end;
end pck1;
/

call pck1.p1(10);
call pck1.p2(10);
DROP package pck1;

--test procedure var duplicate with package private
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	procedure p1(a int);
	procedure p2(a int);
end pck1;
/

create or replace package body pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int) is 
	va int;
	vb int;
	vc int;
	vd int;
	ve int;
	vf int;
	r1 int;
	r2 int;
	r3 int;
	r4 int;
	begin
	va := a;
	vb := va + 1;
	vc := vb + 1;
	vd := vc + 1;
	ve := vd + 1;
	vf := ve + 1;
	r1 := ve + 1;
	r2 := r1 + 1;
	r3 := r2 + 1;
	r4 := r3 + 1;
    raise info '%, %, %, %, %, %, %, %, %, %', va,vb,vc,vd,ve,vf,r1,r2,r3,r4;
	end;

	procedure p2(a int) is
	val1 r1;
	val2 r2;
	val3 r3;
	begin
	va := (1 , 2);
	vb := array[3,4,5];
	vc := array[7,8,9];
	val1 := va;
	val2 := vb;
	val3 := vc;
	raise info '%, %, %, %, %, %', va,vb,vc,val1,val2,val3;
	end;
end pck1;
/

call pck1.p1(10);
call pck1.p2(10);
DROP package pck1;

--test procedure type duplicate with package public
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int);
	procedure p2(a int);
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is 
    type r1 is record (a int := 1, b int := 2);
	type r2 is record (a int := 3, b int := 4);
	type r3 is record (a int := 7, b int := 6);
	type r4 is record (a int := 9, b int := 8);
	type va is record (a int := 11, b int := 10);
	type vb is record (a int := 13, b int := 12);
	type vc is record (a int := 15, b int := 14);
	type vd is record (a int := 17, b int := 16);
	type ve is record (a int := 19, b int := 18);
	type vf is record (a int := 21, b int := 20);
	val1 r1;
	val2 r2;
	val3 r3;
	val4 r4;
	val5 va;
	val6 vb;
	val7 vc;
	val8 vd;
	val9 ve;
	val10 vf;
	begin
    raise info '%, %, %, %, %, %, %, %, %, %', val1,val2,val3,val4,val5,val6,val7,val8,val9,val10;
	end;

	procedure p2(a int) is
	val1 r1;
	val2 r2;
	val3 r3;
	begin
	va := (1 , 2);
	vb := array[3,4,5];
	vc := array[7,8,9];
	val1 := va;
	val2 := vb;
	val3 := vc;
	raise info '%, %, %, %, %, %', va,vb,vc,val1,val2,val3;
	end;
end pck1;
/

call pck1.p1(10);
call pck1.p2(10);
DROP package pck1;

--test procedure type duplicate with package private
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	procedure p1(a int);
	procedure p2(a int);
end pck1;
/

create or replace package body pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int) is 
    type r1 is record (a int := 1, b int := 2);
	type r2 is record (a int := 3, b int := 4);
	type r3 is record (a int := 7, b int := 6);
	type r4 is record (a int := 9, b int := 8);
	type va is record (a int := 11, b int := 10);
	type vb is record (a int := 13, b int := 12);
	type vc is record (a int := 15, b int := 14);
	type vd is record (a int := 17, b int := 16);
	type ve is record (a int := 19, b int := 18);
	type vf is record (a int := 21, b int := 20);
	val1 r1;
	val2 r2;
	val3 r3;
	val4 r4;
	val5 va;
	val6 vb;
	val7 vc;
	val8 vd;
	val9 ve;
	val10 vf;
	begin
    raise info '%, %, %, %, %, %, %, %, %, %', val1,val2,val3,val4,val5,val6,val7,val8,val9,val10;
	end;

	procedure p2(a int) is
	val1 r1;
	val2 r2;
	val3 r3;
	begin
	va := (1 , 2);
	vb := array[3,4,5];
	vc := array[7,8,9];
	val1 := va;
	val2 := vb;
	val3 := vc;
	raise info '%, %, %, %, %, %', va,vb,vc,val1,val2,val3;
	end;
end pck1;
/

call pck1.p1(10);
call pck1.p2(10);
DROP package pck1;

--test public var duplicated with private var
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int);
end pck1;
/

create or replace package body pck1 is
    va int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    vb int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    ve int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    vf int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    r1 int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    r2 int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    r3 int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    r4 int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    TYPE va is table of int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    TYPE r2 is table of int;
	procedure p1(a int) is 
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
    va int;
	procedure p1(a int) is
	va int;
	begin
    NULL;
	end;
end pck1;
/

DROP package pck1;

--test procedure dulpicalte wite itself
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int);
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	va int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	vb int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r1 int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r2 int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r4 int;
	begin
    NULL;
	end;
end pck1;
/

DROP package pck1;

--test procedure duplicate
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	va int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	vb int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
        vd r4;
	vf int;
	cursor ve is select * from test_t1;
	vd int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	ve int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r1 int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r2 int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r3 int;
begin
    NULL;
end;
/

create or replace procedure pro1(a int) is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	r4 int;
begin
    NULL;
end;
/

DROP procedure pro1(int);
DROP table test_t1;

-- test use type before define
DROP TABLE if exists test_t1;
create table test_t1(a int, b int);
create or replace package pck1 is
	type r1 is record (a int, b int);
	type r2 is varray(10) of int;
	type r3 is table of int;
	type r4 is ref cursor;
    va r1;
	vb r2;
	vc r3;
	vf int;
	cursor ve is select * from test_t1;
	procedure p1(a int);
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
    va r1;
	r1 int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
    va r1;
	type r1 is record (a int, b int);
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
    va r2;
	r2 int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
    va r3;
	r3 int;
	begin
    NULL;
	end;
end pck1;
/

create or replace package body pck1 is
	procedure p1(a int) is
    va r4;
	r4 int;
	begin
    NULL;
	end;
end pck1;
/

DROP package pck1;
DROP table test_t1;

-- test duplicated with used package variable
create or replace package pck1 is
TYPE r1 is record (a int, c varchar2(10), d int);
TYPE r2 is varray(10) of int;
va r1;
vb r2;
vc int;
procedure p1;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
vd int := va;
va int;
begin
end;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
vd r2 := vb;
vb int;
begin
end;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
vd int := vc;
vc int;
begin
end;
end pck1;
/

DROP PACKAGE pck1;

--test var name same with type name
create type o1 as (a int, b int);
create or replace package pck1 is
o1 o1;
end pck1;
/

create or replace package pck1 is
type o1 is varray(10) of o1;
end pck1;
/

create or replace package pck1 is
type o1 is table of o1;
end pck1;
/

create or replace package pck1 is
TYPE r1 is record (a int, c varchar2(10), d int);
TYPE r2 is varray(10) of int;
TYPE r3 is table of int;
procedure p1;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
r1 r1;
begin
end;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
r2 r2;
begin
end;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
r3 r3;
begin
end;
end pck1;
/

create or replace package body pck1 is
procedure p1  is
r2 r3;
begin
end;
end pck1;
/

create or replace procedure pp1 is
type r1 is record (a int, b int);
r1 r1;
begin
null;
end;
/

create or replace procedure pp1 is
type r1 is varray(10) of int;
r1 r1;
begin
null;
end;
/

create or replace procedure pp1 is
type r1 is table of int;
r1 r1;
begin
null;
end;
/

create or replace procedure pp1 is
type r1 is table of int;
r2 r1;
begin
null;
end;
/

DROP procedure pp1;
DROP PACKAGE pck1;
DROP TYPE o1 cascade;

-- test row.col%TYPE as procedure param type
-- (1) va.a%TYPE
create or replace package pck2 is
type ta is record(a int, b int);
va ta;
procedure p1(v1 in va.a%type);
end pck2;
/

create or replace package body pck2 is
procedure p1(v1 in va.a%type) is
begin
raise info '%', v1;
end;
end pck2;
/

call pck2.p1(11);
DROP package pck2;

--(2) private va.a%TYPE
create or replace package pck2 is
type ta is record(a int, b int);
procedure p1;
end pck2;
/

create or replace package body pck2 is
va ta;

procedure p2(v1 in va.a%type) is
begin
raise info '%', v1;
end;

procedure p1 is
begin
p2(11);
end;

end pck2;
/

call pck2.p1();
DROP package pck2;

-- (3) pck.va.a%TYPE
create or replace package pck1 is
type ta is record (a int);
va ta;
end pck1;
/

create or replace package pck2 is
type ta is record(a int, b int);
procedure p1(v1 in pck1.va.a%type);
end pck2;
/

create or replace package body pck2 is
procedure p1(v1 in pck1.va.a%type) is
begin
raise info '%', v1;
end;
end pck2;
/

call pck2.p1(11);
DROP package pck2;
DROP package pck1;

-- (4) schema.pkg.row.col%TYPE
create or replace package pkg_val_1.pck1 is
type ta is record (a int);
va ta;
end pck1;
/

create or replace package pck2 is
type ta is record(a int, b int);
procedure p1(v1 in pkg_val_1.pck1.va.a%type);
end pck2;
/

create or replace package body pck2 is
procedure p1(v1 in pkg_val_1.pck1.va.a%type) is
begin
raise info '%', v1;
end;
end pck2;
/

call pck2.p1(11);
DROP package pck2;
DROP package pkg_val_1.pck1;

--test pkg.val.col%TYPE
create or replace package pck1 as
type t1 is record(c1 int,c2 int);
va t1;
end pck1;
/

create or replace package pck2 as
vb pck1.va.c1%type;
end pck2;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- not allow declare ref cursor type variable in package
drop package if exists pck1;
create or replace package pck1 as
type t1 is ref cursor;
v1 t1;
end pck1;
/
create or replace package pck1 as
type t1 is ref cursor;
-- v1 t1;
end pck1;
/
create or replace package body pck1 as
type t2 is ref cursor;
v1 t2;
end pck1;
/

drop package if exists pck1;
create or replace package pck1 as
type t3 is ref cursor;
v3 pkg_val_2.pck1.t3;
end pck1;
/
create or replace package pck1 as
type t3 is ref cursor;
-- v3 pkg_val_2.pck1.t3;
end pck1;
/                                                                                                                                                                                                                                                
create or replace package body pck1 as
type t4 is ref cursor;
v4 pkg_val_2.pck1.t4;
end pck1;      
/  
drop package if exists pck1;

-- test select into error in for in loop condition
create table tab1(a int,b int);
create or replace package pck1 is
procedure p1;
end pck1;
/
create or replace package body pck1 is
procedure p1 is
v1 tab1%rowtype;
begin
for rec in (select a,b into v1 from tab1) loop
end loop;
end;
end pck1;
/

create or replace package pck1 as
func int;
function func() return int;
end pck1;
/

create or replace package pck1 as
func int;
procedure func();
end pck1;
/

create or replace package pck1 as
func constant int;
procedure func();
end pck1;
/

create or replace package pck1 as
type arr is varray(10) of int;
func arr;
procedure func();
end pck1;
/

DROP PACKAGE pck1;
DROP TABLE tab1;
-- test 4 word parse now
-- (1) schema.pkg.row.col
create or replace package pkg_val_1.pck1 is
type r1 is record (a int, b int);
va r1;
end pck1;
/

create or replace package pck2 is
procedure p1();
end pck2;
/
create or replace package body pck2 is
procedure p1() is
v1 int;
begin
select 1 into pkg_val_1.pck1.va.a;
v1 := pkg_val_1.pck1.va.a;
pkg_val_1.pck1.va.b := 11;
raise info '%, %', v1, pkg_val_1.pck1.va;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pkg_val_1.pck1;

-- (2) schema.pkg.array.next
create or replace package pkg_val_1.pck1 is
type t1 is varray(10) of int;
va t1;
type t2 is table of int index by varchar2(10);
vb t2;
end pck1;
/

create or replace package pck2 is
procedure p1();
end pck2;
/
create or replace package body pck2 is
procedure p1() is
begin
pkg_val_1.pck1.va.extend(9);
pkg_val_1.pck1.va(1) := 111;
raise NOTICE '%',pkg_val_1.pck1.va.first;
raise NOTICE '%',pkg_val_1.pck1.va.count();
pkg_val_1.pck1.va.delete;
raise NOTICE '%',pkg_val_1.pck1.va.first;
raise NOTICE '%',pkg_val_1.pck1.va.count();

pkg_val_1.pck1.vb('aa') := 222;
raise NOTICE '%',pkg_val_1.pck1.vb.first;
raise NOTICE '%',pkg_val_1.pck1.vb.count();
pkg_val_1.pck1.vb.delete;
raise NOTICE '%',pkg_val_1.pck1.vb.first;
raise NOTICE '%',pkg_val_1.pck1.vb.count();

end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pkg_val_1.pck1;

-- (3) pkg.row.col1.col2
create or replace package pck1 is
type r1 is record (a int, b int);
type r2 is record (a r1);
va r2;
end pck1;
/

create or replace package pck2 is
procedure p1();
end pck2;
/
create or replace package body pck2 is
procedure p1() is
v1 int;
begin
select 1 into pck1.va.a.a;
v1 := pck1.va.a.a;
pck1.va.a.b := 11;
raise info '%, %', v1, pck1.va.a;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- (4) pkg.row.col.extend
create or replace package pck1 is
type t1 is varray(10) of int;
type t2 is table of int index by varchar2(10);
type r1 is record(a t1, b t2);
va r1;
end pck1;
/

create or replace package pck2 is
procedure p1();
end pck2;
/
create or replace package body pck2 is
procedure p1() is
begin
pck1.va.a.extend(9);
pck1.va.a(1) := 111;
raise NOTICE '%',pck1.va.a.first;
raise NOTICE '%',pck1.va.a.count();
pck1.va.a.delete;
raise NOTICE '%',pck1.va.a.first;
raise NOTICE '%',pck1.va.a.count();

pck1.va.b('aa') := 222;
raise NOTICE '%', pck1.va.b.first;
raise NOTICE '%', pck1.va.b.count();
pck1.va.b.delete;
raise NOTICE '%', pck1.va.b.first;
raise NOTICE '%', pck1.va.b.count();

end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- (5) row.col1.col2.col3
create or replace package pck2 is
procedure p1();
end pck2;
/
create or replace package body pck2 is
procedure p1() is
TYPE r1 is record (a int, b int);
TYPE r2 is record (a r1, b int);
TYPE r3 is record (a r2, b int);
v1 int;
va r3;
begin
select 1 into va.a.a.a;
v1 := va.a.a.a;
va.a.a.b := 11;
raise info '%, %', v1, va.a.a;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;

--test package variable as default value
create or replace package pck1 as
type t1 is record(c1 int,c2 varchar2);
v1 t1 := (1,'a');
procedure p1;
end pck1;
/

create or replace package body pck1 as
procedure p1 is
begin
v1 := (2, 'b');
end;
end pck1;
/

create or replace package pck2 as
v2 int := pck1.v1.c1;
end pck2;
/

declare
a int;
begin
a := pck2.v2;
raise info '%', a;
a := pck1.v1.c1;
raise info '%', a;
pck1.p1();
a := pck2.v2;
raise info '%', a;
a := pck1.v1.c1;
raise info '%', a;
end;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

--test package self with schema
create table pkg_val_2.t1(a int, b int);
create or replace package  pkg_val_2.pck2 is
va int;
vb int;
procedure p1;
end pck2;
/
create or replace package body pkg_val_2.pck2 is
procedure p1 is
cursor cur1 is select * from t1 where a between pkg_val_2.pck2.va and pkg_val_2.pck2.vb;
begin
pkg_val_2.pck2.va := 1;
raise info '%', pkg_val_2.pck2.va;
end;
end pck2;
/

call pkg_val_2.pck2.p1();

DROP PACKAGE pkg_val_2.pck2;
DROP TABLE pkg_val_2.t1;

-- test package duplicate name
create or replace package pkg_val_1.pckg_test1 as
var varchar2 :='abc';
var2 int :=4;
procedure p1(c1 int,c2 out varchar2);
end pckg_test1;
/
create or replace package body pkg_val_1.pckg_test1 as
procedure p1(c1 int,c2 out varchar2) as
begin
c2 :=var||c1;
end;
end pckg_test1;
/

create or replace package pkg_val_2.pckg_test1 as
var2 varchar2;
procedure p1();
end pckg_test1;
/
create or replace package body pkg_val_2.pckg_test1 as
procedure p1() as
begin
pkg_val_1.pckg_test1.p1(pkg_val_1.pckg_test1.var2,var2);
raise info 'var2:%' ,var2;
end;
end pckg_test1;
/
call pkg_val_2.pckg_test1.p1();

DROP PACKAGE pkg_val_2.pckg_test1;
DROP PACKAGE pkg_val_1.pckg_test1;

-- 1. package variable as => out param
drop package if exists pkg_val_1.pckg_test1;
create or replace package pkg_val_1.pckg_test1 as
var varchar2 :='abc';
procedure p1(c1 int,c2 out varchar2);
end pckg_test1;
/
create or replace package body pkg_val_1.pckg_test1 as
procedure p1(c1 int,c2 out varchar2) as
begin
c2 :=var||c1;
end;
end pckg_test1;
/

drop package if exists pkg_val_2.pckg_test1;
create or replace package pkg_val_2.pckg_test1 as

procedure p1(t1 int ,t2 out varchar2);
var2 varchar2;
end pckg_test1;
/
create or replace package body pkg_val_2.pckg_test1 as
procedure p1(t1 int ,t2 out varchar2) as
begin
pkg_val_1.pckg_test1.p1(c1 => t1,c2 => pkg_val_1.pckg_test1.var);
raise info '%', pkg_val_1.pckg_test1.var;
end;
end pckg_test1;
/
call pkg_val_2.pckg_test1.p1(3,'');

drop package if exists pkg_val_1.pckg_test1;
drop package if exists pkg_val_2.pckg_test1;

-- 2. package varibal as out param
create or replace package pkg_val_1.pckg_test1 as
var varchar2 :='abc';
procedure p1(c1 int,c2 out varchar2);
end pckg_test1;
/
create or replace package body pkg_val_1.pckg_test1 as
procedure p1(c1 int,c2 out varchar2) as
begin
c2 :=var||c1;
end;
end pckg_test1;
/

create or replace package pkg_val_2.pckg_test1 as

procedure p1(t1 int ,t2 out varchar2);
var2 varchar2;
end pckg_test1;
/
create or replace package body pkg_val_2.pckg_test1 as
procedure p1(t1 int ,t2 out varchar2) as
begin
pkg_val_1.pckg_test1.p1(t1,pkg_val_1.pckg_test1.var);
raise info '%', pkg_val_1.pckg_test1.var;
end;
end pckg_test1;
/
call pkg_val_2.pckg_test1.p1(3,'');
create schema ss1;
create or replace package ss1.pkg8 is
  progname varchar2(60);
  workdate_bpc varchar2(10);
end pkg8;
/

create or replace package pkg7 is 
  var1 int:=1;
type t_pfxp_athfcdtl is record (dapcode int);
procedure proc1();
end pkg7;
/

create table testtab (a int, b varchar2(10));

create or replace package body pkg7 is
  procedure proc1() is
  v_pfxp_athfcdtl t_pfxp_athfcdtl;
  cursor cur_pfxp_athfcdtl is
    select a from testtab where b=ss1.pkg8.workdate_bpc 
    order by a;
  begin
    raise notice 'pkg7';
  end;
end pkg7;
/

call pkg7.proc1();

drop table testtab;
drop package pkg7;
drop package ss1.pkg8;
drop schema ss1;
drop package if exists pkg_val_1.pckg_test1;
drop package if exists pkg_val_2.pckg_test1;

--test package cursor
create table t1(a int, b int);
insert into t1 values(1,2);
insert into t1 values(2,4);
insert into t1 values(3,6);
create or replace package pck1 is
cursor c1 is select * from t1;
end pck1;
/
create or replace package pck2 is
procedure p1;
end pck2;
/
create or replace package body pck2 is
procedure p1 as
type r1 is record (a int, b int);
va r1;
begin
open pck1.c1;
loop 
fetch pck1.c1 into va;
exit when pck1.c1%notfound;
raise info 'va:  %',va;
raise info 'rowcount: %', pck1.c1%ROWCOUNT;
raise info 'isopend: %', pck1.c1%isopen;
raise info 'isfound: %', pck1.c1%found;
end loop;
close pck1.c1;
raise info 'isopend: %', pck1.c1%isopen;
end;
end pck2;
/

call pck2.p1();

DROP PACKAGE pck2;
DROP PACKAGE pck1;
DROP TABLE t1;

-- test package cursor error
create or replace package pck1 is
c1 int;
end pck1;
/

create or replace package pck2 is
procedure p1;
end pck2;
/
create or replace package body pck2 is
procedure p1 as
type r1 is record (a int, b int);
va r1;
begin
pck1.c1 := 1;
raise info 'rowcount: %', pck1.c1%ROWCOUNT;
raise info 'isopend: %', pck1.c1%isopen;
raise info 'isfound: %', pck1.c1%found;
end;
end pck2;
/

create or replace package body pck2 is
procedure p1 as
type r1 is record (a int, b int);
va r1;
begin
pck1.c1 := 1;
raise info 'rowcount: %', pck1.c2%ROWCOUNT;
raise info 'isopend: %', pck1.c2%isopen;
raise info 'isfound: %', pck1.c2%found;
end;
end pck2;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- test comment errors
CREATE OR REPLACE PROCEDURE test1
IS /*aa*/
a INT:=1; /*aa*/
c INT:=2; /*aa*/
BEGIN /*aa*/
IF a<>1 THEN /*aa*/
c:=3;   /*aa*/
END IF; /*aa*/
END; /*aa*/
/

CREATE TABLE t1(a INT, b INT);
CREATE OR REPLACE PROCEDURE test2 IS
va INT;
BEGIN
SELECT  /* aa */ 1 INTO va;
RAISE INFO '%', va;
INSERT /* aa */ INTO t1 VALUES(3,2);
INSERT /* aa */ INTO t1 VALUES(3,3);
UPDATE /* aa */ t1 SET a = 1 WHERE b =2;
DELETE /* aa */ FROM t1 WHERE a = 1;
END;
/

CALL test2();
SELECT * FROM t1;

DROP PROCEDURE test1;
DROP PROCEDURE test2;
DROP TABLE t1;

-- test create matview
drop table if exists materialized_view_tb;
create table materialized_view_tb(c1 int,c2 int);
create or replace package materialized_view_package as
procedure materialized_view_proc();
end materialized_view_package;
/
create or replace package body materialized_view_package AS
procedure materialized_view_proc() AS
begin
CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM materialized_view_tb;
INSERT INTO materialized_view_tb VALUES(1,1),(2,2);
REFRESH MATERIALIZED VIEW my_mv;
end;
end materialized_view_package;
/

call materialized_view_package.materialized_view_proc();

DROP MATERIALIZED VIEW my_mv;
DROP PACKAGE materialized_view_package;
DROP TABLE materialized_view_tb;

-- test drop package memory leak when ref other package variable
create type rec is (col1 varchar2,col2 varchar2);
create or replace package pckg_test as
type t_arr is table of rec;
type t_arr1 is table of varchar2;
v_arr t_arr;
v_arr1 t_arr1;
v_rec rec;
end pckg_test;
/
create or replace package pckg_test1 as
procedure proc_test(i_var1 in varchar2,i_var2 in varchar2);
end pckg_test1;
/

create or replace package body pckg_test1 as
procedure proc_test(i_var1 in varchar2,i_var2 in varchar2) as
v_var1 varchar2;
begin
pckg_test.v_arr(1) := rec(1,2);
pckg_test.v_arr1(1) := 1;
pckg_test.v_rec.col1 :=1;
end;
end pckg_test1;
/
call pckg_test1.proc_test('1','1');

drop package if exists pckg_test;
drop package if exists pckg_test1;
drop type if exists rec;

-- test drop loop ref variable package
create or replace package pckg_test1 as
procedure p1;
var varchar2 := 'pck1';
end pckg_test1;
/


create or replace package pckg_test2 as
procedure pp1;
var2 varchar2 := 'pck2';
end pckg_test2;
/

create or replace package pckg_test3 as
procedure ppp1;
var3 varchar2 := 'pck3';
end pckg_test3;
/

create or replace package body pckg_test1 as
procedure p1 as
begin
raise info '%', pckg_test3.var3;
end;
end pckg_test1;
/

create or replace package body pckg_test2 as
procedure pp1 as
begin
raise info '%', pckg_test1.var;
end;
end pckg_test2;
/


create or replace package body pckg_test3 as
procedure ppp1 as
begin
raise info '%', pckg_test2.var2;
end;
end pckg_test3;
/

call pckg_test3.ppp1();
call pckg_test2.pp1();
call pckg_test1.p1();


drop package if exists pckg_test1;
drop package if exists pckg_test2;
drop package if exists pckg_test3;
-- test schema.pkg.cursor
create table t1 (a int, b int);

create or replace package pck1 as
cursor c1 for select * from t1;
procedure p1;
end pck1;
/
create or replace package body pck1 as
procedure p1 as
va t1;
begin
open pkg_val_2.pck1.c1;
close pkg_val_2.pck1.c1;
end;
end pck1;
/

DROP PACKAGE pck1;

-- test auto cursor
create or replace package pck2 as
cursor c1 for select * from t1;
procedure p1;
end pck2;
/
create or replace package pck1 as
cursor c1 for select * from t1;
procedure p1;
end pck1;
/

-- cross package cursor
create or replace package body pck1 as
procedure p1 as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open pck2.c1;
close pck2.c1;
end;
end pck1;
/

-- own package cursor
create or replace package body pck1 as
procedure p1 as
PRAGMA AUTONOMOUS_TRANSACTION;
va t1;
begin
open c1;
close c1;
end;
end pck1;
/


DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- auto ref package with cursor, but not use, should not error
create or replace package pck2 as
cursor c1 for select * from t1;
va int;
procedure p1;
end pck2;
/
create or replace package body pck2 as
procedure p1 as
begin
open c1;
end;
begin
va := 1;
end pck2;
/

create or replace package pck1 as
cursor c1 for select * from t1;
procedure p1;
end pck1;
/
create or replace package body pck1 as
procedure p1 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
pck2.va := 1;
end;
end pck1;
/

call pck1.p1();
DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- auto call another normal procedure ref package cursor,should error when call
create or replace package pck2 as
cursor c1 for select * from t1;
va int;
procedure p1;
end pck2;
/

create or replace procedure p2 as
va t1;
begin
open pck2.c1;
close pck2.c1;
end;
/

create or replace package pck1 as
cursor c1 for select * from t1;
procedure p1;
end pck1;
/
create or replace package body pck1 as
procedure p1 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p2();
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;
DROP PACKAGE pck2;
DROP PROCEDURE p2();
DROP TABLE t1;

-- test check sql in for rec in query loop
create or replace function check_f04 return int as
declare
v_2 number(10,2):=-213.989;
v_3 float:=22.34;
v_4 char(1):='q';
v_6 date:='1999-09-09';
v_7 clob:='dddd';
v_8 blob:='ad';
begin
raise info '1-0 %', v_6;
for v_6 in select c3 from check_tab1 order by c3 limit 6
loop
raise info '1-1 %', v_6;
end loop;
raise info '1-2 %', v_6;

return 1;
exception
when others then
raise info 'error is %',sqlerrm;
return 0;
end;
/

create table check_tab1(a int);

create or replace function check_f04 return int as
declare
v_2 number(10,2):=-213.989;
v_3 float:=22.34;
v_4 char(1):='q';
v_6 date:='1999-09-09';
v_7 clob:='dddd';
v_8 blob:='ad';
begin
raise info '1-0 %', v_6;
for v_6 in select c3 from check_tab1 order by c3 limit 6
loop
raise info '1-1 %', v_6;
end loop;
raise info '1-2 %', v_6;

return 1;
exception
when others then
raise info 'error is %',sqlerrm;
return 0;
end;
/

drop table check_tab1;

reset behavior_compat_options;

-- ref package cursor attr at first
create table t1(a int, b int);
create or replace package pck1 is
cursor c1 is select * from t1;
end pck1;
/
begin
raise info 'isopend: %', pck1.c1%isopen;
end
/
drop package pck1;
drop table t1;

-- package cursor attribute reset
drop table t1;
create table t1 (a int, b int);
insert into t1 values (1,2);

create or replace package pck1 as
cursor c1 for select * from t1;
procedure p1;
end pck1;
/
create or replace package body pck1 as
procedure p1 as
declare
va t1;
begin
raise info 'isopend: %', c1%isopen;
raise info 'rowcount: %', c1%rowcount;
open  c1;
fetch c1 into va;
raise info 'va:%', va;
raise info 'isopend: %', c1%isopen;
raise info 'rowcount: %', c1%rowcount;
end;
end pck1;
/

call pck1.p1();
call pck1.p1();

drop package pck1;
drop table t1;

-- package cursor with arguments
drop table t1;
create table t1 (a int, b int);
insert into t1 values (1,2);
insert into t1 values (3,4);
create or replace package pck1 as
cursor c1(va int) for select * from t1 where a < va;
procedure p1;
end pck1;
/

create or replace procedure pp1() as
va t1;
begin
fetch pck1.c1 into va;
raise info 'va:%', va;
raise info 'isopend: %', pck1.c1%isopen;
raise info 'rowcount: %', pck1.c1%rowcount;
end;
/

create or replace package body pck1 as
procedure p1 as
declare
va t1;
begin
raise info 'isopend: %', c1%isopen;
raise info 'rowcount: %', c1%rowcount;
open  pck1.c1(10);
fetch pck1.c1 into va;
raise info 'va:%', va;
raise info 'isopend: %', c1%isopen;
raise info 'rowcount: %', c1%rowcount;
pp1();
end;
end pck1;
/

call pck1.p1();

declare
va t1;
begin
open pck1.c1(4);
fetch pck1.c1 into va;
end;
/

drop procedure pp1;
drop package pck1;
drop table t1;

-- test package variable as in param
create type o1 as (a int, b varchar2);
create or replace package pck1 as
va varchar2;
vb varchar2 := 'vb';
vc o1;
procedure p1;
end pck1;
/
create or replace package body pck1 as
procedure p1 as
begin
va := 'hahahah';
vc := (1,'cccc');
end;
end pck1;
/
create table testlog(a int);
create or replace procedure insertlog() is
PRAGMA AUTONOMOUS_TRANSACTION;
begin
insert into testlog values(1);
pck1.va := 'bbbbbbb';
pck1.vc := (123,'ayayayayay');
exception
when others then
raise notice 'sqlerrm:%',sqlerrm;
raise;
end;
/
create or replace procedure p1(va in varchar2, vb in varchar2, vc in o1) is
begin
raise info 'before auto: %,%', vb,vc;
insertlog();
raise info 'after auto: %,%', vb,vc;
exception
when others then
raise notice 'sqlerrm:%',sqlerrm;
raise;
end;
/
declare
begin
pck1.p1();
p1(150,pck1.va,pck1.vc);
raise info 'after p1: %,%', pck1.va,pck1.vc;
end;
/
 
drop procedure p1;
drop procedure insertlog;
drop package pck1;
drop table testlog;
drop type o1;

-- package init src when annother package ref it
create or replace package pkg062
is
type type001 is record(c1 int,c2 number,c3 varchar2(30),c4 clob,c5 blob);
type type002 is table of type001 index by integer;
col1 type002:=type002();
procedure proc062_1();
end pkg062;
/

create or replace package body pkg062
is
procedure proc062_1()
is
begin
null;
end;
begin
col1(1):=(1,1,'varchar1',repeat('clob1',2),'abcdef1');
col1(2):=(2,2,'varchar10',repeat('clob2',2),'abcdef2');
end pkg062;
/

create or replace package pkg062_1
is
procedure p1();
end pkg062_1;
/

create or replace package body pkg062_1
is
procedure p1()
is
begin
raise info 'pkg062.col1 %',pkg062.col1;
end;
end pkg062_1;
/

drop package pkg062_1;
drop package pkg062;

-- tuple desc free in time
create or replace package pkg048 is   
type type000 is record (c1 number(9,3),c2 varchar2(30));   
type type001 is table of type000;   
type type002 is record (c1 type001,c2 type000);   
end pkg048;
/
create or replace package pkg048_1 
is
function func048_1_1(c1 out pkg048.type002) return pkg048.type001;
end pkg048_1;
/
create or replace package body pkg048_1
is
function func048_1_1(c1 out pkg048.type002) return pkg048.type001
is
a int;
c2 pkg048.type001;
begin
c1.c1(1).c2 = 'ab';
c1.c2.c2:=c1.c1(1).c2;
return c1.c1;
exception
when others then
raise info 'sqlerrm is %',sqlerrm;
raise info 'sqlcode is %',sqlcode;
return c1.c1;
end;
end pkg048_1;
/
call pkg048_1.func048_1_1(null);
call pkg048_1.func048_1_1(null);

drop package pkg048_1;
drop package pkg048;

-- test nest ref package
create or replace package pck2 IS
TYPE rateTable is TABLE of NUMBER INDEX by varchar2(50);
v_ratetable pck2.rateTable;
PROCEDURE p2(IN_NODE_CODE IN VARCHAR2,IN_CINO IN VARCHAR2);
END pck2;
/

create or replace package pck1 IS
v_ratetable pck2.rateTable;
type r1 is record (a int, b int);
PROCEDURE p1(OUT_FLAG OUT VARCHAR2,OUT_MSG OUT VARCHAR2);
END pck1;
/

create or replace package body pck1 IS
PROCEDURE p1(OUT_FLAG OUT VARCHAR2,OUT_MSG OUT VARCHAR2) as
begin
null;
end;
END pck1;
/

create or replace package body pck2 is
PROCEDURE p2(IN_NODE_CODE IN VARCHAR2,IN_CINO IN VARCHAR2) as
begin
pck1.p1(IN_NODE_CODE,IN_CINO);
end;
end pck2;
/

call pck2.p2('a','b');

drop package pck2;
drop package pck1;
-- clean 
DROP SCHEMA IF EXISTS pkg_val_1 CASCADE;
DROP SCHEMA IF EXISTS pkg_val_2 CASCADE;

