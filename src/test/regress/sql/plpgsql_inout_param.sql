-- test create type table of 
-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists plpgsql_inout;
create schema plpgsql_inout;
set current_schema = plpgsql_inout;
set behavior_compat_options="proc_outparam_override";
------------------------------------------------
--------------------inout-----------------------
------------------------------------------------
create or replace procedure proc1(a1 in out int, a2 in out int) 
is 
begin 
a1 := a1 + 1;
a2 := a2 + 1;
end;
/

create or replace procedure proc2()
is 
a1 int := 1;
a2 int := 2;
begin 
raise info '%', a1;
proc1(a1, a2);
raise info 'a1:%', a1;
raise info 'a2:%', a2;
end;
/

call proc2();

create or replace procedure proc2()
is 
a1 int := 1;
a2 int := 2;
begin 
raise info '%', a1;
proc1(a1=>a1, a2=>a2);
raise info 'a1:%', a1;
raise info 'a2:%', a2;
end;
/

call proc2();

-- test table
create or replace procedure proc3()
is 
type arr is table OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2(1), a2(2));
raise info 'a2:%', a2;
end;
/

call proc3();

create or replace procedure proc3()
is 
type arr is table OF integer;
aa2 arr;
begin 
aa2[1] = 2;
aa2[2] = 3;
proc1(a1=>aa2(1), a2=>aa2(2));
raise info 'aa2:%', aa2;
end;
/

call proc3();

-- table nest error
create or replace procedure proc3()
is 
type arr is table OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2(1)(1), a2(2));
raise info 'a2:%', a2;
end;
/

create or replace procedure proc3()
is 
type arr is table OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2[1], a2[2]);
raise info 'a2:%', a2;
end;
/

call proc3();

create or replace procedure proc3()
is 
type arr is table OF integer;
aa2 arr;
aa1 int;
begin 
aa2[1] = 2;
aa2[2] = 3;
aa1 := 1;
proc1(a1=>aa2[1], a2=>aa2[2]);
raise info 'aa2:%', aa2;
end;
/

call proc3();

-- test array
create or replace procedure proc4()
is 
type arr is varray(10) OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2(1), a2(2));
raise info 'a2:%', a2;
end;
/

call proc4();

create or replace procedure proc4()
is 
type arr is varray(10) OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2(1), a2(2));
raise info 'a2:%', a2;
end;
/

call proc4();

create or replace procedure proc4()
is 
type arr is varray(10) OF integer;
a2 arr;
a1 int;
begin 
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2[1], a2[2]);
raise info 'a2:%', a2;
end;
/

call proc4();

create or replace procedure proc4()
is 
type arr is varray(10) OF integer;
aa2 arr;
aa1 int;
begin 
aa2[1] = 2;
aa2[2] = 3;
aa1 := 1;
proc1(a1=>aa2[1], a2=>aa2[2]);
raise info 'aa2:%', aa2;
end;
/

call proc4();

create or replace procedure proc4()
is 
a int[][] := ARRAY[ARRAY[1,2,3],ARRAY[4,5,6],ARRAY[7,8,9]];
begin 
a[1][2] = 2;
a[2][1] = 3;
proc1(a[2][3], a[3][2]);
raise info 'a2:%', a;
end;
/

call proc4();

create or replace procedure proc4()
is 
a int[][] := ARRAY[ARRAY[1,2,3],ARRAY[4,5,6],ARRAY[7,8,9]];
begin 
a[1][2] = 2;
a[2][1] = 3;
proc1(a1=>a[2][3], a2=>a[3][2]);
raise info 'a2:%', a;
end;
/

call proc4();

create or replace procedure proc5(a1 in out int[]) 
is 
begin 
a1[2] = 2;
end;
/

create or replace procedure proc6()
is 
a1 int[];
begin 
a1[1] = 1;
proc5(a1);
raise info 'a1:%', a1;
end;
/

call proc6();

create or replace procedure proc6()
is 
a1 int[];
begin 
a1[1] = 1;
proc5(a1=>a1);
raise info 'a1:%', a1;
end;
/

call proc6();

create or replace procedure proc7(a1 in out int[], a2 in out int) 
is 
begin 
a1[2] = 2;
a2 = 2;
end;
/

create or replace procedure proc8()
is 
a1 int[];
a2 int;
begin 
a1[1] = 1;
a2 = 1;
proc7(a1, a2);
raise info 'a1:%', a1;
raise info 'a2:%', a2;
end;
/

call proc8();

create or replace procedure proc8()
is 
a1 int[];
a2 int;
begin 
a1[1] = 1;
a2 = 1;
proc7(a1=>a1, a2=>a2);
raise info 'a1:%', a1;
raise info 'a2:%', a2;
end;
/

call proc8();

create table tb_test(col1 int,col2 int,col3 int);
insert into tb_test values (1,1,1);
insert into tb_test values (2,2,2);

-- test for rec
create or replace procedure proc9()
is 
begin 
for rec in (select col1,col2,col3 from tb_test) loop
proc1(rec.col1, rec.col2);
raise info 'col1:%', rec.col1;
raise info 'col2:%', rec.col2;
end loop;
end;
/

call proc9();

create or replace procedure proc9()
is 
begin 
for rec in (select col1,col2,col3 from tb_test) loop
proc1(a1=>rec.col1, a2=>rec.col2);
raise info 'col1:%', rec.col1;
raise info 'col2:%', rec.col2;
end loop;
end;
/

call proc9();

create or replace procedure proc10()
is 
begin 
for rec in (select col1,col2,col3 from tb_test) loop
proc1(rec.col1, rec.col2);
raise info 'col1:%', rec.col1;
end loop;
end;
/

call proc10();

create type info as (name varchar2(50), age int, address varchar2(20), salary float(2));
-- test 1 out param
create or replace procedure proc12(a inout info)
is 
    
begin 
    a = ('Vera' ,32, 'Paris', 22999.00);
end;
/

-- test record 
create or replace procedure proc11()
is 
    a info;
begin 
    proc12(a);
    raise info '%', a;
end;
/

call proc11();

create or replace procedure proc11()
is 
    a info;
begin 
    proc12(a=>a);
    raise info '%', a;
end;
/

call proc11();

-- test 2 out param
create or replace procedure proc20(a inout info, b inout int)
is 
    
begin 
    b = 1;
    a = ('Vera' ,32, 'Paris', 22999.00);
end;
/

-- test record 
create or replace procedure proc21()
is 
    a info;
    b int;
begin 
    proc20(a,b);
    raise info '%', a;
    raise info '%', b;
end;
/

call proc21();

-- test record 
create or replace procedure proc21()
is 
    a info;
    b int;
begin 
    proc20(a=>a,b=>b);
    raise info '%', a;
    raise info '%', b;
end;
/

call proc21();

--test reord error
create or replace procedure proc11()
is 
    type r is record (name varchar2(50), age int, address varchar2(20), salary float(2));
    a r;
begin 
    a = ('Vera' ,33, 'Paris', 22999.00);
    proc12(row(a));
    raise info '%', a;
end;
/

--test record nest
create or replace procedure proc12()
is 
    type r is varray(10) of info;
    a r;
    a2 int := 1;
begin 
    a[1] = ('Vera' ,33, 'Paris', 22999.00);
    proc1(a[1].age, a2);
    raise info '%', a;
end;
/

call proc12();

create or replace procedure proc12()
is 
    type r is varray(10) of info;
    a r;
    a2 int := 1;
begin 
    a[1] = ('Vera' ,33, 'Paris', 22999.00);
    proc1(a1=>a[1].age, a2=>a2);
    raise info '%', a;
end;
/

call proc12();

create or replace procedure proc12()
is 
    a info[][] := ARRAY[ARRAY[('',1,'',0), ('',2,'',0)],ARRAY[('',3,'',0), ('',4,'',0)]];
    a2 int := 1;
begin 
    proc1(a[1][2].age, a2);
    raise info '%', a;
end;
/

call proc12();

create or replace procedure proc12()
is 
    a info[][] := ARRAY[ARRAY[('',1,'',0), ('',2,'',0)],ARRAY[('',3,'',0), ('',4,'',0)]];
    a2 int := 1;
begin 
    proc1(a1=>a[1][2].age, a2=>a2);
    raise info '%', a;
end;
/

call proc12();

create type o1 as (a int, b int);
create type o2 as (a int, b o1);
create type o3 as (a int, b o2);

create or replace procedure proc13()
is 
    a o2;
    a2 int := 1;
begin 
    a.b.b = 1;
    proc1(a.b.b, a2);
    raise info '%', a;
end;
/

call proc13();

create or replace procedure proc13()
is 
    a o2;
    a2 int := 1;
begin 
    a.b.b = 1;
    proc1(a1=>a.b.b, a2=>a2);
    raise info '%', a;
end;
/

call proc13();

create or replace procedure proc14()
is 
    a o3;
    a2 int := 1;
begin 
    a.b.b.b = 1;
    raise info '%', a;
    proc1(a.b.b.b, a2);
    raise info '%', a;
end;
/

call proc14();

create or replace procedure proc14()
is 
    a o3;
    a2 int := 1;
begin 
    a.b.b.b = 1;
    raise info '%', a;
    proc1(a1=>a.b.b.b, a2=>a2);
    raise info '%', a;
end;
/

call proc14();

create type customer as (id number(10), c_info info);
create table customers (id number(10), c_info info);

insert into customers (id, c_info) values (1, ('Vera' ,32, 'Paris', 22999.00));

create or replace procedure proc15()
is 
rec record;
begin 
for rec in (select id, c_info from customers) loop
proc1(rec.c_info.id,1);
raise info '%', rec.c_info.id;
end loop;
end;
/

call proc15();

create or replace procedure proc15()
is 
rec record;
begin 
for rec in (select id, c_info from customers) loop
proc1(a1=>rec.c_info.id, a2=>1);
raise info '%', rec.c_info.id;
end loop;
end;
/

call proc15();

create or replace procedure proc16(a1 in out varchar) 
is 
begin 
a1 := 'bbbb';
end;
/

create or replace procedure proc17()
is 
type arr is varray(10) OF varchar(10);
a arr;
begin 
a[1] = 'aaa';
proc16(a[1]);
raise info '%', a;
end;
/

call proc17();

create or replace procedure proc17()
is 
type arr is varray(10) OF varchar(10);
a arr;
begin 
a[1] = 'aaa';
proc16(a1=>a[1]);
raise info '%', a;
end;
/

call proc17();

create or replace package pckg_test1 as
array_info info[][] := ARRAY[ARRAY[('',1,'',0), ('',2,'',0)],ARRAY[('',3,'',0), ('',4,'',0)]];
array_int int[][] := ARRAY[ARRAY[1,2,3],ARRAY[4,5,6],ARRAY[7,8,9]];
procedure pr_test(i_col1 inout int,i_col2 inout int);
procedure pr_test1();
procedure pr_test2();
procedure pr_test3();
end pckg_test1;
/

create or replace package body pckg_test1 as
procedure pr_test(i_col1 inout int,i_col2 inout int)as
begin
i_col1 = i_col1+1;
i_col2 = i_col2+2;
end;

procedure pr_test1()as
begin
for rec in (select col1,col2,col3 from tb_test) loop
raise info '%', rec.col2;
pr_test(rec.col1,rec.col2);
raise info '%', rec.col2;
end loop;
end;

procedure pr_test2()as
a o2;
b o3;
begin
a.b.b = 1;
b.b.b.b = 1;
pr_test(a.b.b, b.b.b.b);
raise info '%', a;
raise info '%', b;

pr_test(array_info[1][2].age,array_int[2][3]);
raise info '%',array_info;
raise info '%',array_int;

end;

procedure pr_test3()as
type arr is varray(10) OF integer;
a2 arr;
type tbl is table of integer;
a3 tbl;
begin
a2[1] = 1;
a3[2] = 1;
pr_test(a2[1],a3[2]);
raise info '%',a2;
raise info '%',a3;
end;
end pckg_test1;
/

call pckg_test1.pr_test1();
call pckg_test1.pr_test2();
call pckg_test1.pr_test3();

create or replace package body pckg_test1 as
procedure pr_test(i_col1 inout int,i_col2 inout int)as
begin
i_col1 = i_col1+1;
i_col2 = i_col2+2;
end;

procedure pr_test1()as
begin
for rec in (select col1,col2,col3 from tb_test) loop
raise info '%', rec.col2;
pr_test(i_col1=>rec.col1, i_col2=>rec.col2);
raise info '%', rec.col2;
end loop;
end;

procedure pr_test2()as
a o2;
b o3;
begin
a.b.b = 1;
b.b.b.b = 1;
pr_test(i_col1=>a.b.b, i_col2=>b.b.b.b);
raise info '%', a;
raise info '%', b;

pr_test(i_col1=>array_info[1][2].age, i_col2=>array_int[2][3]);
raise info '%',array_info;
raise info '%',array_int;

end;

procedure pr_test3()as
type arr is varray(10) OF integer;
a2 arr;
type tbl is table of integer;
a3 tbl;
begin
a2[1] = 1;
a3[2] = 1;
pr_test(i_col1=>a2[1], i_col2=>a3[2]);
raise info '%',a2;
raise info '%',a3;
end;
end pckg_test1;
/

call pckg_test1.pr_test1();
call pckg_test1.pr_test2();
call pckg_test1.pr_test3();

create or replace procedure proc1(c1 out INT, c2 out INT)
is
begin
raise info '%', c1;
c1 := 10000;
c2 := 20000;
end;
/
create or replace procedure proc3()
is
type arr is table OF INT;
a2 arr;
a1 INT;
begin
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(a2(1), a2(2));
raise info 'a2:%', a2;
raise info 'a1:%', a1;
end;
/

call proc3();

create or replace procedure proc3()
is
type arr is table OF INT;
a2 arr;
a1 INT;
begin
a2[1] = 2;
a2[2] = 3;
a1 := 1;
proc1(c1=>a2(1), c2=>a2(2));
raise info 'a2:%', a2;
raise info 'a1:%', a1;
end;
/

call proc3();

create or replace procedure proc1(a1 in BIGINT, a2 out BIGINT, a3 inout BIGINT)
is
begin
a1 := a1 + 10000;
a2 := a2 + 20000;
a3 := a3 + 30000;
end;
/
create or replace procedure proc3()
is
type arr is table OF BIGINT;
a2 arr;
begin
a2[1] = 1;
a2[2] = 2;
a2[3] = 3;
proc1(a2(1), a2(2), a2(3));
raise info 'a2:%', a2;
raise info 'a2:%', a2[1];
raise info 'a2:%', a2[2];
end;
/

call proc3();

create or replace procedure proc3()
is
type arr is table OF BIGINT;
c2 arr;
begin
c2[1] = 1;
c2[2] = 2;
c2[3] = 3;
proc1(a1=>c2(1), a2=>c2(2), a3=>c2(3));
raise info 'a2:%', c2;
raise info 'a2:%', c2[1];
raise info 'a2:%', c2[2];
end;
/

call proc3();

create type t as (a int, b boolean);
-- complex type
create or replace procedure proc1(a1 in out t, a2 in out boolean) 
is 
begin 
a1.a := a1.a + 1;
a1.b := false;
a2 := false;
end;
/

declare
a t;
b boolean;
begin
a.a = 1;
a.b = true;
b = true;
proc1(a,b);
raise info '%', a.a;
raise info '%', a.b;
raise info '%', b;
end;
/

declare
a t;
b boolean;
begin
a.a = 1;
a.b = true;
b = true;
proc1(a1=>a,a2=>b);
raise info '%', a.a;
raise info '%', a.b;
raise info '%', b;
end;
/

------------------------------------------------
---------------------out------------------------
------------------------------------------------
create or replace procedure proc1(a1 out INT)
is
begin
a1 := 10000;
raise info 'a1:%', a1;
end;
/

create or replace procedure proc3()
is
type arr is table OF INT;
a2 arr;
a1 int := 2;
begin
a2[1] = 1;
proc1(a2(1));
raise info 'a2:%', a2;
proc1(a1);
raise info 'a1:%', a1;
end;
/

call proc3();

create or replace procedure proc3()
is
type arr is table OF INT;
a2 arr;
a1 int := 2;
begin
a2[1] = 1;
proc1(a1=>a2(1));
raise info 'a2:%', a2;
proc1(a1=>a1);
raise info 'a1:%', a1;
end;
/

call proc3();

create or replace procedure proc3()
is
type arr is varray(10) OF INT;
a2 arr;
a1 int := 2;
begin
a2[1] = 1;
proc1(a2(1));
raise info 'a2:%', a2;
proc1(a1);
raise info 'a1:%', a1;
end;
/

call proc3();

create or replace procedure proc1(a1 out t) 
is 
begin 
a1.a := 1;
a1.b := false;
end;
/

declare
a t;
begin
proc1(a);
raise info '%', a;
end;
/

declare
a t;
begin
proc1(a1=>a);
raise info '%', a;
end;
/


create or replace procedure proc1(a1 out t, a2 out boolean) 
is 
begin 
a1.a := 1;
a1.b := false;
a2 := false;
end;
/

declare
a t;
b boolean;
begin
proc1(a,b);
raise info '%', a;
raise info '%', b;
end;
/

declare
a t;
b boolean;
begin
proc1(a1=>a,a2=>b);
raise info '%', a;
raise info '%', b;
end;
/

create type t1 is table of int;
create or replace procedure p1(c1 in int, c2 out t1)
is
a int;
begin
a := c1;
raise info '%',a;
c2(1) := 1;
c2(2) := 2;
return;
end;
/

create or replace procedure p2()
is
a t1;
begin
p1(c1=>'12',c2=>a);
raise info '%',a;
end;
/

call p2();

create or replace package pck6 is
type tp_2 is record(v01 number, v03 varchar2, v02 number);
end pck6;
/

create or replace package pck5 is
type tp_1 is record(v01 number, v03 varchar2, v02 number);
procedure pp11(v01 out tp_1);
procedure pp11(v121 out number,v122 out pck6.tp_2);
end pck5;
/

create or replace package body pck5 is
procedure pp11(v01 out tp_1) is
v122 pck6.tp_2;
begin
pp11(v121 => v01.v01, v122 => v122);
raise notice 'v01 : %', v01.v01;
end;
procedure pp11(v121 out number,v122 out pck6.tp_2) is
v_id1 varchar2;
begin
select id1 into v_id1 from test_tb1 limit 1;
raise notice '%', v_id1;
v121 := 12;
EXCEPTION
when no_data_found then
raise notice 'no data found: %', v121||SQLERRM;
v121 :=1;
WHEN others then
raise notice 'others :%', v121||SQLERRM;
v121 := 2;
end;
end pck5;
/

create or replace function fun1 return number as
v01 pck5.tp_1;
begin
pck5.pp11(v01);
return 0;
end;
/

select fun1();

create or replace package body pck5 is
procedure pp11(v01 out tp_1) is
v122 pck6.tp_2;
begin
pp11(v01.v01, v122);
raise notice 'v01 : %', v01.v01;
end;
procedure pp11(v121 out number,v122 out pck6.tp_2) is
v_id1 varchar2;
begin
select id1 into v_id1 from test_tb1 limit 1;
raise notice '%', v_id1;
v121 := 12;
EXCEPTION
when no_data_found then
raise notice 'no data found: %', v121||SQLERRM;
v121 :=1;
WHEN others then
raise notice 'others :%', v121||SQLERRM;
v121 := 2;
end;
end pck5;
/

select fun1();

create or replace package pck7 is
type t_out is record(retcode number, 
                     errcode number,
                     eerm varchar2(4000),
                     sqlcode varchar2(100),
                     sqlerrm varchar2(4000)
                     );
success constant number(1) = 0;
fail constant number(1) = 1;
end pck7;
/

create or replace package pck8 is
v_out pck7.t_out;
procedure pp11(in_groupno in varchar2,
               in_workdate in varchar2,
               o_retcode out number);
procedure pp11(in_groupno in varchar2,
               in_workdate in varchar2,
               o_base out pck7.t_out);
end pck8;
/

create or replace package body pck8 is
procedure pp11(in_groupno in varchar2,
               in_workdate in varchar2,
               o_retcode out number
               ) is
v_out pck7.t_out;
begin
pp11(in_groupno=>in_groupno, 
     in_workdate => in_workdate,
     o_base => v_out);
raise notice 'v_out : %', v_out;
end;
procedure pp11(in_groupno in varchar2,
               in_workdate in varchar2,
               o_base out pck7.t_out)is
v_id1 varchar2;
begin
o_base := (1,1,'a','b','c');
--o_base.retcode := 2;
end;
end pck8;
/

declare
va number;
begin
pck8.pp11('a','b',va);
end;
/

drop package pck5;
drop package pck6;
drop package pck7;
drop package pck8;
drop package pckg_test1;
drop package pckg_test2;

create or replace procedure pp1(va int, vb int, vc out int) as
begin
null;
end;
/
declare
v1 int;
begin
pp1(vd=>v1, va=>v1, vb=>v1);
end;
/

drop procedure pp1;

-- two out param, one is valid (should error)
drop package if exists pck1;
create or replace package pck1 is
procedure p1(a out varchar2,b out int);
end pck1;
/
create or replace package body pck1 is
procedure p1(a out varchar2,b out int) is
begin
b:=1;
a:='a'||b;
end;
end pck1;
/

declare
var varchar2;
begin
pck1.p1(var,1);
raise info 'var:%',var;
end;
/

drop package pck1;
-- two out param, one is valid, overload situation (should error)
create or replace package pkg070
is
type type000 is record (c1 int,c2 number,c3 varchar2(30),c4 clob,c5 blob);
type type001 is table of integer index by integer;
procedure proc070_1(col2 out type001,col3 out int,col4 out type001,col5 out int);
procedure proc070_1(col4 out type001,col5 out int);
procedure proc070_2();
end pkg070;
/

create or replace package body pkg070
is
procedure proc070_1(col2 out type001,col3 out int,col4 out type001,col5 out int)
is
col1 type001;
begin
col2(1):=3;
col2(2):=4;
col3:=col2.count;
col4(2):=44;
col4(6):=55;
col5:=col4.count;
end;
procedure proc070_1(col4 out type001,col5 out int)
is
begin
col4(1):=4;
col4(2):=44;
 --col4(3):=444;
col5:=col4.count;
raise info '2 parameter col5 is %',col5;
end;
procedure proc070_2()
is
tbcor1 type001;
tbcor2 type001;
begin
tbcor1(1):=1;
tbcor1(3):=3;
tbcor2(2):=2;
tbcor2(3):=23;
--proc070_1(tbcor1,tbcor1.count,tbcor2,tbcor2.count);
raise info 'tbcor1 is %',tbcor1;
raise info 'tbcor1.count is %',tbcor1.count;
raise info 'tbcor2 is %',tbcor2;
raise info 'tbcor2.count is %',tbcor2.count;
proc070_1(tbcor2,tbcor2.count);
raise info 'tbcor2 is %',tbcor2;
raise info 'tbcor2.count is %',tbcor2.count;
--raise info 'tbcor2.first is %',tbcor2.first;
end;
end pkg070;
/

call pkg070.proc070_2();

drop package pkg070;
-- two out param, one is valid, => situation (should error)
create or replace procedure pp1(a out int, b out int) as
begin
a := 1;
b := 1;
end;
/

declare
var1 int;
begin
pp1(a=>var1,b=>3);
end;
/

drop procedure pp1;

-- test one in row, one out shcalar
-- 1
drop package if exists pck1;
create or replace package pck1 is
type tp_1 is record(v01 number, v03 varchar2, v02 number);
type tp_2 is varray(10) of int;
procedure p1(a tp_1,b out varchar2);
procedure p1(a2 tp_2, b2 out varchar2);
end pck1;
/

create or replace package body pck1 is
procedure p1(a tp_1,b out varchar2) is
begin
b:=a.v01;
raise info 'b:%',b;
end;
procedure p1(a2 tp_2, b2 out varchar2) is
begin
b2:=a2(2);
raise info 'b2:%',b2;
end;
end pck1;
/

declare
var1 pck1.tp_1:=(2,'a',3);
var2 pck1.tp_2:=array[1,3];
var varchar2;
begin
pck1.p1(var1,var);
raise info 'var:%', var;
end;
/

drop package if exists pck1;

-- 2.
drop package if exists pck1;
create or replace package pck1 is
type tp_1 is record(v01 number, v03 varchar2, v02 number);
type tp_2 is record(v01 tp_1, v03 varchar2, v02 number);
procedure p1(a tp_1,b out int);
procedure p1(a2 in tp_2,b2 out int);
end pck1;
/

create or replace package body pck1 is
procedure p1(a tp_1,b out int) is
begin
b:=a.v02;
raise info 'b:%',b;
end;
procedure p1(a2 in tp_2,b2 out int) is
begin
b2:=a2.v01.v01;
raise info 'b2:%',b2;
end;
end pck1;
/

declare
var1 pck1.tp_1:=(1,'bb',3);
var2 pck1.tp_2:=((2,'aa',4),'c',5);
var int;
varr int;
begin
pck1.p1(var1,var);
pck1.p1(var2,varr);
raise info 'var:%',var;
end;
/
drop package if exists pck1;

--3.
drop table if exists tb_test;
create table tb_test(c1 int, c2 varchar2);
drop package if exists pck1;
create or replace package pck1 is
type tp_1 is record(v01 number, v03 varchar2, v02 number);
procedure p1(in a tb_test%rowtype,out b tp_1);
procedure p1(out a tp_1,in b tb_test%rowtype);
end pck1;
/

create or replace package body pck1 is
procedure p1(in a tb_test%rowtype,out b tp_1) is
begin
b.v01:=a.c1;
b.v03:=a.c2;
end;
procedure p1(out a tp_1,in b tb_test%rowtype) is
begin
a.v01:=b.c1+1;
a.v03:=b.c2;
end;
end pck1;
/
declare
var1 pck1.tp_1;
var2 tb_test%rowtype:=(1,'a');
var3 pck1.tp_1;
begin
pck1.p1(a=>var2,b=>var1);
raise info 'var1:%',var1;
pck1.p1(a=>var3,b=>var2);
raise info 'var3:%',var3;
end;
/

drop package pck1;
drop table tb_test;

-- clean
drop schema if exists plpgsql_inout cascade;