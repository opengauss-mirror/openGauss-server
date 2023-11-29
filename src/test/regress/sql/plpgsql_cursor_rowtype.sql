-- test cursor%type
-- check compatibility --
-- create new schema --
drop schema if exists plpgsql_cursor_rowtype;
create schema plpgsql_cursor_rowtype;
set current_schema = plpgsql_cursor_rowtype;
set behavior_compat_options='allow_procedure_compile_check';

create table emp (empno int, ename varchar(10), job varchar(10));
insert into emp values (1, 'zhangsan', 'job1');
insert into emp values (2, 'lisi', 'job2');

create or replace package pck1 is 
vvv emp%rowtype;
cursor cur1 is
select * from emp where empno=vvv.empno and ename=vvv.ename;
emp_row cur1%rowtype;
procedure p1();
end pck1;
/

create or replace package body pck1 is
procedure p1() is
a int;
begin
vvv.empno = 1;
vvv.ename = 'zhangsan';
open cur1;
fetch cur1 into emp_row;
raise info '%', emp_row.job;
end;
end pck1;
/

call pck1.p1();

create or replace procedure pro_cursor_args
is
    b varchar(10) := 'job1';
    cursor c_job
       is
       select empno,ename t 
       from emp
       where job=b;
    c_row c_job%rowtype;
begin
    for c_row in c_job loop
        raise info '%', c_row.t;
    end loop;
end;
/

call pro_cursor_args();

create or replace procedure pro_cursor_no_args_1
is
    b varchar(10);
    cursor c_job
       is
       select empno,ename t 
       from emp;
    c_row c_job%rowtype;
begin
    c_row.empno = 3;
    raise info '%', c_row.empno;
    for c_row in c_job loop
        raise info '%', c_row.empno;
    end loop;
end;
/

call pro_cursor_no_args_1();

-- test alias error 
create or replace procedure pro_cursor_args
is
    b varchar(10) := 'job1';
    cursor c_job
       is
       select empno,ename t 
       from emp
       where job=b;
    c_row c_job%rowtype;
begin
    for c_row in c_job loop
        raise info '%', c_row.ename;
    end loop;
end;
/

call pro_cursor_args();

create or replace procedure pro_cursor_no_args_2
is
    b varchar(10);
    cursor c_job
       is
       select empno,ename t 
       from emp;
    c_row c_job%rowtype;
begin
    open c_job;
    fetch c_job into c_row;
    raise info '%', c_row.empno;
    fetch c_job into c_row;
    raise info '%', c_row.empno;
end;
/

call pro_cursor_no_args_2();

create table test12(col1 varchar2,col2 varchar2);
insert into test12 values ('a', 'aa');
insert into test12 values ('b', 'bb');

create or replace package pck2 is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype;
procedure pp1;
end pck2;
/

create or replace package body pck2 is
procedure pp1() is
cursor cur2 is
select col1,col2 from test12;
begin
var1.col1 = 'c';
raise info '%', var1.col1;
open cur2;
fetch cur2 into var1;
raise info '%', var1.col1;
fetch cur2 into var1;
raise info '%', var1.col1;
end;
end pck2;
/

call pck2.pp1();

create or replace package pck3 is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype;
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck3;
/

create or replace package body pck3 is
procedure ppp1() is
cursor cur2 is
select col1,col2 from test12;
begin
open cur2;
fetch cur2 into var1;
ppp2(var1);
raise info '%', var1.col1;
end;

procedure ppp2(a cur1%rowtype) is
begin
    raise info '%', a.col1;
end;
end pck3;
/

call pck3.ppp1();

create or replace package pck4 
is 
v1 varchar2; 
procedure proc1(a1 in v1%type);
end pck4;
/

create or replace package body pck4
is 
procedure proc1(a1 in v1%type) 
is 
begin 
raise info '%', a1;
end;
end pck4;
/

call pck4.proc1('aa');

-- test cusor.col
create or replace package pck5 is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype;
var2 cur1.col1%type;
procedure ppppp1(a1 cur1.col1%type);
end pck5;
/

create or replace package body pck5
is 
procedure ppppp1(a1 cur1.col1%type) 
is 
begin 
var2 = 2;
raise info '%', a1;
raise info '%', var2;
end;
end pck5;
/

call pck5.ppppp1(1);

drop schema if exists schema1;
create schema schema1;
set search_path=schema1;
create table t11(a int, b varchar(10));
insert into t11 values (1,'a');

set search_path=plpgsql_cursor_rowtype;

create or replace procedure cursor1()
as 
declare
  c_b varchar(10);
  cursor cur1 is select schema1.t11.* from schema1.t11 where b = c_b;
  var1 cur1%rowtype;
begin 
  c_b = 'a';
  open cur1;
  fetch cur1 into var1;
  raise info '%', var1;
  raise info '%', var1.a;
end;
/

call cursor1();

create or replace package pck6 is
  c_b varchar(10);
  cursor cur1 is select schema1.t11.* from schema1.t11 where b = c_b;
  var1 cur1%rowtype;
procedure p2();
end pck6;
/

create or replace package body pck6
is 
procedure p2()
is 
begin 
  c_b = 'a';
  open cur1;
  fetch cur1 into var1;
  raise info '%', var1;
  raise info '%', var1.a;
end;
end pck6;
/

call pck6.p2();

create table tb1 (c1 int,c2 varchar2);
insert into tb1 values(4,'a');

create or replace package pck7 as 
  cursor cur is select c1,c2 from tb1;
  v_s cur%rowtype := (1,'1');
  function func1(c1 in cur%rowtype) return cur%rowtype;
  procedure proc1(c1 out cur%rowtype);
  procedure proc2(c1 inout cur%rowtype); 
end pck7;
/

create or replace package body pck7 
is
  function func1(c1 in cur%rowtype) return cur%rowtype
  as
  begin
    return v_s;
  end;

  procedure proc1 (c1 out cur%rowtype) 
  as
  begin
    c1 := (4,'d');
  end;

  procedure proc2(c1 inout cur%rowtype)
  is
    vs cur%rowtype := (2,'1');
    c2 cur%rowtype;
  begin
    c1 := func1(vs);
    proc1(c2);
    raise info '%', c2;
  end;
end pck7;
/

call pck7.proc2(row(3,'c'));

-- test duplicate column name
create or replace procedure pro_cursor_args
is
    b varchar(10) := 'job1';
    cursor c_job
       is
       select empno,empno,ename
       from emp
       where job=b;
    c_row c_job%rowtype;
begin
    for c_row in c_job loop
        raise info '%', c_row.empno;
    end loop;
end;
/

call pro_cursor_args();

create or replace package pck8 is
cursor cur1 is select col2,col2 from test12;
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck8;
/

insert into emp values (1, 'zhangsan', 'job3');

create or replace package pck8 is 
vvv emp%rowtype;
cursor cur1 is
select empno,empno,job from emp where empno=vvv.empno and ename=vvv.ename;
emp_row cur1%rowtype;
procedure p1();
end pck8;
/

create or replace package body pck8 is
procedure p1() is
a int;
begin
vvv.empno = 1;
vvv.ename = 'zhangsan';
open cur1;
fetch cur1 into emp_row;
raise info '%', emp_row.job;
fetch cur1 into emp_row;
raise info '%', emp_row.job;
end;
end pck8;
/

call pck8.p1();

create or replace package pck9 is 
vvv emp%rowtype;
cursor cur1 is
select empno,empno,job from emp where empno=vvv.empno and ename=vvv.ename;
emp_row record;
procedure p1();
end pck9;
/

create or replace package body pck9 is
procedure p1() is
a int;
begin
vvv.empno = 1;
vvv.ename = 'zhangsan';
open cur1;
fetch cur1 into emp_row;
raise info '%', emp_row.job;
fetch cur1 into emp_row;
raise info '%', emp_row.job;
end;
end pck9;
/

call pck9.p1();

create or replace package pck10 as 
  cursor cur is select c2,c2 from tb1;
  function func1 return cur%rowtype;
end pck10;
/

create table FOR_LOOP_TEST_001(
deptno smallint,
ename char(100),
salary int
);

create table FOR_LOOP_TEST_002(
deptno smallint,
ename char(100),
salary int
);

insert into FOR_LOOP_TEST_001 values (10,'CLARK',7000),(10,'KING',8000),(10,'MILLER',12000),(20,'ADAMS',5000),(20,'FORD',4000);

create or replace procedure test_forloop_001()
as
begin
  for data in update FOR_LOOP_TEST_001 set salary=20000 where ename='CLARK' returning * loop
    insert into FOR_LOOP_TEST_002 values(data.deptno,data.ename,data.salary);
  end loop;
end;
/

call test_forloop_001();
select * from FOR_LOOP_TEST_001;
select * from FOR_LOOP_TEST_002;

--test execption close cursor 
create or replace package pckg_test1 as
procedure p1;
end pckg_test1;
/

create or replace package body pckg_test1 as
procedure p1() is 
a number;
begin 
a := 2/0;
end;
end pckg_test1;
/

create or replace package pckg_test2 as
cursor CURRR is select * from FOR_LOOP_TEST_002;
curr_row CURRR%rowtype;
procedure p1;
end pckg_test2;
/

create or replace package body pckg_test2 as
procedure p1() is 
a number;
begin 
open CURRR;
fetch CURRR into curr_row;
raise info '%', curr_row;
pckg_test1.p1();
exception 
when others then 
raise notice '%', '1111';
close CURRR;
end;
end pckg_test2;
/

call pckg_test2.p1();

create or replace procedure pro_close_cursor1
is
    my_cursor REFCURSOR;
    sql_stmt VARCHAR2(500);
    curr_row record;
begin
    sql_stmt := 'select * from FOR_LOOP_TEST_002';
    OPEN my_cursor FOR EXECUTE sql_stmt;
    fetch my_cursor into curr_row;
    raise info '%', curr_row;
    pckg_test1.p1();
    exception 
    when others then 
    raise notice '%', '1111';
    close my_cursor;
end;
/

call pro_close_cursor1();

create or replace procedure pro_close_cursor2
is
    type cursor_type is ref cursor;
    my_cursor cursor_type;
    sql_stmt VARCHAR2(500);
    curr_row record;
begin
    sql_stmt := 'select * from FOR_LOOP_TEST_002';
    OPEN my_cursor FOR EXECUTE sql_stmt;
    fetch my_cursor into curr_row;
    raise info '%', curr_row;
    pckg_test1.p1();
    exception 
    when others then 
    raise notice '%', '1111';
    close my_cursor;
end;
/

call pro_close_cursor2();

create table cs_trans_1(a int);
create or replace procedure pro_cs_trans_1() as  
cursor c1 is select * from cs_trans_1 order by 1; 
rec_1 cs_trans_1%rowtype;
va int;
begin  
open c1;   
va := 3/0;
exception  
when division_by_zero then   
close c1;
close c1;
end;
/

call pro_cs_trans_1();

create or replace procedure pro_cs_trans_1() as  
cursor c1 is select * from cs_trans_1 order by 1; 
rec_1 cs_trans_1%rowtype;
va int;
begin  
open c1;
close c1;
va := 3/0;
exception  
when division_by_zero then   
close c1;
end;
/

call pro_cs_trans_1();

create or replace procedure pro_cs_trans_1() as  
cursor c1 is select * from cs_trans_1 order by 1; 
rec_1 cs_trans_1%rowtype;
va int;
begin  
open c1;
close c1;
close c1;
va := 3/0;
close c1;
exception  
when division_by_zero then
null;
when others then
raise info 'cursor alread closed';
end;
/

call pro_cs_trans_1();

drop procedure pro_cs_trans_1;
drop table cs_trans_1; 

-- test for rec in cursor loop
show behavior_compat_options;
create table test_table(col1 varchar2(10));
create or replace package test_pckg as
    procedure test_proc(v01 in varchar2);
end test_pckg;
/


create or replace package body test_pckg as
    procedure test_proc(v01 in varchar2) as
 cursor cur(vcol1 varchar2) is select col1 from test_table where col1 = vcol1;
 v02 varchar2;
 begin
 for rec in cur(v01) loop
 v02 := 'a';
 end loop;
 end;
end test_pckg;
/
drop table test_table;
drop package test_pckg;

-- test for rec in select loop when rec is defined
set behavior_compat_options='proc_implicit_for_loop_variable';
create table t1(a int, b int);
create table t2(a int, b int, c int);
insert into t1 values(1,1);
insert into t1 values(2,2);
insert into t1 values(3,3);
insert into t2 values(1,1,1);
insert into t2 values(2,2,2);
insert into t2 values(3,3,3);

-- (a) definde as record
create or replace package pck_for is
type r1 is record(a int, b int);
temp_result t1;
procedure p1;
end pck_for;
/
create or replace package body pck_for is
procedure p1 as
vb t1;
begin
for temp_result in select * from t2 loop
raise info '%', temp_result;
    for temp_result in select * from t1 loop
    raise info '%', temp_result;
    end loop;
end loop;
raise info 'after loop: %', temp_result;
end;
end pck_for;
/

call pck_for.p1();
drop package pck_for;

set behavior_compat_options='';
set plsql_compile_check_options='for_loop';

-- (b) definde as scarlar
create or replace package pck_for is
temp_result int;
procedure p1;
end pck_for;
/
create or replace package body pck_for is
procedure p1 as
vb t1;
begin
for temp_result in select * from t2 loop
raise info '%', temp_result;
    for temp_result in select * from t1 loop
    raise info '%', temp_result;
    end loop;
end loop;
raise info 'after loop: %', temp_result;
end;
end pck_for;
/

call pck_for.p1();
drop package pck_for;

-- (c) select only one col
create or replace package pck_for is
temp_result int;
procedure p1;
end pck_for;
/
create or replace package body pck_for is
procedure p1 as
vb t1;
begin
for temp_result in select c from t2 loop
raise info '%', temp_result;
    for temp_result in select a from t1 loop
    raise info '%', temp_result;
    end loop;
end loop;
raise info 'after loop: %', temp_result;
end;
end pck_for;
/

call pck_for.p1();
drop package pck_for;

drop table t1;
drop table t2;
set behavior_compat_options='';
set plsql_compile_check_options='';

create or replace procedure check_compile() as
declare
	cursor c1 is select sysdate a;
	v_a varchar2;
begin
	for rec in c1 loop 
	select 'aa' into v_a from sys_dummy where sysdate = rec.a;
	raise info '%' ,v_a;
	end loop;
end;
/

call  check_compile();

set behavior_compat_options='allow_procedure_compile_check';
create or replace procedure check_compile_1() as
declare
	cursor c1 is select sysdate a;
	v_a varchar2;
begin
	for rec in c1 loop 
	select 'aa' into v_a from sys_dummy where sysdate = rec.a;
	raise info '%' ,v_a;
	end loop;
end;
/

call  check_compile_1();
set behavior_compat_options='';

drop procedure check_compile;

--游标依赖row type，后续alter type
create type foo as (a int, b int);

--游标依赖type，alter type报错
begin;
declare c cursor for select (i,2^30)::foo from generate_series(1,10) i;
fetch c;
fetch c;
alter type foo alter attribute b type text;--error
end;

--第二次开始从缓存中获取type
begin;
cursor c for select (i,2^30)::foo from generate_series(1,10) i;
fetch c;
fetch c;
alter type foo alter attribute b type text;--error
end;

--close后，可以成功alter
begin;
declare c cursor for select (i,2^30)::foo from generate_series(1,10) i;
fetch c;
fetch c;
close c;
alter type foo alter attribute b type text;--success
declare c cursor for select (i,2^30)::foo from generate_series(1,10) i;
fetch c;
fetch c;
rollback;

begin;
cursor c for select (i,2^30)::foo from generate_series(1,10) i;
close c;
alter type foo alter attribute b type text;--success
end;

--多个游标依赖，只关闭一个
begin;
cursor c1 for select (i,2^30)::foo from generate_series(1,10) i;
cursor c2 for select (i,2^30)::foo from generate_series(1,10) i;
close c1;
alter type foo alter attribute b type text;--error
end;

--多个游标依赖，都关闭
begin;
cursor c1 for select (i,2^30)::foo from generate_series(1,10) i;
cursor c2 for select (i,2^30)::foo from generate_series(1,10) i;
close c1;
close c2;
alter type foo alter attribute b type text;--success
end;

--WITH HOLD游标，事务结束继续保留
begin;
cursor c3 WITH HOLD for select (i,2^30)::foo from generate_series(1,10) i;
fetch c3;
end;
fetch c3;
alter type foo alter attribute b type text;--success
fetch c3;
close c3;

---- 不在 TRANSACTION Block里的游标声明导致 core的问题
--游标依赖row type，后续alter type
drop type if exists type_cursor_bugfix_0001;
create type type_cursor_bugfix_0001 as (a int, b int);

--游标依赖type，alter type报错
begin;
declare c5 cursor for select (i,2^30)::type_cursor_bugfix_0001 from generate_series(1,10) i;
fetch c5;
fetch c5;
alter type type_cursor_bugfix_0001 alter attribute b type text;--error
end;
/

--close后，可以成功alter
begin;
declare c7 cursor for select (i,2^30)::type_cursor_bugfix_0001 from generate_series(1,10) i;
fetch c7;
fetch c7;
close c7;
alter type type_cursor_bugfix_0001 alter attribute b type text;--success
declare c8 cursor for select (i,2^30)::type_cursor_bugfix_0001 from generate_series(1,10) i;
fetch c8;
fetch c8;
rollback;
/

begin;
cursor c9 for select (i,2^30)::type_cursor_bugfix_0001 from generate_series(1,10) i;
close c9;
alter type type_cursor_bugfix_0001 alter attribute b type text;--success
end;

drop type if exists type_cursor_bugfix_0001;


----  clean  ----
drop package pck1;
drop package pck2;
drop package pck3;
drop package pck4;
drop package pck5;
drop package pck6;
drop package pck7;
drop package pck8;
drop package pck9;
drop package pckg_test1;
drop package pckg_test2;
drop schema plpgsql_cursor_rowtype cascade;
drop schema schema1 cascade;
