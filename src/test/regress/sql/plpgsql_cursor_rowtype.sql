-- test cursor%type
-- check compatibility --
-- create new schema --
drop schema if exists plpgsql_cursor_rowtype;
create schema plpgsql_cursor_rowtype;
set current_schema = plpgsql_cursor_rowtype;

CREATE TABLE test_2 (
    id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50), 
    email VARCHAR2(25) NOT NULL,
    phone_number VARCHAR2(20),
    hire_date DATE NOT NULL
);

INSERT INTO test_2 VALUES (100, 'John', 'Doe','john.doe@example.com', '123-4567-8901', TO_DATE('2000-01-01', 'YYYY-MM-DD'));
INSERT INTO test_2 VALUES (101, 'Jane', 'Smith','jane.smith@example.com', '456-8324-4579', TO_DATE('1999-02-08', 'YYYY-MM-DD'));

DECLARE    
     CURSOR curtest_2(p_id INT , p_first_name VARCHAR2, p_last_name VARCHAR2 DEFAULT 'Doe') IS  
     SELECT id, first_name, last_name, email, phone_number, hire_date  
        FROM test_2   
        WHERE (first_name = p_first_name)  
          AND (last_name = p_last_name)
          AND (id = p_id);  

     v_result curtest_2%ROWTYPE;
BEGIN  
    v_result:=NULL;
    raise notice 'Result: %', v_result;        
END;  
/

DECLARE    
     CURSOR curtest_2(p_id INT , p_first_name VARCHAR2, p_last_name VARCHAR2 DEFAULT 'Doe') IS  
     SELECT id, first_name, last_name, email, phone_number, hire_date  
        FROM test_2   
        WHERE (first_name = p_first_name)  
          AND (last_name = p_last_name)
          AND (id = p_id);  

     v_result curtest_2%ROWTYPE;
BEGIN  
    OPEN curtest_2(100,'John');  
    FETCH curtest_2 INTO v_result;   
    raise notice 'Result: %', v_result;        
    CLOSE curtest_2; 

    v_result:=NULL;
    raise notice 'Result: %', v_result;        
END;  
/

DECLARE    
     CURSOR curtest_2(p_id INT , p_first_name VARCHAR2, p_last_name VARCHAR2 DEFAULT 'Doe') IS  
     SELECT id, first_name, last_name, email, phone_number, hire_date  
        FROM test_2   
        WHERE (first_name = p_first_name)  
          AND (last_name = p_last_name)
          AND (id = p_id);  

     v_result curtest_2%ROWTYPE;
BEGIN
    raise notice 'Result: %', v_result;        
END;  
/

drop table test_2 cascade;

set behavior_compat_options='allow_procedure_compile_check,disable_record_type_in_dml';

create table t_CurRowtype_Def_Case0001(col1 int primary key,col2 varchar(100));
insert into t_CurRowtype_Def_Case0001 values(1,'one');
insert into t_CurRowtype_Def_Case0001 values(2,'two');
insert into t_CurRowtype_Def_Case0001 values(3,'three');
insert into t_CurRowtype_Def_Case0001 values(4,NULL);

--创建游标rowtype，select 伪列;expect:成功,输出4行
declare
   cursor cur_CurRowtype_Def_Case0001_1 is select col1,col2,rownum from t_CurRowtype_Def_Case0001;
   source cur_CurRowtype_Def_Case0001_1%rowtype;
begin
   open cur_CurRowtype_Def_Case0001_1;
   loop
   fetch cur_CurRowtype_Def_Case0001_1 into source;
   exit when cur_CurRowtype_Def_Case0001_1%notfound;
      raise notice '% , % ,% ',source.col1,source.col2,source.rownum;  
   end loop;
   close cur_CurRowtype_Def_Case0001_1; 
end;
/

drop table t_CurRowtype_Def_Case0001;

create table t_CurRowtype_Def_Case0002(col1 int primary key,rownum varchar(100));
create table t_CurRowtype_Def_Case0002(col1 int primary key);
insert into t_CurRowtype_Def_Case0002 values(1);
select t_CurRowtype_Def_Case0002.rownum from t_CurRowtype_Def_Case0002;
select rownum from t_CurRowtype_Def_Case0002;
drop table t_CurRowtype_Def_Case0002;

CREATE TYPE type_record AS (
	first text,
	rownum int4
) ;

DECLARE 
    type rectype is record(rownum int,row2 text);
    source rectype := (2, 'dsaw');
BEGIN
    raise notice '% , %',source.row2,source.rownum;
END;
/

-- Prohibit virtual column insertion
create table t1(col1 varchar(10),col varchar(10));
create table t2(col1 varchar(10),col varchar(10));
insert into t1 values('one','two');
declare
  cursor cur1 is select * from t1;
  source cur1%rowtype:=('ten','wtu');
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values(source.col1, source.col);
  end loop; 
end;
/

insert into t1 values('one','two');
declare
  cursor cur1 is select * from t1;
  source cur1%rowtype:=('ten','wtu');
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values(source);
  end loop; 
end;
/
select * from t2;
drop table t1;
drop table t2;

create table t1 (a int);
create table t2 (a t1);
declare
  source t2%rowtype;
begin
  insert into t2 values(source.a);
end;
/

declare
  source t2%rowtype;
begin
  update t2 set a = source;
end;
/

declare
  source t2%rowtype;
begin
  update t2 set a = source.a;
end;
/

drop table t2;
drop table t1;

-- Prohibit virtual column insertion
create table t1(col1 varchar(10), col2 int, col3 varchar(10), col4 varchar(10));
insert into t1 values('one',5,'dsa','e');
insert into t1 values('two',7,'daw','d');
insert into t1 values('three',7,'dsaw','sw');
insert into t1 values(NULL);

create table t2(col1 varchar(10), col2 int, col3 varchar(10), col4 varchar(10));

declare
  cursor cur1 is select * from t1;
  source cur1%rowtype;
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values('o', 5, source.col4, source.col1);
  end loop; 
end;
/

declare
  cursor cur1 is select * from t1;
  source cur1%rowtype;
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values('o', 5, source.col4, source);
  end loop; 
end;
/

declare
  cursor cur1 is select * from t1;
  source cur1%rowtype;
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values('o', 5, source, source.col1);
  end loop; 
end;
/

declare
  cursor cur1 is select * from t1;
  source cur1%rowtype;
begin
  for source in cur1
  loop
    raise notice '%',source;
    insert into t2 values(source);
  end loop; 
end;
/
select * from t2;
drop table t1;
drop table t2;

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

-- test change cxt
drop table if exists t_CurRowtype_Use_Case0007_1;
create table t_CurRowtype_Use_Case0007_1(col1 varchar(30),col2 varchar(30));
insert into t_CurRowtype_Use_Case0007_1 values ('col1_a', 'col2_aa');
insert into t_CurRowtype_Use_Case0007_1 values ('col1_b', 'col2_bb');

drop table if exists t_CurRowtype_Use_Case0007_2;
create table t_CurRowtype_Use_Case0007_2(col1 varchar(30),col2 varchar(30));

create or replace package pac_CurRowtype_Use_Case0007_5 is
cursor cur1 is select col1,col2 from t_CurRowtype_Use_Case0007_1;
var1 cur1%rowtype;
procedure p_CurRowtype_Use_Case0007_5(a cur1%rowtype);
end pac_CurRowtype_Use_Case0007_5;
/
create or replace package body pac_CurRowtype_Use_Case0007_5 is
procedure p_CurRowtype_Use_Case0007_5(a cur1%rowtype) is
begin
  var1.col1:='pack5_proc_col1';
  var1.col2:='pack5_proc_col2';
  insert into t_CurRowtype_Use_Case0007_2 values(var1.col1,var1.col2);
end;
end pac_CurRowtype_Use_Case0007_5;
/

create or replace package pac_CurRowtype_Use_Case0007_6 is
cursor cur1 is select col1,col2 from t_CurRowtype_Use_Case0007_1;
var1 cur1%rowtype;
procedure p_CurRowtype_Use_Case0007_6;
end pac_CurRowtype_Use_Case0007_6;
/
create or replace package body pac_CurRowtype_Use_Case0007_6 is
procedure p_CurRowtype_Use_Case0007_6 is
begin
  open cur1;
  fetch cur1 into var1;
  pac_CurRowtype_Use_Case0007_5.p_CurRowtype_Use_Case0007_5(var1);
end;
end pac_CurRowtype_Use_Case0007_6;
/

call pac_CurRowtype_Use_Case0007_6.p_CurRowtype_Use_Case0007_6();
drop table if exists t_CurRowtype_Use_Case0007_1;
drop table if exists t_CurRowtype_Use_Case0007_2;
drop package pac_CurRowtype_Use_Case0007_5;
drop package pac_CurRowtype_Use_Case0007_6;

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

-- test: max len
drop table if exists t1;
create table t1(col1 tinyint primary key,col2 varchar(10));

declare
  cursor case1 is select * from t1;
  source case1%rowtype:=(200,'abcdeabcedone');
begin
  raise notice '% , %',source.col1,source.col2;
end;
/

declare
  cursor case1 is select * from t1;
  source case1%rowtype;
begin
  source:=(200,'abcdeabcedone');
  raise notice '% , %',source.col1,source.col2;
end;
/
drop table if exists t1;
-- test:pkg head max len
create table test12(col1 varchar(10), col2 varchar(10));
insert into test12 values ('a', 'aa');
insert into test12 values ('a', 'aa');
insert into test12 values ('b', 'bb');
create table test22(col1 varchar2, col2 varchar2);
insert into test22 values ('dsasdad6sad','d6sasdadsad');

create or replace package pck3p is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype:=('dsasdad6sad','d6sasdadsad');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck3p;
/

create or replace package body pck3p is
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
    a.col1:='dsasdadsad';
    raise info '%', a.col1;
end;
end pck3p;
/

call pck3p.ppp1();

-- test:pkg body max len
create or replace package pck3p is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype:=('GJHGH','TYUTD');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck3p;
/

create or replace package body pck3p is
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
    a.col1:='dsasdaGJHGdsad';
    raise info '%', a.col1;
end;
end pck3p;
/

call pck3p.ppp1();

-- test:cursor fetch max len
create or replace package pck3p is
cursor cur1 is select col1,col2 from test12;
var1 cur1%rowtype:=('GJHGH','TYUTD');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck3p;
/

create or replace package body pck3p is
procedure ppp1() is
cursor cur2 is
select col1,col2 from test22;
begin
open cur2;
fetch cur2 into var1;
ppp2(var1);
raise info '%', var1.col1;
end;

procedure ppp2(a cur1%rowtype) is
begin
    a.col1:='dsasdaGJHGdsad';
    raise info '%', a.col1;
end;
end pck3p;
/

call pck3p.ppp1();
drop package pck3p;
drop table test12;
drop table test22;

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

create table int_4(a NUMBER, b VARCHAR2(5));
insert into int_4(a) values(3,'johan');
create or replace package pck3_1 is
cursor cur1 is select a,b from int_4;
var1 cur1%rowtype:=(3, 'ada');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck3_1;
/

create or replace package body pck3_1 is
procedure ppp1() is
cursor cur2 is
select a,b from int_4;
begin
var1.a:=var1.a + 5;
raise info '(ppp1)var1: %', var1.a;
ppp2(var1);
raise info '(ppp1)var1: %', var1.a;
end;

procedure ppp2(a cur1%rowtype) is
begin
    var1.a:=8+var1.a;
    a.a := a.a + 2;
	raise info '[ppp2]a: %', a.a;
	raise info '[ppp2]var1: %', var1.a;
	var1.a:=8+var1.a;
    raise info '[ppp2]a: %', a.a;
	raise info '[ppp2]var1: %', var1.a;
end;
end pck3_1;
/

call pck3_1.ppp1();
call pck3_1.ppp1();
call pck3_1.ppp1();

CREATE TABLE STORAGE_LARGE_TABLE_STORAGE_TABLE_000 (c_id int,
c_d_id int NOT NULL,
c_w_id int NOT NULL,
c_first varchar(16) NOT NULL,
c_middle char(2),
c_last varchar(16) NOT NULL,
c_street_1 varchar(20) NOT NULL,
c_street_2 varchar(20),
c_city varchar(20) NOT NULL,
c_state char(2) NOT NULL,
c_zip char(9) NOT NULL,
c_phone char(16) NOT NULL,
c_since timestamp,
c_credit char(2) NOT NULL,
c_credit_lim numeric(12,2),
c_discount numeric(4,4),
c_balance numeric(12,2),
c_ytd_payment numeric(12,2) NOT NULL,
c_payment_cnt int NOT NULL,
c_delivery_cnt int NOT NULL,
c_data varchar(500) NOT NULL);

CREATE TABLE STORAGE_LARGE_CURSOR_TABLE_216 AS SELECT * FROM STORAGE_LARGE_TABLE_STORAGE_TABLE_000 WHERE C_ID=0;

declare
temp integer := 0;
id_temp integer;
name_temp text;
begin
for temp in 0..99 loop
INSERT INTO STORAGE_LARGE_CURSOR_TABLE_216 VALUES (temp,temp,temp,'iscmvlstpn','OE','BARBARBAR','bkilipzfcxcle','pmbwodmpvhvpafbj','dyfaoptppzjcgjrvyqa','uq',480211111,9400872216162535,null,'GC',50000.0,0.4361328,-10.0,10.0,1,0,'QVLDETANRBRBURBMZQUJSHOQNGGSMNTECCIPRIIRDHIRWIYNPFZCSYKXXYSCDSF');
end loop;
end;
/

Declare
Type MyRefCur IS Ref Cursor RETURN STORAGE_LARGE_CURSOR_TABLE_216%ROWTYPE;
c1 MyRefCur;
temp c1%RowType;
Begin
Open c1 For Select * from STORAGE_LARGE_CURSOR_TABLE_216 ORDER BY C_ID;
LOOP
FETCH C1 INTO temp;
EXIT WHEN C1%NOTFOUND;
raise info 'str1 is %', temp.C_id;
END LOOP;
Close c1;
End;
/

drop table STORAGE_LARGE_TABLE_STORAGE_TABLE_000;
drop table STORAGE_LARGE_CURSOR_TABLE_216;
-- test none execute error sql
set behavior_compat_options='';
create table t_CurRowtype_Def_Case0001_1(
col1 tinyint primary key,
col2 smallint,
col3 int,
col4 bigint
);

declare
   cursor cur_CurRowtype_Def_Case0003_1 is select * from t_CurRowtype_Def_Case0001_1;
   source cur_CurRowtype_Def_Case0003_1%rowtype;
begin
   open cur_CurRowtype_Def_Case0003_1;
   loop
   fetch cur_CurRowtype_Def_Case0003_1 into source;
   exit when cur_CurRowtype_Def_Case0003_1%notfound;
       raise notice '% , %',source.col1,source.col5;  
   end loop;
   close cur_CurRowtype_Def_Case0003_1; 
end;
/

set behavior_compat_options='allow_procedure_compile_check';
declare
   cursor cur_CurRowtype_Def_Case0003_1 is select * from t_CurRowtype_Def_Case0001_1;
   source cur_CurRowtype_Def_Case0003_1%rowtype;
begin
   open cur_CurRowtype_Def_Case0003_1;
   loop
   fetch cur_CurRowtype_Def_Case0003_1 into source;
   exit when cur_CurRowtype_Def_Case0003_1%notfound;
       raise notice '% , %',source.col1,source.col5;  
   end loop;
   close cur_CurRowtype_Def_Case0003_1; 
end;
/
drop table t_CurRowtype_Def_Case0001_1;

--test: drop column
create table int_4_2(a NUMBER, d NUMBER, b VARCHAR2(5));
insert into int_4_2(a, d, b) values(3, 6,'johan');

create or replace package pck32 is
cursor cur1 is select * from int_4;
var1 cur1%rowtype:=(3, 'ada');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck32;
/

create or replace package body pck32 is
procedure ppp1() is
cursor cur2 is select * from int_4_2;
begin
open cur2;
fetch cur2 into var1;
ppp2(var1);
raise info '%', var1.a;
end;

procedure ppp2(a cur1%rowtype) is
begin
    raise info '%', a.a;
end;
end pck32;
/

ALTER TABLE int_4_2 DROP COLUMN d;
call pck32.ppp1();
drop table int_4_2 cascade;

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

create or replace package pck31 is
cursor cur1 is select a,b from int_4;
var1 cur1%rowtype:=(3, 'ada');
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck31;
/

create or replace package body pck31 is
procedure ppp1() is
cursor cur2 is
select col1,col2 from test12;
begin
open cur2;
fetch cur2 into var1;
ppp2(var1);
raise info '%', var1.a;
end;

procedure ppp2(a cur1%rowtype) is
begin
    raise info '%', a.a;
end;
end pck31;
/

call pck31.ppp1();

create or replace package pck12 is
cursor cur1 is select a from int_4;
var1 cur1%rowtype:=(6);
procedure ppp1;
procedure ppp2(a cur1%rowtype);
end pck12;
/
create or replace package body pck12 is
procedure ppp1() is
cursor cur2 is
select a,b from int_4;
begin
var1.a:=var1.a + 5;
raise info '(ppp1)var1: %', var1.a;
ppp2(var1);
raise info '(ppp1)var1: %', var1.a;
end;

procedure ppp2(a cur1%rowtype) is
begin
    var1.a:=8+var1.a;
    a.a := a.a + 2;
	raise info '[ppp2]a: %', a.a;
	raise info '[ppp2]var1: %', var1.a;
	var1.a:=8+var1.a;
    raise info '[ppp2]a: %', a.a;
	raise info '[ppp2]var1: %', var1.a;
end;
end pck12;
/

call pck12.ppp1();
drop table int_4 cascade;

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

create table numeric_test(col1 numeric(10,3));
insert into numeric_test values(100.1111); 

declare
   cursor cur1 is select * from numeric_test;
   source cur1%rowtype := (100.2345);    
begin
   raise info 'col1 : %',source.col1;
   insert into numeric_test values (source.col1);
end;
/
drop table numeric_test;

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

create table t_Compare_Case0013(id int,first_name varchar(100), last_name varchar(100));
create table t_CurRowtype_PLObject_Case0013(first_name varchar(100), last_name varchar(100));
insert into t_CurRowtype_PLObject_Case0013 values('Jason','Statham');

create or replace function f_CurRowtype_PLObject_Case0013() returns trigger as
$$
declare
  cursor cur_1 is select * from t_CurRowtype_PLObject_Case0013;
  source cur_1%rowtype;
begin
   source.first_name:=new.first_name;
   source.last_name:=new.last_name;      
   insert into t_Compare_Case0013 values (source.first_name,source.last_name);
   return new;
end
$$ language plpgsql;

drop function f_CurRowtype_PLObject_Case0013;
drop table t_CurRowtype_PLObject_Case0013;
drop table t_Compare_Case0013;

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

drop table test1;
drop table test2;

create table test1(id int, name varchar, job varchar);
create table test2(id int, age int);
insert into test1 values (1, 'zhang', 'worker'),(2, 'li', 'teacher'),(3, 'wang', 'engineer');
insert into test2 values (1, 20),(2, 30),(3, 40);

DECLARE 
  CURSOR c1 IS SELECT t.age, CURSOR(SELECT name FROM test1 t1 where t1.id = t.id) abc FROM test2 t;-- 在匿名块中使用游标表达式样例
  age_temp int;
  name_temp varchar;
  type emp_cur_type is ref cursor;
  c2 emp_cur_type;
  source c1%rowtype;
BEGIN
  OPEN c1;
  loop
    fetch c1 into source;
    exit when c1%notfound;
    raise notice '%',source;
  end loop;
  close c1;
END;
/

drop table test1;
drop table test2;

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
drop procedure check_compile_1;
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

drop type if exists foo;
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

create schema cursor_rowtype;
set current_schema=cursor_rowtype;

-- ==========================================================
-- not open precompile
-- ANONYMOUS BLOCK: If table does not exist -----error
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  open c;
  fetch c into source;
  close c;
END;
/

-- FUNC: If table does not exist
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2); -----error

create table employees(oid int, first_name varchar(20), last_name varchar(20));
create table jointest(oid int, jname varchar(20));
insert into jointest(oid, jname) values (1,'jj');
-- ==========================================================
-- if table exist data
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');

-- PKG: normal
CREATE OR REPLACE PACKAGE emp_bonus IS
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
PROCEDURE testpro1(var3 int);
END emp_bonus;
/

create or replace package body emp_bonus is
var4 int:=4;
procedure testpro1(var3 int)
is
begin
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
end;
end emp_bonus;
/

call emp_bonus.testpro1(1);

-- ANONYMOUS BLOCK: assign record
DECLARE
  TYPE name_rec IS RECORD (
    last1   jointest.jname%TYPE DEFAULT 'Doe',
    first1  employees.first_name%TYPE DEFAULT 'John'
  );

  CURSOR c IS
    SELECT jname,first_name
    FROM employees join jointest on employees.oid = jointest.oid;
  
  t1 name_rec;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.jname := 'Smith';
  t1 := source;
  raise notice '%', t1.last1;
  raise notice '%', t1.first1;
  open c;
  fetch c into source;
  t1 := source;
  raise notice '%', t1.last1;
  raise notice '%', t1.first1;
  close c;
END;
/

-- ANONYMOUS BLOCK: insert data
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.oid := 6; source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  insert into employees(oid, first_name, last_name) values (source.oid, source.first_name, source.last_name);
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: insert data -- error
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.oid := 6; source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  insert into employees(oid, first_name, last_name) values (source);
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale before open
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale after open
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  fetch c into source;
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale after fetch
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  close c;
END;
/

-- ANONYMOUS BLOCK: If the column does not exist -----error
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  close c;
END;
/

-- FUNC: normale
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2);

-- FUNC: If the column does not exist
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2); -----error

-- FUNC: If change table struct
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

drop table employees;
create table employees(a varchar(20),b int);
insert into employees(a,b) values ('johan',22);
call f1(2); -----error
drop table employees;
call f1(2); -----error
create table employees(oid int, first_name varchar(20), last_name varchar(20));
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');
call f1(2);
delete from employees;
call f1(2);

-- ==========================================================
-- if table does not exist data
delete from employees;

-- ANONYMOUS BLOCK: assign record
DECLARE
  TYPE name_rec IS RECORD (
    last1   jointest.jname%TYPE DEFAULT 'Doe',
    first1  employees.first_name%TYPE DEFAULT 'John'
  );

  CURSOR c IS
    SELECT jname,first_name
    FROM employees join jointest on employees.oid = jointest.oid;
  
  t1 name_rec;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.jname := 'Smith';
  t1 := source;
  raise notice '%', t1.last1;
  raise notice '%', t1.first1;
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: insert data
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.oid := 6; source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  insert into employees(oid, first_name, last_name) values (source.oid, source.first_name, source.last_name);
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: insert data -- error
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.oid := 6; source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  insert into employees(oid, first_name, last_name) values (source);
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale before open
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  open c;
  fetch c into source;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale after open
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  fetch c into source;
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  close c;
END;
/

-- ANONYMOUS BLOCK: normale after fetch
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  close c;
END;
/

-- ANONYMOUS BLOCK: If the column does not exist -----error
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  close c;
END;
/

-- FUNC: normale
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2);

-- FUNC: If the column does not exist
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2); -----error

-- FUNC: If change table struct
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

drop table employees;
create table employees(a varchar(20),b int);
insert into employees(a,b) values ('johan',22);
call f1(2); -----error
drop table employees;
call f1(2); -----error
create table employees(oid int, first_name varchar(20), last_name varchar(20));
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');
call f1(2);
delete from employees;
call f1(2);
drop table employees;

-- ==========================================================
-- open precompile
set behavior_compat_options='allow_procedure_compile_check';

-- FUNC: If table does not exist -----error
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

create table employees(oid int, first_name varchar(20), last_name varchar(20));
-- ==========================================================
-- if table exist data
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');

-- FUNC: normale
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2);

-- FUNC: If the column does not exist -----error
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

-- FUNC: If change table struct
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

drop table employees;
create table employees(a varchar(20),b int);
insert into employees(a,b) values ('johan',22);
call f1(2); -----error
drop table employees;
call f1(2); -----error
create table employees(oid int, first_name varchar(20), last_name varchar(20));
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');
call f1(2);
delete from employees;
call f1(2);

-- ==========================================================
-- if table does not exist data
delete from employees;

-- FUNC: normale
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2);

-- FUNC: If the column does not exist -----error
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;
call f1(2);

-- FUNC: If change table struct
create or replace function f1(b int) returns int
as $$
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
  return b;
END;
$$language plpgsql;

drop table employees;
create table employees(a varchar(20),b int);
insert into employees(a,b) values ('johan',22);
call f1(2); -----error
drop table employees;
call f1(2); -----error
create table employees(oid int, first_name varchar(20), last_name varchar(20));
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');
call f1(2);
delete from employees;
call f1(2);

DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE:= (1,NULL,2,4);
BEGIN
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
END;
/

DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE:= (1,NULL,'A');
BEGIN
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
END;
/

DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE:= (1,'a','B');
BEGIN
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
END;
/

DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE:= (1,'a');
BEGIN
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
END;
/

DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE:= ('DASDAS','a','DAS');
BEGIN
  raise notice '%', source.oid;
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
END;
/

-- PROC: normale
create or replace PROCEDURE p1(b int) is
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
END;
/
call p1(2);

-- PROC: If the column does not exist
create or replace PROCEDURE P1(b int) IS
DECLARE
  CURSOR c IS
    SELECT first_name,last_name
    FROM employees;
  source c%ROWTYPE;
BEGIN
  source.first_name := 'Jane'; source.last_name := 'Smith';
  raise notice '%', source.last_name;
  raise notice '%', source.first_name;
  source.oid := 5;
  raise notice '%', source.oid;
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
END;
/
call P1(2); -----error

-- PROC: If change table struct
create or replace function f1(b int) is
DECLARE
  CURSOR c IS
    SELECT *
    FROM employees;
  source c%ROWTYPE;
BEGIN
  open c;
  fetch c into source;
  source.first_name := 'Jane';
  raise notice '%', source.first_name;
  close c;
END;
/
drop table employees;
create table employees(a varchar(20),b int);
insert into employees(a,b) values ('johan',22);
call f1(2); -----error
drop table employees;
call f1(2); -----error
create table employees(oid int, first_name varchar(20), last_name varchar(20));
insert into employees(oid, first_name, last_name) values (1,'johan','mikumiku');
call f1(2);
delete from employees;
call f1(2);

set current_schema=public;
drop schema cursor_rowtype cascade;
