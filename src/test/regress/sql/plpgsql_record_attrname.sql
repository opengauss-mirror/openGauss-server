-- FOR PL/pgSQL ARRAY of RECORD TYPE scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_record;
create schema plpgsql_record;
set search_path=plpgsql_record;



-- initialize table and type--
CREATE TABLE DCT_DATACLR_LOG(TYPE int NOT NULL ENABLE);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- test TYPE as a table col name
create or replace package p_test1 as
    TYPE IN_CLEANLOG_TYPE IS RECORD(IN_TYPE DCT_DATACLR_LOG.TYPE%TYPE);
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE;
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE as
        va IN_CLEANLOG_TYPE;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;	
end p_test1;
/

select p_test1.f1(ROW(3));

-- test TYPE as a col name of record
create or replace package p_test1 as
    TYPE IN_CLEANLOG_TYPE IS RECORD(TYPE int);
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE;
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE as
        va IN_CLEANLOG_TYPE;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;	
end p_test1;
/

select p_test1.f1(ROW(3));

--test RECORD col name of exist type and var name
create or replace package p_test2 is
    type array_type is varray(10) of int;
    type tab_type is table of int;
    type r_type is record (a int, b int);
    va array_type;
    vb tab_type;
    vc r_type;
    type IN_CLEANLOG_TYPE is record (array_type int, tab_type int, r_type int, va int, vb int, vc int);
    function f1(ss in IN_CLEANLOG_TYPE) return int;
end p_test2;
/
create or replace package body p_test2 as
    function f1(ss in IN_CLEANLOG_TYPE) return int as
        vaa IN_CLEANLOG_TYPE;
	begin
        vaa := ss;
        raise info '%',vaa;
        return vaa.va;
	end;	
end p_test2;
/

select p_test2.f1((1,2,3,4,5,6));

--test not null
CREATE OR REPLACE FUNCTION regress_record1(p_w VARCHAR2)
RETURNS
VARCHAR2 AS $$
DECLARE
type rec_type is record (name varchar2(100) not null default 'a', epno int);
employer rec_type;
BEGIN
employer.name := null;
employer.epno = 18;
raise info 'employer name: % , epno:%', employer.name, employer.epno;
return employer.name;
END;
$$
LANGUAGE plpgsql;
CALL regress_record1('aaa');
DROP FUNCTION regress_record1;

--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop package p_test2;
drop package p_test1;

drop table if exists aa;
create table aa(a int, b varchar(5));
insert into aa(a,b) values (3,'aaa');

declare 
 type ty_record is record (a int, b varchar(5)); 
 type ty1 is ref cursor return ty_record; 
 my_cur ty1; 
 var ty_record; 
begin 
 open my_cur for SELECT * FROM aa;
 loop 
 Fetch my_cur InTo var; 
 Exit When my_cur%NotFound; 
 raise info '%',var; 
 end loop; 
 close my_cur; 
end; 
/

create or replace function f1() returns int
as $$
DECLARE
 type ty_record is record (a int, b varchar(5)); 
 type ty1 is ref cursor return ty_record; 
 my_cur ty1; 
 var ty_record; 
BEGIN
 open my_cur for SELECT * FROM aa;
 loop 
 Fetch my_cur InTo var; 
 Exit When my_cur%NotFound; 
 raise info '%',var; 
 end loop; 
 close my_cur; 
 return var.a;
END;
$$language plpgsql;
call f1();
drop function f1;

create or replace PROCEDURE p1() is
DECLARE
 type ty_record is record (a int, b varchar(5)); 
 type ty1 is ref cursor return ty_record; 
 my_cur ty1; 
 var ty_record; 
BEGIN
 open my_cur for SELECT * FROM aa;
 loop 
 Fetch my_cur InTo var; 
 Exit When my_cur%NotFound; 
 raise info '%',var; 
 end loop; 
 close my_cur; 
END;
/
call p1();
drop PROCEDURE p1;
drop table aa;

drop table if exists user_table cascade;

create table user_table 
( 
user_id number(10,0), 
ca_card VARCHAR(60), 
dept_id number, 
is_del number 
); 
insert into user_table(user_id,ca_card,dept_id,is_del) values(1,'card1',101,1); 
insert into user_table(user_id,ca_card,dept_id,is_del) values(2,'card2',102,0); 
insert into user_table(user_id,ca_card,dept_id,is_del) values(3,'card3',103,1);

create or replace type nationalstringarray is table of nvarchar2(2000);
create or replace type key_value_array_t is object (key varchar2(1024),value_array nationalstringarray);
create or replace type key_value_array_tab_t is table of key_value_array_t ;

create or replace package pk4 is
type t_record is record (key varchar2(100), na_val nationalstringarray);
type t_record_cur is ref cursor return t_record ;
function key_vatt return key_value_array_tab_t ;
procedure test(i integer);
end pk4;
/

create or replace package body pk4 is
function key_vatt  return  key_value_array_tab_t is
  vaary nationalstringarray := nationalstringarray();
  kvat key_value_array_t;
  vatt key_value_array_tab_t :=key_value_array_tab_t();
begin
    vaary.extend();
    vaary(1) := 'abc100';
    vaary.extend();
    vaary(2) := 'abc200';
    vaary.extend();
    vaary(3) := 'abc300';
    kvat :=key_value_array_t('keys_string_1',vaary);
    vatt.extend();
    vatt(1) := kvat;
    vaary.extend();
    vaary(4) := 'ABC100';
    vaary.extend();
    vaary(5) := 'ABC200';
    vaary.extend();
    vaary(6) := 'ABC300';
   kvat :=key_value_array_t('keys_string_2',vaary);
    vatt.extend();
   vatt(2) := kvat;
return vatt;
end ;
procedure  test(i integer) as
  t1 t_record;
  v_cur t_record_cur; 
  begin
    open v_cur for select * from table(key_vatt()) ;
    loop
    Fetch v_cur InTo t1;
    Exit When v_cur%NotFound;
    raise info '%',t1;
    end loop;
    close v_cur;
  end;
end pk4;
/

--step1: 在匿名块中使用record嵌套表
declare 
 type ty_record is record (key varchar2(100), na_val nationalstringarray); 
 type ty1 is ref cursor return ty_record; 
 my_cur ty1; 
 var ty_record; 
begin 
 open my_cur for select * from table(pk4.key_vatt()); 
 loop 
 Fetch my_cur InTo var; 
 Exit When my_cur%NotFound; 
 raise info '%',var; 
 end loop; 
 close my_cur; 
end; 
/

--step2:环境清理
drop type key_value_array_tab_t cascade;
drop type key_value_array_t cascade; 
drop type nationalstringarray cascade; 
drop procedure plpgsql_record.test;
drop package pk4;
drop table if exists user_table cascade;

-- clean up --
drop schema if exists plpgsql_record cascade;
