-- test create type table of 
-- check compatibility --
-- create new schema --
drop schema if exists plpgsql_table_opengauss;
create schema plpgsql_table_opengauss;
set current_schema = plpgsql_table_opengauss;

create type parms as table of varchar2(4000);

create type string_agg_type as
(
total varchar2(4000)
);

CREATE OR REPLACE FUNCTION sfunc(state string_agg_type, value parms)
return string_agg_type
is
l_delimiter varchar2(30) := ',';
begin
if (value.count = 2)
then
l_delimiter := value(2);
end if;
state.total := state.total || l_delimiter || value(1);
return state;
end;
/

CREATE OR REPLACE FUNCTION ffunc(state string_agg_type)
return varchar2
is
begin
return ltrim(state.total,',');
end;
/

CREATE AGGREGATE stragg ( parms ) (
SFUNC = sfunc,
STYPE = string_agg_type,
FINALFUNC = ffunc
);

DROP AGGREGATE stragg( parms );
DROP FUNCTION ffunc(string_agg_type);
DROP FUNCTION sfunc(string_agg_type, parms);
DROP type string_agg_type;
drop type parms;

--test inout param
CREATE TABLE INT8_TBL(q1 int8, q2 int8);
create view tt17v as select * from int8_tbl i where i in (values(i));
select * from tt17v order by 1,2;

create type s_type as (
	id integer,
	name varchar,
	addr text
);

create type typeA as table of s_type;
create type typeB as table of s_type.id%type;
create type typeC as table of s_type.name%type;
create type typeD as table of varchar(100);

-- test table of nest table of  error
create type typeF as table of typeD;
-- don't support alter attr
alter type typeA ADD ATTRIBUTE a int;

-- test type nest table of 
create type type1 as table of varchar(10);
create type type2 as (c1 type1);
declare
    a type2;
 begin
	a.c1(1) = ('aaa');
    a.c1(2) = ('bbb');
    RAISE INFO 'a.c1: %' ,a.c1;
end;
/

CREATE TYPE type3 as (a varchar2(1000),b varchar2(1000));
CREATE TYPE type4 AS TABLE OF type3;
CREATE TYPE type5 as (c1 varchar2(1000),c2 varchar2(1000), c3 type4);
declare
    a5 type5;
 begin
	a5.c1 = 'aaa';
    a5.c3(1) = ('1','2');
    a5.c3(2) = ('11','21');
    RAISE INFO 'a.c1: %' ,a5.c3[1];
end;
/

-- test record nest table of
create table tycod01(c1 int[],c2 int);
insert into tycod01 values(array[1],1);
create type tycod02 as(c1 int,c2 tycod01%rowtype);

create table tycod03(c1 int[],c2 tycod02,c3 tycod01);
insert into tycod03 values (array[3],(3,(array[3],3)),(array[3],3));

create type tycode23 is table of tycod03.c3%type;

create or replace procedure  recordnes23()
is
type tycode01 is table of varchar(20) index by varchar(20);
type tycode02 is record (c1 tycode01,c2 int,c3 tycode23);
tycode001 tycode02;
begin
tycode001.c1('aa'):=('22','33','44');
tycode001.c1('bb'):=array['2222'];
tycode001.c2:=2222;
tycode001.c3(1):=(array[1],3);
raise info 'tycode001.c1 is %,tycode001.c2 is %,tycode001.c3 is %', tycode001.c1,tycode001.c2,tycode001.c3;
end;
/

call recordnes23();

--  test in paramter
create or replace procedure tableof_1(a typeA)
is
	
begin
	RAISE INFO 'a(1): %' ,a(1);
	a(1) = (2, 'lisi', 'beijing');
	a(2) = (3, 'zahngwu', 'chengdu');
end;
/

create or replace procedure tableof_2()
is
	a typeA;
begin
	a(1) = (1, 'zhangsan', 'shanghai');
	RAISE INFO 'before call a(1): %' ,a(1);
	perform tableof_1(a);
	RAISE INFO 'after call a(2): %' ,a(2);
end;
/

call tableof_2();

-- don't support create type = ()
create or replace procedure tableof_3
 is
    aa typeA = typeA();
 begin
	RAISE INFO '%' ,aa;
end;
/

call tableof_3();

-- test return 
create or replace function tableof_4()
  return typeA as  
	a typeA;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	return a;
end;
/

select tableof_4();

create or replace function tableof_4()
  return typeA as  
	a typeA;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	return a;
end;
/

select tableof_4();

create or replace function tableof_5()
  return typeA as  
	a typeA;
	b typeA;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	b = a;
	b(2) = (2, 'zahngwu', 'chengdu');
	RAISE INFO 'a:%' ,a;
	return b;
end;
/

select tableof_5();

-- test cast 
create or replace function tableof_6()
  return typeC as  
	a typeA;
	b typeC;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	b = a;
	b(2) = (2, 'zahngwu', 'chengdu');
	RAISE INFO 'a:%' ,a;
	return b;
end;
/

select tableof_6();

--test return wrong type
create or replace function tableof_7()
  return typeB as  
	a typeA;
	b typeC;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	b = a;
	b(2) = (2, 'zahngwu', 'chengdu');
	RAISE INFO 'a:%' ,a;
	return b;
end;
/

select tableof_7();

-- add one column from s_type
create type s_type_extend as (
	id integer,
	name varchar,
	addr text,
	comment varchar
);

create type typeA_ext as table of s_type_extend;

create or replace function tableof_8()
  return typeA_ext as  
	a typeA;
	b typeA_ext;
 begin
	a(1) = (1, 'lisi', 'beijing'); 
	b = a;
	b(2) = (2, 'zahngwu', 'chengdu','good');
	RAISE INFO 'a:%' ,a;
	return b;
end;
/

select tableof_8();

-- test return index
create or replace function tableof_9()
  return typeA as  
	a typeA;
 begin
	a(-1) = (1, 'lisi', 'beijing'); 
	a(2) = (2, 'zahngwu', 'chengdu');
	return a;
end;
/

select tableof_9();

create or replace procedure tableof_10()
 as  
	a typeA;
 begin
	a = tableof_9();
	RAISE INFO 'a(-1):%' ,a(-1);
	RAISE INFO 'a(0):%' ,a(0);
	RAISE INFO 'a(2):%' ,a(2).id;
end;
/

call tableof_10();

create or replace procedure tableof_11()
 as  
	a typeA;
 begin
	a = tableof_9();
	RAISE INFO 'a(-1):%' ,a(-1);
end;
/

call tableof_11();

-- test index by
create or replace procedure tableof_12
 is
    TYPE SalTabTyp is TABLE OF varchar(10) index by BINARY_INTEGER;  
	aa SalTabTyp;
 begin
	aa('aa') = 1;
	aa('bb') = 2;
	RAISE INFO '%' ,aa('aa');
	RAISE INFO '%' ,aa('bb');
end;
/

call tableof_12();

create or replace procedure tableof_13
 is
    TYPE SalTabTyp is TABLE OF integer index by varchar(10);  
	aa SalTabTyp;
 begin
	aa('aa') = 1;
	aa('bb') = 2;
	RAISE INFO '%' ,aa(0);
	RAISE INFO '%' ,aa('bb');
end;
/

call tableof_13();

create or replace procedure tableof_14
 is
    TYPE SalTabTyp is TABLE OF integer index by varchar(10);  
	aa SalTabTyp;
	b varchar(10);
 begin
	aa('a') = 1;
	b = 'aa';
	aa(b) = 2;
	RAISE INFO '%' ,aa('a');
	RAISE INFO '%' ,aa('aa');
	RAISE INFO '%' ,aa(b);
end;
/

call tableof_14();
 
create or replace procedure tableof_15
 is
    TYPE SalTabTyp is TABLE OF varchar(10) index by date;
	aa SalTabTyp;
 begin
	
end;
/

create or replace procedure tableof_15
 is
    TYPE SalTabTyp is TABLE OF varchar(10) index by text;
	aa SalTabTyp;
 begin
	
end;
/

-- test table = table
create or replace procedure tableof_16
 is
    TYPE SalTabTyp is TABLE OF varchar(10) index by BINARY_INTEGER;  
	aa SalTabTyp;
	bb SalTabTyp;
 begin
	aa(-1) = 'b';
	aa(1) = 'a';
	RAISE INFO '%' ,aa(-1);
	bb = aa;
	RAISE INFO '%' ,bb(-1);
	bb(8) = 'g';
	RAISE INFO '%' ,bb(8);
	RAISE INFO '%' ,aa(8);
 end;
/

call tableof_16();

-- test define
create or replace procedure tableof_17
 is
    TYPE SalTabTyp is TABLE OF s_type%rowtype index by varchar(10);  
	aa SalTabTyp;
 begin
	aa('a') = (1, 'zhangsan', 'shanghai');
	aa('b') = (2, 'lisi', 'beijing');
	RAISE INFO '%' ,aa('a').id;
	RAISE INFO '%' ,aa('b');
end;
/
call tableof_17();

create or replace procedure tableof_18
 is
    TYPE SalTabTyp is TABLE OF s_type.id%type index by varchar(10);  
	aa SalTabTyp;
 begin
	aa('a') = 1;
	aa('b') = 2;
	RAISE INFO '%' ,aa('a');
	RAISE INFO '%' ,aa('b');
end;
/

call tableof_18();


-- test not null gram
create or replace procedure tableof_19
 is
    TYPE SalTabTyp is TABLE OF s_type%rowtype not null index by varchar(10);  
	aa SalTabTyp;
 begin
	aa('a') = (1, 'zhangsan', 'shanghai');
	RAISE INFO '%' ,aa('a');
end;
/

call tableof_19();

-- test assign one attr 
create or replace procedure tableof_20
 is
    TYPE SalTabTyp is TABLE OF s_type%rowtype not null index by varchar(10);  
	aa SalTabTyp;
 begin
	aa('a') = (1, 'zhangsan', 'shanghai');
	aa('a').id = 1;
end;
/

call tableof_20();

create type info as (name varchar2(50), age int, address varchar2(20), salary float(2));
create type customer as (id number(10), c_info info);
create table customers (id number(10), c_info info);

insert into customers (id, c_info) values (1, ('Vera' ,32, 'Paris', 22999.00));
insert into customers (id, c_info) values (2, ('Zera' ,25, 'London', 5999.00));
insert into customers (id, c_info) values (3, ('Alice' ,22, 'Bangkok', 9800.98));
insert into customers (id, c_info) values (4, ('Jim' ,26, 'Dubai', 18700.00));
insert into customers (id, c_info) values (5, ('Kevin' ,28, 'Singapore', 18999.00));
insert into customers (id, c_info) values (6, ('Gauss' ,42, 'Beijing', 32999.00));
-- test curosor fetch into
create or replace procedure tableof_21
as
declare
	TYPE id_1 is TABLE OF customer.id%type index by varchar(10); 
	TYPE c_info_1 is TABLE OF customers.c_info%type index by varchar(10);  
    CURSOR C1 IS SELECT id FROM customers order by id;
    CURSOR C2 IS SELECT c_info FROM customers order by id;
    info_a c_info_1:=c_info_1();
	id_a id_1:=id_1();
begin
    OPEN C1;
    OPEN C2;
    FETCH C1 into id_a(2);
    FETCH C2 into info_a(2);
    FETCH C1 into id_a(3);
    FETCH C2 into info_a(3);
    CLOSE C1;
    CLOSE C2;
	RAISE INFO '%', id_a;
	RAISE INFO '%', info_a;
end;
/

call tableof_21();

-- test select into 
create or replace procedure tableof_22
as  
declare
	TYPE id_1 is TABLE OF customer.id%type index by varchar(10); 
	TYPE c_info_1 is TABLE OF customers.c_info%type index by varchar(10);  
    info_a c_info_1:=c_info_1();
	id_a id_1:=id_1();
begin
    select id into id_a(2) from customers where id = 3;
    select c_info into info_a(2) from customers where id = 3;
    select id into id_a(3) from customers where id = 4;
    select c_info into info_a(3) from customers where id = 4;
	RAISE INFO '%', id_a(2);
	RAISE INFO '%', info_a(3).age;
end;
/

call tableof_22();

-- test curosor for
create or replace procedure tableof_23 
as 
declare
    type c_list is TABLE of customer; 
    customer_table c_list:=c_list();
    CURSOR C1 IS SELECT * FROM customers order by id;
    counter int := 0;
begin 
    for n in C1 loop
	    counter := counter + 1;
        customer_table(counter) := n;
	end loop;
	RAISE INFO '%', customer_table(3);
end;
/

call tableof_23();

create or replace procedure tableof_24 
as 
declare
    type c_list is TABLE of customers%rowtype; 
    customer_table c_list:=c_list();
    CURSOR C1 IS SELECT * FROM customers order by id;
    counter int := 0;
begin 
    for n in C1 loop
	    counter := counter + 1;
        customer_table(counter) := n;
	end loop;
	RAISE INFO '%', customer_table(4);
end;
/

call tableof_24();

-- test row type
create type typeE as table of s_type%rowtype;
create type typeE as table of customers%rowtype;

create or replace procedure tableof_25
as 
declare
    customer_table typeE;
    CURSOR C1 IS SELECT * FROM customers order by id;
    counter int := 0;
begin 
    for n in C1 loop
	    counter := counter + 1;
        customer_table(counter) := n;
	end loop;
	RAISE INFO '%', customer_table(4);
end;
/

call tableof_25();

-- test insert
create or replace procedure tableof_26
as 
declare
	type c_list is TABLE of customers%rowtype; 
    customer_table c_list:=c_list();
begin 
    customer_table(1) := (7, ('Vera' ,32, 'Paris', 22999.00));
	customer_table(2) := (8, ('Vera' ,32, 'Paris', 22999.00));
	insert into customers values (customer_table(1).id, customer_table(1).c_info);
	insert into customers values (customer_table(2).id, customer_table(2).c_info);
end;
/

call tableof_26();
select * from customers where id = 7;

-- expect error table[]
create or replace procedure tableof_27
as 
declare
	type c_list is TABLE of customers%rowtype; 
    customer_table c_list:=c_list();
begin 
    customer_table(1) := (7, ('Vera' ,32, 'Paris', 22999.00));
	insert into customers values (customer_table[1].id, customer_table[1].c_info);
end;
/

-- test deault
declare
    type students is table of varchar2(10);
    type grades is table of integer;
    marks grades := grades(98, 97, 74 + 4, (87), 92, 100); -- batch initialize --
    names students default students('none'); -- default --
    total integer;
begin
    names := students();  -- should append NULL then do the coerce --
    names := students('Vera ', 'Zera ', 'Alice', 'Jim  ', 'Kevin', to_char('G') || 'auss'); -- batch insert --
    total := names.count;
    RAISE INFO 'Total % Students', total;
    for i in 1 .. total loop
        RAISE INFO 'Student: % Marks: %', names(i), marks(i);
    end loop;
end;
/

create type mytype as (
    id integer,
    biome varchar2(100)
);

create type mytype2 as (
    id integer,
    locale myType
);
declare
    type finaltype is table of mytype2;
    aa finaltype := finaltype(
        mytype2(1, mytype(1, 'ground')),
        mytype2(1, mytype(2, 'air'))
    );
begin
    aa.extend(10);
    aa(2) := (2, (3, 'water')); -- overwrite record (1, (2, 'air')) --
    RAISE INFO 'locale id is: %', aa(1).id;
    RAISE INFO 'biome 1.3 is: %', aa(2).locale.biome;
end;
/

-- test of uneven brackets --
-- error out --
declare
    type students is table of varchar2(10);
    names students;
begin
    names := students(1, 'Zera ', 'Alice', 'Jim  ', 'Kevin'); -- should be able read all values correctly --
    for i in 1 .. 5 loop
        RAISE INFO 'Student: %', names(i];
    end loop;
end;
/

-- Using composite type defined outside of precedure block --
declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        mytype2(1, (1, 'ground')),
        mytype2(1, (2, 'air'))
    );
begin
    aa(2) := (2, (3, 'water')); -- overwrite record (1, (2, 'air')) --
    RAISE INFO 'locale id is: %', aa(1).id;
    RAISE INFO 'biome 1.3 is: %', aa(2).locale.biome;
end;
/

declare
    type finaltype is table of mytype2;
    aa finaltype := finaltype(
        mytype2(1, mytype(1, 'ground')),
        mytype2(1, mytype(2, 'air'))
    );
begin
    aa.extend(10);
    aa(2) := mytype2(2, mytype(3, 'water'));
    RAISE INFO 'locale id is: %', aa(1).id;
    RAISE INFO 'biome 1.3 is: %', aa(2).locale.biome;
end;
/

create type functype as (
    id integer,
    locale myType
);

create or replace function functype(habitat in mytype2)
return mytype2
is
    ret mytype2;
begin
    ret := (-1, (1, 'unknown realm'));
    return ret;
end;
/

declare
    type finaltype is table of mytype2;
    aa finaltype := finaltype(
        functype(1, mytype(1, 'ground')), -- we are prioritizing types here --
        functype(1, mytype(2, 'air'))
    );
begin
    RAISE INFO 'locale id is: %', aa(1).id;
    RAISE INFO 'biome 1.2 is: %', aa(2).locale.biome; -- air --
end;
/
-- abandon type functype
drop type functype; 

declare
    type finaltype is table of mytype2;
    aa finaltype := finaltype(
        functype((1, mytype(1, 'ground'))), -- here we have to use function functype --
        functype((1, mytype(2, 'air')))
    );
begin
    aa.extend(10);
    RAISE INFO 'locale ?? is: %', aa(1).id;
    RAISE INFO 'biome ??? is: %', aa(2).locale.biome; -- weird places --
end;
/

drop function functype;
-- error
declare
    type finaltype is table of mytype2;
    aa finaltype := finaltype(
        functype((1, mytype(1, 'ground'))), -- not sure --
        functype((1, mytype(2, 'air')))
    );
begin
    aa.extend(10);
    RAISE INFO 'This message worth 300 tons of gold (once printed).';
end;
/

-- test table of array
declare
    type arrayfirst is table(10) of int[];
    arr arrayfirst := arrayfirst();
begin
    
end;
/

create type typeG as (a int[]);
declare
    type arrayfirst is table of typeG;
    arr arrayfirst := arrayfirst();
begin
    arr(1) = row(ARRAY[1, 2, 3]);
    RAISE INFO '%', arr(1).a[1];
end;
/

-- test unreserved key word
declare 
    index int;
begin
	index = 1;
end;
/

create or replace package pck1 as
  type t1 is record(c1 int,c2 varchar2);
  type t2 is table of int;
  type t3 is varray(10) of int;
  v1 t1;
  v2 t2;
  v3 t3;
  v_c1 int;
  v_c2 varchar2;
end pck1;
/

create or replace package body pck1 as
  type t5 is record(c1 int,c2 varchar2);
  type t6 is table of int;
  type t7 is varray(10) of int;
  v5 t5;
  v6 t6;
  v7 t7;
end pck1;
/

create or replace function func2() return int as
begin
    pck1.v2 :=pck1.t2();
    pck1.v2.extend(3); 
    pck1.v2(0) := 1;
    pck1.v2(1) := 2;
    plpgsql_table_opengauss.pck1.v2(2) := 3;
    raise info 'pck1.v2(0) is %',pck1.v2(0);
    raise info 'pck1.v2(1) is %',pck1.v2(1);
    raise info 'plpgsql_table_opengauss.pck1.v2(2) is %',plpgsql_table_opengauss.pck1.v2(2);
    return 0;
end;
/
call func2();

drop type typeA;
drop type typeB;
drop type s_type cascade;
drop type typeC;
drop type typeE;
drop type typeG;
drop type s_type_extend;
drop type typeA_ext;
drop type info;
drop type customer;
drop type mytype;
drop type mytype2;
drop procedure tableof_1;
drop procedure tableof_2;
drop procedure tableof_3;
drop function tableof_6;
drop function tableof_7;
drop function tableof_8;
drop procedure tableof_10;
drop procedure tableof_11;
drop procedure tableof_12;
drop procedure tableof_13;
drop procedure tableof_14;
drop procedure tableof_16;
drop procedure tableof_17;
drop procedure tableof_18;
drop procedure tableof_19;
drop procedure tableof_21;
drop procedure tableof_22;
drop procedure tableof_23;
drop procedure tableof_24;
drop procedure tableof_25;
drop procedure tableof_26;
drop procedure tableof_27;
drop table customers;
drop schema if exists plpgsql_table_opengauss cascade;

create database db_gbk encoding='gbk' template=template0 lc_collate='zh_CN.gbk' lc_ctype='zh_CN.gbk';
\c db_gbk
set client_encoding to 'utf8';
CREATE OR REPLACE FUNCTION fun() RETURNS VOID AS $$
DECLARE
Type array_gbk_type IS TABLE OF int2 INDEX BY varchar(10);
array_gbk array_gbk_type;
BEGIN
array_gbk('乁') := 6;
array_gbk('丂') := 12;
array_gbk('亇') := 2; 
array_gbk('亅') := 4; 
raise info 'FIRST=%',array_gbk.FIRST;
raise info 'LAST=%',array_gbk.LAST; 
END;
$$
LANGUAGE plpgsql;
select fun();
reset client_encoding;
\c regression
drop database db_gbk;
