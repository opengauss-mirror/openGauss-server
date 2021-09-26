create schema hw_function_p_4;
set search_path to hw_function_p_4;

 create table test_info_column(a int1, b int4, c nvarchar2, d nvarchar2(10), f varchar, g varchar(10), h date, j smalldatetime);
 select table_name,column_name,numeric_precision, numeric_precision_radix, numeric_scale, datetime_precision from information_schema.columns where table_name='test_info_column' order by 1,2;
 drop table test_info_column;

START TRANSACTION;
create user "mppdba4Cluster" with sysadmin password 'Test@Mpp';
set role "mppdba4Cluster" password 'Test@Mpp';
create table test1 (c1 int);
drop table test1;
reset role;
drop user "mppdba4Cluster";
COMMIT;


--quotes in function name
create schema "Schema""quote";

create or replace function "Schema""quote"."Func""1"()
returns integer
as $$
begin
return 10;
end $$
language plpgsql;

create or replace function func3_RT()
returns integer
as $$
declare
m int;
begin 
m := 5;
m := m + 1;
m := "Schema""quote"."Func""1"();
return m;
end 
$$language plpgsql;

select func3_RT();

create or replace function test_savepoint(a int) returns void as  $$
declare 
id1 int;
no int;
begin
	savepoint a;
end;
$$ language plpgsql;

drop function "Schema""quote"."Func""1"();
drop function func3_RT();
drop schema "Schema""quote";

\o /dev/null
select * from pg_get_xidlimit();
\o

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp  as (select sysdate as s);
ret :=0;
end $$
;
select func_tmp_test('2018/01/01');
drop table test_create_tmp;

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
insert into test_create_tmp values(2);
select a into ret from test_create_tmp;
drop table test_create_tmp;
end $$
;
select func_tmp_test('2018/01/01');

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
create temp table test_create_tmp2(a int);
insert into test_create_tmp values(1);
insert into test_create_tmp2 values(2);
select a into ret from test_create_tmp;
drop table test_create_tmp;
drop table test_create_tmp2;
end $$
;
select func_tmp_test('2018/01/01');

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
insert into test_create_tmp values(1);
select a into ret from test_create_tmp;
drop table test_create_tmp;
end $$
;
create or replace function func_tmp_test2(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare
begin
create temp table test_create_tmp2(a int);
insert into test_create_tmp2 values(2);
select func_tmp_test('2018/01/01') into ret;
drop table test_create_tmp2;
end $$
;
select func_tmp_test2('2018/01/01');

create temp table test_create_tmp(a int);
insert into test_create_tmp values(1);
create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create schema test_schema_temp;
create table test_schema_temp.test_create_tmp(a int);
insert into test_schema_temp.test_create_tmp values(2);
select a into ret from test_create_tmp;
drop table test_schema_temp.test_create_tmp;
drop schema test_schema_temp;
end $$
;
select func_tmp_test('a');
drop table test_create_tmp;

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp  as (select sysdate as s);
ret :=0;
insert into test_create_tmp_no_exit values (2);
exception
when others then
create temp table test_create_tmp  as (select sysdate as s);
ret :=1;
end $$
;
select func_tmp_test('2018/01/01');
drop table test_create_tmp;

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
insert into test_create_tmp values(2);
select a into ret from test_create_tmp;
insert into test_create_tmp_no_exit values (2);
exception
when others then
create temp table test_create_tmp(a int);
insert into test_create_tmp values(3);
select a into ret from test_create_tmp;
drop table test_create_tmp;
end $$
;
select func_tmp_test('2018/01/01');

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
create temp table test_create_tmp2(a int);
insert into test_create_tmp values(1);
insert into test_create_tmp2 values(2);
select a into ret from test_create_tmp;
drop table test_create_tmp;
drop table test_create_tmp2;
insert into test_create_tmp_no_exit values (2);
exception
when others then
create temp table test_create_tmp(a int);
create temp table test_create_tmp2(a int);
insert into test_create_tmp values(3);
insert into test_create_tmp2 values(4);
select a into ret from test_create_tmp;
drop table test_create_tmp;
drop table test_create_tmp2;
end $$
;
select func_tmp_test('2018/01/01');

create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create temp table test_create_tmp(a int);
insert into test_create_tmp values(1);
select a into ret from test_create_tmp;
drop table test_create_tmp;
end $$
;
create or replace function func_tmp_test2(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare
begin
create temp table test_create_tmp2(a int);
insert into test_create_tmp2 values(2);
select func_tmp_test('2018/01/01') into ret;
insert into test_create_tmp_no_exit values (2);
exception
when others then
create temp table test_create_tmp2(a int);
insert into test_create_tmp2 values(2);
select func_tmp_test('2018/01/01') into ret;
drop table test_create_tmp2;
end $$
;
select func_tmp_test2('2018/01/01');

create temp table test_create_tmp(a int);
insert into test_create_tmp values(1);
create or replace function func_tmp_test(v_date  character varying ,out ret integer)
returns integer
language plpgsql 
as $$ declare 
begin
create schema test_schema_temp;
create table test_schema_temp.test_create_tmp(a int);
insert into test_schema_temp.test_create_tmp values(2);
select a into ret from test_create_tmp;
insert into test_create_tmp_no_exit values (2);
exception
when others then
create schema test_schema_temp;
create table test_schema_temp.test_create_tmp(a int);
insert into test_schema_temp.test_create_tmp values(3);
select a into ret from test_create_tmp;
drop table test_schema_temp.test_create_tmp;
drop schema test_schema_temp;
end $$
;
select func_tmp_test('a');

CREATE TABLE public.aa(a INT) ;
CREATE OR REPLACE FUNCTION fuc01( ) RETURNS SETOF text
AS $$ 
BEGIN
  RETURN QUERY 
  EXECUTE 'EXPLAIN (VERBOSE true, costs off) SELECT * FROM public.aa';
END;
$$ LANGUAGE plpgsql;

SELECT fuc01();
set explain_perf_mode=pretty;
SELECT fuc01();

drop table public.aa;

create temp table compos (f1 int, f2 text);

create function fcompos1(v compos) 
returns void as $$
insert into compos values (v.*);
$$ language sql;

select fcompos1((1, 1));
select * from compos;

begin
a.b.c();
end;
/
drop schema hw_function_p_4 cascade;
