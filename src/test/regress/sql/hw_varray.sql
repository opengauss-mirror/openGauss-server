SET CHECK_FUNCTION_BODIES TO ON;
CREATE OR REPLACE FUNCTION TEST_FUNC_VARRAY() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
BEGIN
	arrint.extend(1024);
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--display value with raise info
	raise info'%', arrint(arrint.first);
	raise info'%', arrint(arrint.last);
	raise info'%', arrint(11);
	raise info'%', arrint.count;
END;
$$ LANGUAGE plpgsql;
select test_func_VARRAY();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display value with put_line()
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with select..into clause
	FOR I IN 1..1024 LOOP
		select I into ARRINT(I) from dual;
	END LOOP;
	--display value with put_line()
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display value with put_line()
	--get value with multiple parenthesis
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display with unmatched parenthesis
END;
$$ LANGUAGE plpgsql;
select test_func_varray();


CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
	myint int := 8;
BEGIN
	arrint.extend(1024);
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--assign index with expression, variable and array value 
	raise info'%', arrint(1+1);
	raise info'%', arrint(myint);
	raise info'%', arrint(arrint(9));
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
BEGIN
	--compatible with A db, support "extend" without parameter
	arrint.extend;
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--display value with raise info
	raise info'%', arrint(arrint.first);
	raise info'%', arrint(arrint.last);
	raise info'%', arrint(11);
	raise info'%', arrint.count;
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

drop function test_func_varray();


create table company1 (name varchar2(100), loc varchar2(100), no integer);
insert into company1 values ('lilei', 'china', 1);

create or replace procedure test_cursor_into
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
	TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
	V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
    cursor c1_all is --cursor without args 
        select name, loc, no from company1;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no, V_CCUCOUNT(0);
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/

call test_cursor_into();
create or replace procedure test_cursor_into_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
	TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
	V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
    cursor c1_all is --cursor without args 
        select no from company1;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into V_CCUCOUNT(0);
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_into_1();

create or replace procedure test_into_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
    TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
    V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
begin 
	select * from company1 into company_name, company_loc, V_CCUCOUNT(0);
end;
/
call test_into_1();

drop procedure test_cursor_into;
drop procedure test_cursor_into_1;
drop procedure test_into_1;

drop table company1;
SET CHECK_FUNCTION_BODIES TO OFF;
SET CHECK_FUNCTION_BODIES TO ON;
CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
BEGIN
	arrint.extend(1024);
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--display value with raise info
	raise info'%', arrint(arrint.first);
	raise info'%', arrint(arrint.last);
	raise info'%', arrint(11);
	raise info'%', arrint.count;
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display value with put_line()
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with select..into clause
	FOR I IN 1..1024 LOOP
		select I into ARRINT(I) from dual;
	END LOOP;
	--display value with put_line()
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display value with put_line()
	--get value with multiple parenthesis
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE ARRAY_INTEGER is VARRAY(1024) OF integer;
	ARRINT ARRAY_INTEGER := ARRAY_INTEGER();
BEGIN
	FOR I IN 1..1024 LOOP
		ARRINT(I):=I;
	END LOOP;
	--display with unmatched parenthesis
END;
$$ LANGUAGE plpgsql;
select test_func_varray();


CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
	myint int := 8;
BEGIN
	arrint.extend(1024);
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--assign index with expression, variable and array value 
	raise info'%', arrint(1+1);
	raise info'%', arrint(myint);
	raise info'%', arrint(arrint(9));
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

CREATE OR REPLACE FUNCTION test_func_varray() RETURNS VOID AS $$
DECLARE
	TYPE array_integer is varray(1024) of integer;
	arrint array_integer :=array_integer();
BEGIN
	--compatible with A db, support "extend" without parameter
	arrint.extend;
	--assign value with assign operator
	FOR I IN 1..1024 LOOP
		arrint(I):=I;
	END LOOP;
	--display value with raise info
	raise info'%', arrint(arrint.first);
	raise info'%', arrint(arrint.last);
	raise info'%', arrint(11);
	raise info'%', arrint.count;
END;
$$ LANGUAGE plpgsql;
select test_func_varray();

drop function test_func_varray();


create table company1 (name varchar2(100), loc varchar2(100), no integer);
insert into company1 values ('lilei', 'china', 1);

create or replace procedure test_cursor_into
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
	TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
	V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
    cursor c1_all is --cursor without args 
        select name, loc, no from company1;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no, V_CCUCOUNT(0);
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/

call test_cursor_into();
create or replace procedure test_cursor_into_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
	TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
	V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
    cursor c1_all is --cursor without args 
        select no from company1;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into V_CCUCOUNT(0);
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_into_1();

create or replace procedure test_into_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;
    TYPE CCUCOUNT IS VARRAY(128) OF NUMBER;
    V_CCUCOUNT CCUCOUNT :=CCUCOUNT();
begin 
	select * from company1 into company_name, company_loc, V_CCUCOUNT(0);
end;
/
call test_into_1();

drop procedure test_cursor_into;
drop procedure test_cursor_into_1;
drop procedure test_into_1;

drop table company1;
SET CHECK_FUNCTION_BODIES TO OFF;
