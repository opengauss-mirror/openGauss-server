---RETURN---
create database pl_test_func_out DBCOMPATIBILITY 'pg';
\c pl_test_func_out;

SET CHECK_FUNCTION_BODIES TO ON;
--default return out arg. not supported in A db
CREATE OR REPLACE FUNCTION test_return( i in integer, j out integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

SELECT test_return(1);

declare
a int := 1;
b int;
begin
	test_return(a, b);
end;
/

declare
a int := 1;
c int;
begin
	c := test_return(a);
end;
/

declare
a int := 1;
b int;
c int;
begin
	c := test_return(a, b);
end;
/

--explicitly return out arg.
CREATE OR REPLACE FUNCTION test_return1( i in integer, j out integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

SELECT test_return1(1);

declare
a int := 1;
b int;
begin
	test_return1(a, b);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
	c := test_return1(a, b);
end;
/

--Ora ok --pg ok
CREATE OR REPLACE PROCEDURE test_return2( i in integer, j out integer) 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_return2(a, b);
end;
/

--ora err
declare
a int := 1;
b int;
c int;
begin
	c := test_return2(a, b);
end;
/

declare
a int := 1;
b int;
c int;
begin
    c := test_return2(j=>b, i=>a);
end;
/

CREATE OR REPLACE PROCEDURE test_return3( i in integer) 
AS
	BEGIN
		RETURN;
    END;
/

--Ora error 
CREATE OR REPLACE PROCEDURE test_return4( i in integer, j out integer) 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

CREATE OR REPLACE FUNCTION test_return5( i in integer, j out integer) 
RETURN varchar2 
AS
	BEGIN
		j:=i+1;
		RETURN 'abc';
    END;
/

SELECT test_return5(1);

declare
a int := 1;
b int;
begin
	test_return5(a, b);
end;
/

declare
a int := 1;
b int;
c varchar2(100);
begin
	c := test_return5(a, b);
end;
/

--ora err
CREATE OR REPLACE PROCEDURE test_return6( i in integer) 
AS
	BEGIN
		RETURN i;
    END;
/

--ora err
CREATE OR REPLACE FUNCTION test_return7( i in integer,j out integer) 
RETURNS integer 
AS $$
	BEGIN
		j:=i+1;
		RETURN;
    END;    
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return8( i in integer,j out integer)
RETURNS bigint 
AS $$
    BEGIN
        j:=i+1;
        RETURN j;
    END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return9( i in integer,j out integer)
RETURNS bigint
AS $$
    BEGIN
        j:=i+1;
        RETURN 1.6;
    END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return10( i in integer,j out integer)
RETURNS void 
AS $$
    BEGIN
        j:=i+1;
        RETURN;
    END;
$$ language plpgsql;

--ora err
CREATE OR REPLACE FUNCTION test_return11( ) 
returns int as $$
declare
a int := 1;
b int;
begin
	a := test_return(a, b);
end;
$$ language plpgsql;
/

CREATE OR REPLACE FUNCTION test_return12() 
RETURN integer
AS
	DECLARE
		a int;
	BEGIN
		RETURN test_return(1, a);
	END;
/

CREATE FUNCTION test_return13() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	test_return(p,q);
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION test_return14() 
RETURN integer 
AS
	DECLARE
	p int := 10;
	q int := 1;
	BEGIN
		test_return(p,q);
		return p;
	END;
/

CREATE OR REPLACE FUNCTION test_return15( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN 1;
	END;
/

declare
a int := 1;
b int;
begin
	test_return15(a, b);
end;
/

--ora err
CREATE OR REPLACE FUNCTION test_return16( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN;
	END;
/

CREATE OR REPLACE FUNCTION test_return17( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN 'abc';
    END;
/

CREATE OR REPLACE FUNCTION test_return18( i integer,j out integer)
RETURN record 
AS
	result record;
    BEGIN
        result := test_return(1);
		RETURN result;
    END;
/

CREATE OR REPLACE FUNCTION test_return19( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN result;
    END;
/

CREATE OR REPLACE FUNCTION test_return20( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN NULL;
    END;
/

CREATE OR REPLACE FUNCTION test_return21( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN 1;
    END;
/

drop function test_return;
drop function test_return1;
drop function test_return2;
drop function test_return3;
drop function test_return4;
drop function test_return5;
drop function test_return7;
drop function test_return8;
drop function test_return9;
drop function test_return10;
drop function test_return11;
drop function test_return12;
drop function test_return13;
drop function test_return14;
drop function test_return15;
drop function test_return16;
drop function test_return17;
drop function test_return20;

--default
CREATE FUNCTION test_default(j out integer, i integer default 1) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

declare
a int := 1;
b int;
begin
	test_default(b, a);
end;
/

declare
b int;
begin
	test_default(b);
end;
/

CREATE OR REPLACE FUNCTION test_default(j out integer) 
RETURN integer 
AS
	BEGIN
		j:=1+1;
		RETURN j;
    END;
/

declare
b int;
begin
	test_default(b);
end;
/


declare
b int;
begin
	test_default(b);
end;
/

CREATE FUNCTION test_default1(j out integer, str varchar2 default '1', a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number(str);
		RETURN j;
    END;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	a := test_default1(b, c);
end;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	test_default1(b);
end;
/

CREATE OR REPLACE FUNCTION test_default1(j out integer, a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number('1');
		RETURN j;
    END;
/

--change arg types
CREATE FUNCTION test_arg(j out integer, str varchar2 default '1', a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number(str);
		RETURN j;
    END;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	test_arg(b);
end;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
d int := 1;
begin
	test_arg(b, c, d);
end;
/

CREATE OR REPLACE FUNCTION test_arg(j out integer, str integer default 1, a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+str;
		RETURN j;
    END;
/

declare
a int := 1;
b int;
begin
	a := test_arg(b);
end;
/

CREATE FUNCTION test_func_return_out(j out integer, i integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_func_return_out(b, a);
end;
/

CREATE OR REPLACE FUNCTION test_func_return_out(j out integer) 
RETURN integer 
AS
	BEGIN
		j:=1+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_func_return_out(b);
end;
/

DROP FUNCTION test_default;
DROP FUNCTION test_default1;
DROP FUNCTION test_arg;
DROP FUNCTION test_func_return_out;


CREATE OR REPLACE FUNCTION test_return( i in integer, j out integer)
RETURN integer
AS
    BEGIN
        j:=i+1;
        RETURN j;
    END;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + 1;
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := 2 * test_return(a, b);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + test_return(a);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + test_return(a, b);
end;
/

DROP FUNCTION test_return;

declare 
temp varchar2(20);
begin
temp := substr('abcd',1);
end;
/

declare
temp varchar2(20);
begin
temp := substr('abcd', 1, 1);
end;
/

create or replace function test_assign_func1()
returns INTEGER
AS $$
DECLARE
  VAR1  varchar(40);
  VAR2  varchar(40);
  VAR3  varchar(40);
BEGIN
        VAR1 := '';
 VAR2 := '';
 VAR3 := CONCAT(VAR1,VAR1);
raise info 'MYCHAR3 is %', VAR3;
return 0;
END;
$$LANGUAGE plpgsql;

call test_assign_func1();


create or replace PROCEDURE test_assign_func2
(
    RETURNCODE          OUT     INTEGER 
)
AS
  MYINTEGER INTEGER;
BEGIN
     MYINTEGER := 1234;
     raise info 'MYINTEGER is %', MYINTEGER;
     MYINTEGER := TO_NUMBER(MYINTEGER );
     raise info 'TO_NUMBER(MYINTEGER) RESULT  is %', MYINTEGER;
     RETURNCODE := 0;
END ;
/

select test_assign_func2();

drop function test_assign_func1();
drop procedure test_assigrn_func2;
SET CHECK_FUNCTION_BODIES TO OFF;
---RETURN---
SET CHECK_FUNCTION_BODIES TO ON;
--default return out arg. not supported in A db
CREATE OR REPLACE FUNCTION test_return( i in integer, j out integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

SELECT test_return(1);

declare
a int := 1;
b int;
begin
	test_return(a, b);
end;
/

declare
a int := 1;
c int;
begin
	c := test_return(a);
end;
/

declare
a int := 1;
b int;
c int;
begin
	c := test_return(a, b);
end;
/

--explicitly return out arg.
CREATE OR REPLACE FUNCTION test_return1( i in integer, j out integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

SELECT test_return1(1);

declare
a int := 1;
b int;
begin
	test_return1(a, b);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
	c := test_return1(a, b);
end;
/

--Ora ok --pg ok
CREATE OR REPLACE PROCEDURE test_return2( i in integer, j out integer) 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_return2(a, b);
end;
/

--ora err
declare
a int := 1;
b int;
c int;
begin
	c := test_return2(a, b);
end;
/

declare
a int := 1;
b int;
c int;
begin
    c := test_return2(j=>b, i=>a);
end;
/

CREATE OR REPLACE PROCEDURE test_return3( i in integer) 
AS
	BEGIN
		RETURN;
    END;
/

--Ora error 
CREATE OR REPLACE PROCEDURE test_return4( i in integer, j out integer) 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

CREATE OR REPLACE FUNCTION test_return5( i in integer, j out integer) 
RETURN varchar2 
AS
	BEGIN
		j:=i+1;
		RETURN 'abc';
    END;
/

SELECT test_return5(1);

declare
a int := 1;
b int;
begin
	test_return5(a, b);
end;
/

declare
a int := 1;
b int;
c varchar2(100);
begin
	c := test_return5(a, b);
end;
/

--ora err
CREATE OR REPLACE PROCEDURE test_return6( i in integer) 
AS
	BEGIN
		RETURN i;
    END;
/

--ora err
CREATE OR REPLACE FUNCTION test_return7( i in integer,j out integer) 
RETURNS integer 
AS $$
	BEGIN
		j:=i+1;
		RETURN;
    END;    
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return8( i in integer,j out integer)
RETURNS bigint 
AS $$
    BEGIN
        j:=i+1;
        RETURN j;
    END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return9( i in integer,j out integer)
RETURNS bigint
AS $$
    BEGIN
        j:=i+1;
        RETURN 1.6;
    END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION test_return10( i in integer,j out integer)
RETURNS void 
AS $$
    BEGIN
        j:=i+1;
        RETURN;
    END;
$$ language plpgsql;

--ora err
CREATE OR REPLACE FUNCTION test_return11( ) 
returns int as $$
declare
a int := 1;
b int;
begin
	a := test_return(a, b);
end;
$$ language plpgsql;

/*test_return12*/
CREATE OR REPLACE FUNCTION test_return12() 
RETURN integer
AS
	DECLARE
		a int;
	BEGIN
		RETURN test_return(1, a);
	END;
/

/*****test_return13*****/
CREATE FUNCTION test_return13() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	test_return(p,q);
	return p;
END;
$$ LANGUAGE plpgsql; 

  /*** test_return14 *****/
CREATE OR REPLACE FUNCTION test_return14() 
RETURN integer 
AS
	DECLARE
	p int := 10;
	q int := 1;
	BEGIN
		test_return(p,q);
		return p;
	END;
/

    /*******************/
CREATE OR REPLACE FUNCTION test_return15( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN 1;
	END;
/

declare
a int := 1;
b int;
begin
	test_return15(a, b);
end;
/

--ora err
CREATE OR REPLACE FUNCTION test_return16( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN;
	END;
/

CREATE OR REPLACE FUNCTION test_return17( i integer,j out integer) 
RETURN number 
AS
	BEGIN
		j:=i+1;
		RETURN 'abc';
    END;
/

CREATE OR REPLACE FUNCTION test_return18( i integer,j out integer)
RETURN record 
AS
	result record;
    BEGIN
        result := test_return(1);
		RETURN result;
    END;
/

CREATE OR REPLACE FUNCTION test_return19( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN result;
    END;
/

CREATE OR REPLACE FUNCTION test_return20( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN NULL;
    END;
/

CREATE OR REPLACE FUNCTION test_return21( i integer,j out integer)
RETURN record
AS
    result int;
    BEGIN
        RETURN 1;
    END;
/

drop function test_return;
drop function test_return1;
drop function test_return2;
drop function test_return3;
drop function test_return4;
drop function test_return5;
drop function test_return7;
drop function test_return8;
drop function test_return9;
drop function test_return10;
drop function test_return11;
drop function test_return12;
drop function test_return13;
drop function test_return14;
drop function test_return15;
drop function test_return16;
drop function test_return17;
drop function test_return20;

--default
CREATE FUNCTION test_default(j out integer, i integer default 1) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN j;
    END;
/

declare
a int := 1;
b int;
begin
	test_default(b, a);
end;
/

declare
b int;
begin
	test_default(b);
end;
/

CREATE OR REPLACE FUNCTION test_default(j out integer) 
RETURN integer 
AS
	BEGIN
		j:=1+1;
		RETURN j;
    END;
/

declare
b int;
begin
	test_default(b);
end;
/


declare
b int;
begin
	test_default(b);
end;
/

CREATE FUNCTION test_default1(j out integer, str varchar2 default '1', a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number(str);
		RETURN j;
    END;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	a := test_default1(b, c);
end;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	test_default1(b);
end;
/

CREATE OR REPLACE FUNCTION test_default1(j out integer, a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number('1');
		RETURN j;
    END;
/

--change arg types
CREATE FUNCTION test_arg(j out integer, str varchar2 default '1', a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+to_number(str);
		RETURN j;
    END;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
begin
	test_arg(b);
end;
/

declare
a int := 1;
b int;
c varchar2(10) :='1';
d int := 1;
begin
	test_arg(b, c, d);
end;
/

CREATE OR REPLACE FUNCTION test_arg(j out integer, str integer default 1, a integer default 1, b integer default 2) 
RETURN integer 
AS
	BEGIN
		j:=a+b+str;
		RETURN j;
    END;
/

declare
a int := 1;
b int;
begin
	a := test_arg(b);
end;
/

CREATE FUNCTION test_func_return_out(j out integer, i integer) 
RETURN integer 
AS
	BEGIN
		j:=i+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_func_return_out(b, a);
end;
/

CREATE OR REPLACE FUNCTION test_func_return_out(j out integer) 
RETURN integer 
AS
	BEGIN
		j:=1+1;
		RETURN;
    END;
/

declare
a int := 1;
b int;
begin
	test_func_return_out(b);
end;
/

DROP FUNCTION test_default;
DROP FUNCTION test_default1;
DROP FUNCTION test_arg;
DROP FUNCTION test_func_return_out;


CREATE OR REPLACE FUNCTION test_return( i in integer, j out integer)
RETURN integer
AS
    BEGIN
        j:=i+1;
        RETURN j;
    END;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + 1;
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := 2 * test_return(a, b);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + test_return(a);
end;
/

declare
a int := 1;
b int := 1;
c int;
begin
    c := test_return(a, b) + test_return(a, b);
end;
/

DROP FUNCTION test_return;

declare 
temp varchar2(20);
begin
temp := substr('abcd',1);
end;
/

declare
temp varchar2(20);
begin
temp := substr('abcd', 1, 1);
end;
/

create or replace function test_assign_func1()
returns INTEGER
AS $$
DECLARE
  VAR1  varchar(40);
  VAR2  varchar(40);
  VAR3  varchar(40);
/**function body BEGIN**/
BEGIN
        VAR1 := '';
 VAR2 := '';
 VAR3 := CONCAT(VAR1,VAR1);
raise info 'MYCHAR3 is %', VAR3;
return 0;
END;
/**function body END**/
$$LANGUAGE plpgsql;

call test_assign_func1();


create or replace PROCEDURE test_assign_func2
(
    RETURNCODE          OUT     INTEGER 
)
AS
  MYINTEGER INTEGER;
/**PROCEDURE BODY BEGIN**/
BEGIN
     MYINTEGER := 1234;
     raise info 'MYINTEGER is %', MYINTEGER;
     MYINTEGER := TO_NUMBER(MYINTEGER );
     raise info 'TO_NUMBER(MYINTEGER) RESULT  is %', MYINTEGER;
     RETURNCODE := 0;
END ;
/**PROCEDURE BODY END**/
/

select test_assign_func2();

drop function test_assign_func1();
drop procedure test_assigrn_func2;

/**division**/
select 12
 /
3;

/*add extra comment test1*/
/**/**comment test1**/
select 123;

/*add extra comment test2*/
/****comment test2****/*/
select 123;

/*add extra comment test3*/
/*
comment test3
*/
select 123;


-- test for comments or '/' inside procedure which ends in '/'
CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
  /*********icbc*/
select 3/2;
END;
/

CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
  /****
  *****icbc*/
select 3/2;
END;
/

CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
  /*********icbc*/
select 3/2;
END;
  /
  
CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
  /*********icbc*/
select 3/2;
END;
  /  

CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
select 3
  /2;
END;
/

CREATE OR REPLACE PROCEDURE proc_sql
AS
 v_error VARCHAR2(10);
BEGIN
select 3
  / 2;
END;
/

--test function return record 
create or replace function pro_cursor_c010(val int,out resetseq int,out tt int)
returns  int
AS $$
BEGIN
		 select 1 into resetseq ;
END;
$$ LANGUAGE plpgsql;

select * from pro_cursor_c010(1);


create or replace function pro_cursor_c0101(val int,out resetseq int,out tt int)
returns  int
AS $$
BEGIN
		 select 1 into resetseq ;
		 return 0;
END;
$$ LANGUAGE plpgsql;

select * from pro_cursor_c0101(1);

create or replace function pro_cursor_c0102(val int,out resetseq int,out tt int)
returns setof int
AS $$
BEGIN
		 select 1 into resetseq ;
END;
$$ LANGUAGE plpgsql;
select * from pro_cursor_c0102(1);

create or replace function pro_cursor_c0103(val int,out resetseq int,out tt int)
returns record
AS $$
BEGIN
		 select 1 into resetseq ;

END;
$$ LANGUAGE plpgsql;

select * from pro_cursor_c0103(1);

create or replace function pro_cursor_c0104(val int,out resetseq int,out tt int)
returns  record
AS $$
BEGIN
		 select 1 into resetseq ;
		 select 2 into tt;

END;
$$ LANGUAGE plpgsql;

select * from pro_cursor_c0104(1);
 
 create or replace function pro_cursor_c011(val int,out resetseq int,out tt int)
 returns int
 AS $$
 BEGIN
  select 1 into resetseq ; 
 END;
 $$ LANGUAGE plpgsql;
  select * from pro_cursor_c011(1);
  
  create or replace function pro_cursor_c012(val int,out resetseq int,out tt int)
 returns void
 AS $$
 BEGIN
  select 1 into resetseq ; 
 END;
 $$ LANGUAGE plpgsql;
  select * from pro_cursor_c012(1);

SET CHECK_FUNCTION_BODIES TO OFF;
\c regression;
drop database IF EXISTS pl_test_func_out;
