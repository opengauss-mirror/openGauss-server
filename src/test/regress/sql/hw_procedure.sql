---------------------forall------------------------
create database pl_test_pkg DBCOMPATIBILITY 'pg';
\c pl_test_pkg;
SET CHECK_FUNCTION_BODIES TO ON;
CREATE TABLE if not exists test_forall(a char(10));

CREATE FUNCTION test_forall(IN flag int, IN iter int) RETURNS integer AS $$
DECLARE	

BEGIN
	if flag=0 then	
		forall i in 1..iter
			insert into test_forall values ('aa');	
	end if;
	
	if flag=1 then	
		forall i in 1..10
			insert into test_forall values ('bb');	
	end if;
	
	if flag=2 then	
		forall i in 1..iter	
		update test_forall set a='cc' where a='aa';			
	end if;
	
	if flag=3 then	
		forall i in 1..iter
			delete from test_forall where a='bb';
	end if;

	return iter;
END;
$$ LANGUAGE plpgsql;

-- check prokind
select prokind from pg_proc where proname = 'test_forall';

CREATE FUNCTION test_forallDML1(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter	
		CREATE ROLE jonathan PASSWORD 'ttest@123' LOGIN;	
	return iter;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML2(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter	
		create table if not exists test_forall(a char(10));		
	return iter;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML3(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter	
		alter table test_forall add column b varchar(10);		
	return iter;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML4(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter	
		drop table test_forall;
	return iter;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE    PRO_SQLCODE
(
    PSV_SQLCODE           OUT     INTEGER
)
AS
i    INTEGER; 
BEGIN
         FORALL  i IN 1..2
   INSERT INTO test_forall VALUES(to_char(i));
--         EXCEPTION 
--         WHEN  FORALL_NEED_DML THEN
--              PSV_SQLCODE := SQLCODE;  
--         raise info 'SQLCODE IS %',  PSV_SQLCODE;              
END ;
/

-- check prokind
select prokind from pg_proc where proname = 'pro_sqlcode';

CREATE FUNCTION test_forallDML5(IN iter int) RETURNS integer AS $$
BEGIN
	forall i in 1..iter;
	return iter;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML6(IN iter int) RETURNS integer AS $$
BEGIN
	raise FORALL_NEED_DML;
	return iter;
--EXCEPTION WHEN FORALL_NEED_DML THEN
--	return 0;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML7(IN iter int) RETURNS integer AS $$
BEGIN
	raise SQLSTATE 'P0004';
	return iter;
--EXCEPTION WHEN FORALL_NEED_DML THEN
--	return 0;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_forallDML8(IN iter int) RETURNS integer AS $$
BEGIN
	raise SQLSTATE 'P0004';
	return iter;
END;
$$ LANGUAGE plpgsql;

DECLARE
	a int;
BEGIN
	PRO_SQLCODE(a);
END;
/

SELECT test_forallDML6(10);
SELECT test_forallDML7(10);
SELECT test_forallDML8(10);


SELECT test_forall(0, 10);
SELECT test_forall(1, 10);
SELECT test_forall(2, 10);
SELECT test_forall(3, 10);
SELECT * from test_forall ORDER BY a;

DROP table test_forall;
DROP FUNCTION test_forall(int, int);
DROP FUNCTION test_forallDML6(int);
DROP FUNCTION test_forallDML7(int);
DROP FUNCTION test_forallDML8(int);
DROP PROCEDURE PRO_SQLCODE;

-------------------------function call-----------------------------------
-------------------------function defination-----------------------------
--
--
--

--no argument
CREATE FUNCTION hw_insertion() returns integer AS $$
BEGIN
	insert into test_procedure values ('aa');
	return 0;
END;
$$ LANGUAGE plpgsql; 

--IN
CREATE FUNCTION hw_insertion1 (IN flag INT) returns integer AS $$
BEGIN
	if flag=1 then
		insert into test_procedure values ('aa');
	end if;
	return flag;
END;
$$ LANGUAGE plpgsql; 

--OUT
CREATE FUNCTION hw_insertion2 (OUT arg INT) returns integer AS $$
BEGIN
	insert into test_procedure values ('aa');
	select count(*) into arg from test_procedure;
END;
$$ LANGUAGE plpgsql; 

--IN, OUT
CREATE FUNCTION hw_insertion3 (in flag INT, OUT arg INT) returns integer AS $$
BEGIN
	if flag=1 then
		insert into test_procedure values ('aa');
		select count(*) into arg from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 

--INOUT
CREATE FUNCTION hw_insertion4 (INOUT iter INT) returns integer AS $$
BEGIN
	forall i in 0..iter
		insert into test_procedure values ('aa');
	select count(*) into iter from test_procedure;
END;
$$ LANGUAGE plpgsql; 

--IN,  INOUT
CREATE FUNCTION hw_insertion5 (IN flag int, INOUT iter int) returns integer AS $$
BEGIN
	if flag=1 then
	forall i in 1..iter
		insert into test_procedure values ('aa');
	select count(*) into iter from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 

--INOUT INOUT
CREATE FUNCTION hw_insertion6 (INOUT flag int, INOUT iter int) AS $$
BEGIN
	if flag=1 then
	forall i in 1..iter
		insert into test_procedure values ('aa');
	select count(*) into iter from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 

--OUT OUT
CREATE FUNCTION hw_insertion7 (OUT flag int, OUT iter int) AS $$
DECLARE
	flag int := 1;
	iter int := 10;
BEGIN
	if flag=1 then
	forall i in 1..iter
		insert into test_procedure values ('aa');
	select count(*) into iter from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION hw_insertion8 (IN flag int, IN iter int) RETURNS void AS $$

BEGIN
	if flag=1 then
	forall i in 1..iter
		insert into test_procedure values ('aa');
	select count(*) into iter from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 

CREATE table test_procedure (a char(10));


-------------------------function invoke--------------------------------
--
--
--

---------------------------------normal case---------------------------
CREATE FUNCTION testfunc7() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
	r int := 4;
	s int := 4;
	t int;
BEGIN
    hw_insertion();			--NO ARGUMENT
	hw_insertion1(q);		--IN
	hw_insertion2(t);		--OUT
	hw_insertion3(p, t);		--IN, OUT
	hw_insertion4(p);		--INOUT
	hw_insertion5(q, p);		--IN, INOUT
	hw_insertion6(q, p);	    --INOUT INOUT
	hw_insertion7(q, p);		--OUT OUT
	hw_insertion8(q, p);		--IN IN
	select count(*) into p from test_procedure;
	return p;
END;
$$ LANGUAGE plpgsql; 

SELECT testfunc7();

---------------superflous arguments, error----------------------
--
--
--
CREATE FUNCTION test_func_call_more_arg() RETURNS integer AS $$
DECLARE
	p int := 10;
BEGIN
	hw_insertion(p);
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_more_arg1() RETURNS integer AS $$
DECLARE
	p int :=10;
	t int;
BEGIN	
	hw_insertion2(p, t);		--OUT
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_more_arg2() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
	t int;
BEGIN	
	hw_insertion3(p, q, t);		--IN, OUT
	return q;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_more_arg3() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN	
	hw_insertion4(p, q);		--INOUT
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_more_arg4() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
	t int;
BEGIN	
	hw_insertion5(q, p, t);		--IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_more_arg5() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
		hw_insertion5(q, p, 1);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg6() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	hw_insertion5(q, p, 'abc');     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg7() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	hw_insertion5(q, p 'abc');     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

call test_func_call_more_arg7();

CREATE FUNCTION test_func_call_more_arg8() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	select hw_insertion5(q, p 'abc');     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

call test_func_call_more_arg8();

CREATE FUNCTION test_func_call_more_arg9() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	hw_insertion5(q, p, x);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg10() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
	r varchar2 := 'abc';
BEGIN
	hw_insertion5(q, p, r);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg11() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	hw_insertion5(q, p,);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg12() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 1;
BEGIN
	select hw_insertion5(q, q, 1);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION hw_insertion_no_argmodes(p int, q int) RETURNS integer AS $$
DECLARE
BEGIN
	if q > 10 then
		forall i in 1..p
			insert into test_procedure values ('aa');
	end if;
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg13() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 11;
BEGIN
	hw_insertion_no_argmodes(p, q, 1);     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg14() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 11;
BEGIN
	hw_insertion_no_argmodes(p, q)abc;     --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_more_arg15() RETURNS integer AS $$
DECLARE
	p int := 10;
	q int := 11;
BEGIN
	hw_insertion1(p, length('abc'))abc;     --IN
	return q;
END;
$$ LANGUAGE plpgsql;

BEGIN
	hw_insertion1(1);
END;
/

BEGIN
	hw_insertion1(2,'abc');
END;
/

BEGIN
	hw_insertion1(3,4);
END;
/

------------------less arguments, error-------------------------
--
--
--
CREATE FUNCTION test_func_call_less_arg1() RETURNS integer AS $$
DECLARE

BEGIN	
	hw_insertion2();		--OUT
	return 0;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_less_arg2() RETURNS integer AS $$
DECLARE
	p int := 10;
BEGIN		
	hw_insertion3(p);		--IN, OUT
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_less_arg3() RETURNS integer AS $$
DECLARE

BEGIN		
	hw_insertion4();		--INOUT
	return 0;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_less_arg4() RETURNS integer AS $$
DECLARE
	q int := 1;
BEGIN		
	hw_insertion5(q);		--IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_less_arg5() RETURNS integer AS $$
DECLARE
	q int := 1;
BEGIN
	hw_insertion5;       --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_less_arg6() RETURNS integer AS $$
DECLARE
	q int := 1;
BEGIN
	hw_insertion5[];       --IN, INOUT
	return q;
END;
$$ LANGUAGE plpgsql;
------------------------------------argument type not match---------------------------
--
--
--
CREATE FUNCTION test_func_call_argtype1() RETURNS integer AS $$
DECLARE
	q char := 'a';
BEGIN
	hw_insertion1(q);		--IN
	return q;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_argtype2() RETURNS integer AS $$
DECLARE
	t float := 1.5;
BEGIN
	hw_insertion2(t);		--OUT
	return t;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_argtype3() RETURNS integer AS $$
DECLARE
	p varchar := 'a';
	t float := 1.5;
BEGIN
	hw_insertion3(p,t);		--IN, OUT
	return t;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_argtype4() RETURNS integer AS $$
DECLARE
	p real := 10.6;
BEGIN
	hw_insertion4(p);		--INOUT
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_argtype5() RETURNS integer AS $$
DECLARE
	p text := 10;
	q char := 'a';
BEGIN
	hw_insertion5(q,p);		--IN, INOUT	
	return p;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_argtype6() RETURNS integer AS $$
DECLARE
	q char := 'a';
BEGIN
	hw_insertion5(q, 'abc');     --IN, INOUT
	return p;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_argtype7() RETURNS integer AS $$
DECLARE
	p text := 10;
	q char := 'a';
BEGIN
	hw_insertion5(q, 'abc');     --IN, INOUT
	return p;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION test_func_call_argtype8() RETURNS integer AS $$
DECLARE
	p text := 10;
	q int := 10;
BEGIN
	hw_insertion5(q, 1);     --IN, INOUT
	return p;
END;
$$ LANGUAGE plpgsql;

SELECT test_func_call_argtype1();
SELECT test_func_call_argtype2();
SELECT test_func_call_argtype3();
SELECT test_func_call_argtype4();
SELECT test_func_call_argtype5();
SELECT test_func_call_argtype6();
SELECT test_func_call_argtype7();
SELECT test_func_call_argtype8();
-------------------keyword as function name, error-------------------
--
--
--

CREATE FUNCTION test_func_call_keyword() RETURNS integer AS $$
DECLARE
	p int :=10;
	q int :=1;
	r int :=4;
BEGIN
    to(p,q,r);             -- to is a keyword. error
	return r;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_keyword1() RETURNS integer AS $$
DECLARE
	p int :=10;
	q int :=1;
	r int :=4;
BEGIN
    close(p,q,r);	--close is a keyword
	return r;
END;
$$ LANGUAGE plpgsql; 

---------------------------overloaded function-------------------
--
--
--
CREATE FUNCTION hw_insertion_overload () returns integer AS $$
BEGIN
	insert into test_procedure values ('aa');
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION hw_insertion_overload (in flag INT) returns integer AS $$
BEGIN
	if flag>0 then
		insert into test_procedure values ('aa');
		end if;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION hw_insertion_overload (in flag INT, INOUT arg INT) returns integer AS $$
BEGIN
	if flag>0 then
		insert into test_procedure values ('aa');
		select count(*) into arg from test_procedure;
	end if;
END;
$$ LANGUAGE plpgsql; 


---------------------------overloaded function, error-------------------
--
--
--
CREATE FUNCTION test_func_call_overload() RETURNS integer AS $$
DECLARE
	p int :=10;
	q int :=1;
	r int :=4;
BEGIN
    hw_insertion_overload();             --overloaded	
	return r;
END;
$$ LANGUAGE plpgsql; 

CREATE FUNCTION test_func_call_overload1() RETURNS integer AS $$
DECLARE
	p int :=10;
	q int :=1;
	r int :=4;
BEGIN
    hw_insertion_overload(p);             --overloaded
	return r;
END;
$$ LANGUAGE plpgsql; 

DROP TABLE test_procedure;
DROP FUNCTION hw_insertion();
DROP FUNCTION hw_insertion1(int);
DROP FUNCTION hw_insertion2();
DROP FUNCTION hw_insertion3(int);
DROP FUNCTION hw_insertion4(int);
DROP FUNCTION hw_insertion5(int, int);
DROP FUNCTION hw_insertion6(int, int);
DROP FUNCTION hw_insertion7();
DROP FUNCTION hw_insertion8(int, int);

DROP FUNCTION test_func_call_more_arg7();
DROP FUNCTION testfunc7();
DROP FUNCTION test_func_call_argtype1();
DROP FUNCTION test_func_call_argtype2();
DROP FUNCTION test_func_call_argtype3();
DROP FUNCTION test_func_call_argtype4();
DROP FUNCTION test_func_call_argtype5();
DROP FUNCTION test_func_call_argtype6();
DROP FUNCTION test_func_call_argtype7();
DROP FUNCTION test_func_call_argtype8();

DROP FUNCTION hw_insertion_overload();
DROP FUNCTION hw_insertion_overload(int);
DROP FUNCTION hw_insertion_overload(int, int);

create function  sp_testsp
(
    RETURNCODE        INTEGER,
    testvalue1       varchar(10),
    testvalue2        INTEGER
)
returns integer
AS $$
DECLARE 
    mynumber integer :=0;

BEGIN
		if testvalue2 > 0 then
				insert into test_procedure values ('aa');
		end if;
 				mynumber :=2;
  	RETURN 0;                 
END;
$$  LANGUAGE plpgsql;


create function  sp_testsp1
(
    RETURNCODE        INTEGER,
    testvalue1       varchar(10),
    testvalue2        INTEGER DEFAULT  1
)
returns integer
AS $$
DECLARE 
    mynumber integer :=0;

BEGIN
		if testvalue2 > 0 then
				insert into test_procedure values ('aa');
				 mynumber :=2;
		end if;
  RETURN 0;                 
END;
$$  LANGUAGE plpgsql;

create table test_procedure (a char(10));


create  function  sp_callfunc(a int, b varchar(10), c int)   
returns INTEGER
AS $$
DECLARE
    
BEGIN       
sp_testsp(1+1, $2, $3);
RETURN 0;
END;
$$  LANGUAGE plpgsql;


create  function  sp_callfunc1()   
returns INTEGER
AS $$
DECLARE
a int:=1;
b varchar:='abd';
c int:=1;
    
BEGIN       
 sp_testsp1(length('a'),'abd',1);
RETURN 0;
END;
$$  LANGUAGE plpgsql;


create  function  sp_callfunc2(a int, b varchar(10), c int)   
returns INTEGER
AS $$
DECLARE

    
BEGIN       
sp_testsp1($1, $2);
RETURN 0;
END;
$$  LANGUAGE plpgsql;


create  function  sp_callfunc3()   
returns INTEGER
AS $$
DECLARE
a int:=1;
b varchar:='abd';
c int:=1;
    
BEGIN       
 sp_testsp1(1,'abd');
RETURN 0;
END;
$$  LANGUAGE plpgsql;


select sp_callfunc(1, 'abd', 1);
select sp_callfunc1();
select sp_callfunc2(1, 'abd', 1);
select sp_callfunc3();

select * from test_procedure;

drop table test_procedure;
drop function  sp_testsp(int, varchar(10), int);
drop function  sp_testsp1(int, varchar(10), int);
drop function  sp_callfunc(int, varchar(10), int);
drop function  sp_callfunc1();  
drop function  sp_callfunc2(int, varchar(10), int);   
drop function  sp_callfunc3();

--support ':' out argument.

CREATE OR REPLACE PROCEDURE sp_test_1
(
    param1    in   INTEGER,
    param2    out  INTEGER,
    param3    in   INTEGER
)
AS
BEGIN
   param2:= param1 + param3;
END;
/


DECLARE
    input1 INTEGER:=1;
    input2 INTEGER:=2;
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin sp_test_1(:col_1, :col_2, :col_3);end;'
        USING IN input1, OUT l_param2, IN input2;
END;
/

DECLARE
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin sp_test_1(1, :col_2, 2);end;'
        USING OUT l_param2;
END;
/

DECLARE
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin sp_test_1(1, :col_2, 2.5);end;'
        USING OUT l_param2;
END;
/

DECLARE
    l_param2     INTEGER;
BEGIN
     
    EXECUTE IMMEDIATE 'begin sp_test_1('a', :col_2, 2.5);end;'
        USING OUT l_param2;
END;
/
--deterministic clause
--custom script
CREATE OR REPLACE FUNCTION FN_HW_IP2LONG
(
        IP_STRING IN VARCHAR2
) RETURN NUMBER DETERMINISTIC
IS
BEGIN
        RETURN
                TO_NUMBER(REGEXP_SUBSTR(IP_STRING, '\d+', 1, 1)) * 16777216 + -- 2^24
                TO_NUMBER(REGEXP_SUBSTR(IP_STRING, '\d+', 1, 2)) *    65536 + -- 2^16
                TO_NUMBER(REGEXP_SUBSTR(IP_STRING, '\d+', 1, 3)) *      256 + -- 2^8
                TO_NUMBER(REGEXP_SUBSTR(IP_STRING, '\d+', 1, 4));             -- 2^0
END;
/
SELECT PRONAME FROM PG_PROC WHERE PRONAME = 'fn_hw_ip2long';
DROP FUNCTION FN_HW_IP2LONG;

--create function with deterministic clause
CREATE OR REPLACE FUNCTION TEST_FUNC(NUM IN INTEGER) RETURN NUMBER DETERMINISTIC
IS
        DIVIDE INTEGER;
        LOOK NUMBER;
BEGIN
        DIVIDE := 3;
        RETURN NUM/DIVIDE;
END;
/
SELECT PRONAME FROM PG_PROC WHERE PRONAME = 'test_func';
SELECT TEST_FUNC(99) FROM DUAL;
SELECT TEST_FUNC(35678) FROM DUAL;
SELECT TEST_FUNC(8934) FROM DUAL;
DROP FUNCTION TEST_FUNC;

CREATE OR REPLACE FUNCTION TEST_FUNC(NUM IN INTEGER) RETURN NUMBER DETERMINISTIC
AS
BEGIN
	RETURN NUM/99;
END;
/
SELECT PRONAME FROM PG_PROC WHERE PRONAME = 'test_func';
SELECT TEST_FUNC(99) FROM DUAL;
SELECT TEST_FUNC(35678) FROM DUAL;
SELECT TEST_FUNC(89234) FROM DUAL;
DROP FUNCTION TEST_FUNC;

CREATE OR REPLACE PROCEDURE test_direct_call
(
    param1 IN INTEGER,
	result OUT INTEGER,
    param3 IN INTEGER
)
AS
BEGIN
    result := param1 + param3;
END;
/

CREATE OR REPLACE PROCEDURE test_direct_call2()
AS
	a int;
BEGIN
	test_direct_call(1,a;);
END;
/

--------------------------------------------------------------------------
-- support return the two-string-composed type, like "CHARACTER VARYING"
--------------------------------------------------------------------------
CREATE FUNCTION test_func_define(v1 CHARACTER VARYING)
RETURN CHARACTER VARYING
AS
    v2 varchar2(100);
BEGIN 
    v2 := 'Function ';
    RETURN v2 || v1;
END;
/
SELECT TEST_FUNC_DEFINE('test_func_define called') from dual;
DROP FUNCTION test_func_define;

--procedure name contain $
CREATE OR REPLACE PROCEDURE "$TEST$"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "$TEST$"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;
DROP PROCEDURE "$TEST$";

--procedure name contain special ident, "." could not support.
CREATE OR REPLACE PROCEDURE "~!@#$%^&*()`,/<>:;'{}[]|\-_+="
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "~!@#$%^&*()`,/<>:;'{}[]|\-_+="();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;
DROP PROCEDURE "~!@#$%^&*()`,/<>:;'{}[]|\-_+=";

--other test
CREATE OR REPLACE PROCEDURE "$TEST"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "$TEST"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();

-- with schema
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
	public."$TEST"();
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();

DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;
DROP PROCEDURE "$TEST";

-- with uppercase schema
CREATE SCHEMA "AAA";
CREATE SCHEMA SQL_BASE_SYNTAX;

CREATE OR REPLACE PROCEDURE "AAA"."$TEST$"
AS
BEGIN
	RETURN;
END;
/

CREATE OR REPLACE PROCEDURE  SQL_BASE_SYNTAX.FUNCTION_018
AS
BEGIN
	"AAA"."$TEST$"();
END;
/

CALL SQL_BASE_SYNTAX.FUNCTION_018();

-- with quoted keyword
CREATE OR REPLACE PROCEDURE "create"
AS
BEGIN
	RETURN;
END;
/

CREATE OR REPLACE PROCEDURE  SQL_BASE_SYNTAX.FUNCTION_018
AS
BEGIN
	public."create"();
END;
/

CALL SQL_BASE_SYNTAX.FUNCTION_018();

DROP PROCEDURE SQL_BASE_SYNTAX.FUNCTION_018;
DROP PROCEDURE "AAA"."$TEST$";
DROP PROCEDURE "create";
DROP SCHEMA "AAA";
DROP SCHEMA SQL_BASE_SYNTAX;

CREATE OR REPLACE PROCEDURE "!@#TEST!@#"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "!@#TEST!@#"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;
DROP PROCEDURE "!@#TEST!@#";

--uppercase and lowercase
CREATE OR REPLACE PROCEDURE test_uppercase
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   --uppercase
   test_uppercase();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_uppercase;
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

CREATE OR REPLACE PROCEDURE test_lowercase
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   --lowercase
   test_lowercase();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_lowercase;
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

--create uppercase, call use lowercase, failed!          
CREATE OR REPLACE PROCEDURE test_0004
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "test_0004"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_0004;
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

--create uppercase, call use uppercase
CREATE OR REPLACE PROCEDURE test_0005
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "TEST_0005"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_0005;
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

--create lowercase, call use lowercase
CREATE OR REPLACE PROCEDURE "test_0006"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "test_0006"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_0006;
DROP PROCEDURE "test_0006";
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

--create lowercase, call use uppercase, failed!
CREATE OR REPLACE PROCEDURE "test_0007"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "TEST_0007"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE test_0007;
DROP PROCEDURE "test_0007";
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

CREATE OR REPLACE PROCEDURE "$TEst$"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   "$TEst$"();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;
DROP PROCEDURE "$TEST$";
DROP PROCEDURE "$TEst$";

CREATE OR REPLACE PROCEDURE "TEST_0008"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   test_0008();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE "test_0008";
DROP PROCEDURE test_0008;
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

CREATE OR REPLACE PROCEDURE "test_0009"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   test_0009();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE "test_0009";
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

CREATE OR REPLACE PROCEDURE "test_00010"
AS
BEGIN
	RETURN;                  
END;
/
CREATE OR REPLACE PROCEDURE  FVT_GAUSSDB_ADAPT_FUNCTION_018
AS
BEGIN
   TEST_00010();                 
END;
/
CALL FVT_GAUSSDB_ADAPT_FUNCTION_018();
DROP PROCEDURE "test_00010";
DROP PROCEDURE FVT_GAUSSDB_ADAPT_FUNCTION_018;

--test select into behavior
drop table if exists tbl_into;
create table tbl_into(s1 integer,s2 varchar2,s3 numeric);
insert into tbl_into values(3,'a',4);
insert into tbl_into values(2,'a',4);

create or replace procedure proc_into_gs1
as
tmp integer;
begin
    select s1 into tmp from tbl_into where s3=5;
end;
/
call proc_into_gs1();
create or replace procedure proc_into_gs2
as
tmp integer;
begin
    select s1 into tmp from tbl_into where s3=4;
end;
/
call proc_into_gs2();

drop table tbl_into;

CREATE OR REPLACE PROCEDURE SP_TEST( I IN INTEGER)
AS
ERROR HERE; -- ERROR OCCURED HERE
  BEGIN
	  RETURN I;
  END;
/
SELECT PG_GET_FUNCTIONDEF(OID) FROM PG_PROC WHERE PRONAME='sp_test';
SELECT PROSRC FROM PG_PROC WHERE PRONAME='sp_test';
CALL SP_TEST(8);
DROP PROCEDURE SP_TEST;

create table alt_partnm_tbl_003(c_integer integer, c_varchar varchar)
partition by range(c_integer)
(partition p_10 values less than(10),
partition p_20 values less than(20),
partition p_30 values less than(30)
);

create table alt_partnm_tbl_003_1(
c_integer integer,
c_varchar varchar
)with(orientation=column);

create or replace procedure pro_alt_partnm_tbl_003()
as
begin
insert into alt_partnm_tbl_003_1 select * from alt_partnm_tbl_003 partition (p_10);
end;
/

select pro_alt_partnm_tbl_003();
alter table alt_partnm_tbl_003 rename partition p_10 to _10; 
select pro_alt_partnm_tbl_003();

--add to check set command
set check_function_bodies = off;
show current_schema;
create or replace procedure expbeg_call_noexpbeg_5() as
declare
count_num int;
begin
  create schema expbeg_call_noexpbeg_5;
  set current_schema = expbeg_call_noexpbeg_5;
  create table expbeg_call_noexpbeg_5(id int,info text);
  insert into expbeg_call_noexpbeg values(1,'hello');
end;
/
call expbeg_call_noexpbeg_5();
show current_schema;                                                                                                                                                                
create table pro_schema(id int);
------------------------------------------------------
------------------------------------------------------

------------------------------------------------------plpgsql_exec_function PLPGSQL_DTYPE_ROW
create type ct_int as (col1 int,col2 int);
drop function if exists test_prc_pram(x int, y ct_int, out z ct_int);
create or replace function test_prc_pram(x int, y ct_int, out z ct_int) as $$
begin
	z.col1 := x;
	z.col2 := y.col2;
end;
$$ LANGUAGE plpgsql;
select *from test_prc_pram(1,row(2,3));


------------------------------------------------------stmt_execsql

CREATE TYPE u_country AS ENUM ('Brazil', 'England', 'Germany');

CREATE TYPE u_street_type AS (
  street VARCHAR(100),
  no VARCHAR(30)
);

CREATE TYPE u_address_type AS (
  street u_street_type,
  zip VARCHAR(50),
  city VARCHAR(50),
  country u_country,
  since DATE,
  code INTEGER
);
drop table if exists t_author;
CREATE TABLE t_author (
  id INTEGER NOT NULL PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50) NOT NULL,
  date_of_birth DATE,
  year_of_birth INTEGER,
  address u_address_type
);

INSERT INTO t_author VALUES (1, 'George', 'Orwell',
TO_DATE('1903-06-25', 'YYYY-MM-DD'), 1903, ROW(ROW('Parliament Hill',
'77'), 'NW31A9', 'Hampstead', 'England', '1980-01-01', null));
INSERT INTO t_author VALUES (2, 'Paulo', 'Coelho',
TO_DATE('1947-08-24', 'YYYY-MM-DD'), 1947, ROW(ROW('Caixa Postal',
'43.003'), null, 'Rio de Janeiro', 'Brazil', '1940-01-01', 2));

drop function if exists p_enhance_address2();
CREATE FUNCTION p_enhance_address2 (address OUT u_address_type)
AS $$
BEGIN
        SELECT t_author.address
        INTO address
        FROM t_author
        WHERE first_name = 'George';
END;
$$ LANGUAGE plpgsql;

select *from p_enhance_address2();



------------------------------------------------------dynexecute
drop table if exists tt_proc;
create table tt_proc(col1 ct_int, col2 int);
insert into tt_proc values(row(1,2),3);


drop function if exists test_prc_into1();
create or replace function test_prc_into1() returns ct_int as $$
declare
res ct_int;
begin
	execute 'select col1 from tt_proc' into res;
	return res;
end;
$$ LANGUAGE plpgsql;

select *from test_prc_into1();

drop function if exists test_prc_into2();
create or replace function test_prc_into2() returns int as $$
declare
res int;
begin
	execute 'select col2 from tt_proc' into res;
	return res;
end;
$$ LANGUAGE plpgsql;
select *from test_prc_into2();

------------------------------------------------------cursor
drop table if exists tt_staff;
create table tt_staff(section_id int,first_name VARCHAR2(20), phone_number VARCHAR2(20), salary NUMBER(8,2));
insert into tt_staff values(30, 'hlc','11111', 123);


DECLARE
    name          VARCHAR2(20);
    phone_number  VARCHAR2(20);
    salary        NUMBER(8,2);
    sqlstr        VARCHAR2(1024);

    TYPE app_ref_cur_type IS REF CURSOR;  --定义游标类型
    my_cur app_ref_cur_type;  --定义游标变量
    
BEGIN
    sqlstr := 'select first_name,phone_number,salary from tt_staff
         where section_id = :1';
    OPEN my_cur FOR sqlstr USING '30';  --打开游标, using是可选的
    FETCH my_cur INTO name, phone_number, salary; --获取数据
    WHILE my_cur%FOUND LOOP
          FETCH my_cur INTO name, phone_number, salary;
    END LOOP;
    CLOSE my_cur;   --关闭游标
END;
/


------------------------------------------------------一个成员的composite变量

create type cpt_1 as (ct int);
drop table if exists tt_cpt;
create table tt_cpt(id int, ct cpt_1);
insert into tt_cpt values(1,row(2));
drop function if exists test_ct1();
create or replace function test_ct1() returns setof cpt_1 as $$
declare
cpt cpt_1;
begin
	for cpt in select ct from tt_cpt
	loop
	return next cpt;
	end loop;
	return;
end;
$$LANGUAGE plpgsql;

select *from test_ct1();

drop function if exists test_ct2();
create or replace function test_ct2() returns setof  tt_cpt as $$
declare
cpt tt_cpt%rowtype;
begin
	for cpt in select * from tt_cpt
	loop
	return next cpt;
	end loop;
	return;
end;
$$LANGUAGE plpgsql;

select *from test_ct2();

CREATE TYPE dt_text AS
(col1 text,
 col2 text,
 col3 text
);

CREATE TYPE dt_int AS
(col1 int,
 col2 int,
 col3 int
);

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
  id INT,
  val dt_text
) ;

DROP TABLE IF EXISTS test_table2;
CREATE TABLE test_table2 (
  id INT,
  val1 dt_text,
  val2 dt_int
) ;

INSERT INTO test_table VALUES (1, ('a','b','c'));
INSERT INTO test_table2 VALUES (1, ('a','b','c'), (1,2,3));
drop function if exists get_row0();
CREATE OR REPLACE FUNCTION get_row0() RETURNS setof dt_text AS
$$
DECLARE
    r dt_text;
BEGIN
    FOR r IN SELECT val FROM test_table LOOP
    RETURN NEXT r;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM get_row0();
drop function if exists get_row1();
CREATE OR REPLACE FUNCTION get_row1() RETURNS setof test_table AS
$$
DECLARE
    r test_table%rowtype;
BEGIN
    FOR r IN SELECT * FROM test_table LOOP
    RETURN NEXT r;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM get_row1();
------------------------------------------------------列组合
drop function if exists get_row1();
CREATE OR REPLACE FUNCTION get_row1() RETURNS setof dt_text AS
$$
DECLARE
    r dt_text;
	id int;
BEGIN
    FOR id, r IN SELECT * FROM test_table LOOP
    RETURN NEXT r;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM get_row1();


drop function if exists get_row2();
CREATE OR REPLACE FUNCTION get_row2() RETURNS void AS
$$
DECLARE
    c1 text;
	c2 text;
BEGIN
    FOR c1, c2 IN SELECT val FROM test_table LOOP
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM get_row2();



-----------------------------------------------------匿名块
DECLARE
    r dt_text;
BEGIN
    SELECT val FROM test_table into r;
END;
/
		
drop table test_table;



-----------------------------------------------------function without arguments
DROP FUNCTION IF EXISTS func_add_sql_arg2;
DROP function IF EXISTS f_add_no_arg;
DROP FUNCTION IF EXISTS f_add;

CREATE FUNCTION func_add_sql_arg2(num1 integer, num2 integer) RETURN integer
AS
BEGIN 
RETURN num1 + num2;
END;
/

CREATE FUNCTION func_add_sql_no_arg() RETURN integer
AS
BEGIN 
RETURN 1;
END;
/

create or replace procedure f_add_no_arg
as
begin
    func_add_sql2;
end;
/

create or replace procedure f_add
as
begin
    func_add_sql2(3,4);
end;
/

call f_add();
call f_add_no_arg();

DROP FUNCTION IF EXISTS func_add_sql_arg2;
DROP function IF EXISTS f_add_no_arg;
DROP FUNCTION IF EXISTS f_add;

create or replace procedure func_proc_no_arg
as
begin
    func_add_sql_no_arg;
end;
/

call func_proc_no_arg();
\c regression;
drop database IF EXISTS pl_test_pkg;
