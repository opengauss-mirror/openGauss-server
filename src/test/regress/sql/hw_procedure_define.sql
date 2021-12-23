-------------------------------------------------------
-- Test A db style procedure and Function Defination
-------------------------------------------------------
create database pl_test_pkg_define DBCOMPATIBILITY 'pg';
\c pl_test_pkg_define;

SET CHECK_FUNCTION_BODIES TO ON;
CREATE FUNCTION test_forallDML1(IN iter int) RETURNS integer AS $$
BEGIN
        forall i in 1..iter
                CREATE ROLE jonathan PASSWORD 'gauss@123' LOGIN;
        return iter;
END;
$$ LANGUAGE plpgsql;
begin
    forall i in 1..iter
        CREATE ROLE jonathan PASSWORD 'gauss@123' LOGIN;
end;
/
CREATE PROCEDURE test_proc_define
(
	in_1  	IN VARCHAR2,
	in_2    VARCHAR2,
	out_1  	OUT VARCHAR2,
	inout_1  IN OUT VARCHAR
)
IS
BEGIN  
	out_1 	:= in_1;
	inout_1 := inout_1 || in_2;
END;
/
CALL test_proc_define('Hi', 'world', 'on bind', 'Hello' );

CREATE PROCEDURE   proc_no_parameter
AS
	ret1 int;
BEGIN  
	ret1 := 0;
END;
/
CALL proc_no_parameter();

CREATE FUNCTION test_func_define
(
   in_1  IN VARCHAR2,
   in_2  VARCHAR2
)
RETURN INTEGER 
IS	
	ret INTEGER;
BEGIN  
	ret := 1;
	RETURN ret;
END;
/
CALL test_func_define('Hello', 'World');

CREATE FUNCTION   func_no_parameter
RETURN INTEGER
AS
	ret1 int;
BEGIN  
	ret1 := 0;
	RETURN ret1;
END;
/
CALL func_no_parameter();

-------------------------------------------------------
-- Test Replace procedure
-------------------------------------------------------
--Replace parameter number
CREATE OR REPLACE PROCEDURE   test_proc_define
(
   in_1  IN VARCHAR2,
   in_2    VARCHAR2,
   out_1  OUT VARCHAR2
   -- inout_1  IN OUT VARCHAR
)
IS
BEGIN  
	out_1 := in_1;
	
END;
/
CALL test_proc_define('hello', 'world', 'NO BIND');

--Replace parameter type
CREATE OR REPLACE PROCEDURE   test_proc_define
(
   in_1  IN INTEGER,
   in_2    INTEGER,
   out_1  OUT INTEGER
)
IS
BEGIN  
	out_1 := in_1;
END;
/
CALL test_proc_define(1, 2, 0);

--Replace parameter NAME
CREATE OR REPLACE PROCEDURE   test_proc_define
(
   in1  IN INTEGER,
   in2    INTEGER,
   out1  OUT INTEGER
)
IS
BEGIN  
	out1 := in1;
END;
/
CALL test_proc_define(1, 2, 0);

-------------------------------------------------------
-- Test Replace function
-------------------------------------------------------
--BY NUMBER
CREATE OR REPLACE FUNCTION test_func_define
(
   in_1  IN VARCHAR2,
   in_2  VARCHAR2,
   in_3  VARCHAR2
)
RETURN INTEGER 
IS	
	ret INTEGER;
BEGIN  
	RETURN 2;
END;
/
CALL test_func_define('Hello', 'World', 'China');

--BY TYPE
CREATE OR REPLACE FUNCTION test_func_define
(
   in_1  IN INT,
   in_2  INT,
   in_3  INT
)
RETURN INTEGER 
IS	
	ret INTEGER;
BEGIN  
	RETURN 3;
END;
/
CALL test_func_define(1,2,3);

--BY RETURN
CREATE OR REPLACE FUNCTION test_func_define
(
   in_1  IN INT,
   in_2  INT,
   in_3  INT
)
RETURN VARCHAR 
IS	
	ret VARCHAR;
BEGIN  
	ret := 'function test_func_define replace with return type changed';
	RETURN ret;
END;
/
CALL test_func_define(1, 2, 3);

-- THE RESULT MUST BE 1!
SELECT COUNT(*) FROM pg_proc WHERE proname = 'test_proc_define';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'test_func_define';

-------------------------------------------------------
-- Test Drop Procedure
-------------------------------------------------------
DROP PROCEDURE proc_no_parameter;
DROP PROCEDURE test_proc_define;

-- expected an error; 
DROP PROCEDURE NOTEXIST;
-- expected a notice.
DROP PROCEDURE IF EXISTS NOTEXIST;

-- DROP THE PROCEDURE WITH NO PARAMETER
CREATE  OR REPLACE PROCEDURE test_drop_proc ( para1 INT)
AS  
BEGIN
    para1 := 1 ;
END;
/
DROP PROCEDURE IF EXISTS test_drop_proc;

-- THE RESULT MUST BE ZERO !
SELECT COUNT(*) FROM pg_proc WHERE proname = 'PROC_NO_PARAMETER';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_PROC_DEFINE';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_DROP_PROC';

-------------------------------------------------------
-- Test Drop Function
-------------------------------------------------------
DROP PROCEDURE func_no_parameter;
DROP PROCEDURE test_func_define;
DROP FUNCTION  NOTEXIST();
DROP FUNCTION  IF EXISTS NOTEXIST();
-- DROP THE PROCEDURE WITH NO PARAMETER
CREATE  OR REPLACE FUNCTION test_drop_func ( para1 INT)
RETURN NUMBER
AS  
BEGIN
    para1 := 1 ;
	RETURN 4;
END;
/
DROP FUNCTION IF EXISTS test_drop_func(int);
-- THE RESULT MUST BE ZERO !
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_PROC_DEFINE';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_FUNC_DEFINE';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'PROC_NO_PARAMETER';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'FUNC_NO_PARAMETER';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_DROP_PROC';
SELECT COUNT(*) FROM pg_proc WHERE proname = 'TEST_DROP_FUNC';

-------------------------------------------------------
-- Test Default Parameter
-------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_def_arg(para INT DEFAULT 1001)
AS
	 ret INTEGER;
begin 
	  ret := ret + para;
end;
/
CALL proc_def_arg();

CREATE OR REPLACE PROCEDURE func_def_arg(para INT := 1002)
AS
	 ret INTEGER;
begin 
	  ret := ret + para;
end;
/
CALL func_def_arg();

-------------------------------------------------------
-- Test Nested BEGIN/END
-------------------------------------------------------
CREATE OR REPLACE PROCEDURE test_multi_level
AS
	 ret INT;
begin 
	  ret := 0;
	  BEGIN
		  ret := ret + 1 ;
		  BEGIN
			 ret := ret + 1;
		  END;
	  END;
END;
/
CALL test_multi_level();

-------------------------------------------------------
-- Test Privilage
-------------------------------------------------------
--CREATE USER  USER1 PASSWORD 'gauss@123';
CREATE TABLE t_priv(id INTEGER);
CREATE OR REPLACE PROCEDURE proc_callas_definer
AUTHID DEFINER
AS
BEGIN 
	INSERT INTO t_priv VALUES(1);
END;
/
CREATE OR REPLACE PROCEDURE proc_callas_curr_user
AUTHID CURRENT_USER
AS
BEGIN 
	INSERT INTO t_priv VALUES(2);
END;
/
--CREATE USER  USER2 PASSWORD 'gauss@123';

-------------------------------------------------------
-- Clean up Test envirment
-------------------------------------------------------
DROP PROCEDURE proc_def_arg;
DROP FUNCTION func_def_arg;
DROP PROCEDURE test_multi_level;
DROP PROCEDURE proc_callas_definer;
DROP PROCEDURE proc_callas_curr_user;
DROP TABLE t_priv;


create or replace procedure test_blank (a int)
as




a int;
begin
end;



/

create or replace procedure test_blank1 (a int)
as
		

a int;
begin
end;
/

create or replace procedure test_blank2(a int)
as
a int;
begin
end;

		
/

create or replace procedure test_blank3(a int)
as
a int;
begin
end;  

		
/

create or replace procedure test_blank4(a int)
as
  

				
a int;
begin
end;  

   			
/

create or replace function test_blank5(a int)
returns int
as


$$

begin
return 1;
end;



$$ language plpgsql;

create or replace function test_blank6(a int)
returns int
as
$$

begin
return 1;
end;



$$ language plpgsql;

create or replace function test_blank7 (a int)
returns int
as
$$
begin
return 1;
end;



$$ language plpgsql;

create or replace function test_blank8(a int)
returns int
as
$$

begin
return 1;
end;
$$ language plpgsql;

create table test_blank_tbl (name varchar(50));
create table test_blank_tbl_log(log varchar(100));

create function test_blank_func returns trigger as $$
begin
	begin
		savepoint sp0;
		insert into test_blank_tbl_log values ('sp1');
		savepoint sp1;

		if (TG_OP = 'UPDATE') then
			rollback to sp0;
		end if;
--	exception
--		when in_failed_sql_transaction then
--			rollback to sp0;
--			return null;
	end;
	return null;
end;
$$ language plpgsql;


CREATE TRIGGER test_blank_trigger BEFORE INSERT OR UPDATE ON test_blank_tbl
	FOR EACH statement EXECUTE PROCEDURE test_blank_func();

SELECT proname, prosrc FROM pg_proc WHERE proname LIKE 'TEST_BLANK%';

DROP PROCEDURE test_blank;
DROP PROCEDURE test_blank1;
DROP PROCEDURE test_blank2;
DROP PROCEDURE test_blank3;
DROP PROCEDURE test_blank4;
DROP PROCEDURE test_blank5;
DROP PROCEDURE test_blank6;
DROP PROCEDURE test_blank7;
DROP PROCEDURE test_blank8;
DROP TRIGGER TEST_BLANK_TRIGGER on test_blank_tbl;
DROP TABLE test_blank_tbl;
DROP TABLE test_blank_tbl_log;
 

create or replace procedure sp_comment(a int, b int, c text) as
begin
	return;
end;
/

--- OK
create or replace procedure sp_comment0(name text) as
begin
	sp_comment(1, 	
		1, 			
		'abc'		
	);
end;
/

create or replace procedure sp_comment1(name text) as
begin
	sp_comment(1, 	-- comment 
		1, 			-- comment
		'abc'		-- comment
	);
end;
/
create or replace procedure sp_comment2(name text) as
begin
	sp_comment(1, 	-- comment 
		1, 			
		'abc'
	);
end;
/

create or replace procedure sp_comment3(name text) as
begin
	sp_comment(1, 	
		1, 			-- comment	
		'abc'
	);
end;
/

create or replace procedure sp_comment4(name text) as
begin
	sp_comment(1, 	
		1, 			
		'abc'		-- comment
	);
end;
/

create or replace procedure sp_comment5(name text) as
begin
	sp_comment(1, 	
		1, 			
		'abc');-- comment
end ;
/

create or replace procedure sp_comment6(name text) as
begin
	sp_comment(1 	-- comment
		, 			-- comment
		1			-- comment
		, 			-- comment
		'abc'		-- comment
	);				-- comment
end ;
/
create or replace procedure sp_comment7(name text) as
begin
	sp_comment(1 	-- comment
		, 			-- comment
		1			-- comment
		, 			-- comment
		'abc');		-- comment
end ;
/
create or replace procedure sp_comment8(name text) as
begin
	sp_comment(1 	/* comment */
		, 			/* comment */
		1			/* comment */
		, 			/* comment */
		'abc');		/* comment */
end ;
/
create or replace procedure sp_comment9(name text) as
begin
	sp_comment(1 	/* comment */
		, 			/* comment */
		1			/* comment */
		, 			/* comment */
		'abc'		/* comment */
		);		/* comment */
end ;
/
create or replace procedure sp_comment10(name text) as
begin
	sp_comment(1 	/* comment */
/* c */		, 			/* comment */
/* c */		1			/* comment */
/* c */		, 			/* comment */
/* c */		'abc'		/* comment */
/* c */		);		/* comment */
end ;
/
select sp_comment0('test');
select sp_comment1('test');
select sp_comment2('test');
select sp_comment3('test');
select sp_comment4('test');
select sp_comment5('test');
select sp_comment6('test');
select sp_comment7('test');
select sp_comment8('test');
select sp_comment9('test');
select sp_comment10('test');

drop procedure sp_comment;
drop procedure sp_comment0;
drop procedure sp_comment1;
drop procedure sp_comment2;
drop procedure sp_comment3;
drop procedure sp_comment4;
drop procedure sp_comment5;
drop procedure sp_comment6;
drop procedure sp_comment7;
drop procedure sp_comment8;
drop procedure sp_comment9;
drop procedure sp_comment10;
--user with create privilege on some schema should create function successfully on that schema
CREATE SCHEMA FVT_OBJ_DEFINE;
CREATE USER SCHEMA_AUTHORITY_USER_005 PASSWORD 'Gauss@123';
GRANT CREATE ON SCHEMA FVT_OBJ_DEFINE TO SCHEMA_AUTHORITY_USER_005;
SET ROLE SCHEMA_AUTHORITY_USER_005 PASSWORD 'Gauss@123';
CREATE FUNCTION FVT_OBJ_DEFINE.SCHEMA_AUTHORITY_FUNCTION_005() RETURN INT
AS
BEGIN
	RAISE INFO 'Hello!';
	RETURN 0;
END;
/
CREATE PROCEDURE FVT_OBJ_DEFINE.SCHEMA_AUTHORITY_PROCEDURE_006() AS
BEGIN
	RAISE INFO 'Hello!';
END;
/
SELECT PRONAME FROM PG_PROC WHERE PRONAMESPACE = (SELECT OID FROM PG_NAMESPACE WHERE NSPNAME = 'fvt_obj_define') ORDER BY PRONAME;
RESET ROLE;
DROP SCHEMA FVT_OBJ_DEFINE CASCADE;
DROP USER SCHEMA_AUTHORITY_USER_005 CASCADE;

CREATE OR REPLACE PROCEDURE PRO_NO_EXP_001_1() AS
DECLARE
BEGIN
CREATE USER U_PRO_NO_EXP_001_1 PASSWORD 'Gauss_234';
DROP USER U_PRO_NO_EXP_001_1 CASCADE;
END;
/

CALL PRO_NO_EXP_001_1();
CALL PRO_NO_EXP_001_1();

create table test_emp_001(name varchar(10));
create or replace procedure test_proc_using_001(a int) SHIPPABLE LEAKPROOF CALLED ON NULL INPUT external security invoker cost 0.000056
as
    v_sql varchar2(2000);
begin
    v_sql := 'insert into test_emp_001 values (:v1)';
    execute immediate v_sql using 'kimy';
end;
/
call test_proc_using_001(1);
select * from test_emp_001;
select prosecdef,procost,proleakproof,proisstrict,proshippable,prokind from pg_proc where proname='test_proc_using_001';


create or replace procedure p_definer() AUTHID DEFINER
is
begin
commit;
end;
/

create or replace procedure p_caller() AUTHID DEFINER
is
begin
p_definer();
end;
/

call p_definer();

drop procedure p_definer;
drop procedure p_caller;
drop table test_emp_001;
drop procedure test_proc_using_001;
\c regression;
drop database IF EXISTS pl_test_pkg_define;
