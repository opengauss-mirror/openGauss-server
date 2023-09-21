-- declare handler 
drop database if exists mysql_test;
drop database if exists td_test;

create database mysql_test dbcompatibility='B';
create database td_test dbcompatibility='C';

\c td_test

declare
    a int;
begin
    declare exit handler for 22012
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

\c mysql_test
-- error_code
declare
    a int;
begin
    declare exit handler for 22012
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for 1
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for 0
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- sqlstate [value] sqlstate_value
declare
    a int;
begin
    declare exit handler for sqlstate '22012'
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for sqlstate value "22012"
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

-- condition_name
declare
    a int;
begin
    declare exit handler for DIVISION_BY_ZERO
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- SQLWARNING
DROP USER pri_user_independent cascade;
declare
begin
    declare exit handler for sqlwarning
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    CREATE USER pri_user_independent WITH INDEPENDENT IDENTIFIED BY "1234@abc";
end;
/

DROP USER pri_user_independent cascade;
declare
begin
    declare exit handler for "sqlwarning"
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    CREATE USER pri_user_independent WITH INDEPENDENT IDENTIFIED BY "1234@abc";
end;
/

-- NOT FOUND
declare
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
end;
/

-- sqlexception
declare
    a int;
begin
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/

--condition_values
declare
    a int;
begin
    declare exit handler for sqlexception, not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
end;
/
-- declare handlers
declare
    a int;
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
    a := 1/0;
end;
/

declare
    a int;
begin
    declare exit handler for not FOUND
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
    create table t_rowcompress_pglz_compresslevel(id int) with (compresstype=1,compress_level=2);
end;
/

-- use declare handler and exception when at the same time
declare
    a int;
begin
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
    a := 1/0;
    exception when others then
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;
end;
/

-- delcare continue handler for condition_value
create table declare_handler_t_continue (i INT PRIMARY KEY, j INT);
create table declare_handler_t_exit (i INT PRIMARY KEY, j INT);

CREATE OR REPLACE PROCEDURE proc_continue_sqlexception()  IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        RAISE NOTICE 'SQLEXCEPTION HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (2, 1);
    RAISE division_by_zero;
    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (3, 1);
END;
/
call proc_continue_sqlexception();
SELECT * FROM declare_handler_t_continue ORDER BY i;
TRUNCATE TABLE declare_handler_t_continue;

-- declare continue handler
CREATE OR REPLACE PROCEDURE proc_continue_sqlexception()  IS
BEGIN
    DECLARE CONTINUE HANDLER FOR unique_violation
        RAISE NOTICE 'SQLEXCEPTION HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (2, 1);
    INSERT INTO declare_handler_t_continue VALUES (1, 1);
    INSERT INTO declare_handler_t_continue VALUES (3, 1);
END;
/
call proc_continue_sqlexception();
SELECT * FROM declare_handler_t_continue ORDER BY i;
-- declare exit handler
CREATE OR REPLACE PROCEDURE proc_ex()  IS
BEGIN
    DECLARE EXIT HANDLER FOR unique_violation
        RAISE NOTICE 'unique_violation HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;

    INSERT INTO declare_handler_t_exit VALUES (1, 1);
    INSERT INTO declare_handler_t_exit VALUES (2, 1);
    INSERT INTO declare_handler_t_exit VALUES (1, 1); /* duplicate key */
    INSERT INTO declare_handler_t_exit VALUES (3, 1);
END;
/
call proc_ex();
SELECT * FROM declare_handler_t_exit ORDER BY i;
CREATE OR REPLACE PROCEDURE proc_null()  IS
BEGIN
    DECLARE EXIT HANDLER FOR unique_violation
        RAISE NOTICE 'unique_violation HANDLER: SQLSTATE = %, SQLERRM = %', SQLSTATE, SQLERRM;
END;
/
call proc_null();
CREATE TABLE tb1(
col1 INT PRIMARY KEY,
col2 text
);
CREATE OR REPLACE PROCEDURE proc1(IN col1 INT, IN col2 text) AS
DECLARE result VARCHAR;
declare pragma autonomous_transaction;
BEGIN
DECLARE CONTINUE HANDLER FOR 23505
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
end;
if col1>10 then  
INSERT INTO tb1 VALUES(col1,'lili');
END IF;  
IF col1 <= 10 THEN
INSERT INTO tb1(col1,col2) VALUES(col1,col2);
commit;
ELSE
INSERT INTO tb1(col1,col2) VALUES(col1,col2);
rollback;
END IF;
END;
/
call proc1(1, 1);
call proc1(1, 5);
call proc1(11, 11);
call proc1(11, 5);
select * from tb1;

CREATE OR REPLACE PROCEDURE proc1(IN a text) AS
BEGIN
if a='22012' then
raise info 'zero error';
else
raise info 'emmm....';
end if;
end;
/
CREATE OR REPLACE PROCEDURE proc2(IN var1 int,var2 int) AS
begin
DECLARE CONTINUE HANDLER FOR sqlstate'22012'
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
var1=0;
end;
var1= var1 / var2;
RAISE INFO 'result: %', var1;
END;
/
CREATE OR REPLACE PROCEDURE proc3(a1 int,b1 int) AS
BEGIN
DECLARE CONTINUE HANDLER FOR sqlstate'22012',sqlstate'0A000'
begin
RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
perform proc1(SQLSTATE);
end;
a1=a1/b1;
IF b1 = 0 THEN
raise info 'b1 is zero';
create table tb1();
perform proc2(b1, a1);
END IF;
raise info 'END';
END;
/
CALL proc3(1,0);
CALL proc3(0,0);
create table company(name varchar(100), loc varchar(100), no integer PRIMARY KEY);
insert into company values ('macrosoft',    'usa',          001);
insert into company values ('oracle',       'usa',          002);
insert into company values ('backberry',    'canada',       003);
create or replace procedure test_cursor_handler()
as

  declare company_name    varchar(100);
  declare company_loc varchar(100);
  declare company_no  integer;
begin
  DECLARE CONTINUE HANDLER FOR unique_violation 
  begin 
    RAISE NOTICE 'SQLSTATE = %',SQLSTATE;
  end;
  declare c1_all cursor is --cursor without args 
      select name, loc, no from company order by 1, 2, 3;
  if not c1_all%isopen then
      open c1_all;
  end if;
  loop
      fetch c1_all into company_name, company_loc, company_no;
      exit when c1_all%notfound;
      insert into company values (company_name,company_loc,company_no);
      raise notice '% : % : %',company_name,company_loc,company_no;
  end loop;
  if c1_all%isopen then
      close c1_all;
  end if;
end;
/
call test_cursor_handler();

-- get diagnostics
set enable_set_variable_b_format = on;
set b_format_behavior_compat_options = 'diagnostics';
set @class_origin='',@subclass_origin='',@returned_sqlstate='',@message_text= '',@mysql_errno='',@constraint_catalog='',@constraint_schema='',@constraint_name='',@catalog_name='',@schema_name='',@table_name='',@column_name='',@cursor_name='';
drop table xx;
GET diagnostics @num = NUMBER, @row = ROW_COUNT;
GET diagnostics condition @num @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
GET diagnostics condition 2 @class_origin=CLASS_ORIGIN;
show errors;
select @num, @row;
select @class_origin,@subclass_origin,@returned_sqlstate,@message_text,@mysql_errno,@constraint_catalog,@constraint_schema,@constraint_name,@catalog_name,@schema_name,@table_name,@column_name,@cursor_name;
GET stacked diagnostics @num = NUMBER, @row = ROW_COUNT;

-- condition_number
set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition 1.1 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition 2 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
show errors;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition 1.8 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
show errors;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition '1' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition "1" @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition B'1' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
drop table xx;
GET diagnostics condition B'10' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
show warnings;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
set @con=1;
drop table xx;
GET diagnostics condition @con @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
set @con=1.1;
drop table xx;
GET diagnostics condition @con @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

set @retSqlstate = '', @msg = '';
set @con='1';
drop table xx;
GET diagnostics condition @con @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
select @retSqlstate, @msg;

DROP TABLE IF EXISTS t1 ; 
CREATE TABLE t1(c1 TEXT NOT NULL); 
CREATE OR REPLACE PROCEDURE prc() 
AS
DECLARE num INT;  
DECLARE errcount INT; 
DECLARE errno INT; 
DECLARE msg TEXT; 
BEGIN 
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN 
        -- Here the current DA is nonempty because no prior statements 
        -- executing within the handler have cleared it 
        GET CURRENT DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT; 
        RAISE NOTICE 'current DA before mapped insert , error = % , msg = %', errno, msg; 
        GET STACKED DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT; 
        RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', errno, msg; 

        INSERT INTO t1 (c1) VALUES(0),(1),(2);

        GET CURRENT DIAGNOSTICS num=NUMBER,errcount = ROW_COUNT;
        RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', num, errcount ;
        GET STACKED DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
        RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', errno, msg;
    END;
  
    GET CURRENT DIAGNOSTICS num=NUMBER,errcount = ROW_COUNT;
    RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', num, errcount ;

    INSERT INTO t1 (c1) VALUES(NULL);

    GET CURRENT DIAGNOSTICS num=NUMBER,errcount = ROW_COUNT;
    RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', num, errcount ;
    GET STACKED DIAGNOSTICS num=NUMBER,errcount = ROW_COUNT;

END; 
/ 
call prc();
show errors;

CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN 
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN 
        -- Here the current DA is nonempty because no prior statements 
        -- executing within the handler have cleared it 
        GET CURRENT DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; 
        RAISE NOTICE 'current DA before mapped insert , error = % , msg = %', @errno, @msg; 
        GET STACKED DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT; 
        RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', @errno, @msg; 

        INSERT INTO t1 (c1) VALUES(0),(1),(2);

        GET CURRENT DIAGNOSTICS @num=NUMBER,@errcount = ROW_COUNT;
        RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', @num, @errcount ;
        GET STACKED DIAGNOSTICS CONDITION 1 @errno = MYSQL_ERRNO, @msg = MESSAGE_TEXT;
        RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', @errno, @msg;
    END;
  
    GET CURRENT DIAGNOSTICS @num=NUMBER,@errcount = ROW_COUNT;
    RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', @num, @errcount ;

    INSERT INTO t1 (c1) VALUES(NULL);

    GET CURRENT DIAGNOSTICS @num=NUMBER,@errcount = ROW_COUNT;
    RAISE NOTICE 'current DA before mapped insert , num = % , errcount  = %', @num, @errcount ;

END; 
/ 
call prc();

CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition 1 @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
    END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
select @class_origin,@subclass_origin,@returned_sqlstate,@message_text,@mysql_errno,@constraint_catalog,@constraint_schema,@constraint_name,@catalog_name,@schema_name,@table_name,@column_name,@cursor_name;
--condition_number

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition 1 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition 1.1 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition 1.8 @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition '1' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition "1" @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition B'1' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition B'10' @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @con=1;
set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition @con @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition true @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
show warnings;
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition false @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
show warnings;
select @retSqlstate, @msg;

set @retSqlstate='', @msg='';
CREATE OR REPLACE PROCEDURE prc() 
AS
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics condition NULL @retSqlstate = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT;
        END;
    INSERT INTO t1 (c1) VALUES(NULL);
END;
/
call prc();
show warnings;
select @retSqlstate, @msg;

-- Interaction with SIGNAL
CREATE or replace PROCEDURE p1() IS
  DECLARE errno INT DEFAULT 0;
  DECLARE msg TEXT;
BEGIN
  DECLARE cond CONDITION FOR SQLSTATE "01234";
  DECLARE CONTINUE HANDLER for cond
  BEGIN
    GET DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, msg = MESSAGE_TEXT;
  END;

  SIGNAL cond SET MESSAGE_TEXT = 'Signal message', MYSQL_ERRNO = 1012;
  RAISE NOTICE 'error = % , message  = %', errno, msg ;
END;
/

--vertical_results
CALL p1();

CREATE or replace PROCEDURE p1() IS
BEGIN
  SIGNAL SQLSTATE '77777' SET MYSQL_ERRNO = 1000, MESSAGE_TEXT='ÁÂÃÅÄ';
END;
/

--error 1000
CALL p1();

GET DIAGNOSTICS CONDITION 1
  @mysql_errno = MYSQL_ERRNO, @message_text = MESSAGE_TEXT,
  @returned_sqlstate = RETURNED_SQLSTATE, @class_origin = CLASS_ORIGIN;

--vertical_results
SELECT @mysql_errno, @message_text, @returned_sqlstate, @class_origin;

CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE cond CONDITION FOR SQLSTATE '12345';
  SIGNAL cond SET
    CLASS_ORIGIN = 'CLASS_ORIGIN text',
    SUBCLASS_ORIGIN = 'SUBCLASS_ORIGIN text',
    CONSTRAINT_CATALOG = 'CONSTRAINT_CATALOG text',
    CONSTRAINT_SCHEMA = 'CONSTRAINT_SCHEMA text',
    CONSTRAINT_NAME = 'CONSTRAINT_NAME text',
    CATALOG_NAME = 'CATALOG_NAME text',
    SCHEMA_NAME = 'SCHEMA_NAME text',
    TABLE_NAME = 'TABLE_NAME text',
    COLUMN_NAME = 'COLUMN_NAME text',
    CURSOR_NAME = 'CURSOR_NAME text',
    MESSAGE_TEXT = 'MESSAGE_TEXT text',
    MYSQL_ERRNO = 9999;
END;
/

--error 9999
CALL p1();

GET DIAGNOSTICS CONDITION 1
  @class_origin = CLASS_ORIGIN,
  @subclass_origin = SUBCLASS_ORIGIN,
  @constraint_catalog = CONSTRAINT_CATALOG,
  @constraint_schema = CONSTRAINT_SCHEMA,
  @constraint_name = CONSTRAINT_NAME,
  @catalog_name = CATALOG_NAME,
  @schema_name = SCHEMA_NAME,
  @table_name = TABLE_NAME,
  @column_name = COLUMN_NAME,
  @cursor_name = CURSOR_NAME,
  @message_text = MESSAGE_TEXT,
  @mysql_errno = MYSQL_ERRNO,
  @returned_sqlstate = RETURNED_SQLSTATE;

--vertical_results
SELECT
  @class_origin,
  @subclass_origin,
  @constraint_catalog,
  @constraint_schema,
  @constraint_name,
  @catalog_name,
  @schema_name,
  @table_name,
  @column_name,
  @cursor_name,
  @message_text,
  @mysql_errno,
  @returned_sqlstate;

-- GET DIAGNOSTICS doesn't clear the diagnostics area.
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      GET CURRENT DIAGNOSTICS CONDITION 1 @x = RETURNED_SQLSTATE;
      GET stacked DIAGNOSTICS CONDITION 1 @x1 = RETURNED_SQLSTATE;
      SIGNAL SQLSTATE '01002';
      GET CURRENT DIAGNOSTICS CONDITION 1 @y = RETURNED_SQLSTATE;
      GET stacked DIAGNOSTICS CONDITION 1 @y1 = RETURNED_SQLSTATE;
    END;
  SIGNAL SQLSTATE '01001';
END;
/

CALL p1();
SELECT @x,@x1, @y,@y1;

CREATE or replace PROCEDURE p1(OUT num INT, INOUT message TEXT) IS
BEGIN
  DECLARE warn CONDITION FOR SQLSTATE "01234";
  DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      GET DIAGNOSTICS num = NUMBER;
      GET DIAGNOSTICS CONDITION 1 message = MESSAGE_TEXT;
    END;
  SIGNAL warn SET MESSAGE_TEXT = 'inout parameter';
END;
/
SET @var1 = 0;
SET @var2 = 'message text';
CALL p1(@var1, @var2);
SELECT @var1, @var2;

CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLWARNING
  BEGIN
    -- Should be identical
    GET CURRENT DIAGNOSTICS @num1=NUMBER;
    GET stacked DIAGNOSTICS @num2=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg1 = MESSAGE_TEXT, @errno1 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg2 = MESSAGE_TEXT, @errno2 = MYSQL_ERRNO;
    RESIGNAL SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';

    -- Should be changed, but still identical
    GET CURRENT DIAGNOSTICS @num3=NUMBER;
    GET stacked DIAGNOSTICS @num4=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg3 = MESSAGE_TEXT, @errno3 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg4 = MESSAGE_TEXT, @errno4 = MYSQL_ERRNO;

    RESIGNAL SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal, for caller';
  END;
  SIGNAL SQLSTATE '01001';
END;
/
CALL p1();
show warnings;
select @num1,@num2,@num3,@num4;
select @msg1,@errno1;
select @msg2,@errno2;
select @msg3,@errno3;
select @msg4,@errno4;

CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLWARNING
  BEGIN
    -- Should be identical
    GET CURRENT DIAGNOSTICS @num1=NUMBER;
    GET stacked DIAGNOSTICS @num2=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg1 = MESSAGE_TEXT, @errno1 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg2 = MESSAGE_TEXT, @errno2 = MYSQL_ERRNO;
    SIGNAL SQLSTATE '01002' SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';

    -- Should be changed, but still identical
    GET CURRENT DIAGNOSTICS @num3=NUMBER;
    GET stacked DIAGNOSTICS @num4=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg3 = MESSAGE_TEXT, @errno3 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg4 = MESSAGE_TEXT, @errno4 = MYSQL_ERRNO;

    RESIGNAL SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal, for caller';
  END;
  SIGNAL SQLSTATE '01001';
END;
/
CALL p1();
show warnings;
select @num1,@num2,@num3,@num4;
select @msg1,@errno1;
select @msg2,@errno2;
select @msg3,@errno3;
select @msg4,@errno4;

CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLWARNING
  BEGIN
    -- Should be identical
    GET CURRENT DIAGNOSTICS @num1=NUMBER;
    GET stacked DIAGNOSTICS @num2=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg1 = MESSAGE_TEXT, @errno1 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg2 = MESSAGE_TEXT, @errno2 = MYSQL_ERRNO;
    SIGNAL SQLSTATE '01002' SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';

    -- Should be changed, but still identical
    GET CURRENT DIAGNOSTICS @num3=NUMBER;
    GET stacked DIAGNOSTICS @num4=NUMBER;
    GET CURRENT DIAGNOSTICS CONDITION 1 @msg3 = MESSAGE_TEXT, @errno3 = MYSQL_ERRNO;
    GET STACKED DIAGNOSTICS CONDITION 1 @msg4 = MESSAGE_TEXT, @errno4 = MYSQL_ERRNO;

    SIGNAL SQLSTATE '01003' SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal, for caller';
  END;
  SIGNAL SQLSTATE '01001';
END;
/
CALL p1();
show warnings;
select @num1,@num2,@num3,@num4;
select @msg1,@errno1;
select @msg2,@errno2;
select @msg3,@errno3;
select @msg4,@errno4;
drop procedure p1;

set b_format_behavior_compat_options = '';
CREATE OR REPLACE PROCEDURE prc() 
AS
declare num1,num2,num3 int;
declare row1,row2,row3 int;
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics num1 = NUMBER, row1 = ROW_COUNT;
        insert into t1(c1) VALUES(1),(2),(3);
        GET diagnostics num2 = NUMBER, row2 = ROW_COUNT;
    END;
    INSERT INTO t1 (c1) VALUES(NULL);
    GET diagnostics num3 = NUMBER, row3 = ROW_COUNT;
    RAISE NOTICE 'num1 = % , num2  = %, num1=2 = % , row1  = %, row2 = % , row3  = %', num1,num2,num3,row1,row2,row3;
END;
/
call prc();

set b_format_behavior_compat_options = 'diagnostics';

CREATE OR REPLACE PROCEDURE prc() 
AS
declare num1,num2,num3 int;
declare row1,row2,row3 int;
BEGIN
    DECLARE CONTINUE HANDLER FOR 23502
    BEGIN
        GET diagnostics num1 = NUMBER, row1 = ROW_COUNT;
        insert into t1(c1) VALUES(1),(2),(3);
        GET diagnostics num2 = NUMBER, row2 = ROW_COUNT;
    END;
    INSERT INTO t1 (c1) VALUES(NULL);
    GET diagnostics num3 = NUMBER, row3 = ROW_COUNT;
    RAISE NOTICE 'num1 = % , num2  = %, num1=2 = % , row1  = %, row2 = % , row3  = %', num1,num2,num3,row1,row2,row3;
END;
/
call prc();

set @class_origin='',@subclass_origin='',@returned_sqlstate='',@message_text= '',@mysql_errno='',@constraint_catalog='',@constraint_schema='',@constraint_name='',@catalog_name='',@schema_name='',@table_name='',@column_name='',@cursor_name='';
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE warn CONDITION FOR SQLSTATE "01234";
  DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
      GET diagnostics condition 1 @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
    END;
  SIGNAL warn SET MESSAGE_TEXT = 'inout parameter';
END;
/

call p1();
SELECT
  @class_origin,
  @subclass_origin,
  @constraint_catalog,
  @constraint_schema,
  @constraint_name,
  @catalog_name,
  @schema_name,
  @table_name,
  @column_name,
  @cursor_name,
  @message_text,
  @mysql_errno,
  @returned_sqlstate;

set @class_origin='',@subclass_origin='',@returned_sqlstate='',@message_text= '',@mysql_errno='',@constraint_catalog='',@constraint_schema='',@constraint_name='',@catalog_name='',@schema_name='',@table_name='',@column_name='',@cursor_name='';
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      RESIGNAL;
      GET diagnostics condition 1 @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
    END;
  drop table xx;
END;
/

call p1();
SELECT
  @class_origin,
  @subclass_origin,
  @constraint_catalog,
  @constraint_schema,
  @constraint_name,
  @catalog_name,
  @schema_name,
  @table_name,
  @column_name,
  @cursor_name,
  @message_text,
  @mysql_errno,
  @returned_sqlstate;

set @class_origin='',@subclass_origin='',@returned_sqlstate='',@message_text= '',@mysql_errno='',@constraint_catalog='',@constraint_schema='',@constraint_name='',@catalog_name='',@schema_name='',@table_name='',@column_name='',@cursor_name='';
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      RESIGNAL SQLSTATE '01002' SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';
      GET stacked diagnostics condition 1 @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
    END;
  drop table xx;
END;
/

call p1();
SELECT
  @class_origin,
  @subclass_origin,
  @constraint_catalog,
  @constraint_schema,
  @constraint_name,
  @catalog_name,
  @schema_name,
  @table_name,
  @column_name,
  @cursor_name,
  @message_text,
  @mysql_errno,
  @returned_sqlstate;

set @class_origin1='',@subclass_origin1='',@returned_sqlstate1='',@message_text1= '',@mysql_errno1='',@constraint_catalog1='',@constraint_schema1='',@constraint_name1='',@catalog_name1='',@schema_name1='',@table_name1='',@column_name1='',@cursor_name1='';
set @class_origin2='',@subclass_origin2='',@returned_sqlstate2='',@message_text2= '',@mysql_errno2='',@constraint_catalog2='',@constraint_schema2='',@constraint_name2='',@catalog_name2='',@schema_name2='',@table_name2='',@column_name2='',@cursor_name2='';
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      RESIGNAL SQLSTATE '01002' SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';
      GET diagnostics condition 1 @class_origin1=CLASS_ORIGIN,@subclass_origin1=SUBCLASS_ORIGIN,@returned_sqlstate1=RETURNED_SQLSTATE,@message_text1= MESSAGE_TEXT,@mysql_errno1=MYSQL_ERRNO,@constraint_catalog1=CONSTRAINT_CATALOG,@constraint_schema1=CONSTRAINT_SCHEMA,@constraint_name1=CONSTRAINT_NAME,@catalog_name1=CATALOG_NAME,@schema_name1=SCHEMA_NAME,@table_name1=TABLE_NAME,@column_name1=COLUMN_NAME,@cursor_name1=CURSOR_NAME;
      GET diagnostics condition 2 @class_origin2=CLASS_ORIGIN,@subclass_origin2=SUBCLASS_ORIGIN,@returned_sqlstate2=RETURNED_SQLSTATE,@message_text2= MESSAGE_TEXT,@mysql_errno2=MYSQL_ERRNO,@constraint_catalog2=CONSTRAINT_CATALOG,@constraint_schema2=CONSTRAINT_SCHEMA,@constraint_name2=CONSTRAINT_NAME,@catalog_name2=CATALOG_NAME,@schema_name2=SCHEMA_NAME,@table_name2=TABLE_NAME,@column_name2=COLUMN_NAME,@cursor_name2=CURSOR_NAME;
    END;
  drop table xx;
END;
/

call p1();
SELECT
  @class_origin1,
  @subclass_origin1,
  @constraint_catalog1,
  @constraint_schema1,
  @constraint_name1,
  @catalog_name1,
  @schema_name1,
  @table_name1,
  @column_name1,
  @cursor_name1,
  @message_text1,
  @mysql_errno1,
  @returned_sqlstate1;
SELECT
  @class_origin2,
  @subclass_origin2,
  @constraint_catalog2,
  @constraint_schema2,
  @constraint_name2,
  @catalog_name2,
  @schema_name2,
  @table_name2,
  @column_name2,
  @cursor_name2,
  @message_text2,
  @mysql_errno2,
  @returned_sqlstate2;

set @class_origin='',@subclass_origin='',@returned_sqlstate='',@message_text= '',@mysql_errno='',@constraint_catalog='',@constraint_schema='',@constraint_name='',@catalog_name='',@schema_name='',@table_name='',@column_name='',@cursor_name='';
CREATE or replace PROCEDURE p1() IS
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      RESIGNAL SET MYSQL_ERRNO= 9999, MESSAGE_TEXT= 'Changed by resignal';
      GET diagnostics condition 1 @class_origin=CLASS_ORIGIN,@subclass_origin=SUBCLASS_ORIGIN,@returned_sqlstate=RETURNED_SQLSTATE,@message_text= MESSAGE_TEXT,@mysql_errno=MYSQL_ERRNO,@constraint_catalog=CONSTRAINT_CATALOG,@constraint_schema=CONSTRAINT_SCHEMA,@constraint_name=CONSTRAINT_NAME,@catalog_name=CATALOG_NAME,@schema_name=SCHEMA_NAME,@table_name=TABLE_NAME,@column_name=COLUMN_NAME,@cursor_name=CURSOR_NAME;
    END;
  drop table xx;
END;
/

call p1();
SELECT
  @class_origin,
  @subclass_origin,
  @constraint_catalog,
  @constraint_schema,
  @constraint_name,
  @catalog_name,
  @schema_name,
  @table_name,
  @column_name,
  @cursor_name,
  @message_text,
  @mysql_errno,
  @returned_sqlstate;

-- test access to exception data
create function zero_divide() returns int as $$
declare v int := 0;
begin
  return 10 / v;
end;
$$ language plpgsql;

create function stacked_diagnostics_test() returns void as $$
declare _sqlstate text;
        _message text;
        _context text;
begin
  perform zero_divide();
exception when others then
  get stacked diagnostics
        _sqlstate = returned_sqlstate,
        _message = message_text,
        _context = pg_exception_context;
  raise notice 'sqlstate: %, message: %, context: [%]',
    _sqlstate, _message, replace(_context, E'\n', ' <- ');
end;
$$ language plpgsql;

select stacked_diagnostics_test();
CREATE OR REPLACE PROCEDURE p_resig1() IS
begin
DECLARE EXIT HANDLER FOR SQLSTATE '42P01'
BEGIN
RESIGNAL;
END;
DROP TABLE t1;
end;
/
call p_resig1();
get diagnostics condition 1 @p1 = CLASS_ORIGIN,@p2 = SUBCLASS_ORIGIN,@p3 = MESSAGE_TEXT,@p4 = MYSQL_ERRNO,@p5 = CONSTRAINT_CATALOG,@p6 = CONSTRAINT_SCHEMA,
@p7 = CONSTRAINT_NAME,@p8 = CATALOG_NAME,@p9 = SCHEMA_NAME,@p10 = TABLE_NAME,@p11 = COLUMN_NAME,@p12 = CURSOR_NAME;
select @p1,@p2,@p3,@p4;

-- core 
drop table if exists t1;
create table t3 (w char unique, x char);
insert into t3 values ('a', 'b');

create or replace procedure bug6900_9074(a int)
AS
begin
  declare exit handler for sqlstate '23000' 
  begin
      RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
  end;
  begin
    declare exit handler for sqlexception
    begin
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %', SQLSTATE, SQLCODE, SQLERRM;
    end;

    if a = 1 then
      insert into t3 values ('a', 'b');
    elseif a = 2 then
      insert into t3 values ('c', 'd');
    else
      insert into t3 values ('x', 'y', 'z');
    end if;
  end;
  drop table t1;
end;
/
call bug6900_9074(0);
call bug6900_9074(1);
call bug6900_9074(2);
drop procedure bug6900_9074;
CREATE TABLE t1(c1 TEXT NOT NULL); 
CREATE OR REPLACE PROCEDURE p_1145188()
AS
BEGIN
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
BEGIN
-- Here the current DA is nonempty because no prior statements
-- executing within the handler have cleared it
GET CURRENT DIAGNOSTICS @num=NUMBER,@rowcount=ROW_COUNT;
RAISE NOTICE 'NUMBER=%,ROW_COUNT=%',@num,@rowcount;
GET CURRENT DIAGNOSTICS CONDITION 1 @luser = MYSQL_ERRNO, @msg = MESSAGE_TEXT;
RAISE NOTICE 'current DA before mapped insert , error = % , msg = %', @errno, @msg;
GET STACKED DIAGNOSTICS @num=NUMBER,@rowcount=ROW_COUNT;
RAISE NOTICE 'NUMBER=%,ROW_COUNT=%',@num,@rowcount;
GET STACKED DIAGNOSTICS CONDITION 1 @errno1 = MYSQL_ERRNO, @mingshigang2 = MESSAGE_TEXT;
RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', @errno1, @msg1;

INSERT INTO t1 (c1) VALUES(0),(1),(2);
GET CURRENT DIAGNOSTICS @num=NUMBER,@rowcount=ROW_COUNT;
RAISE NOTICE 'NUMBER=%,ROW_COUNT=%',@num,@rowcount;
GET CURRENT DIAGNOSTICS CONDITION 1 @luser = MYSQL_ERRNO, @msg = MESSAGE_TEXT;
RAISE NOTICE 'current DA before mapped insert , error = % , msg = %', @errno, @msg;
GET STACKED DIAGNOSTICS @num=NUMBER,@rowcount=ROW_COUNT;
RAISE NOTICE 'NUMBER=%,ROW_COUNT=%',@num,@rowcount;
GET STACKED DIAGNOSTICS CONDITION 1 @errno1 = MYSQL_ERRNO, @mingshigang2 = MESSAGE_TEXT;
RAISE NOTICE 'stacked DA before mapped insert , error = % , msg = %', @errno1, @msg1;
END;

GET CURRENT DIAGNOSTICS @num=NUMBER,@errcount= ROW_COUNT;
RAISE NOTICE 'current DA before mapped insert , num = % , errcount = %', @num, @errcount;

INSERT INTO t1 (c1) VALUES(NULL);

GET CURRENT DIAGNOSTICS @num=NUMBER,@errcount= ROW_COUNT;
RAISE NOTICE 'current DA before mapped insert , num = % , errcount = %', @num, @errcount;
GET STACKED DIAGNOSTICS @num=NUMBER,@errcount= ROW_COUNT;

END;
/
call p_1145188();
\c regression
-- test access to exception data
create function zero_divide() returns int as $$
declare v int := 0;
begin
  return 10 / v;
end;
$$ language plpgsql;

create function stacked_diagnostics_test() returns void as $$
declare _sqlstate text;
        _message text;
        _context text;
begin
  perform zero_divide();
exception when others then
  get stacked diagnostics
        _sqlstate = returned_sqlstate,
        _message = message_text,
        _context = pg_exception_context;
  raise notice 'sqlstate: %, message: %, context: [%]',
    _sqlstate, _message, replace(_context, E'\n', ' <- ');
end;
$$ language plpgsql;

select stacked_diagnostics_test();
drop function stacked_diagnostics_test;
drop function zero_divide;

create or replace function rttest()
returns setof int as $$
declare rc int;
begin
  return query values(10),(20);
  get diagnostics rc = row_count;
  raise notice '% %', found, rc;
  return query select * from (values(10),(20)) f(a) where false;
  get diagnostics rc = row_count;
  raise notice '% %', found, rc;
  return query execute 'values(10),(20)';
  get diagnostics rc = row_count;
  raise notice '% %', found, rc;
  return query execute 'select * from (values(10),(20)) f(a) where false';
  get diagnostics rc = row_count;
  raise notice '% %', found, rc;
end;
$$ language plpgsql;

select * from rttest();

drop function rttest();
drop database mysql_test;
drop database td_test;
