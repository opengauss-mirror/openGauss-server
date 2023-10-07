drop database if exists mysql_test_signal;
create database mysql_test_signal dbcompatibility = 'B';

\c mysql_test_signal
-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE set CONDITION FOR SQLSTATE '12012';
    signal set;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE sqlwarning CONDITION FOR SQLSTATE '12012';
    signal sqlwarning;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE sqlexception CONDITION FOR SQLSTATE '12012';
    signal sqlexception;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE VALUE 12345;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '12012' value MYSQL_ERRNO = 1;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE;
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal 12345;
END;
/

-- 1. signal without handler
-- 1.1 signal without signal_information_item and SQLSTATE
-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal;
END;
/

-- 1.2 signal only with SQLSTATE
-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '00000';
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '00100';
END;
/

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '01000';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '02000';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '21012';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE VALUE '21012';
END;
/
call p1();
show warnings;

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '2023';
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '202321';
END;
/

-- parse error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE throw_error CONDITION FOR 1644;
    signal throw_error;
END;
/

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE throw_error CONDITION FOR SQLSTATE '22012';
    signal throw_error;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE throw_warning CONDITION FOR SQLSTATE '01000';
    signal throw_warning;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE throw_not_found CONDITION FOR SQLSTATE '02000';
    signal throw_not_found;
END;
/
call p1();
show warnings;

-- parse_error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal division_by_zero;
END;
/

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '01000';
    signal SQLSTATE '02000';
    signal SQLSTATE '03000';
END;
/
call p1();
show warnings;

-- 2.signa with SQLSTATE and signal_information_item
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
	signal SQLSTATE '22012' SET MESSAGE_TEXT = 'exception error', MYSQL_ERRNO = 1120, CLASS_ORIGIN = 'ISO 9075', SUBCLASS_ORIGIN = 'ISO 9075', CONSTRAINT_NAME = 'constraint_name',
		CONSTRAINT_CATALOG = 'constraint_catalog', CONSTRAINT_SCHEMA = 'constraint_schema', CATALOG_NAME = 'catalog_name', SCHEMA_NAME = 'schema_name', TABLE_NAME = 'table_name',
		COLUMN_NAME = 'column name', CURSOR_NAME = 'cursor name';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
	DECLARE err_exception CONDITION FOR SQLSTATE '22012';
	SIGNAL err_exception SET MESSAGE_TEXT = 'exception error', MYSQL_ERRNO = 1120, CLASS_ORIGIN = 'ISO 9075', SUBCLASS_ORIGIN = 'ISO 9075', CONSTRAINT_NAME = 'constraint_name',
		CONSTRAINT_CATALOG = 'constraint_catalog', CONSTRAINT_SCHEMA = 'constraint_schema', CATALOG_NAME = 'catalog_name', SCHEMA_NAME = 'schema_name', TABLE_NAME = 'table_name',
		COLUMN_NAME = 'column name', CURSOR_NAME = 'cursor name';
END;
/
call p1();
show warnings;

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is error1', MESSAGE_TEXT = 'this is error2';
END;
/

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MESSAGE_TEXT = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CLASS_ORIGIN = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET SUBCLASS_ORIGIN = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CONSTRAINT_NAME = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CONSTRAINT_CATALOG = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CONSTRAINT_SCHEMA = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CATALOG_NAME = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET SCHEMA_NAME = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET TABLE_NAME = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET COLUMN_NAME = NULL;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET CURSOR_NAME = NULL;
END;
/
call p1();

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 1120;
END;
/
call p1();
show warnings;

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 0;
END;
/
call p1();

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 'zero';
END;
/
call p1();

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 1;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = '1120';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 65535;
END;
/
call p1();
show warnings;

-- error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MYSQL_ERRNO = 65536;
END;
/
call p1();

set b_format_behavior_compat_options = 'enable_set_variables';
set @message_text = 'exception error', @mysql_errno = 1120, @class_origin = 'ISO 9075', @subclass_origin = 'ISO 9075',
@constraint_name = 'constraint_name', @constraint_catalog = 'constraint_catalog', @constraint_schema = 'constraint_schema',
@catalog_name = 'catalog_name', @schema_name = 'schema_name', @table_name = 'table_name',
@column_name = 'column name', @cursor_name = 'cursor name';
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MESSAGE_TEXT = @message_text, MYSQL_ERRNO = @mysql_errno, CLASS_ORIGIN = @class_origin,
        SUBCLASS_ORIGIN = @subclass_origin, CONSTRAINT_NAME = @constraint_name, CONSTRAINT_CATALOG = @constraint_catalog,
        CONSTRAINT_SCHEMA = @constraint_schema, CATALOG_NAME = @catalog_name, SCHEMA_NAME = @schema_name, TABLE_NAME = @table_name,
        COLUMN_NAME = @column_name, CURSOR_NAME = @cursor_name;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    signal SQLSTATE '22012' SET MESSAGE_TEXT = concat('exception ', 'error'), MYSQL_ERRNO = abs(-1120);
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1(message_text text, mysql_errno int) IS
BEGIN
    signal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
END;
/
call p1('exception error', 1120);
show warnings;
call p1(concat('exception ', 'error'), abs(-1120));
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
DECLARE
    message_text text;
    mysql_errno int;
BEGIN
    select concat('exception ', 'error') into message_text;
    select abs(-1120) into mysql_errno;
    signal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
DECLARE
    message_text text;
    mysql_errno int;
BEGIN
    message_text := 'exception error';
    mysql_errno := 1120;
    signal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
END;
/
call p1();
show warnings;

-- 3. signal with SQLSTATE in handler
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
	DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' signal SQLSTATE '22012';
	DROP TABLE t1;
	select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
	DECLARE EXIT HANDLER FOR SQLSTATE '42P01' signal SQLSTATE '22012';
	DROP TABLE t1;
	select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the table is not exist';
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120,
        CLASS_ORIGIN = 'ISO 9075', SUBCLASS_ORIGIN = 'ISO 9075', CONSTRAINT_NAME = 'constraint_name',
        CONSTRAINT_CATALOG = 'constraint_catalog', CONSTRAINT_SCHEMA = 'constraint_schema',
        CATALOG_NAME = 'catalog_name', SCHEMA_NAME = 'schema_name', TABLE_NAME = 'table_name',
        COLUMN_NAME = 'column name', CURSOR_NAME = 'cursor name';
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
	DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
	BEGIN
		SELECT 1 into a;
		signal SQLSTATE '22012';
	END;
	DROP TABLE t1;
	select 2 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 42703
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the column is not exist', MYSQL_ERRNO = 1120;
    select f1 from t1 into a;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception CONDITION FOR SQLSTATE '42P01';
    DECLARE CONTINUE HANDLER FOR err_exception
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception CONDITION FOR 42703;
    DECLARE CONTINUE HANDLER FOR err_exception
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'the column is not exist', MYSQL_ERRNO = 1120;
    select f1 from t1 into a;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception1 CONDITION FOR SQLSTATE '42P01';
    DECLARE err_exception2 CONDITION FOR SQLSTATE '22012';
    DECLARE CONTINUE HANDLER FOR err_exception1
        signal err_exception2 SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

-- error
DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception1 CONDITION FOR 42703;
    DECLARE err_exception2 CONDITION FOR 22012;
    DECLARE CONTINUE HANDLER FOR err_exception1
        signal err_exception2 SET MESSAGE_TEXT = 'the column is not exist', MYSQL_ERRNO = 1120;
    select f1 from t1 into a;
    select 1 into a;
END;
/

DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception1 CONDITION FOR 42703;
    DECLARE err_exception2 CONDITION FOR SQLSTATE '22012';
    DECLARE CONTINUE HANDLER FOR err_exception1
        signal err_exception2 SET MESSAGE_TEXT = 'the column is not exist', MYSQL_ERRNO = 1120;
    select f1 from t1 into a;
    select 1 into a;
END;
/
call p1(0);
show warnings;

-- no handler when sqlstate = 01000 without signal/resignal statement
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
	DECLARE EXIT HANDLER FOR SQLSTATE '01000'
		signal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
	SELECT create_wdr_snapshot() INTO a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
	DECLARE EXIT HANDLER FOR 64
		signal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
	SELECT create_wdr_snapshot() INTO a;
END;
/
call p1(0);
show warnings;

-- throw handler when sqlstate = 01000 with signal/resignal statement
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '01000'
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLSTATE '01000'
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '01000'
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 01000
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 2;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 01000
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 3;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 3;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 3;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 3;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 65535;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 65535;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE err_warnings1 CONDITION FOR 1;
    DECLARE err_warnings2 CONDITION FOR SQLSTATE '01000';
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION, SQLSTATE '01000', SQLWARNING, err_warnings1, err_warnings2
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS hdfs_tbl_001;
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLSTATE '02002'
        signal SQLSTATE '11220' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
    CREATE TABLE hdfs_tbl_001(a int, b int) WITH(orientation = row, version = 0.12);
    SELECT 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS hdfs_tbl_001;
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02002'
        signal SQLSTATE '11220' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
    CREATE TABLE hdfs_tbl_001(a int, b int) WITH(orientation = row, version = 0.12);
    SELECT 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS hdfs_tbl_001;
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLSTATE '02002'
        signal SQLSTATE '01111' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
    CREATE TABLE hdfs_tbl_001(a int, b int) WITH(orientation = row, version = 0.12);
    SELECT 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS hdfs_tbl_001;
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '02002'
        signal SQLSTATE '01111' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
    CREATE TABLE hdfs_tbl_001(a int, b int) WITH(orientation = row, version = 0.12);
    SELECT 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS hdfs_tbl_001;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE err_not_found CONDITION FOR 02002;
    DECLARE EXIT HANDLER FOR err_not_found
        signal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
    CREATE TABLE hdfs_tbl_001(a int, b int) WITH(orientation = row, version = 0.12);
END;
/
call p1();
show warnings;

-- nested scenario
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLWARNING 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLSTATE '01000' 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        signal SQLSTATE '12000' SET MESSAGE_TEXT = 'column is not defined2222';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE EXIT HANDLER FOR SQLWARNING 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
            select 1 into a;
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
    END;
    DROP TABLE t1;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLWARNING 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
            select 1 into a;
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
    END;
    DROP TABLE t1;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLWARNING 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
        select 1 into a;
    END;
    DROP TABLE t1;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE EXIT HANDLER FOR SQLWARNING 
        BEGIN
            signal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        signal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
        select 1 into a;
    END;
    DROP TABLE t1;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
DROP VIEW IF EXISTS v1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
        BEGIN
            signal SQLSTATE '42S02' SET MESSAGE_TEXT = 'column is not defined', MYSQL_ERRNO = 1051;
        END;
        DROP VIEW v1;
        signal SQLSTATE '42S02' SET MESSAGE_TEXT = 'column is not defined2222', MYSQL_ERRNO = 1051;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 1 signal SQLSTATE '22012' SET MYSQL_ERRNO = 1000, MESSAGE_TEXT='mysql_errno is 1';
    signal sqlstate '22012' SET MYSQL_ERRNO = 1;
END;
/
call p1();
show warnings;

-- mysql example
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SIGNAL SQLSTATE VALUE '99999' SET MESSAGE_TEXT = 'an error occurred';
    END;
    DROP TABLE t1;
END;
/
call p2();
show warnings;

CREATE OR REPLACE PROCEDURE p2(pval int) IS
BEGIN
    DECLARE specialty condition for sqlstate '45000';
    IF pval = 0 THEN
        SIGNAL SQLSTATE '01000';
    ELSEIF pval = 1 then
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'an error occurred for 1';
    ELSEIF pval = 2 THEN
        SIGNAL specialty SET MESSAGE_TEXT = 'an error occurred for 2';
    ELSE
        SIGNAL SQLSTATE '01000' SET MESSAGE_TEXT = 'an error occurred', MYSQL_ERRNO = 1000;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'an error occurred', MYSQL_ERRNO = 1001;
    END IF;
END;
/
call p2(0);
show warnings;
call p2(1);
show warnings;
call p2(2);
show warnings;
call p2(3);
show warnings;

DROP TABLE IF EXISTS user_info;
CREATE TABLE user_info(f_id int primary key);
CREATE OR REPLACE PROCEDURE p2(f_id in int) IS
BEGIN
    DECLARE con1 condition for sqlstate '23505';
    DECLARE CONTINUE HANDLER FOR con1
    BEGIN
        SIGNAL con1 SET CLASS_ORIGIN = 'action', TABLE_NAME = 'action_tb', MESSAGE_TEXT = 'duplicated!', MYSQL_ERRNO = 22;
    END;
    insert into user_info values(f_id);
END;
/
call p2(0);
call p2(0);
show warnings;

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    signal sqlstate 01001;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    signal sqlstate value 01001;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    signal sqlstate values 01001;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    signal sqlstate value;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    signal sqlstate values;
END;
/

\c regression
drop database mysql_test_signal;