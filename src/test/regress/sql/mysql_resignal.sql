drop database if exists mysql_test_resignal;
create database mysql_test_resignal dbcompatibility = 'B';

\c mysql_test_resignal

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        signal SQLSTATE VALUE 12345;
    END;
    DROP TABLE t1;
END;
/

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '12012' value MYSQL_ERRNO = 1;
    END;
    DROP TABLE t1;
END;
/

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '12012' value MYSQL_ERRNO = 1;
    END;
    DROP TABLE t1;
END;
/

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE;
    END;
    DROP TABLE t1;
END;
/

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal 12345;
    END;
    DROP TABLE t1;
END;
/

-- parse error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '1234';
    END;
    DROP TABLE t1;
END;
/

-- 1. resignal without handler
-- 1.1 signal without signal_information_item and SQLSTATE
-- exec error
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    resignal;
END;
/
call p1();

CREATE OR REPLACE PROCEDURE p1(a int) IS
BEGIN
    if a = 0 then
        resignal;
    else
        RAISE info 'value is %', a;
    end if;
END;
/
-- error
call p1(0);
-- success
call p1(1);

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET MYSQL_ERRNO = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CLASS_ORIGIN = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET SUBCLASS_ORIGIN = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CONSTRAINT_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CONSTRAINT_CATALOG = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CONSTRAINT_SCHEMA = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CATALOG_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET SCHEMA_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET TABLE_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET COLUMN_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- error
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET CURSOR_NAME = NULL;
    END;
    DROP TABLE t1;
END;
/
call p1();

-- 2. resignal with handler
-- 2.1 resignal alone and continue handler
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal;
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
        resignal;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
        resignal;
    END;
    signal SQLSTATE '01111';
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR NOT FOUND
    BEGIN
        resignal;
    END;
    signal SQLSTATE '02222';
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE err_exception CONDITION FOR SQLSTATE '42P01';
    DECLARE CONTINUE HANDLER FOR err_exception
    BEGIN
        resignal;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception CONDITION FOR 42703;
    DECLARE CONTINUE HANDLER FOR err_exception
    BEGIN
        resignal;
    END;
    select f1 from t1 into a;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '01000' signal SQLSTATE '02000' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    signal SQLSTATE '01000';
END;
/
call p1();
show warnings;





DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR NOT FOUND
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    signal SQLSTATE '02222';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE EXIT HANDLER FOR NOT FOUND
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    signal SQLSTATE '02222';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    signal SQLSTATE '01111';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLWARNING
    BEGIN
        resignal SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    signal SQLSTATE '01111';
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '42P01' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = 'table is not defined';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '22012';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '42P01';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
create table t1(a int);
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 42703
    BEGIN
        resignal SQLSTATE '42P01' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    SELECT f1 FROM t1 INTO a;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE err_exception CONDITION FOR SQLSTATE '22012';
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal err_exception SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
create table t1(a int);
-- parse error
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception CONDITION FOR 42703;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal err_exception SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111;
    END;
    SELECT f1 FROM t1 INTO a;
END;
/

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1120;
    END;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLSTATE '42P01'
    BEGIN
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1120;
    END;
    DROP TABLE t1;
    select 1 into a;
END;
/
call p1(0);
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42P01' 
    BEGIN
        resignal SQLSTATE '42P01' SET MESSAGE_TEXT = 'table is not defined', MYSQL_ERRNO = 1111, CLASS_ORIGIN = 'ISO 9075', SUBCLASS_ORIGIN = 'ISO 9075', CONSTRAINT_NAME = 'constraint_name',
        CONSTRAINT_CATALOG = 'constraint_catalog', CONSTRAINT_SCHEMA = 'constraint_schema', CATALOG_NAME = 'catalog_name', SCHEMA_NAME = 'schema_name', TABLE_NAME = 'table_name',
        COLUMN_NAME = 'column name', CURSOR_NAME = 'cursor name';
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

set b_format_behavior_compat_options = 'enable_set_variables';
set @message_text = 'exception error', @mysql_errno = 1120, @class_origin = 'ISO 9075', @subclass_origin = 'ISO 9075',
@constraint_name = 'constraint_name', @constraint_catalog = 'constraint_catalog', @constraint_schema = 'constraint_schema',
@catalog_name = 'catalog_name', @schema_name = 'schema_name', @table_name = 'table_name',
@column_name = 'column name', @cursor_name = 'cursor name';
DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = @message_text, MYSQL_ERRNO = @mysql_errno, CLASS_ORIGIN = @class_origin,
                SUBCLASS_ORIGIN = @subclass_origin, CONSTRAINT_NAME = @constraint_name, CONSTRAINT_CATALOG = @constraint_catalog,
                CONSTRAINT_SCHEMA = @constraint_schema, CATALOG_NAME = @catalog_name, SCHEMA_NAME = @schema_name, TABLE_NAME = @table_name,
                COLUMN_NAME = @column_name, CURSOR_NAME = @cursor_name;
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
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = concat('exception ', 'error'), MYSQL_ERRNO = abs(-1120);
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(message_text text, mysql_errno int) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
    END;
    DROP TABLE t1;
END;
/
call p1('exception error', 1120);
show warnings;
call p1(concat('exception ', 'error'), abs(-1120));
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
DECLARE
    message_text text;
    mysql_errno int;
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
    select concat('exception ', 'error') into message_text;
    select abs(-1120) into mysql_errno;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1() IS
DECLARE
    message_text text;
    mysql_errno int;
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = message_text, MYSQL_ERRNO = mysql_errno;
    message_text := 'exception error';
    mysql_errno := 1120;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p1(a out int) IS
BEGIN
    DECLARE err_exception1 CONDITION FOR SQLSTATE '42P01';
    DECLARE err_exception2 CONDITION FOR SQLSTATE '22012';
    DECLARE CONTINUE HANDLER FOR err_exception1
        resignal err_exception2 SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
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
        resignal err_exception2 SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
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
        resignal err_exception2 SET MESSAGE_TEXT = 'the table is not exist', MYSQL_ERRNO = 1120;
    select f1 from t1 into a;
    select 1 into a;
END;
/
call p1(0);
show warnings;

-- throw handler when sqlstate = 01000 with signal/resignal statement
CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '01000'
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLSTATE '01000'
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLSTATE '01000'
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 01000
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 2;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 2;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 01000
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 3;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 3;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 3;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 3;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
    signal SQLSTATE '01000';
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 65535;
    signal SQLSTATE '01000' SET MYSQL_ERRNO = 65535;
    select 1 into a;
END;
/
call p1(0);
show warnings;

CREATE OR REPLACE PROCEDURE p1(a out text) IS
BEGIN
    DECLARE err_warnings1 CONDITION FOR 01000;
    DECLARE err_warnings2 CONDITION FOR SQLSTATE '01000';
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION, SQLSTATE '01000', SQLWARNING, err_warnings1, err_warnings2
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'this is warnings', MYSQL_ERRNO = 1;
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
        resignal SQLSTATE '11220' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
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
        resignal SQLSTATE '11220' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
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
        resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
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
        resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
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
        resignal SQLSTATE '22012' SET MESSAGE_TEXT = 'this is not found error', MYSQL_ERRNO = 1120;
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        resignal SQLSTATE '12000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
            select 1 into a;
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
            select 1 into a;
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '01111' SET MESSAGE_TEXT = 'column is not defined';
        END;
        resignal SQLSTATE '01000' SET MESSAGE_TEXT = 'column is not defined2222';
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
            resignal SQLSTATE '42S02' SET MESSAGE_TEXT = 'column is not defined', MYSQL_ERRNO = 1051;
        END;
        DROP VIEW v1;
        resignal SQLSTATE '42S02' SET MESSAGE_TEXT = 'column is not defined2222', MYSQL_ERRNO = 1051;
    END;
    DROP TABLE t1;
END;
/
call p1();
show warnings;

CREATE OR REPLACE PROCEDURE p1() IS
BEGIN
    DECLARE CONTINUE HANDLER FOR 1 resignal SQLSTATE '22012' SET MYSQL_ERRNO = 1000, MESSAGE_TEXT='mysql_errno is 1';
    signal sqlstate '22012' SET MYSQL_ERRNO = 1;
END;
/
call p1();
show warnings;

-- mysql example
set b_format_behavior_compat_options = 'enable_set_variables';
CREATE OR REPLACE PROCEDURE p2(numerator in int, denominator in int, result out number) IS
BEGIN
    DECLARE divisio_by_zero condition for SQLSTATE '22012';
    DECLARE CONTINUE handler for divisio_by_zero
        resignal set message_text = 'division by zero / denominator cannot be zero';
    IF denominator = 0 THEN
        signal divisio_by_zero;
    ELSE 
        result := numerator / denominator;
    END IF;
END;
/
call p3(1, 0, 1);
show warnings;
call p3(2, 1, 0);

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET subclass_origin = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal sqlstate '45000' SET mysql_errno = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;


DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET mysql_errno = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET constraint_catalog = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET constraint_schema = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET constraint_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET catalog_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET schema_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET table_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET column_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

DROP TABLE IF EXISTS t1;
CREATE OR REPLACE PROCEDURE p2() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET @error_count = @error_count + 1;
        IF @a = 0 THEN 
            resignal SET cursor_name = 5;
        END IF;
    END;
    DROP TABLE t1;
END;
/
SET @error_count = 1;
SET @a = 0;
call p2();
show warnings;
select @a, @error_count;

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal sqlstate 01001;
    END;
    DROP TABLE t1;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal sqlstate value 01001;
    END;
    DROP TABLE t1;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal sqlstate values 01001;
    END;
    DROP TABLE t1;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal sqlstate value;
    END;
    DROP TABLE t1;
END;
/

CREATE OR REPLACE PROCEDURE p() IS
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        resignal sqlstate values;
    END;
    DROP TABLE t1;
END;
/


\c regression
drop database mysql_test_resignal;