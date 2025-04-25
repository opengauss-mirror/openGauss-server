create schema shark_test_dbcc;
set search_path = 'shark_test_dbcc';

set d_format_behavior_compat_options = 'enable_sbr_identifier';

-- part1: dbcc check NORESEED
 CREATE TABLE Employees (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
insert into Employees(Name) values ('wangwu');
insert into Employees(Name) values ('heliu');

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees'); 

DBCC CHECKIDENT ('Employees', NORESEED) WITH NO_INFOMSGS;

insert into Employees(Name) values ('heqi');

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees'); 

DBCC CHECKIDENT ('Employees', NORESEED) WITH NO_INFOMSGS;


delete from Employees where EmployeeID > 2;

DBCC CHECKIDENT ('Employees', NORESEED); 

update Employees set employeeid = 100 where employeeid = 1;

DBCC CHECKIDENT ('Employees', NORESEED); 
 

-- increment by is negative
 CREATE TABLE Employees_ne (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

ALTER SEQUENCE employees_ne_employeeid_seq MINVALUE  -1000;
ALTER SEQUENCE employees_ne_employeeid_seq INCREMENT BY -2;

insert into Employees_ne(Name) values ('zhangsan');
insert into Employees_ne(Name) values ('lisi');
insert into Employees_ne(Name) values ('wangwu');
insert into Employees_ne(Name) values ('heliu');

DBCC CHECKIDENT ('Employees_ne', NORESEED);

delete from Employees where EmployeeID < -4;

DBCC CHECKIDENT ('Employees_ne', NORESEED);

-- error case expect
DBCC CHECKIDENT ('Employees1', NORESEED); 
DBCC CHECKIDENT ('employees_employeeid_seq', NORESEED); 


drop table if exists Employees2;
create table Employees2(id int, name VARCHAR(100));
insert into Employees2 values (1, 'zhangsan');
insert into Employees2 values (2, 'lisi');
DBCC CHECKIDENT ('Employees2', NORESEED);
drop table if exists Employees2;

-- plsql
create or replace procedure test_procedure_test1(int)
as
begin
      DBCC CHECKIDENT ('Employees', NORESEED);
end;
/

call test_procedure_test1(1);

drop table Employees;
drop table Employees_ne;
drop procedure test_procedure_test1(int);


CREATE TABLE Employees (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
insert into Employees(Name) values ('wangwu');
insert into Employees(Name) values ('heliu');

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, 1);

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees');

DBCC CHECKIDENT ('Employees', NORESEED);

drop table Employees;


CREATE TABLE Employees (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
insert into Employees(Name) values ('wangwu');
insert into Employees(Name) values ('heliu');

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, 1);

DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED);

DBCC CHECKIDENT ('Employees', NORESEED);

drop table Employees;

-- part1: dbcc check RESEED
CREATE TABLE Employees (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
insert into Employees(Name) values ('wangwu');
insert into Employees(Name) values ('heliu');


DBCC CHECKIDENT ('Employees', NORESEED);
DBCC CHECKIDENT ('Employees', RESEED, 20);
insert into Employees(Name) values ('heqi');
insert into Employees(Name) values ('heba');
DBCC CHECKIDENT ('Employees', NORESEED);
select * from Employees order by 1, 2;

DBCC CHECKIDENT ('Employees', RESEED, 30) WITH NO_INFOMSGS;
insert into Employees(Name) values ('zhangqi');
insert into Employees(Name) values ('zhangba');
DBCC CHECKIDENT ('Employees', NORESEED);
select * from Employees order by 1, 2;

delete from Employees where EmployeeID > 2;
DBCC CHECKIDENT ('Employees', RESEED, 1);
insert into Employees(Name) values ('heqi');
insert into Employees(Name) values ('heba');
select * from Employees order by 1, 2;

ALTER SEQUENCE employees_employeeid_seq MINVALUE -100;

DBCC CHECKIDENT ('Employees', RESEED);

DBCC CHECKIDENT ('Employees', RESEED, -2);

insert into Employees(Name) values ('liqi');
insert into Employees(Name) values ('liba');

select * from Employees order by 1, 2;

-- plsql
create or replace procedure test_procedure_test(int)
as
begin
      DBCC CHECKIDENT ('Employees', RESEED, 1);
end;
/

call test_procedure_test(1);


drop table Employees;
drop procedure test_procedure_test(int);


-- newreseed value range
drop table if exists Employees;
CREATE TABLE Employees (EmployeeID serial ,Name VARCHAR(100) NOT NULL);
insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
ALTER SEQUENCE employees_employeeid_seq MINVALUE -9223372036854775808;

DBCC CHECKIDENT ('Employees', RESEED, 9223372036854775806);
DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, 9223372036854775807);
DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, 9223372036854775808);
DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, -9223372036854775807);
DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, -9223372036854775808);
DBCC CHECKIDENT ('Employees', NORESEED);

DBCC CHECKIDENT ('Employees', RESEED, -9223372036854775809);
DBCC CHECKIDENT ('Employees', NORESEED);

drop table if exists Employees;

-- some error case
-- no serial col

CREATE TABLE Employees_no (
     EmployeeID int ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees_no(EmployeeID, Name) values (1, 'zhangsan');
insert into Employees_no(EmployeeID, Name) values (2, 'lisi');
insert into Employees_no(EmployeeID, Name) values (3, 'wangwu');
insert into Employees_no(EmployeeID, Name) values (4, 'heliu');

DBCC CHECKIDENT ('Employees_no', NORESEED);
DBCC CHECKIDENT ('Employees_no', RESEED, 20);

-- mul serial col

CREATE TABLE Employees_mu (
     EmployeeID serial ,
	 EmployeeID1 serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees_mu(Name) values ('zhangsan');
insert into Employees_mu(Name) values ('lisi');
insert into Employees_mu(Name) values ('wangwu');
insert into Employees_mu(Name) values ('heliu');

DBCC CHECKIDENT ('Employees_mu', NORESEED);
DBCC CHECKIDENT ('Employees_mu', RESEED, 20);

-- serial at middle
CREATE TABLE Employees_mi (
     EmployeeID int ,
	 EmployeeID1 serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees_mi(EmployeeID, Name) values (1, 'zhangsan');
insert into Employees_mi(EmployeeID, Name) values (2, 'lisi');
insert into Employees_mi(EmployeeID, Name) values (3, 'wangwu');
insert into Employees_mi(EmployeeID, Name) values (4, 'heliu');

DBCC CHECKIDENT ('Employees_mi', NORESEED);
DBCC CHECKIDENT ('Employees_mi', RESEED, 20);


drop table Employees_no;
drop table Employees_mu;
drop table Employees_mi;


-- permission
 CREATE TABLE Employees (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
insert into Employees(Name) values ('wangwu');
insert into Employees(Name) values ('heliu');



create user normalrole_user_001 password 'Gauss@123';
create user normalrole_user_002 password 'Gauss@123';
create user normalrole_user_003 password 'Gauss@123';
GRANT USAGE ON SCHEMA shark_test_dbcc TO normalrole_user_001;
GRANT USAGE ON SCHEMA shark_test_dbcc TO normalrole_user_002;
GRANT USAGE ON SCHEMA shark_test_dbcc TO normalrole_user_003;
GRANT SELECT ON Employees to normalrole_user_002;
GRANT SELECT,UPDATE  ON Employees to normalrole_user_003;

SET SESSION AUTHORIZATION normalrole_user_001 PASSWORD 'Gauss@123';
DBCC CHECKIDENT ('Employees', NORESEED);
DBCC CHECKIDENT ('Employees', RESEED, 1);
RESET SESSION AUTHORIZATION;


SET SESSION AUTHORIZATION normalrole_user_002 PASSWORD 'Gauss@123';
DBCC CHECKIDENT ('Employees', NORESEED);
DBCC CHECKIDENT ('Employees', RESEED, 1);
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION normalrole_user_003 PASSWORD 'Gauss@123';
DBCC CHECKIDENT ('Employees', NORESEED);
DBCC CHECKIDENT ('Employees', RESEED, 1);
RESET SESSION AUTHORIZATION;

drop user normalrole_user_001 cascade;
drop user normalrole_user_002 cascade;
drop user normalrole_user_003 cascade;
drop table Employees;

-- empty table
 CREATE TABLE Employees1 (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

DBCC CHECKIDENT ('Employees1', NORESEED);
DBCC CHECKIDENT ('Employees1', RESEED, 1);
DBCC CHECKIDENT ('Employees1', NORESEED);
DBCC CHECKIDENT ('Employees1', RESEED, 1);
DBCC CHECKIDENT ('Employees1', NORESEED);

drop table Employees1;


-- ANONYMOUS BLOCK
CREATE TABLE Employees (EmployeeID serial ,Name VARCHAR(100) NOT NULL);
insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');

begin
    DBCC CHECKIDENT ('Employees', NORESEED);
end;
/

begin
    DBCC CHECKIDENT ('Employees', RESEED, 5);
end;
/

CREATE FUNCTION func_20_1() RETURN integer
AS
BEGIN
    DBCC CHECKIDENT ('Employees', NORESEED);
    RETURN 0;
END;
/
select func_20_1();

CREATE FUNCTION func_20_2() RETURN integer
AS
BEGIN
    DBCC CHECKIDENT ('Employees', RESEED, 5);
    RETURN 0;
END;
/
select func_20_2();

drop TABLE Employees;
drop FUNCTION func_20_2();
drop FUNCTION func_20_1();


-- param
drop table if exists Employees;
drop procedure if exists procedure_18;
CREATE TABLE Employees (EmployeeID serial ,Name VARCHAR(100) NOT NULL);
insert into Employees(Name) values ('zhangsan');
insert into Employees(Name) values ('lisi');
create or replace procedure procedure_18(param1 int=0)
is
begin
    insert into Employees values (param1,'wangwu');
    DBCC CHECKIDENT ('Employees', NORESEED);
end;
/
call procedure_18(6);

DBCC CHECKIDENT ('Employees', NORESEED);

create or replace procedure procedure_18(param1 int=0)
is
begin
    DBCC CHECKIDENT ('Employees', RESEED, param1);
end;
/
call procedure_18(8);

DBCC CHECKIDENT ('Employees', NORESEED);


declare
    id int := 5;
begin
    DBCC CHECKIDENT ('Employees', RESEED, id);
end;
/

DBCC CHECKIDENT ('Employees', NORESEED);

CREATE FUNCTION func_20_3(num integer) RETURN integer
AS
BEGIN
    DBCC CHECKIDENT ('Employees', RESEED, num);
    RETURN 0;
END;
/

select func_20_3(20);

DBCC CHECKIDENT ('Employees', NORESEED);

drop FUNCTION func_20_3(integer);
drop table if exists Employees;
drop procedure if exists procedure_18;

-- create table as
create table t2(id int, name int);
insert into t2 values (1, 1);
insert into t2 values (2, 2);
insert into t2 values (3, 3);

CREATE or replace PROCEDURE InsertIntoTempTable
AS
BEGIN
  WITH dd AS (
  SELECT * FROM t2
  )
  SELECT * INTO new_table from dd;
END;
/

call InsertIntoTempTable();

select * from new_table;

CREATE or replace PROCEDURE InsertIntoTempTable2
AS
BEGIN
  WITH dd AS (
  SELECT * FROM t2
  )
  SELECT 1 INTO new_table2;
END;
/

call InsertIntoTempTable2();

-- expect error
CREATE or replace PROCEDURE InsertIntoTempTable3
AS
BEGIN
  WITH dd AS (
  SELECT * FROM t2
  )
  SELECT * INTO new_table;
END;
/

call InsertIntoTempTable3();

drop PROCEDURE insertintotemptable;
drop PROCEDURE InsertIntoTempTable2;
drop PROCEDURE InsertIntoTempTable3;
drop table new_table;
drop table new_table2;
drop table t2;

drop schema shark_test_dbcc cascade;
