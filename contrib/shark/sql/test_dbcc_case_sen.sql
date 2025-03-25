create database "DbcC_test_Case_sen" with dbcompatibility 'D';

\c "DbcC_test_Case_sen";

create extension shark;

create schema "DbcC_test_Case_sen_S";
set search_path = "DbcC_test_Case_sen_S";

set d_format_behavior_compat_options = 'enable_sbr_identifier';

-- part1: dbcc check NORESEED
 CREATE TABLE "Employees" (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into "Employees"(Name) values ('zhangsan');
insert into "Employees"(Name) values ('lisi');
insert into "Employees"(Name) values ('wangwu');
insert into "Employees"(Name) values ('heliu');


 CREATE TABLE "employees" (
     EmployeeID serial ,
     Name VARCHAR(100) NOT NULL
 );

insert into "employees"(Name) values ('zhangsan');
insert into "employees"(Name) values ('lisi');


-- Case-insensitive
DBCC CHECKIDENT ('employees', NORESEED);
DBCC CHECKIDENT ('Employees', RESEED, 1);

-- Case-sensitive
DBCC CHECKIDENT ("employees", NORESEED);
DBCC CHECKIDENT ("Employees", NORESEED);
DBCC CHECKIDENT ("Employees", RESEED, 1);

drop schema "DbcC_test_Case_sen_S" cascade;

\c contrib_regression;

drop database "DbcC_test_Case_sen";
