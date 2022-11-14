create database db_testa;
create database if not exists db_testa;
create database db_testa;
create database if not exists db_testb;
drop database if exists db_testa;
drop database if exists db_testb;


create user test_user with password 'Abcd.123';
ALTER USER IF EXISTS test_user IDENTIFIED BY 'Abcd.1234';
ALTER USER test_user IDENTIFIED BY 'Abcd.12345';
ALTER USER IF EXISTS test_user2 IDENTIFIED BY 'Abcd.1234';
ALTER USER test_user2 IDENTIFIED BY 'Abcd.1234';
DROP USER test_user;

CREATE SCHEMA sch_name;
CREATE SCHEMA IF NOT EXISTS sch_name;
CREATE SCHEMA IF NOT EXISTS sch_name2;
CREATE SCHEMA sch_name2;
drop SCHEMA sch_name;
drop SCHEMA sch_name2;

create user test_user_002 password 'test@1234';
create schema if not exists test_schema_002 authorization test_user_002;
drop schema test_schema_002;
drop user test_user_002;

CREATE USER ZZZ WITH PASSWORD 'openGauss@123';
CREATE USER ZZZ WITH PASSWORD 'openGauss@123';
CREATE USER IF NOT EXISTS ZZZ WITH PASSWORD 'openGauss@123';
DROP USER ZZZ;
CREATE USER IF NOT EXISTS ZZZ WITH PASSWORD 'openGauss@123';
