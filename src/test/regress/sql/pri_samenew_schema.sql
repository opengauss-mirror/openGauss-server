DROP USER test_same_schema_user;
DROP USER ordinary_role;
CREATE USER test_same_schema_user PASSWORD 'Gauss@1234';
CREATE USER ordinary_role PASSWORD 'Gauss@1234';

--test same schema
reset role;
SET ROLE ordinary_role PASSWORD 'Gauss@1234';
--create table
CREATE TABLE test_drop_table(id int);
CREATE TABLE TBL_DOMAIN_PRI
(
  IDOMAINID   NUMBER(10) NOT NULL,
  SDOMAINNAME VARCHAR2(30) NOT NULL,
  b int
);
insert into TBL_DOMAIN_PRI values (1,'gauss',1);

reset role;
GRANT create any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE table ordinary_role.tb_pri (id int, name VARCHAR(10));
alter table ordinary_role.TBL_DOMAIN_PRI add column c int;
drop table ordinary_role.test_drop_table;
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;
reset role;
-- create any type
revoke create any table from test_same_schema_user;
GRANT create any type to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE TYPE ordinary_role.compfoo AS (f1 int, f2 text);
CREATE TABLE ordinary_role.t1_compfoo(a int, b ordinary_role.compfoo);
CREATE TYPE ordinary_role.bugstatus AS ENUM ('create', 'modify', 'closed');
-- create any function
reset role;
revoke create any type from test_same_schema_user;
GRANT create any function to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE FUNCTION ordinary_role.pri_func_add_sql(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
--create any index
reset role;
revoke create any function from test_same_schema_user;
GRANT create any index to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE UNIQUE INDEX ordinary_role.ds_ship_mode_t1_index1 ON ordinary_role.TBL_DOMAIN_PRI(IDOMAINID);
reset role;
DROP INDEX ordinary_role.ds_ship_mode_t1_index1;
--create any sequence
reset role;
revoke create any index from test_same_schema_user;
GRANT create any sequence to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE SEQUENCE sequence_test;

--alter any table
reset role;
revoke  create any type from test_same_schema_user;
grant alter any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table ordinary_role.TBL_DOMAIN_PRI add column c int;
drop table ordinary_role.test_drop_table;
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2,2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;

--drop any table
reset role;
revoke alter any table from test_same_schema_user;
grant drop any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table ordinary_role.TBL_DOMAIN_PRI add column c int;
drop table ordinary_role.test_drop_table;
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2,2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;

--select any table
reset role;
revoke drop any table from test_same_schema_user;
grant select any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table ordinary_role.TBL_DOMAIN_PRI add column c int;
drop table ordinary_role.test_drop_table;
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2,2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;

--insert any table
reset role;
revoke select any table from test_same_schema_user;
grant insert any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2,2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;

--update any table
reset role;
revoke insert any table from test_same_schema_user;
grant update any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
select * from ordinary_role.TBL_DOMAIN_PRI;
insert into ordinary_role.TBL_DOMAIN_PRI values (2,'gauss',2,2);
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;
reset role;
grant select any table to test_same_schema_user;
update ordinary_role.TBL_DOMAIN_PRI set b = 3 where IDOMAINID = 1;

--delete any table
reset role;
revoke update any table from test_same_schema_user;
revoke select any table from test_same_schema_user;
grant delete any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
delete from ordinary_role.TBL_DOMAIN_PRI;

-- test new schema
reset role;
revoke delete any table from test_same_schema_user;
GRANT create any table to test_same_schema_user;
CREATE SCHEMA pri_new_schema;
CREATE TABLE pri_new_schema.TBL_DOMAIN
(
  IDOMAINID   NUMBER(10) NOT NULL,
  SDOMAINNAME VARCHAR2(30) NOT NULL,
  b int
);
insert into pri_new_schema.TBL_DOMAIN values (1,'gauss',1);
CREATE TABLE pri_new_schema.test_new_table1(id int, name text);

GRANT create any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
CREATE TABLE pri_new_schema.test_new_table2(id int, name text);

reset role;
revoke  create any table from test_same_schema_user;
grant alter any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table pri_new_schema.TBL_DOMAIN add column d int;
drop table pri_new_schema.test_new_table1;
select * from pri_new_schema.TBL_DOMAIN;
insert into pri_new_schema.TBL_DOMAIN values (2,'gauss',2,2);
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;

reset role;
revoke  alter any table from test_same_schema_user;
grant drop any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table pri_new_schema.TBL_DOMAIN add column d int;
drop table pri_new_schema.test_new_table1;
select * from pri_new_schema.TBL_DOMAIN;
insert into pri_new_schema.TBL_DOMAIN values (2,'gauss',2,2);
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;

reset role;
revoke  drop any table from test_same_schema_user;
grant select any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
alter table pri_new_schema.TBL_DOMAIN add column d int;
drop table pri_new_schema.test_new_table1;
select * from pri_new_schema.TBL_DOMAIN;
insert into pri_new_schema.TBL_DOMAIN values (2,'gauss',2,2);
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;

reset role;
revoke  select any table from test_same_schema_user;
grant insert any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
select * from pri_new_schema.TBL_DOMAIN;
insert into pri_new_schema.TBL_DOMAIN values (2,'gauss',2,2);
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;

reset role;
revoke  insert any table from test_same_schema_user;
grant update any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
select * from pri_new_schema.TBL_DOMAIN;
insert into pri_new_schema.TBL_DOMAIN values (2,'gauss',2,2);
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;
reset role;
grant select any table to test_same_schema_user;
update pri_new_schema.TBL_DOMAIN set b = 3 where IDOMAINID = 1;

reset role;
revoke  update any table from test_same_schema_user;
revoke  select any table from test_same_schema_user;
GRANT delete any table to test_same_schema_user;
SET ROLE test_same_schema_user PASSWORD 'Gauss@1234';
delete pri_new_schema.TBL_DOMAIN;

reset role;
revoke  delete any table from test_same_schema_user;
drop table  pri_new_schema.TBL_DOMAIN cascade;
drop table  pri_new_schema.test_new_table2 cascade;
drop table  ordinary_role.tb_pri cascade;
drop table  ordinary_role.tbl_domain_pri cascade;
drop type ordinary_role.compfoo;
drop type ordinary_role.bugstatus;
drop function ordinary_role.pri_func_add_sql(integer,integer);
drop schema ordinary_role cascade;
drop user ordinary_role cascade;
drop user test_same_schema_user cascade;
drop schema pri_new_schema cascade;
