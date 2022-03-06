--测试私有用户
CREATE USER any_table_role PASSWORD 'Gauss@1234';

CREATE USER pri_user_independent WITH INDEPENDENT IDENTIFIED BY "1234@abc";
set role pri_user_independent password "1234@abc";
CREATE table pri_user_independent.tb_pri (id int, name VARCHAR(10));
CREATE table pri_user_independent.tb_pri_test (id int, name VARCHAR(10));
CREATE table pri_user_independent.tb_pri_test1 (id int, name VARCHAR(10));
insert into pri_user_independent.tb_pri values(1, 'gauss');

--普通用户
set role any_table_role PASSWORD 'Gauss@1234';
select * from pri_user_independent.tb_pri;
insert into pri_user_independent.tb_pri values(1,'joe');
update pri_user_independent.tb_pri set name = 'gauss' where id = 1;
delete from pri_user_independent.tb_pri;
create table pri_user_independent.tt1(id int);
ALTER TABLE pri_user_independent.tb_pri add column age int;
DROP table pri_user_independent.tb_pri_test;

--初始用户
reset role;
select * from pri_user_independent.tb_pri;
insert into pri_user_independent.tb_pri values(1,'joe');
update pri_user_independent.tb_pri set name = 'gauss' where id = 1;
delete from pri_user_independent.tb_pri;
create table pri_user_independent.tt(id int);
ALTER TABLE pri_user_independent.tb_pri add column age int;
DROP table pri_user_independent.tb_pri_test;

--select any table
reset role;
GRANT select any table to any_table_role;
SET ROLE any_table_role PASSWORD 'Gauss@1234';
select * from pri_user_independent.tb_pri;

--insert any table
reset role;
GRANT insert any table to any_table_role;
SET ROLE any_table_role PASSWORD 'Gauss@1234';
insert into pri_user_independent.tb_pri values(2,'bob');

--update any table
reset role;
GRANT update any table to any_table_role;
SET ROLE any_table_role PASSWORD 'Gauss@1234';
update pri_user_independent.tb_pri set name = 'Bob' where id = 2;

--delete any table
reset role;
GRANT delete any table to any_table_role;
SET ROLE any_table_role PASSWORD 'Gauss@1234';
delete from pri_user_independent.tb_pri;

--create any table
reset role;
GRANT create any table to any_table_role;
SET ROLE any_table_role PASSWORD 'Gauss@1234';
create table pri_user_independent.tt2(id int);

--alter any table
reset role;
CREATE USER user_test_alter password 'Gauss@1234';
GRANT alter any table to user_test_alter;
SET ROLE user_test_alter PASSWORD 'Gauss@1234';
ALTER TABLE pri_user_independent.tb_pri drop column age;

--drop any table
reset role;
GRANT drop any table to user_test_alter;
SET ROLE user_test_alter PASSWORD 'Gauss@1234';
DROP table pri_user_independent.tb_pri_test1;

reset role;
DROP TABLE pri_user_independent.tb_pri;
DROP TABLE pri_user_independent.tt;
DROP TABLE pri_user_independent.tt2;

DROP USER user_test_alter cascade;
DROP USER any_table_role cascade;
DROP USER pri_user_independent cascade;

