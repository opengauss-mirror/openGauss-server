--create trigger
-- test mysql compatibility trigger 
drop database if exists db_mysql;
create database db_mysql dbcompatibility 'B';
drop database if exists db_td;
create database db_td dbcompatibility='C';

\c db_td
create table animals (id int, name char(30));
create table food (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);

create trigger animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/

create or replace trigger animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/
\c db_mysql
create table t (id int);
create table t1 (id int);
create table animals (id int, name char(30));
create table food (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);

--definer test 
create definer=d_user1 trigger animal_trigger1
after insert on t
for each row
begin 
    insert into t1 values(3);
end;
/
/*
create user test with sysadmin password 'Gauss@123';
create user test1 password 'Gauss@123';

grant all on t ,t1 to test;
grant all on t ,t1 to test1;
set role test password 'Gauss@123';

create definer = test1 trigger animal_trigger1
after insert on t
for each row
begin 
    insert into t1 values(3);
end;
/
insert into t values(3);
set role d_user1 password 'Aa123456';
alter role d_user1 identified by '123456Aa' replace 'Aa123456';
insert into t values(3);
select * from t1;

reset role;
drop trigger animal_trigger1 on t;
*/
-- trigger_order{follows|precedes} && begin ... end test
create trigger animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/

create or replace trigger animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/

--different type trigger follows|precedes

create trigger animal_trigger2
before insert on animals
for each row
follows animal_trigger1
begin
    insert into food(id, foodtype, remark, time_flag) values (2,'chocolate', 'sdsdsdsd', now());
end;
/

create trigger animal_trigger2
after insert on animals
for each row
follows animal_trigger1
begin
    insert into food(id, foodtype, remark, time_flag) values (2,'chocolate', 'sdsdsdsd', now());
end;
/

create trigger animal_trigger3
after insert on animals
for each row
follows animal_trigger1
begin
    insert into food(id, foodtype, remark, time_flag) values (3,'cake', 'sdsdsdsd', now());
end;
/

create trigger animal_trigger4
after insert on animals
for each row
follows animal_trigger1
begin
    insert into food(id, foodtype, remark, time_flag) values (4,'sausage', 'sdsdsdsd', now());
end;
/

insert into animals (id, name) values(1,'lion');
select id, foodtype, remark from food;
delete from food;

create trigger animal_trigger5
after insert on animals
for each row
precedes animal_trigger3
begin
    insert into food(id, foodtype, remark, time_flag) values (5,'milk', 'sdsds', now());
end;
/

create trigger animal_trigger6
after insert on animals
for each row
precedes animal_trigger2
begin
    insert into food(id, foodtype, remark, time_flag) values (6,'strawberry', 'sdsds', now());
end;
/
insert into animals (id, name) values (2, 'dog');
select id, foodtype, remark from food;
delete from food;

create trigger animal_trigger7
after insert on animals
for each row
follows animal_trigger5
begin
    insert into food(id, foodtype, remark, time_flag) values (7,'jelly', 'sdsds', now());
end;
/
insert into animals (id,name) values(3,'cat');
select id, foodtype, remark from food;

-- if not exists test
create trigger animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/

create trigger if not exists animal_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
end;
/

CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO food(id, foodtype, remark, time_flag) values (1,'ice cream', 'sdsdsdsd', now());
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;

create trigger trigger_rename_test
after insert on animals
for each row
EXECUTE PROCEDURE tri_insert_func();

create trigger trigger_rename_test
after insert on food
for each row
EXECUTE PROCEDURE tri_insert_func();

drop trigger trigger_rename_test;
-- drop trigger test
drop trigger animal_trigger1;
drop trigger animal_trigger1;
drop trigger if exists animal_trigger1;
drop table food;
drop table animals;

DROP TABLE t_trigger cascade;
CREATE TABLE t_trigger(
id int primary key,
name varchar(20) not null
)partition by hash(id)
(partition p1 ,
partition p2,
partition p3);
INSERT INTO t_trigger values(1,'liuyi');
INSERT INTO t_trigger values(2,'chener');
DROP TABLE t_func_trigger;
CREATE TABLE t_func_trigger(rep text);
create user vbadmin password 'Aa@111111';
CREATE definer=vbadmin TRIGGER trigger_insert
AFTER insert
ON t_trigger
FOR EACH ROW
BEGIN 
insert into t_func_trigger(rep) values('after insert');END;
/
drop trigger trigger_insert on t_trigger;
drop user vbadmin;
-- test declare cursor
create table company(name varchar(100), loc varchar(100), no integer);
insert into company values ('macrosoft',    'usa',          001);
insert into company values ('oracle',       'usa',          002);
insert into company values ('backberry',    'canada',       003);
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

begin 
    declare c1_all cursor is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_1();
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

begin 
    declare c1_all cursor is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
    declare c1_all cursor is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
-- mysql fetch 自动退出
show b_format_behavior_compat_options;
set b_format_behavior_compat_options = 'fetch';
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

begin 
    declare c1_all cursor is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_1();
set b_format_behavior_compat_options = '';
show b_format_behavior_compat_options;
-- test declare condition
create or replace procedure test_condition_1 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE '22012';
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
call test_condition_1();
-- test rename condition
create or replace procedure test_condition_2 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE '22012';
    declare DIVISION_ZERO_two condition for SQLSTATE '22012';
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
call test_condition_2();

-- test reuse condition name
create or replace procedure test_condition_3 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE '22012';
    declare DIVISION_ZERO condition for SQLSTATE '22005';
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
-- declare condition sqlcode
create or replace procedure test_condition_4 as
BEGIN
    declare DIVISION_ZERO condition for 1;
    RAISE NOTICE 'declare condition successed';
END;
/
call test_condition_4();
-- declare condition sqlcode 0
create or replace procedure test_condition_5 as
BEGIN
    declare DIVISION_ZERO condition for 0;
    RAISE NOTICE 'declare condition successed';
END;
/
-- declare condition sqlstate begin with '00'
create or replace procedure test_condition_6 as
BEGIN
    declare DIVISION_ZERO condition for sqlstate '00000';
    RAISE NOTICE 'declare condition successed';
END;
/
create or replace procedure test_condition_1 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE value '22012';
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
call test_condition_1();
create or replace procedure test_condition_1 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE "22012";
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
call test_condition_1();
-- test other values compilte with condition name

create or replace procedure test_condition_7 as
declare
    a int;
BEGIN
    declare a condition for SQLSTATE '22012';
    a := 1/0;
exception
    when a then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
call test_condition_7();
\c regression
drop trigger animal_trigger1;
drop trigger if exists animal_trigger1;
drop database db_mysql;
drop database db_td;

-- test declare condition in other compatibility
create or replace procedure test_condition_1 as
declare
    a int;
BEGIN
    declare DIVISION_ZERO condition for SQLSTATE '22012';
    a := 1/0;
exception
    when DIVISION_ZERO then
    BEGIN
        RAISE NOTICE 'SQLSTATE = %, SQLERRM = %', SQLSTATE,SQLERRM;
    END;
END;
/
