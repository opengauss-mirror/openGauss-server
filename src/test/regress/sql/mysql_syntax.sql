--create trigger
-- test mysql compatibility trigger 
drop database if exists db_mysql;
create database db_mysql dbcompatibility 'B';
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
\c regression
drop database db_mysql;
