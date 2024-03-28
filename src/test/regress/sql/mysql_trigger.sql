-- 1. test B_FORMAT trigger
drop database if exists testdb_m;
create database testdb_m dbcompatibility 'B';
\c testdb_m
create schema testscm;
create schema testscm2;
create table animals (id int, name char(30));
create table food (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);
create table testscm.animals_scm (id int, name char(30));
create table testscm.food_scm (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);
CREATE OR REPLACE FUNCTION food_insert_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
-- 1.1 create trigger
-- 1.1.1 create trigger + drop trigger
-- 1.1.1.1 trigger without schema
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into animals(id, name) values (1,'panda');
select * from animals;
select count(*) from food;
delete from animals;
delete from food;
drop trigger animals_trigger1;
drop trigger animals_trigger1;
select tgname from pg_trigger;
create trigger animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from food;
delete from testscm.animals_scm;
delete from food;
drop trigger animals_trigger2;
drop trigger animals_trigger2;
select tgname from pg_trigger;
create trigger animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from testscm.food_scm;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from testscm.food_scm;
delete from testscm.animals_scm;
delete from testscm.food_scm;
drop trigger animals_trigger3;
drop trigger animals_trigger3;
select tgname from pg_trigger;
-- 1.1.1.2 trigger with right schema
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from food;
delete from testscm.animals_scm;
delete from food;
drop trigger testscm.animals_trigger2;
drop trigger testscm.animals_trigger2;
select tgname from pg_trigger;
create trigger testscm.animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm.animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from testscm.food_scm;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from testscm.food_scm;
delete from testscm.animals_scm;
delete from testscm.food_scm;
drop trigger testscm.animals_trigger3;
drop trigger testscm.animals_trigger3;
select tgname from pg_trigger;
-- 1.1.1.3 trigger with wrong schema
create trigger testscm.animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm_no.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
-- ERROR:  trigger in wrong schema: "testscm"."animals_trigger1"
drop trigger testscm.animals_trigger1;
-- OK
drop trigger animals_trigger1;
-- ERROR:  schema "testscm_no" does not exist
drop trigger testscm_no.animals_trigger2;
-- OK
drop trigger animals_trigger2;
select tgname from pg_trigger;
-- ERROR:  trigger "animals_trigger_no" does not exist
drop trigger animals_trigger_no;
drop trigger testscm.animals_trigger_no;
drop trigger testscm_no.animals_trigger_no;
-- 1.1.1.4 trigger with bad schema
create trigger testscm.
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger .animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger .
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm..animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm..
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm..
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger ..
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testctg.testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
-- 1.1.2 create trigger if not exists  + drop trigger if exists
-- 1.1.2.1 trigger without schema
create trigger if not exists animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into animals(id, name) values (1,'panda');
select * from animals;
select count(*) from food;
delete from animals;
delete from food;
drop trigger if exists animals_trigger1;
drop trigger if exists animals_trigger1;
select tgname from pg_trigger;
create trigger if not exists animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from food;
delete from testscm.animals_scm;
delete from food;
drop trigger if exists animals_trigger2;
drop trigger if exists animals_trigger2;
select tgname from pg_trigger;
create trigger if not exists animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from testscm.food_scm;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from testscm.food_scm;
delete from testscm.animals_scm;
delete from testscm.food_scm;
drop trigger if exists animals_trigger3;
drop trigger if exists animals_trigger3;
select tgname from pg_trigger;
-- 1.1.2.2 trigger with right schema
create trigger if not exists testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from food;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from food;
delete from testscm.animals_scm;
delete from food;
drop trigger if exists testscm.animals_trigger2;
drop trigger if exists testscm.animals_trigger2;
select tgname from pg_trigger;
create trigger if not exists testscm.animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists testscm.animals_trigger3
after insert on testscm.animals_scm
for each row
begin
    insert into testscm.food_scm(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
select count(*) from testscm.food_scm;
insert into testscm.animals_scm(id, name) values (1,'panda');
select * from testscm.animals_scm;
select count(*) from testscm.food_scm;
delete from testscm.animals_scm;
delete from testscm.food_scm;
drop trigger if exists testscm.animals_trigger3;
drop trigger if exists testscm.animals_trigger3;
select tgname from pg_trigger;
-- 1.1.2.3 trigger with wrong schema
create trigger if not exists testscm.animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger if not exists testscm_no.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
-- NOTICE:  trigger "animals_trigger1" does not exist, skipping
drop trigger if exists testscm.animals_trigger1;
-- OK
drop trigger if exists animals_trigger1;
-- NOTICE:  trigger "animals_trigger1" does not exist, skipping
drop trigger if exists testscm_no.animals_trigger2;
-- OK
drop trigger if exists animals_trigger2;
select tgname from pg_trigger;
-- NOTICE:  trigger "animals_trigger_no" does not exist, skipping
drop trigger if exists animals_trigger_no;
drop trigger if exists testscm.animals_trigger_no;
drop trigger if exists testscm_no.animals_trigger_no;
-- 1.1.3 create trigger execute procedure
create trigger animals_trigger1
after insert on animals
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
drop trigger animals_trigger1;
select tgname from pg_trigger;
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
drop trigger testscm.animals_trigger2;
select tgname from pg_trigger;
-- 1.1.4 create constrain trigger execute procedure
create constraint trigger animals_trigger1
after insert on animals
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
drop trigger animals_trigger1;
select tgname from pg_trigger;
create constraint trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
drop trigger testscm.animals_trigger2;
select tgname from pg_trigger;
-- 1.2 drop trigger on table
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
-- ERROR:  trigger in wrong schema: "testscm"."animals_trigger1"
drop trigger testscm.animals_trigger1 on animals;
-- ERROR:  schema "testscm_no" does not exist
drop trigger testscm_no.animals_trigger1 on animals;
-- ERROR:  trigger "animals_trigger_no" for table "animals" does not exist
drop trigger animals_trigger_no on animals;
drop trigger testscm.animals_trigger_no on animals;
drop trigger testscm_no.animals_trigger_no on animals;
-- OK
drop trigger animals_trigger1 on animals;
drop trigger animals_trigger1 on animals;
select tgname from pg_trigger;
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
-- NOTICE:  trigger "animals.animals_trigger1" for table "animals" does not exist, skipping
drop trigger if exists testscm.animals_trigger1 on animals;
-- NOTICE:  trigger "animals.animals_trigger1" for table "animals" does not exist, skipping
drop trigger if exists testscm_no.animals_trigger1 on animals;
-- NOTICE:  trigger "animals.animals_trigger_no" for table "animals" does not exist, skipping
drop trigger if exists animals_trigger_no on animals;
drop trigger if exists testscm.animals_trigger_no on animals;
drop trigger if exists testscm_no.animals_trigger_no on animals;
-- OK
drop trigger if exists animals_trigger1 on animals;
drop trigger if exists animals_trigger1 on animals;
select tgname from pg_trigger;
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
-- ERROR:  trigger in wrong schema: "testscm2"."animals_trigger2"
drop trigger testscm2.animals_trigger2 on testscm.animals_scm;
-- ERROR:  schema "testscm_no" does not exist
drop trigger testscm_no.animals_trigger2 on testscm.animals_scm;
-- ERROR:  trigger "animals_trigger_no" for table "animals_scm" does not exist
drop trigger animals_trigger_no on testscm.animals_scm;
drop trigger testscm.animals_trigger_no on testscm.animals_scm;
drop trigger testscm_no.animals_trigger_no on testscm.animals_scm;
-- OK
drop trigger testscm.animals_trigger2 on testscm.animals_scm;
drop trigger testscm.animals_trigger2 on testscm.animals_scm;
select tgname from pg_trigger;
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
-- NOTICE:  trigger "testscm.animals_scm.animals_trigger2" for table "testscm.animals_scm" does not exist, skipping
drop trigger if exists testscm2.animals_trigger2 on testscm.animals_scm;
-- NOTICE:  trigger "testscm.animals_scm.animals_trigger2" for table "testscm.animals_scm" does not exist, skipping
drop trigger if exists testscm_no.animals_trigger2 on testscm.animals_scm;
-- NOTICE:  trigger "testscm.animals_scm.animals_trigger_no" for table "testscm.animals_scm" does not exist, skipping
drop trigger if exists animals_trigger_no on testscm.animals_scm;
drop trigger if exists testscm.animals_trigger_no on testscm.animals_scm;
drop trigger if exists testscm2.animals_trigger_no on testscm.animals_scm;
drop trigger if exists testscm_no.animals_trigger_no on testscm.animals_scm;
-- OK
drop trigger if exists testscm.animals_trigger2 on testscm.animals_scm;
drop trigger if exists testscm.animals_trigger2 on testscm.animals_scm;
select tgname from pg_trigger;
-- 1.3 alter trigger
create trigger animals_trigger1
after insert on animals
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
alter trigger animals_trigger1 on animals rename to animals_trigger1_1;
select tgname from pg_trigger;
alter trigger testscm.animals_trigger1 on animals rename to animals_trigger1_2;
alter trigger testscm.animals_trigger1_1 on animals rename to animals_trigger1_2;
alter trigger testscm_no.animals_trigger1_1 on animals rename to animals_trigger1_2;
drop trigger animals_trigger1_1 on animals;
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
select tgname from pg_trigger;
alter trigger animals_trigger2 on testscm.animals_scm rename to animals_trigger2_1;
select tgname from pg_trigger;
alter trigger testscm.animals_trigger2_1 on testscm.animals_scm rename to animals_trigger2_2;
select tgname from pg_trigger;
alter trigger testscm.animals_trigger2_1 on testscm.animals_scm rename to animals_trigger2_2;
alter trigger testscm_no.animals_trigger2_2 on testscm.animals_scm rename to animals_trigger2_3;
drop trigger testscm.animals_trigger2_2 on testscm.animals_scm;
-- 1.4 set search_path to schema
set search_path to testscm;
create trigger animals_trigger2
after insert on animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
alter trigger animals_trigger2 on animals_scm rename to animals_trigger2_1;
drop trigger animals_trigger2_1 on animals_scm;
create trigger testscm.animals_trigger2
after insert on animals_scm
for each row
begin
    insert into food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
end;
/
alter trigger testscm.animals_trigger2 on animals_scm rename to animals_trigger2_1;
drop trigger testscm.animals_trigger2_1 on animals_scm;
set search_path to public;

--bugfix for name long
drop table if exists T_ignore_case_in_dquotes_use_case0007;
drop table if exists T_ignore_case_in_dquotes_use_case0007_1;
create table T_ignore_case_in_dquotes_use_case0007(col1 int,col2 varchar(20));
create table T_ignore_case_in_dquotes_use_case0007_1(col1 int,col2 varchar(20));

create trigger Tri_ignore_case_in_dquotes_use_case0007
after insert on T_ignore_case_in_dquotes_use_case0007
for each row
begin
insert into T_ignore_case_in_dquotes_use_case0007_1 values (1,'INSERT');
end;
/

create trigger Tri_ignore_case_in_dquotes_use_case0008
after insert on T_ignore_case_in_dquotes_use_case0007
for each row
begin
insert into T_ignore_case_in_dquotes_use_case0007_1 values (1,'INSERT');
end;
/

create trigger Tri_ignore_case_in_dquotes_use_case0009
after insert on T_ignore_case_in_dquotes_use_case0007
for each row
begin
insert into T_ignore_case_in_dquotes_use_case0007_1 values (1,'INSERT');
end;
/


create trigger Tri_ignore_case_in_dquotes_use_case0011
after insert on T_ignore_case_in_dquotes_use_case0007
for each row
begin
insert into T_ignore_case_in_dquotes_use_case0007_1 values (1,'INSERT');
end;
/

insert into T_ignore_case_in_dquotes_use_case0007 values(11,'test');

select * from T_ignore_case_in_dquotes_use_case0007;

select * from T_ignore_case_in_dquotes_use_case0007_1;

-- 1.5 cleanup
\c regression
drop database testdb_m;
-- 2. test non B_FORMAT trigger
drop database if exists testdb;
create database testdb;
\c testdb
create schema testscm;
create table animals (id int, name char(30));
create table food (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);
create table testscm.animals_scm (id int, name char(30));
create table testscm.food_scm (id int, foodtype varchar(32), remark varchar(32), time_flag timestamp);
CREATE OR REPLACE FUNCTION food_insert_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO food(id, foodtype, remark, time_flag) values (1,'bamboo', 'healthy', now());
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
-- 2.1 create trigger + alter trigger + drop trigger
create trigger animals_trigger1
after insert on animals
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
alter trigger animals_trigger1 on animals rename to animals_trigger1_1;
select tgname from pg_trigger;
drop trigger animals_trigger1_1;
drop trigger animals_trigger1_1 on animals;
select tgname from pg_trigger;
create trigger animals_trigger2
after insert on testscm.animals_scm
for each row
execute procedure food_insert_func();
select tgname from pg_trigger;
alter trigger animals_trigger2 on testscm.animals_scm rename to animals_trigger2_1;
select tgname from pg_trigger;
drop trigger animals_trigger2_1;
drop trigger animals_trigger2_1 on testscm.animals_scm;
select tgname from pg_trigger;
create trigger testscm.animals_trigger2
after insert on testscm.animals_scm
for each row
execute procedure food_insert_func();
alter trigger testscm.animals_trigger2 on testscm.animals_scm rename to animals_trigger2_1;
drop trigger testscm.animals_trigger2;
create trigger testscm_no.animals_trigger2
after insert on testscm.animals_scm
for each row
execute procedure food_insert_func();
alter trigger testscm_no.animals_trigger2 on testscm.animals_scm rename to animals_trigger2_1;
drop trigger testscm_no.animals_trigger2;

-- 2.2 cleanup
\c regression
drop database testdb;