----------------------------------------------------------
-- create table add PRIMARY KEY constraint
----------------------------------------------------------

-- alter table add constrain NULL
create table test1(id int PRIMARY KEY, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constrain ENABLE
create table test1(id int PRIMARY KEY ENABLE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE VALIDATE
create table test1(id int PRIMARY KEY ENABLE VALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
create table test1(id int PRIMARY KEY ENABLE NOVALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint DISABLE VALIDATE
create table test1(id int PRIMARY KEY DISABLE VALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
create table test1(id int PRIMARY KEY DISABLE NOVALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
update test1 set name = 'eee' where id = 1; 
delete from test1 where id = 1;
drop table test1;

-- alter table add constraint DISABLE
create table test1(id int PRIMARY KEY DISABLE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_pkey%';
insert into test1 values(1, 'aaa');
update test1 set name = 'eee' where id = 1; 
delete from test1 where id = 1;
drop table test1;

----------------------------------------------------------
-- alter table add UNIQUE constraint
----------------------------------------------------------

-- alter table add constrain NULL
create table test1(id int UNIQUE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constrain ENABLE
create table test1(id int UNIQUE ENABLE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE VALIDATE
create table test1(id int UNIQUE ENABLE VALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
create table test1(id int UNIQUE ENABLE NOVALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint DISABLE VALIDATE
create table test1(id int UNIQUE DISABLE VALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
create table test1(id int UNIQUE DISABLE NOVALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
update test1 set name = 'eee' where id = 1; 
delete from test1 where id = 1;
drop table test1;

-- alter table add constraint DISABLE
create table test1(id int UNIQUE DISABLE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(1, 'aaa');
update test1 set name = 'eee' where id = 1; 
delete from test1 where id = 1;
drop table test1;
----------------------------------------------------------
-- create table add check constraint
----------------------------------------------------------

-- alter table add constrain NULL
create table test1(id int check (id>10), name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constrain ENABLE
create table test1(id int check (id>10) ENABLE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE VALIDATE
create table test1(id int check (id>10) ENABLE VALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
create table test1(id int check (id>10) ENABLE NOVALIDATE, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(1, 'ccc');
drop table test1;

-- alter table add constraint DISABLE VALIDATE
create table test1(id int check (id>10) disable validate, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
create table test1(id int check (id>10) disable novalidate, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
update test1 set name = 'eee' where id = 11; 
delete from test1 where id = 11;
drop table test1;

-- alter table add constraint DISABLE
create table test1(id int check (id>10) disable, name varchar2(10));
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname like 'test1_%';
insert into test1 values(11, 'aaa');
update test1 set name = 'eee' where id = 11; 
delete from test1 where id = 11;
drop table test1;
----------------------------------------------------------
-- alter table add PRIMARY KEY constraint
----------------------------------------------------------

-- alter table add constrain NULL
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name);
drop table test1;

-- alter table add constrain ENABLE
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name);
drop table test1;

-- alter table add constraint ENABLE VALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) ENABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) ENABLE VALIDATE;
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT bname_unq PRIMARY KEY(name) ENABLE NOVALIDATE;
delete from test1 where id=4;
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) ENABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(5, 'hongloumeng');
insert into test1 values(6, 'xiyouji');
update test1 set name='sanguoyanyi' WHERE id=2;
delete from test1 WHERE id=2; 

-- alter table add constraint DISABLE VALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) DISABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) DISABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

-- alter table add constraint DISABLE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_primary PRIMARY KEY(name) DISABLE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_primary';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

----------------------------------------------------------
-- alter table add unique constraint
----------------------------------------------------------

-- alter table add constrain NULL
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name);
drop table test1;

-- alter table add constrain ENABLE
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name);
drop table test1;

-- alter table add constraint ENABLE VALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) ENABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'hongloumeng');
insert into test1 values(5, 'xiyouji');
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) ENABLE VALIDATE;
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
insert into test1 values(4, 'xiyouji');
ALTER TABLE test1 ADD CONSTRAINT bname_unq UNIQUE(name) ENABLE NOVALIDATE;
delete from test1 where id=4;
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) ENABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(5, 'hongloumeng');
insert into test1 values(6, 'xiyouji');
update test1 set name='sanguoyanyi' WHERE id=2;
delete from test1 WHERE id=2; 

-- alter table add constraint DISABLE VALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) DISABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) DISABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

-- alter table add constraint DISABLE
CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
insert into test1 values(2, 'shuifuzhuan');
insert into test1 values(3, 'sanguoyanyi');
ALTER TABLE test1 ADD CONSTRAINT name_unq UNIQUE(name) DISABLE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'name_unq';
insert into test1 values(4, 'hongloumeng'); 
update test1 set name='sanguozhi' WHERE id=3;
delete from test1 WHERE id=3;
drop table test1;

------------------------------------------------
-- alter table add check constraint
------------------------------------------------

-- alter table add constrain NULL
create table test1(id int, name varchar2(10));
alter table test1 add constraint id_check check(id > 10);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(5, 'eee'); 
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
alter table test1 add constraint id_check check(id > 10);
delete from test1 where id=1;
alter table test1 add constraint id_check check(id > 10);
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(2, 'eee');
drop table test1;

-- alter table add constrain ENABLE
create table test1(id int, name varchar2(10));
alter table test1 add constraint id_check check(id > 10) ENABLE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(5, 'eee'); 
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
alter table test1 add constraint id_check check(id > 10) ENABLE;
delete from test1 where id=1;
alter table test1 add constraint id_check check(id > 10) ENABLE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(2, 'eee');
drop table test1;

-- alter table add constraint ENABLE VALIDATE
create table test1(id int, name varchar2(10));
alter table test1 add constraint id_check check(id > 10) ENABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(5, 'eee'); 
drop table test1;

CREATE TABLE test1 (id int, name VARCHAR2(50));
insert into test1 values(1, 'xiyouji');
alter table test1 add constraint id_check check(id > 10) ENABLE VALIDATE;
delete from test1 where id=1;
alter table test1 add constraint id_check check(id > 10) ENABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(2, 'eee');
drop table test1;

-- alter table add constraint ENABLE NOVALIDATE
create table test1(id int, name varchar2(10));
insert into test1 values(1, 'aaa');
insert into test1 values(2, 'bbb');
insert into test1 values(3, 'ccc');
alter table test1 add constraint id_check check(id > 10) ENABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(11, 'ddd');
insert into test1 values(5, 'eee');
drop table test1;

-- alter table add constraint DISABLE VALIDATE
create table test1(id int, name varchar2(10));
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(13, 'ccc');
alter table test1 add constraint id_check check(id > 10) DISABLE VALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(14, 'ddd');
update test1 set id = 21  where name = 'aaa';
delete from test1 where id = 12;
drop table test1;

-- alter table add constraint DISABLE NOVALIDATE
create table test1(id int, name varchar2(10));
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(13, 'ccc');
alter table test1 add constraint id_check check(id > 10) DISABLE NOVALIDATE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(14, 'ddd');
update test1 set id = 21  where name = 'aaa';
delete from test1 where id = 12;
drop table test1;

-- alter table add constraint DISABLE
create table test1(id int, name varchar2(10));
insert into test1 values(11, 'aaa');
insert into test1 values(12, 'bbb');
insert into test1 values(13, 'ccc');
alter table test1 add constraint id_check check(id > 10) DISABLE;
select conname,contype,condeferrable,condeferred,convalidated,condisable from pg_constraint WHERE conname = 'id_check';
insert into test1 values(14, 'ddd');
update test1 set id = 21  where name = 'aaa';
delete from test1 where id = 12;
drop table test1;