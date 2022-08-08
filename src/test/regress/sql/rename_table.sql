drop database if exists mysql;

create database mysql dbcompatibility 'B';
\c mysql

create schema test; create schema tbinfo;
create table test.t1(id int);
create table test.t2(c_id int not null primary key, name varchar) partition by range (c_id) (partition t2_p1 values less than(100), partition t2_p2 values less than(200), partition t2_p3 values less than(MAXVALUE));
create view test.t3 as select * from test.t1;

rename table test.t1 to tbinfo.t1, test.t2 to tbinfo.t2, test.t3 to test.t4;

\d tbinfo.t1;
\d tbinfo.t2;
\d test.t4;

rename table test.t4 to tbinfo.t3;

create temp table t5(id int);

rename table t5 to tt;

\c regression
drop database mysql;
create schema test; create schema tbinfo;
create table test.t1(id int);
rename table test.t1 to tbinfo.t1;
alter table test.t1 rename to t2;
drop table test.t2;
drop schema test cascade;
drop schema tbinfo cascade;
