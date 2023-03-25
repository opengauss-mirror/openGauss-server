create database db1 dbcompatibility='B';
\c db1;
create user use_1136631 password 'Aa123456';
create table tab_1136631(id int unique,a1 varchar(20));
create view v_1136631 as select * from tab_1136631;
create definer=use_1136631 view v_1136631 as select * from tab_1136631;
create or replace definer=use_1136631 view v_1136631 as select * from tab_1136631;
\c postgres
drop database db1;
drop user use_1136631 cascade;