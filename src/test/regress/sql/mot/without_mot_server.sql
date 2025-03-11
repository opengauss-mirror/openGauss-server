--guc param
show enable_mot_server;
set enable_mot_server = on;
set enable_mot_server = off;
select name, setting, category, short_desc, extra_desc, context, vartype, boot_val, reset_val from pg_settings where name = 'enable_mot_server';
-- pre-operation, create user and schema
drop user if exists test_mot;
create user test_mot password 'Test@123';
GRANT USAGE ON FOREIGN SERVER mot_server TO test_mot;
grant all privileges to test_mot;
set session authorization test_mot password 'Test@123';
create database disable_mot;
\c disable_mot;
drop schema if exists disable_mot_test cascade;
create schema disable_mot_test;
set current_schema to disable_mot_test;
\dt
-- create server
create server my_server foreign data wrapper log_fdw;
drop server my_server;
create server my_server1 foreign data wrapper mot_fdw;
drop server my_server1;
-- create object
create table test_table(id int);
create foreign table test(id int) server mot_server;
insert into test values(1),(2),(3);
update test set id = 5 where id = 3;
select * from test;
delete from test;
prepare pre1 as select * from test;
prepare pre2 as insert into test values(1);
prepare pre3 as delete from test;
prepare pre4 as update test set id = 6;
prepare pre5 as insert into test values($1);
execute pre1;
execute pre2;
execute pre1;
execute pre3;
execute pre1;
insert into test values(1),(2),(3);
execute pre1;
execute pre4;
execute pre1;
execute pre5(8);
execute pre1;
create view test_view as select * from test;
select * from test_view;
insert into test values(1),(2),(3);
select * from test;
explain select * from test;
vacuum test;
analyze test;
analyse test;
truncate table test;
alter foreign table test rename to test_mot_table;
create foreign table test1(id int);
create foreign table test2(id int primary key);
create foreign table bmsql_oorder (
  o_w_id       integer      not null,
  o_d_id       integer      not null,
  o_id         integer      not null,
  o_c_id       integer not null,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  o_entry_d    timestamp,
  primary key (o_w_id, o_d_id, o_id)
);
create index  bmsql_oorder_index1 on bmsql_oorder(o_w_id, o_d_id, o_c_id, o_id);
drop view test_view;
drop foreign table test_mot_table;
drop foreign table test1;
drop foreign table test2;
drop foreign table bmsql_oorder;
\dt
create table test1(id int);
create view test_view1 as select * from test1;
insert into test1 values(1),(2),(3);
select * from test1;
select * from test_view1;
begin;
insert into test1 values(4),(5),(6);
select * from test1;
select * from test_view1;
commit;
select * from test1;
select * from test_view1;
begin;
insert into test1 values(7),(8),(9);
select * from test1;
select * from test_view1;
rollback;
select * from test1;
select * from test_view1;
\dt
CREATE foreign TABLE grade
(
  number INTEGER,
  name CHAR(20),
  class CHAR(20),
  grade INTEGER
);
-- procedure
CREATE PROCEDURE insert_data1(param1 INT = 0, param2 CHAR(20), param3 CHAR(20), param4 INT = 0)
IS
 BEGIN
 INSERT INTO grade VALUES(param1, param2, param3, param4);
END;
/
CALL insert_data1(param1:=210101, param2:='Alan', param3:='21.01', param4:=92);
select * from grade;
DROP PROCEDURE insert_data1;
drop foreign table grade;
-- system tables
select extname from pg_extension where extname = 'mot_fdw';
select * from pg_foreign_table;
select * from pg_foreign_server;
select * from pg_foreign_data_wrapper;
-- system functions
select * from mot_global_memory_detail() limit 1;
select * from mot_jit_detail();
select * from mot_jit_profile();
select * from mot_local_memory_detail() limit 1;
select * from mot_session_memory_detail() limit 1;
select * from mot_fdw_handler();
-- post-operation, clean
drop table test_table;
drop table test1;
drop view test_view1;
drop schema disable_mot_test cascade;
\c regression
revoke USAGE ON FOREIGN SERVER mot_server from test_mot;
drop schema test_mot cascade;
drop database disable_mot;
drop user test_mot;
