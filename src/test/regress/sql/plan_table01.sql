/*
################################################################################
# TESTCASE NAME : plan_table
# COMPONENT(S)  : plan_table功能测试
################################################################################
*/

--I1.建表
create schema pt_test;
set current_schema = pt_test;

--S1.建立数据源表
create table src(a int) with(autovacuum_enabled = off);
insert into src values(1);
create table t1(a int);
insert into t1 select generate_series(1,199) from src;

--S2.建立倾斜表，并插入数据
create table pt_t1(a integer, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into pt_t1 select generate_series(1,5), generate_series(1,10), generate_series(1,100) from src;
insert into pt_t1 select 12, 12, generate_series(100,1000) from src;

create table pt_t2(a int, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into pt_t2 select * from pt_t1;
insert into pt_t2 select 12, 12, generate_series(100,1000) from src;

create table pt_t3(a int, b int, c int)with(autovacuum_enabled = off) distribute by hash (c);
insert into pt_t3 select * from pt_t1;

--S3.创建测试用户
DROP USER IF EXISTS user2 CASCADE;
CREATE USER user2 PASSWORD 'GAUSS@123';

--S3.Grant权限
grant select on ALL TABLES IN SCHEMA pt_test to public;
set explain_perf_mode = pretty;

--I2.计划存储
--S1.打印计划
explain(verbose on, costs off) 
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb
)
select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
--S2.执行explain plan (无statement_id)
explain plan for
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb
)
select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
--S3.查看plan_table
--查看总表数据
select * from plan_table_data order by plan_id, id;
--查看plan_table数据，条目与总表一致
select * from plan_table order by plan_id, id;
--S4.执行explain plan (设置statement_id)
explain plan set statement_id='test statement_id' for
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb
)
select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
--S5.查看plan_table
--查看总表数据
select * from plan_table_data order by plan_id, id;
--查看plan_table数据,带过滤
select * from plan_table where statement_id='test statement_id' order by plan_id, id;

--I3.数据手动删除
--S1.带where条件
delete from plan_table where statement_id='test statement_id';
--S2.查看plan_table, 删除成功
select * from plan_table_data order by plan_id, id;
select * from plan_table order by plan_id, id;
--S3.不带where条件
delete from plan_table;
--S2.查看plan_table，显示删除成功
select * from plan_table_data order by plan_id, id;
select * from plan_table order by plan_id, id;

--I4.用户访问权限控制
--S1.超级用户对plan_table_data表的权限
-- select Y
select * from plan_table_data order by plan_id, id;
-- insert Y
insert into plan_table_data(session_id,user_id,id) values('111',10,11);
-- update Y
update plan_table_data set user_id=1423;
-- delete Y
delete from plan_table_data;
-- drop N
drop table plan_table_data;

--S2.超级用户对plan_table视图的权限
-- select Y
select * from plan_table order by plan_id, id;
-- insert N
insert into plan_table(id) values(11);
-- update N
update plan_table set id=1423;
-- delete Y
delete from plan_table;
-- drop N
drop VIEW plan_table;

--S3.普通用户对plan_table_data表的权限
SET SESSION SESSION AUTHORIZATION user2 PASSWORD 'GAUSS@123';
-- select N
select * from plan_table_data order by plan_id, id;
-- insert N 
insert into plan_table_data(session_id,user_id,id) values('111',10,11);
-- update N
update plan_table_data set user_id=1423;
-- delete N
delete from plan_table_data;
-- drop N
drop table plan_table_data;

--S4.普通用户对plan_table视图的权限
-- select Y
select * from plan_table order by plan_id, id;
-- insert N
insert into plan_table(id) values(11);
-- update N
update plan_table set id=1423;
-- delete Y
delete from plan_table;
-- drop N
drop VIEW plan_table;

--S5.对普通用户赋super权后，对plan_table_data的访问：
RESET SESSION AUTHORIZATION;
GRANT ALL PRIVILEGES to user2;
SET SESSION SESSION AUTHORIZATION user2 PASSWORD 'GAUSS@123';
-- select Y
select * from plan_table_data order by plan_id, id;
-- insert N
insert into plan_table_data(session_id,user_id,id) values('111',10,11);
-- update N
update plan_table_data set user_id=1423;
-- delete Y
delete from plan_table_data;
-- drop N
drop table plan_table_data;

--I5.用户隔离
--S1.用户2执行
explain plan set statement_id='user2' for
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb
)
select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
--S2.查看plan_table
select * from plan_table_data order by plan_id, id;
select * from plan_table order by plan_id, id;
--S3.切换session为用户1，数据不进行清理
RESET SESSION AUTHORIZATION;
--S4.查看plan_table
--plan_table_data中仍然有user1的数据
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table_data order by plan_id, id;
--plan_table中无数据
select * from plan_table order by plan_id, id;
--S5.切换自己，不会清零数据
explain plan set statement_id='user1' for
with tmp(a,b) as
(
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb
)
select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
RESET SESSION AUTHORIZATION;
--plan_table_data中有user1的数据
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table_data order by plan_id, id;

--I6.下推测试
--S1.fast query shipping
delete from plan_table;
--S2.explain query
explain select * from pt_t1;
--s3.explain plan
explain plan for select * from pt_t1;
--s4.可以收集object
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I7.不能下推测试
--s1.explain
explain select current_user from pt_t1,pt_t2;
--s2.explain plan 
explain plan for select current_user from pt_t1,pt_t2;
--s3.无法收集object
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I8.join算子options测试
--cartesian
explain plan for select * from pt_t1,pt_t2;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--cartesian
explain plan for select * from pt_t1,pt_t2 where pt_t1.a>50;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--not cartesian
explain plan for select * from pt_t1,pt_t2 where pt_t1.a>pt_t2.a;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--clean
DROP USER user2 CASCADE;
