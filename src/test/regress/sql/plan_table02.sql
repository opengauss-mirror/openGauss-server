/*
################################################################################
# TESTCASE NAME : plan_table
# COMPONENT(S)  : plan_table:列内容测试
################################################################################
*/

--I1.设置guc参数
set current_schema = pt_test;
set explain_perf_mode = pretty;

--I2.创建列存表
create table vec_pt_t1(a integer, b int, c int)with (orientation=column) distribute by hash (c);
create table vec_pt_t2(a int, b int, c int)with (orientation=column) distribute by hash(c);
create table vec_pt_t3(a int, b int, c int)with (orientation=column) distribute by hash(c);
insert into vec_pt_t1 select * from pt_t1;
insert into vec_pt_t2 select * from pt_t2;
insert into vec_pt_t3 select * from pt_t3;

--I3.行存测试
--S1.stream + join + agg + subquery
--执行explain plan
explain plan 
set statement_id='row' for                                                                                                                
with tmp(a,b) as                                                                                                                                                    
(                                                                                                                                                                       
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb                                                                        
)select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;
--S2.查询plan_table
select * from plan_table_data where statement_id = 'row' order by plan_id, id;
--S3.进行计划的对比
explain(verbose on, costs off)
with tmp(a,b) as 
( 
   select count(s.a) as sa, s.b as sb from pt_t3 s, pt_t2 t2 where s.a = t2.a group by sb      
)select count(*) from pt_t1 t1, tmp where t1.b = tmp.b;

--I4.列存测试
--S1.stream + join + agg + subquery
--执行explain plan
explain plan 
set statement_id='vector' for                                                                                                                
with tmp(a,b) as                                                                                                                                                   
(                                                                                                                                                                       
  select count(s.a) as sa, s.b as sb from vec_pt_t3 s, vec_pt_t2 t2 where s.a = t2.a group by sb                                                                        
)select count(*) from vec_pt_t1 t1, tmp where t1.b = tmp.b;
--S2.查询plan_table
select * from plan_table_data where statement_id = 'vector' order by plan_id, id;
--S3.进行计划的对比
explain(verbose on, costs off)
with tmp(a,b) as 
( 
  select count(s.a) as sa, s.b as sb from vec_pt_t3 s, vec_pt_t2 t2 where s.a = t2.a group by sb      
)select count(*) from vec_pt_t1 t1, tmp where t1.b = tmp.b;

--I5.index scan
create index t1_indexa on pt_t1(a);
create index t2_indexb on pt_t2(b);
--S1.执行explain plan
explain plan set statement_id='indext test' for select t1.a from pt_t1 t1 join pt_t2 on t1.a=pt_t2.c;
--S2.查询plan_table
select * from plan_table_data where statement_id = 'indext test' order by plan_id, id;
--S3.进行计划的对比
explain(verbose on, costs off) select t1.a from pt_t1 t1 join pt_t2 on t1.a=pt_t2.c;

--I6.bitmap heap scan
create index t1_indexb on pt_t1(b);
--S1.执行explain plan
explain plan set statement_id='bitmap test' for select a,b from pt_t1 where a=1 or b=2;
--S2.查询plan_table
select * from plan_table_data where statement_id = 'bitmap test' order by plan_id, id;
--进行计划对比
explain(verbose on, costs off)
select a,b from pt_t1 where a=1 or b=2;

--I7.function
explain plan set statement_id='function test' for select pg_backend_pid();
--S2.查询plan_table
select * from plan_table_data where statement_id = 'function test' order by plan_id, id;
--进行计划对比
explain(verbose on, costs off)
select pg_backend_pid();

--I8.partition表（object_type与A db一致）
create table test_cstore_pg_table_size (a int)
partition by range (a)
(
partition test_cstore_pg_table_size_p1 values less than (2),
partition test_cstore_pg_table_size_p2 values less than (4)
);
insert into test_cstore_pg_table_size values (1), (3);
explain plan set statement_id='partition test' for select * from test_cstore_pg_table_size where a>2;
select * from plan_table_data where statement_id = 'partition test' order by plan_id, id;

--I9.大小写敏感测试
--S1.object_name测试
create table "PT_t5"(A int, b int);
explain plan for select * from "PT_t5";
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--S2.列大小写测试
create table "PT_t6"("A" int, "b" int);
explain plan for select * from "PT_t6";
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--S3.schema测试
create schema "pt_test_SCHEMA";
set current_schema to "pt_test_SCHEMA";
create table "PT_t7"("A" int, "b" int);
explain plan for select * from "PT_t7";
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--S4.表别名测试:不存储别名
explain plan for select * from "PT_t7" "A_pt7";
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
--S5.列别名测试:不存储别名
create table test(a int);
explain plan for select a.b as ab from "PT_t7" a, test where a.b=test.a;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I10.长度测试
--S1.statement_id长度31报错
explain plan set statement_id='kkkkkkkkkkkkkkkkkkkkkkkkkkkk31k' for select * from test;
--S2.statement_id长度30，成功
explain plan set statement_id='kkkkkkkkkkkkkkkkkkkkkkkkkkkk30' for select * from test;
--S3.object name长度
create table kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk63(a int);
explain plan set statement_id='kkkkkkkkkkkkkkkkkkkkkkkkkkkk30' for select * from kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk63;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I11.CTE测试
--S1.with recursive中不下推的CTE，仅收集主plan的信息，收集到ctescan节点。
CREATE TABLE chinamap
(
  id INTEGER,
  pid INTEGER,
  name TEXT
) with (orientation=column) distribute by hash(id);
--计划对比
explain (costs false) with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;
--plan_table收集的计划
explain plan for with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq order by 1;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I12.Object 类型补充测试:rtekind(SUBQUERY, JOIN, VALUES, CTE, REMOTE_DUMMY)
--S1.能下推的CTE,收集所有的节点信息。
--计划对比
delete from plan_table;
--S2.VALUES
--计划对比
explain(verboe on, costs off) select * from (values(1,2)) as t1(a), (values (3,4)) as t2(a);
--plan_table收集的计划
explain plan for select * from (values(1,2)) as t1(a), (values (3,4)) as t2(a);
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--S3.REMOTE QUERY
set current_schema to pt_test;
---计划对比
explain select current_user from pt_t1,pt_t2;
---计划收集
explain plan for select current_user from pt_t1,pt_t2;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;

--I13.Select for update测试
set current_schema to "pt_test_SCHEMA";
--计划对比
explain select * from test for update;
--计划收集
explain plan for select * from test for update;
select statement_id,id,operation,options,object_name,object_type,object_owner,projection from plan_table order by plan_id, id;
delete from plan_table;
explain select * from test for update;
explain plan for select * from test for update;
set current_schema to pt_test;
--clean
drop schema pt_test cascade;
drop schema "pt_test_SCHEMA" cascade;
