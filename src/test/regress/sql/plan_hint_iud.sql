-- GPC test cases are kept but not really useful since centralized fastcheck disables gpc.
-- setups
create database pl_test DBCOMPATIBILITY 'pg';
\c pl_test;
create schema schema_hint_iud;
set current_schema = schema_hint_iud;
set rewrite_rule = 'magicset, partialpush, uniquecheck, disablerep, intargetlist, predpushforce';
create table t1 (c1 int primary key, c2 int);
create index t1_c2_idx on t1 using btree(c2);
create table t2 (c1 int, c2 int);
create index t2_c2_idx on t2 using btree(c2);
-- insert some data to suppress no statistics warning
insert into t1 values(1,1);
insert into t2 values(1,1);
analyze t1;
analyze t2;

\set EXP 'EXPLAIN  (verbose off, costs off)'

-- INSERT
--- No hint
:EXP insert into t1 select c1, c2 from t2 where c2 = 1;
--- Scan
:EXP insert /*+ indexscan(t2) */ into t1 select c1, c2 from t2 where c2 = 1;
--- No hint
:EXP insert into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1;
--- Join
:EXP insert /*+ hashjoin(tt1 tt2) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1;
--- Leading
:EXP insert /*+ leading((tt2 tt1)) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1;
--- Set
:EXP insert /*+ set(query_dop 1008) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1;
--- Plancache
prepare insert_g as insert /*+ use_gplan */ into t1 select c1, c2 from t2 where c2 = $1;
set plan_cache_mode = force_custom_plan;
:EXP execute insert_g(2);
prepare insert_c as insert /*+ use_cplan */ into t1 select c1, c2 from t2 where c2 = $1;
set plan_cache_mode = force_generic_plan;
:EXP execute insert_c(1);
--- No gpc
select * from dbe_perf.global_plancache_clean;
truncate t1;
prepare insert_nogpc as insert /*+ no_gpc */ into t1 select c1, c2 from t2 where c1 = $1;
execute insert_nogpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
truncate t1;
prepare insert_gpc as insert into t1 select c1, c2 from t2 where c1 = $1;
execute insert_gpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
reset plan_cache_mode;
deallocate all;

-- UPSERT
--- No hint
:EXP insert into t1 select c1, c2 from t2 where c2 = 1 on duplicate key update c2 = c2 + 1;
--- Scan
:EXP insert /*+ indexscan(t2) */ into t1 select c1, c2 from t2 where c2 = 1 on duplicate key update c2 = c2 + 1;
--- No hint
:EXP insert into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1 on duplicate key update c2 = c2 + 1;
--- Join
:EXP insert /*+ hashjoin(tt1 tt2) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1 on duplicate key update c2 = c2 + 1;
--- Leading
:EXP insert /*+ leading((tt2 tt1)) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1 on duplicate key update c2 = c2 + 1;
--- Set
:EXP insert /*+ set(query_dop 1008) */ into t1 select tt1.c1, tt2.c2 from t2 tt1, t2 tt2 where tt1.c1 = tt2.c1 on duplicate key update c2 = c2 + 1;
--- Plancache
prepare upsert_g as insert /*+ use_gplan */ into t1 select c1, c2 from t2 where c2 = $1 on duplicate key update c2 = c2 + 1;
set plan_cache_mode = force_custom_plan;
:EXP execute upsert_g(2);
prepare upsert_c as insert /*+ use_cplan */ into t1 select c1, c2 from t2 where c2 = $1 on duplicate key update c2 = c2 + 1;
set plan_cache_mode = force_generic_plan;
:EXP execute upsert_c(1);
--- No gpc
select * from dbe_perf.global_plancache_clean;
truncate t1;
prepare upsert_nogpc as insert /*+ no_gpc */ into t1 select c1, c2 from t2 where c1 = $1 on duplicate key update c2 = c2 + 1;
execute upsert_nogpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
truncate t1;
prepare upsert_gpc as insert into t1 select c1, c2 from t2 where c1 = $1 on duplicate key update c2 = c2 + 1;
execute upsert_gpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
reset plan_cache_mode;
deallocate all;

-- UPDATE
--- No hint
:EXP update t1 set c2 = 2 where c2 = 1;
--- Scan
:EXP update /*+ indexscan(t1) */ t1 set c2 = 2 where c2 = 1;
--- No hint
:EXP update t1 set c2 = 2 where c2 in (select c2 from t2);
--- Join
:EXP update /*+ hashjoin(t1 t2) */ t1 set c2 = 2 where c2 in (select c2 from t2);
--- Leading
:EXP update /*+ leading((t2 t1))*/ t1 set c2 = 2 where c2 in (select c2 from t2);
--- Set
:EXP update /*+ set(query_dop 1008) */ t1 set c2 = 2 where c2 in (select c2 from t2);
--- No expand
:EXP update t1 set c2 = 2 where c2 in (select /*+ no_expand */ c2 from t2);
--- Plancache
prepare update_g as update /*+ use_gplan */ t1 set c2 = 2 where c2 = $1;
set plan_cache_mode = force_custom_plan;
:EXP execute update_g(2);
prepare update_c as update /*+ use_cplan */ t1 set c2 = 2 where c2 = $1;
set plan_cache_mode = force_generic_plan;
:EXP execute update_c(1);
--- No gpc
select * from dbe_perf.global_plancache_clean;
truncate t1;
prepare update_nogpc as update /*+ no_gpc */ t1 set c2 = 2 where c2 = $1;
execute update_nogpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
truncate t1;
prepare update_gpc as update t1 set c2 = 2 where c2 = $1;
execute update_gpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
reset plan_cache_mode;
deallocate all;

-- DELETE
--- No hint
:EXP delete t1 where c2 = 1;
--- Scan
:EXP delete /*+ indexscan(t1) */ t1 where c2 = 1;
--- No hint
:EXP delete t1 where c2 in (select c2 from t2);
--- Join
:EXP delete /*+ hashjoin(t1 t2) */ t1 where c2 in (select c2 from t2);
--- Leading
:EXP delete /*+ leading((t2 t1)) */ t1 where c2 in (select c2 from t2);
--- Set
:EXP delete /*+ set(query_dop 1008) */ t1 where c2 in (select c2 from t2);
--- No expand
:EXP delete t1 where c2 in (select /*+ no_expand */ c2 from t2);
--- Plancache
prepare delete_g as delete /*+ use_gplan */ t1 where c2 = $1;
set plan_cache_mode = force_custom_plan;
:EXP execute delete_g(2);
prepare delete_c as delete /*+ use_cplan */ t1 where c2 = 1;
set plan_cache_mode = force_generic_plan;
:EXP execute delete_c(1);
--- No gpc
select * from dbe_perf.global_plancache_clean;
truncate t1;
insert into t1 values(1,1);
prepare delete_nogpc as delete /*+ no_gpc */ t1 where c1 = $1;
execute delete_nogpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
truncate t1;
insert into t1 values(1,1);
prepare delete_gpc as delete t1 where c1 = $1;
execute delete_gpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
reset plan_cache_mode;
deallocate all;

-- MERGE INTO
--- No hint
:EXP merge into t1 using t2 on t1.c1 = t2.c1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
--- Scan
:EXP merge /*+ indexscan(t1) */ into t1 using t2 on t1.c1 = t2.c1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
--- Join
:EXP merge /*+ mergejoin(t1 t2) */ into t1 using t2 on t1.c1 = t2.c1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
--- Leading
:EXP merge /*+ leading((t1 t2))*/ into t1 using t2 on t1.c1 = t2.c1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
--- Set
:EXP merge /*+ set(query_dop 1008) */ into t1 using t2 on t1.c1 = t2.c1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
--- Plancache
prepare merge_g as merge /*+ use_gplan */ into t1 using t2 on t1.c1 = t2.c1 and t1.c1 = $1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
set plan_cache_mode = force_custom_plan;
:EXP execute merge_g(2);
prepare merge_c as merge /*+ use_cplan */ into t1 using t2 on t1.c1 = t2.c1 and t1.c1 = $1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
set plan_cache_mode = force_generic_plan;
:EXP execute merge_c(1);
--- No gpc / PGXC
select * from dbe_perf.global_plancache_clean;
truncate t1;
insert into t1 values(1,1);
prepare merge_nogpc as merge /*+ no_gpc*/ into t1 using t2 on t1.c1 = t2.c1 and t1.c1 = $1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
execute merge_nogpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
truncate t1;
insert into t1 values(1,1);
prepare merge_gpc as merge into t1 using t2 on t1.c1 = t2.c1 and t1.c1 = $1 when matched then update set t1.c2 = t2.c2 when not matched then insert values (t2.c1, t2.c2);
execute merge_gpc(1);
select * from dbe_perf.global_plancache_status where schema_name = 'schema_hint_iud' order by 1,2;
deallocate all;

-- cleanup
drop schema schema_hint_iud cascade;
\c regression;
drop database IF EXISTS pl_test;
