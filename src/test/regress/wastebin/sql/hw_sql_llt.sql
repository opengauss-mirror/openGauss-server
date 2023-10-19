--test function mark_distribute_dml
create table test_mark_distribute_dml(a integer);
insert into test_mark_distribute_dml values(1), (9);
create view test_mark_distribute_dml_view as select * from test_mark_distribute_dml;
insert into test_mark_distribute_dml_view values(3);
update test_mark_distribute_dml_view set a = 10;
delete from test_mark_distribute_dml_view;
drop view test_mark_distribute_dml_view;
drop table test_mark_distribute_dml;
--test function mark_distribute_dml->stream_broadcast
create table test_mark_distribute_dml_t1 (a integer);
insert into test_mark_distribute_dml_t1 values(1);
create table test_mark_distribute_dml_t2 (col integer primary key);
insert into test_mark_distribute_dml_t2 select * from test_mark_distribute_dml_t1;
drop table test_mark_distribute_dml_t1;
drop table test_mark_distribute_dml_t2;

--test function locate_join_clause
create table test_locate_join_1(a integer);
create table test_locate_join_2(b integer);
insert into test_locate_join_2 (select a from test_locate_join_1, test_locate_join_2 where a < b);
drop table test_locate_join_1;
drop table test_locate_join_2;

--test function stream_walker
create table test_stream_walker(c1 integer, c2 integer); 
insert into test_stream_walker values(1, 2);
WITH RECURSIVE t(n) AS (
    VALUES (1)
  UNION ALL
    SELECT c1+1 FROM test_stream_walker WHERE c1 < 100
)
SELECT sum(c1) FROM test_stream_walker;
insert into test_stream_walker values(2, 9);
select distinct sum(c1) from test_stream_walker;
select distinct on(c2) sum(c1) from test_stream_walker group by c2;

--test function mark_group_stream
create table test_mark_group_stream_tb1(t1_1 integer, t1_2 integer);
create table test_mark_group_stream_tb2 (t2_1 integer, t2_2 integer);
insert into test_mark_group_stream_tb1 values(1, 2);
insert into test_mark_group_stream_tb2 values(1, 2);
select * from test_mark_group_stream_tb2 where (t2_2) in (select t1_2 from test_mark_group_stream_tb1 group by t1_2 order by t1_2);
drop table test_mark_group_stream_tb1;
drop table test_mark_group_stream_tb2;

--test function check_var_nonnullable
create table test_check_var_nonnullable_t1(t1_1 integer, t1_2 integer);
create table test_check_var_nonnullable_t2(t2_1 integer, t2_2 integer);
select * from test_check_var_nonnullable_t1 where t1_1 not in (select t2_1 from test_check_var_nonnullable_t2 inner join test_check_var_nonnullable_t1 on(t2_2 = t1_2));
select * from test_check_var_nonnullable_t1 where t1_1 not in (select min(t2_1) from test_check_var_nonnullable_t2 inner join test_check_var_nonnullable_t1 on(t2_2 = t1_2));
drop table test_check_var_nonnullable_t1;
drop table test_check_var_nonnullable_t2;

--test function adjust_all_pathkeys_by_agg_tlist
create TABLE test_pathkeys_by_agg_tlist_window(a integer, b integer);
select  min(b) OVER (PARTITION BY b) from test_pathkeys_by_agg_tlist_window group by b;

--test function CopyToCompatiblePartions
create table test_copyto_partitioned_table (a int)
partition by range (a)
(
	partition test_reindex_partitioned_table_p1 values less than (10),
	partition test_reindex_partitioned_table_p2 values less than (20)
);
insert into test_copyTo_partitioned_table values(1);
copy test_copyto_partitioned_table to stdout (delimiter '|');
drop table test_copyto_partitioned_table;

--test function _readMergeAppend
create table test_mergeappend_1(a integer);
create table test_mergeappend_2(a integer);
(select * from test_mergeappend_1 order by a) union all (select * from test_mergeappend_2 order by 1) order by 1;

--test function _readFunctionScan
create table test_func_scan_1(col integer);
insert into test_func_scan_1 values(1);
select * from generate_series(1, 21, 1), test_func_scan_1;
drop table test_func_scan_1;

--test function check_log_duration
set log_duration = true;
select 1;
reset log_duration;

--test function show_sort_info
create table test_sort_exlpain(a integer, b integer);
explain analyze select * from test_sort_exlpain order by 1;
explain analyze select * from test_sort_exlpain order by 1;
explain (analyze on,  format json)select * from test_sort_exlpain order by 1;
explain analyze select generate_series(1, 5) order by 1;
explain (analyze on, format json) select generate_series(1, 5) order by 1;
drop table test_sort_exlpain;

--test function AddRoleMems DelRoleMems
create role test_addrolemems_1 password 'Ttest@123';
create role test_addrolemems_2 password 'Ttest@123';
create role test_addrolemems_3 sysadmin password 'Ttest@123';
create role test_addrolemems_4 password 'Ttest@123';
set role test_addrolemems_1 password 'Ttest@123';
grant test_addrolemems_2 to test_addrolemems_1;
grant test_addrolemems_3 to test_addrolemems_2;
reset role;
grant test_addrolemems_1 to test_addrolemems_2;
grant test_addrolemems_1 to test_addrolemems_2;
grant test_addrolemems_2 to test_addrolemems_1;
grant test_addrolemems_3 to test_addrolemems_1;
grant test_addrolemems_1 to test_addrolemems_4;
set role test_addrolemems_4 password 'Ttest@123';
revoke test_addrolemems_1 from test_addrolemems_2;
REVOKE  test_addrolemems_3 FROM test_addrolemems_1;
reset role;
drop role test_addrolemems_1;
drop role test_addrolemems_2;
drop role test_addrolemems_3;
drop role test_addrolemems_4;

--test function check_log_statement
set log_statement = ddl;
create table test_check_log_statement (a integer);
reset log_statement;
drop table test_check_log_statement;

--test function transformExecDirectStmt
create table test_transformExecDirectStmt (a integer, b integer);

--test function contain_stream_plan_node: Append, MergeAppend
create table test_contain_stream_plan_node_1 (a1 integer, b1 integer, c1 integer);
create table test_contain_stream_plan_node_2 (a2 integer, b2 integer, c2 integer);
create table test_contain_stream_plan_node_3 (a3 integer, b3 integer, c3 integer);
set enable_nestloop = false;
select * from test_contain_stream_plan_node_1 as t1 left join (select * from test_contain_stream_plan_node_2 union select * from test_contain_stream_plan_node_3) as t2 on (t1.a1 = t2.a2);
reset enable_nestloop;
select * from test_contain_stream_plan_node_1 as t1 left join ((select a2 from test_contain_stream_plan_node_2 order by 1) union all(select a3 from test_contain_stream_plan_node_3 order by 1) order by 1) as t2 on (t1.a1 < t2.a2);

--test function ResetUsage
set log_parser_stats = on;
select 1;
reset log_parser_stats;

--test function getWeights
SELECT ts_rank(array[[1, 1], [1, 0]], ' a:1 s:2C d g'::tsvector, 'a | s');
SELECT ts_rank(array[1, 1, 1], ' a:1 s:2C d g'::tsvector, 'a | s');
SELECT ts_rank(array[1, 1, 1, ''], ' a:1 s:2C d g'::tsvector, 'a | s');
SELECT ts_rank(array[1, 1, 1, 2], ' a:1 s:2C d g'::tsvector, 'a | s');

--test function tsvector_concat 
select ' a:1 s:2C d g'::tsvector || ' a:1 s:2C d g'::tsvector;
select ' b:1 s:2C d g'::tsvector || ' a:1 s:2C d g'::tsvector;
select ' a s:2C d g'::tsvector || ' a:1 s:2C d g'::tsvector;
select ' e f g h'::tsvector || ' a:1 b:2C c d'::tsvector;
select ' a:1 b:2C c d'::tsvector || ' e f g h'::tsvector;

-- function compute_return_type
create user test_func_user password 'Ttest@123';
CREATE OR REPLACE FUNCTION my_union(internal, internal)
RETURNS test_func_user.internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
set enforce_a_behavior=off;
CREATE OR REPLACE FUNCTION my_union(internal, internal)
RETURNS test_func_user.internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
reset enforce_a_behavior;
--function  CreateFunction
revoke create on schema test_func_user from test_func_user;
create or replace function test_func_user.test_CreateFunction_fun() returns integer
AS
$$
begin
    return 1;
end;
$$language plpgsql;
create table test_CreateFunction_tbl (a integer);
create view test_CreateFunction_view as select * from  test_CreateFunction_tbl;
create or replace function test_CreateFunction_fun()RETURNS SETOF test_CreateFunction_view
as $$
select * from test_CreateFunction_view;
$$language sql;
drop user test_func_user;
drop view  test_CreateFunction_view;
drop table test_CreateFunction_tbl;
--test function rawnlike
select rawnlike('123', 'abc');
--test funciton byteagt
select byteagt('23', '123');

--test pg_thread_wait_status
select count(*)>=0 from pg_thread_wait_status; 
select count(*)>=0 from pgxc_thread_wait_status; 

--test view with setop
create table setop_a(id int,name varchar);
create table setop_b(id int,name varchar);
create view v as (select id ,name from setop_a INTERSECT (select id,name from setop_b));
select definition from pg_views where viewname = 'v';
drop view v;
drop table setop_a,setop_b;

--test data type of pg_sequence_parameters's input parameters
select pg_get_function_arguments(3078);
