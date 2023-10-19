-- workload_transaction
\d+ DBE_PERF.workload_transaction 
\d+ DBE_PERF.global_workload_transaction
\d+ DBE_PERF.user_transaction
\d+ DBE_PERF.global_user_transaction

-- workload transaction
create table temp_info_before as select * from dbe_perf.workload_transaction ;

begin;
select 1;
commit;

begin;
select 1;
rollback;

create table temp_info_after as select * from dbe_perf.workload_transaction ;

select a.commit_counter - b.commit_counter > 0 as commit, a.rollback_counter - b.rollback_counter > 0 as rollback from temp_info_before b join temp_info_after a on b.workload = a.workload;

select workload, commit_counter >=0, rollback_counter >=0, resp_min >=0, resp_max >=0, resp_avg >=0, resp_total >=0
from dbe_perf.workload_transaction  order by workload;

select node_name, workload, commit_counter >=0, rollback_counter >=0, resp_min >= 0, resp_max >= 0, resp_avg >= 0, resp_total >= 0
from DBE_PERF.global_workload_transaction order by node_name, workload;

drop table temp_info_before;
drop table temp_info_after;

-- user transaction
create user workload_u1 password 'qwe123!@#';
create user workload_u2 password 'qwe123!@#';
create user workload_u3 password 'qwe123!@#';
select usename from DBE_PERF.user_transaction where usename like 'workload_u%' order by usename;
select node_name, usename from DBE_PERF.global_user_transaction where usename like 'workload_u%' order by node_name, usename;
drop user workload_u1;
drop user workload_u2;
drop user workload_u3;

select usename from DBE_PERF.user_transaction where usename like 'workload_u%'  order by usename;
select node_name, usename from DBE_PERF.global_user_transaction where usename like 'workload_u%' order by node_name, usename;

-- workload sql
show track_sql_count;

SET track_sql_count=on;
start transaction;
create table t1_workload_sql(id int, num int) distribute by hash(id);
insert into t1_workload_sql values(1,1);
update t1_workload_sql set num = 2 where id = 1;
select * from t1_workload_sql;
delete from t1_workload_sql;
drop table t1_workload_sql;
commit;

select workload, ddl_count>=2, dml_count>=4, dcl_count>=2 from DBE_PERF.workload_sql_count;
select workload, ddl_count>=2, dml_count>=4, dcl_count>=2 from DBE_PERF.summary_workload_sql_count where node_name='coordinator1';

select workload, total_select_elapse>0, total_update_elapse>0, total_insert_elapse>0, total_delete_elapse>0 from DBE_PERF.workload_sql_elapse_time;
select workload, max_select_elapse>0, max_update_elapse>0, max_insert_elapse>0, max_delete_elapse>0 from DBE_PERF.workload_sql_elapse_time;
select workload, min_select_elapse>=0, min_update_elapse>=0, min_insert_elapse>=0, min_delete_elapse>=0 from DBE_PERF.workload_sql_elapse_time;
select workload, avg_select_elapse>0, avg_update_elapse>0, avg_insert_elapse>0, avg_delete_elapse>0 from DBE_PERF.workload_sql_elapse_time;
select workload, total_select_elapse>0, total_update_elapse>0, total_insert_elapse>0, total_delete_elapse>0 from DBE_PERF.summary_workload_sql_elapse_time where node_name='coordinator1';
select workload, max_select_elapse>0, max_update_elapse>0, max_insert_elapse>0, max_delete_elapse>0 from DBE_PERF.summary_workload_sql_elapse_time where node_name='coordinator1';
select workload, min_select_elapse>=0, min_update_elapse>=0, min_insert_elapse>=0, min_delete_elapse>=0 from DBE_PERF.summary_workload_sql_elapse_time where node_name='coordinator1';
select workload, avg_select_elapse>0, avg_update_elapse>0, avg_insert_elapse>0, avg_delete_elapse>0 from DBE_PERF.summary_workload_sql_elapse_time where node_name='coordinator1';
