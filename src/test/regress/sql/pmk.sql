----test pv_os_run_info view
select count(*)>=0 from gs_os_run_info; 

----test pv_session_memory_detail view
select count(*)>=0 from gs_session_memory_detail;

----test pg_shared_memory_detail view
select count(*)>=0 from gs_shared_memory_detail;

----test pv_session_time view
select SCHEMANAME,VIEWNAME, DEFINITION from pg_views where viewname ='pv_session_time';

select * from gs_session_time where 1=2;

select count(*) from (select * from gs_session_time limit 1);

----test pv_file_stat view
select SCHEMANAME,VIEWNAME, DEFINITION from pg_views where viewname ='pv_file_stat';

select count(*)>=0 from gs_file_stat; 

----test pv_redo_stat view
select SCHEMANAME,VIEWNAME, DEFINITION from pg_views where viewname ='pv_redo_stat';

----test pv_session_stat view
select SCHEMANAME,VIEWNAME, DEFINITION from pg_views where viewname ='pv_session_stat';

select * from gs_session_stat where 1=2;

select * from gs_session_stat where statid = -1;

select * from gs_session_memory where 1=2;

select count(*)>=0 from gs_session_memory;

select * from gs_total_memory_detail where 1=2;

select count(*) from (select * from gs_total_memory_detail limit 1);

----test total_cpu function
select total_cpu()>0;

----test get_hostname function
select get_hostname()>'0';

----test total_memory function
select total_memory()>0;

----test sessionid2pid function
select sessionid2pid('123.321');

----test pv_session_memctx_detail
select pv_session_memctx_detail(0, '');
select pv_session_memctx_detail(1, '');
create table qazxsw(pid bigint);
insert into qazxsw select pg_backend_pid();
select pv_session_memctx_detail(pid, 'TopMemoryContext') from qazxsw;
