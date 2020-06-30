--report error under single-node mode
select getbucket(2, 1);
select getbucket(sysdate, 1);
select getbucket('123'::char, 1);
select get_remote_prepared_xacts();
select global_clean_prepared_xacts(1,2);
select global_comm_get_client_info();
select global_comm_get_recv_stream();
select global_comm_get_send_stream();
select global_comm_get_status();

create table t1 (a int);
create table t2 (a int);
select gs_switch_relfilenode('t1', 't2');

select gs_wlm_get_workload_records(1);
select pg_cancel_invalid_query();
select pg_stat_get_wlm_session_info_internal(null,null,null,null);
select pgxc_gtm_snapshot_status();
select pgxc_snapshot_status();

--cleanup
drop table t1;
drop table t2;