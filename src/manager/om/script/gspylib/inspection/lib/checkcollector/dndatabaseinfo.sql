select sysdate, * from pv_total_memory_detail;
select sysdate, * from pv_session_memory_detail order by totalsize desc limit 20;
select sysdate,*  from pg_shared_memory_detail;
select * from pg_stat_activity where state='active';