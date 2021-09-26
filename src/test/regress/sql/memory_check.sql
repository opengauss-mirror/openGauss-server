\c postgres
select * from pg_sleep(10);
select  case  when memorymbytes > 1720 then 'fail' else 'pass' end as result from gs_total_memory_detail where memorytype = 'process_used_memory';