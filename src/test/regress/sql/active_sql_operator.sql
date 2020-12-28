select pg_sleep(2);
select (select count(*) from gs_wlm_operator_statistics) > 0;
drop table tx;
