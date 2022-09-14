begin;
insert into t_gtt_trunc_ddl values(1, 1, 1);

SELECT pg_sleep(5);

end;

insert into t_gtt_trunc_ddl_result values('gtt_trunc_parallel_ddl1');