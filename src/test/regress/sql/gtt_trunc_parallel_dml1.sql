begin;
insert into t_gtt_trunc_dml_f values(1, 1);
insert into t_gtt_trunc_dml_f values(2, 2);
insert into t_gtt_trunc_dml values(1, 1, '0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789');
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;
update t_gtt_trunc_dml set cc=cc||cc where aa=1;

insert into t_gtt_trunc_dml values(2,2,'2');
delete from t_gtt_trunc_dml where aa=2;
select aa,bb from t_gtt_trunc_dml;
select aa,bb from t_gtt_trunc_dml_f;

SELECT pg_sleep(5);

end;

insert into t_gtt_trunc_dml_result values('gtt_trunc_parallel_dml1');