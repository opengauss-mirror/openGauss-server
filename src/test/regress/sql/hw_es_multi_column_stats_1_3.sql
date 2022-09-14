--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== analyze with alter table
alter table t1_3 add statistics ((a, b));
alter table t1_3 add statistics ((b, c));
alter table t1_3 add statistics ((a, d));
alter table t1_3 add statistics ((b, d));
alter table t1_3 add statistics ((b, c, d));

alter table t1r_3 add statistics ((a, b));
alter table t1r_3 add statistics ((b, c));
alter table t1r_3 add statistics ((a, d));
alter table t1r_3 add statistics ((b, d));
alter table t1r_3 add statistics ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_3' order by attname;

analyze t1_3 ((b, d));
analyze t1_3 ((c, d));

analyze t1r_3 ((b, d));
analyze t1r_3 ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_3' order by attname;

analyze t1_3;
analyze t1r_3;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_3' order by attname;

