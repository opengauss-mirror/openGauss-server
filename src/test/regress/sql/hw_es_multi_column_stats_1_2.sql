--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== alter table
analyze t1_2;
analyze t1r_2;

alter table t1_2 add statistics ((a, b));
alter table t1_2 add statistics ((b, c));
alter table t1_2 add statistics ((a, d));
alter table t1_2 add statistics ((b, d));
alter table t1_2 add statistics ((c, d));
alter table t1_2 add statistics ((b, c, d));

alter table t1r_2 add statistics ((a, b));
alter table t1r_2 add statistics ((b, c));
alter table t1r_2 add statistics ((a, d));
alter table t1r_2 add statistics ((b, d));
alter table t1r_2 add statistics ((c, d));
alter table t1r_2 add statistics ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_2' order by attname;

analyze t1_2;
analyze t1r_2;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_2' order by attname;
