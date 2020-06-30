--==========================================================
--==========================================================
\set ECHO all
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== table with index
analyze t6 ((a, c));
analyze t6 ((b, c));

analyze t6r ((a, c));
analyze t6r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

alter table t6 add statistics ((a, b));
alter table t6 add statistics ((b, c));
alter table t6 add statistics ((a, d));
alter table t6 add statistics ((b, d));
alter table t6 add statistics ((b, c, d));
alter table t6 add statistics ((a, b, c, d));

alter table t6r add statistics ((a, b));
alter table t6r add statistics ((b, c));
alter table t6r add statistics ((a, d));
alter table t6r add statistics ((b, d));
alter table t6r add statistics ((b, c, d));
alter table t6r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

analyze t6 ((b, d));
analyze t6 ((c, d));

analyze t6r ((b, d));
analyze t6r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

analyze t6;
analyze t6r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;

drop table t6 cascade;
drop table t6r cascade;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t6r' order by attname;
