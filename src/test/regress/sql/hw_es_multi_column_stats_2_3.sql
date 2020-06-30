--==========================================================
--==========================================================
\set ECHO all
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== table type : partition table
insert into t5 values (1, 1, 1, 1);
insert into t5 values (2, 1, 1, 1);
insert into t5 values (3, 2, 1, 1);
insert into t5 values (4, 2, 1, 1);
insert into t5 values (5, 3, 2, 1);
insert into t5 values (6, 3, 2, 1);
insert into t5 values (7, 4, 2, 1);
insert into t5 values (8, 4, 2, 1);
insert into t5r select * from t5;

analyze t5 ((a, c));
analyze t5 ((b, c));
analyze t5r ((a, c));
analyze t5r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

alter table t5 add statistics ((a, b));
alter table t5 add statistics ((b, c));
alter table t5 add statistics ((a, d));
alter table t5 add statistics ((b, d));
alter table t5 add statistics ((b, c, d));
alter table t5 add statistics ((a, b, c, d));

alter table t5r add statistics ((a, b));
alter table t5r add statistics ((b, c));
alter table t5r add statistics ((a, d));
alter table t5r add statistics ((b, d));
alter table t5r add statistics ((b, c, d));
alter table t5r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

analyze t5 ((b, d));
analyze t5 ((c, d));
analyze t5r ((b, d));
analyze t5r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

analyze t5;
analyze t5r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;

drop table t5;
drop table t5r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t5r' order by attname;
