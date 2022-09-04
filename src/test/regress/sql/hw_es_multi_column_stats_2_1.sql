--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== data feature : data with NULL
insert into t2 values (1, 1, 1, 1);
insert into t2 values (NULL, 1, 1, 1);
insert into t2 values (3, NULL, 1, 1);
insert into t2 values (4, NULL, 1, 1);
insert into t2 values (5, 3, NULL, 1);
insert into t2 values (6, 3, NULL, 1);
insert into t2 values (7, 4, NULL, 1);
insert into t2 values (8, 4, NULL, 1);
insert into t2r select * from t2;

analyze t2 ((a, b));
analyze t2 ((b, c));
analyze t2 ((a, d));
analyze t2 ((b, d));
analyze t2 ((c, d));
analyze t2 ((b, c, d));

analyze t2r ((a, b));
analyze t2r ((b, c));
analyze t2r ((a, d));
analyze t2r ((b, d));
analyze t2r ((c, d));
analyze t2r ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2r' order by attname;

drop table t2;
drop table t2r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t2r' order by attname;
