--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== data feature : data with string
insert into t3 values (1, 'b1b1', 'c1c1c1c1c1c1', 'd');
insert into t3 values (2, 'b1b1', 'c1c1c1c1c1c1', 'd');
insert into t3 values (3, 'b2b2b2', 'c1c1c1c1c1c1', 'd');
insert into t3 values (4, 'b2b2b2', 'c1c1c1c1c1c1', 'd');
insert into t3 values (5, 'b3b3b3b3b3', 'c2c2c2c2c2c2', 'd');
insert into t3 values (6, 'b3b3b3b3b3', 'c2c2c2c2c2c2', 'd');
insert into t3 values (7, 'b4b4b4b4b4b4', 'c2c2c2c2c2c2', 'd');
insert into t3 values (8, 'b4b4b4b4b4b4', 'c2c2c2c2c2c2', 'd');
insert into t3r select * from t3;

analyze t3 ((a, b));
analyze t3 ((b, c));
analyze t3 ((a, d));
analyze t3 ((b, d));
analyze t3 ((c, d));
analyze t3 ((b, c, d));

analyze t3r ((a, b));
analyze t3r ((b, c));
analyze t3r ((a, d));
analyze t3r ((b, d));
analyze t3r ((c, d));
analyze t3r ((b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3r' order by attname;

drop table t3;
drop table t3r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t3r' order by attname;
