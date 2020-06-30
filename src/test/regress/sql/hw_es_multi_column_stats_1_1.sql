--==========================================================
--==========================================================
\set ECHO all
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== analyze t ((a, b))
select relname, relpages > 0, reltuples > 0 from pg_class where relname = 't1_1';

analyze t1_1 ((a, b));
analyze t1_1 ((b, c));
analyze t1_1 ((a, d));
analyze t1_1 ((b, d));
analyze t1_1 ((c, d));
analyze t1_1 ((b, c, d));
analyze t1_1 ((a, b, c, d));

select relname, relpages > 0, reltuples > 0 from pg_class where relname = 't1_1';

analyze t1r_1 ((a, b));
analyze t1r_1 ((b, c));
analyze t1r_1 ((a, d));
analyze t1r_1 ((b, d));
analyze t1r_1 ((c, d));
analyze t1r_1 ((b, c, d));
analyze t1r_1 ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_1' order by attname;

