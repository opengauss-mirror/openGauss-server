--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== column orders
analyze t1_4 ((b, a));
analyze t1_4 ((c, b));
analyze t1_4 ((d, a));
analyze t1_4 ((d, b));
analyze t1_4 ((d, c));
analyze t1_4 ((c, b, d));

analyze t1r_4 ((b, a));
analyze t1r_4 ((c, b));
analyze t1r_4 ((d, a));
analyze t1r_4 ((d, b));
analyze t1r_4 ((d, c));
analyze t1r_4 ((c, b, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_4' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r_4' order by attname;

