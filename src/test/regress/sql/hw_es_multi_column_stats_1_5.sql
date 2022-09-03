--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== system column and system table

analyze t1_5 ((xmax, xmin));
analyze t1r_5 ((xmax, xmin));

alter table t1_5 add statistics ((xmax, xmin));
alter table t1r_5 add statistics ((xmax, xmin));

alter table t1_5 delete statistics ((xmax, xmin));
alter table t1r_5 delete statistics ((xmax, xmin));

analyze pg_class ((relname, relnamespace));
analyze pg_class (abc);
analyze pg_class ((relname, abc));

alter table pg_class add statistics ((relname, relnamespace));
alter table pg_class add statistics (abc);
alter table pg_class add statistics ((relname, abc));

alter table pg_class delete statistics ((relname, relnamespace));
alter table pg_class delete statistics (abc);
alter table pg_class delete statistics ((relname, abc));

--========================================================== syntax error
analyze t1_5 (());
analyze t1_5 ((b));
analyze t1_5 ((a, a));
analyze t1_5 ((c, c, d));
analyze t1_5 ((b, d, b));
analyze t1_5 ((a, b), (b, c));
analyze t1_5 (a, (b, c));
analyze t1_5 ((b, c), a);
analyze t1_5 ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_5' order by attname;

alter table t1_5 add statistics (());
alter table t1_5 add statistics ((b));
alter table t1_5 add statistics ((a, a));
alter table t1_5 add statistics ((c, c, d));
alter table t1_5 add statistics ((b, d, b));
alter table t1_5 add statistics ((a, b), (b, c));
alter table t1_5 add statistics (a, (b, c));
alter table t1_5 add statistics ((b, c), a);
alter table t1_5 add statistics ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_5' order by attname;

alter table t1_5 delete statistics (());
alter table t1_5 delete statistics ((b));
alter table t1_5 delete statistics ((a, a));
alter table t1_5 delete statistics ((c, c, d));
alter table t1_5 delete statistics ((b, d, b));
alter table t1_5 delete statistics ((a, b), (b, c));
alter table t1_5 delete statistics (a, (b, c));
alter table t1_5 delete statistics ((b, c), a);
alter table t1_5 delete statistics ((c, e));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1_5' order by attname;

