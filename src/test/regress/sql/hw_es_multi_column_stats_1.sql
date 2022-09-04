--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== empty table : analyze
analyze t1 ((a, c));
analyze t1 ((b, c));

analyze t1r ((a, c));
analyze t1r ((b, c));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 add statistics ((a, b));
alter table t1 add statistics ((b, c));
alter table t1 add statistics ((a, d));
alter table t1 add statistics ((b, d));
alter table t1 add statistics ((b, c, d));
alter table t1 add statistics ((a, b, c, d));

alter table t1r add statistics ((a, b));
alter table t1r add statistics ((b, c));
alter table t1r add statistics ((a, d));
alter table t1r add statistics ((b, d));
alter table t1r add statistics ((b, c, d));
alter table t1r add statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1 ((b, d));
analyze t1 ((c, d));

analyze t1r ((b, d));
analyze t1r ((c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

analyze t1;
analyze t1r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

alter table t1 delete statistics ((a, c));
alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((c, d));
alter table t1 delete statistics ((b, c, d));
alter table t1 delete statistics ((a, b, c, d));

alter table t1r delete statistics ((a, c));
alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((c, d));
alter table t1r delete statistics ((b, c, d));
alter table t1r delete statistics ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t1r' order by attname;

-- Type point is unanalyzable
create table p_point(a int,b point);
insert into p_point values(1,point(1,1));
set default_statistics_target=-2;
analyze p_point((a,b));
alter table p_point add statistics ((a, b));
analyze p_point;
drop table p_point;
