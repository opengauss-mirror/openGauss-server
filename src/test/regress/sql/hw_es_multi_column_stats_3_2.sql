--==========================================================
--==========================================================
\set ECHO all
set enable_ai_stats=0;
set current_schema = hw_es_multi_column_stats;
set default_statistics_target=-2;

--========================================================== drop column, modify column
analyze t7;
analyze t7r;

--analyze t7 ((a, b));
--analyze t7 ((b, c));
--analyze t7 ((a, d));
analyze t7 ((b, d));
--analyze t7 ((c, d));
--analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

--analyze t7r ((a, b));
--analyze t7r ((b, c));
--analyze t7r ((a, d));
analyze t7r ((b, d));
--analyze t7r ((c, d));
--analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

-- drop column
alter table t7 drop column b;
alter table t7r drop column b;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

alter table t7 add column b int;
alter table t7r add column b int;

update t7 set b = 5 where a = 1;
update t7 set b = 5 where a = 2;
update t7 set b = 6 where a = 3;
update t7 set b = 6 where a = 4;
update t7 set b = 7 where a = 5;
update t7 set b = 7 where a = 6;
update t7 set b = 8 where a = 7;
update t7 set b = 8 where a = 8;

update t7r set b = 5 where a = 1;
update t7r set b = 5 where a = 2;
update t7r set b = 6 where a = 3;
update t7r set b = 6 where a = 4;
update t7r set b = 7 where a = 5;
update t7r set b = 7 where a = 6;
update t7r set b = 8 where a = 7;
update t7r set b = 8 where a = 8;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

--analyze t7 ((a, b));
--analyze t7 ((b, c));
--analyze t7 ((a, d));
analyze t7 ((b, d));
--analyze t7 ((c, d));
--analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

--analyze t7r ((a, b));
--analyze t7r ((b, c));
--analyze t7r ((a, d));
analyze t7r ((b, d));
--analyze t7r ((c, d));
--analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

-- modify type
alter table t7 modify d int2;
alter table t7r modify d text;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

--analyze t7 ((a, b));
--analyze t7 ((b, c));
--analyze t7 ((a, d));
analyze t7 ((b, d));
--analyze t7 ((c, d));
--analyze t7 ((b, c, d));
analyze t7 ((a, b, c, d));

--analyze t7r ((a, b));
--analyze t7r ((b, c));
--analyze t7r ((a, d));
analyze t7r ((b, d));
--analyze t7r ((c, d));
--analyze t7r ((b, c, d));
analyze t7r ((a, b, c, d));

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;

drop table t7;
drop table t7r;

select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7' order by attname;
select * from pg_ext_stats where schemaname='hw_es_multi_column_stats' and tablename='t7r' order by attname;
