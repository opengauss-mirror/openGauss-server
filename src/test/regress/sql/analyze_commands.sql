set enable_ai_stats=0;
create schema analyze_commands;
set search_path to analyze_commands;

drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(generate_series(1,10),generate_series(1,2),generate_series(1,2));
set default_statistics_target=100;
analyze t1;
analyze t1((b,c));
select * from pg_stats where tablename = 't1' order by attname;
select * from pg_catalog.pg_ext_stats;

drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(generate_series(1,10),generate_series(1,2),generate_series(1,2));
set default_statistics_target=100;
analyze;
select * from pg_stats where tablename = 't1' order by attname;
select * from pg_catalog.pg_ext_stats;

drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(generate_series(1,10),generate_series(1,2),generate_series(1,2));
set default_statistics_target=-2;
analyze t1;
analyze t1((b,c));
select * from pg_stats where tablename = 't1' order by attname;
select * from pg_catalog.pg_ext_stats;

insert into t1 values(generate_series(1,10),generate_series(3,4),generate_series(3,4));
analyze;
select * from pg_stats where tablename = 't1' order by attname;
select * from pg_catalog.pg_ext_stats;

reset search_path;
drop schema analyze_commands cascade;

