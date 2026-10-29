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

create table p_1(a int, b int) with (storage_type=astore) partition by range(a) 
(
    partition p_1_1 values less than(100), 
    partition p_1_2 values less than(200), 
    partition p_1_3 values less than(300), 
    partition p_1_4 values less than(400), 
    partition p_1_5 values less than(maxvalue)
);

alter table p_1 set(autovacuum_enabled=off);
alter table p_1 set(fillfactor=70);

insert into p_1 values(generate_series(1, 500),generate_series(1, 100));
update p_1 set a=a+1;
update p_1 set a=a+1;
update p_1 set a=a+1;
delete from p_1 where a < 200;
analyze p_1;
select relname, relpages, reltuples from pg_partition where parentid='p_1'::regclass;

drop table p_1;
