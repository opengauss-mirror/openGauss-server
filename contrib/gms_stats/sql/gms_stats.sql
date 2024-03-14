create extension gms_stats;
create schema gms_stats_test;
set search_path=gms_stats_test;
create table normal_table(a int, b char(10));
insert into normal_table select generate_series(1,500), 'abc';

create table partition_table(a int) partition by range(a) (partition p1 values less than(100),partition p2 values less than(maxvalue));
insert into partition_table select generate_series(1,600);

create materialized view mv_tb as select * from normal_table;

select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

begin
gms_stats.gather_schema_stats('gms_stats_test');
end;
/
select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

create table normal_table2(a int, b char(10));
insert into normal_table2 select generate_series(1,700), 'abc';
call gms_stats.gather_schema_stats('gms_stats_test');

select schemaname, tablename, attname, avg_width, most_common_vals, most_common_freqs from pg_stats where schemaname='gms_stats_test' order by tablename, attname;

drop schema gms_stats_test cascade;
