create database hw_es_multi_column_stats_end;
\c hw_es_multi_column_stats_end;
create schema hw_es_multi_column_stats;
set current_schema = hw_es_multi_column_stats;
set enable_ai_stats=0;
drop table if exists test_range_gist;
create table test_range_gist(ir int4range) ;
set default_statistics_target=100;
analyze test_range_gist;
set default_statistics_target=-2;
analyze test_range_gist;
drop table test_range_gist;

drop schema hw_es_multi_column_stats cascade;
reset default_statistics_target;
\c regression
drop database hw_es_multi_column_stats_end;
