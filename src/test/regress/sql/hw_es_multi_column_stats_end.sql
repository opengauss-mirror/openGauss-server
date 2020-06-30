drop table if exists test_range_gist;
create table test_range_gist(ir int4range) ;
set default_statistics_target=100;
analyze test_range_gist;
set default_statistics_target=-2;
analyze test_range_gist;
drop table test_range_gist;

drop schema hw_es_multi_column_stats cascade;
reset default_statistics_target;
