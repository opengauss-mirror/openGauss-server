-- sys.configurations
\d sys.configurations

select name, value, minimum, maximum, value_in_use, description from sys.configurations
  where name in ('acceleration_with_compute_pool', 'acce_min_datasize_per_thread')
  order by case name when 'acceleration_with_compute_pool' then 1 when 'acce_min_datasize_per_thread' then 2 end;
select name, setting, min_val, max_val, setting, short_desc from pg_settings
  where name in ('acceleration_with_compute_pool', 'acce_min_datasize_per_thread')
  order by case name when 'acceleration_with_compute_pool' then 1 when 'acce_min_datasize_per_thread' then 2 end;

select distinct is_advanced from sys.configurations;
select distinct configuration_id from sys.configurations;

select name, context from pg_settings where name in ('block_size', 'advance_xlog_file_num',
  'acce_min_datasize_per_thread', 'ignore_system_indexes', 'alarm_report_interval', 'autoanalyze');

select name, is_dynamic from sys.configurations where name in ('block_size', 'advance_xlog_file_num');
select name, is_dynamic from sys.configurations where name in ('acce_min_datasize_per_thread',
  'ignore_system_indexes', 'alarm_report_interval', 'autoanalyze');

-- sys.syscurconfigs
\d sys.syscurconfigs

select value, comment from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('acce_min_datasize_per_thread', 'acceleration_with_compute_pool')
  order by case ps.name when 'acce_min_datasize_per_thread' then 1 when 'acceleration_with_compute_pool' then 2 end;
select setting, short_desc from pg_settings
  where name in ('acce_min_datasize_per_thread', 'acceleration_with_compute_pool')
  order by case name when 'acce_min_datasize_per_thread' then 1 when 'acceleration_with_compute_pool' then 2 end;

select distinct config from sys.syscurconfigs;
select status from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('block_size', 'advance_xlog_file_num');
select status from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('acce_min_datasize_per_thread', 'ignore_system_indexes', 'alarm_report_interval',
  'autoanalyze');

-- sys.sysconfigures
\d sys.sysconfigures

select value, comment from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('acce_min_datasize_per_thread', 'acceleration_with_compute_pool')
  order by case ps.name when 'acce_min_datasize_per_thread' then 1 when 'acceleration_with_compute_pool' then 2 end;
select setting, short_desc from pg_settings
  where name in ('acce_min_datasize_per_thread', 'acceleration_with_compute_pool')
  order by case name when 'acce_min_datasize_per_thread' then 1 when 'acceleration_with_compute_pool' then 2 end;

select distinct config from sys.syscurconfigs;
select status from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('block_size', 'advance_xlog_file_num');
select status from sys.syscurconfigs con inner join pg_settings ps on con.comment = ps.short_desc
  where ps.name in ('acce_min_datasize_per_thread', 'ignore_system_indexes', 'alarm_report_interval',
  'autoanalyze');
