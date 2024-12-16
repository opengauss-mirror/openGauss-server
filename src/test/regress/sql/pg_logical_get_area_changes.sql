----select sql
select * from pg_logical_get_area_changes('', '', NULL, 'sql_decoding', '+log/pg_xlog/000000010000000000000001') limit 5;
