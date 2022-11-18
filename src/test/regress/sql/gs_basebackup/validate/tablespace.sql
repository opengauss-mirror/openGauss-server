\db gs_basebackup_tablespace
select tablespace from pg_tables where tablename = 'tablespace_table_test';
select * from tablespace_table_test;