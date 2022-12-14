CREATE SERVER alter_foreign_schema_test FOREIGN DATA WRAPPER file_fdw;
CREATE FOREIGN TABLE file_f_table (id int) SERVER alter_foreign_schema_test OPTIONs (filename 'test');
select nspname from pg_class a left join pg_namespace b on b.oid = a.relnamespace  where relname = 'file_f_table';
create schema alter_foreign_schema_test_schema;
alter foreign table file_f_table set schema alter_foreign_schema_test_schema;
select nspname from pg_class a left join pg_namespace b on b.oid = a.relnamespace  where relname = 'file_f_table';
select * from file_f_table;
select * from alter_foreign_schema_test_schema.file_f_table;
