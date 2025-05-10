drop function if exists sys.day(timestamptz);
drop function if exists sys.day(abstime);
drop function if exists sys.day(date);
drop function if exists sys.day(timestamp(0) with time zone);

drop function if exists sys.rand();
drop function if exists sys.rand(int);
drop function if exists sys.rand(smallint);
drop function if exists sys.rand(tinyint);

drop function if exists sys.object_id(IN object_name VARCHAR, IN object_type VARCHAR);
drop function if exists sys.objectproperty(id INT, property VARCHAR);

drop function if exists sys.dbcc_check_ident_no_reseed(varchar, boolean, boolean);
drop function if exists sys.dbcc_check_ident_reseed(varchar, int16, boolean);
    
drop function if exists sys.fetch_status();
drop function if exists sys.rowcount();
drop function if exists sys.rowcount_big();
drop function if exists sys.spid();

drop view if exists sys.sysobjects;
drop view if exists sys.syscolumns;
drop view if exists sys.sysindexes;
drop view if exists sys.sysindexkeys;
drop view if exists sys.databases;
drop view if exists sys.sysusers;
drop view if exists sys.schemas;
drop view if exists sys.sysdatabases;
drop view if exists information_schema_tsql.views;
drop view if exists information_schema_tsql.tables;
drop view if exists information_schema_tsql.columns;
drop view if exists sys.columns;
drop view if exists sys.all_columns;
drop view if exists sys.objects;
drop view if exists sys.views;
drop view if exists sys.tables;
drop view if exists sys.all_objects;
drop view if exists sys.procedures;
drop view if exists sys.indexes;
drop function if exists sys.tsql_type_max_length_helper(in type text, in typelen smallint, in typemod int);
drop function if exists sys.tsql_type_precision_helper(in type text, in typemod int);
drop function if exists sys.tsql_type_scale_helper(in type text, in typemod int);
drop function if exists sys.tsql_relation_reloptions_helper(in reloptions text[], in targetKey text);

drop view if exists information_schema_tsql.check_constraints;
drop function if exists information_schema_tsql.is_d_format_schema(nspoid oid, nspname name);
drop function if exists information_schema_tsql._pgtsql_datetime_precision(type text, typmod int4);
drop function if exists information_schema_tsql._pgtsql_numeric_scale(type text, typid oid, typmod int4);
drop function if exists information_schema_tsql._pgtsql_numeric_precision_radix(type text, typid oid, typmod int4);
drop function if exists information_schema_tsql._pgtsql_numeric_precision(type text, typid oid, typmod int4);
drop function if exists information_schema_tsql._pg_char_octet_length(type text, typmod int4);
drop function if exists information_schema_tsql._pg_char_max_length(type text, typmod int4);
drop schema information_schema_tsql;
drop function if exists sys.ts_procedure_object_internal();
drop function if exists sys.ts_index_type_helper(in indexid oid, in reloptions text[]);
drop function if exists sys.ts_numeric_scale_helper(in typname text, in typmod int);
drop function if exists sys.ts_numeric_precision_helper(in typname text, in typmod int);
drop function if exists sys.ts_tables_obj_internal();
drop function if exists sys.ts_graph_type_helper(in relid oid, in typ text);
drop function if exists sys.ts_is_publication_helper(in relid oid);
drop function if exists sys.ts_is_mot_table_helper(in reloid oid);
