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
drop function if exists sys.procid();

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

-- sql_variant
-- CAST functions from SQL_VARIANT
DROP CAST IF EXISTS (sys.SQL_VARIANT AS SMALLDATETIME);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS DATE);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS TIME);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS FLOAT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS REAL);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS NUMERIC);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS MONEY);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS BIGINT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS INT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS SMALLINT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS TINYINT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS BIT);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS VARCHAR);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS NVARCHAR);
DROP CAST IF EXISTS (sys.SQL_VARIANT AS CHAR);
DROP FUNCTION IF EXISTS sys.sqlvariant_smalldatetime(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_date(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_time(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_float(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_real(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_numeric(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_money(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_bigint(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_int(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_smallint(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_tinyint(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_bit(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_sysvarchar(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_nvarchar(sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.sqlvariant_char(sys.SQL_VARIANT);
-- CAST FUNCTIONS to SQL_VARIANT
DROP CAST IF EXISTS (SMALLDATETIME AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (DATE AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (TIME AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (FLOAT AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (REAL AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (NUMERIC AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (money AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (BIGINT AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (INT AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (smallint AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (tinyint AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (BIT AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (VARCHAR AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (NVARCHAR AS sys.SQL_VARIANT);
DROP CAST IF EXISTS (CHAR AS sys.SQL_VARIANT);
DROP FUNCTION IF EXISTS sys.smalldatetime_sqlvariant(SMALLDATETIME, int);
DROP FUNCTION IF EXISTS sys.date_sqlvariant(DATE, int);
DROP FUNCTION IF EXISTS sys.time_sqlvariant(TIME, int);
DROP FUNCTION IF EXISTS sys.float_sqlvariant(FLOAT, int);
DROP FUNCTION IF EXISTS sys.real_sqlvariant(REAL, int);
DROP FUNCTION IF EXISTS sys.numeric_sqlvariant(NUMERIC, int);
DROP FUNCTION IF EXISTS sys.money_sqlvariant(money, int);
DROP FUNCTION IF EXISTS sys.bigint_sqlvariant(BIGINT, int);
DROP FUNCTION IF EXISTS sys.int_sqlvariant(INT, int);
DROP FUNCTION IF EXISTS sys.smallint_sqlvariant(smallint, int);
DROP FUNCTION IF EXISTS sys.tinyint_sqlvariant(tinyint, int);
DROP FUNCTION IF EXISTS sys.bit_sqlvariant(BIT, int);
DROP FUNCTION IF EXISTS sys.varchar_sqlvariant(varchar, int);
DROP FUNCTION IF EXISTS sys.nvarchar_sqlvariant(nvarchar, int);
DROP FUNCTION IF EXISTS sys.char_sqlvariant(CHAR, int);

DROP OPERATOR CLASS IF EXISTS sys.sqlvariant_ops USING btree;
DROP OPERATOR IF EXISTS sys.>= (sys.SQL_VARIANT,sys.SQL_VARIANT);
DROP OPERATOR IF EXISTS sys.> (sys.SQL_VARIANT,sys.SQL_VARIANT);
DROP OPERATOR IF EXISTS sys.<= (sys.SQL_VARIANT,sys.SQL_VARIANT);
DROP OPERATOR IF EXISTS sys.< (sys.SQL_VARIANT,sys.SQL_VARIANT);
DROP OPERATOR IF EXISTS sys.= (sys.SQL_VARIANT,sys.SQL_VARIANT);
DROP OPERATOR IF EXISTS sys.<> (sys.SQL_VARIANT,sys.SQL_VARIANT);

DROP FUNCTION IF EXISTS sys.sql_variantne(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_variantlt(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_variantle(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_variantgt(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_variantge(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_varianteq(sys.sql_variant, sys.sql_variant);
DROP FUNCTION IF EXISTS sys.sql_variantcmp(sys.sql_variant, sys.sql_variant);

drop type IF EXISTS sys.sql_variant cascade;

drop function if exists sys.databasepropertyex (nvarchar(128), nvarchar(128));
drop function if exists sys.suser_id_internal(IN login nvarchar(256));
drop function if exists sys.suser_id(IN login nvarchar(256));
drop function if exists sys.suser_id();
drop function if exists sys.suser_name_internal(IN server_user_id OID);
drop function if exists sys.suser_name(IN server_user_id OID);
drop function if exists sys.suser_name();
drop function if exists sys.suser_sname(IN server_user_sid varbinary(85));
drop function if exists sys.suser_sname();
drop function if exists sys.get_scope_identity();
drop function if exists sys.scope_identity();
drop function if exists sys.ident_current(IN tablename nvarchar(128));
drop function if exists sys.get_ident_current(IN tablename nvarchar(128));

drop type if exists sys.varbinary cascade;
drop function if exists sys.varbinarytypmodin(cstring[]);
drop function if exists sys.varbinarytypmodout(integer);