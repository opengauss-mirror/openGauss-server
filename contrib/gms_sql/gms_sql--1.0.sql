/* contrib/gms_sql/gms_sql--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_sql" to load this file. \quit

-- gms_sql package begin
-- gms_sql schema
CREATE SCHEMA gms_sql;
GRANT USAGE ON SCHEMA gms_sql TO PUBLIC;
CREATE FUNCTION gms_sql.is_open(c int) RETURNS bool AS 'MODULE_PATHNAME', 'gms_sql_is_open' LANGUAGE c STABLE NOT FENCED;
CREATE FUNCTION gms_sql.open_cursor() RETURNS int AS 'MODULE_PATHNAME', 'gms_sql_open_cursor' LANGUAGE c STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.close_cursor(c int) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_close_cursor' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.debug_cursor(c int) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_debug_cursor' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.parse(c int, stmt varchar2, ver int) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_parse' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.bind_variable(c int, name varchar2, value "any") LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_bind_variable' STABLE  NOT FENCED;
CREATE FUNCTION gms_sql.bind_variable_f(c int, name varchar2, value "any") RETURNS void AS 'MODULE_PATHNAME', 'gms_sql_bind_variable_f' LANGUAGE c STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.bind_array(c int, name varchar2, value anyarray) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_bind_array_3' package STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.bind_array(c int, name varchar2, value anyarray, index1 int, index2 int) LANGUAGE c  AS 'MODULE_PATHNAME', 'gms_sql_bind_array_5' package STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.define_column(c int, col int, value "any", column_size int DEFAULT -1) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_define_column' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.define_array(c int, col int, value "anyarray", cnt int, lower_bnd int) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_define_array' STABLE NOT FENCED;
CREATE FUNCTION gms_sql.execute(c int) RETURNS bigint AS 'MODULE_PATHNAME', 'gms_sql_execute' LANGUAGE c STABLE NOT FENCED;
CREATE FUNCTION gms_sql.fetch_rows(c int) RETURNS int AS 'MODULE_PATHNAME', 'gms_sql_fetch_rows' LANGUAGE c STABLE NOT FENCED;
CREATE FUNCTION gms_sql.execute_and_fetch(c int, exact bool DEFAULT false) RETURNS int AS 'MODULE_PATHNAME', 'gms_sql_execute_and_fetch' LANGUAGE c STABLE NOT FENCED;
CREATE FUNCTION gms_sql.last_row_count() RETURNS int AS 'MODULE_PATHNAME', 'gms_sql_last_row_count' LANGUAGE c STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.column_value(c int, pos int, INOUT value anyelement) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_column_value' STABLE NOT FENCED;
CREATE FUNCTION gms_sql.column_value_f(c int, pos int, value anyelement) RETURNS anyelement AS 'MODULE_PATHNAME', 'gms_sql_column_value_f' LANGUAGE c STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.return_result(c refcursor, to_client bool DEFAULT false) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_return_result' PACKAGE STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.return_result(c int, to_client bool DEFAULT false) LANGUAGE c  AS 'MODULE_PATHNAME', 'gms_sql_return_result_i' PACKAGE STABLE NOT FENCED;
CREATE FUNCTION gms_sql.v6() RETURNS int AS $$
BEGIN
return 0;
END;
$$ language plpgsql;

CREATE FUNCTION gms_sql.native() RETURNS int AS $$
BEGIN
return 1;
END;
$$ language plpgsql;

CREATE FUNCTION gms_sql.v7() RETURNS int AS $$
BEGIN
return 2;
END;
$$ language plpgsql;

CREATE TYPE gms_sql.desc_rec AS (
    col_type int,
    col_max_len int,
    col_name varchar2(32),
    col_name_len int,
    col_schema_name text,
    col_schema_name_len int,
    col_precision int,
    col_scale int,
    col_charsetid int,
    col_charsetform int,
    col_null_ok boolean);
CREATE TYPE gms_sql.desc_rec2 AS (
    col_type int,
    col_max_len int,
    col_name text,
    col_name_len int,
    col_schema_name text,
    col_schema_name_len int,
    col_precision int,
    col_scale int,
    col_charsetid int,
    col_charsetform int,
    col_null_ok boolean);
CREATE TYPE gms_sql.desc_rec3 AS (
    col_type int,
    col_max_len int,
    col_name text,
    col_name_len int,
    col_schema_name text,
    col_schema_name_len int,
    col_precision int,
    col_scale int,
    col_charsetid int,
    col_charsetform int,
    col_null_ok boolean,
    col_type_name text,
    col_type_name_len int);
CREATE TYPE gms_sql.desc_rec4 AS (
    col_type int,
    col_max_len int,
    col_name text,
    col_name_len int,
    col_schema_name text,
    col_schema_name_len int,
    col_precision int,
    col_scale int,
    col_charsetid int,
    col_charsetform int,
    col_null_ok boolean,
    col_type_name text,
    col_type_name_len int);
    

CREATE TYPE gms_sql.desc_tab IS TABLE OF gms_sql.desc_rec;
CREATE TYPE gms_sql.desc_tab2 IS TABLE OF gms_sql.desc_rec2;
CREATE TYPE gms_sql.desc_tab3 IS TABLE OF gms_sql.desc_rec3;
CREATE TYPE gms_sql.desc_tab4 IS TABLE OF gms_sql.desc_rec4;
CREATE TYPE gms_sql.number_table IS TABLE OF number;
CREATE TYPE gms_sql.varchar2_table IS TABLE OF varchar2;
CREATE TYPE gms_sql.date_table IS TABLE OF date;
CREATE TYPE gms_sql.blob_table IS TABLE OF blob;
CREATE TYPE gms_sql.clob_table IS TABLE OF clob;
CREATE TYPE gms_sql.binary_double_table IS TABLE OF number;

CREATE FUNCTION gms_sql.describe_columns_f(c int, OUT col_cnt int, OUT desc_t gms_sql.desc_rec3[]) AS 'MODULE_PATHNAME', 'gms_sql_describe_columns_f' LANGUAGE c STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.describe_columns(c int, INOUT col_cnt int, INOUT desc_t gms_sql.desc_rec[]) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_describe_columns_f' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.describe_columns2(c int, INOUT col_cnt int, INOUT desc_t gms_sql.desc_rec2[]) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_describe_columns_f' STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.describe_columns3(c int, INOUT col_cnt int, INOUT desc_t gms_sql.desc_rec3[]) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_describe_columns_f' PACKAGE STABLE NOT FENCED;
CREATE PROCEDURE gms_sql.describe_columns3(c int, INOUT col_cnt int, INOUT desc_t gms_sql.desc_rec4[]) LANGUAGE c AS 'MODULE_PATHNAME', 'gms_sql_describe_columns_f' PACKAGE STABLE NOT FENCED;
-- gms_sql package end
