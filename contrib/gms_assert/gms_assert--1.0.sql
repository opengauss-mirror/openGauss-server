/* contrib/gms_assert/gms_assert--1.0.sql */

\echo Use "CREATE EXTENSION gms_assert" to load this file. \quit

CREATE SCHEMA gms_assert;
GRANT USAGE ON SCHEMA gms_assert TO PUBLIC;

CREATE OR REPLACE FUNCTION gms_assert.noop(strIn text)
RETURNS text STRICT AS 'MODULE_PATHNAME','noop'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.enquote_literal(strIn text)
RETURNS text AS 'MODULE_PATHNAME', 'enquote_literal'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.simple_sql_name(strIn text)
RETURNS text AS 'MODULE_PATHNAME', 'simple_sql_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.enquote_name(strIn text, capitalize bool DEFAULT true)
RETURNS text AS 'MODULE_PATHNAME', 'enquote_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.qualified_sql_name(strIn text)
RETURNS text AS 'MODULE_PATHNAME', 'qualified_sql_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.schema_name(strIn text)
RETURNS text AS 'MODULE_PATHNAME', 'schema_name'
LANGUAGE C;

CREATE OR REPLACE FUNCTION gms_assert.sql_object_name(text)
RETURNS text AS 'MODULE_PATHNAME', 'sql_object_name'
LANGUAGE C;
