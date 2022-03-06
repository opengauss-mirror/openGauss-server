/* contrib/gc_fdw/gc_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gc_fdw" to load this file. \quit

CREATE FUNCTION pg_catalog.gc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION pg_catalog.gc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER gc_fdw
  HANDLER gc_fdw_handler
  VALIDATOR gc_fdw_validator;
