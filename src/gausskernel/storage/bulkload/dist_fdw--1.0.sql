/* contrib/dist_fdw/dist_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dist_fdw" to load this file. \quit

CREATE FUNCTION pg_catalog.dist_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION pg_catalog.dist_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER dist_fdw
  HANDLER dist_fdw_handler
  VALIDATOR dist_fdw_validator;
