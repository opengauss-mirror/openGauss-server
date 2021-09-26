-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mot_fdw" to load this file. \quit

CREATE FUNCTION mot_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION mot_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER mot_fdw
  HANDLER mot_fdw_handler
  VALIDATOR mot_fdw_validator;
