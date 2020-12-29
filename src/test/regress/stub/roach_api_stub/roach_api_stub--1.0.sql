/* contrib/roach_api_stub/roach_api_stub--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION roach_api_stub" to load this file. \quit

CREATE FUNCTION roach_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;
