/* contrib/hdfs_fdw/hdfs_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hdfs_fdw" to load this file. \quit

CREATE FUNCTION pg_catalog.hdfs_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION pg_catalog.hdfs_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER hdfs_fdw
  HANDLER hdfs_fdw_handler
  VALIDATOR hdfs_fdw_validator;
/* we define the unified FDW named dfs_fdw. The hdfs_fdw will be reserved */
/* in order to compate with older version. */
CREATE FOREIGN DATA WRAPPER dfs_fdw
  HANDLER hdfs_fdw_handler
  VALIDATOR hdfs_fdw_validator;
