/* contrib/gsredistribute/gsredistribute--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gsredistribute" to load this file. \quit

CREATE FUNCTION pg_get_redis_rel_end_ctid(text, text)
RETURNS tid
AS 'MODULE_PATHNAME','pg_get_redis_rel_end_ctid'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_get_redis_rel_start_ctid(text, text)
RETURNS tid
AS 'MODULE_PATHNAME','pg_get_redis_rel_start_ctid'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION pg_enable_redis_proc_cancelable()
RETURNS bool
AS 'MODULE_PATHNAME','pg_enable_redis_proc_cancelable'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_disable_redis_proc_cancelable()
RETURNS bool
AS 'MODULE_PATHNAME','pg_disable_redis_proc_cancelable'
LANGUAGE C IMMUTABLE STRICT;

