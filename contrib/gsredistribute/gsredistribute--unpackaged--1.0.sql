/* contrib/gsredistribute/gsredistribute--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gsredistribute" to load this file. \quit

ALTER EXTENSION gsredistribute ADD function pg_get_redis_rel_end_ctid(text,text,int,int);
ALTER EXTENSION gsredistribute ADD function pg_get_redis_rel_start_ctid(text,text,int,int);
ALTER EXTENSION gsredistribute ADD function pg_enable_redis_proc_cancelable();
ALTER EXTENSION gsredistribute ADD function pg_disable_redis_proc_cancelable();
