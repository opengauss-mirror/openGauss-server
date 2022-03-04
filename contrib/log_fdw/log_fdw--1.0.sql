/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for log file
 *
 * contrib/log_fdw/log_fdw--1.0.sql
 *
 *-------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION log_fdw" to load this file. \quit

CREATE FUNCTION pg_catalog.log_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT  NOT FENCED;

CREATE FUNCTION pg_catalog.log_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER log_fdw
  HANDLER log_fdw_handler
  VALIDATOR log_fdw_validator;

CREATE SERVER log_srv FOREIGN DATA WRAPPER log_fdw;

create or replace function pg_catalog.gs_create_log_tables()
RETURNS void
AS $$
declare
  sql_text varchar;
  log_line_prefix varchar;
  cols_def varchar;
begin
	CREATE FOREIGN TABLE IF NOT EXISTS public.gs_profile_log_ft(
	  dirname  text,
	  filename text,
	  hostname text,
	  logtime  timestamp with time zone,
	  nodename text,
	  thread bigint,
	  xid bigint,
	  qid bigint,
	  reqsrc text,
	  reqtype text,
	  reqok int,
	  reqcount bigint,
	  reqsize  bigint,
	  requsec  bigint
	) server log_srv OPTIONS( logtype 'gs_profile', master_only 'true', latest_files '2') DISTRIBUTE BY ROUNDROBIN;

	CREATE FOREIGN TABLE IF NOT EXISTS public.gs_pg_log_ft(
	  dirname  text,
	  filename text,
	  hostname text,
	  match    bool,
	  logtime  timestamp with time zone,
	  nodename text,
	  app text,
	  -- be from %s
	  session_start timestamp with time zone,
	  -- be from %c
	  session_id text,
	  db text,
	  remote text,
	  cmdtag text,
	  username text,
	  -- be from %v
	  vxid text,
	  pid bigint,
	  lineno bigint,
	  xid bigint,
	  qid bigint,
	  ecode text,
	  mod text,
	  level text,
	  msg text
	) server log_srv OPTIONS( logtype 'pg_log', master_only 'true', latest_files '2')  DISTRIBUTE BY ROUNDROBIN;

	CREATE VIEW public.gs_pg_log_v AS 
            SELECT * FROM public.gs_pg_log_ft order by hostname, dirname, filename, logtime;
	CREATE VIEW public.gs_profile_log_v AS 
            SELECT * FROM public.gs_profile_log_ft order by hostname, dirname, filename, logtime;
end;
$$language plpgsql NOT FENCED;

