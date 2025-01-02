/* contrib/gms_debug/gms_debug--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_debug" to load this file. \quit

-- gms_debug package begin
set behavior_compat_options='proc_outparam_override';
CREATE SCHEMA gms_debug;
GRANT USAGE ON SCHEMA gms_debug TO PUBLIC;

CREATE TYPE gms_debug.program_info AS (
    namespace oid,
    name varchar2(30),
    owner varchar2(30),
    dblink varchar2(30),
    line#  binary_integer,
    libunittype binary_integer,
    entrypointname varchar2(30));

CREATE TYPE gms_debug.runtime_info AS (
    line#            binary_integer,
    terminated       binary_integer,
    breakpoint       binary_integer,
    stackdepth       binary_integer,
    interpreterdepth binary_integer,
    reason           binary_integer, 
    program          gms_debug.program_info);

CREATE or REPLACE FUNCTION gms_debug.initialize(
IN debug_session_id varchar2(30) DEFAULT '' ,
IN diagnostics binary_integer DEFAULT 0)
returns varchar2
AS 'MODULE_PATHNAME', 'gms_debug_initialize'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.debug_on(
IN no_client_side_plsql_engine BOOLEAN  DEFAULT TRUE ,
IN immediate BOOLEAN DEFAULT FALSE)
returns void AS $$
BEGIN
    return;
END;
$$ language plpgsql IMMUTABLE;

CREATE or REPLACE FUNCTION gms_debug.attach_session(
IN debug_session_id varchar2(30) ,
IN diagnostics binary_integer DEFAULT 0)
returns void
AS 'MODULE_PATHNAME', 'gms_debug_attach_session'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.add_breakpoint(
IN proid oid,
IN line# binary_integer ,
OUT breakpoint# binary_integer,
IN fuzzy binary_integer,
IN iterations binary_integer,
OUT sts binary_integer)
AS 'MODULE_PATHNAME', 'gms_debug_set_breakpoint'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.set_breakpoint(
IN program gms_debug.program_info,
IN line# binary_integer,
OUT breakpoint# binary_integer,
IN fuzzy binary_integer,
IN iterations binary_integer)
returns binary_integer AS $$
DECLARE
proid oid;
sts binary_integer;
BEGIN
     select oid from pg_proc where proname = program.name limit 1 into proid;
     set behavior_compat_options='';
     gms_debug.add_breakpoint(proid, line#, breakpoint#, fuzzy, iterations, sts);
     set behavior_compat_options='proc_outparam_override';
     return sts;
END;
$$ language plpgsql;

CREATE or REPLACE FUNCTION gms_debug.call_continue(
IN  breakflags           binary_integer ,
IN  info_requested       binary_integer DEFAULT NULL,
OUT err_code             binary_integer,
OUT run_line             binary_integer,
OUT run_breakpoint       binary_integer,
OUT run_stackdepth       binary_integer,
OUT run_reason           binary_integer, 
OUT pro_namespace        oid,
OUT pro_name             varchar2(30),
OUT pro_owner            oid)
AS 'MODULE_PATHNAME', 'gms_debug_continue'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.continue(
OUT run_info gms_debug.runtime_info ,
IN breakflags binary_integer ,
IN info_requested binary_integer DEFAULT NULL) 
returns binary_integer AS $$
DECLARE
    err_code             binary_integer;
    run_line             binary_integer;
    run_terminated       binary_integer;
    run_breakpoint       binary_integer;
    run_stackdepth       binary_integer;
    run_interpreterdepth binary_integer;
    run_reason           binary_integer; 
    pro_namespace oid;
    pro_name varchar2(30);
    pro_owner oid;
    pro_ownername varchar2(30);
    pro  gms_debug.program_info;
BEGIN
     set behavior_compat_options='';
     gms_debug.call_continue(breakflags, info_requested, err_code, run_line,
     run_breakpoint, run_stackdepth, run_reason, pro_namespace,
     pro_name, pro_owner);
     set behavior_compat_options='proc_outparam_override';
     select usename from pg_user where usesysid = pro_owner into pro_ownername;
     run_info.line# = run_line;
     run_info.terminated = 0;
     run_info.breakpoint = run_breakpoint;
     run_info.stackdepth = run_stackdepth;
     run_info.interpreterdepth = -1;
     run_info.reason = run_reason;
     pro.namespace = pro_namespace;
     pro.name = pro_name;
     pro.owner = pro_ownername;
     pro.dblink = '';
     pro.line# = run_line;
     pro.libunittype = 0;
     pro.entrypointname = '';
     run_info.program = pro;
     return err_code;
END;
$$ language plpgsql;

CREATE or REPLACE FUNCTION gms_debug.get_debug_runtime_info(
IN info_requested        binary_integer,
OUT err_code             binary_integer,
OUT run_line             binary_integer,
OUT run_breakpoint       binary_integer,
OUT run_stackdepth       binary_integer,
OUT run_reason           binary_integer, 
OUT pro_namespace        oid,
OUT pro_name             varchar2(30),
OUT pro_owner            oid)
AS 'MODULE_PATHNAME', 'gms_debug_get_runtime_info'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.get_runtime_info(
IN info_requested binary_integer,
OUT run_info gms_debug.runtime_info) 
returns binary_integer AS $$
DECLARE
    err_code             binary_integer;
    run_line             binary_integer;
    run_terminated       binary_integer;
    run_breakpoint       binary_integer;
    run_stackdepth       binary_integer;
    run_interpreterdepth binary_integer;
    run_reason           binary_integer; 
    pro_namespace oid;
    pro_name varchar2(30);
    pro_owner oid;
    pro_ownername varchar2(30);
    pro  gms_debug.program_info;
BEGIN
    set behavior_compat_options='';
    gms_debug.get_debug_runtime_info(info_requested, err_code, run_line,
    run_breakpoint, run_stackdepth, run_reason, pro_namespace,
    pro_name, pro_owner);
    set behavior_compat_options='proc_outparam_override';
    select usename from pg_user where usesysid = pro_owner into pro_ownername;
     run_info.line# = run_line;
     run_info.terminated = 0;
     run_info.breakpoint = run_breakpoint;
     run_info.stackdepth = run_stackdepth;
     run_info.interpreterdepth = -1;
     run_info.reason = run_reason;
     pro.namespace = pro_namespace;
     pro.name = pro_name;
     pro.owner = pro_ownername;
     pro.dblink = '';
     pro.line# = run_line;
     pro.libunittype = 0;
     pro.entrypointname = '';
     run_info.program = pro;
     return err_code;
END;
$$ language plpgsql;

CREATE or REPLACE FUNCTION gms_debug.debug_off()
returns void 
AS 'MODULE_PATHNAME', 'gms_debug_off'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_debug.detach_session()
returns void
AS 'MODULE_PATHNAME', 'gms_debug_detach_session'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE PROCEDURE gms_debug.probe_version(OUT major binary_integer, OUT minor binary_integer)
AS
BEGIN
    major := 1;
    minor := 0;
END;

CREATE FUNCTION gms_debug.success() RETURNS int AS $$
BEGIN
return 0;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.error_illegal_line() RETURNS int AS $$
BEGIN
return -1;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.error_bad_handle() RETURNS int AS $$
BEGIN
return -2;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.error_already_exists() RETURNS int AS $$
BEGIN
return -3;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.error_communication() RETURNS int AS $$
BEGIN
return -4;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.info_getstackdepth() RETURNS int AS $$
BEGIN
return 0;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.info_getbreakpoint() RETURNS int AS $$
BEGIN
return 2;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.info_getlineinfo() RETURNS int AS $$
BEGIN
return 4;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.break_next_line() RETURNS int AS $$
BEGIN
return 2;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.break_any_call() RETURNS int AS $$
BEGIN
return 4;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.break_any_return() RETURNS int AS $$
BEGIN
return 8;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_none() RETURNS int AS $$
BEGIN
return 0;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_breakpoint() RETURNS int AS $$
BEGIN
return 1;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_enter() RETURNS int AS $$
BEGIN
return 2;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_return() RETURNS int AS $$
BEGIN
return 3;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_finish() RETURNS int AS $$
BEGIN
return 4;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_line() RETURNS int AS $$
BEGIN
return 5;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_interrupt() RETURNS int AS $$
BEGIN
return 6;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_exception() RETURNS int AS $$
BEGIN
return 7;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_exit() RETURNS int AS $$
BEGIN
return 8;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_knl_exit() RETURNS int AS $$
BEGIN
return 9;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_handler() RETURNS int AS $$
BEGIN
return 10;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_timeout() RETURNS int AS $$
BEGIN
return 11;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_instantiate() RETURNS int AS $$
BEGIN
return 12;
END;
$$ language plpgsql IMMUTABLE;

CREATE FUNCTION gms_debug.reason_abort() RETURNS int AS $$
BEGIN
return 13;
END;
$$ language plpgsql IMMUTABLE;




-- gms_debug package end