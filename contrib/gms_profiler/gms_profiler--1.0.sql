/* contrib/gms_profiler/gms_profiler--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_profiler" to load this file. \quit

CREATE SCHEMA gms_profiler;

CREATE or REPLACE FUNCTION gms_profiler.start_profiler(IN run_comment varchar2 DEFAULT '', IN run_comment1 varchar2 DEFAULT '', OUT run_result binary_integer)
AS 'MODULE_PATHNAME', 'start_profiler'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.start_profiler_1(IN run_comment varchar2 DEFAULT '', IN run_comment1 varchar2 DEFAULT '')
returns void
AS 'MODULE_PATHNAME', 'start_profiler_1'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.start_profiler_ext(IN run_comment varchar2 DEFAULT '' ,
IN run_comment1 varchar2 DEFAULT '', OUT run_number binary_integer, OUT run_result binary_integer)
AS 'MODULE_PATHNAME', 'start_profiler_ext'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.start_profiler_ext_1(IN run_comment varchar2 DEFAULT '' ,
IN run_comment1 varchar2 DEFAULT '', OUT run_number binary_integer)
AS 'MODULE_PATHNAME', 'start_profiler_ext_1'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.stop_profiler()
returns binary_integer
AS 'MODULE_PATHNAME', 'stop_profiler'
LANGUAGE C VOLATILE STRICT NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.flush_data()
returns binary_integer
AS 'MODULE_PATHNAME', 'flush_data'
LANGUAGE C VOLATILE STRICT NOT FENCED;

CREATE or REPLACE PROCEDURE gms_profiler.get_version(OUT major binary_integer, OUT minor binary_integer)
AS
BEGIN
    major := 1;
    minor := 0;
END;

CREATE or REPLACE FUNCTION gms_profiler.internal_version_check()
returns binary_integer as $$
begin
    return 0;
end;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE or REPLACE FUNCTION gms_profiler.pause_profiler()
returns binary_integer
AS 'MODULE_PATHNAME', 'pause_profiler'
LANGUAGE C VOLATILE NOT FENCED;

CREATE or REPLACE FUNCTION gms_profiler.resume_profiler()
returns binary_integer
AS 'MODULE_PATHNAME', 'resume_profiler'
LANGUAGE C VOLATILE NOT FENCED;

CREATE UNLOGGED TABLE gms_profiler.plsql_profiler_runs
(
  runid           number primary key,  -- unique run identifier,
                                       -- from plsql_profiler_runnumber
  related_run     number,              -- runid of related run (for client/
                                       --     server correlation)
  run_owner       varchar2(32),        -- user who started run
  run_date        date,                -- start time of run
  run_comment     varchar2(2047),      -- user provided comment for this run
  run_total_time  number,              -- elapsed time for this run
  run_system_info varchar2(2047),      -- currently unused
  run_comment1    varchar2(2047),      -- additional comment
  spare1          varchar2(256)        -- unused
);

COMMENT ON TABLE gms_profiler.plsql_profiler_runs is
        'Run-specific information for the PL/SQL profiler';



CREATE UNLOGGED TABLE gms_profiler.plsql_profiler_units
(
  runid              number references gms_profiler.plsql_profiler_runs,
  unit_number        number,           -- internally generated library unit #
  unit_type          varchar2(32),     -- library unit type
  unit_owner         varchar2(32),     -- library unit owner name
  unit_name          varchar2(32),     -- library unit name
  -- timestamp on library unit, can be used to detect changes to
  -- unit between runs
  unit_timestamp     date,
  total_time         number DEFAULT 0 NOT NULL,
  spare1             number,           -- unused
  spare2             number,           -- unused
  --  
  primary key (runid, unit_number)
);

COMMENT ON TABLE gms_profiler.plsql_profiler_units is
        'Information about each library unit in a run';

CREATE UNLOGGED TABLE gms_profiler.plsql_profiler_data
(
  runid           number,           -- unique (generated) run identifier
  unit_number     number,           -- internally generated library unit #
  line#           number not null,  -- line number in unit
  total_occur     number,           -- number of times line was executed
  total_time      number,           -- total time spent executing line
  min_time        number,           -- minimum execution time for this line
  max_time        number,           -- maximum execution time for this line
  spare1          number,           -- unused
  spare2          number,           -- unused
  spare3          number,           -- unused
  spare4          number,           -- unused
  --
  primary key (runid, unit_number, line#),
  foreign key (runid, unit_number) references gms_profiler.plsql_profiler_units
);

COMMENT ON TABLE gms_profiler.plsql_profiler_data is
        'Accumulated data from all profiler runs';

CREATE SEQUENCE gms_profiler.plsql_profiler_runnumber start with 1 cache 1;

GRANT USAGE ON SCHEMA gms_profiler TO public;
GRANT SELECT ON ALL tables IN SCHEMA gms_profiler TO public;
REVOKE EXECUTE ON ALL functions IN SCHEMA gms_profiler FROM public;
REVOKE USAGE ON SEQUENCE gms_profiler.plsql_profiler_runnumber FROM public;
