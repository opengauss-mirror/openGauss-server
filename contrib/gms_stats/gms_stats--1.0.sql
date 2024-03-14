/* contrib/gms_stats/gms_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_stats" to load this file. \quit

-- gms_stats package begin
CREATE SCHEMA gms_stats;
GRANT USAGE ON SCHEMA gms_stats TO PUBLIC;
CREATE TYPE objecttab AS (
       ownname varchar2(30),
       objtype varchar2(6),
       objname varchar2(30),
       partname varchar2(30),
       subpartname varchar2(30));

CREATE FUNCTION gms_stats.gs_analyze_schema_tables(schemaname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_analyze_schema_tables'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.gather_schema_stats(
       ownname varchar2,
       estimate_percent number default 100,
       block_sample boolean default false,
       method_opt varchar2 default 'FOR ALL COLUMNS SIZE AUTO',
       degree number default null,
       granularity varchar2 default 'GLOBAL',
       cascade boolean default false,
       stattab varchar2 default null,
       statid varchar2 default null,
       options varchar2 default 'GATHER',
       objlist ObjectTab default null,
       statown varchar2 default null,
       no_invalidate boolean default false,
       force boolean default false,
       obj_filter_list objecttab default null) IS
BEGIN
       perform gms_stats.gs_analyze_schema_tables(ownname);
       raise notice 'PL/SQL procedure successfully completed.';
END;

-- gms_stats package end
