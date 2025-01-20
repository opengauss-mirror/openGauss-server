/* contrib/gms_stats/gms_stats--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION gms_stats UPDATE TO '1.1'" to load this file. \quit

DROP PROCEDURE IF EXISTS gms_stats.gather_schema_stats;
DROP FUNCTION IF EXISTS gms_stats.gs_analyze_schema_tables;

CREATE FUNCTION gms_stats.gs_analyze_schema_tables(
    schemaname varchar2,
    stattab varchar2 default NULL,
    stattid varchar2 default NULL,
    statown varchar2 default NULL,
    force boolean default false)
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
    gms_stats.gs_analyze_schema_tables(ownname, stattab, statid, statown, force);
END;

CREATE FUNCTION gms_stats.gs_create_stat_table(ownname varchar2, stattab varchar2, tblspace varchar2, global_temporary boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_create_stat_table'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.create_stat_table(
    ownname varchar2,
    stattab varchar2,
    tblspace varchar2 default null,
    global_temporary boolean default false) IS
BEGIN
    gms_stats.gs_create_stat_table(ownname, stattab, tblspace, global_temporary);
END;

CREATE FUNCTION gms_stats.gs_drop_stat_table(ownname varchar2, stattab varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_drop_stat_table'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.drop_stat_table(
    ownname varchar2,
    stattab varchar2) IS
BEGIN
    gms_stats.gs_drop_stat_table(ownname, stattab);
END;

CREATE FUNCTION gms_stats.gs_lock_schema_stats(ownname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_lock_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.lock_schema_stats(
    ownname varchar2) IS
BEGIN
    gms_stats.gs_lock_schema_stats(ownname);
END;

CREATE FUNCTION gms_stats.gs_lock_table_stats(ownname varchar2, tabname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_lock_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.lock_table_stats(
    ownname varchar2,
    tabname varchar2) IS
BEGIN
    gms_stats.gs_lock_table_stats(ownname, tabname);
END;

CREATE FUNCTION gms_stats.gs_lock_partition_stats(ownname varchar2, tabname varchar2, partname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_lock_partition_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.lock_partition_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2) IS
BEGIN
    gms_stats.gs_lock_partition_stats(ownname, tabname, partname);
END;

CREATE FUNCTION gms_stats.gs_unlock_schema_stats(ownname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_unlock_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.unlock_schema_stats(
    ownname varchar2) IS
BEGIN
    gms_stats.gs_unlock_schema_stats(ownname);
END;

CREATE FUNCTION gms_stats.gs_unlock_table_stats(ownname varchar2, tabname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_unlock_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.unlock_table_stats(
    ownname varchar2,
    tabname varchar2) IS
BEGIN
    gms_stats.gs_unlock_table_stats(ownname, tabname);
END;

CREATE FUNCTION gms_stats.gs_unlock_partition_stats(ownname varchar2, tabname varchar2, partname varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_unlock_partition_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.unlock_partition_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2) IS
BEGIN
    gms_stats.gs_unlock_partition_stats(ownname, tabname, partname);
END;

CREATE FUNCTION gms_stats.gs_export_column_stats(ownname varchar2, tabname varchar2, colname varchar2, stattab varchar2, statown varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_export_column_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.export_column_stats(
    ownname varchar2,
    tabname varchar2,
    colname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null) IS
BEGIN
    gms_stats.gs_export_column_stats(ownname, tabname, colname, stattab, statown);
END;

CREATE FUNCTION gms_stats.gs_export_index_stats(ownname varchar2, indname varchar2, stattab varchar2, statown varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_export_index_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.export_index_stats(
    ownname varchar2,
    indname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null) IS
BEGIN
    gms_stats.gs_export_index_stats(ownname, indname, stattab, statown);
END;

CREATE FUNCTION gms_stats.gs_export_table_stats(ownname varchar2, tabname varchar2, partname varchar2, stattab varchar2, statown varchar2, cascade boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_export_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.export_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    cascade boolean default true,
    statown varchar2 default null,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_export_table_stats(ownname, tabname, partname, stattab, statown, cascade);
END;

CREATE FUNCTION gms_stats.gs_export_schema_stats(ownname varchar2, stattab varchar2, statown varchar2)
    RETURNS void
AS 'MODULE_PATHNAME','gs_export_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.export_schema_stats(
    ownname varchar2,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_export_schema_stats(ownname, stattab, statown);
END;

CREATE FUNCTION gms_stats.gs_import_column_stats(ownname varchar2, tabname varchar2, colname varchar2, stattab varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_import_column_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.import_column_stats(
    ownname varchar2,
    tabname varchar2,
    colname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false) IS
BEGIN
    gms_stats.gs_import_column_stats(ownname, tabname, colname, stattab, statown, force);
END;

CREATE FUNCTION gms_stats.gs_import_index_stats(ownname varchar2, indname varchar2, stattab varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_import_index_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.import_index_stats(
    ownname varchar2,
    indname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false) IS
BEGIN
    gms_stats.gs_import_index_stats(ownname, indname, stattab, statown, force);
END;

CREATE FUNCTION gms_stats.gs_import_table_stats(ownname varchar2, tabname varchar2, partname varchar2, stattab varchar2, cascade boolean, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_import_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.import_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2 default null,
    stattab varchar2,
    statid varchar2 default null,
    cascade boolean default true,
    statown varchar2 default null,
    no_invalidate boolean default false,
    force boolean default false,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_import_table_stats(ownname, tabname, partname, stattab, cascade, statown, force);
END;

CREATE FUNCTION gms_stats.gs_import_schema_stats(ownname varchar2, stattab varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_import_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.import_schema_stats(
    ownname varchar2,
    stattab varchar2,
    statid varchar2 default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_import_schema_stats(ownname, stattab, statown, force);
END;

CREATE FUNCTION gms_stats.gs_delete_column_stats(ownname varchar2, tabname varchar2, colname varchar2, stattab varchar2, statid varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_delete_column_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.delete_column_stats(
    ownname varchar2,
    tabname varchar2,
    colname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    cascade_parts boolean default true,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false,
    col_stat_type varchar2 default 'ALL') IS
BEGIN
    gms_stats.gs_delete_column_stats(ownname, tabname, colname, stattab, statid, statown, force);
END;

CREATE FUNCTION gms_stats.gs_delete_index_stats(ownname varchar2, indname varchar2, stattab varchar2, statid varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_delete_index_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.delete_index_stats(
    ownname varchar2,
    indname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    cascade_parts boolean default true,
    statown varchar2 default null,
    no_invalidate boolean default null,
    stattype varchar2 default 'ALL',
    force boolean default false,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_delete_index_stats(ownname, indname, stattab, statid, statown, force);
END;

CREATE FUNCTION gms_stats.gs_delete_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2,
    stattab varchar2,
    statid varchar2,
    cascade_parts boolean,
    cascade_columns boolean,
    cascade_indexes boolean,
    statown varchar2,
    force boolean)
RETURNS void
AS 'MODULE_PATHNAME','gs_delete_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.delete_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    cascade_parts boolean default true,
    cascade_columns boolean default true,
    cascade_indexes boolean default true,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_delete_table_stats(ownname, tabname, partname, stattab, statid, cascade_parts, cascade_columns, cascade_indexes, statown, force);
END;

CREATE FUNCTION gms_stats.gs_delete_schema_stats(ownname varchar2, stattab varchar2, statid varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_delete_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.delete_schema_stats(
    ownname varchar2,
    stattab varchar2 default null,
    statid varchar2 default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false,
    stat_category varchar2 default null) IS
BEGIN
    gms_stats.gs_delete_schema_stats(ownname, stattab, statid, statown, force);
END;

CREATE FUNCTION gms_stats.gs_set_column_stats(ownname varchar2, tabname varchar2, colname varchar2, stattab varchar2, statid varchar2, distcnt number, nullcnt number, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_set_column_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.set_column_stats(
    ownname varchar2,
    tabname varchar2,
    colname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    distcnt number default null,
    density number default null,
    nullcnt number default null,
    srec text default null,
    avgclen number default null,
    flags number default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false) IS
BEGIN
    gms_stats.gs_set_column_stats(ownname, tabname, colname, stattab, statid, distcnt, nullcnt, statown, force);
END;

CREATE FUNCTION gms_stats.gs_set_index_stats(ownname varchar2, indname varchar2, stattab varchar2, statid varchar2, numdist number, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_set_index_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.set_index_stats(
    ownname varchar2,
    indname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    numrows number default null,
    numblks number default null,
    numdist number default null,
    avglblk number default null,
    avgdblk number default null,
    clstfct number default null,
    indlevel number default null,
    flags number default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    guessq number default null,
    cachedblk number default null,
    cachehit number default null,
    force boolean default false) IS
BEGIN
    gms_stats.gs_set_index_stats(ownname, indname, stattab, statid, numdist, statown, force);
END;

CREATE FUNCTION gms_stats.gs_set_table_stats(ownname varchar2, tabname varchar2, partname varchar2, stattab varchar2, statid varchar2, numrows number, numblks number, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_set_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.set_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2 default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    numrows number default null,
    numblks number default null,
    avgrlen number default null,
    flags number default null,
    statown varchar2 default null,
    no_invalidate boolean default null,
    cachedblk number default null,
    cachehit number default null,
    force boolean default false,
    im_imcu_count number default null,
    im_block_count number default null,
    scanrate number default null) IS
BEGIN
    gms_stats.gs_set_table_stats(ownname, tabname, partname, stattab, statid, numrows, numblks, statown, force);
END;

CREATE FUNCTION gms_stats.gs_restore_table_stats(ownname varchar2, tabname varchar2, as_of_timestamp timestamp with time zone, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_restore_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.restore_table_stats(
    ownname varchar2,
    tabname varchar2,
    as_of_timestamp timestamp with time zone,
    restore_cluster_index boolean default false,
    force boolean default false,
    no_invalidate boolean default null) IS
BEGIN
    gms_stats.gs_restore_table_stats(ownname, tabname, as_of_timestamp, force);
END;

CREATE FUNCTION gms_stats.gs_restore_schema_stats(ownname varchar2, as_of_timestamp timestamp with time zone, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_restore_schema_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.restore_schema_stats(
    ownname varchar2,
    as_of_timestamp timestamp with time zone,
    force boolean default false,
    no_invalidate boolean default null) IS
BEGIN
    gms_stats.gs_restore_schema_stats(ownname, as_of_timestamp, force);
END;

CREATE FUNCTION gms_stats.gs_gather_table_stats(ownname varchar2, tabname varchar2, partname varchar2, stattab varchar2, statid varchar2, statown varchar2, force boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_gather_table_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.gather_table_stats(
    ownname varchar2,
    tabname varchar2,
    partname varchar2 default null,
    estimate_percent number default null,
    block_sample boolean default false,
    method_opt varchar2 default null,
    degree number default null,
    granularity varchar2 default null,
    cascade boolean default true,
    stattab varchar2 default null,
    statid varchar2 default null,
    statown varchar2 default null,
    no_invalidate boolean default false,
    stattype varchar2 default null,
    force boolean default false,
    context text default null,
    options varchar2 default null) IS
BEGIN
    gms_stats.gs_gather_table_stats(ownname, tabname, partname, stattab, statid, statown, force);
END;

CREATE OR REPLACE PROCEDURE gms_stats.gather_index_stats(
    ownname varchar2,
    indname varchar2,
    partname varchar2 default null,
    estimate_percent number default null,
    stattab varchar2 default null,
    statid varchar2 default null,
    statown varchar2 default null,
    degree number default null,
    granularity varchar2 default null,
    no_invalidate boolean default null,
    force boolean default false) IS
DECLARE
    exists_index BOOLEAN;
    tabname VARCHAR2;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_class a, pg_namespace b WHERE a.relnamespace = b.Oid AND b.nspname = ownname AND relname = indname)
    INTO exists_index;

    IF NOT exists_index THEN
        RAISE EXCEPTION 'Index \"%\" is not exists', indname;
    END IF;

    SELECT relname INTO tabname 
    FROM pg_class WHERE Oid = (SELECT indrelid FROM pg_index WHERE indexrelid = (
        SELECT a.Oid FROM pg_class a, pg_namespace b WHERE a.relnamespace = b.Oid AND b.nspname = ownname AND relname = indname));

    gms_stats.gather_table_stats(ownname, tabname, stattab:=stattab, statid:=statid, statown:=statown, force=>force);
END;

CREATE FUNCTION gms_stats.gs_gather_database_stats(stattab varchar2, statid varchar2, statown varchar2, gather_sys boolean)
    RETURNS void
AS 'MODULE_PATHNAME','gs_gather_database_stats'
LANGUAGE C VOLATILE NOT FENCED;

CREATE OR REPLACE PROCEDURE gms_stats.gather_database_stats(
    estimate_percent number default null,
    block_sample boolean default false,
    method_opt varchar2 default null,
    degree number default null,
    granularity varchar2 default 'GLOBAL',
    cascade boolean default true,
    stattab varchar2 default null,
    statid varchar2 default null,
    options varchar2 default 'GATHER',
    objlist objecttab default null,
    statown varchar2 default null,
    gather_sys boolean default true,
    no_invalidate boolean default false,
    obj_filter_list objecttab default null) IS
BEGIN
    gms_stats.gs_gather_database_stats(stattab, statid, statown, gather_sys);
END;

CREATE FUNCTION gms_stats.get_stats_history_availability()
    RETURNS timestamp with time zone
AS 'MODULE_PATHNAME','get_stats_history_availability'
LANGUAGE C VOLATILE NOT FENCED;

CREATE FUNCTION gms_stats.get_stats_history_retention()
    RETURNS number
AS 'MODULE_PATHNAME','get_stats_history_retention'
LANGUAGE C VOLATILE NOT FENCED;

CREATE FUNCTION gms_stats.purge_stats(before_timestamp timestamp with time zone)
    RETURNS void
AS 'MODULE_PATHNAME','purge_stats'
LANGUAGE C VOLATILE NOT FENCED;
