/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * gms_stats.cpp
 *  gms_stats can effectively estimate statistical data.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_stats/gms_stats.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/skey.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/heap.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_history.h"
#include "catalog/pg_statistic_lock.h"
#include "commands/sqladvisor.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "gms_stats.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gs_analyze_schema_tables);
PG_FUNCTION_INFO_V1(gs_create_stat_table);
PG_FUNCTION_INFO_V1(gs_drop_stat_table);
PG_FUNCTION_INFO_V1(gs_lock_schema_stats);
PG_FUNCTION_INFO_V1(gs_lock_table_stats);
PG_FUNCTION_INFO_V1(gs_lock_partition_stats);
PG_FUNCTION_INFO_V1(gs_unlock_schema_stats);
PG_FUNCTION_INFO_V1(gs_unlock_table_stats);
PG_FUNCTION_INFO_V1(gs_unlock_partition_stats);
PG_FUNCTION_INFO_V1(gs_export_column_stats);
PG_FUNCTION_INFO_V1(gs_export_index_stats);
PG_FUNCTION_INFO_V1(gs_export_table_stats);
PG_FUNCTION_INFO_V1(gs_export_schema_stats);
PG_FUNCTION_INFO_V1(gs_import_column_stats);
PG_FUNCTION_INFO_V1(gs_import_index_stats);
PG_FUNCTION_INFO_V1(gs_import_table_stats);
PG_FUNCTION_INFO_V1(gs_import_schema_stats);
PG_FUNCTION_INFO_V1(gs_delete_column_stats);
PG_FUNCTION_INFO_V1(gs_delete_index_stats);
PG_FUNCTION_INFO_V1(gs_delete_table_stats);
PG_FUNCTION_INFO_V1(gs_delete_schema_stats);
PG_FUNCTION_INFO_V1(gs_set_column_stats);
PG_FUNCTION_INFO_V1(gs_set_index_stats);
PG_FUNCTION_INFO_V1(gs_set_table_stats);
PG_FUNCTION_INFO_V1(gs_restore_table_stats);
PG_FUNCTION_INFO_V1(gs_restore_schema_stats);
PG_FUNCTION_INFO_V1(gs_gather_table_stats);
PG_FUNCTION_INFO_V1(gs_gather_database_stats);
PG_FUNCTION_INFO_V1(get_stats_history_availability);
PG_FUNCTION_INFO_V1(get_stats_history_retention);
PG_FUNCTION_INFO_V1(purge_stats);

static void CheckPermission(Oid relid = InvalidOid);
static Oid GetPartitionOid(Oid relid, char* partname);
static bool CheckColumnIsValid(Oid relid, int16 attnum);
static void UpdateStatsiticInfo(Oid relid, char relkind, int16 attnum,
                        Datum* values, bool* nulls, bool* replaces);
static Oid GetUserTableRelid(char* statown, char* stattab, char* statid);
static void UpdateTableStatsInfo(Oid relid, double numpages, double numtuples);
static void UpdatePartitionStatsInfo(Oid partid, double numpages, double numtuples);
static void UpdatePartitionStatsInfo(Oid parentid, char parttype, double numpages, double numtuples);
static void DeleteColumnStats(Oid namespaceid, Oid relid, int16 attnum, Oid starelid, char relkind);
extern void DoVacuumMppTable(VacuumStmt* stmt, const char* query_string, bool is_top_level, bool sent_to_remote);
static void GatherTableStats(char* sqlString, Oid namespaceid, Oid relid, Oid partid, Oid starelid);

static List* GetRelationsInSchema(char *namespc)
{
    Relation pg_class_rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan = NULL;
    HeapTuple tuple = NULL;
    List* tblRelnames = NIL;
    Oid nspid = InvalidOid;

    nspid = get_namespace_oid(namespc, false);

    ScanKeyInit(&skey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspid));
    pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
    sysscan = systable_beginscan(pg_class_rel, ClassNameNspIndexId, true, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
        if (reltup->relkind == RELKIND_RELATION || reltup->relkind == RELKIND_MATVIEW) {
            tblRelnames = lappend(tblRelnames, pstrdup(reltup->relname.data));
        }
    }
    systable_endscan(sysscan);
    heap_close(pg_class_rel, AccessShareLock);
    return tblRelnames;
}

static void CheckPermission(Oid relid)
{
    bool classOwner = false;
    char* msg = NULL;

    if (OidIsValid(relid)) {
        classOwner = pg_class_ownercheck(relid, GetUserId());
        msg = "Need classowner or dba";
    } else {
        msg = "Need dba or databaseowner";
    }

    if (!classOwner && !((pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()))
        || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Permission denied. %s to call.", msg)));
    }
}

Datum gs_analyze_schema_tables(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(4);
    List* relnamesList = NIL;
    ListCell* lc = NULL;
    char* relname = NULL;
    StringInfo executeSql;

    if (!PG_ARGISNULL(1)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(1));
    }
    if (!PG_ARGISNULL(2)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    if (PG_ARGISNULL(3)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    CheckPermission();

    Oid namespaceId = get_namespace_oid(ownname, false);
    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    executeSql = makeStringInfo();

    relnamesList = GetRelationsInSchema(ownname);
    foreach(lc, relnamesList) {
        relname = (char *) lfirst(lc);
        Oid relid = get_relname_relid(relname, namespaceId);
        if (!force && CheckRelationLocked(namespaceId, relid)) {
            continue;
        }
        resetStringInfo(executeSql);
        appendStringInfo(executeSql, "ANALYZE %s.%s;", quote_identifier(ownname), quote_identifier(relname));
        GatherTableStats(executeSql->data, namespaceId, relid, InvalidOid, starelid);
    }

    DestroyStringInfo(executeSql);
    list_free(relnamesList);
    pfree_ext(ownname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

static void ExecuteSqlString(const char* sql)
{
    int ret;
    if ((ret = SPI_connect()) < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    ret = SPI_execute(sql, false, 0);
    if (ret != SPI_OK_UTILITY) {
        ereport(ERROR,
            (errcode(ERRCODE_SPI_EXECUTE_FAILURE),
                errmsg("SPI_execute execute query failed: %s", SPI_result_code_string(ret))));
    }

    SPI_finish();
}

Datum get_stats_history_availability(PG_FUNCTION_ARGS)
{
    if (t_thrd.proc->workingVersionNum < STATISTIC_HISTORY_VERSION_NUMBER) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Working Version Num less than %u does not support read/write pg_statistic_history systable.",
                    STATISTIC_HISTORY_VERSION_NUMBER)));
    }
    SysScanDesc sysscan;
    HeapTuple tuple = NULL;
    Relation idxRel = NULL;
    Relation rel = NULL;
    ScanKeyData skey[1];
    TimestampTz result = 0;
    bool isnull = true;

    ScanKeyInit(&skey[0], Anum_pg_statistic_history_current_analyzetime, BTLessEqualStrategyNumber, F_TIMESTAMP_LE,
        TimestampTzGetDatum(GetCurrentTimestamp()));

    rel = heap_open(StatisticHistoryRelationId, AccessShareLock);
    idxRel = index_open(StatisticHistoryCurrTimeRelidIndexId, AccessShareLock);

    sysscan = systable_beginscan_ordered(rel, idxRel, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(sysscan, ForwardScanDirection))) {
        TimestampTz time = DatumGetTimestampTz(heap_getattr(tuple, Anum_pg_statistic_history_current_analyzetime,
                                                    rel->rd_att, &isnull));
        if (!isnull) {
            result = time;
            break;
        }
    }

    systable_endscan_ordered(sysscan);
    index_close(idxRel, AccessShareLock);
    heap_close(rel, AccessShareLock);

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum get_stats_history_retention(PG_FUNCTION_ARGS)
{
    int retention = u_sess->attr.attr_sql.gms_stats_history_retention;
    return DirectFunctionCall1(int4_numeric, Int32GetDatum(retention));
}

Datum gs_create_stat_table(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(1));
    bool global_temporary = PG_GETARG_BOOL(3);
    char* tblspace = NULL;

    if (!PG_ARGISNULL(2)) {
        tblspace = text_to_cstring(PG_GETARG_TEXT_P(2));
    }

    bool prev = g_instance.attr.attr_common.allowSystemTableMods;
    StringInfo executeSql = makeStringInfo();
    PG_TRY();
    {
        g_instance.attr.attr_common.allowSystemTableMods = true;

        appendStringInfo(executeSql, "CREATE ");
        if (global_temporary) {
            appendStringInfo(executeSql, "GLOBAL TEMPORARY ");
        }

        appendStringInfo(executeSql, "TABLE %s.%s (namespaceid oid, starelid oid, partid oid, statype \"char\","       \
        "starelkind \"char\", staattnum smallint, stainherit boolean, stanullfrac real, stawidth integer,"              \
        "stadistinct real, reltuples double precision, relpages double precision, stakind1 smallint, stakind2 smallint,"\
        "stakind3 smallint, stakind4 smallint, stakind5 smallint, staop1 oid, staop2 oid, staop3 oid, staop4 oid,"      \
        "staop5 oid, stanumbers1 real[], stanumbers2 real[], stanumbers3 real[], stanumbers4 real[],stanumbers5 real[],"\
        "stavalues1 anyarray, stavalues2 anyarray, stavalues3 anyarray, stavalues4 anyarray, stavalues5 anyarray,"      \
        "stadndistinct real, staextinfo text)", quote_identifier(ownname), quote_identifier(stattab));
        
        if (tblspace != NULL) {
            appendStringInfo(executeSql, " TABLESPACE %s", quote_identifier(tblspace));
        }
        appendStringInfo(executeSql, ";");
        ExecuteSqlString(executeSql->data);

        resetStringInfo(executeSql);
        appendStringInfo(executeSql, "CREATE INDEX %s_namespac_type_rel_idx ON %s.%s USING BTREE (namespaceid, statype, starelid);",
                        stattab, quote_identifier(ownname), quote_identifier(stattab));
        ExecuteSqlString(executeSql->data);
    }
    PG_CATCH();
    {
        g_instance.attr.attr_common.allowSystemTableMods = prev;
        PG_RE_THROW();
    }
    PG_END_TRY();
    g_instance.attr.attr_common.allowSystemTableMods = prev;

    DestroyStringInfo(executeSql);
    pfree_ext(ownname);
    pfree_ext(stattab);
    pfree_ext(tblspace);

    PG_RETURN_VOID();
}

Datum gs_drop_stat_table(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(1));

    StringInfo executeSql = makeStringInfo();
    appendStringInfo(executeSql, "DROP TABLE %s.%s;", quote_identifier(ownname), quote_identifier(stattab));
    ExecuteSqlString(executeSql->data);
    resetStringInfo(executeSql);

    pfree_ext(ownname);
    pfree_ext(stattab);

    PG_RETURN_VOID();
}

Datum gs_lock_schema_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    Oid namespaceid = get_namespace_oid(ownname, false);

    LockStatistic(namespaceid, InvalidOid);

    pfree_ext(ownname);

    PG_RETURN_VOID();
}

Datum gs_lock_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }

    LockStatistic(namespaceid, relid);

    pfree_ext(ownname);
    pfree_ext(tabname);

    PG_RETURN_VOID();
}

Datum gs_lock_partition_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* partname = text_to_cstring(PG_GETARG_TEXT_P(2));

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    Oid partid = GetPartitionOid(relid, partname);
    if (!OidIsValid(partid)) {
        ereport(ERROR, (errmsg("Partition \"%s\" is not exist", partname)));
    }
    LockStatistic(namespaceid, relid, partid);

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);

    PG_RETURN_VOID();
}

Datum gs_unlock_schema_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));

    Oid namespaceid = get_namespace_oid(ownname, false);
    UnlockStatistic(namespaceid, InvalidOid);

    pfree_ext(ownname);

    PG_RETURN_VOID();
}

Datum gs_unlock_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }

    UnlockStatistic(namespaceid, relid);

    pfree_ext(ownname);
    pfree_ext(tabname);

    PG_RETURN_VOID();
}

Datum gs_unlock_partition_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* partname = text_to_cstring(PG_GETARG_TEXT_P(2));

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    Oid partid = GetPartitionOid(relid, partname);
    if (!OidIsValid(partid)) {
        ereport(ERROR, (errmsg("Partition \"%s\" is not exist", partname)));
    }
    UnlockStatistic(namespaceid, relid, partid);

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);

    PG_RETURN_VOID();
}

static Oid GetPartitionOid(Oid relid, char* partname)
{
    Oid partid = GetSysCacheOid3(PARTPARTOID, NameGetDatum(partname), CharGetDatum('p'), ObjectIdGetDatum(relid));
    if (OidIsValid(partid)) {
        return partid;
    }
    /* find subpartition */
    Relation rel = heap_open(relid, AccessShareLock);
    List* partids = relationGetPartitionOidList(rel);
    heap_close(rel, AccessShareLock);
    if (partids == NIL || list_length(partids) == 0) {
        return InvalidOid;
    }
    ListCell* lc = NULL;
    foreach (lc, partids) {
        Oid parentid = lfirst_oid(lc);
        partid = GetSysCacheOid3(PARTPARTOID, NameGetDatum(partname), CharGetDatum('s'), ObjectIdGetDatum(parentid));
        if (OidIsValid(partid)) {
            break;
        }
    }
    list_free(partids);

    return partid;
}

static bool ExistsStatisticInfo(Oid relid, Oid partid)
{
    Relation starel = NULL;
    ScanKeyData skey[1];
    SysScanDesc scan = NULL;
    bool result = false;

    ScanKeyInit(&skey[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    starel = relation_open(StatisticRelationId, AccessShareLock);
    scan = systable_beginscan(starel, StatisticRelidKindAttnumInhIndexId, true, NULL, 1, skey);
    if (HeapTupleIsValid(systable_getnext(scan))) {
        result = true;
    }
    systable_endscan(scan);
    relation_close(starel, AccessShareLock);

    if (!result && OidIsValid(partid)) {
        ScanKeyInit(&skey[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partid));
        starel = relation_open(StatisticRelationId, AccessShareLock);
        scan = systable_beginscan(starel, StatisticRelidKindAttnumInhIndexId, true, NULL, 1, skey);
        if (HeapTupleIsValid(systable_getnext(scan))) {
            result = true;
        }
        systable_endscan(scan);
        relation_close(starel, AccessShareLock);
    }
    return result;
}

static Oid GetUserTableRelid(char* statown, char* stattab, char* statid)
{
    Oid starelid = InvalidOid;
    if (stattab) {
        starelid = get_relname_relid(stattab, SchemaNameGetSchemaOid(statown, false));
    } else if (statid) {
        starelid = atooid(statid);
    }

    if (!OidIsValid(starelid) && (stattab || statid)) {
        ereport(ERROR, (errmsg("The specified user table does not exist")));
    }

    return starelid;
}

static bool CheckColumnIsValid(Oid relid, int16 attnum)
{
    HeapTuple atttup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(atttup)) {
        return false;
    }
    Form_pg_attribute attForm = (Form_pg_attribute)GETSTRUCT(atttup);
    bool result = attForm->attisdropped;
    ReleaseSysCache(atttup);
    return !result;
}

/**
 * update pg_statsitic info.
 * @param relid tableid or partitionid
 * @param relkind table or partition
 * @param attnum column number
 */
static void UpdateStatsiticInfo(Oid relid, char relkind, int16 attnum,
                        Datum* values, bool* nulls, bool* replaces)
{
    Relation pgstatistic = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[3];
    HeapTuple staTuple = NULL;
    HeapTuple newTuple = NULL;

    ScanKeyInit(&key[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_statistic_starelkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[2], Anum_pg_statistic_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

    pgstatistic = heap_open(StatisticRelationId, RowExclusiveLock);
    scan = systable_beginscan(pgstatistic, StatisticRelidKindAttnumInhIndexId, true, NULL, 3, key);
    if (HeapTupleIsValid(staTuple = systable_getnext(scan))) {
        newTuple = heap_modify_tuple(staTuple, RelationGetDescr(pgstatistic), values, nulls, replaces);
        simple_heap_update(pgstatistic, &newTuple->t_self, newTuple);
    } else {
        newTuple = heap_form_tuple(RelationGetDescr(pgstatistic), values, nulls);
        (void) simple_heap_insert(pgstatistic, newTuple);
    }
    CatalogUpdateIndexes(pgstatistic, newTuple);

    heap_freetuple_ext(newTuple);
    systable_endscan(scan);
    heap_close(pgstatistic, RowExclusiveLock);
    CommandCounterIncrement();
}

static void UpdateTableStatsInfo(Oid relid, double numpages, double numtuples)
{
    Datum values[Natts_pg_class] = {0};
    bool nulls[Natts_pg_class];
    bool replaces[Natts_pg_class] = {false};
    Relation rel = NULL;
    HeapTuple reltup = NULL;
    HeapTuple newtup = NULL;

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_class_relpages - 1] = Float8GetDatum(numpages);
    nulls[Anum_pg_class_relpages - 1] = false;
    replaces[Anum_pg_class_relpages - 1] = true;

    values[Anum_pg_class_reltuples - 1] =Float8GetDatum(numtuples);
    nulls[Anum_pg_class_reltuples - 1] = false;
    replaces[Anum_pg_class_reltuples - 1] = true;

    rel = relation_open(RelationRelationId, RowExclusiveLock); 
    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(reltup)) {
        newtup = heap_modify_tuple(reltup, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &newtup->t_self, newtup);
        CatalogUpdateIndexes(rel, newtup);

        heap_freetuple_ext(newtup);
        ReleaseSysCache(reltup);
        CommandCounterIncrement();
    }
    relation_close(rel, RowExclusiveLock);
}

static void UpdatePartitionStatsInfo(Oid partid, double numpages, double numtuples)
{
    Datum values[Natts_pg_partition] = {0};
    bool nulls[Natts_pg_partition];
    bool replaces[Natts_pg_partition] = {false};
    Relation rel = NULL;
    HeapTuple reltup = NULL;
    HeapTuple newtup = NULL;

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_partition_relpages - 1] = Float8GetDatum(numpages);
    nulls[Anum_pg_partition_relpages - 1] = false;
    replaces[Anum_pg_partition_relpages - 1] = true;

    values[Anum_pg_partition_reltuples - 1] = Float8GetDatum(numtuples);
    nulls[Anum_pg_partition_reltuples - 1] = false;
    replaces[Anum_pg_partition_reltuples - 1] = true;

    rel = relation_open(PartitionRelationId, RowExclusiveLock); 
    reltup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    if (HeapTupleIsValid(reltup)) {
        newtup = heap_modify_tuple(reltup, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &newtup->t_self, newtup);
        CatalogUpdateIndexes(rel, newtup);
        heap_freetuple_ext(newtup);
        ReleaseSysCache(reltup);
    }
    relation_close(rel, RowExclusiveLock);
    CommandCounterIncrement();
}

static void UpdatePartitionStatsInfo(Oid parentid, char parttype, double numpages, double numtuples)
{
    Datum values[Natts_pg_partition] = {0};
    bool nulls[Natts_pg_partition];
    bool replaces[Natts_pg_partition] = {false};
    Relation partrel = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[2];
    HeapTuple tuple = NULL;
    HeapTuple newtup = NULL;
    List* updateList = NIL;
    ListCell* lc = NULL;

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_partition_relpages - 1] = Float8GetDatum(numpages);
    nulls[Anum_pg_partition_relpages - 1] = false;
    replaces[Anum_pg_partition_relpages - 1] = true;

    values[Anum_pg_partition_reltuples - 1] = Float8GetDatum(numtuples);
    nulls[Anum_pg_partition_reltuples - 1] = false;
    replaces[Anum_pg_partition_reltuples - 1] = true;

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, ObjectIdGetDatum(parttype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentid));

    partrel = relation_open(PartitionRelationId, RowExclusiveLock);
    scan = systable_beginscan(partrel, PartitionParentOidIndexId, true, NULL, 2, key);

    /* scan all update rows */
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        newtup = heap_modify_tuple(tuple, RelationGetDescr(partrel), values, nulls, replaces);
        updateList = lappend(updateList, newtup);
    }
    systable_endscan(scan);

    /* do update */
    foreach(lc, updateList) {
        newtup = (HeapTuple) lfirst(lc);
        simple_heap_update(partrel, &newtup->t_self, newtup);
        CatalogUpdateIndexes(partrel, newtup);
        heap_freetuple_ext(newtup);
    }

    relation_close(partrel, RowExclusiveLock);
    list_free_ext(updateList);
    CommandCounterIncrement();
}

static void ExportColumnStats(Oid namespaceid, Oid relid, Oid starelid, char relkind, int16 attnum)
{
    Datum values[Natts_user_table];
    bool nulls[Natts_user_table];
    bool replaces[Natts_user_table];
    bool isNull;
    errno_t rc = 0;

    /* If column is droped, sholud delete if exists user-table column info */
    if (!CheckColumnIsValid(relid, attnum)) {
        DeleteColumnStats(namespaceid, relid, attnum, starelid, '\0');
        return;
    }

    HeapTuple tup = SearchSysCache4(STATRELKINDATTINH, ObjectIdGetDatum(relid), CharGetDatum(relkind),
                                        Int16GetDatum(attnum), BoolGetDatum(false));
    if (!HeapTupleIsValid(tup)) {
        return;
    }

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    values[Anum_user_table_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    nulls[Anum_user_table_namespaceid - 1] = false;
    values[Anum_user_table_starelid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_user_table_starelid - 1] = false;
    values[Anum_user_table_statype - 1] = CharGetDatum(STATYPE_COLUMN);
    nulls[Anum_user_table_statype - 1] = false;
    values[Anum_user_table_starelkind - 1] = CharGetDatum(relkind);
    nulls[Anum_user_table_starelkind - 1] = false;
    values[Anum_user_table_staattnum - 1] = Int16GetDatum(attnum);
    nulls[Anum_user_table_staattnum - 1] = false;
    values[Anum_user_table_stainherit - 1] = BoolGetDatum(false);
    nulls[Anum_user_table_stainherit - 1] = false;
    values[Anum_user_table_stanullfrac - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanullfrac, &isNull);
    nulls[Anum_user_table_stanullfrac - 1] = isNull;
    values[Anum_user_table_stawidth - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stawidth, &isNull);
    nulls[Anum_user_table_stawidth - 1] = isNull;
    values[Anum_user_table_stadistinct - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stadistinct, &isNull);
    nulls[Anum_user_table_stadistinct - 1] = isNull;
    values[Anum_user_table_stakind1 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stakind1, &isNull);
    nulls[Anum_user_table_stakind1 - 1] = isNull;
    values[Anum_user_table_stakind2 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stakind2, &isNull);
    nulls[Anum_user_table_stakind2 - 1] = isNull;
    values[Anum_user_table_stakind3 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stakind3, &isNull);
    nulls[Anum_user_table_stakind3 - 1] = isNull;
    values[Anum_user_table_stakind4 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stakind4, &isNull);
    nulls[Anum_user_table_stakind4 - 1] = isNull;
    values[Anum_user_table_stakind5 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stakind5, &isNull);
    nulls[Anum_user_table_stakind5 - 1] = isNull;
    values[Anum_user_table_staop1 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staop1, &isNull);
    nulls[Anum_user_table_staop1 - 1] = isNull;
    values[Anum_user_table_staop2 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staop2, &isNull);
    nulls[Anum_user_table_staop2 - 1] = isNull;
    values[Anum_user_table_staop3 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staop3, &isNull);
    nulls[Anum_user_table_staop3 - 1] = isNull;
    values[Anum_user_table_staop4 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staop4, &isNull);
    nulls[Anum_user_table_staop4 - 1] = isNull;
    values[Anum_user_table_staop5 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staop5, &isNull);
    nulls[Anum_user_table_staop5 - 1] = isNull;
    values[Anum_user_table_stanumbers1 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanumbers1, &isNull);
    nulls[Anum_user_table_stanumbers1 - 1] = isNull;
    values[Anum_user_table_stanumbers2 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanumbers2, &isNull);
    nulls[Anum_user_table_stanumbers2 - 1] = isNull;
    values[Anum_user_table_stanumbers3 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanumbers3, &isNull);
    nulls[Anum_user_table_stanumbers3 - 1] = isNull;
    values[Anum_user_table_stanumbers4 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanumbers4, &isNull);
    nulls[Anum_user_table_stanumbers4 - 1] = isNull;
    values[Anum_user_table_stanumbers5 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stanumbers5, &isNull);
    nulls[Anum_user_table_stanumbers5 - 1] = isNull;
    values[Anum_user_table_stavalues1 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stavalues1, &isNull);
    nulls[Anum_user_table_stavalues1 - 1] = isNull;
    values[Anum_user_table_stavalues2 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stavalues2, &isNull);
    nulls[Anum_user_table_stavalues2 - 1] = isNull;
    values[Anum_user_table_stavalues3 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stavalues3, &isNull);
    nulls[Anum_user_table_stavalues3 - 1] = isNull;
    values[Anum_user_table_stavalues4 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stavalues4, &isNull);
    nulls[Anum_user_table_stavalues4 - 1] = isNull;
    values[Anum_user_table_stavalues5 - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stavalues5, &isNull);
    nulls[Anum_user_table_stavalues5 - 1] = isNull;
    values[Anum_user_table_stadndistinct - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_stadndistinct, &isNull);
    nulls[Anum_user_table_stadndistinct - 1] = isNull;
    values[Anum_user_table_staextinfo - 1] =
        SysCacheGetAttr(STATRELKINDATTINH, tup, Anum_pg_statistic_staextinfo, &isNull);
    nulls[Anum_user_table_staextinfo - 1] = isNull;

    ReleaseSysCache(tup);

    HeapTuple tuple = NULL;
    ScanKeyData skey[4];
    TableScanDesc scan = NULL;
    Relation starel = NULL;
    HeapTuple newtup = NULL;

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STATYPE_COLUMN));
    ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[3], Anum_user_table_staattnum, BTEqualStrategyNumber, F_INT2EQ,
        Int16GetDatum(attnum));

    starel = heap_open(starelid, RowExclusiveLock);
    scan = heap_beginscan(starel, SnapshotNow, 4, skey);
    if (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        replaces[Anum_user_table_reltuples - 1] = false;
        replaces[Anum_user_table_relpages - 1] = false;
        replaces[Anum_user_table_partid - 1] = false;
        newtup = heap_modify_tuple(tuple, RelationGetDescr(starel), values, nulls, replaces);
        simple_heap_update(starel, &newtup->t_self, newtup);
    } else {
        newtup = heap_form_tuple(RelationGetDescr(starel), values, nulls);
        (void)simple_heap_insert(starel, newtup);
    }
    CatalogUpdateIndexes(starel, newtup);

    heap_endscan(scan);
    heap_freetuple_ext(newtup);
    heap_close(starel, RowExclusiveLock);
    CommandCounterIncrement();
}

static void ExportTableClassStats(Oid namespaceid, Oid relid, Oid starelid, Oid partid)
{
    Datum values[Natts_user_table] = {0};
    bool nulls[Natts_user_table];
    bool replaces[Natts_user_table] = {false};
    Relation starel = NULL;
    ScanKeyData skey[3];
    HeapTuple tuple;
    TableScanDesc scan = NULL;
    bool exists = false;
    HeapTuple reltup = NULL;
    HeapTuple newtup = NULL;
    bool isNull;

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_user_table_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    nulls[Anum_user_table_namespaceid - 1] = false;
    values[Anum_user_table_starelid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_user_table_starelid - 1] = false;
    if (OidIsValid(partid)) {
        values[Anum_user_table_partid - 1] = ObjectIdGetDatum(partid);
        nulls[Anum_user_table_partid - 1] = false;
        values[Anum_user_table_statype - 1] = CharGetDatum(STATYPE_PARTITION);
    } else {
        values[Anum_user_table_statype - 1] = CharGetDatum(STATYPE_RELATION);
    }
    nulls[Anum_user_table_statype - 1] = false;

    if (OidIsValid(partid)) {
        reltup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    } else {
        reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    }
    if (HeapTupleIsValid(reltup)) {
        if (OidIsValid(partid)) {
            values[Anum_user_table_reltuples - 1] =
                SysCacheGetAttr(PARTRELID, reltup, Anum_pg_partition_reltuples, &isNull);
            nulls[Anum_user_table_reltuples - 1] = isNull;
            values[Anum_user_table_relpages - 1] =
                SysCacheGetAttr(PARTRELID, reltup, Anum_pg_partition_relpages, &isNull);
            nulls[Anum_user_table_relpages - 1] = isNull;
        } else {
            values[Anum_user_table_reltuples - 1] =
                SysCacheGetAttr(RELOID, reltup, Anum_pg_class_reltuples, &isNull);
            nulls[Anum_user_table_reltuples - 1] = isNull;
            values[Anum_user_table_relpages - 1] =
                SysCacheGetAttr(RELOID, reltup, Anum_pg_class_relpages, &isNull);
            nulls[Anum_user_table_relpages - 1] = isNull;
        }

        ReleaseSysCache(reltup);
    }

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(OidIsValid(partid) ? STATYPE_PARTITION : STATYPE_RELATION));
    ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));

    starel = heap_open(starelid, RowExclusiveLock);
    scan = heap_beginscan(starel, SnapshotNow, 3, skey);
    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        if (OidIsValid(partid)) {
            Datum val = heap_getattr(tuple, Anum_user_table_partid, RelationGetDescr(starel), &isNull);
            if (!isNull && DatumGetObjectId(val) != partid) {
                continue;
            }
        }
        exists = true;
        replaces[Anum_user_table_reltuples - 1] = true;
        replaces[Anum_user_table_relpages - 1] = true;
        newtup = heap_modify_tuple(tuple, RelationGetDescr(starel), values, nulls, replaces);
        simple_heap_update(starel, &newtup->t_self, newtup);
        break;
    }

    if (!exists) {
        newtup = heap_form_tuple(RelationGetDescr(starel), values, nulls);
        (void)simple_heap_insert(starel, newtup);
    }
    CatalogUpdateIndexes(starel, newtup);

    heap_endscan(scan);
    heap_freetuple_ext(newtup);
    heap_close(starel, RowExclusiveLock);
    CommandCounterIncrement();
}

static void ExportTableStats(Oid namespaceid, Oid relid, Oid partid, Oid starelid, bool cascade)
{
    /* If the table doesn't have statistics, not export */
    if (!ExistsStatisticInfo(relid, partid)) {
        return;
    }

    /* 1. export class or partition: pg_class or pg_partition relpages and reptuples */
    if (OidIsValid(partid)) {
        /* We don't handle partition statistic info in pg_statistic now */
        ExportTableClassStats(namespaceid, relid, starelid, partid);
        return;
    } else {
        ExportTableClassStats(namespaceid, relid, starelid, InvalidOid);
    }

    /* 2. export index: pg_statistic info */
    Relation rel = heap_open(relid, AccessShareLock);
    /* Now, we don't handle partition statistic info in pg_statistic */
    char relkind = STARELKIND_CLASS;
    int attnums = RelationGetNumberOfAttributes(rel);
    bool is_index_column[attnums] = {0};
    ListCell* lc = NULL;

    List* indexOids = RelationGetIndexList(rel);
    foreach (lc, indexOids) {
        Oid indexOid = lfirst_oid(lc);
        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
        if (!HeapTupleIsValid(indexTuple)) {
            continue;
        }
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        for (int i = 0; i < indextup->indnatts; i++) {
            is_index_column[indextup->indkey.values[i] - 1] = true;
            if (!cascade) {
                continue;
            }
            ExportColumnStats(namespaceid, relid, starelid, relkind, indextup->indkey.values[i]);
        }
        ReleaseSysCache(indexTuple);
    }
    list_free_ext(indexOids);
    heap_close(rel, AccessShareLock);

    /* 3. export column: pg_statistic info */
    for (int i = 1; i <= attnums; i++) {
        if (!is_index_column[i - 1]) {
            ExportColumnStats(namespaceid, relid, starelid, relkind, i);
        }
    }
}

Datum gs_export_column_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* colname = text_to_cstring(PG_GETARG_TEXT_P(2));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    char* statown = NULL;

    if (PG_ARGISNULL(4)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(4));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    /* Now, we don't handle partition statistic info in pg_statistic */
    char relkind = STARELKIND_CLASS;
    HeapTuple attTup = SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid), NameGetDatum(colname));
    if (HeapTupleIsValid(attTup)) {
        Form_pg_attribute attrtup = (Form_pg_attribute)GETSTRUCT(attTup);
        if (!attrtup->attisdropped) {
            ExportColumnStats(namespaceid, relid, starelid, relkind, attrtup->attnum);
        }
        ReleaseSysCache(attTup);
    } else {
        ereport(ERROR, (errmsg("Column \"%s\" is not exists in relation %s.%s", colname, ownname, tabname)));
    }

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(colname);
    pfree_ext(stattab);

    PG_RETURN_VOID();
}

Datum gs_export_index_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* indname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(2));
    char* statown = NULL;

    if (PG_ARGISNULL(3)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(3));
    }

    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    HeapTuple indrelTup = SearchSysCache2(RELNAMENSP, NameGetDatum(indname), ObjectIdGetDatum(namespaceid));
    if (!HeapTupleIsValid(indrelTup)) {
        ereport(ERROR, (errmsg("Index \"%s\".\"%s\" is not exists", ownname, indname)));
    }
    Oid indexid = HeapTupleGetOid(indrelTup);
    ReleaseSysCache(indrelTup);

    HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexid));
    Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
    Oid relid = indextup->indrelid;
    CheckPermission(relid);

    /* Now, we don't handle partition statistic info in pg_statistic */
    char relkind = STARELKIND_CLASS;
    for (int i = 0; i < indextup->indnatts; i++) {
        ExportColumnStats(namespaceid, relid, starelid, relkind, indextup->indkey.values[i]);
    }
    ReleaseSysCache(indexTuple);

    pfree_ext(ownname);
    pfree_ext(indname);
    pfree_ext(stattab);

    PG_RETURN_VOID();
}

Datum gs_export_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* partname = NULL;
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    char* statown = NULL;
    bool cascade = PG_GETARG_BOOL(5);

    if (PG_ARGISNULL(4)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(4));
    }

    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    Oid partid = InvalidOid;
    if (!PG_ARGISNULL(2)) {
        partname = text_to_cstring(PG_GETARG_TEXT_P(2));
        if (!OidIsValid(partid = GetPartitionOid(relid, partname))) {
            ereport(ERROR, (errmsg("Partition \"%s\" is not exists", partname)));
        }
    }

    ExportTableStats(namespaceid, relid, partid, starelid, cascade);

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);
    pfree_ext(stattab);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_export_schema_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* statown = NULL;

    CheckPermission();

    if (PG_ARGISNULL(2)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    List* relnames = GetRelationsInSchema(ownname);
    ListCell* lc;
    foreach(lc, relnames) {
        char* relname = (char*)lfirst(lc);
        Oid relid = get_relname_relid(relname, namespaceid);
        ExportTableStats(namespaceid, relid, InvalidOid, starelid, true);
    }

    pfree_ext(ownname);
    pfree_ext(stattab);
    pfree_ext(statown);
    list_free(relnames);

    PG_RETURN_VOID();
}

static void ImportStatistic(HeapTuple tuple, TupleDesc tupdesc, Oid relid, char relkind, int16 attnum)
{
    Datum values[Natts_pg_statistic] = {0};
    bool nulls[Natts_pg_statistic] = {false};
    bool replaces[Natts_pg_statistic] = {false};
    bool isNull = true;

    errno_t rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
    replaces[Anum_pg_statistic_starelid - 1] = false;
    values[Anum_pg_statistic_starelkind - 1] = CharGetDatum(relkind);
    replaces[Anum_pg_statistic_starelkind - 1] = false;
    values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
    replaces[Anum_pg_statistic_staattnum - 1] = false;
    values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
    replaces[Anum_pg_statistic_stainherit - 1] = false;
    values[Anum_pg_statistic_stanullfrac - 1] = heap_getattr(tuple, Anum_user_table_stanullfrac, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanullfrac - 1] = isNull;
    values[Anum_pg_statistic_stawidth - 1] = heap_getattr(tuple, Anum_user_table_stawidth, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stawidth - 1] = isNull;
    values[Anum_pg_statistic_stadistinct - 1] = heap_getattr(tuple, Anum_user_table_stadistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stadistinct - 1] = isNull;
    values[Anum_pg_statistic_stakind1 - 1] = heap_getattr(tuple, Anum_user_table_stakind1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind1 - 1] = isNull;
    values[Anum_pg_statistic_stakind2 - 1] = heap_getattr(tuple, Anum_user_table_stakind2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind2 - 1] = isNull;
    values[Anum_pg_statistic_stakind3 - 1] = heap_getattr(tuple, Anum_user_table_stakind3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind3 - 1] = isNull;
    values[Anum_pg_statistic_stakind4 - 1] = heap_getattr(tuple, Anum_user_table_stakind4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind4 - 1] = isNull;
    values[Anum_pg_statistic_stakind5 - 1] = heap_getattr(tuple, Anum_user_table_stakind5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind5 - 1] = isNull;
    values[Anum_pg_statistic_staop1 - 1] = heap_getattr(tuple, Anum_user_table_staop1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop1 - 1] = isNull;
    values[Anum_pg_statistic_staop2 - 1] = heap_getattr(tuple, Anum_user_table_staop2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop2 - 1] = isNull;
    values[Anum_pg_statistic_staop3 - 1] = heap_getattr(tuple, Anum_user_table_staop3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop3 - 1] = isNull;
    values[Anum_pg_statistic_staop4 - 1] = heap_getattr(tuple, Anum_user_table_staop4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop4 - 1] = isNull;
    values[Anum_pg_statistic_staop5 - 1] = heap_getattr(tuple, Anum_user_table_staop5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop5 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers1 - 1] = heap_getattr(tuple, Anum_user_table_stanumbers1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers1 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers2 - 1] = heap_getattr(tuple, Anum_user_table_stanumbers2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers2 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers3 - 1] = heap_getattr(tuple, Anum_user_table_stanumbers3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers3 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers4 - 1] = heap_getattr(tuple, Anum_user_table_stanumbers4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers4 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers5 - 1] = heap_getattr(tuple, Anum_user_table_stanumbers5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers5 - 1] = isNull;
    values[Anum_pg_statistic_stavalues1 - 1] = heap_getattr(tuple, Anum_user_table_stavalues1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues1 - 1] = isNull;
    values[Anum_pg_statistic_stavalues2 - 1] = heap_getattr(tuple, Anum_user_table_stavalues2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues2 - 1] = isNull;
    values[Anum_pg_statistic_stavalues3 - 1] = heap_getattr(tuple, Anum_user_table_stavalues3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues3 - 1] = isNull;
    values[Anum_pg_statistic_stavalues4 - 1] = heap_getattr(tuple, Anum_user_table_stavalues4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues4 - 1] = isNull;
    values[Anum_pg_statistic_stavalues5 - 1] = heap_getattr(tuple, Anum_user_table_stavalues5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues5 - 1] = isNull;
    values[Anum_pg_statistic_stadndistinct - 1] = heap_getattr(tuple, Anum_user_table_stadndistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stadndistinct - 1] = isNull;
    values[Anum_pg_statistic_staextinfo - 1] = heap_getattr(tuple, Anum_user_table_staextinfo, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staextinfo - 1] = isNull;

    UpdateStatsiticInfo(relid, relkind, attnum, values, nulls, replaces);
}

static void ImportColumnStats(Oid namespaceid, Oid relid, int16 attnum, Oid starelid, char relkind, TimestampTz currentAnalyzetime)
{
    if (!CheckColumnIsValid(relid, attnum)) {
        return;
    }

    Relation starel = NULL;
    ScanKeyData skey[4];
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    TupleDesc tupdesc = NULL;

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STATYPE_COLUMN));
    ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[3], Anum_user_table_staattnum, BTEqualStrategyNumber, F_INT2EQ,
        Int16GetDatum(attnum));

    starel = relation_open(starelid, AccessShareLock);
    scan = heap_beginscan(starel, SnapshotNow, 4, skey);
    tupdesc = RelationGetDescr(starel);
    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        ImportStatistic(tuple, tupdesc, relid, relkind, attnum);
        /* import statistic should insert entries into pg_statistic_history */
        ImportColumnStatisticHistory(tuple, tupdesc, namespaceid, relid, relkind, attnum, currentAnalyzetime);
    }
    heap_endscan(scan);
    relation_close(starel, AccessShareLock);
}

static void UpdatePartitionStats(Oid namespaceid, Oid relid, Oid starelid, Oid partid)
{
    Relation starel = NULL;
    ScanKeyData skey[3];
    HeapTuple tuple;
    TableScanDesc scan = NULL;
    bool isNull;
    double numpages = 0;
    double numtuples = 0;

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STATYPE_PARTITION));
    ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));

    starel = relation_open(starelid, AccessShareLock);
    scan = heap_beginscan(starel, SnapshotNow, 3, skey);
    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        Datum val = heap_getattr(tuple, Anum_user_table_partid, RelationGetDescr(starel), &isNull);
        if (DatumGetObjectId(val) != partid) {
            continue;
        }

        Datum tuplesVal = heap_getattr(tuple, Anum_user_table_reltuples, RelationGetDescr(starel), &isNull);
        if (!isNull) {
            numtuples = DatumGetFloat8(tuplesVal);
        }
        Datum pagesVal = heap_getattr(tuple, Anum_user_table_relpages, RelationGetDescr(starel), &isNull);
        if (!isNull) {
            numpages = DatumGetFloat8(pagesVal);
        }
    }
    heap_endscan(scan);
    heap_close(starel, AccessShareLock);

    UpdatePartitionStatsInfo(partid, numpages, numtuples);

    InsertPartitionStatisticHistory(relid, partid, numpages, numtuples);
}

static void UpdateTableStats(Oid namespaceid, Oid relid, Oid starelid)
{
    Relation starel = NULL;
    ScanKeyData skey[3];
    HeapTuple tuple = NULL;
    TableScanDesc scan = NULL;
    bool isNull;
    double numpages = 0;
    double numtuples = 0;

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STATYPE_RELATION));
    ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));

    starel = relation_open(starelid, AccessShareLock);
    scan = heap_beginscan(starel, SnapshotNow, 3, skey);
    if (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        Datum tuplesVal = heap_getattr(tuple, Anum_user_table_reltuples, RelationGetDescr(starel), &isNull);
        if (!isNull) {
            numtuples = DatumGetFloat8(tuplesVal);
        }
        Datum pagesVal = heap_getattr(tuple, Anum_user_table_relpages, RelationGetDescr(starel), &isNull);
        if (!isNull) {
            numpages = DatumGetFloat8(pagesVal);
        }
    }
    heap_endscan(scan);
    heap_close(starel, AccessShareLock);

    UpdateTableStatsInfo(relid, numpages, numtuples);

    InsertClassStatisHistory(relid, numpages, numtuples);
}

static void ImportTableStats(Oid namespaceid, Oid relid, Oid partid, Oid starelid,
                            bool cascade, TimestampTz currentAnalyzetime)
{
    /* 1. import class or partition: pg_class or pg_partition relpages and reptuples */
    if (OidIsValid(partid)) {
        /* We don't handle partition statistic info in pg_statistic now */
        UpdatePartitionStats(namespaceid, relid, starelid, partid);
        return;
    } else {
        UpdateTableStats(namespaceid, relid, starelid);
    }

    /* 2. import index: pg_statistic info */
    Relation rel = heap_open(relid, AccessShareLock);
    /* Now, we don't handle partition statistic info in pg_statistic */
    char relkind = STARELKIND_CLASS;
    int attnums = RelationGetNumberOfAttributes(rel);
    bool is_index_column[attnums] = {0};
    ListCell* lc = NULL;

    List* indexOids = RelationGetIndexList(rel);
    foreach (lc, indexOids) {
        Oid indexOid = lfirst_oid(lc);
        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
        if (!HeapTupleIsValid(indexTuple)) {
            continue;
        }
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        for (int i = 0; i < indextup->indnatts; i++) {
            is_index_column[indextup->indkey.values[i] - 1] = true;
            if (!cascade) {
                continue;
            }
            ImportColumnStats(namespaceid, relid, indextup->indkey.values[i], starelid, relkind, currentAnalyzetime);
        }
        ReleaseSysCache(indexTuple);
    }
    list_free_ext(indexOids);
    heap_close(rel, AccessShareLock);

    /* 3. import column: pg_statistic info */
    for (int i = 1; i <= attnums; i++) {
        if (!is_index_column[i - 1]) {
            ImportColumnStats(namespaceid, relid, i, starelid, relkind, currentAnalyzetime);
        }
    }
}

Datum gs_import_column_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* colname = text_to_cstring(PG_GETARG_TEXT_P(2));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(5);

    if (PG_ARGISNULL(4)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    /* if force, don't check lock info */
    if (!force && CheckRelationLocked(namespaceid, relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
    }

    /* Now, we don't handle partition statistic info in pg_statistic */
    char relkind = STARELKIND_CLASS;
    HeapTuple attTup = SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid), NameGetDatum(colname));
    if (HeapTupleIsValid(attTup)) {
        Form_pg_attribute attrtup = (Form_pg_attribute)GETSTRUCT(attTup);
        ImportColumnStats(namespaceid, relid, attrtup->attnum, starelid, relkind, GetCurrentTimestamp());
        ReleaseSysCache(attTup);
    } else {
        ereport(ERROR, (errmsg("Column \"%s\" is not exists in relation %s.%s", colname, ownname, tabname)));
    }

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(colname);
    pfree_ext(stattab);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_import_index_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* indname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(2));
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(4);

    if (PG_ARGISNULL(3)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    HeapTuple tup = SearchSysCache2(RELNAMENSP, NameGetDatum(indname), ObjectIdGetDatum(namespaceid));
    if (HeapTupleIsValid(tup)) {
        Oid indexid = HeapTupleGetOid(tup);
        /* Now, we don't handle partition statistic info in pg_statistic */
        char relkind = STARELKIND_CLASS;
        ReleaseSysCache(tup);

        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexid));
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        Oid indrelid = indextup->indrelid;

        CheckPermission(indrelid);

        /* if force, don't check lock info */
        if (!force && CheckRelationLocked(namespaceid, indrelid)) {
            ReleaseSysCache(indexTuple);
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, get_rel_name(indrelid))));
        }

        TimestampTz time = GetCurrentTimestamp();
        for (int i = 0; i < indextup->indnatts; i++) {
            ImportColumnStats(namespaceid, indrelid, indextup->indkey.values[i], starelid, relkind, time);
        }
        ReleaseSysCache(indexTuple);
    } else {
        ereport(ERROR, (errmsg("Index \"%s\".\"%s\" is not exists", ownname, indname)));
    }

    pfree_ext(ownname);
    pfree_ext(indname);
    pfree_ext(stattab);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_import_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* partname = NULL;
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    bool cascade = PG_GETARG_BOOL(4);
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(6);

    if (PG_ARGISNULL(5)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(6));
    }
    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    Oid partid = InvalidOid;
    if (PG_ARGISNULL(2)) {
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
        }
    } else {
        partname = text_to_cstring(PG_GETARG_TEXT_P(2));
        if (!OidIsValid(partid = GetPartitionOid(relid, partname))) {
            ereport(ERROR, (errmsg("Partition \"%s\" is not exists", partname)));
        }
        if (!force && CheckRelationLocked(namespaceid, relid, partid)) {
            ereport(ERROR, (errmsg("Partition \"%s\" has been locked", partname)));
        }
    }

    ImportTableStats(namespaceid, relid, partid, starelid, cascade, GetCurrentTimestamp());

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);
    pfree_ext(stattab);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_import_schema_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* stattab = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(3);

    CheckPermission();

    if (PG_ARGISNULL(2)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(2));
    }

    if (!((pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()))
        || (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Permission denied. Need dba or databaseowner to call.")));
    }

    Oid starelid = get_relname_relid(stattab, get_namespace_oid(statown, false));
    if (!OidIsValid(starelid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", statown, stattab)));
    }

    Oid namespaceid = get_namespace_oid(ownname, false);
    List* relnames = GetRelationsInSchema(ownname);

    ListCell* lc;
    foreach(lc, relnames) {
        char* relname = (char*)lfirst(lc);
        Oid relid = get_relname_relid(relname, namespaceid);
        TimestampTz time = GetCurrentTimestamp();
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            continue;
        }
        ImportTableStats(namespaceid, relid, InvalidOid, starelid, true, time);
    }

    pfree_ext(ownname);
    pfree_ext(stattab);
    pfree_ext(statown);
    list_free(relnames);

    PG_RETURN_VOID();
}

static void DeleteColumnStats(Oid namespaceid, Oid relid, int16 attnum, Oid starelid, char relkind)
{
    /* delete user table statistic info */
    if (OidIsValid(starelid)) {
        HeapTuple tuple = NULL;
        ScanKeyData skey[4];
        Relation starel = NULL;
        TableScanDesc scan = NULL;

        ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceid));
        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(STATYPE_COLUMN));
        ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
        ScanKeyInit(&skey[3], Anum_user_table_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

        starel = heap_open(starelid, RowExclusiveLock);
        scan = heap_beginscan(starel, SnapshotNow, 4, skey);
        while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            simple_heap_delete(starel, &tuple->t_self);
        }
        heap_endscan(scan);
        heap_close(starel, RowExclusiveLock);
    }
    /* delete pg_statistic info */
    else {
        Relation pgstatistic = NULL;
        SysScanDesc scan = NULL;
        ScanKeyData key[3];
        HeapTuple tuple = NULL;

        ScanKeyInit(&key[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
        ScanKeyInit(&key[1], Anum_pg_statistic_starelkind, BTEqualStrategyNumber, F_CHAREQ, ObjectIdGetDatum(relkind));
        ScanKeyInit(&key[2], Anum_pg_statistic_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

        pgstatistic = heap_open(StatisticRelationId, RowExclusiveLock);
        scan = systable_beginscan(pgstatistic, StatisticRelidKindAttnumInhIndexId, true, NULL, 3, key);
        while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
            simple_heap_delete(pgstatistic, &tuple->t_self);
        }
        systable_endscan(scan);
        heap_close(pgstatistic, RowExclusiveLock);
    }
    CommandCounterIncrement();
}

static void RemovePartitionStats(Oid namespaceid, Oid relid, Oid starelid, Oid partid)
{
    /* remove user table info */
    if (OidIsValid(starelid)) {
        Relation rel = NULL;
        ScanKeyData skey[3];
        HeapTuple tuple = NULL;
        TableScanDesc scan = NULL;
        bool isNull = true;  

        ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(namespaceid));
        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
            CharGetDatum(STATYPE_PARTITION));
        ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(relid));

        rel = relation_open(starelid, RowExclusiveLock);
        scan = heap_beginscan(rel, SnapshotNow, 3, skey);
        while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            Datum val = heap_getattr(tuple, Anum_user_table_partid, RelationGetDescr(rel), &isNull);
            if (isNull || partid != DatumGetObjectId(val)) {
                continue;
            }
            simple_heap_delete(rel, &tuple->t_self);
        }
        heap_endscan(scan);
        relation_close(rel, RowExclusiveLock);
        CommandCounterIncrement();
    }
    /* update pg_partition reltuples and relpages to 0 */
    else {
        UpdatePartitionStatsInfo(partid, 0, 0);
    }
}

static void RemoveTableStats(Oid namespaceid, Oid relid, Oid starelid, bool cascadePart)
{
    /* remove user table info */
    if (OidIsValid(starelid)) {
        Relation rel = NULL;
        ScanKeyData skey[3];
        HeapTuple tuple = NULL;
        TableScanDesc scan = NULL;  

        ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(namespaceid));
        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
            CharGetDatum(STATYPE_RELATION));
        ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ,
            ObjectIdGetDatum(relid));

        rel = relation_open(starelid, RowExclusiveLock);
        scan = heap_beginscan(rel, SnapshotNow, 3, skey);
        while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            simple_heap_delete(rel, &tuple->t_self);
        }
        heap_endscan(scan);
        scan = NULL;

        /* do delete partitioin info */
        if (!cascadePart) {
            relation_close(rel, RowExclusiveLock);
            return;
        }

        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ,
            CharGetDatum(STATYPE_PARTITION));
        scan = heap_beginscan(rel, SnapshotNow, 3, skey);
        while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            simple_heap_delete(rel, &tuple->t_self);
        }
        heap_endscan(scan);
        relation_close(rel, RowExclusiveLock);
        CommandCounterIncrement();
    }
    /* update pg_class and pg_partition reltuples and relpages to 0 */
    else {
        UpdateTableStatsInfo(relid, 0, 0);

        /* do drop partition info */
        if (!cascadePart) {
            return;
        }

        /* update partition */
        UpdatePartitionStatsInfo(relid, PART_OBJ_TYPE_TABLE_PARTITION, 0, 0);

        /* update subpartition */
        Relation rel = relation_open(relid, AccessShareLock);
        List* partids = relationGetPartitionOidList(rel);
        heap_close(rel, AccessShareLock);
        ListCell* lc = NULL;
        Oid partid = InvalidOid;
        if (partids == NIL || list_length(partids) == 0) {
            return;
        }
        foreach(lc, partids) {
            partid = lfirst_oid(lc);
            UpdatePartitionStatsInfo(partid, PART_OBJ_TYPE_TABLE_SUB_PARTITION, 0, 0);
        }
        list_free(partids);
    }
}

static void DeleteTableStats(Oid namespaceid, Oid relid, Oid partid, Oid starelid,
                            bool cascadePart, bool cascadeColumn, bool cascadeIndex)
{
    /* 1. delete pg_class or pg_partition reltuples and relpages info */
    if (OidIsValid(partid)) {
        /* If is partition, only delete pg_partition info now, do not delete pg_statistic info */
        RemovePartitionStats(namespaceid, relid, starelid, partid);
        return;
    } else {
        RemoveTableStats(namespaceid, relid, starelid, cascadePart);
    }

    /* 2. delete index statistic info */
    Relation rel = heap_open(relid, AccessShareLock);
    int attnums = RelationGetNumberOfAttributes(rel);
    bool is_index_column[attnums] = {0};
    ListCell* lc;
    List* indexOids = RelationGetIndexList(rel);
    heap_close(rel, AccessShareLock);

    foreach (lc, indexOids) {
        Oid indexOid = lfirst_oid(lc);
        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
        if (!HeapTupleIsValid(indexTuple)) {
            continue;
        }
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        for (int i = 0; i < indextup->indnatts; i++) {
            is_index_column[indextup->indkey.values[i] - 1] = true;
            if (cascadeIndex) {
                DeleteColumnStats(namespaceid, relid, indextup->indkey.values[i], starelid, STARELKIND_CLASS);
            }
        }
        ReleaseSysCache(indexTuple);
    }
    list_free_ext(indexOids);

    /* 3. delete column statistic info */
    if (cascadeColumn) {
        for (int i = 1; i <= attnums; i++) {
            if (!is_index_column[i - 1]) {
                DeleteColumnStats(namespaceid, relid, i, starelid, STARELKIND_CLASS);
            }
        }
    }
}

static void DeleteUserTableSchemaStats(Oid namespaceid, Oid starelid, bool force)
{
    Relation rel = NULL;
    ScanKeyData skey[1];
    HeapTuple tuple = NULL;
    TableScanDesc scan = NULL;
    bool isNull = true;
    Oid relid = InvalidOid;

    ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));

    rel = relation_open(starelid, RowExclusiveLock);
    scan = heap_beginscan(rel, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        relid = DatumGetObjectId(heap_getattr(tuple, Anum_user_table_starelid, RelationGetDescr(rel), &isNull));
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            continue;
        }
        simple_heap_delete(rel, &tuple->t_self);
    }
    heap_endscan(scan);
    relation_close(rel, RowExclusiveLock);
}

Datum gs_delete_column_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* colname = text_to_cstring(PG_GETARG_TEXT_P(2));
    bool force = PG_GETARG_BOOL(6);
    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;

    if (!PG_ARGISNULL(3)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!PG_ARGISNULL(4)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    if (PG_ARGISNULL(5)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(5));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation %s.%s is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    /* if force, don't check lock info */
    if (!force && CheckRelationLocked(namespaceid, relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
    }

    HeapTuple attTup = SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid), NameGetDatum(colname));
    if (HeapTupleIsValid(attTup)) {
        Form_pg_attribute attrtup = (Form_pg_attribute)GETSTRUCT(attTup);
        DeleteColumnStats(namespaceid, relid, attrtup->attnum, starelid, STARELKIND_CLASS);
        ReleaseSysCache(attTup);
    } else {
        ereport(ERROR, (errmsg("Column \"%s\" is not exists in relation %s.%s", colname, ownname, tabname)));
    }

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(colname);
    pfree_ext(stattab);

    PG_RETURN_VOID();
}

Datum gs_delete_index_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* indname = text_to_cstring(PG_GETARG_TEXT_P(1));
    bool force = PG_GETARG_BOOL(5);
    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;

    if (!PG_ARGISNULL(2)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    if (!PG_ARGISNULL(3)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (PG_ARGISNULL(4)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(4));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    HeapTuple tup = SearchSysCache2(RELNAMENSP, NameGetDatum(indname), ObjectIdGetDatum(namespaceid));
    if (HeapTupleIsValid(tup)) {
        Oid indexid = HeapTupleGetOid(tup);
        ReleaseSysCache(tup);

        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexid));
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        Oid indrelid = indextup->indrelid;
        CheckPermission(indrelid);

        /* if force, don't check lock info */
        if (!force && CheckRelationLocked(namespaceid, indrelid)) {
            ReleaseSysCache(indexTuple);
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, get_rel_name(indrelid))));
        }

        for (int i = 0; i < indextup->indnatts; i++) {
            DeleteColumnStats(namespaceid, indrelid, indextup->indkey.values[i], starelid, STARELKIND_CLASS);
        }

        ReleaseSysCache(indexTuple);
    } else {
        ereport(ERROR, (errmsg("Index \"%s\".\"%s\" not exists", ownname, indname)));
    }

    pfree_ext(ownname);
    pfree_ext(indname);
    pfree_ext(stattab);

    PG_RETURN_VOID();
}

Datum gs_delete_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* partname = NULL;
    char* stattab = NULL;
    char* statid = NULL;
    bool cascadePart = PG_GETARG_BOOL(5);
    bool cascadeColumn = PG_GETARG_BOOL(6);
    bool cascadeIndex = PG_GETARG_BOOL(7);
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(9);

    if (!PG_ARGISNULL(3)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!PG_ARGISNULL(4)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    if (PG_ARGISNULL(8)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(8));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    Oid partid = InvalidOid;
    if (PG_ARGISNULL(2)) {
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
        }
    } else {
        partname = text_to_cstring(PG_GETARG_TEXT_P(2));
        if (!OidIsValid(partid = GetPartitionOid(relid, partname))) {
            ereport(ERROR, (errmsg("Partition \"%s\" is not exists", partname)));
        }
        if (!force && CheckRelationLocked(namespaceid, relid, partid)) {
            ereport(ERROR, (errmsg("Partition \"%s\" has been locked", partname)));
        }
    }

    DeleteTableStats(namespaceid, relid, partid, starelid, cascadePart, cascadeColumn, cascadeIndex);

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_delete_schema_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    bool force = PG_GETARG_BOOL(4);
    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;

    CheckPermission();

    if (!PG_ARGISNULL(1)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(1));
    }
    if (!PG_ARGISNULL(2)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    if (PG_ARGISNULL(3)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(3));
    }

    Oid namespaceId = get_namespace_oid(ownname, false);
    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    if (OidIsValid(starelid)) {
        DeleteUserTableSchemaStats(namespaceId, starelid, force);
    } else {
        List* relnames = GetRelationsInSchema(ownname);
        ListCell* lc;
        foreach(lc, relnames) {
            char* relname = (char*)lfirst(lc);
            Oid relid = get_relname_relid(relname, namespaceId);
            if (!force && CheckRelationLocked(namespaceId, relid)) {
                continue;
            }
            DeleteTableStats(namespaceId, relid, InvalidOid, InvalidOid, true, true, true);
        }
        list_free_ext(relnames);
    }

    pfree_ext(ownname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

static void SetColumnStats(Oid namespaceid, Oid relid, int16 attnum, Oid starelid, char relkind,
                            float distcnt, bool distcntNull, float nullcnt, bool nullcntNull)
{
    /* set user table statistic info */
    if (OidIsValid(starelid)) {
        HeapTuple tuple = NULL;
        HeapTuple newtup = NULL;
        ScanKeyData skey[4];
        Relation starel = NULL;
        TableScanDesc scan = NULL;
        Datum values[Natts_user_table] = {0};
        bool nulls[Natts_user_table];
        bool replaces[Natts_user_table] = {0};

        errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (!distcntNull) {
            values[Anum_user_table_stadistinct - 1] = Float4GetDatum(distcnt);
            nulls[Anum_user_table_stadistinct - 1] = false;
            replaces[Anum_user_table_stadistinct - 1] = true;
        }
        if (!nullcntNull) {
            values[Anum_user_table_stanullfrac - 1] = Float4GetDatum(nullcnt);
            nulls[Anum_user_table_stanullfrac - 1] = false;
            replaces[Anum_user_table_stanullfrac - 1] = true;
        }

        ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceid));
        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(STATYPE_COLUMN));
        ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
        ScanKeyInit(&skey[3], Anum_user_table_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

        starel = heap_open(starelid, RowExclusiveLock);
        scan = heap_beginscan(starel, SnapshotNow, 4, skey);
        if (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            newtup = heap_modify_tuple(tuple, RelationGetDescr(starel), values, nulls, replaces);
            simple_heap_update(starel, &newtup->t_self, newtup);
            CatalogUpdateIndexes(starel, newtup);
            heap_freetuple_ext(newtup);
        }
        heap_endscan(scan);
        heap_close(starel, RowExclusiveLock);
    }
    /* set pg_statistic info */
    else {
        Relation pgstatistic = NULL;
        SysScanDesc scan = NULL;
        ScanKeyData key[3];
        HeapTuple tuple = NULL;
        HeapTuple newtup = NULL;
        Datum values[Natts_pg_statistic] = {0};
        bool nulls[Natts_pg_statistic];
        bool replaces[Natts_pg_statistic] = {0};

        errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (!distcntNull) {
            values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(distcnt);
            nulls[Anum_pg_statistic_stadistinct - 1] = false;
            replaces[Anum_pg_statistic_stadistinct - 1] = true;
        }
        if (!nullcntNull) {
            values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(nullcnt);
            nulls[Anum_pg_statistic_stanullfrac - 1] = false;
            replaces[Anum_pg_statistic_stanullfrac - 1] = true;
        }

        ScanKeyInit(&key[0], Anum_pg_statistic_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
        ScanKeyInit(&key[1], Anum_pg_statistic_starelkind, BTEqualStrategyNumber, F_CHAREQ, ObjectIdGetDatum(relkind));
        ScanKeyInit(&key[2], Anum_pg_statistic_staattnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

        pgstatistic = heap_open(StatisticRelationId, RowExclusiveLock);
        scan = systable_beginscan(pgstatistic, StatisticRelidKindAttnumInhIndexId, true, NULL, 3, key);
        if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
            newtup = heap_modify_tuple(tuple, RelationGetDescr(pgstatistic), values, nulls, replaces);
            simple_heap_update(pgstatistic, &newtup->t_self, newtup);
            CatalogUpdateIndexes(pgstatistic, newtup);
            heap_freetuple_ext(newtup);
        }
        systable_endscan(scan);
        heap_close(pgstatistic, RowExclusiveLock);
    }
}

static void SetTableStats(Oid namespaceid, Oid relid, Oid partid, Oid starelid,
                double numtuples, bool tupleNull, double numpages, bool pageNull)
{
    /* set user table statistic info */
    if (OidIsValid(starelid)) {
        HeapTuple tuple = NULL;
        HeapTuple newtup = NULL;
        ScanKeyData skey[3];
        Relation starel = NULL;
        TableScanDesc scan = NULL;
        char stattype = OidIsValid(partid) ? STATYPE_PARTITION : STATYPE_RELATION;
        Datum values[Natts_user_table] = {0};
        bool nulls[Natts_user_table];
        bool replaces[Natts_user_table] = {0};
        bool isNull = true;

        errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (!tupleNull) {
            values[Anum_user_table_reltuples - 1] = Float8GetDatum(numtuples);
            nulls[Anum_user_table_reltuples - 1] = false;
            replaces[Anum_user_table_reltuples - 1] = true;
        }
        if (!pageNull) {
            values[Anum_user_table_relpages - 1] = Float8GetDatum(numpages);
            nulls[Anum_user_table_relpages - 1] = false;
            replaces[Anum_user_table_relpages - 1] = true;
        }

        ScanKeyInit(&skey[0], Anum_user_table_namespaceid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceid));
        ScanKeyInit(&skey[1], Anum_user_table_statype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(stattype));
        ScanKeyInit(&skey[2], Anum_user_table_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

        starel = heap_open(starelid, RowExclusiveLock);
        scan = heap_beginscan(starel, SnapshotNow, 3, skey);
        while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
            if (stattype == STATYPE_PARTITION) {
                Oid pid = DatumGetObjectId(heap_getattr(tuple, Anum_user_table_partid, RelationGetDescr(starel), &isNull));
                if (pid != partid) {
                    continue;
                }
            }
            newtup = heap_modify_tuple(tuple, RelationGetDescr(starel), values, nulls, replaces);
            simple_heap_update(starel, &newtup->t_self, newtup);
            CatalogUpdateIndexes(starel, newtup);
            heap_freetuple_ext(newtup);
            break;
        }
        heap_endscan(scan);
        heap_close(starel, RowExclusiveLock);  
    }
    /* set pg_statistic info */
    else {
        bool isNull = true;
        HeapTuple tuple = NULL;
        double reltuples = 0;
        double relpages = 0;
        if (OidIsValid(partid)) {
            tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
            Assert(HeapTupleIsValid(tuple));
            reltuples = DatumGetFloat8(SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_reltuples, &isNull));
            relpages = DatumGetFloat8(SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_relpages, &isNull));
            ReleaseSysCache(tuple);

            if (!tupleNull) {
                reltuples = numtuples;
            }
            if (!pageNull) {
                relpages = numpages;
            }
            UpdatePartitionStatsInfo(partid, relpages, reltuples);
        } else {
            tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
            Assert(HeapTupleIsValid(tuple));
            reltuples = DatumGetFloat8(SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reltuples, &isNull));
            relpages = DatumGetFloat8(SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relpages, &isNull));
            ReleaseSysCache(tuple);

            if (!tupleNull) {
                reltuples = numtuples;
            }
            if (!pageNull) {
                relpages = numpages;
            }
            UpdateTableStatsInfo(relid, relpages, reltuples);
        }
    }
}

Datum gs_set_column_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* colname = text_to_cstring(PG_GETARG_TEXT_P(2));
    char* stattab = NULL;
    char* statid = NULL;
    float distcntNum = 0;
    float nullcntNum = 0;
    char* statown = NULL;
    bool force = PG_GETARG_BOOL(8);
    bool distcntNull = PG_ARGISNULL(5);
    bool nullcntNull = PG_ARGISNULL(6);

    if (!PG_ARGISNULL(3)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!PG_ARGISNULL(4)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    if (PG_ARGISNULL(7)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(7));
    }
    if (!distcntNull) {
        distcntNum = DatumGetFloat4(DirectFunctionCall1(numeric_float4, PG_GETARG_DATUM(5)));
    }
    if (!nullcntNull) {
        nullcntNum = DatumGetFloat4(DirectFunctionCall1(numeric_float4, PG_GETARG_DATUM(6)));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    /* if force, don't check lock info */
    if (!force && CheckRelationLocked(namespaceid, relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
    }

    HeapTuple tup = SearchSysCache2(ATTNAME, ObjectIdGetDatum(relid), NameGetDatum(colname));
    if (HeapTupleIsValid(tup)) {
        Form_pg_attribute attrtup = (Form_pg_attribute)GETSTRUCT(tup);
        if (!distcntNull || !nullcntNull) {
            SetColumnStats(namespaceid, relid, attrtup->attnum, starelid, STARELKIND_CLASS,
                            distcntNum, distcntNull, nullcntNum, nullcntNull);
        }

        ReleaseSysCache(tup);
    } else {
        ereport(ERROR, (errmsg("Column \"%s\" is not exists in relation %s.%s", colname, ownname, tabname)));
    }

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(colname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_set_index_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* indname = text_to_cstring(PG_GETARG_TEXT_P(1));
    bool force = PG_GETARG_BOOL(6);
    char* stattab = NULL;
    char* statid = NULL;
    float numdist;
    char* statown = NULL;
    HeapTuple tup = NULL;
    bool numdistNull = PG_ARGISNULL(4);

    if (!PG_ARGISNULL(2)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    if (!PG_ARGISNULL(3)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!numdistNull) {
        numdist = DatumGetFloat4(DirectFunctionCall1(numeric_float4, PG_GETARG_DATUM(4)));
    }
    if (PG_ARGISNULL(5)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(5));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    tup = SearchSysCache2(RELNAMENSP, NameGetDatum(indname), ObjectIdGetDatum(namespaceid));
    if (HeapTupleIsValid(tup)) {
        Oid indexid = HeapTupleGetOid(tup);
        ReleaseSysCache(tup);

        HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexid));
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(indexTuple);
        Oid indrelid = indextup->indrelid;
        CheckPermission(indrelid);

        if (!force && CheckRelationLocked(namespaceid, indrelid)) {
            char* tabname = get_rel_name(indrelid);
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
        }

        for (int i = 0; i < indextup->indnatts; i++) {
            if (!numdistNull) {
                SetColumnStats(namespaceid, indrelid, indextup->indkey.values[i], starelid, STARELKIND_CLASS,
                                numdist, numdistNull, 0, true);
            }
        }

        ReleaseSysCache(indexTuple);
    } else {
        ereport(ERROR, (errmsg("Index \"%s\".\"%s\" is not exists", ownname, indname)));
    }

    pfree_ext(ownname);
    pfree_ext(indname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_set_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    bool force = PG_GETARG_BOOL(8);
    char* partname = NULL;
    char* stattab = NULL;
    char* statid = NULL;
    double numrows = 0;
    double numblks = 0;
    char* statown = NULL;
    bool rowsNull = PG_ARGISNULL(5);
    bool numblksNull = PG_ARGISNULL(6);

    if (!PG_ARGISNULL(3)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!PG_ARGISNULL(4)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    if (!rowsNull) {
        numrows = DatumGetFloat8(DirectFunctionCall1(numeric_float8, PG_GETARG_DATUM(5)));
    }
    if (!numblksNull) {
        numblks = DatumGetFloat8(DirectFunctionCall1(numeric_float8, PG_GETARG_DATUM(6)));
    }
    if (PG_ARGISNULL(7)) {
        statown = pstrdup(ownname);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(7));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    Oid partid = InvalidOid;
    if (PG_ARGISNULL(2)) {
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
        }
    } else {
        partname = text_to_cstring(PG_GETARG_TEXT_P(2));
        if (!OidIsValid(partid = GetPartitionOid(relid, partname))) {
            ereport(ERROR, (errmsg("Partition \"%s\" is not exists", partname)));
        }
        if (!force && CheckRelationLocked(namespaceid, relid, partid)) {
            ereport(ERROR, (errmsg("Partition \"%s\" has been locked", partname)));
        }
    }

    if (!rowsNull || !numblksNull) {
        SetTableStats(namespaceid, relid, partid, starelid, numrows, rowsNull, numblks, numblksNull);
    }

    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

static void RestoreColumnStats(HeapTuple tuple, TupleDesc tupdesc, Oid relid, char relkind, int16 attnum)
{
    Datum values[Natts_pg_statistic] = {0};
    bool nulls[Natts_pg_statistic] = {false};
    bool replaces[Natts_pg_statistic] = {false};
    bool isNull = true;

    errno_t rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
    replaces[Anum_pg_statistic_starelid - 1] = false;
    values[Anum_pg_statistic_starelkind - 1] = CharGetDatum(relkind);
    replaces[Anum_pg_statistic_starelkind - 1] = false;
    values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
    replaces[Anum_pg_statistic_staattnum - 1] = false;
    values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
    replaces[Anum_pg_statistic_stainherit - 1] = false;
    values[Anum_pg_statistic_stanullfrac - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanullfrac, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanullfrac - 1] = isNull;
    values[Anum_pg_statistic_stawidth - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stawidth, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stawidth - 1] = isNull;
    values[Anum_pg_statistic_stadistinct - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stadistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stadistinct - 1] = isNull;
    values[Anum_pg_statistic_stakind1 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stakind1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind1 - 1] = isNull;
    values[Anum_pg_statistic_stakind2 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stakind2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind2 - 1] = isNull;
    values[Anum_pg_statistic_stakind3 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stakind3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind3 - 1] = isNull;
    values[Anum_pg_statistic_stakind4 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stakind4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind4 - 1] = isNull;
    values[Anum_pg_statistic_stakind5 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stakind5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stakind5 - 1] = isNull;
    values[Anum_pg_statistic_staop1 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staop1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop1 - 1] = isNull;
    values[Anum_pg_statistic_staop2 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staop2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop2 - 1] = isNull;
    values[Anum_pg_statistic_staop3 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staop3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop3 - 1] = isNull;
    values[Anum_pg_statistic_staop4 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staop4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop4 - 1] = isNull;
    values[Anum_pg_statistic_staop5 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staop5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staop5 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers1 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanumbers1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers1 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers2 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanumbers2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers2 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers3 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanumbers3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers3 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers4 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanumbers4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers4 - 1] = isNull;
    values[Anum_pg_statistic_stanumbers5 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stanumbers5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stanumbers5 - 1] = isNull;
    values[Anum_pg_statistic_stavalues1 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stavalues1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues1 - 1] = isNull;
    values[Anum_pg_statistic_stavalues2 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stavalues2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues2 - 1] = isNull;
    values[Anum_pg_statistic_stavalues3 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stavalues3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues3 - 1] = isNull;
    values[Anum_pg_statistic_stavalues4 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stavalues4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues4 - 1] = isNull;
    values[Anum_pg_statistic_stavalues5 - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stavalues5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stavalues5 - 1] = isNull;
    values[Anum_pg_statistic_stadndistinct - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_stadndistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_stadndistinct - 1] = isNull;
    values[Anum_pg_statistic_staextinfo - 1] =
                heap_getattr(tuple, Anum_pg_statistic_history_staextinfo, tupdesc, &isNull);
    nulls[Anum_pg_statistic_staextinfo - 1] = isNull;

    UpdateStatsiticInfo(relid, relkind, attnum, values, nulls, replaces);
}

static void RestoreStats(Oid namespaceid, Oid relid, Oid partid, TimestampTz asOfTime)
{
    Relation pgstahis = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[2];
    HeapTuple tuple = NULL;
    bool isNull = true;

    TimestampTz recentTime = GetRecentAnalyzeTime(asOfTime, relid, '\0');

    ScanKeyInit(&key[0], Anum_pg_statistic_history_current_analyzetime, BTEqualStrategyNumber, F_TIMESTAMP_EQ,
                TimestampTzGetDatum(recentTime));
    ScanKeyInit(&key[1], Anum_pg_statistic_history_starelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    pgstahis = relation_open(StatisticHistoryRelationId, AccessShareLock);
    scan = systable_beginscan(pgstahis, StatisticHistoryCurrTimeRelidIndexId, true, SnapshotNow, 2, key);
    while(HeapTupleIsValid(tuple = systable_getnext(scan))) {
        char statype = DatumGetChar(heap_getattr(tuple, Anum_pg_statistic_history_statype,
                                    pgstahis->rd_att, &isNull));
        double relpages = DatumGetFloat8(heap_getattr(tuple, Anum_pg_statistic_history_relpages,
                                    pgstahis->rd_att, &isNull));
        double reltuples = DatumGetFloat8(heap_getattr(tuple, Anum_pg_statistic_history_reltuples,
                                    pgstahis->rd_att, &isNull));

        if (statype == STATYPE_COLUMN) {
            int16 attnum = DatumGetInt16(heap_getattr(tuple, Anum_pg_statistic_history_staattnum,
                                        pgstahis->rd_att, &isNull));
            RestoreColumnStats(tuple, pgstahis->rd_att, relid, STARELKIND_CLASS, attnum);
        } else if (statype == STATYPE_RELATION && !OidIsValid(partid)) {

            UpdateTableStatsInfo(relid, relpages, reltuples);

            Datum locktypVal = heap_getattr(tuple, Anum_pg_statistic_history_stalocktype,
                                            pgstahis->rd_att, &isNull);
            /* if pg_statistic_history saved lock info, restore should lock table */
            if (!isNull && DatumGetChar(locktypVal) == STALOCKTYPE_RELATION) {
                LockStatistic(namespaceid, relid);
            }
        } else if (statype == STATYPE_PARTITION && OidIsValid(partid)) {
            Oid tempId = DatumGetObjectId(heap_getattr(tuple, Anum_pg_statistic_history_partid,
                                        pgstahis->rd_att, &isNull));
            if (tempId != partid) {
                continue;
            }
            UpdatePartitionStatsInfo(partid, relpages, reltuples);

            Datum locktypVal = heap_getattr(tuple, Anum_pg_statistic_history_stalocktype,
                                pgstahis->rd_att, &isNull);
            /* if pg_statistic_history saved lock info, restore should lock partition */
            if (!isNull && DatumGetChar(locktypVal) == STALOCKTYPE_PARTITION) {
                LockStatistic(namespaceid, relid, partid);
            }
        }
    }
    systable_endscan(scan);
    relation_close(pgstahis, AccessShareLock);
}

Datum gs_restore_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    TimestampTz asOfTimestamp = PG_GETARG_TIMESTAMPTZ(2);
    bool force = PG_GETARG_BOOL(3);

    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    CheckPermission(relid);

    /* if force, don't check lock info */
    if (!force && CheckRelationLocked(namespaceid, relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
    }

    RestoreStats(namespaceid, relid, InvalidOid, asOfTimestamp);

    pfree_ext(ownname);
    pfree_ext(tabname);

    PG_RETURN_VOID();
}

Datum gs_restore_schema_stats(PG_FUNCTION_ARGS)
{
    CheckPermission();

    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    TimestampTz asOfTimestamp = PG_GETARG_TIMESTAMPTZ(1);
    bool force = PG_GETARG_BOOL(2);

    Oid namespaceid = get_namespace_oid(ownname, false);
    List* relnames = GetRelationsInSchema(ownname);
    ListCell* lc;
    foreach(lc, relnames) {
        char* relname = (char*)lfirst(lc);
        Oid relid = get_relname_relid(relname, namespaceid);
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            continue;
        }
        RestoreStats(namespaceid, relid, InvalidOid, asOfTimestamp);
    }

    pfree_ext(ownname);

    PG_RETURN_VOID();
}

Datum purge_stats(PG_FUNCTION_ARGS)
{
    if (t_thrd.proc->workingVersionNum < STATISTIC_HISTORY_VERSION_NUMBER) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Working Version Num less than %u does not support read/write pg_statistic_history systable.",
                    STATISTIC_HISTORY_VERSION_NUMBER)));
    }
    CheckPermission();

    TimestampTz before_timestamp;
    TimestampTz retentionTime;
    SysScanDesc sysscan = NULL;
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    ScanKeyData skey[1];
    int retention = u_sess->attr.attr_sql.gms_stats_history_retention;

    if (PG_ARGISNULL(0)) {
        /* if as_of_time is null and gms_stats_history_retention = -1, don't clear history statistic */
        if (retention == -1) {
            PG_RETURN_VOID();
        }
#ifdef HAVE_INT64_TIMESTAMP
        retentionTime = retention * USECS_PER_DAY;
#else
        retentionTime = retention * SECS_PER_DAY;
#endif
        before_timestamp = GetCurrentTimestamp() - retention;
    } else {
        before_timestamp = PG_GETARG_TIMESTAMPTZ(0);
    }

    ScanKeyInit(&skey[0], Anum_pg_statistic_history_current_analyzetime, BTLessStrategyNumber, F_TIMESTAMP_LT,
        TimestampTzGetDatum(before_timestamp));

    rel = heap_open(StatisticHistoryRelationId, AccessShareLock);
    sysscan = systable_beginscan(rel, StatisticHistoryCurrTimeRelidIndexId, true, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        simple_heap_delete(rel, &tuple->t_self);
    }
    systable_endscan(sysscan);
    heap_close(rel, AccessShareLock);

    PG_RETURN_VOID();
}

static void GatherTableStats(char* sqlString, Oid namespaceid, Oid relid, Oid partid, Oid starelid)
{
    VacuumStmt* stmt;
    List* parsetreeList = NIL;
    ListCell* parsetreeItem = NULL;
    TimestampTz currentAnalyzetime = 0;
    bool lastExistsStats = true;
    bool tmp_enable_autoanalyze = u_sess->attr.attr_sql.enable_autoanalyze;

    /* save last analyzetime */
    currentAnalyzetime = GetRecentAnalyzeTime(0, relid, '\0');
    lastExistsStats = ExistsStatisticInfo(relid, partid);

    parsetreeList = raw_parser(sqlString, NULL);
    foreach (parsetreeItem, parsetreeList) {
        Node* parsetree = (Node*)lfirst(parsetreeItem);
        stmt = (VacuumStmt*)parsetree;
    }
    /* forbid auto-analyze inside vacuum/analyze */
    u_sess->attr.attr_sql.enable_autoanalyze = false;
    DoVacuumMppTable(stmt, sqlString, true, false);
    u_sess->attr.attr_sql.enable_autoanalyze = tmp_enable_autoanalyze;

    /* 
     * Gather to user-table:
     *  1. do [vacuum] analyze
     *  2. export latest statistic info to user-table
     *  3. restore system statistic info to last statistic info
     */
    if (OidIsValid(starelid)) {
        CommandCounterIncrement();
        ExportTableStats(namespaceid, relid, partid, starelid, true);
        if (!lastExistsStats || currentAnalyzetime == 0) {
            DeleteTableStats(namespaceid, relid, partid, InvalidOid, true, true, true);
        } else {
            RestoreStats(namespaceid, relid, partid, currentAnalyzetime);
        }
    }
}

Datum gs_gather_table_stats(PG_FUNCTION_ARGS)
{
    char* ownname = text_to_cstring(PG_GETARG_TEXT_P(0));
    char* tabname = text_to_cstring(PG_GETARG_TEXT_P(1));
    bool force = PG_GETARG_BOOL(6);
    char* partname = NULL;
    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;
    StringInfo executeSql;

    if (!PG_ARGISNULL(3)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(3));
    }
    if (!PG_ARGISNULL(4)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(4));
    }
    if (PG_ARGISNULL(5)) {
        statown = pstrdup(u_sess->attr.attr_common.namespace_current_schema);
    } else {
        statown = text_to_cstring(PG_GETARG_TEXT_P(5));
    }

    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    Oid namespaceid = get_namespace_oid(ownname, false);
    Oid relid = get_relname_relid(tabname, namespaceid);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" is not exists", ownname, tabname)));
    }
    if (pg_class_aclcheck(relid, GetUserId(), ACL_VACUUM) != ACLCHECK_OK) {
        CheckPermission(relid);
    }

    Oid partid = InvalidOid;
    char parttype = '\0';
    if (PG_ARGISNULL(2)) {
        if (!force && CheckRelationLocked(namespaceid, relid)) {
            ereport(ERROR, (errmsg("Relation \"%s\".\"%s\" has been locked", ownname, tabname)));
        }
    } else {
        partname = text_to_cstring(PG_GETARG_TEXT_P(2));
        if (!OidIsValid(partid = GetPartitionOid(relid, partname))) {
            ereport(ERROR, (errmsg("Partition \"%s\" is not exists", partname)));
        }
        if (!force && CheckRelationLocked(namespaceid, relid, partid)) {
            ereport(ERROR, (errmsg("Partition \"%s\" has been locked", partname)));
        }
        HeapTuple tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
        if (HeapTupleIsValid(tuple)) {
            Form_pg_partition part = (Form_pg_partition)GETSTRUCT(tuple);
            parttype = part->parttype;
            ReleaseSysCache(tuple);
        }
    }

    executeSql = makeStringInfo();
    appendStringInfo(executeSql, "ANALYZE %s.%s", quote_identifier(ownname), quote_identifier(tabname));
    if (OidIsValid(partid)) {
        appendStringInfo(executeSql, " %s(%s);", parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION
            ? "SUBPARTITION" : "PARTITION", quote_identifier(partname));
    }

    GatherTableStats(executeSql->data, namespaceid, relid, partid, starelid);

    DestroyStringInfo(executeSql);
    pfree_ext(ownname);
    pfree_ext(tabname);
    pfree_ext(partname);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}

Datum gs_gather_database_stats(PG_FUNCTION_ARGS)
{
    CheckPermission();

    char* stattab = NULL;
    char* statid = NULL;
    char* statown = NULL;
    bool gather_sys = true;
    Oid namespaceid = InvalidOid;
    Oid relid = InvalidOid;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    StringInfo executeSql;

    if (!PG_ARGISNULL(0)) {
        stattab = text_to_cstring(PG_GETARG_TEXT_P(0));
    }
    if (!PG_ARGISNULL(1)) {
        statid = text_to_cstring(PG_GETARG_TEXT_P(1));
    }
    if (!PG_ARGISNULL(2)) {
        statown = text_to_cstring(PG_GETARG_TEXT_P(2));
    }
    if (!PG_ARGISNULL(3)) {
        gather_sys = PG_GETARG_BOOL(3);
    }
    
    Oid starelid = GetUserTableRelid(statown, stattab, statid);
    executeSql = makeStringInfo();

    rel = heap_open(NamespaceRelationId, AccessShareLock);
    scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
    while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
        namespaceid = HeapTupleGetOid(tuple);
        /* skip cstore.xxxxx direct on datanode */
        if (namespaceid == CSTORE_NAMESPACE) {
            continue;
        }
        char* ownname = ((Form_pg_namespace)GETSTRUCT(tuple))->nspname.data;
        List* relnamesList = GetRelationsInSchema(ownname);
        ListCell* lc;
        foreach(lc, relnamesList) {
            char* relname = (char*)lfirst(lc);
            relid = get_relname_relid(relname, namespaceid);
            /* skiping analyze table if locked */
            if (CheckRelationLocked(namespaceid, relid)) {
                continue;
            }
            resetStringInfo(executeSql);
            appendStringInfo(executeSql, "ANALYZE %s.%s;", quote_identifier(ownname), quote_identifier(relname));
            if (gather_sys || !is_sys_table(relid)) {
                GatherTableStats(executeSql->data, namespaceid, relid, InvalidOid, starelid);
            }
        }
        list_free_ext(relnamesList);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    DestroyStringInfo(executeSql);
    pfree_ext(stattab);
    pfree_ext(statid);
    pfree_ext(statown);

    PG_RETURN_VOID();
}
