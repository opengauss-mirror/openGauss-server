/* -------------------------------------------------------------------------
 *
 * pg_statistic_history.cpp
 *      statistic history
 *
 * Portions Copyright (c) 2024, openGauss Contributors
 *
 *
 * IDENTIFICATION
 * src/common/backend/catalog/pg_statistic_history.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/skey.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_lock.h"
#include "commands/sqladvisor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "catalog/pg_statistic_history.h"

TimestampTz GetRecentAnalyzeTime(TimestampTz asOfTime, Oid relid, char statype)
{
    ScanKeyData skey[2];
    Relation pgstat = NULL;
    Relation idxRel = NULL;
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    bool isnull = true;
    char curStatype = STATYPE_RELATION;
    TimestampTz time = 0;
    TimestampTz result = 0;

    if (asOfTime == 0) {
        asOfTime = GetCurrentTimestamp();
    }

    ScanKeyInit(&skey[0], Anum_pg_statistic_history_current_analyzetime, BTLessEqualStrategyNumber, F_TIMESTAMP_LE,
                TimestampTzGetDatum(asOfTime));
    ScanKeyInit(&skey[1], Anum_pg_statistic_history_starelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    pgstat = heap_open(StatisticHistoryRelationId, AccessShareLock);
    idxRel = index_open(StatisticHistoryCurrTimeRelidIndexId, AccessShareLock);
    scan = systable_beginscan_ordered(pgstat, idxRel, SnapshotNow, 2, skey);
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, BackwardScanDirection))) {
        curStatype = DatumGetChar(heap_getattr(tuple, Anum_pg_statistic_history_statype, pgstat->rd_att, &isnull));
        if (statype != '\0' && curStatype != statype) {
            continue;
        }
        time = DatumGetTimestampTz(heap_getattr(tuple, Anum_pg_statistic_history_current_analyzetime,
                                                pgstat->rd_att, &isnull));
        if (!isnull && time > result) {
            result = time;
            break;
        }
    }
    systable_endscan_ordered(scan);
    index_close(idxRel, AccessShareLock);
    relation_close(pgstat, AccessShareLock);

    return result;
}

/*
 * InsertColumnStatisticHistory --- insert cloumn statistic info pg_statistic_history
 */
void InsertColumnStatisticHistory(Oid relid, char relkind, bool inh, VacAttrStats* stats)
{
    /* if gms_stats_history_retention is 0, means never save pg_statistic_history */
    if (u_sess->attr.attr_sql.gms_stats_history_retention == 0) {
        return;
    }

    Relation pgstat = NULL;
    HeapTuple stup = NULL;
    int i, k, n;
    TimestampTz currentAnalyzeTime = t_thrd.vacuum_cxt.vacuumAnalyzeTime;
    Datum values[Natts_pg_statistic_history] = {0};
    bool nulls[Natts_pg_statistic_history] = {false};
    Oid namespaceid = GetNamespaceIdbyRelId(relid);
    TimestampTz lastAnalyzetime = GetRecentAnalyzeTime(0, relid, STATYPE_COLUMN);

    AttrNumber attnum = stats->attrs[0]->attnum;
    values[Anum_pg_statistic_history_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    values[Anum_pg_statistic_history_starelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_statistic_history_statype - 1] = ObjectIdGetDatum(STATYPE_COLUMN);
    if (lastAnalyzetime == 0) {
        nulls[Anum_pg_statistic_history_last_analyzetime - 1] = true;
    } else {
        values[Anum_pg_statistic_history_last_analyzetime - 1] = TimestampTzGetDatum(lastAnalyzetime);
    }
    values[Anum_pg_statistic_history_current_analyzetime - 1] = TimestampTzGetDatum(currentAnalyzeTime);
    values[Anum_pg_statistic_history_starelkind - 1] = CharGetDatum(relkind);
    values[Anum_pg_statistic_history_staattnum - 1] = Int16GetDatum(attnum);
    values[Anum_pg_statistic_history_stainherit - 1] = BoolGetDatum(inh);
    values[Anum_pg_statistic_history_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
    values[Anum_pg_statistic_history_stawidth - 1] = Int32GetDatum(stats->stawidth);
    values[Anum_pg_statistic_history_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
    values[Anum_pg_statistic_history_stadndistinct - 1] = Float4GetDatum(stats->stadndistinct);

    nulls[Anum_pg_statistic_history_reltuples - 1] = true;
    nulls[Anum_pg_statistic_history_relpages - 1] = true;
    /* For single column statistics we keep staextinfo to NULL */
    nulls[Anum_pg_statistic_history_staextinfo - 1] = true;

    /* Process stakind1 -> stakind5 */
    i = Anum_pg_statistic_history_stakind1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
    }

    /* Process staop1 -> staop5 */
    i = Anum_pg_statistic_history_staop1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        values[i++] = ObjectIdGetDatum(stats->staop[k]); /* staopN */
    }

    /* Process stanumbers1 -> stanumber5 */
    i = Anum_pg_statistic_history_stanumbers1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        int nnum = stats->numnumbers[k];

        if (nnum > 0) {
            Datum* numdatums = (Datum*)palloc(nnum * sizeof(Datum));
            ArrayType* arry = NULL;

            for (n = 0; n < nnum; n++)
                numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
            /* XXX knows more than it should about type float4: */
            arry = construct_array(numdatums, nnum, FLOAT4OID, sizeof(float4), FLOAT4PASSBYVAL, 'i');
            values[i++] = PointerGetDatum(arry); /* stanumbersN */
        } else {
            nulls[i] = true;
            values[i++] = (Datum)0;
        }
    }

    /* Process stavalues1 -> stavalues5 */
    i = Anum_pg_statistic_history_stavalues1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        if (stats->numvalues[k] > 0) {
            ArrayType* array = construct_array(stats->stavalues[k],
                stats->numvalues[k],
                stats->statypid[k],
                stats->statyplen[k],
                stats->statypbyval[k],
                stats->statypalign[k]);
            values[i++] = PointerGetDatum(array); /* stavaluesN */
        } else {
            nulls[i] = true;
            values[i++] = (Datum)0;
        }
    }

    pgstat = relation_open(StatisticHistoryRelationId, RowExclusiveLock);
    stup = heap_form_tuple(RelationGetDescr(pgstat), values, nulls);
    (void)simple_heap_insert(pgstat, stup);
    CatalogUpdateIndexes(pgstat, stup);

    heap_close(pgstat, RowExclusiveLock);
    tableam_tops_free_tuple(stup);
}

void ImportColumnStatisticHistory(HeapTuple tuple, TupleDesc tupdesc, Oid namespaceid, Oid relid,
                                char relkind, int16 attnum, TimestampTz currentAnalyzetime)
{
    /* if gms_stats_history_retention is 0, means never save pg_statistic_history */
    if (u_sess->attr.attr_sql.gms_stats_history_retention == 0) {
        return;
    }

    Datum values[Natts_pg_statistic_history] = {0};
    bool nulls[Natts_pg_statistic_history] = {false};
    bool isNull = true;

    TimestampTz lastAnalyzetime = GetRecentAnalyzeTime(currentAnalyzetime, relid, STATYPE_COLUMN);

    values[Anum_pg_statistic_history_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    values[Anum_pg_statistic_history_starelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_statistic_history_statype - 1] = CharGetDatum(STATYPE_COLUMN);
    values[Anum_pg_statistic_history_last_analyzetime - 1] = TimestampTzGetDatum(lastAnalyzetime);
    nulls[Anum_pg_statistic_history_last_analyzetime - 1] = lastAnalyzetime == 0 ? true : false;
    values[Anum_pg_statistic_history_current_analyzetime - 1] = TimestampTzGetDatum(currentAnalyzetime);
    values[Anum_pg_statistic_history_starelkind - 1] = CharGetDatum(relkind);
    values[Anum_pg_statistic_history_staattnum - 1] = Int16GetDatum(attnum);

    values[Anum_pg_statistic_history_stainherit - 1] =
        heap_getattr(tuple, Anum_user_table_stainherit, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stainherit - 1] = isNull;
    values[Anum_pg_statistic_history_stanullfrac - 1] =
        heap_getattr(tuple, Anum_user_table_stanullfrac, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanullfrac - 1] = isNull;
    values[Anum_pg_statistic_history_stawidth - 1] =
        heap_getattr(tuple, Anum_user_table_stawidth, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stawidth - 1] = isNull;
    values[Anum_pg_statistic_history_stadistinct - 1] =
        heap_getattr(tuple, Anum_user_table_stadistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stadistinct - 1] = isNull;

    nulls[Anum_pg_statistic_history_reltuples - 1] = true;
    nulls[Anum_pg_statistic_history_relpages - 1] = true;
    nulls[Anum_pg_statistic_history_stalocktype - 1] = true;

    values[Anum_pg_statistic_history_stakind1 - 1] =
        heap_getattr(tuple, Anum_user_table_stakind1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stakind1 - 1] = isNull;
    values[Anum_pg_statistic_history_stakind2 - 1] =
        heap_getattr(tuple, Anum_user_table_stakind2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stakind2 - 1] = isNull;
    values[Anum_pg_statistic_history_stakind3 - 1] =
        heap_getattr(tuple, Anum_user_table_stakind3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stakind3 - 1] = isNull;
    values[Anum_pg_statistic_history_stakind4 - 1] =
        heap_getattr(tuple, Anum_user_table_stakind4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stakind4 - 1] = isNull;
    values[Anum_pg_statistic_history_stakind5 - 1] =
        heap_getattr(tuple, Anum_user_table_stakind5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stakind5 - 1] = isNull;
    values[Anum_pg_statistic_history_staop1 - 1] =
        heap_getattr(tuple, Anum_user_table_staop1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staop1 - 1] = isNull;
    values[Anum_pg_statistic_history_staop2 - 1] =
        heap_getattr(tuple, Anum_user_table_staop2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staop2 - 1] = isNull;
    values[Anum_pg_statistic_history_staop3 - 1] =
        heap_getattr(tuple, Anum_user_table_staop3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staop3 - 1] = isNull;
    values[Anum_pg_statistic_history_staop4 - 1] =
        heap_getattr(tuple, Anum_user_table_staop4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staop4 - 1] = isNull;
    values[Anum_pg_statistic_history_staop5 - 1] =
        heap_getattr(tuple, Anum_user_table_staop5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staop5 - 1] = isNull;
    values[Anum_pg_statistic_history_stanumbers1 - 1] =
        heap_getattr(tuple, Anum_user_table_stanumbers1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanumbers1 - 1] = isNull;
    values[Anum_pg_statistic_history_stanumbers2 - 1] =
        heap_getattr(tuple, Anum_user_table_stanumbers2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanumbers2 - 1] = isNull;
    values[Anum_pg_statistic_history_stanumbers3 - 1] =
        heap_getattr(tuple, Anum_user_table_stanumbers3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanumbers3 - 1] = isNull;
    values[Anum_pg_statistic_history_stanumbers4 - 1] =
        heap_getattr(tuple, Anum_user_table_stanumbers4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanumbers4 - 1] = isNull;
    values[Anum_pg_statistic_history_stanumbers5 - 1] =
        heap_getattr(tuple, Anum_user_table_stanumbers5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stanumbers5 - 1] = isNull;
    values[Anum_pg_statistic_history_stavalues1 - 1] =
        heap_getattr(tuple, Anum_user_table_stavalues1, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stavalues1 - 1] = isNull;
    values[Anum_pg_statistic_history_stavalues2 - 1] =
        heap_getattr(tuple, Anum_user_table_stavalues2, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stavalues2 - 1] = isNull;
    values[Anum_pg_statistic_history_stavalues3 - 1] =
        heap_getattr(tuple, Anum_user_table_stavalues3, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stavalues3 - 1] = isNull;
    values[Anum_pg_statistic_history_stavalues4 - 1] =
        heap_getattr(tuple, Anum_user_table_stavalues4, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stavalues4 - 1] = isNull;
    values[Anum_pg_statistic_history_stavalues5 - 1] =
        heap_getattr(tuple, Anum_user_table_stavalues5, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stavalues5 - 1] = isNull;
    values[Anum_pg_statistic_history_stadndistinct - 1] =
        heap_getattr(tuple, Anum_user_table_stadndistinct, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_stadndistinct - 1] = isNull;
    values[Anum_pg_statistic_history_staextinfo - 1] =
        heap_getattr(tuple, Anum_user_table_staextinfo, tupdesc, &isNull);
    nulls[Anum_pg_statistic_history_staextinfo - 1] = isNull;

    Relation rel = heap_open(StatisticHistoryRelationId, RowExclusiveLock);
    HeapTuple tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void) simple_heap_insert(rel, tup);

    CatalogUpdateIndexes(rel, tup);
    heap_freetuple_ext(tup);
    heap_close(rel, RowExclusiveLock);
}

/*
 * InsertClassStatisHistory --- insert class statistic info pg_statistic_history
 */
void InsertClassStatisHistory(Oid relid, double numpages, double numtuples)
{
    /* if gms_stats_history_retention is 0, means never save pg_statistic_history */
    if (u_sess->attr.attr_sql.gms_stats_history_retention == 0) {
        return;
    }

    HeapTuple stup = NULL;
    Relation rel = NULL;
    TimestampTz currentAnalyzeTime = t_thrd.vacuum_cxt.vacuumAnalyzeTime;
    Datum values[Natts_pg_statistic_history] = {0};
    bool nulls[Natts_pg_statistic_history];
    Oid namespaceid = GetNamespaceIdbyRelId(relid);
    TimestampTz lastAnalyzeTime = GetRecentAnalyzeTime(0, relid, STATYPE_RELATION);

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pg_statistic_history_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    nulls[Anum_pg_statistic_history_namespaceid - 1] = false;
    values[Anum_pg_statistic_history_starelid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_pg_statistic_history_starelid - 1] = false;
    values[Anum_pg_statistic_history_statype - 1] = ObjectIdGetDatum(STATYPE_RELATION);
    nulls[Anum_pg_statistic_history_statype - 1] = false;
    if (lastAnalyzeTime != 0) {
        nulls[Anum_pg_statistic_history_last_analyzetime - 1] = false;
        values[Anum_pg_statistic_history_last_analyzetime - 1] = TimestampTzGetDatum(lastAnalyzeTime);
    }
    values[Anum_pg_statistic_history_current_analyzetime - 1] = TimestampTzGetDatum(currentAnalyzeTime);
    nulls[Anum_pg_statistic_history_current_analyzetime - 1] = false;
    values[Anum_pg_statistic_history_reltuples - 1] = Float8GetDatum(numtuples);
    nulls[Anum_pg_statistic_history_reltuples - 1] = false;
    values[Anum_pg_statistic_history_relpages - 1] = Float8GetDatum(numpages);
    nulls[Anum_pg_statistic_history_relpages - 1] = false;

    if (CheckRelationLocked(namespaceid, relid, InvalidOid, false)) {
        values[Anum_pg_statistic_history_stalocktype - 1] = CharGetDatum(STALOCKTYPE_RELATION);
        nulls[Anum_pg_statistic_history_stalocktype - 1] = false;
    }

    rel = relation_open(StatisticHistoryRelationId, RowExclusiveLock);
    stup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, stup);
    CatalogUpdateIndexes(rel, stup);

    heap_close(rel, RowExclusiveLock);
    tableam_tops_free_tuple(stup);
}

/*
 * InsertPartitionStatisticHistory --- insert partition statistic info pg_statistic_history
 */
void InsertPartitionStatisticHistory(Oid relid, Oid partid, double numpages, double numtuples)
{
    /* if gms_stats_history_retention is 0, means never save pg_statistic_history */
    if (u_sess->attr.attr_sql.gms_stats_history_retention == 0) {
        return;
    }

    HeapTuple reltup = NULL;
    HeapTuple stup = NULL;
    Relation rel = NULL;
    TimestampTz currentAnalyzeTime = t_thrd.vacuum_cxt.vacuumAnalyzeTime;
    TimestampTz lastAnalyzeTime = 0;
    Datum values[Natts_pg_statistic_history] = {0};
    bool nulls[Natts_pg_statistic_history];
    Oid namespaceid = InvalidOid;

    errno_t rc = memset_s(nulls, sizeof(nulls), true, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* if subpartition, we need find real relid */
    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(reltup)) {
        reltup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(reltup)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("relation with OID %u does not exist", relid)));
        }
        relid = ((Form_pg_partition)GETSTRUCT(reltup))->parentid;
        ReleaseSysCache(reltup);

        reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(reltup)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("relation with OID %u does not exist", relid)));
        }
    }
    namespaceid = ((Form_pg_class)GETSTRUCT(reltup))->relnamespace;
    if (HeapTupleIsValid(reltup)) {
        ReleaseSysCache(reltup);
    }

    lastAnalyzeTime = GetRecentAnalyzeTime(0, relid, STATYPE_PARTITION);

    values[Anum_pg_statistic_history_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    nulls[Anum_pg_statistic_history_namespaceid - 1] = false;
    values[Anum_pg_statistic_history_starelid - 1] = ObjectIdGetDatum(relid);
    nulls[Anum_pg_statistic_history_starelid - 1] = false;
    values[Anum_pg_statistic_history_partid - 1] = ObjectIdGetDatum(partid);
    nulls[Anum_pg_statistic_history_partid - 1] = false;
    values[Anum_pg_statistic_history_statype - 1] = ObjectIdGetDatum(STATYPE_PARTITION);
    nulls[Anum_pg_statistic_history_statype - 1] = false;
    if (lastAnalyzeTime != 0) {
        nulls[Anum_pg_statistic_history_last_analyzetime - 1] = false;
        values[Anum_pg_statistic_history_last_analyzetime - 1] = TimestampTzGetDatum(lastAnalyzeTime);
    }
    values[Anum_pg_statistic_history_current_analyzetime - 1] = TimestampTzGetDatum(currentAnalyzeTime);
    nulls[Anum_pg_statistic_history_current_analyzetime - 1] = false;
    values[Anum_pg_statistic_history_reltuples - 1] = Float8GetDatum(numtuples);
    nulls[Anum_pg_statistic_history_reltuples - 1] = false;
    values[Anum_pg_statistic_history_relpages - 1] = Float8GetDatum(numpages);
    nulls[Anum_pg_statistic_history_relpages - 1] = false;

    if (CheckRelationLocked(namespaceid, relid, partid, false)) {
        values[Anum_pg_statistic_history_stalocktype - 1] = CharGetDatum(STALOCKTYPE_PARTITION);
        nulls[Anum_pg_statistic_history_stalocktype - 1] = false;
    }

    rel = relation_open(StatisticHistoryRelationId, RowExclusiveLock);
    stup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, stup);
    CatalogUpdateIndexes(rel, stup);

    heap_close(rel, RowExclusiveLock);
    tableam_tops_free_tuple(stup);
}

/*
 * RemoveStatisticHistory --- remove entries in pg_statistic_history for a rel or column
 */
void RemoveStatisticHistory(Oid relid, AttrNumber attnum)
{
    Relation pgstahis = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData key[3];
    int nkeys = 2;
    HeapTuple tuple = NULL;
    char statype = STATYPE_RELATION;

    ScanKeyInit(&key[0], Anum_pg_statistic_history_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    if (0 != attnum) {
        ScanKeyInit(&key[2], Anum_pg_statistic_history_staattnum, BTEqualStrategyNumber,
                    F_INT2EQ, Int16GetDatum(attnum));
        nkeys = 3;
        statype = STATYPE_COLUMN;
    }
    ScanKeyInit(&key[1], Anum_pg_statistic_history_statype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(statype));

    pgstahis = relation_open(StatisticHistoryRelationId, RowExclusiveLock);
    scan = systable_beginscan(pgstahis, StatisticHistoryTabTypAttnumIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        simple_heap_delete(pgstahis, &tuple->t_self);
    }
    systable_endscan(scan);
    relation_close(pgstahis, RowExclusiveLock);
}
