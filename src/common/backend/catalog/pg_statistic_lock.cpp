/* -------------------------------------------------------------------------
 *
 * pg_statistic_lock.cpp
 *      statistic lock info
 *
 * Portions Copyright (c) 2024, openGauss Contributors
 *
 *
 * IDENTIFICATION
 * src/common/backend/catalog/pg_statistic_lock.cpp
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
#include "catalog/indexing.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_history.h"
#include "commands/sqladvisor.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "catalog/pg_statistic_lock.h"

static bool CheckSchemaLocked(Oid namespaceid)
{
    SysScanDesc scan = NULL;
    ScanKeyData skey[2];
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    bool isnull = true;
    bool islock = false;
    bool result = false;

    ScanKeyInit(&skey[0], Anum_pg_statistic_lock_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_pg_statistic_lock_stalocktype, BTEqualStrategyNumber, F_CHAREQ,
                CharGetDatum(STALOCKTYPE_SCHEMA));

    rel = relation_open(StatisticLockRelationId, AccessShareLock);
    scan = systable_beginscan(rel, StatisticLockIndexId, true, SnapshotNow, 2, skey);
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        islock = DatumGetBool(heap_getattr(tuple, Anum_pg_statistic_lock_lock, rel->rd_att, &isnull));
        result = isnull ? false : islock;
    }
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return result;
}

/*
 * CheckRelationLocked --- check relation is locked for statistic
 */
bool CheckRelationLocked(Oid namespaceid, Oid relid, Oid partid, bool checkSchema)
{
    if (t_thrd.proc->workingVersionNum < STATISTIC_HISTORY_VERSION_NUMBER) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Working Version Num less than %u does not support read/write pg_statistic_lock systable.",
                    STATISTIC_HISTORY_VERSION_NUMBER)));
    }

    SysScanDesc scan = NULL;
    ScanKeyData skey[4];
    int nkeys = 3;
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    bool isnull = true;
    bool islock = false;
    bool result = false;

    ScanKeyInit(&skey[0], Anum_pg_statistic_lock_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&skey[1], Anum_pg_statistic_lock_stalocktype, BTEqualStrategyNumber, F_CHAREQ,
                CharGetDatum(OidIsValid(partid) ? STALOCKTYPE_PARTITION : STALOCKTYPE_RELATION));
    ScanKeyInit(&skey[2], Anum_pg_statistic_lock_relid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));
    if (OidIsValid(partid)) {
        nkeys = 4;
        ScanKeyInit(&skey[3], Anum_pg_statistic_lock_partid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(partid));
    }

    rel = heap_open(StatisticLockRelationId, AccessShareLock);
    scan = systable_beginscan(rel, StatisticLockIndexId, true, SnapshotNow, nkeys, skey);
    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        islock = DatumGetBool(heap_getattr(tuple, Anum_pg_statistic_lock_lock, rel->rd_att, &isnull));
        result = isnull ? false : islock;
    } else if (checkSchema) {
        result = CheckSchemaLocked(namespaceid);
    }
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return result;
}

/*
 * InsertStatisticLock --- insert entries into pg_statistic_lock
 */
static void InsertStatisticLock(Oid namespaceid, Oid relid, Oid partid, bool lock)
{
    Datum values[Natts_pg_statistic_lock] = {0};
    bool nulls[Natts_pg_statistic_lock] = {false};
    Relation rel = NULL;
    HeapTuple tuple = NULL;
    char statlocktype = STALOCKTYPE_SCHEMA;

    values[Anum_pg_statistic_lock_namespaceid - 1] = ObjectIdGetDatum(namespaceid);
    values[Anum_pg_statistic_lock_relid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_statistic_lock_partid - 1] = ObjectIdGetDatum(InvalidOid);

    if (OidIsValid(relid)) {
        values[Anum_pg_statistic_lock_relid - 1] = ObjectIdGetDatum(relid);
        statlocktype = STALOCKTYPE_RELATION;

        if (OidIsValid(partid)) {
            values[Anum_pg_statistic_lock_partid - 1] = ObjectIdGetDatum(partid);
            statlocktype = STALOCKTYPE_PARTITION;
        }
    }
    values[Anum_pg_statistic_lock_stalocktype - 1] = CharGetDatum(statlocktype);
    values[Anum_pg_statistic_lock_lock - 1] = BoolGetDatum(lock);

    rel = heap_open(StatisticLockRelationId, RowExclusiveLock);
    tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void) simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);
    
    heap_freetuple_ext(tuple);
    relation_close(rel, RowExclusiveLock);
}

/*
 * LockStatistic --- insert or update entries in pg_statistic_lock.
 *
 * If the operation object is a schema, lock all table objects in the schema
 */
static void DoStatisticLockInternal(Oid namespaceid, Oid relid, Oid partid, bool lock)
{
    if (t_thrd.proc->workingVersionNum < STATISTIC_HISTORY_VERSION_NUMBER) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Working Version Num less than %u does not support read/write pg_statistic_lock systable.",
                    STATISTIC_HISTORY_VERSION_NUMBER)));
    }

    SysScanDesc scan;
    ScanKeyData skey[4];
    HeapTuple tuple = NULL;
    Relation rel = NULL;
    int nkey = 1;
    char stalocktype = STALOCKTYPE_SCHEMA;
    bool exists = false;
    Datum values[Natts_pg_statistic_lock] = {0};
    bool nulls[Natts_pg_statistic_lock] = {true, true, true, true, false};
    bool replaces[Natts_pg_statistic_lock] = {false, false, false, false, true};
    HeapTuple newtup = NULL;

    if (!OidIsValid(namespaceid)) {
        ereport(ERROR, (errmsg("Invalid namespaceid %u", namespaceid)));
    }

    ScanKeyInit(&skey[0], Anum_pg_statistic_lock_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(namespaceid));
    if (OidIsValid(relid)) {
        ScanKeyInit(&skey[2], Anum_pg_statistic_lock_relid, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(relid));
        stalocktype = STALOCKTYPE_RELATION;
        nkey = 3;

        if (OidIsValid(partid)) {
            ScanKeyInit(&skey[3], Anum_pg_statistic_lock_partid, BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(partid));
            stalocktype = STALOCKTYPE_PARTITION;
            nkey = 4;
        }
        ScanKeyInit(&skey[1], Anum_pg_statistic_lock_stalocktype, BTEqualStrategyNumber, F_CHAREQ,
                    CharGetDatum(stalocktype));
    }

    rel = heap_open(StatisticLockRelationId, RowExclusiveLock);
    scan = systable_beginscan(rel, StatisticLockIndexId, true, SnapshotNow, nkey, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_statistic_lock lockTup = (Form_pg_statistic_lock)GETSTRUCT(tuple);
        if (stalocktype == STALOCKTYPE_SCHEMA) {
            if (lockTup->stalocktype == STALOCKTYPE_SCHEMA) {
                exists = true;
            }
        } else {
            exists = true;
        }
        /* skiping if the lock state is same */
        if (lockTup->lock == lock) {
            continue;
        }
        values[Anum_pg_statistic_lock_lock - 1] = BoolGetDatum(lock);
        newtup = heap_modify_tuple(tuple, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &newtup->t_self, newtup);
        CatalogUpdateIndexes(rel, newtup);
    }
    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);

    /* if lock statistic not exists, do insert one */
    if (!exists) {
        InsertStatisticLock(namespaceid, relid, partid, lock);
    }
}

void LockStatistic(Oid namespaceid, Oid relid, Oid partid)
{
    DoStatisticLockInternal(namespaceid, relid, partid, true);
}

void UnlockStatistic(Oid namespaceid, Oid relid, Oid partid)
{
    DoStatisticLockInternal(namespaceid, relid, partid, false);
}

/*
 * RemoveStatisticLockTab --- remove table lock info from pg_statistic_lock
 */
void RemoveStatisticLockTab(Oid namespaceid, Oid relid)
{
    Relation pgstalock;
    SysScanDesc scan;
    ScanKeyData key[3];
    HeapTuple tuple;

    ScanKeyInit(&key[0], Anum_pg_statistic_lock_namespaceid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(namespaceid));
    ScanKeyInit(&key[1], Anum_pg_statistic_lock_stalocktype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STALOCKTYPE_RELATION));
    ScanKeyInit(&key[2], Anum_pg_statistic_lock_relid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(relid));

    pgstalock = relation_open(StatisticLockRelationId, RowExclusiveLock);
    scan = systable_beginscan(pgstalock, StatisticLockIndexId, true, NULL, 3, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        simple_heap_delete(pgstalock, &tuple->t_self);
    }
    systable_endscan(scan);
    scan = NULL;

    /* delete partition lock info */
    ScanKeyInit(&key[1], Anum_pg_statistic_lock_stalocktype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(STALOCKTYPE_PARTITION));
    scan = systable_beginscan(pgstalock, StatisticLockIndexId, true, NULL, 3, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        simple_heap_delete(pgstalock, &tuple->t_self);
    }
    systable_endscan(scan);

    relation_close(pgstalock, RowExclusiveLock);
}
