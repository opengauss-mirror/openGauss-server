/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * -------------------------------------------------------------------------
 *
 * pg_partition.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/pg_partition.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "catalog/namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_partition.h"
#include "pgxc/redistrib.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lock/lock.h"
#include "storage/sinval.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "utils/snapmgr.h"

void insertPartitionEntry(Relation pg_partition_desc, Partition new_part_desc, Oid new_part_id, int2vector* pkey,
    const oidvector* tablespaces, Datum interval, Datum maxValues, Datum transitionPoint, Datum reloptions,
    char parttype)
{
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    HeapTuple tup = NULL;
    Form_pg_partition pd_part = NULL;
    errno_t errorno = EOK;

    pd_part = new_part_desc->pd_part;
    Assert(pd_part->parttype == PART_OBJ_TYPE_PARTED_TABLE || pd_part->parttype == PART_OBJ_TYPE_TOAST_TABLE ||
           pd_part->parttype == PART_OBJ_TYPE_TABLE_PARTITION || pd_part->parttype == PART_OBJ_TYPE_INDEX_PARTITION);

    /* This is a tad tedious, but way cleaner than what we used to do... */
    errorno = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(errorno, "\0", "\0");

    errorno = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(errorno, "\0", "\0");

    values[Anum_pg_partition_relname - 1] = NameGetDatum(&pd_part->relname);
    values[Anum_pg_partition_parttype - 1] = CharGetDatum(pd_part->parttype);
    values[Anum_pg_partition_parentid - 1] = ObjectIdGetDatum(pd_part->parentid);
    values[Anum_pg_partition_rangenum - 1] = UInt32GetDatum(pd_part->rangenum);
    values[Anum_pg_partition_intervalnum - 1] = UInt32GetDatum(pd_part->intervalnum);
    values[Anum_pg_partition_partstrategy - 1] = CharGetDatum(pd_part->partstrategy);
    values[Anum_pg_partition_relfilenode - 1] = ObjectIdGetDatum(pd_part->relfilenode);
    values[Anum_pg_partition_reltablespace - 1] = ObjectIdGetDatum(pd_part->reltablespace);
    values[Anum_pg_partition_relpages - 1] = Float8GetDatum(pd_part->relpages);
    values[Anum_pg_partition_reltuples - 1] = Float8GetDatum(pd_part->reltuples);
    values[Anum_pg_partition_relallvisible - 1] = Int32GetDatum(pd_part->relallvisible);
    values[Anum_pg_partition_reltoastrelid - 1] = ObjectIdGetDatum(pd_part->reltoastrelid);
    values[Anum_pg_partition_reltoastidxid - 1] = ObjectIdGetDatum(pd_part->reltoastidxid);
    values[Anum_pg_partition_indextblid - 1] = ObjectIdGetDatum(pd_part->indextblid);
    values[Anum_pg_partition_deltarelid - 1] = ObjectIdGetDatum(pd_part->reldeltarelid);
    values[Anum_pg_partition_reldeltaidx - 1] = ObjectIdGetDatum(pd_part->reldeltaidx);
    values[Anum_pg_partition_relcudescrelid - 1] = ObjectIdGetDatum(pd_part->relcudescrelid);
    values[Anum_pg_partition_relcudescidx - 1] = ObjectIdGetDatum(pd_part->relcudescidx);
    values[Anum_pg_partition_indisusable - 1] = BoolGetDatum(pd_part->indisusable);
    values[Anum_pg_partition_relfrozenxid - 1] = ShortTransactionIdGetDatum(pd_part->relfrozenxid);

    /* partition key */
    if (pkey != NULL) {
        values[Anum_pg_partition_partkey - 1] = PointerGetDatum(pkey);
    } else {
        nulls[Anum_pg_partition_partkey - 1] = true;
    }

    /* interval tablespaces */
    if (tablespaces != NULL) {
        values[Anum_pg_partition_intablespace - 1] = PointerGetDatum(tablespaces);
    } else {
        nulls[Anum_pg_partition_intablespace - 1] = true;
    }

    nulls[Anum_pg_partition_intspnum - 1] = true;

    /* interval */
    if (interval != (Datum)0) {
        values[Anum_pg_partition_interval - 1] = interval;
    } else {
        nulls[Anum_pg_partition_interval - 1] = true;
    }

    /* maxvalue */
    if (maxValues != (Datum)0) {
        values[Anum_pg_partition_boundaries - 1] = maxValues;
    } else {
        nulls[Anum_pg_partition_boundaries - 1] = true;
    }

    /* transit point */
    if (transitionPoint != (Datum)0) {
        values[Anum_pg_partition_transit - 1] = transitionPoint;
    } else {
        nulls[Anum_pg_partition_transit - 1] = true;
    }

    if (reloptions != (Datum)0) {
        values[Anum_pg_partition_reloptions - 1] = reloptions;
        nulls[Anum_pg_partition_reloptions - 1] = false;
    } else {
        nulls[Anum_pg_partition_reloptions - 1] = true;
    }

    if (parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
        values[Anum_pg_partition_relfrozenxid64 - 1] = u_sess->utils_cxt.RecentXmin;
    } else {
        values[Anum_pg_partition_relfrozenxid64 - 1] = InvalidTransactionId;
    }

    /* form a tuple using values and null array, and insert it */
    tup = heap_form_tuple(RelationGetDescr(pg_partition_desc), values, nulls);
    HeapTupleSetOid(tup, new_part_id);
    (void)simple_heap_insert(pg_partition_desc, tup);
    CatalogUpdateIndexes(pg_partition_desc, tup);

    heap_freetuple_ext(tup);
}

typedef struct {
    bool givenPartitionName;

    /* Given partition name info */
    char objectType;       /* PART_OBJ_TYPE_TABLE_PARTITION; PART_OBJ_TYPE_INDEX_PARTITION */
    Oid partitionedRelOid; /* oid of partitioned relation, but not partition */
    const char* partitionName;
    PartitionNameGetPartidCallback callback; /* CALLBACK func arguments */
    void* callbackArgs;
    LOCKMODE callbackObjLockMode;

    /* Given partition values info */
    Relation partitionedRel; /* partitioned relation, but not partition */
    List* partKeyValueList;
    bool topClosed; /* whether the top range is closed or open */
} GetPartitionOidArgs;

/*
 * @Description: get partitoin oid from partition name or values list
 * @Param[IN] args: info about partition name or values list.
 * @Param[IN] lockMode: lock mode for this partition
 * @Param[IN] missingOk: true, the caller will handle this case;
 *                       false, this method will report error.
 * @Param[IN] noWait: lock wait ways
 * @Return: partition oid
 * @See also:
 */
static Oid partitionGetPartitionOid(GetPartitionOidArgs* args, LOCKMODE lockMode, bool missingOk, bool noWait)
{
    /* remember SharedInvalidMessageCounter */
    uint64 invalCount = 0;
    Oid partitionOid = InvalidOid;
    Oid partitionOldOid = InvalidOid;
    bool retry = false;

    /*
     * DDL operations can change the results of a name lookup.	Since all such
     * operations will generate invalidation messages, we keep track of
     * whether any such messages show up while we're performing the operation,
     * and retry until either (1) no more invalidation messages show up or (2)
     * the answer doesn't change.
     *
     * But if lockmode = NoLock, then we assume that either the caller is OK
     * with the answer changing under them, or that they already hold some
     * appropriate lock, and therefore return the first answer we get without
     * checking for invalidation messages.  Also, if the requested lock is
     * already held, no LockRelationOid will not AcceptInvalidationMessages,
     * so we may fail to notice a change.  We could protect against that case
     * by calling AcceptInvalidationMessages() before beginning this loop, but
     * that would add a significant amount overhead, so for now we don't.
     */
    for (;;) {
        /*
         * Remember this value, so that, after looking up the partition name
         * and locking its OID, we can check whether any invalidation messages
         * have been processed that might require a do-over.
         */
        invalCount = u_sess->inval_cxt.SharedInvalidMessageCounter;

        /* get partition oid */
        if (args->givenPartitionName) {
            partitionOid = GetSysCacheOid3(PARTPARTOID,
                NameGetDatum(args->partitionName),
                CharGetDatum(args->objectType),
                ObjectIdGetDatum(args->partitionedRelOid));

            if (args->callback) {
                args->callback(args->partitionedRelOid,
                    args->partitionName,
                    partitionOid,
                    partitionOldOid,
                    args->objectType,
                    args->callbackArgs,
                    args->callbackObjLockMode);
            }
        } else {
            partitionOid =
                partitionKeyValueListGetPartitionOid(args->partitionedRel, args->partKeyValueList, args->topClosed);
        }

        /*
         * If no lock requested, we assume the caller knows what they're
         * doing.  They should have already acquired a heavyweight lock on
         * this relation earlier in the processing of this same statement, so
         * it wouldn't be appropriate to AcceptInvalidationMessages() here, as
         * that might pull the rug out from under them.
         */
        if (NoLock == lockMode) {
            break;
        }

        /*
         * If, upon retry, we get back the same OID we did last time, then the
         * invalidation messages we processed did not change the final answer.
         * So we're done.
         *
         * If we got a different OID, we've locked the relation that used to
         * have this name rather than the one that does now.  So release the
         * lock.
         */
        if (retry) {
            if (partitionOid == partitionOldOid) {
                break;
            }

            if (OidIsValid(partitionOldOid)) {
                UnlockPartition(args->partitionedRelOid, partitionOldOid, lockMode, PARTITION_LOCK);
            }
        }
        /*
         * We need to tell LockCheckConflict if we could cancel redistribution xact
         * if we are doing drop/truncate table, redistribution xact is holding
         * the lock on our relId and relId is being redistributed now.
         */
        if (u_sess->exec_cxt.could_cancel_redistribution) {
            u_sess->catalog_cxt.redistribution_cancelable = CheckRelationInRedistribution(args->partitionedRelOid);
            u_sess->exec_cxt.could_cancel_redistribution = false;
        }

        /*
         * Lock partition.  This will also accept any pending invalidation
         * messages.  If we got back InvalidOid, indicating not found, then
         * there's nothing to lock, but we accept invalidation messages
         * anyway, to flush any negative catcache entries that may be lingering.
         */
        if (!OidIsValid(partitionOid)) {
            AcceptInvalidationMessages();
        } else if (!noWait) {
            LockPartition(args->partitionedRelOid, partitionOid, lockMode, PARTITION_LOCK);
        } else if (!ConditionalLockPartition(args->partitionedRelOid, partitionOid, lockMode, PARTITION_LOCK)) {
            if (args->partitionName != NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("could not obtain lock on partition \"%s\"", args->partitionName)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("could not obtain lock on partition(%u)", partitionOid)));
            }
        }

        /*
         * If no invalidation message were processed, we're done!
         */
        if (u_sess->inval_cxt.SharedInvalidMessageCounter == invalCount) {
            break;
        }

        /*
         * Something may have changed.	Let's repeat the name lookup, to make
         * sure this name still references the same partition it did previously.
         */
        retry = true;
        partitionOldOid = partitionOid;
    }

    if (!OidIsValid(partitionOid)) {
        if (!missingOk) {
            if (args->partitionName != NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("partition \"%s\" does not exist", args->partitionName)));
            } else {
                /* here partitionOid is InvalidOid, so it's meaningless to print its value */
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Specified partition does not exist")));
            }
        }
    }
    return partitionOid;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Get partition oid by partition name
 * Description	:
 * Notes		: If the partition is not found, return InvalidOid if missing_ok = true,
 *                otherwise raise an error.
 *
 *                If nowait = true, throw an error if we'd have to wait for a lock.
 */
Oid partitionNameGetPartitionOid(Oid partitionedRelationOid, const char* partitionName, char objectType,
    LOCKMODE lockMode, bool missingOk, bool noWait, PartitionNameGetPartidCallback callback, void* callback_arg,
    LOCKMODE callbackobj_lockMode)
{
    if (!OidIsValid(partitionedRelationOid) || !PointerIsValid(partitionName)) {
        return InvalidOid;
    }

    GetPartitionOidArgs args;
    /* get partition oid from given name */
    args.givenPartitionName = true;
    args.partitionedRelOid = partitionedRelationOid;
    args.partitionName = partitionName;
    args.objectType = objectType;
    args.callback = callback;
    args.callbackArgs = callback_arg;
    args.callbackObjLockMode = callbackobj_lockMode;
    /* the following arguments is not used. */
    args.partitionedRel = NULL;
    args.partKeyValueList = NULL;
    args.topClosed = false;

    Relation partitionRelRelation = relation_open(PartitionRelationId, AccessShareLock);

    Oid partitionOid = partitionGetPartitionOid(&args, lockMode, missingOk, noWait);

    relation_close(partitionRelRelation, AccessShareLock);

    return partitionOid;
}

/*
 * @Description: get partition oid from given key values list.
 * @Param[IN] lockMode: lock mode for this partition
 * @Param[IN] missingOk: true, the caller will handle this case;
 *                       false, this method will report error.
 * @Param[IN] noWait: lock wait ways
 * @Param[IN] partKeyValueList: partition key values
 * @Param[IN] rel: partitioned relation
 * @Param[IN] topClosed:
 * @Return: partition oid
 * @See also: partitionNameGetPartitionOid()
 */
Oid partitionValuesGetPartitionOid(
    Relation rel, List* partKeyValueList, LOCKMODE lockMode, bool topClosed, bool missingOk, bool noWait)
{
    GetPartitionOidArgs args;
    /* get partition oid from given values */
    args.givenPartitionName = false;
    args.partitionedRel = rel;
    args.partKeyValueList = partKeyValueList;
    args.partitionedRelOid = rel->rd_id;
    args.topClosed = topClosed;
    /* the following arguments is not used. */
    args.partitionName = NULL;
    args.objectType =
        rel->rd_rel->relkind == RELKIND_RELATION ? PART_OBJ_TYPE_TABLE_PARTITION : PART_OBJ_TYPE_INDEX_PARTITION;
    args.callback = NULL;
    args.callbackArgs = NULL;
    args.callbackObjLockMode = NoLock;

    return partitionGetPartitionOid(&args, lockMode, missingOk, noWait);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: is it a partitioned table or partitioned index
 * Description	:
 * Input		: relid: object to tell if it is a partitioned object
 *			: relkind: relation kind, is it a table or index
 *			: missing_ok: If it isnot a partitioned object, returns false if missing_ok,
 *			: else throws error
 * Output	:
 * Notes		:
 */
bool isPartitionedObject(Oid relid, char relkind, bool missing_ok)
{
    HeapTuple reltuple = NULL;
    Form_pg_class relation = NULL;
    bool result = false;

    reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(reltuple)) {
        relation = (Form_pg_class)GETSTRUCT(reltuple);

        /*
         * is it a partitioned table or partitioned index
         */
        if (relation->parttype == PARTTYPE_PARTITIONED_RELATION && relation->relkind == relkind) {
            result = true;
        }

        ReleaseSysCache(reltuple);
    }

    if (!result && !missing_ok) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("the object with oid %u is not a partitioned object", relid)));
    }

    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Input		: partid: object to tell if it is a partition object
 *			: partkind: partition kind, is it a table partition or index partition?
 *			: missing_ok: If it isnot a partition object, returns false if missing_ok,
 *			: else throws error
 * Output	:
 * Notes		:
 */
bool isPartitionObject(Oid partid, char partkind, bool missing_ok)
{
    HeapTuple partuple = NULL;
    char relkind = RELKIND_RELATION;
    bool result = false;

    /*
     * 1. check partition kind for object
     * 2. get relation kind for partition's parent
     */
    switch (partkind) {
        case PART_OBJ_TYPE_TABLE_PARTITION:
            relkind = RELKIND_RELATION;
            break;

        case PART_OBJ_TYPE_INDEX_PARTITION:
            relkind = RELKIND_INDEX;
            break;

        default:
            if (missing_ok) {
                return result;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("object with oid %u is not a partition object", partid)));
            }
            break;
    }

    partuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    if (HeapTupleIsValid(partuple)) {
        Form_pg_partition partition = (Form_pg_partition)GETSTRUCT(partuple);
        Oid relid = partition->parentid;
        HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

        if (HeapTupleIsValid(reltuple)) {
            Form_pg_class relation = (Form_pg_class)GETSTRUCT(reltuple);

            if (relkind == relation->relkind) {
                result = true;
            }

            ReleaseSysCache(reltuple);
        }

        ReleaseSysCache(partuple);
    }

    if (!result && !missing_ok) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("object with oid %u is not a partition object", partid)));
    }

    return result;
}

// if none is the son local index of partitionedIndexid, report error
Oid searchPartitionIndexOid(Oid partitionedIndexid, List* pindex)
{
    Oid result = InvalidOid;
    Oid localIndexOid = InvalidOid;
    HeapTuple htup = NULL;
    ListCell* cell = NULL;

    if (!OidIsValid(partitionedIndexid) || !PointerIsValid(pindex)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("invalid input parameters when searching for local index under some index")));
    }

    // check for partitioned index property
    htup = SearchSysCache1(RELOID, ObjectIdGetDatum(partitionedIndexid));
    if (HeapTupleIsValid(htup)) {
        if ((RELKIND_INDEX != ((Form_pg_class)GETSTRUCT(htup))->relkind) ||
            (PARTTYPE_PARTITIONED_RELATION != ((Form_pg_class)GETSTRUCT(htup))->parttype)) {
            ReleaseSysCache(htup);
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%u is not a partitioned index", partitionedIndexid)));
        } else {
            ReleaseSysCache(htup);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%u not found in pg_class", partitionedIndexid)));
    }

    foreach (cell, pindex) {
        localIndexOid = lfirst_oid(cell);
        Assert(OidIsValid(localIndexOid));
        htup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(localIndexOid));
        if (HeapTupleIsValid(htup)) {
            if (partitionedIndexid == ((Form_pg_partition)GETSTRUCT(htup))->parentid) {
                ReleaseSysCache(htup);
                result = localIndexOid;
                break;
            } else {
                ReleaseSysCache(htup);
            }
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("not found local index %u in pg_partition", localIndexOid)));
        }
    }

    if (!OidIsValid(result)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("none is the son local index of index %u", partitionedIndexid)));
    }
    return result;
}

/*
 * @Description: Given OIDs of index relation and heap partition, get Oid of index partition.
 *               if onlyOid is false, copy pg_partition FormData_pg_partition into *outFormData.
 * @IN indexid: given index relation's OID
 * @IN partitionid: given partition's  OID
 * @OUT outFormData: FormData_pg_partition struct
 * @Return: Oid of index partition.
 * @See also:
 */
template <bool onlyOid>
static Oid getPartitionIndexFormData(Oid indexid, Oid partitionid, Form_pg_partition outFormData)
{
    Relation pg_partition = NULL;
    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Form_pg_partition partitionForm = NULL;
    Oid result = InvalidOid;
    bool found = false;

    pg_partition = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_indextblid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partitionid));

    scan = systable_beginscan(pg_partition, PartitionIndexTableIdIndexId, true, SnapshotNow, 1, key);
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        /* If enter while loop means partition has at least one index */
        partitionForm = (Form_pg_partition)GETSTRUCT(tuple);
        if (partitionForm->parentid == indexid) {
            /* means has the wanted index of this partition */
            result = HeapTupleGetOid(tuple);
            if (!onlyOid) {
                errno_t rc = memcpy_s(outFormData, PARTITION_TUPLE_SIZE, partitionForm, PARTITION_TUPLE_SIZE);
                securec_check(rc, "", "");
            }
            found = true;
            break;
        }
    }
    systable_endscan(scan);
    heap_close(pg_partition, AccessShareLock);

    /*If drop index occur before this function and after pg_get_indexdef_partitions, the index has been deleted now. */
    /*Ext. If drop patition occur, the partition might be deleted. Drop partition just use RowExclusiveLock for parellel
     * performance.*/
    if (!found) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The local index %u on the partition %u not exist.", indexid, partitionid)));
    }

    return result;
}

/*
 * @Description: Given OIDs of index relation and heap partition, get Oid of index partition.
 * @IN indexid: given index relation's OID
 * @IN partitionid: given partition's  OID
 * @Return: Oid of index partition.
 * @See also:
 */
Oid getPartitionIndexOid(Oid indexid, Oid partitionid)
{
    return getPartitionIndexFormData<true>(indexid, partitionid, NULL);
}

/*
 * @Description: Given OIDs of index relation and heap partition, get tablespace Oid of index partition.
 * @IN indexid: given index relation's OID
 * @IN partitionid: given partition's  OID
 * @Return: tablespace Oid of index partition.
 * @See also:
 */
Oid getPartitionIndexTblspcOid(Oid indexid, Oid partitionid)
{
    FormData_pg_partition indexPartFormData;
    Oid indexPartOid = InvalidOid;
    Oid tblspcOid = InvalidOid;

    errno_t rc = memset_s(&indexPartFormData, PARTITION_TUPLE_SIZE, 0, PARTITION_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");

    indexPartOid = getPartitionIndexFormData<false>(indexid, partitionid, &indexPartFormData);
    if (OidIsValid(indexPartOid)) {
        tblspcOid = indexPartFormData.reltablespace;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Invalid Oid of local index %u on the partition %u.", indexid, partitionid)));
    }

    return tblspcOid;
}

/*
 * from index partition oid, get heap partition oid.
 */
Oid indexPartGetHeapPart(Oid indexPart, bool missing_ok)
{
    HeapTuple tuple = NULL;
    Form_pg_partition PartForm = NULL;
    Oid result;

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(indexPart));
    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok)
            return InvalidOid;
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index partition %u", indexPart)));
    }
    Assert(HeapTupleGetOid(tuple) == indexPart);
    PartForm = (Form_pg_partition)GETSTRUCT(tuple);
    result = PartForm->indextblid;
    ReleaseSysCache(tuple);
    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get a list for table partition oid or index partition oid
 * Description	:
 * Input		: relid: oid of a partitioned relation(table or index)
 *			: relkind: relation kind, a table or index?
 * Output	:
 * Notes		: the invoker must be release the list
 */
List* getPartitionObjectIdList(Oid relid, char relkind)
{
    Oid partitionid = InvalidOid;
    List* result = NIL;
    Relation relation = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;

    /* only a table or index can have partitions */
    Assert(PART_OBJ_TYPE_TABLE_PARTITION == relkind || PART_OBJ_TYPE_INDEX_PARTITION == relkind);

    relation = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    scan = systable_beginscan(relation, PartitionParentOidIndexId, true, SnapshotNow, 2, key);
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        partitionid = HeapTupleGetOid(tuple);

        result = lappend_oid(result, partitionid);
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	: give the blid ,find all tuples matched,
 * 					must free the list by call freePartList
 * Notes		:
 */
List* searchPartitionIndexesByblid(Oid blid)
{
    List* l = NULL;
    Relation partRel = NULL;
    ScanKeyData key[1];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    HeapTuple dtp = NULL;

    partRel = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_indextblid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(blid));

    scan = systable_beginscan(partRel, PartitionIndexTableIdIndexId, true, SnapshotNow, 1, key);
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        dtp = heap_copytuple(tup);
        l = lappend(l, dtp);
    }
    systable_endscan(scan);

    heap_close(partRel, AccessShareLock);

    return l;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	: give the parentId and parttype ,find all tuples matched,
 * 					must free the list by call freePartList
 * Notes		:
 */
List* searchPgPartitionByParentId(char parttype, Oid parentId)
{
    List* partition_list = NULL;
    Relation pg_partition = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    HeapTuple dtuple = NULL;

    pg_partition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(parttype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentId));

    scan = systable_beginscan(pg_partition, PartitionParentOidIndexId, true, SnapshotNow, 2, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        dtuple = heap_copytuple(tuple);

        partition_list = lappend(partition_list, dtuple);
    }
    systable_endscan(scan);

    heap_close(pg_partition, AccessShareLock);

    return partition_list;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
void freePartList(List* plist)
{
    ListCell* tuplecell = NULL;
    HeapTuple tuple = NULL;

    foreach (tuplecell, plist) {
        tuple = (HeapTuple)lfirst(tuplecell);

        if (HeapTupleIsValid(tuple)) {
            heap_freetuple_ext(tuple);
        }
    }

    if (PointerIsValid(plist)) {
        pfree_ext(plist);
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	: give the parentId and parttype ,find tuple matched,if more than one tuple
 * 					matched ,throw exception
 *
 * Notes		:
 */
HeapTuple searchPgPartitionByParentIdCopy(char parttype, Oid parentId)
{
    List* list = NIL;
    HeapTuple tuple = NULL;

    list = searchPgPartitionByParentId(parttype, parentId);
    if (NIL == list) {
        tuple = NULL;
    } else if (1 == list->length) {
        tuple = heap_copytuple((HeapTuple)linitial(list));
        freePartList(list);
    } else {

        freePartList(list);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("more than one item satisfied parttype is %c, parentOid is %u", parttype, parentId)));
    }

    return tuple;
}
