/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
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

#include "access/sysattr.h"
#include "access/multi_redo_api.h"
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
#include "utils/knl_relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"

void insertPartitionEntry(Relation pg_partition_desc, Partition new_part_desc, Oid new_part_id,
    PartitionTupleInfo *partTupleInfo)
{
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    HeapTuple tup = NULL;
    Form_pg_partition pd_part = NULL;
    errno_t errorno = EOK;

    pd_part = new_part_desc->pd_part;
    char parttype = pd_part->parttype;
    Assert(parttype == PART_OBJ_TYPE_PARTED_TABLE || parttype == PART_OBJ_TYPE_TOAST_TABLE ||
           parttype == PART_OBJ_TYPE_TABLE_PARTITION || parttype == PART_OBJ_TYPE_INDEX_PARTITION ||
           parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION);

    /* This is a tad tedious, but way cleaner than what we used to do... */
    errorno = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(errorno, "\0", "\0");

    errorno = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(errorno, "\0", "\0");

    values[Anum_pg_partition_relname - 1] = NameGetDatum(&pd_part->relname);
    values[Anum_pg_partition_parttype - 1] = CharGetDatum(parttype);
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
    if (partTupleInfo->pkey != NULL) {
        values[Anum_pg_partition_partkey - 1] = PointerGetDatum(partTupleInfo->pkey);
    } else {
        nulls[Anum_pg_partition_partkey - 1] = true;
    }

    /* interval tablespaces */
    if (partTupleInfo->intablespace != NULL) {
        values[Anum_pg_partition_intablespace - 1] = PointerGetDatum(partTupleInfo->intablespace);
    } else {
        nulls[Anum_pg_partition_intablespace - 1] = true;
    }

    nulls[Anum_pg_partition_intspnum - 1] = true;

    /* interval */
    if (partTupleInfo->interval != (Datum)0) {
        values[Anum_pg_partition_interval - 1] = partTupleInfo->interval;
    } else {
        nulls[Anum_pg_partition_interval - 1] = true;
    }

    /* maxvalue */
    if (partTupleInfo->boundaries != (Datum)0) {
        values[Anum_pg_partition_boundaries - 1] = partTupleInfo->boundaries;
    } else {
        nulls[Anum_pg_partition_boundaries - 1] = true;
    }

    /* transit point */
    if (partTupleInfo->transitionPoint != (Datum)0) {
        values[Anum_pg_partition_transit - 1] = partTupleInfo->transitionPoint;
    } else {
        nulls[Anum_pg_partition_transit - 1] = true;
    }

    if (partTupleInfo->reloptions != (Datum)0) {
        values[Anum_pg_partition_reloptions - 1] = partTupleInfo->reloptions;
        nulls[Anum_pg_partition_reloptions - 1] = false;
    } else {
        nulls[Anum_pg_partition_reloptions - 1] = true;
    }

    /* we only set relfrozenxid64 in a leaf partition, which owns a relfilenode */
    bool isleafpartition =
        ((parttype == PART_OBJ_TYPE_TABLE_PARTITION || parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) &&
        OidIsValid(pd_part->relfilenode));
    if (isleafpartition) {
        values[Anum_pg_partition_relfrozenxid64 - 1] = u_sess->utils_cxt.RecentXmin;

#ifndef ENABLE_MULTIPLE_NODES
        if (!is_cstore_option(RELKIND_RELATION, partTupleInfo->reloptions)) {
            values[Anum_pg_partition_relminmxid - 1] = GetOldestMultiXactId();
        } else {
            values[Anum_pg_partition_relminmxid - 1] = InvalidMultiXactId;
        }
#endif
    } else {
        values[Anum_pg_partition_relfrozenxid64 - 1] = InvalidTransactionId;
#ifndef ENABLE_MULTIPLE_NODES
        values[Anum_pg_partition_relminmxid - 1] = InvalidMultiXactId;
#endif
    }
    if (partTupleInfo->partexprkeyinfo.partkeyexprIsNull) {
        nulls[Anum_pg_partition_partkeyexpr - 1] = true;
    } else if (partTupleInfo->partexprkeyinfo.partkeyIsFunc) {
        if (partTupleInfo->partexprkeyinfo.partExprKeyStr)
            values[Anum_pg_partition_partkeyexpr - 1] = CStringGetTextDatum(partTupleInfo->partexprkeyinfo.partExprKeyStr);
        else
            values[Anum_pg_partition_partkeyexpr - 1] = CStringGetTextDatum("partkeyisfunc");
    } else {
        if (partTupleInfo->partexprkeyinfo.partExprKeyStr)
            values[Anum_pg_partition_partkeyexpr - 1] = CStringGetTextDatum(partTupleInfo->partexprkeyinfo.partExprKeyStr);
        else
            values[Anum_pg_partition_partkeyexpr - 1] = CStringGetTextDatum("");
    }

    if (partTupleInfo->partitionno != INVALID_PARTITION_NO) {
        values[Anum_pg_partition_partitionno - 1] = Int32GetDatum(partTupleInfo->partitionno);
    } else {
        nulls[Anum_pg_partition_partitionno - 1] = true;
    }
    if (partTupleInfo->subpartitionno != INVALID_PARTITION_NO) {
        values[Anum_pg_partition_subpartitionno - 1] = Int32GetDatum(partTupleInfo->subpartitionno);
    } else {
        nulls[Anum_pg_partition_subpartitionno - 1] = true;
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

void ExceptionHandlerForPartition(GetPartitionOidArgs *args, Oid partitionOid, bool missingOk)
{
    // For subpartition, we'll deal with it in SubPartitionGetSubPartitionOid
    if (args->objectType != PART_OBJ_TYPE_TABLE_SUB_PARTITION && !OidIsValid(partitionOid)) {
        if (!missingOk) {
            if (args->partitionName != NULL) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("partition \"%s\" does not exist", args->partitionName)));
            } else {
                /* here partitionOid is InvalidOid, so it's meaningless to print its value */
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Specified partition does not exist")));
            }
        }
    }
}

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
static Oid PartitionGetPartitionOid(GetPartitionOidArgs* args, LOCKMODE lockMode, bool missingOk, bool noWait)
{
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
    uint64 sess_inval_count;
    uint64 thrd_inval_count = 0;
    for (;;) {
        /*
         * Remember this value, so that, after looking up the partition name
         * and locking its OID, we can check whether any invalidation messages
         * have been processed that might require a do-over.
         */
        sess_inval_count = u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {
            thrd_inval_count = t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;
        }

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
        if (EnableLocalSysCache()) {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter &&
                thrd_inval_count == t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter) {
                break;
            }
        } else {
            if (sess_inval_count == u_sess->inval_cxt.SIMCounter) {
                break;
            }
        }

        /*
         * Something may have changed.	Let's repeat the name lookup, to make
         * sure this name still references the same partition it did previously.
         */
        retry = true;
        partitionOldOid = partitionOid;
    }

    ExceptionHandlerForPartition(args, partitionOid, missingOk);

    return partitionOid;
}

static Oid SubPartitionGetSubPartitionOid(GetPartitionOidArgs *args, Oid partitionedRelationOid, Oid *partOidForSubPart,
    LOCKMODE partlock, LOCKMODE subpartlock, bool missingOk, bool noWait)
{
    Relation rel = relation_open(partitionedRelationOid, AccessShareLock);
    List *partOidList = relationGetPartitionOidList(rel);
    ListCell *cell = NULL;
    Oid partitionOid = InvalidOid;
    foreach (cell, partOidList) {
        Oid partOid = lfirst_oid(cell);
        args->partitionedRelOid = partOid;
        partitionOid = PartitionGetPartitionOid(args, subpartlock, missingOk, noWait);
        if (OidIsValid(partitionOid)) {
            if (partlock != NoLock) {
                LockPartitionOid(partitionedRelationOid, partOid, partlock);
            }
            *partOidForSubPart = partOid;
            break;
        }
    }
    relation_close(rel, AccessShareLock);

    if (!OidIsValid(partitionOid)) {
        if (!missingOk) {
            if (args->partitionName != NULL) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("subpartition \"%s\" does not exist", args->partitionName)));
            } else {
                /* here subpartitionOid is InvalidOid, so it's meaningless to print its value */
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Specified subpartition does not exist")));
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
Oid PartitionNameGetPartitionOid(Oid partitionedRelationOid, const char* partitionName, char objectType,
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

    Oid partitionOid = PartitionGetPartitionOid(&args, lockMode, missingOk, noWait);

    relation_close(partitionRelRelation, AccessShareLock);

    return partitionOid;
}

Oid SubPartitionNameGetSubPartitionOid(Oid partitionedRelationOid, const char* subpartitionName, LOCKMODE partlock,
    LOCKMODE subpartlock, bool missingOk, bool noWait, PartitionNameGetPartidCallback callback, void* callback_arg,
    LOCKMODE callbackobj_lockMode, Oid *partOidForSubPart)
{
    if (!OidIsValid(partitionedRelationOid) || !PointerIsValid(subpartitionName)) {
        return InvalidOid;
    }

    GetPartitionOidArgs args;
    /* get partition oid from given name */
    args.givenPartitionName = true;
    args.partitionedRelOid = partitionedRelationOid;
    args.partitionName = subpartitionName;
    args.objectType = PART_OBJ_TYPE_TABLE_SUB_PARTITION;
    args.callback = callback;
    args.callbackArgs = callback_arg;
    args.callbackObjLockMode = callbackobj_lockMode;
    /* the following arguments is not used. */
    args.partitionedRel = NULL;
    args.partKeyValueList = NULL;
    args.topClosed = false;

    Relation partitionRelRelation = relation_open(PartitionRelationId, AccessShareLock);

    Oid subpartitionOid = SubPartitionGetSubPartitionOid(&args, partitionedRelationOid, partOidForSubPart, partlock,
        subpartlock, missingOk, noWait);

    relation_close(partitionRelRelation, AccessShareLock);

    return subpartitionOid;
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
 * @See also: PartitionNameGetPartitionOid()
 */
Oid PartitionValuesGetPartitionOid(Relation rel, List *partKeyValueList, LOCKMODE lockMode, bool topClosed,
    bool missingOk, bool noWait)
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

    return PartitionGetPartitionOid(&args, lockMode, missingOk, noWait);
}

Oid SubPartitionValuesGetSubPartitionOid(Relation rel, List *partKeyValueList, List *subpartKeyValueList,
    LOCKMODE partlock, LOCKMODE subpartlock, bool topClosed, bool missingOk, bool noWait, Oid *partOidForSubPart)
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
    args.objectType = PART_OBJ_TYPE_TABLE_PARTITION;
    args.callback = NULL;
    args.callbackArgs = NULL;
    args.callbackObjLockMode = NoLock;

    *partOidForSubPart = PartitionGetPartitionOid(&args, partlock, missingOk, noWait);
    if (!OidIsValid(*partOidForSubPart)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The partition which owns the subpartition is missing"),
            errdetail("N/A"),
            errcause("Maybe the subpartition table is dropped"),
            erraction("Check system table 'pg_partition' for more information")));
    }

    Partition part = partitionOpen(rel, *partOidForSubPart, NoLock);
    Relation partrel = partitionGetRelation(rel, part);
    Oid subpartOid =
        PartitionValuesGetPartitionOid(partrel, subpartKeyValueList, subpartlock, topClosed, missingOk, noWait);
    releaseDummyRelation(&partrel);
    partitionClose(rel, part, NoLock);

    return subpartOid;
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

bool isSubPartitionedObject(Oid relid, char relkind, bool missing_ok)
{
    HeapTuple reltuple = NULL;
    Form_pg_class relation = NULL;
    bool result = false;

    reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(reltuple)) {
        relation = (Form_pg_class)GETSTRUCT(reltuple);
        if (relation->parttype == PARTTYPE_SUBPARTITIONED_RELATION && relation->relkind == relkind) {
            result = true;
        }
        ReleaseSysCache(reltuple);
    }

    if (!result && !missing_ok) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("the object with oid %u is not a subpartitioned object", relid)));
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
    HeapTuple tuple = NULL;
    Oid result = InvalidOid;
    CatCList* catlist = NULL;
    Form_pg_partition partitionForm = NULL;
    int elevel = ERROR;

    catlist = SearchSysCacheList2(PARTINDEXTBLPARENTOID, ObjectIdGetDatum(partitionid), ObjectIdGetDatum(indexid));

    /*If drop index occur before this function and after pg_get_indexdef_partitions, the index has been deleted now. */
    /*Ext. If drop patition occur, the partition might be deleted. Drop partition just use RowExclusiveLock for parellel
     * performance.*/
    if (catlist->n_members == 0 ){
        ReleaseSysCacheList(catlist);
        Oid redis_ns = get_namespace_oid("data_redis", true);
        if (OidIsValid(redis_ns) && redis_ns == get_rel_namespace(indexid)) {
            elevel = WARNING;
        }
        ereport(elevel,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("The local index %u on the partition %u not exist.", indexid, partitionid),
                errhint("In redistribution, local parititon index maybe not exists.")));
        return InvalidOid;
    }
    tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, 0);
    partitionForm = (Form_pg_partition)GETSTRUCT(tuple);
    result = HeapTupleGetOid(tuple);
    if (!onlyOid) {
        errno_t rc = memcpy_s(outFormData, PARTITION_TUPLE_SIZE, partitionForm, PARTITION_TUPLE_SIZE);
        securec_check(rc, "", "");
    }
    ReleaseSysCacheList(catlist);
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
 * @Description: Given OIDs of index relation and heap partition, get relname of index partition.
 * @IN indexid: given index relation's OID
 * @IN partitionid: given partition's  OID
 * @Return: relname Oid of index partition.
 * @See also:
 */
char* getPartitionIndexName(Oid indexid, Oid partitionid)
{
    FormData_pg_partition indexPartFormData;
    Oid indexPartOid = InvalidOid;
    char* name = NULL;

    errno_t rc = memset_s(&indexPartFormData, PARTITION_TUPLE_SIZE, 0, PARTITION_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");

    indexPartOid = getPartitionIndexFormData<false>(indexid, partitionid, &indexPartFormData);
    if (OidIsValid(indexPartOid)) {
        name = pstrdup(NameStr(indexPartFormData.relname));
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Invalid Oid of local index %u on the partition %u.", indexid, partitionid)));
    }

    return name;
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
    Assert(PART_OBJ_TYPE_TABLE_PARTITION == relkind ||
           PART_OBJ_TYPE_INDEX_PARTITION == relkind ||
           PART_OBJ_TYPE_TABLE_SUB_PARTITION == relkind);

    relation = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    scan = systable_beginscan(relation, PartitionParentOidIndexId, true, NULL, 2, key);
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        partitionid = HeapTupleGetOid(tuple);

        result = lappend_oid(result, partitionid);
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return result;
}

List* getSubPartitionObjectIdList(Oid relid)
{
    List *partid_list = getPartitionObjectIdList(relid, PART_OBJ_TYPE_TABLE_PARTITION);
    List *subpartid_list = NIL;
    ListCell *cell;
    Oid partid = InvalidOid;
    foreach (cell, partid_list) {
        partid = lfirst_oid(cell);
        subpartid_list = list_concat(subpartid_list,
            getPartitionObjectIdList(partid, PART_OBJ_TYPE_TABLE_SUB_PARTITION));
    }
    list_free_ext(partid_list);
    return subpartid_list;
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

    scan = systable_beginscan(partRel, PartitionIndexTableIdIndexId, true, NULL, 1, key);
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
List* searchPgPartitionByParentId(char parttype, Oid parentId, ScanDirection direction)
{
    List* partition_list = NULL;
    Relation pg_partition = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    HeapTuple dtuple = NULL;

    /*
     * Note that all partitions have the same parentid, the tuple inserted last will be read first if we use the index
     * on parentid and scan forward.
     * So when we want to obtain all partition tuples in order, MUST use systable_getnext_back instead.
     */
    Assert(ScanDirectionIsForward(direction) || ScanDirectionIsBackward(direction));
    HeapTuple (*ScanMethod)(SysScanDesc);
    ScanMethod = ScanDirectionIsForward(direction) ? systable_getnext : systable_getnext_back;

    pg_partition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(parttype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentId));

    /*
     * The caller might need a tuple that's newer than the one the historic
     * snapshot; currently the only case requiring to do so is looking up the
     * relfilenode of non mapped system relations during decoding.
     */
    Snapshot snapshot = NULL;
    snapshot = SnapshotNow;
    if (HistoricSnapshotActive() || IS_EXRTO_RECOVERY_IN_PROGRESS) {
        snapshot = GetCatalogSnapshot();
    }

    scan = systable_beginscan(pg_partition, PartitionParentOidIndexId, true, snapshot, 2, key);
    while (HeapTupleIsValid(tuple = ScanMethod(scan))) {
        dtuple = heap_copytuple(tuple);

        partition_list = lappend(partition_list, dtuple);
    }
    systable_endscan(scan);

    heap_close(pg_partition, AccessShareLock);

    return partition_list;
}

/*
 * Get the sub-partition list-list
 */
List* searchPgSubPartitionByParentId(char parttype, List *partTuples, ScanDirection direction)
{
    List *result = NIL;
    ListCell *lc = NULL;

    foreach (lc, partTuples)
    {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        Oid parentOid = HeapTupleGetOid(tuple);
        List *subPartTuples = searchPgPartitionByParentId(parttype, parentOid, direction);
        result = lappend(result, subPartTuples);
    }

    return result;
}

void freePartList(List* plist)
{
    ListCell* tuplecell = NULL;
    HeapTuple tuple = NULL;

    tuplecell = list_head(plist);
    while (tuplecell != NULL) {
        ListCell* tmp = tuplecell;
        tuple = (HeapTuple)lfirst(tuplecell);

        tuplecell = lnext(tuplecell);
        
        if (HeapTupleIsValid(tuple)) {
            heap_freetuple_ext(tuple);
        }
        
        pfree(tmp);
    }

    if (PointerIsValid(plist)) {
        pfree_ext(plist);
    }
}

static int PartOidGetPartitionNo(PartitionMap *map, Oid partOid)
{
    Oid partitionId = InvalidOid;
    int partitionno = INVALID_PARTITION_NO;
    int sumtotal = getPartitionNumber(map);

    for (int conuter = 0; conuter < sumtotal; ++conuter) {
        if (map->type == PART_TYPE_LIST) {
            partitionId = ((ListPartitionMap*)map)->listElements[conuter].partitionOid;
            partitionno = ((ListPartitionMap*)map)->listElements[conuter].partitionno;
        } else if (map->type == PART_TYPE_HASH) {
            partitionId = ((HashPartitionMap*)map)->hashElements[conuter].partitionOid;
            partitionno = ((HashPartitionMap*)map)->hashElements[conuter].partitionno;
        } else {
            partitionId = ((RangePartitionMap*)map)->rangeElements[conuter].partitionOid;
            partitionno = ((RangePartitionMap*)map)->rangeElements[conuter].partitionno;
        }
        if (partitionId == partOid) {
            return partitionno;
        }
    }

    return INVALID_PARTITION_NO;
}

/* IMPORTANT: This function will case invalidation message process,  the relation may
              be rebuild,  and the relation->partMap may be changed.
              After call this founction,  should not call getNumberOfRangePartitions/getNumberOfPartitions,
              using list_length(partitionList) instead
 */
List* relationGetPartitionList(Relation relation, LOCKMODE lockmode)
{
    List* partitionOidList = NIL;
    List* partitionList = NIL;
    ListCell* cell = NULL;
    Oid partitionId = InvalidOid;
    Partition partition = NULL;

    incre_partmap_refcount(relation->partMap);
    partitionOidList = relationGetPartitionOidList(relation);
    if (!PointerIsValid(partitionOidList)) {
        decre_partmap_refcount(relation->partMap);
        return NIL;
    }

    foreach (cell, partitionOidList) {
        partitionId = lfirst_oid(cell);
        Assert(OidIsValid(partitionId));
        partition = tryPartitionOpen(relation, partitionId, lockmode);
        if (!PartitionIsValid(partition)) {
            PARTITION_LOG("could not open partition with partition oid %u, partitionno will be used to search the "
                          "new partition", partitionId);
            int partitionno = PartOidGetPartitionNo(relation->partMap, partitionId);
            if (!PARTITIONNO_IS_VALID(partitionno)) {
                continue;
            }
            partition = PartitionOpenWithPartitionno(relation, partitionId, partitionno, lockmode);
        }

        partitionList = lappend(partitionList, partition);
    }
    list_free_ext(partitionOidList);
    decre_partmap_refcount(relation->partMap);

    return partitionList;
}

List* RelationGetSubPartitionList(Relation relation, LOCKMODE lockmode)
{
    List* partitionOidList = NIL;
    List* subPartList = NIL;
    ListCell* cell = NULL;
    Oid partitionId = InvalidOid;
    Partition partition = NULL;

    incre_partmap_refcount(relation->partMap);
    partitionOidList = relationGetPartitionOidList(relation);
    if (!PointerIsValid(partitionOidList)) {
        decre_partmap_refcount(relation->partMap);
        return NIL;
    }

    foreach (cell, partitionOidList) {
        partitionId = lfirst_oid(cell);
        Assert(OidIsValid(partitionId));
        partition = tryPartitionOpen(relation, partitionId, lockmode);
        if (!PartitionIsValid(partition)) {
            PARTITION_LOG("could not open partition with partition oid %u, partitionno will be used to search the "
                          "new partition", partitionId);
            int partitionno = PartOidGetPartitionNo(relation->partMap, partitionId);
            if (!PARTITIONNO_IS_VALID(partitionno)) {
                continue;
            }
            partition = PartitionOpenWithPartitionno(relation, partitionId, partitionno, lockmode);
        }

        Relation partRel = partitionGetRelation(relation, partition);
        List* subPartListTmp = relationGetPartitionList(partRel, lockmode);
        subPartList = list_concat(subPartList, subPartListTmp);
        releaseDummyRelation(&partRel);
        partitionClose(relation, partition, lockmode);
    }

    list_free_ext(partitionOidList);
    decre_partmap_refcount(relation->partMap);

    return subPartList;
}

/* give one partitioned relation, return a list, consisting of relname of all its partition and subpartition */
List* RelationGetPartitionNameList(Relation relation)
{
    List* partnameList = NIL;
    List *partTupleList = NIL;
    if (!RelationIsPartitioned(relation)) {
        return partnameList;
    }

    if (RelationIsPartitionOfSubPartitionTable(relation)) { /* a partition of subpartition */
        partTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION, relation->rd_id);
        Assert(PointerIsValid(partTupleList));

        ListCell *partCell = NULL;
        Form_pg_partition partitionTuple = NULL;
        foreach (partCell, partTupleList) {
            partitionTuple = (Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(partCell));
            partnameList = lappend(partnameList, pstrdup(partitionTuple->relname.data));
        }
        list_free_deep(partTupleList);
    } else { /* a relation */
        partTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, relation->rd_id);
        Assert(PointerIsValid(partTupleList));

        ListCell *partCell = NULL;
        Form_pg_partition partitionTuple = NULL;
        foreach (partCell, partTupleList) {
            partitionTuple = (Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(partCell));
            partnameList = lappend(partnameList, pstrdup(partitionTuple->relname.data));
            if (RelationIsSubPartitioned(relation)) {
                List *subpartTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION,
                    HeapTupleGetOid((HeapTuple)lfirst(partCell)));

                ListCell *subpartCell = NULL;
                Form_pg_partition subpartitionTuple = NULL;
                foreach (subpartCell, subpartTupleList) {
                    subpartitionTuple = (Form_pg_partition)GETSTRUCT((HeapTuple)lfirst(subpartCell));
                    partnameList = lappend(partnameList, pstrdup(subpartitionTuple->relname.data));
                }
                list_free_deep(subpartTupleList);
            }
        }
        list_free_deep(partTupleList);
    }

    return partnameList;
}

void RelationGetSubpartitionInfo(Relation relation, char *subparttype, List **subpartKeyPosList,
    int2vector **subpartitionKey)
{
    if (!RelationIsSubPartitioned(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Un-support feature"),
            errdetail("Can not get subpartition information for NON-SUBPARTITIONED table"),
            errcause("Try get the subpartition information on a NON-SUBPARTITIONED object"),
            erraction("Check system table 'pg_partition' for more information")));
    }

    Oid partid = InvalidOid;
    ArrayType* pos_array = NULL;
    Datum pos_raw = (Datum)0;
    bool isNull = false;

    Relation pg_partition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relation->rd_id));
    scan = systable_beginscan(pg_partition, PartitionParentOidIndexId, true, NULL, 2, key);
    if (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        partid = HeapTupleGetOid(tuple);
        pos_raw = heap_getattr(tuple, Anum_pg_partition_partkey, RelationGetDescr(pg_partition), &isNull);
    }
    systable_endscan(scan);

    if (isNull) {
        heap_close(pg_partition, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("Null partition key value for relation \"%s\"", RelationGetRelationName(relation)),
            errdetail("N/A"),
            errcause("Unexpected partition tuple in pg_partition for this relation"),
            erraction("Check system table 'pg_partition' for more information")));
    }
    pos_array = DatumGetArrayTypeP(pos_raw);
    int ncolumn = ARR_DIMS(pos_array)[0];
    int16* attnums = (int16*)ARR_DATA_PTR(pos_array);
    Assert(ncolumn > 0);
    if (PointerIsValid(subpartitionKey)) {
        *subpartitionKey = buildint2vector(attnums, ncolumn);
    }
    if (PointerIsValid(subpartKeyPosList)) {
        for (int i = 0; i < ncolumn; i++) {
            *subpartKeyPosList = lappend_int(*subpartKeyPosList, (int)(attnums[i]) - 1);
        }
    }

    if (PointerIsValid(subparttype)) {
        ScanKeyData subkey[2];
        SysScanDesc subscan = NULL;
        HeapTuple subtuple = NULL;
        ScanKeyInit(&subkey[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
            CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));
        ScanKeyInit(&subkey[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partid));
        subscan = systable_beginscan(pg_partition, PartitionParentOidIndexId, true, NULL, 2, subkey);
        if (HeapTupleIsValid((subtuple = systable_getnext(subscan)))) {
            *subparttype = ((Form_pg_partition)GETSTRUCT(subtuple))->partstrategy;
        } else {
            *subparttype = PART_STRATEGY_INVALID;
        }
        systable_endscan(subscan);
    }

    heap_close(pg_partition, AccessShareLock);
}

// give one partitioned index  relation,
// return a list, consisting of oid of all its index partition
List* indexGetPartitionOidList(Relation indexRelation)
{
    List* indexPartitionOidList = NIL;
    List* indexPartitionTupleList = NIL;
    ListCell* cell = NULL;

    if (indexRelation->rd_rel->relkind != RELKIND_INDEX || RelationIsNonpartitioned(indexRelation))
        return indexPartitionOidList;
    indexPartitionTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_INDEX_PARTITION, indexRelation->rd_id);
    foreach (cell, indexPartitionTupleList) {
        Oid indexPartOid = HeapTupleGetOid((HeapTuple)lfirst(cell));
        if (OidIsValid(indexPartOid)) {
            indexPartitionOidList = lappend_oid(indexPartitionOidList, indexPartOid);
        }
    }

    freePartList(indexPartitionTupleList);
    return indexPartitionOidList;
}

List* indexGetPartitionList(Relation indexRelation, LOCKMODE lockmode)
{
    List* indexPartitionList = NIL;
    List* indexPartitionTupleList = NIL;
    ListCell* cell = NULL;

    if (indexRelation->rd_rel->relkind != RELKIND_INDEX || RelationIsNonpartitioned(indexRelation))
        return indexPartitionList;
    indexPartitionTupleList = searchPgPartitionByParentId(PART_OBJ_TYPE_INDEX_PARTITION, indexRelation->rd_id);
    foreach (cell, indexPartitionTupleList) {
        Partition indexPartition = NULL;
        Oid indexPartOid = HeapTupleGetOid((HeapTuple)lfirst(cell));

        if (OidIsValid(indexPartOid)) {
            indexPartition = partitionOpen(indexRelation, indexPartOid, lockmode);
            indexPartitionList = lappend(indexPartitionList, indexPartition);
        }
    }

    freePartList(indexPartitionTupleList);
    return indexPartitionList;
}

Relation SubPartitionGetRelation(Relation heap, Partition subPart, LOCKMODE lockmode)
{
    Oid partOid = subPart->pd_part->parentid;
    Partition part = partitionOpen(heap, partOid, lockmode);
    Relation partRel = partitionGetRelation(heap, part);
    Relation subPartRel = partitionGetRelation(partRel, subPart);
    releaseDummyRelation(&partRel);
    partitionClose(heap, part, lockmode);

    return subPartRel;
}

Partition SubPartitionOidGetPartition(Relation rel, Oid subPartOid, LOCKMODE lockmode)
{
    /* this function is used for partitionOpen(relOid, subpartOid). Do not use it as much as possible.
     * We cannot add high lock on the partOid in case of deadlock. */
    LOCKMODE partlock = lockmode > ShareUpdateExclusiveLock ? ShareUpdateExclusiveLock : lockmode;

    Oid parentOid = partid_get_parentid(subPartOid);
    Partition part = partitionOpen(rel, parentOid, partlock);
    Relation partRel = partitionGetRelation(rel, part);
    Partition subPart = partitionOpen(partRel, subPartOid, lockmode);
    releaseDummyRelation(&partRel);
    partitionClose(rel, part, NoLock);

    return subPart;
}

Relation SubPartitionOidGetParentRelation(Relation rel, Oid subPartOid, LOCKMODE lockmode)
{
    Oid parentOid = partid_get_parentid(subPartOid);
    Partition part = partitionOpen(rel, parentOid, lockmode);
    Relation partRel = partitionGetRelation(rel, part);
    partitionClose(rel, part, lockmode);

    return partRel;
}

void releasePartitionList(Relation relation, List** partList, LOCKMODE lockmode, bool validCheck)
{
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;

    foreach (cell, *partList) {
        partition = (Partition)lfirst(cell);
        Assert(!validCheck || PointerIsValid(partition));
        if (PointerIsValid(partition)) {
            if (partition->pd_part->parentid != relation->rd_id) {
                partRel = SubPartitionOidGetParentRelation(relation, partition->pd_id, lockmode);
                partitionClose(partRel, partition, lockmode);
                releaseDummyRelation(&partRel);
            } else {
                partitionClose(relation, partition, lockmode);
            }
        }
    }

    list_free_ext(*partList);
    *partList = NULL;
}

void releaseSubPartitionList(Relation relation, List** partList, LOCKMODE lockmode)
{
    ListCell* cell = NULL;
    List *subpartList = NULL;
    Partition subPart = NULL;
    Oid partOid = InvalidOid;
    Partition part = NULL;
    Relation partRel = NULL;

    foreach (cell, *partList) {
        subpartList = (List *)lfirst(cell);
        Assert(PointerIsValid(subpartList));
        if (RelationIsIndex(relation)) {
            releasePartitionList(relation, &subpartList, lockmode);
        } else {
            subPart = (Partition)linitial(subpartList);
            partOid = subPart->pd_part->parentid;
            part = partitionOpen(relation, partOid, lockmode);
            partRel = partitionGetRelation(relation, part);
            releasePartitionList(partRel, &subpartList, lockmode);
            releaseDummyRelation(&partRel);
            partitionClose(relation, part, lockmode);
        }
    }

    list_free_ext(*partList);
    *partList = NULL;
}

/*
 * Get partition list, free with releasePartitionOidList.
 */
List* relationGetPartitionOidList(Relation rel)
{
    List* result = NIL;
    Oid partitionId = InvalidOid;

    if (rel == NULL || rel->partMap == NULL) {
        return NIL;
    }

    PartitionMap* map = rel->partMap;
    int sumtotal = getPartitionNumber(map);
    for (int conuter = 0; conuter < sumtotal; ++conuter) {
        if (map->type == PART_TYPE_LIST) {
            partitionId = ((ListPartitionMap*)map)->listElements[conuter].partitionOid;
        } else if (map->type == PART_TYPE_HASH) {
            partitionId = ((HashPartitionMap*)map)->hashElements[conuter].partitionOid;
        } else {
            partitionId = ((RangePartitionMap*)map)->rangeElements[conuter].partitionOid;
        }
        result = lappend_oid(result, partitionId);
    }

    return result;
}

/*
 * Get sub-partition list, free with releasePartitionOidList.
 */
List* RelationGetSubPartitionOidList(Relation rel, LOCKMODE lockmode, bool estimate)
{
    if (!RelationIsSubPartitioned(rel))
        return NULL;

    List *parts = relationGetPartitionOidList(rel);
    List *result = NULL;
    ListCell *lc = NULL;
    Partition part = NULL;
    foreach (lc, parts)
    {
        Oid partOid = lfirst_oid(lc);
        if (estimate) {
            if (!ConditionalLockPartition(rel->rd_id, partOid, lockmode, PARTITION_LOCK))
                continue;
            part = tryPartitionOpen(rel, partOid, NoLock);
            if (part == NULL)
                continue;
        } else {
            part = partitionOpen(rel, partOid, lockmode);
        }

        Relation partRel = partitionGetRelation(rel, part);
        List *subParts = relationGetPartitionOidList(partRel);
        result = list_concat(result, subParts);
        releaseDummyRelation(&partRel);
        partitionClose(rel, part, lockmode);
    }

    return result;
}

/*
 * Get sub-partition list-list, free with ReleaseSubPartitionOidList.
 */
List* RelationGetSubPartitionOidListList(Relation rel)
{
    if (!RelationIsSubPartitioned(rel))
        return NULL;

    List *parts = relationGetPartitionOidList(rel);
    List *result = NULL;
    ListCell *lc = NULL;
    foreach (lc, parts)
    {
        Oid partOid = lfirst_oid(lc);
        Partition part = partitionOpen(rel, partOid, AccessShareLock);
        Relation partRel = partitionGetRelation(rel, part);
        List *subParts = relationGetPartitionOidList(partRel);
        result = lappend(result, subParts);
        releaseDummyRelation(&partRel);
        partitionClose(rel, part, AccessShareLock);
    }

    return result;
}

void releasePartitionOidList(List** partList)
{
    if (PointerIsValid(partList)) {
        list_free_ext(*partList);

        *partList = NIL;
    }
}

void ReleaseSubPartitionOidList(List** partList)
{
    ListCell* cell = NULL;
    List *subPartOidList = NULL;

    foreach (cell, *partList) {
        subPartOidList = (List *)lfirst(cell);
        releasePartitionOidList(&subPartOidList);
    }

    list_free_ext(*partList);
    *partList = NULL;
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

/*
 * @@GaussDB@@
 * Target		: GetBaseRelOid
 * Brief		:
 * Description	: For partition, return parentId, For indexpartition, return parentId,
 *  				  For subpartition, return grandparentId, For indexsubpartition, return parentId,
 *
 * Notes		:
 */
Oid GetBaseRelOidOfParition(Relation relation)
{
    Assert(RelationIsPartition(relation));

    if (RelationIsSubPartitionOfSubPartitionTable(relation)) {
        return relation->grandparentId;
    }

    return relation->parentId;
}

/* Set PARTITION OBJECT_LOCK */
void LockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type)
{
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    LockDatabaseObject(relOid, object, 0, type);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

    const char* objectname = (object == PARTITION_OBJECT_LOCK_SDEQUENCE) ? "partition": "interval";
    PARTITION_LOG("Successfully obtain %s object lock: relid %u locklevel %d", objectname, relOid, type);
}

/* Release PARTITION OBJECT_LOCK */
void UnlockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type)
{
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    UnlockDatabaseObject(relOid, object, 0, type);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

    const char* objectname = (object == PARTITION_OBJECT_LOCK_SDEQUENCE) ? "partition": "interval";
    PARTITION_LOG("Successfully release %s object lock: relid %u locklevel %d", objectname, relOid, type);
}

bool ConditionalLockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type)
{
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    bool flag = ConditionalLockDatabaseObject(relOid, object, 0, type);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

    if (flag) {
        const char* objectname = (object == PARTITION_OBJECT_LOCK_SDEQUENCE) ? "partition": "interval";
        PARTITION_LOG("Successfully obtain %s object lock: relid %u locklevel %d", objectname, relOid, type);
    }
    return flag;
}

#ifndef ENABLE_MULTIPLE_NODES
/* Set DML tag for a partitioned table */
void AddPartitionDMLInfo(Oid relOid)
{
    MemoryContext old_cxt = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    u_sess->storage_cxt.partition_dml_oids = lappend_oid(u_sess->storage_cxt.partition_dml_oids, relOid);
    MemoryContextSwitchTo(old_cxt);
}

/* Set DDL tag for a partitioned table */
void AddPartitionDDLInfo(Oid relOid)
{
    MemoryContext old_cxt = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    u_sess->storage_cxt.partition_ddl_oids = lappend_oid(u_sess->storage_cxt.partition_ddl_oids, relOid);
    MemoryContextSwitchTo(old_cxt);
}

/* Add AccessExclusiveLock for all partitioned tables added in AddPartitionDDLInfo */
void LockPartitionDDLOperation()
{
    ListCell *cell;
    foreach(cell, u_sess->storage_cxt.partition_ddl_oids) {
        Oid relid = lfirst_oid(cell);
        LockPartitionObject(relid, PARTITION_OBJECT_LOCK_SDEQUENCE, PARTITION_EXCLUSIVE_LOCK);
    }
}
#endif

HeapTuple ScanPgPartition(Oid targetPartId, bool indexOK, Snapshot snapshot)
{
    HeapTuple pg_partition_tuple;
    Relation pg_partition_desc;
    SysScanDesc pg_partition_scan;
    ScanKeyData key[1];

    /*
     * If something goes wrong during backend startup, we might find ourselves
     * trying to read pg_partition before we've selected a database.  That ain't
     * gonna work, so bail out with a useful error message.  If this happens,
     * it probably means a partcache entry that needs to be nailed isn't.
     */
    if (!OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
        ereport(FATAL,
            (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("cannot read pg_class without having selected a database")));
    }

    if (snapshot == NULL) {
        snapshot = GetCatalogSnapshot();
    }

    /*
     * form a scan key
     */
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(targetPartId));

    /*
     * Open pg_partition and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical partcache entries (this includes initdb and startup
     * without a pg_internal.init file).  The caller can also force a heap
     * scan by setting indexOK == false.
     */
    pg_partition_desc = heap_open(PartitionRelationId, AccessShareLock);
    pg_partition_scan = systable_beginscan(pg_partition_desc,
        PartitionOidIndexId,
        indexOK && LocalRelCacheCriticalRelcachesBuilt(),
        snapshot,
        1,
        key);

    pg_partition_tuple = systable_getnext(pg_partition_scan);

    /*
     * Must copy tuple before releasing buffer.
     */
    if (HeapTupleIsValid(pg_partition_tuple)) {
        pg_partition_tuple = heap_copytuple(pg_partition_tuple);
    }

    /* all done */
    systable_endscan(pg_partition_scan);
    heap_close(pg_partition_desc, AccessShareLock);

    return pg_partition_tuple;
}

/* get the oid of pg_partition entry from a partitioned table's oid */
Oid RelOidGetPartitionTupleid(Oid relOid)
{
    Relation pg_partition;
    HeapTuple tup;
    ScanKeyData key[2];
    SysScanDesc scan;
    Oid partOid;

    pg_partition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relOid));

    scan = systable_beginscan(pg_partition, PartitionParentOidIndexId, true, NULL, 2, key);
    if (!HeapTupleIsValid((tup = systable_getnext(scan)))) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for relation %u", relOid)));
    }

    partOid = HeapTupleGetOid(tup);
    systable_endscan(scan);
    heap_close(pg_partition, AccessShareLock);

    return partOid;
}

/* get the partitionno of a pg_partition entry */
int GetCurrentPartitionNo(Oid partOid)
{
    HeapTuple tup;
    Datum datum;
    bool isNull;
    int partitionno;

    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(tup)) {
        return INVALID_PARTITION_NO;
    }
    datum = SysCacheGetAttr(PARTRELID, tup, Anum_pg_partition_partitionno, &isNull);
    if (isNull) {
        partitionno = INVALID_PARTITION_NO;
    } else {
        partitionno = DatumGetInt32(datum);
        Assert(partitionno != INVALID_PARTITION_NO);
    }
    ReleaseSysCache(tup);

    return partitionno;
}

/* get the subpartitionno of a pg_partition entry */
int GetCurrentSubPartitionNo(Oid partOid)
{
    HeapTuple tup;
    Datum datum;
    bool isNull;
    int subpartitionno;

    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(tup)) {
        return INVALID_PARTITION_NO;
    }
    datum = SysCacheGetAttr(PARTRELID, tup, Anum_pg_partition_subpartitionno, &isNull);
    if (isNull) {
        subpartitionno = INVALID_PARTITION_NO;
    } else {
        subpartitionno = DatumGetInt32(datum);
        Assert(subpartitionno != INVALID_PARTITION_NO);
    }
    ReleaseSysCache(tup);

    return subpartitionno;
}

/* update the partitionno to new value */
void UpdateCurrentPartitionNo(Oid partOid, int partitionno, bool inplace)
{
    Relation pg_partition;
    HeapTuple tup;
    HeapTuple ntup;
    Datum values[Natts_pg_partition] = {0};
    bool nulls[Natts_pg_partition] = {false};
    bool replaces[Natts_pg_partition] = {false};

    if (GetCurrentPartitionNo(partOid) == partitionno) {
        return;
    }

    int absno = DatumGetInt32(DirectFunctionCall1(int4abs, Int32GetDatum(partitionno)));
    if (absno > MAX_PARTITION_NO) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("current partitionno %d is out of range", partitionno)));
    }

    pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for partition %u", partOid)));
    }

    replaces[Anum_pg_partition_partitionno - 1] = true;
    if (partitionno == INVALID_PARTITION_NO) {
        nulls[Anum_pg_partition_partitionno - 1] = true;
    } else {
        values[Anum_pg_partition_partitionno - 1] = Int32GetDatum(partitionno);
    }
    ntup = heap_modify_tuple(tup, RelationGetDescr(pg_partition), values, nulls, replaces);

    /* Only the tuple of partitioned table can use inplace-update! Since wait_clean_cpi is set inplace-update, we keep
       consistent. Because the partitionno of partitioned table is only used to mask the max value, rollback will not
       violate it. */
    if (inplace) {
        heap_inplace_update(pg_partition, ntup);
    } else {
        simple_heap_update(pg_partition, &ntup->t_self, ntup);
        CatalogUpdateIndexes(pg_partition, ntup);
    }

    heap_freetuple_ext(ntup);
    ReleaseSysCache(tup);
    heap_close(pg_partition, RowExclusiveLock);

    CommandCounterIncrement();
}

/* update the subpartitionno to new value */
void UpdateCurrentSubPartitionNo(Oid partOid, int subpartitionno)
{
    Relation pg_partition;
    HeapTuple tup;
    HeapTuple ntup;
    Datum values[Natts_pg_partition] = {0};
    bool nulls[Natts_pg_partition] = {false};
    bool replaces[Natts_pg_partition] = {false};

    if (GetCurrentSubPartitionNo(partOid) == subpartitionno) {
        return;
    }

    int absno = DatumGetInt32(DirectFunctionCall1(int4abs, Int32GetDatum(subpartitionno)));
    if (absno > MAX_PARTITION_NO) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("current subpartitionno %d is out of range", subpartitionno)));
    }

    pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partOid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("cache lookup failed for subpartition %u", partOid)));
    }

    replaces[Anum_pg_partition_subpartitionno - 1] = true;
    if (subpartitionno == INVALID_PARTITION_NO) {
        nulls[Anum_pg_partition_subpartitionno - 1] = true;
    } else {
        values[Anum_pg_partition_subpartitionno - 1] = Int32GetDatum(subpartitionno);
    }
    ntup = heap_modify_tuple(tup, RelationGetDescr(pg_partition), values, nulls, replaces);

    simple_heap_update(pg_partition, &ntup->t_self, ntup);
    CatalogUpdateIndexes(pg_partition, ntup);

    heap_freetuple_ext(ntup);
    ReleaseSysCache(tup);
    heap_close(pg_partition, RowExclusiveLock);

    CommandCounterIncrement();
    CacheInvalidatePartcacheByPartid(partOid);
}

Oid GetPartOidWithPartitionno(Oid parentid, int partitionno, char parttype)
{
    SysScanDesc scan;
    ScanKeyData key[2];
    Relation partition = NULL;
    HeapTuple tuple = NULL;
    Oid newpartOid = InvalidOid;
    int newpartno;
    Datum datum;
    bool isNull = false;

    Assert(parttype == PART_OBJ_TYPE_TABLE_PARTITION || parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION);
    bool issubpartition = (parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION);

    Assert(PARTITIONNO_IS_VALID(partitionno));

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(parttype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentid));
    partition = heap_open(PartitionRelationId, AccessShareLock);
    scan = systable_beginscan(partition, PartitionParentOidIndexId, true, NULL, 2, key);
    while (HeapTupleIsValid((tuple = systable_getnext(scan)))) {
        if (issubpartition) {
            datum = heap_getattr(tuple, Anum_pg_partition_subpartitionno, RelationGetDescr(partition), &isNull);
        } else {
            datum = heap_getattr(tuple, Anum_pg_partition_partitionno, RelationGetDescr(partition), &isNull);
        }
        newpartno = isNull ? INVALID_PARTITION_NO : DatumGetInt32(datum);
        if (newpartno == partitionno) {
            newpartOid = HeapTupleGetOid(tuple);
            break;
        }
    }
    systable_endscan(scan);
    heap_close(partition, AccessShareLock);

    return newpartOid;
}

Oid InvisiblePartidGetNewPartid(Oid partOid)
{
    HeapTuple partTuple = NULL;
    Form_pg_partition partForm = NULL;
    Relation partition = NULL;
    char parttype;
    Oid parentid = InvalidOid;
    Datum datum;
    bool isNull = false;
    int partitionno;

    partTuple = ScanPgPartition(partOid, false, SnapshotAny);
    if (!HeapTupleIsValid(partTuple)) {
        return InvalidOid;
    }

    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    parttype = partForm->parttype;
    parentid = partForm->parentid;
    partition = heap_open(PartitionRelationId, AccessShareLock);
    if (partForm->parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
        datum = heap_getattr(partTuple, Anum_pg_partition_partitionno, RelationGetDescr(partition), &isNull);
    } else if (partForm->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
        datum = heap_getattr(partTuple, Anum_pg_partition_subpartitionno, RelationGetDescr(partition), &isNull);
    } else {
        pfree_ext(partTuple);
        heap_close(partition, AccessShareLock);
        return InvalidOid;
    }
    partitionno = isNull ? INVALID_PARTITION_NO : DatumGetInt32(datum);
    pfree_ext(partTuple);
    heap_close(partition, AccessShareLock);

    if (!PARTITIONNO_IS_VALID(partitionno)) {
        return InvalidOid;
    }
    return GetPartOidWithPartitionno(parentid, partitionno, parttype);
}

/* init all partitionno/subpartitionno for a partition definition */
void SetPartitionnoForPartitionState(PartitionState *partTableState)
{
    ListCell* cell = NULL;
    ListCell* subcell = NULL;
    int partitionno = 0;
    foreach (cell, partTableState->partitionList) {
        partitionno++;
        PartitionDefState* partitionDefState = (PartitionDefState*)lfirst(cell);
        partitionDefState->partitionno = partitionno;
        int subpartitionno = 0;
        foreach(subcell, partitionDefState->subPartitionDefState) {
            subpartitionno++;
            PartitionDefState* subpartitionDefState = (PartitionDefState*)lfirst(subcell);
            subpartitionDefState->partitionno = subpartitionno;
        }
    }
}

/* reset all partitionno/subpartitionno to an ordered format */
void RelationResetPartitionno(Oid relOid, LOCKMODE relationlock)
{
    Relation rel = heap_open(relOid, relationlock);
    if (!RELATION_IS_PARTITIONED(rel)) {
        heap_close(rel, relationlock);
        return;
    }

    bool issubpartition = RelationIsSubPartitioned(rel);
    List *partidList = relationGetPartitionOidList(rel);
    ListCell *cell = NULL;
    int partitionno = 0;
    foreach (cell, partidList) {
        Oid partid = lfirst_oid(cell);
        partitionno++;
        UpdateCurrentPartitionNo(partid, partitionno, false);

        if (issubpartition) {
            Partition part = partitionOpen(rel, partid, relationlock);
            Relation partrel = partitionGetRelation(rel, part);
            List *subpartidList = relationGetPartitionOidList(partrel);
            ListCell *subcell = NULL;
            int subpartitionno = 0;
            foreach (subcell, subpartidList) {
                Oid subpartid = lfirst_oid(subcell);
                subpartitionno++;
                UpdateCurrentSubPartitionNo(subpartid, subpartitionno);
            }
            releasePartitionOidList(&subpartidList);
            releaseDummyRelation(&partrel);
            partitionClose(rel, part, NoLock);

            UpdateCurrentSubPartitionNo(partid, -subpartitionno);
        }
    }
    releasePartitionOidList(&partidList);

    UpdateCurrentPartitionNo(RelOidGetPartitionTupleid(relOid), -partitionno, false);

    CacheInvalidateRelcache(rel);
    CommandCounterIncrement();
    heap_close(rel, NoLock);
}

/* get partitionno from partseq */
int GetPartitionnoFromSequence(PartitionMap *partmap, int partseq)
{
    if (partseq < 0) {
        return INVALID_PARTITION_NO;
    }

    incre_partmap_refcount(partmap);
    int partitionno = INVALID_PARTITION_NO;
    if (partmap->type == PART_TYPE_RANGE || partmap->type == PART_TYPE_INTERVAL) {
        RangePartitionMap *partitionmap = (RangePartitionMap *)partmap;
        if (partseq < partitionmap->rangeElementsNum) {
            partitionno = partitionmap->rangeElements[partseq].partitionno;
        }
    } else if (partmap->type == PART_TYPE_LIST) {
        ListPartitionMap *partitionmap = (ListPartitionMap *)partmap;
        if (partseq < partitionmap->listElementsNum) {
            partitionno = partitionmap->listElements[partseq].partitionno;
        }
    } else if (partmap->type == PART_TYPE_HASH) {
        HashPartitionMap *partitionmap = (HashPartitionMap *)partmap;
        if (partseq < partitionmap->hashElementsNum) {
            partitionno = partitionmap->hashElements[partseq].partitionno;
        }
    }
    decre_partmap_refcount(partmap);

    if (t_thrd.proc->workingVersionNum >= PARTITION_ENHANCE_VERSION_NUM) {
        Assert(partitionno > 0);
    }
    return partitionno;
}

bool PartExprKeyIsNull(Relation rel, char** partExprKeyStr)
{
    if (!rel) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("The relation can't be null here.")));
    }
    HeapTuple partTuple = NULL;
    if (OidIsValid(rel->parentId)) {
        /* partition re_id is oid of pg_partition */
        
        partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(rel->rd_id));
    } else {
        /* oid of pg_class */
        partTuple = SearchSysCache3(PARTPARTOID, NameGetDatum(RelationGetRelationName(rel)),
                                    CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE), ObjectIdGetDatum(rel->rd_id));
    }
    if (!HeapTupleIsValid(partTuple)) {
        ereport(ERROR, (errcode(ERRCODE_PARTITION_ERROR), errmsg("The partition can't be found")));
    }

    bool isnull = false;
    Datum val = SysCacheGetAttr(PARTPARTOID, partTuple, Anum_pg_partition_partkeyexpr, &isnull);
    if (!isnull && partExprKeyStr) {
        *partExprKeyStr = MemoryContextStrdup(LocalMyDBCacheMemCxt(), TextDatumGetCString(val));
    }
    ReleaseSysCache(partTuple);
    return isnull;
}
