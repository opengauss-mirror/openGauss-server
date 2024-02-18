/* -------------------------------------------------------------------------
 *
 * execReplication.cpp
 * 	  miscellaneous executor routines for logical replication
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/runtime/executor/execReplication.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_publication.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

static bool RelationFindReplTupleByIndex(EState *estate, Relation rel, Relation idxrel, LockTupleMode lockmode,
    TupleTableSlot *searchslot, TupleTableSlot *outslot, FakeRelationPartition *fakeRelPart);
static bool RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode, TupleTableSlot *searchslot,
    TupleTableSlot *outslot, FakeRelationPartition *fakeRelPart);

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 *
 * This is not generic routine, it expects the idxrel to be replication
 * identity of a rel and meet all limitations associated with that.
 */
static bool build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleTableSlot *searchslot,
    EState *estate)
{
    int attoff;
    bool isnull;
    Datum indclassDatum;
    oidvector *opclass;
    int2vector *indkey = &idxrel->rd_index->indkey;
    bool hasnulls = false;

    indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple, Anum_pg_index_indclass, &isnull);
    Assert(!isnull);
    opclass = (oidvector *)DatumGetPointer(indclassDatum);

    List* expressionsState = NIL;
    ExprContext* econtext = GetPerTupleExprContext(estate);
    econtext->ecxt_scantuple = searchslot;
    ListCell* indexpr_item = NULL;

    if (idxrel->rd_indexprs != NIL) {
        expressionsState = ExecPrepareExprList(idxrel->rd_indexprs, estate);
        indexpr_item = list_head(expressionsState);
    }

    /* Build scankey for every attribute in the index. */
    for (attoff = 0; attoff < IndexRelationGetNumberOfKeyAttributes(idxrel); attoff++) {
        Oid op;
        Oid opfamily;
        RegProcedure regop;
        int pkattno = attoff + 1;
        int mainattno = indkey->values[attoff];
        Oid optype = get_opclass_input_type(opclass->values[attoff]);
        if (mainattno > searchslot->tts_tupleDescriptor->natts) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmsg("index key attribute number %d exceeds number of columns %d",
                mainattno, searchslot->tts_tupleDescriptor->natts)));
        }

        /*
         * Load the operator info.  We need this to get the equality operator
         * function for the scan key.
         */
        opfamily = get_opclass_family(opclass->values[attoff]);

        op = get_opfamily_member(opfamily, optype, optype, BTEqualStrategyNumber);
        if (!OidIsValid(op))
            elog(ERROR, "missing operator %d(%u,%u) in opfamily %u", BTEqualStrategyNumber, optype, optype, opfamily);

        regop = get_opcode(op);

        if (mainattno != 0) {
            /* Initialize the scankey. */
            ScanKeyInit(&skey[attoff], pkattno, BTEqualStrategyNumber, regop, searchslot->tts_values[mainattno - 1]);
            skey[attoff].sk_collation = idxrel->rd_indcollation[attoff];

            /* Check for null value. */
            if (searchslot->tts_isnull[mainattno - 1]) {
                hasnulls = true;
                skey[attoff].sk_flags |= SK_ISNULL;
            }
        } else {
            if (idxrel->rd_indexprs == NIL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("wrong number of index expressions")));
            } else {
                bool isNull = false;
                Datum datum = ExecEvalExprSwitchContext((ExprState*)lfirst(indexpr_item), econtext, &isNull, NULL);
                indexpr_item = lnext(indexpr_item);

                ScanKeyInit(&skey[attoff], pkattno, BTEqualStrategyNumber, regop, datum);
                skey[attoff].sk_collation = idxrel->rd_indcollation[attoff];

                /* Check for null value. */
                if (isNull) {
                    hasnulls = true;
                    skey[attoff].sk_flags |= SK_ISNULL;
                }
            }
        }
    }

    return hasnulls;
}

/* Check tableam_tuple_lock result, and return if need to retry */
static bool inline CheckTupleLockRes(TM_Result res)
{
    switch (res) {
        case TM_Ok:
            break;
        case TM_Updated:
        case TM_Deleted:
            /* XXX: Improve handling here */
            ereport(LOG, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("concurrent update or delete, retrying")));
            return true;
        case TM_Invisible:
            elog(ERROR, "attempted to lock invisible tuple");
            break;
        default:
            elog(ERROR, "unexpected heap_lock_tuple status: %u", res);
            break;
    }
    return false;
}

/* Check heap modify result */
static void inline CheckTupleModifyRes(TM_Result res)
{
    switch (res) {
        case TM_SelfModified:
            /* Tuple was already updated in current command? */
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple already updated by self")));
            break;
        case TM_Ok:
            break;
        case TM_Updated:
        case TM_Deleted:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple concurrently updated")));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("unrecognized tuple status: %u", res)));
            break;
    }
}

static inline List* GetPartitionList(Relation rel, LOCKMODE lockmode)
{
    if (RelationIsSubPartitioned(rel)) {
        return RelationGetSubPartitionList(rel, lockmode);
    } else {
        return relationGetPartitionList(rel, lockmode);
    }
}

static bool PartitionFindReplTupleByIndex(EState *estate, Relation rel, Relation idxrel, LockTupleMode lockmode,
    TupleTableSlot *searchslot, TupleTableSlot *outslot, FakeRelationPartition *fakeRelInfo)
{
    /* must be non-GPI index */
    Assert(!RelationIsGlobalIndex(idxrel));

    fakeRelInfo->partList = GetPartitionList(rel, RowExclusiveLock);
    /* search the tuple in partition list one by one */
    ListCell *cell = NULL;
    foreach (cell, fakeRelInfo->partList) {
        Partition heapPart = (Partition)lfirst(cell);
        Relation partionRel = RelationIsSubPartitioned(rel) ? SubPartitionGetRelation(rel, heapPart, NoLock) :
                                                              partitionGetRelation(rel, heapPart);
        /* Get index partition of this heap partition */
        Oid idxPartOid = getPartitionIndexOid(RelationGetRelid(idxrel), heapPart->pd_id);
        Partition idxPart = partitionOpen(idxrel, idxPartOid, RowExclusiveLock);
        Relation idxPartRel = RelationIsSubPartitioned(idxrel) ? SubPartitionGetRelation(idxrel, idxPart, NoLock) :
                                                              partitionGetRelation(idxrel, idxPart);

        fakeRelInfo->partRel = partionRel;
        fakeRelInfo->part = heapPart;
        fakeRelInfo->partOid = heapPart->pd_id;

        if (RelationFindReplTupleByIndex(estate, rel, idxPartRel, lockmode, searchslot, outslot, fakeRelInfo)) {
            /* Hit, release index resource, heap partition need to be used later, so don't release it */
            partitionClose(idxrel, idxPart, NoLock);
            releaseDummyRelation(&idxPartRel);
            /* caller shoud release partRel */
            fakeRelInfo->needRleaseDummyRel = true;
            return true;
        }

        /* didn't find tuple in current partition, release dummy relation and switch to next partition */
        releaseDummyRelation(&fakeRelInfo->partRel);
        partitionClose(idxrel, idxPart, NoLock);
        releaseDummyRelation(&idxPartRel);
    }

    /* do not find tuple in any patition, close and return */
    releasePartitionList(rel, &fakeRelInfo->partList, NoLock);
    return false;
}

static bool PartitionFindReplTupleSeq(Relation rel, LockTupleMode lockmode,
    TupleTableSlot *searchslot, TupleTableSlot *outslot, FakeRelationPartition *fakeRelInfo)
{
    fakeRelInfo->partList = GetPartitionList(rel, RowExclusiveLock);
    ListCell *cell = NULL;
    foreach (cell, fakeRelInfo->partList) {
        Partition heapPart = (Partition)lfirst(cell);
        Relation partionRel = RelationIsSubPartitioned(rel) ? SubPartitionGetRelation(rel, heapPart, NoLock) :
                                                              partitionGetRelation(rel, heapPart);

        fakeRelInfo->partRel = partionRel;
        fakeRelInfo->part = heapPart;
        fakeRelInfo->partOid = heapPart->pd_id;

        if (RelationFindReplTupleSeq(rel, lockmode, searchslot, outslot, fakeRelInfo)) {
            /* caller shoud release partRel */
            fakeRelInfo->needRleaseDummyRel = true;
            return true;
        }
        releaseDummyRelation(&fakeRelInfo->partRel);
    }

    /* do not find tuple in any patition, close and return */
    releasePartitionList(rel, &fakeRelInfo->partList, NoLock);
    return false;
}

/*
 * Search the relation 'rel' for tuple using the index or seq scan.
 *
 * If a matching tuple is found, lock it with lockmode, fill the slot with its
 * contents, and return true.  Return false otherwise.
 *
 * Caller should check and release fakeRelInfo->partList and fakeRelInfo->partRel
 */
bool RelationFindReplTuple(EState *estate, Relation rel, Oid idxoid, LockTupleMode lockmode,
    TupleTableSlot *searchslot, TupleTableSlot *outslot, FakeRelationPartition *fakeRelInfo)
{
    int rc;
    bool found = false;
    Relation idxrel = NULL;

    /* clear fake rel info */
    rc = memset_s(fakeRelInfo, sizeof(FakeRelationPartition), 0, sizeof(FakeRelationPartition));
    securec_check(rc, "", "");

    if (OidIsValid(idxoid)) {
        idxrel = index_open(idxoid, RowExclusiveLock);
    }

    /* for non partitioned table, or partitioned table with GPI, use parent heap and index to do the scan */
    if (RelationIsNonpartitioned(rel) || (idxrel != NULL && RelationIsGlobalIndex(idxrel))) {
        if (idxrel != NULL) {
            found = RelationFindReplTupleByIndex(estate, rel, idxrel, lockmode, searchslot, outslot, fakeRelInfo);
            index_close(idxrel, NoLock);
            return found;
        } else {
            return RelationFindReplTupleSeq(rel, lockmode, searchslot, outslot, fakeRelInfo);
        }
    }

    /* scan with partition */
    if (idxrel != NULL) {
        found = PartitionFindReplTupleByIndex(estate, rel, idxrel, lockmode, searchslot, outslot, fakeRelInfo);
        index_close(idxrel, NoLock);
        return found;
    } else {
        return PartitionFindReplTupleSeq(rel, lockmode, searchslot, outslot, fakeRelInfo);
    }
}

/*
 * Search the relation 'rel' for tuple using the index.
 *
 * If a matching tuple is found, lock it with lockmode, fill the slot with its
 * contents, and return true.  Return false otherwise.
 */
static bool RelationFindReplTupleByIndex(EState *estate, Relation rel, Relation idxrel, LockTupleMode lockmode,
    TupleTableSlot *searchslot, TupleTableSlot *outslot, FakeRelationPartition *fakeRelPart)
{
    HeapTuple scantuple;
    ScanKeyData skey[INDEX_MAX_KEYS];
    IndexScanDesc scan;
    SnapshotData snap;
    TransactionId xwait;
    Relation targetRel = NULL;
    bool found;
    int rc;
    bool isGpi = RelationIsGlobalIndex(idxrel);
    /*
     * For GPI and non-partition table, use parent heap relation to search the tuple,
     * otherwise use partition relation
     */
    if (isGpi || RelationIsNonpartitioned(rel)) {
        targetRel = rel;
    } else {
        targetRel = fakeRelPart->partRel;
    }
    Assert(targetRel != NULL);
    /* Start an index scan. */
    InitDirtySnapshot(snap);
    scan = scan_handler_idx_beginscan(targetRel, idxrel, &snap,
        IndexRelationGetNumberOfKeyAttributes(idxrel), 0);
    /* refer to check_violation, we need to set isUpsert if we want to use dirty snapshot in UStore */
    scan->isUpsert = true;

    /* Build scan key. */
    build_replindex_scan_key(skey, targetRel, idxrel, searchslot, estate);

    while (true) {
        found = false;
        scan_handler_idx_rescan(scan, skey, IndexRelationGetNumberOfKeyAttributes(idxrel), NULL, 0);

        /* Try to find the tuple */
        if (RelationIsUstoreFormat(targetRel)) {
            found = IndexGetnextSlot(scan, ForwardScanDirection, outslot);
        } else {
            if ((scantuple = scan_handler_idx_getnext(scan, ForwardScanDirection)) != NULL) {
                found = true;
                ExecStoreTuple(scantuple, outslot, InvalidBuffer, false);
            }
        }
        if (found) {
            /* Found tuple, try to lock it in the lockmode. */
            xwait = TransactionIdIsValid(snap.xmin) ? snap.xmin : snap.xmax;
            /*
             * If the tuple is locked, wait for locking transaction to finish
             * and retry.
             */
            if (TransactionIdIsValid(xwait)) {
                XactLockTableWait(xwait);
                continue;
            }

            Buffer buf;
            TM_FailureData hufd;
            TM_Result res;
            Tuple locktup;
            HeapTupleData heaplocktup;
            UHeapTupleData UHeaplocktup;
            union {
                UHeapDiskTupleData hdr;
                char data[MaxPossibleUHeapTupleSize + sizeof(UHeapDiskTupleData)];
            } tbuf;
            ItemPointer tid = tableam_tops_get_t_self(targetRel, outslot->tts_tuple);

            if (RelationIsUstoreFormat(targetRel)) {
                /* materialize the slot, so we can visit it after the scan is end */
                outslot->tts_tuple = UHeapMaterialize(outslot);
                ItemPointerCopy(tid, &UHeaplocktup.ctid);
                rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
                securec_check(rc, "\0", "\0");
                UHeaplocktup.disk_tuple = &tbuf.hdr;
                locktup = &UHeaplocktup;
            } else {
                /* materialize the slot, so we can visit it after the scan is end */
                outslot->tts_tuple = ExecMaterializeSlot(outslot);
                ItemPointerCopy(tid, &heaplocktup.t_self);
                locktup = &heaplocktup;
            }

            /* Get the target tuple's partition for GPI */
            if (isGpi) {
                GetFakeRelAndPart(estate, rel, outslot, fakeRelPart);
                targetRel = fakeRelPart->partRel;
            }

            PushActiveSnapshot(GetLatestSnapshot());
            res = tableam_tuple_lock(targetRel,
                locktup, &buf, GetCurrentCommandId(false), lockmode, LockWaitBlock, &hufd,
                false, false,             /* don't follow updates */
                false,                    /* eval */
                GetLatestSnapshot(), tid, /* ItemPointer */
                false);                   /* is select for update */
            /* the tuple slot already has the buffer pinned */
            ReleaseBuffer(buf);
            PopActiveSnapshot();

            if (CheckTupleLockRes(res)) {
                /* lock tuple failed, try again */
                continue;
            }
        }
        /* we are done */
        break;
    }

    scan_handler_idx_endscan(scan);
    return found;
}

/*
 * Compare the tuple and slot and check if they have equal values.
 */
static bool tuple_equals_slot(TupleDesc desc, const Tuple tup, TupleTableSlot *slot, TypeCacheEntry **eq)
{
    Datum values[MaxTupleAttributeNumber];
    bool isnull[MaxTupleAttributeNumber];
    int attrnum;
    Form_pg_attribute att;

    tableam_tops_deform_tuple(tup, desc, values, isnull);

    /* Check equality of the attributes. */
    for (attrnum = 0; attrnum < desc->natts; attrnum++) {
        TypeCacheEntry *typentry;
        /* skip generate column */
        if (GetGeneratedCol(desc, attrnum)) {
            continue;
        }
        /*
         * If one value is NULL and other is not, then they are certainly not
         * equal
         */
        if (isnull[attrnum] != slot->tts_isnull[attrnum])
            return false;

        /*
         * If both are NULL, they can be considered equal.
         */
        if (isnull[attrnum])
            continue;

        att = &desc->attrs[attrnum];
        typentry = eq[attrnum];
        if (typentry == NULL) {
            typentry = lookup_type_cache(att->atttypid, TYPECACHE_EQ_OPR_FINFO);
            if (!OidIsValid(typentry->eq_opr_finfo.fn_oid)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("could not identify an equality operator for type %s", format_type_be(att->atttypid))));
            }
            eq[attrnum] = typentry;
        }

        if (!DatumGetBool(FunctionCall2Coll(&typentry->eq_opr_finfo, att->attcollation, values[attrnum],
            slot->tts_values[attrnum]))) {
            return false;
        }
    }

    return true;
}

/*
 * Search the relation 'rel' for tuple using the sequential scan.
 *
 * If a matching tuple is found, lock it with lockmode, fill the slot with its
 * contents, and return true.  Return false otherwise.
 *
 * Note that this stops on the first matching tuple.
 *
 * This can obviously be quite slow on tables that have more than few rows.
 */
static bool RelationFindReplTupleSeq(Relation rel, LockTupleMode lockmode, TupleTableSlot *searchslot,
    TupleTableSlot *outslot, FakeRelationPartition *fakeRelPart)
{
    Tuple scantuple;
    TableScanDesc scan;
    SnapshotData snap;
    TypeCacheEntry **eq;
    TransactionId xwait;
    bool found;
    int rc;
    Relation targetRel = fakeRelPart->partRel == NULL ? rel : fakeRelPart->partRel;
    TupleDesc desc = RelationGetDescr(rel);
    bool retry = false;

    Assert(equalTupleDescs(desc, outslot->tts_tupleDescriptor));
    eq = (TypeCacheEntry **)palloc0(sizeof(*eq) * outslot->tts_tupleDescriptor->natts);

    /* Start a heap scan. */
    InitDirtySnapshot(snap);
    scan = scan_handler_tbl_beginscan(targetRel, &snap, 0, NULL, NULL);

    while (true) {
        retry = false;
        found = false;
        scan_handler_tbl_rescan(scan, NULL, targetRel);

        /* Try to find the tuple */
        while ((scantuple = scan_handler_tbl_getnext(scan, ForwardScanDirection, targetRel)) != NULL) {
            if (!tuple_equals_slot(desc, scantuple, searchslot, eq)) {
                continue;
            }

            found = true;
            ExecStoreTuple(scantuple, outslot, InvalidBuffer, false);

            xwait = TransactionIdIsValid(snap.xmin) ? snap.xmin : snap.xmax;
            /*
             * If the tuple is locked, wait for locking transaction to finish
             * and retry.
             */
            if (TransactionIdIsValid(xwait)) {
                /* retry */
                retry = true;
                XactLockTableWait(xwait);
            }
            break;
        }

        if (retry) {
            continue;
        }
        if (found) {
            /* Found tuple, try to lock it in the lockmode. */
            Buffer buf;
            TM_FailureData hufd;
            TM_Result res;
            Tuple locktup = NULL;
            HeapTupleData heaplocktup;
            UHeapTupleData UHeaplocktup;
            union {
                UHeapDiskTupleData hdr;
                char data[MaxPossibleUHeapTupleSize + sizeof(UHeapDiskTupleData)];
            } tbuf;
            ItemPointer tid = tableam_tops_get_t_self(rel, outslot->tts_tuple);

            if (RelationIsUstoreFormat(targetRel)) {
                /* materialize the slot, so we can visit it after the scan is end */
                outslot->tts_tuple = UHeapMaterialize(outslot);
                ItemPointerCopy(tid, &UHeaplocktup.ctid);
                rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
                securec_check(rc, "\0", "\0");
                UHeaplocktup.disk_tuple = &tbuf.hdr;
                locktup = &UHeaplocktup;
            } else {
                /* materialize the slot, so we can visit it after the scan is end */
                outslot->tts_tuple = ExecMaterializeSlot(outslot);
                ItemPointerCopy(tid, &heaplocktup.t_self);
                locktup = &heaplocktup;
            }

            PushActiveSnapshot(GetLatestSnapshot());
            res = tableam_tuple_lock(targetRel, locktup, &buf, GetCurrentCommandId(false),
                lockmode, LockWaitBlock, &hufd, false,
                false,                    /* don't follow updates */
                false,                    /* eval */
                GetLatestSnapshot(), tid, /* ItemPointer */
                false);                   /* is select for update */

            /* the tuple slot already has the buffer pinned */
            ReleaseBuffer(buf);
            PopActiveSnapshot();

            if (CheckTupleLockRes(res)) {
                /* lock tuple failed, try again */
                continue;
            }
        }
        /* we are done */
        break;
    }

    scan_handler_tbl_endscan(scan);

    return found;
}

/*
 * Insert tuple represented in the slot to the relation, update the indexes,
 * and execute any constraints and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void ExecSimpleRelationInsert(EState *estate, TupleTableSlot *slot, FakeRelationPartition *relAndPart)
{
    Tuple tuple;
    ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
    Relation rel = resultRelInfo->ri_RelationDesc;
    Relation targetRel = relAndPart->partRel == NULL ? rel : relAndPart->partRel;

    /* For now we support only tables. */
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    CheckCmdReplicaIdentity(rel, CMD_INSERT);

    /* BEFORE ROW INSERT Triggers */
    if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row) {
        slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);
        if (slot == NULL) {
            /* "do nothing" */
            return;
        }
    }

    List *recheckIndexes = NIL;
    /* Materialize slot into a tuple that we can scribble upon. */
    tuple = tableam_tslot_get_tuple_from_slot(rel, slot);
    tableam_tops_update_tuple_with_oid(targetRel, tuple, slot);

    /* Compute stored generated columns */
    if (rel->rd_att->constr && rel->rd_att->constr->has_generated_stored) {
        ExecComputeStoredGenerated(resultRelInfo, estate, slot, tuple, CMD_INSERT);
        tuple = slot->tts_tuple;
    }

    /* Check the constraints of the tuple */
    if (rel->rd_att->constr) {
        ExecConstraints(resultRelInfo, slot, estate, true);
        tuple = ExecAutoIncrement(rel, estate, slot, tuple);
    }

    /* OK, store the tuple and create index entries for it */
    (void)tableam_tuple_insert(targetRel, tuple, GetCurrentCommandId(true), 0, NULL);
    if (resultRelInfo->ri_NumIndices > 0) {
        ItemPointer pTSelf = tableam_tops_get_t_self(rel, tuple);
        recheckIndexes =
            ExecInsertIndexTuples(slot, pTSelf, estate, targetRel, relAndPart->part, InvalidBktId, NULL, NULL);
    }
    /* AFTER ROW INSERT Triggers */
    ExecARInsertTriggers(estate, resultRelInfo, relAndPart->partOid, InvalidBktId, (HeapTuple)tuple, recheckIndexes);

    list_free_ext(recheckIndexes);

    /* try to insert tuple into mlog-table. */
    if (targetRel != NULL && targetRel->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        if (targetRel->rd_tam_ops == TableAmUstore) {
            tuple = (Tuple)UHeapToHeap(targetRel->rd_att, (UHeapTuple)tuple);
        }
        insert_into_mlog_table(targetRel, targetRel->rd_mlogoid, (HeapTuple)tuple,
                            &(((HeapTuple)tuple)->t_self), GetCurrentTransactionId(), 'I');
    }
}

/*
 * Find the searchslot tuple and update it with data in the slot,
 * update the indexes, and execute any constraints and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void ExecSimpleRelationUpdate(EState *estate, EPQState *epqstate, TupleTableSlot *searchslot, TupleTableSlot *slot,
    FakeRelationPartition *relAndPart)
{
    bool allowInplaceUpdate = true;
    Tuple tuple = NULL;
    ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
    Relation rel = resultRelInfo->ri_RelationDesc;
    ItemPointer searchSlotTid = tableam_tops_get_t_self(rel, searchslot->tts_tuple);

    /* For now we support only tables. */
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    CheckCmdReplicaIdentity(rel, CMD_UPDATE);

    if ((resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_update_after_row) ||
        resultRelInfo->ri_RelationDesc->rd_mlogoid) {
        allowInplaceUpdate = false;
    }

    /* BEFORE ROW UPDATE Triggers */
    if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_update_before_row) {
        slot = ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, relAndPart->partOid, InvalidBktId, NULL,
            searchSlotTid, slot);
        if (slot == NULL) {
            /* "do nothing" */
            return;
        }
    }

    /* Materialize slot into a tuple that we can scribble upon. */
    tuple = tableam_tslot_get_tuple_from_slot(rel, slot);
    List *recheckIndexes = NIL;
    Bitmapset *modifiedIdxAttrs = NULL;
    TupleTableSlot *oldslot = NULL;
    bool updateIndexes = false;
    bool rowMovement = false;
    TM_FailureData tmfd;
    FakeRelationPartition newTupleInfo;
    TM_Result res;
    Relation targetRelation = relAndPart->partRel == NULL ? rel : relAndPart->partRel;
    Relation parentRelation = relAndPart->partRel == NULL ? NULL : rel;

    /* Compute stored generated columns */
    if (rel->rd_att->constr && rel->rd_att->constr->has_generated_stored) {
        ExecComputeStoredGenerated(resultRelInfo, estate, slot, tuple, CMD_UPDATE);
    }

    /* Check the constraints of the tuple */
    if (rel->rd_att->constr) {
        ExecConstraints(resultRelInfo, slot, estate);
    }

    /* check whether there is a row movement for partition table */
    GetFakeRelAndPart(estate, rel, slot, &newTupleInfo);
    if (newTupleInfo.partOid != InvalidOid && newTupleInfo.partOid != relAndPart->partOid) {
        if (!rel->rd_rel->relrowmovement) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                errmsg("fail to update partitioned table \"%s\"", RelationGetRelationName(rel)),
                errdetail("disable row movement"))));
        }
        rowMovement = true;
    }

    tuple = slot->tts_tuple;
    CommandId cid = GetCurrentCommandId(true);
    /* OK, update the tuple and index entries for it */
    if (!rowMovement) {
        res = tableam_tuple_update(targetRelation, parentRelation, searchSlotTid, tuple, cid,
            InvalidSnapshot, estate->es_snapshot, true, &oldslot, &tmfd, &updateIndexes, &modifiedIdxAttrs,
            false, allowInplaceUpdate);
        CheckTupleModifyRes(res);
        if (updateIndexes && resultRelInfo->ri_NumIndices > 0) {
            ExecIndexTuplesState exec_index_tuples_state;
            exec_index_tuples_state.estate = estate;
            exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? targetRelation : NULL;
            exec_index_tuples_state.p = relAndPart->part;
            exec_index_tuples_state.conflict = NULL;
            exec_index_tuples_state.rollbackIndex = false;
            recheckIndexes = tableam_tops_exec_update_index_tuples(slot, oldslot, targetRelation, NULL, tuple,
                searchSlotTid, exec_index_tuples_state, InvalidBktId, modifiedIdxAttrs);
        }
    } else {
        /* rowMovement, delete origin tuple and insert new */
        Assert(relAndPart->partRel != NULL);
        Assert(newTupleInfo.partRel != NULL);
        res = tableam_tuple_delete(relAndPart->partRel, searchSlotTid, cid, InvalidSnapshot,
            estate->es_snapshot, true, &oldslot, &tmfd);
        CheckTupleModifyRes(res);

        ExecIndexTuplesState exec_index_tuples_state;
        exec_index_tuples_state.estate = estate;
        exec_index_tuples_state.targetPartRel = relAndPart->partRel;
        exec_index_tuples_state.p = relAndPart->part;
        exec_index_tuples_state.conflict = NULL;
        exec_index_tuples_state.rollbackIndex = false;
        tableam_tops_exec_delete_index_tuples(oldslot, relAndPart->partRel, NULL, searchSlotTid,
            exec_index_tuples_state, modifiedIdxAttrs);

        /* Insert new tuple */
        (void)tableam_tuple_insert(newTupleInfo.partRel, tuple, cid, 0, NULL);
        if (resultRelInfo->ri_NumIndices > 0) {
            ItemPointer pTSelf = tableam_tops_get_t_self(rel, tuple);
            recheckIndexes = ExecInsertIndexTuples(slot, pTSelf, estate, newTupleInfo.partRel,
                newTupleInfo.part, InvalidBktId, NULL, NULL);
        }
    }

    if (oldslot) {
        ExecDropSingleTupleTableSlot(oldslot);
    }

    /* AFTER ROW UPDATE Triggers */
    ExecARUpdateTriggers(estate, resultRelInfo, relAndPart->partOid, InvalidBktId, relAndPart->partOid,
        searchSlotTid, (HeapTuple)tuple, NULL, recheckIndexes);

    list_free(recheckIndexes);

    /* update tuple from mlog of matview(delete + insert). */
    if (rel != NULL && rel->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        /* 1. delete one tuple. */
        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL, searchSlotTid, tmfd.xmin, 'D');
        /* 2. insert new tuple */
        if (rel->rd_tam_ops == TableAmUstore) {
            tuple = (Tuple)UHeapToHeap(rel->rd_att, (UHeapTuple)tuple);
        }
        insert_into_mlog_table(rel, rel->rd_mlogoid, (HeapTuple)tuple, &(((HeapTuple)tuple)->t_self),
            GetCurrentTransactionId(), 'I');
    }
}

/*
 * Find the searchslot tuple and delete it, and execute any constraints
 * and per-row triggers.
 *
 * Caller is responsible for opening the indexes.
 */
void ExecSimpleRelationDelete(EState *estate, EPQState *epqstate, TupleTableSlot *searchslot,
    FakeRelationPartition *relAndPart)
{
    bool skip_tuple = false;
    ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
    Relation rel = resultRelInfo->ri_RelationDesc;
    ItemPointer tid = tableam_tops_get_t_self(rel, searchslot->tts_tuple);

    /* For now we support only tables. */
    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    CheckCmdReplicaIdentity(rel, CMD_DELETE);

    /* BEFORE ROW INSERT Triggers */
    if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_delete_before_row) {
        skip_tuple =
            !ExecBRDeleteTriggers(estate, epqstate, resultRelInfo, relAndPart->partOid, InvalidBktId, NULL, tid);
        if (skip_tuple) {
            return;
        }
    }

    TupleTableSlot *oldslot = NULL;
    Relation targetRel = relAndPart->partRel == NULL ? rel : relAndPart->partRel;
    TM_FailureData tmfd;

    /* OK, delete the tuple */
    TM_Result res = tableam_tuple_delete(targetRel, tid, GetCurrentCommandId(true), InvalidSnapshot,
        estate->es_snapshot, true, &oldslot, &tmfd);
    CheckTupleModifyRes(res);

    Bitmapset *modifiedIdxAttrs = NULL;
    ExecIndexTuplesState exec_index_tuples_state;
    exec_index_tuples_state.estate = estate;
    exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? relAndPart->partRel : NULL;
    exec_index_tuples_state.p = relAndPart->part;
    exec_index_tuples_state.conflict = NULL;
    exec_index_tuples_state.rollbackIndex = false;
    tableam_tops_exec_delete_index_tuples(oldslot, targetRel, NULL, tid, exec_index_tuples_state, modifiedIdxAttrs);
    if (oldslot) {
        ExecDropSingleTupleTableSlot(oldslot);
    }

    /* AFTER ROW DELETE Triggers */
    ExecARDeleteTriggers(estate, resultRelInfo, relAndPart->partOid, InvalidBktId, NULL, tid);

    /* delete tuple from mlog of matview */
    if (rel != NULL && rel->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL, tid, tmfd.xmin, 'D');
    }
}

/*
 * Check if command can be executed with current replica identity.
 */
void CheckCmdReplicaIdentity(Relation rel, CmdType cmd)
{
    PublicationActions *pubactions;

    /* We only need to do checks for UPDATE and DELETE. */
    if (cmd != CMD_UPDATE && cmd != CMD_DELETE)
        return;

    /* If relation has replica identity we are always good. */
    Assert(rel->relreplident == RelationGetRelReplident(rel));
    if (rel->relreplident == REPLICA_IDENTITY_FULL || OidIsValid(RelationGetReplicaIndex(rel)))
        return;

    /*
     * This is either UPDATE OR DELETE and there is no replica identity.
     *
     * Check if the table publishes UPDATES or DELETES.
     */
    pubactions = GetRelationPublicationActions(rel);
    if (cmd == CMD_UPDATE && pubactions->pubupdate) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("cannot update table \"%s\" because it does not have a replica identity and publishes updates",
            RelationGetRelationName(rel)),
            errhint("To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.")));
    } else if (cmd == CMD_DELETE && pubactions->pubdelete) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("cannot delete from table \"%s\" because it does not have a replica identity and publishes deletes",
            RelationGetRelationName(rel)),
            errhint("To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.")));
    }
    pfree(pubactions);
}

void GetFakeRelAndPart(EState *estate, Relation rel, TupleTableSlot *slot, FakeRelationPartition *relAndPart)
{
    relAndPart->partRel = NULL;
    relAndPart->part = NULL;
    relAndPart->partOid = InvalidOid;

    if (RelationIsNonpartitioned(rel)) {
        return;
    }

    Relation partRelation = NULL;
    Partition partition = NULL;
    Oid partitionOid;
    int partitionno = INVALID_PARTITION_NO;
    Tuple tuple = tableam_tslot_get_tuple_from_slot(rel, slot);
    switch (rel->rd_rel->parttype) {
        case PARTTYPE_NON_PARTITIONED_RELATION:
        case PARTTYPE_VALUE_PARTITIONED_RELATION:
            break;
        case PARTTYPE_PARTITIONED_RELATION:
            partitionOid = heapTupleGetPartitionOid(rel, tuple, &partitionno);
            searchFakeReationForPartitionOid(estate->esfRelations, estate->es_query_cxt, rel, partitionOid, partitionno,
                partRelation, partition, RowExclusiveLock);
            relAndPart->partRel = partRelation;
            relAndPart->part = partition;
            relAndPart->partOid = partitionOid;
            break;
        case PARTTYPE_SUBPARTITIONED_RELATION: {
            Relation subPartRel = NULL;
            Partition subPart = NULL;
            Oid subPartOid;
            int subpartitionno = INVALID_PARTITION_NO;
            partitionOid = heapTupleGetPartitionOid(rel, tuple, &partitionno);
            searchFakeReationForPartitionOid(estate->esfRelations, estate->es_query_cxt, rel, partitionOid, partitionno,
                partRelation, partition, RowExclusiveLock);
            subPartOid = heapTupleGetPartitionOid(partRelation, tuple, &subpartitionno);
            searchFakeReationForPartitionOid(estate->esfRelations, estate->es_query_cxt, partRelation, subPartOid,
                subpartitionno, subPartRel, subPart, RowExclusiveLock);

            relAndPart->partRel = subPartRel;
            relAndPart->part = subPart;
            relAndPart->partOid = subPartOid;
            break;
        }
        default:
            ereport(ERROR, (errmodule(MOD_EXECUTOR),
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Unrecognized parttype as \"%c\" for relation \"%s\"",
                rel->rd_rel->parttype, RelationGetRelationName(rel)))));
            break;
    }
}
