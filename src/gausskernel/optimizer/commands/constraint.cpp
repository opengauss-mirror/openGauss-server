/* -------------------------------------------------------------------------
 *
 * constraint.cpp
 *	  openGauss CONSTRAINT support code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/constraint.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_partition_fn.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

#define RELATION_GET_BUCKET_REL(fakeRel, bucketId) \
    ((!RELATION_CREATE_BUCKET(fakeRel)) ? (fakeRel) : bucketGetRelation((fakeRel), NULL, (bucketId)))

#define GET_EXECUTOR_STATE(indexInfo, estate, econtext, slot)                              \
    do {                                                                                   \
        if ((indexInfo)->ii_Expressions != NIL || (indexInfo)->ii_ExclusionOps != NULL) {  \
            (estate) = CreateExecutorState();                                              \
            (econtext) = GetPerTupleExprContext(estate);                                   \
            (econtext)->ecxt_scantuple = (slot);                                           \
        } else                                                                             \
            (estate) = NULL;                                                               \
    } while (0);

/*
 * unique_key_recheck - trigger function to do a deferred uniqueness check.
 *
 * This now also does deferred exclusion-constraint checks, so the name is
 * somewhat historical.
 *
 * This is invoked as an AFTER ROW trigger for both INSERT and UPDATE,
 * for any rows recorded as potentially violating a deferrable unique
 * or exclusion constraint.
 *
 * This may be an end-of-statement check, a commit-time check, or a
 * check triggered by a SET CONSTRAINTS command.
 */
Datum unique_key_recheck(PG_FUNCTION_ARGS)
{
    TriggerData* trigdata = (TriggerData*)fcinfo->context;
    const char* funcname = "unique_key_recheck";
    HeapTuple new_row;
    ItemPointerData tmptid;
    Relation indexRel;
    IndexInfo* indexInfo = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    TupleTableSlot* slot = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    Relation fakeRel = NULL;
    Relation fakeIdxRel = NULL;
    Partition part = NULL;
    Partition indexPart = NULL;
    Relation partRel = NULL;
    Relation idxPartRel = NULL;
    int2 bucketid;
    Oid tableOid;
    /*
     * Make sure this is being called as an AFTER ROW trigger.	Note:
     * translatable error strings are shared with ri_triggers.c, so resist the
     * temptation to fold the function name into them.
     */
    if (!CALLED_AS_TRIGGER(fcinfo))
        ereport(ERROR,
            (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                errmsg("function \"%s\" was not called by trigger manager", funcname)));

    if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        ereport(ERROR,
            (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                errmsg("function \"%s\" must be fired AFTER ROW", funcname)));

    /*
     * Get the new data that was inserted/updated.
     */
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        new_row = trigdata->tg_trigtuple;
    else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        new_row = trigdata->tg_newtuple;
    else {
        ereport(ERROR,
            (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                errmsg("function \"%s\" must be fired for INSERT or UPDATE", funcname)));
        new_row = NULL; /* keep compiler quiet */
    }

    /*
     * If the new_row is now dead (ie, inserted and then deleted within our
     * transaction), we can skip the check.  However, we have to be careful,
     * because this trigger gets queued only in response to index insertions;
     * which means it does not get queued for HOT updates.	The row we are
     * called for might now be dead, but have a live HOT child, in which case
     * we still need to make the check --- effectively, we're applying the
     * check against the live child row, although we can use the values from
     * this row since by definition all columns of interest to us are the
     * same.
     *
     *
     * This might look like just an optimization, because the index AM will
     * make this identical test before throwing an error.  But it's actually
     * needed for correctness, because the index AM will also throw an error
     * if it doesn't find the index entry for the row.  If the row's dead then
     * it's possible the index entry has also been marked dead, and even
     * removed.
     */
    tmptid   = new_row->t_self;
    tableOid = new_row->t_tableOid;
    bucketid = new_row->t_bucketId;

    fakeRel = trigdata->tg_relation;
    if (RelationIsPartitioned(trigdata->tg_relation)) {
        part = partitionOpen(trigdata->tg_relation, tableOid, AccessShareLock);
        partRel = partitionGetRelation(trigdata->tg_relation, part);
        fakeRel = partRel;
    }
    fakeRel = RELATION_GET_BUCKET_REL(fakeRel, bucketid);

    if (!heap_hot_search(&tmptid, fakeRel, SnapshotSelf, NULL)) {
        /*
         * All rows in the HOT chain are dead, so skip the check.
         */
        if (bucketid != InvalidBktId) {
            bucketCloseRelation(fakeRel);
        }
        if (part != NULL) {
            releaseDummyRelation(&partRel);
            partitionClose(trigdata->tg_relation, part, AccessShareLock);
        }
        return PointerGetDatum(NULL);
    }

    /*
     * Open the index, acquiring a RowExclusiveLock, just as if we were going
     * to update it.  (This protects against possible changes of the index
     * schema, not against concurrent updates.)
     */
    indexRel = index_open(trigdata->tg_trigger->tgconstrindid, RowExclusiveLock);
    fakeIdxRel = indexRel;
    if (RelationIsPartitioned(indexRel)) {
        Oid indexPartOid = getPartitionIndexOid(trigdata->tg_trigger->tgconstrindid, tableOid);
        indexPart = partitionOpen(indexRel, indexPartOid, RowExclusiveLock);
        idxPartRel = partitionGetRelation(indexRel, indexPart);
        fakeIdxRel = idxPartRel;
    }
    fakeIdxRel = RELATION_GET_BUCKET_REL(fakeIdxRel, bucketid);
    indexInfo = BuildIndexInfo(fakeIdxRel);

    /*
     * The heap tuple must be put into a slot for FormIndexDatum.
     */
    slot = MakeSingleTupleTableSlot(RelationGetDescr(fakeRel));

    (void)ExecStoreTuple(new_row, slot, InvalidBuffer, false);

    /*
     * Typically the index won't have expressions, but if it does we need an
     * EState to evaluate them.  We need it for exclusion constraints too,
     * even if they are just on simple columns.
     */
    GET_EXECUTOR_STATE(indexInfo, estate, econtext, slot);

    /*
     * Form the index values and isnull flags for the index entry that we need
     * to check.
     *
     * Note: if the index uses functions that are not as immutable as they are
     * supposed to be, this could produce an index tuple different from the
     * original.  The index AM can catch such errors by verifying that it
     * finds a matching index entry with the tuple's TID.  For exclusion
     * constraints we check this in check_exclusion_constraint().
     */
    FormIndexDatum(indexInfo, slot, estate, values, isnull);

    /*
     * Now do the appropriate check.
     */
    if (indexInfo->ii_ExclusionOps == NULL) {
        /*
         * Note: this is not a real insert; it is a check that the index entry
         * that has already been inserted is unique.  Passing t_self is
         * correct even if t_self is now dead, because that is the TID the
         * index will know about.
         */
        index_insert(fakeIdxRel, values, isnull, &(new_row->t_self), fakeRel, UNIQUE_CHECK_EXISTING);
    } else {
        /*
         * For exclusion constraints we just do the normal check, but now it's
         * okay to throw error.  In the HOT-update case, we must use the live
         * HOT child's TID here, else check_exclusion_constraint will think
         * the child is a conflict.
         */
        (void)check_exclusion_constraint(
            fakeRel, fakeIdxRel, indexInfo, &tmptid, values, isnull, estate, false, false);
    }

    /*
     * If that worked, then this index entry is unique or non-excluded, and we
     * are done.
     */
    if (estate != NULL)
        FreeExecutorState(estate);
    ExecDropSingleTupleTableSlot(slot);

    if (RelationIsBucket(fakeIdxRel)) {
        bucketCloseRelation(fakeIdxRel);
    }
    if (indexPart != NULL) {
        releaseDummyRelation(&idxPartRel);
        partitionClose(indexRel, indexPart, RowExclusiveLock);
    }
    index_close(indexRel, RowExclusiveLock);

    if (bucketid != InvalidBktId) {
        bucketCloseRelation(fakeRel);
    }
    if (part != NULL) {
        releaseDummyRelation(&partRel);
        partitionClose(trigdata->tg_relation, part, AccessShareLock);
    }
    return PointerGetDatum(NULL);
}
