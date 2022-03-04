/* -------------------------------------------------------------------------
 *
 * nodeLockRows.cpp
 *	  Routines to handle FOR UPDATE/FOR SHARE row locking
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeLockRows.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecLockRows		- fetch locked rows
 *		ExecInitLockRows	- initialize node and subnodes..
 *		ExecEndLockRows		- shutdown node and subnodes
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "access/ustore/knl_uheap.h"
#include "executor/executor.h"
#include "executor/node/nodeLockRows.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/tableam.h"

/* ----------------------------------------------------------------
 * ExecLockRows
 * return: a tuple or NULL
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecLockRows(LockRowsState* node)
{
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    PlanState* outer_plan = NULL;
    bool epq_started = false;
    ListCell* lc = NULL;
    int2 bucket_id = InvalidOid;
    Relation target_rel = NULL;
    Partition target_part = NULL;
    Relation bucket_rel = NULL;

    /*
     * get information from the node
     */
    estate = node->ps.state;
    outer_plan = outerPlanState(node);

    /*
     * EvalPlanQual is called when concurrent lockrows or update or delete
     * we should skip early free.
     */
    bool orig_early_free = outer_plan->state->es_skip_early_free;
    bool orig_early_deinit = outer_plan->state->es_skip_early_deinit_consumer;

    outer_plan->state->es_skip_early_free = true;
    outer_plan->state->es_skip_early_deinit_consumer = true;

    /*
     * Get next tuple from subplan, if any.
     */
lnext:
    /*
     * We must reset the targetPart and targetRel to NULL for correct used
     * searchFakeReationForPartitionOid in goto condition.
     */
    target_rel = NULL;
    target_part = NULL;

    /* Set flag let executor to acquire RowShareLock */
    u_sess->exec_cxt.isLockRows = true;
    slot = ExecProcNode(outer_plan);
    u_sess->exec_cxt.isLockRows = false;

    outer_plan->state->es_skip_early_free = orig_early_free;
    outer_plan->state->es_skip_early_deinit_consumer = orig_early_deinit;

    if (TupIsNull(slot))
        return NULL;

    /*
     * Attempt to lock the source tuple(s).  (Note we only have locking
     * rowmarks in lr_arowMarks.)
     */
    epq_started = false;
    foreach (lc, node->lr_arowMarks) {
        ExecAuxRowMark* aerm = (ExecAuxRowMark*)lfirst(lc);
        ExecRowMark* erm = aerm->rowmark;
        Datum datum;
        bool isNull = false;
        bool rowMovement = false;
        HeapTupleData tuple;
        struct {
            HeapTupleHeaderData hdr;
            char data[MaxHeapTupleSize];
        } tbuf;

        Buffer buffer;
        TM_FailureData tmfd;
        LockTupleMode lock_mode;
        TM_Result test;
        Tuple copy_tuple;

        target_part = NULL;

        /* if child rel, must check whether it produced this row */
        if (erm->rti != erm->prti) {
            Oid table_oid;

            datum = ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull);
            /* shouldn't ever get a null result... */
            if (isNull) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("tableoid is NULL when try to lock current row.")));
            }
            
            table_oid = DatumGetObjectId(datum);
            if (table_oid != RelationGetRelid(erm->relation)) {
                /* this child is inactive right now */
                ItemPointerSetInvalid(&(erm->curCtid));
                continue;
            }
        }

        if (RELATION_OWN_BUCKET(erm->relation)) {
            Datum bucket_datum;
            bucket_datum = ExecGetJunkAttribute(slot, aerm->tbidAttNo, &isNull);
            if (isNull)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("bucketid is NULL when try to lock current row.")));
            bucket_id = DatumGetObjectId(bucket_datum);
            Assert(bucket_id != InvalidBktId);
        }
        /* for partitioned table */
        if (PointerIsValid(erm->relation->partMap)) {
            Oid tblid = InvalidOid;
            Datum part_datum;

            part_datum = ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull);
            rowMovement = erm->relation->rd_rel->relrowmovement;
            tblid = DatumGetObjectId(part_datum);
            /* if it is a partition */
            if (tblid != erm->relation->rd_id) {
                searchFakeReationForPartitionOid(estate->esfRelations,
                    estate->es_query_cxt, erm->relation, tblid, target_rel,
                    target_part, RowShareLock);
                Assert(tblid == target_rel->rd_id);
            }
        } else {
            /* for non-partitioned table */
            target_rel = erm->relation;
        }

        if (target_rel == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("ExecLockRows:target relation cannot be NULL")));
        }

        /* fetch the tuple's ctid */
        datum = ExecGetJunkAttribute(slot, aerm->ctidAttNo, &isNull);
        /* shouldn't ever get a null result... */
        if (isNull)
            ereport(
                ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("ctid is NULL when try to lock current row.")));
        tuple.t_self = *((ItemPointer)DatumGetPointer(datum));
        tuple.t_data = &tbuf.hdr;

        bucket_rel = target_rel;
        if (RELATION_OWN_BUCKET(target_rel)) {
            searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, target_rel, bucket_id, bucket_rel);
        }
        /* okay, try to lock the tuple */
        switch (erm->markType) {
            case ROW_MARK_EXCLUSIVE:
                lock_mode = LockTupleExclusive;
                break;
            case ROW_MARK_NOKEYEXCLUSIVE:
                lock_mode = LockTupleNoKeyExclusive;
                break;
            case ROW_MARK_SHARE:
                lock_mode = LockTupleShared;
                break;
            case ROW_MARK_KEYSHARE:
                lock_mode = LockTupleKeyShare;
                break;
            default:
                elog(ERROR, "unsupported rowmark type");
                lock_mode = LockTupleNoKeyExclusive; /* keep compiler quiet */
                break;
        }

        /* Need to merge the ustore logic with AM logic */
        test = tableam_tuple_lock(bucket_rel, &tuple, &buffer, 
                                  estate->es_output_cid, lock_mode, erm->noWait, &tmfd,
#ifdef ENABLE_MULTIPLE_NODES
                                  false, false, false, estate->es_snapshot, NULL, true,
#else
                                  false, true, false, estate->es_snapshot, NULL, true,
#endif
                                  false, InvalidTransactionId,
                                  erm->waitSec);
        ReleaseBuffer(buffer);

        switch (test) {
            case TM_SelfCreated:
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("attempted to lock invisible tuple")));
                break;
            case TM_SelfUpdated:
            case TM_SelfModified:
                /* treat it as deleted; do not process */
                goto lnext;

            case TM_Ok:
                /* got the lock successfully */
                break;

             case TM_Updated:
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                /*
                 * EvalPlanQual need to reinitialize child plan to do some recheck due to concurrent update,
                 * but we wrap the left tree of Stream node in backend thread. So the child plan cannot be
                 * reinitialized successful now.
                 */
                if (IS_PGXC_DATANODE && u_sess->exec_cxt.under_stream_runtime &&
                    estate->es_plannedstmt->num_streams > 0) {
                    ereport(ERROR, (errcode(ERRCODE_STREAM_CONCURRENT_UPDATE),
                            errmsg("concurrent update under Stream mode is not yet supported")));
                }

                /* updated, so fetch and lock the updated version */
                copy_tuple = tableam_tuple_lock_updated(estate->es_output_cid, bucket_rel, lock_mode, &tmfd.ctid, tmfd.xmax, estate->es_snapshot, true);
                if (copy_tuple == NULL) {
                    /* Tuple was deleted, so don't return it */
                    if (rowMovement) {
                        /*
                        * the may be a row movement update action which delete tuple from original
                        * partition and insert tuple to new partition or we can add lock on the tuple to
                        * be delete or updated to avoid throw exception
                        */
                        ereport(ERROR, (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                            errmsg("partition table update conflict"),
                            errdetail("disable row movement of table can avoid this conflict")));
                    }
                    goto lnext;
                }
                /* remember the actually locked tuple's TID */
                tuple.t_self = ((HeapTuple)copy_tuple)->t_self;

                /*
                 * Need to run a recheck subquery.	Initialize EPQ state if we
                 * didn't do so already.
                 */
                if (!epq_started) {
                    EvalPlanQualBegin(&node->lr_epqstate, estate);
                    epq_started = true;
                }
                
                /* Store target tuple for relation's scan node */
                EvalPlanQualSetTuple(&node->lr_epqstate, erm->rti, copy_tuple);

                /* Continue loop until we have all target tuples */
                break;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot())
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));

                /* Tuple was deleted, so don't return it */
                if (rowMovement) {
                    /*
                     * the may be a row movement update action which delete tuple from original
                     * partition and insert tuple to new partition or we can add lock on the tuple to
                     * be delete or updated to avoid throw exception
                     */
                    ereport(ERROR, (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                        errmsg("partition table update conflict"),
                        errdetail("disable row movement of table can avoid this conflict")));
                }

                goto lnext;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized tuple_lock_tuple status: %d when lock a tuple", test)));
        }

        /* Remember locked tuple's TID for WHERE CURRENT OF */
        erm->curCtid = tuple.t_self;
    }

    /*
     * If we need to do EvalPlanQual testing, do so.
     */
    if (epq_started) {
        struct {
            HeapTupleHeaderData hdr;
            char data[MaxHeapTupleSize];
        } tbuf;

        /*
         * First, fetch a copy of any rows that were successfully locked
         * without any update having occurred.	(We do this in a separate pass
         * so as to avoid overhead in the common case where there are no
         * concurrent updates.)
         */
        foreach (lc, node->lr_arowMarks) {
            ExecAuxRowMark* aerm = (ExecAuxRowMark*)lfirst(lc);
            ExecRowMark* erm = aerm->rowmark;
            HeapTupleData tuple;
            Buffer buffer;

            target_part = NULL;

            /* ignore non-active child tables */
            if (!ItemPointerIsValid(&(erm->curCtid))) {
                Assert(erm->rti != erm->prti); /* check it's child table */
                continue;
            }

            if (EvalPlanQualGetTuple(&node->lr_epqstate, erm->rti) != NULL)
                continue; /* it was updated and fetched above */

            /* for partitioned table */
            if (PointerIsValid(erm->relation->partMap)) {
                Datum partdatum;
                Oid tblid = InvalidOid;
                bool partisNull = false;

                partdatum = ExecGetJunkAttribute(slot, aerm->toidAttNo, &partisNull);
                tblid = DatumGetObjectId(partdatum);
                /* if it is a partition */
                if (tblid != erm->relation->rd_id) {
                    searchFakeReationForPartitionOid(estate->esfRelations,
                        estate->es_query_cxt,
                        erm->relation,
                        tblid,
                        target_rel,
                        target_part,
                        RowShareLock);
                }
            } else {
                /* for non-partitioned table */
                target_rel = erm->relation;
            }

            if (target_rel == NULL) {
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("ExecLockRows:target relation cannot be NULL for plan qual recheck.")));
            }

            /* okay, fetch the tuple */
            tuple.t_self = erm->curCtid;
            /* Must set a private data buffer for heap_fetch */
            tuple.t_data = &tbuf.hdr;
            bucket_rel = target_rel;
            if (RELATION_OWN_BUCKET(target_rel)) {
                bucket_id = computeTupleBucketId(target_rel, &tuple);
                searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, target_rel, bucket_id, bucket_rel);
            }

            if (!tableam_tuple_fetch(bucket_rel, SnapshotAny, &tuple, &buffer, false, NULL))
                ereport(ERROR,
                    (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("failed to fetch tuple for EvalPlanQual recheck")));

            /* successful, copy and store tuple */
            EvalPlanQualSetTuple(&node->lr_epqstate, erm->rti, tableam_tops_copy_tuple(&tuple));
            ReleaseBuffer(buffer);
        }

        /*
         * Now fetch any non-locked source rows --- the EPQ logic knows how to
         * do that.
         */
        EvalPlanQualSetSlot(&node->lr_epqstate, slot);
        EvalPlanQualFetchRowMarks(&node->lr_epqstate);

        /*
         * And finally we can re-evaluate the tuple.
         */
        slot = EvalPlanQualNext(&node->lr_epqstate);
        if (TupIsNull(slot)) {
            /* Updated tuple fails qual, so ignore it and go on */
            goto lnext;
        }
    }

    /* Got all locks, so return the current tuple */
    return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitLockRows
 *
 *		This initializes the LockRows node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LockRowsState* ExecInitLockRows(LockRows* node, EState* estate, int eflags)
{
    LockRowsState* lrstate = makeNode(LockRowsState);
    Plan* outer_plan = outerPlan(node);
    List* epq_arowmarks = NIL;
    ListCell* lc = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    lrstate->ps.plan = (Plan*)node;
    lrstate->ps.state = estate;

    /*
     * Miscellaneous initialization
     *
     * LockRows nodes never call ExecQual or ExecProject.
     */
    /*
     * Tuple table initialization (XXX not actually used...)
     */
    ExecInitResultTupleSlot(estate, &lrstate->ps);

    /* Set flag let executor to acquire RowShareLock */
    u_sess->exec_cxt.isLockRows = true;
    /*
     * then initialize outer plan
     */
    outerPlanState(lrstate) = ExecInitNode(outer_plan, estate, eflags);
    u_sess->exec_cxt.isLockRows = false;

    /*
     * LockRows nodes do no projections, so initialize projection info for
     * this node appropriately
     */
    TupleDesc resultDesc = ExecGetResultType(outerPlanState(lrstate));
    ExecAssignResultTypeFromTL(&lrstate->ps, resultDesc->tdTableAmType);

    lrstate->ps.ps_ProjInfo = NULL;

    Assert(lrstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

    /*
     * Locate the ExecRowMark(s) that this node is responsible for, and
     * construct ExecAuxRowMarks for them.	(InitPlan should already have
     * built the global list of ExecRowMarks.)
     */
    lrstate->lr_arowMarks = NIL;
    epq_arowmarks = NIL;
    foreach (lc, node->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(lc);
        ExecRowMark* erm = NULL;
        ExecAuxRowMark* aerm = NULL;

        Assert(IsA(rc, PlanRowMark));

        /* ignore "parent" rowmarks; they are irrelevant at runtime */
        if (rc->isParent)
            continue;

        if (!(IS_PGXC_COORDINATOR || u_sess->pgxc_cxt.PGXCNodeId < 0 ||
                bms_is_member(u_sess->pgxc_cxt.PGXCNodeId, rc->bms_nodeids))) {
            continue;
        }

        /* find ExecRowMark and build ExecAuxRowMark */
        erm = ExecFindRowMark(estate, rc->rti);
        aerm = ExecBuildAuxRowMark(erm, outer_plan->targetlist);

        /*
         * Only locking rowmarks go into our own list.	Non-locking marks are
         * passed off to the EvalPlanQual machinery.  This is because we don't
         * want to bother fetching non-locked rows unless we actually have to
         * do an EPQ recheck.
         */
        if (RowMarkRequiresRowShareLock(erm->markType))
            lrstate->lr_arowMarks = lappend(lrstate->lr_arowMarks, aerm);
        else
            epq_arowmarks = lappend(epq_arowmarks, aerm);
    }

    /* Now we have the info needed to set up EPQ state */
    EvalPlanQualInit(&lrstate->lr_epqstate, estate, outer_plan, epq_arowmarks, node->epqParam);

    return lrstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLockRows
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndLockRows(LockRowsState* node)
{
    EvalPlanQualEnd(&node->lr_epqstate);
    ExecEndNode(outerPlanState(node));
}

void ExecReScanLockRows(LockRowsState* node)
{
    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}
