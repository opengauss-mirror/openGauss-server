/* -------------------------------------------------------------------------
 * worker.cpp
 * 	   PostgreSQL logical replication worker (apply)
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 	  src/gausskernel/storage/replication/logical/worker.cpp
 *
 * NOTES
 * 	  This file contains the worker which applies logical changes as they come
 * 	  from remote logical replication stream.
 *
 * 	  The main worker (apply) is started by logical replication worker
 * 	  launcher for every enabled subscription in a database. It uses
 * 	  walsender protocol to communicate with publisher.
 *
 * 	  The apply worker may spawn additional workers (sync) for initial data
 * 	  synchronization of tables.
 *
 * 	  This module includes server facing code and shares libpqwalreceiver
 * 	  module with walreceiver for providing the libpq specific functionality.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_partition_fn.h"

#include "commands/trigger.h"
#include "commands/subscriptioncmds.h"

#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"

#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_relation.h"
#include "parser/parse_merge.h"

#include "postmaster/postmaster.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "replication/logicalworker.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "rewrite/rewriteHandler.h"

#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/relcache.h"

static const int NAPTIME_PER_CYCLE = 10; /* max sleep time between cycles (10ms) */
static const double HALF = 0.5;

typedef struct FlushPosition {
    dlist_node node;
    XLogRecPtr local_end;
    XLogRecPtr remote_end;
} FlushPosition;

typedef struct SlotErrCallbackArg {
    LogicalRepRelMapEntry *rel;
    int remote_attnum;
} SlotErrCallbackArg;

static void send_feedback(XLogRecPtr recvpos, bool force, bool requestReply);
static void store_flush_position(XLogRecPtr remote_lsn);
static void reread_subscription(void);
static void ApplyWorkerProcessMsg(char type, StringInfo s, XLogRecPtr *lastRcv);
static void apply_dispatch(StringInfo s);
static void apply_handle_conninfo(StringInfo s);
static void UpdateConninfo(char* standbysInfo);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void LogicalrepWorkerSighub(SIGNAL_ARGS)
{
    t_thrd.applyworker_cxt.got_SIGHUP = true;
}

/*
 * Make sure that we started local transaction.
 *
 * Also switches to ApplyMessageContext as necessary.
 */
static bool ensure_transaction(void)
{
    if (IsTransactionState()) {
        SetCurrentStatementStartTimestamp();
        if (CurrentMemoryContext != t_thrd.applyworker_cxt.messageContext)
            MemoryContextSwitchTo(t_thrd.applyworker_cxt.messageContext);
        return false;
    }

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();

    if (!t_thrd.applyworker_cxt.mySubscriptionValid)
        reread_subscription();

    MemoryContextSwitchTo(t_thrd.applyworker_cxt.messageContext);
    return true;
}


/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *create_estate_for_relation(LogicalRepRelMapEntry *rel)
{
    EState *estate;
    ResultRelInfo *resultRelInfo;
    RangeTblEntry *rte;

    estate = CreateExecutorState();

    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = RelationGetRelid(rel->localrel);
    rte->relkind = rel->localrel->rd_rel->relkind;
    estate->es_range_table = list_make1(rte);

    resultRelInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(resultRelInfo, rel->localrel, 1, 0);

    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;
    estate->es_output_cid = GetCurrentCommandId(true);

    /* Triggers might need a slot */
    if (resultRelInfo->ri_TrigDesc)
        estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    return estate;
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upstream.
 */
static void slot_fill_defaults(LogicalRepRelMapEntry *rel, EState *estate, TupleTableSlot *slot)
{
    TupleDesc desc = RelationGetDescr(rel->localrel);
    int num_phys_attrs = desc->natts;
    int i;
    int attnum, num_defaults = 0;
    int *defmap;
    ExprState **defexprs;
    ExprContext *econtext;

    econtext = GetPerTupleExprContext(estate);

    /* We got all the data via replication, no need to evaluate anything. */
    if (num_phys_attrs == rel->remoterel.natts)
        return;

    defmap = (int *)palloc(num_phys_attrs * sizeof(int));
    defexprs = (ExprState **)palloc(num_phys_attrs * sizeof(ExprState *));

    for (attnum = 0; attnum < num_phys_attrs; attnum++) {
        Expr *defexpr;

        if (desc->attrs[attnum]->attisdropped || GetGeneratedCol(desc, attnum))
            continue;

        if (rel->attrmap[attnum] >= 0)
            continue;

        defexpr = (Expr *)build_column_default(rel->localrel, attnum + 1);
        if (defexpr != NULL) {
            /* Run the expression through planner */
            defexpr = expression_planner(defexpr);

            /* Initialize executable expression in copycontext */
            defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
            defmap[num_defaults] = attnum;
            num_defaults++;
        }
    }

    for (i = 0; i < num_defaults; i++)
        slot->tts_values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext, &slot->tts_isnull[defmap[i]], NULL);
}

/*
 * Error callback to give more context info about data conversion failures
 * while reading data from the remote server.
 */
static void slot_store_error_callback(void *arg)
{
    SlotErrCallbackArg *errarg = (SlotErrCallbackArg *)arg;
    LogicalRepRelMapEntry *rel = NULL;

    /* Nothing to do if remote attribute number is not set */
    if (errarg->remote_attnum < 0) {
        return;
    }

    rel = errarg->rel;
    errcontext("processing remote data for replication target relation \"%s.%s\" column \"%s\"",
               rel->remoterel.nspname, rel->remoterel.relname,
               rel->remoterel.attnames[errarg->remote_attnum]);
}

/*
 * Store tuple data into slot.
 *
 * Incoming data can be either text or binary format.
 */
static void slot_store_data(TupleTableSlot *slot, LogicalRepRelMapEntry *rel, LogicalRepTupleData *tupleData)
{
    int natts = slot->tts_tupleDescriptor->natts;
    int i;
    SlotErrCallbackArg errarg;
    ErrorContextCallback errcallback;

    ExecClearTuple(slot);

    /* Push callback + info on the error context stack */
    errarg.rel = rel;
    errarg.remote_attnum = -1;
    errcallback.callback = slot_store_error_callback;
    errcallback.arg = (void *)&errarg;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* Call the "in" function for each non-dropped, non-null attribute */
    for (i = 0; i < natts; i++) {
        Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[i];
        int remoteattnum = rel->attrmap[i];

        if (!att->attisdropped && remoteattnum >= 0) {
            StringInfo colvalue = &tupleData->colvalues[remoteattnum];
            errarg.remote_attnum = remoteattnum;

            if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_TEXT) {
                Oid typinput;
                Oid typioparam;

                getTypeInputInfo(att->atttypid, &typinput, &typioparam);
                slot->tts_values[i] = OidInputFunctionCall(typinput, colvalue->data, typioparam, att->atttypmod);
                slot->tts_isnull[i] = false;
            } else if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_BINARY) {
                Oid typreceive;
                Oid typioparam;

                /*
                 * In some code paths we may be asked to re-parse the same
                 * tuple data.  Reset the StringInfo's cursor so that works.
                 */
                colvalue->cursor = 0;

                getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
                slot->tts_values[i] = OidReceiveFunctionCall(typreceive, colvalue, typioparam, att->atttypmod);

                /* Trouble if it didn't eat the whole buffer */
                if (colvalue->cursor != colvalue->len) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                        errmsg("incorrect binary data format in logical replication column %d", 
                        remoteattnum + 1)));
                }
                slot->tts_isnull[i] = false;
            } else {
                /*
                 * NULL value from remote.  (We don't expect to see
                 * LOGICALREP_COLUMN_UNCHANGED here, but if we do, treat it as
                 * NULL.)
                 */
                slot->tts_values[i] = (Datum) 0;
                slot->tts_isnull[i] = true;
            }
            /* Reset attnum for error callback */
            errarg.remote_attnum = -1;
        } else {
            /*
             * We assign NULL to dropped attributes, NULL values, and missing
             * values (missing values should be later filled using
             * slot_fill_defaults).
             */
            slot->tts_values[i] = (Datum)0;
            slot->tts_isnull[i] = true;
        }
    }

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;

    ExecStoreVirtualTuple(slot);
}

/*
 * Replace updated columns with data from the LogicalRepTupleData struct.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input functions on the user data.
 *
 * "slot" is filled with a copy of the tuple in "srcslot", replacing
 * columns provided in "tupleData" and leaving others as-is.
 *
 * Caution: unreplaced pass-by-ref columns in "slot" will point into the
 * storage for "srcslot".  This is OK for current usage, but someday we may
 * need to materialize "slot" at the end to make it independent of "srcslot".
 */
static void slot_modify_data(TupleTableSlot *slot, TupleTableSlot *srcslot, LogicalRepRelMapEntry *rel,
    LogicalRepTupleData *tupleData)
{
    int natts = slot->tts_tupleDescriptor->natts;
    int i;
    int rc;
    SlotErrCallbackArg errarg;
    ErrorContextCallback errcallback;

    /* We'll fill "slot" with a virtual tuple, so we must start with ... */
    ExecClearTuple(slot);

    /*
     * Copy all the column data from srcslot, so that we'll have valid values
     * for unreplaced columns.
     */
    Assert(natts == srcslot->tts_tupleDescriptor->natts);
    tableam_tslot_getallattrs(srcslot);
    rc = memcpy_s(slot->tts_values, natts * sizeof(Datum), srcslot->tts_values, natts * sizeof(Datum));
    securec_check(rc, "", "");
    rc = memcpy_s(slot->tts_isnull, natts * sizeof(Datum), srcslot->tts_isnull, natts * sizeof(bool));
    securec_check(rc, "", "");

    /* For error reporting, push callback + info on the error context stack */
    errarg.rel = rel;
    errarg.remote_attnum = -1;
    errcallback.callback = slot_store_error_callback;
    errcallback.arg = (void *)&errarg;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /* Call the "in" function for each replaced attribute */
    for (i = 0; i < natts; i++) {
        Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[i];
        int remoteattnum = rel->attrmap[i];

        if (remoteattnum < 0) {
            continue;
        }

        if (tupleData->colstatus[remoteattnum] != LOGICALREP_COLUMN_UNCHANGED) {
            StringInfo colvalue = &tupleData->colvalues[remoteattnum];
            errarg.remote_attnum = remoteattnum;

            if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_TEXT) {
                Oid typinput;
                Oid typioparam;

                getTypeInputInfo(att->atttypid, &typinput, &typioparam);
                slot->tts_values[i] = OidInputFunctionCall(typinput, colvalue->data, typioparam, att->atttypmod);
                slot->tts_isnull[i] = false;
            } else if (tupleData->colstatus[remoteattnum] == LOGICALREP_COLUMN_BINARY) {
                Oid typreceive;
                Oid typioparam;

                /*
                 * In some code paths we may be asked to re-parse the same
                 * tuple data.  Reset the StringInfo's cursor so that works.
                 */
                colvalue->cursor = 0;

                getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
                slot->tts_values[i] = OidReceiveFunctionCall(typreceive, colvalue, typioparam, att->atttypmod);

                /* Trouble if it didn't eat the whole buffer */
                if (colvalue->cursor != colvalue->len) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                        errmsg("incorrect binary data format in logical replication column %d", remoteattnum + 1)));
                }
                slot->tts_isnull[i] = false;
            } else {
                /* must be LOGICALREP_COLUMN_NULL */
                slot->tts_values[i] = (Datum) 0;
                slot->tts_isnull[i] = true;
            }
            
            errarg.remote_attnum = -1;
        }
    }

    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;

    /* And finally, declare that "slot" contains a valid virtual tuple */
    ExecStoreVirtualTuple(slot);
}

/*
 * Handle BEGIN message.
 */
static void apply_handle_begin(StringInfo s)
{
    LogicalRepBeginData begin_data;

    logicalrep_read_begin(s, &begin_data);

    t_thrd.applyworker_cxt.inRemoteTransaction = true;

    pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle COMMIT message.
 */
static void apply_handle_commit(StringInfo s)
{
    LogicalRepCommitData commit_data;

    logicalrep_read_commit(s, &commit_data);

    if (IsTransactionState()) {
        /*
         * Update origin state so we can restart streaming from correct
         * position in case of crash.
         */
        u_sess->reporigin_cxt.originTs = commit_data.committime;
        u_sess->reporigin_cxt.originLsn = commit_data.end_lsn;

        CommitTransactionCommand();
        pgstat_report_stat(false);
        store_flush_position(commit_data.end_lsn);
    }

    t_thrd.applyworker_cxt.inRemoteTransaction = false;

    pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Handle ORIGIN message.
 */
static void apply_handle_origin(StringInfo s)
{
    /*
     * ORIGIN message can only come inside remote transaction and before
     * any actual writes.
     */
    if (!t_thrd.applyworker_cxt.inRemoteTransaction || IsTransactionState())
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("ORIGIN message sent out of order")));
}

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here. The validation
 * against local schema is postponed until first change for given relation
 * comes as we only care about it when applying changes for it anyway and we
 * do less locking this way.
 */
static void apply_handle_relation(StringInfo s)
{
    LogicalRepRelation *rel;

    rel = logicalrep_read_rel(s);
    logicalrep_relmap_update(rel);
}

/*
 * Handle TYPE message.
 *
 * This is now vestigial; we read the info and discard it.
 */
static void apply_handle_type(StringInfo s)
{
    LogicalRepTyp typ;

    logicalrep_read_typ(s, &typ);
}

/*
 * Get replica identity index or if it is not defined a primary key.
 *
 * If neither is defined, returns InvalidOid
 */
static Oid GetRelationIdentityOrPK(Relation rel)
{
    Oid idxoid;

    idxoid = RelationGetReplicaIndex(rel);
    if (!OidIsValid(idxoid)) {
        idxoid = RelationGetPrimaryKeyIndex(rel);
    }
    return idxoid;
}

/*
 * Handle INSERT message.
 */
static void apply_handle_insert(StringInfo s)
{
    LogicalRepRelMapEntry *rel;
    LogicalRepTupleData newtup;
    LogicalRepRelId relid;
    EState *estate;
    TupleTableSlot *remoteslot;
    MemoryContext oldctx;
    FakeRelationPartition fakeRelInfo;

    ensure_transaction();

    relid = logicalrep_read_insert(s, &newtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);

    /* Initialize the executor state. */
    estate = create_estate_for_relation(rel);
    remoteslot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);
    ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->localrel));

    /* Input functions may need an active snapshot, so get one */
    PushActiveSnapshot(GetTransactionSnapshot());
    /* Process and store remote tuple in the slot */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, &newtup);
    slot_fill_defaults(rel, estate, remoteslot);
    MemoryContextSwitchTo(oldctx);

    ExecOpenIndices(estate->es_result_relation_info, false);

    /* Get fake relation and partition for patitioned table */
    GetFakeRelAndPart(estate, rel->localrel, remoteslot, &fakeRelInfo);

    /* Do the insert. */
    ExecSimpleRelationInsert(estate, remoteslot, &fakeRelInfo);

    /* Cleanup. */
    ExecCloseIndices(estate->es_result_relation_info);
    /* free the fakeRelationCache */
    if (estate->esfRelations != NULL) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }
    PopActiveSnapshot();

    /* Handle queued AFTER triggers. */
    AfterTriggerEndQuery(estate);

    ExecResetTupleTable(estate->es_tupleTable, false);
    FreeExecutorState(estate);

    logicalrep_rel_close(rel, NoLock);

    CommandCounterIncrement();
}

/*
 * Check if the logical replication relation is updatable and throw
 * appropriate error if it isn't.
 */
static void check_relation_updatable(LogicalRepRelMapEntry *rel)
{
    /* Updatable, no error. */
    if (rel->updatable)
        return;

    /*
     * We are in error mode so it's fine this is somewhat slow.
     * It's better to give user correct error.
     */
    if (OidIsValid(GetRelationIdentityOrPK(rel->localrel))) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("publisher did not send replica identity column "
            "expected by the logical replication target relation \"%s.%s\"",
            rel->remoterel.nspname, rel->remoterel.relname)));
    }

    ereport(ERROR,
        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("logical replication target relation \"%s.%s\" has "
        "neither REPLICA IDENTIY index nor PRIMARY "
        "KEY and published relation does not have "
        "REPLICA IDENTITY FULL",
        rel->remoterel.nspname, rel->remoterel.relname)));
}

static inline void CleanupEstate(EState *estate, EPQState *epqstate, LogicalRepRelMapEntry *rel)
{
    ExecCloseIndices(estate->es_result_relation_info);
    /* free the fakeRelationCache */
    if (estate->esfRelations != NULL) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }
    PopActiveSnapshot();

    /* Handle queued AFTER triggers. */
    AfterTriggerEndQuery(estate);

    EvalPlanQualEnd(epqstate);
    ExecResetTupleTable(estate->es_tupleTable, false);
    FreeExecutorState(estate);

    logicalrep_rel_close(rel, NoLock);

    CommandCounterIncrement();
}

/*
 * Handle UPDATE message.
 */
static void apply_handle_update(StringInfo s)
{
    LogicalRepRelMapEntry *rel;
    LogicalRepRelId relid;
    Oid idxoid;
    EState *estate;
    EPQState epqstate;
    LogicalRepTupleData oldtup;
    LogicalRepTupleData newtup;
    bool has_oldtup;
    TupleTableSlot *localslot;
    TupleTableSlot *remoteslot;
    RangeTblEntry *target_rte = NULL;
    bool found = false;
    MemoryContext oldctx;
    FakeRelationPartition fakeRelInfo;

    ensure_transaction();

    relid = logicalrep_read_update(s, &has_oldtup, &oldtup, &newtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);

    /* Check if we can do the update. */
    check_relation_updatable(rel);

    /* Initialize the executor state. */
    estate = create_estate_for_relation(rel);
    remoteslot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);
    ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->localrel));
    localslot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->localrel));
    EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

    /*
     * Populate updatedCols so that per-column triggers can fire.  This could
     * include more columns than were actually changed on the publisher
     * because the logical replication protocol doesn't contain that
     * information.  But it would for example exclude columns that only exist
     * on the subscriber, since we are not touching those.
     */
    target_rte = (RangeTblEntry*)list_nth(estate->es_range_table, 0);
    for (int i = 0; i < remoteslot->tts_tupleDescriptor->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);
        int remoteattnum = rel->attrmap[i];
        if (!att->attisdropped && remoteattnum >= 0) {
            Assert(remoteattnum < newtup.ncols);
            if (newtup.colstatus[i] != LOGICALREP_COLUMN_UNCHANGED) {
                target_rte->updatedCols = bms_add_member(target_rte->updatedCols,
                                                     i + 1 - FirstLowInvalidHeapAttributeNumber);
            }
        }
    }

    setExtraUpdatedCols(target_rte, RelationGetDescr(rel->localrel));

    PushActiveSnapshot(GetTransactionSnapshot());
    ExecOpenIndices(estate->es_result_relation_info, false);

    /* Build the search tuple. */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, has_oldtup ? &oldtup : &newtup);
    MemoryContextSwitchTo(oldctx);

    /*
     * Try to find tuple using either replica identity index, primary key
     * or if needed, sequential scan.
     */
    idxoid = GetRelationIdentityOrPK(rel->localrel);
    Assert(OidIsValid(idxoid) || (rel->remoterel.replident == REPLICA_IDENTITY_FULL && has_oldtup));

    /* find tuple by index scan or seq scan */
    found = RelationFindReplTuple(estate, rel->localrel, idxoid, LockTupleExclusive, remoteslot,
        localslot, &fakeRelInfo);
    ExecClearTuple(remoteslot);

    /*
     * Tuple found.
     *
     * Note this will fail if there are other conflicting unique indexes.
     */
    if (found) {
        /* Process and store remote tuple in the slot */
        oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        slot_modify_data(remoteslot, localslot, rel, &newtup);
        MemoryContextSwitchTo(oldctx);

        EvalPlanQualSetSlot(&epqstate, remoteslot);

        /* Do the actual update. */
        ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot, &fakeRelInfo);
    } else {
        /*
         * The tuple to be updated could not be found.
         */
        elog(LOG,
            "logical replication did not find row for update "
            "in replication target relation \"%s\"",
            RelationGetRelationName(rel->localrel));
    }

    /* Cleanup. */
    if (fakeRelInfo.needRleaseDummyRel && fakeRelInfo.partRel) {
        releaseDummyRelation(&fakeRelInfo.partRel);
    }
    if (fakeRelInfo.partList) {
        releasePartitionList(rel->localrel, &fakeRelInfo.partList, NoLock);
    }
    CleanupEstate(estate, &epqstate, rel);
}

/*
 * Handle DELETE message.
 */
static void apply_handle_delete(StringInfo s)
{
    LogicalRepRelMapEntry *rel;
    LogicalRepTupleData oldtup;
    LogicalRepRelId relid;
    Oid idxoid;
    EState *estate;
    EPQState epqstate;
    TupleTableSlot *remoteslot;
    TupleTableSlot *localslot;
    bool found = false;
    MemoryContext oldctx;
    FakeRelationPartition fakeRelInfo;

    ensure_transaction();

    relid = logicalrep_read_delete(s, &oldtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);

    /* Check if we can do the delete. */
    check_relation_updatable(rel);

    /* Initialize the executor state. */
    estate = create_estate_for_relation(rel);
    remoteslot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);
    ExecSetSlotDescriptor(remoteslot, RelationGetDescr(rel->localrel));
    localslot = ExecInitExtraTupleSlot(estate, rel->localrel->rd_tam_type);
    ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->localrel));
    EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

    PushActiveSnapshot(GetTransactionSnapshot());
    ExecOpenIndices(estate->es_result_relation_info, false);

    /* Find the tuple using the replica identity index. */
    oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    slot_store_data(remoteslot, rel, &oldtup);
    MemoryContextSwitchTo(oldctx);

    /*
     * Try to find tuple using either replica identity index, primary key
     * or if needed, sequential scan.
     */
    idxoid = GetRelationIdentityOrPK(rel->localrel);
    Assert(OidIsValid(idxoid) || (rel->remoterel.replident == REPLICA_IDENTITY_FULL));

    found = RelationFindReplTuple(estate, rel->localrel, idxoid, LockTupleExclusive,
        remoteslot, localslot, &fakeRelInfo);
    /* If found delete it. */
    if (found) {
        EvalPlanQualSetSlot(&epqstate, localslot);

        /* Do the actual delete. */
        ExecSimpleRelationDelete(estate, &epqstate, localslot, &fakeRelInfo);
    } else {
        /* The tuple to be deleted could not be found. */
        ereport(DEBUG1, (errmsg("logical replication could not find row for delete "
            "in replication target \"%s\"",
            RelationGetRelationName(rel->localrel))));
    }

    /* Cleanup. */
    if (fakeRelInfo.needRleaseDummyRel && fakeRelInfo.partRel) {
        releaseDummyRelation(&fakeRelInfo.partRel);
    }
    if (fakeRelInfo.partList) {
        releasePartitionList(rel->localrel, &fakeRelInfo.partList, NoLock);
    }
    CleanupEstate(estate, &epqstate, rel);
}


/*
 * Logical replication protocol message dispatcher.
 */
static void apply_dispatch(StringInfo s)
{
    char action = pq_getmsgbyte(s);

    switch (action) {
        /* BEGIN */
        case 'B':
            apply_handle_begin(s);
            break;
        /* COMMIT */
        case 'C':
            apply_handle_commit(s);
            break;
        /* INSERT */
        case 'I':
            apply_handle_insert(s);
            break;
        /* UPDATE */
        case 'U':
            apply_handle_update(s);
            break;
        /* DELETE */
        case 'D':
            apply_handle_delete(s);
            break;
        /* RELATION */
        case 'R':
            apply_handle_relation(s);
            break;
        /* TYPE */
        case 'Y':
            apply_handle_type(s);
            break;
        /* ORIGIN */
        case 'O':
            apply_handle_origin(s);
            break;
        case 'S':
            apply_handle_conninfo(s);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("invalid logical replication message type \"%c\"", action)));
    }
}

/*
 * Figure out which write/flush positions to report to the walsender process.
 *
 * We can't simply report back the last LSN the walsender sent us because the
 * local transaction might not yet be flushed to disk locally. Instead we
 * build a list that associates local with remote LSNs for every commit. When
 * reporting back the flush position to the sender we iterate that list and
 * check which entries on it are already locally flushed. Those we can report
 * as having been flushed.
 *
 * The have_pending_txes is true if there are outstanding transactions that
 * need to be flushed.
 */
static void get_flush_position(XLogRecPtr *write, XLogRecPtr *flush, bool *have_pending_txes)
{
    dlist_mutable_iter iter;
    XLogRecPtr local_flush = GetFlushRecPtr();

    *write = InvalidXLogRecPtr;
    *flush = InvalidXLogRecPtr;

    dlist_foreach_modify(iter, &t_thrd.applyworker_cxt.lsnMapping)
    {
        FlushPosition *pos = dlist_container(FlushPosition, node, iter.cur);

        *write = pos->remote_end;

        if (pos->local_end <= local_flush) {
            *flush = pos->remote_end;
            dlist_delete(iter.cur);
            pfree(pos);
        } else {
            /*
             * Don't want to uselessly iterate over the rest of the list which
             * could potentially be long. Instead get the last element and
             * grab the write position from there.
             */
            pos = dlist_tail_element(FlushPosition, node, &t_thrd.applyworker_cxt.lsnMapping);
            *write = pos->remote_end;
            *have_pending_txes = true;
            return;
        }
    }

    *have_pending_txes = !dlist_is_empty(&t_thrd.applyworker_cxt.lsnMapping);
}

/*
 * Store current remote/local lsn pair in the tracking list.
 */
static void store_flush_position(XLogRecPtr remote_lsn)
{
    FlushPosition *flushpos;

    /* Need to do this in permanent context */
    MemoryContextSwitchTo(t_thrd.applyworker_cxt.applyContext);

    /* Track commit lsn  */
    flushpos = (FlushPosition *)palloc(sizeof(FlushPosition));
    flushpos->local_end = t_thrd.xlog_cxt.XactLastCommitEnd;
    flushpos->remote_end = remote_lsn;

    dlist_push_tail(&t_thrd.applyworker_cxt.lsnMapping, &flushpos->node);
    MemoryContextSwitchTo(t_thrd.applyworker_cxt.messageContext);
}


/* Update statistics of the worker. */
static void UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
{
    t_thrd.applyworker_cxt.curWorker->last_lsn = last_lsn;
    t_thrd.applyworker_cxt.curWorker->last_send_time = send_time;
    t_thrd.applyworker_cxt.curWorker->last_recv_time = GetCurrentTimestamp();
    if (reply) {
        t_thrd.applyworker_cxt.curWorker->reply_lsn = last_lsn;
        t_thrd.applyworker_cxt.curWorker->reply_time = send_time;
    }
}

static void ApplyWorkerProcessMsg(char type, StringInfo s, XLogRecPtr *lastRcv)
{
    if (type == 'w') {
        /* The msg header defined in WalSndPrepareWrite */
        XLogRecPtr dataStart = pq_getmsgint64(s);
        XLogRecPtr walEnd = pq_getmsgint64(s);
        TimestampTz sendTime = pq_getmsgint64(s);

        if (*lastRcv < dataStart)
            *lastRcv = dataStart;

        if (*lastRcv < walEnd)
            *lastRcv = walEnd;

        UpdateWorkerStats(*lastRcv, sendTime, false);

        apply_dispatch(s);
    } else if (type == 'k') {
        PrimaryKeepaliveMessage keepalive;
        pq_copymsgbytes(s, (char*)&keepalive, sizeof(PrimaryKeepaliveMessage));

        send_feedback(keepalive.walEnd, keepalive.replyRequested, false);
        UpdateWorkerStats(*lastRcv, keepalive.sendTime, true);
    }
}

static bool CheckTimeout(bool *pingSent, TimestampTz lastRecvTimestamp)
{
    /*
     * We didn't receive anything new. If we haven't heard anything
     * from the server for more than u_sess->attr.attr_storage.wal_receiver_timeout / 2,
     * ping the server. Also, if it's been longer than
     * u_sess->attr.attr_storage.wal_receiver_status_interval since the last update we sent,
     * send a status update to the master anyway, to report any
     * progress in applying WAL.
     */
    bool requestReply = false;

    /*
     * Check if time since last receive from standby has
     * reached the configured limit.
     */
    if (u_sess->attr.attr_storage.wal_receiver_timeout > 0) {
        TimestampTz now = GetCurrentTimestamp();
        TimestampTz timeout;

        timeout =
            TimestampTzPlusMilliseconds(lastRecvTimestamp, u_sess->attr.attr_storage.wal_receiver_timeout);
        if (now > timeout)
            ereport(ERROR, (errmsg("terminating logical replication worker due to timeout")));

        /*
         * We didn't receive anything new, for half of
         * receiver replication timeout. Ping the server.
         */
        if (!(*pingSent)) {
            timeout = TimestampTzPlusMilliseconds(lastRecvTimestamp,
                (u_sess->attr.attr_storage.wal_receiver_timeout * HALF));
            if (now >= timeout) {
                requestReply = true;
                *pingSent = true;
            }
        }
    }

    return requestReply;
}

static inline void ProcessApplyWorkerInterrupts(void)
{
    if (t_thrd.applyworker_cxt.got_SIGTERM) {
        ereport(FATAL, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                        errmsg("terminating logical replication worker due to administrator command")));
    }
}

/*
 * Apply main loop.
 */
static void ApplyLoop(void)
{
    XLogRecPtr last_received = InvalidXLogRecPtr;
    bool ping_sent = false;
    TimestampTz last_recv_timestamp = GetCurrentTimestamp();

    /*
     * Init the ApplyMessageContext which we clean up after each
     * replication protocol message.
     */
    t_thrd.applyworker_cxt.messageContext = AllocSetContextCreate(t_thrd.applyworker_cxt.applyContext,
        "ApplyMessageContext", ALLOCSET_DEFAULT_SIZES);

    /* mark as idle, before starting to loop */
    pgstat_report_activity(STATE_IDLE, NULL);

    for (;;) {
        MemoryContextSwitchTo(t_thrd.applyworker_cxt.messageContext);

        int len;
        char *buf = NULL;
        unsigned char type;

        CHECK_FOR_INTERRUPTS();

        /* Wait a while for data to arrive */
        if ((WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len)) {
            StringInfoData s;
            last_recv_timestamp = GetCurrentTimestamp();
            ping_sent = false;

            s.data = buf;
            s.len = len;
            s.cursor = 0;
            s.maxlen = -1;
            ApplyWorkerProcessMsg(type, &s, &last_received);

            while ((WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_receive(0, &type, &buf, &len)) {
                ProcessApplyWorkerInterrupts();
                last_recv_timestamp = GetCurrentTimestamp();
                ping_sent = false;
                s.data = buf;
                s.len = len;
                s.cursor = 0;
                s.maxlen = -1;
                ApplyWorkerProcessMsg(type, &s, &last_received);
                if (u_sess->attr.attr_storage.wal_receiver_status_interval > 0 &&
                    (TimestampDifferenceExceeds(t_thrd.applyworker_cxt.sendTime, last_recv_timestamp,
                    u_sess->attr.attr_storage.wal_receiver_status_interval * MSECS_PER_SEC) ||
                    TimestampDifferenceExceeds(last_recv_timestamp, t_thrd.applyworker_cxt.sendTime,
                    u_sess->attr.attr_storage.wal_receiver_status_interval * MSECS_PER_SEC))) {
                    ereport(DEBUG1,
                        (errmsg("send feedback, this may happen if we receive too much wal records at one time")));
                    send_feedback(last_received, false, false);
                }
                MemoryContextReset(t_thrd.applyworker_cxt.messageContext);
            }
            /* confirm all writes at once */
            send_feedback(last_received, false, false);
        } else {
            bool requestReply = CheckTimeout(&ping_sent, last_recv_timestamp);
            send_feedback(last_received, requestReply, requestReply);
        }

        if (!t_thrd.applyworker_cxt.inRemoteTransaction) {
            /*
             * If we didn't get any transactions for a while there might be
             * unconsumed invalidation messages in the queue, consume them now.
             */
            StartTransactionCommand();
            /* Check for subscription change */
            if (!t_thrd.applyworker_cxt.mySubscriptionValid)
                reread_subscription();
            CommitTransactionCommand();
        }

        if (t_thrd.applyworker_cxt.got_SIGHUP) {
            t_thrd.applyworker_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* Cleanup the memory. */
        MemoryContextResetAndDeleteChildren(t_thrd.applyworker_cxt.messageContext);
    }
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static void send_feedback(XLogRecPtr recvpos, bool force, bool requestReply)
{
    XLogRecPtr writepos;
    XLogRecPtr flushpos;
    TimestampTz now;
    bool have_pending_txes;
    int len = 0;
    char replybuf[sizeof(StandbyReplyMessage) + 1] = {0};
    StandbyReplyMessage* replyMsg = (StandbyReplyMessage*)(replybuf + 1);

    /*
     * If the user doesn't want status to be reported to the publisher, be
     * sure to exit before doing anything at all.
     */
    if (!force && u_sess->attr.attr_storage.wal_receiver_status_interval <= 0)
        return;

    /* It's legal to not pass a recvpos */
    if (recvpos < t_thrd.applyworker_cxt.lastRecvpos)
        recvpos = t_thrd.applyworker_cxt.lastRecvpos;

    get_flush_position(&writepos, &flushpos, &have_pending_txes);

    /*
     * No outstanding transactions to flush, we can report the latest
     * received position. This is important for synchronous replication.
     */
    if (!have_pending_txes)
        flushpos = writepos = recvpos;

    if (writepos < t_thrd.applyworker_cxt.lastWritepos)
        writepos = t_thrd.applyworker_cxt.lastWritepos;

    if (flushpos < t_thrd.applyworker_cxt.lastFlushpos)
        flushpos = t_thrd.applyworker_cxt.lastFlushpos;

    now = GetCurrentTimestamp();
    /* if we've already reported everything we're good */
    if (!force && writepos == t_thrd.applyworker_cxt.lastWritepos && flushpos == t_thrd.applyworker_cxt.lastFlushpos &&
        !(TimestampDifferenceExceeds(t_thrd.applyworker_cxt.sendTime, now,
        u_sess->attr.attr_storage.wal_receiver_status_interval * MSECS_PER_SEC) ||
        TimestampDifferenceExceeds(now, t_thrd.applyworker_cxt.sendTime,
        u_sess->attr.attr_storage.wal_receiver_status_interval * MSECS_PER_SEC))) {
        return;
    }
    t_thrd.applyworker_cxt.sendTime = now;

    replybuf[len] = 'r';
    len += 1;
    replyMsg->write = recvpos;
    replyMsg->flush = flushpos;
    replyMsg->apply = writepos;
    replyMsg->sendTime = now;
    replyMsg->replyRequested = requestReply;
    len += sizeof(StandbyReplyMessage);

    elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X", force,
        (uint32)(recvpos >> BITS_PER_INT), (uint32)recvpos, (uint32)(writepos >> BITS_PER_INT),
        (uint32)writepos, (uint32)(flushpos >> BITS_PER_INT), (uint32)flushpos);

    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(replybuf, len);

    if (recvpos > t_thrd.applyworker_cxt.lastRecvpos)
        t_thrd.applyworker_cxt.lastRecvpos = recvpos;
    if (writepos > t_thrd.applyworker_cxt.lastWritepos)
        t_thrd.applyworker_cxt.lastWritepos = writepos;
    if (flushpos > t_thrd.applyworker_cxt.lastFlushpos)
        t_thrd.applyworker_cxt.lastFlushpos = flushpos;
}


/*
 * Reread subscription info and exit on change.
 */
static void reread_subscription(void)
{
    MemoryContext oldctx;
    Subscription *newsub;

    /* Ensure allocations in permanent context. */
    oldctx = MemoryContextSwitchTo(t_thrd.applyworker_cxt.applyContext);

    newsub = GetSubscription(t_thrd.applyworker_cxt.curWorker->subid, true);
    /*
     * Exit if the subscription was removed.
     * This normally should not happen as the worker gets killed
     * during DROP SUBSCRIPTION.
     */
    if (!newsub) {
        ereport(LOG, (errmsg(
            "logical replication apply worker for subscription \"%s\" will stop because the subscription was removed",
            t_thrd.applyworker_cxt.mySubscription->name)));

        proc_exit(0);
    }

    /*
     * Exit if connection string was changed. The launcher will start
     * new worker.
     */
    if (strcmp(newsub->conninfo, t_thrd.applyworker_cxt.mySubscription->conninfo) != 0) {
        ereport(LOG, (errmsg("logical replication apply worker for subscription \"%s\" will restart because the "
                             "connection information was changed",
            t_thrd.applyworker_cxt.mySubscription->name)));

        proc_exit(0);
    }

    /*
     * We need to make new connection to new slot if slot name has changed
     * so exit here as well if that's the case.
     */
    if (strcmp(newsub->slotname, t_thrd.applyworker_cxt.mySubscription->slotname) != 0) {
        ereport(LOG, (errmsg("logical replication worker for subscription \"%s\" will "
            "restart because the replication slot name was changed",
            t_thrd.applyworker_cxt.mySubscription->name)));

        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        proc_exit(0);
    }

    /*
     * Exit if the subscription was disabled.
     * This normally should not happen as the worker gets killed
     * during ALTER SUBSCRIPTION ... DISABLE.
     */
    if (!newsub->enabled) {
        ereport(LOG, (errmsg("logical replication apply worker for subscription \"%s\" will "
            "stop because the subscription was disabled",
            t_thrd.applyworker_cxt.mySubscription->name)));

        proc_exit(0);
    }

    /*
     * Exit if publication list was changed. The launcher will start
     * new worker.
     */
    if (!equal(newsub->publications, t_thrd.applyworker_cxt.mySubscription->publications)) {
        ereport(LOG, (errmsg("logical replication apply worker for subscription \"%s\" will "
            "restart because subscription's publications were changed",
            t_thrd.applyworker_cxt.mySubscription->name)));

        proc_exit(0);
    }

    /*
    * Exit if any parameter that affects the remote connection was changed.
    * The launcher will start a new worker.
    */
    if (strcmp(newsub->name, t_thrd.applyworker_cxt.mySubscription->name) != 0 ||
        newsub->binary != t_thrd.applyworker_cxt.mySubscription->binary) {
            ereport(LOG, (errmsg("logical replication apply worker for subscription \"%s\" "
                "will restart because of a parameter change", t_thrd.applyworker_cxt.mySubscription->name)));
            proc_exit(0);
    }

    /* !slotname should never happen when enabled is true. */
    Assert(newsub->slotname);
    
    /* Check for other changes that should never happen too. */
    if (newsub->dbid != t_thrd.applyworker_cxt.mySubscription->dbid) {
        elog(ERROR, "subscription %u changed unexpectedly", t_thrd.applyworker_cxt.curWorker->subid);
    }

    /* Clean old subscription info and switch to new one. */
    FreeSubscription(t_thrd.applyworker_cxt.mySubscription);
    t_thrd.applyworker_cxt.mySubscription = newsub;

    MemoryContextSwitchTo(oldctx);

    /* Change synchronous commit according to the user's wishes */
    SetConfigOption("synchronous_commit", t_thrd.applyworker_cxt.mySubscription->synccommit, PGC_BACKEND,
        PGC_S_OVERRIDE);

    t_thrd.applyworker_cxt.mySubscriptionValid = true;
}

/*
 * Callback from subscription syscache invalidation.
 */
static void subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
    t_thrd.applyworker_cxt.mySubscriptionValid = false;
}


/* Logical Replication Apply worker entry point */
void ApplyWorkerMain()
{
    MemoryContext oldctx;
    char originname[NAMEDATALEN];
    RepOriginId originid;
    XLogRecPtr origin_startpos;
    int rc = 0;
    LibpqrcvConnectParam options;

    /* Attach to slot */
    logicalrep_worker_attach();

    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = APPLY_WORKER;

    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->conn_target = REPCONNTARGET_PUBLICATION;
    SpinLockRelease(&walrcv->mutex);

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "ApplyWorker";

    init_ps_display("apply worker process", "", "", "");

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, LogicalrepWorkerSighub);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, die);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, SIG_IGN);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);

    /* Early initialization */
    BaseInit();

    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        /* since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        FlushErrorState();

        AbortOutOfAnyTransaction();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1L * USECS_PER_MSEC * MSECS_PER_SEC);
    }

    SetProcessingMode(NormalProcessing);

    /* Initialise stats to a sanish value */
    t_thrd.applyworker_cxt.curWorker->last_send_time = t_thrd.applyworker_cxt.curWorker->last_recv_time =
        t_thrd.applyworker_cxt.curWorker->reply_time = GetCurrentTimestamp();

    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "logical replication apply", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /* Run as replica session replication role. */
    SetConfigOption("session_replication_role", "replica", PGC_SUSET, PGC_S_OVERRIDE);
    /*
     * Set always-secure search path, so malicious users can't redirect user
     * code (e.g. pg_index.indexprs).
     */
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, t_thrd.applyworker_cxt.curWorker->dbid, NULL,
        t_thrd.applyworker_cxt.curWorker->userid);
    t_thrd.proc_cxt.PostInit->InitApplyWorker();

    /* Load the subscription into persistent memory context. */
    t_thrd.applyworker_cxt.applyContext =
        AllocSetContextCreate(TopMemoryContext, "ApplyContext", ALLOCSET_DEFAULT_SIZES);
    StartTransactionCommand();
    oldctx = MemoryContextSwitchTo(t_thrd.applyworker_cxt.applyContext);
    t_thrd.applyworker_cxt.mySubscription = GetSubscription(t_thrd.applyworker_cxt.curWorker->subid, true);
    if (!t_thrd.applyworker_cxt.mySubscription) {
        ereport(LOG,
                (errmsg("logical replication apply worker for subscription %u will not "
                        "start because the subscription was removed during startup",
                        t_thrd.applyworker_cxt.curWorker->subid)));
        proc_exit(0);
    }

    t_thrd.applyworker_cxt.mySubscriptionValid = true;
    MemoryContextSwitchTo(oldctx);

    if (!t_thrd.applyworker_cxt.mySubscription->enabled) {
        ereport(LOG, (errmsg("logical replication apply worker for subscription \"%s\" will not "
            "start because the subscription was disabled during startup",
            t_thrd.applyworker_cxt.mySubscription->name)));

        proc_exit(0);
    }

    /* Setup synchronous commit according to the user's wishes */
    SetConfigOption("synchronous_commit", t_thrd.applyworker_cxt.mySubscription->synccommit,
                    PGC_BACKEND, PGC_S_OVERRIDE);

    /* Keep us informed about subscription changes. */
    CacheRegisterThreadSyscacheCallback(SUBSCRIPTIONOID, subscription_change_cb, (Datum)0);

    ereport(LOG, (errmsg("logical replication apply for worker subscription \"%s\" has started",
        t_thrd.applyworker_cxt.mySubscription->name)));

    /* Setup replication origin tracking. */
    rc = sprintf_s(originname, sizeof(originname), "pg_%u", t_thrd.applyworker_cxt.mySubscription->oid);
    securec_check_ss(rc, "", "");
    originid = replorigin_by_name(originname, true);
    if (!OidIsValid(originid))
        originid = replorigin_create(originname);
    replorigin_session_setup(originid);
    u_sess->reporigin_cxt.originId = originid;
    origin_startpos = replorigin_session_get_progress(false);

    CommitTransactionCommand();

    if (!AttemptConnectPublisher(t_thrd.applyworker_cxt.mySubscription->conninfo,
        t_thrd.applyworker_cxt.mySubscription->name, true)) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Failed to connect to publisher.")));
    }

    /*
     * We don't really use the output identify_system for anything
     * but it does some initializations on the upstream so let's still
     * call it.
     */
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_identify_system();

    /* Build logical replication streaming options. */
    rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.logical = true;
    options.startpoint = origin_startpos;
    options.slotname = t_thrd.applyworker_cxt.mySubscription->slotname;
    options.protoVersion = LOGICALREP_PROTO_VERSION_NUM;
    options.publicationNames = t_thrd.applyworker_cxt.mySubscription->publications;
    options.binary = t_thrd.applyworker_cxt.mySubscription->binary;

    /* Start streaming from the slot. */
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_startstreaming(&options);

    /* Run the main loop. */
    ApplyLoop();

    ereport(LOG, (errmsg("ApplyWorker: shutting down")));
    proc_exit(0);
}

static inline void SkipBlank(char **cp)
{
    while (**cp) {
        if (!isspace((unsigned char)**cp)) {
            return;
        }
        (*cp)++;
    }
}

List* ConninfoToDefList(const char *conn)
{
    List *result = NIL;
    if (conn == NULL) {
        return result;
    }

    char* pname = NULL;
    char* pval = NULL;
    char* cp = NULL;
    char* cp2 = NULL;
    char *buf = pstrdup(conn);
    size_t len = strlen(buf);

    cp = buf;

    while (*cp) {
        /* Skip blanks before the parameter name */
        SkipBlank(&cp);

        /* Get the parameter name */
        pname = cp;
        while (*cp) {
            if (*cp == '=') {
                break;
            }
            if (isspace((unsigned char)*cp)) {
                *cp++ = '\0';
                SkipBlank(&cp);
                break;
            }
            cp++;
        }

        /* Check that there is a following '=' */
        if (*cp != '=') {
            pfree(buf);
            ereport(ERROR, (errmsg("missing \"=\" after \"%s\" in connection info string", pname)));
        }
        *cp++ = '\0';

        /* Skip blanks after the '=' */
        SkipBlank(&cp);

        /* Get the parameter value */
        pval = cp;

        if (*cp != '\'') {
            cp2 = pval;
            while (*cp) {
                if (isspace((unsigned char)*cp)) {
                    *cp++ = '\0';
                    break;
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0') {
                        *cp2++ = *cp++;
                    }
                } else {
                    *cp2++ = *cp++;
                }
            }
            *cp2 = '\0';
        } else {
            cp2 = pval;
            cp++;
            for (;;) {
                if (*cp == '\0') {
                    pfree(buf);
                    ereport(ERROR, (errmsg("unterminated quoted string in connection info string")));
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0') {
                        *cp2++ = *cp++;
                    }
                    continue;
                }
                if (*cp == '\'') {
                    *cp2 = '\0';
                    cp++;
                    break;
                }
                *cp2++ = *cp++;
            }
        }

        /*
         * Now that we have the name and the value, store the record.
         */
        result = lappend(result, makeDefElem(pstrdup(pname), (Node *)makeString(pstrdup(pval))));
    }

    int rc = memset_s(buf, len, 0, len);
    securec_check(rc, "", "");
    pfree(buf);
    return result;
}

char* DefListToString(const List *defList)
{
    if (unlikely(defList == NIL)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("LogicDecode[Publication]: failed to transfer DefElem list to string, null input")));
    }

    ListCell* cell = NULL;
    char* srcString = NULL;
    StringInfoData buf;
    initStringInfo(&buf);

    foreach (cell, defList) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (def->arg == NULL || !IsA(def->arg, String))
            continue;

        srcString = strVal(def->arg);
        if (srcString == NULL || strlen(srcString) == 0)
            continue;

        appendStringInfo(&buf, "%s=%s ", def->defname, srcString);
    }
    return buf.data;
}

/*
 * Handle conninfo update message.
 */
static void apply_handle_conninfo(StringInfo s)
{
    char* standbysInfo = NULL;
    logicalrep_read_conninfo(s, &standbysInfo);
    UpdateConninfo(standbysInfo);
    pfree_ext(standbysInfo);
}

static void UpdateConninfo(char* standbysInfo)
{
    Relation rel;
    bool nulls[Natts_pg_subscription];
    bool replaces[Natts_pg_subscription];
    Datum values[Natts_pg_subscription];
    HeapTuple tup;
    Subscription* sub = t_thrd.applyworker_cxt.mySubscription;
    Oid subid = sub->oid;

    StartTransactionCommand();
    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);
    /* Fetch the existing tuple. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId,
        CStringGetDatum(t_thrd.applyworker_cxt.mySubscription->name));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription \"%s\" does not exist",
            t_thrd.applyworker_cxt.mySubscription->name)));
    }
    subid = HeapTupleGetOid(tup);

    /* Form a new tuple. */
    int rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "", "");

    /* get conninfoWithoutHostport */
    StringInfoData conninfoWithoutHostport;
    initStringInfo(&conninfoWithoutHostport);
    ParseConninfo(sub->conninfo, &conninfoWithoutHostport, (HostPort**)NULL);

    /* join conninfoWithoutHostport together with standbysinfo */
    appendStringInfo(&conninfoWithoutHostport, " %s", standbysInfo);
    /* Replace connection information */
    values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(conninfoWithoutHostport.data);
    replaces[Anum_pg_subscription_subconninfo - 1] = true;
    tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    /* Update the catalog. */
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    heap_close(rel, RowExclusiveLock);
    CommitTransactionCommand();

    ereport(LOG, (errmsg("Update conninfo successfully, new conninfo %s.", standbysInfo)));
}

/*
 * Is current process a logical replication worker?
 */
bool IsLogicalWorker(void)
{
    return t_thrd.applyworker_cxt.curWorker != NULL;
}
