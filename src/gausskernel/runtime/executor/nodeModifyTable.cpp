/* -------------------------------------------------------------------------
 *
 * nodeModifyTable.cpp
 *	  routines to handle ModifyTable nodes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeModifyTable.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecInitModifyTable - initialize the ModifyTable node
 *		ExecModifyTable		- retrieve the next tuple from the node
 *		ExecEndModifyTable	- shut down the ModifyTable node
 *		ExecReScanModifyTable - rescan the ModifyTable node
 *
 *	 NOTES
 *		Each ModifyTable node contains a list of one or more subplans,
 *		much like an Append node.  There is one subplan per result relation.
 *		The key reason for this is that in an inherited UPDATE command, each
 *		result relation could have a different schema (more or different
 *		columns) requiring a different plan tree to produce it.  In an
 *		inherited DELETE, all the subplans should produce the same output
 *		rowtype, but we might still find that different plans are appropriate
 *		for different child relations.
 *
 *		If the query specifies RETURNING, then the ModifyTable returns a
 *		RETURNING tuple after completing each row insert, update, or delete.
 *		It must be called again to continue the operation.	Without RETURNING,
 *		we just loop within the node until all the work is done, then
 *		return NULL.  This avoids useless call/return overhead.
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/storage_gtt.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/matview.h"
#ifdef PGXC
#include "access/sysattr.h"
#endif
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/exec/execMerge.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteHandler.h"
#ifdef PGXC
#include "parser/parsetree.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#endif
#include "replication/dataqueue.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "commands/copy.h"
#include "commands/copypartition.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecmergeinto.h"
#include "access/heapam.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_whitebox_test.h"
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"

#ifdef PGXC
static void RecoredGeneratedExpr(ResultRelInfo *resultRelInfo, EState *estate, CmdType cmdtype);

/* Copied from trigger.c */
#define GetUpdatedColumns(relinfo, estate) \
    (rt_fetch((relinfo)->ri_RangeTableIndex, (estate)->es_range_table)->updatedCols)

/* Copied from tid.c */
#define DatumGetItemPointer(X) ((ItemPointer)DatumGetPointer(X))
#endif

static TupleTableSlot* ExecModifyTable(PlanState* state);
extern CopyFromManager initCopyFromManager(MemoryContext parent, Relation heapRel, bool isInsertSelect);
extern void deinitCopyFromManager(CopyFromManager mgr);
extern void FlushInsertSelectBulk(
    DistInsertSelectState* node, EState* estate, bool canSetTag, int hi_options, List** partitionList);
extern void FlushErrorInfo(Relation rel, EState* estate, ErrorCacheEntry* cache);
extern void HeapInsertCStore(Relation relation, ResultRelInfo* resultRelInfo, HeapTuple tup, int option);
extern void HeapDeleteCStore(Relation relation, ItemPointer tid, Oid tableOid, Snapshot snapshot);
extern Oid pg_get_serial_sequence_oid(text* tablename, text* columnname);
#ifdef ENABLE_MULTIPLE_NODES
extern void HeapInsertTsStore(Relation relation, ResultRelInfo* resultRelInfo, HeapTuple tup, int option);
static TupleTableSlot* fill_slot_with_oldvals(TupleTableSlot* slot, HeapTupleHeader oldtuphd, Bitmapset* modifiedCols);
#endif   /* ENABLE_MULTIPLE_NODES */

/* check if set_dummy_tlist_references has set the dummy targetlist */
static bool has_dummy_targetlist(Plan* plan)
{
    bool is_dummy = false;

    switch (nodeTag(plan)) {
        case T_VecToRow:
        case T_RowToVec:
        case T_Stream:
        case T_VecStream:
        case T_Limit:
        case T_VecLimit:
        case T_Sort:
        case T_VecSort:
            is_dummy = true;
            break;
        default:
            is_dummy = false;
            break;
    }

    return is_dummy;
}
/**
 * @Description: Handle plan output.
 * @in subPlan, the subplan of modify node.
 * @in resultRel, the relation to be modified.
 */
static void CheckPlanOutput(Plan* subPlan, Relation resultRel)
{
    /*
     * Compared to pgxc, we have increased the stream plan,
     * this destroy the logic of the function ExecCheckPlanOutput.
     * Modify to use targetlist of stream(VecToRow/RowToVec/PartIterator)->subplan as
     * parameter of ExecCheckPlanOutput.
     */
    switch (nodeTag(subPlan)) {
        case T_Stream:
        case T_VecStream:
        case T_VecToRow:
        case T_RowToVec:
        case T_PartIterator:
        case T_VecPartIterator:
        case T_Limit:
        case T_VecLimit:
        case T_Sort:
        case T_VecSort: {
#ifdef ENABLE_MULTIPLE_NODES
            if (!IS_PGXC_COORDINATOR) {
                break;
            }
#endif
            /*
             * dummy target list cannot pass ExecCheckPlanOutput,
             * so we desend until we found a non-dummy plan
             */
            do {
                /* should not be null, or we have something real bad */
                Assert(subPlan->lefttree != NULL);
                /* let's dig deeper */
                subPlan = subPlan->lefttree;
            } while (has_dummy_targetlist(subPlan));

            CheckPlanOutput(subPlan, resultRel);

            break;
        }
        default: {
            ExecCheckPlanOutput(resultRel, subPlan->targetlist);
            break;
        }
    }
}

/*
 * Verify that the tuples to be produced by INSERT or UPDATE match the
 * target relation's rowtype
 *
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 */
void ExecCheckPlanOutput(Relation resultRel, List* targetList)
{
    TupleDesc result_desc = RelationGetDescr(resultRel);
    int attno = 0;
    ListCell* lc = NULL;

    foreach (lc, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Form_pg_attribute attr;

        if (tle->resjunk)
            continue; /* ignore junk tlist items */

        if (attno >= result_desc->natts)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_EXECUTOR),
                    errmsg("table row type and query-specified row type do not match"),
                    errdetail("Query has too many columns.")));
        attr = &result_desc->attrs[attno++];

        if (!attr->attisdropped) {
            /* Normal case: demand type match */
            if (exprType((Node*)tle->expr) != attr->atttypid)
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_EXECUTOR),
                        errmsg("table row type and query-specified row type do not match"),
                        errdetail("Table has type %s at ordinal position %d, but query expects %s.",
                            format_type_be(attr->atttypid),
                            attno,
                            format_type_be(exprType((Node*)tle->expr)))));
        } else {
            /*
             * For a dropped column, we can't check atttypid (it's likely 0).
             * In any case the planner has most likely inserted an INT4 null.
             * What we insist on is just *some* NULL constant.
             */
            if (!IsA(tle->expr, Const) || !((Const*)tle->expr)->constisnull)
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_EXECUTOR),
                        errmsg("table row type and query-specified row type do not match"),
                        errdetail("Query provides a value for a dropped column at ordinal position %d.", attno)));
        }
    }
    attno = resultRel->rd_isblockchain ? (attno + 1) : attno;
    if (attno != result_desc->natts)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_EXECUTOR),
                errmsg("table row type and query-specified row type do not match"),
                errdetail("Query has too few columns.")));
}

/*
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * projectReturning: RETURNING projection info for current result rel
 * tupleSlot: slot holding tuple actually inserted/updated/deleted
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Returns a slot holding the result tuple
 */
static TupleTableSlot* ExecProcessReturning(
    ProjectionInfo* projectReturning, TupleTableSlot* tupleSlot, TupleTableSlot* planSlot)
{
    ExprContext* econtext = projectReturning->pi_exprContext;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous cycle.
     */
    ResetExprContext(econtext);

    /* Make tuple and any needed join variables available to ExecProject */
    econtext->ecxt_scantuple = tupleSlot;
    econtext->ecxt_outertuple = planSlot;

    /* Compute the RETURNING expressions */
    return ExecProject(projectReturning);
}

static void ExecCheckTIDVisible(Relation        targetrel, EState* estate, Relation rel, ItemPointer tid)
{
    /* check isolation level to tell if tuple visibility check is needed */
    if (!IsolationUsesXactSnapshot()) {
        return;
    }

    Tuple tuple = tableam_tops_new_tuple(targetrel, tid);

    Buffer      buffer = InvalidBuffer;
    if (!tableam_tuple_fetch(rel, SnapshotAny, (HeapTuple)tuple, &buffer, false, NULL)) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
            errmsg("failed to fetch conflicting tuple for DUPLICATE KEY UPDATE")));
    }

    tableam_tuple_check_visible(targetrel, estate->es_snapshot, tuple, buffer);
    tableam_tops_destroy_tuple(targetrel, tuple);
    ReleaseBuffer(buffer);
}

static void RecoredGeneratedExpr(ResultRelInfo *resultRelInfo, EState *estate, CmdType cmdtype)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    int natts = tupdesc->natts;
    MemoryContext oldContext;

    Assert(tupdesc->constr && tupdesc->constr->has_generated_stored);

    /*
     * If first time through for this result relation, build expression
     * nodetrees for rel's stored generation expressions.  Keep them in the
     * per-query memory context so they'll survive throughout the query.
     */
    if (resultRelInfo->ri_GeneratedExprs == NULL) {
        oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

        resultRelInfo->ri_GeneratedExprs = (ExprState **)palloc(natts * sizeof(ExprState *));
        resultRelInfo->ri_NumGeneratedNeeded = 0;

        for (int i = 0; i < natts; i++) {
            if (GetGeneratedCol(tupdesc, i) == ATTRIBUTE_GENERATED_STORED) {
                Expr *expr;

                /*
                 * If it's an update and the current column was not marked as
                 * being updated, then we can skip the computation.  But if
                 * there is a BEFORE ROW UPDATE trigger, we cannot skip
                 * because the trigger might affect additional columns.
                 */
                if (cmdtype == CMD_UPDATE && !(rel->trigdesc && rel->trigdesc->trig_update_before_row) &&
                    !bms_is_member(i + 1 - FirstLowInvalidHeapAttributeNumber,
                    exec_rt_fetch(resultRelInfo->ri_RangeTableIndex, estate)->extraUpdatedCols)) {
                    resultRelInfo->ri_GeneratedExprs[i] = NULL;
                    continue;
                }

                expr = (Expr *)build_column_default(rel, i + 1);
                if (expr == NULL)
                    elog(ERROR, "no generation expression found for column number %d of table \"%s\"", i + 1,
                        RelationGetRelationName(rel));

                resultRelInfo->ri_GeneratedExprs[i] = ExecPrepareExpr(expr, estate);
                resultRelInfo->ri_NumGeneratedNeeded++;
            }
        }

        (void)MemoryContextSwitchTo(oldContext);
    }
}

/*
 * Compute stored generated columns for a tuple
 */
void ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo, EState *estate, TupleTableSlot *slot, Tuple oldtuple,
    CmdType cmdtype)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    uint32 natts = (uint32)tupdesc->natts;
    MemoryContext oldContext;
    Datum *values;
    bool *nulls;
    bool *replaces;
    Tuple newtuple;
    errno_t rc = EOK;

    RecoredGeneratedExpr(resultRelInfo, estate, cmdtype);

    /*
     * If no generated columns have been affected by this change, then skip
     * the rest.
     */
    if (resultRelInfo->ri_NumGeneratedNeeded == 0)
        return;

    oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    values = (Datum *)palloc(sizeof(*values) * natts);
    nulls = (bool *)palloc(sizeof(*nulls) * natts);
    replaces = (bool *)palloc0(sizeof(*replaces) * natts);
    rc = memset_s(replaces, sizeof(bool) * natts, 0, sizeof(bool) * natts);
    securec_check(rc, "\0", "\0");

    for (uint32 i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (GetGeneratedCol(tupdesc, i) == ATTRIBUTE_GENERATED_STORED && resultRelInfo->ri_GeneratedExprs[i]) {
            ExprContext *econtext;
            Datum val;
            bool isnull;

            econtext = GetPerTupleExprContext(estate);
            econtext->ecxt_scantuple = slot;

            val = ExecEvalExpr(resultRelInfo->ri_GeneratedExprs[i], econtext, &isnull, NULL);

            /*
             * We must make a copy of val as we have no guarantees about where
             * memory for a pass-by-reference Datum is located.
             */
            if (!isnull)
                val = datumCopy(val, attr->attbyval, attr->attlen);

            values[i] = val;
            nulls[i] = isnull;
            replaces[i] = true;
        }
    }

    newtuple = tableam_tops_modify_tuple(oldtuple, tupdesc, values, nulls, replaces);
    (void)ExecStoreTuple(newtuple, slot, InvalidBuffer, false);

    (void)MemoryContextSwitchTo(oldContext);
}

static bool GetUpdateExprCol(TupleDesc tupdesc, int atti)
{
    if (!tupdesc->constr)
        return false;

    if (tupdesc->constr->num_defval == 0)
        return false;

    return tupdesc->constr->has_on_update[atti];
}

static void RecoredUpdateExpr(ResultRelInfo *resultRelInfo, EState *estate, CmdType cmdtype)
{
    TupleDesc tupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
    int natts = tupdesc->natts;
    MemoryContext oldContext;

    Assert(tupdesc->constr && tupdesc->constr->has_on_update);

    /*
     * If first time through for this result relation, build expression
     * nodetrees for rel's stored generation expressions.  Keep them in the
     * per-query memory context so they'll survive throughout the query.
     */
    if (resultRelInfo->ri_UpdatedExprs == NULL) {
        oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

        resultRelInfo->ri_UpdatedExprs = (ExprState **)palloc(natts * sizeof(ExprState *));
        resultRelInfo->ri_NumUpdatedNeeded = 0;

        for (int i = 0; i < natts; i++) {
            if (GetUpdateExprCol(tupdesc, i)) {
                Expr *expr;

                expr = (Expr *)build_column_default(resultRelInfo->ri_RelationDesc, i + 1, false, true);
                if (expr == NULL)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("no update expression found for column number %d of table \"%s\"", i + 1,
                        RelationGetRelationName(resultRelInfo->ri_RelationDesc))));

                resultRelInfo->ri_UpdatedExprs[i] = ExecPrepareExpr(expr, estate);
                resultRelInfo->ri_NumUpdatedNeeded++;
            }
        }

        (void)MemoryContextSwitchTo(oldContext);
    }
}

/*
 * Compute stored updated columns for a tuple
 */
bool ExecComputeStoredUpdateExpr(ResultRelInfo *resultRelInfo, EState *estate, TupleTableSlot *slot, Tuple tuple,
    CmdType cmdtype, ItemPointer otid, Oid oldPartitionOid, int2 bucketid)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    uint32 natts = (uint32)tupdesc->natts;
    MemoryContext oldContext;
    Datum *values;
    bool *nulls;
    bool *replaces;
    Tuple newtuple;
    FmgrInfo eqproc;
    Oid opfuncoid = InvalidOid;
    bool match = false;
    bool update_fix_result = true;
    int temp_id = -1;
    int attnum;
    uint32 updated_colnum_resno;
    Bitmapset* updatedCols = GetUpdatedColumns(resultRelInfo, estate);

    /* use pertuple memory for trigger tuple */
    oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    HeapTuple oldtup = GetTupleForTrigger(estate, NULL, resultRelInfo, oldPartitionOid, bucketid, otid, LockTupleShared, NULL);
    MemoryContextSwitchTo(oldContext);

    RecoredUpdateExpr(resultRelInfo, estate, cmdtype);

    /*
     * If no generated columns have been affected by this change, then skip
     * the rest.
     */
    if (resultRelInfo->ri_NumUpdatedNeeded == 0)
        return true;

    /* compare update operator whether the newtuple is equal to the oldtuple,
     * if equal, so update don't fix the default column value  */
    Datum* oldvalues = (Datum*)palloc(natts * sizeof(Datum));
    bool* oldnulls = (bool*)palloc(natts * sizeof(bool));
    heap_deform_tuple(oldtup, tupdesc, oldvalues, oldnulls);
    temp_id = -1;
    attnum = bms_next_member(updatedCols, temp_id);
    updated_colnum_resno = attnum + FirstLowInvalidHeapAttributeNumber;
    temp_id = attnum;
    tableam_tslot_getallattrs(slot);
    for (int32 i = 0; i < (int32)natts; i++) {
        if (updated_colnum_resno  == (uint32)(i + 1)) {
            if (slot->tts_isnull[i] && oldnulls[i]) {
                match = true;
            } else if (slot->tts_isnull[i] && !oldnulls[i]) {
                match = false;
            } else if (!slot->tts_isnull[i] && oldnulls[i]) {
                match = false;
            } else {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                opfuncoid = OpernameGetOprid(list_make1(makeString("=")), attr->atttypid, attr->atttypid);
                if (OidIsValid(opfuncoid)) {
                    RegProcedure oprcode = get_opcode(opfuncoid);
                    fmgr_info(oprcode, &eqproc);
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, slot->tts_values[i], oldvalues[i]));
                } else {
                    Oid typoutput = 0;
                    bool typisvarlena = false;
                    getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
                    char *value_old = DatumGetCString(OidOutputFunctionCall(typoutput, oldvalues[i]));
                    char *value_new = DatumGetCString(OidOutputFunctionCall(typoutput, slot->tts_values[i]));
                    if (pg_strcasecmp(value_old, value_new) == 0) {
                        match = true;
                    } else {
                        match = false;
                    }
                }
            }
            update_fix_result = update_fix_result && match;
            attnum = bms_next_member(updatedCols, temp_id);
            if (attnum >= 0) {
                updated_colnum_resno = attnum + FirstLowInvalidHeapAttributeNumber;
                temp_id = attnum;
            }
        }
    }
    pfree_ext(oldvalues);
    pfree_ext(oldnulls);

    if (update_fix_result)
        return true;

    oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    values = (Datum *)palloc(sizeof(*values) * natts);
    nulls = (bool *)palloc(sizeof(*nulls) * natts);
    replaces = (bool *)palloc0(sizeof(*replaces) * natts);
    temp_id = -1;
    attnum = bms_next_member(updatedCols, temp_id);
    updated_colnum_resno = attnum + FirstLowInvalidHeapAttributeNumber;
    temp_id = attnum;
    for (uint32 i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (updated_colnum_resno == (i + 1)) {
            attnum = bms_next_member(updatedCols, temp_id);
            if (attnum >= 0) {
                updated_colnum_resno = attnum + FirstLowInvalidHeapAttributeNumber;
                temp_id = attnum;
            }
            continue;
        }
        if (GetUpdateExprCol(tupdesc, i) && resultRelInfo->ri_UpdatedExprs[i]) {
            ExprContext *econtext;
            Datum val;
            bool isnull = false;

            econtext = GetPerTupleExprContext(estate);
            econtext->ecxt_scantuple = slot;

            val = ExecEvalExpr(resultRelInfo->ri_UpdatedExprs[i], econtext, &isnull, NULL);

            /*
            * We must make a copy of val as we have no guarantees about where
            * memory for a pass-by-reference Datum is located.
            */
            if (!isnull)
                val = datumCopy(val, attr->attbyval, attr->attlen);

            values[i] = val;
            nulls[i] = isnull;
            replaces[i] = true;
        }
    }

    newtuple = tableam_tops_modify_tuple(tuple, tupdesc, values, nulls, replaces);
    (void)ExecStoreTuple(newtuple, slot, InvalidBuffer, false);
    (void)MemoryContextSwitchTo(oldContext);
    return false;
}

static bool ExecConflictUpdate(ModifyTableState* mtstate, ResultRelInfo* resultRelInfo, ConflictInfoData* conflictInfo,
    TupleTableSlot* planSlot, TupleTableSlot* excludedSlot, EState* estate, Relation targetRel,
    Oid oldPartitionOid, int2 bucketid, bool canSetTag, TupleTableSlot** returning, char* partExprKeyStr)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    Relation    relation = targetRel;
    UpsertState* upsertState = mtstate->mt_upsert;
    TM_Result test;
    TM_FailureData tmfd;
    Buffer      buffer;
    ItemPointer conflictTid = &conflictInfo->conflictTid;
    Tuple tuple = tableam_tops_new_tuple(targetRel, conflictTid);
    test = tableam_tuple_lock(relation, tuple, &buffer,
                              estate->es_output_cid, LockTupleExclusive, LockWaitBlock, &tmfd,
                              false, false, false, GetActiveSnapshot(), &conflictInfo->conflictTid, 
                              false, true, conflictInfo->conflictXid);

    WHITEBOX_TEST_STUB("ExecConflictUpdate_Begin", WhiteboxDefaultErrorEmit);

checktest:
    switch (test) {
        case TM_Ok:
            /* success */
            break;
        case TM_SelfUpdated:
            Assert(RelationIsUstoreFormat(relation));
        case TM_SelfCreated:
            /*
             * This can occur when a just inserted tuple is updated again in
             * the same command. E.g. because multiple rows with the same
             * conflicting key values are inserted using STREAM:
             * INSERT INTO t VALUES(1),(1) ON DUPLICATE KEY UPDATE ...
             *
             * This is somewhat similar to the ExecUpdate()
             * HeapTupleSelfUpdated case.  We do not want to proceed because
             * it would lead to the same row being updated a second time in
             * some unspecified order, and in contrast to plain UPDATEs
             * there's no historical behavior to break.
             *
             * It is the user's responsibility to prevent this situation from
             * occurring.  These problems are why SQL-2003 similarly specifies
             * that for SQL MERGE, an exception must be raised in the event of
             * an attempt to update the same row twice.
             *
             * However, in order by be compatible with SQL, we have to break the
             * rule and update the same row which is created within the command.
             */
            ReleaseBuffer(buffer);
#ifdef ENABLE_MULTIPLE_NODES
            if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("ON DUPLICATE KEY UPDATE command cannot affect row a second time"),
                    errhint("Ensure that no rows proposed for insertion within"
                        "the same command have duplicate constrained values.")));
            }
#endif
            test = tableam_tuple_lock(relation, tuple, &buffer,
                                      estate->es_output_cid, LockTupleExclusive, LockWaitBlock, &tmfd,
                                      true, false, false, estate->es_snapshot, &conflictInfo->conflictTid, 
                                      false, true, conflictInfo->conflictXid);

            Assert(test != TM_SelfCreated && test != TM_SelfUpdated);
            goto checktest;
            break;
        case TM_Deleted:
        case TM_Updated:
            ReleaseBuffer(buffer);
            if (IsolationUsesXactSnapshot()) {
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("could not serialize access due to concurrent update")));
            }
            /*
             * Tell caller to try again from the very start.
             * It does not make sense to use the usual EvalPlanQual() style
             * loop here, as the new version of the row might not conflict
             * anymore, or the conflicting tuple has actually been deleted.
             */
            tableam_tops_destroy_tuple(relation, tuple);
            return false;
        case TM_BeingModified:
            ReleaseBuffer(buffer);
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("unexpected concurrent update tuple")));
            break;
        default:
            ReleaseBuffer(buffer);
            elog(ERROR, "unrecognized heap_lock_tuple status: %u", test);
            break;
    }

    /*
     * Success, the tuple is locked.
     *
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous cycle.
     */
    ResetExprContext(econtext);

    /* NOTE: we rely on ExecUpdate() to do MVCC snapshot check, thus projection is
     * done here although the final ExecUpdate might be failed.
     */
    tableam_tuple_check_visible(relation, estate->es_snapshot, tuple, buffer);

    /* Store target's existing tuple in the state's dedicated slot */
    ExecStoreTuple(tuple, upsertState->us_existing, buffer, false);

    /*
     * Make tuple and any needed join variables available to ExecQual and
     * ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
     * the target's existing tuple is installed in the scantuple.  EXCLUDED has
     * been made to reference INNER_VAR in setrefs.c, but there is no other redirection.
     */
    econtext->ecxt_scantuple = upsertState->us_existing;
    econtext->ecxt_innertuple = excludedSlot;
    econtext->ecxt_outertuple = NULL;

    ExecProject(resultRelInfo->ri_updateProj, NULL);
    /* Evaluate where qual if exists, add to count if filtered */
    if (ExecQual(upsertState->us_updateWhere, econtext, false)) {
        *returning = ExecUpdate(conflictTid, oldPartitionOid, bucketid, NULL,
            upsertState->us_updateproj, planSlot, &mtstate->mt_epqstate,
            mtstate, canSetTag, ((ModifyTable*)mtstate->ps.plan)->partKeyUpdated, NULL, partExprKeyStr);
    } else {
        InstrCountFiltered1(&mtstate->ps, 1);
    }
    tableam_tops_destroy_tuple(relation, tuple);
    ReleaseBuffer(buffer);
    return true;
}

static inline void ReleaseResourcesForUpsertGPI(bool isgpi, Relation parentRel, Relation bucketRel, Relation *partRel,
                                                Partition part)
{
    if (isgpi) {
        if (RELATION_OWN_BUCKET(parentRel)) {
            bucketCloseRelation(bucketRel);
        } else {
            releaseDummyRelation(partRel);
        }
        partitionClose(parentRel, part, RowExclusiveLock);
    }
}

bool CheckPartitionOidForSpecifiedPartition(RangeTblEntry *rte, Oid partitionid, bool canIgnore = false)
{
    int level = canIgnore ? WARNING : ERROR;
    if (rte->isContainPartition) {
        ListCell *cell = NULL;
        foreach(cell, rte->partitionOidList) {
            if (lfirst_oid(cell) == partitionid) {
                return true;
            }
        }
        ereport(level, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("inserted partition key does not map to the table partition"), errdetail("N/A."),
                         errcause("The value is incorrect."), erraction("Use the correct value."))));
        return false;
    }
    return true;
}

bool CheckSubpartitionOidForSpecifiedSubpartition(RangeTblEntry *rte, Oid partitionid, Oid subPartitionId,
                                                  bool canIgnore = false)
{
    int level = canIgnore ? WARNING : ERROR;
    if (rte->isContainSubPartition) {
        ListCell *partCell = NULL;
        ListCell *subpartCell = NULL;
        forboth(partCell, rte->partitionOidList, subpartCell, rte->subpartitionOidList) {
            if (lfirst_oid(partCell) == partitionid &&
                (lfirst_oid(subpartCell) == subPartitionId || !OidIsValid(lfirst_oid(subpartCell)))) {
                return true;
            }
        }
        ereport(level, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("inserted subpartition key does not map to the table subpartition"), errdetail("N/A."),
                         errcause("The value is incorrect."), erraction("Use the correct value."))));
        return false;
    }
    return true;
}

static void ReportErrorForSpecifiedPartitionOfUpsert(const char *partition, const char *table)
{
    ereport(ERROR,
            (errcode(ERRCODE_NO_DATA_FOUND),
             (errmsg("The update target %s of upsert is inconsistent with the specified %s in "
                     "%s table.",
                     partition, partition, table),
              errdetail("N/A."),
              errcause("Specifies that the %s syntax does not allow updating inconsistent %s.", partition, partition),
              erraction("Modify the SQL statement."))));
}

static void CheckPartitionOidForUpsertSpecifiedPartition(RangeTblEntry *rte, Relation resultRelationDesc,
                                                         Oid targetPartOid)
{
    Oid partitionOid;
    Oid subpartitionOid;
    ListCell *partCell = NULL;
    ListCell *subpartCell = NULL;
    const char *table = NULL;
    const char *partition = NULL;

    if (rte->isContainSubPartition) {
        partition = (rte->subpartitionOidList->length == 1) ? "subpartition" : "partition";
    } else if (rte->isContainPartition) {
        partition = "partition";
    } else {
        /* partition is not specified */
        return;
    }

    if (RelationIsSubPartitioned(resultRelationDesc)) {
        table = "subpartition";
        partitionOid = partid_get_parentid(targetPartOid);
        subpartitionOid = targetPartOid;
    } else {
        table = "partition";
        partitionOid = targetPartOid;
        subpartitionOid = InvalidOid;
    }

    forboth(partCell, rte->partitionOidList, subpartCell, rte->subpartitionOidList) {
        if (!OidIsValid(lfirst_oid(subpartCell))) {
            if (lfirst_oid(partCell) == partitionOid) {
                return;
            }
        } else if (lfirst_oid(subpartCell) == subpartitionOid && lfirst_oid(partCell) == partitionOid) {
            return;
        }
    }
    /* targetPartOid is not in specified partition list */
    ReportErrorForSpecifiedPartitionOfUpsert(partition, table);
}

static void ConstraintsForExecUpsert(Relation resultRelationDesc)
{
    if (resultRelationDesc == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        (errmsg("The result relation is null"), errdetail("N/A."), errcause("System error."),
                         erraction("Contact engineer to support."))));
    }
    if (unlikely(RelationIsCUFormat(resultRelationDesc))) {
        ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                 (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("ON DUPLICATE KEY UPDATE is not supported on column orientated table"), errdetail("N/A."),
                  errcause("The function is not supported."), erraction("Contact engineer to support."))));
    }

    if (unlikely(RelationIsPAXFormat(resultRelationDesc))) {
        ereport(ERROR, (errmodule(MOD_EXECUTOR),
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("ON DUPLICATE KEY UPDATE is not supported on DFS table"), errdetail("N/A."),
                         errcause("The function is not supported."), erraction("Contact engineer to support."))));
    }
}

static void UpdateSlotTupleInfo(TupleTableSlot* slot, Tuple tuple)
{
    bool isUheapTuple = TUPLE_IS_UHEAP_TUPLE(tuple);
    if (!isUheapTuple && slot->tts_tam_ops == TableAmUstore) {
        UHeapTuple tup = (UHeapTuple)slot->tts_tuple;
        HeapTuple htup = (HeapTuple)tuple;
        tup->ctid = htup->t_self;
        tup->table_oid = htup->t_tableOid;
        tup->t_xid_base = htup->t_xid_base;
        tup->t_multi_base = htup->t_multi_base;
        tup->xmin = htup->xmin;
        tup->xmax = htup->xmax;
    } else {
        Assert(isUheapTuple && slot->tts_tam_ops == TableAmHeap);
        HeapTuple tup = (HeapTuple)slot->tts_tuple;
        UHeapTuple utup = (UHeapTuple)tuple;
        tup->t_self = utup->ctid;
        tup->t_tableOid = utup->table_oid;
        tup->t_xid_base = utup->t_xid_base;
        tup->t_multi_base = utup->t_multi_base;
        tup->xmin = utup->xmin;
        tup->xmax = utup->xmax;
    }
}

static Oid ExecUpsert(ModifyTableState* state, TupleTableSlot* slot, TupleTableSlot* planSlot, EState* estate,
    bool canSetTag, Tuple tuple, TupleTableSlot** returning, bool* updated, Oid* targetPartOid, char* partExprKeyStr)
{
    Oid newid = InvalidOid;
    bool        specConflict = false;
    List* recheckIndexes = NIL;
    ResultRelInfo* resultRelInfo = NULL;
    Relation    resultRelationDesc = NULL;
    Relation    heaprel = NULL; /* actual relation to upsert index */
    Relation    targetrel = NULL; /* actual relation to upsert tuple */
    Oid         partitionid = InvalidOid;
    Partition   partition = NULL; /* partition info for partition table */
    Oid subPartitionId = InvalidOid;
    Relation subPartRel = NULL;
    Partition subPart = NULL;
    int2 bucketid = InvalidBktId;
    ConflictInfoData conflictInfo;
    UpsertState* upsertState = state->mt_upsert;
    *updated = false;

    /*
     * get information on the (current) result relation
     */
    resultRelInfo = estate->es_result_relation_info;
    resultRelationDesc = resultRelInfo->ri_RelationDesc;
    heaprel = resultRelationDesc;

    ConstraintsForExecUpsert(resultRelationDesc);

    RangeTblEntry *rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex, estate);
    if (RelationIsPartitioned(resultRelationDesc)) {
        int partitionno = INVALID_PARTITION_NO;
        partitionid = heapTupleGetPartitionOid(resultRelationDesc, tuple, &partitionno, false, false, !partExprKeyStr);
        bool res = trySearchFakeReationForPartitionOid(&estate->esfRelations,
            estate->es_query_cxt,
            resultRelationDesc,
            partitionid,
            partitionno,
            &heaprel,
            &partition,
            RowExclusiveLock);
        if (!res) {
            return InvalidOid;
        }
        CheckPartitionOidForSpecifiedPartition(rte, partitionid);

        if (RelationIsSubPartitioned(resultRelationDesc)) {
            int subpartitionno = INVALID_PARTITION_NO;
            bool subpartExprKeyIsNull = PartExprKeyIsNull(heaprel, NULL);
            subPartitionId = heapTupleGetPartitionOid(heaprel, tuple, &subpartitionno, false, false, subpartExprKeyIsNull);
            searchFakeReationForPartitionOid(estate->esfRelations,
                estate->es_query_cxt,
                heaprel,
                subPartitionId,
                subpartitionno,
                subPartRel,
                subPart,
                RowExclusiveLock);
            CheckSubpartitionOidForSpecifiedSubpartition(rte, partitionid, subPartitionId);

            partitionid = subPartitionId;
            heaprel = subPartRel;
            partition = subPart;
        }

        *targetPartOid = partitionid;
    }

    vlock:
    targetrel = heaprel;
    if (RELATION_OWN_BUCKET(resultRelationDesc)) {
        bucketid = computeTupleBucketId(resultRelationDesc, (HeapTuple)tuple);
        if (unlikely(bucketid != InvalidBktId)) {
            searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, heaprel, bucketid, targetrel);
        }
    }

    specConflict = false;
    bool isgpi = false;
    Oid conflictPartOid = InvalidOid;
    int2 conflictBucketid = InvalidBktId;
    if (!ExecCheckIndexConstraints(slot, estate, targetrel, partition, &isgpi, bucketid, &conflictInfo,
                                   &conflictPartOid, &conflictBucketid)) {
        Partition part = NULL;
        Relation partition_relation = NULL;
        Relation bucketRel = NULL;
        if (isgpi) {
            part = partitionOpen(resultRelationDesc, conflictPartOid, RowExclusiveLock);
            if (RELATION_OWN_BUCKET(resultRelationDesc)) {
                bucketRel = bucketGetRelation(resultRelationDesc, part, conflictBucketid);
                targetrel = bucketRel;
            } else {
                partition_relation = partitionGetRelation(resultRelationDesc, part);
                targetrel = partition_relation;
            }
            *targetPartOid = conflictPartOid;
            bucketid = conflictBucketid;
        }
        /* committed conflict tuple found */
        if (upsertState->us_action == UPSERT_UPDATE) {
            CheckPartitionOidForUpsertSpecifiedPartition(rte, resultRelationDesc, *targetPartOid);
            /*
             * In case of DUPLICATE KEY UPDATE, execute the UPDATE part.
             * Be prepared to retry if the UPDATE fails because
             * of another concurrent UPDATE/DELETE to the conflict tuple.
             */
            *returning = NULL;

            if (ExecConflictUpdate(state, resultRelInfo, &conflictInfo, planSlot, slot,
                    estate, targetrel, *targetPartOid, bucketid, canSetTag, returning, partExprKeyStr)) {
                InstrCountFiltered2(&state->ps, 1);
                *updated = true;
                ReleaseResourcesForUpsertGPI(isgpi, resultRelationDesc, bucketRel, &partition_relation, part);
                return InvalidOid;
            } else {
                ReleaseResourcesForUpsertGPI(isgpi, resultRelationDesc, bucketRel, &partition_relation, part);
                goto vlock;
            }
        } else {
            /*
             * In case of DUPLICATE UPDATE NOTHING, do nothing.
             * However, verify that the tuple is visible to the
             * executor's MVCC snapshot at higher isolation levels.
             */
            Assert(upsertState->us_action == UPSERT_NOTHING);
            ExecCheckTIDVisible(targetrel, estate, targetrel, &conflictInfo.conflictTid);
            InstrCountFiltered2(&state->ps, 1);
            *updated = true;
            ReleaseResourcesForUpsertGPI(isgpi, resultRelationDesc, bucketRel, &partition_relation, part);
            return InvalidOid;
        }
    }

    /* insert the tuple */
    newid = tableam_tuple_insert(targetrel, tuple, estate->es_output_cid, 0, NULL);

    if (slot->tts_tuple != tuple) {
        UpdateSlotTupleInfo(slot, tuple);
    }

    /* insert index entries for tuple */
    ItemPointerData item = TUPLE_IS_UHEAP_TUPLE(tuple) ? ((UHeapTuple)tuple)->ctid : ((HeapTuple)tuple)->t_self;
    recheckIndexes = ExecInsertIndexTuples(slot, &item, estate, heaprel,
        partition, bucketid, &specConflict, NULL);

    /* other transaction commit index insertion before us,
     * then abort the tuple and try to find the conflict tuple again
     */
    if (specConflict) {
        /* delete index tuples and mark them as dead */
        ExecIndexTuplesState exec_index_tuples_state;
        exec_index_tuples_state.estate = estate;
        exec_index_tuples_state.targetPartRel = heaprel;
        exec_index_tuples_state.p = partition;
        exec_index_tuples_state.conflict = NULL;
        exec_index_tuples_state.rollbackIndex = true;

        UpsertAction oldAction = state->mt_upsert->us_action;
        state->mt_upsert->us_action = UPSERT_NONE;
        tableam_tops_exec_delete_index_tuples(slot, heaprel, state,
                                              &item, exec_index_tuples_state, NULL);
        state->mt_upsert->us_action = oldAction;

        /* rollback heap/uheap tuple */
        tableam_tuple_abort_speculative(targetrel, tuple);

        list_free(recheckIndexes);
        goto vlock;
    }

    /* try to insert tuple into mlog-table. */
    if (targetrel != NULL && targetrel->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        if (targetrel->rd_tam_ops == TableAmUstore) {
            tuple = (Tuple)UHeapToHeap(targetrel->rd_att, (UHeapTuple)tuple);
        }
        insert_into_mlog_table(targetrel, targetrel->rd_mlogoid, (HeapTuple)tuple,
                    &((HeapTuple)tuple)->t_self, GetCurrentTransactionId(), 'I');
    }

    return newid;
}

extern PartKeyExprResult ComputePartKeyExprTuple(Relation rel, EState *estate, TupleTableSlot *slot, Relation partRel, char* partExprKeyStr);
/* ----------------------------------------------------------------
 *		ExecInsert
 *
 *		For INSERT, we have to insert the tuple into the target relation
 *		and insert appropriate tuples into the index relations.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
template <bool useHeapMultiInsert>
TupleTableSlot* ExecInsertT(ModifyTableState* state, TupleTableSlot* slot, TupleTableSlot* planSlot, EState* estate,
    bool canSetTag, int options, List** partitionList, char* partExprKeyStr, bool replaceNull)
{
    Tuple tuple = NULL;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_relation_desc;
    Oid new_id = InvalidOid;
    List* recheck_indexes = NIL;
    Oid partition_id = InvalidOid;
    Partition partition = NULL;
    Relation heap_rel = NULL;
    Relation target_rel = NULL;
    CopyFromBulk bulk = NULL;
    ItemPointer pTSelf = NULL;
    uint64 res_hash = 0;
    bool rel_isblockchain = false;
    bool is_record = false;
    bool to_flush = false;
    int2 bucket_id = InvalidBktId;
    bool need_flush = enable_heap_bcm_data_replication();
#ifdef PGXC
    RemoteQueryState* result_remote_rel = NULL;
#endif

    /*
     * get information on the (current) result relation
     */
    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;
    rel_isblockchain = result_relation_desc->rd_isblockchain;
    /*
     * get the heap tuple out of the tuple table slot, making sure we have a
     * writable copy
     */
    tuple = tableam_tslot_get_tuple_from_slot(result_rel_info->ri_RelationDesc, slot);    

#ifdef ENABLE_MULTIPLE_NDOES
    result_remote_rel = (RemoteQueryState*)estate->es_result_remoterel;
#endif
    /*
     * If the result relation has OIDs, force the tuple's OID to zero so that
     * heap_insert will assign a fresh OID.  Usually the OID already will be
     * zero at this point, but there are corner cases where the plan tree can
     * return a tuple extracted literally from some table with the same
     * rowtype.
     *
     * XXX if we ever wanted to allow users to assign their own OIDs to new
     * rows, this'd be the place to do it.  For the moment, we make a point of
     * doing this before calling triggers, so that a user-supplied trigger
     * could hack the OID if desired.
     */
    if (result_relation_desc->rd_rel->relhasoids)
        HeapTupleSetOid((HeapTuple)tuple, InvalidOid);

    /*
     * if relation is in ledger scheam, add hash column for tuple.
     */
    if (rel_isblockchain) {
        MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        tuple = set_user_tuple_hash((HeapTuple)tuple, result_relation_desc, slot);
        (void)MemoryContextSwitchTo(old_context);
    }

    /* BEFORE ROW INSERT Triggers
     * Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an except
     * for a MERGE or INSERT ... ON DUPLICATE KEY UPDATE statement.
     */
    if (
#ifdef ENABLE_MULTIPLE_NODES	
        state->operation != CMD_MERGE && state->mt_upsert->us_action == UPSERT_NONE &&
#endif		
        result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_insert_before_row) {
        slot = ExecBRInsertTriggers(estate, result_rel_info, slot);
        if (slot == NULL) /* "do nothing" */
            return NULL;

        /* trigger might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_rel_info->ri_RelationDesc, slot);
    }

    /* INSTEAD OF ROW INSERT Triggers
     * Note: We fire INSREAD OF ROW TRIGGERS for every attempted insertion except
     * for a MERGE or INSERT ... ON DUPLICATE KEY UPDATE statement.
     */
    if (
#ifdef ENABLE_MULTIPLE_NODES	
        state->operation != CMD_MERGE && state->mt_upsert->us_action == UPSERT_NONE &&
#endif		
        result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_insert_instead_row) {
        slot = ExecIRInsertTriggers(estate, result_rel_info, slot);
        if (slot == NULL) /* "do nothing" */
            return NULL;

        /* trigger might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_rel_info->ri_RelationDesc, slot);

        new_id = InvalidOid;
    } else if (result_rel_info->ri_FdwRoutine) {
        /*
         * Compute stored generated columns
         */
        if (result_relation_desc->rd_att->constr && result_relation_desc->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(result_rel_info, estate, slot, tuple, CMD_INSERT);
            tuple = slot->tts_tuple;
        }

#ifdef ENABLE_MOT
        if (result_rel_info->ri_FdwRoutine->GetFdwType && result_rel_info->ri_FdwRoutine->GetFdwType() == MOT_ORC) {
            if (result_relation_desc->rd_att->constr) {
                TupleTableSlot *tmp_slot = state->mt_insert_constr_slot == NULL ? slot : state->mt_insert_constr_slot;
                if (!ExecConstraints(result_rel_info, tmp_slot, estate, false, replaceNull)) {
                    if (u_sess->utils_cxt.sql_ignore_strategy_val == SQL_OVERWRITE_NULL) {
                        tuple = ReplaceTupleNullCol(RelationGetDescr(result_relation_desc), tmp_slot);
                        /* Double check constraints in case that new val in column with not null constraints
                         * violated check constraints */
                        ExecConstraints(result_rel_info, tmp_slot, estate);
                    } else {
                        return NULL;
                    }
                }
            }
            CheckIndexDisableValid(result_rel_info, estate);
        }
#endif
        /*
         * insert into foreign table: let the FDW do it
         */
        slot = result_rel_info->ri_FdwRoutine->ExecForeignInsert(estate, result_rel_info, slot, planSlot);

        if (slot == NULL) {
            /* "do nothing" */
            return NULL;
        }

        /* FDW might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_rel_info->ri_RelationDesc, slot);

        new_id = InvalidOid;
    } else {
        /*
         * Compute stored generated columns
         */
        if (result_relation_desc->rd_att->constr && result_relation_desc->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(result_rel_info, estate, slot, tuple, CMD_INSERT);
            tuple = slot->tts_tuple;
        }
        /*
         * Check the constraints of the tuple
         */
        bool has_bucket = RELATION_OWN_BUCKET(result_relation_desc);
        if (has_bucket) {
            bucket_id = computeTupleBucketId(result_relation_desc, (HeapTuple)tuple);
        }
        if (result_relation_desc->rd_att->constr) {
            TupleTableSlot *tmp_slot = state->mt_insert_constr_slot == NULL ? slot : state->mt_insert_constr_slot;
            if (!ExecConstraints(result_rel_info, tmp_slot, estate, true, replaceNull)) {
                if (u_sess->utils_cxt.sql_ignore_strategy_val == SQL_OVERWRITE_NULL) {
                    tuple = ReplaceTupleNullCol(RelationGetDescr(result_relation_desc), tmp_slot);
                    /* Double check constraints in case that new val in column with not null constraints
                     * violated check constraints */
                    ExecConstraints(result_rel_info, tmp_slot, estate, true);
                } else {
                    return NULL;
                }
            }
            tuple = ExecAutoIncrement(result_relation_desc, estate, slot, tuple);
        }
        CheckIndexDisableValid(result_rel_info, estate);

#ifdef PGXC
        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            slot = ExecProcNodeDMLInXC(estate, planSlot, slot);
            /*
             * If target table uses WITH OIDS, this should be set to the Oid inserted
             * but Oids are not consistent among nodes in openGauss, so this is set to the
             * default value InvalidOid for the time being. It corrects at least tags for all
             * the other INSERT commands.
             */
            new_id = InvalidOid;
        } else
#endif
            if (useHeapMultiInsert) {
                TupleTableSlot* tmp_slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor, false, result_relation_desc->rd_tam_ops);

                bool is_partition_rel = result_relation_desc->rd_rel->parttype == PARTTYPE_PARTITIONED_RELATION;
                Oid targetOid = InvalidOid;
                if (is_partition_rel) {
                    if (RelationIsSubPartitioned(result_relation_desc)) {
                        targetOid = heapTupleGetSubPartitionOid(result_relation_desc, tuple, estate, slot);
                    } else {
                        targetOid = heapTupleGetPartitionOid(result_relation_desc, tuple, NULL, false, false, !partExprKeyStr);
                    }
                } else {
                    targetOid = RelationGetRelid(result_relation_desc);
                }
                bulk = findBulk(((DistInsertSelectState *)state)->mgr, targetOid, bucket_id, &to_flush);

                if (to_flush) {
                    if (is_partition_rel && need_flush) {
                        /* partition oid for sync */
                        CopyFromMemCxt tmpCopyFromMemCxt = bulk->memCxt;
                        for (int16 i = 0; i < tmpCopyFromMemCxt->nextBulk; i++) {
                            *partitionList = list_append_unique_oid(*partitionList, 
                                                                    tmpCopyFromMemCxt->chunk[i]->partOid);
                        }
                    }
                    CopyFromChunkInsert<true>(NULL, estate, bulk, ((DistInsertSelectState*)state)->mgr,
                        ((DistInsertSelectState*)state)->pcState, estate->es_output_cid,
                        options, result_rel_info, tmp_slot, ((DistInsertSelectState*)state)->bistate);
                }

                /* check and record insertion into user chain table */
                if (rel_isblockchain) {
                    MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                    is_record = hist_table_record_insert(result_relation_desc, (HeapTuple)tuple, &res_hash);
                    (void)MemoryContextSwitchTo(old_context);
                }

                tableam_tops_add_to_bulk_insert_select(result_relation_desc, bulk, tuple, true);

                if (isBulkFull(bulk)) {
                    if (is_partition_rel && need_flush) {
                        /* partition oid for sync */
                        CopyFromMemCxt tmpCopyFromMemCxt = bulk->memCxt;
                        for (int16 i = 0; i < tmpCopyFromMemCxt->nextBulk; i++) {
                            *partitionList = list_append_unique_oid(*partitionList, 
                                                                    tmpCopyFromMemCxt->chunk[i]->partOid);
                        }
                    }
                    CopyFromChunkInsert<true>(NULL, estate, bulk, ((DistInsertSelectState*)state)->mgr,
                        ((DistInsertSelectState*)state)->pcState, estate->es_output_cid, options,
                        result_rel_info, tmp_slot, ((DistInsertSelectState*)state)->bistate);
                }

                ExecDropSingleTupleTableSlot(tmp_slot);
            } else if (state->mt_upsert->us_action != UPSERT_NONE && result_rel_info->ri_NumIndices > 0) {
                TupleTableSlot* returning = NULL;
                bool updated = false;
                new_id = InvalidOid;
                tableam_tslot_getallattrs(slot);

                tuple = tableam_tops_form_tuple(slot->tts_tupleDescriptor,
                                                slot->tts_values, 
                                                slot->tts_isnull, 
                                                result_relation_desc->rd_tam_ops);
                if (rel_isblockchain) {
                    MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                    tuple = set_user_tuple_hash((HeapTuple)tuple, result_relation_desc, NULL);
                    (void)MemoryContextSwitchTo(old_context);
                }
                if (tuple != NULL) {
                    /*
                     * If the tuple is modified by set_user_tuple_hash, the tuple was
                     * allocated in per-tuple memory context, and therefore will be
                     * freed by ResetPerTupleExprContext with estate. The tuple table
                     * slot should not try to clear it.
                     */
                    ExecStoreTuple((Tuple)tuple, slot, InvalidBuffer, !rel_isblockchain);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                                errmsg("The tuple to be inserted into the table cannot be NULL")));
                }
                new_id =
                    ExecUpsert(state, slot, planSlot, estate, canSetTag, tuple, &returning, &updated, &partition_id, partExprKeyStr);
                if (updated) {
                    return returning;
                }

                if (rel_isblockchain) {
                    is_record = hist_table_record_insert(result_relation_desc, (HeapTuple)tuple, &res_hash);
                }
            } else {
                /*
                 * insert the tuple
                 *
                 * Note: heap_insert returns the tid (location) of the new tuple in
                 * the t_self field.
                 */
                new_id = InvalidOid;
                RangeTblEntry *rte = exec_rt_fetch(result_rel_info->ri_RangeTableIndex, estate);
                bool isgpi = false;
                ConflictInfoData conflictInfo;
                Oid conflictPartOid = InvalidOid;
                int2 conflictBucketid = InvalidBktId;

                switch (result_relation_desc->rd_rel->parttype) {
                    case PARTTYPE_NON_PARTITIONED_RELATION:
                    case PARTTYPE_VALUE_PARTITIONED_RELATION: {
                        if (RelationIsCUFormat(result_relation_desc)) {
                            HeapInsertCStore(result_relation_desc, estate->es_result_relation_info, (HeapTuple)tuple,
                                0);
                        } else {
                            target_rel = result_relation_desc;
                            if (bucket_id != InvalidBktId) {
                                searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt,
                                    result_relation_desc, bucket_id, target_rel);
                            }
                            // check unique constraints first if SQL has keyword IGNORE
                            if (estate->es_plannedstmt && estate->es_plannedstmt->hasIgnore &&
                                !ExecCheckIndexConstraints(slot, estate, target_rel, partition, &isgpi,
                                                           bucket_id, &conflictInfo, &conflictPartOid,
                                                           &conflictBucketid)) {
                                ereport(WARNING,
                                        (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                                RelationGetRelationName(target_rel))));
                                return NULL;
                            }
                            new_id = tableam_tuple_insert(target_rel, tuple, estate->es_output_cid, 0, NULL);

                            if (rel_isblockchain) {
                                MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                is_record = hist_table_record_insert(result_relation_desc, (HeapTuple)tuple, &res_hash);
                                (void)MemoryContextSwitchTo(old_context);
                            }
                        }
                    } break;

                    case PARTTYPE_PARTITIONED_RELATION: {
                        /* get partititon oid for insert the record */
                        int partitionno = INVALID_PARTITION_NO;
                        if (partExprKeyStr) {
                            PartKeyExprResult partKeyExprTuple = ComputePartKeyExprTuple(result_relation_desc, estate,
                                                                                         slot, NULL, partExprKeyStr);
                            partition_id = heapTupleGetPartitionOid(result_relation_desc, (void *) (&partKeyExprTuple),
                                                                    &partitionno,
                                                                    false, estate->es_plannedstmt->hasIgnore, false);
                        } else {
                            partition_id = heapTupleGetPartitionOid(result_relation_desc, tuple, &partitionno, false,
                                estate->es_plannedstmt->hasIgnore, true);
                        }
                        /* if cannot find valid partition oid and sql has keyword ignore, return and don't insert */
                        if (estate->es_plannedstmt->hasIgnore && partition_id == InvalidOid) {
                            return NULL;
                        }
                        bool partitionCheckPassed = CheckPartitionOidForSpecifiedPartition(
                            rte, partition_id, estate->es_plannedstmt->hasIgnore);
                        if (!partitionCheckPassed) {
                            return NULL;
                        }

                        bool res = trySearchFakeReationForPartitionOid(&estate->esfRelations, estate->es_query_cxt,
                            result_relation_desc, partition_id, partitionno, &heap_rel, &partition, RowExclusiveLock);
                        if (!res) {
                            return NULL;
                        }
                        if (RelationIsColStore(result_relation_desc)) {
                            HeapInsertCStore(heap_rel, estate->es_result_relation_info, (HeapTuple)tuple, 0);
#ifdef ENABLE_MULTIPLE_NODES
                        } else if (RelationIsTsStore(result_relation_desc)) {
                            HeapInsertTsStore(result_relation_desc,
                                estate->es_result_relation_info,
                                (HeapTuple)tuple, 0);
#endif   /* ENABLE_MULTIPLE_NODES */
                        } else {
                            target_rel = heap_rel;
                            /* check unique constraints first if SQL has keyword IGNORE */
                            if (estate->es_plannedstmt && estate->es_plannedstmt->hasIgnore &&
                                !ExecCheckIndexConstraints(slot, estate, target_rel, partition, &isgpi, bucket_id,
                                                           &conflictInfo, &conflictPartOid, &conflictBucketid)) {
                                ereport(WARNING,
                                        (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                                RelationGetRelationName(target_rel))));
                                return NULL;
                            }
                            tableam_tops_update_tuple_with_oid(heap_rel, tuple, slot);
                                if (bucket_id != InvalidBktId) {
                                    searchHBucketFakeRelation(
                                        estate->esfRelations, estate->es_query_cxt, heap_rel, bucket_id, target_rel);
                                }
                            new_id = tableam_tuple_insert(target_rel, tuple, estate->es_output_cid, 0, NULL);
                            if (rel_isblockchain) {
                                MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                is_record = hist_table_record_insert(target_rel, (HeapTuple)tuple, &res_hash);
                                (void)MemoryContextSwitchTo(old_context);
                            }
                        }
                    } break;
                    case PARTTYPE_SUBPARTITIONED_RELATION: {
                        Oid partitionId = InvalidOid;
                        Oid subPartitionId = InvalidOid;
                        int partitionno = INVALID_PARTITION_NO;
                        int subpartitionno = INVALID_PARTITION_NO;
                        Relation partRel = NULL;
                        Partition part = NULL;
                        Relation subPartRel = NULL;
                        Partition subPart = NULL;

                        /* get partition oid for insert the record */
                        if (partExprKeyStr) {
                            PartKeyExprResult partKeyExprTuple = ComputePartKeyExprTuple(result_relation_desc, estate,
                                                                                         slot, NULL, partExprKeyStr);
                            partitionId = heapTupleGetPartitionOid(result_relation_desc, (void *) (&partKeyExprTuple),
                                                                   &partitionno,
                                                                   false, estate->es_plannedstmt->hasIgnore, false);
                        } else {
                            partitionId = heapTupleGetPartitionOid(result_relation_desc, tuple, &partitionno, false,
                                estate->es_plannedstmt->hasIgnore, true);
                        }

                        if (estate->es_plannedstmt->hasIgnore && partitionId == InvalidOid) {
                            return NULL;
                        }
                        bool partitionCheckPassed =
                            CheckPartitionOidForSpecifiedPartition(rte, partitionId, estate->es_plannedstmt->hasIgnore);
                        if (!partitionCheckPassed) {
                            return NULL;
                        }

                        bool res = trySearchFakeReationForPartitionOid(&estate->esfRelations, estate->es_query_cxt,
                            result_relation_desc, partitionId, partitionno, &partRel, &part, RowExclusiveLock);
                        if (!res) {
                            return NULL;
                        }

                        /* get subpartititon oid for insert the record */
                        char* subpartExprKeyStr = NULL;
                        bool subpartExprKeyIsNull = PartExprKeyIsNull(partRel, &subpartExprKeyStr);
                        if (!subpartExprKeyIsNull) {
                            PartKeyExprResult partKeyExprTuple = ComputePartKeyExprTuple(result_relation_desc, estate, slot, partRel, subpartExprKeyStr);
                            subPartitionId = heapTupleGetPartitionOid(partRel, (void *)(&partKeyExprTuple), &subpartitionno, false,
                                estate->es_plannedstmt->hasIgnore, false);
                        } else {
                            subPartitionId = heapTupleGetPartitionOid(partRel, tuple, &subpartitionno, false,
                                estate->es_plannedstmt->hasIgnore, true);
                        }
                        if (estate->es_plannedstmt->hasIgnore && subPartitionId == InvalidOid) {
                            return NULL;
                        }
                        bool subpartitionCheckPassed = CheckSubpartitionOidForSpecifiedSubpartition(
                            rte, partitionId, subPartitionId, estate->es_plannedstmt->hasIgnore);
                        if (!subpartitionCheckPassed) {
                            return NULL;
                        }

                        res = trySearchFakeReationForPartitionOid(&estate->esfRelations, estate->es_query_cxt,
                            partRel, subPartitionId, subpartitionno, &subPartRel, &subPart, RowExclusiveLock);
                        if (!res) {
                            partitionClose(result_relation_desc, part, RowExclusiveLock);
                            return NULL;
                        }

                        partition_id = subPartitionId;
                        heap_rel = subPartRel;
                        partition = subPart;

                        target_rel = heap_rel;
                        // check unique constraints first if SQL has keyword IGNORE
                        if (estate->es_plannedstmt && estate->es_plannedstmt->hasIgnore &&
                            !ExecCheckIndexConstraints(slot, estate, target_rel, partition, &isgpi, bucket_id,
                                                       &conflictInfo, &conflictPartOid, &conflictBucketid)) {
                            ereport(WARNING,
                                    (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                            RelationGetRelationName(result_relation_desc))));
                            return NULL;
                        }
                        tableam_tops_update_tuple_with_oid(heap_rel, tuple, slot);
                        if (bucket_id != InvalidBktId) {
                            searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, heap_rel, bucket_id,
                                                      target_rel);
                        }
                        new_id = tableam_tuple_insert(target_rel, tuple, estate->es_output_cid, 0, NULL);
                        if (rel_isblockchain) {
                            MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                            is_record = hist_table_record_insert(target_rel, (HeapTuple)tuple, &res_hash);
                            (void)MemoryContextSwitchTo(old_context);
                        }
                    } break;

                    default: {
                        /* never happen; just to be self-contained */
                        ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                    errmsg("Unrecognized parttype as \"%c\" for relation \"%s\"",
                                        RelationGetPartType(result_relation_desc),
                                        RelationGetRelationName(result_relation_desc)))));
                    } break;
                }
                /*
                 * insert index entries for tuple
                 */
                pTSelf = tableam_tops_get_t_self(result_relation_desc, tuple);
                if (result_rel_info->ri_NumIndices > 0 && !RelationIsColStore(result_relation_desc)) {
                    if (state->isReplace) {
                        bool specConflict = false;
                        recheck_indexes = ExecInsertIndexTuples(slot,
                        pTSelf,
                        estate,
                        RELATION_IS_PARTITIONED(result_relation_desc) ? heap_rel : NULL,
                        RELATION_IS_PARTITIONED(result_relation_desc) ? partition : NULL,
                        bucket_id, &specConflict, NULL);
                        if (specConflict) {
                            state->isConflict = true;
                            tableam_tuple_abort_speculative(target_rel, tuple);
                            list_free(recheck_indexes);
                            return slot;
                        } else {
                            state->isConflict = false;
                        }
                    } else {
                        recheck_indexes = ExecInsertIndexTuples(slot,
                            pTSelf,
                            estate,
                            RELATION_IS_PARTITIONED(result_relation_desc) ? heap_rel : NULL,
                            RELATION_IS_PARTITIONED(result_relation_desc) ? partition : NULL,
                            bucket_id, NULL, NULL);
                    }
                } else {
                    state->isConflict = false;
                }

            }
    }
    if (pTSelf == NULL) {
        pTSelf = tableam_tops_get_t_self(result_relation_desc, tuple);
    }
    if (canSetTag) {
#ifdef PGXC
        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            estate->es_processed += result_remote_rel->rqs_processed;
        } else {
            if (is_record) {
                estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
            }
            (estate->es_processed)++;
        }
#else
        if (is_record) {
            estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
        }
        (estate->es_processed)++;
#endif
        estate->es_lastoid = new_id;
        setLastTid(pTSelf);
    }

    /* AFTER ROW INSERT Triggers
     * Note: We fire AFTER ROW TRIGGERS for every attempted insertion except
     * for a MERGE or INSERT ... ON DUPLICATE KEY UPDATE statement.
     * But in openGauss, we verify foreign key validity by AFTER ROW TRIGGERS,
     * so we can not fire it.
     */
    if (
#ifdef ENABLE_MULTIPLE_NODES
        state->operation != CMD_MERGE && state->mt_upsert->us_action == UPSERT_NONE &&
#endif
        !useHeapMultiInsert) {
        if (!state->mt_isSplitUpdates) {
            ExecARInsertTriggers(estate, result_rel_info, partition_id, bucket_id, (HeapTuple)tuple, recheck_indexes);
        }
    }

    /* try to insert tuple into mlog-table. */
    if (target_rel != NULL && target_rel->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        if (target_rel->rd_tam_ops == TableAmUstore) {
            tuple = (Tuple)UHeapToHeap(target_rel->rd_att, (UHeapTuple)tuple);
        }
        insert_into_mlog_table(target_rel, target_rel->rd_mlogoid, (HeapTuple)tuple,
                            &(((HeapTuple)tuple)->t_self), GetCurrentTransactionId(), 'I');
    }

    list_free_ext(recheck_indexes);

    /* Check any WITH CHECK OPTION constraints */
    if (result_rel_info->ri_WithCheckOptions != NIL)
        ExecWithCheckOptions(result_rel_info, slot, estate);

    /* Process RETURNING if present */
    if (result_rel_info->ri_projectReturning)
#ifdef PGXC
    {
        if (TupIsNull(slot))
            return NULL;
#endif
        return ExecProcessReturning(result_rel_info->ri_projectReturning, slot, planSlot);
#ifdef PGXC
    }
#endif

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *
 *		When deleting from a table, tupleid identifies the tuple to
 *		delete and oldtuple is NULL.  When deleting from a view,
 *		oldtuple is passed to the INSTEAD OF triggers and identifies
 *		what to delete, and tupleid is invalid.  When deleting from a
 *		foreign table, both tupleid and oldtuple are NULL; the FDW has
 *		to figure out which row to delete using data from the planSlot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecDelete(ItemPointer tupleid, Oid deletePartitionOid, int2 bucketid, HeapTupleHeader oldtuple,
    TupleTableSlot* planSlot, EPQState* epqstate, ModifyTableState* node, bool canSetTag)
{
    EState* estate = node->ps.state;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_relation_desc;
    TM_Result result;
    TM_FailureData tmfd;
    Partition partition = NULL;
    Relation fake_relation = NULL;
    Relation part_relation = NULL;
    uint64 res_hash = 0;
    uint64 hash_del = 0;
    bool is_record = false;
#ifdef PGXC
    RemoteQueryState* result_remote_rel = NULL;
#endif
    TupleTableSlot* slot = NULL;

    /*
     * get information on the (current) result relation
     */
    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;
#ifdef ENABLE_MULTIPLE_NODES
    result_remote_rel = (RemoteQueryState*)estate->es_result_remoterel;
#endif

    /* BEFORE ROW DELETE Triggers */
    if (result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_delete_before_row) {
        bool dodelete = false;

        dodelete = ExecBRDeleteTriggers(estate,
            epqstate,
            result_rel_info,
            deletePartitionOid,
            bucketid,
#ifdef PGXC
            oldtuple,
#endif
            tupleid);
        if (!dodelete && !node->isReplace) /* "do nothing" */
            return NULL;
    }

    /* INSTEAD OF ROW DELETE Triggers */
    if (result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_delete_instead_row) {
        HeapTupleData tuple;

        Assert(oldtuple != NULL);
        tuple.t_data = oldtuple;
        tuple.t_len = HeapTupleHeaderGetDatumLength(oldtuple);
        ItemPointerSetInvalid(&(tuple.t_self));
        tuple.t_tableOid = InvalidOid;
        tuple.t_bucketId = InvalidBktId;
#ifdef PGXC
        tuple.t_xc_node_id = 0;
#endif

        bool dodelete = ExecIRDeleteTriggers(estate, result_rel_info, &tuple);
        if (!dodelete) /* "do nothing" */
            return NULL;
    } else if (result_rel_info->ri_FdwRoutine) {
        /*
         * delete from foreign table: let the FDW do it
         *
         * We offer the trigger tuple slot as a place to store RETURNING data,
         * although the FDW can return some other slot if it wants.  Set up
         * the slot's tupdesc so the FDW doesn't need to do that for itself.
         */
        slot = estate->es_trig_tuple_slot;
        if (slot->tts_tupleDescriptor != RelationGetDescr(result_relation_desc))
            ExecSetSlotDescriptor(slot, RelationGetDescr(result_relation_desc));

        slot = result_rel_info->ri_FdwRoutine->ExecForeignDelete(estate, result_rel_info, slot, planSlot);

        if (slot == NULL) {
            /* "do nothing" */
            return NULL;
        }

        if (TTS_EMPTY(slot)) {
            (void)ExecStoreAllNullTuple(slot);
        }
    } else {
        /*
         * delete the tuple
         *
         * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
         * that the row to be deleted is visible to that snapshot, and throw a
         * can't-serialize error if not. This is a special-case behavior
         * needed for referential integrity updates in transaction-snapshot
         * mode transactions.
         */
        Assert(RELATION_HAS_BUCKET(result_relation_desc) == (bucketid != InvalidBktId));
    fake_relation = result_relation_desc;

ldelete:
        if (result_relation_desc->rd_att->constr)
            CheckDisableValidateConstr(result_rel_info);
        CheckIndexDisableValid(result_rel_info, estate);
#ifdef PGXC
        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            /* for merge into we have to provide the slot */
            slot = ExecProcNodeDMLInXC(estate, planSlot, NULL);
        } else {
#endif
            TupleTableSlot* oldslot = NULL;

            fake_relation = result_relation_desc;
            if (isPartitionedRelation(result_relation_desc->rd_rel)) {
                searchFakeReationForPartitionOid(estate->esfRelations,
                    estate->es_query_cxt,
                    result_relation_desc,
                    deletePartitionOid,
                    INVALID_PARTITION_NO,
                    part_relation,
                    partition,
                    RowExclusiveLock);
                fake_relation = part_relation;
            }

            if (RelationIsColStore(result_relation_desc)) {
                HeapDeleteCStore(fake_relation, tupleid, deletePartitionOid, estate->es_snapshot);
                goto end;
            }

            if (bucketid != InvalidBktId) {
                searchHBucketFakeRelation(
                    estate->esfRelations, estate->es_query_cxt, fake_relation, bucketid, fake_relation);
            }

            if (result_relation_desc->rd_isblockchain) {
                hash_del = get_user_tupleid_hash(fake_relation, tupleid);
            }

            result = tableam_tuple_delete(fake_relation,
                tupleid,
                estate->es_output_cid,
                estate->es_crosscheck_snapshot,
                estate->es_snapshot,
                true /* wait for commit */,
                &oldslot,
                &tmfd,
                node->isReplace);

            switch (result) {
                case TM_SelfUpdated:
                case TM_SelfModified:
                    if (tmfd.cmax != estate->es_output_cid)
                        ereport(ERROR,
                                (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                                 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

                    if (node->mt_isSplitUpdates) {
                        ereport(ERROR,
                                (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                                errmsg("multiple updates to a row by the same query is not allowed")));
                    }
                    return NULL;

                case TM_Ok: {
                    /* Record delete operator to history table */
                    if (result_relation_desc->rd_isblockchain) {
                        MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                        is_record = hist_table_record_delete(fake_relation, hash_del, &res_hash);
                        (void)MemoryContextSwitchTo(old_context);
                    }
                    /* Record deleted tupleid when target table is under cluster resizing */
                    if (RelationInClusterResizing(result_relation_desc) &&
                        !RelationInClusterResizingReadOnly(result_relation_desc)) {
                        ItemPointerData start_ctid;
                        ItemPointerData end_ctid;
                        RelationGetCtids(fake_relation, &start_ctid, &end_ctid);
                        if (ItemPointerCompare(tupleid, &end_ctid) <= 0) {
                            RecordDeletedTuple(RelationGetRelid(fake_relation), bucketid, tupleid, node->delete_delta_rel);
                        }
                    }

                    Bitmapset *modifiedIdxAttrs = NULL;
                    ExecIndexTuplesState exec_index_tuples_state;
                    exec_index_tuples_state.estate = estate;
                    exec_index_tuples_state.targetPartRel =
                        isPartitionedRelation(result_relation_desc->rd_rel) ? part_relation : NULL;
                    exec_index_tuples_state.p = isPartitionedRelation(result_relation_desc->rd_rel) ? partition : NULL;
                    exec_index_tuples_state.conflict = NULL;
                    exec_index_tuples_state.rollbackIndex = false;
                    tableam_tops_exec_delete_index_tuples(oldslot, fake_relation, node,
                        tupleid, exec_index_tuples_state, modifiedIdxAttrs);
                    if (oldslot) {
                        ExecDropSingleTupleTableSlot(oldslot);
                    }
                } break;

                case TM_Updated: {
                    /* just for pg_delta_xxxxxxxx in CSTORE schema */
                    if (!pg_strncasecmp("pg_delta", result_relation_desc->rd_rel->relname.data,
                        strlen("pg_delta")) &&
                        result_relation_desc->rd_rel->relnamespace == CSTORE_NAMESPACE) {
                        ereport(ERROR, (errmodule(MOD_EXECUTOR),
                            (errcode(ERRCODE_MODIFY_CONFLICTS), errmsg("delete conflict in delta table cstore.%s",
                                result_relation_desc->rd_rel->relname.data))));
                    }
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    // EvalPlanQual need to reinitialize child plan to do some recheck due to concurrent update,
                    // but we wrap the left tree of Stream node in backend thread. So the child plan cannot be
                    // reinitialized successful now.
                    //
                    if (IS_PGXC_DATANODE && u_sess->exec_cxt.under_stream_runtime &&
                        estate->es_plannedstmt->num_streams > 0) {
                        ereport(ERROR, (errcode(ERRCODE_STREAM_CONCURRENT_UPDATE),
                            errmsg("concurrent update under Stream mode is not yet supported")));
                    }
                    TupleTableSlot *epqslot = EvalPlanQual(estate, epqstate, fake_relation,
                        result_rel_info->ri_RangeTableIndex, LockTupleExclusive, &tmfd.ctid, tmfd.xmax,
                        result_relation_desc->rd_rel->relrowmovement);
                    if (!TupIsNull(epqslot)) {
                        *tupleid = tmfd.ctid;
                        goto ldelete;
                    }
                    /* Updated tuple not matched; nothing to do */
                    return NULL;
                }
                case TM_Deleted:
                    /* just for pg_delta_xxxxxxxx in CSTORE schema */
                    if (!pg_strncasecmp("pg_delta", result_relation_desc->rd_rel->relname.data,
                        strlen("pg_delta")) &&
                        result_relation_desc->rd_rel->relnamespace == CSTORE_NAMESPACE) {
                        ereport(ERROR, (errmodule(MOD_EXECUTOR),
                            (errcode(ERRCODE_MODIFY_CONFLICTS), errmsg("delete conflict in delta table cstore.%s",
                                result_relation_desc->rd_rel->relname.data))));
                    }
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    Assert(ItemPointerEquals(tupleid, &tmfd.ctid));
                    if (result_relation_desc->rd_rel->relrowmovement) {
                        /*
                         * when: tupleid,equal with &update_ctid
                         * case: current session delete confict with other session row movement update
                         *
                         * the may be a row movement update action which delete tuple from original
                         * partition and insert tuple to new partition or we can add lock on the tuple to
                         * be delete or updated to avoid throw exception
                         */
                        ereport(ERROR,
                            (errcode(ERRCODE_TRANSACTION_ROLLBACK), errmsg("partition table delete conflict"),
                            errdetail("disable row movement of table can avoid this conflict")));
                    }
                    /* tuple already deleted; nothing to do */
                    return NULL;
                default:
                    ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized heap_delete status: %u", result))));
                    return NULL;
            }

                /*
                 * Note: Normally one would think that we have to delete index tuples
                 * associated with the heap tuple now...
                 *
                 * ... but in openGauss, we have no need to do this because VACUUM will
                 * take care of it later.  We can't delete index tuples immediately
                 * anyway, since the tuple is still visible to other transactions.
                 */
#ifdef PGXC
        }
#endif
    }
end:;
    if (canSetTag) {
#ifdef PGXC
        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            estate->es_processed += result_remote_rel->rqs_processed;
        } else {
#endif
            if (is_record) {
                estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
            }
            (estate->es_processed)++;
#ifdef PGXC
        }
#endif
    }

#ifdef PGXC
    if (!node->mt_isSplitUpdates) {
        ExecARDeleteTriggers(estate, result_rel_info, deletePartitionOid, bucketid, oldtuple, tupleid);
    }
#else
    /* AFTER ROW DELETE Triggers */
    ExecARDeleteTriggers(estate, result_rel_info, deletePartitionOid, tupleid);
#endif

    /* delete tuple from mlog of matview */
    if (result_relation_desc != NULL && result_relation_desc->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        insert_into_mlog_table(result_relation_desc,
                    result_relation_desc->rd_mlogoid, NULL, tupleid, tmfd.xmin, 'D');
    }

    /* Process RETURNING if present */
#ifdef PGXC
    if (IS_PGXC_COORDINATOR && result_remote_rel != NULL && result_rel_info->ri_projectReturning != NULL) {
        if (TupIsNull(slot))
            return NULL;

        return ExecProcessReturning(result_rel_info->ri_projectReturning, slot, planSlot);
    } else
#endif
        if (result_rel_info->ri_projectReturning) {
            /*
             * We have to put the target tuple into a slot, which means first we
             * gotta fetch it.	We can use the trigger tuple slot.
             */
            TupleTableSlot* rslot = NULL;
            HeapTupleData del_tuple;
            Buffer del_buffer;

            union {
                HeapTupleHeaderData hdr;
                char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
            } tbuf;
            errno_t errorNo = EOK;
            errorNo = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
            securec_check(errorNo, "\0", "\0");
            if (result_rel_info->ri_FdwRoutine) {
                /* FDW must have provided a slot containing the deleted row */
                Assert(!TupIsNull(slot));
                del_buffer = InvalidBuffer;
            } else {
                slot = estate->es_trig_tuple_slot;
                if (slot->tts_tupleDescriptor != RelationGetDescr(result_relation_desc)) {
                    ExecSetSlotDescriptor(slot, RelationGetDescr(result_relation_desc));
                }
                slot->tts_tam_ops = result_relation_desc->rd_tam_ops;
                if (oldtuple != NULL) {
                    Assert(!TTS_TABLEAM_IS_USTORE(slot));
                    del_tuple.t_data = oldtuple;
                    del_tuple.t_len = HeapTupleHeaderGetDatumLength(oldtuple);
                    ItemPointerSetInvalid(&(del_tuple.t_self));
                    del_tuple.t_tableOid = InvalidOid;
                    del_tuple.t_bucketId = InvalidBktId;
#ifdef PGXC
                    del_tuple.t_xc_node_id = 0;
#endif
                    del_buffer = InvalidBuffer;
                    (void)ExecStoreTuple(&del_tuple, slot, InvalidBuffer, false);
                } else {
                    del_tuple.t_self = *tupleid;
                    del_tuple.t_data = &tbuf.hdr;
                    if (!TableFetchAndStore(fake_relation, SnapshotAny, &del_tuple, &del_buffer, false, false, slot,
                                            NULL)) {
                        ereport(ERROR, (errmodule(MOD_EXECUTOR),
                                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                         errmsg("failed to fetch deleted tuple for DELETE RETURNING"))));
                    }
                }
            }

            rslot = ExecProcessReturning(result_rel_info->ri_projectReturning, slot, planSlot);

            /*
             * Before releasing the target tuple again, make sure rslot has a
             * local copy of any pass-by-reference values.
             */
            ExecMaterializeSlot(rslot);

            (void)ExecClearTuple(slot);
            if (BufferIsValid(del_buffer)) {
                ReleaseBuffer(del_buffer);
            }
            return rslot;
        }

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecUpdate
 *
 *		note: we can't run UPDATE queries with transactions
 *		off because UPDATEs are actually INSERTs and our
 *		scan will mistakenly loop forever, updating the tuple
 *		it just inserted..	This should be fixed but until it
 *		is, we don't want to get stuck in an infinite loop
 *		which corrupts your database..
 *
 *		When updating a table, tupleid identifies the tuple to
 *		update and oldtuple is NULL.  When updating a view, oldtuple
 *		is passed to the INSTEAD OF triggers and identifies what to
 *		update, and tupleid is invalid.  When updating a foreign table,
 *		both tupleid and oldtuple are NULL; the FDW has to figure out
 *		which row to update using data from the planSlot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecUpdate(ItemPointer tupleid,
    Oid oldPartitionOid, /* when update a partitioned table , give a partitionOid to find the tuple */
    int2 bucketid, HeapTupleHeader oldtuple, TupleTableSlot* slot, TupleTableSlot* planSlot, EPQState* epqstate,
    ModifyTableState* node, bool canSetTag, bool partKeyUpdate, TM_Result* uresultp, char* partExprKeyStr, TM_FailureData* tmfdp)
{
    EState* estate = node->ps.state;
    Tuple tuple = NULL;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_relation_desc;
    TM_Result result;
    TM_FailureData tmfd;
    List* recheck_indexes = NIL;
    Partition partition = NULL;
    Relation fake_relation = NULL;
    Relation fake_part_rel = NULL;
    Relation parent_relation = NULL;
    Oid new_partId = InvalidOid;
    int partitionno = INVALID_PARTITION_NO;
    uint64 res_hash;
    uint64 hash_del = 0;
    bool is_record = false;
    /*
     * flag to tell whether violate unique constraint for update indices
     */
    bool isgpi = false;
    ConflictInfoData conflictInfo;
    Oid conflictPartOid = InvalidOid;
    int2 conflictBucketid = InvalidBktId;
    bool cross_partition = (partKeyUpdate || partExprKeyStr != NULL);
#ifdef PGXC
    RemoteQueryState* result_remote_rel = NULL;
#endif
    bool allow_update_self = (node->mt_upsert != NULL &&
        node->mt_upsert->us_action != UPSERT_NONE) ? true : false;

    /*
     * get information on the (current) result relation
     */
    result_rel_info = estate->es_result_relation_info;
    result_relation_desc = result_rel_info->ri_RelationDesc;

    /*
     * abort the operation if not running transactions
     */
    if (IsBootstrapProcessingMode()) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                (errcode(ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED), errmsg("cannot UPDATE during bootstrap"))));
    }

#ifdef ENABLE_MULTIPLE_NODES
    result_remote_rel = (RemoteQueryState*)estate->es_result_remoterel;

    /*
     * For remote tables, the plan slot does not have all NEW tuple values in
     * the plan slot. If oldtuple is supplied, we would also need a complete
     * NEW tuple. Currently for remote tables, triggers are the only case where
     * oldtuple is passed. Craft the NEW tuple using OLD tuple and updated
     * values from NEW tuple slot, and store the NEW tuple back into the NEW
     * tuple slot.
     */
    if (IS_PGXC_COORDINATOR && result_remote_rel != NULL && oldtuple != NULL)
        slot = fill_slot_with_oldvals(slot, oldtuple, GetUpdatedColumns(estate->es_result_relation_info, estate));
#endif

    /*
     * get the heap tuple out of the tuple table slot, making sure we have a
     * writable copy
     */
    /*
     * get the heap tuple out of the tuple table slot, making sure we have a
     * writable copy
     */
    bool allowInplaceUpdate = true;

    tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);
    if ((result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_update_after_row) ||
        result_rel_info->ri_RelationDesc->rd_mlogoid) {
        allowInplaceUpdate = false;
    }

    /* BEFORE ROW UPDATE Triggers */
    if (
#ifdef ENABLE_MULTIPLE_NODES
        node->operation != CMD_MERGE &&
#endif
        result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_update_before_row) {
#ifdef PGXC
        slot = ExecBRUpdateTriggers(estate, epqstate, result_rel_info, oldPartitionOid,
            bucketid, oldtuple, tupleid, slot, uresultp, tmfdp);
#else
        slot = ExecBRUpdateTriggers(estate, epqstate, result_rel_info, tupleid, slot, uresultp, tmfdp);
#endif

        if (slot == NULL) {
            /* "do nothing" */
            return NULL;
        }

        // tableam
        /* trigger might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);

        /* The partition key might have been updated by the trigger. */
        cross_partition = true;
    }

    /* INSTEAD OF ROW UPDATE Triggers */
    if (
#ifdef ENABLE_MULTIPLE_NODES
        node->operation != CMD_MERGE &&
#endif
        result_rel_info->ri_TrigDesc && result_rel_info->ri_TrigDesc->trig_update_instead_row) {
        HeapTupleData oldtup;

        Assert(oldtuple != NULL);
        oldtup.t_data = oldtuple;
        oldtup.t_len = HeapTupleHeaderGetDatumLength(oldtuple);
        ItemPointerSetInvalid(&(oldtup.t_self));
        oldtup.t_tableOid = InvalidOid;
        oldtup.t_bucketId = InvalidBktId;
#ifdef PGXC
        oldtup.t_xc_node_id = 0;
#endif

        slot = ExecIRUpdateTriggers(estate, result_rel_info, &oldtup, slot);

        if (slot == NULL) /* "do nothing" */
            return NULL;

        // tableam
        /* trigger might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);
    } else if (result_rel_info->ri_FdwRoutine) {
        /*
         * Compute stored generated columns
         */
        if (result_relation_desc->rd_att->constr && result_relation_desc->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(result_rel_info, estate, slot, tuple, CMD_UPDATE);
            tuple = slot->tts_tuple;
        }

        /*
         * update in foreign table: let the FDW do it
         */
#ifdef ENABLE_MOT
        if (result_rel_info->ri_FdwRoutine->GetFdwType && result_rel_info->ri_FdwRoutine->GetFdwType() == MOT_ORC) {
            if (result_relation_desc->rd_att->constr) {
                TupleTableSlot *tmp_slot = node->mt_insert_constr_slot == NULL ? slot : node->mt_insert_constr_slot;
                if (!ExecConstraints(result_rel_info, tmp_slot, estate, false, CheckPluginReplaceNull())) {
                    if (u_sess->utils_cxt.sql_ignore_strategy_val == SQL_OVERWRITE_NULL) {
                        tuple = ReplaceTupleNullCol(RelationGetDescr(result_relation_desc), tmp_slot);
                        /* Double check constraints in case that new val in column with not null constraints
                         * violated check constraints */
                        ExecConstraints(result_rel_info, tmp_slot, estate);
                    } else {
                        return NULL;
                    }
                }
            }
            CheckIndexDisableValid(result_rel_info, estate);
        }
#endif
        slot = result_rel_info->ri_FdwRoutine->ExecForeignUpdate(estate, result_rel_info, slot, planSlot);

        if (slot == NULL) {
            /* "do nothing" */
            return NULL;
        }

        /* FDW might have changed tuple */
        tuple = tableam_tslot_get_tuple_from_slot(result_relation_desc, slot);
    } else {
        bool update_indexes = false;
        LockTupleMode lockmode;

        /*
         * Check the constraints of the tuple
         *
         * If we generate a new candidate tuple after EvalPlanQual testing, we
         * must loop back here and recheck constraints.  (We don't need to
         * redo triggers, however.	If there are any BEFORE triggers then
         * trigger.c will have done heap_lock_tuple to lock the correct tuple,
         * so there's no need to do them again.)
         */
        Assert(RELATION_HAS_BUCKET(result_relation_desc) == (bucketid != InvalidBktId));
lreplace:

        /* acquire Form_pg_attrdef ad_on_update */
        if (result_relation_desc->rd_att->constr && result_relation_desc->rd_att->constr->has_on_update) {
            bool update_fix_result =  ExecComputeStoredUpdateExpr(result_rel_info, estate, slot, tuple, CMD_UPDATE, tupleid, oldPartitionOid, bucketid);
            if (!update_fix_result) {
                tuple = slot->tts_tuple;
                /* The partition key might have been updated by on update rule. */
                cross_partition = true;
            }
        }

        /*
         * Compute stored generated columns
         */
        if (result_relation_desc->rd_att->constr && result_relation_desc->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(result_rel_info, estate, slot, tuple, CMD_UPDATE);
            tuple = slot->tts_tuple;
            /* The partition key might have been updated by generated expr. */
            cross_partition = true;
        }

        if (result_relation_desc->rd_att->constr) {
            TupleTableSlot *tmp_slot = node->mt_update_constr_slot == NULL ? slot : node->mt_update_constr_slot;
            if (!ExecConstraints(result_rel_info, tmp_slot, estate, false, CheckPluginReplaceNull())) {
                if (u_sess->utils_cxt.sql_ignore_strategy_val == SQL_OVERWRITE_NULL) {
                    tuple = ReplaceTupleNullCol(RelationGetDescr(result_relation_desc), tmp_slot);
                    /* Double check constraints in case that new val in column with not null constraints
                     * violated check constraints */
                    ExecConstraints(result_rel_info, tmp_slot, estate);
                } else {
                    return NULL;
                }
            }
        }
        CheckIndexDisableValid(result_rel_info, estate);

#ifdef PGXC
        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            slot = ExecProcNodeDMLInXC(estate, planSlot, slot);
        } else {
#endif
            /* restore auto_increment value for multi-rows upsert-update */
            if (node->mt_upsert != NULL && node->mt_upsert->us_action != UPSERT_NONE &&
                RelHasAutoInc(result_relation_desc) &&
                estate->cur_insert_autoinc > 0) {
                RestoreAutoIncrement(result_relation_desc, estate, tuple);
                /* avoid set last_insert_id */
                if (estate->first_autoinc == estate->cur_insert_autoinc) {
                    estate->first_autoinc = 0;
                }
            }

            /*
             * replace the heap tuple
             *
             * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
             * that the row to be updated is visible to that snapshot, and throw a
             * can't-serialize error if not. This is a special-case behavior
             * needed for referential integrity updates in transaction-snapshot
             * mode transactions.
             */
            if (!RELATION_IS_PARTITIONED(result_relation_desc)) {
                /* for non partitioned table */
                    TupleTableSlot* oldslot = NULL;

                    /* for non partitioned table: Heap */
                    fake_relation = result_relation_desc;
                    if (bucketid != InvalidBktId) {
                        searchHBucketFakeRelation(
                            estate->esfRelations, estate->es_query_cxt, result_relation_desc, bucketid, fake_relation);
                        parent_relation = result_relation_desc;
                    }

                    /*
                     * check constraints first if SQL has keyword IGNORE
                     *
                     * Note: we need to exclude the case of UPSERT_UPDATE, so that upsert could be successfully finished.
                     */
                    if (node->mt_upsert->us_action != UPSERT_UPDATE && estate->es_plannedstmt &&
                        estate->es_plannedstmt->hasIgnore &&
                        !ExecCheckIndexConstraints(slot, estate, result_relation_desc, partition, &isgpi, bucketid,
                                                   &conflictInfo, &conflictPartOid, &conflictBucketid)) {
                        // check whether the conflicted info is the update tuple. If not, report warning and return.
                        if (!ItemPointerEquals(&conflictInfo.conflictTid, tupleid)) {
                            ereport(WARNING,
                                    (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                            RelationGetRelationName(result_relation_desc))));
                            return NULL;
                        }
                    }

                    /*
                     * If target relation is under blockchain schema,
                     * we should get the target tuple hash before updating.
                     * And then append new row hash for insertion.
                     */
                    if (result_relation_desc->rd_isblockchain) {
                        MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                        hash_del = get_user_tupleid_hash(fake_relation, tupleid);
                        tuple = set_user_tuple_hash((HeapTuple)tuple, fake_relation, slot);
                        (void)MemoryContextSwitchTo(old_context);
                    }

                    Bitmapset *modifiedIdxAttrs = NULL;
                    /* add para 2 for heap_update */
                    result = tableam_tuple_update(fake_relation, parent_relation, tupleid, tuple, estate->es_output_cid,
                        estate->es_crosscheck_snapshot, estate->es_snapshot, true, // wait for commit
                        &oldslot, &tmfd, &update_indexes, &modifiedIdxAttrs, allow_update_self,
                        allowInplaceUpdate, &lockmode);

                    /* Let the caller know about the status of this operation */
                    if (uresultp)
                        *uresultp = result;
                    if (tmfdp)
                        *tmfdp = tmfd;

                    switch (result) {
                        case TM_SelfUpdated:
                        case TM_SelfModified:
                            /* can not update one row more than once for merge into */
                            if (node->operation == CMD_MERGE && !MERGE_UPDATE_MULTI) {
                                ereport(ERROR,
                                    (errmodule(MOD_EXECUTOR),
                                        (errcode(ERRCODE_TOO_MANY_ROWS),
                                            errmsg("unable to get a stable set of rows in the source tables"))));
                            }

                            if (tmfd.cmax != estate->es_output_cid)
                                ereport(ERROR,
                                        (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                         errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                                         errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

                            /* already deleted by self; nothing to do */
                            return NULL;

                        case TM_Ok:
                            /* Record updating behevior into user chain table */
                            if (result_relation_desc->rd_isblockchain) {
                                MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                is_record =
                                    hist_table_record_update(fake_relation, (HeapTuple)tuple, hash_del, &res_hash);
                                (void)MemoryContextSwitchTo(old_context);
                            }
                            /* Record deleted tupleid when target table is under cluster resizing */
                            if (RelationInClusterResizing(result_relation_desc) &&
                                !RelationInClusterResizingReadOnly(result_relation_desc)) {
                                ItemPointerData start_ctid;
                                ItemPointerData end_ctid;
                                RelationGetCtids(fake_relation, &start_ctid, &end_ctid);
                                if (ItemPointerCompare(tupleid, &end_ctid) <= 0) {                            
                                    RecordDeletedTuple(RelationGetRelid(fake_relation), bucketid, 
                                        tupleid, node->delete_delta_rel);
                                }
                            }
                            break;

                        case TM_Updated: {
                            /* just for pg_delta_xxxxxxxx in CSTORE schema */
                            if (!pg_strncasecmp("pg_delta", result_relation_desc->rd_rel->relname.data, strlen("pg_delta")) &&
                                 result_relation_desc->rd_rel->relnamespace == CSTORE_NAMESPACE) {
                                ereport(ERROR,
                                    (errmodule(MOD_EXECUTOR),
                                        (errcode(ERRCODE_MODIFY_CONFLICTS),
                                            errmsg("update conflict in delta table cstore.%s",
                                                result_relation_desc->rd_rel->relname.data))));
                            }

                            if (IsolationUsesXactSnapshot())
                                ereport(ERROR,
                                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                        errmsg("could not serialize access due to concurrent update")));
                            if (!RelationIsUstoreFormat(fake_relation)) {
                                Assert(!ItemPointerEquals(tupleid, &tmfd.ctid));
                            }
                            // EvalPlanQual need to reinitialize child plan to do some recheck due to concurrent update,
                            // but we wrap the left tree of Stream node in backend thread. So the child plan cannot be
                            // reinitialized successful now.
                            //
                            if (IS_PGXC_DATANODE && u_sess->exec_cxt.under_stream_runtime &&
                                estate->es_plannedstmt->num_streams > 0) {
                                ereport(ERROR,
                                    (errcode(ERRCODE_STREAM_CONCURRENT_UPDATE),
                                        errmsg("concurrent update under Stream mode is not yet supported")));
                            }

                            /*
                             * Recheck the tuple using EPQ. For MERGE, we leave this
                             * to the caller (it must do additional rechecking, and
                             * might end up executing a different action entirely).
                             */
                            if (uresultp && estate->es_plannedstmt->commandType == CMD_MERGE)
                                return NULL;

                            TupleTableSlot *epq_slot = EvalPlanQual(estate, epqstate, fake_relation,
                                result_rel_info->ri_RangeTableIndex, lockmode, &tmfd.ctid, tmfd.xmax, false);
                            if (!TupIsNull(epq_slot)) {
                                *tupleid = tmfd.ctid;

                                slot = ExecFilterJunk(result_rel_info->ri_junkFilter, epq_slot);

                                tuple = tableam_tslot_get_tuple_from_slot(fake_relation, slot);
                                goto lreplace;
                            }

                            /* Updated tuple not matched; nothing to do */
                            return NULL;
                        }

                        case TM_Deleted:
                            /* just for pg_delta_xxxxxxxx in CSTORE schema */
                            if (!pg_strncasecmp("pg_delta", result_relation_desc->rd_rel->relname.data, strlen("pg_delta")) &&
                                 result_relation_desc->rd_rel->relnamespace == CSTORE_NAMESPACE) {
                                ereport(ERROR,
                                    (errmodule(MOD_EXECUTOR),
                                        (errcode(ERRCODE_MODIFY_CONFLICTS),
                                            errmsg("update conflict in delta table cstore.%s",
                                                result_relation_desc->rd_rel->relname.data))));
                            }
                        
                            if (IsolationUsesXactSnapshot())
                                ereport(ERROR,
                                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                        errmsg("could not serialize access due to concurrent update")));
                        
                            Assert(ItemPointerEquals(tupleid, &tmfd.ctid));

                            /* tuple already deleted; nothing to do */
                            return NULL;

                        default:
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                        errmsg("unrecognized heap_update status: %u", result))));
                            return NULL;
                    }

                    if (result_rel_info->ri_NumIndices > 0 && update_indexes)
                    {
                        ExecIndexTuplesState exec_index_tuples_state;
                        exec_index_tuples_state.estate = estate;
                        exec_index_tuples_state.targetPartRel = NULL;
                        exec_index_tuples_state.p = NULL;
                        exec_index_tuples_state.conflict = NULL;
                        exec_index_tuples_state.rollbackIndex = false;
                        recheck_indexes = tableam_tops_exec_update_index_tuples(slot, oldslot, fake_relation,
                            node, tuple, tupleid, exec_index_tuples_state, bucketid, modifiedIdxAttrs);
                    }

                    if (oldslot) {
                            ExecDropSingleTupleTableSlot(oldslot);
                    }
                    /* End of non-partitioned-table: Heap */
            } else {
                /* for partitioned table */
                bool row_movement = false;
                bool need_create_file = false;
                int seqNum = -1;
                bool can_ignore = estate->es_plannedstmt->hasIgnore;

                if (!cross_partition) {
                    row_movement = false;
                    new_partId = oldPartitionOid;
                } else {
                    if (partExprKeyStr) {
                        PartKeyExprResult partKeyExprResult = ComputePartKeyExprTuple(result_relation_desc, estate,
                                                                                      slot, NULL, partExprKeyStr);
                        partitionRoutingForTuple(result_relation_desc, (void*)&partKeyExprResult,
                                                 u_sess->exec_cxt.route, can_ignore, false);
                    } else {
                        partitionRoutingForTuple(result_relation_desc, tuple, u_sess->exec_cxt.route, can_ignore, true);
                    }

                    if (u_sess->exec_cxt.route->fileExist) {
                        new_partId = u_sess->exec_cxt.route->partitionId;
                        partitionno = GetCurrentPartitionNo(new_partId);
                        if (RelationIsSubPartitioned(result_relation_desc)) {
                            Partition part = PartitionOpenWithPartitionno(result_relation_desc, new_partId,
                                partitionno, RowExclusiveLock);
                            Relation partRel = partitionGetRelation(result_relation_desc, part);
                            char* subpartExprKeyStr = NULL;
                            bool subpartExprKeyIsNull = PartExprKeyIsNull(partRel, &subpartExprKeyStr);
                            if (!subpartExprKeyIsNull) {
                                PartKeyExprResult partKeyExprTuple = ComputePartKeyExprTuple(result_relation_desc,
                                                                                             estate, slot, partRel,
                                                                                             subpartExprKeyStr);
                                partitionRoutingForTuple(partRel, (void *) (&partKeyExprTuple), u_sess->exec_cxt.route,
                                                         can_ignore, false);
                            } else {
                                partitionRoutingForTuple(partRel, tuple, u_sess->exec_cxt.route, can_ignore, true);
                            }

                            if (u_sess->exec_cxt.route->fileExist) {
                                new_partId = u_sess->exec_cxt.route->partitionId;
                                partitionno = GetCurrentSubPartitionNo(new_partId);
                            } else {
                                int level = can_ignore ? WARNING : ERROR;
                                ereport(level, (errmodule(MOD_EXECUTOR),
                                                (errcode(ERRCODE_PARTITION_ERROR),
                                                 errmsg("fail to update partitioned table \"%s\"",
                                                        RelationGetRelationName(result_relation_desc)),
                                                 errdetail("new tuple does not map to any table partition"))));
                            }

                            releaseDummyRelation(&partRel);
                            partitionClose(result_relation_desc, part, NoLock);
                            if (!u_sess->exec_cxt.route->fileExist && can_ignore) {
                                return NULL;
                            }
                        }

                        if (oldPartitionOid == new_partId) {
                            row_movement = false;
                        } else if (result_relation_desc->rd_rel->relrowmovement) {
                            row_movement = true;
                        } else {
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                                        errmsg("fail to update partitioned table \"%s\"",
                                            RelationGetRelationName(result_relation_desc)),
                                        errdetail("disable row movement"))));
                        }
                        need_create_file = false;
                    } else { 
                        /* 
                         * a not exist interval partition 
                         * it can not be a range area 
                         */
                        if (u_sess->exec_cxt.route->partArea != PART_AREA_INTERVAL) {
                            if (can_ignore) {
                                ereport(WARNING, (errmsg("fail to update partitioned table \"%s\".new tuple does not "
                                                         "map to any table partition.",
                                                         RelationGetRelationName(result_relation_desc))));
                                return NULL;
                            }
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_PARTITION_ERROR),
                                        errmsg("fail to update partitioned table \"%s\"",
                                            RelationGetRelationName(result_relation_desc)),
                                        errdetail("new tuple does not map to any table partition"))));
                        }

                        if (result_relation_desc->rd_rel->relrowmovement) {
                            row_movement = true;
                            need_create_file = true;
                            seqNum = u_sess->exec_cxt.route->partSeq;
                        } else {
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                                        errmsg("fail to update partitioned table \"%s\"",
                                            RelationGetRelationName(result_relation_desc)),
                                        errdetail("disable row movement"))));
                        }
                    }
                }

                if (!row_movement) {
                    /* no row movement */
                    searchFakeReationForPartitionOid(estate->esfRelations,
                        estate->es_query_cxt,
                        result_relation_desc,
                        new_partId,
                        partitionno,
                        fake_part_rel,
                        partition,
                        RowExclusiveLock);

                    /*
                     * replace the heap tuple
                     *
                     * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
                     * that the row to be updated is visible to that snapshot, and throw a
                     * can't-serialize error if not. This is a special-case behavior
                     * needed for referential integrity updates in transaction-snapshot
                     * mode transactions.
                     */
                    fake_relation = fake_part_rel;

                        TupleTableSlot* oldslot = NULL;

                        /* partition table, no row movement, heap */
                        if (bucketid != InvalidBktId) {
                            searchHBucketFakeRelation(
                                estate->esfRelations, estate->es_query_cxt, fake_relation, bucketid, fake_relation);
                        }

                        /*
                         * check constraints first if SQL has keyword IGNORE
                         */
                        if (can_ignore &&
                            !ExecCheckIndexConstraints(slot, estate, fake_relation, partition, &isgpi, bucketid,
                                                       &conflictInfo, &conflictPartOid, &conflictBucketid)) {
                            // check whether the conflicted info is the update tuple. If not, report warning and return.
                            bool is_same_pointer = ItemPointerEquals(&conflictInfo.conflictTid, tupleid);
                            if (!is_same_pointer || oldPartitionOid != conflictPartOid) {
                                ereport(WARNING,
                                        (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                                RelationGetRelationName(result_relation_desc))));
                                return NULL;
                            }
                        }

                        if (result_relation_desc->rd_isblockchain) {
                            MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                            hash_del = get_user_tupleid_hash(fake_relation, tupleid);
                            tuple = set_user_tuple_hash((HeapTuple)tuple, fake_relation, slot);
                            (void)MemoryContextSwitchTo(old_context);
                        }

                        Bitmapset *modifiedIdxAttrs = NULL;
                        result = tableam_tuple_update(fake_relation,
                            result_relation_desc,
                            tupleid,
                            tuple,
                            estate->es_output_cid,
                            estate->es_crosscheck_snapshot,
                            estate->es_snapshot,
                            true /* wait for commit */,
                            &oldslot,
                            &tmfd,
                            &update_indexes,
                            &modifiedIdxAttrs,
                            allow_update_self,
                            allowInplaceUpdate,
                            &lockmode);

                        /* Let the caller know about the status of this operation */
                        if (uresultp)
                            *uresultp = result;
                        if (tmfdp)
                            *tmfdp = tmfd;

                        switch (result) {
                            case TM_SelfUpdated:
                            case TM_SelfModified:
                                /* can not update one row more than once for merge into */
                                if (node->operation == CMD_MERGE && !MERGE_UPDATE_MULTI) {
                                    ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_TOO_MANY_ROWS),
                                        errmsg("unable to get a stable set of rows in the source tables"))));
                                }

                                if (tmfd.cmax != estate->es_output_cid)
                                    ereport(ERROR, (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                        errmsg("tuple to be updated was already modified by an operation triggered by "
                                               "the current command"),
                                        errhint("Consider using an AFTER trigger instead of a BEFORE trigger to "
                                                "propagate changes to other rows.")));

                                /* already deleted by self; nothing to do */
                                return NULL;

                            case TM_Ok:
                                /* Record updating behevior into user chain table */
                                if (result_relation_desc->rd_isblockchain) {
                                    MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                    is_record =
                                        hist_table_record_update(fake_relation, (HeapTuple)tuple, hash_del, &res_hash);
                                    (void)MemoryContextSwitchTo(old_context);
                                }
                                /* Record deleted tupleid when target table is under cluster resizing */
                                if (RelationInClusterResizing(result_relation_desc) &&
                                    !RelationInClusterResizingReadOnly(result_relation_desc)) {
                                    ItemPointerData start_ctid;
                                    ItemPointerData end_ctid;
                                    RelationGetCtids(fake_relation, &start_ctid, &end_ctid);
                                    if (ItemPointerCompare(tupleid, &end_ctid) <= 0) {
                                        RecordDeletedTuple(RelationGetRelid(fake_relation), bucketid, tupleid,
                                            node->delete_delta_rel);
                                    }
                                }
                                break;

                            case TM_Updated: {
                                if (IsolationUsesXactSnapshot())
                                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                        errmsg("could not serialize access due to concurrent update")));
                                if (!RelationIsUstoreFormat(fake_relation)) {
                                    Assert(!ItemPointerEquals(tupleid, &tmfd.ctid));
                                }
                                // EvalPlanQual need to reinitialize child plan to do some recheck due to concurrent
                                // update, but we wrap the left tree of Stream node in backend thread. So the child plan
                                // cannot be reinitialized successful now.
                                //
                                if (IS_PGXC_DATANODE && u_sess->exec_cxt.under_stream_runtime &&
                                    estate->es_plannedstmt->num_streams > 0) {
                                    ereport(ERROR, (errcode(ERRCODE_STREAM_CONCURRENT_UPDATE),
                                        errmsg("concurrent update under Stream mode is not yet supported")));
                                }


                                /*
                                 * Recheck the tuple using EPQ. For MERGE, we leave this
                                 * to the caller (it must do additional rechecking, and
                                 * might end up executing a different action entirely).
                                 */
                                if (uresultp && estate->es_plannedstmt->commandType == CMD_MERGE)
                                    return NULL;

                                TupleTableSlot *epq_slot = EvalPlanQual(estate, epqstate, fake_relation,
                                    result_rel_info->ri_RangeTableIndex, lockmode, &tmfd.ctid, tmfd.xmax,
                                    result_relation_desc->rd_rel->relrowmovement);

                                if (!TupIsNull(epq_slot)) {
                                    *tupleid = tmfd.ctid;

                                    slot = ExecFilterJunk(result_rel_info->ri_junkFilter, epq_slot);

                                    tuple = tableam_tslot_get_tuple_from_slot(fake_relation, slot);
                                    goto lreplace;
                                }

                                /* Updated tuple not matched; nothing to do */
                                return NULL;
                            }

                            case TM_Deleted:
                                if (IsolationUsesXactSnapshot())
                                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                        errmsg("could not serialize access due to concurrent update")));
                                Assert(ItemPointerEquals(tupleid, &tmfd.ctid));

                                if (result_relation_desc->rd_rel->relrowmovement) {
                                    /*
                                     * the may be a row movement update action which delete tuple from original
                                     * partition and insert tuple to new partition or we can add lock on the tuple to
                                     * be delete or updated to avoid throw exception
                                     */
                                    ereport(ERROR, (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                                        errmsg("partition table update conflict"),
                                        errdetail("disable row movement of table can avoid this conflict")));
                                }

                                /* tuple already deleted; nothing to do */
                                return NULL;

                            default:
                                ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                    errmsg("unrecognized heap_update status: %u", result))));
                        }

                        if (result_rel_info->ri_NumIndices > 0 && update_indexes)
                        {
                            ExecIndexTuplesState exec_index_tuples_state;
                            exec_index_tuples_state.estate = estate;
                            exec_index_tuples_state.targetPartRel = fake_part_rel;
                            exec_index_tuples_state.p = partition;
                            exec_index_tuples_state.conflict = NULL;
                            exec_index_tuples_state.rollbackIndex = false;
                            recheck_indexes = tableam_tops_exec_update_index_tuples(slot, oldslot, fake_relation,
                                node, tuple, tupleid, exec_index_tuples_state, bucketid, modifiedIdxAttrs);
                        }

                        if (oldslot) {
                                ExecDropSingleTupleTableSlot(oldslot);
                        }
                } else {
                    /* partition table, row movement */
                        /* partition table, row movement, heap */

                        /* init old_partition vars and new inserted partition vars first */
                        Partition old_partition = NULL;
                        Relation old_fake_relation = NULL;
                        TupleTableSlot* oldslot = NULL;
                        uint64 hash_del = 0;

                        searchFakeReationForPartitionOid(estate->esfRelations,
                                                         estate->es_query_cxt,
                                                         result_relation_desc,
                                                         oldPartitionOid,
                                                         partitionno,
                                                         old_fake_relation,
                                                         old_partition,
                                                         RowExclusiveLock);

                        if (bucketid != InvalidBktId) {
                            searchHBucketFakeRelation(
                                estate->esfRelations, estate->es_query_cxt, old_fake_relation, bucketid, old_fake_relation);
                        }

                        Partition insert_partition = NULL;
                        Relation fake_insert_relation = NULL;

                        if (need_create_file) {
                            new_partId = AddNewIntervalPartition(result_relation_desc, tuple, &partitionno);
                        }

                        searchFakeReationForPartitionOid(estate->esfRelations,
                                                         estate->es_query_cxt,
                                                         result_relation_desc,
                                                         new_partId,
                                                         partitionno,
                                                         fake_part_rel,
                                                         insert_partition,
                                                         RowExclusiveLock);
                        fake_insert_relation = fake_part_rel;
                        if (bucketid != InvalidBktId) {
                            searchHBucketFakeRelation(estate->esfRelations,
                                                      estate->es_query_cxt,
                                                      fake_insert_relation,
                                                      bucketid,
                                                      fake_insert_relation);
                        }

                        /*
                         * check constraints first if SQL has keyword IGNORE
                         */
                        if (can_ignore &&
                            !ExecCheckIndexConstraints(slot, estate, fake_insert_relation, insert_partition, &isgpi,
                                                       bucketid, &conflictInfo, &conflictPartOid, &conflictBucketid)) {
                            // check whether the conflicted info is the update tuple. If not, report warning and return.
                            bool is_same_pointer = ItemPointerEquals(&conflictInfo.conflictTid, tupleid);
                            if (!is_same_pointer || oldPartitionOid != conflictPartOid) {
                                ereport(WARNING,
                                        (errmsg("duplicate key value violates unique constraint in table \"%s\"",
                                                RelationGetRelationName(result_relation_desc))));
                                return NULL;
                            }
                        }

                        /* delete the old tuple */
                        {
ldelete:
                            /* Record updating behevior into user chain table */
                            if (result_relation_desc->rd_isblockchain) {
                                hash_del = get_user_tupleid_hash(old_fake_relation, tupleid);
                            }

                            result = tableam_tuple_delete(old_fake_relation,
                                tupleid,
                                estate->es_output_cid,
                                estate->es_crosscheck_snapshot,
                                estate->es_snapshot,
                                true, /* wait for commit */
                                &oldslot,
                                &tmfd,
                                allow_update_self);

                            /* Let the caller know about the status of this operation */
                            if (uresultp)
                                *uresultp = result;
                            if (tmfdp)
                                *tmfdp = tmfd;

                            switch (result) {
                                case TM_SelfUpdated:
                                case TM_SelfModified:
                                    /* can not update one row more than once for merge into */
                                    if (node->operation == CMD_MERGE && !MERGE_UPDATE_MULTI) {
                                        ereport(ERROR, (errmodule(MOD_EXECUTOR), (errcode(ERRCODE_TOO_MANY_ROWS),
                                            errmsg("unable to get a stable set of rows in the source tables"))));
                                    }

                                    if (tmfd.cmax != estate->es_output_cid)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                                 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                                                 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                                    return NULL;

                                case TM_Ok: {
                                    if (result_relation_desc->rd_isblockchain) {
                                        MemoryContext old_context =
                                            MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                        is_record = hist_table_record_delete(old_fake_relation, hash_del, &res_hash);
                                        (void)MemoryContextSwitchTo(old_context);
                                    }
                                    /* Record deleted tupleid when target table is under cluster resizing */
                                    if (RelationInClusterResizing(result_relation_desc) &&
                                        !RelationInClusterResizingReadOnly(result_relation_desc)) {
                                        ItemPointerData start_ctid;
                                        ItemPointerData end_ctid;
                                        RelationGetCtids(old_fake_relation, &start_ctid, &end_ctid);
                                        if (ItemPointerCompare(tupleid, &end_ctid) <= 0) {           
                                            RecordDeletedTuple(
                                                RelationGetRelid(old_fake_relation), bucketid, tupleid, node->delete_delta_rel);
                                        }	
                                    }

                                    Bitmapset *modifiedIdxAttrs = NULL;
                                    ExecIndexTuplesState exec_index_tuples_state;
                                    exec_index_tuples_state.estate = estate;
                                    exec_index_tuples_state.targetPartRel = old_fake_relation;
                                    exec_index_tuples_state.p = old_partition;
                                    exec_index_tuples_state.conflict = NULL;
                                    exec_index_tuples_state.rollbackIndex = false;
                                    tableam_tops_exec_delete_index_tuples(oldslot, old_fake_relation, node,
                                                tupleid, exec_index_tuples_state, modifiedIdxAttrs);
                                    if (oldslot) {
                                        ExecDropSingleTupleTableSlot(oldslot);
                                    }

                                    break;
                                }

                                case TM_Updated: {
                                    if (IsolationUsesXactSnapshot())
                                        ereport(ERROR,
                                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                                errmsg("could not serialize access due to concurrent update")));
                                    if (!RelationIsUstoreFormat(old_fake_relation)) {
                                        Assert(!ItemPointerEquals(tupleid, &tmfd.ctid));
                                    }
                                    if (result_relation_desc->rd_rel->relrowmovement) {
                                        /*
                                         * The part key value may has been updated,
                                         * don't know which partition we should deal with.
                                         */
                                        ereport(ERROR,
                                            (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                                                errmsg("partition table update conflict"),
                                                errdetail("disable row movement of table can avoid this conflict")));
                                    }

                                    // EvalPlanQual need to reinitialize child plan to do some recheck due to concurrent
                                    // update, but we wrap the left tree of Stream node in backend thread. So the child
                                    // plan cannot be reinitialized successful now.
                                    //
                                    if (IS_PGXC_DATANODE && u_sess->exec_cxt.under_stream_runtime &&
                                        estate->es_plannedstmt->num_streams > 0) {
                                        ereport(ERROR,
                                            (errcode(ERRCODE_STREAM_CONCURRENT_UPDATE),
                                                errmsg("concurrent update under Stream mode is not yet supported")));
                                    }

                                    /*
                                     * Recheck the tuple using EPQ. For MERGE, we leave this
                                     * to the caller (it must do additional rechecking, and
                                     * might end up executing a different action entirely).
                                     */
                                    if (uresultp && estate->es_plannedstmt->commandType == CMD_MERGE)
                                        return NULL;

                                    TupleTableSlot* epq_slot = EvalPlanQual(estate,
                                        epqstate,
                                        old_fake_relation,
                                        result_rel_info->ri_RangeTableIndex,
                                        LockTupleExclusive,
                                        &tmfd.ctid,
                                        tmfd.xmax,
                                        result_relation_desc->rd_rel->relrowmovement);
                                    /* Try to fetch latest tuple values in row movement case */
                                    if (!TupIsNull(epq_slot)) {
                                        *tupleid = tmfd.ctid;
                                        slot = ExecFilterJunk(result_rel_info->ri_junkFilter, epq_slot);

                                        tuple = tableam_tslot_get_tuple_from_slot(old_fake_relation, slot);
                                        goto ldelete;
                                    }

                                    /* Updated tuple not matched; nothing to do */
                                    return NULL;
                                }

                                case TM_Deleted:
                                    if (IsolationUsesXactSnapshot())
                                        ereport(ERROR,
                                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                                errmsg("could not serialize access due to concurrent update")));

                                    Assert(ItemPointerEquals(tupleid, &tmfd.ctid));
                                    if (result_relation_desc->rd_rel->relrowmovement) {
                                        /*
                                         * the may be a row movement update action which delete tuple from original
                                         * partition and insert tuple to new partition or we can add lock on the tuple
                                         * to be delete or updated to avoid throw exception
                                         */
                                        ereport(ERROR,
                                            (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                                                errmsg("partition table update conflict"),
                                                errdetail("disable row movement of table can avoid this conflict")));
                                    }

                                    /* tuple already deleted; nothing to do */
                                    return NULL;

                                default:
                                    ereport(ERROR,
                                        (errmodule(MOD_EXECUTOR),
                                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                                errmsg("unrecognized heap_delete status: %u", result))));
                            }

                            /*
                             * Note: Normally one would think that we have to delete index tuples
                             * associated with the heap tuple now...
                             *
                             * ... but in openGauss, we have no need to do this because VACUUM will
                             * take care of it later.  We can't delete index tuples immediately
                             * anyway, since the tuple is still visible to other transactions.
                             */
                        }

                        /* insert the new tuple */
                        {
                            Partition insert_partition = NULL;
                            Relation fake_insert_relation = NULL;
                            if (need_create_file) {
                                new_partId = AddNewIntervalPartition(result_relation_desc, tuple, &partitionno);
                            }

                            bool res = trySearchFakeReationForPartitionOid(&estate->esfRelations,
                                estate->es_query_cxt,
                                result_relation_desc,
                                new_partId,
                                partitionno,
                                &fake_part_rel,
                                &insert_partition,
                                RowExclusiveLock);
                            if (!res) {
                                return NULL;
                            }

                            fake_insert_relation = fake_part_rel;
                            if (bucketid != InvalidBktId) {
                                searchHBucketFakeRelation(estate->esfRelations,
                                    estate->es_query_cxt,
                                    fake_insert_relation,
                                    bucketid,
                                    fake_insert_relation);
                            }
                            if (result_relation_desc->rd_isblockchain) {
                                MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                tuple = set_user_tuple_hash((HeapTuple)tuple, fake_insert_relation, slot);
                                (void)MemoryContextSwitchTo(old_context);
                            }

                            (void)tableam_tuple_insert(fake_insert_relation, 
                                tuple, estate->es_output_cid, 0, NULL);

                            if (result_rel_info->ri_NumIndices > 0) {
                                recheck_indexes = ExecInsertIndexTuples(slot, &(((HeapTuple)tuple)->t_self), estate,
                                    fake_part_rel, insert_partition, bucketid, NULL, NULL);
                            }
                            if (result_relation_desc->rd_isblockchain) {
                                MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
                                uint64 delete_hash = res_hash;
                                is_record = hist_table_record_insert(fake_insert_relation, (HeapTuple)tuple, &res_hash);
                                res_hash += delete_hash;
                                (void)MemoryContextSwitchTo(old_context);
                            }
                    }
                    /* End of: partition table, row movement */
                }
                /* End of: partitioned table */
            }
#ifdef PGXC
            /* End of DN: replace the heap tuple */
        }
#endif
    }

    /* update tuple from mlog of matview(delete + insert). */
    if (result_relation_desc != NULL && result_relation_desc->rd_mlogoid != InvalidOid) {
        /* judge whether need to insert into mlog-table */
        /* 1. delete one tuple. */
        insert_into_mlog_table(result_relation_desc, result_relation_desc->rd_mlogoid,
                               NULL, tupleid, tmfd.xmin, 'D');
        /* 2. insert new tuple */
        if (result_relation_desc->rd_tam_ops == TableAmUstore) {
            tuple = (Tuple)UHeapToHeap(result_relation_desc->rd_att, (UHeapTuple)tuple);
        }
        insert_into_mlog_table(result_relation_desc, result_relation_desc->rd_mlogoid,
                               (HeapTuple)tuple, &(((HeapTuple)tuple)->t_self), GetCurrentTransactionId(), 'I');
    }

    if (canSetTag) {
#ifdef PGXC

        if (IS_PGXC_COORDINATOR && result_remote_rel) {
            estate->es_processed += result_remote_rel->rqs_processed;
        } else {
#endif
            if (is_record) {
                estate->es_modifiedRowHash = lappend(estate->es_modifiedRowHash, (void *)UInt64GetDatum(res_hash));
            }
            (estate->es_processed)++;
#ifdef PGXC
        }
#endif
    }

    /* AFTER ROW UPDATE Triggers */
#ifdef ENABLE_MULTIPLE_NODES
    if (node->operation != CMD_MERGE)
#endif
        ExecARUpdateTriggers(estate,
            result_rel_info,
            oldPartitionOid,
            bucketid,
            new_partId,
            tupleid,
            (HeapTuple)tuple,
#ifdef PGXC
            oldtuple,
#endif
            recheck_indexes);

    list_free_ext(recheck_indexes);

    /* Check any WITH CHECK OPTION constraints */
    if (result_rel_info->ri_WithCheckOptions != NIL)
        ExecWithCheckOptions(result_rel_info, slot, estate);

    /* Process RETURNING if present */
    if (result_rel_info->ri_projectReturning)
#ifdef PGXC
    {
        if (TupIsNull(slot))
            return NULL;
#endif
        return ExecProcessReturning(result_rel_info->ri_projectReturning, slot, planSlot);
#ifdef PGXC
    }
#endif

    return NULL;
}

/*
 * Process BEFORE EACH STATEMENT triggers
 */
static void fireBSTriggers(ModifyTableState* node)
{
    ResultRelInfo* resultRelInfo = node->resultRelInfo;
    int resultRelationNum = node->mt_ResultTupleSlots ? list_length(node->mt_ResultTupleSlots) : 1;
    switch (node->operation) {
        case CMD_INSERT:
            ExecBSInsertTriggers(node->ps.state, resultRelInfo);
            if (node->mt_upsert->us_action == UPSERT_UPDATE) {
                ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
            }
            break;
        case CMD_UPDATE:
            for (int i = 0; i < resultRelationNum; i++) {
                ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
                resultRelInfo++;
            }
            break;
        case CMD_DELETE:
            for (int i = 0; i < resultRelationNum; i++) {
                ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
                resultRelInfo++;
            }
            break;
        case CMD_MERGE:
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unknown operation %d when process BEFORE EACH STATEMENT triggers", node->operation))));

            break;
    }
}

/*
 * Process AFTER EACH STATEMENT triggers
 */
static void fireASTriggers(ModifyTableState* node)
{
    ResultRelInfo* resultRelInfo = node->resultRelInfo;
    int resultRelationNum = node->mt_ResultTupleSlots ? list_length(node->mt_ResultTupleSlots) : 1;
    switch (node->operation) {
        case CMD_INSERT:
            if (node->mt_upsert->us_action == UPSERT_UPDATE) {
                ExecASUpdateTriggers(node->ps.state, resultRelInfo);
            }
            ExecASInsertTriggers(node->ps.state, resultRelInfo);
            break;
        case CMD_UPDATE:
            for (int i = 0; i < resultRelationNum; i++) {
                ExecASUpdateTriggers(node->ps.state, resultRelInfo);
                resultRelInfo++;
            }
            break;
        case CMD_DELETE:
            for (int i = 0; i < resultRelationNum; i++) {
                ExecASDeleteTriggers(node->ps.state, resultRelInfo);
                resultRelInfo++;
            }
            break;
        case CMD_MERGE:
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unknown operation %d when process AFTER EACH STATEMENT triggers", node->operation))));
            break;
    }
}

/*
 * Check limit plan can be changed for delete limit
 */
bool IsLimitDML(const Limit *limitPlan)
{
    if (limitPlan->limitCount == NULL) {
        return false;
    }
    if (limitPlan->limitOffset != NULL && IsA(limitPlan->limitOffset, Const)) {
        const Const *flag = (Const*)limitPlan->limitOffset;
        if (flag->ismaxvalue) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}

/*
 * Get limit boundary
 */
uint64 GetDeleteLimitCount(ExprContext* econtext, PlanState* scan, Limit *limitPlan)
{
    ExprState* limitExpr = ExecInitExpr((Expr*)limitPlan->limitCount, scan);
    Datum val;
    bool isNull = false;
    int64 iCount = 0;
    val = ExecEvalExprSwitchContext(limitExpr, econtext, &isNull, NULL);
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                errmodule(MOD_EXECUTOR),
                errmsg("LIMIT must not be null for delete.")));
    }
    iCount = DatumGetInt64(val);
    if (iCount <= 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                errmodule(MOD_EXECUTOR),
                errmsg("LIMIT must not be less than 0 for delete.")));
    }
    return (uint64)iCount;
}

static TupleTableSlot* ExecReplace(EState* estate, ModifyTableState* node, TupleTableSlot* slot,
    TupleTableSlot* plan_slot, int2 bucketid, int hi_options, List* partition_list, char* partExprKeyStr,
    bool replaceNull)
{
    Form_pg_attribute att_tup;
    Oid seqOid = InvalidOid;
    Relation rel = estate->es_result_relation_info->ri_RelationDesc;
    TupleTableSlot* (*ExecInsert)(
        ModifyTableState* state, TupleTableSlot*, TupleTableSlot*, EState*, bool, int, List**, char*, bool) = NULL;

    ExecInsert = ExecInsertT<false>;

    /* REPLACE INTO not support sequence, check columns to report ERROR */
    for (int attrno=1; attrno<=rel->rd_att->natts; ++attrno) {
        att_tup = &rel->rd_att->attrs[attrno-1];
        errno_t rc;
        char *nspname = get_namespace_name(rel->rd_rel->relnamespace);
        char tableName[NAMEDATALEN*2+2] = {0};
        text *tbname;
        text *attname;
        rc = memset_s(tableName, NAMEDATALEN*2, 0, NAMEDATALEN*2);
        securec_check(rc, "\0", "\0");

        if (nspname != NULL) {
            const char *sym = ".";
            int nspnameLen = strlen(nspname);
            int symLen = strlen(sym);
            int tbnameLen = strlen(rel->rd_rel->relname.data);
            rc = memcpy_s(&tableName[0], NAMEDATALEN*2+2, nspname, nspnameLen);
            securec_check(rc, "\0","\0");
            rc = memcpy_s(&tableName[nspnameLen], NAMEDATALEN*2+2, sym, symLen);
            securec_check(rc, "\0","\0");
            rc = memcpy_s(&tableName[nspnameLen+symLen], NAMEDATALEN*2+2, rel->rd_rel->relname.data, tbnameLen);
            securec_check(rc, "\0","\0");
            tableName[nspnameLen+symLen+tbnameLen] = 0;
            tbname = cstring_to_text(&tableName[0]);
        } else {
            tbname = cstring_to_text(rel->rd_rel->relname.data);
        }

        attname = cstring_to_text(att_tup->attname.data);
        seqOid = pg_get_serial_sequence_oid(tbname, attname);

        if (OidIsValid(seqOid))
            elog(ERROR, "REPLACE can not work on sequence!");
    }

    /* set flag to start loop */
    node->isConflict = true;

    /* we assume there is conflict by default, try to delete the conflict tuple then insert new one */
    while (node->isConflict) {
        ConflictInfoData conflictInfo;
        Oid conflictPartOid = InvalidOid;
        int2 conflictBucketid = InvalidBktId;
        bool isgpi = false;
        Oid partitionid = InvalidOid;
        int partitionno = INVALID_PARTITION_NO;
        Oid subPartitionId = InvalidOid;
        int subpartitionno = INVALID_PARTITION_NO;
        Relation targetrel = NULL;
        Relation heaprel = NULL;
        Relation subPartRel = NULL;
        Partition partition = NULL;
        Partition subPart = NULL;
        Tuple tuple = NULL;
        ResultRelInfo* resultRelInfo = NULL;

        slot = plan_slot;
        resultRelInfo = estate->es_result_relation_info;
        targetrel = resultRelInfo->ri_RelationDesc;
        heaprel = targetrel;
        tuple = tableam_tslot_get_tuple_from_slot(targetrel, slot);
        if (RelationIsPartitioned(targetrel)) {
            partitionid = heapTupleGetPartitionOid(targetrel, tuple, &partitionno, false, false, !partExprKeyStr);
            searchFakeReationForPartitionOid(estate->esfRelations,
                                             estate->es_query_cxt,
                                             targetrel,
                                             partitionid,
                                             partitionno,
                                             heaprel,
                                             partition,
                                             RowExclusiveLock);

            if (RelationIsSubPartitioned(targetrel)) {
                bool subpartExprKeyIsNull = PartExprKeyIsNull(heaprel, NULL);
                subPartitionId = heapTupleGetPartitionOid(heaprel, tuple, &subpartitionno, false, false, subpartExprKeyIsNull);
                searchFakeReationForPartitionOid(estate->esfRelations,
                                                 estate->es_query_cxt,
                                                 heaprel,
                                                 subPartitionId,
                                                 subpartitionno,
                                                 subPartRel,
                                                 subPart,
                                                 RowExclusiveLock);
                partitionid = subPartitionId;
                heaprel = subPartRel;
                partition = subPart;
            }
        }

        targetrel = heaprel;
        if (!ExecCheckIndexConstraints(plan_slot, estate, targetrel,
                                       partition, &isgpi, bucketid, &conflictInfo,
                                       &conflictPartOid, &conflictBucketid)) {

            ExecDelete(&(&conflictInfo)->conflictTid, conflictPartOid,
                       conflictBucketid, NULL, plan_slot, &node->mt_epqstate,
                       node, node->canSetTag);
            InstrCountFiltered2(&node->ps, 1);
        } else {
            slot = ExecInsert(node, slot, plan_slot, estate, node->canSetTag, hi_options, &partition_list, partExprKeyStr, replaceNull);
        }
    }
    return slot;
}

/* ----------------------------------------------------------------
 *	   ExecModifyTable
 *
 *		Perform table modifications as required, and return RETURNING results
 *		if needed.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecModifyTable(PlanState* state)
{
    ModifyTableState* node = castNode(ModifyTableState, state);
    EState* estate = node->ps.state;
    CmdType operation = node->operation;
    ResultRelInfo* saved_result_rel_info = NULL;
    ResultRelInfo* result_rel_info = NULL;
    PlanState* subPlanState = NULL;
    char* partExprKeyStr = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    PlanState* remote_rel_state = NULL;
    PlanState* insert_remote_rel_state = NULL;
    PlanState* update_remote_rel_state = NULL;
    PlanState* delete_remote_rel_state = NULL;
    PlanState* saved_result_remote_rel = NULL;
#endif
    JunkFilter* junk_filter = NULL;
    TupleTableSlot* slot = NULL;
    TupleTableSlot* plan_slot = NULL;
    ItemPointer tuple_id = NULL;
    ItemPointerData tuple_ctid;
    HeapTupleHeader old_tuple = NULL;
    Oid old_partition_oid = InvalidOid;
    bool part_key_updated = ((ModifyTable*)node->ps.plan)->partKeyUpdated;
    TupleTableSlot* (*ExecInsert)(
        ModifyTableState* state, TupleTableSlot*, TupleTableSlot*, EState*, bool, int, List**, char*, bool) = NULL;
    bool use_heap_multi_insert = false;
    int hi_options = 0;
    /* indicates whether it is the first time to insert, delete, update or not. */
    bool is_first_modified = true;
    int2 bucketid = InvalidBktId;
    List *partition_list = NIL;

    int resultRelationNum = node->mt_ResultTupleSlots ? list_length(node->mt_ResultTupleSlots): node->mt_nplans;
#ifdef USE_SPQ
    AttrNumber action_attno = InvalidAttrNumber;
    int action = -1;
#endif
    
    CHECK_FOR_INTERRUPTS();

    /*
     * This should NOT get called during EvalPlanQual; we should have passed a
     * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
     * Assert because this condition is easy to miss in testing.  (Note:
     * although ModifyTable should not get executed within an EvalPlanQual
     * operation, we do have to allow it to be initialized and shut down in
     * case it is within a CTE subplan.  Hence this test must be here, not in
     * ExecInitModifyTable.)
     */
    if (estate->es_epqTuple != NULL) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                (errcode(ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
                    errmsg("ModifyTable should not be called during EvalPlanQual"))));
    }

    /*
     * If we've already completed processing, don't try to do more.  We need
     * this test because ExecPostprocessPlan might call us an extra time, and
     * our subplan's nodes aren't necessarily robust against being called
     * extra times.
     */
    if (node->mt_done)
        return NULL;

    /*
     * On first call, fire BEFORE STATEMENT triggers before proceeding.
     */
    if (node->fireBSTriggers) {
        fireBSTriggers(node);
        node->fireBSTriggers = false;
    }

    WHITEBOX_TEST_STUB("ExecModifyTable_Begin", WhiteboxDefaultErrorEmit);

    /* Preload local variables */
    result_rel_info = node->resultRelInfo + estate->result_rel_index;
    subPlanState = node->mt_plans[node->mt_whichplan];
#ifdef USE_SPQ
    action_attno = result_rel_info->ri_actionAttno;
#endif
#ifdef ENABLE_MULTIPLE_NODES
    /* Initialize remote plan state */
    remote_rel_state = node->mt_remoterels[node->mt_whichplan];
    insert_remote_rel_state = node->mt_insert_remoterels[node->mt_whichplan];
    update_remote_rel_state = node->mt_update_remoterels[node->mt_whichplan];
    delete_remote_rel_state = node->mt_delete_remoterels[node->mt_whichplan];
#endif
    /*
     * es_result_relation_info must point to the currently active result
     * relation while we are within this ModifyTable node.	Even though
     * ModifyTable nodes can't be nested statically, they can be nested
     * dynamically (since our subplan could include a reference to a modifying
     * CTE).  So we have to save and restore the caller's value.
     */
    saved_result_rel_info = estate->es_result_relation_info;
#ifdef ENABLE_MULTIPLE_NODES
    saved_result_remote_rel = estate->es_result_remoterel;
#endif

    estate->es_result_relation_info = result_rel_info;
#ifdef ENABLE_MULTIPLE_NODES
    estate->es_result_remoterel = remote_rel_state;
    estate->es_result_insert_remoterel = insert_remote_rel_state;
    estate->es_result_update_remoterel = update_remote_rel_state;
    estate->es_result_delete_remoterel = delete_remote_rel_state;
#endif

    if (operation == CMD_INSERT) {
        if (node->ps.type == T_ModifyTableState ||
                node->mt_upsert->us_action != UPSERT_NONE ||
                node->isReplace ||
                (result_rel_info->ri_TrigDesc != NULL && (result_rel_info->ri_TrigDesc->trig_insert_before_row ||
                                                           result_rel_info->ri_TrigDesc->trig_insert_instead_row)) ||
                !ENABLE_HEAP_MULTI_INSERT_FOR_INSERT_SELECT ||
                (result_rel_info->ri_RelationDesc->rd_att->constr != NULL &&
                     result_rel_info->ri_RelationDesc->rd_att->constr->cons_autoinc != NULL) ||
                RelationIsPartition(result_rel_info->ri_RelationDesc)) {
            ExecInsert = ExecInsertT<false>;
        } else {
            if (RelationIsAstoreFormat(result_rel_info->ri_RelationDesc)) {
                use_heap_multi_insert = true;
                ExecInsert = ExecInsertT<true>;
            } else {
                ExecInsert = ExecInsertT<false>;
            }
        }

        if (use_heap_multi_insert) {
            /*
             * Push the relfilenode to the hash tab, when the transaction abort, we should heap_sync
             * the relation
             */
            if (enable_heap_bcm_data_replication() &&
                !RelationIsForeignTable(estate->es_result_relation_info->ri_RelationDesc) &&
                !RelationIsStream(estate->es_result_relation_info->ri_RelationDesc) &&
                !RelationIsSegmentTable(estate->es_result_relation_info->ri_RelationDesc)) {
                HeapSyncHashSearch(estate->es_result_relation_info->ri_RelationDesc->rd_id, HASH_ENTER);
                LockRelFileNode(estate->es_result_relation_info->ri_RelationDesc->rd_node, RowExclusiveLock);
            }
        }
    }

    /*
     * EvalPlanQual is called when concurrent update or delete, we should skip early free
     */
    bool orig_early_free = subPlanState->state->es_skip_early_free;
    bool orig_early_deinit = subPlanState->state->es_skip_early_deinit_consumer;

    subPlanState->state->es_skip_early_free = true;
    subPlanState->state->es_skip_early_deinit_consumer = true;
    /*
     * Fetch rows from subplan(s), and execute the required table modification
     * for each row.
     */
    for (;;) {
        result_rel_info = node->resultRelInfo + estate->result_rel_index;
        estate->es_result_relation_info = result_rel_info;
        junk_filter = result_rel_info->ri_junkFilter;
#ifdef USE_SPQ
        action_attno = estate->es_result_relation_info->ri_actionAttno;
#endif
        if (!node->isinherit) {
            partExprKeyStr = node->partExprKeyStrArray[estate->result_rel_index];
        }
        if (estate->deleteLimitCount != 0 && estate->es_processed == estate->deleteLimitCount) {
            break;
        }
        /*
         * Reset the per-output-tuple exprcontext. This is needed because
         * triggers expect to use that context as workspace.  It's a bit ugly
         * to do this below the top level of the plan, however.  We might need
         * to rethink this later.
         */
        ResetPerTupleExprContext(estate);
        t_thrd.xact_cxt.ActiveLobRelid = result_rel_info->ri_RelationDesc->rd_id;
        plan_slot = FetchPlanSlot(subPlanState, node->mt_ProjInfos, node->isinherit);
        t_thrd.xact_cxt.ActiveLobRelid = InvalidOid;
        if (TupIsNull(plan_slot)) {
            record_first_time();
            // Flush error recored if need
            //
            if (node->errorRel && node->cacheEnt)
                FlushErrorInfo(node->errorRel, estate, node->cacheEnt);

            /* advance to next subplan if any */
            node->mt_whichplan++;
            if (!node->isinherit) {
                Assert(estate->result_rel_index == 0);
            }
            if (node->mt_whichplan < node->mt_nplans) {
                if (node->isinherit) {
                    if (estate->result_rel_index == resultRelationNum - 1) {
                        estate->result_rel_index = 0;
                    } else {
                        estate->result_rel_index++;
                    }
                    result_rel_info = node->resultRelInfo + estate->result_rel_index;
                }
                subPlanState = node->mt_plans[node->mt_whichplan];
#ifdef ENABLE_MULTIPLE_NODES
                /* Move to next remote plan */
                estate->es_result_remoterel = node->mt_remoterels[node->mt_whichplan];
                remote_rel_state = node->mt_remoterels[node->mt_whichplan];
                insert_remote_rel_state = node->mt_insert_remoterels[node->mt_whichplan];
                update_remote_rel_state = node->mt_update_remoterels[node->mt_whichplan];
                delete_remote_rel_state = node->mt_delete_remoterels[node->mt_whichplan];
#endif
                junk_filter = result_rel_info->ri_junkFilter;
                estate->es_result_relation_info = result_rel_info;
                EvalPlanQualSetPlan(&node->mt_epqstate, subPlanState->plan, node->mt_arowmarks[node->mt_whichplan]);

                if (use_heap_multi_insert) {
                    /*
                     * Push the relfilenode to the hash tab, when the transaction abort, we should heap_sync
                     * the relation
                     */
                    if (enable_heap_bcm_data_replication() &&
                        !RelationIsForeignTable(estate->es_result_relation_info->ri_RelationDesc) &&
                        !RelationIsStream(estate->es_result_relation_info->ri_RelationDesc) &&
                        !RelationIsSegmentTable(estate->es_result_relation_info->ri_RelationDesc)) {
                        HeapSyncHashSearch(estate->es_result_relation_info->ri_RelationDesc->rd_id, HASH_ENTER);
                        LockRelFileNode(estate->es_result_relation_info->ri_RelationDesc->rd_node, RowExclusiveLock);
                    }
                }
                continue;
            } else {
                if (use_heap_multi_insert) {
                    FlushInsertSelectBulk((DistInsertSelectState*)node, estate, node->canSetTag, hi_options,
                                          &partition_list);
                    list_free_ext(partition_list);
                }
                break;
            }
        }

        if (estate->result_rel_index == 0 || node->isinherit)
            EvalPlanQualSetSlot(&node->mt_epqstate, plan_slot);
        slot = plan_slot;
        slot->tts_tupleDescriptor->td_tam_ops = result_rel_info->ri_RelationDesc->rd_tam_ops;
        node->mt_epqstate.plan->isinherit = node->isinherit;
        if (operation == CMD_MERGE) {
            if (junk_filter == NULL) {
                ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                         errmsg("junkfilter should not be NULL")));
            }
            ExecMerge(node, estate, slot, junk_filter, result_rel_info, partExprKeyStr);
            continue;
        }

        if (junk_filter != NULL) {
            /*
             * extract the 'ctid' or 'wholerow' junk attribute.
             */
            if (operation == CMD_UPDATE || operation == CMD_DELETE) {
                char relkind;
                Datum datum;
                bool isNull = false;

                relkind = result_rel_info->ri_RelationDesc->rd_rel->relkind;
                if (relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(relkind)) {
                    datum = ExecGetJunkAttribute(slot, junk_filter->jf_junkAttNo, &isNull);
                    /* when mutil result relations doing left join, maybe exists NULL ctid. */
                    if (isNull) {
                        if (estate->result_rel_index == resultRelationNum - 1) {
                            estate->result_rel_index = 0;
                        } else {
                            estate->result_rel_index++;
                        }

                        continue;
                    }

                    tuple_id = (ItemPointer)DatumGetPointer(datum);
                    tuple_ctid = *tuple_id; /* be sure we don't free ctid!! */
                    tuple_id = &tuple_ctid;

                    if (RELATION_IS_PARTITIONED(result_rel_info->ri_RelationDesc)) {
                        Datum tableOiddatum;
                        bool tableOidisnull = false;

                        tableOiddatum = ExecGetJunkAttribute(slot, result_rel_info->ri_partOidAttNum, &tableOidisnull);

                        if (tableOidisnull) {
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                                        errmsg("tableoid is null when update partitioned table"))));
                        }

                        old_partition_oid = DatumGetObjectId(tableOiddatum);
                    }

                    if (RELATION_HAS_BUCKET(result_rel_info->ri_RelationDesc)) {
                        Datum bucketIddatum;
                        bool bucketIdisnull = false;

                        bucketIddatum = ExecGetJunkAttribute(slot, result_rel_info->ri_bucketIdAttNum,
                                                             &bucketIdisnull);

                        if (bucketIdisnull) {
                            ereport(ERROR,
                                (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("bucketid is null when update table")));
                        }

                        bucketid = DatumGetObjectId(bucketIddatum);
                    }
#ifdef PGXC
                    /* If available, also extract the OLD row */
                    if (IS_PGXC_COORDINATOR && RelationGetLocInfo(result_rel_info->ri_RelationDesc) &&
                        junk_filter->jf_xc_wholerow != InvalidAttrNumber) {
                        datum = ExecGetJunkAttribute(slot, junk_filter->jf_xc_wholerow, &isNull);

                        if (!isNull)
                            old_tuple = DatumGetHeapTupleHeader(datum);
                    } else if (IS_PGXC_DATANODE && junk_filter->jf_xc_node_id) {
                        Assert(!IS_SINGLE_NODE);
                        uint32 xc_node_id = 0;
                        datum = ExecGetJunkAttribute(slot, junk_filter->jf_xc_node_id, &isNull);
                        Assert(!isNull);
                        xc_node_id = DatumGetUInt32(datum);
                        if (xc_node_id != u_sess->pgxc_cxt.PGXCNodeIdentifier) {
                            ereport(ERROR,
                                (errmodule(MOD_EXECUTOR),
                                    (errcode(ERRCODE_NODE_ID_MISSMATCH),
                                        errmsg("invalid node identifier for update/delete"),
                                        errdetail("xc_node_id in tuple is %u, while current node identifier is %u",
                                            xc_node_id,
                                            u_sess->pgxc_cxt.PGXCNodeIdentifier))));
                        }
                    }
#endif
                } else if (relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_STREAM) {
                    /* do nothing; FDW must fetch any junk attrs it wants */
                } else {
                    datum = ExecGetJunkAttribute(slot, junk_filter->jf_junkAttNo, &isNull);
                    /* shouldn't ever get a null result... */
                    if (isNull) {
                        ereport(ERROR,
                            (errmodule(MOD_EXECUTOR),
                                (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                                    errmsg("wholerow is NULL when do operation %d, junk attribute number is %d",
                                        operation,
                                        junk_filter->jf_junkAttNo))));
                    }

                    old_tuple = DatumGetHeapTupleHeader(datum);
                }
                if (IS_SPQ_RUNNING && AttributeNumberIsValid(action_attno)) {
                    datum = ExecGetJunkAttribute(slot, action_attno, &isNull);
                    /* shouldn't ever get a null result... */
                    if (isNull) {
                        ereport(ERROR, (errmsg("action_attno is NULL")));
                    }
                    action = DatumGetInt32(datum);
                }
            }

            /*
             * apply the junk_filter if needed.
             */
            if (operation != CMD_DELETE) {
                slot = ExecFilterJunk(junk_filter, slot);
                slot->tts_tam_ops = result_rel_info->ri_RelationDesc->rd_tam_ops;
            }
        }

#ifdef ENABLE_MULTIPLE_NODES
        estate->es_result_remoterel = remote_rel_state;
        estate->es_result_insert_remoterel = insert_remote_rel_state;
        estate->es_result_update_remoterel = update_remote_rel_state;
        estate->es_result_delete_remoterel = delete_remote_rel_state;
#endif
        switch (operation) {
            case CMD_INSERT: {
                bool replaceNull = CheckPluginReplaceNull() && !IsA(subPlanState, ResultState);
                if (!node->isReplace) {
                    slot = ExecInsert(node, slot, plan_slot, estate, node->canSetTag, hi_options, &partition_list,
                        partExprKeyStr, replaceNull);
                } else {
                    slot = ExecReplace(estate, node, slot, plan_slot, bucketid, hi_options, partition_list,
                        partExprKeyStr, replaceNull);
                }
                break;
            }
            case CMD_UPDATE:
                if (!IS_SPQ_RUNNING) {
                    slot = ExecUpdate(tuple_id,
                                      old_partition_oid,
                                      bucketid,
                                      old_tuple,
                                      slot,
                                      plan_slot,
                                      &node->mt_epqstate,
                                      node,
                                      node->canSetTag,
                                      part_key_updated,
                                      NULL,
                                      partExprKeyStr);
                } else if (DML_INSERT == action) {
                    slot = ExecInsertT<false>(node, slot, plan_slot, estate, node->canSetTag,
                                              hi_options, &partition_list, partExprKeyStr);
                } else { /* DML_DELETE */
                    slot = ExecDelete(tuple_id, old_partition_oid, bucketid, old_tuple, plan_slot,
                                      &node->mt_epqstate, node, false);
                }
                break;
            case CMD_DELETE:
                slot = ExecDelete(
                    tuple_id, old_partition_oid, bucketid, old_tuple, plan_slot, &node->mt_epqstate, node, node->canSetTag);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unknown operation %d when execute the required table modification.", operation))));

                break;
        }

        record_first_time();
        
        /*
         * Switching the result_relation_info here will cause the inheritance function error,
         * but if don't switch the result_relation_info here,
         * openGauss will not be compatible with the multi-table update and deletion of 'B' database
         * ensure result_relation_info must be switched here when perform the multi-table update and deletion
         */
        if (!node->isinherit) {
            if (estate->result_rel_index == resultRelationNum - 1) {
                estate->result_rel_index = 0;
            } else {
                estate->result_rel_index++;
            }
        }

        /*
         * If we got a RETURNING result, return it to caller.  We'll continue
         * the work on next call.
         */
        if (slot != NULL) {
            estate->es_result_relation_info = saved_result_rel_info;
#ifdef ENABLE_MULTIPLE_NODES
            estate->es_result_remoterel = saved_result_remote_rel;
#endif
            return slot;
        }
    }

    subPlanState->state->es_skip_early_free = orig_early_free;
    subPlanState->state->es_skip_early_deinit_consumer = orig_early_deinit;

    /* Restore es_result_relation_info before exiting */
    estate->es_result_relation_info = saved_result_rel_info;
#ifdef ENABLE_MULTIPLE_NODES
    estate->es_result_remoterel = saved_result_remote_rel;
#endif

    list_free_ext(partition_list);

    /*
     * We're done, but fire AFTER STATEMENT triggers before exiting.
     */
    fireASTriggers(node);

    node->mt_done = true;

    ResetTrigShipFlag();

    return NULL;
}

static void InitMultipleModify(ModifyTableState* node, PlanState* subnode, uint32 resultRelationNum)
{
    bool hasoid = false;
    ListCell* l1 = NULL;
    ListCell* l2 = NULL;
    List* targetlists = ((ModifyTable*)(node->ps.plan))->targetlists;
    EState* estate = node->ps.state;

    if (resultRelationNum == 1 || node->isinherit) {
        return;
    }

    node->mt_ProjInfos = (ProjectionInfo**)palloc0((uint32)resultRelationNum * sizeof(ProjectionInfo*));
    if (estate->es_is_flt_frame) {
        node->targetlists = targetlists;
    } else {
        node->targetlists = (List*)ExecInitExprByRecursion((Expr*)targetlists, (PlanState*)node);
    }

    int i = 0;
    forboth (l1, node->targetlists, l2, targetlists) {
        List* targetExprList = (List*)lfirst(l1);
        List* targetList = (List*)lfirst(l2);

        if (!ExecContextForcesOids((PlanState*)node, &hasoid)) {
            hasoid = false;
        }
        estate->es_result_relation_info++;

        TupleTableSlot *slot = ExecAllocTableSlot(&estate->es_tupleTable);
        TupleDesc tupDesc = ExecTypeFromTL(targetList, hasoid, false);
        ExecSetSlotDescriptor(slot, tupDesc);
        node->mt_ResultTupleSlots = lappend(node->mt_ResultTupleSlots, slot);
        ProjectionInfo* projInfo = NULL;
        if (estate->es_is_flt_frame) {
            projInfo = ExecBuildProjectionInfoByFlatten(targetExprList, subnode->ps_ExprContext, slot, (PlanState*)node, NULL);
        } else {
            projInfo = ExecBuildProjectionInfoByRecursion(targetExprList, subnode->ps_ExprContext, slot, NULL);
        }
        /* Use the targetlist of the subplan to build mt_ProjInfos. It will be use for FetchMultipleModifySlot. */
        node->mt_ProjInfos[i] = projInfo;
        i++;
    }
}

/* ----------------------------------------------------------------
 *		ExecInitModifyTable
 * ----------------------------------------------------------------
 */
ModifyTableState* ExecInitModifyTable(ModifyTable* node, EState* estate, int eflags)
{
    ModifyTableState* mt_state = NULL;
    CmdType operation = node->operation;
    int nplans = list_length(node->plans);
    ResultRelInfo* saved_result_rel_info = NULL;
    ResultRelInfo* result_rel_info = NULL;
    TupleDesc tup_desc = NULL;
    Plan* sub_plan = NULL;
    UpsertState* upsertState = NULL;
    ListCell* l = NULL;
    int i;
    int resultRelationNum;
#ifdef PGXC
#ifdef ENABLE_MULTIPLE_NDOES
    PlanState* saved_remote_rel_info = NULL;
#endif
#endif
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    if (node->is_dist_insertselect)
        mt_state = (ModifyTableState*)makeNode(DistInsertSelectState);
    else
        mt_state = makeNode(ModifyTableState);

    estate->deleteLimitCount = 0;
	
    mt_state->isinherit = node->plan.isinherit;
    if (mt_state->isinherit)
        resultRelationNum = list_length(node->resultRelations);
    else
        resultRelationNum = list_length((List*)linitial(node->resultRelations));
	
    if (node->cacheEnt != NULL) {
        ErrorCacheEntry* entry = node->cacheEnt;

        /* fetch query dop from this way but not query_dop */
        int dop = estate->es_plannedstmt->query_dop;

        mt_state->errorRel = relation_open(node->cacheEnt->rte->relid, RowExclusiveLock);
        mt_state->cacheEnt = node->cacheEnt;

        /*
         * Here we will record all the cache files during importing data.
         * if none error happens, all cache files will be removed after importing data is done.
         * if any error happens, these cache files will be removed by CleanupTempFiles().
         * see CleanupTempFiles() and FD_ERRTBL_LOG_OWNER flag.
         */
        entry->loggers = (ImportErrorLogger**)palloc0(sizeof(ImportErrorLogger*) * dop);
        for (i = 0; i < dop; ++i) {
            /* it's my responsibility for unlinking these cache files */
            ErrLogInfo errinfo = {(uint32)i, true};

            ImportErrorLogger* logger = New(CurrentMemoryContext) LocalErrorLogger;
            logger->Initialize(entry->filename, RelationGetDescr(mt_state->errorRel), errinfo);
            entry->loggers[i] = logger;
        }
        entry->logger_num = dop;
    }
    mt_state->ps.plan = (Plan*)node;
    mt_state->ps.state = estate;
    mt_state->ps.targetlist = NIL; /* not actually used */
    mt_state->ps.ExecProcNode = ExecModifyTable;

    mt_state->operation = operation;
    mt_state->canSetTag = node->canSetTag;
    mt_state->mt_done = false;

    mt_state->mt_plans = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
#ifdef ENABLE_MULTIPLE_NDOES
    mt_state->mt_remoterels = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
    mt_state->mt_insert_remoterels = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
    mt_state->mt_update_remoterels = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
    mt_state->mt_delete_remoterels = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
#endif
    mt_state->resultRelInfo = estate->es_result_relations + node->resultRelIndex;
    mt_state->mt_arowmarks = (List**)palloc0(sizeof(List*) * nplans);
    mt_state->mt_nplans = nplans;
    mt_state->limitExprContext = NULL;

    upsertState = (UpsertState*)palloc0(sizeof(UpsertState));
    upsertState->us_action = node->upsertAction;
    upsertState->us_existing = NULL;
    upsertState->us_excludedtlist = NIL;
    upsertState->us_updateproj = NULL;
    upsertState->us_updateWhere = NIL;
    mt_state->mt_upsert = upsertState;
    mt_state->isReplace = node->isReplace;

    mt_state->fireBSTriggers = true;

    mt_state->mt_isSplitUpdates = NULL;
    if (IS_SPQ_RUNNING) {
        if (node->isSplitUpdates) {
            if (list_length(node->isSplitUpdates) != nplans) {
                ereport(ERROR, (errmsg("ModifyTable node is missing is-split-update information")));
            }
            mt_state->mt_isSplitUpdates = (bool *)palloc0(nplans * sizeof(bool));
            i = 0;
            foreach(l, node->isSplitUpdates) {
                mt_state->mt_isSplitUpdates[i++] = (bool)lfirst_int(l);
            }
        }
    }

    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "mt_plans".  This is also a convenient place to
     * verify that the proposed target relations are valid and open their
     * indexes for insertion of new index entries.	Note we *must* set
     * estate->es_result_relation_info correctly while we initialize each
     * sub-plan; ExecContextForcesOids depends on that!
     */
    saved_result_rel_info = estate->es_result_relation_info;
#ifdef ENABLE_MULTIPLE_NDOES
    saved_remote_rel_info = estate->es_result_remoterel;
#endif

    result_rel_info = mt_state->resultRelInfo;

    /*
     * mergeTargetRelation must be set if we're running MERGE and mustn't be
     * set if we're not.
     */
    Assert(operation != CMD_MERGE || node->mergeTargetRelation > 0);
    Assert(operation == CMD_MERGE || node->mergeTargetRelation == 0);

    result_rel_info->ri_mergeTargetRTI = node->mergeTargetRelation;

    i = 0;
    foreach (l, node->plans) {
        sub_plan = (Plan*)lfirst(l);
        sub_plan->rightRefState = node->plan.rightRefState;

        /*
         * Verify result relation is a valid target for the current operation
         */
        for (int ri = 0; ri < resultRelationNum; ri++) {
            CheckValidResultRel(result_rel_info->ri_RelationDesc, operation);
            result_rel_info++;
        }
        result_rel_info = mt_state->resultRelInfo;

        /* Now init the plan for this result rel */
        estate->es_result_relation_info = result_rel_info;
        if (sub_plan->type == T_Limit && operation == CMD_DELETE && IsLimitDML((Limit*)sub_plan)) {
            /* remove limit plan for delete limit */
            if (mt_state->limitExprContext == NULL) {
                mt_state->limitExprContext = CreateExprContext(estate);
            }
            mt_state->mt_plans[i] = ExecInitNode(outerPlan(sub_plan), estate, eflags);
            estate->deleteLimitCount = GetDeleteLimitCount(mt_state->limitExprContext,
                                                           mt_state->mt_plans[i], (Limit*)sub_plan);
        } else {
            mt_state->mt_plans[i] = ExecInitNode(sub_plan, estate, eflags);
        }
        InitMultipleModify(mt_state, mt_state->mt_plans[i], (uint32)resultRelationNum);
        estate->es_result_relation_info = result_rel_info;
        /*
         * If there are indices on the result relation, open them and save
         * descriptors in the result relation info, so that we can add new
         * index entries for the tuples we add/update.	We need not do this
         * for a DELETE, however, since deletion doesn't affect indexes. Also,
         * inside an EvalPlanQual operation, the indexes might be open
         * already, since we share the resultrel state with the original
         * query.
         */
        for (int ri = 0; ri < resultRelationNum; ri++) {
            if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex &&
                result_rel_info->ri_IndexRelationDescs == NULL) {
#ifdef ENABLE_MOT
                if (result_rel_info->ri_FdwRoutine == NULL || result_rel_info->ri_FdwRoutine->GetFdwType == NULL ||
                    result_rel_info->ri_FdwRoutine->GetFdwType() != MOT_ORC) {
#endif
                    if (RelationIsUstoreFormat(result_rel_info->ri_RelationDesc) || operation != CMD_DELETE) {
                        ExecOpenIndices(result_rel_info, (node->upsertAction != UPSERT_NONE || node->isReplace ||
                                        (estate->es_plannedstmt && estate->es_plannedstmt->hasIgnore)));
                    }
#ifdef ENABLE_MOT
                }
#endif
            }
            result_rel_info++;
        }
        result_rel_info = mt_state->resultRelInfo;
        init_gtt_storage(operation, result_rel_info);

        if (operation == CMD_MERGE && RelationInClusterResizing(estate->es_result_relation_info->ri_RelationDesc)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupport 'MERGE INTO' command during online expansion on '%s'",
                        RelationGetRelationName(estate->es_result_relation_info->ri_RelationDesc))));
        }

        /*
         * For update/delete/upsert case, we need further check if it is in cluster resizing, then
         * we need open delete_delta rel for this target relation.
         */
        if (operation == CMD_UPDATE || operation == CMD_DELETE || node->upsertAction == UPSERT_UPDATE) {
            Relation target_rel = estate->es_result_relation_info->ri_RelationDesc;
            Assert(target_rel != NULL && mt_state->delete_delta_rel == NULL);
            if (RelationInClusterResizing(target_rel) && !RelationInClusterResizingReadOnly(target_rel)) {
                mt_state->delete_delta_rel = GetAndOpenDeleteDeltaRel(target_rel, RowExclusiveLock, false);
            }
        }

        /* Also let FDWs init themselves for foreign-table result rels */
        if (result_rel_info->ri_FdwRoutine != NULL && result_rel_info->ri_FdwRoutine->BeginForeignModify != NULL) {
#ifdef ENABLE_MOT
            if (IS_PGXC_DATANODE || result_rel_info->ri_FdwRoutine->GetFdwType == NULL ||
                result_rel_info->ri_FdwRoutine->GetFdwType() != MOT_ORC) {
#endif
                List* fdw_private = (List*)list_nth(node->fdwPrivLists, i);
                result_rel_info->ri_FdwRoutine->BeginForeignModify(mt_state, result_rel_info, fdw_private, i, eflags);
#ifdef ENABLE_MOT
            }
#endif
        }

        i++;
    }

#ifdef ENABLE_MULTIPLE_NDOES
    i = 0;
    foreach (l, node->plans) {

        Plan* remoteplan = NULL;
        if (node->remote_plans) {
            remoteplan = (Plan*)list_nth(node->remote_plans, i);
            mt_state->mt_remoterels[i] = ExecInitNode(remoteplan, estate, eflags);
        }

        if (node->remote_insert_plans) {
            remoteplan = (Plan*)list_nth(node->remote_insert_plans, i);
            mt_state->mt_insert_remoterels[i] = ExecInitNode(remoteplan, estate, eflags);
        }

        if (node->remote_update_plans) {
            remoteplan = (Plan*)list_nth(node->remote_update_plans, i);
            mt_state->mt_update_remoterels[i] = ExecInitNode(remoteplan, estate, eflags);
        }

        if (node->remote_delete_plans) {
            remoteplan = (Plan*)list_nth(node->remote_delete_plans, i);
            mt_state->mt_delete_remoterels[i] = ExecInitNode(remoteplan, estate, eflags);
        }
        i++;
    }
#endif
    /* set up epqstate with dummy subplan data for the moment */
    EvalPlanQualInit(&mt_state->mt_epqstate, estate, NULL, NIL, node->epqParam);

    estate->es_result_relation_info = saved_result_rel_info;
#ifdef ENABLE_MULTIPLE_NDOES
    estate->es_result_remoterel = saved_remote_rel_info;
#endif

    /*
     * Initialize any WITH CHECK OPTION constraints if needed.
     */
    result_rel_info = mt_state->resultRelInfo;
    i = 0;

    /*
     * length of node->withCheckOptionLists must be 1 because inherited
     * table is not supported yet.
     */
    Assert(node->withCheckOptionLists == NULL || list_length(node->withCheckOptionLists) == 1);

    for (int ri = 0; node->withCheckOptionLists != NULL && ri < resultRelationNum; ri++) {
        List* wcoList = NIL;
        List* wcoExprs = NIL;
        ListCell* ll = NULL;

        foreach(ll, (List*)linitial(node->withCheckOptionLists)) {
            WithCheckOption* wco = (WithCheckOption*)lfirst(ll);
            ExprState* wcoExpr = NULL;
            if (wco->rtindex == result_rel_info->ri_RangeTableIndex) {
                if (estate->es_is_flt_frame) {
                    wcoExpr = ExecInitQualByFlatten((List*)wco->qual, mt_state->mt_plans[i]);
                } else {
                    wcoExpr = ExecInitExprByRecursion((Expr*)wco->qual, mt_state->mt_plans[i]);
                }
                wcoExprs = lappend(wcoExprs, wcoExpr);
                wcoList = lappend(wcoList, wco);
            }
        }

        result_rel_info->ri_WithCheckOptions = wcoList;
        result_rel_info->ri_WithCheckOptionExprs = wcoExprs;
        result_rel_info++;
    }

    /*
     * Initialize RETURNING projections if needed.
     */
    if (node->returningLists) {
        TupleTableSlot* slot = NULL;
        ExprContext* econtext = NULL;

        /*
         * Initialize result tuple slot and assign its rowtype using the first
         * RETURNING list.	We assume the rest will look the same.
         */
        tup_desc = ExecTypeFromTL((List *)linitial(node->returningLists), false, false,
                                  mt_state->resultRelInfo->ri_RelationDesc->rd_tam_ops);

        /* Set up a slot for the output of the RETURNING projection(s) */
        ExecInitResultTupleSlot(estate, &mt_state->ps);
        ExecAssignResultType(&mt_state->ps, tup_desc);
        slot = mt_state->ps.ps_ResultTupleSlot;

        /* Need an econtext too */
        econtext = CreateExprContext(estate);
        mt_state->ps.ps_ExprContext = econtext;
        /*
         * Build a projection for each result rel.
         */
        result_rel_info = mt_state->resultRelInfo;
        foreach (l, node->returningLists) {
            List* rlist = (List*)lfirst(l);

            result_rel_info->ri_projectReturning =
                ExecBuildProjectionInfo(rlist, econtext, slot, &mt_state->ps, result_rel_info->ri_RelationDesc->rd_att);
        }
    } else {
        /*
         * We still must construct a dummy result tuple type, because InitPlan
         * expects one (maybe should change that?).
         */
        tup_desc = ExecTypeFromTL(NIL, false);
        ExecInitResultTupleSlot(estate, &mt_state->ps);
        ExecAssignResultType(&mt_state->ps, tup_desc);

        mt_state->ps.ps_ExprContext = NULL;
    }

    /*
     * If needed, Initialize target list, projection and qual for DUPLICATE KEY UPDATE
     */
    result_rel_info = mt_state->resultRelInfo;
    if (node->upsertAction == UPSERT_UPDATE ||node->isReplace) {
        ExprContext* econtext = NULL;
        TupleDesc tupDesc;

        /* insert may only have one plan, inheritance is not expanded */
        Assert(nplans = 1);

        /* already exists if created by RETURNING processing above */
        if (mt_state->ps.ps_ExprContext == NULL) {
            ExecAssignExprContext(estate, &mt_state->ps);
        }

        econtext = mt_state->ps.ps_ExprContext;
        ATTACH_RIGHT_REF_STATE(&(mt_state->ps));

        /* initialize slot for the existing tuple */
        upsertState->us_existing =
            ExecInitExtraTupleSlot(mt_state->ps.state, result_rel_info->ri_RelationDesc->rd_tam_ops);
        ExecSetSlotDescriptor(upsertState->us_existing, result_rel_info->ri_RelationDesc->rd_att);

        upsertState->us_excludedtlist = node->exclRelTlist;

        /* create target slot for UPDATE SET projection */
        tupDesc = ExecTypeFromTL((List*)node->updateTlist, result_rel_info->ri_RelationDesc->rd_rel->relhasoids);
        upsertState->us_updateproj =
            ExecInitExtraTupleSlot(mt_state->ps.state, result_rel_info->ri_RelationDesc->rd_tam_ops);
        ExecSetSlotDescriptor(upsertState->us_updateproj, tupDesc);

        /* build UPDATE SET expression and projection state */
        result_rel_info->ri_updateProj =
        ExecBuildProjectionInfo(node->updateTlist, econtext,
            upsertState->us_updateproj, &mt_state->ps, result_rel_info->ri_RelationDesc->rd_att);
        result_rel_info->ri_updateProj->isUpsertHasRightRef =
                IS_ENABLE_RIGHT_REF(econtext->rightRefState) && econtext->rightRefState->isUpsertHasRightRef;
        /* initialize expression state to evaluate update where clause if exists */
        if (node->upsertWhere) {
            if (estate->es_is_flt_frame) {
                upsertState->us_updateWhere = (List*)ExecInitQualByFlatten((List*)node->upsertWhere, &mt_state->ps);
            } else {
                upsertState->us_updateWhere = (List*)ExecInitExprByRecursion((Expr*)node->upsertWhere, &mt_state->ps);
            }
        }
    }

    /*
     * If we have any secondary relations in an UPDATE or DELETE, they need to
     * be treated like non-locked relations in SELECT FOR UPDATE, ie, the
     * EvalPlanQual mechanism needs to be told about them.	Locate the
     * relevant ExecRowMarks.
     */
    foreach (l, node->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(l);

        Assert(IsA(rc, PlanRowMark));

        /* ignore "parent" rowmarks; they are irrelevant at runtime */
        if (rc->isParent)
            continue;

        if (!(IS_PGXC_COORDINATOR || u_sess->pgxc_cxt.PGXCNodeId < 0 ||
                bms_is_member(u_sess->pgxc_cxt.PGXCNodeId, rc->bms_nodeids))) {
            continue;
        }

        /* find ExecRowMark (same for all subplans) */
        ExecRowMark* erm = ExecFindRowMark(estate, rc->rti);

        /* build ExecAuxRowMark for each sub_plan */
        for (i = 0; i < nplans; i++) {
            sub_plan = mt_state->mt_plans[i]->plan;

            ExecAuxRowMark* aerm = ExecBuildAuxRowMark(erm, sub_plan->targetlist);
            mt_state->mt_arowmarks[i] = lappend(mt_state->mt_arowmarks[i], aerm);
        }
    }

    if (node->is_dist_insertselect) {
        DistInsertSelectState* distInsertSelectState = (DistInsertSelectState*)mt_state;
        distInsertSelectState->rows = 0;
        distInsertSelectState->insert_mcxt = AllocSetContextCreate(CurrentMemoryContext,
            "Insert into Select",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        distInsertSelectState->mgr =
            initCopyFromManager(CurrentMemoryContext, mt_state->resultRelInfo->ri_RelationDesc, true);
        distInsertSelectState->bistate = GetBulkInsertState();
        if (RowRelationIsCompressed(mt_state->resultRelInfo->ri_RelationDesc))
            distInsertSelectState->pcState =
                New(CurrentMemoryContext) PageCompress(mt_state->resultRelInfo->ri_RelationDesc, CurrentMemoryContext);
    }

    result_rel_info = mt_state->resultRelInfo;

    if (mt_state->operation == CMD_MERGE) {
        if (IsA(node, ModifyTable)) {
            ExecInitMerge(mt_state, estate, result_rel_info);
        } else if (IsA(node, VecModifyTable)) {
            Assert(RelationIsCUFormat(result_rel_info->ri_RelationDesc));
            ExecInitVecMerge(mt_state, estate, result_rel_info);
        }
    }

    /* select first sub_plan */
    mt_state->mt_whichplan = 0;
    sub_plan = (Plan*)linitial(node->plans);
    EvalPlanQualSetPlan(&mt_state->mt_epqstate, sub_plan, mt_state->mt_arowmarks[0]);

    /*
     * Initialize the junk filter(s) if needed.  INSERT queries need a filter
     * if there are any junk attrs in the tlist.  UPDATE and DELETE always
     * need a filter, since there's always a junk 'ctid' or 'wholerow'
     * attribute present --- no need to look first.
     *
     * If there are multiple result relations, each one needs its own junk
     * filter.	Note multiple rels are only possible for UPDATE/DELETE, so we
     * can't be fooled by some needing a filter and some not.
     *
     * This section of code is also a convenient place to verify that the
     * output of an INSERT or UPDATE matches the target table(s).
     */
    {
        bool junk_filter_needed = false;

        switch (operation) {
            case CMD_INSERT:
                foreach (l, sub_plan->targetlist) {
                    TargetEntry* tle = (TargetEntry*)lfirst(l);

                    if (tle->resjunk) {
                        junk_filter_needed = true;
                        break;
                    }
                }
                break;
            case CMD_UPDATE:
            case CMD_DELETE:
            case CMD_MERGE:
                junk_filter_needed = true;
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unknown operation %d when execute the required table modification.", operation))));
                break;
        }

        if (junk_filter_needed) {
            if (mt_state->isinherit) {
                result_rel_info = mt_state->resultRelInfo;
                for (i = 0; i < nplans; i++) {
                    JunkFilter *j;
                    sub_plan = mt_state->mt_plans[i]->plan;
                    if (operation == CMD_INSERT || operation == CMD_UPDATE)
                        ExecCheckPlanOutput(result_rel_info->ri_RelationDesc, sub_plan->targetlist);

                    j = ExecInitJunkFilter(sub_plan->targetlist,
                                           result_rel_info->ri_RelationDesc->rd_att->tdhasoid,
                                           ExecInitExtraTupleSlot(estate, NULL), TableAmHeap);

                    if (operation == CMD_UPDATE || operation == CMD_DELETE) {
                        /* For UPDATE/DELETE, find the appropriate junk attr now */
                        char relkind = result_rel_info->ri_RelationDesc->rd_rel->relkind;
                        if (relkind == RELKIND_RELATION ||
                            relkind == RELKIND_MATVIEW ||
                            relkind == PARTTYPE_PARTITIONED_RELATION) {
                            j->jf_junkAttNo = ExecFindJunkAttribute(j, "ctid");
                            if (!AttributeNumberIsValid(j->jf_junkAttNo))
                                elog(ERROR, "could not find junk ctid column");
                        } else {
                            j->jf_junkAttNo = ExecFindJunkAttribute(j, "wholerow");
                            if (!AttributeNumberIsValid(j->jf_junkAttNo))
                                elog(ERROR, "could not find junk wholerow column");
                        }
                    }
                    result_rel_info->ri_junkFilter = j;
                    result_rel_info++;
                }
            } else {
                result_rel_info = mt_state->resultRelInfo;
                for (i = 0; i < nplans; i++) {
                    sub_plan = mt_state->mt_plans[i]->plan;
                    if (resultRelationNum > 1) {
                        foreach (l, node->targetlists) {
                            List* targetlist = (List*)lfirst(l);
                            if (operation == CMD_UPDATE) {
                                ExecCheckPlanOutput(result_rel_info->ri_RelationDesc, targetlist);
                            }
                            ExecInitJunkAttr(estate, operation, targetlist, result_rel_info);
                            result_rel_info++;
                        }
                    } else {
                        if (operation == CMD_UPDATE) {
                            CheckPlanOutput(sub_plan, result_rel_info->ri_RelationDesc);
                        }
                        ExecInitJunkAttr(estate, operation, sub_plan->targetlist, result_rel_info);
                    }
                    if (IS_SPQ_RUNNING && operation == CMD_UPDATE) {
                        char relkind = result_rel_info->ri_RelationDesc->rd_rel->relkind;
                        if (relkind == RELKIND_RELATION ||
                            relkind == RELKIND_MATVIEW ||
                            relkind == PARTTYPE_PARTITIONED_RELATION) {
                            if (mt_state->mt_isSplitUpdates && mt_state->mt_isSplitUpdates[i]) {
                                JunkFilter* j = result_rel_info->ri_junkFilter;
                                result_rel_info->ri_actionAttno = ExecFindJunkAttribute(j, "DMLAction");
                                if (!AttributeNumberIsValid(result_rel_info->ri_actionAttno)) {
                                    ereport(ERROR, (errmsg("could not find junk action column")));
                                }
                            } else {
                                ereport(ERROR, (errmsg("parallel update could not find mt_isSplitUpdates")));
                            }
                        }
                    }
                }
            }
        } else {
            if (operation == CMD_INSERT)
                CheckPlanOutput(sub_plan, mt_state->resultRelInfo->ri_RelationDesc);
        }
    }

    /*
     * Set up a tuple table slot for use for trigger output tuples. In a plan
     * containing multiple ModifyTable nodes, all can share one such slot, so
     * we keep it in the estate.
     */
    if (estate->es_trig_tuple_slot == NULL) {
        result_rel_info = mt_state->resultRelInfo;
        estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, result_rel_info->ri_RelationDesc->rd_tam_ops);
    }

    /*
     * Lastly, if this is not the primary (canSetTag) ModifyTable node, add it
     * to estate->es_auxmodifytables so that it will be run to completion by
     * ExecPostprocessPlan.  (It'd actually work fine to add the primary
     * ModifyTable node too, but there's no need.)  Note the use of lcons not
     * lappend: we need later-initialized ModifyTable nodes to be shut down
     * before earlier ones.  This ensures that we don't throw away RETURNING
     * rows that need to be seen by a later CTE sub_plan.
     */
    if (!mt_state->canSetTag && !mt_state->ps.plan->vec_output &&
        !(IS_PGXC_COORDINATOR && u_sess->exec_cxt.under_stream_runtime))
        estate->es_auxmodifytables = lcons(mt_state, estate->es_auxmodifytables);

    mt_state->partExprKeyStrArray = (char**)palloc0(resultRelationNum * sizeof(char*));
    for (int relIndex = 0; relIndex < resultRelationNum; relIndex++) {
        char* partExprKeyStr = NULL;
        ResultRelInfo* currentRelInfo = mt_state->resultRelInfo + relIndex;
        if (RelationIsPartitioned(currentRelInfo->ri_RelationDesc))
            PartExprKeyIsNull(currentRelInfo->ri_RelationDesc, &partExprKeyStr);
        mt_state->partExprKeyStrArray[relIndex] = partExprKeyStr;
    }

    return mt_state;
}

static void ExecClearMultipleModifyTuple(List* slots)
{
    foreach_cell (l, slots) {
        TupleTableSlot* slot = (TupleTableSlot*)lfirst(l);
        (void)ExecClearTuple(slot);
    }
}

/* ----------------------------------------------------------------
 *		ExecEndModifyTable
 *
 *		Shuts down the plan.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndModifyTable(ModifyTableState* node)
{
    int i;

    /*
     * Allow any FDWs to shut down
     */
    for (i = 0; i < node->mt_nplans; i++) {
        ResultRelInfo* result_rel_info = node->resultRelInfo + i;

        if (result_rel_info->ri_FdwRoutine != NULL && result_rel_info->ri_FdwRoutine->EndForeignModify != NULL) {
#ifdef ENABLE_MOT
            if (IS_PGXC_DATANODE || result_rel_info->ri_FdwRoutine->GetFdwType == NULL ||
                result_rel_info->ri_FdwRoutine->GetFdwType() != MOT_ORC) {
#endif
                result_rel_info->ri_FdwRoutine->EndForeignModify(node->ps.state, result_rel_info);
#ifdef ENABLE_MOT
            }
#endif
        }
    }

    if (IsA(node, DistInsertSelectState)) {
        deinitCopyFromManager(((DistInsertSelectState*)node)->mgr);
        ((DistInsertSelectState*)node)->mgr = NULL;
        FreeBulkInsertState(((DistInsertSelectState*)node)->bistate);
    }

    if (node->errorRel != NULL)
        relation_close(node->errorRel, RowExclusiveLock);
    if (node->cacheEnt != NULL && node->cacheEnt->loggers != NULL) {
        for (i = 0; i < node->cacheEnt->logger_num; ++i) {
            DELETE_EX(node->cacheEnt->loggers[i]);
            /* remove physical file after it's closed. */
            unlink_local_cache_file(node->cacheEnt->filename, (uint32)i);
        }
        pfree_ext(node->cacheEnt->loggers);
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecClearMultipleModifyTuple(node->mt_ResultTupleSlots);

    /*
     * Terminate EPQ execution if active
     */
    EvalPlanQualEnd(&node->mt_epqstate);

    /* clean up relation handler of delete delta table */
    if (node->delete_delta_rel != NULL) {
        relation_close(node->delete_delta_rel, RowExclusiveLock);
        node->delete_delta_rel = NULL;
    }

    /* Drop temp slot */
    if (node->operation == CMD_MERGE) {
        if (node->mt_scan_slot)
            ExecDropSingleTupleTableSlot(node->mt_scan_slot);
        if (node->mt_update_constr_slot)
            ExecDropSingleTupleTableSlot(node->mt_update_constr_slot);
        if (node->mt_insert_constr_slot)
            ExecDropSingleTupleTableSlot(node->mt_insert_constr_slot);
    }

    /*
     * shut down subplans and data modification targets
     */
    for (i = 0; i < node->mt_nplans; i++) {
        ExecEndNode(node->mt_plans[i]);
#ifdef ENABLE_MULTIPLE_NDOES
        ExecEndNode(node->mt_remoterels[i]);
#endif
    }
}

void ExecReScanModifyTable(ModifyTableState* node)
{
    /*
     * Currently, we don't need to support rescan on ModifyTable nodes. The
     * semantics of that would be a bit debatable anyway.
     */
    ereport(ERROR,
        (errmodule(MOD_EXECUTOR),
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ExecReScanModifyTable is not implemented"))));
}

#ifdef ENABLE_MULTIPLE_NDOES

/*
 * fill_slot_with_oldvals:
 * Create a new tuple using the existing 'oldtuphd' and new data from
 * 'replace_slot'. So the values of the modified attributes are taken from
 * replace_slot, and overwritten onto the oldtuphd. Finally the new tuple is
 * stored in 'replace_slot'. This is a convenience function for generating
 * the NEW tuple given the plan slot and old tuple.
 */
static TupleTableSlot* fill_slot_with_oldvals(
    TupleTableSlot* replace_slot, HeapTupleHeader oldtuphd, Bitmapset* modifiedCols)
{
    HeapTupleData old_tuple;
    HeapTuple new_tuple;
    int natts = replace_slot->tts_tupleDescriptor->natts;
    int att_index;
    bool* replaces = NULL;

    if (!oldtuphd) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                (errcode(ERRCODE_TRIGGERED_INVALID_TUPLE), errmsg("expected valid OLD tuple for triggers"))));
    }

    old_tuple.t_data = oldtuphd;
    old_tuple.t_len = HeapTupleHeaderGetDatumLength(oldtuphd);
    ItemPointerSetInvalid(&(old_tuple.t_self));
    HeapTupleSetZeroBase(&old_tuple);
    old_tuple.t_tableOid = InvalidOid;
    old_tuple.t_bucketId = InvalidBktId;
    old_tuple.t_xc_node_id = 0;

    replaces = (bool*)palloc0(natts * sizeof(bool));
    for (att_index = 0; att_index < natts; att_index++) {
        if (bms_is_member(att_index + 1 - FirstLowInvalidHeapAttributeNumber, modifiedCols))
            replaces[att_index] = true;
        else
            replaces[att_index] = false;
    }

    /* Get the Table Accessor Method*/
    Assert(replace_slot != NULL && replace_slot->tts_tupleDescriptor != NULL);

    tableam_tslot_getallattrs(replace_slot);
    new_tuple = (HeapTuple) tableam_tops_modify_tuple(
        &old_tuple, replace_slot->tts_tupleDescriptor, replace_slot->tts_values, replace_slot->tts_isnull, replaces);

    pfree_ext(replaces);

    /*
     * Ultimately store the tuple in the same slot from where we retrieved
     * values to be replaced.
     */
    return ExecStoreTuple(new_tuple, replace_slot, InvalidBuffer, false);
}
#endif
