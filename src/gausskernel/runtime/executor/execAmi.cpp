/* -------------------------------------------------------------------------
 *
 * execAmi.cpp
 *	  miscellaneous executor access method routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	    src/gausskernel/runtime/executor/execAmi.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeAppend.h"
#include "executor/node/nodeBitmapAnd.h"
#include "executor/node/nodeBitmapHeapscan.h"
#include "executor/node/nodeBitmapIndexscan.h"
#include "executor/node/nodeBitmapOr.h"
#include "executor/node/nodeCtescan.h"
#include "executor/node/nodeExtensible.h"
#include "executor/node/nodeForeignscan.h"
#include "executor/node/nodeFunctionscan.h"
#include "executor/node/nodeGroup.h"
#include "executor/node/nodeGroup.h"
#include "executor/node/nodeHash.h"
#include "executor/node/nodeHashjoin.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeLimit.h"
#include "executor/node/nodeLockRows.h"
#include "executor/node/nodeMaterial.h"
#include "executor/node/nodeMergeAppend.h"
#include "executor/node/nodeMergejoin.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/node/nodeNestloop.h"
#include "executor/node/nodePartIterator.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeResult.h"
#include "executor/node/nodeSeqscan.h"
#include "executor/node/nodeSetOp.h"
#include "executor/node/nodeSort.h"
#include "executor/node/nodeSubplan.h"
#include "executor/node/nodeSubqueryscan.h"
#include "executor/node/nodeTidscan.h"
#include "executor/node/nodeUnique.h"
#include "executor/node/nodeValuesscan.h"
#include "executor/node/nodeWindowAgg.h"
#include "executor/node/nodeWorktablescan.h"
#include "nodes/nodeFuncs.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecnodevectorow.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#endif

static bool target_list_supports_backward_scan(List* targetlist);
static bool index_supports_backward_scan(Oid indexid);

/*
 * ExecReScan
 *		Reset a plan node so that its output can be re-scanned.
 *
 * Note that if the plan node has parameters that have changed value,
 * the output might be different from last time.
 */
void ExecReScanByType(PlanState* node)
{
    /* If collecting timing stats, update them */

    /*
     * If we have changed parameters, propagate that info.
     *
     * Note: ExecReScanSetParamPlan() can add bits to node->chgParam,
     * corresponding to the output param(s) that the InitPlan will update.
     * Since we make only one pass over the list, that means that an InitPlan
     * can depend on the output param(s) of a sibling InitPlan only if that
     * sibling appears earlier in the list.  This is workable for now given
     * the limited ways in which one InitPlan could depend on another, but
     * eventually we might need to work harder (or else make the planner
     * enlarge the extParam/allParam sets to include the params of depended-on
     * InitPlans).
     */

    /* Well. Now set chgParam for left/right trees. */

    /* Shut down any SRFs in the plan node's targetlist */

    /* If need stub execution, stop rescan here */

    /* And do node-type-specific processing */
    switch (nodeTag(node)) {
        case T_ResultState:
            ExecReScanResult((ResultState*)node);
            break;

        case T_ModifyTableState:
        case T_DistInsertSelectState:
            ExecReScanModifyTable((ModifyTableState*)node);
            break;

        case T_AppendState:
            ExecReScanAppend((AppendState*)node);
            break;

        case T_MergeAppendState:
            ExecReScanMergeAppend((MergeAppendState*)node);
            break;

        case T_RecursiveUnionState:
            ExecReScanRecursiveUnion((RecursiveUnionState*)node);
            break;

        case T_StartWithOpState:
            ExecReScanStartWithOp((StartWithOpState *)node);
            break;

        case T_BitmapAndState:
            ExecReScanBitmapAnd((BitmapAndState*)node);
            break;

        case T_BitmapOrState:
            ExecReScanBitmapOr((BitmapOrState*)node);
            break;

        case T_SeqScanState:
            ExecReScanSeqScan((SeqScanState*)node);
            break;

        case T_IndexScanState:
            ExecReScanIndexScan((IndexScanState*)node);
            break;

        case T_IndexOnlyScanState:
            ExecReScanIndexOnlyScan((IndexOnlyScanState*)node);
            break;

        case T_BitmapIndexScanState:
            ExecReScanBitmapIndexScan((BitmapIndexScanState*)node);
            break;

        case T_BitmapHeapScanState:
            ExecReScanBitmapHeapScan((BitmapHeapScanState*)node);
            break;

        case T_TidScanState:
            ExecReScanTidScan((TidScanState*)node);
            break;

        case T_SubqueryScanState:
            ExecReScanSubqueryScan((SubqueryScanState*)node);
            break;

        case T_FunctionScanState:
            ExecReScanFunctionScan((FunctionScanState*)node);
            break;

        case T_ValuesScanState:
            ExecReScanValuesScan((ValuesScanState*)node);
            break;

        case T_CteScanState:
            ExecReScanCteScan((CteScanState*)node);
            break;

        case T_WorkTableScanState:
            ExecReScanWorkTableScan((WorkTableScanState*)node);
            break;

        case T_ForeignScanState:
            ExecReScanForeignScan((ForeignScanState*)node);
            break;

        case T_ExtensiblePlanState:
            ExecReScanExtensiblePlan((ExtensiblePlanState*)node);
            break;

            /*
             * partition iterator node
             */
        case T_PartIteratorState:
            ExecReScanPartIterator((PartIteratorState*)node);
            break;

#ifdef PGXC
        case T_RemoteQueryState:
            ExecRemoteQueryReScan((RemoteQueryState*)node, node->ps_ExprContext);
            break;
#endif
        case T_NestLoopState:
            ExecReScanNestLoop((NestLoopState*)node);
            break;

        case T_MergeJoinState:
            ExecReScanMergeJoin((MergeJoinState*)node);
            break;

        case T_HashJoinState:
            ExecReScanHashJoin((HashJoinState*)node);
            break;

        case T_MaterialState:
            ExecReScanMaterial((MaterialState*)node);
            break;

        case T_SortState:
            ExecReScanSort((SortState*)node);
            break;

        case T_GroupState:
            ExecReScanGroup((GroupState*)node);
            break;

        case T_AggState:
            ExecReScanAgg((AggState*)node);
            break;

        case T_WindowAggState:
            ExecReScanWindowAgg((WindowAggState*)node);
            break;

        case T_UniqueState:
            ExecReScanUnique((UniqueState*)node);
            break;

        case T_HashState:
            ExecReScanHash((HashState*)node);
            break;

        case T_SetOpState:
            ExecReScanSetOp((SetOpState*)node);
            break;

        case T_LockRowsState:
            ExecReScanLockRows((LockRowsState*)node);
            break;

        case T_LimitState:
            ExecReScanLimit((LimitState*)node);
            break;

        case T_VecToRowState:
            ExecReScanVecToRow((VecToRowState*)node);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when rescan", (int)nodeTag(node))));
            break;
    }
}

/*
 * ExecReScan
 *		Reset a plan node so that its output can be re-scanned.
 *
 * Note that if the plan node has parameters that have changed value,
 * the output might be different from last time.
 */
void ExecReScan(PlanState* node)
{
    /* If collecting timing stats, update them */
    if (node->instrument) {
        InstrEndLoop(node->instrument);
    }

    /* reset the rownum */
    if (!node->do_not_reset_rownum) {
        node->ps_rownum = 0;
    }

    /*
     * If we have changed parameters, propagate that info.
     *
     * Note: ExecReScanSetParamPlan() can add bits to node->chgParam,
     * corresponding to the output param(s) that the InitPlan will update.
     * Since we make only one pass over the list, that means that an InitPlan
     * can depend on the output param(s) of a sibling InitPlan only if that
     * sibling appears earlier in the list.  This is workable for now given
     * the limited ways in which one InitPlan could depend on another, but
     * eventually we might need to work harder (or else make the planner
     * enlarge the extParam/allParam sets to include the params of depended-on
     * InitPlans).
     */
    if (node->chgParam != NULL) {
        ListCell* l = NULL;

        foreach (l, node->initPlan) {
            SubPlanState* sstate = (SubPlanState*)lfirst(l);
            PlanState* splan = sstate->planstate;

            if (splan->plan->extParam != NULL) { 
                /* don't care about child local Params */
                UpdateChangedParamSet(splan, node->chgParam);
            }
            if (splan->chgParam != NULL) {
                ExecReScanSetParamPlan(sstate, node);
            }
        }
        foreach (l, node->subPlan) {
            SubPlanState* sstate = (SubPlanState*)lfirst(l);
            PlanState* splan = sstate->planstate;

            if (splan->plan->extParam != NULL) {
                UpdateChangedParamSet(splan, node->chgParam);
            }
        }
        /* Well. Now set chgParam for left/right trees. */
        if (node->lefttree != NULL) {
            UpdateChangedParamSet(node->lefttree, node->chgParam);
        }
        if (node->righttree != NULL) {
            UpdateChangedParamSet(node->righttree, node->chgParam);
        }
    }

    /* Shut down any SRFs in the plan node's targetlist */
    if (node->ps_ExprContext) {
        ReScanExprContext(node->ps_ExprContext);
    }

    /* If need stub execution, stop rescan here */
    if (!planstate_need_stub(node)) {
        if (IS_PGXC_DATANODE && EXEC_IN_RECURSIVE_MODE(node->plan) && IsA(node, StreamState)) {
            return;
        }

        ExecReScanByType(node);
    }

    if (node->chgParam != NULL) {
        bms_free_ext(node->chgParam);
        node->chgParam = NULL;
    }
}

/*
 * ExecMarkPos
 *
 * Marks the current scan position.
 */
void ExecMarkPos(PlanState* node)
{
    switch (nodeTag(node)) {
        case T_SeqScanState:
            ExecSeqMarkPos((SeqScanState*)node);
            break;

        case T_IndexScanState:
            ExecIndexMarkPos((IndexScanState*)node);
            break;

        case T_IndexOnlyScanState:
            ExecIndexOnlyMarkPos((IndexOnlyScanState*)node);
            break;

        case T_TidScanState:
            ExecTidMarkPos((TidScanState*)node);
            break;

        case T_ValuesScanState:
            ExecValuesMarkPos((ValuesScanState*)node);
            break;

        case T_MaterialState:
            ExecMaterialMarkPos((MaterialState*)node);
            break;

        case T_SortState:
            ExecSortMarkPos((SortState*)node);
            break;

        case T_ResultState:
            ExecResultMarkPos((ResultState*)node);
            break;

        default:
            /* don't make hard error unless caller asks to restore... */
            elog(DEBUG2, "unrecognized node type: %d", (int)nodeTag(node));
            break;
    }
}

/*
 * ExecRestrPos
 *
 * restores the scan position previously saved with ExecMarkPos()
 *
 * NOTE: the semantics of this are that the first ExecProcNode following
 * the restore operation will yield the same tuple as the first one following
 * the mark operation.	It is unspecified what happens to the plan node's
 * result TupleTableSlot.  (In most cases the result slot is unchanged by
 * a restore, but the node may choose to clear it or to load it with the
 * restored-to tuple.)	Hence the caller should discard any previously
 * returned TupleTableSlot after doing a restore.
 */
void ExecRestrPos(PlanState* node)
{
    switch (nodeTag(node)) {
        case T_SeqScanState:
            ExecSeqRestrPos((SeqScanState*)node);
            break;

        case T_IndexScanState:
            ExecIndexRestrPos((IndexScanState*)node);
            break;

        case T_IndexOnlyScanState:
            ExecIndexOnlyRestrPos((IndexOnlyScanState*)node);
            break;

        case T_TidScanState:
            ExecTidRestrPos((TidScanState*)node);
            break;

        case T_ValuesScanState:
            ExecValuesRestrPos((ValuesScanState*)node);
            break;

        case T_MaterialState:
            ExecMaterialRestrPos((MaterialState*)node);
            break;

        case T_SortState:
            ExecSortRestrPos((SortState*)node);
            break;

        case T_ResultState:
            ExecResultRestrPos((ResultState*)node);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when restore scan position", (int)nodeTag(node))));
            break;
    }
}

/*
 * ExecSupportsMarkRestore - does a plan type support mark/restore?
 *
 * XXX Ideally, all plan node types would support mark/restore, and this
 * wouldn't be needed.  For now, this had better match the routines above.
 * But note the test is on Plan nodetype, not PlanState nodetype.
 *
 * (However, since the only present use of mark/restore is in mergejoin,
 * there is no need to support mark/restore in any plan type that is not
 * capable of generating ordered output.  So the seqscan, tidscan,
 * and valuesscan support is actually useless code at present.)
 */
bool ExecSupportsMarkRestore(Path *pathnode)
{
    switch (pathnode->pathtype) {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_TidScan:
        case T_ValuesScan:
        case T_Material:
        case T_Sort:
            return true;

        case T_BaseResult:

            /*
             * T_BaseResult only supports mark/restore if it has a child plan that
             * does, so we do not have enough information to give a really
             * correct answer.	However, for current uses it's enough to
             * always say "false", because this routine is not asked about
             * gating Result plans, only base-case Results.
             */
            return false;

        case T_ExtensiblePlan:
            return castNode(ExtensiblePath, pathnode)->flags & EXTENSIBLEPATH_SUPPORT_MARK_RESTORE;

        default:
            break;
    }

    return false;
}

/*
 * ExecSupportsBackwardScan - does a plan type support backwards scanning?
 *
 * Ideally, all plan types would support backwards scan, but that seems
 * unlikely to happen soon.  In some cases, a plan node passes the backwards
 * scan down to its children, and so supports backwards scan only if its
 * children do.  Therefore, this routine must be passed a complete plan tree.
 */
bool ExecSupportsBackwardScan(Plan* node)
{
    if (node == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_BaseResult:
            if (outerPlan(node) != NULL) {
                return ExecSupportsBackwardScan(outerPlan(node)) && target_list_supports_backward_scan(node->targetlist);
            } else {
                return false;
            }

        case T_Append: {
            ListCell* l = NULL;

            foreach (l, ((Append*)node)->appendplans) {
                if (!ExecSupportsBackwardScan((Plan*)lfirst(l))) {
                    return false;
                }
            }
            /* need not check tlist because Append doesn't evaluate it */
            return true;
        }

        case T_SeqScan:
            /* Tablesample scan cann't support backward scan. */
            if (((SeqScan*)node)->tablesample) {
                return false;
            }
            /* fall through */
        case T_TidScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
            return target_list_supports_backward_scan(node->targetlist);

        case T_IndexScan:
            return index_supports_backward_scan(((IndexScan*)node)->indexid) &&
                   target_list_supports_backward_scan(node->targetlist);

        case T_IndexOnlyScan:
            return index_supports_backward_scan(((IndexOnlyScan*)node)->indexid) &&
                   target_list_supports_backward_scan(node->targetlist);

        case T_SubqueryScan:
            return ExecSupportsBackwardScan(((SubqueryScan*)node)->subplan) &&
                   target_list_supports_backward_scan(node->targetlist);

        case T_ExtensiblePlan:
            return ((ExtensiblePlan *)node)->flags & EXTENSIBLEPATH_SUPPORT_BACKWARD_SCAN;

        case T_Material:
        case T_Sort:
            /* these don't evaluate tlist */
            return true;

        case T_LockRows:
        case T_Limit:
            /* these don't evaluate tlist */
            return ExecSupportsBackwardScan(outerPlan(node));

        default:
            return false;
    }
}

/*
 * If the tlist contains set-returning functions, we can't support backward
 * scan, because the TupFromTlist code is direction-ignorant.
 */
static bool target_list_supports_backward_scan(List* targetlist)
{
    if (expression_returns_set((Node*)targetlist)) {
        return false;
    }
    return true;
}

/*
 * An IndexScan or IndexOnlyScan node supports backward scan only if the
 * index's AM does.
 */
static bool index_supports_backward_scan(Oid indexid)
{
    bool result = false;
    HeapTuple ht_idxrel;
    HeapTuple ht_am;
    Form_pg_class idxrelrec;
    Form_pg_am amrec;

    /* Fetch the pg_class tuple of the index relation */
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexid));
    if (!HeapTupleIsValid(ht_idxrel)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
             errmsg("cache lookup failed for relation %u when check backward scan for Index.", indexid)));
    }
    idxrelrec = (Form_pg_class)GETSTRUCT(ht_idxrel);

    /* Fetch the pg_am tuple of the index' access method */
    ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
    if (!HeapTupleIsValid(ht_am)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
             errmsg("cache lookup failed for access method %u when test backward scan for Index %s",
                    idxrelrec->relam,
                    NameStr(idxrelrec->relname))));
    }
    amrec = (Form_pg_am)GETSTRUCT(ht_am);

    result = amrec->amcanbackward;

    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);

    return result;
}

/*
 * ExecMaterializesOutput - does a plan type materialize its output?
 *
 * Returns true if the plan node type is one that automatically materializes
 * its output (typically by keeping it in a tuplestore).  For such plans,
 * a rescan without any parameter change will have zero startup cost and
 * very low per-tuple cost.
 */
bool ExecMaterializesOutput(NodeTag plantype)
{
    switch (plantype) {
        case T_Material:
        case T_FunctionScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_Sort:
            return true;

        default:
            break;
    }

    return false;
}
