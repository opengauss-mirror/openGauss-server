/* -------------------------------------------------------------------------
 *
 * execProcnode.cpp
 *	 contains dispatch functions which call the appropriate "initialize",
 *	 "get a tuple", and "cleanup" routines for the given node type.
 *	 If the node has children, then it will presumably call ExecInitNode,
 *	 ExecProcNode, or ExecEndNode on its subnodes and do the appropriate
 *	 processing.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execProcnode.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecInitNode	-		initialize a plan node and its subplans
 *		ExecProcNode	-		get a tuple by executing the plan node
 *		ExecEndNode		-		shut down a plan node and its subplans
 *
 *	 NOTES
 *		This used to be three files.  It is now all combined into
 *		one file so that it is easier to keep ExecInitNode, ExecProcNode,
 *		and ExecEndNode in sync when new nodes are added.
 *
 *	 EXAMPLE
 *		Suppose we want the age of the manager of the shoe department and
 *		the number of employees in that department.  So we have the query:
 *
 *				select DEPT.no_emps, EMP.age
 *				where EMP.name = DEPT.mgr and
 *					  DEPT.name = "shoe"
 *
 *		Suppose the planner gives us the following plan:
 *
 *						Nest Loop (DEPT.mgr = EMP.name)
 *						/		\
 *					   /		 \
 *				   Seq Scan		Seq Scan
 *					DEPT		  EMP
 *				(name = "shoe")
 *
 *		ExecutorStart() is called first.
 *		It calls InitPlan() which calls ExecInitNode() on
 *		the root of the plan -- the nest loop node.
 *
 *	  * ExecInitNode() notices that it is looking at a nest loop and
 *		as the code below demonstrates, it calls ExecInitNestLoop().
 *		Eventually this calls ExecInitNode() on the right and left subplans
 *		and so forth until the entire plan is initialized.	The result
 *		of ExecInitNode() is a plan state tree built with the same structure
 *		as the underlying plan tree.
 *
 *	  * Then when ExecutorRun() is called, it calls ExecutePlan() which calls
 *		ExecProcNode() repeatedly on the top node of the plan state tree.
 *		Each time this happens, ExecProcNode() will end up calling
 *		ExecNestLoop(), which calls ExecProcNode() on its subplans.
 *		Each of these subplans is a sequential scan so ExecSeqScan() is
 *		called.  The slots returned by ExecSeqScan() may contain
 *		tuples which contain the attributes ExecNestLoop() uses to
 *		form the tuples it returns.
 *
 *	  * Eventually ExecSeqScan() stops returning tuples and the nest
 *		loop join ends.  Lastly, ExecutorEnd() calls ExecEndNode() which
 *		calls ExecEndNestLoop() which in turn calls ExecEndNode() on
 *		its subplans which result in ExecEndSeqScan().
 *
 *		This should show how the executor works by having
 *		ExecInitNode(), ExecProcNode() and ExecEndNode() dispatch
 *		their work to the appopriate node support routines which may
 *		in turn call these routines themselves on their subplans.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
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
#include "executor/node/nodeStub.h"
#include "executor/node/nodeSubplan.h"
#include "executor/node/nodeSubqueryscan.h"
#include "executor/node/nodeTidscan.h"
#include "executor/node/nodeUnique.h"
#include "executor/node/nodeValuesscan.h"
#include "executor/node/nodeWindowAgg.h"
#include "executor/node/nodeWorktablescan.h"
#include "executor/exec/execStream.h"
#include "optimizer/clauses.h"
#include "optimizer/encoding.h"
#include "optimizer/ml_model.h"
#include "vecexecutor/vecstream.h"
#include "miscadmin.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnodecstoreindexscan.h"
#include "vecexecutor/vecnodedfsindexscan.h"
#include "vecexecutor/vecnodevectorow.h"
#include "vecexecutor/vecnodecstoreindexctidscan.h"
#include "vecexecutor/vecnodecstoreindexheapscan.h"
#include "vecexecutor/vecnodecstoreindexand.h"
#include "vecexecutor/vecnodecstoreindexor.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "vecexecutor/vecnodeforeignscan.h"
#include "vecexecutor/vecremotequery.h"
#include "vecexecutor/vecnoderesult.h"
#include "vecexecutor/vecsubqueryscan.h"
#include "vecexecutor/vecnodesort.h"
#include "vecexecutor/vecmodifytable.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vecpartiterator.h"
#include "vecexecutor/vecappend.h"
#include "vecexecutor/veclimit.h"
#include "vecexecutor/vecsetop.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "vecexecutor/vectsstorescan.h"
#include "tsdb/cache/tags_cachemgr.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "vecexecutor/vecgroup.h"
#include "vecexecutor/vecunique.h"
#include "vecexecutor/vecnestloop.h"
#include "vecexecutor/vecmaterial.h"
#include "vecexecutor/vecmergejoin.h"
#include "vecexecutor/vecwindowagg.h"
#include "utils/aes.h"
#include "utils/builtins.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#endif
#include "distributelayer/streamMain.h"
#include "pgxc/pgxc.h"
#include "securec.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "executor/node/nodeTrainModel.h"

#define NODENAMELEN 64

/*
 * Function to determine a plannode should be processed in stub-routine when exec_nodes
 * does not match current DN.
 *
 * The term of "processed in stub" means we need let ExecNodeInit() bypass the actual
 * initilaization work like open scanrel, instead allow NodeInit work to continue on its
 * lefttree/righttree
 */
bool NeedStubExecution(Plan* plan)
{
#ifndef ENABLE_MULTIPLE_NODES
    return false;
#endif
    /* If a plan node is under recursive union, we don't consider stub execution */
    if (EXEC_IN_RECURSIVE_MODE(plan)) {
        return false;
    }

    /* First, determine if this plan step needs excution on current dn */
    if (NeedExecute(plan)) {
        return false;
    }

    /* Second, determine if this plan step need stub processing */
    switch (nodeTag(plan)) {
        case T_ModifyTable:
        case T_VecModifyTable:
        case T_Scan:
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapIndexScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_SubqueryScan:
        case T_FunctionScan:
            return true;
        default:
            return false;          
    }
}

/*
 * not need execute active sql if the datanode don't run in multi-nodegroup.
 */
static bool NeedExecuteActiveSql(Plan* plan)
{
    if ((!IS_PGXC_COORDINATOR) && (!IS_SINGLE_NODE) && false == NeedExecute(plan)) {
        return false;
    }

    return true;
}

static inline bool SeqScanNodeIsStub(SeqScanState* seq_scan)
{
    return seq_scan->ss_currentScanDesc == NULL;
}

static inline bool IdxScanNodeIsStub(IndexScanState* index_scan)
{
    return index_scan->iss_ScanDesc == NULL;
}

static inline bool IdxOnlyScanNodeIsStub(IndexOnlyScanState* index_only_scan)
{
    return index_only_scan->ioss_ScanDesc == NULL;
}

static inline bool BmIdxOnlyScanNodeIsStub(BitmapIndexScanState* bm_index_scan)
{
    return bm_index_scan->biss_ScanDesc == NULL;
}

static inline bool BmHeapScanNodeIsStub(BitmapHeapScanState* bm_heap_scan)
{
    return bm_heap_scan->ss.ss_currentScanDesc == NULL;
}

PlanState* ExecInitNodeByType(Plan* node, EState* estate, int eflags)
{
    switch (nodeTag(node)) {
        case T_BaseResult:
            return (PlanState*)ExecInitResult((BaseResult*)node, estate, eflags);
        case T_ModifyTable:
            return (PlanState*)ExecInitModifyTable((ModifyTable*)node, estate, eflags);
        case T_Append:
            return (PlanState*)ExecInitAppend((Append*)node, estate, eflags);
        case T_MergeAppend:
            return (PlanState*)ExecInitMergeAppend((MergeAppend*)node, estate, eflags);
        case T_RecursiveUnion:
            return (PlanState*)ExecInitRecursiveUnion((RecursiveUnion*)node, estate, eflags);
        case T_StartWithOp:
            return (PlanState*)ExecInitStartWithOp((StartWithOp*)node, estate, eflags);
        case T_BitmapAnd:
            return (PlanState*)ExecInitBitmapAnd((BitmapAnd*)node, estate, eflags);
        case T_BitmapOr:
            return (PlanState*)ExecInitBitmapOr((BitmapOr*)node, estate, eflags);
        case T_SeqScan:
            return (PlanState*)ExecInitSeqScan((SeqScan*)node, estate, eflags);
        case T_IndexScan:
            return (PlanState*)ExecInitIndexScan((IndexScan*)node, estate, eflags);
        case T_IndexOnlyScan:
            return (PlanState*)ExecInitIndexOnlyScan((IndexOnlyScan*)node, estate, eflags);
        case T_BitmapIndexScan:
            return (PlanState*)ExecInitBitmapIndexScan((BitmapIndexScan*)node, estate, eflags);
        case T_BitmapHeapScan:
            return (PlanState*)ExecInitBitmapHeapScan((BitmapHeapScan*)node, estate, eflags);
        case T_TidScan:
            return (PlanState*)ExecInitTidScan((TidScan*)node, estate, eflags);
        case T_SubqueryScan:
            return (PlanState*)ExecInitSubqueryScan((SubqueryScan*)node, estate, eflags);
        case T_FunctionScan:
            return (PlanState*)ExecInitFunctionScan((FunctionScan*)node, estate, eflags);
        case T_ValuesScan:
            return (PlanState*)ExecInitValuesScan((ValuesScan*)node, estate, eflags);
        case T_CteScan:
            return (PlanState*)ExecInitCteScan((CteScan*)node, estate, eflags);
        case T_WorkTableScan:
            return (PlanState*)ExecInitWorkTableScan((WorkTableScan*)node, estate, eflags);
        case T_ForeignScan:
            return (PlanState*)ExecInitForeignScan((ForeignScan*)node, estate, eflags);
        case T_ExtensiblePlan:
            return (PlanState*)ExecInitExtensiblePlan((ExtensiblePlan*)node, estate, eflags);
        case T_NestLoop:
            return (PlanState*)ExecInitNestLoop((NestLoop*)node, estate, eflags);
        case T_MergeJoin:
            return (PlanState*)ExecInitMergeJoin((MergeJoin*)node, estate, eflags);
        case T_HashJoin:
            return (PlanState*)ExecInitHashJoin((HashJoin*)node, estate, eflags);
        case T_Material:
            return (PlanState*)ExecInitMaterial((Material*)node, estate, eflags);
        case T_Sort:
            return (PlanState*)ExecInitSort((Sort*)node, estate, eflags);
        case T_Group:
            return (PlanState*)ExecInitGroup((Group*)node, estate, eflags);
        case T_Agg:
            return (PlanState*)ExecInitAgg((Agg*)node, estate, eflags);
        case T_WindowAgg:
            return (PlanState*)ExecInitWindowAgg((WindowAgg*)node, estate, eflags);
        case T_Unique:
            return (PlanState*)ExecInitUnique((Unique*)node, estate, eflags);
        case T_Hash:
            return (PlanState*)ExecInitHash((Hash*)node, estate, eflags);
        case T_SetOp:
            return (PlanState*)ExecInitSetOp((SetOp*)node, estate, eflags);
        case T_LockRows:
            return (PlanState*)ExecInitLockRows((LockRows*)node, estate, eflags);
        case T_Limit:
            return (PlanState*)ExecInitLimit((Limit*)node, estate, eflags);
        case T_PartIterator:
            return (PlanState*)ExecInitPartIterator((PartIterator*)node, estate, eflags);
        case T_Stream:
            return (PlanState*)ExecInitStream((Stream*)node, estate, eflags);
#ifdef PGXC
        case T_RemoteQuery:
            return (PlanState*)ExecInitRemoteQuery((RemoteQuery*)node, estate, eflags);
#endif

        case T_VecToRow:
            return (PlanState*)ExecInitVecToRow((VecToRow*)node, estate, eflags);
        case T_RowToVec:
            return (PlanState*)ExecInitRowToVec((RowToVec*)node, estate, eflags);
        case T_VecRemoteQuery:
            return (PlanState*)ExecInitVecRemoteQuery((VecRemoteQuery*)node, estate, eflags);
        case T_VecStream:
            return (PlanState*)ExecInitVecStream((Stream*)node, estate, eflags);
        case T_CStoreScan:
            return (PlanState*)ExecInitCStoreScan((CStoreScan*)node, NULL, estate, eflags);
        case T_DfsScan:
            return (PlanState*)ExecInitDfsScan((DfsScan*)node, NULL, estate, eflags);
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            return (PlanState*)ExecInitTsStoreScan((TsStoreScan *)node, NULL, estate, eflags);
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_VecHashJoin:
            return (PlanState*)ExecInitVecHashJoin((VecHashJoin*)node, estate, eflags);
        case T_VecAgg:
            return (PlanState*)ExecInitVecAggregation((VecAgg*)node, estate, eflags);
        case T_DfsIndexScan:
            return (PlanState*)ExecInitDfsIndexScan((DfsIndexScan*)node, estate, eflags);
        case T_CStoreIndexScan:
            return (PlanState*)ExecInitCstoreIndexScan((CStoreIndexScan*)node, estate, eflags);
        case T_CStoreIndexCtidScan:
            return (PlanState*)ExecInitCstoreIndexCtidScan((CStoreIndexCtidScan*)node, estate, eflags);
        case T_CStoreIndexHeapScan:
            return (PlanState*)ExecInitCstoreIndexHeapScan((CStoreIndexHeapScan*)node, estate, eflags);
        case T_CStoreIndexAnd:
            return (PlanState*)ExecInitCstoreIndexAnd((CStoreIndexAnd*)node, estate, eflags);
        case T_CStoreIndexOr:
            return (PlanState*)ExecInitCstoreIndexOr((CStoreIndexOr*)node, estate, eflags);
        case T_VecSort:
            return (PlanState*)ExecInitVecSort((Sort*)node, estate, eflags);
        case T_VecMaterial:
            return (PlanState*)ExecInitVecMaterial((VecMaterial*)node, estate, eflags);
        case T_VecResult:
            return (PlanState*)ExecInitVecResult((VecResult*)node, estate, eflags);
        case T_VecSubqueryScan:
            return (PlanState*)ExecInitVecSubqueryScan((VecSubqueryScan*)node, estate, eflags);
        case T_VecForeignScan:
            return (PlanState*)ExecInitVecForeignScan((VecForeignScan*)node, estate, eflags);
        case T_VecModifyTable:
            return (PlanState*)ExecInitVecModifyTable((VecModifyTable*)node, estate, eflags);
        case T_VecPartIterator:
            return (PlanState*)ExecInitVecPartIterator((VecPartIterator*)node, estate, eflags);
        case T_VecAppend:
            return (PlanState*)ExecInitVecAppend((VecAppend*)node, estate, eflags);
        case T_VecGroup:
            return (PlanState*)ExecInitVecGroup((VecGroup*)node, estate, eflags);
        case T_VecLimit:
            return (PlanState*)ExecInitVecLimit((VecLimit*)node, estate, eflags);
        case T_VecUnique:
            return (PlanState*)ExecInitVecUnique((VecUnique*)node, estate, eflags);
        case T_VecSetOp:
            return (PlanState*)ExecInitVecSetOp((VecSetOp*)node, estate, eflags);
        case T_VecNestLoop:
            return (PlanState*)ExecInitVecNestLoop((VecNestLoop*)node, estate, eflags);
        case T_VecMergeJoin:
            return (PlanState*)ExecInitVecMergeJoin((VecMergeJoin*)node, estate, eflags);
        case T_VecWindowAgg:
            return (PlanState*)ExecInitVecWindowAgg((VecWindowAgg*)node, estate, eflags);
        case T_TrainModel:
            return (PlanState*)ExecInitTrainModel((TrainModel*)node, estate, eflags);
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when initializing executor.", (int)nodeTag(node))));
            return NULL; /* keep compiler quiet */
    }
}

void ExecInitNodeSubPlan(Plan* node, EState* estate, PlanState* result)
{
    List* sub_ps = NIL;
    ListCell* l = NULL;
    foreach (l, node->initPlan) {
        SubPlan* sub_plan = (SubPlan*)lfirst(l);
        SubPlanState* s_state = NULL;
        if (NULL == sub_plan)
            continue;
        Assert(IsA(sub_plan, SubPlan));
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR || estate->es_subplan_ids == NIL ||
#else
        if (StreamTopConsumerAmI() || estate->es_subplan_ids == NIL ||
#endif
            node->plan_node_id == list_nth_int(estate->es_subplan_ids, sub_plan->plan_id - 1)) {
            s_state = ExecInitSubPlan(sub_plan, result);
            if (s_state->planstate)
                sub_ps = lappend(sub_ps, s_state);
        }
    }
    result->initPlan = sub_ps;
}
/* ------------------------------------------------------------------------
 *		ExecInitNode
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
PlanState* ExecInitNode(Plan* node, EState* estate, int e_flags)
{
    PlanState* result = NULL;
    MemoryContext old_context;
    MemoryContext node_context;
    MemoryContext query_context;
    char context_name[NODENAMELEN];
    int rc = 0;

    /*
     * do nothing when we get to the end of a leaf on tree.
     */
    if (node == NULL) {
        return NULL;
    }

    gstrace_entry(GS_TRC_ID_ExecInitNode);

    if (!StreamTopConsumerAmI())
        rc = snprintf_s(context_name,
            NODENAMELEN,
            NODENAMELEN - 1,
            "%s_%lu",
            nodeTagToString(nodeTag(node)),
            t_thrd.proc_cxt.MyProcPid);
    else
        rc = snprintf_s(context_name,
            NODENAMELEN,
            NODENAMELEN - 1,
            "%s_%lu_%d",
            nodeTagToString(nodeTag(node)),
            estate->es_plannedstmt->queryId,
            node->plan_node_id);
    securec_check_ss(rc, "", "");

    /*
     * Create working memory for expression evaluation in this context.
     */
    node_context = AllocSetContextCreate(estate->es_const_query_cxt,
        context_name,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    query_context = estate->es_query_cxt;

    // reassign the node context as we must run under this context.
    estate->es_query_cxt = node_context;

    /* Switch to Node Level Memory Context */
    old_context = MemoryContextSwitchTo(node_context);

    /*
     * Check whether this 'plan node' needs be processed in current DN exec_nodes,
     * skip real initialization if it is not in exec-nodes
     *
     * Note: We only have to do such kind of specialy pocessing in some plan nodes
     */
    if (unlikely(IS_PGXC_DATANODE && NeedStubExecution(node))) {
        result = (PlanState*)ExecInitNodeStubNorm(node, estate, e_flags);
    } else {       
        result = ExecInitNodeByType(node, estate, e_flags);
    }

    /* Set the nodeContext */
    result->nodeContext = node_context;

    /*
     * Initialize any initPlans present in this node.  The planner put them in
     * a separate list for us.
     */

    /*
     * We initialize subplan node on coordinator (for explain) or one dn thread
     * that executes the subplan
     */
    ExecInitNodeSubPlan(node, estate, result);

    /* Set up instrumentation for this node if requested */
    if (estate->es_instrument != INSTRUMENT_NONE) {
#ifdef ENABLE_MULTIPLE_NODES
        /*
         * "plan_node_id == 0" is special case, "with recursive + hdfs foreign table"
         * will lead to plan_node_id of all plan node in subplan are zero.
         * u_sess->instr_cxt.thread_instr->allocInstrSlot only return the instrArray->instr->instrPlanData
         * which has allocated in threadinstrumentation.
         */
        if (u_sess->instr_cxt.global_instr != NULL && u_sess->instr_cxt.thread_instr && node->plan_node_id > 0 &&
            IS_PGXC_COORDINATOR && StreamTopConsumerAmI()) {
            /* on compute pool */
            result->instrument = u_sess->instr_cxt.thread_instr->allocInstrSlot(
                node->plan_node_id, node->parent_node_id, result->plan, estate);
        } else if (u_sess->instr_cxt.global_instr != NULL && u_sess->instr_cxt.thread_instr && node->plan_node_id > 0 &&
                 (IS_PGXC_DATANODE || (IS_PGXC_COORDINATOR && node->exec_type == EXEC_ON_COORDS))) {
            /* plannode(exec on cn)or dn */
            result->instrument = u_sess->instr_cxt.thread_instr->allocInstrSlot(
                node->plan_node_id, node->parent_node_id, result->plan, estate);
        } else {
            /* on MPPDB CN */
            result->instrument = InstrAlloc(1, estate->es_instrument);
        }
#else
        if (u_sess->instr_cxt.global_instr != NULL && u_sess->instr_cxt.thread_instr && node->plan_node_id > 0 &&
            (!StreamTopConsumerAmI() ||
            u_sess->instr_cxt.global_instr->get_planIdOffsetArray()[node->plan_node_id - 1] == 0)) {
            result->instrument = u_sess->instr_cxt.thread_instr->allocInstrSlot(
                node->plan_node_id, node->parent_node_id, result->plan, estate);
        } else {
            result->instrument = InstrAlloc(1, estate->es_instrument);
        }
#endif
        if (result->instrument) {
            result->instrument->memoryinfo.nodeContext = node_context;

            if (u_sess->attr.attr_resource.use_workload_manager &&
                u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR &&
                estate->es_can_realtime_statistics && u_sess->exec_cxt.need_track_resource &&
                NeedExecuteActiveSql(node)) {
                Qpid qid;
                qid.plannodeid = node->plan_node_id;
                qid.procId = u_sess->instr_cxt.gs_query_id->procId;
                qid.queryId = u_sess->instr_cxt.gs_query_id->queryId;
                int plan_dop = node->parallel_enabled ? u_sess->opt_cxt.query_dop : 1;
                result->instrument->dop = plan_dop;

                int64 plan_rows = e_rows_convert_to_int64(node->plan_rows);
                if (nodeTag(node) == T_VecAgg &&
                    ((Agg*)node)->aggstrategy == AGG_HASHED && ((VecAgg*)node)->is_sonichash) {
                    ExplainCreateDNodeInfoOnDN(&qid,
                        result->instrument,
                        node->exec_type == EXEC_ON_DATANODES,
                        "VectorSonicHashAgg",
                        plan_dop,
                        plan_rows);
                } else if (nodeTag(node) == T_VecHashJoin && ((HashJoin*)node)->isSonicHash) {
                    ExplainCreateDNodeInfoOnDN(&qid,
                        result->instrument,
                        node->exec_type == EXEC_ON_DATANODES,
                        "VectorSonicHashJoin",
                        plan_dop,
                        plan_rows);
                } else {
                    ExplainCreateDNodeInfoOnDN(&qid,
                        result->instrument,
                        node->exec_type == EXEC_ON_DATANODES,
                        nodeTagToString(nodeTag(node)),
                        plan_dop,
                        plan_rows);
                }
            }
        }
    }

    /* Switch to OldContext */
    MemoryContextSwitchTo(old_context);

    /* restore the per query context */
    estate->es_query_cxt = query_context;
    result->ps_rownum = 0;

    gstrace_exit(GS_TRC_ID_ExecInitNode);
    return result;
}

TupleTableSlot* ExecProcNodeByType(PlanState* node)
{
    TupleTableSlot* result = NULL;
    switch (nodeTag(node)) {
        case T_ResultState:
            return ExecResult((ResultState*)node);
        case T_ModifyTableState:
        case T_DistInsertSelectState:
            return ExecModifyTable((ModifyTableState*)node);
        case T_AppendState:
            return ExecAppend((AppendState*)node);
        case T_MergeAppendState:
            return ExecMergeAppend((MergeAppendState*)node);
        case T_RecursiveUnionState:
            return ExecRecursiveUnion((RecursiveUnionState*)node);
        case T_StartWithOpState:
            return ExecStartWithOp((StartWithOpState*)node);
        case T_SeqScanState:
            return ExecSeqScan((SeqScanState*)node);
        case T_IndexScanState:
            return ExecIndexScan((IndexScanState*)node);
        case T_IndexOnlyScanState:
            return ExecIndexOnlyScan((IndexOnlyScanState*)node);
        case T_BitmapHeapScanState:
            return ExecBitmapHeapScan((BitmapHeapScanState*)node);
        case T_TidScanState:
            return ExecTidScan((TidScanState*)node);
        case T_SubqueryScanState:
            return ExecSubqueryScan((SubqueryScanState*)node);
        case T_FunctionScanState:
            return ExecFunctionScan((FunctionScanState*)node);
        case T_ValuesScanState:
            return ExecValuesScan((ValuesScanState*)node);
        case T_CteScanState:
            return ExecCteScan((CteScanState*)node);
        case T_WorkTableScanState:
            return ExecWorkTableScan((WorkTableScanState*)node);
        case T_ForeignScanState:
            return ExecForeignScan((ForeignScanState*)node);
        case T_ExtensiblePlanState:
            return ExecExtensiblePlan((ExtensiblePlanState*)node);
            /*
             * join nodes
             */
        case T_NestLoopState:
            return ExecNestLoop((NestLoopState*)node);
        case T_MergeJoinState:
            return ExecMergeJoin((MergeJoinState*)node);
        case T_HashJoinState:
            return ExecHashJoin((HashJoinState*)node);

            /*
             * partition iterator node
             */
        case T_PartIteratorState:
            return ExecPartIterator((PartIteratorState*)node);
            /*
             * materialization nodes
             */
        case T_MaterialState:
            return ExecMaterial((MaterialState*)node);
        case T_SortState:
            return ExecSort((SortState*)node);
        case T_GroupState:
            return ExecGroup((GroupState*)node);
        case T_AggState:
            return ExecAgg((AggState*)node);
        case T_WindowAggState:
            return ExecWindowAgg((WindowAggState*)node);
        case T_UniqueState:
            return ExecUnique((UniqueState*)node);
        case T_HashState:
            return ExecHash();
        case T_SetOpState:
            return ExecSetOp((SetOpState*)node);
        case T_LockRowsState:
            return ExecLockRows((LockRowsState*)node);
        case T_LimitState:
            return ExecLimit((LimitState*)node);
        case T_VecToRowState:
            return ExecVecToRow((VecToRowState*)node);
#ifdef PGXC
        case T_RemoteQueryState:
            t_thrd.pgxc_cxt.GlobalNetInstr = node->instrument;
            result = ExecRemoteQuery((RemoteQueryState*)node);
            t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
            return result;
#endif
        case T_StreamState:
            t_thrd.pgxc_cxt.GlobalNetInstr = node->instrument;
            result = ExecStream((StreamState*)node);
            t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
            return result;
        case T_TrainModelState:
            return ExecTrainModel((TrainModelState*)node);
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when executing executor node.", (int)nodeTag(node))));
            return NULL;
    }
}

void ExecProcNodeInstr(PlanState* node, TupleTableSlot* result)
{
    switch (nodeTag(node)) {
        case T_ModifyTableState:
        case T_DistInsertSelectState:
            instr_time first_tuple;
            INSTR_TIME_SET_ZERO(first_tuple);
            INSTR_TIME_ACCUM_DIFF(
                first_tuple, ((ModifyTableState*)node)->first_tuple_modified, node->instrument->starttime);

            /*
             * If the value of es_last_processed is zero means the value of es_processed
             * just come from current operator.	If not means the value of es_processed
             * come from current operator and other operator, es_processed minus
             * es_last_processed is tuples processed of curent operator	when modify
             * the hdfs table, which may include modify the main table and modify the
             * detla table, in this case, the value of es_processed will be set twice,
             * resulting in error row value for modify operator in explain command.
             */
            if (node->state->es_last_processed == 0) {
                InstrStopNode(node->instrument, node->state->es_processed);
            } else {
                InstrStopNode(node->instrument, node->state->es_processed - node->state->es_last_processed);
            }

            node->state->es_last_processed = node->state->es_processed;
            node->instrument->firsttuple = INSTR_TIME_GET_DOUBLE(first_tuple);
            break;
        case T_SeqScanState:
            if (((SeqScanState*) node)->scanBatchMode) {
                if (!TupIsNull(result)) {
                    /* Batch mode does not collect memory info as it takes too much CPU resources. */
                    InstrStopNode(node->instrument, ((SeqScanState*)node)->scanBatchState->scanBatch.rows, false);
                } else {
                    InstrStopNode(node->instrument, 0.0);
                }
                break;
            }
        default:
            InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);
            break;
    }
    node->instrument->memoryinfo.operatorMemory = SET_NODEMEM(node->plan->operatorMemKB[0], node->plan->dop);

    if (TupIsNull(result))
        node->instrument->status = true;
}

typedef TupleTableSlot* (*ExecProcFuncType)(PlanState* node);

static inline TupleTableSlot *DefaultExecProc(PlanState *node)
{
    ereport(ERROR,
        (errmodule(MOD_EXECUTOR),
            errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unrecognized node type: %d when executing executor node.", (int)nodeTag(node))));
    return NULL;
}

static inline TupleTableSlot *ExecResultWrap(PlanState *node)
{
    return ExecResult((ResultState*)node);
};

static inline TupleTableSlot *ExecVecToRowWrap(PlanState *node)
{
    return ExecVecToRow((VecToRowState*)node);
}

static inline TupleTableSlot *ExecModifyTableWrap(PlanState *node)
{
    return ExecModifyTable((ModifyTableState*)node);
};

static inline TupleTableSlot *ExecAppendWrap(PlanState *node)
{
    return ExecAppend((AppendState*)node);
};

static inline TupleTableSlot *ExecPartIteratorWrap(PlanState *node)
{
    return ExecPartIterator((PartIteratorState*)node);
};

static inline TupleTableSlot *ExecMergeAppendWrap(PlanState *node)
{
    return ExecMergeAppend((MergeAppendState*)node);
};

static inline TupleTableSlot *ExecRecursiveUnionWrap(PlanState *node)
{
    return ExecRecursiveUnion((RecursiveUnionState*)node);
};

static inline TupleTableSlot *ExecStartWithOpWrap(PlanState *node)
{
    return ExecStartWithOp((StartWithOpState*)node);
};

static inline TupleTableSlot *ExecSeqScanWrap(PlanState *node)
{
    return ExecSeqScan((SeqScanState *)node);
};

static inline TupleTableSlot *ExecIndexScanWrap(PlanState *node)
{
    return ExecIndexScan((IndexScanState *)node);
};

static inline TupleTableSlot *ExecIndexOnlyScanWrap(PlanState *node)
{
    return ExecIndexOnlyScan((IndexOnlyScanState *)node);
};

static inline TupleTableSlot *ExecBitmapHeapScanWrap(PlanState *node)
{
    return ExecBitmapHeapScan((BitmapHeapScanState *)node);
};

static inline TupleTableSlot *ExecTidScanWrap(PlanState *node)
{
    return ExecTidScan((TidScanState *)node);
};

static inline TupleTableSlot *ExecSubqueryScanWrap(PlanState *node)
{
    return ExecSubqueryScan((SubqueryScanState *)node);
};

static inline TupleTableSlot *ExecFunctionScanWrap(PlanState *node)
{
    return ExecFunctionScan((FunctionScanState *)node);
};

static inline TupleTableSlot *ExecValuesScanWrap(PlanState *node)
{
    return ExecValuesScan((ValuesScanState *)node);
};

static inline TupleTableSlot *ExecCteScanWrap(PlanState *node)
{
    return ExecCteScan((CteScanState *)node);
};

static inline TupleTableSlot *ExecWorkTableScanWrap(PlanState *node)
{
    return ExecWorkTableScan((WorkTableScanState *)node);
};

static inline TupleTableSlot *ExecForeignScanWrap(PlanState *node)
{
    return ExecForeignScan((ForeignScanState *)node);
};

static inline TupleTableSlot *ExecExtensiblePlanWrap(PlanState *node)
{
    return ExecExtensiblePlan((ExtensiblePlanState *)node);
};

static inline TupleTableSlot *ExecNestLoopWrap(PlanState *node)
{
    return ExecNestLoop((NestLoopState *)node);
};

static inline TupleTableSlot *ExecMergeJoinWrap(PlanState *node)
{
    return ExecMergeJoin((MergeJoinState *)node);
};

static inline TupleTableSlot *ExecHashJoinWrap(PlanState *node)
{
    return ExecHashJoin((HashJoinState *)node);
};

static inline TupleTableSlot *ExecMaterialWrap(PlanState *node)
{
    return ExecMaterial((MaterialState *)node);
};

static inline TupleTableSlot *ExecSortWrap(PlanState *node)
{
    return ExecSort((SortState *)node);
};

static inline TupleTableSlot *ExecGroupWrap(PlanState *node)
{
    return ExecGroup((GroupState *)node);
};

static inline TupleTableSlot *ExecAggWrap(PlanState *node)
{
    return ExecAgg((AggState *)node);
};

static inline TupleTableSlot *ExecWindowAggWrap(PlanState *node)
{
    return ExecWindowAgg((WindowAggState *)node);
};

static inline TupleTableSlot *ExecUniqueWrap(PlanState *node)
{
    return ExecUnique((UniqueState *)node);
};

static inline TupleTableSlot *ExecHashWrap(PlanState *node)
{
    return ExecHash();
};

static inline TupleTableSlot *ExecSetOpWrap(PlanState *node)
{
    return ExecSetOp((SetOpState *)node);
};

static TupleTableSlot *ExecLockRowsWrap(PlanState *node)
{
    return ExecLockRows((LockRowsState *)node);
};

static inline TupleTableSlot *ExecLimitWrap(PlanState *node)
{
    return ExecLimit((LimitState *)node);
};

static inline TupleTableSlot *ExecRemoteQueryWrap(PlanState *node)
{
    return ExecRemoteQuery((RemoteQueryState *)node);
};

static inline TupleTableSlot *ExecTrainModelWrap(PlanState *node)
{
   return ExecTrainModel((TrainModelState*)node);
}

static inline TupleTableSlot *ExecStreamWrap(PlanState *node)
{
    return ExecStream((StreamState *)node);
};

ExecProcFuncType g_execProcFuncTable[] = {
    ExecResultWrap,
    ExecVecToRowWrap,
    DefaultExecProc,
    ExecModifyTableWrap,
    ExecModifyTableWrap,
    ExecAppendWrap,
    ExecPartIteratorWrap,
    ExecMergeAppendWrap,
    ExecRecursiveUnionWrap,
    ExecStartWithOpWrap,
    DefaultExecProc,
    DefaultExecProc,
    DefaultExecProc,
    ExecSeqScanWrap,
    ExecIndexScanWrap,
    ExecIndexOnlyScanWrap,
    DefaultExecProc,
    ExecBitmapHeapScanWrap,
    ExecTidScanWrap,
    ExecSubqueryScanWrap,
    ExecFunctionScanWrap,
    ExecValuesScanWrap,
    ExecCteScanWrap,
    ExecWorkTableScanWrap,
    ExecForeignScanWrap,
    ExecExtensiblePlanWrap,
    DefaultExecProc,
    ExecNestLoopWrap,
    ExecMergeJoinWrap,
    ExecHashJoinWrap,
    ExecMaterialWrap,
    ExecSortWrap,
    ExecGroupWrap,
    ExecAggWrap,
    ExecWindowAggWrap,
    ExecUniqueWrap,
    ExecHashWrap,
    ExecSetOpWrap,
    ExecLockRowsWrap,
    ExecLimitWrap,
    ExecRemoteQueryWrap,
    ExecTrainModelWrap,
    ExecStreamWrap
};

/* ----------------------------------------------------------------
 *		ExecProcNode
 *
 *		Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecProcNode(PlanState* node)
{
    TupleTableSlot* result = NULL;

    CHECK_FOR_INTERRUPTS();
    MemoryContext old_context;

    /* Response to stop or cancel signal. */
#ifdef ENABLE_MULTIPLE_NODES
    if (unlikely(executorEarlyStop())) {
        return NULL;
    }
#endif

    /* Switch to Node Level Memory Context */
    old_context = MemoryContextSwitchTo(node->nodeContext);

    if (node->chgParam != NULL) { /* something changed */
        ExecReScan(node);       /* let ReScan handle this */
    }
    
    if (node->instrument != NULL) {
        InstrStartNode(node->instrument);
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (unlikely(planstate_need_stub(node))) {
        result = ExecProcNodeStub(node);
    } else
#endif
    {
        int index = (int)(nodeTag(node))-T_ResultState;
        Assert(index >= 0 && index <= T_StreamState - T_ResultState);
        result = g_execProcFuncTable[index](node);
    }

    if (node->instrument != NULL) {
        ExecProcNodeInstr(node, result);
    }

    MemoryContextSwitchTo(old_context);

    node->ps_rownum++;

    return result;
}

/* ----------------------------------------------------------------
 *		MultiExecProcNode
 *
 *		Execute a node that doesn't return individual tuples
 *		(it might return a hashtable, bitmap, etc).  Caller should
 *		check it got back the expected kind of Node.
 *
 * This has essentially the same responsibilities as ExecProcNode,
 * but it does not do InstrStartNode/InstrStopNode (mainly because
 * it can't tell how many returned tuples to count).  Each per-node
 * function must provide its own instrumentation support.
 * ----------------------------------------------------------------
 */
Node* MultiExecProcNode(PlanState* node)
{
    Node* result = NULL;
    MemoryContext old_context;

    CHECK_FOR_INTERRUPTS();

    /* Switch to Node Level Memory Context */
    old_context = MemoryContextSwitchTo(node->nodeContext);

    if (node->chgParam != NULL) { /* something changed */
        ExecReScan(node);       /* let ReScan handle this */
    }

    switch (nodeTag(node)) {
            /*
             * Only node types that actually support multiexec will be listed
             */
        case T_HashState:
            result = MultiExecHash((HashState*)node);
            break;

        case T_BitmapIndexScanState:
            result = MultiExecBitmapIndexScan((BitmapIndexScanState*)node);
            break;

        case T_BitmapAndState:
            result = MultiExecBitmapAnd((BitmapAndState*)node);
            break;

        case T_BitmapOrState:
            result = MultiExecBitmapOr((BitmapOrState*)node);
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when executing multi executor node.", (int)nodeTag(node))));
            result = NULL;
            break;
    }

    /* Print Operator Memory for Hash operator */
    if (node->instrument) {
        node->instrument->memoryinfo.operatorMemory = node->plan->operatorMemKB[0];
    }

    MemoryContextSwitchTo(old_context);

    return result;
}

void ExplainNodePending(PlanState* result_plan)
{
    if (!u_sess->attr.attr_resource.use_workload_manager ||
        u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_OPERATOR || result_plan == NULL) {
            return;
        }

    if ((!IS_PGXC_COORDINATOR || IsConnFromCoord()) && !IS_SINGLE_NODE) {
        return;
    }

    bool has_found = false;
    Qpid qid;
    int rc = 0;

    qid.procId = u_sess->instr_cxt.gs_query_id->procId;
    qid.queryId = u_sess->instr_cxt.gs_query_id->queryId;
    qid.plannodeid = result_plan->plan->plan_node_id;

    if (IsQpidInvalid(&qid)) {
        return;
    }

    uint32 hash_code = GetHashPlanCode(&qid, sizeof(Qpid));

    LockOperHistHashPartition(hash_code, LW_EXCLUSIVE);
    ExplainDNodeInfo* p_detail =
        (ExplainDNodeInfo*)hash_search(g_operator_table.collected_info_hashtbl, &qid, HASH_ENTER, &has_found);
    if (p_detail != NULL) {
        if (has_found) {
            ereport(LOG,
                (errmsg("History Trace Error: The new information has the same hash key as the existed record in "
                        "the hash table, which is not expected.")));
        }

        rc = memset_s(p_detail, sizeof(ExplainDNodeInfo), 0, sizeof(ExplainDNodeInfo));
        securec_check(rc, "\0", "\0");

        p_detail->qid = qid;
        p_detail->status = Operator_Pending;
    } else {
        ereport(LOG, (errmsg("History Trace Error: Cannot alloc memory, out of memory!")));
    }

    UnLockOperHistHashPartition(hash_code);
}

void ExplainNodeFinish(PlanState* result_plan, PlannedStmt *pstmt, TimestampTz current_time, bool is_pending)
{
    if (!u_sess->attr.attr_resource.use_workload_manager ||
        u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_OPERATOR || result_plan == NULL ||
        !NeedExecuteActiveSql(result_plan->plan)) {
            return;
        }

    if (result_plan->instrument != NULL && result_plan->state->es_can_history_statistics) {
        int plan_dop = result_plan->instrument->dop;

        if (is_pending) {
            ExplainNodePending(result_plan);
        } else {
            int64 plan_rows = e_rows_convert_to_int64(result_plan->plan->plan_rows);
            Plan* node = result_plan->plan;
            char *plan_name = NULL;

            if (nodeTag(node) == T_VecAgg && ((Agg*)node)->aggstrategy == AGG_HASHED && ((VecAgg*)node)->is_sonichash) {
                plan_name = "VectorSonicHashAgg";
            } else if (nodeTag(node) == T_VecHashJoin && ((HashJoin*)node)->isSonicHash) {
                plan_name = "VectorSonicHashJoin";
            } else {
                plan_name = nodeTagToString(nodeTag(node));
            }

            OperatorPlanInfo* opt_plan_info = NULL;
#ifndef ENABLE_MULTIPLE_NODES
            if (pstmt != NULL)
                opt_plan_info = ExtractOperatorPlanInfo(result_plan, pstmt);
#endif /* ENABLE_MULTIPLE_NODES */
            ExplainSetSessionInfo(result_plan->plan->plan_node_id,
                result_plan->instrument, 
                result_plan->plan->exec_type == EXEC_ON_DATANODES,
                plan_name,
                plan_dop,
                plan_rows,
                current_time,
                opt_plan_info);
        }
    }

    switch (nodeTag(result_plan->plan)) {
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppendState* ma = (MergeAppendState*)result_plan;
            for (int i = 0; i < ma->ms_nplans; i++) {
                PlanState* plan = ma->mergeplans[i];
                ExplainNodeFinish(plan, pstmt, current_time, is_pending);
            }
        } break;
        case T_Append:
        case T_VecAppend: {
            AppendState* append = (AppendState*)result_plan;
            for (int i = 0; i < append->as_nplans; i++) {
                PlanState* plan = append->appendplans[i];
                ExplainNodeFinish(plan, pstmt, current_time, is_pending);
            }
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTableState* mt = (ModifyTableState*)result_plan;
            for (int i = 0; i < mt->mt_nplans; i++) {
                PlanState* plan = mt->mt_plans[i];
                ExplainNodeFinish(plan, pstmt, current_time, is_pending);
            }
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScanState* ss = (SubqueryScanState*)result_plan;
            if (ss->subplan)
                ExplainNodeFinish(ss->subplan, pstmt, current_time, is_pending);
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAndState* ba = (BitmapAndState*)result_plan;
            for (int i = 0; i < ba->nplans; i++) {
                PlanState* plan = ba->bitmapplans[i];
                ExplainNodeFinish(plan, pstmt, current_time, is_pending);
            }
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOrState* bo = (BitmapOrState*)result_plan;
            for (int i = 0; i < bo->nplans; i++) {
                PlanState* plan = bo->bitmapplans[i];
                ExplainNodeFinish(plan, pstmt, current_time, is_pending);
            }
        } break;
        default:
            if (result_plan->lefttree)
                ExplainNodeFinish(result_plan->lefttree, pstmt, current_time, is_pending);
            if (result_plan->righttree)
                ExplainNodeFinish(result_plan->righttree, pstmt, current_time, is_pending);
            break;
    }

    ListCell* lst = NULL;
    foreach (lst, result_plan->initPlan) {
        SubPlanState* sps = (SubPlanState*)lfirst(lst);

        if (sps->planstate == NULL) {
            continue;
        }
        ExplainNodeFinish(sps->planstate, pstmt, current_time, is_pending);
    }

    foreach (lst, result_plan->subPlan) {
        SubPlanState* sps = (SubPlanState*)lfirst(lst);

        if (sps->planstate == NULL) {
            continue;
        }
        ExplainNodeFinish(sps->planstate, pstmt, current_time, is_pending);
    }
}

/*
 * Target		: clean up sensitive information used in encryption or decryption.
 * Input		: NA
 * Output		: NA
 */
void cleanup_sensitive_information()
{
    /* used derive_keys and user_key in decryption. */
    extern THR_LOCAL bool decryption_function_call;
    extern THR_LOCAL unsigned char derive_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    extern THR_LOCAL unsigned char mac_vector_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    extern THR_LOCAL unsigned char user_input_used[NUMBER_OF_SAVED_DERIVEKEYS][RANDOM_LEN];
    /* used derive_keys and user_key in encryption. */
    extern THR_LOCAL bool encryption_function_call;
    extern THR_LOCAL unsigned char derive_vector_saved[RANDOM_LEN];
    extern THR_LOCAL unsigned char mac_vector_saved[RANDOM_LEN];
    extern THR_LOCAL unsigned char input_saved[RANDOM_LEN];
    errno_t errorno = EOK;

    if (encryption_function_call == true) {
        errorno = memset_s(derive_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check(errorno, "", "");
        errorno = memset_s(input_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check(errorno, "", "");
        errorno = memset_s(mac_vector_saved, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check(errorno, "", "");
        encryption_function_call = false;
    }
    if (decryption_function_call == true) {
        for (int i = 0; i < NUMBER_OF_SAVED_DERIVEKEYS; ++i) {
            errorno = memset_s(derive_vector_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check(errorno, "", "");
            errorno = memset_s(user_input_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check(errorno, "", "");
            errorno = memset_s(mac_vector_used[i], RANDOM_LEN, 0, RANDOM_LEN);
            securec_check(errorno, "", "");
        }
        decryption_function_call = false;
    }
}

/* ----------------------------------------------------------------
 *		ExecEndNodeByType
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.	This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
static void ExecEndNodeByType(PlanState* node)
{
    /*
     * do nothing when we get to the end of a leaf on tree.
     */

    /* clean up sensitive information used in encryption or decryption */

    /* As for data node, we should end instrument in this function,
     * but in coordinator do in the explain function.
     */
        /* on the CN of the compute pool */
    switch (nodeTag(node)) {
            /*
             * control nodes
             */
        case T_ResultState:
            ExecEndResult((ResultState*)node);
            break;

        case T_ModifyTableState:
        case T_DistInsertSelectState:
            ExecEndModifyTable((ModifyTableState*)node);
            break;

        case T_AppendState:
            ExecEndAppend((AppendState*)node);
            break;

        case T_MergeAppendState:
            ExecEndMergeAppend((MergeAppendState*)node);
            break;

        case T_RecursiveUnionState:
            ExecEndRecursiveUnion((RecursiveUnionState*)node);
            break;

        case T_StartWithOpState:
            ExecEndStartWithOp((StartWithOpState*)node);
            break;

        case T_BitmapAndState:
            ExecEndBitmapAnd((BitmapAndState*)node);
            break;

        case T_BitmapOrState:
            ExecEndBitmapOr((BitmapOrState*)node);
            break;

            /*
             * scan nodes
             */
        case T_SeqScanState:
            ExecEndSeqScan((SeqScanState*)node);
            break;
        case T_CStoreScanState:
            ExecEndCStoreScan((CStoreScanState*)node, false);
            break;

        case T_DfsScanState:
            ExecEndDfsScan((DfsScanState*)node);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScanState:
            ExecEndTsStoreScan((TsStoreScanState *)node, false);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScanState:
            ExecEndIndexScan((IndexScanState*)node);
            break;

        case T_DfsIndexScanState:
            ExecEndDfsIndexScan((DfsIndexScanState*)node);
            break;

        case T_CStoreIndexScanState:
            ExecEndCstoreIndexScan((CStoreIndexScanState*)node);
            break;

        case T_IndexOnlyScanState:
            ExecEndIndexOnlyScan((IndexOnlyScanState*)node);
            break;

        case T_BitmapIndexScanState:
            ExecEndBitmapIndexScan((BitmapIndexScanState*)node);
            break;

        case T_BitmapHeapScanState:
            ExecEndBitmapHeapScan((BitmapHeapScanState*)node);
            break;

        case T_TidScanState:
            ExecEndTidScan((TidScanState*)node);
            break;

        case T_SubqueryScanState:
            ExecEndSubqueryScan((SubqueryScanState*)node);
            break;

        case T_FunctionScanState:
            ExecEndFunctionScan((FunctionScanState*)node);
            break;

        case T_ValuesScanState:
            ExecEndValuesScan((ValuesScanState*)node);
            break;

        case T_CteScanState:
            ExecEndCteScan((CteScanState*)node);
            break;

        case T_WorkTableScanState:
            ExecEndWorkTableScan((WorkTableScanState*)node);
            break;

        case T_ForeignScanState:
            ExecEndForeignScan((ForeignScanState*)node);
            break;

        case T_ExtensiblePlanState:
            ExecEndExtensiblePlan((ExtensiblePlanState*)node);
            break;

        case T_PartIteratorState:
            ExecEndPartIterator((PartIteratorState*)node);
            break;

            /*
             * join nodes
             */
        case T_NestLoopState:
            ExecEndNestLoop((NestLoopState*)node);
            break;

        case T_MergeJoinState:
            ExecEndMergeJoin((MergeJoinState*)node);
            break;

        case T_HashJoinState:
            ExecEndHashJoin((HashJoinState*)node);
            break;

            /*
             * materialization nodes
             */
        case T_MaterialState:
            ExecEndMaterial((MaterialState*)node);
            break;

        case T_SortState:
            ExecEndSort((SortState*)node);
            break;

        case T_GroupState:
            ExecEndGroup((GroupState*)node);
            break;

        case T_AggState:
            ExecEndAgg((AggState*)node);
            break;

        case T_WindowAggState:
            ExecEndWindowAgg((WindowAggState*)node);
            break;

        case T_UniqueState:
            ExecEndUnique((UniqueState*)node);
            break;

        case T_HashState:
            ExecEndHash((HashState*)node);
            break;

        case T_SetOpState:
            ExecEndSetOp((SetOpState*)node);
            break;

        case T_LockRowsState:
            ExecEndLockRows((LockRowsState*)node);
            break;

        case T_LimitState:
            ExecEndLimit((LimitState*)node);
            break;

#ifdef PGXC
        case T_RemoteQueryState:
            ExecEndRemoteQuery((RemoteQueryState*)node);
            break;
#endif
        case T_StreamState:
            ExecEndStream((StreamState*)node);
            break;
        case T_VecStreamState:
            ExecEndVecStream((VecStreamState*)node);
            break;

        case T_VecToRowState:
            ExecEndVecToRow((VecToRowState*)node);
            break;

        case T_RowToVecState:
            ExecEndRowToVec((RowToVecState*)node);
            break;

        case T_VecHashJoinState:
            ExecEndVecHashJoin((VecHashJoinState*)node);
            break;

        case T_VecAggState:
            ExecEndVecAggregation((VecAggState*)node);
            break;

        case T_VecRemoteQueryState:
            ExecEndVecRemoteQuery((VecRemoteQueryState*)node);
            break;

        case T_VecSortState:
            ExecEndVecSort((VecSortState*)node);
            break;

        case T_VecMaterialState:
            ExecEndVecMaterial((VecMaterialState*)node);
            break;

        case T_VecResultState:
            ExecEndVecResult((VecResultState*)node);
            break;

        case T_CStoreIndexCtidScanState:
            ExecEndCstoreIndexCtidScan((CStoreIndexCtidScanState*)node);
            break;

        case T_CStoreIndexHeapScanState:
            ExecEndCstoreIndexHeapScan((CStoreIndexHeapScanState*)node);
            break;

        case T_CStoreIndexAndState:
            ExecEndCstoreIndexAnd((CStoreIndexAndState*)node);
            break;

        case T_CStoreIndexOrState:
            ExecEndCstoreIndexOr((CStoreIndexOrState*)node);
            break;

        case T_VecSubqueryScanState:
            ExecEndVecSubqueryScan((VecSubqueryScanState*)node);
            break;
        case T_VecModifyTableState:
            ExecEndVecModifyTable((VecModifyTableState*)node);
            break;
        case T_VecPartIteratorState:
            ExecEndVecPartIterator((VecPartIteratorState*)node);
            break;
        case T_VecAppendState:
            ExecEndVecAppend((VecAppendState*)node);
            break;
        case T_VecForeignScanState:
            ExecEndVecForeignScan((VecForeignScanState*)node);
            break;
        case T_VecGroupState:
            ExecEndVecGroup((VecGroupState*)node);
            break;

        case T_VecUniqueState:
            ExecEndVecUnique((VecUniqueState*)node);
            break;

        case T_VecLimitState:
            ExecEndVecLimit((VecLimitState*)node);
            break;
        case T_VecSetOpState:
            ExecEndVecSetOp((VecSetOpState*)node);
            break;

        case T_VecNestLoopState:
            ExecEndVecNestLoop((VecNestLoopState*)node);
            break;

        case T_VecMergeJoinState:
            ExecEndVecMergeJoin((VecMergeJoinState*)node);
            break;

        case T_VecWindowAggState:
            ExecEndVecWindowAgg((VecWindowAggState*)node);
            break;

        case T_TrainModelState:
            ExecEndTrainModel((TrainModelState*)node);
            break;
            
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when ending executor.", (int)nodeTag(node))));
            break;
    }
}
void ExecEndNode(PlanState* node)
{
    if (node == NULL) {
        return;
    }
    cleanup_sensitive_information();
    if (node->chgParam != NULL) {
        bms_free_ext(node->chgParam);
        node->chgParam = NULL;
    }
    if (node->instrument != NULL) {
        if (IS_PGXC_DATANODE) {
            InstrEndLoop(node->instrument);
        }
        if (NeedExecuteActiveSql(node->plan)) {
            removeExplainInfo(node->plan->plan_node_id);
        }
    }
    if (node->instrument != NULL && IS_PGXC_COORDINATOR && StreamTopConsumerAmI()) {
        InstrEndLoop(node->instrument);
    }
    if (planstate_need_stub(node)) {
        ExecEndNodeStub(node);
        return;
    }
    ExecEndNodeByType(node);
}
