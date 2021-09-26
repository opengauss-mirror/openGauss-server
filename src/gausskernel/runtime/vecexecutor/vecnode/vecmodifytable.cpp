/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * vecmodifytable.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecmodifytable.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_delete.h"
#include "access/cstore_update.h"
#include "access/dfs/dfs_delete.h"
#include "access/dfs/dfs_update.h"
#include "access/dfs/dfs_insert.h"
#include "catalog/dfsstore_ctlg.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#ifdef PGXC
#include "parser/parsetree.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#endif
#include "nodes/plannodes.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecmergeinto.h"
#include "vecexecutor/vecmodifytable.h"
#include "utils/memutils.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/storage/ts_store_insert.h"
#include "tsdb/storage/ts_store_delete.h"
#include "tsdb/cache/tags_cachemgr.h"
#include "tsdb/cache/queryid_cachemgr.h"
#include "storage/lmgr.h"
#endif   /* ENABLE_MULTIPLE_NODES */

extern void ExecVecConstraints(ResultRelInfo* result_rel_info, VectorBatch* batch, EState* estate);
extern void FlushErrorInfo(Relation rel, EState* estate, ErrorCacheEntry* cache);
extern void InsertNewFileToDfsPending(const char* file_name, Oid owner_id, uint64 file_size);
extern void ResetPendingDfsDelete();

template <class T>
VectorBatch* ExecVecInsert(VecModifyTableState* state, T* insert_op, VectorBatch* batch, VectorBatch* plan_batch,
    EState* estate, bool can_set_tag, int options)
{
    Relation result_rel_desc;
    ResultRelInfo* result_rel_info = NULL;

    /*
     * get information on the (current) result relation
     */
    result_rel_info = estate->es_result_relation_info;
    result_rel_desc = result_rel_info->ri_RelationDesc;

    /*
     * Check the constraints of the batch
     */
    if (result_rel_desc->rd_att->constr)
        ExecVecConstraints(result_rel_info, batch, estate);

    /* Insert the batch */
    insert_op->BatchInsert(batch, options);

    if (can_set_tag)
        (estate->es_processed) += batch->m_rows;

    return NULL;
}

template <class T>
static VectorBatch* ExecVecDelete(VecModifyTableState* state, T* delete_op, EState* estate, bool can_set_tag)
{
    // Workaround for those SQL (delete) unsupported by optimizer
    //
    if (unlikely(!IS_PGXC_DATANODE)) {
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("This query is not supported by optimizer in CStore")));
    }

    uint64 delete_rows = delete_op->ExecDelete();

    if (can_set_tag) {
        estate->es_processed += delete_rows;
    }

    return NULL;
}

template <class T>
VectorBatch* ExecVecUpdate(
    VecModifyTableState* state, T* update_op, VectorBatch* batch, EState* estate, bool can_set_tag, int options)
{
    uint64 updateRows = update_op->ExecUpdate(batch, options);

    if (can_set_tag) {
        (estate->es_processed) += updateRows;
    }

    return NULL;
}

template VectorBatch* ExecVecUpdate<CStoreUpdate>(VecModifyTableState* state, CStoreUpdate* update_op,
    VectorBatch* batch, EState* estate, bool canSetTag, int options);

template VectorBatch* ExecVecUpdate<DfsUpdate>(
    VecModifyTableState* state, DfsUpdate* update_op, VectorBatch* batch, EState* estate, bool can_set_tag, int options);

template VectorBatch* ExecVecInsert<CStorePartitionInsert>(VecModifyTableState* state, CStorePartitionInsert* insert_op,
    VectorBatch* batch, VectorBatch* plan_batch, EState* estate, bool can_set_tag, int options);

template VectorBatch* ExecVecInsert<CStoreInsert>(VecModifyTableState* state, CStoreInsert* insert_op,
    VectorBatch* batch, VectorBatch* plan_batch, EState* estate, bool can_set_tag, int options);

template VectorBatch* ExecVecInsert<DfsInsertInter>(VecModifyTableState* state, DfsInsertInter* insert_op,
    VectorBatch* batch, VectorBatch* plan_batch, EState* estate, bool can_set_tag, int options);

/*
 * @Description:  get table partition key data type
 * @IN relation:  table releation
 * @Return: list of partition key data type oid
 * @See also:
 */
static List* getPartitionkeyDataType(Relation relation)

{
    List* result = NIL;
    RangePartitionMap* part_map = NULL;
    int counter = -1;

    /* get partitionekey datatype's oid as a list */
    incre_partmap_refcount(relation->partMap);
    part_map = (RangePartitionMap*)(relation->partMap);

    Assert(PointerIsValid(part_map->partitionKeyDataType));
    Assert(PointerIsValid(part_map->partitionKey));

    for (counter = 0; counter < part_map->partitionKey->dim1; counter++) {
        result = lappend_oid(result, part_map->partitionKeyDataType[counter]);
    }
    decre_partmap_refcount(relation->partMap);

    return result;
}

/*
 * @Description: check table partition key is same
 * @IN/OUT insert_rel: relation for compare
 * @IN/OUT scan_rel: other relation for compare
 * @Return: true for partition key is same
 * @See also:
 */
bool checkParitionKey(Relation insert_rel, Relation scan_rel)
{
    List* insert_rel_pk = getPartitionkeyDataType(insert_rel);
    List* scan_rel_pk = getPartitionkeyDataType(scan_rel);

    ListCell* insert_cell = NULL;
    ListCell* scan_cell = NULL;
    bool is_pk_same = true;

    if (list_length(insert_rel_pk) == list_length(scan_rel_pk)) {
        forboth(insert_cell, insert_rel_pk, scan_cell, scan_rel_pk)
        {
            if (lfirst_oid(insert_cell) != lfirst_oid(scan_cell)) {
                is_pk_same = false;
                break;
            }
        }
    } else {
        is_pk_same = false;
    }

    list_free_ext(insert_rel_pk);
    list_free_ext(scan_rel_pk);

    return is_pk_same;
}

/*
 * @Description: check table partition boundary is same
 * @IN insert_rel: relation for compare
 * @IN scan_rel: other relation for compare
 * @Return: true for partition boundary is same
 * @See also:
 */
bool checkPartitionBoundary(Relation insert_rel, Relation scan_rel)
{
    int insert_rel_range_num = ((RangePartitionMap*)(insert_rel->partMap))->rangeElementsNum;
    int scan_rel_range_num = ((RangePartitionMap*)(scan_rel->partMap))->rangeElementsNum;

    List* insert_rel_boundary = NIL;
    List* scan_rel_boundary = NIL;

    if (insert_rel_range_num != scan_rel_range_num) {
        return false;
    }

    for (int seq = 0; seq < scan_rel_range_num; ++seq) {
        List* insert_rel_each_boundary = getRangePartitionBoundaryList(insert_rel, seq);
        List* scan_rel_each_boundary = getRangePartitionBoundaryList(scan_rel, seq);

        insert_rel_boundary = list_concat(insert_rel_boundary, insert_rel_each_boundary);
        scan_rel_boundary = list_concat(scan_rel_boundary, scan_rel_each_boundary);
    }

    Const* insert_const = NULL;
    Const* scan_const = NULL;
    ListCell* insert_cell = NULL;
    ListCell* scan_cell = NULL;
    bool result = true;

    forboth(insert_cell, insert_rel_boundary, scan_cell, scan_rel_boundary)
    {
        insert_const = (Const*)lfirst(insert_cell);
        scan_const = (Const*)lfirst(scan_cell);

        if (partitonKeyCompare(&insert_const, &scan_const, 1)) {
            result = false;
            break;
        }
    }

    list_free_ext(insert_rel_boundary);
    list_free_ext(scan_rel_boundary);

    return result;
}

/*
 * @Description: check plan can be optimized for CStorePartitionInsert
 * @IN plan:  subplan of VecModifyTable
 * @OUT relidIdx: scan relation index of ragne table
 * @Return: ture for plan can be optimized
 * @See also:
 */
bool checkPlanAndFindScanRelId(Plan* plan, Index* rel_id_idx)
{
    Assert(plan && rel_id_idx);

    /* check smp */
    if (plan->dop > 1) {
        return false;
    }

    switch (nodeTag(plan)) {
        case T_RowToVec:
        case T_PartIterator:
        case T_VecPartIterator: {
            if (plan->lefttree) {
                return checkPlanAndFindScanRelId(plan->lefttree, rel_id_idx);
            }
            break;
        }

        case T_SeqScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_CStoreScan: {
            /* get scan relation index of ragne table */
            Scan* scan = (Scan*)plan;
            *rel_id_idx = scan->scanrelid;
            return true;
        }

        case T_Stream:
        case T_VecStream: {
            /* check steam */
            return false;
        }

        case T_ForeignScan:
        case T_VecForeignScan: {
            /* check foreignscan */
            return false;
        }


        default: {
            ereport(LOG,
                (errmodule(MOD_VEC_EXECUTOR),
                    errmsg("checkPlanAndFindScanRelId default: %s", nodeTagToString(nodeTag(plan)))));
            return false;
        }
    }

    return false;
}

/*
 * @Description: check the partition info of insert and select relation is same
 * @IN mtstate: VecModifyTableState
 * @Return: true for same , false for other
 * @See also:
 */
bool checkInsertScanPartitionSame(VecModifyTableState* mtstate)
{
    VecModifyTable* node = (VecModifyTable*)mtstate->ps.plan;
    EState* estate = mtstate->ps.state;

    /* distributed_keys exec_nodes ? */
    /* target table */
    ResultRelInfo* result_rel_info = mtstate->resultRelInfo + mtstate->mt_whichplan;
    Relation insert_rel = result_rel_info->ri_RelationDesc;

    /* must partition table */
    if (!RELATION_IS_PARTITIONED(insert_rel)) {
        return false;
    }

    if (list_length(node->plans) != 1) {
        return false;
    }

    Plan* subplan = (Plan*)linitial(node->plans);
    Index rel_id_idx = 0;

    /* check each children plan node and get select relation index id */
    /*  only support INSERT INTO ... SELECT... */
    if (!checkPlanAndFindScanRelId(subplan, &rel_id_idx)) {
        return false;
    }

    /* source table */
    Relation scan_rel = ExecOpenScanRelation(estate, rel_id_idx);

    /* must partition table */
    if (!RELATION_IS_PARTITIONED(scan_rel)) {
        heap_close(scan_rel, NoLock);
        return false;
    }

    /* check partition key is same  */
    if (!checkParitionKey(insert_rel, scan_rel)) {
        heap_close(scan_rel, NoLock);
        return false;
    }

    /* check partition boundary is same */
    if (!checkPartitionBoundary(insert_rel, scan_rel)) {
        heap_close(scan_rel, NoLock);
        return false;
    }

    heap_close(scan_rel, NoLock);
    return true;
}

/* ----------------------------------------------------------------
 * 	 ExecInitVecModifyTable
 * ----------------------------------------------------------------
 */
VecModifyTableState* ExecInitVecModifyTable(VecModifyTable* node, EState* estate, int eflags)
{
    VecModifyTableState* mt_stat = (VecModifyTableState*)makeNode(VecModifyTableState);
    ModifyTableState* mt = ExecInitModifyTable((ModifyTable*)node, estate, eflags);
    *((ModifyTableState*)mt_stat) = *mt;
    mt_stat->ps.type = T_VecModifyTableState;
    mt_stat->ps.vectorized = true;

    if (!mt_stat->canSetTag && !(IS_PGXC_COORDINATOR && u_sess->exec_cxt.under_stream_runtime)) {
        estate->es_auxmodifytables = lcons(mt_stat, estate->es_auxmodifytables);
    }

    return mt_stat;
}

/* ----------------------------------------------------------------
 *	   RecordDataForDeleteDelta
 *     Record the delete data into delete delta table for online extension
 * ----------------------------------------------------------------
 */
static void RecordDataForDeleteDelta(
    Relation result_rel_desc, VectorBatch* plan_batch, JunkFilter* junk_filter, VecModifyTableState* node)
{
    /*
     * In case of relation in resizing and INSERT operation, we are going to fetch
     * each tuple's item pointer.
     */
    ItemPointer tuple_id = NULL;
    ScalarValue* tid_values = NULL;
    ScalarValue* partid_values = NULL;
    Oid target_rel_oid = InvalidOid;
    bool is_partitioned = RELATION_IS_PARTITIONED(result_rel_desc);
    CmdType operation = node->operation;

    if ((operation == CMD_UPDATE || operation == CMD_DELETE)) {
        /*
         * In case of UPDATE/DELETE operation, we should have junk_filter properly
         * set in ExecInitModifyTable(), so just assert it valid here.
         */
        Assert(junk_filter != NULL);

        tid_values = plan_batch->m_arr[junk_filter->jf_junkAttNo - 1].m_vals;

        if (is_partitioned) {
            partid_values = plan_batch->m_arr[junk_filter->jf_xc_part_id - 1].m_vals;
        }

        target_rel_oid = RelationGetRelid(result_rel_desc);

        for (int i = 0; i < plan_batch->m_rows; i++) {
            tuple_id = (ItemPointer)(tid_values + i);

            if (is_partitioned) {
                target_rel_oid = DatumGetObjectId(*(partid_values + i));
            }
            RecordDeletedTuple(target_rel_oid, InvalidBktId, tuple_id, node->delete_delta_rel);
        }
    }
}

void CheckTsOperation(const Relation relation, const VecModifyTableState* node) 
{
    /* only check timeseries table */
    if (!RelationIsTsStore(relation)) {
        return;
    }
    /* only support insert and delete for timeseries table */
    if (!(node->operation == CMD_INSERT || node->operation == CMD_DELETE)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("TIMESERIES store dose not support this operation")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* If operation is DELETE, get LWLOCK in datanode to block compaction producer and consumer workers */
    if (node->operation == CMD_DELETE) {
        ereport(LOG, (errmodule(MOD_TIMESERIES), errcode(ERRCODE_LOG), 
                errmsg("Set delete query id(%lu)", u_sess->debug_query_id)));           
        Tsdb::TableStatus::GetInstance().add_query(u_sess->debug_query_id);
        // add a level 5 lock to relation for compaciton Concurrency control
        LockRelationOid(relation->rd_id, ShareLock);
        // once indelete we mast refresh snapshot after we get lock incase compaction delete file
        PopActiveSnapshot();
        Snapshot tsDerleteSnapshot = GetTransactionSnapshot();
        PushActiveSnapshot(tsDerleteSnapshot);
    }
#endif   /* ENABLE_MULTIPLE_NODES */    
    return;  
}

/* ----------------------------------------------------------------
 *	   ExecVecModifyTable
 *
 *		Perform table modifications as required, and return RETURNING results
 *		if needed.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecModifyTable(VecModifyTableState* node)
{
    EState* estate = node->ps.state;
    CmdType operation = node->operation;
    ResultRelInfo* saved_result_rel_info = NULL;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_rel_desc;
    bool is_partitioned = false;
    PlanState* sub_plan_stat = NULL;
#ifdef PGXC
    PlanState* remote_rel_stat = NULL;
    PlanState* saved_result_remote_rel = NULL;
#endif
    JunkFilter* junk_filter = NULL;
    VectorBatch* batch = NULL;
    VectorBatch* plan_batch = NULL;
    const int hi_options = 0;
    void* batch_opt = NULL;
    InsertArg args;
    /* data redistribution for DFS table. */
    Relation data_dest_rel = InvalidRelation;
    /* indicates whether it is the first time to insert, delete, update or not. */
    bool is_first_modified = true;

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
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_CHECK_VIOLATION),
                errmsg("ModifyTable should not be called during EvalPlanQual")));
    }

    /*
     * If we've already completed processing, don't try to do more.  We need
     * this test because ExecPostprocessPlan might call us an extra time, and
     * our subplan's nodes aren't necessarily robust against being called
     * extra times.
     */
    if (node->mt_done) {
        return NULL;
    }

    /*
     * On first call, fire BEFORE STATEMENT triggers before proceeding.
     */
    if (node->fireBSTriggers) {
        node->fireBSTriggers = false;
    }

    if (operation == CMD_MERGE) {
        ExecVecMerge(node);
        return NULL;
    }

    /* Preload local variables */
    result_rel_info = node->resultRelInfo + node->mt_whichplan;
    sub_plan_stat = node->mt_plans[node->mt_whichplan];
#ifdef PGXC
    /* Initialize remote plan state */
    remote_rel_stat = node->mt_remoterels[node->mt_whichplan];
#endif
    junk_filter = result_rel_info->ri_junkFilter;

    result_rel_desc = result_rel_info->ri_RelationDesc;
    is_partitioned = RELATION_IS_PARTITIONED(result_rel_desc);
    /* only support insert for timeseries table */
	CheckTsOperation(result_rel_desc, node);
    /*
     * es_result_relation_info must point to the currently active result
     * relation while we are within this ModifyTable node.	Even though
     * ModifyTable nodes can't be nested statically, they can be nested
     * dynamically (since our subplan could include a reference to a modifying
     * CTE).  So we have to save and restore the caller's value.
     */
    saved_result_rel_info = estate->es_result_relation_info;
#ifdef PGXC
    saved_result_remote_rel = estate->es_result_remoterel;
#endif

    estate->es_result_relation_info = result_rel_info;
#ifdef PGXC
    estate->es_result_remoterel = remote_rel_stat;
#endif

    batch_opt = CreateOperatorObject(operation,
        is_partitioned,
        result_rel_desc,
        result_rel_info,
        estate,
        ExecGetResultType(sub_plan_stat),
        &args,
        &data_dest_rel,
        node);

    /*
     * Fetch rows from subplan(s), and execute the required table modification
     * for each row.
     */
    for (;;) {
        /*
         * Reset the per-output-tuple exprcontext.	This is needed because
         * triggers expect to use that context as workspace.  It's a bit ugly
         * to do this below the top level of the plan, however.  We might need
         * to rethink this later.
         */
        ResetPerTupleExprContext(estate);

        batch = VectorEngine(sub_plan_stat);

        if (BatchIsNull(batch)) {
            record_first_time();
            /* Flush error recored if need */
            if (node->errorRel && node->cacheEnt)
                FlushErrorInfo(node->errorRel, estate, node->cacheEnt);
            /* advance to next subplan if any */
            Assert(!((++node->mt_whichplan) < node->mt_nplans));

            break;
        }

        plan_batch = batch;
        if (junk_filter != NULL) {
            /*
             * Check node identifier for UPDATE/DELETE
             */
            if (operation == CMD_UPDATE || operation == CMD_DELETE) {
                BatchCheckNodeIdentifier(junk_filter, batch);
            }

            /*
             * Apply the junk_filter if needed.
             */
            if (operation == CMD_UPDATE) {
                batch = BatchExecFilterJunk(junk_filter, batch);
            }
        }

#ifdef PGXC
        estate->es_result_remoterel = remote_rel_stat;
#endif
        switch (operation) {
            case CMD_INSERT:
                if (is_partitioned) {
#ifdef ENABLE_MULTIPLE_NODES
                    if (RelationIsTsStore(result_rel_desc))
                        batch = ExecVecInsert<Tsdb::TsStoreInsert>(node, (Tsdb::TsStoreInsert*)batch_opt, batch,
                                                                plan_batch, estate, node->canSetTag, hi_options);
                    else
#endif
                        batch = ExecVecInsert<CStorePartitionInsert>(node, (CStorePartitionInsert*)batch_opt,
                            batch, plan_batch, estate, node->canSetTag, hi_options);
                } else {
                    if (RelationIsCUFormat(result_rel_desc)) {
                        batch = ExecVecInsert<CStoreInsert>(
                            node, (CStoreInsert*)batch_opt, batch, plan_batch, estate, node->canSetTag, hi_options);
                    } else if (RelationIsPAXFormat(result_rel_desc)) {
                        batch = ExecVecInsert<DfsInsertInter>(
                            node, (DfsInsertInter*)batch_opt, batch, plan_batch, estate, node->canSetTag, hi_options);
                    } else if (RelationIsTsStore(result_rel_desc)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("TIMESERIES store unsupport none partitioned table")));
                    } else {
                        Assert(false);
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OPERATION),
                                errmsg("\"INSERT\" is not supported by the type of relation.")));
                    }
                }
                break;

            case CMD_DELETE:
                if (RelationIsCUFormat(result_rel_desc)) {
                    ((CStoreDelete*)batch_opt)->PutDeleteBatch(batch, junk_filter);
                } else if (RelationIsPAXFormat(result_rel_desc)) {
                    ((DfsDelete*)batch_opt)->PutDeleteBatch(batch, junk_filter);
#ifdef ENABLE_MULTIPLE_NODES
                } else if (RelationIsTsStore(result_rel_desc)) {
                    (reinterpret_cast<Tsdb::TsStoreDelete*>(batch_opt))->put_delete_batch(batch, junk_filter);
#endif   /* ENABLE_MULTIPLE_NODES */                    
                } else {
                    Assert(false);
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("\"DELETE\" is not supported by the type of relation.")));
                }

                batch = NULL;
                break;

            case CMD_UPDATE:
                if (RelationIsCUFormat(result_rel_desc)) {
                    batch = ExecVecUpdate<CStoreUpdate>(
                        node, (CStoreUpdate*)batch_opt, batch, estate, node->canSetTag, hi_options);
                } else if (RelationIsPAXFormat(result_rel_desc)) {
                    batch = ExecVecUpdate<DfsUpdate>(
                        node, (DfsUpdate*)batch_opt, batch, estate, node->canSetTag, hi_options);
                } else {
                    Assert(false);
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("\"UPDATE\" is not supported by the type of relation.")));
                }
                break;

            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unknown operation")));
                break;
        }

        record_first_time();

        if (RelationInClusterResizing(result_rel_desc) && !RelationInClusterResizingReadOnly(result_rel_desc) &&
            !BatchIsNull(plan_batch)) {
            RecordDataForDeleteDelta(result_rel_desc, plan_batch, junk_filter, node);
        }

        /*
         * If we got a RETURNING result, return it to caller.  We'll continue
         * the work on next call.
         */
        if (batch != NULL) {
            estate->es_result_relation_info = saved_result_rel_info;
#ifdef PGXC
            estate->es_result_remoterel = saved_result_remote_rel;
#endif
            return batch;
        }

        if (plan_batch != NULL)
            plan_batch->Reset();
    }

    // process last data in operator cached
    switch (operation) {
        case CMD_INSERT: {
            if (is_partitioned) {
                
#ifdef ENABLE_MULTIPLE_NODES
            if (RelationIsTsStore(result_rel_desc))
                FLUSH_DATA_TSDB(batch_opt, Tsdb::TsStoreInsert);
            else
#endif   /* ENABLE_MULTIPLE_NODES */
                FLUSH_DATA(batch_opt, CStorePartitionInsert);
            } else {
                if (RelationIsCUFormat(result_rel_desc)) {
                    FLUSH_DATA(batch_opt, CStoreInsert);
                    CStoreInsert::DeInitInsertArg(args);
                } else if (RelationIsPAXFormat(result_rel_desc)) {
                    FLUSH_DATA(batch_opt, DfsInsertInter);
                    /* data redistribution for DFS table. */
                    if (u_sess->attr.attr_sql.enable_cluster_resize && data_dest_rel != NULL) {
                        /*
                         * Append old files of redistributed table table into pendingDfsDeletes.
                         */
                        InsertDeletedFilesForTransaction(data_dest_rel);
                        ExecCloseScanRelation(data_dest_rel);
                    }
                } else {
                    Assert(false);
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("\"INSERT\" is not supported by the type of relation.")));
                }
            }
        } break;

        case CMD_DELETE:
            if (RelationIsCUFormat(result_rel_desc)) {
                ExecVecDelete<CStoreDelete>(node, (CStoreDelete*)batch_opt, estate, node->canSetTag);
                DELETE_EX_TYPE(batch_opt, CStoreDelete);
            } else if (RelationIsPAXFormat(result_rel_desc)) {
                ExecVecDelete<DfsDelete>(node, (DfsDelete*)batch_opt, estate, node->canSetTag);
                DELETE_EX_TYPE(batch_opt, DfsDelete);
#ifdef ENABLE_MULTIPLE_NODES
            } else if (RelationIsTsStore(result_rel_desc)) {
                ExecVecDelete<Tsdb::TsStoreDelete>(node, reinterpret_cast<Tsdb::TsStoreDelete*>(batch_opt), estate, node->canSetTag);
                DELETE_EX_TYPE(batch_opt, Tsdb::TsStoreDelete);
#endif   /* ENABLE_MULTIPLE_NODES */                  
            } else {
                Assert(false);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("\"DELETE\" is not supported by the type of relation.")));
            }
            break;

        case CMD_UPDATE:
            if (RelationIsCUFormat(result_rel_desc)) {
                ((CStoreUpdate*)batch_opt)->EndUpdate(hi_options);
                DELETE_EX_TYPE(batch_opt, CStoreUpdate);
            } else if (RelationIsPAXFormat(result_rel_desc)) {
                ((DfsUpdate*)batch_opt)->EndUpdate(hi_options);
                DELETE_EX_TYPE(batch_opt, DfsUpdate);
            } else {
                Assert(false);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("\"UPDATE\" is not supported by the type of relation.")));
            }
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unknown operation")));
            break;
    }

    /* Restore es_result_relation_info before exiting */
    estate->es_result_relation_info = saved_result_rel_info;
#ifdef PGXC
    estate->es_result_remoterel = saved_result_remote_rel;
#endif

    /*
     * We're done, but fire AFTER STATEMENT triggers before exiting.
     */
    node->mt_done = true;

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecEndVecModifyTable
 *
 *		Shuts down the plan.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndVecModifyTable(VecModifyTableState* node)
{
    ExecEndModifyTable((ModifyTableState*)node);
}

void ExecReScanVecModifyTable(VecModifyTableState* node)
{
    /*
     * Currently, we don't need to support rescan on ModifyTable nodes. The
     * semantics of that would be a bit debatable anyway.
     */
    ereport(ERROR,
        (errmodule(MOD_VEC_EXECUTOR),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("ExecReScanVecModifyTable is not implemented")));
}

static void* insert_partiton_table(Relation result_rel_desc, VecModifyTableState* node, ResultRelInfo* result_rel_info)
{
    if (RelationIsTsStore(result_rel_desc)) {
#ifdef ENABLE_MULTIPLE_NODES
        if (!g_instance.attr.attr_common.enable_tsdb) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Please enable timeseries first!")));
        }
        Tsdb::TsStoreInsert *part_insert = New(CurrentMemoryContext) Tsdb::TsStoreInsert(result_rel_desc);
        return (void*)part_insert;
#else
        return NULL;
#endif
    } else {
        CStorePartitionInsert* part_insert = New(CurrentMemoryContext)
            CStorePartitionInsert(result_rel_desc, result_rel_info, TUPLE_SORT, false, node->ps.plan, NULL);
        bool is_same_partition = checkInsertScanPartitionSame(node);
        if (is_same_partition) {
            part_insert->SetPartitionCacheStrategy(FLASH_WHEN_SWICH_PARTITION);
            ereport(LOG,
                (errmodule(MOD_VEC_EXECUTOR), errmsg("SetPartitionCacheStrategy FLASH_WHEN_SWICH_PARTITION")));
        }
        return (void*)part_insert;
    }
}

/*
 * create operator object with operation type.
 */
void* CreateOperatorObject(CmdType operation, bool is_partitioned, Relation result_rel_desc,
    ResultRelInfo* result_rel_info, EState* estate, TupleDesc sort_tup_desc, InsertArg* args, Relation* data_dest_rel,
    VecModifyTableState* node)
{
    void* batch_opt = NULL;

    switch (operation) {
        case CMD_INSERT: {
            if (is_partitioned) {
                batch_opt = insert_partiton_table(result_rel_desc, node, result_rel_info);
            } else {
                if (RelationIsCUFormat(result_rel_desc)) {
                    CStoreInsert::InitInsertArg(result_rel_desc, result_rel_info, true, *args);
                    args->sortType = BATCH_SORT;
                    batch_opt =
                        New(CurrentMemoryContext) CStoreInsert(result_rel_desc, *args, false, node->ps.plan, NULL);
                } else if (RelationIsPAXFormat(result_rel_desc)) {
                    /* data redistribution for DFS table.
                     * In redistribution mode, we need HDFS table relation for DfsInsert object.
                     */
                    if (u_sess->attr.attr_sql.enable_cluster_resize && estate->dataDestRelIndex > 0) {
                        /* Reset pendingDfsDelete. */
                        ResetPendingDfsDelete();

                        *data_dest_rel = ExecOpenScanRelation(estate, estate->dataDestRelIndex);

                        batch_opt = CreateDfsInsert(result_rel_desc, false, *data_dest_rel, node->ps.plan, NULL);
                        ((DfsInsertInter*)batch_opt)->RegisterInsertPendingFunc(InsertNewFileToDfsPending);

                    } else {
                        batch_opt = CreateDfsInsert(result_rel_desc, false, NULL, node->ps.plan, NULL);
                    }

                    ((DfsInsertInter*)batch_opt)->BeginBatchInsert(BATCH_SORT, result_rel_info);
                } else {
                    Assert(false);
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("\"INSERT\" is not supported by the type of relation.")));
                }
            }
        } break;

        case CMD_DELETE:
            if (RelationIsCUFormat(result_rel_desc)) {
                batch_opt =
                    New(CurrentMemoryContext) CStoreDelete(result_rel_desc, estate, false, node->ps.plan, NULL);
                ((CStoreDelete*)batch_opt)->InitSortState();
            } else if (RelationIsPAXFormat(result_rel_desc)) {
                batch_opt = New(CurrentMemoryContext) DfsDelete(result_rel_desc, estate, false, node->ps.plan, NULL);
                ((DfsDelete*)batch_opt)->InitSortState();
#ifdef ENABLE_MULTIPLE_NODES
            } else if (RelationIsTsStore(result_rel_desc)) {
                batch_opt = New(CurrentMemoryContext) Tsdb::TsStoreDelete(result_rel_desc, estate, false, node->ps.plan, NULL);
                (reinterpret_cast<Tsdb::TsStoreDelete *>(batch_opt))->init_mem_arg(node->ps.plan, NULL);
                (reinterpret_cast<Tsdb::TsStoreDelete *>(batch_opt))->init_sort_state();
#endif   /* ENABLE_MULTIPLE_NODES */                  
            } else {
                Assert(false);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("\"DELETE\" is not supported by the type of relation.")));
            }
            break;

        case CMD_UPDATE:
            if (RelationIsCUFormat(result_rel_desc)) {
                batch_opt = New(CurrentMemoryContext) CStoreUpdate(result_rel_desc, estate, node->ps.plan);
                ((CStoreUpdate*)batch_opt)->InitSortState(sort_tup_desc);
            } else if (RelationIsPAXFormat(result_rel_desc)) {
                batch_opt = New(CurrentMemoryContext) DfsUpdate(result_rel_desc, estate, node->ps.plan);
                ((DfsUpdate*)batch_opt)->InitSortState(sort_tup_desc);
            } else {
                Assert(false);
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("\"UPDATE\" is not supported by the type of relation.")));
            }
            break;

        default:
            Assert(false);
            break;
    }

    return batch_opt;
}
