/* -------------------------------------------------------------------------
 *
 * optcommon.cpp
 *	   Common working fucntion to support optimizer related routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/utils/optcommon.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execStream.h"
#include "db4ai/db4ai_api.h"

/*
 * Optimizer common function that return a plan node's plain text, we wrapper it from
 * ExplainNode() in commands/explain.cpp, so treat it as a common working hourse for
 * plan node relavent utilities
 */
void GetPlanNodePlainText(
    Plan* plan, char** pname, char** sname, char** strategy, char** operation, char** pt_operation, char** pt_options)
{
    RemoteQuery* rq = NULL;
    char* extensible_name = NULL;
    switch (nodeTag(plan)) {
        case T_BaseResult:
            *pname = *sname = *pt_operation = "Result";
            break;
        case T_VecResult:
            *pname = *sname = *pt_operation = "Vector Result";
            break;
        case T_ModifyTable:
            *sname = "ModifyTable";
            *pt_operation = "MODIFY TABLE";
            switch (((ModifyTable*)plan)->operation) {
                case CMD_INSERT:
                    *pname = *operation = *pt_options = "Insert";
                    break;
                case CMD_UPDATE:
                    *pname = *operation = *pt_options = "Update";
                    break;
                case CMD_DELETE:
                    *pname = *operation = *pt_options = "Delete";
                    break;
                case CMD_MERGE:
                    *pname = *operation = *pt_options = "Merge";
                    break;
                default:
                    *pname = *pt_operation = "?\?\?";
                    break;
            }
            break;
        case T_Append:
            *pname = *sname = *pt_operation = "Append";
            break;
        case T_MergeAppend:
            *pname = *sname = *pt_operation = "Merge Append";
            break;
        case T_RecursiveUnion:
            *pname = *sname = *pt_operation = "Recursive Union";
            break;
        case T_StartWithOp:
            *pname = *sname = *pt_operation = "StartWith Operator";
            break;
        case T_BitmapAnd:
            *pname = *sname = "BitmapAnd";
            *pt_operation = "BITMAP AND";
            break;
        case T_BitmapOr:
            *pname = *sname = "BitmapOr";
            *pt_operation = "BITMAP OR";
            break;
        case T_NestLoop:
            *pname = *sname = "Nested Loop";
            *pt_operation = "NESTED LOOPS";
            break;
        case T_VecNestLoop:
            *pname = *sname = "Vector Nest Loop";
            *pt_operation = "VECTOR NESTED LOOPS";
            break;
        case T_MergeJoin:
            *pname = "Merge"; /* "Join" gets added by jointype switch */
            *sname = *pt_operation = "Merge Join";
            break;
        case T_HashJoin:
            *pname = "Hash"; /* "Join" gets added by jointype switch */
            *sname = *pt_operation = "Hash Join";
            break;
        case T_VecHashJoin:
            if (((HashJoin*)plan)->isSonicHash) {
                *pname = "Vector Sonic Hash";
                *sname = *pt_operation = "Vector Sonic Hash Join";
            } else {
                *pname = "Vector Hash";
                *sname = *pt_operation = "Vector Hash Join";
            }
            break;
        case T_SeqScan:
            *pt_operation = "TABLE ACCESS";
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned Seq Scan";
                } else {
                    *pname = *sname = *pt_options = "Seq Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned Sample Scan";
                } else {
                    *pname = *sname = *pt_options = "Sample Scan";
                }
            }
            break;
        case T_DfsScan:
            *pt_operation = "TABLE ACCESS";
            if (((Scan*)plan)->isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Dfs Scan";
            else
                *pname = *sname = *pt_options = "Dfs Scan";
            break;
        case T_CStoreScan:
            *pt_operation = "TABLE ACCESS";
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned CStore Scan";
                } else {
                    *pname = *sname = *pt_options = "CStore Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned VecSample Scan";
                } else {
                    *pname = *sname = *pt_options = "VecSample Scan";
                }
            }
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            *pt_operation = "TABLE ACCESS";
            if (!((Scan*)plan)->tablesample) {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned TsStore Scan";
                } else {
                    *pname = *sname = *pt_options = "TsStore Scan";
                }
            } else {
                if (((Scan*)plan)->isPartTbl) {
                    *pname = *sname = *pt_options = "Partitioned VecSample Scan";
                } else {
                    *pname = *sname = *pt_options = "VecSample Scan";
                }
            }
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
            *pt_operation = "INDEX";
            if (((IndexScan*)plan)->scan.isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Index Scan";
            else
                *pname = *sname = *pt_options = "Index Scan";
            break;
        case T_IndexOnlyScan:
            *pt_operation = "INDEX";
            if (((IndexOnlyScan*)plan)->scan.isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Index Only Scan";
            else
                *pname = *sname = *pt_options = "Index Only Scan";
            break;
        case T_BitmapIndexScan:
            *pt_operation = "INDEX";
            if (((BitmapIndexScan*)plan)->scan.isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Bitmap Index Scan";
            else
                *pname = *sname = *pt_options = "Bitmap Index Scan";
            break;
        case T_BitmapHeapScan:
            *pt_operation = "TABLE ACCESS";
            if (((Scan*)plan)->isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Bitmap Heap Scan";
            else
                *pname = *sname = *pt_options = "Bitmap Heap Scan";
            break;
        case T_DfsIndexScan:
            *pt_operation = "INDEX";
            if (((Scan*)plan)->isPartTbl) {
                if (((DfsIndexScan*)plan)->indexonly)
                    *pname = *sname = *pt_options = "Partitioned Dfs Index Only Scan";
                else
                    *pname = *sname = *pt_options = "Partitioned Dfs Index Scan";
            } else {
                if (((DfsIndexScan*)plan)->indexonly)
                    *pname = *sname = *pt_options = "Dfs Index Only Scan";
                else
                    *pname = *sname = "Dfs Index Scan";
            }
            break;
        case T_CStoreIndexScan:
            *pt_operation = "INDEX";
            if (((CStoreIndexScan*)plan)->scan.isPartTbl) {
                if (((CStoreIndexScan*)plan)->indexonly)
                    *pname = *sname = *pt_options = "Partitioned CStore Index Only Scan";
                else
                    *pname = *sname = *pt_options = "Partitioned CStore Index Scan";
            } else {
                if (((CStoreIndexScan*)plan)->indexonly)
                    *pname = *sname = *pt_options = "CStore Index Only Scan";
                else
                    *pname = *sname = *pt_options = "CStore Index Scan";
            }
            break;
        case T_CStoreIndexCtidScan:
            *pt_operation = "INDEX";
            if (((CStoreIndexCtidScan*)plan)->scan.isPartTbl)
                *pname = *sname = *pt_options = "Partitioned CStore Index Ctid Scan";
            else
                *pname = *sname = *pt_options = "CStore Index Ctid Scan";
            break;
        case T_CStoreIndexHeapScan:
            *pt_operation = "TABLE ACCESS";
            if (((CStoreIndexHeapScan*)plan)->scan.isPartTbl)
                *pname = *sname = *pt_options = "Partitioned CStore Index Heap Scan";
            else
                *pname = *sname = *pt_options = "CStore Index Heap Scan";
            break;
        case T_CStoreIndexAnd:
            *pt_operation = "BITMAP";
            *pname = *sname = *pt_options = "CStore Index And";
            break;
        case T_CStoreIndexOr:
            *pt_operation = "BITMAP";
            *pname = *sname = *pt_options = "CStore Index Or";
            break;
        case T_TidScan:
            *pt_operation = "TABLE ACCESS";
            if (((Scan*)plan)->isPartTbl)
                *pname = *sname = *pt_options = "Partitioned Tid Scan";
            else
                *pname = *sname = *pt_options = "Tid Scan";
            break;
        case T_SubqueryScan:
            *pname = *sname = *pt_operation = "Subquery Scan";
            break;
        case T_VecSubqueryScan:
            *pname = *sname = *pt_operation = "Vector Subquery Scan";
            break;
        case T_FunctionScan:
            *pname = *sname = *pt_operation = "Function Scan";
            break;
        case T_ValuesScan:
            *pname = *sname = *pt_operation = "Values Scan";
            break;
        case T_CteScan:
            *pname = *sname = *pt_operation = "CTE Scan";
            break;
        case T_WorkTableScan:
            *pname = *sname = *pt_operation = "WorkTable Scan";
            break;
#ifdef PGXC
        case T_RemoteQuery:
            rq = (RemoteQuery*)plan;
            if (rq->position == PLAN_ROUTER)
                *pname = *sname = "Streaming (type: PLAN ROUTER)";
            else if (rq->position == SCAN_GATHER)
                *pname = *sname = "Streaming (type: SCAN GATHER)";
            else {
                if (rq->is_simple)
                    *pname = *sname = "Streaming (type: GATHER)";
                else
                    *pname = *sname = *pt_operation = "Data Node Scan";
            }
            break;
#endif
        case T_VecRemoteQuery:
            rq = (RemoteQuery*)plan;
            Assert(rq->is_simple);
            if (rq->position == PLAN_ROUTER)
                *pname = *sname = "Vector Streaming (type: PLAN ROUTER)";
            else if (rq->position == SCAN_GATHER)
                *pname = *sname = "Vector Streaming (type: SCAN GATHER)";
            else
                *pname = *sname = "Vector Streaming (type: GATHER)";
            break;
        case T_Stream:
        case T_VecStream:
            *pname = *sname = (char*)GetStreamType((Stream*)plan);
            break;

        case T_ForeignScan:
            *pt_operation = "TABLE ACCESS";
            if (((Scan*)plan)->isPartTbl) {
                /* @hdfs
                 * Add hdfs partitioned foreign scan plan explanation.
                 */
                *pname = *sname = *pt_options = "Partitioned Foreign Scan";
            } else
                *pname = *sname = *pt_options = "Foreign Scan";
            break;
        case T_VecForeignScan:
            *pt_operation = "TABLE ACCESS";
            if (((Scan*)plan)->isPartTbl) {
                /* @hdfs
                 * Add hdfs partitioned foreign scan plan explanation.
                 */
                *pname = *sname = *pt_options = "Partitioned Vector Foreign Scan";
            } else
                *pname = *sname = *pt_options = "Vector Foreign Scan";
            break;
        case T_ExtensiblePlan:
            extensible_name = ((ExtensiblePlan*)plan)->methods->ExtensibleName;
            if (extensible_name != NULL)
                *pname = *pt_operation = *sname = extensible_name;
            else
                *pname = *pt_operation = *sname = "Extensible Plan";
            break;
        case T_Material:
            *pname = *sname = *pt_operation = "Materialize";
            break;
        case T_VecMaterial:
            *pname = *sname = *pt_operation = "Vector Materialize";
            break;
        case T_Sort:
            *pname = *sname = *pt_operation = "Sort";
            break;
        case T_VecSort:
            *pname = *sname = *pt_operation = "Vector Sort";
            break;
        case T_Group:
            *pname = *sname = *pt_operation = "Group";
            break;
        case T_VecGroup:
            *pname = *sname = *pt_operation = "Vector Group";
            break;
        case T_Agg:
            *sname = *pt_operation = "Aggregate";
            switch (((Agg*)plan)->aggstrategy) {
                case AGG_PLAIN:
                    *pname = "Aggregate";
                    *strategy = *pt_options = "Plain";
                    break;
                case AGG_SORTED:
                    *pname = "GroupAggregate";
                    *strategy = *pt_options = "Sorted";
                    break;
                case AGG_HASHED:
                    *pname = "HashAggregate";
                    *strategy = *pt_options = "Hashed";
                    break;
                default:
                    *pname = "Aggregate ?\?\?";
                    *strategy = *pt_options = "?\?\?";
                    break;
            }
            break;
        case T_VecAgg:
            *pt_operation = "Vector Aggregate";
            switch (((Agg*)plan)->aggstrategy) {
                case AGG_PLAIN:
                    *pname = *sname = "Vector Aggregate";
                    *pt_options = "Plain";
                    break;
                case AGG_HASHED: {
                    bool is_sonichash = ((VecAgg*)plan)->is_sonichash;
                    if (is_sonichash)
                        *pname = *sname = "Vector Sonic Hash Aggregate";
                    else
                        *pname = *sname = "Vector Hash Aggregate";
                    *pt_options = "Hashed";
                } break;
                case AGG_SORTED:
                    *pname = *sname = "Vector Sort Aggregate";
                    *pt_options = "Sorted";
                    break;
                default:
                    *pname = "Vector Aggregate ?\?\?";
                    *strategy = *pt_options = "?\?\?";
                    break;
            }
            break;
        case T_WindowAgg:
            *pname = *sname = *pt_operation = "WindowAgg";
            break;
        case T_VecWindowAgg:
            *pname = *sname = *pt_operation = "Vector WindowAgg";
            break;
        case T_Unique:
            *pname = *sname = *pt_operation = "Unique";
            break;
        case T_VecUnique:
            *pname = *sname = *pt_operation = "Vector Unique";
            break;
        case T_SetOp:
            *sname = *pt_operation = "SetOp";
            switch (((SetOp*)plan)->strategy) {
                case SETOP_SORTED:
                    *pname = "SetOp";
                    *strategy = *pt_options = "Sorted";
                    break;
                case SETOP_HASHED:
                    *pname = "HashSetOp";
                    *strategy = *pt_options = "Hashed";
                    break;
                default:
                    *pname = "SetOp ?\?\?";
                    *strategy = *pt_options = "?\?\?";
                    break;
            }
            break;
        case T_VecSetOp:
            *sname = *pt_operation = "Vector SetOp";
            switch (((VecSetOp*)plan)->strategy) {
                case SETOP_SORTED:
                    *pname = "Vector SetOp";
                    *strategy = *pt_options = "Sorted";
                    break;
                case SETOP_HASHED:
                    *pname = "Vector HashSetOp";
                    *strategy = *pt_options = "Hashed";
                    break;
                default:
                    *pname = "Vector SetOp ?\?\?";
                    *strategy = *pt_options = "?\?\?";
                    break;
            }
            break;
        case T_LockRows:
            *pname = *sname = *pt_operation = "LockRows";
            break;
        case T_Limit:
            *pname = *sname = *pt_operation = "Limit";
            break;
        case T_Hash:
            *pname = *sname = *pt_operation = "Hash";
            break;
        case T_PartIterator:
            *pname = *sname = *pt_operation = "Partition Iterator";
            break;
        case T_VecPartIterator:
            *pname = *sname = *pt_operation = "Vector Partition Iterator";
            break;
        case T_VecToRow:
            *pname = *sname = *pt_operation = "Row Adapter";
            break;
        case T_RowToVec:
            if (IsA(plan->lefttree, SeqScan) && ((SeqScan*)plan->lefttree)->scanBatchMode) {
                *pname = *sname = *pt_operation = "Vector Adapter(type: BATCH MODE)";
            } else {
                *pname = *sname = *pt_operation = "Vector Adapter";
            }
            break;
        case T_VecAppend:
            *pname = *sname = *pt_operation = "Vector Append";
            break;
        case T_VecModifyTable:
            *sname = "ModifyTable";
            *pt_operation = "Modify Table";
            switch (((ModifyTable*)plan)->operation) {
                case CMD_INSERT:
                    *pname = *operation = *pt_options = "Vector Insert";
                    break;
                case CMD_UPDATE:
                    *pname = *operation = *pt_options = "Vector Update";
                    break;
                case CMD_DELETE:
                    *pname = *operation = *pt_options = "Vector Delete";
                    break;
                case CMD_MERGE:
                    *pname = *operation = *pt_options = "Vector Merge";
                    break;
                default:
                    *pname = *pt_options = "?\?\?";
                    break;
            }
            break;
        case T_VecLimit:
            *pname = *sname = *pt_operation = "Vector Limit";
            break;
        case T_VecMergeJoin:
            *pname = "Vector Merge";
            *sname = *pt_operation = "Vector Merge Join";
            break;
        case T_TrainModel: {
                TrainModel *ptrain = (TrainModel*)plan;
                AlgorithmAPI *api = get_algorithm_api(ptrain->algorithm);
                *pname = "Train Model";
                *sname = *pt_operation = (char*) api->name;
            }
            break;
        default:
            *pname = *sname = *pt_operation = "?\?\?";
            break;
    }

    if (IsA(plan, Agg) || IsA(plan, VecAgg)) {
        Agg* agg = (Agg*)plan;
        if (agg->is_dummy) {
            StringInfo si = makeStringInfo();
            appendStringInfo(si, "Dummy %s", *pname);
            *pname = si->data;
        }
    }
}
