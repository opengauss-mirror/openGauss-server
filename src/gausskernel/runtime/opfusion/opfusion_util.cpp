/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * opfusion_util.cpp
 * The main part of the bypass executor. Instead of processing through the origin
 * Portal executor, the bypass executor provides a shortcut when the query is
 * simple.
 *
 * IDENTIFICATION
 * src/gausskernel/runtime/executor/opfusion_util.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "opfusion/opfusion_util.h"

#include "access/printtup.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_partition_fn.h"
#include "commands/copy.h"
#include "executor/node/nodeIndexscan.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/dynahash.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

const char* getBypassReason(FusionType result)
{
    switch (result) {
        case NONE_FUSION: {
            return "Bypass not executed";
        }
        case NOBYPASS_NO_CPLAN: {
            return "Bypass not executed because the plan is custom plan";
        }
        case SELECT_FUSION: {
            return "Bypass executed through select fusion";
        }

        case SELECT_FOR_UPDATE_FUSION: {
            return "Bypass executed through select for update fusion";
        }

        case INSERT_FUSION: {
            return "Bypass executed through insert fusion";
        }

        case UPDATE_FUSION: {
            return "Bypass executed through update fusion";
        }

        case DELETE_FUSION: {
            return "Bypass executed through delete fusion";
        }

        case AGG_INDEX_FUSION: {
        	return "Bypass executed through agg fusion";
        }

        case SORT_INDEX_FUSION: {
        	return "Bypass executed through sort fusion";
        }

        case NOBYPASS_NO_SIMPLE_PLAN: {
            return "Bypass not executed because the plan of query is not a simple plan";
        }

        case NOBYPASS_NO_QUERY_TYPE: {
            return "Bypass not executed because query is not an avaliable bypass statement such as select, delete, "
                "update and insert";
        }

        case NOBYPASS_NO_INDEXSCAN: {
            return "Bypass not executed because query\'s scan operator is not index";
        }

        case NOBYPASS_ONLY_SUPPORT_BTREE_INDEX: {
            return "Bypass not executed because only support btree index currently";
        }

        case NOBYPASS_INDEXSCAN_WITH_ORDERBY: {
            return "Bypass not executed because query used indexscan with order by clause method";
        }

        case NOBYPASS_INDEXSCAN_WITH_QUAL: {
            return "Bypass not executed because query used indexscan with qual";
        }

        case NOBYPASS_INDEXSCAN_CONDITION_INVALID: {
            return "Bypass not executed because query used unsupported indexscan condition";
        }

        case NOBYPASS_INDEXONLYSCAN_WITH_ORDERBY: {
            return "Bypass not executed because query used indexonlyscan with order by clause method";
        }

        case NOBYPASS_INDEXONLYSCAN_WITH_QUAL: {
            return "Bypass not executed because query used indexonlyscan with qual";
        }

        case NOBYPASS_INDEXONLYSCAN_CONDITION_INVALID: {
            return "Bypass not executed because query used invalid indexonlyscan condition";
        }

        case NOBYPASS_TARGET_WITH_SYS_COL: {
            return "Bypass not executed because query used the target list with system column";
        }

        case NOBYPASS_TARGET_WITH_NO_TABLE_COL: {
            return "Bypass not executed because query used the target list which only contains table's column";
        }

        case NOBYPASS_NO_TARGETENTRY: {
            return "Bypass not executed because the type of targetlist of query should be targetEntry";
        }

        case NOBYPASS_PARAM_TYPE_INVALID: {
            return "Bypass not executed because query used unsupported param type";
        }

        case NOBYPASS_DML_RELATION_NUM_INVALID: {
            return "Bypass not executed because query\'s relation number is not 1";
        }

        case NOBYPASS_DML_RELATION_NOT_SUPPORT: {
            return "Bypass not executed because query\'s relation is not support";
        }

        case NOBYPASS_DML_TARGET_TYPE_INVALID: {
            return "Bypass not executed because query used unsupported DML target type";
        }

        case NOBYPASS_EXP_NOT_SUPPORT: {
            return "Bypass not executed because the expression of query is not support";
        }

        case NOBYPASS_LIMITOFFSET_CONST_LESS_THAN_ZERO: {
            return "Bypass not executed because query used limit offset grammar with const less than zero";
        }

        case NOBYPASS_LIMITCOUNT_CONST_LESS_THAN_ZERO: {
            return "Bypass not executed because query used limit count grammar with const less than zero";
        }

        case NOBYPASS_LIMIT_NOT_CONST: {
            return "Bypass not executed because query used limit grammar with a non-constant value";
        }

        case NOBYPASS_NO_SIMPLE_INSERT: {
            return "Bypass not executed because query combines insert operator with others";
        }

        case NOBYPASS_INVALID_SELECT_FOR_UPDATE: {
            return "Bypass not executed because query used invalid select for update";
        }

        case NOBYPASS_INVALID_MODIFYTABLE: {
            return "Bypass not executed because query used invalid modifytable";
        }

        case NOBYPASS_STREAM_NOT_SUPPORT: {
            return "Bypass not executed because query used streaming plan";
        }

        case NOBYPASS_NULLTEST_TYPE_INVALID: {
            return "Bypass not executed because query used invalid composite type";
            break;
        }
      case NOBYPASS_INVALID_PLAN: {
            return "Bypass not executed because invalid plan node";
            break;
        }

        case NOBYPASS_NOT_PLAIN_AGG: {
            return "Bypass not executed because it's not a plain agg query";
            break;
        }

        case NOBYPASS_ONE_TARGET_ALLOWED: {
            return "Bypass not executed because it's just one target allowed";
            break;
        }

        case NOBYPASS_AGGREF_TARGET_ALLOWED: {
            return "Bypass not executed because it's just aggref allowed";
            break;
        }

        case NOBYPASS_JUST_SUM_ALLOWED: {
            return "Bypass not executed because it's sum() allowed";
            break;
        }

        case NOBYPASS_JUST_VAR_FOR_AGGARGS: {
            return "Bypass not executed because it's Var type allowed for agg argument";
            break;
        }

        case NOBYPASS_JUST_MERGE_UNSUPPORTED: {
            return "Bypass not executed because it's unsupported that sort node just merge results";
            break;
        }

        case NOBYPASS_JUST_VAR_ALLOWED_IN_SORT: {
            return "Bypass not executed because it's Var type allowed for target in sort query";
            break;
        }
		
        case NOBYPASS_UPSERT_NOT_SUPPORT: {
            return "Bypass not support INSERT INTO ... ON DUPLICATE KEY UPDATE statement";
            break;
        }

        case NOBYPASS_ZERO_PARTITION: {
            return "Bypass not support query in zero partition";
            break;
        }

        case NOBYPASS_MULTI_PARTITION: {
            return "Bypass not support query in multiple partitions";
            break;
        }

        case NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION: {
            return "Bypass not executed because the expression of query is not support in partition table";
            break;
        }

        case NO_BYPASS_PARTITIONKEY_IS_NULL: {
            return "Bypass not executed because the partition key is null";
            break;
        }
        case NOBYPASS_NO_UPDATE_PARTITIONKEY: {
            return "Bypass not support update the partition key";
            break;
        }
        case NOBYPASS_NO_INCLUDING_PARTITIONKEY: {
            return "Bypass not executed because the partition key is not in the parameters";
            break;
        }
        case NOBYPASS_PARTITION_BYPASS_NOT_OPEN: {
            return "enable_partition_opfusion is in the closed state";
        }
        case NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION: {
            return "Bypass not support in list/hash partition currently";
        }

        case NOBYPASS_VERSION_SCAN_PLAN: {
            return "Bypass not executed because the plan contains version table scan.";
            break;
        }

        default: {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized bypass support type: %d", (int)result)));
            return NULL;
        }
    }
}

void BypassUnsupportedReason(FusionType result)
{
    if (result == NONE_FUSION) {
        return;
    }
    if (u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_OFF) {
        return;
    }

    Assert(result != BYPASS_OK);
    Assert(u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_LOG);
    const char *bypass_reason = getBypassReason(result);

    int elevel = DEBUG4;
    if (result != BYPASS_OK) {
        ereport(elevel, (errmodule(MOD_OPFUSION), errcode(ERRCODE_LOG),
                         errmsg("%s: \"%s\".", bypass_reason, t_thrd.postgres_cxt.debug_query_string)));
    }
}

bool checkFusionParam(Param *param, ParamListInfo boundParams)
{
    if (param->paramkind == PARAM_EXTERN && boundParams != NULL && param->paramid > 0 &&
        param->paramid <= boundParams->numParams) {
        ParamExternData *prm = &boundParams->params[param->paramid - 1];

        if (OidIsValid(prm->ptype) && (prm->pflags & PARAM_FLAG_CONST)) {
            return true;
        }
    }

    return false;
}

static bool checkFlinfo(Node *node)
{
    /* check whether the flinfo satisfy conditon */
    FmgrInfo *flinfo = NULL;
    flinfo = (FmgrInfo *)palloc(sizeof(FmgrInfo));
    fmgr_info(((FuncExpr *)node)->funcid, flinfo);
    if (flinfo->fn_retset == true || (flinfo->fn_strict == false && flinfo->fn_expr == NULL)) {
        pfree(flinfo);
        flinfo = NULL;
        return false;
    }
    pfree(flinfo);
    flinfo = NULL;
    return true;
}

static bool checkExpr(Node *node, bool is_first)
{
    NodeTag tag = nodeTag(node);
    switch (tag) {
        case T_Const:
        case T_Param: {
            return true;
        }
        case T_Var: {
            return true;
        }

        case T_FuncExpr: {
            if (is_first == false) {
                return false;
            }
            if (!checkFlinfo(node)) {
                return false;
            }
            bool found_ptr = true;
            void *ans = NULL;
            ans = hash_search(g_instance.exec_cxt.function_id_hashtbl, (void *)&((FuncExpr *)node)->funcid, HASH_FIND,
                &found_ptr);
            if (found_ptr == false) {
                return false;
            }
            List *args = ((FuncExpr *)node)->args;
            if (list_length(args) == 0 || list_length(args) > 4) {
                return false;
            }

            bool result = true;
            ListCell *lc = NULL;
            foreach (lc, args) {
                result = result && checkExpr((Node *)lfirst(lc), is_first);
                is_first = false;
            }
            return result;
        }

        case T_OpExpr: {
            if (is_first == false) {
                return false;
            }
            /* check whether return set */
            if (((OpExpr *)node)->opretset == true) {
                return false;
            }

            List *args = ((OpExpr *)node)->args;
            if (list_length(args) == 0 || list_length(args) > 4) {
                return false;
            }

            bool result = true;
            ListCell *lc = NULL;
            foreach (lc, args) {
                result = result && checkExpr((Node *)lfirst(lc), is_first);
                is_first = false;
            }
            return result;
        }

        case T_RelabelType: {
            return checkExpr((Node *)((RelabelType *)node)->arg, is_first);
        }

        default: {
            return false;
        }
    }
}
FusionType checkFusionAgg(Agg *node, ParamListInfo params)
{
    if (node->plan.righttree != NULL || node->plan.lefttree == NULL) {
        return NOBYPASS_INVALID_PLAN;
    }

    /* check whether to have order by */
    if (node->aggstrategy != AGG_PLAIN ||
            node->groupingSets > 0) {
        return NOBYPASS_NOT_PLAIN_AGG;
    }

    Assert (node->numCols == 0);

    if (list_length(node->plan.targetlist) != 1 ||
            node->plan.qual != NULL) {
        return NOBYPASS_ONE_TARGET_ALLOWED;
    }

    TargetEntry *res = (TargetEntry *)linitial(node->plan.targetlist);
    if (!IsA(res->expr, Aggref)) {
        return NOBYPASS_AGGREF_TARGET_ALLOWED;
    }

    Aggref *aggref = (Aggref *)res->expr;

    if (list_length(aggref->args) != 1 ||
            aggref->aggorder != NULL ||
            aggref->aggdistinct != NULL ||
            aggref->aggvariadic) {
        return NOBYPASS_AGGREF_TARGET_ALLOWED;
    }

    switch (aggref->aggfnoid) {
        case INT2SUMFUNCOID:
        case INT4SUMFUNCOID:
        case INT8SUMFUNCOID:
        case NUMERICSUMFUNCOID:
            break;
        default:
            return NOBYPASS_JUST_SUM_ALLOWED;
    }

    res = (TargetEntry *)linitial(aggref->args);
    if (!IsA(res->expr, Var)) {
        return NOBYPASS_JUST_VAR_FOR_AGGARGS;
    }

    return BYPASS_OK;
}

FusionType checkFusionSort(Sort *node, ParamListInfo params)
{
    if (node->plan.righttree != NULL || node->plan.lefttree == NULL) {
        return NOBYPASS_INVALID_PLAN;
    }

    if (node->srt_start_merge) {
        return NOBYPASS_JUST_MERGE_UNSUPPORTED;
    }

    ListCell *lc = NULL;
    /* check whether targetlist is simple */
    foreach (lc, node->plan.targetlist) {
        Assert (IsA(lfirst(lc), TargetEntry));

        TargetEntry *res = (TargetEntry *)lfirst(lc);
        if (!IsA(res->expr, Var)) {
            return NOBYPASS_JUST_VAR_ALLOWED_IN_SORT;
        }

        Var *var = (Var *)res->expr;
        /* System columns, such as ctid and xmin, are not supported. */
        if (var->varoattno <= 0) {
            return NOBYPASS_TARGET_WITH_SYS_COL;
        }
    }

    return BYPASS_OK;
 }
template <bool is_dml, bool isonlyindex> FusionType checkFusionIndexScan(Node *node, ParamListInfo params)
{
    List *tarlist = NULL;
    List *indexorderby = NULL;
    List *indexqual = NULL;
    List *qual = NULL;
    Oid indexOid = InvalidOid;
    Relation index = NULL;
    if (isonlyindex) {
        tarlist = ((IndexOnlyScan *)node)->scan.plan.targetlist;
        indexorderby = ((IndexOnlyScan *)node)->indexorderby;
        indexqual = ((IndexOnlyScan *)node)->indexqual;
        qual = ((IndexOnlyScan *)node)->scan.plan.qual;
        indexOid = ((IndexOnlyScan *)node)->indexid;
        if (indexorderby != NULL) {
            return NOBYPASS_INDEXONLYSCAN_WITH_ORDERBY;
        }
    } else {
        tarlist = ((IndexScan *)node)->scan.plan.targetlist;
        indexorderby = ((IndexScan *)node)->indexorderby;
        indexqual = ((IndexScan *)node)->indexqual;
        qual = ((IndexScan *)node)->scan.plan.qual;
        indexOid = ((IndexScan *)node)->indexid;
        if (indexorderby != NULL) {
            return NOBYPASS_INDEXSCAN_WITH_ORDERBY;
        }
    }

    index = index_open(indexOid, AccessShareLock);
    if (!OID_IS_BTREE(index->rd_rel->relam)) {
        index_close(index, AccessShareLock);
        return NOBYPASS_ONLY_SUPPORT_BTREE_INDEX;
    }
    index_close(index, AccessShareLock);

    ListCell *lc = NULL;

    if (is_dml == false) {
        /* check whether targetlist is simple */
        foreach (lc, tarlist) {
            if (!IsA(lfirst(lc), TargetEntry)) {
                return NOBYPASS_NO_TARGETENTRY;
            }
            TargetEntry *res = (TargetEntry *)lfirst(lc);

            if (res->resjunk == true) {
                continue;
            }

            if (!IsA(res->expr, Var)) {
                return NOBYPASS_TARGET_WITH_NO_TABLE_COL;
            }

            Var *var = (Var*)res->expr;
            AttrNumber attno = var->varno == INDEX_VAR ? var->varoattno : var->varattno;
            if (attno <= 0) {
                return NOBYPASS_TARGET_WITH_SYS_COL;
            }
        }
    }

    /* check whether index expression is simple */
    foreach (lc, indexqual) {
        if (IsA(lfirst(lc), NullTest)) {
            if (((NullTest *)lfirst(lc))->argisrow == true) {
                return NOBYPASS_NULLTEST_TYPE_INVALID;
            }
            continue;
        }

        if (!IsA(lfirst(lc), OpExpr)) {
            return NOBYPASS_INDEXSCAN_CONDITION_INVALID;
        }

        OpExpr *opexpr = (OpExpr *)lfirst(lc);

        if (list_length(opexpr->args) != 2) {
            if (isonlyindex) {
                return NOBYPASS_INDEXONLYSCAN_CONDITION_INVALID;
            } else {
                return NOBYPASS_INDEXSCAN_CONDITION_INVALID;
            }
        }

        Expr *leftop = NULL;  /* expr on lhs of operator */
        Expr *rightop = NULL; /* expr on rhs ... */

        leftop = (Expr *)linitial(opexpr->args);
        if (leftop != NULL && IsA(leftop, RelabelType)) {
            leftop = ((RelabelType *)leftop)->arg;
        }

        rightop = (Expr *)lsecond(opexpr->args);
        if (rightop != NULL && IsA(rightop, RelabelType)) {
            rightop = ((RelabelType *)rightop)->arg;
        }

        if (leftop == NULL || rightop == NULL) {
            if (isonlyindex) {
                return NOBYPASS_INDEXONLYSCAN_CONDITION_INVALID;
            } else {
                return NOBYPASS_INDEXSCAN_CONDITION_INVALID;
            }
        }

        if (!IsA(leftop, Var) || (!IsA(rightop, Param) && !IsA(rightop, Const))) {
            if (isonlyindex) {
                return NOBYPASS_INDEXONLYSCAN_CONDITION_INVALID;
            } else {
                return NOBYPASS_INDEXSCAN_CONDITION_INVALID;
            }
        }

        if (IsA(rightop, Param) && !checkFusionParam((Param *)rightop, params)) {
            return NOBYPASS_PARAM_TYPE_INVALID;
        }
    }

    /* check whether filter expression is simple */
    if (qual != NULL) {
        if (isonlyindex) {
            return NOBYPASS_INDEXONLYSCAN_WITH_QUAL;
        } else {
            return NOBYPASS_INDEXSCAN_WITH_QUAL;
        }
    }
    return BYPASS_OK;
}

/* check expression can be used for pruning */
void CheckExprPartitionTable(Node* node, ParamListInfo params, FusionType* ftype)
{
    PruningResult* result = IsA(node, IndexScan) ? ((IndexScan *)node)->scan.pruningInfo
                                                    : ((IndexOnlyScan *)node)->scan.pruningInfo;
    Param* paramArg = result->paramArg;
    if (paramArg == NULL) {
        *ftype = NOBYPASS_NO_INCLUDING_PARTITIONKEY;
        return;
    }
    if (params->params[paramArg->paramid - 1].isnull) {
        *ftype = NO_BYPASS_PARTITIONKEY_IS_NULL;
        return;
    }
    Expr* expr = result->expr;
    switch (nodeTag(expr)) {
        case T_BoolExpr: {
            BoolExpr* boolExpr = (BoolExpr *)expr;
            int count = 0;
            ListCell* cell = NULL;
            foreach (cell, boolExpr->args) {
                if (count == result->paramArg->paramid - 1) {
                    if (nodeTag(cell) == T_BoolExpr) {
                        *ftype = NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION;
                        return;
                    }
                    OpExpr* arg = (OpExpr*)lfirst(cell);
                    char* opName = get_opname(arg->opno);
                    if (strncmp("=", opName, 1) != 0) {
                        *ftype = NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION;
                        return;
                    }
                } else {
                    count += 1;
                    continue;
                }
            }
            return;
        } break;
        case T_OpExpr: {
            OpExpr* opExpr = (OpExpr*)expr; 
            char* opName = get_opname(opExpr->opno);
            Assert(opName != NULL);
            if (strncmp("=", opName, 1) != 0) {
                *ftype = NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION;
                return;
            } else {
                return;
            }
        } break;
        default: {
            *ftype = NOBYPASS_EXP_NOT_SUPPORT_IN_PARTITION;
            return;
        } break;
    }
    return;
}

void CheckFusionPartitionNumber(FusionType* ftype, Scan scan)
{
    if (scan.itrs == 0) {
        *ftype = NOBYPASS_ZERO_PARTITION;
        return;
    }
    if (scan.itrs > 1) {
        *ftype = NOBYPASS_MULTI_PARTITION;
        return;
    }
    return;
}

bool checkPartitionType(const Relation rel)
{
    if (!RELATION_IS_PARTITIONED(rel)) {
        return false;
    }
    if (rel->partMap->type == PART_TYPE_RANGE) {
        return false;
    } else {
        return true;
    }
}
bool checkDMLRelation(const Relation rel, const PlannedStmt *plannedstmt, bool isInsert, bool isPartTbl)
{
    bool result = false;
    if (rel->rd_rel->relkind != RELKIND_RELATION || rel->rd_rel->relhasrules || rel->rd_rel->relhastriggers ||
        rel->rd_rel->relhasoids || rel->rd_rel->relhassubclass || RelationIsColStore(rel) || RelationIsTsStore(rel) ||
        RelationInRedistribute(rel) || plannedstmt->hasReturning || RelationIsSubPartitioned(rel)) {
        result = true;
    }

    if (isInsert) {
        return result;
    } else if (!isPartTbl && RELATION_IS_PARTITIONED(rel)) {
        result = true;
    }
    return result;
}


FusionType getSelectFusionType(List *stmt_list, ParamListInfo params)
{
    FusionType ftype = SELECT_FUSION;
    bool limitplan = false;
    bool isPartTbl = false;
    Index res_rel_idx = 0;

    /* check whether is only one index scan */
    PlannedStmt *plannedstmt = (PlannedStmt *)linitial(stmt_list);
    Plan *top_plan = plannedstmt->planTree;

    /* check for limit */
    if (IsA(top_plan, Limit)) {
        Limit *limit = (Limit *)top_plan;
        if (limit->limitOffset != NULL) {
            if (IsA(limit->limitOffset, Const)) {
                Assert(((Const *)limit->limitOffset)->consttype == 20);
                if (DatumGetInt64(((Const *)limit->limitOffset)->constvalue) < 0) {
                    return NOBYPASS_LIMITOFFSET_CONST_LESS_THAN_ZERO;
                }
            } else {
                return NOBYPASS_LIMIT_NOT_CONST;
            }
        }
        if (limit->limitCount != NULL) {
            if (IsA(limit->limitCount, Const)) {
                Assert(((Const *)limit->limitCount)->consttype == 20);
                if (DatumGetInt64(((Const *)limit->limitCount)->constvalue) < 0) {
                    return NOBYPASS_LIMITCOUNT_CONST_LESS_THAN_ZERO;
                }
            } else {
                return NOBYPASS_LIMIT_NOT_CONST;
            }
        }
        top_plan = top_plan->lefttree;
        limitplan = true;
    }

    /* check select for update */
    if (IsA(top_plan, LockRows)) {
        LockRows *lockrows = (LockRows *)top_plan;
        bool is_select_for_update =
            (list_length(lockrows->rowMarks) == 1 && IsA(linitial(lockrows->rowMarks), PlanRowMark) &&
            ((PlanRowMark *)linitial(lockrows->rowMarks))->markType == ROW_MARK_EXCLUSIVE &&
            ((PlanRowMark *)linitial(lockrows->rowMarks))->noWait == false &&
            ((PlanRowMark *)linitial(lockrows->rowMarks))->waitSec == 0);
        if (is_select_for_update) {
            top_plan = top_plan->lefttree;
            ftype = SELECT_FOR_UPDATE_FUSION;
        } else {
            return NOBYPASS_INVALID_SELECT_FOR_UPDATE;
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
        /* check select for agg */
        if (u_sess->attr.attr_sql.enable_beta_opfusion && !limitplan && IsA(top_plan, Agg) && 
            ftype == SELECT_FUSION) {
            FusionType ttype;
            ttype = checkFusionAgg((Agg *)top_plan, params);
            if (ttype > BYPASS_OK) {
                return ttype;
            }
            ftype = AGG_INDEX_FUSION;
            top_plan = top_plan->lefttree;
        }
    
        /* check select for sort */
        if (u_sess->attr.attr_sql.enable_beta_opfusion && !limitplan && IsA(top_plan, Sort) &&
            ftype == SELECT_FUSION) {

            FusionType ttype;
            ttype = checkFusionSort((Sort*)top_plan, params);
            if (ttype > BYPASS_OK) {
                return ttype;
            }
    
            ftype = SORT_INDEX_FUSION;
            top_plan = top_plan->lefttree;
        }
#endif

    /* check for partition table */
    if (IsA(top_plan, PartIterator)) {
        if (u_sess->attr.attr_sql.enable_partition_opfusion)
            top_plan = top_plan->lefttree;
        else
            return NOBYPASS_PARTITION_BYPASS_NOT_OPEN;
    }

    /* check for indexscan or indexonlyscan */
    if ((IsA(top_plan, IndexScan) || IsA(top_plan, IndexOnlyScan)) && top_plan->lefttree == NULL) {
        FusionType ttype;
        if (IsA(top_plan, IndexScan)) {
            ttype = checkFusionIndexScan<false, false>((Node *)top_plan, params);
            IndexScan* node = (IndexScan *)top_plan;
            isPartTbl = node->scan.isPartTbl;
            res_rel_idx = node->scan.scanrelid;
        } else {
            ttype = checkFusionIndexScan<false, true>((Node *)top_plan, params);
            IndexOnlyScan* node = (IndexOnlyScan *)top_plan;
            isPartTbl = node->scan.isPartTbl;
            res_rel_idx = node->scan.scanrelid;
        }
        /* check failed */
        if (ttype > BYPASS_OK) {
            return ttype;
        }
    } else {
        return NOBYPASS_NO_INDEXSCAN;
    }

    Oid relid = getrelid(res_rel_idx, plannedstmt->rtable);
    Relation rel = heap_open(relid, AccessShareLock);
    if (checkDMLRelation(rel, plannedstmt, false, isPartTbl)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_DML_RELATION_NOT_SUPPORT;
    }
    if (checkPartitionType(rel)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION;
    }
    heap_close(rel, AccessShareLock);

    /* check for the number of partitions */
    if (IsA(top_plan, IndexScan)) {
        IndexScan* scan = (IndexScan *)top_plan;
        if (scan->scan.isPartTbl) {
            CheckFusionPartitionNumber(&ftype, ((IndexScan *)scan)->scan); 
            if (params != NULL) {
                CheckExprPartitionTable((Node *)scan, params, &ftype);
            }
        }
    } else {
        IndexOnlyScan* scan = (IndexOnlyScan *)top_plan;
        if (scan->scan.isPartTbl) {
            CheckFusionPartitionNumber(&ftype, ((IndexOnlyScan *)scan)->scan); 
            if (params != NULL) {
                CheckExprPartitionTable((Node *)scan, params, &ftype);
            }
        }
    }

    return ftype;
}

void checkTargetlist(List *targetList, FusionType* ftype)
{
    ListCell *lc = NULL;
    TargetEntry *target = NULL;
    foreach (lc, targetList) {
        target = (TargetEntry *)lfirst(lc);
        if (target->resjunk && nodeTag((Node *)target->expr) == T_Var) {
            continue;
        }
        if (!checkExpr((Node *)target->expr, true)) {
            *ftype = NOBYPASS_EXP_NOT_SUPPORT;
            return;
        }
    }
    return;
}

FusionType checkBaseResult(Plan* top_plan)
{
    FusionType result = INSERT_FUSION;
    ModifyTable *node = (ModifyTable *)top_plan;
    if (!IsA(linitial(node->plans), BaseResult)) {
        return NOBYPASS_NO_SIMPLE_INSERT;
    }
    BaseResult *base = (BaseResult *)linitial(node->plans);
    if (base->plan.lefttree != NULL || base->plan.initPlan != NIL || base->resconstantqual != NULL) {
        return NOBYPASS_NO_SIMPLE_INSERT;
    }
    if (node->upsertAction != UPSERT_NONE) {
        return NOBYPASS_UPSERT_NOT_SUPPORT;
    }
    return result;
}

FusionType getInsertFusionType(List *stmt_list, ParamListInfo params)
{
    FusionType ftype = INSERT_FUSION;

    /* check result relaiton */
    PlannedStmt *plannedstmt = (PlannedStmt *)linitial(stmt_list);
    if (plannedstmt->resultRelations == NULL || list_length(plannedstmt->resultRelations) != 1) {
        return NOBYPASS_DML_RELATION_NUM_INVALID;
    }

    Plan *top_plan = plannedstmt->planTree;
#ifdef ENABLE_MULTIPLE_NODES
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 0) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#else
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 1) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#endif

    /* check subquery num */
    FusionType ttype = checkBaseResult(top_plan);
    if (ttype > BYPASS_OK) {
        return ttype;
    }
    ModifyTable *node = (ModifyTable *)top_plan;
    BaseResult *base = (BaseResult *)linitial(node->plans);

    /* check relation */
    Index res_rel_idx = linitial_int(plannedstmt->resultRelations);
    Oid relid = getrelid(res_rel_idx, plannedstmt->rtable);
    Relation rel = heap_open(relid, AccessShareLock);

    for (int i = 0; i < rel->rd_att->natts; i++) {
        if (rel->rd_att->attrs[i]->attisdropped) {
            continue;
        }
        /* check whether the attrs of */
        HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(rel->rd_att->attrs[i]->atttypid));
        if (!HeapTupleIsValid(tuple)) {
            /* should not happen */
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u", rel->rd_att->attrs[i]->atttypid)));
        }
        Form_pg_type type_form = (Form_pg_type)GETSTRUCT(tuple);
        ReleaseSysCache(tuple);
        if (type_form->typtype != 'b') {
            heap_close(rel, AccessShareLock);
            return NOBYPASS_DML_TARGET_TYPE_INVALID;
        }
    }
    if (checkDMLRelation(rel, plannedstmt, true, RELATION_IS_PARTITIONED(rel))) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_DML_RELATION_NOT_SUPPORT;
    }
    if (RELATION_IS_PARTITIONED(rel) && !u_sess->attr.attr_sql.enable_partition_opfusion) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_PARTITION_BYPASS_NOT_OPEN;
    }
    if (checkPartitionType(rel)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION;
    }
    heap_close(rel, AccessShareLock);
    /*
     * check targetlist
     * maybe expr type is FuncExpr because of type conversion.
     */
    List *targetlist = base->plan.targetlist;
    checkTargetlist(targetlist, &ftype);
    return ftype;
}

FusionType getUpdateFusionType(List *stmt_list, ParamListInfo params)
{
    FusionType ftype = UPDATE_FUSION;

    /* check result relaiton */
    PlannedStmt *plannedstmt = (PlannedStmt *)linitial(stmt_list);
    if (plannedstmt->resultRelations == NULL || list_length(plannedstmt->resultRelations) != 1) {
        return NOBYPASS_DML_RELATION_NUM_INVALID;
    }

    Plan* top_plan = plannedstmt->planTree;
#ifdef ENABLE_MULTIPLE_NODES
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 0) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#else
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 1) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#endif

    /* check subquery num */
    ModifyTable *node = (ModifyTable *)top_plan;
    if (list_length(node->plans) != 1) {
        return NOBYPASS_NO_SIMPLE_PLAN;
    }

    Plan *updatePlan = (Plan *)linitial(node->plans);
    
    if (IsA(updatePlan, PartIterator)) {
        if (u_sess->attr.attr_sql.enable_partition_opfusion) {
            updatePlan = updatePlan->lefttree;
        } else {
            return NOBYPASS_PARTITION_BYPASS_NOT_OPEN;
        }
    }
    if (!IsA(updatePlan, IndexScan)) {
        return NOBYPASS_NO_INDEXSCAN;
    }

    /* check index scan */
    FusionType ttype = checkFusionIndexScan<true, false>((Node *)updatePlan, params);
    /* check failed */
    if (ttype > BYPASS_OK) {
        return ttype;
    }

    /* check relation */
    IndexScan *indexscan = (IndexScan *)updatePlan;
    Index res_rel_idx = linitial_int(plannedstmt->resultRelations);
    Oid relid = getrelid(res_rel_idx, plannedstmt->rtable);
    Relation rel = heap_open(relid, AccessShareLock);
    if (checkDMLRelation(rel, plannedstmt, false, indexscan->scan.isPartTbl)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_DML_RELATION_NOT_SUPPORT;
    }
    if (checkPartitionType(rel)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION;
    }
    heap_close(rel, AccessShareLock);

    /* check target list */
    if (node->partKeyUpdated) {
        return NOBYPASS_NO_UPDATE_PARTITIONKEY;
    }
    List *targetlist = indexscan->scan.plan.targetlist;
    checkTargetlist(targetlist, &ftype);

    /* check the number of partitions */
    if (indexscan->scan.isPartTbl) {
        CheckFusionPartitionNumber(&ftype, indexscan->scan); 
        if (params != NULL) {
            CheckExprPartitionTable((Node *)indexscan, params, &ftype);
        }    
    }
    return ftype;
}

FusionType getDeleteFusionType(List *stmt_list, ParamListInfo params)
{
    FusionType ftype = DELETE_FUSION;

    /* check result relaiton */
    PlannedStmt *plannedstmt = (PlannedStmt *)linitial(stmt_list);
    if (plannedstmt->resultRelations == NULL || list_length(plannedstmt->resultRelations) != 1) {
        return NOBYPASS_DML_RELATION_NUM_INVALID;
    }

    Plan* top_plan = plannedstmt->planTree;
#ifdef ENABLE_MULTIPLE_NODES
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 0) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#else
    if (!IsA(top_plan, ModifyTable) || top_plan->plan_node_id != 1) {
        return NOBYPASS_INVALID_MODIFYTABLE;
    }
#endif

    /* check subquery num */
    ModifyTable *node = (ModifyTable *)top_plan;
    if (list_length(node->plans) != 1) {
        return NOBYPASS_NO_SIMPLE_PLAN;
    }

    Plan *deletePlan = (Plan *)linitial(node->plans);
    if (IsA(deletePlan, PartIterator)) {
        if (u_sess->attr.attr_sql.enable_partition_opfusion) {
            deletePlan = deletePlan->lefttree;
        } else {
            return NOBYPASS_PARTITION_BYPASS_NOT_OPEN;
        }
    }
    if (!IsA(deletePlan, IndexScan)) {
        return NOBYPASS_NO_INDEXSCAN;
    }
    /* check index scan */
    FusionType ttype = checkFusionIndexScan<true, false>((Node *)deletePlan, params);
    /* check failed */
    if (ttype > BYPASS_OK) {
        return ttype;
    }

    IndexScan* indexscan = (IndexScan *)deletePlan;
    /* check relation */
    Index res_rel_idx = linitial_int(plannedstmt->resultRelations);
    Oid relid = getrelid(res_rel_idx, plannedstmt->rtable);
    Relation rel = heap_open(relid, AccessShareLock);
    if (checkDMLRelation(rel, plannedstmt, false, indexscan->scan.isPartTbl)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_DML_RELATION_NOT_SUPPORT;
    }
    if (checkPartitionType(rel)) {
        heap_close(rel, AccessShareLock);
        return NOBYPASS_PARTITION_NOT_SUPPORT_IN_LIST_OR_HASH_PARTITION;
    }
    heap_close(rel, AccessShareLock);

    /* check the number of partitions */
    if (indexscan->scan.isPartTbl) {
        CheckFusionPartitionNumber(&ftype, indexscan->scan); 
        if (params != NULL) {
            CheckExprPartitionTable((Node *)indexscan, params, &ftype);
        }
    }
    return ftype;
}

void InitOpfusionFunctionId()
{
    /* init opfusion function_id hash table */
    HASHCTL ctl_func;
    errno_t rc;
    rc = memset_s(&ctl_func, sizeof(ctl_func), 0, sizeof(ctl_func));
    securec_check_c(rc, "\0", "\0");

    ctl_func.keysize = sizeof(Oid);
    ctl_func.entrysize = sizeof(Oid);
    ctl_func.hcxt = g_instance.instance_context;
    g_instance.exec_cxt.function_id_hashtbl =
        hash_create("Opfusion function id white-list", OPFUSION_FUNCTION_ID_MAX_HASH_SIZE, &ctl_func, HASH_ELEM);

    /* enter function id from array to hash table */
    uint32 i = 0;
    bool found_ptr = false;
    uint32 length = sizeof(function_id) / sizeof(Oid);
    void *ans = NULL;
    for (i = 0; i < length; i++) {
        ans = hash_search(g_instance.exec_cxt.function_id_hashtbl, (void *)&function_id[i], HASH_ENTER, &found_ptr);
    }
}

void tpslot_free_heaptuple(TupleTableSlot* reslot)
{
    if (reslot->tts_tuple) {
        heap_freetuple((HeapTuple)reslot->tts_tuple);
        reslot->tts_tuple = NULL;
    }
}

/* judge plan node is partiterator */
Node* JudgePlanIsPartIterator(Plan* plan)
{
    Node* node = NULL;
    if (IsA(plan, PartIterator)) {
        node = (Node*)plan->lefttree;
    } else {
        node = (Node*)plan;
    }
    return node;
}

/* init partition by the ScanFusion */
void InitPartitionByScanFusion(Relation rel, Relation* fakRel, Partition* part, EState* estate,
                               const ScanFusion* scan)
{
    if (RELATION_IS_PARTITIONED(rel)) {
        estate->esfRelations = NULL;
        *fakRel = scan->m_rel;
        *part = scan->m_partRel;
    }
}

/* init bucket relation */
Relation InitBucketRelation(int2 bucketid, Relation rel, Partition part)
{
    Relation bucketRel = NULL;
    if (bucketid != InvalidBktId) {
        bucketRel = bucketGetRelation(rel, part, bucketid);
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invaild Oid when open hash bucket relation.")));
    }
    return bucketRel;
}

/* execute the process of done in the Fusion */
void ExecDoneStepInFusion(EState* estate)
{
    if (estate->esfRelations) {
        FakeRelationCacheDestroy(estate->esfRelations);
        estate->esfRelations = NULL;
    }
}

/* init the Partition Oid in construct */
Oid GetRelOidForPartitionTable(Scan scan, const Relation rel, ParamListInfo params)
{
    Oid relOid = InvalidOid;
    if (params != NULL) {
        Param* paramArg = scan.pruningInfo->paramArg;
        relOid = GetPartitionOidByParam(rel, paramArg, &(params->params[paramArg->paramid - 1]));
    } else {
        Assert((list_length(scan.pruningInfo->ls_rangeSelectedPartitions) != 0));
        int partId = lfirst_int(list_head(scan.pruningInfo->ls_rangeSelectedPartitions));
        relOid = getPartitionOidFromSequence(rel, partId);
    }
    return relOid;
}

/* init the index in construct */
Relation InitPartitionIndexInFusion(Oid parentIndexOid, Oid partOid, Partition *partIndex, Relation *parentIndex,
    Relation rel)
{
    *parentIndex = relation_open(parentIndexOid, AccessShareLock);
    Oid partIndexOid;
    if (ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(rel->rd_indexlist) == 1) {
        partIndexOid = lfirst_oid(rel->rd_indexlist->head);
    } else {
        partIndexOid = getPartitionIndexOid(parentIndexOid, partOid);
    }
    *partIndex = partitionOpen(*parentIndex, partIndexOid, AccessShareLock);

    Relation index;
    if (PARTITION_ENABLE_CACHE_OPFUSION) {
        if (!(*partIndex)->partrel) {
            (*partIndex)->partrel = partitionGetRelation(*parentIndex, *partIndex);
            /* in function partitionGetRelation, owner->nfakerelrefs plus one due to ResourceOwnerRememberFakerelRef,
             * which is not we expect, so we use ResourceOwnerForgetFakerelRef correspondingly */
            if (!IsBootstrapProcessingMode()) {
                ResourceOwnerForgetFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, (*partIndex)->partrel);
            }
        } else {
            UpdatePartrelPointer((*partIndex)->partrel, *parentIndex, *partIndex);
        }
        index = (*partIndex)->partrel;
    } else {
        index = partitionGetRelation(*parentIndex, *partIndex);
    }
    return index;
}

/* init the Partition in construct */
void InitPartitionRelationInFusion(Oid partOid, Relation parentRel, Partition* partRel, Relation* rel)
{
    *partRel = partitionOpen(parentRel, partOid, AccessShareLock);
    (void)PartitionGetPartIndexList(*partRel);

    if (PARTITION_ENABLE_CACHE_OPFUSION) {
        if (!(*partRel)->partrel) {
            (*partRel)->partrel = partitionGetRelation(parentRel, *partRel);
            /* in function partitionGetRelation, owner->nfakerelrefs plus one due to ResourceOwnerRememberFakerelRef,
             * which is not we expect, so we use ResourceOwnerForgetFakerelRef correspondingly */
            if (!IsBootstrapProcessingMode()) {
                ResourceOwnerForgetFakerelRef(t_thrd.utils_cxt.CurrentResourceOwner, (*partRel)->partrel);
            }
        } else {
            UpdatePartrelPointer((*partRel)->partrel, parentRel, *partRel);
        }
        *rel = (*partRel)->partrel;
    } else {
        *rel = partitionGetRelation(parentRel, *partRel);
    }
}

/* execute the process of done in construct */
void ExeceDoneInIndexFusionConstruct(bool isPartTbl, Relation* parentRel, Partition* part,
                                            Relation* index, Relation* rel)
{
    if (isPartTbl) {
        partitionClose(*parentRel, *part, AccessShareLock);
        *part = NULL;
        if (*index != NULL) {
            if (!PARTITION_ENABLE_CACHE_OPFUSION) {
                releaseDummyRelation(index);
            }
            *index = NULL;
        }
        if (!PARTITION_ENABLE_CACHE_OPFUSION) {
            releaseDummyRelation(rel);
        }
        heap_close(*parentRel, AccessShareLock);
        *parentRel = NULL;
        *rel = NULL;
    } else {
        heap_close((*index == NULL) ? *rel : *index, AccessShareLock);
        *index = NULL;
        *rel = NULL;
    }
}
