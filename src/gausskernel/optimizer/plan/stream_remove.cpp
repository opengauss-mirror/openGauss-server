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
 * -----------------------------------------------------------------------------------------------
 * Description: remove redundant stream node and limit if needed.
 *              use whitelist to filter plan that can do the remove operator.
 *              redundant stream only include plan whose Tag is T_Stream and son node is a special scan,
 *              a special scan just includes a where clause that distribute_key column equals const str 
 *              and a limit 1 clause.
 *              if want do more thing for redundant stream, the stream plan's father node should be an append plan, 
 *              and the other subplans of this append plan should be redundant stream plans, 
 *              or simple select const value subquery.
 * -----------------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_operator.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/stream_remove.h"
#include "optimizer/streamplan.h"


enum RedundantStreamType {
    EXTRA_OTHER_CASE,
    /* for stream, require redstribute or broadcast of select 1 from table where distribute_key = const scan */
    REDUNDANT_MANY_STREAMS,
    /* for limit, require limit 1. */
    REDUNDANT_MANY_STREAM_LIMITS,
    /* for subquery scan, require select 1 baseresult. */
    REDUNDANT_ONE_STREAM_AND_ONE_SUBQUERY_SCAN,
    REDUNDANT_ONE_STREAM_LIMIT_AND_ONE_SUBQUERY_SCAN
};
typedef struct {
    int count_stream;
    /* no double stream limit, we remove it in set_plan_reference */
    int count_stream_limit;
    int count_subquery_scan;
    RedundantStreamType redundant_stream_type;
} RedundantStreamInfo;

static bool is_limit_one(const Limit *limit)
{
    Assert(limit != NULL);
    Assert(IsA(limit, Limit));
    if (limit->limitOffset != NULL) {
        return false;
    }
    if (limit->limitCount == NULL) {
        return false;
    }
    if (!IsA(limit->limitCount, Const)) {
        return false;
    }
    Const *limitCount = (Const *)limit->limitCount;
    if (!limitCount->constbyval || limitCount->consttype != INT8OID || limitCount->constisnull) {
        return false;
    }
    Datum count = ((Const *)limit->limitCount)->constvalue;
    if (count != 1) {
        return false;
    }
    return true;
}

static bool is_equal_operator(const List *qual, const List *distributed_keys)
{
    if (list_length(qual) != 1) {
        return false;
    }
    if (list_length(distributed_keys) != 1) {
        return false;
    }
    Var *distribute_var = (Var *)lfirst(list_head(distributed_keys));
    OpExpr *opexpr = (OpExpr *)lfirst(list_head(qual));
    if (!IsA(opexpr, OpExpr) || !IsA(distribute_var, Var)) {
        return false;
    }
    const int SUPPORT_EQ = 8;
    const int VAR_OP = 2;
    const Oid var_eq_op_array[SUPPORT_EQ][VAR_OP] = {
        {CHAROID, CHAREQOID},
        {BPCHAROID, BPCHAREQOID},
        {VARCHAROID, TEXTEQOID},
        {INT4OID, INT4EQOID},
        {INT8OID, INT84EQOID},
        {INT8OID, INT8EQOID},
        {NUMERICOID, NUMERICEQOID},
        {TEXTOID, TEXTEQOID}};
    for (int i = 0; i < SUPPORT_EQ; i++) {
        if (distribute_var->vartype == var_eq_op_array[i][0] && opexpr->opno == var_eq_op_array[i][1]) {
            return true;
        }
    }
    return false;
}

static bool is_equal_const(const List *qual, const List *distributed_keys)
{
    Var *distribute_var = (Var *)lfirst(list_head(distributed_keys));
    OpExpr *opexpr = (OpExpr *)lfirst(list_head(qual));
    List *expr_args = opexpr->args;
    Assert(expr_args != NULL);
    const int EQUAL_ARGS_LEN = 2;
    if (list_length(expr_args) != EQUAL_ARGS_LEN) {
        return false;
    }
    Var *expr_var = NULL;
    Const *expr_const = NULL;
    ListCell *arg = NULL;
    foreach(arg, expr_args) {
        Expr *expr = (Expr *)lfirst(arg);
        if (IsA(expr, Var)) {
            expr_var = (Var *)expr;
        } else if (IsA(expr, Const)) {
            expr_const = (Const *)expr;
        } else if (IsA(expr, RelabelType) && ((RelabelType *)expr)->relabelformat == COERCE_IMPLICIT_CAST) {
            Var *var = (Var *)((RelabelType *)expr)->arg;
            if (var != NULL && IsA(var, Var)) {
                expr_var = (Var *)var;
            }
        }
    }
    /* qual is var equal const */
    if (expr_var == NULL || expr_const == NULL) {
        return false;
    }
    /* qual var equal distribute var */
    if (!(expr_var->varno == distribute_var->varno || expr_var->varnoold == distribute_var->varno) 
        || !(expr_var->varattno == distribute_var->varattno || expr_var->varoattno == distribute_var->varattno)) {
        return false;
    }
    return true;
}

static bool is_distribute_key_eq_const_qual(const List *qual, const List *distributed_keys)
{
    Assert(qual != NULL);
    Assert(nodeTag(qual) == T_List);
    Assert(distributed_keys != NULL);
    Assert(IsA(distributed_keys, List));
    if (!is_equal_operator(qual, distributed_keys)) {
        return false;
    }
    if (!is_equal_const(qual, distributed_keys)) {
        return false;
    }
    
    return true;
}

static List *fetch_qual_from_scan(const Scan *scan)
{
    switch (nodeTag(scan)) {
        case T_Scan: {
            return scan->plan.qual;
        }
        case T_SeqScan: {
            return scan->plan.qual;
        }
        case T_IndexScan: {
            return ((IndexScan *)scan)->indexqual;
        }
        case T_BitmapIndexScan: {
            return ((BitmapIndexScan *)scan)->indexqual;
        }
        case T_IndexOnlyScan: {
            return ((IndexOnlyScan *)scan)->indexqual;
        }
        default: {
            return NULL;
        }
    }
}

static bool is_select_const_with_distribute_qual_plan(const Scan *scan)
{
    Assert(scan != NULL);
    if (scan->plan.lefttree != NULL) {
        return false;
    }
    List *qual = fetch_qual_from_scan(scan);
    if (qual == NULL || !IsA(qual, List)) {
        return false;
    }
    List *distributed_keys = scan->plan.distributed_keys;
    if (distributed_keys == NULL || !IsA(distributed_keys, List)) {
        return false;
    }
    if (!is_distribute_key_eq_const_qual(qual, distributed_keys)) {
        return false;
    }
    List *targetlist = scan->plan.targetlist;
    if (targetlist == NULL) {
        return false;
    }
    TargetEntry *targetentry = (TargetEntry *)lfirst(list_head(targetlist));
    if (targetentry == NULL) {
        return false;
    }
    Assert(IsA(targetentry, TargetEntry));
    Const *expr = (Const *)targetentry->expr;
    if (expr == NULL || !IsA(expr, Const)) {
        return false;
    }
    return true;
}

static bool is_stream_limit_plan(const Stream *stream)
{
    Assert(stream != NULL);
    Assert(nodeTag(stream) == T_Stream);
    if (stream->type != STREAM_REDISTRIBUTE && stream->type != STREAM_BROADCAST) {
        return false;
    }
    Plan *plan = stream->scan.plan.lefttree;
    if (plan == NULL) {
        return false;
    }
    Limit *limit = NULL;
    if (IsA(plan, Limit)) {
        limit = (Limit *)plan;
    }
    if (IsA(plan, SubqueryScan)) {
        SubqueryScan *sub = (SubqueryScan *)plan;
        if (stream->type != STREAM_REDISTRIBUTE) {
            return false;
        }
        if (sub->subplan != NULL && IsA(sub->subplan, Limit)) {
            limit = (Limit *)sub->subplan;
        }
    }
    if (limit == NULL) {
        return false;
    }
    if (!is_limit_one(limit)) {
        return false;
    }
    Plan *lefttree = limit->plan.lefttree;
    if (lefttree == NULL) {
        return false;
    }
    switch (nodeTag(lefttree)) {
        case T_Stream: {
            return is_stream_limit_plan((Stream *)lefttree);
        }
        /* fall through, we dont care which type of the scan */
        case T_Scan:
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapIndexScan:
        case T_IndexOnlyScan: {
            return is_select_const_with_distribute_qual_plan((Scan *)lefttree);
        }
        default:
            return false;
    }
}

static bool is_stream_plan(const Stream *stream)
{
    Assert(stream != NULL);
    Assert(nodeTag(stream) == T_Stream);
    if (stream->type != STREAM_REDISTRIBUTE) {
        return false;
    }
    Plan *lefttree = (Plan *)stream->scan.plan.lefttree;
    if (lefttree == NULL) {
        return false;
    }
    switch (nodeTag(lefttree)) {
        /* fall through, we dont care which type of the scan */
        case T_Scan:
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapIndexScan:
        case T_IndexOnlyScan: {
            return is_select_const_with_distribute_qual_plan((Scan *)lefttree);
        }
        default:
            return false;
    }
}

static bool is_select_const_with_hashfilter_plan(const SubqueryScan *subqueryscan)
{
    BaseResult *baseresult = (BaseResult *)subqueryscan->subplan;
    if (baseresult == NULL || !IsA(baseresult, BaseResult)) {
        return false;
    }
    if (baseresult->plan.lefttree != NULL) {
        return false;
    }
    List *qual = subqueryscan->scan.plan.qual;
    if (qual == NULL || list_length(qual) != 1 || !IsA(linitial(qual), HashFilter)) {
        return false;
    }
    
    List *targetlist = baseresult->plan.targetlist;
    if (targetlist == NULL || list_length(targetlist) != 1) {
        return false;
    }
    TargetEntry *targetentry = (TargetEntry *)lfirst(list_head(targetlist));
    if (!IsA(targetentry, TargetEntry)) {
        return false;
    }
    Const *expr = (Const *)targetentry->expr;
    if (expr == NULL || !IsA(expr, Const)) {
        return false;
    }
    return true;
}

static void set_redundant_stream_type(RedundantStreamInfo *redundant_info)
{
    /* we have subqueryscan of select const */
    if (redundant_info->count_subquery_scan != 0) {
        if (redundant_info->count_subquery_scan > 1) {
            /* who will write sql like this */
            redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
            return;
        }
        if (redundant_info->count_stream == 1 && redundant_info->count_stream_limit == 0) {
            redundant_info->redundant_stream_type = REDUNDANT_ONE_STREAM_AND_ONE_SUBQUERY_SCAN;
            return;
        }
        if (redundant_info->count_stream == 0 && redundant_info->count_stream_limit == 1) {
            redundant_info->redundant_stream_type = REDUNDANT_ONE_STREAM_LIMIT_AND_ONE_SUBQUERY_SCAN;
            return;
        }
        redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
        return;
    }
    if (redundant_info->count_stream != 0 && redundant_info->count_stream_limit != 0) {
        /* mix type, dont support yet */
        redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
        return;
    }
    if (redundant_info->count_stream != 0) {
        redundant_info->redundant_stream_type = REDUNDANT_MANY_STREAMS;
        return;
    }
    if (redundant_info->count_stream_limit != 0) {
        redundant_info->redundant_stream_type = REDUNDANT_MANY_STREAM_LIMITS;
        return;
    }
    /* the only case we go here is all three counter are 0, but it should never happen */
    redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
    return;
}

static void lookup_redundant_streams_of_append_plan(const Append *append, 
    RedundantStreamInfo *redundant_info)
{
    Assert(append != NULL);
    Assert(redundant_info != NULL);
    Assert(IsA(append, Append));
    ListCell *subplan;
    foreach(subplan, append->appendplans) {
        Plan *plan = (Plan *)lfirst(subplan);
        switch (nodeTag(plan)) {
            case T_Stream: {
                if (is_stream_limit_plan((Stream *)plan)) {
                    redundant_info->count_stream_limit++;
                    break;
                }
                if (is_stream_plan((Stream *)plan)) {
                    redundant_info->count_stream++;
                    break;
                }
                redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
                return;
            }
            case T_SubqueryScan: {
                if (is_select_const_with_hashfilter_plan((SubqueryScan *)plan)) {
                    redundant_info->count_subquery_scan++;
                    break;
                }
                redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
                return;
            }
            default:
                redundant_info->redundant_stream_type = EXTRA_OTHER_CASE;
                return;
        }
    }
    set_redundant_stream_type(redundant_info);
}

static void optimize_stream_plan(Stream *stream)
{
    if (stream->type != STREAM_REDISTRIBUTE) {
        return;
    }
    Limit *limit = NULL;
    Plan *sub = stream->scan.plan.lefttree;
    if (IsA(sub, Limit)) {
        limit = (Limit *)sub;
    } else if (IsA(sub, SubqueryScan)) {
        limit = (Limit *)((SubqueryScan *)sub)->subplan;
    }
    Assert(limit != NULL);
    if (!IsA(limit, Limit)) {
        return;
    }
    Stream *next_stream = (Stream *)limit->plan.lefttree;
    Assert(next_stream != NULL);
    /* just work with double stream limit */
    if (!IsA(next_stream, Stream) || next_stream->type != STREAM_BROADCAST) {
        return;
    }
    /* actually we dont care what type the next_limit is */
    Limit *next_limit = (Limit *)next_stream->scan.plan.lefttree;
    Assert(next_limit != NULL);
    stream->scan.plan.lefttree = (Plan *)next_limit;
    next_limit->limitCount = limit->limitCount;
    next_limit->limitOffset = limit->limitOffset;
    pfree_ext(next_stream);
    pfree_ext(limit);
    pfree_ext(stream->scan.plan.exec_nodes);
    stream->scan.plan.exec_nodes = (ExecNodes *)copyObject(next_limit->plan.exec_nodes);
}

/* delete double stream limit */
void delete_redundant_streams_of_append_plan(const Append *append)
{
    if (!IsA(append, Append)) {
        return;
    }
    RedundantStreamInfo *redundant_info = (RedundantStreamInfo *)palloc0(sizeof(RedundantStreamInfo));
    lookup_redundant_streams_of_append_plan(append, redundant_info);
    if (redundant_info->redundant_stream_type == EXTRA_OTHER_CASE) {
        return;
    }
    /* we deal with double stream limit here, other case not support yet */
    if (redundant_info->redundant_stream_type != REDUNDANT_ONE_STREAM_LIMIT_AND_ONE_SUBQUERY_SCAN && 
        redundant_info->redundant_stream_type != REDUNDANT_MANY_STREAM_LIMITS) {
        return;
    }
    ListCell *subplan;
    foreach(subplan, append->appendplans) {
        Plan *plan = (Plan *)lfirst(subplan);
        switch (nodeTag(plan)) {
            case T_Stream: {
                Stream *stream = (Stream *)plan;
                optimize_stream_plan(stream);
                break;
            }
            default: {
                break;
            }
        }
    }
    pfree_ext(redundant_info);
}

void copy_nodelist(List *dst_node_list, const List *src_node_list)
{
    Assert(dst_node_list != NULL);
    Assert(src_node_list != NULL);
    ListCell *dst_cell;
    ListCell *src_cell;
    while (list_length(dst_node_list) < list_length(src_node_list)) {
        (void)lappend_int(dst_node_list, -1);
    }
    while (list_length(dst_node_list) > list_length(src_node_list)) {
        (void)list_delete_first(dst_node_list);
    }
    forboth(dst_cell, dst_node_list, src_cell, src_node_list) {
        lfirst_int(dst_cell) = lfirst_int(src_cell);
    }
}

void copy_exec_nodes(ExecNodes *dst_exec_nodes, const ExecNodes *src_exec_nodes)
{
    Assert(dst_exec_nodes != NULL);
    Assert(src_exec_nodes != NULL);
    dst_exec_nodes->baselocatortype = src_exec_nodes->baselocatortype;
    copy_nodelist(dst_exec_nodes->nodeList, src_exec_nodes->nodeList);
}

void delete_redundant_case_of_one_stream_and_one_subquery(const RemoteQuery *top_plan)
{
    BaseResult *result = (BaseResult *)top_plan->scan.plan.lefttree;
    Assert(result != NULL);
    Append *append = (Append *)result->plan.lefttree;
    Assert(append != NULL);
    ListCell *subplan;
    ListCell *stream_subplan = NULL;
    ListCell *subquery_subplan = NULL;
    foreach(subplan, append->appendplans) {
        Plan *sub = (Plan *)lfirst(subplan);
        switch (nodeTag(sub)) {
            case T_Stream: {
                stream_subplan = subplan;
                break;
            }
            case T_SubqueryScan: {
                subquery_subplan = subplan;
                break;
            }
            default: {
                return;
            }
        }
    }
    if (stream_subplan == NULL || subquery_subplan == NULL) {
        return;
    }
    /* delete redundant stream node */
    Stream *stream = (Stream *)lfirst(stream_subplan);
    Plan *plan = stream->scan.plan.lefttree;
    Assert(list_length(plan->exec_nodes->nodeList) == 1);
    pfree_ext(stream);
    lfirst(stream_subplan) = (Plan *)plan;
    
    /* modify execnode of brother node */
    const ExecNodes *src_exec_nodes = plan->exec_nodes;

    /* let this query exec on single node of src_exec_nodes */
    SubqueryScan *subquery = (SubqueryScan *)lfirst(subquery_subplan);
    pfree_ext(subquery->scan.plan.qual);
    copy_exec_nodes(subquery->scan.plan.exec_nodes, src_exec_nodes);

    BaseResult *sub_result = (BaseResult *)subquery->subplan;
    copy_exec_nodes(sub_result->plan.exec_nodes, src_exec_nodes);

    /* modify exec_node of father node */
    copy_exec_nodes(append->plan.exec_nodes, src_exec_nodes);
    copy_exec_nodes(result->plan.exec_nodes, src_exec_nodes);
    copy_exec_nodes(top_plan->exec_nodes, src_exec_nodes);
}

void delete_redundant_case_of_many_stream(const RemoteQuery *top_plan)
{
    BaseResult *result = (BaseResult *)top_plan->scan.plan.lefttree;
    Assert(result != NULL);
    Append *append = (Append *)result->plan.lefttree;
    Assert(append != NULL);
    ListCell *subplan;
    ExecNodes *src_exec_nodes = NULL;
    foreach(subplan, append->appendplans) {
        Plan *sub = (Plan *)lfirst(subplan);
        switch (nodeTag(sub)) {
            case T_Stream: {
                Stream *stream = (Stream *)lfirst(subplan);
                Plan *plan = stream->scan.plan.lefttree;
                Assert(list_length(plan->exec_nodes->nodeList) == 1);
                pfree_ext(stream);
                lfirst(subplan) = (Plan *)plan;
                if (src_exec_nodes == NULL) {
                    src_exec_nodes = (ExecNodes *)copyObject(plan->exec_nodes);
                } else {
                    (void)list_concat_unique_int(src_exec_nodes->nodeList, plan->exec_nodes->nodeList);
                }
                break;
            }
            default: {
                /* we should never go here */
                return;
            }
        }
    }
    Assert(src_exec_nodes != NULL);
    /* modify exec_node of father node */
    copy_exec_nodes(append->plan.exec_nodes, src_exec_nodes);
    copy_exec_nodes(result->plan.exec_nodes, src_exec_nodes);
    copy_exec_nodes(top_plan->exec_nodes, src_exec_nodes);
}


/**
 * for now, we support the specific union all sql which are list here
 * 
 *  case 1:
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'xxx' limit 1 ) 
 *      UNION ALL 
 *      SELECT 1;
 *  case 2:
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'xxx' ) 
 *      UNION ALL 
 *      SELECT 1;
 *  case 3:
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'xxx' limit 1 ) 
 *      UNION ALL 
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'yyy' limit 1 );
 *  case 4:
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'xxx' ) 
 *      UNION ALL 
 *      SELECT * FROM ( SELECT 1 FROM t WHERE  t.distribute = 'yyy' );
 */
void delete_redundant_streams_of_remotequery(RemoteQuery *top_plan)
{
    if (top_plan == NULL || !IsA(top_plan, RemoteQuery)) {
        return;
    }
    BaseResult *result = (BaseResult *)top_plan->scan.plan.lefttree;
    if (result == NULL || !IsA(result, BaseResult)) {
        return;
    }
    Append *append = (Append *)result->plan.lefttree;
    if (append == NULL || !IsA(append, Append)) {
        return;
    }
    RedundantStreamInfo *redundant_info = (RedundantStreamInfo *)palloc0(sizeof(RedundantStreamInfo));
    lookup_redundant_streams_of_append_plan(append, redundant_info);
    if (redundant_info->redundant_stream_type == EXTRA_OTHER_CASE) {
        return;
    }
    switch (redundant_info->redundant_stream_type) {
        case REDUNDANT_ONE_STREAM_AND_ONE_SUBQUERY_SCAN:
        /* fallthrough */
        /* do same thing, delete stream node, and modify execnodes of stream's brother and father to stream's son */
        case REDUNDANT_ONE_STREAM_LIMIT_AND_ONE_SUBQUERY_SCAN: {
            delete_redundant_case_of_one_stream_and_one_subquery(top_plan);
            break;
        }
        case REDUNDANT_MANY_STREAM_LIMITS:
        /* fallthrough */
        /* do same thing, delete stream node, and modify execnodes of stream's father to stream's son */
        case REDUNDANT_MANY_STREAMS: {
            delete_redundant_case_of_many_stream(top_plan);
            break;
        }
        default: {
            break;
        }
    }
    pfree_ext(redundant_info);
}