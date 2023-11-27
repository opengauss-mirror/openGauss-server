/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 *  pgxcplan_single.cpp
 *        The main part of the bypass executor. Instead of processing through the origin
 *            Portal executor, the bypass executor provides a shortcut when the query is
 *            simple.
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/plan/pgxcplan_single.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "parser/parse_func.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"

/*
 * Check whether the current statement supports Stream based on the status of 'context' and 'query'.
 * If Stream is supported, a copy of the 'query' is returned as a backup in case generating a plan
 * with Stream fails.
 */
static Query* check_shippable(bool *stream_unsupport, Query* query, shipping_context* context)
{
    if (u_sess->attr.attr_sql.rewrite_rule & PARTIAL_PUSH) {
        *stream_unsupport = !context->query_shippable;
    } else {
        *stream_unsupport = !context->global_shippable;
    }

    if (u_sess->attr.attr_sql.enable_dngather) {
        u_sess->opt_cxt.is_dngather_support = !context->disable_dn_gather;
    } else {
        u_sess->opt_cxt.is_dngather_support = false;
    }

    /* single node do not support parallel query in cursor */
    if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt)) {
        *stream_unsupport = true;
    }

    if (*stream_unsupport || !IS_STREAM) {
        output_unshipped_log();
        set_stream_off();
    } else {
        /*
         * make a copy of query, so we can retry to create an unshippable plan
         * when we fail to generate a stream plan
         */
        return (Query*)copyObject(query);
    }
    return NULL;
}

PlannedStmt* pgxc_planner(Query* query, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = NULL;
    Query* re_query = NULL;
    bool stream_unsupport = true;

    /*
     * Before going into planner, set default work mode.
     */
    set_default_stream();

    if (query->isRowTriggerShippable) {
        /* trigger support shipping only when the whole query can shipping. */
        query->isRowTriggerShippable = false;
    }

#ifdef STREAMPLAN
    if (IS_STREAM) {
        shipping_context context;
        stream_walker_context_init(&context);

        (void)stream_walker((Node*)query, (void*)(&context));
        disable_unshipped_log(query, &context);

        re_query = check_shippable(&stream_unsupport, query, &context);
    } else {
        if (unlikely(u_sess->attr.attr_sql.enable_unshipping_log)) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "\"enable_stream_operator\" is off");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            output_unshipped_log();
        }
        set_stream_off();
    }

#endif
    /*
     * we will create plan with stream first, and if it is not support stream,
     * we should catch the error and recreate plan with no stream.
     */
    MemoryContext current_context = CurrentMemoryContext;
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner tempOwner = NULL;
    /*
     * If the stream-plan is not used, the currentOwner is used to trace resources instead of
     * applying for a temporary owner. This prevents the "memory temporarily unavailable" error
     * caused by memory stacking.
     */
    if (IS_STREAM_PLAN) {
        /*
         * If the stream-plan is used, a temporary owner is used to trace resources. This helps release
         * resources in a unified manner when a stream-plan fails to be generated, preventing resource
         * leakage.
         */
        tempOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "pgxc_planner",
            /*
             * The memory context of the temporary owner must be the same as the currentOwner to ensure
             * that they have the same lifecycle
             */
            ResourceOwnerGetMemCxt(currentOwner));
        t_thrd.utils_cxt.CurrentResourceOwner = tempOwner;
    }

    /* we need Coordinator for evaluation, invoke standard planner */
    PG_TRY();
    {
        result = standard_planner(query, cursorOptions, boundParams);

        /* if we need to do stream-replan for logic-cluster elastic computing */
        if (re_query && QueryNeedPlanB(result)) {
            Query* replan_query = NULL;
            PlannedStmt* ng_planB = NULL;
            bool use_planA = false;

            replan_query = (Query*)copyObject(re_query);
            ng_planB = standard_planner(replan_query, cursorOptions, boundParams);
            elog(DEBUG2,
                "Succeed to replan for query \"%s\" by nodegroup %u",
                query->sql_statement,
                lc_replan_nodegroup);
            lc_replan_nodegroup = InvalidOid;

            /* choose a plan */
            use_planA = WLMChoosePlanA(result);
            if (!use_planA) {
                result = ng_planB;
                elog(DEBUG2, "Plan is reset to according to current resource usage: \"%s\"", query->sql_statement);
            } else {
                elog(DEBUG2, "Plan is not reset to according to current resource usage: \"%s\"", query->sql_statement);
            }
            result->ng_use_planA = use_planA;
            ReSetNgQueryMem(result);
        }
        /* If tempOwner is not NULL, the current plan is a stream-plan using the SMP technology. */
        if (tempOwner != NULL) {
            /*
             * When the stream-plan is successfully generated, the temporary owner tracks the
             * resources opened during the plan generation. Now we put the resources of the
             * stream-plan into the currentOwner for tracking, and release the tempOwner to
             * further reduce the memory. This greatly avoid the "memory temporarily unavailable"
             * error, caused by a large amount of SQLs being executed in a transaction/procedure.
             */
            ResourceOwnerConcat(currentOwner, tempOwner);
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
            ResourceOwnerDelete(tempOwner);
        }
    }
    PG_CATCH();
    {
        ErrorData* edata = NULL;
        MemoryContext ecxt;

        /* save error info */
        ecxt = MemoryContextSwitchTo(current_context);
        edata = CopyErrorData();

        if (SS_STANDBY_MODE_WITH_REMOTE_EXECUTE) {
            LWLockReleaseAll();
            AbortBufferIO();
            UnlockBuffers();
        }

        /*
         * refuse to recreate  plan if
         * 1. no query copy: query have been polluted by rewrite
         * 2. stream unsupport: it is already unshippable plan
         * 3. non unsupport-stream error info
         */
        if (NULL == re_query || stream_unsupport || edata->sqlerrcode != ERRCODE_STREAM_NOT_SUPPORTED) {
            /*
             * Release resources applied in standard_planner, release the tempOwner and reinstate the currentOwner
             * before PG_RE_THROW().
             */
            if (tempOwner != NULL) {
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_LOCKS, false, false);
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
                t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                ResourceOwnerDelete(tempOwner);
            }
            MemoryContextSwitchTo(ecxt);
            PG_RE_THROW();
        }

        /*
         * set stream off if sqlstate is ERRCODE_STREAM_NOT_SUPPORTED,
         * otherwise, rethrow the error.
         */
        if (edata->sqlerrcode == ERRCODE_STREAM_NOT_SUPPORTED) {
            FlushErrorState();
        }

        /* release resource applied in standard_planner of the PG_TRY. */
        if (tempOwner != NULL) {
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_LOCKS, false, false);
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
            ResourceOwnerDelete(tempOwner);
        }

#ifdef STREAMPLAN
        if (OidIsValid(lc_replan_nodegroup)) {
            elog(DEBUG2, "Fail to replan for query \"%s\" by nodegroup %u", query->sql_statement, lc_replan_nodegroup);
            lc_replan_nodegroup = InvalidOid;
        }

        if (!check_stream_support()) {
            set_stream_off();
            result = standard_planner(re_query, cursorOptions, boundParams);
        }
#endif
    }
    PG_END_TRY();

    if (NULL == result) {
        ereport(ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("Fail to generate plan")));
    }

    return result;
}

void stream_walker_context_init(shipping_context *context)
{
    errno_t rc = EOK;

    rc = memset_s(context, sizeof(shipping_context), 0, sizeof(shipping_context));
    securec_check(rc, "\0", "\0");

    context->is_randomfunc_shippable = u_sess->opt_cxt.is_randomfunc_shippable && IS_STREAM_PLAN;
    context->is_ecfunc_shippable = true;
    context->query_list = NIL;
    context->query_count = 0;
    context->current_shippable = true;
    context->query_shippable = true;
    context->global_shippable = true;
}

/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
bool contains_column_tables(List* rtable)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    ListCell* item = NULL;

    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            if (REL_COL_ORIENTED == rte->orientation || REL_PAX_ORIENTED == rte->orientation)
                return true;
        } else if (rte->rtekind == RTE_SUBQUERY && contains_column_tables(rte->subquery->rtable))
            return true;
    }

    return false;
}

List* AddRemoteQueryNode(List* stmts, const char* queryString, RemoteQueryExecType remoteExecType, bool is_temp)
{
    List* result = stmts;

    /* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
    if (remoteExecType == EXEC_ON_NONE)
        return result;

    /* Only a remote Coordinator is allowed to send a query to backend nodes */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        RemoteQuery* step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->sql_statement = (char*)queryString;
        step->exec_type = remoteExecType;
        step->is_temp = is_temp;
        result = lappend(result, step);
    }

    return result;
}

bool pgxc_query_contains_temp_tables(List* queries)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

bool pgxc_query_contains_utility(List* queries)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void pgxc_rqplan_adjust_tlist(PlannerInfo* root, RemoteQuery* rqplan, bool gensql)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool containing_ordinary_table(Node* node)
{
    if (node == NULL || IS_PGXC_DATANODE) {
        return false;
    }

    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)node;

        if (rte->relkind == RELKIND_RELATION && !is_sys_table(rte->relid)) {
            return true;
        } else if (rte->rtekind == RTE_SUBQUERY) {
            Query* subquery = rte->subquery;

            if (containing_ordinary_table((Node*)subquery)) {
                return true;
            }
        }
        return false;
    }

    if (IsA(node, Query)) {
        bool result = false;
        result = query_tree_walker((Query*)node, (bool (*)())containing_ordinary_table, NULL, QTW_EXAMINE_RTES);
        return result;
    }

    return expression_tree_walker(node, (bool (*)())containing_ordinary_table, NULL);
}

Plan* pgxc_make_modifytable(PlannerInfo* root, Plan* topplan)
{
    ModifyTable* mt = (ModifyTable*)topplan;

    /* We expect to work only on ModifyTable node */
    if (!IsA(topplan, ModifyTable))
#ifdef STREAMPLAN
        return topplan;
#else
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("Unexpected node type: %d", topplan->type))));
#endif

    /*
     * PGXC should apply INSERT/UPDATE/DELETE to a Datanode. We are overriding
     * normal openGauss behavior by modifying final plan or by adding a node on
     * top of it.
     * If the optimizer finds out that there is nothing to UPDATE/INSERT/DELETE
     * in the table/s (say using constraint exclusion), it does not add modify
     * table plan on the top. We should send queries to the remote nodes only
     * when there is something to modify.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && root->parse->commandType != CMD_MERGE)
        topplan = create_remotedml_plan(root, topplan, mt->operation);
    else if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && root->parse->commandType == CMD_MERGE) {
        ListCell* lc = NULL;
        foreach (lc, root->parse->mergeActionList) {
            MergeAction* action = (MergeAction*)lfirst(lc);

            if (action->commandType == CMD_INSERT || action->commandType == CMD_UPDATE)
                topplan = create_remote_mergeinto_plan(root, topplan, action->commandType, action);
        }
    }

    return topplan;
}

void pgxc_handle_unsupported_stmts(Query* query)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
bool contains_temp_tables(List* rtable)
{
    ListCell* item = NULL;
    char rel_persistence;

    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            rel_persistence = get_rel_persistence(rte->relid);
            if (rel_persistence == RELPERSISTENCE_TEMP || rel_persistence == RELPERSISTENCE_GLOBAL_TEMP)
                return true;
        } else if (rte->rtekind == RTE_SUBQUERY && contains_temp_tables(rte->subquery->rtable))
            return true;
    }

    return false;
}

Param* pgxc_make_param(int param_num, Oid param_type)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

// in planmain.h defined
Plan* create_remotedml_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_remote_mergeinto_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp, MergeAction* action)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_remotegrouping_plan(PlannerInfo* root, Plan* local_plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_remotequery_plan(PlannerInfo* root, RemoteQueryPath* best_path)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_remotesort_plan(PlannerInfo* root, Plan* local_plan, List* pathkeys)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_remotelimit_plan(PlannerInfo* root, Plan* local_plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
// used in insert_gather_node
RangeTblEntry* make_dummy_remote_rte(char* relname, Alias* alias)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

