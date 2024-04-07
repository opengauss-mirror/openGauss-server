/* -------------------------------------------------------------------------
 *
 * streamwalker.cpp
 *      functions related to stream plan.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/plan/streamwalker.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/transam.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_inherits_fn.h"
#include "nodes/parsenodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pgxcship.h"
#include "optimizer/streamplan.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"
#include "parser/parse_merge.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"

static void stream_walker_query_update(Query* query, shipping_context *cxt);
static void stream_walker_query_recursive(Query* query, shipping_context *cxt);
static void stream_walker_query_distinct(Query* query, shipping_context *cxt);
static void stream_walker_query_returning(Query* query, shipping_context *cxt);
static void stream_walker_query_merge(Query* query, shipping_context *cxt);
static void stream_walker_query_upsert(Query *query, shipping_context *cxt);
static void stream_walker_query_rtable(Query* query, shipping_context *cxt);
static void stream_walker_query_exec_direct(Query* query, shipping_context *cxt);
static void stream_walker_query_cte(Query* query, shipping_context *cxt);
static void stream_walker_query_limitoffset(Query* query, shipping_context *cxt);
static void stream_walker_query_targetlist(Query* query, shipping_context *cxt);
static void stream_walker_query_jointree(Query* query, shipping_context *cxt);
static void stream_walker_query_having(Query* query, shipping_context *cxt);
static void stream_walker_query_window(Query* query, shipping_context *cxt);
static void stream_walker_finalize_cxt(Query* query, shipping_context *cxt);
static void stream_walker_query(Query* query, shipping_context *cxt);
static void stream_walker_target_entry(TargetEntry* te, shipping_context *cxt);
static void stream_walker_func_expr(FuncExpr* func, shipping_context *cxt);
static void stream_walker_aggref(Aggref* aggref, shipping_context *cxt);
static void stream_walker_coerce(CoerceViaIO* cvio, shipping_context *cxt);
static bool contains_unsupport_tables(List* rtable, Query* query, shipping_context* context);
static bool contains_unsupport_tables(List* rtable, Query* query, shipping_context* context);
static bool table_contain_unsupport_feature(Oid relid, Query* query);
static bool contain_unsupport_function(Oid funcId);
static bool rel_contain_unshippable_feature(RangeTblEntry* rte, shipping_context* context, CmdType commandType);
static void inh_shipping_context(shipping_context *dst, shipping_context *src);
static bool contain_unsupport_expression(Node* expr, void* context);


static uint unsupport_func[] = {
    BYTEASTRINGAGGFUNCOID,       // string_agg
    EVERYFUNCOID,                // every
    XMLAGGFUNCOID,               // xmlagg
    CURRVALFUNCOID,              // curval
    SETVAL1FUNCOID,              // setval
    SETVAL3FUNCOID,              // setval
    LASTVALFUNCOID,              // lastval
    PGBACKENDPIDFUNCOID,         // pg_backend_pid
    PGSTATGETBACKENDPIDFUNCOID,  // pg_stat_get_backend_pid
    PERCENTILECONTAGGFUNCOID,    // percentile_cont
    MODEAGGFUNCOID,              // mode
    FLOAT8MEDIANOID,             // median(float8)
    INTERVALMEDIANOID,           // median(interval)
    FIRSTAGGFUNCOID,             // first
    LASTAGGFUNCOID,              // last
    JSONAGGFUNCOID,              // json_agg
    JSONOBJECTAGGFUNCOID         // json_object_agg
};

/*
 * Walk through the node to see if it's supported under the Stream mode.
 * Return true means unsupported, false means supported.
 */
bool stream_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;

    shipping_context *cxt = (shipping_context*)context;

    switch (nodeTag(node)) {
        case T_Query: {
            stream_walker_query((Query*)node, cxt);
        } break;
        case T_TargetEntry: {
            stream_walker_target_entry((TargetEntry*)node, cxt);
        } break;
        case T_FuncExpr: {
            stream_walker_func_expr((FuncExpr*)node, cxt);
        } break;
        case T_Aggref: {
            stream_walker_aggref((Aggref*)node, cxt);
        } break;
        case T_CoerceViaIO: {
            stream_walker_coerce((CoerceViaIO*) node, cxt);
        } break;
        default:
            break;
    }
    return expression_tree_walker(node, (bool (*)())stream_walker, context);
}

static bool containReplicatedTable(List *rtable)
{
    ListCell *lc = NULL;
    foreach(lc, rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        if (IsLocatorReplicated(rte->locator_type)) {
            return true;
        }
    }
    return false;
}

static void stream_walker_query_insertinto_rep(Query* query, shipping_context *cxt)
{
    if (!cxt->current_shippable) {
        return;
    }
    if (query->commandType != CMD_INSERT || linitial2_int(query->resultRelations) == 0 ||
        !IsLocatorReplicated(rt_fetch(linitial_int(query->resultRelations), query->rtable)->locator_type)) {
        return;
    }
    ListCell *lc = NULL;
    int index = 0;
    foreach(lc, query->rtable) {
        index++;
        if (index == linitial_int(query->resultRelations)) {
            continue;
        }
        RangeTblEntry *rte = rt_fetch(index, query->rtable);
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL) {
            continue;
        }

        if (rte->subquery->hasWindowFuncs && containReplicatedTable(rte->subquery->rtable)) {
            cxt->current_shippable = false;
            break;
        }

        /* Cannot shipping if there are junk tlists in replicated subquery */
        if (check_replicated_junktlist(rte->subquery)) {
            cxt->current_shippable = false;
            break;
        }
    }

    if (!cxt->current_shippable) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "\"insert into replicated table with select rep table with winfunc\" can not be shipped");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
    }
}

static void stream_walker_query_update(Query* query, shipping_context *cxt)
{
    /*
     * Concurrent update under stream mode is not yet supported.
     * When u_sess->attr.attr_sql.enable_stream_concurrent_update is off, we will return true to
     * generate non-stream plan for update statements.
     */
    if (query->commandType == CMD_UPDATE && !u_sess->attr.attr_sql.enable_stream_concurrent_update) {
        cxt->current_shippable = false;
    }

    if (query->hasForUpdate) {
        /* turn off dop for FOR UPDATE/SHARE query */
        u_sess->opt_cxt.query_dop = 1;
    }
}

static void stream_walker_query_recursive(Query* query, shipping_context *cxt)
{
    if (query->hasRecursive) {
        /* If query contains recursive union, turn off dop */
        u_sess->opt_cxt.query_dop = 1;
    
        if (!u_sess->attr.attr_sql.enable_stream_recursive) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "\"With Recursive\" can not be shipped, since GUC enable_stream_recursive is turned off");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        }
    }
}

static void stream_walker_query_distinct(Query* query, shipping_context *cxt)
{
    if (query->hasDistinctOn) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "\"Distinct On\" can not be shipped");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
}

static void stream_walker_query_returning(Query* query, shipping_context *cxt)
{
    if (query->returningList) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "\"Returning\" can not be shipped");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
}

static void stream_walker_query_merge(Query* query, shipping_context *cxt)
{
    if (query->commandType == CMD_MERGE) {
        if (expression_tree_walker(
            (Node*)query->mergeSourceTargetList, (bool (*)())stream_walker, (void *)cxt)) {
            cxt->current_shippable = false;
        }
    
        ListCell* lc2 = NULL;
        foreach (lc2, query->mergeActionList) {
            MergeAction* mc = (MergeAction*)lfirst(lc2);
            if (mc->commandType == CMD_INSERT) {
                if (rel_contain_unshippable_feature(
                    (RangeTblEntry*)list_nth(query->rtable, query->mergeTarget_relation - 1),
                    cxt, mc->commandType)) {
                    cxt->current_shippable = false;
                }
    
                bool saved_is_nextval_shippable = cxt->is_nextval_shippable;
                if (cxt->allow_func_in_targetlist) {
                    cxt->is_nextval_shippable = true;
                    if (expression_tree_walker(
                        (Node*)mc->targetList, (bool (*)())stream_walker, (void *)cxt)) {
                        cxt->current_shippable = false;
                    }
    
                    cxt->is_nextval_shippable = saved_is_nextval_shippable;
                    cxt->allow_func_in_targetlist = false;
                } else {
                    if (expression_tree_walker((Node*)mc->targetList, (bool (*)())stream_walker, (void *)cxt)) {
                        cxt->current_shippable = false;
                    }
                }
    
                if (expression_tree_walker((Node*)mc->qual, (bool (*)())stream_walker, (void *)cxt)) {
                    cxt->current_shippable = false;
                }
            } else {
                if (expression_tree_walker((Node*)mc, (bool (*)())stream_walker, (void *)cxt)) {
                    cxt->current_shippable = false;
                }
            }
#ifndef ENABLE_MULTIPLE_NODES
            /* if insert or update has subquery, do not use smp */
            if (contain_subquery_walker((Node*)mc, NULL)) {
                cxt->current_shippable = false;
                break;
            }
#endif
        }
    }
}

static void stream_walker_query_upsert(Query *query, shipping_context *cxt)
{
    if (query->commandType == CMD_INSERT && query->upsertClause != NULL) {
        /* For replicated table in stream, upsertClause cannot contain unshippable expression */
        if (linitial_int(query->resultRelations) &&
            IsLocatorReplicated(rt_fetch(linitial_int(query->resultRelations), query->rtable)->locator_type)) {
            bool saved_disallow_volatile_func_shippable = cxt->disallow_volatile_func_shippable;
            cxt->disallow_volatile_func_shippable = true;
            if (expression_tree_walker((Node *)query->upsertClause, (bool (*)())stream_walker, (void *)cxt)) {
                cxt->current_shippable = false;
            }
            cxt->disallow_volatile_func_shippable = saved_disallow_volatile_func_shippable;
        }
    }
}

static void stream_walker_query_rtable(Query* query, shipping_context *cxt)
{
    if (contains_unsupport_tables(query->rtable, query, cxt)) {
        cxt->current_shippable = false;
    }

    if (query->commandType != CMD_SELECT && 
        linitial_int(query->resultRelations) <= list_length(query->rtable) &&
        rel_contain_unshippable_feature((RangeTblEntry*)list_nth(query->rtable,
            linitial_int(query->resultRelations) - 1), cxt, query->commandType)) {
            cxt->current_shippable = false;
    }
}

static void stream_walker_query_exec_direct(Query* query, shipping_context *cxt)
{
    if (query->is_local) {  /* execute direct */
        cxt->current_shippable = false;
    }
}

static void stream_walker_query_cte(Query* query, shipping_context *cxt)
{
    /*
     * Random func is not allowed in CTE and limit.
     * EC func is not allowed in CTE
     */
    bool random_ori = cxt->is_randomfunc_shippable;
    bool ecfunc_ori = cxt->is_ecfunc_shippable;
    cxt->is_randomfunc_shippable = false;
    cxt->is_ecfunc_shippable = false;

    /* walk the entire query tree to analyse the query */
    ListCell* lc = NULL;
    foreach (lc, query->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
        if (cte->cterecursive) {
            /* Recursive cte does't support dn gather. */
            ((shipping_context*)cxt)->disable_dn_gather = true;  
        }
        (void)stream_walker((Node*)cte->ctequery, (void *)cxt);
    }

    cxt->is_ecfunc_shippable = ecfunc_ori;
    cxt->is_randomfunc_shippable = random_ori;
}

static void stream_walker_query_limitoffset(Query* query, shipping_context *cxt)
{
    /*
     * Random func is not allowed in CTE and limit.
     */
    bool random_ori = cxt->is_randomfunc_shippable;
    cxt->is_randomfunc_shippable = false;

    (void)stream_walker((Node*)query->limitCount, (void *)cxt);
    (void)stream_walker((Node*)query->limitOffset, (void *)cxt);

    cxt->is_randomfunc_shippable = random_ori;
}

static void stream_walker_query_targetlist(Query* query, shipping_context *cxt)
{
    if (cxt->allow_func_in_targetlist) {
        cxt->is_nextval_shippable = true;
        if (expression_tree_walker((Node*)query->targetList, (bool (*)())stream_walker, (void *)cxt)) {
            cxt->current_shippable = false;
        }
        cxt->is_nextval_shippable = false;
        cxt->allow_func_in_targetlist = false;
    } else {
        if (expression_tree_walker((Node*)query->targetList, (bool (*)())stream_walker, (void *)cxt)) {
            cxt->current_shippable = false;
        }
    }
}

static void stream_walker_query_jointree(Query* query, shipping_context *cxt)
{
    if (query->jointree != NULL &&
        expression_tree_walker((Node*)query->jointree->fromlist, (bool (*)())stream_walker, (void *)cxt)) {
        cxt->current_shippable = false;
    }
    if (query->jointree != NULL &&
        contain_unsupport_expression((Node*)query->jointree->quals, (void *)cxt)) {
        cxt->current_shippable = false;
    }
}

static void stream_walker_query_having(Query* query, shipping_context *cxt)
{
    if (stream_walker((Node*)query->havingQual, (void *)cxt)) {
        cxt->current_shippable = false;
    }
}

static void stream_walker_query_window(Query* query, shipping_context *cxt)
{
    if (expression_tree_walker((Node*)query->windowClause, (bool (*)())stream_walker, (void *)cxt)) {
        cxt->current_shippable = false;
    }
}

static void stream_walker_finalize_cxt(Query* query, shipping_context *cxt)
{
    ListCell *lc = NULL;

    if (cxt->current_shippable) {
        foreach(lc, query->rtable) {
            RangeTblEntry *tmp_rte = (RangeTblEntry *) lfirst(lc);
            if (tmp_rte->rtekind == RTE_SUBQUERY && !tmp_rte->subquery->can_push) {
                cxt->current_shippable = false;
                break;
            }
        }
    } else {
        if (query->rtable == NIL) {
            cxt->query_shippable = false;
        } else {
            foreach(lc, query->rtable) {
                RangeTblEntry *tmp_rte = (RangeTblEntry *) lfirst(lc);
                if (tmp_rte->rtekind == RTE_RELATION ||
                    tmp_rte->rtekind == RTE_FUNCTION ||
                    tmp_rte->rtekind == RTE_VALUES) {
                    cxt->query_shippable = false;
                    break;
                }
            }
        }
    }
}

static void stream_walker_query(Query* query, shipping_context *cxt)
{
    /* Set default value of query's can_push. We will modify it according to conditions below. */
    bool save_shippable = cxt->current_shippable;
    cxt->current_shippable = true;
    cxt->query_list = lappend(cxt->query_list, query);
    cxt->query_count = cxt->query_count + 1;

    stream_walker_query_update(query, cxt);
    stream_walker_query_recursive(query, cxt);
    stream_walker_query_distinct(query, cxt);
    stream_walker_query_returning(query, cxt);
    stream_walker_query_rtable(query, cxt);
    stream_walker_query_exec_direct(query, cxt);
    stream_walker_query_merge(query, cxt);
    stream_walker_query_upsert(query, cxt);
    stream_walker_query_cte(query, cxt);
    stream_walker_query_limitoffset(query, cxt);
    stream_walker_query_targetlist(query, cxt);
    stream_walker_query_jointree(query, cxt);
    stream_walker_query_having(query, cxt);
    stream_walker_query_window(query, cxt);

    stream_walker_query_insertinto_rep(query, cxt);
    /* mark shippable flag based on rte shippbility */
    stream_walker_finalize_cxt(query, cxt);

    if (list_length(query->resultRelations) > 1) {
        /* turn off push for multiple modify */
        cxt->current_shippable = false;
    }

    /* Mark query's can_push and global_shippable flag. */
    query->can_push = cxt->current_shippable;
    cxt->global_shippable = cxt->global_shippable && cxt->current_shippable;
    cxt->query_list = list_delete(cxt->query_list, query);
    cxt->current_shippable = save_shippable;
}

static void stream_walker_target_entry(TargetEntry* te, shipping_context *cxt)
{
    if (contain_unsupport_expression((Node*)te->expr, (void *)cxt)) {
        cxt->current_shippable = false;
    }
    /* Handle case like 'select t from t;' */
    if (IsA(te->expr, Var) && !te->resjunk && ((Var*)te->expr)->varattno == 0) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Table in TargetList can not be shipped");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
}
#ifndef ENABLE_MULTIPLE_NODES
static bool vector_search_func_shippable(Oid funcid)
{
    return true;
}
#endif
static void stream_walker_func_expr(FuncExpr* func, shipping_context *cxt)
{
    uint32 i = 0;
    
    if (pgxc_is_shippable_func_contain_any(func->funcid)) {
        /* the args type of concat() and concat_ws() contains ANY, that may cause unshippable */
        if (contain_unsupport_expression((Node*)func->args, (void *)cxt)) {
            cxt->current_shippable = false;
        }
    }
    if (!pgxc_is_func_shippable(func->funcid, cxt)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Function %s() can not be shipped",
            get_func_name(func->funcid));
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
    /* EC function is of record type, but we ship it in some cases */
    if (func->funcid != ECEXTENSIONFUNCOID && func->funcid != ECHADOOPFUNCOID &&
        func->funcresulttype == RECORDOID && !vector_search_func_shippable(func->funcid)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Function %s() can not be shipped because return record",
            get_func_name(func->funcid));
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
    for (i = 0; i < lengthof(unsupport_func); i++) {
        if (func->funcid == unsupport_func[i]) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Function %s() can not be shipped",
                get_func_name(func->funcid));
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        }
    }

    if (NEXTVALFUNCOID == func->funcid &&
        (g_instance.attr.attr_common.lastval_supported || u_sess->attr.attr_common.enable_beta_features)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Function Nextval() can not be shipped when 'lastval_supported'"
            "or 'enable_beta_features' is on");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
}

static void stream_walker_aggref(Aggref* aggref, shipping_context *cxt)
{
    if (contain_unsupport_function(aggref->aggfnoid)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Function %s() can not be shipped",
            get_func_name(aggref->aggfnoid));
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        cxt->current_shippable = false;
    }
}

static void stream_walker_coerce(CoerceViaIO* cvio, shipping_context *cxt)
{
    /* Query like:
     *  select (a.*)::text from view a;
     * can't be shipped, since the defination of VIEW doesn't exists on datanode.
     */
    if (IsA(cvio->arg, Var)) {
        Var* var = (Var*)(cvio->arg);
        if (var->varattno == InvalidAttrNumber) {
            /*
             * Sometimes Var references outer relation, we find the corresponding Query according to the
             * context->query_list and the varlevelsup.
             */
            int len = list_length(cxt->query_list);
            int query_level = len - var->varlevelsup;
            Query* query = (Query*)list_nth(cxt->query_list, query_level - 1);
            RangeTblEntry* rte = rt_fetch(var->varno, query->rtable);
    
            if (rte->relkind == RELKIND_VIEW || rte->relkind == RELKIND_CONTQUERY) {
                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH,
                    "\"%s\" is VIEW that will be treated as Record type can't be shipped",
                    rte->relname);
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                cxt->current_shippable = false;
            }
        }
    }
}

static void inh_shipping_context(shipping_context *dst, shipping_context *src)
{
    if (!src->current_shippable) {
        dst->current_shippable = src->current_shippable;
    }

    if (!src->query_shippable) {
        dst->query_shippable = src->query_shippable; 
    }

    if (!src->global_shippable) {
        dst->global_shippable = src->global_shippable;
    }

    if (src->disable_dn_gather) {
        dst->disable_dn_gather = src->disable_dn_gather;
    }
}

static bool contains_unsupport_tables(List* rtable, Query* query, shipping_context* context)
{
    ListCell* item = NULL;
    char target_table_loctype = LOCATOR_TYPE_HASH;
    int rIdx = 0;
    shipping_context scontext;
    errno_t sprintf_rc = 0;
    errno_t rc = memcpy_s(&scontext, sizeof(scontext), context, sizeof(scontext));
    securec_check(rc, "\0", "\0");

    /* random func and EC func can't be shippable when it appears in CTE,
     * we set context->is_randomfunc_shippable and context->is_ecfunc_shippable
     * be false in stream_walker when walker in CTE.
     */
    scontext.is_randomfunc_shippable =
        u_sess->opt_cxt.is_randomfunc_shippable && context->is_randomfunc_shippable && IS_STREAM_PLAN;
    scontext.is_ecfunc_shippable = context->is_ecfunc_shippable && IS_STREAM_PLAN;
    scontext.current_shippable = true;
    scontext.query_shippable = true;
    scontext.global_shippable = true;
    scontext.disable_dn_gather = false;
    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);
        rIdx++;

        switch (rte->rtekind) {
            case RTE_RELATION: {
                if (table_contain_unsupport_feature(rte->relid, query) &&
                    !u_sess->attr.attr_sql.enable_cluster_resize) {
                    context->current_shippable = false;
                    return true;
                }

                rte->locator_type = GetLocatorType(rte->relid);
                /* SQLONHADOOP has to support RROBIN MODULO distribution mode */
                if (((rte->locator_type == LOCATOR_TYPE_RROBIN || rte->locator_type == LOCATOR_TYPE_MODULO) &&
                    rte->relkind != RELKIND_FOREIGN_TABLE && rte->relkind != RELKIND_STREAM)) {
                    sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "Table %s can not be shipped",
                        get_rel_name(rte->relid));
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    context->current_shippable = false;
                    return true;
                }
                if (rte->inh && has_subclass(rte->relid)) {
                    sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "Table %s inherited can not be shipped",
                        get_rel_name(rte->relid));
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    context->current_shippable = false;
                    return true;
                }

                if (query->commandType == CMD_INSERT && list_length(rtable) == 2 && rIdx == 1)
                    target_table_loctype = rte->locator_type;

                break;
            }
            case RTE_SUBQUERY: {
                /*
                 * We allow to push the nextval and uuid_generate_v1 to DN for the following query:
                 * 	  insert into t1 select nextval('seq1'),* from t2;
                 * 	  insert into t1 select uuid_generate_v1, * from t2;
                 * It fullfill the following conditions:
                 * 1. Top level query is Insert.
                 * 2. There are two RTE in rtable, the first one is the target table,
                 *    which should be hash/range/list distributed.
                 *    The second one is a subquery
                 * We allow the the nextval and uuid_generate_v1 in the target list of the subquery.
                 */
                bool supportLoctype = (target_table_loctype == LOCATOR_TYPE_HASH ||
                                      IsLocatorDistributedBySlice(target_table_loctype) ||
                                      target_table_loctype == LOCATOR_TYPE_NONE);
                if (query->commandType == CMD_INSERT && list_length(rtable) == 2 && rIdx == 2 &&
                    supportLoctype) {
                    scontext.allow_func_in_targetlist = true;
                }

                (void)stream_walker((Node*)rte->subquery, (void*)(&scontext));

                inh_shipping_context(context, &scontext);

                scontext.allow_func_in_targetlist = false;

                break;
            }
            case RTE_FUNCTION: {
                (void)stream_walker((Node*)rte->funcexpr, (void*)(&scontext));

                inh_shipping_context(context, &scontext);

                break;
            }
            case RTE_VALUES: {
                (void)stream_walker((Node*)rte->values_lists, (void*)(&scontext));

                inh_shipping_context(context, &scontext);

                break;
            }
            default: {
                break;
            }
        }
    }

    return false;
}

static bool rel_contain_unshippable_feature(RangeTblEntry* rte, shipping_context* context, CmdType commandType)
{
    errno_t sprintf_rc = 0;
    if (rte->rtekind == RTE_RELATION) {
        if (commandType == CMD_INSERT) {
            Relation rel = relation_open(rte->relid, AccessShareLock);
            /* if the result relation has oid column, go to old way */
            if (rel->rd_rel->relhasoids) {
                relation_close(rel, AccessShareLock);
                sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH,
                    "TargetList with OID type can not be shipped");
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                context->current_shippable = false;
                return true;
            }
            relation_close(rel, AccessShareLock);

            /*
             * Check if nextval and uuid_generate_v1 can be shipped to DN or not.
             * We don't allow FQS for nextval and uuid_generate_v1
             * But in order to increase the performance of bulkload, we allow streaming plan
             * if 1. the target table is hash/range/list distributed table
             *  2. the nextval and uuid_generate_v1 function existed in the target list of the result table
             */
            if (rte->locator_type == LOCATOR_TYPE_HASH || IsLocatorDistributedBySlice(rte->locator_type) ||
                rte->locator_type == LOCATOR_TYPE_NONE) {
                context->allow_func_in_targetlist = true;
            }
        }

        /* Disallow volatile function shippable when the target relation is replicated. */
        if (IsLocatorReplicated(rte->locator_type)) {
            context->disallow_volatile_func_shippable = true;
        }
    }
    return false;
}

/*
 * Attempt to check there are all deferable triggers or not, if yes try to push it to datdanodes.
 * Then stream_walker could refer true under constraints DEFERABLE.
 */
static bool check_trigger_deferable(Relation rel)
{
    HeapTuple indexTuple = NULL;
    Form_pg_index indexStruct = NULL;
    ListCell* item = NULL;
    bool deferablesCheck = false;

    List *indexList = (List *)RelationGetIndexList(rel);

    /* no constaint then retrun true directly. */
    if (indexList == NIL) {
        return true;
    }

    foreach (item, indexList) {
        Oid indexoid = lfirst_oid(item);

        indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if (!HeapTupleIsValid(indexTuple)) {
            ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for index %u", indexoid)));
        }

        indexStruct = (Form_pg_index)GETSTRUCT(indexTuple);
        if (!IndexIsValid(indexStruct)) {
            deferablesCheck = true;
            ReleaseSysCache(indexTuple);
            continue;
        }

        if (!indexStruct->indimmediate) {
            ReleaseSysCache(indexTuple);
            continue;
        }

        /* if indexTuple is invalid or normal(not deferable), then cannot pushable. */
        deferablesCheck = true;
        ReleaseSysCache(indexTuple);
    }

    return deferablesCheck;
}

static bool table_contain_unsupport_feature(Oid relid, Query* query)
{
    Relation rel;
    errno_t sprintf_rc = 0;

    rel = try_relation_open(relid, NoLock);
    if (rel != NULL) {
        /* If contains system relation, we will not output the not shipping reasion */
        if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE) {
            u_sess->opt_cxt.not_shipping_info->need_log = false;
        }

        /* globel temp table could not ship */
        if (rel->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "global template table not support stream operator.");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            relation_close(rel, NoLock);
            return true;
        }

        /* Currently dml with trigger can not get stream plan. */
        if (rel->rd_rel->relhastriggers && NULL != rel->trigdesc &&
            ((query->commandType == CMD_INSERT && pgxc_has_trigger_for_event(TRIGGER_TYPE_INSERT, rel->trigdesc)) ||
            (query->commandType == CMD_UPDATE && check_trigger_deferable(rel) &&
            pgxc_has_trigger_for_event(TRIGGER_TYPE_UPDATE, rel->trigdesc)) ||
            (query->commandType == CMD_DELETE && pgxc_has_trigger_for_event(TRIGGER_TYPE_DELETE, rel->trigdesc)))) {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Table %s with trigger can not be shipped",
                get_rel_name(relid));
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            relation_close(rel, NoLock);
            return true;
        }

        relation_close(rel, NoLock);
    }

    return false;
}

static bool contain_unsupport_function(Oid funcId)
{
    if (funcId >= FirstNormalObjectId) {
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
        if (u_sess->hook_cxt.aggSmpHook != NULL) {
            return ((aggSmpFunc)(u_sess->hook_cxt.aggSmpHook))(funcId);
        }
#endif
        return true;
    }

    for (uint i = 0; i < lengthof(unsupport_func); i++) {
        if (funcId == unsupport_func[i])
            return true;
    }

    return false;
}

static bool contain_unsupport_expression(Node* expr, void* context)
{
    if (expr == NULL) {
        return false;
    }

    errno_t sprintf_rc = 0;
    shipping_context* cxt = (shipping_context *)context;

    switch (nodeTag(expr)) {
        case T_RowExpr: {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "\"Row () Expression\" can not be shipped");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        } break;
        case T_RowCompareExpr: {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Row Compare Expression  can not be shipped");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        } break;
        case T_ArrayExpr: {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Array Expression  can not be shipped");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        } break;
        case T_ArrayCoerceExpr: {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Cast Array Expression  can not be shipped");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        } break;
        case T_List: {
            ListCell* temp = NULL;
            foreach (temp, (List*)expr) {
                if (contain_unsupport_expression((Node*)lfirst(temp), context)) {
                    cxt->current_shippable = false;
                }
            }
        } break;
        case T_WindowFunc: {
            WindowFunc* winfunc = (WindowFunc*)expr;
            ListCell* temp = NULL;

            /*
             * We need to be extra careful with row_number() in INSERT/UPDATE/DELETE/MERGE
             * on replicated relations, since the data on DNs doesn't neccessarily have the same order
             * for example:
             * INSERT INTO rep SELECT c1, c2, row_number() OVER (order by c1) as rn FROM rep WHERE c1 = 1;
             * The output of column rep.c2 in SELECT may vary depends on the order of data in rep.c2,
             * which is not allowed in replicated relations. Thus, we need to make sure the row_number()
             * doesn't ship in this case.
             */
            if (winfunc->winfnoid == ROWNUMBERFUNCOID) {
                foreach (temp, cxt->query_list) {
                    Query* query = (Query*)lfirst(temp);
                    if (linitial2_int(query->resultRelations) &&
                        GetLocatorType(rt_fetch(linitial_int(query->resultRelations), query->rtable)->relid) == 'R') {
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "row_number() can not be shipped when INSERT/UPDATE/DELETE a replication table");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        cxt->current_shippable = false;
                    }
                }
            }
        } break;
        case T_OpExpr: {
            OpExpr* op = (OpExpr*)expr;
            if (contain_unsupport_expression((Node*)op->args, context)) {
                cxt->current_shippable = false;
            }
        } break;
        case T_BoolExpr: {
            BoolExpr* be = (BoolExpr*)expr;
            if (contain_unsupport_expression((Node*)be->args, context)) {
                cxt->current_shippable = false;
            }
        } break;
        case T_FuncExpr: {
            FuncExpr* func = (FuncExpr*)expr;
            if (pgxc_is_shippable_func_contain_any(func->funcid)) {
                Oid valtype;
                Oid typOutput;
                bool typIsVarlena = false;

                for (int j = 0; j < list_length(func->args); j++) {
                    valtype = get_call_expr_argtype((Node*)func, j);
                    if (!OidIsValid(valtype)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                                errmsg("could not determine data type of concat() input")));
                    }
                    getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
                    if (!pgxc_is_func_shippable(typOutput, cxt)) {
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "Function %s() can not be shipped because call %s() implicitly",
                            get_func_name(func->funcid),
                            get_func_name(typOutput));
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        cxt->current_shippable = false;
                    }
                }
            }
        } break;
        case T_Rownum: {
            sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "Rownum can not be shipped.");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            cxt->current_shippable = false;
        } break;
        default:
            /* Record return type is not stream supported */
            if (exprType(expr) == RECORDOID) {
                sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH,
                    "Type of Record in TargetList can not be shipped");
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                cxt->current_shippable = false;
            }
            break;
    }
    return expression_tree_walker(expr, (bool (*)())stream_walker, context);
}
