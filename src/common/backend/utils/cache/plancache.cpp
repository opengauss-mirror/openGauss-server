/* -------------------------------------------------------------------------
 *
 * plancache.c
 *    Plan cache management.
 *
 * The plan cache manager has two principal responsibilities: deciding when
 * to use a generic plan versus a custom (parameter-value-specific) plan,
 * and tracking whether cached plans need to be invalidated because of schema
 * changes in the objects they depend on.
 *
 * The logic for choosing generic or custom plans is in ChooseCustomPlan,
 * which see for comments.
 *
 * Cache invalidation is driven off sinval events.  Any CachedPlanSource
 * that matches the event is marked invalid, as is its generic CachedPlan
 * if it has one.  When (and if) the next demand for a cached plan occurs,
 * parse analysis and rewrite is repeated to build a new valid query tree,
 * and then planning is performed as normal.  We also force re-analysis and
 * re-planning if the active search_path is different from the previous time.
 *
 * Note that if the sinval was a result of user DDL actions, parse analysis
 * could throw an error, for example if a column referenced by the query is
 * no longer present.  Another possibility is for the query's output tupdesc
 * to change (for instance "SELECT *" might expand differently than before).
 * The creator of a cached plan can specify whether it is allowable for the
 * query to change output tupdesc on replan --- if so, it's up to the
 * caller to notice changes and cope with them.
 *
 * Currently, we track exactly the dependencies of plans on relations and
 * user-defined functions.  On relcache invalidation events or pg_proc
 * syscache invalidation events, we invalidate just those plans that depend
 * on the particular object being modified.  (Note: this scheme assumes
 * that any table modification that requires replanning will generate a
 * relcache inval event.)  We also watch for inval events on certain other
 * system catalogs, such as pg_namespace; but for them, our response is
 * just to invalidate all plans.  We expect updates on those catalogs to
 * be infrequent enough that more-detailed tracking is not worth the effort.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/cache/plancache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "access/transam.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "executor/lightProxy.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "opfusion/opfusion.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/globalplancache.h"
#include "instruments/instr_unique_sql.h"
#ifdef PGXC
#include "commands/prepare.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"

static void DropDataNodeStatements(Plan* planNode);
#endif

const float GROWTH_FACTOR = 1.1;
#include "storage/mot/jit_exec.h"


/*
 * We must skip "overhead" operations that involve database access when the
 * cached plan's subject statement is a transaction control command.
 */
#define IsTransactionStmtPlan(plansource) \
    ((plansource)->raw_parse_tree && IsA((plansource)->raw_parse_tree, TransactionStmt))

static void ReleaseGenericPlan(CachedPlanSource* planSource);
List* RevalidateCachedQuery(CachedPlanSource* planSource);
static bool CheckCachedPlan(CachedPlanSource* planSource);
static CachedPlan* BuildCachedPlan(CachedPlanSource* planSource, List* qList, ParamListInfo boundParams);
static bool ChooseCustomPlan(CachedPlanSource* planSource, ParamListInfo boundParams);
static double CachedPlanCost(CachedPlan* plan);
static void AcquireExecutorLocks(List* stmtList, bool acquire);
static void AcquirePlannerLocks(List* stmtList, bool acquire);
static void ScanQueryForLocks(Query* parseTree, bool acquire);
static bool ScanQueryWalker(Node* node, bool* acquire);
static TupleDesc PlanCacheComputeResultDesc(List* stmtList);

/*
 * InitPlanCache: initialize module during InitPostgres.
 *
 * All we need to do is hook into inval.c's callback lists.
 */
void InitPlanCache(void)
{
    CacheRegisterRelcacheCallback(PlanCacheRelCallback, (Datum)0);
    CacheRegisterPartcacheCallback(PlanCacheRelCallback, (Datum)0);
    CacheRegisterSyscacheCallback(PROCOID, PlanCacheFuncCallback, (Datum)0);
    CacheRegisterSyscacheCallback(NAMESPACEOID, PlanCacheSysCallback, (Datum)0);
    CacheRegisterSyscacheCallback(OPEROID, PlanCacheSysCallback, (Datum)0);
    CacheRegisterSyscacheCallback(AMOPOPID, PlanCacheSysCallback, (Datum)0);
}

/*
 * CreateCachedPlan: initially create a plan cache entry.
 *
 * Creation of a cached plan is divided into two steps, CreateCachedPlan and
 * CompleteCachedPlan.  CreateCachedPlan should be called after running the
 * query through raw_parser, but before doing parse analysis and rewrite;
 * CompleteCachedPlan is called after that.  The reason for this arrangement
 * is that it can save one round of copying of the raw parse tree, since
 * the parser will normally scribble on the raw parse tree.  Callers would
 * otherwise need to make an extra copy of the parse tree to ensure they
 * still had a clean copy to present at plan cache creation time.
 *
 * All arguments presented to CreateCachedPlan are copied into a memory
 * context created as a child of the call-time CurrentMemoryContext, which
 * should be a reasonably short-lived working context that will go away in
 * event of an error.  This ensures that the cached plan data structure will
 * likewise disappear if an error occurs before we have fully constructed it.
 * Once constructed, the cached plan can be made longer-lived, if needed,
 * by calling SaveCachedPlan.
 *
 * rawParseTree: output of raw_parser()
 * queryString: original query text
 * commandTag: compile-time-constant tag for query, or NULL if empty query
 */
CachedPlanSource* CreateCachedPlan(Node* rawParseTree, const char* queryString,
#ifdef PGXC
    const char* stmt_name,
#endif
    const char* commandTag)
{
    CachedPlanSource* planSource = NULL;
    MemoryContext sourceContext;
    MemoryContext oldCxt;

    AssertEreport(queryString != NULL, MOD_OPT, ""); /* required as of 8.4 */

    if (ENABLE_DN_GPC && stmt_name != NULL && stmt_name[0] != '\0') {
        sourceContext = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                               "CachedPlanSource",
                                               ALLOCSET_SMALL_MINSIZE,
                                               ALLOCSET_SMALL_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE,
                                               SHARED_CONTEXT);
    } else {
        /*
         * Make a dedicated memory context for the CachedPlanSource and its
         * permanent subsidiary data.  It's probably not going to be large, but
         * just in case, use the default maxsize parameter.  Initially it's a
         * child of the caller's context (which we assume to be transient), so
         * that it will be cleaned up on error.
         */
        sourceContext = AllocSetContextCreate(CurrentMemoryContext,
                                               "CachedPlanSource",
                                               ALLOCSET_SMALL_MINSIZE,
                                               ALLOCSET_SMALL_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);
    }

    /*
     * Create and fill the CachedPlanSource struct within the new context.
     * Most fields are just left empty for the moment.
     */
    oldCxt = MemoryContextSwitchTo(sourceContext);

    planSource = (CachedPlanSource*)palloc0(sizeof(CachedPlanSource));
    planSource->magic = CACHEDPLANSOURCE_MAGIC;
    planSource->raw_parse_tree = (Node*)copyObject(rawParseTree);
    planSource->query_string = pstrdup(queryString);
    planSource->commandTag = commandTag;
    planSource->param_types = NULL;
    planSource->num_params = 0;
    planSource->parserSetup = NULL;
    planSource->parserSetupArg = NULL;
    planSource->cursor_options = 0;
    planSource->rewriteRoleId = InvalidOid;
    planSource->dependsOnRole = false;
    planSource->fixed_result = false;
    planSource->resultDesc = NULL;
    planSource->search_path = NULL;
    planSource->context = sourceContext;
#ifdef PGXC
    planSource->stmt_name = (stmt_name ? pstrdup(stmt_name) : NULL);
    planSource->stream_enabled = u_sess->attr.attr_sql.enable_stream_operator;
    planSource->cplan = NULL;
    planSource->single_exec_node = NULL;
    planSource->is_read_only = false;
    planSource->lightProxyObj = NULL;
    /* Initialize gplan_is_fqs is true, and set to false when find it's not fqs */
    planSource->gplan_is_fqs = true;
#endif

    planSource->query_list = NIL;
    planSource->relationOids = NIL;
    planSource->invalItems = NIL;
    planSource->query_context = NULL;
    planSource->gplan = NULL;
    planSource->is_oneshot = false;
    planSource->storageEngineType = SE_TYPE_UNSPECIFIED;
    planSource->is_complete = false;
    planSource->is_saved = false;
    planSource->is_valid = false;
    planSource->generation = 0;
    planSource->next_saved = NULL;
    planSource->generic_cost = -1;
    planSource->total_custom_cost = 0;
    planSource->num_custom_plans = 0;
    planSource->mot_jit_context = NULL;
    planSource->opFusionObj = NULL;
    planSource->is_checked_opfusion = false;

    planSource->gpc.is_share = false;
    planSource->gpc.is_insert = false;
    planSource->gpc.is_valid = true;
    planSource->gpc.query_hash_code = 0;
    planSource->gpc.env = NULL;
    planSource->gpc.refcount = 1;
    planSource->gpc.in_revalidate = false;

    if (ENABLE_DN_GPC && stmt_name != NULL && stmt_name[0] != '\0') {
        planSource->gpc.env = GPC->EnvCreate();
        GPC->EnvFill(planSource->gpc.env);
        planSource->gpc.env->plansource = planSource;

        planSource->gpc.is_insert = true;
    }

    MemoryContextSwitchTo(oldCxt);

    return planSource;
}

/*
 * CreateOneShotCachedPlan: initially create a one-shot plan cache entry.
 *
 * This variant of CreateCachedPlan creates a plan cache entry that is meant
 * to be used only once.  No data copying occurs: all data structures remain
 * in the caller's memory context (which typically should get cleared after
 * completing execution).  The CachedPlanSource struct itself is also created
 * in that context.
 *
 * A one-shot plan cannot be saved or copied, since we make no effort to
 * preserve the raw parse tree unmodified.  There is also no support for
 * invalidation, so plan use must be completed in the current transaction,
 * and DDL that might invalidate the querytree_list must be avoided as well.
 *
 * rawParseTree: output of raw_parser()
 * queryString: original query text
 * commandTag: compile-time-constant tag for query, or NULL if empty query
 */
CachedPlanSource* CreateOneShotCachedPlan(Node* rawParseTree, const char* queryString, const char* commandTag)
{
    CachedPlanSource* planSource = NULL;

    Assert(queryString != NULL); /* required as of 8.4 */

    /*
     * Create and fill the CachedPlanSource struct within the caller's memory
     * context.  Most fields are just left empty for the moment.
     */
    planSource = (CachedPlanSource*)palloc0(sizeof(CachedPlanSource));
    planSource->magic = CACHEDPLANSOURCE_MAGIC;
    planSource->raw_parse_tree = rawParseTree;
    planSource->query_string = queryString;
    planSource->commandTag = commandTag;
    planSource->param_types = NULL;
    planSource->num_params = 0;
    planSource->parserSetup = NULL;
    planSource->parserSetupArg = NULL;
    planSource->cursor_options = 0;
    planSource->rewriteRoleId = InvalidOid;
    planSource->dependsOnRole = false;
    planSource->fixed_result = false;
    planSource->resultDesc = NULL;
    planSource->search_path = NULL;
    planSource->context = CurrentMemoryContext;
    planSource->query_list = NIL;
    planSource->relationOids = NIL;
    planSource->invalItems = NIL;
    planSource->query_context = NULL;
    planSource->gplan = NULL;
    planSource->is_oneshot = true;
    planSource->storageEngineType = SE_TYPE_UNSPECIFIED;
    planSource->is_complete = false;
    planSource->is_saved = false;
    planSource->is_valid = false;
    planSource->generation = 0;
    planSource->next_saved = NULL;
    planSource->generic_cost = -1;
    planSource->total_custom_cost = 0;
    planSource->num_custom_plans = 0;
    planSource->mot_jit_context = NULL;

    planSource->gpc.is_share = false;
    planSource->gpc.is_insert = false;

#ifdef PGXC
    planSource->stream_enabled = u_sess->attr.attr_sql.enable_stream_operator;
    planSource->cplan = NULL;
    planSource->single_exec_node = NULL;
    planSource->is_read_only = false;
    planSource->lightProxyObj = NULL;
#endif

    return planSource;
}

/*
 * CompleteCachedPlan: second step of creating a plan cache entry.
 *
 * Pass in the analyzed-and-rewritten form of the query, as well as the
 * required subsidiary data about parameters and such.  All passed values will
 * be copied into the CachedPlanSource's memory, except as specified below.
 * After this is called, GetCachedPlan can be called to obtain a plan, and
 * optionally the CachedPlanSource can be saved using SaveCachedPlan.
 *
 * If queryTreeContext is not NULL, the querytree_list must be stored in that
 * context (but the other parameters need not be).  The querytree_list is not
 * copied, rather the given context is kept as the initial query_context of
 * the CachedPlanSource.  (It should have been created as a child of the
 * caller's working memory context, but it will now be reparented to belong
 * to the CachedPlanSource.)  The queryTreeContext is normally the context in
 * which the caller did raw parsing and parse analysis.  This approach saves
 * one tree copying step compared to passing NULL, but leaves lots of extra
 * cruft in the query_context, namely whatever extraneous stuff parse analysis
 * created, as well as whatever went unused from the raw parse tree.  Using
 * this option is a space-for-time tradeoff that is appropriate if the
 * CachedPlanSource is not expected to survive long.
 *
 * plancache.c cannot know how to copy the data referenced by parserSetupArg,
 * and it would often be inappropriate to do so anyway.  When using that
 * option, it is caller's responsibility that the referenced data remains
 * valid for as long as the CachedPlanSource exists.
 *
 * If the CachedPlanSource is a "oneshot" plan, then no querytree copying
 * occurs at all, and queryTreeContext is ignored; it is caller's
 * responsibility that the passed querytree_list is sufficiently long-lived.
 *
 * plansource: structure returned by CreateCachedPlan
 * querytree_list: analyzed-and-rewritten form of query (list of Query nodes)
 * queryTreeContext: memory context containing querytree_list,
 *                    or NULL to copy querytree_list into a fresh context
 * param_types: array of fixed parameter type OIDs, or NULL if none
 * num_params: number of fixed parameters
 * parserSetup: alternate method for handling query parameters
 * parserSetupArg: data to pass to parserSetup
 * cursor_options: options bitmask to pass to planner
 * fixed_result: TRUE to disallow future changes in query's result tupdesc
 */
void CompleteCachedPlan(CachedPlanSource* planSource, List* querytree_list, MemoryContext queryTreeContext,
    Oid* param_types, int num_params, ParserSetupHook parserSetup, void* parserSetupArg, int cursor_options,
    bool fixed_result, const char* stmt_name, ExecNodes* single_exec_node, bool is_read_only)
{
    MemoryContext source_context = planSource->context;
    MemoryContext oldcxt = CurrentMemoryContext;

    /* Assert caller is doing things in a sane order */
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(!planSource->is_complete);

    /*
     * If caller supplied a queryTreeContext, reparent it underneath the
     * CachedPlanSource's context; otherwise, create a suitable context and
     * copy the querytree_list into it.  But no data copying should be done
     * for one-shot plans; for those, assume the passed querytree_list is
     * sufficiently long-lived.
     */
    if (planSource->is_oneshot) {
        queryTreeContext = CurrentMemoryContext;
        if (ENABLE_DN_GPC && planSource->gpc.is_insert == true) {
            queryTreeContext = g_instance.cache_cxt.global_cache_mem;
        }
    } else if (queryTreeContext != NULL) {
        if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
            MemoryContextSetParent(queryTreeContext, source_context);
        }
        MemoryContextSwitchTo(queryTreeContext);
    } else {
        if (ENABLE_DN_GPC && planSource->gpc.is_insert == true) {
            queryTreeContext = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                      "CachedPlanQuery",
                                                      ALLOCSET_SMALL_MINSIZE,
                                                      ALLOCSET_SMALL_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE,
                                                      SHARED_CONTEXT);
        } else {
            /* Again, it's a good bet the queryTreeContext can be small */
            queryTreeContext = AllocSetContextCreate(source_context,
                                                      "CachedPlanQuery",
                                                      ALLOCSET_SMALL_MINSIZE,
                                                      ALLOCSET_SMALL_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE);
        }
        MemoryContextSwitchTo(queryTreeContext);
        querytree_list = (List*)copyObject(querytree_list);
    }

    /*
     * Use the planner machinery to extract dependencies.  Data is saved in
     * query_context.  (We assume that not a lot of extra cruft is created by
     * this call.)  We can skip this for one-shot plans, and transaction
     * control commands have no such dependencies anyway.
     */
    if (!planSource->is_oneshot && !IsTransactionStmtPlan(planSource)) {
        extract_query_dependencies((Node*)querytree_list,
            &planSource->relationOids,
            &planSource->invalItems,
            &planSource->dependsOnRole,
            &planSource->force_custom_plan);

        /*
         * Also save the current search_path in the query_context.  (This
         * should not generate much extra cruft either, since almost certainly
         * the path is already valid.)  Again, we don't really need this for
         * one-shot plans; and we *must* skip this for transaction control
         * commands, because this could result in catalog accesses.
         */
        planSource->search_path = GetOverrideSearchPath(source_context);
    }

    /* Update RLS info as well. */
    planSource->rewriteRoleId = GetUserId();
    planSource->query_context = queryTreeContext;
    planSource->query_list = querytree_list;

    /*
     * Save the final parameter types (or other parameter specification data)
     * into the source_context, as well as our other parameters.  Also save
     * the result tuple descriptor.
     */
    MemoryContextSwitchTo(source_context);

    errno_t rc;
    if (num_params > 0) {
        planSource->param_types = (Oid*)palloc(num_params * sizeof(Oid));
        rc = memcpy_s(planSource->param_types, num_params * sizeof(Oid), param_types, num_params * sizeof(Oid));
        securec_check(rc, "", "");
    } else {
        planSource->param_types = NULL;
    }
    planSource->num_params = num_params;
    planSource->parserSetup = parserSetup;
    planSource->parserSetupArg = parserSetupArg;
    planSource->cursor_options = cursor_options;
    planSource->fixed_result = fixed_result;
#ifdef PGXC
    planSource->stmt_name = (*stmt_name ? pstrdup(stmt_name) : NULL);
#endif
    planSource->resultDesc = PlanCacheComputeResultDesc(querytree_list);
    planSource->single_exec_node = (ExecNodes*)copyObject(single_exec_node);

    MemoryContextSwitchTo(oldcxt);

    planSource->is_complete = true;
    planSource->is_valid = true;
    planSource->is_read_only = is_read_only;
}

/*
 * SaveCachedPlan: save a cached plan permanently
 *
 * This function moves the cached plan underneath u_sess->cache_mem_cxt (making
 * it live for the life of the backend, unless explicitly dropped), and adds
 * it to the list of cached plans that are checked for invalidation when an
 * sinval event occurs.
 *
 * This is guaranteed not to throw error, except for the caller-error case
 * of trying to save a one-shot plan.  Callers typically depend on that
 * since this is called just before or just after adding a pointer to the
 * CachedPlanSource to some permanent data structure of their own.  Up until
 * this is done, a CachedPlanSource is just transient data that will go away
 * automatically on transaction abort.
 */
void SaveCachedPlan(CachedPlanSource* planSource)
{
    /* Assert caller is doing things in a sane order */
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(planSource->is_complete);
    Assert(!planSource->is_saved);

    /* This seems worth a real test, though */
    if (planSource->is_oneshot)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot save one-shot cached plan")));

    /*
     * In typical use, this function would be called before generating any
     * plans from the CachedPlanSource.  If there is a generic plan, moving it
     * into u_sess->cache_mem_cxt would be pretty risky since it's unclear
     * whether the caller has taken suitable care with making references
     * long-lived.  Best thing to do seems to be to discard the plan.
     */
    ReleaseGenericPlan(planSource);

    /*
     * Reparent the source memory context under u_sess->cache_mem_cxt so that it
     * will live indefinitely.  The query_context follows along since it's
     * already a child of the other one.
     */
    if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
        MemoryContextSetParent(planSource->context, u_sess->cache_mem_cxt);
    }

    /*
     * Add the entry to the global list of cached plans.
     */
    planSource->next_saved = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = planSource;

    planSource->is_saved = true;
}

/*
 * DropCachedPlan: destroy a cached plan.
 *
 * Actually this only destroys the CachedPlanSource: any referenced CachedPlan
 * is released, but not destroyed until its refcount goes to zero.  That
 * handles the situation where DropCachedPlan is called while the plan is
 * still in use.
 */
void DropCachedPlan(CachedPlanSource* planSource)
{
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);

    /* If it's been saved, remove it from the list */
    if (planSource->is_saved) {
        if (u_sess->pcache_cxt.first_saved_plan == planSource)
            u_sess->pcache_cxt.first_saved_plan = planSource->next_saved;
        else {
            CachedPlanSource* psrc = NULL;

            for (psrc = u_sess->pcache_cxt.first_saved_plan; psrc; psrc = psrc->next_saved) {
                if (psrc->next_saved == planSource) {
                    psrc->next_saved = planSource->next_saved;
                    break;
                }
            }
        }
        planSource->is_saved = false;
    }

    DropCachedPlanInternal(planSource);

    /* Mark it no longer valid */
    planSource->magic = 0;

    /*
     * Remove the CachedPlanSource and all subsidiary data (including the
     * query_context if any).  But if it's a one-shot we can't free anything.
     */
    if (!planSource->is_oneshot)
        MemoryContextDelete(planSource->context);
}

/*
 * ReleaseGenericPlan: release a CachedPlanSource's generic plan, if any.
 */
static void ReleaseGenericPlan(CachedPlanSource* planSource)
{
    /* Be paranoid about the possibility that ReleaseCachedPlan fails */
    if (planSource->gplan || planSource->cplan) {
        CachedPlan* plan = NULL;

        /* custom plan and generic plan should not both exists */
        Assert(planSource->gplan == NULL || planSource->cplan == NULL);

        if (planSource->gplan)
            plan = planSource->gplan;
        else
            plan = planSource->cplan;

#ifdef PGXC
        /* Drop this plan on remote nodes */
        if (plan != NULL) {
            ListCell* lc = NULL;

            /* Close any active planned Datanode statements */
            foreach (lc, plan->stmt_list) {
                Node* node = (Node*)lfirst(lc);

                if (IsA(node, PlannedStmt)) {
                    PlannedStmt* ps = (PlannedStmt*)node;
                    DropDataNodeStatements(ps->planTree);
                }
            }
        }
#endif

        Assert(plan->magic == CACHEDPLAN_MAGIC);
        planSource->gplan = NULL;
        planSource->cplan = NULL;
        ReleaseCachedPlan(plan, false);
    }
}

/*
 * RevalidateCachedQuery: ensure validity of analyzed-and-rewritten query tree.
 *
 * What we do here is re-acquire locks and redo parse analysis if necessary.
 * On return, the query_list is valid and we have sufficient locks to begin
 * planning.
 *
 * If any parse analysis activity is required, the caller's memory context is
 * used for that work.
 *
 * The result value is the transient analyzed-and-rewritten query tree if we
 * had to do re-analysis, and NIL otherwise.  (This is returned just to save
 * a tree copying step in a subsequent BuildCachedPlan call.)
 */
List* RevalidateCachedQuery(CachedPlanSource* planSource)
{
    bool snapshot_set = false;
    Node* rawTree = NULL;
    List* tList = NIL; /* transient query-tree list */
    List* qList = NIL; /* permanent query-tree list */
    TupleDesc resultDesc;
    MemoryContext queryTreeContext;
    MemoryContext oldcxt;

    /*
     * For one-shot plans, we do not support revalidation checking; it's
     * assumed the query is parsed, planned, and executed in one transaction,
     * so that no lock re-acquisition is necessary.
     */
    if (planSource->is_oneshot || IsTransactionStmtPlan(planSource)) {
        Assert(planSource->is_valid);
        return NIL;
    }

    /*
     * If the query is currently valid, we should have a saved search_path ---
     * check to see if that matches the current environment.  If not, we want
     * to force replan.
     */
    if (planSource->is_valid) {
        Assert(planSource->search_path != NULL);
        if (!OverrideSearchPathMatchesCurrent(planSource->search_path)) {
            /* Invalidate the querytree and generic plan */
            planSource->is_valid = false;
            if (planSource->gplan)
                planSource->gplan->is_valid = false;
        }
    }

    /*
     * If the query rewrite phase had a possible RLS dependency, we must redo
     * it if either the role setting has changed.
     */
    if (planSource->is_valid && planSource->dependsOnRole && (planSource->rewriteRoleId != GetUserId()))
        planSource->is_valid = false;

    /*
     * If the query is currently valid, acquire locks on the referenced
     * objects; then check again.  We need to do it this way to cover the race
     * condition that an invalidation message arrives before we get the locks.
     */
    if (planSource->is_valid) {
        AcquirePlannerLocks(planSource->query_list, true);

        /*
         * By now, if any invalidation has happened, the inval callback
         * functions will have marked the query invalid.
         */
        if (planSource->is_valid) {
            /* Successfully revalidated and locked the query. */
            return NIL;
        }

        /* Ooops, the race case happened.  Release useless locks. */
        AcquirePlannerLocks(planSource->query_list, false);
    }

    /*
     * Discard the no-longer-useful query tree.  (Note: we don't want to do
     * this any earlier, else we'd not have been able to release locks
     * correctly in the race condition case.)
     */
    planSource->is_valid = false;
    planSource->query_list = NIL;
    planSource->relationOids = NIL;
    planSource->invalItems = NIL;
    planSource->search_path = NULL;

    /*
     * Free the query_context.  We don't really expect MemoryContextDelete to
     * fail, but just in case, make sure the CachedPlanSource is left in a
     * reasonably sane state.  (The generic plan won't get unlinked yet, but
     * that's acceptable.)
     */
    if (planSource->query_context) {
        MemoryContext qcxt = planSource->query_context;

        planSource->query_context = NULL;
        MemoryContextDelete(qcxt);
    }

    /*
     * Now re-do parse analysis and rewrite.  This not incidentally acquires
     * the locks we need to do planning safely.
     */
    Assert(planSource->is_complete);

    /*
     * If a snapshot is already set (the normal case), we can just use that
     * for parsing/planning.  But if it isn't, install one.  Note: no point in
     * checking whether parse analysis requires a snapshot; utility commands
     * don't have invalidatable plans, so we'd not get here for such a
     * command.
     */
    snapshot_set = false;
    if (!ActiveSnapshotSet()) {
        PushActiveSnapshot(GetTransactionSnapshot(GTM_LITE_MODE));
        snapshot_set = true;
    }

    /*
     * Run parse analysis and rule rewriting.  The parser tends to scribble on
     * its input, so we must copy the raw parse tree to prevent corruption of
     * the cache.
     */
    rawTree = (Node*)copyObject(planSource->raw_parse_tree);
    if (planSource->parserSetup != NULL)
        tList = pg_analyze_and_rewrite_params(
            rawTree, planSource->query_string, planSource->parserSetup, planSource->parserSetupArg);
    else
        tList =
            pg_analyze_and_rewrite(rawTree, planSource->query_string, planSource->param_types, planSource->num_params);

    /* Release snapshot if we got one */
    if (snapshot_set)
        PopActiveSnapshot();

    /*
     * Check or update the result tupdesc.  XXX should we use a weaker
     * condition than equalTupleDescs() here?
     *
     * We assume the parameter types didn't change from the first time, so no
     * need to update that.
     */
    resultDesc = PlanCacheComputeResultDesc(tList);
    if (resultDesc == NULL && planSource->resultDesc == NULL) {
        /* OK, doesn't return tuples */
    } else if (resultDesc == NULL || planSource->resultDesc == NULL ||
               !equalTupleDescs(resultDesc, planSource->resultDesc)) {
        /* can we give a better error message? */
        if (planSource->fixed_result)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cached plan must not change result type")));
        oldcxt = MemoryContextSwitchTo(planSource->context);
        if (resultDesc)
            resultDesc = CreateTupleDescCopy(resultDesc);
        if (planSource->resultDesc)
            FreeTupleDesc(planSource->resultDesc);
        planSource->resultDesc = resultDesc;
        MemoryContextSwitchTo(oldcxt);
    }

    if (ENABLE_DN_GPC && planSource->gpc.is_insert  == true) {
        queryTreeContext = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                  "CachedPlanQuery",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE,
                                                  SHARED_CONTEXT);
    } else {
        /*
         * Allocate new query_context and copy the completed querytree into it.
         * It's transient until we complete the copying and dependency extraction.
         */
        queryTreeContext = AllocSetContextCreate(u_sess->top_mem_cxt,
                                                  "CachedPlanQuery",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
    }

    oldcxt = MemoryContextSwitchTo(queryTreeContext);

    qList = (List*)copyObject(tList);

    /*
     * Use the planner machinery to extract dependencies.  Data is saved in
     * query_context.  (We assume that not a lot of extra cruft is created by
     * this call.)
     */
    extract_query_dependencies((Node*)qList,
        &planSource->relationOids,
        &planSource->invalItems,
        &planSource->dependsOnRole,
        &planSource->force_custom_plan);

    /* Update RLS info as well. */
    planSource->rewriteRoleId = GetUserId();

    /*
     * Also save the current search_path in the query_context.  (This should
     * not generate much extra cruft either, since almost certainly the path
     * is already valid.)
     */
    planSource->search_path = GetOverrideSearchPath(queryTreeContext);

    MemoryContextSwitchTo(oldcxt);

    if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
        /* Now reparent the finished query_context and save the links */
        MemoryContextSetParent(queryTreeContext, planSource->context);
    }

    planSource->query_context = queryTreeContext;
    planSource->query_list = qList;

    /* Update ExecNodes for Light CN */
    if (planSource->single_exec_node) {
        ExecNodes* single_exec_node = NULL;

        /* should be only one query */
        if (list_length(qList) == 1) {
            Query* query = (Query*)linitial(qList);
            single_exec_node = lightProxy::checkLightQuery(query);

            /* only deal with single node */
            if (single_exec_node != NULL && list_length(single_exec_node->nodeList) > 1) {
                FreeExecNodes(&single_exec_node);
            }
        }

        oldcxt = MemoryContextSwitchTo(planSource->context);

        /* copy first in case memory error occurs */
        ExecNodes* tmp_en1 = (ExecNodes*)copyObject(single_exec_node);
        ExecNodes* tmp_en2 = planSource->single_exec_node;
        planSource->single_exec_node = tmp_en1;
        FreeExecNodes(&tmp_en2);

        MemoryContextSwitchTo(oldcxt);
    }

    /* clean lightProxyObj if exists */
    if (planSource->lightProxyObj != NULL) {
        lightProxy* lp = (lightProxy*)planSource->lightProxyObj;
        lightProxy::tearDown(lp);
        planSource->lightProxyObj = NULL;
    }

    /* clean opFuisonObj if exists */
    if (planSource->opFusionObj != NULL) {
        OpFusion::tearDown((OpFusion*)planSource->opFusionObj);
        planSource->opFusionObj = NULL;
    }

    /* clean JIT context if exists */
    if (planSource->mot_jit_context) {
        JitExec::DestroyJitContext(planSource->mot_jit_context);
        planSource->mot_jit_context = NULL;
    }

    /*
     * Note: we do not reset generic_cost or total_custom_cost, although we
     * could choose to do so.  If the DDL or statistics change that prompted
     * the invalidation meant a significant change in the cost estimates, it
     * would be better to reset those variables and start fresh; but often it
     * doesn't, and we're better retaining our hard-won knowledge about the
     * relative costs.
     */
    planSource->is_valid = true;

    /* Return transient copy of querytrees for possible use in planning */
    return tList;
}

/*
 * CheckCachedPlan: see if the CachedPlanSource's generic plan is valid.
 *
 * Caller must have already called RevalidateCachedQuery to verify that the
 * querytree is up to date.
 *
 * On a "true" return, we have acquired the locks needed to run the plan.
 * (We must do this for the "true" result to be race-condition-free.)
 */
static bool CheckCachedPlan(CachedPlanSource* planSource)
{
    CachedPlan* plan = planSource->gplan;

    /* Assert that caller checked the querytree */
    Assert(planSource->is_valid);

    /* If there's no generic plan, just say "false" */
    if (plan == NULL) {
        return false;
    }
    /* If stream_operator alreadly change, need build plan again.*/
    if (planSource->gpc.is_share == false
        && planSource->stream_enabled != u_sess->attr.attr_sql.enable_stream_operator) {
        return false;
    }

    Assert(plan->magic == CACHEDPLAN_MAGIC);
    /* Generic plans are never one-shot */
    Assert(!plan->is_oneshot);

    /* If plan isn't valid for current role, we can't use it. */
    if (plan->is_valid && plan->dependsOnRole && plan->planRoleId != GetUserId())
        plan->is_valid = false;

    /*
     * If it appears valid, acquire locks and recheck; this is much the same
     * logic as in RevalidateCachedQuery, but for a plan.
     */
    if (plan->is_valid) {
        /*
         * Plan must have positive refcount because it is referenced by
         * planSource; so no need to fear it disappears under us here.
         */
        Assert(plan->refcount > 0);

        AcquireExecutorLocks(plan->stmt_list, true);

        /*
         * If plan was transient, check to see if TransactionXmin has
         * advanced, and if so invalidate it.
         */
        if (plan->is_valid && TransactionIdIsValid(plan->saved_xmin) &&
            !TransactionIdEquals(plan->saved_xmin, u_sess->utils_cxt.TransactionXmin))
            plan->is_valid = false;

        /*
         * By now, if any invalidation has happened, the inval callback
         * functions will have marked the plan invalid.
         */
        if (plan->is_valid) {
            /* Successfully revalidated and locked the query. */
            return true;
        }

        /* Ooops, the race case happened.  Release useless locks. */
        AcquireExecutorLocks(plan->stmt_list, false);
    }

    /*
     * Plan has been invalidated, so unlink it from the parent and release it.
     */
    ReleaseGenericPlan(planSource);

    return false;
}

/*
 * BuildCachedPlan: construct a new CachedPlan from a CachedPlanSource.
 *
 * qList should be the result value from a previous RevalidateCachedQuery,
 * or it can be set to NIL if we need to re-copy the plansource's query_list.
 *
 * To build a generic, parameter-value-independent plan, pass NULL for
 * boundParams.  To build a custom plan, pass the actual parameter values via
 * boundParams.  For best effect, the PARAM_FLAG_CONST flag should be set on
 * each parameter value; otherwise the planner will treat the value as a
 * hint rather than a hard constant.
 *
 * Planning work is done in the caller's memory context.  The finished plan
 * is in a child memory context, which typically should get reparented
 * (unless this is a one-shot plan, in which case we don't copy the plan).
 */
static CachedPlan* BuildCachedPlan(CachedPlanSource* planSource, List* qList, ParamListInfo boundParams)
{
    CachedPlan* plan = NULL;
    List* plist = NIL;
    bool snapshot_set = false;
    bool spi_pushed = false;
    MemoryContext plan_context;
    MemoryContext oldcxt = CurrentMemoryContext;
    bool save_trigger_shipping_flag = u_sess->attr.attr_sql.enable_trigger_shipping;
    ListCell* lc = NULL;
    bool is_transient = false;
    PLpgSQL_execstate *saved_estate = plpgsql_estate;

    /*
     * NOTE: GetCachedPlan should have called RevalidateCachedQuery first, so
     * we ought to be holding sufficient locks to prevent any invalidation.
     * However, if we're building a custom plan after having built and
     * rejected a generic plan, it's possible to reach here with is_valid
     * false due to an invalidation while making the generic plan.  In theory
     * the invalidation must be a false positive, perhaps a consequence of an
     * sinval reset event or the CLOBBER_CACHE_ALWAYS debug code.
     *
     * We should not call RevalidateCachedQuery here for the above case, for
     * something has already been done when building a generic plan, for example
     * QueryRewriteCTAS has already created the table, and if we call
     * RevalidateCachedQuery again it will report an error 'table already exists'.
     * Also as the comments above, there is also no need to call it.
     */

    /*
     * If we don't already have a copy of the querytree list that can be
     * scribbled on by the planner, make one.  For a one-shot plan, we assume
     * it's okay to scribble on the original query_list.
     */
    if (qList == NIL) {
        if (!planSource->is_oneshot)
            qList = (List*)copyObject(planSource->query_list);
        else
            qList = planSource->query_list;
    }

    /*
     * If a snapshot is already set (the normal case), we can just use that
     * for planning.  But if it isn't, and we need one, install one (unless
     * it is a MM table query).
     */
    snapshot_set = false;
    if (!ActiveSnapshotSet() && !(planSource->storageEngineType == SE_TYPE_MM) &&
        analyze_requires_snapshot(planSource->raw_parse_tree)) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * The planner may try to call SPI-using functions, which causes a problem
     * if we're already inside one.  Rather than expect all SPI-using code to
     * do SPI_push whenever a replan could happen, it seems best to take care
     * of the case here.
     */
    spi_pushed = SPI_push_conditional();

    /*
     * Before going into planner, set default work mode.
     */
    set_default_stream();

    u_sess->pcache_cxt.query_has_params = (planSource->num_params > 0);
    /*
     * Generate the plan and we temporarily close enable_trigger_shipping as
     * we don't support cached shipping plan for trigger.
     */
    PG_TRY();
    {
        /* Temporarily close u_sess->attr.attr_sql.enable_trigger_shipping for cached plan condition. */
        u_sess->attr.attr_sql.enable_trigger_shipping = false;
        plist = pg_plan_queries(qList, planSource->cursor_options, boundParams);
    }
    PG_CATCH();
    {
        /* Reset the flag after cached plan have been got. */
        u_sess->attr.attr_sql.enable_trigger_shipping = save_trigger_shipping_flag;
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->attr.attr_sql.enable_trigger_shipping = save_trigger_shipping_flag;
    u_sess->pcache_cxt.query_has_params = false;

    /* Clean up SPI state */
    SPI_pop_conditional(spi_pushed);

    /* Release snapshot if we got one */
    if (snapshot_set)
        PopActiveSnapshot();

    /*
     * Normally we make a dedicated memory context for the CachedPlan and its
     * subsidiary data.  (It's probably not going to be large, but just in
     * case, use the default maxsize parameter.  It's transient for the
     * moment.)  But for a one-shot plan, we just leave it in the caller's
     * memory context.
     */
    if (!planSource->is_oneshot) {
        if (ENABLE_DN_GPC && planSource->gpc.is_insert == true) {
            plan_context = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                 "CachedPlan",
                                                 ALLOCSET_SMALL_MINSIZE,
                                                 ALLOCSET_SMALL_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE,
                                                 SHARED_CONTEXT);
        } else {
            plan_context = AllocSetContextCreate(u_sess->cache_mem_cxt,
                                                 "CachedPlan",
                                                 ALLOCSET_SMALL_MINSIZE,
                                                 ALLOCSET_SMALL_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);
        }

        /*
         * Copy plan into the new context.
         */
        MemoryContextSwitchTo(plan_context);

        plist = (List*)copyObject(plist);
    } else
        plan_context = CurrentMemoryContext;

#ifdef PGXC
    /*
     * If this planSource belongs to a named prepared statement, store the stmt
     * name for the Datanode queries.
     */
    bool is_named_prepare = (IS_PGXC_COORDINATOR && !IsConnFromCoord() && planSource->stmt_name);
    if (is_named_prepare) {
        int n;

        /*
         * Scan the plans and set the statement field for all found RemoteQuery
         * nodes so they use Datanode statements
         */
        n = 0;
        foreach (lc, plist) {
            Node* st = NULL;
            PlannedStmt* ps = NULL;
            st = (Node*)lfirst(lc);

            if (IsA(st, PlannedStmt)) {
                ps = (PlannedStmt*)st;
                n = SetRemoteStatementName(
                    ps->planTree, planSource->stmt_name, planSource->num_params, planSource->param_types, n);
            }
        }
    }
#endif

    /*
     * Create and fill the CachedPlan struct within the new context.
     */
    plan = (CachedPlan*)palloc(sizeof(CachedPlan));
    plan->magic = CACHEDPLAN_MAGIC;
    plan->stmt_list = plist;

    /*
     * CachedPlan is dependent on role either if RLS affected the rewrite
     * phase or if a role dependency was injected during planning.  And it's
     * transient if any plan is marked so.
     */
    plan->planRoleId = GetUserId();
    plan->dependsOnRole = planSource->dependsOnRole;

    foreach (lc, plist) {
        PlannedStmt* plannedStmt = (PlannedStmt*)lfirst(lc);

        if (!IsA(plannedStmt, PlannedStmt))
            continue; /* Ignore utility statements */

        if (plannedStmt->transientPlan)
            is_transient = true;

        if (plannedStmt->dependsOnRole)
            plan->dependsOnRole = true;
    }

    if (is_transient) {
        Assert(TransactionIdIsNormal(u_sess->utils_cxt.TransactionXmin));
        plan->saved_xmin = u_sess->utils_cxt.TransactionXmin;
    } else
        plan->saved_xmin = InvalidTransactionId;
    plan->refcount = 0;
    plan->context = plan_context;
    plan->is_oneshot = planSource->is_oneshot;
    plan->is_saved = false;
    plan->is_valid = true;

    /* assign generation number to new plan */
    plan->generation = ++(planSource->generation);

    MemoryContextSwitchTo(oldcxt);

    /* Set plan real u_sess->attr.attr_sql.enable_stream_operator. */
    planSource->stream_enabled = u_sess->attr.attr_sql.enable_stream_operator;
    plan->is_share = planSource->gpc.is_insert;
    plpgsql_estate = saved_estate;

    return plan;
}

/*
 * ChooseCustomPlan: choose whether to use custom or generic plan
 *
 * This defines the policy followed by GetCachedPlan.
 */
static bool ChooseCustomPlan(CachedPlanSource* planSource, ParamListInfo boundParams)
{
    double avg_custom_cost;
    bool is_global_plan = (ENABLE_DN_GPC &&
                           (planSource->gpc.is_insert == true || planSource->gpc.is_share == true));
    if (is_global_plan) {
        return false;
    }

    /* Don't choose custom plan if using pbe optimization */
    if (u_sess->attr.attr_sql.enable_pbe_optimization && IsMMEngineUsed())
        return false;

    /* One-shot plans will always be considered custom */
    if (planSource->is_oneshot)
        return true;

    /* Otherwise, never any point in a custom plan if there's no parameters */
    if (boundParams == NULL)
        return false;

    /* ... nor for transaction control statements */
    if (IsTransactionStmtPlan(planSource)) {
        return false;
    }

    /* See if caller wants to force the decision */
    if (planSource->cursor_options & CURSOR_OPT_GENERIC_PLAN)
        return false;
    if (planSource->cursor_options & CURSOR_OPT_CUSTOM_PLAN)
        return true;

    /* If we contains cstore table, always custom except fqs on CN */
    if (planSource->force_custom_plan) {
        if (IS_PGXC_DATANODE)
            return true;
        else if (IS_PGXC_COORDINATOR && !planSource->gplan_is_fqs)
            return true;
    }

    /* Only use generic plan when trigger shipping is off */
    if (!u_sess->attr.attr_sql.enable_trigger_shipping)
        return false;

    /* Don't choose custom plan if using pbe optimization */
    if (u_sess->attr.attr_sql.enable_pbe_optimization && planSource->gplan_is_fqs)
        return false;

    /* Let settings force the decision */
    if (u_sess->attr.attr_sql.g_planCacheMode != PLAN_CACHE_MODE_AUTO) {
        if (u_sess->attr.attr_sql.g_planCacheMode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN) {
            return false;
        }

        if (u_sess->attr.attr_sql.g_planCacheMode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN) {
            return true;
        }
    }

    /* Generate custom plans until we have done at least 5 (arbitrary) */
    if (planSource->num_custom_plans < 5)
        return true;

    avg_custom_cost = planSource->total_custom_cost / planSource->num_custom_plans;

    /*
     * Prefer generic plan if it's less than 10% more expensive than average
     * custom plan.  This threshold is a bit arbitrary; it'd be better if we
     * had some means of comparing planning time to the estimated runtime cost
     * differential.
     *
     * Note that if generic_cost is -1 (indicating we've not yet determined
     * the generic plan cost), we'll always prefer generic at this point.
     */
    if (planSource->generic_cost < avg_custom_cost * GROWTH_FACTOR) {
        return false;
    }
    return true;
}

/*
 * CachedPlanCost: calculate estimated cost of a plan
 */
static double CachedPlanCost(CachedPlan* plan)
{
    double result = 0;
    ListCell* lc = NULL;

    foreach (lc, plan->stmt_list) {
        PlannedStmt* plannedStmt = (PlannedStmt*)lfirst(lc);

        if (!IsA(plannedStmt, PlannedStmt))
            continue; /* Ignore utility statements */

        result += plannedStmt->planTree->total_cost;
    }

    return result;
}

/*
 * GetCachedPlan: get a cached plan from a CachedPlanSource.
 *
 * This function hides the logic that decides whether to use a generic
 * plan or a custom plan for the given parameters: the caller does not know
 * which it will get.
 *
 * On return, the plan is valid and we have sufficient locks to begin
 * execution.
 *
 * On return, the refcount of the plan has been incremented; a later
 * ReleaseCachedPlan() call is expected.  The refcount has been reported
 * to the CurrentResourceOwner if useResOwner is true (note that that must
 * only be true if it's a "saved" CachedPlanSource).
 *
 * Note: if any replanning activity is required, the caller's memory context
 * is used for that work.
 */
CachedPlan* GetCachedPlan(CachedPlanSource* planSource, ParamListInfo boundParams, bool useResOwner)
{
    CachedPlan* plan = NULL;
    List* qList = NIL;
    bool customplan = false;

    /* Assert caller is doing things in a sane order */
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(planSource->is_complete);
    /* This seems worth a real test, though */
    if (useResOwner && !planSource->is_saved)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot apply ResourceOwner to non-saved cached plan")));

    /* Make sure the querytree list is valid and we have parse-time locks */
    qList = RevalidateCachedQuery(planSource);

    /* Decide whether to use a custom plan */
    customplan = ChooseCustomPlan(planSource, boundParams);

    if (!customplan) {
        if (CheckCachedPlan(planSource)) {
            /* We want a generic plan, and we already have a valid one */
            plan = planSource->gplan;
            Assert(plan->magic == CACHEDPLAN_MAGIC);

            /* Update soft parse counter for Unique SQL */
            UniqueSQLStatCountSoftParse(1);
        } else {
            /* Whenever plan is rebuild, we need to drop the old one */
            ReleaseGenericPlan(planSource);

            /* Build a new generic plan */
            plan = BuildCachedPlan(planSource, qList, NULL);
            /* Link the new generic plan into the planSource */
            planSource->gplan = plan;
            plan->refcount++;
            /* Immediately reparent into appropriate context */
            if (planSource->is_saved) {
                if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
                    /* saved plans all live under CacheMemoryContext */
                    MemoryContextSetParent(plan->context, u_sess->cache_mem_cxt);
                }
                plan->is_saved = true;
            } else {
                if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
                    /* otherwise, it should be a sibling of the planSource */
                    MemoryContextSetParent(plan->context,
                                    MemoryContextGetParent(planSource->context));
                }
            }
            /* Update generic_cost whenever we make a new generic plan */
            planSource->generic_cost = CachedPlanCost(plan);

            /*
             * Judge if gplan is single-node fqs, if so, we can use it when
             * enable_pbe_optimization is on
             */
            if (IS_PGXC_COORDINATOR && u_sess->attr.attr_sql.enable_pbe_optimization) {
                List* stmtList = plan->stmt_list;
                bool plan_is_fqs = false;
                if (list_length(stmtList) == 1) {
                    Node* pstmt = (Node*)linitial(stmtList);
                    if (IsA(pstmt, PlannedStmt)) {
                        Plan* topPlan = ((PlannedStmt*)pstmt)->planTree;
                        if (IsA(topPlan, RemoteQuery)) {
                            RemoteQuery* rq = (RemoteQuery*)topPlan;
                            if (rq->exec_nodes &&
                                ((rq->exec_nodes->nodeList == NULL && rq->exec_nodes->en_expr != NULL) ||
                                    list_length(rq->exec_nodes->nodeList) == 1))
                                plan_is_fqs = true;
                        }
                    }
                }
                planSource->gplan_is_fqs = plan_is_fqs;
                ereport(
                    DEBUG2, (errmodule(MOD_OPT), errmsg("Customer plan is used for \"%s\"", planSource->query_string)));
            }

            /*
             * If, based on the now-known value of generic_cost, we'd not have
             * chosen to use a generic plan, then forget it and make a custom
             * plan.  This is a bit of a wart but is necessary to avoid a
             * glitch in behavior when the custom plans are consistently big
             * winners; at some point we'll experiment with a generic plan and
             * find it's a loser, but we don't want to actually execute that
             * plan.
             */
            customplan = ChooseCustomPlan(planSource, boundParams);

            /*
             * If we choose to plan again, we need to re-copy the query_list,
             * since the planner probably scribbled on it.  We can force
             * BuildCachedPlan to do that by passing NIL.
             */
            qList = NIL;
        }
    }

    /* In function BuildCachedPlan, we deparse query to obtain sql_statement,
     * If we have generic plan, Sql_statement will have format parameter.
     * We can not send this statement to DN directly. So we should replace format
     * parameter with actual values. But, it will block pbe performance and
     * have side effects, such as unexpected query rewrite. So,we suggest that
     * u_sess->attr.attr_common.max_datanode_for_plan is 0 when testing performance.
     */
    if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && !customplan && boundParams != NULL &&
        boundParams->params_need_process == true && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* we just replace params with virtual values and do not use plan. */
        (void)BuildCachedPlan(planSource, qList, boundParams);
    }

    if (customplan) {
        /* Whenever plan is rebuild, we need to drop the old one */
        ReleaseGenericPlan(planSource);

        /* Build a custom plan */
        plan = BuildCachedPlan(planSource, qList, boundParams);
        /* Link the new custome plan into the planSource */
        planSource->cplan = plan;
        plan->refcount++;

        /* Accumulate total costs of custom plans, but 'ware overflow */
        if (planSource->num_custom_plans < INT_MAX) {
            planSource->total_custom_cost += CachedPlanCost(plan);
            planSource->num_custom_plans++;
        }
    }

    /*
     * Validate the planSource again
     * The planSource may be invalidated by ResetPlanCache when handling invalid
     * messages in BuildCachedPlan.
     */
    planSource->is_valid = true;

    /* Flag the plan as in use by caller */
    if (useResOwner)
        ResourceOwnerEnlargePlanCacheRefs(t_thrd.utils_cxt.CurrentResourceOwner);
    plan->refcount++;
    if (useResOwner)
        ResourceOwnerRememberPlanCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, plan);

    /*
     * Saved plans should be under u_sess->cache_mem_cxt so they will not go away
     * until their reference count goes to zero.  In the generic-plan cases we
     * already took care of that, but for a custom plan, do it as soon as we
     * have created a reference-counted link.
     */
    if (customplan && planSource->is_saved) {
        if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
            MemoryContextSetParent(plan->context, u_sess->cache_mem_cxt);
        }
        plan->is_saved = true;
    }

    /* set plan storageEngineType */
    plan->storageEngineType = planSource->storageEngineType;
    plan->mot_jit_context = planSource->mot_jit_context;

    return plan;
}

/*
 * Find and release all Datanode statements referenced by the plan node and subnodes
 */
#ifdef PGXC
static void DropDataNodeStatements(Plan* planNode)
{
    if (IsA(planNode, RemoteQuery)) {
        RemoteQuery* step = (RemoteQuery*)planNode;

        if (step->statement)
            DropDatanodeStatement(step->statement);
    } else if (IsA(planNode, ModifyTable)) {
        ModifyTable* mt_plan = (ModifyTable*)planNode;
        /* For ModifyTable plan recurse into each of the plans underneath */
        ListCell* l = NULL;
        foreach (l, mt_plan->plans) {
            Plan* plan = (Plan*)lfirst(l);
            DropDataNodeStatements(plan);
        }
    }

    if (innerPlan(planNode))
        DropDataNodeStatements(innerPlan(planNode));

    if (outerPlan(planNode))
        DropDataNodeStatements(outerPlan(planNode));
}
#endif

/*
 * ReleaseCachedPlan: release active use of a cached plan.
 *
 * This decrements the reference count, and frees the plan if the count
 * has thereby gone to zero.  If useResOwner is true, it is assumed that
 * the reference count is managed by the CurrentResourceOwner.
 *
 * Note: useResOwner = false is used for releasing references that are in
 * persistent data structures, such as the parent CachedPlanSource or a
 * Portal.  Transient references should be protected by a resource owner.
 */
void ReleaseCachedPlan(CachedPlan* plan, bool useResOwner)
{
    Assert(plan->magic == CACHEDPLAN_MAGIC);
    if (useResOwner) {
        Assert(plan->is_saved);
        ResourceOwnerForgetPlanCacheRef(t_thrd.utils_cxt.CurrentResourceOwner, plan);
    }
    Assert(plan->refcount > 0);
    plan->refcount--;
    if (plan->refcount == 0) {
        /* Mark it no longer valid */
        plan->magic = 0;

        /* One-shot plans do not own their context, so we can't free them */
        if (!plan->is_oneshot) {
            Assert (plan->is_share == false);
            Assert (plan->context->parent != g_instance.cache_cxt.global_cache_mem);
            MemoryContextDelete(plan->context);
        }
    }
}

/*
 * CachedPlanSetParentContext: move a CachedPlanSource to a new memory context
 *
 * This can only be applied to unsaved plans; once saved, a plan always
 * lives underneath u_sess->cache_mem_cxt.
 */
void CachedPlanSetParentContext(CachedPlanSource* planSource, MemoryContext newContext)
{
    /* Assert caller is doing things in a sane order */
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(planSource->is_complete);

    /* These seem worth real tests, though */
    if (planSource->is_saved)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move a saved cached plan to another context")));
    if (planSource->is_oneshot)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot move a one-shot cached plan to another context")));

    if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
        /* OK, let the caller keep the plan where he wishes */
        MemoryContextSetParent(planSource->context, newContext);
    }

    /*
     * The query_context needs no special handling, since it's a child of
     * planSource->context.  But if there's a generic plan, it should be
     * maintained as a sibling of planSource->context.
     */
    if (planSource->gplan) {
        Assert(planSource->gplan->magic == CACHEDPLAN_MAGIC);
        if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
            MemoryContextSetParent(planSource->gplan->context, newContext);
        }
    }

    if (planSource->cplan) {
        Assert(planSource->cplan->magic == CACHEDPLAN_MAGIC);
        if (!ENABLE_DN_GPC || planSource->gpc.is_insert == false) {
            MemoryContextSetParent(planSource->cplan->context, newContext);
        }
    }
}

/*
 * CopyCachedPlan: make a copy of a CachedPlanSource
 *
 * This is a convenience routine that does the equivalent of
 * CreateCachedPlan + CompleteCachedPlan, using the data stored in the
 * input CachedPlanSource.  The result is therefore "unsaved" (regardless
 * of the state of the source), and we don't copy any generic plan either.
 * The result will be currently valid, or not, the same as the source.
 */
CachedPlanSource* CopyCachedPlan(CachedPlanSource* planSource, bool isShare)
{
    CachedPlanSource* newSource = NULL;
    MemoryContext sourceContext;
    MemoryContext queryTreeContext;
    MemoryContext oldcxt;

    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(planSource->is_complete);

    /*
     * One-shot plans can't be copied, because we haven't taken care that
     * parsing/planning didn't scribble on the raw parse tree or querytrees.
     */
    if (planSource->is_oneshot)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot copy a one-shot cached plan")));

    if (ENABLE_DN_GPC && isShare == true) {
        sourceContext = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                               "CachedPlanSource",
                                               ALLOCSET_SMALL_MINSIZE,
                                               ALLOCSET_SMALL_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE,
                                               SHARED_CONTEXT);
    } else {
        sourceContext = AllocSetContextCreate(u_sess->cache_mem_cxt,
                                               "CachedPlanSource",
                                               ALLOCSET_SMALL_MINSIZE,
                                               ALLOCSET_SMALL_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);
    }

    oldcxt = MemoryContextSwitchTo(sourceContext);

    newSource = (CachedPlanSource*)palloc0(sizeof(CachedPlanSource));
    newSource->magic = CACHEDPLANSOURCE_MAGIC;
    newSource->raw_parse_tree = (Node*)copyObject(planSource->raw_parse_tree);
    newSource->query_string = pstrdup(planSource->query_string);
    newSource->commandTag = planSource->commandTag;
    if (planSource->num_params > 0) {
        newSource->param_types = (Oid*)palloc(planSource->num_params * sizeof(Oid));
        errno_t rc = memcpy_s(newSource->param_types,
            planSource->num_params * sizeof(Oid),
            planSource->param_types,
            planSource->num_params * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    } else {
        newSource->param_types = NULL;
    }
    newSource->num_params = planSource->num_params;
    newSource->parserSetup = planSource->parserSetup;
    newSource->parserSetupArg = planSource->parserSetupArg;
    newSource->cursor_options = planSource->cursor_options;
    newSource->rewriteRoleId = planSource->rewriteRoleId;
    newSource->dependsOnRole = planSource->dependsOnRole;
    newSource->fixed_result = planSource->fixed_result;
    if (planSource->resultDesc) {
        newSource->resultDesc = CreateTupleDescCopy(planSource->resultDesc);
    } else {
        newSource->resultDesc = NULL;
    }
    newSource->context = sourceContext;

    if (ENABLE_DN_GPC && isShare == true) {
        queryTreeContext = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                  "CachedPlanQuery",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE,
                                                  SHARED_CONTEXT);
    } else {
        queryTreeContext = AllocSetContextCreate(sourceContext,
                                                  "CachedPlanQuery",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
    }
    MemoryContextSwitchTo(queryTreeContext);
    newSource->query_list = (List*)copyObject(planSource->query_list);
    newSource->relationOids = (List*)copyObject(planSource->relationOids);
    newSource->invalItems = (List*)copyObject(planSource->invalItems);
    if (planSource->search_path) {
        newSource->search_path = CopyOverrideSearchPath(planSource->search_path);
    }
    newSource->query_context = queryTreeContext;
    newSource->gplan = NULL;

    newSource->is_oneshot = false;
    newSource->is_complete = true;
    newSource->is_saved = false;
    newSource->is_valid = planSource->is_valid;
    newSource->generation = planSource->generation;
    newSource->next_saved = NULL;

    /* We may as well copy any acquired cost knowledge */
    newSource->generic_cost = planSource->generic_cost;
    newSource->total_custom_cost = planSource->total_custom_cost;
    newSource->num_custom_plans = planSource->num_custom_plans;

#ifdef PGXC
    newSource->stream_enabled = planSource->stream_enabled;
    planSource->cplan = NULL;
    planSource->single_exec_node = NULL;
    planSource->is_read_only = false;
    planSource->lightProxyObj = NULL;
#endif

    MemoryContextSwitchTo(oldcxt);

    return newSource;
}

/*
 * CachedPlanIsValid: test whether the rewritten querytree within a
 * CachedPlanSource is currently valid (that is, not marked as being in need
 * of revalidation).
 *
 * This result is only trustworthy (ie, free from race conditions) if
 * the caller has acquired locks on all the relations used in the plan.
 */
bool CachedPlanIsValid(CachedPlanSource* planSource)
{
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    return planSource->is_valid;
}

/*
 * CachedPlanGetTargetList: return tlist, if any, describing plan's output
 *
 * The result is guaranteed up-to-date.  However, it is local storage
 * within the cached plan, and may disappear next time the plan is updated.
 */
List* CachedPlanGetTargetList(CachedPlanSource* planSource)
{
    Node* pstmt = NULL;

    /* Assert caller is doing things in a sane order */
    Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(planSource->is_complete);

    /*
     * No work needed if statement doesn't return tuples (we assume this
     * feature cannot be changed by an invalidation)
     */
    if (planSource->resultDesc == NULL)
        return NIL;

    /* Make sure the querytree list is valid and we have parse-time locks */
    (void)RevalidateCachedQuery(planSource);

    /* Get the primary statement and find out what it returns */
    pstmt = PortalListGetPrimaryStmt(planSource->query_list);

    return FetchStatementTargetList(pstmt);
}

/*
 * AcquireExecutorLocks: acquire locks needed for execution of a cached plan;
 * or release them if acquire is false.
 */
static void AcquireExecutorLocks(List* stmtList, bool acquire)
{
    ListCell* lc1 = NULL;

    foreach (lc1, stmtList) {
        PlannedStmt* plannedStmt = (PlannedStmt*)lfirst(lc1);
        int rt_index;
        ListCell* lc2 = NULL;

        Assert(!IsA(plannedStmt, Query));
        if (!IsA(plannedStmt, PlannedStmt)) {
            /*
             * Ignore utility statements, except those (such as EXPLAIN) that
             * contain a parsed-but-not-planned query.  Note: it's okay to use
             * ScanQueryForLocks, even though the query hasn't been through
             * rule rewriting, because rewriting doesn't change the query
             * representation.
             */
            Query* query = UtilityContainsQuery((Node*)plannedStmt);

            if (query != NULL)
                ScanQueryForLocks(query, acquire);
            continue;
        }

        rt_index = 0;
        foreach (lc2, plannedStmt->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc2);
            LOCKMODE lockmode;
            PlanRowMark* rc = NULL;

            rt_index++;

            if (rte->rtekind != RTE_RELATION)
                continue;

            /*
             * Acquire the appropriate type of lock on each relation OID. Note
             * that we don't actually try to open the rel, and hence will not
             * fail if it's been dropped entirely --- we'll just transiently
             * acquire a non-conflicting lock.
             */
            if (list_member_int(plannedStmt->resultRelations, rt_index))
                lockmode = RowExclusiveLock;
            else if ((rc = get_plan_rowmark(plannedStmt->rowMarks, rt_index)) != NULL &&
                     RowMarkRequiresRowShareLock(rc->markType))
                lockmode = RowShareLock;
            else
                lockmode = AccessShareLock;

            if (acquire)
                LockRelationOid(rte->relid, lockmode);
            else
                UnlockRelationOid(rte->relid, lockmode);
        }
    }
}

/*
 * AcquirePlannerLocks: acquire locks needed for planning of a querytree list;
 * or release them if acquire is false.
 *
 * Note that we don't actually try to open the relations, and hence will not
 * fail if one has been dropped entirely --- we'll just transiently acquire
 * a non-conflicting lock.
 */
static void AcquirePlannerLocks(List* stmtList, bool acquire)
{
    ListCell* lc = NULL;

    foreach (lc, stmtList) {
        Query* query = (Query*)lfirst(lc);

        Assert(IsA(query, Query));

        if (query->commandType == CMD_UTILITY) {
            /* Ignore utility statements, unless they contain a Query */
            query = UtilityContainsQuery(query->utilityStmt);
            if (query != NULL)
                ScanQueryForLocks(query, acquire);
            continue;
        }

        ScanQueryForLocks(query, acquire);
    }
}

/*
 * ScanQueryForLocks: recursively scan one Query for AcquirePlannerLocks.
 */
static void ScanQueryForLocks(Query* parseTree, bool acquire)
{
    ListCell* lc = NULL;
    int rt_index;

    /* Shouldn't get called on utility commands */
    Assert(parseTree->commandType != CMD_UTILITY);

    /*
     * First, process RTEs of the current query level.
     */
    rt_index = 0;
    foreach (lc, parseTree->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        LOCKMODE lockmode;

        rt_index++;
        switch (rte->rtekind) {
            case RTE_RELATION:
                /* Acquire or release the appropriate type of lock */
                if (rt_index == parseTree->resultRelation)
                    lockmode = RowExclusiveLock;
                else if (get_parse_rowmark(parseTree, rt_index) != NULL)
                    lockmode = RowShareLock;
                else
                    lockmode = AccessShareLock;
                if (acquire)
                    LockRelationOid(rte->relid, lockmode);
                else
                    UnlockRelationOid(rte->relid, lockmode);
                break;

            case RTE_SUBQUERY:
                /* Recurse into subquery-in-FROM */
                ScanQueryForLocks(rte->subquery, acquire);
                break;

            default:
                /* ignore other types of RTEs */
                break;
        }
    }

    /* Recurse into subquery-in-WITH */
    foreach (lc, parseTree->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

        ScanQueryForLocks((Query*)cte->ctequery, acquire);
    }

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (parseTree->hasSubLinks) {
        query_tree_walker(parseTree, (bool (*)())ScanQueryWalker, (void*)&acquire, QTW_IGNORE_RC_SUBQUERIES);
    }
}

/*
 * Walker to find sublink subqueries for ScanQueryForLocks
 */
static bool ScanQueryWalker(Node* node, bool* acquire)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, SubLink)) {
        SubLink* sub = (SubLink*)node;

        /* Do what we came for */
        ScanQueryForLocks((Query*)sub->subselect, *acquire);
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because ScanQueryForLocks already
     * processed subselects of subselects for us.
     */
    return expression_tree_walker(node, (bool (*)())ScanQueryWalker, (void*)acquire);
}

/*
 * PlanCacheComputeResultDesc: given a list of analyzed-and-rewritten Queries,
 * determine the result tupledesc it will produce.  Returns NULL if the
 * execution will not return tuples.
 *
 * Note: the result is created or copied into current memory context.
 */
static TupleDesc PlanCacheComputeResultDesc(List* stmtList)
{
    Query* query = NULL;

    switch (ChoosePortalStrategy(stmtList)) {
        case PORTAL_ONE_SELECT:
        case PORTAL_ONE_MOD_WITH:
            query = (Query*)linitial(stmtList);
            Assert(IsA(query, Query));
            return ExecCleanTypeFromTL(query->targetList, false);

        case PORTAL_ONE_RETURNING:
            query = (Query*)PortalListGetPrimaryStmt(stmtList);
            Assert(IsA(query, Query));
            Assert(query->returningList);
            return ExecCleanTypeFromTL(query->returningList, false);

        case PORTAL_UTIL_SELECT:
            query = (Query*)linitial(stmtList);
            Assert(IsA(query, Query));
            Assert(query->utilityStmt);
            return UtilityTupleDescriptor(query->utilityStmt);

        case PORTAL_MULTI_QUERY:
            /* will not return tuples */
            break;
        default:
            break;
    }
    return NULL;
}

/*
 * CheckRelDependency: go through the plansource's dependency list to see if it
 * depends on the given relation by ID.
 */
void CheckRelDependency(CachedPlanSource *planSource, Oid relid)
{
    /*
     * Check the dependency list for the rewritten querytree.
     */
    if ((relid == InvalidOid) ? planSource->relationOids != NIL :
        list_member_oid(planSource->relationOids, relid)) {
        if (planSource->gpc.is_share == true) {
            planSource->gpc.in_revalidate = true;
            return;
        } else {
            /* Invalidate the querytree and generic plan */
            planSource->is_valid = false;
            if (planSource->gplan) {
                planSource->gplan->is_valid = false;
            }
        }
    }

    /*
     * The generic plan, if any, could have more dependencies than the
     * querytree does, so we have to check it too.
     */
    if (planSource->gplan && planSource->gplan->is_valid) {
        ListCell   *lc = NULL;

        foreach(lc, planSource->gplan->stmt_list) {
            PlannedStmt *plannedStmt = (PlannedStmt *) lfirst(lc);

            Assert(!IsA(plannedStmt, Query));
            if (!IsA(plannedStmt, PlannedStmt)) {
                continue;   /* Ignore utility statements */
            }
            if ((relid == InvalidOid) ? plannedStmt->relationOids != NIL :
                list_member_oid(plannedStmt->relationOids, relid)) {
                if (planSource->gpc.is_share == true) {
                    planSource->gpc.in_revalidate = true;
                    return;
                } else {
                    /* Invalidate the generic plan only */
                    planSource->gplan->is_valid = false;
                }
                break;      /* out of stmt_list scan */
            }
        }
    }
}

/*
 * CheckInvalItemDependency: go through the planSource's dependency list to see if it
 * depends on the given object by ID.
 */
void CheckInvalItemDependency(CachedPlanSource *planSource, int cacheid, uint32 hashvalue)
{
    ListCell *lc = NULL;

    /*
     * Check the dependency list for the rewritten querytree.
     */
    foreach(lc, planSource->invalItems) {
        PlanInvalItem *item = (PlanInvalItem *) lfirst(lc);

        if (item->cacheId != cacheid) {
            continue;
        }
        if (hashvalue == 0 ||
            item->hashValue == hashvalue) {
            if (planSource->gpc.is_share == true) {
                planSource->gpc.in_revalidate = true;
                return;
            } else {
                /* Invalidate the querytree and generic plan */
                planSource->is_valid = false;
                if (planSource->gplan) {
                    planSource->gplan->is_valid = false;
                }
            }
            break;
        }
    }

    /*
     * The generic plan, if any, could have more dependencies than the
     * querytree does, so we have to check it too.
     */
    if (planSource->gplan && planSource->gplan->is_valid) {
        foreach(lc, planSource->gplan->stmt_list) {
            PlannedStmt *plannedstmt = (PlannedStmt *) lfirst(lc);
            ListCell *lc3 = NULL;

            Assert(!IsA(plannedstmt, Query));
            if (!IsA(plannedstmt, PlannedStmt)) {
                continue;   /* Ignore utility statements */
            }
            foreach(lc3, plannedstmt->invalItems) {
                PlanInvalItem *item = (PlanInvalItem *) lfirst(lc3);

                if (item->cacheId != cacheid) {
                    continue;
                }
                if (hashvalue == 0 || item->hashValue == hashvalue) {
                    if (planSource->gpc.is_share == true) {
                        planSource->gpc.in_revalidate = true;
                        return;
                    } else {
                        /* Invalidate the generic plan only */
                        planSource->gplan->is_valid = false;
                    }
                    break; /* out of invalItems scan */
                }
            }
            if (!planSource->gplan->is_valid) {
                break; /* out of stmt_list scan */
            }
        }
    }
}

/*
 * PlanCacheRelCallback
 *      Relcache inval callback function
 *
 * Invalidate all plans mentioning the given rel, or all plans mentioning
 * any rel at all if relid == InvalidOid.
 */
void PlanCacheRelCallback(Datum arg, Oid relid)
{
    CachedPlanSource* planSource = NULL;

    for (planSource = u_sess->pcache_cxt.first_saved_plan; planSource; planSource = planSource->next_saved) {
        Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if (!planSource->is_valid) {
            continue;
        }
        /* Never invalidate transaction control commands */
        if (IsTransactionStmtPlan(planSource)) {
            continue;
        }

        CheckRelDependency(planSource, relid);
    }
}

/*
 * PlanCacheFuncCallback
 *      Syscache inval callback function for PROCOID cache
 *
 * Invalidate all plans mentioning the object with the specified hash value,
 * or all plans mentioning any member of this cache if hashvalue == 0.
 *
 * Note that the coding would support use for multiple caches, but right
 * now only user-defined functions are tracked this way.
 */
void PlanCacheFuncCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    CachedPlanSource* planSource = NULL;

    for (planSource = u_sess->pcache_cxt.first_saved_plan; planSource; planSource = planSource->next_saved) {
        Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if (!planSource->is_valid) {
            continue;
        }
        /* Never invalidate transaction control commands */
        if (IsTransactionStmtPlan(planSource)) {
            continue;
        }
        CheckInvalItemDependency(planSource, cacheid, hashvalue);
    }
}

/*
 * PlanCacheSysCallback
 *      Syscache inval callback function for other caches
 *
 * Just invalidate everything...
 */
void PlanCacheSysCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    ResetPlanCache();
}

/*
 * ResetPlanCache: invalidate a cached plans.
 */
void ResetPlanCache(CachedPlanSource *planSource)
{
    ListCell *lc = NULL;

    /*
     * In general there is no point in invalidating utility statements
     * since they have no plans anyway.  So invalidate it only if it
     * contains at least one non-utility statement, or contains a utility
     * statement that contains a pre-analyzed query (which could have
     * dependencies.)
     */
    foreach(lc, planSource->query_list) {
        Query *query = (Query *) lfirst(lc);

        Assert(IsA(query, Query));
        if (query->commandType != CMD_UTILITY ||  UtilityContainsQuery(query->utilityStmt)) {
            /* non-utility statement, so invalidate */
            if (planSource->gpc.is_share == true) {
                planSource->gpc.in_revalidate = true;
                return;
            } else {
                planSource->is_valid = false;
                if (planSource->gplan) {
                    planSource->gplan->is_valid = false;
                }
            }
            /* no need to look further */
            break;
        }
    }
}

/*
 * ResetPlanCache: invalidate all cached plans.
 */
void ResetPlanCache(void)
{
    CachedPlanSource* planSource = NULL;

    for (planSource = u_sess->pcache_cxt.first_saved_plan; planSource; planSource = planSource->next_saved) {
        Assert(planSource->magic == CACHEDPLANSOURCE_MAGIC);

        /* No work if it's already invalidated */
        if (!planSource->is_valid) {
            continue;
        }
        /*
         * We *must not* mark transaction control statements as invalid,
         * particularly not ROLLBACK, because they may need to be executed in
         * aborted transactions when we can't revalidate them (cf bug #5269).
         */
        if (IsTransactionStmtPlan(planSource)) {
            continue;
        }

        ResetPlanCache(planSource);
    }
}

/*
 * Drop lightProxyObj/gplan/cplan inside CachedPlanSource,
 * and drop prepared statements on DN.
 */
void DropCachedPlanInternal(CachedPlanSource* planSource)
{
    /* MOT: clean any JIT context */
    if (planSource->mot_jit_context) {
        JitExec::DestroyJitContext(planSource->mot_jit_context);
        planSource->mot_jit_context = NULL;
    }

    if (planSource->lightProxyObj != NULL) {
        /* always use light proxy */
        Assert(planSource->gplan == NULL && planSource->cplan == NULL);

        lightProxy* lp = (lightProxy*)planSource->lightProxyObj;
        if (lp->m_cplan->stmt_name) {
            DropDatanodeStatement(lp->m_cplan->stmt_name);
        }
        lightProxy::tearDown(lp);
        planSource->lightProxyObj = NULL;
    } else {
        /* Decrement generic CachePlan's refcount and drop if no longer needed */
        ReleaseGenericPlan(planSource);
    }
}
