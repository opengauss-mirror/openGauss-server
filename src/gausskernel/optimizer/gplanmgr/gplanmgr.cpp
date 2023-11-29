/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * gplanmgr.cpp
 *    gplanmgr
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/gplanmgr/gplanmgr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "optimizer/gplanmgr.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/execRemote.h"
#include "instruments/instr_unique_sql.h"
#include "optimizer/planmem_walker.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_hint.h"
#include "access/hash.h"

typedef struct indexUsageWalkerCxt {
    MethodPlanWalkerContext mpwc;
    List *usage_list; /* The list of IdxQual */
    Index varelid;
    List* paramValList;
} indexUsageWalkerCxt;

typedef struct PlanIndexUasge {
    uint32 scanrelid;
    Oid indexoid;
    List *indexquals;
    bool is_partial;
    double selec;
} PlanIndexUasge;

const int8 MAX_CANDIDATE = 10;
const float LREANINGRATE = 0.001;

const int8 MIN_EXPL_TIMES = 5;
const int8 MIN_EVAL_TIMES = 3;

#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))

#define mgrMethod(planmgr) (((const PlanManager *)(planmgr))->method)
#define IsMethod(planmgr, _type_) (mgrMethod(planmgr) == (_type_))

static void SetPlanMemoryContext(CachedPlanSource *plansource, CachedPlan *plan);
static void AcquireManagerLock(PMGRAction *action, LWLockMode mode);
static void ReleaseManagerLock(PMGRAction *action);
static List *GenerateIndexCIs(indexUsageWalkerCxt context, bool *hasPartial);
static void StoreStmtRoot(const char *stmt_name, PlannerInfo *root, const char *qstr, MemoryContext cxt);
static StatementRoot *FetchStmtRoot(const char *stmt_name);
static PlannerInfo *InitGenericRoot(PlannerInfo *pinfo, ParamListInfo boundParams);
static Node *CacheIndexQual(Node *node, indexUsageWalkerCxt *context);
static List *GetBaseRelSelectivity(PlannerInfo *root);
static List *GenerateRelCIs(List *selectivities);
static inline void NextAction(PMGRAction *action, PMGRActionType acttype, PMGRStatCollectType statstype);
static void ClearGenericRootCache(PlannerInfo *pinfo, bool onlyPossionCache);
static bool ExtractPlanIndexesUsages(Node *plan, void *context);

/*
 * ReleaseCustomPlan: release a CachedPlanSource's custom plan, if any.
 */
void
ReleaseCustomPlan(CachedPlanSource *plansource)
{
    /* Be paranoid about the possibility that ReleaseCachedPlan fails */
    if (plansource->cplan) {
        CachedPlan *plan = plansource->cplan;

        Assert(plan->magic == CACHEDPLAN_MAGIC && !plan->is_candidate);
        plansource->cplan = NULL;

        /* release the plan */
        ReleaseCachedPlan(plan, false);
    }
}

GplanSelectionMethod
GetHintSelectionMethod(const CachedPlanSource* plansource)
{
    if (t_thrd.proc->workingVersionNum < PLAN_SELECT_VERSION_NUM) {
        return CHOOSE_DEFAULT_GPLAN;
    }
    Assert(list_length(plansource->query_list) == 1);
    Query *parse = (Query*)linitial(plansource->query_list);
    HintState *hint = parse->hintState;

    if (hint != NULL && hint->cache_plan_hint != NIL) {
        PlanCacheHint* pchint = (PlanCacheHint*)llast(hint->cache_plan_hint);
        if (pchint != NULL) {
            pchint->base.state = HINT_STATE_USED;
            return pchint->method;
        }
    }

    /* return CHOOSE_DEFAULT_GPLAN if no plancache hint is found. */
    return CHOOSE_DEFAULT_GPLAN;
}

bool
selec_gplan_by_hint(const CachedPlanSource* plansource)
{
    GplanSelectionMethod method = GetHintSelectionMethod(plansource);

    // it has chosen a gplan, thus the selection method should not be none.
    Assert(method != CHOOSE_NONE_GPLAN);

    // using default gplan is orthogonal to adaptive plan selection, return false.
    if(method == CHOOSE_DEFAULT_GPLAN){
        Assert(plansource->planManager == NULL);
        return false;
    }

    return true;
}

/* Decide whether to use the adaptive plan selection. */
bool
ChooseAdaptivePlan(CachedPlanSource *plansource, ParamListInfo boundParams)
{
    if (!ENABLE_CACHEDPLAN_MGR) {
        return false;
    }

    /*
     * unnamed stmt is one-shot, which is mainly used for PL funcs or batch
     * execution. It is not worth the effect to maintain temporary plans, just
     * return false.
     */
    if (plansource->stmt_name == NULL) {
        return false;
    }

    // limit number of params to avoid the huge memory consumption
    if (boundParams == NULL || boundParams->numParams >= 50) {
        return false;
    }

    if (strcmp(plansource->commandTag, "SELECT") != 0) {
        return false;
    }

    if (!plansource->is_saved) {
        return false;
    }

    // subquery is not supported by our plan selection method.
    if (plansource->hasSubQuery || !selec_gplan_by_hint(plansource)) {
        PMGR_ReleasePlanManager(plansource);
        return false;
    }

    return true;
}

CachedPlan*
GetDefaultGenericPlan(CachedPlanSource *plansource,
                               ParamListInfo boundParams,
                               List **qlist,
                               bool *mode)
{
    CachedPlan *plan = NULL;

    if (CheckCachedPlan(plansource, plansource->gplan)) {
        /* We want a generic plan, and we already have a valid one */
        plan = plansource->gplan;
        Assert(plan->magic == CACHEDPLAN_MAGIC);

        /* Update soft parse counter for Unique SQL */
        UniqueSQLStatCountSoftParse(1);
    } else {
        /* Whenever plan is rebuilt, we need to drop the old one */
        ReleaseGenericPlan(plansource);
        /* Build a new generic plan */
        plan = BuildCachedPlan(plansource, *qlist, NULL, false);
        Assert(!plan->isShared());

        /* Link the new generic plan into the plansource */
        plansource->gplan = plan;
        plan->refcount++;
        ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, plan->context);
        /* Immediately reparent into appropriate context */
        SetPlanMemoryContext(plansource, plan);
        /* Update generic_cost whenever we make a new generic plan */
        plansource->generic_cost = cached_plan_cost(plan);

        /*
         * Judge if gplan is single-node fqs, if so, we can use it when
         * enable_pbe_optimization is on
         */
        if (IS_PGXC_COORDINATOR && u_sess->attr.attr_sql.enable_pbe_optimization) {
            List *stmt_list = plan->stmt_list;
            bool plan_is_fqs = false;
            if (list_length(stmt_list) == 1) {
                Node *pstmt = (Node *)linitial(stmt_list);
                if (IsA(pstmt, PlannedStmt)) {
                    Plan *topPlan = ((PlannedStmt *)pstmt)->planTree;
                    if (IsA(topPlan, RemoteQuery)) {
                        RemoteQuery *rq = (RemoteQuery *)topPlan;
                        if (rq->exec_nodes &&
                            ((rq->exec_nodes->nodeList == NULL && rq->exec_nodes->en_expr != NULL) ||
                            list_length(rq->exec_nodes->nodeList) == 1))
                            plan_is_fqs = true;
                    }
                }
            }
            plansource->gplan_is_fqs = plan_is_fqs;
            ereport(DEBUG2, (errmodule(MOD_OPT),
                    errmsg("Custom plan is used for \"%s\"", plansource->query_string)));
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
        if (ChooseCustomPlan(plansource, boundParams)) {

            /*
             * If we choose to plan again, we need to re-copy the query_list,
             * since the planner probably scribbled on it.	We can force
             * BuildCachedPlan to do that by passing NIL.
             */
            *mode = true;
            *qlist = NIL;
            ReleaseGenericPlan(plansource);

            return NULL;
        }
    }
    Assert(plan);
    *mode = false;
    return plan;
}

/*
 * UpdateCI - make a embrace X
 */
static void
UpdateCI(CondInterval *tar, double val, double dampfactor)
{
    if(tar == NULL){
        return;
    }

    /* Expand the lowerboud of CI.*/
    if (val < tar->lowerbound) {
        /* leanring rate is used to avoid the aggressive expandsion. */
        tar->lowerbound -= Min(dampfactor, (tar->lowerbound - val) * 1.1);
    }

    /* Expand the upperbound of CI.*/
    if (val > tar->upperbound) {
        tar->upperbound += Min(dampfactor, (val - tar->upperbound) * 1.1);
    }
}

static void
UpdateOffsetCI(CachedPlanInfo *pinfo, int64 offset)
{
    if (pinfo->offsetCi == NULL) {
        return;
    }

    CondInterval *ci = (CondInterval *)pinfo->offsetCi;

    /* Expand the lowerboud of CI. */
    if (ci->lowerbound > offset) {
        /* Here, the leanring rate is used to avoid the aggressive expandsion. */
        ci->lowerbound -= (ci->lowerbound - offset);
    }
    /* Expand the upperbound of CI. */
    if (ci->upperbound < offset) {
        ci->upperbound += (offset - ci->upperbound);
    }
}

/*
 * FallIntoCI: is the given selectivity fall in ci?
 */
static bool
FallIntoCI(CondInterval *ci, double selectivity)
{
    if (ci->lowerbound <= selectivity && \
        ci->upperbound >= selectivity) {
        return true;
    }
    return false;
}

/*
 * Return the hashkey of planTree for a quick plan comparsion. Although
 * the hash collision may happen occasionally and lead to a false positive
 * equivalence, we still accept this issue to trade off the correctness
 * to efficiency.
 */
static uint32
GeneratePlanStmtKey(PlannedStmt *pstmt)
{
    uint32 val = 0;
    char *key = NULL;

    u_sess->opt_cxt.out_plan_stat = false;
    PG_TRY();
    {
        key = nodeToString(pstmt->planTree);
    }
    PG_CATCH();
    {
        u_sess->opt_cxt.out_plan_stat = true;
        PG_RE_THROW();
    }
    PG_END_TRY();
    u_sess->opt_cxt.out_plan_stat = true;
    val = DatumGetUInt32(hash_any((const unsigned char *)key, strlen(key) + 1));

    return val;
}

/* Given a hashkey, return the CachedPlanInfo in cache list. */
static CachedPlan*
SearchCandidatePlan(List *candidates, uint32 key)
{
    const ListCell *cell = NULL;

    foreach (cell, candidates) {
        CachedPlan *plan = (CachedPlan *)lfirst(cell);
        Assert(IsA(plan->cpi, CachedPlanInfo));
        /* We find it and return. */
        if (key == plan->cpi->planHashkey) {
            return plan;
        }
    }
    return NULL;
}

static void
SetPlanMemoryContext(CachedPlanSource *plansource, CachedPlan *plan)
{
    /* saved plans all live under CacheMemoryContext */
    if (plansource->is_saved) {
        if (plansource->gpc.status.IsPrivatePlan()) {
            /* saved plans all live under CacheMemoryContext */
            MemoryContextSetParent(plan->context, u_sess->cache_mem_cxt);
        }
        plan->is_saved = true;
    } else {
        if (plansource->gpc.status.IsPrivatePlan()) {
            /* otherwise, it should be a sibling of the plansource */
            MemoryContextSetParent(plan->context,
                                   MemoryContextGetParent(plansource->context));
        }
    }
}

static void
AcquireManagerLock(PMGRAction *action, LWLockMode mode)
{
    if (action->is_shared) {
        /* apply lock according to LWLockMode */
        Assert(action->lock_id >= 0);
        (void)LWLockAcquire(GetMainLWLockByIndex(action->lock_id), mode);
        action->is_lock = true;
        action->lockmode = mode;
    }
}

static void
ManagerLockSwitchTo(PMGRAction *action, LWLockMode mode)
{
    /* a local plansource does not need a lock. */
    if (!action->is_shared) {
        Assert(!action->is_lock);
        return;
    }
    if (!action->is_lock) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("Please get a lock first and then switch its type. query: \"%s\"", action->psrc->query_string)));
    }

    /* If the requested lock has been acquired, do nothing. */
    if (action->lockmode == mode) {
        return;
    } else {
        /* release current lock for acquiring again. */
        ReleaseManagerLock(action);
        /* apply lock according to LWLockMode */
        (void)LWLockAcquire(GetMainLWLockByIndex(action->lock_id), mode);
        action->lockmode = mode;
        action->is_lock = true;
    }
}

static void
ReleaseManagerLock(PMGRAction *action)
{
    if (action->is_lock) {
        Assert(action->lock_id >= 0 && action->is_shared);
        LWLockRelease(GetMainLWLockByIndex(action->lock_id));
        action->is_lock = false;
    }
}

static CachedPlan*
PMGR_ExplorePlan(CachedPlanSource *plansource,
                      ParamListInfo boundParams,
                      List **qlist)
{
    CachedPlan *plan = NULL;

    /* if 'boundParams' is not NULL, use its information to guide the plan gerneration. */
    if(boundParams != NULL){

        /*
         * Only CHOOSE_ADAPTIVE_GPLAN mode does a param value matching. i.e., it can detect
         * that a plan with partial index on 'age > 30' does not work for queries refering
         * to teenagers.
         */
        if(plansource->planManager->method == CHOOSE_ADAPTIVE_GPLAN){
            boundParams->uParamInfo = PARAM_VAL_SELECTIVITY_INFO;
        } else {
            boundParams->uParamInfo = PARAM_SELECTIVITY_INFO;
        }
        boundParams->params_lazy_bind = true;
    }

    u_sess->pcache_cxt.is_plan_exploration = true;
    /* explore the query plan by planner. */
    plan = BuildCachedPlan(plansource, *qlist, boundParams, false);

    boundParams->params_lazy_bind = false;
    u_sess->pcache_cxt.is_plan_exploration = false;

    plan->cost = cached_plan_cost(plan);

    ereport(DEBUG2, (errmodule(MOD_OPT),
                  errmsg("Explore a plan; ThreadId: %d, query: \"%s\"", gettid(), plansource->query_string)));

    Assert(!plan->isShared());
    ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, plan->context);
    /* saved plans all live under CacheMemoryContext */
    SetPlanMemoryContext(plansource, plan);
    return plan;
}

static inline int64
GetLimitValue(Query *query, ParamListInfo boundParams)
{
    int64 offsetVal = -1;
    if (query->limitOffset != NULL) {
        if (IsA(query->limitOffset, Param)) {
            Param *offset = (Param *)query->limitOffset;
            Assert(boundParams->params[offset->paramid - 1].ptype == INT8OID);
            offsetVal = DatumGetInt64(boundParams->params[offset->paramid - 1].value);
        }
    }

    return offsetVal;
}

bool
MatchPartIdxQuery(PlannerInfo *queryRoot)
{
    for(int rti = 1; rti < queryRoot->simple_rel_array_size; rti++){
        RelOptInfo *rel = queryRoot->simple_rel_array[rti];
        if(rel == NULL || rel->reloptkind != RELOPT_BASEREL || rel->rtekind != RTE_RELATION){
            continue;
        }
        ListCell *lc;
        foreach(lc, rel->indexlist){
            IndexOptInfo *idx = (IndexOptInfo *)lfirst(lc);
            if(idx->predOK){
                return true;
            }
        }
    }
    return false;
}

static void
InsertPlan(CachedPlanSource *plansource,
              ParamListInfo boundParams,
              CachedPlan *plan,
              uint32 planHashkey)
{
    if(!plansource->is_saved){
        ereport(DEBUG2, (errmodule(MOD_OPT),
                      errmsg("skip to cache plan into an unsaved plansource; ThreadId: %d, query: \"%s\"",
                      gettid(), plansource->query_string)));
        return;
    }

    CachedPlanInfo *cpinfo = NULL;
    PlanManager *planMgr = plansource->planManager;
    PlannerInfo *expRoot = (PlannerInfo *)u_sess->pcache_cxt.explored_plan_info;
    Query *query = (Query *)linitial(plansource->query_list);

    PlannedStmt *stmt = (PlannedStmt *)linitial(plan->stmt_list);
    indexUsageWalkerCxt used_indexes_cxt;

    errno_t rc = 0;
    size_t cxt_size = sizeof(indexUsageWalkerCxt);
    rc = memset_s(&used_indexes_cxt, cxt_size, 0, cxt_size);
    securec_check(rc, "\0", "\0");

    exec_init_plan_tree_base(&used_indexes_cxt.mpwc.base, stmt);

    /*
     * Get the usages of indexes by traversing the plan tree.
     */
    ExtractPlanIndexesUsages((Node *)stmt->planTree, &used_indexes_cxt);

    /* collect the plan selectivities and generate CI for selection. */
    MemoryContext oldcxt = MemoryContextSwitchTo(plan->context);
    int64 offset = -1;

    cpinfo = makeNode(CachedPlanInfo);
    cpinfo->learningRate = LREANINGRATE;
    cpinfo->plan = plan;
    cpinfo->planHashkey = planHashkey;
    cpinfo->sample_exec_costs = 0;
    cpinfo->sample_times = 0;
    cpinfo->verification_times = 1;
    cpinfo->status = ADPT_PLAN_UNCHECKED;
    plan->is_candidate = true;

    offset = GetLimitValue(query, boundParams);
    if (offset > 0) {
        CondInterval *offsetCi = makeNode(CondInterval);
        offsetCi->relid = 0;
        offsetCi->lowerbound = offset;
        offsetCi->upperbound = offset;
        cpinfo->offsetCi = (void *)offsetCi;
    }
    cpinfo->relCis = GenerateRelCIs(GetBaseRelSelectivity(expRoot));
    cpinfo->indexCis = GenerateIndexCIs(used_indexes_cxt, &cpinfo->usePartIdx);
    if (plansource->gpc.status.InShareTable()) {
        pg_atomic_fetch_add_u32((volatile uint32 *)&plan->global_refcount, 1);
        plan->is_share = true;
        Assert(plan->context->is_shared);
        MemoryContextSeal(plan->context);
    } else {
        plan->refcount++;
    }
    plan->is_saved = true;
    plan->cpi = cpinfo;

    (void)MemoryContextSwitchTo(planMgr->context);
    planMgr->candidatePlans = lappend(planMgr->candidatePlans, plan);
    (void)MemoryContextSwitchTo(oldcxt);

}

CachedPlan*
SetMostUsedPlan(PlanManager *manager)
{
    CachedPlan *most_used_plan = NULL;
    double max_time = 0;
    ListCell *lc = NULL;

    foreach (lc, manager->candidatePlans) {
        CachedPlan *plan = (CachedPlan *)lfirst(lc);
        if (plan->cpi->verification_times > max_time) {
            max_time = plan->cpi->verification_times;
            most_used_plan = plan;
        }
    }
    return most_used_plan;
}

List*
MakeValuedRestrictinfos(PlannerInfo *queryRoot, List *quals)
{
    ListCell *cell;
    List *rst = NIL;

    queryRoot->glob->boundParams->params_lazy_bind =false;

    foreach(cell, quals){
        Node *qual = (Node *)lfirst(cell);
        qual = eval_const_expressions(queryRoot, qual);
        RestrictInfo *rinfo = make_simple_restrictinfo((Expr *)qual);
        rst = lappend(rst, rinfo);
    }

    return rst;
}

/* check whether a given index is matched  */
IndexOptInfo*
GetIndexInfo(PlannerInfo *root, Index relid, Oid indexid)
{
    ListCell *lc;
    RelOptInfo *rel = root->simple_rel_array[relid];

    if(relid == 0 || rel == NULL){
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
           errmsg("GetIndexInfo: relid is invaild.")));
    }
    foreach (lc, rel->indexlist) {
        IndexOptInfo *idxinfo = (IndexOptInfo *)lfirst(lc);
        if (idxinfo->indexoid == indexid) {
            return idxinfo;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_WARNING),
       errmsg("IsIndexPredOK: please give a vaild indexid.")));

    return NULL;
}

bool
IsMatchedPlan(PlannerInfo *queryRoot, List *query_rel_sels,
                  CachedPlanInfo *cpinfo, bool q_usePartIdx)
{
    int64 queryOffset = -1;

    /*
     * Suppose that a query prefers to use a partial index scan if it matches.
     * Therefore, a plan without using such index should be skipped for a
     * partIdx-matched query.
     */
    if(q_usePartIdx != cpinfo->usePartIdx){
        return false;
    }

    /* check whether offset values are matched */
    queryOffset = GetLimitValue(queryRoot->parse, queryRoot->glob->boundParams);
    if (queryOffset > 0 &&
        !FallIntoCI((CondInterval *)cpinfo->offsetCi, queryOffset)) {
        return false;
    }

    Assert((cpinfo->relCis == NIL && query_rel_sels == NIL) ||
           cpinfo->relCis->length == query_rel_sels->length);

    /* check whether query can fall into baserel CIs of any candidate plan. */
    ListCell *c1;
    ListCell *c2;
    forboth(c1, cpinfo->relCis, c2, query_rel_sels) {
        RelCI *relCI = (RelCI *)lfirst(c1);
        RelSelec *q = (RelSelec *)lfirst(c2);

        if (!FallIntoCI(&relCI->ci, q->selectivity)) {
            return false;
        }
    }

    /* do index matching if indexCis is not NIL. */
    ListCell *cell;
    List *clauses = NIL;
    foreach (cell, cpinfo->indexCis) {
        IndexCI *idxci = (IndexCI *)lfirst(cell);

        /*
         * if the checked index is partial and it does not match with query,
         * return false.
         */
        if (idxci->is_partial) {
            IndexOptInfo *idxinfo = GetIndexInfo(queryRoot,
                                                 idxci->ci.relid, idxci->indexoid);
            /* note that 'predOK' is set by set_base_rel_sizes() */
            if (!idxinfo->predOK) {
                return false;
            }
        }

        /*
         * bind parameters and organize evalated indexquals as a RestrictInfo
         * list. Note that, 'clauses' just is a copy of cached 'indexquals'.
         */
        clauses = MakeValuedRestrictinfos(queryRoot, idxci->indexquals);
        if (clauses == NIL) {
            return false;
        }

        /*
         * At each invocation, 'clauselist_selectivity' would cache one-shot
         * selectivity information, 'varratio, varEqRatio', into 'queryRoot'.
         * These invaild information would lead to an incorrect result,
         * if we do not clear them up before a new selectivity computation.
         */
        ClearGenericRootCache(queryRoot, false);
        /* get index Selectivity */
        double query_idx_selec = clauselist_selectivity(queryRoot,
                                                        clauses,
                                                        idxci->ci.relid,
                                                        JOIN_INNER,
                                                        NULL,
                                                        false);

        if (!FallIntoCI(&idxci->ci, query_idx_selec)) {
            return false;
        }
    }

    return true;
}

bool
MakePlanMatchQuery(PlannerInfo *queryRoot, List *query_rel_sels, CachedPlanInfo *cpinfo)
{
    int64 queryOffset = -1;

    /* check whether offset values are matched */
    queryOffset = GetLimitValue(queryRoot->parse, queryRoot->glob->boundParams);
    UpdateOffsetCI(cpinfo, queryOffset);

    /* Update baserel CI for matching. */
    ListCell *c1;
    ListCell *c2;
    forboth(c1, cpinfo->relCis, c2, query_rel_sels) {
        RelCI *relCI = (RelCI *)lfirst(c1);
        RelSelec *qRelSelec = (RelSelec *)lfirst(c2);

        UpdateCI(&relCI->ci, qRelSelec->selectivity, cpinfo->learningRate);
    }

    /* Update index CI for matching. */
    ListCell *cell;
    List *clauses = NIL;
    foreach (cell, cpinfo->indexCis) {
        IndexCI *idxci = (IndexCI *)lfirst(cell);

        /*
         * bind parameters and organize evalated indexquals as a RestrictInfo
         * list. Note that, 'clauses' just is a copy of cached 'indexquals'.
         */
        clauses = MakeValuedRestrictinfos(queryRoot, idxci->indexquals);

        /*
         * At each invocation, 'clauselist_selectivity' would cache one-shot
         * selectivity information, 'varratio, varEqRatio', into 'queryRoot'.
         * These invaild information would lead to an incorrect result,
         * if we do not clear them up before a new selectivity computation.
         */
        ClearGenericRootCache(queryRoot, false);

        /* get index Selectivity */
        double query_idx_selec = clauselist_selectivity(queryRoot,
                                                        clauses,
                                                        idxci->ci.relid,
                                                        JOIN_INNER,
                                                        NULL,
                                                        false);

        UpdateCI(&idxci->ci, query_idx_selec, cpinfo->learningRate);
    }

    return true;
}

static CachedPlan*
FindMatchedPlan(PlanManager *manager,     PMGRAction *action)
{
    ListCell *cl;
    bool usePartIdx = false;
    PlannerInfo *queryRoot = action->genericRoot;

    if (queryRoot->glob->boundParams == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
           errmsg("FindMatchedPlan: query params is missed.")));
    }

    queryRoot->glob->boundParams->uParamInfo = PARAM_VAL_SELECTIVITY_INFO;
    Assert(queryRoot->glob->boundParams->params_lazy_bind == false);

    /* Do a quick baserel-selectivity estimation. */
    set_base_rel_sizes(queryRoot, true);
    action->qRelSelec = GetBaseRelSelectivity(queryRoot);
    usePartIdx = MatchPartIdxQuery(queryRoot);

    /* A plan is matched if both types of CIs can dominate */
    foreach (cl,  manager->candidatePlans) {
        CachedPlan *plan = (CachedPlan *)lfirst(cl);
        if (IsMatchedPlan(queryRoot, action->qRelSelec, plan->cpi, usePartIdx)) {
            return plan;
        }
    }

    /*
     * If all candidates are mis-matched, trigger a new plan exploration by
     * returning NULL.
     */
    return NULL;
}

bool
ContainSubQuery(PlannerInfo *root)
{
    if (root->glob->subroots != NIL) {
        return true;
    }

    for (int rti = 1; rti < root->simple_rel_array_size; rti++) {
        RelOptInfo *rel = root->simple_rel_array[rti];
        if (rel == NULL) {
            continue;
        }
        if (rel->rtekind == RTE_SUBQUERY) {
            return true;
        }
    }
    return false;
}

void
CacheGenericRoot(CachedPlanSource *plansource, char *psrc_key)
{
    MemoryContext oldcxt = CurrentMemoryContext;

    /*
     * Store the PlannerInfo into a session-shared hashtab when a parameterized-
     * customized plan is generated at the first time.
     *
     * Note that, we do not store PlannerInfo for PLSQL stmts since stmt_name is
     * the hashtab key and should not be NULL.
     */
    if (plansource->stmt_name == NULL ||
        plansource->planManager->method != CHOOSE_ADAPTIVE_GPLAN) {
        return;
    }

    Assert(u_sess->pcache_cxt.explored_plan_info);

    ResourceOwnerEnlargeGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner);
    MemoryContext root_context = AllocSetContextCreate(u_sess->cache_mem_cxt,
                                                       "GenericRoot",
                                                       ALLOCSET_SMALL_MINSIZE,
                                                       ALLOCSET_SMALL_INITSIZE,
                                                       ALLOCSET_DEFAULT_MAXSIZE);
    ResourceOwnerRememberGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, root_context);

    MemoryContextSwitchTo(root_context);
    PlannerInfo* planner_info = (PlannerInfo *)copyObject((void *)u_sess->pcache_cxt.explored_plan_info);

    /*
     * Initialize the cached planinfo by reseting the parameters as default values.
     * It is to avoid the scribble by previous 'set_base_rel_sizes' call.
     */
    ClearGenericRootCache(planner_info, false);

    StoreStmtRoot(psrc_key, planner_info, plansource->query_string, root_context);
    MemoryContextSwitchTo(oldcxt);
    ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, root_context);

    /* reset the pointer for the next round */
    u_sess->pcache_cxt.explored_plan_info = NULL;
}

CachedPlan*
PMGR_InsertPlan(CachedPlanSource *plansource,
                    ParamListInfo boundParams,
                    CachedPlan *new_plan,
                    uint32 new_plan_key,
                    PMGRAction *action)
{
    PlanManager *PlanMgr = plansource->planManager;

    int num_candidates = list_length(PlanMgr->candidatePlans);
    CachedPlan *cachedplan = SearchCandidatePlan(PlanMgr->candidatePlans, new_plan_key);
    bool alreadyCached = cachedplan != NULL ? true : false;

    /*
     * if explored plan has been cached, update the stats; otherwise, add it
     * into the candidate list.
     */
    if (alreadyCached) {
        ereport(DEBUG2, (errmodule(MOD_OPT),
                errmsg("Explored plan already has been cached, try to update planCIs; ThreadId: %d, query: \"%s\"",
                       gettid(), plansource->query_string)));

        pg_atomic_fetch_add_u32((volatile uint32 *)&cachedplan->cpi->verification_times, 1);
        
        /* Update PlanCI */
        MakePlanMatchQuery(action->genericRoot, action->qRelSelec, cachedplan->cpi);

        /* release new_plan after updates are completed. */
        MemoryContextDelete(new_plan->context);
    } else if (num_candidates < MAX_CANDIDATE) {
        InsertPlan(plansource, boundParams, new_plan, new_plan_key);
        ereport(DEBUG2, (errmodule(MOD_OPT),
                errmsg("add explored plan into cache; ThreadId: %d, query: \"%s\"",
                       gettid(), plansource->query_string)));
    }

    return alreadyCached ? cachedplan : new_plan;
}

/*
 * GetFirstAction: get the first step of plan selection according to choosing method.
 *
 * if the selected strategy is determined, move to the corresponding algorithm and
 * begins; otherwise, jump to the step of method selection to find out which method
 * is the best.
 */
void
GetFirstAction(CachedPlanSource *plansource, PMGRAction *action)
{
    PlanManager *manager = plansource->planManager;

    if (manager->method == CHOOSE_MINCOST_GPLAN && manager->mini_cost_plan != NULL) {
        NextAction(action, PMGR_USE_MINICOST, action->statType);
        return;
    } else if (manager->method == CHOOSE_ADAPTIVE_GPLAN) {
        NextAction(action, PMGR_USE_MATCH, action->statType);
        return;
    }

    NextAction(action, PMGR_CHOOSE_BEST_METHOD, action->statType);
}

/*
 * CreateAction: create a PMGRAction
 */
PMGRAction*
CreateAction(CachedPlanSource *plansource)
{
    MemoryContext action_context;
    MemoryContext oldcxt = CurrentMemoryContext;
    action_context = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
                                           "PMGRAction",
                                           ALLOCSET_SMALL_MINSIZE,
                                           ALLOCSET_SMALL_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(action_context);
    PMGRAction *action = (PMGRAction *)palloc0(sizeof(PMGRAction));
    action->type = PMGR_START;
    action->psrc = plansource;
    action->selected_plan = NULL;
    action->qRelSelec = NIL;
    action->valid_plan = false;
    action->statType = PMGR_GET_NONE_STATS;
    action->is_shared = plansource->gpc.status.InShareTable();
    action->lock_id = plansource->gpc_lockid;
    action->is_lock = false;
    action->lockmode = LW_SHARED;
    action->needGenericRoot = false;
    action->genericRoot = NULL;
    action->step = 0;
    MemoryContextSwitchTo(oldcxt);

    return action;
}

/*
 * NextAction: move action to next step by update the acttype. 
 * 
 * statstype indicates how to collect statistic.
 */
static inline void
NextAction(PMGRAction *action,
              PMGRActionType acttype,
              PMGRStatCollectType statstype)
{
    action->type = acttype;
    action->statType = statstype;
    action->step++;
}

/*
 * ActionHandle: the handle of the plan selection step.
 */
void
ActionHandle(CachedPlanSource *plansource,
                 ParamListInfo boundParams,
                 List **qlist,
                 PMGRAction *action)
{
    PlanManager *manager = plansource->planManager;
    char *psrc_key = NULL;

    
    /* Obtain statement root for plan matching. */
    psrc_key = manager->psrc_key;
    Assert(psrc_key[0] != '\0');

    switch (action->type) {
        case PMGR_START: {
            GetFirstAction(plansource, action);
        } break;
        case PMGR_EXPLORE_PLAN: {
            CachedPlan *plan = PMGR_ExplorePlan(plansource, boundParams, qlist);
            uint32 plan_key = GeneratePlanStmtKey((PlannedStmt *)linitial(plan->stmt_list));

            // if plan is not in candidate list, try to insert it.
            if (manager->method == CHOOSE_ADAPTIVE_GPLAN ||
                SearchCandidatePlan(manager->candidatePlans, plan_key) == NULL) {
                /* acquire an exclusive lock to update plan manager. */
                ManagerLockSwitchTo(action, LW_EXCLUSIVE);

                if (ContainSubQuery((PlannerInfo *)u_sess->pcache_cxt.explored_plan_info)) {
                    manager->is_valid = false;
                    plansource->hasSubQuery  = true;
                    ereport(LOG, (errmodule(MOD_OPT),
                            errmsg("plan selection can not handle with SQL with subqueries, go to the default mode in the next round; ThreadId: %d, query: \"%s\"",
                                   gettid(), plansource->query_string)));
                }

                /*
                 * A valid save_xmin (!= 0) imples that the plan is temporary. To prevent
                 * the high cost of plan revaildation, we do not cache such plan to avoid
                 * plansource recreation across all sessions.
                 *
                 * The side effect is that, plan would be generated repeatedly in certain
                 * temporary slots. However, it should not be cost too much as the temporary
                 * slots are narrow.
                 */
                if (!TransactionIdIsValid(plan->saved_xmin) && manager->is_valid) {
                    plan = PMGR_InsertPlan(plansource, boundParams, plan, plan_key, action);
                    if (action->needGenericRoot) {
                        CacheGenericRoot(plansource, psrc_key);
                        action->needGenericRoot = false;
                    ereport(DEBUG2, (errmodule(MOD_OPT),
                            errmsg("Cache generic root; ThreadId: %d, query: \"%s\"",
                                   gettid(), plansource->query_string)));
                    }
                }

                //switch to shared lock for a better concurrence
                ManagerLockSwitchTo(action, LW_SHARED);
            }

            /*
             * although plansource->gplan would not be used in the adaptive mode,
             * we just set it to make the globalplancache routine works.
             */
            if (plansource->gplan == NULL) {
                plansource->gplan = plan;
                if (plan->refcount == 0) {
                    plan->refcount++;
                }
                plansource->generic_cost = cached_plan_cost(plan);
            }
            Assert(plansource->gplan->refcount > 0);

            action->selected_plan = plan;
            action->valid_plan = true;

            /* close the lazy bind labels to avoid the binding failure in executor. */
            boundParams->uParamInfo = DEFUALT_INFO;
            boundParams->params_lazy_bind = false;
            NextAction(action, PMGR_FINISH, action->statType);
        } break;
        case PMGR_CHOOSE_BEST_METHOD: {
            Assert(action->selected_plan == NULL);
            /* acquire an exclusive lock to update plan manager. */
            ereport(ERROR, (errmodule(MOD_OPT),
                        errmsg("choose plan selection method is not supported yet. ")));
        } break;
        case PMGR_USE_MINICOST: {
            //mini cost method
            ereport(ERROR, (errmodule(MOD_OPT),
                        errmsg("mini cost method is not supported yet. ")));
                NextAction(action, PMGR_FINISH, action->statType);
        } break;
        case PMGR_USE_MATCH: {
            Assert(action->selected_plan == NULL);
            StatementRoot *entry = FetchStmtRoot(psrc_key);

            /*
             * If StatementRoot does not exist, trigger a query parsing to generate one.
             */
            if (entry == NULL) {

                /*
                 * Note that, when gpc is open, genericRoot should not be cached until
                 * plansource is shared. It is because that an unshared plansource key,
                 * 'psrc_key', only guarantees the uniqueness in session level. It should
                 * be rewritten as a global key when the plansource becomes shared accorss
                 * sessions. Please find more detail in GlobalPlanCache::TryStore().
                 */
                if (!ENABLE_GPC || plansource->gpc.status.InShareTable()){
                    action->needGenericRoot = true;
                }
                NextAction(action, PMGR_EXPLORE_PLAN, PMGR_GET_NONE_STATS);
            } else {
                Assert(strcmp(plansource->query_string, entry->query_string) == 0);

                /* initialize root's statistics cache and set boundParams */
                action->genericRoot = InitGenericRoot(entry->generic_root, boundParams);
                action->selected_plan = FindMatchedPlan(plansource->planManager, action);
                NextAction(action, PMGR_CHECK_PLAN, PMGR_GET_NONE_STATS);
            }
        } break;
        case PMGR_CHECK_PLAN: {

            /*
             * plansource maybe invalid, as a table update would be ahead of lock
             * fetching in plan matching process. Therefore revalid plansource,
             * and invalid the current plan manager and the selected plan.
             */
            if (!plansource->is_valid) {
                (void)RevalidateCachedQuery(plansource);
                Assert(!plansource->planManager->is_valid);
                if(action->selected_plan != NULL) {
                    action->selected_plan->is_valid = false;
                }
            }

            if (CheckCachedPlan(plansource, action->selected_plan)) {
                Assert(action->selected_plan->magic == CACHEDPLAN_MAGIC);
                /* Update soft parse counter for Unique SQL */
                UniqueSQLStatCountSoftParse(1);
                action->valid_plan = true;
                ereport(DEBUG2, (errmodule(MOD_OPT),
                        errmsg("selected a plan successfully; ThreadId: %d, query: \"%s\"",
                               gettid(), plansource->query_string)));
                NextAction(action, PMGR_FINISH, action->statType);
            } else {
                action->valid_plan = false;
                if (action->selected_plan == NULL) {
                    ereport(DEBUG2, (errmodule(MOD_OPT),
                            errmsg("fail to select a plan; ThreadId: %d, query: \"%s\"",
                                   gettid(), plansource->query_string)));

                } else {
                    // selected_plan is invaild, reset the pointer as NULL
                    action->selected_plan = NULL;
                    ereport(DEBUG2, (errmodule(MOD_OPT),
                            errmsg("selected plan is invalid; ThreadId: %d, query: \"%s\"",
                                   gettid(), plansource->query_string)));
                }

                NextAction(action, PMGR_EXPLORE_PLAN, PMGR_GET_NONE_STATS);
            }
        } break;
        case PMGR_FINISH: {
            // ending tag; do nothing...
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
               errmsg("unsupported action of plan manager: %d",
                   (int)manager->method)));
            break;
    }
}

/*
 * GetAdaptGenericPlan: get a cached plan from a CachedPlanSource
 *
 * Different to GetDefaultGenericPlan, GetAdaptGenericPlan caches k feasible plans
 * and picks the best one by matching query-plan selectivities iteratively. This
 * manner actually can avoid execution regression on heavily skewed data.
 *
 * Note that GetAdaptGenericPlan needs to extra estimate selectivies of queries; it
 * should make the process a little bit slower than the case that the default one
 * can do all thing right.
 */
CachedPlan*
GetAdaptGenericPlan(CachedPlanSource *plansource,
                            ParamListInfo boundParams,
                            List **qlist,
                            bool *mode)
{
    PMGRAction *action;
    GplanSelectionMethod select_method;
    double avg_custom_cost;
    
    /* plan management only supports named stmts. */
    Assert(plansource->stmt_name);

   ReleaseCustomPlan(plansource);
   select_method = GetHintSelectionMethod(plansource);
   Assert(select_method == CHOOSE_ADAPTIVE_GPLAN);

    if(plansource->planManager != NULL && !plansource->planManager->is_valid){
        Assert(!plansource->gpc.status.InShareTable());
        PMGR_ReleasePlanManager(plansource);
        plansource->planManager = NULL;
    }

    /*
     * create adaptive plan mgr when plansource is first used.
     */
    if (plansource->planManager == NULL) {
        Assert(!plansource->gpc.status.InShareTable());
        plansource->planManager = PMGR_CreatePlanManager(plansource->context,
                                                         plansource->stmt_name,
                                                         select_method);
        ereport(DEBUG2,
            (errmodule(MOD_OPT),
             errmsg("Initialize adaptive plan selection context; query: \"%s\"",
             plansource->query_string)));
    }

    u_sess->pcache_cxt.explored_plan_info = NULL;
    u_sess->pcache_cxt.is_plan_exploration = false;

    if (u_sess->pcache_cxt.action != NULL) {
        pfree(u_sess->pcache_cxt.action);
    }

    /* prepare the selection by initializing the selected-action context. */
    action = CreateAction(plansource);
    u_sess->pcache_cxt.action = (void *)action;

    ereport(DEBUG2, (errmodule(MOD_OPT),
            errmsg("begin plan selection. ThreadId: %d, query: \"%s\"",
                   gettid(), plansource->query_string)));

    /* Lock plansource when a selection is beginning. */
    AcquireManagerLock(action, LW_SHARED);

    /*
     * Select a plan for execution. In each round, action is taken one
     * step forward until it reaches a terminated state "PMGR_FINISH". The
     * framework can be brief described as follows:
     *
     * 1. plan exploration
     *    parse a small portion of queries to obtain feasible candidate
     *    plans. Record the average runtime as a benchmark of the stmt
     *    execution. Suppose that planner is wise enough, and thus a
     *    selection method closed to this benchmark can be considered
     *    as a good one.
     *
     *    Note that, the environment changes, w.r.t. concurrent increasing,
     *    guc modification, lock competition, etc., may fluctuate runtimes.
     *
     * 2. plan evaluation
     *    evaluate efficiency of each candidate plan by computing average
     *    runtime of its k executions.
     *
     * 3. method selection
     *    choose the best selection method to minimize the total cost. In
     *    particular, if P is the most efficient plan and it has
     *
     *                 rumtime(P) <= factor * benchmark
     *
     *    we select the mini-cost method and use P to execute all queries
     *    for the stmt. otherwise, an adaptive plan selection is chosen.
     *
     * 4. adaptive plan selection
     *    adaptive pick a plan based on selectivity matching. To avoid
     *    the performance regression, the selection begins with a performance
     *    checking: if a regression is detected, go to step 3; otherwise,
     *    pick a plan and go to step 5.
     *
     * 5. plan verification
     *    check the selected plan: if valid, finished; otherwise, generate
     *    a new plan to execute (go to step 1).
    */
    do {
        ActionHandle(plansource, boundParams, qlist, action);
        Assert(action->step < 10);
    } while (action->type != PMGR_FINISH);

    if (boundParams != NULL) {
        boundParams->uParamInfo = DEFUALT_INFO;
        boundParams->params_lazy_bind = false;
    }

    /* Selection is finished; unlock plansource */
    ReleaseManagerLock(action);

    ereport(DEBUG2, (errmodule(MOD_OPT),
            errmsg("End plan selection. ThreadId: %d, query: \"%s\"",
                   gettid(), plansource->query_string)));

    avg_custom_cost = plansource->total_custom_cost / plansource->num_custom_plans;

    /*
     * Prefer selected plan if it's less expensive than the average custom plan;
     * otherwise, return null for repicking a cplan.
     *
     * Note that, a knockout plan would not be in candidate list. If so, we release
     * it to avoid memory leak.
     */
    if (action->selected_plan->cost > 1.1 * avg_custom_cost) {
        Assert(action->selected_plan->cost >= 0);
        if (!action->selected_plan->is_candidate) {
            Assert(action->selected_plan->refcount == 0);

            /*
             * guarantee that refcount equals to 1, and thus we can use
             * 'ReleaseCachedPlan' to release the plan.
             */
            action->selected_plan->refcount = 1;
            if (plansource->gplan == action->selected_plan) {
                plansource->gplan = NULL;
            }
            ReleaseCachedPlan(action->selected_plan, false);
        }
        action->selected_plan = NULL;
        *mode = true;
    }


    return action->selected_plan;
}

/*
 * GetCustomPlan: return a cplan by parsing query.
 */
CachedPlan*
GetCustomPlan(CachedPlanSource *plansource,
                  ParamListInfo boundParams,
                  List **qlist)
{
    CachedPlan *plan = NULL;
    /* Whenever plan is rebuild, we need to drop the old one */
    ReleaseCustomPlan(plansource);

    /* Build a custom plan */
    plan = BuildCachedPlan(plansource, *qlist, boundParams, true);
    /* Link the new custome plan into the plansource */
    plansource->cplan = plan;
    plan->refcount++;
    ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, plan->context);

    /* saved plans all live under CacheMemoryContext */
    SetPlanMemoryContext(plansource, plan);

    /* Accumulate total costs of custom plans, but 'ware overflow */
    if (plansource->num_custom_plans < INT_MAX) {
        plansource->total_custom_cost += cached_plan_cost(plan);
        plansource->num_custom_plans++;
    }

    return plan;
}

/*
 * PMGR_CreatePlanManager: create the plan selection context.
 */
PlanManager*
PMGR_CreatePlanManager(MemoryContext parent_cxt, char* stmt_name,
                                GplanSelectionMethod method)
{
    MemoryContext mgr_context;
    if (parent_cxt->is_shared) {
        mgr_context = AllocSetContextCreate(parent_cxt, 
                                            "SharedPlanManager",
                                            ALLOCSET_SMALL_MINSIZE,
                                            ALLOCSET_SMALL_INITSIZE,
                                            ALLOCSET_DEFAULT_MAXSIZE,
                                            SHARED_CONTEXT);
    } else {
        mgr_context = AllocSetContextCreate(parent_cxt,
                                            "LocalPlanManager",
                                            ALLOCSET_SMALL_MINSIZE,
                                            ALLOCSET_SMALL_INITSIZE,
                                            ALLOCSET_DEFAULT_MAXSIZE);
    }

    MemoryContext oldcxt = MemoryContextSwitchTo(mgr_context);

    PlanManager *planMgr = (PlanManager *)palloc(sizeof(PlanManager));
    planMgr->method = method;
    planMgr->candidatePlans = NIL;
    planMgr->is_valid = true;
    planMgr->context = mgr_context;
    planMgr->explore_exec_costs = 0;
    planMgr->explore_times = 0;
    planMgr->mini_cost_plan = NULL;
    errno_t rc = strcpy_s(planMgr->psrc_key,
                          sizeof(planMgr->psrc_key)/sizeof(planMgr->psrc_key[0]),
                          stmt_name);
    securec_check_c(rc, "\0", "\0");
    (void)MemoryContextSwitchTo(oldcxt);

    return planMgr;
}

/*
 * PMGR_ReleasePlanManager: release a plan manager, if any.
 */
void
PMGR_ReleasePlanManager(CachedPlanSource *plansource)
{
    ListCell *lc;
    PlanManager *manager = plansource->planManager;
    Assert(ENABLE_CACHEDPLAN_MGR);

    if (manager == NULL) {
        return;
    }

    DropStmtRoot(plansource->planManager->psrc_key);
    ReleaseGenericPlan(plansource);

    /* clear all candiate plans. */
    foreach (lc, manager->candidatePlans) {
        CachedPlan *plan = (CachedPlan *)lfirst(lc);
        Assert(plan->is_candidate);

        /* release the plan */
        ReleaseCachedPlan(plan, false);
    }
    /* release the whole plan manager */
    MemoryContextDelete(manager->context);
    plansource->planManager = NULL;
}

/*
 * GetBaseRelSelectivity: extract selectivity info from a given PlnnerInfo.
 * Caller must have already called set_base_rel_sizes to collect the selectivity
 * of each base rel.
 */
static List*
GetBaseRelSelectivity(PlannerInfo *root)
{
    Assert(root->simple_rel_array != NULL);

    List *feature = NIL;
    for (int i = 1; i < root->simple_rel_array_size; i++) {
        RelOptInfo *rel = root->simple_rel_array[i];
        if (rel == NULL || rel->rtekind != RTE_RELATION) {
            continue;
        }
        RelSelec *rsel = (RelSelec *)palloc0(sizeof(RelSelec));
        rsel->relid = rel->relid;
        rsel->selectivity = rel->rows / rel->tuples;
        feature = lappend(feature, rsel);
    }

    return feature;
}

/*
 * GenerateRelCIs: convert each element of baserels_selec to a RelCI structure.
 */
static List*
GenerateRelCIs(List *selectivities)
{
    List *result = NIL;

    /* convert each element of baserels_selec into a RelCI. */
    ListCell *cell;
    foreach (cell, selectivities) {
        RelSelec *rselec = (RelSelec *)lfirst(cell);
        RelCI *relCI = makeNode(RelCI);
        relCI->relid = rselec->relid;

        /*
         * create a CondInterval for relCI and init its lowerbound and upperbound
         * by current rel's selectivity.
         */
        relCI->ci.relid = rselec->relid;

        /* expand CI by 10% */
        relCI->ci.lowerbound = rselec->selectivity;
        relCI->ci.upperbound = rselec->selectivity;
        relCI->init_selec = rselec->selectivity;

        result = lappend(result, relCI);
    }
    return result;
}

/*
 * CacheIndexQual - copy indexqual from plan and replace the var number of index
 * scan nodes by real scan IDs.
 * 
 * i.e., varno: 65000 => varno: 1
 */
static Node*
CacheIndexQual(Node *node, indexUsageWalkerCxt *context)
{
    Var *newVal = NULL;

    if (node == NULL) {
        return NULL;
    }
    Assert(!IsA(node, Plan) && !IsA(node, RestrictInfo));

    if (IsA(node, Var)) {
        Var *newVal = (Var *)copyObject(node);
        newVal->varno = newVal->varnoold;
        newVal->varattno = newVal->varoattno;

        return (Node *)newVal;
    }

    if (IsA(node, Param)) {
        Param *param = (Param *)node;
        if (param->paramkind == PARAM_EXEC) {
            ListCell *lc;
            foreach (lc, context->paramValList) {
                NestLoopParam *nlp = (NestLoopParam *)lfirst(lc);
                if (param->paramid == nlp->paramno) {
                    newVal = (Var *)copyObject(nlp->paramval);
                    newVal->varno = newVal->varnoold;
                    newVal->varattno = newVal->varoattno;
                    return (Node *)newVal;
                }
            }
        }
    }
    return expression_tree_mutator(node,
                                  (Node* (*)(Node*, void*))CacheIndexQual,
                                  context, true);
}

PlanIndexUasge*
MakePlanIndexUasge(int scanrelid,
                                 Oid indexoid,
                                 List *quals,
                                 bool is_partial,
                                 double s)
{
    PlanIndexUasge *rstInfo =
        (PlanIndexUasge *)palloc0(sizeof(PlanIndexUasge));
    rstInfo->scanrelid = scanrelid;
    rstInfo->indexoid = indexoid;
    rstInfo->indexquals = quals;
    rstInfo->is_partial = is_partial;
    rstInfo ->selec = s;

    return rstInfo;
}

/*
 * ExtractPlanIndexesUsages: traverse plan tree and collect the index usage
 * information.
 *
 * The collected info are organized into a list (context->usage_list), and
 * each element is a PlanIndexUasge corresponding an index/indexonly/bitmapindex
 * scan.
 *
 * Returns false when the current plan node are processed end.
 */
static bool
ExtractPlanIndexesUsages(Node *plan, void *context)
{
    indexUsageWalkerCxt *idxCxt = ((indexUsageWalkerCxt *)context);

    if (plan == NULL || IsA(plan, SubPlan)) {
        return false;
    }

    if (IsA(plan, IndexScan)) {
        IndexScan *idxScan = (IndexScan *)plan;

        PlanIndexUasge *index = MakePlanIndexUasge(idxScan->scan.scanrelid,
                                                   idxScan->indexid,
                                                   idxScan->indexqualorig,
                                                   idxScan->is_partial,
                                                   idxScan->selectivity);

        idxCxt->usage_list = lappend(idxCxt->usage_list, index);

        return false;
    }
    if (IsA(plan, IndexOnlyScan)) {
        IndexOnlyScan *idxOnlyScan = (IndexOnlyScan *)plan;

        PlanIndexUasge *index = MakePlanIndexUasge(idxOnlyScan->scan.scanrelid,
                                                   idxOnlyScan->indexid,
                                                   idxOnlyScan->indexqualorig,
                                                   idxOnlyScan->is_partial,
                                                   idxOnlyScan->selectivity);
        idxCxt->usage_list = lappend(idxCxt->usage_list, index);

        return false;
    }

    if (IsA(plan, BitmapIndexScan)) {
        BitmapIndexScan *btIdxScan = (BitmapIndexScan *)plan;

        PlanIndexUasge *index = MakePlanIndexUasge(btIdxScan->scan.scanrelid,
                                                   btIdxScan->indexid,
                                                   btIdxScan->indexqualorig,
                                                   btIdxScan->is_partial,
                                                   btIdxScan->selectivity);

        idxCxt->usage_list = lappend(idxCxt->usage_list, index);

        return false;
    }

    if (IsA(plan, NestLoop)) {
        NestLoop *nl = (NestLoop *)plan;
        List *tmpList = (List *)copyObject(nl->nestParams);
        idxCxt->paramValList = list_concat(idxCxt->paramValList, tmpList);
    }

    return plan_tree_walker(plan, (MethodWalker)ExtractPlanIndexesUsages, context);
}

/*
 * GenerateIndexCIs: get indexquals and their selectivities from the plan
 * tree and generate index CI.
 */
List*
GenerateIndexCIs(indexUsageWalkerCxt context, bool *hasPartial)
{
    List *result = NIL;
    bool partial = false;

    ListCell *lc;
    foreach (lc, context.usage_list) {
        PlanIndexUasge *usage = (PlanIndexUasge *)lfirst(lc);

        IndexCI *idxci = makeNode(IndexCI);
        idxci->ci.relid = usage->scanrelid;
        idxci->ci.lowerbound = usage->selec;
        idxci->ci.upperbound = usage->selec;
        idxci->init_selec = usage->selec;
        idxci->indexoid = usage->indexoid;

        /*
         * replace pseudo-VarNo (i.e., 65002) by the real rel id. Then,
         * generate the hashkey of reset-quals as the index key.
         */
        context.varelid = usage->scanrelid;
        idxci->indexquals = (List *)CacheIndexQual((Node *)usage->indexquals, &context);
        idxci->is_partial = usage->is_partial;
        partial = usage->is_partial ? usage->is_partial : partial;
        result = lappend(result, idxci);
    }

    if(hasPartial != NULL){
        (*hasPartial) = partial;
    }

    return result;
}

/*
 * InitStmtRootHashTable: initializing a hash table to store GenericRoot.
 */
static void
InitStmtRootHashTable(void)
{
    HASHCTL hash_ctl;

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(PlannerInfo);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;

    u_sess->pcache_cxt.generic_roots = hash_create("Generic Roots",
                                                   32,
                                                   &hash_ctl,
                                                   HASH_ELEM | HASH_CONTEXT);
}

/*
 * StoreStmtRoot: Store a GenericRoot in the hash table using the specified key.
 */
static void
StoreStmtRoot(const char *key,
              PlannerInfo *groot,
              const char *qstr,
              MemoryContext cxt)
{
    StatementRoot *entry;
    bool found;

    Assert(qstr != NULL);
    /* Initialize the hash table, if necessary */
    if (u_sess->pcache_cxt.generic_roots == NULL) {
        InitStmtRootHashTable();
    }
    /* Add entry to hash table */
    entry = (StatementRoot *)hash_search(u_sess->pcache_cxt.generic_roots,
                                         key,
                                         HASH_ENTER,
                                         &found);

    /* Shouldn't get a duplicate entry */
    if (found) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("generic root \"%s\" already exists.", key)));
        return;
    }
    entry->generic_root = groot;
    entry->query_string = qstr;
    entry->context = cxt;
    MemoryContextSetParent(entry->context, u_sess->cache_mem_cxt);
}

/*
 * FetchStmtRoot: fetch a StatementRoot by the specified key.
 */
static StatementRoot*
FetchStmtRoot(const char *key)
{
    StatementRoot *entry;

    if (u_sess->pcache_cxt.generic_roots != NULL) {
        entry = (StatementRoot *)hash_search(u_sess->pcache_cxt.generic_roots,
                                             key,
                                             HASH_FIND,
                                             NULL);
    } else {
        entry = NULL;
        ereport(DEBUG2, (errcode(MOD_OPT),
                errmsg("generic root is not exist \"%s\"", key)));

    }
    return entry;
}

/*
 * DropStmtRoot: drop a StatementRoot entry from hashtab and release it.
 */
void
DropStmtRoot(const char *stmt_name)
{
    StatementRoot *entry;

    /* Find the root's hash table entry. */
    entry = FetchStmtRoot(stmt_name);

    if (entry != NULL) {
        /* Release stmtRoot entry */
        MemoryContextDelete(entry->context);

        /* Now we can remove the hash table entry */
        hash_search(u_sess->pcache_cxt.generic_roots,
                    entry->stmt_name,
                    HASH_REMOVE,
                    NULL);
    }
}

/*
 * DropAllStmtRoot: drop all cached StatementRoot.
 */
void
DropAllStmtRoot(void)
{
    HASH_SEQ_STATUS seq;
    StatementRoot *entry;

    /* nothing cached */
    if (u_sess->pcache_cxt.generic_roots == NULL)
        return;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->pcache_cxt.generic_roots);
    while ((entry = (StatementRoot *)hash_seq_search(&seq)) != NULL) {
        /* Release the plancache entry */
        MemoryContextDelete(entry->context);

        /* Now we can remove the hash table entry */
        hash_search(u_sess->pcache_cxt.generic_roots,
                    entry->stmt_name,
                    HASH_REMOVE,
                    NULL);
    }
}

/*
 * ClearGenericRootCache: clear the sketches on GenericRoot for next execution.
 */
static void
ClearGenericRootCache(PlannerInfo *pinfo, bool onlyPossionCache)
{
    ListCell *lc;

    for (int i = 1; i < pinfo->simple_rel_array_size; i++) {
        RelOptInfo *rel = pinfo->simple_rel_array[i];
        if (rel == NULL) {
            continue;
        }
        rel->rows = 0;
        if (rel->varratio != NIL) {
            list_free_deep(rel->varratio);
            rel->varratio = NIL;
        }
        if (rel->varEqRatio != NIL) {
            list_free_deep(rel->varEqRatio);
            rel->varEqRatio = NIL;
        }

        if(!onlyPossionCache){
            lc = NULL;
            foreach (lc, rel->indexlist) {
                IndexOptInfo *idxinfo = (IndexOptInfo *)lfirst(lc);
                idxinfo->predOK = false;
                if (idxinfo->rel != NULL && idxinfo->rel != rel) {
                    idxinfo->rel = rel;
                }
            }

            foreach (lc, rel->baserestrictinfo) {
                RestrictInfo *rsinfo = (RestrictInfo *)lfirst(lc);
                rsinfo->norm_selec = -1;
            }
        }

        if (rel->subroot != NULL) {
            ClearGenericRootCache(rel->subroot, onlyPossionCache);
        }
    }
}

/*
 * InitGenericRoot: set param values and clear the sketches on GenericRoot.
 */
static PlannerInfo*
InitGenericRoot(PlannerInfo *pinfo, ParamListInfo boundParams)
{
    if (nodeTag(pinfo) != T_PlannerInfo) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("ReSetGenericRoot: please input a valid PlannerInfo.")));
    }
    pinfo->glob->boundParams = boundParams;
    ClearGenericRootCache(pinfo, false);

    return pinfo;
}

/*
 * eval_const_clauses_params: replace the params in given clauses by query
 * param values.
 *
 * The return list is a copy of origin, and a bind only success if the params
 * of 'groot' has been set.
 */
List*
eval_const_clauses_params(PlannerInfo *groot, List *clauses)
{
    ListCell *cell;
    Node *clause = NULL;
    List *eval_clauses = NIL;

    ParamListInfo boundParams = groot->glob->boundParams;

    if (!ENABLE_CACHEDPLAN_MGR ||
        boundParams == NULL ||
        boundParams->uParamInfo == DEFUALT_INFO ||
        boundParams->params_lazy_bind){
        return clauses;
    }

    foreach (cell, clauses) {
        Node* node = (Node *)lfirst(cell);
        if(IsA(node, RestrictInfo)){
            RestrictInfo *rinfo = (RestrictInfo *)lfirst(cell);
            clause = (Node *)rinfo->clause;
        } else {
            clause = node;
        }
        clause = (Node *)eval_const_expressions_params(groot,
                                                       clause,
                                                       boundParams);
        eval_clauses = lappend(eval_clauses, clause);
    }
    return eval_clauses;
}
