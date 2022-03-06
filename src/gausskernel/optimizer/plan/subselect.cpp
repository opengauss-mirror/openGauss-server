/* -------------------------------------------------------------------------
 *
 * subselect.cpp
 *	  Planning routines for subselects and parameters.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/subselect.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/randomplan.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_relation.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "parser/parse_hint.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif
#include "parser/parse_agg.h"

extern void set_path_seed_factor_zero();

typedef struct convert_testexpr_context {
    PlannerInfo* root;
    List* subst_nodes; /* Nodes to substitute for Params */
} convert_testexpr_context;

typedef struct process_sublinks_context {
    PlannerInfo* root;
    bool isTopQual;
} process_sublinks_context;

typedef struct finalize_primnode_context {
    PlannerInfo* root;
    Bitmapset* paramids; /* Non-local PARAM_EXEC paramids found */
} finalize_primnode_context;

typedef struct varWalkerContext {
    int maxLevelsUp; /* Keep level up of var */
} varWalkerContext;

typedef struct pull_node_clause
{
    List    *nodeList;
    List    *nameList;
    char    *name;
    bool     recurse;
    int      flag;
} pull_node_clause;

typedef struct push_qual_context {
    int varno;       /* Var no. */
    List* qual_list; /* Find qual list. */
} push_qual_context;

/* flags bits for pull_expr_walker and pull expr_mutator */
#define PE_OPEXPR   0x01        /* pull expr of op expr */
#define PE_NULLTEST 0x02        /* pull expr of null test */
#define PE_NOTCLAUSE    0x04        /* pull expr of not clause */


static Node* build_subplan(PlannerInfo* root, Plan* plan, PlannerInfo* subroot, List* plan_params,
    SubLinkType subLinkType, Node* testexpr, bool adjust_testexpr, bool unknownEqFalse);
static List* generate_subquery_params(PlannerInfo* root, List* tlist, List** paramIds);
static List* generate_subquery_vars(PlannerInfo* root, List* tlist, Index varno);
static Node* convert_testexpr(PlannerInfo* root, Node* testexpr, List* subst_nodes);
static Node* convert_testexpr_mutator(Node* node, convert_testexpr_context* context);
static bool subplan_is_hashable(Plan* plan);
static bool testexpr_is_hashable(Node* testexpr);
static bool hash_ok_operator(OpExpr* expr);
static bool simplify_EXISTS_query(Query* query);
static Query* convert_EXISTS_to_ANY(PlannerInfo* root, Query* subselect, Node** testexpr, List** paramIds);
static Node* replace_correlation_vars_mutator(Node* node, PlannerInfo* root);
static Node* process_sublinks_mutator(Node* node, process_sublinks_context* context);
static Bitmapset* finalize_plan(PlannerInfo* root, Plan* plan, Bitmapset* valid_params, Bitmapset* scan_params);
static bool finalize_primnode(Node* node, finalize_primnode_context* context);
static Node* convert_joinqual_to_antiqual(Node* node, Query* parse);
static Node* convert_opexpr_to_boolexpr_for_antijoin(Node* node, Query* parse);
static bool get_equal_operates(OpExpr* qual, List** pullUpQual, bool paramAllowed);
static bool equal_expr(Node* node);
static bool has_correlation_in_rte(List* rtable);
static bool safe_convert_EXPR(PlannerInfo *root, Node *clause, SubLink* sublink, Relids available_rels, bool in_or_clause = false);
static void append_target_and_group(PlannerInfo *root, Query* subQuery, Node* node);
static bool get_pullUp_equal_expr(Node* node, List** pullUpQual, bool paramAllowed = true);
static bool pull_sublink_clause_walker(Node* node, pull_node_clause* context);
static bool safe_convert_EXISTS(PlannerInfo *root, SubLink* sublink, Relids available_rels);
static bool safe_convert_ANY(PlannerInfo *root, SubLink* sublink, Relids available_rels);
static bool safe_convert_ORCLAUSE(PlannerInfo *root, Node *clause, SubLink* sublink, Relids available_rels);
static bool finalize_agg_primnode(Node* node, finalize_primnode_context* context);
static Var* locate_base_var(Expr* node);
static void SS_process_one_cte(PlannerInfo* root, CommonTableExpr* cte, Query* subquery);
static Node *generate_filter_on_opexpr_sublink(PlannerInfo *root, int rtIndex,
                                            Node *targetExpr, Query *subQuery);
static Node*transform_equal_expr(PlannerInfo *root, Query *subQuery,
                    List *pullUpEqualExpr,  Node **quals, bool isLimit, bool isnull = false);
static bool all_replicate_table(Query *query);
static Node* convert_expr_subink_with_agg_targetlist(PlannerInfo *root,
                                                                Node **jtlink1,
                                                                Node *inout_quals,
                                                                SubLink *sublink,
                                                                Relids *available_rels,
                                                                Node *all_quals,
                                                                const char *refname);
static Node* convert_expr_sublink_with_limit_clause(PlannerInfo *root,
                                    Node ** currJoinLink,
                                    SubLink *sublink,
                                    Node *exprQual,
                                    Relids *available_rels,
                                    Node *all_quals, const char *refname);
static bool CanExprHashable(List *pullUpEqualExpr);

static bool contain_dml_walker(Node *node, void *context);
static bool recursive_reference_recursive_walker(Node* node);
static bool contain_outer_selfref_walker(Node *node, Index *depth);

#define PLANNAMELEN 64

static char *denominate_sublink_name(int sublink_counter)
{
    int nRet = 0;
    char subname[SUBLINK_COUNTER];

    nRet = sprintf_s(subname, sizeof(subname), "sublink_%d", sublink_counter);
    securec_check_ss_c(nRet, "\0", "\0");

    int newlen = strlen(subname) + 1;
    char *subquery_name = (char *)palloc0(newlen);
    nRet = strncpy_s(subquery_name, newlen, subname, strlen(subname));
    securec_check_ss_c(nRet, "\0", "\0");

    return subquery_name;
}

static bool
all_replicate_table(Query *query)
{
    Relids          varnos = get_relids_in_jointree((Node *) query->jointree, false);
    int         attnum = 0;

    while ((attnum = bms_first_member(varnos)) >= 0) {
        RangeTblEntry *r_table = (RangeTblEntry*)rt_fetch(attnum, query->rtable);
        if (r_table->rtekind == RTE_RELATION) {
            if (GetLocatorType(r_table->relid) == LOCATOR_TYPE_REPLICATED)
                continue;
        }

        return false;
    }

    return true;
}

/*
 * Select a PARAM_EXEC number to identify the given Var as a parameter for
 * the current subquery, or for a nestloop's inner scan.
 * If the Var already has a param in the current context, return that one.
 */
static int assign_param_for_var(PlannerInfo* root, Var* var)
{
    ListCell* ppl = NULL;
    PlannerParamItem* pitem = NULL;
    Index levelsup;

    /* Find the query level the Var belongs to */
    for (levelsup = var->varlevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    /* If there's already a matching PlannerParamItem there, just use it */
    foreach (ppl, root->plan_params) {
        pitem = (PlannerParamItem*)lfirst(ppl);
        if (IsA(pitem->item, Var)) {
            Var* pvar = (Var*)pitem->item;

            /*
             * This comparison must match _equalVar(), except for ignoring
             * varlevelsup.  Note that _equalVar() ignores the location.
             */
            if (pvar->varno == var->varno && pvar->varattno == var->varattno && pvar->vartype == var->vartype &&
                pvar->vartypmod == var->vartypmod && pvar->varcollid == var->varcollid &&
                pvar->varnoold == var->varnoold && pvar->varoattno == var->varoattno)
                return pitem->paramId;
        }
    }

    /* Nope, so make a new one */
    var = (Var*)copyObject(var);
    var->varlevelsup = 0;

    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node*)var;
    pitem->paramId = root->glob->nParamExec++;

    root->plan_params = lappend(root->plan_params, pitem);

    return pitem->paramId;
}

static Param* initParam(Var* var, int i)
{
    Param* retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = i;
    retval->paramtype = var->vartype;
    retval->paramtypmod = var->vartypmod;
    retval->paramcollid = var->varcollid;
    retval->location = var->location;
    return retval;
}

/*
 * Generate a Param node to replace the given Var,
 * which is expected to have varlevelsup > 0 (ie, it is not local).
 */
static Param* replace_outer_var(PlannerInfo* root, Var* var)
{
    int i;

    AssertEreport(var->varlevelsup > 0 && var->varlevelsup < root->query_level,
        MOD_OPT_SUBPLAN,
        "Replaced var should be within valid range");

    /* Find the Var in the appropriate plan_params, or add it if not present */
    i = assign_param_for_var(root, var);

    return initParam(var, i);
}

/*
 * Generate a Param node to replace the given Var, which will be supplied
 * from an upper NestLoop join node.
 *
 * This is effectively the same as replace_outer_var, except that we expect
 * the Var to be local to the current query level.
 */
Param* assign_nestloop_param_var(PlannerInfo* root, Var* var)
{
    int i;

    AssertEreport(var->varlevelsup == 0, MOD_OPT_SUBPLAN, "Replaced var should be at current level");

    i = assign_param_for_var(root, var);

    return initParam(var, i);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get PartIterator ParamId from PlannerInfo
 * Description	:
 * Input		:
 * Output		:
 * Notes		:
 */
int assignPartIteratorParam(PlannerInfo* root)
{
    PlannerParamItem* pitem = NULL;
    Node* itrParam = NULL; /* just for specifying value to PlannerParamItem.item, represent nothing. */

    itrParam = (Node*)palloc(sizeof(Node));
    pitem = makeNode(PlannerParamItem);

    pitem->item = (Node*)itrParam;
    pitem->paramId = SS_assign_special_param(root);
    root->plan_params = lappend(root->plan_params, pitem);

    return pitem->paramId;
}

/*
 * Select a PARAM_EXEC number to identify the given PlaceHolderVar as a
 * parameter for the current subquery, or for a nestloop's inner scan.
 * If the PHV already has a param in the current context, return that one.
 *
 * This is just like assign_param_for_var, except for PlaceHolderVars.
 */
static int assign_param_for_placeholdervar(PlannerInfo* root, PlaceHolderVar* phv)
{
    ListCell* ppl = NULL;
    PlannerParamItem* pitem = NULL;
    Index levelsup;

    /* Find the query level the PHV belongs to */
    for (levelsup = phv->phlevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    /* If there's already a matching PlannerParamItem there, just use it */
    foreach (ppl, root->plan_params) {
        pitem = (PlannerParamItem*)lfirst(ppl);
        if (IsA(pitem->item, PlaceHolderVar)) {
            PlaceHolderVar* pphv = (PlaceHolderVar*)pitem->item;

            /* We assume comparing the PHIDs is sufficient */
            if (pphv->phid == phv->phid)
                return pitem->paramId;
        }
    }

    /* Nope, so make a new one */
    phv = (PlaceHolderVar*)copyObject(phv);
    if (phv->phlevelsup != 0) {
        IncrementVarSublevelsUp((Node*)phv, -((int)phv->phlevelsup), 0);
        AssertEreport(phv->phlevelsup == 0, MOD_OPT_SUBPLAN, "Placeholder var should be at current level");
    }

    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node*)phv;
    pitem->paramId = root->glob->nParamExec++;

    root->plan_params = lappend(root->plan_params, pitem);

    return pitem->paramId;
}

static Param* getParamPHV(PlaceHolderVar* phv, int i)
{
    Param* retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = i;
    retval->paramtype = exprType((Node*)phv->phexpr);
    retval->paramtypmod = exprTypmod((Node*)phv->phexpr);
    retval->paramcollid = exprCollation((Node*)phv->phexpr);
    retval->location = -1;
    return retval;
}

/*
 * Generate a Param node to replace the given PlaceHolderVar,
 * which is expected to have phlevelsup > 0 (ie, it is not local).
 *
 * This is just like replace_outer_var, except for PlaceHolderVars.
 */
static Param* replace_outer_placeholdervar(PlannerInfo* root, PlaceHolderVar* phv)
{
    int i;

    AssertEreport(phv->phlevelsup > 0 && phv->phlevelsup < root->query_level,
        MOD_OPT_SUBPLAN,
        "Placeholder var should be within valid range");

    /* Find the PHV in the appropriate plan_params, or add it if not present */
    i = assign_param_for_placeholdervar(root, phv);

    return getParamPHV(phv, i);
}

/*
 * Generate a Param node to replace the given PlaceHolderVar, which will be
 * supplied from an upper NestLoop join node.
 *
 * This is just like assign_nestloop_param_var, except for PlaceHolderVars.
 */
Param* assign_nestloop_param_placeholdervar(PlannerInfo* root, PlaceHolderVar* phv)
{
    int i;

    AssertEreport(phv->phlevelsup == 0, MOD_OPT_SUBPLAN, "Placeholder var should be at current level");

    i = assign_param_for_placeholdervar(root, phv);

    return getParamPHV(phv, i);
}

/*
 * Generate a Param node to replace the given Aggref
 * which is expected to have agglevelsup > 0 (ie, it is not local).
 */
static Param* replace_outer_agg(PlannerInfo* root, Aggref* agg)
{
    Param* retval = NULL;
    PlannerParamItem* pitem = NULL;
    Index levelsup;

    AssertEreport(agg->agglevelsup > 0 && agg->agglevelsup < root->query_level,
        MOD_OPT_SUBPLAN,
        "Agg expr should be within valid range");

    /* Find the query level the Aggref belongs to */
    for (levelsup = agg->agglevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    /*
     * It does not seem worthwhile to try to match duplicate outer aggs. Just
     * make a new slot every time.
     */
    agg = (Aggref*)copyObject(agg);
    IncrementVarSublevelsUp((Node*)agg, -((int)agg->agglevelsup), 0);
    AssertEreport(agg->agglevelsup == 0, MOD_OPT_SUBPLAN, "Agg expr should be at current level");

    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node*)agg;
    pitem->paramId = root->glob->nParamExec++;

    root->plan_params = lappend(root->plan_params, pitem);

    retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = pitem->paramId;
    retval->paramtype = agg->aggtype;
    retval->paramtypmod = -1;
    retval->paramcollid = agg->aggcollid;
    retval->location = agg->location;

    return retval;
}

/*
 * Generate a Param node to replace the given GroupingFunc expression which is
 * expected to have agglevelsup > 0 (ie, it is not local).
 */
static Param* replace_outer_grouping(PlannerInfo* root, GroupingFunc* grp)
{
    Param* retval = NULL;
    PlannerParamItem* pitem = NULL;
    Index levelsup;

    AssertEreport(grp->agglevelsup > 0 && grp->agglevelsup < root->query_level,
        MOD_OPT_SUBPLAN,
        "Grouping expr should be within valid range");

    /* Find the query level the GroupingFunc belongs to */
    for (levelsup = grp->agglevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    /*
     * It does not seem worthwhile to try to match duplicate outer aggs. Just
     * make a new slot every time.
     */
    grp = (GroupingFunc*)copyObject(grp);
    IncrementVarSublevelsUp((Node*)grp, -((int)grp->agglevelsup), 0);
    AssertEreport(grp->agglevelsup == 0, MOD_OPT_SUBPLAN, "Grouping expr should be at current level");

    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node*)grp;
    pitem->paramId = root->glob->nParamExec++;

    root->plan_params = lappend(root->plan_params, pitem);

    retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = pitem->paramId;
    retval->paramtype = exprType((Node*)grp);
    retval->paramtypmod = -1;
    retval->paramcollid = InvalidOid;
    retval->location = grp->location;

    return retval;
}

/*
 * Generate a new Param node that will not conflict with any other.
 *
 * This is used to create Params representing subplan outputs.
 * We don't need to build a PlannerParamItem for such a Param, but we do
 * need to record the PARAM_EXEC slot number as being allocated.
 */
static Param* generate_new_param(PlannerInfo* root, Oid paramtype, int32 paramtypmod, Oid paramcollation)
{
    Param* retval = NULL;

    retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = root->glob->nParamExec++;
    retval->paramtype = paramtype;
    retval->paramtypmod = paramtypmod;
    retval->paramcollid = paramcollation;
    retval->location = -1;

    return retval;
}

/*
 * Assign a (nonnegative) PARAM_EXEC ID for a special parameter (one that
 * is not actually used to carry a value at runtime).  Such parameters are
 * used for special runtime signaling purposes, such as connecting a
 * recursive union node to its worktable scan node or forcing plan
 * re-evaluation within the EvalPlanQual mechanism.  No actual Param node
 * exists with this ID, however.
 */
int SS_assign_special_param(PlannerInfo* root)
{
    return root->glob->nParamExec++;
}

/*
 * Get the datatype/typmod/collation of the first column of the plan's output.
 *
 * This information is stored for ARRAY_SUBLINK execution and for
 * exprType()/exprTypmod()/exprCollation(), which have no way to get at the
 * plan associated with a SubPlan node.  We really only need the info for
 * EXPR_SUBLINK and ARRAY_SUBLINK subplans, but for consistency we save it
 * always.
 */
static void get_first_col_type(Plan* plan, Oid* coltype, int32* coltypmod, Oid* colcollation)
{
    /* In cases such as EXISTS, tlist might be empty; arbitrarily use VOID */
    if (plan->targetlist) {
        TargetEntry* tent = (TargetEntry*)linitial(plan->targetlist);

        AssertEreport(IsA(tent, TargetEntry), MOD_OPT_SUBPLAN, "Element of targetlist should be TargetEntry");
        if (!tent->resjunk) {
            *coltype = exprType((Node*)tent->expr);
            *coltypmod = exprTypmod((Node*)tent->expr);
            *colcollation = exprCollation((Node*)tent->expr);
            return;
        }
    }
    *coltype = VOIDOID;
    *coltypmod = -1;
    *colcollation = InvalidOid;
}

/*
 * Description: subquery is initplan or subplan.
 * Parameters:
 * @in plan_params: param list.
 * @in subLinkType: sublink type.
 * Return: true if subquery is initplan.
 */
static bool IsInitPlan(List* plan_params, SubLinkType subLinkType)
{
    if (plan_params != NIL) {
        return false;
    }

    if (subLinkType == EXISTS_SUBLINK || subLinkType == EXPR_SUBLINK || subLinkType == ARRAY_SUBLINK ||
        subLinkType == ROWCOMPARE_SUBLINK) {
        return true;
    }

    return false;
}

/*
 * Convert a SubLink (as created by the parser) into a SubPlan.
 *
 * We are given the SubLink's contained query, type, and testexpr.  We are
 * also told if this expression appears at top level of a WHERE/HAVING qual.
 *
 * Note: we assume that the testexpr has been AND/OR flattened (actually,
 * it's been through eval_const_expressions), but not converted to
 * implicit-AND form; and any SubLinks in it should already have been
 * converted to SubPlans.  The subquery is as yet untouched, however.
 *
 * The result is whatever we need to substitute in place of the SubLink
 * node in the executable expression.  This will be either the SubPlan
 * node (if we have to do the subplan as a subplan), or a Param node
 * representing the result of an InitPlan, or a row comparison expression
 * tree containing InitPlan Param nodes.
 */
static Node* make_subplan(
    PlannerInfo* root, Query* orig_subquery, SubLinkType subLinkType, Node* testexpr, bool isTopQual)
{
    Query* subquery = NULL;
    bool simple_exists = false;
    double tuple_fraction;
    Plan* plan = NULL;
    PlannerInfo* subroot = NULL;
    List* plan_params = NIL;
    Node* result = NULL;
    bool tmp_need_autoanalyze = false;

    /*
     * Copy the source Query node.	This is a quick and dirty kluge to resolve
     * the fact that the parser can generate trees with multiple links to the
     * same sub-Query node, but the planner wants to scribble on the Query.
     * Try to clean this up when we do querytree redesign...
     */
    subquery = (Query*)copyObject(orig_subquery);

    /*
     * If it's an EXISTS subplan, we might be able to simplify it.
     */
    if (subLinkType == EXISTS_SUBLINK)
        simple_exists = simplify_EXISTS_query(subquery);

    /*
     * For an EXISTS subplan, tell lower-level planner to expect that only the
     * first tuple will be retrieved.  For ALL and ANY subplans, we will be
     * able to stop evaluating if the test condition fails or matches, so very
     * often not all the tuples will be retrieved; for lack of a better idea,
     * specify 50% retrieval.  For EXPR and ROWCOMPARE subplans, use default
     * behavior (we're only expecting one row out, anyway).
     *
     * NOTE: if you change these numbers, also change cost_subplan() in
     * path/costsize.c.
     *
     * XXX If an ANY subplan is uncorrelated, build_subplan may decide to hash
     * its output.	In that case it would've been better to specify full
     * retrieval.  At present, however, we can only check hashability after
     * we've made the subplan :-(.  (Determining whether it'll fit in work_mem
     * is the really hard part.)  Therefore, we don't want to be too
     * optimistic about the percentage of tuples retrieved, for fear of
     * selecting a plan that's bad for the materialization case.
     */
    if (subLinkType == EXISTS_SUBLINK)
        tuple_fraction = 1.0; /* just like a LIMIT 1 */
    else if (subLinkType == ALL_SUBLINK || subLinkType == ANY_SUBLINK)
        tuple_fraction = 0.5; /* 50% */
    else
        tuple_fraction = 0.0; /* default behavior */

    /* plan_params should not be in use in current query level */
    AssertEreport(root->plan_params == NIL, MOD_OPT_SUBPLAN, "Plan_params should not be in use in current query level");

    /*
     * Generate the plan for the subquery.
     */
    /* Set u_sess->opt_cxt.query_dop to forbidden the parallel of subquery. */
    int outerDop = u_sess->opt_cxt.query_dop;

    if (isIntergratedMachine == false) {
        u_sess->opt_cxt.query_dop = 1;
    } else {
        bool isInitPlan = IsInitPlan(root->plan_params, subLinkType);
        if (isInitPlan == false) {
            u_sess->opt_cxt.query_dop = 1;
        }
    }
    int dopTmp = u_sess->opt_cxt.query_dop;

    /* reset u_sess->analyze_cxt.need_autoanalyze */
    tmp_need_autoanalyze = u_sess->analyze_cxt.need_autoanalyze;
    u_sess->analyze_cxt.need_autoanalyze = false;
    plan = subquery_planner(root->glob, subquery, root, false, tuple_fraction, &subroot, (SUBQUERY_SUBLINK | SUBQUERY_NORMAL));
    u_sess->analyze_cxt.need_autoanalyze = tmp_need_autoanalyze;

    /* Reset u_sess->opt_cxt.query_dop. */
    u_sess->opt_cxt.query_dop = outerDop;
    /* Isolate the params needed by this specific subplan */
    plan_params = root->plan_params;
    root->plan_params = NIL;

    /* And convert to SubPlan or InitPlan format. */
    result = build_subplan(root, plan, subroot, plan_params, subLinkType, testexpr, true, isTopQual);
#ifdef PGXC
    /* This is not necessary for a PGXC Coordinator, we just need one plan */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        return result;
#endif

    /*
     * If it's a correlated EXISTS with an unimportant targetlist, we might be
     * able to transform it to the equivalent of an IN and then implement it
     * by hashing.	We don't have enough information yet to tell which way is
     * likely to be better (it depends on the expected number of executions of
     * the EXISTS qual, and we are much too early in planning the outer query
     * to be able to guess that).  So we generate both plans, if possible, and
     * leave it to the executor to decide which to use.
     */
    if (simple_exists && IsA(result, SubPlan)) {
        Node* newtestexpr = NULL;
        List* paramIds = NIL;

        /* Make a second copy of the original subquery */
        subquery = (Query*)copyObject(orig_subquery);
        /* and re-simplify */
        simple_exists = simplify_EXISTS_query(subquery);
        if (!simple_exists)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Build subPlan failed.. "))));

        /* See if it can be converted to an ANY query */
        subquery = convert_EXISTS_to_ANY(root, subquery, &newtestexpr, &paramIds);
        if (subquery != NULL) {
            u_sess->opt_cxt.query_dop = dopTmp;
            /* Generate the plan for the ANY subquery; we'll need all rows */
            plan = subquery_planner(root->glob, subquery, root, false, 0.0, &subroot, (SUBQUERY_SUBLINK | SUBQUERY_NORMAL));
            u_sess->opt_cxt.query_dop = outerDop;

            /* Isolate the params needed by this specific subplan */
            plan_params = root->plan_params;
            root->plan_params = NIL;

            /* Now we can check if it'll fit in work_mem */
            if (plan != NULL && subplan_is_hashable(plan)) {
                SubPlan* hashplan = NULL;
                AlternativeSubPlan* asplan = NULL;

                /* OK, convert to SubPlan format. */
                hashplan =
                    (SubPlan*)build_subplan(root, plan, subroot, plan_params, ANY_SUBLINK, newtestexpr, false, true);
                if (NULL == hashplan)
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            (errmsg("convert to SubPlan failed. "))));
                /* Check we got what we expected */
                AssertEreport(IsA(hashplan, SubPlan), MOD_OPT_SUBPLAN, "Subplan must be hashplan");
                AssertEreport(hashplan->parParam == NIL, MOD_OPT_SUBPLAN, "Param of hashplan should be null");
                AssertEreport(hashplan->useHashTable, MOD_OPT_SUBPLAN, "Hashtable of hashplan should be used");

                /* build_subplan won't have filled in paramIds */
                hashplan->paramIds = paramIds;

                /* Leave it to the executor to decide which plan to use */
                asplan = makeNode(AlternativeSubPlan);
                asplan->subplans = list_make2(result, hashplan);
                result = (Node*)asplan;
            }
        }
    }

    return result;
}

/*
 * Build a SubPlan node given the raw inputs --- subroutine for make_subplan
 *
 * Returns either the SubPlan, or an expression using initplan output Params,
 * as explained in the comments for make_subplan.
 */
static Node* build_subplan(PlannerInfo* root, Plan* plan, PlannerInfo* subroot, List* plan_params,
    SubLinkType subLinkType, Node* testexpr, bool adjust_testexpr, bool unknownEqFalse)
{
#define MAX_PLAN_NAME_LEN 32
#define MAX_PARAM_LEN 12
    Node* result = NULL;
    SubPlan* splan = NULL;
    bool isInitPlan = false;
    ListCell* lc = NULL;
    errno_t errorno = EOK;

    /*
     * Initialize the SubPlan node.  Note plan_id, plan_name, and cost fields
     * are set further down.
     */
    splan = makeNode(SubPlan);
    splan->subLinkType = subLinkType;
    splan->testexpr = NULL;
    splan->paramIds = NIL;
    get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod, &splan->firstColCollation);
    splan->useHashTable = false;
    splan->unknownEqFalse = unknownEqFalse;
    splan->setParam = NIL;
    splan->parParam = NIL;
    splan->args = NIL;

    /*
     * Make parParam and args lists of param IDs and expressions that current
     * query level will pass to this child plan.
     */
    foreach (lc, plan_params) {
        PlannerParamItem* pitem = (PlannerParamItem*)lfirst(lc);
        Node* arg = pitem->item;

        /*
         * The Var, PlaceHolderVar, or Aggref has already been adjusted to
         * have the correct varlevelsup, phlevelsup, or agglevelsup.
         *
         * If it's a PlaceHolderVar or Aggref, its arguments might contain
         * SubLinks, which have not yet been processed (see the comments for
         * SS_replace_correlation_vars).  Do that now.
         */
        if (IsA(arg, PlaceHolderVar) || IsA(arg, Aggref))
            arg = SS_process_sublinks(root, arg, false);

        splan->parParam = lappend_int(splan->parParam, pitem->paramId);
        splan->args = lappend(splan->args, arg);
    }

    /*
     * Un-correlated or undirect correlated plans of EXISTS, EXPR, ARRAY, or
     * ROWCOMPARE types can be used as initPlans.  For EXISTS, EXPR, or ARRAY,
     * we just produce a Param referring to the result of evaluating the
     * initPlan.  For ROWCOMPARE, we must modify the testexpr tree to contain
     * PARAM_EXEC Params instead of the PARAM_SUBLINK Params emitted by the
     * parser.
     */
    if (splan->parParam == NIL && subLinkType == EXISTS_SUBLINK) {
        Param* prm = NULL;

        AssertEreport(testexpr == NULL, MOD_OPT_SUBPLAN, "No testexpr required for exists sublink");
        prm = generate_new_param(root, BOOLOID, -1, InvalidOid);
        splan->setParam = list_make1_int(prm->paramid);
        isInitPlan = true;
        result = (Node*)prm;
    } else if (splan->parParam == NIL && subLinkType == EXPR_SUBLINK) {
        TargetEntry* te = (TargetEntry*)linitial(plan->targetlist);
        Param* prm = NULL;

        AssertEreport(!te->resjunk, MOD_OPT_SUBPLAN, "Expr sublink shouldn't have junk columns");
        AssertEreport(testexpr == NULL, MOD_OPT_SUBPLAN, "No testexpr required for expr sublink");

        prm = generate_new_param(
            root, exprType((Node*)te->expr), exprTypmod((Node*)te->expr), exprCollation((Node*)te->expr));
        splan->setParam = list_make1_int(prm->paramid);
        isInitPlan = true;
        result = (Node*)prm;
    } else if (splan->parParam == NIL && subLinkType == ARRAY_SUBLINK) {
        TargetEntry* te = (TargetEntry*)linitial(plan->targetlist);
        Oid arraytype;
        Param* prm = NULL;

        AssertEreport(!te->resjunk, MOD_OPT_SUBPLAN, "Array sublink shouldn't have junk columns");
        AssertEreport(testexpr == NULL, MOD_OPT_SUBPLAN, "No testexpr required for array sublink");

        arraytype = get_array_type(exprType((Node*)te->expr));
        if (!OidIsValid(arraytype))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_INDETERMINATE_DATATYPE),
                    errmsg("could not find array type for datatype %s", format_type_be(exprType((Node*)te->expr)))));
        prm = generate_new_param(root, arraytype, exprTypmod((Node*)te->expr), exprCollation((Node*)te->expr));
        splan->setParam = list_make1_int(prm->paramid);
        isInitPlan = true;
        result = (Node*)prm;
    } else if (splan->parParam == NIL && subLinkType == ROWCOMPARE_SUBLINK) {
        /* Adjust the Params */
        List* params = NIL;

        AssertEreport(testexpr != NULL, MOD_OPT_SUBPLAN, "There should be testexpr required for rowcompare sublink");

        params = generate_subquery_params(root, plan->targetlist, &splan->paramIds);
        result = convert_testexpr(root, testexpr, params);
        splan->setParam = list_copy(splan->paramIds);
        isInitPlan = true;

        /*
         * The executable expression is returned to become part of the outer
         * plan's expression tree; it is not kept in the initplan node.
         */
    } else {
        /*
         * Adjust the Params in the testexpr, unless caller said it's not
         * needed.
         */
        if (testexpr != NULL && adjust_testexpr) {
            List* params = NIL;

            params = generate_subquery_params(root, plan->targetlist, &splan->paramIds);
            splan->testexpr = convert_testexpr(root, testexpr, params);
        } else
            splan->testexpr = testexpr;

        /*
         * We can't convert subplans of ALL_SUBLINK or ANY_SUBLINK types to
         * initPlans, even when they are uncorrelated or undirect correlated,
         * because we need to scan the output of the subplan for each outer
         * tuple.  But if it's a not-direct-correlated IN (= ANY) test, we
         * might be able to use a hashtable to avoid comparing all the tuples.
         */
        if (subLinkType == ANY_SUBLINK && splan->parParam == NIL && subplan_is_hashable(plan) &&
            testexpr_is_hashable(splan->testexpr))
            splan->useHashTable = true;

        /*
         * Otherwise, we have the option to tack a Material node onto the top
         * of the subplan, to reduce the cost of reading it repeatedly.  This
         * is pointless for a direct-correlated subplan, since we'd have to
         * recompute its results each time anyway.	For uncorrelated/undirect
         * correlated subplans, we add Material unless the subplan's top plan
         * node would materialize its output anyway.  Also, if enable_material
         * is false, then the user does not want us to materialize anything
         * unnecessarily, so we don't.
         */
        else if (splan->parParam == NIL && u_sess->attr.attr_sql.enable_material &&
                 !ExecMaterializesOutput(nodeTag(plan)))
            plan = materialize_finished_plan(plan, false, root->glob->vectorized);

        result = (Node*)splan;
        isInitPlan = false;
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* For init plan, we should make it broadcasted if not */
    if (IS_STREAM_PLAN && is_execute_on_datanodes(plan) &&
        (isInitPlan || subLinkType == ANY_SUBLINK || subLinkType == ALL_SUBLINK)) {
        Plan* lefttree = IsA(plan, Material) ? plan->lefttree : plan;

        Distribution* distribution = ng_get_dest_distribution(lefttree);
        Distribution* target_distribution = ng_get_correlated_subplan_group_distribution();

        if (!is_replicated_plan(plan) && subLinkType == ARRAY_SUBLINK) {
            plan = add_broacast_under_local_sort(root, subroot, plan);
        }

        if (!is_replicated_plan(plan) || (!ng_is_same_group(distribution, target_distribution))) {
            plan = make_stream_plan(root, lefttree, NIL, 0.0, target_distribution);
        }

        /* We need to add materialize for non-init plan to avoid rescan */
        if (!isInitPlan && contain_special_plan_node(plan, T_Stream)) {
            if (IsA(plan, Material))
                ((Material*)plan)->materialize_all = true;
            else
                plan = materialize_finished_plan(plan, true, root->glob->vectorized);
        }
    }
#endif
    /*
     * Add the subplan and its PlannerInfo to the global lists.
     */
    root->glob->subplans = lappend(root->glob->subplans, plan);
    root->glob->subroots = lappend(root->glob->subroots, subroot);
    splan->plan_id = list_length(root->glob->subplans);

    if (isInitPlan)
        root->init_plans = lappend(root->init_plans, splan);

    /*
     * A parameterless subplan (not initplan) should be prepared to handle
     * REWIND efficiently.	If it has direct parameters then there's no point
     * since it'll be reset on each scan anyway; and if it's an initplan then
     * there's no point since it won't get re-run without parameter changes
     * anyway.	The input of a hashed subplan doesn't need REWIND either.
     */
    if (splan->parParam == NIL && !isInitPlan && !splan->useHashTable)
        root->glob->rewindPlanIDs = bms_add_member(root->glob->rewindPlanIDs, splan->plan_id);

    /* Label the subplan for EXPLAIN purposes */
    if (isInitPlan) {
        ListCell* lc2 = NULL;
        int offset;
        const int len = MAX_PLAN_NAME_LEN + MAX_PARAM_LEN * list_length(splan->setParam);

        splan->plan_name = (char*)palloc(len);
        errorno = sprintf_s(splan->plan_name, len, "InitPlan %d (returns ", splan->plan_id);
        securec_check_ss(errorno, "", "");

        offset = strlen(splan->plan_name);
        foreach (lc2, splan->setParam) {
            errorno =
                sprintf_s(splan->plan_name + offset, len - offset, "$%d%s", lfirst_int(lc2), lnext(lc2) ? "," : "");
            securec_check_ss(errorno, "", "");

            offset += strlen(splan->plan_name + offset);
        }

        errorno = sprintf_s(splan->plan_name + offset, len - offset, ")");
        securec_check_ss(errorno, "", "");
    } else {
        splan->plan_name = (char*)palloc(MAX_PLAN_NAME_LEN);
        errorno = sprintf_s(splan->plan_name, MAX_PLAN_NAME_LEN, "SubPlan %d", splan->plan_id);
        securec_check_ss(errorno, "", "");
    }

    /* Lastly, fill in the cost estimates for use later */
    cost_subplan(root, splan, plan);

    return result;
}

/*
 * generate_subquery_params: build a list of Params representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 *
 * We also return an integer list of the paramids of the Params.
 */
static List* generate_subquery_params(PlannerInfo* root, List* tlist, List** paramIds)
{
    List* result = NIL;
    List* ids = NIL;
    ListCell* lc = NULL;

    result = ids = NIL;
    foreach (lc, tlist) {
        TargetEntry* tent = (TargetEntry*)lfirst(lc);
        Param* param = NULL;

        if (tent->resjunk)
            continue;

        param = generate_new_param(
            root, exprType((Node*)tent->expr), exprTypmod((Node*)tent->expr), exprCollation((Node*)tent->expr));
        result = lappend(result, param);
        ids = lappend_int(ids, param->paramid);
    }

    *paramIds = ids;
    return result;
}

/*
 * generate_subquery_vars: build a list of Vars representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 * The Vars have the specified varno (RTE index).
 */
static List* generate_subquery_vars(PlannerInfo* root, List* tlist, Index varno)
{
    List* result = NIL;
    ListCell* lc = NULL;

    result = NIL;
    foreach (lc, tlist) {
        TargetEntry* tent = (TargetEntry*)lfirst(lc);
        Var* var = NULL;

        if (tent->resjunk)
            continue;

        var = makeVarFromTargetEntry(varno, tent);
        result = lappend(result, var);
    }

    return result;
}

/*
 * convert_testexpr: convert the testexpr given by the parser into
 * actually executable form.  This entails replacing PARAM_SUBLINK Params
 * with Params or Vars representing the results of the sub-select.	The
 * nodes to be substituted are passed in as the List result from
 * generate_subquery_params or generate_subquery_vars.
 *
 * The given testexpr has already been recursively processed by
 * process_sublinks_mutator.  Hence it can no longer contain any
 * PARAM_SUBLINK Params for lower SubLink nodes; we can safely assume that
 * any we find are for our own level of SubLink.
 */
static Node* convert_testexpr(PlannerInfo* root, Node* testexpr, List* subst_nodes)
{
    convert_testexpr_context context;

    context.root = root;
    context.subst_nodes = subst_nodes;
    return convert_testexpr_mutator(testexpr, &context);
}

static Node* convert_testexpr_mutator(Node* node, convert_testexpr_context* context)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Param)) {
        Param* param = (Param*)node;

        if (param->paramkind == PARAM_SUBLINK) {
            if (param->paramid <= 0 || param->paramid > list_length(context->subst_nodes))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("unexpected PARAM_SUBLINK ID: %d", param->paramid)));

            /*
             * We copy the list item to avoid having doubly-linked
             * substructure in the modified parse tree.  This is probably
             * unnecessary when it's a Param, but be safe.
             */
            return (Node*)copyObject(list_nth(context->subst_nodes, param->paramid - 1));
        }
    }

    if (IsA(node, SubLink)) {
        /*
         * If we come across a nested SubLink, it is neither necessary nor
         * correct to recurse into it: any PARAM_SUBLINKs we might find inside
         * belong to the inner SubLink not the outer. So just return it as-is.
         *
         * This reasoning depends on the assumption that nothing will pull
         * subexpressions into or out of the testexpr field of a SubLink, at
         * least not without replacing PARAM_SUBLINKs first.  If we did want
         * to do that we'd need to rethink the parser-output representation
         * altogether, since currently PARAM_SUBLINKs are only unique per
         * SubLink not globally across the query.  The whole point of
         * replacing them with Vars or PARAM_EXEC nodes is to make them
         * globally unique before they escape from the SubLink's testexpr.
         *
         * Note: this can't happen when called during SS_process_sublinks,
         * because that recursively processes inner SubLinks first.  It can
         * happen when called from convert_ANY_sublink_to_join, though.
         */
        return node;
    }
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) convert_testexpr_mutator, (void*)context);
}

/*
 * subplan_is_hashable: can we implement an ANY subplan by hashing?
 */
static bool subplan_is_hashable(Plan* plan)
{
    double subquery_size;

    /*
     * The estimated size of the subquery result must fit in work_mem. (Note:
     * we use sizeof(HeapTupleHeaderData) here even though the tuples will
     * actually be stored as MinimalTuples; this provides some fudge factor
     * for hashtable overhead.)
     */
    subquery_size = plan->plan_rows * (MAXALIGN(plan->plan_width) + MAXALIGN(sizeof(HeapTupleHeaderData)));
    if (subquery_size > u_sess->attr.attr_memory.work_mem * 1024L)
        return false;

    return true;
}

/*
 * testexpr_is_hashable: is an ANY SubLink's test expression hashable?
 */
static bool testexpr_is_hashable(Node* testexpr)
{
    /*
     * The testexpr must be a single OpExpr, or an AND-clause containing only
     * OpExprs.
     *
     * The combining operators must be hashable and strict. The need for
     * hashability is obvious, since we want to use hashing. Without
     * strictness, behavior in the presence of nulls is too unpredictable.	We
     * actually must assume even more than plain strictness: they can't yield
     * NULL for non-null inputs, either (see nodeSubplan.c).  However, hash
     * indexes and hash joins assume that too.
     */
    if (testexpr && IsA(testexpr, OpExpr)) {
        if (hash_ok_operator((OpExpr*)testexpr))
            return true;
    } else if (and_clause(testexpr)) {
        ListCell* l = NULL;

        foreach (l, ((BoolExpr*)testexpr)->args) {
            Node* andarg = (Node*)lfirst(l);

            if (!IsA(andarg, OpExpr))
                return false;
            if (!hash_ok_operator((OpExpr*)andarg))
                return false;
        }
        return true;
    }

    return false;
}

static Node* convert_joinqual_to_antiqual(Node* node, Query* parse)
{
    Node* antiqual = NULL;

    if (node == NULL)
        return NULL;

    switch (nodeTag(node)) {
        case T_OpExpr:
            antiqual = convert_opexpr_to_boolexpr_for_antijoin(node, parse);
            break;
        case T_BoolExpr: {
            /*Not IN, should be and clause.*/
            if (and_clause(node)) {
                BoolExpr* boolexpr = (BoolExpr*)node;
                List* andarglist = NIL;
                ListCell* l = NULL;

                foreach (l, boolexpr->args) {
                    Node* andarg = (Node*)lfirst(l);
                    Node* expr = NULL;

                    /* The listcell type of args should be OpExpr. */
                    expr = convert_opexpr_to_boolexpr_for_antijoin(andarg, parse);
                    if (expr == NULL)
                        return NULL;

                    andarglist = lappend(andarglist, expr);
                }

                antiqual = (Node*)makeBoolExpr(AND_EXPR, andarglist, boolexpr->location);
            } else
                return NULL;

        } break;
        case T_ScalarArrayOpExpr:
        case T_RowCompareExpr:
        default:
            antiqual = NULL;
            break;
    }

    return antiqual;
}

static Node* convert_opexpr_to_boolexpr_for_antijoin(Node* node, Query* parse)
{
    Node* boolexpr = NULL;
    List* antiqual = NIL;
    OpExpr* opexpr = NULL;

    if (!IsA(node, OpExpr))
        return NULL;
    else
        opexpr = (OpExpr*)node;

    antiqual = (List*)list_make1(opexpr);

    Node* larg = (Node*)linitial(opexpr->args);
    if (IsA(larg, RelabelType))
        larg = (Node*)((RelabelType*)larg)->arg;
    if (!check_var_nonnullable(parse, larg))
        antiqual = lappend(antiqual, makeNullTest(IS_NULL, (Expr*)copyObject(larg)));

    Node* rarg = (Node*)lsecond(opexpr->args);
    if (IsA(rarg, RelabelType))
        rarg = (Node*)((RelabelType*)rarg)->arg;
    if (!check_var_nonnullable(parse, rarg))
        antiqual = lappend(antiqual, makeNullTest(IS_NULL, (Expr*)copyObject(rarg)));

    if (list_length(antiqual) > 1)
        boolexpr = (Node*)makeBoolExprTreeNode(OR_EXPR, antiqual);
    else
        boolexpr = (Node*)opexpr;

    return boolexpr;
}

/*
 * check_var_nonnullable
 *	check if the node is nullable
 * Parameters:
 *	@in query: parse tree of current level
 *	@in node: the expression that need to check nullable
 * Return:
 *	true if the var has not-null constraint, else false
 */
bool check_var_nonnullable(Query* query, Node* node)
{
    bool result = false;
    RangeTblEntry* rte = NULL;
    Var* var = NULL;
    bool isFound = false;

    if (node == NULL)
        return false;

    /* Locate real var from type cast expression */
    var = locate_base_var((Expr*)node);
    if (var == NULL || !IsA(var, Var))
        return false;

    /* For parameters that passed from outer level, return false */
    if (var->varlevelsup > 0)
        return false;

    /* Get real var from the base relation */
    var = (Var*)copyObject(var);
    isFound = get_real_rte_varno_attno(query, &var->varno, &var->varattno);
    /* If var appears in full join expr, null can be added */
    if (isFound) {
        pfree_ext(var);
        return false;
    }

    /* If var appears in the inner side of outer join, null can be added */
    if (is_join_inner_side((Node*)query->jointree, (Index)var->varno, false, &isFound)) {
        pfree_ext(var);
        return false;
    }

    rte = (RangeTblEntry*)list_nth(query->rtable, var->varno - 1);
    switch (rte->rtekind) {
        case RTE_RELATION: {
            HeapTuple tp;
            Form_pg_attribute att_tup;

            tp = SearchSysCache(ATTNUM, ObjectIdGetDatum(rte->relid), Int16GetDatum(var->varattno), 0, 0);
            if (!HeapTupleIsValid(tp)) {
                pfree_ext(var);
                return false;
            }
            att_tup = (Form_pg_attribute)GETSTRUCT(tp);
            result = att_tup->attnotnull;

            if (!result) {
                /* If have information not null constraint. */
                result = findConstraintByVar(var, rte->relid, NOT_NULL_CONSTRAINT);
            }
            ReleaseSysCache(tp);
        } break;
        case RTE_JOIN:
            /* shouldn't touch here */
            Assert(false);
            break;
        case RTE_SUBQUERY:
            /* we'll quit for grouping sets case, since null will be added */
            if (rte->subquery->groupingSets == NIL) {
                TargetEntry* te = (TargetEntry*)list_nth(rte->subquery->targetList, var->varattno - 1);
                result = check_var_nonnullable(rte->subquery, (Node*)te->expr);
            }
            break;
        case RTE_FUNCTION:
        case RTE_VALUES:
        case RTE_CTE:
            result = false;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized RTE kind: %d", (int)rte->rtekind)));
            result = false; /* keep compiler quiet */
    }

    pfree_ext(var);
    return result;
}

/*
 * Check expression is hashable + strict
 *
 * We could use op_hashjoinable() and op_strict(), but do it like this to
 * avoid a redundant cache lookup.
 */
static bool hash_ok_operator(OpExpr* expr)
{
    Oid opid = expr->opno;

    /* quick out if not a binary operator */
    if (list_length(expr->args) != 2)
        return false;
    if (opid == ARRAY_EQ_OP) {
        /* array_eq is strict, but must check input type to ensure hashable */
        /* XXX record_eq will need same treatment when it becomes hashable */
        Node* leftarg = (Node*)linitial(expr->args);

        return op_hashjoinable(opid, exprType(leftarg));
    } else {
        /* else must look up the operator properties */
        HeapTuple tup;
        Form_pg_operator optup;

        tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opid));
        if (!HeapTupleIsValid(tup))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for operator %u", opid)));
        optup = (Form_pg_operator)GETSTRUCT(tup);
        if (!optup->oprcanhash || !func_strict(optup->oprcode)) {
            ReleaseSysCache(tup);
            return false;
        }
        ReleaseSysCache(tup);
        return true;
    }
}

static bool recursive_reference_recursive(Query* parse)
{
    /* recursive ctes references itself in as the right argument of the first tier */
    if (parse->setOperations == NULL || !IsA(parse->setOperations, SetOperationStmt) ||
        ((SetOperationStmt*)parse->setOperations)->rarg == NULL ||
        !IsA(((SetOperationStmt*)parse->setOperations)->rarg, RangeTblRef)) {
        return false;
    }
    RangeTblRef* rtr = (RangeTblRef*)((SetOperationStmt*)parse->setOperations)->rarg;
    RangeTblEntry *rte = rt_fetch(rtr->rtindex, parse->rtable);
    List* rightArgs = list_make1(rte);
    return range_table_walker(rightArgs, (bool (*)())recursive_reference_recursive_walker, NULL, 0);
}

static bool recursive_reference_recursive_walker(Node* node)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        if (query->rtable != NIL) {
            ListCell* lc = NULL;
            foreach(lc, query->rtable) {
                RangeTblEntry *rte = (RangeTblEntry*)lfirst(lc);
                if (rte->cterecursive && !rte->self_reference) {
                    return true;
                }
            }
        }
        return query_tree_walker(query, (bool (*)())recursive_reference_recursive_walker, NULL, 0);
    }
    return expression_tree_walker(node, (bool (*)())recursive_reference_recursive_walker, NULL);
}

/*
 * contain_dml: is any subquery not a plain SELECT?
 *
 * We reject SELECT FOR UPDATE/SHARE as well as INSERT etc.
 */
static bool contain_dml(Node* node)
{
    return contain_dml_walker(node, NULL);
}

static bool contain_dml_walker(Node *node, void *context)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        if (query->commandType != CMD_SELECT || query->rowMarks != NIL)
            return true;

        return query_tree_walker(query, (bool (*)())contain_dml_walker, context, 0);
    }
    return expression_tree_walker(node, (bool (*)())contain_dml_walker, context);
}

/*
 * contain_outer_selfref: is there an external recursive self-reference?
 */
static bool contain_outer_selfref(Node* node)
{
    Index depth = 0;

    /*
     * We should be starting with a Query, so that depth will be 1 while
     * examining its immediate contents.
     */
    Assert(IsA(node, Query));

    return contain_outer_selfref_walker(node, &depth);
}

static bool contain_outer_selfref_walker(Node *node, Index *depth)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)node;
        /*
         * Check for a self-reference to a CTE that's above the Query that our
         * search started at.
         */
        if (rte->rtekind == RTE_CTE && rte->self_reference && rte->ctelevelsup >= *depth)
            return true;
        return false; /* allow range_table_walker to continue */
    }

    if (IsA(node, Query)) {
        /* Recurse into subquery, tracking nesting depth properly */
        Query* query = (Query*)node;
        bool result = false;
        (*depth)++;
        result = query_tree_walker(query, (bool (*)())contain_outer_selfref_walker, (void*)depth,  QTW_EXAMINE_RTES);
        (*depth)--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())contain_outer_selfref_walker, (void*)depth);
}

typedef struct UnderGroupingCxt {
    char* ctename;
    int depth;
    bool under_grouping;
    Query* subroot;
} UnderGroupingCxt;

static bool under_grouping_func_walker(Node* node, UnderGroupingCxt* cxt)
{
    if (node == NULL) {
        return false;
    }

    /* increase query depth along with encountered subquery */
    if (IsA(node, Query)) {
        Query* query = (Query*)node;
        bool result = false;
        Query* save_subroot = cxt->subroot;
        cxt->subroot = query;
        cxt->depth++;
        result = query_tree_walker(query, (bool (*)())under_grouping_func_walker, (void*)cxt, 0);
        cxt->depth--;
        cxt->subroot = save_subroot;
        return result;
    }

    if (IsA(node, GroupingFunc)) {
        bool result = false;
        bool save_flag = cxt->under_grouping;
        cxt->under_grouping = true;
        result = expression_tree_walker(node, (bool (*)())under_grouping_func_walker, (void*)cxt);
        cxt->under_grouping = save_flag;
        return result;
    }

    if (IsA(node, Var) && cxt->under_grouping) {
        Var* var = (Var*)node;
        RangeTblEntry* rte = rt_fetch(var->varno, cxt->subroot->rtable);
        if ((int)rte->ctelevelsup == cxt->depth && rte->ctename != NULL && strcmp(rte->ctename, cxt->ctename) == 0) {
            return true;
        }
    }

    return expression_tree_walker(node, (bool (*)())under_grouping_func_walker, (void*)cxt);
}

/* walk through the parent query tree to see if the CTE is referenced within grouping functions */
static bool under_grouping_func(Query* parse, CommonTableExpr* cte)
{
    if (cte->ctename == NULL) {
        return false;
    }
    UnderGroupingCxt cxt = {cte->ctename, -1, false, parse};
    return under_grouping_func_walker((Node*)parse, &cxt);
}

static bool cte_inline_common(CommonTableExpr* cte, char** reason, Query* parse)
{
    /*
     * Consider inlining the CTE (creating RTE_SUBQUERY RTE(s)) instead of
     * implementing it as a separately-planned CTE for common cases.
     *
     * We cannot inline if any of these conditions hold:
     *
     * 1. The user said not to (the CTEMaterializeAlways option).
     *
     * 2. The CTE has side-effects; this includes either not being a plain
     * SELECT, or containing volatile functions.  Inlining might change
     * the side-effects, which would be bad.
     *
     * 3. The CTE is multiply-referenced and contains a self-reference to
     * a recursive CTE outside itself.  Inlining would result in multiple
     * recursive self-references, which we don't support.
     *
     * 4. Note that recursive CTE cannot be inlined. But how we handle them
     * depend upon whether it is stream-recursive plan or other scenario, so
     * it is checked externally by cte_inline() and cte_inline_stream().
     *
     * 5. CTE that contains subqueries cannot be inlined if they appears in
     * grouping functions.
     *
     * Note: we check for volatile functions last, because that's more
     * expensive than the other tests needed.
     */
    if (cte->ctematerialized == CTEMaterializeAlways) {
        *reason = pstrdup("explicitly declared as materialized");
        return false;
    } else if (((Query*)cte->ctequery)->commandType != CMD_SELECT || contain_dml(cte->ctequery)) {
        *reason = pstrdup("not a plain SELECT statement.");
        return false;
    } else if ((cte->cterefcount > 1 && contain_outer_selfref(cte->ctequery))) {
        *reason = pstrdup("contains a self-reference to a recursive CTE outside");
        return false;
    } else if (contain_volatile_functions(cte->ctequery, true)) {
        *reason = pstrdup("contains volatile function");
        return false;
    } else if (under_grouping_func(parse, cte)) {
        contain_subquery_context context = init_contain_subquery_context();
        List* subquery_list = contains_subquery((Node*)cte->ctequery, &context);
        if (list_length(subquery_list) != 0) {
            *reason = pstrdup("contains subquery while in grouping function");
            list_free(subquery_list);
        }
        return false;
    }
    return true;
}

static bool cte_inline(CommonTableExpr* cte, Query* parse)
{
    /*
     * cte_inline common is passed if there is no side effect for inlining.
     *
     * We have an option whether to inline or not.  That should
     * always be a win if there's just a single reference, but if the CTE
     * is multiply-referenced then it's unclear: inlining adds duplicate
     * computations, but the ability to absorb restrictions from the outer
     * query level could outweigh that.  We do not have nearly enough
     * information at this point to tell whether that's true, so we let
     * the user express a preference.  Our default behavior is to inline
     * only singly-referenced CTEs, but a CTE marked CTEMaterializeNever
     * will be inlined even if multiply referenced.
     * Also, if a singly-referenced CTE is referenced by a subquery, it
     * will be not be inlined as default behavior. This is because we do
     * not know if this subquery can be pulled up. If it cannot be pulled
     * up, we may risk nested subplan execution, which can be disastrous.
     */
    char* buf = NULL;
    bool ret = cte_inline_common(cte, &buf, parse);
    if (ret) {
        Assert(buf == NULL);
        if (cte->ctematerialized == CTEMaterializeDefault) {
            if (cte->referenced_by_subquery) {
                buf = pstrdup("CTE is not inlined by default if referenced by subquery");
                ret = false;
            } else if (cte->cterefcount > 1) {
                buf = pstrdup("CTE is not inlined by default if referenced more than once");
                ret = false;
            }
        } else if (cte->cterecursive) {
            buf = pstrdup("recursive CTE cannot be inlined");
            ret = false;
        }
    }
    if (!ret) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("CTE \"%s\"is not inlined. Reason: %s", cte->ctename, buf))));
    }
    pfree_ext(buf);
    return ret;
}

static bool cte_inline_stream(CommonTableExpr* cte, Query* parse)
{
    char* buf = NULL;

    Assert(IS_STREAM_PLAN);

    if (!u_sess->attr.attr_sql.enable_stream_recursive && cte->cterecursive) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Unsupported CTE substitution causes SQL unshipped");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
    }

    if (recursive_reference_recursive((Query*)cte->ctequery)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "Recursive CTE references recursive CTE \"%s\"",
            cte->ctename);
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
    }
    /*
     * For stream plan, CTEs only support inline execution. Fallback to pgxc
     * plan if inline criteria are violated.
     *
     * In preference of stream plan, we DO NOT obey the rule of thumb if there
     * is no explicit expression of (not) materialized.
     */
    if (cte->cterecursive) { /* Recursive CTE will be handled */
        return false;
    } else if (!cte_inline_common(cte, &buf, parse)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "CTE \"%s\" cannot be inlined. Reason: %s",
            cte->ctename, buf);
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        pfree_ext(buf);
        mark_stream_unsupport();
    }
    return true;
}

/*
 * SS_process_one_cte: process a cte in query's WITH list
 *
 * Working hourse for SS_process_ctes()
 */
static void SS_process_one_cte(PlannerInfo* root, CommonTableExpr* cte, Query* subquery)
{
    Plan* plan = NULL;
    bool tmp_need_autoanalyze = false;
    PlannerInfo* subroot = NULL;
    SubPlan* splan;
    int paramid;

    /*
     * Generate the plan for the CTE query.  Always plan for full
     * retrieval --- we don't have enough info to predict otherwise.
     */
    /* reset need_autoanalyze */
    tmp_need_autoanalyze = u_sess->analyze_cxt.need_autoanalyze;
    u_sess->analyze_cxt.need_autoanalyze = false;
    plan = subquery_planner(root->glob, subquery, root, cte->cterecursive, 0.0, &subroot);
    u_sess->analyze_cxt.need_autoanalyze = tmp_need_autoanalyze;

    /*
     * Since the current query level doesn't yet contain any RTEs, it
     * should not be possible for the CTE to have requested parameters of
     * this level.
     */
    if (root->plan_params)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("unexpected outer reference in CTE query")));

    /*
     * Make a SubPlan node for it.	This is just enough unlike
     * build_subplan that we can't share code.
     *
     * Note plan_id, plan_name, and cost fields are set further down.
     */
    splan = makeNode(SubPlan);
    splan->subLinkType = CTE_SUBLINK;
    splan->testexpr = NULL;
    splan->paramIds = NIL;
    get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod, &splan->firstColCollation);
    splan->useHashTable = false;
    splan->unknownEqFalse = false;
    splan->setParam = NIL;
    splan->parParam = NIL;
    splan->args = NIL;

    /*
     * The node can't have any inputs (since it's an initplan), so the
     * parParam and args lists remain empty.  (It could contain references
     * to earlier CTEs' output param IDs, but CTE outputs are not
     * propagated via the args list.)
     */
    /*
     * Assign a param ID to represent the CTE's output.  No ordinary
     * "evaluation" of this param slot ever happens, but we use the param
     * ID for setParam/chgParam signaling just as if the CTE plan were
     * returning a simple scalar output.  (Also, the executor abuses the
     * ParamExecData slot for this param ID for communication among
     * multiple CteScan nodes that might be scanning this CTE.)
     */
    paramid = SS_assign_special_param(root);
    splan->setParam = list_make1_int(paramid);

    /*
     * Add the subplan and its PlannerInfo to the global lists.
     */
    if (IsA(plan, RecursiveUnion)) {
        ((RecursiveUnion*)plan)->is_used = false;
    }

    root->glob->subplans = lappend(root->glob->subplans, plan);
    root->glob->subroots = lappend(root->glob->subroots, subroot);
    splan->plan_id = list_length(root->glob->subplans);

    root->init_plans = lappend(root->init_plans, splan);

    root->cte_plan_ids = lappend_int(root->cte_plan_ids, splan->plan_id);

    /* Label the subplan for EXPLAIN purposes */
    splan->plan_name = (char*)palloc(4 + strlen(cte->ctename) + 1);
    errno_t errorno = EOK;
    errorno = sprintf_s(splan->plan_name, 4 + strlen(cte->ctename) + 1, "CTE %s", cte->ctename);
    securec_check_ss(errorno, "", "");

    /* Set locator_type */
    if (plan->exec_nodes != NULL)
        cte->locator_type = plan->exec_nodes->baselocatortype;

    /* Lastly, fill in the cost estimates for use later */
    cost_subplan(root, splan, plan);
}

typedef struct inline_cte_walker_context
{
    const char *ctename;        /* name and relative level of target CTE */
    Index varlevelsup;
    int refcount;               /* number of remaining references */
    Query *ctequery;            /* query to substitute */
} inline_cte_walker_context;

static bool inline_cte_walker(Node *node, inline_cte_walker_context *context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        context->varlevelsup++;

        /*
         * Visit the query's RTE nodes after their contents; otherwise
         * query_tree_walker would descend into the newly inlined CTE query,
         * which we don't want.
         */
        (void) query_tree_walker(query, (bool (*)())inline_cte_walker, context, QTW_EXAMINE_RTES);

        context->varlevelsup--;

        return false;
    }
    else if (IsA(node, RangeTblEntry))
    {
        RangeTblEntry *rte = (RangeTblEntry *) node;

        if (rte->rtekind == RTE_CTE &&
            strcmp(rte->ctename, context->ctename) == 0 &&
            rte->ctelevelsup == context->varlevelsup) {
            /*
             * Found a reference to replace.  Generate a copy of the CTE query
             * with appropriate level adjustment for outer references (e.g.,
             * to other CTEs).
             */
            Query *newquery = (Query*)copyObject(context->ctequery);

            /*
             * if correlated cte is referenced in different subquery level,
             * we should also adjust correlated params in cte parse tree
             */
            if (context->varlevelsup > 0)
                IncrementVarSublevelsUp((Node *) newquery, context->varlevelsup, 1);

            /*
             * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
             *
             * Historically, a FOR UPDATE clause has been treated as extending
             * into views and subqueries, but not into CTEs.  We preserve this
             * distinction by not trying to push rowmarks into the new
             * subquery.
             */
            rte->rtekind = RTE_SUBQUERY;
            rte->subquery = newquery;
            rte->security_barrier = false;

            /* Zero out CTE-specific fields */
            rte->ctename = NULL;
            rte->ctelevelsup = 0;
            rte->self_reference = false;

            /* Count the number of replacements we've done */
            context->refcount--;
        }

        return false;
    }

    return expression_tree_walker(node, (bool (*)())inline_cte_walker, context);
}

/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
static void inline_cte(PlannerInfo *root, CommonTableExpr *cte)
{
    struct inline_cte_walker_context context;

    context.ctename = cte->ctename;
    /* Start at varlevelsup = -1 because we'll immediately increment it */
    context.varlevelsup = -1;
    context.refcount = cte->cterefcount;
    context.ctequery = castNode(Query, cte->ctequery);

    (void) inline_cte_walker((Node*)root->parse, &context);

    /*
     * We would expect the reference number to be zero after inline, however, the 
     * reference count of cte are not accurate for re-entry issues at parsing stage
     * Until fixed, we only check for non-negative refcnt result.
     */
    Assert(context.refcount >= 0);
    /* Mark this CTE as inlined */
    cte->cterefcount = -1;
}

/*
 * SS_process_ctes: process a query's WITH list
 *
 * We plan each interesting WITH item and convert it to an initplan.
 * A side effect is to fill in root->cte_plan_ids with a list that
 * parallels root->parse->cteList and provides the subplan ID for
 * each CTE's initplan.
 */
void SS_process_ctes(PlannerInfo* root)
{
    ListCell* lc = NULL;

    AssertEreport(
        root->cte_plan_ids == NIL, MOD_OPT_SUBPLAN, "Plan id shouldn't be assigned before CTE plan generation");

    foreach (lc, root->parse->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
        CmdType cmdType = ((Query*)cte->ctequery)->commandType;
        Query* subquery = NULL;

        /*
         * Ignore SELECT CTEs that are not actually referenced anywhere.
         */
        if (cte->cterefcount == 0 && cmdType == CMD_SELECT) {
            /* Make a dummy entry in cte_plan_ids */
            root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
            continue;
        }

        /*
         * Copy the source Query node.	Probably not necessary, but let's keep
         * this similar to make_subplan.
         */
        subquery = (Query*)copyObject(cte->ctequery);

        /* plan_params should not be in use in current query level */
        AssertEreport(
            root->plan_params == NIL, MOD_OPT_SUBPLAN, "Plan_params should not be in use in current query level");

        if (STREAM_RECURSIVECTE_SUPPORTED && cte->cterecursive) {
            if (cte_inline_stream(cte, root->parse)) {
                inline_cte(root, cte);
                /* Make a dummy entry in cte_plan_ids */
                root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
                continue;
            }
            int save_plan_current_seed = u_sess->opt_cxt.plan_current_seed;

            /*
             * When recursive CTE is referenced more than one times, we are goting to
             * generate each CTE sperately for each references, we have to do so as the
             * current SQL execution framework is not supporting an execution plan-tree
             * in share-scan mode. In order to improve the stream planning of recursive
             * CTE when we encounter in this case, we have to setup the subplan tree
             * for each of them
             *
             * Note: Implement share-scan in SQL execution engine to natively support it
             */
            int cterefcount = cte->cterefcount;
            while (cterefcount > 0) {
                /*
                 * In cause of CTE refered multi-times, we need the CTE plan identically
                 * created, in order to do this we reset the current_seed and seed factor
                 * before we generate subplan for recursive-cte.
                 *
                 * We have to do so from historical reasons where distribute-key of CTE
                 * is held by RTE where it is shared by different references(range table)
                 * a different CteScan plan could messup the join order, distribution key
                 * selection logics.
                 */
                if (cte->cterecursive && cte->cterefcount > 1 &&
                    u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
                    set_path_seed_factor_zero();
                    u_sess->opt_cxt.plan_current_seed = save_plan_current_seed;
                }

                SS_process_one_cte(root, cte, subquery);

                /*
                 * In order to eliminate the QRW impact on subquery, we need get a new
                 * COPY of cte->ctequery for this case to plan the subquery of CTE from
                 * scratch
                 */
                subquery = (Query*)copyObject(cte->ctequery);
                cterefcount--;
            }
        } else {
             /* cte which contains dml should always be processed whether referenced or not */
            if (cte->cterefcount < 1 && !contain_dml(cte->ctequery)) {
                continue;
            }
            if ((IS_STREAM_PLAN && cte_inline_stream(cte, root->parse)) ||
                (!IS_STREAM_PLAN && cte_inline(cte, root->parse))) {
                inline_cte(root, cte);
                /* Make a dummy entry in cte_plan_ids */
                root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
                continue;
            }
            SS_process_one_cte(root, cte, subquery);
        }
    }
}

/*
 * convert_ANY_sublink_to_join: try to convert an ANY SubLink to a join
 *
 * The caller has found an ANY SubLink at the top level of one of the query's
 * qual clauses, but has not checked the properties of the SubLink further.
 * Decide whether it is appropriate to process this SubLink in join style.
 * If so, form a JoinExpr and return it.  Return NULL if the SubLink cannot
 * be converted to a join.
 *
 * The only non-obvious input parameter is available_rels: this is the set
 * of query rels that can safely be referenced in the sublink expression.
 * (We must restrict this to avoid changing the semantics when a sublink
 * is present in an outer join's ON qual.)  The conversion must fail if
 * the converted qual would reference any but these parent-query relids.
 *
 * On success, the returned JoinExpr has larg = NULL and rarg = the jointree
 * item representing the pulled-up subquery.  The caller must set larg to
 * represent the relation(s) on the lefthand side of the new join, and insert
 * the JoinExpr into the upper query's jointree at an appropriate place
 * (typically, where the lefthand relation(s) had been).  Note that the
 * passed-in SubLink must also be removed from its original position in the
 * query quals, since the quals of the returned JoinExpr replace it.
 * (Notionally, we replace the SubLink with a constant TRUE, then elide the
 * redundant constant from the qual.)
 *
 * On success, the caller is also responsible for recursively applying
 * pull_up_sublinks processing to the rarg and quals of the returned JoinExpr.
 * (On failure, there is no need to do anything, since pull_up_sublinks will
 * be applied when we recursively plan the sub-select.)
 *
 * Side effects of a successful conversion include adding the SubLink's
 * subselect to the query's rangetable, so that it can be referenced in
 * the JoinExpr's rarg.
 */
JoinExpr* convert_ANY_sublink_to_join(PlannerInfo* root, SubLink* sublink, bool under_not, Relids available_rels)
{
    JoinExpr* result = NULL;
    Query* parse = root->parse;
    Query* subselect = (Query*)sublink->subselect;
    if (has_no_expand_hint(subselect)) {
        return NULL;
    }
    Relids upper_varnos;
    int rtindex;
    RangeTblEntry* rte = NULL;
    RangeTblRef* rtr = NULL;
    List* subquery_vars = NIL;
    Node* quals = NULL;

    AssertEreport(sublink->subLinkType == ANY_SUBLINK, MOD_OPT_REWRITE, "Any sublink is required");

    /*
     * The sub-select must not refer to any Vars of the parent query. (Vars of
     * higher levels should be okay, though.)
     */
    if (contain_vars_of_level((Node*)subselect, 1))
        return NULL;

    /*
     * The test expression must contain some Vars of the parent query, else
     * it's not gonna be a join.  (Note that it won't have Vars referring to
     * the subquery, rather Params.)
     */
    upper_varnos = pull_varnos(sublink->testexpr);
    if (bms_is_empty(upper_varnos))
        return NULL;

    /*
     * However, it can't refer to anything outside available_rels.
     */
    if (!bms_is_subset(upper_varnos, available_rels))
        return NULL;

    /*
     * The combining operators and left-hand expressions mustn't be volatile.
     */
    if (contain_volatile_functions(sublink->testexpr))
        return NULL;

    /*
     * Okay, pull up the sub-select into upper range table.
     *
     * We rely here on the assumption that the outer query has no references
     * to the inner (necessarily true, other than the Vars that we build
     * below). Therefore this is a lot easier than what pull_up_subqueries has
     * to go through.
     */
    rte = addRangeTableEntryForSubquery(NULL, subselect, makeAlias("ANY_subquery", NIL), false, false, true);
    parse->rtable = lappend(parse->rtable, rte);
    rtindex = list_length(parse->rtable);

    /*
     * Form a RangeTblRef for the pulled-up sub-select.
     */
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;

    /*
     * Build a list of Vars representing the subselect outputs.
     */
    subquery_vars = generate_subquery_vars(root, subselect->targetList, rtindex);

    /*
     * Build the new join's qual expression, replacing Params with these Vars.
     */
    quals = convert_testexpr(root, sublink->testexpr, subquery_vars);

    result = makeNode(JoinExpr);
    if (under_not) {
        Node* antiquals = NULL;

        antiquals = convert_joinqual_to_antiqual(quals, parse);
        if (antiquals == NULL)
            return NULL;
        else
            result->quals = antiquals;

        result->jointype = JOIN_ANTI;
    } else {
        result->jointype = JOIN_SEMI;
        result->quals = quals;
    }

    /*
     * And finally, build the JoinExpr node.
     */
    result->isNatural = false;
    result->larg = NULL; /* caller must fill this in */
    result->rarg = (Node*)rtr;
    result->usingClause = NIL;
    result->alias = NULL;
    result->rtindex = 0; /* we don't need an RTE for it */

    /* Mark query's can_push */
    mark_parent_child_pushdown_flag(root->parse, subselect);

    return result;
}

/*
 * convert_EXISTS_sublink_to_join: try to convert an EXISTS SubLink to a join
 *
 * The API of this function is identical to convert_ANY_sublink_to_join's,
 * except that we also support the case where the caller has found NOT EXISTS,
 * so we need an additional input parameter "under_not".
 */
JoinExpr* convert_EXISTS_sublink_to_join(PlannerInfo* root, SubLink* sublink, bool under_not, Relids available_rels)
{
    JoinExpr* result = NULL;
    Query* parse = root->parse;
    Query* subselect = (Query*)sublink->subselect;
    if (has_no_expand_hint(subselect)) {
        return NULL;
    }
    Node* whereClause = NULL;
    int rtoffset;
    int varno;
    Relids clause_varnos;
    Relids upper_varnos;

    AssertEreport(sublink->subLinkType == EXISTS_SUBLINK, MOD_OPT_REWRITE, "Exists sublink is required");

    /*
     * Can't flatten if it contains WITH.  (We could arrange to pull up the
     * WITH into the parent query's cteList, but that risks changing the
     * semantics, since a WITH ought to be executed once per associated query
     * call.)  Note that convert_ANY_sublink_to_join doesn't have to reject
     * this case, since it just produces a subquery RTE that doesn't have to
     * get flattened into the parent query.
     */
    if (subselect->cteList)
        return NULL;

    /*
     * Copy the subquery so we can modify it safely (see comments in
     * make_subplan).
     */
    subselect = (Query*)copyObject(subselect);

    /*
     * See if the subquery can be simplified based on the knowledge that it's
     * being used in EXISTS().	If we aren't able to get rid of its
     * targetlist, we have to fail, because the pullup operation leaves us
     * with noplace to evaluate the targetlist.
     */
    if (!simplify_EXISTS_query(subselect))
        return NULL;

    /*
     * Separate out the WHERE clause.  (We could theoretically also remove
     * top-level plain JOIN/ON clauses, but it's probably not worth the
     * trouble.)
     */
    whereClause = subselect->jointree->quals;
    subselect->jointree->quals = NULL;

    /*
     * The rest of the sub-select must not refer to any Vars of the parent
     * query.  (Vars of higher levels should be okay, though.)
     */
    if (contain_vars_of_level((Node*)subselect, 1))
        return NULL;

    /*
     * On the other hand, the WHERE clause must contain some Vars of the
     * parent query, else it's not gonna be a join.
     */
    if (!contain_vars_of_level(whereClause, 1))
        return NULL;

    /*
     * We don't risk optimizing if the WHERE clause is volatile, either.
     */
    if (contain_volatile_functions(whereClause))
        return NULL;

    /*
     * The subquery must have a nonempty jointree, but we can make it so.
     */
    replace_empty_jointree(subselect);

    /*
     * Prepare to pull up the sub-select into top range table.
     *
     * We rely here on the assumption that the outer query has no references
     * to the inner (necessarily true). Therefore this is a lot easier than
     * what pull_up_subqueries has to go through.
     *
     * In fact, it's even easier than what convert_ANY_sublink_to_join has to
     * do.	The machinations of simplify_EXISTS_query ensured that there is
     * nothing interesting in the subquery except an rtable and jointree, and
     * even the jointree FromExpr no longer has quals.	So we can just append
     * the rtable to our own and use the FromExpr in our jointree. But first,
     * adjust all level-zero varnos in the subquery to account for the rtable
     * merger.
     */
    rtoffset = list_length(parse->rtable);
    OffsetVarNodes((Node*)subselect, rtoffset, 0);
    OffsetVarNodes(whereClause, rtoffset, 0);

    if (subselect->hintState) {
        /* pull up hint */
        pull_up_subquery_hint(root, parse, subselect->hintState);
    }

    /*
     * Upper-level vars in subquery will now be one level closer to their
     * parent than before; in particular, anything that had been level 1
     * becomes level zero.
     */
    IncrementVarSublevelsUp((Node*)subselect, -1, 1);
    IncrementVarSublevelsUp(whereClause, -1, 1);

    /*
     * Now that the WHERE clause is adjusted to match the parent query
     * environment, we can easily identify all the level-zero rels it uses.
     * The ones <= rtoffset belong to the upper query; the ones > rtoffset do
     * not.
     */
    clause_varnos = pull_varnos(whereClause);
    upper_varnos = NULL;
    while ((varno = bms_first_member(clause_varnos)) >= 0) {
        if (varno <= rtoffset)
            upper_varnos = bms_add_member(upper_varnos, varno);
    }
    bms_free_ext(clause_varnos);
    AssertEreport(!bms_is_empty(upper_varnos), MOD_OPT_REWRITE, "Vars from upper query level should be included");

    /*
     * Now that we've got the set of upper-level varnos, we can make the last
     * check: only available_rels can be referenced.
     */
    if (!bms_is_subset(upper_varnos, available_rels))
        return NULL;

    /* Now we can attach the modified subquery rtable to the parent */
    parse->rtable = list_concat(parse->rtable, subselect->rtable);

    /*
     * And finally, build the JoinExpr node.
     */
    result = makeNode(JoinExpr);
    result->jointype = under_not ? JOIN_ANTI : JOIN_SEMI;
    result->isNatural = false;
    result->larg = NULL; /* caller must fill this in */
    /* flatten out the FromExpr node if it's useless */
    if (list_length(subselect->jointree->fromlist) == 1)
        result->rarg = (Node*)linitial(subselect->jointree->fromlist);
    else
        result->rarg = (Node*)subselect->jointree;
    result->usingClause = NIL;
    result->quals = whereClause;
    result->alias = NULL;
    result->rtindex = 0; /* we don't need an RTE for it */

    /* Mark query's can_push */
    mark_parent_child_pushdown_flag(root->parse, subselect);

    return result;
}

/*
 * simplify_EXISTS_query: remove any useless stuff in an EXISTS's subquery
 *
 * The only thing that matters about an EXISTS query is whether it returns
 * zero or more than zero rows.  Therefore, we can remove certain SQL features
 * that won't affect that.  The only part that is really likely to matter in
 * typical usage is simplifying the targetlist: it's a common habit to write
 * "SELECT * FROM" even though there is no need to evaluate any columns.
 *
 * Note: by suppressing the targetlist we could cause an observable behavioral
 * change, namely that any errors that might occur in evaluating the tlist
 * won't occur, nor will other side-effects of volatile functions.  This seems
 * unlikely to bother anyone in practice.
 *
 * Returns TRUE if was able to discard the targetlist, else FALSE.
 */
static bool simplify_EXISTS_query(Query* query)
{
    /*
     * We don't try to simplify at all if the query uses set operations,
     * aggregates, grouping sets, modifying CTEs, HAVING, OFFSET, or FOR
     * UPDATE/SHARE; none of these seem likely in normal usage and their
     * possible effects are complex.  (Note: we could ignore an "OFFSET 0"
     * clause, but that traditionally is used as an optimization fence, so we
     * don't.)
     */
    if (query->commandType != CMD_SELECT || query->setOperations || query->hasAggs || query->groupingSets ||
        query->hasWindowFuncs || query->hasModifyingCTE || query->havingQual || query->limitOffset ||
        query->limitCount || query->rowMarks)
        return false;

    /*
     * Mustn't throw away the targetlist if it contains set-returning
     * functions; those could affect whether zero rows are returned!
     */
    if (expression_returns_set((Node*)query->targetList))
        return false;

    /*
     * Otherwise, we can throw away the targetlist, as well as any GROUP,
     * WINDOW, DISTINCT, and ORDER BY clauses; none of those clauses will
     * change a nonzero-rows result to zero rows or vice versa.  (Furthermore,
     * since our parsetree representation of these clauses depends on the
     * targetlist, we'd better throw them away if we drop the targetlist.)
     */
    query->targetList = NIL;
    query->groupClause = NIL;
    query->windowClause = NIL;
    query->distinctClause = NIL;
    query->sortClause = NIL;
    query->hasDistinctOn = false;

    return true;
}

/*
 * convert_EXISTS_to_ANY: try to convert EXISTS to a hashable ANY sublink
 *
 * The subselect is expected to be a fresh copy that we can munge up,
 * and to have been successfully passed through simplify_EXISTS_query.
 *
 * On success, the modified subselect is returned, and we store a suitable
 * upper-level test expression at *testexpr, plus a list of the subselect's
 * output Params at *paramIds.	(The test expression is already Param-ified
 * and hence need not go through convert_testexpr, which is why we have to
 * deal with the Param IDs specially.)
 *
 * On failure, returns NULL.
 */
static Query* convert_EXISTS_to_ANY(PlannerInfo* root, Query* subselect, Node** testexpr, List** paramIds)
{
    Node* whereClause = NULL;
    List* leftargs = NIL;
    List* rightargs = NIL;
    List* opids = NIL;
    List* opcollations = NIL;
    List* newWhere = NIL;
    List* tlist = NIL;
    List* testlist = NIL;
    List* paramids = NIL;
    ListCell* lc = NULL;
    ListCell* rc = NULL;
    ListCell* oc = NULL;
    ListCell* cc = NULL;
    AttrNumber resno;

    /*
     * Query must not require a targetlist, since we have to insert a new one.
     * Caller should have dealt with the case already.
     */
    AssertEreport(
        subselect->targetList == NIL, MOD_OPT_REWRITE, "Targetlist of rewritten query should already be set to null");

    /*
     * Separate out the WHERE clause.  (We could theoretically also remove
     * top-level plain JOIN/ON clauses, but it's probably not worth the
     * trouble.)
     */
    whereClause = subselect->jointree->quals;
    subselect->jointree->quals = NULL;

    /*
     * The rest of the sub-select must not refer to any Vars of the parent
     * query.  (Vars of higher levels should be okay, though.)
     *
     * Note: we need not check for Aggrefs separately because we know the
     * sub-select is as yet unoptimized; any uplevel Aggref must therefore
     * contain an uplevel Var reference.  This is not the case below ...
     */
    if (contain_vars_of_level((Node*)subselect, 1))
        return NULL;

    /*
     * We don't risk optimizing if the WHERE clause is volatile, either.
     */
    if (contain_volatile_functions(whereClause))
        return NULL;

    /*
     * Clean up the WHERE clause by doing const-simplification etc on it.
     * Aside from simplifying the processing we're about to do, this is
     * important for being able to pull chunks of the WHERE clause up into the
     * parent query.  Since we are invoked partway through the parent's
     * preprocess_expression() work, earlier steps of preprocess_expression()
     * wouldn't get applied to the pulled-up stuff unless we do them here. For
     * the parts of the WHERE clause that get put back into the child query,
     * this work is partially duplicative, but it shouldn't hurt.
     *
     * Note: we do not run flatten_join_alias_vars.  This is OK because any
     * parent aliases were flattened already, and we're not going to pull any
     * child Vars (of any description) into the parent.
     *
     * Note: passing the parent's root to eval_const_expressions is
     * technically wrong, but we can get away with it since only the
     * boundParams (if any) are used, and those would be the same in a
     * subroot.
     */
    whereClause = eval_const_expressions(root, whereClause);
    whereClause = (Node*)canonicalize_qual((Expr*)whereClause, false);
    whereClause = (Node*)make_ands_implicit((Expr*)whereClause);

    /*
     * We now have a flattened implicit-AND list of clauses, which we try to
     * break apart into "outervar = innervar" hash clauses. Anything that
     * can't be broken apart just goes back into the newWhere list.  Note that
     * we aren't trying hard yet to ensure that we have only outer or only
     * inner on each side; we'll check that if we get to the end.
     */
    leftargs = rightargs = opids = opcollations = newWhere = NIL;
    foreach (lc, (List*)whereClause) {
        OpExpr* expr = (OpExpr*)lfirst(lc);

        if (IsA(expr, OpExpr) && hash_ok_operator(expr)) {
            Node* leftarg = (Node*)linitial(expr->args);
            Node* rightarg = (Node*)lsecond(expr->args);

            if (contain_vars_of_level(leftarg, 1)) {
                leftargs = lappend(leftargs, leftarg);
                rightargs = lappend(rightargs, rightarg);
                opids = lappend_oid(opids, expr->opno);
                opcollations = lappend_oid(opcollations, expr->inputcollid);
                continue;
            }
            if (contain_vars_of_level(rightarg, 1)) {
                /*
                 * We must commute the clause to put the outer var on the
                 * left, because the hashing code in nodeSubplan.c expects
                 * that.  This probably shouldn't ever fail, since hashable
                 * operators ought to have commutators, but be paranoid.
                 */
                expr->opno = get_commutator(expr->opno);
                if (OidIsValid(expr->opno) && hash_ok_operator(expr)) {
                    leftargs = lappend(leftargs, rightarg);
                    rightargs = lappend(rightargs, leftarg);
                    opids = lappend_oid(opids, expr->opno);
                    opcollations = lappend_oid(opcollations, expr->inputcollid);
                    continue;
                }
                /* If no commutator, no chance to optimize the WHERE clause */
                return NULL;
            }
        }
        /* Couldn't handle it as a hash clause */
        newWhere = lappend(newWhere, expr);
    }

    /*
     * If we didn't find anything we could convert, fail.
     */
    if (leftargs == NIL)
        return NULL;

    /*
     * There mustn't be any parent Vars or Aggs in the stuff that we intend to
     * put back into the child query.  Note: you might think we don't need to
     * check for Aggs separately, because an uplevel Agg must contain an
     * uplevel Var in its argument.  But it is possible that the uplevel Var
     * got optimized away by eval_const_expressions.  Consider
     *
     * SUM(CASE WHEN false THEN uplevelvar ELSE 0 END)
     */
    if (contain_vars_of_level((Node*)newWhere, 1) || contain_vars_of_level((Node*)rightargs, 1))
        return NULL;
    if (root->parse->hasAggs &&
        (contain_aggs_of_level((Node*)newWhere, 1) || contain_aggs_of_level((Node*)rightargs, 1)))
        return NULL;

    /*
     * And there can't be any child Vars in the stuff we intend to pull up.
     * (Note: we'd need to check for child Aggs too, except we know the child
     * has no aggs at all because of simplify_EXISTS_query's check. The same
     * goes for window functions.)
     */
    if (contain_vars_of_level((Node*)leftargs, 0))
        return NULL;

    /*
     * Also reject sublinks in the stuff we intend to pull up.	(It might be
     * possible to support this, but doesn't seem worth the complication.)
     */
    if (contain_subplans((Node*)leftargs))
        return NULL;

    /*
     * Okay, adjust the sublevelsup in the stuff we're pulling up.
     */
    IncrementVarSublevelsUp((Node*)leftargs, -1, 1);

    /*
     * Put back any child-level-only WHERE clauses.
     */
    if (newWhere != NULL)
        subselect->jointree->quals = (Node*)make_ands_explicit(newWhere);

    /*
     * Build a new targetlist for the child that emits the expressions we
     * need.  Concurrently, build a testexpr for the parent using Params to
     * reference the child outputs.  (Since we generate Params directly here,
     * there will be no need to convert the testexpr in build_subplan.)
     */
    tlist = testlist = paramids = NIL;
    resno = 1;
    /* there's no "forfour" so we have to chase one of the lists manually */
    cc = list_head(opcollations);
    forthree(lc, leftargs, rc, rightargs, oc, opids)
    {
        Node* leftarg = (Node*)lfirst(lc);
        Node* rightarg = (Node*)lfirst(rc);
        Oid opid = lfirst_oid(oc);
        Oid opcollation = lfirst_oid(cc);
        Param* param = NULL;

        cc = lnext(cc);
        param = generate_new_param(root, exprType(rightarg), exprTypmod(rightarg), exprCollation(rightarg));
        tlist = lappend(tlist, makeTargetEntry((Expr*)rightarg, resno++, NULL, false));
        testlist = lappend(
            testlist, make_opclause(opid, BOOLOID, false, (Expr*)leftarg, (Expr*)param, InvalidOid, opcollation));
        paramids = lappend_int(paramids, param->paramid);
    }

    /* Put everything where it should go, and we're done */
    subselect->targetList = tlist;
    *testexpr = (Node*)make_ands_explicit(testlist);
    *paramIds = paramids;

    return subselect;
}

/*
 * Replace correlation vars (uplevel vars) with Params.
 *
 * Uplevel PlaceHolderVars and aggregates are replaced, too.
 *
 * Note: it is critical that this runs immediately after SS_process_sublinks.
 * Since we do not recurse into the arguments of uplevel PHVs and aggregates,
 * they will get copied to the appropriate subplan args list in the parent
 * query with uplevel vars not replaced by Params, but only adjusted in level
 * (see replace_outer_placeholdervar and replace_outer_agg).  That's exactly
 * what we want for the vars of the parent level --- but if a PHV's or
 * aggregate's argument contains any further-up variables, they have to be
 * replaced with Params in their turn. That will happen when the parent level
 * runs SS_replace_correlation_vars.  Therefore it must do so after expanding
 * its sublinks to subplans.  And we don't want any steps in between, else
 * those steps would never get applied to the argument expressions, either in
 * the parent or the child level.
 *
 * Another fairly tricky thing going on here is the handling of SubLinks in
 * the arguments of uplevel PHVs/aggregates.  Those are not touched inside the
 * intermediate query level, either.  Instead, SS_process_sublinks recurses on
 * them after copying the PHV or Aggref expression into the parent plan level
 * (this is actually taken care of in build_subplan).
 */
Node* SS_replace_correlation_vars(PlannerInfo* root, Node* expr)
{
    /* No setup needed for tree walk, so away we go */
    return replace_correlation_vars_mutator(expr, root);
}

static Node* replace_correlation_vars_mutator(Node* node, PlannerInfo* root)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        if (((Var*)node)->varlevelsup > 0)
            return (Node*)replace_outer_var(root, (Var*)node);
    }
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup > 0)
            return (Node*)replace_outer_placeholdervar(root, (PlaceHolderVar*)node);
    }
    if (IsA(node, Aggref)) {
        if (((Aggref*)node)->agglevelsup > 0)
            return (Node*)replace_outer_agg(root, (Aggref*)node);
    }
    if (IsA(node, GroupingFunc)) {
        if (((GroupingFunc*)node)->agglevelsup > 0)
            return (Node*)replace_outer_grouping(root, (GroupingFunc*)node);
    }
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) replace_correlation_vars_mutator, (void*)root);
}

/*
 * Expand SubLinks to SubPlans in the given expression.
 *
 * The isQual argument tells whether or not this expression is a WHERE/HAVING
 * qualifier expression.  If it is, any sublinks appearing at top level need
 * not distinguish FALSE from UNKNOWN return values.
 */
Node* SS_process_sublinks(PlannerInfo* root, Node* expr, bool isQual)
{
    process_sublinks_context context;

    context.root = root;
    context.isTopQual = isQual;
    return process_sublinks_mutator(expr, &context);
}

static Node* process_sublinks_mutator(Node* node, process_sublinks_context* context)
{
    process_sublinks_context locContext;

    locContext.root = context->root;
    locContext.isTopQual = false;

    if (node == NULL)
        return NULL;
    if (IsA(node, SubLink)) {
        SubLink* sublink = (SubLink*)node;
        Node* testexpr = NULL;

        /*
         * First, recursively process the lefthand-side expressions, if any.
         * They're not top-level anymore.
         */
        locContext.isTopQual = false;
        testexpr = process_sublinks_mutator(sublink->testexpr, &locContext);

        /*
         * Now build the SubPlan node and make the expr to return.
         */
        return make_subplan(
            context->root, (Query*)sublink->subselect, sublink->subLinkType, testexpr, context->isTopQual);
    }

    /*
     * Don't recurse into the arguments of an outer PHV or aggregate here. Any
     * SubLinks in the arguments have to be dealt with at the outer query
     * level; they'll be handled when build_subplan collects the PHV or Aggref
     * into the arguments to be passed down to the current subplan.
     */
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup > 0)
            return node;
    } else if (IsA(node, Aggref)) {
        if (((Aggref*)node)->agglevelsup > 0)
            return node;
    }

    /*
     * We should never see a SubPlan expression in the input (since this is
     * the very routine that creates 'em to begin with). But if this query is
     * copied when doing full join rewritting, this is OK.
     * We shouldn't find ourselves invoked directly on a Query, either.
     */
    if (!context->root->parse->is_from_full_join_rewrite) {
        AssertEreport(!IsA(node, SubPlan), MOD_OPT_SUBPLAN, "A subplan expression shouldn't exist in the input");
        AssertEreport(
            !IsA(node, AlternativeSubPlan), MOD_OPT_SUBPLAN, "AlternativeSubPlan shouldn't exist in the input");
    }
    AssertEreport(!IsA(node, Query), MOD_OPT_SUBPLAN, "We shouldn't invoke directly on a Query");

    /*
     * Because make_subplan() could return an AND or OR clause, we have to
     * take steps to preserve AND/OR flatness of a qual.  We assume the input
     * has been AND/OR flattened and so we need no recursion here.
     *
     * (Due to the coding here, we will not get called on the List subnodes of
     * an AND; and the input is *not* yet in implicit-AND format.  So no check
     * is needed for a bare List.)
     *
     * Anywhere within the top-level AND/OR clause structure, we can tell
     * make_subplan() that NULL and FALSE are interchangeable.	So isTopQual
     * propagates down in both cases.  (Note that this is unlike the meaning
     * of "top level qual" used in most other places in Postgres.)
     */
    if (and_clause(node)) {
        List* newargs = NIL;
        ListCell* l = NULL;

        /* Still at qual top-level */
        locContext.isTopQual = context->isTopQual;

        foreach (l, ((BoolExpr*)node)->args) {
            Node* newarg = NULL;

            newarg = process_sublinks_mutator((Node*)lfirst(l), &locContext);
            if (and_clause(newarg))
                newargs = list_concat(newargs, ((BoolExpr*)newarg)->args);
            else
                newargs = lappend(newargs, newarg);
        }
        return (Node*)make_andclause(newargs);
    }

    if (or_clause(node)) {
        List* newargs = NIL;
        ListCell* l = NULL;

        /* Still at qual top-level */
        locContext.isTopQual = context->isTopQual;

        foreach (l, ((BoolExpr*)node)->args) {
            Node* newarg = NULL;

            newarg = process_sublinks_mutator((Node*)lfirst(l), &locContext);
            if (newarg == NULL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        (errmsg("Fail to process sublinks mutator."))));
            if (or_clause(newarg))
                newargs = list_concat(newargs, ((BoolExpr*)newarg)->args);
            else
                newargs = lappend(newargs, newarg);
        }
        return (Node*)make_orclause(newargs);
    }

    /*
     * If we recurse down through anything other than an AND or OR node, we
     * are definitely not at top qual level anymore.
     */
    locContext.isTopQual = false;

    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) process_sublinks_mutator, (void*)&locContext);
}

/*
 * SS_finalize_plan - do final sublink and parameter processing for a
 * completed Plan.
 *
 * This recursively computes the extParam and allParam sets for every Plan
 * node in the given plan tree.  It also optionally attaches any previously
 * generated InitPlans to the top plan node.  (Any InitPlans should already
 * have been put through SS_finalize_plan.)
 */
void SS_finalize_plan(PlannerInfo* root, Plan* plan, bool attach_initplans)
{
    Bitmapset* valid_params = NULL;
    Bitmapset* initExtParam = NULL;
    Bitmapset* initSetParam = NULL;
    Cost initplan_cost;
    PlannerInfo* proot = NULL;
    ListCell* l = NULL;

    /*
     * Examine any initPlans to determine the set of external params they
     * reference, the set of output params they supply, and their total cost.
     * We'll use at least some of this info below.  (Note we are assuming that
     * finalize_plan doesn't touch the initPlans.)
     *
     * In the case where attach_initplans is false, we are assuming that the
     * existing initPlans are siblings that might supply params needed by the
     * current plan.
     */
    initExtParam = initSetParam = NULL;
    initplan_cost = 0;
    foreach (l, root->init_plans) {
        SubPlan* initsubplan = (SubPlan*)lfirst(l);
        Plan* initplan = planner_subplan_get_plan(root, initsubplan);
        ListCell* l2 = NULL;

        initExtParam = bms_add_members(initExtParam, initplan->extParam);
        foreach (l2, initsubplan->setParam) {
            initSetParam = bms_add_member(initSetParam, lfirst_int(l2));
        }
        initplan_cost += initsubplan->startup_cost + initsubplan->per_call_cost;
    }

    /*
     * Now determine the set of params that are validly referenceable in this
     * query level; to wit, those available from outer query levels plus the
     * output parameters of any local initPlans.  (We do not include output
     * parameters of regular subplans.	Those should only appear within the
     * testexpr of SubPlan nodes, and are taken care of locally within
     * finalize_primnode.  Likewise, special parameters that are generated by
     * nodes such as ModifyTable are handled within finalize_plan.)
     */
    valid_params = bms_copy(initSetParam);
    for (proot = root->parent_root; proot != NULL; proot = proot->parent_root) {
        /* Include ordinary Var/PHV/Aggref params */
        foreach (l, proot->plan_params) {
            PlannerParamItem* pitem = (PlannerParamItem*)lfirst(l);

            valid_params = bms_add_member(valid_params, pitem->paramId);
        }
        /* Include any outputs of outer-level initPlans */
        foreach (l, proot->init_plans) {
            SubPlan* initsubplan = (SubPlan*)lfirst(l);
            ListCell* l2 = NULL;

            foreach (l2, initsubplan->setParam) {
                valid_params = bms_add_member(valid_params, lfirst_int(l2));
            }
        }
        /* Include worktable ID, if a recursive query is being planned */
        if (proot->wt_param_id >= 0)
            valid_params = bms_add_member(valid_params, proot->wt_param_id);
    }

    /*
     * Now recurse through plan tree.
     */
    (void)finalize_plan(root, plan, valid_params, NULL);

    bms_free_ext(valid_params);

    /*
     * Finally, attach any initPlans to the topmost plan node, and add their
     * extParams to the topmost node's, too.  However, any setParams of the
     * initPlans should not be present in the topmost node's extParams, only
     * in its allParams.  (As of PG 8.1, it's possible that some initPlans
     * have extParams that are setParams of other initPlans, so we have to
     * take care of this situation explicitly.)
     *
     * We also add the eval cost of each initPlan to the startup cost of the
     * top node.  This is a conservative overestimate, since in fact each
     * initPlan might be executed later than plan startup, or even not at all.
     */
    if (attach_initplans) {
        plan->initPlan = root->init_plans;
        root->init_plans = NIL; /* make sure they're not attached twice */

        /* allParam must include all these params */
        plan->allParam = bms_add_members(plan->allParam, initExtParam);
        plan->allParam = bms_add_members(plan->allParam, initSetParam);
        /* extParam must include any child extParam */
        plan->extParam = bms_add_members(plan->extParam, initExtParam);
        /* but extParam shouldn't include any setParams */
        plan->extParam = bms_del_members(plan->extParam, initSetParam);
        /* ensure extParam is exactly NULL if it's empty */
        if (bms_is_empty(plan->extParam))
            plan->extParam = NULL;

        plan->startup_cost += initplan_cost;
        plan->total_cost += initplan_cost;
    }
}

/*
 * @Description: finalize_agg_primnode: find all Aggref nodes in the given expression tree,
 * and add IDs of all PARAM_EXEC params appearing within their aggregated
 * arguments to the result set.
 * @in node: Need judge node.
 * @out context: Keep paramids struct.
 */
static bool finalize_agg_primnode(Node* node, finalize_primnode_context* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Aggref)) {
        Aggref* agg = (Aggref*)node;

        /* we should not consider the direct arguments, if any */
        (void)finalize_primnode((Node*)agg->args, context);
        return false; /* there can't be any Aggrefs below here */
    }

    return expression_tree_walker(node, (bool (*)())finalize_agg_primnode, (void*)context);
}

static void finalize_plans(
    PlannerInfo* root, finalize_primnode_context* context, List* plans, Bitmapset* valid_params, Bitmapset* scan_params)
{
    ListCell* lc = NULL;

    foreach (lc, plans) {
        context->paramids =
            bms_add_members(context->paramids, finalize_plan(root, (Plan*)lfirst(lc), valid_params, scan_params));
    }
}

/*
 * Recursive processing of all nodes in the plan tree
 *
 * valid_params is the set of param IDs considered valid to reference in
 * this plan node or its children.
 * scan_params is a set of param IDs to force scan plan nodes to reference.
 * This is for EvalPlanQual support, and is always NULL at the top of the
 * recursion.
 *
 * The return value is the computed allParam set for the given Plan node.
 * This is just an internal notational convenience.
 */
static Bitmapset* finalize_plan(PlannerInfo* root, Plan* plan, Bitmapset* valid_params, Bitmapset* scan_params)
{
    finalize_primnode_context context;
    int locally_added_param;
    Bitmapset* nestloop_params = NULL;
    Bitmapset* child_params = NULL;

    if (plan == NULL)
        return NULL;

    context.root = root;
    context.paramids = NULL;  /* initialize set to empty */
    locally_added_param = -1; /* there isn't one */
    nestloop_params = NULL;   /* there aren't any */

    /*
     * When we call finalize_primnode, context.paramids sets are automatically
     * merged together.  But when recursing to self, we have to do it the hard
     * way.  We want the paramids set to include params in subplans as well as
     * at this level.
     */
    /* Find params in targetlist and qual */
    (void)finalize_primnode((Node*)plan->targetlist, &context);
    (void)finalize_primnode((Node*)plan->qual, &context);

    /* Check additional node-type-specific fields */
    switch (nodeTag(plan)) {
        case T_BaseResult:
            (void)finalize_primnode(((BaseResult*)plan)->resconstantqual, &context);
            break;

        case T_SeqScan:
        case T_CStoreScan:
        case T_DfsScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            if (((Scan*)plan)->tablesample) {
                (void)finalize_primnode((Node*)((Scan*)plan)->tablesample, &context);
            }
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_IndexScan:
            (void)finalize_primnode((Node*)((IndexScan*)plan)->indexqual, &context);
            (void)finalize_primnode((Node*)((IndexScan*)plan)->indexorderby, &context);

            /*
             * we need not look at indexqualorig, since it will have the same
             * param references as indexqual.  Likewise, we can ignore
             * indexorderbyorig.
             */
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_IndexOnlyScan:
            (void)finalize_primnode((Node*)((IndexOnlyScan*)plan)->indexqual, &context);
            (void)finalize_primnode((Node*)((IndexOnlyScan*)plan)->indexorderby, &context);

            /*
             * we need not look at indextlist, since it cannot contain Params.
             */
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_BitmapIndexScan:
            (void)finalize_primnode((Node*)((BitmapIndexScan*)plan)->indexqual, &context);

            /*
             * we need not look at indexqualorig, since it will have the same
             * param references as indexqual.
             */
            break;

        case T_BitmapHeapScan:
            (void)finalize_primnode((Node*)((BitmapHeapScan*)plan)->bitmapqualorig, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_CStoreIndexScan:
            (void)finalize_primnode((Node*)((CStoreIndexScan*)plan)->indexqual, &context);
            (void)finalize_primnode((Node*)((CStoreIndexScan*)plan)->indexorderby, &context);
            (void)finalize_primnode((Node*)((CStoreIndexScan*)plan)->cstorequal, &context);

            context.paramids = bms_add_members(context.paramids, scan_params);
            break;
        case T_DfsIndexScan:
            (void)finalize_primnode((Node*)((DfsIndexScan*)plan)->indexqual, &context);
            (void)finalize_primnode((Node*)((DfsIndexScan*)plan)->indexorderby, &context);
            (void)finalize_primnode((Node*)((DfsIndexScan*)plan)->cstorequal, &context);

            context.paramids = bms_add_members(context.paramids, scan_params);
            break;
        case T_CStoreIndexCtidScan:
            (void)finalize_primnode((Node*)((CStoreIndexCtidScan*)plan)->indexqual, &context);

            context.paramids = bms_add_members(context.paramids, scan_params);
            break;
        case T_CStoreIndexHeapScan:
            (void)finalize_primnode((Node*)((CStoreIndexHeapScan*)plan)->bitmapqualorig, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_TidScan:
            (void)finalize_primnode((Node*)((TidScan*)plan)->tidquals, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_SubqueryScan:

            /*
             * In a SubqueryScan, SS_finalize_plan has already been run on the
             * subplan by the inner invocation of subquery_planner, so there's
             * no need to do it again.	Instead, just pull out the subplan's
             * extParams list, which represents the params it needs from my
             * level and higher levels.
             */
            context.paramids = bms_add_members(context.paramids, ((SubqueryScan*)plan)->subplan->extParam);
            /* We need scan_params too, though */
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_FunctionScan:
            (void)finalize_primnode(((FunctionScan*)plan)->funcexpr, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_ValuesScan:
            (void)finalize_primnode((Node*)((ValuesScan*)plan)->values_lists, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_CteScan: {
            /*
             * You might think we should add the node's cteParam to
             * paramids, but we shouldn't because that param is just a
             * linkage mechanism for multiple CteScan nodes for the same
             * CTE; it is never used for changed-param signaling.  What we
             * have to do instead is to find the referenced CTE plan and
             * incorporate its external paramids, so that the correct
             * things will happen if the CTE references outer-level
             * variables.  See test cases for bug #4902.
             */
            int plan_id = ((CteScan*)plan)->ctePlanId;
            Plan* cteplan = NULL;

            /* so, do this ... */
            if (plan_id < 1 || plan_id > list_length(root->glob->subplans))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("could not find plan for CteScan referencing plan ID %d", plan_id)));
            cteplan = (Plan*)list_nth(root->glob->subplans, plan_id - 1);
            context.paramids = bms_add_members(context.paramids, cteplan->extParam);

#ifdef NOT_USED
            /* ... but not this */
            context.paramids = bms_add_member(context.paramids, ((CteScan*)plan)->cteParam);
#endif

            context.paramids = bms_add_members(context.paramids, scan_params);
        } break;

        case T_WorkTableScan:
            context.paramids = bms_add_member(context.paramids, ((WorkTableScan*)plan)->wtParam);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_ForeignScan:
            (void)finalize_primnode((Node*)((ForeignScan*)plan)->fdw_exprs, &context);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_ExtensiblePlan: {
            ExtensiblePlan* cscan = (ExtensiblePlan*)plan;

            (void)finalize_primnode((Node*)cscan->extensible_exprs, &context);
            /* We assume extensible_plan_tlist cannot contain Params */
            context.paramids = bms_add_members(context.paramids, scan_params);

            /* child nodes if any */
            finalize_plans(root, &context, cscan->extensible_plans, valid_params, scan_params);
        } break;

        case T_ModifyTable: {
            ModifyTable* mtplan = (ModifyTable*)plan;

            /* Force descendant scan nodes to reference epqParam */
            locally_added_param = mtplan->epqParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            scan_params = bms_add_member(bms_copy(scan_params), locally_added_param);
            (void)finalize_primnode((Node*)mtplan->returningLists, &context);
            (void)finalize_primnode((Node*)mtplan->updateTlist, &context);
            (void)finalize_primnode((Node*)mtplan->upsertWhere, &context);
            finalize_plans(root, &context, mtplan->plans, valid_params, scan_params);
        } break;
#ifdef PGXC
        case T_RemoteQuery:
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;
#endif

        case T_Append: {
            finalize_plans(root, &context, ((Append*)plan)->appendplans, valid_params, scan_params);
        } break;

        case T_MergeAppend: {
            finalize_plans(root, &context, ((MergeAppend*)plan)->mergeplans, valid_params, scan_params);
        } break;

        case T_BitmapAnd: {
            finalize_plans(root, &context, ((BitmapAnd*)plan)->bitmapplans, valid_params, scan_params);

        } break;

        case T_BitmapOr: {
            finalize_plans(root, &context, ((BitmapOr*)plan)->bitmapplans, valid_params, scan_params);
        } break;

        case T_CStoreIndexAnd: {
            finalize_plans(root, &context, ((CStoreIndexAnd*)plan)->bitmapplans, valid_params, scan_params);
        } break;
        case T_CStoreIndexOr: {
            finalize_plans(root, &context, ((CStoreIndexOr*)plan)->bitmapplans, valid_params, scan_params);
        } break;
        case T_NestLoop: {
            ListCell* l = NULL;

            (void)finalize_primnode((Node*)((Join*)plan)->joinqual, &context);
            /* collect set of params that will be passed to right child */
            foreach (l, ((NestLoop*)plan)->nestParams) {
                NestLoopParam* nlp = (NestLoopParam*)lfirst(l);

                nestloop_params = bms_add_member(nestloop_params, nlp->paramno);
            }
        } break;

        case T_MergeJoin:
            (void)finalize_primnode((Node*)((Join*)plan)->joinqual, &context);
            (void)finalize_primnode((Node*)((MergeJoin*)plan)->mergeclauses, &context);
            break;

        case T_HashJoin: {
            (void)finalize_primnode((Node*)((Join*)plan)->joinqual, &context);
            (void)finalize_primnode((Node*)((HashJoin*)plan)->hashclauses, &context);

            /*
             * If there's already params, then check if there's param in hashkey
             * of inner table, then we should rebuild hashtable since hash key
             * changes for every rescan
             */
            if (!bms_is_empty(context.paramids)) {
                ListCell* lc = NULL;
                List* inner_hash_clause = NIL;
                foreach (lc, ((HashJoin*)plan)->hashclauses) {
                    OpExpr* hclause = (OpExpr*)lfirst(lc);

                    AssertEreport(IsA(hclause, OpExpr), MOD_OPT_SUBPLAN, "hashjoin clause should be opExpr");
                    inner_hash_clause = lappend(inner_hash_clause, lsecond(hclause->args));
                }
                finalize_primnode_context inner_context;
                inner_context.root = root;
                inner_context.paramids = NULL; /* initialize set to empty */
                (void)finalize_primnode((Node*)inner_hash_clause, &inner_context);
                list_free_ext(inner_hash_clause);
                if (inner_context.paramids != NULL) {
                    ((HashJoin*)plan)->rebuildHashTable = true;
                    bms_free_ext(inner_context.paramids);
                }
            }
        } break;

        case T_Limit:
            (void)finalize_primnode(((Limit*)plan)->limitOffset, &context);
            (void)finalize_primnode(((Limit*)plan)->limitCount, &context);
            break;

        case T_RecursiveUnion:
            /* child nodes are allowed to reference wtParam */
            locally_added_param = ((RecursiveUnion*)plan)->wtParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            /* wtParam does *not* get added to scan_params */
            break;

        case T_LockRows:
            /* Force descendant scan nodes to reference epqParam */
            locally_added_param = ((LockRows*)plan)->epqParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            scan_params = bms_add_member(bms_copy(scan_params), locally_added_param);
            break;

        case T_Agg: {
            Agg* agg = (Agg*)plan;

            /*
             * AGG_HASHED plans need to know which Params are referenced
             * in aggregate calls.  Do a separate scan to identify them.
             */
            if (agg->aggstrategy == AGG_HASHED || agg->aggstrategy == AGG_PLAIN) {
                finalize_primnode_context aggcontext;

                aggcontext.root = root;
                aggcontext.paramids = NULL;
                finalize_agg_primnode((Node*)agg->plan.targetlist, &aggcontext);
                finalize_agg_primnode((Node*)agg->plan.qual, &aggcontext);
                agg->aggParams = aggcontext.paramids;
            }
        } break;

        case T_WindowAgg:
            (void)finalize_primnode(((WindowAgg*)plan)->startOffset, &context);
            (void)finalize_primnode(((WindowAgg*)plan)->endOffset, &context);
            break;

        case T_Hash:
        case T_Material:
        case T_Sort:
        case T_Unique:
        case T_SetOp:
        case T_Group:
        case T_Stream:
        case T_PartIterator:
        case T_StartWithOp:
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)nodeTag(plan))));
    }

    /* Process left and right child plans, if any */
    child_params = finalize_plan(root, plan->lefttree, valid_params, scan_params);
    context.paramids = bms_add_members(context.paramids, child_params);

    if (nestloop_params != NULL) {
        /* right child can reference nestloop_params as well as valid_params */
        child_params = finalize_plan(root, plan->righttree, bms_union(nestloop_params, valid_params), scan_params);
        /* ... and they don't count as parameters used at my level */
        child_params = bms_difference(child_params, nestloop_params);
        bms_free_ext(nestloop_params);
    } else {
        /* easy case */
        child_params = finalize_plan(root, plan->righttree, valid_params, scan_params);
    }
    context.paramids = bms_add_members(context.paramids, child_params);

    /*
     * Any locally generated parameter doesn't count towards its generating
     * plan node's external dependencies.  (Note: if we changed valid_params
     * and/or scan_params, we leak those bitmapsets; not worth the notational
     * trouble to clean them up.)
     */
    if (locally_added_param >= 0) {
        context.paramids = bms_del_member(context.paramids, locally_added_param);
    }

    /* Now we have all the paramids */
    if (!bms_is_subset(context.paramids, valid_params))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("plan should not reference subplan's variable")));

    /*
     * Note: by definition, extParam and allParam should have the same value
     * in any plan node that doesn't have child initPlans.  We set them equal
     * here, and later SS_finalize_plan will update them properly in node(s)
     * that it attaches initPlans to.
     *
     * For speed at execution time, make sure extParam/allParam are actually
     * NULL if they are empty sets.
     */
    if (bms_is_empty(context.paramids)) {
        plan->extParam = NULL;
        plan->allParam = NULL;
    } else {
        plan->extParam = context.paramids;
        plan->allParam = bms_copy(context.paramids);
    }

    return plan->allParam;
}

/*
 * finalize_primnode: add IDs of all PARAM_EXEC params appearing in the given
 * expression tree to the result set.
 */
static bool finalize_primnode(Node* node, finalize_primnode_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Param)) {
        if (((Param*)node)->paramkind == PARAM_EXEC) {
            int paramid = ((Param*)node)->paramid;

            context->paramids = bms_add_member(context->paramids, paramid);
        }
        return false; /* no more to do here */
    }
    if (IsA(node, SubPlan)) {
        SubPlan* subplan = (SubPlan*)node;
        Plan* plan = planner_subplan_get_plan(context->root, subplan);
        ListCell* lc = NULL;
        Bitmapset* subparamids = NULL;

        /* Recurse into the testexpr, but not into the Plan */
        (void)finalize_primnode(subplan->testexpr, context);

        /*
         * Remove any param IDs of output parameters of the subplan that were
         * referenced in the testexpr.	These are not interesting for
         * parameter change signaling since we always re-evaluate the subplan.
         * Note that this wouldn't work too well if there might be uses of the
         * same param IDs elsewhere in the plan, but that can't happen because
         * generate_new_param never tries to merge params.
         */
        foreach (lc, subplan->paramIds) {
            context->paramids = bms_del_member(context->paramids, lfirst_int(lc));
        }

        /* Also examine args list */
        (void)finalize_primnode((Node*)subplan->args, context);

        /*
         * Add params needed by the subplan to paramids, but excluding those
         * we will pass down to it.
         */
        subparamids = bms_copy(plan->extParam);
        foreach (lc, subplan->parParam) {
            subparamids = bms_del_member(subparamids, lfirst_int(lc));
        }
        context->paramids = bms_join(context->paramids, subparamids);

        return false; /* no more to do here */
    }
    return expression_tree_walker(node, (bool (*)())finalize_primnode, (void*)context);
}

/*
 * SS_make_initplan_from_plan - given a plan tree, make it an InitPlan
 *
 * The plan is expected to return a scalar value of the given type/collation.
 * We build an EXPR_SUBLINK SubPlan node and put it into the initplan
 * list for the current query level.  A Param that represents the initplan's
 * output is returned.
 *
 * We assume the plan hasn't been put through SS_finalize_plan.
 */
Param* SS_make_initplan_from_plan(
    PlannerInfo* root, Plan* plan, Oid resulttype, int32 resulttypmod, Oid resultcollation)
{
    SubPlan* node = NULL;
    Param* prm = NULL;

    /*
     * We must run SS_finalize_plan(), since that's normally done before a
     * subplan gets put into the initplan list.  Tell it not to attach any
     * pre-existing initplans to this one, since they are siblings not
     * children of this initplan.  (This is something else that could perhaps
     * be cleaner if we did extParam/allParam processing in setrefs.c instead
     * of here?  See notes for materialize_finished_plan.)
     */
    /*
     * Build extParam/allParam sets for plan nodes.
     */
    SS_finalize_plan(root, plan, false);

    /*
     * Add the subplan and its PlannerInfo to the global lists.
     */
    root->glob->subplans = lappend(root->glob->subplans, plan);
    root->glob->subroots = lappend(root->glob->subroots, root);

    /*
     * Create a SubPlan node and add it to the outer list of InitPlans. Note
     * it has to appear after any other InitPlans it might depend on (see
     * comments in ExecReScan).
     */
    node = makeNode(SubPlan);
    node->subLinkType = EXPR_SUBLINK;
    get_first_col_type(plan, &node->firstColType, &node->firstColTypmod, &node->firstColCollation);
    node->plan_id = list_length(root->glob->subplans);

    root->init_plans = lappend(root->init_plans, node);

    /*
     * The node can't have any inputs (since it's an initplan), so the
     * parParam and args lists remain empty.
     */
    cost_subplan(root, node, plan);

    /*
     * Make a Param that will be the subplan's output.
     */
    prm = generate_new_param(root, resulttype, resulttypmod, resultcollation);
    node->setParam = list_make1_int(prm->paramid);

    /* Label the subplan for EXPLAIN purposes */
    node->plan_name = (char*)palloc(PLANNAMELEN);
    errno_t errorno = EOK;
    errorno = sprintf_s(node->plan_name, PLANNAMELEN, "InitPlan %d (returns $%d)", node->plan_id, prm->paramid);
    securec_check_ss(errorno, "", "");

    return prm;
}

#ifdef STREAMPLAN
/*
 * convert_multi_count_distinct - Given a parse tree, convert it to target
 * parse tree if there's multi count(distinct) exprs.
 *
 * Since we can't support pull down of multi count(distinct) query, we should
 * change it to multi subqueries, with one count(distinct) expr in each subquery.
 * Then multi subqueries should be joined to the final query.
 */
void convert_multi_count_distinct(PlannerInfo* root)
{
    Query* parse = root->parse;
    AggClauseCosts agg_costs;
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    List* group_exprs = NIL;
    List* target_list = NIL;
    List* fromlist = NIL;
    List* new_tlist = parse->targetList;
    List* tlist = NIL;
    Node* quals = NULL;
    Node* havingquals = parse->havingQual;
    int resno = 1;
    int i;
    List* join_exprs = NIL;
    List* rtable = NIL;
    Oid* eq_op = NULL;
    Oid* collation = NULL;
    List* orig_target_list = NIL;
    List* varlist = NIL;
    List* simplevarlist = NIL;
    List* joinvarlist = NIL;

    if (parse->groupClause != NIL) {
        if (!u_sess->attr.attr_sql.enable_hashagg) {
            ereport(DEBUG2,
                (errmodule(MOD_OPT_REWRITE),
                    (errmsg("[Multi count(distinct) convert failure reason]: Enable_hashagg disabled."))));
            return;
        }
        if (!grouping_is_hashable(parse->groupClause)) {
            ereport(DEBUG2,
                (errmodule(MOD_OPT_REWRITE),
                    (errmsg(
                        "[Multi count(distinct) convert failure reason]: Group by columns is not hash supported."))));
            return;
        }
    }

    /*
     * Wo don't handle the query if it contains EC/random functions, because the EC/random
     * func may be executed more than once. But the results of EC/random func is volatile, that may
     * cause the query's result incorrect.
     */
    contain_func_context context =
        init_contain_func_context(list_make4_oid(ECEXTENSIONFUNCOID, ECHADOOPFUNCOID, RANDOMFUNCOID, NEXTVALFUNCOID));
    if (contains_specified_func((Node*)parse, &context)) {
        char* func_name = get_func_name(((FuncExpr*)linitial(context.func_exprs))->funcid);
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Multi count(distinct) convert failure reason]: %s functions contained.", func_name))));
        pfree_ext(func_name);
        list_free_ext(context.funcids);
        context.funcids = NIL;
        list_free_ext(context.func_exprs);
        context.func_exprs = NIL;
        return;
    }
    list_free_ext(context.funcids);
    context.funcids = NIL;

    /* If there are agg levelsup > 0 or sublink, count_agg_clauses doesn't handle it, so disable this */
    if (contain_specified_agg_clause((Node*)parse->targetList) || contain_specified_agg_clause(parse->havingQual)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg(
                    "[Multi count(distinct) convert failure reason]: Expr sublink or agg levelsup > 0 contained."))));
        return;
    }

    /* check if multi count(distinct) conversion is needed */
    errno_t errorno = EOK;
    errorno = memset_s(&agg_costs, sizeof(AggClauseCosts), '\0', sizeof(AggClauseCosts));
    securec_check(errorno, "", "");

    count_agg_clauses(root, (Node*)parse->targetList, &agg_costs);
    count_agg_clauses(root, parse->havingQual, &agg_costs);
    if (list_length(agg_costs.exprAggs) <= 1 || agg_costs.hasDnAggs || agg_costs.numOrderedAggs != 0 ||
        agg_costs.unhashable) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Multi count(distinct) convert failure reason]: With dn agg or ordered agg or unhashable "
                        "distinct node."))));
        return;
    }

    /* If there are too many count(distinct), we disable the conversion */
    if (parse->groupClause != NIL && list_length(agg_costs.exprAggs) > u_sess->attr.attr_sql.from_collapse_limit) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Multi count(distinct) convert failure reason]: Num of count(distinct) exceeds threshold."))));
        return;
    }
    if (parse->groupingSets != NIL && agg_costs.hasdctDnAggs) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Multi count(distinct) convert failure reason]: With grouping sets and distinct dn aggs."))));
        return;
    }

    /*
     * refuse to convert if there is recursive CTE in rtables, since recursive CTE will be
     * invoked more than once, which will be lead two problems
     * 1. fail to set plan refs since CTE plan will be set more than once
     * 2. CTEScan doesnot support shared-scan
     */
    ListCell* rt = NULL;
    foreach (rt, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);

        if (rte->rtekind == RTE_CTE) {
            int levelsup = rte->ctelevelsup;
            int equal_parse = (root->parse == parse) ? 0 : 1;
            PlannerInfo* cte_root = root;

            while (equal_parse < levelsup) {
                /* Find the query level that has the CTE */
                cte_root = cte_root->parent_root;
                levelsup--;
                if (cte_root == NULL) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            errmsg("bad levelsup for CTE \"%s\"", rte->ctename)));
                }
            }

            ListCell* lc_cte = NULL;
            foreach (lc_cte, cte_root->parse->cteList) {
                CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc_cte);

                /* refuse to convert if there is recursive CTE in rtables */
                if (strcmp(cte->ctename, rte->ctename) == 0 && cte->cterecursive) {
                    ereport(DEBUG2,
                        (errmodule(MOD_OPT_REWRITE),
                            (errmsg("[Multi count(distinct) convert failure reason]: With recursive CTE."))));
                    return;
                }
            }
        }
    }

    int group_groupId_len = 0;
    /* We need will groupingid append to targetlist and do equal operaotr to Ap function. */
    if (parse->groupClause) {
        if (parse->groupingSets) {
            group_groupId_len = list_length(parse->groupClause) + 1;
        } else {
            group_groupId_len = list_length(parse->groupClause);
        }
    }
    /* make group exprs for group by clause */
    if (parse->groupClause != NIL) {
        eq_op = (Oid*)palloc(sizeof(Oid) * group_groupId_len);
        collation = (Oid*)palloc(sizeof(Oid) * group_groupId_len);
        i = 0;
        foreach (lc, parse->groupClause) {
            SortGroupClause* clause = (SortGroupClause*)lfirst(lc);
            TargetEntry* tle = get_sortgroupclause_tle(clause, parse->targetList);
            if (tle == NULL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        (errmsg("Fail to get sort group clause."))));

            TargetEntry* new_tle = (TargetEntry*)copyObject(tle);
            if (parse->groupingSets || (!check_var_nonnullable(parse, (Node*)tle->expr)))
                root->join_null_info = lappend_int(root->join_null_info, i + 1);
            new_tle->resno = i + 1;
            new_tle->resjunk = false;
            if (new_tle->resname == NULL)
                new_tle->resname = "?column?";
            eq_op[i] = clause->eqop;
            collation[i] = exprCollation((Node*)tle->expr);
            group_exprs = lappend(group_exprs, new_tle);
            i++;
        }

        if (parse->groupingSets) {
            GroupingId* groupId = makeNode(GroupingId);
            TargetEntry* tle = makeTargetEntry((Expr*)groupId, list_length(group_exprs) + 1, "groupingid", false);
            group_exprs = lappend(group_exprs, tle);
            eq_op[i] = INT4EQOID;
            collation[i] = 0;
        }
    }

    /*
     * Invoke make_agg_var_list() to generate all agg expressions into tlist
     *
     * Before do that, we have to unify the var node processing in "targetlist",
     * "havingquals", "groupexprs", we need try to replace single-table var node
     * with join vars, otherwise in multi count-distinct optimization can not be
     * correctly processed  e.g
     *
     * create table t1(a1,b1,c1); --- a1:var(1,1) b1:var(1,2) c1:var(1,3)
     * create table t2(a2,b2,c2); --- a1:var(2,1) b1:var(2,2) c1:var(2,3)
     * t1 join t2 var(3,xx)
     *
     * select a1,                   <<< replace to var(3,1)
     *        count(distinct b1),   <<< replace to var(3,2)
     *        count(distinct b2)    <<< replace to var(3,5)
     * from t1 join t2 on t1.c1 = t2.c2
     * group by t1.a1
     */
    {
        foreach (lc, parse->targetList) {
            TargetEntry* tent = (TargetEntry*)lfirst(lc);
            /*
             * For joined vars in join expr, we need to find original table vars, since
             * it can be used in having clause, and we should replace single table var
             * with join var We should traverse into each expr to find join expr, and then
             * find its original vars. We concat the original vars and join vars into
             * a list, and use them to replace original vars in having quals first
             */
            List* simplevars = pull_var_clause(
                (Node*)tent->expr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true);
            ListCell* newcell = NULL;

            foreach (newcell, simplevars) {
                Var* joinvar = (Var*)lfirst(newcell);
                Index varno = joinvar->varno;
                AttrNumber varattno = joinvar->varattno;

                /* skip vars from upper level, since they are treated as const */
                if (joinvar->varlevelsup > 0)
                    continue;

                (void)get_real_rte_varno_attno(parse, &varno, &varattno);
                if (varno != joinvar->varno || varattno != joinvar->varattno) {
                    Var* newVar = (Var*)copyObject(joinvar);
                    newVar->varno = varno;
                    newVar->varattno = varattno;
                    newVar->varnoold = varno;
                    newVar->varoattno = varattno;
                    simplevarlist = lappend(simplevarlist, newVar);
                    joinvarlist = lappend(joinvarlist, joinvar);
                }
            }
            list_free_ext(simplevars);
        }

        /* Replace the final targetlist with new vars from subqueries */
        if (simplevarlist != NIL && joinvarlist != NIL) {
            /* Replace base table var with join var (if possible) in targetlist */
            new_tlist = (List*)replace_node_clause(
                (Node*)new_tlist, (Node*)simplevarlist, (Node*)joinvarlist, RNC_COPY_NON_LEAF_NODES);

            /* Replace base table var with join var (if possible) in havingquals */
            havingquals =
                replace_node_clause(havingquals, (Node*)simplevarlist, (Node*)joinvarlist, RNC_COPY_NON_LEAF_NODES);

            /* Replace base table var with join var (if possible) in grouping expressions */
            group_exprs = (List*)replace_node_clause(
                (Node*)group_exprs, (Node*)simplevarlist, (Node*)joinvarlist, RNC_COPY_NON_LEAF_NODES);
        }
    }

    tlist = make_agg_var_list(root, new_tlist, NULL);

    /*
     * At this point tlist is [group-by-col] [aggref] [aggref]
     * Remove group-by-col from tlist to [aggref] [aggref]
     */
    foreach (lc, group_exprs) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        foreach (lc2, tlist) {
            Expr* expr = (Expr*)lfirst(lc2);
            if (IsA(expr, TargetEntry)) {
                if (equal(tle->expr, ((TargetEntry*)expr)->expr)) {
                    tlist = list_delete_ptr(tlist, expr);
                    break;
                }
            } else {
                if (equal(tle->expr, expr)) {
                    tlist = list_delete_ptr(tlist, expr);
                    break;
                }
            }
        }
    }

    /* Make the subqueries, and then join them */
    foreach (lc, agg_costs.exprAggs) {
        Aggref* node = (Aggref*)lfirst(lc);
        RangeTblEntry* rte = NULL;
        RangeTblRef* rtr = NULL;
        int rtindex;
        Query* partial_query = makeNode(Query);

        partial_query->commandType = CMD_SELECT;
        partial_query->canSetTag = true;
        partial_query->rtable = (List*)copyObject(parse->rtable);
        partial_query->jointree = (FromExpr*)copyObject(parse->jointree);
        partial_query->groupClause = (List*)copyObject(parse->groupClause);
        partial_query->hasAggs = true;
        partial_query->groupingSets = parse->groupingSets ? (List*)copyObject(parse->groupingSets) : NIL;
        partial_query->constraintDeps = parse->constraintDeps;
        partial_query->hintState = (HintState*)copyObject(parse->hintState);
        partial_query->hasSubLinks = parse->hasSubLinks;
        partial_query->hasRowSecurity = parse->hasRowSecurity;
        /* CTEs may be inlined as subqueries, only inherit CTEs with non-zero refcounts */
        ListCell* lcCte = NULL;
        partial_query->cteList = NIL;
        foreach(lcCte, parse->cteList) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(lcCte);
            if (cte->cterefcount > 0) { /* neither inlined nor ignored */
                partial_query->cteList = lappend(partial_query->cteList, cte);
            }
        }

        /*
         * For the non-last expr, we generate count(distinct) expressions related to
         * it as the targetlist, and remove them from tlist. For the last expr, just use
         * the left expressions as targetlist
         */
        if (lc != list_tail(agg_costs.exprAggs)) {
            target_list = NIL;
            for (lc2 = list_head(tlist); lc2 != NULL; lc2 = lc3) {
                Aggref* n = (Aggref*)lfirst(lc2);
                lc3 = lnext(lc2);
                if (IsA(n, Aggref) && n->aggdistinct != NIL) {
                    TargetEntry* tle = (TargetEntry*)linitial(n->args);
                    if (equal(tle->expr, node)) {
                        tlist = list_delete_ptr(tlist, n);
                        target_list = lappend(target_list, n);
                    }
                }
            }
        } else
            target_list = tlist;

        /* Add all the same agg expressions to the subquery's targetlist */
        resno = list_length(group_exprs) + 1;
        foreach (lc2, target_list) {
            Node* n = (Node*)lfirst(lc2);
            foreach (lc3, new_tlist) {
                TargetEntry* tle = (TargetEntry*)lfirst(lc3);
                if (equal(tle->expr, n)) {
                    TargetEntry* new_tle = flatCopyTargetEntry(tle);
                    new_tle->expr = (Expr*)n;
                    new_tle->resno = resno;
                    resno++;
                    new_tle->resjunk = false;
                    if (new_tle->resname == NULL)
                        new_tle->resname = "?column?";
                    lfirst(lc2) = new_tle;
                    break;
                }
            }
            if (!IsA(lfirst(lc2), TargetEntry))
                lfirst(lc2) = makeTargetEntry((Expr*)n, resno++, "?column?", false);
        }
        partial_query->targetList = list_concat((List*)copyObject(group_exprs), target_list);

#ifdef ENABLE_MULTIPLE_NODES
        /* Set can_push flag of query. */
        mark_query_canpush_flag((Node *) partial_query);
#endif

        /* Generate subquery rte and add it to the range tables */
        rte = addRangeTableEntryForSubquery(NULL, partial_query, makeAlias("subquery", NIL), false, true);
        rtable = lappend(rtable, rte);
        rtindex = list_length(rtable);

        rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        fromlist = lappend(fromlist, rtr);

        /* Generate join clauses for the join between subqueries */
        i = 0;
        foreach (lc2, partial_query->targetList) {
            TargetEntry* tent = (TargetEntry*)lfirst(lc2);
            Var* var = NULL;

            var = makeVarFromTargetEntry(rtindex, tent);
            if (i < list_length(group_exprs)) {
                if (lc == list_head(agg_costs.exprAggs)) {
                    join_exprs = lappend(join_exprs, var);

                    /* Construct old and new expr to do replacement in having clause */
                    orig_target_list = lappend(orig_target_list, tent->expr);
                    varlist = lappend(varlist, var);
                } else {
                    Node *tmp_var, *qual_item;

                    lc3 = (i == 0) ? list_head(join_exprs) : lnext(lc3);
                    tmp_var = (Node*)lfirst(lc3);
                    qual_item = (Node*)make_opclause(eq_op[i],
                        BOOLOID, /* opresulttype */
                        false,   /* opretset */
                        (Expr*)copyObject(tmp_var),
                        (Expr*)copyObject(var),
                        InvalidOid,
                        collation[i]);
                    quals = make_and_qual(quals, qual_item);
                }
            } else {
                orig_target_list = lappend(orig_target_list, tent->expr);
                varlist = lappend(varlist, var);
            }
            i++;
        }
    }

    new_tlist =
        (List*)replace_node_clause((Node*)new_tlist, (Node*)orig_target_list, (Node*)varlist, RNC_COPY_NON_LEAF_NODES);
    havingquals = replace_node_clause(havingquals, (Node*)orig_target_list, (Node*)varlist, RNC_COPY_NON_LEAF_NODES);
    quals = make_and_qual(quals, havingquals);
    list_free_ext(orig_target_list);
    list_free_ext(varlist);
    /* Now change var levels up if there's sublink in it, do it after rewrite node substitution */
    IncrementVarSublevelsUp_rtable(rtable, 1, 0);

    list_free_deep(parse->rtable);
    /* don't free parse->targetList becaused it may be refereced in set operations */
    parse->havingQual = NULL;
    parse->targetList = new_tlist;
    parse->jointree = makeFromExpr(fromlist, quals);
    parse->rtable = rtable;
    parse->cteList = NIL;
    parse->hasAggs = false;
    parse->hasSubLinks = false;
    parse->hasRowSecurity = false;

    list_free_deep(parse->groupingSets);
    parse->groupingSets = NULL;

    if (parse->groupClause != NIL) {
        list_free_deep(parse->groupClause);
        list_free_deep(group_exprs);
        pfree_ext(eq_op);
        pfree_ext(collation);
        parse->groupClause = NIL;
    }

    /* Delete hintState. */
    if (parse->hintState != NULL) {
        HintStateDelete(parse->hintState);
        pfree_ext(parse->hintState);
        parse->hintState = NULL;
    }
}
#endif

static bool pull_sublink_clause_walker(Node* node, pull_node_clause* context)
{
    if (node == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_NullTest:
            if (context->flag & PE_NULLTEST) {
                context->nodeList = lappend(context->nodeList, node);
                return false;
            }
            break;
        case T_CoalesceExpr:
        case T_CaseExpr:
        case T_FuncExpr:
            break;
        case T_NullIfExpr:
            if (!context->recurse) {
                return false;
            }
            break;
        case T_BoolExpr: {
            /* Traversion will ceased when return true */
            if (((BoolExpr*)node)->boolop == NOT_EXPR) {
                if (context->flag & PE_NOTCLAUSE) {
                    context->nodeList = lappend(context->nodeList, node);
                }
                if (!context->recurse) {
                    return false;
                }
            }
            break;
        }
        case T_OpExpr:
        {
            if (context->flag & PE_OPEXPR) {
                context->nodeList = lappend(context->nodeList, node);
                return false;
            }
            break;
        }
        case T_SubLink:
            context->nodeList = lappend(context->nodeList, node);
            context->nameList = lappend(context->nameList, context->name);
            return false;
        case T_TargetEntry:
        {
            TargetEntry *entry= (TargetEntry *)node;
            context->name = entry->resname;
            break;
        }

        default:
            break;
    }

    return expression_tree_walker(node, (bool (*)())pull_sublink_clause_walker, (void*)context);
}

/* @Describe - Get sublink to list.
 *
 * @in node -  input node expr.
 * @in skipOpExpr - mark if skip expr_sublink.
 *
 * @return - sublink list.
 */
List *pull_sublink(Node *node, int flag, bool is_name, bool recurse)
{
    pull_node_clause context;

    context.nodeList = NIL;
    context.nameList = NIL;
    context.recurse = recurse;
    context.flag = flag;
    context.name = NULL;

    (void)pull_sublink_clause_walker(node, &context);

    if (is_name) {
        return context.nameList;
    }

    return context.nodeList;
}

/*
 * @Description: Get all opExpr.
 * @in node - source node
 * @in context - store op expr.
 */
static bool pull_opExpr_clause_walker(Node* node, pull_node_clause* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, OpExpr)) {
        context->nodeList = lappend(context->nodeList, node);
        return false;
    }

    return expression_tree_walker(node, (bool (*)())pull_opExpr_clause_walker, (void*)context);
}

/*
 * @Description: Get all opExpr.
 * @in node - source node.
 * @return - opExpr list.
 */
List* pull_opExpr(Node* node)
{
    pull_node_clause context;

    context.nodeList = NIL;
    context.flag = PE_OPEXPR;

    (void)pull_opExpr_clause_walker(node, &context);

    return context.nodeList;
}

/* get_equal_operates
 * get equal that one size include all var is level up  other include all var is not level up, when
 * two size all include level up var, return false, this qual can not pull
 * up.
 */
static bool get_equal_operates(OpExpr* qual, List** pullUpQual, bool paramAllowed)
{
    List* qualVarList = NULL;
    ListCell* lc = NULL;
    int levelUp[2] = {-1, -1};
    int i = 0;

    foreach (lc, qual->args) {
        Node* node = (Node*)lfirst(lc);
        qualVarList =
            pull_var_clause(node, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true);

        bool isfirst = true;
        ListCell* lc2 = NULL;
        foreach (lc2, qualVarList) {
            Var* var = (Var*)lfirst(lc2);

            if (isfirst) {
                levelUp[i] = var->varlevelsup;
            } else {
                if ((Index)levelUp[i] != var->varlevelsup) {
                    return false;
                }
            }
            isfirst = false;
        }

        /* Can not volatile functions */
        if (levelUp[i] == 0 && contain_volatile_functions(node)) {
            return false;
        }
        i++;
    }

    /* Both sides of args include difference level vars */
    if ((levelUp[0] == 1 && levelUp[1] == 0) || (levelUp[0] == 0 && levelUp[1] == 1)) {
        Oid leftArgType = exprType((Node*)linitial(qual->args));

        if (paramAllowed &&
            (op_hashjoinable(qual->opno, leftArgType) || op_mergejoinable(qual->opno, leftArgType))) {
            *pullUpQual = lappend(*pullUpQual, qual);
        } else {
            return false;
        }
    } else if ((levelUp[0] == 0 && levelUp[1] == 0) || (levelUp[0] == 0 && levelUp[1] == -1) ||
             (levelUp[0] == -1 && levelUp[1] == 0) || (levelUp[0] == -1 && levelUp[1] == -1)) {
        /* Only include this level vars, unnecessary to pull up, no append qual to pullUpQual */
        return true;
    } else {
        /*
         * All var is level up var, can not pull up otherwise can lead to result error.
         * e.g. include or_clause select.
         */
        return false;
    }

    return true;
}

/*
 * @Description: Judge this node is equal exprs and be connected by and_exprs.
 * @in node - Need judge node.
 * return - If this node is equal exprs and be connected by and_exprs return true else return false.
 */
static bool equal_expr(Node* node)
{
    if (node == NULL) {
        return false;
    }

    switch (nodeTag(node)) {
        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;
            Oid leftArgType = exprType((Node*)linitial(expr->args));

            if (!op_hashjoinable(expr->opno, leftArgType) && !op_mergejoinable(expr->opno, leftArgType)) {
                return false;
            }
            break;
        }
        case T_BoolExpr: {
            if (and_clause(node)) {
                BoolExpr* andExpr = (BoolExpr*)node;
                ListCell* lc = NULL;
                foreach (lc, andExpr->args) {
                    Node* qual = (Node*)lfirst(lc);
                    if (!equal_expr(qual)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
            break;
        }
        default: {
            return false;
        }
    }
    return true;
}

/*
 * Check if there is a range table entry of type func expr whose arguments
 * are correlated
 */
static bool has_correlation_in_rte(List* rtable)
{
    ListCell* lc_rte = NULL;
    foreach (lc_rte, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc_rte);
        if (rte->funcexpr != NULL && contain_vars_of_level_or_above(rte->funcexpr, 1)) {
            return true;
        }
    }
    return false;
}

/* judge_OpExpr_pullUp
 * Judge this opexpr which include sublink if can pull up.
 */
static bool safe_convert_EXPR(PlannerInfo *root, Node *clause, SubLink* sublink, Relids available_rels, bool in_or_clause)
{
    Relids varnos = NULL;
    Relids level_up_varnos = NULL;
    Query* subQuery = NULL;
    bool        limit1_in_sublink = false;

    subQuery = (Query*)sublink->subselect;

    if (subQuery->jointree->fromlist == NULL) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery's fromlist is null."))));
        return false;
    }

    /* Can not support gropuing clause*/
    if (subQuery->cteList ||
        subQuery->hasWindowFuncs ||
        subQuery->hasModifyingCTE ||
        subQuery->havingQual ||
        subQuery->groupingSets ||
        subQuery->groupClause ||
        subQuery->limitOffset ||
        subQuery->rowMarks ||
        subQuery->distinctClause ||
        subQuery->windowClause) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery includes cte, windowFun, havingQual, group, "
                        "limitoffset, distinct or rowMark."))));
        return false;
    }

    /* Only handle limit 1. */
    if (subQuery->limitCount) {
        Node *estLimit = NULL;
        int64 limitCnt = 0;

        /*
         * Check whether the limit field exists in the sublink and whether
         * the value of the limit field is 1.
         */
        estLimit = eval_const_expressions(NULL, subQuery->limitCount);
        if (estLimit && IsA(estLimit, Const)) {
            limitCnt = (((Const *) estLimit)->constisnull) ? 0 :
    
                DatumGetInt64(((Const *) estLimit)->constvalue);
        } else {
            limitCnt = 0;
        }
    
        if (limitCnt != 1) {
            ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
    
                (errmsg("[Expr sublink pull up failure reason]: limit count of subquery is't equal to 1."))));
            return false;
        } else {
            limit1_in_sublink = true;
        }
    }

    /*
     * In the case of without agg, we can only pull up sublink when we have limit 1 or
     * unique_check which is not in or clause.
     */
    if (!subQuery->hasAggs) {
        if (!(limit1_in_sublink || (((unsigned int)u_sess->attr.attr_sql.rewrite_rule & SUBLINK_PULLUP_WITH_UNIQUE_CHECK) && permit_from_rewrite_hint(root, SUBLINK_PULLUP_WITH_UNIQUE_CHECK))))  {
            ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
                    (errmsg("[Expr sublink pull up failure reason]: with no agg and don't have limit1 or unique check."))));
            return false;
        } else if (in_or_clause) {
            /* We will remove this restriction after support pull up no agg in or clause in the near future. */
            ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: with no agg in or clause can not pull up temporarily."))));
            return false;
        }
    }

    /* Targetlist of subquery can only include one element without junk attribute*/
    int tle_without_junk_number = 0;
    ListCell* lc = NULL;
    foreach (lc, subQuery->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (!(tle->resjunk)) {
            tle_without_junk_number++;
        }
    }
    if (tle_without_junk_number != 1) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery targetlist contains more than one element "
                        "without the junk attribute."))));
        return false;
    }

    /* No set operations in the subquery */
    if (subQuery->setOperations) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery includes set operation."))));
        return false;
    }

    varnos = pull_varnos(clause, 0, true);

    /* Varnos in op need belong to available_rels otherwise it can not be pulled up */
    if (!bms_is_subset(varnos, available_rels)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg(
                    "[Expr sublink pull up failure reason]: Not all the varnos of opClause are in available_rels."))));
        return false;
    }

    /* Sublink qual must be correlations in the WHERE clause */
    level_up_varnos = pull_varnos(subQuery->jointree->quals, 1, true);

    if (bms_is_empty(level_up_varnos) || !bms_is_subset(level_up_varnos, available_rels)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg(
                    "[Expr sublink pull up failure reason]: Sublink is uncorrelated or refer to unavailable rels."))));
        return false;
    }

    /* Targetlist can not include level up >= 1 vars */
    if (contain_vars_of_level_or_above((Node*)subQuery->targetList, 1)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Sublink targetlist includes var with levelup which is "
                        "greater than 0."))));
        return false;
    }

    /* Target can not include volatile function */
    if (contain_volatile_functions((Node*)subQuery->targetList)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Sublink targetlist includes volatile functions."))));
        return false;
    }

    if (expression_returns_set((Node*)subQuery->targetList)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Sublink targetlist returns a set."))));
        return false;
    }

    /* If deeply correlated, don't bother, in other words this subquery include another sublink and
     * which include leave up gt 1 var.
     */
    if (contain_vars_of_level_or_above((Node*)subQuery, 2)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery includes levelup greater than 1."))));
        return false;
    }

    if (has_correlation_in_rte(subQuery->rtable)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Subquery includes range table entry of type func expr "
                        "whose arguments are correlated."))));
        return false;
    }

    /*
     * Disable pullup sublink for fast query shipping plan or stream plan with replicated table, as
     * these scenarios don't need stream operator, so we can use index plan to accelerate the executor.
     * Notice : We don't know whether have index or whether in TP scenario ,so we use rewrite_rule to control.
     */
    if ((((unsigned int)u_sess->attr.attr_sql.rewrite_rule & SUBLINK_PULLUP_DISABLE_REPLICATED) && 
            permit_from_rewrite_hint(root, SUBLINK_PULLUP_DISABLE_REPLICATED)) &&
        (IS_PGXC_DATANODE || (IS_STREAM_PLAN && all_replicate_table(subQuery)))) {
        ereport(DEBUG2,
               (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Expr sublink pull up failure reason]: Sublink pull up of replicated table is disabled."))));

        return false;
    }

    return true;
}

/* append_target_and_group
 * Append node to targetlist and group by clause
 */
static void append_target_and_group(PlannerInfo *root, Query* subQuery, Node* node)
{
    Oid sortop = InvalidOid;
    Oid eqop = InvalidOid;
    bool hashable = InvalidOid;
    TargetEntry* tle = NULL;
    SortGroupClause* grpcl = NULL;
    int len = list_length(subQuery->targetList) + 1;
    bool find = false;

    if (ENABLE_PRED_PUSH_ALL(root)) {
        ListCell* lc = NULL;
        int pos = 1;

        foreach (lc, subQuery->targetList) {
            TargetEntry* tge = (TargetEntry*)lfirst(lc);

            if (IsA(tge->expr, Var) && equal(tge->expr, node)) {
                len = pos;
                tge->ressortgroupref = len;
                find = true;
                break;
            }
            pos++;
        }
    }

    if (!find)
    {
        /* Append this parameter to subquery targestlist*/
        tle = makeTargetEntry((Expr*)node, len, pstrdup("?column?"), false);
        tle->ressortgroupref = len;
        subQuery->targetList = lappend(subQuery->targetList, tle);
    }

    /* This node need participate group of subquery. determine the eqop and optional sortop
     */
    get_sort_group_operators(exprType(node), false, true, false, &sortop, &eqop, NULL, &hashable);

    grpcl = makeNode(SortGroupClause);
    grpcl->tleSortGroupRef = len;
    grpcl->eqop = eqop;
    grpcl->sortop = sortop;
    grpcl->nulls_first = false;
    grpcl->hashable = hashable;

    subQuery->groupClause = lappend(subQuery->groupClause, grpcl);
}

static bool get_pullUp_equal_expr_internal(JoinExpr* je, List** pullUpQual, bool paramAllowed)
{
    if(!get_pullUp_equal_expr(je->quals, pullUpQual, paramAllowed)) {
        return false;
    }
    if(!get_pullUp_equal_expr(je->larg, pullUpQual, paramAllowed)) {
        return false;
    }
    if(!get_pullUp_equal_expr(je->rarg, pullUpQual, paramAllowed)) {
        return false;
    }

    return true;
}

/* get_pullUp_equal_expr
 * Sublink can be pulled up when function return true and pullUpQual is not null.
 * pullUpQual include all need pull up equal operator.
 */
static bool get_pullUp_equal_expr(Node* node, List** pullUpQual, bool paramAllowed)
{
    if (node == NULL) {
        return true;
    }

    switch (nodeTag(node)) {
        case T_FromExpr: {
            FromExpr* fromExpr = (FromExpr*)node;
            ListCell* lc = NULL;
            foreach (lc, fromExpr->fromlist) {
                Node* jtree = (Node*)lfirst(lc);
                if (!get_pullUp_equal_expr(jtree, pullUpQual, paramAllowed)) {
                    return false;
                }
            }

            if (!get_pullUp_equal_expr(fromExpr->quals, pullUpQual, paramAllowed)) {
                return false;
            }
            break;
        }
        case T_JoinExpr: {
            JoinExpr* je = (JoinExpr*)node;
            if (je->jointype != JOIN_INNER) {
                /*
                 * Supports the promotion of sublinks that contain related columns
                 * in the where condition of outer join. The promotion of on conditions
                 * of outer join does not support promotion.
                 */
                paramAllowed = false;
            }
            if (!get_pullUp_equal_expr_internal(je, pullUpQual, paramAllowed)) {
                return false;
            }
            break;
        }
        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;
            if (!get_equal_operates(expr, pullUpQual, paramAllowed)) {
                return false;
            }
            break;
        }
        case T_BoolExpr: {
            if (not_clause(node) || or_clause(node)) {
                if (contain_vars_of_level_or_above(node, 1)) {
                    return false;
                }
            } else {
                Assert(and_clause(node));

                BoolExpr* andExpr = (BoolExpr*)node;
                ListCell* lc = NULL;
                foreach (lc, andExpr->args) {
                    Node* qual = (Node*)lfirst(lc);
                    if (!get_pullUp_equal_expr(qual, pullUpQual, paramAllowed)) {
                        return false;
                    }
                }
            }
            break;
        }

        default: {
            List* qualVarList =
                pull_var_clause(node, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true);

            ListCell* lc = NULL;
            foreach (lc, qualVarList) {
                Var* var = (Var*)lfirst(lc);

                /*
                 * If other expr include level up >= 1 var, then can not pull up
                 * e.g. NullTest.
                 */
                if (var->varlevelsup >= 1) {
                    return false;
                }
            }

            break;
        }
    }

    return true;
}

/*
 * Disable pullup sublink for fast query shipping plan or stream plan with replicated table, as
 * these scenarios don't need stream operator, so we can use index plan to accelerate the executor.
 * Notice : We don't know whether have index or whether in TP scenario ,so we use rewrite_rule to control.
 */
static bool cannot_covert_ANY(PlannerInfo *root, Query* sub_select)
{
    if ((((unsigned int)u_sess->attr.attr_sql.rewrite_rule & SUBLINK_PULLUP_DISABLE_REPLICATED) && 
            permit_from_rewrite_hint(root, SUBLINK_PULLUP_DISABLE_REPLICATED)) &&
        (IS_PGXC_DATANODE || (IS_STREAM_PLAN && all_replicate_table(sub_select)))) {
        return true;
    }

    return false;
}

/*
 * @Description: Judge this any sublink if can pull up.
 * @in sublink - sublink node.
 * @in available_rels1 - available rel numbers.
 */
static bool safe_convert_ANY(PlannerInfo *root, SubLink* sublink, Relids available_rels)
{
    Relids varnos = NULL;
    Query* sub_select = (Query*)sublink->subselect;

    if (contain_vars_of_level_or_above((Node*)sub_select, 1)) {
        ereport(DEBUG2,
            (errmodule(MOD_OPT_REWRITE),
            (errmsg("[Any sublink pull up failure reason]: Sublink includes var with levelup which is greater than 0."))));

        return false;
    }

    if (sub_select->cteList) {
        return false;
    }

    if (sub_select->commandType != CMD_SELECT ||
        sub_select->cteList ||
        sub_select->setOperations ||
        sub_select->hasAggs ||
        sub_select->groupingSets ||
        sub_select->hasWindowFuncs ||
        sub_select->hasModifyingCTE ||
        sub_select->havingQual ||
        sub_select->limitOffset ||
        sub_select->limitCount ||
        sub_select->rowMarks) {
        ereport(DEBUG2,
                (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Any sublink pull up failure reason]:"
                        "Sublink is not select or includes"
                        " cte, setop, agg, windowfunc, having, grouping, limit or rowmark."))));

        return false;
    }

    if (expression_returns_set((Node*)sub_select->targetList)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
               (errmsg("[Any sublink pull up failure reason]: Sublink targetlist returns a set."))));

        return false;
    }

    /*
     * The subquery must have a nonempty jointree, else we won't have a join.
     */
    if (sub_select->jointree->fromlist == NIL) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
               (errmsg("[Any sublink pull up failure reason]: Sublink's fromlist is null."))));

        return false;
    }

    varnos = pull_varnos(sublink->testexpr);
    if (bms_is_empty(varnos)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
                (errmsg("[Any sublink pull up failure reason]: Sublink's outer query contains no varnos."))));

        return false;
    }

    /*
     * However, it can't refer to anything outside available_rels.
     */
    if (!bms_is_subset(varnos, available_rels)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Any sublink pull up failure reason]: Sublink refer to unavailable rels."))));

        return false;
    }

    /*
     * The combining operators and left-hand expressions mustn't be volatile.
     */
    if (contain_volatile_functions(sublink->testexpr)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Any sublink pull up failure reason]: Sublink's outer query includes volatile functions."))));

        return false;
    }


    if (cannot_covert_ANY(root, sub_select)) {
        return false;
    }

    return true;
}

/*
 * @Description: Judge this exists sublink if can pull up.
 * @in sublink - sublink node.
 * @in available_rels1 - available rel numbers.
 */
static bool safe_convert_EXISTS(PlannerInfo *root, SubLink* sublink, Relids available_rels)
{
    Query* subselect = (Query*)(sublink->subselect);
    Node* whereClause = NULL;
    Relids clause_varnos = NULL;

    if (contain_vars_of_level_or_above((Node*)subselect, 2)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: "
                "Sublink includes var with levelup which is greater than 1."))));

        return false;
    }

    /*
     * Can't flatten if it contains WITH.  (We could arrange to pull up the
     * WITH into the parent query's cteList, but that risks changing the
     * semantics, since a WITH ought to be executed once per associated query
     * call.)  Note that convert_ANY_sublink_to_join doesn't have to reject
     * this case, since it just produces a subquery RTE that doesn't have to
     * get flattened into the parent query.
     */

    if (subselect->commandType != CMD_SELECT ||
        subselect->cteList ||
        subselect->setOperations ||
        subselect->hasAggs ||
        subselect->groupingSets ||
        subselect->hasWindowFuncs ||
        subselect->hasModifyingCTE ||
        subselect->havingQual ||
        subselect->limitOffset ||
        subselect->limitCount ||
        subselect->rowMarks) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
       (errmsg("[Exists sublink pull up failure reason]:"
               " Sublink is not select or includes"
               " cte, setop, agg, windowfunc, having, grouping, limit or rowmark."))));

        return false;
    }

    if (expression_returns_set((Node*)subselect->targetList)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink targetlist returns a set."))));

        return false;
    }

    /*
     * The subquery must have a nonempty jointree, else we won't have a join.
     */
    if (subselect->jointree->fromlist == NIL) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink's fromlist is null."))));

        return false;
    }

    /*
     * Disable pullup sublink for fast query shipping plan or stream plan with replicated table, as
     * these scenarios don't need stream operator, so we can use index plan to accelerate the executor.
     * Notice : We don't know whether have index or whether in TP scenario ,so we use rewrite_rule to control.
     */
    if ((((unsigned int)u_sess->attr.attr_sql.rewrite_rule & SUBLINK_PULLUP_DISABLE_REPLICATED) && 
            permit_from_rewrite_hint(root, SUBLINK_PULLUP_DISABLE_REPLICATED)) &&
        (IS_PGXC_DATANODE || (IS_STREAM_PLAN && all_replicate_table(subselect))))  {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink pull up of replicated table is disabled."))));

        return false;
    }

    /*
     * Separate out the WHERE clause.  (We could theoretically also remove
     * top-level plain JOIN/ON clauses, but it's probably not worth the
     * trouble.)
     */
    whereClause = subselect->jointree->quals;
    subselect->jointree->quals = NULL;

    /*
     * The rest of the sub-select must not refer to any Vars of the parent
     * query.  (Vars of higher levels should be okay, though.)
     */
    if (contain_vars_of_level((Node*)subselect, 1)) {
        subselect->jointree->quals = whereClause;
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: "
        "Sublink without whereclause refer any vars of the parent."))));

        return false;
    }

    /*
     * On the other hand, the WHERE clause must contain some Vars of the
     * parent query, else it's not gonna be a join.
     */
    if (!contain_vars_of_level(whereClause, 1)) {
        subselect->jointree->quals = whereClause;
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink's whereclause have no var of parent query."))));

        return false;
    }

    /*
     * We don't risk optimizing if the WHERE clause is volatile, either.
     */
    if (contain_volatile_functions(whereClause)) {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink whereclause includes volatile functions."))));

        subselect->jointree->quals = whereClause;
        return false;
    }

    clause_varnos = pull_varnos(whereClause, 1, true);
    if (bms_is_empty(clause_varnos) || !bms_is_subset(clause_varnos, available_rels)) {
        subselect->jointree->quals = whereClause;
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Exists sublink pull up failure reason]: Sublink's whereclause refer to unavailable rels."))));

        return false;
    }

    subselect->jointree->quals = whereClause;

    return true;
}

/*
 * @Description: Judge this sublink if can pull up.
 * @in sunlink - SubLink node.
 * @return - true say this sublink can be pulled up; false say this sublink can not pulled up.
 */
static bool safe_convert_ORCLAUSE(PlannerInfo *root, Node* clause, SubLink* sublink, Relids available_rels)
{
    bool result = false;
    switch (sublink->subLinkType) {
        case EXISTS_SUBLINK:
            result = safe_convert_EXISTS(root, sublink, available_rels);
            break;
        case ANY_SUBLINK:
            result = safe_convert_ANY(root, sublink, available_rels);
            break;
        case EXPR_SUBLINK:
            result = safe_convert_EXPR(root, clause, sublink, available_rels, true);
            break;
        default:
            break;
    }

    return result;
}

static int get_target_idx(List *targetlist, Node* node)
{
    ListCell *lc = NULL;
    int pos = 1;
    foreach (lc, targetlist) {
        TargetEntry* tge = (TargetEntry*)lfirst(lc);

        if (IsA(tge->expr, Var) && equal(tge->expr, node)) {
            return pos;
        }
        pos++;
    }

    return -1;
}

static void
append_target_and_windowClause(PlannerInfo *root, Query *subQuery, Node *node, bool isRowNumberFunc)
{
    ListCell        *lc = NULL;
    TargetEntry     *tle = NULL;
    SortGroupClause *sortGroupClause = NULL;
    WindowClause    *windowClause = NULL;
    WindowFunc      *rowNumerFunc = NULL;

    /* Append windowFunc to subquery targetlist. */
    if (subQuery->windowClause == NIL) {
        windowClause = (WindowClause *) makeNode(WindowClause);
        windowClause->partitionClause = NIL;
        windowClause->frameOptions = FRAMEOPTION_DEFAULTS;
        windowClause->winref = 1;
        subQuery->windowClause = lappend(subQuery->windowClause, windowClause);
        subQuery->hasWindowFuncs = true;
        
    } else {
        windowClause = (WindowClause *) linitial(subQuery->windowClause);
    }

    if (!isRowNumberFunc) {

        /* Append this parameter to subquery targestlist. */
        Oid sortOp = InvalidOid;
        Oid eqOp = InvalidOid;
        bool hashable = false;
        int len = list_length(subQuery->targetList) + 1;
        bool find = false;
        
        if (ENABLE_PRED_PUSH_ALL(root)) {
            lc = NULL;
            int pos = 1;
            foreach (lc, subQuery->targetList) {
                TargetEntry* tge = (TargetEntry*)lfirst(lc);

                if (IsA(tge->expr, Var) && equal(tge->expr, node)) {
                    len = pos;
                    tge->ressortgroupref = len;
                    find = true;
                    break;
                }
                pos++;
            }
        }
        
        if (!find)
        {
            tle = makeTargetEntry((Expr*)node, len, pstrdup("?column?"), false);
            tle->ressortgroupref = len;
            subQuery->targetList = lappend(subQuery->targetList, tle);
        }

        get_sort_group_operators(exprType(node),
                                false, true, false,
                                &sortOp, &eqOp, NULL,
                                &hashable);
        sortGroupClause = (SortGroupClause *) makeNode(SortGroupClause);
        sortGroupClause->tleSortGroupRef = len;
        sortGroupClause->eqop = eqOp;
        sortGroupClause->sortop = sortOp;
        sortGroupClause->hashable = hashable;
        windowClause->partitionClause = lappend(windowClause->partitionClause, sortGroupClause);
    } else {
        rowNumerFunc = (WindowFunc *) makeNode(WindowFunc);
        rowNumerFunc->winfnoid = ROWNUMBERFUNCOID;
        rowNumerFunc->wintype = INT8OID;
        rowNumerFunc->wincollid = 0;
        rowNumerFunc->inputcollid = 0;
        rowNumerFunc->winref = windowClause->winref;
        subQuery->targetList = lappend(subQuery->targetList,
                        makeTargetEntry((Expr *) rowNumerFunc,
                        list_length(subQuery->targetList) + 1,
                        pstrdup("?column?"), false));

        foreach(lc, subQuery->sortClause)
        {
            Node * subQueryOrderNode = NULL;
            Node * sortGroupNode = (Node *) lfirst(lc);

            subQueryOrderNode = (Node *)copyObject(sortGroupNode);
            windowClause->orderClause = lappend(windowClause->orderClause, subQueryOrderNode);
        }
        subQuery->sortClause = NULL;        
    }
}

static List* get_targetList_with_resjunk(List** subList)
{
    List* tmptargetList = NIL;
    List* targetList_with_resjunk = NIL;
    ListCell* lc = NULL;
    int len = 0;
    foreach (lc, *subList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        if (te->resjunk) {
            targetList_with_resjunk = lappend(targetList_with_resjunk, te);
        } else {
            len++;
            te->resno = (int16)len;
            tmptargetList = lappend(tmptargetList, te);
        }
    }
    list_free(*subList);
    *subList = tmptargetList;
    return targetList_with_resjunk;
}

/*
 * @Description: transform this euqal expr, append one args of expr to subquery's targetlist and group clauses;
 * 		generate not null oper.
 * @in root - Per-query information for planning/optimization
 * @in subQuery - sublink's subquery.
 * @in pullUpEqualExpr - need pull up equal expr.
 * @out quals - not null expr.
 * @return - join qual.
 */
static Node* transform_equal_expr(PlannerInfo* root,
                                        Query* subQuery,
                                        List* pullUpEqualExpr,
                                        Node** quals,
                                        bool isLimit,
                                        bool isnull)
{
    int targetListLen = 0;
    List* constList = NULL;
    int rtindex = 0;
    ListCell* lc = NULL;
    Node* joinQual = NULL;
    Var* var = NULL;

    /*The position of subquery int rtable*/
    rtindex = list_length(root->parse->rtable) + 1;

    /*
     * We need to extract the junk column, when isLimit is true.
     * Because we will append node to targetlist, if the Junk column is not at the end,
     * It'll go wrong. After apending node,we will add the junk cloumn at the end.
     */
    List* targetList_with_resjunk = NIL;
    if (isLimit) {
        targetList_with_resjunk = get_targetList_with_resjunk(&(subQuery->targetList));
    }

    targetListLen = list_length(subQuery->targetList);

    /* Deal with equal expr in sublink*/
    foreach (lc, pullUpEqualExpr) {
        Assert(IsA(lfirst(lc), OpExpr));
        OpExpr* pullUpExpr = (OpExpr*)lfirst(lc);

        ListCell* cell = NULL;

        /*
         * This equal expr need pull up, and one of parameter need append to targetList of subquery
         * and participate in group of subquery.
         */
        foreach (cell, pullUpExpr->args) {
            Node* node = (Node*)lfirst(cell);

            if (contain_vars_of_level(node, 0)) {
                
                if (ENABLE_PRED_PUSH_ALL(root)) {
                    int pos = get_target_idx(subQuery->targetList, node);
                    if (pos != -1) {
                        targetListLen = pos;
                    }
                    else {
                        targetListLen = list_length(subQuery->targetList);
                        targetListLen++;
                    }
                } else {
                    targetListLen++;
                }

                lfirst(cell) = var =
                    makeVar(rtindex, targetListLen, exprType(node), exprTypmod(node), exprCollation(node), 0);

                /*Append node to targetlist and group by clause of subquery*/
                if (isLimit) {
                    append_target_and_windowClause(root, subQuery, (Node*)copyObject(node), false);
                } else {
                    append_target_and_group(root, subQuery, (Node*)copyObject(node));
                }
                
                if(quals && (*quals) == NULL) {
                    NullTestType nullType = isnull ? IS_NULL : IS_NOT_NULL;
                    *quals = (Node*)makeNullTest(nullType, (Expr*)copyObject(var));
                }

            }
        }

        /*pull up this subQuery equal qual*/
        joinQual = make_and_qual((Node*)joinQual, (Node*)pullUpExpr);
        constList = lappend(constList, makeBoolConst(true, false));
    }

    if (isLimit) {
        append_target_and_windowClause(root, subQuery, NULL, true);

        /* The value of SortGroupRef is incorrect because the junk column is obtained. 
         * After the junk column is added, the size of SortGroupRef needs to be adjusted.
         */
        int len = list_length(targetList_with_resjunk);
        WindowClause* windowClause = (WindowClause *) linitial(subQuery->windowClause);
        foreach (lc, windowClause->partitionClause) {
            SortGroupClause* sortGroupClause = (SortGroupClause*)lfirst(lc);
            ListCell* lc1 = NULL;
            foreach (lc1, subQuery->targetList) {
                TargetEntry* te = (TargetEntry*)lfirst(lc1);
                if (sortGroupClause->tleSortGroupRef == te->ressortgroupref) {
                    sortGroupClause->tleSortGroupRef += (Index)len;
                    te->ressortgroupref += (Index)len;
                    break;
                }
            }
        }
        /* Append junk node to targetlist at the end. */
        targetListLen = list_length(subQuery->targetList);
        foreach (lc, targetList_with_resjunk) {
            targetListLen++;
            TargetEntry* te = (TargetEntry*)lfirst(lc);
            te->resno = (int16)targetListLen;
            subQuery->targetList = lappend(subQuery->targetList, te);
        }
    }

    /* Delete qual from subquery, it will be pull up*/
    subQuery->jointree = (FromExpr*)replace_node_clause((Node*)subQuery->jointree,
        (Node*)pullUpEqualExpr,
        (Node*)constList,
        RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);

    return joinQual;
}

/*
 * @Description: Get qual which only include special varno.
 * @in all_quals: All quals.
 * @in qual_list: Pull qual list struct.
 */
void get_varnode_qual(Node* all_quals, push_qual_context* qual_list)
{
    if (all_quals == NULL) {
        return;
    } else if (and_clause(all_quals)) {
        /* First check this and_clause. */
        if (check_varno(all_quals, qual_list->varno, 0)) {
            qual_list->qual_list = lappend(qual_list->qual_list, all_quals);
        } else {
            ListCell* l = NULL;

            foreach (l, ((BoolExpr*)all_quals)->args) {
                Node* cluase = (Node*)lfirst(l);

                get_varnode_qual(cluase, qual_list);
            }
        }
    } else if (or_clause(all_quals)) {
        if (check_varno(all_quals, qual_list->varno, 0)) {
            qual_list->qual_list = lappend(qual_list->qual_list, all_quals);
        }
    } else {
        /* Check this qual if only include this varno. */
        if (check_varno(all_quals, qual_list->varno, 0)) {
            qual_list->qual_list = lappend(qual_list->qual_list, all_quals);
        }
    }
}

/*
 * @Description: Get any-sublink left args and targetlist from pullUpEqualExpr.
 * @in pullUpEqualExpr: Pull up equal exprs list.
 * @in relid£º Relition id.
 * @return: Args and targetlist.
 */
static List* get_left_args_and_target_expr(OpExpr* pullUpExpr, int relid)
{
    Node* left_arg = NULL;
    Node* targetExpr = NULL;
    List* args_and_target = NIL;

    if (list_length(pullUpExpr->args) == 2) {
        ListCell* cell = NULL;

        foreach (cell, pullUpExpr->args) {
            Node* node = (Node*)lfirst(cell);

            if (contain_vars_of_level(node, 0)) {
                left_arg = node;
            } else if (contain_vars_of_level(node, 1) && check_varno(node, relid, 1)) {
                /* This node come from upper levels query, and only include this relid. */
                targetExpr = node;
            } else {
                break;
            }
        }
    }

    if (left_arg != NULL && targetExpr != NULL) {
        args_and_target = lappend(args_and_target, left_arg);
        args_and_target = lappend(args_and_target, targetExpr);
    }

    return args_and_target;
}

/*
 * @Description: Get can push-down's qual which is any_sublink.
 * @in root: Per-query information for planning/optimization.
 * @in qual_list: Only include this rel's quals.
 * @in relid: Relation id.
 * @n pullUpEqualExpr: Op expr list from subquery and that will be pull up.
 * @out targetlist: Sublink target list.
 */
static Node* build_op_expr(PlannerInfo* root, int relid, List* pullUpEqualExpr, List** targetlist)
{
    Node* sublink_text_expr = NULL;
    int resno = 1;
    ListCell* lc = NULL;
    List* op_args = NIL;

    /* Find one op_expr. its one arg be from relid. */
    foreach (lc, pullUpEqualExpr) {
        OpExpr* pullUpExpr = (OpExpr*)lfirst(lc);

        List* args_and_target = get_left_args_and_target_expr(pullUpExpr, relid);

        if (args_and_target != NIL) {
            Node* left_arg = (Node*)linitial(args_and_target);
            Node* targetExpr = (Node*)lsecond(args_and_target);
            OpExpr* expr = NULL;

            targetExpr = (Node*)copyObject(targetExpr);

            /* Var's varno must be 1 in targetExpr */
            OffsetVarNodes((Node*)targetExpr, 1 - relid, 1);

            /* Var's levelup must be 0 in targetExpr */
            IncrementVarSublevelsUp((Node*)targetExpr, -1, 1);

            (*targetlist) = lappend(*targetlist, makeTargetEntry((Expr*)targetExpr, resno, NULL, false));

            /* Genarate op expr. */
            left_arg = (Node*)copyObject(left_arg);

            Param* param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = resno;
            param->paramtype = exprType((Node*)targetExpr);
            param->paramtypmod = exprTypmod((Node*)targetExpr);
            param->paramcollid = exprCollation((Node*)targetExpr);
            param->location = -1;
            param->tableOfIndexType = InvalidOid;

            expr = (OpExpr*)make_op(NULL, list_make1(makeString("=")), left_arg, (Node*)param, 0);

            op_args = lappend(op_args, expr);
            resno++;

            list_free_ext(args_and_target);
        }
    }
    if (op_args != NIL)
        sublink_text_expr = (Node*)make_andclause(op_args);

    return sublink_text_expr;
}

/*
 * @Description: Build any sublink.
 * @in root: Per-query information for planning/optimization.
 * @in qual: Only include this rel's quals.
 * @in relid: Relation id.
 * @in pullUpEqualExpr: Op expr list from subquery and that will be pull up.
 */
static SubLink* build_any_sublink(PlannerInfo* root, List* qual_list, int relid, List* pullUpEqualExpr)
{
    SubLink* sub_link = NULL;
    List* targetlist = NIL;

    Node* sublink_text_expr = build_op_expr(root, relid, pullUpEqualExpr, &targetlist);

    if (sublink_text_expr != NULL) {
        RangeTblEntry* r_table = (RangeTblEntry*)rt_fetch(relid, root->parse->rtable);

        r_table = (RangeTblEntry*)copyObject(r_table);

        List* qualList = (List*)copyObject(qual_list);
        Node* quals_node = NULL;
        ListCell* lc = NULL;

        foreach (lc, qualList) {
            Node* node = (Node*)lfirst(lc);

            quals_node = make_and_qual(quals_node, node);
        }

        list_free_ext(qualList);

        /* Var's varno should be 1 in qual */
        OffsetVarNodes(quals_node, 1 - relid, 0);
        /* Offset varno for r_table's row level security quals */
        OffsetVarNodes((Node*)r_table->securityQuals, 1 - relid, 0);

        Query* selectQuery = makeNode(Query);

        selectQuery->commandType = CMD_SELECT;
        selectQuery->targetList = targetlist;

        selectQuery->rtable = list_make1(r_table);
        RangeTblRef* rtr = makeNode(RangeTblRef);
        rtr->rtindex = 1;

        selectQuery->jointree = makeFromExpr(list_make1(rtr), quals_node);

#ifdef ENABLE_MULTIPLE_NODES
        /* Set can_push flag of query. */
        mark_query_canpush_flag((Node *) selectQuery);
#endif   /* ENABLE_MULTIPLE_NODES */

        sub_link = makeNode(SubLink);
        sub_link->subLinkType = ANY_SUBLINK;

        sub_link->subselect = (Node*)selectQuery;
        sub_link->testexpr = (Node*)sublink_text_expr;
    }

    return sub_link;
}

/*
 * @Description: When the opexpr sublink is converted to the left join, the converted
 *      filtering condition needs to be generated according to the targetlist.
 *      For example :
 *      select * from t1 where t1.a = (select count(*) from t2 where t1.b = t2.b);
 *      
 *      Rewritten form :
 *      select t1.* from t1 left join (select count(t2.a) as cnt, b from t2 group by t2.b)
 *      as tt on tt.b = t1.b where t1.a = coalesce(tt.cnt,0);.
 *      
 *      This function is used to generate 't1.a = coalesce(tt.cnt, 0)' based on count(*),
 *      other similar.
 * @in root - PlannerInfo.
 * @in rtIndex - Varno of new vars to be built.
 * @in targetExpr - TargetEntry expr.
 * @in subQuery - Subquery of opexpr sublink.
 * @return : Filter condition.
 */
static Node*
generate_filter_on_opexpr_sublink(PlannerInfo *root,
                                int rtIndex,
                                Node *targetExpr,
                                Query *subQuery)
{
    List        *varClauseList = NIL;
    List        *aggFuncList = NIL;
    ListCell    *lc = NULL;
    Node        *filterExpr = NULL;
    Var         *aggVar = NULL;
    TargetEntry *aggTargetEntry = NULL;

    /* Recursively iterates through targetExpr and finds the aggregate function in it. */
    filterExpr = (Node *) copyObject(targetExpr);
    varClauseList = pull_var_clause(filterExpr,
                                    PVC_INCLUDE_AGGREGATES,
                                    PVC_REJECT_PLACEHOLDERS,
                                    PVC_RECURSE_SPECIAL_EXPR);
    /* Notice, We only need agg functions. */
    foreach(lc, varClauseList)
    {
        bool exists = false;
        Node *tNode = (Node *) lfirst(lc);

        if (IsA(tNode, Aggref)) {
            ListCell * innerLc = NULL;
            foreach(innerLc , aggFuncList) {
                Node *aggFuncNode = (Node *) lfirst(innerLc);
                
                Assert(IsA(aggFuncNode, Aggref));
                if (((Aggref *) tNode)->aggfnoid == ((Aggref *) aggFuncNode)->aggfnoid &&
                    equal(((Aggref *) tNode)->args, ((Aggref *) aggFuncNode)->args)) {
                    exists = true;
                    break;
                }
            }

            if (!exists) {
                aggFuncList = lappend(aggFuncList, tNode);
            }
        }
        
        exists = false;
    }

    list_free_ext(varClauseList);

    if (aggFuncList == NIL) {
        return (Node *)makeVar(rtIndex, 1, 
                        exprType((Node *) targetExpr), 
                        exprTypmod((Node *) targetExpr), 
                        exprCollation((Node *) targetExpr),
                        0);
    }
    
    foreach(lc, aggFuncList) {
        ListCell *innerLc = NULL;
        Node *aggFuncNode = (Node *) lfirst(lc);
        Node *decoratedConstraints = NULL;
        Node *constNode = NULL;

        Assert(((Aggref *) aggFuncNode)->agglevelsup == 0);
        /*
         * Try to find target entry only including this function node. If not found,
         * construct a new targetEntry based on agg function node, and append it to
         * the targetList of subQuery.
         */
        foreach(innerLc, subQuery->targetList) {
            TargetEntry *tempTargetEntry = (TargetEntry *) lfirst(innerLc);
            if (equal(tempTargetEntry->expr, aggFuncNode)) {
                aggTargetEntry = tempTargetEntry;
                break;
            }
        }
        
        /* If not found, add a new targetEntry. */
        if (innerLc == NULL) {
            aggTargetEntry = makeTargetEntry((Expr *) aggFuncNode,
                                            list_length(subQuery->targetList) + 1,
                                            pstrdup("?column?"),
                                            false);
            subQuery->targetList = lappend(subQuery->targetList, aggTargetEntry);
        }

        /* Construct a new aggregate var to replace the aggregate function in targetExpr. */
        aggVar = makeVarFromTargetEntry(rtIndex, aggTargetEntry);

        if ((((Aggref *) aggFuncNode)->aggfnoid == ANYCOUNTOID) ||
            (((Aggref *) aggFuncNode)->aggfnoid == COUNTOID)) {
            constNode = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(uint32),
                                    Int32GetDatum(0), false, true);
            decoratedConstraints = (Node *) makeNode(CoalesceExpr);
            ((CoalesceExpr *) decoratedConstraints)->coalescecollid = InvalidOid;
            ((CoalesceExpr *) decoratedConstraints)->coalescetype = exprType((Node *) aggFuncNode);
            ((CoalesceExpr *) decoratedConstraints)->args = list_make2(aggVar, (Node *) constNode);
        } else {
            decoratedConstraints = (Node *) aggVar;
        }

        filterExpr = replace_node_clause((Node *) filterExpr, 
                                        (Node *) aggFuncNode,
                                        (Node *) decoratedConstraints,
                                        RNC_COPY_NON_LEAF_NODES);
    }

    return filterExpr;
}

/*
 * @Description: Set var's varno and attno to base rel's relid and attribute number.
 * @in parse: Query tree.
 * @in node: Qual list.
 * @in level_up: If only deal with level up var.
 */
static void set_varno_attno(Query* parse, Node* node, bool level_up)
{
    List* vars =
        pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true);
    ListCell* cell = NULL;
    foreach (cell, vars) {
        Var* var = (Var*)lfirst(cell);

        /* Only set the levelup = 1 var. */
        if (level_up && var->varlevelsup != 1) {
            continue;
        } else if (!level_up && var->varlevelsup != 0) { /* Only set the levelup = 0 var. */
            continue;
        }

        RangeTblEntry* rte = (RangeTblEntry*)rt_fetch(var->varno, parse->rtable);

        if (rte->rtekind == RTE_JOIN) {
            Node* join_node = (Node*)list_nth(rte->joinaliasvars, var->varattno - 1);

            if (IsA(join_node, Var)) {
                Var* v = (Var*)join_node;
                var->varno = v->varno;
                var->varattno = v->varattno;
                set_varno_attno(parse, (Node*)var, level_up);
            }
        }
    }

    list_free_ext(vars);
}

/*
 * @Description: Get condition which can push down to suquery from master query.
 * @in all_quals: Master query's all quals.
 * @in pullUpEqualExpr: Op expr list from subquery and that will be pull up.
 */
static Node* push_down_qual(PlannerInfo* root, Node* all_quals, List* pullUpEqualExpr)
{
    if (all_quals == NULL) {
        return NULL;
    }

    List* pullUpExprList = (List*)copyObject(pullUpEqualExpr);
    Node* all_quals_list = (Node*)copyObject(all_quals);

    /* pullUpExprList is correlate condition, this function only should set upper levels var. */
    set_varno_attno(root->parse, (Node*)pullUpExprList, true);
    set_varno_attno(root->parse, (Node*)all_quals_list, false);

    Relids varnos = pull_varnos((Node*)pullUpExprList, 1);
    push_qual_context qual_list;
    SubLink* any_sublink = NULL;
    Node* push_quals = NULL;
    int attnum = 0;

    while ((attnum = bms_first_member(varnos)) >= 0) {
        RangeTblEntry* r_table = (RangeTblEntry*)rt_fetch(attnum, root->parse->rtable);

        /* This table need be base table, other not to deal. */
        if (r_table->rtekind == RTE_RELATION) {
            qual_list.varno = attnum;
            qual_list.qual_list = NIL;

            /* Get qual which only include special varno. */
            get_varnode_qual(all_quals_list, &qual_list);

            /* Upper query include qual which only include this attnum. */
            if (qual_list.qual_list != NIL && !contain_volatile_functions((Node*)qual_list.qual_list)) {
                any_sublink = build_any_sublink(root, qual_list.qual_list, attnum, pullUpExprList);
                push_quals = make_and_qual(push_quals, (Node*)any_sublink);
            }

            list_free_ext(qual_list.qual_list);
        }
    }

    list_free_deep(pullUpExprList);
    pfree_ext(all_quals_list);

    return push_quals;
}

/*
 * @Description: Pull up expr sublink can pull up.
 * @in root: Per-query information for planning/optimization.
 * @in jtlink1: Current joinExpr.
 * @in inout_quals: Op expr.
 * @in sublink: Expr sublink.
 * @in available_rels: Available relids.
 * @out isPullUp: If pull up.
 * @in all_quals: Quals.
 * @return: Final join quals.
 */
Node*
convert_EXPR_sublink_to_join(PlannerInfo *root,
                                Node **jtlink1,
                                Node *inout_quals,
                                SubLink *sublink,
                                Relids *available_rels,
                                Node *all_quals,
                                const char *refname)
{
    Query   *subQuery = (Query *)sublink->subselect;

    if (!safe_convert_EXPR(root, inout_quals, sublink, *available_rels) || has_no_expand_hint(subQuery)) {
        return inout_quals;
    }

    /* 
     * Situation 1: limit 1
     * For example:
     * select * from t1 where t1.a = (select t2.a from t2 where t2.b = t1.b limit 1)
     */
    if (!subQuery->hasAggs && subQuery->limitCount)
    {
        return convert_expr_sublink_with_limit_clause(root,
                                                    jtlink1,
                                                    sublink,
                                                    inout_quals,
                                                    available_rels,
                                                    all_quals, refname);
    }
    else
    {
        /* 
         * Situation 2: agg
         * For example:
         * select * from t1 where t1.a = (select avg(t2.a) from t2 where t2.b = t1.b)
         * select * from t1 where t1.a = (select avg(t2.a) from t2 where t2.b = t1.b limit 1)
         * select * from t1 where t1.a = (select t2.a from t2 where where t2.b = t1.b)
         */
        return convert_expr_subink_with_agg_targetlist(root,
                                                    jtlink1,
                                                    inout_quals,
                                                    sublink,
                                                    available_rels,
                                                    all_quals, refname);
    }

    return NULL;
}

static TargetEntry* getRowNumberTargetEntry(const List *subList)
{
    ListCell* lc = NULL;
    foreach (lc, subList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (IsA(tle->expr, WindowFunc) && ((WindowFunc*)(tle->expr))->winfnoid == ROWNUMBERFUNCOID) {
            return tle;
        }
    }
    ereport(ERROR,
        (errmodule(MOD_OPT),
             errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
             (errmsg("Can't find targetEntry with rownumber window function."))));
    return NULL;
}

/*
 * @Description: Convert expr-sublink which targetlist does't contain agg function,
 *      but includes limit clause to left join.
 *      For example:
 *      select * from t1 where t1.a = (select t2.a from t2 where t2.b = t1.b
 *      order by t2.a limit 1)
 *      
 * @in root - PlannerInfo.
 * @in currJoinLink - Current join link.
 * @in sublink - Sublink which will be pulled up.
 * @in exprQual - Condition expression that contains sublink.
 * @in available_rels - Available relids.
 * @return : Expr node.
 */
static Node*
convert_expr_sublink_with_limit_clause(PlannerInfo *root,
                                    Node ** currJoinLink,
                                    SubLink *sublink,
                                    Node *exprQual,
                                    Relids *available_rels,
                                    Node *all_quals, const char *refname)
{
    List        *pullUpEqualQuals = NIL;
    ListCell    *lc = NULL;
    Node        *joinQual = NULL;
    Node        *push_quals = NULL;
    Node        *rowNumVar = NULL;
    Node        *tmpExprQual = exprQual;
    Query       *subQuery = NULL;
    Query       *newSubquery = NULL;
    RangeTblEntry   *rte = NULL;
    RangeTblRef     *rtr = NULL;
    Index           rtIndex = 0;

    subQuery = (Query *) sublink->subselect;

    /* 
     * Check whether the conditions of the sublink meet the requirements
     * of the pull. The conditions are met only when the conditions are
     * all equal.
     */
    if (get_pullUp_equal_expr((Node*)subQuery->jointree, &pullUpEqualQuals) &&
        pullUpEqualQuals)
    {
        /* Guc rewrite_rule need set to magicset.*/
        if (((u_sess->attr.attr_sql.rewrite_rule & MAGIC_SET) && permit_from_rewrite_hint(root, MAGIC_SET)) && !contain_subplans((Node*)subQuery->jointree))
        {
            /* Get can push down to subquery's quals. */
            push_quals = push_down_qual(root, all_quals, pullUpEqualQuals);
        }

        joinQual = transform_equal_expr(root, subQuery, pullUpEqualQuals, NULL, true);

        /*
         * Upper-level vars in subquery will now be one level closer to their
         * parent than before; in particular, anything that had been level 1
         * becomes level zero.
         */
        rtIndex = list_length(root->parse->rtable) + 1;
        IncrementVarSublevelsUp(joinQual, -1, 1);
        *available_rels = bms_add_member(*available_rels, (int)rtIndex);

        if (push_quals != NULL)
        {
            subQuery->jointree->quals = make_and_qual(subQuery->jointree->quals, push_quals);
            subQuery->hasSubLinks = true;
        }
        
        newSubquery = makeNode(Query);
        newSubquery->commandType = CMD_SELECT;
        newSubquery->can_push = true;
        if (refname != NULL && ENABLE_PRED_PUSH_ALL(root)) {
            rte = addRangeTableEntryForSubquery(NULL,
                                            subQuery,
                                            makeAlias(refname, NIL),
                                            false,
                                            false,
                                            true);
        } else {
            rte = addRangeTableEntryForSubquery(NULL,
                                            subQuery,
                                            makeAlias("subquery", NIL),
                                            false,
                                            false,
                                            true);
        }
        newSubquery->rtable = list_make1(rte);
        newSubquery->targetList = NIL;
        
        int tleIdx = 1;
        foreach(lc, subQuery->targetList)
        {
            TargetEntry *te = (TargetEntry *) lfirst(lc);

            if (te->resjunk) {
                continue;
            }

            TargetEntry *newTe = NULL;
            Var *newVar = makeVarFromTargetEntry(1, te);

            Assert(tleIdx <= list_length(subQuery->targetList));
            newTe = makeTargetEntry((Expr *) newVar, tleIdx, pstrdup("?column?"), false);
            newSubquery->targetList = lappend(newSubquery->targetList, newTe);
            tleIdx++;
        }

        rtr = makeNode(RangeTblRef);
        rtr->rtindex = 1;
        newSubquery->jointree = (FromExpr *) makeNode(FromExpr);
        newSubquery->jointree->fromlist = list_make1(rtr);

        rowNumVar = (Node *) makeVarFromTargetEntry(1, getRowNumberTargetEntry(subQuery->targetList));
        newSubquery->jointree->quals = (Node *) make_opclause(INT84EQOID, BOOLOID,
                                    false,
                                    (Expr *) rowNumVar,
                                    (Expr *) subQuery->limitCount,
                                    InvalidOid,
                                    InvalidOid);
        subQuery->limitCount = NULL;

        /* Replace sublink with the first targetentry expr of newSubquery. */
        Var *replaceSublinkVar = makeVarFromTargetEntry(rtIndex, (TargetEntry *) linitial(newSubquery->targetList));
        exprQual = replace_node_clause(exprQual,
                            (Node *) sublink,
                            (Node *) replaceSublinkVar,
                            RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
    
        /* Add new subquery to root->ratble */
        rte = addRangeTableEntryForSubquery(NULL,
                                            newSubquery,
                                            makeAlias("newSubquery", NIL),
                                            false,
                                            false,
                                            true);
        root->parse->rtable = lappend(root->parse->rtable, rte);
        
        /* 
         * This qual of include sublink need be pull up, we will it replace with true here. 
         * For example:
         * select * from t1 inner join t2 on t2.a = (select avg(t3.a) from t3 where t3.b = t2.b);
         */
        if (IsA(*currJoinLink, JoinExpr)) {
            
            ((JoinExpr*)*currJoinLink)->quals = replace_node_clause(((JoinExpr*)*currJoinLink)->quals, 
                                    tmpExprQual, 
                                    makeBoolConst(true, false),
                                    RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
            
        } else if (IsA(*currJoinLink, FromExpr)) {
            ((FromExpr*)*currJoinLink)->quals = replace_node_clause(((FromExpr*)*currJoinLink)->quals, 
                                    tmpExprQual, 
                                    makeBoolConst(true, false),
                                    RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
        }

        /* Make new RangeTblEntry, refference to the newSubquery created. */
        rtr = (RangeTblRef *) makeNode(RangeTblRef);
        rtr->rtindex = list_length(root->parse->rtable);
        
        /* Make JoinExpr of left join. */
        JoinExpr *result = NULL;
        result = (JoinExpr *) makeNode(JoinExpr);
        result->jointype = JOIN_LEFT;
        result->quals = joinQual;
        result->larg = *currJoinLink;
        result->rarg = (Node *) rtr;

        /* 
         * Add JoinExpr to rangetableentry. In subsequent processing, the left
         * external connection may be converted into an internal connection.
         */
        rte = addRangeTableEntryForJoin(NULL,
                                        NIL,
                                        result->jointype,
                                        NIL,
                                        result->alias,
                                        true);
        root->parse->rtable = lappend(root->parse->rtable, rte);
        
        *currJoinLink = (Node *) result;

        /* Mark query's can_push */
        mark_parent_child_pushdown_flag(root->parse, newSubquery);

        list_free_ext(pullUpEqualQuals);

        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Sublink pull up failure reason]: Sublink need 'equal' with suitable level vars and can only combined with 'and'."))));

        return exprQual;
    }
    else
    {
        list_free_ext(pullUpEqualQuals);
        return exprQual;
    }
}

/*
 * @Description: Convert expr-sublink which targetlist contains agg function. 
 *      For example:
 *      select * from t1 where t1.a = (select avg(t2.a) from t2 where t2.b = t1.b);
 *      
 * @in root - PlannerInfo.
 * @in jtlink1 - Current join link.
 * @in inout_quals - Condition expression that contains sublink.
 * @in sublink - Sublink which will be pulled up.
 * @in available_rels - Available relids.
 * @in all_quals - Quals
 * @return : Expr node.
 */
static Node*
convert_expr_subink_with_agg_targetlist(PlannerInfo *root,
                                        Node **jtlink1,
                                        Node *inout_quals,
                                        SubLink *sublink,
                                        Relids *available_rels,
                                        Node *all_quals, const char *refname)
{
    int         rtindex = 0;
    List        *pullUpEqualExpr = NIL; 
    Query       *subQuery = NULL;
    Node        *joinQual = NULL;
    Node        *push_quals = NULL;
    
    subQuery = (Query*)sublink->subselect;

    /*
     * Judge this sublink if all quals is 'equal' and it is connected by 'and', and equal expr
     * one size include level up var other not include if so it can pull up or else not, and
     * append need pull up equal expr in sublink to list. sublink can br pulled up where 
     * get_pullUp_equal_expr return true and pullUpEqualExpr is not null.
     */
    if (get_pullUp_equal_expr((Node*)subQuery->jointree, &pullUpEqualExpr) && pullUpEqualExpr)
    {
        /* Guc rewrite_rule need set to magicset.*/
        if (((u_sess->attr.attr_sql.rewrite_rule & MAGIC_SET) && permit_from_rewrite_hint(root, MAGIC_SET)) && !contain_subplans((Node*)subQuery->jointree))
        {
            /* Get can push down to subquery's quals.*/
            push_quals = push_down_qual(root, all_quals, pullUpEqualExpr);
        }

        /* Mark unique_check flag of subquery */
        subQuery->unique_check = !subQuery->hasAggs;

        /* Rollback to don't pull up sublink when cannot hash or in upgrade. */
        if (subQuery->unique_check &&
            (!CanExprHashable(pullUpEqualExpr) ||
            t_thrd.proc->workingVersionNum < SUBLINKPULLUP_VERSION_NUM))
        {
            subQuery->unique_check = false;
            list_free_ext(pullUpEqualExpr);

            ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
            (errmsg("[Expr sublink pull up failure reason]: Only support unique check for hashable scenario."))));

            return inout_quals;
        }

        joinQual = transform_equal_expr(root, subQuery, pullUpEqualExpr, NULL, false);

        /* Pull up sublink, replace var by sublink that come from subquery. */
        JoinExpr    *result = NULL;
        Node        *tmp_opexpr = inout_quals;
        Node        *expr = (Node *)((TargetEntry *)linitial(subQuery->targetList))->expr;
        Node        *decoratedConstraints = NULL;

        /* Add new rtindex of rangeTblRef, append rindex to available rel number. */
        rtindex = list_length(root->parse->rtable) + 1;
        *available_rels = bms_add_member(*available_rels, rtindex);
        
        /* 
         * Generates filtering conditions for left join based on the targetlist of
         * the subLink.
         */
        decoratedConstraints = generate_filter_on_opexpr_sublink(root,
                                                            rtindex,
                                                            expr,
                                                            subQuery);
        if (decoratedConstraints != NULL) {
            inout_quals = replace_node_clause(inout_quals, (Node*) sublink, 
                            decoratedConstraints, RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
        }
        
        /*
         * Upper-level vars in subquery will now be one level closer to their
         * parent than before; in particular, anything that had been level 1
         * becomes level zero.
         */
        IncrementVarSublevelsUp(joinQual, -1, 1);

        /* This qual of include sublink need be pull up, we will it replace with true here. */
        if (IsA(*jtlink1, JoinExpr)) {
            ((JoinExpr*)*jtlink1)->quals = replace_node_clause(((JoinExpr*)*jtlink1)->quals, 
                                    tmp_opexpr, 
                                    makeBoolConst(true, false),
                                    RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
        } else if (IsA(*jtlink1, FromExpr)) {
            Assert(IsA(*jtlink1, FromExpr));
            ((FromExpr*)*jtlink1)->quals = replace_node_clause(((FromExpr*)*jtlink1)->quals, 
                                    tmp_opexpr, 
                                    makeBoolConst(true, false),
                                    RNC_RECURSE_AGGREF | RNC_COPY_NON_LEAF_NODES);
        }

        if (push_quals != NULL)
        {
            subQuery->jointree->quals = make_and_qual(subQuery->jointree->quals, push_quals);
            subQuery->hasSubLinks = true;
        }

        /* Append subquery to rtable*/
        RangeTblEntry* rte = NULL;

        if (refname != NULL && ENABLE_PRED_PUSH_ALL(root)) {
            rte = addRangeTableEntryForSubquery(NULL, subQuery, makeAlias(refname, NIL), false, false, true);
        } else {
            rte = addRangeTableEntryForSubquery(NULL, subQuery, makeAlias("subquery", NIL), false, false, true);
        }
        root->parse->rtable = lappend(root->parse->rtable, rte);

        /* Append rangeTblRef to fromlist. */
        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        result = makeNode(JoinExpr);
        result->jointype = JOIN_LEFT;
        result->larg = *jtlink1;
        result->rarg = (Node*)rtr;
        result->alias = NULL;
        result->quals = joinQual;

        /* Append joinExpr to rtable. */
        rte = addRangeTableEntryForJoin(NULL,
                                        NIL,
                                        result->jointype,
                                        NIL,
                                        result->alias,
                                        true);
        root->parse->rtable = lappend(root->parse->rtable, rte);
        *jtlink1 = (Node*) result;

        /* Mark query's can_push */
        mark_parent_child_pushdown_flag(root->parse, subQuery);
        
        list_free_ext(pullUpEqualExpr); 
        return inout_quals;
    }
    else
    {
        list_free_ext(pullUpEqualExpr); 

        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Sublink pull up failure reason]: Sublink need 'equal' with suitable level vars and can only combined with 'and'."))));

        return inout_quals;
    }
}

/*
 * @Description:
 * @in root - Per-query information for planning/optimization
 * @in or_clause - or clause args
 * @in exists_sublink - exists sublink
 * @in jtlink1 - points to the link in the jointree
 * @in available_rels1 - available table index
 *
 * @return -   Returns the replacement qual node, or NULL if the qual should be removed.
 */
void convert_OREXISTS_to_join(
    PlannerInfo* root, BoolExpr* or_clause, SubLink* exists_sublink, Node** jtlink1, Relids available_rels1, bool isnull)
{
    Query* subQuery = NULL;
    List* pullUpEqualExpr = NULL;
    Node* joinQual = NULL;
    Node* whereClause = NULL;
    JoinExpr* result = NULL;
    Node* quals = NULL;
    RangeTblEntry* rte = NULL;

    if (!safe_convert_ORCLAUSE(root, NULL, exists_sublink, available_rels1)) {
        return;
    }

    subQuery = (Query*)(exists_sublink->subselect);
    if (has_no_expand_hint(subQuery)) {
        return;
    }

    subQuery = (Query*)copyObject(subQuery);

    /*
     * We can throw away the targetlist, as well as any GROUP, WINDOW, DISTINCT, and ORDER BY clauses.
     * These clauses is not useful, can not effect final results.
     */
    subQuery->targetList = NIL;
    subQuery->groupClause = NIL;
    subQuery->distinctClause = NIL;
    subQuery->sortClause = NIL;
    subQuery->hasDistinctOn = false;

    whereClause = subQuery->jointree->quals;

    if (get_pullUp_equal_expr(whereClause, &pullUpEqualExpr) && pullUpEqualExpr) {
        joinQual = transform_equal_expr(root, subQuery, pullUpEqualExpr, &quals, false, isnull);

        /* Replace this sublink with quals which is "not null" expr.*/
        or_clause = (BoolExpr*)replace_node_clause(
            (Node*)or_clause, (Node*)exists_sublink, (Node*)quals, RNC_RECURSE_AGGREF | RNC_REPLACE_FIRST_ONLY);

        List* l = NULL;
        for (int i = 0; i < list_length(pullUpEqualExpr); i++) {
            l = lappend(l, makeBoolConst(true, false));
        }

        /* Replace these equal exprs with const true . */
        whereClause =
            (Node*)replace_node_clause((Node*)whereClause, (Node*)pullUpEqualExpr, (Node*)l, RNC_RECURSE_AGGREF);

        /*
         * Upper-level vars in subquery will now be one level closer to their
         * parent than before; in particular, anything that had been level 1
         * becomes level zero.
         */
        IncrementVarSublevelsUp(joinQual, -1, 1);

        /* Append subquery to rtable*/
        if (root->glob->sublink_counter != 0 && ENABLE_PRED_PUSH_ALL(root)) {
            char *subquery_name = denominate_sublink_name(root->glob->sublink_counter);
            rte = addRangeTableEntryForSubquery(NULL, subQuery, makeAlias(subquery_name, NIL), false, false, true);
        } else {
            rte = addRangeTableEntryForSubquery(NULL, subQuery, makeAlias("subquery", NIL), false, false, true);
        }

        /* Now we can attach the modified subquery rtable to the parent.*/
        root->parse->rtable = lappend(root->parse->rtable, rte);

        RangeTblRef* rtr = makeNode(RangeTblRef);
        rtr->rtindex = list_length(root->parse->rtable);

        result = makeNode(JoinExpr);
        result->jointype = JOIN_LEFT;
        result->quals = joinQual;
        result->larg = *jtlink1;

        result->rarg = (Node*)rtr;
        result->alias = NULL;
        *jtlink1 = (Node*)result;

        /* Mark query's can_push */
        mark_parent_child_pushdown_flag(root->parse, subQuery);

        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Sublink pull up failure reason]: Sublink need 'equal' with suitable level vars and can only combined with 'and'."))));

        list_free_ext(pullUpEqualExpr);
        return;
    } else {
        list_free_ext(pullUpEqualExpr);
        return;
    }
}

/*
 * @Description: add all targetlist's expr to group clauses.
 * @in query - query.
 */
static void add_targetlist_to_group(Query* query)
{
    AssertEreport(query->groupClause == NIL, MOD_OPT_REWRITE, "Sublink to be pulled up shouldn't have group clause");

    int len = 1;
    SortGroupClause* grpcl = NULL;

    ListCell* lc = NULL;
    foreach (lc, query->targetList) {
        TargetEntry* tg = (TargetEntry*)lfirst(lc);
        Node* node = (Node*)tg->expr;

        Oid sortop = InvalidOid;
        Oid eqop = InvalidOid;
        bool hashable = InvalidOid;

        get_sort_group_operators(exprType(node), false, true, false, &sortop, &eqop, NULL, &hashable);

        tg->ressortgroupref = len;

        grpcl = makeNode(SortGroupClause);
        grpcl->tleSortGroupRef = len;
        grpcl->eqop = eqop;
        grpcl->sortop = sortop;
        grpcl->nulls_first = false;
        grpcl->hashable = hashable;

        query->groupClause = lappend(query->groupClause, grpcl);
        len++;
    }
}

/*
 * @Description: Convert this any sublink to left join.
 * @in root - Per-query information for planning/optimization.
 * @in or_clause - current or clause.
 * @in any_sublink - This any sublink.
 * @in jtlink1 - current joinExpr
 * @available_rels1 - available rel number.
 */
void convert_ORANY_to_join(
    PlannerInfo* root, BoolExpr* or_clause, SubLink* any_sublink, Node** jtlink1, Relids available_rels)
{
    Query* sub_select = (Query*)any_sublink->subselect;
    if (has_no_expand_hint(sub_select)) {
        return;
    }
    Query* parse = root->parse;
    List* subquery_vars = NULL;
    Node* test_quals = NULL;
    int rtindex;
    RangeTblEntry* rte = NULL;
    RangeTblRef* rtr = NULL;
    JoinExpr* result = NULL;

    if (!safe_convert_ORCLAUSE(root, NULL, any_sublink, available_rels)) {
        return;
    }

    sub_select = (Query*)copyObject(sub_select);

    /*
     * We can throw away GROUP, DISTINCT, and ORDER BY clauses.
     * These clauses is not useful, can not effect final results, because targetlist can not
     * include agg.
     */
    sub_select->groupClause = NIL;
    sub_select->distinctClause = NIL;
    sub_select->sortClause = NIL;
    sub_select->hasDistinctOn = false;

    /* New rtable's index. */
    rtindex = list_length(parse->rtable) + 1;

    /*
     * Build the new join's qual expression, replacing Params with these Vars.
     */
    subquery_vars = generate_subquery_vars(root, sub_select->targetList, rtindex);

    test_quals = convert_testexpr(root, any_sublink->testexpr, subquery_vars);

    /* Judge this quals if only include 'and' and 'equal' oper. */
    if (equal_expr(test_quals)) {
        /* add all targetlist to query's group clsuse. */
        add_targetlist_to_group(sub_select);

        TargetEntry* tg = (TargetEntry*)linitial(sub_select->targetList);

        Node* node = (Node*)tg->expr;

        Var* var = makeVar(rtindex, 1, exprType(node), exprTypmod(node), exprCollation(node), 0);

        NullTest* nullTest = makeNullTest(IS_NOT_NULL, (Expr*)var);

        /* Replace this any sublink with "not null" expr. */
        or_clause =
            (BoolExpr*)replace_node_clause((Node*)or_clause, (Node*)any_sublink, (Node*)nullTest, RNC_RECURSE_AGGREF | RNC_REPLACE_FIRST_ONLY);

        if (root->glob->sublink_counter != 0 && ENABLE_PRED_PUSH_ALL(root)) {
            char *subquery_name = denominate_sublink_name(root->glob->sublink_counter);
            rte = addRangeTableEntryForSubquery(NULL, sub_select, makeAlias(subquery_name, NIL), false, false, true);
        } else {
            rte = addRangeTableEntryForSubquery(NULL, sub_select, makeAlias("subquery", NIL), false, false, true);
        }

        /* Append this query to rtable. */
        parse->rtable = lappend(parse->rtable, rte);

        /*
         * Form a RangeTblRef for the pulled-up sub-select.
         */
        rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;

        result = makeNode(JoinExpr);
        result->jointype = JOIN_LEFT;
        result->quals = test_quals;
        result->larg = *jtlink1;

        result->rarg = (Node*)rtr;
        result->alias = NULL;
        *jtlink1 = (Node*)result;

        /* Mark query's can_push */
        mark_parent_child_pushdown_flag(root->parse, sub_select);
    }

    return;
}

/*
 * @Description: Convert sublink in op_expr to left join.
 * @in root - Per-query information for planning/optimization.
 * @in or_clause - current or clause.
 * @in jtlink1 - current joinExpr
 * @available_rels1 - available rel number.
 * @replace - if the node should be replaced
 * @isnull - isnull flag of clause
 * @return - not null expr this expr will replace op_expr.
 */
Node*
convert_OREXPR_to_join(PlannerInfo *root, BoolExpr *or_clause, 
                                Node *clause,
                                SubLink *expr_sublink, 
                                Node **jtlink1, 
                                Relids *available_rels,
                                bool replace,
                                bool isnull)
{
    Query       *subQuery = NULL;
    List        *EqualExprList = NULL;
    Node        *joinQual = NULL;
    Node        *notNullTest = NULL;
    Node        *filterNode = NULL;
    JoinExpr        *result = NULL;
    RangeTblEntry   *rte = NULL;
    RangeTblRef     *rtr = NULL;
    int             rtindex = 0;
    
    /* Judge this sublink if can pull up.*/
    if (!safe_convert_ORCLAUSE(root, clause, expr_sublink, *available_rels)) {
        return NULL;
    }

    subQuery = (Query *)copyObject(expr_sublink->subselect);
    if (has_no_expand_hint(subQuery)) {
        return NULL;
    }
    expr_sublink->subselect = (Node *)subQuery;
    
    if (get_pullUp_equal_expr((Node*)subQuery->jointree, &EqualExprList) && EqualExprList) {
        joinQual = transform_equal_expr(root, subQuery, EqualExprList, &notNullTest, false, isnull);
        
        /*
         * Upper-level vars in subquery will now be one level closer to their
         * parent than before; in particular, anything that had been level 1
         * becomes level zero.
         */
        IncrementVarSublevelsUp(joinQual, -1, 1);

        /* Add new rtindex of rangeTblRef, append rindex to available rel number. */
        rtindex = list_length(root->parse->rtable) + 1;
        *available_rels = bms_add_member(*available_rels, rtindex);

        /* 
         * Now, This sublink will be replaced by var which is come from new rtable, 
         * and this op_expr will as join quals.
         */
        Node *decoratedConstraints = NULL;
        Node *expr = (Node *) ((TargetEntry *)linitial(subQuery->targetList))->expr;
        decoratedConstraints = generate_filter_on_opexpr_sublink(root, rtindex, (Node *) expr, subQuery);
        if (decoratedConstraints != NULL) {
            clause = replace_node_clause(clause,
                                        (Node *)expr_sublink,
                                        decoratedConstraints,
                                        RNC_RECURSE_AGGREF);
        }

        /* Append subquery to rtable. */
        rte = addRangeTableEntryForSubquery(NULL,
                                            subQuery,
                                            makeAlias("subquery", NIL),
                                            false,
                                            false,
                                            true);
        root->parse->rtable = lappend(root->parse->rtable, rte);
        rtr = makeNode(RangeTblRef);
        rtr->rtindex = rtindex;
        
        result = makeNode(JoinExpr);
        result->jointype = JOIN_LEFT;
        result->quals = joinQual;
        filterNode = make_and_qual(notNullTest, clause);

        /*
         * This op_expr already be pull up as current left join's join quals.
         * So, we need delete this op_expr from the previous hoin expr.
         */
        if (replace)
        {
            Assert(IsA(*jtlink1, JoinExpr));

            JoinExpr *join_expr = (JoinExpr*)(*jtlink1);

            join_expr->quals = replace_node_clause((Node *)join_expr->quals, 
                                        clause, 
                                        makeBoolConst(true, false),
                                        RNC_RECURSE_AGGREF);
        }

        result->larg = *jtlink1;
        result->rarg = (Node*)rtr;
        result->alias = NULL;

        /* Append JoinExpr to rtable. */
        rte = addRangeTableEntryForJoin(NULL,
                                        NIL,
                                        result->jointype,
                                        NIL,
                                        result->alias,
                                        true);
        root->parse->rtable = lappend(root->parse->rtable, rte);
        *jtlink1 = (Node*)result;

        /* Mark query's can_push */
        mark_parent_child_pushdown_flag(root->parse, subQuery);

        list_free_ext(EqualExprList);
        return filterNode;
    }
    else
    {
        ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
        (errmsg("[Sublink pull up failure reason]:"
                " Sublink need 'equal' with suitable level vars and can only combined with 'and'."))));

        list_free_ext(EqualExprList);
        return NULL;
    }
}

static void convert_to_join_by_sublinktype(PlannerInfo *root, BoolExpr *or_clause, Node **jtlink1,
    Relids *available_rels1, bool isnull, SubLink *sublink)
{
    switch (sublink->subLinkType) {
        case EXISTS_SUBLINK:
            convert_OREXISTS_to_join(root, or_clause, sublink, jtlink1, *available_rels1, isnull);
            break;
        case ANY_SUBLINK:
            convert_ORANY_to_join(root, or_clause, sublink, jtlink1, *available_rels1);
            break;
        default:
            break;
    }
    return;
}

/*
 * @Description: Convert orclause's sublink to left join.
 * @in root - Per-query information for planning/optimization.
 * @in or_clause - current or clause.
 * @in jtlink1 - current joinExpr
 * @available_rels1 - available rel number.
 */
void convert_ORCLAUSE_to_join(PlannerInfo *root, BoolExpr *or_clause, Node **jtlink1, Relids *available_rels1)
{

    List        *sublinkList = NULL;
    ListCell    *lc = NULL;
    List        *pullExprList = pull_sublink((Node*)or_clause, PE_OPEXPR | PE_NULLTEST | PE_NOTCLAUSE, false);
    Node        *notNullExpr = NULL;

    foreach(lc, pullExprList)
    {
        Node    *clause = (Node *) lfirst(lc);
        bool        isnull = false;
        SubLink *sublink = NULL;

        switch(nodeTag(clause))
        {
            case T_NullTest:
                if (((NullTest *) clause)->nulltesttype == IS_NULL)
                    isnull = true;
                /* fall through */
            case T_OpExpr:
            {
                Node        *notNullAnd = NULL;
                bool        replace = false;
                ListCell    *cell = NULL;

                sublinkList = pull_sublink(clause, 0, false);
                foreach(cell, sublinkList)
                {
                    sublink = (SubLink*)lfirst(cell);

                    /* We only convert expr_sublink of or_clause to join. */
                    if (sublink->subLinkType != EXPR_SUBLINK) {
                        continue;
                    }

                    notNullExpr = convert_OREXPR_to_join(root, or_clause, clause, sublink,
                                            jtlink1, available_rels1, replace, isnull);

                    if (notNullExpr != NULL) {
                        notNullAnd = make_and_qual(notNullAnd, notNullExpr);
                        replace = true;
                    }
                }

                /* This opexpr's sublink always be pulled up, and it will be replaced with not null expr.*/

                if (notNullAnd != NULL) {
                    or_clause = (BoolExpr*)replace_node_clause((Node*)or_clause, clause, notNullAnd, RNC_RECURSE_AGGREF);
                }
            }
                break;

            case T_BoolExpr:
                if (not_clause(clause)) {
                    /* If the immediate argument of NOT is EXISTS, try to convert */
                    sublink = (SubLink *) get_notclausearg((Expr *) clause);

                    /* not in sublink is not supported since it's not equality join */
                    if (!IsA(sublink, SubLink) ||
                            sublink->subLinkType == ANY_SUBLINK) {
                        continue;
                    }
                    /* keep isnull as false since not clause is always here */
                } else {
                    continue;
                }
                /* fall through */
            case T_SubLink:
                if (IsA(clause, SubLink)) {
                    sublink = (SubLink *) clause;
                }

                root->glob->sublink_counter++;
                convert_to_join_by_sublinktype(root, or_clause, jtlink1, available_rels1, isnull, sublink);
                break;
            default:
                break;
        }
    }

    return;
}

/*
 * locate_base_var:
 *	Find if there's base table var in the input node, that
 *	has the same null property with input node
 * Parameters:
 *	@in node: input node
 * Output:
 *	return base table var if found, or NULL
 */
static Var* locate_base_var(Expr* node)
{
    if (IsA(node, Var)) {
        return (Var*)node;
    }

    while (IsA(node, RelabelType) || IsA(node, FuncExpr)) {
        if (IsA(node, RelabelType)) {
            node = ((RelabelType*)node)->arg;
        } else if (IsA(node, FuncExpr)) {
            FuncExpr* funcexpr = (FuncExpr*)node;

            if (1 == list_length(funcexpr->args) && func_strict(funcexpr->funcid)) {
                node = (Expr*)linitial(funcexpr->args);
            } else {
                break;
            }
        }
    }
    if (IsA(node, Var))
        return (Var*)node;

    return NULL;
}

/*
 * CanExprHashable:
 *	Check whether pullup expr can hashable
 * Parameters:
 *	@in node: list of expr pulled up
 * Output:
 *	true for all expr pulled up can hashable
 */
static bool
CanExprHashable(List *pullUpEqualExpr)
{
    ListCell *lc = NULL;

    foreach(lc, pullUpEqualExpr) {
        OpExpr *pullUpExpr = (OpExpr*)lfirst(lc);
        ListCell *cell = NULL;

        foreach(cell, pullUpExpr->args) {
            Node *node = (Node*)lfirst(cell);
            if (contain_vars_of_level(node, 0)) {
                Oid sortop = InvalidOid;
                Oid eqop = InvalidOid;
                bool hashable = false;
                get_sort_group_operators(exprType(node), false, true, false, &sortop, &eqop, NULL, &hashable);
                if (!hashable)
                    return false;
            }
        }
    }
    return true;
}

