/* -------------------------------------------------------------------------
 *
 * planrewrite.cpp
 *    The query cost-based rewrite
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/plan/planrewrite.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "catalog/pgxc_group.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_relation.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#ifdef PGXC
#include "optimizer/dataskew.h"
#include "optimizer/streamplan.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/pgxc.h"
#endif /* PGXC */
#include "catalog/pg_operator.h"
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#include "catalog/pg_aggregate.h"
#include "utils/syscache.h"
#include "parser/parse_oper.h"
#include "catalog/pg_proc.h"
#endif

/* Macros used in inlist2join rewrite */
/* The varno for base rel in subquery relpacement */
const int SUBQUERY_VARNO = 1;

/* The var attno for subquery */
const AttrNumber SUBQUERY_ATTNO = 1;

/* The varno for valuescan */
const int VALUES_SUBLINK_VARNO = 1;

/* The varattno for valuescan */
const AttrNumber VALUES_SUBLINK_VARATTNO = 1;

/*
 * -----------------------------------------------------------------------------
 * Local routine & data structure declarations
 * -----------------------------------------------------------------------------
 */
/*
 * Name: fix_var_context
 *
 * Brief: Data structure to support find fix-var case on parsetree and plannode
 */
typedef struct fix_var_context {
    PlannerInfo* root;
    bool oldtonew;
} fix_var_context;

/*
 * Name: find_inlist2join_context
 *
 * Brief: Data structure to support find inlist2join converted SubQueryScan pathnode
 */
typedef struct find_inlist2join_context {
    PlannerInfo* root;
} find_inlist2join_context;

/*
 * Name: fix_subquery_vars_context
 *
 * Brief: Data structure to support Var-fix in SubQueryScan for base table replacement
 *        in inlist2join QRW optimization optimization.
 */
typedef struct fix_subquery_vars_context {
    Var* v;
    Index old_varno;
    Index new_varno;
} fix_subquery_vars_context;

static bool fix_var_expr_walker(Node* node, fix_var_context* context);
static void fix_var(const List* mappings, Var* old_var, bool oldtonew);
static void find_inlist2join_path_walker(Path* path, find_inlist2join_context* context);
static bool belowInlist2JoinThreshold(const RelOptInfo* rel, int num, RelOrientation orientation);
static Var* fix_subquery_vars_expr(Node* expr, Index old_varno, Index new_varno);
static bool fix_subquery_vars_expr_walker(Node* expr, fix_subquery_vars_context* context);
static void fix_var_expr(PlannerInfo* root, Node* node, bool oldtonew = true);
static void rebuild_subquery(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, RangeTblEntry* new_rte);
static RangeTblEntry* make_rte_with_subquery(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte);
static bool IsConvertableBaseRel(const RangeTblEntry* rte);
static bool IsConvertableInlistRestrict(RelOptInfo* rel, const RestrictInfo* restrict, RelOrientation orientation);
static bool HasConvertableInlistCond(RelOptInfo* rel);
static int ConvertableInlistMaxNum(const RelOptInfo* rel);
static bool IsEqualOpr(Oid opno);
static inline bool IsConvertableType(Oid typoid);
static List* convert_constarray_to_simple(Const* combined_const, Var* listvar, Oid* paramtype);
static char* get_attr_name(int attrnum);
static void rebuild_subquery(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, RangeTblEntry* new_rte);
RelOptInfo* build_alternative_rel(const RelOptInfo* origin, RTEKind rtekind);

/*****************************************************************************
 *
 *	   Cost based inlist2join query rewerite entry point
 *
 * Name: inlist2join_qrw_optimization()
 *
 * Brief: entry point of applying inlist2join optimization conversion
 *
 * Parameters:
 *	@in root: the planner info of current subquery
 *	@in rti: rel && rte index in current subquery level
 *
 * Return: void
 *
 *****************************************************************************/
void inlist2join_qrw_optimization(PlannerInfo* root, int rti)
{
    RelOptInfo* rel = root->simple_rel_array[rti];
    RangeTblEntry* rte = root->simple_rte_array[rti];

    Assert(rel->reloptkind == RELOPT_BASEREL);

    int maxnum = ConvertableInlistMaxNum(rel);

    /* No need process more when inlist2join is disabled */
    if (u_sess->opt_cxt.qrw_inlist2join_optmode == QRW_INLIST2JOIN_DISABLE) {
        return;
    }

    /* Add inlist2join threshold check in case of CBO to protect inaccurate cost estimation
     * when inlist has small num of elements
     */
    if (u_sess->opt_cxt.qrw_inlist2join_optmode == QRW_INLIST2JOIN_CBO &&
        belowInlist2JoinThreshold(rel, maxnum, rel->orientation)) {
        return;
    }

    /* If relation include row level security policy, bypass inlist optimize to avoid
     * infinite recursion
     */
    if (rte->securityQuals != NULL) {
        return;
    }

    /*
     * Return directly if inlist2join optimization can not apply to current rel,
     *  [1]. When inlist2join optimization is not enabled
     *  [2]. When there no convert-safe inlist conditions on baserel
     *  [3]. When current subquery block is not SELECT
     *  [4]. When current rel's RTE kind is not Relation&SubQuery
     */
    /* Do not apply inlist2join if SQL command is not SELECT */
    if (root->parse->commandType != CMD_SELECT) {
        return;
    }

    /* Do not apply inlist2join if rtekind is not relation or subquery */
    if (rel->rtekind != RTE_RELATION && rel->rtekind != RTE_SUBQUERY) {
        return;
    }

    /* Do not apply inlist2join if table is HDFS table or Foreign table */
    if (rel->rtekind == RTE_RELATION && !IsConvertableBaseRel(rte)) {
        return;
    }

    /* Do not apply inlist2join if the rel has no inlist2join baserestrictinfo */
    if (!HasConvertableInlistCond(rel)) {
        return;
    }

    /* Build new reloptinfo (SubQuery) and set it as "alternative" */
    RelOptInfo* new_rel = build_alternative_rel(rel, RTE_SUBQUERY);
    rel->alternatives = lappend(rel->alternatives, new_rel);

    /* Build new rte (SubQuery) */
    RangeTblEntry* new_rte = make_rte_with_subquery(root, new_rel, rte);

    /* Add a joinpath(inlist2join) for current rel */
    set_rel_size(root, new_rel, rti, new_rte);

    /* Append subquery's pathlist to current base rel */
    rel->pathlist = list_concat(rel->pathlist, new_rel->pathlist);
}

/*
 * Name: fix_skew_expr()
 *
 * Brief:  fix expr for skew qual
 * Parameters:
 *	@in root: the planner info data structure of current query level
 *	@in skew_list: skew qual list
 */
static void fix_skew_expr(PlannerInfo* root, const List* skew_list)
{
    if (skew_list == NIL)
        return;

    ListCell* lc = NULL;
    foreach (lc, skew_list) {
        QualSkewInfo* qsinfo = (QualSkewInfo*)lfirst(lc);
        fix_var_expr(root, (Node*)qsinfo->skew_quals);
    }
}

/*
 * Name: fix_subquery_vars_expr_walker()
 *
 * Brief: recursive-iteration function for fix_subquery_vars_expr()
 */
static bool fix_subquery_vars_expr_walker(Node* expr, fix_subquery_vars_context* context)
{
    if (expr == NULL) {
        return false;
    }

    /* Fix var node */
    if (IsA(expr, Var)) {
        Var* v = (Var*)expr;

        if (v->varno == context->old_varno) {
            v->varno = context->new_varno;
        }

        context->v = v;
    }

    return expression_tree_walker(expr, (bool (*)())fix_subquery_vars_expr_walker, (void*)context);
}

/*
 * Name: fix_subquery_vars_expr()
 *
 * Brief: Recursively fix var's varno/varattno in "inlist2join" converted subquery query
 *
 * Parameters:
 *	@in expr: the entry
 *	@in old_varno: the varno that need to be fixed
 *	@in new_varno: the new varno
 *
 * Return: VAR's ref with fixed varno & varattno
 */
static Var* fix_subquery_vars_expr(Node* expr, Index old_varno, Index new_varno)
{
    if (expr == NULL) {
        return NULL;
    }

    fix_subquery_vars_context ctx;
    ctx.v = NULL;
    ctx.old_varno = old_varno;
    ctx.new_varno = new_varno;

    (void)fix_subquery_vars_expr_walker(expr, &ctx);

    return ctx.v;
}

/*
 * Name: fix_var_expr_walker()
 *
 * Brief: recursive-iteration function for fix_var_expr()
 */
static bool fix_var_expr_walker(Node* node, fix_var_context* context)
{
    PlannerInfo* root = context->root;

    if (node == NULL) {
        return false;
    }

    /* Fix var node */
    if (IsA(node, Var)) {
        fix_var(root->var_mappings, (Var*)node, context->oldtonew);
    }

    return expression_tree_walker(node, (bool (*)())fix_var_expr_walker, (void*)context);
}

/*
 * Name: fix_var_expr()
 *
 * Brief: The vars under given node will be fixed by refering to root->var_mappings
 *
 * Parameters:
 *		@in root: the planner info data structure of current query level
 *		@in node: the expr that will be recursively fix-var
 *		@in oldtonew: if we change from old var to new, or reverse
 *
 * Return: void
 */
static void fix_var_expr(PlannerInfo* root, Node* node, bool oldtonew)
{
    fix_var_context ctx;
    ctx.root = root;
    ctx.oldtonew = oldtonew;

    (void)fix_var_expr_walker(node, &ctx);
}

/*
 * Name: fix_var()
 *
 * Brief: fix the var's varno/varattno referring mapping list
 *
 * Parameters:
 *  @in mappings: the var-change references for current subquery level
 *  @in old_var: the var that fill be fixed
 *  @in oldtonew: if we change from old var to new, or reverse
 *
 * Return: void
 */
static void fix_var(const List* mappings, Var* old_var, bool oldtonew)
{
    ListCell* lc = NULL;
    if (mappings == NULL) {
        return;
    }

    foreach (lc, mappings) {
        RewriteVarMapping* rvm = (RewriteVarMapping*)lfirst(lc);
        if (oldtonew && equal(rvm->old_var, old_var)) {
            old_var->varno = rvm->new_var->varno;
            old_var->varattno = rvm->new_var->varattno;

            break;
        } else if (!oldtonew && equal(rvm->new_var, old_var)) {
            old_var->varno = rvm->old_var->varno;
            old_var->varattno = rvm->old_var->varattno;
            break;
        }
    }

    return;
}

/*
 * Name: fix_vars_plannode()
 *
 * Brief: fix the plannode's Var expression from top plan tree
 *
 * Parameters:
 *  @in root: PlannerInfo* ptr of current subquery level
 *  @in node: top plan node pointer
 *
 * Return: void
 */
void fix_vars_plannode(PlannerInfo* root, Plan* node)
{
    Assert(u_sess->opt_cxt.qrw_inlist2join_optmode != QRW_INLIST2JOIN_DISABLE && root->var_mappings);

    if (node == NULL) {
        return;
    }

    /* Pass 1: Fix plan target list and qual */
    fix_var_expr(root, (Node*)node->targetlist);
    fix_var_expr(root, (Node*)node->qual);

    /* For subplan, it is create from base_rel, so we fix the var attno here */
    if (IsA(node, SubqueryScan)) {
        fix_var_expr(root, (Node*)node->distributed_keys);
        return;
    }

    /* Pass 2: Fix plan specific nodes */
    ListCell* lc = NULL;
    switch (nodeTag(node)) {
        case T_HashJoin:
        case T_VecHashJoin: {
            HashJoin* hj = (HashJoin*)node;

            /* Fix Vars in *Hash* clause */
            fix_var_expr(root, (Node*)hj->hashclauses);

            /* Fix Vars in *Join* clause */
            fix_var_expr(root, (Node*)hj->join.joinqual);
            fix_var_expr(root, (Node*)hj->join.nulleqqual);
            fix_var_expr(root, (Node*)node->var_list);
        }

        break;
        case T_NestLoop:
        case T_VecNestLoop: {
            NestLoop* nl = (NestLoop*)node;
            foreach (lc, nl->nestParams) {
                NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);
                fix_var_expr(root, (Node*)nlp->paramval);
            }
            fix_var_expr(root, (Node*)nl->join.joinqual);
            fix_var_expr(root, (Node*)nl->join.nulleqqual);
            fix_var_expr(root, (Node*)node->var_list);
        } break;
        case T_MergeJoin:
        case T_VecMergeJoin: {
            MergeJoin* mj = (MergeJoin*)node;
            fix_var_expr(root, (Node*)mj->mergeclauses);

            /* Fix Vars in *Join* clause */
            fix_var_expr(root, (Node*)mj->join.joinqual);
            fix_var_expr(root, (Node*)mj->join.nulleqqual);
            fix_var_expr(root, (Node*)node->var_list);
        } break;
        case T_Stream:
        case T_VecStream: {
            Stream* sj = (Stream*)node;
            fix_var_expr(root, (Node*)sj->distribute_keys);
            fix_skew_expr(root, sj->skew_list);
        } break;
        case T_RemoteQuery:
        case T_VecRemoteQuery: {
            RemoteQuery* rq = (RemoteQuery*)node;
            fix_var_expr(root, (Node*)rq->base_tlist);
        } break;
        case T_Limit:
        case T_VecLimit: {
            Limit* lm = (Limit*)node;
            fix_var_expr(root, lm->limitCount);
            fix_var_expr(root, lm->limitOffset);
        } break;
        case T_VecWindowAgg:
        case T_WindowAgg: {
            WindowAgg* wa = (WindowAgg*)node;
            fix_var_expr(root, wa->startOffset);
            fix_var_expr(root, wa->endOffset);
        } break;
        case T_BaseResult:
        case T_VecResult: {
            BaseResult* br = (BaseResult*)node;
            fix_var_expr(root, br->resconstantqual);
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)node;
            if (mt->mergeActionList != NIL && (IS_STREAM_PLAN || IS_SINGLE_NODE)) {
                foreach (lc, mt->mergeActionList) {
                    MergeAction* action = (MergeAction*)lfirst(lc);
                    fix_var_expr(root, (Node*)action->targetList);
                    fix_var_expr(root, (Node*)action->qual);
                }
            }
            foreach (lc, mt->plans) {
                fix_vars_plannode(root, (Plan*)lfirst(lc));
            }
            /* Adjust references of remote query nodes in ModifyTable node */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ListCell* elt = NULL;
                RemoteQuery* rq = NULL;

                foreach (elt, mt->remote_plans) {
                    rq = (RemoteQuery*)lfirst(elt);

                    if (rq != NULL) {
                        /*
                         * If base_tlist is set, it means that we have a reduced remote
                         * query plan. So need to set the var references accordingly.
                         */
                        fix_var_expr(root, (Node*)rq->scan.plan.targetlist);
                        fix_var_expr(root, (Node*)rq->scan.plan.qual);
                        fix_var_expr(root, (Node*)rq->base_tlist);
                    }
                }
            }
        } break;
        case T_Append:
        case T_VecAppend: {
            Append* ap = (Append*)node;
            foreach (lc, ap->appendplans) {
                fix_vars_plannode(root, (Plan*)lfirst(lc));
            }
        } break;
        case T_MergeAppend: {
            MergeAppend* ma = (MergeAppend*)node;
            foreach (lc, ma->mergeplans) {
                fix_vars_plannode(root, (Plan*)lfirst(lc));
            }
        } break;
        default:
            break;
    }

    /* Pass3: drilldown into child plan nodes */
    fix_vars_plannode(root, node->lefttree);
    fix_vars_plannode(root, node->righttree);
}

/*
 * Name: fix_var_expr()
 *
 * Brief: The vars under given node will be fixed by refering to root->var_mappings
 *
 * Parameters:
 *		@in root: the planner info data structure of current query level
 *		@in node: the expr that will be recursively fix-var
 *
 * Return: void
 */
void find_inlist2join_path(PlannerInfo* root, Path* best_path)
{
    find_inlist2join_context ctx;
    ctx.root = root;
    List* var_mapping_new = NIL;
    ListCell* lc = NULL;

    find_inlist2join_path_walker(best_path, &ctx);

    foreach (lc, root->var_mappings) {
        RewriteVarMapping* rvm = (RewriteVarMapping*)lfirst(lc);
        if (rvm->need_fix) {
            var_mapping_new = lappend(var_mapping_new, rvm);
        }
    }

    root->var_mappings = var_mapping_new;
}

/*
 * Name: find_inlist2join_path_walker()
 *
 * Brief: recursive-iteration function to find inlist2join SubqueryScan
 */
static void find_inlist2join_path_walker(Path* path, find_inlist2join_context* context)
{
    if (path == NULL) {
        return;
    }

    switch (path->pathtype) {
        case T_Stream: {
            find_inlist2join_path_walker(((StreamPath*)path)->subpath, context);
        } break;
        /* For subqueryscan, we should find the inlist2join subquery */
        case T_SubqueryScan: {
            RelOptInfo* rel = path->parent;
            if (rel->base_rel != NULL) {
                ListCell* lc1 = NULL;
                ListCell* lc = NULL;

                /*
                 * The rel is new_rel and has an inlist2join path
                 * Remove the mapping from root
                 */
                foreach (lc1, rel->reltarget->exprs) {
                    Var* var = (Var*)lfirst(lc1);
                    foreach (lc, context->root->var_mappings) {
                        RewriteVarMapping* rvm = (RewriteVarMapping*)lfirst(lc);
                        if (equal(rvm->old_var, var)) {
                            rvm->need_fix = true;
                            break;
                        }
                    }
                }
            }
        } break;
        case T_Append: {
            ListCell* cell = NULL;

            foreach (cell, ((AppendPath*)path)->subpaths) {
                find_inlist2join_path_walker((Path*)lfirst(cell), context);
            }
        } break;

        case T_MergeAppend: {
            ListCell* cell = NULL;

            foreach (cell, ((MergeAppendPath*)path)->subpaths) {
                find_inlist2join_path_walker((Path*)lfirst(cell), context);
            }
        } break;

        case T_Material: {
            find_inlist2join_path_walker(((MaterialPath*)path)->subpath, context);
        } break;

        case T_Unique: {
            find_inlist2join_path_walker(((UniquePath*)path)->subpath, context);
        } break;

        case T_PartIterator: {
            find_inlist2join_path_walker(((PartIteratorPath*)path)->subPath, context);
        } break;

        case T_NestLoop:
        case T_HashJoin:
        case T_MergeJoin: {
            JoinPath* jp = (JoinPath*)path;

            find_inlist2join_path_walker(jp->innerjoinpath, context);
            find_inlist2join_path_walker(jp->outerjoinpath, context);
        } break;

        case T_BaseResult: {
            find_inlist2join_path_walker(((ResultPath*)path)->subpath, context);
        } break;

        default:
            break;
    }

    return;
}

/*
 * Name: IsConvertableBaseRel()
 *
 * Brief: Check if the rel is convertable for inlist2join
 *        DFS table is not supported now
 *
 * Parameters:
 *  @in rel: the RelOptInfo object to check
 *  @in rte: the RangeTblEntry object
 *
 * Return: bool. indicate Yes/Not for inlist2join conversion
 */
static bool IsConvertableBaseRel(const RangeTblEntry* rte)
{
    Oid relId = rte->relid;
    bool convertable = true;

    Assert(rte->rtekind == RTE_RELATION);
    Relation rel = RelationIdGetRelation(relId);
    if (rel == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("could not open relation with OID %u", relId)));
    }

    /* Disallow Foreign table case */
    if (RelationIsForeignTable(rel) || RelationIsStream(rel)) {
        convertable = false;
    }

    RelationClose(rel);
    return convertable;
}

/*
 * Name: IsConvertableInlistRestrict()
 *
 * Brief: Check if the given inlist restrict is valid for inlist2join conversion
 *
 * Parameters:
 *	@in restrict: the restrict info that need to check
 *
 * Return bool. indicate Yes/Not to convert
 */
static bool IsConvertableInlistRestrict(RelOptInfo* rel, const RestrictInfo* restrict, RelOrientation orientation)
{
    Assert(restrict != NULL && IsA(restrict->clause, ScalarArrayOpExpr));

    ScalarArrayOpExpr* scalarop = (ScalarArrayOpExpr*)restrict->clause;
    Oid opno = scalarop->opno;
    List* var_list =
        pull_var_clause((Node*)list_nth(scalarop->args, 0), PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
    /*  Apply inlist2join rewrite optimization when the var_list number is 1 */
    if (list_length(var_list) != 1) {
        return false;
    }

    Var* listvar = (Var*)linitial(var_list);
    Oid typoid = listvar->vartype;

    /*
     * Confirm current restrictinfo to see it matchs all following cretarias:
     * 1. array operation is "in-any"
     * 2. array operator is not "="
     * 3. array operation is not in "c1 in LIST" form
     * 4. array expr contains more than one var node, for example
     * 		(c1+c2) in (1,2,3,4,5),
     *		(c1,c2) in ((1,1), (2,2))
     *     var only appear in args's first element, for example
     *           c1 in (1,2,3,4,5)
     *           c1 =  ANY(array[1,2,3,4,5])
     *           others wrong example
     *           12 = ANY (p1.proargtypes)
     *           12 = ANY (array[2275,1245,2588]);
     */
    if (scalarop->useOr == false || !IsEqualOpr(opno) || list_length(scalarop->args) != 2 ||
        !IsA((Node*)list_nth(scalarop->args, 1), Const) || !IsConvertableType(typoid)) {
        return false;
    }

    /*
     * Do not apply inlist2join rewrite optimization if optmode greater than 1 but
     * the inlist length is less than threshold
     */
    int inlist_number = estimate_array_length((Node*)list_nth(scalarop->args, 1));
    if (u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_FORCE &&
        inlist_number < u_sess->opt_cxt.qrw_inlist2join_optmode) {
        return false;
    }

    /* We set hard thresholds only for u_sess->opt_cxt.qrw_inlist2join_optmode is cost_base */
    if (u_sess->opt_cxt.qrw_inlist2join_optmode == QRW_INLIST2JOIN_CBO &&
        belowInlist2JoinThreshold(rel, inlist_number, orientation)) {
        return false;
    }

    Assert(u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_DISABLE);

    return true;
}

/*
 * Name: HasConvertableInlistCond()
 *
 * Brief: make determination if given rel is valid for applying inlist2join optimization
 *
 * Parameters:
 *  @in rel: the RelOptInfo object to check
 *
 * Return: bool. indicate Yes/Not for inlist2join conversion
 */
static bool HasConvertableInlistCond(RelOptInfo* rel)
{
    ListCell* lc = NULL;
    bool convertable = false;

    /*
     * If rel has no restriction info, return false as considered as no convertable
     * inlist2join case.
     */
    if (rel->baserestrictinfo == NULL) {
        return false;
    }

    /* Check if there is place holder var in rel's targetlist */
    List* var_list = pull_var_clause((Node*)rel->reltarget->exprs, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    foreach (lc, var_list) {
        if (!IsA(lfirst(lc), Var)) {
            return false;
        }
    }

    /* Search rel's restrictioninfo to confirm it is inlist2join-convertable */
    foreach (lc, rel->baserestrictinfo) {
        RestrictInfo* restrict = (RestrictInfo*)lfirst(lc);

        /* check if  it contains subplan */
        if (check_subplan_expr((Node*)restrict->clause)) {
            convertable = false;
            break;
        }

        if (IsA(restrict->clause, ScalarArrayOpExpr)) {
            /*
             * If we find any ArrayOp is convertable we think the base-rel is valid for
             * inlist2join conversion then create the SubQueryScan path and take inlist2join
             * into consideration.
             */
            if (IsConvertableInlistRestrict(rel, restrict, rel->orientation)) {
                convertable = true;
            }
        }
    }

    return convertable;
}

/*
 * Name: IsEqualOpr()
 *
 * Brief: determin if given operator ID is for equivalence check
 *
 * Parameters:
 *  @in opno: the opno to check
 *
 * Return: bool. indicate Yes/Not for equivalence
 */
static bool IsEqualOpr(Oid opno)
{
    Relation pg_operator;
    HeapTuple tuple;
    SysScanDesc scan;
    ScanKeyData skey[1];

    /* Scan pg_operator */
    pg_operator = heap_open(OperatorRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, opno);

    scan = systable_beginscan(pg_operator, OperatorOidIndexId, true, NULL, 1, skey);

    /* Only one record should be qualified, and get the relid */
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_operator pgoperform = (Form_pg_operator)GETSTRUCT(tuple);
        if (strcmp(NameStr(pgoperform->oprname), "=") == 0) {
            systable_endscan(scan);
            heap_close(pg_operator, AccessShareLock);
            return true;
        }
    } else {
        elog(LOG, "could not find tuple for operator %u", opno);
    }

    systable_endscan(scan);
    heap_close(pg_operator, AccessShareLock);

    return false;
}

/*
 * Name: IsConvertableType()
 *
 * Brief: determin if given ScalarArrayOpExpr is convertable type for check
 *
 * Parameters:
 *  @in scalarop: the ScalarArrayOpExpr to check
 *
 * Return: bool. indicate Yes/Not for convertable type for check
 */
static inline bool IsConvertableType(Oid typoid)
{
    switch (typoid) {
        case INT1OID:          /* for TINYINT */
        case INT2OID:          /* for SMALLINT */
        case INT4OID:          /* for INTEGER */
        case INT8OID:          /* for BIGINT */
        case NUMERICOID:       /* for NUMERIC */
        case FLOAT4OID:        /* for FLOAT4 */
        case FLOAT8OID:        /* for FLOAT8 */
        case BOOLOID:          /* for BOOLEAN */
        case CHAROID:          /* for CHAR */
        case BPCHAROID:        /* for BPCHAR */
        case VARCHAROID:       /* for VARCHAR */
        case NVARCHAR2OID:     /* for NVARCHAR */
        case TEXTOID:          /* for TEXT */
        case DATEOID:          /* for DATE */
        case TIMEOID:          /* for TIME */
        case TIMETZOID:        /* for TIMEZ */
        case TIMESTAMPOID:     /* for TIMESTAMP */
        case TIMESTAMPTZOID:   /* for TIMESTAMPTZOID */
        case SMALLDATETIMEOID: /* for SMALLDATETIME */
        case INTERVALOID:      /* for INTERVAL */
        case TINTERVALOID:     /* for TINTERVAL */
            return true;
        default:
            elog(DEBUG1, "Inlist2join is not converted for type %u", typoid);
    }
    return false;
}

/*
 * Name: ConvertableInlistMaxNum()
 *
 * Brief: return max num of inlist condition
 *
 * Parameters:
 *  @in opno: in rel: the RelOptInfo object
 *
 * Return: int. the max num of inlist condition
 */
static int ConvertableInlistMaxNum(const RelOptInfo* rel)
{
    int maxnum = 0;

    if (rel->baserestrictinfo != NULL) {
        ListCell* lc = NULL;
        foreach (lc, rel->baserestrictinfo) {
            RestrictInfo* restrict = (RestrictInfo*)lfirst(lc);
            if (IsA(restrict->clause, ScalarArrayOpExpr)) {
                ScalarArrayOpExpr* scalarop = (ScalarArrayOpExpr*)restrict->clause;
                Oid opno = scalarop->opno;

                /*
                 * Confirm:
                 * 1. array operation is "in-any"
                 * 2. there is no dataype coerce
                 * 3. equal condition
                 */
                if (scalarop->useOr && IsA(linitial(scalarop->args), Var) && IsEqualOpr(opno)) {
                    Node* arraynode = (Node*)lsecond(scalarop->args);
                    maxnum = Max(maxnum, estimate_array_length(arraynode));
                }
            }
        }
    }

    return maxnum;
}

/*
 * Name: belowInlist2JoinThreshold()
 *
 * Brief: Check if inlist is small enough that we don't consider inlist join,
 *        else will cause regression due to inaccurate cost estimation
 *
 * Parameters:
 *  @in opno: in rel: the RelOptInfo object
 *
 * Return: bool. indicate Yes/Not for inlist2join conversion
 */
static bool belowInlist2JoinThreshold(const RelOptInfo* rel, int num, RelOrientation orientation)
{
    int tarnum = list_length(rel->reltarget->exprs);
    const int row_threshod = 10;

    if (orientation == REL_ROW_ORIENTED || orientation == REL_ORIENT_UNKNOWN) {
        if (num <= row_threshod) {
            return true;
        }
    } else {
        if (!tarnum) {
            tarnum = 1;
        }
        if (num <= tarnum * 3) {
            return true;
        }
    }

    return false;
}

/*
 * Name: make_rte_with_subquery()
 *
 * Brief: make a new RangeTblEntry with subquery and this query contains inlist2join structure.
 *
 * Parameters:
 *  @in root: the PlannerInfo object that includes all information for planning/optimization
 *  @in rel: the RelOptInfo object with inlist restriction
 *  @in rte: the RangeTblEntry object with no inlist-subquery
 *
 * Return: RangeTblEntry. the new RangeTblEntry with inlist-subquery
 */
static RangeTblEntry* make_rte_with_subquery(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    RangeTblEntry* new_rte = makeNode(RangeTblEntry);
    const char* refname = "__unnamed_subquery__";

    List* colnames_new = NIL;

    ListCell* reltarget = NULL;
    if (rte->eref->colnames) {
        foreach (reltarget, rel->reltarget->exprs) {
            Var* relvar = (Var*)lfirst(reltarget);

            if (relvar->varattno > 0) {
                colnames_new = lappend(colnames_new, list_nth(rte->eref->colnames, relvar->varattno - 1));
            } else if (relvar->varattno < 0) {
                colnames_new = lappend(colnames_new, get_attr_name(relvar->varattno));
            } else {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("encounters invalid varno")));
            }
        }
    }

    new_rte->alias = makeAlias(refname, NIL);
    new_rte->eref = makeAlias(refname, colnames_new);

    rebuild_subquery(root, rel, rte, new_rte);
    new_rte->rtekind = RTE_SUBQUERY;
    new_rte->relid = InvalidOid;
    new_rte->orientation = REL_ORIENT_UNKNOWN;

    /* Binding CTE list */
    new_rte->subquery->cteList = root->parse->cteList;

    /* ----------
     * Flags:
     * - this RTE should be expanded to include descendant tables,
     * - this RTE is in the FROM clause,
     * - this RTE should be checked for appropriate access rights.
     *
     * SubQueries are never checked for access rights.
     * ----------
     */
    new_rte->inh = false; /* never true for subqueries */
    new_rte->inFromCl = true;

    new_rte->requiredPerms = 2;
    new_rte->checkAsUser = InvalidOid;

    return new_rte;
}

/*
 * Name: build_alternative_rel()
 *
 * Brief: build a new RelOptInfo with new rtekind.
 *
 * Parameters:
 *  @in origin: the origin RelOptInfo
 *  @in rtekind: the rtekind of new_rel
 *
 * Return: RelOptInfo. the new RelOptInfo with specific rtekind
 */
RelOptInfo* build_alternative_rel(const RelOptInfo* origin, RTEKind rtekind)
{
    RelOptInfo* rel = NULL;
    Assert(origin != NULL);

    rel = makeNode(RelOptInfo);
    rel->reloptkind = origin->reloptkind;
    rel->relids = bms_make_singleton(origin->relid);
    rel->isPartitionedTable = false;
    rel->partflag = origin->partflag;
    rel->rows = origin->rows;
    rel->reltarget = create_empty_pathtarget();
    rel->reltarget->width = origin->reltarget->width;
    rel->encodedwidth = origin->encodedwidth;
    rel->encodednum = origin->encodednum;
    rel->reltarget->exprs = list_copy(origin->reltarget->exprs);
    rel->baserestrictinfo = (List*)copyObject(origin->baserestrictinfo);
    rel->pathlist = NIL;
    rel->ppilist = NIL;
    rel->cheapest_startup_path = NULL;
    rel->cheapest_total_path = NULL;
    rel->cheapest_unique_path = NULL;
    rel->relid = origin->relid;

    rel->rtekind = rtekind;

    rel->min_attr = origin->min_attr;
    rel->max_attr = origin->max_attr;
    rel->attr_needed = origin->attr_needed;
    rel->attr_widths = origin->attr_widths;

    rel->indexlist = NIL;
    rel->pages = 0;
    rel->tuples = 0;
    rel->multiple = 0;
    rel->allvisfrac = 0;
    rel->pruning_result = NULL;
    rel->pruning_result_for_index_usable = NULL;
    rel->pruning_result_for_index_unusable = NULL;
    rel->partItrs = -1;
    rel->partItrs_for_index_usable = -1;
    rel->partItrs_for_index_unusable = -1;
    rel->subplan = NULL;
    rel->subroot = NULL;
    rel->subplan_params = NIL;
    rel->fdwroutine = NULL;
    rel->fdw_private = NULL;
    rel->baserestrictcost.startup = 0;
    rel->baserestrictcost.per_tuple = 0;
    rel->joininfo = (List*)copyObject(origin->joininfo);
    rel->subplanrestrictinfo = (List*)copyObject(origin->subplanrestrictinfo);
    rel->has_eclass_joins = false;
    rel->varratio = NIL;
    rel->lateral_relids = origin->lateral_relids;

    rel->alternatives = NIL;
    rel->base_rel = (RelOptInfo*)origin;

    return rel;
}

/*
 * Name: get_attr_name()
 *
 * Brief: extract the correct name for a system attribute
 *
 * Parameters:
 *  @in attrnum: the attrnum to get attrname
 *
 * Return: char *. the attrname
 */
static char* get_attr_name(int attrnum)
{
    Assert(attrnum < 0);
    switch (attrnum) {
        case SelfItemPointerAttributeNumber:
            return (char*)"ctid";
        case ObjectIdAttributeNumber:
            return (char*)"oid";
        case MinTransactionIdAttributeNumber:
            return (char*)"xmin";
        case MinCommandIdAttributeNumber:
            return (char*)"cmin";
        case MaxTransactionIdAttributeNumber:
            return (char*)"xmax";
        case MaxCommandIdAttributeNumber:
            return (char*)"cmax";
        case TableOidAttributeNumber:
            return (char*)"tableoid";
#ifdef PGXC
        case XC_NodeIdAttributeNumber:
            return (char*)"xc_node_id";
        case BucketIdAttributeNumber:
            return (char*)"tablebucketid";
        case UidAttributeNumber:
            return (char*)"gs_tuple_uid";
#endif
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid column number %d for table \n", attrnum)));
            break;
    }
    return NULL; /* keep compiler quiet */
}

/*
 * Name: convert_constarray_to_simple()
 *
 * Brief: Convert scalararray to simple const list to help build ValueScan's valuelist
 *        later steps.
 *
 * Parameters:
 *  @in scalar: the ScalarArrayOpExpr object to be converted
 *  @in listvar: the Var object of inlist column
 *  @in paramtype: the Oid of these simple consts
 *
 * Return: List *. the converted simple const list
 */
static List* convert_constarray_to_simple(Const* combined_const, Var* listvar, Oid* paramtype)
{
    int i;
    int nitems;
    ArrayType* arr = NULL;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    char* s = NULL;
    bits8* bitmap = NULL;
    int bitmask;
    bool elemNull = false;
    Node* simple_const = NULL;
    List* const_list = NIL;

    arr = DatumGetArrayTypeP(combined_const->constvalue);
    *paramtype = ARR_ELEMTYPE(arr);
    nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

    /* Get array infomation */
    get_typlenbyvalalign(*paramtype, &typlen, &typbyval, &typalign);

    /* Loop over the array elements */
    s = (char*)ARR_DATA_PTR(arr);
    bitmap = ARR_NULLBITMAP(arr);
    bitmask = 1;
    for (i = 0; i < nitems; i++) {
        Datum elt;

        /* Get array element, checking for NULL */
        if (bitmap && (*bitmap & bitmask) == 0) {
            elemNull = true;
            simple_const = (Node*)makeConst(
                *paramtype, combined_const->consttypmod, combined_const->constcollid, typlen, (Datum)0, true, typbyval);
        } else {
            elemNull = false;
            elt = fetch_att(s, typbyval, typlen);
            s = att_addlength_pointer(s, typlen, s);
            s = (char*)att_align_nominal(s, typalign);

            simple_const = (Node*)makeConst(
                *paramtype, combined_const->consttypmod, combined_const->constcollid, typlen, elt, false, typbyval);
        }
        const_list = lappend(const_list, simple_const);
        if (bitmap != NULL) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    return const_list;
}

/*
 * Name: rebuild_subquery()
 *
 * Brief: create the SubQuery object to for new_rte.
 *
 * Parameters:
 *  @in root: the PlannerInfo object that includes all information for planning/optimization
 *  @in rel: the RelOptInfo object for inlist2join
 *  @in rte: the RangeTblEntry object with no inlist2join-subquery
 *  @in new_rte: the RangeTblEntry object with inlist2join-subquery
 *
 * Return: void.
 */
static void rebuild_subquery(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, RangeTblEntry* new_rte)
{
    Query* query = makeNode(Query);
    query->commandType = CMD_SELECT;
    query->querySource = QSRC_ORIGINAL;
    query->canSetTag = true;
    query->resultRelation = 0;
    query->hasSubLinks = true;
    query->mergeTarget_relation = 0;
    query->targetList = NIL;

    query->rtable = list_make1((RangeTblEntry*)copyObject(rte));

    List* colnames = rte->eref->colnames;

    ListCell* lc1 = NULL;

    /*
     * Build target list for new SubQuery and RelOptInfo
     *
     * Note:
     * 1. SubQuery's targetlist, as we are buiding a new separate Query struct,
     *    the targetlist's varno is started from 1, and varattno keeps origin
     *
     * 2. RelOptInfo's targetlist, as we are buiding a new RelOptInfo to replace
     *    the original one from the, the varno keeps origin, but the subquery's
     *    return attribues is start from 1
     */
    /* 1. Build SubQuery's target list */
    foreach (lc1, rel->reltarget->exprs) {
        Assert(IsA(lfirst(lc1), Var));
        Var* relvar = (Var*)copyObject((Var*)lfirst(lc1));

        /* In Separate query, varno is always 1 */
        relvar->varno = SUBQUERY_VARNO;

        char* varname = NULL;
        if (relvar->varattno < 0)
            varname = get_attr_name(relvar->varattno);
        else if (relvar->varattno > 0)
            varname = strVal(list_nth(colnames, relvar->varattno - 1));
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("There is no exist vararrno with 0")));

        /* Make TargetEntry for Query */
        TargetEntry* entry = makeTargetEntry((Expr*)relvar, /* expr */
            (AttrNumber)(list_length(query->targetList) + 1),             /* resno */
            varname,
            false);

        entry->resorigtbl = rte->relid;
        entry->resorigcol = relvar->varattno;
        query->targetList = lappend(query->targetList, entry);
    }

    /* 2. Build RelOptInfo's targetlist as alternative rel */
    AttrNumber attno = SUBQUERY_ATTNO;
    foreach (lc1, rel->reltarget->exprs) {
        Var* old_var = (Var*)copyObject((Var*)lfirst(lc1));
        Var* new_var = (Var*)copyObject((Var*)lfirst(lc1));

        /* Record the var's varattno's change */
        {
            new_var->varattno = attno;

            RewriteVarMapping* mapping = (RewriteVarMapping*)palloc0(sizeof(RewriteVarMapping));
            mapping->old_var = old_var;
            mapping->new_var = new_var;
            mapping->need_fix = false;

            /* Record the changed vars when varattno is changed */
            root->var_mapping_rels = bms_add_member(root->var_mapping_rels, (int)old_var->varno);
            root->var_mappings = lappend(root->var_mappings, mapping);
        }

        attno++;
    }

    /* 3. Build SubQuery's rtf&fromlist */
    RangeTblRef* rtr = makeNode(RangeTblRef);
    rtr->rtindex = SUBQUERY_VARNO;
    List* fromlist = list_make1(rtr);

    Assert(rel->baserestrictinfo);

    /*
     * 4. Build SubQuery's ValueScan part
     * Search the RelOptInto's restriction info to find In-List case and try to convert
     * it to "col in <list>" form to let sublink_pullup to do actual inlist2join conversion
     *
     * example: select * from t1 where t1.c1 in (1,2,3,4)
     *       => select * from (select * from t1 where t1.c1 in (select column1 from (values(1),(2),(3),(4))))
     */
    List* args = NIL;
    ListCell* lc = NULL;
    foreach (lc, rel->baserestrictinfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (!(IsA(rinfo->clause, ScalarArrayOpExpr))) {
            /*
             * Fix vars in restriction list for a case where a Var is changed in new
             * SubQuery and its origin ref var
             */
            (void)fix_subquery_vars_expr((Node*)rinfo->clause, rel->relid, (Index)SUBQUERY_VARNO);

            /* Put it to args if restrict is not a "INLIST" case */
            args = lappend(args, rinfo->clause);
        } else {
            /* Exclude invalid inlist cond, just do Fix-var and continue */
            if (!IsConvertableInlistRestrict(rel, rinfo, rte->orientation)) {
                (void)fix_subquery_vars_expr((Node*)rinfo->clause, rel->relid, (Index)SUBQUERY_VARNO);
                args = lappend(args, rinfo->clause);

                continue;
            }

            ScalarArrayOpExpr* scalar = (ScalarArrayOpExpr*)copyObject(rinfo->clause);

            /*
             * Build the inlist-converted SubLink with content values((),(),()), for example
             *
             * C1 IN (1,2,3,4);
             *    =>
             * C1 = ANY (
             *		   SELECT column1
             *		   FROM
             *		   (values(1),(2),(3),(4)) as x(column1)
             *      )
             */
            SubLink* sublink = makeNode(SubLink);
            sublink->subLinkType = ANY_SUBLINK;
            sublink->operName = list_make1(makeString((char *)"="));
            sublink->location = -1;

            Oid colid = exprCollation((Node*)linitial((List*)scalar->args));
            List* args_in_opexpr = NIL;
            Oid paramtype = 0;
            Var* listvar = fix_subquery_vars_expr((Node*)scalar->args, rel->relid, (Index)SUBQUERY_VARNO);
            Const* combined_const = (Const*)list_nth(scalar->args, 1);
            List* const_list = convert_constarray_to_simple(combined_const, listvar, &paramtype);

            args_in_opexpr = lappend(args_in_opexpr, linitial((List*)scalar->args));

            Param* param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = 1;
            param->paramtype = paramtype;
            param->paramtypmod = -1;
            param->paramcollid = listvar->varcollid;
            param->location = -1;
            param->tableOfIndexTypeList = NULL;
            args_in_opexpr = lappend(args_in_opexpr, param);

            OpExpr* opexpr = makeNode(OpExpr);
            opexpr->opno = scalar->opno;
            opexpr->opfuncid = scalar->opfuncid;
            opexpr->opresulttype = BOOLOID;
            opexpr->opcollid = 0;
            opexpr->inputcollid = listvar->varcollid;
            opexpr->location = -1;
            sublink->testexpr = (Node*)opexpr;

            opexpr->args = args_in_opexpr;

            /* Build SubLink's "subselect" part */
            Query* subselect = makeNode(Query);
            subselect->commandType = CMD_SELECT;
            subselect->querySource = QSRC_ORIGINAL;
            subselect->canSetTag = true;
            subselect->resultRelation = 0;
            subselect->mergeTarget_relation = 0;

            /* Build SubLink's FromExpr but no Quals */
            RangeTblRef* rtf = makeNode(RangeTblRef);
            rtf->rtindex = VALUES_SUBLINK_VARNO;
            subselect->jointree = makeFromExpr(list_make1(rtf), NULL);

            /*
             * Build SubLink's TargetEntry
             *
             * This var is used in RTE *VALUE*, there is one RTE in the query and one column in the RTE
             * so let varno and varattno are 1, the type is same as the listvar->vartype
             */
            TargetEntry* target_entry = makeNode(TargetEntry);

            target_entry->expr = (Expr*)makeVar((Index)VALUES_SUBLINK_VARNO, /* varno for values((a),(b),(c)) */
                VALUES_SUBLINK_VARATTNO,                              /* varattno for values((a),(b),(c)) */
                paramtype,
                -1,
                colid,
                0);

            /* There is only one target column */
            target_entry->resno = VALUES_SUBLINK_VARATTNO;
            target_entry->resname = (char *)"column1";
            target_entry->resorigtbl = 0;
            target_entry->ressortgroupref = 0;
            target_entry->resorigcol = 0;
            subselect->targetList = list_make1(target_entry);

            /* Build VALUES part and put it as a RTE object under SubLink's subselect part */
            List* values_lists = NIL;
            List* collations = NIL;

            ListCell* lc_value = NULL;
            foreach (lc_value, const_list) {
                Const* const_value = (Const*)copyObject((Const*)lfirst(lc_value));
                values_lists = lappend(values_lists, list_make1(const_value));
            }
            collations = lappend_oid(collations, combined_const->constcollid);
            RangeTblEntry* sublink_rte = addRangeTableEntryForValues(NULL, values_lists, collations, NULL, true);

            subselect->rtable = list_make1(sublink_rte);

            /* Bind SubLink's subselect part */
            sublink->subselect = (Node*)subselect;

            args = lappend(args, sublink);
        }
    }

    /* After put the restrictioninfo into subquery we release it on top level */
    rel->baserestrictinfo = NIL;

    /* Build FromExpr */
    Expr* andexpr = makeBoolExpr(AND_EXPR, args, -1);
    query->jointree = makeFromExpr(fromlist, (Node*)andexpr);

#ifdef ENABLE_MULTIPLE_NODES
    /* Set can_push flag of query. */
    mark_query_canpush_flag((Node *) query);
#endif

    new_rte->subquery = query;
}
