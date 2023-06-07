/* -------------------------------------------------------------------------
 *
 * parse_agg.c
 *	  handle aggregates and window functions in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_agg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "parser/parse_expr.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/htup.h"
#include "catalog/pg_aggregate.h"
#include "utils/syscache.h"
#endif

typedef struct {
    ParseState* pstate;
    Query* qry;
    PlannerInfo* root;
    List* groupClauses;
    List* groupClauseCommonVars;
    bool have_non_var_grouping;
    List** func_grouped_rels;
    int sublevels_up;
    bool in_agg_direct_args;
} check_ungrouped_columns_context;

typedef struct
{
	ParseState *pstate;
	int			min_varlevel;
	int			min_agglevel;
	int			sublevels_up;
} check_agg_arguments_context;

static int	check_agg_arguments(ParseState *pstate,
								List *directargs,
								List *args,
								Expr *filter);
static bool check_agg_arguments_walker(Node *node,
									   check_agg_arguments_context *context);
static void check_agglevels_and_constraints(ParseState *pstate, Node *expr);
static void check_ungrouped_columns(Node* node, ParseState* pstate, Query* qry, List* groupClauses,
    List* groupClauseVars, bool have_non_var_grouping, List** func_grouped_rels);
static bool check_ungrouped_columns_walker(Node* node, check_ungrouped_columns_context* context);
static void finalize_grouping_exprs(
    Node* node, ParseState* pstate, Query* qry, List* groupClauses, PlannerInfo* root, bool have_non_var_grouping);

static bool finalize_grouping_exprs_walker(Node* node, check_ungrouped_columns_context* context);
static List* expand_groupingset_node(GroupingSet* gs);
#ifndef ENABLE_MULTIPLE_NODES
static void find_rownum_in_groupby_clauses(Rownum *rownumVar, check_ungrouped_columns_context* context);
#endif
/*
 * transformAggregateCall -
 *		Finish initial transformation of an aggregate call
 *
 * parse_func.c has recognized the function as an aggregate, and has set up
 * all the fields of the Aggref except args, aggorder, aggdistinct and
 * agglevelsup.  The passed-in args list has been through standard expression
 * transformation, while the passed-in aggorder list hasn't been transformed
 * at all.
 *
 * Here we convert the args list into a targetlist by inserting TargetEntry
 * nodes, and then transform the aggorder and agg_distinct specifications to
 * produce lists of SortGroupClause nodes.	(That might also result in adding
 * resjunk expressions to the targetlist.)
 *
 * We must also determine which query level the aggregate actually belongs to,
 * set agglevelsup accordingly, and mark p_hasAggs true in the corresponding
 * pstate level.
 */
void transformAggregateCall(ParseState* pstate, Aggref* agg, List* args, List* aggorder, bool agg_distinct)
{
#define anyenum_typeoid 3500
    List* tlist = NIL;
    List* torder = NIL;
    List* tdistinct = NIL;
    AttrNumber attno = 1;
    int save_next_resno;
    int min_varlevel;
    ListCell* lc = NULL;
#ifdef PGXC
    HeapTuple aggTuple;
    Form_pg_aggregate aggform;
#endif /* PGXC */

    if (AGGKIND_IS_ORDERED_SET(agg->aggkind)) {
        /*
         * The ordered-set aggs contain direct args and aggregated args.
         * The direct args are saved at the first "numDirectArgs" args,
         * and the aggregated args are at the tail. We must split them apart.
         */
        int numDirectArgs = list_length(args) - list_length(aggorder);
        List* aargs = NIL;
        ListCell* lc1 = NULL;
        ListCell* lc2 = NULL;

        Assert(numDirectArgs >= 0);

        aargs = list_copy_tail(args, numDirectArgs);
        agg->aggdirectargs = list_truncate(args, numDirectArgs);

        /*
         * We should save the sort information for ordered-set agg, so we
         * need build a tlist (normally only have a target entry) which contains
         * aggregated args (list of Exprs). And we need save the regarding order
         * target which is use to transformed to SortGroupClause.
         */
        forboth(lc1, aargs, lc2, aggorder)
        {
            TargetEntry* tle = makeTargetEntry((Expr*)lfirst(lc1), attno++, NULL, false);
            tlist = lappend(tlist, tle);

            torder = addTargetToSortList(pstate, tle, torder, tlist, (SortBy*)lfirst(lc2), true); /* fix unknowns */
        }

        /* DISTINCT cannot be used in an ordered-set agg */
        Assert(!agg_distinct);
    } else {
        /* Normal aggregate dose not have direct args */
        agg->aggdirectargs = NIL;

        /*
         * Transform the plain list of Exprs into a targetlist.  We don't bother
         * to assign column names to the entries.
         */
        foreach (lc, args) {
            Expr* arg = (Expr*)lfirst(lc);
            TargetEntry* tle = makeTargetEntry(arg, attno++, NULL, false);
            tlist = lappend(tlist, tle);
        }

        /*
         * If we have an ORDER BY, transform it.  This will add columns to the
         * tlist if they appear in ORDER BY but weren't already in the arg
         * list.  They will be marked resjunk = true so we can tell them apart
         * from regular aggregate arguments later.
         *
         * We need to mess with p_next_resno since it will be used to number
         * any new targetlist entries.
         */
        save_next_resno = pstate->p_next_resno;
        pstate->p_next_resno = attno;

        pstate->shouldCheckOrderbyCol = (agg_distinct && !ALLOW_ORDERBY_UNDISTINCT_COLUMN && !IsInitdb && DB_IS_CMPT(B_FORMAT));
        torder = transformSortClause(pstate,
            aggorder,
            &tlist,
            EXPR_KIND_WINDOW_ORDER,
            true,  /* fix unknowns */
            true); /* force SQL99 rules */

        pstate->shouldCheckOrderbyCol = false;

        /*
         * If we have DISTINCT, transform that to produce a distinctList.
         */
        if (agg_distinct) {
            tdistinct = transformDistinctClause(pstate, &tlist, torder, true);

            /*
             * Remove this check if executor support for hashed distinct for
             * aggregates is ever added.
             */
            foreach (lc, tdistinct) {
                SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);

                if (!OidIsValid(sortcl->sortop)) {
                    Node* expr = get_sortgroupclause_expr(sortcl, tlist);

                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                            errmsg(
                                "could not identify an ordering operator for type %s", format_type_be(exprType(expr))),
                            errdetail("Aggregates with DISTINCT must be able to sort their inputs."),
                            parser_errposition(pstate, exprLocation(expr))));
                }
            }
        }

        pstate->p_next_resno = save_next_resno;
    }

    /* Update the Aggref with the transformation results */
    agg->args = tlist;
    agg->aggorder = torder;
    agg->aggdistinct = tdistinct;

    if (pstate->p_is_flt_frame) {
        check_agglevels_and_constraints(pstate, (Node*)agg);
    } else {
        /*
        * The aggregate's level is the same as the level of the lowest-level
        * variable or aggregate in its arguments; or if it contains no variables
        * at all, we presume it to be local.
        */
        min_varlevel = find_minimum_var_level((Node*)agg->args);
        /*
        * An aggregate can't directly contain another aggregate call of the same
        * level (though outer aggs are okay).	We can skip this check if we
        * didn't find any local vars or aggs.
        */
        if (min_varlevel == 0) {
            if (pstate->p_hasAggs && checkExprHasAggs((Node*)agg->args)) {
                ereport(ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                        errmsg("aggregate function calls cannot be nested"),
                        parser_errposition(pstate, locate_agg_of_level((Node*)agg->args, 0))));
            }
        }

        /* It can't contain set-returning functions either */
        if (checkExprHasSetReturningFuncs((Node*)agg->args)) {
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                    errmsg("aggregate function calls cannot contain set-returning function calls"),
                    parser_errposition(pstate, locate_srfunc((Node*)agg->args))));
        }

        /* It can't contain window functions either */
        if (pstate->p_hasWindowFuncs && checkExprHasWindowFuncs((Node*)agg->args)) {
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                    errmsg("aggregate function calls cannot contain window function calls"),
                    parser_errposition(pstate, locate_windowfunc((Node*)agg->args))));
        }

        if (min_varlevel < 0) {
            min_varlevel = 0;
        }
        agg->agglevelsup = min_varlevel;

        /* Mark the correct pstate as having aggregates */
        while (min_varlevel-- > 0)
            pstate = pstate->parentParseState;
        pstate->p_hasAggs = true;
        /*
        * Complain if we are inside a LATERAL subquery of the aggregation query.
        * We must be in its FROM clause, so the aggregate is misplaced.
        */
        if (pstate->p_lateral_active)
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("aggregates not allowed in FROM clause"),
                parser_errposition(pstate, agg->location)));
    }

#ifdef PGXC
    /*
     * Return data type of PGXC Datanode's aggregate should always return the
     * result of transition function, that is expected by collection function
     * on the Coordinator.
     * Look up the aggregate definition and replace agg->aggtype
     */

    aggTuple = SearchSysCache(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid), 0, 0, 0);
    if (!HeapTupleIsValid(aggTuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for aggregate %u", agg->aggfnoid)));
    aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);
    agg->aggtrantype = aggform->aggtranstype;
    agg->agghas_collectfn = OidIsValid(aggform->aggcollectfn);

    /*
     * We need ensure upgrade successfully when view include avg function,
     * otherwise may lead to similar error: operator does not exist: bigint[] = integer.
     *
     * For example:
     * 			create view t1_v as select a from t1 group by a having avg(a) = 10;
     * For user-defined enum type, do not replace agg->aggtype here, otherwise may lead to error:
     * operator does not exist: (user-defined enum type) = anyenum.
     */
    if (IS_PGXC_DATANODE && !isRestoreMode && !u_sess->catalog_cxt.Parse_sql_language && !IsInitdb &&
        !u_sess->attr.attr_common.IsInplaceUpgrade && !IS_SINGLE_NODE && (anyenum_typeoid != agg->aggtrantype))
        agg->aggtype = agg->aggtrantype;

    ReleaseSysCache(aggTuple);
#endif
}

/*
 * transformWindowFuncCall -
 *		Finish initial transformation of a window function call
 *
 * parse_func.c has recognized the function as a window function, and has set
 * up all the fields of the WindowFunc except winref.  Here we must (1) add
 * the WindowDef to the pstate (if not a duplicate of one already present) and
 * set winref to link to it; and (2) mark p_hasWindowFuncs true in the pstate.
 * Unlike aggregates, only the most closely nested pstate level need be
 * considered --- there are no "outer window functions" per SQL spec.
 */
void transformWindowFuncCall(ParseState* pstate, WindowFunc* wfunc, WindowDef* windef)
{
    /*
     * A window function call can't contain another one (but aggs are OK). XXX
     * is this required by spec, or just an unimplemented feature?
     */
    if (pstate->p_hasWindowFuncs && checkExprHasWindowFuncs((Node*)wfunc->args)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("window function calls cannot be nested"),
                parser_errposition(pstate, locate_windowfunc((Node*)wfunc->args))));
    }

    /*
     * If the OVER clause just specifies a window name, find that WINDOW
     * clause (which had better be present).  Otherwise, try to match all the
     * properties of the OVER clause, and make a new entry in the p_windowdefs
     * list if no luck.
     */
    if (windef->name) {
        Index winref = 0;
        ListCell* lc = NULL;

        AssertEreport(windef->refname == NULL && windef->partitionClause == NIL && windef->orderClause == NIL &&
                          windef->frameOptions == FRAMEOPTION_DEFAULTS,
            MOD_OPT,
            "");

        foreach (lc, pstate->p_windowdefs) {
            WindowDef* refwin = (WindowDef*)lfirst(lc);

            winref++;
            if (refwin->name && strcmp(refwin->name, windef->name) == 0) {
                wfunc->winref = winref;
                break;
            }
        }
        if (lc == NULL) { /* didn't find it? */
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("window \"%s\" does not exist", windef->name),
                    parser_errposition(pstate, windef->location)));
        }
    } else {
        Index winref = 0;
        ListCell* lc = NULL;

        foreach (lc, pstate->p_windowdefs) {
            WindowDef* refwin = (WindowDef*)lfirst(lc);

            winref++;
            if (refwin->refname && windef->refname && strcmp(refwin->refname, windef->refname) == 0)
                /* matched on refname */;
            else if (!refwin->refname && !windef->refname)
                /* matched, no refname */;
            else
                continue;
            if (equal(refwin->partitionClause, windef->partitionClause) &&
                equal(refwin->orderClause, windef->orderClause) && refwin->frameOptions == windef->frameOptions &&
                equal(refwin->startOffset, windef->startOffset) && equal(refwin->endOffset, windef->endOffset)) {
                /* found a duplicate window specification */
                wfunc->winref = winref;
                break;
            }
        }

        /* didn't find it? */
        if (lc == NULL) {
            pstate->p_windowdefs = lappend(pstate->p_windowdefs, windef);
            wfunc->winref = list_length(pstate->p_windowdefs);
        }
    }

    pstate->p_hasWindowFuncs = true;
}

/*
 * parseCheckAggregates
 *	Check for aggregates where they shouldn't be and improper grouping.
 *
 *	Ideally this should be done earlier, but it's difficult to distinguish
 *	aggregates from plain functions at the grammar level.  So instead we
 *	check here.  This function should be called after the target list and
 *	qualifications are finalized.
 */
void parseCheckAggregates(ParseState* pstate, Query* qry)
{
    List* gset_common = NIL;
    List* groupClauses = NIL;
    List* groupClauseCommonVars = NIL;
    bool have_non_var_grouping = false;
    List* func_grouped_rels = NIL;
    ListCell* l = NULL;
    bool hasJoinRTEs = false;
    bool hasSelfRefRTEs = false;
    PlannerInfo* root = NULL;
    Node* clause = NULL;

    /* This should only be called if we found aggregates or grouping */
    AssertEreport(pstate->p_hasAggs || qry->groupClause || qry->havingQual || qry->groupingSets,
        MOD_OPT,
        "only be called if we found aggregates or grouping");

    /*
     * If we have grouping sets, expand them and find the intersection of all
     * sets.
     */
    if (qry->groupingSets) {
        /*
         * The limit of 4096 is arbitrary and exists simply to avoid resource
         * issues from pathological constructs.
         */
        List* gsets = expand_grouping_sets(qry->groupingSets, 4096);

        if (gsets == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
                    errmsg("too many grouping sets present (max 4096)"),
                    parser_errposition(pstate,
                        qry->groupClause ? exprLocation((Node*)qry->groupClause)
                                         : exprLocation((Node*)qry->groupingSets))));

        /*
         * The intersection will often be empty, so help things along by
         * seeding the intersect with the smallest set.
         */
        gset_common = (List*)linitial(gsets);

        if (gset_common != NULL) {
            for_each_cell(l, lnext(list_head(gsets))) {
                gset_common = list_intersection_int(gset_common, (List*)lfirst(l));
                if (gset_common == NULL) {
                    break;
                }
            }
        }

        /*
         * If there was only one grouping set in the expansion, AND if the
         * groupClause is non-empty (meaning that the grouping set is not
         * empty either), then we can ditch the grouping set and pretend we
         * just had a normal GROUP BY.
         */
        if (list_length(gsets) == 1 && qry->groupClause) {
            qry->groupingSets = NIL;
        }
    }
    /*
     * Scan the range table to see if there are JOIN or self-reference CTE
     * entries.  We'll need this info below.
     */
    hasJoinRTEs = hasSelfRefRTEs = false;
    foreach (l, pstate->p_rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

        if (rte->rtekind == RTE_JOIN) {
            hasJoinRTEs = true;
        } else if (rte->rtekind == RTE_CTE && rte->self_reference) {
            hasSelfRefRTEs = true;
        }
    }

    /*
     * Aggregates must never appear in WHERE or JOIN/ON clauses.
     *
     * (Note this check should appear first to deliver an appropriate error
     * message; otherwise we are likely to complain about some innocent
     * variable in the target list, which is outright misleading if the
     * problem is in WHERE.)
     */
    if (checkExprHasAggs(qry->jointree->quals)) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("aggregates not allowed in WHERE clause"),
                parser_errposition(pstate, locate_agg_of_level(qry->jointree->quals, 0))));
    }
    if (checkExprHasAggs((Node*)qry->jointree->fromlist)) { 
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("aggregates not allowed in JOIN conditions"),
                parser_errposition(pstate, locate_agg_of_level((Node*)qry->jointree->fromlist, 0))));
    }

    /*
     * No aggregates allowed in GROUP BY clauses, either.
     *
     * While we are at it, build a list of the acceptable GROUP BY expressions
     * for use by check_ungrouped_columns().
     */
    foreach (l, qry->groupClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(l);
        TargetEntry* expr = NULL;

        expr = get_sortgroupclause_tle(grpcl, qry->targetList);
        if (expr == NULL) {
            continue; /* probably cannot happen */
        }
        if (checkExprHasAggs((Node*)expr->expr)) {
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                    errmsg("aggregates not allowed in GROUP BY clause"),
                    parser_errposition(pstate, locate_agg_of_level((Node*)expr->expr, 0))));
        }
        groupClauses = lcons(expr, groupClauses);
    }

    /*
     * If there are join alias vars involved, we have to flatten them to the
     * underlying vars, so that aliased and unaliased vars will be correctly
     * taken as equal.	We can skip the expense of doing this if no rangetable
     * entries are RTE_JOIN kind. We use the planner's flatten_join_alias_vars
     * routine to do the flattening; it wants a PlannerInfo root node, which
     * fortunately can be mostly dummy.
     */
    if (hasJoinRTEs) {
        root = makeNode(PlannerInfo);
        root->parse = qry;
        root->planner_cxt = CurrentMemoryContext;
        root->hasJoinRTEs = true;

        groupClauses = (List*)flatten_join_alias_vars(root, (Node*)groupClauses);
    } else
        root = NULL; /* keep compiler quiet */

    /*
     * Detect whether any of the grouping expressions aren't simple Vars; if
     * they're all Vars then we don't have to work so hard in the recursive
     * scans.  (Note we have to flatten aliases before this.)
     *
     * Track Vars that are included in all grouping sets separately in
     * groupClauseCommonVars, since these are the only ones we can use to
     * check for functional dependencies.
     */
    have_non_var_grouping = false;
    foreach (l, groupClauses) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        if (!IsA(tle->expr, Var)) {
            have_non_var_grouping = true;
        } else if (!qry->groupingSets || list_member_int(gset_common, tle->ressortgroupref)) {
            groupClauseCommonVars = lappend(groupClauseCommonVars, tle->expr);
        }
    }

    /*
     * Check the targetlist and HAVING clause for ungrouped variables.
     *
     * Note: because we check resjunk tlist elements as well as regular ones,
     * this will also find ungrouped variables that came from ORDER BY and
     * WINDOW clauses.	For that matter, it's also going to examine the
     * grouping expressions themselves --- but they'll all pass the test ...
     *
     * We also finalize GROUPING expressions, but for that we need to traverse
     * the original (unflattened) clause in order to modify nodes.
     */
    clause = (Node*)qry->targetList;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses, root, have_non_var_grouping);
    if (hasJoinRTEs) {
        clause = flatten_join_alias_vars(root, clause);
    }
    check_ungrouped_columns(
        clause, pstate, qry, groupClauses, groupClauseCommonVars, have_non_var_grouping, &func_grouped_rels);

    clause = (Node*)qry->havingQual;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses, root, have_non_var_grouping);
    if (hasJoinRTEs) {
        clause = flatten_join_alias_vars(root, clause);
    }
    check_ungrouped_columns(
        clause, pstate, qry, groupClauses, groupClauseCommonVars, have_non_var_grouping, &func_grouped_rels);

    /*
     * Per spec, aggregates can't appear in a recursive term.
     */
    if (pstate->p_hasAggs && hasSelfRefRTEs) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_RECURSION),
                errmsg("aggregate functions not allowed in a recursive query's recursive term"),
                parser_errposition(pstate, locate_agg_of_level((Node*)qry, 0))));
    }
}

/*
 * parseCheckWindowFuncs
 *	Check for window functions where they shouldn't be.
 *
 *	We have to forbid window functions in WHERE, JOIN/ON, HAVING, GROUP BY,
 *	and window specifications.	(Other clauses, such as RETURNING and LIMIT,
 *	have already been checked.)  Transformation of all these clauses must
 *	be completed already.
 */
void parseCheckWindowFuncs(ParseState* pstate, Query* qry)
{
    ListCell* l = NULL;

    /* This should only be called if we found window functions */
    AssertEreport(pstate->p_hasWindowFuncs, MOD_OPT, "Only deal with WindowFuncs here");

    if (checkExprHasWindowFuncs(qry->jointree->quals)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("window functions not allowed in WHERE clause"),
                parser_errposition(pstate, locate_windowfunc(qry->jointree->quals))));
    }
    if (checkExprHasWindowFuncs((Node*)qry->jointree->fromlist)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("window functions not allowed in JOIN conditions"),
                parser_errposition(pstate, locate_windowfunc((Node*)qry->jointree->fromlist))));
    }
    if (checkExprHasWindowFuncs(qry->havingQual)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("window functions not allowed in HAVING clause"),
                parser_errposition(pstate, locate_windowfunc(qry->havingQual))));
    }

    foreach (l, qry->groupClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(l);
        Node* expr = NULL;

        expr = get_sortgroupclause_expr(grpcl, qry->targetList);
        if (checkExprHasWindowFuncs(expr)) {
            ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                    errmsg("window functions not allowed in GROUP BY clause"),
                    parser_errposition(pstate, locate_windowfunc(expr))));
        }
    }

    foreach (l, qry->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(l);
        ListCell* l2 = NULL;

        foreach (l2, wc->partitionClause) {
            SortGroupClause* grpcl = (SortGroupClause*)lfirst(l2);
            Node* expr = NULL;

            expr = get_sortgroupclause_expr(grpcl, qry->targetList);
            if (checkExprHasWindowFuncs(expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("window functions not allowed in window definition"),
                        parser_errposition(pstate, locate_windowfunc(expr))));
            }
        }
        foreach (l2, wc->orderClause) {
            SortGroupClause* grpcl = (SortGroupClause*)lfirst(l2);
            Node* expr = NULL;

            expr = get_sortgroupclause_expr(grpcl, qry->targetList);
            if (checkExprHasWindowFuncs(expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("window functions not allowed in window definition"),
                        parser_errposition(pstate, locate_windowfunc(expr))));
            }
        }
        /* startOffset and limitOffset were checked in transformFrameOffset */
    }
}

/*
 * Aggregate functions and grouping operations (which are combined in the spec
 * as <set function specification>) are very similar with regard to level and
 * nesting restrictions (though we allow a lot more things than the spec does).
 * Centralise those restrictions here.
 */
static void
check_agglevels_and_constraints(ParseState *pstate, Node *expr)
{
	List	   *directargs = NIL;
	List	   *args = NIL;
	Expr	   *filter = NULL;
	int			min_varlevel;
	int			location = -1;
	Index	   *p_levelsup;
	const char *err;
	bool		errkind;
	bool		isAgg = IsA(expr, Aggref);

	if (isAgg)
	{
		Aggref	   *agg = (Aggref *) expr;

		directargs = agg->aggdirectargs;
		args = agg->args;
		location = agg->location;
		p_levelsup = &agg->agglevelsup;
	}
	else
	{
		GroupingFunc *grp = (GroupingFunc *) expr;

		args = grp->args;
		location = grp->location;
		p_levelsup = &grp->agglevelsup;
	}

	/*
	 * Check the arguments to compute the aggregate's level and detect
	 * improper nesting.
	 */
	min_varlevel = check_agg_arguments(pstate,
									   directargs,
									   args,
									   filter);

	*p_levelsup = min_varlevel;

	/* Mark the correct pstate level as having aggregates */
	while (min_varlevel-- > 0)
		pstate = pstate->parentParseState;
	pstate->p_hasAggs = true;

	/*
	 * Check to see if the aggregate function is in an invalid place within
	 * its aggregation query.
	 *
	 * For brevity we support two schemes for reporting an error here: set
	 * "err" to a custom message, or set "errkind" true if the error context
	 * is sufficiently identified by what ParseExprKindName will return, *and*
	 * what it will return is just a SQL keyword.  (Otherwise, use a custom
	 * message to avoid creating translation problems.)
	 */
	err = NULL;
	errkind = false;
	switch (pstate->p_expr_kind)
	{
		case EXPR_KIND_NONE:
			Assert(false);		/* can't happen */
			break;
		case EXPR_KIND_OTHER:

			/*
			 * Accept aggregate/grouping here; caller must throw error if
			 * wanted
			 */
			break;
		case EXPR_KIND_JOIN_ON:
		case EXPR_KIND_JOIN_USING:
			if (isAgg)
				err = _("aggregate functions are not allowed in JOIN conditions");
			else
				err = _("grouping operations are not allowed in JOIN conditions");

			break;
		case EXPR_KIND_FROM_SUBSELECT:
			/* Should only be possible in a LATERAL subquery */
			Assert(pstate->p_lateral_active);

			/*
			 * Aggregate/grouping scope rules make it worth being explicit
			 * here
			 */
			if (isAgg)
				err = _("aggregate functions are not allowed in FROM clause of their own query level");
			else
				err = _("grouping operations are not allowed in FROM clause of their own query level");

			break;
		case EXPR_KIND_FROM_FUNCTION:
			if (isAgg)
				err = _("aggregate functions are not allowed in functions in FROM");
			else
				err = _("grouping operations are not allowed in functions in FROM");

			break;
		case EXPR_KIND_WHERE:
			errkind = true;
			break;
		case EXPR_KIND_POLICY:
			if (isAgg)
				err = _("aggregate functions are not allowed in policy expressions");
			else
				err = _("grouping operations are not allowed in policy expressions");

			break;
		case EXPR_KIND_HAVING:
			/* okay */
			break;
		case EXPR_KIND_FILTER:
			errkind = true;
			break;
		case EXPR_KIND_WINDOW_PARTITION:
			/* okay */
			break;
		case EXPR_KIND_WINDOW_ORDER:
			/* okay */
			break;
		case EXPR_KIND_WINDOW_FRAME_RANGE:
			if (isAgg)
				err = _("aggregate functions are not allowed in window RANGE");
			else
				err = _("grouping operations are not allowed in window RANGE");

			break;
		case EXPR_KIND_WINDOW_FRAME_ROWS:
			if (isAgg)
				err = _("aggregate functions are not allowed in window ROWS");
			else
				err = _("grouping operations are not allowed in window ROWS");

			break;
		case EXPR_KIND_WINDOW_FRAME_GROUPS:
			if (isAgg)
				err = _("aggregate functions are not allowed in window GROUPS");
			else
				err = _("grouping operations are not allowed in window GROUPS");

			break;
		case EXPR_KIND_SELECT_TARGET:
			/* okay */
			break;
		case EXPR_KIND_INSERT_TARGET:
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
			errkind = true;
			break;
		case EXPR_KIND_MERGE_WHEN:
			if (isAgg)
				err = _("aggregate functions are not allowed in MERGE WHEN conditions");
			else
				err = _("grouping operations are not allowed in MERGE WHEN conditions");

			break;
		case EXPR_KIND_GROUP_BY:
			errkind = true;
			break;
		case EXPR_KIND_ORDER_BY:
			/* okay */
			break;
		case EXPR_KIND_DISTINCT_ON:
			/* okay */
			break;
		case EXPR_KIND_LIMIT:
		case EXPR_KIND_OFFSET:
			errkind = true;
			break;
		case EXPR_KIND_RETURNING:
			errkind = true;
			break;
		case EXPR_KIND_VALUES:
		case EXPR_KIND_VALUES_SINGLE:
			errkind = true;
			break;
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			if (isAgg)
				err = _("aggregate functions are not allowed in check constraints");
			else
				err = _("grouping operations are not allowed in check constraints");

			break;
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:

			if (isAgg)
				err = _("aggregate functions are not allowed in DEFAULT expressions");
			else
				err = _("grouping operations are not allowed in DEFAULT expressions");

			break;
		case EXPR_KIND_INDEX_EXPRESSION:
			if (isAgg)
				err = _("aggregate functions are not allowed in index expressions");
			else
				err = _("grouping operations are not allowed in index expressions");

			break;
		case EXPR_KIND_INDEX_PREDICATE:
			if (isAgg)
				err = _("aggregate functions are not allowed in index predicates");
			else
				err = _("grouping operations are not allowed in index predicates");

			break;
		case EXPR_KIND_STATS_EXPRESSION:
			if (isAgg)
				err = _("aggregate functions are not allowed in statistics expressions");
			else
				err = _("grouping operations are not allowed in statistics expressions");

			break;
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			if (isAgg)
				err = _("aggregate functions are not allowed in transform expressions");
			else
				err = _("grouping operations are not allowed in transform expressions");

			break;
		case EXPR_KIND_EXECUTE_PARAMETER:
			if (isAgg)
				err = _("aggregate functions are not allowed in EXECUTE parameters");
			else
				err = _("grouping operations are not allowed in EXECUTE parameters");

			break;
		case EXPR_KIND_TRIGGER_WHEN:
			if (isAgg)
				err = _("aggregate functions are not allowed in trigger WHEN conditions");
			else
				err = _("grouping operations are not allowed in trigger WHEN conditions");

			break;
		case EXPR_KIND_PARTITION_BOUND:
			if (isAgg)
				err = _("aggregate functions are not allowed in partition bound");
			else
				err = _("grouping operations are not allowed in partition bound");

			break;
		case EXPR_KIND_PARTITION_EXPRESSION:
			if (isAgg)
				err = _("aggregate functions are not allowed in partition key expressions");
			else
				err = _("grouping operations are not allowed in partition key expressions");

			break;
		case EXPR_KIND_GENERATED_COLUMN:

			if (isAgg)
				err = _("aggregate functions are not allowed in column generation expressions");
			else
				err = _("grouping operations are not allowed in column generation expressions");

			break;

		case EXPR_KIND_CALL_ARGUMENT:
			if (isAgg)
				err = _("aggregate functions are not allowed in CALL arguments");
			else
				err = _("grouping operations are not allowed in CALL arguments");

			break;

		case EXPR_KIND_COPY_WHERE:
			if (isAgg)
				err = _("aggregate functions are not allowed in COPY FROM WHERE conditions");
			else
				err = _("grouping operations are not allowed in COPY FROM WHERE conditions");

			break;

		case EXPR_KIND_CYCLE_MARK:
			errkind = true;
			break;

			/*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
	}

	if (err)
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, location)));

	if (errkind)
	{
		if (isAgg)
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("aggregate functions are not allowed in %s");
		else
			/* translator: %s is name of a SQL construct, eg GROUP BY */
			err = _("grouping operations are not allowed in %s");

		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg_internal(err,
								 ParseExprKindName(pstate->p_expr_kind)),
				 parser_errposition(pstate, location)));
	}
}

/*
 * check_agg_arguments
 *	  Scan the arguments of an aggregate function to determine the
 *	  aggregate's semantic level (zero is the current select's level,
 *	  one is its parent, etc).
 *
 * The aggregate's level is the same as the level of the lowest-level variable
 * or aggregate in its aggregated arguments (including any ORDER BY columns)
 * or filter expression; or if it contains no variables at all, we presume it
 * to be local.
 *
 * Vars/Aggs in direct arguments are *not* counted towards determining the
 * agg's level, as those arguments aren't evaluated per-row but only
 * per-group, and so in some sense aren't really agg arguments.  However,
 * this can mean that we decide an agg is upper-level even when its direct
 * args contain lower-level Vars/Aggs, and that case has to be disallowed.
 * (This is a little strange, but the SQL standard seems pretty definite that
 * direct args are not to be considered when setting the agg's level.)
 *
 * We also take this opportunity to detect any aggregates or window functions
 * nested within the arguments.  We can throw error immediately if we find
 * a window function.  Aggregates are a bit trickier because it's only an
 * error if the inner aggregate is of the same semantic level as the outer,
 * which we can't know until we finish scanning the arguments.
 */
static int
check_agg_arguments(ParseState *pstate,
					List *directargs,
					List *args,
					Expr *filter)
{
	int			agglevel;
	check_agg_arguments_context context;

	context.pstate = pstate;
	context.min_varlevel = -1;	/* signifies nothing found yet */
	context.min_agglevel = -1;
	context.sublevels_up = 0;

	(void) check_agg_arguments_walker((Node *) args, &context);
	(void) check_agg_arguments_walker((Node *) filter, &context);

	/*
	 * If we found no vars nor aggs at all, it's a level-zero aggregate;
	 * otherwise, its level is the minimum of vars or aggs.
	 */
	if (context.min_varlevel < 0)
	{
		if (context.min_agglevel < 0)
			agglevel = 0;
		else
			agglevel = context.min_agglevel;
	}
	else if (context.min_agglevel < 0)
		agglevel = context.min_varlevel;
	else
		agglevel = Min(context.min_varlevel, context.min_agglevel);

	/*
	 * If there's a nested aggregate of the same semantic level, complain.
	 */
	if (agglevel == context.min_agglevel)
	{
		int			aggloc;

		aggloc = locate_agg_of_level((Node *) args, agglevel);
		if (aggloc < 0)
			aggloc = locate_agg_of_level((Node *) filter, agglevel);
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate function calls cannot be nested"),
				 parser_errposition(pstate, aggloc)));
	}

	/*
	 * Now check for vars/aggs in the direct arguments, and throw error if
	 * needed.  Note that we allow a Var of the agg's semantic level, but not
	 * an Agg of that level.  In principle such Aggs could probably be
	 * supported, but it would create an ordering dependency among the
	 * aggregates at execution time.  Since the case appears neither to be
	 * required by spec nor particularly useful, we just treat it as a
	 * nested-aggregate situation.
	 */
	if (directargs)
	{
		context.min_varlevel = -1;
		context.min_agglevel = -1;
		(void) check_agg_arguments_walker((Node *) directargs, &context);
		if (context.min_varlevel >= 0 && context.min_varlevel < agglevel)
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("outer-level aggregate cannot contain a lower-level variable in its direct arguments"),
					 parser_errposition(pstate,
										locate_var_of_level((Node *) directargs,
															context.min_varlevel))));
		if (context.min_agglevel >= 0 && context.min_agglevel <= agglevel)
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("aggregate function calls cannot be nested"),
					 parser_errposition(pstate,
										locate_agg_of_level((Node *) directargs,
															context.min_agglevel))));
	}
	return agglevel;
}

static bool
check_agg_arguments_walker(Node *node,
						   check_agg_arguments_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		int			varlevelsup = ((Var *) node)->varlevelsup;

		/* convert levelsup to frame of reference of original query */
		varlevelsup -= context->sublevels_up;
		/* ignore local vars of subqueries */
		if (varlevelsup >= 0)
		{
			if (context->min_varlevel < 0 ||
				context->min_varlevel > varlevelsup)
				context->min_varlevel = varlevelsup;
		}
		return false;
	}
	if (IsA(node, Aggref))
	{
		int			agglevelsup = ((Aggref *) node)->agglevelsup;

		/* convert levelsup to frame of reference of original query */
		agglevelsup -= context->sublevels_up;
		/* ignore local aggs of subqueries */
		if (agglevelsup >= 0)
		{
			if (context->min_agglevel < 0 ||
				context->min_agglevel > agglevelsup)
				context->min_agglevel = agglevelsup;
		}
		/* no need to examine args of the inner aggregate */
		return false;
	}
	if (IsA(node, GroupingFunc))
	{
		int			agglevelsup = ((GroupingFunc *) node)->agglevelsup;

		/* convert levelsup to frame of reference of original query */
		agglevelsup -= context->sublevels_up;
		/* ignore local aggs of subqueries */
		if (agglevelsup >= 0)
		{
			if (context->min_agglevel < 0 ||
				context->min_agglevel > agglevelsup)
				context->min_agglevel = agglevelsup;
		}
		/* Continue and descend into subtree */
	}

	/*
	 * SRFs and window functions can be rejected immediately, unless we are
	 * within a sub-select within the aggregate's arguments; in that case
	 * they're OK.
	 */
	if (context->sublevels_up == 0)
	{
		if ((IsA(node, FuncExpr) && ((FuncExpr *) node)->funcretset) ||
			(IsA(node, OpExpr) && ((OpExpr *) node)->opretset))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate function calls cannot contain set-returning function calls"),
					 parser_errposition(context->pstate, exprLocation(node))));
		if (IsA(node, WindowFunc))
			ereport(ERROR,
					(errcode(ERRCODE_GROUPING_ERROR),
					 errmsg("aggregate function calls cannot contain window function calls"),
					 parser_errposition(context->pstate,
										((WindowFunc *) node)->location)));
	}
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node,
								   (bool(*)())check_agg_arguments_walker,
								   (void *) context,
								   0);
		context->sublevels_up--;
		return result;
	}

	return expression_tree_walker(node,
								  (bool(*)())check_agg_arguments_walker,
								  (void *) context);
}

/*
 * check_ungrouped_columns -
 *	  Scan the given expression tree for ungrouped variables (variables
 *	  that are not listed in the groupClauses list and are not within
 *	  the arguments of aggregate functions).  Emit a suitable error message
 *	  if any are found.
 *
 * NOTE: we assume that the given clause has been transformed suitably for
 * parser output.  This means we can use expression_tree_walker.
 *
 * NOTE: we recognize grouping expressions in the main query, but only
 * grouping Vars in subqueries.  For example, this will be rejected,
 * although it could be allowed:
 *		SELECT
 *			(SELECT x FROM bar where y = (foo.a + foo.b))
 *		FROM foo
 *		GROUP BY a + b;
 * The difficulty is the need to account for different sublevels_up.
 * This appears to require a whole custom version of equal(), which is
 * way more pain than the feature seems worth.
 */
static void check_ungrouped_columns(Node* node, ParseState* pstate, Query* qry, List* groupClauses,
    List* groupClauseCommonVars, bool have_non_var_grouping, List** func_grouped_rels)
{
    check_ungrouped_columns_context context;

    context.pstate = pstate;
    context.qry = qry;
    context.root = NULL;
    context.groupClauses = groupClauses;
    context.groupClauseCommonVars = groupClauseCommonVars;
    context.have_non_var_grouping = have_non_var_grouping;
    context.func_grouped_rels = func_grouped_rels;
    context.sublevels_up = 0;
    context.in_agg_direct_args = false;
    (void)check_ungrouped_columns_walker(node, &context);
}

static bool check_ungrouped_columns_walker(Node* node, check_ungrouped_columns_context* context)
{
    ListCell* gl = NULL;

    if (node == NULL) {
        return false;
    }
    if (IsA(node, Const) || IsA(node, Param)) {
        return false; /* constants are always acceptable */
    }

    if (IsA(node, Aggref)) {
        Aggref* agg = (Aggref*)node;

        if ((int)agg->agglevelsup == context->sublevels_up) {
            /*
             * For ordered set agg, its direct args should not inside an
             * aggregate. If we find an aggregate call of the original level
             * (that means if it is inside an outer query , the context should
             * be same), do not recurse into its normal arguments, ORDER BY
             * arguments, or filter; ungrouped vars there are not an error.
             * We use in_agg_direct_args in the context to help produce a useful
             * error message for ungrouped vars in direct arguments.
             */
            bool result = false;

            if (context->in_agg_direct_args) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_AGG), errmsg("unexpected args inside agg direct args")));
            }
            context->in_agg_direct_args = true;
            result = check_ungrouped_columns_walker((Node*)agg->aggdirectargs, context);
            context->in_agg_direct_args = false;
            return result;
        }

        /*
         * We can also skip looking at the arguments of aggregates of higher levels,
         * since they could not possibly contain Vars of concern to us (see
         * transformAggregateCall).  We do need to look into arguments of aggregates
         * of lower levels, however.
         */
        if ((int)agg->agglevelsup > context->sublevels_up) {
            return false;
        }
    }

    if (IsA(node, GroupingFunc)) {
        GroupingFunc* grp = (GroupingFunc*)node;

        /* handled GroupingFunc separately, no need to recheck at this level */
        if ((int)grp->agglevelsup >= context->sublevels_up) {
            return false;
        }
    }
    /*
     * If we have any GROUP BY items that are not simple Vars, check to see if
     * subexpression as a whole matches any GROUP BY item. We need to do this
     * at every recursion level so that we recognize GROUPed-BY expressions
     * before reaching variables within them. But this only works at the outer
     * query level, as noted above.
     */
    if (context->have_non_var_grouping && context->sublevels_up == 0) {
        foreach (gl, context->groupClauses) {
            TargetEntry* tle = (TargetEntry*)lfirst(gl);
            if (equal(node, tle->expr)) {
                return false; /* acceptable, do not descend more */
            }
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* If There is ROWNUM, it must appear in the GROUP BY clause or be used in an aggregate function. */
    if (IsA(node, Rownum) && context->sublevels_up == 0) {
        find_rownum_in_groupby_clauses((Rownum *)node, context);
    }
#endif
    /*
     * If we have an ungrouped Var of the original query level, we have a
     * failure.  Vars below the original query level are not a problem, and
     * neither are Vars from above it.	(If such Vars are ungrouped as far as
     * their own query level is concerned, that's someone else's problem...)
     */
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        RangeTblEntry* rte = NULL;
        char* attname = NULL;

        if (var->varlevelsup != (unsigned int)context->sublevels_up) {
            return false; /* it's not local to my query, ignore */
        }

        /*
         * Check for a match, if we didn't do it above.
         */
        if (!context->have_non_var_grouping || context->sublevels_up != 0) {
            foreach (gl, context->groupClauses) {
                Var* gvar = (Var*)((TargetEntry*)lfirst(gl))->expr;

                if (IsA(gvar, Var) && gvar->varno == var->varno && gvar->varattno == var->varattno &&
                    gvar->varlevelsup == 0)
                    return false; /* acceptable, we're okay */
            }
        }

        /*
         * Check whether the Var is known functionally dependent on the GROUP
         * BY columns.	If so, we can allow the Var to be used, because the
         * grouping is really a no-op for this table.  However, this deduction
         * depends on one or more constraints of the table, so we have to add
         * those constraints to the query's constraintDeps list, because it's
         * not semantically valid anymore if the constraint(s) get dropped.
         * (Therefore, this check must be the last-ditch effort before raising
         * error: we don't want to add dependencies unnecessarily.)
         *
         * Because this is a pretty expensive check, and will have the same
         * outcome for all columns of a table, we remember which RTEs we've
         * already proven functional dependency for in the func_grouped_rels
         * list.  This test also prevents us from adding duplicate entries to
         * the constraintDeps list.
         */
        if (list_member_int(*context->func_grouped_rels, var->varno)) {
            return false; /* previously proven acceptable */
        }

        AssertEreport(
            var->varno > 0 && (int)var->varno <= list_length(context->pstate->p_rtable), MOD_OPT, "Var is unexpected");
        rte = rt_fetch(var->varno, context->pstate->p_rtable);
        if (rte->rtekind == RTE_RELATION) {
            if (check_functional_grouping(
                    rte->relid, var->varno, 0, context->groupClauseCommonVars, &context->qry->constraintDeps)) {
                *context->func_grouped_rels = lappend_int(*context->func_grouped_rels, var->varno);
                return false; /* acceptable */
            }
        }

        /* Found an ungrouped local variable; generate error message */
        attname = get_rte_attribute_name(rte, var->varattno);

        /* Fix attname if the RTE has been rewrited by start with...connect by. */
        char* orig_attname = attname;
        if (IsSWCBRewriteRTE(rte)) {
            attname = strrchr(attname, '@');
            attname = (attname != NULL) ? (attname + 1) : orig_attname;
        }

        if (context->sublevels_up == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                    errmsg("column \"%s.%s\" must appear in the GROUP BY clause or be used in an aggregate function",
                        rte->eref->aliasname,
                        attname),
                    context->in_agg_direct_args
                        ? errdetail("Direct arguments of an ordered-set aggregate must use only grouped columns.")
                        : 0,
                    rte->swConverted ? errdetail("Please check your start with rewrite table's column.") : 0,
                    parser_errposition(context->pstate, var->location)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR),
                    errmsg("subquery uses ungrouped column \"%s.%s\" from outer query", rte->eref->aliasname, attname),
                    parser_errposition(context->pstate, var->location)));
        }
        if (attname != NULL) {
            pfree_ext(attname);
        }
    }

    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())check_ungrouped_columns_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())check_ungrouped_columns_walker, (void*)context);
}

/*
 * finalize_grouping_exprs -
 *	  Scan the given expression tree for GROUPING() and related calls,
 *	  and validate and process their arguments.
 *
 * This is split out from check_ungrouped_columns above because it needs
 * to modify the nodes (which it does in-place, not via a mutator) while
 * check_ungrouped_columns may see only a copy of the original thanks to
 * flattening of join alias vars. So here, we flatten each individual
 * GROUPING argument as we see it before comparing it.
 */
static void finalize_grouping_exprs(
    Node* node, ParseState* pstate, Query* qry, List* groupClauses, PlannerInfo* root, bool have_non_var_grouping)
{
    check_ungrouped_columns_context context;

    context.pstate = pstate;
    context.qry = qry;
    context.root = root;
    context.groupClauses = groupClauses;
    context.groupClauseCommonVars = NIL;
    context.have_non_var_grouping = have_non_var_grouping;
    context.func_grouped_rels = NULL;
    context.sublevels_up = 0;
    context.in_agg_direct_args = false;
    (void)finalize_grouping_exprs_walker(node, &context);
}

static bool finalize_grouping_exprs_walker(Node* node, check_ungrouped_columns_context* context)
{
    ListCell* gl = NULL;

    if (node == NULL) {
        return false;
    }
    if (IsA(node, Const) || IsA(node, Param)) {
        return false; /* constants are always acceptable */
    }

    if (IsA(node, Aggref)) {
        Aggref* agg = (Aggref*)node;

        if ((int)agg->agglevelsup == context->sublevels_up) {
            /*
             * If we find an aggregate call of the original level, do not
             * recurse into its normal arguments, ORDER BY arguments, or
             * filter; GROUPING exprs of this level are not allowed there. But
             * check direct arguments as though they weren't in an aggregate.
             */
            bool result = false;

            AssertEreport(!context->in_agg_direct_args, MOD_OPT, "");
            context->in_agg_direct_args = true;
            result = finalize_grouping_exprs_walker((Node*)agg->aggdirectargs, context);
            context->in_agg_direct_args = false;
            return result;
        }

        /*
         * We can skip recursing into aggregates of higher levels altogether,
         * since they could not possibly contain exprs of concern to us (see
         * transformAggregateCall).  We do need to look at aggregates of lower
         * levels, however.
         */
        if ((int)agg->agglevelsup > context->sublevels_up) {
            return false;
        }
    }

    if (IsA(node, GroupingFunc)) {
        GroupingFunc* grp = (GroupingFunc*)node;

        /*
         * We only need to check GroupingFunc nodes at the exact level to
         * which they belong, since they cannot mix levels in arguments.
         */
        if ((int)grp->agglevelsup == context->sublevels_up) {
            ListCell* lc = NULL;
            List* ref_list = NIL;

            foreach (lc, grp->args) {
                Node* expr = (Node*)lfirst(lc);
                Index ref = 0;

                if (context->root != NULL) {
                    expr = flatten_join_alias_vars(context->root, expr);
                }

                /*
                 * Each expression must match a grouping entry at the current
                 * query level. Unlike the general expression case, we don't
                 * allow functional dependencies or outer references.
                 */
                if (IsA(expr, Var)) {
                    Var* var = (Var*)expr;

                    if ((int)var->varlevelsup == context->sublevels_up) {
                        foreach (gl, context->groupClauses) {
                            TargetEntry* tle = (TargetEntry*)lfirst(gl);
                            Var* gvar = (Var*)tle->expr;

                            if (IsA(gvar, Var) && gvar->varno == var->varno && gvar->varattno == var->varattno &&
                                gvar->varlevelsup == 0) {
                                ref = tle->ressortgroupref;
                                break;
                            }
                        }
                    }
                } else if (context->have_non_var_grouping && context->sublevels_up == 0) {
                    foreach (gl, context->groupClauses) {
                        TargetEntry* tle = (TargetEntry*)lfirst(gl);

                        if (equal(expr, tle->expr)) {
                            ref = tle->ressortgroupref;
                            break;
                        }
                    }
                }

                if (ref == 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_GROUPING_ERROR),
                            errmsg("arguments to GROUPING must be grouping expressions of the associated query level"),
                            parser_errposition(context->pstate, exprLocation(expr))));
                }

                ref_list = lappend_int(ref_list, ref);
            }

            grp->refs = ref_list;
        }

        if ((int)grp->agglevelsup > context->sublevels_up) {
            return false;
        }
    }

    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())finalize_grouping_exprs_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())finalize_grouping_exprs_walker, (void*)context);
}

/*
 * Given a GroupingSet node, expand it and return a list of lists.
 *
 * For EMPTY nodes, return a list of one empty list.
 *
 * For SIMPLE nodes, return a list of one list, which is the node content.
 *
 * For CUBE and ROLLUP nodes, return a list of the expansions.
 *
 * For SET nodes, recursively expand contained CUBE and ROLLUP.
 */
static List* expand_groupingset_node(GroupingSet* gs)
{
    List* result = NIL;

    switch (gs->kind) {
        case GROUPING_SET_EMPTY:
            result = list_make1(NIL);
            break;

        case GROUPING_SET_SIMPLE:
            result = list_make1(gs->content);
            break;

        case GROUPING_SET_ROLLUP: {
            List* rollup_val = gs->content;
            ListCell* lc = NULL;
            int curgroup_size = list_length(gs->content);

            while (curgroup_size > 0) {
                List* current_result = NIL;
                int i = curgroup_size;

                foreach (lc, rollup_val) {
                    GroupingSet* gs_current = (GroupingSet*)lfirst(lc);

                    AssertEreport(gs_current->kind == GROUPING_SET_SIMPLE, MOD_OPT, "Kind is unexpected");

                    current_result = list_concat(current_result, list_copy(gs_current->content));

                    /* If we are done with making the current group, break */
                    if (--i == 0) {
                        break;
                    }
                }

                result = lappend(result, current_result);
                --curgroup_size;
            }

            result = lappend(result, NIL);
        } break;

        case GROUPING_SET_CUBE: {
            List* cube_list = gs->content;
            int number_bits = list_length(cube_list);
            uint32 num_sets;
            uint32 i;

            /* parser should cap this much lower */
            AssertEreport(number_bits < 31, MOD_OPT, "parser should cap this much lower");

            num_sets = (1U << (unsigned int)number_bits);

            for (i = 0; i < num_sets; i++) {
                List* current_result = NIL;
                ListCell* lc = NULL;
                uint32 mask = 1U;

                foreach (lc, cube_list) {
                    GroupingSet* gs_current = (GroupingSet*)lfirst(lc);

                    AssertEreport(gs_current->kind == GROUPING_SET_SIMPLE, MOD_OPT, "Kind is unexpected");

                    if (mask & i) {
                        current_result = list_concat(current_result, list_copy(gs_current->content));
                    }

                    mask <<= 1;
                }

                result = lappend(result, current_result);
            }
        } break;

        case GROUPING_SET_SETS: {
            ListCell* lc = NULL;

            foreach (lc, gs->content) {
                List* current_result = expand_groupingset_node((GroupingSet*)lfirst(lc));

                result = list_concat(result, current_result);
            }
        } break;
        default:
            break;
    }

    return result;
}

static int cmp_list_len_asc(const void* a, const void* b)
{
    int la = list_length(*(List* const*)a);
    int lb = list_length(*(List* const*)b);

    return (la > lb) ? 1 : (la == lb) ? 0 : -1;
}

/*
 * Create expression trees for the transition and final functions
 * of an aggregate.  These are needed so that polymorphic functions
 * can be used within an aggregate --- without the expression trees,
 * such functions would not know the datatypes they are supposed to use.
 * (The trees will never actually be executed, however, so we can skimp
 * a bit on correctness.)
 *
 * agg_input_types, agg_state_type, agg_result_type identify the input,
 * transition, and result types of the aggregate.  These should all be
 * resolved to actual types (ie, none should ever be ANYELEMENT etc).
 * agg_input_collation is the aggregate function's input collation.
 *
 * transfn_oid and finalfn_oid identify the funcs to be called; the latter
 * may be InvalidOid.
 *
 * Pointers to the constructed trees are returned into *transfnexpr and
 * *finalfnexpr.  The latter is set to NULL if there's no finalfn.
 */
void build_aggregate_fnexprs(Oid* agg_input_types, int agg_num_inputs, Oid agg_state_type, Oid agg_result_type,
    Oid agg_input_collation, Oid transfn_oid, Oid finalfn_oid, Expr** transfnexpr, Expr** finalfnexpr)
{
    Param* argp = NULL;
    List* args = NIL;
    int i;

    /*
     * Build arg list to use in the transfn FuncExpr node. We really only care
     * that transfn can discover the actual argument types at runtime using
     * get_fn_expr_argtype(), so it's okay to use Param nodes that don't
     * correspond to any real Param.
     */
    argp = makeNode(Param);
    argp->paramkind = PARAM_EXEC;
    argp->paramid = -1;
    argp->paramtype = agg_state_type;
    argp->paramtypmod = -1;
    argp->paramcollid = agg_input_collation;
    argp->location = -1;

    args = list_make1(argp);

    for (i = 0; i < agg_num_inputs; i++) {
        argp = makeNode(Param);
        argp->paramkind = PARAM_EXEC;
        argp->paramid = -1;
        argp->paramtype = agg_input_types[i];
        argp->paramtypmod = -1;
        argp->paramcollid = agg_input_collation;
        argp->location = -1;
        args = lappend(args, argp);
    }

    *transfnexpr =
        (Expr*)makeFuncExpr(transfn_oid, agg_state_type, args, InvalidOid, agg_input_collation, COERCE_DONTCARE);

    /* see if we have a final function */
    if (!OidIsValid(finalfn_oid)) {
        *finalfnexpr = NULL;
        return;
    }

    /*
     * Build expr tree for final function
     */
    argp = makeNode(Param);
    argp->paramkind = PARAM_EXEC;
    argp->paramid = -1;
    argp->paramtype = agg_state_type;
    argp->paramtypmod = -1;
    argp->paramcollid = agg_input_collation;
    argp->location = -1;
    args = list_make1(argp);

    *finalfnexpr =
        (Expr*)makeFuncExpr(finalfn_oid, agg_result_type, args, InvalidOid, agg_input_collation, COERCE_DONTCARE);
}

/*
 * Create expression trees for the transition and final functions
 * of an aggregate.  These are needed so that polymorphic functions
 * can be used within an aggregate --- without the expression trees,
 * such functions would not know the datatypes they are supposed to use.
 * (The trees will never actually be executed, however, so we can skimp
 * a bit on correctness.)
 *
 * agg_input_types, agg_state_type, agg_result_type identify the input,
 * transition, and result types of the aggregate.  These should all be
 * resolved to actual types (ie, none should ever be ANYELEMENT etc).
 * agg_input_collation is the aggregate function's input collation.
 *
 * For an ordered-set aggregate, remember that agg_input_types describes
 * the direct arguments followed by the aggregated arguments.
 *
 * transfn_oid and finalfn_oid identify the funcs to be called; the latter
 * may be InvalidOid.
 *
 * Pointers to the constructed trees are returned into *transfnexpr and
 * *finalfnexpr.  The latter is set to NULL if there's no finalfn.
 */
void build_trans_aggregate_fnexprs(int agg_num_inputs, int agg_num_direct_inputs, bool agg_ordered_set,
    bool agg_variadic, Oid agg_state_type, Oid* agg_input_types, Oid agg_result_type, Oid agg_input_collation,
    Oid transfn_oid, Oid finalfn_oid, Expr** transfnexpr, Expr** finalfnexpr)
{
    Param* argp = NULL;
    List* args = NULL;
    FuncExpr* fexpr = NULL;
    int i;

    /*
     * Build arg list to use in the transfn FuncExpr node. We really only care
     * that transfn can discover the actual argument types at runtime using
     * get_fn_expr_argtype(), so it's okay to use Param nodes that don't
     * correspond to any real Param.
     */
    argp = makeParam(PARAM_EXEC, -1, agg_state_type, -1, agg_input_collation, -1);
    args = list_make1(argp);

    for (i = agg_num_direct_inputs; i < agg_num_inputs; i++) {
        argp = makeParam(PARAM_EXEC, -1, agg_input_types[i], -1, agg_input_collation, -1);
        args = lappend(args, argp);
    }

    fexpr = makeFuncExpr(transfn_oid, agg_state_type, args, InvalidOid, agg_input_collation, COERCE_EXPLICIT_CALL);
    fexpr->funcvariadic = agg_variadic;
    *transfnexpr = (Expr*)fexpr;

    /* see if we have a final function */
    if (!OidIsValid(finalfn_oid)) {
        *finalfnexpr = NULL;
        return;
    }

    /*
     * Build expr tree for final function
     */
    argp = makeParam(PARAM_EXEC, -1, agg_state_type, -1, agg_input_collation, -1);
    args = list_make1(argp);

    if (agg_ordered_set) {
        for (i = 0; i < agg_num_inputs; i++) {
            argp = makeParam(PARAM_EXEC, -1, agg_input_types[i], -1, agg_input_collation, -1);
            args = lappend(args, argp);
        }
    }

    *finalfnexpr =
        (Expr*)makeFuncExpr(finalfn_oid, agg_result_type, args, InvalidOid, agg_input_collation, COERCE_EXPLICIT_CALL);
    /* finalfn is currently never treated as variadic */
}


void build_aggregate_transfn_expr(Oid *agg_input_types, int agg_num_inputs, int agg_num_direct_inputs,
                                  bool agg_variadic, Oid agg_state_type, Oid agg_input_collation, Oid transfn_oid,
                                  Expr **transfnexpr)
{
    Param *argp;
    List *args;
    FuncExpr *fexpr;
    int i;

    argp = makeNode(Param);
    argp->paramkind = PARAM_EXEC;
    argp->paramid = -1;
    argp->paramtype = agg_state_type;
    argp->paramtypmod = -1;
    argp->paramcollid = agg_input_collation;
    argp->location = -1;

    args = list_make1(argp);

    for (i = agg_num_direct_inputs; i < agg_num_inputs; i++) {
        argp = makeNode(Param);
        argp->paramkind = PARAM_EXEC;
        argp->paramid = -1;
        argp->paramtype = agg_input_types[i];
        argp->paramtypmod = -1;
        argp->paramcollid = agg_input_collation;
        argp->location = -1;
        args = lappend(args, argp);
    }

    fexpr = makeFuncExpr(transfn_oid, agg_state_type, args, InvalidOid, agg_input_collation, COERCE_EXPLICIT_CALL);
    fexpr->funcvariadic = agg_variadic;
    *transfnexpr = (Expr *)fexpr;
}

void build_aggregate_finalfn_expr(Oid *agg_input_types, int num_finalfn_inputs, Oid agg_state_type, Oid agg_result_type,
                                  Oid agg_input_collation, Oid finalfn_oid, Expr **finalfnexpr)
{
    Param *argp;
    List *args;
    int i;

    argp = makeNode(Param);
    argp->paramkind = PARAM_EXEC;
    argp->paramid = -1;
    argp->paramtype = agg_state_type;
    argp->paramtypmod = -1;
    argp->paramcollid = agg_input_collation;
    argp->location = -1;
    args = list_make1(argp);

    for (i = 0; i < num_finalfn_inputs - 1; i++) {
        argp = makeNode(Param);
        argp->paramkind = PARAM_EXEC;
        argp->paramid = -1;
        argp->paramtype = agg_input_types[i];
        argp->paramtypmod = -1;
        argp->paramcollid = agg_input_collation;
        argp->location = -1;
        args = lappend(args, argp);
    }

    *finalfnexpr =
        (Expr *)makeFuncExpr(finalfn_oid, agg_result_type, args, InvalidOid, agg_input_collation, COERCE_EXPLICIT_CALL);
}

/*
 * Expand a groupingSets clause to a flat list of grouping sets.
 * The returned list is sorted by length, shortest sets first.
 *
 * This is mainly for the planner, but we use it here too to do
 * some consistency checks.
 */
List* expand_grouping_sets(List* groupingSets, int limit)
{
    List* expanded_groups = NIL;
    List* result = NIL;
    double numsets = 1;
    ListCell* lc = NULL;

    if (groupingSets == NIL) {
        return NIL;
    }

    foreach (lc, groupingSets) {
        List* current_result = NIL;
        GroupingSet* gs = (GroupingSet*)lfirst(lc);
        current_result = expand_groupingset_node(gs);
        AssertEreport(current_result != NIL, MOD_OPT, "para should not be NULL here");
        numsets *= list_length(current_result);

        if (limit >= 0 && numsets > limit) {
            return NIL;
        }

        expanded_groups = lappend(expanded_groups, current_result);
    }

    /*
     * Do cartesian product between sublists of expanded_groups. While at it,
     * remove any duplicate elements from individual grouping sets (we must
     * NOT change the number of sets though)
     */
    foreach (lc, (List*)linitial(expanded_groups)) {
        result = lappend(result, list_union_int(NIL, (List*)lfirst(lc)));
    }

    for_each_cell(lc, lnext(list_head(expanded_groups)))
    {
        List* p = (List*)lfirst(lc);
        List* new_result = NIL;
        ListCell* lc2 = NULL;

        foreach (lc2, result) {
            List* q = (List*)lfirst(lc2);
            ListCell* lc3 = NULL;

            foreach (lc3, p) {
                new_result = lappend(new_result, list_union_int(q, (List*)lfirst(lc3)));
            }
        }
        result = new_result;
    }

    if (list_length(result) > 1) {
        int result_len = list_length(result);
        List** buf = (List**)palloc(sizeof(List*) * result_len);
        List** ptr = buf;

        foreach (lc, result) {
            *ptr++ = (List*)lfirst(lc);
        }

        qsort(buf, result_len, sizeof(List*), cmp_list_len_asc);

        result = NIL;
        ptr = buf;

        while (result_len-- > 0)
            result = lappend(result, *ptr++);

        pfree_ext(buf);
    }

    return result;
}

/*
 * transformGroupingFunc
 *		Transform a GROUPING expression
 *
 * GROUPING() behaves very like an aggregate.  Processing of levels and nesting
 * is done as for aggregates.  We set p_hasAggs for these expressions too.
 */
Node* transformGroupingFunc(ParseState* pstate, GroupingFunc* p)
{
    ListCell* lc = NULL;
    List* args = p->args;
    List* result_list = NIL;
    bool orig_is_replace = false;

    GroupingFunc* result = makeNode(GroupingFunc);

    if (list_length(args) > 31) {
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                errmsg("GROUPING must have fewer than 32 arguments"),
                parser_errposition(pstate, p->location)));
    }
    orig_is_replace = pstate->isAliasReplace;

    /* Grouping is not support Alias Replace. */
    pstate->isAliasReplace = false;

    foreach (lc, args) {
        Node* current_result = NULL;
        current_result = transformExpr(pstate, (Node*)lfirst(lc), pstate->p_expr_kind);
        /* acceptability of expressions is checked later */
        result_list = lappend(result_list, current_result);
    }

    pstate->isAliasReplace = orig_is_replace;

    result->args = result_list;
    result->location = p->location;

    if (pstate->p_is_flt_frame) {
        check_agglevels_and_constraints(pstate, (Node*)result);
    }

    pstate->p_hasAggs = true;

    return (Node*)result;
}

/*
 * check_windowagg_can_shuffle
 *		Check if windowagg can be shuffled
 */
bool check_windowagg_can_shuffle(List* partitionClause, List* targetList)
{
    if (partitionClause == NIL) {
        return true;
    }

    ListCell* l = NULL;
    foreach (l, partitionClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(l);
        TargetEntry* expr = get_sortgroupclause_tle(grpcl, targetList, false);
        if (expr == NULL) {
            continue;
        }
        if (checkExprHasAggs((Node*)expr->expr)) {
            return false;
        }
    }

    return true;
}

/*
 * get_aggregate_argtypes
 * Get the actual datatypes passed to an aggregate call and return the
 * number of actual arguments.
 *
 * Given an Aggref, extract the actual datatypes of the input arguments.
 * For ordered-set agg, Aggref contains direct args and aggregated args,
 * and direct args is saved before aggregate args.
 *
 * Datatypes are load into inputTypes[], which must reference an array
 * of length FUNC_MAX_ARGS.
 */
int get_aggregate_argtypes(Aggref* aggref, Oid* inputTypes, int func_max_args)
{
    int narg = 0;
    ListCell* lc = NULL;

    /*
     * If is ordered-set agg, aggref->aggdirectargs is not null.
     * So we need first handle the direct args.
     */
    foreach (lc, aggref->aggdirectargs) {
        inputTypes[narg] = exprType((Node*)lfirst(lc));
        narg++;
        if (narg >= func_max_args) {
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("functions can have at most %d parameters", func_max_args)));
        }
    }

    /*
     * Then get the aggregated arguments, both contained by normal
     * agg and orderd-set agg.
     */
    foreach (lc, aggref->args) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        /* Ignore ordering columns of a plain aggregate */
        if (tle->resjunk) {
            continue;
        }

        inputTypes[narg] = exprType((Node*)tle->expr);
        narg++;
        if (narg >= func_max_args) {
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("functions can have at most %d parameters", func_max_args)));
        }
    }

    return narg;
}

/*
 * resolve_aggregate_transtype
 * Identify the transition state value's datatype for an aggregate call
 * when agg accepts ANY or a polymorphic type.
 *
 * This function resolves a polymorphic aggregate's state datatype.
 * The aggtranstype is passed by searching from the pg_aggregate catalog,
 * as well as the actual argument types extracted by get_aggregate_argtypes.
 */
Oid resolve_aggregate_transtype(Oid aggfuncid, Oid aggtranstype, Oid* inputTypes, int numArguments)
{
    /* Only resolve actual type of transition state when it is polymorphic */
    if (IsPolymorphicType(aggtranstype)) {
        Oid* declaredArgTypes = NULL;
        int agg_nargs = 0;
        /* get the agg's function's argument and result types... */
        (void)get_func_signature(aggfuncid, &declaredArgTypes, &agg_nargs);

        Assert(agg_nargs <= numArguments);

        aggtranstype = enforce_generic_type_consistency(inputTypes, declaredArgTypes, agg_nargs, aggtranstype, false);
        pfree(declaredArgTypes);
    }
    return aggtranstype;
}

#ifndef ENABLE_MULTIPLE_NODES
static void find_rownum_in_groupby_clauses(Rownum *rownumVar, check_ungrouped_columns_context *context)
{
    /*
     * have_non_var_grouping makes SQL
     * SELECT a + a FROM t GROUP BY a + a having rownum <= 1;
     * allowed, but SQL
     * SELECT a FROM t GROUP BY a having rownum <= 1;
     * not allowed, which is different from O.
     */
    if (!context->have_non_var_grouping) {
        bool haveRownum = false;
        ListCell *gl = NULL;
        foreach (gl, context->groupClauses) {
            Node *gnode = (Node *)((TargetEntry *)lfirst(gl))->expr;
            if (IsA(gnode, Rownum)) {
                haveRownum = true;
                break;
            }
        }

        if (haveRownum == false) {
            ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("ROWNUM must appear in the GROUP BY clause or be used in an aggregate function"),
                parser_errposition(context->pstate, rownumVar->location)));
        }
    }
}
#endif
