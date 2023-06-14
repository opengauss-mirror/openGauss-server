/* -------------------------------------------------------------------------
 *
 * clausesel.cpp
 *	  Routines to compute clause selectivities
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/clausesel.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_namespace.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "utils/extended_statistics.h"
#include "utils/syscache.h"

/*
 * Data structure for accumulating info about possible range-query
 * clause pairs in clauselist_selectivity.
 */
typedef struct RangeQueryClause {
    struct RangeQueryClause* next; /* next in linked list */
    Expr* clause;                  /* the second clause for range-query */
    Node* var;                     /* The common variable of the clauses */
    bool have_lobound;             /* found a low-bound clause yet? */
    bool have_hibound;             /* found a high-bound clause yet? */
    Selectivity lobound;           /* Selectivity of a var > something clause */
    Selectivity hibound;           /* Selectivity of a var < something clause */
} RangeQueryClause;

static void addRangeClause(RangeQueryClause** rqlist, Node* clause, bool varonleft, bool isLTsel, Selectivity s2);

static List* switch_arg_items(Node* funExpr, Const* cnst, Oid* eqlOprOid, Oid* inputcollid, bool isequal);
static List* do_restrictinfo_conversion(List* args, Oid* eqlOprOid, Oid* inputcollid, bool isequal);
#define MIN(A, B) ((B) < (A) ? (B) : (A))

/* the context for get var data from clause in restrict info. */
typedef struct {
    PlannerInfo* root;                  /* plan info node */
    int varRelid;                       /* varRelid is either 0 or a rangetable index */
    RatioType ratiotype;                /* filter ratio or join ratio */
    SpecialJoinInfo* sjinfo;            /* special join info for the joinrel */
    VariableStatData filter_vardata;    /* var data for filter by self */
    VariableStatData semijoin_vardata1; /* var data for the left args of semi/anti join  */
    VariableStatData semijoin_vardata2; /* var data for the right args of semi/anti join  */
} get_vardata_for_filter_or_semijoin_context;

/* get var data and cache selectivity for filter or semi/anti join. */
static void get_vardata_for_filter_or_semijoin(
    PlannerInfo* root, Node* clause, int varRelid, Selectivity selec, SpecialJoinInfo* sjinfo, RatioType type);
/* get vardata walker for clause. */
static bool get_vardata_for_filter_or_semijoin_walker(Node* node, get_vardata_for_filter_or_semijoin_context* context);
static bool is_rangequery_clause(Node* clause, RestrictInfo* rinfo, bool* varonleft);
static bool is_rangequery_contain_scalarop(Node* clause, RestrictInfo* rinfo);
static void set_varratio_for_rqclause(
    PlannerInfo* root, List* varlist, int varRelid, double ratio, SpecialJoinInfo* sjinfo);

#ifndef ENABLE_MULTIPLE_NODES
static RelOptInfo *find_single_rel_for_clauses(PlannerInfo *root, const List *clauses);
static Selectivity calculate_selectivity_dependency(bool flag_dependency, PlannerInfo *root, ES_SELECTIVITY *es,
    int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo, List** clauselist);
#endif

/****************************************************************************
 *		ROUTINES TO COMPUTE SELECTIVITIES
 ****************************************************************************/
/*
 * clauselist_selectivity -
 *	  Compute the selectivity of an implicitly-ANDed list of boolean
 *	  expression clauses.  The list can be empty, in which case 1.0
 *	  must be returned.  List elements may be either RestrictInfos
 *	  or bare expression clauses --- the former is preferred since
 *	  it allows caching of results.
 *
 * See clause_selectivity() for the meaning of the additional parameters.
 *
 * Our basic approach is to take the product of the selectivities of the
 * subclauses.	However, that's only right if the subclauses have independent
 * probabilities, and in reality they are often NOT independent.  So,
 * we want to be smarter where we can.

 * Currently, the only extra smarts we have is to recognize "range queries",
 * such as "x > 34 AND x < 42".  Clauses are recognized as possible range
 * query components if they are restriction opclauses whose operators have
 * scalarltsel() or scalargtsel() as their restriction selectivity estimator.
 * We pair up clauses of this form that refer to the same variable.  An
 * unpairable clause of this kind is simply multiplied into the selectivity
 * product in the normal way.  But when we find a pair, we know that the
 * selectivities represent the relative positions of the low and high bounds
 * within the column's range, so instead of figuring the selectivity as
 * hisel * losel, we can figure it as hisel + losel - 1.  (To visualize this,
 * see that hisel is the fraction of the range below the high bound, while
 * losel is the fraction above the low bound; so hisel can be interpreted
 * directly as a 0..1 value but we need to convert losel to 1-losel before
 * interpreting it as a value.	Then the available range is 1-losel to hisel.
 * However, this calculation double-excludes nulls, so really we need
 * hisel + losel + null_frac - 1.)
 *
 * If either selectivity is exactly DEFAULT_INEQ_SEL, we forget this equation
 * and instead use DEFAULT_RANGE_INEQ_SEL.	The same applies if the equation
 * yields an impossible (negative) result.
 *
 * A free side-effect is that we can recognize redundant inequalities such
 * as "x < 4 AND x < 5"; only the tighter constraint will be counted.
 *
 * Of course this is all very dependent on the behavior of
 * scalarltsel/scalargtsel; perhaps some day we can generalize the approach.
 *
 * sjinfo: identify join info include lefthand/righthand.
 * varratio_cached: if we cache the selectivity into the relation or not for estimate distinct using possion.
 */
Selectivity clauselist_selectivity(
    PlannerInfo* root, List* clauses, int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo, bool varratio_cached, bool use_poisson)
{
    Selectivity s1 = 1.0;
    RangeQueryClause* rqlist = NULL;
    ListCell* l = NULL;
    List* varlist = NIL;
    List* clauselist = clauses;
    ES_SELECTIVITY* es = NULL;
    MemoryContext ExtendedStat = NULL;
    MemoryContext oldcontext;
    bool use_muti_stats = true;

    if (ENABLE_CACHEDPLAN_MGR && root->glob->boundParams != NULL) {
        root->glob->boundParams->params_lazy_bind = false;
        use_muti_stats = (root->glob->boundParams->uParamInfo != DEFUALT_INFO) ? false : true;
    }

    /*
     * If there's exactly one clause, then no use in trying to match up pairs,
     * so just go directly to clause_selectivity().
     */
    if (list_length(clauses) == 1)
        return clause_selectivity(root, (Node*)linitial(clauses), varRelid, jointype, sjinfo, varratio_cached, false, use_poisson);

    /* initialize es_selectivity class, list_length(clauses) can be 0 when called by set_baserel_size_estimates */
    if (list_length(clauses) >= 2 && use_muti_stats &&
        (jointype == JOIN_INNER || jointype == JOIN_FULL || jointype == JOIN_LEFT || jointype == JOIN_ANTI ||
            jointype == JOIN_SEMI || jointype == JOIN_LEFT_ANTI_FULL)) {
        ExtendedStat = AllocSetContextCreate(CurrentMemoryContext,
            "ExtendedStat",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        oldcontext = MemoryContextSwitchTo(ExtendedStat);
        es = New(ExtendedStat) ES_SELECTIVITY();
        Assert(root != NULL);
        s1 = es->calculate_selectivity(root, clauses, sjinfo, jointype, NULL, ES_EQJOINSEL);
        clauselist = es->unmatched_clause_group;

#ifndef ENABLE_MULTIPLE_NODES
        /*
         * If these clauses references a single relation and it exists extended statistics for functional dependency
         * in pg_statistic_ext, try to apply functional dependency to compute selecticity.
         */
        bool flag_dependency = u_sess->attr.attr_sql.enable_functional_dependency;
        flag_dependency = flag_dependency && es->unmatched_clause_group && es->statlist;
        s1 *= calculate_selectivity_dependency(flag_dependency, root, es, varRelid, jointype, sjinfo, &clauselist);
#endif

        es->clear();
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /*
     * Initial scan over clauses.  Anything that doesn't look like a potential
     * rangequery clause gets multiplied into s1 and forgotten. Anything that
     * does gets inserted into an rqlist entry.
     */
    foreach (l, clauselist) {
        Node* clause = (Node*)lfirst(l);
        RestrictInfo* rinfo = NULL;
        Selectivity s2;

        /* Always compute the selectivity using clause_selectivity */
        s2 = clause_selectivity(root, clause, varRelid, jointype, sjinfo, varratio_cached, true, use_poisson);

        /*
         * Check for being passed a RestrictInfo.
         *
         * If it's a pseudoconstant RestrictInfo, then s2 is either 1.0 or
         * 0.0; just use that rather than looking for range pairs.
         */
        if (IsA(clause, RestrictInfo)) {
            rinfo = (RestrictInfo*)clause;
            if (rinfo->pseudoconstant) {
                s1 = s1 * s2;
                rinfo->clause->selec = s2;
                continue;
            }
            clause = (Node*)rinfo->clause;
        } else
            rinfo = NULL;

        /*
         * if the clause is range query like 'between and',
         * we should scan the pair of rangequery and compute final selectivity.
         */
        OpExpr* expr = (OpExpr*)clause;
        bool varonleft = true;
        if (is_rangequery_clause(clause, rinfo, &varonleft)) {
            /*
             * If it's not a "<" or ">" operator, just merge the
             * selectivity in generically.	But if it's the right oprrest,
             * add the clause to rqlist for later processing.
             */
            switch (get_oprrest(expr->opno)) {
                case F_SCALARLTSEL:
                    addRangeClause(&rqlist, clause, varonleft, true, s2);
                    break;
                case F_SCALARGTSEL:
                    addRangeClause(&rqlist, clause, varonleft, false, s2);
                    break;
                default:
                    /* Just merge the selectivity in generically */
                    if ((uint32)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT) {
                        s1 = MIN(s1, s2);
                        expr->xpr.selec = s1;
                    } else {
                        s1 = s1 * s2;
                        expr->xpr.selec = s2;
                    }
                    break;
            }
            continue;
        }

        /* Not the right form, so treat it generically. */
        if ((uint32)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT) {
            s1 = MIN(s1, s2);
            expr->xpr.selec = s1;
        } else {
            s1 = s1 * s2;
            expr->xpr.selec = s2;
        }
    }

    /*
     * Now scan the rangequery pair list.
     */
    while (rqlist != NULL) {
        RangeQueryClause* rqnext = NULL;

        if (rqlist->have_lobound && rqlist->have_hibound) {
            /* Successfully matched a pair of range clauses */
            Selectivity s2;

            /*
             * Exact equality to the default value probably means the
             * selectivity function punted.  This is not airtight but should
             * be good enough.
             */
            if (rqlist->hibound == DEFAULT_INEQ_SEL || rqlist->lobound == DEFAULT_INEQ_SEL) {
                s2 = DEFAULT_RANGE_INEQ_SEL;
            } else {
                s2 = rqlist->hibound + rqlist->lobound - 1.0;

                /* Adjust for double-exclusion of NULLs */
                s2 += nulltestsel(root, IS_NULL, rqlist->var, varRelid, jointype, sjinfo);

                /*
                 * A zero or slightly negative s2 should be converted into a
                 * small positive value; we probably are dealing with a very
                 * tight range and got a bogus result due to roundoff errors.
                 * However, if s2 is very negative, then we probably have
                 * default selectivity estimates on one or both sides of the
                 * range that we failed to recognize above for some reason.
                 */
                if (s2 <= 0.0) {
                    if (s2 < -0.01) {
                        /*
                         * No data available --- use a default estimate that
                         * is small, but not real small.
                         */
                        s2 = DEFAULT_RANGE_INEQ_SEL;
                    } else {
                        /*
                         * It's just roundoff error; use a small positive
                         * value
                         */
                        s2 = 1.0e-10;
                    }
                }
            }
            /* Merge in the selectivity of the pair of clauses */
            s1 *= s2;
            rqlist->clause->selec = s2;
        } else {
            /* Only found one of a pair, merge it in generically */
            if (rqlist->have_lobound) {
                s1 *= rqlist->lobound;
                rqlist->clause->selec = rqlist->lobound;
            } else {
                s1 *= rqlist->hibound;
                rqlist->clause->selec = rqlist->hibound;
            }
        }
        varlist = lappend(varlist, rqlist->var);
        /* release storage and advance */
        rqnext = rqlist->next;
        pfree_ext(rqlist);
        rqlist = rqnext;
    }

    /* we should cache the range query's var ratio if can do and there are range query's vars. */
    if (varratio_cached && varlist != NIL)
        set_varratio_for_rqclause(root, varlist, varRelid, s1, sjinfo);

    list_free_ext(varlist);

    /* free space used by extended statistic */
    if (es != NULL) {
        clauselist = NIL;
        list_free_ext(es->unmatched_clause_group);
        delete es;
        MemoryContextDelete(ExtendedStat);
    }

    if (ENABLE_CACHEDPLAN_MGR && root->glob->boundParams != NULL && root->glob->boundParams->uParamInfo != DEFUALT_INFO) {
        root->glob->boundParams->params_lazy_bind = true;
    }

    return s1;
}

/*
 * addRangeClause --- add a new range clause for clauselist_selectivity
 *
 * Here is where we try to match up pairs of range-query clauses
 */
static void addRangeClause(RangeQueryClause** rqlist, Node* clause, bool varonleft, bool isLTsel, Selectivity s2)
{
    RangeQueryClause* rqelem = NULL;
    Node* var = NULL;
    bool is_lobound = false;

    if (varonleft) {
        var = get_leftop((Expr*)clause);
        is_lobound = !isLTsel; /* x < something is high bound */
    } else {
        var = get_rightop((Expr*)clause);
        is_lobound = isLTsel; /* something < x is low bound */
    }

    for (rqelem = *rqlist; rqelem; rqelem = rqelem->next) {
        /*
         * We use full equal() here because the "var" might be a function of
         * one or more attributes of the same relation...
         */
        if (!equal(var, rqelem->var))
            continue;
        /* Found the right group to put this clause in */
        if (is_lobound) {
            if (!rqelem->have_lobound) {
                rqelem->have_lobound = true;
                rqelem->lobound = s2;
            } else {

                /* ------
                 * We have found two similar clauses, such as
                 * x < y AND x < z.
                 * Keep only the more restrictive one.
                 * ------
                 */
                if (rqelem->lobound > s2)
                    rqelem->lobound = s2;
            }
        } else {
            if (!rqelem->have_hibound) {
                rqelem->have_hibound = true;
                rqelem->hibound = s2;
            } else {

                /* ------
                 * We have found two similar clauses, such as
                 * x > y AND x > z.
                 * Keep only the more restrictive one.
                 * ------
                 */
                if (rqelem->hibound > s2)
                    rqelem->hibound = s2;
            }
        }
        rqelem->clause = (Expr*)clause;
        return;
    }

    /* No matching var found, so make a new clause-pair data structure */
    rqelem = (RangeQueryClause*)palloc(sizeof(RangeQueryClause));
    rqelem->var = var;
    if (is_lobound) {
        rqelem->have_lobound = true;
        rqelem->have_hibound = false;
        rqelem->lobound = s2;
    } else {
        rqelem->have_lobound = false;
        rqelem->have_hibound = true;
        rqelem->hibound = s2;
    }
    rqelem->clause = (Expr*)clause;
    rqelem->clause->selec = s2;
    rqelem->next = *rqlist;
    *rqlist = rqelem;
}

/*
 * treat_as_join_clause -
 *	  Decide whether an operator clause is to be handled by the
 *	  restriction or join estimator.  Subroutine for clause_selectivity().
 */
bool treat_as_join_clause(Node* clause, RestrictInfo* rinfo, int varRelid, SpecialJoinInfo* sjinfo)
{
    if (varRelid != 0) {
        /*
         * Caller is forcing restriction mode (eg, because we are examining an
         * inner indexscan qual).
         */
        return false;
    } else if (sjinfo == NULL) {
        /*
         * It must be a restriction clause, since it's being evaluated at a
         * scan node.
         */
        return false;
    } else {
        /*
         * Otherwise, it's a join if there's more than one relation used. We
         * can optimize this calculation if an rinfo was passed.
         *
         * XXX	Since we know the clause is being evaluated at a join, the
         * only way it could be single-relation is if it was delayed by outer
         * joins.  Although we can make use of the restriction qual estimators
         * anyway, it seems likely that we ought to account for the
         * probability of injected nulls somehow.
         */
        if (rinfo != NULL)
            return (bms_membership(rinfo->clause_relids) == BMS_MULTIPLE);
        else
            return (NumRelids(clause) > 1);
    }
}

#ifndef ENABLE_MULTIPLE_NODES

/*
 * bms_is_subset_singleton
 *
 * Same result as bms_is_subset(s, bms_make_singleton(x)),
 * but a little faster and doesn't leak memory.
 *
 * Is this of use anywhere else?  If so move to bitmapset.c ...
 */
static bool
bms_is_subset_singleton(const Bitmapset *s, int x)
{
    BMS_Membership type = bms_membership(s);
    if (type == BMS_EMPTY_SET) {
        return true;
    } else if (type == BMS_SINGLETON) {
        return bms_is_member(x, s);
    } else {
        return false;
    }
}
#endif


/*
 * clause_selectivity -
 *	  Compute the selectivity of a general boolean expression clause.
 *
 * The clause can be either a RestrictInfo or a plain expression.  If it's
 * a RestrictInfo, we try to cache the selectivity for possible re-use,
 * so passing RestrictInfos is preferred.
 *
 * varRelid is either 0 or a rangetable index.
 *
 * When varRelid is not 0, only variables belonging to that relation are
 * considered in computing selectivity; other vars are treated as constants
 * of unknown values.  This is appropriate for estimating the selectivity of
 * a join clause that is being used as a restriction clause in a scan of a
 * nestloop join's inner relation --- varRelid should then be the ID of the
 * inner relation.
 *
 * When varRelid is 0, all variables are treated as variables.	This
 * is appropriate for ordinary join clauses and restriction clauses.
 *
 * jointype is the join type, if the clause is a join clause.  Pass JOIN_INNER
 * if the clause isn't a join clause.
 *
 * sjinfo is NULL for a non-join clause, otherwise it provides additional
 * context information about the join being performed.	There are some
 * special cases:
 *	1. For a special (not INNER) join, sjinfo is always a member of
 *	   root->join_info_list.
 *	2. For an INNER join, sjinfo is just a transient struct, and only the
 *	   relids and jointype fields in it can be trusted.
 * It is possible for jointype to be different from sjinfo->jointype.
 * This indicates we are considering a variant join: either with
 * the LHS and RHS switched, or with one input unique-ified.
 *
 * Note: when passing nonzero varRelid, it's normally appropriate to set
 * jointype == JOIN_INNER, sjinfo == NULL, even if the clause is really a
 * join clause; because we aren't treating it as a join clause.
 *
 * sjinfo: identify join info include lefthand/righthand.
 * varratio_cached: if we cache the selectivity into the relation or not for estimate distinct using possion.
 * check_scalarop: if we need to check tha scalar op which belong to a range query clause, it is true.
 */
Selectivity clause_selectivity(PlannerInfo* root, Node* clause, int varRelid, JoinType jointype,
    SpecialJoinInfo* sjinfo, bool varratio_cached, bool check_scalarop, bool use_poisson)
{
    Selectivity s1 = 0.5; /* default for any unhandled clause type */
    RestrictInfo* rinfo = NULL;
    bool cacheable = false;
    RatioType ratiotype = RatioType_Filter;

    if (clause == NULL) /* can this still happen? */
        return s1;

    if (IsA(clause, RestrictInfo)) {
        rinfo = (RestrictInfo*)clause;

        /*
         * If the clause is marked pseudoconstant, then it will be used as a
         * gating qual and should not affect selectivity estimates; hence
         * return 1.0.	The only exception is that a constant FALSE may be
         * taken as having selectivity 0.0, since it will surely mean no rows
         * out of the plan.  This case is simple enough that we need not
         * bother caching the result.
         */
        if (rinfo->pseudoconstant) {
            if (!IsA(rinfo->clause, Const))
                return (Selectivity)1.0;
        }

        /*
         * If the clause is marked redundant, always return 1.0.
         */
        if (rinfo->norm_selec > 1)
            return (Selectivity)1.0;

        /*
         * If possible, cache the result of the selectivity calculation for
         * the clause.	We can cache if varRelid is zero or the clause
         * contains only vars of that relid --- otherwise varRelid will affect
         * the result, so mustn't cache.  Outer join quals might be examined
         * with either their join's actual jointype or JOIN_INNER, so we need
         * two cache variables to remember both cases.	Note: we assume the
         * result won't change if we are switching the input relations or
         * considering a unique-ified case, so we only need one cache variable
         * for all non-JOIN_INNER cases.
         */
#ifndef ENABLE_MULTIPLE_NODES
        if (!use_poisson &&
            (varRelid == 0 || bms_is_subset_singleton(rinfo->clause_relids, varRelid))) {
            /* Cacheable --- do we already have the result? */
            if (jointype == JOIN_INNER) {
                if (rinfo->norm_selec >= 0) {
                    return rinfo->norm_selec;
                }
            } else {
                if (rinfo->outer_selec >= 0) {
                    return rinfo->outer_selec;
                }
            }
            cacheable = true;
        } else {
#endif
        cacheable = true;
#ifndef ENABLE_MULTIPLE_NODES
    }
#endif

        /*
         * Proceed with examination of contained clause.  If the clause is an
         * OR-clause, we want to look at the variant with sub-RestrictInfos,
         * so that per-subclause selectivities can be cached.
         */
        if (rinfo->orclause)
            clause = (Node*)rinfo->orclause;
        else
            clause = (Node*)rinfo->clause;
    }

    if (IsA(clause, Var)) {
        Var* var = (Var*)clause;

        /*
         * We probably shouldn't ever see an uplevel Var here, but if we do,
         * return the default selectivity...
         */
        if (var->varlevelsup == 0 && (varRelid == 0 || varRelid == (int)var->varno)) {
            /*
             * A Var at the top of a clause must be a bool Var. This is
             * equivalent to the clause reln.attribute = 't', so we compute
             * the selectivity as if that is what we have.
             */
            s1 = restriction_selectivity(
                root, BooleanEqualOperator, list_make2(var, makeBoolConst(true, false)), InvalidOid, varRelid);
        }
    } else if (IsA(clause, Const)) {
        /* bool constant is pretty easy... */
        Const* con = (Const*)clause;

        s1 = con->constisnull ? 0.0 : DatumGetBool(con->constvalue) ? 1.0 : 0.0;
    } else if (IsA(clause, Param)) {
        /* see if we can replace the Param */
        Node* subst = estimate_expression_value(root, clause);

        if (IsA(subst, Const)) {
            /* bool constant is pretty easy... */
            Const* con = (Const*)subst;

            s1 = con->constisnull ? 0.0 : DatumGetBool(con->constvalue) ? 1.0 : 0.0;
        }
    } else if (not_clause(clause)) {
        /* inverse of the selectivity of the underlying clause */
        s1 = 1.0 - clause_selectivity(root, (Node*)get_notclausearg((Expr*)clause), varRelid, jointype, sjinfo);
    } else if (and_clause(clause)) {
        /* share code with clauselist_selectivity() */
        s1 = clauselist_selectivity(root, ((BoolExpr*)clause)->args, varRelid, jointype, sjinfo, varratio_cached);
    } else if (or_clause(clause)) {
        /*
         * Selectivities for an OR clause are computed as s1+s2 - s1*s2 to
         * account for the probable overlap of selected tuple sets.
         *
         * XXX is this too conservative?
         */
        ListCell* arg = NULL;

        s1 = 0.0;
        foreach (arg, ((BoolExpr*)clause)->args) {
            /* DO NOT cache the var ratio of single or-clauses */
            Selectivity s2 = clause_selectivity(root, (Node*)lfirst(arg), varRelid, jointype, sjinfo, false);

            s1 = s1 + s2 - s1 * s2;
        }
        /*
         * Ideally, or-clauses should be splitted into groups identified by Var oprend. However, Poisson optimization
         * is known to bring about NDV underestimation and cardinality overestimation in OLTP cases. Also, it is hard
         * to take acount of the effect of different Vars (e.g. t1.a = 1 or t2.b = 1) on single Vars.
         * Therefore, or clauses is ignored for var ratio cache for now.
         */
    } else if (is_opclause(clause) || IsA(clause, DistinctExpr)) {
        OpExpr* opclause = (OpExpr*)clause;
        Oid opno = opclause->opno;

        if (treat_as_join_clause(clause, rinfo, varRelid, sjinfo)) {
            /* Estimate selectivity for a join clause. */
            s1 = join_selectivity(root, opno, opclause->args, opclause->inputcollid, jointype, sjinfo);
            ratiotype = RatioType_Join;
        } else {
            /* Estimate selectivity for a restriction clause. */
            bool isFinish = false;
            if (is_opclause(clause)) {
                Oid eqlOprOid = 0;
                Oid inputcollid = 0;
                List* argList = NULL;

                /* only handle = or <> operator */
                if (get_oprrest(opno) == EQSELRETURNOID || get_oprrest(opno) == NEQSELRETURNOID) {
                    argList = do_restrictinfo_conversion(
                        opclause->args, &eqlOprOid, &inputcollid, get_oprrest(opno) == EQSELRETURNOID);

                    if (argList != NULL) {
                        s1 = restriction_selectivity(root, eqlOprOid, argList, inputcollid, varRelid);
                        isFinish = true;
                    }
                }
            }

            if (isFinish == false) {
                s1 = restriction_selectivity(root, opno, opclause->args, opclause->inputcollid, varRelid);
            }
        }

        /*
         * DistinctExpr has the same representation as OpExpr, but the
         * contained operator is "=" not "<>", so we must negate the result.
         * This estimation method doesn't give the right behavior for nulls,
         * but it's better than doing nothing.
         */
        if (IsA(clause, DistinctExpr))
            s1 = 1.0 - s1;
    } else if (is_funcclause(clause)) {
        /*
         * This is not an operator, so we guess at the selectivity. THIS IS A
         * HACK TO GET V4 OUT THE DOOR.  FUNCS SHOULD BE ABLE TO HAVE
         * SELECTIVITIES THEMSELVES.	   -- JMH 7/9/92
         */
        s1 = (Selectivity)0.3333333;
    }
#ifdef NOT_USED
    else if (IsA(clause, SubPlan) || IsA(clause, AlternativeSubPlan)) {
        /*
         * Just for the moment! FIX ME! - vadim 02/04/98
         */
        s1 = (Selectivity)0.5;
    }
#endif
    else if (IsA(clause, ScalarArrayOpExpr)) {
        bool is_join_clause = treat_as_join_clause(clause, rinfo, varRelid, sjinfo);
        if (is_join_clause)
            ratiotype = RatioType_Join;

        /* Use node specific selectivity calculation function */
        s1 = scalararraysel(root, (ScalarArrayOpExpr*)clause, is_join_clause, varRelid, jointype, sjinfo);
    } else if (IsA(clause, RowCompareExpr)) {
        /* Use node specific selectivity calculation function */
        s1 = rowcomparesel(root, (RowCompareExpr*)clause, varRelid, jointype, sjinfo);
    } else if (IsA(clause, NullTest)) {
        /* Use node specific selectivity calculation function */
        s1 = nulltestsel(
            root, ((NullTest*)clause)->nulltesttype, (Node*)((NullTest*)clause)->arg, varRelid, jointype, sjinfo);
    } else if (IsA(clause, BooleanTest)) {
        /* Use node specific selectivity calculation function */
        s1 = booltestsel(
            root, ((BooleanTest*)clause)->booltesttype, (Node*)((BooleanTest*)clause)->arg, varRelid, jointype, sjinfo);
    } else if (IsA(clause, CurrentOfExpr)) {
        /* CURRENT OF selects at most one row of its table */
        CurrentOfExpr* cexpr = (CurrentOfExpr*)clause;
        RelOptInfo* crel = find_base_rel(root, cexpr->cvarno);

        if (crel->tuples > 0)
            s1 = 1.0 / crel->tuples;
    } else if (IsA(clause, RelabelType)) {
        /* Not sure this case is needed, but it can't hurt */
        s1 = clause_selectivity(root, (Node*)((RelabelType*)clause)->arg, varRelid, jointype, sjinfo);
    } else if (IsA(clause, CoerceToDomain)) {
        /* Not sure this case is needed, but it can't hurt */
        s1 = clause_selectivity(root, (Node*)((CoerceToDomain*)clause)->arg, varRelid, jointype, sjinfo);
    }

    /* Cache the result if possible */
    if (cacheable) {
        if (jointype == JOIN_INNER)
            rinfo->norm_selec = s1;
        else
            rinfo->outer_selec = s1;
    }

    /*
     * if it is filter for baserel and cached, or not inner join,
     * we should cache the var's selectivity into relation.
     */
    if (u_sess->opfusion_reuse_ctx.opfusionObj == NULL) {
        if (use_poisson &&
            ((RatioType_Filter == ratiotype && varratio_cached) || (jointype == JOIN_SEMI) || (jointype == JOIN_ANTI)) &&
            (!check_scalarop || !is_rangequery_contain_scalarop(clause, rinfo))) {
                get_vardata_for_filter_or_semijoin(root, clause, varRelid, s1, sjinfo, ratiotype); 
            }
    }

#ifdef SELECTIVITY_DEBUG
    ereport(DEBUG4, (errmodule(MOD_OPT_JOIN), (errmsg("clause_selectivity: s1 %f", s1))));
#endif /* SELECTIVITY_DEBUG */

    return s1;
}

/* Produce arg list, const convert to expr type */
static List* switch_arg_items(Node* funExpr, Const* cnst, Oid* eqlOprOid, Oid* inputcollid, bool isequal)
{
    List* argList = NULL;
    Node* arg = NULL;
    Const* cnp = NULL;
    Oid argType = InvalidOid;

    if (IsA(funExpr, FuncExpr) && ((FuncExpr*)funExpr)->funcformat == COERCE_IMPLICIT_CAST) {
        FuncExpr* fun_expr = (FuncExpr*)funExpr;
        arg = (Node*)linitial(fun_expr->args);
        argType = exprType(arg);
        HeapTuple typeTuple;
        Oid funcId = 0;
        Oid constType = exprType((Node*)cnst);
        Datum constValue = (Datum)0;

        /* We only deal with datatypes that their categorys is different */
        if (TypeCategory(argType) == TypeCategory(constType)) {
            return NIL;
        }

        CoercionPathType pathtype = find_coercion_pathway(argType, constType, COERCION_IMPLICIT, &funcId);

        if (pathtype != COERCION_PATH_NONE) {
            MemoryContext current_context = CurrentMemoryContext;
            bool outer_is_stream = false;
            bool outer_is_stream_support = false;
            ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
            ResourceOwner tempOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "SwitchArgItems",
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
            t_thrd.utils_cxt.CurrentResourceOwner = tempOwner;

            if (IS_PGXC_COORDINATOR) {
                outer_is_stream = u_sess->opt_cxt.is_stream;
                outer_is_stream_support = u_sess->opt_cxt.is_stream_support;
            }

            PG_TRY();
            {
                constValue = OidFunctionCall1(funcId, ((Const*)cnst)->constvalue);
            }
            PG_CATCH();
            {
                MemoryContextSwitchTo(current_context);
                FlushErrorState();

                /* in case they are not set back */
                if (IS_PGXC_COORDINATOR) {
                    u_sess->opt_cxt.is_stream = outer_is_stream;
                    u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
                }

                /* release resource applied in OidFunctionCall1 of the PG_TRY. */
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_LOCKS, false, false);
                ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
                t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
                ResourceOwnerDelete(tempOwner);

                return NIL;
            }
            PG_END_TRY();

            /* release resource applied in standard_planner of the PG_TRY. */
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_LOCKS, false, false);
            ResourceOwnerRelease(tempOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
            ResourceOwnerDelete(tempOwner);

            if (IS_PGXC_COORDINATOR) {
                u_sess->opt_cxt.is_stream = outer_is_stream;
                u_sess->opt_cxt.is_stream_support = outer_is_stream_support;
            }
        }

        if (constValue) {
            typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(argType));
            if (!HeapTupleIsValid(typeTuple)) {
                return NIL;
            }
            Form_pg_type type = (Form_pg_type)GETSTRUCT(typeTuple);
            cnp = makeConst(
                argType, exprTypmod(arg), type->typcollation, type->typlen, constValue, false, type->typbyval);
            ReleaseSysCache(typeTuple);
        }
    }
    if (cnp != NULL) {
        argList = lappend(argList, arg);
        argList = lappend(argList, cnp);

        if (argType == VARCHAROID) {
            argType = TEXTOID;
        }

        HeapTuple opertup;
        opertup = SearchSysCache4(OPERNAMENSP,
            CStringGetDatum(isequal ? "=" : "<>"),
            ObjectIdGetDatum(argType),
            ObjectIdGetDatum(argType),
            ObjectIdGetDatum(PG_CATALOG_NAMESPACE));
        if (!HeapTupleIsValid(opertup)) {
            return NIL;
        }

        *eqlOprOid = HeapTupleGetOid(opertup);

        ReleaseSysCache(opertup);

        *inputcollid = exprCollation(arg);
    }

    return argList;
}

static List* do_restrictinfo_conversion(List* args, Oid* eqlOprOid, Oid* inputcollid, bool isequal)
{
    AssertEreport(list_length(args) == 2, MOD_OPT, "");

    bool lIsConst = false;
    bool rIsConst = false;
    Node* lNode = (Node*)linitial(args);
    Node* rNode = (Node*)list_nth(args, 1);

    List* argsList = NULL;

    if (IsA(lNode, Const)) {
        lIsConst = true;
    }

    if (IsA(rNode, Const)) {
        rIsConst = true;
    }

    if (lIsConst == true && rIsConst == false) {
        argsList = switch_arg_items(rNode, (Const*)lNode, eqlOprOid, inputcollid, isequal);
    } else if (lIsConst == false && rIsConst == true) {
        argsList = switch_arg_items(lNode, (Const*)rNode, eqlOprOid, inputcollid, isequal);
    }

    return argsList;
}

/*
 * get_vardata_for_filter_or_semijoin: get var data and cache selectivity for filter or semi/anti join.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	clause: estimate tuples for LIMIT
 *	@in	varRelid: varRelid is either 0 or a rangetable index, When varRelid is not 0,
 *           only variables belonging to that relation are considered in computing selectivity.
 *	@in selec: selectivity for clause
 *	@in	sjinfo: special join info for the joinrel
 *	@in	type: filter ratio or join ratio
 *
 * Returns: void
 */
static void get_vardata_for_filter_or_semijoin(
    PlannerInfo* root, Node* clause, int varRelid, Selectivity selec, SpecialJoinInfo* sjinfo, RatioType type)
{
    get_vardata_for_filter_or_semijoin_context context;
    bool vardataIsValid = false;

    /* construct context members. */
    context.root = root;
    context.varRelid = varRelid;
    context.ratiotype = type;
    context.sjinfo = sjinfo;
    errno_t rc = EOK;

    rc = memset_s(&context.filter_vardata, sizeof(VariableStatData), 0, sizeof(VariableStatData));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&context.semijoin_vardata1, sizeof(VariableStatData), 0, sizeof(VariableStatData));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&context.semijoin_vardata2, sizeof(VariableStatData), 0, sizeof(VariableStatData));
    securec_check(rc, "\0", "\0");

    /* get vardata walker for clause. */
    vardataIsValid = get_vardata_for_filter_or_semijoin_walker(clause, &context);

    /* we don't need set var ratio if vardata is invalid. */
    if (!vardataIsValid) {
        return;
    }

    /* set var ratio for filter or semi/anti join. */
    if (RatioType_Filter == type) {
        set_varratio_after_calc_selectivity(&context.filter_vardata, RatioType_Filter, selec, NULL);
        ReleaseVariableStats(context.filter_vardata);
    } else {
        set_varratio_after_calc_selectivity(&context.semijoin_vardata1, RatioType_Join, selec, sjinfo);
        set_varratio_after_calc_selectivity(&context.semijoin_vardata2, RatioType_Join, selec, sjinfo);

        ReleaseVariableStats(context.semijoin_vardata1);
        ReleaseVariableStats(context.semijoin_vardata2);
    }
}

void getVardataFromScalarArray(Node* node, get_vardata_for_filter_or_semijoin_context* context)
{
    Node* left = NULL;
    if (RatioType_Join == context->ratiotype) {
        bool join_is_reversed = false;
        get_join_variables(context->root, ((ScalarArrayOpExpr*)node)->args, context->sjinfo, 
            &context->semijoin_vardata1, &context->semijoin_vardata2, &join_is_reversed);
    } else {
        left = (Node*)linitial(((ScalarArrayOpExpr*)node)->args);
        examine_variable(context->root, left, context->varRelid, &context->filter_vardata);
    }
}

/*
 * get_vardata_for_filter_or_semijoin_walker: get vardata walker for clause.
 *
 * Parameters:
 *	@in node: the clause node in restrict info
 *	@in	context: the context with in params and will get vardata from clause's args
 *
 * Returns: bool(true:vardata is valid)
 */
static bool get_vardata_for_filter_or_semijoin_walker(Node* node, get_vardata_for_filter_or_semijoin_context* context)
{
    List* args = NIL;
    Node* other = NULL;
    Node* left = NULL;
    Node* clause = NULL;
    bool varonleft = false;

    if (node == NULL)
        return false;

    /* get vardata info from different clause's args. */
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /*
         * We probably shouldn't ever see an uplevel Var here, but if we do,
         * return the default selectivity...
         */
        if (var->varlevelsup == 0 && (context->varRelid == 0 || context->varRelid == (int)var->varno)) {
            examine_variable(context->root, (Node*)var, context->varRelid, &context->filter_vardata);
            return true;
        }

        return false;
    } else if (not_clause(node)) {
        clause = (Node*)get_notclausearg((Expr*)node);
    } else if (is_opclause(node)) {
        OpExpr* opclause = (OpExpr*)node;
        Oid opno = opclause->opno;

        if (RatioType_Join == context->ratiotype) {
            bool join_is_reversed = false;
            get_join_variables(context->root, opclause->args, context->sjinfo, &context->semijoin_vardata1,
                &context->semijoin_vardata2, &join_is_reversed);
            return true;
        } else {
            Oid eqlOprOid = 0;
            List* argList = NULL;
            Oid inputcollid = 0;
            /* only handle = or <> operator */
            if (get_oprrest(opno) == EQSELRETURNOID || get_oprrest(opno) == NEQSELRETURNOID) {
                argList = do_restrictinfo_conversion(
                    opclause->args, &eqlOprOid, &inputcollid, get_oprrest(opno) == EQSELRETURNOID);
            }

            if (argList != NULL)
                args = argList;
            else
                args = opclause->args;

            return get_restriction_variable(
                context->root, args, context->varRelid, &context->filter_vardata, &other, &varonleft);
        }
    } else if (IsA(node, ScalarArrayOpExpr)) {
        getVardataFromScalarArray(node, context);
        return true;
    } else if (IsA(node, RowCompareExpr)) {
        args = list_make2(linitial(((RowCompareExpr*)node)->largs), linitial(((RowCompareExpr*)node)->rargs));
        return get_restriction_variable(
            context->root, args, context->varRelid, &context->filter_vardata, &other, &varonleft);
    } else if (IsA(node, NullTest)) {
        left = (Node*)((NullTest*)node)->arg;
        examine_variable(context->root, left, context->varRelid, &context->filter_vardata);
        return true;
    } else if (IsA(node, BooleanTest)) {
        left = (Node*)((BooleanTest*)node)->arg;
        examine_variable(context->root, left, context->varRelid, &context->filter_vardata);
        return true;
    } else if (IsA(node, RelabelType)) {
        clause = (Node*)((RelabelType*)node)->arg;
    } else if (IsA(node, CoerceToDomain)) {
        clause = (Node*)((CoerceToDomain*)node)->arg;
    }

    return get_vardata_for_filter_or_semijoin_walker(clause, context);
}

/*
 * is_rangequery_clause: the clause is range query or not.
 *
 * Parameters:
 *	@in clause: the clause node in restrict info
 *	@in	rinfo: restrict info
 *	@in	varonleft: identify the var in clause on left or right
 *
 * Returns: bool(true:the clause is range query)
 */
static bool is_rangequery_clause(Node* clause, RestrictInfo* rinfo, bool* varonleft)
{
    bool isrqclause = false;

    /*
     * See if it looks like a restriction clause with a pseudoconstant on
     * one side.  (Anything more complicated than that might not behave in
     * the simple way we are expecting.)  Most of the tests here can be
     * done more efficiently with rinfo than without.
     */
    if (is_opclause(clause) && list_length(((OpExpr*)clause)->args) == 2) {
        OpExpr* expr = (OpExpr*)clause;

        if (rinfo != NULL) {
            isrqclause = (bms_membership(rinfo->clause_relids) == BMS_SINGLETON) &&
                         (is_pseudo_constant_clause_relids((Node*)lsecond(expr->args), rinfo->right_relids) ||
                             (*varonleft = false,
                                 is_pseudo_constant_clause_relids((Node*)linitial(expr->args), rinfo->left_relids)));
        } else {
            isrqclause = (NumRelids(clause) == 1) &&
                         (is_pseudo_constant_clause((Node*)lsecond(expr->args)) ||
                             (*varonleft = false, is_pseudo_constant_clause((Node*)linitial(expr->args))));
        }
    }

    return isrqclause;
}

/*
 * is_rangequery_contain_scalarop: if the range query contain scalar operator or not.
 *
 * Parameters:
 *	@in clause: the clause node in restrict info
 *	@in	rinfo: restrict info
 *
 * Returns: bool(true:the range query contain scalar operator)
 */
static bool is_rangequery_contain_scalarop(Node* clause, RestrictInfo* rinfo)
{
    bool varonleft = false;

    if (is_rangequery_clause(clause, rinfo, &varonleft)) {
        OpExpr* expr = (OpExpr*)clause;

        if ((F_SCALARLTSEL == get_oprrest(expr->opno)) || (F_SCALARGTSEL == get_oprrest(expr->opno)))
            return true;
    }

    return false;
}

/*
 * set_varratio_for_rqclause: set var ratio for range query clause.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in varlist: the range query clause contain many vars
 *	@in varRelid: varRelid is either 0 or a rangetable index, When varRelid is not 0,
 *           only variables belonging to that relation are considered in computing selectivity.
 *	@in ratio: join ratio according to estimation
 * 	@in sjinfo: the join info for current relation join with others
 *
 * Returns: void
 */
static void set_varratio_for_rqclause(
    PlannerInfo* root, List* varlist, int varRelid, double ratio, SpecialJoinInfo* sjinfo)
{
    ListCell* lc = NULL;

    foreach (lc, varlist) {
        VariableStatData vardata;
        Node* node = (Node*)lfirst(lc);

        examine_variable(root, node, varRelid, &vardata);

        if (sjinfo == NULL)
            set_varratio_after_calc_selectivity(&vardata, RatioType_Filter, ratio, NULL);
        else
            set_varratio_after_calc_selectivity(&vardata, RatioType_Join, ratio, sjinfo);

        ReleaseVariableStats(vardata);
    }
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * find_single_rel_for_clauses
 *		Examine each clause in 'clauses' and determine if all clauses
 *		reference only a single relation.  If so return that relation,
 *		otherwise return NULL.
 */
static RelOptInfo* find_single_rel_for_clauses(PlannerInfo* root, const List* clauses)
{
    int lastrelid = 0;
    ListCell* l;

    foreach (l, clauses) {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(l);
        int relid;

        /*
         * If we have a list of bare clauses rather than RestrictInfos, we
         * could pull out their relids the hard way with pull_varnos().
         * However, currently the extended-stats machinery won't do anything
         * with non-RestrictInfo clauses anyway, so there's no point in
         * spending extra cycles; just fail if that's what we have.
         *
         * An exception to that rule is if we have a bare BoolExpr AND clause.
         * We treat this as a special case because the restrictinfo machinery
         * doesn't build RestrictInfos on top of AND clauses.
         */
        if (rinfo != NULL && IsA(rinfo, BoolExpr) && ((const BoolExpr *)rinfo)->boolop == AND_EXPR) {
            RelOptInfo *rel;

            rel = find_single_rel_for_clauses(root, ((BoolExpr *)rinfo)->args);
            if (rel == NULL) {
                return NULL;
            }
            if (lastrelid == 0) {
                lastrelid = rel->relid;
                continue;
            }
            if (lastrelid != 0 && (int)(rel->relid) != lastrelid) {
                return NULL;
            }
        }

        if (!IsA(rinfo, RestrictInfo)) {
            return NULL;
        }

        if (bms_is_empty(rinfo->clause_relids)) {
            continue; /* we can ignore variable-free clauses */
        }
        if (!bms_get_singleton_member(rinfo->clause_relids, &relid)) {
            return NULL; /* multiple relations in this clause */
        }
        if (lastrelid == 0) {
            lastrelid = relid; /* first clause referencing a relation */
        }
        if (lastrelid != 0 && relid != lastrelid) {
            return NULL; /* relation not same as last one */
        }
    }

    if (lastrelid != 0) {
        return find_base_rel(root, lastrelid);
    }

    return NULL; /* no clauses */
}

/*
 * calculate_selectivity_dependency
 *		Calculate selectivity through functional dependency statistics
 */
static Selectivity calculate_selectivity_dependency(bool flag_dependency, PlannerInfo *root, ES_SELECTIVITY *es,
    int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo, List** clauselist)
{
    if (!flag_dependency) {
        return 1.0;
    }

    RelOptInfo *rel = NULL;
    Selectivity sel = 1.0;
    ListCell *l = NULL;

    rel = find_single_rel_for_clauses(root, es->unmatched_clause_group);
    if (rel && rel->rtekind == RTE_RELATION) {
        int listidx = 0;
        Bitmapset *estimatedclauses = NULL;

        rel->statlist = es->statlist;
        sel = dependencies_clauselist_selectivity(root, es->unmatched_clause_group, varRelid, jointype, sjinfo, rel,
            &estimatedclauses);
        *clauselist = NULL;
        foreach (l, es->unmatched_clause_group) {
            Node *clause = (Node *)lfirst(l);
            if (!bms_is_member(listidx, estimatedclauses)) {
                *clauselist = lappend(*clauselist, clause);
            }
            listidx++;
        }
    }
    return sel;
}
#endif
