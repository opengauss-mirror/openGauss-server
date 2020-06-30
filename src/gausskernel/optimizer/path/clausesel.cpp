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
    PlannerInfo* root, List* clauses, int varRelid, JoinType jointype, SpecialJoinInfo* sjinfo, bool varratio_cached)
{
    Selectivity s1 = 1.0;
    RangeQueryClause* rqlist = NULL;
    ListCell* l = NULL;
    List* varlist = NIL;
    List* clauselist = clauses;
    ES_SELECTIVITY* es = NULL;
    MemoryContext ExtendedStat;
    MemoryContext oldcontext;

    /*
     * If there's exactly one clause, then no use in trying to match up pairs,
     * so just go directly to clause_selectivity().
     */
    if (list_length(clauses) == 1)
        return clause_selectivity(root, (Node*)linitial(clauses), varRelid, jointype, sjinfo, varratio_cached);

    /* initialize es_selectivity class, list_length(clauses) can be 0 when called by set_baserel_size_estimates */
    if (list_length(clauses) >= 2 &&
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
        s2 = clause_selectivity(root, clause, varRelid, jointype, sjinfo, varratio_cached, true);

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
                    if ((uint32)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT)
                        s1 = MIN(s1, s2);
                    else
                        s1 = s1 * s2;
                    break;
            }
            continue;
        }

        /* Not the right form, so treat it generically. */
        if ((uint32)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_CONJUNCT)
            s1 = MIN(s1, s2);
        else
            s1 = s1 * s2;
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
        } else {
            /* Only found one of a pair, merge it in generically */
            if (rqlist->have_lobound)
                s1 *= rqlist->lobound;
            else
                s1 *= rqlist->hibound;
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
    SpecialJoinInfo* sjinfo, bool varratio_cached, bool check_scalarop)
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
        cacheable = true;

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
            Selectivity s2 = clause_selectivity(root, (Node*)lfirst(arg), varRelid, jointype, sjinfo);

            s1 = s1 + s2 - s1 * s2;
        }
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
    if (((RatioType_Filter == ratiotype && varratio_cached) || (jointype == JOIN_SEMI) || (jointype == JOIN_ANTI)) &&
        (!check_scalarop || !is_rangequery_contain_scalarop(clause, rinfo)))
        get_vardata_for_filter_or_semijoin(root, clause, varRelid, s1, sjinfo, ratiotype);

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
            ResourceOwner tempOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "SwitchArgItems");
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
            get_join_variables(context->root,
                opclause->args,
                context->sjinfo,
                &context->semijoin_vardata1,
                &context->semijoin_vardata2,
                &join_is_reversed);
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
        left = (Node*)linitial(((ScalarArrayOpExpr*)node)->args);
        examine_variable(context->root, left, context->varRelid, &context->filter_vardata);
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

ES_SELECTIVITY::ES_SELECTIVITY()
    : es_candidate_list(NULL),
      unmatched_clause_group(NULL),
      root(NULL),
      sjinfo(NULL),
      origin_clauses(NULL),
      path(NULL),
      bucketsize_list(NULL)
{}

ES_SELECTIVITY::~ES_SELECTIVITY()
{}

/*
 * @brief		Main entry for using extended statistic to calculate selectivity
 * 			root_input can only be NULL when processing group by clauses
 */
Selectivity ES_SELECTIVITY::calculate_selectivity(PlannerInfo* root_input, List* clauses_input,
    SpecialJoinInfo* sjinfo_input, JoinType jointype, JoinPath* path_input, es_type action, STATS_EST_TYPE eType)
{
    Selectivity result = 1.0;
    root = root_input;
    sjinfo = sjinfo_input;
    origin_clauses = clauses_input;
    path = path_input;
    RelOptInfo* inner_rel = NULL;
    bool inner_on_left = false;

    /* group clauselist */
    if (action == ES_GROUPBY) {
        /* group clauselist for group by clauses */
        group_clauselist_groupby(origin_clauses);
    } else {
        group_clauselist(origin_clauses);
    }

    /* read statistic */
    read_statistic();

    /* calculate selectivity */
    ListCell* l = NULL;
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
                result *= cal_eqsel(temp);
                break;
            case ES_EQJOINSEL:
                /* compute hash bucket size */
                if (action == ES_COMPUTEBUCKETSIZE) {
                    es_bucketsize* bucket = (es_bucketsize*)palloc(sizeof(es_bucketsize));
                    cal_bucket_size(temp, bucket);
                    bucketsize_list = lappend(bucketsize_list, bucket);
                } else {
                    switch (jointype) {
                        case JOIN_INNER:
                        case JOIN_LEFT:
                        case JOIN_FULL:
                            result *= cal_eqjoinsel_inner(temp);
                            break;
                        case JOIN_SEMI:
                        case JOIN_ANTI:
                        case JOIN_LEFT_ANTI_FULL:
                            /*
                             * Look up the join's inner relation.  min_righthand is sufficient
                             * information because neither SEMI nor ANTI joins permit any
                             * reassociation into or out of their RHS, so the righthand will
                             * always be exactly that set of rels.
                             */
                            inner_rel = find_join_input_rel(root, sjinfo->min_righthand);

                            /* inner_rel could be a join rel */
                            inner_on_left = (bms_is_subset(temp->left_rel->relids, inner_rel->relids));
                            if (!inner_on_left) {
                                Assert(bms_is_subset(temp->right_rel->relids, inner_rel->relids));
                            }
                            result *= cal_eqjoinsel_semi(temp, inner_rel, inner_on_left);
                            break;
                        default:
                            /* other values not expected here */
                            break;
                    }
                }
                break;
            case ES_GROUPBY:
                build_pseudo_varinfo(temp, eType);
                break;
            default:
                break;
        }
    }

    /* free memory, but unmatched_clause_group need to be free manually */
    clear();

    return result;
}

/*
 * @brief	 	group clause by clause type and involving rels, for now, only support eqsel and eqjoinsel
 */
void ES_SELECTIVITY::group_clauselist(List* clauses)
{
    ListCell* l = NULL;
    foreach (l, clauses) {
        Node* clause = (Node*)lfirst(l);

        if (!IsA(clause, RestrictInfo)) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            continue;
        }

        RestrictInfo* rinfo = (RestrictInfo*)clause;
        if (rinfo->pseudoconstant || rinfo->norm_selec > 1 || rinfo->orclause) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            continue;
        }

        if (is_opclause(rinfo->clause)) {
            OpExpr* opclause = (OpExpr*)rinfo->clause;
            Oid opno = opclause->opno;

            /* only handle "=" operator */
            if (get_oprrest(opno) == EQSELRETURNOID) {
                int relid_num = bms_num_members(rinfo->clause_relids);
                if (relid_num == 1) {
                    /* only process clause like t1.a = 1, so only one relid */
                    load_eqsel_clause(rinfo);
                    continue;
                } else if (relid_num == 2) {
                    /* only process clause like t1.a = t2.b, so only two relids */
                    load_eqjoinsel_clause(rinfo);
                    continue;
                } else {
                    unmatched_clause_group = lappend(unmatched_clause_group, rinfo);
                    continue;
                }
            }
        } else if ((rinfo->clause) != NULL && IsA(rinfo->clause, NullTest)) {
            NullTest* nullclause = (NullTest*)rinfo->clause;
            int relid_num = bms_num_members(rinfo->clause_relids);
            if (relid_num == 1 && nullclause->nulltesttype == IS_NULL) {
                load_eqsel_clause(rinfo);
                continue;
            }
        }

        unmatched_clause_group = lappend(unmatched_clause_group, clause);
    }

    recheck_candidate_list();
    debug_print();

    return;
}

/*
 * @brief    	group groupby-clause by clause type and involving rels, for
 */
void ES_SELECTIVITY::group_clauselist_groupby(List* varinfos)
{
    ListCell* l = NULL;
    foreach (l, varinfos) {
        GroupVarInfo* varinfo = (GroupVarInfo*)lfirst(l);
        if (!is_var_node(varinfo->var)) {
            unmatched_clause_group = lappend(unmatched_clause_group, varinfo);
            continue;
        }

        Var* var = NULL;
        if (IsA(varinfo->var, RelabelType))
            var = (Var*)((RelabelType*)varinfo->var)->arg;
        else
            var = (Var*)varinfo->var;

        ListCell* l2 = NULL;
        bool found_match = false;
        foreach (l2, es_candidate_list) {
            es_candidate* temp = (es_candidate*)lfirst(l2);

            if (temp->tag != ES_GROUPBY)
                continue;

            if (varinfo->rel == temp->left_rel) {
                /* only use left attnums for group by clauses */
                temp->left_attnums = bms_add_member(temp->left_attnums, var->varattno);
                temp->clause_group = lappend(temp->clause_group, varinfo);
                add_clause_map(temp, var->varattno, 0, (Node*)var, NULL);
                found_match = true;
                break;
            }
        }

        /* if not matched, build a new cell in es_candidate_list */
        if (!found_match) {
            es_candidate* es = (es_candidate*)palloc(sizeof(es_candidate));
            RelOptInfo* temp_rel = NULL;

            init_candidate(es);

            es->tag = ES_GROUPBY;
            es->left_rel = varinfo->rel;
            es->left_relids = bms_copy(varinfo->rel->relids);
            es->left_attnums = bms_add_member(es->left_attnums, var->varattno);
            add_clause_map(es, var->varattno, 0, (Node*)var, NULL);
            read_rel_rte(varinfo->var, &temp_rel, &es->left_rte);
            Assert(es->left_rel == temp_rel);
            es->clause_group = lappend(es->clause_group, varinfo);
            es_candidate_list = lappend(es_candidate_list, es);
        }
    }

    recheck_candidate_list();
    debug_print();

    return;
}

/*
 * @brief	 	initial es_candidate, set all elements to default value or NULL
 */
void ES_SELECTIVITY::init_candidate(es_candidate* es) const
{
    es->tag = ES_EMPTY;
    es->relids = NULL;
    es->left_relids = NULL;
    es->right_relids = NULL;
    es->left_attnums = NULL;
    es->right_attnums = NULL;
    es->left_stadistinct = 0.0;
    es->right_stadistinct = 0.0;
    es->left_first_mcvfreq = 0.0;
    es->right_first_mcvfreq = 0.0;
    es->left_rel = NULL;
    es->right_rel = NULL;
    es->left_rte = NULL;
    es->right_rte = NULL;
    es->clause_group = NIL;
    es->clause_map = NIL;
    es->left_extended_stats = NULL;
    es->right_extended_stats = NULL;
    es->pseudo_clause_list = NIL;
    es->has_null_clause = false;
    return;
}

/*
 * @brief	 	free memory used in calculate_selectivity except unmatched_clause_group
 */
void ES_SELECTIVITY::clear()
{
    /* delete es_candidate_list */
    ListCell* l = NULL;
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        bms_free_ext(temp->relids);
        bms_free_ext(temp->left_relids);
        bms_free_ext(temp->right_relids);
        bms_free_ext(temp->left_attnums);
        bms_free_ext(temp->right_attnums);
        temp->left_rel = NULL;
        temp->right_rel = NULL;
        temp->left_rte = NULL;
        temp->right_rte = NULL;
        list_free_ext(temp->clause_group);
        list_free_deep(temp->clause_map);
        clear_extended_stats(temp->left_extended_stats);
        clear_extended_stats(temp->right_extended_stats);
        list_free_ext(temp->pseudo_clause_list);
    }
    list_free_deep(es_candidate_list);

    /*
     * unmatched_clause_group need to be free manually after
     * it is used in clause_selectivity().
     */
    root = NULL;
    sjinfo = NULL;
    origin_clauses = NULL;
    return;
}

/*
 * @brief    	free memory used by saving extended_stats after calculation
 */
void ES_SELECTIVITY::clear_extended_stats(ExtendedStats* extended_stats) const
{
    if (extended_stats) {
        bms_free_ext(extended_stats->bms_attnum);
        if (extended_stats->mcv_numbers)
            pfree_ext(extended_stats->mcv_numbers);
        if (extended_stats->mcv_values)
            pfree_ext(extended_stats->mcv_values);
        if (extended_stats->mcv_nulls)
            pfree_ext(extended_stats->mcv_nulls);
        if (extended_stats->other_mcv_numbers)
            pfree_ext(extended_stats->other_mcv_numbers);
        pfree_ext(extended_stats);
        extended_stats = NULL;
    }
    return;
}

/*
 * @brief    	free memory of extended_stats_list by calling clear_extended_stats
 */
void ES_SELECTIVITY::clear_extended_stats_list(List* stats_list) const
{
    if (stats_list) {
        ListCell* lc = NULL;
        foreach (lc, stats_list) {
            ExtendedStats* extended_stats = (ExtendedStats*)lfirst(lc);
            clear_extended_stats(extended_stats);
        }
        list_free_ext(stats_list);
    }
    return;
}

/*
 * @brief    	copy the original pointer, repoint it to something else
 * 			in order to avoid failure when using list_free
 * @param     	ListCell* l
 * @return
 * @exception   None
 */
ExtendedStats* ES_SELECTIVITY::copy_stats_ptr(ListCell* l) const
{
    ExtendedStats* result = (ExtendedStats*)lfirst(l);
    lfirst(l) = NULL;
    return result;
}

/*
 * @brief   	add an eqjsel clause to es_candidate_list and group by relid
 *			we should have bms_num_members(clause->clause_relids) == 1
 */
void ES_SELECTIVITY::load_eqsel_clause(RestrictInfo* clause)
{
    /* group clause by rels, add to es_candidate_list */
    ListCell* l = NULL;
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);

        if (temp->tag != ES_EQSEL)
            continue;

        if (bms_equal(clause->clause_relids, temp->relids)) {
            if (add_attnum(clause, temp)) {
                temp->clause_group = lappend(temp->clause_group, clause);
                if (IsA(clause->clause, NullTest))
                    temp->has_null_clause = true;
                return;
            }
        }
    }

    /* if not matched, build a new cell in es_candidate_list */
    if (!build_es_candidate(clause, ES_EQSEL))
        unmatched_clause_group = lappend(unmatched_clause_group, clause);

    return;
}

/*
 * @brief    	add an eqjoinsel clause to es_candidate_list and group by relid
 */
void ES_SELECTIVITY::load_eqjoinsel_clause(RestrictInfo* clause)
{
    /*
     * the relids in the clause should be as same as sjinfo, so we can avoid parameterized conditon.
     */
    if (sjinfo) {
        if (!bms_overlap(sjinfo->min_lefthand, clause->clause_relids) ||
            !bms_overlap(sjinfo->min_righthand, clause->clause_relids)) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            return;
        }
    }

    /* group clause by rels, add to es_candidate_list */
    if (bms_num_members(clause->left_relids) == 1 && bms_num_members(clause->right_relids) == 1) {
        ListCell* l = NULL;
        foreach (l, es_candidate_list) {
            es_candidate* temp = (es_candidate*)lfirst(l);

            if (temp->tag != ES_EQJOINSEL)
                continue;

            if (bms_equal(clause->clause_relids, temp->relids)) {
                if (add_attnum(clause, temp)) {
                    temp->clause_group = lappend(temp->clause_group, clause);
                    return;
                }
            }
        }

        /* if not matched, build a new cell in es_candidate_list */
        if (!build_es_candidate(clause, ES_EQJOINSEL))
            unmatched_clause_group = lappend(unmatched_clause_group, clause);

        return;
    }

    unmatched_clause_group = lappend(unmatched_clause_group, clause);

    return;
}

/*
 * @brief       make a combination of es->right_attnums or es->left_attnums with input attnum by clause map
 * @param    left: true:  add to es->right_attnums; false: add to es->left_attnums
 * @return     combination of Bitmapset
 * @exception   None
 */
Bitmapset* ES_SELECTIVITY::make_attnums_by_clause_map(es_candidate* es, Bitmapset* attnums, bool left) const
{
    ListCell* lc_clause_map = NULL;
    Bitmapset* result = NULL;
    foreach (lc_clause_map, es->clause_map) {
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc_clause_map);
        if (left && bms_is_member(clause_map->left_attnum, attnums))
            result = bms_add_member(result, clause_map->right_attnum);
        else if (!left && bms_is_member(clause_map->right_attnum, attnums))
            result = bms_add_member(result, clause_map->left_attnum);
    }
    return result;
}

/*
 * @brief       find the matched extended stats in stats_list
 * @param    es :proving mathing conditions including relids , attnums
 * @param    stats_list : the extended statistic list
 * @param    left : true : match with the left_rel; false: match with the right_rel
 * @return    None
 * @exception   None
 */
void ES_SELECTIVITY::match_extended_stats(es_candidate* es, List* stats_list, bool left)
{
    int max_matched = 0;
    int num_members = bms_num_members(es->left_attnums);
    char other_side_starelkind;
    RangeTblEntry* other_side_rte = NULL;
    Bitmapset* this_side_attnums = NULL;
    if (left) {
        /* this side is left and the other side is right */
        other_side_starelkind = OidIsValid(es->right_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
        other_side_rte = es->right_rte;
        this_side_attnums = es->left_attnums;
    } else {
        /* this side is right and other side is left */
        other_side_starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
        other_side_rte = es->left_rte;
        this_side_attnums = es->right_attnums;
    }

    /* best_matched_listcell use to save the best match from stats list */
    ListCell* best_matched_listcell = NULL;
    /* best_matched_stats use to save the best match from es_get_multi_column_stats */
    ListCell* best_matched_stats = (ListCell*)palloc(sizeof(ListCell));
    lfirst(best_matched_stats) = NULL;
    ListCell* lc = NULL;
    foreach (lc, stats_list) {
        ExtendedStats* extended_stats = (ExtendedStats*)lfirst(lc);
        ExtendedStats* other_side_extended_stats = NULL;
        if (bms_is_subset(extended_stats->bms_attnum, this_side_attnums)) {
            int matched = bms_num_members(extended_stats->bms_attnum);

            Bitmapset* other_side_attnums = make_attnums_by_clause_map(es, extended_stats->bms_attnum, left);
            other_side_extended_stats = es_get_multi_column_stats(
                other_side_rte->relid, other_side_starelkind, other_side_rte->inh, other_side_attnums);
            if (other_side_extended_stats != NULL && matched == num_members) {
                /* all attnums have extended stats, leave */
                if (left) {
                    es->left_extended_stats = copy_stats_ptr(lc);
                    es->right_extended_stats = other_side_extended_stats;
                } else {
                    es->right_extended_stats = copy_stats_ptr(lc);
                    es->left_extended_stats = other_side_extended_stats;
                }
                clear_extended_stats((ExtendedStats*)lfirst(best_matched_stats));
                break;
            } else if (other_side_extended_stats != NULL && matched > max_matched) {
                /* not all attnums have extended stats, find the first maximum match */
                best_matched_listcell = lc;
                clear_extended_stats((ExtendedStats*)lfirst(best_matched_stats));
                lfirst(best_matched_stats) = other_side_extended_stats;
                max_matched = matched;
            } else
                clear_extended_stats(other_side_extended_stats);
        }
    }

    if (best_matched_listcell && lfirst(best_matched_stats)) {
        if (left) {
            es->left_extended_stats = copy_stats_ptr(best_matched_listcell);
            es->right_extended_stats = (ExtendedStats*)lfirst(best_matched_stats);
        } else {
            es->right_extended_stats = copy_stats_ptr(best_matched_listcell);
            es->left_extended_stats = (ExtendedStats*)lfirst(best_matched_stats);
        }
        lfirst(best_matched_stats) = NULL;
        /* remove members not in the multi-column stats */
        if (max_matched != num_members) {
            Bitmapset* tmpset = bms_difference(es->left_attnums, es->left_extended_stats->bms_attnum);
            int dump_attnum;
            while ((dump_attnum = bms_first_member(tmpset)) > 0) {
                es->left_attnums = bms_del_member(es->left_attnums, dump_attnum);
                remove_attnum(es, dump_attnum);
            }
            bms_free_ext(tmpset);
        }
    }
    pfree_ext(best_matched_stats);
    return;
}

/*
 * @brief       modify distinct value using possion model
 * @param    es : proving the distinct value to modify
 * @param    left  :true : modify the left distinct value; false: modify the right one
 * @param    sjinfo :join infos from inputs of calculate_selecitvity, can be NULL for eqsel
 * @return	None
 * @exception   None
 */
void ES_SELECTIVITY::modify_distinct_by_possion_model(es_candidate* es, bool left, SpecialJoinInfo* spjinfo) const
{
    bool enablePossion = false;
    double varratio = 1.0;
    ListCell* lc = NULL;
    VariableStatData vardata;
    float4 distinct = 0.0;
    double tuples = 0.0;

    /* build vardata */
    vardata.enablePossion = true;
    if (left && es->left_rel->tuples > 0) {
        vardata.rel = es->left_rel;
        distinct = es->left_stadistinct;
        tuples = es->left_rel->tuples;
        foreach (lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->left_var;
            enablePossion = can_use_possion(&vardata, spjinfo, &varratio);
            if (!enablePossion)
                break;
        }
    } else if (!left && es->right_rel->tuples > 0) {
        vardata.rel = es->right_rel;
        distinct = es->right_stadistinct;
        tuples = es->right_rel->tuples;
        foreach (lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->right_var;
            enablePossion = can_use_possion(&vardata, spjinfo, &varratio);
            if (!enablePossion)
                break;
        }
    }

    if (enablePossion) {
        double tmp = distinct;
        distinct = NUM_DISTINCT_SELECTIVITY_FOR_POISSON(distinct, tuples, varratio);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]The origin distinct value is %f. After using possion model with ntuples=%f and ration=%e \
					The new distinct value is %f",
                    tmp,
                    tuples,
                    varratio,
                    distinct))));
    }

    if (left && enablePossion) {
        es->left_stadistinct = distinct;
    } else if ((!left) && enablePossion) {
        es->right_stadistinct = distinct;
    }
    return;
}

/*
 * @brief       build a new es_candidate and add to es_candidate_list
 */
bool ES_SELECTIVITY::build_es_candidate(RestrictInfo* clause, es_type type)
{
    Node* left = NULL;
    Node* right = NULL;

    int left_attnum = 0;
    int right_attnum = 0;

    if (IsA(clause->clause, OpExpr)) {
        OpExpr* opclause = (OpExpr*)clause->clause;

        Assert(list_length(opclause->args) == 2);

        left = (Node*)linitial(opclause->args);
        right = (Node*)lsecond(opclause->args);

        left_attnum = read_attnum(left);
        right_attnum = read_attnum(right);

        if (left_attnum < 0 || right_attnum < 0)
            return false;

        /* check clause type */
        switch (type) {
            case ES_EQSEL:
                if (!IsA(left, Const) && !IsA(right, Const))
                    return false;
                else if (IsA(left, Const) && ((Const*)left)->constisnull)
                    return false;
                else if (IsA(right, Const) && ((Const*)right)->constisnull)
                    return false;
                break;
            case ES_EQJOINSEL:
                break;
            default:
                break;
        }
    } else {
        Assert(IsA(clause->clause, NullTest));
        NullTest* nullclause = (NullTest*)clause->clause;

        left = (Node*)nullclause->arg;
        left_attnum = read_attnum(left);

        if (left_attnum < 0)
            return false;
    }

    es_candidate* es = (es_candidate*)palloc(sizeof(es_candidate));
    init_candidate(es);

    switch (type) {
        case ES_EQSEL:
            /* only use left side */
            if (left_attnum > 0 && right_attnum == 0) {
                read_rel_rte(left, &es->left_rel, &es->left_rte);
                if (es->left_rte == NULL || es->left_rte->rtekind != RTE_RELATION) {
                    break;
                }
                es->left_relids =
                    clause->left_relids != NULL ? bms_copy(clause->left_relids) : bms_copy(clause->clause_relids);
                es->left_attnums = bms_add_member(es->left_attnums, left_attnum);
                add_clause_map(es, left_attnum, 0, left, NULL);
            } else if (right_attnum > 0 && left_attnum == 0) {
                Assert(clause->right_relids != NULL);
                read_rel_rte(right, &es->left_rel, &es->left_rte);
                if (es->left_rte == NULL || es->left_rte->rtekind != RTE_RELATION) {
                    break;
                }
                es->left_relids = bms_copy(clause->right_relids);
                es->left_attnums = bms_add_member(es->left_attnums, right_attnum);
                add_clause_map(es, right_attnum, 0, right, NULL);
            } else {
                pfree_ext(es);
                return false;
            }
            break;
        case ES_EQJOINSEL:
            if (left_attnum > 0 && right_attnum > 0) {
                read_rel_rte(left, &es->left_rel, &es->left_rte);
                read_rel_rte(right, &es->right_rel, &es->right_rte);
                if (es->left_rte == NULL || es->left_rte->rtekind != RTE_RELATION) {
                    break;
                }
                if (es->right_rte == NULL || es->right_rte->rtekind != RTE_RELATION) {
                    break;
                }
                es->left_relids = bms_copy(clause->left_relids);
                es->right_relids = bms_copy(clause->right_relids);
                es->left_attnums = bms_add_member(es->left_attnums, left_attnum);
                es->right_attnums = bms_add_member(es->right_attnums, right_attnum);
                add_clause_map(es, left_attnum, right_attnum, left, right);
            } else {
                pfree_ext(es);
                return false;
            }
            break;
        default:
            /* for future development, should not reach here now */
            pfree_ext(es);
            return false;
    }

    /* double check */
    if ((es->left_rte && es->left_rte->rtekind != RTE_RELATION) ||
        (es->right_rte && es->right_rte->rtekind != RTE_RELATION)) {
        es->left_rel = NULL;
        es->right_rel = NULL;
        es->left_rte = NULL;
        es->right_rte = NULL;
        pfree_ext(es);
        return false;
    }

    es->tag = type;
    es->relids = bms_copy(clause->clause_relids);
    es->clause_group = lappend(es->clause_group, clause);
    es->has_null_clause = IsA(clause->clause, NullTest);
    es_candidate_list = lappend(es_candidate_list, es);
    es->left_first_mcvfreq = 0.0;
    es->right_first_mcvfreq = 0.0;

    return true;
}

/*
 * @brief        remove useless member in es_candidate_list to unmatched_clause_group
 */
void ES_SELECTIVITY::recheck_candidate_list()
{
    if (!es_candidate_list)
        return;
    ListCell* l = NULL;
    bool validate = true;

    /* try to use equivalence_class to re-combinate clauses first */
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        if (temp->tag == ES_EQJOINSEL && list_length(temp->clause_group) == 1 && list_length(es_candidate_list) > 1)
            (void)try_equivalence_class(temp);
    }

    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
                if (list_length(temp->clause_group) <= 1)
                    validate = false;
                else if (temp->left_rte && bms_num_members(temp->left_attnums) <= 1)
                    validate = false;
                break;
            case ES_EQJOINSEL:
                if (list_length(temp->clause_group) <= 1)
                    validate = false;
                else if (bms_num_members(temp->left_attnums) <= 1 || bms_num_members(temp->right_attnums) <= 1)
                    validate = false;
                break;
            case ES_GROUPBY:
                if (bms_num_members(temp->left_attnums) <= 1)
                    validate = false;
                break;
            default:
                break;
        }
        if (!validate) {
            unmatched_clause_group = list_concat(unmatched_clause_group, temp->clause_group);
            temp->tag = ES_EMPTY;
            temp->clause_group = NULL;
        }
    }
    return;
}

/*
 * @brief       try to find a substitude clause building from equivalence classes
 * @return    true when find a substitude clause; false when find nothing
 */
bool ES_SELECTIVITY::try_equivalence_class(es_candidate* es)
{
    if (path && path->path.pathtype == T_MergeJoin) {
        /* for mergejoin, do not adjust clause using equivalence class */
        return false;
    }

    ListCell* lc = NULL;
    bool result = false;

    foreach (lc, root->eq_classes) {
        EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);

        /* only consider var = var situation */
        if (ec->ec_has_const)
            continue;

        /* ignore broken ecs */
        if (ec->ec_broken)
            continue;

        /* if members of ECs are less than two, won't generate any substitute */
        if (list_length(ec->ec_members) <= 2)
            continue;

        /* We can quickly ignore any that don't cover the join, too */
        if (!bms_is_subset(es->relids, ec->ec_relids))
            continue;

        Bitmapset* tmpset = bms_copy(ec->ec_relids);
        int ec_relid = 0;
        while ((ec_relid = bms_first_member(tmpset)) >= 0) {
            if (bms_is_member(ec_relid, es->relids))
                continue;
            ListCell* lc2 = NULL;
            foreach (lc2, es_candidate_list) {
                es_candidate* temp = (es_candidate*)lfirst(lc2);
                if (temp->tag != ES_EQJOINSEL)
                    continue;
                if (bms_equal(temp->relids, es->relids))
                    continue;
                if (bms_is_member(ec_relid, temp->relids) && bms_overlap(temp->relids, es->relids)) {
                    Bitmapset* interset_relids = bms_intersect(temp->relids, es->relids);
                    Bitmapset* join_relids = bms_copy(interset_relids);
                    join_relids = bms_add_member(join_relids, ec_relid);
                    Assert(bms_equal(join_relids, temp->relids));
                    Bitmapset* outer_relids = bms_make_singleton(ec_relid);
                    List* pseudo_clauselist =
                        generate_join_implied_equalities_normal(root, ec, join_relids, outer_relids, interset_relids);
                    if (pseudo_clauselist != NULL) {
                        result = match_pseudo_clauselist(pseudo_clauselist, temp, es->clause_group);
                        if (log_min_messages <= ES_DEBUG_LEVEL) {
                            ereport(ES_DEBUG_LEVEL,
                                (errmodule(MOD_OPT_JOIN), errmsg("[ES]Build new clause using equivalence class:)")));
                            print_clauses(pseudo_clauselist);
                            ereport(ES_DEBUG_LEVEL,
                                (errmodule(MOD_OPT_JOIN),
                                    errmsg("[ES]The old clause will be abandoned? %d)", (int)result)));
                            print_clauses(es->clause_group);
                        }
                    }

                    bms_free_ext(interset_relids);
                    bms_free_ext(join_relids);
                    bms_free_ext(outer_relids);
                    if (result) {
                        /* replace the removed clause in clauselist with the new built one */
                        ListCell* lc3 = NULL;
                        foreach (lc3, origin_clauses) {
                            void* clause = (void*)lfirst(lc3);
                            if (clause == linitial(es->clause_group)) {
                                /* maybe cause memory problem as the old clause is not released */
                                lfirst(lc3) = linitial(pseudo_clauselist);
                            }
                        }
                        /* For hashclause, we have to process joinrestrictinfo in path as well */
                        if (path) {
                            foreach (lc3, path->joinrestrictinfo) {
                                void* clause = (void*)lfirst(lc3);
                                if (clause == linitial(es->clause_group)) {
                                    /* maybe cause memory problem as the old clause is not released */
                                    lfirst(lc3) = linitial(pseudo_clauselist);
                                }
                            }
                        }
                        break;
                    }
                }
            }
            if (result) {
                /*
                 * If sucess, the clause has been tranformed and saved in another es_candidate.
                 * So no need to keep this es_candidate anymore.
                 */
                es->tag = ES_EMPTY;
                es->clause_group = NULL;
                break;
            }
        }
        bms_free_ext(tmpset);
        if (result)
            break;
    }
    return result;
}

/*
 * @brief       try to match the newborn clause building by try_equivalence_class() with the existed clause group
 *		    like what we do in group_clauselist(), but more simple.
 */
bool ES_SELECTIVITY::match_pseudo_clauselist(List* clauses, es_candidate* es, List* origin_clause)
{
    bool result = false;
    ListCell* lc = NULL;
    foreach (lc, clauses) {
        Node* clause = (Node*)lfirst(lc);

        if (!IsA(clause, RestrictInfo)) {
            continue;
        }

        RestrictInfo* rinfo = (RestrictInfo*)clause;
        if (rinfo->pseudoconstant || rinfo->norm_selec > 1 || rinfo->orclause) {
            continue;
        }

        if (is_opclause(rinfo->clause)) {
            OpExpr* opclause = (OpExpr*)rinfo->clause;
            Oid opno = opclause->opno;

            /* only handle "=" operator */
            if (get_oprrest(opno) == EQSELRETURNOID) {
                Assert(bms_num_members(rinfo->clause_relids) == 2);
                if (add_attnum(rinfo, es)) {
                    es->clause_group = lappend(es->clause_group, clause);
                    es->pseudo_clause_list = lappend(es->pseudo_clause_list, clause);
                    es->pseudo_clause_list = list_concat(es->pseudo_clause_list, origin_clause);
                    result = true;
                }
            }
        }
    }
    return result;
}

/*
 * @brief       relpace the original clause in the input clause list with the new clause build by equivalence class,
 *		     the memory used by old clause will be release by optimizer context or something esle
 */
void ES_SELECTIVITY::replace_clause(Datum* old_clause, Datum* new_clause) const
{
    ListCell* lc = NULL;
    foreach (lc, origin_clauses) {
        if (lfirst(lc) == old_clause) {
            lfirst(lc) = new_clause;
            break;
        }
    }
}

/*
 * @brief        main entry to read statistic which will be used to calculate selectivity, called by
 * calculate_selectivity
 */
void ES_SELECTIVITY::read_statistic()
{
    if (!es_candidate_list)
        return;
    ListCell* l = NULL;
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
            case ES_GROUPBY:
                read_statistic_eqsel(temp);
                break;
            case ES_EQJOINSEL:
                read_statistic_eqjoinsel(temp);
                break;
            default:
                /* empty */
                break;
        }
    }
    return;
}

/*
 * @brief       read extended statistic for eqjoinsel
 *			There are three possible situations:
 *			(1) No statistic data in pg_statistic, then num_stats will be 0
 *			(2) There are over 100 records in pg_statistic when search extended statistics for target table,
 *			      then the returned stats_list will be empty and we have to search manually. In this case,
 *			      we have too many combinations to try so that we will make some compromise and only
 *			      try limited possibilities.
 *			(3) There are less than 100 records and the returned stats_list will be all records. Then we will
 *			      search the list for the best answer.
 */
void ES_SELECTIVITY::read_statistic_eqjoinsel(es_candidate* es)
{
    int left_num_stats = 0;
    int right_num_stats = 0;
    char left_starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
    char right_starelkind = OidIsValid(es->right_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;

    /* read all multi-column statistic from pg_statistic if possible */
    List* left_stats_list =
        es_get_multi_column_stats(es->left_rte->relid, left_starelkind, es->left_rte->inh, &left_num_stats);
    List* right_stats_list =
        es_get_multi_column_stats(es->right_rte->relid, right_starelkind, es->right_rte->inh, &right_num_stats);

    /* no multi-column statistic */
    if (left_num_stats == 0 || right_num_stats == 0) {
        report_no_stats(es->left_rte->relid, es->left_attnums);
        report_no_stats(es->right_rte->relid, es->right_attnums);
        remove_candidate(es);
        return;
    }

    /* save attnums for no analyze list */
    Bitmapset* tmp_left = bms_copy(es->left_attnums);
    Bitmapset* tmp_right = bms_copy(es->right_attnums);

    /* when at least one side return with multi-column statistic list */
    if (left_num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE && left_num_stats <= right_num_stats) {
        match_extended_stats(es, left_stats_list, true);
    } else if (right_num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE) {
        match_extended_stats(es, right_stats_list, false);
    } else {
        /*
         * There are too many multi-column statistic, so return null list.
         * We have to search pg_statistic manually with limited combinations.
         * So could lose some matches.
         */
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT_JOIN), errmsg("[ES] Too many multi-column statistic, could lose matches.")));

        while (bms_num_members(es->left_attnums) >= 2) {
            ExtendedStats* left_extended_stats =
                es_get_multi_column_stats(es->left_rte->relid, left_starelkind, es->left_rte->inh, es->left_attnums);
            if (left_extended_stats != NULL) {
                ExtendedStats* right_extended_stats = es_get_multi_column_stats(
                    es->right_rte->relid, right_starelkind, es->right_rte->inh, es->right_attnums);
                if (right_extended_stats != NULL) {
                    es->left_extended_stats = left_extended_stats;
                    es->right_extended_stats = right_extended_stats;
                    break;
                } else
                    clear_extended_stats(left_extended_stats);
            }

            /* delete the first member of attnums and use the rest */
            int dump_attnum = bms_first_member(es->left_attnums);

            if (bms_num_members(es->left_attnums) < 2) {
                /* no need to search any more as we don't have enough attnums */
                remove_candidate(es);
                break;
            }

            /* delete clause and clause_map according to dump */
            remove_attnum(es, dump_attnum);
        }
    }

    /*
     * Since we can not tell how many or which columns are actaully null when nullfrac == 1.0,
     * so we will not use multi-column when nullfrac == 1.0.
     */
    if (es->left_extended_stats &&
        (es->left_extended_stats->nullfrac != 0.0 || es->left_extended_stats->distinct != 0.0 ||
            es->left_extended_stats->mcv_values) &&
        es->right_extended_stats &&
        (es->right_extended_stats->nullfrac != 0.0 || es->right_extended_stats->distinct != 0.0 ||
            es->right_extended_stats->mcv_values)) {
        /* should not recieve empty extended stas here, except all columns are empty */
        if (es->left_extended_stats->distinct < 0)
            es->left_stadistinct = clamp_row_est(-1 * es->left_extended_stats->distinct * es->left_rel->tuples *
                                                 (1.0 - es->left_extended_stats->nullfrac));
        else
            es->left_stadistinct = clamp_row_est(es->left_extended_stats->distinct);

        if (es->right_extended_stats->distinct < 0)
            es->right_stadistinct = clamp_row_est(-1 * es->right_extended_stats->distinct * es->right_rel->tuples *
                                                  (1.0 - es->right_extended_stats->nullfrac));
        else
            es->right_stadistinct = clamp_row_est(es->right_extended_stats->distinct);

        /*
         * Use possion model if satisify condition.
         */
        modify_distinct_by_possion_model(es, true, sjinfo);
        modify_distinct_by_possion_model(es, false, sjinfo);

        /* replace the old clause with the new one */
        if (es->pseudo_clause_list) {
            ListCell* lc2 = NULL;
            foreach (lc2, es->pseudo_clause_list) {
                Datum* new_clause = (Datum*)lfirst(lc2);
                lc2 = lnext(lc2);
                Datum* old_clause = (Datum*)lfirst(lc2);
                /* make sure the new clause is still in the clause group */
                ListCell* lc3 = NULL;
                foreach (lc3, es->clause_group) {
                    if ((Datum*)lfirst(lc3) == new_clause) {
                        replace_clause(old_clause, new_clause);
                        break;
                    }
                }
            }
        }
    } else {
        /* no multi-column statistic matched */
        report_no_stats(es->left_rte->relid, tmp_left);
        report_no_stats(es->right_rte->relid, tmp_right);
        remove_candidate(es);
    }

    bms_free_ext(tmp_left);
    bms_free_ext(tmp_right);
    clear_extended_stats_list(left_stats_list);
    clear_extended_stats_list(right_stats_list);
    return;
}

/*
 * @brief       read extended statistic for eqsel or groupby, details are as same as read_statistic_eqjoinsel
 */
void ES_SELECTIVITY::read_statistic_eqsel(es_candidate* es)
{
    int num_stats = 0;
    char starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
    /* read all multi-column statistic from pg_statistic if possible */
    List* stats_list =
        es_get_multi_column_stats(es->left_rte->relid, starelkind, es->left_rte->inh, &num_stats, es->has_null_clause);
    /* no multi-column statistic */
    if (num_stats == 0) {
        report_no_stats(es->left_rte->relid, es->left_attnums);
        remove_candidate(es);
        return;
    }

    /* return with multi-column statistic list */
    if (num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE) {
        ListCell* l = NULL;
        int max_matched = 0;
        int num_members = bms_num_members(es->left_attnums);
        ListCell* best_matched_stat_ptr = NULL;
        foreach (l, stats_list) {
            ExtendedStats* extended_stats = (ExtendedStats*)lfirst(l);
            if (bms_is_subset(extended_stats->bms_attnum, es->left_attnums)) {
                int matched = bms_num_members(extended_stats->bms_attnum);
                if (matched == num_members) {
                    es->left_extended_stats = copy_stats_ptr(l);
                    break;
                } else if (matched > max_matched) {
                    best_matched_stat_ptr = l;
                    max_matched = matched;
                }
            }
        }

        if (best_matched_stat_ptr != NULL) {
            es->left_extended_stats = copy_stats_ptr(best_matched_stat_ptr);
            /* remove members not in the multi-column stats */
            if (max_matched != num_members) {
                Bitmapset* tmpset = bms_difference(es->left_attnums, es->left_extended_stats->bms_attnum);
                int dump_attnum;
                while ((dump_attnum = bms_first_member(tmpset)) > 0) {
                    es->left_attnums = bms_del_member(es->left_attnums, dump_attnum);
                    remove_attnum(es, dump_attnum);
                }
                bms_free_ext(tmpset);
            }
        }

    } else {
        /*
         * There are too many multi-column statistic, so return null list.
         * We have to search pg_statistic manually with limited combinations.
         * So could lose some matches.
         */
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT_JOIN), errmsg("[ES] Too many multi-column statistic, could lose matches.")));

        while (bms_num_members(es->left_attnums) >= 2) {
            ExtendedStats* extended_stats = es_get_multi_column_stats(
                es->left_rte->relid, starelkind, es->left_rte->inh, es->left_attnums, es->has_null_clause);
            if (extended_stats != NULL) {
                es->left_extended_stats = extended_stats;
                break;
            }

            /* delete the first member of attnums and use the rest */
            int dump_attnum = bms_first_member(es->left_attnums);

            if (bms_num_members(es->left_attnums) < 2) {
                /* no need to search any more as we don't have enough attnums */
                remove_candidate(es);
                break;
            }

            /* delete clause and clause_map according to dump */
            remove_attnum(es, dump_attnum);
        }
    }

    /*
     * Since we can not tell how many or which columns are actaully null when nullfrac == 1.0,
     * so we will not use multi-column when nullfrac == 1.0.
     */
    if (es->left_extended_stats &&
        (es->left_extended_stats->nullfrac != 0.0 || es->left_extended_stats->distinct != 0.0 ||
            es->left_extended_stats->mcv_values)) {
        /* should not recieve empty extended stas here, except all columns are empty */
        if (es->left_extended_stats->distinct < 0)
            es->left_stadistinct = clamp_row_est(-1 * es->left_extended_stats->distinct * es->left_rel->tuples);
        else
            es->left_stadistinct = clamp_row_est(es->left_extended_stats->distinct);

        /*
         * Use possion model if satisify condition.
         * we don't need possion to estimate distinct because
         * we can't estimate accurate for multiple exprs.
         */
        if (es->tag != ES_GROUPBY)
            modify_distinct_by_possion_model(es, true, NULL);
    } else {
        /* no multi-column statistic matched */
        remove_candidate(es);
    }

    clear_extended_stats_list(stats_list);

    return;
}

/*
 * @brief	 	read attnum from input,
 * @param	node: should be a Var*
 * @return	return -1 if varattno <= 0, return 0 means node is not a var
 * @exception	None
 */
int ES_SELECTIVITY::read_attnum(Node* node) const
{
    Node* basenode = NULL;
    int attnum = -1;
    if (IsA(node, RelabelType))
        basenode = (Node*)((RelabelType*)node)->arg;
    else
        basenode = node;

    if (IsA(basenode, Var)) {
        Var* var = (Var*)basenode;
        attnum = var->varattno;
        if (attnum <= 0)
            attnum = -1;
    } else if (IsA(node, Const))
        attnum = 0;

    return attnum;
}

/*
 * @brief       save non-analyze multi-column to g_NoAnalyzeRelNameList
 * @param    relid_oid, relation oid
 * @param    attnums, multi-column attribute number
 * @return
 * @exception   None
 */
void ES_SELECTIVITY::report_no_stats(Oid relid_oid, Bitmapset* attnums) const
{
    /* We don't save non-analyze multi-column to g_NoAnalyzeRelNameList when resource_track_log=summary. */
    if (u_sess->attr.attr_storage.resource_track_log == SUMMARY)
        return;

    if (relid_oid == 0)
        return;

    /*
     * We should not save the relation to non-analyze list if is under analyzing,
     * because it will create temp table and execute some query, the temp table
     * don't be analyzed when 2% analyzing.
     */
    if (u_sess->analyze_cxt.is_under_analyze) {
        return;
    }

    Assert(bms_num_members(attnums) >= 2);
    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    Bitmapset* attnums_tmp = bms_copy(attnums);

    ListCell* lc = NULL;
    bool found = false;
    if (t_thrd.postgres_cxt.g_NoAnalyzeRelNameList != NIL) {
        foreach (lc, t_thrd.postgres_cxt.g_NoAnalyzeRelNameList) {
            List* record = (List*)lfirst(lc);
            if (relid_oid == linitial_oid((List*)linitial(record))) {
                ListCell* sublist = NULL;
                for (sublist = lnext(list_head(record)); sublist != NULL; sublist = lnext(sublist)) {
                    List* attid_list = (List*)lfirst(sublist);
                    if (list_length(attid_list) == bms_num_members(attnums_tmp)) {
                        Bitmapset* attnums_from_list = NULL;
                        ListCell* cell = attid_list->head;
                        while (cell != NULL) {
                            attnums_from_list = bms_add_member(attnums_from_list, (int)lfirst_int(cell));
                            cell = cell->next;
                        }
                        if (bms_equal(attnums_from_list, attnums_tmp))
                            found = true;
                        bms_free_ext(attnums_from_list);
                    }
                }
                if (!found) {
                    List* attid_list = NIL;
                    int attnum = bms_first_member(attnums_tmp);
                    while (attnum != -1) {
                        attid_list = lappend_int(attid_list, attnum);
                        attnum = bms_first_member(attnums_tmp);
                    }
                    bms_free_ext(attnums_tmp);
                    record = lappend(record, attid_list);
                    found = true;
                }
            }
        }
    }

    if (!found) {
        /* add a new rel list */
        List* record = NIL;
        List* relid_list = NIL;
        List* attid_list = NIL;
        int attnum = bms_first_member(attnums_tmp);
        while (attnum != -1) {
            attid_list = lappend_int(attid_list, attnum);
            attnum = bms_first_member(attnums_tmp);
        }
        bms_free_ext(attnums_tmp);
        relid_list = lappend_oid(relid_list, relid_oid);
        record = lappend(record, relid_list);
        record = lappend(record, attid_list);

        /* Add a new rel list into g_NoAnalyzeRelNameList. */
        t_thrd.postgres_cxt.g_NoAnalyzeRelNameList = lappend(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList, record);
    }

    (void)MemoryContextSwitchTo(oldcontext);
    return;
}

/*
 * @brief	 	read and add attnums to es_candidate
 * @return	true if success
 */
bool ES_SELECTIVITY::add_attnum(RestrictInfo* clause, es_candidate* temp) const
{
    Node* left = NULL;
    Node* right = NULL;
    int left_attnum = 0;
    int right_attnum = 0;

    if (IsA(clause->clause, OpExpr)) {
        OpExpr* opclause = (OpExpr*)clause->clause;
        Assert(list_length(opclause->args) == 2);
        left = (Node*)linitial(opclause->args);
        right = (Node*)lsecond(opclause->args);
        left_attnum = read_attnum(left);
        right_attnum = read_attnum(right);
    } else {
        Assert(IsA(clause->clause, NullTest));
        left = (Node*)((NullTest*)clause->clause)->arg;
        left_attnum = read_attnum(left);
    }

    if (left_attnum < 0 || right_attnum < 0 || (left_attnum == 0 && right_attnum == 0))
        return false;

    switch (temp->tag) {
        case ES_EQSEL:
            /*
             * We wouldn't have clauses like: t1.a = 1 and t1.a = 2 here
             * because optimizor will find the conflict first.
             */
            if (left_attnum > 0 && right_attnum > 0)
                return false;

            if (bms_equal(clause->left_relids, temp->left_relids) ||
                (IsA(clause->clause, NullTest) && bms_equal(clause->clause_relids, temp->left_relids))) {
                temp->left_attnums = bms_add_member(temp->left_attnums, left_attnum);
                add_clause_map(temp, left_attnum, 0, left, NULL);
            } else if (bms_equal(clause->right_relids, temp->left_relids)) {
                temp->left_attnums = bms_add_member(temp->left_attnums, right_attnum);
                add_clause_map(temp, right_attnum, 0, right, NULL);
            } else
                return false;

            break;
        case ES_EQJOINSEL:
            /*
             * Normally, we shouldn't have clauses like:  t1.a = t2.a and t1.b = t2.a here
             * because clause is generated from equivalence class, in this case,
             * t1.a, t2.a and t1.b is in one equivalence class and will only generate
             * one clause.
             * However, if we try to build an es_candidate using equivalence class such as tpch Q9,
             * there could be some scenarios we have not forseen now.
             * So, for safety, we still check whether the clauses is something
             * like: t1.a = t2.a and t1.b = t2.a
             */
            if (left_attnum == 0 || right_attnum == 0)
                return false;

            if (bms_equal(clause->left_relids, temp->left_relids) &&
                bms_equal(clause->right_relids, temp->right_relids) &&
                !bms_is_member(left_attnum, temp->left_attnums) && !bms_is_member(right_attnum, temp->right_attnums)) {
                temp->left_attnums = bms_add_member(temp->left_attnums, left_attnum);
                temp->right_attnums = bms_add_member(temp->right_attnums, right_attnum);
                add_clause_map(temp, left_attnum, right_attnum, left, right);
            } else if (bms_equal(clause->right_relids, temp->left_relids) &&
                       bms_equal(clause->left_relids, temp->right_relids) &&
                       !bms_is_member(right_attnum, temp->left_attnums) &&
                       !bms_is_member(left_attnum, temp->right_attnums)) {
                temp->left_attnums = bms_add_member(temp->left_attnums, right_attnum);
                temp->right_attnums = bms_add_member(temp->right_attnums, left_attnum);
                add_clause_map(temp, right_attnum, left_attnum, right, left);
            } else
                return false;

            break;
        default:
            /* should not reach here */
            return false;
    }

    return true;
}

/*
 * @brief       add clause_map to es_candidate, clause_map is a map which can be used to find the right arg
 *		    using an attnum of left arg in a clause. So we don't have to parse the clause again.
 */
void ES_SELECTIVITY::add_clause_map(
    es_candidate* es, int left_attnum, int right_attnum, Node* left_arg, Node* right_arg) const
{
    es_clause_map* clause_map = (es_clause_map*)palloc(sizeof(es_clause_map));
    clause_map->left_attnum = left_attnum;
    clause_map->right_attnum = right_attnum;
    if (left_arg) {
        if (IsA(left_arg, RelabelType))
            clause_map->left_var = (Var*)((RelabelType*)left_arg)->arg;
        else
            clause_map->left_var = (Var*)left_arg;
    }

    if (right_arg) {
        if (IsA(right_arg, RelabelType))
            clause_map->right_var = (Var*)((RelabelType*)right_arg)->arg;
        else
            clause_map->right_var = (Var*)right_arg;
    }
    es->clause_map = lappend(es->clause_map, clause_map);
    return;
}

/*
 * @brief	 	read RelOptInfo and RangeTblEntry, save to es_candidate
 * @param	node: should be a Var*
 */
void ES_SELECTIVITY::read_rel_rte(Node* node, RelOptInfo** rel, RangeTblEntry** rte)
{
    Node* basenode = NULL;

    if (IsA(node, RelabelType))
        basenode = (Node*)((RelabelType*)node)->arg;
    else
        basenode = node;

    if (IsA(basenode, Var)) {
        Assert(root != NULL);
        Assert(root->parse != NULL);

        Var* var = (Var*)basenode;
        *rel = find_base_rel(root, var->varno);
        *rte = rt_fetch((*rel)->relid, root->parse->rtable);
    }

    return;
}

void ES_SELECTIVITY::save_selectivity(
    es_candidate* es, double left_join_ratio, double right_join_ratio, bool save_semi_join)
{
    VariableStatData vardata;
    RatioType type;
    if (es->tag == ES_EQSEL)
        type = RatioType_Filter;
    else if (es->tag == ES_EQJOINSEL)
        type = RatioType_Join;
    else
        return;

    if (es->left_rel) {
        vardata.rel = es->left_rel;
        ListCell* lc = NULL;
        foreach (lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->left_var;
            if (!save_semi_join)
                set_varratio_after_calc_selectivity(&vardata, type, left_join_ratio, sjinfo);
            /* Bloom filter can be used only when var = var. */
            if (es->tag == ES_EQJOINSEL) {
                VariableStatData vardata2;
                vardata2.rel = es->right_rel;
                vardata2.var = (Node*)map->right_var;
                /* Set var's ratio which will be used by bloom filter set. */
                set_equal_varratio(&vardata, vardata2.rel->relids, left_join_ratio, sjinfo);
                if (!save_semi_join) {
                    set_equal_varratio(&vardata2, vardata.rel->relids, right_join_ratio, sjinfo);
                    set_varratio_after_calc_selectivity(&vardata2, type, right_join_ratio, sjinfo);
                }
            }
        }
    }
    return;
}

/*
 * @brief       calculate bucket size, actually only save results for estimate_hash_bucketsize to use
 */
void ES_SELECTIVITY::cal_bucket_size(es_candidate* es, es_bucketsize* bucket) const
{
    double tuples;
    RelOptInfo* rel = NULL;
    ListCell* lc = NULL;

    bucket->left_relids = bms_copy(es->left_relids);
    bucket->right_relids = bms_copy(es->right_relids);
    bucket->left_rel = es->left_rel;
    bucket->right_rel = es->right_rel;
    bucket->left_distinct = es->left_stadistinct;
    bucket->right_distinct = es->right_stadistinct;
    bucket->left_mcvfreq = es->left_first_mcvfreq;
    bucket->right_mcvfreq = es->right_first_mcvfreq;

    if (es->left_extended_stats->dndistinct > 0)
        bucket->left_dndistinct = es->left_extended_stats->dndistinct;
    else {
        rel = es->left_rel;
        tuples = get_local_rows(
            rel->tuples, rel->multiple, IsLocatorReplicated(rel->locator_type), ng_get_dest_num_data_nodes(rel));
        bucket->left_dndistinct = clamp_row_est(-1 * es->left_extended_stats->dndistinct * tuples);
    }

    if (es->right_extended_stats->dndistinct > 0)
        bucket->right_dndistinct = es->right_extended_stats->dndistinct;
    else {
        rel = es->right_rel;
        tuples = get_local_rows(
            rel->tuples, rel->multiple, IsLocatorReplicated(rel->locator_type), ng_get_dest_num_data_nodes(rel));
        bucket->right_dndistinct = clamp_row_est(-1 * es->right_extended_stats->dndistinct * tuples);
    }

    bucket->left_hashkeys = NIL;
    bucket->right_hashkeys = NIL;
    foreach (lc, es->clause_map) {
        es_clause_map* map = (es_clause_map*)lfirst(lc);
        bucket->left_hashkeys = lappend(bucket->left_hashkeys, map->left_var);
        bucket->right_hashkeys = lappend(bucket->right_hashkeys, map->right_var);
    }

    return;
}

Selectivity ES_SELECTIVITY::estimate_hash_bucketsize(
    es_bucketsize* es_bucket, double* distinctnum, bool left, Path* inner_path, double nbuckets)
{
    double estfract, ndistinct, mcvfreq, avgfreq;
    RelOptInfo* rel = left ? es_bucket->left_rel : es_bucket->right_rel;
    List* hashkeys = left ? es_bucket->left_hashkeys : es_bucket->right_hashkeys;

    ndistinct = estimate_local_numdistinct(es_bucket, left, inner_path);
    *distinctnum = ndistinct;

    /* Compute avg freq of all distinct data values in raw relation */
    avgfreq = 1.0 / ndistinct;

    /*
     * Initial estimate of bucketsize fraction is 1/nbuckets as long as the
     * number of buckets is less than the expected number of distinct values;
     * otherwise it is 1/ndistinct.
     */
    if (ndistinct > nbuckets)
        estfract = 1.0 / nbuckets;
    else {
        if (ndistinct < 1.0)
            ndistinct = 1.0;
        estfract = 1.0 / ndistinct;
    }

    /*
     * Look up the frequency of the most common value, if available.
     */
    mcvfreq = left ? es_bucket->left_mcvfreq : es_bucket->right_mcvfreq;

    /* We should adjust mcvfreq with selectivity because mcvfreq is changed after joined or joined. */
    mcvfreq /= (rel->rows / rel->tuples);
    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("[ES]rows=%.lf, tuples=%.lf, multiple=%.lf", rel->rows, rel->tuples, rel->multiple)));

    /*
     * Adjust estimated bucketsize upward to account for skewed distribution.
     */
    if (avgfreq > 0.0 && mcvfreq > avgfreq) {
        /* if hashkey contains distribute key, mcv freq should be multiplied by dn number */
        double multiple = 1.0;
        /* for now, only consider one distribute key situation */
        if (list_length(hashkeys) >= list_length(rel->distribute_keys) &&
            list_is_subset(rel->distribute_keys, hashkeys))
            multiple = u_sess->pgxc_cxt.NumDataNodes;

        estfract *= mcvfreq / avgfreq;
        /* if adjusted selectivity is larger than mcvfreq, then the estimate is too far off,
        take the mcvfreq instead. */
        if (estfract > mcvfreq * multiple)
            estfract = mcvfreq * multiple;
    }

    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("[ES]ndistinct=%.lf, avgfreq=%.10f, mcvfreq=%.10f, estfract=%.10f",
                ndistinct,
                avgfreq,
                mcvfreq,
                estfract)));

    /*
     * Clamp bucketsize to sane range (the above adjustment could easily
     * produce an out-of-range result).  We set the lower bound a little above
     * zero, since zero isn't a very sane result.
     * We should adjust the lower bound as 1.0e-7 because the distinct value
     * may be larger than 1000000 as the increasing of work_mem.
     */
    if (estfract < 1.0e-7)
        estfract = 1.0e-7;
    else if (estfract > 1.0) {
        if (mcvfreq > 0.0)
            estfract = mcvfreq;
        else
            estfract = 1.0;
    }

    return (Selectivity)estfract;
}

/*
 * @brief       mostly as same as estimate_local_numdistinct
 */
double ES_SELECTIVITY::estimate_local_numdistinct(es_bucketsize* bucket, bool left, Path* pathnode)
{
    VariableStatData vardata;
    bool usesinglestats = true;
    double ndistinct = left ? bucket->left_dndistinct : bucket->right_dndistinct;
    double global_distinct = left ? bucket->left_distinct : bucket->right_distinct;
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(pathnode);
    List* hashkeys = left ? bucket->left_hashkeys : bucket->right_hashkeys;

    vardata.rel = left ? bucket->left_rel : bucket->right_rel;

    /* we should adjust local distinct if there is no tuples in dn1 for global stats. */
    if ((ndistinct * num_datanodes) < global_distinct)
        ndistinct = get_local_rows(global_distinct, vardata.rel->multiple, false, num_datanodes);

    /* Adjust global distinct values for STREAM_BROADCAST and STREAM_REDISTRIBUTE. */
    if (IsA(pathnode, StreamPath) || IsA(pathnode, HashPath) || IsLocatorReplicated(pathnode->locator_type)) {
        ndistinct =
            estimate_hash_num_distinct(root, hashkeys, pathnode, &vardata, ndistinct, global_distinct, &usesinglestats);
    }

    /*
     * Adjust ndistinct to account for restriction clauses.  Observe we are
     * assuming that the data distribution is affected uniformly by the
     * restriction clauses!
     *
     * XXX Possibly better way, but much more expensive: multiply by
     * selectivity of rel's restriction clauses that mention the target Var.
     *
     * Only single stat need multiple the ratio as rows/tuples, because
     * possion for global stat.
     * Else if we have use possion, we don't need multiple the ratio.
     */
    return ndistinct;
}

/*
 * @brief       calculate selectivity for eqsel using multi-column statistics
 */
Selectivity ES_SELECTIVITY::cal_eqsel(es_candidate* es)
{
    Selectivity result = 1.0;
    ListCell* lc = NULL;
    int i = 0;
    int j = 0;
    int column_count = 0;
    bool match = false;

    Assert(es->left_extended_stats);

    /* if all clauses are null, just use nullfrac */
    if (es->has_null_clause) {
        foreach (lc, es->clause_group) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
            if (!IsA(rinfo->clause, NullTest))
                break;
        }
        if (lc == NULL) {
            result = es->left_extended_stats->nullfrac;
            CLAMP_PROBABILITY(result);
            save_selectivity(es, result, 0.0);
            return result;
        }
    }

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values) {
        result = (result - es->left_extended_stats->nullfrac) / es->left_stadistinct;
        CLAMP_PROBABILITY(result);
        save_selectivity(es, result, 0.0);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]extended statistic is used to calculate eqsel selectivity as %e", result))));
        return result;
    }

    /* try to use MCV */
    column_count = es->clause_group->length;

    Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

    /* set up attnum order */
    int* attnum_order = (int*)palloc(column_count * sizeof(int));
    set_up_attnum_order(es, attnum_order, true);

    Assert(es->left_extended_stats->mcv_nvalues / column_count == es->left_extended_stats->mcv_nnumbers);

    /* match MCV with const value from clauses */
    double sum_mcv_numbers = 0.0;
    for (i = 0; i < es->left_extended_stats->mcv_nnumbers; i++) {
        match = false;
        j = 0;
        /* process clause one by one */
        foreach (lc, es->clause_group) {
            FmgrInfo eqproc;
            Datum const_value;
            bool var_on_left = false;

            /* set up eqproc */
            RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
            int mcv_position = attnum_order[j] * es->left_extended_stats->mcv_nnumbers + i;

            if (IsA(clause->clause, OpExpr)) {
                OpExpr* opclause = (OpExpr*)clause->clause;
                Oid opno = opclause->opno;
                fmgr_info(get_opcode(opno), &eqproc);

                /* set up const value */
                Node* left = (Node*)linitial(opclause->args);
                Node* right = (Node*)lsecond(opclause->args);
                if (IsA(left, Const)) {
                    const_value = ((Const*)left)->constvalue;
                    var_on_left = false;
                } else if (IsA(right, Const)) {
                    const_value = ((Const*)right)->constvalue;
                    var_on_left = true;
                }

                Datum mcv_value = es->left_extended_stats->mcv_values[mcv_position];
                if (var_on_left)
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, mcv_value, const_value));
                else
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, const_value, mcv_value));
            } else {
                Assert(IsA(clause->clause, NullTest));
                match = es->left_extended_stats->mcv_nulls[mcv_position];
            }

            if (!match)
                break;
            j++;
        }

        if (match) {
            result = es->left_extended_stats->mcv_numbers[i];
            break;
        } else
            sum_mcv_numbers += es->left_extended_stats->mcv_numbers[i];
    }

    if (!match) {
        double sum_other_mcv_numbers = 0.0;
        for (int index = 0; index < es->left_extended_stats->other_mcv_nnumbers; index++)
            sum_other_mcv_numbers += es->left_extended_stats->other_mcv_numbers[index];
        result = 1.0 - sum_mcv_numbers - sum_other_mcv_numbers - es->left_extended_stats->nullfrac;
        CLAMP_PROBABILITY(result);
        float4 other_distinct = clamp_row_est(es->left_stadistinct - es->left_extended_stats->mcv_nnumbers);
        result /= other_distinct;

        /*
         * Another cross-check: selectivity shouldn't be estimated as more
         * than the least common "most common value".
         */
        int last_mcv_member = es->left_extended_stats->mcv_nnumbers - 1;
        float4 least_common_value = es->left_extended_stats->mcv_numbers[last_mcv_member];
        if (result > least_common_value)
            result = least_common_value;
    }

    pfree_ext(attnum_order);

    CLAMP_PROBABILITY(result);
    save_selectivity(es, result, 0.0);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT), (errmsg("[ES]extended statistic is used to calculate eqsel selectivity as %e", result))));
    return result;
}

/*
 * @brief    	calculate selectivity for join using multi-column statistics
 */
Selectivity ES_SELECTIVITY::cal_eqjoinsel_inner(es_candidate* es)
{
    Assert(es->left_extended_stats);
    Selectivity result = 1.0;
    int i;

    /* update nullfrac to contain null mcv fraction */
    for (i = 0; i < es->left_extended_stats->other_mcv_nnumbers; i++)
        es->left_extended_stats->nullfrac += es->left_extended_stats->other_mcv_numbers[i];
    for (i = 0; i < es->right_extended_stats->other_mcv_nnumbers; i++)
        es->right_extended_stats->nullfrac += es->right_extended_stats->other_mcv_numbers[i];

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values || !es->right_extended_stats->mcv_values) {
        result *= (1.0 - es->left_extended_stats->nullfrac) * (1.0 - es->right_extended_stats->nullfrac);
        result /= es->left_stadistinct > es->right_stadistinct ? es->left_stadistinct : es->right_stadistinct;
        CLAMP_PROBABILITY(result);
        double left_ratio = es->right_stadistinct / es->left_stadistinct * (1.0 - es->left_extended_stats->nullfrac);
        double right_ratio = es->left_stadistinct / es->right_stadistinct * (1.0 - es->right_extended_stats->nullfrac);
        save_selectivity(es, left_ratio, right_ratio);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]extended statistic is used to calculate eqjoinsel_inner selectivity as %e", result))));
        return result;
    }

    /* try to use MCV */
    int column_count = es->clause_group->length;
    Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

    /* set up attnum order */
    int* left_attnum_order = (int*)palloc(column_count * sizeof(int));
    int* right_attnum_order = (int*)palloc(column_count * sizeof(int));
    set_up_attnum_order(es, left_attnum_order, true);
    set_up_attnum_order(es, right_attnum_order, false);

    ListCell* lc = NULL;
    FmgrInfo* eqproc = (FmgrInfo*)palloc(column_count * sizeof(FmgrInfo));
    bool* left_var_on_clause_leftside = (bool*)palloc(column_count * sizeof(bool));
    i = 0;
    foreach (lc, es->clause_group) {
        /* set up eqproc */
        RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
        OpExpr* opclause = (OpExpr*)clause->clause;
        Oid opno = opclause->opno;
        fmgr_info(get_opcode(opno), &(eqproc[i]));

        /* set up left_var_on_clause_leftside */
        if (bms_equal(clause->left_relids, es->left_relids))
            left_var_on_clause_leftside[i] = true;
        else
            left_var_on_clause_leftside[i] = false;

        i++;
    }

    /* prepare to match MCVs */
    double left_relfrac = es->left_rel->rows / es->left_rel->tuples;
    double right_relfrac = es->right_rel->rows / es->right_rel->tuples;
    CLAMP_PROBABILITY(left_relfrac);
    CLAMP_PROBABILITY(right_relfrac);
    double relfrac = left_relfrac * right_relfrac;

    int left_mcv_nums = es->left_extended_stats->mcv_nnumbers;
    int right_mcv_nums = es->right_extended_stats->mcv_nnumbers;
    bool* left_match = (bool*)palloc0(left_mcv_nums * sizeof(bool));
    bool* right_match = (bool*)palloc0(right_mcv_nums * sizeof(bool));

    /*
     * The calculation logic here is as same as that in eqjoinsel_inner:
     * Note we assume that each MCV will match at most one member of the
     * other MCV list.	If the operator isn't really equality, there could
     * be multiple matches --- but we don't look for them, both for speed
     * and because the math wouldn't add up...
     */
    double matchprodfreq = 0.0;
    int nmatches = 0;
    for (i = 0; i < left_mcv_nums; i++) {
        int j;

        for (j = 0; j < right_mcv_nums; j++) {
            if (right_match[j])
                continue;
            bool all_match = false;
            int k = 0;
            /* process clause one by one */
            foreach (lc, es->clause_group) {
                int left_mcv_position = left_attnum_order[k] * left_mcv_nums + i;
                int right_mcv_position = right_attnum_order[k] * right_mcv_nums + j;
                Datum left_mcv_value = es->left_extended_stats->mcv_values[left_mcv_position];
                Datum right_mcv_value = es->right_extended_stats->mcv_values[right_mcv_position];
                if (left_var_on_clause_leftside[k])
                    all_match = DatumGetBool(
                        FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, left_mcv_value, right_mcv_value));
                else
                    all_match = DatumGetBool(
                        FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, right_mcv_value, left_mcv_value));
                if (!all_match)
                    break;
                k++;
            }

            if (all_match) {
                left_match[i] = right_match[j] = true;
                matchprodfreq += es->left_extended_stats->mcv_numbers[i] * es->right_extended_stats->mcv_numbers[j];
                nmatches++;
                break;
            }
        }
    }

    /* adjust match freq according to relation's filter fraction */
    double left_nullfrac = es->left_extended_stats->nullfrac;
    double right_nullfrac = es->right_extended_stats->nullfrac;
    double left_matchfreq, right_matchfreq, left_unmatchfreq, right_unmatchfreq, left_otherfreq, right_otherfreq,
        left_totalsel, right_totalsel;
    int tmp_nmatches = (int)ceil((double)nmatches * relfrac);
    if (nmatches != 0)
        matchprodfreq *= (double)tmp_nmatches / nmatches;
    CLAMP_PROBABILITY(matchprodfreq);
    /* Sum up frequencies of matched and unmatched MCVs */
    left_matchfreq = left_unmatchfreq = 0.0;
    for (i = 0; i < left_mcv_nums; i++) {
        if (left_match[i])
            left_matchfreq += es->left_extended_stats->mcv_numbers[i];
        else
            left_unmatchfreq += es->left_extended_stats->mcv_numbers[i];
    }
    CLAMP_PROBABILITY(left_matchfreq);
    CLAMP_PROBABILITY(left_unmatchfreq);
    right_matchfreq = right_unmatchfreq = 0.0;
    for (i = 0; i < right_mcv_nums; i++) {
        if (right_match[i])
            right_matchfreq += es->right_extended_stats->mcv_numbers[i];
        else
            right_unmatchfreq += es->right_extended_stats->mcv_numbers[i];
    }
    CLAMP_PROBABILITY(right_matchfreq);
    CLAMP_PROBABILITY(right_unmatchfreq);
    pfree_ext(left_match);
    pfree_ext(right_match);
    pfree_ext(left_attnum_order);
    pfree_ext(right_attnum_order);

    /*
     * Compute total frequency of non-null values that are not in the MCV
     * lists.
     */
    left_otherfreq = 1.0 - left_nullfrac - left_matchfreq - left_unmatchfreq;
    right_otherfreq = 1.0 - right_nullfrac - right_matchfreq - right_unmatchfreq;
    CLAMP_PROBABILITY(left_otherfreq);
    CLAMP_PROBABILITY(right_otherfreq);

    /*
     * We can estimate the total selectivity from the point of view of
     * relation 1 as: the known selectivity for matched MCVs, plus
     * unmatched MCVs that are assumed to match against random members of
     * relation 2's non-MCV population, plus non-MCV values that are
     * assumed to match against random members of relation 2's unmatched
     * MCVs plus non-MCV values.
     */
    int left_nvalues_frac = (int)ceil((double)left_mcv_nums * left_relfrac);
    int right_nvalues_frac = (int)ceil((double)right_mcv_nums * right_relfrac);
    left_totalsel = matchprodfreq;
    if (es->right_extended_stats->distinct > right_nvalues_frac)
        left_totalsel +=
            left_unmatchfreq * right_otherfreq / (es->right_extended_stats->distinct - right_nvalues_frac) * relfrac;
    if (es->right_extended_stats->distinct > tmp_nmatches)
        left_totalsel += left_otherfreq * (right_otherfreq + right_unmatchfreq) /
                         (es->right_extended_stats->distinct - tmp_nmatches) * relfrac;
    /* Same estimate from the point of view of relation 2. */
    right_totalsel = matchprodfreq;
    if (es->left_extended_stats->distinct > left_nvalues_frac)
        right_totalsel +=
            right_unmatchfreq * left_otherfreq / (es->left_extended_stats->distinct - left_nvalues_frac) * relfrac;
    if (es->left_extended_stats->distinct > tmp_nmatches)
        right_totalsel += right_otherfreq * (left_otherfreq + left_unmatchfreq) /
                          (es->left_extended_stats->distinct - tmp_nmatches) * relfrac;

    /*
     * Use the smaller of the two estimates.  This can be justified in
     * essentially the same terms as given below for the no-stats case: to
     * a first approximation, we are estimating from the point of view of
     * the relation with smaller nd.
     */
    if (0 == relfrac)
        result = 0;
    else
        result = (left_totalsel < right_totalsel) ? left_totalsel / relfrac : right_totalsel / relfrac;

    /*
     * calculate join ratio for both two tables, admitting that smaller distinct
     * values will be all joined out
     */
    double left_join_ratio = 0.0;
    double right_join_ratio = 0.0;

    if (0 != nmatches && 0 != left_relfrac && 0 != right_relfrac) {
        left_join_ratio = right_matchfreq * tmp_nmatches / (nmatches * left_relfrac);
        right_join_ratio = right_matchfreq * tmp_nmatches / (nmatches * right_relfrac);
    }

    if (es->left_extended_stats->distinct > es->right_extended_stats->distinct) {
        if (es->left_extended_stats->distinct != tmp_nmatches) {
            left_join_ratio += left_otherfreq * (es->right_extended_stats->distinct - tmp_nmatches) /
                               (es->left_extended_stats->distinct - tmp_nmatches);
        }
        right_join_ratio += right_otherfreq;
    } else if (es->left_extended_stats->distinct < es->right_extended_stats->distinct) {
        if (es->right_extended_stats->distinct != tmp_nmatches) {
            right_join_ratio += right_otherfreq * (es->left_extended_stats->distinct - tmp_nmatches) /
                                (es->right_extended_stats->distinct - tmp_nmatches);
        }
        left_join_ratio += left_otherfreq;
    }
    CLAMP_PROBABILITY(left_join_ratio);
    CLAMP_PROBABILITY(right_join_ratio);
    CLAMP_PROBABILITY(result);

    save_selectivity(es, left_join_ratio, right_join_ratio);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate eqjoinsel selectivity as %e", result))));

    /* save mcv freq to calculate skew or hash bucket size */
    es->left_first_mcvfreq = es->left_extended_stats->mcv_numbers[0];
    es->right_first_mcvfreq = es->right_extended_stats->mcv_numbers[0];

    return result;
}

Selectivity ES_SELECTIVITY::cal_eqjoinsel_semi(es_candidate* es, RelOptInfo* inner_rel, bool inner_on_left)
{
    Assert(es->left_extended_stats);
    Selectivity result = 1.0;
    double nullfrac = inner_on_left ? es->right_extended_stats->nullfrac : es->left_extended_stats->nullfrac;
    double inner_distinct = inner_on_left ? es->left_stadistinct : es->right_stadistinct;
    double outer_distinct = inner_on_left ? es->right_stadistinct : es->left_stadistinct;

    /*
     * Clamp inner_distinct to be not more than what we estimate the inner relation's
     * size to be, especially when inner_rel can be a joined rel.
     */
    inner_distinct = Min(inner_distinct, inner_rel->rows);

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values || !es->right_extended_stats->mcv_values) {
        result *= (1.0 - nullfrac);
        if (inner_distinct < outer_distinct)
            result *= inner_distinct / outer_distinct;
    } else {
        /* try to use MCV */
        int column_count = es->clause_group->length;
        Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

        /* set up attnum order */
        int* left_attnum_order = (int*)palloc(column_count * sizeof(int));
        int* right_attnum_order = (int*)palloc(column_count * sizeof(int));
        set_up_attnum_order(es, left_attnum_order, true);
        set_up_attnum_order(es, right_attnum_order, false);

        ListCell* lc = NULL;
        FmgrInfo* eqproc = (FmgrInfo*)palloc(column_count * sizeof(FmgrInfo));
        bool* left_var_on_clause_leftside = (bool*)palloc(column_count * sizeof(bool));
        int i = 0;
        foreach (lc, es->clause_group) {
            /* set up eqproc */
            RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
            OpExpr* opclause = (OpExpr*)clause->clause;
            Oid opno = opclause->opno;
            fmgr_info(get_opcode(opno), &(eqproc[i]));

            /* set up left_var_on_clause_leftside */
            if (bms_equal(clause->left_relids, es->left_relids))
                left_var_on_clause_leftside[i] = true;
            else
                left_var_on_clause_leftside[i] = false;

            i++;
        }

        /* prepare to match MCVs */
        int left_mcv_nums = es->left_extended_stats->mcv_nnumbers;
        int right_mcv_nums = es->right_extended_stats->mcv_nnumbers;
        bool* left_match = (bool*)palloc0(left_mcv_nums * sizeof(bool));
        bool* right_match = (bool*)palloc0(right_mcv_nums * sizeof(bool));

        /*
         * The calculation logic here is as same as that in eqjoinsel_inner:
         * Note we assume that each MCV will match at most one member of the
         * other MCV list.	If the operator isn't really equality, there could
         * be multiple matches --- but we don't look for them, both for speed
         * and because the math wouldn't add up...
         */
        double matchprodfreq = 0.0;
        int nmatches = 0;
        for (i = 0; i < left_mcv_nums; i++) {
            int j;

            for (j = 0; j < right_mcv_nums; j++) {
                if (right_match[j])
                    continue;
                bool all_match = false;
                int k = 0;
                /* process clause one by one */
                foreach (lc, es->clause_group) {
                    int left_mcv_position = left_attnum_order[k] * left_mcv_nums + i;
                    int right_mcv_position = right_attnum_order[k] * right_mcv_nums + j;
                    Datum left_mcv_value = es->left_extended_stats->mcv_values[left_mcv_position];
                    Datum right_mcv_value = es->right_extended_stats->mcv_values[right_mcv_position];
                    if (left_var_on_clause_leftside[k])
                        all_match = DatumGetBool(
                            FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, left_mcv_value, right_mcv_value));
                    else
                        all_match = DatumGetBool(
                            FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, right_mcv_value, left_mcv_value));
                    if (!all_match)
                        break;
                    k++;
                }

                if (all_match) {
                    left_match[i] = right_match[j] = true;
                    nmatches++;
                    break;
                }
            }
        }

        pfree_ext(left_attnum_order);
        pfree_ext(right_attnum_order);
        matchprodfreq = 1.0;
        int mcv_num = inner_on_left ? left_mcv_nums : right_mcv_nums;
        for (i = 0; i < mcv_num; i++) {
            if (inner_on_left && right_match[i])
                matchprodfreq += es->right_extended_stats->mcv_numbers[i];
            else if (!inner_on_left && left_match[i])
                matchprodfreq += es->left_extended_stats->mcv_numbers[i];
        }
        CLAMP_PROBABILITY(matchprodfreq);

        /*
         * Now we need to estimate the fraction of relation 1 that has at
         * least one join partner.	We know for certain that the matched MCVs
         * do, so that gives us a lower bound, but we're really in the dark
         * about everything else.  Our crude approach is: if nd1 <= nd2 then
         * assume all non-null rel1 rows have join partners, else assume for
         * the uncertain rows that a fraction nd2/nd1 have join partners. We
         * can discount the known-matched MCVs from the distinct-values counts
         * before doing the division.
         *
         * Crude as the above is, it's completely useless if we don't have
         * reliable ndistinct values for both sides.  Hence, if either nd1 or
         * nd2 is default, punt and assume half of the uncertain rows have
         * join partners.
         */
        inner_distinct -= nmatches;
        outer_distinct -= nmatches;
        double uncertainfrac, uncertain;
        if (inner_distinct >= outer_distinct || inner_distinct < 0)
            uncertainfrac = 1.0;
        else
            uncertainfrac = inner_distinct / outer_distinct;
        uncertain = 1.0 - matchprodfreq - nullfrac;
        CLAMP_PROBABILITY(uncertain);
        result = matchprodfreq + uncertainfrac * uncertain;
    }

    CLAMP_PROBABILITY(result);
    save_selectivity(es, result, 0.0, true);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate eqjoinsel_semi selectivity as %e", result))));
    return result;
}

/*
 * @brief       calculate distinct for groupby using multi-column statistics,
 * 		     in order to use the result in estimate_num_groups(),
 *		     build a varinfo and save it in unmatched_clause_group.
 */
void ES_SELECTIVITY::build_pseudo_varinfo(es_candidate* es, STATS_EST_TYPE eType)
{
    /* build pseudo varinfo here with multi-column distinct */
    GroupVarInfo* varinfo = (GroupVarInfo*)palloc(sizeof(GroupVarInfo));
    varinfo->var = NULL;
    varinfo->rel = es->left_rel;
    if (eType == STATS_TYPE_GLOBAL)
        varinfo->ndistinct = es->left_stadistinct;
    else {
        /* get local distinct */
        if (es->left_extended_stats->dndistinct < 0) {
            double ntuples = get_local_rows(es->left_rel->tuples,
                es->left_rel->multiple,
                IsLocatorReplicated(es->left_rel->locator_type),
                ng_get_dest_num_data_nodes(es->left_rel));
            varinfo->ndistinct = clamp_row_est(
                -1 * es->left_extended_stats->dndistinct * ntuples * (1.0 - es->left_extended_stats->nullfrac));
        } else
            varinfo->ndistinct = clamp_row_est(es->left_extended_stats->dndistinct);
    }
    varinfo->isdefault = false;
    varinfo->es_is_used = true;
    varinfo->es_attnums = bms_copy(es->left_attnums);
    unmatched_clause_group = lappend(unmatched_clause_group, varinfo);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate groupby distinct as %f", varinfo->ndistinct))));

    return;
}

/*
 * @brief       delete an invalid es_candidate, the memory will be free in clear().
 */
void ES_SELECTIVITY::remove_candidate(es_candidate* es)
{
    switch (es->tag) {
        case ES_EQSEL:
        case ES_EQJOINSEL:
        case ES_GROUPBY:
            unmatched_clause_group = list_concat(unmatched_clause_group, es->clause_group);
            break;
        default:
            break;
    }
    es->tag = ES_EMPTY;
    es->clause_group = NULL;

    return;
}

/*
 * @brief       remove corresponding stuff from the left_attnums of es_candidate according dump attnum,
 * 		     dump attnum should be delete outside this function.
 */
void ES_SELECTIVITY::remove_attnum(es_candidate* es, int dump_attnum)
{
    /* delete clause and clause_map according to dump */
    ListCell* lc_clause = list_head(es->clause_group);
    ListCell* lc_clause_map = NULL;
    ListCell* prev_clause = NULL;
    ListCell* prev_clause_map = NULL;
    foreach (lc_clause_map, es->clause_map) {
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc_clause_map);

        if (clause_map->left_attnum == dump_attnum) {
            if (es->tag == ES_EQJOINSEL) {
                Assert(bms_is_member(clause_map->right_attnum, es->right_attnums));
                bms_del_member(es->right_attnums, clause_map->right_attnum);
            }

            /* if found, delete clause map from the list */
            pfree_ext(clause_map);
            lfirst(lc_clause_map) = NULL;
            es->clause_map = list_delete_cell(es->clause_map, lc_clause_map, prev_clause_map);

            /* delete clause from list and add to unmatch list */
            unmatched_clause_group = lappend(unmatched_clause_group, lfirst(lc_clause));
            lfirst(lc_clause) = NULL;
            es->clause_group = list_delete_cell(es->clause_group, lc_clause, prev_clause);

            /* no need to continue, just try to find next matched stats */
            break;
        } else {
            prev_clause = lc_clause;
            prev_clause_map = lc_clause_map;
            lc_clause = lc_clause->next;
        }
    }

    return;
}

/*
 * @brief       Set up attnum order according to clause map, so that we can use this order
 * 		     to locate the corresponding clause when we go through the bitmap of attnums.
 * 	  	     We need this order because the clause is ordered by its position in clauselist and mcv
 * 		     in extended stats are ordered by attnum.
 *		      left == true : set up left attnum order ; left == false : set up right attnum order .
 */
void ES_SELECTIVITY::set_up_attnum_order(es_candidate* es, int* attnum_order, bool left) const
{
    int i;
    int j = 0;
    ListCell* lc = NULL;
    foreach (lc, es->clause_map) {
        i = 0;
        int attnum = 0;
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc);
        Bitmapset* tmpset = left ? bms_copy(es->left_attnums) : bms_copy(es->right_attnums);
        while ((attnum = bms_first_member(tmpset)) >= 0) {
            if ((left && attnum == clause_map->left_attnum) || (!left && attnum == clause_map->right_attnum)) {
                attnum_order[j] = i;
                break;
            }
            i++;
        }
        pfree_ext(tmpset);
        j++;
    }
    return;
}

/*
 * @brief        print debug info including ES type, involving rels and clauses
 */
void ES_SELECTIVITY::debug_print()
{
    if (log_min_messages > ES_DEBUG_LEVEL)
        return;
    ListCell* l = NULL;
    foreach (l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("[ES]ES_TYPE = %d (0:empty; 1:eqsel; 2:eqjoinsel; 3:group by)", (int)temp->tag)));
        if (temp->tag == ES_EMPTY)
            continue;

        print_relids(temp->relids, "All rels:");

        if (temp->left_rte) {
            print_relids(temp->left_relids, "Left rels:");
            print_relids(temp->left_attnums, "Left attnums:");
            print_rel(temp->left_rte);
        }

        if (temp->right_rte) {
            print_relids(temp->right_relids, "Right rels:");
            print_relids(temp->right_attnums, "Right attnums:");
            print_rel(temp->right_rte);
        }

        switch (temp->tag) {
            case ES_EQSEL:
            case ES_EQJOINSEL:
                print_clauses(temp->clause_group);
                break;
            default:
                break;
        }
    }
    return;
}

void ES_SELECTIVITY::print_rel(RangeTblEntry* rel) const
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s, relkind: %c, inheritance or not:%d", rel->relname, rel->relkind, (int)rel->inh);
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[ES]%s", buf.data))));
    pfree_ext(buf.data);
    return;
}

void ES_SELECTIVITY::print_relids(Bitmapset* relids, const char* str) const
{
    StringInfoData buf;
    initStringInfo(&buf);
    Relids tmprelids = bms_copy(relids);
    int x;
    appendStringInfoString(&buf, str);
    while ((x = bms_first_member(tmprelids)) >= 0) {
        appendStringInfo(&buf, "%d, ", x);
    }
    bms_free_ext(tmprelids);
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[ES]%s", buf.data))));
    pfree_ext(buf.data);
    return;
}

void ES_SELECTIVITY::print_clauses(List* clauses) const
{
    if (root == NULL || list_length(clauses) == 0)
        return;

    ListCell* l = NULL;
    StringInfoData buf;
    initStringInfo(&buf);

    appendStringInfo(&buf, "Clause length:%d, clause list:", list_length(clauses));

    foreach (l, clauses) {
        RestrictInfo* c = (RestrictInfo*)lfirst(l);
        char* expr = print_expr((Node*)c->clause, root->parse->rtable);
        appendStringInfoString(&buf, expr);
        pfree_ext(expr);
        if (lnext(l))
            appendStringInfoString(&buf, ", ");
    }

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("[ES]%s", buf.data)));

    pfree_ext(buf.data);

    return;
}

char* ES_SELECTIVITY::print_expr(const Node* expr, const List* rtable) const
{
    return ExprToString(expr, rtable);
}
