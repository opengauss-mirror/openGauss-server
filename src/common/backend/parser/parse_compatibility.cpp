/* -------------------------------------------------------------------------
 *
 * parse_compatibility.cpp
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/common/backend/parser/parse_compatibility.cpp
 *
 * -------------------------------------------------------------------------
 */


#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_agg.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/xml.h"

#include <string.h>

/*
 * The term of "JoinTerm" represents a convertable "(+)" outer join, normally it consists
 * of left & right rel and all conditions (and join filter) (in "t1.c1 = t1.c2 and t1.c2 = 1" format)
 *
 * - lrtf/rrtf: [left-joinrel, right-joinrel]
 * - quals: [aexpr1, aexpr2, aexpr3, aexpr4, ...], AExpr is a "t1.c1 = t2.c2(+)" format
 * - joincond: (t1.c1=t2.c1) AND (t1.c2=t2.c2) AND (t1.c2=t2.2)..
 *
 */
typedef struct JoinTerm {
    /* left/right range-table-entry */
    RangeTblEntry* lrte;
    RangeTblEntry* rrte;

    /*
     * list of single-column join expressions, {{t1.c1 = t2.c1}, {t1.c2 = t2.c2}}
     * Inserted when we do pre-process the whereClause
     */
    List* quals;

    /*
     * The join condition that will be put into the convert left/right outer join's
     * ON clause, it is created by AND all aggregate_jointerms()
     */
    Expr* joincond;

    /* Join type of current join term */
    JoinType jointype;
} JoinTerm;

/* Local function declerations */
static void preprocess_plus_outerjoin_walker(Node** expr, OperatorPlusProcessContext* ctx);
static void aggregate_jointerms(OperatorPlusProcessContext* ctx);
static void plus_outerjoin_check(const OperatorPlusProcessContext* ctx);
static void preprocess_plus_outerjoin(OperatorPlusProcessContext* ctx);
static void convert_plus_outerjoin(const OperatorPlusProcessContext* ctx);
static PlusJoinRTEInfo* makePlusJoinInfo(bool needrecord);
static void insert_jointerm(OperatorPlusProcessContext* ctx, Expr* expr, RangeTblEntry* lrte, RangeTblEntry* rrte);
static void find_joinexpr_walker(Node* node, const RangeTblRef* rtr, bool* found);
static bool find_joinexpr(JoinExpr* jexpr, const RangeTblRef* rtr);
static int find_RangeTblRef(RangeTblEntry* rte, List* p_rtable);
static Node* get_JoinArg(const OperatorPlusProcessContext* ctx, RangeTblEntry* rte);
static int plus_outerjoin_get_rtindex(const OperatorPlusProcessContext* ctx, Node* node, RangeTblEntry* rte);
static void convert_one_plus_outerjoin(const OperatorPlusProcessContext* ctx, JoinTerm* join_term);
static void InitOperatorPlusProcessContext(ParseState* pstate, Node** whereClause, OperatorPlusProcessContext* ctx);
static bool contain_ColumnRefPlus(Node* clause);
static bool contain_ColumnRefPlus_walker(Node* node, void* context);
static bool contain_ExprSubLink(Node* clause);
static bool contain_ExprSubLink_walker(Node* node, void* context);
static void unsupport_syntax_plus_outerjoin(const OperatorPlusProcessContext* ctx, Node* expr);
static bool contain_JoinExpr(List* l);
static bool contain_AEXPR_AND_OR(Node* clause);
static bool contain_AEXPR_AND_OR_walker(Node* node, void* context);
static bool assign_query_ignore_flag_walker(Node* node, void *context);

bool getOperatorPlusFlag()
{
    return u_sess->parser_cxt.stmt_contains_operator_plus;
}

void resetOperatorPlusFlag()
{
    u_sess->parser_cxt.stmt_contains_operator_plus = false;
    return;
}

static PlusJoinRTEInfo* makePlusJoinInfo(bool needrecord)
{
    PlusJoinRTEInfo* info = (PlusJoinRTEInfo*)palloc0(sizeof(PlusJoinRTEInfo));
    info->needrecord = needrecord;
    info->info = NIL;

    return info;
}

PlusJoinRTEItem* makePlusJoinRTEItem(RangeTblEntry* rte, bool hasplus)
{
    PlusJoinRTEItem* item = (PlusJoinRTEItem*)palloc0(sizeof(PlusJoinRTEItem));
    item->rte = rte;
    item->hasplus = hasplus;

    return item;
}

/* Whether ignore operator "(+)", if ignore is true, means ignore */
void setIgnorePlusFlag(ParseState* pstate, bool ignore)
{
    pstate->ignoreplus = ignore;

    return;
}

/*
 * - insert_jointerm()
 *
 * - brief: Try to inert join_expr (t1.c1=t2.c1) into jointerm [RTE1,RTE, quals]
 */
static void insert_jointerm(OperatorPlusProcessContext* ctx, Expr* expr, RangeTblEntry* lrte, RangeTblEntry* rrte)
{
    ListCell* lc = NULL;
    JoinTerm* jterm = NULL;

#ifdef ENABLE_MULTIPLE_NODES
    Assert(IsA(expr, A_Expr));
#else
    Assert(IsA(expr, A_Expr) || IsA(expr, NullTest));
#endif

    /* lrte is the RTE with operator "(+)", it couldn't be NULL */
    Assert(lrte != NULL);

    foreach (lc, ctx->jointerms) {
        JoinTerm* jt = (JoinTerm*)lfirst(lc);

        /*
         * Find the homologous JoinTerm to append the expr.
         *
         * Expr like "t1.c1(+) = 1" need special attention, it will be transform a JoinTerm
         * with rrte is NULL. So if jt->rrte is NULL, it will be overrided by rrte.
         */
        if (jt->lrte == lrte && (jt->rrte == rrte || jt->rrte == NULL || rrte == NULL)) {
            jterm = jt;
            jterm->quals = lappend(jterm->quals, expr);

            if (jt->rrte == NULL) {
                jt->rrte = rrte;
            }
            break;
        }
    }

    if (jterm == NULL) {
        /* Not found, we insert into it */
        JoinTerm* jt = (JoinTerm*)palloc0(sizeof(JoinTerm));
        jt->lrte = lrte;
        jt->rrte = rrte;
        /* Always set jointype be JOINN_RIGHT because lrte specified by operator "(+)" */
        jt->jointype = JOIN_RIGHT;
        jt->quals = lappend(jt->quals, expr);
        ctx->jointerms = lappend(ctx->jointerms, jt);
    }

    return;
}

/*
 * - aggregate_jointerms()
 *
 * - brief: aggregate the quals(in list format) into one AND-ed connected A_Expr
 */
static void aggregate_jointerms(OperatorPlusProcessContext* ctx)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    JoinTerm* jt = NULL;
    bool needputback = false;

    Node** expr = (ctx->whereClause);

    /* First to order jointerm list */
    lc1 = list_head(ctx->jointerms);
    while (lc1 != NULL) {
        needputback = false;
        JoinTerm* e = (JoinTerm*)lfirst(lc1);
        jt = e;

        foreach (lc2, e->quals) {
            A_Expr* jtexpr = (A_Expr*)lfirst(lc2);

            /*
             * If rrte is NULL, operator "(+)" will be ignored there's no rrte for outer join.
             * So need put the jtexpr back to whereClause.
             */
            if (e->rrte == NULL) {
                needputback = true;
                *expr = (Node*)makeA_Expr(AEXPR_AND, NULL, *expr, (Node*)jtexpr, -1);
                continue;
            }

            if (jt->joincond == NULL) {
                jt->joincond = (Expr*)jtexpr;
            } else {
                A_Expr* new_aexpr = makeA_Expr(AEXPR_AND, NULL, (Node*)jt->joincond, (Node*)jtexpr, -1);
                jt->joincond = (Expr*)new_aexpr;
            }
        }

        /* Must reassigns lc1 before list_delete_ptr, lc1->next will be invaild after list_delete_ptr */
        lc1 = lc1->next;

        if (needputback) {
            ctx->jointerms = list_delete_ptr(ctx->jointerms, e);
        }
    }

    return;
}

/*
 * - find_joinexpr_walker()
 *
 * - brief: Recursive walker function for find_joinexpr(), if found returned in output
 *   parameter "found"
 */
static void find_joinexpr_walker(Node* node, const RangeTblRef* rtr, bool* found)
{
    Assert(node);

    if (found == NULL) {
        return;
    }

    switch (nodeTag(node)) {
        case T_RangeTblRef: {
            RangeTblRef* r = (RangeTblRef*)node;

            if (r->rtindex == rtr->rtindex) {
                *found = true;
            }
        }

        break;
        case T_JoinExpr: {
            JoinExpr* jexpr = (JoinExpr*)node;

            /* Try JoinExpr's left tree */
            (void)find_joinexpr_walker(jexpr->larg, rtr, found);
            if (*found) {
                return;
            }

            /* Try JoinExpr's right tree */
            (void)find_joinexpr_walker(jexpr->rarg, rtr, found);
            if (*found) {
                return;
            }
        }

        break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Operator \"(+)\" unknown node detected in %s()", __FUNCTION__)));
            break;
    }

    return;
}

/*
 * - find_joinexpr()
 *
 * - brief: Check if given RangeTblRef exists in jexpr(JoinExpr), major used in multi-table
 *          converted case where, e.g.
 */
static bool find_joinexpr(JoinExpr* jexpr, const RangeTblRef* rtr)
{
    bool found = false;
    if (rtr == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Operator \"(+)\" Invalid rtf %s()", __FUNCTION__)));
    }

    find_joinexpr_walker((Node*)jexpr, rtr, &found);

    return found;
}

static int find_RangeTblRef(RangeTblEntry* rte, List* p_rtable)
{
    ListCell* lc = NULL;
    int index = 0;

    foreach (lc, p_rtable) {
        if (equal(lfirst(lc), rte)) {
            return (index + 1);
        }
        index++;
    }

    return 0;
}

/*
 * -get_JoinArg()
 *
 * - brief: get Join arg by RTE.
 * 		Transform RTE to RTR firstly. The item of p_joinlist can be RTR or JoinExpr. So We
 * will walk through all the RTR and larg/rarg of JoinExpr in p_joinlist.
 */
static Node* get_JoinArg(const OperatorPlusProcessContext* ctx, RangeTblEntry* rte)
{
    ListCell* lc1 = NULL;

    RangeTblRef* rtr = makeNode(RangeTblRef);
    rtr->rtindex = find_RangeTblRef(rte, ctx->ps->p_rtable);

    /* rte MUST BE in ps->p_table */
    Assert(rtr->rtindex != 0);

    /* Check all RTE in p_joinlist, if found, delete it from p_joinlist, and return the rtr */
    foreach (lc1, ctx->ps->p_joinlist) {
        if (IsA(lfirst(lc1), RangeTblRef)) {
            RangeTblRef* r = (RangeTblRef*)lfirst(lc1);
            if (r->rtindex == rtr->rtindex) {
                ctx->ps->p_joinlist = list_delete_ptr(ctx->ps->p_joinlist, (void*)r);
                return (Node*)rtr;
            }
        }
    }

    /* Check JoinExpr in p_joinlist, if found, delete it from p_joinlist, and return the JoinExpr */
    foreach (lc1, ctx->ps->p_joinlist) {
        if (IsA(lfirst(lc1), JoinExpr)) {
            JoinExpr* j = (JoinExpr*)lfirst(lc1);
            if (!find_joinexpr(j, rtr)) {
                continue;
            }

            ctx->ps->p_joinlist = list_delete_ptr(ctx->ps->p_joinlist, (void*)j);
            return (Node*)j;
        }
    }

    return NULL;
}

/*
 * - preprocess_plus_outerjoin()
 *
 * - brief: entry point function of of pre-process operator (+), in the "preprocess"
 *   stage we do following
 *      [1]. Find convertable join expr (t1.c1=t2.c1(+)) in whereClause and insert into ctx->jointerm->quals
 *         list and groupped in same join pairs, see detail in function "insert_jointerm()"
 *      [2]. After walk-thru the whole expressoin tree, we do aggregation of join exprs to
 *           form joincond that will put into OnClause
 *      [3]. Check the collect JoinTerm and mark a convertable jointerm as valid
 */
static void preprocess_plus_outerjoin(OperatorPlusProcessContext* ctx)
{
    Assert(ctx != NULL);

    /*
     * Pre-process the whereClause, where is the "(+)" to outer join conversion's
     * major entry point
     */
    preprocess_plus_outerjoin_walker(ctx->whereClause, ctx);

    /*
     * 2. Join term returned from preprocess_plus_outerjoin_walker() is not aggregated
     * e.g.
     *  - jointerm[1] quals: t1.c1=t2.c1, t1.c2=t2.c1, t1.c2=t2.c2 ...
     *  - jointerm[2] quals: t1.c2 = t1.c2..
     *
     * We need "aggregate" them into RTE-Join-RTE and let the jointerm
     * to be a ANDed conntected join condition and transform them to joinExpr
     */
    aggregate_jointerms(ctx);

    plus_outerjoin_check(ctx);

    /* Mark current statement level contains operator "(+)" join conversion */
    if (ctx->jointerms != NIL) {
        ctx->contain_plus_outerjoin = true;
    }
}

static int plus_outerjoin_get_rtindex(const OperatorPlusProcessContext* ctx, Node* node, RangeTblEntry* rte)
{
    Assert(IsA(node, RangeTblRef) || IsA(node, JoinExpr));

    if (IsA(node, RangeTblRef)) {
        return ((RangeTblRef*)node)->rtindex;
    } else {
        /* node must be JoinExpr */
        return (find_RangeTblRef(rte, ctx->ps->p_rtable));
    }
}

/*
 * - convert_one_plus_outerjoin()
 *
 * - brief: convert jointerm to JoinExpr
 */
static void convert_one_plus_outerjoin(const OperatorPlusProcessContext* ctx, JoinTerm* join_term)
{
    RangeTblEntry* rte = NULL;
    List* l_colnames = NIL;
    List* r_colnames = NIL;
    List* l_colvars = NIL;
    List* r_colvars = NIL;
    List* res_colnames = NIL;
    List* res_colvars = NIL;
    List* my_relnamespace = NIL;
    Relids my_containedRels;
    int k = 0;
    int lrtindex = 0;
    int rrtindex = 0;

    JoinExpr* j = (JoinExpr*)makeNode(JoinExpr);

    /* get join arg from p_joinlist */
    j->larg = get_JoinArg(ctx, join_term->lrte);
    j->rarg = get_JoinArg(ctx, join_term->rrte);

    /*
     * Report error when join condition like "t1.c1 = t2.c1(+) and t2.c2 = t3.c1(+) and t3.c2 = t1.c2(+)".
     *
     * The larg/rarg are extracted from p_joinlist, after handle "t1.c1 = t2.c1(+) and t2.c2 = t3.c1(+)",
     * There be only one JoinExpr left in p_joinlist. when handle "t3.c2 = t1.c2(+)", larg will be set the
     * the JoinExpr, and rarg will be NULL.
     */
    if (j->larg == NULL || j->rarg == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Relation can't outer join with each other.")));
    }

    lrtindex = plus_outerjoin_get_rtindex(ctx, j->larg, join_term->lrte);
    rrtindex = plus_outerjoin_get_rtindex(ctx, j->rarg, join_term->rrte);

    ParseState* pstate = ctx->ps;
    RangeTblEntry* l_rte = join_term->lrte;
    RangeTblEntry* r_rte = join_term->rrte;

    bool lateral_ok = (j->jointype == JOIN_INNER || j->jointype == JOIN_LEFT);
    my_relnamespace = list_make2(makeNamespaceItem(l_rte, false, lateral_ok),
                                makeNamespaceItem(r_rte, false, true));

    Relids lcontainedRels = bms_make_singleton(lrtindex);
    Relids rcontainedRels = bms_make_singleton(rrtindex);

    my_containedRels = bms_join(lcontainedRels, rcontainedRels);

    expandRTE(l_rte, lrtindex, 0, -1, false, &l_colnames, &l_colvars);
    expandRTE(r_rte, rrtindex, 0, -1, false, &r_colnames, &r_colvars);

    res_colnames = list_concat(l_colnames, r_colnames);
    res_colvars = list_concat(l_colvars, r_colvars);

    j->quals = (Node*)join_term->joincond;

    /* No need record RTE info, Just avoid report error when handle operator "(+)" */
    setIgnorePlusFlag(pstate, true);
    j->quals = transformJoinOnClause(pstate, j, l_rte, r_rte, my_relnamespace);
    setIgnorePlusFlag(pstate, false);

    j->alias = NULL;
    j->jointype = join_term->jointype;

    /*
     * Now build an RTE for the result of the join
     */
    rte = addRangeTableEntryForJoin(pstate, res_colnames, j->jointype, res_colvars, j->alias, true);

    j->rtindex = list_length(pstate->p_rtable);

    Assert(rte == rt_fetch(j->rtindex, pstate->p_rtable));

    /* make a matching link to the JoinExpr for later use */
    for (k = list_length(pstate->p_joinexprs) + 1; k < j->rtindex; k++)
        pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
    pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
    Assert(list_length(pstate->p_joinexprs) == j->rtindex);

    /*
     * RTE have been added to p_relnamespace and p_varnamespace when transformFromClause, so
     * no need to add them again, just add the JoinExpr to p_joinlist.
     */
    pstate->p_joinlist = lappend(pstate->p_joinlist, j);

    return;
}

/*
 * - plus_outerjoin_check()
 *
 * - brief: check the ctx->jointerms globally.
 *    Only forbid more than one left RTE with same right RTE for now.
 */
static void plus_outerjoin_check(const OperatorPlusProcessContext* ctx)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    foreach (lc1, ctx->jointerms) {
        JoinTerm* jt1 = (JoinTerm*)lfirst(lc1);

        foreach (lc2, ctx->jointerms) {
            JoinTerm* jt2 = (JoinTerm*)lfirst(lc2);

            /*
             * Report Error when there's more than one left RTE join with same right RTE, like
             *		t1.c1 = t2.c1(+) and t3.c1 = t2.c2(+)
             */
            if (jt1->lrte == jt2->lrte && jt1->rrte != jt2->rrte) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("\"%s\" can't outer join with more than one relation.", jt1->lrte->eref->aliasname)));
            }
        }
    }
}

/*
 * - plus_outerjoin_precheck()
 *
 * - brief: check the join condition is valid.
 */
bool plus_outerjoin_precheck(const OperatorPlusProcessContext* ctx, Node* expr, List* lhasplus, List* lnoplus)
{
    /*
     * If not found "(+)", all JoinTerm with the same RTE will become JOIN_INNER, like
     *		t1.c1 = t2.c1(+)	#cond1
     *		and
     *		t1.c2 = t2.c2	#cond2
     * will be treated as inner join. There isn't "(+)" in cond2, just leave cond2 in WhereClause,
     * it will be handled by transformWhereClause, and cond1 that have been added into ctx->jointerms
     * will become JOIN_INNER during planner.
     */
    if (list_length(lhasplus) == 0 && list_length(lnoplus) == 2) {
        return false;
    }

    /*
     * Do nothing when there's no "(+)", like
     * 		t1.c1 = t2.c1
     */
    if (list_length(lhasplus) == 0) {
        return false;
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* Only support A_Expr with "(+)" for now */
    if (list_length(lhasplus) && !IsA(expr, A_Expr)) {
#else
    /* Only support A_Expr and NullTest with "(+)" for now */
    if (list_length(lhasplus) && !IsA(expr, A_Expr) && !IsA(expr, NullTest)) {
#endif
        ereport(
            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Operator \"(+)\" can only be used in common expression.")));
    }

    /*
     * Report Error when "(+)" specified on more than one relation, like
     * 		t1.c1(+) = t2.c1 + t3.c1(+)
     */
    if (list_length(lhasplus) > 1) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Operator \"(+)\" can't be specified on more than one relation in one join condition"),
                errhint("\"%s\", \"%s\"...are specified Operator \"(+)\" in one condition.",
                    ((RangeTblEntry*)linitial(lhasplus))->eref->aliasname,
                    ((RangeTblEntry*)lsecond(lhasplus))->eref->aliasname)));
    }

    /*
     * Report Error when the side without "(+)" contains more than one relation, like
     * 		t1.c1(+) = t2.c1 + t3.c1
     */
    if (list_length(lnoplus) > 1 && list_length(lhasplus) == 1) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("\"%s\" can't outer join with more than one relation",
                    ((RangeTblEntry*)linitial(lhasplus))->eref->aliasname),
                errhint("\"%s\", \"%s\" are outer join with \"%s\".",
                    ((RangeTblEntry*)linitial(lnoplus))->eref->aliasname,
                    ((RangeTblEntry*)lsecond(lnoplus))->eref->aliasname,
                    ((RangeTblEntry*)linitial(lhasplus))->eref->aliasname)));
    }

    /*
     * Report EROOR when "(+)" used with JoinExpr in fromClause
     */
    if (list_length(lhasplus) > 0 && ctx->contain_joinExpr) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Operator \"(+)\" and Join in FromClause can't be used together")));
    }

    /*
     * Report error when current (+) condition specified under OR branches, like
     *	t1.c1(+) = t13.c1 or t1.c2(+) = t12.c1
     *	t1.c1(+) = t13.c1 or t1.c2(+) = 1
     */
    if (ctx->in_orclause) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Operator \"(+)\" is not allowed used with \"OR\" together")));
    }

    /*
     * When length(lhasplus) == 1 and length(lnoplus) == 1, the RTEs in lhasplus and lnoplus
     * will be treated as left relation and right relation for Join, and the expr will be treated as
     * join condition.
     * if length(lnoplus) == 0, means the expr will be treat as join filter.
     * Otherwise we will report error.
     */
    if (list_length(lhasplus) != 1 || list_length(lnoplus) > 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Operator \"(+)\" transform failed.")));
    }

    if (list_length(lnoplus) == 1) {
        RangeTblEntry* t_noplus = (RangeTblEntry*)linitial(lnoplus);
        RangeTblEntry* t_hasplus = (RangeTblEntry*)linitial(lhasplus);
        /*
         * Disable the (+) when RTE in lnoplus is not in current query level, like
         *
         *	select ...from t1 where c1 in (select ...from t2 where t2.c1(+) = t1.c1);
         *
         * t1 is in the outer query, we ignore "(+)" in this case.
         */
        if (find_RangeTblRef(t_noplus, ctx->ps->p_rtable) == 0) {
            elog(LOG,
                "Operator \"(+)\" is ignored because \"%s\" isn't in current query level.",
                t_noplus->eref->aliasname);
            return false;
        }

        /* Report Error when t_noplus and t_hasplus is the same RTE */
        if (t_noplus == t_hasplus) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("\"%s\" is not allowed to outer join with itself.", t_noplus->eref->aliasname)));
        }
    } 
    return true;
}

static bool contain_ColumnRefPlus(Node* clause)
{
    return contain_ColumnRefPlus_walker(clause, NULL);
}

static bool contain_ColumnRefPlus_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, ColumnRef) && IsColumnRefPlusOuterJoin((ColumnRef*)node)) {
        return true;
    }

    return raw_expression_tree_walker(node, (bool (*)())contain_ColumnRefPlus_walker, (void*)context);
}

static bool contain_ExprSubLink(Node* clause)
{
    return contain_ExprSubLink_walker(clause, NULL);
}

static bool contain_ExprSubLink_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, SubLink) && (((SubLink*)(node))->subLinkType == EXPR_SUBLINK)) {
        return true;
    }

    return raw_expression_tree_walker(node, (bool (*)())contain_ExprSubLink_walker, (void*)context);
}

static bool contain_AEXPR_AND_OR(Node* clause)
{
    return contain_AEXPR_AND_OR_walker(clause, NULL);
}

static bool contain_AEXPR_AND_OR_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, SubLink) || IsA(node, SelectStmt) || IsA(node, MergeStmt) || IsA(node, UpdateStmt) ||
        IsA(node, DeleteStmt) || IsA(node, InsertStmt)) {
        return false;
    }

    if (IsA(node, A_Expr) && (((A_Expr*)node)->kind == AEXPR_AND || ((A_Expr*)node)->kind == AEXPR_OR)) {
        return true;
    }

    return raw_expression_tree_walker(node, (bool (*)())contain_AEXPR_AND_OR_walker, (void*)context);
}

static void unsupport_syntax_plus_outerjoin(const OperatorPlusProcessContext* ctx, Node* expr)
{
    /*
     * If expr is not A_Expr, it Must not contains "(+)", if any, will report error later.
     * So we assume expr is A_Expr.
     */
    if (!IsA(expr, A_Expr)) {
        return;
    }

    A_Expr* a = (A_Expr*)expr;
    if (a->lexpr != NULL && a->rexpr != NULL) {
        /*
         * Unsupport operator ''(+)" and SubLink used together in expr, like  "t1.c1(+) = (SubLink)",
         * but "t1.c1(+) + (SubLink) = 1 " is valid
         */
        if ((contain_ExprSubLink(a->lexpr) && contain_ColumnRefPlus(a->rexpr)) ||
            (contain_ExprSubLink(a->rexpr) && contain_ColumnRefPlus(a->lexpr))) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Operator \"(+)\" can not be used in outer join with SubQuery."),
                    parser_errposition(ctx->ps, a->location)));
        }
    } else {
        /* Unsupport nesting expression, like ''NOT(t1.c1 > t12.c2 and t1.c2 < t2.c1)' '*/
        if (contain_AEXPR_AND_OR(expr)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Operator \"(+)\" can not be used in nesting expression."),
                    parser_errposition(ctx->ps, a->location)));
        }
    }

    return;
}

bool plus_outerjoin_preprocess(const OperatorPlusProcessContext* ctx, Node* expr)
{
    ListCell* lc1 = NULL;
    List* lhasplus = NIL;
    List* lnoplus = NIL;

    ParseState* ps = ctx->ps;

    /* Check the expr, report error if occur syntax error. */
    unsupport_syntax_plus_outerjoin(ctx, expr);

    if (ps->p_plusjoin_rte_info != NULL) {
        list_free_deep(ps->p_plusjoin_rte_info->info);
        pfree_ext(ps->p_plusjoin_rte_info);
    }

    /*
     * We will walk through the expr by transformExpr, ps->p_plusjoin_rte_info will record the
     * RTE info that reference by the column in expr.
     */

    setIgnorePlusFlag(ps, true);

    ps->p_plusjoin_rte_info = makePlusJoinInfo(true);
    (void)transformExpr(ps, expr, EXPR_KIND_WHERE);

    setIgnorePlusFlag(ps, false);

    if (ps->p_plusjoin_rte_info->info == NIL) {
        return false;
    }

    foreach (lc1, ps->p_plusjoin_rte_info->info) {
        PlusJoinRTEItem* info = (PlusJoinRTEItem*)lfirst(lc1);

        if (info->hasplus) {
            /* RTE can be specified more than once in one Expr, so use list_append_unique */
            lhasplus = list_append_unique(lhasplus, info->rte);
        } else {
            lnoplus = list_append_unique(lnoplus, info->rte);
        }
    }

    list_free_deep(ps->p_plusjoin_rte_info->info);
    pfree_ext(ps->p_plusjoin_rte_info);

    if (plus_outerjoin_precheck(ctx, expr, lhasplus, lnoplus)) {
        Assert(list_length(lhasplus) == 1 && list_length(lnoplus) <= 1);

        RangeTblEntry* lrte = (RangeTblEntry*)linitial(lhasplus);
        RangeTblEntry* rrte = list_length(lnoplus) == 1 ? (RangeTblEntry*)linitial(lnoplus) : NULL;

        insert_jointerm((OperatorPlusProcessContext*)ctx, (Expr*)expr, lrte, rrte);

        return true;
    }

    return false;
}

/*
 * - preprocess_plus_outerjoin_walker()
 *
 * - brief: Function to recursively walk-thru the expression tree, if we found a portential
 * convertable col=col condition, we record into "ctx->jointerms".
 */
static void preprocess_plus_outerjoin_walker(Node** expr, OperatorPlusProcessContext* ctx)
{
    if (expr == NULL || *expr == NULL) {
        goto prerocess_exit;
    }

    switch (nodeTag(*expr)) {
        case T_A_Expr: {
            A_Expr* a = (A_Expr*)*expr;
            switch (a->kind) {
                case AEXPR_OR: {
                    bool needreset = false;

                    /*
                     * If already under OrClause, we don't have to restore ctx->in_clause
                     * to false, otherwise we need reset it.
                     */
                    if (!ctx->in_orclause) {
                        needreset = true;
                    }

                    /* Mark in ORClause in current level and its underlying level */
                    ctx->in_orclause = true;
                    preprocess_plus_outerjoin_walker(&((A_Expr*)*expr)->lexpr, ctx);
                    preprocess_plus_outerjoin_walker(&((A_Expr*)*expr)->rexpr, ctx);

                    /* Reset ORClause to false */
                    if (needreset) {
                        ctx->in_orclause = false;
                    }
                } break;

                case AEXPR_AND: {
                    preprocess_plus_outerjoin_walker(&((A_Expr*)*expr)->lexpr, ctx);
                    preprocess_plus_outerjoin_walker(&((A_Expr*)*expr)->rexpr, ctx);
                } break;

                case AEXPR_OP:
                case AEXPR_NOT:
                case AEXPR_OP_ANY:
                case AEXPR_OP_ALL:
                case AEXPR_DISTINCT:
                case AEXPR_NULLIF:
                case AEXPR_OF:
                case AEXPR_IN: {
                    /*
                     * Once we decide to convert the join, we are going to move it up to
                     * join condition and remove it from where Clause by replcing it with
                     * a dummy true condition "1=1"
                     */
                    if (plus_outerjoin_preprocess(ctx, *expr)) {
                        *expr = (Node*)makeSimpleA_Expr(AEXPR_OP,
                            "=",
                            (Node*)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(1), false, true),
                            (Node*)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(1), false, true),
                            -1);
                    }
                } break;
                default: /* like: t1.c1(+) in (1,2,3) */
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized A_Expr kind: %d used", a->kind)));
                }
            }
            break;
        }
        /*
         * Report Error if the expr contains ''(+)" and it's not A_Expr, like: (t1.c1 > t2.c2)::bool,
         * else just leave it in where cluase.
         */
        default: {
            (void)plus_outerjoin_preprocess(ctx, *expr);
        }
    }

prerocess_exit:
    return;
}

/*
 * - IsColumnRefPlusOuterJoin()
 *
 * - brief: help function to lookup if ColumnRef is "(+)" operator attached
 */
bool IsColumnRefPlusOuterJoin(const ColumnRef* cf)
{
    Assert(cf != NULL);

    if (!IsA(cf, ColumnRef)) {
        return false;
    }

    Node* ln = (Node*)lfirst(list_tail(cf->fields));
    if (nodeTag(ln) == T_String && strncmp(strVal(ln), "(+)", 3) == 0) {
        return true;
    }

    return false;
}

/*
 * - convert_plus_outerjoin()
 *
 * - brief: Convert each JoinTerms collected at preprocess step into outer joins
 */
void convert_plus_outerjoin(const OperatorPlusProcessContext* ctx)
{
    ListCell* lc = NULL;

    Assert(ctx->contain_plus_outerjoin);

    /* Loop ctx->jointerms and convert (+) to outer join one-by-one */
    foreach (lc, ctx->jointerms) {
        JoinTerm* jterm = (JoinTerm*)lfirst(lc);

        convert_one_plus_outerjoin(ctx, jterm);
    }
}

/*
 * - InitOperatorPlusProcessContext()
 *
 * - brief: init OperatorPlusProcessContext
 */
static void InitOperatorPlusProcessContext(ParseState* pstate, Node** whereClause, OperatorPlusProcessContext* ctx)
{
    ctx->contain_plus_outerjoin = false;
    ctx->jointerms = NIL;
    ctx->ps = pstate;
    ctx->in_orclause = false;
    ctx->whereClause = whereClause;
    ctx->contain_joinExpr = contain_JoinExpr(pstate->p_joinlist);
}

static bool contain_JoinExpr(List* l)
{
    ListCell* lc = NULL;
    foreach (lc, l) {
        if (IsA(lfirst(lc), JoinExpr)) {
            return true;
        }
    }

    return false;
}
/*
 * - transformPlusOperator()
 *
 * - brief: transform operator "(+)" to outer join.
 */
void transformOperatorPlus(ParseState* pstate, Node** whereClause)
{
    OperatorPlusProcessContext ctx;
    InitOperatorPlusProcessContext(pstate, whereClause, &ctx);

    /*
     * As there will be expr replacement when we do preprocess stage so we need
     * backup the whole whereClause before call preprocess_plus_outerjoin(), we
     * have to do so as we don't want to rescan whereClause twice if we replaces
     * some join condition but finally we found that collected join term is not
     * valid for "(+)" convertion.
     */
    Node* old_whereclause = (Node*)copyObject(*whereClause);

    /* Preprocess where clause */
    preprocess_plus_outerjoin(&ctx);

    /* Check if we can do conversion */
    if (ctx.contain_plus_outerjoin) {
        convert_plus_outerjoin(&ctx);
    } else {
        /*
         * If not able to do operator "(+)" outer join conversion, we need restore
         * the whereClause part
         */
        *whereClause = old_whereclause;
    }
}

/*
 * assign_query_ignore_flag
 *             Mark all query in the given Query with ignore information.
 *
 * Now only INSERT and UPDATE support ignore, so it only be called in
 * InsertStmt and UpdateStmt.
 */
void assign_query_ignore_flag(ParseState* pstate, Query* query)
{
    /* we only set ignore to true, cause false is default value. So if there is no ignore, we can return */
    if (!pstate->p_has_ignore) {
        return;
    }

    (void)query_tree_walker(query, (bool (*)())assign_query_ignore_flag_walker, NULL, 0);
}

/*
 * Walker for assign_query_ignore_flag
 *
 * Each expression found by query_tree_walker is processed independently.
 * Note that query_tree_walker may pass us a whole List, such as the
 * targetlist, in which case each subexpression must be processed
 * independently.
 */
static bool assign_query_ignore_flag_walker(Node* node, void *context)
{
    /* Need do nothing for empty subexpressions */
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Query)) {
        Query *query = (Query *) node;

        /* set it to true and continue to traverse the query tree */
        query->hasIgnore = true;
        return query_tree_walker(query, (bool (*)())assign_query_ignore_flag_walker, NULL, 0);
    }
    return expression_tree_walker(node, (bool (*)())assign_query_ignore_flag_walker, NULL);
}

