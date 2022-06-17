/* -------------------------------------------------------------------------
 *
 * prepjointree.cpp
 *	  Planner preprocessing for subqueries and join tree manipulation.
 *
 * NOTE: the intended sequence for invoking these operations is
 *      replace_empty_jointree
 *		pull_up_sublinks
 *		inline_set_returning_functions
 *		pull_up_subqueries
 *		flatten_simple_union_all
 *		do expression preprocessing (including flattening JOIN alias vars)
 *		reduce_outer_joins
 *      remove_useless_result_rtes
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/prep/prepjointree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_hint.h"
#include "optimizer/clauses.h"
#include "optimizer/nodegroups.h"
#include "optimizer/placeholder.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#ifdef PGXC
#include "access/sysattr.h"
#include "pgxc/pgxc.h"
#endif
#include "optimizer/streamplan.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "access/transam.h"
#include "catalog/pg_operator.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"

typedef struct pullup_replace_vars_context {
    PlannerInfo* root;
    List* targetlist;          /* tlist of subquery being pulled up */
    RangeTblEntry* target_rte; /* RTE of subquery */
    bool* outer_hasSubLinks;   /* -> outer query's hasSubLinks */
    int varno;                 /* varno of subquery */
    bool need_phvs;            /* do we need PlaceHolderVars? */
    bool wrap_non_vars;        /* do we need 'em on *all* non-Vars? */
    Node** rv_cache;           /* cache for results with PHVs */
} pullup_replace_vars_context;

typedef struct reduce_outer_joins_state {
    Relids relids;       /* base relids within this subtree */
    bool contains_outer; /* does subtree contain outer join(s)? */
    List* sub_states;    /* List of states for subtree components */
} reduce_outer_joins_state;

static Node* pull_up_sublinks_jointree_recurse(PlannerInfo* root, Node* jtnode, Relids* relids, Node* all_quals = NULL);
static Node* pull_up_sublinks_qual_recurse(PlannerInfo* root, Node* node, Node** jtlink1, Relids *available_rels1,
    Node** jtlink2, Relids *available_rels2, Node* all_quals = NULL);
static Node *pull_up_subqueries_recurse(PlannerInfo *root, Node *jtnode,
                          JoinExpr *lowest_outer_join,
                          JoinExpr *lowest_nulling_outer_join,
                          AppendRelInfo *containing_appendrel);
static Node* pull_up_simple_subquery(PlannerInfo* root, Node* jtnode,
    RangeTblEntry* rte, JoinExpr* lowest_outer_join, JoinExpr *lowest_nulling_outer_join,
    AppendRelInfo* containing_appendrel);
static Node* pull_up_simple_union_all(PlannerInfo* root, Node* jtnode, RangeTblEntry* rte);
static void pull_up_union_leaf_queries(
    Node* setOp, PlannerInfo* root, int parentRTindex, Query* setOpQuery, int childRToffset);
static void make_setop_translation_list(Query* query, Index newvarno, List** translated_vars);
static bool is_simple_lateral_subquery(Query* subquery, JoinExpr *lowest_outer_join);
static bool is_simple_subquery(Query* subquery, RangeTblEntry *rte, JoinExpr *lowest_outer_join);
static bool is_simple_union_all(Query* subquery);
static bool is_simple_union_all_recurse(Node* setOp, Query* setOpQuery, List* colTypes);
static bool is_safe_append_member(Query* subquery);
static bool jointree_contains_lateral_outer_refs(Node *jtnode,
                                    bool restricted, Relids safe_upper_varnos);
static void replace_vars_in_jointree(Node* jtnode, pullup_replace_vars_context* context, JoinExpr* lowest_nulling_outer_join);
static Node* pullup_replace_vars(Node* expr, pullup_replace_vars_context* context);
static Node* pullup_replace_vars_callback(Var* var, replace_rte_variables_context* context);
static Query *pullup_replace_vars_subquery(Query *query, pullup_replace_vars_context *context);
static reduce_outer_joins_state* reduce_outer_joins_pass1(Node* jtnode);
static void reduce_outer_joins_pass2(Node* jtnode, reduce_outer_joins_state* state, PlannerInfo* root,
    Relids nonnullable_rels, List* nonnullable_vars, List* forced_null_vars);
static void substitute_multiple_relids(Node* node, int varno, Relids subrelids);
static void fix_append_rel_relids(List* append_rel_list, int varno, Relids subrelids);
static Node* find_jointree_node_for_rel(Node* jtnode, int relid);
static Node* deleteRelatedNullTest(Node* node, PlannerInfo* root);
static Node* reduce_inequality_fulljoins_jointree_recurse(PlannerInfo* root, Node* jtnode);
static Node *pull_up_sublinks_targetlist(PlannerInfo *root, Node *node,
                              Node *jtnode, Relids *relids,
                              Node **newTargetList,
                              Node *whereQuals);

#ifndef ENABLE_MULTIPLE_NODES
static bool find_rownum_in_quals(PlannerInfo *root);
static bool contains_swctes(const PlannerInfo *root);
#endif

/*
 * replace_empty_jointree
 *             If the Query's jointree is empty, replace it with a dummy RTE_RESULT
 *             relation.
 *
 * By doing this, we can avoid a bunch of corner cases that formerly existed
 * for SELECTs with omitted FROM clauses.  An example is that a subquery
 * with empty jointree previously could not be pulled up, because that would
 * have resulted in an empty relid set, making the subquery not uniquely
 * identifiable for join or PlaceHolderVar processing.
 *
 * Unlike most other functions in this file, this function doesn't recurse;
 * we rely on other processing to invoke it on sub-queries at suitable times.
 */
void replace_empty_jointree(Query *parse)
{
    RangeTblEntry *rte;
    Index           rti;
    RangeTblRef *rtr;

    /* Nothing to do if jointree is already nonempty */
    if (parse->jointree->fromlist != NIL)
        return;

    /* We mustn't change it in the top level of a setop tree, either */
    if (parse->setOperations)
        return;

    /* Create suitable RTE */
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RESULT;
    rte->eref = makeAlias("*RESULT*", NIL);

    /* Add it to rangetable */
    parse->rtable = lappend(parse->rtable, rte);
    rti = list_length(parse->rtable);

    /* And jam a reference into the jointree */
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = rti;
    parse->jointree->fromlist = list_make1(rtr);
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * helper function to check if SWCB ctes contaisn in current SubQuery, normally help us to
 * idenfity if it is OK to appy SWCB related optimization steps
 */
static bool contains_swctes(const PlannerInfo *root)
{
    if (root->parse == NULL || root->parse->cteList == NIL) {
        return false;
    }

    List     *cteList = root->parse->cteList;
    ListCell *lc = NULL;
    bool      found = false;
    foreach(lc, cteList) {
        CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);

        /* check if cte from parse->ctelist is a swcb converted */
        if (cte->swoptions != NULL) {
            found = true;
            break;
        }
    }

    return found;
}
#endif

/*
 * pull_up_sublinks
 *		Attempt to pull up ANY and EXISTS SubLinks to be treated as
 *		semijoins or anti-semijoins.
 *
 * A clause "foo op ANY (sub-SELECT)" can be processed by pulling the
 * sub-SELECT up to become a rangetable entry and treating the implied
 * comparisons as quals of a semijoin.	However, this optimization *only*
 * works at the top level of WHERE or a JOIN/ON clause, because we cannot
 * distinguish whether the ANY ought to return FALSE or NULL in cases
 * involving NULL inputs.  Also, in an outer join's ON clause we can only
 * do this if the sublink is degenerate (ie, references only the nullable
 * side of the join).  In that case it is legal to push the semijoin
 * down into the nullable side of the join.  If the sublink references any
 * nonnullable-side variables then it would have to be evaluated as part
 * of the outer join, which makes things way too complicated.
 *
 * Under similar conditions, EXISTS and NOT EXISTS clauses can be handled
 * by pulling up the sub-SELECT and creating a semijoin or anti-semijoin.
 *
 * This routine searches for such clauses and does the necessary parsetree
 * transformations if any are found.
 *
 * This routine has to run before preprocess_expression(), so the quals
 * clauses are not yet reduced to implicit-AND format.	That means we need
 * to recursively search through explicit AND clauses, which are
 * probably only binary ANDs.  We stop as soon as we hit a non-AND item.
 */
void pull_up_sublinks(PlannerInfo* root)
{
    Node* jtnode = NULL;
    Relids relids;

#ifndef ENABLE_MULTIPLE_NODES
    /* if quals include rownum, forbid pulling up sublinks */
    if (find_rownum_in_quals(root)) {
        return;
    }

    /* check existance of SWCB converted */
    if (contains_swctes(root)) {
        return;
    }
#endif

    /* Begin recursion through the jointree */
    jtnode = pull_up_sublinks_jointree_recurse(root, (Node*)root->parse->jointree, &relids);

    /*
     * root->parse->jointree must always be a FromExpr, so insert a dummy one
     * if we got a bare RangeTblRef or JoinExpr out of the recursion.
     */
    if (IsA(jtnode, FromExpr))
        root->parse->jointree = (FromExpr*)jtnode;
    else
        root->parse->jointree = makeFromExpr(list_make1(jtnode), NULL);

    if ((u_sess->attr.attr_sql.rewrite_rule & SUBLINK_PULLUP_IN_TARGETLIST) && permit_from_rewrite_hint(root, SUBLINK_PULLUP_IN_TARGETLIST)) {
        /* Begin recursion through the targetlist */
        Node *newTargetList = NULL;
        Node *whereQuals = root->parse->jointree->quals;
        jtnode = pull_up_sublinks_targetlist(root,
                                       (Node *)root->parse->targetList,
                                       (Node *)root->parse->jointree,
                                       &relids,
                                       &newTargetList,
                                       whereQuals);
    
        /*
         * root->parse->jointree must always be a FromExpr, so insert a dummy one
         * if we got a bare RangeTblRef or JoinExpr out of the recursion.
         */
        if (newTargetList != NULL)
            root->parse->targetList = (List *)replace_node_clause((Node *)root->parse->targetList,
                                    (Node *)root->parse->targetList, newTargetList, RNC_REPLACE_FIRST_ONLY);
        if (IsA(jtnode, FromExpr))
            root->parse->jointree = (FromExpr *) jtnode;
        else
            root->parse->jointree = makeFromExpr(list_make1(jtnode), NULL);
    }

}

/*
 * @Description: Put this qual to correct place.
 * @in new_node - New node sublink pull up after.
 * @in old_node - Old node sublink pull up before.
 * @in qual - Need set qual clause.
 * @in old_node_relids - old node available relids.
 * @return - new node.
 */
Node* assign_qual_clause(Node* new_node, Node* old_node, Node* qual, Relids old_node_relids)
{
    if (qual == NULL) {
        return new_node;
    }
    Relids qual_varnos = pull_varnos(qual);
    /*
     * We need add this quals to new_node if old_node_relids can not include qual_varnos.
     * than can happend when or_clause pull up.
     */
    if (!bms_is_subset(qual_varnos, old_node_relids)) {
        if (IsA(new_node, FromExpr)) {
            ((FromExpr*)new_node)->quals = qual;
        } else {
            new_node = (Node*)makeFromExpr(list_make1(new_node), qual);
        }
    }
    /* Only need put qual to old node, this qual can be original(pull up before) qual.*/
    else {
        if (IsA(old_node, FromExpr)) {
            ((FromExpr*)old_node)->quals = qual;
        } else {
            ((JoinExpr*)old_node)->quals = qual;
        }
    }

    bms_free_ext(qual_varnos);

    return new_node;
}

/*
 * Recurse through jointree nodes for pull_up_sublinks()
 *
 * In addition to returning the possibly-modified jointree node, we return
 * a relids set of the contained rels into *relids.
 */
static Node* pull_up_sublinks_jointree_recurse(PlannerInfo* root, Node* jtnode, Relids* relids, Node* all_quals)
{
    if (jtnode == NULL) {
        *relids = NULL;
    } else if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;

        *relids = bms_make_singleton(varno);
        /* jtnode is returned unmodified */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        List* newfromlist = NIL;
        Relids frelids = NULL;
        FromExpr* newf = NULL;
        Node* jtlink = NULL;
        ListCell* l = NULL;
        Node* qual = NULL;
        Relids current_relids = NULL;

        /* First, recurse to process children and collect their relids */
        foreach (l, f->fromlist) {
            Node* newchild = NULL;
            Relids childrelids;

            newchild = pull_up_sublinks_jointree_recurse(root, (Node*)lfirst(l), &childrelids, f->quals);
            newfromlist = lappend(newfromlist, newchild);

            /* Keep current available rtindex. */
            current_relids = bms_union(current_relids, childrelids);

            frelids = bms_join(frelids, childrelids);
        }
        /* Build the replacement FromExpr; no quals yet */
        newf = makeFromExpr(newfromlist, NULL);
        /* Set up a link representing the rebuilt jointree */
        jtlink = (Node*)newf;
        /* Now process qual --- all children are available for use */
        qual = pull_up_sublinks_qual_recurse(root, f->quals, &jtlink, &frelids, NULL, NULL, f->quals);

        /*
         * Put this qual to correct place. It is mean this qual only need put to
         * original FromExpr, also can put to new generate jtlink, e.g. or_clause pull up.
         *
         * For examlpe:
         * select * from t1 where exists (select a from t2 where t1.a = t2.a) or
         * exists (select b from t3 where t1.b = t3.b);
         *
         * This query be equivalent to:
         *
         * select t1.* from t1 left join (select t2.a from t2 group by t2.a) as tt1 on (t1.a = tt1.a)
         * left join (select t3.b from t3 group by t3.a) as tt2 on (tt2.b = t1.b)
         * where tt1.a is not null or tt2.b is not null;
         *
         * This case mean qual 'tt1.a is not null or tt2.b is not null' need put to behind all join
         * instead of behind table t1.
         */
        jtlink = assign_qual_clause(jtlink, (Node*)newf, qual, current_relids);

        /*
         * Note that the result will be either newf, or a stack of JoinExprs
         * with newf at the base.  We rely on subsequent optimization steps to
         * flatten this and rearrange the joins as needed.
         *
         * Although we could include the pulled-up subqueries in the returned
         * relids, there's no need since upper quals couldn't refer to their
         * outputs anyway.
         */
        *relids = frelids;
        jtnode = jtlink;
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = NULL;
        Relids leftrelids;
        Relids rightrelids;
        Node* jtlink = NULL;
        Node* quals = NULL;

        /*
         * Make a modifiable copy of join node, but don't bother copying its
         * subnodes (yet).
         */
        j = (JoinExpr*)palloc(sizeof(JoinExpr));
        errno_t errorno = memcpy_s(j, sizeof(JoinExpr), jtnode, sizeof(JoinExpr));
        securec_check(errorno, "", "");

        jtlink = (Node*)j;

        /* Recurse to process children and collect their relids */
        j->larg = pull_up_sublinks_jointree_recurse(root, j->larg, &leftrelids);
        j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &rightrelids);

        Relids union_relids = bms_union(leftrelids, rightrelids);

        /*
         * Now process qual, showing appropriate child relids as available,
         * and attach any pulled-up jointree items at the right place. In the
         * inner-join case we put new JoinExprs above the existing one (much
         * as for a FromExpr-style join).  In outer-join cases the new
         * JoinExprs must go into the nullable side of the outer join. The
         * point of the available_rels machinations is to ensure that we only
         * pull up quals for which that's okay.
         *
         * We don't expect to see any pre-existing JOIN_SEMI or JOIN_ANTI
         * nodes here.
         */
        switch (j->jointype) {
            case JOIN_INNER:
                quals = pull_up_sublinks_qual_recurse(
                    root, j->quals, &jtlink, &union_relids, NULL, NULL, all_quals);

                /*
                 * To inner join, we need special manage this quals, else can lead to error
                 * when or_clause be pulled up.
                 * For example:
                 * select 1 from t1 inner join t2 on (exists (select * from t3 where t1.a = t3.a)
                 * or exists (select * from t4 where t2.a = t4.a));
                 *
                 * This query be equivalent to:
                 *
                 * select 1 from t1 inner join t2 on (1 = 1)
                 * left join (select t3.a from t3 group by t3.a) as tt3 on (t1.a = t3.a)
                 * left join (select t4.a from t4 group by t4.a) as tt4 on t2.a = t4.a)
                 * where tt3.a is not null or tt4.a is not null;
                 *
                 * To this query, return quals is 'tt3.a is not null or tt4.a is not null',
                 * this qual need put to behind all join, can not put to behind t1 join t2 that will lead
                 * to error(JOIN qualification cannot refer to other relations) because can not find tt3.a and tt4.a.
                 */
                j->quals = NULL;
                jtlink = assign_qual_clause(jtlink, (Node*)j, quals, bms_union(leftrelids, rightrelids));
                break;
            case JOIN_LEFT:
            case JOIN_LEFT_ANTI_FULL:
                j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg, &rightrelids, NULL, NULL);
                break;
            case JOIN_FULL:
            case JOIN_SEMI:
            case JOIN_ANTI:
                if (u_sess->attr.attr_common.log_min_messages <= DEBUG2) {
                    ereport(DEBUG2, (errmodule(MOD_OPT_REWRITE),
                        errmsg("We record join type when semi_join=4 anti_join=5: %d", j->jointype)));
                }
                /* can't do anything with full-join quals */
                break;
            case JOIN_RIGHT:
            case JOIN_RIGHT_ANTI_FULL:
                j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->larg, &leftrelids, NULL, NULL);
                break;
            default: {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized join type: %d", (int)j->jointype)));
            } break;
        }

        /*
         * Although we could include the pulled-up subqueries in the returned
         * relids, there's no need since upper quals couldn't refer to their
         * outputs anyway.	But we *do* need to include the join's own rtindex
         * because we haven't yet collapsed join alias variables, so upper
         * levels would mistakenly think they couldn't use references to this
         * join.
         */
        *relids = bms_join(leftrelids, rightrelids);
        if (j->rtindex)
            *relids = bms_add_member(*relids, j->rtindex);
        jtnode = jtlink;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
    return jtnode;
}

/*
 * Recurse through top-level qual nodes for pull_up_sublinks()
 *
 * jtlink1 points to the link in the jointree where any new JoinExprs should
 * be inserted if they reference available_rels1 (i.e., available_rels1
 * denotes the relations present underneath jtlink1).  Optionally, jtlink2 can
 * point to a second link where new JoinExprs should be inserted if they
 * reference available_rels2 (pass NULL for both those arguments if not used).
 * Note that SubLinks referencing both sets of variables cannot be optimized.
 * If we find multiple pull-up-able SubLinks, they'll get stacked onto jtlink1
 * and/or jtlink2 in the order we encounter them.  We rely on subsequent
 * optimization to rearrange the stack if appropriate.
 *
 * Returns the replacement qual node, or NULL if the qual should be removed.
 */
static Node* pull_up_sublinks_qual_recurse(PlannerInfo* root, Node* node, Node** jtlink1, Relids *available_rels1,
    Node** jtlink2, Relids *available_rels2, Node* all_quals)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, SubLink)) {
        SubLink* sublink = (SubLink*)node;
        JoinExpr* j = NULL;
        Relids child_rels;

        if (has_no_expand_hint((Query*)sublink->subselect)) {
            return node;
        }

        /* Is it a convertible ANY or EXISTS clause? */
        if (sublink->subLinkType == ANY_SUBLINK) {
            if ((j = convert_ANY_sublink_to_join(root, sublink, false, *available_rels1)) != NULL) {
                /* Yes; insert the new join node into the join tree */
                j->larg = *jtlink1;
                *jtlink1 = (Node*)j;
                /* Recursively process pulled-up jointree nodes */
                j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                /*
                 * Now recursively process the pulled-up quals.  Any inserted
                 * joins can get stacked onto either j->larg or j->rarg,
                 * depending on which rels they reference.
                 */
                j->quals =
                    pull_up_sublinks_qual_recurse(root, j->quals, &j->larg, available_rels1, &j->rarg, &child_rels);
                /* Return NULL representing constant TRUE */
                return NULL;
            }
            if (available_rels2 != NULL &&
                (j = convert_ANY_sublink_to_join(root, sublink, false, *available_rels2)) != NULL) {
                /* Yes; insert the new join node into the join tree */
                j->larg = *jtlink2;
                *jtlink2 = (Node*)j;
                /* Recursively process pulled-up jointree nodes */
                j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                /*
                 * Now recursively process the pulled-up quals.  Any inserted
                 * joins can get stacked onto either j->larg or j->rarg,
                 * depending on which rels they reference.
                 */
                j->quals =
                    pull_up_sublinks_qual_recurse(root, j->quals, &j->larg, available_rels2, &j->rarg, &child_rels);
                /* Return NULL representing constant TRUE */
                return NULL;
            }
        } else if (sublink->subLinkType == EXISTS_SUBLINK) {
            if ((j = convert_EXISTS_sublink_to_join(root, sublink, false, *available_rels1)) != NULL) {
                /* Yes; insert the new join node into the join tree */
                j->larg = *jtlink1;
                *jtlink1 = (Node*)j;
                /* Recursively process pulled-up jointree nodes */
                j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                /*
                 * Now recursively process the pulled-up quals.  Any inserted
                 * joins can get stacked onto either j->larg or j->rarg,
                 * depending on which rels they reference.
                 */
                j->quals =
                    pull_up_sublinks_qual_recurse(root, j->quals, &j->larg, available_rels1, &j->rarg, &child_rels);
                /* Return NULL representing constant TRUE */
                return NULL;
            }
            if (available_rels2 != NULL &&
                (j = convert_EXISTS_sublink_to_join(root, sublink, false, *available_rels2)) != NULL) {
                /* Yes; insert the new join node into the join tree */
                j->larg = *jtlink2;
                *jtlink2 = (Node*)j;
                /* Recursively process pulled-up jointree nodes */
                j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                /*
                 * Now recursively process the pulled-up quals.  Any inserted
                 * joins can get stacked onto either j->larg or j->rarg,
                 * depending on which rels they reference.
                 */
                j->quals =
                    pull_up_sublinks_qual_recurse(root, j->quals, &j->larg, available_rels2, &j->rarg, &child_rels);
                /* Return NULL representing constant TRUE */
                return NULL;
            }
        }
        return node;
    }
    if (not_clause(node)) {
        /* If the immediate argument of NOT is EXISTS, try to convert */
        SubLink* sublink = (SubLink*)get_notclausearg((Expr*)node);
        JoinExpr* j = NULL;
        Relids child_rels;

        if (sublink && IsA(sublink, SubLink)) {
            if (sublink->subLinkType == EXISTS_SUBLINK) {
                if ((j = convert_EXISTS_sublink_to_join(root, sublink, true, *available_rels1)) != NULL) {
                    /* Yes; insert the new join node into the join tree */
                    j->larg = *jtlink1;
                    *jtlink1 = (Node*)j;
                    /* Recursively process pulled-up jointree nodes */
                    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                    /*
                     * Now recursively process the pulled-up quals.  Because
                     * we are underneath a NOT, we can't pull up sublinks that
                     * reference the left-hand stuff, but it's still okay to
                     * pull up sublinks referencing j->rarg.
                     */
                    j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg, &child_rels, NULL, NULL);
                    /* Return NULL representing constant TRUE */
                    return NULL;
                }
                if (available_rels2 != NULL &&
                    (j = convert_EXISTS_sublink_to_join(root, sublink, true, *available_rels2)) != NULL) {
                    /* Yes; insert the new join node into the join tree */
                    j->larg = *jtlink2;
                    *jtlink2 = (Node*)j;
                    /* Recursively process pulled-up jointree nodes */
                    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                    /*
                     * Now recursively process the pulled-up quals.  Because
                     * we are underneath a NOT, we can't pull up sublinks that
                     * reference the left-hand stuff, but it's still okay to
                     * pull up sublinks referencing j->rarg.
                     */
                    j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg, &child_rels, NULL, NULL);
                    /* Return NULL representing constant TRUE */
                    return NULL;
                }
            } else if (sublink->subLinkType == ANY_SUBLINK) { /*support convert not any/in to antijoin*/
                if ((j = convert_ANY_sublink_to_join(root, sublink, true, *available_rels1)) != NULL) {
                    /* Now insert the new join node into the join tree */
                    j->larg = *jtlink1;
                    *jtlink1 = (Node*)j;

                    /* Recursively process pulled-up jointree nodes */
                    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                    /*
                     * Now recursively process the pulled-up quals.  Because
                     * we are underneath a NOT, we can't pull up sublinks that
                     * reference the left-hand stuff, but it's still okay to
                     * pull up sublinks referencing j->rarg.
                     */
                    j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg, &child_rels, NULL, NULL);

                    /* and return NULL representing constant TRUE */
                    return NULL;
                }

                if (available_rels2 != NULL &&
                    (j = convert_ANY_sublink_to_join(root, sublink, true, *available_rels2)) != NULL) {
                    /* Yes; insert the new join node into the join tree */
                    j->larg = *jtlink2;
                    *jtlink2 = (Node*)j;
                    /* Recursively process pulled-up jointree nodes */
                    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

                    /*
                     * Now recursively process the pulled-up quals.  Because
                     * we are underneath a NOT, we can't pull up sublinks that
                     * reference the left-hand stuff, but it's still okay to
                     * pull up sublinks referencing j->rarg.
                     */
                    j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg, &child_rels, NULL, NULL);
                    /* Return NULL representing constant TRUE */
                    return NULL;
                }
            }
        }
        return node;
    }
    if (and_clause(node)) {
        /* Recurse into AND clause */
        List* newclauses = NIL;
        ListCell* l = NULL;

        foreach (l, ((BoolExpr*)node)->args) {
            Node* oldclause = (Node*)lfirst(l);
            Node* newclause = NULL;

            newclause = pull_up_sublinks_qual_recurse(
                root, oldclause, jtlink1, available_rels1, jtlink2, available_rels2, all_quals);
            if (newclause != NULL)
                newclauses = lappend(newclauses, newclause);
        }
        /* We might have got back fewer clauses than we started with */
        if (newclauses == NIL)
            return NULL;
        else if (list_length(newclauses) == 1)
            return (Node*)linitial(newclauses);
        else
            return (Node*)make_andclause(newclauses);
    }

    /* convert or_clause to left join. */
    if (or_clause(node)) {
        convert_ORCLAUSE_to_join(root, (BoolExpr*)node, jtlink1, available_rels1);

        if (available_rels2 != NULL) {
            convert_ORCLAUSE_to_join(root, (BoolExpr *)node, jtlink2, available_rels2);
        }

        return node;
    }

    if(IsA(node, OpExpr) || IsA(node, NullTest))
    {
        List *sublinkList = pull_sublink(node, 0, false);

        /*Try to pull up these sublink*/
        if(sublinkList != NULL) {
            ListCell *lc = NULL;
            foreach(lc, sublinkList) {
                Node *sublink = (Node*)lfirst(lc);

                if (unlikely(!(IsA(sublink, SubLink))))
                    ereport(ERROR,
                        (errmodule(MOD_OPT_REWRITE),
                            errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("Node should be SubLink in pull_up_sublinks_qual_recurse")));

                /*Return value is changed opexpr*/
                node = convert_EXPR_sublink_to_join(root, jtlink1,
                                        node,
                                        (SubLink*)sublink,
                                        available_rels1,
                                        all_quals);
                if (available_rels2 != NULL) {
                    node = convert_EXPR_sublink_to_join(root, jtlink2,
                                            node,
                                            (SubLink*)sublink,
                                            available_rels2,
                                            all_quals);
                }
            }
        }

        return node;
    }


    /* Stop if not an AND */
    return node;
}

/*
 * pull_up_sublinks_targetlist
 *  sublink pull up logic for sublinks in targetlist
 *
 * Parameters:
 *  @in root: planner info of current query level
 *  @in node: list of targetlist expression
 *  @in jtnode: from table structure
 *  @in relids: bitmap of relids that is visible to sublink
 *  @in/out newTargetList: converted targetlist
 *  @out: whereQuals: the where quals
 *
 * Returns: new formed from table structure
 */
static Node* pull_up_sublinks_targetlist(PlannerInfo *root,
                            Node *node,
                            Node *jtnode,
                            Relids *relids,
                            Node **newTargetList,
                            Node *whereQuals)
{
    List *sublinkName = pull_sublink(node, 0, true);
    List *sublinkList = pull_sublink(node, 0, false);
    ListCell *lc = NULL;
    ListCell *lc1 = NULL;

    /* Try to pull up all sublinks got from targetlist */
    forboth(lc, sublinkList, lc1, sublinkName)
    {
        SubLink *sublink = (SubLink*) lfirst(lc);
        if (has_no_expand_hint((Query*)sublink->subselect)) {
            continue;
        }
        char *resname = (char*) lfirst(lc1);
        Assert(IsA(sublink, SubLink));

        /* Currently we only support expr sublink */
        if (sublink->subLinkType == EXPR_SUBLINK) {

            /*
            * Return value is changed opexpr. Since we only modify
            * the members of opExpr, so the return value can be ignored
            */
            node = convert_EXPR_sublink_to_join(root, &jtnode,
                                            node,
                                            (SubLink*)sublink,
                                            relids,
                                            whereQuals, resname);
        }
        /* To do: handle of other sublink type */
    }

    /*
     * The changed targetlist is returned to replace the original targetList.
     * In this way, the sublink is excluded.
     */
    *newTargetList = node;

  return jtnode;
}

/*
 * inline_set_returning_functions
 *		Attempt to "inline" set-returning functions in the FROM clause.
 *
 * If an RTE_FUNCTION rtable entry invokes a set-returning function that
 * contains just a simple SELECT, we can convert the rtable entry to an
 * RTE_SUBQUERY entry exposing the SELECT directly.  This is especially
 * useful if the subquery can then be "pulled up" for further optimization,
 * but we do it even if not, to reduce executor overhead.
 *
 * This has to be done before we have started to do any optimization of
 * subqueries, else any such steps wouldn't get applied to subqueries
 * obtained via inlining.  However, we do it after pull_up_sublinks
 * so that we can inline any functions used in SubLink subselects.
 *
 * Like most of the planner, this feels free to scribble on its input data
 * structure.
 */
void inline_set_returning_functions(PlannerInfo* root)
{
    ListCell* rt = NULL;

    foreach (rt, root->parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);

        if (rte->rtekind == RTE_FUNCTION) {
            Query* funcquery = NULL;

            /* Check safety of expansion, and expand if possible */
            funcquery = inline_set_returning_function(root, rte);
            if (funcquery != NULL) {
                /* Successful expansion, replace the rtable entry */
                rte->rtekind = RTE_SUBQUERY;
                rte->subquery = funcquery;
                rte->funcexpr = NULL;
                rte->funccoltypes = NIL;
                rte->funccoltypmods = NIL;
                rte->funccolcollations = NIL;
            }
        }
    }
}

/*
 * This recursively processes the jointree and returns a modified jointree.
 */
Node *
pull_up_subqueries(PlannerInfo *root, Node *jtnode)
{
   /* Start off with no containing join nor appendrel */
   return pull_up_subqueries_recurse(root, jtnode, NULL, NULL, NULL);
}

/*
 * pull_up_subqueries_recurse
 *		Recursive guts of pull_up_subqueries.
 *
 * This recursively processes the jointree and returns a modified jointree.
 *
 * If this jointree node is within either side of an outer join, then
 * lowest_outer_join references the lowest such JoinExpr node; otherwise
 * it is NULL.  We use this to constrain the effects of LATERAL subqueries.
 *
 * If this jointree node is within the nullable side of an outer join, then
 * lowest_nulling_outer_join references the lowest such JoinExpr node;
 * otherwise it is NULL.  This forces use of the PlaceHolderVar mechanism for
 * references to non-nullable targetlist items, but only for references above
 * that join.
 *
 * If we are looking at a member subquery of an append relation,
 * containing_appendrel describes that relation; else it is NULL.
 * This forces use of the PlaceHolderVar mechanism for all non-Var targetlist
 * items, and puts some additional restrictions on what can be pulled up.
 *
 * A tricky aspect of this code is that if we pull up a subquery we have
 * to replace Vars that reference the subquery's outputs throughout the
 * parent query, including quals attached to jointree nodes above the one
 * we are currently processing!  We handle this by being careful to maintain
 * validity of the jointree structure while recursing, in the following sense:
 * whenever we recurse, all qual expressions in the tree must be reachable
 * from the top level, in case the recursive call needs to modify them.
 *
 * Notice also that we can't turn pullup_replace_vars loose on the whole
 * jointree, because it'd return a mutated copy of the tree; we have to
 * invoke it just on the quals, instead.  This behavior is what makes it
 * reasonable to pass lowest_outer_join and lowest_nulling_outer_join as
 * pointers rather than some more-indirect way of identifying the lowest
 * OJs.  Likewise, we don't replace append_rel_list members but only their
 * substructure, so the containing_appendrel reference is safe to use.
 */
Node* pull_up_subqueries_recurse(
    PlannerInfo* root, Node* jtnode, JoinExpr* lowest_outer_join, JoinExpr *lowest_nulling_outer_join,
    AppendRelInfo* containing_appendrel)
{
    if (jtnode == NULL)
        return NULL;
#ifndef ENABLE_MULTIPLE_NODES
    /* if quals include rownum, set hasRownumQual to true */
    if(find_rownum_in_quals(root)) {
        root->hasRownumQual = true;
    }
#endif
    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;
        RangeTblEntry* rte = rt_fetch(varno, root->parse->rtable);

        /* Init flag */
        rte->subquery_pull_up = false;

        /*
         * Is this a subquery RTE, and if so, is the subquery simple enough to
         * pull up?
         *
         * If we are looking at an append-relation member, we can't pull it up
         * unless is_safe_append_member says so.
         */
        if (rte->rtekind == RTE_SUBQUERY && is_simple_subquery(rte->subquery, rte, lowest_outer_join) &&
            (containing_appendrel == NULL || is_safe_append_member(rte->subquery)))
            return pull_up_simple_subquery(root, jtnode, rte, lowest_outer_join, lowest_nulling_outer_join, containing_appendrel);

        /*
         * Alternatively, is it a simple UNION ALL subquery?  If so, flatten
         * into an "append relation".
         *
         * It's safe to do this regardless of whether this query is itself an
         * appendrel member.  (If you're thinking we should try to flatten the
         * two levels of appendrel together, you're right; but we handle that
         * in set_append_rel_pathlist, not here.)
         *
         * In multi-node group scenario, we should not pull up union all because
         * the branchs may be in different node group, we could not determine the
         * group for append path
         */
        if (rte->rtekind == RTE_SUBQUERY && is_simple_union_all(rte->subquery) &&
#ifndef ENABLE_MULTIPLE_NODES
            !root->hasRownumQual &&
#endif
            (!ng_is_multiple_nodegroup_scenario()))
            return pull_up_simple_union_all(root, jtnode, rte);

        /* Otherwise, do nothing at this node. */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        Assert(containing_appendrel == NULL);
        foreach (l, f->fromlist)
            lfirst(l) = pull_up_subqueries_recurse(root, (Node*)lfirst(l), lowest_outer_join, lowest_nulling_outer_join, NULL);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        Assert(containing_appendrel == NULL);
        /* Recurse, being careful to tell myself when inside outer join */
        switch (j->jointype) {
            case JOIN_INNER:
                j->larg = pull_up_subqueries_recurse(root, j->larg, lowest_outer_join, lowest_nulling_outer_join, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, lowest_outer_join, lowest_nulling_outer_join, NULL);
                break;
            case JOIN_LEFT:
            case JOIN_SEMI:
            case JOIN_ANTI:
            case JOIN_LEFT_ANTI_FULL:
                j->larg = pull_up_subqueries_recurse(root, j->larg, lowest_outer_join, lowest_nulling_outer_join, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, j, j, NULL);
                break;
            case JOIN_FULL:
                j->larg = pull_up_subqueries_recurse(root, j->larg, j, j, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, j, j, NULL);
                break;
            case JOIN_RIGHT:
            case JOIN_RIGHT_ANTI_FULL:
                j->larg = pull_up_subqueries_recurse(root, j->larg, j, j, NULL);
                j->rarg = pull_up_subqueries_recurse(root, j->rarg, lowest_outer_join, lowest_nulling_outer_join, NULL);
                break;
            default: {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized join type: %d", (int)j->jointype)));
            } break;
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
    return jtnode;
}

/*
 * @Description: Pull up subquery's hint.
 * @in root: query level info.
 * @in parse: Parent query.
 * @in hint_state: Subquery's hint.
 */
void pull_up_subquery_hint(PlannerInfo* root, Query* parse, HintState* hint_state)
{
    if (parse->hintState == NULL) {
        parse->hintState = HintStateCreate();
    }

    parse->hintState->join_hint = list_concat(parse->hintState->join_hint, hint_state->join_hint);
    parse->hintState->leading_hint = list_concat(parse->hintState->leading_hint, hint_state->leading_hint);
    parse->hintState->row_hint = list_concat(parse->hintState->row_hint, hint_state->row_hint);
    parse->hintState->stream_hint = list_concat(parse->hintState->stream_hint, hint_state->stream_hint);
    parse->hintState->scan_hint = list_concat(parse->hintState->scan_hint, hint_state->scan_hint);
    parse->hintState->hint_warning = list_concat(parse->hintState->hint_warning, hint_state->hint_warning);
    parse->hintState->skew_hint = list_concat(parse->hintState->skew_hint, hint_state->skew_hint);
    parse->hintState->nall_hints = parse->hintState->nall_hints + hint_state->nall_hints;
    parse->hintState->multi_node_hint = hint_state->multi_node_hint;
    /* no_expand hint, set hint, no_gpc hint should not be pulled up */

    if (hint_state->block_name_hint) {
        BlockNameHint* blockNameHint = (BlockNameHint*)linitial(hint_state->block_name_hint);

        append_warning_to_list(
            root, (Hint*)blockNameHint, "Hint%s will become invalid due to sub-query pulling up.", hint_string);

        hintDelete((Hint*)blockNameHint);
        list_free_ext(hint_state->block_name_hint);
        hint_state->block_name_hint = NIL;
    }
}

static bool is_subquery_partial_push(PlannerInfo* root, RangeTblEntry* rte)
{

    /* parent cannot push and subquery can push */
    if (IS_STREAM_PLAN && !root->parse->can_push && rte->subquery->can_push) {
        return true;
    }

    return false;
}

/*
 * pull_up_simple_subquery
 *		Attempt to pull up a single simple subquery.
 *
 * jtnode is a RangeTblRef that has been tentatively identified as a simple
 * subquery by pull_up_subqueries.	We return the replacement jointree node,
 * or jtnode itself if we determine that the subquery can't be pulled up after
 * all.
 *
 * rte is the RangeTblEntry referenced by jtnode.  Remaining parameters are
 * as for pull_up_subqueries_recurse.
 */
static Node* pull_up_simple_subquery(PlannerInfo* root, Node* jtnode, RangeTblEntry* rte, JoinExpr* lowest_outer_join,
    JoinExpr *lowest_nulling_outer_join, AppendRelInfo* containing_appendrel)
{
    Query* parse = root->parse;
    int varno = ((RangeTblRef*)jtnode)->rtindex;
    Query* subquery = NULL;
    PlannerInfo* subroot = NULL;
    int rtoffset;
    pullup_replace_vars_context rvcontext;
    ListCell* lc = NULL;

    if (is_subquery_partial_push(root, rte)) {
        return jtnode;
    }

    /*
     * Need a modifiable copy of the subquery to hack on.  Even if we didn't
     * sometimes choose not to pull up below, we must do this to avoid
     * problems if the same subquery is referenced from multiple jointree
     * items (which can't happen normally, but might after rule rewriting).
     */
    subquery = (Query*)copyObject(rte->subquery);

    /*
     * Create a PlannerInfo data structure for this subquery.
     *
     * NOTE: the next few steps should match the first processing in
     * subquery_planner().	Can we refactor to avoid code duplication, or
     * would that just make things uglier?
     */
    subroot = makeNode(PlannerInfo);
    subroot->parse = subquery;
    subroot->glob = root->glob;
    /*
     * Set simple subquery as subquery of upper level query and change
     * to the same level after pullup
     */
    subroot->query_level = root->query_level + 1;
    subroot->parent_root = root;
    subroot->plan_params = NIL;
    subroot->planner_cxt = CurrentMemoryContext;
    subroot->init_plans = NIL;
    subroot->cte_plan_ids = NIL;
    subroot->eq_classes = NIL;
    subroot->append_rel_list = NIL;
    subroot->rowMarks = NIL;
    subroot->hasRecursion = false;
    subroot->qualSecurityLevel = 0;
    subroot->wt_param_id = -1;
    subroot->non_recursive_plan = NULL;
    subroot->hasRownumQual = root->hasRownumQual;

    /* No CTEs to worry about */
    Assert(subquery->cteList == NIL);

    /*
     * If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE, so
     * that we don't need so many special cases to deal with that situation.
     */
    replace_empty_jointree(subquery);

    /*
     * Pull up any SubLinks within the subquery's quals, so that we don't
     * leave unoptimized SubLinks behind.
     */
    if (subquery->hasSubLinks)
        pull_up_sublinks(subroot);

    /*
     * Similarly, inline any set-returning functions in its rangetable.
     */
    inline_set_returning_functions(subroot);

    /*
     * Recursively pull up the subquery's subqueries, so that
     * pull_up_subqueries' processing is complete for its jointree and
     * rangetable.
     *
     * Note: we should pass NULL for containing-join info even if we are
     * within an outer join in the upper query; the lower query starts with a
     * clean slate for outer-join semantics.  Likewise, we say we aren't
     * handling an appendrel member.
     */
    subquery->jointree = (FromExpr*)pull_up_subqueries_recurse(subroot, (Node*)subquery->jointree, NULL, NULL, NULL);

    /* restore the levels back */
    subroot->query_level--;
    subroot->parent_root = root->parent_root;

    /*
     * Now we must recheck whether the subquery is still simple enough to pull
     * up.	If not, abandon processing it.
     *
     * We don't really need to recheck all the conditions involved, but it's
     * easier just to keep this "if" looking the same as the one in
     * pull_up_subqueries_recurse.
     */
    if (subquery->jointree == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("jointree in subquery could not be NULL")));
    }
    if (is_simple_subquery(subquery, rte, lowest_outer_join) &&
        (containing_appendrel == NULL || is_safe_append_member(subquery)) &&
        !has_no_expand_hint(subquery)) {
        /* good to go */
    } else {
        /*
         * Give up, return unmodified RangeTblRef.
         *
         * Note: The work we just did will be redone when the subquery gets
         * planned on its own.	Perhaps we could avoid that by storing the
         * modified subquery back into the rangetable, but I'm not gonna risk
         * it now.
         */
        return jtnode;
    }

    /*
     * We must flatten any join alias Vars in the subquery's targetlist,
     * because pulling up the subquery's subqueries might have changed their
     * expansions into arbitrary expressions, which could affect
     * pullup_replace_vars' decisions about whether PlaceHolderVar wrappers
     * are needed for tlist entries.  (Likely it'd be better to do
     * flatten_join_alias_vars on the whole query tree at some earlier stage,
     * maybe even in the rewriter; but for now let's just fix this case here.)
     */
    subquery->targetList = (List *) flatten_join_alias_vars(subroot, (Node *) subquery->targetList);

    /*
     * Adjust level-0 varnos in subquery so that we can append its rangetable
     * to upper query's.  We have to fix the subquery's append_rel_list as
     * well.
     */
    rtoffset = list_length(parse->rtable);
    OffsetVarNodes((Node*)subquery, rtoffset, 0);
    OffsetVarNodes((Node*)subroot->append_rel_list, rtoffset, 0);

    if (subquery->hintState) {
        /* pull up hint */
        pull_up_subquery_hint(root, parse, subquery->hintState);
    }

    /*
     * Upper-level vars in subquery are now one level closer to their parent
     * than before.
     */
    IncrementVarSublevelsUp((Node*)subquery, -1, 1);
    IncrementVarSublevelsUp((Node*)subroot->append_rel_list, -1, 1);

    /*
     * The subquery's targetlist items are now in the appropriate form to
     * insert into the top query, but if we are under an outer join then
     * non-nullable items may have to be turned into PlaceHolderVars.  If we
     * are dealing with an appendrel member then anything that's not a simple
     * Var has to be turned into a PlaceHolderVar.	Set up appropriate context
     * data for pullup_replace_vars.
     */
    rvcontext.root = root;
    rvcontext.targetlist = subquery->targetList;
    rvcontext.target_rte = rte;
    rvcontext.outer_hasSubLinks = &parse->hasSubLinks;
    rvcontext.varno = varno;
    rvcontext.need_phvs = (lowest_nulling_outer_join  != NULL || containing_appendrel != NULL);
    rvcontext.wrap_non_vars = (containing_appendrel != NULL);
    /* initialize cache array with indexes 0 .. length(tlist) */
    rvcontext.rv_cache = (Node**)palloc0((list_length(subquery->targetList) + 1) * sizeof(Node*));

    /*
     * Replace all of the top query's references to the subquery's outputs
     * with copies of the adjusted subtlist items, being careful not to
     * replace any of the jointree structure. (This'd be a lot cleaner if we
     * could use query_tree_mutator.)  We have to use PHVs in the targetList,
     * returningList, and havingQual, since those are certainly above any
     * outer join.	replace_vars_in_jointree tracks its location in the
     * jointree and uses PHVs or not appropriately.
     */
    parse->targetList = (List*)pullup_replace_vars((Node*)parse->targetList, &rvcontext);
    parse->returningList = (List*)pullup_replace_vars((Node*)parse->returningList, &rvcontext);
    if (parse->upsertClause != NULL) {
        parse->upsertClause->updateTlist = (List*)
            pullup_replace_vars((Node*)parse->upsertClause->updateTlist, &rvcontext);
    }

    replace_vars_in_jointree((Node*)parse->jointree, &rvcontext, lowest_nulling_outer_join);

    {
        ListCell* l = NULL;
        foreach (l, parse->mergeActionList) {
            MergeAction* action = (MergeAction*)lfirst(l);

            /*
             * We only replace targetlist and qual in pgxc and single node mode. For stream
             * plan, we don't replace the targetlist and qual, since during plan reference, it'll
             * referred to parse->mergeSourceTargetList, which is not replaced in this
             * procedure.
             * XXXX: Replace the mergeSourceTargetList and then targetlist and qual at the
             * same time. However, we need placeholder for this, since for sublink, we'll
             * generate different subplans (with different paramid) for different expression,
             * and it's also can't be referred.
             */
            if (!IS_PGXC_DATANODE && !IS_STREAM_PLAN && !IS_SINGLE_NODE) {
                action->targetList = (List*)pullup_replace_vars((Node*)action->targetList, &rvcontext);
                action->qual = pullup_replace_vars((Node*)action->qual, &rvcontext);
            } else {
                action->pulluped_targetList = (List*)pullup_replace_vars((Node*)action->targetList, &rvcontext);
            }
        }
    }

    Assert(parse->setOperations == NULL);
    parse->havingQual = pullup_replace_vars(parse->havingQual, &rvcontext);

    /*
     * Replace references in the translated_vars lists of appendrels. When
     * pulling up an appendrel member, we do not need PHVs in the list of the
     * parent appendrel --- there isn't any outer join between. Elsewhere, use
     * PHVs for safety.  (This analysis could be made tighter but it seems
     * unlikely to be worth much trouble.)
     */
    foreach (lc, root->append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(lc);
        bool save_need_phvs = rvcontext.need_phvs;

        if (appinfo == containing_appendrel)
            rvcontext.need_phvs = false;
        appinfo->translated_vars = (List*)pullup_replace_vars((Node*)appinfo->translated_vars, &rvcontext);
        rvcontext.need_phvs = save_need_phvs;
    }

    /*
     * Replace references in the joinaliasvars lists of join RTEs.
     *
     * You might think that we could avoid using PHVs for alias vars of joins
     * below lowest_outer_join, but that doesn't work because the alias vars
     * could be referenced above that join; we need the PHVs to be present in
     * such references after the alias vars get flattened.	(It might be worth
     * trying to be smarter here, someday.)
     */
    foreach (lc, parse->rtable) {
        RangeTblEntry* otherrte = (RangeTblEntry*)lfirst(lc);

        if (otherrte->rtekind == RTE_JOIN)
            otherrte->joinaliasvars = (List*)pullup_replace_vars((Node*)otherrte->joinaliasvars, &rvcontext);
    }

    /*
     * If the subquery had a LATERAL marker, propagate that to any of its
     * child RTEs that could possibly now contain lateral cross-references.
     * The children might or might not contain any actual lateral
     * cross-references, but we have to mark the pulled-up child RTEs so that
     * later planner stages will check for such.
     *
     * NB: although the parser only sets the lateral flag in subquery and
     * function RTEs, after this step it can also be set in VALUES RTEs.
     */
    if (rte->lateral)
    {
        foreach(lc, subquery->rtable)
        {
            RangeTblEntry *child_rte = (RangeTblEntry *) lfirst(lc);
    
            switch (child_rte->rtekind)
            {
                case RTE_SUBQUERY:
                case RTE_FUNCTION:
                case RTE_VALUES:
                    child_rte->lateral = true;
                    break;
                case RTE_RELATION:
                case RTE_JOIN:
                case RTE_CTE:
                case RTE_RESULT:
                case RTE_REMOTE_DUMMY:
                    /* these can't contain any lateral references */
                    break;
            }
        }
    }

    foreach(lc, subquery->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        rte->pulled_from_subquery = true;
    }

    /*
     * Now append the adjusted rtable entries to upper query. (We hold off
     * until after fixing the upper rtable entries; no point in running that
     * code on the subquery ones too.)
     */
    parse->rtable = list_concat(parse->rtable, subquery->rtable);

    /* Subquery can be pull, so set flag to true. */
    rte->subquery_pull_up = true;

    /*
     * Pull up any FOR UPDATE/SHARE markers, too.  (OffsetVarNodes already
     * adjusted the marker rtindexes, so just concat the lists.)
     */
    parse->rowMarks = list_concat(parse->rowMarks, subquery->rowMarks);

    /*
     * We also have to fix the relid sets of any PlaceHolderVar nodes in the
     * parent query.  (This could perhaps be done by pullup_replace_vars(),
     * but it seems cleaner to use two passes.)  Note in particular that any
     * PlaceHolderVar nodes just created by pullup_replace_vars() will be
     * adjusted, so having created them with the subquery's varno is correct.
     *
     * Likewise, relids appearing in AppendRelInfo nodes have to be fixed. We
     * already checked that this won't require introducing multiple subrelids
     * into the single-slot AppendRelInfo structs.
     */
    if (parse->hasSubLinks || root->glob->lastPHId != 0 || root->append_rel_list) {
        Relids subrelids;

        subrelids = get_relids_in_jointree((Node*)subquery->jointree, false);
        substitute_multiple_relids((Node*)parse, varno, subrelids);
        fix_append_rel_relids(root->append_rel_list, varno, subrelids);
    }

    /*
     * And now add subquery's AppendRelInfos to our list.
     */
    root->append_rel_list = list_concat(root->append_rel_list, subroot->append_rel_list);

    /*
     * We don't have to do the equivalent bookkeeping for outer-join info,
     * because that hasn't been set up yet.  placeholder_list likewise.
     */
    Assert(root->join_info_list == NIL);
    Assert(subroot->join_info_list == NIL);
    Assert(root->lateral_info_list == NIL);
    Assert(subroot->lateral_info_list == NIL);
    Assert(root->placeholder_list == NIL);
    Assert(subroot->placeholder_list == NIL);

    /*
     * Miscellaneous housekeeping.
     *
     * Although replace_rte_variables() faithfully updated parse->hasSubLinks
     * if it copied any SubLinks out of the subquery's targetlist, we still
     * could have SubLinks added to the query in the expressions of FUNCTION
     * and VALUES RTEs copied up from the subquery.  So it's necessary to copy
     * subquery->hasSubLinks anyway.  Perhaps this can be improved someday.
     */
    parse->hasSubLinks = parse->hasSubLinks || subquery->hasSubLinks;
    /* If subquery had any RLS conditions, now main query does too */
    parse->hasRowSecurity = parse->hasRowSecurity || subquery->hasRowSecurity;

    /*
     * subquery won't be pulled up if it hasAggs or hasWindowFuncs, so no work
     * needed on those flags
     *
     * Return the adjusted subquery jointree to replace the RangeTblRef entry
     * in parent's jointree; or, if the FromExpr is degenerate, just return
     * its single member.
     */
    Assert(IsA(subquery->jointree, FromExpr));
    Assert(subquery->jointree->fromlist != NIL);
    if (subquery->jointree->quals == NULL &&
        list_length(subquery->jointree->fromlist) == 1)
        return (Node *) linitial(subquery->jointree->fromlist);

    return (Node*)subquery->jointree;
}

/*
 * pull_up_simple_union_all
 *		Pull up a single simple UNION ALL subquery.
 *
 * jtnode is a RangeTblRef that has been identified as a simple UNION ALL
 * subquery by pull_up_subqueries.	We pull up the leaf subqueries and
 * build an "append relation" for the union set.  The result value is just
 * jtnode, since we don't actually need to change the query jointree.
 */
static Node* pull_up_simple_union_all(PlannerInfo* root, Node* jtnode, RangeTblEntry* rte)
{
    int varno = ((RangeTblRef*)jtnode)->rtindex;
    Query* subquery = rte->subquery;
    int rtoffset;
    List* rtable = NIL;
    UNIONALL_SHIPPING_TYPE shipping_type = SHIPPING_NONE;

    if (subquery->setOperations == NULL) {
        elog(ERROR, "subquery's setOperations tree should not be NULL in pull_up_simple_union_all");
    }

    if (IS_STREAM_PLAN) {
        shipping_type = precheck_shipping_union_all(subquery, subquery->setOperations);
        if (shipping_type == SHIPPING_ALL && subquery->can_push == false) {
            shipping_type = SHIPPING_PARTIAL;
        }
        
        if (shipping_type == SHIPPING_ALL) {
            if (!root->parse->can_push) {
                return jtnode;
            }
        } else if (shipping_type == SHIPPING_PARTIAL) {
            return jtnode;
        } else {
            if (root->parse->can_push && check_base_rel_in_fromlist(root->parse, jtnode)) {
                return jtnode;
            }
        }
    }

    /*
     * Append child RTEs to parent rtable.
     *
     * Upper-level vars in subquery are now one level closer to their parent
     * than before.  We don't have to worry about offsetting varnos, though,
     * because any such vars must refer to stuff above the level of the query
     * we are pulling into.
     */
    rtoffset = list_length(root->parse->rtable);
    rtable = (List*)copyObject(subquery->rtable);
    IncrementVarSublevelsUp_rtable(rtable, -1, 1);
    root->parse->rtable = list_concat(root->parse->rtable, rtable);

    /* Subquery can be pull, so set flag to true. */
    rte->subquery_pull_up = true;

    /*
     * Recursively scan the subquery's setOperations tree and add
     * AppendRelInfo nodes for leaf subqueries to the parent's
     * append_rel_list.  Also apply pull_up_subqueries to the leaf subqueries.
     */
    AssertEreport(subquery->setOperations != NULL,
        MOD_OPT_REWRITE,
        "subquery's setOperations tree should not be NULL in pull_up_simple_union_all");
    pull_up_union_leaf_queries(subquery->setOperations, root, varno, subquery, rtoffset);

    /*
     * Mark the parent as an append relation.
     */
    rte->inh = true;

    /* Mark can_push flag of parent's parse. */
    if (IS_STREAM_PLAN && root->parse->can_push) {
        if (shipping_type == SHIPPING_NONE && check_base_rel_in_fromlist(root->parse, jtnode))
            set_stream_off();
    }

    return jtnode;
}

/*
 * pull_up_union_leaf_queries -- recursive guts of pull_up_simple_union_all
 *
 * Build an AppendRelInfo for each leaf query in the setop tree, and then
 * apply pull_up_subqueries to the leaf query.
 *
 * Note that setOpQuery is the Query containing the setOp node, whose tlist
 * contains references to all the setop output columns.  When called from
 * pull_up_simple_union_all, this is *not* the same as root->parse, which is
 * the parent Query we are pulling up into.
 *
 * parentRTindex is the appendrel parent's index in root->parse->rtable.
 *
 * The child RTEs have already been copied to the parent.  childRToffset
 * tells us where in the parent's range table they were copied.  When called
 * from flatten_simple_union_all, childRToffset is 0 since the child RTEs
 * were already in root->parse->rtable and no RT index adjustment is needed.
 */
static void pull_up_union_leaf_queries(
    Node* setOp, PlannerInfo* root, int parentRTindex, Query* setOpQuery, int childRToffset)
{
    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        int childRTindex;
        AppendRelInfo* appinfo = NULL;

        /*
         * Calculate the index in the parent's range table
         */
        childRTindex = childRToffset + rtr->rtindex;

        /*
         * Build a suitable AppendRelInfo, and attach to parent's list.
         */
        appinfo = makeNode(AppendRelInfo);
        appinfo->parent_relid = parentRTindex;
        appinfo->child_relid = childRTindex;
        appinfo->parent_reltype = InvalidOid;
        appinfo->child_reltype = InvalidOid;
        make_setop_translation_list(setOpQuery, childRTindex, &appinfo->translated_vars);
        appinfo->parent_reloid = InvalidOid;
        root->append_rel_list = lappend(root->append_rel_list, appinfo);

        /*
         * Recursively apply pull_up_subqueries to the new child RTE.  (We
         * must build the AppendRelInfo first, because this will modify it.)
         * Note that we can pass NULL for containing-join info even if we're
         * actually under an outer join, because the child's expressions
         * aren't going to propagate up above the join.
         */
        rtr = makeNode(RangeTblRef);
        rtr->rtindex = childRTindex;
        (void)pull_up_subqueries_recurse(root, (Node*)rtr, NULL, NULL, appinfo);
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;

        /* Recurse to reach leaf queries */
        pull_up_union_leaf_queries(op->larg, root, parentRTindex, setOpQuery, childRToffset);
        pull_up_union_leaf_queries(op->rarg, root, parentRTindex, setOpQuery, childRToffset);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(setOp))));
    }
}

/*
 * make_setop_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  a UNION ALL member.  (At this point it's just a simple list of
 *	  referencing Vars, but if we succeed in pulling up the member
 *	  subquery, the Vars will get replaced by pulled-up expressions.)
 */
static void make_setop_translation_list(Query* query, Index newvarno, List** translated_vars)
{
    List* vars = NIL;
    ListCell* l = NULL;

    foreach (l, query->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->resjunk)
            continue;

        vars = lappend(vars, makeVarFromTargetEntry(newvarno, tle));
    }

    *translated_vars = vars;
}

/*
 * is_simple_lateral_subquery
 * If the subquery is LATERAL, check for pullup restrictions from that.
 */
static bool is_simple_lateral_subquery(Query* subquery, JoinExpr *lowest_outer_join)
{
    bool        restricted = false;
    Relids      safe_upper_varnos = NULL;

    /*
     * The subquery's WHERE and JOIN/ON quals mustn't contain any lateral
     * references to rels outside a higher outer join (including the case
     * where the outer join is within the subquery itself).  In such a
     * case, pulling up would result in a situation where we need to
     * postpone quals from below an outer join to above it, which is
     * probably completely wrong and in any case is a complication that
     * doesn't seem worth addressing at the moment.
     */
    if (lowest_outer_join != NULL)
    {
        restricted = true;
        safe_upper_varnos = get_relids_in_jointree((Node *) lowest_outer_join,
                                                   true);
    }
    else
    {
        restricted = false;
        safe_upper_varnos = NULL;   /* doesn't matter */
    }

    if (jointree_contains_lateral_outer_refs((Node *) subquery->jointree,
                                             restricted, safe_upper_varnos))
        return false;

    /*
     * If there's an outer join above the LATERAL subquery, also disallow
     * pullup if the subquery's targetlist has any references to rels
     * outside the outer join, since these might get pulled into quals
     * above the subquery (but in or below the outer join) and then lead
     * to qual-postponement issues similar to the case checked for above.
     * (We wouldn't need to prevent pullup if no such references appear in
     * outer-query quals, but we don't have enough info here to check
     * that.  Also, maybe this restriction could be removed if we forced
     * such refs to be wrapped in PlaceHolderVars, even when they're below
     * the nearest outer join?  But it's a pretty hokey usage, so not
     * clear this is worth sweating over.)
     */
    if (lowest_outer_join != NULL)
    {
        Relids      lvarnos = pull_varnos_of_level((Node *) subquery->targetList, 1);

        if (!bms_is_subset(lvarnos, safe_upper_varnos))
            return false;
    }

    return true;
}

static bool is_grouping_subquery(Query* subquery)
{
    /*
     * Can't pull up a subquery involving grouping, aggregation, sorting,
     * limiting, or WITH.  (XXX WITH could possibly be allowed later)
     *
     * We also don't pull up a subquery that has explicit FOR UPDATE/SHARE
     * clauses, because pullup would cause the locking to occur semantically
     * higher than it should.  Implicit FOR UPDATE/SHARE is okay because in
     * that case the locking was originally declared in the upper query
     * anyway.
     */
    if (subquery->hasAggs || subquery->hasWindowFuncs || subquery->groupClause || subquery->groupingSets ||
        subquery->havingQual || subquery->sortClause || subquery->distinctClause || subquery->limitOffset ||
        subquery->limitCount || subquery->hasForUpdate || subquery->cteList)
        return false;

    return true;
}

/*
 * is_simple_subquery
 *	  Check a subquery in the range table to see if it's simple enough
 *	  to pull up into the parent query.

 * rte is the RTE_SUBQUERY RangeTblEntry that contained the subquery.
 * (Note subquery is not necessarily equal to rte->subquery; it could be a
 * processed copy of that.)
 * lowest_outer_join is the lowest outer join above the subquery, or NULL.
 */
static bool is_simple_subquery(Query* subquery, RangeTblEntry *rte, JoinExpr *lowest_outer_join)
{
    /*
     * Let's just make sure it's a valid subselect ...
     */
    if (!IsA(subquery, Query) || subquery->commandType != CMD_SELECT || subquery->utilityStmt != NULL)
        ereport(
            ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), (errmsg("subquery is bogus"))));

    /*
     * Can't currently pull up a query with setops (unless it's simple UNION
     * ALL, which is handled by a different code path). Maybe after querytree
     * redesign...
     */
    if (subquery->setOperations)
        return false;

    /* simple subquery cannot related to any grouping info. */
    if (!is_grouping_subquery(subquery))
        return false;

    /*
     * Don't pull up if the RTE represents a security-barrier view; we couldn't
     * prevent information leakage once the RTE's Vars are scattered about in
     * the upper query.
     */

    if (rte->security_barrier)
        return false;
    if (ContainRownumQual(subquery)) {
        return false;
    }
    if (subquery->hasAggs || subquery->hasWindowFuncs || subquery->groupClause || subquery->groupingSets ||
        subquery->havingQual || subquery->sortClause || subquery->distinctClause || subquery->limitOffset ||
        subquery->limitCount || subquery->hasForUpdate || subquery->cteList)
        return false;

    /*
     * If the subquery is LATERAL, check for pullup restrictions from that.
     */
    if (rte->lateral &&
        !is_simple_lateral_subquery(subquery, lowest_outer_join))
    {
        return false;
    }
#ifndef ENABLE_MULTIPLE_NODES
	    if (ContainRownumQual(subquery)) {
            return false;
        }
#endif

    /*
     * Don't pull up a subquery that has any set-returning functions in its
     * targetlist.	Otherwise we might well wind up inserting set-returning
     * functions into places where they mustn't go, such as quals of higher
     * queries.
     */
    if (expression_returns_set((Node*)subquery->targetList))
        return false;

    /*
     * Don't pull up a subquery that has any volatile functions in its
     * targetlist.	Otherwise we might introduce multiple evaluations of these
     * functions, if they get copied to multiple places in the upper query,
     * leading to surprising results.  (Note: the PlaceHolderVar mechanism
     * doesn't quite guarantee single evaluation; else we could pull up anyway
     * and just wrap such items in PlaceHolderVars ...)
     */
    if (contain_volatile_functions((Node*)subquery->targetList))
        return false;

    /*
     * Hack: don't try to pull up a subquery with an empty jointree.
     * query_planner() will correctly generate a Result plan for a jointree
     * that's totally empty, but I don't think the right things happen if an
     * empty FromExpr appears lower down in a jointree.  It would pose a
     * problem for the PlaceHolderVar mechanism too, since we'd have no way to
     * identify where to evaluate a PHV coming out of the subquery. Not worth
     * working hard on this, just to collapse SubqueryScan/Result into Result;
     * especially since the SubqueryScan can often be optimized away by
     * setrefs.c anyway.
     */
    if (subquery->jointree->fromlist == NIL)
        return false;

    return true;
}

/*
 * is_simple_union_all
 *	  Check a subquery to see if it's a simple UNION ALL.
 *
 * We require all the setops to be UNION ALL (no mixing) and there can't be
 * any datatype coercions involved, ie, all the leaf queries must emit the
 * same datatypes.
 */
static bool is_simple_union_all(Query* subquery)
{
    SetOperationStmt* topop = NULL;

    /* Let's just make sure it's a valid subselect ... */
    if (!IsA(subquery, Query) || subquery->commandType != CMD_SELECT || subquery->utilityStmt != NULL)
        ereport(
            ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), (errmsg("subquery is bogus"))));

    /* Is it a set-operation query at all? */
    topop = (SetOperationStmt*)subquery->setOperations;
    if (topop == NULL)
        return false;
    AssertEreport(
        IsA(topop, SetOperationStmt), MOD_OPT_REWRITE, "subquery's setOperations mismatch in is_simple_union_all");

#ifndef ENABLE_MULTIPLE_NODES
    if (ContainRownumQual(subquery)) {
        return false;
    }
#endif

    /* Can't handle ORDER BY, LIMIT/OFFSET, locking, or WITH */
    if (subquery->sortClause || subquery->limitOffset || subquery->limitCount || subquery->rowMarks ||
        subquery->cteList)
        return false;

    /* Recursively check the tree of set operations */
    return is_simple_union_all_recurse((Node*)topop, subquery, topop->colTypes);
}

static bool is_simple_union_all_recurse(Node* setOp, Query* setOpQuery, List* colTypes)
{
    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* rte = rt_fetch(rtr->rtindex, setOpQuery->rtable);
        Query* subquery = rte->subquery;

        AssertEreport(subquery != NULL, MOD_OPT_REWRITE, "subquery should not be NULL in is_simple_union_all_recurse");

        /* Leaf nodes are OK if they match the toplevel column types */
        /* We don't have to compare typmods or collations here */
        return tlist_same_datatypes(subquery->targetList, colTypes, true);
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;

        /* Must be UNION ALL */
        if (op->op != SETOP_UNION || !op->all)
            return false;

        /* Recurse to check inputs */
        return is_simple_union_all_recurse(op->larg, setOpQuery, colTypes) &&
               is_simple_union_all_recurse(op->rarg, setOpQuery, colTypes);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(setOp))));
        return false; /* keep compiler quiet */
    }
}

/*
 * is_safe_append_member
 *	  Check a subquery that is a leaf of a UNION ALL appendrel to see if it's
 *	  safe to pull up.
 */
static bool is_safe_append_member(Query* subquery)
{
    FromExpr* jtnode = NULL;

    /*
     * It's only safe to pull up the child if its jointree contains exactly
     * one RTE, else the AppendRelInfo data structure breaks. The one base RTE
     * could be buried in several levels of FromExpr, however.
     *
     * Also, the child can't have any WHERE quals because there's no place to
     * put them in an appendrel.  (This is a bit annoying...) If we didn't
     * need to check this, we'd just test whether get_relids_in_jointree()
     * yields a singleton set, to be more consistent with the coding
     * of fix_append_rel_relids().
     */
    jtnode = subquery->jointree;
    while (IsA(jtnode, FromExpr)) {
        if (jtnode->quals != NULL)
            return false;
        if (list_length(jtnode->fromlist) != 1)
            return false;
        jtnode = (FromExpr*)linitial(jtnode->fromlist);
    }
    if (!IsA(jtnode, RangeTblRef))
        return false;

    return true;
}

/*
 * jointree_contains_lateral_outer_refs
 *     Check for disallowed lateral references in a jointree's quals
 *
 * If restricted is false, all level-1 Vars are allowed (but we still must
 * search the jointree, since it might contain outer joins below which there
 * will be restrictions).  If restricted is true, return TRUE when any qual
 * in the jointree contains level-1 Vars coming from outside the rels listed
 * in safe_upper_varnos.
 */
static bool
jointree_contains_lateral_outer_refs(Node *jtnode, bool restricted,
                                    Relids safe_upper_varnos)
{
   if (jtnode == NULL)
       return false;
   if (IsA(jtnode, RangeTblRef))
       return false;
   else if (IsA(jtnode, FromExpr))
   {
       FromExpr   *f = (FromExpr *) jtnode;
       ListCell   *l = NULL;

       /* First, recurse to check child joins */
       foreach(l, f->fromlist)
       {
           if (jointree_contains_lateral_outer_refs((Node *)lfirst(l),
                                                    restricted,
                                                    safe_upper_varnos))
               return true;
       }

       /* Then check the top-level quals */
       if (restricted &&
           !bms_is_subset(pull_varnos_of_level(f->quals, 1),
                          safe_upper_varnos))
           return true;
   }
   else if (IsA(jtnode, JoinExpr))
   {
       JoinExpr   *j = (JoinExpr *) jtnode;

       /*
        * If this is an outer join, we mustn't allow any upper lateral
        * references in or below it.
        */
       if (j->jointype != JOIN_INNER)
       {
           restricted = true;
           safe_upper_varnos = NULL;
       }

       /* Check the child joins */
       if (jointree_contains_lateral_outer_refs(j->larg,
                                                restricted,
                                                safe_upper_varnos))
           return true;
       if (jointree_contains_lateral_outer_refs(j->rarg,
                                                restricted,
                                                safe_upper_varnos))
           return true;

       /* Check the JOIN's qual clauses */
       if (restricted &&
           !bms_is_subset(pull_varnos_of_level(j->quals, 1),
                          safe_upper_varnos))
           return true;
   }
   else
       elog(ERROR, "unrecognized node type: %d",
            (int) nodeTag(jtnode));
   return false;
}

/*
 * Helper routine for pull_up_subqueries: do pullup_replace_vars on every
 * expression in the jointree, without changing the jointree structure itself.
 * Ugly, but there's no other way...
 *
 * If we are at or below lowest_nulling_outer_join, we can suppress use of
 * PlaceHolderVars wrapped around the replacement expressions.
 */
static void replace_vars_in_jointree(Node* jtnode, pullup_replace_vars_context* context, JoinExpr* lowest_nulling_outer_join)
{
    if (jtnode == NULL)
        return;
    if (IsA(jtnode, RangeTblRef)) {
        /*
         * If the RangeTblRef refers to a LATERAL subquery (that isn't the
         * same subquery we're pulling up), it might contain references to the
         * target subquery, which we must replace.  We drive this from the
         * jointree scan, rather than a scan of the rtable, for a couple of
         * reasons: we can avoid processing no-longer-referenced RTEs, and we
         * can use the appropriate setting of need_phvs depending on whether
         * the RTE is above possibly-nulling outer joins or not.
         */
        int         varno = ((RangeTblRef *) jtnode)->rtindex;

        if (varno != context->varno)    /* ignore target subquery itself */
        {
            RangeTblEntry *rte = rt_fetch(varno, context->root->parse->rtable);

            Assert(rte != context->target_rte);
            if (rte->lateral)
            {
                switch (rte->rtekind)
                {
                    case RTE_SUBQUERY:
                        rte->subquery =
                            pullup_replace_vars_subquery(rte->subquery,
                                                         context);
                        break;
                    case RTE_FUNCTION:
                        rte->funcexpr =
                            pullup_replace_vars(rte->funcexpr,
                                                context);
                        break;
                    case RTE_VALUES:
                        rte->values_lists = (List *)
                            pullup_replace_vars((Node *) rte->values_lists,
                                                context);
                        break;
                    case RTE_RELATION:
                    case RTE_JOIN:
                    case RTE_CTE:
                    case RTE_RESULT:
                        /* these shouldn't be marked LATERAL */
                        Assert(false);
                        break;
#ifdef PGXC
                    case RTE_REMOTE_DUMMY:
                        break;
#endif
                }
            }
        }
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist)
            replace_vars_in_jointree((Node*)lfirst(l), context, lowest_nulling_outer_join);
        f->quals = pullup_replace_vars(f->quals, context);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;
        bool save_need_phvs = context->need_phvs;

        if (j == lowest_nulling_outer_join) {
            /* no more PHVs in or below this join */
            context->need_phvs = false;
            lowest_nulling_outer_join = NULL;
        }
        replace_vars_in_jointree(j->larg, context, lowest_nulling_outer_join);
        replace_vars_in_jointree(j->rarg, context, lowest_nulling_outer_join);
        j->quals = pullup_replace_vars(j->quals, context);

        /*
         * We don't bother to update the colvars list, since it won't be used
         * again ...
         */
        context->need_phvs = save_need_phvs;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
}

/*
 * Apply pullup variable replacement throughout an expression tree
 *
 * Returns a modified copy of the tree, so this can't be used where we
 * need to do in-place replacement.
 */
static Node* pullup_replace_vars(Node* expr, pullup_replace_vars_context* context)
{
    return replace_rte_variables(
        expr, context->varno, 0, pullup_replace_vars_callback, (void*)context, context->outer_hasSubLinks);
}

static Node* pullup_replace_vars_callback(Var* var, replace_rte_variables_context* context)
{
    pullup_replace_vars_context* rcon = (pullup_replace_vars_context*)context->callback_arg;
    int varattno = var->varattno;
    Node* newnode = NULL;

    /*
     * If PlaceHolderVars are needed, we cache the modified expressions in
     * rcon->rv_cache[].  This is not in hopes of any material speed gain
     * within this function, but to avoid generating identical PHVs with
     * different IDs.  That would result in duplicate evaluations at runtime,
     * and possibly prevent optimizations that rely on recognizing different
     * references to the same subquery output as being equal().  So it's worth
     * a bit of extra effort to avoid it.
     */
    if (rcon->need_phvs && varattno >= InvalidAttrNumber && varattno <= list_length(rcon->targetlist) &&
        rcon->rv_cache[varattno] != NULL) {
        /* Just copy the entry and fall through to adjust its varlevelsup */
        newnode = (Node*)copyObject(rcon->rv_cache[varattno]);
    } else if (varattno == InvalidAttrNumber) {
        /* Must expand whole-tuple reference into RowExpr */
        RowExpr* rowexpr = NULL;
        List* colnames = NIL;
        List* fields = NIL;
        bool save_need_phvs = rcon->need_phvs;
        int save_sublevelsup = context->sublevels_up;

        /*
         * If generating an expansion for a var of a named rowtype (ie, this
         * is a plain relation RTE), then we must include dummy items for
         * dropped columns.  If the var is RECORD (ie, this is a JOIN), then
         * omit dropped columns. Either way, attach column names to the
         * RowExpr for use of ruleutils.c.
         *
         * In order to be able to cache the results, we always generate the
         * expansion with varlevelsup = 0, and then adjust if needed.
         */
        expandRTE(rcon->target_rte,
            var->varno,
            0 /* not varlevelsup */,
            var->location,
            (var->vartype != RECORDOID),
            &colnames,
            &fields);
        /* Adjust the generated per-field Vars, but don't insert PHVs */
        rcon->need_phvs = false;
        context->sublevels_up = 0; /* to match the expandRTE output */
        fields = (List*)replace_rte_variables_mutator((Node*)fields, context);
        rcon->need_phvs = save_need_phvs;
        context->sublevels_up = save_sublevelsup;

        rowexpr = makeNode(RowExpr);
        rowexpr->args = fields;
        rowexpr->row_typeid = var->vartype;
        rowexpr->row_format = COERCE_IMPLICIT_CAST;
        rowexpr->colnames = colnames;
        rowexpr->location = var->location;
        newnode = (Node*)rowexpr;

        /*
         * Insert PlaceHolderVar if needed.  Notice that we are wrapping one
         * PlaceHolderVar around the whole RowExpr, rather than putting one
         * around each element of the row.	This is because we need the
         * expression to yield NULL, not ROW(NULL,NULL,...) when it is forced
         * to null by an outer join.
         */
        if (rcon->need_phvs) {
            /* RowExpr is certainly not strict, so always need PHV */
            newnode = (Node*)make_placeholder_expr(rcon->root, (Expr*)newnode, bms_make_singleton(rcon->varno));
            /* cache it with the PHV, and with varlevelsup still zero */
            rcon->rv_cache[InvalidAttrNumber] = (Node*)copyObject(newnode);
        }
    }
#ifdef PGXC
    else if (varattno == XC_NodeIdAttributeNumber) {
        /* We don't need to change the entry for xc_node_id */
        newnode = NULL;
    }
#endif
    else {
        /* Normal case referencing one targetlist element */
        TargetEntry* tle = get_tle_by_resno(rcon->targetlist, varattno);

        if (tle == NULL) /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    (errmsg("could not find attribute %d in subquery targetlist", varattno))));
        /* Make a copy of the tlist item to return */
        newnode = (Node*)copyObject(tle->expr);

        /* Insert PlaceHolderVar if needed */
        if (rcon->need_phvs) {
            bool wrap = false;

            if (newnode && IsA(newnode, Var) && ((Var*)newnode)->varlevelsup == 0) {
                /* Simple Vars always escape being wrapped */
                wrap = false;
            } else if (newnode && IsA(newnode, PlaceHolderVar) && ((PlaceHolderVar*)newnode)->phlevelsup == 0) {
                /* No need to wrap a PlaceHolderVar with another one, either */
                wrap = false;
            } else if (rcon->wrap_non_vars) {
                /* Wrap all non-Vars in a PlaceHolderVar */
                wrap = true;
            } else {
                /*
                 * If it contains a Var of current level, and does not contain
                 * any non-strict constructs, then it's certainly nullable so
                 * we don't need to insert a PlaceHolderVar.
                 *
                 * This analysis could be tighter: in particular, a non-strict
                 * construct hidden within a lower-level PlaceHolderVar is not
                 * reason to add another PHV.  But for now it doesn't seem
                 * worth the code to be more exact.
                 *
                 * Note: in future maybe we should insert a PlaceHolderVar
                 * anyway, if the tlist item is expensive to evaluate?
                 */
                if (contain_vars_of_level((Node*)newnode, 0) && !contain_nonstrict_functions((Node*)newnode)) {
                    /* No wrap needed */
                    wrap = false;
                } else {
                    /* Else wrap it in a PlaceHolderVar */
                    wrap = true;
                }
            }

            if (wrap)
                newnode = (Node*)make_placeholder_expr(rcon->root, (Expr*)newnode, bms_make_singleton(rcon->varno));

            /*
             * Cache it if possible (ie, if the attno is in range, which it
             * probably always should be).	We can cache the value even if we
             * decided we didn't need a PHV, since this result will be
             * suitable for any request that has need_phvs.
             */
            if (varattno > InvalidAttrNumber && varattno <= list_length(rcon->targetlist))
                rcon->rv_cache[varattno] = (Node*)copyObject(newnode);
        }
    }

    /* Must adjust varlevelsup if tlist item is from higher query */
    if (var->varlevelsup > 0)
        IncrementVarSublevelsUp(newnode, var->varlevelsup, 0);

    return newnode;
}

/*
 * Apply pullup variable replacement to a subquery
 *
 * This needs to be different from pullup_replace_vars() because
 * replace_rte_variables will think that it shouldn't increment sublevels_up
 * before entering the Query; so we need to call it with sublevels_up == 1.
 */
static Query *
pullup_replace_vars_subquery(Query *query,
                            pullup_replace_vars_context *context)
{
   Assert(IsA(query, Query));
   return (Query *) replace_rte_variables((Node *) query,
                                          context->varno, 1,
                                          pullup_replace_vars_callback,
                                          (void *) context,
                                          NULL);
}

/*
 * flatten_simple_union_all
 *		Try to optimize top-level UNION ALL structure into an appendrel
 *
 * If a query's setOperations tree consists entirely of simple UNION ALL
 * operations, flatten it into an append relation, which we can process more
 * intelligently than the general setops case.	Otherwise, do nothing.
 *
 * In most cases, this can succeed only for a top-level query, because for a
 * subquery in FROM, the parent query's invocation of pull_up_subqueries would
 * already have flattened the UNION via pull_up_simple_union_all.  But there
 * are a few cases we can support here but not in that code path, for example
 * when the subquery also contains ORDER BY.
 */
void flatten_simple_union_all(PlannerInfo* root)
{
    Query* parse = root->parse;
    SetOperationStmt* topop = NULL;
    Node* leftmostjtnode = NULL;
    int leftmostRTI = 0;
    RangeTblEntry* leftmostRTE = NULL;
    int childRTI;
    RangeTblEntry* childRTE = NULL;
    RangeTblRef* rtr = NULL;

    /* Shouldn't be called unless query has setops */
    topop = (SetOperationStmt*)parse->setOperations;
    AssertEreport(topop != NULL && IsA(topop, SetOperationStmt),
        MOD_OPT_REWRITE,
        "SetOperationStmt shouldn't be NULL in flatten_simple_union_all");

    /* Can't optimize away a recursive UNION */
    if (root->hasRecursion)
        return;

    /*
     * Recursively check the tree of set operations.  If not all UNION ALL
     * with identical column types, punt.
     *
     * In multi-node group scenario, we should not flatten union all because
     * the branchs may be in different node group, we could not determine the
     * group for append path
     */
    if (!is_simple_union_all_recurse((Node*)topop, parse, topop->colTypes) || ng_is_multiple_nodegroup_scenario())
        return;

    /*
     * Locate the leftmost leaf query in the setops tree.  The upper query's
     * Vars all refer to this RTE (see transformSetOperationStmt).
     */
    leftmostjtnode = topop->larg;
    while (leftmostjtnode && IsA(leftmostjtnode, SetOperationStmt))
        leftmostjtnode = ((SetOperationStmt*)leftmostjtnode)->larg;

    if (leftmostjtnode && IsA(leftmostjtnode, RangeTblRef)) {
        leftmostRTI = ((RangeTblRef*)leftmostjtnode)->rtindex;
        leftmostRTE = rt_fetch(leftmostRTI, parse->rtable);
        AssertEreport(leftmostRTE->rtekind == RTE_SUBQUERY,
            MOD_OPT_REWRITE,
            "leftmostRTE should be SUBQUERY in flatten_simple_union_all");
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), (errmsg("RangeTblRef not found."))));
    }

    /*
     * Make a copy of the leftmost RTE and add it to the rtable.  This copy
     * will represent the leftmost leaf query in its capacity as a member of
     * the appendrel.  The original will represent the appendrel as a whole.
     * (We must do things this way because the upper query's Vars have to be
     * seen as referring to the whole appendrel.)
     */
    childRTE = (RangeTblEntry*)copyObject(leftmostRTE);
    parse->rtable = lappend(parse->rtable, childRTE);
    childRTI = list_length(parse->rtable);

    /* Modify the setops tree to reference the child copy */
    ((RangeTblRef*)leftmostjtnode)->rtindex = childRTI;

    /* Modify the formerly-leftmost RTE to mark it as an appendrel parent */
    leftmostRTE->inh = true;

    /*
     * Form a RangeTblRef for the appendrel, and insert it into FROM.  The top
     * Query of a setops tree should have had an empty FromClause initially.
     */
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = leftmostRTI;
    AssertEreport(parse->jointree->fromlist == NIL,
        MOD_OPT_REWRITE,
        "The top Query of a setops tree should have had an empty FromClause in flatten_simple_union_all");
    parse->jointree->fromlist = list_make1(rtr);

    /*
     * Now pretend the query has no setops.  We must do this before trying to
     * do subquery pullup, because of Assert in pull_up_simple_subquery.
     */
    parse->setOperations = NULL;

    /*
     * Build AppendRelInfo information, and apply pull_up_subqueries to the
     * leaf queries of the UNION ALL.  (We must do that now because they
     * weren't previously referenced by the jointree, and so were missed by
     * the main invocation of pull_up_subqueries.)
     */
    pull_up_union_leaf_queries((Node*)topop, root, leftmostRTI, parse, 0);
}

/*
 * @Description: Delete not Null Test if var type have not null constraint.
 * @in node: Qual cluase.
 * @in qry: Query tree.
 * @return: return the remaining node.
 */
static Node* deleteRelatedNullTest(Node* node, PlannerInfo* root)
{
    if (node == NULL) {
        return NULL;
    }

    if (or_clause(node)) {
        BoolExpr* bool_node = (BoolExpr*)node;

        ListCell* cell = NULL;
        foreach (cell, bool_node->args) {
            /*
             * If or clause exists 'not null test' can be deleted, then this or clause can all deleted.
             * That is bescuse 'not null test' is always true.
             */
            Node* result = deleteRelatedNullTest((Node*)lfirst(cell), root);

            if (result == NULL) {
                return NULL;
            }
        }
    } else if (and_clause(node)) {
        BoolExpr* bool_node = (BoolExpr*)node;

        bool_node->args = (List*)deleteRelatedNullTest((Node*)bool_node->args, root);

        /*
         * If and clause exists 'not null test' BOTH can be deleted, then this and clause can be deleted.
         * That is bescuse BOTH 'not null test' are always true.
         */
        if (bool_node->args == NULL)
            return NULL;
    } else if (IsA(node, List)) {
        List* args_list = NIL;
        List* l = (List*)node;

        ListCell* cell = NULL;

        foreach (cell, l) {
            Node* result = deleteRelatedNullTest((Node*)lfirst(cell), root);

            if (result != NULL) {
                args_list = lappend(args_list, result);
            }
        }

        return (Node*)args_list;
    } else if (IsA(node, NullTest)) { /* Check this not null test can be deleted. */
        NullTest* NullExpr = (NullTest*)node;

        if (IS_NOT_NULL == NullExpr->nulltesttype && IsA(NullExpr->arg, Var)) {
            Var* var = (Var*)NullExpr->arg;

            /* If do not find primary key informatinal constraint, try not null constraint  */
            if (check_var_nonnullable(root->parse, (Node*)var)) {
                return NULL;
            }
        }
    }

    return node;
}

/*
 * @Description: Delete related NullTest.
 * @in parse: The Query Tree after parsing for SQL.
 */
void removeNotNullTest(PlannerInfo* root)
{
    Query* parse = root->parse;

    /* Just now, we only support single table optimization */
    if (list_length(parse->rtable) == 1) {
        RangeTblEntry* rte = (RangeTblEntry*)linitial(parse->rtable);
        if (rte->rtekind == RTE_RELATION) {
            parse->jointree->quals = deleteRelatedNullTest(parse->jointree->quals, root);
        }
    }
}

/*
 * reduce_outer_joins
 *		Attempt to reduce outer joins to plain inner joins.
 *
 * The idea here is that given a query like
 *		SELECT ... FROM a LEFT JOIN b ON (...) WHERE b.y = 42;
 * we can reduce the LEFT JOIN to a plain JOIN if the "=" operator in WHERE
 * is strict.  The strict operator will always return NULL, causing the outer
 * WHERE to fail, on any row where the LEFT JOIN filled in NULLs for b's
 * columns.  Therefore, there's no need for the join to produce null-extended
 * rows in the first place --- which makes it a plain join not an outer join.
 * (This scenario may not be very likely in a query written out by hand, but
 * it's reasonably likely when pushing quals down into complex views.)
 *
 * More generally, an outer join can be reduced in strength if there is a
 * strict qual above it in the qual tree that constrains a Var from the
 * nullable side of the join to be non-null.  (For FULL joins this applies
 * to each side separately.)
 *
 * Another transformation we apply here is to recognize cases like
 *		SELECT ... FROM a LEFT JOIN b ON (a.x = b.y) WHERE b.y IS NULL;
 * If the join clause is strict for b.y, then only null-extended rows could
 * pass the upper WHERE, and we can conclude that what the query is really
 * specifying is an anti-semijoin.	We change the join type from JOIN_LEFT
 * to JOIN_ANTI.  The IS NULL clause then becomes redundant, and must be
 * removed to prevent bogus selectivity calculations, but we leave it to
 * distribute_qual_to_rels to get rid of such clauses.
 *
 * Also, we get rid of JOIN_RIGHT cases by flipping them around to become
 * JOIN_LEFT.  This saves some code here and in some later planner routines,
 * but the main reason to do it is to not need to invent a JOIN_REVERSE_ANTI
 * join type.
 *
 * To ease recognition of strict qual clauses, we require this routine to be
 * run after expression preprocessing (i.e., qual canonicalization and JOIN
 * alias-var expansion).
 */
void reduce_outer_joins(PlannerInfo* root)
{
    reduce_outer_joins_state* state = NULL;

    /*
     * To avoid doing strictness checks on more quals than necessary, we want
     * to stop descending the jointree as soon as there are no outer joins
     * below our current point.  This consideration forces a two-pass process.
     * The first pass gathers information about which base rels appear below
     * each side of each join clause, and about whether there are outer
     * join(s) below each side of each join clause. The second pass examines
     * qual clauses and changes join types as it descends the tree.
     */
    state = reduce_outer_joins_pass1((Node*)root->parse->jointree);
    /* planner.c shouldn't have called me if no outer joins */
    if (state == NULL || !state->contains_outer)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("so where are the outer joins?"))));

    reduce_outer_joins_pass2((Node*)root->parse->jointree, state, root, NULL, NIL, NIL);
}

/*
 * reduce_outer_joins_pass1 - phase 1 data collection
 *
 * Returns a state node describing the given jointree node.
 */
static reduce_outer_joins_state* reduce_outer_joins_pass1(Node* jtnode)
{
    reduce_outer_joins_state* result = NULL;

    result = (reduce_outer_joins_state*)palloc(sizeof(reduce_outer_joins_state));
    result->relids = NULL;
    result->contains_outer = false;
    result->sub_states = NIL;

    if (jtnode == NULL)
        return result;
    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;

        result->relids = bms_make_singleton(varno);
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist) {
            reduce_outer_joins_state* sub_state = NULL;

            sub_state = reduce_outer_joins_pass1((Node*)lfirst(l));
            result->relids = bms_add_members(result->relids, sub_state->relids);
            result->contains_outer |= sub_state->contains_outer;
            result->sub_states = lappend(result->sub_states, sub_state);
        }
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;
        reduce_outer_joins_state* sub_state = NULL;

        /* join's own RT index is not wanted in result->relids */
        if (IS_OUTER_JOIN(j->jointype))
            result->contains_outer = true;

        sub_state = reduce_outer_joins_pass1(j->larg);
        result->relids = bms_add_members(result->relids, sub_state->relids);
        result->contains_outer |= sub_state->contains_outer;
        result->sub_states = lappend(result->sub_states, sub_state);

        sub_state = reduce_outer_joins_pass1(j->rarg);
        result->relids = bms_add_members(result->relids, sub_state->relids);
        result->contains_outer |= sub_state->contains_outer;
        result->sub_states = lappend(result->sub_states, sub_state);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
    return result;
}

/*
 * reduce_outer_joins_pass2 - phase 2 processing
 *
 *	jtnode: current jointree node
 *	state: state data collected by phase 1 for this node
 *	root: toplevel planner state
 *	nonnullable_rels: set of base relids forced non-null by upper quals
 *	nonnullable_vars: list of Vars forced non-null by upper quals
 *	forced_null_vars: list of Vars forced null by upper quals
 */
static void reduce_outer_joins_pass2(Node* jtnode, reduce_outer_joins_state* state, PlannerInfo* root,
    Relids nonnullable_rels, List* nonnullable_vars, List* forced_null_vars)
{
    /*
     * pass 2 should never descend as far as an empty subnode or base rel,
     * because it's only called on subtrees marked as contains_outer.
     */
    if (jtnode == NULL)
        ereport(
            ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), (errmsg("reached empty jointree"))));
    if (IsA(jtnode, RangeTblRef))
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), (errmsg("reached base rel"))));
    else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;
        ListCell* s = NULL;
        Relids pass_nonnullable_rels;
        List* pass_nonnullable_vars = NIL;
        List* pass_forced_null_vars = NIL;

        /* Scan quals to see if we can add any constraints */
        pass_nonnullable_rels = find_nonnullable_rels(f->quals);
        pass_nonnullable_rels = bms_add_members(pass_nonnullable_rels, nonnullable_rels);
        /* NB: we rely on list_concat to not damage its second argument */
        pass_nonnullable_vars = find_nonnullable_vars(f->quals);
        pass_nonnullable_vars = list_concat(pass_nonnullable_vars, nonnullable_vars);
        pass_forced_null_vars = find_forced_null_vars(f->quals);
        pass_forced_null_vars = list_concat(pass_forced_null_vars, forced_null_vars);
        /* And recurse --- but only into interesting subtrees */
        AssertEreport(list_length(f->fromlist) == list_length(state->sub_states),
            MOD_OPT_REWRITE,
            "list length mismatch in reduce_outer_joins_pass2");
        forboth(l, f->fromlist, s, state->sub_states)
        {
            reduce_outer_joins_state* sub_state = (reduce_outer_joins_state*)lfirst(s);

            if (sub_state->contains_outer)
                reduce_outer_joins_pass2((Node*)lfirst(l),
                    sub_state,
                    root,
                    pass_nonnullable_rels,
                    pass_nonnullable_vars,
                    pass_forced_null_vars);
        }
        bms_free_ext(pass_nonnullable_rels);
        /* can't so easily clean up var lists, unfortunately */
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;
        int rtindex = j->rtindex;
        JoinType jointype = j->jointype;
        reduce_outer_joins_state* left_state = (reduce_outer_joins_state*)linitial(state->sub_states);
        reduce_outer_joins_state* right_state = (reduce_outer_joins_state*)lsecond(state->sub_states);
        List* local_nonnullable_vars = NIL;
        bool computed_local_nonnullable_vars = false;

        /* Can we simplify this join? */
        switch (jointype) {
            case JOIN_INNER:
                break;
            case JOIN_LEFT:
                if (bms_overlap(nonnullable_rels, right_state->relids))
                    jointype = JOIN_INNER;
                break;
            case JOIN_RIGHT:
                if (bms_overlap(nonnullable_rels, left_state->relids))
                    jointype = JOIN_INNER;
                break;
            case JOIN_FULL:
                if (bms_overlap(nonnullable_rels, left_state->relids)) {
                    if (bms_overlap(nonnullable_rels, right_state->relids))
                        jointype = JOIN_INNER;
                    else
                        jointype = JOIN_LEFT;
                } else {
                    if (bms_overlap(nonnullable_rels, right_state->relids))
                        jointype = JOIN_RIGHT;
                }
                break;
            case JOIN_SEMI:
            case JOIN_ANTI:
                /*
                 * These could only have been introduced by pull_up_sublinks,
                 * so there's no way that upper quals could refer to their
                 * righthand sides, and no point in checking.
                 */
            case JOIN_LEFT_ANTI_FULL:
            case JOIN_RIGHT_ANTI_FULL:
                /*
                 * This should happened, since they are introduced by full join
                 * conversion, and before that, reduce outer has already be done
                 */
                break;
            default: {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized join type: %d", (int)jointype)));
            } break;
        }

        /*
         * Convert JOIN_RIGHT to JOIN_LEFT.  Note that in the case where we
         * reduced JOIN_FULL to JOIN_RIGHT, this will mean the JoinExpr no
         * longer matches the internal ordering of any CoalesceExpr's built to
         * represent merged join variables.  We don't care about that at
         * present, but be wary of it ...
         */
        if (jointype == JOIN_RIGHT || jointype == JOIN_RIGHT_ANTI_FULL) {
            Node* tmparg = NULL;

            tmparg = j->larg;
            j->larg = j->rarg;
            j->rarg = tmparg;
            if (jointype == JOIN_RIGHT)
                jointype = JOIN_LEFT;
            else
                jointype = JOIN_LEFT_ANTI_FULL;
            right_state = (reduce_outer_joins_state*)linitial(state->sub_states);
            left_state = (reduce_outer_joins_state*)lsecond(state->sub_states);
        }

        /*
         * See if we can reduce JOIN_LEFT to JOIN_ANTI.  This is the case if
         * the join's own quals are strict for any var that was forced null by
         * higher qual levels.	NOTE: there are other ways that we could
         * detect an anti-join, in particular if we were to check whether Vars
         * coming from the RHS must be non-null because of table constraints.
         * That seems complicated and expensive though (in particular, one
         * would have to be wary of lower outer joins). For the moment this
         * seems sufficient.
         */
        if (jointype == JOIN_LEFT) {
            List* overlap = NIL;

            local_nonnullable_vars = find_nonnullable_vars(j->quals);
            computed_local_nonnullable_vars = true;

            /*
             * It's not sufficient to check whether local_nonnullable_vars and
             * forced_null_vars overlap: we need to know if the overlap
             * includes any RHS variables.
             */
            overlap = list_intersection(local_nonnullable_vars, forced_null_vars);
            if (overlap != NIL && bms_overlap(pull_varnos((Node*)overlap), right_state->relids))
                jointype = JOIN_ANTI;
        }

        /* Apply the jointype change, if any, to both jointree node and RTE */
        if (rtindex && jointype != j->jointype) {
            RangeTblEntry* rte = rt_fetch(rtindex, root->parse->rtable);

            AssertEreport(rte->rtekind == RTE_JOIN,
                MOD_OPT_REWRITE,
                "RangeTblEntry should be kind of JOIN in reduce_outer_joins_pass2");
            AssertEreport(
                rte->jointype == j->jointype, MOD_OPT_REWRITE, "jointype mismatch in reduce_outer_joins_pass2");
            rte->jointype = jointype;
        }
        j->jointype = jointype;

        /* Only recurse if there's more to do below here */
        if (left_state->contains_outer || right_state->contains_outer) {
            Relids local_nonnullable_rels;
            List* local_forced_null_vars = NIL;
            Relids pass_nonnullable_rels;
            List* pass_nonnullable_vars = NIL;
            List* pass_forced_null_vars = NIL;

            /*
             * If this join is (now) inner, we can add any constraints its
             * quals provide to those we got from above.  But if it is outer,
             * we can pass down the local constraints only into the nullable
             * side, because an outer join never eliminates any rows from its
             * non-nullable side.  Also, there is no point in passing upper
             * constraints into the nullable side, since if there were any
             * we'd have been able to reduce the join.  (In the case of upper
             * forced-null constraints, we *must not* pass them into the
             * nullable side --- they either applied here, or not.) The upshot
             * is that we pass either the local or the upper constraints,
             * never both, to the children of an outer join.
             *
             * Note that a SEMI join works like an inner join here: it's okay
             * to pass down both local and upper constraints.  (There can't be
             * any upper constraints affecting its inner side, but it's not
             * worth having a separate code path to avoid passing them.)
             *
             * At a FULL join we just punt and pass nothing down --- is it
             * possible to be smarter?
             */
            if (jointype != JOIN_FULL) {
                local_nonnullable_rels = find_nonnullable_rels(j->quals);
                if (!computed_local_nonnullable_vars)
                    local_nonnullable_vars = find_nonnullable_vars(j->quals);
                local_forced_null_vars = find_forced_null_vars(j->quals);
                if (jointype == JOIN_INNER || jointype == JOIN_SEMI) {
                    /* OK to merge upper and local constraints */
                    local_nonnullable_rels = bms_add_members(local_nonnullable_rels, nonnullable_rels);
                    local_nonnullable_vars = list_concat(local_nonnullable_vars, nonnullable_vars);
                    local_forced_null_vars = list_concat(local_forced_null_vars, forced_null_vars);
                }
            } else {
                /* no use in calculating these */
                local_nonnullable_rels = NULL;
                local_forced_null_vars = NIL;
            }

            if (left_state->contains_outer) {
                if (jointype == JOIN_INNER || jointype == JOIN_SEMI) {
                    /* pass union of local and upper constraints */
                    pass_nonnullable_rels = local_nonnullable_rels;
                    pass_nonnullable_vars = local_nonnullable_vars;
                    pass_forced_null_vars = local_forced_null_vars;
                } else if (jointype != JOIN_FULL) { /* ie, LEFT or ANTI */
                    /* can't pass local constraints to non-nullable side */
                    pass_nonnullable_rels = nonnullable_rels;
                    pass_nonnullable_vars = nonnullable_vars;
                    pass_forced_null_vars = forced_null_vars;
                } else {
                    /* no constraints pass through JOIN_FULL */
                    pass_nonnullable_rels = NULL;
                    pass_nonnullable_vars = NIL;
                    pass_forced_null_vars = NIL;
                }
                reduce_outer_joins_pass2(
                    j->larg, left_state, root, pass_nonnullable_rels, pass_nonnullable_vars, pass_forced_null_vars);
            }

            if (right_state->contains_outer) {
                if (jointype != JOIN_FULL) { /* ie, INNER/LEFT/SEMI/ANTI */
                     /* pass appropriate constraints, per comment above */
                    pass_nonnullable_rels = local_nonnullable_rels;
                    pass_nonnullable_vars = local_nonnullable_vars;
                    pass_forced_null_vars = local_forced_null_vars;
                } else {
                    /* no constraints pass through JOIN_FULL */
                    pass_nonnullable_rels = NULL;
                    pass_nonnullable_vars = NIL;
                    pass_forced_null_vars = NIL;
                }
                reduce_outer_joins_pass2(
                    j->rarg, right_state, root, pass_nonnullable_rels, pass_nonnullable_vars, pass_forced_null_vars);
            }
            bms_free_ext(local_nonnullable_rels);
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
}

/*
 * remove_useless_result_rtes
 *             Attempt to remove RTE_RESULT RTEs from the join tree.
 *
 * We can remove RTE_RESULT entries from the join tree using the knowledge
 * that RTE_RESULT returns exactly one row and has no output columns.  Hence,
 * if one is inner-joined to anything else, we can delete it.  Optimizations
 * are also possible for some outer-join cases, as detailed below.
 *
 * Some of these optimizations depend on recognizing empty (constant-true)
 * quals for FromExprs and JoinExprs.  That makes it useful to apply this
 * optimization pass after expression preprocessing, since that will have
 * eliminated constant-true quals, allowing more cases to be recognized as
 * optimizable.  What's more, the usual reason for an RTE_RESULT to be present
 * is that we pulled up a subquery or VALUES clause, thus very possibly
 * replacing Vars with constants, making it more likely that a qual can be
 * reduced to constant true.  Also, because some optimizations depend on
 * the outer-join type, it's best to have done reduce_outer_joins() first.
 *
 * A PlaceHolderVar referencing an RTE_RESULT RTE poses an obstacle to this
 * process: we must remove the RTE_RESULT's relid from the PHV's phrels, but
 * we must not reduce the phrels set to empty.  If that would happen, and
 * the RTE_RESULT is an immediate child of an outer join, we have to give up
 * and not remove the RTE_RESULT: there is noplace else to evaluate the
 * PlaceHolderVar.  (That is, in such cases the RTE_RESULT *does* have output
 * columns.)  But if the RTE_RESULT is an immediate child of an inner join,
 * we can change the PlaceHolderVar's phrels so as to evaluate it at the
 * inner join instead.  This is OK because we really only care that PHVs are
 * evaluated above or below the correct outer joins.
 *
 * We used to try to do this work as part of pull_up_subqueries() where the
 * potentially-optimizable cases get introduced; but it's way simpler, and
 * more effective, to do it separately.
 */
void remove_useless_result_rtes(PlannerInfo *root)
{
    ListCell   *cell;
    ListCell   *prev;
    ListCell   *next;

    /* Top level of jointree must always be a FromExpr */
    Assert(IsA(root->parse->jointree, FromExpr));
    /* Recurse ... */
    root->parse->jointree = (FromExpr *)
            remove_useless_results_recurse(root, (Node *) root->parse->jointree);
    /* We should still have a FromExpr */
    Assert(IsA(root->parse->jointree, FromExpr));

    /*
     * Remove any PlanRowMark referencing an RTE_RESULT RTE.  We obviously
     * must do that for any RTE_RESULT that we just removed.  But one for a
     * RTE that we did not remove can be dropped anyway: since the RTE has
     * only one possible output row, there is no need for EPQ to mark and
     * restore that row.
     *
     * It's necessary, not optional, to remove the PlanRowMark for a surviving
     * RTE_RESULT RTE; otherwise we'll generate a whole-row Var for the
     * RTE_RESULT, which the executor has no support for.
     */
    prev = NULL;
    for (cell = list_head(root->rowMarks); cell; cell = next) {
        PlanRowMark *rc = (PlanRowMark *) lfirst(cell);

        next = lnext(cell);
        if (rt_fetch(rc->rti, root->parse->rtable)->rtekind == RTE_RESULT)
            root->rowMarks = list_delete_cell(root->rowMarks, cell, prev);
        else
            prev = cell;
    }
}

/*
 * get_result_relid
 *             If jtnode is a RangeTblRef for an RTE_RESULT RTE, return its relid;
 *             otherwise return 0.
 */
int get_result_relid(PlannerInfo *root, Node *jtnode)
{
    int                     varno;

    if (!IsA(jtnode, RangeTblRef))
        return 0;
    varno = ((RangeTblRef *) jtnode)->rtindex;
    if (rt_fetch(varno, root->parse->rtable)->rtekind != RTE_RESULT)
        return 0;
    return varno;
}

/*
 * remove_result_refs
 *             Helper routine for dropping an unneeded RTE_RESULT RTE.
 *
 * This doesn't physically remove the RTE from the jointree, because that's
 * more easily handled in remove_useless_results_recurse.  What it does do
 * is the necessary cleanup in the rest of the tree: we must adjust any PHVs
 * that may reference the RTE.  Be sure to call this at a point where the
 * jointree is valid (no disconnected nodes).
 *
 * Note that we don't need to process the append_rel_list, since RTEs
 * referenced directly in the jointree won't be appendrel members.
 *
 * varno is the RTE_RESULT's relid.
 * newjtloc is the jointree location at which any PHVs referencing the
 * RTE_RESULT should be evaluated instead.
 */
void remove_result_refs(PlannerInfo *root, int varno, Node *newjtloc)
{
    /* Fix up PlaceHolderVars as needed */
    /* If there are no PHVs anywhere, we can skip this bit */
    if (root->glob->lastPHId != 0) {
        Relids          subrelids;

        subrelids = get_relids_in_jointree(newjtloc, false);
        Assert(!bms_is_empty(subrelids));
        substitute_multiple_relids((Node *) root->parse, varno, subrelids);
    }

    /*
     * We also need to remove any PlanRowMark referencing the RTE, but we
     * postpone that work until we return to remove_useless_result_rtes.
     */
}

bool find_dependent_phvs_walker(Node *node,
    find_dependent_phvs_context *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        if ((int)(phv->phlevelsup) == context->sublevels_up &&
            bms_equal(context->relids, phv->phrels))
            return true;
            /* fall through to examine children */
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool            result;

        context->sublevels_up++;
        result = query_tree_walker((Query *) node, (bool (*)())find_dependent_phvs_walker,
                                   (void *) context, 0);
        context->sublevels_up--;
        return result;
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    Assert(!IsA(node, SpecialJoinInfo));
    Assert(!IsA(node, AppendRelInfo));
    Assert(!IsA(node, PlaceHolderInfo));
    Assert(!IsA(node, MinMaxAggInfo));

    return expression_tree_walker(node, (bool (*)())find_dependent_phvs_walker,
                                  (void *) context);
}

bool find_dependent_phvs(Node *node, int varno)
{
    find_dependent_phvs_context context;

    context.relids = bms_make_singleton(varno);
    context.sublevels_up = 0;

    /*
     * Must be prepared to start with a Query or a bare expression tree.
     */
    return query_or_expression_tree_walker(node, (bool (*)())find_dependent_phvs_walker,
                                           (void *) &context,
                                           0);
}

/*
 * substitute_multiple_relids - adjust node relid sets after pulling up
 * a subquery
 *
 * Find any PlaceHolderVar nodes in the given tree that reference the
 * pulled-up relid, and change them to reference the replacement relid(s).
 *
 * NOTE: although this has the form of a walker, we cheat and modify the
 * nodes in-place.	This should be OK since the tree was copied by
 * pullup_replace_vars earlier.  Avoid scribbling on the original values of
 * the bitmapsets, though, because expression_tree_mutator doesn't copy those.
 */
typedef struct {
    int varno;
    int sublevels_up;
    Relids subrelids;
} substitute_multiple_relids_context;

static bool substitute_multiple_relids_walker(Node* node, substitute_multiple_relids_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        if ((int)phv->phlevelsup == context->sublevels_up && bms_is_member(context->varno, phv->phrels)) {
            phv->phrels = bms_union(phv->phrels, context->subrelids);
            phv->phrels = bms_del_member(phv->phrels, context->varno);
        }
        /* fall through to examine children */
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())substitute_multiple_relids_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    /* Shouldn't need to handle planner auxiliary nodes here */
    AssertEreport(!IsA(node, SpecialJoinInfo),
        MOD_OPT_REWRITE,
        "Shouldn't need to handle planner auxiliary node SpecialJoinInfo in substitute_multiple_relids_walker");
    Assert(!IsA(node, LateralJoinInfo));
    AssertEreport(!IsA(node, AppendRelInfo),
        MOD_OPT_REWRITE,
        "Shouldn't need to handle planner auxiliary node AppendRelInfo in substitute_multiple_relids_walker");
    AssertEreport(!IsA(node, PlaceHolderInfo),
        MOD_OPT_REWRITE,
        "Shouldn't need to handle planner auxiliary node PlaceHolderInfo in substitute_multiple_relids_walker");
    AssertEreport(!IsA(node, MinMaxAggInfo),
        MOD_OPT_REWRITE,
        "Shouldn't need to handle planner auxiliary node MinMaxAggInfo in substitute_multiple_relids_walker");

    return expression_tree_walker(node, (bool (*)())substitute_multiple_relids_walker, (void*)context);
}

static void substitute_multiple_relids(Node* node, int varno, Relids subrelids)
{
    substitute_multiple_relids_context context;

    context.varno = varno;
    context.sublevels_up = 0;
    context.subrelids = subrelids;

    /*
     * Must be prepared to start with a Query or a bare expression tree.
     */
    (void)query_or_expression_tree_walker(node, (bool (*)())substitute_multiple_relids_walker, (void*)&context, 0);
}

/*
 * fix_append_rel_relids: update RT-index fields of AppendRelInfo nodes
 *
 * When we pull up a subquery, any AppendRelInfo references to the subquery's
 * RT index have to be replaced by the substituted relid (and there had better
 * be only one).  We also need to apply substitute_multiple_relids to their
 * translated_vars lists, since those might contain PlaceHolderVars.
 *
 * We assume we may modify the AppendRelInfo nodes in-place.
 */
static void fix_append_rel_relids(List* append_rel_list, int varno, Relids subrelids)
{
    ListCell* l = NULL;
    int subvarno = -1;

    /*
     * We only want to extract the member relid once, but we mustn't fail
     * immediately if there are multiple members; it could be that none of the
     * AppendRelInfo nodes refer to it.  So compute it on first use. Note that
     * bms_singleton_member will complain if set is not singleton.
     */
    foreach (l, append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(l);

        /* The parent_relid shouldn't ever be a pullup target */
        AssertEreport(appinfo->parent_relid != (uint)varno,
            MOD_OPT_REWRITE,
            "The parent_relid shouldn't ever be a pullup target in fix_append_rel_relids");

        if (appinfo->child_relid == (uint)varno) {
            if (subvarno < 0)
                subvarno = bms_singleton_member(subrelids);
            appinfo->child_relid = subvarno;
        }

        /* Also finish fixups for its translated vars */
        substitute_multiple_relids((Node*)appinfo->translated_vars, varno, subrelids);
    }
}

/*
 * get_relids_in_jointree: get set of RT indexes present in a jointree
 *
 * If include_joins is true, join RT indexes are included; if false,
 * only base rels are included.
 */
Relids get_relids_in_jointree(Node* jtnode, bool include_joins)
{
    Relids result = NULL;

    if (jtnode == NULL)
        return result;
    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;

        result = bms_make_singleton(varno);
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist) {
            result = bms_join(result, get_relids_in_jointree((Node*)lfirst(l), include_joins));
        }
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        result = get_relids_in_jointree(j->larg, include_joins);
        result = bms_join(result, get_relids_in_jointree(j->rarg, include_joins));
        if (include_joins && j->rtindex)
            result = bms_add_member(result, j->rtindex);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
    return result;
}

/*
 * get_relids_for_join: get set of base RT indexes making up a join
 */
Relids get_relids_for_join(PlannerInfo* root, int joinrelid)
{
    Node* jtnode = NULL;

    jtnode = find_jointree_node_for_rel((Node*)root->parse->jointree, joinrelid);
    if (jtnode == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                (errmsg("could not find join node %d", joinrelid))));
    return get_relids_in_jointree(jtnode, false);
}

/*
 * find_jointree_node_for_rel: locate jointree node for a base or join RT index
 *
 * Returns NULL if not found
 */
static Node* find_jointree_node_for_rel(Node* jtnode, int relid)
{
    if (jtnode == NULL)
        return NULL;
    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;

        if (relid == varno)
            return jtnode;
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist) {
            jtnode = find_jointree_node_for_rel((Node*)lfirst(l), relid);
            if (jtnode != NULL)
                return jtnode;
        }
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        if (relid == j->rtindex)
            return jtnode;
        jtnode = find_jointree_node_for_rel(j->larg, relid);
        if (jtnode != NULL)
            return jtnode;
        jtnode = find_jointree_node_for_rel(j->rarg, relid);
        if (jtnode != NULL)
            return jtnode;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
    }
    return NULL;
}

/*
 * We use this function to determine whether we should rewrite a 'full join on P' query to
 * a 'left join on P union all right anti full join on P'.
 * If we can find a condition to redistribute two relations, we will not rewrite, otherwise, we will.
 */
bool equalAndRedistributable(Node* quals)
{
    bool ret = false;

    if (quals == NULL)
        return false;

    switch (nodeTag(quals)) {
        case T_OpExpr: {
            OpExpr* op = (OpExpr*)quals;

            if (list_length(op->args) != 2) /* 2 is the length of arguments to the operator. */
                break;
            /* We only consider system operators. */
            if (op->opno >= FirstNormalObjectId)
                break;

            char* opname = get_opname(op->opno);

            if (opname == NULL)
                break;

            /* If it is a '=' and all of its operators are redistributale,
             * no need to rewrite.
             */
            if (strcmp(opname, "=") == 0) {
                ret = true;
                ListCell* lc = NULL;
                foreach (lc, op->args) {
                    Node* node = (Node*)lfirst(lc);
                    if (!IsTypeDistributable(exprType(node))) {
                        ret = false;
                        break;
                    }
                }
            }
            pfree_ext(opname);
            break;
        }
        case T_List: {
            ListCell* lc = NULL;
            /*
             * If it is a List, all ListCells must be AND-ed, if we find a
             * qual that is '=' and redistributable, then return true;
             */
            foreach (lc, (List*)quals) {
                bool tmp = equalAndRedistributable((Node*)lfirst(lc));
                if (tmp) {
                    ret = true;
                    break;
                }
            }
            break;
        }
        case T_BoolExpr: {
            BoolExpr* op = (BoolExpr*)quals;
            ListCell* lc = NULL;
            /*
             * If it is a List, all ListCells must be AND-ed, if we find a
             * qual that is '=' and redistributable, then return true;
             * All other cases must rewrite.
             */
            if (op->boolop == AND_EXPR) {
                foreach (lc, op->args) {
                    bool tmp = equalAndRedistributable((Node*)lfirst(lc));
                    if (tmp) {
                        ret = true;
                        break;
                    }
                }
            }
            break;
        }
        default:
            /* Default: rewrite */
            ret = false;
            break;
    }
    return ret;
}

/*
 * After rewritting a 'full join on 1=1' to a 'left join union all right anti full join' query,
 * some operations can not be done on subqueries, so we reset them to NULL.
 * These operations will be done on the parent query.
 */
static void reset_operations_need_done_on_parent(Query* query)
{
    AssertEreport(IsA(query, Query), MOD_OPT_REWRITE, "query mismatch in reset_operations_need_done_on_parent");

    query->hasAggs = false;

    query->groupClause = NIL;
    query->groupingSets = NIL;

    query->sortClause = NIL;

    query->distinctClause = NIL;
    query->hasDistinctOn = NIL;

    query->limitCount = NULL;
    query->limitOffset = NULL;

    query->hasWindowFuncs = false;
    query->windowClause = NULL;

    query->havingQual = NULL;

    query->mergeTarget_relation = 0;
    query->mergeSourceTargetList = NIL;
    query->mergeActionList = NIL;
}

/*
 * reduce_inequality_fulljoins
 * Entry function to implement 'full join on 1=1' rewriting. We will call
 * reduce_inequality_fulljoins_jointree_recurse recursively to implement it.
 */
void reduce_inequality_fulljoins(PlannerInfo* root)
{
    Node* jtnode = NULL;

    /* Begin recursion through the jointree */
    jtnode = reduce_inequality_fulljoins_jointree_recurse(root, (Node*)root->parse->jointree);
    /*
     * root->parse->jointree must always be a FromExpr, so insert a dummy one
     * if we got a bare RangeTblRef or JoinExpr out of the recursion.
     */
    if (IsA(jtnode, FromExpr))
        root->parse->jointree = (FromExpr*)jtnode;
    else
        root->parse->jointree = makeFromExpr(list_make1(jtnode), NULL);
}

/*
 * reduce_inequality_fulljoins_jointree_recurse
 *
 * Rewrite a 'full join' to 'left join union all right anti full join'.
 *
 * Consider this query: select * from t1 full join t2 on 1=1
 *
 * In stand-alone system, all data of t1 and t2 are on a same node, we can use
 * merge join to process this query. But, in a distribute system, data of t1 and
 * t2 are redistributed on multiple servers, we can not use merge join directly.
 * If we move all data of t1 and t2 to the same node, this node maybe can not
 * hold it, especially on a cluster which processes large amount of data.
 *
 * This function will rewrite this query to (select *from t1 left join t2 on 1=1)
 * union all (select *from t1 right anti full join t2 on 1=1) NOTE: This query
 * is a user-friendly expression of the internal query tree,and users can not
 * send this query to GaussDB directly.
 *
 * We can only user Nestloop join method to execute this query, because there is
 * no join key.
 *
 * If a query like this: select * from t1 full join t2 on P, and no expressions
 * on P is distributable, we will rewrite the query also. For example, t1.floata
 * = t2.floata, floata's type is float, which is not redistributable. In this
 * example, we can use nestloop, hashjoin or mergejoin after rewritting and
 * broadcast one of the two tables.
 *
 * If we find a JoinExpr which meets the rewritting condition, we begin to rewrite.
 * We copy the original query twice, one of them expresses left join and the other
 * one expresses right anti full join. We use another query which called
 * partial_query to express union query.
 *
 * Some operations, such as limit, agg, etc., can not be execute on the two
 * subqueries copied from the original query, so we should reset this operation
 * nodes to empty.
 *
 * If multiple JoinExpr on the same layer(in the same fromlist), we process them
 * one by one in one recursive invocation.
 *
 * If multiple JoinExpr on different layers, we only process the top layer in
 * one recursive invocation, lower layers will be processesed in deeper recursive
 * invocations.
 */
static Node* reduce_inequality_fulljoins_jointree_recurse(PlannerInfo* root, Node* jtnode)
{
    Assert(IS_STREAM_PLAN);

    if (jtnode == NULL || IsA(jtnode, RangeTblRef)) {
        return jtnode; /* jtnode is returned unmodified */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        List* newfromlist = NIL;
        ListCell* l = NULL;

        foreach (l, f->fromlist) {
            Node* newchild = NULL;

            newchild = reduce_inequality_fulljoins_jointree_recurse(root, (Node*)lfirst(l));
            newfromlist = lappend(newfromlist, newchild);
        }
        /* QUALS SHOULD BE HERE. Build the replacement FromExpr; */
        jtnode = (Node*)makeFromExpr(newfromlist, (Node*)copyObject(f->quals));
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        if (j->jointype == JOIN_FULL && !equalAndRedistributable(j->quals)) {
            Query* partial_query = makeNode(Query);
            Query* setop1 = (Query*)copyObject(root->parse);
            Query* setop2 = (Query*)copyObject(root->parse);

            setop1->is_from_full_join_rewrite = true;
            setop2->is_from_full_join_rewrite = true;

            JoinExpr* j1 = (JoinExpr*)copyObject(j);
            JoinExpr* j2 = (JoinExpr*)copyObject(j);

            RangeTblEntry* joinrte = (RangeTblEntry*)list_nth(root->parse->rtable, j->rtindex - 1);
            RangeTblEntry* joinrte1 = (RangeTblEntry*)list_nth(setop1->rtable, j->rtindex - 1);
            RangeTblEntry* joinrte2 = (RangeTblEntry*)list_nth(setop2->rtable, j->rtindex - 1);

            j1->jointype = JOIN_LEFT;
            j2->jointype = JOIN_RIGHT_ANTI_FULL;
            joinrte1->jointype = JOIN_LEFT;
            joinrte2->jointype = JOIN_RIGHT_ANTI_FULL;

            /* Because the two subqueries are pushed down for 2 layers, fix
             * vars' levelsup in qulas.
             */
            IncrementVarSublevelsUp(j1->quals, 2, 1);
            IncrementVarSublevelsUp(j2->quals, 2, 1);

            /* No quals in FromExpr. Search 'QUALS SHOULD BE HERE.' in this
             * source code file.
             */
            setop1->jointree = makeFromExpr(list_make1(j1), NULL);
            setop2->jointree = makeFromExpr(list_make1(j2), NULL);

            setop1->commandType = CMD_SELECT;
            setop2->commandType = CMD_SELECT;
            setop1->resultRelation = 0;
            setop2->resultRelation = 0;

            int resno = 1;
            ListCell* lc = NULL;
            List* targetList = NIL;
            List* colnames = joinrte->eref->colnames;

            /*
             * Make targetlist for two subqueries.
             * All vars in joinaliasvars should be added to the subquery's targetlist.
             */
            foreach (lc, joinrte->joinaliasvars) {
                TargetEntry* te = NULL;
                Expr* expr = NULL;

                Node* node = (Node*)lfirst(lc);
                char* varname = strVal(list_nth(colnames, resno - 1));
                switch (nodeTag(node)) {
                    case T_Var:
                        expr =
                            (Expr*)makeVar(j->rtindex, resno, exprType(node), exprTypmod(node), exprCollation(node), 0);
                        break;
                    default:
                        expr = (Expr*)copyObject(node);
                        break;
                }

                te = makeTargetEntry((Expr*)expr, resno, varname, false);

                targetList = lappend(targetList, te);
                resno++;
            }
            setop1->targetList = targetList;
            setop2->targetList = targetList;

            /*
             * Reset some members to NULL, such as members related to agg, limit,
             * window function. We will do these operations in the parent query,
             * not in subqueries, so reset them to NULL.
             */
            reset_operations_need_done_on_parent(setop1);
            reset_operations_need_done_on_parent(setop2);

            /* Create RTEs for partial_query */
            RangeTblEntry* rte1 = addRangeTableEntryForSubquery(NULL, setop1, makeAlias("setop1", NIL), false, true);
            RangeTblEntry* rte2 = addRangeTableEntryForSubquery(NULL, setop2, makeAlias("setop2", NIL), false, true);
            partial_query->commandType = CMD_SELECT;
            partial_query->canSetTag = true;
            partial_query->rtable = list_make2(rte1, rte2);
            partial_query->jointree = makeFromExpr(NIL, NULL);
            partial_query->can_push = root->parse->can_push;

            /* Generate setop struct for partial_query */
            SetOperationStmt* op = makeNode(SetOperationStmt);
            op->op = SETOP_UNION;
            op->all = true;
            RangeTblRef* rtr1 = makeNode(RangeTblRef);
            RangeTblRef* rtr2 = makeNode(RangeTblRef);
            rtr1->rtindex = 1;
            rtr2->rtindex = 2;
            op->larg = (Node*)rtr1;
            op->rarg = (Node*)rtr2;

            partial_query->setOperations = (Node*)op;

            int newVarNo = list_length(root->parse->rtable) + 1;

            List* old_tlist = NIL;
            List* new_tlist = NIL;
            AttrNumber iterator_attno = 1;

            /* 1. Create targetList for partial_query
             * 2. Create old_tlist and new_tlist which mapped old vars to new vars.
             *    We will use these two lists to replace old vars in the parent query to new vars.
             */
            foreach (lc, joinrte->joinaliasvars) {
                Node* node = (Node*)lfirst(lc);
                char* varname = strVal(list_nth(colnames, iterator_attno - 1));

                /* For partial_query's targetList */
                Var* pvar = makeVar(1, iterator_attno, exprType(node), exprTypmod(node), exprCollation(node), 0);
                TargetEntry* te = makeTargetEntry((Expr*)pvar,  /* expr */
                    list_length(partial_query->targetList) + 1, /* resno */
                    varname,
                    false);
                partial_query->targetList = lappend(partial_query->targetList, te);

                Node* oldvar = NULL;
                Node* newvar = NULL;

                /* Gernerate old vars */
                switch (node->type) {
                    case T_Var: {
                        Var* var = (Var*)node;
                        Index varno = var->varno;
                        AttrNumber varattno = var->varattno;

                        Node* retnode = get_real_rte_varno_attno_or_node(root->parse, &varno, &varattno);

                        if (retnode == NULL) {
                            oldvar = (Node*)makeVar(
                                varno, varattno, exprType(node), exprTypmod(node), exprCollation(node), 0);
                        } else {
                            oldvar = (Node*)copyObject(retnode);
                        }
                    } break;
                    default:
                        oldvar = (Node*)copyObject(node);
                        break;
                }

                /* New vars that will be used to replace old vars */
                newvar =
                    (Node*)makeVar(newVarNo, iterator_attno, exprType(node), exprTypmod(node), exprCollation(node), 0);

                old_tlist = lappend(old_tlist, oldvar);
                new_tlist = lappend(new_tlist, newvar);

                op->colTypes = lappend_oid(op->colTypes, exprType(node));
                op->colTypmods = lappend_int(op->colTypmods, exprTypmod(node));
                op->colCollations = lappend_oid(op->colCollations, exprCollation(node));

                iterator_attno++;
            }

            /* Generate subquery rte and add it to the range tables */
            RangeTblEntry* rte = addRangeTableEntryForSubquery(NULL, partial_query, makeAlias("subquery", NIL), false, true);
            root->parse->rtable = lappend(root->parse->rtable, rte);

            root->parse->targetList = (List*)replace_node_clause(
                (Node*)root->parse->targetList, (Node*)old_tlist, (Node*)new_tlist, RNC_RECURSE_AGGREF);
            root->parse->jointree = (FromExpr*)replace_node_clause(
                (Node*)root->parse->jointree, (Node*)old_tlist, (Node*)new_tlist, RNC_RECURSE_AGGREF);
            root->parse->havingQual = (Node*)replace_node_clause(
                (Node*)root->parse->havingQual, (Node*)old_tlist, (Node*)new_tlist, RNC_RECURSE_AGGREF);
            root->parse->mergeSourceTargetList = (List*)replace_node_clause(
                (Node*)root->parse->mergeSourceTargetList, (Node*)old_tlist, (Node*)new_tlist, RNC_RECURSE_AGGREF);
            root->parse->mergeActionList = (List*)replace_node_clause(
                (Node*)root->parse->mergeActionList, (Node*)old_tlist, (Node*)new_tlist, RNC_RECURSE_AGGREF);

            /* Use this simple RangeTblRef to replace the JoinExpr that represents full join */
            RangeTblRef* rtr = makeNode(RangeTblRef);
            rtr->rtindex = newVarNo;
            jtnode = (Node*)rtr;
        } else {
            /* Recurse to process children and collect their relids */
            j->larg = reduce_inequality_fulljoins_jointree_recurse(root, j->larg);
            j->rarg = reduce_inequality_fulljoins_jointree_recurse(root, j->rarg);
        }
    } else
        ereport(ERROR,
            ((errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode)))));
    return jtnode;
}

extern bool find_rownum_in_quals(PlannerInfo *root)
{
    if (root->parse == NULL) {
        return false;
    }

    if(root->hasRownumQual) {
        return true;
    }

    bool hasRownum = false;
    ListCell *qualcell = NULL;
    List *quallist = get_quals_lists((Node *)root->parse->jointree);
    foreach (qualcell, quallist) {
        Node *clause = (Node *)lfirst(qualcell);
        if (contain_rownum_walker(clause, NULL)) {
            hasRownum = true;
            break;
        }
    }

    if (quallist) {
        list_free(quallist);
    }

    return hasRownum;
}


bool ContainRownumQual(const Query *parse)
{
    if (!IsA(parse->jointree, FromExpr)) {
        return false;
    }

    return contain_rownum_walker(((FromExpr *)parse->jointree)->quals, NULL);
}

