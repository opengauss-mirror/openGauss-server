/* -------------------------------------------------------------------------
 *
 * rewriteHandler.cpp
 *		Primary module of query rewriter.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/rewrite/rewriteHandler.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/gs_matview.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "commands/trigger.h"
#include "commands/matview.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "parser/parse_merge.h"
#include "parser/parse_hint.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "catalog/pg_constraint.h"
#include "catalog/namespace.h"
#include "client_logic/client_logic.h"
#include "catalog/pg_proc.h"
#include "commands/sqladvisor.h"

#ifdef PGXC
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/ts_redis.h"
#endif
#endif

/* We use a list of these to detect recursion in RewriteQuery */
typedef struct rewrite_event {
    Oid relation;  /* OID of relation having rules */
    CmdType event; /* type of rule being fired */
} rewrite_event;

static bool acquireLocksOnSubLinks(Node* node, void* context);
static Query* rewriteRuleAction(
    Query* parsetree, Query* rule_action, Node* rule_qual, int rt_index, CmdType event, bool* returning_flag);
static List* adjustJoinTreeList(Query* parsetree, bool removert, int rt_index);
static List *rewriteTargetListIU(List *targetList, CmdType commandType, Relation target_relation, int result_rtindex,
    List **attrno_list, bool *hasGenCol);
static TargetEntry* process_matched_tle(TargetEntry* src_tle, TargetEntry* prior_tle, const char* attrName);
static Node* get_assignment_input(Node* node);
static bool rewriteValuesRTE(Query* parsetree, RangeTblEntry* rte, Relation target_relation, List* attrnos,
    bool force_nulls);
static void rewriteTargetListUD(Query* parsetree, RangeTblEntry* target_rte, Relation target_relation, int rtindex);
static void rewriteTargetListMutilUD(Query* parsetree, List* rtable, List* resultRelations);
static void rewriteTargetListMutilUpdate(Query* parsetree, List* rtable, List* resultRelations);
static void markQueryForLocking(Query* qry, Node* jtnode, LockClauseStrength strength, LockWaitPolicy waitPolicy, bool pushedDown,
                                int waitSec);
static List* matchLocks(CmdType event, RuleLock* rulelocks, int varno, Query* parsetree);
static Query* fireRIRrules(Query* parsetree, List* activeRIRs, bool forUpdatePushedDown);
static Bitmapset* adjust_view_column_set(Bitmapset* cols, List* targetlist);
static bool findAttrByName(const char* attributeName, List* tableElts, int maxlen);

#ifdef PGXC
typedef struct pull_qual_vars_context {
    List* varlist;
    int sublevels_up;
    int resultRelation;
    bool noRepeat;
} pull_qual_vars_context;
static bool pull_qual_vars_walker(Node* node, pull_qual_vars_context* context);
#endif

/*
 * AcquireRewriteLocks -
 *	  Acquire suitable locks on all the relations mentioned in the Query.
 *	  These locks will ensure that the relation schemas don't change under us
 *	  while we are rewriting and planning the query.
 *
 * forUpdatePushedDown indicates that a pushed-down FOR UPDATE/SHARE applies
 * to the current subquery, requiring all rels to be opened with RowShareLock.
 * This should always be false at the start of the recursion.
 *
 * Caution: A secondary purpose of this routine is to fix up JOIN RTE references
 * to dropped columns (see details below).  Because the RTEs are modified in
 * place, it is generally appropriate for the caller of this routine to have
 * first done a copyObject() to make a writable copy of the querytree in the
 * current memory context.
 *
 * This processing can, and for efficiency's sake should, be skipped when the
 * querytree has just been built by the parser: parse analysis already got
 * all the same locks we'd get here, and the parser will have omitted dropped
 * columns from JOINs to begin with.  But we must do this whenever we are
 * dealing with a querytree produced earlier than the current command.
 *
 * About JOINs and dropped columns: although the parser never includes an
 * already-dropped column in a JOIN RTE's alias var list, it is possible for
 * such a list in a stored rule to include references to dropped columns.
 * (If the column is not explicitly referenced anywhere else in the query,
 * the dependency mechanism won't consider it used by the rule and so won't
 * prevent the column drop.)  To support get_rte_attribute_is_dropped(),
 * we replace join alias vars that reference dropped columns with NULL Const
 * nodes.
 *
 * (In PostgreSQL 8.0, we did not do this processing but instead had
 * get_rte_attribute_is_dropped() recurse to detect dropped columns in joins.
 * That approach had horrible performance unfortunately; in particular
 * construction of a nested join was O(N^2) in the nesting depth.)
 */
void AcquireRewriteLocks(Query* parsetree, bool forUpdatePushedDown)
{
    ListCell* l = NULL;
    int rt_index;

    /*
     * First, process RTEs of the current query level.
     */
    rt_index = 0;
    foreach (l, parsetree->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);
        Relation rel;
        LOCKMODE lockmode;
        List* newaliasvars = NIL;
        Index curinputvarno;
        RangeTblEntry* curinputrte = NULL;
        ListCell* ll = NULL;

        ++rt_index;
        switch (rte->rtekind) {
            case RTE_RELATION:

                /*
                 * Grab the appropriate lock type for the relation, and do not
                 * release it until end of transaction. This protects the
                 * rewriter and planner against schema changes mid-query.
                 *
                 * If the relation is the query's result relation, then we
                 * need RowExclusiveLock.  Otherwise, check to see if the
                 * relation is accessed FOR [KEY] UPDATE/SHARE or not.  We can't
                 * just grab AccessShareLock because then the executor would
                 * be trying to upgrade the lock, leading to possible
                 * deadlocks.
                 */
                if (rt_index == linitial2_int(parsetree->resultRelations))
                    lockmode = RowExclusiveLock;
                else if (forUpdatePushedDown || get_parse_rowmark(parsetree, rt_index) != NULL)
                    lockmode = RowShareLock;
                else
                    lockmode = AccessShareLock;

                rel = heap_open(rte->relid, lockmode);

                /*
                 * While we have the relation open, update the RTE's relkind,
                 * just in case it changed since this rule was made.
                 */
                rte->relkind = rel->rd_rel->relkind;

                heap_close(rel, NoLock);
                break;

            case RTE_JOIN:

                /*
                 * Scan the join's alias var list to see if any columns have
                 * been dropped, and if so replace those Vars with NULL
                 * Consts.
                 *
                 * Since a join has only two inputs, we can expect to see
                 * multiple references to the same input RTE; optimize away
                 * multiple fetches.
                 */
                newaliasvars = NIL;
                curinputvarno = 0;
                curinputrte = NULL;
                foreach (ll, rte->joinaliasvars) {
                    Var* aliasvar = (Var*)lfirst(ll);

                    /*
                     * If the list item isn't a simple Var, then it must
                     * represent a merged column, ie a USING column, and so it
                     * couldn't possibly be dropped, since it's referenced in
                     * the join clause.  (Conceivably it could also be a NULL
                     * constant already?  But that's OK too.)
                     */
                    if (IsA(aliasvar, Var)) {
                        /*
                         * The elements of an alias list have to refer to
                         * earlier RTEs of the same rtable, because that's the
                         * order the planner builds things in.	So we already
                         * processed the referenced RTE, and so it's safe to
                         * use get_rte_attribute_is_dropped on it. (This might
                         * not hold after rewriting or planning, but it's OK
                         * to assume here.)
                         */
                        AssertEreport(aliasvar->varlevelsup == 0, MOD_OPT, "");
                        if (aliasvar->varno != curinputvarno) {
                            curinputvarno = aliasvar->varno;
                            curinputrte = rt_fetch(curinputvarno, parsetree->rtable);
                        }
                        if (curinputrte != NULL && get_rte_attribute_is_dropped(curinputrte, aliasvar->varattno)) {
                            /*
                             * can't use vartype here, since that might be a
                             * now-dropped type OID, but it doesn't really
                             * matter what type the Const claims to be.
                             */
                            aliasvar = (Var*)makeNullConst(INT4OID, -1, InvalidOid);
                        }
                    }
                    newaliasvars = lappend(newaliasvars, aliasvar);
                }
                rte->joinaliasvars = newaliasvars;
                break;

            case RTE_SUBQUERY:

                /*
                 * The subquery RTE itself is all right, but we have to
                 * recurse to process the represented subquery.
                 */
                AcquireRewriteLocks(
                    rte->subquery, (forUpdatePushedDown || get_parse_rowmark(parsetree, rt_index) != NULL));
                break;

            default:
                /* ignore other types of RTEs */
                break;
        }
    }

    /* Recurse into subqueries in WITH */
    foreach (l, parsetree->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(l);

        AcquireRewriteLocks((Query*)cte->ctequery, false);
    }

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (parsetree->hasSubLinks)
        (void)query_tree_walker(parsetree, (bool (*)())acquireLocksOnSubLinks, NULL, QTW_IGNORE_RC_SUBQUERIES);
}

/*
 * Walker to find sublink subqueries for AcquireRewriteLocks
 */
static bool acquireLocksOnSubLinks(Node* node, void* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubLink)) {
        SubLink* sub = (SubLink*)node;

        /* Do what we came for */
        AcquireRewriteLocks((Query*)sub->subselect, false);
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because AcquireRewriteLocks already
     * processed subselects of subselects for us.
     */
    return expression_tree_walker(node, (bool (*)())acquireLocksOnSubLinks, context);
}

/*
 * Walker to pass down views' invoker info to RangeTableEnrty or FuncExpr in a Query
 * B format mode use this feature
 */
static bool viewSecurityPassDown(Node* node, void* context)
{
    Oid* asUser = (Oid*)context;
    if (node == NULL)
        return false;
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)node;
        /* Do what we came for */
        if (rte->rtekind == RTE_RELATION) {
            rte->checkAsUser = *asUser;
            /* Check namespace permissions. */
            AclResult aclresult;
            /* No lock here ,cause relation already opend */
            Relation rel = heap_open(rte->relid, NoLock);
            Oid namespaceId = RelationGetNamespace(rel);
            aclresult = pg_namespace_aclcheck(namespaceId, *asUser, ACL_USAGE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
            heap_close(rel, NoLock);
        }
        /* allow rangetable entry continue */
        return false;
    } 
    /*
     * Do NOT recurse into Query nodes, because fireRIRrules already processed
     * subselects of subselects for us.
     */
    return expression_tree_walker(node, (bool (*)())viewSecurityPassDown, context);
}
/*
 * rewriteRuleAction -
 *	  Rewrite the rule action with appropriate qualifiers (taken from
 *	  the triggering query).
 *
 * Input arguments:
 *	parsetree - original query
 *	rule_action - one action (query) of a rule
 *	rule_qual - WHERE condition of rule, or NULL if unconditional
 *	rt_index - RT index of result relation in original query
 *	event - type of rule event
 * Output arguments:
 *	*returning_flag - set TRUE if we rewrite RETURNING clause in rule_action
 *					(must be initialized to FALSE)
 * Return value:
 *	rewritten form of rule_action
 */
static Query* rewriteRuleAction(
    Query* parsetree, Query* rule_action, Node* rule_qual, int rt_index, CmdType event, bool* returning_flag)
{
    int current_varno, new_varno;
    int rt_length;
    Query* sub_action = NULL;
    Query** sub_action_ptr;

    /*
     * Make modifiable copies of rule action and qual (what we're passed are
     * the stored versions in the relcache; don't touch 'em!).
     */
    rule_action = (Query*)copyObject(rule_action);
    rule_qual = (Node*)copyObject(rule_qual);

    /*
     * Acquire necessary locks and fix any deleted JOIN RTE entries.
     */
    AcquireRewriteLocks(rule_action, false);
    (void)acquireLocksOnSubLinks(rule_qual, NULL);

    current_varno = rt_index;
    rt_length = list_length(parsetree->rtable);
    new_varno = PRS2_NEW_VARNO + rt_length;

    /*
     * Adjust rule action and qual to offset its varnos, so that we can merge
     * its rtable with the main parsetree's rtable.
     *
     * If the rule action is an INSERT...SELECT, the OLD/NEW rtable entries
     * will be in the SELECT part, and we have to modify that rather than the
     * top-level INSERT (kluge!).
     */
    sub_action = getInsertSelectQuery(rule_action, &sub_action_ptr);

    OffsetVarNodes((Node*)sub_action, rt_length, 0);
    OffsetVarNodes(rule_qual, rt_length, 0);
    /* but references to OLD should point at original rt_index */
    ChangeVarNodes((Node*)sub_action, PRS2_OLD_VARNO + rt_length, rt_index, 0);
    ChangeVarNodes(rule_qual, PRS2_OLD_VARNO + rt_length, rt_index, 0);

    /*
     * Generate expanded rtable consisting of main parsetree's rtable plus
     * rule action's rtable; this becomes the complete rtable for the rule
     * action.	Some of the entries may be unused after we finish rewriting,
     * but we leave them all in place for two reasons:
     *
     * We'd have a much harder job to adjust the query's varnos if we
     * selectively removed RT entries.
     *
     * If the rule is INSTEAD, then the original query won't be executed at
     * all, and so its rtable must be preserved so that the executor will do
     * the correct permissions checks on it.
     *
     * RT entries that are not referenced in the completed jointree will be
     * ignored by the planner, so they do not affect query semantics.  But any
     * permissions checks specified in them will be applied during executor
     * startup (see ExecCheckRTEPerms()).  This allows us to check that the
     * caller has, say, insert-permission on a view, when the view is not
     * semantically referenced at all in the resulting query.
     *
     * When a rule is not INSTEAD, the permissions checks done on its copied
     * RT entries will be redundant with those done during execution of the
     * original query, but we don't bother to treat that case differently.
     *
     * NOTE: because planner will destructively alter rtable, we must ensure
     * that rule action's rtable is separate and shares no substructure with
     * the main rtable.  Hence do a deep copy here.
     */
    sub_action->rtable = list_concat((List*)copyObject(parsetree->rtable), sub_action->rtable);

    /*
     * There could have been some SubLinks in parsetree's rtable, in which
     * case we'd better mark the sub_action correctly.
     */
    if (parsetree->hasSubLinks && !sub_action->hasSubLinks) {
        ListCell* lc = NULL;

        foreach (lc, parsetree->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

            switch (rte->rtekind) {
                case RTE_RELATION:
                    sub_action->hasSubLinks = checkExprHasSubLink((Node*)rte->tablesample)
                        || checkExprHasSubLink((Node*)rte->timecapsule);
                    break;
                case RTE_FUNCTION:
                    sub_action->hasSubLinks = checkExprHasSubLink(rte->funcexpr);
                    break;
                case RTE_VALUES:
                    sub_action->hasSubLinks = checkExprHasSubLink((Node*)rte->values_lists);
                    break;
                default:
                    /* other RTE types don't contain bare expressions */
                    break;
            }
            if (sub_action->hasSubLinks)
                break; /* no need to keep scanning rtable */
        }
    }

    /*
     * Also, we might have absorbed some RTEs with RLS conditions into the
     * sub_action.  If so, mark it as hasRowSecurity, whether or not those
     * RTEs will be referenced after we finish rewriting.  (Note: currently
     * this is a no-op because RLS conditions aren't added till later, but it
     * seems like good future-proofing to do this anyway.)
     */
    sub_action->hasRowSecurity = (sub_action->hasRowSecurity || parsetree->hasRowSecurity);

    /*
     * Each rule action's jointree should be the main parsetree's jointree
     * plus that rule's jointree, but usually *without* the original rtindex
     * that we're replacing (if present, which it won't be for INSERT). Note
     * that if the rule action refers to OLD, its jointree will add a
     * reference to rt_index.  If the rule action doesn't refer to OLD, but
     * either the rule_qual or the user query quals do, then we need to keep
     * the original rtindex in the jointree to provide data for the quals.	We
     * don't want the original rtindex to be joined twice, however, so avoid
     * keeping it if the rule action mentions it.
     *
     * As above, the action's jointree must not share substructure with the
     * main parsetree's.
     */
    if (sub_action->commandType != CMD_UTILITY) {
        bool keeporig = false;
        List* newjointree = NIL;

        AssertEreport(sub_action->jointree != NULL, MOD_OPT, "");
        keeporig = (!rangeTableEntry_used((Node*)sub_action->jointree, rt_index, 0)) &&
                   (rangeTableEntry_used(rule_qual, rt_index, 0) ||
                       rangeTableEntry_used(parsetree->jointree->quals, rt_index, 0));
        newjointree = adjustJoinTreeList(parsetree, !keeporig, rt_index);
        if (newjointree != NIL) {
            /*
             * If sub_action is a setop, manipulating its jointree will do no
             * good at all, because the jointree is dummy.	(Perhaps someday
             * we could push the joining and quals down to the member
             * statements of the setop?)
             */
            if (sub_action->setOperations != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));

            sub_action->jointree->fromlist = list_concat(newjointree, sub_action->jointree->fromlist);

            /*
             * There could have been some SubLinks in newjointree, in which
             * case we'd better mark the sub_action correctly.
             */
            if (parsetree->hasSubLinks && !sub_action->hasSubLinks)
                sub_action->hasSubLinks = checkExprHasSubLink((Node*)newjointree);
        }
    }

    /*
     * If the original query has any CTEs, copy them into the rule action. But
     * we don't need them for a utility action.
     */
    if (parsetree->cteList != NIL && sub_action->commandType != CMD_UTILITY) {
        ListCell* lc = NULL;

        /*
         * Annoying implementation restriction: because CTEs are identified by
         * name within a cteList, we can't merge a CTE from the original query
         * if it has the same name as any CTE in the rule action.
         *
         * This could possibly be fixed by using some sort of internally
         * generated ID, instead of names, to link CTE RTEs to their CTEs.
         */
        foreach (lc, parsetree->cteList) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
            ListCell* lc2 = NULL;

            foreach (lc2, sub_action->cteList) {
                CommonTableExpr* cte2 = (CommonTableExpr*)lfirst(lc2);

                if (strcmp(cte->ctename, cte2->ctename) == 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("WITH query name \"%s\" appears in both a rule action and the query being rewritten",
                                cte->ctename)));
            }
        }

        /* OK, it's safe to combine the CTE lists */
        sub_action->cteList = list_concat(sub_action->cteList, (List*)copyObject(parsetree->cteList));
    }

    /*
     * Event Qualification forces copying of parsetree and splitting into two
     * queries one w/rule_qual, one w/NOT rule_qual. Also add user query qual
     * onto rule action
     */
    AddQual(sub_action, rule_qual);

    AddQual(sub_action, parsetree->jointree->quals);

    /*
     * Rewrite new.attribute with right hand side of target-list entry for
     * appropriate field name in insert/update.
     *
     * KLUGE ALERT: since ReplaceVarsFromTargetList returns a mutated copy, we
     * can't just apply it to sub_action; we have to remember to update the
     * sublink inside rule_action, too.
     */
    if ((event == CMD_INSERT || event == CMD_UPDATE) && sub_action->commandType != CMD_UTILITY) {
        sub_action = (Query*)ReplaceVarsFromTargetList((Node*)sub_action,
            new_varno,
            0,
            rt_fetch(new_varno, sub_action->rtable),
            parsetree->targetList,
            (event == CMD_UPDATE) ?
            REPLACEVARS_CHANGE_VARNO :
            REPLACEVARS_SUBSTITUTE_NULL,
            current_varno,
            NULL);
        if (sub_action_ptr != NULL)
            *sub_action_ptr = sub_action;
        else
            rule_action = sub_action;
    }

    /*
     * If rule_action has a RETURNING clause, then either throw it away if the
     * triggering query has no RETURNING clause, or rewrite it to emit what
     * the triggering query's RETURNING clause asks for.  Throw an error if
     * more than one rule has a RETURNING clause.
     */
    if (!parsetree->returningList)
        rule_action->returningList = NIL;
    else if (rule_action->returningList) {
        if (*returning_flag)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot have RETURNING lists in multiple rules")));
        *returning_flag = true;
        rule_action->returningList = (List*)ReplaceVarsFromTargetList((Node*)parsetree->returningList,
            linitial_int(parsetree->resultRelations),
            0,
            rt_fetch(linitial_int(parsetree->resultRelations), parsetree->rtable),
            rule_action->returningList,
            REPLACEVARS_REPORT_ERROR,
            0,
            &rule_action->hasSubLinks);

        /*
         * There could have been some SubLinks in parsetree's returningList,
         * in which case we'd better mark the rule_action correctly.
         */
        if (parsetree->hasSubLinks && !rule_action->hasSubLinks)
            rule_action->hasSubLinks = checkExprHasSubLink((Node*)rule_action->returningList);
    }
    return rule_action;
}

/*
 * Copy the query's jointree list, and optionally attempt to remove any
 * occurrence of the given rt_index as a top-level join item (we do not look
 * for it within join items; this is OK because we are only expecting to find
 * it as an UPDATE or DELETE target relation, which will be at the top level
 * of the join).  Returns modified jointree list --- this is a separate copy
 * sharing no nodes with the original.
 */
static List* adjustJoinTreeList(Query* parsetree, bool removert, int rt_index)
{
    List* newjointree = (List*)copyObject(parsetree->jointree->fromlist);
    ListCell* l = NULL;

    if (removert) {
        foreach (l, newjointree) {
            RangeTblRef* rtr = (RangeTblRef*)lfirst(l);

            if (IsA(rtr, RangeTblRef) && rtr->rtindex == rt_index) {
                newjointree = list_delete_ptr(newjointree, rtr);

                /*
                 * foreach is safe because we exit loop after list_delete...
                 */
                break;
            }
        }
    }
    return newjointree;
}

/*
 * rewriteTargetListIU - rewrite INSERT/UPDATE targetlist into standard form
 *
 * This has the following responsibilities:
 *
 * 1. For an INSERT, add tlist entries to compute default values for any
 * attributes that have defaults and are not assigned to in the given tlist.
 * (We do not insert anything for default-less attributes, however.  The
 * planner will later insert NULLs for them, but there's no reason to slow
 * down rewriter processing with extra tlist nodes.)  Also, for both INSERT
 * and UPDATE, replace explicit DEFAULT specifications with column default
 * expressions.
 *
 * 2. For an UPDATE on a trigger-updatable view, add tlist entries for any
 * unassigned-to attributes, assigning them their old values.  These will
 * later get expanded to the output values of the view.  (This is equivalent
 * to what the planner's expand_targetlist() will do for UPDATE on a regular
 * table, but it's more convenient to do it here while we still have easy
 * access to the view's original RT index.)  This is only necessary for
 * trigger-updatable views, for which the view remains the result relation of
 * the query.  For auto-updatable views we must not do this, since it might
 * add assignments to non-updatable view columns.  For rule-updatable views it
 * is unnecessary extra work, since the query will be rewritten with a
 * different result relation which will be processed when we recurse via
 * RewriteQuery.
 *
 * 3. Merge multiple entries for the same target attribute, or declare error
 * if we can't.  Multiple entries are only allowed for INSERT/UPDATE of
 * portions of an array or record field, for example
 *			UPDATE table SET foo[2] = 42, foo[4] = 43;
 * We can merge such operations into a single assignment op.  Essentially,
 * the expression we want to produce in this case is like
 *		foo = array_set(array_set(foo, 2, 42), 4, 43)
 *
 * 4. Sort the tlist into standard order: non-junk fields in order by resno,
 * then junk fields (these in no particular order).
 *
 * We must do items 1,2,3 before firing rewrite rules, else rewritten
 * references to NEW.foo will produce wrong or incomplete results.	Item 4
 * is not needed for rewriting, but will be needed by the planner, and we
 * can do it essentially for free while handling the other items.
 *
 * If attrno_list isn't NULL, we return an additional output besides the
 * rewritten targetlist: an integer list of the assigned-to attnums, in
 * order of the original tlist's non-junk entries.  This is needed for
 * processing VALUES RTEs.
 */
static List* rewriteTargetListIU(List* targetList, CmdType commandType, Relation target_relation,
    int result_rtindex, List** attrno_list, bool* hasGenCol)
{
    TargetEntry** new_tles;
    List* new_tlist = NIL;
    List* junk_tlist = NIL;
    Form_pg_attribute att_tup;
    int attrno, next_junk_attrno, numattrs;
    ListCell* temp = NULL;

    if (attrno_list != NULL) /* initialize optional result list */
        *attrno_list = NIL;

    /*
     * We process the normal (non-junk) attributes by scanning the input tlist
     * once and transferring TLEs into an array, then scanning the array to
     * build an output tlist.  This avoids O(N^2) behavior for large numbers
     * of attributes.
     *
     * Junk attributes are tossed into a separate list during the same tlist
     * scan, then appended to the reconstructed tlist.
     */
    numattrs = RelationGetNumberOfAttributes(target_relation);
    new_tles = (TargetEntry**)palloc0(numattrs * sizeof(TargetEntry*));
    next_junk_attrno = numattrs + 1;

    foreach (temp, targetList) {
        TargetEntry* old_tle = (TargetEntry*)lfirst(temp);

        if (!old_tle->resjunk) {
            /* Normal attr: stash it into new_tles[] */
            attrno = old_tle->resno;
            if (attrno < 1 || attrno > numattrs) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("bogus resno %d in targetlist", attrno)));
            }
            att_tup = &target_relation->rd_att->attrs[attrno - 1];

            /* put attrno into attrno_list even if it's dropped */
            if (attrno_list != NULL && IsA(old_tle->expr, Var))
                *attrno_list = lappend_int(*attrno_list, attrno);

            /* We can (and must) ignore deleted attributes */
            if (att_tup->attisdropped)
                continue;

            /* Merge with any prior assignment to same attribute */
            new_tles[attrno - 1] = process_matched_tle(old_tle, new_tles[attrno - 1], NameStr(att_tup->attname));
        } else {
            /*
             * Copy all resjunk tlist entries to junk_tlist, and assign them
             * resnos above the last real resno.
             *
             * Typical junk entries include ORDER BY or GROUP BY expressions
             * (are these actually possible in an INSERT or UPDATE?), system
             * attribute references, etc.
             *
             * Get the resno right, but don't copy unnecessarily
             */
            if (old_tle->resno != next_junk_attrno) {
                old_tle = flatCopyTargetEntry(old_tle);
                old_tle->resno = next_junk_attrno;
            }
            junk_tlist = lappend(junk_tlist, old_tle);
            next_junk_attrno++;
        }
    }

    for (attrno = 1; attrno <= numattrs; attrno++) {
        TargetEntry* new_tle = new_tles[attrno - 1];
        bool applyDefault = false;
        bool generateCol = ISGENERATEDCOL(target_relation->rd_att, attrno - 1);

        att_tup = &target_relation->rd_att->attrs[attrno - 1];

        /* We can (and must) ignore deleted attributes */
        if (att_tup->attisdropped)
            continue;

        /*
         * Handle the two cases where we need to insert a default expression:
         * it's an INSERT and there's no tlist entry for the column, or the
         * tlist entry is a DEFAULT placeholder node.
         */
        applyDefault = ((new_tle == NULL && commandType == CMD_INSERT) ||
            (new_tle != NULL && new_tle->expr != NULL && IsA(new_tle->expr, SetToDefault)));

        if (generateCol && !applyDefault) {
            if (commandType == CMD_INSERT && attrno_list == NULL) {
                ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("cannot insert into column \"%s\"", NameStr(att_tup->attname)),
                    errdetail("Column \"%s\" is a generated column.", NameStr(att_tup->attname))));
            }
            if (commandType == CMD_UPDATE && new_tle) {
                ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("column \"%s\" can only be updated to DEFAULT", NameStr(att_tup->attname)),
                    errdetail("Column \"%s\" is a generated column.", NameStr(att_tup->attname))));
            }
        }

        if (generateCol) {
            /*
             * stored generated column will be fixed in executor
             */
            new_tle = NULL;
            *hasGenCol = true;
        } else if (applyDefault) {
            Node* new_expr = NULL;

            new_expr = build_column_default(target_relation, attrno, true);

            /*
             * If there is no default (ie, default is effectively NULL), we
             * can omit the tlist entry in the INSERT case, since the planner
             * can insert a NULL for itself, and there's no point in spending
             * any more rewriter cycles on the entry.  But in the UPDATE case
             * we've got to explicitly set the column to NULL.
             */
            if (new_expr == NULL) {
                if (commandType == CMD_INSERT) {
                    new_tle = NULL;
                    ereport(DEBUG2, (errmodule(MOD_PARSER), errcode(ERRCODE_LOG),
                        errmsg("default column \"%s\" is effectively NULL, and hence omitted.",
                            NameStr(att_tup->attname))));
                } else if (target_relation->rd_rel->relkind != RELKIND_VIEW) {
                    new_expr = (Node*)makeConst(att_tup->atttypid,
                        -1,
                        att_tup->attcollation,
                        att_tup->attlen,
                        (Datum)0,
                        true, /* isnull */
                        att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(
                        new_expr, InvalidOid, -1, att_tup->atttypid, COERCE_IMPLICIT_CAST, -1, false, false);
                }
            }

            if (new_expr != NULL)
                new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
        }

        /*
         * For an UPDATE on a view, provide a dummy entry whenever there is no
         * explicit assignment.
         */
        if (new_tle == NULL && commandType == CMD_UPDATE && ((target_relation->rd_rel->relkind == RELKIND_VIEW
            && view_has_instead_trigger(target_relation, CMD_UPDATE))
            || target_relation->rd_rel->relkind == RELKIND_CONTQUERY)) {
            Node* new_expr = NULL;

            new_expr = (Node*)makeVar(
                result_rtindex, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
            new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
        }

        if (new_tle != NULL)
            new_tlist = lappend(new_tlist, new_tle);
    }

    pfree_ext(new_tles);

    targetList = list_concat(new_tlist, junk_tlist);
    return targetList;
}

static void rewriteTargetListMutilUpdate(Query* parsetree, List* rtable, List* resultRelations)
{
    TargetEntry** new_tles;
    List* new_tlist = NIL;
    List* junk_tlist = NIL;
    Form_pg_attribute att_tup;
    int attrno, numattrs;
    ListCell* l = NULL;
    int result_relation;

    /*
     * We process the normal (non-junk) attributes by scanning the input tlist
     * once and transferring TLEs into an array, then scanning the array to
     * build an output tlist.  This avoids O(N^2) behavior for large numbers
     * of attributes.
     *
     * Junk attributes are tossed into a separate list during the same tlist
     * scan, then appended to the reconstructed tlist.
     */
    ListCell* temp = list_head(parsetree->targetList);
    TargetEntry* old_tle = (TargetEntry*)lfirst(temp);

    foreach (l, resultRelations) {
        result_relation = lfirst_int(l);
        Assert(((TargetEntry*)lfirst(temp))->rtindex == (Index)result_relation);

        RangeTblEntry* rt_entry = rt_fetch(result_relation, rtable);
        Relation target_relation = heap_open(rt_entry->relid, NoLock);
        numattrs = RelationGetNumberOfAttributes(target_relation);

        new_tles = (TargetEntry**)palloc0(numattrs * sizeof(TargetEntry*));
        
        while (temp != NULL) {
            old_tle = (TargetEntry*)lfirst(temp);
            /*
             * For multi-relations update, old_tle has been sorted according to the order of each result relation.
             * So when old_tle->rtindex != (Index)result_relation, break and process the next step.
             */
            if (old_tle->rtindex != (Index)result_relation)
                break;

            if (!old_tle->resjunk) {
                /* Normal attr: stash it into new_tles[] */
                attrno = old_tle->resno;
                if (attrno < 1 || attrno > numattrs) {
                    ereport(ERROR,
                        (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("bogus resno %d in targetlist",
                                                                               attrno)));
                }
                att_tup = &target_relation->rd_att->attrs[attrno - 1];

                /* We can (and must) ignore deleted attributes */
                if (att_tup->attisdropped) {
                    temp = lnext(temp);
                    continue;
                }
                /* Merge with any prior assignment to same attribute */
                new_tles[attrno - 1] = process_matched_tle(old_tle, new_tles[attrno - 1], NameStr(att_tup->attname));
            } else {
                /*
                * Copy all resjunk tlist entries to junk_tlist, and assign them
                * resnos above the last real resno.
                *
                * Typical junk entries include ORDER BY or GROUP BY expressions
                * (are these actually possible in an INSERT or UPDATE?), system
                * attribute references, etc.
                *
                * Get the resno right, but don't copy unnecessarily
                */
                old_tle = flatCopyTargetEntry(old_tle);
                junk_tlist = lappend(junk_tlist, old_tle);
            }
            temp = lnext(temp);
        }

        for (attrno = 1; attrno <= numattrs; attrno++) {
            TargetEntry* new_tle = new_tles[attrno - 1];
            /*
            * We need to insert a default expression when the
            * tlist entry is a DEFAULT placeholder node.
            */
            bool applyDefault = new_tle != NULL && new_tle->expr != NULL && IsA(new_tle->expr, SetToDefault);
            bool generateCol = ISGENERATEDCOL(target_relation->rd_att, attrno - 1);

            att_tup = &target_relation->rd_att->attrs[attrno - 1];

            /* We can (and must) ignore deleted attributes */
            if (att_tup->attisdropped)
                continue;

            if (generateCol && !applyDefault && new_tle) {
                    ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("column \"%s\" can only be updated to DEFAULT", NameStr(att_tup->attname)),
                        errdetail("Column \"%s\" is a generated column.", NameStr(att_tup->attname))));
            }

            if (generateCol) {
                /*
                * stored generated column will be fixed in executor
                */
                new_tle = NULL;
            } else if (applyDefault) {
                Node* new_expr = build_column_default(target_relation, attrno, true);
                if (new_expr == NULL && target_relation->rd_rel->relkind != RELKIND_VIEW) {
                    new_expr = (Node*)makeConst(att_tup->atttypid,
                        -1,
                        att_tup->attcollation,
                        att_tup->attlen,
                        (Datum)0,
                        true, /* isnull */
                        att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(
                        new_expr, InvalidOid, -1, att_tup->atttypid, COERCE_IMPLICIT_CAST, -1, false, false);
                }
                if (new_expr != NULL) {
                    new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
                }
                new_tle->rtindex = result_relation;
            }

            /*
            * For an UPDATE on a view, provide a dummy entry whenever there is no
            * explicit assignment.
            */
            if (new_tle == NULL && ((target_relation->rd_rel->relkind == RELKIND_VIEW
                && view_has_instead_trigger(target_relation, CMD_UPDATE))
                || target_relation->rd_rel->relkind == RELKIND_CONTQUERY)) {
                Node* new_expr = NULL;

                new_expr = (Node*)makeVar(
                    result_relation, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
                new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
                new_tle->rtindex = result_relation;
            }

            if (new_tle != NULL)
                new_tlist = lappend(new_tlist, new_tle);
        }
    
        heap_close(target_relation, NoLock);
        pfree_ext(new_tles);
    }

    int next_junk_attrno = list_length(new_tlist) + 1;

    foreach (l, junk_tlist) {
        old_tle = (TargetEntry*)lfirst(l);
        if (old_tle->resno != next_junk_attrno) {
            old_tle = flatCopyTargetEntry(old_tle);
            old_tle->resno = next_junk_attrno;
        }
        new_tlist = lappend(new_tlist, old_tle);
        next_junk_attrno++;
    }

    parsetree->targetList = new_tlist;

    rewriteTargetListMutilUD(parsetree, rtable, resultRelations);
}

static void multiUpdateSetExtraUpdatedCols(Query* parsetree)
{
    ListCell* lc = NULL;
    RangeTblEntry* rte = NULL;
    Relation rel;

    foreach (lc, parsetree->resultRelations) {
        rte = rt_fetch(lfirst_int(lc), parsetree->rtable);
        rel = heap_open(rte->relid, NoLock);
        setExtraUpdatedCols(rte, rel->rd_att);
        heap_close(rel, NoLock);
    }
}

/*
 * Convert a matched TLE from the original tlist into a correct new TLE.
 *
 * This routine detects and handles multiple assignments to the same target
 * attribute.  (The attribute name is needed only for error messages.)
 */
static TargetEntry* process_matched_tle(TargetEntry* src_tle, TargetEntry* prior_tle, const char* attrName)
{
    TargetEntry* result = NULL;
    Node* src_expr = NULL;
    Node* prior_expr = NULL;
    Node* src_input = NULL;
    Node* prior_input = NULL;
    Node* priorbottom = NULL;
    Node* newexpr = NULL;
    errno_t errorno = EOK;

    if (prior_tle == NULL) {
        /*
         * Normal case where this is the first assignment to the attribute.
         */
        return src_tle;
    }

    /* ----------
     * Multiple assignments to same attribute.	Allow only if all are
     * FieldStore or ArrayRef assignment operations.  This is a bit
     * tricky because what we may actually be looking at is a nest of
     * such nodes; consider
     *		UPDATE tab SET col.fld1.subfld1 = x, col.fld2.subfld2 = y
     * The two expressions produced by the parser will look like
     *		FieldStore(col, fld1, FieldStore(placeholder, subfld1, x))
     *		FieldStore(col, fld2, FieldStore(placeholder, subfld2, x))
     * However, we can ignore the substructure and just consider the top
     * FieldStore or ArrayRef from each assignment, because it works to
     * combine these as
     *		FieldStore(FieldStore(col, fld1,
     *							  FieldStore(placeholder, subfld1, x)),
     *				   fld2, FieldStore(placeholder, subfld2, x))
     * Note the leftmost expression goes on the inside so that the
     * assignments appear to occur left-to-right.
     *
     * For FieldStore, instead of nesting we can generate a single
     * FieldStore with multiple target fields.	We must nest when
     * ArrayRefs are involved though.
     * ----------
     */
    src_expr = (Node*)src_tle->expr;
    prior_expr = (Node*)prior_tle->expr;
    src_input = get_assignment_input(src_expr);
    prior_input = get_assignment_input(prior_expr);
    if (src_input == NULL || prior_input == NULL || exprType(src_expr) != exprType(prior_expr)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("multiple assignments to same column \"%s\"", attrName)));
    }
    /*
     * Prior TLE could be a nest of assignments if we do this more than once.
     */
    priorbottom = prior_input;
    for (;;) {
        Node* newbottom = get_assignment_input(priorbottom);

        if (newbottom == NULL) {
            break; /* found the original Var reference */
        }
        priorbottom = newbottom;
    }
    if (!equal(priorbottom, src_input)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("multiple assignments to same column \"%s\"", attrName)));
    }
    /*
     * Looks OK to nest 'em.
     */
    size_t fstore_len = sizeof(FieldStore);
    if (IsA(src_expr, FieldStore)) {
        FieldStore* fstore = makeNode(FieldStore);

        if (IsA(prior_expr, FieldStore)) {
            /* combine the two */
            errorno = memcpy_s(fstore, fstore_len, prior_expr, fstore_len);
            securec_check(errorno, "\0", "\0");
            fstore->newvals =
                list_concat(list_copy(((FieldStore*)prior_expr)->newvals), list_copy(((FieldStore*)src_expr)->newvals));
            fstore->fieldnums = list_concat(
                list_copy(((FieldStore*)prior_expr)->fieldnums), list_copy(((FieldStore*)src_expr)->fieldnums));
        } else {
            /* general case, just nest 'em */
            errorno = memcpy_s(fstore, fstore_len, src_expr, fstore_len);
            securec_check(errorno, "\0", "\0");
            fstore->arg = (Expr*)prior_expr;
        }
        newexpr = (Node*)fstore;
    } else if (IsA(src_expr, ArrayRef)) {
        ArrayRef* aref = makeNode(ArrayRef);

        errorno = memcpy_s(aref, sizeof(ArrayRef), src_expr, sizeof(ArrayRef));
        securec_check(errorno, "\0", "\0");
        aref->refexpr = (Expr*)prior_expr;
        newexpr = (Node*)aref;
    } else {
        ereport(ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("cannot happen")));

        newexpr = NULL;
    }

    result = flatCopyTargetEntry(src_tle);
    result->expr = (Expr*)newexpr;
    return result;
}

/*
 * If node is an assignment node, return its input; else return NULL
 */
static Node* get_assignment_input(Node* node)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, FieldStore)) {
        FieldStore* fstore = (FieldStore*)node;

        return (Node*)fstore->arg;
    } else if (IsA(node, ArrayRef)) {
        ArrayRef* aref = (ArrayRef*)node;

        if (aref->refassgnexpr == NULL)
            return NULL;
        return (Node*)aref->refexpr;
    }
    return NULL;
}

static bool check_sequence_return_numeric_walker(Node *node, int *ret)
{
    /* Traverse through the expression tree and check requence related function return type */
    if (node == NULL) {
        *ret = NDE_UNKNOWN;
        return false;
    }

    if (IsA(node, Query)) {
        return (Node *)query_tree_walker((Query *)node, (bool (*)())check_sequence_return_numeric_walker,
            (void *)ret, 0);
    }

    if (IsA(node, FuncExpr)) {
        FuncExpr *func = (FuncExpr *)node;
        if (func->funcid == NEXTVALFUNCOID || func->funcid == CURRVALFUNCOID || func->funcid == LASTVALFUNCOID) {
            if (func->funcresulttype == NUMERICOID) {
                *ret = NDE_NUMERIC;
                return true;
            } else {
                *ret = NDE_BIGINT;
                return true;
            }
        }
    }

    return expression_tree_walker(node, (bool (*)())check_sequence_return_numeric_walker, (void *)ret);
}

/*
 * Make an expression tree for the default value for a column.
 *
 * If there is no default, return a NULL instead.
 * Add one input arg isInsertCmd to show if current statement is insert.
 * If auto truncation function enabled and it is insert statement then
 * we use this arg to determin if default should be casted explict.
 */
Node* build_column_default(Relation rel, int attrno, bool isInsertCmd, bool needOnUpdate)
{
    TupleDesc rd_att = rel->rd_att;
    Form_pg_attribute att_tup = &rd_att->attrs[attrno - 1];
    Oid atttype = att_tup->atttypid;
    int32 atttypmod = att_tup->atttypmod;
    Node* expr = NULL;
    Oid exprtype;

    /*
     * Scan to see if relation has a default for this column.
     */
    if (rd_att->constr && rd_att->constr->num_defval > 0) {
        AttrDefault* defval = rd_att->constr->defval;
        int ndef = rd_att->constr->num_defval;

        while (--ndef >= 0) {
            if (attrno == defval[ndef].adnum) {
                /*
                 * Found it, convert string representation to node tree.
                 *
                 * isInsertCmd is false, has_on_update is true and adbin_on_update is not null character string,
                 * then doing convert adbin_on_update to expression.
                 * if adbin is not null character string, then doing convert adbin to expression.
                 */
                
                if (needOnUpdate && (!isInsertCmd) && defval[ndef].adbin_on_update != nullptr &&
                    pg_strcasecmp(defval[ndef].adbin_on_update, "") != 0) {
                    expr = (Node*)stringToNode_skip_extern_fields(defval[ndef].adbin_on_update);
                } else if (defval[ndef].adbin != nullptr && pg_strcasecmp(defval[ndef].adbin, "") != 0) {
                    expr = (Node*)stringToNode_skip_extern_fields(defval[ndef].adbin);
                }
                if (t_thrd.proc->workingVersionNum < LARGE_SEQUENCE_VERSION_NUM) {
                    (void)check_sequence_return_numeric_walker(expr, &(u_sess->opt_cxt.nextval_default_expr_type));
                }
                break;
            }
        }
    }

    if (expr == NULL && !ISGENERATEDCOL(rd_att, attrno - 1)) {
        /*
         * No per-column default, so look for a default for the type itself.But
         * not for generated columns.
         */
        expr = get_typdefault(atttype);
    }

    if (expr == NULL)
        return NULL; /* No default anywhere */

    if (IsA(expr, AutoIncrement)) {
        expr = (Node*)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), !att_tup->attnotnull, true);
    }

    /*
     * Make sure the value is coerced to the target column type; this will
     * generally be true already, but there seem to be some corner cases
     * involving domain defaults where it might not be true. This should match
     * the parser's processing of non-defaulted expressions --- see
     * transformAssignedExpr().
     */
    exprtype = exprType(expr);

    expr = coerce_to_target_type(NULL, /* no UNKNOWN params here */
        expr,
        exprtype,
        atttype,
        atttypmod,
        COERCION_ASSIGNMENT,
        COERCE_IMPLICIT_CAST,
        -1);

    /*
     * When td_compatible_truncation is set to on, this part of code will set column default
     * value to isExplicte args to true, to let bpchar know one explict cast has been added to
     * this default value already.
     */
    if (u_sess->attr.attr_sql.td_compatible_truncation && u_sess->attr.attr_sql.sql_compatibility == C_FORMAT &&
        isInsertCmd && (atttype == BPCHAROID || atttype == VARCHAROID) && expr != NULL) {
        AssertEreport(IsA(expr, FuncExpr), MOD_OPT, "");

        FuncExpr* fe = (FuncExpr*)expr;
        Const* const_arg = (Const*)llast(fe->args);
        if (IsA(const_arg, Const) && const_arg->consttype == BOOLOID)
            const_arg->constvalue = (Datum) true;
    }

    if (expr == NULL)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("column \"%s\" is of type %s but %s expression is of type %s", NameStr(att_tup->attname),
            format_type_be(atttype), ISGENERATEDCOL(rd_att, attrno - 1) ? "generated column" : "deault",
            format_type_be(exprtype)), errhint("You will need to rewrite or cast the expression.")));
    /*
     * If there is nextval FuncExpr, we should lock the quoted sequence to avoid deadlock,
     * this has beed done in transformFuncExpr. See lockNextvalOnCn for more details.
     */
    (void)lockNextvalWalker(expr, NULL);
    return expr;
}

/* Does VALUES RTE contain any SetToDefault items? */
static bool searchForDefault(RangeTblEntry* rte)
{
    ListCell* lc = NULL;

    foreach (lc, rte->values_lists) {
        List* sublist = (List*)lfirst(lc);
        ListCell* lc2 = NULL;

        foreach (lc2, sublist) {
            Node* col = (Node*)lfirst(lc2);

            if (IsA(col, SetToDefault))
                return true;
        }
    }
    return false;
}

static void checkGenDefault(RangeTblEntry* rte, Relation target_relation, List* attrnos, bool hasGenCol)
{
    ListCell* lc = NULL;

    if (!hasGenCol) {
        return ;
    }

    foreach (lc, rte->values_lists) {
        List* sublist = (List*)lfirst(lc);
        ListCell* lc2 = NULL;
        ListCell* lc3 = NULL;

        forboth (lc2, sublist, lc3, attrnos) {
            Node* col = (Node*)lfirst(lc2);
            int attrno = lfirst_int(lc3);
            Form_pg_attribute att_tup = &target_relation->rd_att->attrs[attrno - 1];
            bool generatedCol = ISGENERATEDCOL(target_relation->rd_att, attrno - 1);
            bool applyDefault = IsA(col, SetToDefault);

            if (!applyDefault && generatedCol)
                ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("cannot insert into column \"%s\"", NameStr(att_tup->attname)),
                    errdetail("Column \"%s\" is a generated column.", NameStr(att_tup->attname))));
        }
    }
}

/*
 * When processing INSERT ... VALUES with a VALUES RTE (ie, multiple VALUES
 * lists), we have to replace any DEFAULT items in the VALUES lists with
 * the appropriate default expressions.  The other aspects of targetlist
 * rewriting need be applied only to the query's targetlist proper.
 *
 * For an auto-updatable view, each DEFAULT item in the VALUES list is
 * replaced with the default from the view, if it has one.  Otherwise it is
 * left untouched so that the underlying base relation's default can be
 * applied instead (when we later recurse to here after rewriting the query
 * to refer to the base relation instead of the view).
 *
 * For other types of relation, including rule- and trigger-updatable views,
 * all DEFAULT items are replaced, and if the target relation doesn't have a
 * default, the value is explicitly set to NULL.
 *
 * Additionally, if force_nulls is true, the target relation's defaults are
 * ignored and all DEFAULT items in the VALUES list are explicitly set to
 * NULL, regardless of the target relation's type.  This is used for the
 * product queries generated by DO ALSO rules attached to an auto-updatable
 * view, for which we will have already called this function with force_nulls
 * false.  For these product queries, we must then force any remaining DEFAULT
 * items to NULL to provide concrete values for the rule actions.
 * Essentially, this is a mix of the 2 cases above --- the original query is
 * an insert into an auto-updatable view, and the product queries are inserts
 * into a rule-updatable view.
 *
 * Note that we may have subscripted or field assignment targetlist entries,
 * as well as more complex expressions from already-replaced DEFAULT items if
 * we have recursed to here for an auto-updatable view. However, it ought to
 * be impossible for such entries to have DEFAULTs assigned to them --- we
 * should only have to replace DEFAULT items for targetlist entries that
 * contain simple Vars referencing the VALUES RTE.
 *
 * Returns true if all DEFAULT items were replaced, and false if some were
 * left untouched.
 */
static bool rewriteValuesRTE(Query* parsetree, RangeTblEntry* rte, Relation target_relation, List* attrnos,
    bool force_nulls)
{
    List* newValues = NIL;
    ListCell* lc = NULL;
    bool isAutoUpdatableView;
    bool allReplaced;

    /* Steps below are not sensible for non-INSERT queries */
    Assert(parsetree->commandType == CMD_INSERT);
    Assert(rte->rtekind == RTE_VALUES);

    /*
     * Rebuilding all the lists is a pretty expensive proposition in a big
     * VALUES list, and it's a waste of time if there aren't any DEFAULT
     * placeholders.  So first scan to see if there are any.
     * We skip this check if force_nulls is true, because we know that there
     * are DEFAULT items present in that case.
     */
    if (!force_nulls && !searchForDefault(rte))
        return true; /* nothing to do */

    /*
     * Check if the target relation is an auto-updatable view, in which case
     * unresolved defaults will be left untouched rather than being set to
     * NULL. If force_nulls is true, we always set DEFAULT items to NULL, so
     * skip this check in that case --- it isn't an auto-updatable view.
     */
    isAutoUpdatableView = false;
    if (!force_nulls && target_relation->rd_rel->relkind == RELKIND_VIEW &&
        !view_has_instead_trigger(target_relation, CMD_INSERT)) {
        List* locks = NIL;
        bool found;
        ListCell* l = NULL;

        /* Look for an unconditional DO INSTEAD rule */
        locks = matchLocks(CMD_INSERT, target_relation->rd_rules, linitial_int(parsetree->resultRelations), parsetree);

        found = false;
        foreach (l, locks) {
            RewriteRule* rule_lock = (RewriteRule*)lfirst(l);

            if (rule_lock->isInstead && rule_lock->qual == NULL) {
                found = true;
                break;
            }
        }

        /*
         * If we didn't find an unconditional DO INSTEAD rule, assume that the
         * view is auto-updatable.  If it isn't, rewriteTargetView() will
         * throw an error.
         */
        if (!found)
            isAutoUpdatableView = true;
    }

    newValues = NIL;
    allReplaced = true;
    foreach (lc, rte->values_lists) {
        List* sublist = (List*)lfirst(lc);
        List* newList = NIL;
        ListCell* lc2 = NULL;
        ListCell* lc3 = NULL;
        int i = 0;

        forboth(lc2, sublist, lc3, attrnos)
        {
            Node *col = (Node *)lfirst(lc2);
            int attrno = lfirst_int(lc3);
            Form_pg_attribute att_tup = &target_relation->rd_att->attrs[attrno - 1];
            bool generatedCol = ISGENERATEDCOL(target_relation->rd_att, attrno - 1);
            bool applyDefault = IsA(col, SetToDefault);

            if (applyDefault) {
                Node *new_expr = NULL;

                if (attrno == 0) {
                    ereport(ERROR, (errmsg("cannot set value in column %d to DEFAULT", ++i)));
                }

                /* stored generated column will be computed in executor */
                if (force_nulls || att_tup->attisdropped || generatedCol)
                    new_expr = NULL;
                else
                    new_expr = build_column_default(target_relation, attrno, true);

                /*
                 * If there is no default (ie, default is effectively NULL),
                 * we've got to explicitly set the column to NULL, unless the
                 * target relation is an auto-updatable view.
                 */
                if (new_expr == NULL) {
                    if (isAutoUpdatableView) {
                        /* Leave the value untouched */
                        newList = lappend(newList, col);
                        allReplaced = false;
                        continue;
                    }

                    new_expr = (Node*)makeConst(att_tup->atttypid,
                        -1,
                        att_tup->attcollation,
                        att_tup->attlen,
                        (Datum)0,
                        true, /* isnull */
                        att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(
                        new_expr, InvalidOid, -1, att_tup->atttypid, COERCE_IMPLICIT_CAST, -1, false, false);
                }
                newList = lappend(newList, new_expr);
            } else {
                newList = lappend(newList, col);
            }
        }
        newValues = lappend(newValues, newList);
    }
    rte->values_lists = newValues;

    return allReplaced;
}

#ifdef PGXC
/*
 * pull_qual_vars(Node *node, int varno)
 * Extract vars from quals belonging to resultRelation. This function is mainly
 * taken from pull_qual_vars_clause(), but since the later does not peek into
 * subquery, we need to write this walker.
 *
 * @param (in) varno:
 *     the varno of result relation
 *     pull_qual_vars will returen all vars in current level if do NOT set 'varno' param
 *
 * @return:
 *     (1) vars from quals belonging to resultRelation
 *     (2) all vars from quals if 'varno' is not set
 */
List* pull_qual_vars(Node* node, int varno, int flags, bool nonRepeat)

{
    pull_qual_vars_context context;
    context.varlist = NIL;
    context.sublevels_up = 0;
    context.resultRelation = varno;
    context.noRepeat = nonRepeat;

    (void)query_or_expression_tree_walker(node, (bool (*)())pull_qual_vars_walker, (void*)&context, flags);
    return context.varlist;
}

static bool pull_qual_vars_walker(Node* node, pull_qual_vars_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /*
         * Add only if this var belongs to the resultRelation and refers to the table
         * from the same query.
         * BUT==> if context->resultRelation is not set,
         *        all var(s) refers to the table from the same query will be included
         */
        if ((context->resultRelation == 0 || var->varno == (Index)context->resultRelation) &&
            var->varlevelsup == (Index)context->sublevels_up) {
            if (context->noRepeat && list_member(context->varlist, var))
                return false;

            Var* newvar = (Var*)copyObject(var);
            newvar->varlevelsup = 0;
            context->varlist = lappend(context->varlist, newvar);
        }
        return false;
    }
    if (IsA(node, Query)) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())pull_qual_vars_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())pull_qual_vars_walker, (void*)context);
}

#endif /* PGXC */

/*
 * rewriteTargetListUD - rewrite UPDATE/DELETE targetlist as needed
 *
 * This function adds a "junk" TLE that is needed to allow the executor to
 * find the original row for the update or delete.	When the target relation
 * is a regular table, the junk TLE emits the ctid attribute of the original
 * row.  When the target relation is a view, there is no ctid, so we instead
 * emit a whole-row Var that will contain the "old" values of the view row.
 * If it's a foreign table, we let the FDW decide what to add.
 *
 * For UPDATE queries, this is applied after rewriteTargetListIU.  The
 * ordering isn't actually critical at the moment.
 */
static void rewriteTargetListUD(Query* parsetree, RangeTblEntry* target_rte, Relation target_relation, int rtindex)
{
    Var* var = NULL;
    const char* attrname = NULL;
    TargetEntry* tle = NULL;

#ifdef PGXC
    List* var_list = NIL;
    ListCell* elt = NULL;

    /*
     * In openGauss, we need to evaluate quals of the parse tree and determine
     * if they are Coordinator quals. If they are, their attribute need to be
     * added to target list for evaluation. In case some are found, add them as
     * junks in the target list. The junk status will be used by remote UPDATE
     * planning to associate correct element to a clause.
     * For DELETE, having such columns in target list helps to evaluate Quals
     * correctly on Coordinator.
     * This list could be reduced to keep only in target list the
     * vars using Coordinator Quals.
     */
    if (IS_PGXC_COORDINATOR && parsetree->jointree)
        var_list = pull_qual_vars((Node*)parsetree->jointree, rtindex);

    foreach (elt, var_list) {
        Form_pg_attribute att_tup;
        int numattrs = RelationGetNumberOfAttributes(target_relation);

        var = (Var*)lfirst(elt);
        /* Bypass in case of extra target items like ctid */
        if (var->varattno < 1 || var->varattno > numattrs) {
            if (var->varattno < 1) {
                RangeTblEntry* rte = rt_fetch(var->varno, parsetree->rtable);
                if (LOCATOR_TYPE_REPLICATED == GetLocatorType(rte->relid)) {
                    t_thrd.postmaster_cxt.forceNoSeparate = true;
                }

                parsetree->equalVars = lappend(parsetree->equalVars, copyObject(var));
            }
            continue;
        }

        att_tup = &target_relation->rd_att->attrs[var->varattno - 1];
        tle = makeTargetEntry(
            (Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(NameStr(att_tup->attname)), true);
        tle->rtindex = rtindex;

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }
    parsetree->equalVars = list_concat(
        parsetree->equalVars, pull_qual_vars((Node*)parsetree->targetList, rtindex, 0, true));

    if (IS_PGXC_COORDINATOR && RelationGetLocInfo(target_relation) &&
        IsRelationReplicated(RelationGetLocInfo(target_relation)) && RelationIsRelation(target_relation)) {
        int16* indexed_col = NULL;
        int index_col_count = 0;
        int counter = 0;

        index_col_count = pgxc_find_primarykey(target_relation->rd_id, &indexed_col);

        AssertEreport(index_col_count >= 0, MOD_OPT, "");

        for (counter = 0; counter < index_col_count; counter++) {
            AttrNumber att_no = indexed_col[counter];
            HeapTuple heaptuple = NULL;
            Form_pg_attribute att_tup = NULL;
            Node* new_expr = NULL;
            TargetEntry* new_tle = NULL;

            if (att_no > 0) {
                att_tup = &target_relation->rd_att->attrs[att_no - 1];
            } else {
                heaptuple =
                    SearchSysCache2(ATTNUM, ObjectIdGetDatum(RelationGetRelid(target_relation)), Int16GetDatum(att_no));
                if (!HeapTupleIsValid(heaptuple))
                    ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for attribute %d of relation %u", att_no,
                        RelationGetRelid(target_relation))));
                att_tup = (Form_pg_attribute)GETSTRUCT(heaptuple);
            }

            new_expr = (Node*)makeVar(
                rtindex, att_no, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
            new_tle = makeTargetEntry((Expr*)new_expr, list_length(parsetree->targetList) + 1, "xc_primary_key", true);
            new_tle->rtindex = rtindex;

            parsetree->targetList = lappend(parsetree->targetList, new_tle);

            if (att_no <= 0) {
                ReleaseSysCache(heaptuple);
            }
        }
    }
#endif

    if (target_relation->rd_rel->relkind == RELKIND_RELATION || target_relation->rd_rel->relkind == RELKIND_MATVIEW) {
        /*
         * Emit CTID so that executor can find the row to update or delete.
         */
        var = makeVar(rtindex, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);

        attrname = "ctid";
    } else if (target_relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE 
               || target_relation->rd_rel->relkind == RELKIND_STREAM) {
        /*
         * Let the foreign table's FDW add whatever junk TLEs it wants.
         */
        FdwRoutine* fdwroutine = NULL;

        fdwroutine = GetFdwRoutineForRelation(target_relation, false);

        if (fdwroutine->AddForeignUpdateTargets != NULL)
            fdwroutine->AddForeignUpdateTargets(parsetree, target_rte, target_relation);

        return;
    } else {
        /*
         * Emit whole-row Var so that executor will have the "old" view row to
         * pass to the INSTEAD OF trigger.
         */
        var = makeWholeRowVar(target_rte, rtindex, 0, false);
        if (var == NULL) {
            ereport(ERROR,(errcode(ERRCODE_UNDEFINED_FILE),
                    errmsg("Fail to get  the previous view row.")));
            return;
        }
        attrname = "wholerow";
    }

    tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);
    tle->rtindex = rtindex;

    parsetree->targetList = lappend(parsetree->targetList, tle);

    /* if a partitioned table , need get tableOid column */
    if (target_relation->rd_rel->relkind == RELKIND_RELATION &&
        (RELATION_IS_PARTITIONED(target_relation) || RelationIsCUFormat(target_relation))) {

        var = makeVar(rtindex, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
        attrname = "tableoid";

        tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);
        tle->rtindex = rtindex;

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }

    if (target_relation->rd_rel->relkind == RELKIND_RELATION && RELATION_HAS_BUCKET(target_relation)) {
        var = makeVar(rtindex, BucketIdAttributeNumber, INT2OID, -1, InvalidBktId, 0);
        attrname = "tablebucketid";
        tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);
        tle->rtindex = rtindex;

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }
#ifdef PGXC
    /* Add further attributes required for Coordinator */
    if (IS_PGXC_COORDINATOR && RelationGetLocInfo(target_relation) != NULL &&
        target_relation->rd_rel->relkind == RELKIND_RELATION) {
        /*
         * If relation is non-replicated, we need also to identify the Datanode
         * from where tuple is fetched.
         */
        if (!IsRelationReplicated(RelationGetLocInfo(target_relation))) {
            var = makeVar(rtindex, XC_NodeIdAttributeNumber, INT4OID, -1, InvalidOid, 0);

            tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup("xc_node_id"), true);
            tle->rtindex = rtindex;

            parsetree->targetList = lappend(parsetree->targetList, tle);
        }

        /* For non-shippable triggers, we need OLD row. */
        if (pgxc_trig_oldrow_reqd(target_relation, parsetree->commandType)) {
            var = makeWholeRowVar(target_rte, rtindex, 0, false);

            tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup("wholerow"), true);
            tle->rtindex = rtindex;

            parsetree->targetList = lappend(parsetree->targetList, tle);
        }
    }
#endif
}

static void rewriteTargetListMutilUD(Query* parsetree, List* rtable, List* resultRelations)
{
    ListCell* lc;
    int result_relation;
    RangeTblEntry* rt_entry = NULL;
    Relation rt_entry_relation;

    foreach (lc, resultRelations) {
        result_relation = lfirst_int(lc);
        rt_entry = rt_fetch(result_relation, rtable);
        AssertEreport(rt_entry->rtekind == RTE_RELATION, MOD_OPT, "");
        rt_entry_relation = heap_open(rt_entry->relid, NoLock);
        rewriteTargetListUD(parsetree, rt_entry, rt_entry_relation, result_relation);
        heap_close(rt_entry_relation, NoLock);
    }
}

void rewriteTargetListMerge(Query* parsetree, Index result_relation, List* range_table)
{
    Var* var = NULL;
    const char* attrname = NULL;
    TargetEntry* tle = NULL;
    Relation rel;

    /*
     * The rewriter should have already ensured that the TLEs are in correct
     * order; but we have to insert TLEs for any missing attributes.
     *
     * Scan the tuple description in the relation's relcache entry to make
     * sure we have all the user attributes in the right order.  We assume
     * that the rewriter already acquired at least AccessShareLock on the
     * relation, so we need no lock here.
     */
    rel = heap_open(getrelid(result_relation, range_table), NoLock);

    Assert(rel->rd_rel->relkind == RELKIND_RELATION);

    parsetree->targetList = expandTargetTL(parsetree->targetList, parsetree);

    /*
     * Emit CTID so that executor can find the row to update or delete.
     */
    var = makeVar(parsetree->mergeTarget_relation, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);

    attrname = "ctid";
    tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);

    parsetree->targetList = lappend(parsetree->targetList, tle);

    /* We need add xc_node_id for MPP cluster mode, but only ctid for single datanode */
    if (IS_PGXC_COORDINATOR) {
        /*
         * Emit xc_node_id so that executor can find the row to update or delete.
         */
        var = makeVar(parsetree->mergeTarget_relation, XC_NodeIdAttributeNumber, INT4OID, -1, InvalidOid, 0);

        attrname = "xc_node_id";
        tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }

    /*
     * If we are dealing with partitioned table, then emit TABLEOID so that
     * executor can find the partition the row belongs to.
     */
    if (RELATION_IS_PARTITIONED(rel) || RelationIsCUFormat(rel)) {
        var = makeVar(parsetree->mergeTarget_relation, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);

        attrname = "tableoid";
        tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);
        parsetree->targetList = lappend(parsetree->targetList, tle);
    }
    if (RELATION_HAS_BUCKET(rel)) {
        var = makeVar(parsetree->mergeTarget_relation, BucketIdAttributeNumber, INT2OID, -1, InvalidBktId, 0);
        attrname = "tablebucketid";
        tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }

    heap_close(rel, NoLock);
}

/*
 * matchLocks -
 *	  match the list of locks and returns the matching rules
 */
static List* matchLocks(CmdType event, RuleLock* rulelocks, int varno, Query* parsetree)
{
    List* matching_locks = NIL;
    int nlocks;
    int i;

    if (rulelocks == NULL)
        return NIL;

    if (parsetree->commandType != CMD_SELECT) {
        if (linitial2_int(parsetree->resultRelations) != varno)
            return NIL;
    }

    nlocks = rulelocks->numLocks;

    for (i = 0; i < nlocks; i++) {
        RewriteRule* oneLock = rulelocks->rules[i];

        /*
         * Suppress ON INSERT/UPDATE/DELETE rules that are disabled or
         * configured to not fire during the current sessions replication
         * role. ON SELECT rules will always be applied in order to keep views
         * working even in LOCAL or REPLICA role.
         */
        if (oneLock->event != CMD_SELECT) {
            if (u_sess->attr.attr_common.SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA) {
                if (oneLock->enabled == RULE_FIRES_ON_ORIGIN || oneLock->enabled == RULE_DISABLED)
                    continue;
            } else { /* ORIGIN or LOCAL ROLE */
                if (oneLock->enabled == RULE_FIRES_ON_REPLICA || oneLock->enabled == RULE_DISABLED)
                    continue;
            }
        }

        if (oneLock->event == event) {
            if (parsetree->commandType != CMD_SELECT ||
                (oneLock->attrno == -1 ? rangeTableEntry_used((Node*)parsetree, varno, 0)
                                       : attribute_used((Node*)parsetree, varno, oneLock->attrno, 0)))
                matching_locks = lappend(matching_locks, oneLock);
        }
    }

    return matching_locks;
}

/*
 * ApplyRetrieveRule - expand an ON SELECT rule
 */
static Query* ApplyRetrieveRule(Query* parsetree, RewriteRule* rule, int rt_index, bool relation_level,
    Relation relation, List* activeRIRs, bool forUpdatePushedDown)
{
    Query* rule_action = NULL;
    RangeTblEntry* rte = NULL;
    RangeTblEntry* subrte = NULL;
    RowMarkClause* rc = NULL;
    bool is_flt_frame = parsetree->is_flt_frame;
    /* b_format view sql security option use */
    Oid checkAsUser = InvalidOid;

    if (list_length(rule->actions) != 1) {
        ereport(ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("expected just one rule action")));
    }

    if (rule->qual != NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("cannot handle qualified ON SELECT rule")));
    }

    if (!relation_level) {
        ereport(ERROR,
            (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("cannot handle per-attribute ON SELECT rule")));
    }

    if (rt_index == linitial2_int(parsetree->resultRelations)) {
        /*
         * We have a view as the result relation of the query, and it wasn't
         * rewritten by any rule.  This case is supported if there is an
         * INSTEAD OF trigger that will trap attempts to insert/update/delete
         * view rows.  The executor will check that; for the moment just plow
         * ahead.  We have two cases:
         *
         * For INSERT, we needn't do anything.  The unmodified RTE will serve
         * fine as the result relation.
         *
         * For UPDATE/DELETE, we need to expand the view so as to have source
         * data for the operation.	But we also need an unmodified RTE to
         * serve as the target.  So, copy the RTE and add the copy to the
         * rangetable.	Note that the copy does not get added to the jointree.
         * Also note that there's a hack in fireRIRrules to avoid calling this
         * function again when it arrives at the copied RTE.
         */
        if (parsetree->commandType == CMD_INSERT)
            return parsetree;
        else if (parsetree->commandType == CMD_UPDATE || parsetree->commandType == CMD_DELETE) {
            RangeTblEntry* newrte = NULL;

            rte = rt_fetch(rt_index, parsetree->rtable);
            newrte = (RangeTblEntry*)copyObject(rte);
            parsetree->rtable = (List*)lappend(parsetree->rtable, newrte);
            linitial_int(parsetree->resultRelations) = list_length(parsetree->rtable);
            parsetree->resultRelation = linitial_int(parsetree->resultRelations);
            /*
             * There's no need to do permissions checks twice, so wipe out the
             * permissions info for the original RTE (we prefer to keep the
             * bits set on the result RTE).
             */
            rte->requiredPerms = 0;
            rte->checkAsUser = InvalidOid;
            rte->selectedCols = NULL;
            rte->insertedCols = NULL;
            rte->updatedCols = NULL;
            rte->extraUpdatedCols = NULL;

            /*
             * For the most part, Vars referencing the view should remain as
             * they are, meaning that they implicitly represent OLD values.
             * But in the RETURNING list if any, we want such Vars to
             * represent NEW values, so change them to reference the new RTE.
             *
             * Since ChangeVarNodes scribbles on the tree in-place, copy the
             * RETURNING list first for safety.
             */
            parsetree->returningList = (List*)copyObject(parsetree->returningList);
            ChangeVarNodes((Node*)parsetree->returningList, rt_index, linitial2_int(parsetree->resultRelations), 0);

            /* rtindex and vars in withCheckOptions also need to change */
            parsetree->withCheckOptions = (List*)copyObject(parsetree->withCheckOptions);
            ChangeVarNodes((Node*)parsetree->withCheckOptions, rt_index, linitial2_int(parsetree->resultRelations), 0);

            /* Now, continue with expanding the original view RTE */
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("unrecognized commandType: %d", (int)parsetree->commandType)));
        }
    }

    /*
     * If FOR [KEY] UPDATE/SHARE of view, be sure we get right initial lock on the
     * relations it references.
     */
    rc = get_parse_rowmark(parsetree, rt_index);
    forUpdatePushedDown = forUpdatePushedDown || (rc != NULL);

    /*
     * Make a modifiable copy of the view query, and acquire needed locks on
     * the relations it mentions.
     */
    rule_action = (Query*)copyObject(linitial(rule->actions));

    AcquireRewriteLocks(rule_action, forUpdatePushedDown);

    /*
     * If FOR [KEY] UPDATE/SHARE of view, mark all the contained tables as implicit
     * FOR [KEY] UPDATE/SHARE, the same as the parser would have done if the view's
     * subquery had been written out explicitly.
     *
     * Note: we don't consider forUpdatePushedDown here; such marks will be
     * made by recursing from the upper level in markQueryForLocking.
     */
    if (rc != NULL)
        markQueryForLocking(rule_action, (Node*)rule_action->jointree, rc->strength, rc->waitPolicy, true,
                            rc->waitSec);

    /* Pass the is_flt_frame from parsetree to rule_action */
    rule_action->is_flt_frame = is_flt_frame;

    /*
     * in B format database ,deal with security options 
     * cause checkAsUser shoule be seted as definers' oid 
     * we should do some check here ,after expand views' query definition
     */

    /* get from here, before Recursive call this function, transform outside view first */
    rte = rt_fetch(rt_index, parsetree->rtable);
    
    if (DB_IS_CMPT(B_FORMAT) && rte->relkind == RELKIND_VIEW) {
        if (RelationHasViewSecurityDefinerOption(relation)) {
            checkAsUser = RelationGetOwner(relation);
        } else if (RelationHasViewSecurityInvokerOption(relation)) {
            /* for invoker ,if checkAsUser is seted as owner id, we shoule use it */
            checkAsUser = rte->checkAsUser == InvalidOid ? GetUserId() : rte->checkAsUser;
        } else {
            /* default is definer in b format database */
            checkAsUser = RelationGetOwner(relation);
        }
        if (checkAsUser != RelationGetOwner(relation)) {
            /* set all relations' and functions' invoker information */
            query_tree_walker((Query *)rule_action, (bool (*)())viewSecurityPassDown, (void *)&checkAsUser, QTW_EXAMINE_RTES);
        }
    } else if (RelationHasViewSecurityOption(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SQL Security option should only used in view and B format database")));
    }

    /*
     * Recursively expand any view references inside the view.
     *
     * Note: this must happen after markQueryForLocking.  That way, any UPDATE
     * permission bits needed for sub-views are initially applied to their
     * RTE_RELATION RTEs by markQueryForLocking, and then transferred to their
     * OLD rangetable entries by the action below (in a recursive call of this
     * routine).
     */
    rule_action = fireRIRrules(rule_action, activeRIRs, forUpdatePushedDown);

    /*
     * Refresh view SRFs
     *
     * If we create a view when enable_expr_fusion is off, SRFs will not be identified, everything will be okay.
     * If user set the guc on later, we get an old rule_action, but actually we have the ability to identify
     * SRFs and resolve them.  We travel the tule_action to fix hasTargetSRFs in Query.
     *
     * More detail:
     *  rule_action
     *      hasTargetSRFs true
     *          parsetree->is_flt_frame true, it's ok
     *          parsetree->is_flt_frame false, modify hasTargetSRFs to false
     *      hasTargetSRFs false
     *          parsetree->is_flt_frame, walk and check if it has SRFs
     *          parsetree->is_flt_frame, it's ok
     */
    if (u_sess->attr.attr_common.enable_expr_fusion && u_sess->attr.attr_sql.query_dop_tmp == 1) {
        /* adjust the Query */
        query_check_srf(rule_action);
    }

    /*
     * Now, plug the view query in as a subselect, replacing the relation's
     * original RTE.
     */
    rte = rt_fetch(rt_index, parsetree->rtable);

    rte->rtekind = RTE_SUBQUERY;
    rte->relid = InvalidOid;
    rte->security_barrier = RelationIsSecurityView(relation);
    rte->subquery = rule_action;
    rte->inh = false; /* must not be set for a subquery */

    /*
     * We move the view's permission check data down to its rangetable. The
     * checks will actually be done against the OLD entry therein.
     */
    subrte = rt_fetch(PRS2_OLD_VARNO, rule_action->rtable);
    AssertEreport(subrte->relid == relation->rd_id, MOD_OPT, "");
    subrte->requiredPerms = rte->requiredPerms;
    subrte->checkAsUser = rte->checkAsUser;
    subrte->selectedCols = rte->selectedCols;
    subrte->insertedCols = rte->insertedCols;
    subrte->updatedCols = rte->updatedCols;
    subrte->extraUpdatedCols = rte->extraUpdatedCols;

    rte->requiredPerms = 0; /* no permission check on subquery itself */
    rte->checkAsUser = InvalidOid;
    rte->selectedCols = NULL;
    rte->insertedCols = NULL;
    rte->updatedCols = NULL;
    rte->extraUpdatedCols = NULL;

    /*
     * If FOR [KEY] UPDATE/SHARE of view, mark all the contained tables as implicit
     * FOR [KEY] UPDATE/SHARE, the same as the parser would have done if the view's
     * subquery had been written out explicitly.
     *
     * Note: we don't consider forUpdatePushedDown here; such marks will be
     * made by recursing from the upper level in markQueryForLocking.
     */
    if (rc != NULL)
        markQueryForLocking(rule_action, (Node*)rule_action->jointree, rc->strength, rc->waitPolicy, true,
            rc->waitSec);

    return parsetree;
}

/*
 * Recursively mark all relations used by a view as FOR [KEY] UPDATE/SHARE.
 *
 * This may generate an invalid query, eg if some sub-query uses an
 * aggregate.  We leave it to the planner to detect that.
 *
 * NB: this must agree with the parser's transformLockingClause() routine.
 * However, unlike the parser we have to be careful not to mark a view's
 * OLD and NEW rels for updating.  The best way to handle that seems to be
 * to scan the jointree to determine which rels are used.
 */
static void markQueryForLocking(Query* qry, Node* jtnode, LockClauseStrength strength, LockWaitPolicy waitPolicy, bool pushedDown,
                                int waitSec)
{
    if (jtnode == NULL)
        return;
    if (IsA(jtnode, RangeTblRef)) {
        int rti = ((RangeTblRef*)jtnode)->rtindex;
        RangeTblEntry* rte = rt_fetch(rti, qry->rtable);

        if (rte->rtekind == RTE_RELATION) {
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown, waitSec);
            rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
        } else if (rte->rtekind == RTE_SUBQUERY) {
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown, waitSec);
            /* FOR UPDATE/SHARE of subquery is propagated to subquery's rels */
            markQueryForLocking(rte->subquery, (Node*)rte->subquery->jointree, strength, waitPolicy, true,
                                waitSec);
        }
        /* other RTE types are unaffected by FOR UPDATE */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist)
            markQueryForLocking(qry, (Node*)lfirst(l), strength, waitPolicy, pushedDown, waitSec);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        markQueryForLocking(qry, j->larg, strength, waitPolicy, pushedDown, waitSec);
        markQueryForLocking(qry, j->rarg, strength, waitPolicy, pushedDown, waitSec);
    } else
        ereport(ERROR,
            (errmodule(MOD_OPT_REWRITE),
                errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
}

/*
 * fireRIRonSubLink -
 *	Apply fireRIRrules() to each SubLink (subselect in expression) found
 *	in the given tree.
 *
 * NOTE: although this has the form of a walker, we cheat and modify the
 * SubLink nodes in-place.	It is caller's responsibility to ensure that
 * no unwanted side-effects occur!
 *
 * This is unlike most of the other routines that recurse into subselects,
 * because we must take control at the SubLink node in order to replace
 * the SubLink's subselect link with the possibly-rewritten subquery.
 */
static bool fireRIRonSubLink(Node* node, List* activeRIRs)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubLink)) {
        SubLink* sub = (SubLink*)node;

        /* Do what we came for */
        sub->subselect = (Node*)fireRIRrules((Query*)sub->subselect, activeRIRs, false);
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because fireRIRrules already processed
     * subselects of subselects for us.
     */
    return expression_tree_walker(node, (bool (*)())fireRIRonSubLink, (void*)activeRIRs);
}

/*
 * fireRIRrules -
 *	Apply all RIR rules on each rangetable entry in a query
 */
static Query* fireRIRrules(Query* parsetree, List* activeRIRs, bool forUpdatePushedDown)
{
    int origResultRelation = linitial2_int(parsetree->resultRelations);
    int rt_index;
    ListCell* lc = NULL;

    /*
     * don't try to convert this into a foreach loop, because rtable list can
     * get changed each time through...
     */
    rt_index = 0;
    while (rt_index < list_length(parsetree->rtable)) {
        RangeTblEntry* rte = NULL;
        Relation rel;
        List* locks = NIL;
        RuleLock* rules = NULL;
        RewriteRule* rule = NULL;
        int i;

        ++rt_index;

        rte = rt_fetch(rt_index, parsetree->rtable);

        /*
         * A subquery RTE can't have associated rules, so there's nothing to
         * do to this level of the query, but we must recurse into the
         * subquery to expand any rule references in it.
         */
        if (rte->rtekind == RTE_SUBQUERY) {
            rte->subquery = fireRIRrules(
                rte->subquery, activeRIRs, (forUpdatePushedDown || get_parse_rowmark(parsetree, rt_index) != NULL));
            continue;
        }

        /*
         * Joins and other non-relation RTEs can be ignored completely.
         */
        if (rte->rtekind != RTE_RELATION) {
            continue;
        }

        /*
         * Always ignore RIR rules for materialized views referenced in
         * queries.  (This does not prevent refreshing MVs, since they aren't
         * referenced in their own query definitions.)
         *
         * Note: in the future we might want to allow MVs to be conditionally
         * expanded as if they were regular views, if they are not scannable.
         * In that case this test would need to be postponed till after we've
         * opened the rel, so that we could check its state.
         */
        if (rte->relkind == RELKIND_MATVIEW) {
            continue;
        }

        /*
         * If the table is not referenced in the query, then we ignore it.
         * This prevents infinite expansion loop due to new rtable entries
         * inserted by expansion of a rule. A table is referenced if it is
         * part of the join set (a source table), or is referenced by any Var
         * nodes, or is the result table.
         */
        if (rt_index != linitial2_int(parsetree->resultRelations) &&
            !rangeTableEntry_used((Node*)parsetree, rt_index, 0)) {
            continue;
        }

        /*
         * Also, if this is a new result relation introduced by
         * ApplyRetrieveRule, we don't want to do anything more with it.
         */
        if (rt_index == linitial2_int(parsetree->resultRelations) && rt_index != origResultRelation) {
            continue;
        }

        /*
         * We can use NoLock here since either the parser or
         * AcquireRewriteLocks should have locked the rel already.
         */
        rel = heap_open(rte->relid, NoLock);

        /*
         * Collect the RIR rules that we must apply
         */
        rules = rel->rd_rules;
        if (rules == NULL) {
            heap_close(rel, NoLock);
            continue;
        }
        for (i = 0; i < rules->numLocks; i++) {
            rule = rules->rules[i];
            if (rule->event != CMD_SELECT) {
                continue;
            }

            if (rule->attrno > 0) {
                /* per-attr rule; do we need it? */
                if (!attribute_used((Node*)parsetree, rt_index, rule->attrno, 0))
                    continue;
            }

            locks = lappend(locks, rule);
        }

        /*
         * If we found any, apply them --- but first check for recursion!
         */
        if (locks != NIL) {
            ListCell* l = NULL;

            if (list_member_oid(activeRIRs, RelationGetRelid(rel))) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg(
                            "infinite recursion detected in rules for relation \"%s\"", RelationGetRelationName(rel))));
            }
            activeRIRs = lcons_oid(RelationGetRelid(rel), activeRIRs);

            foreach (l, locks) {
                rule = (RewriteRule*)lfirst(l);

                parsetree = ApplyRetrieveRule(
                    parsetree, rule, rt_index, rule->attrno == -1, rel, activeRIRs, forUpdatePushedDown);
            }

            activeRIRs = list_delete_first(activeRIRs);
        }

        heap_close(rel, NoLock);
    }

    /* Recurse into subqueries in WITH */
    foreach (lc, parsetree->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

        cte->ctequery = (Node*)fireRIRrules((Query*)cte->ctequery, activeRIRs, false);
    }

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (parsetree->hasSubLinks) {
        (void)query_tree_walker(parsetree, (bool (*)())fireRIRonSubLink, (void*)activeRIRs, QTW_IGNORE_RC_SUBQUERIES);
    }

    /*
     * Apply row level security policies.  Do this work here because it
     * requires special recursion detection if the new quals have sublink
     * subqueries, and if we did it in the loop above query_tree_walker would
     * then recurse into those quals a second time.
     * Only bind R.L.S policies to plan on coordinator node like view. Currently
     * R.L.S only suport SELECT, UPDATE, DELETE.
     */
    bool SupportRlsOnNode = true;
#ifdef ENABLE_MULTIPLE_NODES
    SupportRlsOnNode = IS_PGXC_COORDINATOR;
#endif
    if (SupportRlsOnNode &&
        ((parsetree->commandType == CMD_SELECT) || (parsetree->commandType == CMD_UPDATE) ||
         (parsetree->commandType == CMD_DELETE) || (parsetree->commandType == CMD_MERGE))) {
        rt_index = 0;
        foreach (lc, parsetree->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
            List* securityQuals = NIL;
            bool hasRowSecurity = false;
            bool hasSubLink = false;
            rt_index++;

            /* Row-Level-Security policy can only applied to normal
             * relation(include partitioned relation)
             */
            if ((rte->rtekind != RTE_RELATION) || (rte->relkind != RELKIND_RELATION)) {
                continue;
            }

            Relation targetTable = relation_open(rte->relid, NoLock);
            /* Fetch all R.L.S security quals that must be applied to this RTE */
            GetRlsPolicies(parsetree, rte, targetTable, &securityQuals, rt_index, hasRowSecurity, hasSubLink);
            if (securityQuals != NIL) {
                /*
                 * Add the new security barrier quals to the start of the RTE's
                 * list so that they get applied before any existing barrier quals
                 * (which would have come from a security-barrier view, and should
                 * get lower priority than RLS conditions on the table itself).
                 */
                if (hasSubLink) {
                    if (list_member_oid(activeRIRs, rte->relid)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("infinite recursion detected, please check the row level security "
                                       "policies for relation \"%s\"",
                                    RelationGetRelationName(targetTable))));
                    }
                    activeRIRs = lcons_oid(rte->relid, activeRIRs);
                    /*
                     * Get row level security policies just passed back securityQuals and
                     * there were SubLink, make sure we lock any relations which are referenced.
                     * These locks would normally be acquired by the parser, but securityQuals
                     * are added post-parsing.
                     */
                    (void)acquireLocksOnSubLinks((Node*)securityQuals, NULL);
                    expression_tree_walker((Node*)securityQuals, (bool (*)())fireRIRonSubLink, (void*)activeRIRs);
                    activeRIRs = list_delete_first(activeRIRs);
                    parsetree->hasSubLinks = true;
                }
                rte->securityQuals = list_concat(securityQuals, rte->securityQuals);
            }

            /* For case CMD_MERGE, add securityQuals to MergeAction */
            if (rt_index == linitial2_int(parsetree->resultRelations) && parsetree->commandType == CMD_MERGE &&
                rte->securityQuals != NIL) {
                ListCell *lc = NULL;
                ListCell *next = NULL;
                Expr *security_quals_expr = (Expr *)lfirst(list_head(rte->securityQuals));
                for (lc = lnext(list_head(rte->securityQuals)); lc != NULL; lc = next) {
                    next = lnext(lc);
                    List *lst = list_make2(security_quals_expr, lfirst(lc));
                    security_quals_expr = makeBoolExpr(AND_EXPR, lst, -1);
                }

                foreach (lc, parsetree->mergeActionList) {
                    MergeAction *action = (MergeAction *)lfirst(lc);
                    if (action->commandType == CMD_UPDATE) {
                        if (action->qual == NULL) {
                            action->qual = (Node *)security_quals_expr;
                        } else {
                            List *qual_list = list_make2(security_quals_expr, action->qual);
                            action->qual = (Node *)makeBoolExpr(AND_EXPR, qual_list, -1);
                        }
                        rte->securityQuals = NIL;
                        break;
                    }
                }
            }

            /* Update Query hasRowSecurity */
            parsetree->hasRowSecurity = (hasRowSecurity || parsetree->hasRowSecurity);
            relation_close(targetTable, NoLock);
        }
    }
    return parsetree;
}

/*
 * Modify the given query by adding 'AND rule_qual IS NOT TRUE' to its
 * qualification.  This is used to generate suitable "else clauses" for
 * conditional INSTEAD rules.  (Unfortunately we must use "x IS NOT TRUE",
 * not just "NOT x" which the planner is much smarter about, else we will
 * do the wrong thing when the qual evaluates to NULL.)
 *
 * The rule_qual may contain references to OLD or NEW.	OLD references are
 * replaced by references to the specified rt_index (the relation that the
 * rule applies to).  NEW references are only possible for INSERT and UPDATE
 * queries on the relation itself, and so they should be replaced by copies
 * of the related entries in the query's own targetlist.
 */
static Query* CopyAndAddInvertedQual(Query* parsetree, Node* rule_qual, int rt_index, CmdType event)
{
    /* Don't scribble on the passed qual (it's in the relcache!) */
    Node* new_qual = (Node*)copyObject(rule_qual);

    /*
     * In case there are subqueries in the qual, acquire necessary locks and
     * fix any deleted JOIN RTE entries.  (This is somewhat redundant with
     * rewriteRuleAction, but not entirely ... consider restructuring so that
     * we only need to process the qual this way once.)
     */
    (void)acquireLocksOnSubLinks(new_qual, NULL);

    /* Fix references to OLD */
    ChangeVarNodes(new_qual, PRS2_OLD_VARNO, rt_index, 0);
    /* Fix references to NEW */
    if (event == CMD_INSERT || event == CMD_UPDATE)
        new_qual = ReplaceVarsFromTargetList(new_qual,
            PRS2_NEW_VARNO,
            0,
            rt_fetch(rt_index, parsetree->rtable),
            parsetree->targetList,
            (event == CMD_UPDATE) ?
            REPLACEVARS_CHANGE_VARNO :
            REPLACEVARS_SUBSTITUTE_NULL,
            rt_index,
            &parsetree->hasSubLinks);
    /* And attach the fixed qual */
    AddInvertedQual(parsetree, new_qual);

    return parsetree;
}
/*
 * Generated column can not be manually insert or updated. 
 */
static void CheckGeneratedColConstraint(CmdType commandType, Form_pg_attribute attTup, const TargetEntry *newTle)
{
    if (commandType == CMD_INSERT) {
        ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("cannot insert into column \"%s\"", NameStr(attTup->attname)),
            errdetail("Column \"%s\" is a generated column.", NameStr(attTup->attname))));
    } else if (commandType == CMD_UPDATE && newTle) {
        ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("column \"%s\" can only be updated to DEFAULT", NameStr(attTup->attname)),
            errdetail("Column \"%s\" is a generated column.", NameStr(attTup->attname))));
    }
}

/*
 * very same to pg's rewriteTargetListIU. we adapt it to use for MergeInto
 */
static List* rewriteTargetListMergeInto(
    List* targetList, CmdType commandType, Relation target_relation, int result_rti, List** attrno_list)
{
    TargetEntry** new_tles;
    List* new_tlist = NIL;
    List* junk_tlist = NIL;
    Form_pg_attribute att_tup;
    int attrno, next_junk_attrno, numattrs;
    ListCell* temp = NULL;

    if (attrno_list != NULL) /* initialize optional result list */
        *attrno_list = NIL;

    /*
     * We process the normal (non-junk) attributes by scanning the input tlist
     * once and transferring TLEs into an array, then scanning the array to
     * build an output tlist.  This avoids O(N^2) behavior for large numbers
     * of attributes.
     *
     * Junk attributes are tossed into a separate list during the same tlist
     * scan, then appended to the reconstructed tlist.
     */
    numattrs = RelationGetNumberOfAttributes(target_relation);
    new_tles = (TargetEntry**)palloc0(numattrs * sizeof(TargetEntry*));
    next_junk_attrno = numattrs + 1;

    foreach (temp, targetList) {
        TargetEntry* old_tle = (TargetEntry*)lfirst(temp);

        if (!old_tle->resjunk) {
            /* Normal attr: stash it into new_tles[] */
            attrno = old_tle->resno;
            if (attrno < 1 || attrno > numattrs) {
                ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_COLUMN), errmsg("bogus resno %d in targetlist", attrno)));
            }

            att_tup = &target_relation->rd_att->attrs[attrno - 1];

            /* put attrno into attrno_list even if it's dropped */
            if (attrno_list != NULL)
                *attrno_list = lappend_int(*attrno_list, attrno);

            /* We can (and must) ignore deleted attributes */
            if (att_tup->attisdropped)
                continue;

            /* Merge with any prior assignment to same attribute */
            new_tles[attrno - 1] = process_matched_tle(old_tle, new_tles[attrno - 1], NameStr(att_tup->attname));
        } else {
            /*
             * Copy all resjunk tlist entries to junk_tlist, and assign them
             * resnos above the last real resno.
             *
             * Typical junk entries include ORDER BY or GROUP BY expressions
             * (are these actually possible in an INSERT or UPDATE?), system
             * attribute references, etc.
             */
            /* Get the resno right, but don't copy unnecessarily */
            if (old_tle->resno != next_junk_attrno) {
                old_tle = flatCopyTargetEntry(old_tle);
                old_tle->resno = next_junk_attrno;
            }
            junk_tlist = lappend(junk_tlist, old_tle);
            next_junk_attrno++;
        }
    }

    for (attrno = 1; attrno <= numattrs; attrno++) {
        TargetEntry* new_tle = new_tles[attrno - 1];
        bool apply_default = false;

        att_tup = &target_relation->rd_att->attrs[attrno - 1];

        /* We can (and must) ignore deleted attributes */
        if (att_tup->attisdropped)
            continue;

        /*
         * Handle the two cases where we need to insert a default expression:
         * it's an INSERT and there's no tlist entry for the column, or the
         * tlist entry is a DEFAULT placeholder node.
         */
        apply_default = ((new_tle == NULL && commandType == CMD_INSERT) ||
                         (new_tle && new_tle->expr && IsA(new_tle->expr, SetToDefault)));
        
        bool isGeneratedCol = ISGENERATEDCOL(target_relation->rd_att, attrno - 1);


        if (isGeneratedCol) {
            if (!apply_default) {
                CheckGeneratedColConstraint(commandType, att_tup, new_tle);
            }
            /*
             * stored generated column will be fixed in executor
             */
            new_tle = NULL;
        } else if (apply_default) {
            Node* new_expr = NULL;

            new_expr = build_column_default(target_relation, attrno, (commandType == CMD_INSERT));

            /*
             * If there is no default (ie, default is effectively NULL), we
             * can omit the tlist entry in the INSERT case, since the planner
             * can insert a NULL for itself, and there's no point in spending
             * any more rewriter cycles on the entry.  But in the UPDATE case
             * we've got to explicitly set the column to NULL.
             */
            if (new_expr == NULL) {
                if (commandType == CMD_INSERT)
                    new_tle = NULL;
                else {
                    new_expr = (Node*)makeConst(att_tup->atttypid,
                        -1,
                        att_tup->attcollation,
                        att_tup->attlen,
                        (Datum)0,
                        true, /* isnull */
                        att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(
                        new_expr, InvalidOid, -1, att_tup->atttypid, COERCE_IMPLICIT_CAST, -1, false, false);
                }
            }

            if (new_expr != NULL)
                new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
        }

        if (new_tle != NULL)
            new_tlist = lappend(new_tlist, new_tle);
    }

    pfree_ext(new_tles);

    return list_concat(new_tlist, junk_tlist);
}

/*
 *	fireRules -
 *	   Iterate through rule locks applying rules.
 *
 * Input arguments:
 *	parsetree - original query
 *	rt_index - RT index of result relation in original query
 *	event - type of rule event
 *	locks - list of rules to fire
 * Output arguments:
 *	*instead_flag - set TRUE if any unqualified INSTEAD rule is found
 *					(must be initialized to FALSE)
 *	*returning_flag - set TRUE if we rewrite RETURNING clause in any rule
 *					(must be initialized to FALSE)
 *	*qual_product - filled with modified original query if any qualified
 *					INSTEAD rule is found (must be initialized to NULL)
 * Return value:
 *	list of rule actions adjusted for use with this query
 *
 * Qualified INSTEAD rules generate their action with the qualification
 * condition added.  They also generate a modified version of the original
 * query with the negated qualification added, so that it will run only for
 * rows that the qualified action doesn't act on.  (If there are multiple
 * qualified INSTEAD rules, we AND all the negated quals onto a single
 * modified original query.)  We won't execute the original, unmodified
 * query if we find either qualified or unqualified INSTEAD rules.	If
 * we find both, the modified original query is discarded too.
 */
static List* fireRules(Query* parsetree, int rt_index, CmdType event, List* locks, bool* instead_flag,
    bool* returning_flag, Query** qual_product)
{
    List* results = NIL;
    ListCell* l = NULL;

    foreach (l, locks) {
        RewriteRule* rule_lock = (RewriteRule*)lfirst(l);
        Node* event_qual = rule_lock->qual;
        List* actions = rule_lock->actions;
        QuerySource qsrc;
        ListCell* r = NULL;

        /* Determine correct QuerySource value for actions */
        if (rule_lock->isInstead) {
            if (event_qual != NULL)
                qsrc = QSRC_QUAL_INSTEAD_RULE;
            else {
                qsrc = QSRC_INSTEAD_RULE;
                *instead_flag = true; /* report unqualified INSTEAD */
            }
        } else
            qsrc = QSRC_NON_INSTEAD_RULE;

        if (qsrc == QSRC_QUAL_INSTEAD_RULE) {
            /*
             * If there are INSTEAD rules with qualifications, the original
             * query is still performed. But all the negated rule
             * qualifications of the INSTEAD rules are added so it does its
             * actions only in cases where the rule quals of all INSTEAD rules
             * are false. Think of it as the default action in a case. We save
             * this in *qual_product so RewriteQuery() can add it to the query
             * list after we mangled it up enough.
             *
             * If we have already found an unqualified INSTEAD rule, then
             * *qual_product won't be used, so don't bother building it.
             */
            if (!*instead_flag) {
                if (*qual_product == NULL)
                    *qual_product = (Query*)copyObject(parsetree);
                *qual_product = CopyAndAddInvertedQual(*qual_product, event_qual, rt_index, event);
            }
        }

        /* Now process the rule's actions and add them to the result list */
        foreach (r, actions) {
            Query* rule_action = (Query*)lfirst(r);

            if (rule_action->commandType == CMD_NOTHING)
                continue;

            rule_action = rewriteRuleAction(parsetree, rule_action, event_qual, rt_index, event, returning_flag);
            rule_action->querySource = qsrc;
            rule_action->canSetTag = false; /* might change later */

            results = lappend(results, rule_action);
        }
    }

    return results;
}

/*
 * get_view_query - get the Query from a view's _RETURN rule.
 *
 * Caller should have verified that the relation is a view, and therefore
 * we should find an ON SELECT action.
 *
 * Note that the pointer returned is into the relcache and therefore must
 * be treated as read-only to the caller and not modified or scribbled on.
 */
Query* get_view_query(Relation view)
{
    int i;

    Assert(view->rd_rel->relkind == RELKIND_VIEW);

    for (i = 0; i < view->rd_rules->numLocks; i++) {
        RewriteRule *rule = view->rd_rules->rules[i];

        if (rule->event == CMD_SELECT) {
            /* A _RETURN rule should have only one action */
            if (list_length(rule->actions) != 1)
                ereport(ERROR, (errmsg("invalid _RETURN rule action specification")));

            return (Query*)linitial(rule->actions);
        }
    }

    ereport(ERROR, (errmsg("failed to find _RETURN rule for view")));
    return NULL; /* keep compiler quiet */
}

/*
 * view_has_instead_trigger - does view have an INSTEAD OF trigger for event?
 *
 * If it does, we don't want to treat it as auto-updatable.  This test can't
 * be folded into view_query_is_auto_updatable because it's not an error
 * condition.
 */
bool view_has_instead_trigger(Relation view, CmdType event)
{
    TriggerDesc *trigDesc = view->trigdesc;

    switch (event) {
        case CMD_INSERT:
            if (trigDesc && trigDesc->trig_insert_instead_row)
                return true;
            break;
        case CMD_UPDATE:
            if (trigDesc && trigDesc->trig_update_instead_row)
                return true;
            break;
        case CMD_DELETE:
            if (trigDesc && trigDesc->trig_delete_instead_row)
                return true;
            break;
        default:
            ereport(ERROR, (errmsg("unrecognized CmdType: %d", (int)event)));
            break;
    }
    return false;
}

/*
 * view_col_is_auto_updatable - test whether the specified column of a view
 * is auto-updatable. Returns NULL (if the column can be updated) or a message
 * string giving the reason that it cannot be.
 *
 * Note that the checks performed here are local to this view. We do not check
 * whether the referenced column of the underlying base relation is updatable.
 */
static const char* view_col_is_auto_updatable(RangeTblRef* rtr, TargetEntry* tle)
{
    Var* var = (Var*)tle->expr;

    /*
     * For now, the only updatable columns we support are those that are Vars
     * referring to user columns of the underlying base relation.
     *
     * The view targetlist may contain resjunk columns (e.g., a view defined
     * like "SELECT * FROM t ORDER BY a+b" is auto-updatable) but such columns
     * are not auto-updatable, and in fact should never appear in the outer
     * query's targetlist.
     */
    if (tle->resjunk)
        return gettext_noop("Junk view columns are not updatable.");

    if (!IsA(var, Var) || var->varno != (unsigned int)rtr->rtindex || var->varlevelsup != 0)
        return gettext_noop("View columns that are not columns of their base relation are not updatable.");

    if (var->varattno < 0)
        return gettext_noop("View columns that refer to system columns are not updatable.");

    if (var->varattno == 0)
        return gettext_noop("View columns that return whole-row references are not updatable.");

    return NULL; /* the view column is updatable */
}

/*
 * view_query_is_auto_updatable - test whether the specified view definition
 * represents an auto-updatable view. Returns NULL (if the view can be updated)
 * or a message string giving the reason that it cannot be.
 *
 * If check_cols is true, the view is required to have at least one updatable
 * column (necessary for INSERT/UPDATE). Otherwise the view's columns are not
 * checked for updatability. See also view_cols_are_auto_updatable.
 *
 * Note that the checks performed here are only based on the view definition.
 * We do not check whether any base relations referred to by the view are
 * updatable.
 */
const char* view_query_is_auto_updatable(Query *viewquery, bool check_cols)
{
    RangeTblRef* rtr = NULL;
    RangeTblEntry* base_rte = NULL;

    /*----------
     * Check if the view is simply updatable.  According to SQL-92 this means:
     *	- No DISTINCT clause.
     *	- Each TLE is a column reference, and each column appears at most once.
     *	- FROM contains exactly one base relation.
     *	- No GROUP BY or HAVING clauses.
     *	- No set operations (UNION, INTERSECT or EXCEPT).
     *	- No sub-queries in the WHERE clause that reference the target table.
     *
     * We ignore that last restriction since it would be complex to enforce
     * and there isn't any actual benefit to disallowing sub-queries.  (The
     * semantic issues that the standard is presumably concerned about don't
     * arise in Postgres, since any such sub-query will not see any updates
     * executed by the outer query anyway, thanks to MVCC snapshotting.)
     *
     * We also relax the second restriction by supporting part of SQL:1999
     * feature T111, which allows for a mix of updatable and non-updatable
     * columns, provided that an INSERT or UPDATE doesn't attempt to assign to
     * a non-updatable column.
     *
     * In addition we impose these constraints, involving features that are
     * not part of SQL-92:
     *	- No CTEs (WITH clauses).
     *	- No OFFSET or LIMIT clauses (this matches a SQL:2008 restriction).
     *	- No system columns (including whole-row references) in the tlist.
     *	- No window functions in the tlist.
     *	- No set-returning functions in the tlist.
     *
     * Note that we do these checks without recursively expanding the view.
     * If the base relation is a view, we'll recursively deal with it later.
     *----------
     */
    if (viewquery->distinctClause != NIL)
        return gettext_noop("Views containing DISTINCT are not automatically updatable.");

    if (viewquery->groupClause != NIL)
        return gettext_noop("Views containing GROUP BY are not automatically updatable.");

    if (viewquery->havingQual != NULL)
        return gettext_noop("Views containing HAVING are not automatically updatable.");

    if (viewquery->setOperations != NULL)
        return gettext_noop("Views containing UNION, INTERSECT or EXCEPT are not automatically updatable.");

    if (viewquery->cteList != NIL)
        return gettext_noop("Views containing WITH are not automatically updatable.");

    if (viewquery->limitOffset != NULL || viewquery->limitCount != NULL)
        return gettext_noop("Views containing LIMIT or OFFSET are not automatically updatable.");

    /*
     * We must not allow window functions or set returning functions in the
     * targetlist. Otherwise we might end up inserting them into the quals of
     * the main query. We must also check for aggregates in the targetlist in
     * case they appear without a GROUP BY.
     *
     * These restrictions ensure that each row of the view corresponds to a
     * unique row in the underlying base relation.
     */
    if (viewquery->hasAggs)
        return gettext_noop("Views that return aggregate functions are not automatically updatable.");

    if (viewquery->hasWindowFuncs)
        return gettext_noop("Views that return window functions are not automatically updatable.");

    if (expression_returns_set((Node *) viewquery->targetList))
        return gettext_noop("Views that return set-returning functions are not automatically updatable.");

    /*
     * The view query should select from a single base relation, which must be
     * a table or another view.
     */
    if (list_length(viewquery->jointree->fromlist) != 1)
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    rtr = (RangeTblRef*)linitial(viewquery->jointree->fromlist);
    if (!IsA(rtr, RangeTblRef))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
    if (base_rte->rtekind != RTE_RELATION || (base_rte->relkind != RELKIND_RELATION &&
        base_rte->relkind != RELKIND_FOREIGN_TABLE && base_rte->relkind != RELKIND_VIEW))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    /*
     * Check that the view has at least one updatable column. This is required
     * for INSERT/UPDATE but not for DELETE.
     */
    if (check_cols) {
        ListCell* cell= NULL;
        bool found = false;

        foreach (cell, viewquery->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);

            if (view_col_is_auto_updatable(rtr, tle) == NULL) {
                found = true;
                break;
            }
        }

        if (!found)
            return gettext_noop("Views that have no updatable columns are not automatically updatable.");
    }

    return NULL; /* the view is simply updatable */
}

/*
 * view_cols_are_auto_updatable - test whether all of the required columns of
 * an auto-updatable view are actually updatable. Returns NULL (if all the
 * required columns can be updated) or a message string giving the reason that
 * they cannot be.
 *
 * This should be used for INSERT/UPDATE to ensure that we don't attempt to
 * assign to any non-updatable columns.
 *
 * Additionally it may be used to retrieve the set of updatable columns in the
 * view, or if one or more of the required columns is not updatable, the name
 * of the first offending non-updatable column.
 *
 * The caller must have already verified that this is an auto-updatable view
 * using view_query_is_auto_updatable.
 *
 * Note that the checks performed here are only based on the view definition.
 * We do not check whether the referenced columns of the base relation are
 * updatable.
 */
static const char* view_cols_are_auto_updatable(Query *viewquery, Bitmapset *required_cols,
    Bitmapset **updatable_cols, char **non_updatable_col)
{
    RangeTblRef* rtr = NULL;
    AttrNumber col;
    ListCell* cell = NULL;

    /*
    * The caller should have verified that this view is auto-updatable and
    * so there should be a single base relation.
    */
    Assert(list_length(viewquery->jointree->fromlist) == 1);
    rtr = (RangeTblRef *) linitial(viewquery->jointree->fromlist);
    Assert(IsA(rtr, RangeTblRef));

    /* Initialize the optional return values */
    if (updatable_cols != NULL)
        *updatable_cols = NULL;
    if (non_updatable_col != NULL)
        *non_updatable_col = NULL;

    /* Test each view column for updatability */
    col = -FirstLowInvalidHeapAttributeNumber;
    foreach (cell, viewquery->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(cell);
        const char* col_update_detail;

        col++;
        col_update_detail = view_col_is_auto_updatable(rtr, tle);

        if (col_update_detail == NULL) {
            /* The column is updatable */
            if (updatable_cols != NULL)
                *updatable_cols = bms_add_member(*updatable_cols, col);
        } else if (bms_is_member(col, required_cols)) {
            /* The required column is not updatable */
            if (non_updatable_col != NULL)
                *non_updatable_col = tle->resname;
            return col_update_detail;
        }
    }

    return NULL; /* all the required view columns are updatable */
}

/*
 * relation_is_updatable - determine which update events the specified
 * relation supports.
 *
 * Note that views may contain a mix of updatable and non-updatable columns.
 * For a view to support INSERT/UPDATE it must have at least one updatable
 * column, but there is no such restriction for DELETE. If include_cols is
 * non-NULL, then only the specified columns are considered when testing for
 * updatability.
 *
 * This is used for the information_schema views, which have separate concepts
 * of "updatable" and "trigger updatable".	A relation is "updatable" if it
 * can be updated without the need for triggers (either because it has a
 * suitable RULE, or because it is simple enough to be automatically updated).
 * A relation is "trigger updatable" if it has a suitable INSTEAD OF trigger.
 * The SQL standard regards this as not necessarily updatable, presumably
 * because there is no way of knowing what the trigger will actually do.
 * The information_schema views therefore call this function with
 * include_triggers = false.  However, other callers might only care whether
 * data-modifying SQL will work, so they can pass include_triggers = true
 * to have trigger updatability included in the result.
 *
 * The return value is a bitmask of rule event numbers indicating which of
 * the INSERT, UPDATE and DELETE operations are supported.	(We do it this way
 * so that we can test for UPDATE plus DELETE support in a single call.)
 */
int relation_is_updatable(Oid reloid, bool include_triggers, Bitmapset* include_cols)
{
    int events = 0;
    Relation rel;
    RuleLock* rulelocks = NULL;

#define ALL_EVENTS ((1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE))

    rel = try_relation_open(reloid, AccessShareLock);

    /*
     * If the relation doesn't exist, return zero rather than throwing an
     * error.  This is helpful since scanning an information_schema view
     * under MVCC rules can result in referencing rels that were just
     * deleted according to a SnapshotNow probe.
     */
    if (rel == NULL)
        return 0;

    /* If the relation is a table, it is always updatable */
    if (rel->rd_rel->relkind == RELKIND_RELATION) {
        relation_close(rel, AccessShareLock);
        return ALL_EVENTS;
    }

    /* Look for unconditional DO INSTEAD rules, and note supported events */
    rulelocks = rel->rd_rules;
    if (rulelocks != NULL) {
        int i;

        for (i = 0; i < rulelocks->numLocks; i++) {
            if (rulelocks->rules[i]->isInstead && rulelocks->rules[i]->qual == NULL) {
                events |= ((1 << rulelocks->rules[i]->event) & ALL_EVENTS);
            }
        }

        /* If we have rules for all events, we're done */
        if (events == ALL_EVENTS) {
            relation_close(rel, AccessShareLock);
            return events;
        }
    }

    /* Similarly look for INSTEAD OF triggers, if they are to be included */
    if (include_triggers) {
        TriggerDesc *trigDesc = rel->trigdesc;

        if (trigDesc) {
            if (trigDesc->trig_insert_instead_row)
                events |= (1 << CMD_INSERT);
            if (trigDesc->trig_update_instead_row)
                events |= (1 << CMD_UPDATE);
            if (trigDesc->trig_delete_instead_row)
                events |= (1 << CMD_DELETE);

            /* If we have triggers for all events, we're done */
            if (events == ALL_EVENTS) {
                relation_close(rel, AccessShareLock);
                return events;
            }
        }
    }

    /* If this is a foreign table, check which update events it supports */
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
        FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);

        if (fdwroutine->IsForeignRelUpdatable != NULL)
            events |= fdwroutine->IsForeignRelUpdatable(rel);
        else {
            /* Assume presence of executor functions is sufficient */
            if (fdwroutine->ExecForeignInsert != NULL)
                events |= (1 << CMD_INSERT);
            if (fdwroutine->ExecForeignUpdate != NULL)
                events |= (1 << CMD_UPDATE);
            if (fdwroutine->ExecForeignDelete != NULL)
                events |= (1 << CMD_DELETE);
        }

        relation_close(rel, AccessShareLock);
        return events;
    }

    /* Check if this is an automatically updatable view */
    if (rel->rd_rel->relkind == RELKIND_VIEW) {
        Query* viewquery = get_view_query(rel);

        if (view_query_is_auto_updatable(viewquery, false) == NULL) {
            Bitmapset* updatable_cols;
            int auto_events;
            RangeTblRef* rtr;
            RangeTblEntry* base_rte;
            Oid baseoid;

            /*
             * Determine which of the view's columns are updatable. If there
             * are none within the set of of columns we are looking at, then
             * the view doesn't support INSERT/UPDATE, but it may still
             * support DELETE.
             */
            view_cols_are_auto_updatable(viewquery, NULL, &updatable_cols, NULL);

            if (include_cols != NULL)
                updatable_cols = bms_int_members(updatable_cols, include_cols);

            if (bms_is_empty(updatable_cols))
                auto_events = (1 << CMD_DELETE); /* May support DELETE */
            else
                auto_events = ALL_EVENTS; /* May support all events */

            /*
             * The base relation must also support these update commands.
             * Tables are always updatable, but for any other kind of base
             * relation we must do a recursive check limited to the columns
             * referenced by the locally updatable columns in this view.
             */
            rtr = (RangeTblRef*)linitial(viewquery->jointree->fromlist);
            base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
            Assert(base_rte->rtekind == RTE_RELATION);

            if (base_rte->relkind != RELKIND_RELATION) {
                baseoid = base_rte->relid;
                include_cols = adjust_view_column_set(updatable_cols, viewquery->targetList);
                auto_events &= relation_is_updatable(baseoid,
                                                    include_triggers,
                                                    include_cols);
            }
            events |= auto_events;
        }
       
    }

    /* If we reach here, the relation may support some update commands */
    relation_close(rel, AccessShareLock);
    return events;
}

/*
 * adjust_view_column_set - map a set of column numbers according to targetlist
 *
 * This is used with simply-updatable views to map column-permissions sets for
 * the view columns onto the matching columns in the underlying base relation.
 * The targetlist is expected to be a list of plain Vars of the underlying
 * relation (as per the checks above in view_query_is_auto_updatable).
 */
static Bitmapset* adjust_view_column_set(Bitmapset* cols, List* targetlist)
{
    Bitmapset* result = NULL;
    Bitmapset* tmpcols = NULL;
    AttrNumber col;

    tmpcols = bms_copy(cols);
    while ((col = bms_first_member(tmpcols)) >= 0) {
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

        if (attno == InvalidAttrNumber) {
            /*
            * There's a whole-row reference to the view.  For permissions
            * purposes, treat it as a reference to each column available from
            * the view.  (We should *not* convert this to a whole-row
            * reference to the base relation, since the view may not touch
            * all columns of the base relation.)
            */
            ListCell* lc = NULL;

            foreach(lc, targetlist) {
                TargetEntry* tle = (TargetEntry *) lfirst(lc);
                Var* var = NULL;

                if (tle->resjunk)
                    continue;
                var = (Var*)tle->expr;
                Assert(IsA(var, Var));
                result = bms_add_member(result, var->varattno - FirstLowInvalidHeapAttributeNumber);
            }
        } else {
            /*
            * Views do not have system columns, so we do not expect to see
            * any other system attnos here.  If we do find one, the error
            * case will apply.
            */
            TargetEntry* tle = get_tle_by_resno(targetlist, attno);

            if (tle != NULL && !tle->resjunk && IsA(tle->expr, Var)) {
                Var* var = (Var*)tle->expr;

                result = bms_add_member(result, var->varattno - FirstLowInvalidHeapAttributeNumber);
            } else
                ereport(ERROR, (errmsg("attribute number %d not found in view targetlist", attno)));
        }
    }
    bms_free(tmpcols);

    return result;
}

/*
 * If target relation is already exist in parsetree, make new_rte
 * and new_rt_index point to it, and return true.
 */
static bool setNewRteIfExist(Query* parsetree, Oid base_relid, int result_relation, RangeTblEntry** new_rte,
    int* new_rt_index)
{
    int rtindex = 1;
    bool targetIsExist = false;
    ListCell* lc = NULL;

    foreach (lc, parsetree->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (base_relid == rte->relid && rtindex != result_relation) {
            parsetree->resultRelations = list_delete_int(parsetree->resultRelations, result_relation);
            *new_rt_index = rtindex;
            *new_rte = rte;

            targetIsExist = true;
            break;
        }
        rtindex++;
    }

    if (targetIsExist) {
        ListCell* l = NULL;
        foreach (l, parsetree->jointree->fromlist) {
            RangeTblRef* rtf = (RangeTblRef*)lfirst(l);

            if (rtf->rtindex == result_relation) {
                parsetree->jointree->fromlist = list_delete_ptr(parsetree->jointree->fromlist, rtf);
                break;
            }
        }
    }

    return targetIsExist;
}

/*
 * rewriteTargetView -
 *	  Attempt to rewrite a query where the target relation is a view, so that
 *	  the view's base relation becomes the target relation.
 *
 * Note that the base relation here may itself be a view, which may or may not
 * have INSTEAD OF triggers or rules to handle the update.	That is handled by
 * the recursion in RewriteQuery.
 *
 * For multiple modifying, result_relation is needed to indicate which modified
 * view to rewrite.
 */
static Query* rewriteTargetView(Query *parsetree, Relation view, int result_relation)
{
    Query* viewquery = NULL;
    const char* auto_update_detail = NULL;
    RangeTblRef* rtr = NULL;
    int base_rt_index;
    int new_rt_index;
    RangeTblEntry* base_rte = NULL;
    RangeTblEntry* view_rte = NULL;
    RangeTblEntry* new_rte = NULL;
    Relation base_rel;
    List* view_targetlist = NIL;
    ListCell* lc = NULL;

    /*
     * Get the Query from the view's ON SELECT rule.  We're going to munge the
     * Query to change the view's base relation into the target relation,
     * along with various other changes along the way, so we need to make a
     * copy of it (get_view_query() returns a pointer into the relcache, so we
     * have to treat it as read-only).
     */
    viewquery = (Query*)copyObject(get_view_query(view));

    auto_update_detail = view_query_is_auto_updatable(viewquery, parsetree->commandType != CMD_DELETE);

    if (auto_update_detail) {
        /* messages here should match execMain.c's CheckValidResultRel */
        switch (parsetree->commandType) {
            case CMD_INSERT:
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot insert into view \"%s\"", RelationGetRelationName(view)),
                        errdetail_internal("%s", _(auto_update_detail)),
                        errhint("To enable inserting into the view, provide an INSTEAD OF INSERT trigger or "
                                "an unconditional ON INSERT DO INSTEAD rule.")));
                break;
            case CMD_UPDATE:
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot update view \"%s\"", RelationGetRelationName(view)),
                        errdetail_internal("%s", _(auto_update_detail)),
                        errhint("To enable updating the view, provide an INSTEAD OF UPDATE trigger or "
                                "an unconditional ON UPDATE DO INSTEAD rule.")));
                break;
            case CMD_DELETE:
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot delete from view \"%s\"", RelationGetRelationName(view)),
                        errdetail_internal("%s", _(auto_update_detail)),
                        errhint("To enable deleting from the view, provide an INSTEAD OF DELETE trigger or "
                                "an unconditional ON DELETE DO INSTEAD rule.")));
                break;
            default:
                ereport(ERROR, (errmsg("unrecognized CmdType: %d", (int)parsetree->commandType)));
                break;
        }
    }

    /*
     * For INSERT/UPDATE the modified columns must all be updatable. Note that
     * we get the modified columns from the query's targetlist, not from the
     * result RTE's modifiedCols set, since rewriteTargetListIU may have added
     * additional targetlist entries for view defaults, and these must also be
     * updatable.
     */
    if (parsetree->commandType != CMD_DELETE) {
        Bitmapset *modified_cols = NULL;
        char* non_updatable_col = NULL;

        foreach (lc, parsetree->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);

            if ((tle->rtindex == 0 || tle->rtindex == (Index)result_relation) && !tle->resjunk)
                modified_cols = bms_add_member(modified_cols, tle->resno - FirstLowInvalidHeapAttributeNumber);
        }

        auto_update_detail = view_cols_are_auto_updatable(viewquery, modified_cols, NULL, &non_updatable_col);
        if (auto_update_detail) {
            /*
            * This is a different error, caused by an attempt to update a
            * non-updatable column in an otherwise updatable view.
            */
            switch (parsetree->commandType) {
                case CMD_INSERT:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot insert into column \"%s\" of view \"%s\"", non_updatable_col,
                                    RelationGetRelationName(view)),
                            errdetail_internal("%s", _(auto_update_detail))));
                    break;
                case CMD_UPDATE:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot update column \"%s\" of view \"%s\"", non_updatable_col,
                                    RelationGetRelationName(view)),
                            errdetail_internal("%s", _(auto_update_detail))));
                    break;
                default:
                    elog(ERROR, "unrecognized CmdType: %d", (int)parsetree->commandType);
                    break;
            }
        }
    }

    /* Locate RTE describing the view in the outer query */
    view_rte = rt_fetch(result_relation, parsetree->rtable);

    /*
     * If we get here, view_query_is_auto_updatable() has verified that the
     * view contains a single base relation.
     */

    Assert(list_length(viewquery->jointree->fromlist) == 1);
    rtr = (RangeTblRef*)linitial(viewquery->jointree->fromlist);
    Assert(IsA(rtr, RangeTblRef));

    base_rt_index = rtr->rtindex;
    base_rte = rt_fetch(base_rt_index, viewquery->rtable);
    Assert(base_rte->rtekind == RTE_RELATION);

    /*
     * Up to now, the base relation hasn't been touched at all in our query.
     * We need to acquire lock on it before we try to do anything with it.
     * (The subsequent recursive call of RewriteQuery will suppose that we
     * already have the right lock!)  Since it will become the query target
     * relation, RowExclusiveLock is always the right thing.
     */
    base_rel = heap_open(base_rte->relid, RowExclusiveLock);

    /*
     * While we have the relation open, update the RTE's relkind, just in case
     * it changed since this view was made (cf. AcquireRewriteLocks).
     */
    base_rte->relkind = base_rel->rd_rel->relkind;

    heap_close(base_rel, NoLock);

    /*
     * If the view query contains any sublink subqueries then we need to also
     * acquire locks on any relations they refer to.  We know that there won't
      * be any subqueries in the range table or CTEs, so we can skip those, as
     * in AcquireRewriteLocks.
     */
    if (viewquery->hasSubLinks) {
        (void)query_tree_walker(viewquery, (bool (*)())acquireLocksOnSubLinks,NULL, QTW_IGNORE_RC_SUBQUERIES);
    }

    /*
     * Create a new target RTE describing the base relation, and add it to the
     * outer query's rangetable.  (What's happening in the next few steps is
     * very much like what the planner would do to "pull up" the view into the
     * outer query.  Perhaps someday we should refactor things enough so that
     * we can share code with the planner.)
     *
     * We will not do so if basic relation of view is already exist in rtables,
     * cause multiple-relation modifying include views is allowed.
     */
    if (!setNewRteIfExist(parsetree, base_rte->relid, result_relation, &new_rte, &new_rt_index)) {
        new_rte = base_rte;
        parsetree->rtable = lappend(parsetree->rtable, new_rte);
        new_rt_index = list_length(parsetree->rtable);
    }

    /*
     * Adjust the view's targetlist Vars to reference the new target RTE, ie
     * make their varnos be new_rt_index instead of base_rt_index.  There can
     * be no Vars for other rels in the tlist, so this is sufficient to pull
     * up the tlist expressions for use in the outer query.  The tlist will
     * provide the replacement expressions used by ReplaceVarsFromTargetList
     * below.
     */
    view_targetlist = viewquery->targetList;

    ChangeVarNodes((Node*)view_targetlist, base_rt_index, new_rt_index, 0);

    /*
     * Mark the new target RTE for the permissions checks that we want to
     * enforce against the view owner, as distinct from the query caller.  At
     * the relation level, require the same INSERT/UPDATE/DELETE permissions
     * that the query caller needs against the view.  We drop the ACL_SELECT
     * bit that is presumably in new_rte->requiredPerms initially.
     *
     * Note: the original view RTE remains in the query's rangetable list.
     * Although it will be unused in the query plan, we need it there so that
     * the executor still performs appropriate permissions checks for the
     * query caller's use of the view.
     */
    new_rte->checkAsUser = view->rd_rel->relowner;
    new_rte->requiredPerms = view_rte->requiredPerms;

    /*
     * Now for the per-column permissions bits.
     *
     * Initially, new_rte contains selectedCols permission check bits for all
     * base-rel columns referenced by the view, but since the view is a SELECT
     * query its modifiedCols is empty.  We set modifiedCols to include all
     * the columns the outer query is trying to modify, adjusting the column
     * numbers as needed.  But we leave selectedCols as-is, so the view owner
     * must have read permission for all columns used in the view definition,
     * even if some of them are not read by the outer query.  We could try to
     * limit selectedCols to only columns used in the transformed query, but
     * that does not correspond to what happens in ordinary SELECT usage of a
     * view: all referenced columns must have read permission, even if
     * optimization finds that some of them can be discarded during query
     * transformation.  The flattening we're doing here is an optional
     * optimization, too.  (If you are unpersuaded and want to change this,
     * note that applying adjust_view_column_set to view_rte->selectedCols is
     * clearly *not* the right answer, since that neglects base-rel columns
     * used in the view's WHERE quals.)
     *
     * This step needs the modified view targetlist, so we have to do things
     * in this order.
     */
    new_rte->insertedCols = bms_add_members(new_rte->insertedCols,
                                            adjust_view_column_set(view_rte->insertedCols, view_targetlist));
    new_rte->updatedCols = bms_add_members(new_rte->updatedCols,
                                           adjust_view_column_set(view_rte->updatedCols, view_targetlist));
    new_rte->modifiedCols = bms_union(new_rte->insertedCols, new_rte->updatedCols);

    /*
     * Move any security barrier quals from the view RTE onto the new target
     * RTE. Any such quals should now apply to the new target RTE and will not
     * reference the original view RTE in the rewritten query.
     */
    new_rte->securityQuals = list_concat(new_rte->securityQuals, view_rte->securityQuals);
    view_rte->securityQuals = NIL;

    /*
     * For UPDATE/DELETE, rewriteTargetListUD will have added a wholerow junk
     * TLE for the view to the end of the targetlist, which we no longer need.
     * Remove it to avoid unnecessary work when we process the targetlist.
     * Note that when we recurse through rewriteQuery a new junk TLE will be
     * added to allow the executor to find the proper row in the new target
     * relation.  (So, if we failed to do this, we might have multiple junk
     * TLEs with the same name, which would be disastrous.)
     */
    if (parsetree->commandType != CMD_INSERT) {
        ListCell* res = NULL;
        TargetEntry* tle = NULL;

        foreach (res, parsetree->targetList) {
            tle = (TargetEntry*)lfirst(res);
            if (tle->rtindex == (Index)result_relation && strcmp(tle->resname, "wholerow") == 0) {
                parsetree->targetList = list_delete_ptr(parsetree->targetList, tle);
                break;
            }
        }
    }

    /*
     * Now update all Vars in the outer query that reference the view to
     * reference the appropriate column of the base relation instead.
     */
    parsetree = (Query*)ReplaceVarsFromTargetList((Node*)parsetree,
                                                  result_relation,
                                                  0,
                                                  view_rte,
                                                  view_targetlist,
                                                  REPLACEVARS_REPORT_ERROR,
                                                  0,
                                                  &parsetree->hasSubLinks);

    /*
     * Update all other RTI references in the query that point to the view
     * (for example, parsetree->resultRelation itself) to point to the new
     * base relation instead.  Vars will not be affected since none of them
     * reference parsetree->resultRelation any longer.
     */
    ChangeVarNodes((Node*)parsetree, result_relation, new_rt_index, 0);

    /*
     * For INSERT/UPDATE we must also update resnos in the targetlist to refer
     * to columns of the base relation, since those indicate the target
     * columns to be affected.
     *
     * Note that this destroys the resno ordering of the targetlist, but that
     * will be fixed when we recurse through rewriteQuery, which will invoke
     * rewriteTargetListIU again on the updated targetlist.
     */
    if (parsetree->commandType != CMD_DELETE) {
        foreach(lc, parsetree->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            TargetEntry* view_tle = NULL;

            if (tle->resjunk)
                continue;

            view_tle = get_tle_by_resno(view_targetlist, tle->resno);
            if (view_tle != NULL && !view_tle->resjunk && IsA(view_tle->expr, Var))
                tle->resno = ((Var*)view_tle->expr)->varattno;
            else
                ereport(ERROR, (errmsg("attribute number %d not found in view targetlist", tle->resno)));
        }
    }

    /*
     * For UPDATE/DELETE, pull up any WHERE quals from the view.  We know that
     * any Vars in the quals must reference the one base relation, so we need
     * only adjust their varnos to reference the new target (just the same as
     * we did with the view targetlist).
     *
     * Note that there is special-case handling for the quals of a security
     * barrier view, since they need to be kept separate from any user-supplied
     * quals, so these quals are kept on the new target RTE.
     * For INSERT, the view's quals can be ignored in the main query.
     */
    if (parsetree->commandType != CMD_INSERT && viewquery->jointree->quals != NULL) {
        Node* viewqual = (Node*)viewquery->jointree->quals;

        ChangeVarNodes(viewqual, base_rt_index, new_rt_index, 0);

        if (RelationIsSecurityView(view)) {
            /*
            * Note: the parsetree has been mutated, so the new_rte pointer is
            * stale and needs to be re-computed.
            */
            new_rte = rt_fetch(new_rt_index, parsetree->rtable);
            new_rte->securityQuals = lcons(viewqual, new_rte->securityQuals);

            /*
            * Make sure that the query is marked correctly if the added qual
            * has sublinks.
            */
            if (!parsetree->hasSubLinks)
                parsetree->hasSubLinks = checkExprHasSubLink(viewqual);
        } else
            AddQual(parsetree, (Node*)viewqual);
    }

    /*
     * For INSERT/UPDATE, if the view has the WITH CHECK OPTION, or any parent
     * view specified WITH CASCADED CHECK OPTION, add the quals from the view
     * to the query's withCheckOptions list.
     */
    if (parsetree->commandType != CMD_DELETE) {
        bool has_wco = RelationHasCheckOption(view);
        bool cascaded = RelationHasCascadedCheckOption(view);

        /*
         * If the parent view has a cascaded check option, treat this view as
         * if it also had a cascaded check option.
         *
         * New WithCheckOptions are added to the start of the list, so if there
         * is a cascaded check option, it will be the first item in the list.
         */
        if (parsetree->withCheckOptions != NIL) {
            WithCheckOption* parent_wco = (WithCheckOption*)linitial(parsetree->withCheckOptions);

            if (parent_wco->cascaded) {
                has_wco = true;
                cascaded = true;
            }
        }

        /*
         * Add the new WithCheckOption to the start of the list, so that
         * checks on inner views are run before checks on outer views, as
         * required by the SQL standard.
         *
         * If the new check is CASCADED, we need to add it even if this view
         * has no quals, since there may be quals on child views.  A LOCAL
         * check can be omitted if this view has no quals.
         */
        if (has_wco && (cascaded || viewquery->jointree->quals != NULL)) {
            WithCheckOption* wco = makeNode(WithCheckOption);
            wco->viewname = pstrdup(RelationGetRelationName(view));
            wco->qual = NULL;
            wco->cascaded = cascaded;
            wco->rtindex = new_rt_index;

            parsetree->withCheckOptions = lcons(wco, parsetree->withCheckOptions);

            if (viewquery->jointree->quals != NULL) {
                wco->qual = (Node*)viewquery->jointree->quals;
                ChangeVarNodes(wco->qual, base_rt_index, new_rt_index, 0);

                /*
                 * Make sure that the query is marked correctly if the added
                 * qual has sublinks.  We can skip this check if the query is
                 * already marked, or if the command is an UPDATE, in which
                 * case the same qual will have already been added, and this
                 * check will already have been done.
                 */
                if (!parsetree->hasSubLinks &&
                    parsetree->commandType != CMD_UPDATE)
                    parsetree->hasSubLinks = checkExprHasSubLink(wco->qual);
            }
        }
    }

    return parsetree;
}

void ereport_for_each_cmdtype(CmdType event, Relation rt_entry_relation)
{
    switch (event) {
        case CMD_INSERT:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot perform INSERT RETURNING on relation \"%s\"",
                        RelationGetRelationName(rt_entry_relation)),
                    errhint("You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.")));
            break;
        case CMD_UPDATE:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot perform UPDATE RETURNING on relation \"%s\"",
                        RelationGetRelationName(rt_entry_relation)),
                    errhint("You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.")));
            break;
        case CMD_DELETE:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot perform DELETE RETURNING on relation \"%s\"",
                        RelationGetRelationName(rt_entry_relation)),
                    errhint("You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.")));
            break;
        default: {
            ereport(
                ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("unrecognized commandType: %d", (int)event)));
        } break;
    }
}

/*
 * RewriteQuery -
 *	  rewrites the query and apply the rules again on the queries rewritten
 *
 * rewrite_events is a list of open query-rewrite actions, so we can detect
 * infinite recursion.
 */
static List* RewriteQuery(Query* parsetree, List* rewrite_events)
{
    CmdType event = parsetree->commandType;
    bool instead = false;
    bool returning = false;
    Query* qual_product = NULL;
    List* rewritten = NIL;
    ListCell* lc1 = NULL;

#ifdef PGXC
    List* parsetree_list = NIL;
    List* qual_product_list = NIL;
    ListCell* pt_cell = NULL;
#endif

    /*
     * First, recursively process any insert/update/delete statements in WITH
     * clauses.  (We have to do this first because the WITH clauses may get
     * copied into rule actions below.)
     */
    foreach (lc1, parsetree->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc1);
        Query* ctequery = (Query*)cte->ctequery;
        List* newstuff = NIL;

        AssertEreport(IsA(ctequery, Query), MOD_OPT, "");

        if (ctequery->commandType == CMD_SELECT)
            continue;

        newstuff = RewriteQuery(ctequery, rewrite_events);

        /*
         * Currently we can only handle unconditional, single-statement DO
         * INSTEAD rules correctly; we have to get exactly one Query out of
         * the rewrite operation to stuff back into the CTE node.
         */
        if (list_length(newstuff) == 1) {
            /* Push the single Query back into the CTE node */
            ctequery = (Query*)linitial(newstuff);
            AssertEreport(IsA(ctequery, Query), MOD_OPT, "");
            /* WITH queries should never be canSetTag */
            AssertEreport(!ctequery->canSetTag, MOD_OPT, "");
            cte->ctequery = (Node*)ctequery;
        } else if (newstuff == NIL) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH")));
        } else {
            ListCell* lc2 = NULL;

            /* examine queries to determine which error message to issue */
            foreach (lc2, newstuff) {
                Query* q = (Query*)lfirst(lc2);

                if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("conditional DO INSTEAD rules are not supported for data-modifying statements in "
                                   "WITH")));
                if (q->querySource == QSRC_NON_INSTEAD_RULE)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("DO ALSO rules are not supported for data-modifying statements in WITH")));
            }

            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg(
                        "multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH")));
        }
    }

    /*
     * If the statement is an insert, update, or delete, adjust its targetlist
     * as needed, and then fire INSERT/UPDATE/DELETE rules on it.
     *
     * SELECT rules are handled later when we have all the queries that should
     * get executed.  Also, utilities aren't rewritten at all (do we still
     * need that check?)
     */
    if (event != CMD_SELECT && event != CMD_UTILITY) {
        int result_relation;
        RangeTblEntry* rt_entry = NULL;
        Relation rt_entry_relation;
        List* locks = NIL;
        bool hasGenCol = false;
        List* product_queries = NIL;
        List* attrnos = NIL;
        int values_rte_index = 0;
        bool defaults_remaining = false;
        ListCell* resultRel = NULL;
        bool rewriteView = false;
        List* rewriteRelations = NIL;

        result_relation = linitial2_int(parsetree->resultRelations);

        if (result_relation == 0)
            return rewritten;

        rt_entry = rt_fetch(result_relation, parsetree->rtable);
        AssertEreport(rt_entry->rtekind == RTE_RELATION, MOD_OPT, "");

        /*
         * We can use NoLock here since either the parser or
         * AcquireRewriteLocks should have locked the rel already.
         */
        rt_entry_relation = heap_open(rt_entry->relid, NoLock);

        /*
         * Rewrite the targetlist as needed for the command type.
         */
        if (event == CMD_INSERT) {
            RangeTblEntry* values_rte = NULL;

            /*
             * If it's an INSERT ... VALUES (...), (...), ... there will be a
             * single RTE for the VALUES targetlists.
             */
            if (list_length(parsetree->jointree->fromlist) == 1) {
                RangeTblRef* rtr = (RangeTblRef*)linitial(parsetree->jointree->fromlist);

                if (IsA(rtr, RangeTblRef)) {
                    RangeTblEntry* rte = rt_fetch(rtr->rtindex, parsetree->rtable);

                    if (rte->rtekind == RTE_VALUES) {
                        values_rte = rte;
                        values_rte_index = rtr->rtindex;
                    }
                }
            }

            if (values_rte != NULL) {
                /* Process the main targetlist ... */
                parsetree->targetList =
                    rewriteTargetListIU(parsetree->targetList, parsetree->commandType,
                                        rt_entry_relation, result_relation, &attrnos,
                                        &hasGenCol);
                checkGenDefault(values_rte, rt_entry_relation, attrnos, hasGenCol);
                /* ... and the VALUES expression lists */
                if (!rewriteValuesRTE(parsetree, values_rte, rt_entry_relation, attrnos, false)) {
                    defaults_remaining = true;
                }
            } else {
                /* Process just the main targetlist */
                parsetree->targetList =
                    rewriteTargetListIU(parsetree->targetList, parsetree->commandType,
                                        rt_entry_relation, result_relation, NULL,
                                        &hasGenCol);
            }

            if (parsetree->upsertClause != NULL &&
                parsetree->upsertClause->upsertAction == UPSERT_UPDATE) {
                parsetree->upsertClause->updateTlist =
                    rewriteTargetListIU(parsetree->upsertClause->updateTlist, CMD_UPDATE,
                                        rt_entry_relation, result_relation, NULL,
                                        &hasGenCol);
            }
        } else if (event == CMD_UPDATE) {
            if (list_length(parsetree->resultRelations) > 1) {
                rewriteTargetListMutilUpdate(parsetree, parsetree->rtable, parsetree->resultRelations);
                /* Also populate extraUpdatedCols (for generated columns) */
                multiUpdateSetExtraUpdatedCols(parsetree);
            } else {
                parsetree->targetList =
                    rewriteTargetListIU(parsetree->targetList, parsetree->commandType,
                                        rt_entry_relation, result_relation, NULL,
                                        &hasGenCol);
                /* Also populate extraUpdatedCols (for generated columns) */
                setExtraUpdatedCols(rt_entry, rt_entry_relation->rd_att);
                rewriteTargetListUD(parsetree, rt_entry, rt_entry_relation, result_relation);
            }
        } else if (event == CMD_DELETE) {
            if (list_length(parsetree->resultRelations) > 1) {
                rewriteTargetListMutilUD(parsetree, parsetree->rtable, parsetree->resultRelations);
            } else {
                rewriteTargetListUD(parsetree, rt_entry, rt_entry_relation, result_relation);
            }
        } else if (event == CMD_MERGE) {
            /*
             * Rewrite each action targetlist separately
             */
            foreach (lc1, parsetree->mergeActionList) {
                MergeAction* action = (MergeAction*)lfirst(lc1);

                switch (action->commandType) {
                    case CMD_DELETE: /* Nothing to do here */
                        break;
                    case CMD_UPDATE:
                        action->targetList = rewriteTargetListMergeInto(action->targetList,
                            action->commandType,
                            rt_entry_relation,
                            result_relation,
                            NULL);
                        break;
                    case CMD_INSERT: {
                        action->targetList = rewriteTargetListMergeInto(action->targetList,
                            action->commandType,
                            rt_entry_relation,
                            result_relation,
                            NULL);
                    } break;
                    default: {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                errmsg("unrecognized commandType: %d", action->commandType)));
                    } break;
                }
            }
            if (parsetree->upsertQuery != NULL) {
                List* querytree_list = QueryRewrite(parsetree->upsertQuery);
                if (list_length(querytree_list) != 1) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS ), errmsg("Unexpected status in upsert to merge.")));
                }
                parsetree->upsertQuery = (Query*)linitial(querytree_list);
            }
            /* Also populate extraUpdatedCols (for generated columns) */
            setExtraUpdatedCols(rt_entry, rt_entry_relation->rd_att);
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unrecognized commandType: %d", (int)event)));
        }

#ifdef PGXC
        if (parsetree_list == NIL) {
#endif
            /*
             * Collect and apply the appropriate rules.
             */
            locks = matchLocks(event, rt_entry_relation->rd_rules, result_relation, parsetree);

#ifdef ENABLE_MULTIPLE_NODES
            if (IS_PGXC_COORDINATOR) {
                product_queries =
                    fireRules(parsetree, result_relation, event, locks, &instead, &returning, &qual_product);
            }
#else
            product_queries =
                fireRules(parsetree, result_relation, event, locks, &instead, &returning, &qual_product);
#endif

            /*
             * Relation has rules in multiple-relations modifying doesn't support,
             * which is checked in CheckUDRelations.
             */
            Assert(list_length(parsetree->resultRelations) <= 1 || product_queries == NULL);

            /*
             * If we have a VALUES RTE with any remaining untouched DEFAULT items,
             * and we got any product queries, finalize the VALUES RTE for each
             * product query (replacing the remaining DEFAULT items with NULLs).
             * We don't do this for the original query, because we know that it
             * must be an auto-insert on a view, and so should use the base
             * relation's defaults for any remaining DEFAULT items.
             */
            if (defaults_remaining && product_queries != NIL) {
                ListCell* n = NULL;

                /*
                 * Each product query has its own copy of the VALUES RTE at the
                 * same index in the rangetable, so we must finalize each one.
                 */
                foreach(n, product_queries) {
                    Query* pt = (Query*)lfirst(n);
                    RangeTblEntry* values_rte = rt_fetch(values_rte_index, pt->rtable);

                    rewriteValuesRTE(pt, values_rte, rt_entry_relation, attrnos, true);
                }
            }

            if (product_queries != NIL) {
                rewriteRelations = lappend_oid(rewriteRelations, RelationGetRelid(rt_entry_relation));
            }

            /* Element of resultRelations may be deleted under rewriteTargetView. */
            List* tempResultRelations = (List*)copyObject(parsetree->resultRelations);
            foreach (resultRel, tempResultRelations) {
                result_relation = lfirst_int(resultRel);

                heap_close(rt_entry_relation, NoLock);

                rt_entry = rt_fetch(result_relation, parsetree->rtable);
                rt_entry_relation = heap_open(rt_entry->relid, NoLock);

                /*
                 * If there was no unqualified INSTEAD rule, and the target relation
                 * is a view without any INSTEAD OF triggers, see if the view can be
                 * automatically updated.  If so, we perform the necessary query
                 * transformation here and add the resulting query to the
                 * product_queries list, so that it gets recursively rewritten if
                 * necessary.
                 *
                 * If the view cannot be automatically updated, we throw an error here
                 * which is OK since the query would fail at runtime anyway.  Throwing
                 * the error here is preferable to the executor check since we have
                 * more detailed information available about why the view isn't
                 * updatable.
                 */
                if (!instead && rt_entry_relation->rd_rel->relkind == RELKIND_VIEW &&
                    !view_has_instead_trigger(rt_entry_relation, event)) {
                    /*
                     * If there were any qualified INSTEAD rules, don't allow the view
                     * to be automatically updated (an unqualified INSTEAD rule or
                     * INSTEAD OF trigger is required).
                     *
                     * The messages here should match execMain.c's CheckValidResultRel
                     * and in principle make those checks in executor unnecessary, but
                     * we keep them just in case.
                     */
                    if (qual_product != NULL) {
                        switch (parsetree->commandType) {
                            case CMD_INSERT:
                                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                        errmsg("cannot insert into view \"%s\"",
                                            RelationGetRelationName(rt_entry_relation)),
                                        errdetail("Views with conditional DO INSTEAD rules are not "
                                                "automatically updatable."),
                                        errhint("To enable inserting into the view, provide an INSTEAD OF INSERT trigger "
                                                "or an unconditional ON INSERT DO INSTEAD rule.")));
                                break;
                            case CMD_UPDATE:
                                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                        errmsg("cannot update view \"%s\"",
                                                RelationGetRelationName(rt_entry_relation)),
                                        errdetail("Views with conditional DO INSTEAD rules are not "
                                                "automatically updatable."),
                                        errhint("To enable updating the view, provide an INSTEAD OF UPDATE trigger "
                                                "or an unconditional ON UPDATE DO INSTEAD rule.")));
                                break;
                            case CMD_DELETE:
                                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                        errmsg("cannot delete from view \"%s\"",
                                                RelationGetRelationName(rt_entry_relation)),
                                        errdetail("Views with conditional DO INSTEAD rules are not "
                                                "automatically updatable."),
                                        errhint("To enable deleting from the view, provide an INSTEAD OF DELETE trigger "
                                                "or an unconditional ON DELETE DO INSTEAD rule.")));
                                break;
                            default:
                                ereport(ERROR, (errmsg("unrecognized CmdType: %d", (int)parsetree->commandType)));
                                break;
                        }
                    }

                    /*
                     * Attempt to rewrite the query to automatically update the view.
                     * This throws an error if the view can't be automatically
                     * updated.
                     */
                    parsetree = rewriteTargetView(parsetree, rt_entry_relation, result_relation);

                    /*
                     * Set the "instead" flag, as if there had been an unqualified
                     * INSTEAD, to prevent the original query from being included a
                     * second time below.  The transformation will have rewritten any
                     * RETURNING list, so we can also set "returning" to forestall
                     * throwing an error below.
                     */
                    instead = true;
                    returning = true;
                    rewriteView = true;

                    /*
                     * If there were any unqualified INSTEAD rules, rewriteRelations
                     * may contain duplicated oid. But it is ok cause we just use the
                     * list to check if recursive.
                     */
                    rewriteRelations = lappend_oid(rewriteRelations, RelationGetRelid(rt_entry_relation));
                }
            }
            pfree(tempResultRelations);

            /*
             * At this point product_queries contains any DO ALSO rule actions.
             * Add the rewritten query before or after those.  This must match
             * the handling the original query would have gotten below, if
             * we allowed it to be included again.
             */
            if (rewriteView) {
                if (parsetree->commandType == CMD_INSERT)
                    product_queries = lcons(parsetree, product_queries);
                else
                    product_queries = lappend(product_queries, parsetree);
            }

            /*
             * If we got any product queries, recursively rewrite them --- but
             * first check for recursion!
             */
            if (product_queries != NIL) {
                ListCell* n = NULL;
                rewrite_event* rev = NULL;
                int i;

                foreach (n, rewrite_events) {
                    rev = (rewrite_event*)lfirst(n);
                    if (list_member_oid(rewriteRelations, rev->relation) && rev->event == event)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("infinite recursion detected in rules for relation \"%s\"",
                                    RelationGetRelationName(RelationIdGetRelation(rev->relation)))));
                }

                foreach (n, rewriteRelations) {
                    rev = (rewrite_event*)palloc(sizeof(rewrite_event));
                    rev->relation = lfirst_oid(n);
                    rev->event = event;
                    rewrite_events = lcons(rev, rewrite_events);
                }

                foreach (n, product_queries) {
                    Query* pt = (Query*)lfirst(n);
                    List* newstuff = NULL;

                    newstuff = RewriteQuery(pt, rewrite_events);
                    rewritten = list_concat(rewritten, newstuff);
                }

                for (i = 0; i < list_length(rewriteRelations); i++) {
                    rewrite_events = list_delete_first(rewrite_events);
                }
            }

            /*
             * If there is an INSTEAD, and the original query has a RETURNING, we
             * have to have found a RETURNING in the rule(s), else fail. (Because
             * DefineQueryRewrite only allows RETURNING in unconditional INSTEAD
             * rules, there's no need to worry whether the substituted RETURNING
             * will actually be executed --- it must be.)
             *
             * Ignore for multiple modifying cause returning clause is not supported.
             */
            if ((instead || qual_product != NULL) && parsetree->returningList && !returning) {
                ereport_for_each_cmdtype(event, rt_entry_relation);
            }

            heap_close(rt_entry_relation, NoLock);
#ifdef PGXC
        } else {
            foreach (pt_cell, parsetree_list) {
                Query* query = NULL;

                query = (Query*)lfirst(pt_cell);

                /*
                 * Collect and apply the appropriate rules.
                 */
                locks = matchLocks(event, rt_entry_relation->rd_rules, result_relation, query);

                if (locks != NIL) {
                    List* product_queries = NIL;

                    if (IS_PGXC_COORDINATOR)
                        product_queries =
                            fireRules(query, result_relation, event, locks, &instead, &returning, &qual_product);

                    qual_product_list = lappend(qual_product_list, qual_product);

                    /*
                     * If we got any product queries, recursively rewrite them --- but
                     * first check for recursion!
                     */
                    if (product_queries != NIL) {
                        ListCell* n = NULL;
                        rewrite_event* rev = NULL;

                        foreach (n, rewrite_events) {
                            rev = (rewrite_event*)lfirst(n);
                            if (rev->relation == RelationGetRelid(rt_entry_relation) && rev->event == event)
                                ereport(ERROR,
                                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                        errmsg("infinite recursion detected in rules for relation \"%s\"",
                                            RelationGetRelationName(rt_entry_relation))));
                        }

                        rev = (rewrite_event*)palloc(sizeof(rewrite_event));
                        rev->relation = RelationGetRelid(rt_entry_relation);
                        rev->event = event;
                        rewrite_events = lcons(rev, rewrite_events);

                        foreach (n, product_queries) {
                            Query* pt = (Query*)lfirst(n);
                            List* newstuff = NIL;

                            newstuff = RewriteQuery(pt, rewrite_events);
                            rewritten = list_concat(rewritten, newstuff);
                        }

                        rewrite_events = list_delete_first(rewrite_events);
                    }
                }

                /*
                 * If there is an INSTEAD, and the original query has a RETURNING, we
                 * have to have found a RETURNING in the rule(s), else fail. (Because
                 * DefineQueryRewrite only allows RETURNING in unconditional INSTEAD
                 * rules, there's no need to worry whether the substituted RETURNING
                 * will actually be executed --- it must be.)
                 */
                if ((instead || qual_product != NULL) && query->returningList && !returning) {
                    ereport_for_each_cmdtype(event, rt_entry_relation);
                }
            }

            heap_close(rt_entry_relation, NoLock);
        }
    }

    if (parsetree_list == NIL) {
#endif
        /*
         * For INSERTs, the original query is done first; for UPDATE/DELETE, it is
         * done last.  This is needed because update and delete rule actions might
         * not do anything if they are invoked after the update or delete is
         * performed. The command counter increment between the query executions
         * makes the deleted (and maybe the updated) tuples disappear so the scans
         * for them in the rule actions cannot find them.
         *
         * If we found any unqualified INSTEAD, the original query is not done at
         * all, in any form.  Otherwise, we add the modified form if qualified
         * INSTEADs were found, else the unmodified form.
         */
        if (!instead) {
            if (parsetree->commandType == CMD_INSERT) {
                if (qual_product != NULL)
                    rewritten = lcons(qual_product, rewritten);
                else
                    rewritten = lcons(parsetree, rewritten);
            } else {
                if (qual_product != NULL)
                    rewritten = lappend(rewritten, qual_product);
                else
                    rewritten = lappend(rewritten, parsetree);
            }
        }
#ifdef PGXC
    } else {
        int query_no = 0;

        foreach (pt_cell, parsetree_list) {

            Query* query = NULL;
            Query* qual = NULL;

            query = (Query*)lfirst(pt_cell);
            if (instead == false) {
                if (qual_product_list != NIL)
                    qual = (Query*)list_nth(qual_product_list, query_no);

                if (query->commandType == CMD_INSERT) {
                    if (qual != NULL)
                        rewritten = lcons(qual, rewritten);
                    else
                        rewritten = lcons(query, rewritten);
                } else {
                    if (qual != NULL)
                        rewritten = lappend(rewritten, qual);
                    else
                        rewritten = lappend(rewritten, query);
                }
            }
            query_no++;
        }
    }
#endif

    /*
     * If the original query has a CTE list, and we generated more than one
     * non-utility result query, we have to fail because we'll have copied the
     * CTE list into each result query.  That would break the expectation of
     * single evaluation of CTEs.  This could possibly be fixed by
     * restructuring so that a CTE list can be shared across multiple Query
     * and PlannableStatement nodes.
     */
    if (parsetree->cteList != NIL) {
        int qcount = 0;

        foreach (lc1, rewritten) {
            Query* q = (Query*)lfirst(lc1);

            if (q->commandType != CMD_UTILITY)
                qcount++;
        }
        if (qcount > 1)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("WITH cannot be used in a query that is rewritten by rules into multiple queries")));
    }

    if (parsetree->isReplace) {
        foreach (lc1, rewritten) {
            Query* q = (Query*)lfirst(lc1);
 
            q->isReplace = parsetree->isReplace;
        }
    }

    return rewritten;
}

/*
 * QueryRewrite -
 *	  Primary entry point to the query rewriter.
 *	  Rewrite one query via query rewrite system, possibly returning 0
 *	  or many queries.
 *
 * NOTE: the parsetree must either have come straight from the parser,
 * or have been scanned by AcquireRewriteLocks to acquire suitable locks.
 */
List* QueryRewrite(Query* parsetree)
{
    uint64 input_query_id = parsetree->queryId;
    List* querylist = NIL;
    List* results = NIL;
    ListCell* l = NULL;
    CmdType origCmdType;
    bool foundOriginalQuery = false;
    Query* lastInstead = NULL;

    /*
     * This function is only applied to top-level original queries
     */
    AssertEreport(parsetree->querySource == QSRC_ORIGINAL, MOD_OPT, "");
    AssertEreport(parsetree->canSetTag, MOD_OPT, "");
    /*
     * Step 1
     *
     * Apply all non-SELECT rules possibly getting 0 or many queries
     */
    querylist = RewriteQuery(parsetree, NIL);

    /*
     * Step 2
     *
     * Apply all the RIR rules on each query
     *
     * This is also a handy place to mark each query with the original queryId
     */
    results = NIL;
    foreach (l, querylist) {
        Query* query = (Query*)lfirst(l);

        query = fireRIRrules(query, NIL, false);

        query->queryId = input_query_id;

        results = lappend(results, query);
    }

    /*
     * Step 3
     *
     * Determine which, if any, of the resulting queries is supposed to set
     * the command-result tag; and update the canSetTag fields accordingly.
     *
     * If the original query is still in the list, it sets the command tag.
     * Otherwise, the last INSTEAD query of the same kind as the original is
     * allowed to set the tag.	(Note these rules can leave us with no query
     * setting the tag.  The tcop code has to cope with this by setting up a
     * default tag based on the original un-rewritten query.)
     *
     * The Asserts verify that at most one query in the result list is marked
     * canSetTag.  If we aren't checking asserts, we can fall out of the loop
     * as soon as we find the original query.
     */
    origCmdType = parsetree->commandType;
    foundOriginalQuery = false;
    lastInstead = NULL;

    foreach (l, results) {
        Query* query = (Query*)lfirst(l);

        if (query->querySource == QSRC_ORIGINAL) {
            AssertEreport(query->canSetTag, MOD_OPT, "");
#ifndef PGXC
            AssertEreport(!foundOriginalQuery, MOD_OPT, "");
#endif
            foundOriginalQuery = true;
#ifndef USE_ASSERT_CHECKING
            break;
#endif
        } else {
            AssertEreport(!query->canSetTag, MOD_OPT, "");
            if (query->commandType == origCmdType &&
                (query->querySource == QSRC_INSTEAD_RULE || query->querySource == QSRC_QUAL_INSTEAD_RULE))
                lastInstead = query;
        }
    }

    if (!foundOriginalQuery && lastInstead != NULL)
        lastInstead->canSetTag = true;

    if (CONVERT_STRING_DIGIT_TO_NUMERIC) {
        foreach (l, results) {
            (void)PreprocessOperator((Node*)lfirst(l), NULL);
        }
    }

    return results;
}

Const* processResToConst(char* value, Oid atttypid, Oid collid)
{
    Const *con = NULL;
    uint len = strlen(value);
    char *str_value = (char *)palloc(len + 1);
    errno_t rc = strncpy_s(str_value, len + 1, value, len + 1);
    securec_check(rc, "\0", "\0");
    str_value[len] = '\0';
    Datum str_datum = CStringGetDatum(str_value);

    /* convert value to const expression. */
    if (atttypid == BOOLOID) {
        if (strcmp(str_value, "t") == 0) {
            con = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(true), false, true);
        } else {
            con = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(false), false, true);
        }
    } else {
        con = makeConst(UNKNOWNOID, -1, collid, -2, str_datum, false, false);
    }
    return con;
}

#ifdef PGXC

char* GetCreateViewStmt(Query* parsetree, CreateTableAsStmt* stmt)
{
    /* Obtain the target list of new table */
    AssertEreport(IsA(stmt->query, Query), MOD_OPT, "");

    ViewStmt* view_stmt = makeNode(ViewStmt);
    view_stmt->view = stmt->into->rel;
    view_stmt->query = stmt->query;
    view_stmt->view->relpersistence = RELPERSISTENCE_PERMANENT;
    view_stmt->aliases = stmt->into->colNames;
    view_stmt->options = stmt->into->options;
    view_stmt->relkind = OBJECT_MATVIEW;
    view_stmt->ivm = stmt->into->ivm;

#ifdef ENABLE_MULTIPLE_NODES
    PGXCSubCluster* subcluster = NULL;

    if (stmt->into->subcluster == NULL && view_stmt->ivm) {
        char *group_name = ng_get_group_group_name(stmt->groupid);

        subcluster = makeNode(PGXCSubCluster);
        subcluster->clustertype = SUBCLUSTER_GROUP;
        subcluster->members = list_make1(makeString(group_name));

        view_stmt->subcluster = subcluster;
        stmt->into->subcluster = subcluster;
    } else {
        view_stmt->subcluster = stmt->into->subcluster;
    }
#endif

    parsetree->commandType = CMD_UTILITY;
    parsetree->utilityStmt = (Node*)view_stmt;

    StringInfo cquery = makeStringInfo();

    deparse_query(parsetree, cquery, NIL, false, false);

    return cquery->data;
}

static bool findAttrByName(const char* attributeName, List* tableElts, int maxlen)
{
    ListCell* lc = NULL;
    int i = 0;
    foreach (lc, tableElts) {
        if (i >= maxlen) {
            return false;
        }
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, ColumnDef)) {
            ColumnDef* def = (ColumnDef*)node;
            if (pg_strcasecmp(attributeName, def->colname) == 0)
                return true;
        }
        ++i;
    }
    return false;
}
/*
 * for create table as in B foramt, add type's oid and typemod in tableElts
 */
static void addInitAttrType(List* tableElts)
{
    ListCell* lc = NULL;

    foreach (lc, tableElts) {
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, ColumnDef)) {
            ColumnDef* def = (ColumnDef*)node;
            typenameTypeIdAndMod(NULL, def->typname, &def->typname->typeOid, &def->typname->typemod);
        }
    }
}

char* GetCreateTableStmt(Query* parsetree, CreateTableAsStmt* stmt)
{
    /* Start building a CreateStmt for creating the target table */
    CreateStmt* create_stmt = makeNode(CreateStmt);
    create_stmt->relation = stmt->into->rel;
    create_stmt->charset = PG_INVALID_ENCODING;
    IntoClause* into = stmt->into;
    List* tableElts = NIL;

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        tableElts = stmt->into->tableElts;
        addInitAttrType(tableElts);
    }
    int initlen = list_length(tableElts);

    /* Obtain the target list of new table */
    AssertEreport(IsA(stmt->query, Query), MOD_OPT, "");
    Query* cparsetree = (Query*)stmt->query;
    List* tlist = cparsetree->targetList;

    /*
     * Based on the targetList, populate the column information for the target
     * table. If a column name list was specified in CREATE TABLE AS, override
     * the column names derived from the query. (Too few column names are OK, too
     * many are not.).
     */
    ListCell* col = NULL;
    ListCell* lc = list_head(into->colNames);
    foreach (col, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(col);
        ColumnDef* coldef = NULL;
        ClientLogicColumnRef *coldef_enc = NULL;
        TypeName* tpname = NULL;
    
        if (IsA(tle->expr, Var)) {
            Var* ColTypProperty = (Var*)(tle->expr);
            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.  (We must check
             * this here because DefineRelation would adopt the type's default
             * collation rather than complaining.)
             */
            if (!OidIsValid(ColTypProperty->varcollid) && type_is_collatable(ColTypProperty->vartype))
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("no collation was derived for column \"%s\" with collatable type %s",
                            tle->resname,
                            format_type_be(ColTypProperty->vartype)),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
        }
    
        /* Ignore junk columns from the targetlist */
        if (tle->resjunk)
            continue;

        if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT && findAttrByName(tle->resname, tableElts, initlen)) {
            continue;
        }

        coldef = makeNode(ColumnDef);
        tpname = makeNode(TypeName);
    
        /* Take the column name specified if any */
        if (lc != NULL) {
            coldef->colname = strVal(lfirst(lc));
            lc = lnext(lc);
        } else
            coldef->colname = pstrdup(tle->resname);
    
        coldef->inhcount = 0;
        coldef->is_local = true;
        coldef->is_not_null = false;
        /* The best way to set cmprs_mode is copied from the source column defination.
         * but the cost is heavy, so make it the default method without forbiting compressing.
         * refer to ExecCreateTableAs() --> CreateIntoRelDestReceiver() --> intorel_startup()
         */
        coldef->cmprs_mode = ATT_CMPR_UNDEFINED;
        coldef->raw_default = NULL;
        coldef->cooked_default = NULL;
        coldef->update_default = NULL;
        coldef->constraints = NIL;
        coldef->collOid = exprCollation((Node*)tle->expr);

        /*
         * Set typeOid and typemod. The name of the type is derived while
         * generating query
         */
        tpname->typeOid = exprType((Node*)tle->expr);
        tpname->typemod = exprTypmod((Node*)tle->expr);
        tpname->charset = exprCharset((Node*)tle->expr);

        /* 
         * If the column of source relation is encrypted
         * instead of copying its typeOid directly
         * we should get its original defination from the catalog
         */
        if (is_enc_type(tpname->typeOid)) {
            coldef_enc = get_column_enc_def(tle->resorigtbl, tle->resname);
            if (coldef_enc != NULL) { /* should never be NULL */
                coldef_enc->dest_typname = makeTypeNameFromOid(tpname->typeOid, -1);
                
                tpname->typeOid = coldef_enc->orig_typname->typeOid;
                tpname->typemod = coldef_enc->orig_typname->typemod;
            }

            coldef->clientLogicColumnRef = coldef_enc;
        }

        coldef->typname = tpname;
    
        tableElts = lappend(tableElts, coldef);
    }
    
    if (lc != NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("%s specifies too many column names",
                       stmt->relkind == OBJECT_MATVIEW ? "CREATE MATERIALIZED VIEW" : "CREATE TABLE AS")));
    
    /*
     * Set column information and the distribution mechanism (which will be
     * NULL for SELECT INTO and the default mechanism will be picked)
     */
    create_stmt->tableElts = tableElts;
    create_stmt->distributeby = stmt->into->distributeby;
    create_stmt->subcluster = stmt->into->subcluster;

    create_stmt->tablespacename = stmt->into->tableSpaceName;
    create_stmt->oncommit = stmt->into->onCommit;
    create_stmt->row_compress = stmt->into->row_compress;
    create_stmt->options = stmt->into->options;
    create_stmt->ivm = stmt->into->ivm;
    create_stmt->relkind = stmt->relkind == OBJECT_MATVIEW ? RELKIND_MATVIEW : RELKIND_RELATION;
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        create_stmt->autoIncStart = stmt->into->autoIncStart;
    }
    /*
     * Check consistency of arguments
     */
    if (create_stmt->oncommit != ONCOMMIT_NOOP &&
        create_stmt->relation->relpersistence != RELPERSISTENCE_TEMP && 
        create_stmt->relation->relpersistence != RELPERSISTENCE_GLOBAL_TEMP) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("ON COMMIT can only be used on temporary tables")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (create_stmt->subcluster == NULL && create_stmt->relkind == RELKIND_MATVIEW && create_stmt->ivm) {
        PGXCSubCluster* subcluster = NULL;
        char *group_name = ng_get_group_group_name(stmt->groupid);

        subcluster = makeNode(PGXCSubCluster);
        subcluster->clustertype = SUBCLUSTER_GROUP;
        subcluster->members = list_make1(makeString(group_name));
        create_stmt->subcluster = subcluster;
        stmt->into->subcluster = subcluster;
    }
#endif

    /*
     * Now build a utility statement in order to run the CREATE TABLE DDL on
     * the local and remote nodes. We keep others fields as it is since they
     * are ignored anyways by deparse_query.
     */
    parsetree->commandType = CMD_UTILITY;
    parsetree->utilityStmt = (Node*)create_stmt;

    StringInfo cquery = makeStringInfo();

    deparse_query(parsetree, cquery, NIL, false, false);

    return cquery->data;
}

static bool selectNeedRecovery(Query* query)
{
    if (!query->hasRecursive || query->sql_statement == NULL) {
        return false;
    }
    return strcasestr(query->sql_statement, "CONNECT") != NULL;
}

/*
 * @Description: copy(shallow) hints which needs to be displayed in top.
*                (e.g. pull "select HINT..."  to "Insert HINT INTO XXX SELECT HINT..." )
 * @in src: hint state.
 * @out dest: hint state.
 */
static void _copy_top_HintState(HintState *dest, HintState *src)
{
    if (dest == NULL || src == NULL) {
        return;
    }

    dest->stream_hint = src->stream_hint;
    dest->gather_hint = src->gather_hint;
    dest->cache_plan_hint = src->cache_plan_hint;
    dest->set_hint = src->set_hint;
    dest->no_gpc_hint = src->no_gpc_hint;
    dest->multi_node_hint = src->multi_node_hint;
    dest->skew_hint = src->skew_hint;
    dest->predpush_hint = src->predpush_hint;
    dest->predpush_same_level_hint = src->predpush_same_level_hint;
    dest->rewrite_hint = src->rewrite_hint;
    dest->no_expand_hint = src->no_expand_hint;
}

char* GetInsertIntoStmt(CreateTableAsStmt* stmt, bool hasNewColumn)

{
    /* Get the SELECT query string */
    /*
     * If target table name is the same as an existed one and schema name is NULL, INSERT INTO
     * statement may not find the right target table. Therefore we should get the schema name
     * first so that the INSERT INTO statement can insert into the target table.
     */
    RangeVar *relation = stmt->into->rel;
    if (relation->schemaname == NULL && relation->relpersistence != RELPERSISTENCE_TEMP) {
        Oid namespaceid = RangeVarGetAndCheckCreationNamespace(relation, NoLock, NULL, RELKIND_RELATION);
        relation->schemaname = get_namespace_name(namespaceid);
    }

    StringInfo cquery = makeStringInfo();
    deparse_query((Query*)stmt->query, cquery, NIL, false, false, stmt->parserSetupArg);
    char* selectstr = pstrdup(cquery->data);

    /*
     * Check if we should recover from the rewriting performed on the select part,
     * e.g. for START WITH cases we need to do this after rewriting
     */
    Query* select_query = (Query*)stmt->query;
    if (selectNeedRecovery(select_query)) {
        selectstr = pstrdup(select_query->sql_statement);
    }

    /* Now, finally build the INSERT INTO statement */
    initStringInfo(cquery);

    appendStringInfo(cquery, "INSERT ");

    HintState *top_hintState = HintStateCreate();
    _copy_top_HintState(top_hintState, ((Query *)stmt->query)->hintState);
    get_hint_string(top_hintState, cquery);
    if (top_hintState)
        pfree((void *)top_hintState);
    if (relation->schemaname)
        appendStringInfo(
            cquery, " INTO %s.%s", quote_identifier(relation->schemaname), quote_identifier(relation->relname));
    else
        appendStringInfo(cquery, " INTO %s", quote_identifier(relation->relname));

    /* if has new column and have data to insert */
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT && hasNewColumn && !stmt->into->skipData) {
        appendStringInfoString(cquery, " (");
        ListCell* lc = NULL;
        const char* delimiter = "";
        foreach (lc, select_query->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            /* ignore junk column*/
            if (tle->resjunk)
                continue;
            appendStringInfoString(cquery, delimiter);
            delimiter = ", ";
            appendStringInfo(cquery, "%s", quote_identifier(tle->resname));
        }
        appendStringInfoString(cquery, ")");
    }

    /*
     * If the original sql contains "WITH NO DATA", just create
     * the table without inserting any data.
     */
    if (stmt->into->skipData)
        appendStringInfoString(cquery, " select null where false");
    else
        appendStringInfo(cquery, " %s", selectstr);

    /* If there is a new column, there may be a uniqueness conflict */
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT && hasNewColumn && !stmt->into->skipData) {
        switch (stmt->into->onduplicate) {
            case DUPLICATE_ERROR:
                break;
            case DUPLICATE_IGNORE:
                appendStringInfoString(cquery, " ON DUPLICATE KEY UPDATE NOTHING");
                break;
            case DUPLICATE_REPLACE: {
                appendStringInfoString(cquery, " ON DUPLICATE KEY UPDATE ");
                ListCell* lc = NULL;
                const char* delimiter = "";
                foreach (lc, select_query->targetList) {
                    TargetEntry* tle = (TargetEntry*)lfirst(lc);
                    /* ignore junk column*/
                    if (tle->resjunk)
                        continue;
                    appendStringInfoString(cquery, delimiter);
                    delimiter = ", ";
                    appendStringInfo(cquery, "%s = VALUES(%s)", quote_identifier(tle->resname), quote_identifier(tle->resname));
                }
                break;
            }
            default:
                break;
        }
    }
    return cquery->data;
}

List *query_rewrite_multiset_stmt(Query *query)
{
    List* querytree_list = NIL;
    VariableMultiSetStmt* muti_stmt = (VariableMultiSetStmt*)query->utilityStmt;
    List* stmts = muti_stmt->args;
    ListCell* cell = NULL;
    VariableSetStmt *set_stmt;

    foreach(cell, stmts) {
        Node* stmt = (Node*)lfirst(cell);

        if (nodeTag(stmt) == T_AlterSystemStmt) {
            AlterSystemStmt* alter_sys_stmt = (AlterSystemStmt *)stmt;
            set_stmt = alter_sys_stmt->setstmt;
        } else {
            set_stmt = (VariableSetStmt*)stmt;
        }

        if (list_length(set_stmt->args) > 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("set %s takes only one argument", set_stmt->name)));
        }

        if(set_stmt->kind == VAR_SET_VALUE) {
            List *resultlist = NIL;
            ListCell *l = NULL;

            foreach(l, set_stmt->args) {
                Node* expr = (Node*)lfirst(l);
                Node* node = NULL;

                if(IsA(expr, A_Const)) {
                    node = expr;
                } else {
                    node = eval_const_expression_value(NULL, expr, NULL);

                    if(nodeTag(node) != T_Const) {
                        node = QueryRewriteNonConstant(expr);
                    }
                    node = transferConstToAconst(node);
                }
                resultlist = lappend(resultlist, (A_Const*)node);
            }
            list_free(set_stmt->args);
            set_stmt->args = resultlist;
        }
    }

    querytree_list = list_make1(query);
    return querytree_list;
}

List *query_rewrite_set_stmt(Query *query)
{
    List* querytree_list = NIL;
    VariableSetStmt *stmt = (VariableSetStmt *)query->utilityStmt;

    if(DB_IS_CMPT(B_FORMAT) && stmt->kind == VAR_SET_VALUE &&
       (u_sess->attr.attr_common.enable_set_variable_b_format || ENABLE_SET_VARIABLES)) {
        List *resultlist = NIL;
        ListCell *l = NULL;

        foreach(l, stmt->args) {
            Node* expr = (Node*)lfirst(l);
            Node* node = NULL;
            if(nodeTag(expr) == T_A_Const) {
                node = expr;
            } else {
                node = eval_const_expression_value(NULL, expr, NULL);

                if(nodeTag(node) != T_Const) {
                    node = QueryRewriteNonConstant(expr);
                }
                node = transferConstToAconst(node);
            }
            resultlist = lappend(resultlist, (A_Const*)node);
        }
        list_free(stmt->args);
        stmt->args = resultlist;
    } 
    
    querytree_list = list_make1(query);
    return querytree_list;
}

List *QueryRewriteRefresh(Query *parse_tree)
{
    RefreshMatViewStmt* stmt = NULL;
    RewriteRule *rule = NULL;
    Oid         matviewOid;
    Relation    matviewRel;
    List       *actions = NIL;
    Query      *dataQuery;
    List* raw_parsetree_list = NIL;

    if (parse_tree->commandType != CMD_UTILITY || !IsA(parse_tree->utilityStmt, RefreshMatViewStmt)) {
        ereport(ERROR,
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("Unexpected commandType or intoClause is not set properly")));
    }

    /* Get the target table */
    stmt = (RefreshMatViewStmt *)parse_tree->utilityStmt;
    RangeVar *relation = stmt->relation;

    if (relation->schemaname == NULL && relation->relpersistence != RELPERSISTENCE_TEMP) {
        Oid namespaceid = RangeVarGetAndCheckCreationNamespace(relation, NoLock, NULL, RELKIND_MATVIEW);
        relation->schemaname = get_namespace_name(namespaceid);
    }

    StringInfo tquery = makeStringInfo();

    /* 1. TRUNCATE mv */
    initStringInfo(tquery);

    if (IS_PGXC_COORDINATOR) {
        appendStringInfo(tquery, "REFRESH MATERIALIZED VIEW ");
    } else {
        appendStringInfo(tquery, "TRUNCATE TABLE ");
    }

    if (relation->schemaname)
        appendStringInfo(
            tquery, "%s.%s", quote_identifier(relation->schemaname), quote_identifier(relation->relname));
    else
        appendStringInfo(tquery, "%s", quote_identifier(relation->relname));

    char *refresh_sql = tquery->data;
    if (IS_PGXC_COORDINATOR) {
        ExecUtilityStmtOnNodes(refresh_sql, NULL, false, false, EXEC_ON_DATANODES, false);
    } else {
        raw_parsetree_list = pg_parse_query(refresh_sql);
        return pg_analyze_and_rewrite((Node*)linitial(raw_parsetree_list), refresh_sql, NULL, 0);
    }

    /* 2. Insert Into .... Select statement */
    StringInfo cquery = makeStringInfo();

    /*
     * Get a lock until end of transaction.
     */
    matviewOid = RangeVarGetRelidExtended(stmt->relation,
                                         AccessExclusiveLock, false, false, false, false,
                                         RangeVarCallbackOwnsMatView, NULL);
    matviewRel = heap_open(matviewOid, NoLock);

    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("\"%s\" is not a materialized view",
                 RelationGetRelationName(matviewRel))));
    }

    if (IS_PGXC_COORDINATOR) {
        CheckRefreshMatview(matviewRel, false);
    }

    rule = matviewRel->rd_rules->rules[0];
    actions = rule->actions;
    if (list_length(actions) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("the rule for materialized view \"%s\" is not a single action",
                 RelationGetRelationName(matviewRel))));
    }

    dataQuery = (Query *) linitial(actions);

    Query* parsetree = (Query*)copyObject(dataQuery);
    deparse_query(parsetree, cquery, NIL, false, false, NULL);
    char* selectstr = pstrdup(cquery->data);
    pfree_ext(parsetree);

    initStringInfo(cquery);
    appendStringInfo(cquery, "INSERT ");

    HintState *hintState = dataQuery->hintState;
    if (hintState != NULL && hintState->multi_node_hint) {
        appendStringInfo(cquery, " /*+ multinode */ ");
    }
    
    if (hintState != NULL && hintState->sql_ignore_hint) {
        appendStringInfo(cquery, " /*+ ignore_error */ ");
    }

    if (relation->schemaname) {
        appendStringInfo(cquery, " INTO %s.%s", quote_identifier(relation->schemaname),
                         quote_identifier(relation->relname));
    } else {
        appendStringInfo(cquery, " INTO %s", quote_identifier(relation->relname));
    }

    appendStringInfo(cquery, " %s", selectstr);
    char *insert_select_sql = cquery->data;

    raw_parsetree_list = pg_parse_query(insert_select_sql);

    heap_close(matviewRel, NoLock);

    Assert(IsA(linitial(raw_parsetree_list), InsertStmt));
    linitial_node(InsertStmt, raw_parsetree_list)->isRewritten = true;

    return pg_analyze_and_rewrite((Node*)linitial(raw_parsetree_list), insert_select_sql, NULL, 0);
}


/*
 * Rewrite the CREATE TABLE AS and SELECT INTO queries as a
 * INSERT INTO .. SELECT query. The target table must be created first using
 * utility command processing. This takes care of creating the target table on
 * all the Coordinators and the Datanodes.
 */
List* QueryRewriteCTAS(Query* parsetree)
{
    List* raw_parsetree_list = NIL;
    CreateTableAsStmt* stmt = NULL;
    Query* zparsetree = NULL;
    char* view_sql = NULL;

    if (parsetree->commandType != CMD_UTILITY || !IsA(parsetree->utilityStmt, CreateTableAsStmt)) {
        ereport(ERROR,
            (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("Unexpected commandType or intoClause is not set properly")));
    }

    /* Get the target table */
    stmt = (CreateTableAsStmt*)parsetree->utilityStmt;

    /* CREATE TABLE AS */
    Query* cparsetree = (Query*)copyObject(parsetree);
    bool hasNewColumn = stmt->into->tableElts != NULL;
    char* create_sql = GetCreateTableStmt(cparsetree, stmt);
    /* move stmt->into->tableElts to create_stmt->tableElts */
    stmt->into->tableElts = NULL;

    if (stmt->relkind == OBJECT_MATVIEW) {
        zparsetree = (Query*)copyObject(cparsetree);
        view_sql = GetCreateViewStmt(zparsetree, stmt);

        ViewStmt *mvStmt = (ViewStmt *)zparsetree->utilityStmt;
        mvStmt->mv_stmt = cparsetree->utilityStmt;
        mvStmt->mv_sql = create_sql;
    }

    /* If MATILIZED VIEW exists, cannot send create table to DNs. */
    if (stmt->relkind != OBJECT_MATVIEW) {
        processutility_context proutility_cxt;
        proutility_cxt.parse_tree = cparsetree->utilityStmt;
        proutility_cxt.query_string = create_sql;
        proutility_cxt.readOnlyTree = false;
        proutility_cxt.params = NULL;
        proutility_cxt.is_top_level = true;
        ProcessUtility(&proutility_cxt, NULL, false, NULL, PROCESS_UTILITY_GENERATED, true);
    }

    /* CREATE MATILIZED VIEW AS*/
    if (stmt->relkind == OBJECT_MATVIEW) {
        Query *query = (Query *)stmt->query;

        processutility_context proutility_cxt;
        proutility_cxt.parse_tree = zparsetree->utilityStmt;
        proutility_cxt.query_string = view_sql;
        proutility_cxt.readOnlyTree = false;
        proutility_cxt.params = NULL;
        proutility_cxt.is_top_level = true;
        ProcessUtility(&proutility_cxt, NULL, false, NULL, PROCESS_UTILITY_GENERATED, true);

        create_matview_meta(query, stmt->into->rel, stmt->into->ivm);

        /* for ivm should not execute insert into ... select just return. */
        if (stmt->into->ivm) {
            return NIL;
        }

        if (IS_PGXC_COORDINATOR && IsConnFromCoord()) {
            return NIL;
        }
    }

    /*
        * Now fold the CTAS statement into an INSERT INTO statement. The
        * utility is no more required.
        */
    parsetree->utilityStmt = NULL;

    /*
        * Now fold the CTAS statement into an INSERT INTO statement. The
        * utility is no more required.
        */
    parsetree->utilityStmt = NULL;

    char* insert_into_sqlstr = GetInsertIntoStmt(stmt, hasNewColumn);

    raw_parsetree_list = pg_parse_query(insert_into_sqlstr);

    Assert(IsA(linitial(raw_parsetree_list), InsertStmt));
    linitial_node(InsertStmt, raw_parsetree_list)->isRewritten = true;

    if (stmt->parserSetup != NULL)
        return pg_analyze_and_rewrite_params(
            (Node*)linitial(raw_parsetree_list), insert_into_sqlstr, (ParserSetupHook)stmt->parserSetup, stmt->parserSetupArg);
    else {
        if (strchr(insert_into_sqlstr, '$') == NULL) {
            return pg_analyze_and_rewrite((Node*)linitial(raw_parsetree_list), insert_into_sqlstr, NULL, 0);
        } else {
            /* For plpy CTAS with $1, $2... */
            return pg_analyze_and_rewrite((Node*)linitial(raw_parsetree_list),
                insert_into_sqlstr,
                parsetree->fixed_paramTypes,
                parsetree->fixed_numParams);
        }
    }
}

/*
 * Get value from a subquery or non-constant expression by constructing SQL.
 * input:
         node: a subquery expression or non-constant expression.
 * return: Const expression.
 */
Node* QueryRewriteNonConstant(Node *node)
{
    Query* cparsetree = NULL;
    Const* con = NULL;
    List *p_target = NIL;
    Node *res = NULL;
    SelectStmt* select_stmt = makeNode(SelectStmt);

    /* get targetList. */
    TargetEntry* target = makeTargetEntry((Expr*)node, (AttrNumber)1, NULL, false);
    p_target = list_make1(target);
    select_stmt->targetList = list_copy(p_target);

    /* construct Query node for subquery. */
    cparsetree = (Query *)makeNode(Query);
    cparsetree->commandType = CMD_SELECT;
    cparsetree->utilityStmt = (Node *)select_stmt;
    cparsetree->hasSubLinks = true;
    cparsetree->canSetTag = true;
    cparsetree->jointree = makeFromExpr(NULL, NULL);
    cparsetree->targetList = list_copy(p_target);

    StringInfo select_sql = makeStringInfo();

    /* deparse the SQL statement from the subquery. */
    deparse_query(cparsetree, select_sql, NIL, false, false);

    StmtResult *result = NULL;
    if (u_sess->attr.attr_sql.dolphin) {
        int origin = u_sess->attr.attr_common.bytea_output;
        u_sess->attr.attr_common.bytea_output = BYTEA_OUTPUT_HEX;
        PG_TRY();
        {
            result = execute_stmt(select_sql->data, true);
        }
        PG_CATCH();
        {
            u_sess->attr.attr_common.bytea_output = origin;
            PG_RE_THROW();
        }
        PG_END_TRY();
        u_sess->attr.attr_common.bytea_output = origin;
    } else {
        result = execute_stmt(select_sql->data, true);
    }

    DestroyStringInfo(select_sql);

    bool isnull = result->isnulls[0];
    if (isnull) {
        Oid collid = exprCollation(node);
        /* return a null const */
        con = makeConst(UNKNOWNOID, -1, collid, -2, (Datum)0, true, false);
        (*result->pub.rDestroy)((DestReceiver *)result);
        return (Node *)con;
    }

    char* value = (char *)linitial((List *)linitial(result->tuples));
    Oid atttypid = result->atttypids[0];
    /* convert value to const expression. */
    con = processResToConst(value, atttypid, result->collids[0]);
    res = atttypid == BOOLOID ? (Node *)con : type_transfer((Node *)con, atttypid, true);

    (*result->pub.rDestroy)((DestReceiver *)result);

    return res;
}

List* QueryRewriteSelectIntoVarList(Node *node, int res_len)
{
    Query *parsetree = (Query *)((SubLink *)node)->subselect;
    List *resList = NIL;

    StringInfo select_sql = makeStringInfo();
    deparse_query(parsetree, select_sql, NIL, false, false);

    StmtResult *result = execute_stmt(select_sql->data, true);
    DestroyStringInfo(select_sql);

    if (result->tuples == NULL) {
        ListCell *target_cell = list_head(parsetree->targetList);
        for (int i = 0; i < res_len; i++, target_cell = lnext(target_cell)) {
            Oid collid = exprCollation((Node*)((TargetEntry*)lfirst(target_cell))->expr);
            Const *con = makeConst(UNKNOWNOID, -1, collid, -2, (Datum)0, true, false);
            resList = lappend(resList, con);
        }

        (*result->pub.rDestroy)((DestReceiver *)result);
        return resList;
    }

    if(result->tuples->length > 1) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("select result consisted of more than one row")));
    }

    ListCell *stmt_res_cur = list_head((List *)linitial(result->tuples));

    for (int idx = 0; idx < res_len; idx++) {
        Oid collid = result->collids[idx];
        if (result->isnulls[idx]) {
            Const *con = makeConst(UNKNOWNOID, -1, collid, -2, (Datum)0, true, false);
            resList = lappend(resList, con);
        } else {
            char *value = (char *)lfirst(stmt_res_cur);
            Oid atttypid = result->atttypids[idx];
            /* convert value to const expression. */
            Const *con = processResToConst(value, atttypid, collid);
            Node* rnode  = atttypid == BOOLOID ? (Node*)con : type_transfer((Node *)con, atttypid, true);
            resList = lappend(resList, rnode);
            stmt_res_cur = lnext(stmt_res_cur);
        }
    }

    (*result->pub.rDestroy)((DestReceiver *)result);
    return resList;
}

#endif
