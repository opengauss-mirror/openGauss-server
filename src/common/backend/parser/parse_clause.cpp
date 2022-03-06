/* -------------------------------------------------------------------------
 *
 * parse_clause.cpp
 *	  handle clauses in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_clause.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_cte.h"
#include "pgxc/pgxc.h"
#include "rewrite/rewriteManip.h"
#include "storage/tcap.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/partitionkey.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

/* clause types for findTargetlistEntrySQL92 */
#define ORDER_CLAUSE 0
#define GROUP_CLAUSE 1
#define DISTINCT_ON_CLAUSE 2

static const char* const clauseText[] = {"ORDER BY", "GROUP BY", "DISTINCT ON"};

static void extractRemainingColumns(
    List* common_colnames, List* src_colnames, List* src_colvars, List** res_colnames, List** res_colvars);
static Node* transformJoinUsingClause(
    ParseState* pstate, RangeTblEntry* leftRTE, RangeTblEntry* rightRTE, List* leftVars, List* rightVars);
static RangeTblEntry* transformTableEntry(
    ParseState* pstate, RangeVar* r, bool isFirstNode = true, bool isCreateView = false);
static RangeTblEntry* transformCTEReference(ParseState* pstate, RangeVar* r, CommonTableExpr* cte, Index levelsup);
static RangeTblEntry* transformRangeSubselect(ParseState* pstate, RangeSubselect* r);
static RangeTblEntry* transformRangeFunction(ParseState* pstate, RangeFunction* r);
static TableSampleClause* transformRangeTableSample(ParseState* pstate, RangeTableSample* rts);
static TimeCapsuleClause* transformRangeTimeCapsule(ParseState* pstate, RangeTimeCapsule* rtc);

static void setNamespaceLateralState(List *l_namespace, bool lateral_only, bool lateral_ok);
static Node* buildMergedJoinVar(ParseState* pstate, JoinType jointype, Var* l_colvar, Var* r_colvar);
static void checkExprIsVarFree(ParseState* pstate, Node* n, const char* constructName);
static TargetEntry* findTargetlistEntrySQL92(ParseState* pstate, Node* node, List** tlist, int clause);
static TargetEntry* findTargetlistEntrySQL99(ParseState* pstate, Node* node, List** tlist);
static int get_matching_location(int sortgroupref, List* sortgrouprefs, List* exprs);
static List* addTargetToGroupList(
    ParseState* pstate, TargetEntry* tle, List* grouplist, List* targetlist, int location, bool resolveUnknown);
static WindowClause* findWindowClause(List* wclist, const char* name);
static Node* transformFrameOffset(ParseState* pstate, int frameOptions, Node* clause);
static Node* flatten_grouping_sets(Node* expr, bool toplevel, bool* hasGroupingSets);
static Node* transformGroupingSet(List** flatresult, ParseState* pstate, GroupingSet* gset, List** targetlist,
    List* sortClause, bool useSQL99, bool toplevel);

static Index transformGroupClauseExpr(List** flatresult, Bitmapset* seen_local, ParseState* pstate, Node* gexpr,
    List** targetlist, List* sortClause, bool useSQL99, bool toplevel);

/*
 * @Description: append from clause item to the left tree
 * @in pstate: plan state
 * @in rte: the RTE to be appended
 * @inout top_rte: receives the RTE corresponding to the join tree item
 * @inout top_rti: received the rangetable index of the top_rte.
 * @inout relnamespace: receives a List of the RTEs exposed as relation names
 * @inout containedRels: receives a bitmap set of the rangetable indexes
 * @return: the range table reference of the RTE
 */
static RangeTblRef* transformItem(ParseState* pstate, RangeTblEntry* rte, RangeTblEntry** top_rte, int* top_rti,
    List** relnamespace)
{
    /* assume new rte is at end */
    RangeTblRef* rtr = NULL;
    int rtindex;
    rtindex = list_length(pstate->p_rtable);
    if (unlikely(rte != rt_fetch(rtindex, pstate->p_rtable))) {
        ereport(ERROR, (errmodule(MOD_OPT), errmsg("check failure with rt_fetch function")));
    }
    *top_rte = rte;
    *top_rti = rtindex;
    *relnamespace = list_make1(makeNamespaceItem(rte, false, true));
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;
    return rtr;
}

/*
 * transformFromClause -
 *	  Process the FROM clause and add items to the query's range table,
 *	  joinlist, and namespaces.
 *
 * Note: we assume that pstate's p_rtable, p_joinlist, p_relnamespace, and
 * p_varnamespace lists were initialized to NIL when the pstate was created.
 * We will add onto any entries already present --- this is needed for rule
 * processing, as well as for UPDATE and DELETE.
 *
 * The range table may grow still further when we transform the expressions
 * in the query's quals and target list. (This is possible because in
 * POSTQUEL, we allowed references to relations not specified in the
 * from-clause.  openGauss keeps this extension to standard SQL.)
 */
void transformFromClause(ParseState* pstate, List* frmList, bool isFirstNode, bool isCreateView)
{
    ListCell* fl = NULL;

    /*
     * copy original fromClause for future start with rewrite
     */
    if (pstate->p_addStartInfo) {
        pstate->sw_fromClause = (List *)copyObject(frmList);
    }

    /*
     * The grammar will have produced a list of RangeVars, RangeSubselects,
     * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
     * entries to the rtable), check for duplicate refnames, and then add it
     * to the joinlist and namespaces.
     */
    foreach (fl, frmList) {
        Node* n = (Node*)lfirst(fl);
        RangeTblEntry* rte = NULL;
        int rtindex;
        List* relnamespace = NIL;

        n = transformFromClauseItem(
            pstate, n, &rte, &rtindex, NULL, NULL, &relnamespace, isFirstNode, isCreateView);

        /* Mark the new relnamespace items as visible to LATERAL */
        setNamespaceLateralState(relnamespace, true, true);

        checkNameSpaceConflicts(pstate, pstate->p_relnamespace, relnamespace);
        pstate->p_joinlist = lappend(pstate->p_joinlist, n);
        pstate->p_relnamespace = list_concat(pstate->p_relnamespace, relnamespace);
        pstate->p_varnamespace = lappend(pstate->p_varnamespace, makeNamespaceItem(rte, true, true));
    }

    /*
     * We're done parsing the FROM list, so make all namespace items
     * unconditionally visible.  Note that this will also reset lateral_only
     * for any namespace items that were already present when we were called;
     * but those should have been that way already.
     */
    setNamespaceLateralState(pstate->p_relnamespace, false, true);
    setNamespaceLateralState(pstate->p_varnamespace, false, true);
}

/*
 * setTargetTable
 *	  Add the target relation of INSERT/UPDATE/DELETE to the range table,
 *	  and make the special links to it in the ParseState.
 *
 *	  We also open the target relation and acquire a write lock on it.
 *	  This must be done before processing the FROM list, in case the target
 *	  is also mentioned as a source relation --- we want to be sure to grab
 *	  the write lock before any read lock.
 *
 *	  If alsoSource is true, add the target to the query's joinlist and
 *	  namespace.  For INSERT, we don't want the target to be joined to;
 *	  it's a destination of tuples, not a source.	For UPDATE/DELETE,
 *	  we do need to scan or join the target.  (NOTE: we do not bother
 *	  to check for namespace conflict; we assume that the namespace was
 *	  initially empty in these cases.)
 *
 *	  Finally, we mark the relation as requiring the permissions specified
 *	  by requiredPerms.
 *
 *	  Returns the rangetable index of the target relation.
 */
int setTargetTable(ParseState* pstate, RangeVar* relation, bool inh, bool alsoSource, AclMode requiredPerms)
{
    RangeTblEntry* rte = NULL;
    int rtindex;

    /* Close old target; this could only happen for multi-action rules */
    if (pstate->p_target_relation != NULL) {
        heap_close(pstate->p_target_relation, NoLock);
    }

    /*
     * Open target rel and grab suitable lock (which we will hold till end of
     * transaction).
     *
     * free_parsestate() will eventually do the corresponding heap_close(),
     * but *not* release the lock.
     */
    pstate->p_target_relation = parserOpenTable(pstate, relation, RowExclusiveLock, true, false, true);

    /*
     * Now build an RTE.
     */
    rte = addRangeTableEntryForRelation(pstate, pstate->p_target_relation, relation->alias, inh, false);

    /* IUD contain partition. */
    if (relation->ispartition) {
        rte->isContainPartition = true;
        rte->partitionOid = getPartitionOidForRTE(rte, relation, pstate, pstate->p_target_relation);
    }
    /* IUD contain subpartition. */
    if (relation->issubpartition) {
        rte->isContainSubPartition = true;
        rte->subpartitionOid =
            GetSubPartitionOidForRTE(rte, relation, pstate, pstate->p_target_relation, &rte->partitionOid);
    }

    pstate->p_target_rangetblentry = rte;

    /* assume new rte is at end */
    rtindex = list_length(pstate->p_rtable);
    Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
    /*
     * Restrict DML privileges to pg_authid which stored sensitive messages like rolepassword.
     * there are lots of system catalog and restrict permissions of all system catalog
     * may cause too much influence. so we only restrict permissions of pg_authid temporarily
     *
     * Restrict DML privileges to gs_global_config which stored parameters like bucketmap
     * length. These parameters will not be modified after initdb.
     */
    if (IsUnderPostmaster && !g_instance.attr.attr_common.allowSystemTableMods &&
        !u_sess->attr.attr_common.IsInplaceUpgrade && IsSystemRelation(pstate->p_target_relation) &&
        (strcmp(RelationGetRelationName(pstate->p_target_relation), "pg_authid") == 0 ||
        strcmp(RelationGetRelationName(pstate->p_target_relation), "gs_global_config") == 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog",
                    RelationGetRelationName(pstate->p_target_relation))));
    }

    /* Restrict DML privileges to gs_global_chain which stored history and consistent message like hash. */
    if (IsUnderPostmaster && !u_sess->attr.attr_common.IsInplaceUpgrade && IsSystemRelation(pstate->p_target_relation)
        && strcmp(RelationGetRelationName(pstate->p_target_relation), "gs_global_chain") == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog",
                    RelationGetRelationName(pstate->p_target_relation))));
    }

    if (IS_FOREIGNTABLE(pstate->p_target_relation)||IS_STREAM_TABLE(pstate->p_target_relation)) {
        /*
         * In the security mode, the useft privilege of a user must be
         * checked before the user inserts into a foreign table.
         */
        if (isSecurityMode && !have_useft_privilege()) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to insert into foreign table in security mode")));
        }
    }

    /*
     * Override addRangeTableEntry's default ACL_SELECT permissions check, and
     * instead mark target table as requiring exactly the specified
     * permissions.
     *
     * If we find an explicit reference to the rel later during parse
     * analysis, we will add the ACL_SELECT bit back again; see
     * markVarForSelectPriv and its callers.
     */
    rte->requiredPerms = requiredPerms;

    /*
     * If UPDATE/DELETE, add table to joinlist and namespaces.
     */
    if (alsoSource) {
        addRTEtoQuery(pstate, rte, true, true, true);    
    }
    return rtindex;
}

/*
 * Simplify InhOption (yes/no/default) into boolean yes/no.
 *
 * The reason we do things this way is that we don't want to examine the
 * SQL_inheritance option flag until parse_analyze() is run.	Otherwise,
 * we'd do the wrong thing with query strings that intermix SET commands
 * with queries.
 */
bool interpretInhOption(InhOption inhOpt)
{
    switch (inhOpt) {
        case INH_NO:
            return false;
        case INH_YES:
            return true;
        case INH_DEFAULT:
            return u_sess->attr.attr_sql.SQL_inheritance;
        default:
            break;
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bogus InhOption value: %d", inhOpt)));
    return false; /* keep compiler quiet */
}

/*
 * Given a relation-options list (of DefElems), return true iff the specified
 * table/result set should be created with OIDs. This needs to be done after
 * parsing the query string because the return value can depend upon the
 * default_with_oids GUC var.
 */
bool interpretOidsOption(List* defList)
{
    ListCell* cell = NULL;

    /* Scan list to see if OIDS was included */
    foreach (cell, defList) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (def->defnamespace == NULL && pg_strcasecmp(def->defname, "oids") == 0) {
            return defGetBoolean(def);
        }
    }

    /* OIDS option was not specified, so use default. */
    return false;
}

/*
 * Extract all not-in-common columns from column lists of a source table
 */
static void extractRemainingColumns(
    List* common_colnames, List* src_colnames, List* src_colvars, List** res_colnames, List** res_colvars)
{
    List* new_colnames = NIL;
    List* new_colvars = NIL;
    ListCell *lnames = NULL;
    ListCell *lvars = NULL;

    AssertEreport(list_length(src_colnames) == list_length(src_colvars), MOD_OPT, "list length inconsistant");

    forboth(lnames, src_colnames, lvars, src_colvars)
    {
        char* colname = strVal(lfirst(lnames));
        bool match = false;
        ListCell* cnames = NULL;

        foreach (cnames, common_colnames) {
            char* ccolname = strVal(lfirst(cnames));

            if (strcmp(colname, ccolname) == 0) {
                match = true;
                break;
            }
        }

        if (!match) {
            new_colnames = lappend(new_colnames, lfirst(lnames));
            new_colvars = lappend(new_colvars, lfirst(lvars));
        }
    }

    *res_colnames = new_colnames;
    *res_colvars = new_colvars;
}

/* transformJoinUsingClause
 *	  Build a complete ON clause from a partially-transformed USING list.
 *	  We are given lists of nodes representing left and right match columns.
 *	  Result is a transformed qualification expression.
 */
static Node* transformJoinUsingClause(
    ParseState* pstate, RangeTblEntry* leftRTE, RangeTblEntry* rightRTE, List* leftVars, List* rightVars)
{
    Node* result = NULL;
    ListCell* lvars = NULL;
    ListCell* rvars = NULL;

    /*
     * We cheat a little bit here by building an untransformed operator tree
     * whose leaves are the already-transformed Vars.  This is OK because
     * transformExpr() won't complain about already-transformed subnodes.
     * However, this does mean that we have to mark the columns as requiring
     * SELECT privilege for ourselves; transformExpr() won't do it.
     */
    forboth(lvars, leftVars, rvars, rightVars)
    {
        Var* lvar = (Var*)lfirst(lvars);
        Var* rvar = (Var*)lfirst(rvars);
        A_Expr* e = NULL;

        /* Require read access to the join variables */
        markVarForSelectPriv(pstate, lvar, leftRTE);
        markVarForSelectPriv(pstate, rvar, rightRTE);

        /* Now create the lvar = rvar join condition */
        e = makeSimpleA_Expr(AEXPR_OP, "=", (Node*)copyObject(lvar), (Node*)copyObject(rvar), -1);

        /* And combine into an AND clause, if multiple join columns */
        if (result == NULL)
            result = (Node*)e;
        else {
            A_Expr* a = NULL;

            a = makeA_Expr(AEXPR_AND, NIL, result, (Node*)e, -1);
            result = (Node*)a;
        }
    }

    /*
     * Since the references are already Vars, and are certainly from the input
     * relations, we don't have to go through the same pushups that
     * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
     * the operators, and we're done.
     */
    result = transformExpr(pstate, result);

    result = coerce_to_boolean(pstate, result, "JOIN/USING");

    return result;
}

/* transformJoinOnClause
 *	  Transform the qual conditions for JOIN/ON.
 *	  Result is a transformed qualification expression.
 */
Node* transformJoinOnClause(ParseState* pstate, JoinExpr* j, RangeTblEntry* l_rte, RangeTblEntry* r_rte,
    List* relnamespace)
{
    Node* result = NULL;
    List* save_relnamespace = NIL;
    List* save_varnamespace = NIL;

    /*
     * This is a tad tricky, for two reasons.  First, the namespace that the
     * join expression should see is just the two subtrees of the JOIN plus
     * any outer references from upper pstate levels.  So, temporarily set
     * this pstate's namespace accordingly.  (We need not check for refname
     * conflicts, because transformFromClauseItem() already did.) NOTE: this
     * code is OK only because the ON clause can't legally alter the namespace
     * by causing implicit relation refs to be added.
     */
    save_relnamespace = pstate->p_relnamespace;
    save_varnamespace = pstate->p_varnamespace;

    setNamespaceLateralState(relnamespace, false, true);
    pstate->p_relnamespace = relnamespace;
    pstate->p_varnamespace = list_make2(makeNamespaceItem(l_rte, false, true),
                                        makeNamespaceItem(r_rte, false, true));

    result = transformWhereClause(pstate, j->quals, "JOIN/ON");

    pstate->p_relnamespace = save_relnamespace;
    pstate->p_varnamespace = save_varnamespace;

    return result;
}

/*
 * transformTableEntry --- transform a RangeVar (simple relation reference)
 */
static RangeTblEntry* transformTableEntry(ParseState* pstate, RangeVar* r, bool isFirstNode, bool isCreateView)
{
    RangeTblEntry* rte = NULL;

    /*
     * mark this entry to indicate it comes from the FROM clause. In SQL, the
     * target list can only refer to range variables specified in the from
     * clause but we follow the more powerful POSTQUEL semantics and
     * automatically generate the range variable if not specified. However
     * there are times we need to know whether the entries are legitimate.
     * Here, option isSupportSynonym is true, means that we one synonym object is this entry.
     */
    rte = addRangeTableEntry(pstate, r, r->alias, interpretInhOption(r->inhOpt), true, isFirstNode, isCreateView, true);

    return rte;
}

/*
 * transformCTEReference --- transform a RangeVar that references a common
 * table expression (ie, a sub-SELECT defined in a WITH clause)
 */
static RangeTblEntry* transformCTEReference(ParseState* pstate, RangeVar* r, CommonTableExpr* cte, Index levelsup)
{
    RangeTblEntry* rte = NULL;

    if (r->ispartition || r->issubpartition) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" is not partitioned table", r->relname)));
    }

    rte = addRangeTableEntryForCTE(pstate, cte, levelsup, r, true);

    return rte;
}

/*
 * transformRangeSubselect --- transform a sub-SELECT appearing in FROM
 */
static RangeTblEntry* transformRangeSubselect(ParseState* pstate, RangeSubselect* r)
{
    Query* query = NULL;
    RangeTblEntry* rte = NULL;

    /*
     * We require user to supply an alias for a subselect, per SQL92. To relax
     * this, we'd have to be prepared to gin up a unique alias for an
     * unlabeled subselect.  (This is just elog, not ereport, because the
     * grammar should have enforced it already.)
     */
    if (r->alias == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("subquery in FROM must have an alias")));
    }
    /*
     * If the subselect is LATERAL, make lateral_only names of this level
     * visible to it.  (LATERAL can't nest within a single pstate level, so we
     * don't need save/restore logic here.)
     */
    Assert(!pstate->p_lateral_active);
    pstate->p_lateral_active = r->lateral;

    /*
     * Analyze and transform the subquery.
     */
    query = parse_sub_analyze(r->subquery, pstate, NULL, isLockedRefname(pstate, r->alias->aliasname), true);

    pstate->p_lateral_active = false;

    /*
     * Check that we got something reasonable.	Many of these conditions are
     * impossible given restrictions of the grammar, but check 'em anyway.
     */
    if (!IsA(query, Query) || query->commandType != CMD_SELECT || query->utilityStmt != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("unexpected non-SELECT command in subquery in FROM")));
    }

    /*
     * OK, build an RTE for the subquery.
     */
    rte = addRangeTableEntryForSubquery(pstate, query, r->alias, r->lateral, true);

    return rte;
}

/*
 * transformRangeFunction --- transform a function call appearing in FROM
 */
static RangeTblEntry* transformRangeFunction(ParseState* pstate, RangeFunction* r)
{
    Node* funcexpr = NULL;
    char* funcname = NULL;
    RangeTblEntry* rte = NULL;

    /*
     * Get function name for possible use as alias.  We use the same
     * transformation rules as for a SELECT output expression.	For a FuncCall
     * node, the result will be the function name, but it is possible for the
     * grammar to hand back other node types.
     */
    funcname = FigureColname(r->funccallnode);

    /*
     * If the function is LATERAL, make lateral_only names of this level
     * visible to it.  (LATERAL can't nest within a single pstate level, so we
     * don't need save/restore logic here.)
     */
    Assert(!pstate->p_lateral_active);
    pstate->p_lateral_active = r->lateral;

    /*
     * Transform the raw expression.
     */
    funcexpr = transformExpr(pstate, r->funccallnode);

    pstate->p_lateral_active = false;

    /*
     * We must assign collations now so that we can fill funccolcollations.
     */
    assign_expr_collations(pstate, funcexpr);

    /*
     * Disallow aggregate functions in the expression.	(No reason to postpone
     * this check until parseCheckAggregates.)
     */
    if (pstate->p_hasAggs && checkExprHasAggs(funcexpr)) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("cannot use aggregate function in function expression in FROM"),
                parser_errposition(pstate, locate_agg_of_level(funcexpr, 0))));
    }
    if (pstate->p_hasWindowFuncs && checkExprHasWindowFuncs(funcexpr)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("cannot use window function in function expression in FROM"),
                parser_errposition(pstate, locate_windowfunc(funcexpr))));
    }

    /*
     * OK, build an RTE for the function.
     */
    rte = addRangeTableEntryForFunction(pstate, funcname, funcexpr, r, r->lateral, true);

    /*
     * If a coldeflist was supplied, ensure it defines a legal set of names
     * (no duplicates) and datatypes (no pseudo-types, for instance).
     * addRangeTableEntryForFunction looked up the type names but didn't check
     * them further than that.
     */
    if (r->coldeflist) {
        TupleDesc tupdesc;

        tupdesc =
            BuildDescFromLists(rte->eref->colnames, rte->funccoltypes, rte->funccoltypmods, rte->funccolcollations);
        CheckAttributeNamesTypes(tupdesc, RELKIND_COMPOSITE_TYPE, false);
    }

    return rte;
}

/*
 * Description: Transform a TABLESAMPLE clause
 * 			Caller has already transformed rts->relation, we just have to validate
 * 			the remaining fields and create a TableSampleClause node.
 *
 * Parameters:
 *	@in pstate: state information used during parse analysis.
 * 	@in rts: TABLESAMPLE appearing in a raw FROM clause.
 *
 * Return: TABLESAMPLE appearing in a transformed FROM clause.
 */
static TableSampleClause* transformRangeTableSample(ParseState* pstate, RangeTableSample* rts)
{
    TableSampleClause* tablesample = NULL;
    List* fargs = NIL;
    ListCell* larg = NULL;

    /* Method only one, system or bernoulli. */
    AssertEreport(list_length(rts->method) == 1, MOD_OPT, "Method shoud only one, system or bernoulli");

    char* methodName = strVal(linitial(rts->method));

    tablesample = makeNode(TableSampleClause);

    if (strncmp(methodName, "system", sizeof("system")) == 0) {
        tablesample->sampleType = SYSTEM_SAMPLE;
    } else if (strncmp(methodName, "bernoulli", sizeof("bernoulli")) == 0) {
        tablesample->sampleType = BERNOULLI_SAMPLE;
    } else if (strncmp(methodName, "hybrid", sizeof("hybrid")) == 0) {
        tablesample->sampleType = HYBRID_SAMPLE;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Invalid tablesample method %s", methodName),
                parser_errposition(pstate, rts->location)));
    }

    /* check user provided the expected number of arguments */
    if ((HYBRID_SAMPLE == tablesample->sampleType && list_length(rts->args) != SAMPLEARGSNUM) ||
        (HYBRID_SAMPLE != tablesample->sampleType && list_length(rts->args) != SAMPLEARGSNUM - 1)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("tablesample method %s requires %d argument, not %d",
                    methodName,
                    (HYBRID_SAMPLE == tablesample->sampleType ? SAMPLEARGSNUM : SAMPLEARGSNUM - 1),
                    list_length(rts->args)),
                parser_errposition(pstate, rts->location)));
    }

    /*
     * Transform the arguments, typecasting them as needed.  Note we must also
     * assign collations now, because assign_query_collations() doesn't
     * examine any substructure of RTEs.
     */
    fargs = NIL;
    foreach (larg, rts->args) {
        Node* arg = (Node*)lfirst(larg);

        arg = transformExpr(pstate, arg);
        arg = coerce_to_specific_type(pstate, arg, FLOAT4OID, "TABLESAMPLE");
        assign_expr_collations(pstate, arg);
        fargs = lappend(fargs, arg);
    }
    tablesample->args = fargs;

    /* Process REPEATABLE (seed) */
    if (rts->repeatable != NULL) {
        Node* arg = NULL;

        arg = transformExpr(pstate, rts->repeatable);
        arg = coerce_to_specific_type(pstate, arg, FLOAT8OID, "REPEATABLE");
        assign_expr_collations(pstate, arg);
        tablesample->repeatable = (Expr*)arg;
    } else {
        tablesample->repeatable = NULL;
    }

    return tablesample;
}


/*
 * Description: Transform a TABLECAPSULE clause
 * 			Caller has already transformed rtc->relation, we just have to validate
 * 			the remaining fields and create a TimeCapsuleClause node.
 *
 * Parameters:
 *	@in pstate: state information used during parse analysis.
 * 	@in rts: TABLECAPSULE appearing in a raw FROM clause.
 *
 * Return: TABLECAPSULE appearing in a transformed FROM clause.
 */
static TimeCapsuleClause* transformRangeTimeCapsule(ParseState* pstate, RangeTimeCapsule* rtc)
{
    TimeCapsuleClause* timeCapsule;

    if (rtc->tvver == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("timecapsule value must be specified"),
                parser_errposition(pstate, rtc->location)));
    }

    timeCapsule = makeNode(TimeCapsuleClause);

    timeCapsule->tvver = TvTransformVersionExpr(pstate, rtc->tvtype, rtc->tvver);
    timeCapsule->tvtype = rtc->tvtype;

    return timeCapsule;
}

/*
 * transformFromClauseItem -
 *	  Transform a FROM-clause item, adding any required entries to the
 *	  range table list being built in the ParseState, and return the
 *	  transformed item ready to include in the joinlist and namespaces.
 *	  This routine can recurse to handle SQL92 JOIN expressions.
 *
 * The function return value is the node to add to the jointree (a
 * RangeTblRef or JoinExpr).  Additional output parameters are:
 *
 * *top_rte: receives the RTE corresponding to the jointree item.
 * (We could extract this from the function return node, but it saves cycles
 * to pass it back separately.)
 *
 * *top_rti: receives the rangetable index of top_rte.	(Ditto.)
 *
 * *right_rte: receives the RTE corresponding to the right side of the
 * jointree. Only MERGE really needs to know about this and only MERGE passes a
 * non-NULL pointer.
 *
 * *right_rti: receives the rangetable index of the right_rte.
 *
 * *relnamespace: receives a List of the RTEs exposed as relation names
 * by this item.
 *
 * *containedRels: receives a bitmap set of the rangetable indexes
 * of all the base and join relations represented in this jointree item.
 * This is needed for checking JOIN/ON conditions in higher levels.
 *
 * We do not need to pass back an explicit varnamespace value, because
 * in all cases the varnamespace contribution is exactly top_rte.
 */
Node* transformFromClauseItem(ParseState* pstate, Node* n, RangeTblEntry** top_rte, int* top_rti,
    RangeTblEntry** right_rte, int* right_rti, List** relnamespace, bool isFirstNode,
    bool isCreateView, bool isMergeInto)

{
    if (IsA(n, RangeVar)) {
        /* Plain relation reference, or perhaps a CTE reference */
        RangeVar* rv = (RangeVar*)n;
        RangeTblRef* rtr = NULL;
        RangeTblEntry* rte = NULL;

        /* if it is an unqualified name, it might be a CTE reference */
        if (!rv->schemaname) {
            CommonTableExpr* cte = NULL;
            Index levelsup;

            cte = scanNameSpaceForCTE(pstate, rv->relname, &levelsup);
            if (cte != NULL) {
                rte = transformCTEReference(pstate, rv, cte, levelsup);
            }
        }

        /* if not found as a CTE, must be a table reference */
        if (rte == NULL) {
            rte = transformTableEntry(pstate, rv, isFirstNode, isCreateView);
        }

        rtr = transformItem(pstate, rte, top_rte, top_rti, relnamespace);

        /* add startinfo if needed */
        if (pstate->p_addStartInfo) {
            AddStartWithTargetRelInfo(pstate, n, rte, rtr);
        }

        return (Node*)rtr;
    } else if (IsA(n, RangeSubselect)) {
        /* sub-SELECT is like a plain relation */
        RangeTblRef* rtr = NULL;
        RangeTblEntry* rte = NULL;
        Node *sw_backup = NULL;

        if (pstate->p_addStartInfo) {
            /*
             * In start with case we should back up SubselectStmt for further
             * SW Rewrite.
             * */
            sw_backup = (Node *)copyObject(n);
        }

        rte = transformRangeSubselect(pstate, (RangeSubselect*)n);
        rtr = transformItem(pstate, rte, top_rte, top_rti, relnamespace);

        /* add startinfo if needed */
        if (pstate->p_addStartInfo) {
            AddStartWithTargetRelInfo(pstate, sw_backup, rte, rtr);

            /*
             * (RangeSubselect*)n is mainly related to RTE during whole transform as pointer,
             * so anything fixed on sw_backup could also fix back to (RangeSubselect*)n.
             * */
            ((RangeSubselect*)n)->alias->aliasname = ((RangeSubselect*)sw_backup)->alias->aliasname;
        }

        return (Node*)rtr;
    } else if (IsA(n, RangeFunction)) {
        /* function is like a plain relation */
        RangeTblRef* rtr = NULL;
        RangeTblEntry* rte = NULL;

        rte = transformRangeFunction(pstate, (RangeFunction*)n);
        rtr = transformItem(pstate, rte, top_rte, top_rti, relnamespace);
        return (Node*)rtr;
    } else if (IsA(n, RangeTableSample)) {
        /* TABLESAMPLE clause (wrapping some other valid FROM NODE) */
        RangeTableSample* rts = (RangeTableSample*)n;
        Node* rel = NULL;
        RangeTblRef* rtr = NULL;
        RangeTblEntry* rte = NULL;

        /* Recursively transform the contained relation. */
        rel = transformFromClauseItem(pstate, rts->relation, top_rte, top_rti, NULL, NULL, relnamespace);
        if (unlikely(rel == NULL)) {
            ereport(
                ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("rel should not be NULL")));
        }

        /* Currently, grammar could only return a RangeVar as contained rel */
        Assert(IsA(rel, RangeTblRef));
        rtr = (RangeTblRef*)rel;
        rte = rt_fetch(rtr->rtindex, pstate->p_rtable);

        /* We only support this on plain relations */
        if (rte->relkind != RELKIND_RELATION) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("TABLESAMPLE clause can only be applied to tables."),
                    parser_errposition(pstate, exprLocation(rts->relation))));
        }

        if (REL_COL_ORIENTED != rte->orientation && REL_ROW_ORIENTED != rte->orientation) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    (errmsg("TABLESAMPLE clause only support relation of oriented-row, oriented-column and oriented-inplace."))));
        }

        /* Transform TABLESAMPLE details and attach to the RTE */
        rte->tablesample = transformRangeTableSample(pstate, rts);
        return (Node*)rtr;
    } else if (IsA(n, RangeTimeCapsule)) {
        /* TABLECAPSULE clause (wrapping some other valid FROM NODE) */
        RangeTimeCapsule* rtc = (RangeTimeCapsule *)n;
        Node* rel = NULL;
        RangeTblRef* rtr = NULL;
        RangeTblEntry* rte = NULL;

        /* Recursively transform the contained relation. */
        rel = transformFromClauseItem(pstate, rtc->relation, top_rte, top_rti, NULL, NULL, relnamespace);
        if (unlikely(rel == NULL)) {
            ereport(
                ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Range table with timecapsule clause should not be null")));
        }

        /* Currently, grammar could only return a RangeVar as contained rel */
        Assert(IsA(rel, RangeTblRef));
        rtr = (RangeTblRef*)rel;
        rte = rt_fetch(rtr->rtindex, pstate->p_rtable);

        TvCheckVersionScan(rte);

        /* Transform TABLECAPSULE details and attach to the RTE */
        rte->timecapsule = transformRangeTimeCapsule(pstate, rtc);
        return (Node*)rtr;
    } else if (IsA(n, JoinExpr)) {
        /* A newfangled join expression */
        JoinExpr* j = (JoinExpr*)n;
        RangeTblEntry* l_rte = NULL;
        RangeTblEntry* r_rte = NULL;
        int l_rtindex;
        int r_rtindex;
        List* l_relnamespace = NIL;
        List* r_relnamespace = NIL;
        List* my_relnamespace = NIL;
        List* l_colnames = NIL;
        List* r_colnames = NIL;
        List* res_colnames = NIL;
        List* l_colvars = NIL;
        List* r_colvars = NIL;
        List* res_colvars = NIL;
        bool lateral_ok = false;
        int sv_relnamespace_length, sv_varnamespace_length;
        RangeTblEntry* rte = NULL;
        int k;

        /*
         * Recursively process the left and right subtrees
         * For merge into clause, left arg is alse the target relation, which has been
         * added to the range table. Here, we only build RangeTblRef.
         */
        if (isMergeInto == false) {
            j->larg = transformFromClauseItem(
                pstate, j->larg, &l_rte, &l_rtindex, NULL, NULL, &l_relnamespace);
        } else {
            RangeTblRef* rtr = makeNode(RangeTblRef);
            rtr->rtindex = list_length(pstate->p_rtable);
            j->larg = (Node*)rtr;
            l_rte = pstate->p_target_rangetblentry;
            l_rtindex = rtr->rtindex;
            l_relnamespace = list_make1(makeNamespaceItem(l_rte, false, true));
        }

        /*
         * Make the left-side RTEs available for LATERAL access within the
         * right side, by temporarily adding them to the pstate's namespace
         * lists.  Per SQL:2008, if the join type is not INNER or LEFT then
         * the left-side names must still be exposed, but it's an error to
         * reference them.  (Stupid design, but that's what it says.)  Hence,
         * we always push them into the namespaces, but mark them as not
         * lateral_ok if the jointype is wrong.
         *
         * NB: this coding relies on the fact that list_concat is not
         * destructive to its second argument.
         */
        lateral_ok = (j->jointype == JOIN_INNER || j->jointype == JOIN_LEFT);
        setNamespaceLateralState(l_relnamespace, true, lateral_ok);
        sv_relnamespace_length = list_length(pstate->p_relnamespace);
        pstate->p_relnamespace = list_concat(pstate->p_relnamespace,
                                             l_relnamespace);
        sv_varnamespace_length = list_length(pstate->p_varnamespace);
        pstate->p_varnamespace = lappend(pstate->p_varnamespace,
                                 makeNamespaceItem(l_rte, true, lateral_ok));

        /* And now we can process the RHS */
        j->rarg = transformFromClauseItem(pstate, j->rarg, &r_rte, &r_rtindex, NULL, NULL, &r_relnamespace);

        /* Remove the left-side RTEs from the namespace lists again */
        pstate->p_relnamespace = list_truncate(pstate->p_relnamespace,sv_relnamespace_length);
        pstate->p_varnamespace = list_truncate(pstate->p_varnamespace, sv_varnamespace_length);

        /*
         * Check for conflicting refnames in left and right subtrees. Must do
         * this because higher levels will assume I hand back a self-
         * consistent namespace subtree.
         */
        checkNameSpaceConflicts(pstate, l_relnamespace, r_relnamespace);

        /*
         * Generate combined relation membership info for possible use by
         * transformJoinOnClause below.
         */
        my_relnamespace = list_concat(l_relnamespace, r_relnamespace);

        /*
         * Extract column name and var lists from both subtrees
         *
         * Note: expandRTE returns new lists, safe for me to modify
         */
        expandRTE(l_rte, l_rtindex, 0, -1, false, &l_colnames, &l_colvars);
        expandRTE(r_rte, r_rtindex, 0, -1, false, &r_colnames, &r_colvars);

        if (right_rte != NULL) {
            *right_rte = r_rte;
        }

        if (right_rti != NULL) {
            *right_rti = r_rtindex;
        }

        /*
         * Natural join does not explicitly specify columns; must generate
         * columns to join. Need to run through the list of columns from each
         * table or join result and match up the column names. Use the first
         * table, and check every column in the second table for a match.
         * (We'll check that the matches were unique later on.) The result of
         * this step is a list of column names just like an explicitly-written
         * USING list.
         */
        if (j->isNatural) {
            List* rlist = NIL;
            ListCell* lx = NULL;
            ListCell* rx = NULL;

            /* shouldn't have USING() too */
            Assert(j->usingClause == NIL);

            foreach (lx, l_colnames) {
                char* l_colname = strVal(lfirst(lx));
                Value* m_name = NULL;

                foreach (rx, r_colnames) {
                    char* r_colname = strVal(lfirst(rx));

                    if (strcmp(l_colname, r_colname) == 0) {
                        m_name = makeString(l_colname);
                        break;
                    }
                }

                /* matched a right column? then keep as join column... */
                if (m_name != NULL) {
                    rlist = lappend(rlist, m_name);
                }
            }

            j->usingClause = rlist;
        }

        /*
         * Now transform the join qualifications, if any.
         */
        res_colnames = NIL;
        res_colvars = NIL;

        if (j->usingClause) {
            /*
             * JOIN/USING (or NATURAL JOIN, as transformed above). Transform
             * the list into an explicit ON-condition, and generate a list of
             * merged result columns.
             */
            List* ucols = j->usingClause;
            List* l_usingvars = NIL;
            List* r_usingvars = NIL;
            ListCell* ucol = NULL;

            /* shouldn't have ON() too */
            Assert(j->quals == NULL);

            foreach (ucol, ucols) {
                char* u_colname = strVal(lfirst(ucol));
                ListCell* col = NULL;
                int ndx;
                int l_index = -1;
                int r_index = -1;
                Var *l_colvar = NULL;
                Var *r_colvar = NULL;

                /* Check for USING(foo,foo) */
                foreach (col, res_colnames) {
                    char* res_colname = strVal(lfirst(col));

                    if (strcmp(res_colname, u_colname) == 0) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DUPLICATE_COLUMN),
                                errmsg("column name \"%s\" appears more than once in USING clause", u_colname)));
                    }
                }

                /* Find it in left input */
                ndx = 0;
                foreach (col, l_colnames) {
                    char* l_colname = strVal(lfirst(col));

                    if (strcmp(l_colname, u_colname) == 0) {
                        if (l_index >= 0)
                            ereport(ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                                    errmsg(
                                        "common column name \"%s\" appears more than once in left table", u_colname)));
                        l_index = ndx;
                    }
                    ndx++;
                }
                if (l_index < 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("column \"%s\" specified in USING clause does not exist in left table", u_colname)));
                }

                /* Find it in right input */
                ndx = 0;
                foreach (col, r_colnames) {
                    char* r_colname = strVal(lfirst(col));

                    if (strcmp(r_colname, u_colname) == 0) {
                        if (r_index >= 0) {
                            ereport(ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                                    errmsg(
                                        "common column name \"%s\" appears more than once in right table", u_colname)));
                        }
                        r_index = ndx;
                    }
                    ndx++;
                }
                if (r_index < 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg(
                                "column \"%s\" specified in USING clause does not exist in right table", u_colname)));
                }

                l_colvar = (Var*)list_nth(l_colvars, l_index);
                l_usingvars = lappend(l_usingvars, l_colvar);
                r_colvar = (Var*)list_nth(r_colvars, r_index);
                r_usingvars = lappend(r_usingvars, r_colvar);

                res_colnames = lappend(res_colnames, lfirst(ucol));
                res_colvars = lappend(res_colvars, buildMergedJoinVar(pstate, j->jointype, l_colvar, r_colvar));
            }

            j->quals = transformJoinUsingClause(pstate, l_rte, r_rte, l_usingvars, r_usingvars);
        } else if (j->quals) {
            /* User-written ON-condition; transform it */
            j->quals = transformJoinOnClause(pstate, j, l_rte, r_rte, my_relnamespace);
        } else {
            /* CROSS JOIN: no quals */
        }

        /* Add remaining columns from each side to the output columns */
        extractRemainingColumns(res_colnames, l_colnames, l_colvars, &l_colnames, &l_colvars);
        extractRemainingColumns(res_colnames, r_colnames, r_colvars, &r_colnames, &r_colvars);
        res_colnames = list_concat(res_colnames, l_colnames);
        res_colvars = list_concat(res_colvars, l_colvars);
        res_colnames = list_concat(res_colnames, r_colnames);
        res_colvars = list_concat(res_colvars, r_colvars);

        /*
         * Check alias (AS clause), if any.
         */
        if (j->alias) {
            if (j->alias->colnames != NIL) {
                if (list_length(j->alias->colnames) > list_length(res_colnames))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("column alias list for \"%s\" has too many entries", j->alias->aliasname)));
            }
        }

        /*
         * Now build an RTE for the result of the join
         */
        rte = addRangeTableEntryForJoin(pstate, res_colnames, j->jointype, res_colvars, j->alias, true);

        /* assume new rte is at end */
        j->rtindex = list_length(pstate->p_rtable);
        Assert(rte == rt_fetch(j->rtindex, pstate->p_rtable));

        *top_rte = rte;
        *top_rti = j->rtindex;

        /* make a matching link to the JoinExpr for later use */
        for (k = list_length(pstate->p_joinexprs) + 1; k < j->rtindex; k++) {
            pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
        }
        pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
        Assert(list_length(pstate->p_joinexprs) == j->rtindex);

        /*
         * Prepare returned namespace list.  If the JOIN has an alias then it
         * hides the contained RTEs as far as the relnamespace goes;
         * otherwise, put the contained RTEs and *not* the JOIN into
         * relnamespace.
         */
        if (j->alias) {
            *relnamespace = list_make1(makeNamespaceItem(rte, false, true));
        } else
            *relnamespace = my_relnamespace;

        return (Node*)j;
    } else
        ereport(
            ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(n))));
    return NULL; /* can't get here, keep compiler quiet */
}

/*
 * makeNamespaceItem -
 *   Convenience subroutine to construct a ParseNamespaceItem.
 */
ParseNamespaceItem *
makeNamespaceItem(RangeTblEntry *rte, bool lateral_only, bool lateral_ok)
{
   ParseNamespaceItem *nsitem;

   nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
   nsitem->p_rte = rte;
   nsitem->p_lateral_only = lateral_only;
   nsitem->p_lateral_ok = lateral_ok;
   return nsitem;
}

/*
 * setNamespaceLateralState -
 *   Convenience subroutine to update LATERAL flags in a namespace list.
 */
static void
setNamespaceLateralState(List *l_namespace, bool lateral_only, bool lateral_ok)
{
   ListCell   *lc = NULL;

   foreach(lc, l_namespace)
   {
       ParseNamespaceItem *nsitem = (ParseNamespaceItem *) lfirst(lc);

       nsitem->p_lateral_only = lateral_only;
       nsitem->p_lateral_ok = lateral_ok;
   }
}

/*
 * buildMergedJoinVar -
 *	  generate a suitable replacement expression for a merged join column
 */
static Node* buildMergedJoinVar(ParseState* pstate, JoinType jointype, Var* l_colvar, Var* r_colvar)
{
    Oid outcoltype;
    int32 outcoltypmod;
    Node* l_node = NULL;
    Node* r_node = NULL;
    Node* res_node = NULL;

    /*
     * Choose output type if input types are dissimilar.
     */
    outcoltype = l_colvar->vartype;
    outcoltypmod = l_colvar->vartypmod;
    if (outcoltype != r_colvar->vartype) {
        outcoltype = select_common_type(pstate, list_make2(l_colvar, r_colvar), "JOIN/USING", NULL);
        outcoltypmod = -1; /* ie, unknown */
    } else if (outcoltypmod != r_colvar->vartypmod) {
        /* same type, but not same typmod */
        outcoltypmod = -1; /* ie, unknown */
    }

    /*
     * Insert coercion functions if needed.  Note that a difference in typmod
     * can only happen if input has typmod but outcoltypmod is -1. In that
     * case we insert a RelabelType to clearly mark that result's typmod is
     * not same as input.  We never need coerce_type_typmod.
     */
    if (l_colvar->vartype != outcoltype) {
        l_node = coerce_type(pstate,
            (Node*)l_colvar,
            l_colvar->vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1);
    } else if (l_colvar->vartypmod != outcoltypmod) {
        l_node = (Node*)makeRelabelType((Expr*)l_colvar,
            outcoltype,
            outcoltypmod,
            InvalidOid, /* fixed below */
            COERCE_IMPLICIT_CAST);
    } else {
        l_node = (Node*)l_colvar;
    }
    if (r_colvar->vartype != outcoltype) {
        r_node = coerce_type(pstate,
            (Node*)r_colvar,
            r_colvar->vartype,
            outcoltype,
            outcoltypmod,
            COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1);
    } else if (r_colvar->vartypmod != outcoltypmod) {
        r_node = (Node*)makeRelabelType((Expr*)r_colvar,
            outcoltype,
            outcoltypmod,
            InvalidOid, /* fixed below */
            COERCE_IMPLICIT_CAST);
    } else {
        r_node = (Node*)r_colvar;
    }
    /*
     * Choose what to emit
     */
    switch (jointype) {
        case JOIN_INNER:

            /*
             * We can use either var; prefer non-coerced one if available.
             */
            if (IsA(l_node, Var)) {
                res_node = l_node;
            } else if (IsA(r_node, Var)) {
                res_node = r_node;
            } else {
                res_node = l_node;
            }
            break;
        case JOIN_LEFT:
        case JOIN_LEFT_ANTI_FULL:
            /* Always use left var */
            res_node = l_node;
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI_FULL:
            /* Always use right var */
            res_node = r_node;
            break;
        case JOIN_FULL: {
            /*
             * Here we must build a COALESCE expression to ensure that the
             * join output is non-null if either input is.
             */
            CoalesceExpr* c = makeNode(CoalesceExpr);

            c->coalescetype = outcoltype;
            /* coalescecollid will get set below */
            c->args = list_make2(l_node, r_node);
            c->location = -1;
            res_node = (Node*)c;
            break;
        }
        default:
            ereport(
                ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized join type: %d", (int)jointype)));
            res_node = NULL; /* keep compiler quiet */
            break;
    }

    /*
     * Apply assign_expr_collations to fix up the collation info in the
     * coercion and CoalesceExpr nodes, if we made any.  This must be done now
     * so that the join node's alias vars show correct collation info.
     */
    assign_expr_collations(pstate, res_node);

    return res_node;
}

/*
 * transformWhereClause -
 *	  Transform the qualification and make sure it is of type boolean.
 *	  Used for WHERE and allied clauses.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
Node* transformWhereClause(ParseState* pstate, Node* clause, const char* constructName)
{
    Node* qual = NULL;

    if (clause == NULL) {
        return NULL;
    }

    qual = transformExpr(pstate, clause);

    qual = coerce_to_boolean(pstate, qual, constructName);

    return qual;
}

/*
 * transformLimitClause -
 *	  Transform the expression and make sure it is of type bigint.
 *	  Used for LIMIT and allied clauses.
 *
 * Note: as of Postgres 8.2, LIMIT expressions are expected to yield int8,
 * rather than int4 as before.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
Node* transformLimitClause(ParseState* pstate, Node* clause, const char* constructName)
{
    Node* qual = NULL;

    if (clause == NULL) {
        return NULL;
    }
    qual = transformExpr(pstate, clause);

    qual = coerce_to_specific_type(pstate, qual, INT8OID, constructName);

    /* LIMIT can't refer to any vars or aggregates of the current query */
    checkExprIsVarFree(pstate, qual, constructName);

    return qual;
}

/*
 * checkExprIsVarFree
 *		Check that given expr has no Vars of the current query level
 *		(and no aggregates or window functions, either).
 *
 * This is used to check expressions that have to have a consistent value
 * across all rows of the query, such as a LIMIT.  Arguably it should reject
 * volatile functions, too, but we don't do that --- whatever value the
 * function gives on first execution is what you get.
 *
 * constructName does not affect the semantics, but is used in error messages
 */
static void checkExprIsVarFree(ParseState* pstate, Node* n, const char* constructName)
{
    if (contain_vars_of_level(n, 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                /* translator: %s is name of a SQL construct, eg LIMIT */
                errmsg("argument of %s must not contain variables", constructName),
                parser_errposition(pstate, locate_var_of_level(n, 0))));
    }
    if (pstate->p_hasAggs && checkExprHasAggs(n)) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                /* translator: %s is name of a SQL construct, eg LIMIT */
                errmsg("argument of %s must not contain aggregate functions", constructName),
                parser_errposition(pstate, locate_agg_of_level(n, 0))));
    }
    if (pstate->p_hasWindowFuncs && checkExprHasWindowFuncs(n)) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                /* translator: %s is name of a SQL construct, eg LIMIT */
                errmsg("argument of %s must not contain window functions", constructName),
                parser_errposition(pstate, locate_windowfunc(n))));
    }
}

/*
 *	findTargetlistEntrySQL92 -
 *	  Returns the targetlist entry matching the given (untransformed) node.
 *	  If no matching entry exists, one is created and appended to the target
 *	  list as a "resjunk" node.
 *
 * This function supports the old SQL92 ORDER BY interpretation, where the
 * expression is an output column name or number.  If we fail to find a
 * match of that sort, we fall through to the SQL99 rules.	For historical
 * reasons, openGauss also allows this interpretation for GROUP BY, though
 * the standard never did.	However, for GROUP BY we prefer a SQL99 match.
 * This function is *not* used for WINDOW definitions.
 *
 * node		the ORDER BY, GROUP BY, or DISTINCT ON expression to be matched
 * tlist	the target list (passed by reference so we can append to it)
 * clause	identifies clause type being processed
 */
static TargetEntry* findTargetlistEntrySQL92(ParseState* pstate, Node* node, List** tlist, int clause)
{
    ListCell* tl = NULL;

    /* ----------
     * Handle two special cases as mandated by the SQL92 spec:
     *
     * 1. Bare ColumnName (no qualifier or subscripts)
     *	  For a bare identifier, we search for a matching column name
     *	  in the existing target list.	Multiple matches are an error
     *	  unless they refer to identical values; for example,
     *	  we allow	SELECT a, a FROM table ORDER BY a
     *	  but not	SELECT a AS b, b FROM table ORDER BY b
     *	  If no match is found, we fall through and treat the identifier
     *	  as an expression.
     *	  For GROUP BY, it is incorrect to match the grouping item against
     *	  targetlist entries: according to SQL92, an identifier in GROUP BY
     *	  is a reference to a column name exposed by FROM, not to a target
     *	  list column.	However, many implementations (including pre-7.0
     *	  openGauss) accept this anyway.  So for GROUP BY, we look first
     *	  to see if the identifier matches any FROM column name, and only
     *	  try for a targetlist name if it doesn't.  This ensures that we
     *	  adhere to the spec in the case where the name could be both.
     *	  DISTINCT ON isn't in the standard, so we can do what we like there;
     *	  we choose to make it work like ORDER BY, on the rather flimsy
     *	  grounds that ordinary DISTINCT works on targetlist entries.
     *
     * 2. IntegerConstant
     *	  This means to use the n'th item in the existing target list.
     *	  Note that it would make no sense to order/group/distinct by an
     *	  actual constant, so this does not create a conflict with SQL99.
     *	  GROUP BY column-number is not allowed by SQL92, but since
     *	  the standard has no other behavior defined for this syntax,
     *	  we may as well accept this common extension.
     *
     * Note that pre-existing resjunk targets must not be used in either case,
     * since the user didn't write them in his SELECT list.
     *
     * If neither special case applies, fall through to treat the item as
     * an expression per SQL99.
     * ----------
     */
    if (IsA(node, ColumnRef) && list_length(((ColumnRef*)node)->fields) == 1 &&
        IsA(linitial(((ColumnRef*)node)->fields), String)) {
        char* name = strVal(linitial(((ColumnRef*)node)->fields));
        int location = ((ColumnRef*)node)->location;

        if (clause == GROUP_CLAUSE) {
            /*
             * In GROUP BY, we must prefer a match against a FROM-clause
             * column to one against the targetlist.  Look to see if there is
             * a matching column.  If so, fall through to use SQL99 rules.
             * NOTE: if name could refer ambiguously to more than one column
             * name exposed by FROM, colNameToVar will ereport(ERROR). That's
             * just what we want here.
             *
             * Small tweak for 7.4.3: ignore matches in upper query levels.
             * This effectively changes the search order for bare names to (1)
             * local FROM variables, (2) local targetlist aliases, (3) outer
             * FROM variables, whereas before it was (1) (3) (2). SQL92 and
             * SQL99 do not allow GROUPing BY an outer reference, so this
             * breaks no cases that are legal per spec, and it seems a more
             * self-consistent behavior.
             */
            if (colNameToVar(pstate, name, true, location) != NULL)
                name = NULL;
        }

        if (name != NULL) {
            TargetEntry* target_result = NULL;

            foreach (tl, *tlist) {
                TargetEntry* tle = (TargetEntry*)lfirst(tl);

                if (!tle->resjunk && strcmp(tle->resname, name) == 0) {
                    if (target_result != NULL) {
                        if (!equal(target_result->expr, tle->expr)) {
                            ereport(ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),

                                    /* ------
                                      translator: first %s is name of a SQL construct, eg ORDER BY */
                                    errmsg("%s \"%s\" is ambiguous", clauseText[clause], name),
                                    parser_errposition(pstate, location)));
                        }
                    } else {
                        target_result = tle; /* Stay in loop to check for ambiguity */
                    }
                }
            }
            if (target_result != NULL)
                return target_result; /* return the first match */
        }
    }
    if (IsA(node, A_Const)) {
        Value* val = &((A_Const*)node)->val;
        int location = ((A_Const*)node)->location;
        int targetlist_pos = 0;
        int target_pos;

        if (!IsA(val, Integer)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    /* translator: %s is name of a SQL construct, eg ORDER BY */
                    errmsg("non-integer constant in %s", clauseText[clause]),
                    parser_errposition(pstate, location)));
        }

        target_pos = intVal(val);
        foreach (tl, *tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(tl);

            if (!tle->resjunk) {
                if (++targetlist_pos == target_pos) {
                    return tle; /* return the unique match */
                }
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                /* translator: %s is name of a SQL construct, eg ORDER BY */
                errmsg("%s position %d is not in select list", clauseText[clause], target_pos),
                parser_errposition(pstate, location)));
    }

    /*
     * Otherwise, we have an expression, so process it per SQL99 rules.
     */
    return findTargetlistEntrySQL99(pstate, node, tlist);
}

/*
 *	findTargetlistEntrySQL99 -
 *	  Returns the targetlist entry matching the given (untransformed) node.
 *	  If no matching entry exists, one is created and appended to the target
 *	  list as a "resjunk" node.
 *
 * This function supports the SQL99 interpretation, wherein the expression
 * is just an ordinary expression referencing input column names.
 *
 * node		the ORDER BY, GROUP BY, etc expression to be matched
 * tlist	the target list (passed by reference so we can append to it)
 */
static TargetEntry* findTargetlistEntrySQL99(ParseState* pstate, Node* node, List** tlist)
{
    TargetEntry* target_result = NULL;
    ListCell* tl = NULL;
    Node* expr = NULL;

    /*
     * Convert the untransformed node to a transformed expression, and search
     * for a match in the tlist.  NOTE: it doesn't really matter whether there
     * is more than one match.	Also, we are willing to match an existing
     * resjunk target here, though the SQL92 cases above must ignore resjunk
     * targets.
     */
    expr = transformExpr(pstate, node);

    foreach (tl, *tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);
        Node* texpr = NULL;

        /*
         * Ignore any implicit cast on the existing tlist expression.
         *
         * This essentially allows the ORDER/GROUP/etc item to adopt the same
         * datatype previously selected for a textually-equivalent tlist item.
         * There can't be any implicit cast at top level in an ordinary SELECT
         * tlist at this stage, but the case does arise with ORDER BY in an
         * aggregate function.
         */
        texpr = strip_implicit_coercions((Node*)tle->expr);
        if (equal(expr, texpr)) {
            return tle;
        }
    }

    /*
     * If no matches, construct a new target entry which is appended to the
     * end of the target list.	This target is given resjunk = TRUE so that it
     * will not be projected into the final tuple.
     */
    target_result = transformTargetEntry(pstate, node, expr, NULL, true);

    *tlist = lappend(*tlist, target_result);

    return target_result;
}

/* -------------------------------------------------------------------------
 * Flatten out parenthesized sublists in grouping lists, and some cases
 * of nested grouping sets.
 *
 * Inside a grouping set (ROLLUP, CUBE, or GROUPING SETS), we expect the
 * content to be nested no more than 2 deep: i.e. ROLLUP((a,b),(c,d)) is
 * ok, but ROLLUP((a,(b,c)),d) is flattened to ((a,b,c),d), which we then
 * (later) normalize to ((a,b,c),(d)).
 *
 * CUBE or ROLLUP can be nested inside GROUPING SETS (but not the reverse),
 * and we leave that alone if we find it. But if we see GROUPING SETS inside
 * GROUPING SETS, we can flatten and normalize as follows:
 *	 GROUPING SETS (a, (b,c), GROUPING SETS ((c,d),(e)), (f,g))
 * becomes
 *	 GROUPING SETS ((a), (b,c), (c,d), (e), (f,g))
 *
 * This is per the spec's syntax transformations, but these are the only such
 * transformations we do in parse analysis, so that queries retain the
 * originally specified grouping set syntax for CUBE and ROLLUP as much as
 * possible when deparsed. (Full expansion of the result into a list of
 * grouping sets is left to the planner.)
 *
 * When we're done, the resulting list should contain only these possible
 * elements:
 *	 - an expression
 *	 - a CUBE or ROLLUP with a list of expressions nested 2 deep
 *	 - a GROUPING SET containing any of:
 *		- expression lists
 *		- empty grouping sets
 *		- CUBE or ROLLUP nodes with lists nested 2 deep
 * The return is a new list, but doesn't deep-copy the old nodes except for
 * GroupingSet nodes.
 *
 * As a side effect, flag whether the list has any GroupingSet nodes.
 * -------------------------------------------------------------------------
 */
static Node* flatten_grouping_sets(Node* expr, bool toplevel, bool* hasGroupingSets)
{
    /* just in case of pathological input */
    check_stack_depth();

    if (expr == (Node*)NIL)
        return (Node*)NIL;

    switch (expr->type) {
        case T_RowExpr: {
            RowExpr* r = (RowExpr*)expr;

            if (r->row_format == COERCE_IMPLICIT_CAST)
                return flatten_grouping_sets((Node*)r->args, false, NULL);
        } break;
        case T_GroupingSet: {
            GroupingSet* gset = (GroupingSet*)expr;
            ListCell* l2 = NULL;
            List* result_set = NIL;

            if (hasGroupingSets != NULL) {
                *hasGroupingSets = true;
            }

            /*
             * at the top level, we skip over all empty grouping sets; the
             * caller can supply the canonical GROUP BY () if nothing is
             * left.
             */
            if (toplevel && gset->kind == GROUPING_SET_EMPTY)
                return (Node*)NIL;

            foreach (l2, gset->content) {
                Node* n1 = (Node*)lfirst(l2);
                Node* n2 = flatten_grouping_sets(n1, false, NULL);

                if (IsA(n1, GroupingSet) && ((GroupingSet*)n1)->kind == GROUPING_SET_SETS) {
                    result_set = list_concat(result_set, (List*)n2);
                } else {
                    result_set = lappend(result_set, n2);
                }
            }

            /*
             * At top level, keep the grouping set node; but if we're in a
             * nested grouping set, then we need to concat the flattened
             * result into the outer list if it's simply nested.
             */
            if (toplevel || (gset->kind != GROUPING_SET_SETS)) {
                return (Node*)makeGroupingSet(gset->kind, result_set, gset->location);
            } else {
                return (Node*)result_set;
            }
        }
        case T_List: {
            List* result = NIL;
            ListCell* l = NULL;

            foreach (l, (List*)expr) {
                Node* n = flatten_grouping_sets((Node*)lfirst(l), toplevel, hasGroupingSets);

                if (n != (Node*)NIL) {
                    if (IsA(n, List)) {
                        result = list_concat(result, (List*)n);
                    } else {
                        result = lappend(result, n);
                    }
                }
            }

            return (Node*)result;
        }
        default:
            break;
    }

    return expr;
}

/*
 * Transform a single expression within a GROUP BY clause or grouping set.
 *
 * The expression is added to the targetlist if not already present, and to the
 * flatresult list (which will become the groupClause) if not already present
 * there.  The sortClause is consulted for operator and sort order hints.
 *
 * Returns the ressortgroupref of the expression.
 *
 * flatresult	reference to flat list of SortGroupClause nodes
 * seen_local	bitmapset of sortgrouprefs already seen at the local level
 * pstate		ParseState
 * gexpr		node to transform
 * targetlist	reference to TargetEntry list
 * sortClause	ORDER BY clause (SortGroupClause nodes)
 * exprKind		expression kind
 * useSQL99		SQL99 rather than SQL92 syntax
 * toplevel		false if within any grouping set
 */
static Index transformGroupClauseExpr(List** flatresult, Bitmapset* seen_local, ParseState* pstate, Node* gexpr,
    List** targetlist, List* sortClause, bool useSQL99, bool toplevel)
{
    TargetEntry* tle = NULL;
    bool found = false;

    if (useSQL99) {
        tle = findTargetlistEntrySQL99(pstate, gexpr, targetlist);
    } else {
        tle = findTargetlistEntrySQL92(pstate, gexpr, targetlist, GROUP_CLAUSE);
    }
    
    if (tle->ressortgroupref > 0) {
        ListCell* sl = NULL;

        /*
         * Eliminate duplicates (GROUP BY x, x) but only at local level.
         * (Duplicates in grouping sets can affect the number of returned
         * rows, so can't be dropped indiscriminately.)
         *
         * Since we don't care about anything except the sortgroupref, we can
         * use a bitmapset rather than scanning lists.
         */
        if (bms_is_member(tle->ressortgroupref, seen_local)) {
            return 0;
        }
        /*
         * If we're already in the flat clause list, we don't need to consider
         * adding ourselves again.
         */
        found = targetIsInSortList(tle, InvalidOid, *flatresult);
        if (found) {
            return tle->ressortgroupref;
        }
        /*
         * If the GROUP BY tlist entry also appears in ORDER BY, copy operator
         * info from the (first) matching ORDER BY item.  This means that if
         * you write something like "GROUP BY foo ORDER BY foo USING <<<", the
         * GROUP BY operation silently takes on the equality semantics implied
         * by the ORDER BY.  There are two reasons to do this: it improves the
         * odds that we can implement both GROUP BY and ORDER BY with a single
         * sort step, and it allows the user to choose the equality semantics
         * used by GROUP BY, should she be working with a datatype that has
         * more than one equality operator.
         *
         * If we're in a grouping set, though, we force our requested ordering
         * to be NULLS LAST, because if we have any hope of using a sorted agg
         * for the job, we're going to be tacking on generated NULL values
         * after the corresponding groups. If the user demands nulls first,
         * another sort step is going to be inevitable, but that's the
         * planner's problem.
         */
        foreach (sl, sortClause) {
            SortGroupClause* sc = (SortGroupClause*)lfirst(sl);

            if (sc->tleSortGroupRef == tle->ressortgroupref) {
                SortGroupClause* grpc = (SortGroupClause*)copyObject(sc);

                if (!toplevel) {
                    grpc->nulls_first = false;
                }
                *flatresult = lappend(*flatresult, grpc);
                found = true;
                break;
            }
        }
    }

    /*
     * If no match in ORDER BY, just add it to the result using default
     * sort/group semantics.
     */
    if (!found) {
        *flatresult = addTargetToGroupList(pstate, tle, *flatresult, *targetlist, exprLocation(gexpr), true);
    }
    /*
     * _something_ must have assigned us a sortgroupref by now...
     */
    return tle->ressortgroupref;
}

/*
 * Transform a list of expressions within a GROUP BY clause or grouping set.
 *
 * The list of expressions belongs to a single clause within which duplicates
 * can be safely eliminated.
 *
 * Returns an integer list of ressortgroupref values.
 *
 * flatresult	reference to flat list of SortGroupClause nodes
 * pstate		ParseState
 * list			nodes to transform
 * targetlist	reference to TargetEntry list
 * sortClause	ORDER BY clause (SortGroupClause nodes)
 * exprKind		expression kind
 * useSQL99		SQL99 rather than SQL92 syntax
 * toplevel		false if within any grouping set
 */
static List* transformGroupClauseList(List** flatresult, ParseState* pstate, List* list, List** targetlist,
    List* sortClause, bool useSQL99, bool toplevel)
{
    Bitmapset* seen_local = NULL;
    List* result = NIL;
    ListCell* gl = NULL;

    foreach (gl, list) {
        Node* gexpr = (Node*)lfirst(gl);

        Index ref =
            transformGroupClauseExpr(flatresult, seen_local, pstate, gexpr, targetlist, sortClause, useSQL99, toplevel);
        if (ref > 0) {
            seen_local = bms_add_member(seen_local, ref);
            result = lappend_int(result, ref);
        }
    }

    return result;
}

/*
 * Transform a grouping set and (recursively) its content.
 *
 * The grouping set might be a GROUPING SETS node with other grouping sets
 * inside it, but SETS within SETS have already been flattened out before
 * reaching here.
 *
 * Returns the transformed node, which now contains SIMPLE nodes with lists
 * of ressortgrouprefs rather than expressions.
 *
 * flatresult	reference to flat list of SortGroupClause nodes
 * pstate		ParseState
 * gset			grouping set to transform
 * targetlist	reference to TargetEntry list
 * sortClause	ORDER BY clause (SortGroupClause nodes)
 * exprKind		expression kind
 * useSQL99		SQL99 rather than SQL92 syntax
 * toplevel		false if within any grouping set
 */
static Node* transformGroupingSet(List** flatresult, ParseState* pstate, GroupingSet* gset, List** targetlist,
    List* sortClause, bool useSQL99, bool toplevel)
{
    ListCell* gl = NULL;
    List* content = NIL;

    AssertEreport(toplevel || gset->kind != GROUPING_SET_SETS, MOD_OPT, "");

    foreach (gl, gset->content) {
        Node* n = (Node*)lfirst(gl);

        if (IsA(n, List)) {
            List* l = transformGroupClauseList(flatresult, pstate, (List*)n, targetlist, sortClause, useSQL99, false);

            content = lappend(content, makeGroupingSet(GROUPING_SET_SIMPLE, l, exprLocation(n)));
        } else if (IsA(n, GroupingSet)) {
            GroupingSet* gset2 = (GroupingSet*)lfirst(gl);

            content = lappend(
                content, transformGroupingSet(flatresult, pstate, gset2, targetlist, sortClause, useSQL99, false));
        } else {
            Index ref = transformGroupClauseExpr(flatresult, NULL, pstate, n, targetlist, sortClause, useSQL99, false);

            content = lappend(content, makeGroupingSet(GROUPING_SET_SIMPLE, list_make1_int(ref), exprLocation(n)));
        }
    }

    /* Arbitrarily cap the size of CUBE, which has exponential growth */
    if (gset->kind == GROUPING_SET_CUBE) {
        if (list_length(content) > 12) {
            ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_COLUMNS),
                    errmsg("CUBE is limited to 12 elements"),
                    parser_errposition(pstate, gset->location)));
        }
    }

    return (Node*)makeGroupingSet(gset->kind, content, gset->location);
}

/*
 * transformGroupClause -
 *	  transform a GROUP BY clause
 *
 * GROUP BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 * This is also used for window PARTITION BY clauses (which act almost the
 * same, but are always interpreted per SQL99 rules).
 *
 * Grouping sets make this a lot more complex than it was. Our goal here is
 * twofold: we make a flat list of SortGroupClause nodes referencing each
 * distinct expression used for grouping, with those expressions added to the
 * targetlist if needed. At the same time, we build the groupingSets tree,
 * which stores only ressortgrouprefs as integer lists inside GroupingSet nodes
 * (possibly nested, but limited in depth: a GROUPING_SET_SETS node can contain
 * nested SIMPLE, CUBE or ROLLUP nodes, but not more sets - we flatten that
 * out; while CUBE and ROLLUP can contain only SIMPLE nodes).
 *
 * We skip much of the hard work if there are no grouping sets.
 *
 * One subtlety is that the groupClause list can end up empty while the
 * groupingSets list is not; this happens if there are only empty grouping
 * sets, or an explicit GROUP BY (). This has the same effect as specifying
 * aggregates or a HAVING clause with no GROUP BY; the output is one row per
 * grouping set even if the input is empty.
 *
 * Returns the transformed (flat) groupClause.
 *
 * pstate		ParseState
 * grouplist	clause to transform
 * groupingSets reference to list to contain the grouping set tree
 * targetlist	reference to TargetEntry list
 * sortClause	ORDER BY clause (SortGroupClause nodes)
 * exprKind		expression kind
 * useSQL99		SQL99 rather than SQL92 syntax
 */
List* transformGroupClause(
    ParseState* pstate, List* grouplist, List** groupingSets, List** targetlist, List* sortClause, bool useSQL99)
{
    List* result = NIL;
    List* flat_grouplist = NIL;
    List* gsets = NIL;
    ListCell* gl = NULL;
    bool hasGroupingSets = false;
    Bitmapset* seen_local = NULL;

    /*
     * Recursively flatten implicit RowExprs. (Technically this is only needed
     * for GROUP BY, per the syntax rules for grouping sets, but we do it
     * anyway.)
     */
    flat_grouplist = (List*)flatten_grouping_sets((Node*)grouplist, true, &hasGroupingSets);

    /*
     * If the list is now empty, but hasGroupingSets is true, it's because we
     * elided redundant empty grouping sets. Restore a single empty grouping
     * set to leave a canonical form: GROUP BY ()
     */
    if (flat_grouplist == NIL && hasGroupingSets) {
        flat_grouplist = list_make1(makeGroupingSet(GROUPING_SET_EMPTY, NIL, exprLocation((Node*)grouplist)));
    }

    foreach (gl, flat_grouplist) {
        Node* gexpr = (Node*)lfirst(gl);

        if (IsA(gexpr, GroupingSet)) {
            GroupingSet* gset = (GroupingSet*)gexpr;

            switch (gset->kind) {
                case GROUPING_SET_EMPTY:
                    gsets = lappend(gsets, gset);
                    break;
                case GROUPING_SET_SIMPLE:
                    /* can't happen */
                    Assert(false);
                    break;
                case GROUPING_SET_SETS:
                case GROUPING_SET_CUBE:
                case GROUPING_SET_ROLLUP:
                    gsets = lappend(
                        gsets, transformGroupingSet(&result, pstate, gset, targetlist, sortClause, useSQL99, true));
                    break;
                default:
                    break;
            }
        } else {
            Index ref =
                transformGroupClauseExpr(&result, seen_local, pstate, gexpr, targetlist, sortClause, useSQL99, true);
            if (ref > 0) {
                seen_local = bms_add_member(seen_local, ref);
                if (hasGroupingSets) {
                    gsets =
                        lappend(gsets, makeGroupingSet(GROUPING_SET_SIMPLE, list_make1_int(ref), exprLocation(gexpr)));
                }
            }
        }
    }

    /* parser should prevent this */
    AssertEreport(gsets == NIL || groupingSets != NULL, MOD_OPT, "");

    if (groupingSets != NULL) {
        *groupingSets = gsets;
    }
    return result;
}
/*
 * transformSortClause -
 *	  transform an ORDER BY clause
 *
 * ORDER BY items will be added to the targetlist (as resjunk columns)
 * if not already present, so the targetlist must be passed by reference.
 *
 * This is also used for window and aggregate ORDER BY clauses (which act
 * almost the same, but are always interpreted per SQL99 rules).
 */
List* transformSortClause(ParseState* pstate, List* orderlist, List** targetlist, bool resolveUnknown, bool useSQL99)
{
    List* sortlist = NIL;
    ListCell* olitem = NULL;

    foreach (olitem, orderlist) {
        SortBy* sortby = (SortBy*)lfirst(olitem);
        TargetEntry* tle = NULL;

        if (useSQL99) {
            tle = findTargetlistEntrySQL99(pstate, sortby->node, targetlist);
        } else {
            tle = findTargetlistEntrySQL92(pstate, sortby->node, targetlist, ORDER_CLAUSE);
        }
        sortlist = addTargetToSortList(pstate, tle, sortlist, *targetlist, sortby, resolveUnknown);
    }

    return sortlist;
}

/*
 * transformWindowDefinitions -
 *		transform window definitions (WindowDef to WindowClause)
 */
List* transformWindowDefinitions(ParseState* pstate, List* windowdefs, List** targetlist)
{
    List* result = NIL;
    Index winref = 0;
    ListCell* lc = NULL;

    foreach (lc, windowdefs) {
        WindowDef* windef = (WindowDef*)lfirst(lc);
        WindowClause* refwc = NULL;
        List* partitionClause = NIL;
        List* orderClause = NIL;
        WindowClause* wc = NULL;

        winref++;

        /*
         * Check for duplicate window names.
         */
        if (windef->name && findWindowClause(result, windef->name) != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                    errmsg("window \"%s\" is already defined", windef->name),
                    parser_errposition(pstate, windef->location)));
        }
        /*
         * If it references a previous window, look that up.
         */
        if (windef->refname) {
            refwc = findWindowClause(result, windef->refname);
            if (refwc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("window \"%s\" does not exist", windef->refname),
                        parser_errposition(pstate, windef->location)));
            }
        }

        /*
         * Transform PARTITION and ORDER specs, if any.  These are treated
         * almost exactly like top-level GROUP BY and ORDER BY clauses,
         * including the special handling of nondefault operator semantics.
         */
        orderClause = transformSortClause(
            pstate, windef->orderClause, targetlist, true /* fix unknowns */, true /* force SQL99 rules */);
        partitionClause = transformGroupClause(
            pstate, windef->partitionClause, NULL, targetlist, orderClause, true /* force SQL99 rules */);

        /*
         * And prepare the new WindowClause.
         */
        wc = makeNode(WindowClause);
        wc->name = windef->name;
        wc->refname = windef->refname;

        /*
         * Per spec, a windowdef that references a previous one copies the
         * previous partition clause (and mustn't specify its own).  It can
         * specify its own ordering clause. but only if the previous one had
         * none.  It always specifies its own frame clause, and the previous
         * one must not have a frame clause.  (Yeah, it's bizarre that each of
         * these cases works differently, but SQL:2008 says so; see 7.11
         * <window clause> syntax rule 10 and general rule 1.)
         */
        if (refwc != NULL) {
            if (partitionClause != NIL) {
                ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("cannot override PARTITION BY clause of window \"%s\"", windef->refname),
                        parser_errposition(pstate, windef->location)));
            }
            wc->partitionClause = (List*)copyObject(refwc->partitionClause);
        } else {
            wc->partitionClause = partitionClause;
        }

        if (refwc != NULL) {
            if (orderClause != NIL && refwc->orderClause != NIL)
                ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("cannot override ORDER BY clause of window \"%s\"", windef->refname),
                        parser_errposition(pstate, windef->location)));
            if (orderClause != NIL) {
                wc->orderClause = orderClause;
                wc->copiedOrder = false;
            } else {
                wc->orderClause = (List*)copyObject(refwc->orderClause);
                wc->copiedOrder = true;
            }
        } else {
            wc->orderClause = orderClause;
            wc->copiedOrder = false;
        }
        if (refwc != NULL && refwc->frameOptions != FRAMEOPTION_DEFAULTS) {
            ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                    errmsg("cannot override frame clause of window \"%s\"", windef->refname),
                    parser_errposition(pstate, windef->location)));
        }
        wc->frameOptions = windef->frameOptions;
        /* Process frame offset expressions */
        wc->startOffset = transformFrameOffset(pstate, wc->frameOptions, windef->startOffset);
        wc->endOffset = transformFrameOffset(pstate, wc->frameOptions, windef->endOffset);
        wc->winref = winref;

        result = lappend(result, wc);
    }

    return result;
}

/*
 * transformDistinctClause -
 *	  transform a DISTINCT clause
 *
 * Since we may need to add items to the query's targetlist, that list
 * is passed by reference.
 *
 * As with GROUP BY, we absorb the sorting semantics of ORDER BY as much as
 * possible into the distinctClause.  This avoids a possible need to re-sort,
 * and allows the user to choose the equality semantics used by DISTINCT,
 * should she be working with a datatype that has more than one equality
 * operator.
 *
 * is_agg is true if we are transforming an aggregate(DISTINCT ...)
 * function call.  This does not affect any behavior, only the phrasing
 * of error messages.
 */
List* transformDistinctClause(ParseState* pstate, List** targetlist, List* sortClause, bool is_agg)
{
    List* result = NIL;
    ListCell* slitem = NULL;
    ListCell* tlitem = NULL;

    /*
     * The distinctClause should consist of all ORDER BY items followed by all
     * other non-resjunk targetlist items.	There must not be any resjunk
     * ORDER BY items --- that would imply that we are sorting by a value that
     * isn't necessarily unique within a DISTINCT group, so the results
     * wouldn't be well-defined.  This construction ensures we follow the rule
     * that sortClause and distinctClause match; in fact the sortClause will
     * always be a prefix of distinctClause.
     *
     * Note a corner case: the same TLE could be in the ORDER BY list multiple
     * times with different sortops.  We have to include it in the
     * distinctClause the same way to preserve the prefix property. The net
     * effect will be that the TLE value will be made unique according to both
     * sortops.
     */
    foreach (slitem, sortClause) {
        SortGroupClause* scl = (SortGroupClause*)lfirst(slitem);
        TargetEntry* tle = get_sortgroupclause_tle(scl, *targetlist);

        if (tle != NULL && tle->resjunk) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                    is_agg ? errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
                            : errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
                    parser_errposition(pstate, exprLocation((Node*)tle->expr))));
        }
        result = lappend(result, copyObject(scl));
    }

    /*
     * Now add any remaining non-resjunk tlist items, using default sort/group
     * semantics for their data types.
     */
    foreach (tlitem, *targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tlitem);

        if (tle != NULL && tle->resjunk) {
            continue; /* ignore junk */
        }

        if (tle != NULL) {
            result = addTargetToGroupList(pstate, tle, result, *targetlist, exprLocation((Node*)tle->expr), true);
        }
        else{
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid tle value")));
        }
    }

    return result;
}

/*
 * transformDistinctOnClause -
 *	  transform a DISTINCT ON clause
 *
 * Since we may need to add items to the query's targetlist, that list
 * is passed by reference.
 *
 * As with GROUP BY, we absorb the sorting semantics of ORDER BY as much as
 * possible into the distinctClause.  This avoids a possible need to re-sort,
 * and allows the user to choose the equality semantics used by DISTINCT,
 * should she be working with a datatype that has more than one equality
 * operator.
 */
List* transformDistinctOnClause(ParseState* pstate, List* distinctlist, List** targetlist, List* sortClause)
{
    List* result = NIL;
    List* sortgrouprefs = NIL;
    bool skipped_sortitem = false;
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;

    /*
     * Add all the DISTINCT ON expressions to the tlist (if not already
     * present, they are added as resjunk items).  Assign sortgroupref numbers
     * to them, and make a list of these numbers.  (NB: we rely below on the
     * sortgrouprefs list being one-for-one with the original distinctlist.
     * Also notice that we could have duplicate DISTINCT ON expressions and
     * hence duplicate entries in sortgrouprefs.)
     */
    foreach (lc, distinctlist) {
        Node* dexpr = (Node*)lfirst(lc);
        int sortgroupref;
        TargetEntry* tle = NULL;

        tle = findTargetlistEntrySQL92(pstate, dexpr, targetlist, DISTINCT_ON_CLAUSE);
        sortgroupref = assignSortGroupRef(tle, *targetlist);
        sortgrouprefs = lappend_int(sortgrouprefs, sortgroupref);
    }

    /*
     * If the user writes both DISTINCT ON and ORDER BY, adopt the sorting
     * semantics from ORDER BY items that match DISTINCT ON items, and also
     * adopt their column sort order.  We insist that the distinctClause and
     * sortClause match, so throw error if we find the need to add any more
     * distinctClause items after we've skipped an ORDER BY item that wasn't
     * in DISTINCT ON.
     */
    skipped_sortitem = false;
    foreach (lc, sortClause) {
        SortGroupClause* scl = (SortGroupClause*)lfirst(lc);

        if (list_member_int(sortgrouprefs, scl->tleSortGroupRef)) {
            if (skipped_sortitem) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                        errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                        parser_errposition(
                            pstate, get_matching_location(scl->tleSortGroupRef, sortgrouprefs, distinctlist))));
            } else {
                result = lappend(result, copyObject(scl));
            }
        } else {
            skipped_sortitem = true;
        }
    }

    /*
     * Now add any remaining DISTINCT ON items, using default sort/group
     * semantics for their data types.	(Note: this is pretty questionable; if
     * the ORDER BY list doesn't include all the DISTINCT ON items and more
     * besides, you certainly aren't using DISTINCT ON in the intended way,
     * and you probably aren't going to get consistent results.  It might be
     * better to throw an error or warning here.  But historically we've
     * allowed it, so keep doing so.)
     */
    forboth(lc, distinctlist, lc2, sortgrouprefs)
    {
        Node* dexpr = (Node*)lfirst(lc);
        int sortgroupref = lfirst_int(lc2);
        TargetEntry* tle = get_sortgroupref_tle(sortgroupref, *targetlist);

        if (targetIsInSortList(tle, InvalidOid, result)) {
            continue; /* already in list (with some semantics) */
        }
        if (skipped_sortitem) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                    errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                    parser_errposition(pstate, exprLocation(dexpr))));
        }
        result = addTargetToGroupList(pstate, tle, result, *targetlist, exprLocation(dexpr), true);
    }

    return result;
}

/*
 * getMatchingLocation
 *		Get the exprLocation of the exprs member corresponding to the
 *		(first) member of sortgrouprefs that equals sortgroupref.
 *
 * This is used so that we can point at a troublesome DISTINCT ON entry.
 * (Note that we need to use the original untransformed DISTINCT ON list
 * item, as whatever TLE it corresponds to will very possibly have a
 * parse location pointing to some matching entry in the SELECT list
 * or ORDER BY list.)
 */
static int get_matching_location(int sortgroupref, List* sortgrouprefs, List* exprs)
{
    ListCell* lcs = NULL;
    ListCell* lce = NULL;

    forboth(lcs, sortgrouprefs, lce, exprs)
    {
        if (lfirst_int(lcs) == sortgroupref) {
            return exprLocation((Node*)lfirst(lce));
        }
    }
    /* if no match, caller blew it */
    ereport(ERROR,
        (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH), errmsg("get_matching_location: no matching sortgroupref")));
    return -1; /* keep compiler quiet */
}

bool has_not_null_constraint(ParseState* pstate,TargetEntry* tle)
{
    if(!IsA(tle->expr, Var))
        return false;
    Var* var =(Var*)tle->expr;
    RangeTblEntry* rte = (RangeTblEntry*)linitial(pstate->p_rtable);
    Oid reloid = rte->relid;
    AttrNumber attno = var->varoattno;
    if (reloid != InvalidOid && attno != InvalidAttrNumber) {
        HeapTuple atttuple =
            SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(reloid), Int16GetDatum(attno));
        if (!HeapTupleIsValid(atttuple)) {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for attribute %u of relation %hd", reloid, attno)));
        }
        Form_pg_attribute attStruct = (Form_pg_attribute)GETSTRUCT(atttuple);
        bool attHasNotNull = attStruct->attnotnull;
        heap_freetuple_ext(atttuple);
        return attHasNotNull;
    }
    return false;
}

bool is_single_table_query(ParseState* pstate)
{
    if (list_length(pstate->p_rtable) != 1) {
        return false;
    }

    RangeTblEntry* rte = (RangeTblEntry*)linitial(pstate->p_rtable);
    if (rte->rtekind != RTE_RELATION) {
        return false;
    }
    return true;
}

/*
 * addTargetToSortList
 *		If the given targetlist entry isn't already in the SortGroupClause
 *		list, add it to the end of the list, using the given sort ordering
 *		info.
 *
 * If resolveUnknown is TRUE, convert TLEs of type UNKNOWN to TEXT.  If not,
 * do nothing (which implies the search for a sort operator will fail).
 * pstate should be provided if resolveUnknown is TRUE, but can be NULL
 * otherwise.
 *
 * Returns the updated SortGroupClause list.
 */
List* addTargetToSortList(
    ParseState* pstate, TargetEntry* tle, List* sortlist, List* targetlist, SortBy* sortby, bool resolveUnknown)
{
    Oid restype = exprType((Node*)tle->expr);
    Oid sortop;
    Oid eqop;
    bool hashable = false;
    bool reverse = false;
    int location;
    ParseCallbackState pcbstate;

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if (restype == UNKNOWNOID && resolveUnknown) {
        tle->expr = (Expr*)coerce_type(
            pstate, (Node*)tle->expr, restype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
        restype = TEXTOID;
    }

    /*
     * Rather than clutter the API of get_sort_group_operators and the other
     * functions we're about to use, make use of error context callback to
     * mark any error reports with a parse position.  We point to the operator
     * location if present, else to the expression being sorted.  (NB: use the
     * original untransformed expression here; the TLE entry might well point
     * at a duplicate expression in the regular SELECT list.)
     */
    location = sortby->location;
    if (location < 0) {
        location = exprLocation(sortby->node);
    }
    setup_parser_errposition_callback(&pcbstate, pstate, location);

    /* determine the sortop, eqop, and directionality */
    switch (sortby->sortby_dir) {
        case SORTBY_DEFAULT:
        case SORTBY_ASC:
            get_sort_group_operators(restype, true, true, false, &sortop, &eqop, NULL, &hashable);
            reverse = false;
            break;
        case SORTBY_DESC:
            get_sort_group_operators(restype, false, true, true, NULL, &eqop, &sortop, &hashable);
            reverse = true;
            break;
        case SORTBY_USING:
            AssertEreport(sortby->useOp != NIL, MOD_OPT, "para cannot be NIL");
            sortop = compatible_oper_opid(sortby->useOp, restype, restype, false);

            /*
             * Verify it's a valid ordering operator, fetch the corresponding
             * equality operator, and determine whether to consider it like
             * ASC or DESC.
             */
            eqop = get_equality_op_for_ordering_op(sortop, &reverse);
            if (!OidIsValid(eqop)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("operator %s is not a valid ordering operator", strVal(llast(sortby->useOp))),
                        errhint("Ordering operators must be \"<\" or \">\" members of btree operator families.")));
            }
            /*
             * Also see if the equality operator is hashable.
             */
            hashable = op_hashjoinable(eqop, restype);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized sortby_dir: %d", sortby->sortby_dir)));
            sortop = InvalidOid; /* keep compiler quiet */
            eqop = InvalidOid;
            hashable = false;
            reverse = false;
            break;
    }

    cancel_parser_errposition_callback(&pcbstate);

    /* avoid making duplicate sortlist entries */
    if (!targetIsInSortList(tle, sortop, sortlist)) {
        SortGroupClause* sortcl = makeNode(SortGroupClause);

        sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

        sortcl->eqop = eqop;
        sortcl->sortop = sortop;
        sortcl->hashable = hashable;

        /*
         * For a single table query if the sorted column has constraints,
         * we can remove NULLS LAST/FIRST to get a better plan.
         * In some scenarios, we cannot optimize NULLS LAST/FIRST, such as the full outer join statement, 
         * because the processing process may complement the null value in the non-null constraint column.
         * If NULLS LAST/FIRST is removed, the null value will be sorted incorrectly,
         * so we only optimize the single table query.
         */ 
        if (sortby->sortby_nulls != SORTBY_NULLS_DEFAULT && is_single_table_query(pstate) &&
            has_not_null_constraint(pstate, tle)) {
            sortby->sortby_nulls = SORTBY_NULLS_DEFAULT;
        }

        switch (sortby->sortby_nulls) {
            case SORTBY_NULLS_DEFAULT:
                /* NULLS FIRST is default for DESC; other way for ASC */
                sortcl->nulls_first = reverse;
                break;
            case SORTBY_NULLS_FIRST:
                sortcl->nulls_first = true;
                break;
            case SORTBY_NULLS_LAST:
                sortcl->nulls_first = false;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("unrecognized sortby_nulls: %d", sortby->sortby_nulls)));
                break;
        }

        sortlist = lappend(sortlist, sortcl);
    }

    return sortlist;
}

/*
 * addTargetToGroupList
 *		If the given targetlist entry isn't already in the SortGroupClause
 *		list, add it to the end of the list, using default sort/group
 *		semantics.
 *
 * This is very similar to addTargetToSortList, except that we allow the
 * case where only a grouping (equality) operator can be found, and that
 * the TLE is considered "already in the list" if it appears there with any
 * sorting semantics.
 *
 * location is the parse location to be fingered in event of trouble.  Note
 * that we can't rely on exprLocation(tle->expr), because that might point
 * to a SELECT item that matches the GROUP BY item; it'd be pretty confusing
 * to report such a location.
 *
 * If resolveUnknown is TRUE, convert TLEs of type UNKNOWN to TEXT.  If not,
 * do nothing (which implies the search for an equality operator will fail).
 * pstate should be provided if resolveUnknown is TRUE, but can be NULL
 * otherwise.
 *
 * Returns the updated SortGroupClause list.
 */
static List* addTargetToGroupList(
    ParseState* pstate, TargetEntry* tle, List* grouplist, List* targetlist, int location, bool resolveUnknown)
{
    Oid restype = exprType((Node*)tle->expr);

    /* if tlist item is an UNKNOWN literal, change it to TEXT */
    if (restype == UNKNOWNOID && resolveUnknown) {
        tle->expr = (Expr*)coerce_type(
            pstate, (Node*)tle->expr, restype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
        restype = TEXTOID;
    }

    /* avoid making duplicate grouplist entries */
    if (!targetIsInSortList(tle, InvalidOid, grouplist)) {
        SortGroupClause* grpcl = makeNode(SortGroupClause);
        Oid sortop;
        Oid eqop;
        bool hashable = false;
        ParseCallbackState pcbstate;

        setup_parser_errposition_callback(&pcbstate, pstate, location);

        /* determine the eqop and optional sortop */
        get_sort_group_operators(restype, false, true, false, &sortop, &eqop, NULL, &hashable);

        cancel_parser_errposition_callback(&pcbstate);

        grpcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
        grpcl->eqop = eqop;
        grpcl->sortop = sortop;
        grpcl->nulls_first = false; /* OK with or without sortop */
        grpcl->hashable = hashable;

        grouplist = lappend(grouplist, grpcl);
    }

    return grouplist;
}

/*
 * assignSortGroupRef
 *	  Assign the targetentry an unused ressortgroupref, if it doesn't
 *	  already have one.  Return the assigned or pre-existing refnumber.
 *
 * 'tlist' is the targetlist containing (or to contain) the given targetentry.
 */
Index assignSortGroupRef(TargetEntry* tle, List* tlist)
{
    Index maxRef;
    ListCell* l = NULL;

    if (tle->ressortgroupref) {
        /* already has one? */
        return tle->ressortgroupref;
    }
    /* easiest way to pick an unused refnumber: max used + 1 */
    maxRef = 0;
    foreach (l, tlist) {
        Index ref = ((TargetEntry*)lfirst(l))->ressortgroupref;

        if (ref > maxRef) {
            maxRef = ref;
        }
    }
    tle->ressortgroupref = maxRef + 1;
    return tle->ressortgroupref;
}

/*
 * targetIsInSortList
 *		Is the given target item already in the sortlist?
 *		If sortop is not InvalidOid, also test for a match to the sortop.
 *
 * It is not an oversight that this function ignores the nulls_first flag.
 * We check sortop when determining if an ORDER BY item is redundant with
 * earlier ORDER BY items, because it's conceivable that "ORDER BY
 * foo USING <, foo USING <<<" is not redundant, if <<< distinguishes
 * values that < considers equal.  We need not check nulls_first
 * however, because a lower-order column with the same sortop but
 * opposite nulls direction is redundant.  Also, we can consider
 * ORDER BY foo ASC, foo DESC redundant, so check for a commutator match.
 *
 * Works for both ordering and grouping lists (sortop would normally be
 * InvalidOid when considering grouping).  Note that the main reason we need
 * this routine (and not just a quick test for nonzeroness of ressortgroupref)
 * is that a TLE might be in only one of the lists.
 */
bool targetIsInSortList(TargetEntry* tle, Oid sortop, List* sortList)
{
    Index ref = tle->ressortgroupref;
    ListCell* l = NULL;

    /* no need to scan list if tle has no marker */
    if (ref == 0)
        return false;

    foreach (l, sortList) {
        SortGroupClause* scl = (SortGroupClause*)lfirst(l);

        if (scl->tleSortGroupRef == ref &&
            (sortop == InvalidOid || sortop == scl->sortop || sortop == get_commutator(scl->sortop))) {
            return true;
        }
    }
    return false;
}

/*
 * findWindowClause
 *		Find the named WindowClause in the list, or return NULL if not there
 */
static WindowClause* findWindowClause(List* wclist, const char* name)
{
    ListCell* l = NULL;

    foreach (l, wclist) {
        WindowClause* wc = (WindowClause*)lfirst(l);

        if (wc->name && strcmp(wc->name, name) == 0) {
            return wc;
        }
    }

    return NULL;
}

/*
 * transformFrameOffset
 *		Process a window frame offset expression
 */
static Node* transformFrameOffset(ParseState* pstate, int frameOptions, Node* clause)
{
    const char* constructName = NULL;
    Node* node = NULL;

    /* Quick exit if no offset expression */
    if (clause == NULL) {
        return NULL;
    }
    /* Transform the raw expression tree */
    node = transformExpr(pstate, clause);

    if (frameOptions & FRAMEOPTION_ROWS) {
        /*
         * Like LIMIT clause, simply coerce to int8
         */
        constructName = "ROWS";
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
    } else if (frameOptions & FRAMEOPTION_RANGE) {
        /*
         * this needs a lot of thought to decide how to support in the context
         * of Postgres' extensible datatype framework
         */
        constructName = "RANGE";
        /* error was already thrown by gram.y, this is just a backstop */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("window frame with value offset is not implemented")));
    } else {
        Assert(false);
    }
    /* Disallow variables and aggregates in frame offsets */
    checkExprIsVarFree(pstate, node, constructName);

    return node;
}
