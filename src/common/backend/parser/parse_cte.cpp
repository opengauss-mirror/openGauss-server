/* -------------------------------------------------------------------------
 *
 * parse_cte.cpp
 *	  handle CTEs (common table expressions) in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_cte.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_cte.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* Enumeration of contexts in which a self-reference is disallowed */
typedef enum {
    RECURSION_OK,
    RECURSION_NONRECURSIVETERM, /* inside the left-hand term */
    RECURSION_SUBLINK,          /* inside a sublink */
    RECURSION_OUTERJOIN,        /* inside nullable side of an outer join */
    RECURSION_INTERSECT,        /* underneath INTERSECT (ALL) */
    RECURSION_EXCEPT            /* underneath EXCEPT (ALL) */
} RecursionContext;

/* Associated error messages --- each must have one %s for CTE name */
static const char* const recursion_errormsgs[] = {
    /* RECURSION_OK */
    NULL,
    /* RECURSION_NONRECURSIVETERM */
    gettext_noop("recursive reference to query \"%s\" must not appear within its non-recursive term"),
    /* RECURSION_SUBLINK */
    gettext_noop("recursive reference to query \"%s\" must not appear within a subquery"),
    /* RECURSION_OUTERJOIN */
    gettext_noop("recursive reference to query \"%s\" must not appear within an outer join"),
    /* RECURSION_INTERSECT */
    gettext_noop("recursive reference to query \"%s\" must not appear within INTERSECT"),
    /* RECURSION_EXCEPT */
    gettext_noop("recursive reference to query \"%s\" must not appear within EXCEPT")};

/*
 * For WITH RECURSIVE, we have to find an ordering of the clause members
 * with no forward references, and determine which members are recursive
 * (i.e., self-referential).  It is convenient to do this with an array
 * of CteItems instead of a list of CommonTableExprs.
 */
typedef struct CteItem {
    CommonTableExpr* cte;  /* One CTE to examine */
    int id;                /* Its ID number for dependencies */
    Bitmapset* depends_on; /* CTEs depended on (not including self) */
} CteItem;

/* CteState is what we need to pass around in the tree walkers */
typedef struct CteState {
    /* global state: */
    ParseState* pstate; /* global parse state */
    CteItem* items;     /* array of CTEs and extra data */
    int numitems;       /* number of CTEs */
    /* working state during a tree walk: */
    int curitem;      /* index of item currently being examined */
    List* innerwiths; /* list of lists of CommonTableExpr */
    /* working state for checkWellFormedRecursion walk only: */
    int selfrefcount;         /* number of self-references detected */
    RecursionContext context; /* context to allow or disallow self-ref */
} CteState;

static void analyzeCTE(ParseState* pstate, CommonTableExpr* cte);

/* Dependency processing functions */
static void makeDependencyGraph(CteState* cstate);
static bool makeDependencyGraphWalker(Node* node, CteState* cstate);
static void TopologicalSort(ParseState* pstate, CteItem* items, int numitems);

/* Recursion validity checker functions */
static void checkWellFormedRecursion(CteState* cstate);
static bool checkWellFormedRecursionWalker(Node* node, CteState* cstate);
static void checkWellFormedSelectStmt(SelectStmt* stmt, CteState* cstate);

/*
 * transformWithClause -
 *	  Transform the list of WITH clause "common table expressions" into
 *	  Query nodes.
 *
 * The result is the list of transformed CTEs to be put into the output
 * Query.  (This is in fact the same as the ending value of p_ctenamespace,
 * but it seems cleaner to not expose that in the function's API.)
 */
List* transformWithClause(ParseState* pstate, WithClause* withClause)
{
    ListCell* lc = NULL;

    /* Only one WITH clause per query level */
    AssertEreport(pstate->p_ctenamespace == NIL, MOD_OPT, "");
    AssertEreport(pstate->p_future_ctes == NIL, MOD_OPT, "");

    /*
     * For either type of WITH, there must not be duplicate CTE names in the
     * list.  Check this right away so we needn't worry later.
     *
     * Also, tentatively mark each CTE as non-recursive, and initialize its
     * reference count to zero, and set pstate->p_hasModifyingCTE if needed.
     */
    foreach (lc, withClause->ctes) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
        ListCell* rest = NULL;

        for_each_cell(rest, lnext(lc))
        {
            CommonTableExpr* cte2 = (CommonTableExpr*)lfirst(rest);

            if (strcmp(cte->ctename, cte2->ctename) == 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_ALIAS),
                        errmsg("WITH query name \"%s\" specified more than once", cte2->ctename),
                        parser_errposition(pstate, cte2->location)));
            }
        }

        cte->cterecursive = false;
        cte->cterefcount = 0;

        if (!IsA(cte->ctequery, SelectStmt)) {
            /* must be a data-modifying statement */
            AssertEreport(
                IsA(cte->ctequery, InsertStmt) || IsA(cte->ctequery, UpdateStmt) || IsA(cte->ctequery, DeleteStmt),
                MOD_OPT,
                "");

            pstate->p_hasModifyingCTE = true;
        }
    }

    if (withClause->recursive) {
        /*
         * For WITH RECURSIVE, we rearrange the list elements if needed to
         * eliminate forward references.  First, build a work array and set up
         * the data structure needed by the tree walkers.
         */
        CteState cstate;
        int i;

        cstate.pstate = pstate;
        cstate.numitems = list_length(withClause->ctes);
        cstate.items = (CteItem*)palloc0(cstate.numitems * sizeof(CteItem));
        i = 0;
        foreach (lc, withClause->ctes) {
            cstate.items[i].cte = (CommonTableExpr*)lfirst(lc);
            cstate.items[i].id = i;
            i++;
        }

        /*
         * Find all the dependencies and sort the CteItems into a safe
         * processing order.  Also, mark CTEs that contain self-references.
         */
        makeDependencyGraph(&cstate);

        /*
         * Check that recursive queries are well-formed.
         */
        checkWellFormedRecursion(&cstate);

        /*
         * Set up the ctenamespace for parse analysis.	Per spec, all the WITH
         * items are visible to all others, so stuff them all in before parse
         * analysis.  We build the list in safe processing order so that the
         * planner can process the queries in sequence.
         */
        for (i = 0; i < cstate.numitems; i++) {
            CommonTableExpr* cte = cstate.items[i].cte;

            pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
        }

        /*
         * Do parse analysis in the order determined by the topological sort.
         */
        for (i = 0; i < cstate.numitems; i++) {
            CommonTableExpr* cte = cstate.items[i].cte;

            analyzeCTE(pstate, cte);
        }
    } else {
        /*
         * For non-recursive WITH, just analyze each CTE in sequence and then
         * add it to the ctenamespace.	This corresponds to the spec's
         * definition of the scope of each WITH name.  However, to allow error
         * reports to be aware of the possibility of an erroneous reference,
         * we maintain a list in p_future_ctes of the not-yet-visible CTEs.
         */
        pstate->p_future_ctes = list_copy(withClause->ctes);

        foreach (lc, withClause->ctes) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

            analyzeCTE(pstate, cte);
            pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
            pstate->p_future_ctes = list_delete_first(pstate->p_future_ctes);
        }
    }

    return pstate->p_ctenamespace;
}

/*
 * Perform the actual parse analysis transformation of one CTE.  All
 * CTEs it depends on have already been loaded into pstate->p_ctenamespace,
 * and have been marked with the correct output column names/types.
 */
static void analyzeCTE(ParseState* pstate, CommonTableExpr* cte)
{
    Query* query = NULL;

    /* Analysis not done already */
    AssertEreport(!IsA(cte->ctequery, Query), MOD_OPT, "");

    query = parse_sub_analyze(cte->ctequery, pstate, cte, false, true);
    cte->ctequery = (Node*)query;

    /*
     * Check that we got something reasonable.	These first two cases should
     * be prevented by the grammar.
     */
    if (!IsA(query, Query))
        ereport(
            ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("unexpected non-Query statement in WITH")));
    if (query->utilityStmt != NULL)
        ereport(ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("unexpected utility statement in WITH")));

    /*
     * We disallow data-modifying WITH except at the top level of a query,
     * because it's not clear when such a modification should be executed.
     */
    if (query->commandType != CMD_SELECT && pstate->parentParseState != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("WITH clause containing a data-modifying statement must be at the top level"),
                parser_errposition(pstate, cte->location)));
    }

    /*
     * CTE queries are always marked not canSetTag.  (Currently this only
     * matters for data-modifying statements, for which the flag will be
     * propagated to the ModifyTable plan node.)
     */
    query->canSetTag = false;

    if (!cte->cterecursive) {
        /* Compute the output column names/types if not done yet */
        analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
    } else {
        /*
         * Verify that the previously determined output column types and
         * collations match what the query really produced.  We have to check
         * this because the recursive term could have overridden the
         * non-recursive term, and we don't have any easy way to fix that.
         */
        ListCell *lctlist = NULL;
        ListCell *lctyp = NULL;
        ListCell *lctypmod = NULL;
        ListCell *lccoll = NULL;
        int varattno;

        lctyp = list_head(cte->ctecoltypes);
        lctypmod = list_head(cte->ctecoltypmods);
        lccoll = list_head(cte->ctecolcollations);
        varattno = 0;
        foreach (lctlist, GetCTETargetList(cte)) {
            TargetEntry* te = (TargetEntry*)lfirst(lctlist);
            Node* texpr = NULL;

            if (te->resjunk) {
                continue;
            }
            varattno++;
            AssertEreport(varattno == te->resno, MOD_OPT, "");
            if (lctyp == NULL || lctypmod == NULL || lccoll == NULL) { /* shouldn't happen */
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("wrong number of output columns in WITH")));
            }
            texpr = (Node*)te->expr;
            if (exprType(texpr) != lfirst_oid(lctyp) || exprTypmod(texpr) != lfirst_int(lctypmod)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("recursive query \"%s\" column %d has type %s in non-recursive term but type %s overall",
                            cte->ctename,
                            varattno,
                            format_type_with_typemod(lfirst_oid(lctyp), lfirst_int(lctypmod)),
                            format_type_with_typemod(exprType(texpr), exprTypmod(texpr))),
                        errhint("Cast the output of the non-recursive term to the correct type."),
                        parser_errposition(pstate, exprLocation(texpr))));
            }
            if (exprCollation(texpr) != lfirst_oid(lccoll)) {
                ereport(ERROR,
                    (errcode(ERRCODE_COLLATION_MISMATCH),
                        errmsg("recursive query \"%s\" column %d has collation \"%s\" in non-recursive term but "
                               "collation \"%s\" overall",
                            cte->ctename,
                            varattno,
                            get_collation_name(lfirst_oid(lccoll)),
                            get_collation_name(exprCollation(texpr))),
                        errhint("Use the COLLATE clause to set the collation of the non-recursive term."),
                        parser_errposition(pstate, exprLocation(texpr))));
            }
            lctyp = lnext(lctyp);
            lctypmod = lnext(lctypmod);
            lccoll = lnext(lccoll);
        }
        if (lctyp != NULL || lctypmod != NULL || lccoll != NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("wrong number of output columns in WITH")));
        }
    }
}

/*
 * Compute derived fields of a CTE, given the transformed output targetlist
 *
 * For a nonrecursive CTE, this is called after transforming the CTE's query.
 * For a recursive CTE, we call it after transforming the non-recursive term,
 * and pass the targetlist emitted by the non-recursive term only.
 *
 * Note: in the recursive case, the passed pstate is actually the one being
 * used to analyze the CTE's query, so it is one level lower down than in
 * the nonrecursive case.  This doesn't matter since we only use it for
 * error message context anyway.
 */
void analyzeCTETargetList(ParseState* pstate, CommonTableExpr* cte, List* tlist)
{
    int numaliases;
    int varattno;
    ListCell* tlistitem = NULL;

    /* Not done already ... */
    AssertEreport(cte->ctecolnames == NIL, MOD_OPT, "");

    /*
     * We need to determine column names, types, and collations.  The alias
     * column names override anything coming from the query itself.  (Note:
     * the SQL spec says that the alias list must be empty or exactly as long
     * as the output column set; but we allow it to be shorter for consistency
     * with Alias handling.)
     */
    cte->ctecolnames = (List*)copyObject(cte->aliascolnames);
    cte->ctecoltypes = cte->ctecoltypmods = cte->ctecolcollations = NIL;
    numaliases = list_length(cte->aliascolnames);
    varattno = 0;
    foreach (tlistitem, tlist) {
        TargetEntry* te = (TargetEntry*)lfirst(tlistitem);
        Oid coltype;
        int32 coltypmod;
        Oid colcoll;

        if (te->resjunk) {
            continue;
        }
        varattno++;
        AssertEreport(varattno == te->resno, MOD_OPT, "");
        if (varattno > numaliases) {
            char* attrname = NULL;

            attrname = pstrdup(te->resname);
            cte->ctecolnames = lappend(cte->ctecolnames, makeString(attrname));
        }
        coltype = exprType((Node*)te->expr);
        coltypmod = exprTypmod((Node*)te->expr);
        colcoll = exprCollation((Node*)te->expr);

        /*
         * If the CTE is recursive, force the exposed column type of any
         * "unknown" column to "text".	This corresponds to the fact that
         * SELECT 'foo' UNION SELECT 'bar' will ultimately produce text. We
         * might see "unknown" as a result of an untyped literal in the
         * non-recursive term's select list, and if we don't convert to text
         * then we'll have a mismatch against the UNION result.
         *
         * The column might contain 'foo' COLLATE "bar", so don't override
         * collation if it's already set.
         */
        if (cte->cterecursive && coltype == UNKNOWNOID) {
            coltype = TEXTOID;
            coltypmod = -1; /* should be -1 already, but be sure */
            if (!OidIsValid(colcoll))
                colcoll = DEFAULT_COLLATION_OID;
        }
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, coltype);
        cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, coltypmod);
        cte->ctecolcollations = lappend_oid(cte->ctecolcollations, colcoll);
    }
    if (varattno < numaliases) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("WITH query \"%s\" has %d columns available but %d columns specified",
                    cte->ctename,
                    varattno,
                    numaliases),
                parser_errposition(pstate, cte->location)));
    }
}

/*
 * Identify the cross-references of a list of WITH RECURSIVE items,
 * and sort into an order that has no forward references.
 */
static void makeDependencyGraph(CteState* cstate)
{
    int i;

    for (i = 0; i < cstate->numitems; i++) {
        CommonTableExpr* cte = cstate->items[i].cte;

        cstate->curitem = i;
        cstate->innerwiths = NIL;
        (void)makeDependencyGraphWalker((Node*)cte->ctequery, cstate);
        AssertEreport(cstate->innerwiths == NIL, MOD_OPT, "");
    }

    TopologicalSort(cstate->pstate, cstate->items, cstate->numitems);
}

/*
 * Tree walker function to detect cross-references and self-references of the
 * CTEs in a WITH RECURSIVE list.
 */
static bool makeDependencyGraphWalker(Node* node, CteState* cstate)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, RangeVar)) {
        RangeVar* rv = (RangeVar*)node;

        /* If unqualified name, might be a CTE reference */
        if (!rv->schemaname) {
            ListCell* lc = NULL;
            int i;

            /* ... but first see if it's captured by an inner WITH */
            foreach (lc, cstate->innerwiths) {
                List* withlist = (List*)lfirst(lc);
                ListCell* lc2 = NULL;

                foreach (lc2, withlist) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc2);

                    if (strcmp(rv->relname, cte->ctename) == 0) {
                        return false; /* yes, so bail out */
                    }
                }
            }

            /* No, could be a reference to the query level we are working on */
            for (i = 0; i < cstate->numitems; i++) {
                CommonTableExpr* cte = cstate->items[i].cte;

                if (strcmp(rv->relname, cte->ctename) == 0) {
                    int myindex = cstate->curitem;

                    if (i != myindex) {
                        /* Add cross-item dependency */
                        cstate->items[myindex].depends_on =
                            bms_add_member(cstate->items[myindex].depends_on, cstate->items[i].id);
                    } else {
                        /* Found out this one is self-referential */
                        cte->cterecursive = true;
                    }
                    break;
                }
            }
        }
        return false;
    }
    if (IsA(node, SelectStmt)) {
        SelectStmt* stmt = (SelectStmt*)node;
        ListCell* lc = NULL;

        if (stmt->withClause) {
            if (stmt->withClause->recursive) {
                /*
                 * In the RECURSIVE case, all query names of the WITH are
                 * visible to all WITH items as well as the main query. So
                 * push them all on, process, pop them all off.
                 */
                cstate->innerwiths = lcons(stmt->withClause->ctes, cstate->innerwiths);
                foreach (lc, stmt->withClause->ctes) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

                    (void)makeDependencyGraphWalker(cte->ctequery, cstate);
                }
                (void)raw_expression_tree_walker(node, (bool (*)())makeDependencyGraphWalker, (void*)cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            } else {
                /*
                 * In the non-RECURSIVE case, query names are visible to the
                 * WITH items after them and to the main query.
                 */
                ListCell* cell1 = NULL;

                cstate->innerwiths = lcons(NIL, cstate->innerwiths);
                cell1 = list_head(cstate->innerwiths);
                foreach (lc, stmt->withClause->ctes) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

                    (void)makeDependencyGraphWalker(cte->ctequery, cstate);
                    lfirst(cell1) = lappend((List*)lfirst(cell1), cte);
                }
                (void)raw_expression_tree_walker(node, (bool (*)())makeDependencyGraphWalker, (void*)cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            }
            /* We're done examining the SelectStmt */
            return false;
        }
        /* if no WITH clause, just fall through for normal processing */
    }
    if (IsA(node, WithClause)) {
        /*
         * Prevent raw_expression_tree_walker from recursing directly into a
         * WITH clause.  We need that to happen only under the control of the
         * code above.
         */
        return false;
    }
    return raw_expression_tree_walker(node, (bool (*)())makeDependencyGraphWalker, (void*)cstate);
}

/*
 * Sort by dependencies, using a standard topological sort operation
 */
static void TopologicalSort(ParseState* pstate, CteItem* items, int numitems)
{
    int i;
    int j;

    /* for each position in sequence ... */
    for (i = 0; i < numitems; i++) {
        /* ... scan the remaining items to find one that has no dependencies */
        for (j = i; j < numitems; j++) {
            if (bms_is_empty(items[j].depends_on)) {
                break;
            }
        }

        /* if we didn't find one, the dependency graph has a cycle */
        if (j >= numitems) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("mutual recursion between WITH items is not implemented"),
                    parser_errposition(pstate, items[i].cte->location)));
        }

        /*
         * Found one.  Move it to front and remove it from every other item's
         * dependencies.
         */
        if (i != j) {
            CteItem tmp;

            tmp = items[i];
            items[i] = items[j];
            items[j] = tmp;
        }

        /*
         * Items up through i are known to have no dependencies left, so we
         * can skip them in this loop.
         */
        for (j = i + 1; j < numitems; j++) {
            items[j].depends_on = bms_del_member(items[j].depends_on, items[i].id);
        }
    }
}

/*
 * Check that recursive queries are well-formed.
 */
static void checkWellFormedRecursion(CteState* cstate)
{
    int i;

    for (i = 0; i < cstate->numitems; i++) {
        CommonTableExpr* cte = cstate->items[i].cte;
        SelectStmt* stmt = (SelectStmt*)cte->ctequery;

        AssertEreport(!IsA(stmt, Query), MOD_OPT, ""); /* not analyzed yet */

        /* Ignore items that weren't found to be recursive */
        if (!cte->cterecursive) {
            continue;
        }

        /* Must be a SELECT statement */
        if (!IsA(stmt, SelectStmt)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_RECURSION),
                    errmsg("recursive query \"%s\" must not contain data-modifying statements", cte->ctename),
                    parser_errposition(cstate->pstate, cte->location)));
        }

        /* Must have top-level UNION */
        if (stmt->op != SETOP_UNION) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_RECURSION),
                    errmsg(
                        "recursive query \"%s\" does not have the form non-recursive-term UNION [ALL] recursive-term",
                        cte->ctename),
                    parser_errposition(cstate->pstate, cte->location)));
        }

        /* The left-hand operand mustn't contain self-reference at all */
        cstate->curitem = i;
        cstate->innerwiths = NIL;
        cstate->selfrefcount = 0;
        cstate->context = RECURSION_NONRECURSIVETERM;
        (void)checkWellFormedRecursionWalker((Node*)stmt->larg, cstate);
        AssertEreport(cstate->innerwiths == NIL, MOD_OPT, "");

        /* Right-hand operand should contain one reference in a valid place */
        cstate->curitem = i;
        cstate->innerwiths = NIL;
        cstate->selfrefcount = 0;
        cstate->context = RECURSION_OK;
        (void)checkWellFormedRecursionWalker((Node*)stmt->rarg, cstate);
        AssertEreport(cstate->innerwiths == NIL, MOD_OPT, "");
        if (cstate->selfrefcount != 1) { /* shouldn't happen */
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("missing recursive reference")));
        }

        /* WITH mustn't contain self-reference, either */
        if (stmt->withClause) {
            cstate->curitem = i;
            cstate->innerwiths = NIL;
            cstate->selfrefcount = 0;
            cstate->context = RECURSION_SUBLINK;
            (void)checkWellFormedRecursionWalker((Node*)stmt->withClause->ctes, cstate);
            AssertEreport(cstate->innerwiths == NIL, MOD_OPT, "");
        }

        /*
         * Disallow ORDER BY and similar decoration atop the UNION. These
         * don't make sense because it's impossible to figure out what they
         * mean when we have only part of the recursive query's results. (If
         * we did allow them, we'd have to check for recursive references
         * inside these subtrees.)
         */
        if (stmt->sortClause) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("ORDER BY in a recursive query is not implemented"),
                    parser_errposition(cstate->pstate, exprLocation((Node*)stmt->sortClause))));
        }
        if (stmt->limitOffset) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("OFFSET in a recursive query is not implemented"),
                    parser_errposition(cstate->pstate, exprLocation(stmt->limitOffset))));
        }
        if (stmt->limitCount) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("LIMIT in a recursive query is not implemented"),
                    parser_errposition(cstate->pstate, exprLocation(stmt->limitCount))));
        }
        if (stmt->lockingClause) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("FOR UPDATE/SHARE in a recursive query is not implemented"),
                    parser_errposition(cstate->pstate, exprLocation((Node*)stmt->lockingClause))));
        }
    }
}

/*
 * Tree walker function to detect invalid self-references in a recursive query.
 */
static bool checkWellFormedRecursionWalker(Node* node, CteState* cstate)
{
    RecursionContext save_context = cstate->context;

    if (node == NULL) {
        return false;
    }
    if (IsA(node, RangeVar)) {
        RangeVar* rv = (RangeVar*)node;

        /* If unqualified name, might be a CTE reference */
        if (!rv->schemaname) {
            ListCell* lc = NULL;
            CommonTableExpr* mycte = NULL;

            /* ... but first see if it's captured by an inner WITH */
            foreach (lc, cstate->innerwiths) {
                List* withlist = (List*)lfirst(lc);
                ListCell* lc2 = NULL;

                foreach (lc2, withlist) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc2);

                    if (strcmp(rv->relname, cte->ctename) == 0) {
                        return false; /* yes, so bail out */
                    }
                }
            }

            /* No, could be a reference to the query level we are working on */
            mycte = cstate->items[cstate->curitem].cte;
            if (strcmp(rv->relname, mycte->ctename) == 0) {
                /* Found a recursive reference to the active query */
                if (cstate->context != RECURSION_OK) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_RECURSION),
                            errmsg(recursion_errormsgs[cstate->context], mycte->ctename),
                            parser_errposition(cstate->pstate, rv->location)));
                }
                /* Count references */
                if (++(cstate->selfrefcount) > 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_RECURSION),
                            errmsg(
                                "recursive reference to query \"%s\" must not appear more than once", mycte->ctename),
                            parser_errposition(cstate->pstate, rv->location)));
                }
            }
        }
        return false;
    }
    if (IsA(node, SelectStmt)) {
        SelectStmt* stmt = (SelectStmt*)node;
        ListCell* lc = NULL;

        if (stmt->withClause) {
            if (stmt->withClause->recursive) {
                /*
                 * In the RECURSIVE case, all query names of the WITH are
                 * visible to all WITH items as well as the main query. So
                 * push them all on, process, pop them all off.
                 */
                cstate->innerwiths = lcons(stmt->withClause->ctes, cstate->innerwiths);
                foreach (lc, stmt->withClause->ctes) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

                    (void)checkWellFormedRecursionWalker(cte->ctequery, cstate);
                }
                checkWellFormedSelectStmt(stmt, cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            } else {
                /*
                 * In the non-RECURSIVE case, query names are visible to the
                 * WITH items after them and to the main query.
                 */
                ListCell* cell1 = NULL;

                cstate->innerwiths = lcons(NIL, cstate->innerwiths);
                cell1 = list_head(cstate->innerwiths);
                foreach (lc, stmt->withClause->ctes) {
                    CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);

                    (void)checkWellFormedRecursionWalker(cte->ctequery, cstate);
                    lfirst(cell1) = lappend((List*)lfirst(cell1), cte);
                }
                checkWellFormedSelectStmt(stmt, cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            }
        } else
            checkWellFormedSelectStmt(stmt, cstate);
        /* We're done examining the SelectStmt */
        return false;
    }
    if (IsA(node, WithClause)) {
        /*
         * Prevent raw_expression_tree_walker from recursing directly into a
         * WITH clause.  We need that to happen only under the control of the
         * code above.
         */
        return false;
    }
    if (IsA(node, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)node;

        switch (j->jointype) {
            case JOIN_INNER:
                (void)checkWellFormedRecursionWalker(j->larg, cstate);
                (void)checkWellFormedRecursionWalker(j->rarg, cstate);
                (void)checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_LEFT:
                (void)checkWellFormedRecursionWalker(j->larg, cstate);
                if (save_context == RECURSION_OK) {
                    cstate->context = RECURSION_OUTERJOIN;
                }
                (void)checkWellFormedRecursionWalker(j->rarg, cstate);
                cstate->context = save_context;
                (void)checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_FULL:
                if (save_context == RECURSION_OK) {
                    cstate->context = RECURSION_OUTERJOIN;
                }
                (void)checkWellFormedRecursionWalker(j->larg, cstate);
                (void)checkWellFormedRecursionWalker(j->rarg, cstate);
                cstate->context = save_context;
                (void)checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_RIGHT:
                if (save_context == RECURSION_OK) {
                    cstate->context = RECURSION_OUTERJOIN;
                }
                (void)checkWellFormedRecursionWalker(j->larg, cstate);
                cstate->context = save_context;
                (void)checkWellFormedRecursionWalker(j->rarg, cstate);
                (void)checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized join type: %d", (int)j->jointype)));
        }
        return false;
    }
    if (IsA(node, SubLink)) {
        SubLink* sl = (SubLink*)node;

        /*
         * we intentionally override outer context, since subquery is
         * independent
         */
        cstate->context = RECURSION_SUBLINK;
        (void)checkWellFormedRecursionWalker(sl->subselect, cstate);
        cstate->context = save_context;
        (void)checkWellFormedRecursionWalker(sl->testexpr, cstate);
        return false;
    }
    return raw_expression_tree_walker(node, (bool (*)())checkWellFormedRecursionWalker, (void*)cstate);
}

/*
 * subroutine for checkWellFormedRecursionWalker: process a SelectStmt
 * without worrying about its WITH clause
 */
static void checkWellFormedSelectStmt(SelectStmt* stmt, CteState* cstate)
{
    RecursionContext save_context = cstate->context;

    if (save_context != RECURSION_OK) {
        /* just recurse without changing state */
        raw_expression_tree_walker((Node*)stmt, (bool (*)())checkWellFormedRecursionWalker, (void*)cstate);
    } else {
        switch (stmt->op) {
            case SETOP_NONE:
            case SETOP_UNION:
                raw_expression_tree_walker((Node*)stmt, (bool (*)())checkWellFormedRecursionWalker, (void*)cstate);
                break;
            case SETOP_INTERSECT:
                if (stmt->all) {
                    cstate->context = RECURSION_INTERSECT;
                }
                (void)checkWellFormedRecursionWalker((Node*)stmt->larg, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->rarg, cstate);
                cstate->context = save_context;
                (void)checkWellFormedRecursionWalker((Node*)stmt->sortClause, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->limitOffset, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->limitCount, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->lockingClause, cstate);
                /* stmt->withClause is intentionally ignored here */
                break;
            case SETOP_EXCEPT:
                if (stmt->all) {
                    cstate->context = RECURSION_EXCEPT;
                }
                (void)checkWellFormedRecursionWalker((Node*)stmt->larg, cstate);
                cstate->context = RECURSION_EXCEPT;
                (void)checkWellFormedRecursionWalker((Node*)stmt->rarg, cstate);
                cstate->context = save_context;
                (void)checkWellFormedRecursionWalker((Node*)stmt->sortClause, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->limitOffset, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->limitCount, cstate);
                (void)checkWellFormedRecursionWalker((Node*)stmt->lockingClause, cstate);
                /* stmt->withClause is intentionally ignored here */
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("unrecognized set op: %d", (int)stmt->op)));
        }
    }
}
