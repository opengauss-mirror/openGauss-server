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


/*
这段代码是 PostgreSQL 数据库中处理 WITH RECURSIVE 查询的一部分。
WITH RECURSIVE 允许在查询内部引用其自身，因此需要对查询进行分析和处理，以确保递归引用是正确的和可行的。
*/

/*
RecursionContext	枚举类型，指定在 WITH RECURSIVE 查询中禁止自引用的上下文。
recursion_errormsgs	包含与每个递归上下文相关的错误消息的数组。
CteItem	结构体，表示 WITH RECURSIVE 查询中的一个公共表达式（Common Table Expression, CTE）及其相关信息。包括 CTE 指针、ID 编号以及依赖的其他 CTE 的位图。
CteState	结构体，在分析 WITH RECURSIVE 查询期间传递状态信息。包括全局状态（ParseState、CTE 数组等）以及在树遍历期间的工作状态。
analyzeCTE	用于分析 WITH RECURSIVE 查询中单个 CTE 的函数。
makeDependencyGraph	构建 CTE 之间的依赖关系图的函数，以确定它们之间的关系。
makeDependencyGraphWalker	递归函数，在树上遍历的过程中收集 CTE 之间的依赖关系。
TopologicalSort	执行 CTE 的拓扑排序的函数，以确保建立一个有效的计算顺序。
checkWellFormedRecursion	检查递归引用是否良好形成，即在给定上下文中是否允许。
checkWellFormedRecursionWalker	递归函数，在树上遍历的过程中检查递归引用的有效性。
checkWellFormedSelectStmt	检查 SELECT 语句中递归引用是否良好形成的函数。
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

/*

这个函数是 PostgreSQL 中负责处理 WITH 子句的函数，主要用于对 WITH 子句进行解析和分析。具体来说，它的作用包括：

检查 WITH 子句的合法性：

确保在一个查询层次中只能有一个 WITH 子句。
检查 WITH 子句中是否存在重复的 CTE 名称。
初始化 CTE：

为每个 CTE 进行初始化，标记为非递归，引用计数为零，并初始化其他相关标记。
如果 CTE 包含非查询语句（如插入、更新、删除等），则将 p_hasModifyingCTE 标记为 true。
处理递归 WITH 子句（WITH RECURSIVE）：

如果 WITH 子句是递归的，进行以下步骤：
创建一个 CteState 结构，用于在递归查询处理过程中传递状态信息。
构建依赖关系图，并对 CTE 进行拓扑排序，以确保没有循环依赖，并标记包含自引用的 CTE。
检查递归查询的有效性。
设置 ctenamespace，并按照安全的处理顺序将 CTE 添加到命名空间中。
按照拓扑排序的顺序对每个 CTE 进行解析。
处理非递归 WITH 子句：

如果 WITH 子句不是递归的，按照顺序对每个 CTE 进行解析，并添加到命名空间中。
返回 ctenamespace：

返回最终的 ctenamespace，其中包含了所有解析后的 CTE。
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
        cte->referenced_by_subquery = false;

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

 /*
 这段代码定义了 PostgreSQL 中处理 WITH 子句中每个 Common Table Expression (CTE) 的解析过程。以下是代码的主要功能和关键步骤的解释：

检查 CTE 是否已经被分析：

使用 AssertEreport 断言确保 CTE 还没有被解析过。如果 CTE 的查询已经是一个完整的查询树（Query），则会报告错误，因为该函数假定 CTE 查询还没有被解析。
解析 CTE 查询：

使用 parse_sub_analyze 函数对 CTE 的查询进行解析。
设置 CTE 的查询属性为解析后的查询树。
检查查询的合法性：

检查查询树的类型，确保是一个查询（Query），否则报告错误。
检查查询树是否包含 utility 语句，如果包含则报告错误。
限制数据修改的 WITH 子句位置：

如果 CTE 包含数据修改语句（INSERT、UPDATE、DELETE），则要求该 WITH 子句必须位于查询的顶层，以防止不明确的执行时机。
设置 canSetTag 标记：

对于 CTE 查询，将 canSetTag 标记设置为 false，因为 CTE 查询不会设置命令标签。
处理递归 CTE：

如果 CTE 是递归的，验证之前确定的输出列类型和排序与实际查询的结果是否匹配。
检查每个输出列的数据类型、类型修饰符和排序规则是否与之前的定义一致，如果不一致则报告错误。
提供相应的错误提示和错误位置信息，以帮助用户调查问题。
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

 /*
 
 这段代码是 PostgreSQL 中处理 WITH RECURSIVE 查询的一部分，具体负责分析 CTE 的输出列信息，包括列名、数据类型、类型修饰符和排序规则等，并确保递归 CTE 的输出列符合规范。同时，该代码还实现了构建 CTE 之间依赖关系图并进行拓扑排序的功能。

以下是主要函数的解释：

analyzeCTETargetList 函数：

该函数用于分析 CTE 查询的输出列信息。
检查并使用 CTE 的别名列表（如果存在）初始化 CTE 的列名，同时检查和设置每个输出列的数据类型、类型修饰符和排序规则。
对于递归 CTE，如果存在 "unknown" 类型的列，将其强制转换为 "text" 类型，以保持与 UNION 的结果类型一致。
makeDependencyGraph 函数：

该函数用于构建 CTE 之间的依赖关系图，并确保没有循环依赖。
遍历 CTE 列表，为每个 CTE 启动一个树遍历过程，检测 CTE 之间的依赖关系。
调用 TopologicalSort 函数对 CTE 进行拓扑排序，确保没有前向引用。
makeDependencyGraphWalker 函数：

该函数是树遍历函数，用于检测 CTE 之间的交叉引用和自引用。
通过递归地处理树的每个节点，收集内部引用的 CTE，以建立依赖关系。
确保递归过程中不会出现循环引用，即一个 CTE 引用了它自身。
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


 /*
 这段代码实现了一个拓扑排序算法，用于对 CTE 项进行排序，以确保不存在前向依赖关系。
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
 这段代码是 PostgreSQL 中用于检查 WITH RECURSIVE 查询是否符合规范的函数。以下是该函数的主要功能和步骤解释：

遍历每个 WITH RECURSIVE 项：

使用循环遍历 CteState 结构中的每个 CTE 项。
检查是否为递归项：

如果 CTE 不是递归项（cterecursive 为 false），则跳过该项的检查。
检查 CTE 的查询类型：

确保 CTE 的查询是一个 SELECT 语句，因为递归查询必须是 SELECT 语句。
检查 UNION 结构：

确保 SELECT 语句使用 UNION 运算符，且左右操作数分别为 non-recursive term 和 recursive term。
检查左操作数中的自引用：

在左操作数中检查是否包含自引用。设置 context 为 RECURSION_NONRECURSIVETERM，表示在左操作数中检查自引用。
使用 checkWellFormedRecursionWalker 函数遍历左操作数，确保不存在自引用。
检查右操作数中的自引用：

在右操作数中检查是否包含自引用。设置 context 为 RECURSION_OK，表示在右操作数中检查自引用。
使用 checkWellFormedRecursionWalker 函数遍历右操作数，确保存在且只有一个自引用。
检查 WITH 子句中的自引用：

如果 SELECT 语句有 WITH 子句，检查 WITH 子句中是否包含自引用。设置 context 为 RECURSION_SUBLINK。
使用 checkWellFormedRecursionWalker 函数遍历 WITH 子句，确保不存在自引用。
禁止其他修饰子：

禁止在 UNION 结构之上使用 ORDER BY、LIMIT、OFFSET 以及 FOR UPDATE/SHARE 修饰子。因为这些在递归查询中难以解释和实现。
报告错误：

如果发现递归查询不符合规范，报告相应的错误，包括但不限于查询类型错误、UNION 结构错误和其他修饰子的使用错误。
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
 * 这段代码是 PostgreSQL 中用于递归地遍历表达式树，检查 WITH RECURSIVE 查询中是否存在自引用的函数。以下是该函数的主要功能和步骤解释：

保存递归上下文：

保存当前递归上下文，以便在递归过程中进行恢复。
处理特殊情况：

如果节点为空，直接返回 false。
如果节点是 RangeVar（表示关系变量），检查是否是 CTE 的引用，如果是则报错。
处理 SelectStmt 节点：

如果节点是 SelectStmt（SELECT 语句），检查是否包含 WITH 子句。根据 WITH 子句的类型，分别处理 RECURSIVE 和非 RECURSIVE 情况。
调用 checkWellFormedSelectStmt 函数对 SelectStmt 进行检查。
处理 WithClause 节点：

防止 raw_expression_tree_walker 直接递归进 WITH 子句的内容，因为 WITH 子句的处理需要在特定的上下文下进行。
处理 JoinExpr 节点：

如果节点是 JoinExpr（JOIN 表达式），根据 JOIN 类型（INNER、LEFT、RIGHT、FULL）分别处理左右子树和连接条件。
对于 LEFT JOIN，在检查右子树之前设置上下文为 RECURSION_OUTERJOIN。
对于 FULL JOIN 和 RIGHT JOIN，在检查左子树之前和右子树之前分别设置上下文为 RECURSION_OUTERJOIN。
处理 SubLink 节点：

如果节点是 SubLink（子查询），设置上下文为 RECURSION_SUBLINK，然后递归处理子查询的主体和测试表达式。
调用 raw_expression_tree_walker：

对于其他节点类型，调用 raw_expression_tree_walker 递归处理子节点。
恢复递归上下文：

恢复保存的递归上下文。
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

这段代码是用于检查 WITH RECURSIVE 查询中 SELECT 语句的格式是否正确的函数。以下是主要功能和步骤的解释：

保存递归上下文：

保存当前递归上下文，以便在递归过程中进行恢复。
处理 SET 操作：

根据 stmt->op 的不同值（SETOP_NONE、SETOP_UNION、SETOP_INTERSECT、SETOP_EXCEPT），分别进行处理。
如果 stmt->op 为 SETOP_NONE 或 SETOP_UNION，或者递归上下文不是 RECURSION_OK，则调用 raw_expression_tree_walker 递归处理节点。
处理 INTERSECT 和 EXCEPT：

如果 stmt->op 为 SETOP_INTERSECT 或 SETOP_EXCEPT，并且 stmt->all 为真，设置递归上下文为 RECURSION_INTERSECT 或 RECURSION_EXCEPT。
对左子树和右子树进行递归检查，并在检查后恢复递归上下文。
分别对 stmt->sortClause、stmt->limitOffset、stmt->limitCount、stmt->lockingClause 进行递归检查。
stmt->withClause 在这里被故意忽略。
处理未知 SET 操作类型：

如果 stmt->op 不是已知的 SETOP 类型，报告错误。
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
