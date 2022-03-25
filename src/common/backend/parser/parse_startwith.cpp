/* -------------------------------------------------------------------------
 *
 * parse_startwith.cpp
 *    Implement start with related modules in transform state
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *    src/common/backend/parser/parse_startwith.cpp
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
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

/*
 ****************************************************************************************
 * @Brief: Oracle compatible START WITH...CONNECT BY support, define start with relevant
 *         data structures and routines
 *
 * @Note: The only entry point of START WITH in parser is transformStartWith() where
 *        all internal implementations are hiden behind
 *
 ****************************************************************************************
 */
typedef struct StartWithTransformContext {
    ParseState* pstate;
    List *relInfoList;
    List *where_infos;
    List *connectby_prior_name;
    Node *startWithExpr;
    Node *connectByExpr;
    Node *whereClause;
    List *siblingsOrderBy;
    bool is_where;
    StartWithConnectByType  connect_by_type;

    /*
     * for multiple pushdown case
     */
    List *fromClause;

    /*
     * connectByLevelExpr & connectByOtherExpr
     *
     * Normally consider performance to for connect by level/rownum, we extract the
     * level/rownum expr from connectByExpr
     *    - connectByOtherExpr: evaluated in RecursiveUnion's inner join cond
     *    - connectByLevelExpr: evaluated in StartWithOp's node
     *
     * e.g.
     *  connectByExpr: (level < 10) AND (t1.c1 > 1) AND (t1.c2 > 0)
     *  connectByLevelExpr: level < 10
     *  connectByOtherExpr: t1.c1 = 1 AND
     */
    Node *connectByLevelExpr;
    Node *connectByOtherExpr;
    bool nocycle;
} StartWithTransformContext;

typedef enum StartWithRewrite {
    SW_NONE = 0,
    SW_SINGLE,
    SW_FROMLIST,
    SW_JOINEXP,
} StaretWithRewrite;

typedef struct StartWithCTEPseudoReturnAtts {
    char    *colname;
    Oid      coltype;
    int32    coltypmod;
    Oid      colcollation;
} StartWithCTEPseudoReturnAtts;

static StartWithCTEPseudoReturnAtts g_StartWithCTEPseudoReturnAtts[] =
{
    {"level", INT4OID, -1, InvalidOid},
    {"connect_by_isleaf", INT4OID, -1, InvalidOid},
    {"connect_by_iscycle", INT4OID, -1, InvalidOid},
    {"rownum", INT4OID, -1, InvalidOid}
};

/* function parsing startWithExpr and connectByExpr */
static void transformStartWithClause(StartWithTransformContext *context, SelectStmt *stmt);

static List *expandAllTargetList(List *targetRelInfoList);

static List *ExtractColumnRefStartInfo(ParseState *pstate, ColumnRef *column, char *relname, char *colname,
                                       Node *preResult);
static void HandleSWCBColumnRef(StartWithTransformContext* context, Node *node);
static void StartWithWalker(StartWithTransformContext *context, Node *expr);
static void AddWithClauseToBranch(ParseState *pstate, SelectStmt *stmt, List *relInfoList);

static void transformSingleRTE(ParseState* pstate,
                                 Query* qry,
                                 StartWithTransformContext *context,
                                 Node *start_clause);
static void transformFromList(ParseState* pstate,
                                Query* qry,
                                StartWithTransformContext *context,
                                Node *n);
static RangeTblEntry *transformStartWithCTE(ParseState* pstate, List *prior_names);
static bool preSkipPLSQLParams(ParseState *pstate, ColumnRef *cref);


/* functions to make start-with converted CTE's left/right branch */
static SelectStmt *CreateStartWithCTEOuterBranch(ParseState* pstate,
                                                 StartWithTransformContext *context,
                                                 List *relInfoList,
                                                 Node *startWithExpr,
                                                 Node *whereClause);
static SelectStmt *CreateStartWithCTEInnerBranch(ParseState* pstate,
                                                     StartWithTransformContext *context,
                                                     List *relInfoList,
                                                     Node *connectByExpr,
                                                     Node *whereClause);
static void CreateStartWithCTE(ParseState *pstate,
                                   Query *qry,
                                   SelectStmt *initpart,
                                   SelectStmt *workpart,
                                   StartWithTransformContext *context);

static Node *makeBoolAConst(bool state, int location);
static StartWithRewrite ChooseSWCBStrategy(StartWithTransformContext context);
static Node *tryReplaceFakeValue(Node *node);

static Node *makeBoolAConst(bool state, int location)
{
    A_Const *constExpr = makeNode(A_Const);

    constExpr->val.type = T_String;
    constExpr->val.val.str = (char *)(state ? "t" : "f");
    constExpr->location = location;

    TypeCast *typecast = makeNode(TypeCast);
    typecast->arg = (Node *)constExpr;
    typecast->typname = makeTypeNameFromNameList(
                            list_make2(makeString("pg_catalog"),
                            makeString("bool")));
    typecast->location = location;

    return (Node *)typecast;
}

/*
 * ---------------------------------------------------------------------------------------
 *
 * Oracle Compatible "START WITH...CONNECT BY" support routines
 *
 * ---------------------------------------------------------------------------------------
 */
/*
 ****************************************************************************************
 *  transformStartWith -
 *
 *  @Brief: Start With primary entry point, all internal implementations are hiden behind
 *          processing implemantion are described as follow:
 *
 *  exec_simple_query()
 *          -> parse_and_analyze()
 *                  -> transformStmt()
 *                          -> transformSelectStmt() {... details described as below ...}
 *
 *  transformSelectStmt(pstate) {
 *      ...
 *      transformFromClause() {
 *          // identify START WITH...CONNECT BY's target relation and create a relevant
 *          // StartWithTargetRelInfo put them in parse->p_start_info and later processed in top
 *          // level transformStartWith()
 *      }
 *      ...
 *      transformStartWith() {
 *          // 1st. parse startWithExpr/connectByExpr
 *          transformStartWithClause();
 *
 *          // 2nd. convert a START WITH...CONNECT BY object into a recursive CTE
 *          transformSingleRTE() {
 *              // 2.1 construct CTE's lefttree, a.w.k non-recursive term
 *              CreateStartWithCTEOuterBranch();
 *
 *              // 2.2 construct CTE's righttree, a.w.k recursive term
 *              CreateStartWithCTEInnerBranch();
 *
 *              // 2.3 construct start with converted CTE object
 *              CreateStartWithCTE();
 *
 *              // 2.4 transform start with CTE normally as we did for a regular
 *              //     with-recursive case
 *              transformStartWithCTE();
 *          }
 *      }
 *   }
 *
 ****************************************************************************************
 */
void transformStartWith(ParseState *pstate, SelectStmt *stmt, Query *qry)
{
    ListCell *lc = NULL;
    StartWithTransformContext context;

    if (pstate->p_start_info == NULL) {
        ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("Not found correct element in fromclause could be rewrited by start with/connect by."),
               errdetail("Only support table and subquery in fromclause can be rewrited.")));
    }

    errno_t rc = memset_s(&context, sizeof(StartWithTransformContext),
                0, sizeof(StartWithTransformContext));
    securec_check(rc, "\0", "\0");

    context.pstate = pstate;
    context.relInfoList = NULL;
    context.connectby_prior_name = NULL;

    transformStartWithClause(&context, stmt);
    pstate->p_hasStartWith = true;

    int lens = list_length(pstate->p_start_info);
    if (lens != 1) {
        context.relInfoList = pstate->p_start_info;
        context.fromClause = pstate->sw_fromClause;
    }

    /* set result-RTEs as replaced flag */
    foreach(lc, context.relInfoList) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc);
        info->rte->swAborted = true;
    }

    /* Choose SWCB Rewrite Strategy. */
    StartWithRewrite op = ChooseSWCBStrategy(context);
    if (op == SW_SINGLE) {
        transformSingleRTE(pstate, qry, &context, (Node *)stmt->startWithClause);
    } else {
        transformFromList(pstate, qry, &context, (Node *)stmt->startWithClause);
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: SWCB's helper function in transform stage, here we create StartWithInfo object
 *         to each "start with" applied FromClauseItem and insert it into pastate,
 *         normally we are handling following case:
 *              - RangeVar(baserel)
 *              - RangeSubSelect (subquery)
 *              - Function and CTE will considered in the future
 *
 * @Param
 *    - pstate: paser stage
 *    - n: one FromClauseItem
 *    - rte: the FromClauseItem relevant RTE
 *    - rte: the FromClauseItem relevant RTR
 *
 * @Return: none
 * --------------------------------------------------------------------------------------
 */
void AddStartWithTargetRelInfo(ParseState* pstate, Node* relNode,
                RangeTblEntry* rte, RangeTblRef *rtr)
{
    StartWithTargetRelInfo *startInfo = makeNode(StartWithTargetRelInfo);

    if (IsA(relNode, RangeVar)) {
        RangeVar* rv = (RangeVar*)relNode;

        startInfo->relname = rv->relname;
        if (rv->alias != NULL) {
            startInfo->aliasname = rv->alias->aliasname;
        }
        startInfo->rte = rte;
        startInfo->rtr = rtr;
        startInfo->rtekind = rte->rtekind;

        RangeVar *tbl = (RangeVar *)copyObject(relNode);

        startInfo->tblstmt = (Node *)tbl;
        startInfo->columns = rte->eref->colnames;
    } else if (IsA(relNode, RangeSubselect)) {
        RangeSubselect *sub = (RangeSubselect *)relNode;

        /* name __unnamed_subquery__ */
        if (pg_strcasecmp(sub->alias->aliasname, "__unnamed_subquery__") != 0) {
            startInfo->aliasname = sub->alias->aliasname;
        } else {
            char swname[NAMEDATALEN];
            int rc = snprintf_s(swname, sizeof(swname), sizeof(swname) - 1,
                                "sw_subquery_%d", pstate->sw_subquery_idx);
            securec_check_ss(rc, "", "");
            pstate->sw_subquery_idx++;

            sub->alias->aliasname = pstrdup(swname);
            startInfo->aliasname = sub->alias->aliasname;
        }

        SelectStmt *substmt = (SelectStmt*)sub->subquery;
        if (substmt->withClause == NULL) {
            substmt->withClause = (WithClause*)copyObject(pstate->origin_with);
        }

        startInfo->rte = rte;
        startInfo->rtr = rtr;
        startInfo->rtekind = rte->rtekind;
        startInfo->tblstmt = relNode;
        startInfo->columns = rte->eref->colnames;
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Not support unrecognized node type: %d %s once AddStartWithInfo.",
                (int)nodeTag(relNode), nodeToString(relNode))));
    }

    pstate->p_start_info = lappend(pstate->p_start_info, startInfo);

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Generate dummy target name for start with converted RTE, relation is renamed
 *         as "t1" -> "tmp_result", "t1.c1" -> "tmp_result@c1"
 *
 * @param:
 *      - alias: base cte's ctename
 *      - colname: base table's attr name
 *
 * @Return: a copied string version of dummy-colname
 * --------------------------------------------------------------------------------------
 */
char *makeStartWithDummayColname(char *alias, char *column)
{
    errno_t rc;
    char *result = NULL;
    char *split = "@";
    char dumy_name[NAMEDATALEN];

    int total = strlen(alias) + strlen(column) + strlen(split);
    if (total >= NAMEDATALEN) {
        ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Exceed maximum StartWithDummayColname length %d, relname %s, column %s.",
                total, alias, column)));
    }

    rc = memset_s(dumy_name, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check_c(rc, "\0", "\0");
    rc = strncat_s(dumy_name, NAMEDATALEN, alias, strlen(alias));
    securec_check_c(rc, "\0", "\0");

    /* add split operator @ */
    rc = strncat_s(dumy_name, NAMEDATALEN, split, strlen(split));
    securec_check_c(rc, "\0", "\0");

    rc = strncat_s(dumy_name, NAMEDATALEN, column, strlen(column));
    securec_check_c(rc, "\0", "\0");

    result = pstrdup(dumy_name);

    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: to be added
 *
 * @param:
 *
 * @Return: to be add
 * --------------------------------------------------------------------------------------
 */
void AdaptSWSelectStmt(ParseState *pstate, SelectStmt *stmt)
{
    ListCell *lc = NULL;
    List *tbls = NULL;

    foreach(lc, pstate->p_start_info) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc);

        if (!info->rte->swAborted) {
            tbls = lappend(tbls, info->tblstmt);
        }
    }

    RangeVar *cte = makeRangeVar(NULL, "tmp_reuslt", -1);
    tbls = lappend(tbls, cte);

    stmt->fromClause = tbls;

    return;
}

bool IsSWCBRewriteRTE(RangeTblEntry *rte)
{
    bool result = false;

    if (rte->rtekind != RTE_CTE) {
        return result;
    }

    if (pg_strcasecmp(rte->ctename, "tmp_reuslt") != 0) {
        return result;
    }

    result = rte->swConverted;
    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: to be added
 *
 * @param:
 *
 * @Return: to be add
 * --------------------------------------------------------------------------------------
 */
bool IsQuerySWCBRewrite(Query *query)
{
    bool isSWCBRewrite = false;
    ListCell *lc = NULL;

    foreach(lc, query->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

        /*
         * Only RTE_CTE chould be rerwite startwith CTE, others skip.
         * */
        if (rte->rtekind != RTE_CTE) {
            continue;
        }

        if (pg_strcasecmp(rte->ctename, "tmp_reuslt") == 0) {
            isSWCBRewrite = true;
            break;
        }
    }

    return isSWCBRewrite;
}

static StartWithRewrite ChooseSWCBStrategy(StartWithTransformContext context)
{
    StartWithRewrite op = SW_NONE;
    int info_lens = list_length(context.relInfoList);

    /*
     * Only one StartWithInfos is SW_SINGLE, and multiples stands for
     * SW_FROMLIST Strategy. Others is coming.
     */
    if (info_lens == 1) {
        op = SW_SINGLE;
    } else {
        op = SW_FROMLIST;
    }

    return op;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: pre-check PLSQL input/output columns same as transformColumnRef
 *
 * @param:
 *
 * @Return: skip these columns during startWithWalker or not
 * --------------------------------------------------------------------------------------
 */
static bool preSkipPLSQLParams(ParseState *pstate, ColumnRef *cref)
{
    Node* node = NULL;

    /*
     * Give the PreParseColumnRefHook, if any, first shot.  If it returns
     * non-null then that's all, folks.
     */
    if (pstate->p_pre_columnref_hook != NULL) {
        node = (*pstate->p_pre_columnref_hook)(pstate, cref);
        if (node != NULL) {
            return true;
        }
    }

    if (pstate->p_post_columnref_hook != NULL) {
        Node* hookresult = NULL;

        /*
         * if node is not a table column, we should pass a null to the hook,
         * or it will misjudge the columnref as ambiguous.
         */
        hookresult = (*pstate->p_post_columnref_hook)(pstate, cref, node);
        if (hookresult != NULL) {
            return true;
        }
    }

    return false;
}

/**
 * @param preResult Var struct
 * @return true if is a upper query column
 */
static inline bool IsAUpperQueryColumn(Node *preResult)
{
    if (IsA(preResult, Var)) {
        Var *varNode = (Var *)preResult;
        if (!IS_SPECIAL_VARNO(varNode->varno) && varNode->varlevelsup >= 1) {
            return true;
        }
    }
    return false;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: to be input
 *
 * @param: to be input
 *
 * @Return: to be input
 * --------------------------------------------------------------------------------------
 */
static List *ExtractColumnRefStartInfo(ParseState *pstate, ColumnRef *column, char *relname, char *colname,
                                       Node *preResult)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    List *extract_infos = NIL;

    foreach(lc1, pstate->p_start_info) {
        StartWithTargetRelInfo *start_info = (StartWithTargetRelInfo *)lfirst(lc1);

        bool column_exist = false;
        /*
         * relname not exist, then check column names,
         * otherwise relanme exist, then check StartWithInfo's relname/aliasname directly.
         */
        if (relname == NULL) {
            foreach(lc2, start_info->columns) {
                Value *val = (Value *)lfirst(lc2);
                if (strcmp(colname, strVal(val)) == 0) {
                    column_exist = true;
                    break;
                }
            }
        } else {
            if (start_info->aliasname != NULL &&
                        strcmp(start_info->aliasname, relname) == 0) {
                column_exist = true;
            } else if (start_info->relname != NULL &&
                        strcmp(start_info->relname, relname) == 0) {
                column_exist = true;
            } else {
                column_exist = false;
            }
        }

        /* handle with sub startwith cte targetlist */
        if (start_info->rte->swSubExist) {
            Assert(start_info->rtekind == RTE_SUBQUERY);

            foreach(lc2, start_info->columns) {
                Value *val = (Value *)lfirst(lc2);
                if (strstr(strVal(val), colname)) {
                    column_exist = true;
                    colname = pstrdup(strVal(val));
                    break;
                }
            }
        }

        if (column_exist) {
            extract_infos = lappend(extract_infos, start_info);

            if (start_info->rtekind == RTE_RELATION) {
                relname = start_info->aliasname ? start_info->aliasname : start_info->relname;
            } else if (start_info->rtekind == RTE_SUBQUERY) {
                relname = start_info->aliasname;
            } else if (start_info->rtekind == RTE_CTE) {
                relname = start_info->aliasname ? start_info->aliasname : start_info->relname;
            }

            column->fields = list_make2(makeString(relname), makeString(colname));
        }
    }

    if (list_length(extract_infos) > 1) {
        ereport(ERROR,
               (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                errmsg("column reference \"%s\" is ambiguous.", colname)));
    }

    if (list_length(extract_infos) == 0 && !IsAUpperQueryColumn(preResult)) {
        ereport(WARNING,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("Cannot match table with startwith/connectby column %s.%s, maybe it is a upper query column",
                      relname, colname)));
    }

    return extract_infos;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: A helper function invoked under startWithWalker(), where transform ConnectBy
 *         or StartWith expression, reach a ColumnRef node, we need evaluate it into its
 *         "baserel->cte" format by call makeStartWithDummayColname()
 *
 * @param:
 *     - context: the startwith conversion context
 *     - node: the ColumnRef node, basically we need assum it is with type of ColumnRef
 *
 * @Return: none
 * --------------------------------------------------------------------------------------
 */
static void HandleSWCBColumnRef(StartWithTransformContext *context, Node *node)
{
    List *result = NIL;
    char* relname = NULL;
    char* colname = NULL;
    ParseState *pstate = context->pstate;

    if (node == NULL) {
        return;
    }

    Assert(nodeTag(node) == T_ColumnRef);
    ColumnRef *column = (ColumnRef *)node;
    bool prior = column->prior;
    char *initname = strVal(linitial(column->fields));

    if (pg_strcasecmp(initname, "tmp_reuslt") == 0) {
        return;
    }

    /* Skip params in procedure */
    if (preSkipPLSQLParams(pstate, column) && !prior) {
        return;
    }

    ColumnRef *preColumn = (ColumnRef *)copyObject(column);
    Node *preResult = transformColumnRef(pstate, preColumn);
    if (preResult == NULL) {
        return;
    }

    int len = list_length(column->fields);
    switch (len) {
        case 1: {
            Node* field1 = (Node*)linitial(column->fields);
            colname = strVal(field1);

            /*
             * In-place replace columnref with two fields, like convert id to test.id,
             * which will be helpful for after SWCB rewrite. So here columnref will contain
             * two fields after ExtractColumnRefStartInfo.
             */
            result = ExtractColumnRefStartInfo(pstate, column, NULL, colname, preResult);

            if (prior) {
                char *dummy = makeStartWithDummayColname(
                                            strVal(linitial(column->fields)),
                                            strVal(lsecond(column->fields)));

                column->fields = list_make2(makeString("tmp_reuslt"), makeString(dummy));

                /* record working table name */
                context->connectby_prior_name = lappend(context->connectby_prior_name,
                                                        makeString(dummy));
            }

            break;
        }
        case 2: {
            Node* field1 = (Node*)linitial(column->fields);
            Node* field2 = (Node*)lsecond(column->fields);
            relname = strVal(field1);
            colname = strVal(field2);

            result = ExtractColumnRefStartInfo(pstate, column, relname, colname, preResult);

            if (prior) {
                char *dummy = makeStartWithDummayColname(strVal(linitial(column->fields)),
                                                         strVal(lsecond(column->fields)));
                column->fields = list_make2(makeString("tmp_reuslt"), makeString(dummy));

                /* record working table name */
                context->connectby_prior_name = lappend(context->connectby_prior_name,
                                                        makeString(dummy));
            }

            break;
        }
        default: {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Not support %d column lengths when HandleSWCBColumnRef", len)));
        }
    }

    if (context->is_where) {
        context->where_infos = list_concat_unique(context->where_infos, result);
    } else {
        context->relInfoList = list_concat_unique(context->relInfoList, result);
    }

    return;
}

static bool is_cref_by_name(Node *expr, const char* col_name)
{
    if (expr == NULL || !IsA(expr, ColumnRef)) {
        return false;
    }
    ColumnRef *cref = (ColumnRef *) expr;
    char* colname = NameListToString(cref->fields);
    bool ret = (strcasecmp(colname, col_name) == 0) ? true : false;
    pfree(colname);
    return ret;
}

static Node* makeIntConst(int val, int location)
{
    A_Const *n = makeNode(A_Const);

    n->val.type = T_Integer;
    n->val.val.ival = val;
    n->location = location;

    return (Node *)n;
}

static Node *replaceListFakeValue(List *lst)
{
    List *newArgs = NIL;
    ListCell *lc = NULL;
    bool replaced = false;

    /* replace level/rownum var attr with fake consts */
    foreach (lc, lst) {
        Node *n = (Node *)lfirst(lc);
        Node *newNode = tryReplaceFakeValue(n);
        if (newNode != n) {
            replaced = true;
            n = newNode;
        }
        newArgs = lappend(newArgs, n);
    }
    return replaced ? (Node *)newArgs : (Node *)lst;
}

static Node *tryReplaceFakeValue(Node *node)
{
    if (node == NULL) {
        return node;
    }

    if (IsA(node, Rownum)) {
        node = makeIntConst(CONNECT_BY_ROWNUM_FAKEVALUE, -1);
    } else if (is_cref_by_name(node, "level")) {
        node = makeIntConst(CONNECT_BY_LEVEL_FAKEVALUE, -1);
    } else if (IsA(node, List)) {
        node = replaceListFakeValue((List *) node);
    }
    return node;
}

static bool pseudo_level_rownum_walker(Node *node, Node *context_parent_node)
{
    if (node == NULL) {
        return false;
    }

    Node* newNode = tryReplaceFakeValue(node);

    /* Case 1: Fake value replacement did not occur. Return immediately. */
    if (newNode == node) {
        return raw_expression_tree_walker(node, (bool (*)()) pseudo_level_rownum_walker, node);
    }

    /* Case 2: Fake value replacement occurred. Need to update parent node */
    switch (nodeTag(context_parent_node)) {
        case T_A_Expr: {
            A_Expr *expr = (A_Expr *) context_parent_node;
            if (expr->lexpr == node) {
                expr->lexpr = newNode;
            } else {
                expr->rexpr = newNode;
            }
            break;
        }
        case T_TypeCast: {
            TypeCast *tc = (TypeCast *) context_parent_node;
            tc->arg = newNode;
            break;
        }
        case T_NullTest: {
            NullTest *nt = (NullTest *) context_parent_node;
            nt->arg = (Expr*) newNode;
            break;
        }
        case T_FuncCall: {
            FuncCall *fc = (FuncCall *) context_parent_node;
            fc->args = (List *)newNode;
            break;
        }
        default:
            break;
    }
    return raw_expression_tree_walker(node, (bool (*)()) pseudo_level_rownum_walker, node);
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: SWCB's expr processing function, normally we do tow kinds of process
 *    (1). Recursively find a base column and do column name normalization for example:
 *         c1 -> t1.c1 aiming to provide more detail for later process
 *    (2). Find a const and identify connect-by-type ROWNUM/LEVEL
 *
 * @param
 *    - context: SWCB expression processing context, hold in-processing states and some
 *               processign result
 *    - expr: expression node
 *
 * @return: none, some of information is returned under *context
 * --------------------------------------------------------------------------------------
 */
static void StartWithWalker(StartWithTransformContext *context, Node *expr)
{
    if (expr == NULL) {
        return;
    }

    check_stack_depth();

    switch (nodeTag(expr)) {
        case T_ColumnRef: {
            ColumnRef* colRef = (ColumnRef*)expr;
            HandleSWCBColumnRef(context, (Node *)colRef);
            break;
        }

        case T_NullTest: {
            NullTest* nullTestExpr = (NullTest*)expr;
            Node* arg = (Node *)nullTestExpr->arg;
            if (arg != NULL && nodeTag(arg) == T_ColumnRef) {
                HandleSWCBColumnRef(context, arg);
            } else {
                StartWithWalker(context, arg);
            }
            break;
        }

        case T_SubLink: {
            SubLink *sublink = (SubLink *)expr;
            Node *testexpr = sublink->testexpr;
            if (testexpr != NULL && nodeTag(testexpr) == T_ColumnRef) {
                HandleSWCBColumnRef(context, testexpr);
            } else {
                StartWithWalker(context, testexpr);
            }
            break;
        }

        case T_CollateClause: {
            CollateClause *cc = (CollateClause*) expr;
            StartWithWalker(context, cc->arg);
            break;
        }

        case T_TypeCast: {
            TypeCast* tc = (TypeCast*)expr;
            StartWithWalker(context, tc->arg);
            break;
        }

        case T_List: {
            List* l = (List*)expr;
            ListCell* lc = NULL;
            foreach (lc, l) {
                StartWithWalker(context, (Node*)lfirst(lc));
            }
            break;
        }

        case T_FuncCall: {
            ListCell *args = NULL;
            FuncCall* fn = (FuncCall *)expr;

            foreach (args, fn->args) {
                StartWithWalker(context, (Node*)lfirst(args));
            }
            break;
        }

        case T_A_Indirection: {
            A_Indirection* idn = (A_Indirection *)expr;
            StartWithWalker(context, idn->arg);
            break;
        }

        case T_A_Expr: {
            A_Expr *a_expr = (A_Expr *)expr;

            switch (a_expr->kind) {
                case AEXPR_OP: {
                    Node *left_expr = a_expr->lexpr;
                    Node *right_expr = a_expr->rexpr;

                    if (left_expr != NULL && (is_cref_by_name(left_expr, "level") ||
                            IsA(left_expr, Rownum))) {
                        context->connect_by_type = CONNECT_BY_MIXED_LEVEL;
                        context->connectByLevelExpr = (Node *)copyObject(a_expr);
                        a_expr->kind = AEXPR_OR;
                        a_expr->lexpr = makeBoolAConst(true, -1);
                        a_expr->rexpr = makeBoolAConst(true, -1);
                        A_Expr* cble = (A_Expr*) context->connectByLevelExpr;
                        A_Const *n = makeNode(A_Const);
                        n->val.type = T_Integer;
                        n->val.val.ival = IsA(left_expr, Rownum) ?
                            CONNECT_BY_ROWNUM_FAKEVALUE : CONNECT_BY_LEVEL_FAKEVALUE;
                        n->location = -1;
                        cble->lexpr = (Node*) n;
                        break;
                    }

                    /* convert right/left expr surround AEXPR_OP */
                    if (left_expr != NULL && nodeTag(left_expr) == T_ColumnRef) {
                        HandleSWCBColumnRef(context, left_expr);
                    } else {
                        StartWithWalker(context, left_expr);
                    }

                    if (right_expr != NULL && nodeTag(right_expr) == T_ColumnRef) {
                        HandleSWCBColumnRef(context, right_expr);
                    } else {
                        StartWithWalker(context, right_expr); 
                    }

                    break;
                }
                case AEXPR_AND:
                case AEXPR_OR: {
                    /* deep copy rexpr before walking into lexpr:
                       this is to prevent lexpr transformation procedures from tweaking rexpr
                       in cases when lexpr and rexpr share common nodes,
                       e.g. in 'between ... and ...' clauses duplicated columnrefs are found
                       in both lexpr and rexpr */
                    a_expr->rexpr = (Node*) copyObject(a_expr->rexpr);
                    StartWithWalker(context, a_expr->lexpr);
                    StartWithWalker(context, a_expr->rexpr);
                    break;
                }
                case AEXPR_IN: {
                    StartWithWalker(context, a_expr->lexpr);
                    StartWithWalker(context, a_expr->rexpr);
                    break;
                }
                case AEXPR_NOT: {
                    StartWithWalker(context, a_expr->rexpr);
                    break;
                }
                default: {
                    ereport(ERROR,
                        (errmodule(MOD_PARSER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported expression found in START WITH / CONNECT BY clause."),
                            errdetail("Unsupported expr type: %d.", (int)a_expr->kind),
                            errcause("Unsupported expression in START WITH / CONNECT BY clause."),
                            erraction("Check and revise your query or contact Huawei engineers.")));
                    break;
                }
            }
            break;
        }

        case T_A_Const: {
            A_Const *n = (A_Const *)expr;

            int32 val = GetStartWithFakeConstValue(n);
            if (val == CONNECT_BY_LEVEL_FAKEVALUE) {
                context->connect_by_type = CONNECT_BY_LEVEL;
            } else if (val == CONNECT_BY_ROWNUM_FAKEVALUE) {
                context->connect_by_type = CONNECT_BY_ROWNUM;
            }

            break;
        }

        case T_ParamRef: {
            break;
        }

        default: {
            ereport(ERROR,
                (errmodule(MOD_PARSER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported expression found in START WITH / CONNECT BY clause."),
                    errdetail("Unsupported node type: %d.", (int)nodeTag(expr)),
                    errcause("Unsupported expression in START WITH / CONNECT BY clause."),
                    erraction("Check and revise your query or contact Huawei engineers.")));
            break;
        }

    }

    return;
}

static bool isForbiddenClausesPresent(SelectStmt *stmt)
{
    ListCell* cell = NULL;
    foreach (cell, stmt->fromClause) {
        Node* n = (Node*)lfirst(cell);
        if (IsA(n, RangeTimeCapsule) || IsA(n, RangeTableSample)) {
            return true;
        }
    }
    return false;
}

static void checkConnectByExprValidity(Node* connectByExpr)
{
    Node* node = tryReplaceFakeValue(connectByExpr);
    if (node != connectByExpr) {
        ereport(ERROR,
            (errmodule(MOD_PARSER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupported expression found in CONNECT BY clause."),
                errdetail("Pseudo column expects an operator"),
                errcause("Unsupported expression in CONNECT BY clause."),
                erraction("Check and revise your query or contact Huawei engineers.")));
    }
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: transfrom startwith/connectby clasue
 *     1. For startwith we need to replace column name, return info lists
 *     2. For connect by we should replace column name(include table name and work table
 *        tmp_result name) and also record prior name then return ino lists.
 *
 * @param:
 *     - context: global SWCB conversion context
 *     - stmt: current SWCB's belonging SelectStmt
 *
 * @Return: to be input
 * --------------------------------------------------------------------------------------
 */
static void transformStartWithClause(StartWithTransformContext *context, SelectStmt *stmt)
{
    if (stmt == NULL) {
        return;
    }

    if (isForbiddenClausesPresent(stmt)) {
        ereport(ERROR,
            (errmodule(MOD_PARSER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Cannot use start with / connect by with TABLESAMPLE / TIMECAPSULE."),
                errdetail("Unsupported target type"),
                errcause("Unsupported target type in START WITH / CONNECT BY clause."),
                erraction("Check and revise your query or contact Huawei engineers.")));
    }
 
    StartWithClause *clause = (StartWithClause *)stmt->startWithClause;
    Node *startWithExpr = clause->startWithExpr;
    Node *connectByExpr = clause->connectByExpr;
    context->connectByExpr = connectByExpr;
    context->startWithExpr = startWithExpr;
    context->connect_by_type = CONNECT_BY_PRIOR;
    context->nocycle = clause->nocycle;
    context->siblingsOrderBy = (List *)clause->siblingsOrderBy;

    /* Handling CONNECT BY ROWNUM / LEVEL */
    if (startWithExpr == NULL) {
        raw_expression_tree_walker((Node*)context->connectByExpr,
            (bool (*)())pseudo_level_rownum_walker, (Node*)context->connectByExpr);
        checkConnectByExprValidity((Node*)connectByExpr);
        StartWithWalker(context, connectByExpr);
        context->relInfoList = context->pstate->p_start_info;
        if (context->connect_by_type != CONNECT_BY_PRIOR) {
            context->connectByLevelExpr = connectByExpr;
            context->connectByOtherExpr = NULL;
        }
    }

    /* transform start with ... connect by's expr */
    StartWithWalker(context, startWithExpr);
    if (startWithExpr != NULL) {
        StartWithWalker(context, connectByExpr);
    }

    /*
     * now handle where quals which might need to be pushed down.
     * 1. this is necessary only for implicitly joined tables
     *    such as ... FROM t1,t2 WHERE t1.xx = t2.yy ...
     * 2. note that only join quals should be pushed down while
     *    non-join quals such as (t1.xx < n) should be kept in the outer loop
     *    of the CTE scan, otherwise the end-result will be different from
     *    those produced by the standard swcb syntax.
     * (the issue here is that implicitly joined tables are difficult to handle
     *  in our implementation of start with .. connect by .. syntax,
     *  as we don't have a clear cut of join quals from the non-join quals at this stage.
     *  users should be encouraged to use explicity joined tables whenever
     *  possible before a clear-cut solution is implemented.)
     */
    int lens = list_length(context->pstate->p_start_info);
    if (lens != 1) {
        Node *whereClause = (Node *)copyObject(stmt->whereClause);
        context->whereClause = whereClause;
    }


    if (startWithExpr != NULL && context->connectby_prior_name == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("START WITH CONNECT BY clauses must have at least one prior key.")));
    }
    Assert(context->relInfoList);
    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Add SWCB's pseudo column to basereal converted CTE object, we do so is want to
 *         consider level/rownum/iscycle/isleaf is SWCB's return column, so that follow
 *         up steps could keep their origin
 *
 * @param:
 *     - cte: cte of SWCB converted
 *     - rte: rte of SWCB converted that pointing cte in current transform level
 *     - rte_index: rte's rt_index, a.w.k. varno, rt_fetch() 
 *
 * @Return: none
 * --------------------------------------------------------------------------------------
 */
void AddStartWithCTEPseudoReturnColumns(CommonTableExpr *cte,
       RangeTblEntry *rte, Index rte_index)
{
    Assert (rte->rtekind == RTE_CTE);

    /* Add and fix target entry for pseudo columns */
    /* step 1. fix ctequery's output */
    Query *ctequery = (Query *)cte->ctequery;

    /* Add pseudo output column into rte->eref->colnames */
    Node        *expr = NULL;
    TargetEntry *tle = NULL;
    StartWithCTEPseudoReturnAtts *att = NULL;
    bool pseudoExist = false;
    ListCell *lc = NULL;

    foreach(lc, ctequery->targetList) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);
 
        if (IsPseudoReturnTargetEntry(entry)) {
            pseudoExist = true;
            break;
        }
    }

    if (pseudoExist) {
        return;
    }

    for (uint i = 0; i < STARTWITH_PSEUDO_RETURN_ATTNUMS; i++) {
        /* Create a pseudo return column in "TargetEntry" format */
        att = &g_StartWithCTEPseudoReturnAtts[i];

        /* make var for pseudo return column */
        expr = (Node *)makeVar(rte_index,
                               list_length(ctequery->targetList) + 1,
                               att->coltype,
                               att->coltypmod,
                               att->colcollation, 0);

        /* make pseudo return column's TLE */
        tle = makeTargetEntry((Expr *)expr, list_length(ctequery->targetList) + 1, att->colname, false);

        /* Add the pseudo return column to CTE's target list */
        ctequery->targetList = lappend(ctequery->targetList, tle);

        /* Fix cte's relevant output data structure */
        rte->eref->colnames = lappend(rte->eref->colnames, makeString(att->colname));
        rte->ctecoltypes = lappend_oid(rte->ctecoltypes, att->coltype);
        rte->ctecoltypmods = lappend_int(rte->ctecoltypmods, att->coltypmod);
        rte->ctecolcollations = lappend_oid(rte->ctecolcollations, att->colcollation);
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: A helper function invoked in CreateStartWithCteInner/OuterBranch() where we
 *         add SWCB's information ot inner/outer subselect
 *
 * @param:
 *    - (1). pstate: parse states of current level
 *    - (2). stmt: WithClause's branch SelectStmt
 *    - (3). relInfoList: current SWCB's constitute target rels
 *
 * @Return: none
 * --------------------------------------------------------------------------------------
 */
static void AddWithClauseToBranch(ParseState *pstate, SelectStmt *stmt, List *relInfoList)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    List *ctes = NIL;

    if (pstate->origin_with == NULL) {
        return;
    }

    WithClause *clause = (WithClause *)copyObject(pstate->origin_with);

    foreach(lc1, relInfoList) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc1);
        CommonTableExpr *removed = NULL;

        if (info->rtekind != RTE_CTE) {
            continue;
        }

        foreach(lc2, clause->ctes) {
            CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc2);

            if (pg_strcasecmp(cte->ctename, info->relname) == 0) {
                ctes = lappend(ctes, cte);
            }
        }

        foreach(lc2, pstate->p_ctenamespace) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc2);
 
            if (pg_strcasecmp(cte->ctename, info->relname) == 0) {
                removed = cte;
                break;
            }
        }

        if (removed != NULL) {
            pstate->p_ctenamespace = list_delete(pstate->p_ctenamespace, removed);
        }
    }

    if (ctes == NULL) {
        return;
    }

    clause->ctes = ctes;
    stmt->withClause = clause;

    return;
}

static bool walker_to_exclude_non_join_quals(Node *node, Node *context_node)
{
    if (node == NULL) {
        return false;
    }

    if (!IsA(node, A_Expr)) {
        return raw_expression_tree_walker(node, (bool (*)()) walker_to_exclude_non_join_quals, (void*)NULL);
    }

    A_Expr* expr = (A_Expr*) node;
    /*
     * this is to achieve consistent result sets with those produced by the original
     * start with .. connect by syntax, which does not push filter quals down to connect quals.
     * if non-column item appears on any side of an operator, we guess that it is
     * not a join qual so should not be filtered in sw op, and force it to be true.
     * this rule is not always correct but should work fine most of the time.
     * could be improved later on, e.g. find better ways to extract non-join quals
     * from the where clause.
     */
    if (expr->kind == AEXPR_OP &&
        (!IsA(expr->lexpr, ColumnRef) || !IsA(expr->rexpr, ColumnRef))) {
        expr->lexpr = makeBoolAConst(true, -1);
        expr->rexpr = makeBoolAConst(true, -1);
        expr->kind = AEXPR_OR;
    }

    return false;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Create SWCB's conversion CTE's inner branch, normally we add ConnectByExpr to
 *         inner branch's joincond
 *
 * @param:
 *    - (1). pstate, current SWCB belonging query level's parseState
 *    - (2). context current SWCB's trnasform context
 *    - (3). relInfoList, SWCB's relating base RangeVar
 *    - (4). connectByExpr: expr of connectby clause will be put as RQ's join cond
 *    - (5). whereClause: expr of where clause
 *
 * @Return: to be input
 * --------------------------------------------------------------------------------------
 */
static SelectStmt *CreateStartWithCTEInnerBranch(ParseState* pstate,
                        StartWithTransformContext *context,
                        List *relInfoList,
                        Node *connectByExpr,
                        Node *whereClause)
{
    Node *origin_table = NULL;
    SelectStmt *result = makeNode(SelectStmt);
    JoinExpr *join = makeNode(JoinExpr);
    RangeVar *work_table = makeRangeVar(NULL, "tmp_reuslt", -1);

    AddWithClauseToBranch(pstate, result, relInfoList);

    if (list_length(relInfoList) == 1) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)linitial(relInfoList);
        origin_table = (Node *)copyObject(info->tblstmt);
    } else {
        ListCell *lc = NULL;

        List *fromClause = (List *)copyObject(context->fromClause);
        foreach(lc, fromClause) {
            Node *node = (Node *)lfirst(lc);

            if (origin_table == NULL) {
                origin_table = node;
                continue;
            }

            /* make new JoinExpr */
            Node *qual = makeBoolAConst(true, -1);
            JoinExpr *joiniter = makeNode(JoinExpr);

            joiniter->larg = origin_table;
            joiniter->rarg = node;
            joiniter->quals = qual;

            origin_table = (Node *)joiniter;
        }

        if (whereClause != NULL) {
            JoinExpr *final_join = (JoinExpr *)origin_table;
            /* pushdown requires deep copying of the quals */
            Node *whereCopy = (Node *)copyObject(whereClause);
            /* only join quals can be pushed down */
            raw_expression_tree_walker((Node *)whereCopy,
                (bool (*)())walker_to_exclude_non_join_quals, (void*)NULL);

            if (final_join->quals == NULL) {
                final_join->quals = whereCopy;
            } else {
                final_join->quals =
                    (Node *)makeA_Expr(AEXPR_AND, NULL, whereCopy, final_join->quals, -1);
            }
        }
    }

    /* process regular/level */
    switch (context->connect_by_type) {
        case CONNECT_BY_PRIOR: {
            join->jointype = JOIN_INNER;
            join->isNatural = FALSE;
            join->larg = (Node *)work_table;
            join->rarg = origin_table;
            join->usingClause = NIL;
            join->quals = (Node *)copyObject(connectByExpr);

            result->targetList = expandAllTargetList(relInfoList);
            result->fromClause = list_make1(join);

            break;
        }
        case CONNECT_BY_ROWNUM:
        case CONNECT_BY_LEVEL: {
            join->jointype = JOIN_INNER;
            join->isNatural = TRUE;
            join->larg = (Node *)work_table;
            join->rarg = origin_table;
            join->usingClause = NIL;

            /* for connect-by-level we set joinquals to TRUE as a netural join */
            join->quals = makeBoolAConst(true, -1);

            result->targetList = expandAllTargetList(relInfoList);
            result->fromClause = list_make1(join);
            result->whereClause = (Node *)context->connectByOtherExpr;

            break;
        }
        case CONNECT_BY_MIXED_LEVEL: {
            join->jointype = JOIN_INNER;
            join->isNatural = TRUE;
            join->larg = (Node *)work_table;
            join->rarg = origin_table;
            join->usingClause = NIL;
            /* for MIXED connect-by-level we keep the original quals except for the level part */
            join->quals = (Node *)copyObject(connectByExpr);
            result->targetList = expandAllTargetList(relInfoList);
            result->fromClause = list_make1(join);
            result->whereClause = (Node *)context->connectByOtherExpr;
            break;
        }
        default: {
            elog(ERROR, "unrecognized connect by type %d", context->connect_by_type);
        }
    }

    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Create SWCB's conversion CTE's outer branch, normally we add StartWithExpr to
 *         outer branch's filtering cond
 *
 * @param:
 *    - (1). pstate, current SWCB belonging query level's parseState
 *    - (2). context current SWCB's trnasform context
 *    - (3). relInfoList, SWCB's relating base RangeVar
 *    - (4). startWithExpr: expr of start with clause will be put as RQ's init cond
 *    - (5). whereClause: expr of where clause
 *
 * @Return: to be input
 * --------------------------------------------------------------------------------------
 */
static SelectStmt *CreateStartWithCTEOuterBranch(ParseState *pstate,
                                                    StartWithTransformContext *context,
                                                    List *relInfoList,
                                                    Node *startWithExpr,
                                                    Node *whereClause)
{
    ListCell *lc = NULL;
    List *targetlist = NIL;
    List *tblist = NIL;
    Node *quals = NULL;

    SelectStmt *result = makeNode(SelectStmt);

    AddWithClauseToBranch(pstate, result, relInfoList);

    /* form targetlist */
    targetlist = expandAllTargetList(relInfoList);

    if (context->fromClause != NULL) {
        tblist = (List *)copyObject(context->fromClause);
    } else {
        foreach(lc, relInfoList) {
            StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc);
            tblist = lappend(tblist, copyObject(info->tblstmt));
        }
    }

    /* push whereClause down to init part, taking care to avoid NULL in expr. */
    quals = (Node *)startWithExpr;
    Node* whereClauseCopy = (Node *)copyObject(whereClause);

    if (whereClause != NULL) {
        /* only join quals can be pushed down */
        raw_expression_tree_walker((Node*)whereClauseCopy,
            (bool (*)())walker_to_exclude_non_join_quals, (void*)NULL);
    }

    if (quals == NULL) {
        quals = whereClauseCopy;
    } else if (whereClause != NULL) {
        quals = (Node *)makeA_Expr(AEXPR_AND, NULL, whereClauseCopy,
            (Node*)startWithExpr, -1);
    }

    result->fromClause = tblist;
    result->whereClause = quals;
    result->targetList = targetlist;

    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: transform recursive part and non-recursive part to recursive union
 * --------------------------------------------------------------------------------------
 */
static void CreateStartWithCTE(ParseState *pstate, Query *qry,
                   SelectStmt *initpart, SelectStmt *workpart, StartWithTransformContext *context)
{
    SelectStmt *setop = makeNode(SelectStmt);
    setop->op = SETOP_UNION;
    setop->all = true;
    setop->larg = initpart;
    setop->rarg = workpart;

    CommonTableExpr *common_expr = makeNode(CommonTableExpr);
    common_expr->swoptions = makeNode(StartWithOptions);
    common_expr->ctename = "tmp_reuslt";
    common_expr->ctequery = (Node *)setop;
    common_expr->swoptions->siblings_orderby_clause = context->siblingsOrderBy;
    common_expr->swoptions->connect_by_type = context->connect_by_type;

    /*
     *  CTE is not available at this stage yet so we disable start-with style columnref
     * transformation with maneuvers on p_hasStartWith to avoid column not found errors
     */
    pstate->p_hasStartWith = false;
    common_expr->swoptions->connect_by_level_quals =
                transformWhereClause(pstate, context->connectByLevelExpr, "LEVEL/ROWNUM quals");

    /* need to fix the collations in the quals as well */
    assign_expr_collations(pstate, common_expr->swoptions->connect_by_level_quals);

    pstate->p_hasStartWith = true;

    common_expr->swoptions->connect_by_other_quals = context->connectByOtherExpr;
    common_expr->swoptions->nocycle= context->nocycle;

    WithClause *with_clause = makeNode(WithClause);
    with_clause->ctes = NULL;
    with_clause->recursive = true;

    with_clause->ctes = lappend(with_clause->ctes, common_expr);

    /* used for multiple level startwith...connnectby */
    pstate->p_sw_selectstmt->withClause = (WithClause *)copyObject(with_clause);
    pstate->p_sw_selectstmt->startWithClause = NULL;

    /* backup p_ctenamespace and recovery */
    List *p_ctenamespace = pstate->p_ctenamespace;
    pstate->p_ctenamespace = NULL;

    qry->hasRecursive = with_clause->recursive;
    qry->cteList = transformWithClause(pstate, with_clause);
    qry->hasModifyingCTE = pstate->p_hasModifyingCTE;

    pstate->p_ctenamespace = list_concat_unique(pstate->p_ctenamespace, p_ctenamespace);
    list_free_ext(p_ctenamespace);

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: detail to be added
 * --------------------------------------------------------------------------------------
 */
static RangeTblEntry *transformStartWithCTE(ParseState* pstate, List *prior_names)
{
    ListCell *lc = NULL;
    Index ctelevelsup = 0;
    RangeTblEntry* rte = makeNode(RangeTblEntry);

    CommonTableExpr *cte = scanNameSpaceForCTE(pstate, "tmp_reuslt", &ctelevelsup);

    Assert(cte != NULL);

    rte->rtekind = RTE_CTE;
    rte->ctename = cte->ctename;
    rte->relname = cte->ctename;
    rte->ctelevelsup = ctelevelsup;
    rte->inh = false;
    rte->inFromCl = true;

    rte->self_reference = !IsA(cte->ctequery, Query);
    if (!rte->self_reference) {
        cte->cterefcount++;
    }

    rte->ctecoltypes = cte->ctecoltypes;
    rte->ctecoltypmods = cte->ctecoltypmods;
    rte->ctecolcollations = cte->ctecolcollations;

    Alias* eref = makeAlias(cte->ctename, NIL);
    Alias* alias = makeAlias(cte->ctename, NIL);

    foreach(lc, cte->ctecolnames) {
        eref->colnames = lappend(eref->colnames, lfirst(lc));
    }

    rte->eref = eref;
    rte->alias = alias;

    int index = 0;
    List *key_index = NIL;
    foreach(lc, cte->ctecolnames) {
        Value *colname = (Value *)lfirst(lc);

        if (list_member(prior_names, colname)) {
            key_index = lappend_int(key_index, index);
        }

        index++;
    }
    cte->swoptions->prior_key_index = key_index;

    /*
     * We also should fill some rewrite informations in origin selectStmt,
     * So it can be took for upper level for next step rewrite.
     */
    foreach(lc, pstate->p_sw_selectstmt->withClause->ctes) {
        CommonTableExpr* common = (CommonTableExpr*)lfirst(lc);

        if (strcmp(common->ctename, "tmp_reuslt") == 0) {
            common->swoptions->prior_key_index = key_index;
            break;
        }
    }

    /* set rewrited RTE to aborted */
    foreach (lc, pstate->p_rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        rte->swAborted = true;
    }

    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    AddStartWithCTEPseudoReturnColumns(cte, rte, list_length(pstate->p_rtable));

    return rte;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: detail to be added
 *
 * @param: detail to be added
 *
 * @Return: detail to be added
 * --------------------------------------------------------------------------------------
 */
static void transformFromList(ParseState* pstate, Query* qry,
                        StartWithTransformContext *context, Node *n)
{
    ListCell *lc = NULL;
    A_Expr *startWithExpr = (A_Expr *)context->startWithExpr;
    A_Expr *connectByExpr = (A_Expr *)context->connectByExpr;
    List *relInfoList = context->relInfoList;
    Node *whereClause = context->whereClause;

    /* make union-all branch for none- recursive part */
    SelectStmt *outerBranch = CreateStartWithCTEOuterBranch(pstate, context,
                        relInfoList, (Node *)startWithExpr, whereClause);

    /* make joinExpr for recursive part */
    SelectStmt *innerBranch = CreateStartWithCTEInnerBranch(pstate, context,
                        relInfoList, (Node *)connectByExpr, whereClause);

    CreateStartWithCTE(pstate, qry, outerBranch, innerBranch, context);

    /*
     * FINAL NEED FIX RTE POSITION, replace one of start_with_rtes with cte
     **/
    foreach(lc, relInfoList) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc);
        RangeTblEntry* rte = info->rte;
        RangeTblRef *rtr = info->rtr;

        pstate->p_joinlist = list_delete(pstate->p_joinlist, rtr);

        /* delete from p_varnamespace */
        ListCell *lcc = NULL;
        ParseNamespaceItem *nsitem = NULL;
        foreach(lcc, pstate->p_varnamespace) {
            ParseNamespaceItem *sitem = (ParseNamespaceItem *)lfirst(lcc);

            if (sitem->p_rte == rte) {
                nsitem = sitem;
                break;
            }
        }
        pstate->p_varnamespace = list_delete(pstate->p_varnamespace, nsitem);
    }

    RangeTblEntry *replace = transformStartWithCTE(pstate, context->connectby_prior_name);
    int rtindex = list_length(pstate->p_rtable);
    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;
    replace->swConverted = true;
    replace->origin_index = list_make1_int(rtr->rtindex);

    pstate->p_joinlist = list_make1(rtr);
    pstate->p_varnamespace = list_make1(makeNamespaceItem(replace, false, true));
    pstate->p_relnamespace = lappend(pstate->p_relnamespace,
                                    makeNamespaceItem(replace, false, true));

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: detail to be added
 *
 * @param: detail to be added
 *
 * @Return: detail to be added
 * --------------------------------------------------------------------------------------
 */
static void transformSingleRTE(ParseState* pstate, Query* qry,
           StartWithTransformContext *context, Node *start_clause)
{
    ListCell *lc = NULL;

    A_Expr *connectByExpr = (A_Expr *)context->connectByExpr;
    A_Expr *startWithExpr = (A_Expr *)context->startWithExpr;
    List *startWithRelInfoList = context->relInfoList;

    StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)linitial(context->relInfoList);

    /* first non-recursive part */
    SelectStmt *outerBranch = CreateStartWithCTEOuterBranch(pstate, context,
                            startWithRelInfoList, (Node *)startWithExpr, NULL);

    /* second recursive part */
    SelectStmt *innerBranch = CreateStartWithCTEInnerBranch(pstate, context,
                            startWithRelInfoList, (Node *)connectByExpr, NULL);

    /* final finish setop and commonTableExpr */
    CreateStartWithCTE(pstate, qry, outerBranch, innerBranch, context);

    /* found origin rte then replace it by new cte */
    RangeTblEntry* rte = info->rte;
    RangeTblRef *rtr = info->rtr;
    pstate->p_joinlist = list_delete(pstate->p_joinlist, rtr);

    //delete from p_varnamespace
    ParseNamespaceItem *nsitem = NULL;
    foreach(lc, pstate->p_varnamespace) {
        ParseNamespaceItem *sitem = (ParseNamespaceItem *)lfirst(lc);

        if (sitem->p_rte == rte) {
            nsitem = sitem;
            break;
        }
    }
    pstate->p_varnamespace = list_delete(pstate->p_varnamespace, nsitem);

    RangeTblEntry *replace = transformStartWithCTE(pstate, context->connectby_prior_name);
    replace->swConverted = true;
    replace->origin_index = list_make1_int(info->rtr->rtindex);
    int rtindex = list_length(pstate->p_rtable);
    rtr = makeNode(RangeTblRef);
    rtr->rtindex = rtindex;

    pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
    pstate->p_varnamespace = lappend(pstate->p_varnamespace,
                                     makeNamespaceItem(replace, false, true));
    pstate->p_relnamespace = lappend(pstate->p_relnamespace,
                                     makeNamespaceItem(replace, false, true));

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: detail to be added
 *
 * @param: detail to be added
 *
 * @Return: detail to be added
 * --------------------------------------------------------------------------------------
 */
static List *expandAllTargetList(List *targetRelInfoList)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    List *targetlist = NIL;

    foreach(lc1, targetRelInfoList) {
        StartWithTargetRelInfo *info = (StartWithTargetRelInfo *)lfirst(lc1);
        RangeTblEntry *rte = info->rte;

        /* transForm targetlist with table alias name */
        char *relname = NULL;
        Alias *alias = rte->eref;

        if (info->rtekind == RTE_SUBQUERY) {
            relname = info->aliasname;
        } else if (info->rtekind == RTE_RELATION) {
            relname = info->aliasname ? info->aliasname : info->relname;
        } else if (info->rtekind == RTE_CTE) {
            relname = info->aliasname ? info->aliasname : info->relname;
        }

        foreach(lc2, alias->colnames) {
            Value *val = (Value *)lfirst(lc2);

            ColumnRef *column = makeNode(ColumnRef);
            char *colname = strVal(val);

            /* skip already dropped column where attached as tableAlias."" */
            if (strlen(colname) == 0) {
                continue;
            }

            ResTarget *target = makeNode(ResTarget);

            column->fields = list_make2(makeString(relname), val);
            column->location = -1;
            target->val = (Node *)column;
            target->location = -1;

            target->name = makeStartWithDummayColname(relname, colname);
            targetlist = lappend(targetlist, target);
        }
    }

    return targetlist;
}
