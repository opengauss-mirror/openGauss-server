/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 *  planstartwith.cpp
 *	  The query optimizer external interface.
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/plan/planstartwith.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>
#include <math.h>

#include "access/transam.h"
#include "catalog/indexing.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_constraint.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "lib/bipartite_match.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "rewrite/rewriteManip.h"
#include "securec.h"
#include "utils/rel.h"
#include "commands/prepare.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/streamplan.h"
#include "workload/workload.h"
#include "optimizer/streamplan.h"
#include "utils/relcache.h"
#include "utils/selfuncs.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "vecexecutor/vecfunc.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeCtescan.h"
#include "optimizer/randomplan.h"
#include "optimizer/optimizerdebug.h"
#include "parser/parse_oper.h"
#include "parser/parse_expr.h"

extern Node* preprocess_expression(PlannerInfo* root, Node* expr, int kind);

StartWithCTEPseudoReturnColumns g_StartWithCTEPseudoReturnColumns[] =
{
    {"level", INT4OID, -1, InvalidOid},
    {"connect_by_isleaf", INT4OID, -1, InvalidOid},
    {"connect_by_iscycle", INT4OID, -1, InvalidOid},
    {"rownum", INT4OID, -1, InvalidOid}
};

/*
 * --------------------------------------------------------------------------------------
 *    Data & Local routines support major planning for start with
 * --------------------------------------------------------------------------------------
 */
static StartWithOp *CreateStartWithOpNode(PlannerInfo *root,
        CteScan *cteplan, RecursiveUnion *ruplan);
static void ProcessConnectByFakeConst(PlannerInfo *root, StartWithOp *swplan);
static void BuildStartWithInternalTargetList(PlannerInfo *root,
        CteScan *cteplan, StartWithOp *swplan);
static void ProcessOrderSiblings(PlannerInfo *root, StartWithOp *swplan);
static void OptimizeStartWithPlan(PlannerInfo *root, StartWithOp *swplan);

#define SWCB_MATERIAL_OUTER_THREADHOLD  100
static inline bool NeedMaterialJoinOuter(Plan *outer, Plan *inner)
{
    return (outer->total_cost > inner->total_cost * SWCB_MATERIAL_OUTER_THREADHOLD);
}

/*
 * --------------------------------------------------------------------------------------
 *    Data & Local routines support internal Key/Col generation and also
 * --------------------------------------------------------------------------------------
 */
typedef struct PullUpConnectByFuncVarContext {
    PlannerInfo *root;
    List *pullupVars;
    CteScan  *cteplan;
    StartWithOp *swplan;
} PullUpConnectByFuncVarContext;

typedef struct PullUpConnectByFuncExprContext {
    List *pullupFuncs;
} PullUpConnectByFuncExprContext;

static List *GetPRCTargetEntryList(PlannerInfo *root, RangeTblEntry *rte, StartWithOp *swplan);
static int GetVarPRCType(List *prcList, const Var* var);
static void MarkPRCNotSkip(StartWithOp *swplan, int prcType);
typedef struct ReplaceFakeConstContext {
    Var *levelVar;
    Var *rownumVar;
    Node **nodeRef;
} ReplaceFakeConstContext;

static bool PullUpConnectByFuncVarsWalker(Node *node, PullUpConnectByFuncVarContext *context);
static List *PullUpConnectByFuncVars(PlannerInfo *root, CteScan *cteScan, Node *targetEntry);
static void CheckInvalidConnectByfuncArgs(CteScan *cteplan, Oid funcid, List *arg_vars);

static void GenerateStartWithInternalEntries(PlannerInfo *root, CteScan *cteplan,
                                             List **keyEntryList, List **colEntryList);
static List* BuildStartWithPlanPseudoTargetList(Plan *plan, Index varno,
                                                List *key_list, List *col_list, bool needSiblings);

static inline bool StartWithNeedSiblingSort(PlannerInfo *root, CteScan *cteplan)
{
    Assert (IsA(cteplan, CteScan) && IsCteScanProcessForStartWith(cteplan));

    return (cteplan->cteRef->swoptions->siblings_orderby_clause != NULL);
}

/*
 * --------------------------------------------------------------------------------------
 *    Data & Local routines supporot ORDER SIBLING BY
 * --------------------------------------------------------------------------------------
 */
#define SORTCMP_TOTAL_NUM 3
#define SORTCMP_LT    0
#define SORTCMP_EQ    1
#define SORTCMP_GT    2

#define CF_ONE        1
#define CF_TWO        2

#define TEXT_COLLCATION   100

typedef struct OrderSiblingSortEntry {
    TargetEntry *tle;
    bool sortCmpOp[SORTCMP_TOTAL_NUM];
    bool sortByNullsFirst;
} OrderSiblingSortEntry;

static OrderSiblingSortEntry* CreateOrderSiblingSortEntry(
        TargetEntry *entry, SortByDir dir);
static Sort *CreateSiblingsSortPlan(PlannerInfo* root, Plan* lefttree,
        List *sortEntryList, double limit_tuples);
static Sort *CreateSortPlanUnderRU(PlannerInfo* root, Plan* lefttree,
        List *siblings, double limit_tuples);
static char *CheckAndFixSiblingsColName(PlannerInfo *root, Plan *basePlan,
                                        char *colname, TargetEntry *te);
static char *GetOrderSiblingsColName(PlannerInfo* root, SortBy *sb, Plan *basePlan);

/*
 * --------------------------------------------------------------------------------------
 *    Data & Local routines supporot pseudo targetlist push down
 * --------------------------------------------------------------------------------------
 */
#define UnknownVarno  0
#define MAX_PLAN_DEPTH 100

typedef struct StackNode {
    Plan *value_array[MAX_PLAN_DEPTH];
    int   top;
} StackNode;

static inline void StackNodePush(StackNode *s, Plan *node)
{
    s->top++;
    s->value_array[s->top] = node;
}

static inline Plan* StackNodePop(StackNode *s)
{
    if (s->top < 0) {
        elog(ERROR, "error to pop() element in %s", __FUNCTION__);
    }

    Plan *node = s->value_array[s->top];
    s->value_array[s->top] = NULL;
    s->top--;
    return node;
}

typedef struct StackVarno {
    Index value_array[MAX_PLAN_DEPTH];
    int   top;
} StackVarno;

static inline void StackVarnoPush(StackVarno *s, Index varno)
{
    s->top++;
    s->value_array[s->top] = varno;
}

static inline Index StackVarnoPop(StackVarno *s)
{
    if (s->top < 0) {
        elog(ERROR, "error to pop() element in %s", __FUNCTION__);
    }

    Index varno = s->value_array[s->top];
    s->value_array[s->top] = 0;
    s->top--;
    return varno;
}

typedef struct PlanAccessPathSearchContext {
    Plan *topNode;
    Plan *botNode;
    List *fullEntryList;
    StackNode   planStack;
    StackVarno  varnoStack;
    Index relid;
    bool  done;
    int numsPrevTlist;
} PlanAccessPathSearchContext;

static void GetWorkTableScanPlanPath(PlannerInfo *root, Plan *node,
                PlanAccessPathSearchContext *context);
static void BindPlanNodePseudoEntries(PlannerInfo *root, Plan *node, Index varno,
                PlanAccessPathSearchContext *context);
static void AddPseudoEntries(Plan *plan, Index relid, PlanAccessPathSearchContext *context);

/*
 * --------------------------------------------------------------------------------------
 *    EXPORT functions
 * --------------------------------------------------------------------------------------
 */
/* -------------------------------------------------------------------------------------
 * - brief: A helper function to determin if we need add pseudo *TLE* on CteScan node
 *          to fetch information to support start-with processing. Normally, if a CteScan
 *          node transfomed from Start/With with ORDER-SIBLINGS-BY, LEVEL, ISLEAF,
 *          ISCYCLE we need do so
 * -------------------------------------------------------------------------------------
 */
bool IsCteScanProcessForStartWith(CteScan *ctescan)
{
    if (!IsA((Plan *)ctescan, CteScan)) {
        return false;
    }

    return (ctescan->cteRef != NULL && ctescan->cteRef->swoptions != NULL);
}

/*
 * -------------------------------------------------------------------------------------
 * - brief: A helper function to indicate if a target entry is for SWCB's internal
 *          target entry, e.g. RUTIR, array_key, array_col.
 *
 * - return: TRUE if tle->resname is RUITR/array_key/array_col
 * -------------------------------------------------------------------------------------
 */
bool IsPseudoInternalTargetEntry(const TargetEntry *tle)
{
    if (tle == NULL || tle->resname == NULL) {
        return false;
    }

    StartWithOpColumnType type = GetPseudoColumnType(tle);

    return (type == SWCOL_RUITR || type == SWCOL_ARRAY_KEY || type == SWCOL_ARRAY_COL || type == SWCOL_ARRAY_SIBLINGS);
}

/*
 * -------------------------------------------------------------------------------------
 * - brief: A helper function to indicate if a target entry is for SWCB's pseudo return
 *          columns
 *
 * - return: TRUE if tle->resname is level/rownum/iscycle/isleaf
 * -------------------------------------------------------------------------------------
 */
bool IsPseudoReturnTargetEntry(const TargetEntry *tle)
{
    if (tle == NULL || tle->resname == NULL) {
        return false;
    }

    StartWithOpColumnType type = GetPseudoColumnType(tle);

    return (type == SWCOL_LEVEL || type == SWCOL_ISLEAF ||
                type == SWCOL_ISCYCLE || type == SWCOL_ROWNUM);
}

/*
 * -------------------------------------------------------------------------------------
 * - brief: function to help fix tlist with explicite name
 *
 * - return: return a new list with resname assigned
 * -------------------------------------------------------------------------------------
 */
List *FixSwTargetlistResname(PlannerInfo *root, RangeTblEntry *curRte, List *tlist)
{
    if (curRte->rtekind != RTE_CTE || !curRte->swConverted) {
        elog(WARNING, "unusable case just original tlist");
        return tlist;
    }
    int baseColNum = list_length(curRte->eref->colnames);
    int natts = list_length(tlist);

    if (natts - baseColNum != STARTWITH_PSEUDO_RETURN_ATTNUMS &&
        baseColNum != natts) {
        elog(ERROR, "unrecognized case baseColNum/tlist");
    }

    /* attach resname for target entry */
    ListCell *lc = NULL;
    List *newList = NIL;
    AttrNumber attno = 0;

    /* fix the origianl output column */
    foreach (lc, tlist) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);
        const char *colname = NULL;
        if (attno < baseColNum) {
            colname = strVal(list_nth(curRte->eref->colnames, attno));
        } else {
            colname = g_StartWithCTEPseudoReturnColumns[attno - baseColNum].colname;
        }

        entry->resname = pstrdup(colname);

        attno++;
        newList = lappend(newList, entry);
    }

    return newList;
}

/*
 * @Brief: identify if it is a connect by level/rownum plan node
 */
bool IsConnectByLevelStartWithPlan(const StartWithOp *plan)
{
    Assert (IsA(plan, StartWithOp) && plan->swoptions != NULL);

    return (plan->swoptions->connect_by_type == CONNECT_BY_LEVEL ||
            plan->swoptions->connect_by_type == CONNECT_BY_ROWNUM ||
            plan->swoptions->connect_by_type == CONNECT_BY_MIXED_LEVEL ||
            plan->swoptions->connect_by_type == CONNECT_BY_MIXED_ROWNUM);
}

/*
 * @Brief: return a INT const node value, 0 if not INT const, considered as exception
 */
int32 GetStartWithFakeConstValue(A_Const *n)
{
    Assert (IsA(n, A_Const));

    if (n->val.type == T_Integer) {
        return n->val.val.ival;
    } else {
        elog(LOG, "Accept a other Const val when evaluate FakeConst to rownum/level");
        return 0;
    }
}

/*
 * @Brief: return a INT const node value, 0 if not INT const, considered as exception
 */
int32 GetStartWithFakeConstValue(Const *val)
{
    Assert (IsA(val, Const));

    if (IsA(val, Const) && !((Const*)val)->constisnull &&
        ((Const*)val)->consttype == INT4OID) {
        return DatumGetInt32(((Const*)val)->constvalue);
    } else {
        elog(LOG, "Accept an invalid Const val when evaluate FakeConst to rownum/level");
        return 0;
    }
}

static char* GetSiblingsColNameFromFunc(Node* node)
{
    if (!IsA(node, FuncCall)) {
        return NULL;
    }
    ListCell* lc = NULL;
    foreach(lc, ((FuncCall*) node)->args) {
        Node *n = (Node *) lfirst(lc);
        if (!IsA(n, ColumnRef)) {
            continue;
        }
        ColumnRef  *cr = (ColumnRef *) n;
        int len = list_length(cr->fields);
        if (len == CF_ONE) {
            return strVal(linitial(cr->fields));
        } else if (len == CF_TWO) {
            return strVal(lsecond(cr->fields));
        }
    }
    return NULL;
}

static bool raw_unsupported_func_walker(Node *node, Node *context_node)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, FuncCall)) {
        FuncCall* fcall = (FuncCall*)node;
        Value *val = (Value *)linitial(fcall->funcname);
        char* name = strVal(val);
        if (strcmp(name, "connect_by_root") == 0) {
            ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER),
                            errmsg("CONNECT BY ROOT operator is not supported in the "
                                    "START WITH or in the CONNECT BY condition")));
        } else if (strcmp(name, "sys_connect_by_path") == 0) {
            ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER),
                            errmsg("SYS_CONNECT_BY_PATH function is not allowed here")));
        }
    }

    return raw_expression_tree_walker(node, (bool (*)()) raw_unsupported_func_walker, context_node);
}

static bool unsupported_func_walker(Node *node, Node *context_node)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, FuncExpr)) {
        FuncExpr* expr = (FuncExpr*)node;
        if (expr->funcid == CONNECT_BY_ROOT_FUNCOID) {
            ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER),
                            errmsg("CONNECT BY ROOT operator is not supported in the "
                                    "START WITH or in the CONNECT BY condition")));
        } else if (expr->funcid == SYS_CONNECT_BY_PATH_FUNCOID) {
            ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER),
                            errmsg("SYS_CONNECT_BY_PATH function is not allowed here")));
        }
    }

    return expression_tree_walker(node, (bool (*)()) unsupported_func_walker, context_node);
}

/*
 * @Brief: check fix order-siblings columns, normally we can find sort-key from from
 * basePlan's targetlist, but some special case we need additional process:
 *   1. check colname exists in base-plan's targetlist, return if ok
 *   2. check colname exists in parse->targetlist's rename
 *   un-named expr, assign "?column?" as default
 *   3. alias name
 *
 * For these cases, we need scan parse->targetlist(alias case) and find the basePlan's
 * to recreate sort key
 */
static char *CheckAndFixSiblingsColName(PlannerInfo *root, Plan *basePlan,
    char *colname, TargetEntry *te)
{
    /* step1. check if searched colname exists in base-plan's targetlist */
    char *resultColName = NULL;
    ListCell *lc = NULL;
    foreach (lc, basePlan->targetlist) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);
        if (entry->resname == NULL) {
            continue;
        }

        char *label = strrchr(entry->resname, '@');
        label += 1;

        if (strcmp(colname, label) == 0) {
            resultColName = colname;
            break;
        }
    }

    /*
     * If resultColName is found in basePlan's targelist, it says we are in regular
     * case and it is safely to build sort plan for Order-Sibling clause
     *
     * regular case, we found sort-entry in base-plan's targetlist
     */
    if (resultColName != NULL) {
        return resultColName;
    }

    /*
     *  step2. try to find sort-entry in parse->targetlist as resname(for alias case)
     *     case 1. regular alias, e.g. select id as alias_id,... te->resname: "alias_id"
     *     case 2. unnamed expr,  e.g. select id * 2, pid,....   te->resname: "?column?"
     */
    bool found = false;
    if (te == NULL) {
        /* normally for alias case */
        foreach (lc, root->parse->targetList) {
            te = (TargetEntry *)lfirst(lc);

            /* te->resname may be null, so check it first */
            if (te->resname && strcmp(te->resname, colname) == 0) {
                unsupported_func_walker((Node*)te->expr, NULL);
                found = true;
                break;
            }
        }

        /*
         * Still not found just return origin colname, error-out later in sort-plan
         * creation stage
         */
        if (!found || te == NULL) {
            return colname;
        }
    }

    /* Pull the aggregates and var nodes from the quals */
    List *vars = pull_var_clause((Node *)te->expr,
                                 PVC_INCLUDE_AGGREGATES,
                                 PVC_RECURSE_PLACEHOLDERS);

    /*
     * do not support none-table column's ref specified as order siblings's sort entry
     *
     * Keep origin behavior as previous version
     */
    if (vars == NIL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("expression with none-var in order siblings is not supported")));
    }

    /* do not support multi-column refs specified as order sibling's sort entry */
    if (list_length(vars) > 1) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("expression with multi-vars in order siblings is not full-supported,"
                       " only 1st var will be sorted.")));
    }
    Var *var = (Var *)linitial(vars);
    RangeTblEntry *rte = root->simple_rte_array[var->varno];
    char *raw_cte_alias = (char *)strVal(list_nth(rte->eref->colnames, var->varattno - 1));
    resultColName = strrchr(raw_cte_alias, '@');
    resultColName += 1;   /* fix '@' offset */

    return resultColName;
}

/*
 * @Brief: Get Siblings column name according Siblings-SortBy Clause.
 */
static char *GetOrderSiblingsColName(PlannerInfo* root, SortBy *sb, Plan *basePlan)
{
    char *colname = NULL;
    TargetEntry *te = NULL;

    /* check whether connect_by_root and sys_connect_by_path exist in sortBy */
    raw_unsupported_func_walker(sb->node, NULL);

    if (IsA(sb->node, ColumnRef)) {
        ColumnRef  *cr = (ColumnRef *)sb->node;
        int len = list_length(cr->fields);

        if (len == CF_ONE) {
            colname = strVal(linitial(cr->fields));
        } else if (len == CF_TWO) {
            colname = strVal(lsecond(cr->fields));
        }
    } else if (IsA(sb->node, A_Const)) {
        A_Const *con = (A_Const *)sb->node;
        Value* val = &((A_Const*)con)->val;

        if (!IsA(val, Integer)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("not integer constant in order siblings by clause")));
        }

        int siblingIdx = intVal(val);

        if (siblingIdx > list_length(root->parse->targetList)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("Order siblings by tlistIdx %d exceed length of targetList.", siblingIdx)));
        }

        te = (TargetEntry *)list_nth(root->parse->targetList, siblingIdx - 1);

        unsupported_func_walker((Node*)te->expr, NULL);

        if (te->resname != NULL && IsPseudoReturnColumn(te->resname)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("Not support refer startwith Pseudo column in order siblings by.")));
        }

        colname = te->resname;
    }

    /*
     * We do not support function call as sort key here yet.
     * Try to do our best by returning the first arg column anyway.
     */
    if (colname == NULL) {
        colname = GetSiblingsColNameFromFunc(sb->node);
    }

    /*
     * Check and fix none regular columns, e.g. expr with no alias ?column? and
     * name-alias that can not be found from StartWithOp plan's targetlist
     *
     * Special case: we do not have to process PRC
     */
    if (colname != NULL && !IsPseudoReturnColumn(colname)) {
        colname = CheckAndFixSiblingsColName(root, basePlan, colname, te);
    }

    return colname;
}

static Node* tryReplaceFakeConstWithTarget(Node* node, ReplaceFakeConstContext *context)
{
    if (node == NULL) {
        return node;
    }

    if (IsA(node, Const)) {
        int constVal = GetStartWithFakeConstValue((Const *)node);
        if (constVal == CONNECT_BY_LEVEL_FAKEVALUE) {
            node = (Node*) context->levelVar;
        } else if (constVal == CONNECT_BY_ROWNUM_FAKEVALUE) {
            node = (Node *) context->rownumVar;
        }
    }
    return node;
}

static bool ReplaceFakeConstWalker(Node *node, ReplaceFakeConstContext *context)
{
    if (node == NULL) {
        return false;
    }

    switch (node->type) {
        case T_OpExpr: {
            OpExpr *op = (OpExpr *)node;
            List *newArgs = NIL;
            ListCell *lc = NULL;
            /* replace "fake-const" to level/rownum var attr */
            foreach (lc, op->args) {
                Node *n = (Node *)lfirst(lc);
                n = tryReplaceFakeConstWithTarget(n, context);
                newArgs = lappend(newArgs, n);
            }
            op->args= newArgs;
            break;
        }
        case T_TypeCast: {
            TypeCast* tc = (TypeCast*) node;
            tc->arg = tryReplaceFakeConstWithTarget(tc->arg, context);
            break;
        }
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
            ListCell *lc = NULL;
            List *newArgs = NIL;
            foreach (lc, opexpr->args) {
                Node *n = (Node *)lfirst(lc);
                n = tryReplaceFakeConstWithTarget(n, context);
                newArgs = lappend(newArgs, n);
            }
            opexpr->args = newArgs;
            break;
        }
        case T_FuncExpr: {
            FuncExpr* fexpr = (FuncExpr*)node;
            List *newArgs = NIL;
            ListCell *lc = NULL;
            /* replace "fake-const" to level/rownum var attr */
            foreach (lc, fexpr->args) {
                Node *n = (Node *)lfirst(lc);
                n = tryReplaceFakeConstWithTarget(n, context);
                newArgs = lappend(newArgs, n);
            }
            fexpr->args= newArgs;
            break;
        }
        default: {
            ereport(DEBUG1,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("StartWithOp's ConnectByLevel only suport simple connect-by expr"),
                     errdetail("Maybe you see the support scope of connect by level"),
                     errhint("Please check your connect by clause carefully")));
            break;
        }
    }
    return expression_tree_walker(node,
                (bool (*)())ReplaceFakeConstWalker, (void*)context);
}

/*
 * -------------------------------------------------------------------------------------
 * @Brief: replace the fake-const value into real level/rownum attribute, so that we can
 *         do level/rownum condition check regularly
 *
 * Revisit the whole implementation for connect-by level/rownum:
 *    (1). In syntax stage, we convert level/rownum column into a "fake-const"
 *    (2). In semantic stage, as we don't add level/rownum pseodu return column to
 *         base table, so a "fake-const" value in connect-by expression ensure
 *         safe-transform
 *    (3). In planning state(StartWithOp), level/rownum pseudo column is inherited from
 *         CteScan, so we replace "fake-const" back to its correct format and do
 *         expression preprocess, SSQ/CSSQ can be proper planned into InitPlan/SubPlan
 * -------------------------------------------------------------------------------------
 */
static void ReplaceFakeConst(PlannerInfo *root, StartWithOp *swplan)
{
    StartWithOptions *swoptions = swplan->swoptions;
    Node *connectByLevelExpr = swoptions->connect_by_level_quals;

    /* Initialize replacement context */
    errno_t rc = 0;
    ReplaceFakeConstContext context;
    rc = memset_s(&context,
                  sizeof(ReplaceFakeConstContext),
                  0,
                  sizeof(ReplaceFakeConstContext));
    securec_check(rc, "\0", "\0");

    /* Step 1. Find level/rownum pseudo return columns from startwith's targetlist */
    ListCell *lc = NULL;
    foreach (lc, swplan->plan.targetlist) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);
        if (pg_strcasecmp(entry->resname, "level") == 0) {
            context.levelVar = (Var *)entry->expr;
        }
        if (pg_strcasecmp(entry->resname, "rownum") == 0) {
            context.rownumVar = (Var *)entry->expr;
        }
    }

    /* && rownumVar != NULL */
    Assert (context.levelVar != NULL);

    /* Step 2. Apply fake const replacement walker to the expression */
    ReplaceFakeConstWalker(connectByLevelExpr, &context);

    /* Step 3. Do reqular const expression process */
    swoptions->connect_by_level_quals = eval_const_expressions(root, connectByLevelExpr);
}

/*
 * - brief: Do possible post-path stage optimization, e.g. add Material node on an
 *          NestLoop's outer part
 *
 * - Note: Ideally should be handled in PathNode generation stage, in order to do so we have
 *         serveral case need to come up with e.g. identfy which SubQuery's output should be
 *         materilzed, for short term we only to improve to avoid the worst case where
 *         RuPlan's inner branch ReScan()-ed many times and its BaseRel with high selectivity
 */
static void OptimizeStartWithPlan(PlannerInfo *root, StartWithOp *swplan)
{
    RecursiveUnion *ruplan = swplan->ruplan;

    /* shunking only supported scenarios */
    if (!IsA(innerPlan(ruplan), SubqueryScan)) {
        return;
    }

    SubqueryScan *sqscan = (SubqueryScan *)innerPlan(ruplan);
    Plan *joinPlan = sqscan->subplan;

    /* only handle nestloop and hash join case */
    if (!IsA(joinPlan, NestLoop) && !IsA(joinPlan, HashJoin)) {
        return;
    }

    /* add materialize */
    Plan *joinOuter = outerPlan(joinPlan);
    Plan *joinInner = innerPlan(joinPlan);
    Plan *wtscan = NULL;

    /*
     * Only handle WorkTableScan is in inner case, when WorkTableScan is in outer tree it
     * can not be material-optimized as its result is volatile for each iteration.
     */
    if (IsA(joinPlan, NestLoop) && IsA(joinInner, WorkTableScan)) {
        wtscan = joinInner;
    } else if (IsA(joinPlan, HashJoin) && IsA(joinInner, Hash) &&
               IsA(joinInner->lefttree, WorkTableScan)) {
        wtscan = joinInner->lefttree;
    } else {
        /* unrecognized cases */
        return;
    }

    Assert (wtscan != NULL && IsA(wtscan, WorkTableScan));

    if (!NeedMaterialJoinOuter(joinOuter, wtscan)) {
        return;
    }

    Material *mtplan = make_material(joinOuter, true);
    inherit_plan_locator_info((Plan *)mtplan, joinOuter);
    copy_plan_costsize((Plan *)mtplan, joinOuter);

    joinPlan->lefttree = (Plan *)mtplan;
}

/*
 * -------------------------------------------------------------------------------------
 * @Brief: The major processing logic entry point for start-with, given a CteScan node,
 *         we insert a StartWithOp proc node to do START WITH .... CONNECT BY processing,
 *         normally the process is separate many steps, see details as below:
 *
 *  step(1): Create StartWithOp node and insert it between CtScan and RuScan, also replace
 *           the subplan refs in current PlannerInfo level
 *  step(2): Process ConnectByLevel/Rownum's fake const value
 *  step(3): Add possible post-path generation plan optimization e.g. rescan-avoid for
 *           start with
 *  step(4): Create Internal target list (RUITR/array_key/array_columns)
 *  step(5): Specially process ORDER SIBLINGS BY case
 * -------------------------------------------------------------------------------------
 */
Plan *AddStartWithOpProcNode(PlannerInfo *root, CteScan *cteplan, RecursiveUnion *ruplan)
{
    Plan          *plan = (Plan *)cteplan;
    StartWithOp   *swplan = NULL;

    /*
     * Step1. Create a StartWithOp node between CteScan and RuScan, also update
     *        SubPlan-ref in curernt PlannerInfo's glob->subplans
     */
    swplan = CreateStartWithOpNode(root, cteplan, ruplan);

    /*
     * Step2. In case of connect by level/rownum, we need create new quals to evaluate
     *        level/rownum condition in StartWith operator
     */
    ProcessConnectByFakeConst(root, swplan);

    /*
     * Step3. Do some possible post-path generation plan optimization, to avoid worst
     *        cases, e.g. recursive part with high selectivity but rescan many times
     */
    OptimizeStartWithPlan(root, swplan);

    /*
     * Step4. Add internal TargetEntryList to CteScan & RuScan, also add some runtime level
     *        optimization hints, e.g. swSkipOptions
     */
    BuildStartWithInternalTargetList(root, cteplan, swplan);

    /*
     * Step5. Process order-siblings cases
     */
    ProcessOrderSiblings(root, swplan);

    /* Copy Params from ruplan */
    swplan->plan.extParam = bms_copy(ruplan->plan.extParam);
    swplan->plan.allParam = bms_copy(ruplan->plan.allParam);

    return plan;
}

/*
 * Local Routines to support AddStartWithOpProcNode()
 */
static StartWithOp* CreateStartWithOpNode(PlannerInfo *root,
        CteScan *cteplan, RecursiveUnion *ruplan)
{
    Plan            *plan = (Plan *)cteplan;
    StartWithOp     *swplan = NULL;

    Index          relid = cteplan->scan.scanrelid;
    int            natts = 0;
    RangeTblEntry *rte = NULL;

    /* 1. fix targetlist with explict name */
    natts = list_length(plan->targetlist);
    rte = rt_fetch(relid, root->parse->rtable);
    plan->targetlist = FixSwTargetlistResname(root, rte, plan->targetlist);
    

    /* 2. Build StartWithOp's targetlist */
    swplan = makeNode(StartWithOp);
    swplan->plan.lefttree = (Plan *)ruplan;
    swplan->plan.targetlist = (List *)copyObject(plan->targetlist);
    swplan->ruplan = ruplan;
    swplan->cteplan = cteplan;
    swplan->swoptions = (StartWithOptions *)copyObject(cteplan->cteRef->swoptions);

    /* generate PRC list for both swplan and ctescan */
    swplan->prcTargetEntryList = GetPRCTargetEntryList(root, rte, swplan);
    cteplan->prcTargetEntryList = (List *)copyObject(swplan->prcTargetEntryList);

    /* Inherit ctescan's targetlist to StartWithOp */
    inherit_plan_locator_info((Plan *)swplan, plan);
    copy_plan_costsize((Plan *)swplan, (Plan *)ruplan);

    /*
     * Initialize PRC runtime optimization hints.
     *
     * Basically, we do optimistic scheme assuming all PRC on CTE is able to skip
     */
    {
        SetColumnSkipOptions(swplan->swExecOptions);

        /* Mark level/rownum is not skipped, as they are lit-enough */
        ClearSkipLevel(swplan->swExecOptions);
        SetSkipIsLeaf(swplan->swExecOptions);
        SetSkipIsCycle(swplan->swExecOptions);
        ClearSkipRownum(swplan->swExecOptions);
    }
    
    /* fix original Ruplan refered place */
    cteplan->subplan = (RecursiveUnion *)swplan;
    List *newSubplans = NIL;
    ListCell *lc = NULL;
    foreach (lc, root->glob->subplans) {
        Plan *plan = (Plan *)lfirst(lc);
        if (IsA(plan, RecursiveUnion) && (Plan *)plan == (Plan *)ruplan) {
            plan = (Plan *)swplan;
        }
        newSubplans = lappend(newSubplans, plan);
    }
    root->glob->subplans = newSubplans;

    return swplan;
}

static void ProcessConnectByFakeConst(PlannerInfo *root, StartWithOp *swplan)
{
    if (!IsConnectByLevelStartWithPlan(swplan)) {
        return;
    }

    StartWithOptions *swoptions = swplan->swoptions;

    /* fix fake const in qual expr */
    ReplaceFakeConst(root, swplan);

    /* do preprocess for connect-by-level/rownum condition */
    ((Plan *)swplan)->qual = (List *)preprocess_expression(root,
            swoptions->connect_by_level_quals, EXPRKIND_QUAL);
}

static void BuildStartWithInternalTargetList(PlannerInfo *root,
            CteScan *cteplan, StartWithOp *swplan)
{
    List *keyEntryList = NIL;
    List *colEntryList = NIL;
    bool needSiblings = (swplan->swoptions->siblings_orderby_clause != NULL) ? true : false;

    /* Generate internal target entry keyEntryList & colEntryList */
    GenerateStartWithInternalEntries(root, cteplan, &keyEntryList, &colEntryList);

    if (keyEntryList == NULL) {
        ereport(DEBUG1,
                (errmodule(MOD_OPT_PLANNER), errmsg("keyEntryList is NULL, query:%s",
                t_thrd.postgres_cxt.debug_query_string)));
    }

    if (colEntryList == NULL) {
        ereport(DEBUG1,
                (errmodule(MOD_OPT_PLANNER), errmsg("colEntryList is NULL, query:%s",
                t_thrd.postgres_cxt.debug_query_string)));
    }

    /* Add internal key/col entry list and pseudo_list for StartWithOp node */
    swplan->internalEntryList =
            BuildStartWithPlanPseudoTargetList((Plan *)swplan, cteplan->scan.scanrelid,
                                                keyEntryList,
                                                colEntryList,
                                                needSiblings);

    /* Add internal key/col entry list for CteScan node */
    cteplan->internalEntryList =
            BuildStartWithPlanPseudoTargetList((Plan *)cteplan, cteplan->scan.scanrelid,
                                                keyEntryList,
                                                colEntryList,
                                                needSiblings);

    /* construct fullEntryList (RUITR + array_key + array_col ) */
    List *fullEntryList = NIL;
    ListCell *entry = NULL;
    foreach (entry, cteplan->scan.plan.targetlist) {
        TargetEntry *te = (TargetEntry *)lfirst(entry);

        /* skip regular columns */
        if (IsPseudoReturnTargetEntry(te) || IsPseudoInternalTargetEntry(te)) {
            fullEntryList = lappend(fullEntryList, copyObject(te));
        }
    }

    swplan->keyEntryList = keyEntryList;
    swplan->colEntryList = colEntryList;
    swplan->fullEntryList = fullEntryList;
}

static void ProcessOrderSiblings(PlannerInfo *root, StartWithOp *swplan)
{
    CteScan          *cteplan = swplan->cteplan;
    RecursiveUnion   *ruplan = swplan->ruplan;
    StartWithOptions *swoptions = swplan->swoptions;

    if (!StartWithNeedSiblingSort(root, cteplan)) {
        return;
    }

    /* Fix RecursiveUnion TargetList */
    ListCell *lc = NULL;
    foreach(lc, cteplan->scan.plan.targetlist) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);

        if (IsPseudoReturnColumn(te->resname)) {
            ruplan->plan.targetlist = lappend(ruplan->plan.targetlist, te);
        }
    }

    ruplan->internalEntryList =
        BuildStartWithPlanPseudoTargetList((Plan *)ruplan, cteplan->scan.scanrelid,
                                            swplan->keyEntryList,
                                            swplan->colEntryList,
                                            true);

    /* 1. Add under RU sort plan */
    ruplan->plan.lefttree = (Plan *)CreateSortPlanUnderRU(root, ruplan->plan.lefttree,
                                            swoptions->siblings_orderby_clause, -1);
    ruplan->plan.righttree = (Plan *)CreateSortPlanUnderRU(root, ruplan->plan.righttree,
                                            swoptions->siblings_orderby_clause, -1);
}


/*
 * --------------------------------------------------------------------------------------
 *    LOCAL functions
 * --------------------------------------------------------------------------------------
 */
/*
 * @Brief: build full pseudo entries for given pro node, with input a keylist and collist
 *
 * pseudo target list
 *  [1]. RUIRT
 *  [2]. array_key_1
 *  [3]. array_col_2
 *  [4]. array_col_3
 *  [6]....
 *
 * example:
 * Table:
 *      t1(id, name, fatherid, name_desc)
 * Query:
 *      select *,
 *             level, connect_by_isleaf, connect_is_cycle,
 *             connect_by_root name_desc, sys_connect_by_path(name, '@')
 *      from t1
 *      start with name = 'shanghai'
 *      connect by id = PRIOR fatherid;
 *
 * pseudo-targetlist: array [
 *          - RUTI(type:int)
 *          - array_key_1(type:text)
 *          - array_col_2(type:text)
 *          - array_col_4(type:text)
 *          ]
 */
static List* BuildStartWithPlanPseudoTargetList(Plan *plan, Index varno,
            List *key_list, List *col_list, bool needSiblings)
{
    Node *expr = NULL;
    TargetEntry *te = NULL;
    TargetEntry *pte = NULL;
    List *internalEntryList = NIL;
    int rc = 0;

    /* we only have to handle CteScan & RecursiveUnion node */
    Assert (IsA(plan, CteScan) || IsA(plan, RecursiveUnion) || IsA(plan, StartWithOp));

    /* 1. Add "RUITR" entry to support LEVEL */
    expr = (Node *)makeVar(varno, list_length(plan->targetlist) + 1,
                           INT4OID, -1, InvalidOid, 0);
    pte = makeTargetEntry((Expr *)expr, list_length(plan->targetlist) + 1, "RUITR", false);
    plan->targetlist = lappend(plan->targetlist, pte);
    internalEntryList = lappend(internalEntryList, pte);

    /*
     * 2. Add "array_key" entry we have to add to support connect_by_isleaf, connect_by_iscycle,
     *    order-siblings
     */
    ListCell *lc = NULL;
    foreach (lc, key_list) {
        te = (TargetEntry *)lfirst(lc);
        char resname[NAMEDATALEN] = {0};

        rc = sprintf_s(resname, NAMEDATALEN, "array_key_%d", te->resno);
        securec_check_ss(rc, "\0", "\0");

        expr = (Node *)makeVar(varno, list_length(plan->targetlist) + 1,
                               TEXTOID, -1, TEXT_COLLCATION, 0);
        pte = makeTargetEntry((Expr *)expr, list_length(plan->targetlist) + 1,
                               pstrdup(resname), false);
        plan->targetlist = lappend(plan->targetlist, pte);
        internalEntryList = lappend(internalEntryList, pte);
    }

    /*
     * 3. Add "array_col" to support , connect_by_root
     */
    foreach (lc, col_list) {
        te = (TargetEntry *)lfirst(lc);
        char resname[NAMEDATALEN] = {0};

        rc = sprintf_s(resname, NAMEDATALEN, "array_col_%d", te->resno);
        securec_check_ss(rc, "\0", "\0");

        expr = (Node *)makeVar(varno, list_length(plan->targetlist) + 1,
                               TEXTOID, -1, TEXT_COLLCATION, 0);
        pte = makeTargetEntry((Expr *)expr, list_length(plan->targetlist) + 1,
                               pstrdup(resname), false);
        plan->targetlist = lappend(plan->targetlist, pte);
        internalEntryList = lappend(internalEntryList, pte);
    }

    /*
     * Add "array_siblings" pseudo columns support if need
     */
    if (needSiblings) {
        expr = (Node *)makeVar(varno, list_length(plan->targetlist) + 1,
                               BYTEAOID, -1, 0, 0);
        pte = makeTargetEntry((Expr *)expr, list_length(plan->targetlist) + 1, "array_siblings", false);
        plan->targetlist = lappend(plan->targetlist, pte);
        internalEntryList = lappend(internalEntryList, pte);
    }

    return internalEntryList;
}

/*
 * @brief: check if a given connect by function is valid for its args, currently only
 *         connect_by_root()/sys_connect_by_path() is supported
 */
static void CheckInvalidConnectByfuncArgs(CteScan *cteplan, Oid funcid, List *arg_vars)
{
    /* for none connect by function we do not do arg checks on its type */
    if (funcid != CONNECT_BY_ROOT_FUNCOID && funcid != SYS_CONNECT_BY_PATH_FUNCOID) {
        return;
    }

    int maxBaseRelAttnum =
            list_length(cteplan->scan.plan.targetlist) - STARTWITH_PSEUDO_RETURN_ATTNUMS;

    /* validate common cases */
    if (list_length(arg_vars) > 1) {
        elog(ERROR, "only single column can be put as argument in connect_by_root/sys_connect_by_path()");
    }

    /*
     * For compatible with Oracle where  sys_connect_by_path() and connect_by_root()
     * allow a const value set as 1st paramameter, return anyway
     */
    if (arg_vars == NIL) {
        return;
    }

    Var *arg = (Var *)linitial(arg_vars);
    if (arg->varattno > maxBaseRelAttnum) {
        elog(ERROR, "only base table column can be specified in connect_by_root/sys_connect_by_path()");
    }

    /* valid specific cases */
    switch (funcid) {
        case CONNECT_BY_ROOT_FUNCOID: {
            /* for connect_by_root() we only check if base column */
            break;
        }
        case SYS_CONNECT_BY_PATH_FUNCOID: {
            /* for sys_connect_by_path() allow textual data types TEXT/VARCHAR/CHAR */
            Oid typid = arg->vartype;
            if (typid != TEXTOID && typid != VARCHAROID && typid != BPCHAROID &&
                typid != NVARCHAR2OID) {
                elog(ERROR, "only text type(CHAR/VARCHAR/NVARCHAR2/TEXT) is allow for sys_connect_by_path()");
            }
            break;
        }

        default: {
            elog(ERROR, "unknown functions funid:%u in ConnectByFuncArg check.", funcid);
        }
    }
}

static bool PullUpConnectByFuncExprWalker(Node *node, PullUpConnectByFuncExprContext *context)
{
    if (node == NULL) {
        return false;
    }

    /* FuncExpr we do connect-by func validation check */
    if (IsA(node, FuncExpr)) {
        FuncExpr *func = (FuncExpr *)node;

        /* Only handle start with hierachocal query's function cases */
        if (IsHierarchicalQueryFuncOid(func->funcid)) {
            context->pullupFuncs = list_append_unique(context->pullupFuncs, func);
        }
    }

    return expression_tree_walker(node, (bool (*)())PullUpConnectByFuncExprWalker, (void *)context);
}

List *pullUpConnectByFuncExprs(Node* node)
{
    errno_t rc = 0;
    PullUpConnectByFuncExprContext context;
    rc = memset_s(&context,
                  sizeof(PullUpConnectByFuncExprContext),
                  0,
                  sizeof(PullUpConnectByFuncExprContext));
    securec_check(rc, "\0", "\0");

    (void)PullUpConnectByFuncExprWalker(node, &context);

    return context.pullupFuncs;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Functions to support var pull up from subquery's target list, also check each
 *         possible vars, so that we can do possible PRC skip optimizations
 *
 * PullUpConnectByFuncVars()
 * --------------------------------------------------------------------------------------
 */
static List *PullUpConnectByFuncVars(PlannerInfo *root, CteScan *cteScan, Node *targetEntry)
{
    errno_t rc = 0;
    PullUpConnectByFuncVarContext context;
    rc = memset_s(&context,
                  sizeof(PullUpConnectByFuncVarContext),
                  0,
                  sizeof(PullUpConnectByFuncVarContext));
    securec_check(rc, "\0", "\0");

    context.root = root;
    context.pullupVars = NIL;
    context.cteplan = cteScan;
    context.swplan = (StartWithOp *)cteScan->subplan;

    (void)PullUpConnectByFuncVarsWalker(targetEntry, &context);

    return context.pullupVars;
}

static bool PullUpConnectByFuncVarsWalker(Node *node, PullUpConnectByFuncVarContext *context)
{
    if (node == NULL) {
        return false;
    }

    /*
     * We handle two cases here:
     *   1. FuncExpr we do connect-by func validation check
     *   2. Var, we check if its PRC and possible PRC-skip optimization could apply
     */
    if (IsA(node, FuncExpr)) {
        FuncExpr *func = (FuncExpr *)node;

        /* Only handle start with hierachocal query's function cases */
        if (IsHierarchicalQueryFuncOid(func->funcid)) {
            /* pull-up basic vars from FuncExpr node */
            List *vars = pull_var_clause((Node*)func->args,
                             PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
            CheckInvalidConnectByfuncArgs(context->cteplan, func->funcid, vars);
            context->pullupVars = list_concat_unique(context->pullupVars, vars);
        }
    } else if (IsA(node, Var)) {
        /* check if there is target var refer to PRC and mark them not-skipable */
        Var *var = (Var *)node;
        int prcType = GetVarPRCType(context->cteplan->prcTargetEntryList, var);
        if (prcType == SWCOL_LEVEL || prcType == SWCOL_ISLEAF ||
                prcType == SWCOL_ISCYCLE || prcType == SWCOL_ROWNUM) {
            MarkPRCNotSkip(context->swplan, prcType);
        }
    }

    return expression_tree_walker(node,
                (bool (*)())PullUpConnectByFuncVarsWalker, (void*)context);
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Functions to relevant to PRC optimization in hierachical queries
 *   - GetPRCTargetEntryList()
 *   - GetVarPRCType()
 *   - MarkPRCNotSkip()
 * --------------------------------------------------------------------------------------
 */
static List *GetPRCTargetEntryList(PlannerInfo *root, RangeTblEntry *rte, StartWithOp *swplan)
{
    int            natts = 0;
    int            baseColNum = 0;
    Plan          *plan = (Plan *)swplan;
    List          *prcTargetEntryList = NIL;

    /* check if PRC targetlist is already generated */
    if (swplan->prcTargetEntryList != NIL) {
        return swplan->prcTargetEntryList;
    }

    baseColNum = list_length(rte->eref->colnames) - STARTWITH_PSEUDO_RETURN_ATTNUMS;
    natts = list_length(plan->targetlist);

    if (natts - baseColNum != STARTWITH_PSEUDO_RETURN_ATTNUMS &&
        baseColNum != natts) {
        elog(ERROR, "unrecognized case baseColNum/tlist");
    }

    /* attach resname for target entry */
    ListCell *lc = NULL;
    foreach (lc, plan->targetlist) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);
        if (entry->resno > baseColNum) {
            prcTargetEntryList = lappend(prcTargetEntryList, entry);
        }
    }

    Assert (prcTargetEntryList != NIL);

    return prcTargetEntryList;
}

static int GetVarPRCType(List *prcList, const Var* var)
{
    ListCell *lc = NULL;
    int prcType = SWCOL_LEVEL;

    foreach (lc, prcList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!equal(var, tle->expr)) {
            prcType++;
            continue;
        }

        break;
    }

    /* none-prc column, we consider its type as unknown */
    if (prcType != SWCOL_LEVEL && prcType != SWCOL_ISLEAF &&
            prcType != SWCOL_ISCYCLE && prcType != SWCOL_ROWNUM) {
        prcType = SWCOL_UNKNOWN;
    }

    return prcType;
}

static void MarkPRCNotSkip(StartWithOp *swplan, int prcType)
{
    if (prcType == SWCOL_LEVEL || prcType == SWCOL_ROWNUM) {
        return;
    }

    switch (prcType) {
        case SWCOL_ISLEAF: {
            ClearSkipIsLeaf(swplan->swExecOptions);
            break;
        }
        case SWCOL_ISCYCLE: {
            ClearSkipIsCycle(swplan->swExecOptions);
            break;
        }
        default: {
            /* do nothing */
        }
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Generate internal entries for given CteScan node, here the term internal
 *         entry of key-value arraies prfixed with "array_key_nn" & col-value arraies
 *         prefixed with "array_col_mm" and store them in separate output list parm
 *
 * @param:
 *      - cteplan: the target CtePlan node where we generate key/col array
 *      - keyEntryList: A ** pointer to accept pseudo key entry
 *      - colEntryList: A ** pointer to accept pseudo col entry
 *
 * @Return: void, return value are set in keyEntryList & colEntryList
 * --------------------------------------------------------------------------------------
 */
static void GenerateStartWithInternalEntries(PlannerInfo *root, CteScan *cteplan,
                                             List **keyEntryList, List **colEntryList)
{
    Assert (IsA(cteplan, CteScan) && IsA(cteplan->subplan, StartWithOp));

    Plan *plan = (Plan *)cteplan;
    StartWithOp *swplan = (StartWithOp *)cteplan->subplan;
    ListCell *lc = NULL;
    List *tmp_list = NULL;

    /*
     * First, match cte targetEntry in funcs like connect_by_root(xxx) and
     * SYS_CONNECT_BY_PATH(xxx, '/')
     */
    foreach(lc, root->parse->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        List *vars = PullUpConnectByFuncVars(root, cteplan, (Node *)tle);
        tmp_list = list_concat(tmp_list, vars);
    }

    /* process those specified in where clause */
    foreach(lc, plan->qual) {
        TargetEntry *origin = (TargetEntry *)lfirst(lc);
        List *vars = PullUpConnectByFuncVars(root, cteplan, (Node *)origin);
        tmp_list = list_concat(tmp_list, vars);
    }

    foreach (lc, plan->targetlist) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);
        Assert (IsA(te->expr, Var));
        if (list_member(tmp_list, te->expr)) {
            *colEntryList = lappend(*colEntryList, te);
        }
    }

    /*
     * Second, match cte targetEntry of connectby prior columns, like prior pid = id then pid is
     * key
     */
    CommonTableExpr *cteRef = cteplan->cteRef;
    foreach(lc, cteRef->swoptions->prior_key_index) {
        int index = lfirst_int(lc);
        TargetEntry *te = (TargetEntry *)list_nth(plan->targetlist, index);
        *keyEntryList = lappend(*keyEntryList, te);
    }

    /* do not skip iscycle PRC if "nocycle" is specified in HQ clause */
    if (swplan->swoptions->nocycle) {
        ClearSkipIsCycle(swplan->swExecOptions);
    }

    return;
}

/*
 * @Brief: Add sort operator on top of CteScan with column with sort key
 *         {"LEVEL", "sibling_order_columns"}
 */
static OrderSiblingSortEntry* CreateOrderSiblingSortEntry(TargetEntry *entry, SortByDir dir)
{
    OrderSiblingSortEntry *sortEntry =
            (OrderSiblingSortEntry *)palloc0(sizeof(OrderSiblingSortEntry));

    sortEntry->tle = entry;
    switch (dir) {
        case SORTBY_DEFAULT:
        case SORTBY_ASC: {
            sortEntry->sortCmpOp[SORTCMP_LT] = true;
            sortEntry->sortCmpOp[SORTCMP_EQ] = true;
            sortEntry->sortCmpOp[SORTCMP_GT] = false;
            break;
        }
        case SORTBY_DESC: {
            sortEntry->sortCmpOp[SORTCMP_LT] = false;
            sortEntry->sortCmpOp[SORTCMP_EQ] = false;
            sortEntry->sortCmpOp[SORTCMP_GT] = true;
            break;
        }

        default: {
            /* for default case we treat as ASC */
            sortEntry->sortCmpOp[SORTCMP_LT] = true;
            sortEntry->sortCmpOp[SORTCMP_EQ] = false;
            sortEntry->sortCmpOp[SORTCMP_GT] = false;
        }
    }

    return sortEntry;
}

static Sort *CreateSiblingsSortPlan(PlannerInfo* root, Plan* lefttree,
                                    List *sortEntryList, double limit_tuples)
{
    Sort *sort = NULL;
    int numsortkeys = list_length(sortEntryList);
    AttrNumber* sortColIdx = (AttrNumber*)palloc(numsortkeys * sizeof(AttrNumber));
    Oid* sortOperators = (Oid*)palloc(numsortkeys * sizeof(Oid));
    Oid* collations = (Oid*)palloc(numsortkeys * sizeof(Oid));
    bool* nullsFirst = (bool*)palloc(numsortkeys * sizeof(bool));
    numsortkeys = 0;
    Oid sortoplt = InvalidOid;
    Oid sortopeq = InvalidOid;
    Oid sortopgt = InvalidOid;
    bool hashable = false;
    ListCell* l = NULL;

    foreach (l, sortEntryList) {
        OrderSiblingSortEntry *entry = (OrderSiblingSortEntry *)lfirst(l);

        get_sort_group_operators(exprType((Node*)entry->tle->expr),
                    entry->sortCmpOp[SORTCMP_LT],
                    entry->sortCmpOp[SORTCMP_EQ],
                    entry->sortCmpOp[SORTCMP_GT],
                    &sortoplt, &sortopeq, &sortopgt, &hashable);

        Oid sortop = InvalidOid;
        if (entry->sortCmpOp[SORTCMP_LT]) {
            sortop = sortoplt;
        } else if (entry->sortCmpOp[SORTCMP_GT]) {
            sortop = sortopgt;
        } else {
            /* we shouldn't get here */
            sortop = sortopeq;
        }

        sortColIdx[numsortkeys] = entry->tle->resno;
        sortOperators[numsortkeys] = sortop;
        collations[numsortkeys] = exprCollation((Node*)entry->tle->expr);
        nullsFirst[numsortkeys] = entry->sortByNullsFirst;

        numsortkeys++;
    }

    sort = make_sort(root, lefttree, numsortkeys, sortColIdx,
                     sortOperators, collations, nullsFirst, limit_tuples);

    return sort;
}

static bool IsNullsFirst(SortBy *sortby)
{
    Assert(sortby != NULL);
    bool ret = false;
    bool reverse = (sortby->sortby_dir == SORTBY_DESC);
    switch (sortby->sortby_nulls) {
        case SORTBY_NULLS_DEFAULT:
            /* NULLS FIRST is default for DESC; other way for ASC */
            ret = reverse;
            break;
        case SORTBY_NULLS_FIRST:
            ret = true;
            break;
        case SORTBY_NULLS_LAST:
            ret = false;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("unrecognized sortby_nulls: %d", sortby->sortby_nulls)));
            break;
    }
    return ret;
}

/*
 * @Brief: Generate sort key under recursive union according siblings sort key
 *         select * from t1 start with...connect by...order siblings by c1,c2,c3
 *         the plan like this:
 * ------------------QUERY PLAN--------------------
 *         > StartWithOP
 *             > Recursive Union
 *                 > Sort Key, sort by c1, c2, c3       <<<<< we are have
 *                     > Seqscan
 *                 > Sort Key, sort by c1, c2, c3       <<<<< we are have
 *                     > Seqscan
 */
static Sort *CreateSortPlanUnderRU(PlannerInfo* root, Plan* lefttree, List *siblings, double limit_tuples)
{
    Sort *sort = NULL;
    List *sortEntryList = NIL;
    ListCell *lc = NULL;
    ListCell *lc1 = NULL;

    foreach (lc, siblings) {
        SortBy *sb = (SortBy *)lfirst(lc);

        char *colname = GetOrderSiblingsColName(root, sb, lefttree);
        if (colname == NULL) {
            ereport(WARNING,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Order siblings by clause has unexpected key.")));

            continue;
        }

        bool found = false;
        /* search targetlist */
        foreach (lc1, lefttree->targetlist) {
            TargetEntry *tle = (TargetEntry *)lfirst(lc1);

            if (tle->resname == NULL) {
                continue;
            }

            /* one more fix name */
            char *label = strrchr(tle->resname, '@');
            label += 1;
            label = pstrdup(label);

            if (pg_strcasecmp(label, colname) == 0) {
                found = true;
                OrderSiblingSortEntry *entry =
                        CreateOrderSiblingSortEntry(tle,sb->sortby_dir);
                entry->sortByNullsFirst = IsNullsFirst(sb);
                sortEntryList = lappend(sortEntryList, entry);

                if (u_sess->attr.attr_sql.enable_startwith_debug) {
                    elog(WARNING, "Good we got siblings sort key under RU.");
                }
            }
        }

        if (!found) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                    errmsg("Siblings sort entry not found"),
                    errdetail("Column %s not found or not allowed here", colname),
                    errcause("Incorrect query input"),
                    erraction("Please check and revise your query")));
        }
    }

    if (sortEntryList == NIL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                errmsg("Invalid order siblings by clause"),
                errdetail("Siblings sort entry not found"),
                errcause("Incorrect query input"),
                erraction("Please check and revise your query")));

    }
    sort = CreateSiblingsSortPlan(root, lefttree, sortEntryList, limit_tuples);

    /* Free sort entries */
    list_free_deep(sortEntryList);

    return sort;
}


/*
 * --------------------------------------------------------------------------------------
 *    fix WorkTableScan's targetlist
 * --------------------------------------------------------------------------------------
 */


/*
 * @Brief: push down all pseudo columns into plan node access (start from topNode
 *         end to botNode)
 */
void PushDownFullPseudoTargetlist(PlannerInfo *root, Plan *topNode, Plan *botNode,
            List *fullEntryList)
{
    Assert (fullEntryList != NIL);

    PlanAccessPathSearchContext ctx;
    errno_t rc = EOK;
    rc = memset_s(&ctx, sizeof(PlanAccessPathSearchContext), 0, sizeof(PlanAccessPathSearchContext));
    securec_check(rc, "\0", "\0");

    ctx.topNode = topNode;
    ctx.botNode = botNode;
    ctx.fullEntryList = fullEntryList;

    for (int i = 0; i < MAX_PLAN_DEPTH; i++) {
        ctx.planStack.value_array[i] = NULL;
        ctx.varnoStack.value_array[i] = 0;
    }
    ctx.planStack.top = -1;
    ctx.varnoStack.top = -1;

    ctx.done = false;
    ctx.numsPrevTlist = -1;

    /*
     * Get the plan search path and add pseudo entry to let pseudo content return
     * along with executor iteration normally.
     */
    GetWorkTableScanPlanPath(root, (Plan *)topNode, &ctx);
    Assert (ctx.planStack.top == ctx.varnoStack.top);
    int nodeNums = ctx.planStack.top + 1;
    if (nodeNums == 0) {
        return;
    }

    StringInfoData si;
    initStringInfo(&si);

    /* Iterate the plan search path and add pseudo entry properly */
    Plan *plan = NULL;
    Index varno = 0;
    while (nodeNums > 0) {
        plan = StackNodePop(&ctx.planStack);
        varno = StackVarnoPop(&ctx.varnoStack);

        Assert (plan != NULL && varno > 0);

        /* bind current node with proper pseudo entry info */
        (void)BindPlanNodePseudoEntries(root, plan, varno, &ctx);

        /* record current numbers of tlist */
        ctx.numsPrevTlist = list_length(plan->targetlist);

        /* build debug path string */
        appendStringInfo(&si, " -> %s(tlist_len:%d)", nodeTagToString(nodeTag(plan)),
                        list_length(plan->targetlist));
        nodeNums--;
    }

    if (u_sess->attr.attr_sql.enable_startwith_debug) {
        elog(WARNING, "Pushdown pseudo_tlist >>>>> [%s]", si.data);
    }

    pfree_ext(si.data);

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Fix Array Internal Entry based on current targetlist level
 * --------------------------------------------------------------------------------------
 */
static void FixArrayInternalEntry(List *targetlsit)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;

    foreach(lc1, targetlsit) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc1);

        if (entry->resname != NULL &&
           (strstr(entry->resname, "array_key_") ||
            strstr(entry->resname, "array_col_"))) {
            int varno = ((Var *)entry->expr)->varno;
            int attno = entry->resname[10] - '0';
            int resno = 0;

            foreach(lc2, targetlsit) {
                TargetEntry *entry2 = (TargetEntry *)lfirst(lc2);

                if (GetPseudoColumnType(entry2) == SWCOL_REGULAR) {
                    int varno2 = ((Var *)entry2->expr)->varno;
                    int attno2 = ((Var *)entry2->expr)->varattno;

                    if (varno == varno2 && attno2 == attno) {
                        resno = entry2->resno;
                        break;
                    }
                }
            }

            char newArrayName[NAMEDATALEN];
            errno_t rc = memset_s(newArrayName, NAMEDATALEN, 0, NAMEDATALEN);
            securec_check(rc, "\0", "\0");

            if (strstr(entry->resname, "array_key_")) {
                rc = sprintf_s(newArrayName, NAMEDATALEN, "array_key_%d", resno);
                securec_check_ss(rc, "\0", "\0");
            } else if (strstr(entry->resname, "array_col_")) {
                rc = sprintf_s(newArrayName, NAMEDATALEN, "array_col_%d", resno);
                securec_check_ss(rc, "\0", "\0");
            }

            entry->resname = pstrdup(newArrayName);
        }
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Determine if a given pseudo target entry exits in given targetlist, the term
 *         "exists" says with identical varno/varattno
 * --------------------------------------------------------------------------------------
 */
static bool IsPseudoInternalEntryExists(List *targetlist, TargetEntry *tle)
{
    bool result = false;
    ListCell *lc = NULL;

    Assert (tle != NULL);

    /*
     * Exist emtry targetlist during start with...connect by push down pseudo columns.
     * e.x
     * Select * from (select t1.c1 as num from t1 join t2 on true) as ss, t3
     * start with ss.num = t3.c2
     * connect by prior t3.c1 = t3.c3;
     *
     * In this case, if we have plan like
     * Nestloop1 --> t1
     *           --> NestLoop2 --> t2
     *                         --> worktable
     *
     * Then the NestLoop2 don't have any targetlist, but we also should
     * inherit targetlist from wortable scan.
     * */
    if (targetlist == NULL) {
        return result;
    }


    foreach (lc, targetlist) {
        TargetEntry *curEntry = (TargetEntry *)lfirst(lc);
        if (GetPseudoColumnType(curEntry) == SWCOL_REGULAR) {
            /*
             * skip regular entry, we only check special entries e.g. pseodu return column,
             * internal entry
             */
            continue;
        }

        Assert (IsA(tle->expr, Var));
        if (pg_strcasecmp(curEntry->resname, tle->resname) == 0) {
            result = true;
            break;
        }
    }

    return result;
}

/*
 * --------------------------------------------------------------------------------------
 * @brief: Add each pseudo entry to arget plan node
 * --------------------------------------------------------------------------------------
 */
static void AddPseudoEntries(Plan *plan, Index relid, PlanAccessPathSearchContext *context)
{
    ListCell *lc = NULL;
    List *fullEntryList = context->fullEntryList;

    Assert (plan != NULL && relid > 0 && fullEntryList != NIL);

    int index = list_length(plan->targetlist);
    int attno = 0;
    bool adapt = false;

    /* caculate PseudoColumn varattno from prev plan targetlist */
    if (context->numsPrevTlist != -1) {
        attno = context->numsPrevTlist - list_length(fullEntryList);
        adapt = true;
    }

    /* Only add entry that does not added before */
    foreach (lc, fullEntryList) {
        TargetEntry *entry = (TargetEntry *)copyObject((TargetEntry *)lfirst(lc));

        /*
         * In "pseudo targetlist" push down process, we are going to avoid adding
         * duplicate entry we do such kind of check, not-exit puls resno is appended
         * at tail
         */
        if (!IsPseudoInternalEntryExists(plan->targetlist, entry)) {
            /*
             * The target entry's resno,varno,varattno is created in CteScan planing stage,
             * before we add it to curernt plan, we need do proper resno adjustment
             */
            Assert (IsA(entry->expr, Var));
            index++;
            ((Var *)entry->expr)->varno = relid;
            if (adapt) {
                attno++;
                ((Var *)entry->expr)->varattno = attno;
            }
            entry->resno = index;
            plan->targetlist = lappend(plan->targetlist, entry);
        }
    }

    if (context->botNode != NULL) {
        FixArrayInternalEntry(plan->targetlist);
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @brief: Add pseudo entries to arget plan node
 * --------------------------------------------------------------------------------------
 */
static void BindPlanNodePseudoEntries(PlannerInfo *root, Plan *plan,
                                      Index varno, PlanAccessPathSearchContext *context)
{
    ListCell *lc = NULL;

    switch (nodeTag(plan)) {
        case T_RecursiveUnion: {
            /*
             * for recursive-union, besides do proper pseudo entry adding, we need build
             * a separate list to help tupleslot-conversion(RuScan->StartWithOp)
             */
            RecursiveUnion *ruplan = (RecursiveUnion *)plan;

            /*
             * Already add pseudo entry to RecursiveUnion once order siblings by exist,
             * so we don't need add pseudo entry again.
             */
            if (ruplan->internalEntryList != NULL) {
                break;
            }

            AddPseudoEntries(plan, varno, context);
            foreach (lc, context->fullEntryList) {
                TargetEntry *entry = (TargetEntry *)copyObject((TargetEntry *)lfirst(lc));
                if (IsPseudoInternalTargetEntry(entry)) {
                    ruplan->internalEntryList = lappend(ruplan->internalEntryList, entry);
                }
            }
            break;
        }

        /* process regular case */
        case T_Hash:
        case T_HashJoin:
        case T_SubqueryScan:
        case T_CteScan:
        case T_NestLoop:
        case T_MergeJoin:
        case T_Sort:
        case T_Agg:
        case T_BaseResult:
        case T_Material:
        case T_Append:
        case T_Limit:
        case T_Unique:
        case T_WindowAgg:
        case T_WorkTableScan: {
            AddPseudoEntries(plan, varno, context);
            break;
        }

        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                    errmsg("%s is not supported in start with / connect by clauses", nodeTagToString(nodeTag(plan))),
                    errdetail("%s is not suppoted", nodeTagToString(nodeTag(plan))),
                    errcause("Input unsupported feature"),
                    erraction("NA")));
        }
    }
}

/*
 * --------------------------------------------------------------------------------------
 * @Brief: Generate a minimal set of plan-path reaching WTScan and to add pseudo entry,
 *         take example as follow: the generate path:
 *         RecursiveUnion -> HashJoin -> Hash -> WorkTableScan
 *
 *                CteScan
 *                 /
 *            StartWithOp
 *               /
 *        RecursiveUnion            >>> CONVERT TO >>>>>>>              RecursiveUnion
 *         /        \                                                           \
 *    SeqScan     HashJoin                                                     HashJoin
 *                 /   \                                                             \
 *           SeqScan   Hash                                                         Hash
 *                      /                                                             /
 *                   WorktableScan   <<<< target                                   WorkTableScan
 * --------------------------------------------------------------------------------------
 */
static void GetWorkTableScanPlanPath(PlannerInfo *root, Plan *node,
                                     PlanAccessPathSearchContext *context)
{
    StackNode  *planStack  = &context->planStack;
    StackVarno *varnoStack = &context->varnoStack;

    if (node == NULL) {
        return;
    }

    if (context->done) {
        return;
    }

    /* enqueue() */
    StackNodePush(planStack, node);

    switch (nodeTag(node)) {
        case T_CteScan: {
                CteScan *ctescan = (CteScan *)node;
                if ((Node *)context->botNode == (Node *)ctescan) {
                    context->done = true;

                    Assert (context->relid == 0);
                    context->relid = ctescan->scan.scanrelid;
                    StackVarnoPush(varnoStack, ctescan->scan.scanrelid);
                }
            }
            break;
        case T_WorkTableScan: {
                WorkTableScan *wtscan = (WorkTableScan *)node;

                /* mark finish and to exit the iteration call stack immediately */
                if (context->botNode == NULL) {
                    context->done = true;

                    /* set relid in current subquery level */
                    Assert (context->relid == 0);
                    context->relid = wtscan->scan.scanrelid;
                    StackVarnoPush(varnoStack, wtscan->scan.scanrelid);
                }
            }
            break;

        case T_VecSubqueryScan:
        case T_SubqueryScan: {
                SubqueryScan *sq = (SubqueryScan *)node;
                StackVarnoPush(varnoStack, sq->scan.scanrelid);
                GetWorkTableScanPlanPath(root, sq->subplan, context);
                if (!context->done) {
                    StackVarnoPop(varnoStack);
                }
            }
            break;
        default: {

            Plan *lplan = outerPlan(node);
            Plan *rplan = innerPlan(node);

            /*
             * We need handle a case where RecursiveUnion's inner branch is BaseResult
             * node, normally this happens a connectByExpr is evaluate as FALSE, instad
             * of WT-join a BaseResult node is planned, e.g.
             * e.g.
             * [QUERY]
             *    SELECT * FROM test_hcb_ptb t1
             *    START WITH id=141
             *    CONNECT BY (prior pid)=id and prior pid>10 and 1=0;
             *
             * [PLAN]
             *                          QUERY PLAN
             *    ------------------------------------------------------
             *     CTE Scan on tmp_reuslt
             *       CTE tmp_reuslt
             *         ->  StartWith Operator
             *               Start With pseudo atts: RUITR, array_key_9
             *               ->  Recursive Union
             *                     ->  Seq Scan on test_hcb_ptb
             *                           Filter: (id = 141)
             *                     ->  Result
             *                           One-Time Filter: false
             *    (9 rows)
             *
             *
             */
            if (IsA(node, RecursiveUnion) && IsA(rplan, BaseResult) &&
                    context->botNode == NULL) {
                context->done = true;
                StackVarnoPush(varnoStack, INNER_VAR);

                /* print-out debug information */
                ereport(DEBUG1,
                        (errmodule(MOD_OPT_PLANNER), errmsg("SWCB query %s does not generate WTScan",
                        t_thrd.postgres_cxt.debug_query_string)));

                break;
            }

            /*
             * Before drill down into outer/inner plan tree, we set varno as OUTER_VAR/INNER for
             * current level of plan node, as suppose the next outer/inner plan will build the final
             * plan access path
             */
            if (!context->done) {
                StackVarnoPush(varnoStack, OUTER_VAR);
            }
            GetWorkTableScanPlanPath(root, lplan, context);
            if (!context->done) {
                StackVarnoPop(varnoStack);
            }

            /* process right tree */
            if (!context->done) {
                /* set varno for current level of plan node, we need skip once lplan is done */
                StackVarnoPush(varnoStack, INNER_VAR);
            }
            GetWorkTableScanPlanPath(root, rplan, context);
            if (!context->done) {
                StackVarnoPop(varnoStack);
            }
        }
    }

    /* dequeue() */
    if (!context->done) {
        StackNodePop(planStack);
    }

    return;
}

/*
 * --------------------------------------------------------------------------------------
 * @brief:
 *   We do some special post-planing work for each StartWithOp node, normally, invokced from
 *   top planning level(root->query_level == 1)'s subplans, detail as follow:
 *      1. Block some unspported case identified at planning stage
 *      2. Add internal targetlist(ruitr, array_key, array_col) for Startwith
 *      3. DFX to support output internal columns from a start with converted CTE
 *
 *
 * @Note: There are two cases of of pseudo entries push-down for internal targetlist
 *      case1: StartWithOp => WorkTableScan, level information is store in WTScan and can be
 *             return to RecursiveUnion
 *      case2: TOP-1/TOP => CteScan, internal information need pop up to query's targetlist level,
 *             so that connect_by_root(), sys_connect_by_path() can get proper information
 *
 *              Result        ---------------
 *                 |                |
 *                ...               |
 *                 |              case2: (ruitr/array_key/array_col)
 *              CteScan             |
 *                 |                |
 *              StartWithOp    ---------------
 *                 |                |
 *              RecursiveUnion      |
 *                 |                |
 *              HashJoin          case1: (level/isleaf/iscycle，ruitr/array_key/array_col)
 *                 |                |
 *                ...               |
 *                 |                |
 *              WorkTableScan  ---------------
 *
 * --------------------------------------------------------------------------------------
 */
void ProcessStartWithOpMixWork(PlannerInfo *root, Plan *topplan,
                               PlannerInfo *subroot, StartWithOp *swplan)
{
    Assert (IsA(swplan, StartWithOp));

    /*
     * 1. Build fullEntry from current StartWithOp node to "start with"
     *    WorkTable
     */
    PushDownFullPseudoTargetlist(root, (Plan *)swplan->ruplan,
                                 NULL, swplan->fullEntryList);

    /*
     * 2. Add internalEntryList to CteScan node to support SWCB functions, by defualt
     *    we add full internal entries, only do this when there is SWCB functions exits,
     *    where check the StartWithOp's colEntryList
     */
    if (swplan->colEntryList != NIL) {
        CteScan *cteplan = swplan->cteplan;
        PushDownFullPseudoTargetlist(root, (Plan *)cteplan, (Plan *)cteplan,
                                     swplan->internalEntryList);
    }

    /*
     * 3. Removing the unnecessary "Internal Target Entry" created by PRC push-down,
     *    ruitr/array_key/array_col, normally this kind of entry only visible on
     *    CteScan node
     */
    if (!u_sess->attr.attr_sql.enable_startwith_debug) {
        List *newList = NIL;
        ListCell *lc = NULL;
        foreach (lc, topplan->targetlist) {
            TargetEntry *entry = (TargetEntry *)lfirst(lc);
            if (IsPseudoInternalTargetEntry(entry)) {
                continue;
            }

            newList = lappend(newList, entry);
        }

        topplan->targetlist = newList;
    }

    return;
}
