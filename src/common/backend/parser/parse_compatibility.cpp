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



 /*
 函数/结构体	作用
JoinTerm 结构体	表示可转换的 "(+)" 外连接的术语，包括左右 RangeTblEntry、单列连接表达式的列表、连接条件和连接类型。
OperatorPlusProcessContext 结构体	在识别和处理查询中的 "(+)" 外连接的过程中保持上下文信息的结构体。
preprocess_plus_outerjoin	预处理查询的 WHERE 子句，识别并提取 "(+)" 外连接条件。
convert_plus_outerjoin	将预处理的 "(+)" 外连接条件转换为相应的 LEFT/RIGHT OUTER JOIN 表达式。
aggregate_jointerms	将预处理的 "(+)" 外连接条件聚合为 JoinTerm 结构体的列表。
plus_outerjoin_check	检查 WHERE 子句是否包含 "(+)" 外连接运算符，并相应地设置标志。
getOperatorPlusFlag	获取标志，指示查询是否包含 "(+)" 外连接运算符。
resetOperatorPlusFlag	重置指示是否存在 "(+)" 外连接运算符的标志。
makePlusJoinInfo	创建 PlusJoinRTEInfo 结构体，用于记录 "(+)" 外连接的信息。
makePlusJoinRTEItem	创建 PlusJoinRTEItem 结构体，用于记录 RangeTblEntry 的信息和是否具有 "(+)" 外连接。
setIgnorePlusFlag	设置解析期间是否忽略 "(+)" 外连接运算符的标志。
insert_jointerm	将连接表达式（例如 t1.c1 = t2.c1）插入到 JoinTerm 结构体中，并与相关的 RangeTblEntry 和条件关联。
find_joinexpr	在 JoinExpr 中查找与给定的 RangeTblRef 关联的连接表达式。
get_JoinArg	从指定的 RangeTblEntry 中获取连接参数（Node）。
plus_outerjoin_get_rtindex	获取与指定节点和 RangeTblEntry 关联的 RangeTblRef 索引。
convert_one_plus_outerjoin	将单个 (+) 外连接表达式转换为 LEFT/RIGHT OUTER JOIN。
InitOperatorPlusProcessContext	初始化处理 "(+)" 外连接的上下文。
contain_ColumnRefPlus	检查给定表达式是否在 ColumnRef 中包含 "(+)" 外连接运算符。
contain_ExprSubLink	检查给定表达式是否包含 ExprSubLink。
contain_JoinExpr	检查给定列表是否包含 JoinExpr。
contain_AEXPR_AND_OR	检查给定表达式是否包含 AEXPR_AND/OR。
unsupport_syntax_plus_outerjoin	处理与 "(+)" 外连接相关的不支持的语法。
contain_ColumnRefPlus_walker	用于检查 ColumnRef 是否包含 "(+)" 外连接运算符的 Walker 函数。
contain_ExprSubLink_walker	用于检查表达式是否包含 ExprSubLink 的 Walker 函数。
contain_AEXPR_AND_OR_walker	用于检查表达式是否包含 AEXPR_AND/OR 的 Walker 函数。
 
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
insert_jointerm 函数的功能是将连接表达式插入到 JoinTerm 结构体中，
其中 JoinTerm 表示可转换的 "(+)" 外连接的术语。函数会遍历已有的 JoinTerm 列表，查找与给定参数匹配的项。
如果找到匹配项，则将表达式插入到该 JoinTerm 中；如果未找到匹配项，则创建新的 JoinTerm 并插入到列表中。
如果 JoinTerm 的 rrte 为 NULL，则会被参数中的 rrte 覆盖。
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
aggregate_jointerms 函数的功能是整理连接条件，将连接条件聚合到 JoinTerm 结构体中的 joincond 字段中。
该函数会遍历 JoinTerm 列表，对于每个 JoinTerm，它会将其中的连接条件合并成一个逻辑AND表达式，存储在 joincond 中。如果 JoinTerm 的 rrte 为 NULL，表示该项为 "(+)" 外连接的左表，此时会将连接条件放回到原始的 whereClause 中。
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
 *find_joinexpr_walker 函数的作用是在查询树中查找指定的 RangeTblRef，判断该引用是否存在于查询树中的 JoinExpr 中的左表或右表。
 该函数是一个递归遍历树的过程，通过深度优先搜索遍历树的每个节点，判断节点的类型，如果是 RangeTblRef，则比较其 rtindex 是否与目标一致，如果是 JoinExpr，则递归遍历其左右子树。
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
find_joinexpr 函数的作用是在给定的 JoinExpr 中查找指定的 RangeTblRef 是否存在。函数内部通过调用 find_joinexpr_walker 函数，通过深度优先搜索遍历 JoinExpr 中的左右子树，查找目标 RangeTblRef 是否存在。如果找到了，将 found 设置为 true，表示找到了指定的 RangeTblRef。
 * - brief: Check if given RangeTblRef exists in jexpr(JoinExpr), major used in multi-table
 *          converted case where, e.g.
 find_RangeTblRef 函数的作用是在给定的 p_rtable 中查找指定的 RangeTblEntry 的索引。函数通过遍历 p_rtable 列表，比较其中的每个 RangeTblEntry 是否与目标相等，如果相等则返回对应的索引（从 1 开始），如果未找到则返回 0。
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

 /*
 get_JoinArg 函数的作用是在给定的查询上下文和范围表条目 rte 的情况下，获取与该范围表条目相关的 JoinExpr 或 RangeTblRef。

首先，通过调用 find_RangeTblRef 函数获取范围表条目 rte 在查询上下文中范围表中的索引，构建 RangeTblRef 结构，并确保索引不为 0。
然后，遍历查询上下文中的 p_joinlist 列表，查找其中与构建的 RangeTblRef 相同索引的 RangeTblRef，如果找到，从 p_joinlist 中删除该 RangeTblRef 并返回。
如果在 p_joinlist 中没有找到与 RangeTblRef 相同索引的 RangeTblRef，再次遍历 p_joinlist，查找其中的 JoinExpr，调用 find_joinexpr 函数判断是否包含目标 RangeTblRef，如果找到，同样从 p_joinlist 中删除该 JoinExpr 并返回。
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

 /*
 
preprocess_plus_outerjoin 函数的作用：

调用 preprocess_plus_outerjoin_walker 函数对 whereClause 进行预处理，将其中的 "(+)" 转换为外连接条件。这是 "(+)" 外连接转换的入口。

调用 aggregate_jointerms 函数将 preprocess_plus_outerjoin_walker 返回的连接条件进行聚合。连接条件形如 "t1.c1=t2.c1, t1.c2=t2.c1, t1.c2=t2.c2"，需要将其聚合成一个 JoinExpr。

调用 plus_outerjoin_check 函数进行检查。

如果 jointerms 列表非空，表示当前语句层次包含 "(+)" 连接转换，将 contain_plus_outerjoin 标记为 true。
 
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
/*
plus_outerjoin_get_rtindex 函数的作用：

如果给定的节点是 RangeTblRef 类型，则返回其 rtindex。
如果给定的节点是 JoinExpr 类型，则调用 find_RangeTblRef 函数获取对应的 rtindex。
两个函数一起完成了 "(+)" 外连接的预处理和检查工作。
*/

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
 * ChatGPT
convert_one_plus_outerjoin 函数的作用：

根据传入的 JoinTerm 结构，创建一个 JoinExpr 节点，并设置其左右子树为通过 get_JoinArg 函数获取的对应 RangeTblRef 或 JoinExpr。
检查左右子树是否为空，如果有一个为空，则报错，因为 "(+)" 连接条件处理后应该形成一颗连接树。
获取左右子树在 p_rtable 中的 rtindex。
设置 JoinExpr 的 quals 为 JoinTerm 中存储的连接条件。
设置 JoinExpr 的其他属性，如 jointype。
根据左右子树的 rtindex 和连接条件创建新的 RangeTblEntry。
将新的 RangeTblEntry 添加到 p_rtable 中，并将其 rtindex 设置为 JoinExpr 的 rtindex。
将 JoinExpr 添加到 p_joinlist 中。
最后，将 JoinExpr 添加到 p_joinexprs 中。
这个函数完成了 "(+)" 外连接的转换工作，生成了 JoinExpr 表达式，并将其添加到相应的列表中。
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
 *该函数用于检查 "(+)" 外连接条件的合法性，主要检查以下情况：
是否存在多个左关系表与相同的右关系表进行外连接，例如 "t1.c1 = t2.c1(+) and t3.c1 = t2.c2(+)"。
如果存在上述情况，报错，因为 "(+)" 连接条件在语法上不支持多个左表与相同的右表进行连接。
该函数通过遍历 ctx->jointerms 中的每个 JoinTerm，并检查是否存在上述情况的冲突，如果存在，则报错。
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
 plus_outerjoin_precheck 函数的作用：

该函数用于在进行 "(+)" 外连接转换之前进行预检查，主要检查以下情况：
是否存在没有 "(+)" 的情况，如果是，则不进行外连接转换。
是否在多个关系表上同时指定了 "(+)"，如果是，则报错。
是否 "(+)" 与 JoinExpr 同时出现在 FROM 子句中，如果是，则报错。
是否 "(+)" 与 OR 子句同时出现，如果是，则报错。
是否在 OR 子句中存在 "(+)"，如果是，则报错。
是否 "(+)" 出现在不同层次的查询中，如果是，则忽略 "(+)"。
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

/*

unsupport_syntax_plus_outerjoin 函数的作用：

该函数用于检查在外连接转换过程中不支持的语法结构，主要包括以下情况的检查：
是否包含了外连接 "(+)" 和子查询 SubLink 结构的组合，如果是，则报错。
是否包含了 "(+)" 和嵌套表达式 (AND/OR) 的组合，如果是，则报错。
如果检查出不支持的语法结构，会报错并指出具体的位置。
*/

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

/*
plus_outerjoin_preprocess 函数的作用：

该函数用于在进行 "(+)" 外连接转换之前的预处理。主要进行以下步骤：
调用 unsupport_syntax_plus_outerjoin 检查语法结构，报错处理不支持的结构。
设置解析状态中的标志，使其忽略 "(+)"，然后调用 transformExpr 遍历表达式。
在遍历过程中，记录涉及到的关系表（RTE）信息，包括是否含有 "(+)"。
根据记录的信息，进行外连接的预检查，如果通过，将信息插入到外连接项中，用于后续处理。
该函数最终返回一个布尔值，表示是否成功进行了 "(+)" 外连接的预处理。
*/
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
 preprocess_plus_outerjoin_walker 函数的作用：

该函数是一个递归遍历表达式树的函数，用于在表达式中查找 "(+)" 外连接的语法结构，并进行相应的处理。
遍历过程中，对于不同的 A_Expr 类型的节点，会有不同的处理逻辑：
对于 AEXPR_OR 类型，标记当前在 OR 子句中，然后递归处理左右子表达式。
对于 AEXPR_AND 类型，递归处理左右子表达式。
对于其他 A_Expr 类型，调用 plus_outerjoin_preprocess 进行预处理，如果返回 true，说明发现了可以转换的外连接结构，将当前节点替换为一个始终为真的条件。
对于其他节点类型，同样调用 plus_outerjoin_preprocess 进行预处理。
最终达到的效果是将表达式中的 "(+)" 转换为对应的外连接条件，并将表达式中的 "(+)" 移到了连接条件中
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
sColumnRefPlusOuterJoin 函数的作用：

该函数用于判断给定的 ColumnRef 节点是否含有 "(+)"，即判断是否是外连接的语法结构。
如果 ColumnRef 的最后一个元素是字符串 "(+)"，则返回 true，表示含有 "(+)"。

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

convert_plus_outerjoin 函数的作用：

该函数用于将在预处理阶段收集到的外连接信息进行转换，将 "(+)" 转换为对应的外连接结构。
通过循环遍历 ctx->jointerms 中的每一个 JoinTerm，调用 convert_one_plus_outerjoin 函数进行转换。
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
 * InitOperatorPlusProcessContext 函数的作用：

用于初始化 OperatorPlusProcessContext 结构体，设置初始的上下文信息。
将 contain_plus_outerjoin 置为 false，表示当前上下文中还没有包含 "(+)" 外连接。
将 jointerms 初始化为空列表。
将 ps 字段设置为传入的 pstate。
将 in_orclause 置为 false，表示当前不在 OR 子句中。
将 whereClause 字段设置为传入的 whereClause。
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
 * transformOperatorPlus 函数的作用：

是外部调用的主函数，用于将表达式中的 "(+)" 转换为相应的外连接结构。
初始化 OperatorPlusProcessContext 上下文。
备份原始的 whereClause。
调用 preprocess_plus_outerjoin 进行预处理，收集可能的外连接信息。
如果包含了 "(+)" 外连接，则调用 convert_plus_outerjoin 进行转换。
如果没有包含 "(+)" 外连接，恢复原始的 whereClause。
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

