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


/*
------------------------------------------------------------------------------
函数                                          | 功能
------------------------------------------------------------------------------
extractRemainingColumns                         | 从源列中提取剩余的列
transformJoinUsingClause                         | 转换连接的 USING 子句
transformTableEntry                             | 将 RangeVar 转换为 RangeTblEntry
transformCTEReference                           | 转换表示通用表达式 (CTE) 引用的 RangeVar
transformRangeSubselect                         | 转换 RangeSubselect 节点
transformRangeFunction                          | 转换 RangeFunction 节点
transformRangeTableSample                       | 转换 RangeTableSample 节点
transformRangeTimeCapsule                       | 转换 RangeTimeCapsule 节点
setNamespaceLateralState                        | 为命名空间设置 lateral 状态
buildMergedJoinVar                              | 构建合并的 JoinVar 用于连接列
checkExprIsVarFree                              | 检查表达式是否不包含变量
findTargetlistEntrySQL92                        | 在目标列表中查找 SQL-92 子句的目标项
findTargetlistEntrySQL99                        | 在目标列表中查找 SQL:1999 子句的目标项
get_matching_location                           | 获取在排序组引用列表中匹配的位置
addTargetToGroupList                            | 将目标项添加到分组列表中
findWindowClause                                | 根据名称在列表中查找 WindowClause
transformFrameOffset                            | 转换帧偏移子句
flatten_grouping_sets                           | 将分组集展平为列表
transformGroupingSet                             | 转换 GroupingSet 节点
transformGroupClauseExpr                        | 转换 GroupClause 表达式
CheckOrderbyColumns                             | 检查 ORDER BY 子句中的列
------------------------------------------------------------------------------

*/


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
static TargetEntry* findTargetlistEntrySQL92(ParseState* pstate, Node* node, List** tlist, int clause, ParseExprKind exprKind);
static TargetEntry* findTargetlistEntrySQL99(ParseState* pstate, Node* node, List** tlist, ParseExprKind exprKind);
static int get_matching_location(int sortgroupref, List* sortgrouprefs, List* exprs);
static List* addTargetToGroupList(
    ParseState* pstate, TargetEntry* tle, List* grouplist, List* targetlist, int location, bool resolveUnknown);
static WindowClause* findWindowClause(List* wclist, const char* name);
static Node* transformFrameOffset(ParseState* pstate, int frameOptions, Node* clause);
static Node* flatten_grouping_sets(Node* expr, bool toplevel, bool* hasGroupingSets);
static Node* transformGroupingSet(List** flatresult, ParseState* pstate, GroupingSet* gset, List** targetlist,
    List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel);

static Index transformGroupClauseExpr(List** flatresult, Bitmapset* seen_local, ParseState* pstate, Node* gexpr,
    List** targetlist, List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel);
static void CheckOrderbyColumns(ParseState* pstate, List* targetList, bool isAggregate);

/*
transformItem:

功能：将RangeTblEntry转换为RangeTblRef，并添加到范围表中。
参数：
ParseState* pstate：解析状态。
RangeTblEntry* rte：范围表条目。
RangeTblEntry** top_rte：顶层范围表条目的指针。
int* top_rti：顶层范围表索引的指针。
List** relnamespace：命名空间的指针。
返回值：转换后的RangeTblRef。
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
transformFromClause:

功能：处理FROM子句，将其中的关系转换为查询的范围表、连接列表和命名空间。
参数：
ParseState* pstate：解析状态。
List* frmList：FROM子句中的关系列表。
bool isFirstNode：标识是否是第一个节点。
bool isCreateView：标识是否是创建视图的操作。
bool addUpdateTable：标识是否将目标表添加到更新表的列表中。
返回值：无。
 */
void transformFromClause(ParseState* pstate, List* frmList, bool isFirstNode, bool isCreateView, bool addUpdateTable)
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
            pstate, n, &rte, &rtindex, NULL, NULL, &relnamespace, isFirstNode, isCreateView, false, addUpdateTable);

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
setTargetTable:

功能：将INSERT/UPDATE/DELETE目标表添加到范围表中，并进行相关设置。
参数：
ParseState* pstate：解析状态。
RangeVar* relRv：目标表的RangeVar。
bool inh：标识是否包含继承表。
bool alsoSource：标识是否将目标表添加到查询的连接列表和命名空间中。
AclMode requiredPerms：目标表的权限要求。
bool multiModify：标识是否为多表修改操作。
返回值：目标表在范围表中的索引。

 */
int setTargetTable(ParseState* pstate, RangeVar* relRv, bool inh, bool alsoSource, AclMode requiredPerms,
                   bool multiModify)
{
    RangeTblEntry* rte = NULL;
    Relation relation = NULL;
    int index = 0;

    /*
     * for DELETE and UPDATE, maybe target table has been added to rtable before.
     * if that, no need to do that again, just set resultRelation to the existed rtindex.
     */
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        if (multiModify && (requiredPerms & ACL_UPDATE)) {
            relation = parserOpenTable(pstate, relRv, RowExclusiveLock, true, false, true);
            /* When update multiple relations, rte has been just now added to p_rtable in transformFromClauseItem. */
            rte = (RangeTblEntry *)llast(pstate->p_rtable);
            index = list_length(pstate->p_rtable);
        } else if (requiredPerms & ACL_DELETE) {
            int rtindex = 1;
            /* for sql_compatibility B, relation may has been added in p_rtable by usingClause. */
            foreach_cell (l, pstate->p_rtable) {
                RangeTblEntry *rte1 = (RangeTblEntry *)lfirst(l);
                if (relRv->alias != NULL && strcmp(rte1->eref->aliasname, relRv->alias->aliasname) == 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_ALIAS), errmsg("table name \"%s\" specified more than once",
                            relRv->alias->aliasname)));
                } else if (relRv->alias == NULL && strcmp(rte1->eref->aliasname, relRv->relname) == 0) {
                    if (list_member_ptr(pstate->p_target_rangetblentry, rte1)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DUPLICATE_ALIAS), errmsg("table name \"%s\" specified more than once",
                                relRv->relname)));
                    }
                    rte = rte1;
                    index = rtindex;
                    relRv->relname = rte1->relname;
                    relation = parserOpenTable(pstate, relRv, RowExclusiveLock, true, false, true);
                    break;
                }
                rtindex++;
            }
        }
    }

    if (index == 0) {
        relation = parserOpenTable(pstate, relRv, RowExclusiveLock, true, false, true);
        /*
        * Now build an RTE.
        */
        rte = addRangeTableEntryForRelation(pstate, relation, relRv->alias, inh, false);
    }
    pstate->p_target_relation = lappend(pstate->p_target_relation, relation);

    if (requiredPerms & ACL_UPDATE) {
        pstate->p_updateRangeVars = lappend(pstate->p_updateRangeVars, relRv);
    }

    /* IUD contain partition. */
    if (relRv->ispartition) {
        rte->isContainPartition = GetPartitionOidForRTE(rte, relRv, pstate, relation);
    }
    /* IUD contain subpartition. */
    if (relRv->issubpartition) {
        rte->isContainSubPartition = GetSubPartitionOidForRTE(rte, relRv, pstate, relation);
    }
    /* delete from clause contain PARTIION (..., ...). */
    if (list_length(relRv->partitionNameList) > 0) {
        GetPartitionOidListForRTE(rte, relRv);
    }

    pstate->p_target_rangetblentry = lappend(pstate->p_target_rangetblentry, rte);

    /*
     * Restrict DML privileges to pg_authid which stored sensitive messages like rolepassword.
     * there are lots of system catalog and restrict permissions of all system catalog
     * may cause too much influence. so we only restrict permissions of pg_authid temporarily
     *
     * Restrict DML privileges to gs_global_config which stored parameters like bucketmap
     * length. These parameters will not be modified after initdb.
     */
    if (IsUnderPostmaster && !g_instance.attr.attr_common.allowSystemTableMods &&
        !u_sess->attr.attr_common.IsInplaceUpgrade && IsSystemRelation(relation) &&
        (strcmp(RelationGetRelationName(relation), "pg_authid") == 0 ||
        strcmp(RelationGetRelationName(relation), "gs_global_config") == 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog",
                    RelationGetRelationName(relation))));
    }

    /* Restrict DML privileges to gs_global_chain which stored history and consistent message like hash. */
    if (IsUnderPostmaster && !u_sess->attr.attr_common.IsInplaceUpgrade && IsSystemRelation(relation)
        && strcmp(RelationGetRelationName(relation), "gs_global_chain") == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog",
                    RelationGetRelationName(relation))));
    }

    if (IS_FOREIGNTABLE(relation)||IS_STREAM_TABLE(relation)) {
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
     * when index equal to 0, indicate that result relation does not exist in rtable.
     * assume new rte is at end.
     */
    if (index == 0) {
        index = list_length(pstate->p_rtable);
        Assert(rte == rt_fetch(index, pstate->p_rtable));
        /*
         * If UPDATE/DELETE, and haven't added by fromClause/usingClause, add table to joinlist and namespaces.
         * But when UPDATE use relationClause, no need to add.
         */
        if (alsoSource) {
            addRTEtoQuery(pstate, rte, true, true, true);
        } else if (IS_SUPPORT_RIGHT_REF(pstate->rightRefState)) {
            addRTEtoQuery(pstate, rte, false, false, true);
        }
    }
    return index;
}

/*
setTargetTables:

功能：将目标表的RangeVar列表转换为范围表索引的列表，并对每个目标表进行设置。
参数：
ParseState* pstate：解析状态。
List* relations：目标表的RangeVar列表。
bool expandInh：标识是否扩展继承关系。
bool alsoSource：标识是否将目标表添加到查询的连接列表和命名空间中。
AclMode requiredPerms：目标表的权限要求。
返回值：目标表在范围表中的索引列表。
interpretInhOption:

功能：根据InhOption的值（yes/no/default）解释成布尔值。
参数：
InhOption inhOpt：继承选项的枚举值。
返回值：布尔值，表示继承选项是否为"yes"。
interpretOidsOption:

功能：根据表的选项列表（DefElem列表）判断表是否需要使用OIDs。
参数：
List* defList：表的选项列表。
返回值：布尔值，表示是否需要使用OIDs。
这些函数主要用于处理目标表的设置，包括继承选项、OIDs选项等，并最终返回目标表在范围表中的索引列表。其中，setTargetTables函数通过调用setTargetTable函数来处理每个目标表的设置。

*/
List* setTargetTables(ParseState* pstate, List* relations, bool expandInh, bool alsoSource, AclMode requiredPerms)
{
    List* rtindex = NULL;
    ListCell* l;
    bool inhOpt = false;
    bool multiModify = (list_length(relations) > 1);

    /* Close old target; this could only happen for multi-action rules */
    foreach (l, pstate->p_target_relation) {
        Relation relation = (Relation)lfirst(l);
        if (relation != NULL) {
            heap_close(relation, NoLock);
        }
    }

    /*
     * Open target rel and grab suitable lock (which we will hold till end of
     * transaction).
     *
     * free_parsestate() will eventually do the corresponding heap_close(),
     * but *not* release the lock.
     */
    foreach (l, relations) {
        RangeVar* relRv = (RangeVar*)lfirst(l);
        if (expandInh) {
            inhOpt = interpretInhOption(relRv->inhOpt);
        }
        rtindex = lappend_int(rtindex, setTargetTable(pstate, relRv, inhOpt, alsoSource, requiredPerms, multiModify));
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


/*
transformJoinUsingClause函数：

功能：根据使用USING子句指定的列构建完整的ON条件。
参数：
ParseState* pstate：解析状态。
RangeTblEntry* leftRTE：左侧关系表条目。
RangeTblEntry* rightRTE：右侧关系表条目。
List* leftVars：左侧关系表的列变量列表。
List* rightVars：右侧关系表的列变量列表。
返回值：一个已经经过转换的ON条件。
transformJoinOnClause函数：

功能：转换JOIN/ON子句中的条件表达式。
参数：
ParseState* pstate：解析状态。
JoinExpr* j：JOIN表达式。
RangeTblEntry* l_rte：左侧关系表条目。
RangeTblEntry* r_rte：右侧关系表条目。
List* relnamespace：关系表的命名空间。
返回值：已转换的ON条件表达式。
这两个函数主要用于处理JOIN操作中的条件部分。transformJoinUsingClause函数构建ON条件，通过对左右关系表的列进行匹配，生成等值比较的条件。而transformJoinOnClause函数用于转换JOIN/ON子句中的条件表达式，包括设置解析状态中的命名空间等操作。
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
    result = transformExpr(pstate, result, EXPR_KIND_JOIN_USING);

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

    result = transformWhereClause(pstate, j->quals, EXPR_KIND_JOIN_ON, "JOIN/ON");

    pstate->p_relnamespace = save_relnamespace;
    pstate->p_varnamespace = save_varnamespace;

    return result;
}

/*
 * transformTableEntry --- transform a RangeVar (simple relation reference)
 */

/*
transformTableEntry函数：

功能：转换RangeVar（简单关系引用）为关系表条目（RangeTblEntry）。
参数：
ParseState* pstate：解析状态。
RangeVar* r：RangeVar结构，表示关系引用。
bool isFirstNode：标志是否是FROM子句中的第一个节点。
bool isCreateView：标志是否是CREATE VIEW语句中的关系表。
返回值：已转换的关系表条目。
transformCTEReference函数：

功能：转换引用公共表达式（Common Table Expression，CTE）的RangeVar。
参数：
ParseState* pstate：解析状态。
RangeVar* r：RangeVar结构，表示关系引用。
CommonTableExpr* cte：指向CommonTableExpr的指针。
Index levelsup：CTE的层级。
返回值：已转换的关系表条目。
transformRangeSubselect函数：

功能：转换出现在FROM子句中的子查询（sub-SELECT）为关系表条目。
参数：
ParseState* pstate：解析状态。
RangeSubselect* r：RangeSubselect结构，表示子查询。
返回值：已转换的关系表条目。
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

    if (r->ispartition || r->issubpartition || list_length(r->partitionNameList) > 0) {
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
/*
transformRangeFunction函数：

功能：转换RangeFunction（代表函数调用）为关系表条目（RangeTblEntry）。
参数：
ParseState* pstate：解析状态。
RangeFunction* r：RangeFunction结构，表示函数调用。
返回值：已转换的关系表条目。
transformRangeTableSample函数：

功能：转换RangeTableSample（代表TABLESAMPLE子句）为TableSampleClause。
参数：
ParseState* pstate：解析状态。
RangeTableSample* rts：RangeTableSample结构，表示TABLESAMPLE子句。
返回值：已转换的TableSampleClause。
transformRangeTimeCapsule函数：

功能：转换RangeTimeCapsule（代表TABLECAPSULE子句）为TimeCapsuleClause。
参数：
ParseState* pstate：解析状态。
RangeTimeCapsule* rtc：RangeTimeCapsule结构，表示TABLECAPSULE子句。
返回值：已转换的TimeCapsuleClause。
这些函数主要用于处理FROM子句中的特殊元素，如函数调用、TABLESAMPLE子句和TABLECAPSULE子句。transformRangeFunction处理函数调用，为其创建关系表条目；transformRangeTableSample处理TABLESAMPLE子句，为其创建TableSampleClause；transformRangeTimeCapsule处理TABLECAPSULE子句，为其创建TimeCapsuleClause。
*/
static RangeTblEntry* transformRangeFunction(ParseState* pstate, RangeFunction* r)
{
    Node* funcexpr = NULL;
    char* funcname = NULL;
    RangeTblEntry* rte = NULL;
    Node *last_srf;

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
    last_srf = pstate->p_last_srf;
    funcexpr = transformExpr(pstate, r->funccallnode, EXPR_KIND_FROM_FUNCTION);

    if (pstate->p_is_flt_frame) {
        /* nodeFunctionscan.c requires SRFs to be at top level */
        if (pstate->p_last_srf != last_srf && pstate->p_last_srf != funcexpr)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("set-returning functions must appear at top level of FROM"),
                            parser_errposition(pstate, exprLocation(pstate->p_last_srf))));
    }

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

        arg = transformExpr(pstate, arg, EXPR_KIND_FROM_FUNCTION);
        arg = coerce_to_specific_type(pstate, arg, FLOAT4OID, "TABLESAMPLE");
        assign_expr_collations(pstate, arg);
        fargs = lappend(fargs, arg);
    }
    tablesample->args = fargs;

    /* Process REPEATABLE (seed) */
    if (rts->repeatable != NULL) {
        Node* arg = NULL;

        arg = transformExpr(pstate, rts->repeatable, EXPR_KIND_FROM_FUNCTION);
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

    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED),
            errmsg("cannot execute TimeCapsule Query in recovery state")));
    }

    timeCapsule = makeNode(TimeCapsuleClause);

    timeCapsule->tvver = TvTransformVersionExpr(pstate, rtc->tvtype, rtc->tvver);
    timeCapsule->tvtype = rtc->tvtype;

    return timeCapsule;
}

/*
该函数的目的是将 FROM 子句中的一个元素进行转换，可能是表引用、子查询、函数调用等，具体解释如下：

参数解释：

ParseState* pstate：解析状态。
Node* n：要转换的解析树节点。
RangeTblEntry** top_rte：返回顶层的关系表条目。
int* top_rti：返回顶层关系表的索引。
RangeTblEntry** right_rte：返回右侧关系表条目（在 JOIN 操作中使用）。
int* right_rti：返回右侧关系表的索引。
List** relnamespace：返回关系表命名空间的列表。
bool isFirstNode：标志，指示是否是 FROM 子句的第一个元素。
bool isCreateView：标志，指示是否在创建视图。
bool isMergeInto：标志，指示是否在执行 MERGE INTO 语句。
bool addUpdateTable：标志，指示是否在 UPDATE 语句中添加目标表。
主要功能：

通过判断节点的类型（IsA 宏），确定节点的种类，并调用相应的函数来完成转换。
支持的节点类型包括 RangeVar（表引用）、RangeSubselect（子查询）、RangeFunction（函数调用）、RangeTableSample（TABLESAMPLE 子句）和 RangeTimeCapsule（TABLECAPSULE 子句）。
对于 JoinExpr 节点（JOIN 操作），递归处理左右子树，并生成一个新的关系表条目，处理 JOIN 条件，并更新命名空间等信息。
这个函数是解析阶段的一部分，负责将解析树中的 FROM 子句元素转换为内部表示的关系表条目等数据结构。
 */
Node* transformFromClauseItem(ParseState* pstate, Node* n, RangeTblEntry** top_rte, int* top_rti,
    RangeTblEntry** right_rte, int* right_rti, List** relnamespace, bool isFirstNode,
    bool isCreateView, bool isMergeInto, bool addUpdateTable)

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

        /* If UPDATE multiple relations in sql_compatibility B, add target table here. */
        if (addUpdateTable) {
            pstate->p_updateRelations = lappend_int(pstate->p_updateRelations,
                setTargetTable(pstate, rv, interpretInhOption(rv->inhOpt), true, ACL_UPDATE, true));
        }
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
            rte->eref->aliasname = ((RangeSubselect*)sw_backup)->alias->aliasname;
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
            j->larg = transformFromClauseItem(pstate, j->larg, &l_rte, &l_rtindex, NULL, NULL, &l_relnamespace,
                                              true, false, false, addUpdateTable);
        } else {
            RangeTblRef* rtr = makeNode(RangeTblRef);
            rtr->rtindex = list_length(pstate->p_rtable);
            j->larg = (Node*)rtr;
            l_rte = (RangeTblEntry*)linitial(pstate->p_target_rangetblentry);
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
        j->rarg = transformFromClauseItem(pstate, j->rarg, &r_rte, &r_rtindex, NULL, NULL, &r_relnamespace,
                                          true, false, false, addUpdateTable);

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
该函数的目的是为 JOIN 操作构建合并后的 JoinVar，主要功能包括：

参数解释：

ParseState* pstate：解析状态。
JoinType jointype：JOIN 操作的类型，如 INNER JOIN、LEFT JOIN 等。
Var* l_colvar：左侧变量。
Var* r_colvar：右侧变量。
主要功能：

根据左右变量的类型和类型修饰符，选择合适的输出类型（outcoltype 和 outcoltypmod）。
如果左右变量的类型不同，进行类型强制转换，保持输出类型一致。
如果左右变量的类型相同但类型修饰符不同，插入 RelabelType 表达式以明确标记输出的类型修饰符与输入不同。
根据 JOIN 类型选择输出的节点：
INNER JOIN：优先选择非强制转换的变量，如果都没有，则选择左变量。
LEFT JOIN 或 LEFT ANTI JOIN：总是选择左变量。
RIGHT JOIN 或 RIGHT ANTI JOIN：总是选择右变量。
FULL JOIN：构建 COALESCE 表达式，确保 JOIN 输出在左右变量中任何一个非空时也非空。
使用 assign_expr_collations 函数修复 coercion 和 CoalesceExpr 节点的排序信息。
该函数的主要任务是确保 JOIN 操作输出的 JoinVar 具有合适的类型和表达式结构，以满足 JOIN 操作的要求。
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
 1. transformWhereClause
c
Copy code
Node* transformWhereClause(ParseState* pstate, Node* clause, ParseExprKind exprKind, const char* constructName)
功能：

对 WHERE 子句进行转换，确保其为布尔类型。
transformExpr 用于转换表达式。
coerce_to_boolean 用于将表达式强制转换为布尔类型。
参数解释：

ParseState* pstate：解析状态。
Node* clause：待转换的 WHERE 子句。
ParseExprKind exprKind：表达式的种类。
const char* constructName：用于错误消息的构造名称。
返回值：转换后的 WHERE 子句。
 */
Node* transformWhereClause(ParseState* pstate, Node* clause, ParseExprKind exprKind, const char* constructName)
{
    Node* qual = NULL;

    if (clause == NULL) {
        return NULL;
    }

    qual = transformExpr(pstate, clause, exprKind);

    qual = coerce_to_boolean(pstate, qual, constructName);

    return qual;
}

/*
 transformLimitClause
c
Copy code
Node* transformLimitClause(ParseState* pstate, Node* clause, ParseExprKind exprKind, const char* constructName)
功能：

对 LIMIT 子句进行转换，确保其为 bigint 类型。
transformExpr 用于转换表达式。
coerce_to_specific_type 用于将表达式强制转换为指定类型（INT8OID，即 bigint）。
检查 LIMIT 子句中是否引用了当前查询的变量或聚合函数，如果有则报错。
参数解释：

ParseState* pstate：解析状态。
Node* clause：待转换的 LIMIT 子句。
ParseExprKind exprKind：表达式的种类。
const char* constructName：用于错误消息的构造名称。
返回值：转换后的 LIMIT 子句。
 */
Node* transformLimitClause(ParseState* pstate, Node* clause, ParseExprKind exprKind, const char* constructName)
{
    Node* qual = NULL;

    if (clause == NULL) {
        return NULL;
    }
    qual = transformExpr(pstate, clause, exprKind);

    qual = coerce_to_specific_type(pstate, qual, INT8OID, constructName);

    /* LIMIT can't refer to any vars or aggregates of the current query */
    checkExprIsVarFree(pstate, qual, constructName);

    return qual;
}

/*
这个函数用于检查表达式是否不包含变量、聚合函数以及窗口函数。如果表达式中包含了这些元素，函数将报错并指出具体的错误位置。

函数签名和参数解释：
c
Copy code
static void checkExprIsVarFree(ParseState* pstate, Node* n, const char* constructName)
功能：

检查表达式是否不包含变量、聚合函数和窗口函数。
如果包含这些元素，则抛出相应的错误。
参数解释：

ParseState* pstate：解析状态。
Node* n：待检查的表达式。
const char* constructName：用于错误消息的构造名称。
返回值：无（void）。
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
 这个函数的目的是在目标列表（targetlist）中查找与给定节点相匹配的目标条目（TargetEntry）。这是为了处理 SQL 查询中的特殊情况，如对列名、整数常量等的引用。

函数签名和参数解释：
c
Copy code
static TargetEntry* findTargetlistEntrySQL92(ParseState* pstate, Node* node, List** tlist, int clause, ParseExprKind exprKind)
功能：

在目标列表中查找与给定节点相匹配的目标条目。
处理 SQL92 中的两个特殊情况：列名和整数常量。
对于列名，遵循 SQL92 规范进行处理，尤其是在 GROUP BY 子句中。
参数解释：

ParseState* pstate：解析状态。
Node* node：待查找的节点。
List** tlist：目标列表。
int clause：查询中的子句类型（例如，GROUP_CLAUSE、ORDER_CLAUSE）。
ParseExprKind exprKind：表达式的类型。
返回值：

如果找到匹配的目标条目，则返回该目标条目。
如果未找到匹配的目标条目，则返回 NULL。
 */
static TargetEntry* findTargetlistEntrySQL92(ParseState* pstate, Node* node, List** tlist, int clause, ParseExprKind exprKind)
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
    return findTargetlistEntrySQL99(pstate, node, tlist, exprKind);
}

/*
 
这个函数用于在目标列表（targetlist）中查找与给定节点（node）相匹配的目标条目（TargetEntry）。如果找到匹配的目标条目，直接返回该目标条目；如果没有找到匹配，创建一个新的目标条目，并将其追加到目标列表末尾，然后返回这个新创建的目标条目。

函数签名和参数解释：
c
Copy code
static TargetEntry* findTargetlistEntrySQL99(ParseState* pstate, Node* node, List** tlist, ParseExprKind exprKind)
功能：

在目标列表中查找与给定节点相匹配的目标条目。
处理 SQL99 规则，允许匹配目标列表中的表达式，不仅限于 SQL92 中的列名或整数常量。
参数解释：

ParseState* pstate：解析状态。
Node* node：待查找的节点。
List** tlist：目标列表。
ParseExprKind exprKind：表达式的类型。
返回值：

如果找到匹配的目标条目，则返回该目标条目。
如果未找到匹配的目标条目，则创建一个新的目标条目，并将其追加到目标列表末尾，然后返回这个新创建的目标条目。
 */
static TargetEntry* findTargetlistEntrySQL99(ParseState* pstate, Node* node, List** tlist, ParseExprKind exprKind)
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
    expr = transformExpr(pstate, node, exprKind);

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
    target_result = transformTargetEntry(pstate, node, expr, exprKind, NULL, true);

    *tlist = lappend(*tlist, target_result);

    return target_result;
}

/* -------------------------------------------------------------------------

这个函数的主要目的是将表达式中的 GroupingSet 进行展开，处理成更容易处理的形式。在 SQL 中，GROUP BY 子句中可以使用 GROUPING SETS 子句指定多个分组，而这个函数用于将这些分组展开。

函数签名和参数解释：
c
Copy code
static Node* flatten_grouping_sets(Node* expr, bool toplevel, bool* hasGroupingSets)
功能：
将表达式中的 GroupingSet 进行展开，处理成更容易处理的形式。
参数解释：
Node* expr：待展开的表达式。
bool toplevel：指示是否在顶层（top-level）进行展开。
bool* hasGroupingSets：用于标识是否存在 GroupingSets。
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
transformGroupClauseExpr 函数：
函数签名和参数解释：
c
Copy code
static Index transformGroupClauseExpr(List** flatresult, Bitmapset* seen_local, ParseState* pstate, Node* gexpr,
    List** targetlist, List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
功能：
转换 GROUP BY 子句中的单个表达式，并处理排序信息。
参数解释：
List** flatresult：输出参数，用于保存 SortGroupClause 结构的平面化结果。
Bitmapset* seen_local：已经处理过的 ressortgroupref 的集合，避免处理重复的表达式。
ParseState* pstate：解析状态。
Node* gexpr：待处理的表达式。
List** targetlist：目标列表。
List* sortClause：排序信息。
ParseExprKind exprKind：表达式类型。
bool useSQL99：是否使用 SQL99 语法。
bool toplevel：是否在最顶层。
 */
static Index transformGroupClauseExpr(List** flatresult, Bitmapset* seen_local, ParseState* pstate, Node* gexpr,
    List** targetlist, List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
{
    TargetEntry* tle = NULL;
    bool found = false;

    if (useSQL99) {
        tle = findTargetlistEntrySQL99(pstate, gexpr, targetlist, exprKind);
    } else {
        tle = findTargetlistEntrySQL92(pstate, gexpr, targetlist, GROUP_CLAUSE, exprKind);
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
transformGroupClauseList 函数：
函数签名和参数解释：
c
Copy code
static List* transformGroupClauseList(List** flatresult, ParseState* pstate, List* list, List** targetlist,
    List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
功能：
转换 GROUP BY 子句或 GROUPING SETS 中的表达式列表。
参数解释：
List** flatresult：输出参数，用于保存 SortGroupClause 结构的平面化结果。
ParseState* pstate：解析状态。
List* list：待处理的表达式列表。
List** targetlist：目标列表。
List* sortClause：排序信息。
ParseExprKind exprKind：表达式类型。
bool useSQL99：是否使用 SQL99 语法。
bool toplevel：是否在最顶层。
 */
static List* transformGroupClauseList(List** flatresult, ParseState* pstate, List* list, List** targetlist,
    List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
{
    Bitmapset* seen_local = NULL;
    List* result = NIL;
    ListCell* gl = NULL;

    foreach (gl, list) {
        Node* gexpr = (Node*)lfirst(gl);

        Index ref =
            transformGroupClauseExpr(flatresult, seen_local, pstate, gexpr, targetlist, sortClause, exprKind, useSQL99, toplevel);
        if (ref > 0) {
            seen_local = bms_add_member(seen_local, ref);
            result = lappend_int(result, ref);
        }
    }

    return result;
}

/*
transformGroupingSet 函数：
函数签名和参数解释：
c
Copy code
static Node* transformGroupingSet(List** flatresult, ParseState* pstate, GroupingSet* gset, List** targetlist,
    List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
功能：
转换 GROUPING SETS 子句。
参数解释：
List** flatresult：输出参数，用于保存 SortGroupClause 结构的平面化结果。
ParseState* pstate：解析状态。
GroupingSet* gset：待处理的 GroupingSet 结构。
List** targetlist：目标列表。
List* sortClause：排序信息。
ParseExprKind exprKind：表达式类型。
bool useSQL99：是否使用 SQL99 语法。
bool toplevel：是否在最顶层。
 */
static Node* transformGroupingSet(List** flatresult, ParseState* pstate, GroupingSet* gset, List** targetlist,
    List* sortClause, ParseExprKind exprKind, bool useSQL99, bool toplevel)
{
    ListCell* gl = NULL;
    List* content = NIL;

    AssertEreport(toplevel || gset->kind != GROUPING_SET_SETS, MOD_OPT, "");

    foreach (gl, gset->content) {
        Node* n = (Node*)lfirst(gl);

        if (IsA(n, List)) {
            List* l = transformGroupClauseList(flatresult, pstate, (List*)n, targetlist, sortClause, exprKind, useSQL99, false);

            content = lappend(content, makeGroupingSet(GROUPING_SET_SIMPLE, l, exprLocation(n)));
        } else if (IsA(n, GroupingSet)) {
            GroupingSet* gset2 = (GroupingSet*)lfirst(gl);

            content = lappend(
                content, transformGroupingSet(flatresult, pstate, gset2, targetlist, sortClause, exprKind, useSQL99, false));
        } else {
            Index ref = transformGroupClauseExpr(flatresult, NULL, pstate, n, targetlist, sortClause, exprKind, useSQL99, false);

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
transformGroupClause 函数：
函数签名和参数解释：
c
Copy code
List* transformGroupClause(
    ParseState* pstate, List* grouplist, List** groupingSets, List** targetlist, List* sortClause, ParseExprKind exprKind, bool useSQL99)
功能：
转换 GROUP BY 子句。
参数解释：
ParseState* pstate：解析状态。
List* grouplist：待处理的 GROUP BY 子句。
List** groupingSets：输出参数，用于保存 GroupingSet 结构的列表。
List** targetlist：目标列表。
List* sortClause：排序信息。
ParseExprKind exprKind：表达式类型。
bool useSQL99：是否使用 SQL99 语法。
 */
List* transformGroupClause(
    ParseState* pstate, List* grouplist, List** groupingSets, List** targetlist, List* sortClause, ParseExprKind exprKind, bool useSQL99)
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
                        gsets, transformGroupingSet(&result, pstate, gset, targetlist, sortClause, exprKind, useSQL99, true));
                    break;
                default:
                    break;
            }
        } else {
            Index ref =
                transformGroupClauseExpr(&result, seen_local, pstate, gexpr, targetlist, sortClause, exprKind, useSQL99, true);
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

这部分代码主要处理窗口函数的定义，包括转换 ORDER BY 和 PARTITION BY 子句。主要包括以下两个函数：

transformSortClause 函数：
函数签名和参数解释：
c
Copy code
List* transformSortClause(ParseState* pstate, List* orderlist, List** targetlist, ParseExprKind exprKind, bool resolveUnknown, bool useSQL99)
功能：
转换 ORDER BY 子句。
参数解释：
ParseState* pstate：解析状态。
List* orderlist：待处理的 ORDER BY 子句。
List** targetlist：目标列表。
ParseExprKind exprKind：表达式类型。
bool resolveUnknown：是否解析未知。
bool useSQL99：是否使用 SQL99 语法。
详细解释：
遍历 orderlist 中的每个 SortBy 元素：

对于每个 SortBy，调用 findTargetlistEntrySQL99 或 findTargetlistEntrySQL92 函数查找目标列表中的对应条目。
调用 addTargetToSortList 函数：

将查找到的目标条目添加到排序列表中。
返回结果：

返回排序列表。
 */
List* transformSortClause(ParseState* pstate, List* orderlist, List** targetlist, ParseExprKind exprKind, bool resolveUnknown, bool useSQL99)
{
    List* sortlist = NIL;
    ListCell* olitem = NULL;

    foreach (olitem, orderlist) {
        SortBy* sortby = (SortBy*)lfirst(olitem);
        TargetEntry* tle = NULL;

        if (useSQL99) {
            tle = findTargetlistEntrySQL99(pstate, sortby->node, targetlist, exprKind);
        } else {
            tle = findTargetlistEntrySQL92(pstate, sortby->node, targetlist, ORDER_CLAUSE, exprKind);
        }
        sortlist = addTargetToSortList(pstate, tle, sortlist, *targetlist, sortby, resolveUnknown);
    }

    return sortlist;
}

/*
transformWindowDefinitions 函数：
函数签名和参数解释：
c
Copy code
List* transformWindowDefinitions(ParseState* pstate, List* windowdefs, List** targetlist)
功能：
转换窗口定义（WindowDef 到 WindowClause）。
参数解释：
ParseState* pstate：解析状态。
List* windowdefs：待处理的窗口定义列表。
List** targetlist：目标列表。
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
            pstate, windef->orderClause, targetlist, EXPR_KIND_WINDOW_ORDER, true /* fix unknowns */, true /* force SQL99 rules */);
        partitionClause = transformGroupClause(
            pstate, windef->partitionClause, NULL, targetlist, orderClause, EXPR_KIND_WINDOW_PARTITION, true /* force SQL99 rules */);

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
transformDistinctClause 函数：
函数签名和参数解释：
c
Copy code
List* transformDistinctClause(ParseState* pstate, List** targetlist, List* sortClause, bool is_agg)
功能：
转换 DISTINCT 子句中的 ORDER BY 部分。
参数解释：
ParseState* pstate：解析状态。
List** targetlist：目标列表。
List* sortClause：排序列表。
bool is_agg：是否为聚合查询。
 */
List* transformDistinctClause(ParseState* pstate, List** targetlist, List* sortClause, bool is_agg)
{
    List* result = NIL;
    ListCell* slitem = NULL;
    ListCell* tlitem = NULL;
    bool allowOrderbyExpr = !IsInitdb && DB_IS_CMPT(B_FORMAT);

    if (pstate->orderbyCols) {
        CheckOrderbyColumns(pstate, *targetlist, is_agg);
    }

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
            if (allowOrderbyExpr) {
                continue;
            } else {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     is_agg ? errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
                            : errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
                     parser_errposition(pstate, exprLocation((Node *)tle->expr))));
            }
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
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid tle value")));
        }
    }

    return result;
}

/*
 CheckOrderbyColumns 函数：
函数签名和参数解释：
c
Copy code
static void CheckOrderbyColumns(ParseState* pstate, List* targetList, bool isAggregate)
功能：
检查 ORDER BY 中的列引用。
参数解释：
ParseState* pstate：解析状态。
List* targetList：目标列表。
bool isAggregate：是否为聚合查询。
 */
static void CheckOrderbyColumns(ParseState* pstate, List* targetList, bool isAggregate)
{
    ListCell* colRefCell = nullptr;

    foreach(colRefCell, pstate->orderbyCols) {
        ColumnRef* colRef = (ColumnRef*)lfirst(colRefCell);
        bool isFound = false;

        if (!isAggregate && list_length(colRef->fields) == 1 &&
            IsA(linitial((colRef)->fields), String)) {
            char* refName = strVal(linitial(colRef->fields));

            ListCell* tcell = nullptr;
            foreach(tcell, targetList) {
                TargetEntry* entry = (TargetEntry*)lfirst(tcell);
                if (!entry->resjunk && entry->resname && strcmp(entry->resname, refName) == 0) {
                    isFound = true;
                    break;
                }
            }
        }

        if (!isFound) {
            Node* refExpr = transformExpr(pstate, (Node*)colRef, EXPR_KIND_ORDER_BY);
            ListCell* tcell = nullptr;
            foreach(tcell, targetList) {
                TargetEntry* entry = (TargetEntry*)lfirst(tcell);
                Node* texpr = strip_implicit_coercions((Node*)entry->expr);
                if (!entry->resjunk && equal(refExpr, texpr)) {
                    isFound = true;
                    break;
                }
            }
        }

        if (!isFound) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     isAggregate
                         ? errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list")
                         : errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
                     parser_errposition(pstate, colRef->location)));
        }
    }

    list_free_ext(pstate->orderbyCols);
}

/*
transformDistinctOnClause 函数的主要目的是处理 SELECT DISTINCT ON 子句。以下是对该函数的详细解释：

函数签名和参数解释：
c
Copy code
List* transformDistinctOnClause(ParseState* pstate, List* distinctlist, List** targetlist, List* sortClause)
功能：
处理 SELECT DISTINCT ON 子句。
参数解释：
ParseState* pstate：解析状态。
List* distinctlist：DISTINCT ON 子句中的表达式列表。
List** targetlist：目标列表。
List* sortClause：ORDER BY 子句中的排序列表。
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

        tle = findTargetlistEntrySQL92(pstate, dexpr, targetlist, DISTINCT_ON_CLAUSE, EXPR_KIND_DISTINCT_ON);
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

以下是对给出的三个函数的详细解释：

1. get_matching_location 函数
c
Copy code
static int get_matching_location(int sortgroupref, List* sortgrouprefs, List* exprs)
功能：
根据 sortgroupref 查找对应的表达式在 exprs 列表中的位置，并返回其位置信息。
参数解释：
int sortgroupref：要查找的排序组引用。
List* sortgrouprefs：排序组引用的列表。
List* exprs：表达式的列表。
详细解释：
使用 forboth 宏遍历 sortgrouprefs 和 exprs 列表。
如果找到与 sortgroupref 匹配的排序组引用，则返回对应表达式的位置信息。
如果没有找到匹配项，报错并返回 -1。
返回值：
匹配项的位置信息。
2. has_not_null_constraint 函数
c
Copy code
bool has_not_null_constraint(ParseState* pstate, TargetEntry* tle)
功能：
检查目标条目对应的表达式是否具有 NOT NULL 约束。
参数解释：
ParseState* pstate：解析状态。
TargetEntry* tle：目标条目。
详细解释：
检查目标表达式是否为 Var 类型。
获取变量的相关信息，包括表的 OID、列号等。
通过系统缓存查找列的元数据，获取列的约束信息。
返回该列是否具有 NOT NULL 约束。
返回值：
如果列具有 NOT NULL 约束，则返回 true，否则返回 false。
3. is_single_table_query 函数
c
Copy code
bool is_single_table_query(ParseState* pstate)
功能：
检查查询是否是单表查询。
参数解释：
ParseState* pstate：解析状态。
详细解释：
检查解析状态中的关系表列表（p_rtable）的长度是否为 1。
如果长度为 1 且表的类型为 RTE_RELATION，则返回 true，否则返回 false。
返回值：
如果是单表查询，返回 true，否则返回 false。
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

这是一个用于将目标条目添加到排序列表的函数。以下是对其主要部分的详细解释：

addTargetToSortList 函数
c
Copy code
List* addTargetToSortList(
    ParseState* pstate, TargetEntry* tle, List* sortlist, List* targetlist, SortBy* sortby, bool resolveUnknown)
功能：
将目标条目添加到排序列表中，返回新的排序列表。
参数解释：
ParseState* pstate：解析状态。
TargetEntry* tle：目标条目。
List* sortlist：当前排序列表。
List* targetlist：目标列表。
SortBy* sortby：排序信息。
bool resolveUnknown：如果目标条目的类型为 UNKNOWN，是否进行隐式类型转换。
详细解释：
如果目标条目的类型为 UNKNOWN 且 resolveUnknown 为 true，则将其隐式转换为 TEXT 类型。
使用错误上下文回调，将错误报告与解析位置相关联。
根据排序方向，获取排序和分组操作符的 Oid，以及是否可哈希。
处理 SORTBY_USING 情况，使用指定的操作符，验证其为有效的排序操作符，并获取对应的等于操作符和方向。
检查是否有重复的排序列表项，如果没有，则创建一个 SortGroupClause 并添加到排序列表中。
在单表查询中，如果排序的列有约束且使用 NULLS FIRST/LAST 选项，将其改为默认选项以获得更好的查询计划。
返回值：
新的排序列表。
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
addTargetToGroupList 函数
c
Copy code
static List* addTargetToGroupList(
    ParseState* pstate, TargetEntry* tle, List* grouplist, List* targetlist, int location, bool resolveUnknown)
功能：
将目标条目添加到分组列表中，返回新的分组列表。
参数解释：
ParseState* pstate：解析状态。
TargetEntry* tle：目标条目。
List* grouplist：当前分组列表。
List* targetlist：目标列表。
int location：目标条目的位置信息。
bool resolveUnknown：如果目标条目的类型为 UNKNOWN，是否进行隐式类型转换。
详细解释：
如果目标条目的类型为 UNKNOWN 且 resolveUnknown 为 true，则将其隐式转换为 TEXT 类型。
检查是否有重复的分组列表项，如果没有，则创建一个 SortGroupClause 并添加到分组列表中。
使用错误上下文回调，将错误报告与解析位置相关联。
根据目标条目的数据类型，获取相应的等于操作符和排序操作符。
创建 SortGroupClause 并添加到分组列表中。
返回值：
新的分组列表。
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
 assignSortGroupRef 函数
c
Copy code
Index assignSortGroupRef(TargetEntry* tle, List* tlist)
功能：
为目标条目分配或获取一个未使用的 ressortgroupref（排序分组参考编号）。
参数解释：
TargetEntry* tle：目标条目。
List* tlist：目标列表。
详细解释：
如果目标条目已经有 ressortgroupref，则直接返回该值。
否则，遍历目标列表中的每个条目，找到已使用的最大 ressortgroupref 值，并为目标条目分配一个未使用的值（即最大值加一）。
返回值：
分配或获取的 ressortgroupref。
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

targetIsInSortList 函数
c
Copy code
bool targetIsInSortList(TargetEntry* tle, Oid sortop, List* sortList)
功能：
检查目标条目是否在排序列表中。
参数解释：
TargetEntry* tle：目标条目。
Oid sortop：排序操作符的OID。
List* sortList：排序列表。
详细解释：
获取目标条目的 ressortgroupref（排序分组参考编号）。
如果 ressortgroupref 为 0，表示目标条目没有排序标记，直接返回 false。
遍历排序列表中的每个 SortGroupClause，检查其 tleSortGroupRef 是否等于目标条目的 ressortgroupref，且排序操作符匹配。
如果找到匹配项，返回 true，否则返回 false。
返回值：
如果目标条目在排序列表中，则返回 true，否则返回 false。
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

targetIsInSortList 函数
c
Copy code
bool targetIsInSortList(TargetEntry* tle, Oid sortop, List* sortList)
功能：
检查目标条目是否在排序列表中。
参数解释：
TargetEntry* tle：目标条目。
Oid sortop：排序操作符的OID。
List* sortList：排序列表。
详细解释：
获取目标条目的 ressortgroupref（排序分组参考编号）。
如果 ressortgroupref 为 0，表示目标条目没有排序标记，直接返回 false。
遍历排序列表中的每个 SortGroupClause，检查其 tleSortGroupRef 是否等于目标条目的 ressortgroupref，且排序操作符匹配。
如果找到匹配项，返回 true，否则返回 false。
返回值：
如果目标条目在排序列表中，则返回 true，否则返回 false。
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
transformFrameOffset 函数
c
Copy code
static Node* transformFrameOffset(ParseState* pstate, int frameOptions, Node* clause)
功能：
转换窗口帧的偏移表达式。
参数解释：
ParseState* pstate：解析状态。
int frameOptions：窗口帧的选项，包括 FRAMEOPTION_ROWS 和 FRAMEOPTION_RANGE。
Node* clause：窗口帧的偏移表达式。
详细解释：
如果偏移表达式为 NULL，直接返回 NULL，表示没有偏移表达式。
如果窗口帧选项包括 FRAMEOPTION_ROWS，则对表达式进行变换，然后将其强制转换为 INT8OID 类型。
使用 transformExpr 对原始表达式树进行转换。
将表达式强制转换为 INT8OID 类型，类似于 LIMIT 子句。
如果窗口帧选项包括 FRAMEOPTION_RANGE，则对表达式进行变换，然后抛出错误，因为 FRAMEOPTION_RANGE 中的值偏移在 PostgreSQL 中尚未实现。
使用 checkExprIsVarFree 检查偏移表达式中是否包含变量或聚合函数，如果有，则抛出错误。
返回值：
返回转换后的偏移表达式。如果偏移表达式为 NULL，则返回 NULL。
 */
static Node* transformFrameOffset(ParseState* pstate, int frameOptions, Node* clause)
{
    const char* constructName = NULL;
    Node* node = NULL;

    /* Quick exit if no offset expression */
    if (clause == NULL) {
        return NULL;
    }    

    if (frameOptions & FRAMEOPTION_ROWS) {
        /* Transform the raw expression tree */
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_ROWS);
        /*
         * Like LIMIT clause, simply coerce to int8
         */
        constructName = "ROWS";
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
    } else if (frameOptions & FRAMEOPTION_RANGE) {
        /* Transform the raw expression tree */
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_RANGE);
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
