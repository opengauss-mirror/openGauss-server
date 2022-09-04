/* -------------------------------------------------------------------------
 *
 * plananalyzer.cpp
 *	   Plan Analyzer for SQL self diagnosis & tuning
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/utils/plananalyzer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/prepare.h"
#include "executor/exec/execStream.h"
#include "parser/parse_relation.h"
#ifdef PGXC
#include "optimizer/planmain.h"
#endif
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* -- Criteria/Threshold definition of plan issue -- */
/* Basic factor that identifies the number of Per-DN tuples for "Large Table" */
#define LargeTableFactor 100000

/* Const variables for Large Table in Broadcast */
#define LargerTable_Broadcast_Threshold LargeTableFactor

/* Const variables for Large table as hashjoin's Inner */
#define LTAsHashJoinInner_Rows_Threshold LargeTableFactor
#define LTAsHashJoinInner_Scale_Threshold 10

/* Const variables for large Table in nestloop with equal condition */
#define LTWithEqualCondInNestLoop_Rows_Threshold LargeTableFactor

/* Const variables for Data Skew */
#define DataSkew_Rows_Threshold 100000
#define DataSkew_Scale_Threshold 10

/* Const variables for E-Rows not accurate */
#define EstimationRows_Threshold 100000
#define EstimationRows_Scale_Threshold 10

/* Const variables for Scan Method */
#define SCANMETHOD_ROWS_THRESHOLD 10000
#define SCANMETHOD_RATE_THRESHOLD 0.001
#define SCANMETHOD_VECROWS_THRESHOLD 100
#define SCANMETHOD_VECRATE_THRESHOLD 0.0001
#define SCANMETHOD_INPUT_THRESHOLD 10000

static char* OperatorName(const Plan* plan);

extern double get_float8_infinity(void);

extern bool check_relation_analyzed(Oid relid);

#define MAX_OPTIMIZER_WARNING_LEN 2048

static inline QueryPlanIssueDesc* CheckQueryNotPlanShipping();

static bool DuplicateWithUnderlyingPlanNodes(PlanState* node, QueryIssueType type);

static List* getPlanSubNodes(const PlanState* node);

static inline QueryPlanIssueDesc* CheckLargeTableInBroadcast(PlanState* node, int dn_num, double total_tuples);

static inline QueryPlanIssueDesc* CheckLargeTableInHashJoinInner(
    PlanState* node, int dn_num, double rightchild_total_rows, double leftchild_total_rows);

static inline QueryPlanIssueDesc* CheckLargeTableInNestloopWithEqualCondition(
    PlanState* node, int dn_num, double Maxchild_total_size);

static inline QueryPlanIssueDesc* CheckDataSkew(PlanState* node, double min_dn_tuples, double max_dn_tuples);

static inline QueryPlanIssueDesc* CheckMultiColumnStatsNotCollect(QueryDesc* querydesc);

static inline QueryPlanIssueDesc* CheckInaccurateEstimatedRows(PlanState* node, int dn_num, double actual_rows);

static double ComputeSumOfDNTuples(Plan* node);

static QueryPlanIssueDesc* CreateQueryPlanIssue(PlanState* node, QueryIssueType type);

static void DeleteQueryPlanIssue(QueryPlanIssueDesc* query_issue);

static bool ContainsDuplicateIssues(PlanState* node, QueryIssueType type);

static bool IsEqualConditionandNestLoopOnly(Plan* plan);
/* ----------------------------- Local routine definitions ----------------------------- */
/*
 * - Brief: Return true if for current join-node NestLoop/VecNestLoop is only option
 */
static bool IsEqualConditionandNestLoopOnly(Plan* plan)
{
    Assert(nodeTag(plan) == T_NestLoop || nodeTag(plan) == T_VecNestLoop);

    ListCell* lc = NULL;
    NestLoop* nestloop = (NestLoop*)plan;

    foreach (lc, nestloop->join.joinqual) {
        if (isEqualExpr((Node*)lfirst(lc))) {
            return true;
        }
    }

    return false;
}

/*
 * - Brief: Return plan node's underlying plan nodes that is not create under
 *   left/right plan tree
 */
static List* getPlanSubNodes(const PlanState* node)
{
    List* ps_list = NIL;

    if (node == NULL) {
        return NIL;
    }

    /* Find plan list in special plan nodes. */
    switch (nodeTag(node->plan)) {
        case T_Append:
        case T_VecAppend: {
            AppendState* append = (AppendState*)node;
            for (int i = 0; i < append->as_nplans; i++) {
                PlanState* plan = append->appendplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTableState* mt = (ModifyTableState*)node;
            for (int i = 0; i < mt->mt_nplans; i++) {
                PlanState* plan = mt->mt_plans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppendState* ma = (MergeAppendState*)node;
            for (int i = 0; i < ma->ms_nplans; i++) {
                PlanState* plan = ma->mergeplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAndState* ba = (BitmapAndState*)node;
            for (int i = 0; i < ba->nplans; i++) {
                PlanState* plan = ba->bitmapplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOrState* bo = (BitmapOrState*)node;
            for (int i = 0; i < bo->nplans; i++) {
                PlanState* plan = bo->bitmapplans[i];
                ps_list = lappend(ps_list, plan);
            }
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScanState* ss = (SubqueryScanState*)node;
            PlanState* plan = ss->subplan;
            ps_list = lappend(ps_list, plan);
        } break;
        default: {
            ps_list = NIL;
        } break;
    }

    return ps_list;
}

/*
 * - Brief: Return true if target plan contains given type of issues that its underlying
 *   plan node already have
 */
static bool DuplicateWithUnderlyingPlanNodes(PlanState* node, QueryIssueType type)
{
    bool is_duplicate = false;

    /* First check node's left tree node is already have same issue */
    if (node->lefttree != NULL && ContainsDuplicateIssues(node->lefttree, type)) {
        return true;
    }

    /* Second check node's right tree node is already have same issue */
    if (node->righttree != NULL && ContainsDuplicateIssues(node->righttree, type)) {
        return true;
    }

    /* Check if node's other underlying nodes is already have same issue e.g. setop */
    List* node_list = getPlanSubNodes(node);
    ListCell* lc = NULL;

    if (node_list != NIL) {
        foreach (lc, node_list) {
            PlanState* ps = (PlanState*)lfirst(lc);
            if (ContainsDuplicateIssues(ps, type)) {
                is_duplicate = true;
                break;
            }
        }

        list_free(node_list);
    }

    return is_duplicate;
}

/*
 * - Brief: Return true if target plan contains given type of issues a.w.k. considered
 *   as duplicated issue nodes
 */
static bool ContainsDuplicateIssues(PlanState* node, QueryIssueType type)
{
    ListCell* lc = NULL;

    if (node->plan_issues == NIL) {
        return false;
    }

    foreach (lc, node->plan_issues) {
        QueryPlanIssueDesc* issue = (QueryPlanIssueDesc*)lfirst(lc);

        if (issue->issue_type == type) {
            return true;
        }
    }

    return false;
}

/*
 * - Brief: compute total number of processed tuples of current plan node
 */
static double ComputeSumOfDNTuples(Plan* node)
{
    int dn_index = 0;
    int dop = node->dop;
    double total_tuples = 0.0;
    ListCell* nodeitem = NULL;
    List* exec_nodeList = NIL;
    ExecNodes* exec_nodes = NULL;

    exec_nodes = ng_get_dest_execnodes(node);
    exec_nodeList = exec_nodes->nodeList;

    foreach (nodeitem, exec_nodeList) {
        dn_index = lfirst_int(nodeitem);

        for (int j = 0; j < dop; j++) {
            /* avoid for threadinstr is NULL */
            Instrumentation* instr = u_sess->instr_cxt.global_instr->getInstrSlot(dn_index, node->plan_node_id, j);
            if (instr != NULL)
                total_tuples += instr->ntuples;
        }
    }

    return total_tuples;
}

/*
 * - Brief: Function to help create plan issues
 */
static QueryPlanIssueDesc* CreateQueryPlanIssue(PlanState* node, QueryIssueType type)
{
    QueryPlanIssueDesc* plan_issue = (QueryPlanIssueDesc*)palloc0(sizeof(QueryPlanIssueDesc));

    plan_issue->issue_type = type;
    plan_issue->issue_plannode = node;
    plan_issue->issue_suggestion = makeStringInfo();

    return plan_issue;
}

/*
 * - Brief: Function to help delete plan issues
 */
static void DeleteQueryPlanIssue(QueryPlanIssueDesc* query_issue)
{
    if (query_issue == NULL) {
        return;
    }

    if (query_issue->issue_suggestion) {
        if (query_issue->issue_suggestion->data != NULL) {
            pfree_ext(query_issue->issue_suggestion->data);
        }

        pfree_ext(query_issue->issue_suggestion);
    }

    pfree_ext(query_issue);
}

/*
 * - Brief: Determine whether current Plan node has "not plan-shipping" issue
 * - Parameter:
 *      @node: to be checked plan node
 * - Return:
 *      @not-null: the plan node has "not plan-shipping" issue
 *      @null: the plan node does not has "not plan-shipping" issue
 */
static inline QueryPlanIssueDesc* CheckQueryNotPlanShipping(void)
{
    QueryPlanIssueDesc* plan_issue = NULL;

    /* return null immediately when the plan can be shipped */
    if (0 == strlen(u_sess->opt_cxt.not_shipping_info->not_shipping_reason)) {
        return NULL;
    }
    /* Start standard plan node-level check for query not plan-shipping */
    plan_issue = CreateQueryPlanIssue(NULL, QueryShipping);

    appendStringInfo(plan_issue->issue_suggestion,
        "SQL is not plan-shipping, reason : \"%s\"",
        u_sess->opt_cxt.not_shipping_info->not_shipping_reason);

    errno_t errorno = memset_s(
        u_sess->opt_cxt.not_shipping_info->not_shipping_reason, NOTPLANSHIPPING_LENGTH, '\0', NOTPLANSHIPPING_LENGTH);
    securec_check_c(errorno, "\0", "\0");

    return plan_issue;
}

/*
 * - Brief: Determine whether current Plan node has "large table broadcast" issue
 * - Parameter:
 *      @node: to be checked plan node
 * - Return:
 *      @not-null: the plan node has "large table broadcast" issue
 *      @null: the plan node does not has "large table broadcast" issue
 */
static inline QueryPlanIssueDesc* CheckLargeTableInBroadcast(PlanState* node, int dn_num, double total_tuples)
{
    Assert(node != NULL);
    Stream* sn = NULL;
    QueryPlanIssueDesc* plan_issue = NULL;

    sn = (Stream*)node->plan;

    if (sn->type == STREAM_BROADCAST && total_tuples >= (double)(dn_num * LargerTable_Broadcast_Threshold)) {
        plan_issue = CreateQueryPlanIssue(node, LargeTableBroadCast);

        appendStringInfo(plan_issue->issue_suggestion,
            "PlanNode[%d] Large Table in Broadcast \"%s\"",
            node->plan->plan_node_id,
            OperatorName(node->plan));

        node->plan_issues = lappend(node->plan_issues, plan_issue);
    }

    return plan_issue;
}

/*
 * - Brief: Determine whether current Plan node has "large table as hashjoin inner" issue
 * - Parameter:
 *      @node: to be checked plan node
 * - Return:
 *      @not-null: the plan node has "large table as hashjoin inner" issue
 *      @null: the plan node does not has "large table as hashjoin inner" issue
 */
static inline QueryPlanIssueDesc* CheckLargeTableInHashJoinInner(
    PlanState* node, int dn_num, double rightchild_total_rows, double leftchild_total_rows)
{
    Assert(node != NULL);
    QueryPlanIssueDesc* plan_issue = NULL;

    if (rightchild_total_rows >= (double)(dn_num * LTAsHashJoinInner_Rows_Threshold) &&
        ((leftchild_total_rows == 0.0 ||
            (rightchild_total_rows / leftchild_total_rows >= LTAsHashJoinInner_Scale_Threshold)))) {
        plan_issue = CreateQueryPlanIssue(node, LargeTableAsHashJoinInner);

        appendStringInfo(plan_issue->issue_suggestion,
            "PlanNode[%d] Large Table is INNER in HashJoin \"%s\"",
            node->plan->plan_node_id,
            OperatorName(node->plan));

        node->plan_issues = lappend(node->plan_issues, plan_issue);
    }

    return plan_issue;
}

/*
 * - Brief: Determine whether current Plan node has "large table in nestloop with equal
 *          join condition" issue
 * - Parameter:
 *      @node: to be checked plan node
 * - Return:
 *      @not-null: the plan node has "large table in nestloop with equal join cond" issue
 *      @null: the plan node does not has "large table in nestloop with equal join cond" issue
 */
static inline QueryPlanIssueDesc* CheckLargeTableInNestloopWithEqualCondition(
    PlanState* node, int dn_num, double Maxchild_total_size)
{
    Assert(node != NULL);
    QueryPlanIssueDesc* plan_issue = NULL;

    if (Maxchild_total_size >= (double)(dn_num * LTWithEqualCondInNestLoop_Rows_Threshold)) {
        plan_issue = CreateQueryPlanIssue(node, LargeTableWithEqualCondInNestLoop);

        appendStringInfo(plan_issue->issue_suggestion,
            "PlanNode[%d] Large Table with Equal-Condition use Nestloop\"%s\"",
            node->plan->plan_node_id,
            OperatorName(node->plan));

        node->plan_issues = lappend(node->plan_issues, plan_issue);
    }

    return plan_issue;
}

/*
 * - Brief: Determine whether current query has "multi column statistics not collect" issues
 * - Parameter:
 *      @querydesc: query desc structure for to current SQL statements
 * - Return:
 *      @not-null: the plan node has "multi column statistics not collect" issue
 *      @null: the plan node does not has "multi column statistics not collect" issue
 */
static inline QueryPlanIssueDesc* CheckMultiColumnStatsNotCollect(QueryDesc* querydesc)
{
    List* noanalyze_rellist = querydesc->plannedstmt->noanalyze_rellist;

    if (noanalyze_rellist == NIL) {
        return NULL;
    }

    int relnum = 0;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    StringInfo single_col_total = makeStringInfo();
    StringInfo multi_col_msg = makeStringInfo();
    StringInfo multi_col_total = makeStringInfo();

    relnum = list_length(noanalyze_rellist);

    QueryPlanIssueDesc* plan_issue = (QueryPlanIssueDesc*)CreateQueryPlanIssue(NULL, StatsNotCollect);
    appendStringInfo(plan_issue->issue_suggestion, "Statistic Not Collect:\n");

    /*
     * The content for stats not-analyzed table in g_NoAnalyzeRelNameList:
     *  - element[1]: tableoid, attnum, attnum, attnum
     *  - element[2]: tableoid, attnum, attnum, attnum
     *  - ...
     *  - element[3]: tableoid, attnum, attnum, attnum
     */
    foreach (lc1, noanalyze_rellist) {
        List* record = (List*)lfirst(lc1);

        /* Fetch first element as table oid */
        Oid relid = linitial_oid((List*)linitial(record));

        Relation rel = relation_open(relid, AccessShareLock);
        int attrnum = list_length(record) - 1;
        int token = 1;

        /* Skip rel which there is no reltuples and has been analyzed */
        if (0 == rel->rd_rel->reltuples && check_relation_analyzed(relid)) {
            relation_close(rel, AccessShareLock);
            continue;
        }

        /* The 1st cell is the list of rel , so skip it,  and get the att id */
        lc2 = lnext(list_head(record));

        resetStringInfo(single_col_total);
        resetStringInfo(multi_col_total);
        while (lc2 != NULL) {
            resetStringInfo(multi_col_msg);
            List* tmp_record_list = (List*)lfirst(lc2);

            /* single-col that has no statistics */
            if (list_length(tmp_record_list) == 1) {
                int attid = linitial_int(tmp_record_list);
                if (attid == 0) {
                    /* attid ==0 means the whole table has no statistics, so sikp the colunm info */
                    appendStringInfo(plan_issue->issue_suggestion,
                        "    %s.%s\n",
                        quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
                        quote_identifier(get_rel_name(relid)));
                    break;
                } else {
                    if (single_col_total->len) {
                        appendStringInfo(single_col_total, ",");
                    }
                    appendStringInfo(single_col_total, "%s", quote_identifier((char*)attnumAttName(rel, attid)));
                }
            } else { /* multi-col that has no statistics */
                ListCell* lc3 = NULL;
                appendStringInfo(multi_col_msg,
                    "    %s.%s((",
                    quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
                    quote_identifier(get_rel_name(relid)));
                foreach (lc3, tmp_record_list) {
                    int multi_col_attid = lfirst_int(lc3);
                    appendStringInfo(multi_col_msg, "%s", quote_identifier((char*)attnumAttName(rel, multi_col_attid)));
                    if (lnext(lc3)) {
                        appendStringInfo(multi_col_msg, ",");
                    } else {
                        appendStringInfo(multi_col_msg, "))");
                    }
                }
                appendStringInfo(multi_col_total, "%s", multi_col_msg->data);
            }

            if (token == attrnum) {
                if (single_col_total->len) {
                    appendStringInfo(plan_issue->issue_suggestion,
                        "    %s.%s(",
                        quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
                        quote_identifier(get_rel_name(relid)));

                    appendStringInfo(plan_issue->issue_suggestion, "%s)", single_col_total->data);
                }

                if (multi_col_total->len) {
                    appendStringInfo(plan_issue->issue_suggestion, "%s", multi_col_total->data);
                }

                if (!single_col_total->len && !multi_col_total->len) {
                    appendStringInfo(plan_issue->issue_suggestion,
                        "    %s.%s",
                        quote_identifier(get_namespace_name(RelationGetNamespace(rel))),
                        quote_identifier(get_rel_name(relid)));
                }
                appendStringInfo(plan_issue->issue_suggestion, "\n");
            }

            token++;
            lc2 = lnext(lc2);
        }

        relation_close(rel, AccessShareLock);
    }

    return plan_issue;
}

/*
 * - Brief: Check inaccurate A-Rows/E-Rows estimation issue on target plan node
 * - Parameter:
 *      @node: plan node to check inaccurate e-rows
 *      @dn_num: number of datanodes where the node runs
 *      @actual_rows: total actual number of rows
 *
 * - Note: Since E-Rows not accurate issue may be rooted from upper layer, we have
 *   to do issue-deduplicate before return it to end user, it says if DataSkew is caused
 *   by upper steps instead of current one, we are igoring it.
 */
static inline QueryPlanIssueDesc* CheckInaccurateEstimatedRows(PlanState* node, int dn_num, double actual_rows)
{
    Assert(node != NULL);
    QueryPlanIssueDesc* plan_issue = NULL;
    double estimated_rows = node->plan->plan_rows;

    /*
     * Determine if E-Rows over-estimated OR under-estimated beyond pre-defined
     * threshold(10 times).
     */
    if (Max(estimated_rows, actual_rows) >= dn_num * EstimationRows_Threshold &&
        (estimated_rows / actual_rows >= EstimationRows_Scale_Threshold ||
            actual_rows / estimated_rows >= EstimationRows_Scale_Threshold)) {
        plan_issue = CreateQueryPlanIssue(node, InaccurateEstimationRowNum);

        appendStringInfo(plan_issue->issue_suggestion,
            "PlanNode[%d] Inaccurate Estimation-Rows: \"%s\" A-Rows:%.0f, E-Rows:%.0f",
            node->plan->plan_node_id,
            OperatorName(node->plan),
            actual_rows,
            estimated_rows);

        /*
         * Store it in current plan node's issue list anyway to help issue-deduplicating
         * process
         */
        node->plan_issues = lappend(node->plan_issues, plan_issue);
    }

    /*
     * Check if upper steps is already inaccurate to avoid reporting duplicated issue,
     * here return NULL as not reporting issue.
     */
    if (DuplicateWithUnderlyingPlanNodes(node, InaccurateEstimationRowNum)) {
        return NULL;
    }

    return plan_issue;
}

/*
 * - Brief: Check if DataSkew issues happned on target plan node
 * - Parameter:
 *      @node: plan node to check data skew issues
 *      @min_dn_tuples: min processed-tuples datanodes
 *      @max_dn_tuples: max processed-tuples datanodes
 *
 * - Note: Since DatgaSkew issue may be rooted from upper layer, we have to do
 *   issue-deduplicate before return it to end user, it says if DataSkew is caused
 *   by upper steps instead of current one, we are igoring it.
 */
static inline QueryPlanIssueDesc* CheckDataSkew(PlanState* node, double min_dn_tuples, double max_dn_tuples)
{
    Assert(node != NULL);
    QueryPlanIssueDesc* plan_issue = NULL;

    /*
     * Check if in target plan node the num of processed tuples is differed over
     * pre-defined threshold (10 times)
     */
    if ((min_dn_tuples == 0 || max_dn_tuples / min_dn_tuples >= DataSkew_Scale_Threshold) &&
        max_dn_tuples >= DataSkew_Rows_Threshold) {
        plan_issue = CreateQueryPlanIssue(node, DataSkew);

        appendStringInfo(plan_issue->issue_suggestion,
            "PlanNode[%d] DataSkew:\"%s\", min_dn_tuples:%.0f, max_dn_tuples:%.0f",
            node->plan->plan_node_id,
            OperatorName(node->plan),
            min_dn_tuples,
            max_dn_tuples);

        /*
         * Store it in current plan node's issue list anyway to help issue-deduplicating
         * process
         */
        node->plan_issues = lappend(node->plan_issues, plan_issue);
    }

    /*
     * Check if upper steps is already skewed to avoid dup issue report, here return
     * NULL as not reporting issue.
     */
    if (DuplicateWithUnderlyingPlanNodes(node, DataSkew)) {
        return NULL;
    }

    return plan_issue;
}

/*
 * - Brief: Check if unsuitable scan method issues happened on target plan node
 * - Parameter:
 *      @node: plan node to check scan method issues
 *      @dnNum: number of datanodes where the node runs
 *      @totalTuples: total produced tuples
 *      @totalFiltereds: total removed tuples
 *      @isIndex: is index scan or not
 *      @isCstore: is cstore or not
 * - Return:
 *      @not-null: the plan node has "Scan method is not suitable" issue
 *      @null: the plan node does not has "Scan method is not suitable" issue
 */
static inline QueryPlanIssueDesc* CheckUnsuitableScanMethod(
    PlanState* node, int dnNum, double totalTuples, double totalFiltereds, bool isIndex, bool isCstore)
{
    Assert(node != NULL);
    QueryPlanIssueDesc* issue = NULL;
    Assert(dnNum > 0);
    double output = totalTuples / dnNum;
    double input = output + totalFiltereds / dnNum;
    double rate = output / input;

    /*
     * For indexscan, if there are too many tuples produced, we should use seq scan.
     * For seqscan, if threr are few tuples produced from many ones, we should use index scan.
     */
    if ((isIndex && isCstore && output > SCANMETHOD_VECROWS_THRESHOLD && rate > SCANMETHOD_VECRATE_THRESHOLD) ||
        (isIndex && !isCstore && output > SCANMETHOD_ROWS_THRESHOLD && rate > SCANMETHOD_RATE_THRESHOLD) ||
        (!isIndex && isCstore && input > SCANMETHOD_INPUT_THRESHOLD && output <= SCANMETHOD_VECROWS_THRESHOLD &&
            rate < SCANMETHOD_VECRATE_THRESHOLD) ||
        (!isIndex && !isCstore && input > SCANMETHOD_INPUT_THRESHOLD && output <= SCANMETHOD_ROWS_THRESHOLD &&
            rate < SCANMETHOD_RATE_THRESHOLD)) {
        issue = CreateQueryPlanIssue(node, UnsuitableScanMethod);

        appendStringInfo(issue->issue_suggestion,
            "PlanNode[%d] Indexscan is %s used:\"%s\", output:%.0f, filtered:%.0f, rate:%.5f",
            node->plan->plan_node_id,
            isIndex ? "not properly" : "ought to be",
            OperatorName(node->plan),
            totalTuples,
            totalFiltereds,
            rate);

        node->plan_issues = lappend(node->plan_issues, issue);
    }

    return issue;
}

/* ----------------------------- External routine definitions -------------------------- */
/*
 * - Brief: Main entering pointer of SQL Self-Tuning for Query-Level issue
 *   diagnosis, invoked when query plan is ready but execution not start
 *
 * - Parameter:
 *      @querydesc: query desc to hold plannedstmt objects
 *
 * - Return:
 *      List found found plan issues (query level)
 */
List* PlanAnalyzerQuery(QueryDesc* querydesc)
{
    List* issueResults = NIL;
    QueryPlanIssueDesc* issueResultsItem = NULL;

    /* Try to analyze issues of Not Plan-Shipping */
    if ((issueResultsItem = CheckQueryNotPlanShipping()) != NULL) {
        issueResults = lappend(issueResults, issueResultsItem);
    }

    /* Try to analyze issues of Single/Multi-Column statistic not collected */
    if ((issueResultsItem = CheckMultiColumnStatsNotCollect(querydesc)) != NULL) {
        issueResults = lappend(issueResults, issueResultsItem);
    }

    return issueResults;
}

/*
 * - Brief:
 * 		Main function entrance of SQL Self-Tuning for Operator-Level issue analysis,
 *   it is invoded when execution is finished and runtime information is captured in
 *   globalInstrumentation framework.
 *
 *   The process works in a bottom up recursive way, first reach the leaf node, we
 *   have to do so as we are wanting to make iteration the whole plantree one time
 *   while issue-depuing is required. For issue-deduping, it says when under node is
 *   already "Data Skew" or "E-Rows Inaccurate", we only report the root of issue.
 *
 * - Parameter:
 *      @querydesc: query desc to hold plannedstmt objects
 *      @plan: current(top) plan node being analyzed
 *
 * - Return:
 *      List of found plan issues (operator level)
 */
List* PlanAnalyzerOperator(QueryDesc* querydesc, PlanState* planstate)
{
    PlanState* ps = planstate;
    Plan* plan = NULL;
    List* issueResults = NIL;
    QueryPlanIssueDesc* issueResultsItem = NULL;
    bool skip_inaccurate_erows = false;

    /* Return NIL when we get the end of plantree */
    if (ps == NULL) {
        return NIL;
    }

    /* Return NIL when instrumentation framework is not setup yet */
    if (u_sess->instr_cxt.global_instr == NULL) {
        return NIL;
    }

    plan = ps->plan;

    /*
     * Skip some scenarios where A-Rows/E-Rows does not reflect the actual return row numbers
     * - [1]. Subplan contains Limit/VecLimit operator, we are stopping to analyze current operator
     *        and its under operators
     * - [2]. Material/VecMaterial indicates plan node re-scan, the A-rows does not reflect actual
     *        return rows. Note: An inner plantree of MergeJoin/NestLoop is also a re-scan case, in
     *        current release we only report material node where cover most of rescan scenarios, we
     *        will improve the overall re-scan case by enhanceing explain-perf frameword to tell us
     *        scaned rows & return rows
     */
    if (nodeTag(plan) == T_Limit || nodeTag(plan) == T_VecLimit) {
        elog(DEBUG1,
            "Skip analyze DataSkew, Inaccurate E-Rows for plan nodes under PlanNode[%d]:%s",
            plan->plan_node_id,
            OperatorName(plan));

        return NIL;
    } else if (nodeTag(plan) == T_Material || nodeTag(plan) == T_VecMaterial) {
        /*
         * For Material plan node, we are going to skip analyze e-rows inaccurate issues,
         * as it might be re-scaned and a-rows does not reflect actual num of rows that
         * estimated during query-planning
         */
        elog(DEBUG1,
            "Skip analyze Inaccurate E-Rows due to Re-Scan for plan nodes under PlanNode[%d]:%s",
            plan->plan_node_id,
            OperatorName(plan));
        skip_inaccurate_erows = true;
    }

    /* Output debug information */
    elog(DEBUG1, "QueryAnalayzer check PlanNode[%d]:%s", plan->plan_node_id, OperatorName(plan));

    /* Expaned subplans plan nodes, Init SubPlanState nodes (un-correlated expr subselects) */
    if (ps->initPlan) {
        ListCell* lc = NULL;
        foreach (lc, ps->initPlan) {
            SubPlanState* sps = (SubPlanState*)lfirst(lc);
            issueResults = list_concat(issueResults, PlanAnalyzerOperator(querydesc, (PlanState*)sps->planstate));
        }
    }

    /* Recursively analyze left plan tree */
    if (ps->lefttree) {
        issueResults = list_concat(issueResults, PlanAnalyzerOperator(querydesc, ps->lefttree));
    }

    /* Recursively analyze right plan tree */
    if (ps->righttree) {
        issueResults = list_concat(issueResults, PlanAnalyzerOperator(querydesc, ps->righttree));
    }

    /* Find plan list in special plan nodes. */
    List* ps_list = getPlanSubNodes(ps);

    /* subPlan-s for SubPlanState nodes in my expressions (correlated query) */
    if (ps->subPlan) {
        ListCell* lc = NULL;
        foreach (lc, ps->subPlan) {
            SubPlanState* sps = (SubPlanState*)lfirst(lc);
            issueResults = list_concat(issueResults, PlanAnalyzerOperator(querydesc, (PlanState*)sps->planstate));
        }
    }

    if (ps_list != NIL) {
        ListCell* lc = NULL;

        /* Go ahead to analyze underlying plan nodes as we don get leaf plan node yet */
        foreach (lc, ps_list) {
            PlanState* plan_state = (PlanState*)lfirst(lc);
            issueResults = list_concat(issueResults, PlanAnalyzerOperator(querydesc, plan_state));
        }

        list_free(ps_list);
    }

    {
        /* Ignore issue when the plan exec on coords */
        if (plan->exec_type == EXEC_ON_COORDS || plan->exec_type == EXEC_ON_NONE) {
            return issueResults;
        }
        /* if plan->exec_type == EXEC_ON_ALL_NODES and m_planIdOffsetArray[plan->plan_node_id - 1] == 0
         * means plan exec on CN, it should ignore.
         */
        int* m_planIdOffsetArray = u_sess->instr_cxt.global_instr->get_planIdOffsetArray();
        if (plan->exec_type == EXEC_ON_ALL_NODES && m_planIdOffsetArray[plan->plan_node_id - 1] == 0) {
            return issueResults;
        }

        /* Start to analyze current plan nodes */
        int dn_num = 0;
        int dn_index = 0;
        double dn_tuples = 0.0;
        double dnFiltereds = 0.0;
        double min_dn_tuples = get_float8_infinity();
        double max_dn_tuples = 0.0;
        double total_tuples = 0.0;
        double totalFiltereds = 0.0;
        bool write_file = false;
        int dop = plan->dop;
        ListCell* nodeitem = NULL;
        List* exec_nodeList = NIL;
        ExecNodes* exec_nodes = NULL;

        /* Extract info from u_sess->instr_cxt.global_instr and plan for sql tuning */
        exec_nodes = ng_get_dest_execnodes(plan);
        exec_nodeList = exec_nodes->nodeList;

        dn_num = list_length(exec_nodeList);

        foreach (nodeitem, exec_nodeList) {
            dn_index = lfirst_int(nodeitem);
            dn_tuples = 0.0;
            dnFiltereds = 0.0;
            for (int j = 0; j < dop; j++) {
                Instrumentation* node_instr =
                    u_sess->instr_cxt.global_instr->getInstrSlot(dn_index, plan->plan_node_id, j);
                /* In special, if node_instr is NULL means plan is not executed ,it should return and exit. */
                if (node_instr == NULL)
                    return issueResults;
                dn_tuples += node_instr->ntuples;

                /* index scan: filter + recheck remove tuples */
                dnFiltereds += node_instr->nfiltered1 + node_instr->nfiltered2;

                /* Count the bloomFilterRows that the Optimizer doesn't count
                 * for CheckDataSkew and CheckInaccurateEstimatedRows
                 */
                if (node_instr->bloomFilterRows > 0 && node_instr->bloomFilterBlocks == 0) {
                    dn_tuples += node_instr->bloomFilterRows;
                }

                /*
                 * Prepare to report Large Table as Innser in HashJoin issue, in order to
                 * report such kind of issue gracefully, we need confirm if inner table
                 * encountered as spill issue first.
                 */
                if (IsA(plan, VecHashJoin) && !write_file) {
                    /*
                     * For VecHashJoin, the temp file spilling info is recored in current
                     * T_VecHashJoin node
                     */
                    write_file = node_instr->sorthashinfo.hash_writefile;
                } else if (nodeTag(plan) == T_HashJoin && !write_file) {
                    Assert(plan->righttree->dop == dop);
                    /*
                     * For RowHashJoin, the temp file spilling info is recorded in Hash node of its
                     * inner(righttree) branch, so we need get instr obejct there
                     */
                    Instrumentation* instrument =
                        u_sess->instr_cxt.global_instr->getInstrSlot(dn_index, plan->righttree->plan_node_id, j);
                    if (instrument != NULL) {
                        /* Say HashJoin's inner spilled here */
                        write_file = (instrument->sorthashinfo.nbatch > 1);
                    }
                }
            }

            /* Update DN level min/max tuples and total tuples */
            min_dn_tuples = Min(dn_tuples, min_dn_tuples);
            max_dn_tuples = Max(dn_tuples, max_dn_tuples);
            total_tuples += dn_tuples;
            totalFiltereds += dnFiltereds;
        }

        switch (nodeTag(plan)) {
            case T_VecHashJoin:
            case T_HashJoin: {
                /* Check large table runs as hashjoin inner */
                if (write_file &&
                    (issueResultsItem = CheckLargeTableInHashJoinInner(
                         ps, dn_num, ComputeSumOfDNTuples(plan->righttree), ComputeSumOfDNTuples(plan->lefttree))) !=
                        NULL) {
                    issueResults = lappend(issueResults, issueResultsItem);
                }

                break;
            }
            case T_VecNestLoop:
            case T_NestLoop: {
                bool is_eq_nestloop_only = IsEqualConditionandNestLoopOnly(plan);
                /* Check large table runs as nestloop with equalness join-cond */
                if (is_eq_nestloop_only &&
                    (issueResultsItem = CheckLargeTableInNestloopWithEqualCondition(ps, dn_num,
                        Max(ComputeSumOfDNTuples(plan->righttree), ComputeSumOfDNTuples(plan->lefttree)))) != NULL) {
                    issueResults = lappend(issueResults, issueResultsItem);
                }

                break;
            }
            case T_VecStream:
            case T_Stream: {
                /* Check large table runs as Broadcast */
                if ((issueResultsItem = CheckLargeTableInBroadcast(ps, dn_num, total_tuples)) != NULL) {
                    issueResults = lappend(issueResults, issueResultsItem);
                }

                break;
            }
            case T_SeqScan: {
                /* Check unsuitable seq scan */
                issueResultsItem = CheckUnsuitableScanMethod(ps, dn_num, total_tuples, totalFiltereds, false, false);
                issueResults = issueResultsItem != NULL ? lappend(issueResults, issueResultsItem) : issueResults;
                break;
            }
            case T_IndexScan:
            case T_IndexOnlyScan:
            case T_BitmapIndexScan: {
                /* Check unsuitable index scan */
                issueResultsItem = CheckUnsuitableScanMethod(ps, dn_num, total_tuples, totalFiltereds, true, false);
                issueResults = issueResultsItem != NULL ? lappend(issueResults, issueResultsItem) : issueResults;
                break;
            }
            case T_CStoreScan: {
                /* Check unsuitable seq scan for cstore */
                issueResultsItem = CheckUnsuitableScanMethod(ps, dn_num, total_tuples, totalFiltereds, false, true);
                issueResults = issueResultsItem != NULL ? lappend(issueResults, issueResultsItem) : issueResults;
                break;
            }
            case T_CStoreIndexScan: {
                /* Check unsuitable index scan for cstore */
                issueResultsItem = CheckUnsuitableScanMethod(ps, dn_num, total_tuples, totalFiltereds, true, true);
                issueResults = issueResultsItem != NULL ? lappend(issueResults, issueResultsItem) : issueResults;
                break;
            }
            default: {
                /* Nothing to do just keep compiler silent */
            }
        }

        /* Analyze data skew issue */
        if ((issueResultsItem = CheckDataSkew(ps, min_dn_tuples, max_dn_tuples)) != NULL) {
            issueResults = lappend(issueResults, issueResultsItem);
        }

        /* Analyze Estimation-Rows is inaccurate issue */
        if (!skip_inaccurate_erows &&
            (issueResultsItem = CheckInaccurateEstimatedRows(ps, dn_num, total_tuples)) != NULL) {
            issueResults = lappend(issueResults, issueResultsItem);
        }
    }

    return issueResults;
}

/*
 * Store plan issues into session-level memory context(for output)
 */
void RecordQueryPlanIssues(const List* results)
{
    ListCell* lc = NULL;
    errno_t rc;

    if (u_sess->attr.attr_resource.resource_track_level < RESOURCE_TRACK_QUERY || results == NIL) {
        return;
    }

    char max_issue_desc[MAX_OPTIMIZER_WARNING_LEN];

    rc = memset_s(max_issue_desc, MAX_OPTIMIZER_WARNING_LEN, '\0', MAX_OPTIMIZER_WARNING_LEN);
    securec_check(rc, "\0", "\0");

    int current = 0;

    /* Keep existing warning info which found in SQL_Planned phase */
    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue) {
        Assert(u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR);

        rc = sprintf_s((char*)max_issue_desc, MAX_OPTIMIZER_WARNING_LEN, "%s\n", 
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
        securec_check_ss_c(rc, "\0", "\0");

        current += strlen(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue) + 1;

        /* Free original used memory space */
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
    }

    /* Scan plan issue list to store them */
    foreach (lc, results) {
        QueryPlanIssueDesc* issue = (QueryPlanIssueDesc*)lfirst(lc);
        int issue_str_len = strlen(issue->issue_suggestion->data);

        /* Check if we hit max allowed planner issue buffer length */
        if (MAX_OPTIMIZER_WARNING_LEN - current <= issue_str_len + 1) {
            ereport(LOG,
                (errmodule(MOD_OPT),
                    (errmsg("Planner issue report is truncated, the rest of planner issues will be skipped"))));
            break;
        }

        rc = sprintf_s((char*)max_issue_desc + current, MAX_OPTIMIZER_WARNING_LEN - current, "%s\n",
            issue->issue_suggestion->data);
        securec_check_ss_c(rc, "\0", "\0");

        current += strlen(issue->issue_suggestion->data) + 1;
        DeleteQueryPlanIssue(issue);
    }

    /* Hold the planer issue info in memory context of workload manager */
    AutoContextSwitch memSwitch(g_instance.wlm_cxt->query_resource_track_mcxt);
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue = pstrdup(max_issue_desc);

    return;
}

/*
 * - Brief: return the more readable operator name
 */
static char* OperatorName(const Plan* plan)
{
    char* pname = NULL;
    char* sname = NULL;
    char* strategy = NULL;
    char* operation = NULL;
    char* pt_option = NULL;
    char* pt_operation = NULL;

    GetPlanNodePlainText((Plan*)plan, &pname, &sname, &strategy, &operation, &pt_operation, &pt_option);

    return sname;
}
