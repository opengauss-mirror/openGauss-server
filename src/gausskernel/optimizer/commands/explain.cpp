/* -------------------------------------------------------------------------
 *
 * explain.cpp
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/explain.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/gs_obsscaninfo.h"
#include "catalog/pg_obsscaninfo.h"
#include "catalog/pg_type.h"
#include "db4ai/create_model.h"
#include "db4ai/hyperparameter_validation.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/prepare.h"
#include "executor/exec/execStream.h"
#include "executor/hashjoin.h"
#include "executor/lightProxy.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeSetOp.h"
#include "foreign/dummyserver.h"
#include "foreign/fdwapi.h"
#include "instruments/generate_report.h"
#include "nodes/print.h"
#include "opfusion/opfusion_util.h"
#include "opfusion/opfusion.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/dataskew.h"
#include "optimizer/dynsmp.h"
#include "optimizer/ml_model.h"
#include "optimizer/nodegroups.h"
#include "optimizer/streamplan.h"
#include "optimizer/planmem_walker.h"
#include "optimizer/planner.h"
#include "optimizer/randomplan.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/hotkey.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "utils/batchsort.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vecsetop.h"
#include "vectorsonic/vsonichashagg.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "pgxc/execRemote.h"
#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "catalog/pgxc_node.h"
#include "pgxc/pgxc.h"
#endif
#include "db4ai/aifuncs.h"

/* Thread local variables for plan_table. */
THR_LOCAL bool OnlySelectFromPlanTable = false;
THR_LOCAL bool OnlyDeleteFromPlanTable = false;
THR_LOCAL bool PTFastQueryShippingStore = true;
THR_LOCAL bool IsExplainPlanStmt = false;
THR_LOCAL bool IsExplainPlanSelectForUpdateStmt = false;

/* Hook for plugins to get control in explain_get_index_name() */               
THR_LOCAL explain_get_index_name_hook_type explain_get_index_name_hook = NULL;

extern TrackDesc trackdesc[];
extern sortMessage sortmessage[];
extern DestReceiver* CreateDestReceiver(CommandDest dest);

/* Array to record plan table column names, type, etc */
static const PlanTableEntry plan_table_cols[] = {{"id", PLANID, INT4OID, ANAL_OPT},
    {"operation", PLAN, TEXTOID, ANAL_OPT},
    {"A-time", ACTUAL_TIME, TEXTOID, ANAL_OPT},
    {"P-time", PREDICT_TIME, TEXTOID, PRED_OPT_TIME},
    {"A-rows", ACTUAL_ROWS, FLOAT8OID, ANAL_OPT},
    {"P-rows", PREDICT_ROWS, FLOAT8OID, PRED_OPT_ROW},
    {"E-rows", ESTIMATE_ROWS, FLOAT8OID, COST_OPT},
    {"E-distinct", ESTIMATE_DISTINCT, TEXTOID, VERBOSE_OPT},
    {"Peak Memory", ACTUAL_MEMORY, TEXTOID, ANAL_OPT},
    {"P-memory", PREDICT_MEMORY, TEXTOID, PRED_OPT_MEM},
    {"E-memory", ESTIMATE_MEMORY, TEXTOID, COST_OPT},
    {"A-width", ACTUAL_WIDTH, TEXTOID, ANAL_OPT},
    {"E-width", ESTIMATE_WIDTH, INT4OID, COST_OPT},
    {"E-costs", ESTIMATE_COSTS, TEXTOID, COST_OPT}
};
#define NUM_VER_COLS 1
#define NUM_ANALYZE_COLS 4
#define NUM_COST_COLS 4
#define MIN_FILE_NUM 1024
#define MIN_DISK_USED 1024

/* OR-able flags for ExplainXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4
#define PUSHDOWN_PREDICATE_FLAG "Pushdown Predicate Filter"
#define PUSHDOWN_FILTER_INFORMATIONAL_CONSTRAINT_FLAG "Pushdown Predicate Filter(Informational Constraint Optimization)"
#define FILTER_INFORMATIONAL_CONSTRAINT_FLAG "Filter(Informational Constraint Optimization)"
#define FILTER_FLAG "Filter"

static void ExplainOneQuery(
     Query* query, IntoClause* into, ExplainState* es, const char* queryString, DestReceiver *dest, ParamListInfo params);
static void report_triggers(ResultRelInfo* rInfo, bool show_relname, ExplainState* es);
template <bool is_pretty>
static void ExplainNode(
    PlanState* planstate, List* ancestors, const char* relationship, const char* plan_name, ExplainState* es);

static void CalculateProcessedRows(
    PlanState* planstate, int idx, int smpIdx, const uint64* innerRows, const uint64* outterRows, uint64* processedrows);

static void show_plan_execnodes(PlanState* planstate, ExplainState* es);
static void show_plan_tlist(PlanState* planstate, List* ancestors, ExplainState* es);
static bool ContainRlsQualInRteWalker(Node* node, void* unused_info);
static void show_expression(
    Node* node, const char* qlabel, PlanState* planstate, List* ancestors, bool useprefix, ExplainState* es);
static void show_qual(
    List* qual, const char* qlabel, PlanState* planstate, List* ancestors, bool useprefix, ExplainState* es);
static void show_scan_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, ExplainState *es,
    bool show_prefix = false);
static void show_skew_optimization(const PlanState* planstate, ExplainState* es);
template <bool generate>
static void show_bloomfilter(Plan* plan, PlanState* planstate, List* ancestors, ExplainState* es);
template <bool generate>
static void show_bloomfilter_number(const List* filterNumList, ExplainState* es);
/**
 * @Description: Show pushdown quals in the flag identifier.
 * @in planstate: Keep the current state of the plan node.
 * @in ancestors: A list of parent PlanState nodes, most-closely-nested first.
 * @in identifier: The label of pushdown predicate clause.
 * @in es: A ExplainState struct.
 * @return None.
 */
static void show_pushdown_qual(PlanState* planstate, List* ancestors, ExplainState* es, const char* identifier);
static void show_upper_qual(List* qual, const char* qlabel, PlanState* planstate, List* ancestors, ExplainState* es);
static void show_groupby_keys(AggState* aggstate, List* ancestors, ExplainState* es);
static void show_sort_keys(SortState* sortstate, List* ancestors, ExplainState* es);
static void show_merge_append_keys(MergeAppendState* mstate, List* ancestors, ExplainState* es);
static void show_merge_sort_keys(PlanState* state, List* ancestors, ExplainState* es);
static void show_startwith_pseudo_entries(PlanState* state, List* ancestors, ExplainState* es);
static void show_sort_info(SortState* sortstate, ExplainState* es);
static void show_hash_info(HashState* hashstate, ExplainState* es);
static void show_vechash_info(VecHashJoinState* hashstate, ExplainState* es);
static void show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es);
static void show_instrumentation_count(const char* qlabel, int which, const PlanState* planstate, ExplainState* es);
static void show_removed_rows(int which, const PlanState* planstate, int idx, int smpIdx, int* removeRows);
static int check_integer_overflow(double var);
static void show_foreignscan_info(ForeignScanState* fsstate, ExplainState* es);
static void show_dfs_block_info(PlanState* planstate, ExplainState* es);
static void show_detail_storage_info_text(Instrumentation* instr, StringInfo instr_info);
static void show_detail_storage_info_json(Instrumentation* instr, StringInfo instr_info, ExplainState* es);
static void show_storage_filter_info(PlanState* planstate, ExplainState* es);
static void show_llvm_info(const PlanState* planstate, ExplainState* es);
static void show_modifytable_merge_info(const PlanState* planstate, ExplainState* es);
static void show_recursive_info(RecursiveUnionState* rustate, ExplainState* es);
static void show_startwith_dfx(StartWithOpState* rustate, ExplainState* es);
static const char* explain_get_index_name(Oid indexId);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir, ExplainState* es);
static void ExplainScanTarget(Scan* plan, ExplainState* es);
static void ExplainModifyTarget(ModifyTable* plan, ExplainState* es);
static void ExplainTargetRel(Plan* plan, Index rti, ExplainState* es);
static void show_on_duplicate_info(ModifyTableState* mtstate, ExplainState* es, List* ancestors);
#ifndef PGXC
static void show_modifytable_info(ModifyTableState* mtstate, ExplainState* es);
#endif /* PGXC */
static void ExplainMemberNodes(const List* plans, PlanState** planstates, List* ancestors, ExplainState* es);
static void ExplainSubPlans(List* plans, List* ancestors, const char* relationship, ExplainState* es);
static void ExplainProperty(const char* qlabel, const char* value, bool numeric, ExplainState* es);
static void ExplainOpenGroup(const char* objtype, const char* labelname, bool labeled, ExplainState* es);
static void ExplainCloseGroup(const char* objtype, const char* labelname, bool labeled, ExplainState* es);
static void ExplainDummyGroup(const char* objtype, const char* labelname, ExplainState* es);
#ifdef PGXC
static void ExplainExecNodes(const ExecNodes* en, ExplainState* es);
static void ExplainRemoteQuery(RemoteQuery* plan, PlanState* planstate, List* ancestors, ExplainState* es);
#endif
static void ExplainXMLTag(const char* tagname, unsigned int flags, ExplainState* es);
static void ExplainJSONLineEnding(ExplainState* es);
static void ExplainYAMLLineStarting(ExplainState* es);
static void escape_yaml(StringInfo buf, const char* str);

template <bool is_detail>
static void show_time(ExplainState* es, const Instrumentation* instrument, int idx);
static void show_cpu(ExplainState* es, const Instrumentation* instrument, double innerCycles, double outerCycles, int nodeIdx,
    int smpIdx, uint64 proRows);
static void show_detail_cpu(ExplainState* es, PlanState* planstate);
static void show_datanode_buffers(ExplainState* es, PlanState* planstate);
static void show_buffers(ExplainState* es, StringInfo infostr, const Instrumentation* instrument, bool is_datanode,
    int nodeIdx, int smpIdx, const char* nodename);
static void show_datanode_time(ExplainState* es, PlanState* planstate);
static void ShowStreamRunNodeInfo(Stream* stream, ExplainState* es);
static void ShowRunNodeInfo(const ExecNodes* en, ExplainState* es, const char* qlabel);
template <bool is_detail>
static void show_datanode_hash_info(ExplainState* es, int nbatch, int nbatch_original, int nbuckets, long spacePeakKb);
static void ShowRoughCheckInfo(ExplainState* es, Instrumentation* instrument, int nodeIdx, int smpIdx);
static void show_hashAgg_info(AggState* hashaggstate, ExplainState* es);
static void ExplainPrettyList(List* data, ExplainState* es);
static void show_pretty_time(ExplainState* es, Instrumentation* instrument, char* node_name, int nodeIdx, int smpIdx,
    int dop, bool executed = true);
static void show_analyze_buffers(ExplainState* es, const PlanState* planstate, StringInfo infostr, int nodeNum);

inline static void show_cpu_info(StringInfo infostr, double incCycles, double exCycles, uint64 proRows);

static void show_track_time_info(ExplainState* es);
template <bool datanode>
static void show_child_cpu_cycles_and_rows(PlanState* planstate, int idx, int smpIdx, double* outerCycles,
    double* innerCycles, uint64* outterRows, uint64* innerRows);
template <bool datanode>
static void CalCPUMemberNode(const List* plans, PlanState** planstates, int idx, int smpIdx, double* Cycles);

static void showStreamnetwork(Stream* stream, ExplainState* es);
static int get_digit(int value);
template <bool datanode>
static void get_oper_time(ExplainState* es, PlanState* planstate, const Instrumentation* instr, int nodeIdx, int smpIdx);
static void show_peak_memory(ExplainState* es, int plan_size);
static bool get_execute_mode(const ExplainState* es, int idx);
static void show_setop_info(SetOpState* setopstate, ExplainState* es);
static void show_grouping_sets(PlanState* planstate, Agg* agg, List* ancestors, ExplainState* es);
static void show_group_keys(GroupState* gstate, List* ancestors, ExplainState* es);
static void show_sort_group_keys(PlanState* planstate, const char* qlabel, int nkeys, const AttrNumber* keycols,
    const Oid* sortOperators, const Oid* collations, const bool* nullsFirst, List* ancestors, ExplainState* es);
static void show_sortorder_options(StringInfo buf, const Node* sortexpr, Oid sortOperator, Oid collation, bool nullsFirst);
static void show_grouping_set_keys(PlanState* planstate, Agg* aggnode, Sort* sortnode, List* context, bool useprefix,
    List* ancestors, ExplainState* es);
static void show_dn_executor_time(ExplainState* es, int plan_node_id, ExecutorTime stage);
static void show_stream_send_time(ExplainState* es, const PlanState* planstate);
static void append_datanode_name(ExplainState* es, char* node_name, int dop, int j);
static void show_tablesample(Plan* plan, PlanState* planstate, List* ancestors, ExplainState* es);
void insert_obsscaninfo(
    uint64 queryid, const char* rel_name, int64 file_count, double scan_data_size, double total_time, int format);
extern List* get_str_targetlist(List* fdw_private);
static void show_wlm_explain_name(ExplainState* es, const char* plan_name, const char* pname, int plan_node_id);
extern unsigned char pg_toupper(unsigned char ch);
static char* set_strtoupper(const char* str, uint32 len);
#ifdef ENABLE_MULTIPLE_NODES
static bool show_scan_distributekey(const Plan* plan)
{
    return (
        IsA(plan, CStoreScan) || IsA(plan, CStoreIndexScan) || IsA(plan, CStoreIndexHeapScan) || IsA(plan, SeqScan) ||
        IsA(plan, DfsScan) || IsA(plan, IndexScan) || IsA(plan, IndexOnlyScan) || IsA(plan, CteScan) ||
        IsA(plan, ForeignScan) || IsA(plan, VecForeignScan) || IsA(plan, BitmapHeapScan) || IsA(plan, TsStoreScan)
    );
}
#endif   /* ENABLE_MULTIPLE_NODES */
static void show_unique_check_info(PlanState *planstate, ExplainState *es);

/*
 * ExplainQuery -
 *	  execute an EXPLAIN command
 */
void ExplainQuery(
    ExplainStmt* stmt, const char* queryString, ParamListInfo params, DestReceiver* dest, char* completionTag)
{
    ExplainState es;
    TupOutputState* tstate = NULL;
    List* rewritten = NIL;
    ListCell* lc = NULL;
    bool timing_set = false;

    /* Initialize ExplainState. */
    ExplainInitState(&es);

    /* Parse options list. */
    foreach (lc, stmt->options) {
        DefElem* opt = (DefElem*)lfirst(lc);

        /*
         * For explain plan stmt:
         * 1.Cache the plan instead of printing it, and return 'EXPLAIN SUCCESS'.
         * 2.Insert the cached info into plan_table, then user can get plan info when select from table;
         * 3.As the explain option "PLAN" cannot work with other options,
         *	 when other options are detected, report as an error.
         * 4.And we do not support execute it on DN now.
         *
         */
        if (strcmp(opt->defname, "plan") == 0) {
            es.plan = defGetBoolean(opt);
            if (es.plan) {
#ifdef ENABLE_MULTIPLE_NODES
                /* We do not support execute explain plan on pgxc datanode or single node. */
                if (IS_PGXC_DATANODE || IS_SINGLE_NODE)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("EXPLAIN PLAN does not support on datanode or single node.")));
#endif
                /* If explain option is "PLAN", it must only has one option. */
                if (list_length(stmt->options) != 1)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("EXPLAIN option 'PLAN' can not work with other options.")));

                /* For explain plan stmt: we mark it to generate plan node id for cte case. */
                IsExplainPlanStmt = true;

                /*
                 * To collect plan info for explain plan, we don't use FQS.
                 * However, select for update can only run when enable_fast_query_shipping = on,
                 * so we should set IsExplainPlanSelectForUpdateStmt = true to make enable_fast_query_shipping work.
                 */
                if (IsA(stmt->query, Query) && ((Query*)stmt->query)->hasForUpdate)
                    IsExplainPlanSelectForUpdateStmt = true;

                /* Trun off fast_query_shipping to collect more plan info. */
                if (PTFastQueryShippingStore == true && IsExplainPlanSelectForUpdateStmt == false)
                    PTFastQueryShippingStore = false;

                /* Get statement_id from ExplainStmt. */
                if (stmt->statement == NULL)
                    es.statement_id = NULL;
                else {
                    Value v = ((A_Const*)stmt->statement)->val;
                    if (v.type == T_Null)
                        es.statement_id = NULL;
                    else
                        es.statement_id = v.val.str;
                }

                /* results are discarded for explain plan we will returen "EXPLAIN SUCCESS" */
                dest->mydest = DestNone;
                t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_PRETTY;

                /* set completionTag */
                errno_t errorno =
                    snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "EXPLAIN SUCCESS");
                securec_check_ss(errorno, "\0", "\0");
            }
        } else if (strcmp(opt->defname, "analyze") == 0)
            es.analyze = defGetBoolean(opt);
        else if (strcmp(opt->defname, "verbose") == 0)
            es.verbose = defGetBoolean(opt);
        else if (strcmp(opt->defname, "costs") == 0)
            es.costs = defGetBoolean(opt);
        else if (strcmp(opt->defname, "buffers") == 0)
            es.buffers = defGetBoolean(opt);
#ifdef ENABLE_MULTIPLE_NODES
        else if (strcmp(opt->defname, "nodes") == 0)
            es.nodes = defGetBoolean(opt);
        else if (strcmp(opt->defname, "num_nodes") == 0)
            es.num_nodes = defGetBoolean(opt);
        else if (pg_strcasecmp(opt->defname, "detail") == 0)
            es.detail = defGetBoolean(opt);
#endif /* ENABLE_MULTIPLE_NODES */
        else if (strcmp(opt->defname, "timing") == 0) {
            timing_set = true;
            es.timing = defGetBoolean(opt);
        } else if (pg_strcasecmp(opt->defname, "cpu") == 0)
            es.cpu = defGetBoolean(opt);
        else if (pg_strcasecmp(opt->defname, "performance") == 0)
            es.performance = defGetBoolean(opt);
        else if (strcmp(opt->defname, "format") == 0) {
            char* p = defGetString(opt);

            if (strcmp(p, "text") == 0)
                es.format = EXPLAIN_FORMAT_TEXT;
            else if (strcmp(p, "xml") == 0)
                es.format = EXPLAIN_FORMAT_XML;
            else if (strcmp(p, "json") == 0)
                es.format = EXPLAIN_FORMAT_JSON;
            else if (strcmp(p, "yaml") == 0)
                es.format = EXPLAIN_FORMAT_YAML;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unrecognized value for EXPLAIN option \"%s\": \"%s\"", opt->defname, p)));
#ifndef ENABLE_MULTIPLE_NODES 
        } else if (strcmp(opt->defname, "predictor") == 0) {
            es.opt_model_name = defGetString(opt);
#endif       
        } else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unrecognized EXPLAIN option \"%s\"", opt->defname)));
    }

    if (es.performance) {
        es.analyze = true;
        es.buffers = true;
        es.costs = true;
        es.cpu = true;
        es.detail = true;
#ifdef ENABLE_MULTIPLE_NODES
#ifdef PGXC
        es.nodes = true;
        es.num_nodes = true;
#endif /* PGXC */
#endif /* ENABLE_MULTIPLE_NODES */
        es.timing = true;
        es.verbose = true;
    }

    if (es.buffers && !es.analyze)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("EXPLAIN option BUFFERS requires ANALYZE")));

    if (es.cpu && !es.analyze)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("EXPLAIN option CPU requires ANALYZE")));
    if (es.detail && !es.analyze)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("EXPLAIN option DETAIL requires ANALYZE")));

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es.format != EXPLAIN_FORMAT_TEXT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("explain_perf_mode requires FORMAT TEXT")));

    /* if the timing was not set explicitly, set default value */
    es.timing = (timing_set) ? es.timing : es.analyze;

    /* check that timing is used with EXPLAIN ANALYZE */
    if (es.timing && !es.analyze)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("EXPLAIN option TIMING requires ANALYZE")));

    /*
     * Parse analysis was done already, but we still have to run the rule
     * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
     * came straight from the parser, or suitable locks were acquired by
     * plancache.c.
     *
     * Because the rewriter and planner tend to scribble on the input, we make
     * a preliminary copy of the source querytree.	This prevents problems in
     * the case that the EXPLAIN is in a portal or plpgsql function and is
     * executed repeatedly.  (See also the same hack in DECLARE CURSOR and
     * PREPARE.)
     */
    AssertEreport(IsA(stmt->query, Query), MOD_EXECUTOR, "unexpect query type");
    rewritten = QueryRewrite((Query*)copyObject(stmt->query));

    /* emit opening boilerplate */
    ExplainBeginOutput(&es);

    if (rewritten == NIL) {
        /*
         * In the case of an INSTEAD NOTHING, tell at least that.  But in
         * non-text format, the output is delimited, so this isn't necessary.
         */
        if (es.format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es.str, "Query rewrites to nothing\n");

        /*
         * In centralized mode, non-stream plans only support EXPLAIN_NORMAL.
         * So set explain_perf_mode to EXPLAIN_NORMAL here.
         */
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL &&
            !(IS_STREAM_PLAN && u_sess->exec_cxt.under_stream_runtime)) {
            t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
        }
    } else {
        ListCell* l = NULL;

        /* Specially, not explain LightCN when explain analyze */
        t_thrd.explain_cxt.explain_light_proxy = IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                                                 u_sess->attr.attr_sql.enable_light_proxy &&
                                                 (list_length(rewritten) == 1) && !es.analyze;

        /* For explain auto-analyze time */
        if (u_sess->attr.attr_sql.enable_autoanalyze && es.analyze &&
            t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
            /* can be not NULL if last-time explain reports error */
            MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->temp_mem_cxt);
            u_sess->analyze_cxt.autoanalyze_timeinfo = makeStringInfo();
            (void)MemoryContextSwitchTo(oldcontext);
        } else
            u_sess->analyze_cxt.autoanalyze_timeinfo = NULL;

        /* show real running datanodes if analyze/performance for pbe */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && es.analyze) {
            t_thrd.postgres_cxt.mark_explain_analyze = true;
            t_thrd.postgres_cxt.mark_explain_only = false;
        } else {
            t_thrd.postgres_cxt.mark_explain_analyze = false;
            t_thrd.postgres_cxt.mark_explain_only = true;
        }

        /* Explain every plan */
        foreach (l, rewritten) {
            ExplainOneQuery((Query*)lfirst(l), NULL, &es, queryString, None_Receiver, params);

            /* Separate plans with an appropriate separator */
            if (lnext(l) != NULL)
                ExplainSeparatePlans(&es);
        }

        t_thrd.explain_cxt.explain_light_proxy = false;
    }

    /* emit closing boilerplate */
    ExplainEndOutput(&es);
    AssertEreport(es.indent == 0, MOD_EXECUTOR, "unexpect es.indent value");

    /* Do not print plan when option is 'plan' and mydest should be set to 'DestNone'. */
    if (!es.plan) {
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
            es.planinfo->dump(dest);
        } else {

            /* output tuples */
            tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));
            if (es.format == EXPLAIN_FORMAT_TEXT)
                (void)do_text_output_multiline(tstate, es.str->data);
            else
                do_text_output_oneline(tstate, es.str->data);
            end_tup_output(tstate);
        }
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (u_sess->instr_cxt.global_instr != NULL) {
        delete u_sess->instr_cxt.global_instr;
        u_sess->instr_cxt.thread_instr = NULL;
        u_sess->instr_cxt.global_instr = NULL;
    }
#endif /* ENABLE_MULTIPLE_NODES */

    if (u_sess->analyze_cxt.autoanalyze_timeinfo != NULL) {
        pfree_ext(u_sess->analyze_cxt.autoanalyze_timeinfo->data);
        pfree_ext(u_sess->analyze_cxt.autoanalyze_timeinfo);
        u_sess->analyze_cxt.autoanalyze_timeinfo = NULL;
    }

    stmt->planinfo = es.planinfo;
    pfree_ext(es.str->data);
    list_free_deep(rewritten);
}

/*
 * Initialize ExplainState.
 */
void ExplainInitState(ExplainState* es)
{
    /* Set default options. */
    errno_t rc = memset_s(es, sizeof(ExplainState), 0, sizeof(ExplainState));
    securec_check(rc, "\0", "\0");
    es->costs = true;
#ifdef ENABLE_MULTIPLE_NODES
#ifdef PGXC
    es->nodes = true;
#endif /* PGXC */
#endif /* ENABLE_MULTIPLE_NODES */
    /* Prepare output buffer. */
    es->str = makeStringInfo();
    es->planinfo = NULL;
    es->from_dn = false;
    es->sql_execute = true;
    es->isexplain_execute = false;
    es->datanodeinfo.all_datanodes = true;
    es->datanodeinfo.len_nodelist = 0;
    es->datanodeinfo.node_index = NULL;
    es->indent = 0;
    es->pindent = 0;
    es->wlm_statistics_plan_max_digit = NULL;
    /* Reset flag for plan_table. */
    IsExplainPlanStmt = false;
    IsExplainPlanSelectForUpdateStmt = false;
}

/*
 * ExplainResultDesc -
 *	  construct the result tupledesc for an EXPLAIN
 */
TupleDesc ExplainResultDesc(ExplainStmt* stmt)
{
    TupleDesc tupdesc = NULL;
    ListCell* lc = NULL;
    Oid result_type = TEXTOID;
    char* attributeName = NULL;
    char RandomPlanName[NAMEDATALEN] = {0};
    bool explain_plan = false; /* for explain plan stmt */

    /* Check for XML format option */
    foreach (lc, stmt->options) {
        DefElem* opt = (DefElem*)lfirst(lc);

        if (strcmp(opt->defname, "format") == 0) {
            char* p = defGetString(opt);

            if (strcmp(p, "xml") == 0)
                result_type = XMLOID;
            else if (strcmp(p, "json") == 0)
                result_type = JSONOID;
            else
                result_type = TEXTOID;
            /* don't "break", as ExplainQuery will use the last value */
        } else if (strcmp(opt->defname, "plan") == 0 && defGetBoolean(opt) && list_length(stmt->options) == 1) {
            /* For explain plan */
            explain_plan = true;
        }          
    }

    if (!explain_plan) {
        /* Need a tuple descriptor representing a single TEXT or XML column */
        tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);

        /* If current plan is set as random plan, explain desc should show random seed value */
        if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
            int rc = 0;
            rc = snprintf_s(
                RandomPlanName, NAMEDATALEN, NAMEDATALEN - 1, "QUERY PLAN (RANDOM seed %u)", get_inital_plan_seed());
            securec_check_ss(rc, "\0", "\0");
        }
        attributeName =
            ((OPTIMIZE_PLAN == u_sess->attr.attr_sql.plan_mode_seed) ? (char*)"QUERY PLAN" : RandomPlanName);

        TupleDescInitEntry(tupdesc, (AttrNumber)1, attributeName, result_type, -1, 0);
    }

    return tupdesc;
}

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
static void ExplainOneQuery(
    Query* query, IntoClause* into, ExplainState* es, const char* queryString, DestReceiver *dest, ParamListInfo params)
{
    /* only use t_thrd.explain_cxt.explain_light_proxy once */
    if (t_thrd.explain_cxt.explain_light_proxy) {
        ExecNodes* single_exec_node = lightProxy::checkLightQuery(query);
        if (single_exec_node == NULL || list_length(single_exec_node->nodeList) +
            list_length(single_exec_node->primarynodelist) != 1) {
            t_thrd.explain_cxt.explain_light_proxy = false;
        }
        CleanHotkeyCandidates(true);
    }

    u_sess->exec_cxt.remotequery_list = NIL;
    /* planner will not cope with utility statements */
    if (query->commandType == CMD_UTILITY) {
        if (IsA(query->utilityStmt, CreateTableAsStmt)) {
            CreateTableAsStmt* ctas = (CreateTableAsStmt*)query->utilityStmt;
            List* rewritten = NIL;

            // INSERT INTO statement needs target table to be created first,
            // so we just support EXPLAIN ANALYZE.
            //
            if (!es->analyze) {
                const char* stmt = ctas->is_select_into ? "SELECT INTO" : "CREATE TABLE AS SELECT";

                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("EXPLAIN %s requires ANALYZE", stmt)));
            }

            // CREATE TABLE AS SELECT and SELECT INTO are rewritten so that the
            // target table is created first. The SELECT query is then transformed
            // into an INSERT INTO statement.
            //
            rewritten = QueryRewriteCTAS(query);

            ExplainOneQuery((Query*)linitial(rewritten), ctas->into, es, queryString, dest, params);

            return;
        }
        else if (IsA(query->utilityStmt, CreateModelStmt)) {
            CreateModelStmt* cm = (CreateModelStmt*) query->utilityStmt;

            /*
             * Create the tuple receiver object and insert hyperp it will need
             */
            DestReceiverTrainModel* dest_train_model = NULL;
            dest_train_model = (DestReceiverTrainModel*) CreateDestReceiver(DestTrainModel);
            configure_dest_receiver_train_model(dest_train_model, CurrentMemoryContext, (AlgorithmML)cm->algorithm,
                                                cm->model, queryString, true);

            PlannedStmt *plan =
                plan_create_model(cm, queryString, params, (DestReceiver *)dest_train_model, CurrentMemoryContext);
            print_hyperparameters(DEBUG1, dest_train_model->hyperparameters);

            ExplainOnePlan(plan, into, es, queryString, dest, params);
            return;

        }

        ExplainOneUtility(query->utilityStmt, into, es, queryString, params);
        return;
    }

    /*
     * Query is not actually executed when declaring a cursor, but will generate a plan.
     * So we can use explain to print the plan, but can not use explain analyze to execute the query.
     */
    if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt)) {
        const char* s = es->performance ? "PERFORMANCE" : "ANALYZE";
        if (es->analyze)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("EXPLAIN %s is not supported when declaring a cursor.", s),
                    errdetail("Query is not actually executed when declaring a cursor.")));
    }

    PlannedStmt* plan = NULL;

    /*
     * plan the query
     * Temporarily apply SET hint using PG_TRY for later recovery
     */
    int nest_level = apply_set_hint(query);
    AFTER_EXPLAIN_APPLY_SET_HINT();
    PG_TRY();
    {
        plan = pg_plan_query(query, 0, params, true);
    }
    PG_CATCH();
    {
        recover_set_hint(nest_level);
        AFTER_EXPLAIN_RECOVER_SET_HINT();
        PG_RE_THROW();
    }
    PG_END_TRY();

    recover_set_hint(nest_level);
    AFTER_EXPLAIN_RECOVER_SET_HINT();
    CleanHotkeyCandidates(true);

    check_gtm_free_plan((PlannedStmt *)plan, es->analyze ? ERROR : WARNING);
    check_plan_mergeinto_replicate(plan, es->analyze ? ERROR : WARNING);
    es->is_explain_gplan = true;

    /* run it (if needed) and produce output */
    ExplainOnePlan(plan, into, es, queryString, dest, params);
}

/*
 * ExplainOneUtility -
 *	  print out the execution plan for one utility statement
 *	  (In general, utility statements don't have plans, but there are some
 *	  we treat as special cases)
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case.
 */
void ExplainOneUtility(
    Node* utilityStmt, IntoClause* into, ExplainState* es, const char* queryString, ParamListInfo params)
{
    if (utilityStmt == NULL)
        return;

    if (IsA(utilityStmt, ExecuteStmt))
        ExplainExecuteQuery((ExecuteStmt*)utilityStmt, into, es, queryString, params);
    else if (IsA(utilityStmt, NotifyStmt)) {
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es->str, "NOTIFY\n");
        else
            ExplainDummyGroup("Notify", NULL, es);
    } else {
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es->str, "Utility statements have no plan structure\n");
        else
            ExplainDummyGroup("Utility Statement", NULL, es);
    }
}

/*
 * @Description: read data from channel and get data before print performance data
 * @in estate - estate information
 * @out - void
 */
static void ExecRemoteprocessPlan(EState* estate)
{
    ListCell* lc = NULL;
    foreach (lc, estate->es_remotequerystates) {
        PlanState* ps = (PlanState*)lfirst(lc);
        ExecEndRemoteQuery((RemoteQueryState*)ps, true);
    }
}

/*
 * @Description: Find real exec dn nodes within all RemoteQuery PlanNodes.
 * @in qd - queryDesc information
 * @out - void
 */
static void GetNodeList(QueryDesc* qd)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    RemoteQuery* rq = NULL;
    foreach (lc, u_sess->exec_cxt.remotequery_list) {
        rq = (RemoteQuery*)lfirst(lc);

        /*
         * when execute pbe query, rq->exec_nodes->nodeList could be NIL,
         * so we should get nodeLists through FindExecNodesInPBE function.
         */
        foreach (lc2, qd->estate->es_remotequerystates) {
            RemoteQueryState* rqs = (RemoteQueryState*)lfirst(lc2);

            /* find node_lists and fill in exec_nodes */
            FindExecNodesInPBE(rqs, rq->exec_nodes, EXEC_ON_DATANODES);
        }
    }
}

/*
 * @Description: Reorganize the executed query which will be pushed down to DN node.
 * @in es - explainstate information
 * @in rq - remote query plannode
 * @in queryString - prepared sql statement in pbe or simple queryString
 * @in explain_sql - executed sql statement in pbe
 * @in nodeoid - DN nodeoid
 * @out - void
 */
void ReorganizeSqlStatement(
    ExplainState* es, RemoteQuery* rq, const char* queryString, const char* explain_sql, Oid nodeoid)
{
    const char* perf_mode = NULL;

    switch (u_sess->attr.attr_sql.guc_explain_perf_mode) {
        case EXPLAIN_NORMAL:
            perf_mode = "normal";
            break;
        case EXPLAIN_PRETTY:
            perf_mode = "pretty";
            break;
        case EXPLAIN_SUMMARY:
            perf_mode = "summary";
            break;
        case EXPLAIN_RUN:
            perf_mode = "run";
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value for parameter explain_perf_mode.")));
            break;
    }

    const char* format = NULL;
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            format = "text";
            break;
        case EXPLAIN_FORMAT_XML:
            format = "xml";
            break;
        case EXPLAIN_FORMAT_JSON:
            format = "json";
            break;
        case EXPLAIN_FORMAT_YAML:
            format = "yaml";
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("unrecognized value for EXPLAIN option \"format\".")));
            break;
    }

    const char* verbose = es->verbose ? "on" : "off";
    const char* costs = es->costs ? "on" : "off";
    const char* num_nodes = es->num_nodes ? "on" : "off";

    StringInfo prefix = makeStringInfo();
    appendStringInfo(prefix,
        "set explain_perf_mode=%s; "
        "explain (format %s, verbose %s, costs %s, nodes off, num_nodes %s)",
        perf_mode,
        format,
        verbose,
        costs,
        num_nodes);
    if (es->isexplain_execute && (!CheckPrepared(rq, nodeoid) || ENABLE_GPC)) {
        /*
         * When execute firstly explain pbe statement in dfx, we should send
         * prepared statement and execute statement at the same time.
         * Also, we should not need to deallocate prepare statement, because
         * prepared statement in DN will only be sent once.
         */
        StringInfo sql = makeStringInfo();
        appendStringInfo(sql, "%s %s %s", queryString, prefix->data, explain_sql);

        rq->execute_statement = sql->data;
    } else {
        StringInfo sql = makeStringInfo();
        appendStringInfo(sql, "%s %s", prefix->data, explain_sql);

        rq->execute_statement = sql->data;
    }
}

/*
 * @Description: Execute FQS query in DN
 * @in es - explainstate information
 * @in queryString - prepared sql statement in pbe or simple queryString
 * @out - void
 */
static void ExecFQSQueryinDn(ExplainState* es, const char* queryString)
{
    ListCell* lc = NULL;
    foreach (lc, u_sess->exec_cxt.remotequery_list) {
        RemoteQuery* rq = (RemoteQuery*)lfirst(lc);

        int dnnum = 0;
        StringInfo* rs = SendExplainToDNs(es, rq, &dnnum, queryString);
        for (int i = 0; i < dnnum; i++) {
            appendStringInfo(es->str, "%s\n", rs[i]->data);
        }
    }
}

/*
 * ExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt,
 * in which case executing the query should result in creating that table.
 *
 * Since we ignore any DeclareCursorStmt that might be attached to the query,
 * if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll actually run the
 * query.  This is different from pre-8.3 behavior but seems more useful than
 * not running the query.  No cursor will be created, however.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
void ExplainOnePlan(
    PlannedStmt* plannedstmt,
    IntoClause* into,
    ExplainState* es,
    const char* queryString,
    DestReceiver *dest,
    ParamListInfo params)
{
    QueryDesc* queryDesc = NULL;
    instr_time starttime;
    instr_time exec_starttime;    /* executor init start time */
    double exec_totaltime = 0;    /* executor init total time */
    double execrun_totaltime = 0; /* executor run total time */
    double execend_totaltime = 0; /* executor end total time */
    double totaltime = 0;
    int eflags;
    int instrument_option = 0;

    if (es->analyze && es->timing)
        instrument_option |= INSTRUMENT_TIMER;
    else if (es->analyze)
        instrument_option |= INSTRUMENT_ROWS;

    if (es->buffers)
        instrument_option |= INSTRUMENT_BUFFERS;

    INSTR_TIME_SET_CURRENT(starttime);

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.
     */
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    /* Create a QueryDesc for the query */
    queryDesc = CreateQueryDesc(
        plannedstmt, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, params, instrument_option);

    queryDesc->plannedstmt->instrument_option = instrument_option;

    /* Select execution options */
    if (es->analyze)
        eflags = 0; /* default run-to-completion flags */
    else
        eflags = EXEC_FLAG_EXPLAIN_ONLY;
    if (into != NULL)
        eflags |= GetIntoRelEFlags(into);

#ifdef STREAMPLAN
    /*
     * u_sess->exec_cxt.under_stream_runtime is set to true in ExecInitRemoteQuery() if
     * node->is_simple is true. ExecInitRemoteQuery() will be called during
     * calling ExecutorStart(queryDesc, eflags).
     * light performance is changed to allocate streaminstrumentation firstly,
     * and than calling ExecutorStart for ExecInitNode in CN.
     */
    /*  only stream plan can use  u_sess->instr_cxt.global_instr to collect executor info */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR &&
#else
    if (StreamTopConsumerAmI() &&
#endif
        queryDesc->plannedstmt->is_stream_plan == true &&
        check_stream_support() && instrument_option != 0 && u_sess->instr_cxt.global_instr == NULL &&
        queryDesc->plannedstmt->num_nodes != 0) {
        int dop = queryDesc->plannedstmt->query_dop;

        u_sess->instr_cxt.global_instr = StreamInstrumentation::InitOnCn(queryDesc, dop);

        // u_sess->instr_cxt.thread_instr in CN
        u_sess->instr_cxt.thread_instr =
            u_sess->instr_cxt.global_instr->allocThreadInstrumentation(queryDesc->plannedstmt->planTree->plan_node_id);

        {
            AutoContextSwitch cxtGuard(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
            u_sess->instr_cxt.obs_instr = New(CurrentMemoryContext) OBSInstrumentation();
        }
    }

#endif

    /* call ExecutorStart to prepare the plan for execution */
    INSTR_TIME_SET_CURRENT(exec_starttime);

    if (ENABLE_WORKLOAD_CONTROL && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        /* Check if need track resource */
        u_sess->exec_cxt.need_track_resource =
            ((unsigned int)eflags & EXEC_FLAG_EXPLAIN_ONLY) ? false : WLMNeedTrackResource(queryDesc);
    }

    ExecutorStart(queryDesc, eflags);

    if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
        !es->analyze && es->is_explain_gplan) {
        GetNodeList(queryDesc);
    }

    exec_totaltime += elapsed_time(&exec_starttime);

#ifdef STREAMPLAN
    /*
     * Init planinfo when:
     * 1. explain_perf_mode is not EXPLAIN_NORMAL and it`s a stream plan.
     * 2. For EXPLAIN PLAN, we still collect plan info.
     */
    if ((t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && IS_STREAM_PLAN &&
            u_sess->exec_cxt.under_stream_runtime) ||
        es->plan) {
        es->planinfo = (PlanInformation*)palloc0(sizeof(PlanInformation));
        es->planinfo->init(es, plannedstmt->num_plannodes, plannedstmt->num_nodes, (plannedstmt->query_mem[0] > 0));
    } else
        t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
#endif

    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        if (!((unsigned int)eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
            WLMInitQueryPlan(queryDesc);
            dywlm_client_manager(queryDesc);
        } else if (DY_MEM_ADJ(queryDesc->plannedstmt)) {
            /* for explain statement, dywlm will not adjust the mem, so adjust it here */
            bool use_tenant = (u_sess->wlm_cxt->wlm_params.rpdata.rpoid != DEFAULT_POOL_OID);
            CalculateQueryMemMain(queryDesc->plannedstmt, use_tenant, true);
        }
    }

    /* Execute the plan for statistics if asked for */
    if (es->analyze) {
        ScanDirection dir;

        /* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
        if (into != NULL && into->skipData) {
            dir = NoMovementScanDirection;
            es->sql_execute = false;
        } else
            dir = ForwardScanDirection;

        if (u_sess->exec_cxt.need_track_resource) {
            ExplainNodeFinish(queryDesc->planstate, queryDesc->plannedstmt, 0.0, true);
        }

        INSTR_TIME_SET_CURRENT(exec_starttime);
        /* run the plan */
        ExecutorRun(queryDesc, dir, 0L);
        execrun_totaltime += elapsed_time(&exec_starttime);

        /* run cleanup too */
        ExecutorFinish(queryDesc);

        INSTR_TIME_SET_CURRENT(exec_starttime);
        /* run cleanup for limit node */
        ExecRemoteprocessPlan(queryDesc->estate);
        execend_totaltime += elapsed_time(&exec_starttime);

        /* SQL Self-Tuning : Analyze query plan issues based on runtime info when query execution is finished */
        if (u_sess->exec_cxt.need_track_resource &&
            u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR) {
            List* issueResults = PlanAnalyzerOperator(queryDesc, queryDesc->planstate);

            /* If plan issue is found, store it in sysview gs_wlm_session_history */
            if (issueResults != NIL) {
                RecordQueryPlanIssues(issueResults);
            }
        }

        if (u_sess->exec_cxt.need_track_resource) {
            u_sess->instr_cxt.can_record_to_table = true;
            ExplainNodeFinish(queryDesc->planstate, queryDesc->plannedstmt, GetCurrentTimestamp(), false);
        }

        /* We can't run ExecutorEnd 'till we're done printing the stats... */
        totaltime += elapsed_time(&starttime);
    }

#ifndef ENABLE_MULTIPLE_NODES
    SetNullPrediction(queryDesc->planstate);
    /* Call machine learning prediction routine for test */
    if (es->opt_model_name != NULL && PredictorIsValid((const char*)es->opt_model_name)) {
        char* file_name = PreModelPredict(queryDesc->planstate, queryDesc->plannedstmt);
        ModelPredictForExplain(queryDesc->planstate, file_name, (const char*)es->opt_model_name);
    }
#endif /* ENABLE_MULTIPLE_NODES */

    ExplainOpenGroup("Query", NULL, true, es);

    if (IS_PGXC_DATANODE && u_sess->attr.attr_sql.enable_opfusion == true &&
        es->format == EXPLAIN_FORMAT_TEXT && u_sess->attr.attr_sql.enable_hypo_index == false) {
        FusionType type = OpFusion::getFusionType(NULL, params, list_make1(queryDesc->plannedstmt));
        if (!es->is_explain_gplan)
            type = NOBYPASS_NO_CPLAN;
        if (type < BYPASS_OK && type > NONE_FUSION) {
            appendStringInfo(es->str, "[Bypass]\n");
        } else if (u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_LOG) {
            appendStringInfo(es->str, "[No Bypass]");
            const char* bypass_reason = getBypassReason(type);
            appendStringInfo(es->str, "reason: %s.\n", bypass_reason);
        }
    }

    /* Create textual dump of plan tree */
    ExplainPrintPlan(es, queryDesc);

    /* Print post-plan-tree explanation info, if there is any. */
    if (es->post_str != NULL) {
        appendStringInfo(es->str, "%s\n", es->post_str->data);
        DestroyStringInfo(es->post_str);
        es->post_str = NULL;
    }

    /* for explain plan: after explained all nodes */
    if (es->plan && es->planinfo != NULL) {
        es->planinfo->m_planTableData->set_plan_table_ids(queryDesc->plannedstmt->queryId, es);

        /* insert all nodes` tuple into table. */
        es->planinfo->m_planTableData->insert_plan_table_tuple();
    }

#ifdef STREAMPLAN

    if (u_sess->instr_cxt.global_instr && u_sess->instr_cxt.global_instr->needTrack() &&
        u_sess->instr_cxt.global_instr->isTrack()) {
        if (es->format == EXPLAIN_FORMAT_TEXT && t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL)
            show_track_time_info(es);
    }

    if (u_sess->instr_cxt.obs_instr) {
        u_sess->instr_cxt.obs_instr->insertData(queryDesc->plannedstmt->queryId);
    }
#endif

    /* Print info about runtime of triggers */
    if (es->analyze) {
        ResultRelInfo* rInfo = NULL;
        bool show_relname = false;
        int numrels = queryDesc->estate->es_num_result_relations;
        List* targrels = queryDesc->estate->es_trig_target_relations;
        int nr;
        ListCell* l = NULL;

        ExplainOpenGroup("Triggers", "Triggers", false, es);

        show_relname = (numrels > 1 || targrels != NIL);
        rInfo = queryDesc->estate->es_result_relations;
        for (nr = 0; nr < numrels; rInfo++, nr++)
            report_triggers(rInfo, show_relname, es);

        foreach (l, targrels) {
            rInfo = (ResultRelInfo*)lfirst(l);
            report_triggers(rInfo, show_relname, es);
        }

        ExplainCloseGroup("Triggers", "Triggers", false, es);
    }

    /* Check plan was influenced by row level security or not, here need to skip remote dummy node */
    if (range_table_walker(
        plannedstmt->rtable, (bool (*)())ContainRlsQualInRteWalker, NULL, QTW_EXAMINE_RTES | QTW_IGNORE_DUMMY)) {
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL 
            && es->planinfo->m_detailInfo != NULL) {
            appendStringInfo(es->planinfo->m_detailInfo->info_str,
                "Notice: This query is influenced by row level security feature\n");
        }
        ExplainPropertyText("Notice", "This query is influenced by row level security feature", es);
    }

    /* traverse all remote query nodes and exec fqs query */
    if (u_sess->exec_cxt.remotequery_list != NIL) {
        if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
            !es->analyze) {
            ExecFQSQueryinDn(es, queryString);
        }
        u_sess->exec_cxt.remotequery_list = NIL;
    }
    /*
     * Close down the query and free resources.  Include time for this in the
     * total runtime (although it should be pretty minimal).
     */
    INSTR_TIME_SET_CURRENT(starttime);

    ExecutorEnd(queryDesc);

    execend_totaltime += elapsed_time(&starttime);

    FreeQueryDesc(queryDesc);

    PopActiveSnapshot();

    /* We need a CCI just in case query expanded to multiple plans */
    if (es->analyze)
        CommandCounterIncrement();

    totaltime += elapsed_time(&starttime);

    if (es->analyze) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
                if (MEMORY_TRACKING_QUERY_PEAK)
                    appendStringInfo(es->str, "Total runtime: %.3f ms, Peak Memory: %ld KB\n", 1000.0 * totaltime,
                                     (int64)(t_thrd.utils_cxt.peakedBytesInQueryLifeCycle/1024));
                else
                    appendStringInfo(es->str, "Total runtime: %.3f ms\n", 1000.0 * totaltime);
            }
            else if (es->planinfo != NULL && es->planinfo->m_query_summary) {
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Coordinator executor start time: %.3f ms\n",
                    1000.0 * exec_totaltime);

                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Coordinator executor run time: %.3f ms\n",
                    1000.0 * execrun_totaltime);

                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Coordinator executor end time: %.3f ms\n",
                    1000.0 * execend_totaltime);

                if (es->planinfo->m_runtimeinfo && es->planinfo->m_query_summary->m_size) {
                    long spacePeakKb = (es->planinfo->m_query_summary->m_size + 1023) / 1024;
                    appendStringInfo(es->planinfo->m_query_summary->info_str, "Total network : %ldkB\n", spacePeakKb);
                }
                es->planinfo->m_query_summary->m_size = 0;

                /* show auto-analyze time */
                if (u_sess->analyze_cxt.autoanalyze_timeinfo && u_sess->analyze_cxt.autoanalyze_timeinfo->len > 0) {
                    appendStringInfo(es->planinfo->m_query_summary->info_str,
                        "Autoanalyze runtime: %s\n",
                        u_sess->analyze_cxt.autoanalyze_timeinfo->data);
                }

                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Planner runtime: %.3f ms\n",
                    1000.0 * plannedstmt->plannertime);
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Plan size: %d byte\n",
                    u_sess->instr_cxt.plan_size);

                appendStringInfo(es->planinfo->m_query_summary->info_str, "Query Id: %lu\n", u_sess->debug_query_id);
                appendStringInfo(
                    es->planinfo->m_query_summary->info_str, "Total runtime: %.3f ms\n", 1000.0 * totaltime);
            }
        } else
            ExplainPropertyFloat("Total Runtime", 1000.0 * totaltime, 3, es);
    }

    output_hint_warning(plannedstmt->plan_hint_warning, WARNING);
    /* output warning for no-analyzed relation name if set explain verbose. */
    if (es->verbose)
        output_noanalyze_rellist_to_log(WARNING);

    ExplainCloseGroup("Query", NULL, true, es);
}

/*
 * ExplainPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.	Other fields in *es are
 * initialized here.
 *
 * NB: will not work on utility statements
 */
void ExplainPrintPlan(ExplainState* es, QueryDesc* queryDesc)
{
    AssertEreport(queryDesc->plannedstmt != NULL, MOD_EXECUTOR, "unexpect null value");
    es->pstmt = queryDesc->plannedstmt;
    es->rtable = queryDesc->plannedstmt->rtable;
    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL) {
        es->planinfo->m_query_id = queryDesc->plannedstmt->queryId;
        /* print peak memory info if analyze is on */
        if (es->analyze && es->planinfo->m_staticInfo)
            show_peak_memory(es, es->planinfo->m_planInfo->m_size);
        ExplainNode<true>(queryDesc->planstate, NIL, NULL, NULL, es);

        /* print other info, like random seed, query mem info, etc */
        if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
            appendStringInfo(es->planinfo->m_query_summary->info_str, "Random seed: %u\n", get_inital_plan_seed());
        }
        if (es->pstmt->query_mem[0] != 0) {
            if (es->pstmt->assigned_query_mem[0] > 0)
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "System available mem: %dKB\n",
                    es->pstmt->assigned_query_mem[0]);
            if (es->pstmt->assigned_query_mem[1] > 0)
                appendStringInfo(
                    es->planinfo->m_query_summary->info_str, "Query Max mem: %dKB\n", es->pstmt->assigned_query_mem[1]);
            appendStringInfo(
                es->planinfo->m_query_summary->info_str, "Query estimated mem: %dKB\n", es->pstmt->query_mem[0]);
        }

        if (es->pstmt->is_dynmaic_smp) {
            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Avail/Max core: %d/%d\n",
                    es->pstmt->dynsmp_avail_cpu,
                    es->pstmt->dynsmp_max_cpu);
                appendStringInfo(es->planinfo->m_query_summary->info_str, "Cpu util: %d\n", es->pstmt->dynsmp_cpu_util);
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Active statement: %d\n",
                    es->pstmt->dynsmp_active_statement);
            } else {
                appendStringInfo(es->planinfo->m_query_summary->info_str, "Avail/Max core: NA/NA\n");
                appendStringInfo(es->planinfo->m_query_summary->info_str, "Cpu util: NA\n");
                appendStringInfo(es->planinfo->m_query_summary->info_str, "Active statement: NA\n");
            }

            appendStringInfo(es->planinfo->m_query_summary->info_str,
                "Query estimated cpu: %.1lf\n",
                es->pstmt->dynsmp_query_estimate_cpu_usge);

            appendStringInfo(
                es->planinfo->m_query_summary->info_str, "Mem allowed dop: %d\n", es->pstmt->dynsmp_dop_mem_limit);

            if (es->pstmt->dynsmp_min_non_spill_dop == DYNMSP_ALREADY_SPILLED) {
                appendStringInfo(es->planinfo->m_query_summary->info_str, "Min non-spill dop: already spilled\n");
            } else if (es->pstmt->dynsmp_min_non_spill_dop == DYNMSP_SPILL_IF_USE_HIGHER_DOP) {
                appendStringInfo(
                    es->planinfo->m_query_summary->info_str, "Min non-spill dop: spill if use higher dop\n");
            } else {
                appendStringInfo(es->planinfo->m_query_summary->info_str,
                    "Min non-spill dop: %d\n",
                    es->pstmt->dynsmp_min_non_spill_dop);
            }

            appendStringInfo(
                es->planinfo->m_query_summary->info_str, "Initial dop: %d\n", es->pstmt->dynsmp_plan_original_dop);
            appendStringInfo(
                es->planinfo->m_query_summary->info_str, "Final dop: %d\n", es->pstmt->dynsmp_plan_optimal_dop);
        }
    } else
        ExplainNode<false>(queryDesc->planstate, NIL, NULL, NULL, es);
}

/*
 * ExplainQueryText -
 *	  add a "Query Text" node that contains the actual text of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void ExplainQueryText(ExplainState* es, QueryDesc* queryDesc)
{
    if (queryDesc->sourceText)
        ExplainPropertyText("Query Text", queryDesc->sourceText, es);
}

/*
 * Explain a list of children of a ExtensiblePlan.
 */
static void ExplainExtensibleChildren(ExtensiblePlanState* planState, List* ancestors, ExplainState* es)
{
    ListCell* cell = NULL;
    const char* label = (list_length(planState->extensible_ps) != 1 ? "children" : "child");

    foreach (cell, planState->extensible_ps)
        ExplainNode<false>((PlanState*)lfirst(cell), ancestors, label, NULL, es);
}

/*
 * ExplainOneQueryForStatistic -
 *	  print out the execution plan for one Query for wlm statistics
 * in - queryDesc: query plan description.
 */
void ExplainOneQueryForStatistics(QueryDesc* queryDesc)
{
    ExplainState es;
    ExplainInitState(&es);
    es.costs = true;
    es.nodes = false;
    es.format = EXPLAIN_FORMAT_TEXT;

    int before_explain_perf_mode = t_thrd.explain_cxt.explain_perf_mode;
    t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;

    if (queryDesc->plannedstmt == NULL || queryDesc->planstate == NULL)
        return;

    es.pstmt = queryDesc->plannedstmt;
    es.rtable = queryDesc->plannedstmt->rtable;

    int max_plan_id = queryDesc->plannedstmt->num_plannodes;
    int wlm_statistics_plan_max_digit = get_digit(max_plan_id);
    es.wlm_statistics_plan_max_digit = &wlm_statistics_plan_max_digit;

#ifndef ENABLE_MULTIPLE_NODES
    /* avoid showing prediction info for wlm statistics */
    SetNullPrediction(queryDesc->planstate);
#endif

    /* adaptation for active sql */
    StreamInstrumentation* oldInstr = u_sess->instr_cxt.global_instr;
    u_sess->instr_cxt.global_instr = NULL;
    u_sess->exec_cxt.under_auto_explain = true;
    appendStringInfo(es.str, "Coordinator Name: %s\n", g_instance.attr.attr_common.PGXCNodeName);
    ExplainNode<false>(queryDesc->planstate, NIL, NULL, NULL, &es);
    u_sess->exec_cxt.under_auto_explain = false;
    u_sess->instr_cxt.global_instr = oldInstr;

    AutoContextSwitch memSwitch(g_instance.wlm_cxt->query_resource_track_mcxt);
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan = (char*)palloc0(es.str->len + 1 + 1);
    errno_t rc =
        memcpy_s(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan, es.str->len, es.str->data, es.str->len);
    securec_check(rc, "\0", "\0");
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan[es.str->len] = '\n';
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan[es.str->len + 1] = '\0';
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = es.str->len + 1 + 1;

    t_thrd.explain_cxt.explain_perf_mode = before_explain_perf_mode;
}

/*
 * report_triggers -
 *		report execution stats for a single relation's triggers
 */
static void report_triggers(ResultRelInfo* rInfo, bool show_relname, ExplainState* es)
{
    int nt;

    if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
        return;
    for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++) {
        Trigger* trig = rInfo->ri_TrigDesc->triggers + nt;
        Instrumentation* instr = rInfo->ri_TrigInstrument + nt;
        char* relname = NULL;
        char* conname = NULL;

        /* Must clean up instrumentation state */
        InstrEndLoop(instr);

        /*
         * We ignore triggers that were never invoked; they likely aren't
         * relevant to the current query type.
         */
        if (instr->ntuples == 0)
            continue;

        ExplainOpenGroup("Trigger", NULL, true, es);

        relname = RelationGetRelationName(rInfo->ri_RelationDesc);
        if (OidIsValid(trig->tgconstraint))
            conname = get_constraint_name(trig->tgconstraint);

        /*
         * In text format, we avoid printing both the trigger name and the
         * constraint name unless VERBOSE is specified.  In non-text formats
         * we just print everything.
         */
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (es->verbose || conname == NULL)
                appendStringInfo(es->str, "Trigger %s", trig->tgname);
            else
                appendStringInfoString(es->str, "Trigger");
            if (conname != NULL)
                appendStringInfo(es->str, " for constraint %s", conname);
            if (show_relname)
                appendStringInfo(es->str, " on %s", relname);
            appendStringInfo(es->str, ": time=%.3f calls=%.0f\n", 1000.0 * instr->total, instr->ntuples);
        } else {
            ExplainPropertyText("Trigger Name", trig->tgname, es);
            if (conname != NULL)
                ExplainPropertyText("Constraint Name", conname, es);
            ExplainPropertyText("Relation", relname, es);
            ExplainPropertyFloat("Time", 1000.0 * instr->total, 3, es);
            ExplainPropertyFloat("Calls", instr->ntuples, 0, es);
        }

        if (conname != NULL)
            pfree_ext(conname);

        ExplainCloseGroup("Trigger", NULL, true, es);
    }
}

/* Compute elapsed time in seconds since given timestamp */
double elapsed_time(instr_time* starttime)
{
    instr_time endtime;

    INSTR_TIME_SET_CURRENT(endtime);
    INSTR_TIME_SUBTRACT(endtime, *starttime);
    return INSTR_TIME_GET_DOUBLE(endtime);
}

static void show_bucket_info(PlanState* planstate, ExplainState* es, bool is_pretty)
{
    Scan* scanplan = (Scan*)planstate->plan;
    BucketInfo* bucket_info = scanplan->bucketInfo;
    int selected_buckets = list_length(bucket_info->buckets);
    if (selected_buckets == 0) {
        /* 0 means all buckets */
        selected_buckets = BUCKETDATALEN;
    }

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        if (is_pretty) {
            es->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str,
                "Selected Buckets %d of %d : %s\n",
                selected_buckets,
                BUCKETDATALEN,
                bucketInfoToString(bucket_info));
        } else {
            if (es->wlm_statistics_plan_max_digit) {
                appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
                appendStringInfoString(es->str, " | ");
                appendStringInfoSpaces(es->str, es->indent);
            } else {
                appendStringInfoSpaces(es->str, es->indent * 2);
            }

            appendStringInfo(es->str,
                "Selected Buckets %d of %d : %s\n",
                selected_buckets,
                BUCKETDATALEN,
                bucketInfoToString(bucket_info));
        }
    }
}

typedef struct PrintFlags {
    bool print_number;
    bool print_twodots;
    bool print_comma;
    bool print_prev;
} PrintFlags;

static void set_print_flags(int i, bool showPrev, bool isLast, PrintFlags* flags)
{
    if (i == 0) {
        flags->print_prev = false;
        flags->print_number = true;
        flags->print_twodots = false;
        flags->print_comma = false;
    } else if (showPrev) {
        /* the last partition */
        if (isLast) {
            flags->print_prev = false;
            flags->print_number = true;
            flags->print_twodots = true;
            flags->print_comma = false;
        } else {
            flags->print_prev = false;
            flags->print_number = false;
            flags->print_twodots = false;
            flags->print_comma = false;
        }
    } else {
        /* the previous has not been printed */
        if (!flags->print_number)
            flags->print_prev = true;
        else
            flags->print_prev = false;

        flags->print_number = true;
        flags->print_twodots = false;
        flags->print_comma = true;
    }
}

static void add_pruning_info_nums(StringInfo dest, PrintFlags flags, int partID, int partID_prev)
{
    /* first check whether print the previous partition number */
    if (flags.print_prev) {
        appendStringInfoString(dest, "..");
        appendStringInfo(dest, "%d", partID_prev + 1);
    }

    /* then print the current partition number */
    if (flags.print_twodots) {
        appendStringInfoString(dest, "..");
    } else if (flags.print_comma) {
        appendStringInfoString(dest, ",");
    }

    if (flags.print_number) {
        appendStringInfo(dest, "%d", partID + 1);
    }
}

/*
 * Show number of subpartitions selected for each partition.
 * If subpartitions is not pruned at all, show "ALL".
 * The caller should call DestroyStringInfo to free allocated memory.
 */
static StringInfo get_subpartition_pruning_info(Scan* scanplan, List* rtable)
{
    PruningResult* pr = scanplan->pruningInfo;
    StringInfo strif = makeStringInfo();
    ListCell* lc = NULL;
    bool all = true;
    RangeTblEntry* rte = rt_fetch(scanplan->scanrelid, rtable);
    Relation rel = heap_open(rte->relid, AccessShareLock);
    List* subpartList = RelationGetSubPartitionOidListList(rel);
    int idx = 0;
    foreach (lc, pr->ls_selectedSubPartitions) {
        idx++;
        if (idx > list_length(subpartList)) {
            break;
        }
        SubPartitionPruningResult* spr = (SubPartitionPruningResult*)lfirst(lc);
        /* check if all subpartition is selected */
        int selected = list_length(spr->ls_selectedSubPartitions);
        int count = list_length((List*)list_nth(subpartList, spr->partSeq));
        all &= (selected == count);
        /* save pruning map in strif temporarily */
        appendStringInfo(strif, "%d:%d", spr->partSeq + 1, selected);
        if (lc != list_tail(pr->ls_selectedSubPartitions)) {
            appendStringInfo(strif, ", ");
        }
    }

    if (all) {
        resetStringInfo(strif);
        appendStringInfo(strif, "ALL");
    }

    /* clean-ups */
    ReleaseSubPartitionOidList(&subpartList);
    heap_close(rel, AccessShareLock);
    return strif;
}
/*
 * Show subpartition pruning information in the form of:
 *      Selected Partitions: 1,3
 *      Selected Subpartitions: 1:1,3:1
 * Which means select first subpartition of 1 and 3 partition
 * Note that pretty mode is not supported in this version, since subpartitions can only
 * be created in Cent deployment for now and pretty is only for stream plans.
 */
static void add_subpartition_pruning_info_text(PlanState* planstate, ExplainState* es)
{
    Scan* scanplan = (Scan*)planstate->plan;
    PruningResult* pr = scanplan->pruningInfo;
    if (pr->ls_selectedSubPartitions == NIL) {
        return;
    }

    StringInfo dest = es->str; /* Consider perf_mode = normal only */

    /* Apply indent properly */
    if (es->wlm_statistics_plan_max_digit) {
        appendStringInfoSpaces(dest, *es->wlm_statistics_plan_max_digit);
        appendStringInfoString(dest, " | ");
        appendStringInfoSpaces(dest, es->indent);
    } else {
        appendStringInfoSpaces(dest, es->indent * 2); /* 2 is the coefficient for text format indent */
    }
    appendStringInfo(dest, "Selected Subpartitions:  ");

    /* Show the content */
    if (scanplan->itrs <= 0) {
        appendStringInfo(dest, "NONE");
    } else if (scanplan->pruningInfo->expr != NULL) {
        appendStringInfo(dest, "PART");
    } else {
        StringInfo strif = get_subpartition_pruning_info(scanplan, es->rtable);
        appendStringInfoString(dest, strif->data);
        DestroyStringInfo(strif);
    }
    appendStringInfoChar(dest, '\n');
}

static void add_subpartition_pruning_info_others(PlanState* planstate, ExplainState* es)
{
    Scan* scanplan = (Scan*)planstate->plan;
    PruningResult* pr = scanplan->pruningInfo;
    if (pr->ls_selectedSubPartitions == NIL) {
        return;
    }

    if (scanplan->itrs <= 0) {
        ExplainPropertyText("Selected Subpartitions", "NONE", es);
    } else if (scanplan->pruningInfo->expr != NULL) {
        ExplainPropertyText("Selected Subpartitions", "PART", es);
    } else {
        StringInfo strif = get_subpartition_pruning_info(scanplan, es->rtable);
        ExplainPropertyText("Selected Subpartitions", strif->data, es);
        DestroyStringInfo(strif);
    }
}

static void show_pruning_info(PlanState* planstate, ExplainState* es, bool is_pretty)
{
    Scan* scanplan = (Scan*)planstate->plan;
    PrintFlags flags = {false, false, false, false};
    int partID = 0;
    int partID_prev = 0;

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        int i = 0;

        if (is_pretty == false) {
            if (es->wlm_statistics_plan_max_digit) {
                appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
                appendStringInfoString(es->str, " | ");
                appendStringInfoSpaces(es->str, es->indent);
            } else {
                appendStringInfoSpaces(es->str, es->indent * 2);
            }
            appendStringInfo(es->str, "Selected Partitions:  ");
        } else {
            es->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str, "Selected Partitions:  ");
        }

        if (scanplan->itrs <= 0) {
            if (is_pretty == false)
                appendStringInfo(es->str, "NONE");
            else
                appendStringInfo(es->planinfo->m_detailInfo->info_str, "NONE");
        } else if (scanplan->pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "%s", "PART");
        } else {
            ListCell* cell = NULL;
            List* part_seqs = scanplan->pruningInfo->ls_rangeSelectedPartitions;

            AssertEreport(scanplan->itrs == scanplan->pruningInfo->ls_rangeSelectedPartitions->length,
                MOD_EXECUTOR,
                "unexpect list length");
            foreach (cell, part_seqs) {
                // store the prev first partID
                if (i > 0) {
                    partID_prev = partID;
                }

                partID = lfirst_int(cell);
                if (partID < 0)
                    break;

                // set print flags for the current and the previouse partition number
                set_print_flags(i, partID - partID_prev == 1, i == scanplan->itrs - 1, &flags);

                if (is_pretty) {
                    add_pruning_info_nums(es->planinfo->m_detailInfo->info_str, flags, partID, partID_prev);
                } else {
                    add_pruning_info_nums(es->str, flags, partID, partID_prev);
                }
                i++;
            }
        }
        if (is_pretty)
            appendStringInfoChar(es->planinfo->m_detailInfo->info_str, '\n');
        else
            appendStringInfoChar(es->str, '\n');

        if (scanplan->itrs > 0 && scanplan->pruningInfo->expr == NULL) {
            add_subpartition_pruning_info_text(planstate, es);
        }
    } else {
        if (scanplan->itrs <= 0) {
            ExplainPropertyText("Selected Partitions", "NONE", es);
        } else if (scanplan->pruningInfo->expr != NULL) {
            ExplainPropertyText("Selected Partitions", "PART", es);
        } else {
            int i = 0;
            StringInfo strif;
            ListCell* cell = NULL;
            List* part_seqs = scanplan->pruningInfo->ls_rangeSelectedPartitions;

            AssertEreport(scanplan->itrs == scanplan->pruningInfo->ls_rangeSelectedPartitions->length,
                MOD_EXECUTOR,
                "unexpect list length");

            /*form a tring of partition numbers, by 2 charaters space splitted*/
            strif = makeStringInfo();
            foreach (cell, part_seqs) {
                // store the prev first partID
                if (i > 0) {
                    partID_prev = partID;
                }

                partID = lfirst_int(cell);
                if (partID < 0)
                    break;

                // set print flags for the current and the previouse partition number
                set_print_flags(i, partID - partID_prev == 1, i == scanplan->itrs - 1, &flags);

                add_pruning_info_nums(strif, flags, partID, partID_prev);

                i++;
            }
            /*print out the partition numbers string*/
            ExplainPropertyText("Selected Partitions", strif->data, es);
            pfree_ext(strif->data);
            pfree_ext(strif);
            add_subpartition_pruning_info_others(planstate, es);
        }
    }
}

static void ExplainNodePartition(const Plan* plan, ExplainState* es)
{
    int flag = 0;
    switch (nodeTag(plan->lefttree)) {
        case T_SeqScan:
            if (((SeqScan*)plan->lefttree)->pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
        case T_IndexScan:
            if (((IndexScan*)plan->lefttree)->scan.pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
        case T_IndexOnlyScan:
            if (((IndexOnlyScan*)plan->lefttree)->scan.pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
        case T_BitmapIndexScan:
            if (((BitmapIndexScan*)plan->lefttree)->scan.pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
        case T_BitmapHeapScan:
            if (((BitmapHeapScan*)plan->lefttree)->scan.pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
        case T_CStoreScan:
            if (((CStoreScan*)plan->lefttree)->pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            if (((TsStoreScan*)plan->lefttree)->pruningInfo->expr != NULL) {
                appendStringInfo(es->str, "Iterations: %s", "PART");
                flag = 1;
            }
            break;
#endif
        default:
            appendStringInfo(es->str, "Iterations: %d", ((PartIterator*)plan)->itrs);
            flag = 1;
            break;
    }
    if (flag == 0) {
        appendStringInfo(es->str, "Iterations: %d", ((PartIterator*)plan)->itrs);
    }
}

static bool GetSubPartitionIterations(const Plan* plan, const ExplainState* es, int* cnt)
{
    *cnt = 0;
    const Plan* curPlan = plan;
    switch (nodeTag(curPlan->lefttree)) {
        case T_RowToVec: {
            RowToVec* rowToVecPlan = (RowToVec*)curPlan->lefttree;
            Plan* scanPlan = (Plan*)rowToVecPlan->plan.lefttree;
            if (!(IsA(scanPlan, Scan) || IsA(scanPlan, SeqScan) || IsA(scanPlan, IndexOnlyScan) ||
                IsA(scanPlan, IndexScan) || IsA(scanPlan, BitmapHeapScan) || IsA(scanPlan, TidScan))) {
                break;
            }
            curPlan = &rowToVecPlan->plan;
            /* fallthrough */
        }
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapIndexScan:
        case T_BitmapHeapScan:
        case T_CStoreScan:
        case T_TidScan: {
            PruningResult* pr = ((Scan*)curPlan->lefttree)->pruningInfo;
            if (pr == NULL || pr->ls_selectedSubPartitions == NIL || pr->expr != NULL) {
                return false;
            }
            ListCell* lc = NULL;
            foreach (lc, pr->ls_selectedSubPartitions) {
                SubPartitionPruningResult* spr = (SubPartitionPruningResult*)lfirst(lc);
                *cnt += list_length(spr->ls_selectedSubPartitions);
            }
            return true;
        }
        default:
            return false;
    }
    return false; /* Syntactic sugar */
}

#ifndef ENABLE_MULTIPLE_NODES
static void PredAppendInfo(Plan* plan, StringInfoData buf, ExplainState* es)
{
    if (plan->pred_total_time >= 0) {
        initStringInfo(&buf);
        appendStringInfo(&buf, "%.0f", plan->pred_total_time);
        es->planinfo->m_planInfo->put(PREDICT_TIME, PointerGetDatum(cstring_to_text(buf.data)));
        pfree_ext(buf.data);
    }

if (plan->pred_rows >= 0) {
        es->planinfo->m_planInfo->put(PREDICT_ROWS, 
                                        DirectFunctionCall1(dround,
                                                            Float8GetDatum(plan->pred_rows)));
    }

if (plan->pred_max_memory >= 0) {
        es->planinfo->m_planInfo->put(PREDICT_MEMORY,
                                        DirectFunctionCall1(pg_size_pretty, 
                                                            Int64GetDatum(plan->pred_max_memory)));
    }
}

static void PredGetInfo(Plan* plan, ExplainState* es)
{
    if (plan->pred_startup_time >= 0) {
        ExplainPropertyFloat("Pred Startup Time", plan->pred_startup_time, 2, es);
    }
    if (plan->pred_total_time >= 0) {
        ExplainPropertyFloat("Pred Total Time", plan->pred_total_time, 2, es);
    }
    if (plan->pred_rows >= 0) {
        ExplainPropertyFloat("Pred Rows", plan->pred_rows, 0, es);
    }
    if (plan->pred_max_memory >= 0) {
        ExplainPropertyLong("Pred Peak Memory", plan->pred_max_memory, es);
    }
} 
#endif

/*
 * ExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries.  In non-text formats, es->indent
 * corresponds to the nesting depth of logical output groups, and therefore
 * is controlled by ExplainOpenGroup/ExplainCloseGroup.
 */
template <bool is_pretty>
static void ExplainNode(
    PlanState* planstate, List* ancestors, const char* relationship, const char* plan_name, ExplainState* es)
{
    Plan* plan = planstate->plan;
    char* pname = NULL; /* node type name for text output */
    char* sname = NULL; /* node type name for non-text output */
    char* strategy = NULL;
    char* operation = NULL;
    int save_indent = es->indent;
    int save_pindent = es->pindent;
    bool haschildren = false;
    int plan_node_id = planstate->plan->plan_node_id;
    int parentid = planstate->plan->parent_node_id;
    StringInfo tmpName;
    bool from_datanode = false;
    bool old_dn_flag = false;

    /* For plan_table column */
    char* pt_operation = NULL;
    char* pt_options = NULL;
    const char* pt_index_name = NULL;
    const char* pt_index_owner = NULL;

    if (is_pretty) {
        if (plan->plan_node_id == 0)
            goto runnext;
        tmpName = &es->planinfo->m_planInfo->m_pname;
        resetStringInfo(tmpName);
    }

    if (is_pretty) {
        es->planinfo->set_id(plan_node_id);
        es->planinfo->m_planInfo->put(PLANID, plan_node_id);

        if (es->planinfo->m_runtimeinfo) {
            es->planinfo->m_runtimeinfo->m_plan_node_id = plan_node_id;
            es->planinfo->m_runtimeinfo->put(-1, -1, PLAN_PARENT_ID, parentid);
            es->planinfo->m_runtimeinfo->put(-1, -1, QUERY_ID, es->planinfo->m_query_id);
            es->planinfo->m_runtimeinfo->put(-1, -1, PLAN_NODE_ID, plan_node_id);
            es->planinfo->m_runtimeinfo->put(-1, -1, PLAN_TYPE, nodeTag(plan));
            from_datanode = es->from_dn;
            es->planinfo->m_runtimeinfo->put(-1, -1, FROM_DATANODE, from_datanode);
        }

        if (es->analyze && (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery))) {
            show_dn_executor_time(es, plan_node_id, DN_START_TIME);  /* print executor start time */
            show_dn_executor_time(es, plan_node_id, DN_RUN_TIME); /* print executor run time */
            show_dn_executor_time(es, plan_node_id, DN_END_TIME); /* print executor end time */
        }
    }

    /* Fetch plan node's plain text info */
    GetPlanNodePlainText(plan, &pname, &sname, &strategy, &operation, &pt_operation, &pt_options);

    ExplainOpenGroup("Plan", relationship ? NULL : "Plan", true, es);

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        if (is_pretty) {
            appendStringInfoString(tmpName, pname);
        } else {
            if (es->wlm_statistics_plan_max_digit) {
                show_wlm_explain_name(es, plan_name, pname, plan_node_id);
            } else {
                if (plan_name != NULL) {
                    appendStringInfoSpaces(es->str, es->indent * 2);
                    appendStringInfo(es->str, "%s\n", plan_name);
                    es->indent++;
                }
                if (es->indent) {
                    appendStringInfoSpaces(es->str, es->indent * 2);
                    appendStringInfoString(es->str, "->  ");
                    es->indent += 2;
                }
                appendStringInfoString(es->str, pname);

                es->indent++;
            }
        }
    } else {
        ExplainPropertyText("Node Type", sname, es);
        if (strategy != NULL)
            ExplainPropertyText("Strategy", strategy, es);
        if (operation != NULL)
            ExplainPropertyText("Operation", operation, es);
        if (relationship != NULL)
            ExplainPropertyText("Parent Relationship", relationship, es);
        if (plan_name != NULL)
            ExplainPropertyText("Subplan Name", plan_name, es);
    }

    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_DfsScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan:
        case T_SubqueryScan:
        case T_VecSubqueryScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_ForeignScan:
        case T_VecForeignScan:
            ExplainScanTarget((Scan*)plan, es);
            break;
        case T_TrainModel:
            appendStringInfo(es->str, " - %s", sname);
            break;
        case T_ExtensiblePlan:
            if (((Scan*)plan)->scanrelid > 0)
                ExplainScanTarget((Scan*)plan, es);
            break;
#ifdef PGXC
        case T_RemoteQuery:
        case T_VecRemoteQuery:
            /* Emit node execution list */
            ExplainExecNodes(((RemoteQuery*)plan)->exec_nodes, es);
#ifdef STREAMPLAN
            if (!IS_STREAM)
#endif
                ExplainScanTarget((Scan*)plan, es);
            break;
#endif
        case T_IndexScan: {
            IndexScan* indexscan = (IndexScan*)plan;

            ExplainIndexScanDetails(indexscan->indexid, indexscan->indexorderdir, es);
            ExplainScanTarget((Scan*)indexscan, es);

            pt_index_name = explain_get_index_name(indexscan->indexid);
            pt_index_owner = get_namespace_name(get_rel_namespace(indexscan->indexid));
        } break;
        case T_IndexOnlyScan: {
            IndexOnlyScan* indexonlyscan = (IndexOnlyScan*)plan;

            ExplainIndexScanDetails(indexonlyscan->indexid, indexonlyscan->indexorderdir, es);
            ExplainScanTarget((Scan*)indexonlyscan, es);

            pt_index_name = explain_get_index_name(indexonlyscan->indexid);
            pt_index_owner = get_namespace_name(get_rel_namespace(indexonlyscan->indexid));
        } break;
        case T_BitmapIndexScan: {
            BitmapIndexScan* bitmapindexscan = (BitmapIndexScan*)plan;
            const char* indexname = explain_get_index_name(bitmapindexscan->indexid);

            if (es->format == EXPLAIN_FORMAT_TEXT) {
                appendStringInfo(es->str, " on %s", indexname);
                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
                    StringInfo tmpName = &es->planinfo->m_planInfo->m_pname;
                    appendStringInfo(tmpName, " using %s", indexname);
                }
            } else {
                ExplainPropertyText("Index Name", indexname, es);
            }

            pt_index_name = indexname;
            pt_index_owner = get_namespace_name(get_rel_namespace(bitmapindexscan->indexid));
        } break;
        case T_DfsIndexScan: {
            DfsIndexScan* indexscan = (DfsIndexScan*)plan;

            ExplainIndexScanDetails(indexscan->indexid, indexscan->indexorderdir, es);
            ExplainScanTarget((Scan*)indexscan, es);

            pt_index_name = explain_get_index_name(indexscan->indexid);
            pt_index_owner = get_namespace_name(get_rel_namespace(indexscan->indexid));
        } break;
        case T_CStoreIndexScan: {
            CStoreIndexScan* indexscan = (CStoreIndexScan*)plan;

            ExplainIndexScanDetails(indexscan->indexid, indexscan->indexorderdir, es);
            ExplainScanTarget((Scan*)indexscan, es);

            pt_index_name = explain_get_index_name(indexscan->indexid);
            pt_index_owner = get_namespace_name(get_rel_namespace(indexscan->indexid));
        } break;
        case T_CStoreIndexCtidScan: {
            CStoreIndexCtidScan* bitmapindexscan = (CStoreIndexCtidScan*)plan;
            const char* indexname = explain_get_index_name(bitmapindexscan->indexid);

            if (es->format == EXPLAIN_FORMAT_TEXT)
                appendStringInfo(es->str, " on %s", indexname);
            else
                ExplainPropertyText("Index Name", indexname, es);

            pt_index_name = indexname;
            pt_index_owner = get_namespace_name(get_rel_namespace(bitmapindexscan->indexid));
        } break;
        case T_ModifyTable:
        case T_VecModifyTable:
            ExplainModifyTarget((ModifyTable*)plan, es);
            break;
        case T_NestLoop:
        case T_VecNestLoop:
        case T_VecMergeJoin:
        case T_MergeJoin:
        case T_HashJoin:
        case T_VecHashJoin: {
            const char* jointype = NULL;

            switch (((Join*)plan)->jointype) {
                case JOIN_INNER:
                    jointype = pt_options = "Inner";
                    break;
                case JOIN_LEFT:
                    jointype = pt_options = "Left";
                    break;
                case JOIN_FULL:
                    jointype = pt_options = "Full";
                    break;
                case JOIN_RIGHT:
                    jointype = pt_options = "Right";
                    break;
                case JOIN_SEMI:
                    jointype = pt_options = "Semi";
                    break;
                case JOIN_ANTI:
                    jointype = pt_options = "Anti";
                    break;
                case JOIN_RIGHT_SEMI:
                    jointype = pt_options = "Right Semi";
                    break;
                case JOIN_RIGHT_ANTI:
                    jointype = pt_options = "Right Anti";
                    break;
                case JOIN_LEFT_ANTI_FULL:
                    jointype = pt_options = "Left Anti Full";
                    break;
                case JOIN_RIGHT_ANTI_FULL:
                    jointype = pt_options = "Right Anti Full";
                    break;
                default:
                    jointype = pt_options = "?\?\?";
                    break;
            }

            /* In order to adapt join option 'CARTESIAN' in A db, we reset 'options' here. */
            if (es->plan && es->planinfo != NULL)
                es->planinfo->m_planTableData->set_plan_table_join_options(plan, &pt_options);

            if (es->format == EXPLAIN_FORMAT_TEXT) {
                /*
                 * For historical reasons, the join type is interpolated
                 * into the node type name...
                 */
                if (is_pretty == false) {
                    if (((Join*)plan)->jointype != JOIN_INNER) {
                        appendStringInfo(es->str, " %s Join", jointype);
                    } else if (!(IsA(plan, NestLoop) || IsA(plan, VecNestLoop))) {
                        appendStringInfo(es->str, " Join");
                    }

                } else {
                    if (((Join*)plan)->jointype != JOIN_INNER) {
                        appendStringInfo(tmpName,
                            " %s Join (%d, %d)",
                            jointype,
                            planstate->lefttree->plan->plan_node_id,
                            planstate->righttree->plan->plan_node_id);
                    } else if (!(IsA(plan, NestLoop) || IsA(plan, VecNestLoop))) {
                        appendStringInfo(tmpName,
                            " Join (%d,%d)",
                            planstate->lefttree->plan->plan_node_id,
                            planstate->righttree->plan->plan_node_id);
                    } else {
                        appendStringInfo(tmpName,
                            " (%d,%d)",
                            planstate->lefttree->plan->plan_node_id,
                            planstate->righttree->plan->plan_node_id);
                    }
                }
            } else
                ExplainPropertyText("Join Type", jointype, es);
        } break;
        case T_SetOp:
        case T_VecSetOp: {
            const char* setopcmd = NULL;

            switch (((SetOp*)plan)->cmd) {
                case SETOPCMD_INTERSECT:
                    setopcmd = "Intersect";
                    break;
                case SETOPCMD_INTERSECT_ALL:
                    setopcmd = "Intersect All";
                    break;
                case SETOPCMD_EXCEPT:
                    setopcmd = "Except";
                    break;
                case SETOPCMD_EXCEPT_ALL:
                    setopcmd = "Except All";
                    break;
                default:
                    setopcmd = "?\?\?";
                    break;
            }
            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (is_pretty == false)
                    appendStringInfo(es->str, " %s", setopcmd);
                else
                    appendStringInfo(tmpName, " %s", setopcmd);
            } else
                ExplainPropertyText("Command", setopcmd, es);
        } break;

        case T_PartIterator: {
            const char* scandir = NULL;

            if (ScanDirectionIsBackward(((PartIterator*)plan)->direction))
                scandir = "Scan Backward";

            if (scandir != NULL) {
                if (es->format == EXPLAIN_FORMAT_TEXT) {
                    if (is_pretty == false) {
                        appendStringInfo(es->str, " %s", scandir);
                    } else {
                        appendStringInfo(tmpName, " %s", scandir);
                    }
                } else {
                    ExplainPropertyText("Scan Direction", scandir, es);
                }
            }
        } break;
        case T_Append:
        case T_VecAppend: {
            if (is_pretty) {
                /* print append plan's subplan node id, like Vector Append(6, 7) */
                int nplans = list_length(((Append*)plan)->appendplans);
                PlanState** append_planstate = ((AppendState*)planstate)->appendplans;
                bool first = true;

                appendStringInfo(tmpName, "(");

                for (int j = 0; j < nplans; j++) {
                    if (first == false)
                        appendStringInfoString(tmpName, ", ");
                    appendStringInfo(tmpName, "%d", append_planstate[j]->plan->plan_node_id);

                    first = false;
                }
                appendStringInfo(tmpName, ")");
            }
        } break;
        case T_RecursiveUnion: {
            if (es->format == EXPLAIN_FORMAT_TEXT && is_pretty)
                appendStringInfo(tmpName,
                    " (%d,%d)",
                    planstate->lefttree->plan->plan_node_id,
                    planstate->righttree->plan->plan_node_id);
        } break;
        default:
            break;
    }

    /* For recursive query consideration */
    if (GET_CONTROL_PLAN_NODEID(plan) != 0 && u_sess->attr.attr_sql.enable_stream_recursive) {
        appendStringInfo(
            es->str, " <<ruid:[%d] ctlid:[%d]", GET_RECURSIVE_UNION_PLAN_NODEID(plan), GET_CONTROL_PLAN_NODEID(plan));
        if (is_pretty)
            appendStringInfo(tmpName,
                " <<ruid:[%d] ctlid:[%d]",
                GET_RECURSIVE_UNION_PLAN_NODEID(plan),
                GET_CONTROL_PLAN_NODEID(plan));

        if (plan->is_sync_plannode) {
            appendStringInfo(es->str, " (SYNC)");
            if (is_pretty)
                appendStringInfo(tmpName, " (SYNC)");
        }

        appendStringInfo(es->str, ">>");
        if (is_pretty)
            appendStringInfo(tmpName, ">>");
    }

    if (u_sess->attr.attr_sql.enable_stream_recursive && IsA(plan, Stream) && ((Stream*)plan)->stream_level > 0 &&
        plan->recursive_union_plan_nodeid != 0) {
        Stream* stream_plan = (Stream*)plan;

        appendStringInfo(es->str, " stream_level:%d ", stream_plan->stream_level);
        if (is_pretty)
            appendStringInfo(tmpName, " stream_level:%d ", stream_plan->stream_level);
    }

    if (is_pretty) {

        StringInfoData pretty_plan_name;
        initStringInfo(&pretty_plan_name);
        appendStringInfoSpaces(&pretty_plan_name, es->pindent);
        appendStringInfoString(&pretty_plan_name, "->  ");
        es->pindent += 2;
        appendStringInfoString(&pretty_plan_name, es->planinfo->m_planInfo->m_pname.data);

        if (plan_name != NULL) {
            appendStringInfoSpaces(&pretty_plan_name, 2);
            appendStringInfo(&pretty_plan_name, "[%d, %s]", plan->parent_node_id, plan_name);
        }

        es->planinfo->m_planInfo->put(PLAN, PointerGetDatum(cstring_to_text(pretty_plan_name.data)));
        pfree_ext(pretty_plan_name.data);

        es->pindent++;

        es->planinfo->append_str_info("%3d --%s\n", plan_node_id, es->planinfo->m_planInfo->m_pname.data);
        es->planinfo->set_pname(es->planinfo->m_planInfo->m_pname.data);
        if (es->planinfo->m_runtimeinfo)
            es->planinfo->m_runtimeinfo->put(
                -1, -1, PLAN_NAME, PointerGetDatum(cstring_to_text(es->planinfo->m_planInfo->m_pname.data)));
    }

    if (es->costs) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (is_pretty == false) {
                appendStringInfo(
                    es->str, "  (cost=%.2f..%.2f rows=%.0f ", plan->startup_cost, plan->total_cost, plan->plan_rows);
#ifndef ENABLE_MULTIPLE_NODES
                if (plan->pred_total_time >= 0) {
                    appendStringInfo(es->str, "p-time=%.0f ", plan->pred_total_time);
                }
                if (plan->pred_rows >= 0) {
                    appendStringInfo(es->str, "p-rows=%.0f ", plan->pred_rows);
                }
#endif /* ENABLE_MULTIPLE_NODES */
                if ((IsA(plan, HashJoin) || IsA(plan, VecHashJoin)) && es->verbose)
                    appendStringInfo(es->str,
                        "distinct=[%.0f, %.0f] ",
                        plan->outerdistinct > 0 ? ceil(plan->outerdistinct) : 1.0,
                        plan->innerdistinct > 0 ? ceil(plan->innerdistinct) : 1.0);
                appendStringInfo(es->str, "width=%d)", plan->plan_width);
            } else {
                StringInfoData buf;
                initStringInfo(&buf);
                appendStringInfo(&buf, "%.2f", plan->total_cost);
                es->planinfo->m_planInfo->put(ESTIMATE_COSTS, PointerGetDatum(cstring_to_text(buf.data)));
                pfree_ext(buf.data);
#ifndef ENABLE_MULTIPLE_NODES
                PredAppendInfo(plan, buf, es);
#endif
                /* Here we need call round function, avoid E_rows appear decimal fraction.*/
                es->planinfo->m_planInfo->put(
                    ESTIMATE_ROWS, DirectFunctionCall1(dround, Float8GetDatum(plan->plan_rows)));
                if ((IsA(plan, HashJoin) || IsA(plan, VecHashJoin)) && es->verbose) {
                    char opdist[130] = "\0";
                    int rc = EOK;
                    rc = sprintf_s(opdist,
                        sizeof(opdist),
                        "%.0f, %.0f",
                        plan->outerdistinct > 1 ? ceil(plan->outerdistinct) : 1.0,
                        plan->innerdistinct > 1 ? ceil(plan->innerdistinct) : 1.0);
                    securec_check_ss(rc, "\0", "\0");
                    es->planinfo->m_planInfo->put(ESTIMATE_DISTINCT, PointerGetDatum(cstring_to_text(opdist)));
                }
                es->planinfo->m_planInfo->put(ESTIMATE_WIDTH, plan->plan_width);

                if (es->planinfo->m_planInfo->m_query_mem_mode) {
                    char memstr[50] = "\0";
                    if (plan->operatorMemKB[0] > 0) {
                        int rc = 0;
                        if (plan->operatorMemKB[0] < 1024)
                            rc = sprintf_s(memstr, sizeof(memstr), "%dKB", (int)plan->operatorMemKB[0]);
                        else {
                            if (plan->operatorMemKB[0] > MIN_OP_MEM && plan->operatorMaxMem > plan->operatorMemKB[0])
                                rc = sprintf_s(memstr,
                                    sizeof(memstr),
                                    "%dMB(%dMB)",
                                    (int)plan->operatorMemKB[0] / 1024,
                                    (int)plan->operatorMaxMem / 1024);
                            else
                                rc = sprintf_s(memstr, sizeof(memstr), "%dMB", (int)plan->operatorMemKB[0] / 1024);
                        }
                        securec_check_ss(rc, "\0", "\0");
                    }
                    es->planinfo->m_planInfo->put(ESTIMATE_MEMORY, PointerGetDatum(cstring_to_text(memstr)));
                }
            }
        } else {
            ExplainPropertyFloat("Startup Cost", plan->startup_cost, 2, es);
            ExplainPropertyFloat("Total Cost", plan->total_cost, 2, es);
            ExplainPropertyFloat("Plan Rows", plan->plan_rows, 0, es);
            ExplainPropertyInteger("Plan Width", plan->plan_width, es);
#ifndef ENABLE_MULTIPLE_NODES
            PredGetInfo(plan, es);
#endif
        }
    }

    /*
     * We have to forcibly clean up the instrumentation state because we
     * haven't done ExecutorEnd yet.  This is pretty grotty ...
     */
    if (planstate->instrument)
        InstrEndLoop(planstate->instrument);

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        show_datanode_time(es, planstate);
        show_stream_send_time(es, planstate);
    } else {
        if (is_pretty) {
            if (!es->from_dn) {
                show_pretty_time(es, planstate->instrument, NULL, -1, -1, 0);
                if (es->planinfo->m_runtimeinfo != NULL) {
                    if (unlikely(planstate->instrument == NULL)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid planstate->instrument value.")));
                    }
                    get_oper_time<false>(es, planstate, planstate->instrument, -1, -1);
                }
            }
        } else {
            show_time<false>(es, planstate->instrument, -1);
        }
    }

    /* target list */
    if (es->verbose || es->plan)
        show_plan_tlist(planstate, ancestors, es);

    /* Show exec nodes of plan nodes when it's not a single installation group scenario */
    if (es->nodes && es->verbose && ng_enable_nodegroup_explain()) {
        show_plan_execnodes(planstate, es);
    }

    /* quals, sort keys, etc */
    switch (nodeTag(plan)) {
        case T_IndexScan:
            show_scan_qual(((IndexScan*)plan)->indexqualorig, "Index Cond", planstate, ancestors, es);
            if (((IndexScan*)plan)->indexqualorig)
                show_instrumentation_count("Rows Removed by Index Recheck", 2, planstate, es);
            show_scan_qual(((IndexScan*)plan)->indexorderbyorig, "Order By", planstate, ancestors, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            break;
        case T_IndexOnlyScan:
            show_scan_qual(((IndexOnlyScan*)plan)->indexqual, "Index Cond", planstate, ancestors, es);
            if (((IndexOnlyScan*)plan)->indexqual)
                show_instrumentation_count("Rows Removed by Index Recheck", 2, planstate, es);
            show_scan_qual(((IndexOnlyScan*)plan)->indexorderby, "Order By", planstate, ancestors, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            if (es->analyze)
                ExplainPropertyLong("Heap Fetches", ((IndexOnlyScanState*)planstate)->ioss_HeapFetches, es);
            break;
        case T_BitmapIndexScan:
            show_scan_qual(((BitmapIndexScan*)plan)->indexqualorig, "Index Cond", planstate, ancestors, es);
            break;
        case T_CStoreIndexCtidScan:
            show_scan_qual(((CStoreIndexCtidScan*)plan)->indexqualorig, "Index Cond", planstate, ancestors, es);
            break;
        case T_CStoreIndexScan:
            show_scan_qual(((CStoreIndexScan*)plan)->indexqualorig, "Index Cond", planstate, ancestors, es);
            if (((CStoreIndexScan*)plan)->indexqualorig)
                show_instrumentation_count("Rows Removed by Index Recheck", 2, planstate, es);
            show_scan_qual(((CStoreIndexScan*)plan)->indexorderbyorig, "Order By", planstate, ancestors, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            show_llvm_info(planstate, es);
            break;
        case T_DfsIndexScan:
            show_scan_qual(((DfsIndexScan*)plan)->indexqualorig, "Index Cond", planstate, ancestors, es);
            if (((DfsIndexScan*)plan)->indexqualorig)
                show_instrumentation_count("Rows Removed by Index Recheck", 2, planstate, es);
            show_scan_qual(((DfsIndexScan*)plan)->indexorderbyorig, "Order By", planstate, ancestors, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            show_pushdown_qual(planstate, ancestors, es, PUSHDOWN_PREDICATE_FLAG);
            show_llvm_info(planstate, es);
            break;

#ifdef PGXC
        case T_ModifyTable:
        case T_VecModifyTable: {
            /* Remote query planning on DMLs */
            ModifyTable* mt = (ModifyTable*)plan;
            ListCell* elt = NULL;
            ListCell* lc = NULL;
            if (mt->operation == CMD_MERGE) {
                if (es->analyze) {
                    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL
                        && es->planinfo->m_detailInfo != NULL) {
                        es->planinfo->m_detailInfo->set_plan_name<true, false>();
                    }

                    show_instrumentation_count("Merge Inserted", 1, planstate, es);
                    show_instrumentation_count("Merge Updated", 2, planstate, es);
                    show_instrumentation_count("Merge Deleted", 3, planstate, es);
                }
                show_modifytable_merge_info(planstate, es);

                foreach (lc, mt->mergeActionList) {
                    MergeAction* action = (MergeAction*)lfirst(lc);

                    if (action->matched) {
                        if (mt->remote_update_plans) {
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str, "WHEN MATCHED\n");
                            es->indent++;
                            foreach (elt, mt->remote_update_plans) {
                                if (lfirst(elt)) {
                                    ExplainRemoteQuery((RemoteQuery*)lfirst(elt), planstate, ancestors, es);
                                }
                            }
                            es->indent--;
                        } else
                            show_scan_qual((List*)action->qual, "Update Cond", planstate, ancestors, es, true);
                    } else if (!action->matched) {
                        if (mt->remote_insert_plans) {
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str, "WHEN NOT MATCHED\n");
                            es->indent++;
                            foreach (elt, mt->remote_insert_plans) {
                                if (lfirst(elt)) {
                                    ExplainRemoteQuery((RemoteQuery*)lfirst(elt), planstate, ancestors, es);
                                }
                            }
                            es->indent--;
                        } else
                            show_scan_qual((List*)action->qual, "Insert Cond", planstate, ancestors, es, true);
                    }
                }

            } else {
                /* upsert cases */
                ModifyTableState* mtstate = (ModifyTableState*)planstate;
                if (mtstate->mt_upsert != NULL && 
                    mtstate->mt_upsert->us_action != UPSERT_NONE && mtstate->resultRelInfo->ri_NumIndices > 0) {
                    show_on_duplicate_info(mtstate, es, ancestors);
                }
                /* non-merge cases */
                foreach (elt, mt->remote_plans) {
                    if (lfirst(elt)) {
                        ExplainRemoteQuery((RemoteQuery*)lfirst(elt), planstate, ancestors, es);
                    }
                }
            }
        } break;
        case T_RemoteQuery:
        case T_VecRemoteQuery:
            /* Remote query */
            show_merge_sort_keys(planstate, ancestors, es);
            ExplainRemoteQuery((RemoteQuery*)plan, planstate, ancestors, es);
            show_scan_qual(plan->qual, "Coordinator quals", planstate, ancestors, es);
            break;
#else
        case T_ModifyTable:
            show_modifytable_info((ModifyTableState*)planstate, es);
            break;
#endif
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
            show_scan_qual(((BitmapHeapScan*)plan)->bitmapqualorig, "Recheck Cond", planstate, ancestors, es);
            if (((BitmapHeapScan*)plan)->bitmapqualorig)
                show_instrumentation_count("Rows Removed by Index Recheck", 2, planstate, es);

            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            if (nodeTag(plan) == T_BitmapHeapScan && es->analyze) {
                show_tidbitmap_info((BitmapHeapScanState*)planstate, es);
            }
            show_llvm_info(planstate, es);
            break;
        case T_StartWithOp:
            show_startwith_pseudo_entries(planstate, ancestors, es);
            show_startwith_dfx((StartWithOpState*)planstate, es);
            break;
        case T_SeqScan:
            show_tablesample(plan, planstate, ancestors, es);
            if (!((SeqScan*)plan)->scanBatchMode) {
                show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
                if (plan->qual) {
                    show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
                }
            }
            break;

        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            show_tablesample(plan, planstate, ancestors, es);

            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            show_llvm_info(planstate, es);
            break;
        case T_DfsScan: {
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            show_pushdown_qual(planstate, ancestors, es, PUSHDOWN_PREDICATE_FLAG);
            show_bloomfilter<false>(plan, planstate, ancestors, es);

            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            show_storage_filter_info(planstate, es);
            show_dfs_block_info(planstate, es);
            show_llvm_info(planstate, es);
            break;
        }
            /* FALL THRU */
        case T_Stream:
        case T_VecStream: {
            show_merge_sort_keys(planstate, ancestors, es);
            ShowStreamRunNodeInfo((Stream*)plan, es);
            if (is_pretty && es->planinfo->m_query_summary) {
                showStreamnetwork((Stream*)plan, es);
            }
        } break;
        case T_FunctionScan:
            if (es->verbose)
                show_expression(
                    ((FunctionScan*)plan)->funcexpr, "Function Call", planstate, ancestors, es->verbose, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            break;
        case T_TidScan: {
            /*
             * The tidquals list has OR semantics, so be sure to show it
             * as an OR condition.
             */
            List* tidquals = ((TidScan*)plan)->tidquals;

            if (list_length(tidquals) > 1)
                tidquals = list_make1(make_orclause(tidquals));
            show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
        } break;
        case T_ForeignScan:
        case T_VecForeignScan: {
            ForeignScan* fScan = (ForeignScan*)plan;
            /*
             * show_predicate_flag represents the label of non-pushdown
             * predicate restriction clause on foreign scan level.
             */
            char* show_predicate_flag = NULL;
            /*
             * show_pushdown_flag represents the label of pushdown predicate
             * restriction clause on ORC reader level.
             */
            char* show_pushdown_flag = NULL;
            if (fScan->scan.scan_qual_optimized) {
                show_predicate_flag = FILTER_INFORMATIONAL_CONSTRAINT_FLAG;
            } else {
                show_predicate_flag = FILTER_FLAG;
            }

            if (fScan->scan.predicate_pushdown_optimized) {
                show_pushdown_flag = PUSHDOWN_FILTER_INFORMATIONAL_CONSTRAINT_FLAG;
            } else {
                show_pushdown_flag = PUSHDOWN_PREDICATE_FLAG;
            }

            show_scan_qual(plan->qual, show_predicate_flag, planstate, ancestors, es);
            show_pushdown_qual(planstate, ancestors, es, show_pushdown_flag);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            if (es->wlm_statistics_plan_max_digit == NULL) {
                show_foreignscan_info((ForeignScanState*)planstate, es);
            }
            show_storage_filter_info(planstate, es);
            show_dfs_block_info(planstate, es);
            show_llvm_info(planstate, es);

            if (IsA(plan, VecForeignScan)) {
                show_bloomfilter<false>(plan, planstate, ancestors, es);
            }
        } break;
        case T_VecNestLoop:
        case T_NestLoop: {
            NestLoop* nestLoop = (NestLoop*)plan;
            if (nestLoop->join.optimizable) {
                show_upper_qual(((NestLoop*)plan)->join.joinqual,
                    "Join Filter(Informational Constraint Optimization)",
                    planstate,
                    ancestors,
                    es);
            } else {
                show_upper_qual(((NestLoop*)plan)->join.joinqual, "Join Filter", planstate, ancestors, es);
            }
            if (((NestLoop*)plan)->join.joinqual)
                show_instrumentation_count("Rows Removed by Join Filter", 1, planstate, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 2, planstate, es);
            show_llvm_info(planstate, es);
            show_skew_optimization(planstate, es);
        } break;
        case T_VecMergeJoin:
        case T_MergeJoin: {
            MergeJoin* mergeJoin = (MergeJoin*)plan;
            if (mergeJoin->join.optimizable) {
                show_upper_qual(((MergeJoin*)plan)->mergeclauses,
                    "Merge Cond(Informational Constraint Optimization)",
                    planstate,
                    ancestors,
                    es);
            } else {
                show_upper_qual(((MergeJoin*)plan)->mergeclauses, "Merge Cond", planstate, ancestors, es);
            }
            show_upper_qual(((MergeJoin*)plan)->join.joinqual, "Join Filter", planstate, ancestors, es);
            if (((MergeJoin*)plan)->join.joinqual)
                show_instrumentation_count("Rows Removed by Join Filter", 1, planstate, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 2, planstate, es);
            show_llvm_info(planstate, es);
            show_skew_optimization(planstate, es);
        } break;
        case T_HashJoin: {
            show_upper_qual(((HashJoin*)plan)->hashclauses, "Hash Cond", planstate, ancestors, es);
            show_upper_qual(((HashJoin*)plan)->join.joinqual, "Join Filter", planstate, ancestors, es);
            if (((HashJoin*)plan)->join.joinqual)
                show_instrumentation_count("Rows Removed by Join Filter", 1, planstate, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 2, planstate, es);
            show_skew_optimization(planstate, es);
        } break;
        case T_VecHashJoin: {
            show_upper_qual(((HashJoin*)plan)->hashclauses, "Hash Cond", planstate, ancestors, es);
            show_upper_qual(((HashJoin*)plan)->join.joinqual, "Join Filter", planstate, ancestors, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            show_llvm_info(planstate, es);

            show_bloomfilter<true>(plan, planstate, ancestors, es);
            show_skew_optimization(planstate, es);
        }
            show_vechash_info((VecHashJoinState*)planstate, es);
            break;
        case T_VecAgg:
        case T_Agg:
            show_groupby_keys((AggState*)planstate, ancestors, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            switch (((Agg*)plan)->aggstrategy) {
                case AGG_HASHED: {
                    show_hashAgg_info((AggState*)planstate, es);
                    show_llvm_info(planstate, es);
                } break;
                case AGG_SORTED: {
                    show_llvm_info(planstate, es);
                } break;
                default:
                    break;
            }
            show_skew_optimization(planstate, es);
            show_unique_check_info(planstate, es);
            break;
        case T_Group:
        case T_VecGroup:
            show_group_keys((GroupState*)planstate, ancestors, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            break;
        case T_Sort:
        case T_VecSort:
            show_sort_keys((SortState*)planstate, ancestors, es);
            show_sort_info((SortState*)planstate, es);
            show_llvm_info(planstate, es);
            break;
        case T_MergeAppend:
            show_merge_append_keys((MergeAppendState*)planstate, ancestors, es);
            break;
        case T_BaseResult:
        case T_VecResult:
            show_upper_qual((List*)((BaseResult*)plan)->resconstantqual, "One-Time Filter", planstate, ancestors, es);
            show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual)
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            break;
        case T_Hash:
            show_hash_info((HashState*)planstate, es);
            break;
        case T_SetOp:
        case T_VecSetOp:
            switch (((SetOp*)plan)->strategy) {
                case SETOP_HASHED:
                    show_setop_info((SetOpState*)planstate, es);
                    break;
                default:
                    break;
            }
            break;
        case T_ExtensiblePlan:
            show_pushdown_qual(planstate, ancestors, es, PUSHDOWN_PREDICATE_FLAG);
            break;
        case T_RecursiveUnion:
            show_recursive_info((RecursiveUnionState*)planstate, es);
            break;
        case T_RowToVec:
            if (IsA(plan->lefttree, SeqScan) && ((SeqScan*)plan->lefttree)->scanBatchMode) {
                show_scan_qual(plan->lefttree->qual, "Filter", planstate, ancestors, es);
                if (plan->lefttree->qual) {
                    show_instrumentation_count("Rows Removed by Filter", 1, planstate->lefttree, es);
                }
            }
            break;

        default:
            break;
    }
    switch (nodeTag(plan)) {
        case T_VecNestLoop:
        case T_NestLoop:
        case T_VecMergeJoin:
        case T_MergeJoin:
        case T_Group:
        case T_VecGroup:
        case T_BaseResult:
        case T_VecResult:
        case T_Append:
        case T_Unique:
        case T_VecUnique:
        case T_Limit:
        case T_PartIterator:
        case T_VecPartIterator:
        case T_VecToRow:
        case T_RowToVec:
        case T_VecAppend:
        case T_VecLimit:
        case T_MergeAppend:
        case T_SubqueryScan:
        case T_VecSubqueryScan:
        case T_ValuesScan:
            break;
        default: {
            /* Show buffer usage */
            if (es->buffers) {
                if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
                    u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id))
                    show_datanode_buffers(es, planstate);
                else if (!es->from_dn) {
                    if (is_pretty && es->planinfo->m_IOInfo)
                        show_buffers(es, es->planinfo->m_IOInfo->info_str, planstate->instrument, false, -1, -1, NULL);
                    else
                        show_buffers(es, es->str, planstate->instrument, false, -1, -1, NULL);
                }
            }
        } break;
    }
    /* if 'cpu' is specified, display the cpu cost */
    if (es->cpu) {
        uint64 proRows = 0;
        double incCycles = 0.0;
        double exCycles = 0.0;
        double outerCycles = 0.0;
        double innerCycles = 0.0;
        uint64 outterRows = 0;
        uint64 innerRows = 0;

        if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
            u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
            Instrumentation* instr = NULL;
            if (es->detail) {
                ExplainOpenGroup("Cpus In Detail", "Cpus In Detail", false, es);
                int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
                for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
                    char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                    char* node_name = g_instance.exec_cxt.nodeName;
#endif
                    for (int j = 0; j < dop; j++) {
                        outerCycles = 0.0;
                        innerCycles = 0.0;
                        ExplainOpenGroup("Plan", NULL, true, es);
                        instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                        if (instr == NULL)
                            continue;
                        append_datanode_name(es, node_name, dop, j);

                        show_child_cpu_cycles_and_rows<true>(
                            planstate, i, j, &outerCycles, &innerCycles, &outterRows, &innerRows);
                        if (es->planinfo != NULL && es->planinfo->m_runtimeinfo != NULL && instr != NULL &&
                            instr->nloops > 0) {
                            es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                            es->planinfo->m_datanodeInfo->set_datanode_name(node_name, j, dop);
                        }

                        proRows = (long)(instr->ntuples);

                        CalculateProcessedRows(planstate, i, j, &innerRows, &outterRows, &proRows);

                        show_cpu(es, instr, innerCycles, outerCycles, i, j, proRows);
                        ExplainCloseGroup("Plan", NULL, true, es);
                    }
                }
                ExplainCloseGroup("Cpus In Detail", "Cpus In Detail", false, es);
            } else
                show_detail_cpu(es, planstate);
        } else if (!es->from_dn) {
            const CPUUsage* usage = &planstate->instrument->cpuusage;

            show_child_cpu_cycles_and_rows<false>(planstate, 0, 0, &outerCycles, &innerCycles, &outterRows, &innerRows);

            incCycles = usage->m_cycles;
            exCycles = incCycles - outerCycles - innerCycles;

            proRows = (long)(planstate->instrument->ntuples);

            CalculateProcessedRows(planstate, 0, 0, &innerRows, &outterRows, &proRows);

            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (is_pretty) {
                    int64 ex_cyc_rows = proRows != 0 ? (long)(exCycles / proRows) : 0;
                    if (es->planinfo->m_runtimeinfo) {
                        es->planinfo->m_runtimeinfo->put(-1, -1, EX_CYC, Int64GetDatum(exCycles));
                        es->planinfo->m_runtimeinfo->put(-1, -1, INC_CYC, Int64GetDatum(incCycles));
                        es->planinfo->m_runtimeinfo->put(-1, -1, EX_CYC_PER_ROWS, Int64GetDatum(ex_cyc_rows));
                        appendStringInfoSpaces(es->planinfo->m_datanodeInfo->info_str, 8);
                        show_cpu_info(es->planinfo->m_datanodeInfo->info_str, incCycles, exCycles, proRows);
                    }
                    if (es->planinfo->m_IOInfo) {
                        es->planinfo->m_IOInfo->set_plan_name<true, true>();

                        show_cpu_info(es->planinfo->m_IOInfo->info_str, incCycles, exCycles, proRows);
                    }
                } else {
                    appendStringInfoSpaces(es->str, es->indent * 2);
                    show_cpu_info(es->str, incCycles, exCycles, proRows);
                }
            } else {
                ExplainPropertyLong("Exclusive Cycles Per Row", proRows != 0 ? (long)(exCycles / proRows) : 0, es);
                ExplainPropertyLong("Exclusive Cycles", (long)exCycles, es);
                ExplainPropertyLong("Inclusive Cycles", (long)incCycles, es);
            }
        }
    }

    /* in text format, partition line start here */
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_BitmapIndexScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan: {
            if (((Scan*)plan)->isPartTbl) {
                show_pruning_info(planstate, es, is_pretty);
            }

            if (es->verbose && ((Scan*)plan)->bucketInfo != NULL) {
                show_bucket_info(planstate, es, is_pretty);
            }
        } break;
        case T_PartIterator:
        case T_VecPartIterator:
            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (is_pretty == false) {
                    if (es->wlm_statistics_plan_max_digit) {
                        appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
                        appendStringInfoString(es->str, " | ");
                        appendStringInfoSpaces(es->str, es->indent);
                    } else {
                        appendStringInfoSpaces(es->str, es->indent * 2);
                    }
                    ExplainNodePartition(plan, es);
                    int subPartCnt = 0;
                    if (GetSubPartitionIterations(plan, es, &subPartCnt)) {
                        appendStringInfo(es->str, ", Sub Iterations: %d", subPartCnt);
                    }
                    appendStringInfoChar(es->str, '\n');

                } else {

                    es->planinfo->m_detailInfo->set_plan_name<true, true>();
                    appendStringInfo(
                        es->planinfo->m_detailInfo->info_str, "Iterations: %d", ((PartIterator*)plan)->itrs);
                    appendStringInfoChar(es->planinfo->m_detailInfo->info_str, '\n');
                }
            } else {
                ExplainPropertyInteger("Iterations", ((PartIterator*)plan)->itrs, es);
                int subPartCnt = 0;
                if (GetSubPartitionIterations(plan, es, &subPartCnt)) {
                    ExplainPropertyInteger("Sub Iterations", subPartCnt, es);
                }
            }
            break;

        default:
            break;
    }

runnext:

    /* Get ready to display the child plans */
    haschildren = planstate->initPlan || outerPlanState(planstate) || innerPlanState(planstate) ||
                  IsA(plan, ModifyTable) || IsA(plan, VecModifyTable) || IsA(plan, Append) || IsA(plan, VecAppend) ||
                  IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend) || IsA(plan, BitmapAnd) || IsA(plan, BitmapOr) ||
                  IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan) ||
                  (IsA(planstate, ExtensiblePlanState) && ((ExtensiblePlanState*)planstate)->extensible_ps != NIL) ||
                  planstate->subPlan;
    if (haschildren) {
        ExplainOpenGroup("Plans", "Plans", false, es);
        /* Pass current PlanState as head of ancestors list for children */
        ancestors = lcons(planstate, ancestors);
    }

    /* initPlan-s */
    if (planstate->initPlan)
        ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es);

    /* Cte distributed support */
    if (IS_STREAM_PLAN && IsA(plan, CteScan)) {
        CteScanState* css = (CteScanState*)planstate;

        if (css->cteplanstate) {
            ExplainNode<is_pretty>(css->cteplanstate, ancestors, "CTE Sub", NULL, es);
        }
    }

    old_dn_flag = es->from_dn;

    /* lefttree */
    if (outerPlanState(planstate)) {
        if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery))
            es->from_dn = true;
        ExplainNode<is_pretty>(outerPlanState(planstate), ancestors, "Outer", NULL, es);
    }

    es->from_dn = old_dn_flag;

    /* righttree */
    if (innerPlanState(planstate)) {
        if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery))
            es->from_dn = true;
        ExplainNode<is_pretty>(innerPlanState(planstate), ancestors, "Inner", NULL, es);
    }

    es->from_dn = old_dn_flag;

    /* special child plans */
    switch (nodeTag(plan)) {
        case T_ModifyTable:
        case T_VecModifyTable:
            ExplainMemberNodes(((ModifyTable*)plan)->plans, ((ModifyTableState*)planstate)->mt_plans, ancestors, es);
            break;
        case T_VecAppend:
        case T_Append:
            ExplainMemberNodes(((Append*)plan)->appendplans, ((AppendState*)planstate)->appendplans, ancestors, es);
            break;
        case T_MergeAppend:
            ExplainMemberNodes(
                ((MergeAppend*)plan)->mergeplans, ((MergeAppendState*)planstate)->mergeplans, ancestors, es);
            break;
        case T_BitmapAnd:
            ExplainMemberNodes(
                ((BitmapAnd*)plan)->bitmapplans, ((BitmapAndState*)planstate)->bitmapplans, ancestors, es);
            break;
        case T_BitmapOr:
            ExplainMemberNodes(((BitmapOr*)plan)->bitmapplans, ((BitmapOrState*)planstate)->bitmapplans, ancestors, es);
            break;
        case T_CStoreIndexAnd:
            ExplainMemberNodes(
                ((CStoreIndexAnd*)plan)->bitmapplans, ((BitmapAndState*)planstate)->bitmapplans, ancestors, es);
            break;
        case T_CStoreIndexOr:
            ExplainMemberNodes(
                ((CStoreIndexOr*)plan)->bitmapplans, ((BitmapOrState*)planstate)->bitmapplans, ancestors, es);
            break;
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            ExplainNode<is_pretty>(((SubqueryScanState*)planstate)->subplan, ancestors, "Subquery", NULL, es);
            break;
        case T_ExtensiblePlan:
            ExplainExtensibleChildren((ExtensiblePlanState*)planstate, ancestors, es);
            break;
        default:
            break;
    }

    /* subPlan-s */
    if (planstate->subPlan)
        ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es);

    /* end of child plans */
    if (haschildren) {
        ancestors = list_delete_first(ancestors);
        ExplainCloseGroup("Plans", "Plans", false, es);
    }

    /* in text format, undo whatever indentation we added */
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        es->indent = save_indent;
        es->pindent = save_pindent;
    }

    ExplainCloseGroup("Plan", relationship ? NULL : "Plan", true, es);

    /* Set info for explain plan. Note that we do not deal with query shipping except "explain plan for select for
     * update". */
    if (es->plan && (planstate->plan->plan_node_id != 0 || IsExplainPlanSelectForUpdateStmt)) {
        /*
         * 1.Handle the case for select for update.
         * Step 1: Check if it's a select for update case.
         */
        if (planstate->plan->plan_node_id == 0 && IsExplainPlanSelectForUpdateStmt) {
            /* Step 2: Set operation for plan table. */
            GetPlanNodePlainText(plan, &pname, &sname, &strategy, &operation, &pt_operation, &pt_options);

            /* Step 3: Set object_type for plan table. */
            RangeTblEntry* rte = (RangeTblEntry*)llast(es->rtable);
            char* objectname = rte->relname;
            const char* object_type = "REMOTE_QUERY";
            es->planinfo->m_planTableData->set_plan_table_objs(plan->plan_node_id, objectname, object_type, NULL);

            /* Step 4: Set projection for plan table. */
            show_plan_tlist(planstate, ancestors, es);
        }

        /* 2.set operation and options, for 'stream' type, operation will be null */
        if (pt_operation == NULL && pname != NULL)
            es->planinfo->m_planTableData->set_plan_table_streaming_ops(pname, &pt_operation, &pt_options);
        es->planinfo->m_planTableData->set_plan_table_ops(planstate->plan->plan_node_id, pt_operation, pt_options);

        /*
         * 3.set object info for index.
         * For index scan we cannot get its object info from ExplainTargetRel. We only can get the info from
         * ExplainNode.
         */
        if (strcmp(pt_operation, "INDEX") == 0 && pt_index_name != NULL && pt_index_owner != NULL)
            es->planinfo->m_planTableData->set_plan_table_objs(
                planstate->plan->plan_node_id, pt_index_name, pt_operation, pt_index_owner);

        /* 4. set cost and cardinality */
        es->planinfo->m_planTableData->set_plan_table_cost_card(plan->plan_node_id, plan->total_cost, plan->plan_rows);
    }
}

/*
 * CalculateProcessedRows
 * The input processed tuple number equals to the returned tuple number
 * of the current node. Here we modify the iput processed tuple rows more precisely.
 * For Example in SeqScan, the processed tuple number should add the tuple number
 * removed by filter.
 * @param (in) innerRows:
 *		returned tuple number for right child node.
 * @param (in) outterRows:
 *		returned tuple number for left child node.
 * @param (in) processedrows:
 *		returned tuple number for current node.
 * @param (out) processedrows:
 *		the processed tuple number for the current node.
 */
static void CalculateProcessedRows(
    PlanState* planstate, int idx, int smpIdx, const uint64* inner_rows, const uint64* outter_rows, uint64* processed_rows)
{
    int removed_rows = 0;
    Plan* plan = planstate->plan;
    switch (nodeTag(plan)) {
        case T_SeqScan:
            if (!((SeqScan*)plan)->scanBatchMode) {
                show_removed_rows(1, planstate, idx, smpIdx, &removed_rows);
                *processed_rows += removed_rows;
            }
            break;
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_DfsScan:
            show_removed_rows(1, planstate, idx, smpIdx, &removed_rows);
            *processed_rows += removed_rows;
            break;
        case T_NestLoop:
        case T_VecNestLoop:
        case T_VecMergeJoin:
        case T_MergeJoin:
        case T_HashJoin:
        case T_VecHashJoin:
            *processed_rows = (*inner_rows) + (*outter_rows);
            break;
        case T_Agg:
        case T_Sort:
        case T_SetOp:
        case T_VecSetOp:
        case T_VecAgg:
        case T_VecSort:
            *processed_rows = *outter_rows;
            break;
        default:
            break;
    }
}

/*
 * show_plan_execnodes
 *     show exec nodes information of a plan node
 *
 * @param (in) planstate:
 *     the plan state
 * @param (in) es:
 *     the explain state
 *
 * @return: void
 */
static void show_plan_execnodes(PlanState* planstate, ExplainState* es)
{
    Plan* plan = planstate->plan;

    if (IsA(plan, Stream) || IsA(plan, VecStream) || IsA(plan, ModifyTable) || IsA(plan, VecModifyTable) ||
        IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery) || u_sess->pgxc_cxt.NumDataNodes == 0) {
        return;
    }

    ShowRunNodeInfo(plan->exec_nodes, es, "Exec Nodes");
    return;
}

/*
 * Show the targetlist of a plan node
 */
static void show_plan_tlist(PlanState* planstate, List* ancestors, ExplainState* es)
{
    Plan* plan = planstate->plan;
    List* context = NIL;
    List* result = NIL;
    bool useprefix = false;
    ListCell* lc = NULL;

    /* No work if empty tlist (this occurs eg in bitmap indexscans) */
    if (plan->targetlist == NIL)
        return;
    /* The tlist of an Append isn't real helpful, so suppress it */
    if (IsA(plan, Append) || IsA(plan, VecAppend))
        return;
    /* Likewise for MergeAppend and RecursiveUnion */
    if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend))
        return;
    if (IsA(plan, RecursiveUnion) || IsA(plan, VecRecursiveUnion))
        return;

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);
    useprefix = list_length(es->rtable) > 1;

    /* Deparse each result column (we now include resjunk ones) */
    foreach (lc, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        /* skip pseudo columns once startwith exist */
        if (IsPseudoReturnTargetEntry(tle) || IsPseudoInternalTargetEntry(tle)) {
            continue;
        }

        result = lappend(result, deparse_expression((Node*)tle->expr, context, useprefix, false));
    }

    if (IsA(plan, ForeignScan) || IsA(plan, VecForeignScan)) {
        ForeignScan* fscan = (ForeignScan*)plan;
        if (IsSpecifiedFDWFromRelid(fscan->scan_relid, GC_FDW)) {
            List* str_targetlist = get_str_targetlist(fscan->fdw_private);
            if (str_targetlist != NULL)
                result = str_targetlist;
        }
    }

    /* Print results */
    ExplainPropertyList("Output", result, es);

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
        es->planinfo->m_verboseInfo->set_plan_name<false, true>();
        appendStringInfo(es->planinfo->m_verboseInfo->info_str, "Output: ");
        ExplainPrettyList(result, es);
    }

    /* For explain plan */
    if (es->plan)
        es->planinfo->m_planTableData->set_plan_table_projection(plan->plan_node_id, result);
#ifdef ENABLE_MULTIPLE_NODES
    /* show distributeKey of scan */
    if (plan->distributed_keys != NIL && show_scan_distributekey(plan)) {
        List* distKey = NIL;
        List* keyList = NIL;
        keyList = plan->distributed_keys;
        foreach (lc, keyList) {
            Var* distriVar = (Var*)lfirst(lc);
            distKey = lappend(distKey, deparse_expression((Node*)distriVar, context, useprefix, false));
        }

        ExplainPropertyList("Distribute Key", distKey, es);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfoString(es->planinfo->m_verboseInfo->info_str, "Distribute Key: ");
            ExplainPrettyList(distKey, es);
        }
    }
#endif
#ifdef STREAMPLAN
    if ((IsA(plan, Stream) || IsA(plan, VecStream)) &&
        (is_redistribute_stream((Stream*)plan) || is_hybid_stream((Stream*)plan)) &&
        ((Stream*)plan)->distribute_keys != NIL) {
        List* distKey = NIL;
        List* keyList = NIL;
        keyList = ((Stream*)plan)->distribute_keys;
        foreach (lc, keyList) {
            Var* distriVar = (Var*)lfirst(lc);
            distKey = lappend(distKey, deparse_expression((Node*)distriVar, context, useprefix, false));
        }

        ExplainPropertyList("Distribute Key", distKey, es);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfoString(es->planinfo->m_verboseInfo->info_str, "Distribute Key: ");
            ExplainPrettyList(distKey, es);
        }
        list_free_deep(distKey);
    }
#endif
}

/*
 * ContainRteInRlsPolicyWalker
 *     Check node whether contain specified RangeTblEntry
 *
 * @param (in)node: Node info
 * @param (in)unused_info: do not use this infor
 * @return: Find row level security policy in this RangeTblEntry or not
 */
static bool ContainRlsQualInRteWalker(Node* node, void* unused_info)
{
    if (node == NULL) {
        return false;
    }
    /* Check range table entry */
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)node;
        if ((rte->rtekind == RTE_RELATION) && (rte->securityQuals != NIL)) {
            return true;
        }

        return false;
    }
    /* Check Query */
    if (IsA(node, Query)) {
        /* Check RTE and skip remote dummy node */
        return range_table_walker(((Query*)node)->rtable,
            (bool (*)())ContainRlsQualInRteWalker,
            unused_info,
            QTW_EXAMINE_RTES | QTW_IGNORE_DUMMY);
    }
    return expression_tree_walker(node, (bool (*)())ContainRlsQualInRteWalker, unused_info);
}

/*
 * Show a generic expression
 */
static void show_expression(
    Node* node, const char* qlabel, PlanState* planstate, List* ancestors, bool useprefix, ExplainState* es)
{
    List* context = NIL;
    char* exprstr = NULL;

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);

    /* Deparse the expression */
    exprstr = deparse_expression(node, context, useprefix, false);

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_detailInfo) {
        es->planinfo->m_detailInfo->set_plan_name<true, true>();
        appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s: %s\n", qlabel, exprstr);
    }

    /* And add to es->str */
    ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void show_qual(
    List* qual, const char* qlabel, PlanState* planstate, List* ancestors, bool useprefix, ExplainState* es)
{
    Node* node = NULL;

    /* No work if empty qual */
    if (qual == NIL)
        return;

    /* Convert AND list to explicit AND */
    node = (Node*)make_ands_explicit(qual);

    /* And show it */
    show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void show_scan_qual(List *qual, const char *qlabel, PlanState *planstate, List *ancestors, ExplainState *es,
    bool show_prefix)
{
    bool useprefix = false;

    useprefix =
        (show_prefix || IsA(planstate->plan, SubqueryScan) || IsA(planstate->plan, VecSubqueryScan) || es->verbose);
    show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * @Description: Show bloom filter index info.
 * @in filterNumList: Index list.
 * @in es: Explain state.
 */
template <bool generate>
static void show_bloomfilter_number(const List* filterNumList, ExplainState* es)
{
    if (filterNumList != NULL) {
        StringInfo str = NULL;
        StringInfoData index_info;
        initStringInfo(&index_info);
        bool first = true;

        ListCell* l = NULL;
        foreach (l, filterNumList) {
            if (first) {
                appendStringInfo(&index_info, "%d", lfirst_int(l));
            } else {
                appendStringInfo(&index_info, ", %d", lfirst_int(l));
            }
            first = false;
        }

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
            es->planinfo->m_detailInfo->set_plan_name<false, true>();
            str = es->planinfo->m_detailInfo->info_str;
        } else {
            if (es->format == EXPLAIN_FORMAT_TEXT)
                appendStringInfoSpaces(es->str, es->indent * 2);
            str = es->str;
        }

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            /* Append bloom filter num.*/
            if (generate) {
                appendStringInfo(str, "%s: %s\n", "Generate Bloom Filter On Index", index_info.data);
            } else {
                appendStringInfo(str, "%s: %s\n", "Filter By Bloom Filter On Index", index_info.data);
            }
        } else {
            /* Append bloom filter num.*/
            if (generate) {
                ExplainPropertyText("Generate Bloom Filter On Index", index_info.data, es);
            } else {
                ExplainPropertyText("Filter By Bloom Filter On Index", index_info.data, es);
            }
        }
        pfree_ext(index_info.data);
    }
}

/*
 * @Description: Show bloomfilter information, include filter var and filter index.
 * @in plan: Vechashjoin plan or Scan plan(include hdfs, hdfs foreign scan).
 * @in planstate: PlanState node.
 * @in ancestors: Ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 * @in es: Explain state.
 */
template <bool generate>
static void show_bloomfilter(Plan* plan, PlanState* planstate, List* ancestors, ExplainState* es)
{
    if (plan->var_list) {
        if (generate) {
            show_expression((Node*)plan->var_list, "Generate Bloom Filter On Expr", planstate, ancestors, true, es);
        } else {
            show_expression((Node*)plan->var_list, "Filter By Bloom Filter On Expr", planstate, ancestors, true, es);
        }

        /* Show bloom filter numbers. */
        show_bloomfilter_number<generate>(plan->filterIndexList, es);
    }
}

static void show_skew_optimization(const PlanState* planstate, ExplainState* es)
{
#ifdef ENABLE_MULTIPLE_NODES
    int skew_opt = SKEW_RES_NONE;
    char* skew_txt = NULL;

    switch (planstate->plan->type) {
        case T_NestLoop:
        case T_VecNestLoop:
        case T_HashJoin:
        case T_VecHashJoin:
        case T_MergeJoin:
        case T_VecMergeJoin:
            skew_opt = ((Join*)planstate->plan)->skewoptimize;
            skew_txt = "Skew Join Optimized";
            break;
        case T_Agg:
        case T_VecAgg:
            skew_opt = ((Agg*)planstate->plan)->skew_optimize;
            skew_txt = "Skew Agg Optimized";
            break;
        default:
            return;
    }

    if (skew_opt == SKEW_RES_NONE)
        return;

    /* Choose skew optimize source. */
    StringInfoData str;
    initStringInfo(&str);

    appendStringInfo(&str, "%s by", skew_txt);

    if ((unsigned int)skew_opt & SKEW_RES_HINT) {
        if ((unsigned int)skew_opt & (SKEW_RES_HINT - 1))
            appendStringInfo(&str, " %s,", "Hint");
        else
            appendStringInfo(&str, " %s", "Hint");
    }

    if ((unsigned int)skew_opt & SKEW_RES_RULE) {
        if ((unsigned int)skew_opt & (SKEW_RES_RULE - 1))
            appendStringInfo(&str, " %s,", "Rule");
        else
            appendStringInfo(&str, " %s", "Rule");
    }

    if ((unsigned int)skew_opt & SKEW_RES_STAT)
        appendStringInfo(&str, " %s", "Statistic");

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
        /* For skew agg, we add node info before print skew info in pretty mode. */
        if (planstate->plan->type == T_Agg || planstate->plan->type == T_VecAgg) {
            StringInfo agg_node_des = makeStringInfo();
            int rc = snprintf_s(agg_node_des->data,
                agg_node_des->maxlen,
                agg_node_des->maxlen - 1,
                "%3d --%s",
                planstate->plan->plan_node_id,
                es->planinfo->m_planInfo->m_pname.data);
            securec_check_ss(rc, "\0", "\0");

            if (strstr(es->planinfo->m_detailInfo->info_str->data, agg_node_des->data) == NULL)
                appendStringInfo(es->planinfo->m_detailInfo->info_str,
                    "%3d --%s\n",
                    planstate->plan->plan_node_id,
                    es->planinfo->m_planInfo->m_pname.data);
        }

        appendStringInfoSpaces(es->planinfo->m_detailInfo->info_str, 8);
        appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s\n", str.data);
    } else {
        if (es->format == EXPLAIN_FORMAT_TEXT && !es->wlm_statistics_plan_max_digit) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfo(es->str, "%s\n", str.data);
        }
    }

    pfree_ext(str.data);
#else
    return;
#endif
}

/**
 * @Description: Show pushdown quals in the flag identifier.
 */
static void show_pushdown_qual(PlanState* planstate, List* ancestors, ExplainState* es, const char* identifier)
{
    Plan* plan = planstate->plan;

    switch (nodeTag(plan)) {
        case T_DfsScan: {
            DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(((DfsScan*)plan)->privateData))->arg;
            show_scan_qual(item->hdfsQual, identifier, planstate, ancestors, es);
            break;
        }
        case T_DfsIndexScan: {
            DfsScan* scan = ((DfsIndexScan*)plan)->dfsScan;
            if (!((DfsIndexScan*)plan)->indexonly) {
                DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(scan->privateData))->arg;
                show_scan_qual(item->hdfsQual, identifier, planstate, ancestors, es);
            }
            break;
        }
        case T_ForeignScan:
        case T_VecForeignScan: {
            /*
             * Only the HDFS foreign plan has pushdown predicate feature,
             * So show the pushdown predicate filter on HDFS foreign plan.
             */
            if (((ForeignScanState*)planstate)->fdwroutine->GetFdwType != NULL &&
                ((ForeignScanState*)planstate)->fdwroutine->GetFdwType() == HDFS_ORC) {
                DefElem* private_data = (DefElem*)linitial(((ForeignScan*)plan)->fdw_private);
                DfsPrivateItem* item = (DfsPrivateItem*)private_data->arg;
                show_scan_qual(item->hdfsQual, identifier, planstate, ancestors, es);
            }
            break;
        }
        case T_ExtensiblePlan: {
            ExtensiblePlanState* css = (ExtensiblePlanState*)planstate;
            show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
            if (plan->qual) {
                show_instrumentation_count("Rows Removed by Filter", 1, planstate, es);
            }
            if (css->methods->ExplainExtensiblePlan) {
                css->methods->ExplainExtensiblePlan(css, ancestors, es);
            }
            break;
        }
        default: {
            /*
             * Do nothing.
             */
            break;
        }
    }
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void show_upper_qual(List* qual, const char* qlabel, PlanState* planstate, List* ancestors, ExplainState* es)
{
    bool useprefix = false;

    useprefix = (list_length(es->rtable) > 1 || es->verbose);
    show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * @Description: append the datanode name to str.
 * @in es: explain state.
 * @in node_name: datanode name
 * @in dop: query dop
 * @in j: current dop index
 * @out: return void
 */
static void append_datanode_name(ExplainState* es, char* node_name, int dop, int j)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoSpaces(es->str, es->indent * 2);
        if (dop == 1)
            appendStringInfo(es->str, " %s ", node_name);
        else
            appendStringInfo(es->str, " %s[worker %d] ", node_name, j);
    } else {
        if (dop == 1) {
            ExplainPropertyText("DN Name", node_name, es);
        } else {
            ExplainPropertyText("DN Name", node_name, es);
            ExplainPropertyInteger("Worker", j, es);
        }
    }
}

/*
 * Show the group by keys for a Agg node.
 */
static void show_groupby_keys(AggState* aggstate, List* ancestors, ExplainState* es)
{
    Agg* plan = (Agg*)aggstate->ss.ps.plan;

    if (plan->numCols > 0 || plan->groupingSets) {
        /* The key columns refer to the tlist of the child plan */
        ancestors = lcons(aggstate, ancestors);

        if (plan->groupingSets)
            show_grouping_sets(outerPlanState(aggstate), plan, ancestors, es);
        else
            show_sort_group_keys(outerPlanState(aggstate),
                "Group By Key",
                plan->numCols,
                plan->grpColIdx,
                NULL,
                NULL,
                NULL,
                ancestors,
                es);

        ancestors = list_delete_first(ancestors);
    }
}

static void show_grouping_sets(PlanState* planstate, Agg* agg, List* ancestors, ExplainState* es)
{
    List* context = NIL;
    bool useprefix = false;
    ListCell* lc = NULL;

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);

    useprefix = (list_length(es->rtable) > 1 || es->verbose);

    ExplainOpenGroup("Grouping Sets", "Grouping Sets", false, es);

    show_grouping_set_keys(planstate, agg, NULL, context, useprefix, ancestors, es);

    foreach (lc, agg->chain) {
        Agg* aggnode = (Agg*)lfirst(lc);
        Sort* sortnode = (Sort*)aggnode->plan.lefttree;

        show_grouping_set_keys(planstate, aggnode, sortnode, context, useprefix, ancestors, es);
    }

    ExplainCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

static void show_grouping_set_keys(PlanState* planstate, Agg* aggnode, Sort* sortnode, List* context, bool useprefix,
    List* ancestors, ExplainState* es)
{
    Plan* plan = planstate->plan;
    char* exprstr = NULL;
    ListCell* lc = NULL;
    List* gsets = aggnode->groupingSets;
    AttrNumber* keycols = aggnode->grpColIdx;

    ExplainOpenGroup("Grouping Set", NULL, true, es);

    if (sortnode != NULL) {
        show_sort_group_keys(planstate,
            "Sort Key",
            sortnode->numCols,
            sortnode->sortColIdx,
            sortnode->sortOperators,
            sortnode->collations,
            sortnode->nullsFirst,
            ancestors,
            es);
        if (es->format == EXPLAIN_FORMAT_TEXT)
            es->indent++;
    }

    ExplainOpenGroup("Group Keys", "Group Keys", false, es);

    foreach (lc, gsets) {
        List* result = NIL;
        ListCell* lc2 = NULL;

        foreach (lc2, (List*)lfirst(lc)) {
            Index i = lfirst_int(lc2);
            AttrNumber keyresno = keycols[i];
            TargetEntry* target = get_tle_by_resno(plan->targetlist, keyresno);

            if (target == NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE), errmsg("no tlist entry for key %d", keyresno)));
            /* Deparse the expression, showing any top-level cast */
            exprstr = deparse_expression((Node*)target->expr, context, useprefix, true);

            result = lappend(result, exprstr);
        }

        if (result == NIL && es->format == EXPLAIN_FORMAT_TEXT)
            ExplainPropertyText("Group By Key", "()", es);
        else
            ExplainPropertyListNested("Group By Key", result, es);
    }

    ExplainCloseGroup("Group Keys", "Group Keys", false, es);

    if (sortnode != NULL && es->format == EXPLAIN_FORMAT_TEXT)
        es->indent--;

    ExplainCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * @Description:Show the grouping keys for a Group node.
 * @in gstate - group node state
 * @in ancestors - a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 * @in es - explain state.
 */
static void show_group_keys(GroupState* gstate, List* ancestors, ExplainState* es)
{
    Group* plan = (Group*)gstate->ss.ps.plan;

    /* The key columns refer to the tlist of the child plan */
    ancestors = lcons(gstate, ancestors);
    show_sort_group_keys(
        outerPlanState(gstate), "Group By Key", plan->numCols, plan->grpColIdx, NULL, NULL, NULL, ancestors, es);
    ancestors = list_delete_first(ancestors);
}

/*
 * Show the sort keys for a Sort node.
 */
static void show_sort_keys(SortState* sortstate, List* ancestors, ExplainState* es)
{
    Sort* plan = (Sort*)sortstate->ss.ps.plan;

    show_sort_group_keys((PlanState*)sortstate,
        "Sort Key",
        plan->numCols,
        plan->sortColIdx,
        plan->sortOperators,
        plan->collations,
        plan->nullsFirst,
        ancestors,
        es);
}

/*
 * Likewise, for a MergeAppend node.
 */
static void show_merge_append_keys(MergeAppendState* mstate, List* ancestors, ExplainState* es)
{
    MergeAppend* plan = (MergeAppend*)mstate->ps.plan;

    show_sort_group_keys((PlanState*)mstate,
        "Sort Key",
        plan->numCols,
        plan->sortColIdx,
        plan->sortOperators,
        plan->collations,
        plan->nullsFirst,
        ancestors,
        es);
}

/*
 * Likewise, for a merge sort info of RemoteQuery or Broadcast node.
 */
static void show_merge_sort_keys(PlanState* state, List* ancestors, ExplainState* es)
{
    SimpleSort* sort = NULL;
    Plan* plan = state->plan;

    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery))
        sort = ((RemoteQuery*)plan)->sort;
    else if (IsA(plan, Stream) || IsA(plan, VecStream))
        sort = ((Stream*)plan)->sort;

    /* No merge sort keys, just return */
    if (sort == NULL)
        return;

    show_sort_group_keys((PlanState*)state,
        "Merge Sort Key",
        sort->numCols,
        sort->sortColIdx,
        sort->sortOperators,
        sort->sortCollations,
        sort->nullsFirst,
        ancestors,
        es);
}

static void show_startwith_pseudo_entries(PlanState* state, List* ancestors, ExplainState* es)
{
    Plan* plan = state->plan;
    StartWithOp *swplan = (StartWithOp *)plan;
    List* result = NIL;
    const char *qlabel = "Start With pseudo atts";

    ListCell *lc = NULL;
    foreach (lc, swplan->internalEntryList) {
        TargetEntry *entry = (TargetEntry *)lfirst(lc);

        result = lappend(result, pstrdup(entry->resname));
    }

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
        es->planinfo->m_verboseInfo->set_plan_name<false, true>();

        appendStringInfo(es->planinfo->m_verboseInfo->info_str, "%s: ", qlabel);
        ExplainPrettyList(result, es);
    } else {
        ExplainPropertyList(qlabel, result, es);
    }

    list_free_deep(result);
}

static void show_detail_sortinfo(ExplainState* es, const char* sortMethod, const char* spaceType, long spaceUsed)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n", sortMethod, spaceType, spaceUsed);
    } else {
        ExplainPropertyText("Sort Method", sortMethod, es);
        ExplainPropertyLong("Sort Space Used", spaceUsed, es);
        ExplainPropertyText("Sort Space Type", spaceType, es);
    }
}

/*
 * show min and max sort info
 */
static void show_detail_sortinfo_min_max(
    ExplainState* es, const char* sortMethod, const char* spaceType, long minspaceUsed, long maxspaceUsed)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfo(
            es->str, "Sort Method: %s  %s: %ldkB ~ %ldkB\n", sortMethod, spaceType, minspaceUsed, maxspaceUsed);
    } else {
        ExplainPropertyText("Sort Method", sortMethod, es);
        ExplainPropertyLong("Sort Space Min Used", minspaceUsed, es);
        ExplainPropertyLong("Sort Space Max Used", maxspaceUsed, es);
        ExplainPropertyText("Sort Space Type", spaceType, es);
    }
}

/*
 * @Description: show peak memory for each datanode and coordinator
 * @in es -  the explain state info
 * @in plan_size - current plan size
 * @out - void
 */
static void show_peak_memory(ExplainState* es, int plan_size)
{
    if (u_sess->instr_cxt.global_instr == NULL) {
            elog(ERROR, "u_sess->instr_cxt.global_instr is NULL");
    }

    bool from_datanode = false;
    bool last_datanode = u_sess->instr_cxt.global_instr->isFromDataNode(1);

    for (int i = 1; i < plan_size; i++) {
        int dop = u_sess->instr_cxt.global_instr->getNodeDop(i + 1);
        from_datanode = u_sess->instr_cxt.global_instr->isFromDataNode(i + 1);

        if (!last_datanode && from_datanode) {
            int64 peak_memory = (int64)(unsigned)((uint)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery -
                                               t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks)
                                << (unsigned int)(chunkSizeInBits - BITS_IN_MB));
            appendStringInfo(es->planinfo->m_staticInfo->info_str, "Coordinator Query Peak Memory:\n");
            es->planinfo->m_staticInfo->set_plan_name<false, true>();
            appendStringInfo(es->planinfo->m_staticInfo->info_str, "Query Peak Memory: %ldMB\n", peak_memory);

            Instrumentation* instr = NULL;
            int64 max_peak_memory = -1;
            int64 min_peak_memory = LONG_MAX;
            bool is_execute = false;

            if (es->detail)
                appendStringInfo(es->planinfo->m_staticInfo->info_str, "DataNode Query Peak Memory\n");

            for (int j = 0; j < u_sess->instr_cxt.global_instr->getInstruNodeNum(); j++) {
                for (int k = 0; k < dop; k++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(j, i + 1, k);
#ifdef ENABLE_MULTIPLE_NODES
                    char* node_name = PGXCNodeGetNodeNameFromId(j, PGXC_NODE_DATANODE);
#else
                    char* node_name = g_instance.exec_cxt.nodeName;
#endif
                    if (instr != NULL) {
                        if (!is_execute)
                            is_execute = true;
                        if (es->detail) {
                            peak_memory = instr->memoryinfo.peakNodeMemory;

                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);
                            appendStringInfo(
                                es->planinfo->m_staticInfo->info_str, "Query Peak Memory: %ldMB\n", peak_memory);
                        } else {
                            max_peak_memory = rtl::max(max_peak_memory, instr->memoryinfo.peakNodeMemory);
                            min_peak_memory = rtl::min(min_peak_memory, instr->memoryinfo.peakNodeMemory);
                        }
                    }
                }
            }

            if (is_execute && !es->detail) {
                appendStringInfo(es->planinfo->m_staticInfo->info_str, "Datanode:\n");
                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                appendStringInfo(
                    es->planinfo->m_staticInfo->info_str, "Max Query Peak Memory: %ldMB\n", max_peak_memory);
                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                appendStringInfo(
                    es->planinfo->m_staticInfo->info_str, "Min Query Peak Memory: %ldMB\n", min_peak_memory);
            }
        }

        last_datanode = from_datanode;
    }
}

/*
 * @Description: show the min/max of datanode executor start/run/end time.
 * @in es -  the explain state info
 * @in plan_node_id - current plan node id
 * @in stage - the stage of executor time
 * @out - void
 */
static void show_dn_executor_time(ExplainState* es, int plan_node_id, ExecutorTime stage)
{
    Instrumentation* instr = NULL;
    double min_time = 0;
    double max_time = 0;
    int min_idx = 0;
    int max_idx = 0;
    bool fisrt_time = true;
    const char* symbol_time = "nostage";
    int data_size = u_sess->instr_cxt.global_instr->getInstruNodeNum();
    for (int i = 0; i < data_size; i++) {
        double exec_time;
        instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, plan_node_id + 1);
        if (instr != NULL) {
            switch (stage) {
                case DN_START_TIME:
                    exec_time = instr->init_time * 1000.0;
                    symbol_time = "start";
                    break;
                case DN_RUN_TIME:
                    exec_time = instr->run_time * 1000.0;
                    symbol_time = "run";
                    break;
                case DN_END_TIME:
                    exec_time = instr->end_time * 1000.0;
                    symbol_time = "end";
                    break;
            }
            if (fisrt_time) {
                min_time = exec_time;
                min_idx = i;
                max_time = exec_time;
                max_idx = i;
                fisrt_time = false;
            } else {
                if (exec_time > max_time) {
                    max_time = exec_time;
                    max_idx = i;
                }

                if (exec_time < min_time) {
                    min_time = exec_time;
                    min_idx = i;
                }
            }
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    char* min_node_name = PGXCNodeGetNodeNameFromId(min_idx, PGXC_NODE_DATANODE);
    char* max_node_name = PGXCNodeGetNodeNameFromId(max_idx, PGXC_NODE_DATANODE);
#else
    char* min_node_name = g_instance.exec_cxt.nodeName;
    char* max_node_name = g_instance.exec_cxt.nodeName;
#endif

    appendStringInfo(es->planinfo->m_query_summary->info_str, "Datanode executor %s time [%s, %s]: [%.3f ms,%.3f ms]\n", symbol_time, min_node_name, max_node_name, min_time, max_time);
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
static void show_sort_info(SortState* sortstate, ExplainState* es)
{
    PlanState* planstate = NULL;
    planstate = (PlanState*)sortstate;
    AssertEreport(IsA(sortstate, SortState) || IsA(sortstate, VecSortState), MOD_EXECUTOR, "unexpect sortstate type");

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;
        const char* sortMethod = NULL;
        const char* spaceType = NULL;
        long spaceUsed = 0;
        bool has_sort_info = false;
        long max_diskUsed = 0;
        long min_diskUsed = MIN_DISK_USED;
        long max_memoryUsed = 0;
        long min_memoryUsed = u_sess->attr.attr_memory.work_mem;
        int sortMethodId = 0;
        int spaceTypeId = 0;

        if (es->detail) {
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                /*
                 * when this operator does not actual executed, we will not show sort info.
                 */
                if (instr == NULL || instr->sorthashinfo.sortMethodId < (int)HEAPSORT ||
                    instr->sorthashinfo.sortMethodId > (int)STILLINPROGRESS)
                    continue;
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                sortMethodId = instr->sorthashinfo.sortMethodId;
                spaceTypeId = instr->sorthashinfo.spaceTypeId;
                sortMethod = sortmessage[sortMethodId].sortName;
                spaceUsed = instr->sorthashinfo.spaceUsed;
                if (spaceTypeId == SORT_IN_DISK)
                    spaceType = "Disk";
                else
                    spaceType = "Memory";

                if (es->planinfo != NULL && es->planinfo->m_runtimeinfo != NULL) {
                    es->planinfo->m_runtimeinfo->put(i, 0, SORT_METHOD, PointerGetDatum(cstring_to_text(sortMethod)));
                    es->planinfo->m_runtimeinfo->put(i, 0, SORT_TYPE, PointerGetDatum(cstring_to_text(spaceType)));
                    es->planinfo->m_runtimeinfo->put(i, 0, SORT_SPACE, Int64GetDatum(spaceUsed));

                    if (es->planinfo->m_staticInfo != NULL) {
                        es->planinfo->m_staticInfo->set_plan_name<true, true>();
                        appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);
                        appendStringInfo(es->planinfo->m_staticInfo->info_str,
                            "Sort Method: %s  %s: %ldkB\n",
                            sortMethod,
                            spaceType,
                            spaceUsed);
                    }
                    
                    continue;
                }

                if (has_sort_info == false)
                    ExplainOpenGroup("Sort Detail", "Sort Detail", false, es);
                has_sort_info = true;
                ExplainOpenGroup("Plan", NULL, true, es);
                append_datanode_name(es, node_name, 1, 0);
                show_detail_sortinfo(es, sortMethod, spaceType, spaceUsed);
                ExplainCloseGroup("Plan", NULL, true, es);
            }

            if (has_sort_info)
                ExplainCloseGroup("Sort Detail", "Sort Detail", false, es);
        } else {
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL || instr->sorthashinfo.sortMethodId < (int)HEAPSORT ||
                    instr->sorthashinfo.sortMethodId > (int)STILLINPROGRESS)
                    continue;

                sortMethodId = instr->sorthashinfo.sortMethodId;
                spaceTypeId = instr->sorthashinfo.spaceTypeId;
                sortMethod = sortmessage[sortMethodId].sortName;
                spaceUsed = instr->sorthashinfo.spaceUsed;
                if (spaceTypeId == SORT_IN_DISK) {
                    spaceType = "Disk";

                    max_diskUsed = rtl::max(spaceUsed, max_diskUsed);
                    min_diskUsed = rtl::min(spaceUsed, min_diskUsed);
                    if (min_diskUsed == MIN_DISK_USED)
                        min_diskUsed = spaceUsed;
                } else {
                    spaceType = "Memory";

                    max_memoryUsed = rtl::max(spaceUsed, max_memoryUsed);
                    min_memoryUsed = rtl::min(spaceUsed, min_memoryUsed);
                }
            }
            if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL 
                && es->planinfo->m_staticInfo != NULL) {
                if (max_memoryUsed != 0) {
                    es->planinfo->m_staticInfo->set_plan_name<true, true>();
                }
                appendStringInfo(es->planinfo->m_staticInfo->info_str,
                    "Sort Method: %s  %s: %ldkB ~ %ldkB\n",
                    sortMethod,
                    "Memory",
                    min_memoryUsed,
                    max_memoryUsed);

                if (max_diskUsed != 0) {
                    es->planinfo->m_staticInfo->set_plan_name<true, true>();
                }
                appendStringInfo(es->planinfo->m_staticInfo->info_str,
                    "Sort Method: %s  %s: %ldkB ~ %ldkB\n",
                    sortMethod,
                    "Disk",
                    min_diskUsed,
                    max_diskUsed);
            } else {
                if (max_memoryUsed != 0) {
                    if (es->format == EXPLAIN_FORMAT_TEXT)
                        appendStringInfoSpaces(es->str, es->indent * 2);
                    show_detail_sortinfo_min_max(es, sortMethod, "Memory", min_memoryUsed, max_memoryUsed);
                }

                if (max_diskUsed != 0) {
                    if (es->format == EXPLAIN_FORMAT_TEXT)
                        appendStringInfoSpaces(es->str, es->indent * 2);
                    show_detail_sortinfo_min_max(es, sortMethod, "Disk", min_diskUsed, max_diskUsed);
                }
            }
        }
    } else {
        char* sortMethod = NULL;
        char* spaceType = NULL;
        int spaceTypeId = 0;
        if (es->analyze && sortstate->sort_Done && sortstate->sortMethodId >= (int)HEAPSORT &&
            sortstate->sortMethodId <= (int)STILLINPROGRESS &&
            (sortstate->spaceTypeId == SORT_IN_DISK || sortstate->spaceTypeId == SORT_IN_MEMORY)) {
            sortMethod = sortmessage[sortstate->sortMethodId].sortName;
            spaceTypeId = sortstate->spaceTypeId;
            if (spaceTypeId == SORT_IN_DISK)
                spaceType = "Disk";
            else
                spaceType = "Memory";

            if (es->planinfo && es->planinfo->m_runtimeinfo) {
                es->planinfo->m_runtimeinfo->put(-1, -1, SORT_METHOD, PointerGetDatum(cstring_to_text(sortMethod)));
                es->planinfo->m_runtimeinfo->put(-1, -1, SORT_TYPE, PointerGetDatum(cstring_to_text(spaceType)));
                es->planinfo->m_runtimeinfo->put(-1, -1, SORT_SPACE, Int64GetDatum(sortstate->spaceUsed));
            }

            if (es->detail == false && t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo &&
                es->planinfo->m_staticInfo) {
                es->planinfo->m_staticInfo->set_plan_name<true, true>();
                appendStringInfo(es->planinfo->m_staticInfo->info_str,
                    "Sort Method: %s  %s: %ldkB\n",
                    sortMethod,
                    spaceType,
                    sortstate->spaceUsed);
            } else {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                    appendStringInfoSpaces(es->str, es->indent * 2);

                show_detail_sortinfo(es, sortMethod, spaceType, sortstate->spaceUsed);
            }
        }
    }
}

template <bool is_detail>
static void show_datanode_hash_info(ExplainState* es, int nbatch, int nbatch_original, int nbuckets, long spacePeakKb)
{
    if (es->format != EXPLAIN_FORMAT_TEXT) {
        ExplainPropertyLong("Hash Buckets", nbuckets, es);
        ExplainPropertyLong("Hash Batches", nbatch, es);
        ExplainPropertyLong("Original Hash Batches", nbatch_original, es);
        ExplainPropertyLong("Peak Memory Usage", spacePeakKb, es);
    } else if (is_detail == false && t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL &&
               es->planinfo->m_staticInfo) {
        if (nbatch_original != nbatch) {
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                " Buckets: %d  Batches: %d (originally %d)  Memory Usage: %ldkB\n",
                nbuckets,
                nbatch,
                nbatch_original,
                spacePeakKb);
        } else {
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                " Buckets: %d  Batches: %d  Memory Usage: %ldkB\n",
                nbuckets,
                nbatch,
                spacePeakKb);
        }
    } else {
        if (nbatch_original != nbatch) {
            appendStringInfo(es->str,
                " Buckets: %d  Batches: %d (originally %d)	Memory Usage: %ldkB\n",
                nbuckets,
                nbatch,
                nbatch_original,
                spacePeakKb);
        } else {
            appendStringInfo(
                es->str, " Buckets: %d  Batches: %d  Memory Usage: %ldkB\n", nbuckets, nbatch, spacePeakKb);
        }
    }
}

/*
 * Brief        : Display LLVM info through explain performance syntax.
 * Description  : Display LLVM info through explain performance syntax.
 * Input        : fsstate, a ForeignScanState struct.
 *			      es, a ExplainState struct.
 * Output       : none.
 * Return Value : none.
 * Notes        : none.
 */
static void show_llvm_info(const PlanState* planstate, ExplainState* es)
{
    if (!es->detail)
        return;

    AssertEreport(planstate != NULL && es != NULL, MOD_EXECUTOR, "unexpect null value");
    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);

                if (instr != NULL && instr->isLlvmOpt) {
#ifdef ENABLE_MULTIPLE_NODES
                    char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                    char* node_name = g_instance.exec_cxt.nodeName;
#endif
                    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_runtimeinfo) {
                        es->planinfo->m_runtimeinfo->put(i, 0, LLVM_OPTIMIZATION, true);
                        es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                        appendStringInfo(es->planinfo->m_datanodeInfo->info_str, "%s ", node_name);
                        appendStringInfoString(es->planinfo->m_datanodeInfo->info_str, "(LLVM Optimized)\n");
                    } else {
                        appendStringInfoSpaces(es->str, es->indent * 2);
                        appendStringInfo(es->str, " %s", node_name);
                        appendStringInfo(es->str, " (LLVM Optimized)\n");
                    }
                }
            }
        } else {
            bool first = true;
            bool has_llvm = false;
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL)
                    continue;
                if (instr->isLlvmOpt) {
                    has_llvm = true;
                    if (first) {
                        ExplainOpenGroup("LLVM Detail", "LLVM Detail", false, es);
                    }
                    ExplainOpenGroup("Plan", NULL, true, es);
#ifdef ENABLE_MULTIPLE_NODES
                    char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                    char* node_name = g_instance.exec_cxt.nodeName;
#endif
                    ExplainPropertyText("DN Name", node_name, es);
                    ExplainPropertyText("LLVM", "LLVM Optimized", es);
                    ExplainCloseGroup("Plan", NULL, true, es);
                    first = false;
                }
            }
            if (has_llvm)
                ExplainCloseGroup("LLVM Detail", "LLVM Detail", false, es);
        }
    }
}

/*
 * @Description: show datanode filenum and respill info
 * @in es: current explainstate
 * @in file_num: file num to show
 * @in respill_time: respill times if any(>0) to show
 * @in expand_times: hashtable expand times
 * @return: void
 */
static void show_datanode_filenum_info(
    ExplainState* es, int file_num, int respill_time, int expand_times, StringInfo info_str)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        if (file_num > 0) {
            appendStringInfo(info_str, "Temp File Num: %d", file_num);
            if (respill_time > 0) {
                appendStringInfo(info_str, ", Spill Time: %d", respill_time);
            }

            if (expand_times > 0)
                appendStringInfo(info_str, ", Expand Times: %d", expand_times);
        }

        if (file_num == 0 && expand_times > 0) {
            appendStringInfo(info_str, "Expand Times: %d", expand_times);
        }

        appendStringInfoChar(info_str, '\n');
    } else {
        ExplainPropertyLong("Temp File Num", file_num, es);
        ExplainPropertyLong("Spill Time", respill_time, es);
        ExplainPropertyLong("Expand Times", expand_times, es);
    }
}

/*
 * @Description: show detail filenum
 * @in es: current explainstate
 * @in planstate: planstate info
 * @in label_name: operator label name
 * @return: void
 */
static void show_detail_filenum_info(const PlanState* planstate, ExplainState* es, const char* label_name)
{
    Instrumentation* instr = NULL;
    int max_file_num = 0;
    int min_file_num = MIN_FILE_NUM;
    bool is_execute = false;
    bool is_writefile = false;
    int datanode_size = 0;
    int i = 0;
    int j = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
    int count_dn_writefile = 0;

    if (u_sess->instr_cxt.global_instr)
        datanode_size = u_sess->instr_cxt.global_instr->getInstruNodeNum();

    if (es->detail) {
        ExplainOpenGroup(label_name, label_name, false, es);
        for (i = 0; i < datanode_size; i++) {
#ifdef ENABLE_MULTIPLE_NODES
            char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
            char* node_name = g_instance.exec_cxt.nodeName;
#endif
            for (j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr != NULL && instr->nloops > 0) {
                    is_writefile = instr->sorthashinfo.hash_writefile;
                    int expand_times = instr->sorthashinfo.hashtable_expand_times;

                    if (is_writefile || expand_times > 0) {
                        ExplainOpenGroup("Plan", NULL, true, es);
                        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL
                            && es->planinfo->m_runtimeinfo != NULL && es->planinfo->m_staticInfo != NULL) {
                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            es->planinfo->m_staticInfo->set_datanode_name(node_name, j, dop);

                            if (is_writefile) {
                                es->planinfo->m_runtimeinfo->put(i, j, HASH_FILENUM, instr->sorthashinfo.hash_FileNum);
                            }

                            show_datanode_filenum_info(es,
                                instr->sorthashinfo.hash_FileNum,
                                instr->sorthashinfo.hash_spillNum,
                                expand_times,
                                es->planinfo->m_staticInfo->info_str);
                        } else {
                            append_datanode_name(es, node_name, dop, j);
                            show_datanode_filenum_info(es,
                                instr->sorthashinfo.hash_FileNum,
                                instr->sorthashinfo.hash_spillNum,
                                expand_times,
                                es->str);
                        }
                        ExplainCloseGroup("Plan", NULL, true, es);
                    }
                }
            }
        }
        ExplainCloseGroup(label_name, label_name, false, es);
    } else {
        if (datanode_size > 0) {
            int file_num = 0;
            for (i = 0; i < datanode_size; i++) {
                ThreadInstrumentation* threadinstr =
                    u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
                if (threadinstr == NULL)
                    continue;
                int count_dop_writefile = 0;
                for (j = 0; j < dop; j++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                    if (instr != NULL && instr->nloops > 0) {
                        if (!is_execute) {
                            is_execute = true;
                        }
                        is_writefile = instr->sorthashinfo.hash_writefile;
                        if (is_writefile == true) {
                            // in all dop,count_dn_writefile only add 1
                            // count_dop_writefile + 1 if writefile is true in current dop
                            if (count_dop_writefile++ == 0) {
                                count_dn_writefile++;
                            }

                            file_num = instr->sorthashinfo.hash_FileNum;
                            max_file_num = rtl::max(max_file_num, file_num);
                            min_file_num = rtl::min(min_file_num, file_num);
                            if (min_file_num == MIN_FILE_NUM) {
                                min_file_num = file_num;
                            }
                        }
                    }
                }
            }

            if (count_dn_writefile < datanode_size) {
                min_file_num = 0;
            }

            if (is_execute && max_file_num > 0) {
                if (es->format != EXPLAIN_FORMAT_TEXT) {
                    ExplainPropertyLong("Max File Num", max_file_num, es);
                    ExplainPropertyLong("Min File Num", min_file_num, es);
                } else {
                    appendStringInfoSpaces(es->str, es->indent * 2);
                    appendStringInfo(es->str, "Max File Num: %d  Min File Num: %d\n", max_file_num, min_file_num);
                    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_PRETTY && es->planinfo->m_staticInfo) {
                        es->planinfo->m_staticInfo->set_plan_name<true, true>();
                        appendStringInfo(es->planinfo->m_staticInfo->info_str,
                            "Max File Num: %d  Min File Num: %d\n",
                            max_file_num,
                            min_file_num);
                    }
                }
            }
        }
    }
}

/*
 * Show information on hashed_setop.
 */
static void show_setop_info(SetOpState* setopstate, ExplainState* es)
{
    PlanState* planstate = (PlanState*)setopstate;
    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr && es->from_dn) {
        show_detail_filenum_info(planstate, es, "Hash Setop Detail");
    } else if (es->analyze) {
        if (!es->from_dn) {
            int filenum = 0;
            int expand_times = 0;
            if (IsA(setopstate, VecSetOpState)) {
                setOpTbl* vechashSetopTbl = (setOpTbl*)((VecSetOpState*)setopstate)->vecSetOpInfo;
                if (vechashSetopTbl != NULL)
                    filenum = vechashSetopTbl->getFileNum();
                expand_times = setopstate->ps.instrument->sorthashinfo.hashtable_expand_times;
            } else {
                SetopWriteFileControl* TempFileControl = (SetopWriteFileControl*)setopstate->TempFileControl;
                if (TempFileControl != NULL)
                    filenum = TempFileControl->filenum;
            }

            if (filenum > 0 || expand_times > 0) {
                if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL)
                    show_datanode_filenum_info(es, filenum, 0, expand_times, es->str);
                else {
                    if (es->planinfo->m_runtimeinfo)
                        es->planinfo->m_runtimeinfo->put(-1, -1, HASH_FILENUM, filenum);
                    if (es->planinfo->m_staticInfo) {
                        es->planinfo->m_staticInfo->set_plan_name<true, true>();
                        show_datanode_filenum_info(es, filenum, 0, expand_times, es->planinfo->m_staticInfo->info_str);
                    }
                }
            }
        }
    }
}

/*
 * @Description: Show hashagg build and probe time info
 * @in planstate: PlanState node.+ * @in es: Explain state.
 */
static void show_detail_execute_info(const PlanState* planstate, ExplainState* es)
{
    Instrumentation* instr = NULL;
    int datanode_size = 0;
    int i = 0;
    int j = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    if (u_sess->instr_cxt.global_instr)
        datanode_size = u_sess->instr_cxt.global_instr->getInstruNodeNum();

    if (!es->detail)
        return;

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_runtimeinfo) {
        for (i = 0; i < datanode_size; i++) {
            ThreadInstrumentation* threadinstr =
                u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
            if (threadinstr == NULL)
                continue;
#ifdef ENABLE_MULTIPLE_NODES
            char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
            char* node_name = g_instance.exec_cxt.nodeName;
#endif
            for (j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr != NULL && instr->nloops > 0) {
                    double build_time = instr->sorthashinfo.hashbuild_time;
                    double agg_time = instr->sorthashinfo.hashagg_time;

                    if (build_time > 0.0 || agg_time > 0.0) {
                        es->planinfo->m_staticInfo->set_plan_name<false, true>();
                        es->planinfo->m_staticInfo->set_datanode_name(node_name, j, dop);

                        appendStringInfo(es->planinfo->m_staticInfo->info_str,
                            "Hashagg Build time: %.3f, Aggregation time: %.3f\n",
                            build_time * 1000.0,
                            agg_time * 1000.0);
                    }
                }
            }
        }
    }
}

/*
 * Show information on hash agg.
 */
static void show_hashAgg_info(AggState* hashaggstate, ExplainState* es)
{
    PlanState* planstate = (PlanState*)hashaggstate;
    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr && es->from_dn) {
        show_detail_filenum_info(planstate, es, "Hash Aggregate Detail");
        show_detail_execute_info(planstate, es);
    } else if (es->analyze) {
        if (!es->from_dn) {
            int filenum = 0;
            int expand_times = 0;
            double hashagg_build_time = 0.0;
            double hashagg_agg_time = 0.0;
            if (IsA(hashaggstate, VecAggState)) {
                VecAgg* plan = (VecAgg*)(hashaggstate->ss.ps.plan);
                if (plan->is_sonichash) {
                    SonicHashAgg* vechashAggTbl = (SonicHashAgg*)((VecAggState*)hashaggstate)->aggRun;
                    if (vechashAggTbl != NULL)
                        filenum = vechashAggTbl->getFileNum();
                } else {
                    HashAggRunner* vechashAggTbl = (HashAggRunner*)((VecAggState*)hashaggstate)->aggRun;
                    if (vechashAggTbl != NULL)
                        filenum = vechashAggTbl->getFileNum();
                }

                expand_times = hashaggstate->ss.ps.instrument->sorthashinfo.hashtable_expand_times;
                hashagg_build_time = hashaggstate->ss.ps.instrument->sorthashinfo.hashbuild_time;
                hashagg_agg_time = hashaggstate->ss.ps.instrument->sorthashinfo.hashagg_time;
            } else {
                AggWriteFileControl* TempFileControl = (AggWriteFileControl*)hashaggstate->aggTempFileControl;
                if (TempFileControl != NULL)
                    filenum = TempFileControl->filenum;
            }

            if (filenum > 0 || expand_times > 0) {
                if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
                    show_datanode_filenum_info(es,
                        filenum,
                        planstate->instrument->sorthashinfo.hash_spillNum,
                        planstate->instrument->sorthashinfo.hashtable_expand_times,
                        es->str);
                } else {
                    if (es->planinfo->m_runtimeinfo != NULL)
                        es->planinfo->m_runtimeinfo->put(-1, -1, HASH_FILENUM, filenum);
                    if (es->planinfo->m_staticInfo != NULL) {
                        es->planinfo->m_staticInfo->set_plan_name<true, true>();

                        show_datanode_filenum_info(es,
                            filenum,
                            planstate->instrument->sorthashinfo.hash_spillNum,
                            planstate->instrument->sorthashinfo.hashtable_expand_times,
                            es->planinfo->m_staticInfo->info_str);
                    }
                }
            }

            if (hashagg_build_time > 0.0 || hashagg_agg_time > 0.0) {
                /* Here must be pretty format */
                if (es->planinfo && es->planinfo->m_staticInfo) {
                    es->planinfo->m_staticInfo->set_plan_name<true, true>();
                    appendStringInfo(es->planinfo->m_staticInfo->info_str,
                        "Hashagg Build time: %.3f, Aggregation time: %.3f\n",
                        hashagg_build_time * 1000.0,
                        hashagg_agg_time * 1000.0);
                }
            }
        }
    }
}
/*
 * Show information on hash buckets/batches.
 */
static void show_hash_info(HashState* hashstate, ExplainState* es)
{
    HashJoinTable hashtable;
    PlanState* planstate = NULL;
    planstate = (PlanState*)hashstate;
    int nbatch;
    int nbatch_original;
    int nbuckets;
    long spacePeakKb = 0;
    int max_nbatch = -1;
    int min_nbatch = INT_MAX;
    int max_nbatch_original = -1;
    int min_nbatch_original = INT_MAX;
    int max_nbuckets = -1;
    int min_nbuckets = INT_MAX;
    long max_spacePeakKb = -1;
    long min_spacePeakKb = LONG_MAX;
    bool is_execute = false;

    AssertEreport(IsA(hashstate, HashState), MOD_EXECUTOR, "unexpect hashstate type");
    hashtable = hashstate->hashtable;

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;

        if (es->detail) {
            ExplainOpenGroup("Hash Detail", "Hash Detail", false, es);
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                ExplainOpenGroup("Plan", NULL, true, es);
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL)
                    continue;
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                append_datanode_name(es, node_name, 1, 0);

                spacePeakKb = (instr->sorthashinfo.spacePeak + 1023) / 1024;
                nbatch = instr->sorthashinfo.nbatch;
                nbatch_original = instr->sorthashinfo.nbatch_original;
                nbuckets = instr->sorthashinfo.nbuckets;
                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo 
                    && es->planinfo->m_staticInfo && es->planinfo->m_runtimeinfo) {
                    es->planinfo->m_runtimeinfo->put(i, 0, HASH_BATCH, nbatch);
                    es->planinfo->m_runtimeinfo->put(i, 0, HASH_BATCH_ORIGNAL, nbatch_original);
                    es->planinfo->m_runtimeinfo->put(i, 0, HASH_BUCKET, nbuckets);
                    es->planinfo->m_runtimeinfo->put(i, 0, HASH_SPACE, spacePeakKb);

                    es->planinfo->m_staticInfo->set_plan_name<false, true>();
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);
                    show_datanode_hash_info<false>(es, nbatch, nbatch_original, nbuckets, spacePeakKb);
                }
                show_datanode_hash_info<true>(es, nbatch, nbatch_original, nbuckets, spacePeakKb);
                ExplainCloseGroup("Plan", NULL, true, es);
            }
            ExplainCloseGroup("Hash Detail", "Hash Detail", false, es);
        } else {
            if (u_sess->instr_cxt.global_instr->getInstruNodeNum() > 0) {
                SortHashInfo* psi = NULL;
                for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);

                    if (instr != NULL) {
                        if (!is_execute)
                            is_execute = true;
                        psi = &instr->sorthashinfo;

                        spacePeakKb = (instr->sorthashinfo.spacePeak + 1023) / 1024;
                        max_nbatch = rtl::max(max_nbatch, psi->nbatch);
                        min_nbatch = rtl::min(min_nbatch, psi->nbatch);
                        max_nbatch_original = rtl::max(max_nbatch_original, psi->nbatch_original);
                        min_nbatch_original = rtl::min(min_nbatch_original, psi->nbatch_original);
                        max_nbuckets = rtl::max(max_nbuckets, psi->nbuckets);
                        min_nbuckets = rtl::min(min_nbuckets, psi->nbuckets);
                        max_spacePeakKb = rtl::max(max_spacePeakKb, spacePeakKb);
                        min_spacePeakKb = rtl::min(min_spacePeakKb, spacePeakKb);
                    }
                }

                if (is_execute) {
                    AssertEreport(max_nbatch != -1 && min_nbatch != INT_MAX && max_nbatch_original != -1 &&
                                      min_nbatch_original != INT_MAX && max_nbuckets != -1 && min_nbuckets != INT_MAX &&
                                      max_spacePeakKb != -1 && min_spacePeakKb != LONG_MAX,
                        MOD_EXECUTOR,
                        "unexpect min/max value");

                    if (es->format != EXPLAIN_FORMAT_TEXT) {
                        ExplainPropertyLong("Max Hash Buckets", max_nbuckets, es);
                        ExplainPropertyLong("Min Hash Buckets", min_nbuckets, es);
                        ExplainPropertyLong("Max Hash Batches", max_nbatch, es);
                        ExplainPropertyLong("Min Hash Batches", min_nbatch, es);
                        ExplainPropertyLong("Max Original Hash Batches", max_nbatch_original, es);
                        ExplainPropertyLong("Min Original Hash Batches", min_nbatch_original, es);
                        ExplainPropertyLong("Max Peak Memory Usage", max_spacePeakKb, es);
                        ExplainPropertyLong("min Peak Memory Usage", min_spacePeakKb, es);
                    } else if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
                        if (max_nbatch_original != max_nbatch) {
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str,
                                "Max Buckets: %d  Max Batches: %d (max originally %d)  Max Memory Usage: %ldkB\n",
                                max_nbuckets,
                                max_nbatch,
                                max_nbatch_original,
                                max_spacePeakKb);
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str,
                                "Min Buckets: %d  Min Batches: %d (min originally %d)  Min Memory Usage: %ldkB\n",
                                min_nbuckets,
                                min_nbatch,
                                min_nbatch_original,
                                min_spacePeakKb);
                        } else {
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str,
                                "Max Buckets: %d  Max Batches: %d  Max Memory Usage: %ldkB\n",
                                max_nbuckets,
                                max_nbatch,
                                max_spacePeakKb);
                            appendStringInfoSpaces(es->str, es->indent * 2);
                            appendStringInfo(es->str,
                                "Min Buckets: %d  Min Batches: %d  Min Memory Usage: %ldkB\n",
                                min_nbuckets,
                                min_nbatch,
                                min_spacePeakKb);
                        }
                    } else if (es->planinfo->m_staticInfo) {
                        if (max_nbatch_original != max_nbatch) {
                            es->planinfo->m_staticInfo->set_plan_name<true, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                                "Max Buckets: %d  Max Batches: %d (max originally %d)  Max Memory Usage: %ldkB\n",
                                max_nbuckets,
                                max_nbatch,
                                max_nbatch_original,
                                max_spacePeakKb);
                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                                "Min Buckets: %d  Min Batches: %d (min originally %d)  Min Memory Usage: %ldkB\n",
                                min_nbuckets,
                                min_nbatch,
                                min_nbatch_original,
                                min_spacePeakKb);
                        } else {
                            es->planinfo->m_staticInfo->set_plan_name<true, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                                "Max Buckets: %d  Max Batches: %d  Max Memory Usage: %ldkB\n",
                                max_nbuckets,
                                max_nbatch,
                                max_spacePeakKb);

                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                                "Min Buckets: %d  Min Batches: %d  Min Memory Usage: %ldkB\n",
                                min_nbuckets,
                                min_nbatch,
                                min_spacePeakKb);
                        }
                    }
                }
            }
        }
    } else if (hashstate->ps.instrument) {
        SortHashInfo hashinfo = hashstate->ps.instrument->sorthashinfo;
        spacePeakKb = (hashinfo.spacePeak + 1023) / 1024;
        nbatch = hashinfo.nbatch;
        nbatch_original = hashinfo.nbatch_original;
        nbuckets = hashinfo.nbuckets;

        /* wlm_statistics_plan_max_digit: this variable is used to judge, isn't it a active sql */
        if (es->wlm_statistics_plan_max_digit == NULL) {
            if (es->format == EXPLAIN_FORMAT_TEXT)
                appendStringInfoSpaces(es->str, es->indent * 2);
            show_datanode_hash_info<false>(es, nbatch, nbatch_original, nbuckets, spacePeakKb);
        }
    }
}

inline static void show_buffers_info(
    StringInfo infostr, bool has_shared, bool has_local, bool has_temp, const BufferUsage* usage)
{
    appendStringInfoString(infostr, "(Buffers:");
    if (has_shared) {
        appendStringInfoString(infostr, " shared");
        if (usage->shared_blks_hit > 0)
            appendStringInfo(infostr, " hit=%ld", usage->shared_blks_hit);
        if (usage->shared_blks_read > 0)
            appendStringInfo(infostr, " read=%ld", usage->shared_blks_read);
        if (usage->shared_blks_dirtied > 0)
            appendStringInfo(infostr, " dirtied=%ld", usage->shared_blks_dirtied);
        if (usage->shared_blks_written > 0)
            appendStringInfo(infostr, " written=%ld", usage->shared_blks_written);
    }
    if (has_local) {

        appendStringInfoString(infostr, " local");
        if (usage->local_blks_hit > 0)
            appendStringInfo(infostr, " hit=%ld", usage->local_blks_hit);
        if (usage->local_blks_read > 0)
            appendStringInfo(infostr, " read=%ld", usage->local_blks_read);
        if (usage->local_blks_dirtied > 0)
            appendStringInfo(infostr, " dirtied=%ld", usage->local_blks_dirtied);
        if (usage->local_blks_written > 0)
            appendStringInfo(infostr, " written=%ld", usage->local_blks_written);
        if (has_temp)
            appendStringInfoChar(infostr, ',');
    }
    if (has_temp) {
        appendStringInfoString(infostr, " temp");
        if (usage->temp_blks_read > 0)
            appendStringInfo(infostr, " read=%ld", usage->temp_blks_read);
        if (usage->temp_blks_written > 0)
            appendStringInfo(infostr, " written=%ld", usage->temp_blks_written);
    }

    appendStringInfoString(infostr, ")\n");
}

static void put_buffers(ExplainState* es, const BufferUsage* usage, int nodeIdx, int smpIdx)
{
    bool has_timing = false;
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, SHARED_BLK_HIT, Int64GetDatum(usage->shared_blks_hit));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, SHARED_BLK_READ, Int64GetDatum(usage->shared_blks_read));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, SHARED_BLK_DIRTIED, Int64GetDatum(usage->shared_blks_dirtied));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, SHARED_BLK_WRITTEN, Int64GetDatum(usage->shared_blks_written));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, LOCAL_BLK_HIT, Int64GetDatum(usage->local_blks_hit));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, LOCAL_BLK_READ, Int64GetDatum(usage->local_blks_read));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, LOCAL_BLK_DIRTIED, Int64GetDatum(usage->local_blks_dirtied));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, LOCAL_BLK_WRITTEN, Int64GetDatum(usage->local_blks_written));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, TEMP_BLK_READ, Int64GetDatum(usage->temp_blks_read));
    es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, TEMP_BLK_WRITTEN, Int64GetDatum(usage->temp_blks_written));

    has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) || !INSTR_TIME_IS_ZERO(usage->blk_write_time));
    if (has_timing) {
        double blk_time;
        if (!INSTR_TIME_IS_ZERO(usage->blk_read_time)) {
            blk_time = INSTR_TIME_GET_MILLISEC(usage->blk_read_time);
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, BLK_READ_TIME, Float8GetDatum(blk_time));
        }
        if (!INSTR_TIME_IS_ZERO(usage->blk_write_time)) {
            blk_time = INSTR_TIME_GET_MILLISEC(usage->blk_write_time);
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, BLK_WRITE_TIME, Float8GetDatum(blk_time));
        }
    }
}

static void show_datanode_vecjoin_info(
    ExplainState* es, StringInfo instr, int64 file_number, int spillNum, long spillSize, long space, bool is_writefile)
{
    if (is_writefile == false) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfo(instr, "Memory Used : %ldkB\n", space);
        } else {
            ExplainPropertyLong("Memory Used", space, es);
        }
    } else {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (spillNum > 0)
                appendStringInfo(instr,
                    "Temp File Num: %ld, Spill Time: %d, Total Written Disk IO: %ldKB\n",
                    file_number,
                    spillNum,
                    spillSize);
            else
                appendStringInfo(instr, "Temp File Num: %ld, Total Written Disk IO: %ldKB\n", file_number, spillSize);
        } else {
            ExplainPropertyLong("Temp File Num", file_number, es);
            ExplainPropertyLong("Spill Time", spillNum, es);
            ExplainPropertyLong("Total Written Disk IO", spillSize, es);
        }
    }
}

static void show_datanode_sonicjoin_info(
    ExplainState* es, StringInfo instr, int partNum, int spillNum, long space, bool is_writefile)
{
    if (is_writefile == false) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfo(instr, "Memory Used : %ldkB\n", space);
        } else {
            ExplainPropertyLong("Memory Used", space, es);
        }
    } else {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (spillNum > 0)
                appendStringInfo(instr, "Partition Num: %d, Spill Times: %d\n", partNum, spillNum);
            else
                appendStringInfo(instr, "Partition Num: %d\n", partNum);
        } else {
            ExplainPropertyLong("Partition Num", partNum, es);
            ExplainPropertyLong("Spill Times", spillNum, es);
        }
    }
}

static void show_datanode_sonicjoin_detail_info(ExplainState* es, StringInfo instr, bool buildside, int partNum,
    int fileNum, long spillSize, long spillSizeMin, long spillSizeMax)
{
    const char* side = buildside ? "Inner Partition" : "Outer Partition";

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfo(instr,
            "%s Spill Num: %d, "
            "Temp File Num: %d, "
            "Written Disk IO: %ldKB [%ldKB %ldKB]\n",
            side,
            partNum,
            fileNum,
            spillSize,
            spillSizeMin,
            spillSizeMax);
    } else {
        ExplainOpenGroup(side, side, false, es);
        ExplainPropertyLong("Spill Num", partNum, es);
        ExplainPropertyLong("Temp File Num", fileNum, es);
        ExplainPropertyLong("Total Written Disk IO", spillSize, es);
        ExplainPropertyLong("Min Partition Written Disk IO", spillSizeMin, es);
        ExplainPropertyLong("Max Partition Written Disk IO", spillSizeMax, es);
        ExplainCloseGroup(side, side, false, es);
    }
}

static void show_vechash_info(VecHashJoinState* hashstate, ExplainState* es)
{

    PlanState* planstate = (PlanState*)hashstate;
    int64 max_file_num = 0;
    int min_file_num = MIN_FILE_NUM;
    bool is_execute = false;
    long spaceUsed, spillSize;
    int64 file_num;
    long max_spaceused = 0;
    long min_spaceused = u_sess->attr.attr_memory.work_mem;
    bool is_writefile = false;
    int spillNum;

    int partNum, innerPartNum, outerPartNum;
    int innerFileNum, outerFileNum;
    long spillInnerSize, spillInnerSizeMax, spillInnerSizeMin;
    long spillOuterSize, spillOuterSizeMax, spillOuterSizeMin;

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;

        if (es->detail) {
            ExplainOpenGroup("VecHashJoin Detail", "VecHashJoin Detail", false, es);
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                ExplainOpenGroup("Plan", NULL, true, es);
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL)
                    continue;
                is_writefile = instr->sorthashinfo.hash_writefile;
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                spaceUsed = (instr->sorthashinfo.spaceUsed + 1023) / 1024;
                spillSize = (instr->sorthashinfo.spill_size + 1023) / 1024;
                file_num = instr->sorthashinfo.hash_FileNum;
                spillNum = instr->sorthashinfo.hash_spillNum;

                partNum = instr->sorthashinfo.hash_partNum;
                innerPartNum = instr->sorthashinfo.spill_innerPartNum;
                outerPartNum = instr->sorthashinfo.spill_outerPartNum;

                innerFileNum = instr->sorthashinfo.hash_innerFileNum;
                outerFileNum = instr->sorthashinfo.hash_outerFileNum;

                spillInnerSize = (instr->sorthashinfo.spill_innerSize + 1023) / 1024;
                spillInnerSizeMax = (instr->sorthashinfo.spill_innerSizePartMax + 1023) / 1024;
                spillInnerSizeMin = (instr->sorthashinfo.spill_innerSizePartMin + 1023) / 1024;
                spillOuterSize = (instr->sorthashinfo.spill_outerSize + 1023) / 1024;
                spillOuterSizeMax = (instr->sorthashinfo.spill_outerSizePartMax + 1023) / 1024;
                spillOuterSizeMin = (instr->sorthashinfo.spill_outerSizePartMin + 1023) / 1024;

                double build_time = instr->sorthashinfo.hashbuild_time;
                double probe_time = instr->sorthashinfo.hashagg_time;
                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && 
                    es->planinfo != NULL && es->planinfo->m_staticInfo != NULL) {
                    if (is_writefile)
                        es->planinfo->m_runtimeinfo->put(i, 0, HASH_FILENUM, file_num);
                    else
                        es->planinfo->m_runtimeinfo->put(i, 0, HASH_SPACE, spaceUsed);

                    es->planinfo->m_staticInfo->set_plan_name<false, true>();
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);

                    if (((HashJoin*)planstate->plan)->isSonicHash) {
                        show_datanode_sonicjoin_info(
                            es, es->planinfo->m_staticInfo->info_str, partNum, spillNum, spaceUsed, is_writefile);
                        if (is_writefile) {
                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);
                            show_datanode_sonicjoin_detail_info(es,
                                es->planinfo->m_staticInfo->info_str,
                                true,
                                innerPartNum,
                                innerFileNum,
                                spillInnerSize,
                                spillInnerSizeMin,
                                spillInnerSizeMax);
                            es->planinfo->m_staticInfo->set_plan_name<false, true>();
                            appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s ", node_name);
                            show_datanode_sonicjoin_detail_info(es,
                                es->planinfo->m_staticInfo->info_str,
                                false,
                                outerPartNum,
                                outerFileNum,
                                spillOuterSize,
                                spillOuterSizeMin,
                                spillOuterSizeMax);
                        }
                    } else
                        show_datanode_vecjoin_info(es,
                            es->planinfo->m_staticInfo->info_str,
                            file_num,
                            spillNum,
                            spillSize,
                            spaceUsed,
                            is_writefile);

                    es->planinfo->m_staticInfo->set_plan_name<false, true>();
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, "%s", node_name);
                    appendStringInfo(es->planinfo->m_staticInfo->info_str,
                        " Hashjoin Build time: %.3f, Probe time: %.3f\n",
                        build_time * 1000.0,
                        probe_time * 1000.0);
                } else {
                    append_datanode_name(es, node_name, 1, 0);
                    if (((HashJoin*)planstate->plan)->isSonicHash) {
                        show_datanode_sonicjoin_info(es, es->str, partNum, spillNum, spaceUsed, is_writefile);
                        if (is_writefile) {
                            append_datanode_name(es, node_name, 1, 0);
                            show_datanode_sonicjoin_detail_info(es,
                                es->str,
                                true,
                                innerPartNum,
                                innerFileNum,
                                spillInnerSize,
                                spillInnerSizeMin,
                                spillInnerSizeMax);
                            append_datanode_name(es, node_name, 1, 0);
                            show_datanode_sonicjoin_detail_info(es,
                                es->str,
                                false,
                                outerPartNum,
                                outerFileNum,
                                spillOuterSize,
                                spillOuterSizeMin,
                                spillOuterSizeMax);
                        }
                    } else {
                        show_datanode_vecjoin_info(es, es->str, file_num, spillNum, spillSize, spaceUsed, is_writefile);
                    }
                }
                ExplainCloseGroup("Plan", NULL, true, es);
            }
            ExplainCloseGroup("VecHashJoin Detail", "VecHashJoin Detail", false, es);
        } else {
            if (u_sess->instr_cxt.global_instr->getInstruNodeNum() > 0) {
                file_num = 0;
                for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                    ThreadInstrumentation* threadinstr =
                        u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
                    if (threadinstr == NULL)
                        continue;
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                    if (instr != NULL) {
                        if (!is_execute)
                            is_execute = true;
                        file_num = instr->sorthashinfo.hash_FileNum;
                        spaceUsed = (instr->sorthashinfo.spaceUsed + 1023) / 1024;

                        max_file_num = rtl::max(max_file_num, file_num);
                        min_file_num = rtl::min((int64)min_file_num, file_num);
                        if (min_file_num == MIN_FILE_NUM)
                            min_file_num = file_num;

                        max_spaceused = rtl::max(max_spaceused, spaceUsed);
                        min_spaceused = rtl::min(min_spaceused, spaceUsed);
                    }
                }

                if (is_execute) {
                    if (es->format == EXPLAIN_FORMAT_TEXT) {
                        if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
                            /*
                             * Here, we adjust output information and here our output will be used
                             * major showing whether writing to disk occurs or not.
                             * 1, min_file_num and max_file_num are both equal to 0.
                             *    It means All DNs' work mem can hold themselves hash table in memory,
                             * 	  then we only print out memory usage related information.
                             * 2, min_spaceused and max_spaceused are both equal to 0.
                             * 	  It means all the DNs have written out the hash table content to disk,
                             * 	  then we only print out min and max file num information.
                             * 3, If file num and spaceused are not both equal to 0.
                             *    It means some DNs' work mem are not enough to hold themselves hash table in memory,
                             * 	  here we print out only file num related informaiton to show the writing out operation
                             * occurs. 4, All above four variables equal to 0, this situation could occur when the hash
                             * table is empty.
                             */
                            if (min_file_num != 0 || max_file_num != 0) {
                                appendStringInfoSpaces(es->str, es->indent * 2);
                                appendStringInfo(es->str, "Max File Num: %ld\n", max_file_num);
                                appendStringInfoSpaces(es->str, es->indent * 2);
                                appendStringInfo(es->str, "Min File Num: %d\n", min_file_num);
                            } else {
                                appendStringInfoSpaces(es->str, es->indent * 2);
                                appendStringInfo(es->str, "Max Memory Used : %ldkB\n", max_spaceused);
                                appendStringInfoSpaces(es->str, es->indent * 2);
                                appendStringInfo(es->str, "Min Memory Used : %ldkB\n", min_spaceused);
                            }
                        } else if (es->planinfo->m_staticInfo) {
                            if (min_file_num != 0 || max_file_num != 0) {
                                es->planinfo->m_staticInfo->set_plan_name<true, true>();
                                appendStringInfo(
                                    es->planinfo->m_staticInfo->info_str, "Max File Num: %ld\n", max_file_num);
                                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                                appendStringInfo(
                                    es->planinfo->m_staticInfo->info_str, "Min File Num: %d\n", min_file_num);
                            } else {
                                es->planinfo->m_staticInfo->set_plan_name<true, true>();
                                appendStringInfo(
                                    es->planinfo->m_staticInfo->info_str, "Max Memory Used : %ldkB\n", max_spaceused);
                                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                                appendStringInfo(
                                    es->planinfo->m_staticInfo->info_str, "Min Memory Used : %ldkB\n", min_spaceused);
                            }
                        }
                    } else {
                        ExplainPropertyLong("Min Memory Used", min_spaceused, es);
                        ExplainPropertyLong("Max Memory Used", max_spaceused, es);
                        ExplainPropertyLong("Min File Num", min_file_num, es);
                        ExplainPropertyLong("Max File Num", max_file_num, es);
                    }
                }
            }
        }
    }
}

static void show_startwith_dfx(StartWithOpState* swstate, ExplainState* es)
{
    List* result = NIL;
    IterationStats* iters = &(swstate->iterStats);

    /* return if not verbose or not actually executed */
    if (!es->verbose || iters->currentStartTime.tv_sec == 0) {
        return;
    }

    const char *qlabel = "Start With Iteration Statistics";
    bool rotated = (iters->totalIters > SW_LOG_ROWS_FULL) ? true : false;
    int offset = rotated ? iters->totalIters % SW_LOG_ROWS_HALF : 0;
    StringInfo si = makeStringInfo();

    for (int i = 0; i < SW_LOG_ROWS_FULL && i < iters->totalIters; i++) {
        int ri = (i >= SW_LOG_ROWS_HALF && rotated) ?
                     SW_LOG_ROWS_HALF + (i + offset) % SW_LOG_ROWS_HALF : i;
        double total_time = (iters->endTimeBuf[ri].tv_sec - iters->startTimeBuf[ri].tv_sec) / 0.001 +
                            (iters->endTimeBuf[ri].tv_usec - iters->startTimeBuf[ri].tv_usec) * 0.001;
        appendStringInfo(
            si, "\nIteration: %d, Row(s): %ld, Time Cost: %.3lf ms",
            iters->levelBuf[ri], iters->rowCountBuf[ri], total_time);
        result = lappend(result, pstrdup(si->data));
        resetStringInfo(si);
    }
    DestroyStringInfo(si);

    ExplainPropertyListPostPlanTree(qlabel, result, es);

    list_free_deep(result);
}

static void show_recursive_info(RecursiveUnionState* rustate, ExplainState* es)
{
    PlanState* planstate = (PlanState*)rustate;

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;

        if (es->detail && t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL) {
            ExplainOpenGroup("RecursiveUnion Detail", "RecursiveUnion Detail", false, es);

            /* Find iteration number */
            int niters = 0;
            bool has_reach_limit = false;
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL) {
                    continue;
                }

                niters = Max(niters, instr->recursiveInfo.niters);

                if (instr->recursiveInfo.has_reach_limit)
                    has_reach_limit = true;
            }

            if (niters == 0) {
                ExplainCloseGroup("RecursiveUnion Detail", "RecursiveUnion Detail", false, es);
                return;
            }

            appendStringInfo(
                es->planinfo->m_recursiveInfo->info_str, "%3d --Recursive Union\n", planstate->plan->plan_node_id);

            appendStringInfoSpaces(es->planinfo->m_recursiveInfo->info_str, 8);
            appendStringInfo(es->planinfo->m_recursiveInfo->info_str, "Iteration times: %d\n", niters - 1);

            /* report non-recursive part */
            appendStringInfoSpaces(es->planinfo->m_recursiveInfo->info_str, 8);
            appendStringInfo(es->planinfo->m_recursiveInfo->info_str, "Iteration[0] (Step 0 None-Recursive)\n");
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL) {
                    continue;
                }
                appendStringInfoSpaces(es->planinfo->m_recursiveInfo->info_str, 16);
                appendStringInfo(es->planinfo->m_recursiveInfo->info_str,
                    "%s return tuples: %lu\n",
                    node_name,
                    instr->recursiveInfo.iter_ntuples[0]);
            }

            /* report recursive part */
            for (int iteridx = 1; iteridx < niters; iteridx++) {
                appendStringInfoSpaces(es->planinfo->m_recursiveInfo->info_str, 8);
                appendStringInfo(es->planinfo->m_recursiveInfo->info_str,
                    "Iteration[%d] (Step %d Recursive[%d])%s\n",
                    iteridx,
                    iteridx,
                    iteridx,
                    (iteridx == niters - 1 && !has_reach_limit) ? " --- finish" : "");

                for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                    if (instr == NULL) {
                        continue;
                    }
#ifdef ENABLE_MULTIPLE_NODES
                    char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                    char* node_name = g_instance.exec_cxt.nodeName;
#endif
                    appendStringInfoSpaces(es->planinfo->m_recursiveInfo->info_str, 16);
                    appendStringInfo(es->planinfo->m_recursiveInfo->info_str,
                        "%s return tuples: %lu\n",
                        node_name,
                        instr->recursiveInfo.iter_ntuples[iteridx]);
                }
            }

            ExplainCloseGroup("RecursiveUnion Detail", "RecursiveUnion Detail", false, es);
        }
    }
}

static void show_datanode_buffers(ExplainState* es, PlanState* planstate)
{
    Instrumentation* instr = NULL;
    int nodeNum = u_sess->instr_cxt.global_instr->getInstruNodeNum();
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
    int i = 0;
    int j = 0;

    if (es->detail) {
        ExplainOpenGroup("Buffers In Detail", "Buffers In Detail", false, es);
        for (i = 0; i < nodeNum; i++) {
            for (j = 0; j < dop; j++) {
                ExplainOpenGroup("Plan", NULL, true, es);
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr == NULL)
                    continue;
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                append_datanode_name(es, node_name, dop, j);

                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
                    es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                    es->planinfo->m_datanodeInfo->set_datanode_name(node_name, j, dop);
                }

                show_buffers(es, es->str, instr, true, i, j, node_name);
                ExplainCloseGroup("Plan", NULL, true, es);
            }
        }
        ExplainCloseGroup("Buffers In Detail", "Buffers In Detail", false, es);
    } else {
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_IOInfo)
            show_analyze_buffers(es, planstate, es->planinfo->m_IOInfo->info_str, nodeNum);
        else
            show_analyze_buffers(es, planstate, es->str, nodeNum);
    }
}

static void show_analyze_buffers(ExplainState* es, const PlanState* planstate, StringInfo infostr, int nodeNum)
{
    bool has_shared = false;
    bool has_local = false;
    bool has_temp = false;
    bool has_timing = false;
    long shared_blks_hit_max = -1;
    long shared_blks_hit_min = LONG_MAX;
    long shared_blks_read_max = -1;
    long shared_blks_read_min = LONG_MAX;
    long shared_blks_dirtied_max = -1;
    long shared_blks_dirtied_min = LONG_MAX;
    long shared_blks_written_max = -1;
    long shared_blks_written_min = LONG_MAX;
    long local_blks_hit_max = -1;
    long local_blks_hit_min = LONG_MAX;
    long local_blks_read_max = -1;
    long local_blks_read_min = LONG_MAX;
    long local_blks_dirtied_max = -1;
    long local_blks_dirtied_min = LONG_MAX;
    long local_blks_written_max = -1;
    long local_blks_written_min = LONG_MAX;
    long temp_blks_read_max = -1;
    long temp_blks_read_min = LONG_MAX;
    long temp_blks_written_max = -1;
    long temp_blks_written_min = LONG_MAX;
    instr_time blk_read_time_max, blk_read_time_min;
    instr_time blk_write_time_max, blk_write_time_min;
    bool is_execute = false;
    Instrumentation* instr = NULL;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    INSTR_TIME_SET_ZERO(blk_read_time_max);
    INSTR_TIME_SET_ZERO(blk_write_time_max);
    INSTR_TIME_INITIAL_MIN(blk_read_time_min);
    INSTR_TIME_INITIAL_MIN(blk_write_time_min);

    if (nodeNum > 0) {
        for (int i = 0; i < nodeNum; i++) {
            ThreadInstrumentation* threadinstr =
                u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
            if (threadinstr == NULL)
                continue;
            for (int j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr != NULL && instr->nloops > 0) {
                    BufferUsage* pbu = &instr->bufusage;

                    if (!is_execute) {
                        is_execute = true;
                    }

                    if (!has_shared)
                        has_shared = (pbu->shared_blks_hit > 0 || pbu->shared_blks_read > 0 ||
                                      pbu->shared_blks_dirtied > 0 || pbu->shared_blks_written > 0);
                    shared_blks_read_max = rtl::max(shared_blks_read_max, pbu->shared_blks_read);
                    shared_blks_read_min = rtl::min(shared_blks_read_min, pbu->shared_blks_read);
                    shared_blks_hit_max = rtl::max(shared_blks_hit_max, pbu->shared_blks_hit);
                    shared_blks_hit_min = rtl::min(shared_blks_hit_min, pbu->shared_blks_hit);
                    shared_blks_dirtied_max = rtl::max(shared_blks_dirtied_max, pbu->shared_blks_dirtied);
                    shared_blks_dirtied_min = rtl::min(shared_blks_dirtied_min, pbu->shared_blks_dirtied);
                    shared_blks_written_max = rtl::max(shared_blks_written_max, pbu->shared_blks_written);
                    shared_blks_written_min = rtl::min(shared_blks_written_min, pbu->shared_blks_written);

                    if (!has_local)
                        has_local = (pbu->local_blks_hit > 0 || pbu->local_blks_read > 0 ||
                                     pbu->local_blks_dirtied > 0 || pbu->local_blks_written > 0);
                    local_blks_read_max = rtl::max(local_blks_read_max, pbu->local_blks_read);
                    local_blks_read_min = rtl::min(local_blks_read_min, pbu->local_blks_read);
                    local_blks_hit_max = rtl::max(local_blks_hit_max, pbu->local_blks_hit);
                    local_blks_hit_min = rtl::min(local_blks_hit_min, pbu->local_blks_hit);
                    local_blks_dirtied_max = rtl::max(local_blks_dirtied_max, pbu->local_blks_dirtied);
                    local_blks_dirtied_min = rtl::min(local_blks_dirtied_min, pbu->local_blks_dirtied);
                    local_blks_written_max = rtl::max(local_blks_written_max, pbu->local_blks_written);
                    local_blks_written_min = rtl::min(local_blks_written_min, pbu->local_blks_written);

                    if (!has_temp)
                        has_temp = (pbu->temp_blks_read > 0 || pbu->temp_blks_written > 0);
                    temp_blks_read_max = rtl::max(temp_blks_read_max, pbu->temp_blks_read);
                    temp_blks_read_min = rtl::min(temp_blks_read_min, pbu->temp_blks_read);
                    temp_blks_written_max = rtl::max(temp_blks_written_max, pbu->temp_blks_written);
                    temp_blks_written_min = rtl::min(temp_blks_written_min, pbu->temp_blks_written);

                    if (!has_timing)
                        has_timing =
                            (!INSTR_TIME_IS_ZERO(pbu->blk_read_time) || !INSTR_TIME_IS_ZERO(pbu->blk_write_time));
                    if (!INSTR_TIME_IS_BIGGER(blk_read_time_max, pbu->blk_read_time))
                        blk_read_time_max = pbu->blk_read_time;
                    if (INSTR_TIME_IS_BIGGER(blk_read_time_min, pbu->blk_read_time))
                        blk_read_time_min = pbu->blk_read_time;
                    if (!INSTR_TIME_IS_BIGGER(blk_write_time_max, pbu->blk_write_time))
                        blk_write_time_max = pbu->blk_write_time;
                    if (INSTR_TIME_IS_BIGGER(blk_write_time_min, pbu->blk_write_time))
                        blk_write_time_min = pbu->blk_write_time;
                }
            }
        }

        if (is_execute) {
            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (has_shared || has_local || has_temp) {
                    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_IOInfo) {
                        es->planinfo->m_IOInfo->set_plan_name<true, true>();
                        appendStringInfoString(es->planinfo->m_IOInfo->info_str, "Buffers:");
                    } else {
                        appendStringInfoSpaces(es->str, es->indent * 2);
                        appendStringInfoString(es->str, "Buffers:");
                    }

                    if (has_shared) {
                        appendStringInfoString(infostr, " shared");
                        if (shared_blks_hit_max > 0)
                            appendStringInfo(infostr, " max hit=%ld", shared_blks_hit_max);
                        if (shared_blks_hit_min > 0 && shared_blks_hit_min < LONG_MAX)
                            appendStringInfo(infostr, " min hit=%ld", shared_blks_hit_min);
                        if (shared_blks_read_max > 0)
                            appendStringInfo(infostr, " max read=%ld", shared_blks_read_max);
                        if (shared_blks_read_min > 0 && shared_blks_read_min < LONG_MAX)
                            appendStringInfo(infostr, " min  read=%ld", shared_blks_read_min);
                        if (shared_blks_dirtied_max > 0)
                            appendStringInfo(infostr, " max dirtied=%ld", shared_blks_dirtied_max);
                        if (shared_blks_dirtied_min > 0 && shared_blks_dirtied_min < LONG_MAX)
                            appendStringInfo(infostr, " min dirtied=%ld", shared_blks_dirtied_min);
                        if (shared_blks_written_max > 0)
                            appendStringInfo(infostr, " max written=%ld", shared_blks_written_max);
                        if (shared_blks_written_min > 0 && shared_blks_written_min < LONG_MAX)
                            appendStringInfo(infostr, " min written=%ld", shared_blks_written_min);
                        if (has_local || has_temp)
                            appendStringInfoChar(infostr, ',');
                    }
                    if (has_local) {
                        appendStringInfoString(infostr, " local");
                        if (local_blks_hit_max > 0)
                            appendStringInfo(infostr, " max hit=%ld", local_blks_hit_max);
                        if (local_blks_hit_min > 0 && local_blks_hit_min < LONG_MAX)
                            appendStringInfo(infostr, " min hit=%ld", local_blks_hit_min);
                        if (local_blks_read_max > 0)
                            appendStringInfo(infostr, " max read=%ld", local_blks_read_max);
                        if (local_blks_read_min > 0 && local_blks_read_min < LONG_MAX)
                            appendStringInfo(infostr, " min read=%ld", local_blks_read_min);
                        if (local_blks_dirtied_max > 0)
                            appendStringInfo(infostr, " max dirtied=%ld", local_blks_dirtied_max);
                        if (local_blks_dirtied_min > 0 && local_blks_dirtied_min < LONG_MAX)
                            appendStringInfo(infostr, " min dirtied=%ld", local_blks_dirtied_min);
                        if (local_blks_written_max > 0)
                            appendStringInfo(infostr, " max written=%ld", local_blks_written_max);
                        if (local_blks_written_min > 0 && local_blks_written_min < LONG_MAX)
                            appendStringInfo(infostr, " min written=%ld", local_blks_written_min);
                        if (has_temp)
                            appendStringInfoChar(infostr, ',');
                    }
                    if (has_temp) {
                        appendStringInfoString(infostr, " temp");
                        if (temp_blks_read_max > 0)
                            appendStringInfo(infostr, " max read=%ld", temp_blks_read_max);
                        if (temp_blks_read_min > 0 && temp_blks_read_min < LONG_MAX)
                            appendStringInfo(infostr, " min read=%ld", temp_blks_read_min);
                        if (temp_blks_written_max > 0)
                            appendStringInfo(infostr, " max written=%ld", temp_blks_written_max);
                        if (temp_blks_written_min > 0 && temp_blks_written_min < LONG_MAX)
                            appendStringInfo(infostr, " min written=%ld", temp_blks_written_min);
                    }
                    appendStringInfoChar(infostr, '\n');
                }

                /* As above, show only positive counter values. */
                if (has_timing) {
                    appendStringInfoSpaces(infostr, es->indent * 2);
                    appendStringInfoString(infostr, "I/O Timings:");
                    if (!INSTR_TIME_IS_ZERO(blk_read_time_max))
                        appendStringInfo(infostr, " max read=%.3f", INSTR_TIME_GET_MILLISEC(blk_read_time_max));
                    if (!INSTR_TIME_IS_ZERO(blk_read_time_min) && !INSTR_TIME_IS_INTMAX(blk_read_time_min))
                        appendStringInfo(infostr, " min read=%.3f", INSTR_TIME_GET_MILLISEC(blk_read_time_min));
                    if (!INSTR_TIME_IS_ZERO(blk_write_time_max))
                        appendStringInfo(infostr, " max write=%.3f", INSTR_TIME_GET_MILLISEC(blk_write_time_max));
                    if (!INSTR_TIME_IS_ZERO(blk_write_time_min) && !INSTR_TIME_IS_INTMAX(blk_write_time_min))
                        appendStringInfo(infostr, " min write=%.3f", INSTR_TIME_GET_MILLISEC(blk_write_time_min));
                    appendStringInfoChar(infostr, '\n');
                }
            } else {
                ExplainPropertyLong("Max Shared Hit Blocks", shared_blks_hit_max, es);
                ExplainPropertyLong("Min Shared Hit Blocks", shared_blks_hit_min, es);
                ExplainPropertyLong("Max Shared Read Blocks", shared_blks_read_max, es);
                ExplainPropertyLong("Min Shared Read Blocks", shared_blks_read_min, es);
                ExplainPropertyLong("Max Shared Dirtied Blocks", shared_blks_dirtied_max, es);
                ExplainPropertyLong("Min Shared Dirtied Blocks", shared_blks_dirtied_min, es);
                ExplainPropertyLong("Max Shared Written Blocks", shared_blks_written_max, es);
                ExplainPropertyLong("Max Shared Written Blocks", shared_blks_written_min, es);

                ExplainPropertyLong("Max Local Hit Blocks", local_blks_hit_max, es);
                ExplainPropertyLong("Min Local Hit Blocks", local_blks_hit_min, es);
                ExplainPropertyLong("Max Local Read Blocks", local_blks_read_max, es);
                ExplainPropertyLong("Min Local Read Blocks", local_blks_read_min, es);
                ExplainPropertyLong("Max Local Dirtied Blocks", local_blks_dirtied_max, es);
                ExplainPropertyLong("Min Local Dirtied Blocks", local_blks_dirtied_min, es);
                ExplainPropertyLong("Max Local Written Blocks", local_blks_written_max, es);
                ExplainPropertyLong("Min Local Written Blocks", local_blks_written_min, es);

                ExplainPropertyLong("Max Temp Read Blocks", temp_blks_read_max, es);
                ExplainPropertyLong("Min Temp Read Blocks", temp_blks_read_min, es);
                ExplainPropertyLong("Max Temp Written Blocks", temp_blks_written_max, es);
                ExplainPropertyLong("Min Temp Written Blocks", temp_blks_written_min, es);
                ExplainPropertyFloat("Max IO Read Time", INSTR_TIME_GET_MILLISEC(blk_read_time_max), 3, es);
                ExplainPropertyFloat("Min IO Read Time", INSTR_TIME_GET_MILLISEC(blk_read_time_min), 3, es);
                ExplainPropertyFloat("Max IO Write Time", INSTR_TIME_GET_MILLISEC(blk_write_time_max), 3, es);
                ExplainPropertyFloat("Min IO Write Time", INSTR_TIME_GET_MILLISEC(blk_write_time_min), 3, es);
            }
        }
    }
}

static void show_buffers(ExplainState* es, StringInfo infostr, const Instrumentation* instrument, bool is_datanode,
    int nodeIdx, int smpIdx, const char* node_name)
{
    const BufferUsage* usage = &instrument->bufusage;

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        bool has_shared = (usage->shared_blks_hit > 0 || usage->shared_blks_read > 0 ||
                           usage->shared_blks_dirtied > 0 || usage->shared_blks_written > 0);
        bool has_local = (usage->local_blks_hit > 0 || usage->local_blks_read > 0 || usage->local_blks_dirtied > 0 ||
                          usage->local_blks_written > 0);
        bool has_temp = (usage->temp_blks_read > 0 || usage->temp_blks_written > 0);
        bool has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) || !INSTR_TIME_IS_ZERO(usage->blk_write_time));

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->detail) {
            put_buffers(es, usage, nodeIdx, smpIdx);

            if (node_name == NULL)
                appendStringInfoSpaces(es->planinfo->m_datanodeInfo->info_str, 8);

            if (has_shared || has_temp) {
                show_buffers_info(es->planinfo->m_datanodeInfo->info_str, has_shared, has_local, has_temp, usage);
            } else {
                appendStringInfo(es->planinfo->m_datanodeInfo->info_str, "(Buffers: 0)\n");
            }
            return;
        }

        /* Show only positive counter values. */
        if (has_shared || has_local || has_temp) {
            if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_IOInfo) {
                es->planinfo->m_IOInfo->set_plan_name<true, true>();
                appendStringInfoString(es->planinfo->m_IOInfo->info_str, "Buffers:");
            }

            if (!is_datanode)
                appendStringInfoSpaces(es->str, es->indent * 2);
            show_buffers_info(infostr, has_shared, has_local, has_temp, usage);
        } else if (is_datanode) {
            appendStringInfo(infostr, get_execute_mode(es, nodeIdx) ? "(Buffers: 0)\n" : "(Buffers: unknown)\n");
        }

        /* As above, show only positive counter values. */
        if (has_timing) {
            appendStringInfoSpaces(infostr, es->indent * 2);
            appendStringInfoString(infostr, "I/O Timings:");
            if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
                appendStringInfo(infostr, " read=%.3f", INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
            if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
                appendStringInfo(infostr, " write=%.3f", INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
            appendStringInfoChar(infostr, '\n');
        }
    } else {
        ExplainPropertyLong("Shared Hit Blocks", usage->shared_blks_hit, es);
        ExplainPropertyLong("Shared Read Blocks", usage->shared_blks_read, es);
        ExplainPropertyLong("Shared Dirtied Blocks", usage->shared_blks_dirtied, es);
        ExplainPropertyLong("Shared Written Blocks", usage->shared_blks_written, es);
        ExplainPropertyLong("Local Hit Blocks", usage->local_blks_hit, es);
        ExplainPropertyLong("Local Read Blocks", usage->local_blks_read, es);
        ExplainPropertyLong("Local Dirtied Blocks", usage->local_blks_dirtied, es);
        ExplainPropertyLong("Local Written Blocks", usage->local_blks_written, es);
        ExplainPropertyLong("Temp Read Blocks", usage->temp_blks_read, es);
        ExplainPropertyLong("Temp Written Blocks", usage->temp_blks_written, es);
        ExplainPropertyFloat("IO Read Time", INSTR_TIME_GET_MILLISEC(usage->blk_read_time), 3, es);
        ExplainPropertyFloat("IO Write Time", INSTR_TIME_GET_MILLISEC(usage->blk_write_time), 3, es);
    }
}

/*
 * Calculate the child plan's exclusive cpu cycles/inclusive cpu cycles/
 * left child processed rows/right child processedrows.
 */
template <bool datanode>
static void show_child_cpu_cycles_and_rows(PlanState* planstate, int idx, int smpIdx, double* outerCycles,
    double* innerCycles, uint64* outterRows, uint64* innerRows)
{
    PlanState* outerChild = NULL;
    PlanState* innerChild = NULL;
    Instrumentation* tmp_instr = NULL;
    Plan* plan = planstate->plan;
    int outer_smp_idx = 0;
    int inner_smp_idx = 0;

    switch (nodeTag(plan)) {
        case T_ModifyTable:
        case T_VecModifyTable:
            CalCPUMemberNode<datanode>(((ModifyTable*)plan)->plans,
                ((ModifyTableState*)planstate)->mt_plans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_VecAppend:
        case T_Append:
            CalCPUMemberNode<datanode>(((Append*)plan)->appendplans,
                ((AppendState*)planstate)->appendplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_MergeAppend:
            CalCPUMemberNode<datanode>(((MergeAppend*)plan)->mergeplans,
                ((MergeAppendState*)planstate)->mergeplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_BitmapAnd:
            CalCPUMemberNode<datanode>(((BitmapAnd*)plan)->bitmapplans,
                ((BitmapAndState*)planstate)->bitmapplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_BitmapOr:
            CalCPUMemberNode<datanode>(((BitmapOr*)plan)->bitmapplans,
                ((BitmapOrState*)planstate)->bitmapplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_CStoreIndexAnd:
            CalCPUMemberNode<datanode>(((CStoreIndexAnd*)plan)->bitmapplans,
                ((BitmapAndState*)planstate)->bitmapplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_CStoreIndexOr:
            CalCPUMemberNode<datanode>(((CStoreIndexOr*)plan)->bitmapplans,
                ((BitmapOrState*)planstate)->bitmapplans,
                idx,
                smpIdx,
                outerCycles,
                outterRows);
            break;
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            /* Left Tree Only */
            outerChild = ((SubqueryScanState*)planstate)->subplan;
            if (outerChild != NULL) {
                if (datanode) {
                    outer_smp_idx = outerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, outerChild->plan->plan_node_id, outer_smp_idx);
                } else
                    tmp_instr = planstate->instrument;
                if (tmp_instr != NULL) {
                    *outerCycles = tmp_instr->cpuusage.m_cycles;
                    *outterRows = tmp_instr->ntuples;
                }
            }
            break;
        case T_RemoteQuery:
        case T_VecRemoteQuery:
        case T_Stream:
        case T_VecStream:
            /* Treat them as scan nodes, because their left trees are in different thread. */
            break;

        default:
            /* Left Tree */
            outerChild = outerPlanState(planstate);
            if (outerChild != NULL) {
                if (datanode) {
                    outer_smp_idx = outerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, outerChild->plan->plan_node_id, outer_smp_idx);
                } else
                    tmp_instr = outerChild->instrument;
                if (tmp_instr != NULL) {
                    *outerCycles = tmp_instr->cpuusage.m_cycles;
                    *outterRows = tmp_instr->ntuples;
                }
            }
            /* Right Tree */
            innerChild = innerPlanState(planstate);
            if (innerChild != NULL) {
                if (datanode) {
                    inner_smp_idx = innerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, innerChild->plan->plan_node_id, inner_smp_idx);
                } else
                    tmp_instr = innerChild->instrument;
                if (tmp_instr != NULL) {
                    *innerCycles = tmp_instr->cpuusage.m_cycles;
                    *innerRows = tmp_instr->ntuples;
                }
            }
            break;
    }
}

/*
 * Calculate the cpu cycles of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the total cpu cycles
 * of the operator.
 */
template <bool datanode>
static void CalCPUMemberNode(const List* plans, PlanState** planstates, int idx, int smpIdx, double* Cycles, uint64* proRows)
{
    int nplans = list_length(plans);
    int k;
    Instrumentation* tmp_instr = NULL;

    for (k = 0; k < nplans; k++) {
        if (datanode) {
            int child_smp_idx = planstates[k]->plan->parallel_enabled ? smpIdx : 0;
            tmp_instr =
                u_sess->instr_cxt.global_instr->getInstrSlot(idx, planstates[k]->plan->plan_node_id, child_smp_idx);
        } else {
            tmp_instr = planstates[k]->instrument;
        }

        if (tmp_instr != NULL) {
            *Cycles += tmp_instr->cpuusage.m_cycles;
            *proRows += tmp_instr->ntuples;
        }
    }
}

template <bool datanode>
static void CalOperTimeMemberNode(const List* plans, PlanState** planstates, int idx, int smpIdx, double* oper_time)
{
    int nplans = list_length(plans);
    int k;
    Instrumentation* tmp_instr = NULL;

    for (k = 0; k < nplans; k++) {
        if (datanode) {
            int child_smp_idx = planstates[k]->plan->parallel_enabled ? smpIdx : 0;
            tmp_instr =
                u_sess->instr_cxt.global_instr->getInstrSlot(idx, planstates[k]->plan->plan_node_id, child_smp_idx);
        } else {
            tmp_instr = planstates[k]->instrument;
        }

        if (tmp_instr != NULL && tmp_instr->nloops > 0) {
            *oper_time += 1000.0 * tmp_instr->total;
        }
    }
}

/*
 * Calculate the child node's run time
 */
template <bool datanode>
static void show_child_time(
    PlanState* planstate, int idx, int smpIdx, double* inner_time, double* outer_time, ExplainState* es)
{
    PlanState* outerChild = NULL;
    PlanState* innerChild = NULL;
    Instrumentation* tmp_instr = NULL;
    Plan* plan = planstate->plan;
    int outer_smp_idx = 0;
    int inner_smp_idx = 0;

    switch (nodeTag(plan)) {
        case T_ModifyTable:
        case T_VecModifyTable:
            CalOperTimeMemberNode<datanode>(
                ((ModifyTable*)plan)->plans, ((ModifyTableState*)planstate)->mt_plans, idx, smpIdx, outer_time);
            break;
        case T_VecAppend:
        case T_Append:
            CalOperTimeMemberNode<datanode>(
                ((Append*)plan)->appendplans, ((AppendState*)planstate)->appendplans, idx, smpIdx, outer_time);
            break;
        case T_MergeAppend:
            CalOperTimeMemberNode<datanode>(
                ((MergeAppend*)plan)->mergeplans, ((MergeAppendState*)planstate)->mergeplans, idx, smpIdx, outer_time);
            break;
        case T_BitmapAnd:
            CalOperTimeMemberNode<datanode>(
                ((BitmapAnd*)plan)->bitmapplans, ((BitmapAndState*)planstate)->bitmapplans, idx, smpIdx, outer_time);
            break;
        case T_BitmapOr:
            CalOperTimeMemberNode<datanode>(
                ((BitmapOr*)plan)->bitmapplans, ((BitmapOrState*)planstate)->bitmapplans, idx, smpIdx, outer_time);
            break;
        case T_CStoreIndexAnd:
            CalOperTimeMemberNode<datanode>(((CStoreIndexAnd*)plan)->bitmapplans,
                ((BitmapAndState*)planstate)->bitmapplans,
                idx,
                smpIdx,
                outer_time);
            break;
        case T_CStoreIndexOr:
            CalOperTimeMemberNode<datanode>(
                ((CStoreIndexOr*)plan)->bitmapplans, ((BitmapOrState*)planstate)->bitmapplans, idx, smpIdx, outer_time);
            break;
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            /* Left Tree Only */
            outerChild = ((SubqueryScanState*)planstate)->subplan;
            if (outerChild != NULL) {
                if (datanode) {
                    outer_smp_idx = outerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, outerChild->plan->plan_node_id, outer_smp_idx);
                } else
                    tmp_instr = planstate->instrument;
                if (tmp_instr != NULL && tmp_instr->nloops > 0)
                    *outer_time = 1000.0 * tmp_instr->total;
            }
            break;
        case T_RemoteQuery:
        case T_VecRemoteQuery:

            // remove poll time
            *outer_time = planstate->instrument->network_perfdata.total_poll_time;
            if (es->planinfo->m_query_summary) {
                double deserialze_time = planstate->instrument->network_perfdata.total_deserialize_time;
                appendStringInfo(
                    es->planinfo->m_query_summary->info_str, "Remote query poll time: %.3f ms", *outer_time);
                appendStringInfo(
                    es->planinfo->m_query_summary->info_str, ", Deserialze time: %.3f ms\n", deserialze_time);
            }
            break;

        case T_Stream:
        case T_VecStream:

            /* remove poll time */
            if (datanode) {
                tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(idx, planstate->plan->plan_node_id, smpIdx);
                if (tmp_instr != NULL)
                    *outer_time = tmp_instr->network_perfdata.total_poll_time;
            }
            break;

        default:
            /* Left Tree */
            outerChild = outerPlanState(planstate);
            if (outerChild != NULL) {
                if (datanode) {
                    outer_smp_idx = outerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, outerChild->plan->plan_node_id, outer_smp_idx);
                } else {
                    InstrEndLoop(outerChild->instrument);
                    tmp_instr = outerChild->instrument;
                }

                if (tmp_instr != NULL && tmp_instr->nloops > 0) {
                    *outer_time = 1000.0 * tmp_instr->total;
                }
            }
            /* Right Tree */
            innerChild = innerPlanState(planstate);
            if (innerChild != NULL) {
                if (datanode) {
                    inner_smp_idx = innerChild->plan->parallel_enabled ? smpIdx : 0;
                    tmp_instr = u_sess->instr_cxt.global_instr->getInstrSlot(
                        idx, innerChild->plan->plan_node_id, inner_smp_idx);
                } else {
                    InstrEndLoop(innerChild->instrument);
                    tmp_instr = innerChild->instrument;
                }

                if (tmp_instr != NULL && tmp_instr->nloops > 0) {
                    *inner_time = 1000.0 * tmp_instr->total;
                }
            }
            break;
    }
}

inline static void show_cpu_info(StringInfo infostr, double incCycles, double exCycles, uint64 proRows)
{
    appendStringInfoString(infostr, "(CPU:");

    appendStringInfo(infostr, " ex c/r=%.0f,", proRows != 0 ? (long)(exCycles / proRows) : 0.0);
    appendStringInfo(infostr, " ex row=%lu,", proRows);
    appendStringInfo(infostr, " ex cyc=%.0f,", exCycles);
    appendStringInfo(infostr, " inc cyc=%.0f)", incCycles);
    appendStringInfoChar(infostr, '\n');
}

static void show_detail_cpu(ExplainState* es, PlanState* planstate)
{
    Instrumentation* instr = NULL;
    uint64 proRows = 0;
    double incCycles = 0.0;
    double exCycles = 0.0;
    double incCycles_max = -1;
    double outerCycles = 0.0;
    double innerCycles = 0.0;
    uint64 outterRows = 0;
    uint64 innerRows = 0;
    double incCycles_min = HUGE_VAL - 1;
    double exCycles_max = -1;
    double exCycles_min = HUGE_VAL - 1;
    double proRows_max = -1;
    double proRows_min = HUGE_VAL - 1;
    bool is_null = false;
    int i = 0;
    int j = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    if (u_sess->instr_cxt.global_instr->getInstruNodeNum() > 0) {
        for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
            for (j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                outerCycles = 0.0;
                innerCycles = 0.0;
                if (instr != NULL && instr->nloops > 0) {
                    if (!is_null)
                        is_null = true;

                    show_child_cpu_cycles_and_rows<true>(
                        planstate, i, j, &outerCycles, &innerCycles, &outterRows, &innerRows);

                    incCycles = instr->cpuusage.m_cycles;
                    exCycles = incCycles - outerCycles - innerCycles;
                    proRows = (long)(instr->ntuples / instr->nloops);

                    proRows = (proRows != 0 ? (long)(exCycles / proRows) : 0.0);
                    incCycles_max = rtl::max(incCycles_max, incCycles);
                    incCycles_min = rtl::min(incCycles_min, incCycles);
                    exCycles_max = rtl::max(exCycles_max, exCycles);
                    exCycles_min = rtl::min(exCycles_min, exCycles);
                    proRows_max = rtl::max((uint64)proRows_max, proRows);
                    proRows_min = rtl::min((uint64)proRows_min, proRows);
                }
            }
        }
    }

    if (is_null) {
        AssertEreport(
            (incCycles_max != -1 || exCycles_max != -1 || proRows_max != -1) &&
                (incCycles_min != HUGE_VAL - 1 || exCycles_min != HUGE_VAL - 1 || proRows_min != HUGE_VAL - 1),
            MOD_EXECUTOR,
            "unexpect min/max value");

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfoString(es->str, "CPU:");

            appendStringInfo(es->str, " max_ex c/r=%.0f,", proRows_max);
            appendStringInfo(es->str, " max_ex cyc=%.0f,", exCycles_max);
            appendStringInfo(es->str, " max_inc cyc=%.0f", incCycles_max);
            appendStringInfoChar(es->str, '\n');
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfoString(es->str, "    ");
            appendStringInfo(es->str, " min_ex c/r=%.0f,", proRows_min);
            appendStringInfo(es->str, " min_ex cyc=%.0f,", exCycles_min);
            appendStringInfo(es->str, " min_inc cyc=%.0f", incCycles_min);
            appendStringInfoChar(es->str, '\n');

            if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_IOInfo) {

                es->planinfo->m_IOInfo->set_plan_name<true, true>();
                appendStringInfoString(es->planinfo->m_IOInfo->info_str, "CPU:");

                appendStringInfo(es->planinfo->m_IOInfo->info_str, " max_ex c/r=%.0f,", proRows_max);
                appendStringInfo(es->planinfo->m_IOInfo->info_str, " max_ex cyc=%.0f,", exCycles_max);
                appendStringInfo(es->planinfo->m_IOInfo->info_str, " max_inc cyc=%.0f", incCycles_max);
                appendStringInfoChar(es->planinfo->m_IOInfo->info_str, '\n');
                appendStringInfoSpaces(es->planinfo->m_IOInfo->info_str, 11);
                appendStringInfo(es->planinfo->m_IOInfo->info_str, "  min_ex c/r=%.0f,", proRows_min);
                appendStringInfo(es->planinfo->m_IOInfo->info_str, "  min_ex cyc=%.0f,", exCycles_min);
                appendStringInfo(es->planinfo->m_IOInfo->info_str, "  min_inc cyc=%.0f", incCycles_min);
                appendStringInfoChar(es->planinfo->m_IOInfo->info_str, '\n');
            }
        } else {
            ExplainPropertyLong("Exclusive Max Cycles Per Row", (long)proRows_max, es);
            ExplainPropertyLong("Exclusive Max Cycles", (long)exCycles_max, es);
            ExplainPropertyLong("Inclusive Max Cycles", (long)incCycles_max, es);
            ExplainPropertyLong("Exclusive Min Cycles Per Row", (long)proRows_min, es);
            ExplainPropertyLong("Exclusive Min Cycles", (long)exCycles_min, es);
            ExplainPropertyLong("Inclusive Min Cycles", (long)incCycles_min, es);
        }
    } else if (es->analyze) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfo(es->str, "(CPU: never executed)");
            appendStringInfoChar(es->str, '\n');
        } else {
            ExplainPropertyFloat("Actual Exclusive Per Row", 0.0, 0, es);
            ExplainPropertyFloat("Actual Exclusive Cycles", 0.0, 0, es);
            ExplainPropertyFloat("Actual Inclusive Cycles", 0.0, 0, es);
        }
    }
}

// To determine whether the current operator is actually executed.
static bool get_execute_mode(const ExplainState* es, int idx)
{
    int id;
    if (!es->from_dn || !es->sql_execute)
        return true;
    id = idx < 0 ? 0 : idx;

    if (u_sess->instr_cxt.global_instr) {
        ThreadInstrumentation* threadinstr =
#ifdef ENABLE_MULTIPLE_NODES
            u_sess->instr_cxt.global_instr->getThreadInstrumentationCN(id);
#else
            u_sess->instr_cxt.global_instr->getThreadInstrumentationDN(1, 0);
#endif
        if (threadinstr == NULL)
            return true;
        bool execute = threadinstr->m_instrArray[0].instr.isExecute;

        if (!execute) {
            if (es->datanodeinfo.all_datanodes)
                return false;
            for (int i = 0; i < es->datanodeinfo.len_nodelist; i++) {
                if (id == es->datanodeinfo.node_index[i])
                    return false;
            }
        }
    }
    return true;
}

static void show_cpu(ExplainState* es, const Instrumentation* instrument, double innerCycles, double outerCycles, int nodeIdx,
    int smpIdx, uint64 proRows)
{
    if (instrument != NULL && instrument->nloops > 0) {
        double incCycles = 0.0;
        double exCycles = 0.0;
        int64 ex_cyc_rows;

        incCycles = instrument->cpuusage.m_cycles;
        exCycles = incCycles - outerCycles - innerCycles;

        ex_cyc_rows = proRows != 0 ? (long)(exCycles / proRows) : 0;

        if (es->planinfo && es->planinfo->m_runtimeinfo) {
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, EX_CYC, Int64GetDatum(exCycles));
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, INC_CYC, Int64GetDatum(incCycles));
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, EX_CYC_PER_ROWS, Int64GetDatum(ex_cyc_rows));
            show_cpu_info(es->planinfo->m_datanodeInfo->info_str, incCycles, exCycles, proRows);
            return;
        }

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            show_cpu_info(es->str, incCycles, exCycles, proRows);
        } else {
            ExplainPropertyLong("Exclusive Cycles Per Row", proRows != 0 ? (long)(exCycles / proRows) : 0, es);
            ExplainPropertyLong("Exclusive Cycles", (long)exCycles, es);
            ExplainPropertyLong("Inclusive Cycles", (long)incCycles, es);
        }
    } else if (es->analyze) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (get_execute_mode(es, nodeIdx))
                appendStringInfo(es->str, "(CPU: never executed)");
            else
                appendStringInfo(es->str, "(CPU: unknown)");
            appendStringInfoChar(es->str, '\n');
        } else {
            ExplainPropertyFloat("Actual Exclusive Cycles Per Row", 0.0, 0, es);
            ExplainPropertyFloat("Actual Exclusive Cycles", 0.0, 0, es);
            ExplainPropertyFloat("Actual Inclusive Cycles", 0.0, 0, es);
        }
    }
}

/*
 * @Description: show memory info about each plan node in pretty mode
 * @in es -	the explain state info
 * @in node_name - datanode name
 * @in instrument - instrument info
 * @in smpIdx - current smp idx
 * @in dop - query dop
 * @out - void
 */
static void show_pretty_memory(ExplainState* es, char* node_name, const Instrumentation* instrument, int smpIdx, int dop)
{
    es->planinfo->m_staticInfo->set_plan_name<true, true>();

    if (node_name != NULL) {
        es->planinfo->m_staticInfo->set_datanode_name(node_name, smpIdx, dop);
    }

    if (instrument->memoryinfo.peakOpMemory <= 1024)
        appendStringInfo(
            es->planinfo->m_staticInfo->info_str, "Peak Memory: %ldBYTE", instrument->memoryinfo.peakOpMemory);
    else
        appendStringInfo(
            es->planinfo->m_staticInfo->info_str, "Peak Memory: %ldKB", instrument->memoryinfo.peakOpMemory / 1024);

    if (instrument->memoryinfo.peakControlMemory > 0) {
        if (instrument->memoryinfo.peakControlMemory <= 1024)
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                ", Control Memory: %ldBYTE",
                instrument->memoryinfo.peakControlMemory);
        else
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                ", Control Memory: %ldKB",
                instrument->memoryinfo.peakControlMemory / 1024);
    }

    /* show operator memory information */
    if (instrument->memoryinfo.operatorMemory > 0) {
        if (instrument->memoryinfo.operatorMemory <= 1024)
            appendStringInfo(
                es->planinfo->m_staticInfo->info_str, ", Estimate Memory: %dKB", instrument->memoryinfo.operatorMemory);
        else
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                ", Estimate Memory: %dMB",
                instrument->memoryinfo.operatorMemory / 1024);
    }

    if (instrument->spreadNum > 0) {
        appendStringInfo(es->planinfo->m_staticInfo->info_str, ", Auto spread times: %d", instrument->spreadNum);
    }

    if (instrument->sysBusy) {
        appendStringInfo(es->planinfo->m_staticInfo->info_str, ", Early spilled");
    }

    if (instrument->width > 0) {
        appendStringInfo(es->planinfo->m_staticInfo->info_str, ", Width: %d", instrument->width);
    }

    appendStringInfo(es->planinfo->m_staticInfo->info_str, "\n");
}

static void show_pretty_time(
    ExplainState* es, Instrumentation* instrument, char* node_name, int nodeIdx, int smpIdx, int dop, bool executed)
{
    if (instrument != NULL && instrument->nloops > 0) {
        double nloops = instrument->nloops;
        const double startup_sec = 1000.0 * instrument->startup;
        const double total_sec = 1000.0 * instrument->total;
        double rows = instrument->ntuples;

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            char str[100] = "\0";
            int rc = sprintf_s(str, sizeof(str), "%.3f", total_sec);
            securec_check_ss(rc, "\0", "\0");
            es->planinfo->m_planInfo->put(ACTUAL_ROWS, Float8GetDatum(rows));
            if (es->timing) {
                es->planinfo->m_planInfo->put(ACTUAL_TIME, PointerGetDatum(cstring_to_text(str)));
            }
            char memoryPeakSize[100] = "\0";
            if (instrument->memoryinfo.peakOpMemory <= 1024)
                rc = sprintf_s(memoryPeakSize, sizeof(memoryPeakSize), "%ldBYTE", instrument->memoryinfo.peakOpMemory);
            else
                rc = sprintf_s(
                    memoryPeakSize, sizeof(memoryPeakSize), "%ldKB", instrument->memoryinfo.peakOpMemory / 1024);
            securec_check_ss(rc, "\0", "\0");

            es->planinfo->m_planInfo->put(ACTUAL_WIDTH, PointerGetDatum(cstring_to_text("")));
            es->planinfo->m_planInfo->put(ACTUAL_MEMORY, PointerGetDatum(cstring_to_text(memoryPeakSize)));

            if (es->planinfo->m_runtimeinfo) {
                es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, START_TIME, Float8GetDatum(startup_sec));
                es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, TOTAL_TIME, Float8GetDatum(total_sec));
                es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, PLAN_ROWS, rows);
                es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, PLAN_LOOPS, nloops);
                es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, PEAK_OP_MEMORY, instrument->memoryinfo.peakOpMemory);

                if (node_name != NULL)
                    es->planinfo->m_runtimeinfo->put(
                        nodeIdx, smpIdx, NODE_NAME, PointerGetDatum(cstring_to_text(node_name)));
                else
                    es->planinfo->m_runtimeinfo->put(
                        nodeIdx, smpIdx, NODE_NAME, PointerGetDatum(cstring_to_text("coordinator")));
            }

            if (es->detail) {
                es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                if (node_name != NULL) {
                    es->planinfo->m_datanodeInfo->set_datanode_name(node_name, smpIdx, dop);
                }

                if (instrument->need_timer)
                    appendStringInfo(es->planinfo->m_datanodeInfo->info_str,
                        "(actual time=%.3f..%.3f rows=%.0f loops=%.0f)\n",
                        startup_sec,
                        total_sec,
                        rows,
                        nloops);
                else
                    appendStringInfo(
                        es->planinfo->m_datanodeInfo->info_str, "(actual rows=%.0f loops=%.0f)\n", rows, nloops);

                show_pretty_memory(es, node_name, instrument, smpIdx, dop);
            }
        }
    } else if (es->planinfo->m_runtimeinfo) {
        if (node_name != NULL) {
            if (executed)
                es->planinfo->m_runtimeinfo->put(
                    nodeIdx, smpIdx, NODE_NAME, PointerGetDatum(cstring_to_text(node_name)));
        } else
            es->planinfo->m_runtimeinfo->put(
                nodeIdx, smpIdx, NODE_NAME, PointerGetDatum(cstring_to_text("coordinator")));
    }
}

template <bool isdetail>
static StreamTime* get_instrument(
    int j, int smpid, Track* trackpoint, int plan_node_id, StringInfo str, bool* first_time)
{
    StreamTime* instrument = NULL;

    int dop = u_sess->instr_cxt.global_instr->getNodeDop(plan_node_id);
#ifdef ENABLE_MULTIPLE_NODES
    char* node_name = PGXCNodeGetNodeNameFromId(j, PGXC_NODE_DATANODE);
#else
    char* node_name = g_instance.exec_cxt.nodeName;
#endif

    instrument = &trackpoint->track_time;

    if (*first_time) {
        if (trackpoint->node_id > 0)
            appendStringInfo(str,
                "Plan Node id: %d  Track name: %s\n",
                trackpoint->node_id,
                trackdesc[trackpoint->registerTrackId].trackName);

        /* nodeid == -1 means generaltrack */
        if (trackpoint->node_id == -1)
            appendStringInfo(str,
                "Segment Id: %d  Track name: %s\n",
                plan_node_id,
                trackdesc[trackpoint->registerTrackId].trackName);
        *first_time = false;
    }

    if (isdetail) {
        appendStringInfoSpaces(str, 6);

        if (dop == 1)
            appendStringInfo(str, " %s", node_name);
        else
            appendStringInfo(str, "%s[worker %d]", node_name, smpid);
    }

    StreamEndLoop(instrument);

    return instrument;
}

/* show general track info which is not related to plannodeid */
static void show_track_time_without_plannodeid(ExplainState* es)
{
    StringInfo str;
    StreamTime* instrument = NULL;
    int i = 0;
    int j = 0;
    bool has_perf = CPUMon::m_has_perf;
    AccumCounters accumcount;
    const char* track_name = NULL;
    bool first_time = true;
    int track_Id;

    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL)
        str = es->str;
    else
        str = es->planinfo->m_profileInfo->info_str;

    int m_num_streams = u_sess->instr_cxt.global_instr->getInstruThreadNum();
    int m_query_dop = u_sess->instr_cxt.global_instr->get_query_dop();
    int threadlen = u_sess->instr_cxt.global_instr->get_threadInstrArrayLen();
    int m_gather_count = u_sess->instr_cxt.global_instr->get_gather_num();

    ThreadInstrumentation* threadinstr = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(0);

    int generaltracknum = threadinstr->get_generaltracknum();

    // show generaltrack info
    for (i = 0; i < generaltracknum; i++) {
        track_Id = i;
        track_name = trackdesc[track_Id].trackName;

        ThreadInstrumentation* threadinstr_tmp = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(0);

        Track* generaltrack = &threadinstr_tmp->m_generalTrackArray[i];
        int segmentid = threadinstr_tmp->getSegmentId();
        if (segmentid > 0 && generaltrack->registerTrackId == track_Id && IS_NULL(generaltrack->active)) {
            instrument = &generaltrack->track_time;
            StreamEndLoop(instrument);
            if (instrument != NULL && instrument->nloops > 0) {
                double nloops = instrument->nloops;
                const double total_sec = 1000.0 * instrument->total;
                double rows = instrument->ntuples;

                appendStringInfo(str, "Segment Id: %d  Track name: %s\n", segmentid, track_name);
#ifdef ENABLE_MULTIPLE_NODES
                char* nodename = PGXCNodeGetNodeNameFromId(0, PGXC_NODE_COORDINATOR);
#else
                char* nodename = g_instance.exec_cxt.nodeName;
#endif
                appendStringInfoSpaces(str, 6);
                appendStringInfo(str, " %s:", nodename);

                if (instrument->need_timer) {
                    if (!has_perf) {
                        appendStringInfo(str, " (time=%.3f total_calls=%.0f loops=%.0f)", total_sec, rows, nloops);
                    } else {
                        accumcount = generaltrack->accumCounters;
                        appendStringInfo(str,
                            " (time=%.3f Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                            total_sec,
                            accumcount.accumCounters[1],
                            accumcount.accumCounters[0],
                            rows,
                            nloops);
                    }
                } else {
                    if (!has_perf) {
                        appendStringInfo(str, " (total_calls=%.0f loops=%.0f)", rows, nloops);
                    } else {
                        accumcount = generaltrack->accumCounters;
                        appendStringInfo(str,
                            " (Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                            accumcount.accumCounters[1],
                            accumcount.accumCounters[0],
                            rows,
                            nloops);
                    }
                }
                appendStringInfoChar(str, '\n');
            }
        }

        for (j = 1; j < threadlen; j++) {

            ThreadInstrumentation* threadinstr_tmp = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(j);

            if (threadinstr_tmp != NULL) {
                Track* generaltrack_tmp = &threadinstr_tmp->m_generalTrackArray[i];
                int segmentid_tmp = threadinstr_tmp->getSegmentId();
                if (segmentid_tmp > 0 && generaltrack_tmp->registerTrackId == track_Id && 
                    IS_NULL(generaltrack_tmp->active)) {
                    int nodeidx = (j - 1) / ((m_gather_count + m_num_streams) * m_query_dop);
                    int smpid = (j - 1) % m_query_dop;

                    instrument = get_instrument<true>(nodeidx, smpid, generaltrack_tmp, segmentid_tmp, str, 
                                                      &first_time);
                    if (instrument != NULL && instrument->nloops > 0) {
                        double nloops = instrument->nloops;
                        const double total_sec = 1000.0 * instrument->total;
                        double rows = instrument->ntuples;

                        if (instrument->need_timer) {
                            if (!has_perf) {
                                appendStringInfo(
                                    str, " (time=%.3f total_calls=%.0f loops=%.0f)", total_sec, rows, nloops);
                            } else {
                                accumcount = generaltrack_tmp->accumCounters;
                                appendStringInfo(str,
                                    " (time=%.3f Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                                    total_sec,
                                    accumcount.accumCounters[1],
                                    accumcount.accumCounters[0],
                                    rows,
                                    nloops);
                            }
                        } else {
                            if (!has_perf) {
                                appendStringInfo(str, " (total_calls=%.0f loops=%.0f)", rows, nloops);
                            } else {
                                accumcount = generaltrack_tmp->accumCounters;
                                appendStringInfo(str,
                                    " (Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                                    accumcount.accumCounters[1],
                                    accumcount.accumCounters[0],
                                    rows,
                                    nloops);
                            }
                        }
                        appendStringInfoChar(str, '\n');
                    }
                }
            }
        }
    }
}

/* show all track info */
static void show_track_time_info(ExplainState* es)
{
    StreamTime* instrument = NULL;
    Track* trackArray = NULL;
    bool first_time = true;
    int i = 0;
    int j = 0;
    StringInfo str;
    int plan_node_id;
    AccumCounters accumcount;
    const char* track_name = NULL;
    bool has_perf = CPUMon::m_has_perf;
    int track_Id;

    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
        str = es->str;
        appendStringInfo(str, "*********User Define Profiling*********\n");
    } else
        str = es->planinfo->m_profileInfo->info_str;

    int m_num_streams = u_sess->instr_cxt.global_instr->getInstruThreadNum();
    int m_query_dop = u_sess->instr_cxt.global_instr->get_query_dop();
    int plannodes_num = u_sess->instr_cxt.global_instr->getInstruPlanSize() + 1;
    int m_gather_count = u_sess->instr_cxt.global_instr->get_gather_num();

    ThreadInstrumentation* threadinstr = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(0);

    int tracknum = threadinstr->get_tracknum();
    int generaltracknum = threadinstr->get_generaltracknum();
    int dn_start_node_id = u_sess->instr_cxt.global_instr->get_startnodeid();

    if (es->detail) {
        /* show generaltrackinfo */
        show_track_time_without_plannodeid(es);

        /* show nodetrack info */
        for (i = 0; i < tracknum; i++) {
            track_Id = i + generaltracknum;
            track_name = trackdesc[track_Id].trackName;
            /* print the track info on coordinator  */
            ThreadInstrumentation* threadinstr_tmp = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(0);
            int trackArrayLen = threadinstr_tmp->get_instrArrayLen();

            for (j = 0; j < trackArrayLen; j++) {
                trackArray = threadinstr_tmp->m_instrArray[j].tracks;
                Track* trackpoint = &trackArray[i];
                plan_node_id = trackpoint->node_id;
                if (plan_node_id > 0 && trackpoint->registerTrackId == track_Id && IS_NULL(trackpoint->active)) {
                    instrument = &trackpoint->track_time;
                    StreamEndLoop(instrument);
                    if (instrument != NULL && instrument->nloops > 0) {
                        double nloops = instrument->nloops;
                        const double total_sec = 1000.0 * instrument->total;
                        double rows = instrument->ntuples;

                        appendStringInfo(str, "Plan Node id: %d  Track name: %s\n", plan_node_id, track_name);
#ifdef ENABLE_MULTIPLE_NODES
                        char* nodename = PGXCNodeGetNodeNameFromId(0, PGXC_NODE_COORDINATOR);
#else
                        char* nodename = g_instance.exec_cxt.nodeName;
#endif
                        appendStringInfoSpaces(str, 6);
                        appendStringInfo(str, " %s:", nodename);
                        if (instrument->need_timer) {
                            if (!has_perf) {
                                appendStringInfo(
                                    str, " (time=%.3f total_calls=%.0f loops=%.0f)", total_sec, rows, nloops);
                            } else {
                                accumcount = trackpoint->accumCounters;
                                appendStringInfo(str,
                                    " (time=%.3f Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                                    total_sec,
                                    accumcount.accumCounters[1],
                                    accumcount.accumCounters[0],
                                    rows,
                                    nloops);
                            }

                        } else {
                            if (!has_perf) {
                                appendStringInfo(str, " (total_calls=%.0f loops=%.0f)", rows, nloops);
                            } else {
                                accumcount = trackpoint->accumCounters;
                                appendStringInfo(str,
                                    " (Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                                    accumcount.accumCounters[1],
                                    accumcount.accumCounters[0],
                                    rows,
                                    nloops);
                            }
                        }
                        appendStringInfoChar(str, '\n');
                    }
                }
            }
        }

        /* print DN track info */
        for (j = dn_start_node_id; j < plannodes_num; j++) {
            for (i = 0; i < tracknum; i++) {
                first_time = true;
                track_Id = i + generaltracknum;
                for (int k = 0; k < u_sess->instr_cxt.global_instr->getInstruNodeNum(); k++) {
                    for (int m = 0; m < u_sess->instr_cxt.global_instr->getNodeDop(j); m++) {
                        ThreadInstrumentation* threadinstr_tmp =
                            u_sess->instr_cxt.global_instr->getThreadInstrumentation(k, j, m);
                        if (threadinstr_tmp != NULL) {
                            trackArray = threadinstr_tmp->get_tracks(j);
                            /* when m_instrArrayMap is initialized but nodeinstr is not executed, it should return. */
                            if (trackArray == NULL)
                                continue;
                            Track* trackpoint = &trackArray[i];
                            plan_node_id = trackpoint->node_id;
                            if (plan_node_id > 0 && trackpoint->registerTrackId == track_Id &&
                                IS_NULL(trackpoint->active)) {
                                instrument = get_instrument<true>(k, m, trackpoint, plan_node_id, str, &first_time);
                                if (instrument != NULL && instrument->nloops > 0) {
                                    double nloops = instrument->nloops;
                                    const double total_sec = 1000.0 * instrument->total;
                                    double rows = instrument->ntuples;

                                    if (instrument->need_timer) {
                                        if (!has_perf)
                                            appendStringInfo(str,
                                                " (time=%.3f total_calls=%.0f loops=%.0f)",
                                                total_sec,
                                                rows,
                                                nloops);
                                        else {
                                            accumcount = trackpoint->accumCounters;
                                            appendStringInfo(str,
                                                " (time=%.3f Instructions=%ld  cpu cycles=%ld total_calls=%.0f "
                                                "loops=%.0f)",
                                                total_sec,
                                                accumcount.accumCounters[1],
                                                accumcount.accumCounters[0],
                                                rows,
                                                nloops);
                                        }
                                    } else {
                                        if (!has_perf)
                                            appendStringInfo(str, " (total_calls=%.0f loops=%.0f)", rows, nloops);
                                        else {
                                            accumcount = trackpoint->accumCounters;
                                            appendStringInfo(str,
                                                " (Instructions=%ld  cpu cycles=%ld total_calls=%.0f loops=%.0f)",
                                                accumcount.accumCounters[1],
                                                accumcount.accumCounters[0],
                                                rows,
                                                nloops);
                                        }
                                    }
                                    appendStringInfoChar(str, '\n');
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        /* show generaltrack */
        for (i = 0; i < generaltracknum; i++) {
            double total_sec_max = -1;
            double total_sec_min = HUGE_VAL - 1;
            bool need_timer = false;
            double max_calls = -1;
            double min_calls = HUGE_VAL - 1;
            double total_sec;
            double calls = 0;
            int64 max_stru = -1;
            int64 min_stru = LONG_MAX;
            int64 max_cpu_cycle = -1;
            int64 min_cpu_cycle = LONG_MAX;

            /* generaltrack */
            track_Id = i;
            first_time = true;

            for (j = 0; j < u_sess->instr_cxt.global_instr->get_threadInstrArrayLen(); j++) {
                ThreadInstrumentation* threadinstr_tmp = u_sess->instr_cxt.global_instr->get_cnthreadinstrumentation(j);

                if (threadinstr_tmp != NULL) {
                    Track* trackpoint = &threadinstr_tmp->m_generalTrackArray[i];
                    int segmentid = threadinstr_tmp->getSegmentId();
                    if (segmentid > 0 && trackpoint->registerTrackId == track_Id && IS_NULL(trackpoint->active)) {
                        int nodeidx = (j == 0 ? 0 : (j - 1) / ((m_gather_count + m_num_streams) * m_query_dop));
                        int smpid = (j == 0 ? 0 : (j - 1) % m_query_dop);

                        instrument = get_instrument<false>(nodeidx, smpid, trackpoint, segmentid, str, &first_time);
                        if (instrument != NULL && instrument->nloops > 0) {
                            if (!need_timer)
                                need_timer = instrument->need_timer;

                            calls = instrument->ntuples;

                            max_calls = rtl::max(max_calls, calls);
                            min_calls = rtl::min(min_calls, calls);
                            total_sec = 1000.0 * instrument->total;
                            total_sec_max = rtl::max(total_sec_max, total_sec);
                            total_sec_min = rtl::min(total_sec_min, total_sec);
                        }

                        if (has_perf) {
                            accumcount = trackpoint->accumCounters;

                            max_stru = rtl::max(max_stru, accumcount.accumCounters[1]);
                            min_stru = rtl::min(min_stru, accumcount.accumCounters[1]);

                            max_cpu_cycle = rtl::max(max_cpu_cycle, accumcount.accumCounters[0]);
                            min_cpu_cycle = rtl::min(min_cpu_cycle, accumcount.accumCounters[0]);
                        }
                    }
                }
            }

            if (need_timer && es->format == EXPLAIN_FORMAT_TEXT) {
                AssertEreport(
                    total_sec_max != -1 && total_sec_min != HUGE_VAL - 1, MOD_EXECUTOR, "unexpect min/max value");

                if (es->format == EXPLAIN_FORMAT_TEXT) {
                    if (!has_perf)
                        appendStringInfo(str,
                            " (actual time=[%.3f, %.3f], calls=[%.0f, %.0f])\n",
                            total_sec_min,
                            total_sec_max,
                            min_calls,
                            max_calls);
                    else
                        appendStringInfo(str,
                            " (actual time=[%.3f, %.3f], Instructions=[%ld, %ld], cpu cycles=[%ld, %ld], calls=[%.0f, "
                            "%.0f])\n",
                            total_sec_min,
                            total_sec_max,
                            min_stru,
                            max_stru,
                            min_cpu_cycle,
                            max_cpu_cycle,
                            min_calls,
                            max_calls);
                }
            }
        }

        /* show nodetrack for every plannodeid */
        for (j = 1; j < plannodes_num; j++) {
            for (i = 0; i < tracknum; i++) {
                double total_sec_max = -1;
                double total_sec_min = HUGE_VAL - 1;
                bool need_timer = false;
                double max_calls = -1;
                double min_calls = HUGE_VAL - 1;
                double total_sec;
                double calls = 0;
                int64 max_stru = -1;
                int64 min_stru = LONG_MAX;
                int64 max_cpu_cycle = -1;
                int64 min_cpu_cycle = LONG_MAX;

                first_time = true;
                track_Id = i + generaltracknum;
                for (int k = 0; k < u_sess->instr_cxt.global_instr->getInstruNodeNum(); k++) {
                    for (int m = 0; m < u_sess->instr_cxt.global_instr->getNodeDop(j); m++) {
                        ThreadInstrumentation* threadinstr_tmp =
                            u_sess->instr_cxt.global_instr->getThreadInstrumentation(k, j, m);
                        if (threadinstr_tmp != NULL) {
                            Track* trackArray_tmp = threadinstr_tmp->get_tracks(j);
                            /* when m_instrArrayMap is initialized but nodeinstr is not executed, it should return. */
                            if (trackArray_tmp == NULL)
                                continue;
                            Track* trackpoint = &trackArray_tmp[i];
                            plan_node_id = trackpoint->node_id;
                            if (plan_node_id > 0 && trackpoint->registerTrackId == track_Id &&
                                IS_NULL(trackpoint->active)) {
                                instrument = get_instrument<false>(k, m, trackpoint, plan_node_id, str, &first_time);
                                if (instrument != NULL && instrument->nloops > 0) {
                                    if (!need_timer)
                                        need_timer = instrument->need_timer;

                                    calls = instrument->ntuples;

                                    max_calls = rtl::max(max_calls, calls);
                                    min_calls = rtl::min(min_calls, calls);
                                    total_sec = 1000.0 * instrument->total;
                                    total_sec_max = rtl::max(total_sec_max, total_sec);
                                    total_sec_min = rtl::min(total_sec_min, total_sec);
                                }

                                if (has_perf) {
                                    accumcount = trackpoint->accumCounters;

                                    max_stru = rtl::max(max_stru, accumcount.accumCounters[1]);
                                    min_stru = rtl::min(min_stru, accumcount.accumCounters[1]);

                                    max_cpu_cycle = rtl::max(max_cpu_cycle, accumcount.accumCounters[0]);
                                    min_cpu_cycle = rtl::min(min_cpu_cycle, accumcount.accumCounters[0]);
                                }
                            }
                        }
                    }
                }

                if (need_timer && es->format == EXPLAIN_FORMAT_TEXT) {
                    AssertEreport(
                        total_sec_max != -1 && total_sec_min != HUGE_VAL - 1, MOD_EXECUTOR, "unexpect min/max value");

                    if (es->format == EXPLAIN_FORMAT_TEXT) {
                        if (!has_perf)
                            appendStringInfo(str,
                                " (actual time=[%.3f, %.3f], calls=[%.0f, %.0f])\n",
                                total_sec_min,
                                total_sec_max,
                                min_calls,
                                max_calls);
                        else
                            appendStringInfo(str,
                                " (actual time=[%.3f, %.3f], Instructions=[%ld, %ld], cpu cycles=[%ld, %ld], "
                                "calls=[%.0f, %.0f])\n",
                                total_sec_min,
                                total_sec_max,
                                min_stru,
                                max_stru,
                                min_cpu_cycle,
                                max_cpu_cycle,
                                min_calls,
                                max_calls);
                    }
                }
            }
        }
    }
}

int get_track_time(ExplainState* es, PlanState* planstate, bool show_track, bool show_buffer, bool show_dummygroup,
    bool show_indexinfo, bool show_storageinfo)
{
    if (show_track)
        show_track_time_info(es);
    if (show_buffer) {
        append_datanode_name(es, "datanode1", 2, 1);
        show_analyze_buffers(es, planstate, es->str, 1);
        show_buffers(es, es->str, u_sess->instr_cxt.global_instr->getInstrSlot(0, 1), false, 0, 0, NULL);
    }
    if (show_dummygroup) {
        ExplainDummyGroup("ut", "test", es);
        ExplainDummyGroup("ut", NULL, es);
    }

    if (show_indexinfo) {
        ExplainIndexScanDetails(0, BackwardScanDirection, es);
        ExplainIndexScanDetails(0, NoMovementScanDirection, es);
        ExplainIndexScanDetails(0, ForwardScanDirection, es);
    }

    if (show_storageinfo) {
        append_datanode_name(es, "datanode1", 2, 1);
        show_detail_storage_info_json(u_sess->instr_cxt.global_instr->getInstrSlot(0, 1), es->str, es);
    }
    return 0;
}

template <bool is_detail>
static void show_time(ExplainState* es, const Instrumentation* instrument, int idx)
{
    if (instrument != NULL && instrument->nloops > 0) {
        double nloops = instrument->nloops;
        const double startup_sec = 1000.0 * instrument->startup;
        const double total_sec = 1000.0 * instrument->total;
        double rows = instrument->ntuples;
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (is_detail == false)
                appendStringInfoSpaces(es->str, 1);
            if (instrument->need_timer)
                appendStringInfo(
                    es->str, "(actual time=%.3f..%.3f rows=%.0f loops=%.0f)", startup_sec, total_sec, rows, nloops);
            else
                appendStringInfo(es->str, "(actual rows=%.0f loops=%.0f)", rows, nloops);
        } else {
            if (instrument->need_timer) {
                ExplainPropertyFloat("Actual Startup Time", startup_sec, 3, es);
                ExplainPropertyFloat("Actual Total Time", total_sec, 3, es);
            }

            ExplainPropertyFloat("Actual Rows", rows, 0, es);
            ExplainPropertyFloat("Actual Loops", nloops, 0, es);
        }
    } else if (es->analyze) {
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (is_detail == false)
                appendStringInfoSpaces(es->str, 1);
            if (get_execute_mode(es, idx))
                appendStringInfo(es->str, "(Actual time: never executed)");
            else
                appendStringInfo(es->str, "(Actual time: unknown)");
        } else if (instrument != NULL && instrument->need_timer) {
            ExplainPropertyFloat("Actual Startup Time", 0.0, 3, es);
            ExplainPropertyFloat("Actual Total Time", 0.0, 3, es);
        } else {
            ExplainPropertyFloat("Actual Rows", 0.0, 0, es);
            ExplainPropertyFloat("Actual Loops", 0.0, 0, es);
        }
    }

    /* in text format, first line ends here */
    if (es->format == EXPLAIN_FORMAT_TEXT)
        appendStringInfoChar(es->str, '\n');
}

/*
 * @Description: get max plan id from a plannedstmt
 * @in value -a integer
 * @out int - digit of value
 */
static int get_digit(int value)
{
    AssertEreport(value >= 0, MOD_EXECUTOR, "unexpect input value");
    int result = 0;
    while (value > 0) {
        result++;
        value /= 10;
    }

    if (result == 0) {
        return 1;
    }
    return result;
}

template <bool datanode>
static void get_oper_time(ExplainState* es, PlanState* planstate, const Instrumentation* instr, int nodeIdx, int smpIdx)
{
    double outer_total_time = 0.0;
    double inner_total_time = 0.0;
    double oper_time;

    show_child_time<datanode>(planstate, nodeIdx, smpIdx, &inner_total_time, &outer_total_time, es);
    if (instr != NULL && instr->nloops > 0) {
        oper_time = 1000.0 * instr->total;
        oper_time = oper_time - outer_total_time - inner_total_time;

        es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, OPER_TIME, Float8GetDatum(oper_time));
    }
}

/*
 * @Description: show time info about stream send data to other thread
 * @in es -  the explain state info
 * @in planstate - current planstate
 * @out - void
 */
static void show_stream_send_time(ExplainState* es, const PlanState* planstate)
{
    bool isSend = false;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    /* stream send time will print only es->detail is true and t_thrd.explain_cxt.explain_perf_mode is not normal */
    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL || es->detail == false)
        return;

    /* is current plan node is the top node */
    isSend = u_sess->instr_cxt.global_instr->IsSend(0, planstate->plan->plan_node_id, 0);
    if (isSend == false) {
        return;
    }

    for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
        ThreadInstrumentation* threadinstr =
            u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
        if (threadinstr == NULL)
            continue;
#ifdef ENABLE_MULTIPLE_NODES
        char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
        char* node_name = g_instance.exec_cxt.nodeName;
#endif
        for (int j = 0; j < dop; j++) {
            Instrumentation* instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
            if (instr != NULL && instr->stream_senddata.loops == true) {
                double send_time = instr->stream_senddata.total_send_time;
                double wait_quota_time = instr->stream_senddata.total_wait_quota_time;
                double os_send_time = instr->stream_senddata.total_os_send_time;
                double serialize_time = instr->stream_senddata.total_serialize_time;
                double copy_time = instr->stream_senddata.total_copy_time;

                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                es->planinfo->m_staticInfo->set_datanode_name(node_name, j, dop);

                appendStringInfo(es->planinfo->m_staticInfo->info_str, "Stream Send time: %.3f", send_time);

                /* Only SCTP mode can collect quota and os send info. */
                if (wait_quota_time > 0.0)
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, ", Wait Quota time: %.3f", wait_quota_time);
                if (os_send_time > 0.0)
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, ", OS Kernel Send time: %.3f", os_send_time);

                /* Local Stream use data copy instead of serialize. */
                if (copy_time > 0.0)
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, ", Data Copy time: %.3f", copy_time);
                else
                    appendStringInfo(
                        es->planinfo->m_staticInfo->info_str, "; Data Serialize time: %.3f", serialize_time);

                appendStringInfoChar(es->planinfo->m_staticInfo->info_str, '\n');
            }
        }
    }
}
static void show_datanode_time(ExplainState* es, PlanState* planstate)
{
    Instrumentation* instr = NULL;

    double total_sec_max = -1;
    double total_sec_min = HUGE_VAL - 1;
    double start_sec_max = -1;
    double start_sec_min = HUGE_VAL - 1;
    bool need_timer = false;
    double rows = 0;
    double total_sec;
    double start_sec;
    int64 peak_memory_min = (int64)(0x6FFFFFFFFFFFFFFF);
    int64 peak_memory_max = 0;
    int width_min = (int)(0x6FFFFFFF);
    int width_max = 0;
    bool executed = true;
    int i = 0;
    int j = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    if (es->detail) {
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoChar(es->str, '\n');
        ExplainOpenGroup("Actual In Detail", "Actual In Detail", false, es);
        for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
            char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
            ThreadInstrumentation* threadinstr = u_sess->instr_cxt.global_instr->getThreadInstrumentationCN(i);
#else
            char* node_name = g_instance.exec_cxt.nodeName;
            ThreadInstrumentation* threadinstr = u_sess->instr_cxt.global_instr->getThreadInstrumentationDN(1, 0);
#endif
            if (threadinstr == NULL)
                continue;
            executed = threadinstr->m_instrArray[0].instr.isExecute;

            for (j = 0; j < dop; j++) {
                ExplainOpenGroup("Plan", NULL, true, es);
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr == NULL)
                    continue;
                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
                    show_pretty_time(es, instr, node_name, i, j, dop, executed);
                    get_oper_time<true>(es, planstate, instr, i, j);

                    if (instr->needRCInfo) {
                        es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                        es->planinfo->m_datanodeInfo->set_datanode_name(node_name, j, dop);
                        ShowRoughCheckInfo(es, instr, i, j);
                    }

                    if (instr != NULL && instr->nloops > 0) {
                        if (!need_timer)
                            need_timer = instr->need_timer;

                        rows += instr->ntuples;
                        total_sec = 1000.0 * instr->total;
                        start_sec = 1000.0 * instr->startup;
                        total_sec_max = rtl::max(total_sec_max, total_sec);
                        total_sec_min = rtl::min(total_sec_min, total_sec);
                        start_sec_max = rtl::max(start_sec_max, start_sec);
                        start_sec_min = rtl::min(start_sec_min, start_sec);
                        peak_memory_min = rtl::min(peak_memory_min, instr->memoryinfo.peakOpMemory);
                        peak_memory_max = rtl::max(peak_memory_max, instr->memoryinfo.peakOpMemory);
                        width_min = rtl::min(width_min, instr->width);
                        width_max = rtl::max(width_max, instr->width);
                    }
                } else {
                    append_datanode_name(es, node_name, dop, j);
                    show_time<true>(es, instr, i);
                    ShowRoughCheckInfo(es, instr, i, j);
                }
                ExplainCloseGroup("Plan", NULL, true, es);
            }
        }
        ExplainCloseGroup("Actual In Detail", "Actual In Detail", false, es);
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->analyze) {
                char str[100] = "\0";
                int rc = 0;
                if (need_timer)
                    rc = sprintf_s(str, sizeof(str), "[%.3f,%.3f]", total_sec_min, total_sec_max);
                else
                    rc = sprintf_s(str, sizeof(str), "[0,0]");
                securec_check_ss(rc, "\0", "\0");
                es->planinfo->m_planInfo->put(ACTUAL_ROWS, Float8GetDatum(rows));
                if (es->timing) {
                    es->planinfo->m_planInfo->put(ACTUAL_TIME, PointerGetDatum(cstring_to_text(str)));
                }
                char width[50] = "\0";
                if (width_max > 0) {
                    rc = sprintf_s(width, sizeof(width), "[%d,%d]", width_min, width_max);
                    securec_check_ss(rc, "\0", "\0");
                }
                es->planinfo->m_planInfo->put(ACTUAL_WIDTH, PointerGetDatum(cstring_to_text(width)));

                if (es->planinfo->m_planInfo->m_query_mem_mode) {
                    char opmem[50] = "\0";
                    if (planstate->plan->operatorMemKB[0] > 0) {
                        if (planstate->plan->operatorMemKB[0] < 1024)
                            rc = sprintf_s(opmem, sizeof(opmem), "%dKB", (int)planstate->plan->operatorMemKB[0]);
                        else {
                            if (planstate->plan->operatorMemKB[0] > MIN_OP_MEM &&
                                planstate->plan->operatorMaxMem > planstate->plan->operatorMemKB[0])
                                rc = sprintf_s(opmem,
                                    sizeof(opmem),
                                    "%dMB(%dMB)",
                                    (int)planstate->plan->operatorMemKB[0] / 1024,
                                    (int)planstate->plan->operatorMaxMem / 1024);
                            else
                                rc = sprintf_s(
                                    opmem, sizeof(opmem), "%dMB", (int)planstate->plan->operatorMemKB[0] / 1024);
                        }
                        securec_check_ss(rc, "\0", "\0");
                    }
                    es->planinfo->m_planInfo->put(ESTIMATE_MEMORY, PointerGetDatum(cstring_to_text(opmem)));
                }

                char memoryPeakMinSize[100] = "\0";
                if (peak_memory_min < 1024)
                    rc = sprintf_s(memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldBYTE", peak_memory_min);
                else if (peak_memory_min < 1024 * 1024)
                    rc = sprintf_s(memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldKB", peak_memory_min / 1024);
                else
                    rc = sprintf_s(
                        memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldMB", peak_memory_min / (1024 * 1024));
                securec_check_ss(rc, "\0", "\0");

                char memoryPeakMaxSize[100] = "\0";
                if (peak_memory_max < 1024)
                    rc = sprintf_s(memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldBYTE", peak_memory_max);
                else if (peak_memory_max < 1024 * 1024)
                    rc = sprintf_s(memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldKB", peak_memory_max / 1024);
                else
                    rc = sprintf_s(
                        memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldMB", peak_memory_max / (1024 * 1024));
                securec_check_ss(rc, "\0", "\0");

                char memoryStr[256] = "\0";
                if (need_timer)
                    rc = sprintf_s(memoryStr, sizeof(memoryStr), "[%s, %s]", memoryPeakMinSize, memoryPeakMaxSize);
                else
                    rc = sprintf_s(memoryStr, sizeof(memoryStr), "[0, 0]");
                securec_check_ss(rc, "\0", "\0");
                es->planinfo->m_planInfo->put(ACTUAL_MEMORY, PointerGetDatum(cstring_to_text(memoryStr)));
            }
        }
    } else {
        if (u_sess->instr_cxt.global_instr->getInstruNodeNum() > 0) {
            for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                ThreadInstrumentation* threadinstr =
                    u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
                if (threadinstr == NULL)
                    continue;
                for (j = 0; j < dop; j++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                    if (instr != NULL && instr->nloops > 0) {
                        if (!need_timer)
                            need_timer = true;

                        rows += instr->ntuples;
                        total_sec = 1000.0 * instr->total;
                        start_sec = 1000.0 * instr->startup;
                        total_sec_max = rtl::max(total_sec_max, total_sec);
                        total_sec_min = rtl::min(total_sec_min, total_sec);
                        start_sec_max = rtl::max(start_sec_max, start_sec);
                        start_sec_min = rtl::min(start_sec_min, start_sec);
                        peak_memory_min = rtl::min(peak_memory_min, instr->memoryinfo.peakOpMemory);
                        peak_memory_max = rtl::max(peak_memory_max, instr->memoryinfo.peakOpMemory);
                        width_min = rtl::min(width_min, instr->width);
                        width_max = rtl::max(width_max, instr->width);
                    }
                }
            }
        }

        if (need_timer) {
            AssertEreport((total_sec_max != -1 || start_sec_max != -1) &&
                              (total_sec_min != HUGE_VAL - 1 || start_sec_min != HUGE_VAL - 1),
                MOD_EXECUTOR,
                "unexpect min/max value");

            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (es->timing)
                    appendStringInfo(es->str,
                        " (actual time=[%.3f,%.3f]..[%.3f,%.3f], rows=%.0f)",
                        start_sec_min,
                        total_sec_min,
                        start_sec_max,
                        total_sec_max,
                        rows);
                else
                    appendStringInfo(es->str, " (actual rows=%.0f)", rows);

                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->analyze) {
                    char str[100] = "\0";
                    int rc = sprintf_s(str, sizeof(str), "[%.3f,%.3f]", total_sec_min, total_sec_max);
                    securec_check_ss(rc, "\0", "\0");
                    es->planinfo->m_planInfo->put(ACTUAL_ROWS, Float8GetDatum(rows));
                    if (es->timing) {
                        es->planinfo->m_planInfo->put(ACTUAL_TIME, PointerGetDatum(cstring_to_text(str)));
                    }
                    char width[50] = "\0";
                    if (width_max > 0) {
                        rc = sprintf_s(width, sizeof(width), "[%d,%d]", width_min, width_max);
                        securec_check_ss(rc, "\0", "\0");
                    }
                    es->planinfo->m_planInfo->put(ACTUAL_WIDTH, PointerGetDatum(cstring_to_text(width)));

                    if (es->planinfo->m_planInfo->m_query_mem_mode) {
                        char opmem[50] = "\0";
                        if (planstate->plan->operatorMemKB[0] > 0) {
                            if (planstate->plan->operatorMemKB[0] < 1024)
                                rc = sprintf_s(opmem, sizeof(opmem), "%dKB", (int)planstate->plan->operatorMemKB[0]);
                            else {
                                if (planstate->plan->operatorMemKB[0] > MIN_OP_MEM &&
                                    planstate->plan->operatorMaxMem > planstate->plan->operatorMemKB[0])
                                    rc = sprintf_s(opmem,
                                        sizeof(opmem),
                                        "%dMB(%dMB)",
                                        (int)planstate->plan->operatorMemKB[0] / 1024,
                                        (int)planstate->plan->operatorMaxMem / 1024);
                                else
                                    rc = sprintf_s(
                                        opmem, sizeof(opmem), "%dMB", (int)planstate->plan->operatorMemKB[0] / 1024);
                            }
                            securec_check_ss(rc, "\0", "\0");
                        }
                        es->planinfo->m_planInfo->put(ESTIMATE_MEMORY, PointerGetDatum(cstring_to_text(opmem)));
                    }

                    char memoryPeakMinSize[100] = "\0";
                    if (peak_memory_min < 1024)
                        rc = sprintf_s(memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldBYTE", peak_memory_min);
                    else if (peak_memory_min < 1024 * 1024)
                        rc = sprintf_s(memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldKB", peak_memory_min / 1024);
                    else
                        rc = sprintf_s(
                            memoryPeakMinSize, sizeof(memoryPeakMinSize), "%ldMB", peak_memory_min / (1024 * 1024));
                    securec_check_ss(rc, "\0", "\0");

                    char memoryPeakMaxSize[100] = "\0";
                    if (peak_memory_max < 1024)
                        rc = sprintf_s(memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldBYTE", peak_memory_max);
                    else if (peak_memory_max < 1024 * 1024)
                        rc = sprintf_s(memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldKB", peak_memory_max / 1024);
                    else
                        rc = sprintf_s(
                            memoryPeakMaxSize, sizeof(memoryPeakMaxSize), "%ldMB", peak_memory_max / (1024 * 1024));
                    securec_check_ss(rc, "\0", "\0");

                    char memoryStr[256] = "\0";
                    rc = sprintf_s(memoryStr, sizeof(memoryStr), "[%s,%s]", memoryPeakMinSize, memoryPeakMaxSize);
                    securec_check_ss(rc, "\0", "\0");
                    es->planinfo->m_planInfo->put(ACTUAL_MEMORY, PointerGetDatum(cstring_to_text(memoryStr)));
                }
            } else {
                /* Do not show time information when timing is off */
                if (es->timing) {
                    ExplainPropertyFloat("Actual Min Startup Time", start_sec_min, 3, es);
                    ExplainPropertyFloat("Actual Max Startup Time", start_sec_max, 3, es);
                    ExplainPropertyFloat("Actual Min Total Time", total_sec_min, 3, es);
                    ExplainPropertyFloat("Actual Max Total Time", total_sec_max, 3, es);
                }

                ExplainPropertyFloat("Actual Total Rows", rows, 0, es);
            }
        } else if (es->analyze) {
            if (es->format == EXPLAIN_FORMAT_TEXT) {
                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
                    char str[100] = "\0";
                    int rc = sprintf_s(str, sizeof(str), "[0,0]");
                    securec_check_ss(rc, "\0", "\0");
                    es->planinfo->m_planInfo->put(ACTUAL_ROWS, Float8GetDatum(rows));
                    if (es->timing) {
                        es->planinfo->m_planInfo->put(ACTUAL_TIME, PointerGetDatum(cstring_to_text(str)));
                    }
                    char memoryStr[100] = "\0";
                    rc = sprintf_s(memoryStr, sizeof(memoryStr), "[0, 0]");
                    securec_check_ss(rc, "\0", "\0");
                    es->planinfo->m_planInfo->put(ACTUAL_MEMORY, PointerGetDatum(cstring_to_text(memoryStr)));
                } else
                    appendStringInfo(es->str, " (Actual time: never executed)");
            } else {
                ExplainPropertyFloat("Actual Startup Time", 0.0, 3, es);
                ExplainPropertyFloat("Actual Total Time", 0.0, 3, es);
            }
        }

        /* in text format, first line ends here */
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoChar(es->str, '\n');
    }

    return;
}

static void show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es)
{
    if (es->format != EXPLAIN_FORMAT_TEXT)
    {
        ExplainPropertyLong("Exact Heap Blocks", planstate->exact_pages, es);
        ExplainPropertyLong("Lossy Heap Blocks", planstate->lossy_pages, es);
    } else {
        if (planstate->exact_pages > 0 || planstate->lossy_pages > 0) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfoString(es->str, "Heap Blocks:");
            if (planstate->exact_pages > 0) {
                appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
            }
            if (planstate->lossy_pages > 0) {
                appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
            }
            appendStringInfoChar(es->str, '\n');
        }
    }
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
static void show_instrumentation_count(const char* qlabel, int which, const PlanState* planstate, ExplainState* es)
{
    double nfiltered = 0.0;
    Instrumentation* instr = NULL;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

    if (!es->analyze || !planstate->instrument)
        return;

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
            ThreadInstrumentation* threadinstr =
                u_sess->instr_cxt.global_instr->getThreadInstrumentation(i, planstate->plan->plan_node_id, 0);
            if (threadinstr == NULL)
                continue;
            for (int j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                if (instr != NULL && instr->nloops > 0) {
                    if (which == 1)
                        nfiltered += instr->nfiltered1;
                    else if (which == 2)
                        nfiltered += instr->nfiltered2;
                }
            }
        }
    } else {
        if (which == 1)
            nfiltered = planstate->instrument->nfiltered1;
        else if (which == 2)
            nfiltered = planstate->instrument->nfiltered2;
    }

    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL &&
        (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)) {
        ExplainPropertyFloat(qlabel, nfiltered, 0, es);
    }

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_detailInfo) {
        if (nfiltered > 0 && es->format == EXPLAIN_FORMAT_TEXT) {
            char buf[512];
            int rc = 0;
            rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%.*f", 0, nfiltered);
            securec_check_ss(rc, "\0", "\0");
            es->planinfo->m_detailInfo->set_plan_name<false, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s: %s\n", qlabel, buf);
        }
    }
}

/*
 * Show removed rows by filters.
 */
static void show_removed_rows(int which, const PlanState* planstate, int idx, int smpIdx, int* removeRows)
{
    Instrumentation* instr = NULL;

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        instr = u_sess->instr_cxt.global_instr->getInstrSlot(idx, planstate->plan->plan_node_id, smpIdx);
        if (instr != NULL && instr->nloops > 0) {
            if (which == 1) {
                *removeRows = check_integer_overflow(instr->nfiltered1);
            } else if (which == 2) {
                *removeRows = check_integer_overflow(instr->nfiltered2);
            }
        }
    } else {
        if (which == 1) {
            *removeRows = check_integer_overflow(planstate->instrument->nfiltered1);
        } else if (which == 2) {
            *removeRows = check_integer_overflow(planstate->instrument->nfiltered2);
        }
    }
}

/*
 * Check for possible integer overflow when assign a double variable to an int variable.
 */
static int check_integer_overflow(double var)

{
    if (var > (double)PG_INT32_MAX || var < (double)PG_INT32_MIN) {
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmodule(MOD_OPT),
            errmsg("Integer overflow."),
            errdetail("Integer overflow occurred when assigning a double variable to an int variable."),
            errcause("Try to assign a double variable to an int variable."),
            erraction("Please check whether the double variable exceeds the representation range of int.")));
    }
    return (int)var;
}

/*
 * Show extra information for a ForeignScan node.
 */
static void show_foreignscan_info(ForeignScanState* fsstate, ExplainState* es)
{
    FdwRoutine* fdwroutine = fsstate->fdwroutine;

    /* Let the FDW emit whatever fields it wants */
    if (NULL != fdwroutine && NULL != fdwroutine->ExplainForeignScan)
        fdwroutine->ExplainForeignScan(fsstate, es);
}

/*
 * @Description: check whether Instrumentation object has
 *               bloomfilter info to display about storage.
 * @IN instr: Instrumentation object
 * @Return: true if needed to display bloomfilter info; otherwise false.
 * @See also:
 */
static inline bool storage_has_bloomfilter_info(const Instrumentation* instr)
{
    return (instr->bloomFilterRows > 0);
}

/*
 * @Description: check whether Instrumentation object has
 *               min/max info to display about storage.
 * @IN instr: Instrumentation object
 * @Return: true if needed to display minmax filter info; otherwise false.
 * @See also:
 */
static inline bool storage_has_minmax_filter(const Instrumentation* instr)
{
    return (instr->minmaxFilterFiles > 0 || instr->minmaxFilterStripe > 0 || instr->minmaxFilterStride > 0 ||
            instr->minmaxFilterRows > 0);
}

/*
 * @Description: check whether Instrumentation object has
 *               pruned-partition info to display about storage.
 * @IN instr: Instrumentation object
 * @Return: true if needed to display pruned-partition info; otherwise false.
 * @See also:
 */
static inline bool storage_has_pruned_info(const Instrumentation* instr)
{
    return (instr->dynamicPrunFiles > 0 || instr->staticPruneFiles > 0);
}

/*
 * @Description: check whether Instrumentation object has
 *               orc data cache info to display about storage.
 * @IN instr: Instrumentation object
 * @Return: true if needed to display orc data cache info; otherwise false.
 * @See also:
 */
static inline bool storage_has_cached_info(const Instrumentation* instr)
{
    return (instr->orcMetaCacheBlockCount > 0 || instr->orcMetaLoadBlockCount > 0 ||
            instr->orcDataCacheBlockCount > 0 || instr->orcDataLoadBlockCount > 0);
}

/*
 * @Description: append dfs block info to str
 * @in planstate - current plan state info
 * @in es - explain state info
 * @return - void
 */
static void append_dfs_block_info(
    ExplainState* es, double localRatio, double metaCacheRatio, double dataCacheRatio, Instrumentation* instr)
{
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfo(es->str,
            "(local read ratio: %.1f%s, local: %.0f, remote: %.0f)",
            localRatio * 100,
            "%",
            instr->localBlock,
            instr->remoteBlock);
        appendStringInfo(
            es->str, " (nn intersection count: %lu, dn intersection count: %lu)", instr->nnCalls, instr->dnCalls);

        appendStringInfo(es->str,
            " (meta cache: hit ratio %.1f%s, hit[count %lu, size %lu], read[count %lu, size %lu]",
            metaCacheRatio * 100,
            "%",
            instr->orcMetaCacheBlockCount,
            instr->orcMetaCacheBlockSize,
            instr->orcMetaLoadBlockCount,
            instr->orcMetaLoadBlockSize);
        appendStringInfo(es->str,
            " data cache: hit ratio %.1f%s, hit[count %lu, size %lu], read[count %lu, size %lu])",
            dataCacheRatio * 100,
            "%",
            instr->orcDataCacheBlockCount,
            instr->orcDataCacheBlockSize,
            instr->orcDataLoadBlockCount,
            instr->orcDataLoadBlockSize);
        appendStringInfo(es->str, "\n");
    } else {
        ExplainPropertyFloat("local read ratio", localRatio * 100, 1, es);
        ExplainPropertyFloat("local block", instr->localBlock, 0, es);
        ExplainPropertyFloat("remote block", instr->remoteBlock, 0, es);
        ExplainPropertyLong("nn intersection count", instr->nnCalls, es);
        ExplainPropertyLong("dn intersection count", instr->dnCalls, es);
        ExplainPropertyFloat("meta cache hit ratio", metaCacheRatio * 100, 1, es);
        ExplainPropertyLong("meta cache hit count", instr->orcMetaCacheBlockCount, es);
        ExplainPropertyLong("meta cache hit size", instr->orcMetaCacheBlockSize, es);
        ExplainPropertyLong("meta cache read count", instr->orcMetaLoadBlockCount, es);
        ExplainPropertyLong("meta cache read size", instr->orcMetaLoadBlockSize, es);
        ExplainPropertyFloat("data cache hit ratio", dataCacheRatio * 100, 1, es);
        ExplainPropertyLong("data cache hit count", instr->orcDataCacheBlockCount, es);
        ExplainPropertyLong("data cache hit size", instr->orcDataCacheBlockSize, es);
        ExplainPropertyLong("data cache read count", instr->orcDataLoadBlockCount, es);
        ExplainPropertyLong("data cache read size", instr->orcDataLoadBlockSize, es);
    }
}

/*
 * @Description: print dfs info in detail=off
 * @in planstate - current plan state info
 * @in es - explain state info
 * @return - void
 */
static void show_analyze_dfs_info(const PlanState* planstate, ExplainState* es)
{
    Instrumentation* instr = NULL;
    int i = 0;
    int j = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
    double total_local_block = 0.0;
    double total_remote_block = 0.0;
    double total_datacache_block_count = 0.0;
    double total_metacache_block_count = 0.0;
    double total_metaload_block_count = 0.0;
    double total_dataload_block_count = 0.0;

    for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
        for (j = 0; j < dop; j++) {
            instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
            if (instr != NULL) {
                total_local_block += instr->localBlock;
                total_remote_block += instr->remoteBlock;
                total_datacache_block_count += instr->orcDataCacheBlockCount;
                total_metacache_block_count += instr->orcMetaCacheBlockCount;
                total_dataload_block_count += instr->orcDataLoadBlockCount;
                total_metaload_block_count += instr->orcMetaLoadBlockCount;
            }
        }
    }

    if (total_local_block > 0 || total_remote_block > 0 || total_datacache_block_count > 0 ||
        total_metacache_block_count > 0 || total_metaload_block_count || total_dataload_block_count) {
        double localRatio = (total_local_block + total_remote_block == 0)
                                ? 0
                                : (total_local_block / (total_local_block + total_remote_block));
        double metaCacheRatio =
            (total_metacache_block_count + total_metaload_block_count == 0)
                ? 0
                : (double)total_metacache_block_count / (total_metacache_block_count + total_metaload_block_count);
        double dataCacheRatio =
            (total_datacache_block_count + total_dataload_block_count == 0)
                ? 0
                : (double)total_datacache_block_count / (total_dataload_block_count + total_datacache_block_count);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_staticInfo) {
            es->planinfo->m_staticInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_staticInfo->info_str, "(local read ratio: %.1f%s,", localRatio * 100, "%");
            appendStringInfo(
                es->planinfo->m_staticInfo->info_str, " meta cache hit ratio: %.1f%s,", metaCacheRatio * 100, "%");
            appendStringInfo(
                es->planinfo->m_staticInfo->info_str, " data cache hit ratio %.1f%s)\n", dataCacheRatio * 100, "%");
        }

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfo(es->str, "(local read ratio: %.1f%s,", localRatio * 100, "%");
            appendStringInfo(es->str, " meta cache hit ratio: %.1f%s,", metaCacheRatio * 100, "%");
            appendStringInfo(es->str, " data cache hit ratio: %.1f%s)\n", dataCacheRatio * 100, "%");
        } else {
            ExplainPropertyFloat("local read ratio", localRatio, 1, es);
            ExplainPropertyFloat("meta cache hit ratio", metaCacheRatio, 1, es);
            ExplainPropertyFloat("data cache hit ratio", dataCacheRatio, 1, es);
        }
    }
}

static void show_dfs_block_info(PlanState* planstate, ExplainState* es)
{
    AssertEreport(planstate != NULL && es != NULL, MOD_EXECUTOR, "unexpect null value");

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;
        int i = 0;
        int j = 0;
        bool has_info = false;
        int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

        if (es->detail) {
            for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                for (j = 0; j < dop; j++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                    if (instr == NULL)
                        continue;
                    bool blockOrHasCachedInfo = instr->localBlock > 0 || instr->remoteBlock > 0 ||
                        storage_has_cached_info(instr);
                    if (blockOrHasCachedInfo) {
                        if (has_info == false)
                            ExplainOpenGroup("Dfs Block Detail", "Dfs Block Detail", false, es);
                        has_info = true;
                        ExplainOpenGroup("Plan", NULL, true, es);
                        double localRatio = instr->localBlock / (instr->localBlock + instr->remoteBlock);
                        double metaCacheRatio =
                            (instr->orcMetaCacheBlockCount + instr->orcMetaLoadBlockCount == 0)
                                ? 0
                                : (double)instr->orcMetaCacheBlockCount /
                                      (instr->orcMetaCacheBlockCount + instr->orcMetaLoadBlockCount);
                        double dataCacheRatio =
                            (instr->orcDataCacheBlockCount + instr->orcDataLoadBlockCount == 0)
                                ? 0
                                : (double)instr->orcDataCacheBlockCount /
                                      (instr->orcDataCacheBlockCount + instr->orcDataLoadBlockCount);

                        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_runtimeinfo) {
                            es->planinfo->m_runtimeinfo->put(i, 0, DFS_BLOCK_INFO, BoolGetDatum(true));
                            es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                            es->planinfo->m_datanodeInfo->set_datanode_name(node_name, j, dop);

                            appendStringInfo(es->planinfo->m_datanodeInfo->info_str,
                                "(local read ratio: %.1f%s, local: %.0f, remote: %.0f)",
                                localRatio * 100,
                                "%",
                                instr->localBlock,
                                instr->remoteBlock);

                            /* as for the obs foreign table, we do not have the datanode and namenode. */
                            if (instr->nnCalls != 0 && instr->dnCalls != 0) {
                                appendStringInfo(es->planinfo->m_datanodeInfo->info_str,
                                    " (nn intersection count: %lu, dn intersection count: %lu)",
                                    instr->nnCalls,
                                    instr->dnCalls);
                            }

                            appendStringInfo(es->planinfo->m_datanodeInfo->info_str,
                                " (meta cache: hit ratio %.1f%s, hit[count %lu, size %lu], read[count %lu, size %lu]",
                                metaCacheRatio * 100,
                                "%",
                                instr->orcMetaCacheBlockCount,
                                instr->orcMetaCacheBlockSize,
                                instr->orcMetaLoadBlockCount,
                                instr->orcMetaLoadBlockSize);
                            appendStringInfo(es->planinfo->m_datanodeInfo->info_str,
                                " data cache: hit ratio %.1f%s, hit[count %lu, size %lu], read[count %lu, size %lu])",
                                dataCacheRatio * 100,
                                "%",
                                instr->orcDataCacheBlockCount,
                                instr->orcDataCacheBlockSize,
                                instr->orcDataLoadBlockCount,
                                instr->orcDataLoadBlockSize);
                            appendStringInfo(es->planinfo->m_datanodeInfo->info_str, "\n");
                            continue;
                        }

                        append_datanode_name(es, node_name, dop, j);
                        append_dfs_block_info(es, localRatio, metaCacheRatio, dataCacheRatio, instr);
                        ExplainCloseGroup("Plan", NULL, true, es);
                    }
                }
            }
            if (has_info)
                ExplainCloseGroup("Dfs Block Detail", "Dfs Block Detail", false, es);
        } else
            show_analyze_dfs_info(planstate, es);
    }
}

/*
 * @Description: print filter info in detail in storage level
 * @in instr - instrument info
 * @in instr_info - the string to be append
 * @return - void
 */
static void show_detail_storage_info_json_of_dfs(Instrumentation* instr, StringInfo instr_info, ExplainState* es)
{
    if (storage_has_bloomfilter_info(instr)) {
        ExplainPropertyLong("rows skipped by bloom filter", instr->bloomFilterRows, es);
        if (instr->bloomFilterBlocks > 0) {
            ExplainPropertyLong("include strides) ", instr->bloomFilterBlocks, es);
        }
    }

    if (storage_has_minmax_filter(instr)) {
        if (instr->minmaxFilterRows > 0) {
            ExplainPropertyLong("min max skip rows", instr->minmaxFilterRows, es);
        }
        if (instr->minmaxFilterStride > 0) {
            ExplainPropertyLong("min max filter strides", instr->minmaxFilterStride, es);
            ExplainPropertyLong("min max check strides", instr->minmaxCheckStride, es);
        }
        if (instr->minmaxFilterStripe > 0) {
            ExplainPropertyLong("min max filter stripes", instr->minmaxFilterStripe, es);
            ExplainPropertyLong("min max check stripes", instr->minmaxCheckStripe, es);
        }
        if (instr->minmaxFilterFiles > 0) {
            ExplainPropertyLong("min max filter files", instr->minmaxFilterFiles, es);
            ExplainPropertyLong("min max check files", instr->minmaxCheckFiles, es);
        }
    }

    if (storage_has_pruned_info(instr)) {
        if (instr->staticPruneFiles > 0) {
            ExplainPropertyLong("pruned files static", instr->staticPruneFiles, es);
        }
        if (instr->dynamicPrunFiles > 0) {
            ExplainPropertyLong("pruned files dynamic", instr->dynamicPrunFiles, es);
        }
    }
}

static void show_detail_storage_info_json_of_logft(const Instrumentation* instr, StringInfo instr_info, ExplainState* es)
{
    if (instr->minmaxFilterStripe > 0) {
        ExplainPropertyText("refuted by hostname", "true", es);
    }
    if (instr->minmaxCheckStride > 0) {
        ExplainPropertyLong("dirname total", instr->minmaxCheckStride, es);
        if (instr->minmaxFilterStride > 0) {
            ExplainPropertyLong("dirname refuted", instr->minmaxFilterStride, es);
            ExplainPropertyLong("dirname scan", (instr->minmaxCheckStride - instr->minmaxFilterStride), es);
        }
    }
    if (instr->minmaxCheckFiles > 0) {
        ExplainPropertyLong("filename total", instr->minmaxCheckFiles, es);
        if (instr->minmaxFilterFiles > 0) {
            ExplainPropertyLong("filename refuted", instr->minmaxFilterFiles, es);
            ExplainPropertyLong("filename scan", (instr->minmaxCheckFiles - instr->minmaxFilterFiles), es);
        }
        if (instr->staticPruneFiles > 0) {
            ExplainPropertyLong("filename incompleted", instr->staticPruneFiles, es);
        }
        if (instr->dynamicPrunFiles) {
            ExplainPropertyLong("filename latest files", instr->dynamicPrunFiles, es);
        }
    }
}

static void show_detail_storage_info_json(Instrumentation* instr, StringInfo instr_info, ExplainState* es)
{
    if (instr->dfsType == TYPE_LOG_FT) {
        show_detail_storage_info_json_of_logft(instr, instr_info, es);
    } else {
        show_detail_storage_info_json_of_dfs(instr, instr_info, es);
    }
}

static void show_detail_storage_info_text_of_dfs(Instrumentation* instr, StringInfo instr_info)
{
    if (storage_has_bloomfilter_info(instr)) {
        appendStringInfo(instr_info, "(skip %lu rows by bloom filter", instr->bloomFilterRows);
        if (instr->bloomFilterBlocks > 0) {
            appendStringInfo(instr_info, " include %lu strides) ", instr->bloomFilterBlocks);
        } else {
            appendStringInfo(instr_info, ") ");
        }
    }

    if (storage_has_minmax_filter(instr)) {
        appendStringInfo(instr_info, "(min max skip:");
        if (instr->minmaxFilterRows > 0) {
            appendStringInfo(instr_info, " rows %lu", instr->minmaxFilterRows);
        }
        if (instr->minmaxFilterStride > 0) {
            appendStringInfo(instr_info, " strides %lu/%lu", instr->minmaxFilterStride, instr->minmaxCheckStride);
        }
        if (instr->minmaxFilterStripe > 0) {
            appendStringInfo(instr_info, " stripes %lu/%lu", instr->minmaxFilterStripe, instr->minmaxCheckStripe);
        }
        if (instr->minmaxFilterFiles > 0) {
            appendStringInfo(instr_info, " files %lu/%lu", instr->minmaxFilterFiles, instr->minmaxCheckFiles);
        }
        appendStringInfo(instr_info, ")");
    }

    if (storage_has_pruned_info(instr)) {
        appendStringInfo(instr_info, "(pruned files:");
        if (instr->staticPruneFiles > 0) {
            appendStringInfo(instr_info, " static %lu", instr->staticPruneFiles);
        }
        if (instr->dynamicPrunFiles > 0) {
            appendStringInfo(instr_info, " dynamic %lu", instr->dynamicPrunFiles);
        }
        appendStringInfo(instr_info, ")");
    }
    appendStringInfo(instr_info, "\n");
}

static void show_detail_storage_info_text_of_logft(const Instrumentation* instr, StringInfo instr_info)
{
    if (instr->minmaxFilterStripe > 0) {
        appendStringInfo(instr_info, "(refuted by hostname)");
    }
    if (instr->minmaxCheckStride > 0) {
        appendStringInfo(instr_info, "(dirname: total %lu", instr->minmaxCheckStride);
        if (instr->minmaxFilterStride > 0) {
            appendStringInfo(instr_info,
                ", refuted %lu, scan %lu",
                instr->minmaxFilterStride,
                (instr->minmaxCheckStride - instr->minmaxFilterStride));
        }
        appendStringInfo(instr_info, ")");
    }
    if (instr->minmaxCheckFiles > 0) {
        appendStringInfo(instr_info, "(filename: total %lu", instr->minmaxCheckFiles);
        if (instr->minmaxFilterFiles > 0) {
            appendStringInfo(instr_info,
                ", refuted %lu, scan %lu",
                instr->minmaxFilterFiles,
                (instr->minmaxCheckFiles - instr->minmaxFilterFiles));
        }
        if (instr->staticPruneFiles > 0) {
            appendStringInfo(instr_info, ", incompleted %lu", instr->staticPruneFiles);
        }
        if (instr->dynamicPrunFiles) {
            appendStringInfo(instr_info, ", latest files %lu", instr->dynamicPrunFiles);
        }
        appendStringInfo(instr_info, ")");
    }
    appendStringInfo(instr_info, "\n");
}

static void show_detail_storage_info_text(Instrumentation* instr, StringInfo instr_info)
{
    if (instr->dfsType == TYPE_LOG_FT) {
        show_detail_storage_info_text_of_logft(instr, instr_info);
    } else {
        /* DFS type */
        show_detail_storage_info_text_of_dfs(instr, instr_info);
    }
}

/*
 * @Description: print storage info in detail=off
 * @in planstate - current plan state info
 * @in es - explain state info
 * @return - void
 */
static void show_analyze_storage_info_of_dfs(const PlanState* planstate, ExplainState* es)
{
    int i = 0;
    int j = 0;
    uint64 total_bloomfilter_rows = 0;
    uint64 total_dynamicfiles = 0;
    uint64 total_staticFiles = 0;
    Instrumentation* instr = NULL;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
    for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
        for (j = 0; j < dop; j++) {
            instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
            if (instr != NULL) {
                total_bloomfilter_rows += instr->bloomFilterRows;
                total_dynamicfiles += instr->dynamicPrunFiles;
                total_staticFiles += instr->staticPruneFiles;
            }
        }
    }

    if (total_bloomfilter_rows > 0 || total_dynamicfiles > 0 || total_staticFiles > 0) {
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_staticInfo) {
            es->planinfo->m_staticInfo->set_plan_name<true, true>();
            appendStringInfo(
                es->planinfo->m_staticInfo->info_str, "(skip %lu rows by bloom filter,", total_bloomfilter_rows);
            appendStringInfo(es->planinfo->m_staticInfo->info_str,
                " pruned files: static %lu, dynamic %lu)\n",
                total_dynamicfiles,
                total_staticFiles);
        }

        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfo(es->str, "(skip %lu rows by bloom filter,", total_bloomfilter_rows);
            appendStringInfo(
                es->str, " pruned files: static %lu, dynamic %lu)\n", total_dynamicfiles, total_staticFiles);
        } else {
            ExplainPropertyLong("rows skipped by bloom filter", total_bloomfilter_rows, es);
            ExplainPropertyLong("pruned files static", total_dynamicfiles, es);
            ExplainPropertyLong("pruned files dynamic", total_staticFiles, es);
        }
    }
}

static void show_analyze_storage_info_of_logft(const PlanState* planstate, ExplainState* es)
{
    uint64 total_hostname = 0;
    uint64 total_refuted_by_hostname = 0;
    uint64 total_dirname = 0;
    uint64 total_refuted_by_dirname = 0;
    uint64 total_filename = 0;
    uint64 total_refuted_by_filename = 0;
    uint64 total_incompleted = 0;
    int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;
    Instrumentation* instr = NULL;

    int node_idx = 0;
    int dop_idx = 0;
    for (node_idx = 0; node_idx < u_sess->instr_cxt.global_instr->getInstruNodeNum(); node_idx++) {
        for (dop_idx = 0; dop_idx < dop; dop_idx++) {
            instr = u_sess->instr_cxt.global_instr->getInstrSlot(node_idx, planstate->plan->plan_node_id, dop_idx);
            if (instr != NULL) {
                total_hostname += instr->minmaxCheckStripe;
                total_refuted_by_hostname += instr->minmaxFilterStripe;
                total_dirname += instr->minmaxCheckStride;
                total_refuted_by_dirname += instr->minmaxFilterStride;
                total_filename += instr->minmaxCheckFiles;
                total_refuted_by_filename += instr->minmaxFilterFiles;
                total_incompleted += instr->staticPruneFiles;
            }
        }
    }

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_staticInfo) {
        es->planinfo->m_staticInfo->set_plan_name<true, true>();
        appendStringInfo(es->planinfo->m_staticInfo->info_str,
            "(nodes refuted %lu, total %lu)",
            total_refuted_by_hostname,
            total_hostname);
        appendStringInfo(es->planinfo->m_staticInfo->info_str,
            "(dirname refuted %lu, total %lu)",
            total_refuted_by_dirname,
            total_dirname);
        appendStringInfo(es->planinfo->m_staticInfo->info_str,
            "(filename refuted %lu, total %lu, incompleted %lu)\n",
            total_refuted_by_filename,
            total_filename,
            total_incompleted);
    }

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfoSpaces(es->str, es->indent * 2);
        appendStringInfo(es->str, "(nodes refuted  %lu, total %lu)", total_refuted_by_hostname, total_hostname);
        appendStringInfo(es->str, "(dirname refuted %lu, total %lu)", total_refuted_by_dirname, total_dirname);
        appendStringInfo(es->str,
            "(filename refuted %lu, total %lu, incompleted %lu)\n",
            total_refuted_by_filename,
            total_filename,
            total_incompleted);
    } else {
        ExplainPropertyLong("nodes refuted", total_refuted_by_hostname, es);
        ExplainPropertyLong("nodes total", total_hostname, es);
        ExplainPropertyLong("dirname refuted", total_refuted_by_dirname, es);
        ExplainPropertyLong("dirname total", total_dirname, es);
        ExplainPropertyLong("filename refuted", total_refuted_by_filename, es);
        ExplainPropertyLong("filename total", total_filename, es);
        ExplainPropertyLong("filename incompleted", total_incompleted, es);
    }
}

static void show_analyze_storage_info(PlanState* planstate, ExplainState* es)
{
    if (u_sess->instr_cxt.global_instr->getInstruNodeNum() > 0) {
        Instrumentation* instr = u_sess->instr_cxt.global_instr->getInstrSlot(0, planstate->plan->plan_node_id, 0);
        if (instr && instr->dfsType == TYPE_LOG_FT) {
            show_analyze_storage_info_of_logft(planstate, es);
        } else {
            show_analyze_storage_info_of_dfs(planstate, es);
        }
    }
}

static void show_storage_filter_info(PlanState* planstate, ExplainState* es)
{
    AssertEreport(planstate != NULL && es != NULL, MOD_EXECUTOR, "unexpect null value");

    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;
        int i = 0;
        int j = 0;
        bool has_info = false;
        int dop = planstate->plan->parallel_enabled ? planstate->plan->dop : 1;

        if (es->detail) {
            for (i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                for (j = 0; j < dop; j++) {
                    instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id, j);
                    if (instr == NULL)
                        continue;
                    if (storage_has_bloomfilter_info(instr) || storage_has_minmax_filter(instr) ||
                        storage_has_pruned_info(instr)) {
                        if (has_info == false)
                            ExplainOpenGroup("Storage Detail", "Storage Detail", false, es);
                        has_info = true;
                        ExplainOpenGroup("Plan", NULL, true, es);
                        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_runtimeinfo) {
                            es->planinfo->m_runtimeinfo->put(i, 0, BLOOM_FILTER_INFO, true);

                            es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                            es->planinfo->m_datanodeInfo->set_datanode_name(node_name, j, dop);
                            show_detail_storage_info_text(instr, es->planinfo->m_datanodeInfo->info_str);
                            continue;
                        }

                        append_datanode_name(es, node_name, dop, j);
                        if (es->format == EXPLAIN_FORMAT_TEXT)
                            show_detail_storage_info_text(instr, es->str);
                        else
                            show_detail_storage_info_json(instr, es->str, es);
                        ExplainCloseGroup("Plan", NULL, true, es);
                    }
                }
            }
            if (has_info)
                ExplainCloseGroup("Storage Detail", "Storage Detail", false, es);
        } else
            show_analyze_storage_info(planstate, es);
    }
}

static void show_wlm_explain_name(ExplainState* es, const char* plan_name, const char* pname, int plan_node_id)
{
    if (plan_name != NULL) {
        appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
        appendStringInfoString(es->str, " | ");
        appendStringInfoSpaces(es->str, es->indent);
        appendStringInfo(es->str, "%s\n", plan_name);
        es->indent++;
    }

    int digit = get_digit(plan_node_id);
    appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit - digit);
    appendStringInfo(es->str, "%d | ", plan_node_id);

    if (es->indent) {
        appendStringInfoSpaces(es->str, es->indent);
        appendStringInfoString(es->str, "->  ");
    }
    appendStringInfoString(es->str, pname);

    es->indent++;
}

/*
 * Show extra information of MERGE for a ModifyTable node
 */
static void show_modifytable_merge_info(const PlanState* planstate, ExplainState* es)
{
    if (planstate->plan->plan_node_id > 0 && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.global_instr->isFromDataNode(planstate->plan->plan_node_id)) {
        Instrumentation* instr = NULL;

        if (es->detail) {
            ExplainOpenGroup("Merge Detail", "Merge Detail", false, es);
            for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
                ExplainOpenGroup("Plan", NULL, true, es);
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, planstate->plan->plan_node_id);
                if (instr == NULL)
                    continue;
#ifdef ENABLE_MULTIPLE_NODES
                char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
                char* node_name = g_instance.exec_cxt.nodeName;
#endif
                append_datanode_name(es, node_name, 1, 0);

                if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_runtimeinfo) {
                    es->planinfo->m_runtimeinfo->put(i, 0, MERGE_INSERTED, instr->nfiltered1);
                    es->planinfo->m_runtimeinfo->put(i, 0, MERGE_UPDATED, instr->nfiltered2);

                    es->planinfo->m_datanodeInfo->set_plan_name<false, true>();
                    appendStringInfo(es->planinfo->m_datanodeInfo->info_str, "%s ", node_name);

                    appendStringInfo(
                        es->planinfo->m_datanodeInfo->info_str, "(Merge: inserted %.0f, ", instr->nfiltered1);
                    appendStringInfo(es->planinfo->m_datanodeInfo->info_str, "updated %.0f)\n", instr->nfiltered2);
                    continue;
                }

                if (es->format == EXPLAIN_FORMAT_TEXT) {
                    appendStringInfo(es->str, "(Merge: inserted %.0f, ", instr->nfiltered1);
                    appendStringInfo(es->str, "updated %.0f)\n", instr->nfiltered2);
                } else {
                    ExplainPropertyFloat("Merge Inserted", instr->nfiltered1, 0, es);
                    ExplainPropertyFloat("Merge Updated", instr->nfiltered2, 0, es);
                }

                ExplainCloseGroup("Plan", NULL, true, es);
            }
            ExplainCloseGroup("Merge Detail", "Merge Detail", false, es);
        }
    }
}

static void ShowRoughCheckInfo(ExplainState* es, struct Instrumentation* instrument, int nodeIdx, int smpIdx)
{
    RCInfo* rcPtr = &instrument->rcInfo;
    if (instrument->needRCInfo) {
        if (es->planinfo && es->planinfo->m_runtimeinfo) {
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, CU_NONE, Int64GetDatum(rcPtr->m_CUNone));
            es->planinfo->m_runtimeinfo->put(nodeIdx, smpIdx, CU_SOME, Int64GetDatum(rcPtr->m_CUSome));
            appendStringInfoString(es->planinfo->m_datanodeInfo->info_str, "(RoughCheck CU: ");
            appendStringInfo(
                es->planinfo->m_datanodeInfo->info_str, "CUNone: %lu, CUSome: %lu", rcPtr->m_CUNone, rcPtr->m_CUSome);
            appendStringInfoString(es->planinfo->m_datanodeInfo->info_str, ")\n");
        }
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfoString(es->str, " (RoughCheck CU: ");
            appendStringInfo(es->str, "CUNone: %lu, CUSome: %lu", rcPtr->m_CUNone, rcPtr->m_CUSome);
            appendStringInfoString(es->str, ")\n");
        } else {
            ExplainPropertyLong("RoughCheck CUNone", rcPtr->m_CUNone, es);
            ExplainPropertyLong("RoughCheck CUSome", rcPtr->m_CUSome, es);
        }
    }
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 */
static const char* explain_get_index_name(Oid indexId)
{
    const char* result = NULL;

    if (explain_get_index_name_hook) {
       result = (*explain_get_index_name_hook) (indexId);
       if(result)
           return result;
    }

    /* default behavior: look in the catalogs and quote it */
    result = get_rel_name(indexId);
    if (result == NULL)
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexId)));
    result = quote_identifier(result);

    return result;
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir, ExplainState* es)
{
    const char* indexname = explain_get_index_name(indexid);

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        if (ScanDirectionIsBackward(indexorderdir))
            appendStringInfoString(es->str, " Backward");
        appendStringInfo(es->str, " using %s", indexname);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL) {
            StringInfo tmpName = &es->planinfo->m_planInfo->m_pname;
            if (ScanDirectionIsBackward(indexorderdir))
                appendStringInfoString(tmpName, " Backward");

            appendStringInfo(tmpName, " using %s", indexname);
        }
    } else {
        const char* scandir = NULL;

        switch (indexorderdir) {
            case BackwardScanDirection:
                scandir = "Backward";
                break;
            case NoMovementScanDirection:
                scandir = "NoMovement";
                break;
            case ForwardScanDirection:
                scandir = "Forward";
                break;
            default:
                scandir = "?\?\?";
                break;
        }
        ExplainPropertyText("Scan Direction", scandir, es);
        ExplainPropertyText("Index Name", indexname, es);
    }
}

/*
 * Show the target of a Scan node
 */
static void ExplainScanTarget(Scan* plan, ExplainState* es)
{
    ExplainTargetRel((Plan*)plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 */
static void ExplainModifyTarget(ModifyTable* plan, ExplainState* es)
{
    Index rti;

    /*
     * We show the name of the first target relation.  In multi-target-table
     * cases this should always be the parent of the inheritance tree.
     */
    AssertEreport(plan->resultRelations != NIL, MOD_EXECUTOR, "unexpect empty list");
    rti = linitial_int(plan->resultRelations);

    ExplainTargetRel((Plan*)plan, rti, es);
}

/*
 * Show the target relation of a scan or modify node
 */
static void ExplainTargetRel(Plan* plan, Index rti, ExplainState* es)
{
    char* objectname = NULL;
    char* namespc = NULL;
    const char* objecttag = NULL;
    char* object_type = NULL; /* for plan_table column "object_type" */
    RangeTblEntry* rte = NULL;

    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery)) {
        RemoteQuery* rq = (RemoteQuery*)plan;

        /* skip Streaming(Gather) since there is no corresponding RangeTblEntry */
        if (rq->is_simple)
            return;
    }

    rte = rt_fetch(rti, es->rtable);

    if (rte == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Unable to get relation.")));
    }

    /* Set object_type from rte->rtekind or rte->relkind for 'plan_table'. */
    if (es->plan && rte) {
        es->planinfo->m_planTableData->set_object_type(rte, &object_type);
    }

    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
        case T_VecForeignScan:
        case T_ModifyTable:
        case T_VecModifyTable: {
            /* Assert it's on a real relation */
            Assert(rte != NULL);
            Assert(rte->rtekind == RTE_RELATION);
            objectname = get_rel_name(rte->relid);
            if (es->verbose || es->plan)
                namespc = get_namespace_name(get_rel_namespace(rte->relid));
            objecttag = "Relation Name";
        } break;
        case T_FunctionScan: {
            Node* funcexpr = NULL;

            /* Assert it's on a RangeFunction */
            Assert(rte != NULL && rte->rtekind == RTE_FUNCTION);

            /*
             * If the expression is still a function call, we can get the
             * real name of the function.  Otherwise, punt (this can
             * happen if the optimizer simplified away the function call,
             * for example).
             */
            funcexpr = ((FunctionScan*)plan)->funcexpr;
            if (funcexpr && IsA(funcexpr, FuncExpr)) {
                Oid funcid = ((FuncExpr*)funcexpr)->funcid;

                objectname = get_func_name(funcid);
                if (es->verbose || es->plan)
                    namespc = get_namespace_name(get_func_namespace(funcid));
            }
            objecttag = "Function Name";
        } break;
        case T_ValuesScan: {
            Assert(rte != NULL);
            Assert(rte->rtekind == RTE_VALUES);
        } break;
        case T_CteScan: {
            /* Assert it's on a non-self-reference CTE */
            Assert(rte != NULL && rte->rtekind == RTE_CTE && !rte->self_reference);
            objectname = rte->ctename;
            objecttag = "CTE Name";
        } break;
        case T_WorkTableScan: {
            /* Assert it's on a self-reference CTE */
            Assert(rte != NULL && rte->rtekind == RTE_CTE && rte->self_reference);
            objectname = rte->ctename;
            objecttag = "CTE Name";
        } break;
        case T_RemoteQuery: {
            /* get the object name from RTE itself */
            RemoteQuery* rq = (RemoteQuery*)plan;

            if (rq->is_simple == false && rte) {
                Assert(rte->rtekind == RTE_REMOTE_DUMMY);
                objectname = rte->relname;
                objecttag = "RemoteQuery name";
            }
        } break;
        default:
            break;
    }

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        StringInfo tmpName;
        if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL)
            tmpName = es->str;
        else
            tmpName = &es->planinfo->m_planInfo->m_pname;

        appendStringInfoString(tmpName, " on");
        if (namespc != NULL && objectname != NULL) {
            appendStringInfo(tmpName, " %s.%s", quote_identifier(namespc), quote_identifier(objectname));
        } else if (objectname != NULL) {
            appendStringInfo(tmpName, " %s", quote_identifier(objectname));
        }

        if (rte && rte->eref && (objectname == NULL || strcmp(rte->eref->aliasname, objectname) != 0)) {
            appendStringInfo(tmpName, " %s", quote_identifier(rte->eref->aliasname));
        }

        /* Show if use column table min/max optimization. */
        if (IsA(plan, CStoreScan) && ((CStoreScan*)plan)->minMaxInfo != NULL) {
            appendStringInfo(tmpName, " %s", "(min-max optimization)");
        }

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL)
            es->planinfo->m_planInfo->put(PLAN, PointerGetDatum(cstring_to_text(tmpName->data)));
    } else {
        if (objecttag != NULL && objectname != NULL)
            ExplainPropertyText(objecttag, objectname, es);
        if (namespc != NULL)
            ExplainPropertyText("Schema", namespc, es);
        if (rte != NULL && rte->eref != NULL) {
            ExplainPropertyText("Alias", rte->eref->aliasname, es);
        }
    }

    /* Set object_name, object_type, object_owner for 'plan_table'. */
    if (es->plan) {
        /* If rte are subquery and values, then rte name will be null. And we get their name from alias. */
        if (objectname == NULL && rte != NULL && rte->eref != NULL)
            objectname = rte->eref->aliasname;

        /* CTE\SUBQUERY\VALUES\JOIN\REMOTEQUERY do not have object owner. */
        if (objectname != NULL)
            es->planinfo->m_planTableData->set_plan_table_objs(plan->plan_node_id, objectname, object_type, namespc);
    }
}

/*
 * Show extra information for upsert info
 */
static void show_on_duplicate_info(ModifyTableState* mtstate, ExplainState* es, List* ancestors)
{
    ResultRelInfo* resultRelInfo = mtstate->resultRelInfo;
    IndexInfo* indexInfo = NULL;
    List* idxNames = NIL;

    /* Gather names of ON CONFLICT Arbiter indexes */
    for (int i = 0; i < resultRelInfo->ri_NumIndices; ++i) {
        indexInfo = resultRelInfo->ri_IndexRelationInfo[i];
        if (!indexInfo->ii_Unique && !indexInfo->ii_ExclusionOps) {
            continue;
        }

        Relation indexRelation = resultRelInfo->ri_IndexRelationDescs[i];
        char* indexName = RelationGetRelationName(indexRelation);
        idxNames = lappend(idxNames, indexName);
    }

    ExplainPropertyText("Conflict Resolution",
                        mtstate->mt_upsert->us_action == UPSERT_NOTHING ? "NOTHING" : "UPDATE",
                        es);
    /*
     * Don't display arbiter indexes at all when DO NOTHING variant
     * implicitly ignores all conflicts
     */
    if (idxNames != NIL) {
        ExplainPropertyList("Conflict Arbiter Indexes", idxNames, es);
    }

    /* Show ON DUPLICATE KEY UPDATE WHERE quals info if specified */
    if (mtstate->mt_upsert->us_updateWhere != NIL) {
        ModifyTable *node = (ModifyTable*)mtstate->ps.plan;
        Node* expr = NULL;
        if (IsA(node->upsertWhere, List)) {
            expr = (Node*)make_ands_explicit((List*)node->upsertWhere);
        } else {
            expr = node->upsertWhere;
        }
        List* clauseList = list_make1(expr);
        show_upper_qual(clauseList, "Conflict Filter", &mtstate->ps, ancestors, es);
        list_free(clauseList);
        show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
    }
}
#ifndef PGXC
/*
 * Show extra information for a ModifyTable node
 */
static void show_modifytable_info(ModifyTableState* mtstate, ExplainState* es)
{
    FdwRoutine* fdwroutine = mtstate->resultRelInfo->ri_FdwRoutine;

    /*
     * If the first target relation is a foreign table, call its FDW to
     * display whatever additional fields it wants to.	For now, we ignore the
     * possibility of other targets being foreign tables, although the API for
     * ExplainForeignModify is designed to allow them to be processed.
     */
    if (fdwroutine != NULL && fdwroutine->ExplainForeignModify != NULL) {
        ModifyTable* node = (ModifyTable*)mtstate->ps.plan;
        List* fdw_private = (List*)linitial(node->fdwPrivLists);

        fdwroutine->ExplainForeignModify(mtstate, mtstate->resultRelInfo, fdw_private, 0, es);
    }
}
#endif /* PGXC */

/*
 * Explain the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 *
 * Note: we don't actually need to examine the Plan list members, but
 * we need the list in order to determine the length of the PlanState array.
 */
static void ExplainMemberNodes(const List* plans, PlanState** planstates, List* ancestors, ExplainState* es)
{
    int nplans = list_length(plans);
    int j;
    bool old_flag = es->from_dn;

    for (j = 0; j < nplans; j++) {
        es->from_dn = old_flag;
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL)
            ExplainNode<true>(planstates[j], ancestors, "Member", NULL, es);
        else
            ExplainNode<false>(planstates[j], ancestors, "Member", NULL, es);
    }

    es->from_dn = old_flag;
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
static void ExplainSubPlans(List* plans, List* ancestors, const char* relationship, ExplainState* es)
{
    ListCell* lst = NULL;
    bool old_flag = es->from_dn;

    foreach (lst, plans) {
        SubPlanState* sps = (SubPlanState*)lfirst(lst);
        SubPlan* sp = (SubPlan*)sps->xprstate.expr;
        es->from_dn = old_flag;

        if (sps->planstate == NULL)
            continue;
        if (sps->planstate->plan->exec_type == EXEC_ON_DATANODES)
            es->from_dn = true;

        /*
         * For stream planning of RecursiveUnion, we already print the RecursiveUnion
         * plan node in normal position, so in subplan list, we skip this part
         *
         * e.g.
         *
         *	 id |									 operation
         *	----+---------------------------------------------------------------------------------
         *	  1 | ->  Streaming (type: GATHER)
         *	  2 |	 ->  Sort
         *	  3 |		->	CTE Scan on rq
         *	  4 |		   ->  Recursive Union
         *	  5 |			  ->  Seq Scan on chinamap
         *	  6 |			  ->  Hash Join (7,9)
         *	  7 |				 ->  Streaming(type: REDISTRIBUTE) stream_level:1
         *	  8 |					->	Seq Scan on chinamap origin <<ruid:[4] ctlid:[4] (SYNC)>>
         *	  9 |				 ->  Hash
         *	 10 |					->	Streaming(type: REDISTRIBUTE) stream_level:1
         *	 11 |					   ->  WorkTable Scan on rq <<ruid:[4] ctlid:[4]>>
         */
        if (STREAM_RECURSIVECTE_SUPPORTED && sp->subLinkType == CTE_SUBLINK) {
            continue;
        }

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo != NULL)
            ExplainNode<true>(sps->planstate, ancestors, relationship, sp->plan_name, es);
        else
            ExplainNode<false>(sps->planstate, ancestors, relationship, sp->plan_name, es);
    }
    es->from_dn = old_flag;
}

static void ExplainPrettyList(List* data, ExplainState* es)
{
    ListCell* lc = NULL;
    bool first = true;

    foreach (lc, data) {
        if (!first) {
            appendStringInfoString(es->planinfo->m_verboseInfo->info_str, ", ");
        }

        appendStringInfoString(es->planinfo->m_verboseInfo->info_str, (const char*)lfirst(lc));

        first = false;
    }

    appendStringInfoChar(es->planinfo->m_verboseInfo->info_str, '\n');
}

/*
 * Explain a property like what ExplainPropertyList does but append it to
 * the end of the plan tree.
 * We reuse ExplainPropertyList here by temporarily setting and recovering
 * es->str such that the explation is printed into es->post_str instead.
 * "data" is a list of C strings.
 */
void ExplainPropertyListPostPlanTree(const char* qlabel, List* data, ExplainState* es)
{
    if (es->post_str == NULL) {
        es->post_str = makeStringInfo();
    }
    StringInfo str_backup = es->str;
    int indent_backup = es->indent;
    es->str = es->post_str;
    es->indent = 0;
    ExplainPropertyList(qlabel, data, es);
    es->str = str_backup;
    es->indent = indent_backup;
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
void ExplainPropertyList(const char* qlabel, List* data, ExplainState* es)
{
    ListCell* lc = NULL;
    bool first = true;

    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            if (es->wlm_statistics_plan_max_digit) {
                appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
                appendStringInfoString(es->str, " | ");
                appendStringInfoSpaces(es->str, es->indent);
            } else {
                appendStringInfoSpaces(es->str, es->indent * 2);
            }
            appendStringInfo(es->str, "%s: ", qlabel);
            foreach (lc, data) {
                if (!first)
                    appendStringInfoString(es->str, ", ");
                appendStringInfoString(es->str, (const char*)lfirst(lc));
                first = false;
            }
            appendStringInfoChar(es->str, '\n');
            break;

        case EXPLAIN_FORMAT_XML:
            ExplainXMLTag(qlabel, X_OPENING, es);
            foreach (lc, data) {
                char* str = NULL;

                appendStringInfoSpaces(es->str, es->indent * 2 + 2);
                appendStringInfoString(es->str, "<Item>");
                str = escape_xml((const char*)lfirst(lc));
                appendStringInfoString(es->str, str);
                pfree_ext(str);
                appendStringInfoString(es->str, "</Item>\n");
            }
            ExplainXMLTag(qlabel, X_CLOSING, es);
            break;

        case EXPLAIN_FORMAT_JSON:
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, es->indent * 2);
            escape_json(es->str, qlabel);
            appendStringInfoString(es->str, ": [");
            foreach (lc, data) {
                if (!first)
                    appendStringInfoString(es->str, ", ");
                escape_json(es->str, (const char*)lfirst(lc));
                first = false;
            }
            appendStringInfoChar(es->str, ']');
            break;

        case EXPLAIN_FORMAT_YAML:
            ExplainYAMLLineStarting(es);
            appendStringInfo(es->str, "%s: ", qlabel);
            foreach (lc, data) {
                appendStringInfoChar(es->str, '\n');
                appendStringInfoSpaces(es->str, es->indent * 2 + 2);
                appendStringInfoString(es->str, "- ");
                escape_yaml(es->str, (const char*)lfirst(lc));
            }
            break;
        default:
            break;
    }
}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
void ExplainPropertyListNested(const char* qlabel, List* data, ExplainState* es)
{
    ListCell* lc = NULL;
    bool first = true;

    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
        case EXPLAIN_FORMAT_XML:
            ExplainPropertyList(qlabel, data, es);
            return;

        case EXPLAIN_FORMAT_JSON:
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, es->indent * 2);
            appendStringInfoChar(es->str, '[');
            foreach (lc, data) {
                if (!first)
                    appendStringInfoString(es->str, ", ");
                escape_json(es->str, (const char*)lfirst(lc));
                first = false;
            }
            appendStringInfoChar(es->str, ']');
            break;

        case EXPLAIN_FORMAT_YAML:
            ExplainYAMLLineStarting(es);
            appendStringInfoString(es->str, "- [");
            foreach (lc, data) {
                if (!first)
                    appendStringInfoString(es->str, ", ");
                escape_yaml(es->str, (const char*)lfirst(lc));
                first = false;
            }
            appendStringInfoChar(es->str, ']');
            break;
        default:
            break;
    }
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines ExplainPropertyText, ExplainPropertyInteger, etc.
 */
static void ExplainProperty(const char* qlabel, const char* value, bool numeric, ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            if (es->wlm_statistics_plan_max_digit) {
                appendStringInfoSpaces(es->str, *es->wlm_statistics_plan_max_digit);
                appendStringInfoString(es->str, " | ");
                appendStringInfoSpaces(es->str, es->indent);
            } else {
                appendStringInfoSpaces(es->str, es->indent * 2);
            }
            appendStringInfo(es->str, "%s: %s\n", qlabel, value);
            break;

        case EXPLAIN_FORMAT_XML: {
            char* str = NULL;

            appendStringInfoSpaces(es->str, es->indent * 2);
            ExplainXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, es);
            str = escape_xml(value);
            appendStringInfoString(es->str, str);
            pfree_ext(str);
            ExplainXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, es);
            appendStringInfoChar(es->str, '\n');
        } break;

        case EXPLAIN_FORMAT_JSON:
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, es->indent * 2);
            escape_json(es->str, qlabel);
            appendStringInfoString(es->str, ": ");
            if (numeric)
                appendStringInfoString(es->str, value);
            else
                escape_json(es->str, value);
            break;

        case EXPLAIN_FORMAT_YAML:
            ExplainYAMLLineStarting(es);
            appendStringInfo(es->str, "%s: ", qlabel);
            if (numeric)
                appendStringInfoString(es->str, value);
            else
                escape_yaml(es->str, value);
            break;
        default:
            break;
    }
}

/*
 * Explain a string-valued property.
 */
void ExplainPropertyText(const char* qlabel, const char* value, ExplainState* es)
{
    ExplainProperty(qlabel, value, false, es);
}

/*
 * Explain an integer-valued property.
 */
void ExplainPropertyInteger(const char* qlabel, int value, ExplainState* es)
{
    char buf[32];

    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%d", value);
    securec_check_ss(rc, "\0", "\0");
    ExplainProperty(qlabel, buf, true, es);
}

/*
 * Explain a long-integer-valued property.
 */
void ExplainPropertyLong(const char* qlabel, long value, ExplainState* es)
{
    char buf[32];

    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%ld", value);
    securec_check_ss(rc, "\0", "\0");
    ExplainProperty(qlabel, buf, true, es);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
void ExplainPropertyFloat(const char* qlabel, double value, int ndigits, ExplainState* es)
{
    char buf[256];

    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%.*f", ndigits, value);
    securec_check_ss(rc, "\0", "\0");
    ExplainProperty(qlabel, buf, true, es);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
static void ExplainOpenGroup(const char* objtype, const char* labelname, bool labeled, ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* nothing to do */
            break;

        case EXPLAIN_FORMAT_XML:
            ExplainXMLTag(objtype, X_OPENING, es);
            es->indent++;
            break;

        case EXPLAIN_FORMAT_JSON:
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, 2 * es->indent);
            if (labelname != NULL) {
                escape_json(es->str, labelname);
                appendStringInfoString(es->str, ": ");
            }
            appendStringInfoChar(es->str, labeled ? '{' : '[');

            /*
             * In JSON format, the grouping_stack is an integer list.  0 means
             * we've emitted nothing at this grouping level, 1 means we've
             * emitted something (and so the next item needs a comma). See
             * ExplainJSONLineEnding().
             */
            es->grouping_stack = lcons_int(0, es->grouping_stack);
            es->indent++;
            break;

        case EXPLAIN_FORMAT_YAML:

            /*
             * In YAML format, the grouping stack is an integer list.  0 means
             * we've emitted nothing at this grouping level AND this grouping
             * level is unlabelled and must be marked with "- ".  See
             * ExplainYAMLLineStarting().
             */
            ExplainYAMLLineStarting(es);
            if (labelname != NULL) {
                appendStringInfo(es->str, "%s: ", labelname);
                es->grouping_stack = lcons_int(1, es->grouping_stack);
            } else {
                appendStringInfoString(es->str, "- ");
                es->grouping_stack = lcons_int(0, es->grouping_stack);
            }
            es->indent++;
            break;
        default:
            break;
    }
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
static void ExplainCloseGroup(const char* objtype, const char* labelname, bool labeled, ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* nothing to do */
            break;

        case EXPLAIN_FORMAT_XML:
            es->indent--;
            ExplainXMLTag(objtype, X_CLOSING, es);
            break;

        case EXPLAIN_FORMAT_JSON:
            es->indent--;
            appendStringInfoChar(es->str, '\n');
            appendStringInfoSpaces(es->str, 2 * es->indent);
            appendStringInfoChar(es->str, labeled ? '}' : ']');
            es->grouping_stack = list_delete_first(es->grouping_stack);
            break;

        case EXPLAIN_FORMAT_YAML:
            es->indent--;
            es->grouping_stack = list_delete_first(es->grouping_stack);
            break;
        default:
            break;
    }
}

/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
static void ExplainDummyGroup(const char* objtype, const char* labelname, ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* nothing to do */
            break;

        case EXPLAIN_FORMAT_XML:
            ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es);
            break;

        case EXPLAIN_FORMAT_JSON:
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces(es->str, 2 * es->indent);
            if (labelname != NULL) {
                escape_json(es->str, labelname);
                appendStringInfoString(es->str, ": ");
            }
            escape_json(es->str, objtype);
            break;

        case EXPLAIN_FORMAT_YAML:
            ExplainYAMLLineStarting(es);
            if (labelname != NULL) {
                escape_yaml(es->str, labelname);
                appendStringInfoString(es->str, ": ");
            } else {
                appendStringInfoString(es->str, "- ");
            }
            escape_yaml(es->str, objtype);
            break;
        default:
            break;
    }
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
void ExplainBeginOutput(ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* nothing to do */
            break;

        case EXPLAIN_FORMAT_XML:
            appendStringInfoString(es->str, "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
            es->indent++;
            break;

        case EXPLAIN_FORMAT_JSON:
            /* top-level structure is an array of plans */
            appendStringInfoChar(es->str, '[');
            es->grouping_stack = lcons_int(0, es->grouping_stack);
            es->indent++;
            break;

        case EXPLAIN_FORMAT_YAML:
            es->grouping_stack = lcons_int(0, es->grouping_stack);
            break;
        default:
            break;
    }
}

/*
 * Emit the end-of-output boilerplate.
 */
void ExplainEndOutput(ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* nothing to do */
            break;

        case EXPLAIN_FORMAT_XML:
            es->indent--;
            appendStringInfoString(es->str, "</explain>");
            break;

        case EXPLAIN_FORMAT_JSON:
            es->indent--;
            appendStringInfoString(es->str, "\n]");
            es->grouping_stack = list_delete_first(es->grouping_stack);
            break;

        case EXPLAIN_FORMAT_YAML:
            es->grouping_stack = list_delete_first(es->grouping_stack);
            break;
        default:
            break;
    }
}

/*
 * Put an appropriate separator between multiple plans
 */
void ExplainSeparatePlans(ExplainState* es)
{
    switch (es->format) {
        case EXPLAIN_FORMAT_TEXT:
            /* add a blank line */
            appendStringInfoChar(es->str, '\n');
            break;

        case EXPLAIN_FORMAT_XML:
        case EXPLAIN_FORMAT_JSON:
        case EXPLAIN_FORMAT_YAML:
            /* nothing to do */
            break;
        default:
            break;
    }
}

#ifdef PGXC
/*
 * Emit execution node list number.
 */
static void ExplainExecNodes(const ExecNodes* en, ExplainState* es)
{
    int primary_node_count = en ? list_length(en->primarynodelist) : 0;
    int node_count = en ? list_length(en->nodeList) : 0;

    if (!es->num_nodes)
        return;

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        appendStringInfo(es->str, " (primary node count=%d, node count=%d)", primary_node_count, node_count);
    } else {
        ExplainPropertyInteger("Primary node count", primary_node_count, es);
        ExplainPropertyInteger("Node count", node_count, es);
    }
}

static void showStreamnetwork(Stream* stream, ExplainState* es)
{
    Instrumentation* instr = NULL;
    Plan* plan = (Plan*)stream;
    long spacePeakKb = 0;
    double toltal_time = 0;
    double deserialize_time = 0;
    double copy_time = 0;
    if (u_sess->instr_cxt.global_instr && es->planinfo->m_runtimeinfo) {
        int dop = ((Plan*)stream)->parallel_enabled ? u_sess->opt_cxt.query_dop : 1;
        for (int i = 0; i < u_sess->instr_cxt.global_instr->getInstruNodeNum(); i++) {
#ifdef ENABLE_MULTIPLE_NODES
            char* node_name = PGXCNodeGetNodeNameFromId(i, PGXC_NODE_DATANODE);
#else
            char* node_name = g_instance.exec_cxt.nodeName;
#endif
            for (int j = 0; j < dop; j++) {
                instr = u_sess->instr_cxt.global_instr->getInstrSlot(i, plan->plan_node_id, j);
                if (instr == NULL)
                    continue;
                spacePeakKb = (instr->network_perfdata.network_size + 1023) / 1024;
                toltal_time = instr->network_perfdata.total_poll_time;
                deserialize_time = instr->network_perfdata.total_deserialize_time;
                copy_time = instr->network_perfdata.total_copy_time;

                es->planinfo->m_runtimeinfo->put(i, j, QUERY_NETWORK, spacePeakKb);
                es->planinfo->m_runtimeinfo->put(i, j, NETWORK_POLL_TIME, Float8GetDatum(toltal_time));

                es->planinfo->m_query_summary->m_size += instr->network_perfdata.network_size;

                es->planinfo->m_staticInfo->set_plan_name<false, true>();
                es->planinfo->m_staticInfo->set_datanode_name(node_name, j, dop);

                /* Local stream transfer through memory rather than net. */
                if (!STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType))
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, "Stream Network: %ldkB, ", spacePeakKb);
                appendStringInfo(es->planinfo->m_staticInfo->info_str, "Network Poll Time: %.3f", toltal_time);
                if (copy_time > 0.0)
                    appendStringInfo(es->planinfo->m_staticInfo->info_str, "; Data Copy Time: %.3f\n", copy_time);
                else
                    appendStringInfo(
                        es->planinfo->m_staticInfo->info_str, "; Data Deserialize Time: %.3f\n", deserialize_time);
            }
        }
    }
}

/*
 * ShowRunNodeInfo
 *     show exec node information, including group name and node list
 *
 * @param (in) en:
 *     the exec nodes
 * @param (in) es:
 *     the explain state
 * @param (in) qlabel:
 *     the leading label of this item in explain information
 *
 * @return: void
 */
static void ShowRunNodeInfo(const ExecNodes* en, ExplainState* es, const char* qlabel)
{
    if (en != NULL && es->nodes) {
        StringInfo node_names = makeStringInfo();
#ifdef ENABLE_MULTIPLE_NODES
        if (list_length(en->nodeList) == u_sess->pgxc_cxt.NumDataNodes) {
            appendStringInfo(node_names, "All datanodes");
        } else {
            ListCell* lcell = NULL;
            char* sep = "";
            int node_no;

            /* we show group name information except in single installation group scenario */
            if (ng_enable_nodegroup_explain()) {
                char* group_name = ng_get_dist_group_name((Distribution*)&(en->distribution));
                appendStringInfo(node_names, "(%s) ", group_name);
            }

            foreach (lcell, en->nodeList) {
                NameData nodename = {{0}};
                node_no = lfirst_int(lcell);
                appendStringInfo(node_names,
                    "%s%s",
                    sep,
                    get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, PGXC_NODE_DATANODE), &nodename));
                sep = ", ";
            }
        }
#else
        appendStringInfo(node_names, "All datanodes");
#endif
        ExplainPropertyText(qlabel, node_names->data, es);
        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfo(es->planinfo->m_verboseInfo->info_str, "%s: %s\n", qlabel, node_names->data);
        }
    }
}

/*
 * ShowStreamRunNodeInfo
 *     show the stream thread spawn on which node,
 *     also include consumer nodes.
 *
 * @param (in) stream:
 *     the stream node
 * @param (in) es:
 *     the explain state
 *
 * @return: void
 */
static void ShowStreamRunNodeInfo(Stream* stream, ExplainState* es)
{
    ExecNodes* en = stream->scan.plan.exec_nodes;
    ShowRunNodeInfo(en, es, "Spawn on");

    /* show consumerNodes */
    if (es->nodes && es->verbose) {
        ExecNodes* cen = stream->consumer_nodes;
        ShowRunNodeInfo(cen, es, "Consumer Nodes");
    }
}

/*
 * Emit remote query planning details
 */
static void ExplainRemoteQuery(RemoteQuery* plan, PlanState* planstate, List* ancestors, ExplainState* es)
{
    ExecNodes* en = plan->exec_nodes;
    const char* label_name = NULL;

    if (es->format == EXPLAIN_FORMAT_TEXT) {
        label_name = "Node/s";
    } else {
        label_name = "Nodes";
    }

    /* add names of the nodes if they exist */
    if (en != NULL && es->nodes && plan->position == GATHER) {
        StringInfo node_names = makeStringInfo();
        ListCell* lcell = NULL;
        char* sep = NULL;
        int node_no;

        if (en->primarynodelist) {
            if (plan->exec_type == EXEC_ON_ALL_NODES) {
                appendStringInfo(node_names, "All nodes");
            } else {
                char exec_node_type =
                    (plan->exec_type == EXEC_ON_DATANODES) ? (PGXC_NODE_DATANODE) : (PGXC_NODE_COORDINATOR);
                sep = "";
                foreach (lcell, en->primarynodelist) {
                    NameData nodename = {{0}};
                    node_no = lfirst_int(lcell);
                    appendStringInfo(node_names,
                        "%s%s",
                        sep,
                        get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, exec_node_type), &nodename));
                    sep = ", ";
                }
            }

            if (es->format == EXPLAIN_FORMAT_TEXT) {
                ExplainPropertyText("Primary node/s", node_names->data, es);
            } else {
                ExplainPropertyText("Primary nodes", node_names->data, es);
            }
        }

        if (en->nodeList) {
            resetStringInfo(node_names);

            if (list_length(en->nodeList) == u_sess->pgxc_cxt.NumDataNodes) {
                appendStringInfo(node_names, "All datanodes");
                es->datanodeinfo.all_datanodes = true;
            } else {
                char exec_node_type =
                    (plan->exec_type == EXEC_ON_DATANODES) ? (PGXC_NODE_DATANODE) : (PGXC_NODE_COORDINATOR);
                es->datanodeinfo.all_datanodes = false;
                es->datanodeinfo.len_nodelist = list_length(en->nodeList);
                if (es->datanodeinfo.node_index) {
                    pfree_ext(es->datanodeinfo.node_index);
                    es->datanodeinfo.node_index = NULL;
                }

                if (es->datanodeinfo.node_index == NULL)
                    es->datanodeinfo.node_index = (int*)palloc0(sizeof(int) * es->datanodeinfo.len_nodelist);
                sep = "";

                /* we show group name information except in single installation group scenario */
                if (ng_enable_nodegroup_explain()) {
                    char* group_name = ng_get_dist_group_name(&(en->distribution));
                    appendStringInfo(node_names, "(%s) ", group_name);
                }

                int i = 0;
                foreach (lcell, en->nodeList) {
                    NameData nodename = {{0}};
                    node_no = lfirst_int(lcell);
                    es->datanodeinfo.node_index[i++] = node_no;
                    appendStringInfo(node_names,
                        "%s%s",
                        sep,
                        get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, exec_node_type), &nodename));
                    sep = ", ";
                }
            }

            ExplainPropertyText(label_name, node_names->data, es);

            if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
                es->planinfo->m_verboseInfo->set_plan_name<false, true>();
                appendStringInfo(es->planinfo->m_verboseInfo->info_str, "Node/s: %s\n", node_names->data);
            }
        }
    }

    if (plan->position == SCAN_GATHER) {
        ExplainPropertyText(label_name, "DNs in compute pool", es);
    }

    if (plan->position == PLAN_ROUTER) {
        StringInfo address = makeStringInfo();

        Plan* fsplan = get_foreign_scan((Plan*)plan);

        ServerTypeOption srvtype = getServerType(((ForeignScan*)fsplan)->scan_relid);
        if (T_OBS_SERVER == srvtype || T_TXT_CSV_OBS_SERVER == srvtype) {
            ComputePoolConfig** conf = get_cp_conninfo();

            appendStringInfo(address, "%s:%s", conf[0]->cpip, conf[0]->cpport);
        } else {
            // T_HDFS_SERVER
            DummyServerOptions* options = getDummyServerOption();

            AssertEreport(options != NULL, MOD_EXECUTOR, "unexpect null value");

            appendStringInfo(address, "%s", options->address);

            AssertEreport(address != NULL, MOD_EXECUTOR, "unexpect null value");
        }

        StringInfo si = makeStringInfo();
        appendStringInfo(si, "compute pool: %s", address->data);

        ExplainPropertyText(label_name, si->data, es);
    }

    if (en != NULL && en->en_expr && es->pstmt->commandType != CMD_MERGE)
        show_expression((Node*)en->en_expr, "Node expr", planstate, ancestors, es->verbose, es);
    else if (en != NULL && en->en_expr && es->pstmt->commandType == CMD_MERGE) {
        ListCell* lc = NULL;
        int cnt = 0;
        StringInfo si = makeStringInfo();
        /* the var in merge into en_expr point to the insert cols on the plan's targetlist */
        foreach (lc, en->en_expr) {
            if (IsA(lfirst(lc), Var)) {
                Var* var = (Var*)lfirst(lc);
                /* we show it as $%d as it point to the plan's targetlist */
                if (cnt == 0)
                    appendStringInfo(si, "$%d", var->varattno);
                else
                    appendStringInfo(si, ", $%d", var->varattno);

                cnt++;
            }
        }

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_detailInfo) {
            es->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s: %s\n", "Node expr: ", si->data);
        }

        /* And add to es->str */
        ExplainPropertyText("Node expr", si->data, es);
    }

    /* Remote query statement */
    if (es->verbose && !plan->is_simple)
        ExplainPropertyText("Remote query", plan->sql_statement, es);
}
#endif

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML tag names can't contain white space, so we replace any spaces in
 * "tagname" with dashes.
 */
static void ExplainXMLTag(const char* tagname, unsigned int flags, ExplainState* es)
{
    const char* s = NULL;

    if ((flags & X_NOWHITESPACE) == 0)
        appendStringInfoSpaces(es->str, 2 * es->indent);
    appendStringInfoCharMacro(es->str, '<');
    if ((flags & X_CLOSING) != 0)
        appendStringInfoCharMacro(es->str, '/');
    for (s = tagname; *s; s++)
        appendStringInfoCharMacro(es->str, (*s == ' ') ? '-' : *s);
    if ((flags & X_CLOSE_IMMEDIATE) != 0)
        appendStringInfoString(es->str, " /");
    appendStringInfoCharMacro(es->str, '>');
    if ((flags & X_NOWHITESPACE) == 0)
        appendStringInfoCharMacro(es->str, '\n');
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.	To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void ExplainJSONLineEnding(ExplainState* es)
{
    AssertEreport(es->format == EXPLAIN_FORMAT_JSON, MOD_EXECUTOR, "unexpect explain state format");
    if (linitial_int(es->grouping_stack) != 0)
        appendStringInfoChar(es->str, ',');
    else
        linitial_int(es->grouping_stack) = 1;
    appendStringInfoChar(es->str, '\n');
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabelled group, for which
 * it begins immediately after the "- " that introduces the group.	The first
 * property of the group appears on the same line as the opening "- ".
 */
static void ExplainYAMLLineStarting(ExplainState* es)
{
    AssertEreport(es->format == EXPLAIN_FORMAT_YAML, MOD_EXECUTOR, "unexpect explain state format");
    if (linitial_int(es->grouping_stack) == 0) {
        linitial_int(es->grouping_stack) = 1;
    } else {
        appendStringInfoChar(es->str, '\n');
        appendStringInfoSpaces(es->str, es->indent * 2);
    }
}

/*
 * YAML is a superset of JSON; unfortuantely, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
static void escape_yaml(StringInfo buf, const char* str)
{
    escape_json(buf, str);
}

#ifndef ENABLE_MULTIPLE_NODES
static void SetPredictor(ExplainState* es, PlanTable* m_planInfo)
{
    if (es->opt_model_name != NULL) {
        /* look up gs_opt_model for predictor's targets */
        char *labels = NULL;
        int nLabel;
        (void) CheckModelTargets(RLSTM_TEMPLATE_NAME, es->opt_model_name, &labels, &nLabel);
        for (int i = 0; i < nLabel; ++i) {
            switch (labels[i]) {
                case gs_opt_model_label_total_time: {
                    m_planInfo->m_pred_time = true;
                    break;
                }
                case gs_opt_model_label_rows: {
                    m_planInfo->m_pred_row = true;
                    break;
                }
                case gs_opt_model_label_peak_memory: {
                    m_planInfo->m_pred_mem = true;
                    break;
                }
                default:
                    /* keep compiler silent */
                    break;
            }
        }
    }
}
#endif

void PlanInformation::init(ExplainState* es, int plansize, int num_nodes, bool query_mem_mode)
{
    m_detail = es->detail;
    m_planInfo = (PlanTable*)palloc0(sizeof(PlanTable));
    m_planInfo->m_costs = es->costs;
    m_planInfo->m_verbose = es->verbose;
    m_planInfo->m_cpu = false;
    m_planInfo->m_analyze = es->analyze;
    m_planInfo->m_timing = es->timing;

    /* determine which prediction columns should be made visible */
    m_planInfo->m_pred_time = false;
    m_planInfo->m_pred_row = false;
    m_planInfo->m_pred_mem = false;
#ifndef ENABLE_MULTIPLE_NODES
    SetPredictor(es, m_planInfo);
#endif

    if (m_planInfo->m_costs)
        m_planInfo->m_query_mem_mode = query_mem_mode;

    m_planInfo->init(PLANINFO);
    m_planInfo->init_planinfo(plansize);

    m_detailInfo = (PlanTable*)palloc0(sizeof(PlanTable));
    m_detailInfo->init(DETAILINFO);

    m_query_summary = (PlanTable*)palloc0(sizeof(PlanTable));
    m_query_summary->init(QUERYSUMMARY);

    /* Init data struct for explain plan. */
    if (es->plan) {
        m_planTableData = (PlanTable*)palloc0(sizeof(PlanTable));
        if (IsExplainPlanSelectForUpdateStmt && plansize == 0)
            plansize = 1;
        m_planTableData->init_plan_table_data(plansize);
    } else
        m_planTableData = NULL;

    if (es->analyze) {
        m_staticInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_staticInfo->init(STATICINFO);

        m_profileInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_profileInfo->init(PROFILEINFO);
    } else {
        m_staticInfo = NULL;
        m_profileInfo = NULL;
    }

    if (es->verbose) {
        m_verboseInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_verboseInfo->init(VERBOSEINFO);
    } else
        m_verboseInfo = NULL;

    if (es->detail) {
        m_datanodeInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_datanodeInfo->init(DATANODEINFO);

        m_runtimeinfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_runtimeinfo->init(RUNTIMEINFO);
        m_runtimeinfo->init_multi_info(es, plansize, num_nodes);
    } else {
        m_datanodeInfo = NULL;
        m_runtimeinfo = NULL;
    }

    if ((es->cpu || es->buffers) && es->detail == false) {
        m_planInfo->m_cpu = true;
        m_IOInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_IOInfo->init(IOINFO);
    } else
        m_IOInfo = NULL;

    if (es->detail) {
        m_recursiveInfo = (PlanTable*)palloc0(sizeof(PlanTable));
        m_recursiveInfo->init(RECURSIVEINFO);
    } else {
        m_recursiveInfo = NULL;
    }
}

void PlanInformation::append_str_info(const char* data, int id, const char* value)
{
    if (m_verboseInfo)
        appendStringInfo(m_verboseInfo->info_str, data, id, value);
    if (m_datanodeInfo)
        appendStringInfo(m_datanodeInfo->info_str, data, id, value);
}

static void writefile(const char* path, char* text)
{
    FILE* out_file = NULL;
    char* line = NULL;
    errno_t rc = 0;

    if ((out_file = fopen(path, "w")) == NULL) {
        return;
    }

    while (*text) {
        char* eol = NULL;
        int len;

        eol = strchr(text, '\n');
        if (eol != NULL) {
            len = eol - text;

            eol++;
        } else {
            len = strlen(text);
            eol += len;
        }

        line = (char*)palloc0(sizeof(char) * (len + 2));
        rc = memcpy_s(line, len, text, len);
        securec_check(rc, "\0", "\0");
        line[len] = '\n';
        if (fputs(line, out_file) < 0) {
            (void)fclose(out_file);
            return;
        }
        pfree_ext(line);

        text = eol;
    }

    if (fclose(out_file)) {
        return;
    }
}

void PlanInformation::dump_runtimeinfo_file()
{
    int rc = 0;
    char tempdirpath[1024] = {0};
    FILE* fp = NULL;
    bool is_absolute = false;

    if (u_sess->attr.attr_common.Perf_log == NULL) {
        is_absolute = is_absolute_path(u_sess->attr.attr_common.Log_directory);
        if (is_absolute)
            rc = snprintf_s(tempdirpath,
                sizeof(tempdirpath),
                sizeof(tempdirpath) - 1,
                "%s/explain_table_%d.csv",
                u_sess->attr.attr_common.Log_directory,
                m_query_id);
        else
            rc = snprintf_s(tempdirpath,
                sizeof(tempdirpath),
                sizeof(tempdirpath) - 1,
                "%s/pg_log/explain_table_%d.csv",
                g_instance.attr.attr_common.data_directory,
                m_query_id);
    } else {
        rc = snprintf_s(
            tempdirpath, sizeof(tempdirpath), sizeof(tempdirpath) - 1, "%s", u_sess->attr.attr_common.Perf_log);
    }

    securec_check_ss(rc, "\0", "\0");
    fp = fopen(tempdirpath, PG_BINARY_W);
    if (fp == NULL)
        return;
    fclose(fp);

    m_runtimeinfo->flush_data_to_file();
    writefile(tempdirpath, m_runtimeinfo->info_str->data);
}

void PlanInformation::free_memory()
{
    if (m_planInfo) {
        pfree_ext(m_planInfo);
        m_planInfo = NULL;
    }

    if (m_detailInfo) {
        pfree_ext(m_detailInfo);
        m_detailInfo = NULL;
    }

    if (m_staticInfo) {
        pfree_ext(m_staticInfo);
        m_staticInfo = NULL;
    }

    if (m_verboseInfo) {
        pfree_ext(m_verboseInfo);
        m_verboseInfo = NULL;
    }

    if (m_datanodeInfo) {
        pfree_ext(m_datanodeInfo);
        m_datanodeInfo = NULL;
    }

    if (m_IOInfo) {
        pfree_ext(m_IOInfo);
        m_IOInfo = NULL;
    }

    if (m_profileInfo) {
        pfree_ext(m_profileInfo);
        m_profileInfo = NULL;
    }

    if (m_query_summary) {
        pfree_ext(m_query_summary);
        m_query_summary = NULL;
    }

    if (m_recursiveInfo) {
        pfree(m_recursiveInfo);
        m_recursiveInfo = NULL;
    }
}
int PlanInformation::print_plan(Portal portal, DestReceiver* dest)
{
    m_count = 0;
    /* Do not print tupDesc and result '(xx rows)' for explain plan */
    if (m_planTableData) {
        free_memory();
        return m_count;
    }
    if (m_planInfo)
        m_count += m_planInfo->print_plan(portal, dest);
    if (m_detailInfo && m_detailInfo->m_size > 0)
        m_count += m_detailInfo->print_plan(portal, dest);
    if (m_staticInfo && m_staticInfo->m_size > 0)
        m_count += m_staticInfo->print_plan(portal, dest);
    if (m_verboseInfo && m_verboseInfo->m_size > 0)
        m_count += m_verboseInfo->print_plan(portal, dest);
    if (m_datanodeInfo && m_datanodeInfo->m_size > 0)
        m_count += m_datanodeInfo->print_plan(portal, dest);
    if (m_IOInfo && m_IOInfo->m_size)
        m_count += m_IOInfo->print_plan(portal, dest);
    if (m_recursiveInfo && m_recursiveInfo->m_size)
        m_count += m_recursiveInfo->print_plan(portal, dest);
    if (m_profileInfo && m_profileInfo->m_size > 0)
        m_count += m_profileInfo->print_plan(portal, dest);
    if (m_query_summary && m_query_summary->m_size > 0)
        m_count += m_query_summary->print_plan(portal, dest);

    free_memory();

    return m_count;
}

void PlanTable::init(int plantype)
{
    switch (plantype) {
        case PLANINFO:
            m_desc = getTupleDesc();
            initStringInfo(&m_pname);
            break;
        case DETAILINFO:
            m_desc = getTupleDesc("Predicate Information (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case STATICINFO:
            m_desc = getTupleDesc("Memory Information (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case VERBOSEINFO:
            m_desc = getTupleDesc("Targetlist Information (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case DATANODEINFO:
            m_desc = getTupleDesc("Datanode Information (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case IOINFO:
            m_desc = getTupleDesc("Cpu-Buffer Information (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case RUNTIMEINFO:
            m_desc = getTupleDesc_detail();
            info_str = makeStringInfo();
            break;
        case PROFILEINFO:
            m_desc = getTupleDesc("User Define Profiling");
            info_str = makeStringInfo();
            break;
        case RECURSIVEINFO:
            m_desc = getTupleDesc("Recursive Union Plan node (identified by plan id)");
            info_str = makeStringInfo();
            break;
        case QUERYSUMMARY:
            m_desc = getTupleDesc("====== Query Summary =====");
            info_str = makeStringInfo();
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupported type %d", plantype)));
    }
}

void PlanTable::init_multi_info(ExplainState* es, int plansize, int num_nodes)
{
    int i = 0;
    int j = 0;

    m_plan_size = plansize;
    m_data_size = num_nodes;

    m_multi_info = (MultiInfo***)palloc0(sizeof(MultiInfo**) * m_data_size);

    for (i = 0; i < m_data_size; i++) {
        m_multi_info[i] = (MultiInfo**)palloc0(sizeof(MultiInfo*) * m_plan_size);
        for (j = 0; j < m_plan_size; j++) {
            int dop = u_sess->instr_cxt.global_instr->getNodeDop(j + 1);
            m_multi_info[i][j] = (MultiInfo*)palloc0(sizeof(MultiInfo) * dop);
            init_multi_info_data(m_multi_info[i][j], dop);
        }
    }
}

/*
 * @Description: init the data of MultiInfo
 *
 * @param[IN] multi_info:  each element of m_multi_info
 * @param[IN] dop:  dop of specific operator
 * @return: void
 */
void PlanTable::init_multi_info_data(MultiInfo* multi_info, int dop)
{
    for (int i = 0; i < dop; i++) {
        multi_info[i].m_Datum = (Datum*)palloc0(EXPLAIN_TOTAL_ATTNUM * sizeof(Datum));
        multi_info[i].m_Nulls = (bool*)palloc0(EXPLAIN_TOTAL_ATTNUM * sizeof(bool));
        for (int j = 0; j < EXPLAIN_TOTAL_ATTNUM; j++)
            multi_info[i].m_Nulls[j] = true;
    }
}

void PlanTable::init_planinfo(int plansize)
{
    int i = 0;
    int j = 0;
    m_size = plansize;
    int attnum;
    int analyze_num = m_analyze ? 1 : 0;
    int costs_num = m_costs ? 1 : 0;
    int verbose_num = m_verbose ? 1 : 0;

    attnum = (PLAN + 1) + analyze_num * NUM_ANALYZE_COLS + \
             costs_num * (NUM_COST_COLS + verbose_num * NUM_VER_COLS + m_pred_time + m_pred_row + m_pred_mem);

    /* If cost enabled and not query mem mode, don't show estimate_memory column */
    if (m_costs && !m_query_mem_mode)
        attnum = attnum - 1;

    m_data = (Datum**)palloc0(plansize * sizeof(Datum*));
    m_isnull = (bool**)palloc0(plansize * sizeof(bool*));
    for (i = 0; i < plansize; i++) {
        m_data[i] = (Datum*)palloc0(attnum * sizeof(Datum));
        m_isnull[i] = (bool*)palloc0(attnum * sizeof(bool));

        for (j = 0; j < attnum; j++)
            m_isnull[i][j] = true;
    }
}

/*
 * Description: Init data struct for plan table according to plan size.
 *
 * Parameters:
 * @in num_plan_nodes: number of plan node.
 * Return: void
 */
void PlanTable::init_plan_table_data(int num_plan_nodes)
{
    int i = 0;
    errno_t rc = EOK;

    /* for query shipping we do not collect plan info. */
    if (num_plan_nodes == 0)
        return;

    m_plan_node_num = num_plan_nodes;

    /* palloc memory for every node */
    m_plan_table = (PlanTableMultiData**)palloc0(num_plan_nodes * sizeof(PlanTableMultiData*));

    for (i = 0; i < num_plan_nodes; i++) {
        m_plan_table[i] = (PlanTableMultiData*)palloc0(sizeof(PlanTableMultiData));

        m_plan_table[i]->m_datum = (PlanTableData*)palloc0(sizeof(PlanTableData));
        m_plan_table[i]->m_isnull = (bool*)palloc0(PLANTABLECOLNUM * sizeof(bool));

        /* init m_isnull */
        rc = memset_s(m_plan_table[i]->m_isnull, PLANTABLECOLNUM * sizeof(bool), 1, PLANTABLECOLNUM * sizeof(bool));
        securec_check(rc, "\0", "\0");
    }
}

TupleDesc PlanTable::getTupleDesc()
{

    TupleDesc tupdesc;
    int attnum;
    int cur = 1;
    int analyze_num = m_analyze ? 1 : 0;
    int costs_num = m_costs ? 1 : 0;
    int verbose_num = m_verbose ? 1 : 0;

    attnum = (PLAN + 1) + analyze_num * NUM_ANALYZE_COLS + \
             costs_num * (NUM_COST_COLS + verbose_num * NUM_VER_COLS + m_pred_time + m_pred_row + m_pred_mem);

    /* If cost enabled and not query mem mode, don't show estimate_memory column */
    if (m_costs && !m_query_mem_mode)
        attnum = attnum - 1;

    /* If timing not enabled, don't show A-time column */
    if (!m_timing && m_analyze) {
        attnum = attnum - 1;
    }

    tupdesc = CreateTemplateTupleDesc(attnum, false, TAM_HEAP);
    for (int i = 0; i < SLOT_NUMBER; i++) {
        bool add_slot = false;

        /* Figure out whether current column should be added */
        if (i <= PLAN)
            add_slot = true;
        else if (m_analyze && plan_table_cols[i].disoption == ANAL_OPT) {
            if (!m_timing && plan_table_cols[i].val == ACTUAL_TIME) {
                add_slot = false;
            } else {
                add_slot = true;
            }
        } else if (m_costs && plan_table_cols[i].disoption == COST_OPT) {
            if (m_query_mem_mode || i != ESTIMATE_MEMORY)
                add_slot = true;
        } else if (m_costs && (m_verbose && plan_table_cols[i].disoption == VERBOSE_OPT)) {
            /* cost opt controls distinct display */
            add_slot = true;
        } else if (m_costs && (m_pred_time && plan_table_cols[i].disoption == PRED_OPT_TIME)) {
            add_slot = true;
        } else if (m_costs && (m_pred_row && plan_table_cols[i].disoption == PRED_OPT_ROW)) {
            add_slot = true;
        } else if (m_costs && (m_pred_mem && plan_table_cols[i].disoption == PRED_OPT_MEM)) {
            add_slot = true;
        }
        if (add_slot) {
            TupleDescInitEntry(tupdesc, (AttrNumber)cur, plan_table_cols[i].name, plan_table_cols[i].typid, -1, 0);
            m_col_loc[i] = cur - 1;
            cur++;
        } else
            m_col_loc[i] = -1;
    }

    return tupdesc;
}

TupleDesc PlanTable::getTupleDesc_detail()
{

    TupleDesc tupdesc;
    int attnum = EXPLAIN_TOTAL_ATTNUM;
    int i = 1;

    tupdesc = CreateTemplateTupleDesc(attnum, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "query id", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "plan parent node id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "plan node id", INT4OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "plan type", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "from datanode", BOOLOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "node name", TEXTOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "plan name", TEXTOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "start time", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "total time", FLOAT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "oper time", FLOAT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "plan rows", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "loops", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "ex cyc", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "inc cyc", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "ex cyc/rows", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "memory", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "peak query memory", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "shared hit", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "shared read", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "shared dirtied", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "shared written", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "local hit", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "local read", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "local dirtied", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "local written", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "temp read", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "temp written", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "blk read time", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "blk write time", FLOAT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "CU None", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "CU Some", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "sort method", TEXTOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "sort type", TEXTOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "sort space", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hash batch", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hash batch orignal", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hash bucket", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hash space", INT8OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hash file number", INT4OID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "query network", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "network poll time", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "llvm optimization", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "bloom filter", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "dfs block info", BOOLOID, -1, 0);

    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "merge inserted", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "merge updated", INT8OID, -1, 0);

    return tupdesc;
}

TupleDesc PlanTable::getTupleDesc(const char* attname)
{
    TupleDesc tupdesc;

    tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, attname, TEXTOID, -1, 0);
    return tupdesc;
}

void PlanTable::put(int type, Datum value)
{
    AssertEreport(type < SLOT_NUMBER && m_col_loc[type] >= 0, MOD_EXECUTOR, "unexpect input value");
    m_data[m_plan_node_id - 1][m_col_loc[type]] = value;
    m_isnull[m_plan_node_id - 1][m_col_loc[type]] = false;
}

void PlanTable::put(int nodeId, int smpId, int type, Datum value)
{
    if (nodeId == -1) {
        for (int i = 0; i < m_data_size; i++) {
            int dop = u_sess->instr_cxt.global_instr->getNodeDop(m_plan_node_id);
            for (int j = 0; j < dop; j++) {
                m_multi_info[i][m_plan_node_id - 1][j].m_Datum[type] = value;
                m_multi_info[i][m_plan_node_id - 1][j].m_Nulls[type] = false;
            }
        }
    } else {
        m_multi_info[nodeId][m_plan_node_id - 1][smpId].m_Datum[type] = value;
        m_multi_info[nodeId][m_plan_node_id - 1][smpId].m_Nulls[type] = false;
    }
}

void PlanTable::flush(DestReceiver* dest, int plantype)
{
    TupOutputState* tstate = NULL;
    tstate = begin_tup_output_tupdesc(dest, m_desc);
    switch (plantype) {
        case PLANINFO:
            flush_plan(tstate);
            break;
        case DETAILINFO:
        case STATICINFO:
        case VERBOSEINFO:
        case DATANODEINFO:
        case IOINFO:
        case PROFILEINFO:
        case RECURSIVEINFO:
        case QUERYSUMMARY:
            flush_other(tstate);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupported type %d", plantype)));
    }
    end_tup_output(tstate);
}

int PlanTable::print_plan(Portal portal, DestReceiver* dest)
{
    TupleTableSlot* slot = NULL;
    int current_tuple_count = 0;

    portal->tupDesc = m_desc;
    slot = MakeSingleTupleTableSlot(portal->tupDesc);

    current_tuple_count = RunGetSlotFromExplain(portal, slot, dest, m_size);

    ExecDropSingleTupleTableSlot(slot);

    return current_tuple_count;
}
void PlanTable::flush_plan(TupOutputState* tstate)
{
    int i = 0;
    for (i = 0; i < m_size; i++) {
        do_tup_output(tstate,
            m_data[i],
            tstate->slot->tts_tupleDescriptor->natts,
            m_isnull[i],
            tstate->slot->tts_tupleDescriptor->natts);
        pfree_ext(m_data[i]);
        pfree_ext(m_isnull[i]);
    }
    pfree_ext(m_data);
    pfree_ext(m_isnull);
    pfree_ext(m_pname.data);
}

void PlanTable::flush_other(TupOutputState* tstate)
{
    m_size = do_text_output_multiline(tstate, info_str->data);
    pfree_ext(info_str->data);
}

void PlanTable::set_pname(char* data)
{
    m_pname.data = data;
}

/*
 * @Description: set datanode name
 * @in nodename  - current datanode name to be set
 * @in smp_idx - current smp index
 * @in dop - degree of parallel, 1 means not parallel
 * @out - void
 */
void PlanTable::set_datanode_name(char* nodename, int smp_idx, int dop)
{
    if (dop == 1)
        appendStringInfo(info_str, "%s ", nodename);
    else
        appendStringInfo(info_str, "%s[worker %d] ", nodename, smp_idx);
}

template <bool set_name, bool set_space>
void PlanTable::set_plan_name()
{
    if (set_name) {
        if (m_has_write_planname == false) {
            appendStringInfo(info_str, "%3d --%s\n", m_plan_node_id, m_pname.data);
            m_has_write_planname = true;
        }
    }

    if (set_space) {
        appendStringInfoSpaces(info_str, 8);
    }
}
template void PlanTable::set_plan_name<true, true>();

/* --------------------------------function for explain plan--------------------- */
/*
 * Description: Transform the vaules into upper case.
 * Parameters:
 * @in str: string wait transform.
 * @in len: transform length.
 * Return: void
 */
static char* set_strtoupper(const char* str, uint32 len)
{
    if (str == NULL)
        return NULL;

    uint32 out_len = 0;
    if (len < strlen(str))
        out_len = len;
    else
        out_len = strlen(str);

    char* ptrout = (char*)palloc0(out_len + 1);

    for (uint32 i = 0; i < out_len; i++)
        ptrout[i] = pg_toupper((unsigned char)str[i]);

    ptrout[out_len] = '\0';

    return ptrout;
}

/*
 * Description: Make session id and prepare to insert.
 *              Session id is make up of thread start time and thread pid.
 * Parameters:
 * @in sessid: session_id.
 * Return: void
 */
void PlanTable::make_session_id(char* sessid)
{
    AssertEreport(sessid != NULL, MOD_EXECUTOR, "unexpect null value");

    errno_t rc = EOK;

    rc = snprintf_s(sessid,
        SESSIONIDLEN,
        SESSIONIDLEN - 1,
        "%ld.%lu",
        IS_THREAD_POOL_WORKER ? u_sess->proc_cxt.MyProcPort->SessionStartTime : t_thrd.proc_cxt.MyStartTime,
        IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);
    securec_check_ss(rc, "\0", "\0");
}

/*
 * Description: Set session_id, user_id, statement_id, query_id and plan node id for the query.
 * Parameters:
 * @in query_id: query id of current plan.
 * @in es: explain state.
 * Return: void
 */
void PlanTable::set_plan_table_ids(uint64 query_id, ExplainState* es)
{
    for (int i = 0; i < m_plan_node_num; i++) {
        make_session_id(m_plan_table[i]->m_datum->session_id);
        m_plan_table[i]->m_isnull[PT_SESSION_ID] = false;

        m_plan_table[i]->m_datum->user_id = GetCurrentUserId();
        m_plan_table[i]->m_isnull[PT_USER_ID] = false;

        /* Print error when statement_id that user input exceeded the STMTIDLEN. */
        if (es->statement_id != NULL) {
            uint32 s_len = strlen(es->statement_id);
            if (s_len > (STMTIDLEN - 1))
                ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("statement_id is too long. Input statement_id length=%u, however max length=%d.",
                            s_len,
                            STMTIDLEN - 1)));

            errno_t rc = strncpy_s(m_plan_table[i]->m_datum->statement_id, STMTIDLEN, es->statement_id, s_len);
            securec_check(rc, "\0", "\0");

            m_plan_table[i]->m_isnull[PT_STATEMENT_ID] = false;
        }

        m_plan_table[i]->m_datum->query_id = u_sess->debug_query_id;
        m_plan_table[i]->m_isnull[PT_QUERY_ID] = false;

        m_plan_table[i]->m_datum->node_id = i + 1;
        m_plan_table[i]->m_isnull[PT_NODE_ID] = false;
    }
}

/*
 * Description: Set operation and options for remote query and stream node.
 * Parameters:
 * @in pname: plain description for node.
 * @out operation: node operation.
 * @out options: node options.
 * Return: void
 */
void PlanTable::set_plan_table_streaming_ops(char* pname, char** operation, char** options)
{
    errno_t rc = EOK;
    char* start = NULL;
    char* end = NULL;

    if (strncasecmp("Streaming", pname, strlen("Streaming")) == 0 ||
        strncasecmp("Vector Streaming", pname, strlen("Vector Streaming")) == 0) {
        start = strrchr(pname, ':') + 2;
        end = strrchr(pname, ')');
        uint32 option_len = end - start;
        *options = (char*)palloc0(option_len + 1);
        rc = memcpy_s(*options, option_len + 1, start, option_len);
        securec_check(rc, "\0", "\0");

        if (pname[0] == 'S')
            *operation = "Streaming";
        else
            *operation = "Vector Streaming";
    }
}

/*
 * Description: For join node, we may need to set option 'CARTESIAN' to fit with A db.
 * Parameters:
 * @in plan: plan node.
 * @int options: node options.
 * Return: void
 */
void PlanTable::set_plan_table_join_options(Plan* plan, char** options)
{
    switch (nodeTag(plan)) {
        case T_NestLoop:
        case T_VecNestLoop: {
            Join* nestloop = (Join*)plan;
            if (nestloop->joinqual == NULL)
                *options = "CARTESIAN";
        } break;
        case T_VecMergeJoin:
        case T_MergeJoin: {
            MergeJoin* mergejoin = (MergeJoin*)plan;
            if (mergejoin->mergeclauses == NULL)
                *options = "CARTESIAN";
        } break;
        case T_HashJoin:
        case T_VecHashJoin: {
            HashJoin* hashjoin = (HashJoin*)plan;
            if (hashjoin->hashclauses == NULL)
                *options = "CARTESIAN";
        } break;
        default:
            break;
    }
}

/*
 * Description: Set operation and options for node except stream node.
 * Parameters:
 * @in plan_node_id: plan node id.
 * @int operation: node operation.
 * @int options: node options.
 * Return: void
 */
void PlanTable::set_plan_table_ops(int plan_node_id, char* operation, char* options)
{
    errno_t rc = EOK;

    /*
     * for query shipping we do not collect plan info.
     * However we need collect plan info for select for update.
     */
    if (plan_node_id == 0) {
        if (IsExplainPlanSelectForUpdateStmt)
            plan_node_id = 1;
        else
            return;
    }

    if (operation != NULL) {
        /* Transform the vaules into upper case. */
        operation = set_strtoupper(operation, OPERATIONLEN);
        rc = strncpy_s(m_plan_table[plan_node_id - 1]->m_datum->operation, OPERATIONLEN, operation, strlen(operation));
        securec_check(rc, "\0", "\0");
        pfree(operation);

        m_plan_table[plan_node_id - 1]->m_isnull[PT_OPERATION] = false;
    }
    if (options != NULL) {
        options = set_strtoupper(options, OPTIONSLEN);
        rc = strncpy_s(m_plan_table[plan_node_id - 1]->m_datum->options, OPTIONSLEN, options, strlen(options));
        securec_check(rc, "\0", "\0");
        pfree(options);

        m_plan_table[plan_node_id - 1]->m_isnull[PT_OPTIONS] = false;
    }
}

/*
 * Description: Set object_type according to relkind in pg_class.
 * Parameters:
 * @in rte: related ralation.
 * @out object_type: object type.
 * Return: void
 */
void PlanTable::set_object_type(RangeTblEntry* rte, char** object_type)
{
    if (rte == NULL)
        return;
    /* 1.It's on a real relation */
    if (rte->rtekind == RTE_RELATION) {
        switch (rte->relkind) {
            case 'r':
                *object_type = "TABLE";
                break;
            case 'i':
                *object_type = "INDEX";
                break;
            case 'S':
                *object_type = "SEQUENCE";
                break;
            case 't':
                *object_type = "TOASTVALUE";
                break;
            case 'v':
                *object_type = "VIEW";
                break;
            case 'c':
                *object_type = "COMPOSITE TYPE";
                break;
            case 'f':
                *object_type = "FOREIGN TABLE";
                break;
            case 'u':
                *object_type = "UNCATALOGED";
                break;
            default:
                break;
        }
    } else {
        /* 2. For other rte kind. */
        switch (rte->rtekind) {
            case RTE_SUBQUERY:
                *object_type = "SUBQUERY";
                break;
            case RTE_JOIN:
                *object_type = "JOIN";
                break;
            case RTE_FUNCTION:
                *object_type = "FUNCTION";
                break;
            case RTE_VALUES:
                *object_type = "VALUES";
                break;
            case RTE_CTE:
                *object_type = "CTE";
                break;
#ifdef PGXC
            case RTE_REMOTE_DUMMY: /* RTEs created by remote plan reduction */
                *object_type = "REMOTE_QUERY";
                break;
#endif /* PGXC */
            default:
                break;
        }
    }
}

/*
 * Description: Set object_name, object_type, object_owner for one paln node.
 * Parameters:
 * @in plan_node_id: plan node id.
 * @in object_name: object name.
 * @in object_type: object type.
 * @in object_owner: object schema.
 * Return: void
 */
void PlanTable::set_plan_table_objs(
    int plan_node_id, const char* object_name, const char* object_type, const char* object_owner)
{
    AssertEreport(plan_node_id <= m_plan_node_num, MOD_EXECUTOR, "plan_node_id exceeds max value");

    errno_t rc = EOK;

    /*
     * for query shipping we do not collect plan info.
     * However we need collect plan info for select for update.
     */
    if (plan_node_id == 0) {
        if (IsExplainPlanSelectForUpdateStmt)
            plan_node_id = 1;
        else
            return;
    }

    /* For 'INDEX' operation, we may set twice, so do cleanning before set. */
    if (object_name != NULL) {
        /* the max object name length is 'NAMEDATALEN - 1' byte. */
        rc = memset_s(m_plan_table[plan_node_id - 1]->m_datum->object_name, NAMEDATALEN, '0', NAMEDATALEN);
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(m_plan_table[plan_node_id - 1]->m_datum->object_name, NAMEDATALEN, object_name, NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");

        m_plan_table[plan_node_id - 1]->m_isnull[PT_OBJECT_NAME] = false;
    }

    if (object_type != NULL) {
        rc = memset_s(m_plan_table[plan_node_id - 1]->m_datum->object_type, OBJECTLEN, '0', OBJECTLEN);
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(m_plan_table[plan_node_id - 1]->m_datum->object_type, OBJECTLEN, object_type, OBJECTLEN - 1);
        securec_check(rc, "\0", "\0");
        m_plan_table[plan_node_id - 1]->m_isnull[PT_OBJECT_TYPE] = false;
    }

    if (object_owner != NULL) {
        rc = memset_s(m_plan_table[plan_node_id - 1]->m_datum->object_owner, NAMEDATALEN, '0', NAMEDATALEN);
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(
            m_plan_table[plan_node_id - 1]->m_datum->object_owner, NAMEDATALEN, object_owner, NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");

        m_plan_table[plan_node_id - 1]->m_isnull[PT_OBJECT_OWNER] = false;
    }
}

/*
 * Description: Set targetlist into column 'projection' for one plan node.
 * Parameters:
 * @in plan_node_id: plan node id.
 * @in tlist: targetlist of node.
 * Return: void
 */
void PlanTable::set_plan_table_projection(int plan_node_id, List* tlist)
{
    if (tlist == NULL)
        return;

    ListCell* lc = NULL;
    bool first = true;

    /*
     * for query shipping we do not collect plan info.
     * However we need collect plan info for select for update.
     */
    if (plan_node_id == 0) {
        if (IsExplainPlanSelectForUpdateStmt)
            plan_node_id = 1;
        else
            return;
    }

    m_plan_table[plan_node_id - 1]->m_datum->projection = makeStringInfo();

    /* Add the targetlist into column 'projection' till the length exceed PROJECTIONLEN.*/
    foreach (lc, tlist) {
        if (m_plan_table[plan_node_id - 1]->m_datum->projection->len + strlen((const char*)lfirst(lc)) > PROJECTIONLEN)
            break;

        if (!first)
            appendStringInfoString(m_plan_table[plan_node_id - 1]->m_datum->projection, ", ");

        appendStringInfoString(m_plan_table[plan_node_id - 1]->m_datum->projection, (const char*)lfirst(lc));

        first = false;
    }

    m_plan_table[plan_node_id - 1]->m_isnull[PT_PROJECTION] = false;
}

/*
 * Description: Set cost and cardinality into PlanTable.
 * Parameters:
 * @in plan_cost: plan cost.
 * @in plan_cardinality: rows accessed by current operation.
 * Return: void
 */
void PlanTable::set_plan_table_cost_card(int plan_node_id, double plan_cost, double plan_cardinality)
{
    m_plan_table[plan_node_id - 1]->m_datum->cost = plan_cost;
    m_plan_table[plan_node_id - 1]->m_datum->cardinality = plan_cardinality;
    m_plan_table[plan_node_id - 1]->m_isnull[PT_COST] = false;
    m_plan_table[plan_node_id - 1]->m_isnull[PT_CARDINALITY] = false;
}

/*
 * Description: Call heap_insert to insert all nodes tuples of the plan into table.
 * Parameters:
 * Return: void
 */
void PlanTable::insert_plan_table_tuple()
{
    const char* plan_table_name = T_PLAN_TABLE_DATA;
    Relation plan_table;
    TupleDesc plan_table_des;
    HeapTuple tuple;
    Datum new_record[PLANTABLECOLNUM];
    errno_t rc = EOK;

    Oid plan_table_oid = RelnameGetRelid(plan_table_name);
    if (plan_table_oid == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("Relation 'plan_table_data' does not exist")));

    plan_table = heap_open(plan_table_oid, RowExclusiveLock);

    plan_table_des = RelationGetDescr(plan_table);

    for (int i = 0; i < m_plan_node_num; i++) {
        /*
         * Build a tuple to insert
         */
        rc = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
        securec_check(rc, "\0", "\0");

        new_record[PT_SESSION_ID] = CStringGetTextDatum(m_plan_table[i]->m_datum->session_id);
        new_record[PT_USER_ID] = ObjectIdGetDatum(m_plan_table[i]->m_datum->user_id);
        new_record[PT_STATEMENT_ID] = CStringGetTextDatum(m_plan_table[i]->m_datum->statement_id);
        new_record[PT_QUERY_ID] = UInt64GetDatum(m_plan_table[i]->m_datum->query_id);
        new_record[PT_NODE_ID] = Int32GetDatum(m_plan_table[i]->m_datum->node_id);
        new_record[PT_OPERATION] = CStringGetTextDatum(m_plan_table[i]->m_datum->operation);
        new_record[PT_OPTIONS] = CStringGetTextDatum(m_plan_table[i]->m_datum->options);
        new_record[PT_OBJECT_NAME] = NameGetDatum(m_plan_table[i]->m_datum->object_name);
        new_record[PT_OBJECT_TYPE] = CStringGetTextDatum(m_plan_table[i]->m_datum->object_type);
        new_record[PT_OBJECT_OWNER] = NameGetDatum(m_plan_table[i]->m_datum->object_owner);

        if (m_plan_table[i]->m_datum->projection != NULL)
            new_record[PT_PROJECTION] = CStringGetTextDatum(m_plan_table[i]->m_datum->projection->data);

        new_record[PT_COST] = Float8GetDatum(m_plan_table[i]->m_datum->cost);
        new_record[PT_CARDINALITY] = Float8GetDatum(m_plan_table[i]->m_datum->cardinality);

        tuple = (HeapTuple)heap_form_tuple(plan_table_des, new_record, m_plan_table[i]->m_isnull);

        /*
         * Insert new record into plan_table table
         */
        (void)simple_heap_insert(plan_table, tuple);
    }

    /* make sure whether need to keep lock till commit */
    heap_close(plan_table, RowExclusiveLock);
}

/*
 * Description: We only control the select/delete permission for pan_table_data.
 * 				For 'plan_table_data' we will handing its permission check espacially, We do not allow ordinary user to
 * select/D/U/I
 * 1. "Select xx from plan_table" the rte has subquery contain "plan_table_data" and will be pull up in
 * subquery_planner.
 * 2. In function ExecCheckRTEPerms will check the permission for every rte including those been pulled up.
 * 3. So we need to check whether has rte of "plan_table_data" in origin FromClause in this function for select/delete
 * cmd. Parameters:
 * @ in rangeTable: rte list of query
 * Return: return true if there is no rte of 'plan_table_data' in origin FromClause.
 */
bool checkSelectStmtForPlanTable(List* rangeTable)
{
    OnlySelectFromPlanTable = false;

    ListCell* lc = NULL;

    foreach (lc, rangeTable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (rte->relname != NULL && strcasecmp(rte->relname, T_PLAN_TABLE_DATA) == 0)
            return false;
    }

    return true;
}

/*
 * Description: Deal with 'plan_table_data' permission checking here.
 * 				We do not allow ordinary user to select from 'plan_table_data'.
 * Parameters:
 * @ in rangeTable: rte of "plan_table_data"
 * Return: return -1 for other action(insert/update).
 *         return 1 if pass permission check.
 *         return 0 if have no permission.
 */
int checkPermsForPlanTable(RangeTblEntry* rte)
{
    /*
     * Note that:
     * 1. Planner will replace 'plan_table' with 'plan_table_data' in subquery_planner routine.
     * If it is only select/delete from plan_table view, we should return ture.
     * 2. If current user has superuser or sysdba privileges, we will allow the user do select/delelte on
     * 'plan_table_data'.
     */
    /* For select action */
    if (rte->requiredPerms == ACL_SELECT) {
        if (OnlySelectFromPlanTable || superuser())
            return 1;
        else
            return 0;
    } else if (rte->requiredPerms == (ACL_SELECT + ACL_DELETE) /* for plan_table. */
             || rte->requiredPerms == ACL_DELETE /* for plan_table_data.*/) {
        /* For delete action */
        if (OnlyDeleteFromPlanTable || superuser())
            return 1;
        else
            return 0;
    } else {
        /* For other action(insert/update) will be contorl by rel_acl. */
        return -1;
    } 
}

/* ----------------------End for explain plan----------------------------------- */
static inline void appendCSV(StringInfo buf, const char* data)
{
    const char* p = data;
    char c;

    /* avoid confusing an empty string with NULL */
    if (p == NULL)
        return;

    appendStringInfoCharMacro(buf, '"');
    while ((c = *p++) != '\0') {
        if (c == '"')
            appendStringInfoCharMacro(buf, '"');
        appendStringInfoCharMacro(buf, c);
    }
    appendStringInfoCharMacro(buf, '"');
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void show_sort_group_keys(PlanState* planstate, const char* qlabel, int nkeys, const AttrNumber* keycols,
    const Oid* sortOperators, const Oid* collations, const bool* nullsFirst, List* ancestors, ExplainState* es)
{
    Plan* plan = planstate->plan;
    List* context = NIL;
    List* result = NIL;
    StringInfoData sortkeybuf;
    bool useprefix = false;
    int keyno;

    if (nkeys <= 0)
        return;

    initStringInfo(&sortkeybuf);

    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);

    useprefix = (list_length(es->rtable) > 1 || es->verbose);

    for (keyno = 0; keyno < nkeys; keyno++) {
        /* find key expression in tlist */
        AttrNumber keyresno = keycols[keyno];
        TargetEntry* target = get_tle_by_resno(plan->targetlist, keyresno);

        /*
         * Start/With support
         *
         * Skip pseudo target entry in SORT list
         */
        if (IsPseudoInternalTargetEntry(target)) {
            result = lappend(result, pstrdup(target->resname));
            continue;
        }

        char* exprstr = NULL;

        if (target == NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no tlist entry for key %d", keyresno)));
        /* Deparse the expression, showing any top-level cast */
        exprstr = deparse_expression((Node*)target->expr, context, useprefix, true);
        resetStringInfo(&sortkeybuf);
        appendStringInfoString(&sortkeybuf, exprstr);
        /* Append sort order information, if relevant */
        if (sortOperators != NULL)
            show_sortorder_options(
                &sortkeybuf, (Node*)target->expr, sortOperators[keyno], collations[keyno], nullsFirst[keyno]);
        /* Emit one property-list item per sort key */
        result = lappend(result, pstrdup(sortkeybuf.data));
    }

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
        es->planinfo->m_verboseInfo->set_plan_name<false, true>();

        appendStringInfo(es->planinfo->m_verboseInfo->info_str, "%s: ", qlabel);
        ExplainPrettyList(result, es);
    } else {
        ExplainPropertyList(qlabel, result, es);
    }
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void show_sortorder_options(StringInfo buf, const Node* sortexpr, Oid sortOperator, Oid collation, bool nullsFirst)
{
    Oid sortcoltype = exprType(sortexpr);
    bool reverse = false;
    TypeCacheEntry* typentry = NULL;

    typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

    /*
     * Print COLLATE if it's not default.  There are some cases where this is
     * redundant, eg if expression is a column whose declared collation is
     * that collation, but it's hard to distinguish that here.
     */
    if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID) {
        char* collname = get_collation_name(collation);

        if (collname == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for collation %u", collation)));
        appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
    }

    /* Print direction if not ASC, or USING if non-default sort operator */
    if (sortOperator == typentry->gt_opr) {
        appendStringInfoString(buf, " DESC");
        reverse = true;
    } else if (sortOperator != typentry->lt_opr) {
        char* opname = get_opname(sortOperator);

        if (opname == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", sortOperator)));
        appendStringInfo(buf, " USING %s", opname);
        /* Determine whether operator would be considered ASC or DESC */
        (void)get_equality_op_for_ordering_op(sortOperator, &reverse);
    }

    /* Add NULLS FIRST/LAST only if it wouldn't be default */
    if (nullsFirst && !reverse) {
        appendStringInfoString(buf, " NULLS FIRST");
    } else if (!nullsFirst && reverse) {
        appendStringInfoString(buf, " NULLS LAST");
    }
}

/*
 * Description: Show tablesample properties.
 *
 * Parameters:
 * @in plan: Scan plan.
 * @in planstate: PlanState node.
 * @in ancestors: The ancestors list is a list of the PlanState's parent PlanStates.
 * @in es: Explain state.
 *
 * Return: void
 */
static void show_tablesample(Plan* plan, PlanState* planstate, List* ancestors, ExplainState* es)
{
    List* context = NIL;
    bool useprefix = false;
    const char* method_name = NULL;
    List* params = NIL;
    char* repeatable = NULL;
    ListCell* lc = NULL;
    TableSampleClause* tsc = ((Scan*)plan)->tablesample;

    if (tsc == NULL) {
        return;
    }

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);
    useprefix = list_length(es->rtable) > 1;

    /* Get the tablesample method name */
    method_name =
        (tsc->sampleType == SYSTEM_SAMPLE ? "system" : (tsc->sampleType == BERNOULLI_SAMPLE ? "bernoulli" : "hybrid"));

    /* Deparse parameter expressions */
    foreach (lc, tsc->args) {
        Node* arg = (Node*)lfirst(lc);

        params = lappend(params, deparse_expression(arg, context, useprefix, false));
    }
    if (tsc->repeatable) {
        repeatable = deparse_expression((Node*)tsc->repeatable, context, useprefix, false);
    } else {
        repeatable = NULL;
    }

    /* Print results */
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        bool first = true;

        appendStringInfoSpaces(es->str, es->indent * 2);
        appendStringInfo(es->str, "Sampling: %s (", method_name);
        foreach (lc, params) {
            if (!first)
                appendStringInfoString(es->str, ", ");
            appendStringInfoString(es->str, (const char*)lfirst(lc));
            first = false;
        }
        appendStringInfoChar(es->str, ')');
        if (repeatable != NULL) {
            appendStringInfo(es->str, " REPEATABLE (%s)", repeatable);
        }
        appendStringInfoChar(es->str, '\n');
    } else {
        ExplainPropertyText("Sampling Method", method_name, es);
        ExplainPropertyList("Sampling Parameters", params, es);
        if (repeatable != NULL) {
            ExplainPropertyText("Repeatable Seed", repeatable, es);
        }
    }
    list_free_deep(params);
}

void PlanTable::flush_data_to_file()
{
    int i = 0;
    int j = 0;
    int k = 0;
    Datum* values = NULL;
    bool* nulls = NULL;
    bool is_null = false;
    Datum data;
    Datum val;
    Oid typoid;
    Oid foutoid;
    bool typisvarlena = false;
    char* result = NULL;
    bool from_datanode = false;
    int node_num = 0;

    for (i = 0; i < m_plan_size; i++) {
        values = m_multi_info[0][i][0].m_Datum;
        from_datanode = values[FROM_DATANODE];
        node_num = from_datanode ? m_data_size : 1;

        for (j = 0; j < node_num; j++) {
            values = m_multi_info[j][i][0].m_Datum;
            nulls = m_multi_info[j][i][0].m_Nulls;

            for (k = 0; k < EXPLAIN_TOTAL_ATTNUM; k++) {
                is_null = nulls[k];
                data = values[k];
                if (is_null) {
                    if (k == EXPLAIN_TOTAL_ATTNUM - 1)
                        appendBinaryStringInfo(info_str, "", 0);
                    else
                        appendBinaryStringInfo(info_str, ",", 1);
                } else {
                    typoid = m_desc->attrs[k]->atttypid;

                    getTypeOutputInfo(typoid, &foutoid, &typisvarlena);

                    if (typisvarlena)
                        val = PointerGetDatum(PG_DETOAST_DATUM(data));
                    else
                        val = data;

                    result = OidOutputFunctionCall(foutoid, val);

                    if (val != data)
                        pfree(DatumGetPointer(val));
                    if (typisvarlena)
                        appendCSV(info_str, result);
                    else if (typoid == BOOLOID) {
                        if (strcmp(result, "t") == 0)
                            appendStringInfo(info_str, "true");
                        else
                            appendStringInfo(info_str, "false");
                    } else
                        appendStringInfo(info_str, "%s", result);

                    if (k != EXPLAIN_TOTAL_ATTNUM - 1)
                        appendStringInfoChar(info_str, ',');
                }
            }

            appendStringInfoChar(info_str, '\n');
        }
    }
}

/*
 * Insert obs scan information into pg_obsscaninfo table
 */
void insert_obsscaninfo(
    uint64 queryid, const char* rel_name, int64 file_count, double scan_data_size, double total_time, int format)
{
    Relation pgobsscaninforel;
    HeapTuple htup;
    bool nulls[Natts_pg_obsscaninfo];
    Datum values[Natts_pg_obsscaninfo];
    int i;
    TimestampTz ts = GetCurrentTimestamp();
    StringInfo insert_stmt1, insert_stmt2;
    const char* billing_info = "unbilled";
    char* file_format = NULL;

    if (format == 0)
        file_format = "orc";
    else if (format == 1)
        file_format = "text";
    else if (format == 2)
        file_format = "csv";
    else
        file_format = "unknown";

    MemoryContext current_ctx = CurrentMemoryContext;

    /* Initialize tuple data structures */
    for (i = 0; i < Natts_pg_obsscaninfo; i++) {
        nulls[i] = false;
        values[i] = (Datum)0;
    }

    values[Anum_pg_obsscaninfo_query_id - 1] = Int64GetDatum(queryid);
    values[Anum_pg_obsscaninfo_user_id - 1] = CStringGetTextDatum(u_sess->misc_cxt.CurrentUserName);
    values[Anum_pg_obsscaninfo_table_name - 1] = CStringGetTextDatum(rel_name);
    values[Anum_pg_obsscaninfo_file_type - 1] = CStringGetTextDatum(file_format);
    values[Anum_pg_obsscaninfo_time_stamp - 1] = TimestampTzGetDatum(ts);
    values[Anum_pg_obsscaninfo_actual_time - 1] = Float8GetDatum(total_time);
    values[Anum_pg_obsscaninfo_file_scanned - 1] = file_count;
    values[Anum_pg_obsscaninfo_data_size - 1] = Float8GetDatum(scan_data_size);
    values[Anum_pg_obsscaninfo_billing_info - 1] = CStringGetTextDatum(billing_info);

    /* insert the new row into pg_obsscaninfo catalog table */
    pgobsscaninforel = heap_open(ObsScanInfoRelationId, RowExclusiveLock);
    htup = heap_form_tuple(pgobsscaninforel->rd_att, values, nulls);
    (void)simple_heap_insert(pgobsscaninforel, htup);

    CatalogUpdateIndexes(pgobsscaninforel, htup);
    tableam_tops_free_tuple(htup);
    heap_close(pgobsscaninforel, RowExclusiveLock);

    /* insert the new row into pg_obsscaninfo catalog table */
    pgobsscaninforel = heap_open(GSObsScanInfoRelationId, RowExclusiveLock);
    htup = heap_form_tuple(pgobsscaninforel->rd_att, values, nulls);
    (void)simple_heap_insert(pgobsscaninforel, htup);

    CatalogUpdateIndexes(pgobsscaninforel, htup);
    tableam_tops_free_tuple(htup);
    heap_close(pgobsscaninforel, RowExclusiveLock);

    elog(DEBUG1, "SyncUp with other CN");

    insert_stmt1 = makeStringInfo();
    appendStringInfo(insert_stmt1,
        "insert into pg_obsscaninfo values (%lu, '%s', '%s', '%s', '%s', '%lf', %ld, %lf, '%s');",
        queryid, /* we delibrately let it as a int64 due to unsupporting uint64 in pg */
        u_sess->misc_cxt.CurrentUserName,
        rel_name,
        file_format,
        timestamptz_to_str(ts),
        total_time,
        file_count,
        scan_data_size,
        billing_info);

    insert_stmt2 = makeStringInfo();
    appendStringInfo(insert_stmt2,
        "insert into gs_obsscaninfo values (%lu, '%s', '%s', '%s', '%s', '%lf', %ld, %lf, '%s');",
        queryid, /* we delibrately let it as a int64 due to unsupporting uint64 in pg */
        u_sess->misc_cxt.CurrentUserName,
        rel_name,
        file_format,
        timestamptz_to_str(ts),
        total_time,
        file_count,
        scan_data_size,
        billing_info);

    elog(DEBUG1, "%s", insert_stmt1->data);
    elog(DEBUG1, "%s", insert_stmt2->data);

    PG_TRY();
    {
        /*
         * If insert job status in local success and only synchronize to other coordinator fail,
         * we should consider the worker success, so that we at least have one record for billing.
         */
        ExecUtilityStmtOnNodes(insert_stmt1->data, NULL, false, false, EXEC_ON_COORDS, false);
        ExecUtilityStmtOnNodes(insert_stmt2->data, NULL, false, false, EXEC_ON_COORDS, false);
    }
    PG_CATCH();
    {
        /* t_thrd.int_cxt.InterruptHoldoffCount is set 0 in function "errfinish"
         * So if we invoke LWLockrelease, we must call HOLD_INTERRUPT
         * the function called by OBSInstrumentation::insertData() which holds
         * OBSRuntimeLock.
         */
        HOLD_INTERRUPTS();

        MemoryContextSwitchTo(current_ctx);

        /* Save error info */
        ErrorData* edata = CopyErrorData();

        FlushErrorState();
        ereport(LOG,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("Synchronize OBS scan info to other coordinator failed, OBS billing info may be out of sync"),
                errdetail("Synchronize fail reason: %s.", edata->message)));

        FreeErrorData(edata);
    }
    PG_END_TRY();

    pfree_ext(insert_stmt1);
    pfree_ext(insert_stmt2);
}

static void show_unique_check_info(PlanState *planstate, ExplainState *es)
{
    Agg *aggnode = (Agg *) planstate->plan;

    if (aggnode->unique_check) {
        const char *str = "Unique Check Required";

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfo(es->planinfo->m_verboseInfo->info_str, "%s\n", str);
        }
        else {
            if (es->format == EXPLAIN_FORMAT_TEXT && !es->wlm_statistics_plan_max_digit) {
                appendStringInfoSpaces(es->str, es->indent * 2);
                appendStringInfo(es->str, "%s\n", str);
            }
        }
    }
}

void ExplainDatumProperty(char const *name, Datum const value, Oid const type, ExplainState* es)
{
    Datum output_datum = 0;
    char const *output = nullptr;

    if (type_is_array_domain(type)) {
        output_datum = OidFunctionCall2(F_ARRAY_OUT, value, type);
        output = DatumGetCString(output_datum);
        ExplainPropertyText(name, output, es);
    } else {
        switch (type) {
            case INT1OID:
                ExplainPropertyInteger(name, DatumGetInt8(value), es);
                break;
            case INT2OID:
                ExplainPropertyInteger(name, DatumGetInt16(value), es);
                break;
            case INT4OID:
                ExplainPropertyInteger(name, DatumGetInt32(value), es);
                break;
            case INT8OID:
                ExplainPropertyLong(name, DatumGetInt64(value), es);
                break;
            case FLOAT4OID:
                ExplainPropertyFloat(name, DatumGetFloat4(value), 6, es);
                break;
            case FLOAT8OID:
                ExplainPropertyFloat(name, DatumGetFloat8(value), 10, es);
                break;
            default:
                output = Datum_to_string(value, type, type == InvalidOid);
                ExplainPropertyText(name, output, es);
                break;
        }
    }
}
/*
 * this function explains a group of model information (recursively if necessary)
 * as provided by the list
 */
ListCell* ExplainTrainInfoGroup(List const* group, ExplainState *es, bool const group_opened)
{
    ListCell *lc = nullptr;
    TrainingInfo *train_info = nullptr;

    /*
     * in a single traversal of the group there will (potentially) be
     * recursive calls that will print nested sub-groups (by properly
     * opening and closing them)
     */
    foreach(lc, group) {
        train_info = lfirst_node(TrainingInfo, lc);
        if (train_info->open_group) {
            /*
             * if the cell for some reason has both opening and closing booleans set we ignore it
             */
            if (train_info->close_group)
                continue;

            if (!train_info->name)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("the name of a group cannot be empty (when opening)")));

            // open the group at the output
            ExplainOpenGroup(train_info->name, train_info->name, true, es);
            // here comes a recursive call to print the group
            List next_group;
            next_group.length = 0; // not needed for us, not properly set (you are warned)
            next_group.type = group->type;
            next_group.head = lnext(lc); // next group continues with the first element of the group
            next_group.tail = group->tail;

            /*
             * once we have printed a whole group we gotta fast-forward to the first element after the group.
             * the returned ListCell is the closing element of the group we just printed (foreach will do the rest)
             */
            lc = ExplainTrainInfoGroup(&next_group, es, true);
            /*
             * if there are more opening groups than closing one at some point the list will have to get
             * exhausted and there will be recursive call that cannot cleanly finish
             */
            if (!lc)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("group of name \"%s\" was not closed", train_info->name)));

            auto current_train_info = lfirst_node(TrainingInfo, lc);

            /*
             * the name of opening and closing groups has to match, otherwise error.
             * observe that both strings are non-empty (otherwise error before this)
             */
            if (strcmp(train_info->name, current_train_info->name) != 0)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("group of name \"%s\" was not properly closed, closing group name was \"%s\"",
                               train_info->name, current_train_info->name)));
            continue;
        } else if (train_info->close_group) {
            /*
             * this is in charge of validating that closing a group is correct (by having opened at the
             * parent call)
             */
            if (!group_opened)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("badly formed group of name \"%s\" of training information (opening group not found)",
                               train_info->name)));

            if (!train_info->name)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("the name of a group cannot be empty (when closing)")));

            ExplainCloseGroup(train_info->name, train_info->name, true, es);
            // we return the closing ListCell
            break;
        } else {
            /*
             * we output the next object of the group
             */
            ExplainDatumProperty(train_info->name, train_info->value, train_info->type, es);
        }
    }
    return lc;
}
void do_model_explain(ExplainState *es, const Model *model)
{
    ListCell *lc = nullptr;
    ExplainBeginOutput(es);

    /*
     * there is a common part of each model that can be formatted now
     */
    ExplainOpenGroup("General information", "General information", true, es);
    ExplainPropertyText("Name", model->model_name, es);
    ExplainPropertyText("Algorithm", algorithm_ml_to_string(model->algorithm), es);
    if (model->sql != NULL)
        ExplainPropertyText("Query", model->sql, es);

    char const *return_type_str = nullptr;
    switch (model->return_type) {
        case BOOLOID:
            return_type_str = prediction_type_to_string(TYPE_BOOL);
            break;
        case BYTEAOID:
            return_type_str = prediction_type_to_string(TYPE_BYTEA);
            break;
        case INT4OID:
            return_type_str = prediction_type_to_string(TYPE_INT32);
            break;
        case INT8OID:
            return_type_str = prediction_type_to_string(TYPE_INT64);
            break;
        case FLOAT4OID:
            return_type_str = prediction_type_to_string(TYPE_FLOAT32);
            break;
        case FLOAT8OID:
            return_type_str = prediction_type_to_string(TYPE_FLOAT64);
            break;
        case FLOAT8ARRAYOID:
            return_type_str = prediction_type_to_string(TYPE_FLOAT64ARRAY);
            break;
        case NUMERICOID:
            return_type_str = prediction_type_to_string(TYPE_NUMERIC);
            break;
        case TEXTOID:
            return_type_str = prediction_type_to_string(TYPE_TEXT);
            break;
        case VARCHAROID:
            return_type_str = prediction_type_to_string(TYPE_VARCHAR);
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid return type (%d) for model of name \"%s\"", model->return_type,
                           model->model_name)));
    }
    ExplainPropertyText("Return type", return_type_str, es);
    ExplainPropertyFloat("Pre-processing time", model->pre_time_secs, 6, es);
    ExplainPropertyFloat("Execution time", model->exec_time_secs, 6, es);
    ExplainPropertyInteger("Processed tuples", model->processed_tuples, es);
    ExplainPropertyInteger("Discarded tuples", model->discarded_tuples, es);
    ExplainCloseGroup("General information", "General information", true, es);

    /*
     * hyper-parameters are now output
     */
    ExplainOpenGroup("Hyper-parameters", "Hyper-parameters", true, es);
    foreach(lc, model->hyperparameters) {
        auto hyperparameter = lfirst_node(Hyperparameter, lc);
        ExplainDatumProperty(hyperparameter->name, hyperparameter->value, hyperparameter->type, es);
    }
    ExplainCloseGroup("Hyper-parameters", "Hyper-parameters", true, es);

    /*
     * scores are now output
     */
    ExplainOpenGroup("Scores", "Scores", true, es);
    foreach(lc, model->scores) {
        auto training_score = lfirst_node(TrainingScore, lc);
        ExplainPropertyFloat(training_score->name, training_score->value, 10, es);
    }
    ExplainCloseGroup("Scores", "Scores", true, es);

    /*
     * Model-dependent information
     */
    AlgorithmAPI *algo_api = get_algorithm_api(model->algorithm);
    if (algo_api->explain && model->data.version > DB4AI_MODEL_V00) {
        List *train_data = algo_api->explain(algo_api, &model->data, model->return_type);
        if (train_data) {
            ExplainOpenGroup("Train data", "Train data", true, es);
            ExplainTrainInfoGroup(train_data, es, false);
            ExplainCloseGroup("Train data", "Train data", true, es);
        }
    }

    /* emit closing boilerplate */
    ExplainEndOutput(es);
}
