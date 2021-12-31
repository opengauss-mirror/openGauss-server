/*-------------------------------------------------------------------------
 *
 * auto_explain.c
 *
 *
 * Copyright (c) 2008-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      commands/auto_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include "optimizer/streamplan.h"
#include "commands/explain.h"
#include "executor/instrument.h"
#include "utils/guc.h"
#include "parser/parsetree.h"
#include "instruments/instr_slow_query.h"
#include "instruments/instr_statement.h"
#include "workload/gscgroup.h"
#include "workload/statctl.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
void InitPlan(QueryDesc* queryDesc, int eflags);

/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static instr_time plan_time;
#define auto_explain_enabled() \
    (u_sess->attr.attr_storage.log_min_duration_statement >= 0 && u_sess->exec_cxt.nesting_level == 0)
#define auto_explain_plan() \
	(!u_sess->attr.attr_sql.under_explain && u_sess->attr.attr_resource.enable_auto_explain)
#ifdef ENABLE_MULTIPLE_NODES
#define is_valid_query(queryDesc) \
    (queryDesc!=NULL && queryDesc->sourceText != NULL && strcmp(queryDesc->sourceText, "DUMMY") != 0 && IS_PGXC_COORDINATOR)
#else
#define is_valid_query(queryDesc) \
    (queryDesc!=NULL && queryDesc->sourceText != NULL && strcmp(queryDesc->sourceText, "DUMMY") != 0)
#endif
void  auto_explain_init(void);
void  _PG_fini(void);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction,
                    long count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void opfusion_execEnd(PlanState* planstate, EState* estate);

/*
 * Module load callback
 */
void auto_explain_init(void)
{
    /* Install hooks. */
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = explain_ExecutorStart;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = explain_ExecutorRun;
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = explain_ExecutorFinish;
}

/*
 * Module unload callback
 */
void _PG_fini(void)
{
    /* Uninstall hooks. */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorRun_hook = prev_ExecutorRun;
    ExecutorFinish_hook = prev_ExecutorFinish;
    ExecutorEnd_hook = prev_ExecutorEnd;
}
 
/*
 * ExecutorStart hook: start up logging if needed
 */
static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    if (auto_explain_enabled())
    {
        /*
         * Set up to track total elapsed time in ExecutorRun.  Make sure the
         * space is allocated in the per-query context so it will go away at
         * ExecutorEnd.
         */
        if (queryDesc->totaltime == NULL)
        {
            MemoryContext oldcxt;

            oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
            queryDesc->totaltime = InstrAlloc(1, (int)INSTRUMENT_ALL);
            MemoryContextSwitchTo(oldcxt);
        }
    }
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    u_sess->exec_cxt.nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorRun)
            prev_ExecutorRun(queryDesc, direction, count);
        else
            standard_ExecutorRun(queryDesc, direction, count);
        u_sess->exec_cxt.nesting_level--;
    }
    PG_CATCH();
    {
        u_sess->exec_cxt.nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void explain_ExecutorFinish(QueryDesc *queryDesc)
{
    u_sess->exec_cxt.nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorFinish)
            prev_ExecutorFinish(queryDesc);
        else
            standard_ExecutorFinish(queryDesc);
        u_sess->exec_cxt.nesting_level--;
    }
    PG_CATCH();
    {
        u_sess->exec_cxt.nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void opfusion_executeEnd(PlannedStmt* plannedstmt, const char *queryString, Snapshot snapshot)
{
    long secs;
    int usecs;
    int msecs;
    TimestampDifference(GetCurrentStatementLocalStartTimestamp(), GetCurrentTimestamp(), &secs, &usecs);
    msecs = usecs / 1000 + secs * 1000;

    if (!auto_explain_enabled() || !instr_stmt_need_track_plan())
        return;

    QueryDesc  *queryDesc = NULL;
    queryDesc = CreateQueryDesc(plannedstmt, queryString,
                                snapshot, InvalidSnapshot,
                                None_Receiver, NULL, 1);
    EState *estate = CreateExecutorState();
    estate->es_range_table = plannedstmt->rtable;
    queryDesc->estate = estate;

    if (queryDesc->totaltime == NULL)
    {
        MemoryContext oldcxt;

        oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
        queryDesc->totaltime = InstrAlloc(1, (int)INSTRUMENT_ALL);
        MemoryContextSwitchTo(oldcxt);
    }
    InitPlan(queryDesc, 0);
    if (queryDesc->totaltime)
    {
        instr_stmt_report_query_plan(queryDesc);
    }
    opfusion_execEnd(queryDesc->planstate, queryDesc->estate);
    FreeExecutorState(queryDesc->estate);
    queryDesc->estate = NULL;
    FreeQueryDesc(queryDesc);
}

void print_parameters(const QueryDesc *queryDesc, ExplainState es)
{
    if (queryDesc->params == NULL)
        return;
    ParamListInfo param_list_info = queryDesc->params;
    int num_params = 0;
    for (int i = 0; i < param_list_info->numParams; i++) {
        ParamExternData param_data = param_list_info->params[i];
        char* output_str = NULL;
        char* oid_name = NULL;
        Oid oid = param_data.ptype;
        if (oid == InvalidOid)
            continue;
        num_params++;
        if (param_data.isnull) {
            appendStringInfo(es.str, "Parameter%d value: null ", num_params);
        } else {
            Datum value = param_data.value;
            Datum data  = static_cast<uintptr_t>(0);
            Oid typoutput;
            FmgrInfo finfo;
            bool typIsVarlena;
            getTypeOutputInfo(oid, &typoutput, &typIsVarlena);
            fmgr_info(typoutput, &finfo);
            if (typIsVarlena) {
                data = PointerGetDatum(PG_DETOAST_DATUM(value));
            } else {
                data = value;
            }
            output_str = OutputFunctionCall(&finfo, value);
            Type oid_type = typeidType(oid);
            oid_name = typeTypeName(oid_type);
            appendStringInfo(es.str, "param%d value: %s ", num_params, output_str);
            appendStringInfo(es.str, "type: %s\n", oid_name);
            pfree(oid_name);
            pfree(output_str);
            ReleaseSysCache((HeapTuple)oid_type);
            if (DatumGetPointer(data) != DatumGetPointer(value))
                pfree(DatumGetPointer(data));
        }
    } 
}
void exec_explain_plan(QueryDesc *queryDesc)
{
    ExplainState es;
    if (is_valid_query(queryDesc) && auto_explain_plan()) {
        INSTR_TIME_SET_CURRENT(plan_time);
        ExplainInitState(&es);
        es.costs = true;
        es.nodes = true;
        es.format = EXPLAIN_FORMAT_TEXT;
        es.verbose = true;
        es.analyze = false;
        es.timing = false;
        int old_explain_perf_mode = t_thrd.explain_cxt.explain_perf_mode;
        t_thrd.explain_cxt.explain_perf_mode = (int)EXPLAIN_NORMAL;
        appendStringInfo(es.str, "\n---------------------------"
            "-NestLevel:%d----------------------------\n", u_sess->exec_cxt.nesting_level);
        ExplainQueryText(&es, queryDesc);
        appendStringInfo(es.str, "Name: %s\n", g_instance.attr.attr_common.PGXCNodeName);
        ExplainBeginOutput(&es);
        MemoryContext current_ctx = CurrentMemoryContext;
        PG_TRY();
        {
            ExplainPrintPlan(&es, queryDesc);
            ExplainEndOutput(&es);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(current_ctx);
            ErrorData* edata = CopyErrorData();
            FlushErrorState();
            ereport(u_sess->attr.attr_resource.auto_explain_level,
                (errmsg("\nQueryPlan\nexplain failed\nerror:%s", edata->message), errhidestmt(true)));
            FreeErrorData(edata);
            pfree(es.str->data);
            t_thrd.explain_cxt.explain_perf_mode = old_explain_perf_mode;
            return;
        }
        PG_END_TRY();
        t_thrd.explain_cxt.explain_perf_mode = old_explain_perf_mode;
        print_parameters(queryDesc, es);
        ereport(u_sess->attr.attr_resource.auto_explain_level,
            (errmsg("\nQueryPlan\n%s\n", es.str->data), errhidestmt(true)));
        pfree(es.str->data);
    }
}
void print_duration(const QueryDesc *queryDesc) {
    if (is_valid_query(queryDesc) && auto_explain_plan()) {
        double time_diff = 0;
        time_diff += elapsed_time(&plan_time);
        ereport(u_sess->attr.attr_resource.auto_explain_level,
            (errmsg("\n----------------------------NestLevel:%d"
            "----------------------------\nduration: %.3f s\n",u_sess->exec_cxt.nesting_level, time_diff),
            errhidestmt(true)));
    }
}

void explain_querydesc(ExplainState *es, QueryDesc *queryDesc)
{
    int old_explain_perf_mode = t_thrd.explain_cxt.explain_perf_mode;

    u_sess->exec_cxt.under_auto_explain = true;
    t_thrd.explain_cxt.explain_perf_mode = (int)EXPLAIN_NORMAL;

    ExplainInitState(es);
    es->costs = true;
    es->nodes = true;
    es->format = EXPLAIN_FORMAT_TEXT;

    /* head */
    appendStringInfo(es->str, "%s Name: %s\n",
        (IS_PGXC_COORDINATOR) ? "Coordinator" : "Datanode", g_instance.attr.attr_common.PGXCNodeName);

    ExplainBeginOutput(es);
    ExplainPrintPlan(es, queryDesc);
    ExplainEndOutput(es);

    /* tail */
    appendStringInfoString(es->str, "\n\n");

    t_thrd.explain_cxt.explain_perf_mode = old_explain_perf_mode;
    u_sess->exec_cxt.under_auto_explain = false;

    /* Remove last line break */
    if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n') {
        es->str->data[--es->str->len] = '\0';
    }
}

static void opfusion_execEnd(PlanState* planstate, EState* estate)
{
    ResultRelInfo* resultRelInfo = NULL;
    int i;
    ListCell* l = NULL;

    /*
     * shut down the node-type-specific query processing
     */
    ExecEndNode(planstate);

    /*
     * for subplans too
     */
    foreach (l, estate->es_subplanstates) {
        PlanState* subplanstate = (PlanState*)lfirst(l);

        ExecEndNode(subplanstate);
    }

    /*
     * destroy the executor's tuple table.  Actually we only care about
     * releasing buffer pins and tupdesc refcounts; there's no need to pfree
     * the TupleTableSlots, since the containing memory context is about to go
     * away anyway.
     */
    ExecResetTupleTable(estate->es_tupleTable, false);

    /*
     * close the result relation(s) if any, but hold locks until xact commit.
     */
    resultRelInfo = estate->es_result_relations;
    for (i = estate->es_num_result_relations; i > 0; i--) {
        /* Close indices and then the relation itself */
        ExecCloseIndices(resultRelInfo);
        heap_close(resultRelInfo->ri_RelationDesc, NoLock);
        resultRelInfo++;
    }

    /* free the fakeRelationCache */
    if (NULL != estate->esfRelations) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }
    estate->esCurrentPartition = NULL;

    /*
     * likewise close any trigger target relations
     */
    foreach (l, estate->es_trig_target_relations) {
        resultRelInfo = (ResultRelInfo*)lfirst(l);
        /* Close indices and then the relation itself */
        ExecCloseIndices(resultRelInfo);
        heap_close(resultRelInfo->ri_RelationDesc, NoLock);
    }

    /*
     * close any relations selected FOR UPDATE/FOR SHARE, again keeping locks
     */
    foreach (l, estate->es_rowMarks) {
        ExecRowMark* erm = (ExecRowMark*)lfirst(l);

        if (erm->relation)
            heap_close(erm->relation, NoLock);
    }
}
