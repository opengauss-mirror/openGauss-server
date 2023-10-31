/* -------------------------------------------------------------------------
 *
 * pquery.cpp
 *	  POSTGRES process query command code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/tcop/pquery.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "commands/prepare.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "pg_trace.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/execRemote.h"
#include "access/relscan.h"
#endif
#include "tcop/pquery.h"
#include "tcop/utility.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/ts_redis.h"
#endif
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "workload/commgr.h"
#include "pgstat.h"
#include "access/printtup.h"
#include "access/tableam.h"
#include "executor/lightProxy.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/tcop_gstrace.h"
#include "instruments/instr_slow_query.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif

/*
 * ActivePortal is the currently executing Portal (the most closely nested,
 * if there are several).
 */
THR_LOCAL Portal ActivePortal = NULL;

static void FillPortalStore(Portal portal, bool isTopLevel);
static uint32 RunFromStore(Portal portal, ScanDirection direction, long count, DestReceiver* dest);
static uint32 RunFromExplainStore(Portal portal, ScanDirection direction, DestReceiver* dest);

static uint64 PortalRunSelect(Portal portal, bool forward, long count, DestReceiver* dest);
static void PortalRunUtility(
    Portal portal, Node* utilityStmt, bool isTopLevel, DestReceiver* dest, char* completionTag);
static void PortalRunMulti(
    Portal portal, bool isTopLevel, DestReceiver* dest, DestReceiver* altdest, char* completionTag);
static long DoPortalRunFetch(Portal portal, FetchDirection fdirection, long count, DestReceiver* dest);
static void DoPortalRewind(Portal portal);

extern bool StreamTopConsumerAmI();

extern void report_qps_type(CmdType commandType);
extern CmdType set_cmd_type(const char* commandTag);

/*
 * CreateQueryDesc
 */
#ifdef ENABLE_MOT
QueryDesc* CreateQueryDesc(PlannedStmt* plannedstmt, const char* sourceText, Snapshot snapshot,
    Snapshot crosscheck_snapshot, DestReceiver* dest, ParamListInfo params, int instrument_options,
    JitExec::MotJitContext* motJitContext /* = nullptr */)
#else
QueryDesc* CreateQueryDesc(PlannedStmt* plannedstmt, const char* sourceText, Snapshot snapshot,
    Snapshot crosscheck_snapshot, DestReceiver* dest, ParamListInfo params, int instrument_options)
#endif
{
    QueryDesc* qd = (QueryDesc*)palloc(sizeof(QueryDesc));

    qd->operation = plannedstmt->commandType;   /* operation */
    qd->plannedstmt = plannedstmt;              /* plan */
    qd->utilitystmt = plannedstmt->utilityStmt; /* in case DECLARE CURSOR */
    qd->sourceText = sourceText;                /* query text */
    qd->snapshot = RegisterSnapshot(snapshot);  /* snapshot */
    /* RI check snapshot */
    qd->crosscheck_snapshot = RegisterSnapshot(crosscheck_snapshot);
    qd->dest = dest;     /* output dest */
    qd->params = params; /* parameter values passed into query */

    if (IS_PGXC_DATANODE && plannedstmt->instrument_option)
        qd->instrument_options = plannedstmt->instrument_option;
    else
        qd->instrument_options = instrument_options; /* instrumentation
                                                      * wanted? */
    /* null these fields until set by ExecutorStart */
    qd->tupDesc = NULL;
    qd->estate = NULL;
    qd->planstate = NULL;
    qd->totaltime = NULL;
    qd->executed = false;
#ifdef ENABLE_MOT
    if (motJitContext != nullptr && JitExec::IsJitContextValid(motJitContext)) {
        qd->mot_jit_context = motJitContext;
    } else {
        qd->mot_jit_context = nullptr;
    }
#endif

    return qd;
}

/*
 * CreateUtilityQueryDesc
 */
QueryDesc* CreateUtilityQueryDesc(
    Node* utilitystmt, const char* sourceText, Snapshot snapshot, DestReceiver* dest, ParamListInfo params)
{
    QueryDesc* qd = (QueryDesc*)palloc(sizeof(QueryDesc));

    qd->operation = CMD_UTILITY; /* operation */
    qd->plannedstmt = NULL;
    qd->utilitystmt = utilitystmt;             /* utility command */
    qd->sourceText = sourceText;               /* query text */
    qd->snapshot = RegisterSnapshot(snapshot); /* snapshot */
    qd->crosscheck_snapshot = InvalidSnapshot; /* RI check snapshot */
    qd->dest = dest;                           /* output dest */
    qd->params = params;                       /* parameter values passed into query */
    qd->instrument_options = false;            /* uninteresting for utilities */

    /* null these fields until set by ExecutorStart */
    qd->tupDesc = NULL;
    qd->estate = NULL;
    qd->planstate = NULL;
    qd->totaltime = NULL;
    qd->executed = false;
#ifdef ENABLE_MOT
    qd->mot_jit_context = nullptr;
#endif

    return qd;
}

/*
 * FreeQueryDesc
 */
void FreeQueryDesc(QueryDesc* qdesc)
{
    /* Can't be a live query */
    AssertEreport(qdesc->estate == NULL, MOD_EXECUTOR, "query is still living");

    /* forget our snapshots */
    UnregisterSnapshot(qdesc->snapshot);
    UnregisterSnapshot(qdesc->crosscheck_snapshot);

    /* Only the QueryDesc itself need be freed */
    pfree_ext(qdesc);
}

#ifdef ENABLE_MOT
/*
 * MOT LLVM
 */
static void ProcessMotJitQuery(PlannedStmt* plan, const char* sourceText, ParamListInfo params,
    JitExec::MotJitContext* motJitContext, char* completionTag)
{
    Oid lastOid = InvalidOid;
    uint64 tuplesProcessed = 0;
    int scanEnded = 0;

    if (JitExec::IsMotCodegenPrintEnabled()) {
        elog(DEBUG1, "Invoking jitted mot query and query string: %s\n", sourceText);
    }

    int rc = JitExec::JitExecQuery(motJitContext, params, NULL, &tuplesProcessed, &scanEnded);
    if (JitExec::IsMotCodegenPrintEnabled()) {
        elog(DEBUG1, "jitted mot query returned: %d\n", rc);
    }

    if (completionTag != NULL) {
        errno_t ret = EOK;
        switch (plan->commandType) {
            case CMD_INSERT:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "INSERT %u %lu", lastOid, tuplesProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_UPDATE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "UPDATE %lu", tuplesProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_DELETE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "DELETE %lu", tuplesProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            default:
                break;
        }
    }
}
#endif

/*
 * ProcessQuery
 *		Execute a single plannable query within a PORTAL_MULTI_QUERY,
 *		PORTAL_ONE_RETURNING, or PORTAL_ONE_MOD_WITH portal
 *
 *	plan: the plan tree for the query
 *	sourceText: the source text of the query
 *	params: any parameters needed
 *	dest: where to send results
 *	completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *		in which to store a command completion status string.
 *
 * completionTag may be NULL if caller doesn't want a status string.
 *
 * Must be called in a memory context that will be reset or deleted on
 * error; otherwise the executor's memory usage will be leaked.
 */
#ifdef ENABLE_MOT
static void ProcessQuery(PlannedStmt* plan, const char* sourceText, ParamListInfo params, bool isMOTTable,
    JitExec::MotJitContext* motJitContext, DestReceiver* dest, char* completionTag)
#else
static void ProcessQuery(
    PlannedStmt* plan, const char* sourceText, ParamListInfo params, DestReceiver* dest, char* completionTag)
#endif
{
    QueryDesc* queryDesc = NULL;

    elog(DEBUG3, "ProcessQuery");

#ifdef ENABLE_MOT
    Snapshot snap = GetActiveSnapshot();  // Check for snapshot before calling ProcessMotJitQuery

    if (isMOTTable && motJitContext && JitExec::IsJitContextValid(motJitContext) &&
        !IS_PGXC_COORDINATOR && JitExec::IsMotCodegenEnabled()) {
        // MOT LLVM
        ProcessMotJitQuery(plan, sourceText, params, motJitContext, completionTag);
        return;
    }

    /*
     * Create the QueryDesc object
     */
    queryDesc = CreateQueryDesc(plan, sourceText, snap, InvalidSnapshot, dest, params, 0, motJitContext);
#else
    /*
     * Create the QueryDesc object
     */
    queryDesc = CreateQueryDesc(plan, sourceText, GetActiveSnapshot(), InvalidSnapshot, dest, params, 0);
#endif

    if (ENABLE_WORKLOAD_CONTROL && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        WLMTopSQLReady(queryDesc);
    }

    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        if (u_sess->exec_cxt.need_track_resource) {
            queryDesc->instrument_options = plan->instrument_option;
            queryDesc->plannedstmt->instrument_option = plan->instrument_option;
        } else {
            queryDesc->plannedstmt->instrument_option = 0;
        }
    }

    /*
     * Call ExecutorStart to prepare the plan for execution
     */
    ExecutorStart(queryDesc, 0);

    /* Pass row trigger shippability info to estate */
    queryDesc->estate->isRowTriggerShippable = plan->isRowTriggerShippable;

    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL) {
        WLMInitQueryPlan(queryDesc);
        dywlm_client_manager(queryDesc);
    }

    /*
     * Run the plan to completion.
     */
    ExecutorRun(queryDesc, ForwardScanDirection, 0L);

    u_sess->ledger_cxt.resp_tag = completionTag;

    /*
     * Build command completion status string, if caller wants one.
     */
    if (completionTag != NULL) {
        Oid lastOid;
        errno_t ret = EOK;

        switch (queryDesc->operation) {
            case CMD_SELECT:
                ret = snprintf_s(completionTag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "SELECT %lu",
                    queryDesc->estate->es_processed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_INSERT:
                if (queryDesc->estate->es_processed == 1)
                    lastOid = queryDesc->estate->es_lastoid;
                else
                    lastOid = InvalidOid;
                if (((ModifyTableState*)queryDesc->planstate)->isReplace) {
                    ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "REPLACE %u %lu", lastOid, queryDesc->estate->es_processed);
                } else {
                    ret = snprintf_s(completionTag,
                        COMPLETION_TAG_BUFSIZE,
                        COMPLETION_TAG_BUFSIZE - 1,
                        "INSERT %u %lu",
                        lastOid,
                        queryDesc->estate->es_processed);
                }
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_UPDATE:
                ret = snprintf_s(completionTag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "UPDATE %lu",
                    queryDesc->estate->es_processed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_DELETE:
                ret = snprintf_s(completionTag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "DELETE %lu",
                    queryDesc->estate->es_processed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_MERGE:
                ret = snprintf_s(completionTag,
                    COMPLETION_TAG_BUFSIZE,
                    COMPLETION_TAG_BUFSIZE - 1,
                    "MERGE %lu",
                    queryDesc->estate->es_processed);
                securec_check_ss(ret, "\0", "\0");
                break;
            default:
                ret = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, "?\?\?");
                securec_check(ret, "\0", "\0");
                break;
        }
    }

    /*
     * Now, we close down all the scans and free allocated resources.
     */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);
}

static PortalStrategy choose_portal_strategy_for_remote_query(const RemoteQuery* step)
{
    /*
     * Let's choose PORTAL_ONE_SELECT for now
     * After adding more PGXC functionality we may have more
     * sophisticated algorithm of determining portal strategy
     *
     * EXECUTE DIRECT is a utility but depending on its inner query
     * it can return tuples or not depending on the query used.
     */
    if (step->exec_direct_type == EXEC_DIRECT_SELECT || step->exec_direct_type == EXEC_DIRECT_UPDATE ||
        step->exec_direct_type == EXEC_DIRECT_DELETE || step->exec_direct_type == EXEC_DIRECT_INSERT ||
        step->exec_direct_type == EXEC_DIRECT_LOCAL)
        return PORTAL_ONE_SELECT;
    else if (step->exec_direct_type == EXEC_DIRECT_UTILITY ||
             step->exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY)
        return PORTAL_MULTI_QUERY;
    else
        return PORTAL_ONE_SELECT;
}

/*
 * ChoosePortalStrategy
 *		Select portal execution strategy given the intended statement list.
 *
 * The list elements can be Querys, PlannedStmts, or utility statements.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * See the comments in portal.h.
 */
PortalStrategy ChoosePortalStrategy(List* stmts)
{
    int nSetTag;
    ListCell* lc = NULL;

    /*
     * PORTAL_ONE_SELECT and PORTAL_UTIL_SELECT need only consider the
     * single-statement case, since there are no rewrite rules that can add
     * auxiliary queries to a SELECT or a utility command. PORTAL_ONE_MOD_WITH
     * likewise allows only one top-level statement.
     */
    if (list_length(stmts) == 1) {
        Node* stmt = (Node*)linitial(stmts);

        if (IsA(stmt, Query)) {
            Query* query = (Query*)stmt;

            if (query->canSetTag) {
                if (query->commandType == CMD_SELECT && query->utilityStmt == NULL) {
                    if (query->hasModifyingCTE)
                        return PORTAL_ONE_MOD_WITH;
                    else
                        return PORTAL_ONE_SELECT;
                }
                if (query->commandType == CMD_UTILITY && query->utilityStmt != NULL) {
                    if (UtilityReturnsTuples(query->utilityStmt))
                        return PORTAL_UTIL_SELECT;
                    /* it can't be ONE_RETURNING, so give up */
                    return PORTAL_MULTI_QUERY;
                }
#ifdef PGXC
                /*
                 * This is possible with an EXECUTE DIRECT in a SPI.
                 * There might be a better way to manage the
                 * cases with EXECUTE DIRECT here like using a special
                 * utility command and redirect it to a correct portal
                 * strategy.
                 * Something like PORTAL_UTIL_SELECT might be far better.
                 */
                if (query->commandType == CMD_SELECT && query->utilityStmt != NULL &&
                    IsA(query->utilityStmt, RemoteQuery)) {
                    return choose_portal_strategy_for_remote_query((RemoteQuery*)query->utilityStmt);
                }
#endif
            }
        }
#ifdef PGXC
        else if (IsA(stmt, RemoteQuery)) {
            return choose_portal_strategy_for_remote_query((RemoteQuery*)stmt);
        }
#endif
        else if (IsA(stmt, PlannedStmt)) {
            PlannedStmt* pstmt = (PlannedStmt*)stmt;

            if (pstmt->canSetTag) {
                if (pstmt->commandType == CMD_SELECT && pstmt->utilityStmt == NULL) {
                    if (pstmt->hasModifyingCTE)
                        return PORTAL_ONE_MOD_WITH;
                    else
                        return PORTAL_ONE_SELECT;
                } else if ((pstmt->utilityStmt != NULL) &&
                           IsA(pstmt->utilityStmt, RemoteQuery)) {
                    return choose_portal_strategy_for_remote_query((RemoteQuery*)pstmt->utilityStmt);
                }
            }
        } else {
            /* must be a utility command; assume it's canSetTag */
            if (UtilityReturnsTuples(stmt))
                return PORTAL_UTIL_SELECT;
            /* it can't be ONE_RETURNING, so give up */
            return PORTAL_MULTI_QUERY;
        }
    }

    /*
     * PORTAL_ONE_RETURNING has to allow auxiliary queries added by rewrite.
     * Choose PORTAL_ONE_RETURNING if there is exactly one canSetTag query and
     * it has a RETURNING list.
     */
    nSetTag = 0;
    foreach (lc, stmts) {
        Node* stmt = (Node*)lfirst(lc);

        if (IsA(stmt, Query)) {
            Query* query = (Query*)stmt;

            if (query->canSetTag) {
                if (++nSetTag > 1)
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                if (query->returningList == NIL)
                    return PORTAL_MULTI_QUERY; /* no need to look further */
            }
        } else if (IsA(stmt, PlannedStmt)) {
            PlannedStmt* pstmt = (PlannedStmt*)stmt;

            if (pstmt->canSetTag) {
                if (++nSetTag > 1)
                    return PORTAL_MULTI_QUERY; /* no need to look further */
                if (!pstmt->hasReturning)
                    return PORTAL_MULTI_QUERY; /* no need to look further */
            }
        }
        /* otherwise, utility command, assumed not canSetTag */
    }
    if (nSetTag == 1)
        return PORTAL_ONE_RETURNING;

    /* Else, it's the general case... */
    return PORTAL_MULTI_QUERY;
}

/*
 * FetchPortalTargetList
 *		Given a portal that returns tuples, extract the query targetlist.
 *		Returns NIL if the portal doesn't have a determinable targetlist.
 *
 * Note: do not modify the result.
 */
List* FetchPortalTargetList(Portal portal)
{
    /* no point in looking if we determined it doesn't return tuples */
    if (portal->strategy == PORTAL_MULTI_QUERY)
        return NIL;
    /* get the primary statement and find out what it returns */
    return FetchStatementTargetList(PortalGetPrimaryStmt(portal));
}

/*
 * FetchStatementTargetList
 *		Given a statement that returns tuples, extract the query targetlist.
 *		Returns NIL if the statement doesn't have a determinable targetlist.
 *
 * This can be applied to a Query, a PlannedStmt, or a utility statement.
 * That's more general than portals need, but plancache.c uses this too.
 *
 * Note: do not modify the result.
 *
 * XXX be careful to keep this in sync with UtilityReturnsTuples.
 */
List* FetchStatementTargetList(Node* stmt)
{
    if (stmt == NULL)
        return NIL;
    if (IsA(stmt, Query)) {
        Query* query = (Query*)stmt;

        if (query->commandType == CMD_UTILITY && query->utilityStmt != NULL) {
            /* transfer attention to utility statement */
            stmt = query->utilityStmt;
        } else {
            if (query->commandType == CMD_SELECT && query->utilityStmt == NULL)
                return query->targetList;
            if (query->returningList)
                return query->returningList;
            return NIL;
        }
    }
    if (IsA(stmt, PlannedStmt)) {
        PlannedStmt* pstmt = (PlannedStmt*)stmt;

        if (pstmt->commandType == CMD_SELECT && pstmt->utilityStmt == NULL)
            return pstmt->planTree->targetlist;
        if (pstmt->hasReturning)
            return pstmt->planTree->targetlist;
        return NIL;
    }
    if (IsA(stmt, FetchStmt)) {
        FetchStmt* fstmt = (FetchStmt*)stmt;
        Portal subportal;

        AssertEreport(!fstmt->ismove, MOD_EXECUTOR, "FetchStmt ismove can not be true");
        subportal = GetPortalByName(fstmt->portalname);
        AssertEreport(PortalIsValid(subportal), MOD_EXECUTOR, "subportal not valid");
        return FetchPortalTargetList(subportal);
    }
    if (IsA(stmt, ExecuteStmt)) {
        ExecuteStmt* estmt = (ExecuteStmt*)stmt;
        PreparedStatement *entry = NULL;

        entry = FetchPreparedStatement(estmt->name, true, true);
        return FetchPreparedStatementTargetList(entry);
    }
    return NIL;
}

static bool IsSupportExplain(const char* command_tag)
{
    if (strncmp(command_tag, "SELECT", 7) == 0)
        return true;
    if (strncmp(command_tag, "INSERT", 7) == 0)
        return true;
    if (strncmp(command_tag, "DELETE", 7) == 0)
        return true;
    if (strncmp(command_tag, "UPDATE", 7) == 0)
        return true;
    if (strncmp(command_tag, "MERGE", 6) == 0)
        return true;
    if (strncmp(command_tag, "SELECT INTO", 12) == 0)
        return true;
    if (strncmp(command_tag, "CREATE TABLE AS", 16) == 0)
        return true;
    return false;
}

/*
 * PortalStart
 *		Prepare a portal for execution.
 *
 * Caller must already have created the portal, done PortalDefineQuery(),
 * and adjusted portal options if needed.
 *
 * If parameters are needed by the query, they must be passed in "params"
 * (caller is responsible for giving them appropriate lifetime).
 *
 * The caller can also provide an initial set of "eflags" to be passed to
 * ExecutorStart (but note these can be modified internally, and they are
 * currently only honored for PORTAL_ONE_SELECT portals).  Most callers
 * should simply pass zero.
 *
 * The caller can optionally pass a snapshot to be used; pass InvalidSnapshot
 * for the normal behavior of setting a new snapshot.  This parameter is
 * presently ignored for non-PORTAL_ONE_SELECT portals (it's only intended
 * to be used for cursors).
 *
 * On return, portal is ready to accept PortalRun() calls, and the result
 * tupdesc (if any) is known.
 */

bool shouldDoInstrument(Portal portal, PlannedStmt* ps)
{
    if (((IS_PGXC_COORDINATOR && ps->is_stream_plan == true && !u_sess->attr.attr_sql.enable_cluster_resize) ||
        IS_SINGLE_NODE) &&
        u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR &&
        u_sess->attr.attr_resource.use_workload_manager &&
        t_thrd.wlm_cxt.collect_info->status != WLM_STATUS_RUNNING &&
        !portal->visible &&
        IsSupportExplain(portal->commandTag)) {
        return true;
    }

    return false;
}

void PortalStart(Portal portal, ParamListInfo params, int eflags, Snapshot snapshot)
{
    gstrace_entry(GS_TRC_ID_PortalStart);
    Portal saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldContext;
    QueryDesc* queryDesc = NULL;
    int myeflags;
    PlannedStmt* ps = NULL;
    int instrument_option = 0;

    AssertArg(PortalIsValid(portal));
    AssertState(portal->status == PORTAL_DEFINED);

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    savePortalContext = t_thrd.mem_cxt.portal_mem_cxt;
    PG_TRY();
    {
        ActivePortal = portal;
        t_thrd.utils_cxt.CurrentResourceOwner = portal->resowner;
        t_thrd.mem_cxt.portal_mem_cxt = PortalGetHeapMemory(portal);

        oldContext = MemoryContextSwitchTo(PortalGetHeapMemory(portal));

        /* Must remember portal param list, if any */
        portal->portalParams = params;

        /*
         * Determine the portal execution strategy
         */
        portal->strategy = ChoosePortalStrategy(portal->stmts);

        // Allocate and initialize scan descriptor
        portal->scanDesc = (TableScanDesc)palloc0(SizeofHeapScanDescData + MaxHeapTupleSize);

        /*
         * Fire her up according to the strategy
         */
        switch (portal->strategy) {
            case PORTAL_ONE_SELECT: {
                ps = (PlannedStmt*)linitial(portal->stmts);

                /* Must set snapshot before starting executor. */
                if (snapshot) {
                    PushActiveSnapshot(snapshot);
                } else {
                    if (u_sess->pgxc_cxt.gc_fdw_snapshot) {
                        PushActiveSnapshot(u_sess->pgxc_cxt.gc_fdw_snapshot);
                    } else {
                        bool force_local_snapshot = false;

                        if (portal->cplan != NULL && portal->cplan->single_shard_stmt) {
                            /* with single shard, we will be forced to do local snapshot work */
                            force_local_snapshot = true;
                        }
                        PushActiveSnapshot(GetTransactionSnapshot(force_local_snapshot));
                    }
                }

                /*
                 * For operator track of active SQL, explain performance is triggered for SELECT SQL,
                 * except cursor case(portal->visible), which can't be run out within one fetch stmt
                 */
                if (shouldDoInstrument(portal, ps)) {
                    instrument_option |= INSTRUMENT_TIMER;
                    instrument_option |= INSTRUMENT_BUFFERS;
                }

#ifdef ENABLE_MOT
                JitExec::MotJitContext* mot_jit_context =
                    (portal->cplan != NULL) ? portal->cplan->mot_jit_context : nullptr;

                /*
                 * Create QueryDesc in portal's context; for the moment, set
                 * the destination to DestNone.
                 */
                queryDesc = CreateQueryDesc(ps, portal->sourceText, GetActiveSnapshot(), InvalidSnapshot, None_Receiver,
                    params, 0, mot_jit_context);
#else
                /*
                 * Create QueryDesc in portal's context; for the moment, set
                 * the destination to DestNone.
                 */
                queryDesc = CreateQueryDesc(
                    ps, portal->sourceText, GetActiveSnapshot(), InvalidSnapshot, None_Receiver, params, 0);
#endif

                /* means on CN of the compute pool. */
                if (((IS_PGXC_COORDINATOR && StreamTopConsumerAmI()) || IS_SINGLE_NODE) && ps->instrument_option) {
                    queryDesc->instrument_options |= ps->instrument_option;
                }

                /* Check if need track resource */
                if (u_sess->attr.attr_resource.use_workload_manager && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE))
                    u_sess->exec_cxt.need_track_resource = WLMNeedTrackResource(queryDesc);

                if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
                    if (queryDesc->plannedstmt != NULL && u_sess->exec_cxt.need_track_resource) {
                        queryDesc->instrument_options |= instrument_option;
                        queryDesc->plannedstmt->instrument_option = instrument_option;
                    }
                }

                if (!u_sess->instr_cxt.obs_instr &&
                    ((queryDesc->plannedstmt) != NULL && queryDesc->plannedstmt->has_obsrel)) {
                    AutoContextSwitch cxtGuard(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

                    u_sess->instr_cxt.obs_instr = New(CurrentMemoryContext) OBSInstrumentation();
                }

                /*
                 * If it's a scrollable cursor, executor needs to support
                 * REWIND and backwards scan, as well as whatever the caller
                 * might've asked for.
                 */
                if (portal->cursorOptions & CURSOR_OPT_SCROLL)
                    myeflags = eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD;
                /* with hold cursor need rewind portal to store all tuples */
                else if (portal->cursorOptions & CURSOR_OPT_HOLD)
                    myeflags = eflags | EXEC_FLAG_REWIND;
                else
                    myeflags = eflags;

                if (ENABLE_WORKLOAD_CONTROL && IS_PGXC_DATANODE) {
                    WLMCreateDNodeInfoOnDN(queryDesc);

                    // create IO info on DN
                    WLMCreateIOInfoOnDN();
                }

                /*
                 * Call ExecutorStart to prepare the plan for execution
                 */
                ExecutorStart(queryDesc, myeflags);

                /*
                 * This tells PortalCleanup to shut down the executor
                 */
                portal->queryDesc = queryDesc;

                /*
                 * Remember tuple descriptor (computed by ExecutorStart)
                 */
                portal->tupDesc = queryDesc->tupDesc;

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false; /* allow fetches */
                portal->portalPos = 0;
                portal->posOverflow = false;

                PopActiveSnapshot();
                break;
            }
            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:

                /*
                 * We don't start the executor until we are told to run the
                 * portal.	We do need to set up the result tupdesc.
                 */
                {
                    PlannedStmt* pstmt = NULL;

                    pstmt = (PlannedStmt*)PortalGetPrimaryStmt(portal);
                    AssertEreport(IsA(pstmt, PlannedStmt), MOD_EXECUTOR, "pstmt is not a PlannedStmt");
                    portal->tupDesc = ExecCleanTypeFromTL(pstmt->planTree->targetlist, false);
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false; /* allow fetches */
                portal->portalPos = 0;
                portal->posOverflow = false;
                break;

            case PORTAL_UTIL_SELECT:

                /*
                 * We don't set snapshot here, because PortalRunUtility will
                 * take care of it if needed.
                 */
                {
                    Node* ustmt = PortalGetPrimaryStmt(portal);

                    AssertEreport(!IsA(ustmt, PlannedStmt), MOD_EXECUTOR, "ustmt can not be a PlannedStmt");
                    portal->tupDesc = UtilityTupleDescriptor(ustmt);

                    if (portal->tupDesc != NULL)
                    {
                        portal->tupDesc->td_tam_ops = TableAmHeap;
                    }
                }

                /*
                 * Reset cursor position data to "start of query"
                 */
                portal->atStart = true;
                portal->atEnd = false; /* allow fetches */
                portal->portalPos = 0;
                portal->posOverflow = false;
                break;

            case PORTAL_MULTI_QUERY:
                /* Need do nothing now */
                portal->tupDesc = NULL;

                if (ENABLE_WORKLOAD_CONTROL && IS_PGXC_DATANODE) {
                    WLMCreateDNodeInfoOnDN(NULL);

                    // create IO info on DN
                    WLMCreateIOInfoOnDN();
                }
                break;
            default:
                break;
        }

        portal->stmtMemCost = 0;
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    ActivePortal = saveActivePortal;
    t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
    t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

    portal->status = PORTAL_READY;
    gstrace_exit(GS_TRC_ID_PortalStart);
}

/*
 * PortalSetResultFormat
 *		Select the format codes for a portal's output.
 *
 * This must be run after PortalStart for a portal that will be read by
 * a DestRemote or DestRemoteExecute destination.  It is not presently needed
 * for other destination types.
 *
 * formats[] is the client format request, as per Bind message conventions.
 */
void PortalSetResultFormat(Portal portal, int nFormats, int16* formats)
{
    int natts;
    int i;

#ifndef ENABLE_MULTIPLE_NODES
#ifndef USE_SPQ
    if (StreamTopConsumerAmI()) {
        portal->streamInfo.RecordSessionInfo();
        u_sess->stream_cxt.global_obj->m_portal = portal;
    }
#endif
#endif

    /* Do nothing if portal won't return tuples */
    if (portal->tupDesc == NULL)
        return;
    natts = portal->tupDesc->natts;
    portal->formats = (int16*)MemoryContextAlloc(PortalGetHeapMemory(portal), natts * sizeof(int16));
    if (nFormats > 1) {
        /* format specified for each column */
        if (nFormats != natts)
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("bind message has %d result formats but query has %d columns", nFormats, natts)));
        errno_t errorno = EOK;
        errorno = memcpy_s(portal->formats, natts * sizeof(int16), formats, natts * sizeof(int16));
        securec_check(errorno, "\0", "\0");
    } else if (nFormats > 0) {
        /* single format specified, use for all columns */
        int16 format1 = formats[0];

        for (i = 0; i < natts; i++)
            portal->formats[i] = format1;
    } else {
        /* use default format for all columns */
        for (i = 0; i < natts; i++)
            portal->formats[i] = 0;
    }
}

void PotalSetIoState(Portal portal)
{
    if (strcmp(portal->commandTag, "COPY") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "VACUUM") == 0)
        pgstat_set_io_state(IOSTATE_VACUUM);

    if (strcmp(portal->commandTag, "UPDATE") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "INSERT") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "CREATE TABLE") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "ALTER TABLE") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "CREATE INDEX") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "REINDEX INDEX") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "CLUSTER") == 0)
        pgstat_set_io_state(IOSTATE_WRITE);

    if (strcmp(portal->commandTag, "ANALYZE") == 0)
        pgstat_set_io_state(IOSTATE_READ);

    /* set write for backend status for the thread, we will use it to check default transaction readOnly */
    pgstat_set_stmt_tag(STMTTAG_NONE);
    if (strcmp(portal->commandTag, "INSERT") == 0 || strcmp(portal->commandTag, "UPDATE") == 0 ||
        strcmp(portal->commandTag, "CREATE TABLE AS") == 0 || strcmp(portal->commandTag, "CREATE INDEX") == 0 ||
        strcmp(portal->commandTag, "ALTER TABLE") == 0 || strcmp(portal->commandTag, "CLUSTER") == 0)
        pgstat_set_stmt_tag(STMTTAG_WRITE);

}

/*
 * PortalRun
 *		Run a portal's query or queries.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".  Note that count is ignored in multi-query
 * situations, where we always run the portal to completion.
 *
 * isTopLevel: true if query is being executed at backend "top level"
 * (that is, directly from a client command message)
 *
 * dest: where to send output of primary (canSetTag) query
 *
 * altdest: where to send output of non-primary queries
 *
 * completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *		in which to store a command completion status string.
 *		May be NULL if caller doesn't want a status string.
 *
 * Returns TRUE if the portal's execution is complete, FALSE if it was
 * suspended due to exhaustion of the count parameter.
 */
bool PortalRun(
    Portal portal, long count, bool isTopLevel, DestReceiver* dest, DestReceiver* altdest, char* completionTag)
{
    gstrace_entry(GS_TRC_ID_PortalRun);
    increase_instr_portal_nesting_level();
    OgRecordAutoController _local_opt(EXECUTION_TIME);

    bool result = false;
    uint64 nprocessed;
    ResourceOwner saveTopTransactionResourceOwner;
    MemoryContext saveTopTransactionContext;
    Portal saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext saveMemoryContext;
    errno_t errorno = EOK;

    AssertArg(PortalIsValid(portal));
    AssertArg(PointerIsValid(portal->commandTag));

    char* old_stmt_name = u_sess->pcache_cxt.cur_stmt_name;
    u_sess->pcache_cxt.cur_stmt_name = (char*)portal->prepStmtName;
    /* match portal->commandTag with CmdType */
    CmdType cmdType = CMD_UNKNOWN;
    CmdType queryType = CMD_UNKNOWN;
    if (isTopLevel && u_sess->attr.attr_common.pgstat_track_activities &&
        u_sess->attr.attr_common.pgstat_track_sql_count && !u_sess->attr.attr_sql.enable_cluster_resize) {
        /*
         * Match at the beginning of PortalRun for
         * portal->commandTag can be changed during process.
         * Only handle the top portal.
         */
        cmdType = set_cmd_type(portal->commandTag);
        queryType = set_command_type_by_commandTag(portal->commandTag);
    }

    PGSTAT_INIT_TIME_RECORD();

    TRACE_POSTGRESQL_QUERY_EXECUTE_START();

    /* Initialize completion tag to empty string */
    if (completionTag != NULL)
        completionTag[0] = '\0';

    if (portal->strategy != PORTAL_MULTI_QUERY) {
        if (u_sess->attr.attr_common.log_executor_stats) {
            elog(DEBUG3, "PortalRun");
            /* PORTAL_MULTI_QUERY logs its own stats per query */
            ResetUsage();
        }
        PGSTAT_START_TIME_RECORD();
    }

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    QueryDesc* queryDesc = portal->queryDesc;

    if (IS_PGXC_DATANODE && queryDesc != NULL && (queryDesc->plannedstmt) != NULL &&
        queryDesc->plannedstmt->has_obsrel) {
        increase_rp_number();
    }

#ifdef USE_SPQ
    if (!IS_SPQ_RUNNING && queryDesc != NULL && (queryDesc->plannedstmt) != NULL &&
        queryDesc->plannedstmt->is_spq_optmized) {
        t_thrd.spq_ctx.spq_role = ROLE_QUERY_COORDINTOR;
    }
#endif /* USE_SPQ */

    /*
     * Set up global portal context pointers.
     *
     * We have to play a special game here to support utility commands like
     * VACUUM and CLUSTER, which internally start and commit transactions.
     * When we are called to execute such a command, CurrentResourceOwner will
     * be pointing to the TopTransactionResourceOwner --- which will be
     * destroyed and replaced in the course of the internal commit and
     * restart.  So we need to be prepared to restore it as pointing to the
     * exit-time TopTransactionResourceOwner.  (Ain't that ugly?  This idea of
     * internally starting whole new transactions is not good.)
     * CurrentMemoryContext has a similar problem, but the other pointers we
     * save here will be NULL or pointing to longer-lived objects.
     */

    saveTopTransactionResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    saveTopTransactionContext = u_sess->top_transaction_mem_cxt;
    saveActivePortal = ActivePortal;
    saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    savePortalContext = t_thrd.mem_cxt.portal_mem_cxt;
    saveMemoryContext = CurrentMemoryContext;

    u_sess->attr.attr_sql.create_index_concurrently = false;

    PotalSetIoState(portal);

    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL && queryDesc != NULL) {
        WLMTopSQLReady(queryDesc);
        WLMInitQueryPlan(queryDesc);
        dywlm_client_manager(queryDesc);
    }
    /* save flag for nest plpgsql compile */
    PLpgSQL_compile_context* save_compile_context = u_sess->plsql_cxt.curr_compile_context;
    int save_compile_list_length = list_length(u_sess->plsql_cxt.compile_context_list);
    int save_compile_status = u_sess->plsql_cxt.compile_status;
    int savePortalDepth = u_sess->plsql_cxt.portal_depth;
    bool savedisAllowCommitRollback = false;
    int save_nextval_default_expr_type = u_sess->opt_cxt.nextval_default_expr_type;
    bool needResetErrMsg = false;

    PG_TRY();
    {
        ActivePortal = portal;
        u_sess->opt_cxt.nextval_default_expr_type = portal->nextval_default_expr_type;
        t_thrd.utils_cxt.CurrentResourceOwner = portal->resowner;
        u_sess->plsql_cxt.portal_depth++;
        if (u_sess->plsql_cxt.portal_depth > 1) {
            /* commit rollback procedure not support in multi-layer portal called */
            needResetErrMsg = stp_disable_xact_and_set_err_msg(&savedisAllowCommitRollback, STP_XACT_TOO_MANY_PORTAL);
        }
        t_thrd.mem_cxt.portal_mem_cxt = PortalGetHeapMemory(portal);

        MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);

        switch (portal->strategy) {
            case PORTAL_ONE_SELECT:
            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
            case PORTAL_UTIL_SELECT:

                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.  But we don't do that
                 * for the PORTAL_ONE_SELECT case.
                 */
                if (portal->strategy != PORTAL_ONE_SELECT && !portal->holdStore) {
                    /* DestRemoteExecute can not send T message automatically */
                    if (strcmp(portal->commandTag, "EXPLAIN") == 0 && dest->mydest != DestRemote)
                        t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
                    FillPortalStore(portal, isTopLevel);
                }

                /*
                 * Now fetch desired portion of results.
                 */
                nprocessed = PortalRunSelect(portal, true, count, dest);

                /*
                 * If the portal result contains a command tag and the caller
                 * gave us a pointer to store it, copy it. Patch the "SELECT"
                 * tag to also provide the rowcount.
                 */
                if (completionTag != NULL) {
                    if (strcmp(portal->commandTag, "SELECT") == 0) {
                        errorno = snprintf_s(completionTag,
                            COMPLETION_TAG_BUFSIZE,
                            COMPLETION_TAG_BUFSIZE - 1,
                            "SELECT %lu",
                            nprocessed);
                        securec_check_ss(errorno, "\0", "\0");
                    } else {
                        errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE,
                            portal->commandTag);
                        securec_check(errorno, "\0", "\0");
                    }
                }

                /* Mark portal not active */
                portal->status = PORTAL_READY;

                /*
                 * Since it's a forward fetch, say DONE iff atEnd is now true.
                 */
                result = portal->atEnd;
                break;

            case PORTAL_MULTI_QUERY:
                PortalRunMulti(portal, isTopLevel, dest, altdest, completionTag);

                /* Prevent portal's commands from being re-executed */
                MarkPortalDone(portal);
                /* Always complete at end of RunMulti */
                result = true;
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("Unrecognized portal strategy: %d", (int)portal->strategy)));
                result = false; /* keep compiler quiet */
                break;
        }
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);
        u_sess->opt_cxt.nextval_default_expr_type = save_nextval_default_expr_type;
        u_sess->plsql_cxt.portal_depth = savePortalDepth;
        stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);

        /* Restore global vars and propagate error */
        if (saveMemoryContext == saveTopTransactionContext ||
            saveTopTransactionContext != u_sess->top_transaction_mem_cxt) {
            MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
        } else if (ResourceOwnerIsValid(saveResourceOwner)) {
            MemoryContextSwitchTo(saveMemoryContext);
        }
        ActivePortal = saveActivePortal;
        if (saveResourceOwner == saveTopTransactionResourceOwner ||
            saveTopTransactionResourceOwner != t_thrd.utils_cxt.TopTransactionResourceOwner) {
            t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
        } else if (ResourceOwnerIsValid(saveResourceOwner)) {
            t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        }
        t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("%s clear curr_compile_context because of error.", __func__)));
        /* reset nest plpgsql compile */
        u_sess->plsql_cxt.curr_compile_context = save_compile_context;
        u_sess->plsql_cxt.compile_status = save_compile_status;
        clearCompileContextList(save_compile_list_length);

        PG_RE_THROW();
    }
    PG_END_TRY();

    u_sess->opt_cxt.nextval_default_expr_type = save_nextval_default_expr_type;
    u_sess->plsql_cxt.portal_depth = savePortalDepth;
    stp_reset_xact_state_and_err_msg(savedisAllowCommitRollback, needResetErrMsg);

    if (ENABLE_WORKLOAD_CONTROL) {
        t_thrd.wlm_cxt.parctl_state.except = 0;

        if (g_instance.wlm_cxt->dynamic_workload_inited && (t_thrd.wlm_cxt.parctl_state.simple == 0)) {
            dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
        } else {
            // only release resource pool count
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                (u_sess->wlm_cxt->parctl_state_exit || IsQueuedSubquery())) {
                WLMReleaseGroupActiveStatement();
            }
        }
    }

    /*
     * switch to topTransaction if (1) it is TopTransactionContext, (2) Top transaction was changed.
     * While subtransaction was terminated inside statement running, its ResourceOwner should be reserved.
     */
    if (saveMemoryContext == saveTopTransactionContext ||
        saveTopTransactionContext != u_sess->top_transaction_mem_cxt) {
        MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    } else if (ResourceOwnerIsValid(saveResourceOwner)) {
        MemoryContextSwitchTo(saveMemoryContext);
    }
    ActivePortal = saveActivePortal;
    if (saveResourceOwner == saveTopTransactionResourceOwner ||
        saveTopTransactionResourceOwner != t_thrd.utils_cxt.TopTransactionResourceOwner) {
        t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
    } else if (ResourceOwnerIsValid(saveResourceOwner)) {
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
    }
    t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

    if (portal->strategy != PORTAL_MULTI_QUERY) {
        PGSTAT_END_TIME_RECORD(EXECUTION_TIME);

        if (u_sess->attr.attr_common.log_executor_stats)
            ShowUsage("EXECUTOR STATISTICS");
    }
    TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();

    /* doing sql count accordiong to cmdType */
    if (cmdType != CMD_UNKNOWN || queryType != CMD_UNKNOWN) {
        report_qps_type(cmdType);
        report_qps_type(queryType);
    }

    /* update unique sql stat */
    if (((IS_UNIQUE_SQL_TRACK_TOP && is_instr_top_portal()) || IS_UNIQUE_SQL_TRACK_ALL)
        && is_unique_sql_enabled() && is_local_unique_sql()) {
        /* Instrumentation: update unique sql returned rows(SELECT) */
        // only CN can update this counter
        if (portal->queryDesc != NULL && portal->queryDesc->estate && portal->queryDesc->estate->es_plannedstmt &&
            portal->queryDesc->estate->es_plannedstmt->commandType == CMD_SELECT) {
            ereport(DEBUG1,
                (errmodule(MOD_INSTR),
                    errmsg("[UniqueSQL]"
                           "unique id: %lu , select returned rows: %lu",
                        u_sess->unique_sql_cxt.unique_sql_id,
                        portal->queryDesc->estate->es_processed)));
            UniqueSQLStatCountReturnedRows(portal->queryDesc->estate->es_processed);
        }

        /* PortalRun using unique_sql_start_time as unique sql elapse start time */
        if ((IsNeedUpdateUniqueSQLStat(portal) && IS_UNIQUE_SQL_TRACK_TOP && IsTopUniqueSQL())
            || IS_UNIQUE_SQL_TRACK_ALL) {
            instr_unique_sql_report_elapse_time(u_sess->unique_sql_cxt.unique_sql_start_time);
        }

        if (u_sess->unique_sql_cxt.unique_sql_start_time != 0) {
            int64 duration = GetCurrentTimestamp() - u_sess->unique_sql_cxt.unique_sql_start_time;
            if (IS_SINGLE_NODE) {
                pgstat_update_responstime_singlenode(
                    u_sess->unique_sql_cxt.unique_sql_id, u_sess->unique_sql_cxt.unique_sql_start_time, duration);
            } else {
                pgstat_report_sql_rt(
                    u_sess->unique_sql_cxt.unique_sql_id, u_sess->unique_sql_cxt.unique_sql_start_time, duration);
            }
        }
    }
    decrease_instr_portal_nesting_level();
    gstrace_exit(GS_TRC_ID_PortalRun);
    u_sess->pcache_cxt.cur_stmt_name = old_stmt_name;
    return result;
}

/*
 * PortalRunSelect
 *		Execute a portal's query in PORTAL_ONE_SELECT mode, and also
 *		when fetching from a completed holdStore in PORTAL_ONE_RETURNING,
 *		PORTAL_ONE_MOD_WITH, and PORTAL_UTIL_SELECT cases.
 *
 * This handles simple N-rows-forward-or-backward cases.  For more complex
 * nonsequential access to a portal, see PortalRunFetch.
 *
 * count <= 0 is interpreted as a no-op: the destination gets started up
 * and shut down, but nothing else happens.  Also, count == FETCH_ALL is
 * interpreted as "all rows".
 *
 * Caller must already have validated the Portal and done appropriate
 * setup (cf. PortalRun).
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
static uint64 PortalRunSelect(Portal portal, bool forward, long count, DestReceiver* dest)
{
    QueryDesc* queryDesc = NULL;
    ScanDirection direction;
    uint64 nprocessed;

    /*
     * NB: queryDesc will be NULL if we are fetching from a held cursor or a
     * completed utility query; can't use it in that path.
     */
    queryDesc = PortalGetQueryDesc(portal);

    /* Caller messed up if we have neither a ready query nor held data. */
    AssertEreport(queryDesc || portal->holdStore, MOD_EXECUTOR, "have no ready query or held data");

    /*
     * Force the queryDesc destination to the right thing.	This supports
     * MOVE, for example, which will pass in dest = DestNone.  This is okay to
     * change as long as we do it on every fetch.  (The Executor must not
     * assume that dest never changes.)
     */
    if (queryDesc != NULL)
        queryDesc->dest = dest;

    /*
     * Determine which direction to go in, and check to see if we're already
     * at the end of the available tuples in that direction.  If so, set the
     * direction to NoMovement to avoid trying to fetch any tuples.  (This
     * check exists because not all plan node types are robust about being
     * called again if they've already returned NULL once.)  Then call the
     * executor (we must not skip this, because the destination needs to see a
     * setup and shutdown even if no tuples are available).  Finally, update
     * the portal position state depending on the number of tuples that were
     * retrieved.
     */
    if (forward) {
        if (portal->atEnd || count <= 0)
            direction = NoMovementScanDirection;
        else
            direction = ForwardScanDirection;

        /* In the executor, zero count processes all rows */
        if (count == FETCH_ALL)
            count = 0;

        if (portal->holdStore) {
            /* If it`s a explain plan stmt, then we have changed the tag in ExplainQuery. */
            if (strcmp(portal->commandTag, "EXPLAIN") == 0 || strcmp(portal->commandTag, "EXPLAIN SUCCESS") == 0)
                nprocessed = RunFromExplainStore(portal, direction, dest);
            else
                nprocessed = RunFromStore(portal, direction, count, dest);
        } else {
            PushActiveSnapshot(queryDesc->snapshot);

#ifdef PGXC
            if (portal->name != NULL && portal->name[0] != '\0' && IsA(queryDesc->planstate, RemoteQueryState)) {
                /*
                 * The snapshot in the query descriptor contains the
                 * command id of the command creating the cursor. We copy
                 * that snapshot in RemoteQueryState so that the do_query
                 * function knows while sending the select (resulting from
                 * a fetch) to the corresponding remote node with the command
                 * id of the command that created the cursor.
                 */
                RemoteQueryState* rqs = (RemoteQueryState*)queryDesc->planstate;

                // get the cached scan descriptor in portal
                rqs->ss.ss_currentScanDesc = (TableScanDesc)portal->scanDesc;
                // copy snapshot into the scan descriptor
                portal->scanDesc->rs_snapshot = queryDesc->snapshot;
                rqs->cursor = (char*)portal->name;
            }
#endif

            ExecutorRun(queryDesc, direction, count);

            /*
             * <<IS_PGXC_COORDINATOR && !StreamTopConsumerAmI()>> means that
             * we are on DWS CN.
             */
            if ((IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) &&
	        !StreamTopConsumerAmI() && queryDesc->plannedstmt->has_obsrel &&
                u_sess->instr_cxt.obs_instr) {
                u_sess->instr_cxt.obs_instr->insertData(queryDesc->plannedstmt->queryId);
            }

            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        if (!ScanDirectionIsNoMovement(direction)) {
            long oldPos;

            if (nprocessed > 0)
                portal->atStart = false; /* OK to go backward now */
            if (count == 0 || (unsigned long)nprocessed < (unsigned long)count)
                portal->atEnd = true; /* we retrieved 'em all */
            oldPos = portal->portalPos;
            portal->portalPos += nprocessed;
            /* portalPos doesn't advance when we fall off the end */
            if (portal->portalPos < oldPos)
                portal->posOverflow = true;
        }
    } else {
        if (portal->cursorOptions & CURSOR_OPT_NO_SCROLL)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Cursor can only scan forward")));

        if (portal->atStart || count <= 0)
            direction = NoMovementScanDirection;
        else
            direction = BackwardScanDirection;

        /* In the executor, zero count processes all rows */
        if (count == FETCH_ALL)
            count = 0;

        if (portal->holdStore)
            nprocessed = RunFromStore(portal, direction, count, dest);
        else {
            PushActiveSnapshot(queryDesc->snapshot);
            ExecutorRun(queryDesc, direction, count);
            nprocessed = queryDesc->estate->es_processed;
            PopActiveSnapshot();
        }

        if (!ScanDirectionIsNoMovement(direction)) {
            if (nprocessed > 0 && portal->atEnd) {
                portal->atEnd = false; /* OK to go forward now */
                portal->portalPos++;   /* adjust for endpoint case */
            }
            if (count == 0 || (unsigned long)nprocessed < (unsigned long)count) {
                portal->atStart = true; /* we retrieved 'em all */
                portal->portalPos = 0;
                portal->posOverflow = false;
            } else {
                long oldPos;

                oldPos = portal->portalPos;
                portal->portalPos -= nprocessed;
                if (portal->portalPos > oldPos || portal->portalPos <= 0)
                    portal->posOverflow = true;
            }
        }
    }

    return nprocessed;
}

/*
 * FillPortalStore
 *		Run the query and load result tuples into the portal's tuple store.
 *
 * This is used for PORTAL_ONE_RETURNING, PORTAL_ONE_MOD_WITH, and
 * PORTAL_UTIL_SELECT cases only.
 */
static void FillPortalStore(Portal portal, bool isTopLevel)
{
    DestReceiver* treceiver = NULL;
    char completionTag[COMPLETION_TAG_BUFSIZE];

    PortalCreateHoldStore(portal);
    treceiver = CreateDestReceiver(DestTuplestore);
    SetTuplestoreDestReceiverParams(treceiver, portal->holdStore, portal->holdContext, false);

    completionTag[0] = '\0';

    switch (portal->strategy) {
        case PORTAL_ONE_RETURNING:
        case PORTAL_ONE_MOD_WITH:

            /*
             * Run the portal to completion just as for the default
             * MULTI_QUERY case, but send the primary query's output to the
             * tuplestore. Auxiliary query outputs are discarded.
             */
            PortalRunMulti(portal, isTopLevel, treceiver, None_Receiver, completionTag);
            break;

        case PORTAL_UTIL_SELECT:
            PortalRunUtility(portal, (Node*)linitial(portal->stmts), isTopLevel, treceiver, completionTag);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unsupported portal strategy: %d", (int)portal->strategy)));
            break;
    }

    /* Override default completion tag with actual command result */
    if (completionTag[0] != '\0')
        portal->commandTag = pstrdup(completionTag);

    (*treceiver->rDestroy)(treceiver);
}

/*
 * RunFromStore
 *		Fetch tuples from the portal's tuple store.
 *
 * Calling conventions are similar to ExecutorRun, except that we
 * do not depend on having a queryDesc or estate.  Therefore we return the
 * number of tuples processed as the result, not in estate->es_processed.
 *
 * One difference from ExecutorRun is that the destination receiver functions
 * are run in the caller's memory context (since we have no estate).  Watch
 * out for memory leaks.
 */
static uint32 RunFromStore(Portal portal, ScanDirection direction, long count, DestReceiver* dest)
{
    long current_tuple_count = 0;
    TupleTableSlot* slot = NULL;

    slot = MakeSingleTupleTableSlot(portal->tupDesc);

    (*dest->rStartup)(dest, CMD_SELECT, portal->tupDesc);

    if (ScanDirectionIsNoMovement(direction)) {
        /* do nothing except start/stop the destination */
    } else {
        bool forward = ScanDirectionIsForward(direction);
        /* UniqueSQL: handle n_returned_rows case for FETCH statement */
        bool is_fetch_returned_rows = false;
        if (portal->strategy == PORTAL_UTIL_SELECT && is_local_unique_sql() && list_length(portal->stmts) == 1) {
            Node *stmt = (Node*)linitial(portal->stmts);
            if (IsA(stmt, FetchStmt)) {
                is_fetch_returned_rows = true;
            }
        }

        for (;;) {
            MemoryContext oldcontext;
            bool ok = false;

            oldcontext = MemoryContextSwitchTo(portal->holdContext);

            ok = tuplestore_gettupleslot(portal->holdStore, forward, false, slot);

            MemoryContextSwitchTo(oldcontext);

            if (!ok) {
                break;
            }

            (*dest->receiveSlot)(slot, dest);

            ExecClearTuple(slot);

            if (is_fetch_returned_rows) {
                UniqueSQLStatCountReturnedRows(1);
            }

            /*
             * check our tuple count.. if we've processed the proper number
             * then quit, else loop again and process more tuples. Zero count
             * means no limit.
             */
            current_tuple_count++;
            if (count && count == current_tuple_count) {
                break;
            }
        }
    }

    (*dest->rShutdown)(dest);

    ExecDropSingleTupleTableSlot(slot);

    return (uint32)current_tuple_count;
}

extern uint32 RunGetSlotFromExplain(Portal portal, TupleTableSlot* slot, DestReceiver* dest, int count)
{

    bool forward = true;
    long current_tuple_count = 0;

    (*dest->rStartup)(dest, CMD_SELECT, portal->tupDesc);

    for (;;) {
        MemoryContext oldcontext;
        bool ok = false;

        oldcontext = MemoryContextSwitchTo(portal->holdContext);

        ok = tuplestore_gettupleslot(portal->holdStore, forward, false, slot);

        MemoryContextSwitchTo(oldcontext);

        if (!ok) {
            break;
        }

        (*dest->receiveSlot)(slot, dest);

        ExecClearTuple(slot);

        current_tuple_count++;
        if (count && count == current_tuple_count) {
            break;
        }
    }

    (*dest->rShutdown)(dest);

    return (uint32)current_tuple_count;
}

static uint32 RunFromExplainStore(Portal portal, ScanDirection direction, DestReceiver* dest)
{
    long current_tuple_count = 0;
    TupleTableSlot* slot = NULL;
    ExplainStmt* stmt = NULL;
    PlanInformation* planinfo = NULL;

    if (IsA((Node*)linitial(portal->stmts), ExplainStmt))
        stmt = (ExplainStmt*)((Node*)linitial(portal->stmts));
    if (stmt == NULL)
        return 0;

    planinfo = stmt->planinfo;

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && planinfo) {
        portal->formats = NULL;
        current_tuple_count = planinfo->print_plan(portal, dest);
    } else {
        slot = MakeSingleTupleTableSlot(portal->tupDesc);
        current_tuple_count += RunGetSlotFromExplain(portal, slot, dest, 0);
        ExecDropSingleTupleTableSlot(slot);
    }

    (*dest->rShutdown)(dest);

    return (uint32)current_tuple_count;
}

/*
 * PortalRunUtility
 *		Execute a utility statement inside a portal.
 */
static void PortalRunUtility(Portal portal, Node* utilityStmt, bool isTopLevel, DestReceiver* dest, char* completionTag)
{
    bool active_snapshot_set = false;

    elog(DEBUG3, "ProcessUtility");

    /*
     * Set snapshot if utility stmt needs one.	Most reliable way to do this
     * seems to be to enumerate those that do not need one; this is a short
     * list.  Transaction control, LOCK, and SET must *not* set a snapshot
     * since they need to be executable at the start of a transaction-snapshot
     * mode transaction without freezing a snapshot.  By extension we allow
     * SHOW not to set a snapshot.	The other stmts listed are just efficiency
     * hacks.  Beware of listing anything that can modify the database --- if,
     * say, it has to update an index with expressions that invoke
     * user-defined functions, then it had better have a snapshot.
     */
    if (!(IsA(utilityStmt, TransactionStmt) || IsA(utilityStmt, LockStmt) || IsA(utilityStmt, VariableSetStmt) ||
            IsA(utilityStmt, VariableShowStmt) || IsA(utilityStmt, ConstraintsSetStmt) ||
            /* efficiency hacks from here down */
            IsA(utilityStmt, FetchStmt) || IsA(utilityStmt, ListenStmt) || IsA(utilityStmt, NotifyStmt) ||
            IsA(utilityStmt, UnlistenStmt) ||
#ifdef PGXC
        (IsA(utilityStmt, RefreshMatViewStmt) && IS_PGXC_COORDINATOR) ||
            (IsA(utilityStmt, CheckPointStmt) && IS_PGXC_DATANODE)))
#else
            IsA(utilityStmt, CheckPointStmt)))
#endif
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        active_snapshot_set = true;
    } else
        active_snapshot_set = false;

    /* Exec workload client manager if commandTag is not EXPLAIN or EXECUTE */
    if (ENABLE_WORKLOAD_CONTROL)
        WLMSetExecutorStartTime();

    processutility_context proutility_cxt;
    proutility_cxt.parse_tree = utilityStmt;
    proutility_cxt.query_string = portal->sourceText;
    proutility_cxt.readOnlyTree = (portal->cplan != NULL);  /* protect tree if in plancache */
    proutility_cxt.params = portal->portalParams;
    proutility_cxt.is_top_level = isTopLevel;
    ProcessUtility(&proutility_cxt,
        dest,
#ifdef PGXC
        false,
#endif /* PGXC */
        completionTag,
			isTopLevel ? PROCESS_UTILITY_TOPLEVEL : PROCESS_UTILITY_QUERY);
        

    if (proutility_cxt.parse_tree != NULL && nodeTag(proutility_cxt.parse_tree) == T_ExplainStmt && ((ExplainStmt*)proutility_cxt.parse_tree)->planinfo != NULL) {
        ((ExplainStmt*)utilityStmt)->planinfo = ((ExplainStmt*)proutility_cxt.parse_tree)->planinfo;
    }

    /* Some utility statements may change context on us */
    MemoryContextSwitchTo(PortalGetHeapMemory(portal));

    /*
     * Some utility commands may pop the u_sess->utils_cxt.ActiveSnapshot stack from under us,
     * so we only pop the stack if we actually see a snapshot set.	Note that
     * the set of utility commands that do this must be the same set
     * disallowed to run inside a transaction; otherwise, we could be popping
     * a snapshot that belongs to some other operation.
     */
    if (active_snapshot_set && ActiveSnapshotSet())
        PopActiveSnapshot();

    perm_space_value_reset();
}

/*
 * PortalRunMulti
 *		Execute a portal's queries in the general case (multi queries
 *		or non-SELECT-like queries)
 */
static void PortalRunMulti(
    Portal portal, bool isTopLevel, DestReceiver* dest, DestReceiver* altdest, char* completionTag)
{
    bool active_snapshot_set = false;
    ListCell* stmtlist_item = NULL;
    OgRecordAutoController _local_opt(EXECUTION_TIME);
    PGSTAT_INIT_TIME_RECORD();
#ifdef PGXC
    CombineTag combine;

    combine.cmdType = CMD_UNKNOWN;
    combine.data[0] = '\0';
#endif

    AssertEreport(PortalIsValid(portal), MOD_EXECUTOR, "portal not valid");

    bool force_local_snapshot = false;
    
    if (portal->cplan != NULL) {
        /* copy over the single_shard_stmt into local variable force_local_snapshot */
        force_local_snapshot = portal->cplan->single_shard_stmt;
    }
    /*
     * If the destination is DestRemoteExecute, change to DestNone.  The
     * reason is that the client won't be expecting any tuples, and indeed has
     * no way to know what they are, since there is no provision for Describe
     * to send a RowDescription message when this portal execution strategy is
     * in effect.  This presently will only affect SELECT commands added to
     * non-SELECT queries by rewrite rules: such commands will be executed,
     * but the results will be discarded unless you use "simple Query"
     * protocol.
     */
    if (dest->mydest == DestRemoteExecute)
        dest = None_Receiver;
    if (altdest->mydest == DestRemoteExecute)
        altdest = None_Receiver;

    /* sql active feature: create table as case */
    uint32 instrument_option = 0;
    if (IS_PGXC_COORDINATOR && u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR &&
        IS_STREAM && u_sess->attr.attr_resource.use_workload_manager &&
        t_thrd.wlm_cxt.collect_info->status != WLM_STATUS_RUNNING && IsSupportExplain(portal->commandTag) &&
        !u_sess->attr.attr_sql.enable_cluster_resize) {
        instrument_option |= INSTRUMENT_TIMER;
        instrument_option |= INSTRUMENT_BUFFERS;
    }

    /*
     * Loop to handle the individual queries generated from a single parsetree
     * by analysis and rewrite.
     */
    foreach (stmtlist_item, portal->stmts) {
        Node* stmt = (Node*)lfirst(stmtlist_item);
#ifdef ENABLE_MOT
        bool isMOTTable = false;
        JitExec::MotJitContext* mot_jit_context = nullptr;
#endif

        /*
         * If we got a cancel signal in prior command, quit
         */
        CHECK_FOR_INTERRUPTS();

        if (IsA(stmt, PlannedStmt) && ((PlannedStmt*)stmt)->utilityStmt == NULL) {
            /*
             * process a plannable query.
             */
            PlannedStmt* pstmt = (PlannedStmt*)stmt;

            TRACE_POSTGRESQL_QUERY_EXECUTE_START();

            if (u_sess->attr.attr_common.log_executor_stats)
                ResetUsage();

            PGSTAT_START_TIME_RECORD();

            /*
             * Must always have a snapshot for plannable queries.
             * First time through, take a new snapshot; for subsequent queries in the
             * same portal, just update the snapshot's copy of the command
             * counter.
             */
            if (!active_snapshot_set) {
                PushActiveSnapshot(GetTransactionSnapshot(force_local_snapshot));
                active_snapshot_set = true;
            } else
                UpdateActiveSnapshotCommandId();

#ifdef ENABLE_MOT
            if ((portal->cplan != NULL && portal->cplan->storageEngineType == SE_TYPE_MOT)) {
                isMOTTable = true;
                mot_jit_context = portal->cplan->mot_jit_context;
            }
#endif

            if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)
                pstmt->instrument_option = instrument_option;

            if (pstmt->canSetTag) {
                /* statement can set tag string */
#ifdef ENABLE_MOT
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams,
                    isMOTTable, mot_jit_context, dest, completionTag);
#else
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams, dest, completionTag);
#endif
#ifdef PGXC
                /* it's special for INSERT */
                if (IS_PGXC_COORDINATOR && pstmt->commandType == CMD_INSERT)
                    HandleCmdComplete(pstmt->commandType, &combine, completionTag, strlen(completionTag));
#endif
            } else {
                /* stmt added by rewrite cannot set tag */
#ifdef ENABLE_MOT
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams,
                    isMOTTable, mot_jit_context, altdest, NULL);
#else
                ProcessQuery(pstmt, portal->sourceText, portal->portalParams, altdest, NULL);
#endif
            }

            PGSTAT_END_TIME_RECORD(EXECUTION_TIME);

            if (u_sess->attr.attr_common.log_executor_stats)
                ShowUsage("EXECUTOR STATISTICS");

            TRACE_POSTGRESQL_QUERY_EXECUTE_DONE();
        } else {
            /*
             * process utility functions (create, destroy, etc..)
             *
             * These are assumed canSetTag if they're the only stmt in the
             * portal.
             *
             * We must not set a snapshot here for utility commands (if one is
             * needed, PortalRunUtility will do it).  If a utility command is
             * alone in a portal then everything's fine.  The only case where
             * a utility command can be part of a longer list is that rules
             * are allowed to include NotifyStmt.  NotifyStmt doesn't care
             * whether it has a snapshot or not, so we just leave the current
             * snapshot alone if we have one.
             */
            if (list_length(portal->stmts) == 1) {
                AssertEreport(!active_snapshot_set, MOD_EXECUTOR, "No active snapshot for utility commands");
                /* statement can set tag string */
                PortalRunUtility(portal, stmt, isTopLevel, dest, completionTag);
            } else if (IsA(stmt, AlterTableStmt) || IsA(stmt, ViewStmt) || IsA(stmt, RuleStmt)) {
                AssertEreport(!active_snapshot_set, MOD_EXECUTOR, "No active snapshot for utility commands");
                /* statement can set tag string */
                PortalRunUtility(portal, stmt, isTopLevel, dest, NULL);
            } else {
                AssertEreport(IsA(stmt, NotifyStmt), MOD_EXECUTOR, "Not a NotifyStmt");
                /* stmt added by rewrite cannot set tag */
                PortalRunUtility(portal, stmt, isTopLevel, altdest, NULL);
            }
        }

        /*
         * Increment command counter between queries, but not after the last
         * one.
         */
        if (lnext(stmtlist_item) != NULL)
            CommandCounterIncrement();

        /*
         * Clear subsidiary contexts to recover temporary memory.
         */
        AssertEreport(
            PortalGetHeapMemory(portal) == CurrentMemoryContext, MOD_EXECUTOR, "Memory context is not consistant");

        MemoryContextDeleteChildren(PortalGetHeapMemory(portal), NULL);
    }

    /* Pop the snapshot if we pushed one. */
    if (active_snapshot_set)
        PopActiveSnapshot();

    /*
     * If a command completion tag was supplied, use it.  Otherwise use the
     * portal's commandTag as the default completion tag.
     *
     * Exception: Clients expect INSERT/UPDATE/DELETE tags to have counts, so
     * fake them with zeros.  This can happen with DO INSTEAD rules if there
     * is no replacement query of the same type as the original.  We print "0
     * 0" here because technically there is no query of the matching tag type,
     * and printing a non-zero count for a different query type seems wrong,
     * e.g.  an INSERT that does an UPDATE instead should not print "0 1" if
     * one row was updated.  See QueryRewrite(), step 3, for details.
     */
    errno_t errorno = EOK;
#ifdef PGXC
    if (IS_PGXC_COORDINATOR && completionTag != NULL && combine.data[0] != '\0') {
        errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, combine.data);
        securec_check(errorno, "\0", "\0");
    }
#endif

    if (completionTag != NULL && completionTag[0] == '\0') {
        if (portal->commandTag) {
            errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, portal->commandTag);
            securec_check(errorno, "\0", "\0");
        }
        if (strcmp(completionTag, "SELECT") == 0) {
            errorno = sprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, "SELECT 0 0");
            securec_check_ss(errorno, "\0", "\0");
        } else if (strcmp(completionTag, "INSERT") == 0) {
            errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, "INSERT 0 0");
            securec_check(errorno, "\0", "\0");
        } else if (strcmp(completionTag, "UPDATE") == 0) {
            errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, "UPDATE 0");
            securec_check(errorno, "\0", "\0");
        } else if (strcmp(completionTag, "DELETE") == 0) {
            errorno = strcpy_s(completionTag, COMPLETION_TAG_BUFSIZE, "DELETE 0");
            securec_check(errorno, "\0", "\0");
        }
    }
}

/*
 * PortalRunFetch
 *		Variant form of PortalRun that supports SQL FETCH directions.
 *
 * Note: we presently assume that no callers of this want isTopLevel = true.
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
long PortalRunFetch(Portal portal, FetchDirection fdirection, long count, DestReceiver* dest)
{
    long result;
    Portal saveActivePortal;
    ResourceOwner saveResourceOwner;
    MemoryContext savePortalContext;
    MemoryContext oldContext;

    AssertArg(PortalIsValid(portal));
    Assert(portal->prepStmtName == NULL || portal->prepStmtName[0] == '\0');

    /*
     * Check for improper portal use, and mark portal active.
     */
    MarkPortalActive(portal);

    /* Disable early free when using cursor which may need rescan */
    bool saved_early_free = u_sess->attr.attr_sql.enable_early_free;
    u_sess->attr.attr_sql.enable_early_free = false;

    /*
     * Set up global portal context pointers.
     */
    saveActivePortal = ActivePortal;
    saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    savePortalContext = t_thrd.mem_cxt.portal_mem_cxt;
    PG_TRY();
    {
        ActivePortal = portal;
        t_thrd.utils_cxt.CurrentResourceOwner = portal->resowner;
        t_thrd.mem_cxt.portal_mem_cxt = PortalGetHeapMemory(portal);

        oldContext = MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);

        switch (portal->strategy) {
            case PORTAL_ONE_SELECT:
                result = DoPortalRunFetch(portal, fdirection, count, dest);
                break;

            case PORTAL_ONE_RETURNING:
            case PORTAL_ONE_MOD_WITH:
            case PORTAL_UTIL_SELECT:

                /*
                 * If we have not yet run the command, do so, storing its
                 * results in the portal's tuplestore.
                 */
                if (!portal->holdStore) {
                    /* DestRemoteExecute can not send T message automatically */
                    if (strcmp(portal->commandTag, "EXPLAIN") == 0 && dest->mydest != DestRemote)
                        t_thrd.explain_cxt.explain_perf_mode = EXPLAIN_NORMAL;
                    FillPortalStore(portal, false /* isTopLevel */);
                }

                /*
                 * Now fetch desired portion of results.
                 */
                result = DoPortalRunFetch(portal, fdirection, count, dest);
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("unsupported portal strategy")));
                result = 0; /* keep compiler quiet */
                break;
        }
    }
    PG_CATCH();
    {
        /* Uncaught error while executing portal: mark it dead */
        MarkPortalFailed(portal);

        /* Restore global vars and propagate error */
        ActivePortal = saveActivePortal;
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

        /* Restore GUC variable enable_early_free */
        u_sess->attr.attr_sql.enable_early_free = saved_early_free;

        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldContext);

    /* Mark portal not active */
    portal->status = PORTAL_READY;

    ActivePortal = saveActivePortal;
    t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
    t_thrd.mem_cxt.portal_mem_cxt = savePortalContext;

    /* Restore GUC variable enable_early_free */
    u_sess->attr.attr_sql.enable_early_free = saved_early_free;

    return result;
}

/*
 * DoPortalRunFetch
 *		Guts of PortalRunFetch --- the portal context is already set up
 *
 * Returns number of rows processed (suitable for use in result tag)
 */
static long DoPortalRunFetch(Portal portal, FetchDirection fdirection, long count, DestReceiver* dest)
{
    bool forward = false;

    AssertEreport(portal->strategy == PORTAL_ONE_SELECT || portal->strategy == PORTAL_ONE_RETURNING ||
                      portal->strategy == PORTAL_ONE_MOD_WITH || portal->strategy == PORTAL_UTIL_SELECT,
        MOD_EXECUTOR,
        "portal strategy is not select, returning, mod_with, or util select");

    /* workload client manager */
    if (ENABLE_WORKLOAD_CONTROL && portal->queryDesc && !portal->queryDesc->executed) {
        if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
            /* Check if need track resource */
            u_sess->exec_cxt.need_track_resource = WLMNeedTrackResource(portal->queryDesc);

            /* Add the definition of CURSOR to the end of the query */
            if (u_sess->exec_cxt.need_track_resource && t_thrd.wlm_cxt.collect_info->sdetail.statement &&
                portal->queryDesc->sourceText && !t_thrd.wlm_cxt.has_cursor_record) {
                USE_MEMORY_CONTEXT(g_instance.wlm_cxt->query_resource_track_mcxt);

                pgstat_set_io_state(IOSTATE_READ);

                uint32 query_str_len = strlen(t_thrd.wlm_cxt.collect_info->sdetail.statement) +
                                       strlen(portal->queryDesc->sourceText) + 3; /* 3 is the length of "()" and '\0' */
                char* query_str = (char*)palloc0(query_str_len);
                int rc = snprintf_s(query_str,
                    query_str_len,
                    query_str_len - 1,
                    "%s(%s)",
                    t_thrd.wlm_cxt.collect_info->sdetail.statement,
                    portal->queryDesc->sourceText);
                securec_check_ss(rc, "\0", "\0");

                pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
                t_thrd.wlm_cxt.collect_info->sdetail.statement = query_str;

                uint32 hashCode = WLMHashCode(&u_sess->wlm_cxt->wlm_params.qid, sizeof(Qid));
                LockSessRealTHashPartition(hashCode, LW_EXCLUSIVE);
                WLMDNodeInfo* info = (WLMDNodeInfo*)hash_search(g_instance.wlm_cxt->stat_manager.collect_info_hashtbl,
                    &u_sess->wlm_cxt->wlm_params.qid,
                    HASH_FIND,
                    NULL);
                if (info != NULL) {
                    pfree_ext(info->statement);
                    info->statement = pstrdup(t_thrd.wlm_cxt.collect_info->sdetail.statement);
                    t_thrd.wlm_cxt.has_cursor_record = true;
                }

                UnLockSessRealTHashPartition(hashCode);
            }
        }

        WLMInitQueryPlan(portal->queryDesc);
        dywlm_client_manager(portal->queryDesc);
    }

    switch (fdirection) {
        case FETCH_FORWARD:
            if (count < 0) {
                fdirection = FETCH_BACKWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_BACKWARD */
            break;
        case FETCH_BACKWARD:
            if (count < 0) {
                fdirection = FETCH_FORWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_FORWARD */
            break;
        case FETCH_ABSOLUTE:
            if (count > 0) {
                /*
                 * Definition: Rewind to start, advance count-1 rows, return
                 * next row (if any).  If the goal is less than portalPos,
                 * we need to rewind, or we can fetch the target row forwards.
                 */
                if (portal->posOverflow || portal->portalPos == LONG_MAX || count - 1 < portal->portalPos) {
                    DoPortalRewind(portal);
                    if (count > 1)
                        (void)PortalRunSelect(portal, true, count - 1, None_Receiver);
                } else {
                    long pos = portal->portalPos;

                    if (portal->atEnd)
                        pos++; /* need one extra fetch if off end */
                    if (count <= pos)
                        (void)PortalRunSelect(portal, false, pos - count + 1, None_Receiver);
                    else if (count > pos + 1)
                        (void)PortalRunSelect(portal, true, count - pos - 1, None_Receiver);
                }
                return PortalRunSelect(portal, true, 1L, dest);
            } else if (count < 0) {
                /*
                 * Definition: Advance to end, back up abs(count)-1 rows,
                 * return prior row (if any).  We could optimize this if we
                 * knew in advance where the end was, but typically we won't.
                 * (Is it worth considering case where count > half of size of
                 * query?  We could rewind once we know the size ...)
                 */
                (void)PortalRunSelect(portal, true, FETCH_ALL, None_Receiver);
                if (count < -1)
                    (void)PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            } else {
                /* Rewind to start, return zero rows */
                DoPortalRewind(portal);
                return PortalRunSelect(portal, true, 0L, dest);
            }
            break;
        case FETCH_RELATIVE:
            if (count > 0) {
                /*
                 * Definition: advance count-1 rows, return next row (if any).
                 */
                if (count > 1)
                    (void)PortalRunSelect(portal, true, count - 1, None_Receiver);
                return PortalRunSelect(portal, true, 1L, dest);
            } else if (count < 0) {
                /*
                 * Definition: back up abs(count)-1 rows, return prior row (if
                 * any).
                 */
                if (count < -1)
                    (void)PortalRunSelect(portal, false, -count - 1, None_Receiver);
                return PortalRunSelect(portal, false, 1L, dest);
            } else {
                /* Same as FETCH FORWARD 0, so fall out of switch */
                fdirection = FETCH_FORWARD;
            }
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("bogus direction")));
            break;
    }

    /*
     * Get here with fdirection == FETCH_FORWARD or FETCH_BACKWARD, and count
     * >= 0.
     */
    forward = (fdirection == FETCH_FORWARD);

    /*
     * Zero count means to re-fetch the current row, if any (per SQL92)
     */
    if (count == 0) {
        bool on_row = false;

        /* Are we sitting on a row? */
        on_row = (!portal->atStart && !portal->atEnd);

        if (dest->mydest == DestNone) {
            /* MOVE 0 returns 0/1 based on if FETCH 0 would return a row */
            return on_row ? 1L : 0L;
        } else {
            /*
             * If we are sitting on a row, back up one so we can re-fetch it.
             * If we are not sitting on a row, we still have to start up and
             * shut down the executor so that the destination is initialized
             * and shut down correctly; so keep going.	To PortalRunSelect,
             * count == 0 means we will retrieve no row.
             */
            if (on_row) {
                (void)PortalRunSelect(portal, false, 1L, None_Receiver);
                /* Set up to fetch one row forward */
                count = 1;
                forward = true;
            }
        }
    }

    /*
     * Optimize MOVE BACKWARD ALL into a Rewind.
     */
    if (!forward && count == FETCH_ALL && dest->mydest == DestNone) {
        long result = portal->portalPos;

        if (result > 0 && !portal->atEnd)
            result--;
        DoPortalRewind(portal);
        /* result is bogus if pos had overflowed, but it's best we can do */
        return result;
    }

    return PortalRunSelect(portal, forward, count, dest);
}

/*
 * DoPortalRewind - rewind a Portal to starting point
 */
static void DoPortalRewind(Portal portal)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_EXECUTOR), errmsg("Cursor rewind are not supported.")));
#endif
    if (portal->holdStore) {
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(portal->holdContext);
        tuplestore_rescan(portal->holdStore);
        MemoryContextSwitchTo(oldcontext);
    }
    if (PortalGetQueryDesc(portal))
        ExecutorRewind(PortalGetQueryDesc(portal));

    portal->atStart = true;
    portal->atEnd = false;
    portal->portalPos = 0;
    portal->posOverflow = false;
}