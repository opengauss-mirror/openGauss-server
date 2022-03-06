/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * opfusion.cpp
 * The main part of the bypass executor. Instead of processing through the origin
 * Portal executor, the bypass executor provides a shortcut when the query is
 * simple.
 *
 * IDENTIFICATION
 * src/gausskernel/runtime/executor/opfusion.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion.h"

#include "access/printtup.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "catalog/heap.h"
#include "commands/copy.h"
#include "executor/node/nodeIndexscan.h"
#include "gstrace/executer_gstrace.h"
#include "instruments/instr_unique_sql.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "opfusion/opfusion_agg.h"
#include "opfusion/opfusion_delete.h"
#include "opfusion/opfusion_insert.h"
#include "opfusion/opfusion_select.h"
#include "opfusion/opfusion_selectforupdate.h"
#include "opfusion/opfusion_sort.h"
#include "opfusion/opfusion_update.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"
#include "storage/tcap.h"
#include "access/ustore/knl_uheap.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#include "opfusion/opfusion_mot.h"
#endif
#include "gs_ledger/blockchain.h"
#include "gs_policy/policy_common.h"

extern void opfusion_executeEnd(PlannedStmt *plannedstmt, const char *queryString, Snapshot snapshot);

OpFusion::OpFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list)
{
    /* for shared plancache, we only need to init local variables */
    if (psrc && psrc->opFusionObj && ((OpFusion *)(psrc->opFusionObj))->m_global->m_is_global) {
        Assert(psrc->gpc.status.InShareTable());
        m_global = ((OpFusion *)(psrc->opFusionObj))->m_global;
        InitLocals(context);
        m_global->m_psrc->gpc.status.AddRefcount();
    } else {
        InitGlobals(context, psrc, plantree_list);
        InitLocals(context);
    }
}

void OpFusion::InitGlobals(MemoryContext context, CachedPlanSource *psrc, List *plantree_list)
{
    bool is_shared = psrc && psrc->gpc.status.InShareTable();
    bool needShardCxt = !is_shared && psrc && psrc->gpc.status.IsSharePlan();
    MemoryContext cxt = NULL;
    if (needShardCxt) {
        cxt = AllocSetContextCreate(GLOBAL_PLANCACHE_MEMCONTEXT, "SharedOpfusionContext", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    } else {
        cxt = AllocSetContextCreate(context, "OpfusionContext", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);
    }

    MemoryContext old_context = MemoryContextSwitchTo(cxt);
    m_global = (OpFusionGlobalVariable *)palloc0(sizeof(OpFusionGlobalVariable));
    m_global->m_context = cxt;

    if (psrc == NULL && plantree_list != NULL) {
        m_global->m_is_pbe_query = false;
        m_global->m_psrc = NULL;
        m_global->m_cacheplan = NULL;
        m_global->m_planstmt = (PlannedStmt *)linitial(plantree_list);
    } else if (plantree_list == NULL && psrc != NULL) {
        Assert(psrc->gplan != NULL);
        m_global->m_is_pbe_query = true;
        m_global->m_psrc = psrc;
        m_global->m_cacheplan = psrc->gplan;
        m_global->m_cacheplan->refcount++;
        (void)pg_atomic_add_fetch_u32((volatile uint32 *)&m_global->m_cacheplan->global_refcount, 1);
        m_global->m_planstmt = (PlannedStmt *)linitial(m_global->m_cacheplan->stmt_list);
    } else {
        Assert(0);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("Both cacheplan and planstmt are NULL")));
    }
    m_global->m_reloid = 0;
    m_global->m_attrno = NULL;
    m_global->m_tupDesc = NULL;
    m_global->m_paramNum = 0;
    m_global->m_paramLoc = NULL;
    m_global->m_is_global = false;
    m_global->m_natts = 0;
    m_global->m_table_type = TAM_HEAP;
    m_global->m_is_bucket_rel = false;
    (void)MemoryContextSwitchTo(old_context);
}

void OpFusion::InitLocals(MemoryContext context)
{
    m_local.m_tmpContext = AllocSetContextCreate(context, "OpfusionTemporaryContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_local.m_localContext = AllocSetContextCreate(context, "OpfusionLocalContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_local.m_outParams = NULL;
    m_local.m_isFirst = true;
    m_local.m_rformats = NULL;
    m_local.m_isCompleted = false;
    m_local.m_position = 0;
    m_local.m_isInsideRec = false;
    m_local.m_tmpvals = NULL;
    m_local.m_isnull = NULL;
    m_local.m_reslot = NULL;
    m_local.m_receiver = NULL;
    m_local.m_values = NULL;
    m_local.m_tmpisnull = NULL;
    m_local.m_portalName = NULL;
    m_local.m_snapshot = NULL;
    m_local.m_scan = NULL;
    m_local.m_params = NULL;
    m_local.m_ledger_hash_exist = false;
    m_local.m_ledger_relhash = 0;
    m_local.m_optype = NONE_FUSION;
    m_local.m_resOwner = NULL;
}

/* clear local variables before global it */
void OpFusion::SaveInGPC(OpFusion *obj)
{
    Assert(!obj->IsGlobal());
    Assert(obj->m_global->m_context->is_shared);
    /* only can change global flag here */
    obj->m_global->m_is_global = true;
    removeFusionFromHtab(obj->m_local.m_portalName);
    MemoryContextDelete(obj->m_local.m_tmpContext);
    MemoryContextDelete(obj->m_local.m_localContext);
    int rc = -1;
    rc = memset_s((void *)&obj->m_local, sizeof(OpFusionLocaleVariable), 0, sizeof(OpFusionLocaleVariable));
    securec_check(rc, "\0", "\0");
    MemoryContextSeal(obj->m_global->m_context);
}

void OpFusion::DropGlobalOpfusion(OpFusion *obj)
{
    Assert(obj->IsGlobal());
    MemoryContextUnSeal(obj->m_global->m_context);
    ReleaseCachedPlan(obj->m_global->m_cacheplan, false);
    MemoryContextDelete(obj->m_global->m_context);
    delete obj;
}

#ifdef ENABLE_MOT
FusionType OpFusion::GetMotFusionType(PlannedStmt *plannedStmt)
{
    FusionType result;
    switch (plannedStmt->commandType) {
        case CMD_SELECT:
            result = MOT_JIT_SELECT_FUSION;
            break;
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
            result = MOT_JIT_MODIFY_FUSION;
            break;
        default:
            result = NOBYPASS_NO_QUERY_TYPE;
            break;
    }
    return result;
}
#endif

FusionType OpFusion::getFusionType(CachedPlan *plan, ParamListInfo params, List *plantree_list)
{
    if (IsInitdb == true) {
        return NONE_FUSION;
    }

    if (!u_sess->attr.attr_sql.enable_opfusion) {
        return NONE_FUSION;
    }

    List *plist = NULL;
    if (plan && plantree_list == NULL) {
        plist = plan->stmt_list;
    } else if (plantree_list && plan == NULL) {
        plist = plantree_list;
    } else {
        /* sql has no plan, do nothing */
        return NONE_FUSION;
    }

    /* check stmt num */
    if (list_length(plist) != 1) {
        return NOBYPASS_NO_SIMPLE_PLAN;
    }

    /* check whether is planedstmt */
    Node *st = (Node *)linitial(plist);
    if (!IsA(st, PlannedStmt)) {
        /* may be ddl */
        return NONE_FUSION;
    }

    PlannedStmt *planned_stmt = (PlannedStmt *)st;
    if (planned_stmt->utilityStmt != NULL) {
        /* may be utility functions */
        return NONE_FUSION;
    }

    /* IMPORTANT: Opfusion NOT SUPPORT version table scan. */
    if (TvIsVersionPlan(planned_stmt)) {
        return NOBYPASS_VERSION_SCAN_PLAN;
    }

    FusionType result = NONE_FUSION;
#ifdef ENABLE_MOT
    if (plan && plan->mot_jit_context && JitExec::IsMotCodegenEnabled()) {
        result = GetMotFusionType(planned_stmt);
    } else {
#endif
        if (planned_stmt->subplans != NULL || planned_stmt->initPlan != NULL) {
            return NOBYPASS_NO_SIMPLE_PLAN;
        }

        if (((PlannedStmt *)st)->commandType == CMD_SELECT) {
            result = getSelectFusionType(plist, params);
        } else if (planned_stmt->commandType == CMD_INSERT) {
            result = getInsertFusionType(plist, params);
        } else if (planned_stmt->commandType == CMD_UPDATE) {
            result = getUpdateFusionType(plist, params);
        } else if (planned_stmt->commandType == CMD_DELETE) {
            result = getDeleteFusionType(plist, params);
        } else {
            result = NOBYPASS_NO_QUERY_TYPE;
        }

#ifdef ENABLE_MOT
    }
#endif

    return result;
}

void OpFusion::setCurrentOpFusionObj(OpFusion *obj)
{
    u_sess->exec_cxt.CurrentOpFusionObj = obj;
}

void OpFusion::checkPermission()
{
    bool check = false;
    if (!(IS_PGXC_DATANODE && (IsConnFromCoord() || IsConnFromDatanode()))) {
        check = true;
    }
    if (m_global->m_planstmt->in_compute_pool) {
        check = false;
    }

    if (t_thrd.pgxc_cxt.is_gc_fdw && t_thrd.pgxc_cxt.is_gc_fdw_analyze) {
        check = false;
    }

    if (check) {
        (void)ExecCheckRTPerms(m_global->m_planstmt->rtable, true);
    }
}

void OpFusion::executeInit()
{
    if (m_local.m_isFirst == true) {
        checkPermission();
    }
    if (m_local.m_resOwner == NULL) {
        m_local.m_resOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    }
    if (IS_SINGLE_NODE && ENABLE_WORKLOAD_CONTROL) {
        u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
        WLMCreateDNodeInfoOnDN(NULL);
        WLMCreateIOInfoOnDN();
    }

    if (u_sess->attr.attr_common.XactReadOnly) {
        ExecCheckXactReadOnly(m_global->m_planstmt);
    }

#ifdef ENABLE_MOT
    if (!(u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_cacheplan &&
        u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_cacheplan->storageEngineType == SE_TYPE_MOT)) {
#endif
        if (m_local.m_snapshot == NULL) {
            m_local.m_snapshot = RegisterSnapshot(GetTransactionSnapshot());
        }
        PushActiveSnapshot(m_local.m_snapshot);
#ifdef ENABLE_MOT
    }
#endif
}

void OpFusion::auditRecord()
{
    if ((u_sess->attr.attr_security.Audit_DML_SELECT != 0 || u_sess->attr.attr_security.Audit_DML != 0) &&
        u_sess->attr.attr_security.Audit_enabled && IsPostmasterEnvironment) {
        char *object_name = NULL;

        switch (m_global->m_planstmt->commandType) {
            case CMD_INSERT:
            case CMD_DELETE:
            case CMD_UPDATE:
                if (u_sess->attr.attr_security.Audit_DML != 0) {
                    object_name = pgaudit_get_relation_name(m_global->m_planstmt->rtable);
                    pgaudit_dml_table(object_name, m_global->m_is_pbe_query ? m_global->m_psrc->query_string :
                                                                              t_thrd.postgres_cxt.debug_query_string);
                }
                break;
            case CMD_SELECT:
                if (u_sess->attr.attr_security.Audit_DML_SELECT != 0) {
                    object_name = pgaudit_get_relation_name(m_global->m_planstmt->rtable);
                    pgaudit_dml_table_select(object_name, m_global->m_is_pbe_query ?
                        m_global->m_psrc->query_string :
                        t_thrd.postgres_cxt.debug_query_string);
                }
                break;
            /* Not support others */
            default:
                break;
        }
    }
}

bool OpFusion::executeEnd(const char *portal_name, bool *isQueryCompleted)
{
#ifdef ENABLE_MOT
    if (!(u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_cacheplan &&
        u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_cacheplan->storageEngineType == SE_TYPE_MOT)) {
#endif
        opfusion_executeEnd(m_global->m_planstmt,
            ((m_global->m_psrc == NULL) ? NULL : (m_global->m_psrc->query_string)), GetActiveSnapshot());
        const char *query_string = t_thrd.postgres_cxt.debug_query_string;
        if (query_string == NULL && m_global->m_psrc != NULL) {
            query_string = m_global->m_psrc->query_string;
        }
        if (m_local.m_ledger_hash_exist && query_string != NULL) {
            opfusion_ledger_ExecutorEnd(m_local.m_optype, m_global->m_reloid, query_string, m_local.m_ledger_relhash);
        }

        PopActiveSnapshot();
#ifdef ENABLE_MOT
    }
#endif

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check all memory contexts when executor starts */
    MemoryContextCheck(TopMemoryContext, false);

    /* Check per-query memory context before Opfusion temp memory */
    MemoryContextCheck(m_local.m_tmpContext, true);
#endif
    bool has_completed = m_local.m_isCompleted;
    m_local.m_isFirst = false;

    /* reset the context. */
    if (m_local.m_isCompleted) {
        UnregisterSnapshot(m_local.m_snapshot);
        m_local.m_snapshot = NULL;
        MemoryContextDeleteChildren(m_local.m_tmpContext);
        /* reset the context. */
        MemoryContextReset(m_local.m_tmpContext);
        /* clear hash table */
        removeFusionFromHtab(portal_name);
        m_local.m_outParams = NULL;
        m_local.m_isCompleted = false;
        if (isQueryCompleted) {
            *isQueryCompleted = true;
        }
        u_sess->xact_cxt.pbe_execute_complete = true;
        m_local.m_resOwner = NULL;
    } else {
        if (isQueryCompleted)
            *isQueryCompleted = false;
        u_sess->xact_cxt.pbe_execute_complete = false;
        if (ENABLE_GPC)
            Assert(locateFusion(m_local.m_portalName) != NULL);
    }

    auditRecord();
    if (u_sess->attr.attr_common.pgstat_track_activities && u_sess->attr.attr_common.pgstat_track_sql_count) {
        report_qps_type(m_global->m_planstmt->commandType);
        report_qps_type(CMD_DML);
    }

    return has_completed;
}

void OpFusion::fusionExecute(StringInfo msg, char *completionTag, bool isTopLevel, bool *isQueryCompleted)
{
    long max_rows = FETCH_ALL;
    bool completed = false;
    const char *portal_name = NULL;

    /* msg is null means 'U' message, need fetch all. */
    if (msg != NULL) {
        /* 'U' message and simple query has already assign value to debug_query_string. */
        if (u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_psrc != NULL) {
            t_thrd.postgres_cxt.debug_query_string =
                u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_psrc->query_string;
        }
        portal_name = pq_getmsgstring(msg);
        max_rows = (long)pq_getmsgint(msg, 4);
        if (max_rows <= 0)
            max_rows = FETCH_ALL;
    }

    u_sess->exec_cxt.CurrentOpFusionObj->executeInit();
    gstrace_entry(GS_TRC_ID_BypassExecutor);
    bool old_status = u_sess->exec_cxt.need_track_resource;
    if (u_sess->attr.attr_resource.resource_track_cost == 0 && u_sess->attr.attr_resource.enable_resource_track &&
        u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_NONE) {
        u_sess->exec_cxt.need_track_resource = true;
        WLMSetCollectInfoStatus(WLM_STATUS_RUNNING);
    }
    ResourceOwner saveResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = u_sess->exec_cxt.CurrentOpFusionObj->m_local.m_resOwner;
        if (!(g_instance.status > NoShutdown) && opfusion_unified_audit_executor_hook != NULL) {
            opfusion_unified_audit_executor_hook(u_sess->exec_cxt.CurrentOpFusionObj->m_global->m_planstmt);
        }
        u_sess->exec_cxt.CurrentOpFusionObj->execute(max_rows, completionTag);
        u_sess->exec_cxt.need_track_resource = old_status;
        gstrace_exit(GS_TRC_ID_BypassExecutor);
        completed = u_sess->exec_cxt.CurrentOpFusionObj->executeEnd(portal_name, isQueryCompleted);
        if (completed && u_sess->exec_cxt.CurrentOpFusionObj->IsGlobal()) {
            Assert(ENABLE_GPC);
            tearDown(u_sess->exec_cxt.CurrentOpFusionObj);
        }
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        if (!(g_instance.status > NoShutdown) && opfusion_unified_audit_flush_logs_hook != NULL) {
            opfusion_unified_audit_flush_logs_hook(AUDIT_OK);
        }
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = saveResourceOwner;
        if (!(g_instance.status > NoShutdown) && opfusion_unified_audit_flush_logs_hook != NULL) {
            opfusion_unified_audit_flush_logs_hook(AUDIT_FAILED);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    UpdateSingleNodeByPassUniqueSQLStat(isTopLevel);
}

bool OpFusion::process(int op, StringInfo msg, char *completionTag, bool isTopLevel, bool *isQueryCompleted)
{
    if (op == FUSION_EXECUTE && msg != NULL)
        refreshCurFusion(msg);

    bool res = false;
    if (u_sess->exec_cxt.CurrentOpFusionObj == NULL) {
        return res;
    }

    switch (op) {
        case FUSION_EXECUTE: {
            u_sess->exec_cxt.CurrentOpFusionObj->fusionExecute(msg, completionTag, isTopLevel, isQueryCompleted);
            break;
        }

        case FUSION_DESCRIB: {
            u_sess->exec_cxt.CurrentOpFusionObj->describe(msg);
            break;
        }

        default: {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("unrecognized bypass support process option: %d", (int)op)));
        }
    }
    res = true;

    /*
     * Emit duration logging if appropriate.
     */
    u_sess->exec_cxt.CurrentOpFusionObj->CheckLogDuration();

    if (op == FUSION_EXECUTE) {
        u_sess->exec_cxt.CurrentOpFusionObj = NULL;
    }
    return res;
}

void OpFusion::CheckLogDuration()
{
    char msec_str[PRINTF_DST_MAX];
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2: {
            ereport(LOG, (errmsg("duration: %s ms queryid %lu unique id %lu", msec_str, u_sess->debug_query_id,
                u_sess->slow_query_cxt.slow_query.unique_sql_id), errhidestmt(true)));
            break;
        }
        default:
            break;
    }
}

void OpFusion::CopyFormats(int16 *formats, int numRFormats)
{
    MemoryContext old_context = MemoryContextSwitchTo(m_local.m_tmpContext);
    int natts;
    if (m_global->m_tupDesc == NULL) {
        Assert(0);
        return;
    }
    if (formats == NULL && numRFormats > 0) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("r_formats can not be NULL when num of rformats is not 0")));
    }
    natts = m_global->m_tupDesc->natts;
    m_local.m_rformats = (int16 *)palloc(natts * sizeof(int16));
    if (numRFormats > 1) {
        /* format specified for each column */
        if (numRFormats != natts)
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("bind message has %d result formats but query has %d columns", numRFormats, natts)));
        errno_t errorno = EOK;
        errorno = memcpy_s(m_local.m_rformats, natts * sizeof(int16), formats, natts * sizeof(int16));
        securec_check(errorno, "\0", "\0");
    } else if (numRFormats > 0) {
        /* single format specified, use for all columns */
        int16 format1 = formats[0];
        for (int i = 0; i < natts; i++) {
            m_local.m_rformats[i] = format1;
        }
    } else {
        /* use default format for all columns */
        for (int i = 0; i < natts; i++) {
            m_local.m_rformats[i] = 0;
        }
    }
    MemoryContextSwitchTo(old_context);
}

void OpFusion::useOuterParameter(ParamListInfo params)
{
    m_local.m_outParams = params;
}


void *OpFusion::FusionFactory(FusionType ftype, MemoryContext context, CachedPlanSource *psrc, List *plantree_list,
    ParamListInfo params)
{
    Assert(ftype != BYPASS_OK);
    if (u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_LOG) {
        BypassUnsupportedReason(ftype);
    }
    if (ftype > BYPASS_OK) {
        return NULL;
    }
    void *opfusionObj = NULL;
    MemoryContext objCxt = NULL;
    bool isShared = psrc && psrc->gpc.status.InShareTable();
    if (isShared) {
        /* global plansource cannot new bypass on plansource context, must generate on local context */
        objCxt = context;
    } else {
        /* for opfusion for plansource may global later, generate on global context */
        objCxt = (psrc && psrc->gpc.status.IsSharePlan()) ? GLOBAL_PLANCACHE_MEMCONTEXT : context;
    }
    switch (ftype) {
        case SELECT_FUSION:
            opfusionObj = New(objCxt)SelectFusion(context, psrc, plantree_list, params);
            break;
        case INSERT_FUSION:
            opfusionObj = New(objCxt)InsertFusion(context, psrc, plantree_list, params);
            break;
        case UPDATE_FUSION:
            opfusionObj = New(objCxt)UpdateFusion(context, psrc, plantree_list, params);
            break;
        case DELETE_FUSION:
            opfusionObj = New(objCxt)DeleteFusion(context, psrc, plantree_list, params);
            break;
        case SELECT_FOR_UPDATE_FUSION:
            opfusionObj = New(objCxt)SelectForUpdateFusion(context, psrc, plantree_list, params);
            break;
#ifdef ENABLE_MOT
        case MOT_JIT_SELECT_FUSION:
            opfusionObj = New(objCxt)MotJitSelectFusion(context, psrc, plantree_list, params);
            break;
        case MOT_JIT_MODIFY_FUSION:
            opfusionObj = New(objCxt)MotJitModifyFusion(context, psrc, plantree_list, params);
            break;
#endif
        case AGG_INDEX_FUSION:
            opfusionObj = New(objCxt)AggFusion(context, psrc, plantree_list, params);
            break;
        case SORT_INDEX_FUSION:
            opfusionObj = New(objCxt)SortFusion(context, psrc, plantree_list, params);
            break;
        case NONE_FUSION:
            opfusionObj = NULL;
            break;
        default:
            opfusionObj = NULL;
            break;
    }
    if (opfusionObj != NULL && !((OpFusion *)opfusionObj)->m_global->m_is_global)
        ((OpFusion *)opfusionObj)->m_global->m_type = ftype;
    return opfusionObj;
}

void OpFusion::updatePreAllocParamter(StringInfo input_message)
{
    /* Switch back to message context */
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    int num_pformats;
    int16 *pformats = NULL;
    int num_params;
    int num_rformats;
    int paramno;
    ParamListInfo params = m_local.m_params;
    /* Get the parameter format codes */
    num_pformats = pq_getmsgint(input_message, 2);
    if (num_pformats > 0) {
        int i;

        pformats = (int16 *)palloc(num_pformats * sizeof(int16));
        for (i = 0; i < num_pformats; i++) {
            pformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the parameter value count */
    num_params = pq_getmsgint(input_message, 2);
    if (unlikely(num_params != m_global->m_paramNum)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("unmatched parameter number")));
    }
    (void)MemoryContextSwitchTo(m_local.m_tmpContext);
    if (num_params > 0) {
        for (paramno = 0; paramno < num_params; paramno++) {
            Oid ptype = m_global->m_psrc->param_types[paramno];
            int32 plength;
            Datum pval;
            bool isNull = false;
            StringInfoData pbuf;
            char csave;
            int16 pformat;
            plength = pq_getmsgint(input_message, 4);
            isNull = (plength == -1);
            /* add null value process for date type */
            if ((VARCHAROID == ptype || TIMESTAMPOID == ptype || TIMESTAMPTZOID == ptype || TIMEOID == ptype ||
                TIMETZOID == ptype || INTERVALOID == ptype || SMALLDATETIMEOID == ptype) &&
                plength == 0 && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                isNull = true;
            }

            /*
             * Insert into bind values support illegal characters import,
             * and this just wroks for char type attribute.
             */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = IsCharType(ptype);

            if (!isNull) {
                const char *pvalue = pq_getmsgbytes(input_message, plength);

                /*
                 * Rather than copying data around, we just set up a phony
                 * StringInfo pointing to the correct portion of the message
                 * buffer.    We assume we can scribble on the message buffer so
                 * as to maintain the convention that StringInfos have a
                 * trailing null.  This is grotty but is a big win when
                 * dealing with very large parameter strings.
                 */
                pbuf.data = (char *)pvalue;
                pbuf.maxlen = plength + 1;
                pbuf.len = plength;
                pbuf.cursor = 0;

                csave = pbuf.data[plength];
                pbuf.data[plength] = '\0';
            } else {
                pbuf.data = NULL; /* keep compiler quiet */
                csave = 0;
            }

            if (num_pformats > 1) {
                Assert(pformats != NULL);
                pformat = pformats[paramno];
            } else if (pformats != NULL) {
                Assert(pformats != NULL);
                pformat = pformats[0];
            } else {
                pformat = 0; /* default = text */
            }

            if (pformat == 0) {
                /* text mode */
                Oid typinput;
                Oid typioparam;
                char *pstring = NULL;

                getTypeInputInfo(ptype, &typinput, &typioparam);

                /*
                 * We have to do encoding conversion before calling the
                 * typinput routine.
                 */
                if (isNull) {
                    pstring = NULL;
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                /* Free result of encoding conversion, if any */
                if (pstring != NULL && pstring != pbuf.data) {
                    pfree(pstring);
                }
            } else if (pformat == 1) {
                /* binary mode */
                Oid typreceive;
                Oid typioparam;
                StringInfo bufptr;

                /*
                 * Call the parameter type's binary input converter
                 */
                getTypeBinaryInputInfo(ptype, &typreceive, &typioparam);

                if (isNull) {
                    bufptr = NULL;
                } else {
                    bufptr = &pbuf;
                }

                pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

                /* Trouble if it didn't eat the whole buffer */
                if (!isNull && pbuf.cursor != pbuf.len) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                        errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
                }
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
                pval = 0; /* keep compiler quiet */
            }

            /* Restore message buffer contents */
            if (!isNull) {
                pbuf.data[plength] = csave;
            }

            params->params[paramno].value = pval;
            params->params[paramno].isnull = isNull;

            /*
             * We mark the params as CONST.  This ensures that any custom plan
             * makes full use of the parameter values.
             */
            params->params[paramno].pflags = PARAM_FLAG_CONST;
            params->params[paramno].ptype = ptype;

            /* Reset the compatible illegal chars import flag */
            u_sess->mb_cxt.insertValuesBind_compatible_illegal_chars = false;
        }
    }

    int16 *rformats = NULL;
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    /* Get the result format codes */
    num_rformats = pq_getmsgint(input_message, 2);
    if (num_rformats > 0) {
        int i;
        rformats = (int16 *)palloc(num_rformats * sizeof(int16));
        for (i = 0; i < num_rformats; i++) {
            rformats[i] = pq_getmsgint(input_message, 2);
        }
    }
    CopyFormats(rformats, num_rformats);
    pq_getmsgend(input_message);
    pfree_ext(rformats);
    MemoryContextSwitchTo(old_context);
}

void OpFusion::describe(StringInfo msg)
{
    if (m_global->m_psrc->resultDesc != NULL) {
        StringInfoData buf;
        initStringInfo(&buf);
        SendRowDescriptionMessage(&buf, m_global->m_tupDesc, m_global->m_planstmt->planTree->targetlist,
            m_local.m_rformats);
        pfree_ext(buf.data);
    } else {
        pq_putemptymessage('n');
    }
}

void OpFusion::setPreparedDestReceiver(DestReceiver *preparedDest)
{
    m_local.m_receiver = preparedDest;
    m_local.m_isInsideRec = false;
}

/* evaluate datum from node, simple node types such as Const, Param, Var excludes FuncExpr and OpExpr */
Datum OpFusion::EvalSimpleArg(Node *arg, bool *is_null, Datum *values, bool *isNulls)
{
    switch (nodeTag(arg)) {
        case T_Const:
            *is_null = ((Const *)arg)->constisnull;
            return ((Const *)arg)->constvalue;
        case T_Var:
            *is_null = isNulls[(((Var *)arg)->varattno - 1)];
            return values[(((Var *)arg)->varattno - 1)];
        case T_Param: {
            ParamListInfo param_list = m_local.m_outParams != NULL ? m_local.m_outParams : m_local.m_params;

            if (param_list->params[(((Param *)arg)->paramid - 1)].isnull) {
                *is_null = true;
            }
            return param_list->params[(((Param *)arg)->paramid - 1)].value;
        }
        case T_RelabelType: {
            if (IsA(((RelabelType *)arg)->arg, FuncExpr)) {
                FuncExpr *expr = (FuncExpr *)(((RelabelType *)arg)->arg);
                return CalFuncNodeVal(expr->funcid, expr->args, is_null, values, isNulls);
            } else if (IsA(((RelabelType *)arg)->arg, OpExpr)) {
                OpExpr *expr = (OpExpr *)(((RelabelType *)arg)->arg);
                return CalFuncNodeVal(expr->opfuncid, expr->args, is_null, values, isNulls);
            }

            return EvalSimpleArg((Node *)((RelabelType *)arg)->arg, is_null, values, isNulls);
        }
        default:
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected node type: %d when processing bypass expression.", (int)nodeTag(arg))));
            break;
    }

    Assert(0);
    return 0;
}

Datum OpFusion::CalFuncNodeVal(Oid functionId, List *args, bool *is_null, Datum *values, bool *isNulls)
{
    if (*is_null) {
        return 0;
    }

    Node *first_arg_node = (Node *)linitial(args);
    Datum *arg;
    bool *is_nulls = (bool *)palloc0(4 * sizeof(bool));
    arg = (Datum *)palloc0(4 * sizeof(Datum));

    /* for now, we assuming FuncExpr and OpExpr only appear in the first arg */
    if (IsA(first_arg_node, FuncExpr)) {
        arg[0] = CalFuncNodeVal(((FuncExpr *)first_arg_node)->funcid, ((FuncExpr *)first_arg_node)->args, &is_nulls[0],
            values, isNulls);
    } else if (IsA(first_arg_node, OpExpr)) {
        arg[0] = CalFuncNodeVal(((OpExpr *)first_arg_node)->opfuncid, ((OpExpr *)first_arg_node)->args, &is_nulls[0],
            values, isNulls);
    } else {
        arg[0] = EvalSimpleArg(first_arg_node, &is_nulls[0], values, isNulls);
    }

    int length = list_length(args);
    if (length < 1 || length > 4) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unexpected arg length : %d when processing bypass expression.", length)));
    }
    *is_null = is_nulls[0];
    ListCell *tmp_arg = list_head(args);
    for (int i = 1; i < length; i++) {
        tmp_arg = lnext(tmp_arg);
        arg[i] = EvalSimpleArg((Node *)lfirst(tmp_arg), &is_nulls[i], values, isNulls);
        *is_null = *is_null || is_nulls[i];
    }
    if (*is_null) {
        return 0;
    }

    switch (length) {
        case 1:
            return OidFunctionCall1(functionId, arg[0]);
        case 2:
            return OidFunctionCall2(functionId, arg[0], arg[1]);
        case 3:
            return OidFunctionCall3(functionId, arg[0], arg[1], arg[2]);
        case 4:
            return OidFunctionCall4(functionId, arg[0], arg[1], arg[2], arg[3]);
        default: {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                errmsg("unrecognized bypass support number of arguments function calls: %d", (int)length)));
        }
    }

    Assert(0);
    return 0;
}

void OpFusion::tearDown(OpFusion *opfusion)
{
    if (opfusion == NULL) {
        return;
    }

    removeFusionFromHtab(opfusion->m_local.m_portalName);

    if (!opfusion->IsGlobal()) {
        if (opfusion->m_global->m_cacheplan != NULL) {
            ReleaseCachedPlan(opfusion->m_global->m_cacheplan, false);
        }
        if (opfusion->m_global->m_psrc != NULL) {
            opfusion->m_global->m_psrc->is_checked_opfusion = false;
            opfusion->m_global->m_psrc->opFusionObj = NULL;
        }
        MemoryContextDelete(opfusion->m_global->m_context);
    } else {
        opfusion->m_global->m_psrc->gpc.status.SubRefCount();
    }

    MemoryContextDelete(opfusion->m_local.m_tmpContext);
    MemoryContextDelete(opfusion->m_local.m_localContext);

    delete opfusion;

    OpFusion::setCurrentOpFusionObj(NULL);
}

void OpFusion::clearForCplan(OpFusion *opfusion, CachedPlanSource *psrc)
{
    if (opfusion == NULL)
        return;
    if (psrc->cplan != NULL) {
        Assert(!psrc->gpc.status.InShareTable());
        tearDown(opfusion);
    }
}

/* just for select and select for */
void OpFusion::setReceiver()
{
    if (m_local.m_isInsideRec) {
        m_local.m_receiver = CreateDestReceiver((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
    }

    ((DR_printtup *)m_local.m_receiver)->sendDescrip = false;
    if (m_local.m_receiver->mydest == DestRemote || m_local.m_receiver->mydest == DestRemoteExecute) {
        ((DR_printtup *)m_local.m_receiver)->formats = m_local.m_rformats;
    }
    (*m_local.m_receiver->rStartup)(m_local.m_receiver, CMD_SELECT, m_global->m_tupDesc);
    if (!m_global->m_is_pbe_query) {
        StringInfoData buf = ((DR_printtup *)m_local.m_receiver)->buf;
        initStringInfo(&buf);
        SendRowDescriptionMessage(&buf, m_global->m_tupDesc, m_global->m_planstmt->planTree->targetlist,
            m_local.m_rformats);
    }
}

void OpFusion::initParams(ParamListInfo params)
{
    m_local.m_outParams = params;
    /* init params */
    if (m_global->m_is_pbe_query && !IsGlobal() && params != NULL) {
        m_global->m_paramNum = params->numParams;
        m_local.m_params =
            (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + m_global->m_paramNum * sizeof(ParamExternData));
        m_local.m_params->paramFetch = NULL;
        m_local.m_params->paramFetchArg = NULL;
        m_local.m_params->parserSetup = NULL;
        m_local.m_params->parserSetupArg = NULL;
        m_local.m_params->params_need_process = false;
        m_local.m_params->numParams = m_global->m_paramNum;
    } else if (IsGlobal() && m_global->m_paramNum > 0) {
        m_local.m_params =
            (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + m_global->m_paramNum * sizeof(ParamExternData));
        m_local.m_params->paramFetch = NULL;
        m_local.m_params->paramFetchArg = NULL;
        m_local.m_params->parserSetup = NULL;
        m_local.m_params->parserSetupArg = NULL;
        m_local.m_params->params_need_process = false;
        m_local.m_params->numParams = m_global->m_paramNum;
    }
}

bool OpFusion::isQueryCompleted()
{
    if (u_sess->exec_cxt.CurrentOpFusionObj != NULL)
        return false;
    else {
        return true; /* bypass completed */
    }
}

void OpFusion::bindClearPosition()
{
    m_local.m_isCompleted = false;
    m_local.m_position = 0;
    m_local.m_outParams = NULL;
    m_local.m_snapshot = NULL;

    MemoryContextDeleteChildren(m_local.m_tmpContext);
    /* reset the context. */
    MemoryContextReset(m_local.m_tmpContext);
}

void OpFusion::initFusionHtab()
{
    HASHCTL hash_ctl;
    errno_t rc = 0;
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(pnFusionObj);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->pcache_cxt.pn_fusion_htab =
        hash_create("OpFusion_hashtable", HASH_TBL_LEN, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
}

void OpFusion::ClearInUnexpectSituation()
{
    if (u_sess->pcache_cxt.pn_fusion_htab != NULL) {
        HASH_SEQ_STATUS seq;
        pnFusionObj *entry = NULL;
        hash_seq_init(&seq, u_sess->pcache_cxt.pn_fusion_htab);
        while ((entry = (pnFusionObj *)hash_seq_search(&seq)) != NULL) {
            OpFusion *curr = entry->opfusion;
            if (curr->IsGlobal()) {
                curr->clean();
                OpFusion::tearDown(curr);
            } else if (curr->m_local.m_portalName == NULL ||
                strcmp(curr->m_local.m_portalName, entry->portalname) == 0) {
                curr->clean();
                /* only opfusion has reference on cachedplan, no need any more */
                if (curr->m_global->m_cacheplan && curr->m_global->m_cacheplan->refcount == 1) {
                    ReleaseCachedPlan(curr->m_global->m_cacheplan, false);
                    MemoryContextDelete(curr->m_local.m_tmpContext);
                    MemoryContextDelete(curr->m_local.m_localContext);
                    MemoryContextDelete(curr->m_global->m_context);
                    delete curr;
                }
            }
            removeFusionFromHtab(entry->portalname);
        }
    }
    u_sess->exec_cxt.CurrentOpFusionObj = NULL;
}

void OpFusion::ClearInSubUnexpectSituation(ResourceOwner owner)
{
    if (u_sess->pcache_cxt.pn_fusion_htab != NULL) {
        HASH_SEQ_STATUS seq;
        pnFusionObj *entry = NULL;
        hash_seq_init(&seq, u_sess->pcache_cxt.pn_fusion_htab);
        while ((entry = (pnFusionObj *)hash_seq_search(&seq)) != NULL) {
            OpFusion *curr = entry->opfusion;
            if (curr->m_local.m_resOwner != owner)
                continue;
            if (curr->IsGlobal()) {
                curr->clean();
                OpFusion::tearDown(curr);
            } else if (curr->m_local.m_portalName == NULL ||
                strcmp(curr->m_local.m_portalName, entry->portalname) == 0) {
                curr->clean();
                /* only opfusion has reference on cachedplan, no need any more */
                if (curr->m_global->m_cacheplan && curr->m_global->m_cacheplan->refcount == 1) {
                    ReleaseCachedPlan(curr->m_global->m_cacheplan, false);
                    MemoryContextDelete(curr->m_local.m_tmpContext);
                    MemoryContextDelete(curr->m_local.m_localContext);
                    MemoryContextDelete(curr->m_global->m_context);
                    delete curr;
                }
            }
            removeFusionFromHtab(entry->portalname);
        }
    }
}


void OpFusion::clean()
{
    ResourceOwner oldResourceOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    bool hasChangeResOwner = false;
    if (m_local.m_resOwner && m_local.m_resOwner != oldResourceOwner) {
        t_thrd.utils_cxt.CurrentResourceOwner = m_local.m_resOwner;
        hasChangeResOwner = true;
    }
    PG_TRY();
    {
        UnregisterSnapshot(m_local.m_snapshot);
        m_local.m_snapshot = NULL;
        m_local.m_position = 0;
        m_local.m_outParams = NULL;
        if (m_local.m_scan)
            m_local.m_scan->End(true);
        m_local.m_isCompleted = false;
        MemoryContextDeleteChildren(m_local.m_tmpContext);
        /* reset the context. */
        MemoryContextReset(m_local.m_tmpContext);
    }
    PG_CATCH();
    {
        t_thrd.utils_cxt.CurrentResourceOwner = oldResourceOwner;
        PG_RE_THROW();
    }
    PG_END_TRY();
    t_thrd.utils_cxt.CurrentResourceOwner = oldResourceOwner;
    if (hasChangeResOwner) {
        m_local.m_resOwner = NULL;
    }
}

void OpFusion::storeFusion(const char *portalname)
{
    if (portalname == NULL || portalname[0] == '\0') {
        pfree_ext(m_local.m_portalName);
        return;
    }
    pnFusionObj *entry = NULL;
    if (!u_sess->pcache_cxt.pn_fusion_htab)
        initFusionHtab();

    removeFusionFromHtab(m_local.m_portalName);
    entry = (pnFusionObj *)(hash_search(u_sess->pcache_cxt.pn_fusion_htab, portalname, HASH_ENTER, NULL));
    entry->opfusion = this;

    MemoryContext old_cxt = MemoryContextSwitchTo(m_local.m_localContext);
    pfree_ext(m_local.m_portalName);
    m_local.m_portalName = pstrdup(portalname);
    MemoryContextSwitchTo(old_cxt);
}

OpFusion *OpFusion::locateFusion(const char *portalname)
{
    pnFusionObj *entry = NULL;
    if (u_sess->pcache_cxt.pn_fusion_htab) {
        entry = (pnFusionObj *)(hash_search(u_sess->pcache_cxt.pn_fusion_htab, portalname, HASH_FIND, NULL));
    }

    if (entry) {
        return entry->opfusion;
    } else {
        return NULL;
    }
}

void OpFusion::removeFusionFromHtab(const char *portalname)
{
    if (u_sess->pcache_cxt.pn_fusion_htab && portalname != NULL && portalname[0] != '\0') {
        hash_search(u_sess->pcache_cxt.pn_fusion_htab, portalname, HASH_REMOVE, NULL);
    }
}

void OpFusion::refreshCurFusion(StringInfo msg)
{
    OpFusion *opfusion = NULL;
    int oldCursor = msg->cursor;
    const char *portal_name = pq_getmsgstring(msg);
    if (portal_name[0] != '\0') {
        opfusion = OpFusion::locateFusion(portal_name);
        OpFusion::setCurrentOpFusionObj(opfusion);
    }
    msg->cursor = oldCursor;
}
