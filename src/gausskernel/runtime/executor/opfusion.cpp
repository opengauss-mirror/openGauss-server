/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * opfusion.cpp
 *        The main part of the bypass executor. Instead of processing through the origin
 *            Portal executor, the bypass executor provides a shortcut when the query is
 *            simple.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/opfusion.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion.h"

#include "access/printtup.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/tupdesc.h"
#include "catalog/pg_aggregate.h"
#include "catalog/storage_gtt.h"
#include "catalog/heap.h"
#include "commands/copy.h"
#include "executor/nodeIndexscan.h"
#include "gstrace/executer_gstrace.h"
#include "instruments/instr_unique_sql.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "tcop/tcopprot.h"
#include "commands/matview.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif

extern void opfusion_executeEnd(PlannedStmt* plannedstmt, const char *queryString, Snapshot snapshot);

OpFusion::OpFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list)
{
    m_context = AllocSetContextCreate(
        context, "OpfusionContext", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_tmpContext = AllocSetContextCreate(context,
                                        "OpfusionTemporaryContext",
                                        ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE,
                                        ALLOCSET_DEFAULT_MAXSIZE);

    if (psrc == NULL && plantree_list != NULL) {
        m_is_pbe_query = false;
        m_psrc = NULL;
        m_cacheplan = NULL;
        m_planstmt = (PlannedStmt*)linitial(plantree_list);
    } else if (plantree_list == NULL && psrc != NULL) {
        m_is_pbe_query = true;
        m_psrc = psrc;
        m_cacheplan = psrc->cplan ? psrc->cplan : psrc->gplan;
        m_cacheplan->refcount++;
        m_planstmt = (PlannedStmt*)linitial(m_cacheplan->stmt_list);
    } else {
        Assert(0);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("Both cacheplan and planstmt are NULL")));
    }

    m_outParams = NULL;
    m_params = NULL;
	m_isFirst = true;
    m_rformats = NULL;
    m_isCompleted = false;
    m_position = 0;
    m_isInsideRec = false;
    m_tmpvals = NULL;
	m_reloid = 0;
    m_isnull = NULL;
    m_attrno = NULL;
    m_reslot = NULL;
    m_receiver = NULL;
	m_tupDesc = NULL;
    m_values = NULL;
    m_paramNum = 0;
    m_paramLoc = NULL;
	m_tmpisnull = NULL;
    m_portalName = NULL;
    m_snapshot = NULL;
    m_scan = NULL;
}

#ifdef ENABLE_MOT
FusionType OpFusion::GetMotFusionType(PlannedStmt* plannedStmt)
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

FusionType OpFusion::getFusionType(CachedPlan* plan, ParamListInfo params, List* plantree_list)
{
    if (IsInitdb == true) {
        return NONE_FUSION;
    }

    if (ENABLE_GPC) {
        return NONE_FUSION;
    }

    if (!u_sess->attr.attr_sql.enable_opfusion) {
        return NONE_FUSION;
    }

    List* plist = NULL;
    if (plan && plantree_list == NULL) {
        plist = plan->stmt_list;
    } else if (plantree_list && plan == NULL) {
        plist = plantree_list;
    } else {
        Assert(0);
    }

    /* check stmt num */
    if (list_length(plist) != 1) {
        return NOBYPASS_NO_SIMPLE_PLAN;
    }

    /* check whether is planedstmt */
    Node* st = (Node*)linitial(plist);
    if (!IsA(st, PlannedStmt)) {
        /* may be ddl */
        return NONE_FUSION;
    }

    PlannedStmt* planned_stmt = (PlannedStmt*)st;
    if (planned_stmt->utilityStmt != NULL) {
        /* may be utility functions */
        return NONE_FUSION;
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

        if (((PlannedStmt*)st)->commandType == CMD_SELECT) {
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

void OpFusion::setCurrentOpFusionObj(OpFusion* obj)
{
    u_sess->exec_cxt.CurrentOpFusionObj = obj;
}

void OpFusion::checkPermission()
{
    bool check = false;
    if (!(IS_PGXC_DATANODE && (IsConnFromCoord() || IsConnFromDatanode()))) {
        check = true;
    }
    if (m_planstmt->in_compute_pool) {
        check = false;
    }

    if (t_thrd.pgxc_cxt.is_gc_fdw && t_thrd.pgxc_cxt.is_gc_fdw_analyze) {
        check = false;
    }

    if (check) {
        (void)ExecCheckRTPerms(m_planstmt->rtable, true);
    }
}

void OpFusion::executeInit()
{
    if (m_isFirst == true) {
        checkPermission();
    }

    if (IS_SINGLE_NODE && ENABLE_WORKLOAD_CONTROL) {
        u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
        WLMCreateDNodeInfoOnDN(NULL);
        WLMCreateIOInfoOnDN();
    }

    if (u_sess->attr.attr_common.XactReadOnly) {
        ExecCheckXactReadOnly(m_planstmt);
    }

#ifdef ENABLE_MOT
    if (!(u_sess->exec_cxt.CurrentOpFusionObj->m_cacheplan &&
            u_sess->exec_cxt.CurrentOpFusionObj->m_cacheplan->storageEngineType == SE_TYPE_MOT)) {
#endif
        if (m_snapshot == NULL) {
            m_snapshot = RegisterSnapshot(GetTransactionSnapshot());
        }
        PushActiveSnapshot(m_snapshot);
#ifdef ENABLE_MOT
    }
#endif
}

void OpFusion::auditRecord()
{
    if ((u_sess->attr.attr_security.Audit_DML_SELECT != 0 || u_sess->attr.attr_security.Audit_DML != 0) &&
        u_sess->attr.attr_security.Audit_enabled &&
        IsPostmasterEnvironment) {
        char* object_name = NULL;

        switch (m_planstmt->commandType) {
            case CMD_INSERT:
            case CMD_DELETE:
            case CMD_UPDATE:
                if (u_sess->attr.attr_security.Audit_DML != 0) {
                    object_name = pgaudit_get_relation_name(m_planstmt->rtable);
                    pgaudit_dml_table(object_name, m_is_pbe_query ? m_psrc->query_string : t_thrd.postgres_cxt.debug_query_string);
                }
                break;
            case CMD_SELECT:
                if (u_sess->attr.attr_security.Audit_DML_SELECT != 0) {
                    object_name = pgaudit_get_relation_name(m_planstmt->rtable);
                    pgaudit_dml_table_select(object_name, m_is_pbe_query ? m_psrc->query_string : t_thrd.postgres_cxt.debug_query_string);
                }
                break;
            /* Not support others */
            default:
                break;
        }
    }
}

void OpFusion::executeEnd(const char* portal_name, bool* isQueryCompleted)
{
#ifdef ENABLE_MOT
    if (!(u_sess->exec_cxt.CurrentOpFusionObj->m_cacheplan &&
          u_sess->exec_cxt.CurrentOpFusionObj->m_cacheplan->storageEngineType == SE_TYPE_MOT)) {
#endif
        opfusion_executeEnd(m_planstmt, ((m_psrc == NULL)?(NULL):(m_psrc->query_string)), GetActiveSnapshot());

        PopActiveSnapshot();
#ifdef ENABLE_MOT
    }
#endif

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check all memory contexts when executor starts */
    MemoryContextCheck(TopMemoryContext, false);

    /* Check per-query memory context before Opfusion temp memory */
    MemoryContextCheck(m_tmpContext, true);
#endif

    /* reset the context. */
    if (m_isCompleted) {
        UnregisterSnapshot(m_snapshot);
        m_snapshot = NULL;
        MemoryContextDeleteChildren(m_tmpContext);
        /* reset the context. */
        MemoryContextReset(m_tmpContext);
        /* clear hash table */
        removeFusionFromHtab(portal_name);
        m_outParams = NULL;
        m_isCompleted = false;
        if (isQueryCompleted)
            *isQueryCompleted = true;
        u_sess->xact_cxt.pbe_execute_complete = true;
    } else {
        if (isQueryCompleted)
            *isQueryCompleted = false;
        u_sess->xact_cxt.pbe_execute_complete = false;
    }
    u_sess->exec_cxt.CurrentOpFusionObj = NULL;

    m_isFirst = false;
    auditRecord();
    if (u_sess->attr.attr_common.pgstat_track_activities && u_sess->attr.attr_common.pgstat_track_sql_count) {
        report_qps_type(m_planstmt->commandType);
        report_qps_type(CMD_DML);
    }
}

bool OpFusion::process(int op, StringInfo msg, char* completionTag, bool isTopLevel, bool* isQueryCompleted)
{
    if (op == FUSION_EXECUTE && msg != NULL)
        refreshCurFusion(msg);

    bool res = false;
    if (u_sess->exec_cxt.CurrentOpFusionObj != NULL) {
        switch (op) {
            case FUSION_EXECUTE: {
                long max_rows = FETCH_ALL;
                const char* portal_name = NULL;
                /* msg is null means 'U' message, need fetch all. */
                if (msg != NULL) {
                    portal_name = pq_getmsgstring(msg);
                    max_rows = (long)pq_getmsgint(msg, 4);
                    if (max_rows <= 0)
                        max_rows = FETCH_ALL;
                }

                u_sess->exec_cxt.CurrentOpFusionObj->executeInit();
                gstrace_entry(GS_TRC_ID_BypassExecutor);
                bool old_status = u_sess->exec_cxt.need_track_resource;
                if (u_sess->attr.attr_resource.resource_track_cost == 0 &&
                    u_sess->attr.attr_resource.enable_resource_track &&
                    u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_NONE) {
                    u_sess->exec_cxt.need_track_resource = true;
                    WLMSetCollectInfoStatus(WLM_STATUS_RUNNING);
                }
                u_sess->exec_cxt.CurrentOpFusionObj->execute(max_rows, completionTag);
                u_sess->exec_cxt.need_track_resource = old_status;
                gstrace_exit(GS_TRC_ID_BypassExecutor);
                u_sess->exec_cxt.CurrentOpFusionObj->executeEnd(portal_name, isQueryCompleted);
                UpdateSingleNodeByPassUniqueSQLStat(isTopLevel);
                break;
            }

            case FUSION_DESCRIB: {
                u_sess->exec_cxt.CurrentOpFusionObj->describe(msg);
                break;
            }

            default: {
                Assert(0);
                ereport(ERROR,
                    (errcode(ERRCODE_CASE_NOT_FOUND),
                        errmsg("unrecognized bypass support process option: %d", (int)op)));
            }
        }
        res = true;
    }

   /*
    * Emit duration logging if appropriate.
    */

    char msec_str[32];
    switch (check_log_duration(msec_str, false)) {
        case 1:
            ereport(DEBUG1, (errmsg("duration: %s ms, queryid %lu, unique id %lu", msec_str, u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id), errhidestmt(true)));
            break;
        case 2: {
            ereport(DEBUG1,
                (errmsg("duration: %s ms queryid %lu unique id %lu", msec_str,  u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id),
                    errhidestmt(true)));
            break;
        }
        default:
            break;
    }

    return res;
}

void OpFusion::CopyFormats(int16* formats, int numRFormats)
{
    MemoryContext old_context = MemoryContextSwitchTo(m_tmpContext);
    int natts;
    if (m_tupDesc == NULL) {
        Assert(0);
        return;
    }
    if (formats == NULL && numRFormats > 0) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("r_formats can not be NULL when num of rformats is not 0")));
    }
    natts = m_tupDesc->natts;
    m_rformats = (int16*)palloc(natts * sizeof(int16));
    if (numRFormats > 1) {
        /* format specified for each column */
        if (numRFormats != natts)
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("bind message has %d result formats but query has %d columns", numRFormats, natts)));
        errno_t errorno = EOK;
        errorno = memcpy_s(m_rformats, natts * sizeof(int16), formats, natts * sizeof(int16));
        securec_check(errorno, "\0", "\0");
    } else if (numRFormats > 0) {
        /* single format specified, use for all columns */
        int16 format1 = formats[0];
        for (int i = 0; i < natts; i++) {
            m_rformats[i] = format1;
        }
    } else {
        /* use default format for all columns */
        for (int i = 0; i < natts; i++) {
            m_rformats[i] = 0;
        }
    }
    MemoryContextSwitchTo(old_context);
}

void OpFusion::useOuterParameter(ParamListInfo params)
{
    m_outParams = params;
}


void* OpFusion::FusionFactory(
    FusionType ftype, MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
{
    Assert(ftype != BYPASS_OK);
    if (u_sess->attr.attr_sql.opfusion_debug_mode == BYPASS_LOG) {
        BypassUnsupportedReason(ftype);
    }
    if (ftype > BYPASS_OK) {
        return NULL;
    }

    switch (ftype) {
        case SELECT_FUSION:
            return New(context) SelectFusion(context, psrc, plantree_list, params);

        case INSERT_FUSION:
            return New(context) InsertFusion(context, psrc, plantree_list, params);

        case UPDATE_FUSION:
            return New(context) UpdateFusion(context, psrc, plantree_list, params);

        case DELETE_FUSION:
            return New(context) DeleteFusion(context, psrc, plantree_list, params);

        case SELECT_FOR_UPDATE_FUSION:
            return New(context) SelectForUpdateFusion(context, psrc, plantree_list, params);

#ifdef ENABLE_MOT
        case MOT_JIT_SELECT_FUSION:
            return New(context) MotJitSelectFusion(context, psrc, plantree_list, params);

        case MOT_JIT_MODIFY_FUSION:
            return New(context) MotJitModifyFusion(context, psrc, plantree_list, params);
#endif

        case AGG_INDEX_FUSION:
            return New(context) AggFusion(context, psrc, plantree_list, params);

        case SORT_INDEX_FUSION:
            return New(context) SortFusion(context, psrc, plantree_list, params);

        case NONE_FUSION:
            return NULL;

        default:
            return NULL;
    }

    return NULL;
}

void OpFusion::updatePreAllocParamter(StringInfo input_message)
{
    /* Switch back to message context */
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    int num_pformats;
    int16* pformats = NULL;
    int num_params;
    int num_rformats;
    int paramno;
    ParamListInfo params = m_params;
    /* Get the parameter format codes */
    num_pformats = pq_getmsgint(input_message, 2);
    if (num_pformats > 0) {
        int i;

        pformats = (int16*)palloc(num_pformats * sizeof(int16));
        for (i = 0; i < num_pformats; i++) {
            pformats[i] = pq_getmsgint(input_message, 2);
        }
    }

    /* Get the parameter value count */
    num_params = pq_getmsgint(input_message, 2);
    if (unlikely(num_params != m_paramNum)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("unmatched parameter number")));
    }
    MemoryContextSwitchTo(m_tmpContext);
    if (num_params > 0) {
        for (paramno = 0; paramno < num_params; paramno++) {
            Oid ptype = m_psrc->param_types[paramno];
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
                const char* pvalue = pq_getmsgbytes(input_message, plength);

                /*
                 * Rather than copying data around, we just set up a phony
                 * StringInfo pointing to the correct portion of the message
                 * buffer.    We assume we can scribble on the message buffer so
                 * as to maintain the convention that StringInfos have a
                 * trailing null.  This is grotty but is a big win when
                 * dealing with very large parameter strings.
                 */
                pbuf.data = (char*)pvalue;
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
                char* pstring = NULL;

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
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                            errmsg("incorrect binary data format in bind parameter %d", paramno + 1)));
                }
            } else {
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported format code: %d", pformat)));
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

    int16* rformats = NULL;
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    /* Get the result format codes */
    num_rformats = pq_getmsgint(input_message, 2);
    if (num_rformats > 0) {
        int i;
        rformats = (int16*)palloc(num_rformats * sizeof(int16));
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
    if (m_psrc->resultDesc != NULL) {
        StringInfoData buf;
        initStringInfo(&buf);
        SendRowDescriptionMessage(&buf, m_tupDesc, m_planstmt->planTree->targetlist, m_rformats);
        pfree_ext(buf.data);
    } else {
        pq_putemptymessage('n');
    }
}

void OpFusion::setPreparedDestReceiver(DestReceiver* preparedDest)
{
    m_receiver = preparedDest;
    m_isInsideRec = false;
}

/* evaluate datum from node, simple node types such as Const, Param, Var excludes FuncExpr and OpExpr */
Datum OpFusion::EvalSimpleArg(Node* arg, bool* is_null, Datum* values, bool* isNulls)
{
    switch (nodeTag(arg)) {
        case T_Const:
            *is_null = ((Const*)arg)->constisnull;
            return ((Const*)arg)->constvalue;
        case T_Var:
            *is_null = isNulls[(((Var*)arg)->varattno - 1)];
            return values[(((Var*)arg)->varattno - 1)];
        case T_Param: {
            ParamListInfo param_list = m_outParams != NULL ? m_outParams : m_params;

            if (param_list->params[(((Param*)arg)->paramid - 1)].isnull) {
                *is_null = true;
            }
            return param_list->params[(((Param*)arg)->paramid - 1)].value;
        }
        case T_RelabelType: {
            if (IsA(((RelabelType*)arg)->arg, FuncExpr)) {
                FuncExpr* expr = (FuncExpr*)(((RelabelType*)arg)->arg);
                return CalFuncNodeVal(expr->funcid, expr->args, is_null, values, isNulls);
            } else if (IsA(((RelabelType*)arg)->arg, OpExpr)) {
                OpExpr* expr = (OpExpr*)(((RelabelType*)arg)->arg);
                return CalFuncNodeVal(expr->opfuncid, expr->args, is_null, values, isNulls);
            }

            return EvalSimpleArg((Node*)((RelabelType*)arg)->arg, is_null, values, isNulls);
        }
        default:
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unexpected node type: %d when processing bypass expression.", (int)nodeTag(arg))));
            break;
    }

    Assert(0);
    return 0;
}

Datum OpFusion::CalFuncNodeVal(Oid functionId, List* args, bool* is_null, Datum* values, bool* isNulls)
{
    if (*is_null) {
        return 0;
    }

    Node* first_arg_node = (Node*)linitial(args);
    Datum* arg;
    bool* is_nulls = (bool*)palloc0(4 * sizeof(bool));
    arg = (Datum*)palloc0(4 * sizeof(Datum));

    /* for now, we assuming FuncExpr and OpExpr only appear in the first arg */
    if (IsA(first_arg_node, FuncExpr)) {
        arg[0] = CalFuncNodeVal(((FuncExpr*)first_arg_node)->funcid, ((FuncExpr*)first_arg_node)->args,
                                &is_nulls[0], values, isNulls);
    } else if (IsA(first_arg_node, OpExpr)) {
        arg[0] = CalFuncNodeVal(((OpExpr*)first_arg_node)->opfuncid, ((OpExpr*)first_arg_node)->args,
                                &is_nulls[0], values, isNulls);
    } else {
        arg[0] = EvalSimpleArg(first_arg_node, &is_nulls[0], values, isNulls);
    }

    int length = list_length(args);
    if (length < 1 || length > 4) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected arg length : %d when processing bypass expression.", length)));
    }
    *is_null = is_nulls[0];
    ListCell* tmp_arg = list_head(args);
    for (int i = 1; i < length; i++) {
        tmp_arg = lnext(tmp_arg);
        arg[i] = EvalSimpleArg((Node*)lfirst(tmp_arg), &is_nulls[i], values, isNulls);
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
            ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                    errmsg("unrecognized bypass support number of arguments function calls: %d", (int)length)));
        }
    }

    Assert(0);
    return 0;
}

void OpFusion::tearDown(OpFusion* opfusion)
{
    if (opfusion == NULL) {
        return;
    }

    removeFusionFromHtab(opfusion->m_portalName);

    if (opfusion->m_cacheplan != NULL) {
        ReleaseCachedPlan(opfusion->m_cacheplan, false);
    }

    if (opfusion->m_psrc != NULL) {
        opfusion->m_psrc->is_checked_opfusion = false;
        opfusion->m_psrc->opFusionObj = NULL;
    }

    MemoryContextDelete(opfusion->m_tmpContext);
    MemoryContextDelete(opfusion->m_context);

    delete opfusion;

    OpFusion::setCurrentOpFusionObj(NULL);
}

void OpFusion::clearForCplan(OpFusion* opfusion, CachedPlanSource* psrc)
{
    if (opfusion == NULL)
        return;
    if (psrc->cplan != NULL) {
        tearDown(opfusion);
    }
}

/* just for select and select for */
void OpFusion::setReceiver()
{
    if (m_isInsideRec) {
        m_receiver = CreateDestReceiver((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
    }

    ((DR_printtup*)m_receiver)->sendDescrip = false;
    if (m_receiver->mydest == DestRemote || m_receiver->mydest == DestRemoteExecute) {
        ((DR_printtup*)m_receiver)->formats = m_rformats;
    }
    (*m_receiver->rStartup)(m_receiver, CMD_SELECT, m_tupDesc);
    if (!m_is_pbe_query) {
        StringInfoData buf = ((DR_printtup*)m_receiver)->buf;
        initStringInfo(&buf);
        SendRowDescriptionMessage(&buf, m_tupDesc, m_planstmt->planTree->targetlist, m_rformats);
    }
}

void OpFusion::initParams(ParamListInfo params)
{
    m_outParams = params;

    /* init params */
    if (m_is_pbe_query && params != NULL) {
        m_paramNum = params->numParams;
        m_params = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + m_paramNum * sizeof(ParamExternData));
        m_params->paramFetch = NULL;
        m_params->paramFetchArg = NULL;
        m_params->parserSetup = NULL;
        m_params->parserSetupArg = NULL;
        m_params->params_need_process = false;
        m_params->numParams = m_paramNum;
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
    m_isCompleted = false;
    m_position = 0;
    m_outParams = NULL;
    m_snapshot = NULL;

    MemoryContextDeleteChildren(m_tmpContext);
    /* reset the context. */
    MemoryContextReset(m_tmpContext);
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
    u_sess->pcache_cxt.pn_fusion_htab = hash_create("OpFusion_hashtable",
        HASH_TBL_LEN, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
}

void OpFusion::ClearInUnexpectSituation()
{
    if (u_sess->pcache_cxt.pn_fusion_htab != NULL) {
        HASH_SEQ_STATUS seq;
        pnFusionObj *entry = NULL;
        hash_seq_init(&seq, u_sess->pcache_cxt.pn_fusion_htab);
        while ((entry = (pnFusionObj *)hash_seq_search(&seq)) != NULL) {
            OpFusion* curr = entry->opfusion;
            if (curr->m_portalName == NULL || strcmp(curr->m_portalName, entry->portalname) == 0) {
                    curr->clean();
                    /* only opfusion has reference on cachedplan, no need any more */
                    if (curr->m_cacheplan && curr->m_cacheplan->refcount == 1) {
                        ReleaseCachedPlan(curr->m_cacheplan, false);
                        MemoryContextDelete(curr->m_tmpContext);
                        MemoryContextDelete(curr->m_context);
                        delete curr;
                    }
            }
            hash_search(u_sess->pcache_cxt.pn_fusion_htab, entry->portalname, HASH_REMOVE, NULL);
        }
    }
    u_sess->exec_cxt.CurrentOpFusionObj = NULL;
}

void OpFusion::clean()
{
    UnregisterSnapshot(m_snapshot);
    m_snapshot = NULL;
    m_position = 0;
    m_outParams = NULL;
    if (m_scan)
        m_scan->End(true);
    m_isCompleted = false;
    MemoryContextDeleteChildren(m_tmpContext);
    /* reset the context. */
    MemoryContextReset(m_tmpContext);
}

void OpFusion::storeFusion(const char *portalname)
{
    pnFusionObj *entry = NULL;
    if (!u_sess->pcache_cxt.pn_fusion_htab)
        initFusionHtab();

    removeFusionFromHtab(m_portalName);
    entry = (pnFusionObj *)(hash_search(u_sess->pcache_cxt.pn_fusion_htab, portalname, HASH_ENTER, NULL));
    entry->opfusion = this;

    MemoryContext old_cxt = MemoryContextSwitchTo(m_context);
    pfree_ext(m_portalName);
    m_portalName = pstrdup(portalname);
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

/* judge plan node is partiterator */
Node* JudgePlanIsPartIterator(Plan* plan)
{
    Node* node = NULL;
    if (IsA(plan, PartIterator)) {
        node = (Node*)plan->lefttree;
    } else {
        node = (Node*)plan;
    }
    return node;
}

SelectFusion::SelectFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_tmpvals = NULL;
    m_values = NULL;
    m_isnull = NULL;
    m_paramLoc = NULL;
    m_tmpisnull = NULL;
    m_attrno = NULL;
    m_reloid = 0;

    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    Node* node = NULL;

    m_limitCount = -1;
    m_limitOffset = -1;

    /* get limit num */
    if (IsA(m_planstmt->planTree, Limit)) {
        Limit* limit = (Limit*)m_planstmt->planTree;
        node = JudgePlanIsPartIterator(m_planstmt->planTree->lefttree);
        if (limit->limitCount != NULL && IsA(limit->limitCount, Const) && !((Const*)limit->limitCount)->constisnull) {
            m_limitCount = DatumGetInt64(((Const*)limit->limitCount)->constvalue);
        }
        if (limit->limitOffset != NULL && IsA(limit->limitOffset, Const) &&
            !((Const*)limit->limitOffset)->constisnull) {
            m_limitOffset = DatumGetInt64(((Const*)limit->limitOffset)->constvalue);
        }
    } else {
        node = JudgePlanIsPartIterator(m_planstmt->planTree);
    }

    initParams(params);
    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion(node, m_planstmt, m_outParams);
    m_tupDesc = m_scan->m_tupDesc;
    m_reslot = NULL;
    MemoryContextSwitchTo(old_context);
}

bool SelectFusion::execute(long max_rows, char* completionTag)
{
    MemoryContext oldContext = MemoryContextSwitchTo(m_tmpContext);

    bool success = false;
    int64 start_row = 0;
    int64 get_rows = 0;

    /*******************
     * step 1: prepare *
     *******************/
    start_row = m_limitOffset >= 0 ? m_limitOffset : start_row;
    get_rows = m_limitCount >= 0 ? (m_limitCount + start_row) : max_rows;

    /**********************
     * step 2: begin scan *
     **********************/
    if (m_position == 0) {
        m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);
        m_scan->Init(max_rows);
    }
    setReceiver();

    unsigned long nprocessed = 0;
    /* put selected tuple into receiver */
    TupleTableSlot* offset_reslot = NULL;
    while (nprocessed < (unsigned long)start_row && (offset_reslot = m_scan->getTupleSlot()) != NULL) {
        tpslot_free_heaptuple(offset_reslot);
        nprocessed++;
    }
    while (nprocessed < (unsigned long)get_rows && (m_reslot = m_scan->getTupleSlot()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        nprocessed++;
        (*m_receiver->receiveSlot)(m_reslot, m_receiver);
        tpslot_free_heaptuple(m_reslot);
    }
    if (!ScanDirectionIsNoMovement(*(m_scan->m_direction))) {
        if (max_rows == 0 || nprocessed < (unsigned long)max_rows) {
            m_isCompleted = true;
        }
        m_position += nprocessed;
    } else {
        m_isCompleted = true;
    }
    success = true;

    /****************
     * step 3: done *
     ****************/
    if (m_isInsideRec) {
        (*m_receiver->rDestroy)(m_receiver);
    }
    m_scan->End(m_isCompleted);
    if (m_isCompleted) {
        m_position = 0;
    }
    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");
    MemoryContextSwitchTo(oldContext);

    return success;
}

void SelectFusion::close()
{
    if (m_isCompleted == false) {
        m_scan->End(true);
        m_isCompleted = true;
        m_position = 0;
    }
}

#ifdef ENABLE_MOT
MotJitSelectFusion::MotJitSelectFusion(
    MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext oldContext = MemoryContextSwitchTo(m_context);
    Node* node = NULL;
    node = (Node*)m_planstmt->planTree;
    initParams(params);
    m_receiver = NULL;
    m_isInsideRec = true;

    if (m_planstmt->planTree->targetlist) {
        m_tupDesc = ExecCleanTypeFromTL(m_planstmt->planTree->targetlist, false);
    } else {
        ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                 errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                 errmsg("unrecognized node type: %d when executing executor node.", (int)nodeTag(node))));
    }
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    MemoryContextSwitchTo(oldContext);
}

bool MotJitSelectFusion::execute(long max_rows, char* completionTag)
{
    ParamListInfo params = (m_outParams != NULL) ? m_outParams : m_params;
    bool success = false;
    setReceiver();
    unsigned long nprocessed = 0;
    bool finish = false;
    int rc = 0;
    while (!finish) {
        uint64_t tpProcessed = 0;
        int scanEnded = 0;
        rc = JitExec::JitExecQuery(m_cacheplan->mot_jit_context, params, m_reslot, &tpProcessed, &scanEnded);
        if (scanEnded || (tpProcessed == 0) || (rc != 0)) {
            // raise flag so that next round we will bail out (current tuple still must be reported to user)
            finish = true;
        }
        CHECK_FOR_INTERRUPTS();
        if (tpProcessed > 0) {
            nprocessed++;
            (*m_receiver->receiveSlot)(m_reslot, m_receiver);
            (void)ExecClearTuple(m_reslot);
            if ((max_rows != FETCH_ALL) && (nprocessed == (unsigned long)max_rows)) {
                finish = true;
            }
        }
    }

    success = true;

    if (m_isInsideRec) {
        (*m_receiver->rDestroy)(m_receiver);
    }

    m_position = 0;
    m_isCompleted = true;

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                                 "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}
#endif

/* init partition by the ScanFusion */
void InitPartitionByScanFusion(Relation rel, Relation* fakRel, Partition* part, EState* estate,
                               const ScanFusion* scan)
{
    if (RELATION_IS_PARTITIONED(rel)) {
        estate->esfRelations = NULL;
        *fakRel = scan->m_rel;
        *part = scan->m_partRel;
    }
}
/* init bucket relation */
Relation InitBucketRelation(int2 bucketid, Relation rel, Partition part)
{
    Relation bucketRel = NULL;
    if (bucketid != InvalidBktId) {
        bucketRel = bucketGetRelation(rel, part, bucketid);
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invaild Oid when open hash bucket relation.")));
    }
    return bucketRel;
}

/* execute the process of done in the Fusion */
void ExecDoneStepInFusion(Relation bucketRel, EState* estate)
{
    if (estate->esfRelations) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }

    if (bucketRel != NULL) {
        bucketCloseRelation(bucketRel);
    }
}

InsertFusion::InsertFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_tmpisnull = NULL;
    m_tmpvals = NULL;
    m_attrno = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    ModifyTable* node = (ModifyTable*)m_planstmt->planTree;

    m_reloid = getrelid(linitial_int(m_planstmt->resultRelations), m_planstmt->rtable);
    Relation rel = heap_open(m_reloid, AccessShareLock);

    m_estate = CreateExecutorState();
    m_estate->es_range_table = m_planstmt->rtable;

    BaseResult* baseresult = (BaseResult*)linitial(node->plans);
    List* targetList = baseresult->plan.targetlist;
    m_tupDesc = ExecTypeFromTL(targetList, false, false, rel->rd_tam_type);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_values = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_isnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_curVarValue = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_curVarIsnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_is_bucket_rel = RELATION_OWN_BUCKET(rel);

    heap_close(rel, AccessShareLock);

    /* init param */
    m_paramNum = 0;
    m_paramLoc = (ParamLoc*)palloc0(RelationGetDescr(rel)->natts * sizeof(ParamLoc));
    m_targetParamNum = 0;

    m_targetFuncNum = 0;
    m_targetFuncNodes = (FuncExprInfo*)palloc0(RelationGetDescr(rel)->natts * sizeof(FuncExprInfo));

    ListCell* lc = NULL;
    int i = 0;
    FuncExpr* func = NULL;
    TargetEntry* res = NULL;
    Expr* expr = NULL;
    OpExpr* opexpr = NULL;
    foreach (lc, targetList) {
        res = (TargetEntry*)lfirst(lc);
        expr = res->expr;

        Assert(
            IsA(expr, Const) || IsA(expr, Param) || IsA(expr, FuncExpr) || IsA(expr, RelabelType) || IsA(expr, OpExpr));
        while (IsA(expr, RelabelType)) {
            expr = ((RelabelType*)expr)->arg;
        }

        if (IsA(expr, FuncExpr)) {
            func = (FuncExpr*)expr;

            m_targetFuncNodes[m_targetFuncNum].resno = res->resno;
            m_targetFuncNodes[m_targetFuncNum].resname = res->resname;
            m_targetFuncNodes[m_targetFuncNum].funcid = func->funcid;
            m_targetFuncNodes[m_targetFuncNum].args = func->args;
            ++m_targetFuncNum;
        } else if (IsA(expr, Param)) {
            Param* param = (Param*)expr;
            m_paramLoc[m_targetParamNum].paramId = param->paramid;
            m_paramLoc[m_targetParamNum++].scanKeyIndx = i;
        } else if (IsA(expr, Const)) {
            Assert(IsA(expr, Const));
            m_isnull[i] = ((Const*)expr)->constisnull;
            m_values[i] = ((Const*)expr)->constvalue;
        } else if (IsA(expr, OpExpr)) {
            opexpr = (OpExpr*)expr;

            m_targetFuncNodes[m_targetFuncNum].resno = res->resno;
            m_targetFuncNodes[m_targetFuncNum].resname = res->resname;
            m_targetFuncNodes[m_targetFuncNum].funcid = opexpr->opfuncid;
            m_targetFuncNodes[m_targetFuncNum].args = opexpr->args;
            ++m_targetFuncNum;
        }

        i++;
    }

    initParams(params);

    m_receiver = NULL;
    m_isInsideRec = true;

    MemoryContextSwitchTo(old_context);
}

void InsertFusion::refreshParameterIfNecessary()
{
    ParamListInfo parms = m_outParams != NULL ? m_outParams : m_params;
    bool func_isnull = false;
    /* save cur var value */
    for (int i = 0; i < m_tupDesc->natts; i++) {
        m_curVarValue[i] = m_values[i];
        m_curVarIsnull[i] = m_isnull[i];
    }

    /* calculate func result */
    for (int i = 0; i < m_targetFuncNum; ++i) {
        ELOG_FIELD_NAME_START(m_targetFuncNodes[i].resname);
        if (m_targetFuncNodes[i].funcid != InvalidOid) {
            func_isnull = false;
            m_values[m_targetFuncNodes[i].resno - 1] = CalFuncNodeVal(
                m_targetFuncNodes[i].funcid, m_targetFuncNodes[i].args, &func_isnull, m_curVarValue, m_curVarIsnull);
            m_isnull[m_targetFuncNodes[i].resno - 1] = func_isnull;
        }
        ELOG_FIELD_NAME_END;
    }
    /* mapping params */
    if (m_targetParamNum > 0) {
        for (int i = 0; i < m_targetParamNum; i++) {
            m_values[m_paramLoc[i].scanKeyIndx] = parms->params[m_paramLoc[i].paramId - 1].value;
            m_isnull[m_paramLoc[i].scanKeyIndx] = parms->params[m_paramLoc[i].paramId - 1].isnull;
        }
    }
}

bool InsertFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    Oid partOid = InvalidOid;
    Partition part = NULL;
    Relation partRel = NULL;

    /*******************
     * step 1: prepare *
     *******************/
    Relation rel = heap_open(m_reloid, RowExclusiveLock);
    Relation bucket_rel = NULL;
    int2 bucketid = InvalidBktId;

    ResultRelInfo* result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_estate->es_result_relation_info = result_rel_info;


    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    CommandId mycid = GetCurrentCommandId(true);

    refreshParameterIfNecessary();
    init_gtt_storage(CMD_INSERT, result_rel_info);
    /************************
     * step 2: begin insert *
     ************************/
    HeapTuple tuple = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, m_values, m_isnull, HEAP_TUPLE);
    Assert(tuple != NULL);
    if (RELATION_IS_PARTITIONED(rel)) {
        m_estate->esfRelations = NULL;
        partOid = heapTupleGetPartitionId(rel, tuple);
        part = partitionOpen(rel, partOid, RowExclusiveLock);
        partRel = partitionGetRelation(rel, part);
    }

    if (m_is_bucket_rel) {
        bucketid = computeTupleBucketId(result_rel_info->ri_RelationDesc, tuple);
        bucket_rel = InitBucketRelation(bucketid, rel, part);
    }

    (void)ExecStoreTuple(tuple, m_reslot, InvalidBuffer, false);

    if (rel->rd_att->constr) {
        ExecConstraints(result_rel_info, m_reslot, m_estate);
    }
    Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
    (void)tableam_tuple_insert(bucket_rel == NULL ? destRel : bucket_rel, tuple, mycid, 0, NULL);

    if (!RELATION_IS_PARTITIONED(rel)) {
        /* try to insert tuple into mlog-table. */
        if (rel != NULL && rel->rd_mlogoid != InvalidOid) {
            /* judge whether need to insert into mlog-table */
            insert_into_mlog_table(rel, rel->rd_mlogoid, tuple, &tuple->t_self,
                                   GetCurrentTransactionId(), 'I');
        }
    }

    /* insert index entries for tuple */
    List* recheck_indexes = NIL;
    if (result_rel_info->ri_NumIndices > 0) {
        recheck_indexes = ExecInsertIndexTuples(m_reslot,
                                                &(tuple->t_self),
                                                m_estate,
                                                RELATION_IS_PARTITIONED(rel) ? partRel : NULL,
                                                RELATION_IS_PARTITIONED(rel) ? part : NULL,
                                                bucketid, NULL);
    }
    list_free_ext(recheck_indexes);

    tableam_tops_free_tuple(tuple);

    (void)ExecClearTuple(m_reslot);
    success = true;
    m_isCompleted = true;
    /****************
     * step 3: done *
     ****************/
    ExecCloseIndices(result_rel_info);

    heap_close(rel, RowExclusiveLock);

    ExecDoneStepInFusion(bucket_rel, m_estate);

    if (RELATION_IS_PARTITIONED(rel)) {
        partitionClose(rel, part, RowExclusiveLock);
        releaseDummyRelation(&partRel);
    }

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "INSERT 0 1");
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

#ifdef ENABLE_MOT
MotJitModifyFusion::MotJitModifyFusion(
    MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext oldContext = MemoryContextSwitchTo(m_context);
    ModifyTable* node = (ModifyTable*)m_planstmt->planTree;
    m_cmdType = node->operation;

    m_reloid = getrelid(linitial_int(m_planstmt->resultRelations), m_planstmt->rtable);

    m_estate = CreateExecutorState();
    m_estate->es_range_table = m_planstmt->rtable;

    BaseResult* baseresult = (BaseResult*)linitial(node->plans);
    List* targetList = baseresult->plan.targetlist;
    m_tupDesc = ExecTypeFromTL(targetList, false);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);

    /* init param */
    m_paramNum = 0;
    initParams(params);
    m_receiver = NULL;
    m_isInsideRec = true;

    MemoryContextSwitchTo(oldContext);
}

bool MotJitModifyFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    uint64_t tpProcessed = 0;
    int scanEnded = 0;
    ParamListInfo params = (m_outParams != NULL) ? m_outParams : m_params;
    int rc = JitExec::JitExecQuery(m_cacheplan->mot_jit_context, params, m_reslot, &tpProcessed, &scanEnded);
    if (rc == 0) {
        (void)ExecClearTuple(m_reslot);
        success = true;
        errno_t ret = EOK;
        switch (m_cmdType)
        {
            case CMD_INSERT:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "INSERT 0 %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_UPDATE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "UPDATE %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_DELETE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "DELETE %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            default:
                break;
        }
    }

    m_isCompleted = true;

    return success;
}
#endif

HeapTuple UpdateFusion::heapModifyTuple(HeapTuple tuple)
{
    HeapTuple new_tuple;

    tableam_tops_deform_tuple(tuple, m_tupDesc, m_values, m_isnull);

    refreshTargetParameterIfNecessary();

    /*
     * create a new tuple from the values and isnull arrays
     */
    new_tuple = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, m_values, m_isnull, HEAP_TUPLE);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    new_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
    new_tuple->t_self = tuple->t_self;
    new_tuple->t_tableOid = tuple->t_tableOid;
    new_tuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(new_tuple, tuple);
#ifdef PGXC
    new_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif

    Assert(m_tupDesc->tdhasoid == false);

    return new_tuple;
}

UpdateFusion::UpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_attrno = NULL;
    m_paramLoc = NULL;
    m_tmpisnull = NULL;
    m_tmpvals = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    ModifyTable* node = (ModifyTable*)m_planstmt->planTree;
    Plan *updatePlan = (Plan *)linitial(node->plans);
    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(updatePlan);

    m_reloid = getrelid(linitial_int(m_planstmt->resultRelations), m_planstmt->rtable);
    Relation rel = heap_open(m_reloid, AccessShareLock);

    m_estate = CreateExecutorState();
    m_estate->es_range_table = m_planstmt->rtable;

    m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_values = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_isnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_is_bucket_rel = RELATION_OWN_BUCKET(rel);

    heap_close(rel, AccessShareLock);

    if (m_is_bucket_rel) {
        // ctid + tablebucketid
        Assert(RelationGetDescr(rel)->natts + 2 == list_length(indexscan->scan.plan.targetlist));
    } else {
        // ctid
        if (indexscan->scan.isPartTbl) {
            Assert(RelationGetDescr(rel)->natts + 2 == list_length(indexscan->scan.plan.targetlist));
        } else {
            Assert(RelationGetDescr(rel)->natts + 1 == list_length(indexscan->scan.plan.targetlist));
        }
    }
    m_outParams = params;

    /* init target, include param and const */
    m_targetParamNum = 0;
    m_targetValues = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_targetIsnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_targetParamLoc = (ParamLoc*)palloc0(RelationGetDescr(rel)->natts * sizeof(ParamLoc));
    m_targetConstLoc = (int*)palloc0(RelationGetDescr(rel)->natts * sizeof(int));
    m_curVarValue = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_curVarIsnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_targetVarLoc = (VarLoc*)palloc0(RelationGetDescr(rel)->natts * sizeof(VarLoc));

    m_targetFuncNum = 0;
    m_targetFuncNodes = (FuncExprInfo*)palloc0(RelationGetDescr(rel)->natts * sizeof(FuncExprInfo));
    m_varNum = 0;

    int i = 0;
    ListCell* lc = NULL;
    OpExpr* opexpr = NULL;
    FuncExpr* func = NULL;
    Expr* expr = NULL;
    foreach (lc, indexscan->scan.plan.targetlist) {
        // ignore ctid + tablebucketid or ctid at last
        if (i >= RelationGetDescr(rel)->natts) {
            break;
        }

        TargetEntry* res = (TargetEntry*)lfirst(lc);
        expr = res->expr;

        while (IsA(expr, RelabelType)) {
            expr = ((RelabelType*)expr)->arg;
        }

        m_targetConstLoc[i] = -1;
        if (IsA(expr, Param)) {
            Param* param = (Param*)expr;
            m_targetParamLoc[m_targetParamNum].paramId = param->paramid;
            m_targetParamLoc[m_targetParamNum++].scanKeyIndx = i;
        } else if (IsA(expr, Const)) {
            m_targetIsnull[i] = ((Const*)expr)->constisnull;
            m_targetValues[i] = ((Const*)expr)->constvalue;
            m_targetConstLoc[i] = i;
        } else if (IsA(expr, Var)) {
            Var* var = (Var*)expr;
            m_targetVarLoc[m_varNum].varNo = var->varattno;
            m_targetVarLoc[m_varNum++].scanKeyIndx = i;
        } else if (IsA(expr, OpExpr)) {
            opexpr = (OpExpr*)expr;

            m_targetFuncNodes[m_targetFuncNum].resno = res->resno;
            m_targetFuncNodes[m_targetFuncNum].resname = res->resname;
            m_targetFuncNodes[m_targetFuncNum].funcid = opexpr->opfuncid;
            m_targetFuncNodes[m_targetFuncNum].args = opexpr->args;
            ++m_targetFuncNum;
        } else if (IsA(expr, FuncExpr)) {
            func = (FuncExpr*)expr;

            m_targetFuncNodes[m_targetFuncNum].resno = res->resno;
            m_targetFuncNodes[m_targetFuncNum].resname = res->resname;
            m_targetFuncNodes[m_targetFuncNum].funcid = func->funcid;
            m_targetFuncNodes[m_targetFuncNum].args = func->args;
            ++m_targetFuncNum;
        }

        i++;
    }

    m_targetNum = i;

    initParams(params);

    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_planstmt, m_outParams);

    MemoryContextSwitchTo(old_context);
}

void UpdateFusion::refreshTargetParameterIfNecessary()
{
    ParamListInfo parms = m_outParams != NULL ? m_outParams : m_params;
    /* save cur var value */
    for (int i = 0; i < m_tupDesc->natts; i++) {
        m_curVarValue[i] = m_values[i];
        m_curVarIsnull[i] = m_isnull[i];
    }
    if (m_varNum > 0) {
        for (int i = 0; i < m_varNum; i++) {
            m_values[m_targetVarLoc[i].scanKeyIndx] = m_curVarValue[m_targetVarLoc[i].varNo - 1];
            m_isnull[m_targetVarLoc[i].scanKeyIndx] = m_curVarIsnull[m_targetVarLoc[i].varNo - 1];
        }
    }
    /* mapping value for update from target */
    if (m_targetParamNum > 0) {
        Assert(m_targetParamNum > 0);
        for (int i = 0; i < m_targetParamNum; i++) {
            m_values[m_targetParamLoc[i].scanKeyIndx] = parms->params[m_targetParamLoc[i].paramId - 1].value;
            m_isnull[m_targetParamLoc[i].scanKeyIndx] = parms->params[m_targetParamLoc[i].paramId - 1].isnull;
        }
    }

    for (int i = 0; i < m_targetNum; i++) {
        if (m_targetConstLoc[i] >= 0) {
            m_values[i] = m_targetValues[m_targetConstLoc[i]];
            m_isnull[i] = m_targetIsnull[m_targetConstLoc[i]];
        }
    }
    /* calculate func result */
    for (int i = 0; i < m_targetFuncNum; ++i) {
        ELOG_FIELD_NAME_START(m_targetFuncNodes[i].resname);
        if (m_targetFuncNodes[i].funcid != InvalidOid) {
            bool func_isnull = false;
            m_values[m_targetFuncNodes[i].resno - 1] =
                CalFuncNodeVal(m_targetFuncNodes[i].funcid, m_targetFuncNodes[i].args, &func_isnull,
                               m_curVarValue, m_curVarIsnull);
            m_isnull[m_targetFuncNodes[i].resno - 1] = func_isnull;
        }
        ELOG_FIELD_NAME_END;
    }
}

bool UpdateFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    bool execMatview = false;

    /*******************
     * step 1: prepare *
     *******************/
    m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);

    m_scan->Init(max_rows);

    Relation rel = ((m_scan->m_parentRel) == NULL ? m_scan->m_rel : m_scan->m_parentRel);
    Relation partRel = NULL;
    Partition part = NULL;
    Relation bucket_rel = NULL;
    int2 bucketid = InvalidBktId;

    ResultRelInfo* result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_estate->es_result_relation_info = result_rel_info;
    m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    InitPartitionByScanFusion(rel, &partRel, &part, m_estate, m_scan);

    /*********************************
     * step 2: begin scan and update *
     *********************************/
    HeapTuple oldtup = NULL;
    HeapTuple tup = NULL;
    unsigned long nprocessed = 0;
    List* recheck_indexes = NIL;
    m_tupDesc = RelationGetDescr(rel);

    while ((oldtup = m_scan->getTuple()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        TM_Result result;
        TM_FailureData tmfd;

    lreplace:
        Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
        Relation parentRel = RELATION_IS_PARTITIONED(rel) ? rel : NULL;
        tup = heapModifyTuple(oldtup);

        m_scan->UpdateCurrentRel(&rel);
        if (m_is_bucket_rel) {
            Assert(tup->t_bucketId == computeTupleBucketId(result_rel_info->ri_RelationDesc, tup));
            bucketid = tup->t_bucketId;
            bucket_rel = InitBucketRelation(bucketid, rel, part);
        }

        (void)ExecStoreTuple(tup, m_reslot, InvalidBuffer, false);
        if (rel->rd_att->constr)
            ExecConstraints(result_rel_info, m_reslot, m_estate);

        bool update_indexes = false;
        result = tableam_tuple_update(bucket_rel == NULL ? destRel : bucket_rel,
                                      bucket_rel == NULL ? parentRel : rel,
                                      &oldtup->t_self,
                                      tup,
                                      m_estate->es_output_cid,
                                      InvalidSnapshot,
                                      m_estate->es_snapshot,
                                      true,
                                      &tmfd,
                                      &update_indexes,
                                      false);

        switch (result) {
            case TM_SelfModified:
                if (tmfd.cmax != m_estate->es_output_cid)
                    ereport(ERROR,
                            (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                             errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                             errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                /* already deleted by self; nothing to do */
                break;

            case TM_Ok:
                if (!RELATION_IS_PARTITIONED(rel)) {
                    /* handle with matview. */
                    execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                    if (execMatview) {
                        /* judge whether need to insert into mlog-table */
                        /* 1. delete one tuple. */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL, &oldtup->t_self,
                                               tmfd.xmin, 'D');
                        /* 2. insert new tuple */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, tup, &(tup->t_self),
                                               GetCurrentTransactionId(), 'I');
                    }
                    /* done successfully */
                }
                nprocessed++;
                if (result_rel_info->ri_NumIndices > 0 && update_indexes) {
                    recheck_indexes = ExecInsertIndexTuples(m_reslot,
                                                            &(tup->t_self),
                                                            m_estate,
                                                            RELATION_IS_PARTITIONED(rel) ? partRel : NULL,
                                                            RELATION_IS_PARTITIONED(rel) ? part : NULL,
                                                            bucketid, NULL);
                    list_free_ext(recheck_indexes);
                }
                break;

            case TM_Updated: {
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert(!ItemPointerEquals(&tup->t_self, &tmfd.ctid));

                bool* isnullfornew = NULL;
                Datum* valuesfornew = NULL;
                HeapTuple copyTuple;
                copyTuple = heap_lock_updated(m_estate->es_output_cid,
                                              bucket_rel == NULL ? destRel : bucket_rel,
                                              LockTupleExclusive,
                                              &tmfd.ctid,
                                              tmfd.xmax);
                if (copyTuple == NULL) {
                    pfree_ext(valuesfornew);
                    pfree_ext(isnullfornew);
                    break;
                }

                valuesfornew = (Datum*)palloc0(m_tupDesc->natts * sizeof(Datum));
                isnullfornew = (bool*)palloc0(m_tupDesc->natts * sizeof(bool));

                tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);

                if (m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                    pfree_ext(valuesfornew);
                    pfree_ext(isnullfornew);
                    break;
                }
                oldtup->t_self = tmfd.ctid;
                oldtup = copyTuple;
                pfree(valuesfornew);
                pfree(isnullfornew);

                goto lreplace;
                break;
            }

            case TM_Deleted:
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert (ItemPointerEquals(&tup->t_self, &tmfd.ctid));
                break;

            default:
                elog(ERROR, "unrecognized heap_update status: %u", result);
                break;
        }
    }

    tableam_tops_free_tuple(tup);

    (void)ExecClearTuple(m_reslot);
    success = true;

    /****************
     * step 3: done *
     ****************/
    ExecCloseIndices(result_rel_info);
    m_isCompleted = true;
    m_scan->End(true);
    ExecDoneStepInFusion(bucket_rel, m_estate);
    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "UPDATE %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

DeleteFusion::DeleteFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_paramLoc = NULL;
    m_tmpvals = NULL;
    m_tmpisnull = NULL;
    m_attrno = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    ModifyTable* node = (ModifyTable*)m_planstmt->planTree;

    Plan *deletePlan = (Plan *)linitial(node->plans);

    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(deletePlan);

    m_reloid = getrelid(linitial_int(m_planstmt->resultRelations), m_planstmt->rtable);
    Relation rel = heap_open(m_reloid, AccessShareLock);

    m_estate = CreateExecutorState();
    m_estate->es_range_table = m_planstmt->rtable;

    m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_values = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_isnull = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));
    m_is_bucket_rel = RELATION_OWN_BUCKET(rel);

    heap_close(rel, AccessShareLock);
    initParams(params);

    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_planstmt, m_outParams);
    MemoryContextSwitchTo(old_context);
}

bool DeleteFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    bool execMatview = false;

    /*******************
     * step 1: prepare *
     *******************/
    m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);

    m_scan->Init(max_rows);

    Relation rel = ((m_scan->m_parentRel) == NULL ? m_scan->m_rel : m_scan->m_parentRel);
    Relation bucket_rel = NULL;
    int2 bucketid = InvalidBktId;

    Relation partRel = NULL;
    Partition part = NULL;

    ResultRelInfo* result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_estate->es_result_relation_info = result_rel_info;
    m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    InitPartitionByScanFusion(rel, &partRel, &part, m_estate, m_scan);

    /********************************
     * step 2: begin scan and delete*
     ********************************/
    HeapTuple oldtup = NULL;
    unsigned long nprocessed = 0;
    m_tupDesc = RelationGetDescr(rel);

    while ((oldtup = m_scan->getTuple()) != NULL) {
        TM_Result result;
        TM_FailureData tmfd;
        m_scan->UpdateCurrentRel(&rel);
        if (m_is_bucket_rel) {
            Assert(oldtup->t_bucketId == computeTupleBucketId(result_rel_info->ri_RelationDesc, oldtup));
            bucketid = oldtup->t_bucketId;
            bucket_rel = InitBucketRelation(bucketid, rel, part);
        }

    ldelete:
        Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
        result = tableam_tuple_delete(bucket_rel == NULL ? destRel : bucket_rel,
                             &oldtup->t_self,
                             m_estate->es_output_cid,
                             //m_estate->es_snapshot,
                             InvalidSnapshot,
                             NULL,
                             true,
                             &tmfd,
                             false);

        switch (result) {
            case TM_SelfModified:
                if (tmfd.cmax != m_estate->es_output_cid)
                    ereport(ERROR,
                            (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                             errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                             errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                /* already deleted by self; nothing to do */
                break;

            case TM_Ok:
                if (!RELATION_IS_PARTITIONED(rel)) {
                    /* Here Do the thing for Matview. */
                    execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                    if (execMatview) {
                        /* judge whether need to insert into mlog-table */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL,
                                            &oldtup->t_self, tmfd.xmin, 'D');
                    }
                }
                /* done successfully */
                nprocessed++;
                break;

            case TM_Updated: {
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert(!ItemPointerEquals(&oldtup->t_self, &tmfd.ctid));

                bool* isnullfornew = NULL;
                Datum* valuesfornew = NULL;
                HeapTuple copyTuple;
                copyTuple = heap_lock_updated(m_estate->es_output_cid,
                                              bucket_rel == NULL ? destRel : bucket_rel,
                                              LockTupleExclusive,
                                              &tmfd.ctid,
                                              tmfd.xmax);
                if (copyTuple == NULL) {
                    break;
                }
                valuesfornew = (Datum*)palloc0(m_tupDesc->natts * sizeof(Datum));
                isnullfornew = (bool*)palloc0(m_tupDesc->natts * sizeof(bool));
                tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);
                if (m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                    break;
                }
                oldtup->t_self = tmfd.ctid;
                oldtup = copyTuple;
                pfree(valuesfornew);
                pfree(isnullfornew);
                goto ldelete;
                break;
            }

            case TM_Deleted:
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert(ItemPointerEquals(&oldtup->t_self, &tmfd.ctid));
                break;

            default:
                elog(ERROR, "unrecognized heap_delete status: %u", result);
                break;
        }
    }

    (void)ExecClearTuple(m_reslot);
    success = true;

    /****************
     * step 3: done *
     ****************/
    ExecCloseIndices(result_rel_info);
    m_isCompleted = true;
    m_scan->End(true);
    ExecDoneStepInFusion(bucket_rel, m_estate);

    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "DELETE %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

SelectForUpdateFusion::SelectForUpdateFusion(
    MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_paramLoc = NULL;

    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    IndexScan* node = NULL;
    m_limitCount = -1;
    m_limitOffset = -1;

    /* get limit num */
    if (IsA(m_planstmt->planTree, Limit)) {
        Limit* limit = (Limit*)m_planstmt->planTree;
        node = (IndexScan *)JudgePlanIsPartIterator(m_planstmt->planTree->lefttree->lefttree);
        if (limit->limitOffset != NULL && IsA(limit->limitOffset, Const) &&
            !((Const*)limit->limitOffset)->constisnull) {
            m_limitOffset = DatumGetInt64(((Const*)limit->limitOffset)->constvalue);
        }
        if (limit->limitCount != NULL && IsA(limit->limitCount, Const) && !((Const*)limit->limitCount)->constisnull) {
            m_limitCount = DatumGetInt64(((Const*)limit->limitCount)->constvalue);
        }
    } else {
        node = (IndexScan *)JudgePlanIsPartIterator(m_planstmt->planTree->lefttree);
    }

    List* targetList = node->scan.plan.targetlist;
    m_reloid = getrelid(node->scan.scanrelid, m_planstmt->rtable);
    Relation rel = heap_open(m_reloid, AccessShareLock);

    Assert(list_length(targetList) >= 2);
    m_tupDesc = ExecCleanTypeFromTL(targetList, false, rel->rd_tam_type);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);

    m_estate = CreateExecutorState();
    m_estate->es_range_table = m_planstmt->rtable;

    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    m_is_bucket_rel = RELATION_OWN_BUCKET(rel);

    ListCell* lc = NULL;
    int cur_resno = 1;
    TargetEntry* res = NULL;
    foreach (lc, targetList) {
        res = (TargetEntry*)lfirst(lc);
        if (res->resjunk) {
            continue;
        }
        m_attrno[cur_resno - 1] = res->resorigcol;
        cur_resno++;
    }
    Assert(m_tupDesc->natts == cur_resno - 1);
    heap_close(rel, AccessShareLock);
    initParams(params);

    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion((Node*)node, m_planstmt, m_outParams);
    MemoryContextSwitchTo(old_context);
}

bool SelectForUpdateFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    int64 start_row = 0;
    int64 get_rows = 0;

    /*******************
     * step 1: prepare *
     *******************/
    IndexScan* node = NULL;
    if (m_limitCount >= 0 || m_limitOffset >= 0) {
        node = (IndexScan*)m_planstmt->planTree->lefttree->lefttree;
    } else {
        node = (IndexScan*)m_planstmt->planTree->lefttree;
    }

    start_row = m_limitOffset >= 0 ? m_limitOffset : start_row;
    get_rows = m_limitCount >= 0 ? (m_limitCount + start_row) : max_rows;
    if (m_position == 0) {
        m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);
        m_scan->Init(max_rows);
    }
    Relation rel = ((m_scan->m_parentRel == NULL) ? m_scan->m_rel : m_scan->m_parentRel);
    Relation partRel = NULL;
    Relation bucket_rel = NULL;
    Partition part = NULL;
    int2 bucketid = InvalidBktId;

    ResultRelInfo* result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_estate->es_result_relation_info = result_rel_info;
    m_estate->es_output_cid = GetCurrentCommandId(true);


    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    InitPartitionByScanFusion(rel, &partRel, &part, m_estate, m_scan);
    /**************************************
     * step 2: begin scan and update xmax *
     **************************************/
    setReceiver();

    HeapTuple tuple = NULL;
    HeapTuple tmptup = NULL;
    unsigned long nprocessed = 0;

    TM_Result result;
    TM_FailureData tmfd;
    Buffer buffer;
    while (nprocessed < (unsigned long)start_row && (tuple = m_scan->getTuple()) != NULL) {
        nprocessed++;
    }

    while (nprocessed < (unsigned long)get_rows && (tuple = m_scan->getTuple()) != NULL) {
        m_scan->UpdateCurrentRel(&rel);
        CHECK_FOR_INTERRUPTS();
        tableam_tops_deform_tuple(tuple, RelationGetDescr(rel), m_values, m_isnull);

        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        tmptup = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull, HEAP_TUPLE);

        Assert(tmptup != NULL);

        {
            if (m_is_bucket_rel) {
                Assert(tuple->t_bucketId == computeTupleBucketId(result_rel_info->ri_RelationDesc, tuple));
                bucketid = tuple->t_bucketId;
                bucket_rel = InitBucketRelation(bucketid, rel, part);
            }

            (void)ExecStoreTuple(tmptup, /* tuple to store */
                m_reslot,                /* slot to store in */
                InvalidBuffer,           /* TO DO: survey */
                false);                  /* don't pfree this pointer */

            Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
            tableam_tslot_getsomeattrs(m_reslot, m_tupDesc->natts);
            result = tableam_tuple_lock(bucket_rel == NULL ? destRel : bucket_rel,
                                        tuple,
                                        &buffer,
                                        m_estate->es_output_cid,
                                        LockTupleExclusive,
                                        false,
                                        &tmfd, false, false, false, InvalidSnapshot, NULL, false);
            ReleaseBuffer(buffer);

            if (result == TM_SelfModified) {
                continue;
            }

            switch (result) {
                case TM_SelfCreated:
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("attempted to lock invisible tuple")));
                    break;
                case TM_SelfModified:

                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.  We *must* ignore the tuple in the former
                     * case, so as to avoid the "Halloween problem" of repeated
                     * update attempts.  In the latter case it might be sensible
                     * to fetch the updated tuple instead, but doing so would
                     * require changing heap_update and heap_delete to not
                     * complain about updating "invisible" tuples, which seems
                     * pretty scary (heap_lock_tuple will not complain, but few
                     * callers expect HeapTupleInvisible, and we're not one of
                     * them).  So for now, treat the tuple as deleted and do not
                     * process.
                     */

                    /* already deleted by self; nothing to do */
                    break;

                case TM_Ok:
                    /* done successfully */
                    nprocessed++;
                    (*m_receiver->receiveSlot)(m_reslot, m_receiver);
                    tpslot_free_heaptuple(m_reslot);
                    break;

                case TM_Updated: {
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR,
                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("could not serialize access due to concurrent update")));
                    }
                    Assert(!ItemPointerEquals(&tuple->t_self, &tmfd.ctid));

                    bool* isnullfornew = NULL;
                    Datum* valuesfornew = NULL;
                    HeapTuple copyTuple;
                    copyTuple = heap_lock_updated(m_estate->es_output_cid,
                                                  bucket_rel == NULL ? destRel : bucket_rel,
                                                  LockTupleExclusive,
                                                  &tmfd.ctid,
                                                  tmfd.xmax);
                    if (copyTuple == NULL) {
                        break;
                    }
                    valuesfornew = (Datum*)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
                    isnullfornew = (bool*)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));

                    tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);

                    if (m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                        pfree_ext(valuesfornew);
                        pfree_ext(isnullfornew);
                        break;
                    }

                    for (int i = 0; i < m_tupDesc->natts; i++) {
                        m_tmpvals[i] = valuesfornew[m_attrno[i] - 1];
                        m_tmpisnull[i] = isnullfornew[m_attrno[i] - 1];
                    }

                    tmptup = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull, HEAP_TUPLE);
                    Assert(tmptup != NULL);

                    (void)ExecStoreTuple(tmptup, /* tuple to store */
                        m_reslot,                /* slot to store in */
                        InvalidBuffer,           /* TO DO: survey */
                        false);                  /* don't pfree this pointer */

                    tableam_tslot_getsomeattrs(m_reslot, m_tupDesc->natts);
                                nprocessed++;
                                (*m_receiver->receiveSlot)(m_reslot, m_receiver);
                                tuple->t_self = tmfd.ctid;
                                tuple = copyTuple;
                                pfree(valuesfornew);
                                pfree(isnullfornew);
                            break;
                        }
                    
                case TM_Deleted:
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR,
                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("could not serialize access due to concurrent update")));
                    }
                    Assert(ItemPointerEquals(&tuple->t_self, &tmfd.ctid));
                    break;
                default:
                    elog(ERROR, "unrecognized heap_lock_tuple status: %u", result);
                    break;
            }
        }
    }

    if (!ScanDirectionIsNoMovement(*(m_scan->m_direction))) {
        if (max_rows == 0 || nprocessed < (unsigned long)max_rows) {
            m_isCompleted = true;
        }
        m_position += nprocessed;
    } else {
        m_isCompleted = true;
    }

    (void)ExecClearTuple(m_reslot);

    success = true;
    /****************
     * step 3: done *
     ****************/
    ExecCloseIndices(result_rel_info);

    m_scan->End(m_isCompleted);
	if (m_isCompleted) {
        m_position = 0;
    }
    ExecDoneStepInFusion(bucket_rel, m_estate);

    if (m_isInsideRec) {
        (*m_receiver->rDestroy)(m_receiver);
    }
    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

void SelectForUpdateFusion::close()
{
    if (m_isCompleted == false) {
        m_scan->End(true);
        m_isCompleted = true;
        m_position = 0;
    }
}
AggFusion::AggFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_tmpvals = NULL;
    m_values = NULL;
    m_isnull = NULL;
    m_tmpisnull = NULL;

    m_paramLoc = NULL;
    m_attrno = NULL;
    m_reloid = 0;

    MemoryContext oldContext = MemoryContextSwitchTo(m_context);

    Agg *aggnode = (Agg *)m_planstmt->planTree;
    Node* scan = JudgePlanIsPartIterator(aggnode->plan.lefttree);

    initParams(params);
    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion(scan, m_planstmt, m_outParams);
    m_reslot = NULL;

    /* agg init */
    List *targetList = aggnode->plan.targetlist;
    TargetEntry *tar = (TargetEntry *)linitial(targetList);
    Aggref *aggref = (Aggref *)tar->expr;

    switch (aggref->aggfnoid) {
        case INT2SUMFUNCOID:
            m_aggSumFunc = &AggFusion::agg_int2_sum;
            break;
        case INT4SUMFUNCOID:
            m_aggSumFunc = &AggFusion::agg_int4_sum;
            break;
        case INT8SUMFUNCOID:
            m_aggSumFunc = &AggFusion::agg_int8_sum;
            break;
        case NUMERICSUMFUNCOID:
            m_aggSumFunc = &AggFusion::agg_numeric_sum;
            break;
        default:
            elog(ERROR, "unsupported aggfnoid %u for bypass.", aggref->aggfnoid);
            break;
    }

    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
    if (!HeapTupleIsValid(aggTuple)) {
        elog(ERROR, "cache lookup failed for aggregate %u",
             aggref->aggfnoid);
    }
    ReleaseSysCache(aggTuple);

    m_tupDesc = ExecTypeFromTL(targetList, false);

    m_attrno = (int16 *) palloc(m_tupDesc->natts * sizeof(int16));

    /* m_tupDesc->natts always be 1 currently. */
    TargetEntry *res = NULL;
    res = (TargetEntry *)linitial(aggref->args);
    Var *var = (Var *)res->expr;
    m_attrno[0] = var->varattno;
    m_isCompleted = true;

    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_values = (Datum*)palloc0(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc0(m_tupDesc->natts * sizeof(bool));

    MemoryContextSwitchTo(oldContext);
}

bool AggFusion::execute(long max_rows, char *completionTag)
{
    max_rows = FETCH_ALL;
    bool success = false;

    TupleTableSlot *reslot = m_reslot;
    Datum* values = m_values;
    bool * isnull = m_isnull;
    for (int i = 0; i < m_tupDesc->natts; i++) {
        isnull[i] = true;
    }

    /* step 2: begin scan */
    m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);

    m_scan->Init(max_rows);

    setReceiver();

    long nprocessed = 0;
    TupleTableSlot *slot = NULL;
    while ((slot = m_scan->getTupleSlot()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        nprocessed++;

        {
            AutoContextSwitch memSwitch(m_tmpContext);
            for (int i = 0; i < m_tupDesc->natts; i++) {
                reslot->tts_values[i] = slot->tts_values[m_attrno[i] - 1];
                reslot->tts_isnull[i] = slot->tts_isnull[m_attrno[i] - 1];
                (this->*m_aggSumFunc)(&values[i], isnull[i],
                        &reslot->tts_values[i], reslot->tts_isnull[i]);
                isnull[i] = false;
            }
        }

        if (nprocessed >= max_rows) {
            break;
        }
    }

    HeapTuple tmptup = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, values, isnull, HEAP_TUPLE);
    (void)ExecStoreTuple(tmptup, reslot, InvalidBuffer, false);

    tableam_tslot_getsomeattrs(reslot, m_tupDesc->natts);

    (*m_receiver->receiveSlot)(reslot, m_receiver);

    tpslot_free_heaptuple(m_reslot);

    success = true;

    /* step 3: done */
    if (m_isInsideRec == true) {
        (*m_receiver->rDestroy)(m_receiver);
    }

    m_isCompleted = true;
    m_scan->End(true);

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

void
AggFusion::agg_int2_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    int64 newval;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        newval = (int64)DatumGetInt16(*inVal);
        *transVal = Int64GetDatum(newval);
        return;
    }

    int64 oldsum = DatumGetInt64(*transVal);
    newval = oldsum + (int64)DatumGetInt16(*inVal);
    *transVal = Int64GetDatum(newval);
}

void
AggFusion::agg_int4_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    int64 newval;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        newval = (int64)DatumGetInt32(*inVal);
        *transVal = Int64GetDatum(newval);
        return;
    }

    int64 oldsum = DatumGetInt64(*transVal);
    newval = oldsum + (int64)DatumGetInt32(*inVal);
    *transVal = Int64GetDatum(newval);
}

void
AggFusion::agg_int8_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    Numeric     num1;
    Numeric     num2;
    Numeric     res;
    NumericVar  result;
    int64       val;
    Datum       newVal;
    NumericVar      arg1;
    NumericVar      arg2;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        val = DatumGetInt64(*inVal);
        init_var(&result);
        int64_to_numericvar(val, &result);
        res = make_result(&result);
        free_var(&result);
        newVal = NumericGetDatum(res);
        newVal = datumCopy(newVal, false, -1);
        *transVal = newVal;

        return;
    }

    val = DatumGetInt64(*inVal);
    init_var(&result);
    int64_to_numericvar(val, &result);
    res = make_result(&result);
    free_var(&result);

    num1 = DatumGetNumeric(*transVal);
    num2 = res;

    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);
    add_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);
    newVal = NumericGetDatum(res);
    newVal = datumCopy(newVal, false, -1);

    if (likely(!transIsNull)) {
        pfree(DatumGetPointer(*transVal));
    }

    *transVal = newVal;

    return;
}

void AggFusion::agg_numeric_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    Numeric     num1;
    Numeric     num2;
    Numeric     res;
    NumericVar  result;
    Datum       newVal;
    NumericVar      arg1;
    NumericVar      arg2;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        *transVal = datumCopy(*inVal, false, -1);
        return;
    }

    num1 = DatumGetNumeric(*transVal);
    num2 = DatumGetNumeric(*inVal);

    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);
    add_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);
    newVal = NumericGetDatum(res);
    newVal = datumCopy(newVal, false, -1);

    if (likely(!transIsNull)) {
        pfree(DatumGetPointer(*transVal));
    }

    *transVal = newVal;

    return;
}

SortFusion::SortFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    m_tmpvals = NULL;
    m_values = NULL;
    m_isnull = NULL;
    m_tmpisnull = NULL;

    m_paramLoc = NULL;
    m_attrno = NULL;
    m_reloid = 0;

    MemoryContext oldContext = MemoryContextSwitchTo(m_context);

    Sort *sortnode = (Sort *)m_planstmt->planTree;
    m_tupDesc = ExecCleanTypeFromTL(sortnode->plan.targetlist, false);

    m_attrno = (int16 *) palloc(m_tupDesc->natts * sizeof(int16));

    ListCell *lc = NULL;
    int cur_resno = 1;
    foreach (lc, sortnode->plan.targetlist) {
        TargetEntry *res = (TargetEntry *)lfirst(lc);
        if (res->resjunk)
            continue;

        Var *var = (Var *)res->expr;
        m_attrno[cur_resno - 1] = var->varattno;
        cur_resno++;
    }

    Plan *node = (Plan*)m_planstmt->planTree->lefttree;
    m_scanDesc = ExecTypeFromTL(node->targetlist, false);

    initParams(params);
    m_receiver = NULL;
    m_isInsideRec = true;
    m_scan = ScanFusion::getScanFusion((Node*)sortnode->plan.lefttree, m_planstmt, m_outParams);
    m_tupDesc->tdTableAmType = m_scan->m_tupDesc->tdTableAmType;
    m_scanDesc->tdTableAmType = m_scan->m_tupDesc->tdTableAmType;
    m_reslot = NULL;
    m_isCompleted = true;

    m_reslot = MakeSingleTupleTableSlot(m_tupDesc, false, m_scan->m_tupDesc->tdTableAmType);
    m_values = (Datum*)palloc0(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc0(m_tupDesc->natts * sizeof(bool));

    MemoryContextSwitchTo(oldContext);
}

bool SortFusion::execute(long max_rows, char *completionTag)
{
    max_rows = FETCH_ALL;
    bool success = false;

    TupleTableSlot *reslot = m_reslot;
    Datum *values = m_values;
    bool  *isnull = m_isnull;
    for (int i = 0; i < m_tupDesc->natts; i++) {
        isnull[i] = true;
    }

    /* prepare */
    m_scan->refreshParameter(m_outParams == NULL ? m_params : m_outParams);

    m_scan->Init(max_rows);

    setReceiver();

    Tuplesortstate *tuplesortstate = NULL;
    Sort       *sortnode = (Sort *)m_planstmt->planTree;
    int64       sortMem = SET_NODEMEM(sortnode->plan.operatorMemKB[0], sortnode->plan.dop);
    int64       maxMem = (sortnode->plan.operatorMaxMem > 0) ?
        SET_NODEMEM(sortnode->plan.operatorMaxMem, sortnode->plan.dop) : 0;

    {
        AutoContextSwitch memSwitch(m_context);
        tuplesortstate = tuplesort_begin_heap(m_scanDesc,
                sortnode->numCols,
                sortnode->sortColIdx,
                sortnode->sortOperators,
                sortnode->collations,
                sortnode->nullsFirst,
                sortMem,
                false,
                maxMem,
                sortnode->plan.plan_node_id,
                SET_DOP(sortnode->plan.dop));
    }

    /* receive data from indexscan node */
    long nprocessed = 0;
    TupleTableSlot *slot = NULL;
    while ((slot = m_scan->getTupleSlot()) != NULL) {
        tuplesort_puttupleslot(tuplesortstate, slot);
    }

    /* sort all data */
    tuplesort_performsort(tuplesortstate);

    /* send sorted data to client */
    slot = MakeSingleTupleTableSlot(m_scanDesc);
    while (tuplesort_gettupleslot(tuplesortstate, true, slot, NULL)) {
        tableam_tslot_getsomeattrs(slot, m_scanDesc->natts);
        for (int i = 0; i < m_tupDesc->natts; i++) {
            values[i] = slot->tts_values[m_attrno[i] - 1];
            isnull[i] = slot->tts_isnull[m_attrno[i] - 1];
        }

        HeapTuple tmptup = (HeapTuple) tableam_tops_form_tuple(m_tupDesc, values, isnull,  TableAMGetTupleType(m_tupDesc->tdTableAmType));
        (void)ExecStoreTuple(tmptup, reslot, InvalidBuffer, false);
        (*m_receiver->receiveSlot)(reslot, m_receiver);
        tableam_tops_free_tuple(tmptup);

        CHECK_FOR_INTERRUPTS();
        nprocessed++;
        if (nprocessed >= max_rows) {
            break;
        }
    }

    success = true;

    /* step 3: done */
    if (m_isInsideRec == true) {
        (*m_receiver->rDestroy)(m_receiver);
    }

    m_isCompleted = true;
    m_scan->End(true);

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}
