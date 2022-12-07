/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * mot_fdw_helpers.cpp
 *    MOT Foreign Data Wrapper helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_helpers.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "funcapi.h"
#include "catalog/pg_operator.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/date.h"
#include "mot_internal.h"
#include "mot_fdw_helpers.h"
#include "jit_context.h"

// enable MOT Engine logging facilities
DECLARE_LOGGER(InternalExecutorHelper, FDW)

bool IsNotEqualOper(OpExpr* op)
{
    switch (op->opno) {
        case INT48NEOID:
        case BooleanNotEqualOperator:
        case 402:
        case INT8NEOID:
        case INT84NEOID:
        case INT4NEOID:
        case INT2NEOID:
        case 531:
        case INT24NEOID:
        case INT42NEOID:
        case 561:
        case 567:
        case 576:
        case 608:
        case 644:
        case FLOAT4NEOID:
        case 630:
        case 5514:
        case 643:
        case FLOAT8NEOID:
        case 713:
        case 812:
        case 901:
        case BPCHARNEOID:
        case 1071:
        case DATENEOID:
        case 1109:
        case 1551:
        case FLOAT48NEOID:
        case FLOAT84NEOID:
        case 1321:
        case 1331:
        case 1501:
        case 1586:
        case 1221:
        case 1202:
        case NUMERICNEOID:
        case 1785:
        case 1805:
        case INT28NEOID:
        case INT82NEOID:
        case 1956:
        case 3799:
        case TIMESTAMPNEOID:
        case 2350:
        case 2363:
        case 2376:
        case 2389:
        case 2539:
        case 2545:
        case 2973:
        case 3517:
        case 3630:
        case 3677:
        case 2989:
        case 3883:
        case 5551:
            return true;

        default:
            return false;
    }
}

inline void RevertKeyOperation(KEY_OPER& oper)
{
    if (oper == KEY_OPER::READ_KEY_BEFORE) {
        oper = KEY_OPER::READ_KEY_AFTER;
    } else if (oper == KEY_OPER::READ_KEY_OR_PREV) {
        oper = KEY_OPER::READ_KEY_OR_NEXT;
    } else if (oper == KEY_OPER::READ_KEY_AFTER) {
        oper = KEY_OPER::READ_KEY_BEFORE;
    } else if (oper == KEY_OPER::READ_KEY_OR_NEXT) {
        oper = KEY_OPER::READ_KEY_OR_PREV;
    }
    return;
}

inline bool GetKeyOperation(OpExpr* op, KEY_OPER& oper)
{
    switch (op->opno) {
        case FLOAT8EQOID:
        case FLOAT4EQOID:
        case INT2EQOID:
        case INT4EQOID:
        case INT8EQOID:
        case INT24EQOID:
        case INT42EQOID:
        case INT84EQOID:
        case INT48EQOID:
        case INT28EQOID:
        case INT82EQOID:
        case FLOAT48EQOID:
        case FLOAT84EQOID:
        case 5513:  // INT1EQ
        case BPCHAREQOID:
        case TEXTEQOID:
        case 92:    // CHAREQ
        case 2536:  // timestampVStimestamptz
        case 2542:  // timestamptzVStimestamp
        case 2347:  // dateVStimestamp
        case 2360:  // dateVStimestamptz
        case 2373:  // timestampVSdate
        case 2386:  // timestamptzVSdate
        case TIMESTAMPEQOID:
            oper = KEY_OPER::READ_KEY_EXACT;
            break;
        case FLOAT8LTOID:
        case FLOAT4LTOID:
        case INT2LTOID:
        case INT4LTOID:
        case INT8LTOID:
        case INT24LTOID:
        case INT42LTOID:
        case INT84LTOID:
        case INT48LTOID:
        case INT28LTOID:
        case INT82LTOID:
        case FLOAT48LTOID:
        case FLOAT84LTOID:
        case 5515:  // INT1LT
        case 1058:  // BPCHARLT
        case 631:   // CHARLT
        case TEXTLTOID:
        case 2534:  // timestampVStimestamptz
        case 2540:  // timestamptzVStimestamp
        case 2345:  // dateVStimestamp
        case 2358:  // dateVStimestamptz
        case 2371:  // timestampVSdate
        case 2384:  // timestamptzVSdate
        case TIMESTAMPLTOID:
            oper = KEY_OPER::READ_KEY_BEFORE;
            break;
        case FLOAT8LEOID:
        case FLOAT4LEOID:
        case INT2LEOID:
        case INT4LEOID:
        case INT8LEOID:
        case INT24LEOID:
        case INT42LEOID:
        case INT84LEOID:
        case INT48LEOID:
        case INT28LEOID:
        case INT82LEOID:
        case FLOAT48LEOID:
        case FLOAT84LEOID:
        case 5516:  // INT1LE
        case 1059:  // BPCHARLE
        case 632:   // CHARLE
        case 665:   // TEXTLE
        case 2535:  // timestampVStimestamptz
        case 2541:  // timestamptzVStimestamp
        case 2346:  // dateVStimestamp
        case 2359:  // dateVStimestamptz
        case 2372:  // timestampVSdate
        case 2385:  // timestamptzVSdate
        case TIMESTAMPLEOID:
            oper = KEY_OPER::READ_KEY_OR_PREV;
            break;
        case FLOAT8GTOID:
        case FLOAT4GTOID:
        case INT2GTOID:
        case INT4GTOID:
        case INT8GTOID:
        case INT24GTOID:
        case INT42GTOID:
        case INT84GTOID:
        case INT48GTOID:
        case INT28GTOID:
        case INT82GTOID:
        case FLOAT48GTOID:
        case FLOAT84GTOID:
        case 5517:       // INT1GT
        case 1060:       // BPCHARGT
        case 633:        // CHARGT
        case TEXTGTOID:  // TEXTGT
        case 2538:       // timestampVStimestamptz
        case 2544:       // timestamptzVStimestamp
        case 2349:       // dateVStimestamp
        case 2362:       // dateVStimestamptz
        case 2375:       // timestampVSdate
        case 2388:       // timestamptzVSdate
        case TIMESTAMPGTOID:
            oper = KEY_OPER::READ_KEY_AFTER;
            break;
        case FLOAT8GEOID:
        case FLOAT4GEOID:
        case INT2GEOID:
        case INT4GEOID:
        case INT8GEOID:
        case INT24GEOID:
        case INT42GEOID:
        case INT84GEOID:
        case INT48GEOID:
        case INT28GEOID:
        case INT82GEOID:
        case FLOAT48GEOID:
        case FLOAT84GEOID:
        case 5518:  // INT1GE
        case 1061:  // BPCHARGE
        case 634:   // CHARGE
        case 667:   // TEXTGE
        case 2537:  // timestampVStimestamptz
        case 2543:  // timestamptzVStimestamp
        case 2348:  // dateVStimestamp
        case 2361:  // dateVStimestamptz
        case 2374:  // timestampVSdate
        case 2387:  // timestamptzVSdate
        case TIMESTAMPGEOID:
            oper = KEY_OPER::READ_KEY_OR_NEXT;
            break;
        case OID_TEXT_LIKE_OP:
        case OID_BPCHAR_LIKE_OP:
            oper = KEY_OPER::READ_KEY_LIKE;
            break;
        default:
            oper = KEY_OPER::READ_INVALID;
            break;
    }

    return (oper != KEY_OPER::READ_INVALID);
}

static bool IsMismatchDataTypesSupported(Oid columnType, Oid resultDatumType)
{
    if ((IS_INT_TYPE(columnType) && IS_INT_TYPE(resultDatumType)) ||
        (IS_TIME_TYPE(columnType) && IS_TIME_TYPE(resultDatumType)) ||
        (IS_CHAR_TYPE(columnType) && IS_CHAR_TYPE(resultDatumType))) {
        return true;
    }

    return false;
}

bool IsSameRelation(Expr* expr, uint32_t id)
{
    switch (expr->type) {
        case T_Param:
        case T_Const: {
            return false;
        }
        case T_Var: {
            return (((Var*)expr)->varno == id);
        }
        case T_OpExpr: {
            OpExpr* op = (OpExpr*)expr;
            bool l = IsSameRelation((Expr*)linitial(op->args), id);
            bool r = IsSameRelation((Expr*)lsecond(op->args), id);
            return (l || r);
        }
        case T_FuncExpr: {
            FuncExpr* func = (FuncExpr*)expr;

            if (func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) {
                return IsSameRelation((Expr*)linitial(func->args), id);
            } else if (list_length(func->args) == 0) {
                return false;
            } else {
                return true;
            }
        }
        case T_RelabelType: {
            return IsSameRelation(((RelabelType*)expr)->arg, id);
        }
        default:
            return true;
    }
}

static bool CheckOperationAdjustOperands(RelOptInfo* baserel, Expr* l, Expr* r, Var*& v, Expr*& e, KEY_OPER& oper)
{
    // this covers case when baserel.a = t2.a <==> t2.a = baserel.a both will be of type Var
    // we have to choose as Expr t2.a cause it will be replaced later with a Param type
    if (IsA(l, Var)) {
        if (!IsA(r, Var)) {
            if (IsSameRelation(r, ((Var*)l)->varno)) {
                return false;
            }
            v = (Var*)l;
            e = r;
        } else if (((Var*)l)->varno == ((Var*)r)->varno) {  // same relation
            return false;
        } else if (bms_is_member(((Var*)l)->varno, baserel->relids)) {
            v = (Var*)l;
            e = r;
        } else {
            v = (Var*)r;
            e = l;
            RevertKeyOperation(oper);
        }
    } else if (IsA(r, Var)) {
        if (IsSameRelation(l, ((Var*)r)->varno)) {
            return false;
        }
        v = (Var*)r;
        e = l;
        RevertKeyOperation(oper);
    } else {
        return false;
    }

    if (oper == KEY_OPER::READ_KEY_LIKE) {
        if (!IsA(e, Const)) {
            return false;
        }

        // we support only prefix search: 'abc%' or 'abc', the last transforms into equal
        Const* c = (Const*)e;
        if (DatumGetPointer(c->constvalue) == nullptr) {
            return false;
        }

        int len = 0;
        char* s = DatumGetPointer(c->constvalue);
        int i = 0;

        if (c->constlen > 0) {
            len = c->constlen;
        } else if (c->constlen == -1) {
            struct varlena* vs = (struct varlena*)DatumGetPointer(c->constvalue);
            s = VARDATA(c->constvalue);
            len = VARSIZE_ANY(vs) - VARHDRSZ;
        } else if (c->constlen == -2) {
            len = strlen(s);
        }

        for (; i < len; i++) {
            if (s[i] == '%') {
                break;
            }

            if (s[i] == '_') {  // we do not support single char pattern
                return false;
            }
        }

        if (i < len - 1) {
            return false;
        }
    }

    return true;
}

static bool CheckCastFunction(MOTFdwStateSt* state, FuncExpr* func)
{
    Expr* argExpr = (Expr*)linitial(func->args);
    if (argExpr && argExpr->type == T_Var) {
        Var* var = (Var*)argExpr;
        AttrNumber attno = var->varoattno;
        if (state->m_table && ((uint32_t)attno < state->m_table->GetFieldCount())) {
            MOT::Column* col = state->m_table->GetField(attno);
            Oid columnType = ConvertMotColumnTypeToOid(col->m_type);
            Oid resultDatumType = func->funcresulttype;

            if (!IsMismatchDataTypesSupported(columnType, resultDatumType)) {
                return false;
            }
        } else {
            return false;
        }
    }

    return true;
}

bool IsMOTExpr(RelOptInfo* baserel, MOTFdwStateSt* state, MatchIndexArr* marr, Expr* expr, Expr** result, bool setLocal)
{
    /*
     * We only support the following operators and data types.
     */
    bool isOperatorMOTReady = false;

    switch (expr->type) {
        case T_Const: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }

        case T_Var: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }
        case T_Param: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }
        case T_OpExpr: {
            KEY_OPER oper;
            OpExpr* op = (OpExpr*)expr;
            Expr* l = (Expr*)linitial(op->args);

            if (list_length(op->args) == 1) {
                isOperatorMOTReady = IsMOTExpr(baserel, state, marr, l, &l, setLocal);
                break;
            }

            Expr* r = (Expr*)lsecond(op->args);
            isOperatorMOTReady = IsMOTExpr(baserel, state, marr, l, &l, setLocal);
            isOperatorMOTReady = (isOperatorMOTReady && IsMOTExpr(baserel, state, marr, r, &r, setLocal));
            // handles case when column = column|const <oper> column|const
            if (result != nullptr && isOperatorMOTReady) {
                isOperatorMOTReady = !(IsA(l, Var) && IsA(r, Var) && ((Var*)l)->varno == ((Var*)r)->varno);
                break;
            }

            isOperatorMOTReady = (isOperatorMOTReady && GetKeyOperation(op, oper));
            if (isOperatorMOTReady && marr != nullptr) {
                Var* v = nullptr;
                Expr* e = nullptr;

                isOperatorMOTReady = CheckOperationAdjustOperands(baserel, l, r, v, e, oper);
                if (!isOperatorMOTReady) {
                    break;
                }
                isOperatorMOTReady = MOTAdaptor::SetMatchingExpr(state, marr, v->varoattno, oper, e, expr, setLocal);
            }
            break;
        }
        case T_FuncExpr: {
            FuncExpr* func = (FuncExpr*)expr;

            bool isCastFunction =
                (func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST);
            if (isCastFunction) {
                if (!CheckCastFunction(state, func)) {
                    return false;
                }

                isOperatorMOTReady = IsMOTExpr(baserel, state, marr, (Expr*)linitial(func->args), nullptr, setLocal);
            } else if (list_length(func->args) == 0) {
                isOperatorMOTReady = true;
            }

            break;
        }
        case T_RelabelType: {
            isOperatorMOTReady = IsMOTExpr(baserel, state, marr, ((RelabelType*)expr)->arg, result, setLocal);
            break;
        }
        default: {
            isOperatorMOTReady = false;
            break;
        }
    }

    return isOperatorMOTReady;
}

uint16_t MOTTimestampToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    Timestamp timestamp = DatumGetTimestamp(src);
    tmp = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return static_cast<uint16_t>(erc);
}

uint16_t MOTTimestampTzToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    TimestampTz timestamp = DatumGetTimestampTz(src);
    tmp = DatumGetCString(DirectFunctionCall1(timestamptz_out, timestamp));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return static_cast<uint16_t>(erc);
}

uint16_t MOTDateToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    DateADT date = DatumGetDateADT(src);
    tmp = DatumGetCString(DirectFunctionCall1(date_out, date));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return static_cast<uint16_t>(erc);
}

void DestroySession(MOT::SessionContext* sessionContext)
{
    MOT_ASSERT(MOTAdaptor::m_engine);
    MOT_LOG_DEBUG("Destroying session context %p, connection_id %u", sessionContext, sessionContext->GetConnectionId());

    if (u_sess->mot_cxt.jit_session_context_pool) {
        JitExec::FreeSessionJitContextPool(u_sess->mot_cxt.jit_session_context_pool);
    }
    MOT::GetSessionManager()->DestroySessionContext(sessionContext);
}

// Global map of PG session identification (required for session statistics)
// This approach is safer than saving information in the session context
static pthread_spinlock_t sessionDetailsLock;
typedef std::map<MOT::SessionId, pair<::ThreadId, pg_time_t>> SessionDetailsMap;
static SessionDetailsMap sessionDetailsMap;

void InitSessionDetailsMap()
{
    (void)pthread_spin_init(&sessionDetailsLock, 0);
}

void DestroySessionDetailsMap()
{
    (void)pthread_spin_destroy(&sessionDetailsLock);
}

void RecordSessionDetails()
{
    MOT::SessionId sessionId = u_sess->mot_cxt.session_id;
    if (sessionId != INVALID_SESSION_ID) {
        (void)pthread_spin_lock(&sessionDetailsLock);
        (void)sessionDetailsMap.emplace(sessionId, std::make_pair(t_thrd.proc->pid, t_thrd.proc->myStartTime));
        (void)pthread_spin_unlock(&sessionDetailsLock);
    }
}

void ClearSessionDetails(MOT::SessionId sessionId)
{
    if (sessionId != INVALID_SESSION_ID) {
        (void)pthread_spin_lock(&sessionDetailsLock);
        SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
        if (itr != sessionDetailsMap.end()) {
            (void)sessionDetailsMap.erase(itr);
        }
        (void)pthread_spin_unlock(&sessionDetailsLock);
    }
}

void ClearCurrentSessionDetails()
{
    ClearSessionDetails(u_sess->mot_cxt.session_id);
}

void GetSessionDetails(MOT::SessionId sessionId, ::ThreadId* gaussSessionId, pg_time_t* sessionStartTime)
{
    // although we have the PGPROC in the user data of the session context, we prefer not to use
    // it due to safety (in some unknown constellation we might hold an invalid pointer)
    // it is much safer to save a copy of the two required fields
    (void)pthread_spin_lock(&sessionDetailsLock);
    SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
    if (itr != sessionDetailsMap.end()) {
        *gaussSessionId = itr->second.first;
        *sessionStartTime = itr->second.second;
    }
    (void)pthread_spin_unlock(&sessionDetailsLock);
}

// provide safe session auto-cleanup in case of missing session closure
// This mechanism relies on the fact that when a session ends, eventually its thread is terminated
// ATTENTION: in thread-pooled envelopes this assumption no longer holds true, since the container thread keeps
// running after the session ends, and a session might run each time on a different thread, so we
// disable this feature, instead we use this mechanism to generate thread-ended event into the MOT Engine
static pthread_key_t sessionCleanupKey;

static void DestroyJitContextsInPlanCacheList(CachedPlanSource* planSourceList)
{
    if (planSourceList == nullptr) {
        return;
    }

    // ATTENTION: JIT'ed SP queries are pointed by SPI plans, so in order to avoid double free (once through plan, and
    // once through plan of grand-parent invoke), we first nullify all non-top-level contexts.
    MOT_LOG_TRACE("Nullifying non top-level contexts in their respective plans");
    CachedPlanSource* psrc = planSourceList;
    while (psrc != nullptr) {
        if ((psrc->mot_jit_context != nullptr) && JitExec::IsJitSubContextInline(psrc->mot_jit_context)) {
            MOT_LOG_TRACE("Found JIT sub-context %p in plan %p: %s",
                psrc->mot_jit_context,
                psrc,
                psrc->mot_jit_context->m_queryString);
            psrc->mot_jit_context = nullptr;
        }
        psrc = psrc->next_saved;
    }
    MOT_LOG_TRACE("Cleaning up all top-level JIT context objects for current session");
    psrc = planSourceList;
    while (psrc != nullptr) {
        if (psrc->mot_jit_context != nullptr) {
            MOT_LOG_TRACE("Found top-level JIT context %p in plan %p", psrc->mot_jit_context, psrc);
            JitExec::DestroyJitContext(psrc->mot_jit_context, true);
            psrc->mot_jit_context = nullptr;
        }
        psrc = psrc->next_saved;
    }
}

void DestroySessionJitContexts()
{
    MOT_LOG_TRACE("Destroying all JIT context objects for current session in first_saved_plan list: %p",
        u_sess->pcache_cxt.first_saved_plan);
    DestroyJitContextsInPlanCacheList(u_sess->pcache_cxt.first_saved_plan);

    MOT_LOG_TRACE("Destroying all JIT context objects for current session in ungpc_saved_plan list: %p",
        u_sess->pcache_cxt.ungpc_saved_plan);
    DestroyJitContextsInPlanCacheList(u_sess->pcache_cxt.ungpc_saved_plan);

    MOT_LOG_TRACE("Destroying JIT context object for current session in unnamed_stmt_psrc: %p",
        u_sess->pcache_cxt.unnamed_stmt_psrc);
    if (u_sess->pcache_cxt.unnamed_stmt_psrc != nullptr) {
        CachedPlanSource* psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
        if (psrc->mot_jit_context != nullptr) {
            if (JitExec::IsJitSubContextInline(psrc->mot_jit_context)) {
                MOT_LOG_TRACE("Found JIT sub-context %p in plan %p: %s",
                    psrc->mot_jit_context,
                    psrc,
                    psrc->mot_jit_context->m_queryString);
            } else {
                MOT_LOG_TRACE("Found top-level JIT context %p in plan %p", psrc->mot_jit_context, psrc);
                JitExec::DestroyJitContext(psrc->mot_jit_context, true);
            }
            psrc->mot_jit_context = nullptr;
        }
    }

    MOT_LOG_TRACE("DONE Cleaning up all JIT context objects for current session, JIT context count: %u",
        u_sess->mot_cxt.jit_context_count);
    MOT_ASSERT(u_sess->mot_cxt.jit_context_count == 0);
    JitExec::CleanupJitSourceTxnState();
}

static void SessionCleanup(void* key)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // in order to ensure session-id cleanup for session 0 we use positive values
    MOT::SessionId sessionId = (MOT::SessionId)(((uint64_t)key) - 1);
    if (sessionId != INVALID_SESSION_ID) {
        MOT_LOG_WARN("Encountered unclosed session %u (missing call to DestroyTxn()?)", (unsigned)sessionId);
        ClearSessionDetails(sessionId);
        MOT_LOG_DEBUG("SessionCleanup(): Calling DestroySessionJitContext()");
        DestroySessionJitContexts();
        if (MOTAdaptor::m_engine) {
            MOT::SessionContext* sessionContext = MOT::GetSessionManager()->GetSessionContext(sessionId);
            if (sessionContext != nullptr) {
                DestroySession(sessionContext);
            }
            // since a call to on_proc_exit(destroyTxn) was probably missing, we should also cleanup thread-locals
            // pay attention that if we got here it means the thread pool is disabled, so we must ensure thread-locals
            // are cleaned up right now. Due to these complexities, onCurrentThreadEnding() was designed to be proof
            // for repeated calls.
            MOTAdaptor::m_engine->OnCurrentThreadEnding();
        }
    }
}

void InitSessionCleanup()
{
    (void)pthread_key_create(&sessionCleanupKey, SessionCleanup);
}

void DestroySessionCleanup()
{
    (void)pthread_key_delete(sessionCleanupKey);
}

void ScheduleSessionCleanup()
{
    (void)pthread_setspecific(sessionCleanupKey, (const void*)(uint64_t)(u_sess->mot_cxt.session_id + 1));
}

void CancelSessionCleanup()
{
    (void)pthread_setspecific(sessionCleanupKey, nullptr);
}

/** @brief Notification from thread pool that a session ended or called from sess_exit callback MOTCleanupSession(). */
void MOTOnSessionClose()
{
    MOT_LOG_TRACE("Received session close notification (session id: %u, connection id: %u, JIT context count: %u)",
        u_sess->mot_cxt.session_id,
        u_sess->mot_cxt.connection_id,
        u_sess->mot_cxt.jit_context_count);
    if (u_sess->mot_cxt.session_id != INVALID_SESSION_ID) {
        ClearCurrentSessionDetails();
        MOT_LOG_DEBUG("MOTOnSessionClose(): Calling DestroySessionJitContexts()");
        DestroySessionJitContexts();
        if (!MOTAdaptor::m_engine) {
            MOT_LOG_ERROR("MOTOnSessionClose(): MOT engine is not initialized");
        } else {
            // initialize thread data - this is ok, it will be cleaned up when thread exits
            // avoid throwing errors and ignore them at this phase
            (void)EnsureSafeThreadAccess(false);
            MOT::SessionContext* sessionContext = u_sess->mot_cxt.session_context;
            if (sessionContext == nullptr) {
                MOT_LOG_WARN("Received session close notification, but no current session is found. Current session id "
                             "is %u. Request ignored.",
                    u_sess->mot_cxt.session_id);
            } else {
                DestroySession(sessionContext);
            }
        }
    }

    MOT_ASSERT(u_sess->mot_cxt.session_id == INVALID_SESSION_ID);
    MOT_ASSERT(u_sess->mot_cxt.session_context == nullptr);
    MOT_ASSERT(u_sess->mot_cxt.jit_context_count == 0);
}

/** @brief Notification from thread pool that a pooled thread ended (only when thread pool is ENABLED). */
void MOTOnThreadShutdown()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    MOT_LOG_TRACE("Received thread shutdown notification");
    if (!MOTAdaptor::m_engine) {
        MOT_LOG_ERROR("MOTOnThreadShutdown(): MOT engine is not initialized");
    } else {
        MOTAdaptor::m_engine->OnCurrentThreadEnding();
    }
    knl_thread_mot_init();  // reset all thread locals
}

/**
 * @brief on_proc_exit() callback to handle thread-cleanup - regardless of whether thread pool is enabled or not.
 * registration to on_proc_exit() is triggered by first call to EnsureSafeThreadAccess().
 */
void MOTCleanupThread(int status, Datum ptr)
{
    MOT_ASSERT(g_instance.attr.attr_common.enable_thread_pool);
    MOT_LOG_TRACE("Received thread cleanup notification (thread-pool ON, thread id: %u, session id: %u)",
        (unsigned)MOTCurrThreadId,
        u_sess->mot_cxt.session_id);

    // When FATAL error is thrown, proc_exit doesn't call sess_exit. So we call MOTOnSessionClose() to make sure
    // session-level resources are destroyed properly.
    MOTOnSessionClose();

    // when thread pool is used we just cleanup current thread
    // this might be a duplicate because thread pool also calls MOTOnThreadShutdown() - this is still ok
    // because we guard against repeated calls in MOTEngine::onCurrentThreadEnding()
    MOTOnThreadShutdown();
}

MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID)
{
    MOTFdwStateSt* state = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
    List* values = (List*)fdwState;

    state->m_allocInScan = true;
    state->m_foreignTableId = exTableID;
    if (list_length(values) > 0) {
        ListCell* cell = list_head(values);
        int type = ((Const*)lfirst(cell))->constvalue;
        if (type != static_cast<int>(FDW_LIST_TYPE::FDW_LIST_STATE)) {
            return state;
        }
        cell = lnext(cell);
        state->m_cmdOper = (CmdType)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_order = (SortDir)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_hasForUpdate = (bool)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_foreignTableId = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numAttrs = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_ctidNum = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        state->m_numExpr = ((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        int len = BITMAP_GETLEN(state->m_numAttrs);
        state->m_attrsUsed = (uint8_t*)palloc0(len);
        state->m_attrsModified = (uint8_t*)palloc0(len);
        BitmapDeSerialize(state->m_attrsUsed, len, state->m_hasIndexedColUpdate, &cell);

        if (cell != nullptr) {
            state->m_bestIx = &state->m_bestIxBuf;
            state->m_bestIx->Deserialize(cell, exTableID);
        }

        if (fdwExpr != nullptr && *fdwExpr != nullptr) {
            ListCell* c = nullptr;
            int i = 0;

            // divide fdw expr to param list and original expr
            state->m_remoteCondsOrig = nullptr;

            foreach (c, *fdwExpr) {
                if (i < state->m_numExpr) {
                    i++;
                    continue;
                } else {
                    state->m_remoteCondsOrig = lappend(state->m_remoteCondsOrig, lfirst(c));
                }
            }

            *fdwExpr = list_truncate(*fdwExpr, state->m_numExpr);
        }
    }
    return state;
}

void* SerializeFdwState(MOTFdwStateSt* state)
{
    List* result = nullptr;

    // set list type to FDW_LIST_TYPE::FDW_LIST_STATE
    result = lappend(
        result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(FDW_LIST_TYPE::FDW_LIST_STATE), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_cmdOper), false, true));
    result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(state->m_order), false, true));
    result = lappend(result, makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(state->m_hasForUpdate), false, true));
    result =
        lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_foreignTableId), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_numAttrs), false, true));
    result = lappend(result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(state->m_ctidNum), false, true));
    result = lappend(result, makeConst(INT2OID, -1, InvalidOid, 2, Int16GetDatum(state->m_numExpr), false, true));
    int len = BITMAP_GETLEN(state->m_numAttrs);
    result = BitmapSerialize(result, state->m_attrsUsed, len, state->m_hasIndexedColUpdate);

    if (state->m_bestIx != nullptr) {
        state->m_bestIx->Serialize(&result);
    }
    ReleaseFdwState(state);
    return result;
}

void ReleaseFdwState(MOTFdwStateSt* state)
{
    CleanCursors(state);

    if (state->m_bestIx && state->m_bestIx != &state->m_bestIxBuf) {
        pfree(state->m_bestIx);
    }

    if (state->m_remoteCondsOrig != nullptr) {
        list_free(state->m_remoteCondsOrig);
    }

    if (state->m_attrsUsed != nullptr) {
        pfree(state->m_attrsUsed);
    }

    if (state->m_attrsModified != nullptr) {
        pfree(state->m_attrsModified);
    }

    state->m_table = nullptr;
    pfree(state);
}

Oid ConvertMotColumnTypeToOid(MOT::MOT_CATALOG_FIELD_TYPES motColumnType)
{
    Oid typeOid = InvalidOid;
    switch (motColumnType) {
        case MOT::MOT_TYPE_DECIMAL:
            typeOid = NUMERICOID;
            break;

        case MOT::MOT_TYPE_VARCHAR:
            typeOid = VARCHAROID;
            break;

        case MOT::MOT_TYPE_CHAR:
            typeOid = CHAROID;
            break;

        case MOT::MOT_TYPE_TINY:
            typeOid = INT1OID;
            break;

        case MOT::MOT_TYPE_SHORT:
            typeOid = INT2OID;
            break;

        case MOT::MOT_TYPE_INT:
            typeOid = INT4OID;
            break;

        case MOT::MOT_TYPE_LONG:
            typeOid = INT8OID;
            break;

        case MOT::MOT_TYPE_FLOAT:
            typeOid = FLOAT4OID;
            break;

        case MOT::MOT_TYPE_DOUBLE:
            typeOid = FLOAT8OID;
            break;

        case MOT::MOT_TYPE_DATE:
            typeOid = DATEOID;
            break;

        case MOT::MOT_TYPE_TIME:
            typeOid = TIMEOID;
            break;

        case MOT::MOT_TYPE_TIMESTAMP:
            typeOid = TIMESTAMPOID;
            break;

        case MOT::MOT_TYPE_TIMESTAMPTZ:
            typeOid = TIMESTAMPTZOID;
            break;

        case MOT::MOT_TYPE_INTERVAL:
            typeOid = INTERVALOID;
            break;

        case MOT::MOT_TYPE_TIMETZ:
            typeOid = TIMETZOID;
            break;

        case MOT::MOT_TYPE_BLOB:
            typeOid = BLOBOID;
            break;

        default:
            break;
    }

    return typeOid;
}
