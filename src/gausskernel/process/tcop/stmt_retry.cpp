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
 * -------------------------------------------------------------------------
 *
 * stmt_retry.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/process/tcop/stmt_retry.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "tcop/stmt_retry.h"

#include <cctype>
#include <cmath>
#include <cstring>

#include "access/xact.h"
#include "access/hash.h"
#include "commands/prepare.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "utils/guc.h"
#include "utils/builtins.h"

#ifdef USE_RETRY_STUB
#define DEFAULT_PQ_RECV_BUFFER_SIZE 16
#else
#define DEFAULT_PQ_RECV_BUFFER_SIZE (8192 * 8)
#endif

/*
 * -------------------------------------------------------------------------------------------------------------------
 * error code usage define
 * -------------------------------------------------------------------------------------------------------------------
 */
const int ECODE_LENGTH = 5;

typedef enum {
    FINDING_CODE_FIRST_CH, /* try to find the first char of a error code */
    FINDING_CODE_CH,       /* try to find a normal char (except first char) of a error code */
    FINDING_SEPARATOR,     /* try to find a separator between error code */
} FindState;

/*
 * @Description: checking given error code list is valid or not.
 */
bool validate_errcodes(const char* errcodes, size_t length)
{
    if (errcodes == NULL || length < ECODE_LENGTH)
        return false;

    char ch;
    FindState state = FINDING_CODE_FIRST_CH;
    size_t ecode_len = 0;
    int found_ecode = 0;

    for (size_t i = 0; i < length; ++i) {
        ch = errcodes[i];
        bool is_alpha = std::isupper(static_cast<unsigned char>(ch));
        bool is_digit = std::isdigit(static_cast<unsigned char>(ch));
        bool is_space = std::isspace(static_cast<unsigned char>(ch));
        bool is_valid_ecode_ch = (is_alpha || is_digit);

        if (!is_valid_ecode_ch && !is_space)
            return false;

        /*
         * parse error code
         * if reaching here meaning current item is a valid error code char or a separator char.
         */
        switch (state) {
            case FINDING_CODE_FIRST_CH:
                if (is_valid_ecode_ch) {
                    ++ecode_len;
                    state = FINDING_CODE_CH;
                }
                /* if get a space here, just continue */
                break;

            case FINDING_CODE_CH:
                if (is_valid_ecode_ch) {
                    if (++ecode_len == ECODE_LENGTH) {
                        state = FINDING_SEPARATOR;
                        ++found_ecode;
                        ecode_len = 0;
                    }
                } else {
                    return false;
                }
                break;

            case FINDING_SEPARATOR:
                if (is_space) {
                    state = FINDING_CODE_FIRST_CH;
                    ;
                } else {
                    return false;
                }
                break;

            default: /* should not reach here */
                return false;
        }
    }

    return (state != FINDING_CODE_CH && found_ecode > 0);
}

/*
 * @Description: callback function for guc checking.
 */
bool check_errcode_list(char** newval, void** extra, GucSource source)
{
    return validate_errcodes(*newval, strlen(*newval));
}

/*
 * @Description: check if the sql statement need to retry by error code.
 * @const char* ecode_str - error code str
 * @in elevel - elevel type
 * @return - true, statement retry is needed by error code.
 */
bool IsStmtNeedRetryByErrCode(const char* ecode_str, int errlevel)
{
    if (unlikely(ecode_str == NULL)) {
        SimpleLogToServer(LOG, false, "%s null ecode str.", PRINT_PREFIX_TYPE_ALERT);
        return false;
    }

    if (unlikely(u_sess->attr.attr_sql.retry_errcode_list == NULL)) {
        /* in case guc hasn't run yet */
        SimpleLogToServer(LOG, false, "%s guc not run yet.", PRINT_PREFIX_TYPE_ALERT);
        return false;
    }

    return (strstr(u_sess->attr.attr_sql.retry_errcode_list, ecode_str) != NULL);
}

/*
 * -------------------------------------------------------------------------------------------------------------------
 * common usage define
 * -------------------------------------------------------------------------------------------------------------------
 */
/*
 * @Description: decision criterions for statment retry.
 * @in elevel - elevel type
 * @in sqlErrCode - error code type
 * @in errcodes - error code set of retry supported error codes
 * @return - true, statement retry is avaliable, flase statement retry is not avaliable.
 */
bool IsStmtRetryAvaliable(int elevel, int sqlErrCode)
{
#ifdef USE_RETRY_STUB
    if (u_sess->exec_cxt.RetryController != NULL && u_sess->exec_cxt.RetryController->stub_.OnStubTest()) {
        return true;
    }
#endif

    if (unlikely(u_sess->exec_cxt.RetryController == NULL))
        return false;

    u_sess->exec_cxt.RetryController->is_trans = IsInTransactionChain(true);

    return (IsStmtRetryEnabled() && !(u_sess->exec_cxt.RetryController->is_trans) &&
            !(u_sess->exec_cxt.RetryController->is_unsupported_query_type) &&
            !(u_sess->exec_cxt.RetryController->is_transaction_committed) &&
            (u_sess->exec_cxt.RetryController->retry_times < u_sess->attr.attr_common.max_query_retry_times) &&
            !(u_sess->exec_cxt.RetryController->is_tempfile_size_exceeded) &&
            IsStmtNeedRetryByErrCode(sqlErrCode, elevel));
}

/*
 * @Description: verdict whether statement can be retry.
 * @in controller - statement retry controller pointer
 * @in is_extend_query - true, meaning extend query. false, simple query
 * @in query_string - query string of simple query
 * @return - true, statement retry is capable, do retry in the following steps, flase statement retry is uncapable, do
             not do retry.
 */
bool IsStmtRetryCapable(StatementRetryController* controller, bool is_extend_query)
{
#ifdef USE_RETRY_STUB
    if (controller->stub_.OnStubTest()) {
        controller->stub_.ECodeValidate();
        return true;
    }
#endif
    if (unlikely(controller == NULL))
        return false;

    bool bret = false;

    int elevel;
    int sqlerrcode;
    getElevelAndSqlstate(&elevel, &sqlerrcode);
    controller->error_code = sqlerrcode;

    bool valid_appname = true;
    bool valid_cachedmessage = true;

    if ((valid_appname = IsStmtNeedRetryByApplicationName(u_sess->attr.attr_common.application_name)) &&
        IsStmtRetryAvaliable(elevel, sqlerrcode) &&
        (valid_cachedmessage = controller->IsTrackedQueryMessageInfoValid(is_extend_query))) {
        pgstat_report_activity(STATE_RETRYING, NULL);

        /* do some sleep by error type */
        uint8 sleep_level = (controller->retry_times > u_sess->attr.attr_common.max_query_retry_times)
                                ? u_sess->attr.attr_common.max_query_retry_times
                                : controller->retry_times + 1;
        uint32 sleep_duration = 10000000L * sleep_level; /* 10000000L for 10s. */

        TimestampTz start_time = GetCurrentTimestamp();
        SimpleLogToServer(LOG, false, "Try to wait for cluster recovery in %ld s.", sleep_duration / 1000000L);

        t_thrd.int_cxt.InterruptByCN = false;
        do {
            pg_usleep(sleep_duration);

            if (t_thrd.int_cxt.InterruptByCN) {
                long secs;
                int usecs;
                TimestampTz stop_time = GetCurrentTimestamp();

                TimestampDifference(start_time, stop_time, &secs, &usecs);
                uint64 already_sleep_time = secs * 1000000L + usecs + 1000L;
                if (already_sleep_time < sleep_duration) {
                    sleep_duration -= already_sleep_time;
                    SimpleLogToServer(DEBUG2,
                        false,
                        "%s sleep interrupted, remaining %ld s.",
                        PRINT_PREFIX_TYPE_ALERT,
                        sleep_duration / 1000000L);
                    t_thrd.int_cxt.InterruptByCN = false;
                    continue;
                } else
                    break;
            } else
                break;
        } while (1);

        bret = true;
    }

    if (!bret) {
        const char* ecode_str = plpgsql_get_sqlstate(sqlerrcode);
        bool valid_ecode = IsStmtNeedRetryByErrCode(ecode_str, elevel);
        bool is_transblock = IsInTransactionChain(true);
        bool is_unsupported_query = controller->is_unsupported_query_type;
        /* in these situations we skip logging, unsupported application name, unsupported error code, query in a
         * transaction block, stored procedure */
        bool skip_logging = (!valid_appname || !valid_ecode || is_transblock || is_unsupported_query);

        SimpleLogToServer(DEBUG2,
            !(!valid_ecode || is_transblock || is_unsupported_query),
            "retry unsupported (ecode %s)%s%s%s.",
            ecode_str,
            !valid_ecode ? ", unsupported error code" : "",
            is_transblock ? ", query in a transaction block" : "",
            is_unsupported_query ? ", unsupported query type" : "");
        SimpleLogToServer(LOG,
            skip_logging,
            "%s retry not allowed (already retry %d times) %s%s%s%s%s.",
            is_extend_query ? "extend query" : "simple query",
            controller->retry_times,
            controller->retry_times >= u_sess->attr.attr_common.max_query_retry_times ? ", max retry times reached"
                                                                                      : "",
            controller->is_tempfile_size_exceeded ? ", temp file size exceeded" : "",
            !valid_cachedmessage ? ", query statement untracked" : "",
            controller->is_transaction_committed ? ", transaction committed" : "",
            !controller->is_retry_enabled ? ", retry switch off" : "");
    }

    return bret;
}

/*
 * @Description: init statement retry controller, it should be a single instance in a postgres thread
 * @return  - poniter of statement retry controller instance
 */
StatementRetryController* StmtRetryInitController(void)
{
    u_sess->exec_cxt.RetryController = New(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER))
        StatementRetryController(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    return u_sess->exec_cxt.RetryController;
}

void StmtRetryDisable(void)
{
    if (u_sess->exec_cxt.RetryController) {
        u_sess->exec_cxt.RetryController->is_retry_enabled = false;
    }
}

void StmtRetrySetQuerytUnsupportedFlag(void)
{
    if (u_sess->exec_cxt.RetryController) {
        u_sess->exec_cxt.RetryController->is_unsupported_query_type = true;
    }
}

void StmtRetrySetTransactionCommitFlag(bool flag)
{
    if (u_sess->exec_cxt.RetryController) {
        u_sess->exec_cxt.RetryController->is_transaction_committed = flag;
    }
}

/*
 * @Description: check statement retry is enabled or not
 * @return - true, statement retry is already enabled. false, statement retry is already disabled.
 */
bool IsStmtRetryEnabled(void)
{
    if (u_sess->exec_cxt.RetryController)
        return u_sess->exec_cxt.RetryController->IsRetryEnabled();

    return false;
}

/*
 * @Description: check statement retry is retrying or not
 * @return - true, statement retry is doing retry.
 */
bool IsStmtRetryQueryRetrying(void)
{
    if (u_sess->exec_cxt.RetryController)
        return u_sess->exec_cxt.RetryController->IsQueryRetrying();

    return false;
}

/*
 * @Description: reset instance of statement retry controller
 */
void StmtRetryResetController(void)
{
    if (u_sess->exec_cxt.RetryController && u_sess->exec_cxt.RetryController->IsRetryEnabled())
        u_sess->exec_cxt.RetryController->Reset();
}

/*
 * @Description: set flag of file exceeded, which means current tempfile using for cache query result is exceeded.
 */
void StmtRetrySetFileExceededFlag(void)
{
    if (u_sess->exec_cxt.RetryController && u_sess->exec_cxt.RetryController->IsRetryEnabled())
        u_sess->exec_cxt.RetryController->is_tempfile_size_exceeded = true;
}

/*
 * @Description: check whether statement retry conditions is satisfied, if conditions is satisfied then enable statement
                 retry, if not then disable it.
 * @in controller - statement retry controller pointer for validating
 */
void StmtRetryValidate(StatementRetryController* controller)
{
    if (!IS_PGXC_COORDINATOR)
        return;

    if (controller != NULL) {
        if (IS_PGXC_COORDINATOR && IsConnFromApp() &&
            IsStmtNeedRetryByApplicationName(u_sess->attr.attr_common.application_name) && STMT_RETRY_ENABLED) {
            controller->EnableRetry();
        } else {
            controller->DisableRetry();
        }
    }
}

/*
 * -------------------------------------------------------------------------------------------------------------------
 * QueryMessageContext define
 * -------------------------------------------------------------------------------------------------------------------
 */
/*
 * @Description: reset
 */
void QueryMessageContext::Reset(void)
{
    if (cached_msg_.maxlen > DEFAULT_PQ_RECV_BUFFER_SIZE) {
        /* to avoid memory ballooning */
        ereport(DEBUG2,
            (errmodule(MOD_CN_RETRY),
                errmsg("%s caching buffer realloc, buffer size %d", PRINT_PREFIX_TYPE_ALERT, cached_msg_.maxlen)));

        if (cached_msg_.data != NULL) {
            pfree(cached_msg_.data);
            cached_msg_.data = NULL;
            cached_msg_.len = 0;
            cached_msg_.maxlen = 0;
        }

        MemoryContext old_context = MemoryContextSwitchTo(context_);
        initStringInfo(&cached_msg_);
        (void)MemoryContextSwitchTo(old_context);
    } else {
        resetStringInfo(&cached_msg_);
    }
}
/*
 * @Description: cache messages into a buffer, for simple query a solo Q message is cached, for extended query multi
                 PBE messages will be cached.
 * @in msg_type - query message type
 * @in msg_data - query message body
 */
void QueryMessageContext::CacheMessage(char msg_type, StringInfo msg_data)
{
    /* construct message header */
    const size_t msg_header_size = 4;
    char msg_head[5]; /* msg tpye + length */
    msg_head[0] = msg_type;
    uint32 msg_length = msg_header_size + msg_data->len;
    msg_length = htonl(msg_length);

    errno_t rc = memcpy_s(msg_head + 1, msg_header_size, &msg_length, msg_header_size);
    securec_check(rc, "", "");

    /* append message head to buffer */
    appendBinaryStringInfo(&cached_msg_, msg_head, 5);

    /* append message body to buffer */
    appendBinaryStringInfo(&cached_msg_, msg_data->data, msg_data->len);
}
/*
 * @Description: dump cached history messages to PqRecvBuffer
 */
void QueryMessageContext::DumpHistoryCommandsToPqBuffer(void)
{
    /* checking remaining data in command buffer(PqRecvBuffer) */
    if (t_thrd.libpq_cxt.PqRecvPointer < t_thrd.libpq_cxt.PqRecvLength) {
#ifdef USE_RETRY_STUB
        ereport(LOG,
            (errmodule(MOD_CN_RETRY),
                errmsg("%s %s copy remaining data size  %d",
                    STUB_PRINT_PREFIX,
                    STUB_PRINT_PREFIX_TYPE_S,
                    t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer)));
#endif
        appendBinaryStringInfo(&cached_msg_,
            t_thrd.libpq_cxt.PqRecvBuffer + t_thrd.libpq_cxt.PqRecvPointer,
            t_thrd.libpq_cxt.PqRecvLength - t_thrd.libpq_cxt.PqRecvPointer);
    }
    /* enlarge command buffer if necessary */
    if (cached_msg_.len > t_thrd.libpq_cxt.PqRecvBufferSize) {
        pq_resize_recvbuffer(cached_msg_.len);
    }
    /* revert command buffer to histroy commands */
    pq_revert_recvbuffer(cached_msg_.data, cached_msg_.len);
}
/*
 * -------------------------------------------------------------------------------------------------------------------
 * PBEFlowTracker define
 * -------------------------------------------------------------------------------------------------------------------
 */
/*
 * @Description: checking whether message execution track is same or not between retrying and before retring
 * @in type - pbe messsge type
 */
void PBEFlowTracker::Validate(char type)
{
    if (history_pbe_flow_.cursor >= history_pbe_flow_.len || history_pbe_flow_.len == 0)
        return;

    char history_ch = history_pbe_flow_.data[history_pbe_flow_.cursor++];

    if (history_ch != type)
        ereport(ERROR,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("sql retrying routine off the track."),
                errdetail("history executed message :%c, retrying message : %c", history_ch, type)));
}

/*
 * -------------------------------------------------------------------------------------------------------------------
 * StatementRetryController define
 * -------------------------------------------------------------------------------------------------------------------
 */
/*
 * @Description: Reset
 */
void StatementRetryController::Reset(void)
{
    cached_msg_context.Reset();
    pbe_tracker.Reset();

    retry_times = 0;
    error_code = 0;
    is_trans = true;
    is_unsupported_query_type = false;
    is_tempfile_size_exceeded = false;
    is_transaction_committed = false;
    retry_phase = STMT_RETRY_DEFAULT;
    executing_msg_type = INVALID_MESSAGE;
    resetStringInfo(&prepared_stmt_name);
    /* retry_id, is_retry_enabled should not reset here */
}

/*
 * @Description: trigger statement retry. in extend query we reset the PqRecvBuffer, in simple query we cache the
                 debug query string for further command reading.
 * @in is_extend_query - true, meaning extend query. false, simple query
 * @in query_string - query string of simple query
 */
void StatementRetryController::TriggerRetry(bool is_extend_query)
{
    int saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    bool dumpok = true;
    PG_TRY();
    {
        cached_msg_context.DumpHistoryCommandsToPqBuffer();
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        SimpleLogToServer(
            LOG, false, "%s fail to dump history command, unable to retry statement.", PRINT_PREFIX_TYPE_ALERT);
        DisableRetry();
        dumpok = false;
        FlushErrorState();
    }
    PG_END_TRY();

    if (!dumpok)
        return;

    if (is_extend_query) {
        pbe_tracker.InitValidating();

        PG_TRY();
        {
            HandlePreparedStatementsForRetry();
        }
        PG_CATCH();
        {
            t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
            SimpleLogToServer(LOG, false, "%s fail to invalid all prepared statements", PRINT_PREFIX_TYPE_ALERT);
        }
        PG_END_TRY();
    }

    if (retry_phase == STMT_RETRY_DEFAULT)
        ++retry_id; /* if reached here, meaning this is a new tracked query, so update retry id */

    retry_phase = is_extend_query ? STMT_RETRY_EXTEND_QUREY_RETRYING : STMT_RETRY_SIMPLE_QUREY_RETRYING;

    SimpleLogToServer(DEBUG2, false, "%s retry triggered", PRINT_PREFIX_TYPE_PATH);
}

/*
 * @Description: turn on the statement retry, enable tempfile to save query result at the same time.
 */
void StatementRetryController::EnableRetry(void)
{
    if (is_retry_enabled == false) {
        Reset();
    }
    is_retry_enabled = true;
}

/*
 * @Description: turn off the statement retry, disable tempfile to save query result at the same time.
 */
void StatementRetryController::DisableRetry(void)
{
    Reset();
    is_retry_enabled = false;
}

/*
 * @Description: before we do retry, we need to check the message we tracked is valid or enough
                 to retry it, so do some check
 * @in is_extend_query - true, meaning extend query. false, simple query
 * @in query_string - query string of simple query
 * @return - true, meaning tracked query messsage is enough and valid to retry it, if not then false.
 */
bool StatementRetryController::IsTrackedQueryMessageInfoValid(bool is_extend_query)
{
    /* check cached query type */
    if (!IsQueryMessage(executing_msg_type)) {
        SimpleLogToServer(
            LOG, false, "%s unsupported message type \"%c\"", PRINT_PREFIX_TYPE_ALERT, executing_msg_type);
        return false;
    }

    if (!IsMessageTypeMatchWithProtocol(is_extend_query, executing_msg_type)) {
        SimpleLogToServer(LOG,
            false,
            "%s message type \"%c\" dismatch protocol type \"%s\"",
            PRINT_PREFIX_TYPE_ALERT,
            executing_msg_type,
            is_extend_query ? " extend query" : " simple query");
        return false;
    }

    /* check query message data */
    int buffer_size = cached_msg_context.MessageSize();
    if (buffer_size <= 0) {
        SimpleLogToServer(LOG, false, "%s invalid cached message size \"%d\"", PRINT_PREFIX_TYPE_ALERT, buffer_size);
        return false;
    }

    char firstchar = cached_msg_context.PeekFirstChar(); /* in extended query this is the firstchar of the first
                                                          * message in PBE message flow */
    if (!IsMessageTypeMatchWithProtocol(is_extend_query, firstchar)) {
        SimpleLogToServer(LOG,
            false,
            "%s cached message type \"%c\" dismatch protocol type \"%s\"",
            PRINT_PREFIX_TYPE_ALERT,
            firstchar,
            is_extend_query ? "doing extend query" : "doing simple query");
        return false;
    }

    return true;
}

/*
 * @Description: clean some reset of StatementRetryController object once a sql execution finished
 */
void StatementRetryController::FinishRetry(void)
{
    if (t_thrd.libpq_cxt.PqRecvBufferSize > DEFAULT_PQ_RECV_BUFFER_SIZE) {
        if (unlikely(t_thrd.libpq_cxt.PqRecvPointer < t_thrd.libpq_cxt.PqRecvLength)) {
            ereport(LOG,
                (errmodule(MOD_CN_RETRY),
                    errmsg("%s remaining data in command buffer, skip resize.", PRINT_PREFIX_TYPE_ALERT)));
        } else {
            pq_resize_recvbuffer(DEFAULT_PQ_RECV_BUFFER_SIZE);
        }
    }

    if (retry_phase > STMT_RETRY_DEFAULT)
        ereport(DEBUG2, (errmodule(MOD_CN_RETRY), errmsg("%s retry finish.", PRINT_PREFIX_TYPE_PATH)));

    Reset();
}

/*
 * @Description: clean prepared statement
 */
void StatementRetryController::CleanPreparedStmt(void)
{
    if (prepared_stmt_name.len > 0) {
        char* token = NULL;
        char* next_token = NULL;

        token = strtok_s(prepared_stmt_name.data, STMT_TOKEN, &next_token);
        while (token != NULL) {
            ereport(DEBUG2, (errmodule(MOD_CN_RETRY), errmsg("clean prepared statement (%s)for retry.", token)));
            DropPreparedStatement(token, false);

            token = strtok_s(NULL, STMT_TOKEN, &next_token);
        }

        resetStringInfo(&prepared_stmt_name);
    }
}

/*
 * -------------------------------------------------------------------------------------------------------------------
 * StatementRetryStub define
 * -------------------------------------------------------------------------------------------------------------------
 */
/*
 * @Description: start a stub test, just raised a error.
 */
void StatementRetryStub::StartOneStubTest(char msg_type)
{
    if (IsInTransactionChain(true) || !IsStmtNeedRetryByApplicationName(u_sess->attr.attr_common.application_name))
        return;

    if (stub_pos >= RETRY_STUB_CASE_SIZE) {
        ereport(LOG, (errmodule(MOD_CN_RETRY), errmsg("%s invalid stub pos %d", STUB_PRINT_PREFIX_ALERT, stub_pos)));
        return;
    }

    if (stub_marker[stub_pos] == STUB_DO_RETRY) {
        on_stub_test = true;
        stub_marker[stub_pos] = STUB_PASS;
        ereport(ERROR,
            (errmodule(MOD_CN_RETRY),
                errcode(ERRCODE_CN_RETRY_STUB),
                errmsg("%s %s do stub test \"%c\"", STUB_PRINT_PREFIX, STUB_PRINT_PREFIX_TYPE_R, msg_type)));
    } else {
        ++stub_pos;
    }
}

/*
 * @Description: once a stub test is triggerd, use this method to do some reset
 */
void StatementRetryStub::CloseOneStub(void)
{
    stub_pos = 0;
    on_stub_test = false;
}

/*
 * @Description: once a sql execution finished, do some reset and logging
 */
void StatementRetryStub::FinishStubTest(const char* info)
{
    if (!(IsInTransactionChain(true) || !IsStmtNeedRetryByApplicationName(u_sess->attr.attr_common.application_name)))
        ereport(LOG,
            (errmodule(MOD_CN_RETRY),
                errmsg("%s %s stub test result, message track %s, \"%d\" stub test finished",
                    STUB_PRINT_PREFIX,
                    STUB_PRINT_PREFIX_TYPE_R,
                    info,
                    stub_pos)));

    Reset();
}

/*
 * @Description: reset all data for stub
 */
void StatementRetryStub::Reset(void)
{
    errno_t rc = memset_s(
        stub_marker, sizeof(uint8) * RETRY_STUB_CASE_SIZE, STUB_DO_RETRY, sizeof(uint8) * RETRY_STUB_CASE_SIZE);
    securec_check(rc, "", "");

    stub_pos = 0;
    on_stub_test = false;
    ecode_pos = 0;
}

/*
 * @Description: reset all data for stub
 */
void StatementRetryStub::InitECodeMarker(void)
{
    /*
     * YY001 YY002 YY003 YY004 YY005 YY006 YY007 YY008 YY009 YY010 YY011 YY012 YY013 YY014 YY015
     * 53200 08006 08000 57P01 XX003 XX009
     */
    ecode_marker[0] = ERRCODE_CONNECTION_RESET_BY_PEER;
    ecode_marker[1] = ERRCODE_STREAM_CONNECTION_RESET_BY_PEER;
    ecode_marker[2] = ERRCODE_LOCK_WAIT_TIMEOUT;
    ecode_marker[3] = ERRCODE_CONNECTION_TIMED_OUT;
    ecode_marker[4] = ERRCODE_SET_QUERY;
    ecode_marker[5] = ERRCODE_OUT_OF_LOGICAL_MEMORY;
    ecode_marker[6] = ERRCODE_SCTP_MEMORY_ALLOC;
    ecode_marker[7] = ERRCODE_SCTP_NO_DATA_IN_BUFFER;
    ecode_marker[8] = ERRCODE_SCTP_RELEASE_MEMORY_CLOSE;
    ecode_marker[9] = ERRCODE_SCTP_TCP_DISCONNECT;
    ecode_marker[10] = ERRCODE_SCTP_DISCONNECT;
    ecode_marker[11] = ERRCODE_SCTP_REMOTE_CLOSE;
    ecode_marker[12] = ERRCODE_SCTP_WAIT_POLL_UNKNOW;
    ecode_marker[13] = ERRCODE_SNAPSHOT_INVALID;
    ecode_marker[14] = ERRCODE_CONNECTION_RECEIVE_WRONG;
    ecode_marker[15] = ERRCODE_STREAM_REMOTE_CLOSE_SOCKET;
    ecode_marker[16] = ERRCODE_STREAM_DUPLICATE_QUERY_ID;
    ecode_marker[17] = ERRCODE_CONNECTION_FAILURE;
    ecode_marker[18] = ERRCODE_CONNECTION_EXCEPTION;
    ecode_marker[19] = ERRCODE_OUT_OF_MEMORY;
    ecode_marker[20] = ERRCODE_ADMIN_SHUTDOWN;
}

/*
 * @Description: reset all data for stub
 */
void StatementRetryStub::ECodeStubTest(void)
{
    if (u_sess->attr.attr_common.max_query_retry_times != 20) /* use it trigger ecode stub test */
        return;

    if (IsInTransactionChain(true) || !IsStmtNeedRetryByApplicationName(u_sess->attr.attr_common.application_name))
        return;

    if (ecode_pos >= RETRY_STUB_CASE_ECODE_SIZE)
        return;

    on_stub_test = true;
    int ecode = ecode_marker[ecode_pos++];
    const char* ecode_str = plpgsql_get_sqlstate(ecode);
    ereport(ERROR,
        (errmodule(MOD_CN_RETRY),
            errcode(ecode),
            errmsg("%s ecode stub test raise error %s", STUB_PRINT_PREFIX, ecode_str)));
}

/*
 * @Description: reset all data for stub
 */
void StatementRetryStub::ECodeValidate(void)
{
    if (u_sess->attr.attr_common.max_query_retry_times != 20) /* use it trigger ecode stub test */
        return;

    int elevel;
    int sqlerrcode;
    getElevelAndSqlstate(&elevel, &sqlerrcode);

    const char* ecode_str = plpgsql_get_sqlstate(sqlerrcode);
    if (IsStmtNeedRetryByErrCode(sqlerrcode, elevel)) {
        ereport(LOG, (errmodule(MOD_CN_RETRY), errmsg("%s catch error pass %s", STUB_PRINT_PREFIX, ecode_str)));
    } else {
        ereport(LOG, (errmodule(MOD_CN_RETRY), errmsg("%s catch error fail %s", STUB_PRINT_PREFIX, ecode_str)));
    }
}
/* file end stmt_retry.cpp */
