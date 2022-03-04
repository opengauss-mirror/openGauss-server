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
 * stmt_retry.h
 *        support for statement retry
 * 
 * 
 * IDENTIFICATION
 *        src/include/tcop/stmt_retry.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STMT_RETRY_H
#define STMT_RETRY_H

#include "c.h"

#include "lib/stringinfo.h"
#include "utils/palloc.h"
#include "utils/elog.h"
#include "utils/hsearch.h"

/*
 * test stub usage
 */
const uint16 RETRY_STUB_CASE_SIZE = 256;
const uint16 RETRY_STUB_CASE_ECODE_SIZE = 21;

typedef enum { STUB_DO_RETRY, STUB_PASS } RETRY_STUB_MARKER;

/*
 * log usage
 */
const char* const STUB_PRINT_PREFIX = " [retry stub] ";
const char* const STUB_PRINT_PREFIX_ALERT = " [retry stub alert] ";
const char* const STUB_PRINT_PREFIX_TYPE_S = " ~special path~ ";
const char* const STUB_PRINT_PREFIX_TYPE_R = " ~retry path~ ";
const char* const PRINT_PREFIX_TYPE_ALERT = " alert : ";
const char* const PRINT_PREFIX_TYPE_PATH = " retry path : ";

/*
 * marco for statement retry
 */
#define STMT_RETRY_ENABLED \
    (u_sess->attr.attr_common.max_query_retry_times > 0 && !u_sess->attr.attr_sql.enable_hadoop_env)

/*message types which retry is supported*/
const char Q_MESSAGE = 'Q';
const char P_MESSAGE = 'P';
const char B_MESSAGE = 'B';
const char E_MESSAGE = 'E';
const char D_MESSAGE = 'D';
const char C_MESSAGE = 'C';
const char S_MESSAGE = 'S';
const char U_MESSAGE = 'U';
const char F_MESSAGE = 'F';
const char INVALID_MESSAGE = '0';

const char* const STMT_TOKEN = ";";

inline bool IsExtendQueryMessage(char msg_type)
{
    return ((msg_type == P_MESSAGE) || (msg_type == B_MESSAGE) || (msg_type == E_MESSAGE) || (msg_type == D_MESSAGE) ||
               (msg_type == C_MESSAGE) || (msg_type == S_MESSAGE) || (msg_type == U_MESSAGE) || (msg_type == F_MESSAGE))
               ? true
               : false;
}

inline bool IsSimpleQueryMessage(char msg_type)
{
    return (msg_type == Q_MESSAGE);
}

inline bool IsQueryMessage(char msg_type)
{
    return (IsSimpleQueryMessage(msg_type) || IsExtendQueryMessage(msg_type));
}

inline bool IsMessageTypeMatchWithProtocol(bool is_extend_query, char msg_type)
{
    return is_extend_query ? IsExtendQueryMessage(msg_type) : IsSimpleQueryMessage(msg_type);
}

/*
 * QueryMessageContext is used to cache retry message context info for now
 * in the future.
 */
class QueryMessageContext : public BaseObject {
public:
    QueryMessageContext(MemoryContext mem_context) : context_(mem_context)
    {

        MemoryContext old_context = MemoryContextSwitchTo(context_);
        initStringInfo(&cached_msg_);
        (void)MemoryContextSwitchTo(old_context);
    }

    ~QueryMessageContext(void)
    {}

public:
    void CacheMessage(char msg_type, StringInfo msg_data);

    int MessageSize(void) const
    {
        return cached_msg_.len;
    }

    void DumpHistoryCommandsToPqBuffer(void);

    void Reset(void);
    char PeekFirstChar(void) const
    {
        return cached_msg_.data[0];
    };

private:
    /* disallow copy*/
    QueryMessageContext(const QueryMessageContext&);
    QueryMessageContext& operator=(const QueryMessageContext&);

private:
    StringInfoData cached_msg_; /* If 'P' 'B' 'E''D', msg cached from input_message, If 'Q' cached from
                                 * debug_query string */
    MemoryContext context_;
};

typedef enum {
    STMT_RETRY_DEFAULT,               /*default status of retry, meaning retry is not triggered*/
    STMT_RETRY_SIMPLE_QUREY_RETRYING, /*indicate we are in simple query retry flow*/
    STMT_RETRY_EXTEND_QUREY_RETRYING, /*indicate we are in pbe retry flow*/
} StatementRetryPhase;

typedef StatementRetryPhase CombinedStatementRetryPhase;

typedef struct MessageInfo {
    char type;
    int len;

    MessageInfo(char _type, int _len) : type(_type), len(_len)
    {}
} MessageInfo;

/*
 * since pbe messages in extend query executed in a continues way, for example messages flow can be
 * combination of "p-b-d-e-b-e-s", in extend query retry, we treat pbe message flow as a whole,
 * PBEFlowTracker track pbe flow and using tracked pbe flow info to check retrying pbe execution if
 * retry happens.
 */
class PBEFlowTracker : public BaseObject {
public:
    PBEFlowTracker(MemoryContext mem_context)
    {
        MemoryContext old_context = MemoryContextSwitchTo(mem_context);

        initStringInfo(&pbe_flow_);
        initStringInfo(&history_pbe_flow_);

        (void)MemoryContextSwitchTo(old_context);
    }
    ~PBEFlowTracker(void)
    {}

    void Reset(void)
    {
        resetStringInfo(&pbe_flow_);
        resetStringInfo(&history_pbe_flow_);
    }

public:
    void TrackPBE(char type)
    {
        appendStringInfoChar(&pbe_flow_, type);
    }

    void InitValidating(void)
    {
        if (pbe_flow_.len > 0) {
            copyStringInfo(&history_pbe_flow_, &pbe_flow_);
            resetStringInfo(&pbe_flow_);
        }
    }
    void Validate(char type);

    const char* PBEFlowStr(void)
    {
        return pbe_flow_.data;
    }

private:
    StringInfoData pbe_flow_;         /* recording executed pbe message flow */
    StringInfoData history_pbe_flow_; /* recording executed pbe message flow before retrying */
};

/*
 * stub for statement retry, use it to Manually construct sql retry both in extended query and simple query
 */
class StatementRetryStub {
public:
    StatementRetryStub(void)
    {
        InitECodeMarker();
        Reset();
    }
    ~StatementRetryStub(void)
    {}

    void Reset(void);

public:
    void StartOneStubTest(char msg_type);
    bool OnStubTest(void) const
    {
        return on_stub_test;
    }
    void CloseOneStub(void);
    void FinishStubTest(const char* info);

    void InitECodeMarker(void);
    void ECodeStubTest(void);
    void ECodeValidate(void);

private:
    /* disallow copy*/
    StatementRetryStub(const StatementRetryStub&);
    StatementRetryStub& operator=(const StatementRetryStub&);

private:
    uint8 stub_marker[RETRY_STUB_CASE_SIZE];
    int ecode_marker[RETRY_STUB_CASE_ECODE_SIZE];
    uint16 stub_pos;
    uint16 ecode_pos;
    bool on_stub_test;
};

/*
 * this is the main class of statement retry control
 * 1.A StatementRetryController will track a simple query or a extend query(PBE flow)
 *   during query execution, tracking behave like cache query string in simple query,
 *   record p message pointer in extend query.
 * 2.once query retry is triggerd, StatementRetryController will control the retry routine,
 *   such as how to settle the read/send buffer between cn and client, how to ReadCommand and so on
 */
class StatementRetryController : public BaseObject {
public:
    StatementRetryController(MemoryContext mem_context)
        : retry_id(0), is_retry_enabled(false), cached_msg_context(mem_context), pbe_tracker(mem_context)
    {

        MemoryContext old_context = MemoryContextSwitchTo(mem_context);
        initStringInfo(&prepared_stmt_name);
        (void)MemoryContextSwitchTo(old_context);

        Reset();
    }
    ~StatementRetryController(void)
    {}

    void Reset(void);

public:
    /* common usage */
    void EnableRetry(void);
    void DisableRetry(void);
    bool IsRetryEnabled(void)
    {
        return is_retry_enabled;
    }
    bool IsExtendQueryRetrying(void) const
    {
        return STMT_RETRY_EXTEND_QUREY_RETRYING == retry_phase;
    }
    bool IsSimpleQueryRetrying(void) const
    {
        return STMT_RETRY_SIMPLE_QUREY_RETRYING == retry_phase;
    }
    bool IsQueryRetrying(void) const
    {
        return (IsExtendQueryRetrying() || IsSimpleQueryRetrying());
    }
    bool IsTrackedQueryMessageInfoValid(bool is_extend_query);
    void TriggerRetry(bool is_extend_query);
    void FinishRetry(void);
    void CleanPreparedStmt(void);

    void LogTraceInfo(MessageInfo info)
    {
        if (retry_phase > STMT_RETRY_DEFAULT) {
            ereport(DEBUG3,
                (errmodule(MOD_CN_RETRY), errmsg("retrying \"%c\" message, length \"%d\"", info.type, info.len)));
        } else {
            ereport(DEBUG3,
                (errmodule(MOD_CN_RETRY), errmsg("executing \"%c\" message, length \"%d\"", info.type, info.len)));
        }
    }

    /* message caching */
    void CacheCommand(char type, StringInfo data)
    {
        cached_msg_context.CacheMessage(type, data);
    }
    void TrackMessageOnExecuting(char type)
    {
#if USE_ASSERT_CHECKING
        if (IsExtendQueryMessage(type))
            pbe_tracker.TrackPBE(type);
#endif
        executing_msg_type = type;
    }
    void CacheStmtName(const char* stmt_name)
    {
        appendStringInfoString(&prepared_stmt_name, stmt_name);
        appendStringInfoString(&prepared_stmt_name, STMT_TOKEN);
    }
    char MessageOnExecuting(void) const
    {
        return executing_msg_type;
    }

    /* pbe flow tracking */
    void ValidateExecuting(char type)
    {
        pbe_tracker.Validate(type);
    }
    const char* PBEFlowStr(void)
    {
        return pbe_tracker.PBEFlowStr();
    }

public:              /*in order to compatible with legacy code, declaring all data member as public here */
    uint64 retry_id; /* id for retrying query */
    int retry_times; /* mark current retry times */
    int error_code;  /* recording error code that trigger long jump */
    bool is_trans;   /* if the query within either a transaction or a transaction block,
                      * it is not allowed to retry */
    bool is_tempfile_size_exceeded; /* if true, meaning tempfile for caching query result is exceeded,
                                     * so there is no way to make sure the query result can be exact,
                                     * in this situation retry can not be allowed */
    bool is_unsupported_query_type; /* if true, meaning query type can't retry for now */
    bool is_retry_enabled;          /* if true, meaning retry feature is enabled,
                                     * now StatementRetryController will track query and trigger retry at long jump point */
    bool is_transaction_committed;  /* if true, meaning current transaction is committed, if error happens between
                                       transaction  committed and finish, we disallow retry during this period */
    CombinedStatementRetryPhase retry_phase; /* recording current retry phase */
    QueryMessageContext cached_msg_context;  /* cached statement context store statement info of a statement to
                                              * be retry subsequently */

#ifdef USE_RETRY_STUB
    StatementRetryStub stub_; /* test stub for auto trigger sql retry */
#endif

private:
    char executing_msg_type;           /* tracking msg on executing */
    PBEFlowTracker pbe_tracker;        /* tracking pbe flow execution */
    StringInfoData prepared_stmt_name; /* caching prepared statement name */
};

/*
 *statement retry common usage
 */
extern StatementRetryController* StmtRetryInitController(void);
extern void StmtRetryResetController(void);
extern void StmtRetrySetFileExceededFlag(void);
extern void StmtRetrySetQuerytUnsupportedFlag(void);
extern void StmtRetrySetTransactionCommitFlag(bool flag);
extern void StmtRetryValidate(StatementRetryController* controller);
extern void StmtRetryDisable(void);

extern bool IsStmtRetryEnabled(void);
extern bool IsStmtRetryQueryRetrying(void);
extern bool IsStmtRetryAvaliable(int elevel, int sqlErrCode);
extern bool IsStmtRetryCapable(StatementRetryController* controller, bool is_extend_query);
extern bool IsStmtNeedRetryByErrCode(const char* ecode_str, int errlevel);

/*
 * lnline functions
 */

/*
 * @Description: check if the sql statement need to retry by error code.
 * @in ecode - error code type
 * @in elevel - elevel type
 * @return - true, statement retry is needed by error code.
 */
inline bool IsStmtNeedRetryByErrCode(int ecode, int errlevel)
{
    const char* ecode_str = plpgsql_get_sqlstate(ecode);
    return IsStmtNeedRetryByErrCode(ecode_str, errlevel);
}

/*
 * @Description: filter statement retry by application name, filtering sql from application such as cm_agent and
                 gs_clean, not to retry.
 * @in name - application name
 * @return
 */
inline bool IsStmtNeedRetryByApplicationName(const char* name)
{
    return ((name != NULL) && (strcmp(name, "cm_agent") != 0) && (strcmp(name, "gs_clean") != 0) &&
            (strcmp(name, "gc_fdw") != 0));
}

#endif

/*file end stmt_retry.h */
