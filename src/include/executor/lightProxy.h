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
 * lightProxy.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/lightProxy.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_EXECUTOR_LIGHTPROXY_H_
#define SRC_INCLUDE_EXECUTOR_LIGHTPROXY_H_

#include "commands/prepare.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "pgxc/pgxcnode.h"
#include "utils/plancache.h"

#define BIND_MESSAGE 1
#define DESC_MESSAGE 2
#define EXEC_MESSAGE 3

#ifdef USE_ASSERT_CHECKING
#ifndef LPROXY_DEBUG
#define LPROXY_DEBUG(A) A
#endif
#else
#ifndef LPROXY_DEBUG
#define LPROXY_DEBUG(A) /* A */
#endif
#endif

/* check whether query executed through light proxy or not*/
extern bool IsLightProxyOn(void);
extern void GPCDropLPIfNecessary(const char *stmt_name, bool need_drop_dnstmt,
                                 bool need_del, CachedPlanSource *reset_plan);
void GPCFillMsgForLp(CachedPlanSource* psrc);

CmdType set_command_type_by_commandTag(const char* commandTag);

typedef enum LightUnSupportType {
    CTRL_DISABLE = 0,
    ENCODE_UNSUPPORT,
    CURSOR_UNSUPPORT,
    REMOTE_UNSUPPORT,
    CMD_UNSUPPORT,
    FOREIGN_UNSUPPORT,
    STATEMENT_UNSUPPORT,
    USERTYPE_UNSUPPORT,
    NODE_NAME_UNSUPPORT,
    MAX_UNSUPPORT_TYPE,
} LightUnSupportType;

/* like in RemoteQueryState */
struct lightProxyErrData {
    bool hasError; /* indicate whether some error occurs */

    int errorCode;                 /* error code to send back to client */
    char* errorMessage;            /* error message to send back to client */
    char* errorDetail;             /* error detail to send back to client */
    char* errorContext;            /* error context to send back to client */
    bool is_fatal_error;           /* mark if it is a FATAL error */
    RemoteErrorData remoteErrData; /* error data from remote */
};

struct lightProxyMsgCtl {
    bool cnMsg;
    bool sendDMsg;
    bool hasResult;
    lightProxyErrData* errData;

    int* process_count;
    uint64 relhash;
    bool has_relhash;

#ifdef LPROXY_DEBUG
    char* stmt_name;
    const char* query_string;
#endif
};

typedef struct stmtLpObj {
    char stmtname[NAMEDATALEN];
    lightProxy *proxy;
} stmtLpObj;

typedef struct lightProxyNamedObj {
    char portalname[NAMEDATALEN];
    lightProxy *proxy;
} lightProxyNamedObj;

class lightProxy : public BaseObject {
public:
    // constructor.
    lightProxy(MemoryContext context, CachedPlanSource *psrc, const char *portalname, const char *stmtname);

    lightProxy(Query *query);

    ~lightProxy();

    static ExecNodes *checkLightQuery(Query *query);

    // check if enable router for lightproxy
    static ExecNodes *checkRouterQuery(Query *query);

    // after bind we need to set this for discrible and execute
    static void setCurrentProxy(lightProxy *proxy);

    // process B/D/E message
    static bool processMsg(int msgType, StringInfo msg);

    static void initStmtHtab();

    static void initlightProxyTable();

    static lightProxy *locateLpByStmtName(const char *stmtname);

    static lightProxy *locateLightProxy(const char *portalname);

    static lightProxy *tryLocateLightProxy(StringInfo msg);
    // run with simple query message
    void runSimpleQuery(StringInfo exec_message);

    // run with batch message
    int runBatchMsg(StringInfo batch_message, bool sendDMsg, int batch_count);

    static void tearDown(lightProxy *proxy);

    void storeLpByStmtName(const char *stmtname);

    void storeLightProxy(const char *portalname);

    void removeLpByStmtName(const char *stmtname);

    static void removeLightProxy(const char* portalname);

    static bool isDeleteLimit(const Query *query);

public:
    CachedPlanSource *m_cplan;

    int m_nodeIdx;

    MemoryContext m_context;

    /* whether row trigger can shippable. */
    bool m_isRowTriggerShippable;

    const char *m_stmtName;

    const char *m_portalName;

    int16 *m_formats;

    DatanodeStatement *m_entry;

protected:
    // saveMsg
    void saveMsg(int msgType, StringInfo msg);
    // get result format from msg
    void getResultFormat(StringInfo message);

    void assemableMsg(char msgtype, StringInfo msgBuf, bool trigger_ship = false);

    // run with execute message
    void runMsg(StringInfo exec_message);

    void connect();

    void handleResponse();

    void sendParseIfNecessary();

    void proxyNodeBegin(bool is_read_only);

    CmdType m_cmdType;
    CmdType queryType;

private:
    Query *m_query;

    PGXCNodeHandle *m_handle;

    StringInfoData m_bindMessage;

    StringInfoData m_describeMessage;

    lightProxyMsgCtl *m_msgctl;
};

extern bool exec_query_through_light_proxy(List* querytree_list, Node* parsetree, bool snapshot_set, StringInfo msg,
                                           MemoryContext OptimizerContext);

#endif /* SRC_INCLUDE_EXECUTOR_LIGHTPROXY_H_ */
