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
 * autonomoustransaction.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/autonomoustransaction.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AUTONOMOUSTRANSACTION_H
#define AUTONOMOUSTRANSACTION_H

#include "c.h"
#include "datatypes.h"
#include "utils/palloc.h"
#include "libpq/libpq-fe.h"
#include "storage/spin.h"

enum PQResult {
    RES_DEFAULT,
    RES_COMMAND_OK,
    RES_SINGLE_TUPLE,
    RES_TUPLES_OK,
    RES_ERROR
};

enum PQFormat {
    PQ_FORMAT_TEXT,
    PQ_FORMAT_BINARY
};

struct PQ_ParamInfo {
    int     nparams;
    Oid*    paramtypes;
    char**  paramvalues;
    int*    paramlengths;
    int*    paramformats;
};

class ATManager {
public:
    ATManager() : m_sessioncnt(0)
    {
        SpinLockInit(&m_lock);
    }

    bool AddSession(void);
    void RemoveSession(void);

private:
    slock_t m_lock;
    uint32 m_sessioncnt;
};

extern ATManager g_atManager;

struct ATResult {
    bool withtuple;
    PQResult result;

    ATResult() : withtuple(false), result(RES_DEFAULT) {}
    ATResult(bool btuple, PQResult pqres) : withtuple(btuple), result(pqres) {}
};

class AutonomousSession : public BaseObject {
public:
    AutonomousSession(ATManager* manager) : m_conn(NULL), m_manager(&g_atManager), m_res(NULL) {}

    /* disallow copy */
    AutonomousSession(const AutonomousSession&);
    AutonomousSession& operator=(const AutonomousSession&);

public:
    void Init(ATManager* manager)
    {
        m_conn = NULL;
        m_manager = manager;
        m_res = NULL;
        m_refcount = 0;
    }

    ATResult ExecSimpleQuery(const char* query);
    ATResult ExecQueryWithParams(const char* query, PQ_ParamInfo* pinfo);
    // attach with a plpgsql block, call this before using AutonomousSession object.
    void Attach(void);
    // detach with a plpgsql block, call this before quiting block.
    void Detach(void);

    void CloseSession(void);

protected:
    /* create a new session using libpq */
    void CreateSession(void);
    void CreateSession(const char* conninfo);

private:
    PGconn* m_conn;
    ATManager* m_manager;
    PGresult* m_res;
    uint32 m_refcount;
};

ATResult HandlePGResult(PGconn* conn, PGresult* pgresult);

enum PLpgSQL_exectype {
    STMT_SQL,
    STMT_PERFORM,
    STMT_DYNAMIC,
    STMT_UNKNOW
};
struct PLpgSQL_execstate;
struct PLpgSQL_stmt_block;
struct PLpgSQL_expr;

bool IsAutonomousTransaction(const PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block);
bool IsValidAutonomousTransaction(const PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block);
bool IsValidAutonomousTransactionQuery(PLpgSQL_exectype exectype, const PLpgSQL_expr* stmtexpr, bool isinto);

void AttachToAutonomousSession(PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block);
void DetachToAutonomousSession(const PLpgSQL_execstate* estate);

void InitPQParamInfo(PQ_ParamInfo* pinfo, int n);
void FreePQParamInfo(PQ_ParamInfo* pinfo);


#endif /* AUTONOMOUSTRANSACTION_H */
