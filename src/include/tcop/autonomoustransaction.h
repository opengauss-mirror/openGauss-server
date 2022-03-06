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
#include "fmgr.h"
#include "access/tupdesc.h"
#include "utils/atomic.h"


enum PQResult {
    RES_DEFAULT,
    RES_COMMAND_OK,
    RES_SINGLE_TUPLE,
    RES_TUPLES_OK,
    RES_ERROR
};

struct ATResult {
    bool withtuple;
    PQResult result;
    Datum ResTup;
    bool resisnull;

    ATResult() : withtuple(false), result(RES_DEFAULT) {}
    ATResult(bool btuple, PQResult pqres) : withtuple(btuple), result(pqres) {}
};

class AutonomousSession : public BaseObject {
public:
    AutonomousSession() : m_conn(NULL), m_res(NULL) {}

    /* disallow copy */
    AutonomousSession(const AutonomousSession&);
    AutonomousSession& operator=(const AutonomousSession&);

public:
    void Init()
    {
        m_conn = NULL;
        m_res = NULL;
        current_attach_sessionid = 0;
        saved_deadlock_timeout = 0;
        RefSessionCount();
    }

    ATResult ExecSimpleQuery(const char* query, TupleDesc resultTupleDesc, int64 currentXid, bool isLockWait = false,
                                bool is_plpgsql_func_with_outparam = false);

    void DetachSession(void);
    void AttachSession(void);
    bool GetConnStatus(void);
    bool ReConnSession(void);
    void SetDeadLockTimeOut(void);
    void ReSetDeadLockTimeOut(void);

public:
    uint64 current_attach_sessionid = 0;

private:
    void AddSessionCount(void);
    void ReduceSessionCount(void);
    inline void AddRefcount()
    {
        pg_atomic_fetch_add_u32((volatile uint32*)&m_sessioncnt, 1);
    }

    inline void SubRefCount()
    {
        pg_atomic_fetch_sub_u32((volatile uint32*)&m_sessioncnt, 1);
    }

    /* this check function can only be used to change gpc table when has write lock */
    inline bool RefSessionCount()
    {
        /* if is same as 0 then set 0 and return true, if is different then do nothing and return false */
        uint32 expect = 0;
        return pg_atomic_compare_exchange_u32((volatile uint32*)&m_sessioncnt, &expect, 0);
    }

    inline uint32 GetSessionCount()
    {
        return m_sessioncnt;
    }
private:
    PGconn* m_conn = NULL;
    PGresult* m_res = NULL;

    static pg_atomic_uint32 m_sessioncnt;

    int saved_deadlock_timeout = 0;
};

ATResult HandlePGResult(PGconn* conn, PGresult* pgresult, TupleDesc resultTupleDesc, bool is_plpgsql_func_with_outparam);

enum PLpgSQL_exectype {
    STMT_SQL,
    STMT_PERFORM,
    STMT_DYNAMIC,
    STMT_UNKNOW
};
struct PLpgSQL_execstate;
struct PLpgSQL_stmt_block;
struct PLpgSQL_expr;

bool IsAutonomousTransaction(bool IsAutonomous);

void CreateAutonomousSession(void);
void DestoryAutonomousSession(bool force);

#endif /* AUTONOMOUSTRANSACTION_H */
