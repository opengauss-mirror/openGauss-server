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
 * threadpool_sessctl.h
 *     Class ThreadPoolSessControl is used to control all session info in thread pool.
 * 
 * IDENTIFICATION
 *        src/include/threadpool/threadpool_sessctl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef THREAD_POOL_SESSION_CONTROL_H
#define THREAD_POOL_SESSION_CONTROL_H

#include "storage/procsignal.h"
#include "knl/knl_variable.h"
#include "utils/tuplestore.h"

#define MEMORY_CONTEXT_NAME_LEN 64
#define PROC_NAME_LEN 64
#define NUM_SESSION_MEMORY_DETAIL_ELEM 8
#define NUM_THREAD_MEMORY_DETAIL_ELEM 9
#define NUM_SHARED_MEMORY_DETAIL_ELEM 6


typedef struct knl_sess_control {
    Dlelem elem;
    int idx;  // session array index;
    knl_session_context* sess;
    volatile sig_atomic_t lock;
} knl_sess_control;

class ThreadPoolSessControl : public BaseObject {
public:
    ThreadPoolSessControl(MemoryContext context);
    ~ThreadPoolSessControl();
    knl_session_context* CreateSession(Port* port);
    knl_sess_control* AllocateSlot(knl_session_context* sc);
    void FreeSlot(int ctrl_index);
    void MarkAllSessionClose();
    int SendSignal(int ctrl_index, int signal);
    void SendProcSignal(int ctrl_index, ProcSignalReason reason, uint64 query_id);
    int CountDBSessions(Oid dbId);
    int CountDBSessionsNotCleaned(Oid dbOid, Oid userOid);
    int CleanDBSessions(Oid dbOid, Oid userOid);
    bool ValidDBoidAndUseroid(Oid dbOid, Oid userOid, knl_sess_control* ctrl);
    void SigHupHandler();
    void HandlePoolerReload();
    void CheckSessionTimeout();
    void CheckPermissionForSendSignal(knl_session_context* sess, sig_atomic_t* lock);
    void getSessionMemoryDetail(Tuplestorestate* tupStore, TupleDesc tupDesc, knl_sess_control** sess);
    void getSessionClientInfo(Tuplestorestate* tupStore, TupleDesc tupDesc);
    void getSessionMemoryContextInfo(const char* ctx_name, StringInfoData* buf, knl_sess_control** sess);
    knl_session_context* GetSessionByIdx(int idx);
    int FindCtrlIdxBySessId(uint64 id);
    TransactionId ListAllSessionGttFrozenxids(int maxSize, ThreadId *pids, TransactionId *xids, int *n);
    bool IsActiveListEmpty();
    void releaseLockIfNecessary();
    inline int GetActiveSessionCount()
    {
        return m_activeSessionCount;
    }

private:
    void recursiveSessMemCxt(
        knl_session_context* sess, const MemoryContext context, Tuplestorestate* tupStore, TupleDesc tupDesc);
    void calculateSessMemCxtStats(
        knl_session_context* sess, const MemoryContext context, Tuplestorestate* tupStore, TupleDesc tupDesc);
    void calculateClientInfo(
        knl_session_context* sess, Tuplestorestate* tupStore, TupleDesc tupDesc);

    inline bool IsValidCtrlIndex(int ctrl_index)
    {
        if (ctrl_index < m_maxReserveSessionCount || ctrl_index - m_maxReserveSessionCount >= m_maxActiveSessionCount) {
            ereport(WARNING,
                (errmsg("Invalid session control index %d, "
                        "maxReserveSessionCount %d, m_maxActiveSessionCount %d",
                    ctrl_index,
                    m_maxReserveSessionCount,
                    m_maxActiveSessionCount)));
            return false;
        } else {
            return true;
        }
    }

private:
    /* session id generate */
    uint64 m_sessionId;
    /* current session count. */
    volatile int m_activeSessionCount;
    /* max session count we can accept. */
    int m_maxActiveSessionCount;
    int m_maxReserveSessionCount;
    /* session control structure. */
    knl_sess_control* m_base;
    Dllist m_freelist;
    Dllist m_activelist;
    pthread_mutex_t m_sessCtrlock;

    MemoryContext m_context;
};

#endif /* THREAD_POOL_SESSION_CONTROL_H */
