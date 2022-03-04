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
 * threadpool_worker.h
 *     ThreadPoolWorker will get an active session from ThreadPoolListener,
 *     read command from the session and execute the command. This class is
 *     also response to init and free session.
 *
 * IDENTIFICATION
 *        src/include/threadpool/threadpool_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef THREAD_POOL_WORKER_H
#define THREAD_POOL_WORKER_H

#include "lib/dllist.h"
#include "knl/knl_variable.h"

#define THRD_SIGTERM 0x00000001
#define THRD_EXIT 0x00000010
extern void SetThreadLocalGUC(knl_session_context* session);
/*
 * List of active backends (or child processes anyway; we don't actually
 * know whether a given child has become a backend or is still in the
 * authorization phase).  This is used mainly to keep track of how many
 * children we have and send them appropriate signals when necessary.
 *
 * "Special" children such as the startup, bgwriter and autovacuum launcher
 * tasks are not in this list Autovacuum worker and walsender processes are
 * in it. Also, "dead_end" children are in it: these are children launched just
 * for the purpose of sending a friendly rejection message to a would-be
 * client.	We must track them because they are attached to shared memory,
 * but we know they will never become live backends.  dead_end children are
 * not assigned a PMChildSlot.
 */
typedef struct Backend {
    ThreadId pid;       /* process id of backend */
    knl_thread_role role; /* thread role */
    long cancel_key;    /* cancel key for cancels for this backend */
    int child_slot;     /* PMChildSlot for this backend, if any */
    bool is_autovacuum; /* is it an autovacuum process? */
    volatile bool dead_end; /* is it going to send an quit? */
    volatile int flag;
    Dlelem elem; /* list link in BackendList */
} Backend;

typedef enum {
    THREAD_UNINIT = 0,
    THREAD_RUN,
    THREAD_PENDING,
    THREAD_EXIT,  // set by PM , as PM down.
} ThreadStatus;

typedef enum {
    TWORKER_HOLDSESSIONLOCK = 0,
    TWORKER_TWOPHASECOMMIT,
    TWORKER_HOLDLWLOCK,
    TWORKER_GETNEXTXID,
    TWORKER_STILLINTRANS,
    TWORKER_UNCONSUMEMESSAGE,
    TWORKER_CANSEEKNEXTSESSION,
    TWORKER_PREDEADSESSION
} ThreadStayReason;

class ThreadPoolGroup;
class ThreadPoolWorker : public BaseObject {
public:
    ThreadPoolWorker(uint idx, ThreadPoolGroup* group, pthread_mutex_t* mutex, pthread_cond_t* m_cond);
    ~ThreadPoolWorker();
    int StartUp();
    void ShutDown();
    void NotifyReady();
    void WaitMission();
    void CleanUpSession(bool threadexit);
    void CleanUpSessionWithLock();
    bool WakeUpToWork(knl_session_context* session);
    void WakeUpToUpdate(ThreadStatus status);
    bool WakeUpToPendingIfFree();

    friend class ThreadPoolListener;

    inline ThreadPoolGroup* GetGroup()
    {
        return m_group;
    }

    inline ThreadId GetThreadId()
    {
        return m_tid;
    }

    inline void SetSession(knl_session_context* session)
    {
        m_currentSession = session;
    }
    const inline knl_thrd_context *GetThreadContextPtr()
    {
        return m_thrd;
    }


    inline ThreadStatus GetthreadStatus()
    {
        pg_memory_barrier();
        return m_threadStatus;
    }

    static Backend* CreateBackend();
    static void AddBackend(Backend* bn);

public:
    volatile int m_waitState;

private:
    void CleanThread();
    bool AttachSessionToThread();
    void DetachSessionFromThread();
    void WaitNextSession();
    bool InitPort(Port* port);
    void FreePort(Port* port);
    void Pending();
    void ShutDownIfNecessary();
    void RestoreSessionVariable();
    void RestoreThreadVariable();
    void RestoreLocaleInfo();
    void SetSessionInfo();

private:
    ThreadId m_tid;
    uint m_idx;
    knl_session_context* m_currentSession;
    volatile ThreadStatus m_threadStatus;
    ThreadStayReason m_reason;
    Dlelem m_elem;
    ThreadPoolGroup* m_group;
    pthread_mutex_t* m_mutex;
    pthread_cond_t* m_cond;
    knl_thrd_context *m_thrd;
};

#endif /* THREAD_POOL_WORKER_H */
