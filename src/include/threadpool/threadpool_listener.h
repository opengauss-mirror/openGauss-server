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
 * threadpool_listener.h
 *     Listener thread epoll all connections belongs to this thread group, and
 *     dispatch active session to a free worker.
 * 
 * IDENTIFICATION
 *        src/include/threadpool/threadpool_listener.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef THREAD_POOL_LISTENER_H
#define THREAD_POOL_LISTENER_H

#include <signal.h>
#include "lib/dllist.h"
#include "knl/knl_variable.h"

class ThreadPoolListener : public BaseObject {
public:
    ThreadPoolGroup* m_group;
    volatile bool m_reaperAllSession;

    ThreadPoolListener(ThreadPoolGroup* group);
    ~ThreadPoolListener();
    int StartUp();
    void CreateEpoll();
    void NotifyReady();
    bool TryFeedWorker(ThreadPoolWorker* worker);
    void AddNewSession(knl_session_context* session);
    void WaitTask();
    void DelSessionFromEpoll(knl_session_context* session);
    void RemoveWorkerFromList(ThreadPoolWorker* worker);
    void AddEpoll(knl_session_context* session);
    void SendShutDown();
    void ReaperAllSession();
    void ShutDown() const;
    bool GetSessIshang(instr_time* current_time, uint64* sessionId);

    inline ThreadPoolGroup* GetGroup()
    {
        return m_group;
    }
    inline ThreadId GetThreadId()
    {
        return m_tid;
    }
    inline void ResetThreadId()
    {
        m_tid = 0;
    }
private:
    void HandleConnEvent(int nevets);
    knl_session_context* GetSessionBaseOnEvent(struct epoll_event* ev);
    void DispatchSession(knl_session_context* session);

private:
    ThreadId m_tid;
    int m_epollFd;
    struct epoll_event* m_epollEvents;

    DllistWithLock* m_freeWorkerList;
    DllistWithLock* m_readySessionList;
    DllistWithLock* m_idleSessionList;
};

#endif /* THREAD_POOL_LISTENER_H */
