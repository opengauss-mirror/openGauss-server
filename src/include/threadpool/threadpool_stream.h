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
 * -------------------------------------------------------------------------
 *
 * threadpool_stream.h
 *
 * IDENTIFICATION
 *	 src/include/threadpool/threadpool_stream.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef THREAD_POOL_STREAM_H
#define THREAD_POOL_STREAM_H

#include "knl/knl_variable.h"

class ThreadPoolStream : public BaseObject {
public:
    ThreadPoolStream();
    ~ThreadPoolStream();
    int StartUp(int idx, StreamProducer* producer, ThreadPoolGroup* group,
                                pthread_mutex_t* mutex, pthread_cond_t* cond);
    void WaitMission();
    void WakeUpToWork(StreamProducer* producer);
    void WakeUpToUpdate(ThreadStatus status);
    void CleanUp();
    void ShutDown();

    inline ThreadPoolGroup* GetGroup()
    {
        return m_group;
    }

    inline ThreadId GetThreadId()
    {
        return m_tid;
    }

    inline StreamProducer* GetProducer()
    {
        return m_producer;
    }

private:
    void InitStream();

private:
    int m_idx;
    ThreadId m_tid;
    Dlelem m_elem;
    ThreadStatus m_threadStatus;
    StreamProducer* m_producer;
    ThreadPoolGroup* m_group;

    pthread_mutex_t* m_mutex;
    pthread_cond_t* m_cond;
};

#endif