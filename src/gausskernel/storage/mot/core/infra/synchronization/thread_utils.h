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
 * thread_utils.h
 *    Utility classes for managing threads (notifier, context, etc).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/thread_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_THREAD_UTILS_H
#define MOT_THREAD_UTILS_H

#include <stdint.h>
#include <mutex>
#include <condition_variable>

namespace MOT {
typedef bool (*ThreadNotifierWaitPredicate)(void* obj);

class ThreadNotifier {
public:
    enum ThreadState : uint32_t { TERMINATE = 0, SLEEP = 1, ACTIVE = 2 };

    ThreadNotifier(ThreadState state = ThreadState::SLEEP) : m_state(state)
    {}

    ~ThreadNotifier()
    {}

    void SetState(ThreadState state)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_state = state;
    }

    ThreadState GetState() const
    {
        return m_state;
    }

    void Notify(ThreadState state)
    {
        SetState(state);
        m_cv.notify_all();
    }

    ThreadState Wait(ThreadNotifierWaitPredicate pred, void* obj)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [pred, obj] { return pred(obj); });
        return m_state;
    }

private:
    volatile ThreadState m_state;
    std::mutex m_mutex;
    std::condition_variable m_cv;
};

class ThreadContext {
public:
    ThreadContext() : m_ready(false), m_error(false)
    {}

    virtual ~ThreadContext()
    {
        m_ready.store(false);
        m_error = false;
    }

    void SetReady()
    {
        m_ready.store(true);
    }

    bool IsReady() const
    {
        return m_ready.load();
    }

    virtual void SetError()
    {
        m_error.store(true);
    }

    virtual bool IsError()
    {
        return m_error.load();
    }

    static constexpr uint32_t THREAD_START_WAIT_US = 1000;
    static constexpr uint32_t THREAD_START_TIMEOUT_US = 1000 * 1000 * 60;  // 60 sec
    static constexpr uint32_t THREAD_SLEEP_TIME_US = 100;
    static constexpr uint32_t THREAD_NAME_LEN = 64;

private:
    std::atomic<bool> m_ready;
    std::atomic<bool> m_error;
};

inline bool WaitForThreadStart(ThreadContext* thrdContext)
{
    uint32_t rounds = 0;
    uint32_t maxRounds = ThreadContext::THREAD_START_TIMEOUT_US / ThreadContext::THREAD_START_WAIT_US;
    while (true) {
        if (thrdContext->IsReady()) {
            return true;
        }
        if (thrdContext->IsError()) {
            return false;
        }
        if (rounds++ > maxRounds) {
            break;
        }
        (void)usleep(ThreadContext::THREAD_START_WAIT_US);
    }
    return false;
}
}  // namespace MOT

#endif /* MOT_THREAD_UTILS_H */
