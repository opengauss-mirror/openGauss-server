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
 * ctxctl.h
 *     definitions for context control functions
 * 
 * IDENTIFICATION
 *        src/include/workload/ctxctl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CTXCTL_H
#define CTXCTL_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "gstrace/gstrace_infra.h"

#define THREADID (gettid())
#define PROCESSID (getpid())

#define DUP_ARRAY_POINTER(p, n) pobjdup(p, n)
#define DUP_POINTER(p) DUP_ARRAY_POINTER(p, 1)

/* use a mutext lock */
#define USE_CONTEXT_LOCK(mutex)          \
    WLMContextLock sp_mutex_lock(mutex); \
    sp_mutex_lock.Lock()
/* use a lwlock */
#define USE_AUTO_LWLOCK(lockid, lockmode)      \
    WLMAutoLWLock sp_lwlock(lockid, lockmode); \
    sp_lwlock.AutoLWLockAcquire()
/* use a memory context */
#define USE_MEMORY_CONTEXT(context) \
    WLMContextGuard spcxt((CleanHandler)MemoryContextSwitchTo, MemoryContextSwitchTo(context))
/* use a smart context guard */
#define USE_CONTEXT_GUARD(handler, object) WLMContextGuard sp_guard((CleanHandler)handler, object)
/* make ptr a smart pointer */
#define MAKE_SMART_POINTER(ptr) USE_CONTEXT_GUARD(pfree, ptr)

#define RELEASE_CONTEXT_LOCK() sp_mutex_lock.UnLock()
#define RELEASE_AUTO_LWLOCK() sp_lwlock.AutoLWLockRelease()
#define REVERT_MEMORY_CONTEXT() spcxt.handle()

#define USE_LOCK_TO_ADD(mutex, src, inc) \
    do {                                 \
        USE_CONTEXT_LOCK(&mutex);        \
        src += inc;                      \
        if (src < 0)                     \
            src = 0;                     \
    } while (0);

#define HANDLE_CONTEXT_GUARD() sp_guard.handle()

#define IS_MUTEX_HELD(mutex, pid) ((mutex)->__data.__owner == pid)

#define securec_check_errval(errno, express, elevel)                                                                   \
    do {                                                                                                               \
        errno_t resno = errno;                                                                                         \
        if (EOK != resno) {                                                                                            \
            express;                                                                                                   \
            switch (resno) {                                                                                           \
                case EINVAL:                                                                                           \
                    elog(elevel,                                                                                       \
                        "%s : %d : The destination buffer is NULL or not terminated. The second case only occures in " \
                        "function strcat_s/strncat_s.",                                                                \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    break;                                                                                             \
                case EINVAL_AND_RESET:                                                                                 \
                    elog(elevel, "%s : %d : The Source Buffer is NULL.", __FILE__, __LINE__);                          \
                    break;                                                                                             \
                case ERANGE:                                                                                           \
                    elog(elevel,                                                                                       \
                        "%s : %d : The parameter destMax is equal to zero or larger than the macro : "                 \
                        "SECUREC_STRING_MAX_LEN.",                                                                     \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    break;                                                                                             \
                case ERANGE_AND_RESET:                                                                                 \
                    elog(elevel,                                                                                       \
                        "%s : %d : The parameter destMax is too small or parameter count is larger than macro "        \
                        "parameter SECUREC_STRING_MAX_LEN. The second case only occures in functions "                 \
                        "strncat_s/strncpy_s.",                                                                        \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    break;                                                                                             \
                case EOVERLAP_AND_RESET:                                                                               \
                    elog(elevel,                                                                                       \
                        "%s : %d : The destination buffer and source buffer are overlapped.",                          \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    break;                                                                                             \
                default:                                                                                               \
                    elog(elevel, "%s : %d : Unrecognized return type.", __FILE__, __LINE__);                           \
                    break;                                                                                             \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)

#define securec_check_ssval(errno, express, elevel)                                                      \
    do {                                                                                                           \
        errno_t resno = errno;                                                                                     \
        if (resno == -1) {                                                                                   \
            express;                                                                                               \
            elog(elevel,                                                                                           \
                "%s : %d : The destination buffer or format is a NULL pointer or the invalid parameter handle is " \
                "invoked.",                                                                                        \
                __FILE__,                                                                                          \
                __LINE__);                                                                                         \
        }                                                                                                          \
    } while (0)

typedef void (*CleanHandler)(void*);

extern void* palloc0_noexcept(Size size);

template <typename T>
struct smart_ptr {
    smart_ptr() : m_handler(NULL), m_ptr(NULL)
    {}

    explicit smart_ptr(T* ptr) : m_handler(pfree), m_ptr(ptr)
    {}

    smart_ptr(CleanHandler myHandler, T* ptr) : m_handler(myHandler), m_ptr(ptr)
    {}

    ~smart_ptr()
    {
        handle();
    }

    T* set(Size size)
    {
        m_handler = pfree;
        m_ptr = palloc0(size);

        return m_ptr;
    }

    void set(T* ptr)
    {
        m_handler = pfree;
        m_ptr = ptr;
    }

    void set(CleanHandler handler, T* ptr)
    {
        m_handler = handler;
        m_ptr = ptr;
    }

    void reset()
    {
        m_handler = NULL;
        m_ptr = NULL;
    }

    T* ptr()
    {
        return m_ptr;
    }

    T* operator->() const
    {
        return m_ptr;
    }

    void handle()
    {
        if (m_handler)
            m_handler(m_ptr);

        reset();
    }

protected:
    smart_ptr(const smart_ptr&);
    smart_ptr& operator=(const smart_ptr&);

private:
    CleanHandler m_handler;
    T* m_ptr;
};

struct WLMContextLock {
    WLMContextLock(pthread_mutex_t* mutex)
        : m_mutex(mutex), m_isLocked(false), m_exitUnlock(true), m_clean(NULL), m_ptr(NULL), m_tid(THREADID)
    {}

    WLMContextLock(pthread_mutex_t* mutex, bool thread_exit_unlock)
        : m_mutex(mutex),
          m_isLocked(false),
          m_exitUnlock(thread_exit_unlock),
          m_clean(NULL),
          m_ptr(NULL),
          m_tid(THREADID)
    {}

    ~WLMContextLock()
    {
        if (t_thrd.proc_cxt.proc_exit_inprogress && !m_exitUnlock) {
            m_isLocked = false;
        }
        UnLock();
    }

    void Lock(bool isForce = false)
    {
        if (isForce || !m_isLocked) {
            if (!IsOwner())
                (void)pthread_mutex_lock(m_mutex);
            m_isLocked = true;
        }
    }

    bool TryLock()
    {
        if (!m_isLocked) {
            int ret = pthread_mutex_trylock(m_mutex);
            if (ret == 0)
                m_isLocked = true;
        }

        return m_isLocked;
    }

    void UnLock(bool isSafe = false)
    {
        if (m_isLocked) {
            clean();

            if (!isSafe || IsOwner())
                (void)pthread_mutex_unlock(m_mutex);

            m_isLocked = false;
        }
    }

    bool IsOwner()
    {
        return IS_MUTEX_HELD(m_mutex, m_tid);
    }

    void ReleaseLock(pthread_mutex_t* mutex)
    {
        pthread_mutex_t* old_mutex = m_mutex;

        m_mutex = mutex;

        if (IsOwner())
            (void)pthread_mutex_unlock(m_mutex);

        m_mutex = old_mutex;
    }

    bool replace(pthread_mutex_t* mutex)
    {
        if (!m_isLocked) {
            m_mutex = mutex;
            return true;
        }

        return false;
    }

    void set(CleanHandler func, void* ptr)
    {
        m_clean = func;
        m_ptr = ptr;
    }
    /*
     * function name: reset
     * description  : reset params to handle for context lock.
     * return value : void
     */
    void reset()
    {
        m_clean = NULL;
        m_ptr = NULL;
    }
    /*
     * function name: clean
     * description  : execute the clean handler for context lock.
     * return value : void
     */
    void clean()
    {
        if (m_clean) {
            m_clean(m_ptr);
            reset();
        }
    }
    /*
     * function name: ConditionWait
     * description  : execute the condition wait
     * return value : void
     */
    void ConditionWait(pthread_cond_t* condition)
    {
        (void)pthread_cond_wait(condition, m_mutex);
    }

    void ConditionTimedWait(pthread_cond_t* condition, int seconds)
    {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += seconds;
        (void)pthread_cond_timedwait(condition, m_mutex, &ts);
    }

    void ConditionWakeUp(pthread_cond_t* condition)
    {
        (void)pthread_cond_signal(condition);
    }

private:
    pthread_mutex_t* m_mutex;
    bool m_isLocked;
    bool m_exitUnlock;

    CleanHandler m_clean;
    void* m_ptr;

    pid_t m_tid;
};

#define WORKLOAD_LOCK_NUM ((int)WorkloadNodeGroupLock + 10)

class WLMAutoLWLock {
public:
    WLMAutoLWLock(LWLock *lock, LWLockMode lockMode) : m_lockId(lock), m_lockMode(lockMode), m_isLocked(false)
    {}

    ~WLMAutoLWLock()
    {
        AutoLWLockRelease();
    }

    inline void AutoLWLockAcquire()
    {
        // Guard against recursive lock
        if (!m_isLocked) {
            LWLockAcquire(m_lockId, m_lockMode);
            m_isLocked = true;
        }
    }

    inline void AutoLWLockRelease()
    {
        if (m_isLocked && !t_thrd.port_cxt.thread_is_exiting) {
            /* maybe there is try-catch operation,
             * it resets the t_thrd.int_cxt.InterruptHoldoffCount as 0.
             * So reset it again to avoid core issue */
            if (t_thrd.int_cxt.InterruptHoldoffCount == 0)
                HOLD_INTERRUPTS();
            LWLockRelease(m_lockId);
            m_isLocked = false;
        }
    }

private:
    LWLock *m_lockId;
    LWLockMode m_lockMode;
    bool m_isLocked;
};

typedef smart_ptr<void> WLMContextGuard;

template <typename T>
T* pobjdup(T* ptr, size_t n)
{
    if (ptr == NULL)
        return ptr;

    T* duptr = (T*)palloc0_noexcept(sizeof(T) * n);

    if (duptr == NULL)
        return duptr;

    errno_t errval = memcpy_s(duptr, n * sizeof(T), ptr, n * sizeof(T));
    securec_check_errval(errval, , LOG);

    return duptr;
}

template <class NodeType, class KeyType>
NodeType* search_node(const List* list, const KeyType* keydata)
{
    foreach_cell(cell, list)
    {
        NodeType* node = (NodeType*)lfirst(cell);

        if (node->equals(keydata))
            return node;
    }

    return NULL;
}

template <typename KeyType, class NodeType>
ListCell* search_list(const List* list, const KeyType* keydata, bool* found)
{
    ListCell* curr = NULL;
    ListCell* prev = NULL;
    NodeType* node = NULL;

    if (found != NULL)
        *found = false;

    foreach (curr, list) {
        node = (NodeType*)lfirst(curr);

        int comp = node->compare(keydata);

        if (comp == 0) {
            if (found != NULL)
                *found = true;

            return curr;
        } else if (comp > 0)
            return prev;

        prev = curr;
    }

    return prev;
}

template <typename KeyType, class NodeType, bool ToAlloc>
ListCell* append_to_list(List** list, const KeyType* keydata, NodeType* node = NULL)
{
    bool found = false;

    ListCell* lcnode = search_list<KeyType, NodeType>(*list, keydata, &found);

    if (found)
        return lcnode;

    /* cannot find the node with this priority, we must create a new one. */
    if (ToAlloc)
        node = (NodeType*)palloc0_noexcept(sizeof(NodeType));

    if (node == NULL)
        return NULL;

    PG_TRY();
    {
        if (lcnode == NULL) {
            *list = lcons(node, *list);

            lcnode = list_head(*list);
        } else {
            lcnode = lappend_cell(*list, lcnode, node);
        }
    }
    PG_CATCH();
    {
        lcnode = NULL;

        if (ToAlloc)
            pfree(node);

        FlushErrorState();
    }
    PG_END_TRY();

    return lcnode;
}

/*
 * @Description: scan hash table, assign all the info "is_dirty" true
 * @IN htab: hash table
 * @Return: void
 * @See also:
 */
template <class DataType>
void AssignHTabInfoDirtyWithLock(HTAB* htab, LWLock *lwlock)
{
    DataType* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;

    USE_AUTO_LWLOCK(lwlock, LW_EXCLUSIVE);

    hash_seq_init(&hash_seq, htab);

    while ((hdata = (DataType*)hash_seq_search(&hash_seq)) != NULL)
        hdata->is_dirty = true;
}

template <class DataType>
void ProcessHTabRecordWithLock(HTAB* htab, LWLock *lwlock, CleanHandler handle)
{
    DataType* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;

    USE_AUTO_LWLOCK(lwlock, LW_EXCLUSIVE);

    hash_seq_init(&hash_seq, htab);

    while ((hdata = (DataType*)hash_seq_search(&hash_seq)) != NULL)
        handle(hdata);
}

#endif
