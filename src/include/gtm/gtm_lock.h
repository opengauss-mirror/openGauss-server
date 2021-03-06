/* -------------------------------------------------------------------------
 *
 * gtm_lock.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */

#ifndef GTM_LOCK_H
#define GTM_LOCK_H

#include <pthread.h>

typedef struct GTM_RWLock {
    pthread_rwlock_t lk_lock;
} GTM_RWLock;

typedef struct GTM_MutexLock {
    pthread_mutex_t lk_lock;
} GTM_MutexLock;

typedef enum GTM_LockMode {
    GTM_LOCKMODE_WRITE,
    GTM_LOCKMODE_READ
} GTM_LockMode;

typedef struct GTM_CV {
    pthread_cond_t cv_condvar;
} GTM_CV;

extern void GTM_RWLockAcquire(GTM_RWLock* lock, GTM_LockMode mode);
extern void GTM_RWLockRelease(GTM_RWLock* lock);
extern void GTM_RWLockInit(GTM_RWLock* lock);
extern void GTM_RWLockDestroy(GTM_RWLock* lock);
extern bool GTM_RWLockConditionalAcquire(GTM_RWLock* lock, GTM_LockMode mode);

extern void GTM_MutexLockAcquire(GTM_MutexLock* lock);
extern void GTM_MutexLockRelease(GTM_MutexLock* lock);
extern void GTM_MutexLockInit(GTM_MutexLock* lock);
extern void GTM_MutexLockDestroy(GTM_MutexLock* lock);
extern void GTM_MutexLockConditionalAcquire(GTM_MutexLock* lock);

extern int GTM_CVInit(GTM_CV* cv);
extern int GTM_CVDestroy(GTM_CV* cv);
extern int GTM_CVSignal(GTM_CV* cv);
extern int GTM_CVBcast(GTM_CV* cv);
extern int GTM_CVWait(GTM_CV* cv, GTM_MutexLock* lock);

// AutoGTMRWLock
//		 		 Auto object for GTM reader-writer lock
//
class AutoGTMRWLock {
public:
    AutoGTMRWLock(_in_ volatile GTM_RWLock* glock)
    {
        m_glock = (GTM_RWLock*)glock;
    }

    ~AutoGTMRWLock()
    {
        LockRelease();
    }

    inline void LockAcquire(_in_ GTM_LockMode mode)
    {
        GTM_RWLockAcquire(m_glock, mode);
    }

    inline bool LockConditionalAcquire(_in_ GTM_LockMode mode)
    {
        return GTM_RWLockConditionalAcquire(m_glock, mode);
    }

    inline void LockRelease()
    {
        if (m_glock != NULL) {
            GTM_RWLockRelease(m_glock);
            m_glock = NULL;
        }
    }

private:
    // Point to an existing GTM_RWLock object
    //
    GTM_RWLock* m_glock;
};
#endif