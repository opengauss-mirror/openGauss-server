/* -------------------------------------------------------------------------
 *
 * spin.cpp
 *	   Hardware-independent implementation of spinlocks.
 *
 *
 * For machines that have test-and-set (TAS) instructions, s_lock.h/.c
 * define the spinlock implementation.	This file contains only a stub
 * implementation for spinlocks using PGSemaphores.  Unless semaphores
 * are implemented in a way that doesn't involve a kernel call, this
 * is too slow to be very useful :-(
 *
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/spin.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"
#include "replication/walsender.h"
#include "storage/lock/lwlock.h"
#include "storage/spin.h"

#ifdef HAVE_SPINLOCKS

/*
 * Report number of semaphores needed to support spinlocks.
 */
int SpinlockSemas(void)
{
    return 0;
}
#else /* !HAVE_SPINLOCKS */

/*
 * No TAS, so spinlocks are implemented as PGSemaphores.
 *
 * Report number of semaphores needed to support spinlocks.
 */
int SpinlockSemas(void)
{
    int nsemas;

    /*
     * It would be cleaner to distribute this logic into the affected modules,
     * similar to the way shmem space estimation is handled.
     *
     * For now, though, there are few enough users of spinlocks that we just
     * keep the knowledge here.
     */
    nsemas = NumLWLocks();                                      /* one for each lwlock */
    nsemas += TOTAL_BUFFER_NUM;            /* one for each buffer header */
    nsemas += 2 * g_instance.attr.attr_storage.max_wal_senders; /* one for each wal and data sender process */
    nsemas += 30;                                               /* plus a bunch for other small-scale use */

    return nsemas;
}

/*
 * s_lock.h hardware-spinlock emulation
 */
void s_init_lock_sema(volatile slock_t *lock)
{
    PGSemaphoreCreate((PGSemaphore)lock);
}

void s_unlock_sema(volatile slock_t *lock)
{
    PGSemaphoreUnlock((PGSemaphore)lock);
}

bool s_lock_free_sema(volatile slock_t *lock)
{
    /* We don't currently use S_LOCK_FREE anyway */
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("spin.c does not support S_LOCK_FREE()")));
    return false;
}

int tas_sema(volatile slock_t *lock)
{
    /* Note that TAS macros return 0 if *success* */
    return !PGSemaphoreTryLock((PGSemaphore)lock);
}

#endif /* !HAVE_SPINLOCKS */
