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
 * posix_semaphore.cpp
 *      A thin wrapper to the posix semaphore library.  All system errors are
 *      handled within the library.
 *
 *      The reason why this wrapper is created instead of reusing PGSemaphore
 *      is that PGSemaphore can only be created in the postmaster process and
 *      the maximum number of PGSemaphores must be known at database startup.
 *      For parallel recovery, each log record that touches multiple pages
 *      requires a new semaphore, so we need an interface that can create and
 *      destroy semaphores dynamically.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/parallel_recovery/posix_semaphore.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"

#include "access/parallel_recovery/posix_semaphore.h"

namespace parallel_recovery {

/*
 * PosixSemaphoreInit
 *   -- Initialize a semaphore with the specified initial value.  The
 *      semaphore must point to an allocated and zeroed structure.
 *
 * @in  sem       - The semaphore to be initialized.
 * @in  initValue - The initial value for the semaphore.
 */
void PosixSemaphoreInit(PosixSemaphore *sem, unsigned int initValue)
{
    Assert(!sem->initialized);

    if (sem_init(&sem->semaphore, 0, initValue) == 0)
        sem->initialized = true;
    else
        ereport(FATAL, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("sem_init failed: %m")));
}

/*
 * PosixSemaphoreDestroy
 *   -- Destroy a semaphore.  It is OK to destroy an uninitialized semaphore
 *      if the structure had been zeroed.  This is intentional to simplify
 *      the code that releases resources on error conditions.
 *
 * @in  sem - The semaphore to destroy.
 */
void PosixSemaphoreDestroy(PosixSemaphore *sem)
{
    if (sem->initialized && sem_destroy(&sem->semaphore) != 0)
        ereport(FATAL, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("sem_destroy failed: %m")));
    sem->initialized = false;
}

/*
 * PosixSemaphoreWait
 *   -- Decrement a semaphore, blocking if count would be < 0.
 *
 * @in  sem - The semaphore to decrement.
 */
void PosixSemaphoreWait(PosixSemaphore *sem)
{
    int ret = 0;
    do {
        t_thrd.int_cxt.ImmediateInterruptOK = false;
        CHECK_FOR_INTERRUPTS();
    } while ((ret = sem_wait(&sem->semaphore)) != 0 && errno == EINTR);

    if (ret != 0)
        ereport(FATAL, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("sem_wait failed: %m")));
}

/*
 * PosixSemaphorePost
 *   -- Increment a semaphore.
 *
 * @in  sem - The semaphore to increment.
 */
void PosixSemaphorePost(PosixSemaphore *sem)
{
    int ret = 0;
    while ((ret = sem_post(&sem->semaphore)) != 0 && errno == EINTR)
        ;

    if (ret != 0)
        ereport(FATAL, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("sem_wait failed: %m")));
}

}  // namespace parallel_recovery
