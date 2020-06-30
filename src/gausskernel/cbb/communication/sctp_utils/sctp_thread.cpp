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
 * sctp_thread.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_thread.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <signal.h>
#include "sctp_thread.h"

static void* mc_thread_main_routine(void* arg)
{
    struct mc_thread* self = (struct mc_thread*)arg;

    /*  Run the thread routine. */
    self->routine(self->arg);
    return NULL;
}

int mc_thread_init(struct mc_thread* self, mc_thread_routine* routine, void* arg)
{
    int rc;
    sigset_t new_sigmask;
    sigset_t old_sigmask;

    //  No signals should be processed by this thread. The library doesn't
    //		use signals and thus all the signals should be delivered to application
    //		threads, not to worker threads.
    rc = sigfillset(&new_sigmask);
    if (rc == 0) {
        rc = pthread_sigmask(SIG_BLOCK, &new_sigmask, &old_sigmask);
    }

    if (rc == 0) {
        self->routine = routine;
        self->arg = arg;
        rc = pthread_create(&self->handle, NULL, mc_thread_main_routine, (void*)self);
    }
    /*  Restore signal set to what it was before. */
    if (rc == 0) {
        rc = pthread_sigmask(SIG_SETMASK, &old_sigmask, NULL);
    }
    return rc;
}

void mc_thread_block_signal()
{
    sigset_t sigs;
    (void)sigemptyset(&sigs);
    (void)sigaddset(&sigs, SIGHUP);
    (void)sigaddset(&sigs, SIGINT);
    (void)sigaddset(&sigs, SIGTERM);
    (void)sigaddset(&sigs, SIGUSR1);
    (void)sigaddset(&sigs, SIGUSR2);
    (void)pthread_sigmask(SIG_BLOCK, &sigs, NULL);
}
