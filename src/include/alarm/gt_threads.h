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
 * gt_threads.h
 *        Thread operations functionality header file.  Uses pthread library.
 *        This library is part of Gauss tools common code library.
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gt_threads.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __GT_THREADS_H__
#define __GT_THREADS_H__

#include "postgres.h"
#include "knl/knl_variable.h"
#include <pthread.h>
#include <unistd.h>
#include "alarm/gs_warn_common.h"

extern "C" {

#define GT_THREAD_STACK_SIZE 131072             /* 128K [32768 - 32K thread stack size] */
#define GT_THREAD_SLEEP_TIME (100)              /* 1/10th of a sec */
#define GT_THREAD_START_MAX_RETRY_COUNT (10000) /* 10s */
#define MAX_THREAD_NAME_LEN 16

typedef pthread_mutex_t THREAD_LOCK;

/* Thread state  enum. */
typedef enum tagGTThreadStatUS_E {
    GT_THREAD_FREE = 0,
    GT_THREAD_STARTING,
    GT_THREAD_STARTED,
    GT_THREAD_RUNNING,
    GT_THREAD_START_FAILED,
    GT_THREAD_EXITING,

    GT_THREAD_BUTT = 0xffff
} GT_THREAD_STATUS_E;

/* Thread structure definition. */
typedef struct tagGTThread {
    pthread_t id;

    /* thread's run-time status, other thread can read it*/
    GT_THREAD_STATUS_E enStatus;

    void* pvProc;         /* pointer of thread entry procedure*/
    void* pvArgument;     /* argument */
    int32 ulResult;       /* result of thread's execution */
    bool bExitThreadFlag; /* thread will quit gracefully, if set */
    void* pvCleanup;      /* Any cleanup like release thread to pool etc */
} GT_THREAD_S, *LPGT_THREAD_S;

/***
 * Function signature for the thread entry functions.
 *
 * @param pstThread Thread context
 * @return NA
 ***/
typedef void (*LPGT_THREAD_PROC)(LPGT_THREAD_S pstThread);

/***
 *  Create thread on demand.
 *
 *  @param pfnProc : Function pointer, entry function to the thread.
 *  @param pvArgs  : Argument to the entry function.
 *  @param ulStackSize : Stack size of the new thread.
 *  @param pstThread   : Thread context structure.
 *
 *  @ return GT_SUCCESS on success or error code on failure.
 ***/
WARNERRCODE createThread(LPGT_THREAD_PROC pfnThreadProc, void* pvArgs, uint32 ulStackSize, LPGT_THREAD_S pstThread);

/***
 *  Joins the thread to parent. Wait for the thread to close.
 *
 *  @param pstThread   : Thread context structure.
 *  @ return NA.
 ***/
void closeThread(LPGT_THREAD_S pstThread);

/***
 *  Get the current thread id
 *
 *  @return id of the current thread.
 ***/
int32 getThreadID();

/***
 * Sleep function for the thread.
 *
 * @param sleep time in milli-second
 ***/
void threadSleep(uint32 ulSleepMs);

/***
 *  Initialize the thread lock
 *
 *  @param lock parameter
 ***/
WARNERRCODE initThreadLock(THREAD_LOCK* lock);

/***
 * Destory the thread lock
 *
 * @param lock parameter
 ***/
WARNERRCODE destroyThreadLock(THREAD_LOCK* lock);

/***
 * Acquire Lock the thread
 *
 * @param lock parameter
 ***/
WARNERRCODE lock(THREAD_LOCK* lock);

/***
 * Release the thread lock
 *
 * @param lock parameter
 ***/
WARNERRCODE unlock(THREAD_LOCK* lock);
}

#endif /* __GT_THREADS_H__ */
