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
 * threadpool.h
 *     Include all thread pool header file.
 * 
 * IDENTIFICATION
 *        src/include/threadpool/threadpool.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "threadpool/threadpool_controler.h"
#include "threadpool/threadpool_group.h"
#include "threadpool/threadpool_listener.h"
#include "threadpool/threadpool_sessctl.h"
#include "threadpool/threadpool_worker.h"
#include "threadpool/threadpool_scheduler.h"
#include "threadpool/threadpool_stream.h"

#define ENABLE_THREAD_POOL (g_threadPoolControler != NULL)
#define IS_THREAD_POOL_WORKER (t_thrd.role == THREADPOOL_WORKER)
#define IS_THREAD_POOL_LISTENER (t_thrd.role == THREADPOOL_LISTENER)
#define IS_THREAD_POOL_SCHEDULER (t_thrd.role == THREADPOOL_SCHEDULER)
#define IS_THREAD_POOL_STREAM (t_thrd.role == THREADPOOL_STREAM)
#define IS_THREAD_POOL_SESSION (u_sess->session_id > 0)
#define BackendIdForTempRelations (ENABLE_THREAD_POOL ? (BackendId)u_sess->session_ctr_index : t_thrd.proc_cxt.MyBackendId)
#define THREAD_CORE_RATIO 1
#define DEFAULT_THREAD_POOL_SIZE 16
#define DEFAULT_THREAD_POOL_GROUPS 2
#define MAX_THREAD_POOL_SIZE 4096
#define MAX_STREAM_POOL_SIZE 16384
#define MAX_THREAD_POOL_GROUPS 64
#define CHAR_SIZE 512
#define DEFAULT_THREAD_POOL_STREAM_PROC_RATIO 0.2

/* dop max is 8 */
#define MAX_THREAD_POOL_STREAM_PROC_RATIO 8



extern ThreadPoolControler* g_threadPoolControler;

extern void BackendWorkerIAm();

/* Interface for thread pool listener */
extern void TpoolListenerMain(ThreadPoolListener* listener);
extern void ThreadPoolListenerIAm();

extern void PreventSignal();
extern void AllowSignal();

#endif /* THREAD_POOL_H */
