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
 * gs_thread.h
 *        Exports from src/port/gs_thread.c.
 *
 *
 * IDENTIFICATION
 *        src/include/gs_thread.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_THREAD_H_
#define GS_THREAD_H_

#include "gs_threadlocal.h"

#ifdef WIN32
#include <winsock2.h>
#define _WINSOCKAPI_
#include <windows.h>
#else
#include <pthread.h>
#endif

#define gs_thread_id(_t) ((_t).thid)

#ifdef WIN32
#define gs_thread_handle(_t) ((HANDLE)(_t).os_handle)
typedef unsigned ThreadId;
typedef struct gs_thread_t {
    ThreadId thid;
    HANDLE os_handle;
} gs_thread_t;
#else

typedef pthread_t ThreadId;
typedef struct gs_thread_t {
    ThreadId thid;
} gs_thread_t;
#endif

typedef enum knl_thread_role {
    MASTER_THREAD = 0,
    WORKER = 1,
    THREADPOOL_WORKER = 2,
    THREADPOOL_LISTENER,
    THREADPOOL_SCHEDULER,
    THREADPOOL_STREAM,
    STREAM_WORKER,
    AUTOVACUUM_LAUNCHER,
    AUTOVACUUM_WORKER,
    JOB_SCHEDULER,
    JOB_WORKER,
    WLM_WORKER,
    WLM_MONITOR,
    WLM_ARBITER,
    WLM_CPMONITOR,
    AUDITOR,
    PGSTAT,
    SYSLOGGER,
    CATCHUP,
    ARCH,
    ALARMCHECK,
    REAPER,
    PAGEREDO,
    TWOPASECLEANER,
    STARTUP,
    FAULTMONITOR,
    BGWRITER,
    SPBGWRITER,
    PERCENTILE_WORKER,
    TXNSNAP_CAPTURER,
    TXNSNAP_WORKER,
    RBCLEANER,
    RBWORKER,
    SNAPSHOT_WORKER,
    ASH_WORKER,
    TRACK_STMT_WORKER,
    TRACK_STMT_CLEANER,
    CHECKPOINT_THREAD,
    WALWRITER,
    WALWRITERAUXILIARY,
    WALRECEIVER,
    WALRECWRITE,
    DATARECIVER,
    DATARECWRITER,
    CBMWRITER,
    PAGEWRITER_THREAD,
    PAGEREPAIR_THREAD,
    HEARTBEAT,
    COMM_SENDERFLOWER,
    COMM_RECEIVERFLOWER,
    COMM_RECEIVER,
    COMM_AUXILIARY,
    COMM_POOLER_CLEAN,
    LOGICAL_READ_RECORD,
    PARALLEL_DECODE,

    UNDO_RECYCLER,
    UNDO_LAUNCHER,
    UNDO_WORKER,
    CSNMIN_SYNC,
    GLOBALSTATS_THREAD,
    BARRIER_CREATOR,
    BGWORKER,
    BARRIER_ARCH,
    SHARE_STORAGE_XLOG_COPYER,
    APPLY_LAUNCHER,
    APPLY_WORKER,
    BARRIER_PREPARSE,
    TS_COMPACTION,
    TS_COMPACTION_CONSUMER,
    TS_COMPACTION_AUXILIAY,
    STREAMING_BACKEND,
    STREAMING_ROUTER_BACKEND,
    STREAMING_WORKER_BACKEND,
    STREAMING_COLLECTOR_BACKEND,
    STREAMING_QUEUE_BACKEND,
    STREAMING_REAPER_BACKEND,

    COMM_PROXYER,
    /* should be last valid thread. */
    THREAD_ENTRY_BOUND,

    NO_SUBROLE,
    REDISTRIBUTION_WORKER,
    WAL_NORMAL_SENDER,
    WAL_HADR_SENDER, /* A cross cluster wal sender to hadr cluster main standby */
    WAL_HADR_CN_SENDER, /* A cross cluster wal sender to hadr cluster coordinator standby */
    WAL_SHARE_STORE_SENDER, /* A cross cluster wal sender to share storage cluster standby */
    WAL_STANDBY_SENDER, /* Am I cascading WAL to another standby ? */
    WAL_DB_SENDER,
    TOP_CONSUMER,
    DCF_WORKER
} knl_thread_role;

/*
 * It is an 64bit identifier in Linux x64 system. There are many legacy
 * code assumes the original pid is 32 bit where we replace with threadId.
 * If printf this number, we use %lu within the server. When compare valid
 * ThreadIds, use PtThread::Equal() function.
 */
#define INVALID_NEXT_ADDR (void*)((unsigned long)(-1))

typedef struct knl_thread_arg {
    knl_thread_role role;
    char* save_para;
    void* payload;
    void* t_thrd;
    union {
        struct syslog_thread {
            int syslog_handle;
        } log_thread;
    } extra_payload;
} knl_thread_arg;

typedef int (*GaussdbThreadEntry)(knl_thread_arg* arg);

typedef struct ThreadMetaData {
    GaussdbThreadEntry func;
    knl_thread_role    role;
    const char*        thr_name; /* keep strlen(thr_name) < 16 */
    const char*        thr_long_name;
} ThreadMetaData;

/*
 * A generic holder to keep all necessary arguments passed to generic
 * ThreadStarterFunc() function.
 */
typedef struct ThreadArg {
    void* (*m_taskRoutine)(void*); /* Task function caller passed in by the caller. */
    struct ThreadArg* next;        /* if next is INVALID_NEXT_ID, the ThreadArg is malloc when create a thread */
    knl_thread_arg m_thd_arg;
} ThreadArg;

#define InvalidTid ((ThreadId)(-1)) /* An invalid thread identifier */

#ifdef WIN32
extern HANDLE gs_thread_get_handle(unsigned ThreadId);
#endif

extern gs_thread_t gs_thread_get_cur_thread(void);

extern ThreadId gs_thread_self(void);

extern void gs_thread_exit(int code);
extern void gs_thread_args_free(void);

extern int gs_thread_create(gs_thread_t* thread, void* (*taskRoutine)(void*), int argc, void* argv);

extern int gs_thread_join(gs_thread_t thread, void** value_ptr);

extern void gs_thread_args_pool_init(unsigned long pool_size, unsigned long backend_para_size);

extern void gs_thread_release_args_slot(ThreadArg* thrArg);

extern ThreadArg* gs_thread_get_args_slot(void);

extern void gs_thread_get_name(char** name_thread, char** argv, int argc);

extern void ThreadExitCXX(int code);
extern int ShowThreadName(const char* name);

typedef void (*uuid_struct_destroy_hook_type)(int which);
extern THR_LOCAL uuid_struct_destroy_hook_type uuid_struct_destroy_hook;

#endif /*GS_THREAD_H_ */
