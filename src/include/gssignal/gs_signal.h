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
 * gs_signal.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/gssignal/gs_signal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_SIGNAL_H_
#define GS_SIGNAL_H_

#include "gs_thread.h"
#include <signal.h>
#include "storage/lock/s_lock.h"
#include <sys/time.h>

#ifdef WIN32
#include "pthread-win32.h"
#endif

#define GS_SIGNAL_COUNT         32

typedef void (*gs_sigfunc)(int);

#ifdef WIN32
typedef int sigset_t;
#endif

typedef enum GsSignalCheckType {
    SIGNAL_CHECK_NONE,
    SIGNAL_CHECK_EXECUTOR_STOP,
    SIGNAL_CHECK_STREAM_STOP,
    SIGNAL_CHECK_SESS_KEY
} GsSignalCheckType;

/* the struct of signal check */
typedef struct GsSignalCheck {
    GsSignalCheckType check_type;
    uint64 debug_query_id;
    uint64 session_id;
} GsSignalCheck;

/* the struct of signal to be handled and the signal sender's thread id */
typedef struct GsSndSignal {
    unsigned int signo;  /* the signal to be handled */
    gs_thread_t thread;  /* the signal sender's thread id */
    GsSignalCheck check; /* the signal sender's check information if need check */
} GsSndSignal;

/* the list struct of GsSndSignal */
typedef struct GsNode {
    GsSndSignal sig_data;
    struct GsNode* next;
} GsNode;

/*
 * Each thread has a signal pool, which contains the used signal list and free signal list.
 * free signal list keeps the free signal slots for this thread.
 * used signal list keeps the signal which will be handled for this thread.
 * When a analog signal arrives, it will search a free signal slot, fill the sig_data
 * then put it info the used list; after a signal handled, the used slot will be put
 * back to the free list.
 */
typedef struct SignalPool {
    GsNode* free_head; /* the head of free signal list */
    GsNode* free_tail; /* the tail of free signal list  */
    GsNode* used_head; /* the head of used signal list */
    GsNode* used_tail; /* the tail of used signal list */
    int pool_size;     /* the size of the array list */
    pthread_mutex_t sigpool_lock;
} SignalPool;

/* the struct keeps signal handle function array and signal pool for each thread */
typedef struct GsSignal {
    gs_sigfunc handlerList[GS_SIGNAL_COUNT];
    sigset_t masksignal;
    SignalPool sig_pool;
#ifndef WIN32
    volatile unsigned int bitmapSigProtectFun;
#else
    HANDLE signal_event;
#endif
} GsSignal;

/* each thread has a GsSignalSlot to keep their thread id, thread name, signals and signal handles */
typedef struct GsSignalSlot {
    ThreadId thread_id;
    char* thread_name;
    GsSignal* gssignal;
    pid_t lwtid;
} GsSignalSlot;

typedef volatile int gs_atomic_t;

#ifndef WIN32
extern sigset_t gs_signal_unblock_sigusr2(void);

extern sigset_t gs_signal_block_sigusr2(void);

extern void gs_signal_recover_mask(sigset_t mask);

#else

#define gs_signal_unblock_sigusr2() 0
#define gs_signal_block_sigusr2() 0
#define gs_signal_recover_mask(_mask) _mask

#endif

#ifndef WIN32
typedef void (*gs_sigaction_func) (int, siginfo_t *, void *);
#endif

/* global GsSignalSlot manager, keeps all threads' GsSignalSlot */
typedef struct GsSignalBase {
    GsSignalSlot* slots;
    unsigned int slots_size;
    pthread_mutex_t slots_lock;
} GsSignalBase;

#ifdef WIN32
#define pgwin32_signal_event    (MySignalSlot->gssignal->signal_event)
#endif

extern gs_sigfunc gspqsignal(int signo, gs_sigfunc func);

extern void gs_signal_setmask(sigset_t* mask, sigset_t* old_mask);

/*
 * initialize signal slots
 */
extern void gs_signal_slots_init(unsigned long size);

extern GsSignalSlot* gs_signal_slot_release(ThreadId thread_id);

/*gs send signal to thread*/
extern int gs_signal_send(ThreadId thread_id, int signo, int nowait = 0);
extern int gs_signal_send(ThreadId thread_id, int signo, bool is_thread_pool);

extern void gs_signal_startup_siginfo(char* thread_name);

extern int gs_thread_kill(ThreadId tid, int sig);

extern void gs_signal_handle(void);

/* Create timer support for current thread */
extern int gs_signal_createtimer(void);

/* A thread-safe version replacement of setitimer () call. */
extern int gs_signal_settimer(struct itimerval* interval);

/* A thread-safe version replacement of CancelTimer (0) call. */
extern int gs_signal_canceltimer(void);

/* A thread-safe version replacement of delteitimer (0) call. */
extern int gs_signal_deletetimer(void);

extern void gs_signal_monitor_startup(void);

#ifdef WIN32
#define pgwin32_dispatch_queued_signals gs_signal_handle
#define UNBLOCKED_SIGNAL_QUEUE() true
#endif

#endif /* GS_SIGNAL_H_ */
