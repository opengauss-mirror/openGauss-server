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
 * gs_thread.cpp
 *
 * IDENTIFICATION
 *    src/common/port/gs_thread.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef FRONTEND

#include "postgres.h"
#include "knl/knl_variable.h"
#include "gstrace/gstrace_infra.h"

#include <malloc.h>

#ifndef WIN32
#include <pthread.h>
#endif

#include <signal.h>

#include "access/gtm.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamMain.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "storage/smgr/smgr.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "storage/cstore/cstore_mem_alloc.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "gs_thread.h"
#include "gssignal/gs_signal.h"
#include "utils/pg_locale.h"
#include "gs_policy/policy_common.h"
#ifdef ENABLE_GSS
#include "gssapi/gssapi_krb5.h"
#endif /* ENABLE_GSS */
#ifdef KRB5
#include "krb5.h"
#endif
#ifndef WIN32_ONLY_COMPILER
#include "dynloader.h"
#else
#include "port/dynloader/win32.h"
#endif

THR_LOCAL ReseThreadValuesPtr reset_policy_thr_hook = NULL;

/* keep the args of each thread */
typedef struct tag_gs_thread_pool {
    ThreadArg* thr_args;
    unsigned int save_backend_para_size;
    slock_t thr_lock;
} gs_thread_args_pool;

static THR_LOCAL ThreadId local_thread_id = InvalidTid;

static volatile gs_thread_args_pool thread_args_pool;

extern bool IsPostmasterEnvironment;

extern volatile ThreadId PostmasterPid;

extern void EarlyBindingTLSVariables(void);

extern void proc_exit_prepare(int code);

extern void MemoryContextDestroyAtThreadExit(MemoryContext context);

static void* ThreadStarterFunc(void* arg);

static void check_backend_name(const char* argv, char** name_thread);

static void check_avlauncher_name(const char* argv, char** name_thread);

static void check_avworker_name(const char* argv, char** name_thread);

static void check_jobschd_name(const char* argv, char** name_thread);

static void check_jobworker_name(const char* argv, char** name_thread);

static void check_arch_name(const char* argv, char** name_thread);

static void check_col_name(const char* argv, char** name_thread);

static void check_snapshot_name(const char* argv, char** name_thread);

static void check_log_name(const char* argv, char** name_thread);

static void check_audit_name(const char* argv, char** name_thread);

static void check_boot_name(char** argv, int argc, char** name_thread);

static void check_undocleanup_name(const char* argv, char** name_thread);

static void check_catchup_name(const char* argv, char** name_thread);

extern void CodeGenThreadTearDown();

extern void CancelAutoAnalyze();

extern void uuid_struct_destroy_function();

static void clean_kerberos_cache();

typedef void (*uuid_struct_destroy_hook_type)(int which);
THR_LOCAL uuid_struct_destroy_hook_type uuid_struct_destroy_hook = NULL;

/* =================== static functions =================== */

static void check_backend_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkbackend", strlen("--forkbackend")) == 0) {
        *name_thread = "postgres";
    }

    return;
}

static void check_avlauncher_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkavlauncher", strlen("--forkavlauncher")) == 0) {
        *name_thread = "AutoVacLauncher";
    }

    return;
}

static void check_avworker_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkavworker", strlen("--forkavworker")) == 0) {
        *name_thread = "AutoVacWorker";
    }

    return;
}

static void check_catchup_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkcatchup", strlen("--forkcatchup")) == 0) {
        *name_thread = "Catchup";
    }

    return;
}

static void check_jobschd_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkjobschd", strlen("--forkjobschd")) == 0) {
        *name_thread = "JobScheduler";
    }

    return;
}

static void check_jobworker_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkjobworker", strlen("--forkjobworker")) == 0) {
        *name_thread = "JobExecuteWorker";
    }

    return;
}

static void check_undocleanup_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkcleanup", strlen("--forkcleanup")) == 0) {
        *name_thread = "AutoUndoCleanUpProcessor";
    }

    return;
}

static void check_arch_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkarch", strlen("--forkarch")) == 0) {
        *name_thread = "PgArchiver";
    }

    return;
}

static void check_col_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkcol", strlen("--forkcol")) == 0) {
        *name_thread = "PgstatCollector";
    }

    return;
}

static void check_snapshot_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forksnap", strlen("--forksnap")) == 0) {
        *name_thread = "snapshotCollector";
    }

    return;
}

static void check_log_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forklog", strlen("--forklog")) == 0) {
        *name_thread = "SysLogger";
    }

    return;
}

static void check_audit_name(const char* argv, char** name_thread)
{
    if (*name_thread != NULL) {
        return;
    }

    if (strncmp(argv, "--forkaudit", strlen("--forkaudit")) == 0) {  // exp: @suppress("Function cannot be resolved")
        *name_thread = "PgAuditor";
    }

    return;
}

static void check_boot_name(char** argv, int argc, char** name_thread)
{
    char** tmp_argv = NULL;
    int tmp_argc = 0;
    OptParseContext optCtxt;
    int flag = 0;
    AuxProcType aux_thread_type = CheckerProcess;

    if (*name_thread != NULL) {
        return;
    }

    if (0 != strncmp(argv[1], "--forkboot", strlen("--forkboot"))) {
        return;
    }

    Assert((argv + 3) != NULL);
    Assert((argc - 3) >= 1);

    tmp_argv = argv + 2;
    tmp_argc = argc - 2;

    initOptParseContext(&optCtxt);

    while ((flag = getopt_r(tmp_argc, tmp_argv, "B:c:d:D:Fr:w:x:-:", &optCtxt)) != -1) {
        switch (flag) {
            case 'x':
                aux_thread_type = (AuxProcType)atoi(optCtxt.optarg);
                break;
            default:
                break;
        }
    }

    switch (aux_thread_type) {
        case StartupProcess:
            *name_thread = "Startup";
            break;
        case BgWriterProcess:
            *name_thread = "BgWriter";
            break;
        case CheckpointerProcess:
            *name_thread = "Checkpointer";
            break;
        case WalWriterProcess:
            *name_thread = "WalWriter";
            break;
        case WalWriterAuxiliaryProcess:
            *name_thread = "WalWriterAuxiliary";
            break;
        case WalReceiverProcess:
            *name_thread = "WalReceiver";
            break;
        case CBMWriterProcess:
            *name_thread = "CBMWriter";
            break;
        case RemoteServiceProcess:
            *name_thread = "RemoteService";
            break;
        default:
            *name_thread = "??? process";
            break;
    }

    return;
}

void gs_thread_get_name(char** name_thread, char** argv, int argc)
{
    check_backend_name(argv[1], name_thread);
    check_avlauncher_name(argv[1], name_thread);
    check_avworker_name(argv[1], name_thread);
    check_jobschd_name(argv[1], name_thread);
    check_jobworker_name(argv[1], name_thread);
    check_arch_name(argv[1], name_thread);
    check_col_name(argv[1], name_thread);
    check_snapshot_name(argv[1], name_thread);
    check_log_name(argv[1], name_thread);
    check_audit_name(argv[1], name_thread);
    check_boot_name(argv, argc, name_thread);
    check_undocleanup_name(argv[1], name_thread);
    check_catchup_name(argv[1], name_thread);
    GetThreadNameIfMultiRedo(argc, argv, name_thread);
    return;
}

static void* ThreadStarterFunc(void* arg)
{
    /* binding static TLS variables for current thread */
    EarlyBindingTLSVariables();

    /* Initialize thread IDs only once */
    t_thrd.port_cxt.m_pThreadArg = (ThreadArg*)arg;
    t_thrd.port_cxt.m_pThreadArg->m_thd_arg.t_thrd = &t_thrd;

    /* register the signal handler for this thread */
    return (*t_thrd.port_cxt.m_pThreadArg->m_taskRoutine)(&(t_thrd.port_cxt.m_pThreadArg->m_thd_arg));
}

/* =================== interface functions =================== */
/*
 * @@GaussDB@@
 * Brief		: get current thread's gs_thread_t
 * Description	:
 * Notes		:
 */
gs_thread_t gs_thread_get_cur_thread(void)
{
    gs_thread_t thread;

    thread.thid = gs_thread_self();

#ifdef WIN32
    thread.os_handle = OpenThread(SYNCHRONIZE, FALSE, thread.thid);
#endif

    return thread;
}

/*
 * @@GaussDB@@
 * Brief		: Get thread Id by pthread_create() call for Postmaster environment.
 * Description	:
 * Notes		:
 */
ThreadId gs_thread_self(void)
{
    if (InvalidTid == local_thread_id) {
#ifdef WIN32
        local_thread_id = (ThreadId)GetCurrentThreadId();
#else
        local_thread_id = (ThreadId)pthread_self();
#endif
    }

    return local_thread_id;
}

/*
 * @@GaussDB@@
 * Brief		: thread exit handle function.
 * Description	: the resource should be released before the thread exit.
 * Notes		:
 */
void gs_thread_exit(int code)
{
    int exitCode = code;

    (void)gs_signal_block_sigusr2();

    /* release the args slot in thread_args_pool */
    if (t_thrd.port_cxt.m_pThreadArg != NULL) {
        gs_thread_release_args_slot(t_thrd.port_cxt.m_pThreadArg);
        t_thrd.port_cxt.m_pThreadArg = NULL;
    }

    /* 
     * policy plugin is released when PM thread exit(existing as last one) 
     * so that we can make sure reset_policy_thr_hook is valid in gs_thread_exit
     */
    Assert(t_thrd.proc_cxt.MyProcPid != PostmasterPid);
    if (reset_policy_thr_hook) {
        reset_policy_thr_hook();
    }

    /* release the memory of kerberos */
    clean_kerberos_cache();

    /* release the memory of uuid_t struct */
    uuid_struct_destroy_function();

    /* unregiste the signal handler */
    /* invoke  all registered cleanup functions */
    proc_exit_prepare(exitCode);

    CloseGTM();

    if (exitCode != STATUS_ERROR) {
        CloseClientSocket(u_sess, true);
    }

    gs_poll_close();

    /* close the pipe */
    ClosePipesAtThreadExit();

    /* close the xlog files */
    CloseXlogFilesAtThreadExit();

    /* delete the signal timer */
    (void)gs_signal_deletetimer();

    /* free the locale cache */
    freeLocaleCache(true);

#ifdef ENABLE_LLVM_COMPILE
    /* release llvm context memory */
    CodeGenThreadTearDown();
#endif

    CancelAutoAnalyze();

    RestoreStream();

    if (t_thrd.bn != NULL) {
        t_thrd.bn->dead_end = true;
    } else if (!t_thrd.is_inited) {
        /* if thread has error befor get backend, get backend from childSlot. */
        Backend* bn = GetBackend(t_thrd.child_slot);
        if (bn != NULL) {
            bn->dead_end = true;
        }
    }

    /* release the signal slot in signal_base */
    (void)gs_signal_slot_release(gs_thread_self());
    if (IsPostmasterEnvironment && !t_thrd.postmaster_cxt.IsRPCWorkerThread) {
        if (u_sess->attr.attr_resource.enable_reaper_backend && StreamThreadAmI() &&
            g_instance.pid_cxt.ReaperBackendPID && g_instance.status == NoShutdown) {
            (void)gs_signal_send(g_instance.pid_cxt.ReaperBackendPID, SIGCHLD);
        } else {
            (void)gs_signal_send(PostmasterPid, SIGCHLD);
        }
    }

    MemoryContextDestroyAtThreadExit(t_thrd.top_mem_cxt);
    t_thrd.top_mem_cxt = NULL;
    TopMemoryContext = NULL;
    u_sess = NULL;
    CStoreMemAlloc::Reset();

    ThreadExitCXX(code);
}

/*
 * @@GaussDB@@
 * Brief		: init the thread args pool.
 * Description	:
 * Notes		:
 */
void gs_thread_args_pool_init(unsigned long pool_size, unsigned long backend_para_size)
{
    unsigned long loop;
    char* backend_para_base = NULL;

    /* according to the number of threads and backend parameter size fo each thread apply the memory */
    thread_args_pool.thr_args = (ThreadArg*)malloc(pool_size * (sizeof(ThreadArg) + backend_para_size));
    if (thread_args_pool.thr_args == NULL) {
        ereport(FATAL, (errmsg("out of memory")));
    }

    backend_para_base = (char*)((char*)(thread_args_pool.thr_args) + pool_size * sizeof(ThreadArg));

    for (loop = 0; loop < pool_size - 1; loop++) {
        thread_args_pool.thr_args[loop].next = &(thread_args_pool.thr_args[loop + 1]);
        thread_args_pool.thr_args[loop].m_taskRoutine = NULL;
    }

    thread_args_pool.thr_args[pool_size - 1].next = NULL;
    thread_args_pool.thr_args[pool_size - 1].m_taskRoutine = NULL;

    for (loop = 0; loop < pool_size; loop++) {
        thread_args_pool.thr_args[loop].m_thd_arg.save_para = (backend_para_base + (loop * backend_para_size));
    }

    thread_args_pool.save_backend_para_size = backend_para_size;
    SpinLockInit(&(thread_args_pool.thr_lock));

    return;
}

/*
 * @@GaussDB@@
 * Brief		: assign a slot to one thread.
 * Description	:
 * Notes		:
 */
ThreadArg* gs_thread_get_args_slot(void)
{
    ThreadArg* fetch_arg = NULL;
    sigset_t old_set;

    if (thread_args_pool.thr_args == NULL) {
        ereport(WARNING, (errmsg("sorry, too many clients already")));
        return NULL;
    }

    old_set = gs_signal_block_sigusr2();

    /* first search in the thread_args_pool */
    SpinLockAcquire(&(thread_args_pool.thr_lock));
    if (thread_args_pool.thr_args != NULL) {
        fetch_arg = thread_args_pool.thr_args;
        thread_args_pool.thr_args = fetch_arg->next;
        SpinLockRelease(&(thread_args_pool.thr_lock));
        gs_signal_recover_mask(old_set);

        return fetch_arg;
    }
    SpinLockRelease(&(thread_args_pool.thr_lock));

    /* then malloc a new one */
    fetch_arg = (ThreadArg*)malloc(sizeof(ThreadArg) + thread_args_pool.save_backend_para_size);
    if (fetch_arg == NULL) {
        ereport(WARNING, (errmsg("Failed to malloc memory for new thread.")));
        gs_signal_recover_mask(old_set);
        return NULL;
    }

    fetch_arg->m_taskRoutine = NULL;
    fetch_arg->next = (struct ThreadArg*)INVALID_NEXT_ADDR; /* present this arg is malloc when create a new thread */
    fetch_arg->m_thd_arg.save_para = ((char*)fetch_arg + sizeof(ThreadArg));
    gs_signal_recover_mask(old_set);
    return fetch_arg;
}

/*
 * @@GaussDB@@
 * Brief		: release the args slot to the thread_args_pool or os.
 * Description	:
 * Notes		:
 */
void gs_thread_release_args_slot(ThreadArg* thrArg)
{
    sigset_t old_set;

    old_set = gs_signal_block_sigusr2();

    /* if the slot is got from malloc, then free it */
    if (INVALID_NEXT_ADDR == thrArg->next) {
        free(thrArg);
        thrArg = NULL;
        gs_signal_recover_mask(old_set);
        return;
    }

    /* then release the slot to the thread_args_pool */
    SpinLockAcquire(&(thread_args_pool.thr_lock));
    thrArg->next = thread_args_pool.thr_args;
    thread_args_pool.thr_args = thrArg;
    SpinLockRelease(&(thread_args_pool.thr_lock));

    gs_signal_recover_mask(old_set);
    return;
}

/*
 * @@GaussDB@@
 * Brief		: gs_thread_args_free
 * Description	: free m_pThreadArg just for memcheck
 * Notes		:
 */
void gs_thread_args_free(void)
{
    if (t_thrd.port_cxt.m_pThreadArg != NULL) {
        /* if the slot is got from malloc, then free it */
        if (INVALID_NEXT_ADDR == t_thrd.port_cxt.m_pThreadArg->next) {
            free(t_thrd.port_cxt.m_pThreadArg);
            t_thrd.port_cxt.m_pThreadArg = NULL;
        }
    }
}

#ifdef ENABLE_QUNIT
slock_t g_qunit_thread_stack_map_lock;

const int g_qunit_estimate_thread_count = 1024;

static char* get_stack_space(size_t size)
{
    Assert(!(size & (size - 1)));

    char* stack_addr = NULL;
    int ret = posix_memalign((void**)&stack_addr, size, size + sizeof(u_sess->utils_cxt.qunit_case_number));
    if (ret == 0) {
        return stack_addr;
    }

    if (ret == EINVAL) {
        ereport(ERROR,
            (errmsg("[get_stack_space] The alignment argument was not a power of two,"
                    " or was not a multiple of sizeof(void *)")));
    }
    if (ret == ENOMEM) {
        ereport(ERROR, (errmsg("[get_stack_space] Out of memory")));
    }

    return NULL;
}

typedef struct ThreadHashKey {
    ThreadId tid;
} ThreadHashKey;

typedef struct ThreadStackAddrHashElem {
    ThreadHashKey key;
    char* stackAddr;
} HashElem;

static HTAB* g_threadStackAddrHashtable = NULL;

void saveThreadStackAddr(ThreadId tid, char* stack_addr)
{
    SpinLockAcquire(&(g_qunit_thread_stack_map_lock));
    if (g_threadStackAddrHashtable == NULL) {
        HASHCTL ctl;

        int ret = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(ret, "", "");
        ctl.keysize = sizeof(ThreadHashKey);
        ctl.entrysize = sizeof(ThreadStackAddrHashElem);
        ctl.hash = tag_hash;
        g_threadStackAddrHashtable =
            hash_create("Thread stack hash table", g_qunit_estimate_thread_count, &ctl, HASH_ELEM | HASH_FUNCTION);
    }

    ThreadHashKey key;
    ThreadStackAddrHashElem* elem = NULL;
    bool found = false;

    key.tid = tid;
    elem = (ThreadStackAddrHashElem*)hash_search(g_threadStackAddrHashtable, (void*)&key, HASH_ENTER, &found);
    if (found) {
        ereport(PANIC, (errmsg("Internal error: stack info is in the hash table unexpectedly.")));
    }

    elem->stackAddr = stack_addr;

    SpinLockRelease(&(g_qunit_thread_stack_map_lock));
}

void free_thread_stack(ThreadId tid)
{
    SpinLockAcquire(&(g_qunit_thread_stack_map_lock));
    ThreadHashKey key;
    ThreadStackAddrHashElem* elem = NULL;
    bool found = false;

    key.tid = tid;
    elem = (ThreadStackAddrHashElem*)hash_search(g_threadStackAddrHashtable, (void*)&key, HASH_REMOVE, &found);
    if (!found) {
        ereport(PANIC, (errmsg("Internal error: can not find stack info.")));
    }

    free(elem->stackAddr);

    SpinLockRelease(&(g_qunit_thread_stack_map_lock));
}

#endif

/*
 * @@GaussDB@@
 * Brief		: create a thread  with  argc/argv parameters.
 * Description	: this interfacce is designed for currently mimic fokexec interface. callers
 * 			  shall not worry on releasing memory associate with.
 * Notes		:
 */
int gs_thread_create(gs_thread_t* th, void* (*taskRoutine)(void*), int argc, void* argv)
{
    ThreadArg* pArg = NULL;
    int error_code = 0;
    bool needFree = false;

    pArg = (ThreadArg*)argv;
    if (argv == NULL) {
        /*
         * just special thread which not exit at gaussdb runtime, so the pArg no need to free. for example: signal
         * monitor thread. if a thread can exit at running time of DBMS, the argv must not NULL
         */
        pArg = (ThreadArg*)malloc(sizeof(ThreadArg));
        if (pArg == NULL) {
            return -1;
        }

        /*
         * set the next pointer, and pArg will be freed before exiting if
         * a new thread is created successfully.
         * otherwise, set needFree flag and free pArg immediately following the call pthread_create().
         *
         * see also gs_thread_release_args_slot().
         */
        pArg->next = (struct ThreadArg*)INVALID_NEXT_ADDR;
        needFree = true;
    }

    pArg->m_taskRoutine = taskRoutine;

    {
        pthread_attr_t pThreadAttr;
        size_t size;

        /*
         * Use default attributes.
         * Don't set to detached state as pthread_t can be immediately reused. Instead
         * we release kernel resource during repear.
         */
        error_code = pthread_attr_init(&pThreadAttr);
        if (error_code != 0) {
            if (needFree) {
                free(pArg);
                pArg = NULL;
            }

            return error_code;
        }

        size = DEFUALT_STACK_SIZE * 1024L;

#ifdef ENABLE_QUNIT
        char* stack_addr = get_stack_space(size);
        Assert(stack_addr != NULL);
        pthread_attr_setstack(&pThreadAttr, stack_addr, size);
#endif

        pthread_attr_setstacksize(&pThreadAttr, size);
        error_code = pthread_attr_setdetachstate(&pThreadAttr, PTHREAD_CREATE_JOINABLE);
        /* Create a pthread */
        if (error_code == 0) {
            error_code = pthread_create(&th->thid, &pThreadAttr, ThreadStarterFunc, pArg);
        }

#ifdef ENABLE_QUNIT
        saveThreadStackAddr(th->thid, stack_addr);
#endif

        (void)pthread_attr_destroy(&pThreadAttr);
        if (error_code != 0 && needFree) {
            free(pArg);
            pArg = NULL;
        }
    }

    return error_code;
}

/*
 * @@GaussDB@@
 * Brief		: join the other thread
 * Description	:
 * Notes		:
 */
int gs_thread_join(gs_thread_t thread, void** value_ptr)
{
#ifndef WIN32
#define PTHREAD_JOIN_TIMEOUT 300

    struct timespec ts;
    int ret;

    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        ereport(WARNING, (errmsg("clock gettime failed before join thread %lu : %m", gs_thread_id(thread))));

#ifdef ENABLE_QUNIT
        free_thread_stack(thread.thid);
#endif

        return -1;
    }

    ts.tv_sec += PTHREAD_JOIN_TIMEOUT;

    ret = pthread_timedjoin_np(gs_thread_id(thread), value_ptr, &ts);
    if (ret != 0) {
        ereport(
            WARNING, (errmsg("failed to join thread %lu, ret is %d: %s", gs_thread_id(thread), ret, gs_strerror(ret))));
    }

#ifdef ENABLE_QUNIT
    free_thread_stack(thread.thid);
#endif

    return ret;
#else
    if (thread.os_handle == NULL)
        return errno = EINVAL;

    if (WaitForSingleObject(thread.os_handle, INFINITE) != WAIT_OBJECT_0) {
        _dosmaperr(GetLastError());
        return errno;
    }

    CloseHandle(thread.os_handle);

    return 0;
#endif
}

void ThreadExitCXX(int code)
{
    t_thrd.port_cxt.thread_is_exiting = true;

    try {
#ifdef WIN32
        _endthreadex((unsigned)(code));
#else
        pthread_exit((void*)(size_t)(code));
#endif
    } catch (abi::__forced_unwind&) {
        throw;
    }
}

int ShowThreadName(const char* name)
{
    int rc;
    t_thrd.proc_cxt.MyProgName = const_cast<char*>(name);
#ifndef WIN32
    rc = pthread_setname_np(gs_thread_self(), name);
    Assert(rc == 0);
#endif
    return rc;
}

void uuid_struct_destroy_function()
{
    if (uuid_struct_destroy_hook != NULL) {
        uuid_struct_destroy_hook(0);
        uuid_struct_destroy_hook(1);
        uuid_struct_destroy_hook = NULL;
    }
}

/* release kerberos profile path which is thread local var */
static void clean_kerberos_cache()
{
#ifdef ENABLE_GSS
    krb5_clean_cache_profile_path();
#endif
}

#endif
