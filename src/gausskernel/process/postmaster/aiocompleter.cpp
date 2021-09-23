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
 * aiocompleter.cpp
 *
 * The AIO completer threads complete Prefetch and BackWrite I/O so they
 * may only be stopped after all the worker threads or bgwrite threads have
 * been stopped.
 *
 * The aiocompleter threads complete  AIO requests using Linux Native AIO.
 * A single AIO completer thread serves on AIO queue associated with a
 * specific AIO context and I/O priority.
 *
 * The AIO completer threads are started by the postmaster as soon as the
 * startup subprocess finishes, or as soon as recovery begins if we are
 * doing archive recovery.  They remain alive until the postmaster commands
 * them to terminate.  Normal termination is by SIGTERM, which instructs the
 * threads to wait for any pending AIO and be prepared to exit.
 * via exit(0).  Emergency termination is by SIGQUIT.
 *
 * If completer thread exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/aiocompleter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/aiocompleter.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include <pthread.h>

/*
 * Each AIO completer thread has a unique context, and potentially
 * processes different types of requests.  There is an aioCompltrThread_t
 * structure for each completer thread, located in the compltrArray.
 * The compltrArray contains MAX_AIOCOMPLTR_THREADS slots.
 * The compltrArray is defined in the postmaster context.
 *
 * The first three parameters context, eventsp and tid
 * are set when the thread is started, they are unique to each thread.
 * The cmpltrDesc pointer points to a AioCompltrDesc_t structure in the
 * compltrDescArray containing the parameters for the completer.
 * Completers of the same type use the same descriptor.
 * The compltrDescArray has NUM_AIOCOMPLTR_TYPES AioCompltrDesc_t structures
 * one for each AioCompltrType.
 * The compltrDescArray array is defined in the postmaster context.
 *
 */
int CompltrReadReq(void* aioDesc, long res);
int CompltrWriteReq(void* aioDesc, long res);
int CompltrReadCUReq(void* aioDesc, long res);
int CompltrWriteCUReq(void* aioDesc, long res);

ThreadId Compltrfork_exec(int compltrIdx);

/*
 * GUC parameters
 */
/* Maximum number of Completer threads -compile time define */
#define MAX_AIOCOMPLTR_THREADS 4

/* Number of Completer threads and the number of sets of Completers */
const int AioCompltrThreads = 4;
int AioCompltrSets = 1;
const int AioCompltrShutdownTimeout = 1;

/* Completer callback to handle the AIO event */
typedef int (*AioCallback_t)(void*, long);

/*
 * Completer Thread definitions
 */
typedef struct AioCompltrDesc {
    /* Completer characteristics */
    AioCompltrType reqtype; /* Completer type */
    AioCallback_t callback; /* Completer function */
    int maxevents;          /* AIO Maximum events in progress */

    /* Completer parameters */
    int min_nr;  /* Min number of events to wait for */
    int max_nr;  /* Max number of events to retrieve */
    int timeout; /* Max time to wait */

    /* Request properties */
    AioPriority reqprio; /* Request priority served */

} AioCompltrDesc_t;

typedef struct {
    io_context_t context;           /* AIO context */
    struct io_event* eventsp;       /* AIO events to process */
    ThreadId tid;                   /* AIO thread tid */
    AioCompltrDesc_t* compltrDescp; /* Completer descriptor */
} AioCompltrThread_t;

/*
 * The compltrDescArray contains the description of the different types
 * of completer threads. These are used to setup the context for each
 * completer thread. Each type could potentially has a unique set
 * of parameters.
 *
 * For now, only the completer function and priority varies.
 * The rest of the values are the same for all the threads:
 *
 * reqtype and callback
 * The reqtype is the type of async I/O request.  The
 * callback is the function used to process the request.
 * The completers are configured to each handle a different type
 * of request.  The type of request dictates the callback used.
 * At least that is the case now, this could change in the future.
 *
 * maxevents
 * The number of requests in the queue for the context is limited to 64K here.
 * So in total, the 4 threads require 64k * 4 io descriptors.
 * Typically the system io descriptor maximum is 64k.
 * Verify that there are sufficient available by checking aio-max-nr.
 * Compare /proc/sys/fs/aio-nr against /proc/sys/fs/aio-max-nr to determine
 * whether the limit is too small. It is advisable to set fs.aio-max-nr to
 * 1048576 (1m) in /etc/sysctl.conf.
 *
 * min_nr, max_nr and timeout
 * The minimum number of requests to return is set to 1, this
 * makes io_getevents a blocking call that sleeps until one i/o is available.
 * The maximum number of requests in the queue is arbitrarily set
 * to here, but it could be as large as the entire queue.
 * The maximum wait is set to 10 minutes, but any nonzero value will allow
 * io_getevents() to enter an interruptable sleep (Returning -EINTR
 * when a signal arrives).
 *
 * priority
 * The priority values are intended to give specific Page List prefetchs the
 * highest priority, while giving Range prefetch and Range write-back a
 * medium priority, and the periodic write-back the lowest priority.
 * The effectiveness of these relative settings depends upon the i/o scheduling
 * policy employed.  CFQ takes into account the priorities, but there is also a
 * wide gulf between the priority of sync and async i/o that dwarfs these.
 */
AioCompltrDesc_t compltrDescArray[NUM_AIOCOMPLTR_TYPES] = {
    /* reqtype,          callback,       maxevents, min_nr, max_nr, tsec, reqprio */
    {PageListPrefetchType, CompltrReadReq, 65536, 1, 16384, 60, HighPri},
    {PageListBackWriteType, CompltrWriteReq, 65536, 1, 16384, 60, HighPri},
    {CUListPrefetchType, CompltrReadCUReq, 65536, 1, 16384, 60, HighPri},
    {CUListWriteType, CompltrWriteCUReq, 65536, 1, 16384, 60, HighPri}};

/*
 *  AIO Completer Array defines the AIO completer threads, it is
 *  initialized by the postmaster using CompltrAioInit prior to starting
 *  any AIO Completer theads. The Array contains one element for
 *  each completer thread.
 */
AioCompltrThread_t compltrArray[MAX_AIOCOMPLTR_THREADS];

/*
 * AioCompltrReady flag is set/cleared from the postmaster
 * context.  It is used to remember the Completer state.
 */
static bool volatile AioCompltrReady = false;

/* Associate a template with a thread index */
#define AIOCOMPLTR_TEMPLATE(threadIdx) (&compltrDescArray[(threadIdx) % NUM_AIOCOMPLTR_TYPES])

/* Determine a completer thread index to be used for a given type and h val */
#define AIOCOMPLTR_THREAD_IDX(typeIdx, h) ((((h) % AioCompltrSets) * NUM_AIOCOMPLTR_TYPES) + (typeIdx))

/*
 * Exported functions
 */
/*
 * @Description: Check whether the Completers have been started
 * @Return: true start ok
 * @See also:
 */
bool AioCompltrIsReady(void)
{
    return AioCompltrReady;
}

/*
 * @Description:  Obtain callback for request type
 * @Param[IN] reqType: aio completer type
 * @Return: function ptr
 * @See also:
 */
AioCallback_t ComptrCallback(AioCompltrType reqType)
{
    return compltrDescArray[reqType].callback;
}

/*
 * @Description: Obtain the priority for the request type
 * @Param[IN] reqType: aio completer type
 * @Return: Request priority
 * @See also:
 */
short CompltrPriority(AioCompltrType reqType)
{
    return compltrDescArray[reqType].reqprio;
}

/*
 * @Description:  Obtain the completer context for the i/o request, Must consult the AioCompltrArray
 *  to find the specific thread to  get its context.
 * @Param[IN] h:  index
 * @Param[IN] reqType: aio completer type
 * @Return:io_context
 * @See also:
 */
io_context_t CompltrContext(AioCompltrType reqType, int h)
{
    return compltrArray[AIOCOMPLTR_THREAD_IDX(reqType, h)].context;
}

/* Prototypes for private functions */
/*
 * Signal handlers
 */
static void CompltrConfig(SIGNAL_ARGS);
static void CompltrQuickDie(SIGNAL_ARGS);
static void CompltrShutdown(SIGNAL_ARGS);

/*
 * @Description:  Compltrfork_exec() and AioCompltrStart() are used to start the
 * completer threads.
 *
 * PG was designed to start processes and pass parameters on the
 * command line and via shared memory.  We do not need to do
 * that with our threads implementation, but rather than change all that
 * now we are going to pass the compltrIdx on the command line
 * and allow the running thread to find its descriptor in the compltrArray
 * in the global context.
 *
 * Compltrfork_exec formats the arglist, then fork and exec the AIO
 * Completer thread.  The compltrIdx is converted to a 3 character string,
 * so that the parameter does not have to be handled specially by the
 * intervening PG code.
 * @Param[IN] compltrIdx: aio thread index
 * @Return: thread id
 * @See also:
 */
ThreadId Compltrfork_exec(int compltrIdx)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("openGauss current do not support AIO")));
    return InvalidTid;
}

/*
 * @Description: AioCompltrStart
 * Set-up the Aio Completer thread descriptors and start the threads.
 * This function is invoked in the global context by the postmaster
 * to start all the Aio Completer Threads.
 *
 * Globals
 * The compltrArray and AioCompltrThreads
 * are globals that control the allocation and configuration of the
 * AIO completer threads.
 *
 * AioCompltrThreads is the number of AIO completer threads.
 * Each Completer thread has a unique AioCompltrThread_t context in the
 * compltrArray.  Each Completer refers to a compltrDescp that
 * contains the parameters for the type of request the Completer servers.
 * Each Completer context also contains an AIO context and event array.
 * @Return: 0 --success; error --failed
 * @See also:
 */
int AioCompltrStart(void)
{
    int error = 0;
    int try_times = 0;

    /*
     * Only allow MAX_AIOCOMPLTR_THREADS
     */
    if (AioCompltrThreads > MAX_AIOCOMPLTR_THREADS) {
        error = 1;
        return error;
    }

    errno_t rc = memset_s(&compltrArray,
        sizeof(AioCompltrThread_t) * MAX_AIOCOMPLTR_THREADS,
        0,
        sizeof(AioCompltrThread_t) * MAX_AIOCOMPLTR_THREADS);
    securec_check(rc, "\0", "\0");

    /*
     * Initialize the compltrArray
     */
    for (int i = 0; i < AioCompltrThreads; i++) {
        try_times = 0;
        /* Assign a template to the thread descriptor */
        compltrArray[i].compltrDescp = AIOCOMPLTR_TEMPLATE(i);

        /* Create the i/o queue and fill in the context */
        do {
            error = io_setup(compltrArray[i].compltrDescp->maxevents, &compltrArray[i].context);
            if (error == 0 || error != -EAGAIN) {
                break;
            }

            try_times++;
            ereport(LOG, (errmsg("AIO Startup, Completer thread id =%d try times=%d, error=%d", i, try_times, error)));
            pg_usleep(100000L);
        } while (try_times < 5);

        if (error != 0) {
            goto AioCompltrStartError;
        }

        /* Allocate the event array for the thread */
        compltrArray[i].eventsp = (io_event*)malloc(compltrArray[i].compltrDescp->max_nr * sizeof(struct io_event));

        if (compltrArray[i].eventsp == (struct io_event*)NULL) {

            /* malloc failed for some reason... */
            error = 2;
            ereport(LOG,
                (errmsg("AIO Startup malloc io_event failed: max_nr(%d), %d",
                    compltrArray[i].compltrDescp->max_nr,
                    error)));
            goto AioCompltrStartError;
        }

        /* Start AIO Completer thread */
        compltrArray[i].tid = Compltrfork_exec(i);

        if (compltrArray[i].tid == (ThreadId)-1) {
            /* starting a thread failed */
            error = 3;
            ereport(LOG, (errmsg("Start AIO Completer thread failed: %d", error)));
            goto AioCompltrStartError;
        }
    };

    /* The AIO Completers are open for business */
    AioCompltrReady = true;

    /* successful return */
    return 0;

AioCompltrStartError:
    /*
     * If anything went wrong, then stop any threads started
     * and deallocate the resources.
     */
    AioCompltrStop(SIGTERM);
    ereport(LOG, (errmsg("AIO Startup Failed,error=%d", error)));

    return error;
}

/*
 * @Description: Stop the Completer threads, cleanup any partially started ones.
 * Send SIGQUIT and forget about the threads.
 * The caller must ensure that no AIO is in progress prior to using this  function.
 * @Param[IN] signal:signal
 * @See also:
 */
void AioCompltrStop(int signal)
{
    gs_thread_t thread;
    AioCompltrReady = false;

    /*
     * Stop the threads in the compltrArray.
     */
    for (int i = 0; i < AioCompltrThreads; i++) {
        /*
         * Stop the threads that were started
         */
        if (compltrArray[i].tid != 0) {
            if (gs_signal_send(compltrArray[i].tid, signal) < 0) {
                ereport(LOG, (errmsg("kill(%ld,%d) failed: %m", (long)(compltrArray[i].tid), signal)));
            }
        }
    }

    if (signal == SIGQUIT) {
        /*
         * if the database is crashing just kill the completers
         * and bail-out.
         */
        return;
    }

    /*
     * Wait for the stopped threads to exit.
     */
    for (int i = 0; i < AioCompltrThreads; i++) {
        /*
         * Wait for the killed threads to exit
         */
        if (compltrArray[i].tid != 0) {
            thread.thid = compltrArray[i].tid;
            if (gs_thread_join(thread, NULL) != 0) {
                /*
                 * If the thread does not exist, treat it as normal exit and we continue to
                 * do our clean-up work. Otherwise, we treate it as crashed 'cause we do
                 * not know the current status of the thread and it's better to quit directly
                 * which sames more safely.
                 */
                if (ESRCH == pthread_kill(thread.thid, 0))
                    ereport(LOG, (errmsg("failed to join thread %lu, no such process", thread.thid)));
                else
                    HandleChildCrash(thread.thid, 1, "AIO process");
            }
            compltrArray[i].tid = (pid_t)0;
        }
    }

    /*
     * Deallocate their context and event arrays, if any
     */
    for (int i = 0; i < AioCompltrThreads; i++) {
        compltrArray[i].compltrDescp = (AioCompltrDesc_t*)NULL;

        /* destroy the AIO context */
        if (compltrArray[i].context) {
            io_destroy(compltrArray[i].context);
            compltrArray[i].context = (io_context_t)NULL;
        }

        /* Deallocate the events array */
        if (compltrArray[i].eventsp) {
            free(compltrArray[i].eventsp);
            compltrArray[i].eventsp = (struct io_event*)NULL;
        }
    }

    /* successful return */
    return;
}

/*
 * @Description:  Main entry point for an AIO Completer thread
 * @Param[IN] ac: param count
 * @Param[IN] av: param list
 * @See also:
 */
void AioCompltrMain(int ac, char** av)
{
    if (ac < 4) {
        ereport(WARNING, (errmsg("invalid AIO argument num:%d", ac)));
        exit(1);
    }
    /* compltrIdx identifies this thread. */
    int compltrIdx = atoi(av[3]);

    /*
     * Global thread local shortcuts to the completer descriptor
     * in the compltrArray, these are assigned on entry.
     */
    io_context_t context = compltrArray[compltrIdx].context;
    io_event* eventsp = compltrArray[compltrIdx].eventsp;
    AioCompltrDesc_t* compltrDescp = compltrArray[compltrIdx].compltrDescp;
    int min_nr = compltrDescp->min_nr;
    int max_nr = compltrDescp->max_nr;
    struct timespec timeout;
    struct timespec shutdown_timeout;
    timeout.tv_sec = compltrDescp->timeout;
    timeout.tv_nsec = 0;
    shutdown_timeout.tv_sec = AioCompltrShutdownTimeout;
    shutdown_timeout.tv_nsec = 0;
    AioCallback_t callback = compltrDescp->callback;

    /*
     * Handle signals the postmaster might send us
     * SIGTERM causes the thread to prepare to exit
     * SIGQUIT causes immediate exit without cleanup.
     * SIGUSR1 is presently unused- reserved for future use.
     */
    (void)gspqsignal(SIGHUP, CompltrConfig); /* retrieve config */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, CompltrShutdown); /* shutdown */
    (void)gspqsignal(SIGQUIT, CompltrQuickDie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN); /* reserved */
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but we don't
     * need.
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Create a resource owner to keep track of our resources (buffer
     * pins etc...
     *
     * Create a memory context if we ever allocate memory here...
     *
     * Handle exceptions like from ereport, elog if we do not want
     * to just die...
// AioCompltrMain() perhaps need better exception handling?  jeh
     */
    /*
     * Unblock signals (blocked when postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Announce that the Completer has been started */
    ereport(LOG, (errmsg("AIO Completer %d STARTED.", compltrIdx)));

    for (;;) {
        int eventsReceived;

        /*
         * Reload configuration -if requested.
         */
        if (t_thrd.aio_cxt.config_requested) {
            /* disabled config request, not time for this now. ProcessConfigFile PGC_SIGHUP; */
            t_thrd.aio_cxt.config_requested = false;
        }

        /*
         * If shutdown is requested, set shutdown_pending and
         * restart io_getevents() if it was interrupted.
         * The io_getevents() timeout is reduced to shutdown_timeout to
         * allow the thread to exit quickly when its time comes.
         * Once shutdown is requested, there is no going back.
         */
        if (t_thrd.aio_cxt.shutdown_requested) {
            timeout = shutdown_timeout;

            ereport(LOG, (errmsg("AIO Completer %d EXITED.", compltrIdx)));
            proc_exit(0);
        }

        /*
         * Wait for some AIO request(s) to complete
         * on the given context. Retry if the syscall is
         * interrupted.
         */
        eventsReceived = io_getevents(context, min_nr, max_nr, eventsp, &timeout);
        /*
         * If io_getevents() got interrupted,
         * take the opportunity to check for pending requests.
         * Then restart the io_getevents() call.
         */
        if (eventsReceived == -EINTR) {
            continue;
        }

        /*
         * io_getevents() reports errors as negative values.
         */
        if (eventsReceived < 0) {
            /* Report error */
            ereport(PANIC, (errmsg("AIO Completer io_getevents() failed: error %d .", eventsReceived)));
        }

        Assert(eventsReceived <= max_nr);

        /*
         * Call the callback for each event returned
         * We expect 0 to max_nr requests. The obj here is
         * the I/O request and the db context.
         */
        for (struct io_event* eventp = eventsp; eventsReceived--; eventp++) {
            callback((void*)eventp->obj, eventp->res);
        }
    }

    ereport(LOG, (errmsg("AIO Completer %d EXITED.", compltrIdx)));
    exit(0);
}

/*
 * @Description: signal handler routines for config,not used now
 * @See also:
 */
static void CompltrConfig(SIGNAL_ARGS)
{
    t_thrd.aio_cxt.config_requested = true;
}

/*
 * @Description:  CompltrQuickDie() occurs when signalled SIGQUIT by the postmaster.
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 * @See also:
 */
static void CompltrQuickDie(SIGNAL_ARGS)
{
    PG_SETMASK(&t_thrd.libpq_cxt.BlockSig);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted.  Like the other postmaster
     * children, ...Just nail the windows shut and get out of town....
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0)...
     * ...just like the other postmaster children.
     */
    exit(2);
}

/*
 * @Description: CompltrShutdown() occurs when signalled SIGTERM by the postmaster.
 * @See also:
 */
static void CompltrShutdown(SIGNAL_ARGS)
{
    t_thrd.aio_cxt.shutdown_requested = true;
}

/**
 * @Description: Initialize  Resource used by adio
 * @in  void
 * @return void
 */
void AioResourceInitialize(void)
{
    AdioSharedContext = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
        "AdioSharedMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
}
