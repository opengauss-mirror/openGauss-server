/* -------------------------------------------------------------------------
 *
 * sinvaladt.cpp
 *	  openGauss shared cache invalidation data manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/sinvaladt.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "miscadmin.h"
#include "access/multi_redo_api.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "gs_thread.h"
#include "threadpool/threadpool.h"

/*
 * Conceptually, the shared cache invalidation messages are stored in an
 * infinite array, where maxMsgNum is the next array subscript to store a
 * submitted message in, minMsgNum is the smallest array subscript containing
 * a message not yet read by all backends, and we always have maxMsgNum >=
 * minMsgNum.  (They are equal when there are no messages pending.)  For each
 * active backend, there is a nextMsgNum pointer indicating the next message it
 * needs to read; we have maxMsgNum >= nextMsgNum >= minMsgNum for every
 * backend.
 *
 * (In the current implementation, minMsgNum is a lower bound for the
 * per-process nextMsgNum values, but it isn't rigorously kept equal to the
 * smallest nextMsgNum --- it may lag behind.  We only update it when
 * SICleanupQueue is called, and we try not to do that often.)
 *
 * In reality, the messages are stored in a circular buffer of MAXNUMMESSAGES
 * entries.  We translate MsgNum values into circular-buffer indexes by
 * computing MsgNum % MAXNUMMESSAGES (this should be fast as long as
 * MAXNUMMESSAGES is a constant and a power of 2).	As long as maxMsgNum
 * doesn't exceed minMsgNum by more than MAXNUMMESSAGES, we have enough space
 * in the buffer.  If the buffer does overflow, we recover by setting the
 * "reset" flag for each backend that has fallen too far behind.  A backend
 * that is in "reset" state is ignored while determining minMsgNum.  When
 * it does finally attempt to receive inval messages, it must discard all
 * its invalidatable state, since it won't know what it missed.
 *
 * To reduce the probability of needing resets, we send a "catchup" interrupt
 * to any backend that seems to be falling unreasonably far behind.  The
 * normal behavior is that at most one such interrupt is in flight at a time;
 * when a backend completes processing a catchup interrupt, it executes
 * SICleanupQueue, which will signal the next-furthest-behind backend if
 * needed.	This avoids undue contention from multiple backends all trying
 * to catch up at once.  However, the furthest-back backend might be stuck
 * in a state where it can't catch up.  Eventually it will get reset, so it
 * won't cause any more problems for anyone but itself.  But we don't want
 * to find that a bunch of other backends are now too close to the reset
 * threshold to be saved.  So SICleanupQueue is designed to occasionally
 * send extra catchup interrupts as the queue gets fuller, to backends that
 * are far behind and haven't gotten one yet.  As long as there aren't a lot
 * of "stuck" backends, we won't need a lot of extra interrupts, since ones
 * that aren't stuck will propagate their interrupts to the next guy.
 *
 * We would have problems if the MsgNum values overflow an integer, so
 * whenever minMsgNum exceeds MSGNUMWRAPAROUND, we subtract MSGNUMWRAPAROUND
 * from all the MsgNum variables simultaneously.  MSGNUMWRAPAROUND can be
 * large so that we don't need to do this often.  It must be a multiple of
 * MAXNUMMESSAGES so that the existing circular-buffer entries don't need
 * to be moved when we do it.
 *
 * Access to the shared sinval array is protected by two locks, SInvalReadLock
 * and SInvalWriteLock.  Readers take SInvalReadLock in shared mode; this
 * authorizes them to modify their own ProcState but not to modify or even
 * look at anyone else's.  When we need to perform array-wide updates,
 * such as in SICleanupQueue, we take SInvalReadLock in exclusive mode to
 * lock out all readers.  Writers take SInvalWriteLock (always in exclusive
 * mode) to serialize adding messages to the queue.  Note that a writer
 * can operate in parallel with one or more readers, because the writer
 * has no need to touch anyone's ProcState, except in the infrequent cases
 * when SICleanupQueue is needed.  The only point of overlap is that
 * the writer wants to change maxMsgNum while readers need to read it.
 * We deal with that by having a spinlock that readers must take for just
 * long enough to read maxMsgNum, while writers take it for just long enough
 * to write maxMsgNum.	(The exact rule is that you need the spinlock to
 * read maxMsgNum if you are not holding SInvalWriteLock, and you need the
 * spinlock to write maxMsgNum unless you are holding both locks.)
 *
 * Note: since maxMsgNum is an int and hence presumably atomically readable/
 * writable, the spinlock might seem unnecessary.  The reason it is needed
 * is to provide a memory barrier: we need to be sure that messages written
 * to the array are actually there before maxMsgNum is increased, and that
 * readers will see that data after fetching maxMsgNum.  Multiprocessors
 * that have weak memory-ordering guarantees can fail without the memory
 * barrier instructions that are included in the spinlock sequences.
 */
/*
 * Configurable parameters.
 *
 * MAXNUMMESSAGES: max number of shared-inval messages we can buffer.
 * Must be a power of 2 for speed.
 *
 * MSGNUMWRAPAROUND: how often to reduce MsgNum variables to avoid overflow.
 * Must be a multiple of MAXNUMMESSAGES.  Should be large.
 *
 * CLEANUP_MIN: the minimum number of messages that must be in the buffer
 * before we bother to call SICleanupQueue.
 *
 * CLEANUP_QUANTUM: how often (in messages) to call SICleanupQueue once
 * we exceed CLEANUP_MIN.  Should be a power of 2 for speed.
 *
 * SIG_THRESHOLD: the minimum number of messages a backend must have fallen
 * behind before we'll send it PROCSIG_CATCHUP_INTERRUPT.
 *
 * WRITE_QUANTUM: the max number of messages to push into the buffer per
 * iteration of SIInsertDataEntries.  Noncritical but should be less than
 * CLEANUP_QUANTUM, because we only consider calling SICleanupQueue once
 * per iteration.
 */
#define MAXNUMMESSAGES 4096
#define MSGNUMWRAPAROUND (MAXNUMMESSAGES * 262144)
#define CLEANUP_MIN (MAXNUMMESSAGES / 2)
#define CLEANUP_QUANTUM (MAXNUMMESSAGES / 16)
#define SIG_THRESHOLD (MAXNUMMESSAGES / 2)
#define WRITE_QUANTUM 64

/* Per-backend state in shared invalidation structure */
typedef struct ProcState {
    /* procPid is zero in an inactive ProcState array entry. */
    ThreadId procPid; /* PID of backend, for signaling */
    PGPROC* proc;     /* PGPROC of backend */
    /* nextMsgNum is meaningless if procPid == 0 or resetState is true. */
    int nextMsgNum;   /* next message number to read */
    bool resetState;  /* backend needs to reset its state */
    bool signaled;    /* backend has been sent catchup signal */
    bool hasMessages; /* backend has unread messages */

    /*
     * Backend only sends invalidations, never receives them. This only makes
     * sense for Startup process during recovery because it doesn't maintain a
     * relcache, yet it fires inval messages to allow query backends to see
     * schema changes.
     */
    bool sendOnly; /* backend only sends, never receives */

    /*
     * Next LocalTransactionId to use for each idle backend slot.  We keep
     * this here because it is indexed by BackendId and it is convenient to
     * copy the value to and from local memory when t_thrd.proc_cxt.MyBackendId is set. It's
     * meaningless in an active ProcState entry.
     */
    LocalTransactionId nextLXID;
} ProcState;

/* Shared cache invalidation memory segment */
typedef struct SISeg {
    /*
     * General state information
     */
    int minMsgNum;     /* oldest message still needed */
    int maxMsgNum;     /* next message number to be assigned */
    int nextThreshold; /* # of messages to call SICleanupQueue */
    int lastBackend;   /* index of last active procState entry, +1 */
    int lastreserveBackend;
    int maxBackends; /* size of procState array */
    int maxreserveBackends;
    slock_t msgnumLock; /* spinlock protecting maxMsgNum */

    /*
     * Circular buffer holding shared-inval messages
     */
    SharedInvalidationMessageEx buffer[MAXNUMMESSAGES];

    /*
     * Per-backend state info.
     *
     * We declare procState as 1 entry because C wants a fixed-size array, but
     * actually it is maxBackends entries long.
     */
    ProcState procState[1]; /* reflects the invalidation state */
} SISeg;

static void CleanupInvalidationState(int status, Datum arg);

/*
 * SInvalShmemSize --- return shared-memory space needed
 */
Size SInvalShmemSize(void)
{
    Size size;

    size = offsetof(SISeg, procState);
    if (g_instance.attr.attr_common.enable_thread_pool) {
        if (EnableGlobalSysCache()) {
            /* palloc another g_instance.shmem_cxt.MaxBackends for gsc, register share msg event for threads */
            size = add_size(size, mul_size(sizeof(ProcState), MAX_THREAD_POOL_SIZE +
                MAX_SESSION_SLOT_COUNT));
        } else {
            size = add_size(size, mul_size(sizeof(ProcState), MAX_SESSION_SLOT_COUNT));
        }
    }
    else
        size = add_size(size, mul_size(sizeof(ProcState), (Size)g_instance.shmem_cxt.MaxBackends));

    return size;
}

/*
 * SharedInvalBufferInit
 *		Create and initialize the SI message buffer
 */
void CreateSharedInvalidationState(void)
{
    Size size;
    int i;
    bool found = false;

    /* Allocate space in shared memory */
    size = SInvalShmemSize();

    t_thrd.shemem_ptr_cxt.shmInvalBuffer = (SISeg*)ShmemInitStruct("shmInvalBuffer", size, &found);

    if (found) {
        return;
    }

    /* Clear message counters, save size of procState array, init spinlock */
    t_thrd.shemem_ptr_cxt.shmInvalBuffer->minMsgNum = 0;
    t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxMsgNum = 0;
    t_thrd.shemem_ptr_cxt.shmInvalBuffer->nextThreshold = CLEANUP_MIN;
    t_thrd.shemem_ptr_cxt.shmInvalBuffer->lastBackend = 0;
    t_thrd.shemem_ptr_cxt.shmInvalBuffer->lastreserveBackend = 0;

    if (g_instance.attr.attr_common.enable_thread_pool) {
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxBackends = MAX_SESSION_SLOT_COUNT;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxreserveBackends = GLOBAL_RESERVE_SESSION_NUM;
    } else {
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxBackends = g_instance.shmem_cxt.MaxBackends;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxreserveBackends = g_instance.shmem_cxt.MaxBackends;
    }
    SpinLockInit(&t_thrd.shemem_ptr_cxt.shmInvalBuffer->msgnumLock);

    /* The buffer[] array is initially all unused, so we need not fill it */
    /* Mark all backends inactive, and initialize nextLXID */
    for (i = 0; i < t_thrd.shemem_ptr_cxt.shmInvalBuffer->maxBackends; i++) {
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].procPid = 0; /* inactive */
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].proc = NULL;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].nextMsgNum = 0; /* meaningless */
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].resetState = false;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].signaled = false;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].hasMessages = false;
        t_thrd.shemem_ptr_cxt.shmInvalBuffer->procState[i].nextLXID = InvalidLocalTransactionId;
    }
}

/*
 * SharedInvalReserveBackendInit
 *		Initialize a new Reserve backend to operate on the sinval buffer
 */
static void SharedInvalReserveBackendInit(bool sendOnly)
{
    int index;
    ProcState* stateP = NULL;
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;

    /*
     * This can run in parallel with read operations, but not with write
     * operations, since SIInsertDataEntries relies on lastBackend to set
     * hasMessages appropriately.
     */
    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    /* Look for a free entry in the procState array */
    for (index = 0; index < segP->lastreserveBackend; index++) {
        if (segP->procState[index].procPid == 0) { /* inactive slot? */
            stateP = &segP->procState[index];
            break;
        }
    }

    if (stateP == NULL) {
        /* Fetch slot from reserved procState entry */
        if (segP->lastreserveBackend < segP->maxreserveBackends) {
            stateP = &segP->procState[segP->lastreserveBackend];
            Assert(stateP->procPid == 0);
            segP->lastreserveBackend++;
        } else {
            /*
             * no proc found
             *
             * out of procState slots: g_instance.shmem_cxt.MaxBackends exceeded -- report normally
             */
            t_thrd.proc_cxt.MyBackendId = InvalidBackendId;
            LWLockRelease(SInvalWriteLock);
            ereport(FATAL,
                    (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                     errmsg("sorry, too many reserve thread already lastreserveBackend:%d, maxreserveBackends:%d",
                            segP->lastreserveBackend,
                            segP->maxreserveBackends)));
        }
    }
    if (segP->lastreserveBackend > segP->lastBackend) {
        segP->lastBackend = segP->lastreserveBackend;
    }
    t_thrd.proc_cxt.MyBackendId = (stateP - &segP->procState[0]) + 1;

    /* Advertise assigned backend ID in t_thrd.proc */
    t_thrd.proc->backendId = t_thrd.proc_cxt.MyBackendId;
    if (t_thrd.proc_cxt.MyPMChildSlot > 0) {
        t_thrd.proc->backendSlot = t_thrd.proc_cxt.MyPMChildSlot;
    }

    /* Fetch next local transaction ID into local memory */
    u_sess->storage_cxt.nextLocalTransactionId = stateP->nextLXID;

    /* mark myself active, with all extant messages already read */
    stateP->procPid = t_thrd.proc_cxt.MyProcPid;
    stateP->proc = t_thrd.proc;
    stateP->nextMsgNum = segP->maxMsgNum;
    stateP->resetState = false;
    stateP->signaled = false;
    stateP->hasMessages = false;
    stateP->sendOnly = sendOnly;

    LWLockRelease(SInvalWriteLock);
}

/*
 * SharedInvalReserveBackendInit
 *		Initialize a new Reserve backend to operate on the sinval buffer
 */
static void SharedInvalWorkSessionInit(bool sendOnly)
{
    int index;
    ProcState* stateP = NULL;
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;

    /*
     * This can run in parallel with read operations, but not with write
     * operations, since SIInsertDataEntries relies on lastBackend to set
     * hasMessages appropriately.
     */
    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    index = u_sess->session_ctr_index;

    if ((index < segP->maxreserveBackends) || (index >= segP->maxBackends)) {
        /*
         * out of procState slots: g_instance.shmem_cxt.MaxBackends exceeded -- report normally
         */
        t_thrd.proc_cxt.MyBackendId = InvalidBackendId;
        LWLockRelease(SInvalWriteLock);
        ereport(FATAL,
                (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                 errmsg("sorry, session index isnot valid index:%d, maxreserveBackends:%d, maxBackends:%d",
                        index,
                        segP->maxreserveBackends,
                        segP->maxBackends)));
    }
    stateP = &segP->procState[index];

    if (stateP->procPid != 0) {
        LWLockRelease(SInvalWriteLock);
        ereport(FATAL,
                (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                 errmsg("sorry, session slot isnot valid:%d, maxreserveBackends:%d, maxBackends:%d",
                        index,
                        segP->maxreserveBackends,
                        segP->maxBackends)));
    }

    if ((index + 1) > segP->lastBackend) {
        segP->lastBackend = (index + 1);
    }

    /* Advertise assigned backend ID in t_thrd.proc */
    t_thrd.proc->backendId = t_thrd.proc_cxt.MyBackendId;
    if (t_thrd.proc_cxt.MyPMChildSlot > 0) {
        t_thrd.proc->backendSlot = t_thrd.proc_cxt.MyPMChildSlot;
    }

    /* Fetch next local transaction ID into local memory */
    u_sess->storage_cxt.nextLocalTransactionId = stateP->nextLXID;

    /* mark myself active, with all extant messages already read */
    stateP->procPid = t_thrd.proc_cxt.MyProcPid;
    stateP->proc = t_thrd.proc;
    stateP->nextMsgNum = segP->maxMsgNum;
    stateP->resetState = false;
    stateP->signaled = false;
    stateP->hasMessages = false;
    stateP->sendOnly = sendOnly;

    LWLockRelease(SInvalWriteLock);
}

/*
 * SharedInvalBackendInit
 *		Initialize a new backend to operate on the sinval buffer
 */
void SharedInvalBackendInit(bool sendOnly, bool worksession)
{
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;

    if (ENABLE_THREAD_POOL && (worksession)) {
        SharedInvalWorkSessionInit(sendOnly);
    } else {
        SharedInvalReserveBackendInit(sendOnly);

        /* register exit routine to mark my entry inactive at exit */
        on_shmem_exit(CleanupInvalidationState, PointerGetDatum(segP));
    }

    ereport(DEBUG4, (errmsg("my backend ID is %d, worksession :%d", t_thrd.proc_cxt.MyBackendId, worksession)));
}

/*
 * CleanupInvalidationState
 *		Mark the current backend as no longer active.
 *
 * This function is called via on_shmem_exit() during backend shutdown.
 *
 * arg is really of type "SISeg*".
 */
static void CleanupInvalidationState(int status, Datum arg)
{
    SISeg* segP = (SISeg*)DatumGetPointer(arg);
    ProcState* stateP = NULL;
    int i;

    Assert(PointerIsValid(segP));

    /*
     * Clear the invalid msg slot of any session that might still be attached to pool worker.
     * We need to do this after AbortOutOfAnyTransaction but before ProcKill.
     * Conservertively, we could just leave the slot as it is, which will eventually be marked as reset. However,
     * this might lead to inefficient invalid-msg-array traverse, because last backend is not refreshed in that case.
     */
    if (IS_THREAD_POOL_WORKER && u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM) {
        CleanupWorkSessionInvalidation();
    }

    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    stateP = &segP->procState[t_thrd.proc_cxt.MyBackendId - 1];

    /* Update next local transaction ID for next holder of this backendID */
    stateP->nextLXID = u_sess->storage_cxt.nextLocalTransactionId;

    /* Mark myself inactive */
    stateP->procPid = 0;
    stateP->proc = NULL;
    stateP->nextMsgNum = 0;
    stateP->resetState = false;
    stateP->signaled = false;

    /* Recompute index of last active backend */
    for (i = segP->lastBackend; i > 0; i--) {
        if (segP->procState[i - 1].procPid != 0)
            break;
    }
    /* update lastbackend */
    segP->lastBackend = i;

    /* Recompute index of last reserve active backend */
    for (i = segP->lastreserveBackend; i > 0; i--) {
        if (segP->procState[i - 1].procPid != 0)
            break;
    }
    /* update lastreserve backend */
    segP->lastreserveBackend = i;

    LWLockRelease(SInvalWriteLock);
}

void CleanupWorkSessionInvalidation(void)
{
    int index;
    ProcState* stateP = NULL;
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;
    int i;

    LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

    index = u_sess->session_ctr_index;
    stateP = &segP->procState[index];

    /* Update next local transaction ID for next holder of this backendID */
    stateP->nextLXID = u_sess->storage_cxt.nextLocalTransactionId;

    /* Mark myself inactive */
    stateP->procPid = 0;
    stateP->proc = NULL;
    stateP->nextMsgNum = 0;
    stateP->resetState = false;
    stateP->signaled = false;

    /* Recompute index of last active backend */
    for (i = segP->lastBackend; i > 0; i--) {
        if (segP->procState[i - 1].procPid != 0)
            break;
    }
    /* update lastbackend */
    segP->lastBackend = i;

    LWLockRelease(SInvalWriteLock);
}

/*
 * BackendIdGetProc
 *		Get the PGPROC structure for a backend, given the backend ID.
 *		The result may be out of date arbitrarily quickly, so the caller
 *		must be careful about how this information is used.  NULL is
 *		returned if the backend is not active.
 */
PGPROC* BackendIdGetProc(int backendID)
{
    PGPROC* result = NULL;
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;

    /* Need to lock out additions/removals of backends */
    LWLockAcquire(SInvalWriteLock, LW_SHARED);

    if (backendID > 0 && backendID <= segP->lastreserveBackend) {
        ProcState* stateP = &segP->procState[backendID - 1];

        result = stateP->proc;
    }

    LWLockRelease(SInvalWriteLock);

    return result;
}

/*
 * SIInsertDataEntries
 *		Add new invalidation message(s) to the buffer.
 */
void SIInsertDataEntries(const SharedInvalidationMessage* data, int n, XLogRecPtr lsn)
{
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;

    /*
     * N can be arbitrarily large.	We divide the work into groups of no more
     * than WRITE_QUANTUM messages, to be sure that we don't hold the lock for
     * an unreasonably long time.  (This is not so much because we care about
     * letting in other writers, as that some just-caught-up backend might be
     * trying to do SICleanupQueue to pass on its signal, and we don't want it
     * to have to wait a long time.)  Also, we need to consider calling
     * SICleanupQueue every so often.
     */
    while (n > 0) {
        int nthistime = Min(n, WRITE_QUANTUM);
        int numMsgs;
        int max;
        int i;

        n -= nthistime;

        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

        /*
         * If the buffer is full, we *must* acquire some space.  Clean the
         * queue and reset anyone who is preventing space from being freed.
         * Otherwise, clean the queue only when it's exceeded the next
         * fullness threshold.	We have to loop and recheck the buffer state
         * after any call of SICleanupQueue.
         */
        for (;;) {
            numMsgs = segP->maxMsgNum - segP->minMsgNum;

            if (numMsgs + nthistime > MAXNUMMESSAGES || numMsgs >= segP->nextThreshold) {
                SICleanupQueue(true, nthistime);
            } else {
                break;
            }
        }

        /*
         * Insert new message(s) into proper slot of circular buffer
         */
        max = segP->maxMsgNum;

        while (nthistime-- > 0) {
            int index = max % MAXNUMMESSAGES;
            segP->buffer[index].msg = *data++;
            segP->buffer[index].lsn = lsn;
            max++;
        }

        /* Update current value of maxMsgNum using spinlock */
        {
            /* use volatile pointer to prevent code rearrangement */
            volatile SISeg* vsegP = segP;

            SpinLockAcquire(&vsegP->msgnumLock);
            vsegP->maxMsgNum = max;
            SpinLockRelease(&vsegP->msgnumLock);
        }

        /*
         * Now that the maxMsgNum change is globally visible, we give everyone
         * a swift kick to make sure they read the newly added messages.
         * Releasing SInvalWriteLock will enforce a full memory barrier, so
         * these (unlocked) changes will be committed to memory before we exit
         * the function.
         */
        for (i = 0; i < segP->lastBackend; i++) {
            ProcState* stateP = &segP->procState[i];

            if (stateP->procPid != 0) {
                stateP->hasMessages = true;
            }
        }

        LWLockRelease(SInvalWriteLock);
    }
}

/*
 * SIGetDataEntries
 *		get next SI message(s) for current backend, if there are any
 *
 * Possible return values:
 *	0:	 no SI message available
 *	n>0: next n SI messages have been extracted into data[]
 * -1:	 SI reset message extracted
 *
 * If the return value is less than the array size "datasize", the caller
 * can assume that there are no more SI messages after the one(s) returned.
 * Otherwise, another call is needed to collect more messages.
 *
 * NB: this can run in parallel with other instances of SIGetDataEntries
 * executing on behalf of other backends, since each instance will modify only
 * fields of its own backend's ProcState, and no instance will look at fields
 * of other backends' ProcStates.  We express this by grabbing SInvalReadLock
 * in shared mode.	Note that this is not exactly the normal (read-only)
 * interpretation of a shared lock! Look closely at the interactions before
 * allowing SInvalReadLock to be grabbed in shared mode for any other reason!
 *
 * NB: this can also run in parallel with SIInsertDataEntries.	It is not
 * guaranteed that we will return any messages added after the routine is
 * entered.
 *
 * Note: we assume that "datasize" is not so large that it might be important
 * to break our hold on SInvalReadLock into segments.
 */
int SIGetDataEntries(SharedInvalidationMessage* data, int datasize, bool worksession)
{
    SISeg* segP = NULL;
    ProcState* stateP = NULL;
    int max;
    int n;

    segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;
    /* fetch inval msg state info for backend or session */
    if (IS_THREAD_POOL_WORKER) {
        if (EnableLocalSysCache() && !worksession) {
            /* GSC is on, and this is a thread pool worker, fetch inval msg slot by backendid */
            stateP = &segP->procState[t_thrd.proc_cxt.MyBackendId - 1];
        } else {
            /* this is a thread pool worker, fetch inval msg slot by session index */
            stateP = &segP->procState[u_sess->session_ctr_index];
        }
    } else {
        /* if not thread pool worker(either threadpool mode or not),  proc slot is located by current backendId */
        stateP = &segP->procState[t_thrd.proc_cxt.MyBackendId - 1];
    }

    /*
     * Before starting to take locks, do a quick, unlocked test to see whether
     * there can possibly be anything to read.	On a multiprocessor system,
     * it's possible that this load could migrate backwards and occur before
     * we actually enter this function, so we might miss a sinval message that
     * was just added by some other processor.	But they can't migrate
     * backwards over a preceding lock acquisition, so it should be OK.  If we
     * haven't acquired a lock preventing against further relevant
     * invalidations, any such occurrence is not much different than if the
     * invalidation had arrived slightly later in the first place.
     */
    if (!stateP->hasMessages) {
        return 0;
    }

    LWLockAcquire(SInvalReadLock, LW_SHARED);

    /*
     * We must reset hasMessages before determining how many messages we're
     * going to read.  That way, if new messages arrive after we have
     * determined how many we're reading, the flag will get reset and we'll
     * notice those messages part-way through.
     *
     * Note that, if we don't end up reading all of the messages, we had
     * better be certain to reset this flag before exiting!
     */
    stateP->hasMessages = false;

    /* Fetch current value of maxMsgNum using spinlock */
    {
        /* use volatile pointer to prevent code rearrangement */
        volatile SISeg* vsegP = segP;

        SpinLockAcquire(&vsegP->msgnumLock);
        max = vsegP->maxMsgNum;
        SpinLockRelease(&vsegP->msgnumLock);
    }

    if (stateP->resetState) {
        /*
         * Force reset.  We can say we have dealt with any messages added
         * since the reset, as well; and that means we should clear the
         * signaled flag, too.
         */
        stateP->nextMsgNum = max;
        stateP->resetState = false;
        stateP->signaled = false;
        LWLockRelease(SInvalReadLock);
        return -1;
    }

    /*
     * Retrieve messages and advance backend's counter, until data array is
     * full or there are no more messages.
     *
     * There may be other backends that haven't read the message(s), so we
     * cannot delete them here.  SICleanupQueue() will eventually remove them
     * from the queue.
     */
    n = 0;
 
    XLogRecPtr read_lsn = InvalidXLogRecPtr;
    if (IS_EXRTO_STANDBY_READ) {
        if (u_sess->utils_cxt.CurrentSnapshot != NULL &&
            XLogRecPtrIsValid(u_sess->utils_cxt.CurrentSnapshot->read_lsn)) {
            read_lsn = u_sess->utils_cxt.CurrentSnapshot->read_lsn;
        } else if (XLogRecPtrIsValid(t_thrd.proc->exrto_read_lsn)) {
            read_lsn = t_thrd.proc->exrto_read_lsn;
        }
    }

    while (n < datasize && stateP->nextMsgNum < max) {
        int index = stateP->nextMsgNum % MAXNUMMESSAGES;
        if (XLogRecPtrIsValid(read_lsn) && XLogRecPtrIsValid(segP->buffer[index].lsn)) {
            if (XLByteLT(read_lsn, segP->buffer[index].lsn)) {
                break;
            }
        }
        data[n++] = segP->buffer[index].msg;
        stateP->nextMsgNum++;
    }

    /*
     * If we have caught up completely, reset our "signaled" flag so that
     * we'll get another signal if we fall behind again.
     *
     * If we haven't caught up completely, reset the hasMessages flag so that
     * we see the remaining messages next time.
     */
    if (stateP->nextMsgNum >= max) {
        stateP->signaled = false;
    } else {
        stateP->hasMessages = true;
    }

    LWLockRelease(SInvalReadLock);
    return n;
}

/*
 * SICleanupQueue
 *		Remove messages that have been consumed by all active backends
 *
 * callerHasWriteLock is TRUE if caller is holding SInvalWriteLock.
 * minFree is the minimum number of message slots to make free.
 *
 * Possible side effects of this routine include marking one or more
 * backends as "reset" in the array, and sending PROCSIG_CATCHUP_INTERRUPT
 * to some backend that seems to be getting too far behind.  We signal at
 * most one backend at a time, for reasons explained at the top of the file.
 *
 * Caution: because we transiently release write lock when we have to signal
 * some other backend, it is NOT guaranteed that there are still minFree
 * free message slots at exit.	Caller must recheck and perhaps retry.
 */
void SICleanupQueue(bool callerHasWriteLock, int minFree)
{
    SISeg* segP = t_thrd.shemem_ptr_cxt.shmInvalBuffer;
    int min, minsig, lowbound, numMsgs, i;
    ProcState* needSig = NULL;

    /* Lock out all writers and readers */
    if (!callerHasWriteLock) {
        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
    }

    LWLockAcquire(SInvalReadLock, LW_EXCLUSIVE);

    /*
     * Recompute minMsgNum = minimum of all backends' nextMsgNum, identify the
     * furthest-back backend that needs signaling (if any), and reset any
     * backends that are too far back.	Note that because we ignore sendOnly
     * backends here it is possible for them to keep sending messages without
     * a problem even when they are the only active backend.
     */
    min = segP->maxMsgNum;
    minsig = min - SIG_THRESHOLD;
    lowbound = min - MAXNUMMESSAGES + minFree;

    for (i = 0; i < segP->lastBackend; i++) {
        ProcState* stateP = &segP->procState[i];
        int n = stateP->nextMsgNum;

        /* Ignore if inactive or already in reset state */
        if (stateP->procPid == 0 || stateP->resetState || stateP->sendOnly) {
            continue;
        }

        /*
         * If we must free some space and this backend is preventing it, force
         * him into reset state and then ignore until he catches up.
         */
        if (n < lowbound) {
            stateP->resetState = true;
            /* no point in signaling him ... */
            continue;
        }

        /* Track the global minimum nextMsgNum */
        if (n < min) {
            min = n;
        }

        /* Also see who's furthest back of the unsignaled backends */
        if ((i < segP->maxreserveBackends) && (n < minsig) && (!stateP->signaled)) {
            minsig = n;
            needSig = stateP;
        }
    }

    segP->minMsgNum = min;

    /*
     * When minMsgNum gets really large, decrement all message counters so as
     * to forestall overflow of the counters.  This happens seldom enough that
     * folding it into the previous loop would be a loser.
     */
    if (min >= MSGNUMWRAPAROUND) {
        segP->minMsgNum -= MSGNUMWRAPAROUND;
        segP->maxMsgNum -= MSGNUMWRAPAROUND;

        for (i = 0; i < segP->lastBackend; i++) {
            /* we don't bother skipping inactive entries here */
            if (segP->procState[i].procPid != 0)
                segP->procState[i].nextMsgNum -= MSGNUMWRAPAROUND;
        }
    }

    /*
     * Determine how many messages are still in the queue, and set the
     * threshold at which we should repeat SICleanupQueue().
     */
    numMsgs = segP->maxMsgNum - segP->minMsgNum;

    if (numMsgs < CLEANUP_MIN) {
        segP->nextThreshold = CLEANUP_MIN;
    } else {
        segP->nextThreshold = (numMsgs / CLEANUP_QUANTUM + 1) * CLEANUP_QUANTUM;
    }

    /*
     * Lastly, signal anyone who needs a catchup interrupt.  Since
     * SendProcSignal() might not be fast, we don't want to hold locks while
     * executing it.
     */
    if (needSig != NULL) {
        ThreadId his_pid = needSig->procPid;
        BackendId his_backendId = (needSig - &segP->procState[0]) + 1;

        needSig->signaled = true;
        LWLockRelease(SInvalReadLock);
        LWLockRelease(SInvalWriteLock);
        ereport(DEBUG4, (errmsg("sending sinval catchup signal to ThreadId %lu", his_pid)));
        SendProcSignal(his_pid, PROCSIG_CATCHUP_INTERRUPT, his_backendId);

        if (callerHasWriteLock) {
            LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);
        }
    } else {
        LWLockRelease(SInvalReadLock);

        if (!callerHasWriteLock) {
            LWLockRelease(SInvalWriteLock);
        }
    }
}

/*
 * GetNextLocalTransactionId --- allocate a new LocalTransactionId
 *
 * We split VirtualTransactionIds into two parts so that it is possible
 * to allocate a new one without any contention for shared memory, except
 * for a bit of additional overhead during backend startup/shutdown.
 * The high-order part of a VirtualTransactionId is a BackendId, and the
 * low-order part is a LocalTransactionId, which we assign from a local
 * counter.  To avoid the risk of a VirtualTransactionId being reused
 * within a short interval, successive procs occupying the same backend ID
 * slot should use a consecutive sequence of local IDs, which is implemented
 * by copying nextLocalTransactionId as seen above.
 */
LocalTransactionId GetNextLocalTransactionId(void)
{
    LocalTransactionId result;

    /* loop to avoid returning InvalidLocalTransactionId */
    do {
        result = u_sess->storage_cxt.nextLocalTransactionId++;
    } while (!LocalTransactionIdIsValid(result));

    return result;
}

