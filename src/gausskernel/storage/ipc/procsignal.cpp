/* -------------------------------------------------------------------------
 *
 * procsignal.cpp
 *	  Routines for interprocess signalling
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/procsignal.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "commands/async.h"
#include "distributelayer/streamCore.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/sinval.h"
#include "tcop/tcopprot.h"
#include "threadpool/threadpool.h"
#ifdef PGXC
    #include "pgxc/poolutils.h"
#endif
#include "gssignal/gs_signal.h"

#include "workload/workload.h"
#include "access/multi_redo_settings.h"


/*
 * The SIGUSR1 signal is multiplexed to support signalling multiple event
 * types. The specific reason is communicated via flags in shared memory.
 * We keep a boolean flag for each possible "reason", so that different
 * reasons can be signaled to a process concurrently.  (However, if the same
 * reason is signaled more than once nearly simultaneously, the process may
 * observe it only once.)
 *
 * Each process that wants to receive signals registers its process ID
 * in the ProcSignalSlots array. The array is indexed by backend ID to make
 * slot allocation simple, and to avoid having to search the array when you
 * know the backend ID of the process you're signalling.  (We do support
 * signalling without backend ID, but it's a bit less efficient.)
 *
 * The flags are actually declared as "volatile sig_atomic_t" for maximum
 * portability.  This should ensure that loads and stores of the flag
 * values are atomic, allowing us to dispense with any explicit locking.
 */
typedef struct ProcSignalSlot {
    ThreadId pss_pid;
    sig_atomic_t pss_signalFlags[NUM_PROCSIGNALS];
} ProcSignalSlot;

/*
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.)
 */
#define NumProcSignalSlots (g_instance.shmem_cxt.MaxBackends + NUM_AUXILIARY_PROCS)

static ProcSignalSlot* g_libcomm_proc_signal_slots = NULL;
bool CheckProcSignal(ProcSignalReason reason);
static void CleanupProcSignalState(int status, Datum arg);

/*
 * ProcSignalShmemSize
 *		Compute space needed for procsignal's shared memory
 */
Size ProcSignalShmemSize(void)
{
    return NumProcSignalSlots * sizeof(ProcSignalSlot);
}

/*
 * ProcSignalShmemInit
 *		Allocate and initialize procsignal's shared memory
 */
void ProcSignalShmemInit(void)
{
    Size size = ProcSignalShmemSize();
    bool found = false;

    t_thrd.shemem_ptr_cxt.ProcSignalSlots = (ProcSignalSlot*)ShmemInitStruct("ProcSignalSlots", size, &found);

    /* If we're first, set everything to zeroes */
    if (!found) {
        errno_t ret = memset_s(t_thrd.shemem_ptr_cxt.ProcSignalSlots, size, 0, size);
        securec_check(ret, "\0", "\0");
    }

    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid)
        g_libcomm_proc_signal_slots = t_thrd.shemem_ptr_cxt.ProcSignalSlots;
}

/*
 * ProcSignalInit
 *		Register the current process in the procsignal array
 *
 * The passed index should be my BackendId if the process has one,
 * or g_instance.shmem_cxt.MaxBackends + aux process type if not.
 */
void ProcSignalInit(int pss_idx)
{
    volatile ProcSignalSlot* slot = NULL;

    Assert(pss_idx >= 1 && pss_idx <= NumProcSignalSlots);

    slot = &t_thrd.shemem_ptr_cxt.ProcSignalSlots[pss_idx - 1];

    /* sanity check */
    if (slot->pss_pid != 0)
        ereport(LOG,
                (errmsg(
                     "process %lu taking over ProcSignal slot %d, but it's not empty", t_thrd.proc_cxt.MyProcPid, pss_idx)));

    /* Clear out any leftover signal reasons */
    errno_t rc = memset_s((void*)slot->pss_signalFlags,
                          NUM_PROCSIGNALS * sizeof(sig_atomic_t),
                          0,
                          NUM_PROCSIGNALS * sizeof(sig_atomic_t));
    securec_check(rc, "\0", "\0");

    /* Mark slot with my PID */
    slot->pss_pid = t_thrd.proc_cxt.MyProcPid;

    /* Remember slot location for CheckProcSignal */
    t_thrd.shemem_ptr_cxt.MyProcSignalSlot = slot;

    /* Set up to release the slot on process exit */
    on_shmem_exit(CleanupProcSignalState, Int32GetDatum(pss_idx));
}

/*
 * CleanupProcSignalState
 *		Remove current process from ProcSignalSlots
 *
 * This function is called via on_shmem_exit() during backend shutdown.
 */
static void CleanupProcSignalState(int status, Datum arg)
{
    int pss_idx = DatumGetInt32(arg);
    volatile ProcSignalSlot* slot = NULL;

    slot = &t_thrd.shemem_ptr_cxt.ProcSignalSlots[pss_idx - 1];
    Assert(slot == t_thrd.shemem_ptr_cxt.MyProcSignalSlot);

    /* sanity check */
    if (slot->pss_pid != t_thrd.proc_cxt.MyProcPid) {
        /*
         * don't ERROR here. We're exiting anyway, and don't want to get into
         * infinite loop trying to exit
         */
        ereport(LOG,
                (errmsg("thread %lu releasing ProcSignal slot %d, but it contains %lu",
                        t_thrd.proc_cxt.MyProcPid,
                        pss_idx,
                        slot->pss_pid)));
        return; /* XXX better to zero the slot anyway? */
    }

    slot->pss_pid = 0;
}

/*
 * SendProcSignal
 *		Send a signal to a Postgres process
 *
 * Providing backendId is optional, but it will speed up the operation.
 *
 * On success (a signal was sent), zero is returned.
 * On error, -1 is returned, and errno is set (typically to ESRCH or EPERM).
 *
 * Not to be confused with ProcSendSignal
 */
int SendProcSignal(ThreadId pid, ProcSignalReason reason, BackendId backendId)
{
    volatile ProcSignalSlot* slot = NULL;

    if (pid == 0)
        return -1;

    if (backendId != InvalidBackendId) {
        slot = &t_thrd.shemem_ptr_cxt.ProcSignalSlots[backendId - 1];

        /*
         * Note: Since there's no locking, it's possible that the target
         * process detaches from shared memory and exits right after this
         * test, before we set the flag and send signal. And the signal slot
         * might even be recycled by a new process, so it's remotely possible
         * that we set a flag for a wrong process. That's OK, all the signals
         * are such that no harm is done if they're mistakenly fired.
         */
        if (slot->pss_pid == pid) {
            /* Atomically set the proper flag */
            slot->pss_signalFlags[reason] = true;
            /* Send signal */
            return gs_signal_send(pid, SIGUSR1);
        }
    } else {
        /*
         * BackendId not provided, so search the array using pid.  We search
         * the array back to front so as to reduce search overhead.  Passing
         * InvalidBackendId means that the target is most likely an auxiliary
         * process, which will have a slot near the end of the array.
         */
        int i;

        for (i = NumProcSignalSlots - 1; i >= 0; i--) {
            slot = &t_thrd.shemem_ptr_cxt.ProcSignalSlots[i];

            if (slot->pss_pid == pid) {
                /* the above note about race conditions applies here too
                 * Atomically set the proper flag
                 */
                slot->pss_signalFlags[reason] = true;
                /* Send signal */
                return gs_signal_send(pid, SIGUSR1);
            }
        }
    }

    errno = ESRCH;
    return -1;
}

int SendProcSignalForLibcomm(ThreadId pid, ProcSignalReason reason, BackendId backendId)
{
    t_thrd.shemem_ptr_cxt.ProcSignalSlots = g_libcomm_proc_signal_slots;
    return SendProcSignal(pid, reason, backendId);
}

/*
 * CheckProcSignal - check to see if a particular reason has been
 * signaled, and clear the signal flag.  Should be called after receiving
 * SIGUSR1.
 */
bool CheckProcSignal(ProcSignalReason reason)
{
    volatile ProcSignalSlot* slot = t_thrd.shemem_ptr_cxt.MyProcSignalSlot;

    if (slot != NULL) {
        /* Careful here --- don't clear flag if we haven't seen it set */
        if (slot->pss_signalFlags[reason]) {
            slot->pss_signalFlags[reason] = false;
            return true;
        }
    }

    return false;
}

/*
 * procsignal_sigusr1_handler - handle SIGUSR1 signal.
 */
void procsignal_sigusr1_handler(SIGNAL_ARGS)
{
    if (t_thrd.int_cxt.ignoreBackendSignal) {
        return;
    }
    int save_errno = errno;
    t_thrd.int_cxt.InterruptByCN = true;

    if (CheckProcSignal(PROCSIG_CATCHUP_INTERRUPT))
        HandleCatchupInterrupt();

    if (CheckProcSignal(PROCSIG_NOTIFY_INTERRUPT))
        HandleNotifyInterrupt();

#ifdef PGXC

    if (CheckProcSignal(PROCSIG_PGXCPOOL_RELOAD))
        HandlePoolerReload();
    if (CheckProcSignal(PROCSIG_MEMORYCONTEXT_DUMP))
        HandleMemoryContextDump();
    if (CheckProcSignal(PROCSIG_EXECUTOR_FLAG))
        HandleExecutorFlag();
    if (CheckProcSignal(PROCSIG_UPDATE_WORKLOAD_DATA))
        WLMCheckSigRecvData();
    if (CheckProcSignal(PROCSIG_SPACE_LIMIT))
        WLMCheckSpaceLimit();
#if (!defined ENABLE_MULTIPLE_NODES) && (!defined USE_SPQ)
    if (CheckProcSignal(PROCSIG_STREAM_STOP_CHECK))
        StreamMarkStop();
#endif
#endif

    if (CheckProcSignal(PROCSIG_DEFAULTXACT_READONLY))
        WLMCheckDefaultXactReadOnly();
    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_DATABASE))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_DATABASE);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_TABLESPACE))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_TABLESPACE);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_LOCK))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_LOCK);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_SNAPSHOT);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);

    if (CheckProcSignal(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN))
        RecoveryConflictInterrupt(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);

    latch_sigusr1_handler();

    errno = save_errno;
}

