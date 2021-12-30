/* -------------------------------------------------------------------------
 *
 * pmsignal.cpp
 *	  routines for signaling the postmaster from its child processes
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/pmsignal.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "replication/datasender.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "gssignal/gs_signal.h"

/*
 * The postmaster is signaled by its children by sending SIGUSR1.  The
 * specific reason is communicated via flags in shared memory.	We keep
 * a boolean flag for each possible "reason", so that different reasons
 * can be signaled by different backends at the same time.	(However,
 * if the same reason is signaled more than once simultaneously, the
 * postmaster will observe it only once.)
 *
 * The flags are actually declared as "volatile sig_atomic_t" for maximum
 * portability.  This should ensure that loads and stores of the flag
 * values are atomic, allowing us to dispense with any explicit locking.
 *
 * In addition to the per-reason flags, we store a set of per-child-process
 * flags that are currently used only for detecting whether a backend has
 * exited without performing proper shutdown.  The per-child-process flags
 * have three possible states: UNUSED, ASSIGNED, ACTIVE.  An UNUSED slot is
 * available for assignment.  An ASSIGNED slot is associated with a postmaster
 * child process, but either the process has not touched shared memory yet,
 * or it has successfully cleaned up after itself.	A ACTIVE slot means the
 * process is actively using shared memory.  The slots are assigned to
 * child processes at random, and postmaster.c is responsible for tracking
 * which one goes with which PID.
 *
 * Actually there is a fourth state, WALSENDER.  This is just like ACTIVE,
 * but carries the extra information that the child is a WAL sender.
 * WAL senders too start in ACTIVE state, but switch to WALSENDER once they
 * start streaming the WAL (and they never go back to ACTIVE after that).
 */
#define PM_CHILD_UNUSED 0 /* these values must fit in sig_atomic_t */
#define PM_CHILD_ASSIGNED 1
#define PM_CHILD_ACTIVE 2
#define PM_CHILD_WALSENDER 3
#define PM_CHILD_DATASENDER 4

#define PM_CHILD_SUSPECT 5     /* the status is used for avoiding the tcp stress attack. */
#define PM_CHILD_TEMPBACKEND 6 /* the status is used for temp thread processing stream connection. */

/* "typedef struct PMSignalData PMSignalData" appears in pmsignal.h */
struct PMSignalData {
    /* per-reason flags */
    sig_atomic_t PMSignalFlags[NUM_PMSIGNALS];
    /* per-child-process flags */
    int num_child_flags;          /* # of entries in PMChildFlags[] */
    int next_child_flag;          /* next slot to try to assign */
    sig_atomic_t PMChildFlags[1]; /* VARIABLE LENGTH ARRAY */
};

/*
 * PMSignalShmemSize
 *		Compute space needed for pmsignal.c's shared memory
 */
Size PMSignalShmemSize(void)
{
    Size size;

    size = offsetof(PMSignalData, PMChildFlags);
    size = add_size(size, mul_size((Size)MaxLivePostmasterChildren(), sizeof(sig_atomic_t)));

    return size;
}

/*
 * PMSignalShmemInit - initialize during shared-memory creation
 */
void PMSignalShmemInit(void)
{
    bool found = false;

    t_thrd.shemem_ptr_cxt.PMSignalState = (PMSignalData*)ShmemInitStruct("PMSignalState", PMSignalShmemSize(), &found);

    if (!found) {
        errno_t rc = memset_s((void*)t_thrd.shemem_ptr_cxt.PMSignalState, PMSignalShmemSize(), 0, PMSignalShmemSize());
        securec_check(rc, "\0", "\0");
        t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags = MaxLivePostmasterChildren();
    }
}

/*
 * SendPostmasterSignal - signal the postmaster from a child process
 */
void SendPostmasterSignal(PMSignalReason reason)
{
    /* If called in a standalone backend, do nothing */
    if (!IsUnderPostmaster)
        return;

    /* Atomically set the proper flag */
    t_thrd.shemem_ptr_cxt.PMSignalState->PMSignalFlags[reason] = true;
    gs_signal_send(PostmasterPid, SIGUSR1);
}

/*
 * CheckPostmasterSignal - check to see if a particular reason has been
 * signaled, and clear the signal flag.  Should be called by postmaster
 * after receiving SIGUSR1.
 */
bool CheckPostmasterSignal(PMSignalReason reason)
{
    /* Careful here --- don't clear flag if we haven't seen it set */
    if (t_thrd.shemem_ptr_cxt.PMSignalState->PMSignalFlags[reason]) {
        t_thrd.shemem_ptr_cxt.PMSignalState->PMSignalFlags[reason] = false;
        return true;
    }

    return false;
}

/*
 * AssignPostmasterChildSlot - select an unused slot for a new postmaster
 * child process, and set its state to ASSIGNED.  Returns a slot number
 * (one to N).
 *
 * Only the postmaster is allowed to execute this routine, so we need no
 * special locking.
 */
int AssignPostmasterChildSlot(void)
{
    int slot = t_thrd.shemem_ptr_cxt.PMSignalState->next_child_flag;
    int n;

    /*
     * Scan for a free slot.  We track the last slot assigned so as not to
     * waste time repeatedly rescanning low-numbered slots.
     */
    for (n = t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags; n > 0; n--) {
        volatile sig_atomic_t* pTarget = NULL;

        if (--slot < 0)
            slot = t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags - 1;
        pTarget = &t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot];

        if (*pTarget == PM_CHILD_UNUSED) {
            sig_atomic_t value;

            value = __sync_val_compare_and_swap(pTarget, PM_CHILD_UNUSED, PM_CHILD_ASSIGNED);
            if (value == PM_CHILD_UNUSED) {
                /*
                 * We get an unused slot and mark it used. Otherwise, we will
                 * continue to try next possible slot. Next child flag is a hint
                 * which we try to maintain but ok to keep it loose.
                 */
                t_thrd.shemem_ptr_cxt.PMSignalState->next_child_flag = slot;
                return slot + 1;
            }
        }
    }

    /* Out of slots ... should never happen, else postmaster.c messed up */
    ereport(WARNING, (errmsg("no free slots in PMChildFlags array")));
    return -1;
}

/*
 * ReleasePostmasterChildSlot - release a slot after death of a postmaster
 * child process.  This must be called in the postmaster process.
 *
 * Returns true if the slot had been in ASSIGNED state (the expected case),
 * false otherwise (implying that the child failed to clean itself up).
 */
bool ReleasePostmasterChildSlot(int slot)
{
    bool result = false;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    /*
     * Note: the slot state might already be unused, because the logic in
     * postmaster.c is such that this might get called twice when a child
     * crashes.  So we don't try to Assert anything about the state.
     */
    result = (t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED ||
              t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_SUSPECT ||
              t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_TEMPBACKEND);

    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_UNUSED;
    return result;
}

/*
 * IsPostmasterChildWalSender - check if given slot is in use by a
 * walsender process.
 */
bool IsPostmasterChildWalSender(int slot)
{
    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    if (t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER)
        return true;
    else
        return false;
}

/*
 * IsPostmasterChildDataSender - check if given slot is in use by a
 * datasender process.
 */
bool IsPostmasterChildDataSender(int slot)
{
    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    if (t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_DATASENDER)
        return true;
    else
        return false;
}

/*
 * IsPostmasterChildSuspect - check if given slot is pending in ProcessStartupPacket.
 */
bool IsPostmasterChildSuspect(int slot)
{
    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    if (t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_SUSPECT)
        return true;
    else
        return false;
}

/*
 * @Description: check if given slot is in use by a temp thread processing
 *               cancel signal or stream connection.
 *
 * @param[IN] slot:  index of postmaster child slot
 * @return: bool, true if it is a temp thread.
 */
bool IsPostmasterChildTempBackend(int slot)
{
    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    if (t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_TEMPBACKEND)
        return true;
    else
        return false;
}

/*
 * MarkPostmasterChildActive - mark a postmaster child as about to begin
 * actively using shared memory.  This is called in the child process.
 */
void MarkPostmasterChildActive(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_ACTIVE;
}

/*
 * MarkPostmasterChildWalSender - mark a postmaster child as a WAL sender
 * process.  This is called in the child process, sometime after marking the
 * child as active.
 */
void MarkPostmasterChildWalSender(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(AM_WAL_SENDER);

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_WALSENDER;
}

/*
 * MarkPostmasterChildWalSender - mark a postmaster child as a WAL sender
 * process.  This is called in the child process, sometime after marking the
 * child as active.
 */
void MarkPostmasterChildDataSender(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(t_thrd.datasender_cxt.am_datasender);

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_DATASENDER;
}

void MarkPostmasterChildNormal(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(AM_WAL_SENDER);

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_DATASENDER);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_ACTIVE;
}

/*
 * MarkPostmasterChildInactive - mark a postmaster child as done using
 * shared memory.  This is called in the child process.
 */
void MarkPostmasterChildInactive(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;

    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_DATASENDER ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_SUSPECT ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_TEMPBACKEND);

    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_ASSIGNED;
}

/*
 * Mark stream worker process slot as unused - mark a postmaster child as done using
 * shared memory.  This is called in the child process.
 */
void MarkPostmasterChildUnuseForStreamWorker(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ACTIVE ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_WALSENDER ||
           t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_DATASENDER);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_UNUSED;
}

/*
 * MarkPostmasterChildSusPect - mark a postmaster child as suspect that maybe receive no data.
 * This is called in the child process. The function should be called in ProcessStartupPacket.
 */
void MarkPostmasterChildSusPect(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_SUSPECT;
}

/*
 * @Description: mark a postmaster child as temp thread processing cancel signal
 *               or stream connection. This is called in the child process.
 *               The function should be called in ProcessStartupPacket.
 * @return: void
 */
void MarkPostmasterTempBackend(void)
{
    int slot = t_thrd.proc_cxt.MyPMChildSlot;

    Assert(slot > 0 && slot <= t_thrd.shemem_ptr_cxt.PMSignalState->num_child_flags);
    slot--;
    Assert(t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] == PM_CHILD_ASSIGNED);
    t_thrd.shemem_ptr_cxt.PMSignalState->PMChildFlags[slot] = PM_CHILD_TEMPBACKEND;
}

/*
 * PostmasterIsAlive - check whether postmaster process is still alive
 *
 * amDirectChild should be passed as "true" by code that knows it is
 * executing in a direct child process of the postmaster; pass "false"
 * if an indirect child or not sure.  The "true" case uses a faster and
 * more reliable test, so use it when possible.
 */
bool PostmasterIsAlive(void)
{
    // Postmaster always live or the whole process is gone.
    //
    return true;
}

