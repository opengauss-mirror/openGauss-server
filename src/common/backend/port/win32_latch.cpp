/* -------------------------------------------------------------------------
 *
 * win32_latch.c
 *    Routines for inter-process latches
 *
 * See unix_latch.c for header comments for the exported functions;
 * the API presented here is supposed to be the same as there.
 *
 * The Windows implementation uses Windows events that are inherited by
 * all postmaster child processes.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/common/backend/port/win32_latch.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <signal.h>

#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "storage/barrier.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"

void InitializeLatchSupport(void)
{
    /* currently, nothing to do here for Windows */
}

void InitLatch(volatile Latch* latch)
{
    latch->is_set = false;
    latch->owner_pid = t_thrd.proc_cxt.MyProcPid;
    latch->is_shared = false;

    latch->event = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (latch->event == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("CreateEvent failed: error code %d", (int)GetLastError())));
}

void InitSharedLatch(volatile Latch* latch)
{
    SECURITY_ATTRIBUTES sa;

    latch->is_set = false;
    latch->owner_pid = 0;
    latch->is_shared = true;

    /*
     * Set up security attributes to specify that the events are inherited.
     */
    ZeroMemory(&sa, sizeof(sa));
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;

    latch->event = CreateEvent(&sa, TRUE, FALSE, NULL);
    if (latch->event == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("CreateEvent failed: error code %d", (int)GetLastError())));
}

void OwnLatch(volatile Latch* latch)
{
    /* Sanity checks */
    Assert(latch->is_shared);
    if (latch->owner_pid != 0)
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("latch already owned")));

    latch->owner_pid = t_thrd.proc_cxt.MyProcPid;
}

void DisownLatch(volatile Latch* latch)
{
    Assert(latch->is_shared);
    Assert(latch->owner_pid == t_thrd.proc_cxt.MyProcPid);

    latch->owner_pid = 0;
}

int WaitLatch(volatile Latch* latch, int wakeEvents, long timeout)
{
    return WaitLatchOrSocket(latch, wakeEvents, PGINVALID_SOCKET, timeout);
}

int WaitLatchOrSocket(volatile Latch* latch, int wakeEvents, pgsocket sock, long timeout)
{
    DWORD rc;
    instr_time start_time, cur_time;
    long cur_timeout;
    HANDLE events[4];
    HANDLE latchevent;
    HANDLE sockevent = WSA_INVALID_EVENT;
    int numevents;
    int result = 0;
    int pmdeath_eventno = 0;

    /* Ignore WL_SOCKET_* events if no valid socket is given */
    if (sock == PGINVALID_SOCKET)
        wakeEvents &= ~(WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);

    Assert(wakeEvents != 0); /* must have at least one wake event */
    /* Cannot specify WL_SOCKET_WRITEABLE without WL_SOCKET_READABLE */
    Assert((wakeEvents & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) != WL_SOCKET_WRITEABLE);

    if ((wakeEvents & WL_LATCH_SET) && latch->owner_pid != t_thrd.proc_cxt.MyProcPid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("cannot wait on a latch owned by another process")));

    /*
     * Initialize timeout if requested.  We must record the current time so
     * that we can determine the remaining timeout if WaitForMultipleObjects
     * is interrupted.
     */
    if (wakeEvents & WL_TIMEOUT) {
        INSTR_TIME_SET_CURRENT(start_time);
        Assert(timeout >= 0);
        cur_timeout = timeout;
    } else
        cur_timeout = INFINITE;

    /*
     * Construct an array of event handles for WaitforMultipleObjects().
     *
     * Note: pgwin32_signal_event should be first to ensure that it will be
     * reported when multiple events are set.  We want to guarantee that
     * pending signals are serviced.
     */
    latchevent = latch->event;

    events[0] = pgwin32_signal_event;
    events[1] = latchevent;
    numevents = 2;
    if (wakeEvents & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) {
        /* Need an event object to represent events on the socket */
        int flags = 0;

        if (wakeEvents & WL_SOCKET_READABLE)
            flags |= (FD_READ | FD_CLOSE);
        if (wakeEvents & WL_SOCKET_WRITEABLE)
            flags |= FD_WRITE;

        sockevent = WSACreateEvent();
        if (sockevent == WSA_INVALID_EVENT)
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("failed to create event for socket: error code %u", WSAGetLastError())));
        if (WSAEventSelect(sock, sockevent, flags) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("failed to set up event for socket: error code %u", WSAGetLastError())));

        events[numevents++] = sockevent;
    }
    if (wakeEvents & WL_POSTMASTER_DEATH) {
        pmdeath_eventno = numevents;
        events[numevents++] = PostmasterHandle;
    }

    /* Ensure that signals are serviced even if latch is already set */
    pgwin32_dispatch_queued_signals();

    do {
        /*
         * Reset the event, and check if the latch is set already. If someone
         * sets the latch between this and the WaitForMultipleObjects() call
         * below, the setter will set the event and WaitForMultipleObjects()
         * will return immediately.
         */
        if (!ResetEvent(latchevent))
            ereport(
                ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("ResetEvent failed: error code %lu", GetLastError())));

        if ((wakeEvents & WL_LATCH_SET) && latch->is_set) {
            result |= WL_LATCH_SET;

            /*
             * Leave loop immediately, avoid blocking again. We don't attempt
             * to report any other events that might also be satisfied.
             */
            break;
        }

        rc = WaitForMultipleObjects(numevents, events, FALSE, cur_timeout);
        if (rc == WAIT_FAILED)
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("WaitForMultipleObjects() failed: error code %lu", GetLastError())));
        else if (rc == WAIT_TIMEOUT) {
            result |= WL_TIMEOUT;
        } else if (rc == WAIT_OBJECT_0) {
            /* Service newly-arrived signals */
            pgwin32_dispatch_queued_signals();
        } else if (rc == WAIT_OBJECT_0 + 1) {
            /*
             * Latch is set.  We'll handle that on next iteration of loop, but
             * let's not waste the cycles to update cur_timeout below.
             */
            continue;
        } else if ((wakeEvents & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) &&
                   rc == WAIT_OBJECT_0 + 2) {/* socket is at event slot 2 */
            WSANETWORKEVENTS resEvents;

            ZeroMemory(&resEvents, sizeof(resEvents));
            if (WSAEnumNetworkEvents(sock, sockevent, &resEvents) != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("failed to enumerate network events: error code %u", WSAGetLastError())));
            if ((wakeEvents & WL_SOCKET_READABLE) && (resEvents.lNetworkEvents & (FD_READ | FD_CLOSE))) {
                result |= WL_SOCKET_READABLE;
            }
            if ((wakeEvents & WL_SOCKET_WRITEABLE) && (resEvents.lNetworkEvents & FD_WRITE)) {
                result |= WL_SOCKET_WRITEABLE;
            }
        } else if ((wakeEvents & WL_POSTMASTER_DEATH) && rc == WAIT_OBJECT_0 + pmdeath_eventno) {
            /*
             * Postmaster apparently died.	Since the consequences of falsely
             * returning WL_POSTMASTER_DEATH could be pretty unpleasant, we
             * take the trouble to positively verify this with
             * PostmasterIsAlive(), even though there is no known reason to
             * think that the event could be falsely set on Windows.
             */
            if (!PostmasterIsAlive())
                result |= WL_POSTMASTER_DEATH;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("unexpected return code from WaitForMultipleObjects(): %lu", rc)));

        /* If we're not done, update cur_timeout for next iteration */
        if (result == 0 && cur_timeout != INFINITE) {
            INSTR_TIME_SET_CURRENT(cur_time);
            INSTR_TIME_SUBTRACT(cur_time, start_time);
            cur_timeout = timeout - (long)INSTR_TIME_GET_MILLISEC(cur_time);
            if (cur_timeout < 0) {
                cur_timeout = 0;
            }
        }
    } while (result == 0);

    /* Clean up the event object we created for the socket */
    if (sockevent != WSA_INVALID_EVENT) {
        WSAEventSelect(sock, NULL, 0);
        WSACloseEvent(sockevent);
    }

    return result;
}

void SetLatch(volatile Latch* latch)
{
    HANDLE handle;

    /*
     * The memory barrier has be to be placed here to ensure that any flag
     * variables possibly changed by this process have been flushed to main
     * memory, before we check/set is_set.
     */
    pg_memory_barrier();

    /* Quick exit if already set */
    if (latch->is_set)
        return;

    latch->is_set = true;

    /*
     * See if anyone's waiting for the latch. It can be the current process if
     * we're in a signal handler.
     *
     * Use a local variable here just in case somebody changes the event field
     * concurrently (which really should not happen).
     */
    handle = latch->event;
    if (handle) {
        SetEvent(handle);

        /*
         * Note that we silently ignore any errors. We might be in a signal
         * handler or other critical path where it's not safe to call elog().
         */
    }
}

void ResetLatch(volatile Latch* latch)
{
    /* Only the owner should reset the latch */
    Assert(latch->owner_pid == t_thrd.proc_cxt.MyProcPid);

    latch->is_set = false;

    /*
     * Ensure that the write to is_set gets flushed to main memory before we
     * examine any flag variables.  Otherwise a concurrent SetLatch might
     * falsely conclude that it needn't signal us, even though we have missed
     * seeing some flag updates that SetLatch was supposed to inform us of.
     */
    pg_memory_barrier();
}
