/*
 *
 * startup.cpp
 *
 * The Startup process initialises the server and performs any recovery
 * actions that have been specified. Notice that there is no "main loop"
 * since the Startup process ends as soon as initialisation is complete.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/startup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/startup.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "storage/mot/mot_fdw.h"

#include "gssignal/gs_signal.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/extreme_rto/dispatcher.h"

/* Signal handlers */
static void startupproc_quickdie(SIGNAL_ARGS);
static void StartupProcSigUsr1Handler(SIGNAL_ARGS);
static void StartupProcSigHupHandler(SIGNAL_ARGS);
static void StartupProcSigusr2Handler(SIGNAL_ARGS);
static void SetStaticConnNum(void);

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
/*
 * startupproc_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void startupproc_quickdie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/* SIGUSR1: let latch facility handle the signal */
static void StartupProcSigUsr1Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

/* SIGUSR2: set flag to finish recovery */
/*
 * SIGUSR2 handler for the startup process
 * When the SIGUSR2 is receiverd, then check the reason of the SIGUSR2
 * and do the corresponding operations
 */
static void StartupProcSigusr2Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    if (dummyStandbyMode)
        return;

    if (CheckNotifySignal(NOTIFY_PRIMARY)) {
        t_thrd.startup_cxt.primary_triggered = true;
    } else if (CheckNotifySignal(NOTIFY_STANDBY)) {
        t_thrd.startup_cxt.standby_triggered = true;
    } else if (CheckNotifySignal(NOTIFY_FAILOVER)) {
        t_thrd.startup_cxt.failover_triggered = true;
        WakeupRecovery();
    } else if (CheckNotifySignal(NOTIFY_SWITCHOVER)) {
        t_thrd.startup_cxt.switchover_triggered = true;
        WakeupRecovery();
    }

    errno = save_errno;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void StartupProcSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.startup_cxt.got_SIGHUP = true;
    WakeupRecovery();

    errno = save_errno;
}

/* SIGTERM: set flag to abort redo and exit */
static void StartupProcShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    if (t_thrd.startup_cxt.in_restore_command)
        proc_exit(1);
    else
        t_thrd.startup_cxt.shutdown_requested = true;

    WakeupRecovery();

    errno = save_errno;
}

/* Handle SIGHUP and SIGTERM signals of startup process */
void HandleStartupProcInterrupts(void)
{
    /*
     * Check if we were requested to re-read config file.
     */
    if (t_thrd.startup_cxt.got_SIGHUP) {
        t_thrd.startup_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    /*
     * Check if we were requested to exit without finishing recovery.
     */
    if (t_thrd.startup_cxt.shutdown_requested && SmartShutdown != g_instance.status) {
        proc_exit(1);
    }

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (IsUnderPostmaster && !PostmasterIsAlive())
        gs_thread_exit(1);
}

static void StartupReleaseAllLocks(int code, Datum arg)
{
    Assert(t_thrd.proc != NULL);

    /* Do nothing if we're not in hot standby mode */
    if (t_thrd.xlog_cxt.standbyState == STANDBY_DISABLED)
        return;

    /* If waiting, get off wait queue (should only be needed after error) */
    LockErrorCleanup();
    /* Release standard locks, including session-level if aborting */
    LockReleaseAll(DEFAULT_LOCKMETHOD, true);

    /*
     * User locks are not released by transaction end, so be sure to release
     * them explicitly.
     */
    LockReleaseAll(USER_LOCKMETHOD, true);
}

/* ----------------------------------
 *	Startup Process main entry point
 * ----------------------------------
 */
void StartupProcessMain(void)
{
    knl_thread_set_name("StartupProcess");

    /*
     * Properly accept or ignore signals the postmaster might send us.
     *
     * Note: ideally we'd not enable handle_standby_sig_alarm unless actually
     * doing hot standby, but we don't know that yet.  Rely on it to not do
     * anything if it shouldn't.
     */
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, StartupProcSigHupHandler);    /* reload config file */
    (void)gspqsignal(SIGINT, SIG_IGN);                     /* ignore query cancel */
    (void)gspqsignal(SIGTERM, StartupProcShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, startupproc_quickdie);       /* hard crash time */

    if (g_instance.attr.attr_storage.EnableHotStandby)
        (void)gspqsignal(SIGALRM, handle_standby_sig_alarm); /* ignored unless

                                                      * InHotStandby */
    else
        (void)gspqsignal(SIGALRM, SIG_IGN);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, StartupProcSigUsr1Handler);
    (void)gspqsignal(SIGUSR2, StartupProcSigusr2Handler);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    SetStaticConnNum();

    if (dummyStandbyMode) {
        StartupDummyStandby();
    } else {
        on_shmem_exit(StartupReleaseAllLocks, 0);

        /*
         * Init MOT first
         */
        InitMOT();

        /*
         * Mot recovery is part of StartupXlog
         */
        StartupXLOG();
    }

    /*
     * Exit normally. Exit code 0 tells postmaster that we completed recovery
     * successfully.
     */
    proc_exit(0);
}

void PreRestoreCommand(void)
{
    /*
     * Set t_thrd.startup_cxt.in_restore_command to tell the signal handler that we should exit
     * right away on SIGTERM. We know that we're at a safe point to do that.
     * Check if we had already received the signal, so that we don't miss a
     * shutdown request received just before this.
     */
    t_thrd.startup_cxt.in_restore_command = true;

    if (t_thrd.startup_cxt.shutdown_requested) {
        proc_exit(1);
    }
}

void PostRestoreCommand(void)
{
    t_thrd.startup_cxt.in_restore_command = false;
}

bool IsFailoverTriggered(void)
{
    if (AmStartupProcess())
        return t_thrd.startup_cxt.failover_triggered;
	else
	{
	    /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
		if (tgigger == (uint32)extreme_rto::TRIGGER_FAILOVER)
	        return true;	
	}
	return false;
}


bool IsSwitchoverTriggered(void)
{
    if (AmStartupProcess())
        return t_thrd.startup_cxt.switchover_triggered;
    else
	{
	    /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
		if (tgigger == (uint32)extreme_rto::TRIGGER_SWITCHOVER)
	        return true;
	}
	return false;
}

bool IsPrimaryTriggered(void)
{
    if (AmStartupProcess())
        return t_thrd.startup_cxt.primary_triggered;
    else
	{
	    /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
		if (tgigger == (uint32)extreme_rto::TRIGGER_PRIMARY)
	        return true;
	}
	return false;
}

bool IsStandbyTriggered(void)
{
    if (AmStartupProcess())
        return t_thrd.startup_cxt.standby_triggered;
    else
	{
	    /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
		if (tgigger == (uint32)extreme_rto::TRIGGER_STADNBY)
	        return true;
	}
	return false;
}

void ResetFailoverTriggered(void)
{
    t_thrd.startup_cxt.failover_triggered = false;
}

void ResetSwitchoverTriggered(void)
{
    t_thrd.startup_cxt.switchover_triggered = false;
}

void ResetPrimaryTriggered(void)
{
    t_thrd.startup_cxt.primary_triggered = false;
}

void ResetStandbyTriggered(void)
{
    t_thrd.startup_cxt.standby_triggered = false;
}

Size NotifySignalShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(NotifySignalData));

    return size;
}

void NotifySignalShmemInit(void)
{
    bool found = false;
    errno_t rc = 0;

    t_thrd.startup_cxt.NotifySigState =
        (NotifySignalData*)ShmemInitStruct("NotifySignalState", NotifySignalShmemSize(), &found);

    if (!found) {
        rc = memset_s(t_thrd.startup_cxt.NotifySigState, NotifySignalShmemSize(), 0, NotifySignalShmemSize());
        securec_check(rc, "", "");
    }
}

/*
 * Set the reason in notify signal share memory, and send the SIGUSR2 to the process
 */
void SendNotifySignal(NotifyReason reason, ThreadId ProcPid)
{
    t_thrd.startup_cxt.NotifySigState->NotifySignalFlags[reason] = true;

    if (0 != gs_signal_send(ProcPid, SIGUSR2)) {
        ereport(WARNING, (errmsg("Send signal failed")));
    }
}

/*
 * Check the notify sinal share memory, and find the reason of the signal.
 */
bool CheckNotifySignal(NotifyReason reason)
{
    /* Careful here --- don't clear flag if we haven't seen it set */
    if (t_thrd.startup_cxt.NotifySigState->NotifySignalFlags[reason]) {
        t_thrd.startup_cxt.NotifySigState->NotifySignalFlags[reason] = false;
        return true;
    }
    return false;
}

/*
 * Updata the static connection numbers in HaSHmData
 */
static void SetStaticConnNum(void)
{
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int i = 0;
    int repl_list_num = 0;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        repl_list_num = (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL) ? (repl_list_num + 1) : repl_list_num;

        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->repl_list_num = repl_list_num;
        SpinLockRelease(&hashmdata->mutex);
    }
}
