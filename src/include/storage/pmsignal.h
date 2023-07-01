/* -------------------------------------------------------------------------
 *
 * pmsignal.h
 *	  routines for signaling the postmaster from its child processes
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pmsignal.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PMSIGNAL_H
#define PMSIGNAL_H

/*
 * Reasons for signaling the postmaster.  We can cope with simultaneous
 * signals for different reasons.  If the same reason is signaled multiple
 * times in quick succession, however, the postmaster is likely to observe
 * only one notification of it.  This is okay for the present uses.
 */
typedef enum {
    PMSIGNAL_RECOVERY_STARTED,         /* recovery has started */
    PMSIGNAL_BEGIN_HOT_STANDBY,        /* begin Hot Standby */
    PMSIGNAL_LOCAL_RECOVERY_DONE,      /* local recovery has done */
    PMSIGNAL_WAKEN_ARCHIVER,           /* send a NOTIFY signal to xlog archiver */
    PMSIGNAL_ROTATE_LOGFILE,           /* send SIGUSR1 to syslogger to rotate logfile */
    PMSIGNAL_START_AUTOVAC_LAUNCHER,   /* start an autovacuum launcher */
    PMSIGNAL_START_AUTOVAC_WORKER,     /* start an autovacuum worker */
    PMSIGNAL_START_CLEAN_STATEMENT,     /* start an clean statement worker */
    PMSIGNAL_START_CATCHUP,            /* start a data catchup worker */
    PMSIGNAL_START_WALRECEIVER,        /* start a walreceiver */
    PMSIGNAL_START_DATARECEIVER,       /* start a datareceiver */
    PMSIGNAL_ADVANCE_STATE_MACHINE,    /* advance postmaster's state machine */
    PMSIGNAL_DEMOTE_PRIMARY,           /* start to demote primary */
    PMSIGNAL_PROMOTE_STANDBY,          /* start to promote standby */
    PMSIGNAL_SWITCHOVER_TIMEOUT,       /* start to backtrace for switchover */
    PMSIGNAL_UPDATE_WAITING,           /* update waiting status to gaussdb.state */
    PMSIGNAL_UPDATE_PROMOTING,         /* update promoting status to gaussdb.state */
    PMSIGNAL_UPDATE_HAREBUILD_REASON,  /* update ha rebuild reason to gaussdb.state */
    PMSIGNAL_UPDATE_NORMAL,            /* update normal status to gaussdb.state */
    PMSIGNAL_START_JOB_SCHEDULER,      /* start a job scheduler */
    PMSIGNAL_START_JOB_WORKER,         /* start a job worker */
    PMSIGNAL_ROLLBACK_STANDBY_PROMOTE, /* roll back standby promoting */
    PMSIGNAL_START_PAGE_WRITER,        /* start a new page writer thread */
    PMSIGNAL_START_THREADPOOL_WORKER,  /* start thread pool woker */
	PMSIGNAL_START_UNDO_WORKER,        /* start a new undo worker */
    PMSIGNAL_START_RB_WORKER,          /* start a rbworker */
    PMSIGNAL_START_TXNSNAPWORKER,      /* start a snapcaputure worker */
    PMSIGNAL_START_LOGICAL_READ_WORKER,/* start logical read worker */
    PMSIGNAL_START_PARALLEL_DECODE_WORKER,/* start parallel decoding worker */
    PMSIGNAL_START_APPLY_WORKER,       /* start a apply worker */
    PMSIGNAL_DMS_FAILOVER_TERM_BACKENDS,  /* term backends in alive failover */
    PMSIGNAL_DMS_FAILOVER_STARTUP,     /* start startup thread in alive failover */
    PMSIGNAL_DMS_SWITCHOVER_PROMOTE,   /* dms standby switchover promote */
    PMSIGNAL_DMS_REFORM,               /* dms reform start during PM_RUN */
    PMSIGNAL_DMS_REFORM_DONE,          /* dms reform done */
    PMSIGNAL_DMS_TERM_STARTUP,         /* term startup thread*/
    NUM_PMSIGNALS                      /* Must be last value of enum! */
} PMSignalReason;

/* PMSignalData is an opaque struct, details known only within pmsignal.c */
typedef struct PMSignalData PMSignalData;

/*
 * prototypes for functions in pmsignal.c
 */
extern Size PMSignalShmemSize(void);
extern void PMSignalShmemInit(void);
extern void SendPostmasterSignal(PMSignalReason reason);
extern bool CheckPostmasterSignal(PMSignalReason reason);
extern int AssignPostmasterChildSlot(void);
extern bool ReleasePostmasterChildSlot(int slot);
extern bool IsPostmasterChildWalSender(int slot);
extern bool IsPostmasterChildDataSender(int slot);
extern void MarkPostmasterChildActive(void);
extern void MarkPostmasterChildInactive(void);
extern void MarkPostmasterChildWalSender(void);
extern void MarkPostmasterChildDataSender(void);
extern void MarkPostmasterChildNormal(void);
extern bool PostmasterIsAlive(void);
extern void MarkPostmasterChildUnuseForStreamWorker(void);

extern bool IsPostmasterChildSuspect(int slot);
extern void MarkPostmasterChildSusPect(void);
extern void MarkPostmasterTempBackend(void);
extern bool IsPostmasterChildTempBackend(int slot);
#endif /* PMSIGNAL_H */
