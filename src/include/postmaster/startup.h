/* -------------------------------------------------------------------------
 *
 * startup.h
 *	  Exports from postmaster/startup.c.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/include/postmaster/startup.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _STARTUP_H
#define _STARTUP_H

typedef enum { NOTIFY_PRIMARY = 0, NOTIFY_STANDBY, NOTIFY_CASCADE_STANDBY, NOTIFY_FAILOVER,
               NOTIFY_SWITCHOVER, NUM_NOTIFYS } NotifyReason;

/*
 * Save the notify signal reason in the share memory.
 * NotifySignalFlags include primary signal, standby signal and promote signal.
 */
typedef struct notifysignaldata {
    sig_atomic_t NotifySignalFlags[NUM_NOTIFYS];
} NotifySignalData;

extern void StartupProcessMain(void);
extern void PreRestoreCommand(void);
extern void PostRestoreCommand(void);

extern bool IsFailoverTriggered(void);
extern bool IsSwitchoverTriggered(void);
extern bool IsPrimaryTriggered(void);
extern bool IsStandbyTriggered(void);
extern void ResetSwitchoverTriggered(void);
extern void ResetFailoverTriggered(void);
extern void ResetPrimaryTriggered(void);
extern void ResetStandbyTriggered(void);

extern Size NotifySignalShmemSize(void);
extern void NotifySignalShmemInit(void);
extern void SendNotifySignal(NotifyReason reason, ThreadId ProcPid);
extern bool CheckNotifySignal(NotifyReason reason);
#endif /* _STARTUP_H */
