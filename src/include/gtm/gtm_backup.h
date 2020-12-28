/* -------------------------------------------------------------------------
 *
 * gtm_backup.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2013 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GTM_BACKUP_H
#define _GTM_BACKUP_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_seq.h"

extern GTM_RWLock gtm_bkup_lock;
extern GTM_RWLock seq_bkup_lock;
extern uint32 GTMWorkingGrandVersionNum;

#define SeqRestoreDuration 20000

extern void GTM_ReadRestorePoint(GlobalTransactionId next_gxid);
extern void GTM_ReadTimeLineAndTxactLen(GlobalTransactionId next_gxid);
extern void GTM_ReadRestoreSequence(void);
extern void GTM_WriteRestoreControlPoint(bool xid_restore);
extern void GTM_WriteRestoreSequencePoint(GTM_UUID* seq_id, bool all_seq_restore, bool uuid_restore);
extern void GTM_WriteRestorePoint(GTM_UUID* seq_id, bool xid_restore, bool all_seq_restore, bool uuid_restore);
extern void GTM_WriteControlFile(bool saveinfo, bool xid_restore);
extern void GTM_WriteSequenceFile(bool saveinfo, GTM_UUID* seq_uuid, bool all_seq_restore, bool uuid_restore);
extern void GTM_WriteXidToFile(bool saveinfo, bool xid_restore);
extern void GTM_MakeBackup(char* path);
extern void GTM_SetNeedBackup(void);
extern bool GTM_NeedBackup(void);
extern void GTM_SetXidAndTxnBackup(void);
extern void GTM_SetSequenceBackup(void);
extern bool GTM_SequenceBackup(void);
extern void GTM_WriteBarrierBackup(const char* barrier_id);

#endif /* GTM_BACKUP_H */
