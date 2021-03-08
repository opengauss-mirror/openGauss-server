/* -------------------------------------------------------------------------
 *
 * gtm_seq.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef GTM_SEQ_H
#define GTM_SEQ_H

#include "gtm/utils/stringinfo.h"
#include "gtm/gtm_lock.h"
#include "gtm/utils/libpq-be.h"
#include "gtm/gtm_list.h"
#include "gtm/gtm_msg.h"

typedef volatile int32 pg_atomic_int32;
typedef volatile uint32 pg_atomic_uint32;

/* Global sequence  related structures */
typedef struct GTM_SeqUUID {
    GTM_UUID uuid_nextId;
    GTM_UUID uuid_backUpId;
    GTM_RWLock uuid_seqLock;
} GTM_SeqUUID;

extern GTM_SeqUUID GTMSeqUUID;

typedef struct GTM_SeqInfo {
    GTM_SequenceKey gs_key;
    GTM_UUID gs_seqId;
    GTM_Sequence gs_value;
    GTM_Sequence gs_backedUpValue;
    GTM_Sequence gs_init_value;
    GTM_Sequence gs_last_value;
    GTM_Sequence gs_increment_by;
    GTM_Sequence gs_min_value;
    GTM_Sequence gs_max_value;
    GTM_Sequence gs_range; /* cache in client */

    /*
     * the blocked list for the same sequence, wait until gs_backedUpValue is synced
     * to standby/etcd successfully.
     * thread index will be remmebered, see also GTM_ThreadInfo::next_blocked_thread.
     */
    pg_atomic_uint32 gs_blocked_head;

    pg_atomic_int32 gs_ref_count;
    int32 gs_state;
    GTM_RWLock gs_lock;
    GTM_DBName gs_database;

    /* true if need to backup. reset false after backup finish. */
    bool gs_need_backup;
    bool gs_called_after_startup;
    bool gs_cycle;
    bool gs_called;
} GTM_SeqInfo;

typedef struct {
    GTM_UUID seq_uuid;
    GTM_Sequence increment;
    GTM_Sequence minval;
    GTM_Sequence maxval;
    GTM_Sequence startval;
    GTM_Sequence lastval;
    GTM_Sequence range;
    bool cycle;
    bool is_standby;
    bool is_restart;
} GTM_SeqMsgData;

typedef struct GTM_ThreadSeqInfo {
    GTM_SeqInfo* seqinfo;
    GTM_Sequence range;
    GTM_Sequence* rangemax;
    bool backup;
    GTM_Sequence seq_value;
} GTM_ThreadSeqInfo;

#define SEQ_STATE_ACTIVE 1
#define SEQ_STATE_DELETED 2

#define SEQ_IS_ASCENDING(s) ((s)->gs_increment_by > 0)
#define SEQ_IS_CYCLE(s) ((s)->gs_cycle)
#define SEQ_IS_CALLED(s) ((s)->gs_called)

#define SEQ_DEF_MAX_SEQVAL_ASCEND 0x7ffffffffffffffeLL
#define SEQ_DEF_MIN_SEQVAL_ASCEND 0x1

#define SEQ_DEF_MAX_SEQVAL_DESCEND -0x1
#define SEQ_DEF_MIN_SEQVAL_DESCEND -0x7ffffffffffffffeLL

#define MaxGlobalUUId ((GTM_UUID)0x7fffffffffffffffLL)

#define SEQ_MAX_REFCOUNT 1024

/* SEQUENCE Management */
void GTM_InitSeqManager(void);
int GTM_SeqOpen(GTM_UUID seq_uuid, GTM_Sequence increment_by, GTM_Sequence minval, GTM_Sequence maxval,
    GTM_Sequence startval, bool cycle, GTM_DBName seq_dbName, bool is_backup);
int GTM_SeqAlter(GTM_SeqMsgData* msg);
int GTM_SeqClose(GTM_UUID seq_uuid, GTM_DBName seq_dbName);
int GTM_SeqDBRename(GTM_DBName dbName, GTM_DBName newdbName);
GTM_Sequence GTM_SeqGetNext(GTM_SeqMsgData* msg, GTM_Sequence* rangemax);
int GTM_SeqSetVal(GTM_UUID seq_uuid, GTM_Sequence nextval, GTM_Sequence range, bool iscalled);
int GTM_SeqReset(GTM_UUID seq_uuid);
void GTM_SyncSequence(GTM_MessageType mtype, GTM_UUID seq_uuid, GTM_SeqInfo *thisSeq, GTM_DBName dbName = NULL,
                      GTM_DBName newdbName = NULL);
bool GTM_SyncSequenceToStandby(GTM_MessageType mtype, GTM_UUID seq_uuid, GTM_SeqInfo *thisSeq, GTM_DBName dbName,
                               GTM_DBName newdbName = NULL);

void GTM_SyncUUID(GTM_UUID seq_uuid);
void GTM_GetSyncUUIDFromEtcd();
bool GTM_GetUUIDFromEtcd(GTM_UUID& seq_uuid, bool force);
bool GTM_SyncUUIDToStandby(GTM_UUID seq_uuid);

void ProcessSequenceGetUUIDCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceInitCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceGetNextCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceSetValCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceResetCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceCloseCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceDBRenameCommand(Port* myport, StringInfo message, bool is_backup);
void ProcessSequenceAlterCommand(Port* myport, StringInfo message, bool is_backup);

void ProcessSequenceListCommand(Port* myport, StringInfo message);
void ProcessBkupGTMSequenceFileUUIDCommand(Port* myport, StringInfo message);
void ProcessGetNextSeqUUIDTransactionCommand(Port* myport, StringInfo message);

void GTM_SaveSeqInfo(FILE* ctlf);
void GTM_RestoreSeqUUIDInfo(FILE* ctlf);
int GTM_SeqRestore(GTM_SequenceKey seqkey, GTM_Sequence increment_by, GTM_Sequence minval, GTM_Sequence maxval,
    GTM_Sequence startval, GTM_Sequence curval, int32 state, bool cycle, bool called);
int GTM_SeqUUIDRestore(GTM_UUID seq_uuid, GTM_Sequence increment_by, GTM_Sequence minval, GTM_Sequence maxval,
                       GTM_Sequence startval, GTM_Sequence curval, GTM_Sequence range, int32 state, bool cycle,
                       bool called, GTM_DBName seq_dbName);

void GTM_RestoreUUIDInfo(FILE* seqf, GTM_UUID seq_uuid);
void SetNextGlobalUUID(GTM_UUID seq_uuid);
bool GlobalUUIDIsValid(GTM_UUID seq_uuid);
bool GTM_NeedUUIDRestoreUpdate(void);

bool GTM_NeedSeqRestoreUpdate(GTM_UUID seq_uuid);
void GTM_WriteRestorePointSeq(FILE* ctlf, GTM_UUID* seqId, bool all_seq_restore);

void RemoveGTMSeqs(void);

void GTM_WriteRestorePointUUID(FILE* file, bool uuid_restore);

void GTM_SaveUUIDInfo(FILE* file);
GTM_UUID ReadNewGlobalUUId();

bool GTM_SetSeqToEtcd(GTM_MessageType mtype, GTM_SeqInfo* thisSeq, bool isBackup, GTM_UUID seq_uuid);
extern void release_group_seq_lock();
void handleGetSeqException();

bool GTM_SetUUIDToEtcd(GTM_UUID seq_uuid, bool save);
bool GTMSyncUUIDToEtcdInternal(GTM_UUID seq_uuid, bool force);

#endif
