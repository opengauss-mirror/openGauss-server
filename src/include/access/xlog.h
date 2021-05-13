/*
 * xlog.h
 *
 * PostgreSQL transaction log manager
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * src/include/access/xlog.h
 */
#ifndef XLOG_H
#define XLOG_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "access/parallel_recovery/redo_item.h"
#include "knl/knl_instance.h"
#include "access/htup.h"
#include "storage/lock/lwlock.h"

#include <time.h>

/* Sync methods */
#define SYNC_METHOD_FSYNC 0
#define SYNC_METHOD_FDATASYNC 1
#define SYNC_METHOD_OPEN 2 /* for O_SYNC */
#define SYNC_METHOD_FSYNC_WRITETHROUGH 3
#define SYNC_METHOD_OPEN_DSYNC 4 /* for O_DSYNC */

/*
 * Like InRecovery, standbyState is only valid in the startup process.
 * In all other processes it will have the value STANDBY_DISABLED (so
 * InHotStandby will read as FALSE).
 *
 * In DISABLED state, we're performing crash recovery or hot standby was
 * disabled in postgresql.conf.
 *
 * In INITIALIZED state, we've run InitRecoveryTransactionEnvironment, but
 * we haven't yet processed a RUNNING_XACTS or shutdown-checkpoint WAL record
 * to initialize our master-transaction tracking system.
 *
 * When the transaction tracking is initialized, we enter the SNAPSHOT_PENDING
 * state. The tracked information might still be incomplete, so we can't allow
 * connections yet, but redo functions must update the in-memory state when
 * appropriate.
 *
 * In SNAPSHOT_READY mode, we have full knowledge of transactions that are
 * (or were) running in the master at the current WAL location. Snapshots
 * can be taken, and read-only queries can be run.
 */
typedef enum {
    STANDBY_DISABLED,
    STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING,
    STANDBY_SNAPSHOT_READY
} HotStandbyState;

#define InHotStandby (t_thrd.xlog_cxt.standbyState >= STANDBY_SNAPSHOT_PENDING)

#define DUMMYSTANDBY_CONNECT_INTERVAL 3  // unit second
#define DUMMYSTANDBY_CHECK_INTERVAL 1    // unit second
#define SYNC_DUMMY_STANDBY_END 100

#define REP_CONN_ARRAY 2  // support 2 relp  connection

const static XLogRecPtr MAX_XLOG_REC_PTR = (XLogRecPtr)0xFFFFFFFFFFFFFFFF;
extern volatile uint64 sync_system_identifier;

/*
 * Codes indicating where we got a WAL file from during recovery, or where
 * to attempt to get one.  These are chosen so that they can be OR'd together
 * in a bitmask state variable.
 */
#define XLOG_FROM_ARCHIVE (1 << 0) /* Restored using restore_command */
#define XLOG_FROM_PG_XLOG (1 << 1) /* Existing file in pg_xlog */
#define XLOG_FROM_STREAM (1 << 2)  /* Streamed from master */

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when standby_mode = on
 */
typedef enum {
    RECOVERY_TARGET_UNSET,
    RECOVERY_TARGET_XID,
    RECOVERY_TARGET_TIME,
    RECOVERY_TARGET_NAME,
    RECOVERY_TARGET_LSN
#ifdef PGXC
    ,
    RECOVERY_TARGET_BARRIER
#endif
} RecoveryTargetType;

/* WAL levels */
typedef enum WalLevel {
    WAL_LEVEL_MINIMAL = 0,
    WAL_LEVEL_ARCHIVE,
    WAL_LEVEL_HOT_STANDBY,
    WAL_LEVEL_LOGICAL
} WalLevel;

#define XLogArchivingActive() \
    (u_sess->attr.attr_common.XLogArchiveMode && g_instance.attr.attr_storage.wal_level >= WAL_LEVEL_ARCHIVE)
#define XLogArchiveCommandSet() (u_sess->attr.attr_storage.XLogArchiveCommand[0] != '\0')
#define XLogArchiveDestSet() (u_sess->attr.attr_storage.XLogArchiveDest[0] != '\0')

/*
 * Is WAL-logging necessary for archival or log-shipping, or can we skip
 * WAL-logging if we fsync() the data before committing instead?
 */
#define XLogIsNeeded() (g_instance.attr.attr_storage.wal_level >= WAL_LEVEL_ARCHIVE)

/*
 * Is a full-page image needed for hint bit updates?
 *
 * Normally, we don't WAL-log hint bit updates, but if checksums are enabled,
 * we have to protect them against torn page writes.  When you only set
 * individual bits on a page, it's still consistent no matter what combination
 * of the bits make it to disk, but the checksum wouldn't match.  Also WAL-log
 * them if forced by wal_log_hints=on.
 */
#define XLogHintBitIsNeeded() (g_instance.attr.attr_storage.wal_log_hints)

/* Do we need to WAL-log information required only for Hot Standby and logical replication? */
#define XLogStandbyInfoActive() (g_instance.attr.attr_storage.wal_level >= WAL_LEVEL_HOT_STANDBY)
/* Do we need to WAL-log information required only for logical replication? */
#define XLogLogicalInfoActive() (g_instance.attr.attr_storage.wal_level >= WAL_LEVEL_LOGICAL)
extern const char* DemoteModeDescs[];
extern const int DemoteModeNum;

#define DemoteModeDesc(mode) (((mode) > 0 && (mode) < DemoteModeNum) ? DemoteModeDescs[(mode)] : DemoteModeDescs[0])

typedef struct {
    LWLock* lock;
    PGSemaphoreData	sem;
} WALFlushWaitLock;

typedef struct {
    LWLock* lock;
    PGSemaphoreData	sem;
} WALInitSegLock;

typedef struct {
    LWLock* lock;
    PGSemaphoreData	sem;
} WALBufferInitWaitLock;

#define WAL_INSERT_STATUS_ENTRIES 4194304
#define WAL_NOT_COPIED 0
#define WAL_COPIED 1
#define WAL_COPY_SUSPEND (-1)
#define WAL_SCANNED_LRC_INIT (-2)

/* (ientry + 1) % WAL_INSERT_STATUS_ENTRIES */
#define GET_NEXT_STATUS_ENTRY(ientry) ((ientry + 1) & (WAL_INSERT_STATUS_ENTRIES - 1))

#define GET_STATUS_ENTRY_INDEX(ientry) ientry

struct WalInsertStatusEntry {
    /* The end LSN of the record corresponding to this entry */
    uint64 endLSN;

    /* The log record counter of the record corresponding to this entry */
    int32  LRC;

    /* WAL copy status: "0" - not copied; "1" - copied */
    uint32 status;
};

struct WALFlushWaitLockPadded {
    WALFlushWaitLock l;
    char padding[PG_CACHE_LINE_SIZE];
};

struct WALBufferInitWaitLockPadded {
    WALBufferInitWaitLock l;
    char padding[PG_CACHE_LINE_SIZE];
};

struct WALInitSegLockPadded {
    WALInitSegLock l;
    char padding[PG_CACHE_LINE_SIZE];
};

/*
 * OR-able request flag bits for checkpoints.  The "cause" bits are used only
 * for logging purposes.  Note: the flags must be defined so that it's
 * sensible to OR together request flags arising from different requestors.
 *
 * Note: one exception is CHECKPOINT_FILE_SYNC, with which checkpointer
 * will only do file sync operation. Since normal checkpointer process also
 * includes file sync, this flag is unset while there are concurrent normal checkpoint
 * requests.
 */

/* These directly affect the behavior of CreateCheckPoint and subsidiaries */
#define CHECKPOINT_IS_SHUTDOWN 0x0001 /* Checkpoint is for shutdown */
#define CHECKPOINT_END_OF_RECOVERY                               \
    0x0002                          /* Like shutdown checkpoint, \
                                     * but issued at end of WAL  \
                                     * recovery */
#define CHECKPOINT_FILE_SYNC 0x0004 /* File sync */
#define CHECKPOINT_IMMEDIATE 0x0008 /* Do it without delays */
#define CHECKPOINT_FORCE 0x0010     /* Force even if no activity */
/* These are important to RequestCheckpoint */
#define CHECKPOINT_WAIT 0x0020 /* Wait for completion */
/* These indicate the cause of a checkpoint request */
#define CHECKPOINT_CAUSE_XLOG 0x0040 /* XLOG consumption */
#define CHECKPOINT_CAUSE_TIME 0x0080 /* Elapsed time */

/* Passed to PageListBackWrite() */
#define CHECKPOINT_BACKWRITE 0x0100 /* checkpoint request */
#define STRATEGY_BACKWRITE 0x0200   /* strategy backwrite */
#define LAZY_BACKWRITE 0x0400       /* lazy backwrite */
#define PAGERANGE_BACKWRITE 0x0800  /* PageRangeBackWrite */

/* Checkpoint statistics */
typedef struct CheckpointStatsData {
    TimestampTz ckpt_start_t;    /* start of checkpoint */
    TimestampTz ckpt_write_t;    /* start of flushing buffers */
    TimestampTz ckpt_sync_t;     /* start of fsyncs */
    TimestampTz ckpt_sync_end_t; /* end of fsyncs */
    TimestampTz ckpt_end_t;      /* end of checkpoint */

    int ckpt_bufs_written; /* # of buffers written */

    int ckpt_segs_added;    /* # of new xlog segments created */
    int ckpt_segs_removed;  /* # of xlog segments deleted */
    int ckpt_segs_recycled; /* # of xlog segments recycled */

    int ckpt_sync_rels;        /* # of relations synced */
    uint64 ckpt_longest_sync;  /* Longest sync for one relation */
    uint64 ckpt_agg_sync_time; /* The sum of all the individual sync
                                * times, which is not necessarily the
                                * same as the total elapsed time for
                                * the entire sync phase. */
} CheckpointStatsData;

/*
 * Current full page write info we get. forcePageWrites here is different with
 * forcePageWrites in XLogInsert, if forcePageWrites sets, we force to write
 * the page regardless of the page lsn.
 */
typedef struct XLogFPWInfo {
    XLogRecPtr redoRecPtr;
    bool doPageWrites;
    bool forcePageWrites;
} XLogFPWInfo;

struct XLogRecData;

/*
 * Shared-memory data structures for XLOG control
 *
 * LogwrtRqst indicates a byte position that we need to write and/or fsync
 * the log up to (all records before that point must be written or fsynced).
 * LogwrtResult indicates the byte positions we have already written/fsynced.
 * These structs are identical but are declared separately to indicate their
 * slightly different functions.
 *
 * To read XLogCtl->LogwrtResult, you must hold either info_lck or
 * WALWriteLock.  To update it, you need to hold both locks.  The point of
 * this arrangement is that the value can be examined by code that already
 * holds WALWriteLock without needing to grab info_lck as well.  In addition
 * to the shared variable, each backend has a private copy of LogwrtResult,
 * which is updated when convenient.
 *
 * The request bookkeeping is simpler: there is a shared XLogCtl->LogwrtRqst
 * (protected by info_lck), but we don't need to cache any copies of it.
 *
 * info_lck is only held long enough to read/update the protected variables,
 * so it's a plain spinlock.  The other locks are held longer (potentially
 * over I/O operations), so we use LWLocks for them.  These locks are:
 *
 * WALInsertLock: must be held to insert a record into the WAL buffers.
 *
 * WALWriteLock: must be held to write WAL buffers to disk (XLogWrite or
 * XLogFlush).
 *
 * ControlFileLock: must be held to read/update control file or create
 * new log file.
 *
 * CheckpointLock: must be held to do a checkpoint or restartpoint (ensures
 * only one checkpointer at a time; currently, with all checkpoints done by
 * the checkpointer, this is just pro forma).
 *
 * ----------
 */

typedef struct XLogwrtRqst {
    XLogRecPtr Write; /* last byte + 1 to write out */
    XLogRecPtr Flush; /* last byte + 1 to flush */
} XLogwrtRqst;

typedef struct XLogwrtResult {
    XLogRecPtr Write; /* last byte + 1 written out */
    XLogRecPtr Flush; /* last byte + 1 flushed */
} XLogwrtResult;

typedef struct TermFileData {
    uint32 term;
    bool finish_redo;
} TermFileData;

extern bool PreInitXlogFileForStandby(XLogRecPtr requestLsn);
extern void PreInitXlogFileForPrimary(int advance_xlog_file_num);
extern XLogRecPtr XLogInsertRecord(struct XLogRecData* rdata, XLogRecPtr fpw_lsn, bool isupgrade = false);
extern void XLogWaitFlush(XLogRecPtr recptr);
extern void XLogWaitBufferInit(XLogRecPtr recptr);
extern void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force);
extern bool XLogBackgroundFlush(void);
extern bool XLogNeedsFlush(XLogRecPtr RecPtr);
extern int XLogFileInit(XLogSegNo segno, bool* use_existent, bool use_lock);
extern int XLogFileOpen(XLogSegNo segno);

extern void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn);
extern void XLogSetReplicationSlotMaximumLSN(XLogRecPtr lsn);
extern XLogRecPtr XLogGetReplicationSlotMaximumLSN(void);
extern XLogRecPtr XLogGetReplicationSlotMinimumLSNByOther(void);

extern XLogRecPtr XLogGetLastRemovedSegno(void);
extern void xlog_redo(XLogReaderState* record);
extern void xlog_desc(StringInfo buf, XLogReaderState* record);

extern void issue_xlog_fsync(int fd, XLogSegNo segno);

extern bool RecoveryInProgress(void);
extern bool HotStandbyActive(void);
extern bool HotStandbyActiveInReplay(void);
extern bool XLogInsertAllowed(void);
extern void GetXLogReceiptTime(TimestampTz* rtime, bool* fromStream);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID* targetTLI, XLogRecPtr* ReplayReadPtr = NULL);
extern void SetXLogReplayRecPtr(XLogRecPtr readRecPtr, XLogRecPtr endRecPtr);
extern void DumpXlogCtl();

extern void CheckRecoveryConsistency(void);

extern XLogRecPtr GetXLogReplayRecPtrInPending(void);
extern XLogRecPtr GetStandbyFlushRecPtr(TimeLineID* targetTLI);
extern XLogRecPtr GetXLogInsertRecPtr(void);
extern XLogRecPtr GetXLogInsertEndRecPtr(void);
extern XLogRecPtr GetXLogWriteRecPtr(void);
extern bool RecoveryIsPaused(void);
extern void SetRecoveryPause(bool recoveryPause);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern char* XLogFileNameP(TimeLineID tli, XLogSegNo segno);
extern pg_crc32 GetXlogRecordCrc(XLogRecPtr RecPtr, bool& crcvalid);
extern bool IsCheckPoint(XLogReaderState* record);
extern bool IsRestartPointSafe(const XLogRecPtr checkPoint);
extern bool HasTimelineUpdate(XLogReaderState* record, bool bOld);
extern void UpdateTimeline(CheckPoint* checkPoint);

extern void UpdateControlFile(void);
extern uint64 GetSystemIdentifier(void);
extern void SetSystemIdentifier(uint64 system_identifier);
extern TimeLineID GetThisTimeID(void);
extern void SetThisTimeID(uint64 timelineID);
extern Size XLOGShmemSize(void);
extern void XLOGShmemInit(void);
extern void BootStrapXLOG(void);
extern int XLogSemas(void);
extern void InitWalSemaphores(void);
extern void StartupXLOG(void);
extern void ShutdownXLOG(int code, Datum arg);
extern void InitXLOGAccess(void);
extern void StartupDummyStandby(void);
extern void CreateCheckPoint(int flags);
extern bool CreateRestartPoint(int flags);
extern void XLogPutNextOid(Oid nextOid);
extern XLogRecPtr XLogRestorePoint(const char* rpName);
extern void UpdateFullPageWrites(void);
extern void GetFullPageWriteInfo(XLogFPWInfo* fpwInfo_p);
extern XLogRecPtr GetRedoRecPtr(void);
extern XLogRecPtr GetInsertRecPtr(void);
extern XLogRecPtr GetFlushRecPtr(void);
extern TimeLineID GetRecoveryTargetTLI(void);
extern void DummyStandbySetRecoveryTargetTLI(TimeLineID timeLineID);

extern bool CheckFinishRedoSignal(void);
extern bool CheckPromoteSignal(void);
extern bool CheckPrimarySignal(void);
extern bool CheckStandbySignal(void);
extern bool CheckNormalSignal(void);
extern int CheckSwitchoverSignal(void);

extern void WakeupRecovery(void);
extern void WakeupDataRecovery(void);
extern void SetWalWriterSleeping(bool sleeping);
extern uint64 XLogDiff(XLogRecPtr end, XLogRecPtr start);

extern bool CheckFpwBeforeFirstCkpt(void);
extern void DisableFpwBeforeFirstCkpt(void);

extern Size LsnXlogFlushChkShmemSize(void);
extern void LsnXlogFlushChkShmInit(void);
extern void heap_xlog_logical_new_page(XLogReaderState* record);
extern bool IsServerModeStandby(void);

extern void SetCBMTrackedLSN(XLogRecPtr trackedLSN);
extern XLogRecPtr GetCBMTrackedLSN(void);
extern XLogRecPtr GetCBMRotateLsn(void);
void SetCBMRotateLsn(XLogRecPtr rotate_lsn);
extern void SetCBMFileStartLsn(XLogRecPtr currCbmNameStartLsn);
extern XLogRecPtr GetCBMFileStartLsn(void);

extern void SetDelayXlogRecycle(bool toDelay, bool isRedo = false);
extern bool GetDelayXlogRecycle(void);
extern void SetDDLDelayStartPtr(XLogRecPtr ddlDelayStartPtr);
extern XLogRecPtr GetDDLDelayStartPtr(void);
extern void heap_xlog_bcm_new_page(xl_heap_logical_newpage* xlrec, RelFileNode node, char* cuData);

/*
 * Starting/stopping a base backup
 */
extern XLogRecPtr do_pg_start_backup(const char* backupidstr, bool fast, char** labelfile, DIR* tblspcdir,
    char** tblspcmapfile, List** tablespaces, bool infotbssize, bool needtblspcmapfile);
extern void set_start_backup_flag(bool startFlag);
extern bool get_startBackup_flag(void);
extern bool check_roach_start_backup(const char *slotName);
extern bool GetDelayXlogRecycle(void);
extern XLogRecPtr GetDDLDelayStartPtr(void);
extern XLogRecPtr do_pg_stop_backup(char* labelfile, bool waitforarchive);
extern void do_pg_abort_backup(void);
extern void RegisterAbortExclusiveBackup();
extern void enable_delay_xlog_recycle(bool isRedo = false);
extern void disable_delay_xlog_recycle(bool isRedo = false);
extern void startupInitDelayXlog(void);
extern XLogRecPtr enable_delay_ddl_recycle(void);
extern char* getLastRewindTime();
extern void disable_delay_ddl_recycle(XLogRecPtr barrierLSN, bool isForce, XLogRecPtr* startLSN, XLogRecPtr* endLSN);
extern void startupInitDelayDDL(void);
extern void execDelayedDDL(XLogRecPtr startLSN, XLogRecPtr endLSN, bool ignoreCBMErr);
extern bool IsRoachRestore(void);
extern XLogRecPtr do_roach_start_backup(const char *backupidstr);
extern XLogRecPtr do_roach_stop_backup(const char *backupidstr);
extern XLogRecPtr enable_delay_ddl_recycle_with_slot(const char* slotname);
extern void disable_delay_ddl_recycle_with_slot(const char* slotname, XLogRecPtr *startLSN, XLogRecPtr *endLSN);

extern void CloseXlogFilesAtThreadExit(void);
extern void SetLatestXTime(TimestampTz xtime);
extern CheckPoint update_checkpoint(XLogReaderState* record);
XLogRecord* XLogParallelReadNextRecord(XLogReaderState* xlogreader);

void ResourceManagerStartup(void);
void ResourceManagerStop(void);
void rm_redo_error_callback(void* arg);
extern void load_server_mode(void);
extern void WaitCheckpointSync(void);
extern void btree_clear_imcompleteAction();
extern void update_max_page_flush_lsn(XLogRecPtr biggest_lsn, ThreadId thdId, bool is_force);
extern void set_global_max_page_flush_lsn(XLogRecPtr lsn);
XLogRecPtr get_global_max_page_flush_lsn();
void GetRecoveryLatch();
void ReLeaseRecoveryLatch();
void ExtremRtoUpdateMinCheckpoint();
bool IsRecoveryDone();

void CopyXlogForForceFinishRedo(XLogSegNo logSegNo, uint32 termId, XLogReaderState *xlogreader,
                                XLogRecPtr lastRplEndLsn);
void RenameXlogForForceFinishRedo(XLogSegNo beginSegNo, TimeLineID tli, uint32 termId);
void ReOpenXlog(XLogReaderState *xlogreader);
void SetSwitchHistoryFile(XLogRecPtr switchLsn, XLogRecPtr catchLsn, uint32 termId);
void CheckMaxPageFlushLSN(XLogRecPtr reqLsn);
bool CheckForForceFinishRedoTrigger(TermFileData *term_file);

extern XLogRecPtr XlogRemoveSegPrimary;

void XLogArchiveNotify(const char *xlog);
extern bool IsValidArchiverStandby(WalSnd* walsnd);

/* File path names (all relative to $PGDATA) */
#define BACKUP_LABEL_FILE "backup_label"
#define DISABLE_CONN_FILE "disable_conn_file"
#define BACKUP_LABEL_OLD "backup_label.old"
#define BACKUP_LABEL_FILE_ROACH "backup_label.roach"
#define BACKUP_LABEL_FILE_ROACH_DONE "backup_label.roach.done"
#define DELAY_XLOG_RECYCLE_FILE "delay_xlog_recycle"
#define DELAY_DDL_RECYCLE_FILE "delay_ddl_recycle"
#define REWIND_LABLE_FILE "rewind_lable"

typedef struct delayddlrange {
    XLogRecPtr startLSN;
    XLogRecPtr endLSN;
} DelayDDLRange;

#define InvalidDelayRange                    \
    (DelayDDLRange)                          \
    {                                        \
        InvalidXLogRecPtr, InvalidXLogRecPtr \
    }
#define DISABLE_DDL_DELAY_TIMEOUT 1800000
#define ENABLE_DDL_DELAY_TIMEOUT 60000

#define TABLESPACE_MAP "tablespace_map"
#define TABLESPACE_MAP_OLD "tablespace_map.old"

#define ROACH_BACKUP_PREFIX "gs_roach"
#define ROACH_FULL_BAK_PREFIX "gs_roach_full"
#define ROACH_INC_BAK_PREFIX "gs_roach_inc"

static const uint64 INVALID_READ_OFF = 0xFFFFFFFF;

inline bool force_finish_enabled()
{
    return (g_instance.attr.attr_storage.enable_update_max_page_flush_lsn != 0);
}

static inline void WakeupWalSemaphore(PGSemaphore sema)
{
    PGSemaphoreReset(sema);
    PGSemaphoreUnlock(sema);
}

#endif /* XLOG_H */
