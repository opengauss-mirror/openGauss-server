/*
 * xlog.h
 *
 * openGauss transaction log manager
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * src/include/access/xlog.h
 */
#ifndef XLOG_H
#define XLOG_H

#include "access/htup.h"
#include "access/parallel_recovery/redo_item.h"
#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "knl/knl_instance.h"

#include "access/htup.h"
#include "storage/lock/lwlock.h"

#include <time.h>
#include "nodes/pg_list.h"

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
    RECOVERY_TARGET_LSN,
    RECOVERY_TARGET_TIME_OBS
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

extern bool XLogBackgroundFlush(void);

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

#define WAL_NOT_COPIED 0
#define WAL_COPIED 1
#define WAL_COPY_SUSPEND (-1)
#define WAL_SCANNED_LRC_INIT (-2)


/* The current size of walInsertStatusTable is calculated based on 
 * the max_connections parameter or the number of threads configured 
 * in thread_pool_attr. WAL_INSERT_STATUS_TBL_EXPAND_FACTOR is used 
 * as the expansion factor to ensure that walInsertStatusTable has 
 * certain redundancy.
 */
#define WAL_INSERT_STATUS_TBL_EXPAND_FACTOR (4)
/* 
 * Indicates the min size of the g_instance.wal_cxt.walInsertStatusTable, 
 * which is calculated to the power of 2. To ensure xlog performance, 
 * the size of each xlog write is estimated based on experience. (Currently,
 * the number is several hundred KB. We expand it to 2 MB to cope with the 
 * CPU performance improvement.) , divided by the smallest xlog (only the 
 * header of the xlog and the size of the xlog is 32 bytes). Therefore, 
 * the size of walInsertStatusTable is calculated as 64 KB.
 */
#define MIN_WAL_INSERT_STATUS_ENTRY_POW (16)

#define GET_WAL_INSERT_STATUS_ENTRY_CNT(num) \
    ((num <= 0) ? (1 << MIN_WAL_INSERT_STATUS_ENTRY_POW) : (1 << num))
/* (ientry + 1) % WAL_INSERT_STATUS_ENTRIES */
#define GET_NEXT_STATUS_ENTRY(num, ientry) ((ientry + 1) & (GET_WAL_INSERT_STATUS_ENTRY_CNT(num) - 1))

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

/* standby file repair, need wait all dirty page flush, can remove old bad file */
#define CHECKPOINT_FLUSH_DIRTY 0x0004 /* only flush dirty page, don't update redo point */

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

extern void XLogMultiFileInit(int advance_xlog_file_num);
typedef struct XLogwrtPaxos {
    XLogRecPtr  Write; /* last byte + 1 written out */
    XLogRecPtr  Consensus; /* last byte + 1 consensus in DCF */
    unsigned long long PaxosIndex; /* The index in DCF log, which is corresponding to consensus. */
} XLogwrtPaxos;

typedef struct TermFileData {
    uint32 term;
    bool finish_redo;
} TermFileData;

/*
 * Inserting to WAL is protected by a small fixed number of WAL insertion
 * locks. To insert to the WAL, you must hold one of the locks - it doesn't
 * matter which one. To lock out other concurrent insertions, you must hold
 * of them. Each WAL insertion lock consists of a lightweight lock, plus an
 * indicator of how far the insertion has progressed (insertingAt).
 *
 * The insertingAt values are read when a process wants to flush WAL from
 * the in-memory buffers to disk, to check that all the insertions to the
 * region the process is about to write out have finished. You could simply
 * wait for all currently in-progress insertions to finish, but the
 * insertingAt indicator allows you to ignore insertions to later in the WAL,
 * so that you only wait for the insertions that are modifying the buffers
 * you're about to write out.
 *
 * This isn't just an optimization. If all the WAL buffers are dirty, an
 * inserter that's holding a WAL insert lock might need to evict an old WAL
 * buffer, which requires flushing the WAL. If it's possible for an inserter
 * to block on another inserter unnecessarily, deadlock can arise when two
 * inserters holding a WAL insert lock wait for each other to finish their
 * insertion.
 *
 * Small WAL records that don't cross a page boundary never update the value,
 * the WAL record is just copied to the page and the lock is released. But
 * to avoid the deadlock-scenario explained above, the indicator is always
 * updated before sleeping while holding an insertion lock.
 */
typedef struct {
    LWLock lock;
#ifdef __aarch64__
    pg_atomic_uint32 xlogGroupFirst;
#endif
    XLogRecPtr insertingAt;
} WALInsertLock;

/*
 * All the WAL insertion locks are allocated as an array in shared memory. We
 * force the array stride to be a power of 2, which saves a few cycles in
 * indexing, but more importantly also ensures that individual slots don't
 * cross cache line boundaries. (Of course, we have to also ensure that the
 * array start address is suitably aligned.)
 */
typedef union WALInsertLockPadded {
    WALInsertLock l;
    char pad[PG_CACHE_LINE_SIZE];
} WALInsertLockPadded;

/*
 * Shared state data for WAL insertion.
 */
typedef struct XLogCtlInsert {
    /*
     * CurrBytePos is the end of reserved WAL. The next record will be inserted
     * at that position. PrevBytePos is the start position of the previously
     * inserted (or rather, reserved) record - it is copied to the the prev-
     * link of the next record. These are stored as "usable byte positions"
     * rather than XLogRecPtrs (see XLogBytePosToRecPtr()).
     */
    uint64 CurrBytePos;
    uint32 PrevByteSize;
    int32 CurrLRC;

#if (!defined __x86_64__) && (!defined __aarch64__)
    slock_t insertpos_lck; /* protects CurrBytePos and PrevBytePos */
#endif
    /*
     * Make sure the above heavily-contended spinlock and byte positions are
     * on their own cache line. In particular, the RedoRecPtr and full page
     * write variables below should be on a different cache line. They are
     * read on every WAL insertion, but updated rarely, and we don't want
     * those reads to steal the cache line containing Curr/PrevBytePos.
     */
    char pad[PG_CACHE_LINE_SIZE];
    /*
     * WAL insertion locks.
     */
    WALInsertLockPadded **WALInsertLocks;

    /*
     * fullPageWrites is the master copy used by all backends to determine
     * whether to write full-page to WAL, instead of using process-local one.
     * This is required because, when full_page_writes is changed by SIGHUP,
     * we must WAL-log it before it actually affects WAL-logging by backends.
     * Checkpointer sets at startup or after SIGHUP.
     * To read these fields, you must hold an insertion slot. To modify them,
     * you must hold ALL the slots.
     */
    XLogRecPtr RedoRecPtr; /* current redo point for insertions */
    bool forcePageWrites;  /* forcing full-page writes for PITR? */
    bool fullPageWrites;

    /*
     * exclusiveBackup is true if a backup started with pg_start_backup() is
     * in progress, and nonExclusiveBackups is a counter indicating the number
     * of streaming base backups currently in progress. forcePageWrites is set
     * to true when either of these is non-zero. lastBackupStart is the latest
     * checkpoint redo location used as a starting point for an online backup.
     */
    bool exclusiveBackup;
    int nonExclusiveBackups;
    XLogRecPtr lastBackupStart;
} XLogCtlInsert;

/*
 * Total shared-memory state for XLOG.
 */
typedef struct XLogCtlData {
    /* Protected by WALInsertLock: */
    XLogCtlInsert Insert;

    /* Protected by info_lck: */
    XLogwrtRqst LogwrtRqst;
    XLogRecPtr RedoRecPtr; /* a recent copy of Insert->RedoRecPtr */
    TransactionId ckptXid;
    XLogRecPtr asyncXactLSN;          /* LSN of newest async commit/abort */
    XLogRecPtr replicationSlotMinLSN; /* oldest LSN needed by any slot */
    XLogRecPtr replicationSlotMaxLSN; /* latest LSN for dummy startpoint */
    XLogSegNo lastRemovedSegNo;       /* latest removed/recycled XLOG segment */

    /* Time of last xlog segment switch. Protected by WALWriteLock. */
    pg_time_t lastSegSwitchTime;

    /*
     * Protected by info_lck and WALWriteLock (you must hold either lock to
     * read it, but both to update)
     */
    XLogwrtResult LogwrtResult;

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * Protected by info_lck and WALWritePaxosLock (you must hold either lock to
     * read it, but both to update)
     */
    XLogwrtPaxos LogwrtPaxos;
#endif

    /*
     * Latest initialized block index in cache.
     *
     * To change curridx and the identity of a buffer, you need to hold
     * WALBufMappingLock.  To change the identity of a buffer that's still
     * dirty, the old page needs to be written out first, and for that you
     * need WALWriteLock, and you need to ensure that there are no in-progress
     * insertions to the page by calling WaitXLogInsertionsToFinish().
     */
    XLogRecPtr InitializedUpTo;

    /*
     * These values do not change after startup, although the pointed-to pages
     * and xlblocks values certainly do.  xlblock values are protected by
     * WALBufMappingLock.
     */
    char *pages;          /* buffers for unwritten XLOG pages */
    XLogRecPtr *xlblocks; /* 1st byte ptr-s + XLOG_BLCKSZ */
    int XLogCacheBlck;    /* highest allocated xlog buffer index */
    TimeLineID ThisTimeLineID;

    /*
     * archiveCleanupCommand is read from recovery.conf but needs to be in
     * shared memory so that the checkpointer process can access it.
     */
    char archiveCleanupCommand[MAXPGPATH];

    /*
     * SharedRecoveryInProgress indicates if we're still in crash or archive
     * recovery.  Protected by info_lck.
     */
    bool SharedRecoveryInProgress;

    bool IsRecoveryDone;

    /*
     * SharedHotStandbyActive indicates if we're still in crash or archive
     * recovery.  Protected by info_lck.
     */
    bool SharedHotStandbyActive;

    /*
     * WalWriterSleeping indicates whether the WAL writer is currently in
     * low-power mode (and hence should be nudged if an async commit occurs).
     * Protected by info_lck.
     */
    bool WalWriterSleeping;

    /*
     * recoveryWakeupLatch is used to wake up the startup process to continue
     * WAL replay, if it is waiting for WAL to arrive or failover trigger file
     * to appear.
     */
    Latch recoveryWakeupLatch;

    Latch dataRecoveryLatch;

    /*
     * During recovery, we keep a copy of the latest checkpoint record here.
     * Used by the background writer when it wants to create a restartpoint.
     *
     * Protected by info_lck.
     */
    XLogRecPtr lastCheckPointRecPtr;
    CheckPoint lastCheckPoint;

    /* lastReplayedReadRecPtr points to the header of last apply lsn. */
    XLogRecPtr lastReplayedReadRecPtr;
    /*
     * lastReplayedEndRecPtr points to end+1 of the last record successfully
     * replayed. When we're currently replaying a record, ie. in a redo
     * function, replayEndRecPtr points to the end+1 of the record being
     * replayed, otherwise it's equal to lastReplayedEndRecPtr.
     */
    XLogRecPtr lastReplayedEndRecPtr;
    XLogRecPtr replayEndRecPtr;
    /* timestamp of last COMMIT/ABORT record replayed (or being replayed) */
    TimestampTz recoveryLastXTime;
    /* current effective recovery target timeline */
    TimeLineID RecoveryTargetTLI;

    /*
     * timestamp of when we started replaying the current chunk of WAL data,
     * only relevant for replication or archive recovery
     */
    TimestampTz currentChunkStartTime;
    /* Are we requested to pause recovery? */
    bool recoveryPause;

    /* Are we requested to suspend recovery? */
    bool recoverySusPend;

    /*
     * lastFpwDisableRecPtr points to the start of the last replayed
     * XLOG_FPW_CHANGE record that instructs full_page_writes is disabled.
     */
    XLogRecPtr lastFpwDisableRecPtr;

    /*
     * After started up, we need to make sure that
     * it will do full page write before the first checkpoint.
     */
    bool FpwBeforeFirstCkpt;

    /* LSN of xlogs already tracked by CBM, which checkpoint can now recycle. */
    XLogRecPtr cbmTrackedLSN;
    /* the bitmap file rotate lsn requested by outter */
    volatile XLogRecPtr cbmMaxRequestRotateLsn;
    /* curr cbm file name  like pg_xlog_xx_xxxx_xxxx.cbm */
    XLogRecPtr currCbmFileStartLsn;

    /* if true, stale xlog segments are not recycled during checkpoint, for backup purpose */
    bool delayXlogRecycle;

    /* start point from where dropped column relation files are delayed to do physical unlinking */
    XLogRecPtr ddlDelayStartPtr;

    /* start point for logging new remain segments or extents */
    XLogRecPtr remain_segs_start_point;
    bool is_need_log_remain_segs;
    XLogRecPtr remainCommitLsn;

    slock_t info_lck; /* locks shared variables shown above */
} XLogCtlData;

/* Xlog flush statistics*/
struct XlogFlushStats{
    bool statSwitch;
    uint64 writeTimes;
    uint64 syncTimes;
    uint64 totalXlogSyncBytes;
    uint64 totalActualXlogSyncBytes;
    uint32 avgWriteBytes;
    uint32 avgActualWriteBytes;
    uint32 avgSyncBytes;
    uint32 avgActualSyncBytes;
    uint64 totalWriteTime;
    uint64 totalSyncTime;
    uint64 avgWriteTime;
    uint64 avgSyncTime;
    uint64 currOpenXlogSegNo;
    TimestampTz lastRestTime;
};

extern XLogSegNo GetNewestXLOGSegNo(const char* workingPath);
/*
 * Hint bit for whether xlog contains CSN info, which is stored in xl_term.
 */
#define XLOG_CONTAIN_CSN 0x80000000
#define XLOG_MASK_TERM 0x7FFFFFFF

extern void XLogMultiFileInit(int advance_xlog_file_num);

extern XLogRecPtr XLogInsertRecord(struct XLogRecData* rdata, XLogRecPtr fpw_lsn);
extern void XLogWaitFlush(XLogRecPtr recptr);
extern void XLogWaitBufferInit(XLogRecPtr recptr);
extern void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force);
extern bool XLogBackgroundFlush(void);
extern bool XLogNeedsFlush(XLogRecPtr RecPtr);
extern int XLogFileInit(XLogSegNo segno, bool* use_existent, bool use_lock);
extern int XLogFileOpen(XLogSegNo segno);
extern bool PreInitXlogFileForStandby(XLogRecPtr requestLsn);
extern void PreInitXlogFileForPrimary(int advance_xlog_file_num);

extern void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn);
extern void XLogSetReplicationSlotMaximumLSN(XLogRecPtr lsn);
extern XLogRecPtr XLogGetReplicationSlotMaximumLSN(void);
extern XLogRecPtr XLogGetReplicationSlotMinimumLSNByOther(void);

extern XLogSegNo XLogGetLastRemovedSegno(void);
extern void xlog_redo(XLogReaderState* record);
extern void xlog_desc(StringInfo buf, XLogReaderState* record);
extern const char *xlog_type_name(uint8 subtype);

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
#ifndef ENABLE_MULTIPLE_NODES
extern XLogRecPtr GetPaxosWriteRecPtr(void);
extern XLogRecPtr GetPaxosConsensusRecPtr(void);
#endif
extern bool RecoveryIsPaused(void);
extern void SetRecoveryPause(bool recoveryPause);
extern void SetRecoverySuspend(bool recoverySuspend);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern char* XLogFileNameP(TimeLineID tli, XLogSegNo segno);
extern pg_crc32 GetXlogRecordCrc(XLogRecPtr RecPtr, bool& crcvalid, XLogPageReadCB pagereadfunc, Size bufAlignSize);
extern bool IsCheckPoint(XLogReaderState* record);
extern bool IsRestartPointSafe(const XLogRecPtr checkPoint);
extern bool HasTimelineUpdate(XLogReaderState* record);
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
extern void ShutdownShareStorageXLogCopy();
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
extern TimeLineID GetThisTargetTLI(void);
extern void DummyStandbySetRecoveryTargetTLI(TimeLineID timeLineID);

extern bool CheckFinishRedoSignal(void);
extern bool CheckPromoteSignal(void);
extern bool CheckPrimarySignal(void);
extern bool CheckStandbySignal(void);
extern bool CheckCascadeStandbySignal(void);
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
extern XLogRecPtr StandbyDoStartBackup(const char* backupidstr, char** labelFile, char** tblSpcMapFile,
    List** tableSpaces, DIR* tblSpcDir, bool infoTbsSize);
extern void set_start_backup_flag(bool startFlag);
extern bool get_startBackup_flag(void);
extern bool check_roach_start_backup(const char *slotName);
extern bool GetDelayXlogRecycle(void);
extern XLogRecPtr GetDDLDelayStartPtr(void);
extern XLogRecPtr do_pg_stop_backup(char *labelfile, bool waitforarchive, unsigned long long* consensusPaxosIdx = NULL);
extern XLogRecPtr StandbyDoStopBackup(char *labelfile);
extern void do_pg_abort_backup(void);
extern void RegisterPersistentAbortBackupHandler(void);
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
extern char* TrimStr(const char* str);


extern void CloseXlogFilesAtThreadExit(void);
extern void SetLatestXTime(TimestampTz xtime);
XLogRecord* XLogParallelReadNextRecord(XLogReaderState* xlogreader);

void ResourceManagerStartup(void);
void ResourceManagerStop(void);
void rm_redo_error_callback(void* arg);
extern void load_server_mode(void);
extern void WaitCheckpointSync(void);
extern void btree_clear_imcompleteAction();
extern void update_max_page_flush_lsn(XLogRecPtr biggest_lsn, ThreadId thdId, bool is_force);
extern void set_global_max_page_flush_lsn(XLogRecPtr lsn);
extern void DcfUpdateConsensusLsnAndIndex(XLogRecPtr ConsensusLsn, unsigned long long dcfIndex);

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
void close_readFile_if_open();
void InitShareStorageCtlInfo(ShareStorageXLogCtl *ctlInfo, uint64 sysidentifier);
void UpdateShareStorageCtlInfo(const ShareStorageXLogCtl *ctlInfo);
void ReadShareStorageCtlInfo(ShareStorageXLogCtl* ctlInfo);
pg_crc32c CalShareStorageCtlInfoCrc(const ShareStorageXLogCtl *ctlInfo);
int ReadXlogFromShareStorage(XLogRecPtr startLsn, char *buf, int expectReadLen);
int WriteXlogToShareStorage(XLogRecPtr startLsn, char *buf, int writeLen);
void FsyncXlogToShareStorage();
Size SimpleValidatePage(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, char* page);
void ShareStorageInit();
void FindLastRecordCheckInfoOnShareStorage(XLogRecPtr *lastRecordPtr, pg_crc32 *lastRecordCrc,
    int *lastRecordLen);
int SharedStorageXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, 
    XLogRecPtr targetRecPtr, char *readBuf, TimeLineID *readTLI, char *xlog_path);
void ShareStorageSetBuildErrorAndExit(HaRebuildReason reason, bool setRcvDone = true);
void rename_recovery_conf_for_roach();
bool CheckForFailoverTrigger(void);
bool CheckForSwitchoverTrigger(void);
void HandleCascadeStandbyPromote(XLogRecPtr *recptr);

extern XLogRecPtr XlogRemoveSegPrimary;

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
#define ENABLE_DDL_DELAY_TIMEOUT 3600000 /* 1 hour */

#define TABLESPACE_MAP "tablespace_map"
#define TABLESPACE_MAP_OLD "tablespace_map.old"

#define ROACH_BACKUP_PREFIX "gs_roach"
#define ROACH_FULL_BAK_PREFIX "gs_roach_full"
#define ROACH_INC_BAK_PREFIX "gs_roach_inc"
#define CSN_BARRIER_PREFIX "csn"

static const uint64 INVALID_READ_OFF = 0xFFFFFFFF;

#define FORCE_FINISH_ENABLED \
    (g_instance.attr.attr_storage.enable_update_max_page_flush_lsn != 0)

struct PGSemaphoreData;
typedef PGSemaphoreData* PGSemaphore;
void PGSemaphoreReset(PGSemaphore sema);
void PGSemaphoreUnlock(PGSemaphore sema);

#define TABLESPACE_MAP "tablespace_map"
#define TABLESPACE_MAP_OLD "tablespace_map.old"

static inline void WakeupWalSemaphore(PGSemaphore sema)
{
    PGSemaphoreReset(sema);
    PGSemaphoreUnlock(sema);
}

/*
 * Options for enum values stored in other modules
 */
extern struct config_enum_entry wal_level_options[];
extern struct config_enum_entry sync_method_options[];

extern void SetRemainSegsStartPoint(XLogRecPtr remain_segs_start_point);
extern XLogRecPtr GetRemainSegsStartPoint(void);
extern void UpdateRemainSegsStartPoint(XLogRecPtr new_start_point);
extern bool IsNeedLogRemainSegs(XLogRecPtr cur_lsn);

extern void SetRemainCommitLsn(XLogRecPtr remainCommitLsn);
extern XLogRecPtr GetRemainCommitLsn(void);
extern void UpdateRemainCommitLsn(XLogRecPtr newRemainCommitLsn);

void RecoveryAllocSegsFromRemainSegsFile(struct HTAB* alloc_segs_htbl, char* buf, uint32* used_len);
void RecoveryDropSegsFromRemainSegsFile(struct HTAB* drop_segs_hbtl, char* buf, uint32* used_len);
void RecoveryShrinkExtentsFromRemainSegsFile(struct HTAB* shrink_extents_htbl, char* buf, uint32* used_len);
void WriteRemainSegsFile(int fd, const char* buffer, uint32 used_len);
void InitXlogStatuEntryTblSize();
void CheckShareStorageWriteLock();
XLogRecPtr GetFlushMainStandby();
extern bool RecoveryIsSuspend(void);

extern void InitUndoCountThreshold();
#endif /* XLOG_H */
