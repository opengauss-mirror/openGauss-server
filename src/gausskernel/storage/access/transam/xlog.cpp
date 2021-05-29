/* -------------------------------------------------------------------------
 *
 * xlog.cpp
 *		PostgreSQL transaction log manager
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/xlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <net/if.h>
#ifdef __USE_NUMA
#include <numa.h>
#endif

#include "access/cbmparsexlog.h"
#include "access/clog.h"
#include "access/csnlog.h"
#include "access/cstore_insert.h"
#include "access/double_write.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/xlogdefs.h"
#include "access/hash.h"
#include "access/xlogproc.h"
#include "access/parallel_recovery/dispatcher.h"

#include "commands/tablespace.h"
#include "commands/matview.h"

#include "catalog/catalog.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "catalog/storage.h"
#include "gs_bbox.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/barrier.h"
#endif
#include "pgstat.h"
#include "port.h"
#include "postmaster/bgwriter.h"
#include "postmaster/startup.h"
#include "postmaster/postmaster.h"
#include "postmaster/pagewriter.h"
#include "replication/logical.h"
#include "replication/bcm.h"
#include "replication/basebackup.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "replication/reorderbuffer.h"
#include "replication/replicainternal.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender_private.h"
#include "replication/walsender.h"
#include "replication/walprotocol.h"
#include "replication/obswalreceiver.h"
#include "storage/buf/bufmgr.h"
#include "storage/fd.h"
#include "storage/copydir.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/reinit.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/lock/lwlock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/inet.h"
#include "utils/atomic.h"
#include "pg_trace.h"
#include "gssignal/gs_signal.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include <sched.h>
#include <utmpx.h>
#include <time.h>

#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif

/* just for libpqrcv_connect_for_TLI */
#include "replication/libpqwalreceiver.h"

/* Used for parallel recovery */

#include "access/redo_statistic.h"
#include "access/multi_redo_api.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/spsc_blocking_queue.h"
#include "access/extreme_rto/page_redo.h"
#include "vectorsonic/vsonichash.h"
/* File path names (all relative to $PGDATA) */
#define RECOVERY_COMMAND_FILE "recovery.conf"
#define RECOVERY_COMMAND_DONE "recovery.done"
#define FAILOVER_SIGNAL_FILE "failover"
#define SWITCHOVER_SIGNAL_FILE "switchover"
#define PRIMARY_SIGNAL_FILE "primary"
#define STANDBY_SIGNAL_FILE "standby"
#define XLOG_SWITCH_HISTORY_FILE "switch.history"
#define MAX_PATH_LEN 1024
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#define ENABLE_INCRE_CKPT g_instance.attr.attr_storage.enableIncrementalCheckpoint

#define RecoveryFromDummyStandby() (t_thrd.postmaster_cxt.ReplConnArray[2] != NULL && IS_DN_DUMMY_STANDYS_MODE())

#define CHECKPOINT_LEN (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint))
#define CHECKPOINTNEW_LEN (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPointNew))
#define CHECKPOINTPLUS_LEN (SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPointPlus))

/* MaxMacAddrList controls GetMACAddrHash function to get the max mac number */
#define MaxMacAddrList 10

const int SIZE_OF_UINT64 = 8;
const int SIZE_OF_UINT32 = 4;
const int SIZE_OF_TWO_UINT64 = 16;
const int XLOG_LSN_SWAP = 32;
const char *DemoteModeDescs[] = { "unknown", "smart", "fast", "immediate" };
const int DemoteModeNum = sizeof(DemoteModeDescs) / sizeof(char *);

static const uint64 REDO_SPEED_LOG_LEN = (XLogSegSize * 64); /* 64 segs */
static const int PG_TBLSPCS = 10; /* strlen(pg_tblspcs/) */

/* MAX_PAGE_FLUSH_LSN_FILE SIZE : 1024 pages, 8k each, file size 8M in total */
static const int MPFL_PAGE_NUM = NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS;
static const int MPFL_FILE_SIZE = (BLCKSZ * MPFL_PAGE_NUM);
static const int MPFL_FILE_FLAG = (O_RDWR | O_SYNC | O_DIRECT | PG_BINARY);
static const mode_t MPFL_FILE_PERM = (S_IRUSR | S_IWUSR);

THR_LOCAL bool redo_oldversion_xlog = false;

/*
 * XLOGfileslop is the maximum number of preallocated future XLOG segments.
 * When we are done with an old XLOG segment file, we will recycle it as a
 * future XLOG segment as long as there aren't already XLOGfileslop future
 * segments; else we'll delete it.  This could be made a separate GUC
 * variable, but at present I think it's sufficient to hardwire it as
 * 2*CheckPointSegments+1.	Under normal conditions, a checkpoint will free
 * no more than 2*CheckPointSegments log segments, and we want to recycle all
 * of them; the +1 allows boundary cases to happen without wasting a
 * delete/create-segment cycle.
 */
#define XLOGfileslop (2 * u_sess->attr.attr_storage.CheckPointSegments + 1)

/*
 * GUC support
 */
struct config_enum_entry sync_method_options[] = {
    { "fsync", SYNC_METHOD_FSYNC, false },
#ifdef HAVE_FSYNC_WRITETHROUGH
    { "fsync_writethrough", SYNC_METHOD_FSYNC_WRITETHROUGH, false },
#endif
#ifdef HAVE_FDATASYNC
    { "fdatasync", SYNC_METHOD_FDATASYNC, false },
#endif
#ifdef OPEN_SYNC_FLAG
    { "open_sync", SYNC_METHOD_OPEN, false },
#endif
#ifdef OPEN_DATASYNC_FLAG
    { "open_datasync", SYNC_METHOD_OPEN_DSYNC, false },
#endif
    { NULL, 0, false }
};

XLogRecPtr latestValidRecord = InvalidXLogRecPtr;
pg_crc32 latestRecordCrc = InvalidXLogRecPtr;

XLogSegNo XlogRemoveSegPrimary = InvalidXLogSegPtr;


/* The nextXid and oldestXid in ShmemVariableCache when recovery done */
TransactionId NextXidAfterReovery;
TransactionId OldestXidAfterRecovery;

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
    /* curr cbm file name  like pg_xlog_xx_xxxx_xxxx.cbm*/
    XLogRecPtr currCbmFileStartLsn;

    /* if true, stale xlog segments are not recycled during checkpoint, for backup purpose */
    bool delayXlogRecycle;

    /* start point from where dropped column relation files are delayed to do physical unlinking */
    XLogRecPtr ddlDelayStartPtr;

    slock_t info_lck; /* locks shared variables shown above */
} XLogCtlData;

static void remove_xlogtemp_files(void);
static bool validate_parse_delay_ddl_file(DelayDDLRange *delayRange);
static bool write_delay_ddl_file(const DelayDDLRange &delayRange, bool onErrDelete);
extern void CalculateLocalLatestSnapshot(bool forceCalc);
void update_dirty_page_queue_rec_lsn(XLogRecPtr current_insert_lsn, bool need_immediately_update = false);

/*
 * Calculate the amount of space left on the page after 'endptr'. Beware
 * multiple evaluation!
 */
#define INSERT_FREESPACE(endptr) (((endptr) % XLOG_BLCKSZ == 0) ? 0 : (XLOG_BLCKSZ - (endptr) % XLOG_BLCKSZ))

/* Macro to advance to next buffer index. */
#define NextBufIdx(idx) (((idx) == t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck) ? 0 : ((idx) + 1))

/* Added for XLOG scaling */
/*
 * Added for XLOG scaling
 *
 * XLogRecPtrToBufIdx returns the index of the WAL buffer that holds, or
 * would hold if it was in cache, the page containing 'recptr'.
 *
 * XLogRecEndPtrToBufIdx is the same, but a pointer to the first byte of a
 * page is taken to mean the previous page.
 */
#define XLogRecPtrToBufIdx(recptr) (((recptr) / XLOG_BLCKSZ) % (t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck + 1))

/*
 * Return the previous xlblock[i] mapped to the same page
 */
#define XLogPrevBlockInBufPage(recptr) \
    (((recptr) < (XLogRecPtr)(XLOG_BLCKSZ * (t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck + 1))) ? 0 : (XLogRecPtr)((recptr) - XLOG_BLCKSZ * (t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck + 1)))

/*
 * These are the number of bytes in a WAL page and segment usable for WAL data.
 */
#define UsableBytesInPage (XLOG_BLCKSZ - SizeOfXLogShortPHD)
#define UsableBytesInSegment \
    ((XLOG_SEG_SIZE / XLOG_BLCKSZ) * UsableBytesInPage - (SizeOfXLogLongPHD - SizeOfXLogShortPHD))

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

/*
 * xlog switch information for forcing failover.
 */
typedef struct XLogSwitchInfo {
    uint32 termId;
    uint64 switchLsn;
    uint64 catchLsn;
    uint64 oldestRunningXid;
    uint64 latestCompletedXid;
    uint32 invaildPageCnt;
    uint32 imcompleteActionCnt;
} XLogSwitchInfo;

volatile bool IsPendingXactsRecoveryDone = false;

static void XLogFlushCore(XLogRecPtr WriteRqstPtr);
static void XLogSelfFlush(void);
static void XLogSelfFlushWithoutStatus(int numHitsOnStartPage, XLogRecPtr CurrPos, int currLRC);

static void XLogArchiveNotifySeg(XLogSegNo segno);
static bool XLogArchiveCheckDone(const char *xlog);
static bool HasBeenArchivedOnHaMode(const char* xlog);
static bool XLogArchiveIsBusy(const char *xlog);
static bool XLogArchiveIsReady(const char *xlog);
static void XLogArchiveCleanup(const char *xlog);
static void readRecoveryCommandFile(void);
static void exitArchiveRecovery(TimeLineID endTLI, XLogSegNo endSegNo);
static bool recoveryStopsHere(XLogReaderState *record, bool *includeThis);
static void recoveryPausesHere(void);
#ifndef ENABLE_MULTIPLE_NODES
static bool RecoveryApplyDelay(const XLogReaderState *record);
#endif
static void SetCurrentChunkStartTime(TimestampTz xtime);
static void CheckRequiredParameterValues(bool DBStateShutdown);
static void XLogReportParameters(void);
static void LocalSetXLogInsertAllowed(void);
static void CheckPointGuts(XLogRecPtr checkPointRedo, int flags, bool doFullCheckpoint);
static void KeepLogSeg(XLogRecPtr recptr, XLogSegNo *logSegNo, XLogRecPtr curInsert);

static XLogRecPtr XLogGetReplicationSlotMinimumLSN(void);

template <bool isGroupInsert>
static void AdvanceXLInsertBuffer(XLogRecPtr upto, bool opportunistic, PGPROC *proc = NULL);

static bool XLogCheckpointNeeded(XLogSegNo new_segno);
static void XLogWrite(const XLogwrtRqst &WriteRqst, bool flexible);
static bool InstallXLogFileSegment(XLogSegNo *segno, const char *tmppath, bool find_free, int *max_advance,
                                   bool use_lock);
static int XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli, int source, bool notexistOk);
static int XLogFileReadAnyTLI(XLogSegNo segno, int emode, uint32 sources);
static int emode_for_corrupt_record(int emode, XLogRecPtr RecPtr);
static void XLogFileClose(void);
static void KeepFileRestoredFromArchive(const char *path, const char *xlogfname);
static bool RestoreArchivedFile(char *path, const char *xlogfname, const char *recovername, off_t expectedSize);
static void ExecuteRecoveryCommand(char *command, char *commandName, bool failOnerror);
static void PreallocXlogFiles(XLogRecPtr endptr);
static void RemoveOldXlogFiles(XLogSegNo segno, XLogRecPtr endptr);
static void RemoveXlogFile(const char *segname, XLogRecPtr ednptr);
static void RemoveNonParentXlogFiles(XLogRecPtr switchpoint, TimeLineID newTLI);
static void UpdateLastRemovedPtr(const char *filename);
static void ValidateXLOGDirectoryStructure(void);
static void CleanupBackupHistory(void);
static XLogRecord *ReadRecord(XLogReaderState *xlogreader, XLogRecPtr RecPtr, int emode, bool fetching_ckpt,
                              bool readoldversion = false);
void CheckRecoveryConsistency(void);
static XLogRecord *ReadCheckpointRecord(XLogReaderState *xlogreader, XLogRecPtr RecPtr, int whichChkpt,
                                        bool readoldversion = false);
static List *readTimeLineHistory(TimeLineID targetTLI);
static bool existsTimeLineHistory(TimeLineID probeTLI);
static bool rescanLatestTimeLine(void);
static TimeLineID findNewestTimeLine(TimeLineID startTLI);
static bool timeLineInHistory(TimeLineID tli, List *expectedTLEs);
static void WriteControlFile(void);
static void ReadControlFile(void);
static void RecoverControlFile(void);
static char *str_time(pg_time_t tnow);
static bool CheckForFailoverTrigger(void);
static bool CheckForSwitchoverTrigger(void);
static bool CheckForPrimaryTrigger(void);
static bool CheckForStandbyTrigger(void);

#ifdef WAL_DEBUG
static void xlog_outrec(StringInfo buf, XLogReaderState *record);
#endif
static void pg_start_backup_callback(int code, Datum arg);
static void AbortExcluseBackupCallback(int code, Datum arg);
static bool read_tablespace_map(List** tablespaces);
static bool read_backup_label(XLogRecPtr *checkPointLoc, bool *backupEndRequired, bool *backupFromStandby, bool *backupFromRoach);
static void check_roach_backup_condition(bool backup_started_in_recovery, const char *backupidstr);
static int get_sync_bit(int method);
static void ResetSlotLSNEndRecovery(StringInfo slotname);
static void ShutdownReadFileFacility(void);
static void SetDummyStandbyEndRecPtr(XLogReaderState *xlogreader);

/* XLOG scaling: start */
static void CopyXLogRecordToWAL(int write_len, bool isLogSwitch, XLogRecData* rdata, XLogRecPtr StartPos,
    XLogRecPtr EndPos, int32 *const currlrc_ptr);
static void ReserveXLogInsertLocation(uint32 size, XLogRecPtr* StartPos, XLogRecPtr* EndPos, XLogRecPtr* PrevPtr,
    int32 *const currlrc_ptr);
static bool ReserveXLogSwitch(XLogRecPtr* StartPos, XLogRecPtr* EndPos, XLogRecPtr* PrevPtr, bool isupgrade = false);
static void StartSuspendWalInsert(int32 *const lastlrc_ptr);
static void StopSuspendWalInsert(int32 lastlrc);

template <bool isGroupInsert>
static char *GetXLogBuffer(XLogRecPtr ptr, PGPROC *proc = NULL);
static XLogRecPtr XLogBytePosToRecPtr(uint64 bytepos);
static XLogRecPtr XLogBytePosToEndRecPtr(uint64 bytepos);
static uint64 XLogRecPtrToBytePos(XLogRecPtr ptr);

static XLogRecPtr XLogInsertRecordSingle(XLogRecData *rdata, XLogRecPtr fpw_lsn, bool isupgrade);

void ArchiveXlogForForceFinishRedo(XLogReaderState *xlogreader, TermFileData *term_file);
TermFileData GetTermFileDataAndClear(void);
XLogRecPtr mpfl_read_max_flush_lsn();
void mpfl_new_file();
void mpfl_ulink_file();
bool mpfl_pread_file(int fd, void *buf, int32 size, int64 offset);
int ParallelXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
    TimeLineID *readTLI);

#ifdef __aarch64__
static XLogRecPtr XLogInsertRecordGroup(XLogRecData *rdata, XLogRecPtr fpw_lsn);

static void XLogInsertRecordNolock(XLogRecData* rdata, PGPROC* proc, XLogRecPtr StartPos, XLogRecPtr EndPos,
    XLogRecPtr PrevPos, int32 *const currlrc_ptr);
static void ReserveXLogInsertByteLocation(uint32 size, uint32 lastRecordSize, uint64* StartBytePos, uint64* EndBytePos,
    uint64* PrevBytePos, int32 *const currlrc_ptr);
static void CopyXLogRecordToWALForGroup(int write_len, XLogRecData* rdata, XLogRecPtr StartPos, XLogRecPtr EndPos,
    PGPROC* proc, int32 *const currlrc_ptr);

static void WakeUpProc(uint32 wakeidx)
{
    while (wakeidx != INVALID_PGPROCNO) {
        PGPROC *proc = g_instance.proc_base_all_procs[wakeidx];

        wakeidx = pg_atomic_read_u32(&proc->xlogGroupNext);
        pg_atomic_write_u32(&proc->xlogGroupNext, INVALID_PGPROCNO);
        proc->xlogGroupMember = false;
        /* ensure all previous writes are visible before follower continues. */
        pg_memory_barrier();

        if (proc != t_thrd.proc) {
            PGSemaphoreUnlock(&proc->sem);
        }
    }
}

static void XLogInsertRecordGroupLeader(PGPROC *leader, uint64 *end_byte_pos_ptr, int32 *const currlrc_ptr)
{
    uint32 record_size;
    uint64 prev_byte_pos = 0;
    uint64 start_byte_pos = 0;
    uint64 end_byte_pos = 0;
    int32 current_lrc = 0;
    uint64 dirty_page_queue_lsn = 0;

    *leader->xlogGroupRedoRecPtr = t_thrd.shemem_ptr_cxt.XLogCtl->Insert.RedoRecPtr;

    /*
     * In some cases, some xlog records about full page write are not needed to write to buffer,
     * so reserving space for these xlog records is not needed.
     */
    *leader->xlogGroupDoPageWrites = (t_thrd.shemem_ptr_cxt.XLogCtl->Insert.fullPageWrites ||
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites);

    bool flag = (leader->xlogGroupfpw_lsn != InvalidXLogRecPtr) &&
        (leader->xlogGroupfpw_lsn <= *leader->xlogGroupRedoRecPtr) &&
        *leader->xlogGroupDoPageWrites;
    if (unlikely(flag)) {
        leader->xlogGroupReturntRecPtr = InvalidXLogRecPtr;
        leader->xlogGroupIsFPW = true;
    }

    record_size = MAXALIGN(((XLogRecord *)(leader->xlogGrouprdata->data))->xl_tot_len);
    Assert(record_size != 0);

    if (likely(record_size != 0)) {
        ReserveXLogInsertByteLocation(record_size, record_size, &start_byte_pos,
            &end_byte_pos, &prev_byte_pos, &current_lrc);
        dirty_page_queue_lsn = start_byte_pos;
    }

    /* insert the leader WAL first */
    if (unlikely(leader->xlogGroupIsFPW)) {
        leader->xlogGroupIsFPW = false;
    } else {
        XLogInsertRecordNolock(leader->xlogGrouprdata, leader, XLogBytePosToRecPtr(start_byte_pos),
            XLogBytePosToEndRecPtr(start_byte_pos +
                MAXALIGN(((XLogRecord *)(leader->xlogGrouprdata->data))->xl_tot_len)),
            XLogBytePosToRecPtr(prev_byte_pos), &current_lrc);
    }

    if (dirty_page_queue_lsn != 0) {
        update_dirty_page_queue_rec_lsn(XLogBytePosToRecPtr(dirty_page_queue_lsn));
    }

    if (end_byte_pos_ptr != NULL) {
        *end_byte_pos_ptr = end_byte_pos;
    }

    if (currlrc_ptr != NULL) {
        *currlrc_ptr = current_lrc;
    }
}

static void XLogInsertRecordGroupFollowers(PGPROC *leader, const uint32 head, uint64 *end_byte_pos_ptr,
    int32 *const currlrc_ptr)
{
    uint32 nextidx;
    uint32 total_size = 0;
    uint32 record_size = 0;
    PGPROC *follower = NULL;
    uint64 start_byte_pos = 0;
    uint64 end_byte_pos = 0;
    uint64 prev_byte_pos = 0;
    int32 current_lrc = 0;
    uint64 dirty_page_queue_lsn = 0;

    /* Walk the list and update the status of all xloginserts. */
    nextidx = head;
    while (nextidx != (uint32)(leader->pgprocno)) {
        follower = g_instance.proc_base_all_procs[nextidx];
        *follower->xlogGroupRedoRecPtr = t_thrd.shemem_ptr_cxt.XLogCtl->Insert.RedoRecPtr;

        /* 
         * In some cases, some xlog records about full page write are not needed to
         * write to buffer, so reserving space for these xlog records is not needed.
         */
        *follower->xlogGroupDoPageWrites = (t_thrd.shemem_ptr_cxt.XLogCtl->Insert.fullPageWrites ||
            t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites);

        bool flag = (follower->xlogGroupfpw_lsn != InvalidXLogRecPtr) &&
            (follower->xlogGroupfpw_lsn <= *follower->xlogGroupRedoRecPtr) &&
            *follower->xlogGroupDoPageWrites;
        if (unlikely(flag)) {
            follower->xlogGroupReturntRecPtr = InvalidXLogRecPtr;
            follower->xlogGroupIsFPW = true;
            nextidx = pg_atomic_read_u32(&follower->xlogGroupNext);
            continue;
        }

        record_size = MAXALIGN(((XLogRecord *)(follower->xlogGrouprdata->data))->xl_tot_len);
        Assert(record_size != 0);
        /* Calculate total size in the group. */
        total_size += record_size;
        /* Move to next proc in list. */
        nextidx = pg_atomic_read_u32(&follower->xlogGroupNext);
    }

    if (likely(total_size != 0)) {
        ReserveXLogInsertByteLocation(total_size, record_size, &start_byte_pos, &end_byte_pos, &prev_byte_pos,
            &current_lrc);
        dirty_page_queue_lsn = start_byte_pos;
    }

    nextidx = head;
    /* The lead thread insert xlog records in the group one by one. */
    while (nextidx != (uint32)(leader->pgprocno)) {
        follower = g_instance.proc_base_all_procs[nextidx];

        if (unlikely(follower->xlogGroupIsFPW)) {
            nextidx = pg_atomic_read_u32(&follower->xlogGroupNext);
            follower->xlogGroupIsFPW = false;
            continue;
        }
        XLogInsertRecordNolock(follower->xlogGrouprdata, follower, XLogBytePosToRecPtr(start_byte_pos),
            XLogBytePosToEndRecPtr(start_byte_pos +
                MAXALIGN(((XLogRecord *)(follower->xlogGrouprdata->data))->xl_tot_len)),
            XLogBytePosToRecPtr(prev_byte_pos), &current_lrc);
        prev_byte_pos = start_byte_pos;
        start_byte_pos += MAXALIGN(((XLogRecord *)(follower->xlogGrouprdata->data))->xl_tot_len);
        /* Move to next proc in list. */
        nextidx = pg_atomic_read_u32(&follower->xlogGroupNext);
    }

    if (dirty_page_queue_lsn != 0) {
        update_dirty_page_queue_rec_lsn(XLogBytePosToRecPtr(dirty_page_queue_lsn));
    }

    if (end_byte_pos_ptr != NULL) {
        *end_byte_pos_ptr = end_byte_pos;
    }
    if (currlrc_ptr != NULL) {
        *currlrc_ptr = current_lrc;
    }
}

static void XLogReportCopyLocation(const int32 current_lrc, const uint64 end_byte_pos)
{
    bool stop = true;
    uint32 current_entry = current_lrc & (WAL_INSERT_STATUS_ENTRIES - 1);
    volatile WALInsertStatusEntry *status_entry_ptr = 
        &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(current_entry)];
    
    if (g_instance.wal_cxt.isWalWriterUp != 0) {
        do {
            stop = (status_entry_ptr->LRC == current_lrc &&
                (status_entry_ptr->status == WAL_NOT_COPIED));
        } while (!stop);
    } else {
        while (status_entry_ptr->LRC != current_lrc) {
           XLogSelfFlush();
        }
     }

    (void)pg_atomic_exchange_u64(&status_entry_ptr->endLSN, XLogBytePosToEndRecPtr(end_byte_pos));
    pg_memory_barrier();
    status_entry_ptr->status = WAL_COPIED;
}

/*
 * @Description: Insert an XLOG record represented by an already-constructed chain of data
 * chunks. some threads in one xlog insert lock can be inserted in a group.
 * @in rdata: the xlog data.
 * @in fpw_lsn: the LSN of full page write.
 * @return: the LSN of the insert end position.
 */
static XLogRecPtr XLogInsertRecordGroup(XLogRecData *rdata, XLogRecPtr fpw_lsn)
{
    PGPROC *proc = t_thrd.proc;
    uint32 head = 0;
    uint32 nextidx = 0;
    int32 current_lrc_leader = 0;
    int32 current_lrc = 0;
    uint64 end_byte_pos_leader = 0;
    uint64 end_byte_pos = 0;

    /* cross-check on whether we should be here or not */
    if (unlikely(!XLogInsertAllowed())) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("cannot make new WAL entries during recovery")));
    }

    START_CRIT_SECTION();

    /* Add ourselves to the list of processes needing a group xlog status update. */
    proc->xlogGroupMember = true;
    proc->xlogGrouprdata = rdata;
    proc->xlogGroupfpw_lsn = fpw_lsn;
    proc->xlogGroupProcLastRecPtr = &t_thrd.xlog_cxt.ProcLastRecPtr;
    proc->xlogGroupXactLastRecEnd = &t_thrd.xlog_cxt.XactLastRecEnd;
    proc->xlogGroupCurrentTransactionState = GetCurrentTransactionState();
    proc->xlogGroupRedoRecPtr = &t_thrd.xlog_cxt.RedoRecPtr;
    proc->xlogGroupLogwrtResult = t_thrd.xlog_cxt.LogwrtResult;
    proc->xlogGroupTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    proc->xlogGroupDoPageWrites = &t_thrd.xlog_cxt.doPageWrites;

    int groupnum = (proc->pgprocno / g_instance.shmem_cxt.numaNodeNum) % (g_instance.wal_cxt.num_locks_in_group);

    nextidx = pg_atomic_read_u32(&t_thrd.shemem_ptr_cxt.LocalGroupWALInsertLocks[groupnum].l.xlogGroupFirst);
    while (true) {
        pg_atomic_write_u32(&proc->xlogGroupNext, nextidx);

        /* Ensure all previous writes are visible before follower continues. */
        pg_write_barrier();

        if (pg_atomic_compare_exchange_u32(&t_thrd.shemem_ptr_cxt.LocalGroupWALInsertLocks[groupnum].l.xlogGroupFirst,
            &nextidx, (uint32)proc->pgprocno)) {
            break;
        }
    }

    /*
     * If the list was not empty, the leader will insert all xlog in the same xlog insert slot.
     * It is impossible to have followers without a leader because the first process that
     * has added itself to the list will always have nextidx as INVALID_PGPROCNO.
     */
    if (nextidx != INVALID_PGPROCNO) {
        int extraWaits = 0;

        /* Sleep until the leader updates our XLOG insert status. */
        for (;;) {
            /* acts as a read barrier */
            PGSemaphoreLock(&proc->sem, false);
            /* acts as a read barrier */
            pg_memory_barrier();
            if (!proc->xlogGroupMember) {
                break;
            }
            extraWaits++;
        }

        Assert(pg_atomic_read_u32(&proc->xlogGroupNext) == INVALID_PGPROCNO);

        /* Fix semaphore count for any absorbed wakeups */
        while (extraWaits-- > 0) {
            PGSemaphoreUnlock(&proc->sem);
        }
        END_CRIT_SECTION();
        return proc->xlogGroupReturntRecPtr;
    }

    /* I am the leader; Let me insert my own WAL first. */
    PGPROC *leader = t_thrd.proc;
    XLogInsertRecordGroupLeader(leader, &end_byte_pos_leader, &current_lrc_leader);
    XLogReportCopyLocation(current_lrc_leader, end_byte_pos_leader);

    /*
     * Clear the list of processes waiting for group xlog insert, saving a pointer to the head of the list.
     * Trying to pop elements one at a time could lead to an ABA problem.
     */
    head = pg_atomic_exchange_u32(&t_thrd.shemem_ptr_cxt.LocalGroupWALInsertLocks[groupnum].l.xlogGroupFirst,
                                  INVALID_PGPROCNO);

    bool has_follower = (head != (uint32)(leader->pgprocno));
    if (has_follower) {
        /* I have at least one follower and I will next insert their WAL for them. */
        XLogInsertRecordGroupFollowers(leader, head, &end_byte_pos, &current_lrc);
    }

    /*
     * Wake all waiting threads up.
     */
    WakeUpProc(head);

    /*
     * Examine the entry's LRC and status all together to make sure the writer already
     * finishes updating this entry and this entry is now owned by this thread
     *
     * If the status entry is owned by this thread, update it with the copied xlog record's
     * end LSN and set the WAL copy status to WAL_COPIED to signal copy completion as well
     */
    if (has_follower) {
        XLogReportCopyLocation(current_lrc, end_byte_pos);
    }

    if (g_instance.wal_cxt.isWalWriterSleeping) {
        pthread_mutex_lock(&g_instance.wal_cxt.criticalEntryMutex);
        pthread_cond_signal(&g_instance.wal_cxt.criticalEntryCV);
        pthread_mutex_unlock(&g_instance.wal_cxt.criticalEntryMutex);
    }

    END_CRIT_SECTION();

    return proc->xlogGroupReturntRecPtr;
}

/*
 * @Description: Insert an XLOG record represented by an already-constructed chain of data
 * chunks. Becaus of the group insert mode, the xlog insert lock is not needed.
 * @in rdata: the xlog data.
 * @in proc: the waiting proc.
 * @in StartPos: the start LSN postion.
 * @in EndPos: the end LSN postion.
 * @in PrevPos: the prev LSN postion.
 */
static void XLogInsertRecordNolock(XLogRecData *rdata, PGPROC *proc, XLogRecPtr StartPos, XLogRecPtr EndPos,
                                   XLogRecPtr PrevPos, int32 *const currlrc_ptr)
{
    XLogRecord *rechdr = (XLogRecord *)rdata->data;
    rechdr->xl_prev = PrevPos;
    /* pg_crc32c as same as pg_crc32 */
    pg_crc32c rdata_crc;

    Assert(XLByteLT(PrevPos, StartPos));
    /* we assume that all of the record header is in the first chunk */
    Assert(rdata->len >= SizeOfXLogRecord);

    START_CRIT_SECTION();

    /* Now that xl_prev has been filled in, calculate CRC of the record header. */
    rdata_crc = ((XLogRecord *)rechdr)->xl_crc;

    COMP_CRC32C(rdata_crc, (XLogRecord *)rechdr, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(rdata_crc); /* FIN_CRC32C as same as FIN_CRC32 */
    ((XLogRecord *)rechdr)->xl_crc = rdata_crc;

    /*
     * All the record data, including the header, is now ready to be
     * inserted. Copy the record in the space reserved.
     */
    CopyXLogRecordToWALForGroup(rechdr->xl_tot_len, rdata, StartPos, EndPos, proc, currlrc_ptr);

    CopyTransactionIdLoggedIfAny((TransactionState)proc->xlogGroupCurrentTransactionState);
    END_CRIT_SECTION();

    /* Update shared LogwrtRqst.Write, if we crossed page boundary. */
    if (unlikely(StartPos / XLOG_BLCKSZ != EndPos / XLOG_BLCKSZ)) {
        SpinLockAcquire(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
        /* advance global request to include new block(s) */
        if (t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write < EndPos) {
            t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write = EndPos;
        }
        /* update local result copy while I have the chance */
        *((XLogwrtResult *)proc->xlogGroupLogwrtResult) = t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult;
        SpinLockRelease(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
    }

#ifdef WAL_DEBUG
    if (u_sess->attr.attr_storage.XLOG_DEBUG) {
        StringInfoData buf;

        initStringInfo(&buf);
        appendStringInfo(&buf, "INSERT @ %X/%X: ", (uint32)(EndPos >> 32), (uint32)EndPos);
        xlog_outrec(&buf, rechdr);
        if (rdata->data != NULL) {
            appendStringInfo(&buf, " - ");
            RmgrTable[rechdr->xl_rmid].rm_desc(&buf, rechdr->xl_info, rdata->data);
        }
        ereport(LOG, (errmsg("%s", buf.data)));
        pfree_ext(buf.data);
    }
#endif

    /* Update our global variables */
    *proc->xlogGroupProcLastRecPtr = StartPos;
    *proc->xlogGroupXactLastRecEnd = EndPos;
    proc->xlogGroupReturntRecPtr = EndPos;

    return;
}

/*
 * @Description: Reserves the right amount of space for a given size from the WAL.
 * already-reserved area in the WAL. The StartBytePos, EndBytePos and PrevBytePos
 * are stored as "usable byte positions" rather than XLogRecPtrs (see XLogBytePosToRecPtr()).
 * @in size: the size for right amount of space.
 * @in lastRecordSize: the last record size in the group.
 * @out StartBytePos: the start position of the WAL.
 * @out EndBytePos: the end position of the WAL.
 * @out PrevBytePos: the previous position of the WAL.
 */
static void ReserveXLogInsertByteLocation(uint32 size, uint32 lastRecordSize, uint64* StartBytePos, uint64* EndBytePos,
    uint64* PrevBytePos, int32 *const currlrc_ptr)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;

    size = MAXALIGN(size);

    /* All (non xlog-switch) records should contain data. */
    Assert(size > SizeOfXLogRecord);

    /*
     * The duration the spinlock needs to be held is minimized by minimizing
     * the calculations that have to be done while holding the lock. The
     * current tip of reserved WAL is kept in CurrBytePos, as a byte position
     * that only counts "usable" bytes in WAL, that is, it excludes all WAL
     * page headers. The mapping between "usable" byte positions and physical
     * positions (XLogRecPtrs) can be done outside the locked region, and
     * because the usable byte position doesn't include any headers, reserving
     * X bytes from WAL is almost as simple as "CurrBytePos += X".
     */
#if defined(__x86_64__) || defined(__aarch64__)
    union Union128 compare;
    union Union128 exchange;
    union Union128 current;

    compare.value = atomic_compare_and_swap_u128((volatile uint128_u *)&Insert->CurrBytePos);

    Assert(sizeof(Insert->CurrBytePos) == SIZE_OF_UINT64);
    Assert(sizeof(Insert->PrevByteSize) == SIZE_OF_UINT32);
    Assert(sizeof(Insert->CurrLRC) == SIZE_OF_UINT32);
loop:
    /*
     * |CurrBytePos			|bytePos	  | CurrLRC
     * ------------------------+------------ +-----------
     * |64 bits				|32 bits	  |32 bits
     */

    /*
     * Suspend WAL insert threads when currlrc equals WAL_COPY_SUSPEND
     */
    if (unlikely(compare.struct128.LRC == WAL_COPY_SUSPEND)) {
        compare.value = atomic_compare_and_swap_u128((volatile uint128_u *)&Insert->CurrBytePos);
        goto loop;
    }

    /* increment currlrc by 1 and store it back to the global LRC for the next record */

    exchange.struct128.currentBytePos = compare.struct128.currentBytePos + size;
    exchange.struct128.byteSize = lastRecordSize;
    exchange.struct128.LRC = (compare.struct128.LRC + 1) & 0x7FFFFFFF;

    current.value = atomic_compare_and_swap_u128((volatile uint128_u *)&Insert->CurrBytePos, compare.value,
        exchange.value);

    if (!UINT128_IS_EQUAL(compare.value, current.value)) {
        UINT128_COPY(compare.value, current.value);
        goto loop;
    }
#else
    uint64 startbytepos;
    uint64 endbytepos;
    uint64 prevbytepos;

    SpinLockAcquire(&Insert->insertpos_lck);

    startbytepos = Insert->CurrBytePos;
    endbytepos = startbytepos + size;
    prevbytepos = Insert->PrevBytePos;
    Insert->CurrBytePos = endbytepos;
    Insert->PrevBytePos = endbytepos - lastRecordSize;

    SpinLockRelease(&Insert->insertpos_lck);
#endif /* __x86_64__ || __aarch64__ */
    *currlrc_ptr = compare.struct128.LRC;
    *StartBytePos = compare.struct128.currentBytePos;
    *EndBytePos = exchange.struct128.currentBytePos;
    *PrevBytePos = compare.struct128.currentBytePos - compare.struct128.byteSize;
}

/*
 * @Description: In xlog group insert mode, copy a WAL record to an
 * already-reserved area in the WAL.
 * @in write_len: the length of xlog data.
 * @in rdata: the xlog data.
 * @in StartPos: the start LSN.
 * @in EndPos: the end LSN.
 * @in proc: the waiting proc.
 */
static void CopyXLogRecordToWALForGroup(int write_len, XLogRecData *rdata, XLogRecPtr StartPos, XLogRecPtr EndPos,
                                        PGPROC *proc, int32 *const currlrc_ptr)
{
    char *currpos = NULL;
    uint32 freespace;
    int written;
    XLogRecPtr CurrPos;
    XLogPageHeader pagehdr;
    errno_t errorno = EOK;

    /*
     * The following 2 variables are used to handle large records
     */
    uint32 startBufPageIdx = XLogRecPtrToBufIdx(StartPos);
    int numHitsOnStartPage = 1;

    /* Get a pointer to the right place in the right WAL buffer to start inserting to. */
    CurrPos = StartPos;
    currpos = GetXLogBuffer<true>(CurrPos, proc);
    freespace = INSERT_FREESPACE(CurrPos);

    /*
     * there should be enough space for at least the first field (xl_tot_len)
     * on this page.
     */
    Assert(freespace >= sizeof(uint32));

    /* Copy record data */
    written = 0;
    while (rdata != NULL) {
        char *rdata_data = rdata->data;
        uint32 rdata_len = rdata->len;

        while (rdata_len > freespace) {
            /* Write what fits on this page, and continue on the next page. */
            Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || freespace == 0);
            errorno = memcpy_s(currpos, SECUREC_STRING_MAX_LEN, rdata_data, freespace);
            securec_check(errorno, "", "");

            rdata_data += freespace;
            rdata_len -= freespace;
            written += freespace;
            CurrPos += freespace;

            /*
             * We hit the page we start from within the same copy. We must initiate
             * a flush without updating WAL copy status entry or we will be stuck
             */
            if (unlikely(XLogRecPtrToBufIdx(CurrPos) == startBufPageIdx)) {
                numHitsOnStartPage++;
                XLogSelfFlushWithoutStatus(numHitsOnStartPage, CurrPos, *currlrc_ptr);
            }

            /*
             * Get pointer to beginning of next page, and set the xlp_rem_len
             * in the page header. Set XLP_FIRST_IS_CONTRECORD.
             *
             * It's safe to set the contrecord flag and xlp_rem_len without a
             * lock on the page. All the other flags were already set when the
             * page was initialized, in AdvanceXLInsertBuffer, and we're the
             * only backend that needs to set the contrecord flag.
             */
            currpos = GetXLogBuffer<true>(CurrPos, proc);
            pagehdr = (XLogPageHeader)currpos;
            pagehdr->xlp_rem_len = write_len - written;
            pagehdr->xlp_info |= XLP_FIRST_IS_CONTRECORD;

            /* skip over the page header */
            if (CurrPos % XLogSegSize == 0) {
                CurrPos += SizeOfXLogLongPHD;
                currpos += SizeOfXLogLongPHD;
            } else {
                CurrPos += SizeOfXLogShortPHD;
                currpos += SizeOfXLogShortPHD;
            }
            freespace = INSERT_FREESPACE(CurrPos);
        }

        Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || rdata_len == 0);
        errorno = memcpy_s(currpos, SECUREC_STRING_MAX_LEN, rdata_data, rdata_len);
        securec_check(errorno, "", "");

        currpos += rdata_len;
        CurrPos += rdata_len;
        freespace -= rdata_len;
        written += rdata_len;

        rdata = rdata->next;
    }
    Assert(written == write_len);

    /* Align the end position, so that the next record starts aligned */
    CurrPos = MAXALIGN(CurrPos);

    if (SECUREC_UNLIKELY(CurrPos != EndPos)) {
        ereport(PANIC, (errmsg("space reserved for WAL record does not match what was written")));
    }

    pgstat_report_waitevent_count(WAIT_EVENT_WAL_BUFFER_ACCESS);
}

#endif

inline void SetMinRecoverPointForStats(XLogRecPtr lsn)
{
    if (XLByteLT(g_instance.comm_cxt.predo_cxt.redoPf.min_recovery_point, lsn)) {
        g_instance.comm_cxt.predo_cxt.redoPf.min_recovery_point = lsn;
    }
}

/*
 * @Description: Insert an XLOG record represented by an already-constructed chain of data
 * chunks. The action is the same as the function XLogInsertRecord.
 * @in rdata: the xlog data.
 * @in fpw_lsn: the LSN of full page write.
 * @in isupgrade: whether this xlog insert is for upgrade.
 * @return: the LSN of the insert end position.
 */
static XLogRecPtr XLogInsertRecordSingle(XLogRecData *rdata, XLogRecPtr fpw_lsn, bool isupgrade)
{
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    XLogRecord *rechdr = (XLogRecord *)rdata->data;
    pg_crc32c rdata_crc; /* pg_crc32c as same as pg_crc32 */
    bool inserted = false;
    bool stop = true;
    XLogRecPtr StartPos = InvalidXLogRecPtr;
    XLogRecPtr EndPos = InvalidXLogRecPtr;
    int32 currlrc = 0;

    bool isLogSwitch =
        ((isupgrade ? ((XLogRecordOld *)rechdr)->xl_rmid : ((XLogRecord *)rechdr)->xl_rmid) == RM_XLOG_ID &&
         (isupgrade ? ((XLogRecordOld *)rechdr)->xl_info : ((XLogRecord *)rechdr)->xl_info) == XLOG_SWITCH);

    /* we assume that all of the record header is in the first chunk */
    Assert(rdata->len >= (isupgrade ? SizeOfXLogRecordOld : SizeOfXLogRecord));

    /* cross-check on whether we should be here or not */
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("cannot make new WAL entries during recovery")));
    }

    /* ----------
     *
     * We have now done all the preparatory work we can without holding a
     * lock or modifying shared state. From here on, inserting the new WAL
     * record to the shared WAL buffer cache is a two-step process:
     *
     * 1. Reserve the right amount of space from the WAL. The current head of
     *	  reserved space is kept in Insert->CurrBytePos, and is protected by
     *	  insertpos_lck.
     *
     * 2. Copy the record to the reserved WAL space. This involves finding the
     *	  correct WAL buffer containing the reserved space, and copying the
     *	  record in place. This can be done concurrently in multiple processes.
     *
     * To keep track of which insertions are still in-progress, each concurrent
     * inserter acquires an insertion lock. In addition to just indicating that
     * an insertion is in progress, the lock tells others how far the inserter
     * has progressed. There is a small fixed number of insertion locks,
     * determined by num_xloginsert_locks. When an inserter crosses a page
     * boundary, it updates the value stored in the lock to the how far it has
     * inserted, to allow the previous buffer to be flushed.
     *
     * Holding onto an insertion lock also protects RedoRecPtr and
     * fullPageWrites from changing until the insertion is finished.
     *
     * Step 2 can usually be done completely in parallel. If the required WAL
     * page is not initialized yet, you have to grab WALBufMappingLock to
     * initialize it, but the WAL writer tries to do that ahead of insertions
     * to avoid that from happening in the critical path.
     *
     * ----------
     */
    START_CRIT_SECTION();
    if (isLogSwitch) {
        StartSuspendWalInsert(&currlrc);
    }

    /*
     * Check to see if my copy of RedoRecPtr or doPageWrites is out of date.
     * If so, may have to go back and have the caller recompute everything.
     * This can only happen just after a checkpoint, so it's better to be slow
     * in this case and fast otherwise.
     *
     * If we aren't doing full-page writes then RedoRecPtr doesn't actually
     * affect the contents of the XLOG record, so we'll update our local copy
     * but not force a recomputation.  (If doPageWrites was just turned off,
     * we could recompute the record without full pages, but we choose not to
     * bother.)
     */
    if (t_thrd.xlog_cxt.RedoRecPtr != Insert->RedoRecPtr) {
        Assert(t_thrd.xlog_cxt.RedoRecPtr < Insert->RedoRecPtr);
        t_thrd.xlog_cxt.RedoRecPtr = Insert->RedoRecPtr;
    }
    t_thrd.xlog_cxt.doPageWrites = (Insert->fullPageWrites || Insert->forcePageWrites);

    if (fpw_lsn != InvalidXLogRecPtr && fpw_lsn <= t_thrd.xlog_cxt.RedoRecPtr && t_thrd.xlog_cxt.doPageWrites) {
        /* Oops, some buffer now needs to be backed up that the caller didn't back up.  Start over. */
        END_CRIT_SECTION();
        return InvalidXLogRecPtr;
    }

    /*
     * Reserve space for the record in the WAL. This also sets the xl_prev
     * pointer.
     */
    if (isLogSwitch) {
        XLogRecPtr tmp_xl_prev = InvalidXLogRecPtr;
        inserted = ReserveXLogSwitch(&StartPos, &EndPos, &tmp_xl_prev, isupgrade);
        if (isupgrade) {
            ((XLogRecordOld *)rechdr)->xl_prev.xlogid = (uint32)(tmp_xl_prev >> 32);
            ((XLogRecordOld *)rechdr)->xl_prev.xrecoff = (uint32)tmp_xl_prev;
            ereport(LOG, (errmsg("A XLOG_SWITCH is started at %X/%X, ended at %X/%X", (uint32)(StartPos >> 32),
                                 (uint32)StartPos, (uint32)(EndPos >> 32), (uint32)EndPos)));
        } else {
            rechdr->xl_prev = tmp_xl_prev;
        }
    } else {
        ReserveXLogInsertLocation(rechdr->xl_tot_len, &StartPos, &EndPos, &rechdr->xl_prev, &currlrc);
        inserted = true;
    }

    if (inserted) {
        uint32 current_entry = currlrc & (WAL_INSERT_STATUS_ENTRIES - 1);
        volatile WALInsertStatusEntry *status_entry_ptr =
            &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(current_entry)];

        /* Now that xl_prev has been filled in, calculate CRC of the record header. */
        rdata_crc = (isupgrade ? ((XLogRecordOld *)rechdr)->xl_crc : ((XLogRecord *)rechdr)->xl_crc);

        if (isupgrade) {
            /* using PG's CRC32 */
            COMP_CRC32(rdata_crc, (XLogRecordOld *)rechdr, offsetof(XLogRecordOld, xl_crc));
        } else {
            /* using CRC32C */
            COMP_CRC32C(rdata_crc, (XLogRecord *)rechdr, offsetof(XLogRecord, xl_crc));
        }

        FIN_CRC32C(rdata_crc); /* FIN_CRC32C as same as FIN_CRC32 */
        (isupgrade ? ((XLogRecordOld *)rechdr)->xl_crc : ((XLogRecord *)rechdr)->xl_crc) = rdata_crc;

        /*
         * All the record data, including the header, is now ready to be
         * inserted. Copy the record in the space reserved.
         */
        CopyXLogRecordToWAL(rechdr->xl_tot_len, isLogSwitch, rdata, StartPos, EndPos, &currlrc);

        /*
         * Examine the entry's LRC and status all together to make sure the writer already
         * finishes updating this entry and this entry is now owned by this thread
         *
         * If the status entry is owned by this thread, update it with the copied xlog record's
         * end LSN and set the WAL copy status to WAL_COPIED to signal copy completion as well
         *
         * XLog switch already handles its flush in CopyXLogRecordToWAL so here we only
         * update the WAL copy status for the non switch records
         */
        if (!isLogSwitch) {
            if (g_instance.wal_cxt.isWalWriterUp) {
                do {
                    stop = (status_entry_ptr->LRC == currlrc && (status_entry_ptr->status == WAL_NOT_COPIED));
                } while (!stop);
            } else {
                while (status_entry_ptr->LRC != currlrc) {
                    XLogSelfFlush();
                }
            }

            /*
             * We now own this status entry
             * Update status_entry_ptr->endLSN with an atomic call to prevent half updated value being
             * read by the writer. The writer should use atomic read
             * The 2nd CAS call should succeed in updating the value since this thread is the only
             * one updating this field at this moment
             */
            (void)pg_atomic_exchange_u64(&status_entry_ptr->endLSN, EndPos);

            pg_memory_barrier();

            status_entry_ptr->status = WAL_COPIED;
        }

        if (g_instance.wal_cxt.isWalWriterSleeping) {
            pthread_mutex_lock(&g_instance.wal_cxt.criticalEntryMutex);
            pthread_cond_signal(&g_instance.wal_cxt.criticalEntryCV);
            pthread_mutex_unlock(&g_instance.wal_cxt.criticalEntryMutex);
        }
    } else {
        /*
         * This was an xlog-switch record, but the current insert location was
         * already exactly at the beginning of a segment, so there was no need
         * to do anything.
         */
    }

    /* Done! Let others know that we're finished. */
    if (isLogSwitch) {
        /*
         * Swap the recorded LRC back into the shared memory location after adding 1
         * for the next WAL insert thread to pick up
         */
        StopSuspendWalInsert(currlrc);
    }
    MarkCurrentTransactionIdLoggedIfAny();
    END_CRIT_SECTION();

    /*
     * Use the StartPos update the dirty page queue reclsn, the dirty page push to
     * dirty page queue use this reclsn, then record XLog, the dirty page reclsn is
     * smaller than the redo lsn.
     */
    update_dirty_page_queue_rec_lsn(StartPos);

    /*
     * Update shared LogwrtRqst.Write, if we crossed page boundary.
     */
    if (StartPos / XLOG_BLCKSZ != EndPos / XLOG_BLCKSZ) {
        SpinLockAcquire(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
        /* advance global request to include new block(s) */
        if (t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write < EndPos) {
            t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write = EndPos;
        }
        /* update local result copy while I have the chance */
        *t_thrd.xlog_cxt.LogwrtResult = t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult;
        SpinLockRelease(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
    }

    /*
     * If this was an XLOG_SWITCH record, flush the record and the empty
     * padding space that fills the rest of the segment, and perform
     * end-of-segment actions (eg, notifying archiver).
     */
    if (isLogSwitch) {
        TRACE_POSTGRESQL_XLOG_SWITCH();
        XLogWaitFlush(EndPos);

        /*
         * Even though we reserved the rest of the segment for us, which is
         * reflected in EndPos, we return a pointer to just the end of the
         * xlog-switch record.
         */
        if (inserted) {
            EndPos = StartPos + (isupgrade ? MAXALIGN(SizeOfXLogRecordOld) : MAXALIGN(SizeOfXLogRecord));
            if (StartPos / XLOG_BLCKSZ != EndPos / XLOG_BLCKSZ) {
                if (EndPos % XLOG_SEG_SIZE == EndPos % XLOG_BLCKSZ) {
                    EndPos += SizeOfXLogLongPHD;
                } else {
                    EndPos += SizeOfXLogShortPHD;
                }
            }
        }
    }

#ifdef WAL_DEBUG
    if (u_sess->attr.attr_storage.XLOG_DEBUG) {
        StringInfoData buf;

        initStringInfo(&buf);
        appendStringInfo(&buf, "INSERT @ %X/%X: ", (uint32)(EndPos >> 32), (uint32)EndPos);
        xlog_outrec(&buf, rechdr);
        if (rdata->data != NULL) {
            appendStringInfo(&buf, " - ");
            RmgrTable[rechdr->xl_rmid].rm_desc(&buf, rechdr->xl_info, rdata->data);
        }
        ereport(LOG, (errmsg("%s", buf.data)));
        pfree_ext(buf.data);
    }
#endif

    // Update our global variables
    t_thrd.xlog_cxt.ProcLastRecPtr = StartPos;
    t_thrd.xlog_cxt.XactLastRecEnd = EndPos;

    return EndPos;
}

/*
 * Insert an XLOG record represented by an already-constructed chain of data
 * chunks.  This is a low-level routine; to construct the WAL record header
 * and data, use the higher-level routines in xloginsert.cpp.
 *
 * If 'fpw_lsn' is valid, it is the oldest LSN among the pages that this
 * WAL record applies to, that were not included in the record as full page
 * images.  If fpw_lsn >= RedoRecPtr, the function does not perform the
 * insertion and returns InvalidXLogRecPtr.  The caller can then recalculate
 * which pages need a full-page image, and retry.  If fpw_lsn is invalid, the
 * record is always inserted.
 *
 * The first XLogRecData in the chain must be for the record header, and its
 * data must be MAXALIGNed.  XLogInsertRecord fills in the xl_prev and
 * xl_crc fields in the header, the rest of the header must already be filled
 * by the caller.
 *
 * Returns XLOG pointer to end of record (beginning of next record).
 * This can be used as LSN for data pages affected by the logged action.
 * (LSN is the XLOG point up to which the XLOG must be flushed to disk
 * before the data page can be written out.  This implements the basic
 * WAL rule "write the log before the data".)
 *
 * @in rdata: the xlog data.
 * @in fpw_lsn: the LSN of full page write.
 * @in isupgrade: whether this xlog insert is for upgrade.
 * @return: the LSN of the insert end position.
 */
XLogRecPtr XLogInsertRecord(XLogRecData *rdata, XLogRecPtr fpw_lsn, bool isupgrade)
{
#ifdef __aarch64__
    /*
     * In ARM architecture, insert an XLOG record represented by an already-constructed chain of data
     * chunks. If the record is LogSwitch or upgrade data, insert the record in single mode, and in other
     * situation, insert the record in group mode.
     */
    XLogRecord *rechdr = (XLogRecord *)rdata->data;
    bool isLogSwitch =
        ((isupgrade ? ((XLogRecordOld *)rechdr)->xl_rmid : ((XLogRecord *)rechdr)->xl_rmid) == RM_XLOG_ID &&
         (isupgrade ? ((XLogRecordOld *)rechdr)->xl_info : ((XLogRecord *)rechdr)->xl_info) == XLOG_SWITCH);
    if (isLogSwitch || isupgrade) {
        return XLogInsertRecordSingle(rdata, fpw_lsn, isupgrade);
    } else {
        return XLogInsertRecordGroup(rdata, fpw_lsn);
    }
#else
    return XLogInsertRecordSingle(rdata, fpw_lsn, isupgrade);
#endif /* __aarch64__ */
}

/*
 * Reserves the right amount of space for a record of given size from the WAL.
 * *StartPos is set to the beginning of the reserved section, *EndPos to
 * its end+1. *PrevPtr is set to the beginning of the previous record; it is
 * used to set the xl_prev of this record.
 *
 * This is the performance critical part of XLogInsert that must be serialized
 * across backends. The rest can happen mostly in parallel. Try to keep this
 * section as short as possible, insertpos_lck can be heavily contended on a
 * busy system.
 *
 * NB: The space calculation here must match the code in CopyXLogRecordToWAL,
 * where we actually copy the record to the reserved space.
 */
static void ReserveXLogInsertLocation(uint32 size, XLogRecPtr* StartPos, XLogRecPtr* EndPos, XLogRecPtr* PrevPtr,
    int32 *const currlrc_ptr)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;

    size = MAXALIGN(size);

    /* All (non xlog-switch) records should contain data. */
    Assert(size > SizeOfXLogRecord);

    /*
     * The duration the spinlock needs to be held is minimized by minimizing
     * the calculations that have to be done while holding the lock. The
     * current tip of reserved WAL is kept in CurrBytePos, as a byte position
     * that only counts "usable" bytes in WAL, that is, it excludes all WAL
     * page headers. The mapping between "usable" byte positions and physical
     * positions (XLogRecPtrs) can be done outside the locked region, and
     * because the usable byte position doesn't include any headers, reserving
     * X bytes from WAL is almost as simple as "CurrBytePos += X".
     */
#if defined(__x86_64__) || defined(__aarch64__)
    union Union128 compare;
    union Union128 exchange;
    union Union128 current;

    compare.value = atomic_compare_and_swap_u128((uint128_u *)&Insert->CurrBytePos);
    Assert(sizeof(Insert->CurrBytePos) == SIZE_OF_UINT64);
    Assert(sizeof(Insert->PrevByteSize) == SIZE_OF_UINT32);
    Assert(sizeof(Insert->CurrLRC) == SIZE_OF_UINT32);

loop1:
    /*
     * |CurrBytePos 		   |bytePos 	 | CurrLRC
     * ------------------------+------------ +-----------
     * |64 bits                |32 bits      |32 bits
     */

    /*
     * Suspend WAL insert threads when currlrc equals WAL_COPY_SUSPEND
     */
    if (unlikely(compare.struct128.LRC == WAL_COPY_SUSPEND)) {
        compare.value = atomic_compare_and_swap_u128((volatile uint128_u *)&Insert->CurrBytePos);
        pg_usleep(1);
        goto loop1;
    }

    /* increment currlrc by 1 and store it back to the global LRC for the next record */

    exchange.struct128.currentBytePos = compare.struct128.currentBytePos + size;
    exchange.struct128.byteSize = size;
    exchange.struct128.LRC = (compare.struct128.LRC + 1) & 0x7FFFFFFF;

    current.value = atomic_compare_and_swap_u128((volatile uint128_u *)&Insert->CurrBytePos, compare.value,
        exchange.value);
    if (!UINT128_IS_EQUAL(compare.value, current.value)) {
        UINT128_COPY(compare.value, current.value);
        goto loop1;
    }

#else
    uint64 startbytepos;
    uint64 endbytepos;
    uint64 prevbytepos;

    SpinLockAcquire(&Insert->insertpos_lck);

    startbytepos = Insert->CurrBytePos;
    prevbytepos = Insert->PrevBytePos;
    endbytepos = startbytepos + size;
    Insert->CurrBytePos = endbytepos;
    Insert->PrevBytePos = startbytepos;

    SpinLockRelease(&Insert->insertpos_lck);
#endif /* __x86_64__|| __aarch64__ */
    *currlrc_ptr = compare.struct128.LRC;
    *StartPos = XLogBytePosToRecPtr(compare.struct128.currentBytePos);
    *EndPos = XLogBytePosToEndRecPtr(exchange.struct128.currentBytePos);
    *PrevPtr = XLogBytePosToRecPtr(compare.struct128.currentBytePos - compare.struct128.byteSize);
}

/*
 * Like ReserveXLogInsertLocation(), but for an xlog-switch record.
 *
 * A log-switch record is handled slightly differently. The rest of the
 * segment will be reserved for this insertion, as indicated by the returned
 * *EndPos value. However, if we are already at the beginning of the current
 * segment, *StartPos and *EndPos are set to the current location without
 * reserving any space, and the function returns false.
 */
static bool ReserveXLogSwitch(XLogRecPtr *StartPos, XLogRecPtr *EndPos, XLogRecPtr *PrevPtr, bool isupgrade)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint64 startbytepos;
    uint64 endbytepos;
    uint32 prevbytesize;
    int32 currlrc;
    uint32 size = isupgrade ? MAXALIGN(SizeOfXLogRecordOld) : MAXALIGN(SizeOfXLogRecord);
    XLogRecPtr ptr;
    uint32 segleft;
    uint32 freespace;

    /*
     * These calculations are a bit heavy-weight to be done while holding a
     * spinlock, but since we're holding all the WAL insertion locks, there
     * are no other inserters competing for it. GetXLogInsertRecPtr() does
     * compete for it, but that's not called very frequently.
     */
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u exchange;
    uint128_u current;
    uint128_u compare = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos);

loop:
    startbytepos = compare.u64[0];

    if (isupgrade) {
        ptr = XLogBytePosToRecPtr(startbytepos);
        freespace = INSERT_FREESPACE(ptr);

        if (freespace < SizeOfXLogRecordOld) {
            ereport(LOG, (errmsg("The switch xlog need not be inserted in upgrade "
                                 "if the position is in the last 24 Bytes of xlog page.")));
            startbytepos += (XLOG_BLCKSZ - (ptr % XLOG_BLCKSZ));
            exchange.u64[0] = startbytepos;
            exchange.u64[1] = compare.u64[1];

            current = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos, compare, exchange);
            if (!UINT128_IS_EQUAL(compare, current)) {
                UINT128_COPY(compare, current);
                goto loop;
            }
            *EndPos = *StartPos = ptr;
            return false;
        }
    }

    ptr = XLogBytePosToEndRecPtr(startbytepos);
    if (ptr % XLOG_SEG_SIZE == 0) {
        *EndPos = *StartPos = ptr;
        return false;
    }

    endbytepos = startbytepos + size;
    prevbytesize = compare.u32[2];
    currlrc = (int32)compare.u32[3];

    *StartPos = XLogBytePosToRecPtr(startbytepos);
    *EndPos = XLogBytePosToEndRecPtr(endbytepos);

    segleft = XLOG_SEG_SIZE - ((*EndPos) % XLOG_SEG_SIZE);
    if (segleft != XLOG_SEG_SIZE) {
        /* consume the rest of the segment */
        *EndPos += segleft;
        endbytepos = XLogRecPtrToBytePos(*EndPos);
    }

    exchange.u64[0] = endbytepos;
    size = (uint32)(endbytepos - startbytepos);
    exchange.u64[1] = (uint64)size;
    exchange.u32[3] = currlrc;

    current = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos, compare, exchange);
    if (!UINT128_IS_EQUAL(compare, current)) {
        UINT128_COPY(compare, current);
        goto loop;
    }
#else
    SpinLockAcquire(&Insert->insertpos_lck);

    startbytepos = Insert->CurrBytePos;

    if (isupgrade) {
        ptr = XLogBytePosToRecPtr(startbytepos);
        freespace = INSERT_FREESPACE(ptr);

        if (freespace < SizeOfXLogRecordOld) {
            ereport(LOG, (errmsg("The switch xlog need not be inserted in upgrade if the position "
                                 "is in the last 24 Bytes of xlog page.")));
            startbytepos += (XLOG_BLCKSZ - (ptr % XLOG_BLCKSZ));
            Insert->CurrBytePos = startbytepos;
            SpinLockRelease(&Insert->insertpos_lck);
            *EndPos = *StartPos = ptr;
            return false;
        }
    }

    ptr = XLogBytePosToEndRecPtr(startbytepos);
    if (ptr % XLOG_SEG_SIZE == 0) {
        SpinLockRelease(&Insert->insertpos_lck);
        *EndPos = *StartPos = ptr;
        return false;
    }

    endbytepos = startbytepos + size;
    prevbytesize = startbytepos - Insert->PrevByteSize;

    *StartPos = XLogBytePosToRecPtr(startbytepos);
    *EndPos = XLogBytePosToEndRecPtr(endbytepos);

    segleft = XLOG_SEG_SIZE - ((*EndPos) % XLOG_SEG_SIZE);
    if (segleft != XLOG_SEG_SIZE) {
        /* consume the rest of the segment */
        *EndPos += segleft;
        endbytepos = XLogRecPtrToBytePos(*EndPos);
    }
    Insert->CurrBytePos = endbytepos;
    Insert->PrevByteSize = size;

    SpinLockRelease(&Insert->insertpos_lck);
#endif /* __x86_64__ || __aarch64__ */

    *PrevPtr = XLogBytePosToRecPtr(startbytepos - prevbytesize);

    Assert((*EndPos) % XLOG_SEG_SIZE == 0);
    Assert(XLogRecPtrToBytePos(*EndPos) == endbytepos);
    Assert(XLogRecPtrToBytePos(*StartPos) == startbytepos);
    Assert(XLogRecPtrToBytePos(*PrevPtr) == (startbytepos - prevbytesize));

    return true;
}

static void StartSuspendWalInsert(int32 *const lastlrc_ptr)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint64 startbytepos;
    uint32 prevbytesize;
    int32  currlrc;
    int32  ientry;
    volatile WALInsertStatusEntry *entry;
    uint128_u compare;
    uint128_u exchange;
    uint128_u current;

    compare = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos);
    Assert(sizeof(Insert->CurrBytePos) == 8);
    Assert(sizeof(Insert->PrevByteSize) == 4);
    Assert(sizeof(Insert->CurrLRC) == 4);

loop:
    /*
     * |CurrBytePos            |PrevByteSize | CurrLRC
     * ------------------------+------------ +-----------
     * |64 bits                |32 bits      |32 bits
     */

    startbytepos = compare.u64[0];
    prevbytesize = compare.u32[2];
    currlrc = (int32)compare.u32[3];

    /*
     * if WAL Insert threads are already suspended, wait
     */
    if (unlikely(currlrc == WAL_COPY_SUSPEND)) {
        compare = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos);
        goto loop;
    }

    /*
     * Return the last LRC
     */
    *lastlrc_ptr = currlrc;

    currlrc = WAL_COPY_SUSPEND;

    /*
     * Swap in currlrc to suspend WAL copy threads but keep startbytepos and prevbytesize
     * as it is
     */
    exchange.u64[0] = startbytepos;
    exchange.u32[2] = prevbytesize;
    exchange.u32[3] = currlrc;

    current = atomic_compare_and_swap_u128((uint128_u*)&Insert->CurrBytePos, compare, exchange);
    if (!UINT128_IS_EQUAL(compare, current)) {
        UINT128_COPY(compare, current);
        goto loop;
    }

    /*
     * Wait for the WAL copy thread obtaining the "*lastlrc_ptr - 1" to finish
     * *lastlrc_ptr is the LRC for the next WAL insert thread to acquire
     * I probably should give it a better name other than "lastlrc_ptr" that is
     * somewhat misleading but I am trying to say that is the last LRC stored
     * in the shared memory before we swap in WAL_COPY_SUSPEND
     * The last working WAL insert thread should be holding "*lastlrc_ptr - 1"
     */
    int ilrc = (*lastlrc_ptr - 1) & 0x7FFFFFFF;
    ientry = ilrc & (WAL_INSERT_STATUS_ENTRIES - 1);
    entry = &g_instance.wal_cxt.walInsertStatusTable[ientry];

    /*
     * Wait if the WAL copy thread obtaining the "*lastlrc_ptr - 1" didn't finish.
     */
    bool stop = true;
    while (stop) {
        if (!g_instance.wal_cxt.isWalWriterUp) {
            XLogSelfFlush();
        }

        /*
         * Wait till the Wal writer scans to "*lastlrc_ptr - 1"
         * except that there is nobody before me after StartupXLOG
         * this edge case needs to be excluded can be identified by:
         *
         * (*lastlrc_ptr == 0 && entry->endLSN == 0)
         *
         * if the current LRC  is 0 and the endLSN corresponding to the LRC before
         * the current LRC is also 0 then we know there is nobody doing the WAL
         * insert till this point. We need to also check entry->endLSN because we
         * need to consider the 31bit LRC wrap-around  
         */
        stop = ((*lastlrc_ptr != 0 || entry->endLSN != 0) && (g_instance.wal_cxt.lastLRCScanned != ilrc));
    }

    return;
}

/*
 * This function must be called by whoever successfully suspended all WAL insert threads or
 * it will be a disaster
 */
static void StopSuspendWalInsert(int32 lastlrc)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;

    /*
     * Since I already suspend all other WAL copy threads,
     * it is safe to do a direct 32 bit value assignment
     */
    Insert->CurrLRC = lastlrc;

    return;
}

/*
 * Subroutine of XLogInsert.  Copies a WAL record to an already-reserved
 * area in the WAL.
 */
static void CopyXLogRecordToWAL(int write_len, bool isLogSwitch, XLogRecData* rdata, XLogRecPtr StartPos,
    XLogRecPtr EndPos, int32 *const currlrc_ptr)
{
    char *currpos = NULL;
    uint32 freespace;
    int written;
    XLogRecPtr CurrPos;
    XLogPageHeader pagehdr;
    errno_t errorno = EOK;
    bool stop = true;
    /*
     * The following 2 variables are used to handle large records
     */
    uint32 startBufPageIdx = XLogRecPtrToBufIdx(StartPos);
    int numHitsOnStartPage = 1;

    /*
     * Get a pointer to the right place in the right WAL buffer to start
     * inserting to.
     */
    CurrPos = StartPos;
    currpos = GetXLogBuffer<false>(CurrPos);
    freespace = INSERT_FREESPACE(CurrPos);

    /*
     * there should be enough space for at least the first field (xl_tot_len)
     * on this page.
     */
    Assert(freespace >= sizeof(uint32));

    /* Copy record data */
    written = 0;
    while (rdata != NULL) {
        char *rdata_data = rdata->data;
        uint32 rdata_len = rdata->len;

        while (rdata_len > freespace) {
            /*
             * Write what fits on this page, and continue on the next page.
             */
            Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || freespace == 0);
            errorno = memcpy_s(currpos, SECUREC_STRING_MAX_LEN, rdata_data, freespace);
            securec_check(errorno, "", "");

            rdata_data += freespace;
            rdata_len -= freespace;
            written += freespace;
            CurrPos += freespace;

            /*
             * We hit the page we start from within the same copy. We must initiate
             * a flush without updating WAL copy status entry or we will be stuck
             */
            if (unlikely(XLogRecPtrToBufIdx(CurrPos) == startBufPageIdx)) {
                numHitsOnStartPage++;
                XLogSelfFlushWithoutStatus(numHitsOnStartPage, CurrPos, *currlrc_ptr);
            }

            /*
             * Get pointer to beginning of next page, and set the xlp_rem_len
             * in the page header. Set XLP_FIRST_IS_CONTRECORD.
             *
             * It's safe to set the contrecord flag and xlp_rem_len without a
             * lock on the page. All the other flags were already set when the
             * page was initialized, in AdvanceXLInsertBuffer, and we're the
             * only backend that needs to set the contrecord flag.
             */
            currpos = GetXLogBuffer<false>(CurrPos);
            pagehdr = (XLogPageHeader)currpos;
            pagehdr->xlp_rem_len = write_len - written;
            pagehdr->xlp_info |= XLP_FIRST_IS_CONTRECORD;

            /* skip over the page header */
            if (CurrPos % XLogSegSize == 0) {
                CurrPos += SizeOfXLogLongPHD;
                currpos += SizeOfXLogLongPHD;
            } else {
                CurrPos += SizeOfXLogShortPHD;
                currpos += SizeOfXLogShortPHD;
            }
            freespace = INSERT_FREESPACE(CurrPos);
        }

        Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || rdata_len == 0);
        errorno = memcpy_s(currpos, SECUREC_STRING_MAX_LEN, rdata_data, rdata_len);
        securec_check(errorno, "", "");

        currpos += rdata_len;
        CurrPos += rdata_len;
        freespace -= rdata_len;
        written += rdata_len;

        rdata = rdata->next;
    }
    Assert(written == write_len);

    /* Align the end position, so that the next record starts aligned */
    CurrPos = MAXALIGN(CurrPos);

    /*
     * If this was an xlog-switch, it's not enough to write the switch record,
     * we also have to consume all the remaining space in the WAL segment.
     * We have already reserved it for us, but we still need to make sure it's
     * allocated and zeroed in the WAL buffers so that when the caller (or
     * someone else) does XLogWrite(), it can really write out all the zeros.
     */
    if (isLogSwitch && CurrPos % XLOG_SEG_SIZE != 0) {
        int currlrc = *currlrc_ptr;
        /* An xlog-switch record doesn't contain any data besides the header */
        Assert(write_len == SizeOfXLogRecord || write_len == SizeOfXLogRecordOld);

        /*
         * We do this one page at a time, to make sure we don't deadlock
         * against ourselves if wal_buffers < XLOG_SEG_SIZE.
         */
        Assert(EndPos % XLogSegSize == 0);

        /* Use up all the remaining space on the first page */
        CurrPos += freespace;

        /*
         * When inserting XLOG switch, we communicate with the WAL writer
         * page by page
         */
        while (CurrPos < EndPos) {
            uint32 current_entry = currlrc & (WAL_INSERT_STATUS_ENTRIES - 1);
            WALInsertStatusEntry *status_entry_ptr =
                &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(current_entry)];

            CurrPos += XLOG_BLCKSZ;

            /* 
             * Initialize the WAL buffer page if needed
             * Please note that endPtr passed into XLogWaitBufferInit is the LSN currently tagged
             * on the buffer page we want
             */
            int idx = (int)XLogRecPtrToBufIdx(CurrPos);
            XLogRecPtr endPtr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[idx];
            XLogRecPtr expectedEndPtr = CurrPos + XLOG_BLCKSZ - CurrPos % XLOG_BLCKSZ;
            if (expectedEndPtr != endPtr) {
                pgstat_report_waitevent(WAIT_EVENT_WAL_BUFFER_FULL);
                XLogRecPtr waitEndPtr = XLogPrevBlockInBufPage(expectedEndPtr);
                XLogWaitBufferInit(waitEndPtr);
                pgstat_report_waitevent(WAIT_EVENT_END);
            }
            /*
             * Update the WAL copy status entry to trigger flush
             *
             * First we wait to own a WAL copy status entry assigned
             * to us if the WAL writer is alive
             *
             * If the wal writer is not up for the xlog flush job, we need to invoke XLogBackgroundFlush
             * ourselves to push the status array recycling.
             */
            if (g_instance.wal_cxt.isWalWriterUp) {
                do {
                    stop = (status_entry_ptr->LRC == currlrc && (status_entry_ptr->status == WAL_NOT_COPIED));
                } while (!stop);
            } else if (status_entry_ptr->LRC != currlrc) {
                /*
                 * Here we don't need to loop on XLogSelfFlush to ensure our stuff is
                 * flushed because:
                 * 1) I am an XLog switch so I already called StartSuspendWalInsert to suspend
                 *    all other XLog insert accesses and the flush function already scanned till
                 *    the LRC right before the suspended LRC
                 * 2) XLog switch is inserted and flushed page by page sequentially 
                 */
                 XLogSelfFlush();
            }

            /*
             * We own the entry so we don't expect need to loop the CAS call
             * The CAS call here is to protext reads in the WAL writer thread
             */
            (void)pg_atomic_exchange_u64(&status_entry_ptr->endLSN, CurrPos);
            pg_memory_barrier();
            status_entry_ptr->status = WAL_COPIED;
            /*
             * Trigger a flush if WAL writer thread is not up yet
             */
            if (!g_instance.wal_cxt.isWalWriterUp) {
                /*
                 * Here we don't need to loop on XLogSelfFlush to ensure our stuff is
                 * flushed because:
                 * 1) I am an XLog switch so I already called StartSuspendWalInsert to suspend
                 *    all other XLog insert accesses and the flush function already scanned till
                 *    the LRC right before the suspended LRC
                 * 2) XLog switch is inserted and flushed page by page sequentially 
                 */
                XLogSelfFlush();
            }
            currlrc = (currlrc + 1) & 0x7FFFFFFF;
        }
        *currlrc_ptr = currlrc;
    }

    if (CurrPos != EndPos) {
        ereport(PANIC, (errmsg("space reserved for WAL record does not match what was written")));
    }
    pgstat_report_waitevent_count(WAIT_EVENT_WAL_BUFFER_ACCESS);
}

/*
 * Get a pointer to the right location in the WAL buffer containing the
 * given XLogRecPtr.
 *
 * If the page is not initialized yet, it is initialized. That might require
 * evicting an old dirty buffer from the buffer cache, which means I/O.
 *
 * The caller must ensure that the page containing the requested location
 * isn't evicted yet, and won't be evicted. The way to ensure that is to
 * hold onto an XLogInsertLock with the xlogInsertingAt position set to
 * something <= ptr. GetXLogBuffer() will update xlogInsertingAt if it needs
 * to evict an old page from the buffer. (This means that once you call
 * GetXLogBuffer() with a given 'ptr', you must not access anything before
 * that point anymore, and must not call GetXLogBuffer() with an older 'ptr'
 * later, because older buffers might be recycled already)
 */
template <bool isGroupInsert>
static char *GetXLogBuffer(XLogRecPtr ptr, PGPROC *proc)
{
    int idx;
    XLogRecPtr endptr;
    XLogRecPtr expectedEndPtr;

    /*
     * Fast path for the common case that we need to access again the same
     * page as last time.
     */
    if (ptr / XLOG_BLCKSZ == t_thrd.xlog_cxt.cachedPage) {
        Assert(((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_magic == XLOG_PAGE_MAGIC);
        Assert(((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_pageaddr == ptr - (ptr % XLOG_BLCKSZ));
        return t_thrd.xlog_cxt.cachedPos + ptr % XLOG_BLCKSZ;
    }

    /*
     * The XLog buffer cache is organized so that a page is always loaded
     * to a particular buffer.  That way we can easily calculate the buffer
     * a given page must be loaded into, from the XLogRecPtr alone.
     */
    idx = XLogRecPtrToBufIdx(ptr);

    /*
     * See what page is loaded in the buffer at the moment. It could be the
     * page we're looking for, or something older. It can't be anything newer
     * - that would imply the page we're looking for has already been written
     * out to disk and evicted, and the caller is responsible for making sure
     * that doesn't happen.
     *
     * However, we don't hold a lock while we read the value. If someone has
     * just initialized the page, it's possible that we get a "torn read" of
     * the XLogRecPtr if 64-bit fetches are not atomic on this platform. In
     * that case we will see a bogus value. That's ok, we'll grab the mapping
     * lock (in AdvanceXLInsertBuffer) and retry if we see anything else than
     * the page we're looking for. But it means that when we do this unlocked
     * read, we might see a value that appears to be ahead of the page we're
     * looking for. Don't PANIC on that, until we've verified the value while
     * holding the lock.
     */
    expectedEndPtr = ptr;
    expectedEndPtr += XLOG_BLCKSZ - ptr % XLOG_BLCKSZ;

    endptr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[idx];
    if (expectedEndPtr != endptr) {
        pgstat_report_waitevent(WAIT_EVENT_WAL_BUFFER_FULL);
        /*
         * We need to wait for the background flush to flush till the endptr
         * The background flush will also initialize the flushed pages
         * with new pages
         */
        XLogRecPtr waitEndPtr = XLogPrevBlockInBufPage(expectedEndPtr);
        XLogWaitBufferInit(waitEndPtr);

        pgstat_report_waitevent(WAIT_EVENT_END);

        endptr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[idx];
        if (expectedEndPtr != endptr) {
            ereport(PANIC, (errmsg("could not find WAL buffer for %X/%X", (uint32)(ptr >> 32), (uint32)ptr)));
        }
    } else {
        /*
         * Make sure the initialization of the page is visible to us, and
         * won't arrive later to overwrite the WAL data we write on the page.
         */
        pg_memory_barrier();
    }

    /*
     * Found the buffer holding this page. Return a pointer to the right
     * offset within the page.
     */
    t_thrd.xlog_cxt.cachedPage = ptr / XLOG_BLCKSZ;
    t_thrd.xlog_cxt.cachedPos = t_thrd.shemem_ptr_cxt.XLogCtl->pages + idx * (Size)XLOG_BLCKSZ;

    Assert((((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_magic == XLOG_PAGE_MAGIC) ||
           (((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_magic == XLOG_PAGE_MAGIC_OLD));
    Assert(((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_pageaddr == ptr - (ptr % XLOG_BLCKSZ) ||
           (((((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_pageaddr >> 32) |
             (((XLogPageHeader)t_thrd.xlog_cxt.cachedPos)->xlp_pageaddr << 32))) == ptr - (ptr % XLOG_BLCKSZ));

    return t_thrd.xlog_cxt.cachedPos + ptr % XLOG_BLCKSZ;
}

/*
 * Converts a "usable byte position" to XLogRecPtr. A usable byte position
 * is the position starting from the beginning of WAL, excluding all WAL
 * page headers.
 */
static XLogRecPtr XLogBytePosToRecPtr(uint64 bytepos)
{
    uint64 fullsegs;
    uint64 fullpages;
    uint64 bytesleft;
    uint32 seg_offset;
    XLogRecPtr result;

    fullsegs = bytepos / UsableBytesInSegment;
    bytesleft = bytepos % UsableBytesInSegment;

    if (bytesleft < XLOG_BLCKSZ - SizeOfXLogLongPHD) {
        /* fits on first page of segment */
        seg_offset = bytesleft + SizeOfXLogLongPHD;
    } else {
        /* account for the first page on segment with long header */
        seg_offset = XLOG_BLCKSZ;
        bytesleft -= XLOG_BLCKSZ - SizeOfXLogLongPHD;

        fullpages = bytesleft / UsableBytesInPage;
        bytesleft = bytesleft % UsableBytesInPage;

        seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
    }

    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, result);

    return result;
}

/*
 * Like XLogBytePosToRecPtr, but if the position is at a page boundary,
 * returns a pointer to the beginning of the page (ie. before page header),
 * not to where the first xlog record on that page would go to. This is used
 * when converting a pointer to the end of a record.
 */
static XLogRecPtr XLogBytePosToEndRecPtr(uint64 bytepos)
{
    uint64 fullsegs;
    uint64 fullpages;
    uint64 bytesleft;
    uint32 seg_offset;
    XLogRecPtr result;

    fullsegs = bytepos / UsableBytesInSegment;
    bytesleft = bytepos % UsableBytesInSegment;

    if (bytesleft < XLOG_BLCKSZ - SizeOfXLogLongPHD) {
        /* fits on first page of segment */
        if (bytesleft == 0) {
            seg_offset = 0;
        } else {
            seg_offset = bytesleft + SizeOfXLogLongPHD;
        }
    } else {
        /* account for the first page on segment with long header */
        seg_offset = XLOG_BLCKSZ;
        bytesleft -= XLOG_BLCKSZ - SizeOfXLogLongPHD;

        fullpages = bytesleft / UsableBytesInPage;
        bytesleft = bytesleft % UsableBytesInPage;

        if (bytesleft == 0) {
            seg_offset += fullpages * XLOG_BLCKSZ + bytesleft;
        } else {
            seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
        }
    }

    XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, result);

    return result;
}

/*
 * Convert an XLogRecPtr to a "usable byte position".
 */
static uint64 XLogRecPtrToBytePos(XLogRecPtr ptr)
{
    uint64 fullsegs;
    uint32 fullpages;
    uint32 offset;
    uint64 result;

    XLByteToSeg(ptr, fullsegs);

    fullpages = (ptr % XLOG_SEG_SIZE) / XLOG_BLCKSZ;
    offset = ptr % XLOG_BLCKSZ;

    if (fullpages == 0) {
        result = fullsegs * UsableBytesInSegment;
        if (offset > 0) {
            Assert(offset >= SizeOfXLogLongPHD);
            result += offset - SizeOfXLogLongPHD;
        }
    } else {
        result = fullsegs * UsableBytesInSegment + (XLOG_BLCKSZ - SizeOfXLogLongPHD) /* account for first page */
                 + (fullpages - 1) * UsableBytesInPage;                              /* full pages */
        if (offset > 0) {
            Assert(offset >= SizeOfXLogShortPHD);
            result += offset - SizeOfXLogShortPHD;
        }
    }

    return result;
}

/*
 * XLogArchiveNotify
 *
 * Create an archive notification file
 *
 * The name of the notification file is the message that will be picked up
 * by the archiver, e.g. we write 0000000100000001000000C6.ready
 * and the archiver then knows to archive XLOGDIR/0000000100000001000000C6,
 * then when complete, rename it to 0000000100000001000000C6.done
 */
void XLogArchiveNotify(const char *xlog)
{
    char archiveStatusPath[MAXPGPATH];
    FILE *fd = NULL;
    errno_t errorno = EOK;

    /* insert an otherwise empty file called <XLOG>.ready */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(errorno, "", "");

    fd = AllocateFile(archiveStatusPath, "w");
    if (fd == NULL) {
        ereport(LOG, (errcode_for_file_access(),
                      errmsg("could not create archive status file \"%s\": %m", archiveStatusPath)));
        return;
    }
    if (FreeFile(fd)) {
        ereport(LOG, (errcode_for_file_access(),
                      errmsg("could not write archive status file \"%s\": %m", archiveStatusPath)));
        fd = NULL;
        return;
    }

    /* Notify archiver that it's got something to do */
    if (IsUnderPostmaster) {
        SendPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER);
    }
}

/*
 * Convenience routine to notify using segment number representation of filename
 */
static void XLogArchiveNotifySeg(XLogSegNo segno)
{
    char xlog[MAXFNAMELEN];
    errno_t errorno = EOK;

    errorno = snprintf_s(xlog, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    XLogArchiveNotify(xlog);
}

/*
 * XLogArchiveForceDone
 *
 * Emit notification forcibly that an XLOG segment file has been successfully
 * archived, by creating <XLOG>.done regardless of whether <XLOG>.ready
 * exists or not.
 */
void XLogArchiveForceDone(const char *xlog)
{
    char archiveReady[MAXPGPATH];
    char archiveDone[MAXPGPATH];
    struct stat stat_buf;
    FILE *fd = NULL;
    errno_t errorno = EOK;

    /* Exit if already known done */
    errorno = snprintf_s(archiveDone, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");

    if (stat(archiveDone, &stat_buf) == 0) {
        return;
    }

    /* If .ready exists, rename it to .done */
    errorno = snprintf_s(archiveReady, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(errorno, "", "");

    if (stat(archiveReady, &stat_buf) == 0) {
        (void)durable_rename(archiveReady, archiveDone, WARNING);
        return;
    }

    /* insert an otherwise empty file called <XLOG>.done */
    fd = AllocateFile(archiveDone, "w");
    if (fd == NULL) {
        ereport(LOG,
                (errcode_for_file_access(), errmsg("could not create archive status file \"%s\": %m", archiveDone)));
        return;
    }
    if (FreeFile(fd)) {
        ereport(LOG,
                (errcode_for_file_access(), errmsg("could not write archive status file \"%s\": %m", archiveDone)));
        fd = NULL;
        return;
    }
}

/*
 * XLogArchiveCheckDone
 *
 * This is called when we are ready to delete or recycle an old XLOG segment
 * file or backup history file.  If it is okay to delete it then return true.
 * If it is not time to delete it, make sure a .ready file exists, and return
 * false.
 *
 * If <XLOG>.done exists, then return true; else if <XLOG>.ready exists,
 * then return false; else create <XLOG>.ready and return false.
 *
 * The reason we do things this way is so that if the original attempt to
 * create <XLOG>.ready fails, we'll retry during subsequent checkpoints.
 */
static bool XLogArchiveCheckDone(const char *xlog)
{
    char archiveStatusPath[MAXPGPATH];
    struct stat stat_buf;
    errno_t errorno = EOK;

    /* Only the primary need this step to check this xlog has been archived on standby.
     * If this xlog has been archived by all standby which start up the archive thread,
     * we should check weather the primary (if primary start up the archive s)
     */
    if (!HasBeenArchivedOnHaMode(xlog)) {
        return false;
    }

    /* Always deletable if archiving is off or in recovery process. Archiving is always disabled on standbys. */
    if (!XLogArchivingActive() || RecoveryInProgress()) {
        return true;
    }

    /* First check for .done --- this means archiver is done with it */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return true;
    }

    /* check for .ready --- this means archiver is still busy with it */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return false;
    }

    /* Race condition --- maybe archiver just finished, so recheck */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return true;
    }

    /* Retry creation of the .ready file */
    XLogArchiveNotify(xlog);
    return false;
}

static bool HasBeenArchivedOnHaMode(const char* xlog)
{
    /*
     * Generally, the validity of the xlog transferred from the upper layer has been verified.
     * Therefore, if the length of the xlog name transferred is greater than the standard length
     * of the xlog name (24 characters), the transferred file is a .backup file.
     * Therefore, if the xlog name contains more than 24 characters, return true directly.
     */
    if (strlen(xlog) > XLOG_NAME_LENGTH) {
        return true;
    }

    load_server_mode();
    int mode = t_thrd.xlog_cxt.server_mode;
    XLogRecPtr minium_lsn = PG_UINT64_MAX;

    for (int i = 0; mode == PRIMARY_MODE && i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd* walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        if (walsnd == NULL) {
            continue;
        }

        SpinLockAcquire(&walsnd->mutex);
        if (IsValidArchiverStandby((WalSnd*)walsnd) &&
            walsnd->arch_task_last_lsn != 0 && walsnd->arch_task_last_lsn < minium_lsn) {
            minium_lsn = walsnd->arch_task_last_lsn;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    if (mode == STANDBY_MODE && XLogArchivingActive()) {
        XLogRecPtr target_lsn = g_instance.archive_standby_cxt.archive_task.targetLsn;
        XLogRecPtr start_point = g_instance.archive_standby_cxt.standby_archive_start_point;
        minium_lsn = (target_lsn == 0) ? start_point : target_lsn;
    }

    if (minium_lsn == PG_UINT64_MAX) {
        return true;
    }
    char minium_archived_xlog_name[MAXFNAMELEN];
    XLogSegNo xlogSegno = 0;
    XLByteToSeg(minium_lsn, xlogSegno);
    XLogFileName(minium_archived_xlog_name, DEFAULT_TIMELINE_ID, xlogSegno);

    if (mode == STANDBY_MODE) {
        /* 
         * targetLsn or start_point may be the xlog to be archived. Therefore,
         * the value of targetLsn or start_point must be smaller than minium_archived_xlog_name
         * to ensure that the xlog has been archived.
         */
        return strcmp(xlog, minium_archived_xlog_name) < 0;
    }

    /*
     * if the input xlog name is smaller than or equal to the minium_archived_xlog_name,
     * we can confirm that the xlog has been archived by all standby nodes.
     */
    return strcmp(xlog, minium_archived_xlog_name) <= 0;
}

/*
 * XLogArchiveIsBusy
 *
 * Check to see if an XLOG segment file is still unarchived.
 * This is almost but not quite the inverse of XLogArchiveCheckDone: in
 * the first place we aren't chartered to recreate the .ready file, and
 * in the second place we should consider that if the file is already gone
 * then it's not busy.  (This check is needed to handle the race condition
 * that a checkpoint already deleted the no-longer-needed file.)
 */
static bool XLogArchiveIsBusy(const char *xlog)
{
    char archiveStatusPath[MAXPGPATH];
    struct stat stat_buf;
    errno_t errorno = EOK;

    /* First check for .done --- this means archiver is done with it */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return false;
    }

    /* check for .ready --- this means archiver is still busy with it */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return true;
    }

    /* Race condition --- maybe archiver just finished, so recheck */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");
    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return false;
    }

    /*
     * Check to see if the WAL file has been removed by checkpoint, which
     * implies it has already been archived, and explains why we can't see a
     * status file for it.
     */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlog);
    securec_check_ss(errorno, "", "");

    if (stat(archiveStatusPath, &stat_buf) != 0 && errno == ENOENT) {
        return false;
    }

    return true;
}

/*
 * XLogArchiveIsReady
 *
 * Check to see if an XLOG segment file has an archive notification (.ready)
 * file.
 */
bool XLogArchiveIsReady(const char *xlog)
{
    char archiveStatusPath[MAXPGPATH];
    struct stat stat_buf;
    errno_t rc = EOK;
    rc = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(rc, "", "");

    if (stat(archiveStatusPath, &stat_buf) == 0) {
        return true;
    }

    return false;
}

/*
 * XLogArchiveCleanup
 *
 * Cleanup archive notification file(s) for a particular xlog segment
 */
static void XLogArchiveCleanup(const char *xlog)
{
    char archiveStatusPath[MAXPGPATH];
    errno_t errorno = EOK;

    /* Remove the .done file */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".done");
    securec_check_ss(errorno, "", "");
    unlink(archiveStatusPath);

    /* Remove the .ready file if present --- normally it shouldn't be */
    errorno = snprintf_s(archiveStatusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", xlog, ".ready");
    securec_check_ss(errorno, "", "");
    unlink(archiveStatusPath);
}

/*
 * Initialize XLOG buffers, writing out old buffers if they still contain
 * unwritten data, upto the page containing 'upto'. Or if 'opportunistic' is
 * true, initialize as many pages as we can without having to write out
 * unwritten data. Any new pages are initialized to zeros, with pages headers
 * initialized properly.
 */
template <bool isGroupInsert>
static void AdvanceXLInsertBuffer(XLogRecPtr upto, bool opportunistic, PGPROC *proc)
{
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    int nextidx;
    XLogRecPtr OldPageRqstPtr;
    XLogRecPtr NewPageEndPtr = InvalidXLogRecPtr;
    XLogRecPtr NewPageBeginPtr;
    XLogPageHeader NewPage;
    int npages = 0;
    errno_t errorno = EOK;
    XLogwrtResult *LogwrtResultPtr = NULL;
    TimeLineID xlogTimeLineID = 0;

#ifdef __aarch64__
    if (isGroupInsert) {
        LogwrtResultPtr = (XLogwrtResult *)proc->xlogGroupLogwrtResult;
        xlogTimeLineID = proc->xlogGroupTimeLineID;
    } else {
        LogwrtResultPtr = t_thrd.xlog_cxt.LogwrtResult;
        xlogTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    }
#else
    LogwrtResultPtr = t_thrd.xlog_cxt.LogwrtResult;
    xlogTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
#endif

    LWLockAcquire(WALBufMappingLock, LW_EXCLUSIVE);

    nextidx = (int)XLogRecPtrToBufIdx(t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo);
    /*
     * Get ending-offset of the buffer page we need to replace (this may
     * be zero if the buffer hasn't been used yet).  Fall through if it's
     * already written out.
     */
    OldPageRqstPtr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[nextidx];

    /*
     * Now that we have the lock, check if someone initialized the page
     * already.
     */
    while (upto >= OldPageRqstPtr) {
        /* Now the next buffer slot is free and we can set it up to be the next output page. */
        NewPageBeginPtr = t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo;
        NewPageEndPtr = NewPageBeginPtr + XLOG_BLCKSZ;

        Assert(XLogRecPtrToBufIdx(NewPageBeginPtr) == (uint32)nextidx);

        NewPage = (XLogPageHeader)(t_thrd.shemem_ptr_cxt.XLogCtl->pages + nextidx * (Size)XLOG_BLCKSZ);

        /*
         * Be sure to re-zero the buffer so that bytes beyond what we've
         * written will look like zeroes and not valid XLOG records...
         */
        errorno = memset_s((char *)NewPage, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
        securec_check(errorno, "", "");

        /* Fill the new page's header */
        NewPage->xlp_magic = XLOG_PAGE_MAGIC;
        NewPage->xlp_tli = xlogTimeLineID;
        NewPage->xlp_pageaddr = NewPageBeginPtr;

        /*
         * If online backup is not in progress, mark the header to indicate
         * that* WAL records beginning in this page have removable backup
         * blocks.  This allows the WAL archiver to know whether it is safe to
         * compress archived WAL data by transforming full-block records into
         * the non-full-block format.  It is sufficient to record this at the
         * page level because we force a page switch (in fact a segment switch)
         * when starting a backup, so the flag will be off before any records
         * can be written during the backup.  At the end of a backup, the last
         * page will be marked as all unsafe when perhaps only part is unsafe,
         * but at worst the archiver would miss the opportunity to compress a
         * few records.
         */
        if (!Insert->forcePageWrites) {
            NewPage->xlp_info |= XLP_BKP_REMOVABLE;
        }

        /* If first page of an XLOG segment file, make it a long header. */
        if ((NewPage->xlp_pageaddr % XLogSegSize) == 0) {
            XLogLongPageHeader NewLongPage = (XLogLongPageHeader)NewPage;
            NewLongPage->xlp_sysid = t_thrd.shemem_ptr_cxt.ControlFile->system_identifier;
            NewLongPage->xlp_seg_size = XLogSegSize;
            NewLongPage->xlp_xlog_blcksz = XLOG_BLCKSZ;
            NewPage->xlp_info |= XLP_LONG_HEADER;
        }

        /*
         * Make sure the initialization of the page becomes visible to others
         * before the xlblocks update. GetXLogBuffer() reads xlblocks without
         * holding a lock.
         */
        pg_write_barrier();

        *((volatile XLogRecPtr *)&t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[nextidx]) = NewPageEndPtr;
        t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo = NewPageEndPtr;

        nextidx = (int)XLogRecPtrToBufIdx(t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo);
        OldPageRqstPtr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[nextidx];

        npages++;
    }

    LWLockRelease(WALBufMappingLock);

#ifdef WAL_DEBUG
    if (npages > 0) {
        ereport(DEBUG1, (errmsg("initialized %d pages, upto %X/%X", npages, (uint32)(NewPageEndPtr >> 32),
                                (uint32)NewPageEndPtr)));
    }
#endif
}

template static void AdvanceXLInsertBuffer<true>(XLogRecPtr, bool, PGPROC *);
template static void AdvanceXLInsertBuffer<false>(XLogRecPtr, bool, PGPROC *);

/*
 * Check whether we've consumed enough xlog space that a checkpoint is needed.
 *
 * new_segno indicates a log file that has just been filled up (or read
 * during recovery). We measure the distance from RedoRecPtr to new_segno

 * and see if that exceeds CheckPointSegments.
 *
 * Note: it is caller's responsibility that RedoRecPtr is up-to-date.
 */
static bool XLogCheckpointNeeded(XLogSegNo new_segno)
{
    XLogSegNo old_segno;

    XLByteToSeg(t_thrd.xlog_cxt.RedoRecPtr, old_segno);

    if (new_segno >= old_segno + ((uint32)u_sess->attr.attr_storage.CheckPointSegments - 1)) {
        return true;
    }
    return false;
}

/*
 * Write and/or fsync the log at least as far as WriteRqst indicates.
 *
 * If flexible == TRUE, we don't have to write as far as WriteRqst, but
 * may stop at any convenient boundary (such as a cache or logfile boundary).
 * This option allows us to avoid uselessly issuing multiple writes when a
 * single one would do.
 *
 * If xlog_switch == TRUE, we are intending an xlog segment switch, so
 * perform end-of-segment actions after writing the last page, even if
 * it's not physically the end of its segment.  (NB: this will work properly
 * only if caller specifies WriteRqst == page-end and flexible == false,
 * and there is some data to write.)
 *
 * Must be called with WALWriteLock held.
 */
static void XLogWrite(const XLogwrtRqst &WriteRqst, bool flexible)
{
    bool ispartialpage = false;
    bool last_iteration = false;
    bool finishing_seg = false;
    bool use_existent = false;
    int curridx = 0;
    int npages = 0;
    int startidx = 0;
    uint32 startoffset = 0;
    Size actualBytes;

    /* We should always be inside a critical section here */
    Assert(t_thrd.int_cxt.CritSectionCount > 0);

    // Update local LogwrtResult (caller probably did this already, but...)
    *t_thrd.xlog_cxt.LogwrtResult = t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult;

    /*
     * Since successive pages in the xlog cache are consecutively allocated,
     * we can usually gather multiple pages together and issue just one
     * write() call.  npages is the number of pages we have determined can be
     * written together; startidx is the cache block index of the first one,
     * and startoffset is the file offset at which it should go. The latter
     * two variables are only valid when npages > 0, but we must initialize
     * all of them to keep the compiler quiet.
     */
    npages = 0;
    startidx = 0;
    startoffset = 0;

    /*
     * Within the loop, curridx is the cache block index of the page to
     * consider writing.  We advance Write->curridx only after successfully
     * writing pages.  (Right now, this refinement is useless since we are
     * going to PANIC if any error occurs anyway; but someday it may come in
     * useful.)
     */
    curridx = XLogRecPtrToBufIdx(t_thrd.xlog_cxt.LogwrtResult->Write);

    while (XLByteLT(t_thrd.xlog_cxt.LogwrtResult->Write, WriteRqst.Write)) {
        /*
         * Make sure we're not ahead of the insert process.  This could happen
         * if we're passed a bogus WriteRqst.Write that is past the end of the
         * last page that's been initialized by AdvanceXLInsertBuffer.
         */
        XLogRecPtr EndPtr = t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[curridx];
        if (!XLByteLT(t_thrd.xlog_cxt.LogwrtResult->Write, EndPtr))
            ereport(PANIC,
                    (errmsg("xlog write request %X/%X is past end of log %X/%X",
                            (uint32)(t_thrd.xlog_cxt.LogwrtResult->Write >> 32),
                            (uint32)t_thrd.xlog_cxt.LogwrtResult->Write, (uint32)(EndPtr >> 32), (uint32)EndPtr)));

        /* Advance LogwrtResult.Write to end of current buffer page */
        t_thrd.xlog_cxt.LogwrtResult->Write = EndPtr;
        ispartialpage = XLByteLT(WriteRqst.Write, t_thrd.xlog_cxt.LogwrtResult->Write);

        if (!XLByteInPrevSeg(t_thrd.xlog_cxt.LogwrtResult->Write, t_thrd.xlog_cxt.openLogSegNo)) {
            /*
             * Switch to new logfile segment.  We cannot have any pending
             * pages here (since we dump what we have at segment end).
             */
            Assert(npages == 0);
            if (t_thrd.xlog_cxt.openLogFile >= 0) {
                XLogFileClose();
            }
            XLByteToPrevSeg(t_thrd.xlog_cxt.LogwrtResult->Write, t_thrd.xlog_cxt.openLogSegNo);

            /* create/use new log file */
            use_existent = true;
            t_thrd.xlog_cxt.openLogFile = XLogFileInit(t_thrd.xlog_cxt.openLogSegNo, &use_existent, true);
            t_thrd.xlog_cxt.openLogOff = 0;

            /*
             * Unlock WalAuxiliary thread to init new xlog segment if we are running out
             * of xlog segments.
             */
            if (!use_existent) {
                g_instance.wal_cxt.globalEndPosSegNo = t_thrd.xlog_cxt.openLogSegNo;
                WakeupWalSemaphore(&g_instance.wal_cxt.walInitSegLock->l.sem);
            }
        }

        /* Make sure we have the current logfile open */
        if (t_thrd.xlog_cxt.openLogFile <= 0) {
            XLByteToPrevSeg(t_thrd.xlog_cxt.LogwrtResult->Write, t_thrd.xlog_cxt.openLogSegNo);
            t_thrd.xlog_cxt.openLogFile = XLogFileOpen(t_thrd.xlog_cxt.openLogSegNo);
            t_thrd.xlog_cxt.openLogOff = 0;
        }

        /* Add current page to the set of pending pages-to-dump */
        if (npages == 0) {
            /* first of group */
            startidx = curridx;
            startoffset = (t_thrd.xlog_cxt.LogwrtResult->Write - XLOG_BLCKSZ) % XLogSegSize;
        }
        npages++;

        /*
         * Dump the set if this will be the last loop iteration, or if we are
         * at the last page of the cache area (since the next page won't be
         * contiguous in memory), or if we are at the end of the logfile
         * segment.
         */
        last_iteration = !XLByteLT(t_thrd.xlog_cxt.LogwrtResult->Write, WriteRqst.Write);
        finishing_seg = !ispartialpage && (startoffset + npages * XLOG_BLCKSZ) >= XLogSegSize;

        if (last_iteration || curridx == t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck || finishing_seg) {
            /* record elapsed time */
            instr_time startTime;
            instr_time endTime;
            PgStat_Counter elapsedTime;

            /* Need to seek in the file? */
            if (t_thrd.xlog_cxt.openLogOff != startoffset) {
                if (lseek(t_thrd.xlog_cxt.openLogFile, (off_t)startoffset, SEEK_SET) < 0) {
                    ereport(PANIC, (errcode_for_file_access(),
                                    errmsg("could not seek in log file %s to offset %u: %m",
                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.openLogSegNo),
                                           startoffset)));
                }
                t_thrd.xlog_cxt.openLogOff = startoffset;
            }

            /* OK to write the page(s) */
            char *from = t_thrd.shemem_ptr_cxt.XLogCtl->pages + startidx * (Size)XLOG_BLCKSZ;
            Size nbytes = npages * (Size)XLOG_BLCKSZ;
            errno = 0;

            pgstat_report_waitevent(WAIT_EVENT_WAL_WRITE);
            INSTR_TIME_SET_CURRENT(startTime);
            actualBytes = write(t_thrd.xlog_cxt.openLogFile, from, nbytes);
            INSTR_TIME_SET_CURRENT(endTime);
            INSTR_TIME_SUBTRACT(endTime, startTime);
            /* when track_activities and enable_instr_track_wait are on,
             * elapsedTime can be replaced by beentry->waitInfo.event_info.duration.
             */
            elapsedTime = (PgStat_Counter)INSTR_TIME_GET_MICROSEC(endTime);
            pgstat_report_waitevent(WAIT_EVENT_END);

            if (actualBytes != (Size)nbytes) {
                /* if write didn't set errno, assume no disk space */
                if (errno == 0) {
                    errno = ENOSPC;
                }
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("could not write to log file %s at offset %u, length %lu: %m",
                                       XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.openLogSegNo),
                                       t_thrd.xlog_cxt.openLogOff, (unsigned long)nbytes)));
            }

            reportRedoWrite((PgStat_Counter)npages, elapsedTime);

            /* Update state for write */
            t_thrd.xlog_cxt.openLogOff += nbytes;
            npages = 0;

            /*
             * If we just wrote the whole last page of a logfile segment,
             * fsync the segment immediately.  This avoids having to go back
             * and re-open prior segments when an fsync request comes along
             * later. Doing it here ensures that one and only one backend will
             * perform this fsync.
             *
             * We also do this if this is the last page written for an xlog
             * switch.
             *
             * This is also the right place to notify the Archiver that the
             * segment is ready to copy to archival storage, and to update the
             * timer for archive_timeout, and to signal for a checkpoint if
             * too many logfile segments have been used since the last
             * checkpoint.
             */
            if (finishing_seg) {
                issue_xlog_fsync(t_thrd.xlog_cxt.openLogFile, t_thrd.xlog_cxt.openLogSegNo);
                /* signal that we need to wakeup walsenders later */
                WalSndWakeupRequest();
                t_thrd.xlog_cxt.LogwrtResult->Flush = t_thrd.xlog_cxt.LogwrtResult->Write; /* end of page */
                if (XLogArchivingActive()) {
                    XLogArchiveNotifySeg(t_thrd.xlog_cxt.openLogSegNo);
                }
                t_thrd.shemem_ptr_cxt.XLogCtl->lastSegSwitchTime = (pg_time_t)time(NULL);

                /*
                 * Request a checkpoint if we've consumed too much xlog since
                 * the last one.  For speed, we first check using the local
                 * copy of RedoRecPtr, which might be out of date; if it looks
                 * like a checkpoint is needed, forcibly update RedoRecPtr and
                 * recheck.
                 */
                if (IsUnderPostmaster && XLogCheckpointNeeded(t_thrd.xlog_cxt.openLogSegNo)) {
                    (void)GetRedoRecPtr();
                    if (XLogCheckpointNeeded(t_thrd.xlog_cxt.openLogSegNo)) {
                        RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
                    }
                }
            }
        }

        if (ispartialpage) {
            /* Only asked to write a partial page */
            t_thrd.xlog_cxt.LogwrtResult->Write = WriteRqst.Write;
            break;
        }
        curridx = NextBufIdx(curridx);

        /* If flexible, break out of loop as soon as we wrote something */
        if (flexible && npages == 0) {
            break;
        }
    }

    Assert(npages == 0);

    // If asked to flush, do so
    if (XLByteLT(t_thrd.xlog_cxt.LogwrtResult->Flush, WriteRqst.Flush) &&
        XLByteLT(t_thrd.xlog_cxt.LogwrtResult->Flush, t_thrd.xlog_cxt.LogwrtResult->Write)) {
        /*
         * Could get here without iterating above loop, in which case we might
         * have no open file or the wrong one.	However, we do not need to
         * fsync more than one file.
         */
        if (u_sess->attr.attr_storage.sync_method != SYNC_METHOD_OPEN &&
            u_sess->attr.attr_storage.sync_method != SYNC_METHOD_OPEN_DSYNC) {
            if (t_thrd.xlog_cxt.openLogFile >= 0 &&
                !XLByteInPrevSeg(t_thrd.xlog_cxt.LogwrtResult->Write, t_thrd.xlog_cxt.openLogSegNo)) {
                XLogFileClose();
            }
            if (t_thrd.xlog_cxt.openLogFile < 0) {
                XLByteToPrevSeg(t_thrd.xlog_cxt.LogwrtResult->Write, t_thrd.xlog_cxt.openLogSegNo);
                t_thrd.xlog_cxt.openLogFile = XLogFileOpen(t_thrd.xlog_cxt.openLogSegNo);
                t_thrd.xlog_cxt.openLogOff = 0;
            }
            issue_xlog_fsync(t_thrd.xlog_cxt.openLogFile, t_thrd.xlog_cxt.openLogSegNo);
        }
        /* signal that we need to wakeup walsenders later */
        WalSndWakeupRequest();
        t_thrd.xlog_cxt.LogwrtResult->Flush = t_thrd.xlog_cxt.LogwrtResult->Write;
    }

    /*
     * Update shared-memory status
     *
     * We make sure that the shared 'request' values do not fall behind the
     * 'result' values.  This is not absolutely essential, but it saves some
     * code in a couple of places.
     */
    {
        /* use volatile pointer to prevent code rearrangement */
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        xlogctl->LogwrtResult = *t_thrd.xlog_cxt.LogwrtResult;
        if (XLByteLT(xlogctl->LogwrtRqst.Write, t_thrd.xlog_cxt.LogwrtResult->Write)) {
            xlogctl->LogwrtRqst.Write = t_thrd.xlog_cxt.LogwrtResult->Write;
        }
        if (XLByteLT(xlogctl->LogwrtRqst.Flush, t_thrd.xlog_cxt.LogwrtResult->Flush)) {
            xlogctl->LogwrtRqst.Flush = t_thrd.xlog_cxt.LogwrtResult->Flush;
            g_instance.comm_cxt.predo_cxt.redoPf.primary_flush_ptr = t_thrd.xlog_cxt.LogwrtResult->Flush;
        }
        SpinLockRelease(&xlogctl->info_lck);
    }
}

/*
 * Record the LSN for an asynchronous transaction commit/abort
 * and nudge the WALWriter if there is work for it to do.
 * (This should not be called for synchronous commits.)
 */
void XLogSetAsyncXactLSN(XLogRecPtr asyncXactLSN)
{
    XLogRecPtr WriteRqstPtr = asyncXactLSN;
    bool sleeping = false;

    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    *t_thrd.xlog_cxt.LogwrtResult = xlogctl->LogwrtResult;
    sleeping = xlogctl->WalWriterSleeping;
    if (XLByteLT(xlogctl->asyncXactLSN, asyncXactLSN)) {
        xlogctl->asyncXactLSN = asyncXactLSN;
    }
    SpinLockRelease(&xlogctl->info_lck);

    /*
     * If the WALWriter is sleeping, we should kick it to make it come out of
     * low-power mode.	Otherwise, determine whether there's a full page of
     * WAL available to write.
     */
    if (!sleeping) {
        /* back off to last completed page boundary */
        WriteRqstPtr -= WriteRqstPtr % XLOG_BLCKSZ;

        /* if we have already flushed that far, we're done */
        if (XLByteLE(WriteRqstPtr, t_thrd.xlog_cxt.LogwrtResult->Flush)) {
            return;
        }
    }

    /*
     * Nudge the WALWriter: it has a full page of WAL to write, or we want it
     * to come out of low-power mode so that this async commit will reach disk
     * within the expected amount of time.
     */
    if (g_instance.proc_base->walwriterLatch) {
        SetLatch(g_instance.proc_base->walwriterLatch);
    }
}

/*
 * Record the LSN up to which we can remove WAL because it's not required by
 * any replication slot.
 */
void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->replicationSlotMinLSN = lsn;
    SpinLockRelease(&xlogctl->info_lck);
    if (XLByteEQ(lsn, InvalidXLogRecPtr) && !IsInitdb && !RecoveryInProgress() &&
        !AM_WAL_SENDER && !AM_WAL_DB_SENDER && strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0) {
        ereport(WARNING, (errmsg("replicationSlotMinLSN is InvalidXLogRecPtr!!!")));
    }
}

/*
 * Return the oldest LSN we must retain to satisfy the needs of some
 * replication slot.
 */
static XLogRecPtr XLogGetReplicationSlotMinimumLSN(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr retval;
    SpinLockAcquire(&xlogctl->info_lck);
    retval = xlogctl->replicationSlotMinLSN;
    SpinLockRelease(&xlogctl->info_lck);

    return retval;
}

/*
 * To facilitate calling static function 'XLogGetReplicationSlotMinimumLSN()' in other files
 */
XLogRecPtr XLogGetReplicationSlotMinimumLSNByOther(void)
{
    XLogRecPtr minlsn;
    minlsn = XLogGetReplicationSlotMinimumLSN();
    return minlsn;
}

/*
 * Record the LSN up to which we can choose it as a startpoint to dummy standby.
 */
void XLogSetReplicationSlotMaximumLSN(XLogRecPtr lsn)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->replicationSlotMaxLSN = lsn;
    SpinLockRelease(&xlogctl->info_lck);
    if (XLByteEQ(lsn, InvalidXLogRecPtr) && !IsInitdb && !RecoveryInProgress() &&
        !AM_WAL_SENDER && !AM_WAL_DB_SENDER && strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0) {
        ereport(WARNING, (errmsg("replicationSlotMaxLSN is InvalidXLogRecPtr!!!")));
    }
}

/*
 * Return the latest LSN we must retain to satisfy the needs of some
 * replication slot.
 */
XLogRecPtr XLogGetReplicationSlotMaximumLSN(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr retval;

    SpinLockAcquire(&xlogctl->info_lck);
    retval = xlogctl->replicationSlotMaxLSN;
    SpinLockRelease(&xlogctl->info_lck);

    return retval;
}

/*
 * Reset replication slot before promote to primary.
 *	We should free the slotname in caller.
 */
static void ResetSlotLSNEndRecovery(StringInfo slotname)
{
    int i = 0;
    bool resetslot = false;
    errno_t rc = 0;
    ReplicationSlot *slot = NULL;

    Assert(t_thrd.slot_cxt.ReplicationSlotCtl != NULL);
    Assert(slotname != NULL);

    if (slotname->len <= 0) {
        return;
    }

    LWLockAcquire(ReplicationSlotControlLock, LW_EXCLUSIVE);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        /* We got the valid peer slot, reset it */
        if (s->in_use) {
            SpinLockAcquire(&s->mutex);
            if (s->data.isDummyStandby) {
                s->data.restart_lsn = 0;
            } else if (strncmp(NameStr(s->data.name), slotname->data, strlen(slotname->data)) == 0) {
                s->data.restart_lsn = latestValidRecord;
                resetslot = true;
            }
            s->just_dirtied = true;
            s->dirty = true;
            SpinLockRelease(&s->mutex);
        }
    }

    /*
     * If we can not find slot for PGXCNodeName, we will create it.
     * Explame for the scene: standby promotes(failover or switchover) to
     * primary firstly, pg_replslot is empty.
     */
    if (!resetslot) {
        /* choose an invalid slot record, and set as peer slot */
        for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
            SpinLockAcquire(&s->mutex);
            if (!s->in_use) {
                rc = strncpy_s(NameStr(s->data.name), NAMEDATALEN, slotname->data, NAMEDATALEN - 1);
                securec_check(rc, "\0", "\0");
                NameStr(s->data.name)[NAMEDATALEN - 1] = '\0';
                s->data.restart_lsn = latestValidRecord;
                SET_SLOT_PERSISTENCY(s->data, RS_PERSISTENT);
                s->data.database = InvalidOid;
                s->in_use = true;
                s->just_dirtied = true;
                s->dirty = true;
                SpinLockRelease(&s->mutex);
                slot = s;
                resetslot = true;
                break;
            }
            SpinLockRelease(&s->mutex);
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    if (!resetslot) {
        ereport(WARNING,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                 errmsg("could not reset replication slot at the end of recovery"),
                 errhint("Delete replication slot or increase g_instance.attr.attr_storage.max_replication_slots.")));
    }

    if (slot != NULL) {
        CreateSlotOnDisk(slot);
    }

    CheckPointReplicationSlots();
    ReplicationSlotsComputeRequiredLSN(NULL);
}

void inline XlogUpMinRecovPointSwitchChk(XLogRecPtr lsn)
{
    if (g_instance.comm_cxt.predo_cxt.pre_enable_switch !=
        g_instance.attr.attr_storage.enable_update_max_page_flush_lsn) {
        ereport(WARNING, (errmsg("XlogUpMinRecovPointSwitchChk lsn %X/%X, local minRecoveryPoint %X/%X, "
                                 "ControlFile minRecoveryPoint %X/%X, updateMinRecoveryPointSwitch:%u change to :%u",
                                 (uint32)(lsn >> 32), (uint32)lsn, (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32),
                                 (uint32)t_thrd.xlog_cxt.minRecoveryPoint,
                                 (uint32)(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint >> 32),
                                 (uint32)(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint),
                                 g_instance.comm_cxt.predo_cxt.pre_enable_switch,
                                 g_instance.attr.attr_storage.enable_update_max_page_flush_lsn)));
        g_instance.comm_cxt.predo_cxt.pre_enable_switch = g_instance.attr.attr_storage.enable_update_max_page_flush_lsn;
    }
}

/*
 * Advance minRecoveryPoint in control file.
 *
 * If we crash during recovery, we must reach this point again before the
 * database is consistent.
 *
 * If 'force' is true, 'lsn' argument is ignored. Otherwise, minRecoveryPoint
 * is only updated if it's not already greater than or equal to 'lsn'.
 */
void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force)
{
    /* Quick check using our local copy of the variable */
    if (!t_thrd.xlog_cxt.updateMinRecoveryPoint || (!force && XLByteLE(lsn, t_thrd.xlog_cxt.minRecoveryPoint))) {
        return;
    }

    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

    /* update local copy */
    t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
    SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);

    /*
     * An invalid minRecoveryPoint means that we need to recover all the WAL,
     * i.e., we're doing crash recovery.  We never modify the control file's
     * value in that case, so we can short-circuit future checks here too.
     */
    if (t_thrd.xlog_cxt.minRecoveryPoint == 0) {
        t_thrd.xlog_cxt.updateMinRecoveryPoint = false;
    } else if (force || XLByteLT(t_thrd.xlog_cxt.minRecoveryPoint, lsn)) {
        /* use volatile pointer to prevent code rearrangement */
        volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
        XLogRecPtr newMinRecoveryPoint;

        /*
         * To avoid having to update the control file too often, we update it
         * all the way to the last record being replayed, even though 'lsn'
         * would suffice for correctness.  This also allows the 'force' case
         * to not need a valid 'lsn' value.
         *
         * Another important reason for doing it this way is that the passed
         * 'lsn' value could be bogus, i.e., past the end of available WAL, if
         * the caller got it from a corrupted heap page.  Accepting such a
         * value as the min recovery point would prevent us from coming up at
         * all.  Instead, we just log a warning and continue with recovery.
         * (See also the comments about corrupt LSNs in XLogFlush.)
         */
        SpinLockAcquire(&xlogctl->info_lck);
        newMinRecoveryPoint = xlogctl->lastReplayedEndRecPtr;
        SpinLockRelease(&xlogctl->info_lck);

        if (!force && XLByteLT(newMinRecoveryPoint, lsn) && !enable_heap_bcm_data_replication()) {
            ereport(DEBUG1, (errmsg("xlog min recovery request %X/%X is past current point %X/%X", (uint32)(lsn >> 32),
                                    (uint32)lsn, (uint32)(newMinRecoveryPoint >> 32), (uint32)newMinRecoveryPoint)));
        }

        /* update control file */
        if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, newMinRecoveryPoint)) {
            t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = newMinRecoveryPoint;
            UpdateControlFile();
            t_thrd.xlog_cxt.minRecoveryPoint = newMinRecoveryPoint;
            SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);
            ereport(DEBUG1,
                    (errmsg("updated min recovery point to %X/%X", (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32),
                            (uint32)t_thrd.xlog_cxt.minRecoveryPoint)));
        }
    }
    LWLockRelease(ControlFileLock);
}

void XLogWaitFlush(XLogRecPtr recptr)
{
    /*
     * During REDO, we are reading not writing WAL.
     *
     * Test XLogInsertAllowed() instead of RecoveryInProgress(). XLogInsertAllowed()
     * allows WAL writing at the end of recovery by setting a thread local variable
     * if this thread local variable is not set, it returns RecoveryInProgress() 
     *
     * The checkpointer acts this way as well - when it tries to write
     * end-of-recovery checkpoint, this function should indeed flush
     *
     * During recovery, if the caller thread does not set allow XLogInsert, only
     * update minRecoveryPoint and return
     */
    if (!XLogInsertAllowed()) {
        if (g_supportHotStandby) {
            UpdateMinRecoveryPoint(recptr, false);
        }
        return;
    }

    volatile XLogRecPtr flushTo = gs_compare_and_swap_u64(&g_instance.wal_cxt.flushResult, 0, 0);

    while (XLByteLT(flushTo, recptr)) {
        if (!g_instance.wal_cxt.isWalWriterUp) {
            XLogSelfFlush();
        } else if (LWLockAcquireOrWait(g_instance.wal_cxt.walFlushWaitLock->l.lock, LW_EXCLUSIVE)) {
            PGSemaphoreLock(&g_instance.wal_cxt.walFlushWaitLock->l.sem, true);
            LWLockRelease(g_instance.wal_cxt.walFlushWaitLock->l.lock);
        }
        flushTo = pg_atomic_barrier_read_u64(&g_instance.wal_cxt.flushResult);
    }

    return;
}

void XLogWaitBufferInit(XLogRecPtr recptr)
{
    volatile XLogRecPtr sentTo = gs_compare_and_swap_u64(&g_instance.wal_cxt.sentResult, 0, 0);
    while (XLByteLT(sentTo, recptr)) {
        if (!g_instance.wal_cxt.isWalWriterUp) {
            XLogSelfFlush();
        } else if (LWLockAcquireOrWait(g_instance.wal_cxt.walBufferInitWaitLock->l.lock, LW_EXCLUSIVE)) {
            PGSemaphoreLock(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem, true);
            LWLockRelease(g_instance.wal_cxt.walBufferInitWaitLock->l.lock);
        }
        sentTo = gs_compare_and_swap_u64(&g_instance.wal_cxt.sentResult, 0, 0);
    }

    return;
}

static void XLogFlushCore(XLogRecPtr WriteRqstPtr)
{
    START_CRIT_SECTION();

    XLogwrtRqst WriteRqst;

    WriteRqst.Write = WriteRqstPtr;
    WriteRqst.Flush = WriteRqstPtr;
    XLogWrite(WriteRqst, false);
    END_CRIT_SECTION();

    pg_memory_barrier();

    /* wake up walsenders now that we've released heavily contended locks */
    WalSndWakeupProcessRequests();

    /*
     * Wake up waiting commit threads to check on the latest LSN flushed.
     */
    (void)pg_atomic_exchange_u64((uint64 *)&g_instance.wal_cxt.flushResult,
        t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult.Flush);

    /*
     * Notifity commit agents to wake up and check latest flushResult
     */
    WakeupWalSemaphore(&g_instance.wal_cxt.walFlushWaitLock->l.sem);
}

/*
 * Flush xlog, but without specifying exactly where to flush to.
 *
 * We normally flush only completed blocks; but if there is nothing to do on
 * that basis, we check for unflushed async commits in the current incomplete
 * block, and flush through the latest one of those.  Thus, if async commits
 * are not being used, we will flush complete blocks only.	We can guarantee
 * that async commits reach disk after at most three cycles; normally only
 * one or two.	(When flushing complete blocks, we allow XLogWrite to write
 * "flexibly", meaning it can stop at the end of the buffer ring; this makes a
 * difference only with very high load or long wal_writer_delay, but imposes
 * one extra cycle for the worst case for async commits.)
 *
 * This routine is invoked periodically by the background walwriter process.
 *
 * Returns TRUE if we flushed anything.
 */
bool XLogBackgroundFlush(void)
{
    XLogRecPtr WriteRqstPtr = InvalidXLogRecPtr;
    XLogRecPtr InitializeRqstPtr = InvalidXLogRecPtr;
    int start_entry_idx, curr_entry_idx, next_entry_idx, entry_idx;
    volatile WALInsertStatusEntry *start_entry_ptr;
    volatile WALInsertStatusEntry *curr_entry_ptr;
    volatile WALInsertStatusEntry *next_entry_ptr;
    WALInsertStatusEntry *status_entry_ptr;
    uint32 entry_count = 0;

    /*    
     * During REDO, we are reading not writing WAL.
     *
     * Test XLogInsertAllowed(), not RecoveryInProgress. XLogInsertAllowed()
     * allows WAL writing at the end of recovery by setting a thread local variable
     * if this thread local variable is not set, it returns RecoveryInProgress() 
     *
     * The checkpointer acts this way as well - when it tries to write
     * end-of-recovery checkpoint, this function should indeed flush.
     */
    if (!XLogInsertAllowed()) {
        return false;
    }

    /*
     *  Entry #:                  g_instance.wal_cxt.walInsertStatusTable
     *                            +---------------+--------+-------+
     *  0:                        |               |        |       |
     *                            +---------------+--------+-------+
     *                            ......
     *                            +---------------+--------+-------+
     *  *lastWalStatusEntry+1:    |               |        |COPIED | <--- while loop start here till hit a hole or
     *                            +---------------+--------+-------+      and older LRC
     *                            ......                    COPIED
     *                            +---------------+--------+-------+
     *  prev_entry:               |   endLSN(N-1) |LRC(N-1)|COPIED |  <== curr_entry_ptr <--- flush till here
     *                            +---------------+--------+-------+
     *  curr_entry_idx:           |   endLSN(N)   |LRC(N)  |NOT_CP |  <== next_entry_ptr
     *                            +---------------+--------+-------+
     *  next_entry_idx:           |   endLSN(N+1) |LRC(N+1)|COPIED |
     *                            +---------------+--------+-------+
     *                            ......
     *                            +---------------+--------+-------+
     *  WAL_INSERT_STATUS_ENTRIES:|               |        |       |
     *  - 1                       +---------------+--------+-------+
     *
     */

    start_entry_idx = GET_NEXT_STATUS_ENTRY(g_instance.wal_cxt.lastWalStatusEntryFlushed);
    start_entry_ptr = &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(start_entry_idx)];

    /*
     * Bail out if the very first entry is not copied.
     */
    if (start_entry_ptr->status != WAL_COPIED) {
        return false;
    }

    next_entry_idx = start_entry_idx;
    next_entry_ptr = start_entry_ptr;
    do {
        curr_entry_ptr = next_entry_ptr;
        curr_entry_idx = next_entry_idx;
        next_entry_idx = GET_NEXT_STATUS_ENTRY(curr_entry_idx);
        next_entry_ptr = &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(next_entry_idx)];
        entry_count++;

        Assert(curr_entry_ptr->status == WAL_COPIED);

        /*
         * Flush if we have accumulate enough bytes or till the LSN in the entry before
         * an entry associated with the first uncopied record found in the current loop.
         */
        if (next_entry_ptr->status == WAL_NOT_COPIED) {
            break;
        }
    } while (((curr_entry_ptr->LRC + 1) & 0x7FFFFFFF) == next_entry_ptr->LRC);

    g_instance.wal_cxt.lastWalStatusEntryFlushed = curr_entry_idx;
    g_instance.wal_cxt.lastLRCScanned = curr_entry_ptr->LRC;

    WriteRqstPtr = (XLogRecPtr)pg_atomic_barrier_read_u64(&curr_entry_ptr->endLSN);

#ifdef WAL_DEBUG
    if (u_sess->attr.attr_storage.XLOG_DEBUG) {
        ereport(LOG,
            (errmsg("xlog bg flush request %X/%X; write %X/%X; flush %X/%X",
                (uint32)(WriteRqstPtr >> 32),
                (uint32)WriteRqstPtr,
                (uint32)(t_thrd.xlog_cxt.LogwrtResult->Write >> 32),
                (uint32)t_thrd.xlog_cxt.LogwrtResult->Write,
                (uint32)(t_thrd.xlog_cxt.LogwrtResult->Flush >> 32),
                (uint32)t_thrd.xlog_cxt.LogwrtResult->Flush)));
    }
#endif

    XLogFlushCore(WriteRqstPtr);

    g_instance.wal_cxt.lastLRCFlushed = curr_entry_ptr->LRC;

    /*
     * Update LRC and status in the status entry that is already flushed (from start to current entry).
     * All WAL insert threads need to examine both of the LRC and the status
     * fiels before copying its endLSN and set status to WAL_COPIED in this
     * entry. This is to ensure it truly owns this entry, i.e., the writer thread
     * finishes updating the entry.
     */
    entry_idx = start_entry_idx - 1;
    do {
        /*
         * Increment entry_idx by 1 and wrap the result around WAL_INSERT_STATUS_ENTRIES.
         */
        entry_idx = GET_NEXT_STATUS_ENTRY(entry_idx);
        status_entry_ptr = &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(entry_idx)];
        status_entry_ptr->LRC = (status_entry_ptr->LRC + WAL_INSERT_STATUS_ENTRIES) & 0x7FFFFFFF;
        /* Reset the status to WAL_NOT_COPIED for the flushed entries. */
        status_entry_ptr->status = WAL_NOT_COPIED;
    } while (entry_idx != curr_entry_idx);

    InitializeRqstPtr = g_instance.wal_cxt.flushResult - g_instance.wal_cxt.flushResult % XLOG_BLCKSZ;

    if (InitializeRqstPtr != InvalidXLogRecPtr && XLByteLT(g_instance.wal_cxt.sentResult, InitializeRqstPtr)) {
        AdvanceXLInsertBuffer<false>(InitializeRqstPtr, false);

        (void)pg_atomic_exchange_u64((uint64 *)&g_instance.wal_cxt.sentResult, InitializeRqstPtr);

        WakeupWalSemaphore(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem);
    }

    return true;
}

/*
 * This function calls XLogBackgroundFlush ONCE when no one else is
 * doing it
 * The caller of this function should loop on this function to make
 * sure the LSN the caller waiting on is indeed flushed
 */
static void XLogSelfFlush(void)
{
    /*
     * The current callers of this function all check whether
     * the WAL writer is present, but as a generic function,
     * it should also does the check.
     */
    if (g_instance.wal_cxt.isWalWriterUp) {
        return;
    }

    /*
     * We only try self-flush when nobody else is already doing it
     */
    if (LWLockAcquireOrWait(WALWriteLock, LW_EXCLUSIVE)) {
        XLogBackgroundFlush();
        LWLockRelease(WALWriteLock);
    }

    return;
}

/*
 * This function is called by CopyXLogRecordToWAL or CopyXLogRecordToWALForGroup
 * when large records ( > wal buffer size ) are copied to WAL.
 *
 * During copying such a large record, we will wrap around the wal buffer and
 * access the page the large record starts from at least twice. When an agent
 * hits the start page again, the agent needs to initiate a full wal buffer
 * flush or it will get stuck - it has no more buffer page to use but
 * the wal writer cannot flush for this agent because the wal writer should only
 * flush when a status entry is updated but this agent cannot update its status
 * entry before it finishes copying its record.
 * 
 * This function should not change the wal copy status entries. 
 *
 * numHitsOnStartPage - the 2nd hit on the start page needs to wait for all
 *                      potential concurrent flushes to finish while the
 *                      subsequent hits on the start page don't 
 * CurrPos - The requested LSN
 * currLRC - need this to calculate the previous LRC for use to wait for
 */
static void XLogSelfFlushWithoutStatus(int numHitsOnStartPage,
		XLogRecPtr currPos, int currLRC)
{
    /*
     * I only needs to wait for the wal writer or other agents'
     * flushes (when wal writer is absent) to finish when I hit
     * the start page the 2nd time within one WAL copy because
     * after that the wal flush is blocked by me
     */
    if (numHitsOnStartPage == 2) {
        int prevLRC = (currLRC - 1) & 0x7FFFFFFF;
        while (g_instance.wal_cxt.lastLRCFlushed != prevLRC) {
            if (!g_instance.wal_cxt.isWalWriterUp) {
                XLogSelfFlush();
            }
            pg_usleep(1);
        }
    }

    /*
     * To this point, we are the one holding up the wal writer or
     * anyone that does the flush but we will still get the write
     * lock to be safe since acquiring a lock without contention
     * doesn't hurt performance here
     */
    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
    XLogFlushCore(currPos);
    LWLockRelease(WALWriteLock);
    
    XLogRecPtr initializeRqstPtr = currPos - currPos % XLOG_BLCKSZ;
    AdvanceXLInsertBuffer<false>(initializeRqstPtr, false);
    (void)pg_atomic_exchange_u64((uint64 *)&g_instance.wal_cxt.sentResult, initializeRqstPtr);
    WakeupWalSemaphore(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem);
}

/*
 * Test whether XLOG data has been flushed up to (at least) the given position.
 *
 * Returns true if a flush is still needed.  (It may be that someone else
 * is already in process of flushing that far, however.)
 */
bool XLogNeedsFlush(XLogRecPtr record)
{
    /*
     * During recovery, we don't flush WAL but update minRecoveryPoint
     * instead. So "needs flush" is taken to mean whether minRecoveryPoint
     * would need to be updated.
     */
    if (RecoveryInProgress()) {
        /* Quick exit if already known updated */
        if (XLByteLE(record, t_thrd.xlog_cxt.minRecoveryPoint) || !t_thrd.xlog_cxt.updateMinRecoveryPoint) {
            return false;
        }

        /*
         * Update local copy of minRecoveryPoint. But if the lock is busy,
         * just return a conservative guess.
         */
        if (!LWLockConditionalAcquire(ControlFileLock, LW_SHARED)) {
            return true;
        }
        t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
        SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);
        LWLockRelease(ControlFileLock);

        /*
         * An invalid minRecoveryPoint means that we need to recover all the
         * WAL, i.e., we're doing crash recovery.  We never modify the control
         * file's value in that case, so we can short-circuit future checks
         * here too.
         */
        if (t_thrd.xlog_cxt.minRecoveryPoint == 0) {
            t_thrd.xlog_cxt.updateMinRecoveryPoint = false;
        }

        /* check again */
        if (XLByteLE(record, t_thrd.xlog_cxt.minRecoveryPoint) || !t_thrd.xlog_cxt.updateMinRecoveryPoint) {
            return false;
        } else {
            return true;
        }
    }

    /* Quick exit if already known flushed */
    if (XLByteLE(record, t_thrd.xlog_cxt.LogwrtResult->Flush)) {
        return false;
    }

    /* read LogwrtResult and update local state */
    {
        /* use volatile pointer to prevent code rearrangement */
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        *t_thrd.xlog_cxt.LogwrtResult = xlogctl->LogwrtResult;
        SpinLockRelease(&xlogctl->info_lck);
    }

    /* check again */
    if (XLByteLE(record, t_thrd.xlog_cxt.LogwrtResult->Flush)) {
        return false;
    }

    return true;
}

/*
 * Create a new XLOG file segment, or open a pre-existing one.
 *
 * log, seg: identify segment to be created/opened.
 *
 * *use_existent: if TRUE, OK to use a pre-existing file (else, any
 * pre-existing file will be deleted).	On return, TRUE if a pre-existing
 * file was used.
 *
 * use_lock: if TRUE, acquire ControlFileLock while moving file into
 * place.  This should be TRUE except during bootstrap log creation.  The
 * caller must *not* hold the lock at call.
 *
 * Returns FD of opened file.
 *
 * Note: errors here are ERROR not PANIC because we might or might not be
 * inside a critical section (eg, during checkpoint there is no reason to
 * take down the system on failure).  They will promote to PANIC if we are
 * in a critical section.
 */
int XLogFileInit(XLogSegNo logsegno, bool *use_existent, bool use_lock)
{
    char path[MAXPGPATH];
    char tmppath[MAXPGPATH];
    char *zbuffer = NULL;
    char zbuffer_raw[XLOG_BLCKSZ + MAXIMUM_ALIGNOF];
    XLogSegNo installed_segno;
    int max_advance;
    int fd;
    int nbytes;
    errno_t rc = EOK;

    gstrace_entry(GS_TRC_ID_XLogFileInit);
    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                    (uint32)((logsegno) / XLogSegmentsPerXLogId), (uint32)((logsegno) % XLogSegmentsPerXLogId));
    securec_check_ss(rc, "", "");

    // Try to use existent file (checkpoint maker may have created it already)
    if (*use_existent) {
        fd = BasicOpenFile(path, O_RDWR | PG_BINARY | (unsigned int)get_sync_bit(u_sess->attr.attr_storage.sync_method),
                           S_IRUSR | S_IWUSR);
        if (fd < 0) {
            if (errno != ENOENT) {
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, logsegno))));
            }
        } else {
            gstrace_exit(GS_TRC_ID_XLogFileInit);
            return fd;
        }
    }

    /*
     * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
     * another process is doing the same thing.  If so, we will end up
     * pre-creating an extra log segment.  That seems OK, and better than
     * holding the lock throughout this lengthy process.
     */
    ereport(DEBUG2, (errmsg("creating and filling new WAL file")));

    rc = snprintf_s(tmppath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/xlogtemp.%lu", gs_thread_self());
    securec_check_ss(rc, "\0", "\0");

    unlink(tmppath);

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", tmppath)));
    }

    /*
     * Zero-fill the file.	We have to do this the hard way to ensure that all
     * the file space has really been allocated --- on platforms that allow
     * "holes" in files, just seeking to the end doesn't allocate intermediate
     * space.  This way, we know that we have all the space and (after the
     * fsync below) that all the indirect blocks are down on disk.	Therefore,
     * fdatasync(2) or O_DSYNC will be sufficient to sync future writes to the
     * log file.
     *
     * Note: ensure the buffer is reasonably well-aligned; this may save a few
     * cycles transferring data to the kernel.
     */
    zbuffer = (char *)MAXALIGN(zbuffer_raw);
    rc = memset_s(zbuffer, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
    securec_check(rc, "\0", "\0");
    for (nbytes = 0; (uint32)nbytes < XLogSegSize; nbytes += XLOG_BLCKSZ) {
        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_WAL_INIT_WRITE);
        if ((int)write(fd, zbuffer, XLOG_BLCKSZ) != (int)XLOG_BLCKSZ) {
            int save_errno = errno;

            // If we fail to make the file, delete it to release disk space
            unlink(tmppath);
            close(fd);
            /* if write didn't set errno, assume problem is no disk space */
            errno = save_errno ? save_errno : ENOSPC;

            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    }

    pgstat_report_waitevent(WAIT_EVENT_WAL_INIT_SYNC);
    if (pg_fsync(fd) != 0) {
        int save_errno = errno;
        close(fd);
        errno = save_errno;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmppath)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
    }

    /*
     * Now move the segment into place with its final name.
     *
     * If caller didn't want to use a pre-existing file, get rid of any
     * pre-existing file.  Otherwise, cope with possibility that someone else
     * has created the file while we were filling ours: if so, use ours to
     * pre-create a future log segment.
     */
    installed_segno = logsegno;
    max_advance = XLOGfileslop;
    if (!InstallXLogFileSegment(&installed_segno, (const char *)tmppath, *use_existent, &max_advance, use_lock)) {
        /*
         * No need for any more future segments, or InstallXLogFileSegment()
         * failed to rename the file into place. If the rename failed, opening
         * the file below will fail.
         */
        unlink(tmppath);
    }

    /* Set flag to tell caller there was no existent file */
    *use_existent = false;

    /* Now open original target segment (might not be file I just made) */
    fd = BasicOpenFile(path, O_RDWR | PG_BINARY | (unsigned int)get_sync_bit(u_sess->attr.attr_storage.sync_method),
                       S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                          XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, logsegno))));
    }

    ereport(DEBUG2, (errmsg("done creating and filling new WAL file:%s", path)));

    gstrace_exit(GS_TRC_ID_XLogFileInit);
    return fd;
}

void XLogFileCutPage(char *buffer, uint32 bufLen, uint32 cpyLen)
{
    if (bufLen != 0) {
        uint32 offsetLen = cpyLen % bufLen;
        uint32 truncateLen = bufLen - offsetLen;
        errno_t rc = EOK;
        ereport(LOG, (errcode_for_file_access(),
                      errmsg("XLogFileCutPage begin, bufLen:%u, cpyLen:%u, offsetLen:%u, truncateLen:%u", bufLen,
                             cpyLen, offsetLen, truncateLen)));
        rc = memset_s((buffer + offsetLen), truncateLen, 0, truncateLen);
        securec_check(rc, "\0", "\0");
    }
}

bool PreInitXlogFileForStandby(XLogRecPtr requestLsn)
{
    /* The last processed xlog lsn. */
    static XLogRecPtr lastWriteLsn = 0;
    /* The next segment to be created. */
    static XLogSegNo nextSegNo = 0;

    if (requestLsn < lastWriteLsn) { /* switch over. need reset */
        ereport(WARNING,
            (errmsg("PreInitXlogFileForStandby need reset. requestLsn: %X/%X; lastWriteLsn: %X/%X",
                (uint32)(requestLsn >> 32),
                (uint32)requestLsn,
                (uint32)(lastWriteLsn >> 32),
                (uint32)lastWriteLsn)));
        lastWriteLsn = requestLsn;
        nextSegNo = 0;
    } else if (requestLsn < lastWriteLsn + XLOG_SEG_SIZE) {
        /* If the requestLsn is not more than one segement, skip! */
        return false;
    } else {
        lastWriteLsn = requestLsn;
    }

    /* Start to pre-init xlog segment. */
    XLogSegNo reqSegNo;
    XLByteToPrevSeg(requestLsn, reqSegNo);
    /* If the request pos is bigger, start from the segment next to requestLsn. */
    if (nextSegNo <= reqSegNo) {
        nextSegNo = reqSegNo + 1;
    }
    XLogSegNo targetSegNo = reqSegNo + g_instance.attr.attr_storage.advance_xlog_file_num;
    for (; nextSegNo <= targetSegNo; nextSegNo++) {
        bool use_existent = true;
        int lf = XLogFileInit(nextSegNo, &use_existent, true);
        if (!use_existent)
            ereport(DEBUG1,
                (errmsg("PreInitXlogFileForStandby new nextSegNo: %X/%X ThisTimeLineID: %u, RecoveryTargetTLI: %u",
                    (uint32)(nextSegNo >> 32),
                    (uint32)nextSegNo,
                    t_thrd.shemem_ptr_cxt.XLogCtl->ThisTimeLineID,
                    t_thrd.shemem_ptr_cxt.XLogCtl->RecoveryTargetTLI)));
        if (lf >= 0)
            close(lf);
    }

    return true;
}

void PreInitXlogFileForPrimary(int advance_xlog_file_num)
{
    XLogSegNo startSegNo, nextSegNo, target;
    int lf;
    bool use_existent;

    g_xlog_stat_shared->walAuxWakeNum++;

    startSegNo = g_instance.wal_cxt.globalEndPosSegNo + 1;
    target = startSegNo + advance_xlog_file_num - 1;
    for (nextSegNo = startSegNo; nextSegNo <= target; nextSegNo++) {
        use_existent = true;
        lf = XLogFileInit(nextSegNo, &use_existent, true);
        if (lf >= 0) {
            close(lf);
        }
    }
}
/*
 * Create a new XLOG file segment by copying a pre-existing one.
 *
 * log, seg: identify segment to be created.
 *
 * srcTLI, srclog, srcseg: identify segment to be copied (could be from
 *		a different timeline)
 *
 * Currently this is only used during recovery, and so there are no locking
 * considerations.	But we should be just as tense as XLogFileInit to avoid
 * emplacing a bogus file.
 */
static void XLogFileCopy(char *path, char *tmppath)
{
    char buffer[XLOG_BLCKSZ];
    int srcfd;
    int fd;
    uint32 nbytes;
    errno_t rc = EOK;

    Assert(path);
    Assert(tmppath);
    /*
     * Open the source file
     */
    srcfd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
    if (srcfd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
    }
    /*
     * Copy into a temp file name.
     */
    unlink(tmppath);

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        close(srcfd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", tmppath)));
    }

    /*
     * Do the data copying.
     */
    for (nbytes = 0; nbytes < (uint32)XLogSegSize; nbytes += sizeof(buffer)) {
        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_WAL_COPY_READ);
        if ((int)read(srcfd, buffer, sizeof(buffer)) != (int)sizeof(buffer)) {
            close(srcfd);
            close(fd);
            if (errno != 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", path)));
            } else {
                ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("not enough data in file \"%s\"", path)));
            }
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_WAL_COPY_WRITE);
        if ((int)write(fd, buffer, sizeof(buffer)) != (int)sizeof(buffer)) {
            int save_errno = errno;

            close(srcfd);
            close(fd);
            /*
             * If we fail to make the file, delete it to release disk space
             */
            unlink(tmppath);
            /* if write didn't set errno, assume problem is no disk space */
            errno = save_errno ? save_errno : ENOSPC;
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    }

    rc = memset_s(buffer, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
    securec_check(rc, "\0", "\0");

    pgstat_report_waitevent(WAIT_EVENT_WAL_COPY_SYNC);
    if (pg_fsync(fd) != 0) {
        close(srcfd);
        close(fd);
        ereport(data_sync_elevel(ERROR),
                (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmppath)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        close(srcfd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tmppath)));
    }

    close(srcfd);
}

static void XLogFileTruncateInner(char *path, int fd, uint32 targetPageOff, char *buffer, int bufLen)
{
    uint32 nbytes;
    errno_t rc = EOK;
    uint32 startBytes = targetPageOff;
    if (lseek(fd, (off_t)targetPageOff, SEEK_SET) < 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("lseek2:could not seek in log file %s to offset %u: %m", path, targetPageOff)));
    }
    for (nbytes = startBytes; nbytes < (uint32)XLogSegSize; nbytes += bufLen) {
        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_WAL_COPY_WRITE);
        if ((int)write(fd, buffer, bufLen) != bufLen) {
            close(fd);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", path)));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (nbytes == startBytes) {
            rc = memset_s(buffer, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
            securec_check(rc, "\0", "\0");
        }
    }
    pgstat_report_waitevent(WAIT_EVENT_WAL_COPY_SYNC);
    if (pg_fsync(fd) != 0) {
        close(fd);
        ereport(data_sync_elevel(ERROR), (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", path)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static void XLogFileTruncate(char *path, XLogRecPtr RecPtr)
{
    char buffer[XLOG_BLCKSZ];
    int fd;
    XLogRecPtr targetPagePtr;
    uint32 targetPageOff;
    targetPagePtr = RecPtr - RecPtr % XLOG_BLCKSZ;
    targetPageOff = (targetPagePtr % XLogSegSize);
    fd = BasicOpenFile(path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
    if (lseek(fd, (off_t)targetPageOff, SEEK_SET) < 0) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not seek in log file %s to offset %u: %m", path, targetPageOff)));
    }
    if ((int)read(fd, buffer, sizeof(buffer)) != (int)sizeof(buffer)) {
        close(fd);
        if (errno != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", path)));
        else
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("not enough data in file \"%s\"", path)));
    }
    XLogFileCutPage(buffer, XLOG_BLCKSZ, RecPtr % XLogSegSize);
    XLogFileTruncateInner(path, fd, targetPageOff, buffer, (int)(sizeof(buffer)));
    close(fd);
}

/*
 * Install a new XLOG segment file as a current or future log segment.
 *
 * This is used both to install a newly-created segment (which has a temp
 * filename while it's being created) and to recycle an old segment.
 *
 * *segno: identify segment to install as (or first possible target).
 * When find_free is TRUE, this is modified on return to indicate the
 * actual installation location or last segment searched.
 *
 * tmppath: initial name of file to install.  It will be renamed into place.
 *
 * find_free: if TRUE, install the new segment at the first empty segno
 * number at or after the passed numbers.  If FALSE, install the new segment
 * exactly where specified, deleting any existing segment file there.
 *
 * *max_advance: maximum number of segno slots to advance past the starting
 * point.  Fail if no free slot is found in this range.  On return, reduced
 * by the number of slots skipped over.  (Irrelevant, and may be NULL,
 * when find_free is FALSE.)
 *
 * use_lock: if TRUE, acquire ControlFileLock while moving file into
 * place.  This should be TRUE except during bootstrap log creation.  The
 * caller must *not* hold the lock at call.
 *
 * Returns TRUE if the file was installed successfully.  FALSE indicates that
 * max_advance limit was exceeded, or an error occurred while renaming the
 * file into place.
 */
static bool InstallXLogFileSegment(XLogSegNo *segno, const char *tmppath, bool find_free, int *max_advance,
                                   bool use_lock)
{
    char path[MAXPGPATH];
    struct stat stat_buf;
    errno_t errorno = EOK;

    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((*segno) / XLogSegmentsPerXLogId), (uint32)((*segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    /*
     * We want to be sure that only one process does this at a time.
     */
    if (use_lock) {
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    }

    if (!find_free) {
        /* Force installation: get rid of any pre-existing segment file */
        unlink(path);
    } else {
        /* Find a free slot to put it in */
        while (stat(path, &stat_buf) == 0) {
            if (*max_advance <= 0) {
                /* Failed to find a free slot within specified range */
                if (use_lock) {
                    LWLockRelease(ControlFileLock);
                }
                return false;
            }
            (*segno)++;
            (*max_advance)--;
            errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X",
                                 t_thrd.xlog_cxt.ThisTimeLineID, (uint32)((*segno) / XLogSegmentsPerXLogId),
                                 (uint32)((*segno) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");
        }
    }

    if (durable_rename(tmppath, path, LOG) != 0) {
        if (use_lock) {
            LWLockRelease(ControlFileLock);
        }

        /* durable_rename already emitted log message */
        return false;
    }

    if (use_lock) {
        LWLockRelease(ControlFileLock);
    }

    return true;
}

/*
 * Open a pre-existing logfile segment for writing.
 */
int XLogFileOpen(XLogSegNo segno)
{
    char path[MAXPGPATH];
    int fd;
    errno_t errorno = EOK;

    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    fd = BasicOpenFile(path, O_RDWR | PG_BINARY | (unsigned int)get_sync_bit(u_sess->attr.attr_storage.sync_method),
                       S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open xlog file \"%s\" (log segment %s): %m", path,
                                                          XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
    }

    return fd;
}

/*
 * Open a logfile segment for reading (during recovery).
 *
 * If source = XLOG_FROM_ARCHIVE, the segment is retrieved from archive.
 * Otherwise, it's assumed to be already available in pg_xlog.
 */
static int XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli, int source, bool notfoundOk)
{
    char xlogfname[MAXFNAMELEN];
    char activitymsg[MAXFNAMELEN + 16];
    char path[MAXPGPATH];
    int fd;
    errno_t errorno = EOK;

    errorno = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", tli,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    switch (source) {
        case XLOG_FROM_ARCHIVE:
            /* Report recovery progress in PS display */
            errorno = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "waiting for %s",
                                 xlogfname);
            securec_check_ss(errorno, "", "");
            set_ps_display(activitymsg, false);

            t_thrd.xlog_cxt.restoredFromArchive = RestoreArchivedFile(path, xlogfname, "RECOVERYXLOG", XLogSegSize);
            if (!t_thrd.xlog_cxt.restoredFromArchive) {
                return -1;
            }
            break;

        case XLOG_FROM_PG_XLOG:
        case XLOG_FROM_STREAM:
            errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", tli,
                                 (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");
            t_thrd.xlog_cxt.restoredFromArchive = false;
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("invalid XLogFileRead source %d", source)));
            break;
    }

    /*
     * If the segment was fetched from archival storage, replace the existing
     * xlog segment (if any) with the archival version.
     */
    if (source == XLOG_FROM_ARCHIVE) {
        KeepFileRestoredFromArchive((const char *)path, (const char *)xlogfname);

        // Set path to point at the new file in pg_xlog.
        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlogfname);
        securec_check_ss(errorno, "", "");
    }

    fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
    if (fd >= 0) {
        /* Success! */
        t_thrd.xlog_cxt.curFileTLI = tli;

        /* Report recovery progress in PS display */
        errorno = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "recovering %s", xlogfname);
        securec_check_ss(errorno, "", "");
        set_ps_display(activitymsg, false);

        /* Track source of data in assorted state variables */
        t_thrd.xlog_cxt.readSource = source;
        t_thrd.xlog_cxt.XLogReceiptSource = source;
        /* In FROM_STREAM case, caller tracks receipt time, not me */
        if (source != XLOG_FROM_STREAM) {
            t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
        }

        return fd;
    }
    if (errno != ENOENT || !notfoundOk) { /* unexpected failure? */
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                          XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
    }
    return -1;
}

/*
 * Open a logfile segment for reading (during recovery).
 * This version searches for the segment with any TLI listed in expectedTLIs.
 */
static int XLogFileReadAnyTLI(XLogSegNo segno, int emode, uint32 sources)
{
    char path[MAXPGPATH];
    ListCell *cell = NULL;
    int fd = -1;
    errno_t errorno = EOK;

    /*
     * Loop looking for a suitable timeline ID: we might need to read any of
     * the timelines listed in expectedTLIs.
     *
     * We expect curFileTLI on entry to be the TLI of the preceding file in
     * sequence, or 0 if there was no predecessor.	We do not allow curFileTLI
     * to go backwards; this prevents us from picking up the wrong file when a
     * parent timeline extends to higher segment numbers than the child we
     * want to read.
     */
    foreach (cell, t_thrd.xlog_cxt.expectedTLIs) {
        TimeLineID tli = (TimeLineID)lfirst_int(cell);

        if (tli < t_thrd.xlog_cxt.curFileTLI) {
            break; /* don't bother looking at too-old TLIs */
        }

        if (sources & XLOG_FROM_ARCHIVE) {
            fd = XLogFileRead(segno, emode, tli, XLOG_FROM_ARCHIVE, true);
            if (fd != -1) {
                ereport(DEBUG1, (errmsg("got WAL segment from archive")));
                return fd;
            }
        }

        if (sources & XLOG_FROM_PG_XLOG) {
            fd = XLogFileRead(segno, emode, tli, XLOG_FROM_PG_XLOG, true);
            if (fd != -1) {
                return fd;
            }
        }
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", t_thrd.xlog_cxt.recoveryTargetTLI,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errno = ENOENT;
    ereport(emode, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                      XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
    return -1;
}

/*
 * Close the current logfile segment for writing.
 */
static void XLogFileClose(void)
{
    Assert(t_thrd.xlog_cxt.openLogFile >= 0);

    /*
     * WAL segment files will not be re-read in normal operation, so we advise
     * the OS to release any cached pages.	But do not do so if WAL archiving
     * or streaming is active, because archiver and walsender process could
     * use the cache to read the WAL segment.
     */
#if defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
    if (!XLogIsNeeded()) {
        (void)posix_fadvise(t_thrd.xlog_cxt.openLogFile, 0, 0, POSIX_FADV_DONTNEED);
    }
#endif

    if (close(t_thrd.xlog_cxt.openLogFile)) {
        ereport(PANIC, (errcode_for_file_access(),
                        errmsg("could not close log file %s: %m",
                               XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.openLogSegNo))));
    }

    t_thrd.xlog_cxt.openLogFile = -1;
}

/*
 * A file was restored from the archive under a temporary filename (path),
 * and now we want to keep it. Rename it under the permanent filename in
 * in pg_xlog (xlogfname), replacing any existing file with the same name.
 */
static void KeepFileRestoredFromArchive(const char *path, const char *xlogfname)
{
    char xlogfpath[MAXPGPATH];
    bool reload = false;
    struct stat statbuf;
    errno_t errorno = EOK;

    errorno = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlogfname);
    securec_check_ss(errorno, "", "");

    if (stat(xlogfpath, &statbuf) == 0) {
        char oldpath[MAXPGPATH] = {0};

        errorno = memset_s(oldpath, MAXPGPATH, 0, MAXPGPATH);
        securec_check(errorno, "", "");

#ifdef WIN32
        /*
         * On Windows, if another process (e.g a walsender process) holds
         * the file open in FILE_SHARE_DELETE mode, unlink will succeed,
         * but the file will still show up in directory listing until the
         * last handle is closed, and we cannot rename the new file in its
         * place until that. To avoid that problem, rename the old file to
         * a temporary name first. Use a counter to create a unique
         * filename, because the same file might be restored from the
         * archive multiple times, and a walsender could still be holding
         * onto an old deleted version of it.
         */
        errorno = snprintf_s(oldpath, MAXPGPATH, MAXPGPATH - 1, "%s.deleted%u", xlogfpath,
                             t_thrd.xlog_cxt.deletedcounter++);
        securec_check_ss(errorno, "", "");

        if (rename(xlogfpath, oldpath) != 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not rename file \"%s\" to \"%s\": %m", xlogfpath, oldpath)));
        }
#else
        errorno = strncpy_s(oldpath, MAXPGPATH, xlogfpath, MAXPGPATH - 1);
        securec_check(errorno, "", "");
#endif
        if (unlink(oldpath) != 0) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", xlogfpath)));
        }
        reload = true;
    }

    durable_rename(path, xlogfpath, ERROR);

    // Create .done file forcibly to prevent the restored segment from being archived again later.
    XLogArchiveForceDone(xlogfname);

    /*
     * If the existing file was replaced, since walsenders might have it
     * open, request them to reload a currently-open segment. This is only
     * required for WAL segments, walsenders don't hold other files open,
     * but there's no harm in doing this too often, and we don't know what
     * kind of a file we're dealing with here.
     */
    if (reload) {
        WalSndRqstFileReload();
    }

    /* Signal walsender that new WAL has arrived */
    if (AllowCascadeReplication()) {
        WalSndWakeup();
    }
}

/*
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, fill "path" with its complete path (note that this will be
 * a temp file name that doesn't follow the normal naming convention), and
 * return TRUE.
 * If not successful, fill "path" with the name of the normal on-line file
 * (which may or may not actually exist, but we'll try to use it), and return FALSE.
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery. If the file size is not
 * known, set expectedSize = 0.
 */
static bool RestoreArchivedFile(char *path, const char *xlogfname, const char *recovername, off_t expectedSize)
{
    char xlogpath[MAXPGPATH];
    char xlogRestoreCmd[MAXPGPATH];
    char lastRestartPointFname[MAXPGPATH];
    char *dp = NULL;
    char *endp = NULL;
    const char *sp = NULL;
    int rc = 0;
    bool signaled = false;
    struct stat stat_buf;
    XLogSegNo restartSegNo;
    errno_t errorno = EOK;

    /* In standby mode, restore_command might not be supplied */
    if (t_thrd.xlog_cxt.recoveryRestoreCommand == NULL) {
        goto not_available;
    }

    /*
     * When doing archive recovery, we always prefer an archived log file even
     * if a file of the same name exists in XLOGDIR.  The reason is that the
     * file in XLOGDIR could be an old, un-filled or partly-filled version
     * that was copied and restored as part of backing up $PGDATA.
     *
     * We could try to optimize this slightly by checking the local copy
     * lastchange timestamp against the archived copy, but we have no API to
     * do this, nor can we guarantee that the lastchange timestamp was
     * preserved correctly when we copied to archive. Our aim is robustness,
     * so we elect not to do this.
     *
     * If we cannot obtain the log file from the archive, however, we will try
     * to use the XLOGDIR file if it exists.  This is so that we can make use
     * of log segments that weren't yet transferred to the archive.
     *
     * Notice that we don't actually overwrite any files when we copy back
     * from archive because the recoveryRestoreCommand may inadvertently
     * restore inappropriate xlogs, or they may be corrupt, so we may wish to
     * fallback to the segments remaining in current XLOGDIR later. The
     * copy-from-archive filename is always the same, ensuring that we don't
     * run out of disk space on long recoveries.
     */
    errorno = snprintf_s(xlogpath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", recovername);
    securec_check_ss(errorno, "", "");
    /*
     * Make sure there is no existing file named recovername.
     */
    if (stat(xlogpath, &stat_buf) != 0) {
        if (errno != ENOENT) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", xlogpath)));
        }
    } else {
        if (unlink(xlogpath) != 0) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", xlogpath)));
        }
    }

    /*
     * Calculate the archive file cutoff point for use during log shipping
     * replication. All files earlier than this point can be deleted from the
     * archive, though there is no requirement to do so.
     *
     * We initialise this with the filename of an InvalidXLogRecPtr, which
     * will prevent the deletion of any WAL files from the archive because of
     * the alphabetic sorting property of WAL filenames.
     *
     * Once we have successfully located the redo pointer of the checkpoint
     * from which we start recovery we never request a file prior to the redo
     * pointer of the last restartpoint. When redo begins we know that we have
     * successfully located it, so there is no need for additional status
     * flags to signify the point when we can begin deleting WAL files from
     * the archive.
     */
    if (t_thrd.xlog_cxt.InRedo) {
        XLByteToSeg(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, restartSegNo);
        errorno = snprintf_s(lastRestartPointFname, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X",
                             t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID,
                             (uint32)((restartSegNo) / XLogSegmentsPerXLogId),
                             (uint32)((restartSegNo) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");

        /* we shouldn't need anything earlier than last restart point */
        Assert(strcmp(lastRestartPointFname, xlogfname) <= 0);
    } else {
        errorno = snprintf_s(lastRestartPointFname, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X", 0,
                             (uint32)((0L) / XLogSegmentsPerXLogId), (uint32)((0L) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");
    }

    /*
     * construct the command to be executed
     */
    dp = xlogRestoreCmd;
    endp = xlogRestoreCmd + MAXPGPATH - 1;
    *endp = '\0';

    for (sp = t_thrd.xlog_cxt.recoveryRestoreCommand; *sp; sp++) {
        if (*sp == '%') {
            switch (sp[1]) {
                case 'p':
                    /* %p: relative path of target file */
                    sp++;
                    errorno = strncpy_s(dp, MAXPGPATH - (dp - xlogRestoreCmd), xlogpath, strlen(xlogpath));
                    securec_check(errorno, "", "");
                    make_native_path(dp);
                    dp += strlen(dp);
                    break;
                case 'f':
                    /* %f: filename of desired file */
                    sp++;
                    errorno = strncpy_s(dp, MAXPGPATH - (dp - xlogRestoreCmd), xlogfname, strlen(xlogfname));
                    securec_check(errorno, "", "");
                    dp += strlen(dp);
                    break;
                case 'r':
                    /* %r: filename of last restartpoint */
                    sp++;
                    errorno = strncpy_s(dp, MAXPGPATH - (dp - xlogRestoreCmd), lastRestartPointFname,
                                        strlen(lastRestartPointFname));
                    securec_check(errorno, "", "");
                    dp += strlen(dp);
                    break;
                case '%':
                    /* convert %% to a single % */
                    sp++;
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
                default:
                    /* otherwise treat the % as not special */
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
            }
        } else {
            if (dp < endp) {
                *dp++ = *sp;
            }
        }
    }
    *dp = '\0';

    ereport(DEBUG3, (errmsg_internal("executing restore command \"%s\"", xlogRestoreCmd)));

    // Check signals before restore command and reset afterwards.
    PreRestoreCommand();

    // Copy xlog from archival storage to XLOGDIR
    rc = gs_popen_security(xlogRestoreCmd);

    PostRestoreCommand();

    if (rc == 0) {
        // command apparently succeeded, but let's make sure the file is
        // really there now and has the correct size.
        if (stat(xlogpath, &stat_buf) == 0) {
            if (expectedSize > 0 && stat_buf.st_size != expectedSize) {
                /*
                 * If we find a partial file in standby mode, we assume it's
                 * because it's just being copied to the archive, and keep
                 * trying.
                 *
                 * Otherwise treat a wrong-sized file as FATAL to ensure the
                 * DBA would notice it, but is that too strong? We could try
                 * to plow ahead with a local copy of the file ... but the
                 * problem is that there probably isn't one, and we'd
                 * incorrectly conclude we've reached the end of WAL and we're
                 * done recovering ...
                 */
                int elevel = (t_thrd.xlog_cxt.StandbyMode && stat_buf.st_size < expectedSize) ? DEBUG1 : FATAL;
                ereport(elevel, (errmsg("archive file \"%s\" has wrong size: %lu instead of %lu", xlogfname,
                                        (unsigned long)stat_buf.st_size, (unsigned long)expectedSize)));
                return false;
            } else {
                ereport(LOG, (errmsg("restored log file \"%s\" from archive", xlogfname)));
                errorno = strcpy_s(path, MAXPGPATH, xlogpath);
                securec_check(errorno, "", "");
                return true;
            }
        } else if (errno != ENOENT) { /* stat failed */
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", xlogpath)));
        }
    }

    /*
     * Remember, we rollforward UNTIL the restore fails so failure here is
     * just part of the process... that makes it difficult to determine
     * whether the restore failed because there isn't an archive to restore,
     * or because the administrator has specified the restore program
     * incorrectly.  We have to assume the former.
     *
     * However, if the failure was due to any sort of signal, it's best to
     * punt and abort recovery.  (If we "return false" here, upper levels will
     * assume that recovery is complete and start up the database!) It's
     * essential to abort on child SIGINT and SIGQUIT, because per spec
     * system() ignores SIGINT and SIGQUIT while waiting; if we see one of
     * those it's a good bet we should have gotten it too.
     *
     * On SIGTERM, assume we have received a fast shutdown request, and exit
     * cleanly. It's pure chance whether we receive the SIGTERM first, or the
     * child process. If we receive it first, the signal handler will call
     * proc_exit, otherwise we do it here. If we or the child process received
     * SIGTERM for any other reason than a fast shutdown request, postmaster
     * will perform an immediate shutdown when it sees us exiting
     * unexpectedly.
     *
     * Per the Single Unix Spec, shells report exit status > 128 when a called
     * command died on a signal.  Also, 126 and 127 are used to report
     * problems such as an unfindable command; treat those as fatal errors
     * too.
     */
    if (WIFSIGNALED(rc) && WTERMSIG(rc) == SIGTERM) {
        proc_exit(1);
    }

    signaled = WIFSIGNALED(rc) || WEXITSTATUS(rc) > 125;

    ereport(signaled ? FATAL : DEBUG2,
            (errmsg("could not restore file \"%s\" from archive: return code %d", xlogfname, rc)));

not_available:

    /*
     * if an archived file is not available, there might still be a version of
     * this file in XLOGDIR, so return that as the filename to open.
     *
     * In many recovery scenarios we expect this to fail also, but if so that
     * just means we've reached the end of WAL.
     */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlogfname);
    securec_check_ss(errorno, "", "");

    return false;
}

/*
 * Attempt to execute an external shell command during recovery.
 *
 * 'command' is the shell command to be executed, 'commandName' is a
 * human-readable name describing the command emitted in the logs. If
 * 'failOnSignal' is true and the command is killed by a signal, a FATAL
 * error is thrown. Otherwise a WARNING is emitted.
 *
 * This is currently used for recovery_end_command and archive_cleanup_command.
 */
static void ExecuteRecoveryCommand(char *command, char *commandName, bool failOnSignal)
{
    char xlogRecoveryCmd[MAXPGPATH];
    char lastRestartPointFname[MAXPGPATH];
    char *dp = NULL;
    char *endp = NULL;
    const char *sp = NULL;
    int rc = 0;
    bool signaled = false;
    XLogSegNo restartSegNo;
    errno_t errorno = EOK;

    Assert(command && commandName);

    /*
     * Calculate the archive file cutoff point for use during log shipping
     * replication. All files earlier than this point can be deleted from the
     * archive, though there is no requirement to do so.
     */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    XLByteToSeg(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, restartSegNo);

    errorno = snprintf_s(lastRestartPointFname, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X",
                         t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID,
                         (uint32)((restartSegNo) / XLogSegmentsPerXLogId),
                         (uint32)((restartSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");
    LWLockRelease(ControlFileLock);

    // construct the command to be executed
    dp = xlogRecoveryCmd;
    endp = xlogRecoveryCmd + MAXPGPATH - 1;
    *endp = '\0';

    for (sp = command; *sp; sp++) {
        if (*sp == '%') {
            switch (sp[1]) {
                case 'r':
                    /* %r: filename of last restartpoint */
                    sp++;
                    errorno = strncpy_s(dp, MAXPGPATH - (dp - xlogRecoveryCmd), lastRestartPointFname,
                                        strlen(lastRestartPointFname));
                    securec_check(errorno, "", "");
                    dp += strlen(dp);
                    break;
                case '%':
                    /* convert %% to a single % */
                    sp++;
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
                default:
                    /* otherwise treat the % as not special */
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
            }
        } else {
            if (dp < endp) {
                *dp++ = *sp;
            }
        }
    }
    *dp = '\0';

    ereport(DEBUG3, (errmsg_internal("executing %s \"%s\"", commandName, command)));

    // execute the constructed command
    rc = gs_popen_security(xlogRecoveryCmd);
    if (rc != 0) {
        /*
         * If the failure was due to any sort of signal, it's best to punt and
         * abort recovery. See also detailed comments on signals in RestoreArchivedFile().
         */
        signaled = WIFSIGNALED(rc) || WEXITSTATUS(rc) > 125;
        ereport((signaled && failOnSignal) ? FATAL : WARNING,
                (errmsg("%s \"%s\": return code %d", commandName, command, rc)));
    }
}

/*
 * Preallocate log files beyond the specified log endpoint.
 *
 * XXX this is currently extremely conservative, since it forces only one
 * future log segment to exist, and even that only if we are 75% done with
 * the current one.  This is only appropriate for very low-WAL-volume systems.
 * High-volume systems will be OK once they've built up a sufficient set of
 * recycled log segments, but the startup transient is likely to include
 * a lot of segment creations by foreground processes, which is not so good.
 */
static void PreallocXlogFiles(XLogRecPtr endptr)
{
    XLogSegNo _logSegNo;
    int lf;
    bool use_existent = false;

    gstrace_entry(GS_TRC_ID_PreallocXlogFiles);
    XLByteToPrevSeg(endptr, _logSegNo);
    if ((endptr - 1) % XLogSegSize >= (uint32)(0.75 * XLogSegSize)) {
        _logSegNo++;
        use_existent = true;
        lf = XLogFileInit(_logSegNo, &use_existent, true);
        close(lf);
        if (!use_existent) {
            t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_added++;
        }
    }

    gstrace_exit(GS_TRC_ID_PreallocXlogFiles);
}

/*
 * Throws an error if the given log segment has already been removed or
 * recycled. The caller should only pass a segment that it knows to have
 * existed while the server has been running, as this function always
 * succeeds if no WAL segments have been removed since startup.
 * 'tli' is only used in the error message.
 */
void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogSegNo lastRemovedSegNo;
    errno_t errorno = EOK;

    SpinLockAcquire(&xlogctl->info_lck);
    lastRemovedSegNo = xlogctl->lastRemovedSegNo;
    SpinLockRelease(&xlogctl->info_lck);

    if (segno <= lastRemovedSegNo) {
        char filename[MAXFNAMELEN];

        errorno = snprintf_s(filename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", tli,
                             (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");

        WalSegmemtRemovedhappened = true;
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("requested WAL segment %s has already been removed", filename)));
    }
}

/*
 * Return the last WAL segment removed, or 0 if no segment has been removed
 * since startup.
 *
 * NB: the result can be out of date arbitrarily fast, the caller has to deal
 * with that.
 */
XLogRecPtr XLogGetLastRemovedSegno(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr lastRemovedSegNo;

    SpinLockAcquire(&xlogctl->info_lck);
    lastRemovedSegNo = xlogctl->lastRemovedSegNo;
    SpinLockRelease(&xlogctl->info_lck);

    return lastRemovedSegNo;
}

static void remove_xlogtemp_files(void)
{
    DIR *dir = NULL;
    char fullpath[MAXPGPATH] = {0};
    struct dirent *de = NULL;
    struct stat st;
    errno_t errorno = EOK;

    if ((dir = opendir(XLOGDIR)) != NULL) {
        while ((de = readdir(dir)) != NULL) {
            /* Skip special stuff */
            if (strncmp(de->d_name, ".", 1) == 0 || strncmp(de->d_name, "..", 2) == 0) {
                continue;
            }

            if (!strstr(de->d_name, "xlogtemp")) {
                continue;
            }

            errorno = snprintf_s(fullpath, sizeof(fullpath), sizeof(fullpath) - 1, XLOGDIR "/%s", de->d_name);
            securec_check_ss(errorno, "\0", "\0");

            if (lstat(fullpath, &st) != 0) {
                if (errno != ENOENT) {
                    ereport(WARNING, (errmsg("could not stat file or directory : %s", fullpath)));
                }
                /* If the file went away while scanning, it's not an error. */
                continue;
            }

            if (S_ISDIR(st.st_mode)) {
                continue;
            }
            /* only handle the normal file */
            if (S_ISREG(st.st_mode)) {
                (void)unlink(fullpath);
            }
        }
        (void)closedir(dir);
    }
}

/*
 * Update the last removed segno pointer in shared memory, to reflect
 * that the given XLOG file has been removed.
 */
static void UpdateLastRemovedPtr(const char *filename)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    uint32 tli;
    uint32 log_temp = 0;
    uint32 seg_temp = 0;
    XLogSegNo segno;

    if (sscanf_s(filename, "%08X%08X%08X", &tli, &log_temp, &seg_temp) != 3) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not parse xlog segment file \"%s\"", filename)));
    }
    segno = (uint64)log_temp * XLogSegmentsPerXLogId + seg_temp;

    SpinLockAcquire(&xlogctl->info_lck);
    if (segno > xlogctl->lastRemovedSegNo) {
        xlogctl->lastRemovedSegNo = segno;
    }
    SpinLockRelease(&xlogctl->info_lck);
}

/*
 * Recycle or remove all log files older or equal to passed segno#
 *
 * endptr is current (or recent) end of xlog; this is used to determine
 * whether we want to recycle rather than delete no-longer-wanted log files.
 */
static void RemoveOldXlogFiles(XLogSegNo segno, XLogRecPtr endptr)
{
    if (segno == 0) {
        return;
    }

    DIR *xldir = NULL;
    struct dirent *xlde = NULL;
    char lastoff[MAXFNAMELEN];
    errno_t errorno = EOK;

    xldir = AllocateDir(XLOGDIR);
    if (xldir == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open transaction log directory \"%s\": %m", XLOGDIR)));
    }

    errorno = snprintf_s(lastoff, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");
    /* segno is about to remove, so (segno + 1) will be the oldest seg */
    g_instance.comm_cxt.predo_cxt.redoPf.oldest_segment = segno + 1;
    ereport(LOG, (errmsg("attempting to remove WAL segments older than log file %s", lastoff)));

    gstrace_entry(GS_TRC_ID_RemoveOldXlogFiles);
    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL) {
        /* Ignore files that are not XLOG segments */
        if (strlen(xlde->d_name) != 24 || strspn(xlde->d_name, "0123456789ABCDEF") != 24) {
            continue;
        }

        /*
         * We ignore the timeline part of the XLOG segment identifiers in
         * deciding whether a segment is still needed.	This ensures that we
         * won't prematurely remove a segment from a parent timeline. We could
         * probably be a little more proactive about removing segments of
         * non-parent timelines, but that would be a whole lot more
         * complicated.
         *
         * We use the alphanumeric sorting property of the filenames to decide
         * which ones are earlier than the lastoff segment.
         */
        if (strcmp(xlde->d_name + 8, lastoff + 8) <= 0) {
            if (XLogArchiveCheckDone(xlde->d_name)) {
                /* Update the last removed location in shared memory first */
                UpdateLastRemovedPtr((const char *)xlde->d_name);

                RemoveXlogFile(xlde->d_name, endptr);
            }
        }
    }
    FreeDir(xldir);
    gstrace_data(1, GS_TRC_ID_RemoveOldXlogFiles, TRC_DATA_FMT_DFLT, lastoff, MAXFNAMELEN);
    gstrace_exit(GS_TRC_ID_RemoveOldXlogFiles);
}

/*
 * Recycle or remove a log file that's no longer needed.
 *
 * endptr is current (or recent) end of xlog; this is used to determine
 * whether we want to recycle rather than delete no-longer-wanted log files.
 */
static void RemoveXlogFile(const char *segname, XLogRecPtr endptr)
{
    char path[MAXPGPATH];
#ifdef WIN32
    char newpath[MAXPGPATH];
#endif
    struct stat statbuf;
    XLogSegNo endLogSegNo;
    int max_advance;

    /*
     * Initialize info about where to try to recycle to.  We allow recycling
     * segments up to XLOGfileslop segments beyond the current XLOG location.
     */
    XLByteToPrevSeg(endptr, endLogSegNo);
    max_advance = XLOGfileslop;

    errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", segname);
    securec_check_ss(errorno, "", "");

    /*
     * Before deleting the file, see if it can be recycled as a future log
     * segment. Only recycle normal files, pg_standby for example can create
     * symbolic links pointing to a separate archive directory.
     */
    if (lstat(path, &statbuf) == 0 && S_ISREG(statbuf.st_mode) &&
        InstallXLogFileSegment(&endLogSegNo, (const char *)path, true, &max_advance, true)) {
        ereport(DEBUG2, (errmsg("recycled transaction log file \"%s\"", segname)));
        t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_recycled++;
        /* Needn't recheck that slot on future iterations */
        if (max_advance > 0) {
            endLogSegNo++;
            max_advance--;
        }
    } else {
        /* No need for any more future segments... */
        int rc;

        ereport(DEBUG2, (errmsg("removing transaction log file \"%s\"", segname)));

#ifdef WIN32
        /*
         * On Windows, if another process (e.g another backend) holds the file
         * open in FILE_SHARE_DELETE mode, unlink will succeed, but the file
         * will still show up in directory listing until the last handle is
         * closed. To avoid confusing the lingering deleted file for a live
         * WAL file that needs to be archived, rename it before deleting it.
         *
         * If another process holds the file open without FILE_SHARE_DELETE
         * flag, rename will fail. We'll try again at the next checkpoint.
         */
        errorno = snprintf_s(newpath, MAXPGPATH, MAXPGPATH - 1, "%s.deleted", path);
        securec_check_ss(errorno, "", "");

        if (rename(path, newpath) != 0) {
            ereport(LOG,
                    (errcode_for_file_access(), errmsg("could not rename old transaction log file \"%s\": %m", path)));
            return;
        }
        rc = unlink(newpath);
#else
        rc = unlink(path);
#endif
        if (rc != 0) {
            ereport(LOG,
                    (errcode_for_file_access(), errmsg("could not remove old transaction log file \"%s\": %m", path)));
            return;
        }
        t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_removed++;
    }
    XLogArchiveCleanup(segname);
}

/*
 * Verify whether pg_xlog and pg_xlog/archive_status exist.
 * If the latter does not exist, recreate it.
 *
 * It is not the goal of this function to verify the contents of these
 * directories, but to help in cases where someone has performed a cluster
 * copy for PITR purposes but omitted pg_xlog from the copy.
 *
 * We could also recreate pg_xlog if it doesn't exist, but a deliberate
 * policy decision was made not to.  It is fairly common for pg_xlog to be
 * a symlink, and if that was the DBA's intent then automatically making a
 * plain directory would result in degraded performance with no notice.
 */
static void ValidateXLOGDirectoryStructure(void)
{
    char path[MAXPGPATH];
    struct stat stat_buf;
    errno_t errorno = EOK;

    /* Check for pg_xlog; if it doesn't exist, error out */
    if (stat(XLOGDIR, &stat_buf) != 0 || !S_ISDIR(stat_buf.st_mode))
        ereport(FATAL, (errmsg("required WAL directory \"%s\" does not exist", XLOGDIR)));

    /* Check for archive_status */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status");
    securec_check_ss(errorno, "", "");

    if (stat(path, &stat_buf) == 0) {
        /* Check for weird cases where it exists but isn't a directory */
        if (!S_ISDIR(stat_buf.st_mode)) {
            ereport(FATAL, (errmsg("required WAL directory \"%s\" does not exist", path)));
        }
    } else {
        ereport(LOG, (errmsg("creating missing WAL directory \"%s\"", path)));
        if (mkdir(path, S_IRWXU) < 0) {
            ereport(FATAL, (errmsg("could not create missing directory \"%s\": %m", path)));
        }
    }
}

/*
 * Remove previous backup history files.  This also retries creation of
 * .ready files for any backup history files for which XLogArchiveNotify
 * failed earlier.
 */
static void CleanupBackupHistory(void)
{
    DIR *xldir = NULL;
    struct dirent *xlde = NULL;
    char path[MAXPGPATH];
    errno_t errorno = EOK;

    xldir = AllocateDir(XLOGDIR);
    if (xldir == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open transaction log directory \"%s\": %m", XLOGDIR)));
    }

    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL) {
        if (strlen(xlde->d_name) > 24 && strspn(xlde->d_name, "0123456789ABCDEF") == 24 &&
            strcmp(xlde->d_name + strlen(xlde->d_name) - strlen(".backup"), ".backup") == 0) {
            if (XLogArchiveCheckDone(xlde->d_name)) {
                ereport(DEBUG2, (errmsg("removing transaction log backup history file \"%s\"", xlde->d_name)));
                errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlde->d_name);
                securec_check_ss(errorno, "", "");

                unlink(path);
                XLogArchiveCleanup(xlde->d_name);
            }
        }
    }

    FreeDir(xldir);
}

inline static XLogReaderState *ReadNextRecordFromQueue(int emode)
{
    char *errormsg = NULL;
    bool readoldversion = false;
    extreme_rto::SPSCBlockingQueue *linequeue = extreme_rto::g_dispatcher->readLine.readPageThd->queue;
    XLogReaderState *xlogreader = NULL;
    do {
        xlogreader = (XLogReaderState *)extreme_rto::SPSCBlockingQueueTake(linequeue);
        if (!xlogreader->isDecode) {
            XLogRecord *record = (XLogRecord *)xlogreader->readRecordBuf;
            ;
            if (!DecodeXLogRecord(xlogreader, record, &errormsg, readoldversion)) {
                ereport(emode,
                        (errmsg("ReadNextRecordFromQueue %X/%X decode error, %s", (uint32)(xlogreader->EndRecPtr >> 32),
                                (uint32)(xlogreader->EndRecPtr), errormsg)));

                extreme_rto::RedoItem *item = extreme_rto::GetRedoItemPtr(xlogreader);

                extreme_rto::FreeRedoItem(item);

                xlogreader = NULL;
            }
        }

        if ((void *)xlogreader == (void *)&(extreme_rto::g_GlobalLsnForwarder.record) || 
            (void *)xlogreader == (void *)&(extreme_rto::g_cleanupMark.record)) {
            extreme_rto::StartupSendFowarder(extreme_rto::GetRedoItemPtr(xlogreader));
            xlogreader = NULL;
        }

        RedoInterruptCallBack();
    } while (xlogreader == NULL);

    return xlogreader;
}

static XLogRecord *ReadNextXLogRecord(XLogReaderState **xlogreaderptr, int emode)
{
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = ReadNextRecordFromQueue(emode);

    if ((void *)xlogreader != (void *)&(extreme_rto::g_redoEndMark.record)) {
        *xlogreaderptr = xlogreader;
        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.read_ptr = t_thrd.xlog_cxt.ReadRecPtr;
        /* Set up lastest valid record */
        latestValidRecord = t_thrd.xlog_cxt.ReadRecPtr;

        /* Great, got a record */
        record = (XLogRecord *)xlogreader->readRecordBuf;
        latestRecordCrc = record->xl_crc;
    } else {
        *xlogreaderptr = &extreme_rto::g_redoEndMark.record;
        if (t_thrd.startup_cxt.shutdown_requested) {
            proc_exit(0);
        }
    }
    return record;
}

/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is not NULL, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If no valid record is available, returns NULL, or fails if emode is PANIC.
 * (emode must be either PANIC, LOG). In standby mode, retries until a valid
 * record is available.
 *
 * The record is copied into readRecordBuf, so that on successful return,
 * the returned record pointer always points there.
 */
static XLogRecord *ReadRecord(XLogReaderState *xlogreader, XLogRecPtr RecPtr, int emode, bool fetching_ckpt,
                              bool readoldversion)
{
    XLogRecord *record = NULL;
    uint32 streamFailCount = 0;
    XLogPageReadPrivate *readprivate = (XLogPageReadPrivate *)xlogreader->private_data;

    /* Pass through parameters to XLogPageRead */
    readprivate->fetching_ckpt = fetching_ckpt;
    readprivate->emode = emode;
    readprivate->randAccess = !XLByteEQ(RecPtr, InvalidXLogRecPtr);

    /* This is the first try to read this page. */
    t_thrd.xlog_cxt.failedSources = 0;

    for (;;) {
        char *errormsg = NULL;

        record = XLogReadRecord(xlogreader, RecPtr, &errormsg, readoldversion);
        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.read_ptr = t_thrd.xlog_cxt.ReadRecPtr;

        if (((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
            readoldversion = false;
        }
        if (record == NULL) {
            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            /*
             * We only end up here without a message when XLogPageRead() failed
             * - in that case we already logged something.
             * In StandbyMode that only happens if we have been triggered, so
             * we shouldn't loop anymore in that case.
             */
            if (errormsg != NULL) {
                ereport(emode_for_corrupt_record(emode, XLByteEQ(RecPtr, InvalidXLogRecPtr) ? t_thrd.xlog_cxt.EndRecPtr
                                                                                            : RecPtr),
                        (errmsg_internal("%s", errormsg) /* already translated */));
            }
        } else if ((!timeLineInHistory(xlogreader->latestPageTLI, t_thrd.xlog_cxt.expectedTLIs)) &&
                   (!(g_instance.attr.attr_storage.IsRoachStandbyCluster && dummyStandbyMode))) {
            /* Check page TLI is one of the expected values. */
            /* Check page TLI is one of the expected values. */
            char fname[MAXFNAMELEN];
            XLogSegNo targetSegNo;
            uint32 offset;
            errno_t errorno = EOK;

            XLByteToSeg(xlogreader->latestPagePtr, targetSegNo);
            offset = xlogreader->latestPagePtr % XLogSegSize;

            errorno = snprintf_s(fname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", xlogreader->readPageTLI,
                                 (uint32)((targetSegNo) / XLogSegmentsPerXLogId),
                                 (uint32)((targetSegNo) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");

            ereport(emode_for_corrupt_record(emode,
                                             XLByteEQ(RecPtr, InvalidXLogRecPtr) ? t_thrd.xlog_cxt.EndRecPtr : RecPtr),
                    (errmsg("unexpected timeline ID %u in log segment %s, offset %u", xlogreader->latestPageTLI, fname,
                            offset)));
            record = NULL;
        }

        if (record != NULL) {
            /* Set up lastest valid record */
            latestValidRecord = t_thrd.xlog_cxt.ReadRecPtr;
            latestRecordCrc = record->xl_crc;
            /* Great, got a record */
            return record;
        } else {
            /* No valid record available from this source */
            if (streamFailCount < XLOG_STREAM_READREC_MAXTRY) {
                streamFailCount++;
                /* if xlog record failed retry 100 ms. */
                if (u_sess->attr.attr_storage.HaModuleDebug) {
                    ereport(LOG,
                            (errmsg("reread xlog record for %uth times at %X/%X", streamFailCount,
                                    (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), (uint32)t_thrd.xlog_cxt.ReadRecPtr)));
                }
                pg_usleep(XLOG_STREAM_READREC_INTERVAL);
                ProcTxnWorkLoad(true);
            } else {
                t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;
                if (u_sess->attr.attr_storage.HaModuleDebug) {
                    ereport(LOG,
                            (errmsg("xlog record failed failedsource %u at %X/%X", t_thrd.xlog_cxt.failedSources,
                                    (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), (uint32)t_thrd.xlog_cxt.ReadRecPtr)));
                }
            }

            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            /*
             * If archive recovery was requested, but we were still doing
             * crash recovery, switch to archive recovery and retry using the
             * offline archive. We have now replayed all the valid WAL in
             * pg_xlog, so we are presumably now consistent.
             *
             * We require that there's at least some valid WAL present in
             * pg_xlog, however (!fetch_ckpt). We could recover using the WAL
             * from the archive, even if pg_xlog is completely empty, but we'd
             * have no idea how far we'd have to replay to reach consistency.
             * So err on the safe side and give up.
             */
            if (!t_thrd.xlog_cxt.InArchiveRecovery && t_thrd.xlog_cxt.ArchiveRecoveryRequested && !fetching_ckpt) {
                ProcTxnWorkLoad(false);
                volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
                XLogRecPtr newMinRecoveryPoint;
                ereport(DEBUG1, (errmsg_internal("reached end of WAL in pg_xlog, entering archive recovery")));
                t_thrd.xlog_cxt.InArchiveRecovery = true;
                if (t_thrd.xlog_cxt.StandbyModeRequested) {
                    t_thrd.xlog_cxt.StandbyMode = true;
                }

                /* initialize minRecoveryPoint to this record */
                LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
                t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
                SpinLockAcquire(&xlogctl->info_lck);
                newMinRecoveryPoint = xlogctl->lastReplayedEndRecPtr;
                SpinLockRelease(&xlogctl->info_lck);
                if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, newMinRecoveryPoint)) {
                    t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = newMinRecoveryPoint;
                }

                /* update local copy */
                t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
                SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);

                UpdateControlFile();
                LWLockRelease(ControlFileLock);

                CheckRecoveryConsistency();

                ereport(LOG, (errmsg("update minrecovery point to %X/%X in archive recovery",
                                     (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32),
                                     (uint32)(t_thrd.xlog_cxt.minRecoveryPoint))));

                /*
                 * Before we retry, reset lastSourceFailed and currentSource
                 * so that we will check the archive next.
                 */
                t_thrd.xlog_cxt.failedSources = 0;
                continue;
            }
            ProcTxnWorkLoad(false);

            /* In standby mode, loop back to retry. Otherwise, give up. */
            if (t_thrd.xlog_cxt.StandbyMode && !dummyStandbyMode && !t_thrd.xlog_cxt.recoveryTriggered) {
                continue;
            } else {
                return NULL;
            }
        }
    }
}

int ParallelReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, bool readoldversion)
{
    int readLen;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;
    XLogPageHeader hdr;

    Assert((pageptr % XLOG_BLCKSZ) == 0);

    XLByteToSeg(pageptr, targetSegNo);
    targetPageOff = (pageptr % XLogSegSize);

    /* check whether we have all the requested data already */
    if (targetSegNo == state->readSegNo && targetPageOff == state->readOff && reqLen < (int)state->readLen) {
        return state->readLen;
    }

    /*
     * First, read the requested data length, but at least a short page header
     * so that we can validate it.
     */
    readLen = ParallelXLogPageRead(state, pageptr, Max(reqLen, (int)SizeOfXLogShortPHD), state->currRecPtr, 
        &state->readPageTLI);
    if (readLen < 0) {
        goto err;
    }

    Assert(readLen <= XLOG_BLCKSZ);

    /* Do we have enough data to check the header length? */
    if (readLen <= (int)SizeOfXLogShortPHD) {
        goto err;
    }

    Assert(readLen >= reqLen);

    hdr = (XLogPageHeader)state->readBuf;

    if (hdr->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    /* still not enough */
    if (readLen < (int)XLogPageHeaderSize(hdr)) {
        readLen = ParallelXLogPageRead(state, pageptr, XLogPageHeaderSize(hdr), state->currRecPtr, 
            &state->readPageTLI);
        if (readLen < 0) {
            goto err;
        }
    }

    /*
     * Now that we know we have the full header, validate it.
     */
    if (!ValidXLogPageHeader(state, pageptr, hdr, readoldversion)) {
        goto err;
    }

    /* update read state information */
    state->readSegNo = targetSegNo;
    state->readOff = targetPageOff;
    state->readLen = readLen;

    return readLen;

err:
    XLogReaderInvalReadState(state);
    return -1;
}

XLogRecord *ParallelReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg, bool readoldversion,
                               bool doDecode)
{
    XLogRecord *record = NULL;
    XLogRecPtr targetPagePtr;
    bool randAccess = false;
    uint32 len, total_len;
    uint32 targetRecOff;
    uint32 pageHeaderSize;
    bool gotheader = false;
    int readOff;
    errno_t errorno = EOK;

    /*
     * randAccess indicates whether to verify the previous-record pointer of
     * the record we're reading.  We only do this if we're reading
     * sequentially, which is what we initially assume.
     */
    randAccess = false;

    /* reset error state */
    *errormsg = NULL;
    state->errormsg_buf[0] = '\0';

    ResetDecoder(state);

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if (XLByteEQ(RecPtr, InvalidXLogRecPtr)) {
        /* No explicit start point; read the record after the one we just read */
        RecPtr = state->EndRecPtr;

        if (readoldversion && (XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ) < SizeOfXLogRecordOld) {
            NextLogPage(RecPtr);
        }

        if (XLByteEQ(state->ReadRecPtr, InvalidXLogRecPtr))
            randAccess = true;

        /*
         * If at page start, we must skip over the page header using xrecoff check.
         */
        if (0 == RecPtr % XLogSegSize) {
            XLByteAdvance(RecPtr, SizeOfXLogLongPHD);
        } else if (0 == RecPtr % XLOG_BLCKSZ) {
            XLByteAdvance(RecPtr, SizeOfXLogShortPHD);
        }
    } else {
        /*
         * Caller supplied a position to start at.
         *
         * In this case, the passed-in record pointer should already be
         * pointing to a valid record starting position.
         */
        Assert(XRecOffIsValid(RecPtr));
        randAccess = true;
    }

    state->currRecPtr = RecPtr;

    targetPagePtr = RecPtr - RecPtr % XLOG_BLCKSZ;
    targetRecOff = RecPtr % XLOG_BLCKSZ;

    /*
     * Read the page containing the record into state->readBuf. Request
     * enough byte to cover the whole record header, or at least the part of
     * it that fits on the same page.
     */
    readOff = ParallelReadPageInternal(state, targetPagePtr, Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ),
                                       readoldversion);
    if (readOff < 0) {
        report_invalid_record(state, "read xlog page failed at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /* The page has been read. Check the XLOG version again. */
    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    /*
     * ReadPageInternal always returns at least the page header, so we can
     * examine it now.
     */
    pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
    if (targetRecOff == 0) {
        /*
         * At page start, so skip over page header.
         */
        RecPtr += pageHeaderSize;
        targetRecOff = pageHeaderSize;
    } else if (targetRecOff < pageHeaderSize) {
        report_invalid_record(state, "invalid record offset at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    if ((((XLogPageHeader)state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) && targetRecOff == pageHeaderSize) {
        report_invalid_record(state, "contrecord is requested by %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /* ReadPageInternal has verified the page header */
    Assert((int)pageHeaderSize <= readOff);

    /*
     * Read the record length.
     *
     * NB: Even though we use an XLogRecord pointer here, the whole record
     * header might not fit on this page. xl_tot_len is the first field of the
     * struct, so it must be on this page (the records are MAXALIGNed), but we
     * cannot access any other fields until we've verified that we got the
     * whole header.
     */
    record = (XLogRecord *)(state->readBuf + RecPtr % XLOG_BLCKSZ);
    total_len = record->xl_tot_len;

    /*
     * If the whole record header is on this page, validate it immediately.
     * Otherwise do just a basic sanity check on xl_tot_len, and validate the
     * rest of the header after reading it from the next page.  The xl_tot_len
     * check is necessary here to ensure that we enter the "Need to reassemble
     * record" code path below; otherwise we might fail to apply
     * ValidXLogRecordHeader at all.
     */
    if (targetRecOff <= XLOG_BLCKSZ - (readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord)) {
        if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
            goto err;
        gotheader = true;
    } else {
        /* more validation should be done here */
        if (total_len < (readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord) || total_len >= XLogRecordMaxSize) {
            report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                                  (uint32)RecPtr, (uint32)(readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord),
                                  total_len);
            goto err;
        }
        gotheader = false;
    }

    /*
     * Enlarge readRecordBuf as needed.
     */
    if (total_len > state->readRecordBufSize && !allocate_recordbuf(state, total_len)) {
        /* We treat this as a "bogus data" condition */
        report_invalid_record(state, "record length %u at %X/%X too long", total_len, (uint32)(RecPtr >> 32),
                              (uint32)RecPtr);
        goto err;
    }

    len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
    if (total_len > len) {
        /* Need to reassemble record */
        char *contdata = NULL;
        XLogPageHeader pageHeader;
        char *buffer = NULL;
        uint32 gotlen;
        errno_t errorno = EOK;

        readOff = ParallelReadPageInternal(state, targetPagePtr, XLOG_BLCKSZ, readoldversion);
        if (readOff < 0) {
            goto err;
        }

        /* Copy the first fragment of the record from the first page. */
        errorno = memcpy_s(state->readRecordBuf, len, state->readBuf + RecPtr % XLOG_BLCKSZ, len);
        securec_check_c(errorno, "\0", "\0");
        buffer = state->readRecordBuf + len;
        gotlen = len;

        do {
            /* Calculate pointer to beginning of next page */
            XLByteAdvance(targetPagePtr, XLOG_BLCKSZ);

            /* Wait for the next page to become available */
            readOff = ParallelReadPageInternal(state, targetPagePtr,
                                               Min(total_len - gotlen + SizeOfXLogShortPHD, XLOG_BLCKSZ),
                                               readoldversion);

            if (readOff < 0)
                goto err;

            Assert((int)SizeOfXLogShortPHD <= readOff);

            /* Check that the continuation on next page looks valid */
            pageHeader = (XLogPageHeader)state->readBuf;
            if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD)) {
                report_invalid_record(state, "there is no contrecord flag at %X/%X", (uint32)(RecPtr >> 32),
                                      (uint32)RecPtr);
                goto err;
            }

            /*
             * Cross-check that xlp_rem_len agrees with how much of the record
             * we expect there to be left.
             */
            if (pageHeader->xlp_rem_len == 0 || total_len != (pageHeader->xlp_rem_len + gotlen)) {
                report_invalid_record(state, "invalid contrecord length %u at %X/%X", pageHeader->xlp_rem_len,
                                      (uint32)(RecPtr >> 32), (uint32)RecPtr);
                goto err;
            }

            /* Append the continuation from this page to the buffer */
            pageHeaderSize = XLogPageHeaderSize(pageHeader);

            if (readOff < (int)pageHeaderSize)
                readOff = ParallelReadPageInternal(state, targetPagePtr, pageHeaderSize, readoldversion);

            Assert((int)pageHeaderSize <= readOff);

            contdata = (char *)state->readBuf + pageHeaderSize;
            len = XLOG_BLCKSZ - pageHeaderSize;
            if (pageHeader->xlp_rem_len < len)
                len = pageHeader->xlp_rem_len;

            if (readOff < (int)(pageHeaderSize + len))
                readOff = ParallelReadPageInternal(state, targetPagePtr, pageHeaderSize + len, readoldversion);

            errorno = memcpy_s(buffer, total_len - gotlen, (char *)contdata, len);
            securec_check_c(errorno, "", "");
            buffer += len;
            gotlen += len;

            /* If we just reassembled the record header, validate it. */
            if (!gotheader) {
                record = (XLogRecord *)state->readRecordBuf;
                if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
                    goto err;
                gotheader = true;
            }
        } while (gotlen < total_len);

        Assert(gotheader);

        record = (XLogRecord *)state->readRecordBuf;
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
        state->ReadRecPtr = RecPtr;
        state->EndRecPtr = targetPagePtr;
        XLByteAdvance(state->EndRecPtr, (pageHeaderSize + MAXALIGN(pageHeader->xlp_rem_len)));
    } else {
        /* Wait for the record data to become available */
        readOff = ParallelReadPageInternal(state, targetPagePtr, Min(targetRecOff + total_len, XLOG_BLCKSZ),
                                           readoldversion);
        if (readOff < 0) {
            goto err;
        }

        /* Record does not cross a page boundary */
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        state->EndRecPtr = RecPtr;
        XLByteAdvance(state->EndRecPtr, MAXALIGN(total_len));

        state->ReadRecPtr = RecPtr;
        errorno = memcpy_s(state->readRecordBuf, total_len, record, total_len);
        securec_check_c(errorno, "\0", "\0");
        record = (XLogRecord *)state->readRecordBuf;
    }

    /*
     * Special processing if it's an XLOG SWITCH record
     */

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if ((readoldversion ? ((XLogRecordOld *)record)->xl_rmid : ((XLogRecord *)record)->xl_rmid) == RM_XLOG_ID &&
        (readoldversion ? ((XLogRecordOld *)record)->xl_info : ((XLogRecord *)record)->xl_info) == XLOG_SWITCH) {
        /* Pretend it extends to end of segment */
        state->EndRecPtr += XLogSegSize - 1;
        state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
    }

    if (doDecode) {
        if (DecodeXLogRecord(state, record, errormsg, readoldversion)) {
            return record;
        } else
            return NULL;
    } else {
        return record;
    }

err:

    /*
     * Invalidate the read state. We might read from a different source after
     * failure.
     */
    XLogReaderInvalReadState(state);

    if (state->errormsg_buf[0] != '\0')
        *errormsg = state->errormsg_buf;

    return NULL;
}


static void UpdateMinrecoveryInAchive()
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr newMinRecoveryPoint;

    /* initialize minRecoveryPoint to this record */
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
    SpinLockAcquire(&xlogctl->info_lck);
    newMinRecoveryPoint = xlogctl->lastReplayedEndRecPtr;
    SpinLockRelease(&xlogctl->info_lck);
    if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, newMinRecoveryPoint)) {
        t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = newMinRecoveryPoint;
    }

    /* update local copy */
    t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
    g_instance.comm_cxt.predo_cxt.redoPf.min_recovery_point = t_thrd.xlog_cxt.minRecoveryPoint;

    UpdateControlFile();
    LWLockRelease(ControlFileLock);

    ereport(LOG, (errmsg("update minrecovery point to %X/%X in archive recovery",
                         (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32),
                         (uint32)(t_thrd.xlog_cxt.minRecoveryPoint))));
}

XLogRecord *XLogParallelReadNextRecord(XLogReaderState *xlogreader)
{
    XLogRecord *record = NULL;

    /* This is the first try to read this page. */
    t_thrd.xlog_cxt.failedSources = 0;
    for (;;) {
        char *errormsg = NULL;

        record = ParallelReadRecord(xlogreader, InvalidXLogRecPtr, &errormsg, false, false);
        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.read_ptr = t_thrd.xlog_cxt.ReadRecPtr;

        if (record == NULL) {
            /*
             * We only end up here without a message when XLogPageRead() failed
             * - in that case we already logged something.
             * In StandbyMode that only happens if we have been triggered, so
             * we shouldn't loop anymore in that case.
             */
            if (errormsg != NULL)
                ereport(emode_for_corrupt_record(LOG, t_thrd.xlog_cxt.EndRecPtr),
                        (errmsg_internal("%s", errormsg) /* already translated */));
        }

        /*
         * Check page TLI is one of the expected values.
         */
        else if ((!timeLineInHistory(xlogreader->latestPageTLI, t_thrd.xlog_cxt.expectedTLIs)) &&
                 (!(g_instance.attr.attr_storage.IsRoachStandbyCluster && dummyStandbyMode))) {
            char fname[MAXFNAMELEN];
            XLogSegNo targetSegNo;
            int32 offset;
            errno_t errorno = EOK;

            XLByteToSeg(xlogreader->latestPagePtr, targetSegNo);
            offset = xlogreader->latestPagePtr % XLogSegSize;

            errorno = snprintf_s(fname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", xlogreader->readPageTLI,
                                 (uint32)((targetSegNo) / XLogSegmentsPerXLogId),
                                 (uint32)((targetSegNo) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");

            ereport(emode_for_corrupt_record(LOG, t_thrd.xlog_cxt.EndRecPtr),
                    (errmsg("unexpected timeline ID %u in log segment %s, offset %u", xlogreader->latestPageTLI, fname,
                            offset)));
            record = NULL;
        }

        if (record != NULL) {
            /* Set up lastest valid record */
            latestValidRecord = t_thrd.xlog_cxt.ReadRecPtr;
            latestRecordCrc = record->xl_crc;
            ADD_ABNORMAL_POSITION(9);
            /* Great, got a record */
            return record;
        } else {
            /* No valid record available from this source */
            t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            /*
             * If archive recovery was requested, but we were still doing
             * crash recovery, switch to archive recovery and retry using the
             * offline archive. We have now replayed all the valid WAL in
             * pg_xlog, so we are presumably now consistent.
             *
             * We require that there's at least some valid WAL present in
             * pg_xlog, however (!fetch_ckpt). We could recover using the WAL
             * from the archive, even if pg_xlog is completely empty, but we'd
             * have no idea how far we'd have to replay to reach consistency.
             * So err on the safe side and give up.
             */
            if (!t_thrd.xlog_cxt.InArchiveRecovery && t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
                t_thrd.xlog_cxt.InArchiveRecovery = true;
                if (t_thrd.xlog_cxt.StandbyModeRequested)
                    t_thrd.xlog_cxt.StandbyMode = true;
                /* construct a minrecoverypoint, update LSN */
                UpdateMinrecoveryInAchive();
                CheckRecoveryConsistency();
                /*
                 * Before we retry, reset lastSourceFailed and currentSource
                 * so that we will check the archive next.
                 */
                t_thrd.xlog_cxt.failedSources = 0;
                continue;
            }

            /* In standby mode, loop back to retry. Otherwise, give up. */
            if (t_thrd.xlog_cxt.StandbyMode && !t_thrd.xlog_cxt.recoveryTriggered)
                continue;
            else
                return NULL;
        }
    }
}

/*
 * Try to read a timeline's history file.
 *
 * If successful, return the list of component TLIs (the given TLI followed by
 * its ancestor TLIs).	If we can't find the history file, assume that the
 * timeline has no parents, and return a list of just the specified timeline
 * ID.
 */
static List *readTimeLineHistory(TimeLineID targetTLI)
{
    List *result = NIL;
    char path[MAXPGPATH];
    char histfname[MAXFNAMELEN];
    char fline[MAXPGPATH];
    FILE *fd = NULL;
    bool fromArchive = false;
    errno_t errorno = EOK;

    /* Timeline 1 does not have a history file, so no need to check */
    if (targetTLI == 1) {
        return list_make1_int((int)targetTLI);
    }

    if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
        errorno = snprintf_s(histfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X.history", targetTLI);
        securec_check_ss(errorno, "", "");
        fromArchive = RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0);
    } else {
        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X.history", targetTLI);
        securec_check_ss(errorno, "", "");
    }

    fd = AllocateFile(path, "r");
    if (fd == NULL) {
        if (errno != ENOENT) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }
        /* Not there, so assume no parents */
        return list_make1_int((int)targetTLI);
    }

    result = NIL;

    /*
     * Parse the file...
     */
    while (fgets(fline, sizeof(fline), fd) != NULL) {
        /* skip leading whitespace and check for # comment */
        char *ptr = NULL;
        char *endptr = NULL;
        TimeLineID tli;

        for (ptr = fline; *ptr; ptr++) {
            if (!isspace((unsigned char)*ptr)) {
                break;
            }
        }
        if (*ptr == '\0' || *ptr == '#') {
            continue;
        }

        /* expect a numeric timeline ID as first field of line */
        tli = (TimeLineID)strtoul(ptr, &endptr, 0);
        if (errno == EINVAL || errno == ERANGE) {
            ereport(FATAL, (errmsg("recovery_target_timeline is not a valid number: \"%s\"", ptr)));
        }
        if (endptr == ptr) {
            (void)FreeFile(fd);
            fd = NULL;
            ereport(FATAL,
                    (errmsg("syntax error in history file: %s", fline), errhint("Expected a numeric timeline ID.")));
        }

        if (result != NIL && tli <= (TimeLineID)linitial_int(result)) {
            (void)FreeFile(fd);
            fd = NULL;
            ereport(FATAL, (errmsg("invalid data in history file: %s", fline),
                            errhint("Timeline IDs must be in increasing sequence.")));
        }

        /* Build list with newest item first */
        result = lcons_int((int)tli, result);

        /* we ignore the remainder of each line */
    }

    (void)FreeFile(fd);
    fd = NULL;

    if (result != NULL && targetTLI <= (TimeLineID)linitial_int(result)) {
        ereport(FATAL, (errmsg("invalid data in history file \"%s\"", path),
                        errhint("Timeline IDs must be less than child timeline's ID.")));
    }

    result = lcons_int((int)targetTLI, result);

    ereport(DEBUG3, (errmsg_internal("history of timeline %u is %s", targetTLI, nodeToString(result))));

    // If the history file was fetched from archive, save it in pg_xlog for future reference.
    if (fromArchive) {
        KeepFileRestoredFromArchive((const char *)path, (const char *)histfname);
    }

    return result;
}

/*
 * Probe whether a timeline history file exists for the given timeline ID
 */
static bool existsTimeLineHistory(TimeLineID probeTLI)
{
    char path[MAXPGPATH];
    char histfname[MAXFNAMELEN];
    FILE *fd = NULL;
    errno_t errorno = EOK;

    /* Timeline 1 does not have a history file, so no need to check */
    if (probeTLI == 1) {
        return false;
    }

    if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
        errorno = snprintf_s(histfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X.history", probeTLI);
        securec_check_ss(errorno, "", "");
        RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0);
    } else {
        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X.history", probeTLI);
        securec_check_ss(errorno, "", "");
    }

    fd = AllocateFile(path, "r");
    if (fd != NULL) {
        (void)FreeFile(fd);
        fd = NULL;
        return true;
    } else {
        if (errno != ENOENT) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }
        return false;
    }
}

/*
 * Scan for new timelines that might have appeared in the archive since we
 * started recovery.
 *
 * If there are any, the function changes recovery target TLI to the latest
 * one and returns 'true'.
 */
static bool rescanLatestTimeLine(void)
{
    TimeLineID newtarget;

    newtarget = findNewestTimeLine(t_thrd.xlog_cxt.recoveryTargetTLI);
    if (newtarget != t_thrd.xlog_cxt.recoveryTargetTLI) {
        /*
         * Determine the list of expected TLIs for the new TLI
         */
        List *newExpectedTLIs = readTimeLineHistory(newtarget);

        /*
         * If the current timeline is not part of the history of the new
         * timeline, we cannot proceed to it.
         *
         * XXX This isn't foolproof: The new timeline might have forked from
         * the current one, but before the current recovery location. In that
         * case we will still switch to the new timeline and proceed replaying
         * from it even though the history doesn't match what we already
         * replayed. That's not good. We will likely notice at the next online
         * checkpoint, as the TLI won't match what we expected, but it's not
         * guaranteed. The admin needs to make sure that doesn't happen.
         */
        if (!list_member_int(newExpectedTLIs, (int)t_thrd.xlog_cxt.recoveryTargetTLI)) {
            ereport(LOG, (errmsg("new timeline %u is not a child of database system timeline %u", newtarget,
                                 t_thrd.xlog_cxt.ThisTimeLineID)));
        } else {
            /* use volatile pointer to prevent code rearrangement */
            volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

            /* Switch target */
            t_thrd.xlog_cxt.recoveryTargetTLI = newtarget;
            list_free(t_thrd.xlog_cxt.expectedTLIs);
            t_thrd.xlog_cxt.expectedTLIs = newExpectedTLIs;

            SpinLockAcquire(&xlogctl->info_lck);
            xlogctl->RecoveryTargetTLI = t_thrd.xlog_cxt.recoveryTargetTLI;
            SpinLockRelease(&xlogctl->info_lck);

            ereport(LOG, (errmsg("new target timeline is %u", t_thrd.xlog_cxt.recoveryTargetTLI)));
            return true;
        }
    }
    return false;
}

/*
 * Find the newest existing timeline, assuming that startTLI exists.
 *
 * Note: while this is somewhat heuristic, it does positively guarantee
 * that (result + 1) is not a known timeline, and therefore it should
 * be safe to assign that ID to a new timeline.
 */
static TimeLineID findNewestTimeLine(TimeLineID startTLI)
{
    TimeLineID newestTLI;
    TimeLineID probeTLI;

    /*
     * The algorithm is just to probe for the existence of timeline history
     * files.  XXX is it useful to allow gaps in the sequence?
     */
    newestTLI = startTLI;

    for (probeTLI = startTLI + 1;; probeTLI++) {
        if (existsTimeLineHistory(probeTLI)) {
            newestTLI = probeTLI; /* probeTLI exists */
        } else {
            /* doesn't exist, assume we're done */
            break;
        }
    }

    return newestTLI;
}

/*
 * I/O routines for pg_control
 *
 * *ControlFile is a buffer in shared memory that holds an image of the
 * contents of pg_control.	WriteControlFile() initializes pg_control
 * given a preloaded buffer, ReadControlFile() loads the buffer from
 * the pg_control file (during postmaster or standalone-backend startup),
 * and UpdateControlFile() rewrites pg_control after we modify xlog state.
 *
 * For simplicity, WriteControlFile() initializes the fields of pg_control
 * that are related to checking backend/database compatibility, and
 * ReadControlFile() verifies they are correct.  We could split out the
 * I/O and compatibility-check functions, but there seems no need currently.
 */
static void WriteControlFile(void)
{
    int fd = -1;
    char buffer[PG_CONTROL_SIZE]; /* need not be aligned */
    errno_t errorno = EOK;

    /*
     * Initialize version and compatibility-check fields
     */
    t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version = PG_CONTROL_VERSION;
    t_thrd.shemem_ptr_cxt.ControlFile->catalog_version_no = CATALOG_VERSION_NO;

    t_thrd.shemem_ptr_cxt.ControlFile->maxAlign = MAXIMUM_ALIGNOF;
    t_thrd.shemem_ptr_cxt.ControlFile->floatFormat = FLOATFORMAT_VALUE;

    t_thrd.shemem_ptr_cxt.ControlFile->blcksz = BLCKSZ;
    t_thrd.shemem_ptr_cxt.ControlFile->relseg_size = RELSEG_SIZE;
    t_thrd.shemem_ptr_cxt.ControlFile->xlog_blcksz = XLOG_BLCKSZ;
    t_thrd.shemem_ptr_cxt.ControlFile->xlog_seg_size = XLOG_SEG_SIZE;

    t_thrd.shemem_ptr_cxt.ControlFile->nameDataLen = NAMEDATALEN;
    t_thrd.shemem_ptr_cxt.ControlFile->indexMaxKeys = INDEX_MAX_KEYS;

    t_thrd.shemem_ptr_cxt.ControlFile->toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;

#ifdef HAVE_INT64_TIMESTAMP
    t_thrd.shemem_ptr_cxt.ControlFile->enableIntTimes = true;
#else
    t_thrd.shemem_ptr_cxt.ControlFile->enableIntTimes = false;
#endif
    t_thrd.shemem_ptr_cxt.ControlFile->float4ByVal = FLOAT4PASSBYVAL;
    t_thrd.shemem_ptr_cxt.ControlFile->float8ByVal = FLOAT8PASSBYVAL;

    /* Timeline */
    t_thrd.shemem_ptr_cxt.ControlFile->timeline = 0;

    /* Contents are protected with a CRC */
    INIT_CRC32C(t_thrd.shemem_ptr_cxt.ControlFile->crc);
    COMP_CRC32C(t_thrd.shemem_ptr_cxt.ControlFile->crc, (char *)t_thrd.shemem_ptr_cxt.ControlFile,
                offsetof(ControlFileData, crc));
    FIN_CRC32C(t_thrd.shemem_ptr_cxt.ControlFile->crc);

    /*
     * We write out PG_CONTROL_SIZE bytes into pg_control, zero-padding the
     * excess over sizeof(ControlFileData).  This reduces the odds of
     * premature-EOF errors when reading pg_control.  We'll still fail when we
     * check the contents of the file, but hopefully with a more specific
     * error than "couldn't read pg_control".
     */
    if (sizeof(ControlFileData) > PG_CONTROL_SIZE) {
        ereport(PANIC, (errmsg("sizeof(ControlFileData) is larger than PG_CONTROL_SIZE; fix either one")));
    }

    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check(errorno, "", "");

    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, t_thrd.shemem_ptr_cxt.ControlFile, sizeof(ControlFileData));
    securec_check(errorno, "", "");

#ifdef USE_ASSERT_CHECKING
#define MAX_SIZE 1024
    char current_absolute_path[MAX_SIZE] = {0};
    getcwd(current_absolute_path, MAX_SIZE);
#endif

    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not create control file \"%s\": %m", XLOG_CONTROL_FILE)));
    }

    errno = 0;
    pgstat_report_waitevent(WAIT_EVENT_CONTROL_FILE_WRITE);
    if (write(fd, buffer, PG_CONTROL_SIZE) != PG_CONTROL_SIZE) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to control file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    pgstat_report_waitevent(WAIT_EVENT_CONTROL_FILE_SYNC);
    if (pg_fsync(fd) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync control file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }
}

static void ReadControlFile(void)
{
    pg_crc32c crc;
    int fd = -1;
    char *fname = NULL;
    bool retry = false;

    // Read data...
#ifdef USE_ASSERT_CHECKING
#define MAX_SIZE 1024
    char current_absolute_path[MAX_SIZE] = {0};
    getcwd(current_absolute_path, MAX_SIZE);
#endif

    fname = XLOG_CONTROL_FILE;

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname)));
    }
    pgstat_report_waitevent(WAIT_EVENT_CONTROL_FILE_READ);
    if (read(fd, t_thrd.shemem_ptr_cxt.ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read from control file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }

    /* Now check the CRC. */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *)t_thrd.shemem_ptr_cxt.ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, t_thrd.shemem_ptr_cxt.ControlFile->crc)) {
        if (retry == false) {
            ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
            fname = XLOG_CONTROL_FILE_BAK;
            retry = true;
            goto loop;
        } else {
            ereport(FATAL, (errmsg("incorrect checksum in control file")));
        }
    }

    /*
     * Check for expected pg_control format version.  If this is wrong, the
     * CRC check will likely fail because we'll be checking the wrong number
     * of bytes.  Complaining about wrong version will probably be more
     * enlightening than complaining about wrong CRC.
     */
    if (t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version != PG_CONTROL_VERSION &&
        t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version % 65536 == 0 &&
        t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version / 65536 != 0) {
        ereport(
            FATAL,
            (errmsg("database files are incompatible with server"),
             errdetail("The database cluster was initialized with PG_CONTROL_VERSION %u (0x%08x),"
                       " but the server was compiled with PG_CONTROL_VERSION %d (0x%08x).",
                       t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version,
                       t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version, PG_CONTROL_VERSION, PG_CONTROL_VERSION),
             errhint("This could be a problem of mismatched byte ordering.  It looks like you need to gs_initdb.")));
    }

    if (t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version != PG_CONTROL_VERSION) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with PG_CONTROL_VERSION %u,"
                                  " but the server was compiled with PG_CONTROL_VERSION %d.",
                                  t_thrd.shemem_ptr_cxt.ControlFile->pg_control_version, PG_CONTROL_VERSION),
                        errhint("It looks like you need to gs_initdb.")));
    }

    /*
     * Do compatibility checking immediately.  If the database isn't
     * compatible with the backend executable, we want to abort before we can
     * possibly do any damage.
     */
    if (t_thrd.shemem_ptr_cxt.ControlFile->catalog_version_no != CATALOG_VERSION_NO) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with CATALOG_VERSION_NO %u,"
                                  " but the server was compiled with CATALOG_VERSION_NO %d.",
                                  t_thrd.shemem_ptr_cxt.ControlFile->catalog_version_no, CATALOG_VERSION_NO),
                        errhint("It looks like you need to gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->maxAlign != MAXIMUM_ALIGNOF) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with MAXALIGN %u (old version), %u,"
                                  " but the server was compiled with MAXALIGN %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->maxAlign,
                                  t_thrd.shemem_ptr_cxt.ControlFile->maxAlign, MAXIMUM_ALIGNOF),
                        errhint("It looks like you need to gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->floatFormat != FLOATFORMAT_VALUE) {
        ereport(FATAL,
                (errmsg("database files are incompatible with server"),
                 errdetail("The database cluster appears to use a different floating-point number format than the "
                           "server executable."),
                 errhint("It looks like you need to gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->blcksz != BLCKSZ) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with BLCKSZ %u (old version), %u"
                                  " but the server was compiled with BLCKSZ %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->blcksz,
                                  t_thrd.shemem_ptr_cxt.ControlFile->blcksz, BLCKSZ),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->relseg_size != RELSEG_SIZE) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with RELSEG_SIZE %u (old version), %u,"
                                  " but the server was compiled with RELSEG_SIZE %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->relseg_size,
                                  t_thrd.shemem_ptr_cxt.ControlFile->relseg_size, RELSEG_SIZE),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->xlog_blcksz != XLOG_BLCKSZ) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with XLOG_BLCKSZ %u (old version), %u,"
                                  " but the server was compiled with XLOG_BLCKSZ %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->xlog_blcksz,
                                  t_thrd.shemem_ptr_cxt.ControlFile->xlog_blcksz, XLOG_BLCKSZ),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->xlog_seg_size != XLOG_SEG_SIZE) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with XLOG_SEG_SIZE %d (old version), %u,"
                                  " but the server was compiled with XLOG_SEG_SIZE %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->xlog_seg_size,
                                  t_thrd.shemem_ptr_cxt.ControlFile->xlog_seg_size, XLOG_SEG_SIZE),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->nameDataLen != NAMEDATALEN) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with NAMEDATALEN %u (old version), %u,"
                                  " but the server was compiled with NAMEDATALEN %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->nameDataLen,
                                  t_thrd.shemem_ptr_cxt.ControlFile->nameDataLen, NAMEDATALEN),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->indexMaxKeys != INDEX_MAX_KEYS) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with INDEX_MAX_KEYS %u (old version), %u"
                                  " but the server was compiled with INDEX_MAX_KEYS %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->indexMaxKeys,
                                  t_thrd.shemem_ptr_cxt.ControlFile->indexMaxKeys, INDEX_MAX_KEYS),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
    if (t_thrd.shemem_ptr_cxt.ControlFile->toast_max_chunk_size != TOAST_MAX_CHUNK_SIZE) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with TOAST_MAX_CHUNK_SIZE %u (old version), %u,"
                                  " but the server was compiled with TOAST_MAX_CHUNK_SIZE %d.",
                                  ((ControlFileDataOld *)t_thrd.shemem_ptr_cxt.ControlFile)->toast_max_chunk_size,
                                  t_thrd.shemem_ptr_cxt.ControlFile->toast_max_chunk_size, (int)TOAST_MAX_CHUNK_SIZE),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }

#ifdef HAVE_INT64_TIMESTAMP
    if (t_thrd.shemem_ptr_cxt.ControlFile->enableIntTimes != true) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized without HAVE_INT64_TIMESTAMP"
                                  " but the server was compiled with HAVE_INT64_TIMESTAMP."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#else
    if (t_thrd.shemem_ptr_cxt.ControlFile->enableIntTimes != false) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with HAVE_INT64_TIMESTAMP"
                                  " but the server was compiled without HAVE_INT64_TIMESTAMP."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#endif

#ifdef USE_FLOAT4_BYVAL
    if (t_thrd.shemem_ptr_cxt.ControlFile->float4ByVal != true) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized without USE_FLOAT4_BYVAL"
                                  " but the server was compiled with USE_FLOAT4_BYVAL."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#else
    if (t_thrd.shemem_ptr_cxt.ControlFile->float4ByVal != false) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with USE_FLOAT4_BYVAL"
                                  " but the server was compiled without USE_FLOAT4_BYVAL."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#endif

#ifdef USE_FLOAT8_BYVAL
    if (t_thrd.shemem_ptr_cxt.ControlFile->float8ByVal != true) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized without USE_FLOAT8_BYVAL"
                                  " but the server was compiled with USE_FLOAT8_BYVAL."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#else
    if (t_thrd.shemem_ptr_cxt.ControlFile->float8ByVal != false) {
        ereport(FATAL, (errmsg("database files are incompatible with server"),
                        errdetail("The database cluster was initialized with USE_FLOAT8_BYVAL"
                                  " but the server was compiled without USE_FLOAT8_BYVAL."),
                        errhint("It looks like you need to recompile or gs_initdb.")));
    }
#endif
    /* recover the bad primary control file */
    if (retry == true) {
        RecoverControlFile();
    }
}

/*
 * copy ControlFile to local variable, then calculate CRC and write to pg_control
 * with the local variable.
 * If all work is with ControlFile, there is a time window between calculating CRC
 * and writing file, other thread can modify ControlFile before writing file, then
 * crc in ControlFile is invalid.
 */
void UpdateControlFile(void)
{
    int fd = -1;
    int len;
    errno_t err = EOK;
    char *fname[2];
    ControlFileData copy_of_ControlFile;

    len = sizeof(ControlFileData);
    err = memcpy_s(&copy_of_ControlFile, len, t_thrd.shemem_ptr_cxt.ControlFile, len);
    securec_check(err, "\0", "\0");

    INIT_CRC32C(copy_of_ControlFile.crc);
    COMP_CRC32C(copy_of_ControlFile.crc, (char *)&copy_of_ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(copy_of_ControlFile.crc);

#ifdef USE_ASSERT_CHECKING
#define MAX_SIZE 1024
    char current_absolute_path[MAX_SIZE] = {0};
    getcwd(current_absolute_path, MAX_SIZE);
#endif

    fname[0] = XLOG_CONTROL_FILE_BAK;
    fname[1] = XLOG_CONTROL_FILE;

    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            fd = BasicOpenFile(fname[i], O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        } else {
            fd = BasicOpenFile(fname[i], O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        }

        if (fd < 0) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname[i])));
        }

        errno = 0;
        pgstat_report_waitevent(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
        if (write(fd, &copy_of_ControlFile, len) != len) {
            /* if write didn't set errno, assume problem is no disk space */
            if (errno == 0) {
                errno = ENOSPC;
            }
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to control file: %m")));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        pgstat_report_waitevent(WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE);
        if (fsync(fd) != 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync control file: %m")));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        if (close(fd)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }
    }
}

/*
 * when incorrect checksum is detected in control file,
 * we should recover the control file using the content of backup file
 */
static void RecoverControlFile(void)
{
    int fd;
    char buffer[PG_CONTROL_SIZE];
    errno_t errorno = EOK;

    ereport(WARNING, (errmsg("recover the control file due to incorrect checksum")));

    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check(errorno, "", "");

    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, t_thrd.shemem_ptr_cxt.ControlFile, sizeof(ControlFileData));
    securec_check(errorno, "", "");

    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_TRUNC | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(),
                        errmsg("recover failed could not open control file \"%s\": %m", XLOG_CONTROL_FILE)));
    }
    errno = 0;
    /* write the whole block */
    if (write(fd, &buffer, PG_CONTROL_SIZE) != PG_CONTROL_SIZE) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(), errmsg("recover failed could not write to control file: %m")));
    }

    if (pg_fsync(fd) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("recover failed could not fsync control file: %m")));
    }

    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("recover failed could not close control file: %m")));
    }
}

/*
 * Returns the unique system identifier from control file.
 */
uint64 GetSystemIdentifier(void)
{
    Assert(t_thrd.shemem_ptr_cxt.ControlFile != NULL);
    return t_thrd.shemem_ptr_cxt.ControlFile->system_identifier;
}

void SetSystemIdentifier(uint64 system_identifier)
{
    Assert(t_thrd.shemem_ptr_cxt.ControlFile != NULL);
    t_thrd.shemem_ptr_cxt.ControlFile->system_identifier = system_identifier;
    return;
}

TimeLineID GetThisTimeID(void)
{
    Assert(t_thrd.shemem_ptr_cxt.ControlFile != NULL);
    return t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID;
}

void SetThisTimeID(TimeLineID timelineID)
{
    Assert(t_thrd.shemem_ptr_cxt.ControlFile != NULL);
    t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID = timelineID;
    return;
}

/*
 * Auto-tune the number of XLOG buffers.
 *
 * The preferred setting for wal_buffers is about 3% of shared_buffers, with
 * a maximum of one XLOG segment (there is little reason to think that more
 * is helpful, at least so long as we force an fsync when switching log files)
 * and a minimum of 8 blocks (which was the default value prior to PostgreSQL
 * 9.1, when auto-tuning was added).
 *
 * This should not be called until NBuffers has received its final value.
 */
static int XLOGChooseNumBuffers(void)
{
    int xbuffers = g_instance.attr.attr_storage.NBuffers / 32;
    if (xbuffers > XLOG_SEG_SIZE / XLOG_BLCKSZ) {
        xbuffers = XLOG_SEG_SIZE / XLOG_BLCKSZ;
    }
    if (xbuffers < 8) {
        xbuffers = 8;
    }
    return xbuffers;
}

/*
 * GUC check_hook for wal_buffers
 */
bool check_wal_buffers(int *newval, void **extra, GucSource source)
{
    /* -1 indicates a request for auto-tune. */
    if (*newval == -1) {
        /*
         * If we haven't yet changed the boot_val default of -1, just let it
         * be.	We'll fix it when XLOGShmemSize is called.
         */
        if (g_instance.attr.attr_storage.XLOGbuffers == -1) {
            return true;
        }

        /* Otherwise, substitute the auto-tune value */
        *newval = XLOGChooseNumBuffers();
    }

    /*
     * We clamp manually-set values to at least 4 blocks.  Prior to PostgreSQL
     * 9.1, a minimum of 4 was enforced by guc.c, but since that is no longer
     * the case, we just silently treat such values as a request for the
     * minimum.  (We could throw an error instead, but that doesn't seem very
     * helpful.)
     */
    if (*newval < 4) {
        *newval = 4;
    }

    return true;
}

/*
 * Initialization of shared memory for XLOG
 */
Size XLOGShmemSize(void)
{
    Size size;
    errno_t errorno = EOK;

    /*
     * If the value of wal_buffers is -1, use the preferred auto-tune value.
     * This isn't an amazingly clean place to do this, but we must wait till
     * NBuffers has received its final value, and must do it before using the
     * value of XLOGbuffers to do anything important.
     */
    if (g_instance.attr.attr_storage.XLOGbuffers == -1) {
        char buf[32];

        errorno = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%d", XLOGChooseNumBuffers());
        securec_check_ss(errorno, "", "");
        SetConfigOption("wal_buffers", buf, PGC_POSTMASTER, PGC_S_OVERRIDE);
    }
    Assert(g_instance.attr.attr_storage.XLOGbuffers > 0);

    /* XLogCtl */
    size = sizeof(XLogCtlData) + 16;

    /* xlog insertion slots, plus alignment will be alloced individually */
    /* xlblocks array */
    size = add_size(size, mul_size(sizeof(XLogRecPtr), g_instance.attr.attr_storage.XLOGbuffers));
    /* extra alignment padding for XLOG I/O buffers */
    size = add_size(size, XLOG_BLCKSZ);
    /* and the buffers themselves */
    size = add_size(size, mul_size(XLOG_BLCKSZ, g_instance.attr.attr_storage.XLOGbuffers));

    /*
     * Note: we don't count ControlFileData, it comes out of the "slop factor"
     * added by CreateSharedMemoryAndSemaphores.  This lets us use this
     * routine again below to compute the actual allocation size.
     */
    return size;
}

void XLOGShmemInit(void)
{
    bool foundCFile = false;
    bool foundXLog = false;
    char *allocptr = NULL;
    int i;
    errno_t errorno = EOK;

    int nNumaNodes = g_instance.shmem_cxt.numaNodeNum;
    if (!IsUnderPostmaster && g_instance.attr.attr_storage.num_xloginsert_locks % nNumaNodes != 0) {
        ereport(PANIC,
                (errmsg("XLOGShmemInit num_xloginsert_locks should be multiple of NUMA node number in the system.")));
    }
    g_instance.wal_cxt.num_locks_in_group = g_instance.attr.attr_storage.num_xloginsert_locks / nNumaNodes;

    t_thrd.shemem_ptr_cxt.ControlFile = (ControlFileData *)ShmemInitStruct("Control File", sizeof(ControlFileData),
                                                                           &foundCFile);
    t_thrd.shemem_ptr_cxt.XLogCtl = (XLogCtlData *)ShmemInitStruct("XLOG Ctl", XLOGShmemSize(), &foundXLog);

    t_thrd.shemem_ptr_cxt.XLogCtl = (XLogCtlData *)TYPEALIGN(16, t_thrd.shemem_ptr_cxt.XLogCtl);

    if (foundCFile || foundXLog) {
        /* both should be present or neither */
        Assert(foundCFile && foundXLog);

        /* Initialize local copy of WALInsertLocks and register the tranche */
        t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks = t_thrd.shemem_ptr_cxt.XLogCtl->Insert.WALInsertLocks;
        t_thrd.shemem_ptr_cxt.LocalGroupWALInsertLocks =
            t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks[t_thrd.proc->nodeno];
        return;
    }
    errorno = memset_s(t_thrd.shemem_ptr_cxt.XLogCtl, sizeof(XLogCtlData), 0, sizeof(XLogCtlData));
    securec_check(errorno, "", "");

    /*
     * Since XLogCtlData contains XLogRecPtr fields, its sizeof should be a
     * multiple of the alignment for same, so no extra alignment padding is
     * needed here.
     */
    allocptr = ((char *)t_thrd.shemem_ptr_cxt.XLogCtl) + sizeof(XLogCtlData);
    t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks = (XLogRecPtr *)allocptr;
    errorno = memset_s(t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks,
                       sizeof(XLogRecPtr) * g_instance.attr.attr_storage.XLOGbuffers, 0,
                       sizeof(XLogRecPtr) * g_instance.attr.attr_storage.XLOGbuffers);
    securec_check(errorno, "", "");

    allocptr += sizeof(XLogRecPtr) * g_instance.attr.attr_storage.XLOGbuffers;

    /* WAL insertion locks. Ensure they're aligned to the full padded size */
    WALInsertLockPadded **insertLockGroupPtr = (WALInsertLockPadded **)CACHELINEALIGN(
        palloc0(nNumaNodes * sizeof(WALInsertLockPadded *) + PG_CACHE_LINE_SIZE));
#ifdef __USE_NUMA
    if (nNumaNodes > 1) {
        size_t allocSize = sizeof(WALInsertLockPadded) * g_instance.wal_cxt.num_locks_in_group + PG_CACHE_LINE_SIZE;
        for (int i = 0; i < nNumaNodes; i++) {
            char *pInsertLock = (char *)numa_alloc_onnode(allocSize, i);
            if (pInsertLock == NULL) {
                ereport(PANIC, (errmsg("XLOGShmemInit could not alloc memory on node %d", i)));
            }
            add_numa_alloc_info(pInsertLock, allocSize);
            insertLockGroupPtr[i] = (WALInsertLockPadded *)(CACHELINEALIGN(pInsertLock));
        }
    } else {
#endif
        char *pInsertLock = (char *)CACHELINEALIGN(palloc(
            sizeof(WALInsertLockPadded) * g_instance.attr.attr_storage.num_xloginsert_locks + PG_CACHE_LINE_SIZE));
        insertLockGroupPtr[0] = (WALInsertLockPadded *)(CACHELINEALIGN(pInsertLock));
#ifdef __USE_NUMA
    }
#endif

    t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks = t_thrd.shemem_ptr_cxt.XLogCtl->Insert.WALInsertLocks =
        insertLockGroupPtr;
    t_thrd.shemem_ptr_cxt.LocalGroupWALInsertLocks = t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks[0];

    for (int processorIndex = 0; processorIndex < nNumaNodes; processorIndex++) {
        for (i = 0; i < g_instance.wal_cxt.num_locks_in_group; i++) {
            LWLockInitialize(&t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks[processorIndex][i].l.lock,
                             LWTRANCHE_WAL_INSERT);
            t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks[processorIndex][i].l.insertingAt = InvalidXLogRecPtr;
#ifdef __aarch64__
            pg_atomic_init_u32(&t_thrd.shemem_ptr_cxt.GlobalWALInsertLocks[processorIndex][i].l.xlogGroupFirst,
                               INVALID_PGPROCNO);
#endif
        }
    }

    /*
     * Align the start of the page buffers to a full xlog block size boundary.
     * This simplifies some calculations in XLOG insertion. It is also required
     * for O_DIRECT.
     */
    allocptr = (char *)TYPEALIGN(XLOG_BLCKSZ, allocptr);
    t_thrd.shemem_ptr_cxt.XLogCtl->pages = allocptr;
    errorno = memset_s(t_thrd.shemem_ptr_cxt.XLogCtl->pages,
                       (Size)XLOG_BLCKSZ * g_instance.attr.attr_storage.XLOGbuffers, 0,
                       (Size)XLOG_BLCKSZ * g_instance.attr.attr_storage.XLOGbuffers);
    securec_check(errorno, "", "");

    if (BBOX_BLACKLIST_XLOG_BUFFER) {
        bbox_blacklist_add(XLOG_BUFFER, t_thrd.shemem_ptr_cxt.XLogCtl->pages,
                           (uint64)XLOG_BLCKSZ * g_instance.attr.attr_storage.XLOGbuffers);
    }

    /*
     * Do basic initialization of XLogCtl shared data. (StartupXLOG will fill
     * in additional info.)
     */
    t_thrd.shemem_ptr_cxt.XLogCtl->XLogCacheBlck = g_instance.attr.attr_storage.XLOGbuffers - 1;
    t_thrd.shemem_ptr_cxt.XLogCtl->SharedRecoveryInProgress = true;
    t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone = false;
    t_thrd.shemem_ptr_cxt.XLogCtl->SharedHotStandbyActive = false;
    t_thrd.shemem_ptr_cxt.XLogCtl->WalWriterSleeping = false;

#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&t_thrd.shemem_ptr_cxt.XLogCtl->Insert.insertpos_lck);
#endif
    SpinLockInit(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
    InitSharedLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
    InitSharedLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->dataRecoveryLatch);

    /*
     * If we are not in bootstrap mode, pg_control should already exist. Read
     * and validate it immediately (see comments in ReadControlFile() for the
     * reasons why).
     */
    if (!IsBootstrapProcessingMode()) {
        ReadControlFile();
    }
}

static uint64 GetMACAddr(void)
{
    macaddr mac;
    uint64 macAddr;
    int sockFd = NO_SOCKET;
    struct ifconf ifconfInfo;
    struct ifreq ifreqInfo;
    char *buf = NULL;
    errno_t ss_rc = EOK;
    uint32 i;

    ss_rc = memset_s((void *)&mac, sizeof(macaddr), 0, sizeof(macaddr));
    securec_check(ss_rc, "\0", "\0");

    sockFd = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sockFd != NO_SOCKET) {
        buf = (char *)palloc(MaxMacAddrList * sizeof(ifreq));
        ifconfInfo.ifc_len = MaxMacAddrList * sizeof(ifreq);
        ifconfInfo.ifc_buf = buf;

        if (ioctl(sockFd, SIOCGIFCONF, &ifconfInfo) != -1) {
            struct ifreq *ifrepTmp = ifconfInfo.ifc_req;
            for (i = 0; i < (ifconfInfo.ifc_len / sizeof(struct ifreq)); i++) {
                ss_rc = strcpy_s(ifreqInfo.ifr_name, strlen(ifrepTmp->ifr_name) + 1, ifrepTmp->ifr_name);
                securec_check(ss_rc, "\0", "\0");

                if (ioctl(sockFd, SIOCGIFFLAGS, &ifreqInfo) == 0) {
                    if (!(ifreqInfo.ifr_flags & IFF_LOOPBACK)) {
                        if (ioctl(sockFd, SIOCGIFHWADDR, &ifreqInfo) == 0) {
                            mac.a = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[0];
                            mac.b = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[1];
                            mac.c = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[2];
                            mac.d = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[3];
                            mac.e = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[4];
                            mac.f = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[5];
                            break;
                        }
                    }
                }
                ifrepTmp++;
            }
        }
        pfree_ext(buf);
        close(sockFd);
    }
    macAddr = ((uint64)mac.a << 40) | ((uint64)mac.b << 32) | ((uint64)mac.c << 24) | ((uint64)mac.d << 16) |
              ((uint64)mac.e << 8) | (uint64)mac.f;
    return macAddr;
}

/*
 * This func must be called ONCE on system install.  It creates pg_control
 * and the initial XLOG segment.
 */
void BootStrapXLOG(void)
{
    CheckPoint checkPoint;
    char *buffer = NULL;
    XLogPageHeader page;
    XLogLongPageHeader longpage;
    XLogRecord *record = NULL;
    char *recptr = NULL;
    bool use_existent = false;
    uint64 sysidentifier;
    uint64 macAddr;
    pg_crc32c crc;
    errno_t ret = EOK;
    TransactionId latestCompletedXid;
    int fd = 0;
    int len = 0;
    int randomNum;

    /*
     * Select a hopefully-unique system identifier code for this installation.
     * We use the result of GetMACAddr() to make the first 48 bits.
     * And the last is a random number from linux system to make sure the difference.
     */
    macAddr = GetMACAddr();
    sysidentifier = macAddr << 16;
    fd = open("/dev/urandom", O_RDONLY);
    if (fd == -1) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("cann't read a random number from file \"/dev/urandom\".")));
    }
    if (fd > 0) {
        len = read(fd, &randomNum, sizeof(randomNum));
        if (len != sizeof(randomNum)) {
            close(fd);
            ereport(ERROR,
                    (errcode_for_file_access(), errmsg("cann't read a random number from file \"/dev/urandom\".")));
        }
    }
    close(fd);
    sysidentifier |= (uint32)randomNum & 0xFFFF;
    ereport(WARNING, (errmsg_internal("macAddr is %u/%u, sysidentifier is %u/%u, randomNum is %u",
                                      (uint32)(macAddr >> 32), (uint32)macAddr, (uint32)(sysidentifier >> 32),
                                      (uint32)sysidentifier, (uint32)randomNum)));

    /* First timeline ID is always 1 */
    t_thrd.xlog_cxt.ThisTimeLineID = 1;

    /* page buffer must be aligned suitably for O_DIRECT */
    buffer = (char *)palloc(XLOG_BLCKSZ + XLOG_BLCKSZ);
    page = (XLogPageHeader)TYPEALIGN(XLOG_BLCKSZ, buffer);
    ret = memset_s(page, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
    securec_check(ret, "", "");

    /*
     * Set up information for the initial checkpoint record
     *
     * The initial checkpoint record is written to the beginning of the WAL
     * segment with logid=0 logseg=1. The very first WAL segment, 0/0, is not
     * used, so that we can use 0/0 to mean "before any valid WAL segment".
     */
    if (ENABLE_INCRE_CKPT) {
        u_sess->attr.attr_storage.fullPageWrites = false;
    }
    checkPoint.redo = XLogSegSize + SizeOfXLogLongPHD;
    checkPoint.ThisTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    checkPoint.fullPageWrites = u_sess->attr.attr_storage.fullPageWrites;
    checkPoint.nextXid = FirstNormalTransactionId + 1;
    checkPoint.nextOid = FirstBootstrapObjectId;
    checkPoint.nextMulti = FirstMultiXactId + 1;
    checkPoint.nextMultiOffset = 0;
    checkPoint.oldestXid = FirstNormalTransactionId;
    checkPoint.oldestXidDB = TemplateDbOid;
    checkPoint.time = (pg_time_t)time(NULL);
    checkPoint.oldestActiveXid = InvalidTransactionId;
    checkPoint.remove_seg = InvalidXLogSegPtr;

    t_thrd.xact_cxt.ShmemVariableCache->nextXid = checkPoint.nextXid;
    t_thrd.xact_cxt.ShmemVariableCache->nextOid = checkPoint.nextOid;
    t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;

    t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = COMMITSEQNO_FIRST_NORMAL + 1;
    latestCompletedXid = checkPoint.nextXid;
    TransactionIdRetreat(latestCompletedXid);
    t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestCompletedXid;

    /* init recentGlobalXmin and xmin, to nextXid */
    t_thrd.xact_cxt.ShmemVariableCache->xmin = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin = latestCompletedXid;
    t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);

    /* Set up the XLOG page header */
    page->xlp_magic = XLOG_PAGE_MAGIC;
    page->xlp_info = XLP_LONG_HEADER;
    page->xlp_tli = t_thrd.xlog_cxt.ThisTimeLineID;
    page->xlp_pageaddr = XLogSegSize;
    longpage = (XLogLongPageHeader)page;
    longpage->xlp_sysid = sysidentifier;
    longpage->xlp_seg_size = XLogSegSize;
    longpage->xlp_xlog_blcksz = XLOG_BLCKSZ;

    /* Insert the initial checkpoint record */
    recptr = ((char *)page + SizeOfXLogLongPHD);
    record = (XLogRecord *)recptr;
    record->xl_prev = 0;
    record->xl_xid = InvalidTransactionId;
    record->xl_tot_len = CHECKPOINT_LEN;
    record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    record->xl_rmid = RM_XLOG_ID;
    recptr += SizeOfXLogRecord;
    /* fill the XLogRecordDataHeaderShort struct */
    *(recptr++) = XLR_BLOCK_ID_DATA_SHORT;
    *(recptr++) = sizeof(checkPoint);
    ret = memcpy_s(recptr, sizeof(checkPoint), &checkPoint, sizeof(checkPoint));
    securec_check(ret, "", "");
    recptr += sizeof(checkPoint);
    Assert(recptr - (char *)record == record->xl_tot_len);

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char *)record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    COMP_CRC32C(crc, (char *)record, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);
    record->xl_crc = crc;

    /* Create first XLOG segment file */
    use_existent = false;
    t_thrd.xlog_cxt.openLogFile = XLogFileInit(1, &use_existent, false);

    /* Write the first page with the initial record */
    errno = 0;
    pgstat_report_waitevent(WAIT_EVENT_WAL_BOOTSTRAP_WRITE);
    if (write(t_thrd.xlog_cxt.openLogFile, page, XLOG_BLCKSZ) != XLOG_BLCKSZ) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write bootstrap transaction log file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    pgstat_report_waitevent(WAIT_EVENT_WAL_BOOTSTRAP_SYNC);
    if (pg_fsync(t_thrd.xlog_cxt.openLogFile) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync bootstrap transaction log file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(t_thrd.xlog_cxt.openLogFile)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close bootstrap transaction log file: %m")));
    }

    t_thrd.xlog_cxt.openLogFile = -1;

    /* Now create pg_control */
    ret = memset_s(t_thrd.shemem_ptr_cxt.ControlFile, sizeof(ControlFileData), 0, sizeof(ControlFileData));
    securec_check(ret, "", "");

    /* Initialize pg_control status fields */
    t_thrd.shemem_ptr_cxt.ControlFile->system_identifier = sysidentifier;
    t_thrd.shemem_ptr_cxt.ControlFile->state = DB_SHUTDOWNED;
    t_thrd.shemem_ptr_cxt.ControlFile->time = checkPoint.time;
    t_thrd.shemem_ptr_cxt.ControlFile->checkPoint = checkPoint.redo;
    t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy = checkPoint;

    /* Set important parameter values for use when replaying WAL */
    t_thrd.shemem_ptr_cxt.ControlFile->MaxConnections = g_instance.shmem_cxt.MaxConnections;
    t_thrd.shemem_ptr_cxt.ControlFile->max_prepared_xacts = g_instance.attr.attr_storage.max_prepared_xacts;
    t_thrd.shemem_ptr_cxt.ControlFile->max_locks_per_xact = g_instance.attr.attr_storage.max_locks_per_xact;
    t_thrd.shemem_ptr_cxt.ControlFile->wal_level = g_instance.attr.attr_storage.wal_level;

    /* some additional ControlFile fields are set in WriteControlFile() */
    WriteControlFile();
    /* bootstrap double write */
    MemoryContext mem_cxt = AllocSetContextCreate(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
        "DoubleWriteContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    MemoryContext old_mem_cxt = MemoryContextSwitchTo(mem_cxt);
    dw_bootstrap();
    (void)MemoryContextSwitchTo(old_mem_cxt);

    /* Bootstrap the commit log, too */
    BootStrapCLOG();
    BootStrapCSNLOG();
    BootStrapMultiXact();

    pfree_ext(buffer);
}

static char *str_time(pg_time_t tnow)
{
    pg_strftime(t_thrd.xlog_cxt.buf, sizeof(t_thrd.xlog_cxt.buf), "%Y-%m-%d %H:%M:%S %Z",
                pg_localtime(&tnow, log_timezone));

    return t_thrd.xlog_cxt.buf;
}

/*
 * See if there is a recovery command file (recovery.conf), and if so
 * read in parameters for archive recovery and XLOG streaming.
 *
 * The file is parsed using the main configuration parser.
 */
static void readRecoveryCommandFile(void)
{
    FILE *fd = NULL;
    TimeLineID rtli = 0;
    bool rtliGiven = false;
    ConfigVariable *item = NULL;
    ConfigVariable *head = NULL;
    ConfigVariable *tail = NULL;

    fd = AllocateFile(RECOVERY_COMMAND_FILE, "r");
    if (fd == NULL) {
        if (errno == ENOENT) {
            return; /* not there, so no archive recovery */
        }
        ereport(FATAL, (errcode_for_file_access(),
                        errmsg("could not open recovery command file \"%s\": %m", RECOVERY_COMMAND_FILE)));
    }

    /*
     * Since we're asking ParseConfigFp() to report errors as FATAL, there's
     * no need to check the return value.
     */
    (void)ParseConfigFp(fd, RECOVERY_COMMAND_FILE, 0, FATAL, &head, &tail);

    (void)FreeFile(fd);
    fd = NULL;

    for (item = head; item; item = item->next) {
        if (strcmp(item->name, "restore_command") == 0 && t_thrd.xlog_cxt.server_mode != STANDBY_MODE) {
            t_thrd.xlog_cxt.recoveryRestoreCommand = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("restore_command = '%s'", t_thrd.xlog_cxt.recoveryRestoreCommand)));
        } else if (strcmp(item->name, "recovery_end_command") == 0) {
            t_thrd.xlog_cxt.recoveryEndCommand = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("recovery_end_command = '%s'", t_thrd.xlog_cxt.recoveryEndCommand)));
        } else if (strcmp(item->name, "archive_cleanup_command") == 0) {
            t_thrd.xlog_cxt.archiveCleanupCommand = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("archive_cleanup_command = '%s'", t_thrd.xlog_cxt.archiveCleanupCommand)));
        } else if (strcmp(item->name, "pause_at_recovery_target") == 0) {
            if (!parse_bool(item->value, &t_thrd.xlog_cxt.recoveryPauseAtTarget)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("parameter \"%s\" requires a Boolean value", "pause_at_recovery_target")));
            }
            ereport(DEBUG2, (errmsg_internal("pause_at_recovery_target = '%s'", item->value)));
        } else if (strcmp(item->name, "recovery_target_timeline") == 0) {
            rtliGiven = true;
            if (strcmp(item->value, "latest") == 0) {
                rtli = 0;
            } else {
                errno = 0;
                rtli = (TimeLineID)strtoul(item->value, NULL, 0);
                if (errno == EINVAL || errno == ERANGE) {
                    ereport(FATAL, (errmsg("recovery_target_timeline is not a valid number: \"%s\"", item->value)));
                }
            }
            if (rtli) {
                ereport(DEBUG2, (errmsg_internal("recovery_target_timeline = %u", rtli)));
            } else {
                ereport(DEBUG2, (errmsg_internal("recovery_target_timeline = latest")));
            }
        } else if (strcmp(item->name, "recovery_target_xid") == 0) {
            errno = 0;
            t_thrd.xlog_cxt.recoveryTargetXid = (TransactionId)pg_strtouint64(item->value, NULL, 0);
            if (errno == EINVAL || errno == ERANGE) {
                ereport(FATAL, (errmsg("recovery_target_xid is not a valid number: \"%s\"", item->value)));
            }
            ereport(DEBUG2, (errmsg_internal("recovery_target_xid = " XID_FMT, t_thrd.xlog_cxt.recoveryTargetXid)));
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_XID;
        } else if (strcmp(item->name, "recovery_target_time") == 0) {
            /*
             * if recovery_target_xid or recovery_target_name specified, then
             * this overrides recovery_target_time
             */
            if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID ||
                t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_NAME) {
                continue;
            }
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_TIME;

            /*
             * Convert the time string given by the user to TimestampTz form.
             */
            t_thrd.xlog_cxt.recoveryTargetTime =
                DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in, CStringGetDatum(item->value),
                                                        ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
            ereport(DEBUG2, (errmsg_internal("recovery_target_time = '%s'",
                                             timestamptz_to_str(t_thrd.xlog_cxt.recoveryTargetTime))));
#ifdef PGXC
        } else if (strcmp(item->name, "recovery_target_barrier") == 0) {
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_BARRIER;
            t_thrd.xlog_cxt.recoveryTargetBarrierId = pstrdup(item->value);
#endif
        } else if (strcmp(item->name, "recovery_target_name") == 0) {
            /*
             * if recovery_target_xid specified, then this overrides
             * recovery_target_name
             */
            if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID) {
                continue;
            }
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_NAME;
            t_thrd.xlog_cxt.recoveryTargetName = pstrdup(item->value);
            if (strlen(t_thrd.xlog_cxt.recoveryTargetName) >= MAXFNAMELEN) {
                ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("recovery_target_name is too long (maximum %d characters)", MAXFNAMELEN - 1)));
            }

            ereport(DEBUG2, (errmsg_internal("recovery_target_name = '%s'", t_thrd.xlog_cxt.recoveryTargetName)));
        } else if (strcmp(item->name, "recovery_target_lsn") == 0) {
            /*
             * if recovery_target_xid or recovery_target_name or recovery_target_time
             * specified, then this overrides recovery_target_lsn
             */
            if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID ||
                t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_NAME ||
                t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME) {
                continue;
            }
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_LSN;

            /*
             * Convert the LSN string given by the user to XLogRecPtr form.
             */
            t_thrd.xlog_cxt.recoveryTargetLSN = DatumGetLSN(DirectFunctionCall3(
                pg_lsn_in, CStringGetDatum(item->value), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
            ereport(DEBUG2,
                (errmsg_internal("recovery_target_lsn = '%X/%X'",
                    (uint32)(t_thrd.xlog_cxt.recoveryTargetLSN >> 32),
                    (uint32)t_thrd.xlog_cxt.recoveryTargetLSN)));
        } else if (strcmp(item->name, "recovery_target_inclusive") == 0) {
            // does nothing if a recovery_target is not also set
            if (!parse_bool(item->value, &t_thrd.xlog_cxt.recoveryTargetInclusive)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("parameter \"%s\" requires a Boolean value", "recovery_target_inclusive")));
            }
            ereport(DEBUG2, (errmsg_internal("recovery_target_inclusive = %s", item->value)));
        } else if (strcmp(item->name, "standby_mode") == 0) {
            if (!parse_bool(item->value, &t_thrd.xlog_cxt.StandbyModeRequested)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("parameter \"%s\" requires a Boolean value", "standby_mode")));
            }
            ereport(DEBUG2, (errmsg_internal("standby_mode = '%s'", item->value)));
        } else if (strcmp(item->name, "primary_conninfo") == 0) {
            t_thrd.xlog_cxt.PrimaryConnInfo = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("primary_conninfo = '%s'", t_thrd.xlog_cxt.PrimaryConnInfo)));
        } else if (strcmp(item->name, "primary_slotname") == 0) {
            ReplicationSlotValidateName(item->value, ERROR);
            u_sess->attr.attr_storage.PrimarySlotName = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("primary_slotname = '%s'", u_sess->attr.attr_storage.PrimarySlotName)));
        } else if (strcmp(item->name, "trigger_file") == 0) {
            t_thrd.xlog_cxt.TriggerFile = pstrdup(item->value);
            ereport(DEBUG2, (errmsg_internal("trigger_file = '%s'", t_thrd.xlog_cxt.TriggerFile)));
        } else {
            ereport(FATAL, (errmsg("unrecognized recovery parameter \"%s\"", item->name)));
        }
    }

    /*
     * Check for compulsory parameters
     */
    if (t_thrd.xlog_cxt.StandbyModeRequested) {
        if (t_thrd.xlog_cxt.PrimaryConnInfo == NULL && t_thrd.xlog_cxt.recoveryRestoreCommand == NULL) {
            ereport(WARNING,
                    (errmsg("recovery command file \"%s\" specified neither primary_conninfo nor restore_command",
                            RECOVERY_COMMAND_FILE),
                     errhint("The database server will regularly poll the pg_xlog subdirectory to check for files "
                             "placed there.")));
        }
    } else {
        if (t_thrd.xlog_cxt.recoveryRestoreCommand == NULL && XLogInsertAllowed()) {
            ereport(
                FATAL,
                (errmsg("recovery command file \"%s\" must specify restore_command when standby mode is not enabled",
                        RECOVERY_COMMAND_FILE)));
        }
    }

    /* Enable fetching from archive recovery area */
    t_thrd.xlog_cxt.ArchiveRestoreRequested = true;

    /*
     * If user specified recovery_target_timeline, validate it or compute the
     * "latest" value.	We can't do this until after we've gotten the restore
     * command and set InArchiveRecovery, because we need to fetch timeline
     * history files from the archive.
     */
    if (rtliGiven) {
        if (rtli) {
            /* Timeline 1 does not have a history file, all else should */
            if (rtli != 1 && !existsTimeLineHistory(rtli)) {
                ereport(FATAL, (errmsg("recovery target timeline %u does not exist", rtli)));
            }
            t_thrd.xlog_cxt.recoveryTargetTLI = rtli;
            t_thrd.xlog_cxt.recoveryTargetIsLatest = false;
        } else {
            /* We start the "latest" search from pg_control's timeline */
            t_thrd.xlog_cxt.recoveryTargetTLI = findNewestTimeLine(t_thrd.xlog_cxt.recoveryTargetTLI);
            t_thrd.xlog_cxt.recoveryTargetIsLatest = true;
        }
    }

    FreeConfigVariables(head);
}

static void close_readFile_if_open()
{
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
    }
}

/*
 * Exit archive-recovery state
 */
static void exitArchiveRecovery(TimeLineID endTLI, XLogSegNo endLogSegNo)
{
    struct stat statbuf;
    char recoveryPath[MAXPGPATH];
    char xlogpath[MAXPGPATH];
    errno_t errorno = EOK;

    // We are no longer in archive recovery state.
    t_thrd.xlog_cxt.InArchiveRecovery = false;

    // Update min recovery point one last time.
    UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);

    /*
     * If the ending log segment is still open, close it (to avoid problems on
     * Windows with trying to rename or delete an open file).
     */
    close_readFile_if_open();

    /*
     * If we are establishing a new timeline, we have to copy data from the
     * last WAL segment of the old timeline to create a starting WAL segment
     * for the new timeline.
     *
     * Notify the archiver that the last WAL segment of the old timeline is
     * ready to copy to archival storage. Otherwise, it is not archived for a
     * while.
     */
    if (endTLI != t_thrd.xlog_cxt.ThisTimeLineID) {
        char path[MAXPGPATH];
        char tmppath[MAXPGPATH];

        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", endTLI,
                             (uint32)((endLogSegNo) / XLogSegmentsPerXLogId),
                             (uint32)((endLogSegNo) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");

        errorno = snprintf_s(tmppath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/xlogtemp.%lu", gs_thread_self());
        securec_check_ss(errorno, "\0", "\0");

        XLogFileCopy(path, tmppath);
        /*
         * Now move the segment into place with its final name.
         */
        if (!InstallXLogFileSegment(&endLogSegNo, (const char *)tmppath, false, NULL, false))
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("InstallXLogFileSegment should not have failed")));
        if (XLogArchivingActive()) {
            errorno = snprintf_s(xlogpath, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X", endTLI,
                                 (uint32)((endLogSegNo) / XLogSegmentsPerXLogId),
                                 (uint32)((endLogSegNo) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");

            XLogArchiveNotify(xlogpath);
        }
    }

    /*
     * Let's just make real sure there are not .ready or .done flags posted
     * for the new segment.
     */
    errorno = snprintf_s(xlogpath, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((endLogSegNo) / XLogSegmentsPerXLogId),
                         (uint32)((endLogSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    XLogArchiveCleanup(xlogpath);

    // Since there might be a partial WAL segment named RECOVERYXLOG, get rid of it.
    errorno = snprintf_s(recoveryPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/RECOVERYXLOG");
    securec_check_ss(errorno, "", "");
    unlink(recoveryPath); /* ignore any error */

    /* Get rid of any remaining recovered timeline-history file, too */
    errorno = snprintf_s(recoveryPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/RECOVERYHISTORY");
    securec_check_ss(errorno, "", "");
    unlink(recoveryPath); /* ignore any error */

    /* If recovery.conf doesn't exist, the server is in standby mode */
    if (stat(RECOVERY_COMMAND_FILE, &statbuf) != 0) {
        ereport(LOG, (errmsg("archive recovery complete")));
        return;
    }

    /*
     * Rename the config file out of the way, so that we don't accidentally
     * re-enter archive recovery mode in a subsequent crash.
     */
    unlink(RECOVERY_COMMAND_DONE);
    durable_rename(RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE, FATAL);

    ereport(LOG, (errmsg("archive recovery complete")));
}

/*
 * Remove WAL files that are not part of the given timeline's history.
 *
 * This is called during recovery, whenever we switch to follow a new
 * timeline, and at the end of recovery when we create a new timeline. We
 * wouldn't otherwise care about extra WAL files lying in pg_xlog, but they
 * can be pre-allocated or recycled WAL segments on the old timeline that we
 * haven't used yet, and contain garbage. If we just leave them in pg_xlog,
 * they will eventually be archived, and we can't let that happen. Files that
 * belong to our timeline history are valid, because we have successfully
 * replayed them, but from others we can't be sure.
 *
 * 'switchpoint' is the current point in WAL where we switch to new timeline,
 * and 'newTLI' is the new timeline we switch to.
 */
static void RemoveNonParentXlogFiles(XLogRecPtr switchpoint, TimeLineID newTLI)
{
    DIR *xldir = NULL;
    struct dirent *xlde = NULL;
    char switchseg[MAXFNAMELEN];
    XLogSegNo endLogSegNo;
    errno_t errorno = EOK;

    XLByteToPrevSeg(switchpoint, endLogSegNo);

    xldir = AllocateDir(XLOGDIR);
    if (xldir == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open transaction log directory \"%s\": %m", XLOGDIR)));
    }

    // Construct a filename of the last segment to be kept.
    errorno = snprintf_s(switchseg, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", newTLI,
                         (uint32)((endLogSegNo) / XLogSegmentsPerXLogId),
                         (uint32)((endLogSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    ereport(DEBUG2, (errmsg("attempting to remove WAL segments newer than log file %s", switchseg)));

    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL) {
        /* Ignore files that are not XLOG segments */
        if (strlen(xlde->d_name) != 24 || strspn(xlde->d_name, "0123456789ABCDEF") != 24) {
            continue;
        }

        /*
         * Remove files that are on a timeline older than the new one we're
         * switching to, but with a segment number >= the first segment on
         * the new timeline.
         */
        if (strncmp(xlde->d_name, switchseg, 8) < 0 && strcmp(xlde->d_name + 8, switchseg + 8) > 0) {
            /*
             * If the file has already been marked as .ready, however, don't
             * remove it yet. It should be OK to remove it - files that are
             * not part of our timeline history are not required for recovery
             * - but seems safer to let them be archived and removed later.
             */
            if (!XLogArchiveIsReady(xlde->d_name)) {
                RemoveXlogFile(xlde->d_name, switchpoint);
            }
        }
    }

    FreeDir(xldir);
    xldir = NULL;
}

/**
 * @Description: After date restore, truncate XLOG after barrier lsn of standby DN.
 * off - The offset of the XLOG log needs to be cleared .
 */
void TruncateXLogForRoachRestore(const char *XLogFileName, uint32 off)
{
    int fd = 0;
    char *writeContent = NULL;
    uint32 truncateLength = 0;
    char XLogFilePath[MAX_PATH_LEN] = {0};
    errno_t rc = EOK;

    rc = snprintf_s(XLogFilePath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/pg_xlog/%s", t_thrd.proc_cxt.DataDir,
                    XLogFileName);
    securec_check_ss(rc, "", "");

    fd = BasicOpenFile(XLogFilePath,
                       O_RDWR | PG_BINARY | (unsigned int)get_sync_bit(u_sess->attr.attr_storage.sync_method),
                       S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open file \"%s\"", XLogFilePath)));
    }

    if (lseek(fd, (off_t)off, SEEK_SET) < 0) {
        fprintf(stderr, "lseek error !\n");
        close(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("lseek file error \"%s\" ", XLogFilePath)));
        return;
    }

    truncateLength = XLogSegSize - off;
    writeContent = (char *)palloc0(truncateLength);

    if (write(fd, writeContent, truncateLength) != truncateLength) {
        close(fd);
        pfree(writeContent);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\" ", XLogFilePath)));
        return;
    }
    if (fsync(fd) != 0) {
        close(fd);
        pfree(writeContent);
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync file \"%s\" ", XLogFilePath)));
        return;
    }
    if (close(fd)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\" ", XLogFilePath)));
    }
    pfree(writeContent);
    if (IS_DISASTER_RECOVER_MODE == false)
        durable_rename(RECOVERY_COMMAND_FILE, RECOVERY_COMMAND_DONE, FATAL);

    return;
}

static bool xlogCanStop(XLogReaderState *record)
{
    /* We only consider stoppping at COMMIT, ABORT or BARRIER records */
    return (XLogRecGetRmid(record) != RM_XACT_ID && XLogRecGetRmid(record) != RM_BARRIER_ID &&
            XLogRecGetRmid(record) != RM_XLOG_ID);
}

/*
 * For point-in-time recovery, this function decides whether we want to
 * stop applying the XLOG at or after the current record.
 *
 * Returns TRUE if we are stopping, FALSE otherwise.  On TRUE return,
 * *includeThis is set TRUE if we should apply this record before stopping.
 *
 * We also track the timestamp of the latest applied COMMIT/ABORT
 * record in XLogCtl->recoveryLastXTime, for logging purposes.
 * Also, some information is saved in recoveryStopXid et al for use in
 * annotating the new timeline's history file.
 */
static bool recoveryStopsHere(XLogReaderState *record, bool *includeThis)
{
    bool stopsHere = false;
    char recordRPName[MAXFNAMELEN] = {0};
    uint8 record_info;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    errno_t rc = EOK;
#ifdef PGXC
    bool stopsAtThisBarrier = false;
#endif
#ifdef PGXC
    TimestampTz recordXtime = 0;
#else
    TimestampTz recordXtime;
#endif
    char *recordBarrierId = NULL;
    char xlogFileName[1024] = {0};
    if (xlogCanStop(record)) {
        return false;
    }

    record_info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
#ifdef ENABLE_MULTIPLE_NODES
    if (XLogRecGetRmid(record) == RM_XACT_ID) {
#endif
        if (XLogRecGetRmid(record) == RM_XACT_ID && record_info == XLOG_XACT_COMMIT_COMPACT) {
            xl_xact_commit_compact *recordXactCommitData = (xl_xact_commit_compact *)XLogRecGetData(record);
            recordXtime = recordXactCommitData->xact_time;
        } else if (XLogRecGetRmid(record) == RM_XACT_ID && record_info == XLOG_XACT_COMMIT) {
            xl_xact_commit *recordXactCommitData = (xl_xact_commit *)XLogRecGetData(record);
            recordXtime = recordXactCommitData->xact_time;
        } else if (XLogRecGetRmid(record) == RM_XACT_ID && record_info == XLOG_XACT_ABORT) {
            xl_xact_abort *recordXactAbortData = (xl_xact_abort *)XLogRecGetData(record);
            recordXtime = recordXactAbortData->xact_time;
        }
#ifdef ENABLE_MULTIPLE_NODES
        /* end if (record->xl_rmid == RM_XACT_ID) */
    }
#endif
    else if (XLogRecGetRmid(record) == RM_BARRIER_ID) {
        if (record_info == XLOG_BARRIER_CREATE) {
            recordBarrierId = (char *)XLogRecGetData(record);
            ereport(DEBUG2, (errmsg("processing barrier xlog record for %s", recordBarrierId)));
            if (IS_DISASTER_RECOVER_MODE)
                update_recovery_barrier();
        }
    }

    else if (XLogRecGetRmid(record) == RM_XLOG_ID && record_info == XLOG_RESTORE_POINT) {
        xl_restore_point *recordRestorePointData = (xl_restore_point *)XLogRecGetData(record);
        recordXtime = recordRestorePointData->rp_time;
        rc = strncpy_s(recordRPName, MAXFNAMELEN, recordRestorePointData->rp_name, MAXFNAMELEN - 1);
        securec_check(rc, "", "");
    } else {
        return false;
    }

    /* Check if target LSN has been reached */
    if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_LSN &&
        record->ReadRecPtr >= t_thrd.xlog_cxt.recoveryTargetLSN) {
        *includeThis = t_thrd.xlog_cxt.recoveryTargetInclusive;

        t_thrd.xlog_cxt.recoveryStopAfter = *includeThis;
        t_thrd.xlog_cxt.recoveryStopXid = InvalidTransactionId;
        t_thrd.xlog_cxt.recoveryStopLSN = record->ReadRecPtr;
        t_thrd.xlog_cxt.recoveryStopTime = 0;
        t_thrd.xlog_cxt.recoveryStopName[0] = '\0';

        if (t_thrd.xlog_cxt.recoveryStopAfter) {
            ereport(LOG,
                (errmsg("recovery stopping after WAL location (LSN) \"%X/%X\"",
                    (uint32)(t_thrd.xlog_cxt.recoveryStopLSN >> 32),
                    (uint32)t_thrd.xlog_cxt.recoveryStopLSN)));
        } else {
            ereport(LOG,
                (errmsg("recovery stopping before WAL location (LSN) \"%X/%X\"",
                    (uint32)(t_thrd.xlog_cxt.recoveryStopLSN >> 32),
                    (uint32)t_thrd.xlog_cxt.recoveryStopLSN)));
        }

        return true;
    }

    /* Do we have a PITR target at all? */
    if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_UNSET) {
        // Save timestamp of latest transaction commit/abort if this is a
        // transaction record
        if (strlen((char *)walrcv->recoveryStopBarrierId) == 0) {
            if (XLogRecGetRmid(record) == RM_XACT_ID) {
            SetLatestXTime(recordXtime);
            }
            return false;
        }
        else {
            t_thrd.xlog_cxt.recoveryTarget = RECOVERY_TARGET_BARRIER;
        }
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID) {
#else
    if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID  && XLogRecGetRmid(record) == RM_XACT_ID) {
#endif
        /*
         * There can be only one transaction end record with this exact
         * transactionid
         *
         * when testing for an xid, we MUST test for equality only, since
         * transactions are numbered in the order they start, not the order
         * they complete. A higher numbered xid will complete before you about
         * 50% of the time...
         */
        stopsHere = (XLogRecGetXid(record) == t_thrd.xlog_cxt.recoveryTargetXid);
        if (stopsHere) {
            *includeThis = t_thrd.xlog_cxt.recoveryTargetInclusive;
        }
    } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_BARRIER) {
        stopsHere = false;
        if ((XLogRecGetRmid(record) == RM_BARRIER_ID) && (record_info == XLOG_BARRIER_CREATE)) {
            ereport(DEBUG2, (errmsg("checking if barrier record matches the target barrier")));
            if ((t_thrd.xlog_cxt.recoveryTargetBarrierId != NULL && recordBarrierId != NULL &&
                strcmp(t_thrd.xlog_cxt.recoveryTargetBarrierId, recordBarrierId) == 0)
                ||(recordBarrierId != NULL &&
                strcmp((char *)walrcv->recoveryStopBarrierId, recordBarrierId) == 0)) {
                stopsAtThisBarrier = true;
                uint32 xlogOff;
                XLogSegNo xlogsegno;

                /* truncate XLOG after barrier lsn of standby DN. */
                if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE || IS_DISASTER_RECOVER_MODE) {
                    XLByteToSeg(record->EndRecPtr, xlogsegno);
                    XLogFileName(xlogFileName, record->readPageTLI, xlogsegno);
                    xlogOff = (uint32)(record->EndRecPtr) % XLogSegSize;

                    TruncateXLogForRoachRestore(xlogFileName, xlogOff);
                    xlogOff = (uint32)(record->EndRecPtr) % XLOG_BLCKSZ;
                    if (xlogOff > 0 && xlogOff < XLOG_BLCKSZ) {
                        rc = memset_s(&record->readBuf[xlogOff], XLOG_BLCKSZ - xlogOff, 0, XLOG_BLCKSZ - xlogOff);
                        securec_check(rc, "", "");
                    }
                }
            }
        }
    } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_NAME) {
        /*
         * There can be many restore points that share the same name, so we
         * stop at the first one
         */
        stopsHere = (strcmp(recordRPName, t_thrd.xlog_cxt.recoveryTargetName) == 0);

        // Ignore recoveryTargetInclusive because this is not a transaction record
        *includeThis = false;
#ifdef ENABLE_MULTIPLE_NODES
    } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME) {
#else
    } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME && XLogRecGetRmid(record) == RM_XACT_ID) {
#endif
        /*
         * There can be many transactions that share the same commit time, so
         * we stop after the last one, if we are inclusive, or stop at the
         * first one if we are exclusive
         */
        if (t_thrd.xlog_cxt.recoveryTargetInclusive) {
            stopsHere = (recordXtime > t_thrd.xlog_cxt.recoveryTargetTime);
        } else {
            stopsHere = (recordXtime >= t_thrd.xlog_cxt.recoveryTargetTime);
        }
        if (stopsHere) {
            *includeThis = false;
        }
    }

    if (stopsHere) {
        t_thrd.xlog_cxt.recoveryStopXid = XLogRecGetXid(record);
        t_thrd.xlog_cxt.recoveryStopTime = recordXtime;
        t_thrd.xlog_cxt.recoveryStopAfter = *includeThis;
        t_thrd.xlog_cxt.recoveryStopLSN = InvalidXLogRecPtr;

        if (record_info == XLOG_XACT_COMMIT_COMPACT || record_info == XLOG_XACT_COMMIT) {
            if (t_thrd.xlog_cxt.recoveryStopAfter)
                ereport(LOG, (errmsg("recovery stopping after commit of transaction " XID_FMT ", time %s",
                                     t_thrd.xlog_cxt.recoveryStopXid,
                                     timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
            else
                ereport(LOG, (errmsg("recovery stopping before commit of transaction " XID_FMT ", time %s",
                                     t_thrd.xlog_cxt.recoveryStopXid,
                                     timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
        } else if (record_info == XLOG_XACT_ABORT) {
            if (t_thrd.xlog_cxt.recoveryStopAfter)
                ereport(LOG, (errmsg("recovery stopping after abort of transaction " XID_FMT ", time %s",
                                     t_thrd.xlog_cxt.recoveryStopXid,
                                     timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
            else
                ereport(LOG, (errmsg("recovery stopping before abort of transaction " XID_FMT ", time %s",
                                     t_thrd.xlog_cxt.recoveryStopXid,
                                     timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
        } else {
            rc = strncpy_s(t_thrd.xlog_cxt.recoveryStopName, MAXFNAMELEN, recordRPName, MAXFNAMELEN - 1);
            securec_check(rc, "", "");

            ereport(LOG, (errmsg("recovery stopping at restore point \"%s\", time %s", t_thrd.xlog_cxt.recoveryStopName,
                                 timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
        }

        /*
         * Note that if we use a RECOVERY_TARGET_TIME then we can stop at a
         * restore point since they are timestamped, though the latest
         * transaction time is not updated.
         */
        if (XLogRecGetRmid(record) == RM_XACT_ID && t_thrd.xlog_cxt.recoveryStopAfter) {
            SetLatestXTime(recordXtime);
        }
#ifdef PGXC
    } else if (stopsAtThisBarrier) {
        t_thrd.xlog_cxt.recoveryStopTime = recordXtime;
        ereport(LOG, (errmsg("recovery stopping at barrier %s, time %s", IS_DISASTER_RECOVER_MODE ?
        (char *)walrcv->recoveryStopBarrierId : t_thrd.xlog_cxt.recoveryTargetBarrierId,
                             timestamptz_to_str(t_thrd.xlog_cxt.recoveryStopTime))));
        return true;
#endif
    } else if (XLogRecGetRmid(record) == RM_XACT_ID) {
        SetLatestXTime(recordXtime);
    }

    return stopsHere;
}

/*
 * Wait until shared recoveryPause flag is cleared.
 *
 * XXX Could also be done with shared latch, avoiding the pg_usleep loop.
 * Probably not worth the trouble though.  This state shouldn't be one that
 * anyone cares about server power consumption in.
 */
static void recoveryPausesHere(void)
{
    /* Don't pause unless users can connect! */
    if (!t_thrd.xlog_cxt.LocalHotStandbyActive) {
        return;
    }

    ereport(LOG, (errmsg("recovery has paused"), errhint("Execute pg_xlog_replay_resume() to continue.")));

    while (RecoveryIsPaused()) {
        pg_usleep(1000000L); /* 1000 ms */
        RedoInterruptCallBack();
    }
}

bool RecoveryIsPaused(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    bool recoveryPause = false;

    SpinLockAcquire(&xlogctl->info_lck);
    recoveryPause = xlogctl->recoveryPause;
    SpinLockRelease(&xlogctl->info_lck);

    return recoveryPause;
}

void SetRecoveryPause(bool recoveryPause)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->recoveryPause = recoveryPause;
    SpinLockRelease(&xlogctl->info_lck);
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * When recovery_min_apply_delay is set, we wait long enough to make sure
 * certain record types are applied at least that interval behind the master.
 *
 * Returns true if we waited.
 *
 * Note that the delay is calculated between the WAL record log time and
 * the current time on standby. We would prefer to keep track of when this
 * standby received each WAL record, which would allow a more consistent
 * approach and one not affected by time synchronisation issues, but that
 * is significantly more effort and complexity for little actual gain in
 * usability.
 */
static bool RecoveryApplyDelay(const XLogReaderState *record)
{
    uint8 xactInfo;
    TimestampTz xtime;
    long secs;
    int	microsecs;
    long waitTime = 0;

    /* nothing to do if no delay configured */
    if (u_sess->attr.attr_storage.recovery_min_apply_delay <= 0) {
        return false;
    }

    /* no delay is applied on a database not yet consistent */
    if (!t_thrd.xlog_cxt.reachedConsistency) {
        return false;
    }

    /* nothing to do if crash recovery is requested */
    if (!t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
        return false;
    }

    /* the walreceiver process is started behind here,
     * ensure that the walreceiver process has been started,
     * otherwise, the stream replication will be disconnected */
    if (g_instance.pid_cxt.WalReceiverPID == 0) {
        return false;
    }

    /*
     * Is it a COMMIT record?
     * We deliberately choose not to delay aborts since they have no effect on
     * MVCC. We already allow replay of records that don't have a timestamp,
     * so there is already opportunity for issues caused by early conflicts on
     * standbys
     */
    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }

    xactInfo = XLogRecGetInfo(record) & (~XLR_INFO_MASK);
    if ((xactInfo == XLOG_XACT_COMMIT) || (xactInfo == XLOG_STANDBY_CSN_COMMITTING)) {
        xtime = ((xl_xact_commit *)XLogRecGetData(record))->xact_time;
    } else if (xactInfo == XLOG_XACT_COMMIT_COMPACT) {
        xtime = ((xl_xact_commit_compact *)XLogRecGetData(record))->xact_time;
    } else {
        return false;
    }
    
    u_sess->attr.attr_storage.recoveryDelayUntilTime =
        TimestampTzPlusMilliseconds(xtime, u_sess->attr.attr_storage.recovery_min_apply_delay);
    
    /* Exit without arming the latch if it's already past time to apply this record */
    TimestampDifference(GetCurrentTimestamp(), u_sess->attr.attr_storage.recoveryDelayUntilTime, &secs, &microsecs);
    if (secs <= 0 && microsecs <= 0) {
        return false;
    }

    while (true) {
        ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);

        /* might change the trigger file's location */
        RedoInterruptCallBack();

        if (CheckForFailoverTrigger() || CheckForSwitchoverTrigger() || CheckForStandbyTrigger()) {
            break;
        }

        /* Wait for difference between GetCurrentTimestamp() and recoveryDelayUntilTime */
        TimestampDifference(GetCurrentTimestamp(), u_sess->attr.attr_storage.recoveryDelayUntilTime,
                            &secs, &microsecs);

        /* To clear LSNMarker item that has not been processed  in  pageworker */
        ProcTxnWorkLoad(false);

        /* NB: We're ignoring waits below min_apply_delay's resolution. */
        if (secs <= 0 && microsecs / 1000 <= 0) {
            break;
        }

        /* delay 1s every time */
        waitTime = (secs >= 1) ? 1000 : (microsecs / 1000);
        (void)WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch,
                        WL_LATCH_SET | WL_TIMEOUT, waitTime);
    }
    return true;
}
#endif

/*
 * Save timestamp of latest processed commit/abort record.
 *
 * We keep this in XLogCtl, not a simple static variable, so that it can be
 * seen by processes other than the startup process.  Note in particular
 * that CreateRestartPoint is executed in the checkpointer.
 */
void SetLatestXTime(TimestampTz xtime)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->recoveryLastXTime = xtime;
    SpinLockRelease(&xlogctl->info_lck);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 */
TimestampTz GetLatestXTime(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    TimestampTz xtime;

    SpinLockAcquire(&xlogctl->info_lck);
    xtime = xlogctl->recoveryLastXTime;
    SpinLockRelease(&xlogctl->info_lck);

    return xtime;
}

/*
 * Save timestamp of the next chunk of WAL records to apply.
 *
 * We keep this in XLogCtl, not a simple static variable, so that it can be
 * seen by all backends.
 */
static void SetCurrentChunkStartTime(TimestampTz xtime)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->currentChunkStartTime = xtime;
    SpinLockRelease(&xlogctl->info_lck);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 * Startup process maintains an accurate local copy in XLogReceiptTime
 */
TimestampTz GetCurrentChunkReplayStartTime(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    TimestampTz xtime;

    SpinLockAcquire(&xlogctl->info_lck);
    xtime = xlogctl->currentChunkStartTime;
    SpinLockRelease(&xlogctl->info_lck);

    return xtime;
}

void SetXLogReceiptTime(TimestampTz time)
{
    t_thrd.xlog_cxt.XLogReceiptTime = time;
}

/*
 * Returns time of receipt of current chunk of XLOG data, as well as
 * whether it was received from streaming replication or from archives.
 */
void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream)
{
    /*
     * This must be executed in the startup process, since we don't export the
     * relevant state to shared memory.
     */
    Assert(t_thrd.xlog_cxt.InRecovery);

    *rtime = t_thrd.xlog_cxt.XLogReceiptTime;
    *fromStream = (t_thrd.xlog_cxt.XLogReceiptSource == XLOG_FROM_STREAM);
}

TimestampTz GetXLogReceiptTimeOnly()
{
    return t_thrd.xlog_cxt.XLogReceiptTime;
}

void SetXLogReceiptSource(int source)
{
    t_thrd.xlog_cxt.XLogReceiptSource = source;
}

int GetXLogReceiptSource()
{
    return t_thrd.xlog_cxt.XLogReceiptSource;
}

/*
 * Note that text field supplied is a parameter name and does not require
 * translation
 */
#define RecoveryRequiresIntParameter(param_name, currValue, minValue) do { \
    if ((currValue) < (minValue)) {                                                           \
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),                             \
                        errmsg("hot standby is not possible because "                         \
                               "%s = %d is a lower setting than on the master server "        \
                               "(its value was %d)",                                          \
                               param_name, currValue, minValue),                              \
                        errhint("You might need to increase the value of %s ", param_name))); \
    }                                                                                         \
} while (0)

/*
 * Check to see if required parameters are set high enough on this server
 * for various aspects of recovery operation.
 */
static void CheckRequiredParameterValues(bool DBStateShutdown)
{
    /*
     * For archive recovery, the WAL must be generated with at least 'archive'
     * wal_level.
     */
    if (t_thrd.xlog_cxt.InArchiveRecovery && t_thrd.shemem_ptr_cxt.ControlFile->wal_level == WAL_LEVEL_MINIMAL) {
        ereport(WARNING,
                (errmsg("WAL was generated with wal_level=minimal, data may be missing"),
                 errhint("This happens if you temporarily set wal_level=minimal without taking a new base backup.")));
    }

    /*
     * For Hot Standby, the WAL must be generated with 'hot_standby' mode, and
     * we must have at least as many backend slots as the primary.
     */
    if (t_thrd.xlog_cxt.InArchiveRecovery && g_instance.attr.attr_storage.EnableHotStandby && !DBStateShutdown) {
        if (t_thrd.shemem_ptr_cxt.ControlFile->wal_level < WAL_LEVEL_HOT_STANDBY) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("hot standby is not possible because wal_level was not set to \"hot_standby\" or higher on "
                            "the master server"),
                     errhint("Either set wal_level to \"hot_standby\" on the master, or turn off hot_standby here.")));
        }

        /* We ignore autovacuum_max_workers when we make this test. */
        RecoveryRequiresIntParameter("max_locks_per_transaction", g_instance.attr.attr_storage.max_locks_per_xact,
                                     t_thrd.shemem_ptr_cxt.ControlFile->max_locks_per_xact);
    }
}

void StartupDummyStandby(void)
{
    char conninfo[MAXCONNINFO];
    int replIdx = 1;

    ereport(LOG, (errmsg("database system startup Secondary Standby")));

    ReadControlFile();

    TimeLineID timeline = t_thrd.xlog_cxt.ThisTimeLineID = GetThisTimeID();
    DummyStandbySetRecoveryTargetTLI(timeline);
    sync_system_identifier = GetSystemIdentifier();

    ereport(LOG, (errmsg("timline from control file = %u, systemid = %lu", (uint32)timeline, sync_system_identifier)));

    /* Now we can let the postmaster know we are ready to enter into hot standby. */
    SendPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY);

    do {
        /* Handle interrupt signals of startup process */
        RedoInterruptCallBack();
        /* if we have lost the walreceiver process, try to start a new one */
        if (!WalRcvInProgress()) {
            while (1) {
                int replIdxFromFile = -1;

                if (WalSndInProgress(SNDROLE_DUMMYSTANDBY_STANDBY)) {
                    goto retry;
                }

                /* dead loop till success */
                get_failover_host_conninfo_for_dummy(&replIdxFromFile);
                if (replIdxFromFile < 0) {
                    replIdx = ((replIdx - 1) % REP_CONN_ARRAY) + 1;
                } else {
                    replIdx = (replIdxFromFile % REP_CONN_ARRAY) + 1;
                }
                connect_dn_str(conninfo, replIdx);
                replIdx++;

                RedoInterruptCallBack();
                if (libpqrcv_connect_for_TLI(&timeline, conninfo)) {
                    ereport(LOG, (errmsg("timline from primary = %u", (uint32)timeline)));
                    DummyStandbySetRecoveryTargetTLI(timeline);
                    SetThisTimeID(timeline);
                    UpdateControlFile();
                    break;
                }

                sleep(DUMMYSTANDBY_CONNECT_INTERVAL);
            }

            /* For dummystandby, startpos is not required, Primary will choose the startpos */
            ShutdownWalRcv();

            XLogRecPtr startpos = 0;
            RequestXLogStreaming(&startpos, conninfo, REPCONNTARGET_DEFAULT, u_sess->attr.attr_storage.PrimarySlotName);
        }

        if (!g_instance.attr.attr_storage.enable_mix_replication && !IS_DN_MULTI_STANDYS_MODE()) {
            /* if we have lost the datareceiver process, try to start a new one */
            if (!DataRcvInProgress()) {
                if (DataSndInProgress(SNDROLE_DUMMYSTANDBY_STANDBY)) {
                    goto retry;
                }

                RequestDataStreaming(conninfo, REPCONNTARGET_DEFAULT);
            }
        }
    retry:
        sleep(DUMMYSTANDBY_CHECK_INTERVAL);
    } while (1);
}

void ResourceManagerStartup(void)
{
    /* Initialize resource managers */
    for (uint32 rmid = 0; rmid <= RM_MAX_ID; rmid++) {
        if (RmgrTable[rmid].rm_startup != NULL) {
            RmgrTable[rmid].rm_startup();
        }
    }
}

void ResourceManagerStop(void)
{
    // Allow resource managers to do any required cleanup.
    for (uint32 rmid = 0; rmid <= RM_MAX_ID; rmid++) {
        if (RmgrTable[rmid].rm_cleanup != NULL) {
            RmgrTable[rmid].rm_cleanup();
        }
    }
}

#define RecoveryXlogReader(_oldXlogReader, _xlogreader) do { \
    if (get_real_recovery_parallelism() > 1) {                                                 \
        if (GetRedoWorkerCount() > 0) {                                                        \
            errno_t errorno;                                                                   \
            (_oldXlogReader)->ReadRecPtr = (_xlogreader)->ReadRecPtr;                          \
            (_oldXlogReader)->EndRecPtr = (_xlogreader)->EndRecPtr;                            \
            (_oldXlogReader)->readSegNo = (_xlogreader)->readSegNo;                            \
            (_oldXlogReader)->readOff = (_xlogreader)->readOff;                                \
            (_oldXlogReader)->readLen = (_xlogreader)->readLen;                                \
            (_oldXlogReader)->readPageTLI = (_xlogreader)->readPageTLI;                        \
            (_oldXlogReader)->curReadOff = (_xlogreader)->curReadOff;                          \
            (_oldXlogReader)->curReadSegNo = (_xlogreader)->curReadSegNo;                      \
            (_oldXlogReader)->currRecPtr = (_xlogreader)->currRecPtr;                          \
            (_oldXlogReader)->latestPagePtr = (_xlogreader)->latestPagePtr;                    \
            (_oldXlogReader)->latestPageTLI = (_xlogreader)->latestPageTLI;                    \
            (_oldXlogReader)->isPRProcess = false;                                             \
            errorno = memcpy_s((_oldXlogReader)->readBuf, XLOG_BLCKSZ, (_xlogreader)->readBuf, \
                               (_oldXlogReader)->readLen);                                     \
            securec_check(errorno, "", "");                                                    \
            ResetDecoder(_oldXlogReader);                                                      \
            (_xlogreader) = (_oldXlogReader);                                                  \
        }                                                                                      \
        EndDispatcherContext();                                                                \
    }                                                                                          \
} while (0)

static void EndRedoXlog()
{
#ifndef ENABLE_MULTIPLE_NODES
    if (IsExtremeRtoRunning()) {
        extreme_rto::CheckCommittingCsnList();
    }
#endif

    if ((get_real_recovery_parallelism() > 1) && (!parallel_recovery::DispatchPtrIsNull())) {
        SwitchToDispatcherContext();
        t_thrd.xlog_cxt.incomplete_actions =
            parallel_recovery::CheckImcompleteAction(t_thrd.xlog_cxt.incomplete_actions);
        FreeAllocatedRedoItem();
        EndDispatcherContext();
    }
    ResourceManagerStop();
    XLogWaitFlush(t_thrd.xlog_cxt.LogwrtResult->Write);
}

int XLogSemas(void)
{
    return 4 + 2 * g_instance.attr.attr_storage.max_wal_senders;
}

/*
 * Log checkPoint
 */
inline void PrintCkpXctlControlFile(XLogRecPtr oldCkpLoc, CheckPoint *oldCkp, XLogRecPtr newCkpLoc, CheckPoint *newCkp,
                                    XLogRecPtr preCkpLoc, char *name)
{
    ereport(LOG,
            (errmsg("%s PrintCkpXctlControlFile: [checkPoint] oldCkpLoc:%X/%X, oldRedo:%X/%X, newCkpLoc:%X/%X, "
                    "newRedo:%X/%X, preCkpLoc:%X/%X",
                    name, (uint32)(oldCkpLoc >> 32), (uint32)oldCkpLoc, (uint32)(oldCkp->redo >> 32),
                    (uint32)oldCkp->redo, (uint32)(newCkpLoc >> 32), (uint32)newCkpLoc, (uint32)(newCkp->redo >> 32),
                    (uint32)newCkp->redo, (uint32)(preCkpLoc >> 32), (uint32)preCkpLoc)));
}

void CheckForRestartPoint()
{
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    
    if (XLByteLT(xlogctl->lastCheckPointRecPtr, g_instance.comm_cxt.predo_cxt.newestCheckpointLoc)) {
        Assert(XLByteLT(xlogctl->lastCheckPoint.redo, g_instance.comm_cxt.predo_cxt.newestCheckpoint.redo));
        Assert(g_instance.comm_cxt.predo_cxt.newestCheckpoint.nextXid >= xlogctl->lastCheckPoint.nextXid);
        Assert(g_instance.comm_cxt.predo_cxt.newestCheckpoint.nextOid >= xlogctl->lastCheckPoint.nextOid);

        SpinLockAcquire(&xlogctl->info_lck);
        xlogctl->lastCheckPointRecPtr = g_instance.comm_cxt.predo_cxt.newestCheckpointLoc;
        xlogctl->lastCheckPoint = const_cast<CheckPoint &>(g_instance.comm_cxt.predo_cxt.newestCheckpoint);
        SpinLockRelease(&xlogctl->info_lck);
    }

    /*
     * Perform a checkpoint to update all our recovery activity to disk.
     *
     * Note that we write a shutdown checkpoint rather than an on-line
     * one. This is not particularly critical, but since we may be
     * assigning a new TLI, using a shutdown checkpoint allows us to have
     * the rule that TLI only changes in shutdown checkpoints, which
     * allows some extra error checking in xlog_redo.
     *
     * In most scenarios (except new timeline switch), the checkpoint
     * seems unnecessary 'cause we need to improve the performance of
     * switchover. We transfer the checkpoint to the checkpoint process
     * which seems effective. However this would lead to new issue
     * about dirty pages without checkpoint before we do the first
     * checkpoint. So we temporarily switch to full-page write mode,
     * and turn it off after the first checkpoint.
     */
    if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
        if (t_thrd.xlog_cxt.bgwriterLaunched || t_thrd.xlog_cxt.pagewriter_launched) {
            RequestCheckpoint(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);
        } else {
            CreateCheckPoint(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE);
        }
    } else {
        t_thrd.shemem_ptr_cxt.XLogCtl->FpwBeforeFirstCkpt = true;
        if (t_thrd.xlog_cxt.bgwriterLaunched || t_thrd.xlog_cxt.pagewriter_launched) {
            RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE);
        }
    }
}

/*
 * When adding or decreasing semaphores used by xlog in the following function
 * please update XLogSemas accordingly
 */
void InitWalSemaphores()
{
    /* Create a memory context and allocate semaphores used by wal writer. */
    g_instance.wal_context = AllocSetContextCreate(g_instance.instance_context, "WalContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    MemoryContext old_context = MemoryContextSwitchTo(g_instance.wal_context);

    /*
     * Lock structure used for XLog local flush notification
     */
    g_instance.wal_cxt.walFlushWaitLock = (WALFlushWaitLockPadded *)palloc(sizeof(WALFlushWaitLockPadded) * 2);
    g_instance.wal_cxt.walFlushWaitLock = (WALFlushWaitLockPadded *)TYPEALIGN(sizeof(WALFlushWaitLockPadded),
        g_instance.wal_cxt.walFlushWaitLock);
    g_instance.wal_cxt.walFlushWaitLock->l.lock = LWLockAssign(LWTRANCHE_WAL_FLUSH_WAIT);
    PGSemaphoreCreate(&g_instance.wal_cxt.walFlushWaitLock->l.sem);
    PGSemaphoreReset(&g_instance.wal_cxt.walFlushWaitLock->l.sem);

    /*
     * Lock structure used for WAL buffer initialization notification
     */
    g_instance.wal_cxt.walBufferInitWaitLock =
        (WALBufferInitWaitLockPadded *)palloc(sizeof(WALBufferInitWaitLockPadded) * 2);
    g_instance.wal_cxt.walBufferInitWaitLock =
        (WALBufferInitWaitLockPadded *)TYPEALIGN(sizeof(WALBufferInitWaitLockPadded),
            g_instance.wal_cxt.walBufferInitWaitLock);
    g_instance.wal_cxt.walBufferInitWaitLock->l.lock = LWLockAssign(LWTRANCHE_WAL_BUFFER_INIT_WAIT);
    PGSemaphoreCreate(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem);
    PGSemaphoreReset(&g_instance.wal_cxt.walBufferInitWaitLock->l.sem);

    /*
     * Lock structure used for XLog init-ahead notification
     */
    g_instance.wal_cxt.walInitSegLock = (WALInitSegLockPadded *)palloc(sizeof(WALInitSegLockPadded) * 2);
    g_instance.wal_cxt.walInitSegLock = (WALInitSegLockPadded *)TYPEALIGN(sizeof(WALInitSegLockPadded),
        g_instance.wal_cxt.walInitSegLock);
    g_instance.wal_cxt.walInitSegLock->l.lock = LWLockAssign(LWTRANCHE_WAL_INIT_SEGMENT);
    PGSemaphoreCreate(&g_instance.wal_cxt.walInitSegLock->l.sem);
    PGSemaphoreReset(&g_instance.wal_cxt.walInitSegLock->l.sem);

    (void)MemoryContextSwitchTo(old_context);
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
void StartupXLOG(void)
{
    XLogCtlInsert *Insert = NULL;
    CheckPoint checkPoint;
    CheckPointNew checkPointNew;   /* to adapt update and not to modify the storage format */
    CheckPointPlus checkPointPlus; /* to adapt update and not to modify the storage format for global clean */
    uint32 recordLen = 0;
    bool wasShutdown = false;
    bool DBStateShutdown = false;
    bool reachedStopPoint = false;
    bool haveBackupLabel = false;
    bool haveTblspcMap = false;
    XLogRecPtr RecPtr, checkPointLoc, EndOfLog;
    XLogSegNo endLogSegNo;
    XLogRecord *record = NULL;
    TransactionId oldestActiveXID;
    bool backupEndRequired = false;
    bool backupFromStandby = false;
    bool backupFromRoach = false;
    DBState dbstate_at_startup;
    XLogReaderState *xlogreader = NULL;
    XLogPageReadPrivate readprivate;
    bool RecoveryByPending = false;        /* recovery caused by pending mode */
    bool ArchiveRecoveryByPending = false; /* archive recovery caused by pending mode */
    bool AbnormalShutdown = true;
    struct stat st;
    errno_t rcm = 0;
    TransactionId latestCompletedXid;
    bool wasCheckpoint = false;
    errno_t errorno = EOK;
    XLogRecPtr max_page_flush_lsn = MAX_XLOG_REC_PTR;
    checkPointNew.next_csn = COMMITSEQNO_FIRST_NORMAL + 1;
    checkPointPlus.next_csn = COMMITSEQNO_FIRST_NORMAL + 1;
    checkPointPlus.length = 0;
    checkPointPlus.recent_global_xmin = InvalidTransactionId;
    t_thrd.xlog_cxt.startup_processing = true;
    t_thrd.xlog_cxt.RedoDone = false;
    t_thrd.xlog_cxt.forceFinishHappened = false;
    g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr = 0;
    g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time = 0;
    pg_atomic_write_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo), 0);

    /*
     * Initialize WAL insert status array and the flush index - lastWalStatusEntryFlushed.
     */
    if (g_instance.wal_cxt.walInsertStatusTable == NULL) {
        MemoryContext old_context = MemoryContextSwitchTo(g_instance.wal_context);

        g_instance.wal_cxt.walInsertStatusTable =
            (WALInsertStatusEntry *)palloc(sizeof(WALInsertStatusEntry) * (WAL_INSERT_STATUS_ENTRIES + 1));
        g_instance.wal_cxt.walInsertStatusTable = (WALInsertStatusEntry *)TYPEALIGN(sizeof(WALInsertStatusEntry),
            g_instance.wal_cxt.walInsertStatusTable);

        (void)MemoryContextSwitchTo(old_context);
    }

    errorno = memset_s(g_instance.wal_cxt.walInsertStatusTable,
        sizeof(WALInsertStatusEntry) * WAL_INSERT_STATUS_ENTRIES, 0,
        sizeof(WALInsertStatusEntry) * WAL_INSERT_STATUS_ENTRIES);
    securec_check(errorno, "", "");

    for (int i = 0; i < WAL_INSERT_STATUS_ENTRIES; i++) {
        g_instance.wal_cxt.walInsertStatusTable[i].LRC = i;
    }

    g_instance.wal_cxt.lastWalStatusEntryFlushed = -1;
    g_instance.wal_cxt.lastLRCScanned = WAL_SCANNED_LRC_INIT;
    g_instance.wal_cxt.lastLRCFlushed = WAL_SCANNED_LRC_INIT;

    /*
     * Read control file and check XLOG status looks valid.
     *
     * Note: in most control paths, *ControlFile is already valid and we need
     * not do ReadControlFile() here, but might as well do it to be sure.
     */
    ReadControlFile();
    if (force_finish_enabled()) {
        max_page_flush_lsn = mpfl_read_max_flush_lsn();
        /* we can't exit proc here, because init gaussdb will run through here and there must be no LsnInfoFile. */
        ereport(LOG,
                (errcode_for_file_access(), errmsg("StartupXLOG: read page lsn info from file, LSN: %X/%X.",
                                                   (uint32)(max_page_flush_lsn >> 32), (uint32)max_page_flush_lsn)));
        if (max_page_flush_lsn == MAX_XLOG_REC_PTR) {
            mpfl_new_file();
        } else {
            SetMinRecoverPointForStats(max_page_flush_lsn);
        }
    } else {
        mpfl_ulink_file();
    }
    ereport(LOG, (errcode(ERRCODE_LOG),
                  errmsg("StartupXLOG: biggest_lsn_in_page is set to %X/%X, enable_update_max_page_flush_lsn:%u",
                         (uint32)(max_page_flush_lsn >> 32), (uint32)max_page_flush_lsn,
                         g_instance.attr.attr_storage.enable_update_max_page_flush_lsn)));
    t_thrd.xlog_cxt.max_page_flush_lsn = max_page_flush_lsn;
    pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.permitFinishRedo), ATOMIC_FALSE);
    set_global_max_page_flush_lsn(max_page_flush_lsn);
    /* description: timeline > MAX_INT32 */
    if (IsUnderPostmaster) {
        t_thrd.shemem_ptr_cxt.ControlFile->timeline = t_thrd.shemem_ptr_cxt.ControlFile->timeline + 1;

        ereport(LOG, (errmsg("database system timeline: %u", t_thrd.shemem_ptr_cxt.ControlFile->timeline)));

        if (t_thrd.shemem_ptr_cxt.ControlFile->timeline == 0) {
            t_thrd.shemem_ptr_cxt.ControlFile->timeline = 1;
        }
    }

    if (t_thrd.shemem_ptr_cxt.ControlFile->state < DB_SHUTDOWNED ||
        t_thrd.shemem_ptr_cxt.ControlFile->state > DB_IN_PRODUCTION ||
        !XRecOffIsValid(t_thrd.shemem_ptr_cxt.ControlFile->checkPoint)) {
        ereport(FATAL, (errmsg("control file contains invalid data")));
    }

    if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_SHUTDOWNED) {
        DBStateShutdown = true;
        AbnormalShutdown = false;
        ereport(LOG,
                (errmsg("database system was shut down at %s", str_time(t_thrd.shemem_ptr_cxt.ControlFile->time))));
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_SHUTDOWNED_IN_RECOVERY) {
        DBStateShutdown = true;
        ereport(LOG, (errmsg("database system was shut down in recovery at %s",
                             str_time(t_thrd.shemem_ptr_cxt.ControlFile->time))));
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_SHUTDOWNING) {
        ereport(LOG, (errmsg("database system shutdown was interrupted; last known up at %s",
                             str_time(t_thrd.shemem_ptr_cxt.ControlFile->time))));
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_CRASH_RECOVERY) {
        ereport(LOG, (errmsg("database system was interrupted while in recovery at %s",
                             str_time(t_thrd.shemem_ptr_cxt.ControlFile->time)),
                      errhint("This probably means that some data is corrupted and"
                              " you will have to use the last backup for recovery.")));
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_ARCHIVE_RECOVERY) {
        ereport(LOG, (errmsg("database system was interrupted while in recovery at log time %s",
                             str_time(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.time)),
                      errhint("If this has occurred more than once some data might be corrupted"
                              " and you might need to choose an earlier recovery target.")));
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_PRODUCTION) {
        ereport(LOG, (errmsg("database system was interrupted; last known up at %s",
                             str_time(t_thrd.shemem_ptr_cxt.ControlFile->time))));
    }

    /* This is just to allow attaching to startup process with a debugger */
#ifdef XLOG_REPLAY_DELAY
    if (t_thrd.shemem_ptr_cxt.ControlFile->state != DB_SHUTDOWNED) {
        pg_usleep(60000000L);
    }
#endif

    /*
     * Verify that pg_xlog and pg_xlog/archive_status exist.  In cases where
     * someone has performed a copy for PITR, these directories may have been
     * excluded and need to be re-created.
     */
    ValidateXLOGDirectoryStructure();

    /* delete xlogtemp files. */
    remove_xlogtemp_files();
    /*
     * Clear out any old relcache cache files.	This is *necessary* if we do
     * any WAL replay, since that would probably result in the cache files
     * being out of sync with database reality.  In theory we could leave them
     * in place if the database had been cleanly shut down, but it seems
     * safest to just remove them always and let them be rebuilt during the
     * first backend startup.
     */
    RelationCacheInitFileRemove();

    /*
     * Initialize on the assumption we want to recover to the same timeline
     * that's active according to pg_control.
     */
    t_thrd.xlog_cxt.recoveryTargetTLI = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID;

    /* clean temp relation files */
    if (u_sess->attr.attr_storage.max_active_gtt > 0) {
        RemovePgTempFiles();
    }

    // Check for recovery control file, and if so set up state for offline recovery
    readRecoveryCommandFile();

    if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
        t_thrd.xlog_cxt.ArchiveRecoveryRequested = true;
    }

    load_server_mode();

    if (t_thrd.xlog_cxt.server_mode == PENDING_MODE && !t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
        ArchiveRecoveryByPending = true;
    }

    if (t_thrd.xlog_cxt.server_mode == PENDING_MODE || t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
        t_thrd.xlog_cxt.ArchiveRecoveryRequested = true;
        t_thrd.xlog_cxt.StandbyModeRequested = true;
    }
    /* Now we can determine the list of expected TLIs */
    t_thrd.xlog_cxt.expectedTLIs = readTimeLineHistory(t_thrd.xlog_cxt.recoveryTargetTLI);

    /*
     * If pg_control's timeline is not in expectedTLIs, then we cannot
     * proceed: the backup is not part of the history of the requested
     * timeline.
     */
    if (!list_member_int(t_thrd.xlog_cxt.expectedTLIs,
                         (int)t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID)) {
        ereport(FATAL, (errmsg("requested timeline %u is not a child of database system timeline %u",
                               t_thrd.xlog_cxt.recoveryTargetTLI,
                               t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID)));
    }

    /*
     * Save the selected recovery target timeline ID and
     * archive_cleanup_command in shared memory so that other processes can
     * see them
     */
    t_thrd.shemem_ptr_cxt.XLogCtl->RecoveryTargetTLI = t_thrd.xlog_cxt.recoveryTargetTLI;
    errorno = strncpy_s(t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand,
                        sizeof(t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand) - 1,
                        t_thrd.xlog_cxt.archiveCleanupCommand ? t_thrd.xlog_cxt.archiveCleanupCommand : "",
                        sizeof(t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand) - 1);
    securec_check(errorno, "", "");
    t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand[MAXPGPATH - 1] = '\0';

    if (t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
        if (t_thrd.xlog_cxt.StandbyModeRequested) {
            ereport(LOG, (errmsg("entering standby mode")));
        } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_XID) {
            ereport(LOG,
                    (errmsg("starting point-in-time recovery to XID " XID_FMT, t_thrd.xlog_cxt.recoveryTargetXid)));
        } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME) {
            ereport(LOG, (errmsg("starting point-in-time recovery to %s",
                                 timestamptz_to_str(t_thrd.xlog_cxt.recoveryTargetTime))));
        }
#ifdef PGXC
        else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_BARRIER) {
            ereport(LOG, (errmsg("starting point-in-time recovery to barrier %s",
                                 (t_thrd.xlog_cxt.recoveryTargetBarrierId))));
        }
#endif
        else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_NAME) {
            ereport(LOG, (errmsg("starting point-in-time recovery to \"%s\"", t_thrd.xlog_cxt.recoveryTargetName)));
        } else if (t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_LSN) {
            ereport(LOG,
                (errmsg("starting point-in-time recovery to WAL location (LSN) \"%X/%X\"",
                    (uint32)(t_thrd.xlog_cxt.recoveryTargetLSN >> 32),
                    (uint32)t_thrd.xlog_cxt.recoveryTargetLSN)));
        } else {
            ereport(LOG, (errmsg("starting archive recovery")));
        }
    }

    /*
     * Take ownership of the wakeup latch if we're going to sleep during
     * recovery.
     */
    if (t_thrd.xlog_cxt.StandbyModeRequested) {
        OwnLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
        OwnLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->dataRecoveryLatch);
    }

    /* Set up XLOG reader facility */
    errorno = memset_s(&readprivate, sizeof(XLogPageReadPrivate), 0, sizeof(XLogPageReadPrivate));
    securec_check(errorno, "", "");

    xlogreader = XLogReaderAllocate(&XLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor")));
    }
    xlogreader->system_identifier = t_thrd.shemem_ptr_cxt.ControlFile->system_identifier;

    g_instance.comm_cxt.predo_cxt.redoPf.redo_start_time = GetCurrentTimestamp();
    if (read_backup_label(&checkPointLoc, &backupEndRequired, &backupFromStandby, &backupFromRoach)) {
        List* tablespaces = NIL;
        /*
         * Archive recovery was requested, and thanks to the backup label file,
         * we know how far we need to replay to reach consistency. Enter
         * archive recovery directly.
         */
        t_thrd.xlog_cxt.InArchiveRecovery = true;
        if (t_thrd.xlog_cxt.StandbyModeRequested) {
            t_thrd.xlog_cxt.StandbyMode = true;
        }

        ereport(LOG, (errmsg("request archive recovery due to backup label file")));

        /*
         * When a backup_label file is present, we want to roll forward from
         * the checkpoint it identifies, rather than using pg_control.
         */
        record = ReadCheckpointRecord(xlogreader, checkPointLoc, 0);
        if (record != NULL) {
            rcm = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(xlogreader), sizeof(CheckPoint));
            securec_check(rcm, "", "");

            /* decide which kind of checkpoint is used */
            if (!GTM_MODE) {
                recordLen = record->xl_tot_len;
                if (record->xl_tot_len == CHECKPOINTNEW_LEN) {
                    rcm = memcpy_s(&checkPointNew, sizeof(checkPointNew), XLogRecGetData(xlogreader),
                                   sizeof(checkPointNew));
                    securec_check(rcm, "", "");
                } else if (record->xl_tot_len == CHECKPOINTPLUS_LEN) {
                    rcm = memcpy_s(&checkPointPlus, sizeof(checkPointPlus), XLogRecGetData(xlogreader),
                                   sizeof(checkPointPlus));
                    securec_check(rcm, "", "");
                }
            }

            wasShutdown = (record->xl_info == XLOG_CHECKPOINT_SHUTDOWN);
            if (record->xl_term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
                g_instance.comm_cxt.localinfo_cxt.term_from_xlog = record->xl_term;
            }
            ereport(DEBUG1,
                    (errmsg("checkpoint record is at %X/%X", (uint32)(checkPointLoc >> 32), (uint32)checkPointLoc)));
            t_thrd.xlog_cxt.InRecovery = true; /* force recovery even if SHUTDOWNED */

            /*
             * Make sure that REDO location exists. This may not be the case
             * if there was a crash during an online backup, which left a
             * backup_label around that references a WAL segment that's
             * already been archived.
             */
            if (XLByteLT(checkPoint.redo, checkPointLoc) && !ReadRecord(xlogreader, checkPoint.redo, LOG, false)) {
                ereport(FATAL,
                        (errmsg("could not find redo location referenced by checkpoint record"),
                         errhint("If you are not restoring from a backup, try removing the file \"%s/backup_label\".",
                                 t_thrd.proc_cxt.DataDir)));
            }
        } else {
            ereport(FATAL,
                    (errmsg("could not locate required checkpoint record"),
                     errhint("If you are not restoring from a backup, try removing the file \"%s/backup_label\".",
                             t_thrd.proc_cxt.DataDir)));
            wasShutdown = false; /* keep compiler quiet */
        }

        /* read the tablespace_map file if present and create symlinks. */
        if (read_tablespace_map(&tablespaces)) {
            ListCell* lc;
            errno_t rc = EOK;
            foreach (lc, tablespaces) {
                tablespaceinfo* ti = (tablespaceinfo*)lfirst(lc);
                int length = PG_TBLSPCS + strlen(ti->oid) + 1;
                char* linkloc = (char*)palloc0(length);
                rc = snprintf_s(linkloc, length, length - 1, "pg_tblspc/%s", ti->oid);
                securec_check_ss_c(rc, "", "");
                /*
                 * Remove the existing symlink if any and Create the symlink
                 * under PGDATA.
                 */
                remove_tablespace_symlink(linkloc);

                if (symlink(ti->path, linkloc) < 0) {
                    pfree(linkloc);
                    ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not create symbolic link \"%s\": %m", linkloc)));
                }

                pfree(linkloc);
                pfree(ti->oid);
                pfree(ti->path);
                pfree(ti);
            }

            /* set flag to delete it later */
            haveTblspcMap = true;
        }
        /* set flag to delete it later */
        haveBackupLabel = true;
    } else {
        /*
         * If tablespace_map file is present without backup_label file, there
         * is no use of such file.  There is no harm in retaining it, but it
         * is better to get rid of the map file so that we don't have any
         * redundant file in data directory and it will avoid any sort of
         * confusion.  It seems prudent though to just rename the file out
         * of the way rather than delete it completely, also we ignore any
         * error that occurs in rename operation as even if map file is
         * present without backup_label file, it is harmless.
         */
        if (stat(TABLESPACE_MAP, &st) == 0) {
            unlink(TABLESPACE_MAP_OLD);
            if (durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, DEBUG1) == 0)
                ereport(LOG,
                    (errmsg("ignoring file \"%s\" because no file \"%s\" exists", TABLESPACE_MAP, BACKUP_LABEL_FILE),
                        errdetail("File \"%s\" was renamed to \"%s\".", TABLESPACE_MAP, TABLESPACE_MAP_OLD)));
            else
                ereport(LOG,
                    (errmsg("ignoring \"%s\" file because no \"%s\" file exists", TABLESPACE_MAP, BACKUP_LABEL_FILE),
                        errdetail("Could not rename file \"%s\" to \"%s\": %m.", TABLESPACE_MAP, TABLESPACE_MAP_OLD)));
        }

        /*
         * It's possible that archive recovery was requested, but we don't
         * know how far we need to replay the WAL before we reach consistency.
         * This can happen for example if a base backup is taken from a running
         * server using an atomic filesystem snapshot, without calling
         * pg_start/stop_backup. Or if you just kill a running master server
         * and put it into archive recovery by creating a recovery.conf file.
         *
         * Our strategy in that case is to perform crash recovery first,
         * replaying all the WAL present in pg_xlog, and only enter archive
         * recovery after that.
         *
         * But usually we already know how far we need to replay the WAL (up to
         * minRecoveryPoint, up to backupEndPoint, or until we see an
         * end-of-backup record), and we can enter archive recovery directly.
         */
        bool readoldversion = false;

        if (t_thrd.xlog_cxt.ArchiveRecoveryRequested &&
            (!XLByteEQ(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, InvalidXLogRecPtr) ||
             t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired ||
             !XLByteEQ(t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint, InvalidXLogRecPtr) ||
             t_thrd.shemem_ptr_cxt.ControlFile->state == DB_SHUTDOWNED)) {
            t_thrd.xlog_cxt.InArchiveRecovery = true;
            if (t_thrd.xlog_cxt.StandbyModeRequested) {
                t_thrd.xlog_cxt.StandbyMode = true;
            }
        }

        /*
         * Get the last valid checkpoint record.  If the latest one according
         * to pg_control is broken, try the next-to-last one.
         */
        checkPointLoc = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
        t_thrd.xlog_cxt.RedoStartLSN = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo;
        g_instance.comm_cxt.predo_cxt.redoPf.redo_start_ptr = t_thrd.xlog_cxt.RedoStartLSN;
        record = ReadCheckpointRecord(xlogreader, checkPointLoc, 1, true);

        if (record != NULL) {
            ereport(DEBUG1,
                    (errmsg("checkpoint record is at %X/%X", (uint32)(checkPointLoc >> 32), (uint32)checkPointLoc)));
        } else if (t_thrd.xlog_cxt.StandbyMode) {
            /*
             * The last valid checkpoint record required for a streaming
             * recovery exists in neither standby nor the primary.
             */
            ereport(PANIC, (errmsg("could not locate a valid checkpoint record")));
        } else {
            checkPointLoc = t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint;
            record = ReadCheckpointRecord(xlogreader, checkPointLoc, 2);
            if (record != NULL) {
                ereport(LOG, (errmsg("using previous checkpoint record at %X/%X", (uint32)(checkPointLoc >> 32),
                                     (uint32)checkPointLoc)));
                t_thrd.xlog_cxt.InRecovery = true; /* force recovery even if SHUTDOWNED */
            } else {
                ereport(PANIC, (errmsg("could not locate a valid checkpoint record")));
            }
        }
        rcm = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(xlogreader), sizeof(CheckPoint));
        securec_check(rcm, "", "");

        if (!GTM_MODE) {
            recordLen = record->xl_tot_len;
            if (record->xl_tot_len == CHECKPOINTNEW_LEN) {
                rcm = memcpy_s(&checkPointNew, sizeof(checkPointNew), XLogRecGetData(xlogreader),
                               sizeof(checkPointNew));
                securec_check(rcm, "", "");
            } else if (record->xl_tot_len == CHECKPOINTPLUS_LEN) {
                rcm = memcpy_s(&checkPointPlus, sizeof(checkPointPlus), XLogRecGetData(xlogreader),
                               sizeof(checkPointPlus));
                securec_check(rcm, "", "");
            }
        }

        if (((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC_OLD) {
            ereport(LOG, (errmsg("read old version XLog page with magic %X.", (uint32)XLOG_PAGE_MAGIC_OLD)));
            readoldversion = true;
        }
        if (record->xl_term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
            g_instance.comm_cxt.localinfo_cxt.term_from_xlog = record->xl_term;
        }

        uint8 xlinfo = readoldversion ? ((XLogRecordOld *)record)->xl_info : record->xl_info;
        wasShutdown = (xlinfo == XLOG_CHECKPOINT_SHUTDOWN);
        wasCheckpoint = wasShutdown || (xlinfo == XLOG_CHECKPOINT_ONLINE);
        if (readoldversion && !wasCheckpoint) {
            ereport(FATAL, (errmsg("old version XLog must be shutdown checkpoint or online checkpoint.")));
        }
    }

    /* initialize double write, recover partial write */
    dw_init(wasShutdown);

    /* initialize account hash table lock */
    g_instance.policy_cxt.account_table_lock = LWLockAssign(LWTRANCHE_ACCOUNT_TABLE);

    t_thrd.xlog_cxt.LastRec = RecPtr = checkPointLoc;
    bool isoldversion = false;
    if (((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC_OLD) {
        isoldversion = true;
    }
    if (isoldversion) {
        CheckPointOld checkPointOld;
        rcm = memcpy_s(&checkPointOld, sizeof(CheckPointOld), XLogRecGetData(xlogreader), sizeof(CheckPointOld));
        securec_check(rcm, "", "");
        checkPoint.redo = XLogRecPtrSwap(checkPointOld.redo);
        checkPoint.nextXid = checkPointOld.nextXid;
        checkPoint.nextOid = checkPointOld.nextOid;
        checkPoint.nextMulti = checkPointOld.nextMulti;
        checkPoint.nextMultiOffset = checkPointOld.nextMultiOffset;

        if (checkPointOld.oldestXid > checkPointOld.nextXid &&
            TransactionIdLogicallyPrecedes(checkPointOld.oldestXid, FirstNormalTransactionId)) {
            checkPoint.oldestXid = FirstNormalTransactionId;
        } else {
            checkPoint.oldestXid = checkPointOld.oldestXid;
        }
        checkPoint.oldestXidDB = checkPointOld.oldestXidDB;
        checkPoint.time = checkPointOld.time;
        checkPoint.oldestActiveXid = checkPointOld.oldestActiveXid;
    }
    ereport(LOG, (errmsg("redo record is at %X/%X; shutdown %s", (uint32)(checkPoint.redo >> 32),
                         (uint32)checkPoint.redo, wasShutdown ? "TRUE" : "FALSE")));
    ereport(DEBUG1, (errmsg("next MultiXactId: " XID_FMT "; next MultiXactOffset: " XID_FMT, checkPoint.nextMulti,
                            checkPoint.nextMultiOffset)));
    ereport(DEBUG1, (errmsg("oldest unfrozen transaction ID: " XID_FMT ", in database %u", checkPoint.oldestXid,
                            checkPoint.oldestXidDB)));
    if (!TransactionIdIsNormal(checkPoint.nextXid)) {
        ereport(PANIC, (errmsg("invalid next transaction ID")));
    }

#ifdef ENABLE_MOT
    /*
     * Recover MOT
     */
    MOTRecover();
#endif

    /* initialize shared memory variables from the checkpoint record */
    t_thrd.xact_cxt.ShmemVariableCache->nextXid = checkPoint.nextXid;
    t_thrd.xact_cxt.ShmemVariableCache->nextOid = checkPoint.nextOid;
    t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
    t_thrd.shemem_ptr_cxt.XLogCtl->ckptXid = checkPoint.oldestXid;
    t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone = false;

    latestCompletedXid = checkPoint.nextXid;
    TransactionIdRetreat(latestCompletedXid);
    t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestCompletedXid;

    /* init recentGlobalXmin and xmin, to nextXid */
    t_thrd.xact_cxt.ShmemVariableCache->xmin = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin = latestCompletedXid + 1;
    t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    g_instance.ckpt_cxt_ctl->ckpt_current_redo_point = checkPoint.redo;

    /* init dirty page queue rec lsn to checkpoint.redo */
    update_dirty_page_queue_rec_lsn(checkPoint.redo, true);

    /*
     * for gtm environment, we need to set the local csn to next xid to increase.
     * for gtm environment, set local csn to 0 first and then it will be set by redo xact
     * or transaction's commit, which ensure local csn will not be larger than gtm csn.
     */
    if (GTM_FREE_MODE) {

        if ((recordLen == CHECKPOINTNEW_LEN)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = checkPointNew.next_csn;
            ereport(LOG, (errmsg("%s Mode: start local next csn from checkpoint %lu, next xid %lu", "GTM-Free",
                                 checkPointNew.next_csn, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
        } else if ((recordLen == CHECKPOINTPLUS_LEN)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = checkPointPlus.next_csn;
            ereport(LOG, (errmsg("%s Mode: start local next csn from checkpoint %lu, next xid %lu", "GTM-Free",
                                 checkPointPlus.next_csn, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
        } else {
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
            ereport(LOG, (errmsg("%s Mode: start local next csn from next_xid %lu", "GTM-Free",
                                 t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
        }
    } else if (GTM_MODE) {
        /*
         * In initdb progress, it will use local next csn when use local xid.
         * So we must ensure the first next csn is normal.
         * for normal condition, 4 is only used for the first normal transaction
         * when it is set to commit in progress, and will never used for the
         * final result(final result is fetched from gtm), because normal transaction
         * use local xid will not be permitted to commit with gtm.
         */
        ereport(LOG, (errmsg("%s Mode: start local next csn from 4", "GTM")));
        t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = COMMITSEQNO_FIRST_NORMAL + 1;
    } else {
        /* Init nextCommitSeqNo in GTM-Lite Mode */
        t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = COMMITSEQNO_FIRST_NORMAL + 1;
        if (recordLen == CHECKPOINTNEW_LEN) {
            ereport(LOG, (errmsg("%s Mode: start local next csn from checkpoint %lu", "GTM-Lite",
                                 (checkPointNew.next_csn))));
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = checkPointNew.next_csn;
        } else if (recordLen == CHECKPOINTPLUS_LEN) {
            ereport(LOG, (errmsg("%s Mode: start local next csn from checkpoint %lu", "GTM-Lite",
                                 (checkPointPlus.next_csn))));
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = checkPointPlus.next_csn;
        }
    }

    t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin = InvalidTransactionId;
    t_thrd.xact_cxt.ShmemVariableCache->standbyXmin = InvalidTransactionId;

    /*
     * Initialize replication slots, before there's a chance to remove
     * required resources.
     */
    StartupReplicationSlots();

    /*
     * Startup logical state, needs to be setup now so we have proper data
     * during crash recovery.
     */
    StartupReorderBuffer();

    /*
     * Initialize start LSN point of CBM system by validating the last-finished CBM file.
     * At this moment, checkpoint would not happen so it is safe to do the desicion
     * without holding the control file lock.
     */
    if (u_sess->attr.attr_storage.enable_cbm_tracking) {
        CBMTrackInit(true, checkPoint.redo);
        t_thrd.cbm_cxt.XlogCbmSys->needReset = false;
    } else {
        t_thrd.cbm_cxt.XlogCbmSys->needReset = true;
    }

    /* If xlog delay or ddl delay was enabled last time, continue to enable them */
    startupInitDelayXlog();
    startupInitDelayDDL();

    /*
     * We must replay WAL entries using the same TimeLineID they were created
     * under, so temporarily adopt the TLI indicated by the checkpoint (see
     * also xlog_redo()).
     */
    t_thrd.xlog_cxt.ThisTimeLineID = checkPoint.ThisTimeLineID;

    /*
     * Before running in recovery, scan pg_twophase and fill in its status
     * to be able to work on entries generated by redo.  Doing a scan before
     * taking any recovery action has the merit to discard any 2PC files that
     * are newer than the first record to replay, saving from any conflicts at
     * replay.  This avoids as well any subsequent scans when doing recovery
     * of the on-disk two-phase data.
     */
    restoreTwoPhaseData();

    StartupCSNLOG(isoldversion);

    t_thrd.xlog_cxt.lastFullPageWrites = checkPoint.fullPageWrites;
    t_thrd.xlog_cxt.RedoRecPtr = t_thrd.shemem_ptr_cxt.XLogCtl->RedoRecPtr =
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.RedoRecPtr = checkPoint.redo;

    if (ENABLE_INCRE_CKPT) {
        t_thrd.xlog_cxt.doPageWrites = false;
    } else {
        t_thrd.xlog_cxt.doPageWrites = t_thrd.xlog_cxt.lastFullPageWrites;
    }

    if (XLByteLT(RecPtr, checkPoint.redo)) {
        ereport(PANIC, (errmsg("invalid redo in checkpoint record")));
    }

    /*
     * Check whether we need to force recovery from WAL.  If it appears to
     * have been a clean shutdown and we did not have a recovery.conf file,
     * then assume no recovery needed.
     */
    if (XLByteLT(checkPoint.redo, RecPtr)) {
#ifdef ENABLE_MULTIPLE_NODES
        if (wasShutdown) {
            ereport(PANIC, (errmsg("invalid redo record in shutdown checkpoint")));
        }
#endif
        t_thrd.xlog_cxt.InRecovery = true;
    } else if (t_thrd.shemem_ptr_cxt.ControlFile->state != DB_SHUTDOWNED) {
        t_thrd.xlog_cxt.InRecovery = true;
    } else if (t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
        /* force recovery due to presence of recovery.conf */
        t_thrd.xlog_cxt.InRecovery = true;
        if (ArchiveRecoveryByPending) {
            RecoveryByPending = true;
        }
    }

    /* REDO */
    if (t_thrd.xlog_cxt.InRecovery) {
        /* use volatile pointer to prevent code rearrangement */
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        /*
         * Update pg_control to show that we are recovering and to show the
         * selected checkpoint as the place we are starting from. We also mark
         * pg_control with any minimum recovery stop point obtained from a
         * backup history file.
         */
        dbstate_at_startup = t_thrd.shemem_ptr_cxt.ControlFile->state;
        if (t_thrd.xlog_cxt.InArchiveRecovery) {
            t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
        } else {
            ereport(LOG, (errmsg("database system was not properly shut down; "
                                 "automatic recovery in progress")));
            t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_CRASH_RECOVERY;
        }
        PrintCkpXctlControlFile(t_thrd.shemem_ptr_cxt.ControlFile->checkPoint,
                                &(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy), checkPointLoc, &checkPoint,
                                t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint, "StartupXLOG");
        t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
        t_thrd.shemem_ptr_cxt.ControlFile->checkPoint = checkPointLoc;
        t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy = checkPoint;
        if (t_thrd.xlog_cxt.InArchiveRecovery) {
            /* initialize minRecoveryPoint if not set yet */
            if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, checkPoint.redo)) {
                t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = checkPoint.redo;
            }
        }

        /*
         * Set backupStartPoint if we're starting recovery from a backup label guide.
         *
         * NOTICE: there are several scenarios for this as follow, hope you like it.
         * 1. recovery a backup-interrupted master, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(false), backupEndRequired(false) and
         *    backupFromStandby(false).
         * 2. restore a master exclusive backup, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(true), backupEndRequired(false) and
         *    backupFromStandby(false).
         * 3. restore a standby exclusive backup, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(true), backupEndRequired(false) and
         *    backupFromStandby(true).
         * 4. restore a gs_ctl full build standby, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(false), backupEndRequired(true) and
         *    backupFromStandby(false).
         * 5. restore a gs_rewind inc build standby, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(false), backupEndRequired(false) and
         *    backupFromStandby(true).
         * 6. recovery as pending or standby, ArchiveRecoveryRequested(true),
         *    ArchiveRestoreRequested(false), backupEndRequired(false) and
         *    backupFromStandby(false) without backup label file.
         *
         * When we try to recover a backup-interrupted master, treat it as a normal
         * starting even we choose to recover from the checkpoint loaded from backup
         * label file.
         *
         * Set backupEndPoint and use minRecoveryPoint as the backup end location
         * if we're starting recovery from a base backup which was taken from the
         * standby. In this case, the database system status in pg_control must
         * indicate DB_IN_ARCHIVE_RECOVERY. If not, which means that backup is
         * corrupted, so we cancel recovery.
         */
        if (haveBackupLabel) {
            /* set backup to control file except backup-interrupted recovery */
            if (t_thrd.xlog_cxt.ArchiveRestoreRequested || backupEndRequired || backupFromStandby) {
                t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint = checkPoint.redo;
                t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired = backupEndRequired;
            } else {
                t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint = InvalidXLogRecPtr;
                t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired = false;
            }

            if (backupFromStandby) {
                if (dbstate_at_startup != DB_IN_ARCHIVE_RECOVERY) {
                    ereport(FATAL, (errmsg("backup_label contains data inconsistent with control file"),
                                    errhint("This means that the backup is corrupted and you will "
                                            "have to use another backup for recovery.")));
                }
                t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
            } else if (backupFromRoach) {
                t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
                ereport(LOG, (errmsg("perform roach backup restore and set backup end point to %X/%X",
                                         (uint32)(t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint >> 32),
                                         (uint32)t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint)));
            }
        }
        t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
        /* No need to hold ControlFileLock yet, we aren't up far enough */
        UpdateControlFile();

        /* initialize our local copy of minRecoveryPoint */
        t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
        SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);
        /*
         * Reset pgstat data if database system was abnormal shutdown (as
         * crash), because it may be invalid after recovery.
         */
        if (AbnormalShutdown) {
            pgstat_reset_all();
        }

        /*
         * If there was a backup label file, it's done its job and the info
         * has now been propagated into pg_control.  We must get rid of the
         * label file so that if we crash during recovery, we'll pick up at
         * the latest recovery restartpoint instead of going all the way back
         * to the backup start point.  It seems prudent though to just rename
         * the file out of the way rather than delete it completely.
         */
        if (haveBackupLabel) {
            unlink(BACKUP_LABEL_OLD);
            durable_rename(BACKUP_LABEL_FILE, BACKUP_LABEL_OLD, FATAL);
        }
        /*
         * If there was a tablespace_map file, it's done its job and the
         * symlinks have been created.  We must get rid of the map file so
         * that if we crash during recovery, we don't create symlinks again.
         * It seems prudent though to just rename the file out of the way
         * rather than delete it completely.
         */
        if (haveTblspcMap) {
            unlink(TABLESPACE_MAP_OLD);
            durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, FATAL);
        }

        /* Check that the GUCs used to generate the WAL allow recovery */
        CheckRequiredParameterValues(DBStateShutdown);

        /*
         * We're in recovery, so unlogged relations may be trashed and must be
         * reset.  This should be done BEFORE allowing Hot Standby
         * connections, so that read-only backends don't try to read whatever
         * garbage is left over from before.
         */
        if (!RecoveryByPending) {
            ResetUnloggedRelations(UNLOGGED_RELATION_CLEANUP);
        }

        /*
         * Likewise, delete any saved transaction snapshot files that got left
         * behind by crashed backends.
         */
        DeleteAllExportedSnapshotFiles();

        /*
         * Initialize for Hot Standby, if enabled. We won't let backends in
         * yet, not until we've reached the min recovery point specified in
         * control file and we've established a recovery snapshot from a
         * running-xacts WAL record.
         */
        if (t_thrd.xlog_cxt.ArchiveRecoveryRequested && g_instance.attr.attr_storage.EnableHotStandby) {
            TransactionId *xids = NULL;
            int nxids;
            ereport(DEBUG1, (errmsg("initializing for hot standby")));

            InitRecoveryTransactionEnvironment();

            if (wasShutdown) {
                oldestActiveXID = PrescanPreparedTransactions(&xids, &nxids);
            } else {
                oldestActiveXID = checkPoint.oldestActiveXid;
            }
            Assert(TransactionIdIsValid(oldestActiveXID));

            /* Tell procarray about the range of xids it has to deal with */
            ProcArrayInitRecovery(t_thrd.xact_cxt.ShmemVariableCache->nextXid);

            /* Startup commit log and subtrans only. Other SLRUs are not
             * maintained during recovery and need not be started yet. */
            StartupCLOG();

            /*
             * If we're beginning at a shutdown checkpoint, we know that
             * nothing was running on the master at this point. So fake-up an
             * empty running-xacts record and use that here and now. Recover
             * additional standby state for prepared transactions.
             */
            if (wasShutdown) {
#ifdef ENABLE_MULTIPLE_NODES
                TransactionId latestnextXid;
                RunningTransactionsData running;

                /*
                 * Construct a RunningTransactions snapshot representing a
                 * shut down server, with only prepared transactions still
                 * alive. We're never overflowed at this point because all
                 * subxids are listed with their parent prepared transactions.
                 */
                running.nextXid = checkPoint.nextXid;
                running.oldestRunningXid = oldestActiveXID;
                latestnextXid = checkPoint.nextXid;
                TransactionIdRetreat(latestnextXid);
                Assert(TransactionIdIsNormal(latestnextXid));
                running.latestCompletedXid = latestnextXid;
                ProcArrayApplyRecoveryInfo(&running);
#endif
                StandbyRecoverPreparedTransactions();
            }
        }

        /*
         * Initialize shared variables for tracking progress of WAL replay, as
         * if we had just replayed the record before the REDO location (or the
         * checkpoint record itself, if it's a shutdown checkpoint).
         */
        SpinLockAcquire(&xlogctl->info_lck);
        if (XLByteLT(checkPoint.redo, RecPtr)) {
            xlogctl->replayEndRecPtr = checkPoint.redo;
            xlogctl->lastReplayedReadRecPtr = checkPoint.redo;
        } else {
            xlogctl->replayEndRecPtr = t_thrd.xlog_cxt.EndRecPtr;
            xlogctl->lastReplayedReadRecPtr = t_thrd.xlog_cxt.ReadRecPtr;
        }
        g_instance.comm_cxt.predo_cxt.redoPf.last_replayed_end_ptr = xlogctl->lastReplayedEndRecPtr;
        xlogctl->lastReplayedEndRecPtr = xlogctl->replayEndRecPtr;
        xlogctl->recoveryLastXTime = 0;
        xlogctl->currentChunkStartTime = 0;
        xlogctl->recoveryPause = false;
        SpinLockRelease(&xlogctl->info_lck);

        /* Also ensure XLogReceiptTime has a sane value */
        t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();

        /*
         * Let postmaster know we've started redo now, so that it can launch
         * checkpointer to perform restartpoints.  We don't bother during
         * crash recovery as restartpoints can only be performed during
         * archive recovery.  And we'd like to keep crash recovery simple, to
         * avoid introducing bugs that could affect you when recovering after
         * crash.
         *
         * After this point, we can no longer assume that we're the only
         * process in addition to postmaster!  Also, fsync requests are
         * subsequently to be handled by the checkpointer, not locally.
         */
        if ((t_thrd.xlog_cxt.ArchiveRecoveryRequested || IS_PGXC_COORDINATOR || IS_SINGLE_NODE || isRestoreMode) && IsUnderPostmaster) {
            PublishStartupProcessInformation();
            SetForwardFsyncRequests();
            SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED);
            if (ENABLE_INCRE_CKPT) {
                t_thrd.xlog_cxt.pagewriter_launched = true;
            } else {
                t_thrd.xlog_cxt.bgwriterLaunched = true;
            }
        }

        /*
         * Report the values of relevant variables used for the follow consistent check.
         * In some scenario, it takes a long time to recover up to the consistent point
         * before starting up. We can evaluate the remainning time based on the current
         * redo LSN and future consistent point.
         */
        ereport(LOG, (errmsg("redo minRecoveryPoint at %X/%X; backupStartPoint at %X/%X; "
                             "backupEndRequired %s",
                             (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32), (uint32)t_thrd.xlog_cxt.minRecoveryPoint,
                             (uint32)(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint >> 32),
                             (uint32)t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint,
                             t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired ? "TRUE" : "FALSE")));

        // Allow read-only connections immediately if we're consistent already.
        CheckRecoveryConsistency();

        // Find the first record that logically follows the checkpoint --- it
        //  might physically precede it, though.
        if (XLByteLT(checkPoint.redo, RecPtr)) {
            /* back up to find the record */
            record = ReadRecord(xlogreader, checkPoint.redo, PANIC, false, true);
        } else {
            /* just have to read next record after CheckPoint */
            record = ReadRecord(xlogreader, InvalidXLogRecPtr, LOG, false, true);
        }

        XLogReaderState *oldXlogReader = xlogreader;

        if (record != NULL) {
            bool recoveryContinue = true;
            bool recoveryApply = true;
            TimestampTz xtime;
            bool readoldversion = false;
            instr_time rec_startTime;
            instr_time rec_endTime;
            XLogRecPtr redoStartPtr = t_thrd.xlog_cxt.ReadRecPtr;
            uint64 redoTotalBytes;
            if (record->xl_term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
                g_instance.comm_cxt.localinfo_cxt.term_from_xlog = record->xl_term;
            }

            t_thrd.xlog_cxt.InRedo = true;
            g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr = 0;
            ResourceManagerStartup();
            StartUpMultiRedo(xlogreader, sizeof(readprivate));

            if (IsExtremeRedo()) {
                xlogreader->isPRProcess = true;
                record = ReadNextXLogRecord(&xlogreader, LOG);
                if (record == NULL) {
                    ereport(PANIC, (errmsg("redo starts at %X/%X", (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32),
                                           (uint32)t_thrd.xlog_cxt.ReadRecPtr)));
                }
                t_thrd.xlog_cxt.readSource = 0;
            } else if (IsParallelRedo()) {
                xlogreader->isPRProcess = true;
                xlogreader = parallel_recovery::NewReaderState(xlogreader, true);
            }

            ereport(LOG, (errmsg("redo starts at %X/%X", (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32),
                                 (uint32)t_thrd.xlog_cxt.ReadRecPtr)));
            if (IS_DISASTER_RECOVER_MODE)
                update_stop_barrier();
            INSTR_TIME_SET_CURRENT(rec_startTime);
            t_thrd.xlog_cxt.RedoStartLSN = t_thrd.xlog_cxt.ReadRecPtr;
            g_instance.comm_cxt.predo_cxt.redoPf.redo_start_ptr = t_thrd.xlog_cxt.RedoStartLSN;
            knl_g_set_redo_finish_status(0);
            ereport(LOG, (errmsg("set knl_g_set_redo_finish_status to false when starting redo")));

            do {
                TermFileData term_file;
                if (((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC_OLD) {
                    ereport(PANIC, (errmsg("redo can not support old version!!!!")));
                }

#ifdef WAL_DEBUG
                if (u_sess->attr.attr_storage.XLOG_DEBUG ||
                    (rmid == RM_XACT_ID && u_sess->attr.attr_common.trace_recovery_messages <= DEBUG2) ||
                    (rmid != RM_XACT_ID && u_sess->attr.attr_common.trace_recovery_messages <= DEBUG3)) {
                    StringInfoData buf;

                    initStringInfo(&buf);
                    appendStringInfo(&buf, "REDO @ %X/%X; LSN %X/%X: ", (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32),
                                     (uint32)t_thrd.xlog_cxt.ReadRecPtr, (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32),
                                     (uint32)t_thrd.xlog_cxt.EndRecPtr);
                    xlog_outrec(&buf, xlogreader);
                    appendStringInfo(&buf, " - ");

                    RmgrTable[record->xl_rmid].rm_desc(&buf, xlogreader);
                    ereport(LOG, (errmsg("%s", buf.data)));
                    pfree_ext(buf.data);
                }
#endif

                /* Handle interrupt signals of startup process */
                RedoInterruptCallBack();

                /*
                 * Pause WAL replay, if requested by a hot-standby session via SetRecoveryPause().
                 *
                 * Note that we intentionally don't take the info_lck spinlock
                 * here.  We might therefore read a slightly stale value of
                 * the recoveryPause flag, but it can't be very stale (no
                 * worse than the last spinlock we did acquire).  Since a
                 * pause request is a pretty asynchronous thing anyway,
                 * possibly responding to it one WAL record later than we
                 * otherwise would is a minor issue, so it doesn't seem worth
                 * adding another spinlock cycle to prevent that.
                 */
                if (xlogctl->recoveryPause) {
                    recoveryPausesHere();
                }

                // Have we reached our recovery target?
                if (recoveryStopsHere(xlogreader, &recoveryApply)) {
                    reachedStopPoint = true; /* see below */
                    recoveryContinue = false;
                    extreme_rto::ExtremeRtoStopHere();
                    /* Exit loop if we reached non-inclusive recovery target */
                    if (!recoveryApply &&
                        (t_thrd.xlog_cxt.server_mode == PRIMARY_MODE || t_thrd.xlog_cxt.server_mode == NORMAL_MODE
                        || IS_DISASTER_RECOVER_MODE)) {
                        break;
                    }
                }

#ifndef ENABLE_MULTIPLE_NODES
                /*
                 * If we've been asked to lag the master, wait on latch until
                 * enough time has passed.
                 */
                if (RecoveryApplyDelay(xlogreader)) {
                    /*
                     * We test for paused recovery again here. If user sets
                     * delayed apply, it may be because they expect to pause
                     * recovery in case of problems, so we must test again
                     * here otherwise pausing during the delay-wait wouldn't
                     * work.
                     */
                    if (xlogctl->recoveryPause) {
                        recoveryPausesHere();
                    }
				}
#endif
                
                /*
                 * ShmemVariableCache->nextXid must be beyond record's xid.
                 *
                 * We don't expect anyone else to modify nextXid, hence we
                 * don't need to hold a lock while examining it.  We still
                 * acquire the lock to modify it, though.
                 */
                if (TransactionIdFollowsOrEquals(record->xl_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
                    if (TransactionIdFollowsOrEquals(record->xl_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                        t_thrd.xact_cxt.ShmemVariableCache->nextXid = record->xl_xid;
                        TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
                    }
                    LWLockRelease(XidGenLock);
                }

                /*
                 * Update shared replayEndRecPtr before replaying this record,
                 * so that XLogFlush will update minRecoveryPoint correctly.
                 */
                SpinLockAcquire(&xlogctl->info_lck);
                xlogctl->replayEndRecPtr = t_thrd.xlog_cxt.EndRecPtr;
                SpinLockRelease(&xlogctl->info_lck);

                /* If we are attempting to enter Hot Standby mode, process XIDs we see */
                TransactionId xl_xid = record->xl_xid;
                if (t_thrd.xlog_cxt.standbyState >= STANDBY_INITIALIZED && TransactionIdIsValid(xl_xid)) {
                    CSNLogRecordAssignedTransactionId(xl_xid);
                } else if (TransactionIdIsValid(xl_xid)) {
                    ExtendCSNLOG(xl_xid);
                }
                xtime = GetLatestXTime();
                XLogReaderState *newXlogReader = xlogreader;
                if (xlogreader->isPRProcess && !IsExtremeRedo()) {
                    newXlogReader = parallel_recovery::NewReaderState(xlogreader);
                }

                DispatchRedoRecord(xlogreader, t_thrd.xlog_cxt.expectedTLIs, xtime);

                /* Remember this record as the last-applied one */
                t_thrd.xlog_cxt.LastRec = t_thrd.xlog_cxt.ReadRecPtr;

                /* Exit loop if we reached inclusive recovery target */
                if (!recoveryContinue &&
                    (t_thrd.xlog_cxt.server_mode == PRIMARY_MODE || t_thrd.xlog_cxt.server_mode == NORMAL_MODE
                    || IS_DISASTER_RECOVER_MODE)) {
                    break;
                }

                if (!IsExtremeRedo() && CheckForForceFinishRedoTrigger(&term_file)) {
                    t_thrd.xlog_cxt.forceFinishHappened = true;
                    ereport(WARNING,
                            (errmsg("[ForceFinish]force finish redo triggered when StartupXLOG, ReadRecPtr:%08X/%08X,"
                                "EndRecPtr:%08X/%08X, StandbyMode:%u, startup_processing:%u, dummyStandbyMode:%u",
                                    (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), (uint32)t_thrd.xlog_cxt.ReadRecPtr,
                                    (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32), (uint32)t_thrd.xlog_cxt.EndRecPtr, 
                                    t_thrd.xlog_cxt.StandbyMode, t_thrd.xlog_cxt.startup_processing, 
                                    dummyStandbyMode)));
                    ArchiveXlogForForceFinishRedo(xlogreader, &term_file);
                    newXlogReader->readOff = INVALID_READ_OFF;
                }
                if (xlogreader->isPRProcess && IsExtremeRedo()) {
                    record = ReadNextXLogRecord(&xlogreader, LOG);
                } else {
                    xlogreader = newXlogReader;
                    record = ReadRecord(xlogreader, InvalidXLogRecPtr, LOG, false, readoldversion);
                }
            } while (record != NULL);  // end of main redo apply loop
            SendRecoveryEndMarkToWorkersAndWaitForFinish(0);
            RecoveryXlogReader(oldXlogReader, xlogreader);

            if (t_thrd.xlog_cxt.recoveryPauseAtTarget && reachedStopPoint && !IS_DISASTER_RECOVER_MODE) {
                SetRecoveryPause(true);
                recoveryPausesHere();
            }
            /* redo finished, we set is_recovery_done to true for query */
            g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr = t_thrd.xlog_cxt.ReadRecPtr;
            g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time = GetCurrentTimestamp();
            knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL | REDO_FINISH_STATUS_CM);
            ereport(LOG, (errmsg("set knl_g_set_redo_finish_status to true when redo done")));
            ereport(LOG, (errmsg("redo done at %X/%X, end at %X/%X", (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32),
                                 (uint32)t_thrd.xlog_cxt.ReadRecPtr, (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32),
                                 (uint32)t_thrd.xlog_cxt.EndRecPtr)));

            INSTR_TIME_SET_CURRENT(rec_endTime);
            INSTR_TIME_SUBTRACT(rec_endTime, rec_startTime);
            redoTotalBytes = t_thrd.xlog_cxt.EndRecPtr - redoStartPtr;
            uint64 totalTime = INSTR_TIME_GET_MICROSEC(rec_endTime);
            uint64 speed = 0;  // MB/s
            if (totalTime > 0) {
                speed = (redoTotalBytes / totalTime) * 1000000 / 1024 / 1024;
            }
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[PR]: Recoverying elapsed: %lu us, redoTotalBytes:%lu,"
                                 "EndRecPtr:%lu, redoStartPtr:%lu,speed:%lu MB/s, totalTime:%lu",
                                 INSTR_TIME_GET_MICROSEC(rec_endTime), redoTotalBytes, t_thrd.xlog_cxt.EndRecPtr,
                                 redoStartPtr, speed, totalTime)));
            redo_unlink_stats_file();
            parallel_recovery::redo_dump_all_stats();
            /* check all the received xlog have been redo when switchover */
            if (CheckForSwitchoverTrigger()) {
                XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
                XLogRecPtr EndRecPtrTemp = t_thrd.xlog_cxt.EndRecPtr;
                XLByteAdvance(EndRecPtrTemp, SizeOfXLogRecord);
                if (XLByteLT(EndRecPtrTemp, receivedUpto) && !force_finish_enabled()) {
                    ereport(PANIC, (errmsg("there are some received xlog have not been redo "
                                           "the tail of last redo lsn:%X/%X, received lsn:%X/%X",
                                           (uint32)(EndRecPtrTemp >> 32), (uint32)EndRecPtrTemp,
                                           (uint32)(receivedUpto >> 32), (uint32)receivedUpto)));
                }
            }

            xtime = GetLatestXTime();
            if (xtime) {
                ereport(LOG, (errmsg("last completed transaction was at log time %s", timestamptz_to_str(xtime))));
            }
            t_thrd.xlog_cxt.InRedo = false;
        } else {
            /* there are no WAL records following the checkpoint */
            ereport(LOG, (errmsg("redo is not required")));
        }
    }

    /*
     * Kill WAL receiver, if it's still running, before we continue to write
     * the startup checkpoint record. It will trump over the checkpoint and
     * subsequent records if it's still alive when we start writing WAL.
     */
    ShutdownWalRcv();
    ShutdownDataRcv();

    /* Move this check to redo done not in the process of redo.
     *
     * Check to see if the XLOG sequence contained any unresolved
     * references to uninitialized pages.
     */
    XLogCheckInvalidPages();

    /*
     * We don't need the latch anymore. It's not strictly necessary to disown
     * it, but let's do it for the sake of tidiness.
     */
    if (t_thrd.xlog_cxt.StandbyModeRequested) {
        if (!IsExtremeRedo()) {
            DisownLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
        }
        DisownLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->dataRecoveryLatch);
    }

    /*
     * We are now done reading the xlog from stream. Turn off streaming
     * recovery to force fetching the files (which would be required at end of
     * recovery, e.g., timeline history file) from archive or pg_xlog.
     */
    t_thrd.xlog_cxt.StandbyMode = false;
    /*
     * Re-fetch the last valid or last applied record, so we can identify the
     * exact endpoint of what we consider the valid portion of WAL.
     */
    xlogreader->readSegNo = 0;
    xlogreader->readOff = 0;
    xlogreader->readLen = 0;
    record = ReadRecord(xlogreader, t_thrd.xlog_cxt.LastRec, PANIC, false, true);
    if (record->xl_term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog = record->xl_term;
    }
    EndOfLog = t_thrd.xlog_cxt.EndRecPtr;
    XLByteToPrevSeg(EndOfLog, endLogSegNo);

    /*
     * Complain if we did not roll forward far enough to render the backup
     * dump consistent.  Note: it is indeed okay to look at the local variable
     * minRecoveryPoint here, even though ControlFile->minRecoveryPoint might
     * be further ahead --- ControlFile->minRecoveryPoint cannot have been
     * advanced beyond the WAL we processed.
     */
    if (t_thrd.xlog_cxt.InRecovery && (XLByteLT(EndOfLog, t_thrd.xlog_cxt.minRecoveryPoint) ||
                                       !XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint))) {
        if (reachedStopPoint) {
            /* stopped because of stop request */
            ereport(FATAL, (errmsg("requested recovery stop point is before consistent recovery point")));
        }

        /*
         * Ran off end of WAL before reaching end-of-backup WAL record, or
         * minRecoveryPoint. That's usually a bad sign, indicating that you
         * tried to recover from an online backup but never called
         * pg_stop_backup(), or you didn't archive all the WAL up to that
         * point. However, this also happens in crash recovery, if the system
         * crashes while an online backup is in progress. We must not treat
         * that as an error, or the database will refuse to start up.
         */
        if (t_thrd.xlog_cxt.ArchiveRecoveryRequested || t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired) {
            if (t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired) {
                ereport(FATAL,
                        (errmsg("WAL ends before end of online backup"),
                         errhint("All WAL generated while online backup was taken must be available at recovery.")));
            } else if (!XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint)) {
                ereport(FATAL,
                        (errmsg("WAL ends before end of online backup"),
                         errhint(
                             "Online backup started with pg_start_backup() must be ended with pg_stop_backup(), and "
                             "all WAL up to that point must be available at recovery.")));
            } else {
                ereport(FATAL, (errmsg("WAL ends before consistent recovery point")));
            }
        }
    }

    /* Save the selected TimeLineID in shared memory, too */
    t_thrd.shemem_ptr_cxt.XLogCtl->ThisTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;

    /*
     * We are now done reading the old WAL.  Turn off archive fetching if it
     * was active, and make a writable copy of the last WAL segment. (Note
     * that we also have a copy of the last block of the old WAL in readBuf;
     * we will use that below.)
     */
    if (t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
        exitArchiveRecovery(xlogreader->readPageTLI, endLogSegNo);
    }

    /*
     * Prepare to write WAL starting at EndOfLog position, and init xlog
     * buffer cache using the block containing the last record from the
     * previous incarnation.
     */
    t_thrd.xlog_cxt.openLogSegNo = endLogSegNo;
    t_thrd.xlog_cxt.openLogFile = XLogFileOpen(t_thrd.xlog_cxt.openLogSegNo);
    t_thrd.xlog_cxt.openLogOff = 0;
    Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    Insert->CurrBytePos = XLogRecPtrToBytePos(EndOfLog);
    Insert->PrevByteSize = XLogRecPtrToBytePos(EndOfLog) - XLogRecPtrToBytePos(t_thrd.xlog_cxt.LastRec);
    Insert->CurrLRC = 0;

    /*
     * Tricky point here: readBuf contains the *last* block that the LastRec
     * record spans, not the one it starts in.  The last block is indeed the
     * one we want to use.
     */
    if (EndOfLog % XLOG_BLCKSZ != 0) {
        char *page = NULL;
        int len;
        int firstIdx;
        XLogRecPtr pageBeginPtr;

        pageBeginPtr = EndOfLog - (EndOfLog % XLOG_BLCKSZ);
        Assert(t_thrd.xlog_cxt.readOff == pageBeginPtr % XLogSegSize);

        firstIdx = XLogRecPtrToBufIdx(EndOfLog);

        /* Copy the valid part of the last block, and zero the rest */
        page = &t_thrd.shemem_ptr_cxt.XLogCtl->pages[firstIdx * XLOG_BLCKSZ];
        len = EndOfLog % XLOG_BLCKSZ;
        errorno = memcpy_s(page, XLOG_BLCKSZ, xlogreader->readBuf, len);
        securec_check(errorno, "", "");

        errorno = memset_s(page + len, XLOG_BLCKSZ - len, 0, XLOG_BLCKSZ - len);
        securec_check(errorno, "", "");

        t_thrd.shemem_ptr_cxt.XLogCtl->xlblocks[firstIdx] = pageBeginPtr + XLOG_BLCKSZ;
        t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo = pageBeginPtr + XLOG_BLCKSZ;
    } else {
        /*
         * There is no partial block to copy. Just set InitializedUpTo,
         * and let the first attempt to insert a log record to initialize
         * the next buffer.
         */
        t_thrd.shemem_ptr_cxt.XLogCtl->InitializedUpTo = EndOfLog;
    }

    t_thrd.xlog_cxt.LogwrtResult->Write = t_thrd.xlog_cxt.LogwrtResult->Flush = EndOfLog;
    t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult = *t_thrd.xlog_cxt.LogwrtResult;
    t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write = EndOfLog;
    t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Flush = EndOfLog;
    g_instance.comm_cxt.predo_cxt.redoPf.primary_flush_ptr = EndOfLog;

    /*
     * Prepare WAL buffer and other variables for WAL writing before any 
     * call to LocalSetXLogInsertAllowed() calls that enables WAL writing
     */
    g_instance.wal_cxt.flushResult = EndOfLog;
    AdvanceXLInsertBuffer<false>(EndOfLog, false, NULL);
    g_instance.wal_cxt.sentResult = EndOfLog;

    /* add switch at old version xlog page */
    if ((EndOfLog % XLOG_BLCKSZ != 0) && ((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC_OLD) {
        XLogRecPtr switchptr;

        LocalSetXLogInsertAllowed();
        /* XLOG SWITCH has no data */
        XLogBeginInsert();

        switchptr = XLogInsert(RM_XLOG_ID, XLOG_SWITCH, true);
        t_thrd.xlog_cxt.LocalXLogInsertAllowed = -1;
    }

    /* Pre-scan prepared transactions to find out the range of XIDs present */
    oldestActiveXID = PrescanPreparedTransactions(NULL, NULL);

    /*
     * Update full_page_writes in shared memory and write an XLOG_FPW_CHANGE
     * record before resource manager writes cleanup WAL records or checkpoint
     * record is written.
     */
    if (ENABLE_INCRE_CKPT) {
        Insert->fullPageWrites = false;
    } else {
        Insert->fullPageWrites = t_thrd.xlog_cxt.lastFullPageWrites;
    }
    LocalSetXLogInsertAllowed();
    UpdateFullPageWrites();
    t_thrd.xlog_cxt.LocalXLogInsertAllowed = -1;

    if (t_thrd.xlog_cxt.InRecovery) {
        /*
         * Resource managers might need to write WAL records, eg, to record
         * index cleanup actions.  So temporarily enable XLogInsertAllowed in
         * this process only.
         */
        LocalSetXLogInsertAllowed();
        EndRedoXlog();
        /* Disallow XLogInsert again */
        t_thrd.xlog_cxt.LocalXLogInsertAllowed = -1;
        CheckForRestartPoint();
        

        /*
         * And finally, execute the recovery_end_command, if any.
         */
        if (t_thrd.xlog_cxt.recoveryEndCommand) {
            ExecuteRecoveryCommand(t_thrd.xlog_cxt.recoveryEndCommand, "recovery_end_command", true);
        }
    }

    /*
     * Clean up any (possibly bogus) future WAL segments on the old timeline.
     */
    if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
        RemoveNonParentXlogFiles(EndOfLog, t_thrd.xlog_cxt.ThisTimeLineID);
    }

    /*
     * Preallocate additional log files, if wanted.
     */
    PreallocXlogFiles(EndOfLog);

    /*
     * Reset initial contents of unlogged relations.  This has to be done
     * AFTER recovery is complete so that any unlogged relations created
     * during recovery also get picked up.
     */
    if (t_thrd.xlog_cxt.InRecovery && !RecoveryByPending) {
        ResetUnloggedRelations(UNLOGGED_RELATION_INIT);
    }

    /*
     * Okay, we're officially UP.
     */
    t_thrd.xlog_cxt.InRecovery = false;

    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_PRODUCTION;
    t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
    UpdateControlFile();
    LWLockRelease(ControlFileLock);

    /* start the archive_timeout timer running */
    t_thrd.shemem_ptr_cxt.XLogCtl->lastSegSwitchTime = (pg_time_t)time(NULL);

    /* also initialize latestCompletedXid, to nextXid - 1 */
    latestCompletedXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    TransactionIdRetreat(latestCompletedXid);
    t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestCompletedXid;

    /*
     * Start up the commit log and subtrans, if not already done for hot
     * standby.
     */
    StartupCLOG();

    /*
     * Perform end of recovery actions for any SLRUs that need it.
     */
    StartupMultiXact();
    TrimCLOG();

    /* Reload shared-memory state for prepared transactions */
    RecoverPreparedTransactions();

    /*
     * If any of the critical GUCs have changed, log them before we allow
     * backends to write WAL.
     */
    LocalSetXLogInsertAllowed();

    /*
     * Shutdown the recovery environment. This must occur after
     * RecoverPreparedTransactions(), see notes for lock_twophase_recover()
     */
    if (t_thrd.xlog_cxt.standbyState != STANDBY_DISABLED) {
        ShutdownRecoveryTransactionEnvironment();
    }

    /* Shut down readFile facility, free space. */
    ShutdownReadFileFacility();

    /* Shut down the xlog reader facility. */
    XLogReaderFree(xlogreader);
    xlogreader = NULL;

    XLogReportParameters();

    /*
     * If enable_gtm_free, we do not have GTM to coordinate xid assignment.
     * thus we have to bump up the nextXid number to avoid the last xid
     * when CN was brought down (probably need recover/2pc clean up) to be
     * re-used/overwritten by new CN init.
     */
    if (!GTM_MODE) {
        /*
         * if switch gtm-lite mode to gtm-free mode, local next csn maybe larger than next xid,
         * set next xid to the larger one.
         */
        if (GTM_FREE_MODE &&
            t_thrd.xact_cxt.ShmemVariableCache->nextXid < t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
        }
        t_thrd.xact_cxt.ShmemVariableCache->nextXid += g_instance.shmem_cxt.MaxBackends;

        /*
         * nextxid may access the next clog page which has not extended yet,
         * If we do checkpoint, nextxid record as a part of runningxacts, and
         * we may try to access according clog page during recovery.
         * Just extend it here to avoid this situation.
         */
        ExtendCLOG(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        ExtendCSNLOG(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
    }

    /*
     * All done.  Allow backends to write WAL.	(Although the bool flag is
     * probably atomic in itself, we use the info_lck here to ensure that
     * there are no race conditions concerning visibility of other recent
     * updates to shared memory.)
     */
    {
        /* use volatile pointer to prevent code rearrangement */
        volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        xlogctl->SharedRecoveryInProgress = false;
        xlogctl->IsRecoveryDone = true;
        SpinLockRelease(&xlogctl->info_lck);
        if (ENABLE_INCRE_CKPT) {
            RecoveryQueueState *state = &g_instance.ckpt_cxt_ctl->ckpt_redo_state;
            (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
            state->start = state->end;
            (void)LWLockRelease(state->recovery_queue_lock);
        }
    }

    NextXidAfterReovery = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    OldestXidAfterRecovery = t_thrd.xact_cxt.ShmemVariableCache->oldestXid;
    PendingPreparedXactsCount = GetPendingXactCount();
    IsPendingXactsRecoveryDone = false;
    t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    /* for recentGlobalXmin, if it is still invalid, update it use the value from checkpoint */
    if (GTM_LITE_MODE && recordLen == CHECKPOINTPLUS_LEN &&
        t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin == InvalidTransactionId) {
        t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin = checkPointPlus.recent_global_xmin;
    }

    /*
     * May exist gds with no xlog for heap page,
     * so the xid in page may bigger than ShmemVariableCache->nextXid
     */

    /* for primary node, calculate the first multi version snapshot. */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    CalculateLocalLatestSnapshot(true);
    LWLockRelease(ProcArrayLock);

    ereport(LOG, (errmsg("redo done, nextXid: " XID_FMT ", startupMaxXid: " XID_FMT ", recentLocalXmin: " XID_FMT
                         ", recentGlobalXmin: %lu, PendingPreparedXacts: %d"
                         ", NextCommitSeqNo: %lu, cutoff_csn_min: %lu.",
                         NextXidAfterReovery, t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid,
                         t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin,
                         t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin, PendingPreparedXactsCount,
                         t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo,
                         t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min)));

#ifdef ENABLE_MOT
    /*
     * Cleanup MOT recovery
     */
    MOTRecoveryDone();
#endif
}

void CopyXlogForForceFinishRedo(XLogSegNo logSegNo, uint32 termId, XLogReaderState *xlogreader,
                                XLogRecPtr lastRplEndLsn)
{
    char srcPath[MAXPGPATH];
    char dstPath[MAXPGPATH];
    errno_t errorno = EOK;

    errorno = snprintf_s(srcPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", xlogreader->readPageTLI,
                         (uint32)((logSegNo) / XLogSegmentsPerXLogId), (uint32)((logSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errorno = snprintf_s(dstPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X%08X", termId,
                         t_thrd.xlog_cxt.ThisTimeLineID, (uint32)((logSegNo) / XLogSegmentsPerXLogId),
                         (uint32)((logSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");
    XLogFileCopy(srcPath, dstPath);
    XLogFileTruncate(srcPath, lastRplEndLsn);
}

/* there is nothing to do with beginSegNo, we already get the copy of beginSegNo. */
void RenameXlogForForceFinishRedo(XLogSegNo beginSegNo, TimeLineID tli, uint32 termId)
{
    ereport(WARNING, (errcode(ERRCODE_LOG), errmsg("RenameXlogForForceFinishRedo INFO beginSegNo:%lu, tli:%u, "
                                                   "termId:%u",
                                                   beginSegNo, tli, termId)));

    for (uint32 sn = beginSegNo + 1;; ++sn) {
        char srcPath[MAXPGPATH];
        char dstPath[MAXPGPATH];
        errno_t errorno = EOK;

        errorno = snprintf_s(srcPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X", tli,
                             (uint32)((sn) / XLogSegmentsPerXLogId), (uint32)((sn) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");
        errorno = snprintf_s(dstPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X%08X", termId, tli,
                             (uint32)((sn) / XLogSegmentsPerXLogId), (uint32)((sn) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");
        if (durable_rename(srcPath, dstPath, WARNING) != 0) {
            ereport(WARNING, (errcode(ERRCODE_LOG), errmsg("RenameXlogForForceFinishRedo failed, srcPath:%s, "
                                                           "dstPath:%s",
                                                           srcPath, dstPath)));
            break;
        }
    }
}

void SetSwitchHistoryFile(XLogRecPtr switchLsn, XLogRecPtr catchLsn, uint32 termId)
{
    char path[MAXPGPATH];
    char tmpPath[MAXPGPATH];
    errno_t errorno = EOK;
    XLogSwitchInfo info;
    FILE *hfile = NULL;

    info.termId = termId;
    info.switchLsn = switchLsn;
    info.catchLsn = catchLsn;
    info.oldestRunningXid = PrescanPreparedTransactions(NULL, NULL);
    info.latestCompletedXid = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    info.invaildPageCnt = t_thrd.xlog_cxt.invaildPageCnt;
    info.imcompleteActionCnt = t_thrd.xlog_cxt.imcompleteActionCnt;
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%s", termId, XLOG_SWITCH_HISTORY_FILE);
    securec_check_ss(errorno, "", "");
    errorno = snprintf_s(tmpPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%s.temp", termId, XLOG_SWITCH_HISTORY_FILE);
    securec_check_ss(errorno, "", "");

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("[REDO_STATS]SetSwitchHistoryFile: ready to set switch file, termId:%u, "
                         "switchLsn:%lu, catchLsn:%lu, oldestRunningXid:%lu, latestCompleteXid:%lu, "
                         "invaildPageCnt:%u, imcompleteActionCnt:%u",
                         termId, switchLsn, catchLsn, info.oldestRunningXid, info.latestCompletedXid,
                         info.invaildPageCnt, info.imcompleteActionCnt)));

    canonicalize_path(tmpPath);
    hfile = fopen(tmpPath, "w");
    if (hfile == NULL) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[REDO_STATS]SetSwitchHistoryFile: fopen tmpPath:%s failed, %m", tmpPath)));
        return;
    }
    if (0 == (fwrite(&info, 1, sizeof(XLogSwitchInfo), hfile))) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[REDO_STATS]SetSwitchHistoryFile: fwrite tmpPath:%s failed, %m", tmpPath)));
    }
    fclose(hfile);
    if (durable_rename(tmpPath, path, WARNING) != 0) {
        ereport(WARNING,
                (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("SetSwitchHistoryFile should not have failed, tmpPath:%s, "
                                                         "path:%s",
                                                         tmpPath, path)));
    }
}

bool FallbackXlogreader(XLogReaderState *xlogreader, XLogRecPtr readPtr, XLogRecPtr endPtr)
{
    errno_t rc = EOK;
    /* next read, ReadRecPtr will be set to RecPtr in function XLogReadRecord */
    xlogreader->ReadRecPtr = readPtr;
    /* EndRecPtr will be next record start location */
    xlogreader->EndRecPtr = endPtr;
    xlogreader->readOff = INVALID_READ_OFF;
    rc = memset_s(xlogreader->readBuf, xlogreader->readLen, 0, xlogreader->readLen);
    securec_check(rc, "\0", "\0");
    t_thrd.xlog_cxt.LastRec = readPtr;
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        return true;
    }
    return false;
}

void ReOpenXlog(XLogReaderState *xlogreader)
{
    uint32 sources;
    int emode;

    sources = XLOG_FROM_PG_XLOG;
    emode = ((XLogPageReadPrivate *)(xlogreader->private_data))->emode;
    if (t_thrd.xlog_cxt.InArchiveRecovery)
        sources |= XLOG_FROM_ARCHIVE;
    t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, emode, sources);
    if (t_thrd.xlog_cxt.readFile < 0) {
        ereport(WARNING, (errmsg("FallbackXlogreader: XLogFileReadAnyTLI failed, "
                                 "readseg:%lu, emode:%d, sources:%u",
                                 t_thrd.xlog_cxt.readSegNo, emode, sources)));
    }
}

void ArchiveXlogForForceFinishRedo(XLogReaderState *xlogreader, TermFileData *term_file)
{
    bool finishRedo = term_file->finish_redo;
    uint32 termId = term_file->term;
    if (finishRedo == true) {
        if ((get_real_recovery_parallelism() > 1) && (GetRedoWorkerCount() > 0)) {
            ProcTxnWorkLoad(true);

            parallel_recovery::SendClearMarkToAllWorkers();
            parallel_recovery::WaitRedoWorkerIdle();
        }

        /* local replayed lsn */
        XLogRecPtr lastRplReadLsn;
        XLogRecPtr lastRplEndLsn = GetXLogReplayRecPtr(NULL, &lastRplReadLsn);
        /* scene1: startup scene, receivedUpto is 0; */
        /* scene2: walRcv scene, receivedUpto is non-zero; */
        XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
        XLogSegNo beginSegNo;
        bool closed = false;
        ShutdownWalRcv();
        ShutdownDataRcv();
        ereport(WARNING,
                (errcode(ERRCODE_LOG),
                 errmsg("[ForceFinish] ArchiveXlogForForceFinishRedo INFO lastRplReadLsn:%lu, lastRplEndLsn:%lu, "
                    "receivedUpto:%lu",
                    lastRplReadLsn, lastRplEndLsn, receivedUpto)));
        XLByteToSeg(lastRplEndLsn, beginSegNo);

        closed = FallbackXlogreader(xlogreader, lastRplReadLsn, lastRplEndLsn);
        CopyXlogForForceFinishRedo(beginSegNo, termId, xlogreader, lastRplEndLsn);
        RenameXlogForForceFinishRedo(beginSegNo, xlogreader->readPageTLI, termId);
        if (closed) {
            ReOpenXlog(xlogreader);
        }
        t_thrd.xlog_cxt.invaildPageCnt = 0;
        t_thrd.xlog_cxt.imcompleteActionCnt = 0;
        XLogCheckInvalidPages();
        btree_clear_imcompleteAction();
        SetSwitchHistoryFile(lastRplEndLsn, receivedUpto, termId);
        t_thrd.xlog_cxt.invaildPageCnt = 0;
        t_thrd.xlog_cxt.imcompleteActionCnt = 0;
        set_wal_rcv_write_rec_ptr(lastRplEndLsn);
        t_thrd.xlog_cxt.receivedUpto = lastRplEndLsn;
        pg_atomic_write_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo), 0);
        ereport(WARNING, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]ArchiveXlogForForceFinishRedo is over!")));
    }
}

void backup_cut_xlog_file(XLogRecPtr lastReplayedEndRecPtr)
{
    errno_t errorno = EOK;
    ereport(DEBUG1, (errmsg("end of backup reached")));

    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

    if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, lastReplayedEndRecPtr)) {
        t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = lastReplayedEndRecPtr;
        SetMinRecoverPointForStats(lastReplayedEndRecPtr);
    }

    errorno = memset_s(&t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint, sizeof(XLogRecPtr), 0, sizeof(XLogRecPtr));
    securec_check(errorno, "", "");
    errorno = memset_s(&t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint, sizeof(XLogRecPtr), 0, sizeof(XLogRecPtr));
    securec_check(errorno, "", "");

    t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired = false;
    UpdateControlFile();

    LWLockRelease(ControlFileLock);
}



static void sendPMBeginHotStby()
{

    if (!t_thrd.xlog_cxt.LocalHotStandbyActive &&
#ifdef ENABLE_MULTIPLE_NODES
        t_thrd.xlog_cxt.standbyState == STANDBY_SNAPSHOT_READY && 
#endif
        t_thrd.xlog_cxt.reachedConsistency && IsUnderPostmaster) {
        bool xminValid = true;
        XLogRecPtr lastReplayedEndRecPtr = t_thrd.shemem_ptr_cxt.XLogCtl->lastReplayedEndRecPtr;
#ifndef ENABLE_MULTIPLE_NODES
        xminValid = TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin) ||
            !g_instance.attr.attr_storage.EnableHotStandby;
#endif

        if (xminValid) {
            /* use volatile pointer to prevent code rearrangement */
            volatile XLogCtlData* xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

            SpinLockAcquire(&xlogctl->info_lck);
            xlogctl->SharedHotStandbyActive = true;
            SpinLockRelease(&xlogctl->info_lck);

            t_thrd.xlog_cxt.LocalHotStandbyActive = true;
            pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.hotStdby), ATOMIC_TRUE);
            ereport(LOG, (errmsg("send signal to be hot standby at %X/%X",
                                (uint32) (lastReplayedEndRecPtr >> 32), (uint32) lastReplayedEndRecPtr)));
            SendPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY);
        }
    }
}



/*
 * Checks if recovery has reached a consistent state. When consistency is
 * reached and we have a valid starting standby snapshot, tell postmaster
 * that it can start accepting read-only connections.
 */
void CheckRecoveryConsistency(void)
{
    XLogRecPtr lastReplayedEndRecPtr;
    XLogCtlData *XLogCtl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&XLogCtl->info_lck);
    lastReplayedEndRecPtr = t_thrd.shemem_ptr_cxt.XLogCtl->lastReplayedEndRecPtr;
    SpinLockRelease(&XLogCtl->info_lck);
    if (pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.permitFinishRedo)) == ATOMIC_FALSE 
        && XLByteLE(t_thrd.xlog_cxt.max_page_flush_lsn, lastReplayedEndRecPtr)) {
        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.permitFinishRedo), ATOMIC_TRUE);
        ereport(WARNING, (errmsg("permit finish redo at %X/%X", (uint32)(lastReplayedEndRecPtr >> 32),
                                 (uint32)lastReplayedEndRecPtr)));
    }
    /*
     * During crash recovery, we don't reach a consistent state until we've
     * replayed all the WAL.
     */
    if (XLogRecPtrIsInvalid(t_thrd.xlog_cxt.minRecoveryPoint))
        return;
    /*
     * Have we reached the point where our base backup was completed?
     */
    if (!XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint) &&
        !XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint) &&
        XLByteLE(t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint, lastReplayedEndRecPtr)) {
        /*
         * We have reached the end of base backup, as indicated by pg_control.
         * The data on disk is now consistent. Reset backupStartPoint and
         * backupEndPoint, and update minRecoveryPoint to make sure we don't
         * allow starting up at an earlier point even if recovery is stopped
         * and restarted soon after this.
         */
        backup_cut_xlog_file(lastReplayedEndRecPtr);
    }

    /*
     * Have we passed our safe starting point? Note that minRecoveryPoint
     * is known to be incorrectly set if ControlFile->backupEndRequired,
     * until the XLOG_BACKUP_RECORD arrives to advise us of the correct
     * minRecoveryPoint. All we know prior to that is that we're not
     * consistent yet.
     */
    if (!t_thrd.xlog_cxt.reachedConsistency && !t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired &&
        XLByteLE(t_thrd.xlog_cxt.minRecoveryPoint, lastReplayedEndRecPtr) &&
        XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint)) {
        /*
         * We found the maintence of `minRecoveryPoint' is not correct,
         * in some scenario, `minRecoveryPoint' set too early, actually it does not
         * reach the Consitency.
         * For MppDB, standby is a warm standby not hot standby, standby does not
         * supoort read only query any more, we do `XLogCheckInvalidPages` when xlog
         * redo done not in the process of redo to work around this problem.
         */
        t_thrd.xlog_cxt.reachedConsistency = true;
        ereport(LOG, (errmsg("consistent recovery state reached at %X/%X", (uint32)(lastReplayedEndRecPtr >> 32),
                             (uint32)lastReplayedEndRecPtr)));
        parallel_recovery::redo_dump_all_stats();
        redo_unlink_stats_file();
    }

    /*
     * Have we got a valid starting snapshot that will allow queries to be
     * run? If so, we can tell postmaster that the database is consistent now,
     * enabling connections.
     */
    sendPMBeginHotStby();
}

/*
 * Is the system still in recovery?
 *
 * Unlike testing InRecovery, this works in any process that's connected to
 * shared memory.
 *
 * As a side-effect, we initialize the local TimeLineID and RedoRecPtr
 * variables the first time we see that recovery is finished.
 */
bool RecoveryInProgress(void)
{
    /*
     * We check shared state each time only until we leave recovery mode. We
     * can't re-enter recovery, so there's no need to keep checking after the
     * shared variable has once been seen false.
     */
    if (!t_thrd.xlog_cxt.LocalRecoveryInProgress) {
        return false;
    }

    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    t_thrd.xlog_cxt.LocalRecoveryInProgress = xlogctl->SharedRecoveryInProgress;

    /*
     * Initialize TimeLineID and RedoRecPtr when we discover that recovery
     * is finished. InitPostgres() relies upon this behaviour to ensure
     * that InitXLOGAccess() is called at backend startup.	(If you change
     * this, see also LocalSetXLogInsertAllowed.)
     */
    if (!t_thrd.xlog_cxt.LocalRecoveryInProgress) {
        InitXLOGAccess();
    }

    return t_thrd.xlog_cxt.LocalRecoveryInProgress;
}

/*
 * Is HotStandby active yet? This is only important in special backends
 * since normal backends won't ever be able to connect until this returns
 * true. Postmaster knows this by way of signal, not via shared memory.
 *
 * Unlike testing standbyState, this works in any process that's connected to
 * shared memory.  (And note that standbyState alone doesn't tell the truth
 * anyway.)
 */
bool HotStandbyActive(void)
{
    /*
     * We check shared state each time only until Hot Standby is active. We
     * can't de-activate Hot Standby, so there's no need to keep checking
     * after the shared variable has once been seen true.
     */
    if (t_thrd.xlog_cxt.LocalHotStandbyActive) {
        return true;
    }

    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    /* spinlock is essential on machines with weak memory ordering! */
    SpinLockAcquire(&xlogctl->info_lck);
    t_thrd.xlog_cxt.LocalHotStandbyActive = xlogctl->SharedHotStandbyActive;
    SpinLockRelease(&xlogctl->info_lck);

    return t_thrd.xlog_cxt.LocalHotStandbyActive;
}

/*
 * Like HotStandbyActive(), but to be used only in WAL replay code,
 * where we don't need to ask any other process what the state is.
 */
bool HotStandbyActiveInReplay(void)
{
    Assert(AmStartupProcess() || !IsPostmasterEnvironment || AmPageRedoProcess());
    return t_thrd.xlog_cxt.LocalHotStandbyActive;
}

/*
 * Is this process allowed to insert new WAL records?
 *
 * Ordinarily this is essentially equivalent to !RecoveryInProgress().
 * But we also have provisions for forcing the result "true" or "false"
 * within specific processes regardless of the global state.
 */
bool XLogInsertAllowed(void)
{
    // If value is "unconditionally true" or "unconditionally false", just
    // return it.  This provides the normal fast path once recovery is known done.
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed >= 0) {
        return (bool)t_thrd.xlog_cxt.LocalXLogInsertAllowed;
    }

    // Else, must check to see if we're still in recovery.
    if (RecoveryInProgress()) {
        return false;
    }

    // On exit from recovery, reset to "unconditionally true", since there is
    // no need to keep checking.
    t_thrd.xlog_cxt.LocalXLogInsertAllowed = 1;
    return true;
}

/*
 * Make XLogInsertAllowed() return true in the current process only.
 *
 * Note: it is allowed to switch LocalXLogInsertAllowed back to -1 later,
 * and even call LocalSetXLogInsertAllowed() again after that.
 */
static void LocalSetXLogInsertAllowed(void)
{
    Assert(t_thrd.xlog_cxt.LocalXLogInsertAllowed == -1);
    t_thrd.xlog_cxt.LocalXLogInsertAllowed = 1;

    /* Initialize as RecoveryInProgress() would do when switching state */
    InitXLOGAccess();
}

/*
 * Subroutine to try to fetch and validate a prior checkpoint record.
 *
 * whichChkpt identifies the checkpoint (merely for reporting purposes).
 * 1 for "primary", 2 for "secondary", 0 for "other" (backup_label)
 */
static XLogRecord *ReadCheckpointRecord(XLogReaderState *xlogreader, XLogRecPtr RecPtr, int whichChkpt,
                                        bool readoldversion)
{
    XLogRecord *record = NULL;

    if (!XRecOffIsValid(RecPtr)) {
        switch (whichChkpt) {
            case 1:
                ereport(LOG, (errmsg("invalid primary checkpoint link in control file")));
                break;
            case 2:
                ereport(LOG, (errmsg("invalid secondary checkpoint link in control file")));
                break;
            default:
                ereport(LOG, (errmsg("invalid checkpoint link in backup_label file")));
                break;
        }
        return NULL;
    }

    record = ReadRecord(xlogreader, RecPtr, LOG, true, readoldversion);

    if (record == NULL) {
        switch (whichChkpt) {
            case 1:
                ereport(LOG, (errmsg("invalid primary checkpoint record")));
                break;
            case 2:
                ereport(LOG, (errmsg("invalid secondary checkpoint record")));
                break;
            default:
                ereport(LOG, (errmsg("invalid checkpoint record")));
                break;
        }
        return NULL;
    }

    if (((XLogPageHeader)xlogreader->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if ((readoldversion ? ((XLogRecordOld *)record)->xl_rmid : ((XLogRecord *)record)->xl_rmid) != RM_XLOG_ID) {
        switch (whichChkpt) {
            case 1:
                ereport(LOG, (errmsg("invalid resource manager ID in primary checkpoint record")));
                break;
            case 2:
                ereport(LOG, (errmsg("invalid resource manager ID in secondary checkpoint record")));
                break;
            default:
                ereport(LOG, (errmsg("invalid resource manager ID in checkpoint record")));
                break;
        }
        return NULL;
    }
    if ((readoldversion ? ((XLogRecordOld *)record)->xl_info : ((XLogRecord *)record)->xl_info) !=
            XLOG_CHECKPOINT_SHUTDOWN &&
        (readoldversion ? ((XLogRecordOld *)record)->xl_info : ((XLogRecord *)record)->xl_info) !=
            XLOG_CHECKPOINT_ONLINE) {
        switch (whichChkpt) {
            case 1:
                ereport(LOG, (errmsg("invalid xl_info in primary checkpoint record")));
                break;
            case 2:
                ereport(LOG, (errmsg("invalid xl_info in secondary checkpoint record")));
                break;
            default:
                ereport(LOG, (errmsg("invalid xl_info in checkpoint record")));
                break;
        }
        return NULL;
    }

    if (((XLogRecordOld *)record)->xl_tot_len !=
            SizeOfXLogRecordOld + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPointOld) &&
        record->xl_tot_len != CHECKPOINT_LEN && record->xl_tot_len != CHECKPOINTNEW_LEN &&
        record->xl_tot_len != CHECKPOINTPLUS_LEN) {
        switch (whichChkpt) {
            case 1:
                ereport(LOG, (errmsg("invalid length of primary checkpoint record")));
                break;
            case 2:
                ereport(LOG, (errmsg("invalid length of secondary checkpoint record")));
                break;
            default:
                ereport(LOG, (errmsg("invalid length of checkpoint record")));
                break;
        }
        return NULL;
    }
    return record;
}

/*
 * This must be called during startup of a backend process, except that
 * it need not be called in a standalone backend (which does StartupXLOG
 * instead).  We need to initialize the local copies of ThisTimeLineID and
 * RedoRecPtr.
 *
 * Note: before Postgres 8.0, we went to some effort to keep the postmaster
 * process's copies of ThisTimeLineID and RedoRecPtr valid too.  This was
 * unnecessary however, since the postmaster itself never touches XLOG anyway.
 */
void InitXLOGAccess(void)
{
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;

    /* ThisTimeLineID doesn't change so we need no lock to copy it */
    t_thrd.xlog_cxt.ThisTimeLineID = t_thrd.shemem_ptr_cxt.XLogCtl->ThisTimeLineID;
    Assert(t_thrd.xlog_cxt.ThisTimeLineID != 0 || IsBootstrapProcessingMode());

    /* Use GetRedoRecPtr to copy the RedoRecPtr safely */
    (void)GetRedoRecPtr();

    /* Also update our copy of doPageWrites. */
    if (ENABLE_INCRE_CKPT) {
        t_thrd.xlog_cxt.doPageWrites = false;
    } else {
        t_thrd.xlog_cxt.doPageWrites = (Insert->fullPageWrites || Insert->forcePageWrites);
    }

    /* Also initialize the working areas for constructing WAL records */
    InitXLogInsert();
}

/*
 * Return the current Redo pointer from shared memory.
 *
 * As a side-effect, the local RedoRecPtr copy is updated.
 */
XLogRecPtr GetRedoRecPtr(void)
{
    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr ptr;

    /*
     * The possibly not up-to-date copy in XlogCtl is enough. Even if we
     * grabbed a WAL insertion slot to read the master copy, someone might
     * update it just after we've released the lock.
     */
    SpinLockAcquire(&xlogctl->info_lck);
    ptr = xlogctl->RedoRecPtr;
    SpinLockRelease(&xlogctl->info_lck);

    if (t_thrd.xlog_cxt.RedoRecPtr < ptr) {
        t_thrd.xlog_cxt.RedoRecPtr = ptr;
    }

    return t_thrd.xlog_cxt.RedoRecPtr;
}

/*
 * Return information needed to decide whether a modified block needs a
 * full-page image to be included in the WAL record.
 *
 * The returned values are cached copies from backend-private memory, and
 * possibly out-of-date.  XLogInsertRecord will re-check them against
 * up-to-date values, while holding the WAL insert lock.
 */
void GetFullPageWriteInfo(XLogFPWInfo *fpwInfo_p)
{
    fpwInfo_p->redoRecPtr = t_thrd.xlog_cxt.RedoRecPtr;
    fpwInfo_p->doPageWrites = t_thrd.xlog_cxt.doPageWrites && !ENABLE_INCRE_CKPT;

    fpwInfo_p->forcePageWrites = t_thrd.shemem_ptr_cxt.XLogCtl->FpwBeforeFirstCkpt && !IsInitdb && !ENABLE_INCRE_CKPT;
}

/*
 * GetInsertRecPtr -- Returns the current insert position.
 *
 * NOTE: The value *actually* returned is the position of the last full
 * xlog page. It lags behind the real insert position by at most 1 page.
 * For that, we don't need to acquire WALInsertLock which can be quite
 * heavily contended, and an approximation is enough for the current
 * usage of this function.
 */
XLogRecPtr GetInsertRecPtr(void)
{
    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr recptr;

    SpinLockAcquire(&xlogctl->info_lck);
    recptr = xlogctl->LogwrtRqst.Write;
    SpinLockRelease(&xlogctl->info_lck);

    return recptr;
}

/*
 * GetFlushRecPtr -- Returns the current flush position, ie, the last WAL
 * position known to be fsync'd to disk.
 */
XLogRecPtr GetFlushRecPtr(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr recptr;

    SpinLockAcquire(&xlogctl->info_lck);
    recptr = xlogctl->LogwrtResult.Flush;
    SpinLockRelease(&xlogctl->info_lck);

    return recptr;
}

/*
 * Get the time of the last xlog segment switch
 */
pg_time_t GetLastSegSwitchTime(void)
{
    pg_time_t result;

    /* Need WALWriteLock, but shared lock is sufficient */
    LWLockAcquire(WALWriteLock, LW_SHARED);
    result = t_thrd.shemem_ptr_cxt.XLogCtl->lastSegSwitchTime;
    LWLockRelease(WALWriteLock);

    return result;
}

/*
 * GetRecoveryTargetTLI - get the current recovery target timeline ID
 */
TimeLineID GetRecoveryTargetTLI(void)
{
    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    TimeLineID result;

    SpinLockAcquire(&xlogctl->info_lck);
    result = xlogctl->RecoveryTargetTLI;
    SpinLockRelease(&xlogctl->info_lck);

    return result;
}

/*
 * DummyStandbySetRecoveryTargetTLI - set the current recovery target timeline ID
 */
void DummyStandbySetRecoveryTargetTLI(TimeLineID timeLineID)
{
    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->RecoveryTargetTLI = timeLineID;
    xlogctl->ThisTimeLineID = timeLineID;
    SpinLockRelease(&xlogctl->info_lck);

    return;
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void ShutdownXLOG(int code, Datum arg)
{
    ereport(LOG, (errmsg("shutting down")));

    if (RecoveryInProgress()) {
        (void)CreateRestartPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    } else {
        /*
         * If archiving is enabled, rotate the last XLOG file so that all the
         * remaining records are archived (postmaster wakes up the archiver
         * process one more time at the end of shutdown). The checkpoint
         * record will go to the next XLOG file and won't be archived (yet).
         */
        if (!IsInitdb && XLogArchivingActive() && XLogArchiveCommandSet()) {
            (void)RequestXLogSwitch();
        }

        CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    }

    /* Shutdown all the page writer threads. */
    ckpt_shutdown_bgwriter();
    ckpt_shutdown_pagewriter();
    if (g_instance.ckpt_cxt_ctl->dirty_page_queue != NULL) {
        pfree(g_instance.ckpt_cxt_ctl->dirty_page_queue);
        g_instance.ckpt_cxt_ctl->dirty_page_queue = NULL;
    }
    g_instance.ckpt_cxt_ctl->prune_queue_lock = NULL;
    g_instance.ckpt_cxt_ctl->ckpt_redo_state.recovery_queue_lock = NULL;

    ShutdownCLOG();
    ShutdownCSNLOG();
    ShutdownMultiXact();

    /* Shutdown double write. */
    dw_exit(true);
    dw_exit(false);

    ereport(LOG, (errmsg("database system is shut down")));
}

/*
 * Log start of a checkpoint.
 */
static void LogCheckpointStart(unsigned int flags, bool restartpoint)
{
    const char *msg = NULL;

    /*
     * XXX: This is hopelessly untranslatable. We could call gettext_noop for
     * the main message, but what about all the flags?
     */
    if (restartpoint) {
        msg = "restartpoint starting:%s%s%s%s%s%s%s";
    } else {
        msg = "checkpoint starting:%s%s%s%s%s%s%s";
    }

    ereport(LOG, (errmsg(msg, (flags & CHECKPOINT_IS_SHUTDOWN) ? " shutdown" : "",
                         (flags & CHECKPOINT_END_OF_RECOVERY) ? " end-of-recovery" : "",
                         (flags & CHECKPOINT_IMMEDIATE) ? " immediate" : "", (flags & CHECKPOINT_FORCE) ? " force" : "",
                         (flags & CHECKPOINT_WAIT) ? " wait" : "", (flags & CHECKPOINT_CAUSE_XLOG) ? " xlog" : "",
                         (flags & CHECKPOINT_CAUSE_TIME) ? " time" : "")));
}

/*
 * Log end of a checkpoint.
 */
static void LogCheckpointEnd(bool restartpoint)
{
    long write_secs, sync_secs, total_secs, longest_secs, average_secs;
    int write_usecs, sync_usecs, total_usecs, longest_usecs, average_usecs;
    uint64 average_sync_time;

    t_thrd.xlog_cxt.CheckpointStats->ckpt_end_t = GetCurrentTimestamp();

    TimestampDifference(t_thrd.xlog_cxt.CheckpointStats->ckpt_write_t, t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_t,
                        &write_secs, &write_usecs);

    TimestampDifference(t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_t, t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_end_t,
                        &sync_secs, &sync_usecs);

    /* Accumulate checkpoint timing summary data, in milliseconds. */
    u_sess->stat_cxt.BgWriterStats->m_checkpoint_write_time += write_secs * 1000 + write_usecs / 1000;
    u_sess->stat_cxt.BgWriterStats->m_checkpoint_sync_time += sync_secs * 1000 + sync_usecs / 1000;

    /*
     * All of the published timing statistics are accounted for.  Only
     * continue if a log message is to be written.
     */
    if (!u_sess->attr.attr_common.log_checkpoints) {
        return;
    }

    TimestampDifference(t_thrd.xlog_cxt.CheckpointStats->ckpt_start_t, t_thrd.xlog_cxt.CheckpointStats->ckpt_end_t,
                        &total_secs, &total_usecs);

    /*
     * Timing values returned from CheckpointStats are in microseconds.
     * Convert to the second plus microsecond form that TimestampDifference
     * returns for homogeneous printing.
     */
    longest_secs = (long)(t_thrd.xlog_cxt.CheckpointStats->ckpt_longest_sync / 1000000);
    longest_usecs = t_thrd.xlog_cxt.CheckpointStats->ckpt_longest_sync - (uint64)longest_secs * 1000000;

    average_sync_time = 0;
    if (t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels > 0) {
        average_sync_time = t_thrd.xlog_cxt.CheckpointStats->ckpt_agg_sync_time /
                            t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels;
    }
    average_secs = (long)(average_sync_time / 1000000);
    average_usecs = average_sync_time - (uint64)average_secs * 1000000;

    if (restartpoint) {
        ereport(LOG, (errmsg("restartpoint complete: wrote %d buffers (%.1f%%); "
                             "%d transaction log file(s) added, %d removed, %d recycled; "
                             "write=%ld.%03d s, sync=%ld.%03d s, total=%ld.%03d s; "
                             "sync files=%d, longest=%ld.%03d s, average=%ld.%03d s",
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written,
                             (double)t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written * 100 /
                                 g_instance.attr.attr_storage.NBuffers,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_added,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_removed,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_recycled, write_secs, write_usecs / 1000,
                             sync_secs, sync_usecs / 1000, total_secs, total_usecs / 1000,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels, longest_secs, longest_usecs / 1000,
                             average_secs, average_usecs / 1000)));
    } else {
        ereport(LOG, (errmsg("checkpoint complete: wrote %d buffers (%.1f%%); "
                             "%d transaction log file(s) added, %d removed, %d recycled; "
                             "write=%ld.%03d s, sync=%ld.%03d s, total=%ld.%03d s; "
                             "sync files=%d, longest=%ld.%03d s, average=%ld.%03d s",
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written,
                             (double)t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written * 100 /
                                 g_instance.attr.attr_storage.NBuffers,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_added,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_removed,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_recycled, write_secs, write_usecs / 1000,
                             sync_secs, sync_usecs / 1000, total_secs, total_usecs / 1000,
                             t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels, longest_secs, longest_usecs / 1000,
                             average_secs, average_usecs / 1000)));
    }
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 *
 * flags is a bitwise OR of the following:
 *	CHECKPOINT_IS_SHUTDOWN: checkpoint is for database shutdown.
 *	CHECKPOINT_END_OF_RECOVERY: checkpoint is for end of WAL recovery.
 *	CHECKPOINT_IMMEDIATE: finish the checkpoint ASAP,
 *		ignoring checkpoint_completion_target parameter.
 *	CHECKPOINT_FORCE: force a checkpoint even if no XLOG activity has occurred
 *		since the last one (implied by CHECKPOINT_IS_SHUTDOWN or
 *		CHECKPOINT_END_OF_RECOVERY).
 *
 * Note: flags contains other bits, of interest here only for logging purposes.
 * In particular note that this routine is synchronous and does not pay
 * attention to CHECKPOINT_WAIT.
 */
void CreateCheckPoint(int flags)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    bool shutdown = false;
    bool removexlog = false;
    CheckPoint checkPoint;
    CheckPointNew checkPointNew;   /* to adapt update and not to modify the storage format */
    CheckPointPlus checkPointPlus; /* to adapt update and not to modify the storage format for global clean */
    XLogRecPtr recptr = InvalidXLogRecPtr;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint32 freespace;
    XLogSegNo _logSegNo;
    XLogRecPtr curInsert;
    VirtualTransactionId *vxids = NULL;
    int nvxids = 0;
    int32 lastlrc = 0;
    errno_t errorno = EOK;
    XLogRecPtr curMinRecLSN = InvalidXLogRecPtr;
    bool doFullCheckpoint = !ENABLE_INCRE_CKPT;
    TransactionId oldest_active_xid = InvalidTransactionId;
    TransactionId globalXmin = InvalidTransactionId;

    /*
     * An end-of-recovery checkpoint is really a shutdown checkpoint, just
     * issued at a different time.
     */
    gstrace_entry(GS_TRC_ID_CreateCheckPoint);
    if ((unsigned int)flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) {
        shutdown = true;
    }

    /* sanity check */
    if (RecoveryInProgress() && ((unsigned int)flags & CHECKPOINT_END_OF_RECOVERY) == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TRANSACTION_STATE), errmsg("can't create a checkpoint during recovery")));
    }

    /* CHECKPOINT_IS_SHUTDOWN CHECKPOINT_END_OF_RECOVERY CHECKPOINT_FORCE shuld do full checkpoint */
    if (shutdown || ((unsigned int)flags & (CHECKPOINT_FORCE))) {
        doFullCheckpoint = true;
    }

    /*
     * Acquire CheckpointLock to ensure only one checkpoint happens at a time.
     * (This is just pro forma, since in the present system structure there is
     * only one process that is allowed to issue checkpoints at any given
     * time.)
     */
    LWLockAcquire(CheckpointLock, LW_EXCLUSIVE);

    /*
     * Prepare to accumulate statistics.
     *
     * Note: because it is possible for log_checkpoints to change while a
     * checkpoint proceeds, we always accumulate stats, even if
     * log_checkpoints is currently off.
     */
    errorno = memset_s(t_thrd.xlog_cxt.CheckpointStats, sizeof(CheckpointStatsData), 0, sizeof(CheckpointStatsData));
    securec_check(errorno, "", "");
    t_thrd.xlog_cxt.CheckpointStats->ckpt_start_t = GetCurrentTimestamp();

    /*
     * Use a critical section to force system panic if we have trouble.
     */
    START_CRIT_SECTION();

    if (shutdown) {
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        t_thrd.shemem_ptr_cxt.ControlFile->state = DB_SHUTDOWNING;
        t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
        UpdateControlFile();
        LWLockRelease(ControlFileLock);
    }

    /*
     * Let smgr prepare for checkpoint; this has to happen before we determine
     * the REDO pointer.  Note that smgr must not do anything that'd have to
     * be undone if we decide no checkpoint is needed.
     */
    smgrpreckpt();

    /* Begin filling in the checkpoint WAL record */
    errorno = memset_s(&checkPoint, sizeof(checkPoint), 0, sizeof(checkPoint));
    securec_check(errorno, "", "");
    checkPoint.time = (pg_time_t)time(NULL);

    /*
     * For Hot Standby, derive the oldestActiveXid before we fix the redo
     * pointer. This allows us to begin accumulating changes to assemble our
     * starting snapshot of locks and transactions.
     */
    if (!shutdown) {
        oldest_active_xid = GetOldestActiveTransactionId(&globalXmin);
    }     
    if (!shutdown && XLogStandbyInfoActive()) {
        checkPoint.oldestActiveXid = oldest_active_xid;
    } else {
        checkPoint.oldestActiveXid = InvalidTransactionId;
    }

#ifdef ENABLE_MOT
    if (doFullCheckpoint) {
        CallCheckpointCallback(EVENT_CHECKPOINT_CREATE_SNAPSHOT, 0);
    }
#endif

    StartSuspendWalInsert(&lastlrc);

    curInsert = XLogBytePosToRecPtr(Insert->CurrBytePos);

    if ((ENABLE_INCRE_CKPT && (flags & CHECKPOINT_CAUSE_TIME)) || doFullCheckpoint) {
        update_dirty_page_queue_rec_lsn(curInsert, true);
    }

    /*
     * If this isn't a shutdown or forced checkpoint, and we have not inserted
     * any XLOG records since the start of the last checkpoint, skip the
     * checkpoint.	The idea here is to avoid inserting duplicate checkpoints
     * when the system is idle. That wastes log space, and more importantly it
     * exposes us to possible loss of both current and previous checkpoint
     * records if the machine crashes just as we're writing the update.
     * (Perhaps it'd make even more sense to checkpoint only when the previous
     * checkpoint record is in a different xlog page?)
     *
     * We have to make two tests to determine that nothing has happened since
     * the start of the last checkpoint: current insertion point must match
     * the end of the last checkpoint record, and its redo pointer must point
     * to itself.
     */
    if (((unsigned int)flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE)) == 0) {
        if (curInsert ==
                t_thrd.shemem_ptr_cxt.ControlFile->checkPoint + MAXALIGN(SizeOfXLogRecord + sizeof(CheckPoint)) &&
            t_thrd.shemem_ptr_cxt.ControlFile->checkPoint == t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo) {
#ifdef ENABLE_MOT
            if (doFullCheckpoint) {
                CallCheckpointCallback(EVENT_CHECKPOINT_ABORT, 0);
            }
#endif
            StopSuspendWalInsert(lastlrc);
            LWLockRelease(CheckpointLock);
            END_CRIT_SECTION();
            smgrsync_with_absorption();
            gstrace_exit(GS_TRC_ID_CreateCheckPoint);
            return;
        }
    } else if (ENABLE_INCRE_CKPT && doFullCheckpoint) {
        /*
         * enableIncrementalCheckpoint guc is on, but some conditions shuld do
         * full checkpoint.
         */
        g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc = get_dirty_page_queue_tail();
        g_instance.ckpt_cxt_ctl->full_ckpt_redo_ptr = curInsert;
        pg_write_barrier();
        if (get_dirty_page_num() > 0) {
            g_instance.ckpt_cxt_ctl->flush_all_dirty_page = true;
        }
        ereport(LOG, (errmsg("will do full checkpoint, need flush %ld pages.", get_dirty_page_num())));
    }

    /* incremental checkpoint */
    if (!doFullCheckpoint) {
        curMinRecLSN = ckpt_get_min_rec_lsn();
        if (XLogRecPtrIsInvalid(curMinRecLSN)) {
            /*
             * If dirty page list is empty, use current queue rec lsn.
             * Check queue head buffer rec lsn again (if some page dirtied after getting
             * get_dirty_page_queue_rec_lsn), get min rec lsn.
             */
            XLogRecPtr cur_queue_rec_lsn = get_dirty_page_queue_rec_lsn();
            curMinRecLSN = ckpt_get_min_rec_lsn();

            if (XLogRecPtrIsInvalid(curMinRecLSN) || XLByteLT(cur_queue_rec_lsn, curMinRecLSN)) {
                curMinRecLSN = cur_queue_rec_lsn;
            }
            if (u_sess->attr.attr_storage.log_pagewriter) {
                ereport(LOG,
                        (errmodule(MOD_INCRE_CKPT),
                         errmsg("get min_recLSN is invalid, dirty page num is %ld, get queue reclsn is %08X/%08X",
                                get_dirty_page_num(), (uint32)(curMinRecLSN >> XLOG_LSN_SWAP), (uint32)curMinRecLSN)));
            }
        }

        if (XLByteEQ(curMinRecLSN, g_instance.ckpt_cxt_ctl->ckpt_current_redo_point)) {
            StopSuspendWalInsert(lastlrc);
            LWLockRelease(CheckpointLock);
            END_CRIT_SECTION();

            if (u_sess->attr.attr_storage.log_pagewriter) {
                ereport(LOG, (errmodule(MOD_INCRE_CKPT),
                              errmsg("Checkpoint meets prev checkpoint lsn is %08X/%08X, now min rec lsn is %08X/%08X, "
                                     "checkpoint flag is %d",
                                     (uint32)(g_instance.ckpt_cxt_ctl->ckpt_current_redo_point >> XLOG_LSN_SWAP),
                                     (uint32)g_instance.ckpt_cxt_ctl->ckpt_current_redo_point,
                                     (uint32)(curMinRecLSN >> XLOG_LSN_SWAP),
                                     (uint32)curMinRecLSN, flags)));
            }
            if (dw_enabled()) {
                smgrsync_with_absorption();
            }
            gstrace_exit(GS_TRC_ID_CreateCheckPoint);
            return;
        } else if (XLByteLT(curMinRecLSN, g_instance.ckpt_cxt_ctl->ckpt_current_redo_point)) {
            ereport(PANIC,
                    (errmodule(MOD_INCRE_CKPT),
                     errmsg("curMinRecLSN little prev checkpoint lsn is %08X/%08X,now lsn is %08X/%08X",
                            (uint32)(g_instance.ckpt_cxt_ctl->ckpt_current_redo_point >> XLOG_LSN_SWAP),
                            (uint32)g_instance.ckpt_cxt_ctl->ckpt_current_redo_point,
                            (uint32)(curMinRecLSN >> XLOG_LSN_SWAP), (uint32)curMinRecLSN)));
        }

        if (u_sess->attr.attr_storage.log_pagewriter) {
            ereport(LOG,
                    (errmodule(MOD_INCRE_CKPT),
                     errmsg("Checkpoint prev checkpoint lsn is %08X/%08X, now will create new checkpoint is %08X/%08X",
                            (uint32)(g_instance.ckpt_cxt_ctl->ckpt_current_redo_point >> XLOG_LSN_SWAP),
                            (uint32)g_instance.ckpt_cxt_ctl->ckpt_current_redo_point,
                            (uint32)(curMinRecLSN >> XLOG_LSN_SWAP), (uint32)curMinRecLSN)));
        }
    }
    /*
     * An end-of-recovery checkpoint is created before anyone is allowed to
     * write WAL. To allow us to write the checkpoint record, temporarily
     * enable XLogInsertAllowed.  (This also ensures ThisTimeLineID is
     * initialized, which we need here and in AdvanceXLInsertBuffer.)
     */
    if ((unsigned int)flags & CHECKPOINT_END_OF_RECOVERY) {
        LocalSetXLogInsertAllowed();
    }

    checkPoint.ThisTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    checkPoint.fullPageWrites = Insert->fullPageWrites;

    /*
     * Compute new REDO record ptr = location of next XLOG record.
     *
     * NB: this is NOT necessarily where the checkpoint record itself will be,
     * since other backends may insert more XLOG records while we're off doing
     * the buffer flush work.  Those XLOG records are logically after the
     * checkpoint, even though physically before it.  Got that?
     */
    if (!doFullCheckpoint) {
        /*
         * Incremental Checkpoint use queue first page recLSN, when the dirty page queue is empty,
         * choose the dirty page queue recLSN. Dirty page queue lsn has computed redo ptr when
         * update_dirty_page_queue_rec_lsn.
         */
        checkPoint.redo = curMinRecLSN;
    } else {
        /* Full checkpoint use curInsert */
        freespace = INSERT_FREESPACE(curInsert);
        if (freespace == 0) {
            if (curInsert % XLogSegSize == 0) {
                curInsert += SizeOfXLogLongPHD;
            } else {
                curInsert += SizeOfXLogShortPHD;
            }
        }
        checkPoint.redo = curInsert;
    }

    /*
     * Here we update the shared RedoRecPtr for future XLogInsert calls; this
     * must be done while holding the insert lock AND the info_lck.
     *
     * Note: if we fail to complete the checkpoint, RedoRecPtr will be left
     * pointing past where it really needs to point.  This is okay; the only
     * consequence is that XLogInsert might back up whole buffers that it
     * didn't really need to.  We can't postpone advancing RedoRecPtr because
     * XLogInserts that happen while we are dumping buffers must assume that
     * their buffer changes are not included in the checkpoint.
     */
    t_thrd.xlog_cxt.RedoRecPtr = xlogctl->Insert.RedoRecPtr = checkPoint.redo;

#ifdef ENABLE_MOT
    if (doFullCheckpoint) {
        CallCheckpointCallback(EVENT_CHECKPOINT_SNAPSHOT_READY, checkPoint.redo);
    }
#endif

    /*
     * Now we can release WAL insert lock, allowing other xacts to proceed
     * while we are flushing disk buffers.
     */
    StopSuspendWalInsert(lastlrc);

    /* Update the info_lck-protected copy of RedoRecPtr as well */
    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->RedoRecPtr = checkPoint.redo;
    SpinLockRelease(&xlogctl->info_lck);

    /*
     * If enabled, log checkpoint start.  We postpone this until now so as not
     * to log anything if we decided to skip the checkpoint.
     */
    if (u_sess->attr.attr_common.log_checkpoints) {
        LogCheckpointStart((unsigned int)flags, false);
    }

    TRACE_POSTGRESQL_CHECKPOINT_START(flags);

    /*
     * Get the other info we need for the checkpoint record.
     */
    LWLockAcquire(XidGenLock, LW_SHARED);
    checkPoint.nextXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    checkPoint.oldestXid = t_thrd.xact_cxt.ShmemVariableCache->oldestXid;
    checkPoint.oldestXidDB = t_thrd.xact_cxt.ShmemVariableCache->oldestXidDB;
    LWLockRelease(XidGenLock);

    MultiXactGetCheckptMulti(shutdown, &checkPoint.nextMulti, &checkPoint.nextMultiOffset);

    /*
     * Having constructed the checkpoint record, ensure all shmem disk buffers
     * and commit-log buffers are flushed to disk.
     *
     * This I/O could fail for various reasons.  If so, we will fail to
     * complete the checkpoint, but there is no reason to force a system
     * panic. Accordingly, exit critical section while doing it.
     */
    END_CRIT_SECTION();

    /*
     * In some cases there are groups of actions that must all occur on
     * one side or the other of a checkpoint record. Before flushing the
     * checkpoint record we must explicitly wait for any backend currently
     * performing those groups of actions.
     *
     * One example is end of transaction, so we must wait for any transactions
     * that are currently in commit critical sections.  If an xact inserted
     * its commit record into XLOG just before the REDO point, then a crash
     * restart from the REDO point would not replay that record, which means
     * that our flushing had better include the xact's update of pg_clog.  So
     * we wait till he's out of his commit critical section before proceeding.
     * See notes in RecordTransactionCommit().
     *
     * Because we've already released WALInsertLock, this test is a bit fuzzy:
     * it is possible that we will wait for xacts we didn't really need to
     * wait for.  But the delay should be short and it seems better to make
     * checkpoint take a bit longer than to hold locks longer than necessary.
     * (In fact, the whole reason we have this issue is that xact.c does
     * commit record XLOG insertion and clog update as two separate steps
     * protected by different locks, but again that seems best on grounds of
     * minimizing lock contention.)
     *
     * A transaction that has not yet set delayChkpt when we look cannot be at
     * risk, since he's not inserted his commit record yet; and one that's
     * already cleared it is not at risk either, since he's done fixing clog
     * and we will correctly flush the update below.  So we cannot miss any
     * xacts we need to wait for.
     */
    vxids = GetVirtualXIDsDelayingChkpt(&nvxids);
    if (nvxids > 0) {
        do {
            pg_usleep(10000L); /* wait for 10 msec */
        } while (HaveVirtualXIDsDelayingChkpt(vxids, nvxids));
    }
    pfree_ext(vxids);


    /*
     * Truncate pg_csnlog if possible.  We can throw away all data before
     * the oldest XMIN of any running transaction.  No future transaction will
     * attempt to reference any pg_csnlog entry older than that (see Asserts
     * in csnlog.c).  During recovery, though, we mustn't do this because
     * StartupCSNLOG hasn't been called yet.
     */
    pg_time_t now = (pg_time_t)time(NULL);
    int elapsed_secs = now - t_thrd.checkpoint_cxt.last_truncate_log_time;

    if (!RecoveryInProgress() &&
        (GTM_FREE_MODE || TransactionIdIsNormal(t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin))) {
        /*
         * Reduce the frequency of trucate CSN log to avoid the probability of lock contention
         * Incremental chekpoint does not require frequent truncate of csnlog.
         */
        if (doFullCheckpoint || elapsed_secs >= u_sess->attr.attr_storage.fullCheckPointTimeout) {
            TransactionId cutoff_xid = GetOldestXmin(NULL);
            /*
             * Since vacuum xid,xmin are not included in oldestxmin,
             * local oldest active xid may lower than oldestxmin,
             * don't truncate it for safe.
             */
            if (TransactionIdIsNormal(globalXmin) && TransactionIdPrecedes(globalXmin, cutoff_xid)) {
                cutoff_xid = globalXmin;
            }
            TruncateCSNLOG(cutoff_xid);
            t_thrd.checkpoint_cxt.last_truncate_log_time = now;
        }
    }

    CheckPointGuts(checkPoint.redo, flags, doFullCheckpoint);

#ifdef ENABLE_MOT
    if (doFullCheckpoint) {
        CallCheckpointCallback(EVENT_CHECKPOINT_BEGIN_CHECKPOINT, 0);
    }
#endif

    /*
     * Take a snapshot of running transactions and write this to WAL. This
     * allows us to reconstruct the state of running transactions during
     * archive recovery, if required. Skip, if this info disabled.
     *
     * If we are shutting down, or Startup process is completing crash
     * recovery we don't need to write running xact data.
     */

#ifdef ENABLE_MULTIPLE_NODES
    if (!shutdown && XLogStandbyInfoActive()) {
        LogStandbySnapshot();
    }
#else
    if (XLogStandbyInfoActive() && !RecoveryInProgress()) {
        LogStandbySnapshot();
    }
#endif

    START_CRIT_SECTION();

    LWLockAcquire(OidGenLock, LW_SHARED);
    checkPoint.nextOid = t_thrd.xact_cxt.ShmemVariableCache->nextOid;
    if (!shutdown) {
        checkPoint.nextOid += t_thrd.xact_cxt.ShmemVariableCache->oidCount;
    }
    LWLockRelease(OidGenLock);
    /*
     * Select point at which we can truncate the log, which we base on the
     * prior checkpoint's earliest info.
     */
    XLByteToSeg(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, _logSegNo);
    checkPoint.remove_seg = 1;

    if (_logSegNo && !GetDelayXlogRecycle()) {
        KeepLogSeg(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, &_logSegNo, curInsert);
        checkPoint.remove_seg = _logSegNo;
        _logSegNo--;
        /*
         * Save one more segment to handle the corner case. For example, standby has replayed to
         * the end to one segment file (000000010000002400000078) and the LSN is exactly 24/79000000.
         * The corresponding slot on primary is 24/79000000. As a result, if standby restart to
         * request xlog stream from the lastest valid record (23/xxxxxxx), cannot find on primary.
         */
        if (_logSegNo > 0) {
            _logSegNo--;
        }
        removexlog = true;
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(DN_PRIMARY_CHECKPOINT_KEEPXLOG, default_error_emit)) {
            ereport(get_distribute_test_param()->elevel,
                    (errmsg("default_error_emit create check point time:%ds, stub_name:%s",
                            get_distribute_test_param()->sleep_time, get_distribute_test_param()->test_stub_name)));
            checkPoint.remove_seg = 1;
        }
#endif
    }

    /*
     * Now insert the checkpoint record into XLOG.
     */
    XLogBeginInsert();

    /* use version num to control */
    if (t_thrd.proc->workingVersionNum >= GTMLITE_VERSION_NUM && !InplaceUpgradePrecommit) {
        errno_t rcm = memcpy_s(&checkPointPlus, sizeof(CheckPoint), &checkPoint, sizeof(CheckPoint));
        securec_check(rcm, "", "");
        if (IsBootstrapProcessingMode() ||
            !COMMITSEQNO_IS_COMMITTED(t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo)) {
            checkPointPlus.next_csn = COMMITSEQNO_FIRST_NORMAL + 1;
            checkPointPlus.length = (uint64)sizeof(CheckPointPlus);
            checkPointPlus.recent_global_xmin = InvalidTransactionId;
        } else {
            checkPointPlus.next_csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
            checkPointPlus.length = (uint64)sizeof(CheckPointPlus);
            checkPointPlus.recent_global_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin;
        }
        XLogRegisterData((char *)(&checkPointPlus), sizeof(checkPointPlus));
    } else if (t_thrd.proc->workingVersionNum > GTM_OLD_VERSION_NUM && !InplaceUpgradePrecommit) {
        errno_t rcm = memcpy_s(&checkPointNew, sizeof(CheckPoint), &checkPoint, sizeof(CheckPoint));
        securec_check(rcm, "", "");
        if (IsBootstrapProcessingMode() ||
            !COMMITSEQNO_IS_COMMITTED(t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo)) {
            checkPointNew.next_csn = COMMITSEQNO_FIRST_NORMAL + 1;
        } else {
            checkPointNew.next_csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
        }
        XLogRegisterData((char *)(&checkPointNew), sizeof(checkPointNew));
    } else {
        XLogRegisterData((char *)(&checkPoint), sizeof(checkPoint));
    }
    recptr = XLogInsert(RM_XLOG_ID, shutdown ? XLOG_CHECKPOINT_SHUTDOWN : XLOG_CHECKPOINT_ONLINE);

    XLogWaitFlush(recptr);

    /*
     * We mustn't write any new WAL after a shutdown checkpoint, or it will be
     * overwritten at next startup.  No-one should even try, this just allows
     * sanity-checking.  In the case of an end-of-recovery checkpoint, we want
     * to just temporarily disable writing until the system has exited
     * recovery.
     */
    if (shutdown) {
        if ((unsigned int)flags & CHECKPOINT_END_OF_RECOVERY) {
            t_thrd.xlog_cxt.LocalXLogInsertAllowed = -1; /* return to "check" state */
        } else {
            t_thrd.xlog_cxt.LocalXLogInsertAllowed = 0; /* never again write WAL */
        }
    }

    if (doFullCheckpoint && ENABLE_INCRE_CKPT) {
        XLogRecPtr MinRecLSN = ckpt_get_min_rec_lsn();
        if (!XLogRecPtrIsInvalid(MinRecLSN) && XLByteLT(MinRecLSN, t_thrd.xlog_cxt.RedoRecPtr)) {
            ereport(PANIC, (errmsg("current dirty page list head recLSN %08X/%08X smaller than redo lsn %08X/%08X",
                                   (uint32)(MinRecLSN >> XLOG_LSN_SWAP), (uint32)MinRecLSN,
                                   (uint32)(t_thrd.xlog_cxt.RedoRecPtr >> XLOG_LSN_SWAP),
                                   (uint32)t_thrd.xlog_cxt.RedoRecPtr)));
        }
    }
    /*
     * We now have ProcLastRecPtr = start of actual checkpoint record, recptr
     * = end of actual checkpoint record.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (shutdown && !XLByteEQ(checkPoint.redo, t_thrd.xlog_cxt.ProcLastRecPtr)) {
        ereport(PANIC, (errmsg("concurrent transaction log activity while database system is shutting down")));
    }
#else
    if (shutdown && !XLByteEQ(checkPoint.redo, t_thrd.xlog_cxt.ProcLastRecPtr) && !XLogStandbyInfoActive()) {
        ereport(PANIC, (errmsg("concurrent transaction log activity while database system is shutting down")));
    }
#endif
    PrintCkpXctlControlFile(t_thrd.shemem_ptr_cxt.ControlFile->checkPoint,
                            &(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy), t_thrd.xlog_cxt.ProcLastRecPtr,
                            &checkPoint, t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint, "CreateCheckPoint");

    /*
     * Update the control file.
     */
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    if (shutdown) {
        t_thrd.shemem_ptr_cxt.ControlFile->state = DB_SHUTDOWNED;
    }
    t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
    t_thrd.shemem_ptr_cxt.ControlFile->checkPoint = t_thrd.xlog_cxt.ProcLastRecPtr;
    t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy = checkPoint;
    t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
    g_instance.ckpt_cxt_ctl->ckpt_current_redo_point = checkPoint.redo;
    /* crash recovery should always recover to the end of WAL */
    errorno = memset_s(&t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, sizeof(XLogRecPtr), 0, sizeof(XLogRecPtr));
    securec_check(errorno, "", "");
    ereport(LOG, (errmsg("will update control file (create checkpoint), shutdown:%d", shutdown)));

    UpdateControlFile();
    LWLockRelease(ControlFileLock);

    /* Update shared-memory copy of checkpoint XID/epoch */
    {
        /* use volatile pointer to prevent code rearrangement */
        xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        xlogctl->ckptXid = checkPoint.nextXid;
        SpinLockRelease(&xlogctl->info_lck);
    }

    /*
     * We are now done with critical updates; no need for system panic if we
     * have trouble while fooling with old log segments.
     */
    END_CRIT_SECTION();

    // Let smgr do post-checkpoint cleanup (eg, deleting old files).
    smgrpostckpt();

    if (!t_thrd.cbm_cxt.XlogCbmSys->firstCPCreated) {
        t_thrd.cbm_cxt.XlogCbmSys->firstCPCreated = true;
    }

    if (doFullCheckpoint || ((unsigned int)flags & CHECKPOINT_IMMEDIATE) ||
        elapsed_secs >= u_sess->attr.attr_storage.fullCheckPointTimeout) {
        if (g_instance.proc_base->cbmwriterLatch) {
            SetLatch(g_instance.proc_base->cbmwriterLatch);
        }
    }
    /*
     * Delete old log files (those no longer needed even for previous
     * checkpoint or the standbys in XLOG streaming).
     */
    if (removexlog) {
        RemoveOldXlogFiles(_logSegNo, recptr);
    }
    /*
     * Make more log segments if needed.  (Do this after recycling old log
     * segments, since that may supply some of the needed files.)
     */
    if (!shutdown) {
        PreallocXlogFiles(recptr);
    }

    /* Real work is done, but log and update stats before releasing lock. */
    LogCheckpointEnd(false);

    TRACE_POSTGRESQL_CHECKPOINT_DONE(t_thrd.xlog_cxt.CheckpointStats->ckpt_bufs_written,
                                     g_instance.attr.attr_storage.NBuffers,
                                     t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_added,
                                     t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_removed,
                                     t_thrd.xlog_cxt.CheckpointStats->ckpt_segs_recycled);

    LWLockRelease(CheckpointLock);
    gstrace_exit(GS_TRC_ID_CreateCheckPoint);
}

/*
 * Flush all data in shared memory to disk, and fsync
 *
 * This is the common code shared between regular checkpoints and
 * recovery restartpoints.
 */
static void CheckPointGuts(XLogRecPtr checkPointRedo, int flags, bool doFullCheckpoint)
{
    gstrace_entry(GS_TRC_ID_CheckPointGuts);
    CheckPointCLOG();
    CheckPointCSNLOG();
    CheckPointMultiXact();
    CheckPointPredicate();
    CheckPointRelationMap();
    CheckPointReplicationSlots();
    CheckPointSnapBuild();
    /*
     * If enable_incremental_checkpoint is on, there are two scenarios:
     * Incremental checkpoint, don't need flush any dirty page, and don't wait
     * pagewriter thread flush dirty page.
     * Do full checkpoint, checkpoint thread also don't flush any dirty page, but
     * need wait pagewriter thread flush dirty page.
     */
    CheckPointBuffers(flags, doFullCheckpoint); /* performs all required fsyncs */
    /* We deliberately delay 2PC checkpointing as long as possible */
    CheckPointTwoPhase(checkPointRedo);
    gstrace_exit(GS_TRC_ID_CheckPointGuts);
}

void PushRestartPointToQueue(XLogRecPtr recordReadRecPtr, const CheckPoint checkPoint)
{
    RecoveryQueueState *state = &g_instance.ckpt_cxt_ctl->ckpt_redo_state;
    uint loc = 0;

    if (!ENABLE_INCRE_CKPT) {
        return;
    }

    (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
    if (state->end - state->start + 1 >= RESTART_POINT_QUEUE_LEN) {
        state->start++;
    }
    loc = state->end % RESTART_POINT_QUEUE_LEN;
    state->ckpt_rec_queue[loc].CkptLSN = recordReadRecPtr;
    state->ckpt_rec_queue[loc].checkpoint = checkPoint;
    state->end++;
    LWLockRelease(state->recovery_queue_lock);
}

/*
 * Save a checkpoint for recovery restart if appropriate
 *
 * This function is called each time a checkpoint record is read from XLOG.
 * It must determine whether the checkpoint represents a safe restartpoint or
 * not.  If so, the checkpoint record is stashed in shared memory so that
 * CreateRestartPoint can consult it.  (Note that the latter function is
 * executed by the checkpointer, while this one will be executed by the
 * startup process.)
 */
static void RecoveryRestartPoint(const CheckPoint checkPoint, XLogRecPtr recordReadRecPtr)
{
    const uint32 shitRightLength = 32;
    g_instance.comm_cxt.predo_cxt.newestCheckpointLoc = recordReadRecPtr;
    const_cast<CheckPoint &>(g_instance.comm_cxt.predo_cxt.newestCheckpoint) = const_cast<CheckPoint &>(checkPoint);
    if (!IsRestartPointSafe(checkPoint.redo)) {
        return;
    }

    if (IsExtremeRedo()) {
        XLogRecPtr safeCheckPoint = extreme_rto::GetSafeMinCheckPoint();
        if (XLByteEQ(safeCheckPoint, MAX_XLOG_REC_PTR) || XLByteLT(safeCheckPoint, recordReadRecPtr)) {
            ereport(WARNING, (errmsg("RecoveryRestartPoint is false at %X/%X,last safe point is %X/%X",
                (uint32)(recordReadRecPtr >> 32), (uint32)(recordReadRecPtr), (uint32)(safeCheckPoint >> 32), 
                (uint32)(safeCheckPoint))));
            return;
        }
    } else if (!parallel_recovery::IsRecoveryRestartPointSafeForWorkers(recordReadRecPtr)) {
        ereport(WARNING, (errmsg("RecoveryRestartPointSafe is false at %X/%X",
            static_cast<uint32>(recordReadRecPtr >> shitRightLength), static_cast<uint32>(recordReadRecPtr))));
        return;
    }

    update_dirty_page_queue_rec_lsn(recordReadRecPtr, true);
    pg_write_barrier();
    PushRestartPointToQueue(recordReadRecPtr, checkPoint);
    /*
     * Copy the checkpoint record to shared memory, so that checkpointer can
     * work out the next time it wants to perform a restartpoint.
     */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->lastCheckPointRecPtr = recordReadRecPtr;
    xlogctl->lastCheckPoint = checkPoint;
    SpinLockRelease(&xlogctl->info_lck);

    if (t_thrd.xlog_cxt.needImmediateCkp) {
        ereport(LOG, (errmsg("RecoveryRestartPoint do ImmediateCkp at %X/%X", 
            static_cast<uint32>(recordReadRecPtr >> shitRightLength), static_cast<uint32>(recordReadRecPtr))));
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
        t_thrd.xlog_cxt.needImmediateCkp = false;
    }
}

bool IsRestartPointSafe(const XLogRecPtr checkPoint)
{
    /*
     * Is it safe to restartpoint?	We must ask each of the resource managers
     * whether they have any partial state information that might prevent a
     * correct restart from this point.  If so, we skip this opportunity, but
     * return at the next checkpoint record for another try.
     */
    for (int rmid = 0; rmid <= RM_MAX_ID; rmid++) {
        if (RmgrTable[rmid].rm_safe_restartpoint != NULL)
            if (!(RmgrTable[rmid].rm_safe_restartpoint())) {
                ereport(WARNING, (errmsg("RM %d not safe to record restart point at %X/%X", rmid,
                                         (uint32)(checkPoint >> 32), (uint32)checkPoint)));
                return false;
            }
    }

    /*
     * Also refrain from creating a restartpoint if we have seen any
     * references to non-existent pages. Restarting recovery from the
     * restartpoint would not see the references, so we would lose the
     * cross-check that the pages belonged to a relation that was dropped
     * later.
     */
    if (XLogHaveInvalidPages()) {
        ereport(WARNING, (errmsg("could not record restart point at %X/%X because there "
                                 "are unresolved references to invalid pages",
                                 (uint32)(checkPoint >> 32), (uint32)checkPoint)));
        return false;
    }
    return true;
}

void wait_all_dirty_page_flush(int flags, XLogRecPtr redo)
{
    /* need wait all dirty page finish flush */
    if (ENABLE_INCRE_CKPT) {
        g_instance.ckpt_cxt_ctl->full_ckpt_redo_ptr = redo;
        g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc = get_dirty_page_queue_tail();
        pg_write_barrier();
        if (get_dirty_page_num() > 0) {
            g_instance.ckpt_cxt_ctl->flush_all_dirty_page = true;
            ereport(LOG, (errmsg("CreateRestartPoint, need flush %ld pages.", get_dirty_page_num())));
            CheckPointBuffers(flags, true);
        }
    }
    return;
}

bool RecoveryQueueIsEmpty()
{
    RecoveryQueueState *state = &g_instance.ckpt_cxt_ctl->ckpt_redo_state;
    int num;

    (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
    num = state->end - state->start;
    (void)LWLockRelease(state->recovery_queue_lock);

    if (num == 0) {
        return true;
    } else {
        return false;
    }
}

/*
 * In the recovery phase, don't push the redo point to the latest position, avoid the I/O peak.
 * calculate the redo point based on the page flushing speed and max_redo_log_size.
 */
const int BYTE_PER_KB = 1024;
XLogRecPtr GetRestartPointInRecovery(CheckPoint *restartCheckPoint)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr restartRecPtr = InvalidXLogRecPtr;

    XLogRecPtr curMinRecLSN = ckpt_get_min_rec_lsn();
    RecoveryQueueState *state = &g_instance.ckpt_cxt_ctl->ckpt_redo_state;
    if (XLogRecPtrIsInvalid(curMinRecLSN)) {
        /* The dirty page queue is empty, so the redo point can be updated to the latest position. */
        (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
        state->start = state->end > 0 ? state->end - 1 : state->end;
        (void)LWLockRelease(state->recovery_queue_lock);

        SpinLockAcquire(&xlogctl->info_lck);
        restartRecPtr = xlogctl->lastCheckPointRecPtr;
        *restartCheckPoint = const_cast<CheckPoint &>(xlogctl->lastCheckPoint);
        SpinLockRelease(&xlogctl->info_lck);
        Assert(XLByteLE(restartRecPtr, get_dirty_page_queue_rec_lsn()));
    } else {
        int num = 0;
        int loc = 0;
        int i = 0;
        XLogRecPtr replayLastLSN;
        XLogRecPtr targetLSN;

        if (RecoveryQueueIsEmpty()) {
            return restartRecPtr;
        }
        SpinLockAcquire(&xlogctl->info_lck);
        replayLastLSN = xlogctl->lastCheckPointRecPtr;
        SpinLockRelease(&xlogctl->info_lck);

        targetLSN = replayLastLSN - u_sess->attr.attr_storage.max_redo_log_size * BYTE_PER_KB;

        (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
        num = state->end - state->start;

        /* 
         * If the pagewriter flush dirty page to disk quickly, push the redo point to 
         * the position closest to curMinRecLSN .
         */
        if (XLByteLE(targetLSN, curMinRecLSN)) {
            for (i = 0; i < num; i++) {
                loc = (state->start + i) % RESTART_POINT_QUEUE_LEN;
                if (state->ckpt_rec_queue[loc].checkpoint.redo > curMinRecLSN) {
                    if (i > 0) {
                        loc = (state->start + i - 1) % RESTART_POINT_QUEUE_LEN;
                        state->start = state->start + i - 1;
                    }
                    restartRecPtr = state->ckpt_rec_queue[loc].CkptLSN;
                    *restartCheckPoint = state->ckpt_rec_queue[loc].checkpoint;
                    break;
                }
            }
        } else {
            /* In other cases, push the checkpoint loc to the position closest to targetLSN. */
            for (i = 0; i < num; i++) {
                loc = (state->start + i) % RESTART_POINT_QUEUE_LEN;
                if (state->ckpt_rec_queue[loc].CkptLSN > targetLSN) {
                    if (i > 0) {
                        uint64 gap = state->ckpt_rec_queue[loc].CkptLSN - targetLSN;
                        int prevLoc = (state->start + i - 1) % RESTART_POINT_QUEUE_LEN;
                        if (targetLSN - state->ckpt_rec_queue[prevLoc].CkptLSN < gap) {
                            restartRecPtr = state->ckpt_rec_queue[prevLoc].CkptLSN;
                            *restartCheckPoint = state->ckpt_rec_queue[prevLoc].checkpoint;
                            state->start = state->start + i - 1;
                        } else {
                            restartRecPtr = state->ckpt_rec_queue[loc].CkptLSN;
                            *restartCheckPoint = state->ckpt_rec_queue[loc].checkpoint;
                            state->start = state->start + i;
                        }
                    } else {
                        restartRecPtr = state->ckpt_rec_queue[loc].CkptLSN;
                        *restartCheckPoint = state->ckpt_rec_queue[loc].checkpoint;
                    }
                    break;
                }
            }
        }

        if (XLogRecPtrIsInvalid(restartRecPtr) && num > 0) {
            loc = (state->end - 1) % RESTART_POINT_QUEUE_LEN;
            restartRecPtr = state->ckpt_rec_queue[loc].CkptLSN;
            *restartCheckPoint = state->ckpt_rec_queue[loc].checkpoint;
            state->start = state->end - 1;
        }
        (void)LWLockRelease(state->recovery_queue_lock);
    }
    return restartRecPtr;
}

/*
 * Establish a restartpoint if possible.
 *
 * This is similar to CreateCheckPoint, but is used during WAL recovery
 * to establish a point from which recovery can roll forward without
 * replaying the entire recovery log.
 *
 * Returns true if a new restartpoint was established. We can only establish
 * a restartpoint if we have replayed a safe checkpoint record since last
 * restartpoint.
 */
bool CreateRestartPoint(int flags)
{
    XLogRecPtr lastCheckPointRecPtr;
    CheckPoint lastCheckPoint;
    XLogSegNo _logSegNo;
    TimestampTz xtime;
    int32 lastlrc = 0;
    errno_t errorno = EOK;
    bool recoveryInProgress = true;
    bool doFullCkpt = !ENABLE_INCRE_CKPT;

    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    /*
     * Acquire CheckpointLock to ensure only one restartpoint or checkpoint
     * happens at a time.
     */
    gstrace_entry(GS_TRC_ID_CreateRestartPoint);
    LWLockAcquire(CheckpointLock, LW_EXCLUSIVE);

    recoveryInProgress = RecoveryInProgress();
    /*
     * Check that we're still in recovery mode. It's ok if we exit recovery
     * mode after this check, the restart point is valid anyway.
     */
    if (!recoveryInProgress) {
        ereport(DEBUG2, (errmsg("skipping restartpoint, recovery has already ended")));
        LWLockRelease(CheckpointLock);
        smgrsync_with_absorption();
        gstrace_exit(GS_TRC_ID_CreateRestartPoint);
        return false;
    }

    if (doFullCkpt ||
        ((unsigned int)flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE))) {
        doFullCkpt = true;
        if (ENABLE_INCRE_CKPT) {
            RecoveryQueueState *state = &g_instance.ckpt_cxt_ctl->ckpt_redo_state;
            (void)LWLockAcquire(state->recovery_queue_lock, LW_EXCLUSIVE);
            state->start = state->end > 0 ? state->end - 1 : state->end;
            (void)LWLockRelease(state->recovery_queue_lock);
        }
        SpinLockAcquire(&xlogctl->info_lck);
        lastCheckPointRecPtr = xlogctl->lastCheckPointRecPtr;
        lastCheckPoint = xlogctl->lastCheckPoint;
        SpinLockRelease(&xlogctl->info_lck);
    } else {
        lastCheckPointRecPtr = GetRestartPointInRecovery(&lastCheckPoint);
    }

    /*
     * If the last checkpoint record we've replayed is already our last
     * restartpoint, we can't perform a new restart point. We still update
     * minRecoveryPoint in that case, so that if this is a shutdown restart
     * point, we won't start up earlier than before. That's not strictly
     * necessary, but when hot standby is enabled, it would be rather weird if
     * the database opened up for read-only connections at a point-in-time
     * before the last shutdown. Such time travel is still possible in case of
     * immediate shutdown, though.
     *
     * We don't explicitly advance minRecoveryPoint when we do create a
     * restartpoint. It's assumed that flushing the buffers will do that as a
     * side-effect.
     */
    if (XLogRecPtrIsInvalid(lastCheckPointRecPtr) ||
        XLByteLE(lastCheckPoint.redo, t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo)) {
#ifdef USE_ASSERT_CHECKING
        int mode = LOG;
#else
        int mode = DEBUG2;
#endif
        ereport(mode, (errmsg("skipping restartpoint, already performed at %X/%X",
                                (uint32)(lastCheckPoint.redo >> 32), (uint32)lastCheckPoint.redo)));

        UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
        if ((unsigned int)flags & CHECKPOINT_IS_SHUTDOWN) {
            wait_all_dirty_page_flush(flags, lastCheckPoint.redo);
            LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
            t_thrd.shemem_ptr_cxt.ControlFile->state = DB_SHUTDOWNED_IN_RECOVERY;
            t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
            UpdateControlFile();
            LWLockRelease(ControlFileLock);
        }
        LWLockRelease(CheckpointLock);
        smgrsync_with_absorption();
        gstrace_exit(GS_TRC_ID_CreateRestartPoint);
        return false;
    }
    PrintCkpXctlControlFile(t_thrd.shemem_ptr_cxt.ControlFile->checkPoint,
                            &(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy), lastCheckPointRecPtr, &lastCheckPoint,
                            t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint, "CreateRestartPoint");

#ifdef ENABLE_MOT
    CallCheckpointCallback(EVENT_CHECKPOINT_CREATE_SNAPSHOT, 0);
#endif

    /*
     * Update the shared RedoRecPtr so that the startup process can calculate
     * the number of segments replayed since last restartpoint, and request a
     * restartpoint if it exceeds checkpoint_segments.
     *
     * Like in CreateCheckPoint(), hold off insertions to update it, although
     * during recovery this is just pro forma, because no WAL insertions are
     * happening.
     */
    StartSuspendWalInsert(&lastlrc);
    xlogctl->Insert.RedoRecPtr = lastCheckPoint.redo;
#ifdef ENABLE_MOT
    CallCheckpointCallback(EVENT_CHECKPOINT_SNAPSHOT_READY, lastCheckPointRecPtr);
#endif
    StopSuspendWalInsert(lastlrc);

    /* Also update the info_lck-protected copy */
    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->RedoRecPtr = lastCheckPoint.redo;
    SpinLockRelease(&xlogctl->info_lck);

    /*
     * Prepare to accumulate statistics.
     *
     * Note: because it is possible for log_checkpoints to change while a
     * checkpoint proceeds, we always accumulate stats, even if
     * log_checkpoints is currently off.
     */
    errorno = memset_s(t_thrd.xlog_cxt.CheckpointStats, sizeof(CheckpointStatsData), 0, sizeof(CheckpointStatsData));
    securec_check(errorno, "", "");

    t_thrd.xlog_cxt.CheckpointStats->ckpt_start_t = GetCurrentTimestamp();

    if (u_sess->attr.attr_common.log_checkpoints) {
        LogCheckpointStart((unsigned int)flags, true);
    }

    if (ENABLE_INCRE_CKPT && doFullCkpt) {
        g_instance.ckpt_cxt_ctl->full_ckpt_redo_ptr = lastCheckPoint.redo;
        g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc = get_dirty_page_queue_tail();
        pg_write_barrier();
        if (get_dirty_page_num() > 0) {
            g_instance.ckpt_cxt_ctl->flush_all_dirty_page = true;
        }
        ereport(LOG, (errmsg("CreateRestartPoint, need flush %ld pages.", get_dirty_page_num())));
    } else if (ENABLE_INCRE_CKPT) {
        g_instance.ckpt_cxt_ctl->full_ckpt_redo_ptr = lastCheckPoint.redo;
        (void)LWLockAcquire(g_instance.ckpt_cxt_ctl->prune_queue_lock, LW_EXCLUSIVE);
        g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc = get_loc_for_lsn(lastCheckPoint.redo);
        LWLockRelease(g_instance.ckpt_cxt_ctl->prune_queue_lock);
        pg_write_barrier();

        uint64 head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
        int64 need_flush_num = g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc > head ?
            g_instance.ckpt_cxt_ctl->full_ckpt_expected_flush_loc - head : 0;

        if (need_flush_num > 0) {
            g_instance.ckpt_cxt_ctl->flush_all_dirty_page = true;
        }
        ereport(LOG, (errmsg("CreateRestartPoint, need flush %ld pages", need_flush_num)));
    }

    CheckPointGuts(lastCheckPoint.redo, flags, true);

#ifdef ENABLE_MOT
    CallCheckpointCallback(EVENT_CHECKPOINT_BEGIN_CHECKPOINT, 0);
#endif

    /*
     * Select point at which we can truncate the xlog, which we base on the
     * prior checkpoint's earliest info.
     */
    XLByteToSeg(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, _logSegNo);
    if (ENABLE_INCRE_CKPT) {
        XLogRecPtr MinRecLSN = ckpt_get_min_rec_lsn();
        if (!XLogRecPtrIsInvalid(MinRecLSN) && XLByteLT(MinRecLSN, lastCheckPoint.redo)) {
            ereport(WARNING, (errmsg("current dirty page list head recLSN %08X/%08X smaller than redo lsn %08X/%08X",
                                   (uint32)(MinRecLSN >> XLOG_LSN_SWAP), (uint32)MinRecLSN,
                                   (uint32)(lastCheckPoint.redo >> XLOG_LSN_SWAP),
                                   (uint32)lastCheckPoint.redo)));
            LWLockRelease(CheckpointLock);
            smgrsync_with_absorption();
            gstrace_exit(GS_TRC_ID_CreateRestartPoint);
            return false;
        }
    }

    /*
     * Update pg_control, using current time.  Check that it still shows
     * IN_ARCHIVE_RECOVERY state and an older checkpoint, else do nothing;
     * this is a quick hack to make sure nothing really bad happens if somehow
     * we get here after the end-of-recovery checkpoint.
     */
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    if (t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_ARCHIVE_RECOVERY &&
        XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo, lastCheckPoint.redo)) {
        t_thrd.shemem_ptr_cxt.ControlFile->prevCheckPoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
        t_thrd.shemem_ptr_cxt.ControlFile->checkPoint = lastCheckPointRecPtr;
        t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy = lastCheckPoint;
        t_thrd.shemem_ptr_cxt.ControlFile->time = (pg_time_t)time(NULL);
        g_instance.ckpt_cxt_ctl->ckpt_current_redo_point = lastCheckPoint.redo;
        if ((unsigned int)flags & CHECKPOINT_IS_SHUTDOWN) {
            t_thrd.shemem_ptr_cxt.ControlFile->state = DB_SHUTDOWNED_IN_RECOVERY;
        }
        UpdateControlFile();
    }
    LWLockRelease(ControlFileLock);

    /* wake up the cbmwriter */
    if (g_instance.proc_base->cbmwriterLatch) {
        SetLatch(g_instance.proc_base->cbmwriterLatch);
    }

    /*
     * Delete old log files (those no longer needed even for previous
     * checkpoint/restartpoint) to prevent the disk holding the xlog from
     * growing full.
     */
    if (_logSegNo) {
        XLogRecPtr endptr;

        /* Get the current (or recent) end of xlog */
        endptr = GetStandbyFlushRecPtr(NULL);

        /* The third param is not used */
        KeepLogSeg(endptr, &_logSegNo, InvalidXLogRecPtr);
        _logSegNo--;

        /*
         * Save one more segment to handle the corner case. For example, standby has replayed to
         * the end to one segment file (000000010000002400000078) and the LSN is exactly 24/79000000.
         * The corresponding slot on primary is 24/79000000. As a result, if standby restart to
         * request xlog stream from the lastest valid record (23/xxxxxxx), cannot find on primary.
         */
        if (_logSegNo > 0) {
            _logSegNo--;
        }

        /*
         * Update ThisTimeLineID to the timeline we're currently replaying,
         * so that we install any recycled segments on that timeline.
         *
         * There is no guarantee that the WAL segments will be useful on the
         * current timeline; if recovery proceeds to a new timeline right
         * after this, the pre-allocated WAL segments on this timeline will
         * not be used, and will go wasted until recycled on the next
         * restartpoint. We'll live with that.
         */
        SpinLockAcquire(&xlogctl->info_lck);
        t_thrd.xlog_cxt.ThisTimeLineID = t_thrd.shemem_ptr_cxt.XLogCtl->lastCheckPoint.ThisTimeLineID;
        SpinLockRelease(&xlogctl->info_lck);

        RemoveOldXlogFiles(_logSegNo, endptr);

        /*
         * Make more log segments if needed.  (Do this after recycling old log
         * segments, since that may supply some of the needed files.)
         */
        PreallocXlogFiles(endptr);
    }

    /*
     * Truncate pg_csnlog if possible.  We can throw away all data before
     * the oldest XMIN of any running transaction.  No future transaction will
     * attempt to reference any pg_csnlog entry older than that (see Asserts
     * in csnlog.c).  When hot standby is disabled, though, we mustn't do
     * this because StartupCSNLOG hasn't been called yet.
     */
    if (g_instance.attr.attr_storage.EnableHotStandby) {
        pg_time_t now;
        int elapsed_secs;
        now = (pg_time_t)time(NULL);
        elapsed_secs = now - t_thrd.checkpoint_cxt.last_truncate_log_time;
        /*
         * Reduce the frequency of trucate CSN log to avoid the probability of lock contention.
         * Incremental chekpoint does not require frequent truncate of csnlog.
         */
        if (!ENABLE_INCRE_CKPT ||
            elapsed_secs >= u_sess->attr.attr_storage.fullCheckPointTimeout) {
            TransactionId globalXmin = InvalidTransactionId;
            (void)GetOldestActiveTransactionId(&globalXmin);
            TransactionId cutoffXid = GetOldestXmin(NULL);
            if (TransactionIdIsNormal(globalXmin) && TransactionIdPrecedes(globalXmin, cutoffXid)) {
                cutoffXid = globalXmin;
            }
            TruncateCSNLOG(cutoffXid);
            t_thrd.checkpoint_cxt.last_truncate_log_time = now;
        }
    }

    /* Real work is done, but log and update before releasing lock. */
    LogCheckpointEnd(true);

    xtime = GetLatestXTime();
    ereport((u_sess->attr.attr_common.log_checkpoints ? LOG : DEBUG2),
            (errmsg("recovery restart point at %X/%X", (uint32)(lastCheckPoint.redo >> 32),
                    (uint32)lastCheckPoint.redo),
             xtime ? errdetail("last completed transaction was at log time %s", timestamptz_to_str(xtime)) : 0));

    LWLockRelease(CheckpointLock);

    /*
     * Finally, execute archive_cleanup_command, if any.
     */
    if (t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand[0]) {
        ExecuteRecoveryCommand(t_thrd.shemem_ptr_cxt.XLogCtl->archiveCleanupCommand, "archive_cleanup_command", false);
    }

    gstrace_exit(GS_TRC_ID_CreateRestartPoint);
    return true;
}

/*
 * Retreat *logSegNo to the last segment that we need to retain because of
 * either wal_keep_segments or replication slots.
 *
 * This is calculated by subtracting wal_keep_segments from the given xlog
 * location, recptr and by making sure that that result is below the
 * requirement of replication slots.
 */
static void KeepLogSeg(XLogRecPtr recptr, XLogSegNo *logSegNo, XLogRecPtr curInsert)
{
    /* segno is the wal file start index that should be keep */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogSegNo segno;
    XLogRecPtr keep;
    XLogRecPtr xlogcopystartptr;
    ReplicationSlotState repl_slot_state;

    gstrace_entry(GS_TRC_ID_KeepLogSeg);
    XLByteToSeg(recptr, segno);

    /* avoid underflow, don't go below 1 */
    if (segno <= (uint64)(uint32)u_sess->attr.attr_storage.wal_keep_segments) {
        /* segno = 1 show all file should be keep */
        ereport(LOG, (errmsg("keep all the xlog segments, because current segno = %lu, "
            "less than wal_keep_segments = %d", segno, u_sess->attr.attr_storage.wal_keep_segments)));
        segno = 1;

    } else {
        segno = segno - (uint32)u_sess->attr.attr_storage.wal_keep_segments;
    }

    ReplicationSlotsComputeRequiredLSN(&repl_slot_state);
    keep = repl_slot_state.min_required;
    ReplicationSlotReportRestartLSN();

    /* then check whether slots limit removal further */
    if (g_instance.attr.attr_storage.max_replication_slots > 0 && !XLByteEQ(keep, InvalidXLogRecPtr)) {
        XLogSegNo slotSegNo;

        XLByteToSeg(keep, slotSegNo);

        if (slotSegNo <= 0) {
            /* segno = 1 show all file should be keep */
            segno = 1;
            ereport(LOG, (errmsg("keep all the xlog segments, because the minimal replication slot segno "
                "is less than or equal to zero")));
        } else if (slotSegNo < segno) {
            segno = slotSegNo;
        }
    }

    LWLockAcquire(FullBuildXlogCopyStartPtrLock, LW_SHARED);
    xlogcopystartptr = XlogCopyStartPtr;
    LWLockRelease(FullBuildXlogCopyStartPtrLock);

    if (!XLByteEQ(xlogcopystartptr, InvalidXLogRecPtr)) {
        XLogSegNo slotSegNo;
        XLByteToSeg(xlogcopystartptr, slotSegNo);

        if (slotSegNo <= 0) {
            /* segno = 1 show all file should be keep */
            segno = 1;
            ereport(LOG, (errmsg("keep all the xlog segments, because there is a full-build task in the backend, "
                "and start segno is less than or equal to zero")));
        } else if (slotSegNo < segno) {
            segno = slotSegNo;
        }
    }

    load_server_mode();

    /*
     * In primary mode, we should do additional check.
     *
     * 1.When dn is in build(full or incremental), just keep log.
     * 2.When not all standbys are alive in multi standby mode, further check enable_xlog_prune.
     * If false, just keep log; otherwise, keep log only if walSize less than config.
     * 3.When standby is not alive in dummy standby mode, just keep log.
     * 4.Notice the users if slot is invalid
     */
    if (t_thrd.xlog_cxt.server_mode == PRIMARY_MODE &&
        u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH &&
        t_thrd.syncrep_cxt.SyncRepConfig != NULL) {
        if (WalSndInProgress(SNDROLE_PRIMARY_BUILDSTANDBY) ||
            pg_atomic_read_u32(&g_instance.comm_cxt.current_gsrewind_count) > 0) {
            /* segno = 1 show all file should be keep */
            segno = 1;
            ereport(LOG, (errmsg("keep all the xlog segments, because there is a build task in the backend, "
                "and gs_rewind count is more than zero")));
        } else if (IS_DN_MULTI_STANDYS_MODE() && !WalSndAllInProgress((SNDROLE_PRIMARY_STANDBY))) {
            /* wal size can be approximated as two parts: wal not recycled and wal preallocated */
            uint64 walSize = (curInsert - xlogctl->lastRemovedSegNo * XLOG_SEG_SIZE) +
                             (uint64)XLOGfileslop * XLOG_SEG_SIZE;
            /* the unit of max_size_for_xlog_prune is KB */
            uint64 maxKeepSize = ((uint64)u_sess->attr.attr_storage.max_size_for_xlog_prune << 10);
            bool isWalSizeLTConfig = walSize < maxKeepSize;
            if (!u_sess->attr.attr_storage.enable_xlog_prune) {
                /* segno = 1 show all file should be keep */
                segno = 1;
                ereport(LOG, (errmsg("keep all the xlog segments, because the value of enable_xlog_prune(GUC) "
                    "is false")));
            } else if (isWalSizeLTConfig) {
                /* segno = 1 show all file should be keep */
                segno = 1;
                ereport(LOG, (errmsg("keep all the xlog segments, because current wal size = %lu(KB), "
                    "less than max_size_for_xlog_prune = %lu(KB)", walSize, maxKeepSize)));
            }
        } else if (IS_DN_DUMMY_STANDYS_MODE() && !WalSndInProgress(SNDROLE_PRIMARY_STANDBY)) {
            /* segno = 1 show all file should be keep */
            segno = 1;
            ereport(LOG, (errmsg("keep all the xlog segments, because not all the wal senders are "
                "in the role PRIMARY_STANDBY")));
        }
        if (XLByteEQ(keep, InvalidXLogRecPtr) && repl_slot_state.exist_in_use) {
            /* there are slots and lsn is invalid, we keep it */
            segno = 1;
            ereport(WARNING, (errmsg("invalid replication slot recptr"),
                              errhint("Check slot configuration or setup standby/secondary")));
        }
    }

    if (u_sess->attr.attr_storage.enable_cbm_tracking) {
        XLogSegNo cbm_tracked_segno;
        XLByteToSeg(GetCBMTrackedLSN(), cbm_tracked_segno);

        if (cbm_tracked_segno < segno && cbm_tracked_segno > 0) {
            segno = cbm_tracked_segno;
        }
    }
    if (t_thrd.xlog_cxt.server_mode != PRIMARY_MODE && t_thrd.xlog_cxt.server_mode != NORMAL_MODE) {
        LWLockAcquire(XlogRemoveSegLock, LW_SHARED);
        if (XlogRemoveSegPrimary < segno && XlogRemoveSegPrimary > 0) {
            segno = XlogRemoveSegPrimary;
        }
        LWLockRelease(XlogRemoveSegLock);
    }
    /* don't delete WAL segments newer than the calculated segment */
    if (segno < *logSegNo && segno > 0) {
        *logSegNo = segno;
    }
    gstrace_exit(GS_TRC_ID_KeepLogSeg);
}

/*
 * Write a NEXTOID log record
 */
void XLogPutNextOid(Oid nextOid)
{
    XLogBeginInsert();
    XLogRegisterData((char *)(&nextOid), sizeof(Oid));

    (void)XLogInsert(RM_XLOG_ID, XLOG_NEXTOID);

    /*
     * We need not flush the NEXTOID record immediately, because any of the
     * just-allocated OIDs could only reach disk as part of a tuple insert or
     * update that would have its own XLOG record that must follow the NEXTOID
     * record.	Therefore, the standard buffer LSN interlock applied to those
     * records will ensure no such OID reaches disk before the NEXTOID record
     * does.
     *
     * Note, however, that the above statement only covers state "within" the
     * database.  When we use a generated OID as a file or directory name, we
     * are in a sense violating the basic WAL rule, because that filesystem
     * change may reach disk before the NEXTOID WAL record does.  The impact
     * of this is that if a database crash occurs immediately afterward, we
     * might after restart re-generate the same OID and find that it conflicts
     * with the leftover file or directory.  But since for safety's sake we
     * always loop until finding a nonconflicting filename, this poses no real
     * problem in practice. See pgsql-hackers discussion 27-Sep-2006.
     */
}

/*
 * Write an XLOG SWITCH record.
 *
 * Here we just blindly issue an XLogInsert request for the record.
 * All the magic happens inside XLogInsert.
 *
 * The return value is either the end+1 address of the switch record,
 * or the end+1 address of the prior segment if we did not need to
 * write a switch record because we are already at segment start.
 */
XLogRecPtr RequestXLogSwitch(void)
{
    /* XLOG SWITCH has no data */
    XLogBeginInsert();

    return XLogInsert(RM_XLOG_ID, XLOG_SWITCH);
}

/*
 * Write a RESTORE POINT record
 */
XLogRecPtr XLogRestorePoint(const char *rpName)
{
    XLogRecPtr RecPtr;
    xl_restore_point xlrec;
    errno_t retcode;

    xlrec.rp_time = GetCurrentTimestamp();
    retcode = strncpy_s(xlrec.rp_name, MAXFNAMELEN, rpName, strlen(rpName));
    securec_check(retcode, "\0", "\0");

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, sizeof(xl_restore_point));

    RecPtr = XLogInsert(RM_XLOG_ID, XLOG_RESTORE_POINT);

    ereport(LOG, (errmsg("restore point \"%s\" created at %X/%X", rpName, (uint32)(RecPtr >> 32), (uint32)RecPtr)));

    return RecPtr;
}

/*
 * Check if any of the GUC parameters that are critical for hot standby
 * have changed, and update the value in pg_control file if necessary.
 */
static void XLogReportParameters(void)
{
    if (g_instance.attr.attr_storage.wal_level != t_thrd.shemem_ptr_cxt.ControlFile->wal_level ||
        g_instance.shmem_cxt.MaxConnections != t_thrd.shemem_ptr_cxt.ControlFile->MaxConnections ||
        g_instance.attr.attr_storage.max_prepared_xacts != t_thrd.shemem_ptr_cxt.ControlFile->max_prepared_xacts ||
        g_instance.attr.attr_storage.max_locks_per_xact != t_thrd.shemem_ptr_cxt.ControlFile->max_locks_per_xact) {
        /*
         * The change in number of backend slots doesn't need to be WAL-logged
         * if archiving is not enabled, as you can't start archive recovery
         * with wal_level=minimal anyway. We don't really care about the
         * values in pg_control either if wal_level=minimal, but seems better
         * to keep them up-to-date to avoid confusion.
         */
        if (g_instance.attr.attr_storage.wal_level != t_thrd.shemem_ptr_cxt.ControlFile->wal_level || XLogIsNeeded()) {
            xl_parameter_change xlrec;
            XLogRecPtr recptr;

            xlrec.MaxConnections = g_instance.shmem_cxt.MaxConnections;
            xlrec.max_prepared_xacts = g_instance.attr.attr_storage.max_prepared_xacts;
            xlrec.max_locks_per_xact = g_instance.attr.attr_storage.max_locks_per_xact;
            xlrec.wal_level = g_instance.attr.attr_storage.wal_level;

            XLogBeginInsert();
            XLogRegisterData((char *)&xlrec, sizeof(xlrec));

            recptr = XLogInsert(RM_XLOG_ID, XLOG_PARAMETER_CHANGE);
            XLogWaitFlush(recptr);
        }

        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        t_thrd.shemem_ptr_cxt.ControlFile->MaxConnections = g_instance.shmem_cxt.MaxConnections;
        t_thrd.shemem_ptr_cxt.ControlFile->max_prepared_xacts = g_instance.attr.attr_storage.max_prepared_xacts;
        t_thrd.shemem_ptr_cxt.ControlFile->max_locks_per_xact = g_instance.attr.attr_storage.max_locks_per_xact;
        t_thrd.shemem_ptr_cxt.ControlFile->wal_level = g_instance.attr.attr_storage.wal_level;
        UpdateControlFile();
        LWLockRelease(ControlFileLock);
    }
}

/*
 * Update full_page_writes in shared memory, and write an
 * XLOG_FPW_CHANGE record if necessary.
 *
 * Note: this function assumes there is no other process running
 * concurrently that could update it.
 */
void UpdateFullPageWrites(void)
{
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    int32 lastlrc = 0;

    /*
     * Do nothing if full_page_writes has not been changed.
     *
     * It's safe to check the shared full_page_writes without the lock,
     * because we assume that there is no concurrently running process which
     * can update it.
     */
    if (ENABLE_INCRE_CKPT) {
        u_sess->attr.attr_storage.fullPageWrites = false;
    }
    if (u_sess->attr.attr_storage.fullPageWrites == Insert->fullPageWrites) {
        return;
    }

    START_CRIT_SECTION();

    /*
     * It's always safe to take full page images, even when not strictly
     * required, but not the other round. So if we're setting full_page_writes
     * to true, first set it true and then write the WAL record. If we're
     * setting it to false, first write the WAL record and then set the global
     * flag.
     */
    if (u_sess->attr.attr_storage.fullPageWrites) {
        StartSuspendWalInsert(&lastlrc);
        Insert->fullPageWrites = true;
        StopSuspendWalInsert(lastlrc);
    }

    /*
     * Write an XLOG_FPW_CHANGE record. This allows us to keep track of
     * full_page_writes during archive recovery, if required.
     */
    if (XLogStandbyInfoActive() && !RecoveryInProgress()) {
        XLogBeginInsert();
        XLogRegisterData((char *)(&u_sess->attr.attr_storage.fullPageWrites), sizeof(bool));

        (void)XLogInsert(RM_XLOG_ID, XLOG_FPW_CHANGE);
    }

    if (!u_sess->attr.attr_storage.fullPageWrites) {
        StartSuspendWalInsert(&lastlrc);
        Insert->fullPageWrites = false;
        StopSuspendWalInsert(lastlrc);
    }
    END_CRIT_SECTION();
}

// PR:
bool IsCheckPoint(XLogReaderState *record)
{
    uint8 info;
    RmgrId rmid;

    info = XLogRecGetInfo(record) & (~XLR_INFO_MASK);
    rmid = XLogRecGetRmid(record);

    return rmid == RM_XLOG_ID && (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE);
}

bool IsCheckPoint(const XLogRecParseState *parseState)
{
    uint8 info = parseState->blockparse.blockhead.xl_info & (~XLR_INFO_MASK);
    RmgrId rmid = parseState->blockparse.blockhead.xl_rmid;

    return rmid == RM_XLOG_ID && (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE);
}


bool HasTimelineUpdate(XLogReaderState *record, bool bOld)
{
    uint8 info;
    int rmid;

    info = XLogRecGetInfo(record) & (~XLR_INFO_MASK);
    rmid = XLogRecGetRmid(record);

    return (rmid == RM_XLOG_ID && info == XLOG_CHECKPOINT_SHUTDOWN);
}

void UpdateTimeline(CheckPoint *checkPoint)
{
    if (checkPoint->ThisTimeLineID != t_thrd.xlog_cxt.ThisTimeLineID) {
        if (checkPoint->ThisTimeLineID < t_thrd.xlog_cxt.ThisTimeLineID ||
            !list_member_int(t_thrd.xlog_cxt.expectedTLIs, (int)checkPoint->ThisTimeLineID))
            ereport(PANIC, (errmsg("unexpected timeline ID %u (after %u) in checkpoint record",
                                   checkPoint->ThisTimeLineID, t_thrd.xlog_cxt.ThisTimeLineID)));
        /* Following WAL records should be run with new TLI */
        t_thrd.xlog_cxt.ThisTimeLineID = checkPoint->ThisTimeLineID;
    }
}

/**
 * @Description: Assign Checkpoint data from the old version to the new version
 * @in: record, xlog record
 */
CheckPoint update_checkpoint(XLogReaderState * record)
{
    CheckPointOld checkPointOld;
    CheckPoint checkPoint;
    errno_t rc;

    rc = memcpy_s(&checkPointOld, sizeof(CheckPointOld), XLogRecGetData(record), sizeof(CheckPointOld));
    securec_check(rc, "", "");
    checkPoint.redo = XLogRecPtrSwap(checkPointOld.redo);
    checkPoint.ThisTimeLineID = checkPointOld.ThisTimeLineID;
    checkPoint.fullPageWrites = checkPointOld.fullPageWrites;
    checkPoint.nextXid = checkPointOld.nextXid;
    checkPoint.nextOid = checkPointOld.nextOid;
    checkPoint.nextMulti = checkPointOld.nextMulti;
    checkPoint.nextMultiOffset = checkPointOld.nextMultiOffset;

    if (checkPointOld.oldestXid > checkPointOld.nextXid &&
        TransactionIdLogicallyPrecedes(checkPointOld.oldestXid, FirstNormalTransactionId)) {
        checkPoint.oldestXid = FirstNormalTransactionId;
    } else {
        checkPoint.oldestXid = checkPointOld.oldestXid;
    }
    checkPoint.oldestXidDB = checkPointOld.oldestXidDB;
    checkPoint.time = checkPointOld.time;
    checkPoint.oldestActiveXid = checkPointOld.oldestActiveXid;
    checkPoint.remove_seg = InvalidXLogSegPtr;
    return checkPoint;
}
/*
 * XLOG resource manager's routines
 *
 * Definitions of info values are in include/catalog/pg_control.h, though
 * not all record types are related to control file updates.
 */
void xlog_redo(XLogReaderState *record)
{
    uint8 info;

    /* Support  redo old version xlog during upgrade (Just the chekpoint and xlog switch) */
    if (t_thrd.xlog_cxt.redo_oldversion_xlog) {
        info = (((XLogRecordOld *)record->decoded_record)->xl_info) & ~XLR_INFO_MASK;
    } else {
        info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    }

    XLogRecPtr lsn = record->EndRecPtr;

    /* in XLOG rmgr, backup blocks are only used by XLOG_FPI records */
    Assert(info == XLOG_FPI || info == XLOG_FPI_FOR_HINT || !XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_NEXTOID) {
        Oid nextOid;
        errno_t rc;

        /*
         * We used to try to take the maximum of ShmemVariableCache->nextOid
         * and the recorded nextOid, but that fails if the OID counter wraps
         * around.  Since no OID allocation should be happening during replay
         * anyway, better to just believe the record exactly.  We still take
         * OidGenLock while setting the variable, just in case.
         */
        rc = memcpy_s(&nextOid, sizeof(Oid), XLogRecGetData(record), sizeof(Oid));
        securec_check(rc, "", "");

        LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
        t_thrd.xact_cxt.ShmemVariableCache->nextOid = nextOid;
        t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;
        LWLockRelease(OidGenLock);
    } else if (info == XLOG_CHECKPOINT_SHUTDOWN) {
        CheckPoint checkPoint;
        errno_t rc;

        /* The checkpoint structure has changed to a new version, needs to be updated to the new version */
        if (t_thrd.xlog_cxt.redo_oldversion_xlog) {
            checkPoint = update_checkpoint(record);
        } else {
            Assert(XLogRecGetDataLen(record) >= sizeof(checkPoint));
            rc = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(record), sizeof(CheckPoint));
            securec_check(rc, "", "");
        }
        /* In a SHUTDOWN checkpoint, believe the counters exactly */
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, checkPoint.nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = checkPoint.nextXid;
        }
        LWLockRelease(XidGenLock);
        LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
        t_thrd.xact_cxt.ShmemVariableCache->nextOid = checkPoint.nextOid;
        t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;
        LWLockRelease(OidGenLock);
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(DN_SLAVE_CHECKPOINT_KEEPXLOG, default_error_emit)) {
            ereport(get_distribute_test_param()->elevel,
                    (errmsg("default_error_emit xlog_redo time:%ds, stub_name:%s",
                            get_distribute_test_param()->sleep_time, get_distribute_test_param()->test_stub_name)));
            checkPoint.remove_seg = 1;
        }
#endif
        LWLockAcquire(XlogRemoveSegLock, LW_EXCLUSIVE);
        ereport(DEBUG1, (errmsg("redo checkpoint xlog change remove seg from %X/%X to %X/%X",
                                (uint32)(XlogRemoveSegPrimary >> 32), (uint32)XlogRemoveSegPrimary,
                                (uint32)(checkPoint.remove_seg >> 32), (uint32)checkPoint.remove_seg)));
        XlogRemoveSegPrimary = checkPoint.remove_seg;
        LWLockRelease(XlogRemoveSegLock);
        MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);

        /*
         * If we see a shutdown checkpoint while waiting for an end-of-backup
         * record, the backup was canceled and the end-of-backup record will
         * never arrive.
         */
        if (t_thrd.xlog_cxt.ArchiveRestoreRequested &&
            !XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint) &&
            XLogRecPtrIsInvalid(t_thrd.shemem_ptr_cxt.ControlFile->backupEndPoint)) {
            if (IsRoachRestore()) {
                ereport(WARNING,
                        (errmsg("exist a shutdown log during xlog redo due to resume backup appear switchover.")));
            } else {
                ereport(PANIC, (errmsg("online backup was canceled, recovery cannot continue")));
            }
        }
        /*
         * If we see a shutdown checkpoint, we know that nothing was running
         * on the master at this point. So fake-up an empty running-xacts
         * record and use that here and now. Recover additional standby state
         * for prepared transactions.
         */
        if (t_thrd.xlog_cxt.standbyState >= STANDBY_INITIALIZED) {
            TransactionId *xids = NULL;
            int nxids = 0;
            TransactionId oldestActiveXID = InvalidTransactionId;
            TransactionId latestCompletedXid = InvalidTransactionId;
            RunningTransactionsData running;

            oldestActiveXID = PrescanPreparedTransactions(&xids, &nxids);

            /*
             * Construct a RunningTransactions snapshot representing a shut
             * down server, with only prepared transactions still alive. We're
             * never overflowed at this point because all subxids are listed
             * with their parent prepared transactions.
             */
            running.nextXid = checkPoint.nextXid;
            running.oldestRunningXid = oldestActiveXID;
            latestCompletedXid = checkPoint.nextXid;
            TransactionIdRetreat(latestCompletedXid);
            Assert(TransactionIdIsNormal(latestCompletedXid));
            running.latestCompletedXid = latestCompletedXid;

            ProcArrayApplyRecoveryInfo(&running);

            StandbyRecoverPreparedTransactions();
        }

        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock);

        /* Update shared-memory copy of checkpoint XID/epoch */
        {
            /* use volatile pointer to prevent code rearrangement */
            SpinLockAcquire(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
            t_thrd.shemem_ptr_cxt.XLogCtl->ckptXid = checkPoint.nextXid;
            SpinLockRelease(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
        }

        /*
         * TLI may change in a shutdown checkpoint, but it shouldn't decrease
         */
        if (checkPoint.ThisTimeLineID != t_thrd.xlog_cxt.ThisTimeLineID) {
            if (checkPoint.ThisTimeLineID < t_thrd.xlog_cxt.ThisTimeLineID ||
                !list_member_int(t_thrd.xlog_cxt.expectedTLIs, (int)checkPoint.ThisTimeLineID)) {
                ereport(PANIC, (errmsg("unexpected timeline ID %u (after %u) in checkpoint record",
                                       checkPoint.ThisTimeLineID, t_thrd.xlog_cxt.ThisTimeLineID)));
            }
            /* Following WAL records should be run with new TLI */
            t_thrd.xlog_cxt.ThisTimeLineID = checkPoint.ThisTimeLineID;
        }

        RecoveryRestartPoint(checkPoint, record->ReadRecPtr);
    } else if (info == XLOG_CHECKPOINT_ONLINE) {
        CheckPoint checkPoint;
        errno_t rc;

        /* The checkpoint structure has changed to a new version, needs to be updated to the new version */
        if (t_thrd.xlog_cxt.redo_oldversion_xlog) {
            checkPoint = update_checkpoint(record);
        } else {
            Assert(XLogRecGetDataLen(record) >= sizeof(checkPoint));
            rc = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(record), sizeof(CheckPoint));
            securec_check(rc, "", "");
        }
        /* In an ONLINE checkpoint, treat the XID counter as a minimum */
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, checkPoint.nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = checkPoint.nextXid;
        }
        LWLockRelease(XidGenLock);
        /* ... but still treat OID counter as exact */
        LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
        t_thrd.xact_cxt.ShmemVariableCache->nextOid = checkPoint.nextOid;
        t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;

        LWLockRelease(OidGenLock);
        MultiXactAdvanceNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->oldestXid, checkPoint.oldestXid)) {
            SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
        }

        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock);
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(DN_SLAVE_CHECKPOINT_KEEPXLOG, default_error_emit)) {
            ereport(get_distribute_test_param()->elevel,
                    (errmsg("default_error_emit xlog_redo time:%ds, stub_name:%s",
                            get_distribute_test_param()->sleep_time, get_distribute_test_param()->test_stub_name)));
            checkPoint.remove_seg = 1;
        }
#endif
        LWLockAcquire(XlogRemoveSegLock, LW_EXCLUSIVE);
        ereport(DEBUG1, (errmsg("redo checkpoint xlog change remove seg from %X/%X to %X/%X",
                                (uint32)(XlogRemoveSegPrimary >> 32), (uint32)XlogRemoveSegPrimary,
                                (uint32)(checkPoint.remove_seg >> 32), (uint32)checkPoint.remove_seg)));
        XlogRemoveSegPrimary = checkPoint.remove_seg;
        LWLockRelease(XlogRemoveSegLock);
        /* Update shared-memory copy of checkpoint XID/epoch */
        {
            /* use volatile pointer to prevent code rearrangement */
            volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

            SpinLockAcquire(&xlogctl->info_lck);
            xlogctl->ckptXid = checkPoint.nextXid;
            SpinLockRelease(&xlogctl->info_lck);
        }

        /* TLI should not change in an on-line checkpoint */
        if (checkPoint.ThisTimeLineID != t_thrd.xlog_cxt.ThisTimeLineID) {
            ereport(PANIC, (errmsg("unexpected timeline ID %u (should be %u) in checkpoint record",
                                   checkPoint.ThisTimeLineID, t_thrd.xlog_cxt.ThisTimeLineID)));
        }

        RecoveryRestartPoint(checkPoint, record->ReadRecPtr);
    } else if (info == XLOG_NOOP) {
        /* nothing to do here */
    } else if (info == XLOG_SWITCH) {
        /* nothing to do here */
    } else if (info == XLOG_RESTORE_POINT) {
        /* nothing to do here */
    } else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT) {
        RedoBufferInfo buffer;

        /*
         * Full-page image (FPI) records contain nothing else but a backup
         * block. The block reference must include a full-page image -
         * otherwise there would be no point in this record.
         *
         * No recovery conflicts are generated by these generic records - if a
         * resource manager needs to generate conflicts, it has to define a
         * separate WAL record type and redo routine.
         *
         * XLOG_FPI_FOR_HINT records are generated when a page needs to be
         * WAL- logged because of a hint bit update. They are only generated
         * when checksums are enabled. There is no difference in handling
         * XLOG_FPI and XLOG_FPI_FOR_HINT records, they use a different info
         * code just to distinguish them for statistics purposes.
         */
        if (XLogReadBufferForRedo(record, 0, &buffer) != BLK_RESTORED) {
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                            errmsg("unexpected XLogReadBufferForRedo result when restoring backup block")));
        }
        UnlockReleaseBuffer(buffer.buf);
    } else if (info == XLOG_BACKUP_END) {
        XLogRecPtr startpoint;
        errno_t rc = EOK;

        rc = memcpy_s(&startpoint, sizeof(startpoint), XLogRecGetData(record), sizeof(startpoint));
        securec_check(rc, "", "");

        if (XLByteEQ(t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint, startpoint)) {
            /*
             * We have reached the end of base backup, the point where
             * pg_stop_backup() was done. The data on disk is now consistent.
             * Reset backupStartPoint, and update minRecoveryPoint to make
             * sure we don't allow starting up at an earlier point even if
             * recovery is stopped and restarted soon after this.
             */
            ereport(DEBUG1, (errmsg("end of backup reached")));

            LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

            if (XLByteLT(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint, lsn)) {
                t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = lsn;
            }
            rc = memset_s(&t_thrd.shemem_ptr_cxt.ControlFile->backupStartPoint, sizeof(XLogRecPtr), 0,
                          sizeof(XLogRecPtr));
            securec_check(rc, "\0", "\0");
            t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired = false;
            UpdateControlFile();

            LWLockRelease(ControlFileLock);
        }
    } else if (info == XLOG_PARAMETER_CHANGE) {
        xl_parameter_change xlrec;
        errno_t rc = EOK;

        /* Update our copy of the parameters in pg_control */
        rc = memcpy_s(&xlrec, sizeof(xl_parameter_change), XLogRecGetData(record), sizeof(xl_parameter_change));
        securec_check(rc, "", "");

        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        t_thrd.shemem_ptr_cxt.ControlFile->MaxConnections = xlrec.MaxConnections;
        t_thrd.shemem_ptr_cxt.ControlFile->max_prepared_xacts = xlrec.max_prepared_xacts;
        t_thrd.shemem_ptr_cxt.ControlFile->max_locks_per_xact = xlrec.max_locks_per_xact;
        t_thrd.shemem_ptr_cxt.ControlFile->wal_level = xlrec.wal_level;

        /*
         * Update minRecoveryPoint to ensure that if recovery is aborted, we
         * recover back up to this point before allowing hot standby again.
         * This is particularly important if wal_level was set to 'archive'
         * before, and is now 'hot_standby', to ensure you don't run queries
         * against the WAL preceding the wal_level change. Same applies to
         * decreasing max_* settings.
         */
        t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
        SetMinRecoverPointForStats(t_thrd.xlog_cxt.minRecoveryPoint);
        if ((t_thrd.xlog_cxt.minRecoveryPoint != 0) && XLByteLT(t_thrd.xlog_cxt.minRecoveryPoint, lsn)) {
            t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint = lsn;
        }

        UpdateControlFile();
        LWLockRelease(ControlFileLock);

        ereport(LOG, (errmsg("update ControlFile->minRecoveryPoint to %X/%X for xlog parameter change",
                             (uint32)((t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint) >> 32),
                             (uint32)(t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint))));

        /* Check to see if any changes to max_connections give problems */
        CheckRequiredParameterValues(false);
    } else if (info == XLOG_FPW_CHANGE) {
        /* use volatile pointer to prevent code rearrangement */
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
        bool fpw = false;
        errno_t rc = EOK;

        rc = memcpy_s(&fpw, sizeof(bool), XLogRecGetData(record), sizeof(bool));
        securec_check(rc, "", "");

        /*
         * Update the LSN of the last replayed XLOG_FPW_CHANGE record so that
         * do_pg_start_backup() and do_pg_stop_backup() can check whether
         * full_page_writes has been disabled during online backup.
         */
        if (!fpw) {
            SpinLockAcquire(&xlogctl->info_lck);
            if (XLByteLT(xlogctl->lastFpwDisableRecPtr, record->ReadRecPtr)) {
                xlogctl->lastFpwDisableRecPtr = record->ReadRecPtr;
            }
            SpinLockRelease(&xlogctl->info_lck);
        }

        /* Keep track of full_page_writes */
        t_thrd.xlog_cxt.lastFullPageWrites = fpw;
    } else if (info == XLOG_DELAY_XLOG_RECYCLE) {
        bool delay = false;
        errno_t rc = EOK;
        rc = memcpy_s(&delay, sizeof(bool), XLogRecGetData(record), sizeof(bool));
        securec_check(rc, "", "");
        if (delay == true ) {
            enable_delay_xlog_recycle(true);
        } else {
            disable_delay_xlog_recycle(true);
        }
    }
}

#ifdef WAL_DEBUG

static void xlog_outrec(StringInfo buf, XLogReaderState *record)
{
    int block_id;

    appendStringInfo(buf, "prev %X/%X; xid %u", (uint32)(XLogRecGetPrev(record) >> 32), (uint32)XLogRecGetPrev(record),
                     XLogRecGetXid(record));

    appendStringInfo(buf, "; len %u", XLogRecGetDataLen(record));

    /* decode block references */
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blk;

        if (!XLogRecHasBlockRef(record, block_id)) {
            continue;
        }

        XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk);
        if (forknum != MAIN_FORKNUM) {
            appendStringInfo(buf, "; blkref #%u: rel %u/%u/%u, fork %u, blk %u", block_id, rnode.spcNode, rnode.dbNode,
                             rnode.relNode, forknum, blk);
        } else {
            appendStringInfo(buf, "; blkref #%u: rel %u/%u/%u, blk %u", block_id, rnode.spcNode, rnode.dbNode,
                             rnode.relNode, blk);
        }
        if (XLogRecHasBlockImage(record, block_id)) {
            appendStringInfo(buf, " FPW");
        }
    }
}
#endif /* WAL_DEBUG */

/*
 * Return the (possible) sync flag used for opening a file, depending on the
 * value of the GUC wal_sync_method.
 */
static int get_sync_bit(int method)
{
    uint32 o_direct_flag = 0;

    /* If fsync is disabled, never open in sync mode */
    if (!u_sess->attr.attr_storage.enableFsync) {
        return 0;
    }

    /*
     * Optimize writes by bypassing kernel cache with O_DIRECT when using
     * O_SYNC/O_FSYNC and O_DSYNC.	But only if archiving and streaming are
     * disabled, otherwise the archive command or walsender process will read
     * the WAL soon after writing it, which is guaranteed to cause a physical
     * read if we bypassed the kernel cache. We also skip the
     * posix_fadvise(POSIX_FADV_DONTNEED) call in XLogFileClose() for the same
     * reason.
     *
     * Never use O_DIRECT in walreceiver process for similar reasons; the WAL
     * written by walreceiver is normally read by the startup process soon
     * after its written. Also, walreceiver performs unaligned writes, which
     * don't work with O_DIRECT, so it is required for correctness too.
     */
    if (!XLogIsNeeded() && !AmWalReceiverProcess()) {
        o_direct_flag = PG_O_DIRECT;
    }

    switch (method) {
            /*
             * enum values for all sync options are defined even if they are
             * not supported on the current platform.  But if not, they are
             * not included in the enum option array, and therefore will never
             * be seen here.
             */
        case SYNC_METHOD_FSYNC:
        case SYNC_METHOD_FSYNC_WRITETHROUGH:
        case SYNC_METHOD_FDATASYNC:
            return 0;
#ifdef OPEN_SYNC_FLAG
        case SYNC_METHOD_OPEN:
            return OPEN_SYNC_FLAG | o_direct_flag;
#endif
#ifdef OPEN_DATASYNC_FLAG
        case SYNC_METHOD_OPEN_DSYNC:
            return OPEN_DATASYNC_FLAG | o_direct_flag;
#endif
        default:
            /* can't happen (unless we are out of sync with option array) */
            ereport(ERROR,
                    (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH), errmsg("unrecognized wal_sync_method: %d", method)));
            return 0; /* silence warning */
    }
}

/*
 * GUC support
 */
void assign_xlog_sync_method(int new_sync_method, void *extra)
{
    if (u_sess->attr.attr_storage.sync_method != new_sync_method) {
        /*
         * To ensure that no blocks escape unsynced, force an fsync on the
         * currently open log segment (if any).  Also, if the open flag is
         * changing, close the log file so it will be reopened (with new flag
         * bit) at next use.
         */
        if (t_thrd.xlog_cxt.openLogFile >= 0) {
            pgstat_report_waitevent(WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN);
            if (pg_fsync(t_thrd.xlog_cxt.openLogFile) != 0) {
                ereport(PANIC, (errcode_for_file_access(),
                                errmsg("could not fsync log segment %s: %m",
                                       XLogFileNameP(t_thrd.xlog_cxt.curFileTLI, t_thrd.xlog_cxt.readSegNo))));
            }
            pgstat_report_waitevent(WAIT_EVENT_END);
            if (get_sync_bit(u_sess->attr.attr_storage.sync_method) != get_sync_bit(new_sync_method)) {
                XLogFileClose();
            }
        }
    }
}

/*
 * Issue appropriate kind of fsync (if any) for an XLOG output file.
 *
 * 'fd' is a file descriptor for the XLOG file to be fsync'd.
 * 'log' and 'seg' are for error reporting purposes.
 */
void issue_xlog_fsync(int fd, XLogSegNo segno)
{
    switch (u_sess->attr.attr_storage.sync_method) {
        case SYNC_METHOD_FSYNC:
            if (pg_fsync_no_writethrough(fd) != 0) {
                ereport(PANIC,
                        (errcode_for_file_access(), errmsg("could not fsync log file %s: %m",
                                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
            }
            break;
#ifdef HAVE_FSYNC_WRITETHROUGH
        case SYNC_METHOD_FSYNC_WRITETHROUGH:
            if (pg_fsync_writethrough(fd) != 0) {
                ereport(PANIC,
                        (errcode_for_file_access(), errmsg("could not fsync write-through log file %s: %m",
                                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
            }
            break;
#endif
#ifdef HAVE_FDATASYNC
        case SYNC_METHOD_FDATASYNC:
            if (pg_fdatasync(fd) != 0) {
                ereport(PANIC,
                        (errcode_for_file_access(), errmsg("could not fdatasync log file %s: %m",
                                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
            }
            break;
#endif
        case SYNC_METHOD_OPEN:
        case SYNC_METHOD_OPEN_DSYNC:
            /* write synced it already */
            break;
        default:
            ereport(PANIC, (errmsg("unrecognized wal_sync_method: %d", u_sess->attr.attr_storage.sync_method)));
            break;
    }
}

/*
 * Return the filename of given log segment, as a palloc'd string.
 */
char *XLogFileNameP(TimeLineID tli, XLogSegNo segno)
{
    errno_t errorno = EOK;
    char *result = (char *)palloc(MAXFNAMELEN);

    errorno = snprintf_s(result, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", tli,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    return result;
}

bool check_roach_start_backup(const char *slotName)
{
    char backup_label[MAXPGPATH];
    errno_t rc = EOK;
    struct stat statbuf;

    /* check roach backup label file existence. */
    rc = snprintf_s(backup_label, sizeof(backup_label), sizeof(backup_label) - 1,
        "%s.%s", BACKUP_LABEL_FILE_ROACH, slotName);
    securec_check_ss(rc, "", "");

    if (stat(backup_label, &statbuf)) {
        if (errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", backup_label)));
        }
        ereport(DEBUG1, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("roach backup %s is not in progress", slotName)));
        return false;
    }

    return true;
}

static void CollectTableSpace(DIR* tblspcdir, List** tablespaces, StringInfo tblspc_mapfbuf, bool infotbssize)
{
    struct dirent* de = NULL;
    tablespaceinfo* ti = NULL;
    int datadirpathlen = strlen(t_thrd.proc_cxt.DataDir);
    /* Collect information about all tablespaces */
    while ((de = ReadDir(tblspcdir, "pg_tblspc")) != NULL) {
        char fullpath[MAXPGPATH + PG_TBLSPCS];
        char linkpath[MAXPGPATH];
        char* relpath = NULL;
        int rllen;
        errno_t errorno = EOK;

        errorno = memset_s(fullpath, MAXPGPATH + PG_TBLSPCS, '\0', MAXPGPATH + PG_TBLSPCS);
        securec_check(errorno, "", "");

        errorno = memset_s(linkpath, MAXPGPATH, '\0', MAXPGPATH);
        securec_check(errorno, "", "");

        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        errorno =
            snprintf_s(fullpath, MAXPGPATH + PG_TBLSPCS, MAXPGPATH + PG_TBLSPCS - 1, "pg_tblspc/%s", de->d_name);
        securec_check_ss(errorno, "\0", "\0");

#if defined(HAVE_READLINK) || defined(WIN32)
        rllen = readlink(fullpath, linkpath, sizeof(linkpath));
        if (rllen < 0) {
            ereport(WARNING, (errmsg("could not read symbolic link \"%s\": %m", fullpath)));
            continue;
        } else if (rllen >= (int)sizeof(linkpath)) {
            ereport(WARNING, (errmsg("symbolic link \"%s\" target is too long", fullpath)));
            continue;
        }
        linkpath[rllen] = '\0';

        /*
            * Relpath holds the relative path of the tablespace directory
            * when it's located within PGDATA, or NULL if it's located
            * elsewhere.
            */
        if (rllen > datadirpathlen && strncmp(linkpath, t_thrd.proc_cxt.DataDir, datadirpathlen) == 0 &&
            IS_DIR_SEP(linkpath[datadirpathlen]))
            relpath = linkpath + datadirpathlen + 1;

        ti = (tablespaceinfo*)palloc(sizeof(tablespaceinfo));
        ti->oid = pstrdup(de->d_name);
        ti->path = pstrdup(linkpath);
        ti->relativePath = relpath ? pstrdup(relpath) : NULL;
        ti->size = infotbssize ? sendTablespace(fullpath, true) : -1;
        if (tablespaces)
            *tablespaces = lappend(*tablespaces, ti);
        appendStringInfo(tblspc_mapfbuf, "%s %s\n", ti->oid, ti->path);
#else
        /*
            * If the platform does not have symbolic links, it should not be
            * possible to have tablespaces - clearly somebody else created
            * them. Warn about it and ignore.
            */
        ereport(WARNING,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
#endif
    }
}
/*
 * do_pg_start_backup is the workhorse of the user-visible pg_start_backup()
 * function. It creates the necessary starting checkpoint and constructs the
 * backup label file.
 *
 * There are two kind of backups: exclusive and non-exclusive. An exclusive
 * backup is started with pg_start_backup(), and there can be only one active
 * at a time. The backup label file of an exclusive backup is written to
 * $PGDATA/backup_label, and it is removed by pg_stop_backup().
 *
 * A non-exclusive backup is used for the streaming base backups (see
 * src/backend/replication/basebackup.c). The difference to exclusive backups
 * is that the backup label file is not written to disk. Instead, its would-be
 * contents are returned in *labelfile, and the caller is responsible for
 * including it in the backup archive as 'backup_label'. There can be many
 * non-exclusive backups active at the same time, and they don't conflict
 * with an exclusive backup either.
 *
 * Every successfully started non-exclusive backup must
 * be stopped by calling do_pg_stop_backup() or do_pg_abort_backup().
 */
XLogRecPtr do_pg_start_backup(const char* backupidstr, bool fast, char** labelfile, DIR* tblspcdir,
char** tblspcmapfile, List** tablespaces, bool infotbssize, bool needtblspcmapfile)
{
    bool exclusive = (labelfile == NULL);
    bool backup_started_in_recovery = false;
    XLogRecPtr checkpointloc;
    XLogRecPtr startpoint;
    pg_time_t stamp_time;
    char strfbuf[128];
    char xlogfilename[MAXFNAMELEN];
    XLogSegNo _logSegNo;
    struct stat stat_buf;
    FILE *fp = NULL;
    StringInfoData labelfbuf;
    StringInfoData tblspc_mapfbuf;
    int32 lastlrc = 0;
    errno_t errorno = EOK;

    gstrace_entry(GS_TRC_ID_do_pg_start_backup);

    backup_started_in_recovery = RecoveryInProgress();

    if (!superuser() && !has_rolreplication(GetUserId()) &&
        !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be system admin or replication role "
                               "or operator admin in operation mode to run a backup")));
    }

    /*
     * Currently only non-exclusive backup can be taken during recovery.
     */
    if (backup_started_in_recovery && exclusive) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));
    }

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if (!backup_started_in_recovery && !XLogIsNeeded()) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("WAL level not sufficient for making an online backup"),
                 errhint("wal_level must be set to \"archive\", \"hot_standby\" or \"logical\" at server start.")));
    }

    if (strlen(backupidstr) > MAXPGPATH) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("backup label too long (max %d bytes)", MAXPGPATH)));
    }

    /*
     * Mark backup active in shared memory.  We must do full-page WAL writes
     * during an on-line backup even if not doing so at other times, because
     * it's quite possible for the backup dump to obtain a "torn" (partially
     * written) copy of a database page if it reads the page concurrently with
     * our write to the same page.	This can be fixed as long as the first
     * write to the page in the WAL sequence is a full-page write. Hence, we
     * turn on forcePageWrites and then force a CHECKPOINT, to ensure there
     * are no dirty pages in shared memory that might get dumped while the
     * backup is in progress without having a corresponding WAL record.  (Once
     * the backup is complete, we need not force full-page writes anymore,
     * since we expect that any pages not modified during the backup interval
     * must have been correctly captured by the backup.)
     *
     * Note that forcePageWrites has no effect during an online backup from
     * the standby.
     *
     * We must hold WALInsertLock to change the value of forcePageWrites, to
     * ensure adequate interlocking against XLogInsertRecord().
     */
    StartSuspendWalInsert(&lastlrc);
    if (exclusive) {
        if (t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup) {
            StopSuspendWalInsert(lastlrc);
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("a backup is already in progress"), errhint("Run pg_stop_backup() and try again.")));
        }
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup = true;
    } else {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups++;
    }
    if (ENABLE_INCRE_CKPT) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = false;
    } else {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = true;
    }
    StopSuspendWalInsert(lastlrc);

    /* Ensure we release forcePageWrites if fail below */
    PG_ENSURE_ERROR_CLEANUP(pg_start_backup_callback, (Datum)BoolGetDatum(exclusive));
    {
        bool gotUniqueStartpoint = false;

        /*
         * Force an XLOG file switch before the checkpoint, to ensure that the
         * WAL segment the checkpoint is written to doesn't contain pages with
         * old timeline IDs.  That would otherwise happen if you called
         * pg_start_backup() right after restoring from a PITR archive: the
         * first WAL segment containing the startup checkpoint has pages in
         * the beginning with the old timeline ID.	That can cause trouble at
         * recovery: we won't have a history file covering the old timeline if
         * pg_xlog directory was not included in the base backup and the WAL
         * archive was cleared too before starting the backup.
         *
         * This also ensures that we have emitted a WAL page header that has
         * XLP_BKP_REMOVABLE off before we emit the checkpoint record.
         * Therefore, if a WAL archiver (such as pglesslog) is trying to
         * compress out removable backup blocks, it won't remove any that
         * occur after this point.
         *
         * During recovery, we skip forcing XLOG file switch, which means that
         * the backup taken during recovery is not available for the special
         * recovery case described above.
         */
        if (!backup_started_in_recovery) {
            (void)RequestXLogSwitch();
        }

        do {
            bool checkpointfpw = false;

            /*
             * Force a CHECKPOINT.	Aside from being necessary to prevent torn
             * page problems, this guarantees that two successive backup runs
             * will have different checkpoint positions and hence different
             * history file names, even if nothing happened in between.
             *
             * During recovery, establish a restartpoint if possible. We use
             * the last restartpoint as the backup starting checkpoint. This
             * means that two successive backup runs can have same checkpoint
             * positions.
             *
             * Since the fact that we are executing do_pg_start_backup()
             * during recovery means that checkpointer is running, we can use
             * RequestCheckpoint() to establish a restartpoint.
             *
             * We use CHECKPOINT_IMMEDIATE only if requested by user (via
             * passing fast = true).  Otherwise this can take awhile.
             */
            RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | (fast ? CHECKPOINT_IMMEDIATE : 0));

            /*
             * Now we need to fetch the checkpoint record location, and also
             * its REDO pointer.  The oldest point in WAL that would be needed
             * to restore starting from the checkpoint is precisely the REDO
             * pointer.
             */
            LWLockAcquire(ControlFileLock, LW_SHARED);
            checkpointloc = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
            startpoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo;
            checkpointfpw = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.fullPageWrites;
            LWLockRelease(ControlFileLock);

            if (backup_started_in_recovery) {
                /* use volatile pointer to prevent code rearrangement */
                XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
                XLogRecPtr recptr;

                /*
                 * Check to see if all WAL replayed during online backup
                 * (i.e., since last restartpoint used as backup starting
                 * checkpoint) contain full-page writes.
                 */
                SpinLockAcquire(&xlogctl->info_lck);
                recptr = xlogctl->lastFpwDisableRecPtr;
                SpinLockRelease(&xlogctl->info_lck);

#ifdef ENABLE_MULTIPLE_NODES
                if (!checkpointfpw || XLByteLE(startpoint, recptr)) {
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                    errmsg("WAL generated with full_page_writes=off was replayed "
                                           "since last restartpoint"),
                                    errhint("This means that the backup being taken on the standby "
                                            "is corrupt and should not be used. "
                                            "Enable full_page_writes and run CHECKPOINT on the master, "
                                            "and then try an online backup again.")));
                }
#endif

                /*
                 * During recovery, since we don't use the end-of-backup WAL
                 * record and don't write the backup history file, the
                 * starting WAL location doesn't need to be unique. This means
                 * that two base backups started at the same time might use
                 * the same checkpoint as starting locations.
                 */
                gotUniqueStartpoint = true;
            }

            /*
             * If two base backups are started at the same time (in WAL sender
             * processes), we need to make sure that they use different
             * checkpoints as starting locations, because we use the starting
             * WAL location as a unique identifier for the base backup in the
             * end-of-backup WAL record and when we write the backup history
             * file. Perhaps it would be better generate a separate unique ID
             * for each backup instead of forcing another checkpoint, but
             * taking a checkpoint right after another is not that expensive
             * either because only few buffers have been dirtied yet.
             */
            StartSuspendWalInsert(&lastlrc);
            if (XLByteLT(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.lastBackupStart, startpoint)) {
                t_thrd.shemem_ptr_cxt.XLogCtl->Insert.lastBackupStart = startpoint;
                gotUniqueStartpoint = true;
            }
            StopSuspendWalInsert(lastlrc);
        } while (!gotUniqueStartpoint);

        XLByteToSeg(startpoint, _logSegNo);
        errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                             (uint32)((_logSegNo) / XLogSegmentsPerXLogId),
                             (uint32)((_logSegNo) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");

        /*
         * Construct tablespace_map file
         */
        initStringInfo(&tblspc_mapfbuf);

        CollectTableSpace(tblspcdir, tablespaces, &tblspc_mapfbuf, infotbssize);

        /*
         * Construct backup label file
         */
        initStringInfo(&labelfbuf);

        /* Use the log timezone here, not the session timezone */
        stamp_time = (pg_time_t)time(NULL);
        pg_strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", pg_localtime(&stamp_time, log_timezone));
        appendStringInfo(&labelfbuf, "START WAL LOCATION: %X/%X (file %s)\n", (uint32)(startpoint >> 32),
                         (uint32)startpoint, xlogfilename);
        appendStringInfo(&labelfbuf, "CHECKPOINT LOCATION: %X/%X\n", (uint32)(checkpointloc >> 32),
                         (uint32)checkpointloc);
        appendStringInfo(&labelfbuf, "BACKUP METHOD: %s\n", exclusive ? "pg_start_backup" : "streamed");
        appendStringInfo(&labelfbuf, "BACKUP FROM: %s\n", backup_started_in_recovery ? "standby" : "master");
        appendStringInfo(&labelfbuf, "START TIME: %s\n", strfbuf);
        appendStringInfo(&labelfbuf, "LABEL: %s\n", backupidstr);

        /*
         * Okay, write the file, or return its contents to caller.
         */
        if (exclusive) {
            /*
             * Check for existing backup label --- implies a backup is already
             * running.  (XXX given that we checked exclusiveBackup above,
             * maybe it would be OK to just unlink any such label file?)
             */
            char *fileName = BACKUP_LABEL_FILE;

            if (strcmp(u_sess->attr.attr_common.application_name, "gs_roach") == 0) {
                fileName = BACKUP_LABEL_FILE_ROACH;
            }
            if (stat(fileName, &stat_buf) != 0) {
                if (errno != ENOENT) {
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", fileName)));
                }
            } else {
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("a backup is already in progress"),
                         errhint("If you're sure there is no backup in progress, remove file \"%s\" and try again.",
                                 fileName)));
            }

            fp = AllocateFile(fileName, "w");

            if (fp == NULL) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", fileName)));
            }
            if (fwrite(labelfbuf.data, labelfbuf.len, 1, fp) != 1 || fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0 ||
                ferror(fp) || FreeFile(fp)) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", fileName)));
            }
            pfree(labelfbuf.data);

            /* Write backup tablespace_map file. */
            if (tblspc_mapfbuf.len > 0) {
                if (stat(TABLESPACE_MAP, &stat_buf) != 0) {
                    if (errno != ENOENT)
                        ereport(ERROR,
                            (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", TABLESPACE_MAP)));
                } else
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("a backup is already in progress"),
                            errhint("If you're sure there is no backup in progress, remove file \"%s\" and try again.",
                                TABLESPACE_MAP)));

                fp = AllocateFile(TABLESPACE_MAP, "w");

                if (!fp)
                    ereport(
                        ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", TABLESPACE_MAP)));
                if (fwrite(tblspc_mapfbuf.data, tblspc_mapfbuf.len, 1, fp) != 1 || fflush(fp) != 0 ||
                    pg_fsync(fileno(fp)) != 0 || ferror(fp) || FreeFile(fp))
                    ereport(
                        ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", TABLESPACE_MAP)));
            }
            pfree(tblspc_mapfbuf.data);
        } else {
            *labelfile = labelfbuf.data;
            if (tblspc_mapfbuf.len > 0)
                *tblspcmapfile = tblspc_mapfbuf.data;
        }
    }
    PG_END_ENSURE_ERROR_CLEANUP(pg_start_backup_callback, (Datum)BoolGetDatum(exclusive));

    if (exclusive)
    {
        u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_EXCLUSIVE;
    } else {
        u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NON_EXCLUSIVE;
    }

    gstrace_exit(GS_TRC_ID_do_pg_start_backup);

    /*
     * We're done.  As a convenience, return the starting WAL location.
     */
    return startpoint;
}

/* Error cleanup callback for pg_start_backup */
static void pg_start_backup_callback(int code, Datum arg)
{
    bool exclusive = DatumGetBool(arg);
    int32 lastlrc = 0;

    /* Update backup counters and forcePageWrites on failure */
    StartSuspendWalInsert(&lastlrc);
    if (exclusive) {
        Assert(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup);
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup = false;
    } else {
        Assert(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups > 0);
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups--;
    }

    if (!t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup &&
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups == 0) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = false;
    }
    StopSuspendWalInsert(lastlrc);
}

/*
 * do_pg_stop_backup is the workhorse of the user-visible pg_stop_backup()
 * function.

 * If labelfile is NULL, this stops an exclusive backup. Otherwise this stops
 * the non-exclusive backup specified by 'labelfile'.
 */
XLogRecPtr do_pg_stop_backup(char *labelfile, bool waitforarchive)
{
    bool exclusive = (labelfile == NULL);
    bool backup_started_in_recovery = false;
    XLogRecPtr startpoint;
    XLogRecPtr stoppoint;
    pg_time_t stamp_time;
    char strfbuf[128];
    char histfilepath[MAXPGPATH];
    char startxlogfilename[MAXFNAMELEN];
    char stopxlogfilename[MAXFNAMELEN];
    char lastxlogfilename[MAXFNAMELEN];
    char histfilename[MAXFNAMELEN];
    char backupfrom[20];
    XLogSegNo _logSegNo;
    FILE *lfp = NULL;
    FILE *fp = NULL;
    char ch;
    int seconds_before_warning;
    int waits = 0;
    bool reported_waiting = false;
    char *remaining = NULL;
    char *ptr = NULL;
    uint32 hi, lo;
    int32 lastlrc = 0;
    errno_t errorno = EOK;

    gstrace_entry(GS_TRC_ID_do_pg_stop_backup);

    backup_started_in_recovery = RecoveryInProgress();

    if (!superuser() && !has_rolreplication(GetUserId()) &&
        !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        (errmsg("must be system admin or replication role "
                                "or operator admin in operation mode to run a backup"))));
    }

    /*
     * Currently only non-exclusive backup can be taken during recovery.
     */
    if (backup_started_in_recovery && exclusive) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("WAL control functions cannot be executed during recovery.")));
    }

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if (!backup_started_in_recovery && !XLogIsNeeded()) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("WAL level not sufficient for making an online backup"),
                 errhint("wal_level must be set to \"archive\", \"hot_standby\" or \"logical\" at server start.")));
    }

    /*
     * OK to update backup counters and forcePageWrites
     */
    StartSuspendWalInsert(&lastlrc);
    if (exclusive) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup = false;
    } else {
        /*
         * The user-visible pg_start/stop_backup() functions that operate on
         * exclusive backups can be called at any time, but for non-exclusive
         * backups, it is expected that each do_pg_start_backup() call is
         * matched by exactly one do_pg_stop_backup() call.
         */
        Assert(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups > 0);
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups--;
    }

    if (!t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup &&
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups == 0) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = false;
    }
    u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NONE;
    StopSuspendWalInsert(lastlrc);
        
    /*
     * Clean up session-level lock.
     *
    * You might think that WALInsertLockRelease() can be called before
    * cleaning up session-level lock because session-level lock doesn't need
     * to be protected with WAL insertion lock. But since
     * CHECK_FOR_INTERRUPTS() can occur in it, session-level lock must be
     * cleaned up before it.
     */
    u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NONE;

    if (exclusive) {
        /*
         * Read the existing label file into memory.
         */
        struct stat statbuf;
        int r;
        char *fileName = BACKUP_LABEL_FILE;

        if (strcmp(u_sess->attr.attr_common.application_name, "gs_roach") == 0) {
            fileName = BACKUP_LABEL_FILE_ROACH;
        }
        if (stat(fileName, &statbuf)) {
            if (errno != ENOENT) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", fileName)));
            }
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("a backup is not in progress")));
        }

        lfp = AllocateFile(fileName, "r");
        if (lfp == NULL) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", fileName)));
        }
        if (statbuf.st_size > BLCKSZ) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("file size is wrong, \"%s\": %m", fileName)));
        }
        labelfile = (char *)palloc(statbuf.st_size + 1);
        r = fread(labelfile, statbuf.st_size, 1, lfp);
        labelfile[statbuf.st_size] = '\0';

        /*
         * Close and remove the backup label file
         */
        if (r != 1 || ferror(lfp) || FreeFile(lfp)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", fileName)));
        }
        if (strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0) {
            if (unlink(fileName) != 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", fileName)));
            }
            if (unlink(TABLESPACE_MAP) != 0) {
                ereport(DEBUG1, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", TABLESPACE_MAP)));
            }
        }
    }

    /*
     * Read and parse the START WAL LOCATION line (this code is pretty crude,
     * but we are not expecting any variability in the file format).
     */
    if (sscanf_s(labelfile, "START WAL LOCATION: %X/%X (file %24s)%c", &hi, &lo, startxlogfilename,
                 sizeof(startxlogfilename), &ch, 1) != 4 ||
        ch != '\n') {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
    }
    startpoint = (((uint64)hi) << 32) | lo;
    remaining = strchr(labelfile, '\n') + 1; /* %n is not portable enough */

    /*
     * Parse the BACKUP FROM line. If we are taking an online backup from the
     * standby, we confirm that the standby has not been promoted during the
     * backup.
     */
    ptr = strstr(remaining, "BACKUP FROM:");
    if ((ptr == NULL) || sscanf_s(ptr, "BACKUP FROM: %19s\n", backupfrom, sizeof(backupfrom)) != 1) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
    }
    if (strcmp(backupfrom, "standby") == 0 && !backup_started_in_recovery) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("the standby was promoted during online backup"),
                        errhint("This means that the backup being taken is corrupt "
                                "and should not be used. "
                                "Try taking another online backup.")));
    }

    /*
     * During recovery, we don't write an end-of-backup record. We assume that
     * pg_control was backed up last and its minimum recovery point can be
     * available as the backup end location. Since we don't have an
     * end-of-backup record, we use the pg_control value to check whether
     * we've reached the end of backup when starting recovery from this
     * backup. We have no way of checking if pg_control wasn't backed up last
     * however.
     *
     * We don't force a switch to new WAL file and wait for all the required
     * files to be archived. This is okay if we use the backup to start the
     * standby. But, if it's for an archive recovery, to ensure all the
     * required files are available, a user should wait for them to be
     * archived, or include them into the backup.
     *
     * We return the current minimum recovery point as the backup end
     * location. Note that it can be greater than the exact backup end
     * location if the minimum recovery point is updated after the backup of
     * pg_control. This is harmless for current uses.
     *
     * XXX currently a backup history file is for informational and debug
     * purposes only. It's not essential for an online backup. Furthermore,
     * even if it's created, it will not be archived during recovery because
     * an archiver is not invoked. So it doesn't seem worthwhile to write a
     * backup history file during recovery.
     */
    if (backup_started_in_recovery) {
        /* use volatile pointer to prevent code rearrangement */
        volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
        XLogRecPtr recptr;

        /*
         * Check to see if all WAL replayed during online backup contain
         * full-page writes.
         */
        SpinLockAcquire(&xlogctl->info_lck);
        recptr = xlogctl->lastFpwDisableRecPtr;
        SpinLockRelease(&xlogctl->info_lck);

#ifdef ENABLE_MULTIPLE_NODES
        if (XLByteLE(startpoint, recptr)) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("WAL generated with full_page_writes=off was replayed "
                                   "during online backup"),
                            errhint("This means that the backup being taken on the standby "
                                    "is corrupt and should not be used. "
                                    "Enable full_page_writes and run CHECKPOINT on the master, "
                                    "and then try an online backup again.")));
        }
#endif

        LWLockAcquire(ControlFileLock, LW_SHARED);
        stoppoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
        LWLockRelease(ControlFileLock);

        if (XLByteLE(stoppoint, startpoint)) {
            stoppoint = GetXLogReplayRecPtr(NULL);
        }

        gstrace_exit(GS_TRC_ID_do_pg_stop_backup);
        return stoppoint;
    }

    /*
     * Write the backup-end xlog record
     */
    XLogBeginInsert();
    XLogRegisterData((char *)(&startpoint), sizeof(startpoint));
    stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);
    if (strcmp(u_sess->attr.attr_common.application_name, "gs_roach") == 0) {
        char *fileName = BACKUP_LABEL_FILE_ROACH;
        struct stat statbuf;

        /* stoppoint need to flush */
        XLogWaitFlush(stoppoint);
        if (stat(fileName, &statbuf)) {
            if (errno != ENOENT) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", fileName)));
            }
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("the file is not exist \"%s\": %m", fileName)));
        }
        if (durable_rename(BACKUP_LABEL_FILE_ROACH, BACKUP_LABEL_FILE_ROACH_DONE, ERROR)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m",
                                                              BACKUP_LABEL_FILE_ROACH, BACKUP_LABEL_FILE_ROACH_DONE)));
        }
    }

    /*
     * Force a switch to a new xlog segment file, so that the backup is valid
     * as soon as archiver moves out the current segment file.
     */
    (void)RequestXLogSwitch();

    /*
     * When doing gs_rewind after RequestXLogSwitch, there will be a error "No such file or directory",
     * So we RequestCheckpoint to generate a xlog segment following the XLogSwitch record.
     */
    RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);

    XLByteToPrevSeg(stoppoint, _logSegNo);
    errorno = snprintf_s(stopxlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((_logSegNo) / XLogSegmentsPerXLogId), (uint32)((_logSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    /* Use the log timezone here, not the session timezone */
    stamp_time = (pg_time_t)time(NULL);
    pg_strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", pg_localtime(&stamp_time, log_timezone));

    /*
     * Write the backup history file
     */
    XLByteToSeg(startpoint, _logSegNo);
    errorno = snprintf_s(histfilepath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%08X%08X%08X.%08X.backup",
                         t_thrd.xlog_cxt.ThisTimeLineID, (uint32)((_logSegNo) / XLogSegmentsPerXLogId),
                         (uint32)((_logSegNo) % XLogSegmentsPerXLogId), (uint32)(startpoint % XLogSegSize));
    securec_check_ss(errorno, "", "");

    fp = AllocateFile(histfilepath, "w");
    if (fp == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", histfilepath)));
    }
    fprintf(fp, "START WAL LOCATION: %X/%X (file %s)\n", (uint32)(startpoint >> 32), (uint32)startpoint,
            startxlogfilename);
    fprintf(fp, "STOP WAL LOCATION: %X/%X (file %s)\n", (uint32)(stoppoint >> 32), (uint32)stoppoint, stopxlogfilename);
    /* transfer remaining lines from label to history file */
    fprintf(fp, "%s", remaining);
    fprintf(fp, "STOP TIME: %s\n", strfbuf);
    if (fflush(fp) || ferror(fp) || FreeFile(fp)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", histfilepath)));
    }

    /*
     * Clean out any no-longer-needed history files.  As a side effect, this
     * will post a .ready file for the newly created history file, notifying
     * the archiver that history file may be archived immediately.
     */
    CleanupBackupHistory();

    /*
     * If archiving is enabled, wait for all the required WAL files to be
     * archived before returning. If archiving isn't enabled, the required WAL
     * needs to be transported via streaming replication (hopefully with
     * wal_keep_segments set high enough), or some more exotic mechanism like
     * polling and copying files from pg_xlog with script. We have no
     * knowledge of those mechanisms, so it's up to the user to ensure that he
     * gets all the required WAL.
     *
     * We wait until both the last WAL file filled during backup and the
     * history file have been archived, and assume that the alphabetic sorting
     * property of the WAL files ensures any earlier WAL files are safely
     * archived as well.
     *
     * We wait forever, since archive_command is supposed to work and we
     * assume the admin wanted his backup to work completely. If you don't
     * wish to wait, you can set statement_timeout.  Also, some notices are
     * issued to clue in anyone who might be doing this interactively.
     */
    if (waitforarchive && XLogArchivingActive()) {
        XLByteToPrevSeg(stoppoint, _logSegNo);
        errorno = snprintf_s(lastxlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X",
                             t_thrd.xlog_cxt.ThisTimeLineID, (uint32)((_logSegNo) / XLogSegmentsPerXLogId),
                             (uint32)((_logSegNo) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");

        XLByteToSeg(startpoint, _logSegNo);
        errorno = snprintf_s(histfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X.%08X.backup",
                             t_thrd.xlog_cxt.ThisTimeLineID, (uint32)((_logSegNo) / XLogSegmentsPerXLogId),
                             (uint32)((_logSegNo) % XLogSegmentsPerXLogId), (uint32)(startpoint % XLogSegSize));
        securec_check_ss(errorno, "", "");

        seconds_before_warning = 60;
        waits = 0;

        while (XLogArchiveIsBusy(lastxlogfilename) || XLogArchiveIsBusy(histfilename)) {
            CHECK_FOR_INTERRUPTS();

            if (!reported_waiting && waits > 5) {
                ereport(NOTICE,
                        (errmsg("pg_stop_backup cleanup done, waiting for required WAL segments to be archived")));
                reported_waiting = true;
            }

            pg_usleep(1000000L);

            if (++waits >= seconds_before_warning) {
                seconds_before_warning *= 2; /* This wraps in >10 years... */
                ereport(WARNING,
                        (errmsg("pg_stop_backup still waiting for all required WAL segments to be archived (%d seconds "
                                "elapsed)",
                                waits),
                         errhint("Check that your archive_command is executing properly.  "
                                 "pg_stop_backup can be canceled safely, "
                                 "but the database backup will not be usable without all the WAL segments.")));
            }
        }

        ereport(NOTICE, (errmsg("pg_stop_backup complete, all required WAL segments have been archived")));
    } else if (waitforarchive) {
        ereport(NOTICE,
                (errmsg(
                    "WAL archiving is not enabled; you must ensure that all required WAL segments are copied through "
                    "other means to complete the backup")));
    }

    gstrace_exit(GS_TRC_ID_do_pg_stop_backup);
    /*
     * We're done.  As a convenience, return the ending WAL location.
     */
    return stoppoint;
}

/*
 * do_pg_abort_backup: abort a running backup
 *
 * This does just the most basic steps of do_pg_stop_backup(), by taking the
 * system out of backup mode, thus making it a lot more safe to call from
 * an error handler.
 *
 * NB: This is only for aborting a non-exclusive backup that doesn't write
 * backup_label. A backup started with pg_stop_backup() needs to be finished
 * with pg_stop_backup().
 */
void do_pg_abort_backup(void)
{
    if (u_sess->proc_cxt.sessionBackupState != SESSION_BACKUP_NON_EXCLUSIVE)
        return;

    int32 lastlrc = 0;

    StartSuspendWalInsert(&lastlrc);
	/*
     * Quick exit if session is not keeping around a non-exclusive backup
     * already started.
     */
    if (u_sess->proc_cxt.sessionBackupState != SESSION_BACKUP_NON_EXCLUSIVE)
        return;

    Assert(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups > 0);
    t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups--;

    if (!t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup &&
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups == 0) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = false;
    }
    u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NONE;

    StopSuspendWalInsert(lastlrc);
    u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NONE;
}

/*
 * start roach backup.
 * get current checkpoint location, together with its redo location, and write them into
 * roach backup label.
 * We avoid requesting extra checkpoint here and adopt a similar recovery logic as
 * recovery from standby backup, i.e., backup pg_control last and use its minrecovery point
 * to judge recovery consistency.
 */
XLogRecPtr do_roach_start_backup(const char *backupidstr)
{
    bool backup_started_in_recovery = false;
    XLogRecPtr checkpointloc;
    XLogRecPtr startpoint;
    pg_time_t stamp_time;
    char strfbuf[128];
    char xlogfilename[MAXFNAMELEN];
    XLogSegNo _logSegNo;
    struct stat stat_buf;
    FILE *fp = NULL;
    StringInfoData labelfbuf;
    errno_t errorno = EOK;

    gstrace_entry(GS_TRC_ID_do_pg_start_backup);

    backup_started_in_recovery = RecoveryInProgress();

    check_roach_backup_condition(backup_started_in_recovery, backupidstr);

    /*
     * Now we need to fetch the checkpoint record location, and also
     * its REDO pointer. Different from pg_do_start_backup, in order to
     * save IO, this checkpoint is an existing one, rather than a newly-forced full checkpoint.
     */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    checkpointloc = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
    startpoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo;
    LWLockRelease(ControlFileLock);

    /* wait for checkpoint record to be copied */
    if (!backup_started_in_recovery && u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        SyncRepWaitForLSN(checkpointloc);
    }

    XLByteToSeg(startpoint, _logSegNo);
    errorno = snprintf_s(xlogfilename, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                     (uint32)((_logSegNo) / XLogSegmentsPerXLogId), (uint32)((_logSegNo) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    /* Construct backup label file with roach slot name */
    initStringInfo(&labelfbuf);
    /* Use the log timezone here, not the session timezone */
    stamp_time = (pg_time_t)time(NULL);
    pg_strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", pg_localtime(&stamp_time, log_timezone));
    appendStringInfo(&labelfbuf, "START WAL LOCATION: %X/%X (file %s)\n", (uint32)(startpoint >> 32),
                     (uint32)startpoint, xlogfilename);
    appendStringInfo(&labelfbuf, "CHECKPOINT LOCATION: %X/%X\n", (uint32)(checkpointloc >> 32), (uint32)checkpointloc);
    appendStringInfo(&labelfbuf, "BACKUP METHOD: %s\n", "pg_start_backup");
    appendStringInfo(&labelfbuf, "BACKUP FROM: %s\n", "roach_master");
    appendStringInfo(&labelfbuf, "START TIME: %s\n", strfbuf);
    appendStringInfo(&labelfbuf, "LABEL: %s\n", backupidstr);

    errorno = snprintf_s(strfbuf, sizeof(strfbuf), sizeof(strfbuf) - 1, "%s.%s", BACKUP_LABEL_FILE_ROACH, backupidstr);
    securec_check_ss(errorno, "\0", "\0");

    if (stat(strfbuf, &stat_buf) != 0) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", strfbuf)));
        }
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("a backup is already in progress"),
                errhint("If you're sure there is no backup in progress, remove file \"%s\" and try again.", strfbuf)));
    }

    fp = AllocateFile(strfbuf, "w");

    if (fp == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", strfbuf)));
    }
    if (fwrite(labelfbuf.data, labelfbuf.len, 1, fp) != 1 || fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0 ||
        ferror(fp) || FreeFile(fp)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", strfbuf)));
    }
    pfree(labelfbuf.data);

    gstrace_exit(GS_TRC_ID_do_pg_start_backup);

    return startpoint;
}


/*
 * stop roach backup
 * For primary, get current insert position.
 * For standby, get current minRecoveryPoint.
 * As roach start backup, we avoid requesting full checkpoint to save IO.
 */
XLogRecPtr do_roach_stop_backup(const char *backupidstr)
{
    bool backup_started_in_recovery = false;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint64 current_bytepos;
    XLogRecPtr stoppoint;
    char backup_label[MAXPGPATH];
    char backup_label_done[MAXPGPATH];
    errno_t errorno = EOK;
    struct stat statbuf;

    gstrace_entry(GS_TRC_ID_do_pg_stop_backup);

    backup_started_in_recovery = RecoveryInProgress();

    check_roach_backup_condition(backup_started_in_recovery, backupidstr);

    /* check roach backup label file existence. */
    errorno = snprintf_s(backup_label, sizeof(backup_label), sizeof(backup_label) - 1, "%s.%s", BACKUP_LABEL_FILE_ROACH, backupidstr);
    securec_check_ss(errorno, "\0", "\0");

    if (stat(backup_label, &statbuf)) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", backup_label)));
        }
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("roach backup %s is not in progress", backupidstr)));
    }

    /* get backup stop lsn. For standby backup logic, see comments in do_pg_stop_backup. */
    if (backup_started_in_recovery) {
        /* Do we need to worry about reverted minRecoveryPoint? */
        LWLockAcquire(ControlFileLock, LW_SHARED);
        stoppoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
        LWLockRelease(ControlFileLock);
    } else {
#if defined(__x86_64__) || defined(__aarch64__)
        current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
        stoppoint = XLogBytePosToEndRecPtr(current_bytepos);
#else
        SpinLockAcquire(&Insert->insertpos_lck);
        stoppoint = XLogBytePosToEndRecPtr(Insert->CurrBytePos);
        SpinLockRelease(&Insert->insertpos_lck);
#endif
}

    /* wait for stop lsn xlog to be copied */
    if (!backup_started_in_recovery && u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        SyncRepWaitForLSN(stoppoint);
    }

    /* rename roach backup label for resume backup */
    errorno = snprintf_s(backup_label_done, sizeof(backup_label_done), sizeof(backup_label_done) - 1, "%s.%s", BACKUP_LABEL_FILE_ROACH_DONE, backupidstr);
    securec_check_ss(errorno, "\0", "\0");
    if (durable_rename(backup_label, backup_label_done, ERROR)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m",
                                                          backup_label, backup_label_done)));
    }

    gstrace_exit(GS_TRC_ID_do_pg_stop_backup);

    return stoppoint;
}

void check_roach_backup_condition(bool backup_started_in_recovery, const char *backupidstr)
{
    if (!superuser() && !has_rolreplication(GetUserId()) &&
        !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be system admin or replication role or "
                               "operator admin in operation mode to run a roach backup")));
    }

    /* Currently roach backup can only be taken during recovery. */
    if (backup_started_in_recovery) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("roach backup cannot be executed during recovery.")));
    }

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if (!backup_started_in_recovery && !XLogIsNeeded()) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("WAL level not sufficient for making an online roach backup"),
                 errhint("wal_level must be set to \"archive\", \"hot_standby\" or \"logical\" at server start.")));
    }

    /* currently, roach backup id is the same as roach backup replication slot */
    if (strlen(backupidstr) >= NAMEDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("roach backup id name \"%s\" is too long", backupidstr)));
    }

    if (strncmp(backupidstr, "gs_roach", strlen("gs_roach")) == 0 &&
        strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0) {
        ereport(ERROR,
               (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Backup id name starting with gs_roach is internally reserverd")));
    }
}

/*
 *Error cleanup callback for exclusive backup
 */
static void AbortExcluseBackupCallback(int code, Datum arg)
{
    if (u_sess->proc_cxt.sessionBackupState != SESSION_BACKUP_EXCLUSIVE) {
        return;
    }
    int32 lastlrc = 0;

    /* Update backup counters and forcePageWrites on failure */
    StartSuspendWalInsert(&lastlrc);
    Assert(t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup);
    t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup = false;
    if (!t_thrd.shemem_ptr_cxt.XLogCtl->Insert.exclusiveBackup &&
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.nonExclusiveBackups == 0) {
        t_thrd.shemem_ptr_cxt.XLogCtl->Insert.forcePageWrites = false;
    }
    u_sess->proc_cxt.sessionBackupState = SESSION_BACKUP_NONE;
    StopSuspendWalInsert(lastlrc);
    if (strcmp(u_sess->attr.attr_common.application_name, "gs_roach") != 0) {
        (void)unlink(BACKUP_LABEL_FILE);
    }
}

/*
 *Register a handler that will warn about unterminated exclusive backups
 *at end of session, unless this has already been done.
 */
void RegisterAbortExclusiveBackup()
{
    if (u_sess->proc_cxt.registerExclusiveHandlerdone) {
        return;
    }

    on_shmem_exit(AbortExcluseBackupCallback, BoolGetDatum(true));
    u_sess->proc_cxt.registerExclusiveHandlerdone = true;
}

void enable_delay_xlog_recycle(bool isRedo)
{
    FILE *fp = NULL;

    ereport(LOG, (errmsg("start delaying xlog recycle so that checkpoint "
                         "no longer removes stale xlog segments")));

    fp = AllocateFile(DELAY_XLOG_RECYCLE_FILE, PG_BINARY_W);
    if (fp == NULL || fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0 || ferror(fp) || FreeFile(fp)) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("failed to touch %s file during enable xlog delay: %m", DELAY_XLOG_RECYCLE_FILE)));
    }

    SetDelayXlogRecycle(true, isRedo);

    /*
     * acquire and release RowPageReplicationLock to ensure that
     * concurrent heap_multi_insert will no longer use page replication after
     * we return
     */
    LWLockAcquire(RowPageReplicationLock, LW_EXCLUSIVE);
    LWLockRelease(RowPageReplicationLock);
}

void disable_delay_xlog_recycle(bool isRedo)
{
    ereport(LOG, (errmsg("stop delaying xlog recycle and consequent checkpoint "
                         "will remove stale xlog segments")));

    if (unlink(DELAY_XLOG_RECYCLE_FILE) < 0 && errno != ENOENT) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m. This will lead to residual of "
                                                          "stale xlog segments",
                                                          DELAY_XLOG_RECYCLE_FILE)));
    }

    SetDelayXlogRecycle(false, isRedo);
}

void startupInitDelayXlog(void)
{
    struct stat st;

    if (!lstat(DELAY_XLOG_RECYCLE_FILE, &st)) {
        ereport(LOG, (errmsg("xlog delay function was enabled at previous database shutdown")));

        /*
         * Do not turn on xlog delay function if we are recovering from archive or
         * if we are standby. In these cases, our xlog segments will be modified unexpectedly and
         * it is meaningless to keep them for back up.
         */
        if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
            disable_delay_xlog_recycle(true);
        } else {
            SetDelayXlogRecycle(true, true);
        }
    } else if (errno != ENOENT) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("failed to stat %s file:%m", DELAY_XLOG_RECYCLE_FILE)));
    }
}

char *getLastRewindTime()
{
    char buf[MAXPGPATH] = {0};
    char *retStr = NULL;
    errno_t ret;
    FILE *fd = NULL;
    struct stat st;

    /* if the file is not there, return */
    if (stat(REWIND_LABLE_FILE, &st)) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", REWIND_LABLE_FILE)));
        }
        return "";
    }

    fd = fopen(REWIND_LABLE_FILE, "r");
    if (fd == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open rewind file: %s\n", gs_strerror(errno))));
    }

    if (fgets(buf, lengthof(buf), fd) == NULL) {
        if (fclose(fd)) {
            ereport(LOG, (errcode_for_file_access(),
                          errmsg("get null bits, could not close file \"%s\": %m", REWIND_LABLE_FILE)));
        }
        fd = NULL;

        return "";
    }

    retStr = (char *)palloc0(sizeof(char) * (strlen(buf) + 1));
    ret = snprintf_s(retStr, strlen(buf) + 1, strlen(buf), "%s", buf);
    securec_check_ss(ret, "\0", "\0");

    if (fclose(fd)) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", REWIND_LABLE_FILE)));
    }
    fd = NULL;
    return retStr;
}

XLogRecPtr enable_delay_ddl_recycle(void)
{
    DelayDDLRange prevDelayRange = InvalidDelayRange;
    DelayDDLRange curDelayRange = InvalidDelayRange;
    XLogRecPtr globalDelayStartPtr = InvalidXLogRecPtr;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    bool usePrevDelayFile = false;

    if (!u_sess->attr.attr_storage.enable_cbm_tracking) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("could not enable delay ddl when enable_cbm_tracking is off!")));
    }

    ereport(LOG, (errmsg("start delaying ddl recycle so that column relation files "
                         "will remain even after they are logically dropped")));

    LWLockAcquire(DelayDDLLock, LW_EXCLUSIVE);
    if (!validate_parse_delay_ddl_file(&prevDelayRange)) {
        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        SetDDLDelayStartPtr(InvalidXLogRecPtr);
    }

    globalDelayStartPtr = GetDDLDelayStartPtr();

    if (!XLogRecPtrIsInvalid(prevDelayRange.startLSN) && XLByteLT(prevDelayRange.startLSN, prevDelayRange.endLSN)) {
        Assert(XLogRecPtrIsInvalid(globalDelayStartPtr));
        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        SetDDLDelayStartPtr(InvalidXLogRecPtr);
        prevDelayRange = InvalidDelayRange;
    }

    LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);

#if defined(__x86_64__) || defined(__aarch64__)
    uint64 current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
    curDelayRange.startLSN = XLogBytePosToEndRecPtr(current_bytepos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    curDelayRange.startLSN = XLogBytePosToEndRecPtr(Insert->CurrBytePos);
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    XLogWaitFlush(curDelayRange.startLSN);

    advanceXlogPtrToNextPageIfNeeded(&(curDelayRange.startLSN));

    /*
     * We only care about the case that prevDelayRange.startLSN is smaller
     * than curDelayRange.startLSN. Because we hold exclusive wal insert
     * lock to get curDelayRange.startLSN, larger prevDelayRange.startLSN
     * is meaningless to us.
     */
    if (!XLogRecPtrIsInvalid(prevDelayRange.startLSN) && XLByteLE(prevDelayRange.startLSN, curDelayRange.startLSN)) {
        if (!XLogRecPtrIsInvalid(globalDelayStartPtr) && XLByteLE(globalDelayStartPtr, prevDelayRange.startLSN)) {
            curDelayRange.startLSN = prevDelayRange.startLSN;
            usePrevDelayFile = true;
        } else {
            ereport(WARNING, (errmsg("unexpected global xlogctl delay start lsn %08X/%08X "
                                     "and previous delay start lsn %08X/%08X ",
                                     (uint32)(globalDelayStartPtr >> 32), (uint32)(globalDelayStartPtr),
                                     (uint32)(prevDelayRange.startLSN >> 32), (uint32)(prevDelayRange.startLSN))));
        }
    }

    if (!usePrevDelayFile) {
        if (!write_delay_ddl_file(curDelayRange, true))
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("failed to write %s file during enable delay ddl recycle", DELAY_DDL_RECYCLE_FILE)));
    }

    SetDDLDelayStartPtr(curDelayRange.startLSN);
    LWLockRelease(DelayDDLLock);

    ForceTrackCBMOnce(curDelayRange.startLSN, ENABLE_DDL_DELAY_TIMEOUT, true, true);

    return curDelayRange.startLSN;
}

/*
 * We will grab at most 4 exclusive LW locks at the same time, so the
 * sequence is important and deadlock should be avoided.
 *
 * If isForce is true, Please do not pass in valid barrierLSN.
 * In this case, we will ensure global delay lsn is reset and
 * delay file is unlink by ingoring possible errors, though,
 * there may be some residual of dropped column relation files
 * if we do encounter errors.
 */
void disable_delay_ddl_recycle(XLogRecPtr barrierLSN, bool isForce, XLogRecPtr *startLSN, XLogRecPtr *endLSN)
{
    DelayDDLRange delayRange = InvalidDelayRange;
    XLogRecPtr globalDelayStartPtr = InvalidXLogRecPtr;
    XLogRecPtr forceTrackLSN = InvalidXLogRecPtr;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    bool cbmTrackFailed = false;

    if (!u_sess->attr.attr_storage.enable_cbm_tracking) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg(" could not disable delay ddl when enable_cbm_tracking is off!")));
    }

    *startLSN = InvalidXLogRecPtr;
    *endLSN = InvalidXLogRecPtr;

    ereport(LOG, (errmsg("stop delaying ddl recycle and clear column relation files "
                         "that have been dropped during previous delay period. If any error occurs "
                         "in this progress, there may be residual of physical relation files "
                         "that need to be removed manually")));

    if (isForce && !XLogRecPtrIsInvalid(barrierLSN)) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("valid input lsn is not supported "
                                                                "while doing forceful disable delay ddl")));
    }

    /* hold this lock to prevent concurrent ddl still delay unlinking */
    LWLockAcquire(DelayDDLLock, LW_EXCLUSIVE);
    if (!validate_parse_delay_ddl_file(&delayRange)) {
        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        SetDDLDelayStartPtr(InvalidXLogRecPtr);
        LWLockRelease(DelayDDLLock);

        ereport(isForce ? LOG : ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                 errmsg("failed to validate %s file during disable delay ddl recycle", DELAY_DDL_RECYCLE_FILE)));

        return;
    }

    globalDelayStartPtr = GetDDLDelayStartPtr();

    if (!XLogRecPtrIsInvalid(delayRange.endLSN) || !XLByteEQ(globalDelayStartPtr, delayRange.startLSN) ||
        (!XLogRecPtrIsInvalid(barrierLSN) && XLByteLT(barrierLSN, delayRange.startLSN))) {
        ereport(isForce ? LOG : ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                 errmsg("inconsistent delay lsn range: global start lsn %08X/%08X, "
                        "%s file start lsn %08X/%08X end lsn %08X/%08X, "
                        "barrier lsn %08X/%08X",
                        (uint32)(globalDelayStartPtr >> 32), (uint32)(globalDelayStartPtr), DELAY_DDL_RECYCLE_FILE,
                        (uint32)(delayRange.startLSN >> 32), (uint32)(delayRange.startLSN),
                        (uint32)(delayRange.endLSN >> 32), (uint32)(delayRange.endLSN), (uint32)(barrierLSN >> 32),
                        (uint32)(barrierLSN))));

        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        SetDDLDelayStartPtr(InvalidXLogRecPtr);
        LWLockRelease(DelayDDLLock);

        return;
    }

    /* hold this lock to push cbm parse exact to the ddl stop position */
    LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);

    /*
     * hold this lock to avoid mis-unlinking column attribute files that
     * have been reused.
     * At present, only alter table add column for col rels will reuse existing files
     * instead of throwing ERROR. If other operations allow such reuse, must
     * also hold this lock in advance!
     */
    LWLockAcquire(RelfilenodeReuseLock, LW_EXCLUSIVE);

#if defined(__x86_64__) || defined(__aarch64__)
    uint64 current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
    delayRange.endLSN = XLogBytePosToEndRecPtr(current_bytepos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    delayRange.endLSN = XLogBytePosToEndRecPtr(Insert->CurrBytePos);
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    XLogWaitFlush(delayRange.endLSN);

    advanceXlogPtrToNextPageIfNeeded(&(delayRange.endLSN));

    /* if there was no xlog record since enable-delay-ddl, just remove the delay file */
    if (XLByteEQ(delayRange.startLSN, delayRange.endLSN)) {
        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        SetDDLDelayStartPtr(InvalidXLogRecPtr);
        LWLockRelease(RelfilenodeReuseLock);
        LWLockRelease(CBMParseXlogLock);
        LWLockRelease(DelayDDLLock);

        *startLSN = delayRange.startLSN;
        *endLSN = delayRange.endLSN;
        return;
    }

    if (!write_delay_ddl_file(delayRange, false)) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("failed to write %s file during disable delay ddl recycle", DELAY_DDL_RECYCLE_FILE)));
    }

    /*
     * We do not allow long jump before we finish executing delayed ddl and unlinking the
     * delay flag file, though we can ignore exiting finished delay file the next time we enable
     * or disable ddl recycle. Instead, we now take a more aggressive way so that
     * startup can have a chance to clean up the residual column files.
     */
    START_CRIT_SECTION();

    SetDDLDelayStartPtr(InvalidXLogRecPtr);
    LWLockRelease(DelayDDLLock);

    forceTrackLSN = ForceTrackCBMOnce(delayRange.endLSN, DISABLE_DDL_DELAY_TIMEOUT, true, true);

    if (XLogRecPtrIsInvalid(forceTrackLSN)) {
        ereport(WARNING, (errmsg("failed to push cbm track point to delay end lsn %08X/%08X, ",
                                 (uint32)(delayRange.endLSN >> 32), (uint32)(delayRange.endLSN))));

        cbmTrackFailed = true;
    }

    execDelayedDDL(delayRange.startLSN, delayRange.endLSN, cbmTrackFailed);

    END_CRIT_SECTION();

    LWLockRelease(RelfilenodeReuseLock);

    *startLSN = delayRange.startLSN;
    *endLSN = delayRange.endLSN;
}

XLogRecPtr enable_delay_ddl_recycle_with_slot(const char* slotname)
{
    XLogRecPtr delay_start_lsn;
    XLogRecPtr ret_lsn;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;

    if (!u_sess->attr.attr_storage.enable_cbm_tracking) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("could not enable delay ddl when enable_cbm_tracking is off!")));
    }

    ereport(LOG, (errmsg("start delaying ddl recycle with backup slot %s", slotname)));

    LWLockAcquire(DelayDDLLock, LW_EXCLUSIVE);

    /* hold this lock to push cbm parse exact to the ddl stop position */
    LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);

#if defined(__x86_64__) || defined(__aarch64__)
    uint64 current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
    delay_start_lsn = XLogBytePosToEndRecPtr(current_bytepos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    delay_start_lsn = XLogBytePosToEndRecPtr(Insert->CurrBytePos);
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    XLogWaitFlush(delay_start_lsn);

    advanceXlogPtrToNextPageIfNeeded(&delay_start_lsn);

    ret_lsn = slot_advance_confirmed_lsn(slotname, delay_start_lsn);

    LWLockRelease(DelayDDLLock);

    /*
     * Currently, we do not check pushed cbm track location at delay start.
     * If failed to push to the exact point, stop delay will complain and there will be
     * residual col files.
     * Later, we could retry set and push.
     */
    (void)ForceTrackCBMOnce(ret_lsn, ENABLE_DDL_DELAY_TIMEOUT, true, true);

    return ret_lsn;
}

/*
 * We will grab at most 4 exclusive LW locks at the same time, so the
 * sequence is important and deadlock should be avoided.
 *
 * If isForce is true, Please do not pass in valid barrierLSN.
 * In this case, we will ensure global delay lsn is reset and
 * delay file is unlink by ingoring possible errors, though,
 * there may be some residual of dropped column relation files
 * if we do encounter errors.
 */
void disable_delay_ddl_recycle_with_slot(const char* slotname, XLogRecPtr *startLSN, XLogRecPtr *endLSN)
{
    XLogRecPtr delay_start_lsn = InvalidXLogRecPtr;
    XLogRecPtr delay_end_lsn = InvalidXLogRecPtr;
    XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    bool need_recycle = true;

    if (!u_sess->attr.attr_storage.enable_cbm_tracking) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg(" could not disable delay ddl when enable_cbm_tracking is off!")));
    }

    ereport(LOG, (errmsg("stop delaying ddl recycle with backup slot %s", slotname)));

    LWLockAcquire(DelayDDLLock, LW_EXCLUSIVE);

    /* hold this lock to push cbm parse exact to the ddl stop position */
    LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);

    /*
     * hold this lock to avoid mis-unlinking column attribute files that
     * have been reused.
     * At present, only alter table add column for col rels will reuse existing files
     * instead of throwing ERROR. If other operations allow such reuse, must
     * also hold this lock in advance!
     */
    LWLockAcquire(RelfilenodeReuseLock, LW_EXCLUSIVE);

#if defined(__x86_64__) || defined(__aarch64__)
    uint64 current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
    delay_end_lsn = XLogBytePosToEndRecPtr(current_bytepos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    delay_end_lsn = XLogBytePosToEndRecPtr(Insert->CurrBytePos);
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    XLogWaitFlush(delay_end_lsn);

    advanceXlogPtrToNextPageIfNeeded(&delay_end_lsn);

    need_recycle = slot_reset_confirmed_lsn(slotname, &delay_start_lsn);

    /* no need to do recycle if no new xlog written or there are concurrent delay ddl operations */
    if (!need_recycle || XLByteEQ(delay_start_lsn, delay_end_lsn)) {
        LWLockRelease(RelfilenodeReuseLock);
        LWLockRelease(CBMParseXlogLock);
        LWLockRelease(DelayDDLLock);

        *startLSN = delay_start_lsn;
        *endLSN = (need_recycle ? delay_end_lsn : InvalidXLogRecPtr);
        return;
    }

    /* If we fail before removing all delayed col files, there will be residual. */
    LWLockRelease(DelayDDLLock);

    /*
     * Currently not check push result. See comments in enable_delay_ddl_recycle_with_slot.
     * There is a very small race condition with enable_delay_ddl_recycle_with_slot for pushing cbm tracked
     * position. Currently, we do not address this issue. The worst result is col file residual.
     */
    (void)ForceTrackCBMOnce(delay_end_lsn, DISABLE_DDL_DELAY_TIMEOUT, true, true);

    execDelayedDDL(delay_start_lsn, delay_end_lsn, true);

    LWLockRelease(RelfilenodeReuseLock);

    *startLSN = delay_start_lsn;
    *endLSN = delay_end_lsn;
}


/*
 * If system previously crashed while disabling delay ddl, we will try
 * to finish that in startup.
 * However, if cbm has already tracked far beyond delay end point, then we
 * could do nothing except leaving residual files of dropped col tables. The chance
 * is low and the result is safe, anyway.
 */
void startupInitDelayDDL(void)
{
    DelayDDLRange delayRange = InvalidDelayRange;

    if (!validate_parse_delay_ddl_file(&delayRange) || !u_sess->attr.attr_storage.enable_cbm_tracking) {
        if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
        }
    } else {
        Assert(!XLogRecPtrIsInvalid(delayRange.startLSN));

        /*
         * Do not turn on ddl delay function if we are recovering from archive or
         * if we are standby. In these cases, our xlog segments and data pages
         * might be modified unexpectedly and it is meaningless to keep them for back up.
         */
        if (t_thrd.xlog_cxt.ArchiveRestoreRequested) {
            ereport(LOG, (errmsg("Have to turn off ddl delay (start lsn %08X/%08X "
                                 "to end lsn %08X/%08X) before entering archive recovery "
                                 "or standby mode. There may be residual of column data files. "
                                 "Please refer to previous log to clean them manually if necessary.",
                                 (uint32)(delayRange.startLSN >> 32), (uint32)(delayRange.startLSN),
                                 (uint32)(delayRange.endLSN >> 32), (uint32)(delayRange.endLSN))));

            if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not remove %s file: %m", DELAY_DDL_RECYCLE_FILE)));
            }
        } else if (XLogRecPtrIsInvalid(delayRange.endLSN)) {
            ereport(LOG, (errmsg("ddl delay was enabled last time from start lsn %08X/%08X",
                                 (uint32)(delayRange.startLSN >> 32), (uint32)(delayRange.startLSN))));

            SetDDLDelayStartPtr(delayRange.startLSN);
        } else {
            ForceTrackCBMOnce(delayRange.endLSN, 0, false, false);
            execDelayedDDL(delayRange.startLSN, delayRange.endLSN, true);
        }
    }
}

bool validate_parse_delay_ddl_file(DelayDDLRange *delayRange)
{
    struct stat st;
    FILE *fp = NULL;
    DelayDDLRange tmpRange = InvalidDelayRange;
    bool result = false;
    errno_t err;

    if (stat(DELAY_DDL_RECYCLE_FILE, &st) == -1 && errno == ENOENT) {
        ereport(DEBUG5, (errmsg("%s file does not exist", DELAY_DDL_RECYCLE_FILE)));
        return result;
    }

    fp = AllocateFile(DELAY_DDL_RECYCLE_FILE, PG_BINARY_R);
    if (SECUREC_UNLIKELY(fp == NULL)) {
        ereport(LOG,
                (errcode_for_file_access(), errmsg("could not open existing file \"%s\": %m", DELAY_DDL_RECYCLE_FILE)));
        return result;
    }

    if (fread(&tmpRange, 1, sizeof(DelayDDLRange), fp) != sizeof(DelayDDLRange)) {
        ereport(WARNING, (errmsg("%s file is corrupted. We will zap it anyway", DELAY_DDL_RECYCLE_FILE)));
        goto END;
    }

    if (!XLogRecPtrIsInvalid(tmpRange.startLSN) &&
        (XLogRecPtrIsInvalid(tmpRange.endLSN) || XLByteLT(tmpRange.startLSN, tmpRange.endLSN))) {
        ereport(LOG, (errmsg("find existing %s file (start lsn %08X/%08X, end lsn %08X/%08X)", DELAY_DDL_RECYCLE_FILE,
                             (uint32)(tmpRange.startLSN >> 32), (uint32)(tmpRange.startLSN),
                             (uint32)(tmpRange.endLSN >> 32), (uint32)(tmpRange.endLSN))));
    } else {
        ereport(WARNING, (errmsg("%s file has unexpected start lsn %08X/%08X and end lsn "
                                 "%08X/%08X. While there may be residual of dropped column files, "
                                 "We will zap it anyway",
                                 DELAY_DDL_RECYCLE_FILE, (uint32)(tmpRange.startLSN >> 32), (uint32)(tmpRange.startLSN),
                                 (uint32)(tmpRange.endLSN >> 32), (uint32)(tmpRange.endLSN))));
        goto END;
    }

    err = memcpy_s(delayRange, sizeof(DelayDDLRange), &tmpRange, sizeof(DelayDDLRange));
    securec_check(err, "\0", "\0");
    result = true;

END:
    if (FreeFile(fp)) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", DELAY_DDL_RECYCLE_FILE)));
    }
    fp = NULL;
    return result;
}

bool write_delay_ddl_file(const DelayDDLRange &delayRange, bool onErrDelete)
{
    FILE *fp = AllocateFile(DELAY_DDL_RECYCLE_FILE, PG_BINARY_W);
    if (fp == NULL) {
        ereport(LOG, (errcode_for_file_access(),
                      errmsg("could not open file \"%s\" for write: %m", DELAY_DDL_RECYCLE_FILE)));
        return false;
    }

    if (fwrite(&delayRange, 1, sizeof(DelayDDLRange), fp) != sizeof(DelayDDLRange) || fflush(fp) != 0 ||
        pg_fsync(fileno(fp)) != 0 || ferror(fp) || FreeFile(fp)) {
        ereport(onErrDelete ? LOG : PANIC,
                (errcode_for_file_access(), errmsg("failed to write file \"%s\": %m", DELAY_DDL_RECYCLE_FILE)));

        /*
         * PANIC may be unnecessary here, because if we failed to write delay file during
         * enable delay and leave unset xlogctl delay start lsn, we would fix it the next time
         * we try to enable delay. To be safe, at present, we use PANIC.
         */
        if (onErrDelete && unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
            ereport(PANIC, (errcode_for_file_access(),
                            errmsg("could not remove %s file after write failure: %m", DELAY_DDL_RECYCLE_FILE)));
        }

        return false;
    }

    return true;
}

void execDelayedDDL(XLogRecPtr startLSN, XLogRecPtr endLSN, bool ignoreCBMErr)
{
    CBMArray *cbmArray = NULL;
    MemoryContext currentContext = CurrentMemoryContext;
    MemoryContext cbmParseContext;
    bool cbmParseFailed = false;
    long i;
    int saveInterruptHoldoffCount;

    cbmParseContext = AllocSetContextCreate(CurrentMemoryContext, "cbm_parse_context", ALLOCSET_DEFAULT_MINSIZE,
                                            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    LWLockAcquire(CBMParseXlogLock, LW_SHARED);
    saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        (void)MemoryContextSwitchTo(cbmParseContext);
        cbmArray = CBMGetMergedArray(startLSN, endLSN);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        (void)MemoryContextSwitchTo(currentContext);
        ErrorData *edata = CopyErrorData();
        FlushErrorState();
        MemoryContextDelete(cbmParseContext);
        cbmParseContext = NULL;

        if (!ignoreCBMErr) {
            LWLockRelease(CBMParseXlogLock);
        }

        if (ignoreCBMErr) {
            ereport(LOG, (errmsg("Failed to get cbm information during execute delayed DDL: %s", edata->message)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                            errmsg("Failed to get cbm information during execute delayed DDL: %s", edata->message)));
        }

        cbmParseFailed = true;
    }
    PG_END_TRY();
    LWLockRelease(CBMParseXlogLock);

    /*
     * step1. remove delay file
     * step2. remove dropped column file
     *
     * We use this sequence to avoid unlink newly created relations that
     * reuse the relfilenodes.
     * If we fail in step2, there will be residual of
     * column files.
     *
     * If caller told us to ignore cbm parse failue, just return after
     * unlinking the delay file.
     */
    if (unlink(DELAY_DDL_RECYCLE_FILE) < 0 && errno != ENOENT) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not remove %s file before unlink col relation files: %m", DELAY_DDL_RECYCLE_FILE)));
    }

    if (cbmParseFailed) {
        return;
    }

    if (!XLByteEQ(cbmArray->startLSN, startLSN) || !XLByteEQ(cbmArray->endLSN, endLSN)) {
        ereport(WARNING,
                (errmsg("delay start-end %08X/%08X-%08X/%08X is inconsistent with cbm tracking "
                        "result start-end %08X/%08X-%08X/%08X. To be safe, while there may be residual "
                        "of dropped column files, We will leave them anyway",
                        (uint32)(startLSN >> 32), (uint32)(startLSN), (uint32)(endLSN >> 32),
                        (uint32)(endLSN), (uint32)(cbmArray->startLSN >> 32), (uint32)(cbmArray->startLSN),
                        (uint32)(cbmArray->endLSN >> 32), (uint32)(cbmArray->endLSN))));
    } else {
        for (i = 0; i < cbmArray->arrayLength; i++) {
            /*
             * For row relations, relfilenode can not be reused if the physical file has not
             * been unlinked. However, for column relations, add column can reuse
             * the file that a previously crashed add-column-xact leaved. Such operations
             * must hold RelfilenodeReuseLock lock to avoid concurrency with execDelayedDDL.
             */
            if ((cbmArray->arrayEntry[i].changeType & PAGETYPE_DROP) &&
                !(cbmArray->arrayEntry[i].changeType & PAGETYPE_CREATE) && !(cbmArray->arrayEntry[i].totalBlockNum) &&
                IsValidColForkNum(cbmArray->arrayEntry[i].cbmTag.forkNum)) {
                CStore::UnlinkColDataFile(cbmArray->arrayEntry[i].cbmTag.rNode,
                                          ColForkNum2ColumnId(cbmArray->arrayEntry[i].cbmTag.forkNum), false);
            }
        }
    }

    FreeCBMArray(cbmArray);
    cbmArray = NULL;
    (void)MemoryContextSwitchTo(currentContext);
    MemoryContextDelete(cbmParseContext);
}

void RedoSpeedDiag(XLogRecPtr readPtr, XLogRecPtr endPtr)
{
    if (g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr == 0) {
        g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr = endPtr;
        INSTR_TIME_SET_CURRENT(g_instance.comm_cxt.predo_cxt.redoPf.preTime);
    }

    uint64 redoBytes = (uint64)(endPtr - g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr);
    instr_time newTime;
    INSTR_TIME_SET_CURRENT(newTime);
    instr_time tmpTime = newTime;
    INSTR_TIME_SUBTRACT(tmpTime, g_instance.comm_cxt.predo_cxt.redoPf.preTime);
    
    uint64 totalTime = INSTR_TIME_GET_MICROSEC(tmpTime);
    if (redoBytes >= REDO_SPEED_LOG_LEN || (redoBytes > 0 && totalTime >= MAX_OUT_INTERVAL)) {
        uint32 speed = 0;     /* KB/s */
        uint32 speed_raw = 0; /* B/s */
        if (totalTime > 0) {
            speed_raw = (redoBytes / totalTime) * US_TRANSFER_TO_S;
            speed = speed_raw / BYTES_TRANSFER_KBYTES;
        }
        g_instance.comm_cxt.predo_cxt.redoPf.speed_according_seg = speed_raw;
        if (g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr == 0 || module_logging_is_on(MOD_REDO)) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("RedoSpeedDiag: %lu us, redoBytes:%lu,"
                                 "preEndPtr:%lu, readPtr:%lu, endPtr:%lu, speed:%u KB/s, totalTime:%lu",
                                 INSTR_TIME_GET_MICROSEC(tmpTime), redoBytes,
                                 g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr, readPtr, endPtr, speed, totalTime)));
        }
        redo_refresh_stats(speed_raw);
        g_instance.comm_cxt.predo_cxt.redoPf.preEndPtr = endPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.preTime = newTime;
    }
}

/*
 * Get latest redo apply position.
 *
 * Optionally, returns the current recovery target timeline. Callers not
 * interested in that may pass NULL for targetTLI.
 *
 * Exported to allow WALReceiver to read the pointer directly.
 */
XLogRecPtr GetXLogReplayRecPtr(TimeLineID * targetTLI, XLogRecPtr * ReplayReadPtr)
{
    /* use volatile pointer to prevent code rearrangement */
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr recptr;

    SpinLockAcquire(&xlogctl->info_lck);
    recptr = xlogctl->lastReplayedEndRecPtr;
    if (targetTLI != NULL) {
        *targetTLI = xlogctl->RecoveryTargetTLI;
    }
    if (ReplayReadPtr != NULL) {
        *ReplayReadPtr = xlogctl->lastReplayedReadRecPtr;
    }
    SpinLockRelease(&xlogctl->info_lck);

    return recptr;
}

void SetXLogReplayRecPtr(XLogRecPtr readRecPtr, XLogRecPtr endRecPtr)
{
    bool isUpdated = false;
    XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    if (XLByteLT(xlogctl->lastReplayedReadRecPtr, endRecPtr)) {
        xlogctl->lastReplayedReadRecPtr = readRecPtr;
        xlogctl->lastReplayedEndRecPtr = endRecPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.last_replayed_end_ptr = endRecPtr;
        isUpdated = true;
    }
    SpinLockRelease(&xlogctl->info_lck);
    if (isUpdated) {
        RedoSpeedDiag(readRecPtr, endRecPtr);
    }
    update_dirty_page_queue_rec_lsn(readRecPtr);
}

void DumpXlogCtl()
{
    ereport(LOG,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
             errmsg("[REDO_LOG_TRACE]txn : lastReplayedReadRecPtr %lu, lastReplayedEndRecPtr %lu,"
                    " replayEndRecPtr %lu, lastCheckPointRecPtr %lu, lastCheckPoint.redo %lu",
                    t_thrd.shemem_ptr_cxt.XLogCtl->lastReplayedReadRecPtr,
                    t_thrd.shemem_ptr_cxt.XLogCtl->lastReplayedEndRecPtr,
                    t_thrd.shemem_ptr_cxt.XLogCtl->replayEndRecPtr, t_thrd.shemem_ptr_cxt.XLogCtl->lastCheckPointRecPtr,
                    t_thrd.shemem_ptr_cxt.XLogCtl->lastCheckPoint.redo)));
}

/*
 * This function is used for Cluster Manager(CM), when recovery has done
 * it return the current replay record, if recovery is doing, return
 * InvalidXLogRecPtr, so that CM will not use the Replay LSN to arbitrate.
 */
XLogRecPtr GetXLogReplayRecPtrInPending(void)
{
    HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;

    if (PENDING_MODE == hashmdata->current_mode) {
        bool localrecoverydone = false;
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        localrecoverydone = xlogctl->IsRecoveryDone;
        SpinLockRelease(&xlogctl->info_lck);

        if (localrecoverydone == false) {
            return InvalidXLogRecPtr;
        }
    }

    return GetXLogReplayRecPtr(NULL);
}
/*
 * Get current standby flush position, ie, the last WAL position
 * known to be fsync'd to disk in standby.
 *
 * If 'targetTLI' is not NULL, it's set to the current recovery target
 * timeline.
 */
XLogRecPtr GetStandbyFlushRecPtr(TimeLineID * targetTLI)
{
    XLogRecPtr receivePtr;
    XLogRecPtr replayPtr;

    receivePtr = GetWalRcvWriteRecPtr(NULL);
    replayPtr = GetXLogReplayRecPtr(targetTLI);

    if (XLByteLT(receivePtr, replayPtr)) {
        return replayPtr;
    } else {
        return receivePtr;
    }
}

/*
 * Get latest WAL insert pointer
 */
XLogRecPtr GetXLogInsertRecPtr(void)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint64 current_bytepos;

#if defined(__x86_64__) || defined(__aarch64__)
    current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    current_bytepos = Insert->CurrBytePos;
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    return XLogBytePosToRecPtr(current_bytepos);
}

/*
 * Get latest WAL insert pointer.
 * Like GetXLogInsertRecPtr, but if the position is at a page boundary,
 * returns a pointer to the beginning of the page (ie. before page header),
 * not to where the first xlog record on that page would go to.
 */
XLogRecPtr GetXLogInsertEndRecPtr(void)
{
    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    uint64 current_bytepos;

#if defined(__x86_64__) || defined(__aarch64__)
    current_bytepos = pg_atomic_barrier_read_u64((uint64*)&Insert->CurrBytePos);
#else
    SpinLockAcquire(&Insert->insertpos_lck);
    current_bytepos = Insert->CurrBytePos;
    SpinLockRelease(&Insert->insertpos_lck);
#endif

    return XLogBytePosToEndRecPtr(current_bytepos);
}

/*
 * Get latest WAL write pointer
 */
XLogRecPtr GetXLogWriteRecPtr(void)
{
    {
        /* use volatile pointer to prevent code rearrangement */
        XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

        SpinLockAcquire(&xlogctl->info_lck);
        *t_thrd.xlog_cxt.LogwrtResult = xlogctl->LogwrtResult;
        SpinLockRelease(&xlogctl->info_lck);
    }

    return t_thrd.xlog_cxt.LogwrtResult->Write;
}

/*
 * read_backup_label: check to see if a backup_label file is present
 *
 * If we see a backup_label during recovery, we assume that we are recovering
 * from a backup dump file, and we therefore roll forward from the checkpoint
 * identified by the label file, NOT what pg_control says. This avoids the
 * problem that pg_control might have been archived one or more checkpoints
 * later than the start of the dump, and so if we rely on it as the start
 * point, we will fail to restore a consistent database state.
 *
 * Returns TRUE if a backup_label was found (and fills the checkpoint
 * location and its REDO location into *checkPointLoc and RedoStartLSN,
 * respectively); returns FALSE if not. If this backup_label came from a
 * streamed backup, *backupEndRequired is set to TRUE. If this backup_label
 * was created during recovery, *backupFromStandby is set to TRUE. If this backup_label
 * was created during primary mode by gs_roach, *backupFromRoach is set to TRUE.
 */
static bool read_backup_label(XLogRecPtr *checkPointLoc, bool *backupEndRequired, bool *backupFromStandby, bool *backupFromRoach)
{
    char startxlogfilename[MAXFNAMELEN];
    TimeLineID tli;
    FILE *lfp = NULL;
    char ch;
    char backuptype[20];
    char backupfrom[20];
    uint32 hi, lo;

    *backupEndRequired = false;
    *backupFromStandby = false;

    /*
     * See if label file is present
     */
    lfp = AllocateFile(BACKUP_LABEL_FILE, "r");
    if (lfp == NULL) {
        if (errno != ENOENT) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", BACKUP_LABEL_FILE)));
        }
        return false; /* it's not there, all is fine */
    }

    /*
     * Read and parse the START WAL LOCATION and CHECKPOINT lines (this code
     * is pretty crude, but we are not expecting any variability in the file
     * format).
     */
    if (fscanf_s(lfp, "START WAL LOCATION: %X/%X (file %08X%16s)%c", &hi, &lo, &tli, startxlogfilename,
                 sizeof(startxlogfilename), &ch, 1) != 5 ||
        ch != '\n') {
        ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
    }
    t_thrd.xlog_cxt.RedoStartLSN = (((uint64)hi) << 32) | lo;
    g_instance.comm_cxt.predo_cxt.redoPf.redo_start_ptr = t_thrd.xlog_cxt.RedoStartLSN;
    if (fscanf_s(lfp, "CHECKPOINT LOCATION: %X/%X%c", &hi, &lo, &ch, 1) != 3 || ch != '\n') {
        ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("invalid data in file \"%s\"", BACKUP_LABEL_FILE)));
    }
    *checkPointLoc = (((uint64)hi) << 32) | lo;
    /*
     * BACKUP METHOD and BACKUP FROM lines are new in 9.2. We can't restore
     * from an older backup anyway, but since the information on it is not
     * strictly required, don't error out if it's missing for some reason.
     */
    if (fscanf_s(lfp, "BACKUP METHOD: %19s\n", backuptype, sizeof(backuptype)) == 1) {
        if (strcmp(backuptype, "streamed") == 0) {
            *backupEndRequired = true;
        }
    }

    if (fscanf_s(lfp, "BACKUP FROM: %19s\n", backupfrom, sizeof(backupfrom)) == 1) {
        if (strcmp(backupfrom, "standby") == 0) {
            *backupFromStandby = true;
        } else if (strcmp(backupfrom, "roach_master") == 0) {
            *backupFromRoach = true;
        }
    }

    if (ferror(lfp) || FreeFile(lfp)) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", BACKUP_LABEL_FILE)));
    }

    return true;
}

/*
 * read_tablespace_map: check to see if a tablespace_map file is present
 *
 * If we see a tablespace_map file during recovery, we assume that we are
 * recovering from a backup dump file, and we therefore need to create symlinks
 * as per the information present in tablespace_map file.
 *
 * Returns TRUE if a tablespace_map file was found (and fills the link
 * information for all the tablespace links present in file); returns FALSE
 * if not.
 */
static bool read_tablespace_map(List** tablespaces)
{
    tablespaceinfo* ti;
    FILE* lfp;
    char tbsoid[MAXPGPATH];
    char* tbslinkpath;
    char str[MAXPGPATH];
    int ch, prevCh = -1, i = 0, n;

    /*
     * See if tablespace_map file is present
     */
    lfp = AllocateFile(TABLESPACE_MAP, "r");
    if (!lfp) {
        if (errno != ENOENT)
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", TABLESPACE_MAP)));
        return false; /* it's not there, all is fine */
    }

    /*
     * Read and parse the link name and path lines from tablespace_map file
     * (this code is pretty crude, but we are not expecting any variability in
     * the file format).  While taking backup we embed escape character '\\'
     * before newline in tablespace path, so that during reading of
     * tablespace_map file, we could distinguish newline in tablespace path
     * and end of line.  Now while reading tablespace_map file, remove the
     * escape character that has been added in tablespace path during backup.
     */
    while ((ch = fgetc(lfp)) != EOF) {
        if ((ch == '\n' || ch == '\r') && prevCh != '\\') {
            str[i] = '\0';
            if (sscanf_s(str, "%s %n", tbsoid, MAXPGPATH, &n) != 1)
                ereport(FATAL,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("invalid data in file \"%s\"", TABLESPACE_MAP)));
            tbslinkpath = str + n;
            i = 0;

            ti = (tablespaceinfo*)palloc(sizeof(tablespaceinfo));
            ti->oid = pstrdup(tbsoid);
            ti->path = pstrdup(tbslinkpath);

            *tablespaces = lappend(*tablespaces, ti);
            continue;
        } else if ((ch == '\n' || ch == '\r') && prevCh == '\\')
            str[i - 1] = ch;
        else
            str[i++] = ch;
        prevCh = ch;
    }

    if (ferror(lfp) || FreeFile(lfp))
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", TABLESPACE_MAP)));

    return true;
}

/* * Error context callback for errors occurring during rm_redo(). */
void rm_redo_error_callback(void *arg)
{
    XLogReaderState *record = (XLogReaderState *)arg;
    StringInfoData buf;

    initStringInfo(&buf);
    RmgrTable[XLogRecGetRmid(record)].rm_desc(&buf, record);

    errcontext("xlog redo %s", buf.data);

    pfree_ext(buf.data);
}

/*
 * BackupInProgress: check if online backup mode is active
 *
 * This is done by checking for existence of the "backup_label" file.
 */
bool BackupInProgress(void)
{
    struct stat stat_buf;

    return (stat(BACKUP_LABEL_FILE, &stat_buf) == 0);
}

/*
 * CancelBackup: rename the "backup_label" file to cancel backup mode
 *
 * If the "backup_label" file exists, it will be renamed to "backup_label.old".
 * Note that this will render an online backup in progress useless.
 * To correctly finish an online backup, pg_stop_backup must be called.
 */
void CancelBackup(void)
{
    struct stat stat_buf;

    /* if the file is not there, return */
    if (stat(BACKUP_LABEL_FILE, &stat_buf) < 0) {
        return;
    }

    /* remove leftover file from previously canceled backup if it exists */
    unlink(BACKUP_LABEL_OLD);

    if (durable_rename(BACKUP_LABEL_FILE, BACKUP_LABEL_OLD, DEBUG1) == 0) {
        ereport(LOG,
            (errmsg("online backup mode canceled"),
                errdetail("\"%s\" was renamed to \"%s\".", BACKUP_LABEL_FILE, BACKUP_LABEL_OLD)));
        return;
    }

    /* if the tablespace_map file is not there, return */
    if (stat(TABLESPACE_MAP, &stat_buf) < 0) {
        ereport(LOG,
            (errmsg("online backup mode canceled"),
                errdetail("File \"%s\" was renamed to \"%s\".", BACKUP_LABEL_FILE, BACKUP_LABEL_OLD)));
        return;
    }

    /* remove leftover file from previously canceled backup if it exists */
    unlink(TABLESPACE_MAP_OLD);

    if (durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, DEBUG1) == 0) {
        ereport(LOG,
            (errmsg("online backup mode canceled"),
                errdetail("Files \"%s\" and \"%s\" were renamed to "
                          "\"%s\" and \"%s\", respectively.",
                    BACKUP_LABEL_FILE,
                    TABLESPACE_MAP,
                    BACKUP_LABEL_OLD,
                    TABLESPACE_MAP_OLD)));
    } else {
        ereport(WARNING, (errcode_for_file_access(), errmsg("online backup mode was not canceled"),
                          errdetail("Could not rename \"%s\" to \"%s\": %m.", BACKUP_LABEL_FILE, BACKUP_LABEL_OLD)));
    }
}

static bool IsRedoDonePromoting(void)
{
    bool result = false;

    XLogRecPtr redoPos = InvalidXLogRecPtr;
    XLogRecPtr receivePos = InvalidXLogRecPtr;

    if (CheckForFailoverTrigger()) {
        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

        SpinLockAcquire(&walrcv->mutex);
        redoPos = walrcv->receiver_replay_location;
        receivePos = walrcv->receivedUpto;
        SpinLockRelease(&walrcv->mutex);

        result = XLByteEQ(redoPos, receivePos) ? true : false;
    }

    return result;
}

bool XLogReadFromWriteBuffer(XLogRecPtr targetStartPtr, int reqLen, char *readBuf, uint32 *rereadlen)
{
    WalRcvCtlAcquireExitLock();
    WalRcvCtlBlock *walrcb = getCurrentWalRcvCtlBlock();
    const int64 recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    int64 walwriteoffset;
    int64 walReadOffset;
    char *walrecvbuf = NULL;
    char *srcbuf = NULL;
    XLogRecPtr lastreadptr;
    XLogRecPtr startPtr;
    int64 buflen1 = 0;
    int64 buflen2 = 0;
    int64 realLen = reqLen;

    if (walrcb == NULL) {
        WalRcvCtlReleaseExitLock();
        return false;
    }
    SpinLockAcquire(&walrcb->mutex);
    walwriteoffset = walrcb->walWriteOffset;
    walReadOffset = walrcb->walReadOffset;
    walrecvbuf = walrcb->walReceiverBuffer;
    lastreadptr = walrcb->lastReadPtr;
    startPtr = walrcb->walStart;
    SpinLockRelease(&walrcb->mutex);

    if (!XLByteEQ(targetStartPtr, lastreadptr) || XLByteLT(startPtr, lastreadptr + reqLen)) {
        WalRcvCtlReleaseExitLock();
        const uint32 rightShiftSize = 32;
        ereport(WARNING, (errmodule(MOD_REDO), errmsg("XLogReadFromWriteBuffer:"
            "lastreadptr:%X/%X,EndRecPtr:%X/%X,length:%d,reallen:%ld,buf1:%ld,buf2:%ld,writeoffset:%ld,readoffset:%ld",
            (uint32)(lastreadptr >> rightShiftSize), (uint32)(lastreadptr), (uint32)(startPtr >> rightShiftSize),
            (uint32)(startPtr), reqLen, realLen, buflen1, buflen2, walwriteoffset, walReadOffset)));

        pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState), extreme_rto::WORKER_STATE_STOPPING);
        return false;
    }
    
    if (walReadOffset <= walwriteoffset) {
        buflen1 = (walwriteoffset - walReadOffset);
    } else {
        buflen1 = (recBufferSize - walReadOffset);
        buflen2 = walwriteoffset;
    }

    if ((buflen1 + buflen2) < realLen) {
        realLen = buflen1 + buflen2;
    }

    srcbuf = walrecvbuf + walReadOffset;
    errno_t ss_rc = 0;
    if (realLen <= buflen1) {
        ss_rc = memcpy_s(readBuf, XLOG_SEG_SIZE, srcbuf, realLen);
        securec_check(ss_rc, "\0", "\0");
    } else {
        ss_rc = memcpy_s(readBuf, XLOG_SEG_SIZE, srcbuf, buflen1);
        securec_check(ss_rc, "\0", "\0");
        srcbuf = walrecvbuf;
        int64 remain = realLen - buflen1;
        Assert(remain <= buflen2);
        ss_rc = memcpy_s((readBuf + buflen1), (XLOG_SEG_SIZE - buflen1), srcbuf, remain);
        securec_check(ss_rc, "\0", "\0");
    }

    SpinLockAcquire(&walrcb->mutex);
    walrcb->lastReadPtr = lastreadptr + realLen;
    walrcb->walReadOffset = walReadOffset + realLen;

    if (walrcb->walReadOffset == recBufferSize) {
        walrcb->walReadOffset = 0;
        if (walrcb->walWriteOffset == recBufferSize) {
            walrcb->walWriteOffset = 0;
            if (walrcb->walFreeOffset == recBufferSize) {
                walrcb->walFreeOffset = 0;
            }
        }
    }

    SpinLockRelease(&walrcb->mutex);
    WalRcvCtlReleaseExitLock();

    *rereadlen = realLen;
    return true;
}


bool NewDataIsInBuf(XLogRecPtr expectedRecPtr)
{
    bool havedata = false;
    if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
        havedata = true;
    } else {
        XLogRecPtr latestChunkStart;

        t_thrd.xlog_cxt.receivedUpto = GetWalRcvWriteRecPtr(&latestChunkStart);
        if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
            havedata = true;
            if (!XLByteLT(expectedRecPtr, latestChunkStart)) {
                t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
                SetCurrentChunkStartTime(t_thrd.xlog_cxt.XLogReceiptTime);
            }
        } else {
            havedata = false;
        }
    }

    return havedata;
}

void SwitchToReadXlogFromFile(XLogRecPtr pageptr)
{
    pg_atomic_write_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readSource, XLOG_FROM_PG_XLOG);
    pg_atomic_write_u64(&extreme_rto::g_dispatcher->rtoXlogBufState.expectLsn, InvalidXLogRecPtr);
    pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState), extreme_rto::WORKER_STATE_STOPPING);
    uint32 workerState = pg_atomic_read_u32(&(extreme_rto::g_recordbuffer->readWorkerState));
    while (workerState != extreme_rto::WORKER_STATE_EXIT && workerState != extreme_rto::WORKER_STATE_STOP) {
        RedoInterruptCallBack();
        workerState = pg_atomic_read_u32(&(extreme_rto::g_recordbuffer->readWorkerState));
    }
}

static inline XLogRecPtr CalcExpectLsn(XLogRecPtr recPtr)
{
    XLogRecPtr expectedRecPtr = recPtr;
    if (recPtr % XLogSegSize == 0) {
        XLByteAdvance(expectedRecPtr, SizeOfXLogLongPHD);
    } else if (recPtr % XLOG_BLCKSZ == 0) {
        XLByteAdvance(expectedRecPtr, SizeOfXLogShortPHD);
    }
    return expectedRecPtr;
}

bool HasReceivedTrigger()
{
    uint32 trigger = pg_atomic_read_u32(&extreme_rto::g_readManagerTriggerFlag);
    if (trigger > 0) {
        pg_atomic_write_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readSource, XLOG_FROM_PG_XLOG);
        pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState), extreme_rto::WORKER_STATE_STOPPING);
        return true;
    }
    return false;
}

// receivedUpto indicate received new datas, but can not read,we should check
bool IsReceivingStatusOk()
{
    WalRcvCtlBlock *walrcb = getCurrentWalRcvCtlBlock();
    uint32 startreadworker = pg_atomic_read_u32(&(extreme_rto::g_recordbuffer->readWorkerState));
    if (startreadworker == extreme_rto::WORKER_STATE_STOP && walrcb == NULL) {
        return false;
    }
    return true;
}

int ParallelXLogReadWorkBufRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
    XLogRecPtr targetRecPtr, TimeLineID *readTLI)
{
    XLogRecPtr RecPtr = targetPagePtr;
    uint32 targetPageOff = targetPagePtr % XLogSegSize;

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

    XLogRecPtr expectedRecPtr = CalcExpectLsn(RecPtr);

    pg_atomic_write_u64(&extreme_rto::g_dispatcher->rtoXlogBufState.expectLsn, expectedRecPtr);
    for (;;) {
        // Check to see if the trigger file exists. If so, update the gaussdb state file.
        if (CheckForStandbyTrigger()) {
            SendPostmasterSignal(PMSIGNAL_UPDATE_NORMAL);
        }

        /*
         * If we find an invalid record in the WAL streamed from
         * master, something is seriously wrong. There's little
         * chance that the problem will just go away, but PANIC is
         * not good for availability either, especially in hot
         * standby mode. Disconnect, and retry from
         * archive/pg_xlog again. The WAL in the archive should be
         * identical to what was streamed, so it's unlikely that
         * it helps, but one can hope...
         */
        if (t_thrd.xlog_cxt.failedSources & XLOG_FROM_STREAM) {
            pg_atomic_write_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.failSource, XLOG_FROM_STREAM);
            SwitchToReadXlogFromFile(targetPagePtr);
            return -1;
        }

        extreme_rto::ResetRtoXlogReadBuf(targetPagePtr);
        /*
         * Walreceiver is active, so see if new data has arrived.
         *
         * We only advance XLogReceiptTime when we obtain fresh
         * WAL from walreceiver and observe that we had already
         * processed everything before the most recent "chunk"
         * that it flushed to disk.  In steady state where we are
         * keeping up with the incoming data, XLogReceiptTime will
         * be updated on each cycle.  When we are behind,
         * XLogReceiptTime will not advance, so the grace time
         * alloted to conflicting queries will decrease.
         */
        bool havedata = NewDataIsInBuf(expectedRecPtr);
        if (havedata) {
            /* just make sure source info is correct... */
            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
            if ((targetPagePtr / XLOG_BLCKSZ) != (t_thrd.xlog_cxt.receivedUpto / XLOG_BLCKSZ)) {
                t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
            } else {
                t_thrd.xlog_cxt.readLen = t_thrd.xlog_cxt.receivedUpto % XLogSegSize - targetPageOff;
            }

            /*  read from wal writer buffer */
            bool readflag = extreme_rto::XLogPageReadForExtRto(xlogreader, targetPagePtr, t_thrd.xlog_cxt.readLen);
            if (readflag) {
                *readTLI = t_thrd.xlog_cxt.curFileTLI;
                return t_thrd.xlog_cxt.readLen;
            } else {
                if (!IsReceivingStatusOk()) {
                    SwitchToReadXlogFromFile(targetPagePtr);
                    return -1;
                }
            }
        } else {
            if (HasReceivedTrigger()) {
                return -1;
            }

            uint32 waitRedoDone = pg_atomic_read_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.waitRedoDone);
            if (waitRedoDone == 1) {
                SwitchToReadXlogFromFile(targetPagePtr);
                return -1;
            }
            /*
             * Wait for more WAL to arrive, or timeout to be reached
             */
            WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch, WL_LATCH_SET | WL_TIMEOUT, 1000L);
            ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
            extreme_rto::PushToWorkerLsn(false);
        }

        RedoInterruptCallBack();
    }

    return -1;
}

void WaitReplayFinishAfterReadXlogFileComplete(XLogRecPtr lastValidRecordLsn)
{
    Assert(t_thrd.xlog_cxt.EndRecPtr == lastValidRecordLsn);
    XLogRecPtr lastReplayedLsn = GetXLogReplayRecPtr(NULL);

    while (XLByteLT(lastReplayedLsn, lastValidRecordLsn)) {
        RedoInterruptCallBack();
        const long sleepTime = 100;
        pg_usleep(sleepTime);
        lastReplayedLsn = GetXLogReplayRecPtr(NULL);
    }
}

int ParallelXLogPageReadFile(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
    TimeLineID *readTLI)
{
    bool randAccess = false;
    uint32 targetPageOff;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr RecPtr = targetPagePtr;
    uint32 ret;
#ifdef USE_ASSERT_CHECKING
    XLogSegNo targetSegNo;

    XLByteToSeg(targetPagePtr, targetSegNo);
#endif
    targetPageOff = targetPagePtr % XLogSegSize;

    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if (t_thrd.xlog_cxt.readFile >= 0 && !XLByteInSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo)) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        t_thrd.xlog_cxt.readSource = 0;
    }

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

retry:
    /* See if we need to retrieve more data */
    if (t_thrd.xlog_cxt.readFile < 0) {
        if (t_thrd.xlog_cxt.StandbyMode) {
            /*
             * In standby mode, wait for the requested record to become
             * available, either via restore_command succeeding to restore the
             * segment, or via walreceiver having streamed the record.
             */
            for (;;) {
                RedoInterruptCallBack();
                if (t_thrd.xlog_cxt.readFile >= 0) {
                    close(t_thrd.xlog_cxt.readFile);
                    t_thrd.xlog_cxt.readFile = -1;
                }
                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                /*
                 * Try to restore the file from archive, or read an
                 * existing file from pg_xlog.
                 */
                uint32 sources = XLOG_FROM_ARCHIVE | XLOG_FROM_PG_XLOG;
                if (!(sources & ~t_thrd.xlog_cxt.failedSources)) {
                    /*
                     * We've exhausted all options for retrieving the
                     * file. Retry.
                     */
                    t_thrd.xlog_cxt.failedSources = 0;

                    /*
                     * Before we sleep, re-scan for possible new timelines
                     * if we were requested to recover to the latest
                     * timeline.
                     */
                    if (t_thrd.xlog_cxt.recoveryTargetIsLatest) {
                        if (rescanLatestTimeLine()) {
                            continue;
                        }
                    }

                    extreme_rto::PushToWorkerLsn(true);
                    WaitReplayFinishAfterReadXlogFileComplete(t_thrd.xlog_cxt.EndRecPtr);
                    
                    if (!xlogctl->IsRecoveryDone) {
                        g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time = GetCurrentTimestamp();
                        g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr = t_thrd.xlog_cxt.ReadRecPtr;
                    }

                    XLogRecPtr lastReplayedLsn = GetXLogReplayRecPtr(NULL);
                    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                                      errmsg("ParallelXLogPageReadFile IsRecoveryDone is %s set true,"
                                             "ReadRecPtr:%X/%X, EndRecPtr:%X/%X, lastreplayed:%X/%X",
                                             xlogctl->IsRecoveryDone ? "next" : "first",
                                             (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), 
                                             (uint32)(t_thrd.xlog_cxt.ReadRecPtr),
                                             (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32),
                                             (uint32)(t_thrd.xlog_cxt.EndRecPtr),
                                             (uint32)(lastReplayedLsn >> 32),
                                             (uint32)(lastReplayedLsn))));

                    /*
                     * signal postmaster to update local redo end
                     * point to gaussdb state file.
                     */
                    if (!xlogctl->IsRecoveryDone) {
                        SendPostmasterSignal(PMSIGNAL_LOCAL_RECOVERY_DONE);
                    }

                    SpinLockAcquire(&xlogctl->info_lck);
                    xlogctl->IsRecoveryDone = true;
                    SpinLockRelease(&xlogctl->info_lck);
                    knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL | REDO_FINISH_STATUS_CM);
                    /*
                     * If it hasn't been long since last attempt, sleep 1s to
                     * avoid busy-waiting.
                     */
                    pg_usleep(50000L);
                    /*
                     * If primary_conninfo is set, launch walreceiver to
                     * try to stream the missing WAL, before retrying to
                     * restore from archive/pg_xlog.
                     *
                     * If fetching_ckpt is TRUE, RecPtr points to the
                     * initial checkpoint location. In that case, we use
                     * RedoStartLSN as the streaming start position
                     * instead of RecPtr, so that when we later jump
                     * backwards to start redo at RedoStartLSN, we will
                     * have the logs streamed already.
                     */
                    
                    uint32 trigger = pg_atomic_read_u32(&extreme_rto::g_readManagerTriggerFlag);
                    if (trigger > 0) {
                        pg_atomic_write_u32(&extreme_rto::g_readManagerTriggerFlag, extreme_rto::TRIGGER_NORMAL);
                        goto triggered;
                    }
                    
                    load_server_mode();
                    if (t_thrd.xlog_cxt.PrimaryConnInfo || t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                        t_thrd.xlog_cxt.receivedUpto = 0;
                        uint32 failSouce = pg_atomic_read_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.failSource);

                        if (!(failSouce & XLOG_FROM_STREAM)) {
                            volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                            SpinLockAcquire(&walrcv->mutex);
                            walrcv->receivedUpto = 0;
                            SpinLockRelease(&walrcv->mutex);
                            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
                            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
                            pg_atomic_write_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readSource, 
                                XLOG_FROM_STREAM);
                            pg_atomic_write_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.waitRedoDone, 0);
                            return -1;
                        }
                        
                    }
                }
                /* Don't try to read from a source that just failed */
                sources &= ~t_thrd.xlog_cxt.failedSources;
                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, DEBUG2, sources);
                if (t_thrd.xlog_cxt.readFile >= 0) {
                    break;
                }
                /*
                 * Nope, not found in archive and/or pg_xlog.:
                 */
                t_thrd.xlog_cxt.failedSources |= sources;

                /*
                 * Check to see if the trigger file exists. Note that we
                 * do this only after failure, so when you create the
                 * trigger file, we still finish replaying as much as we
                 * can from archive and pg_xlog before failover.
                 */
                uint32 trigger = pg_atomic_read_u32(&extreme_rto::g_readManagerTriggerFlag);
                if (trigger > 0) {
                    pg_atomic_write_u32(&extreme_rto::g_readManagerTriggerFlag, extreme_rto::TRIGGER_NORMAL);
                    goto triggered;
                }
            }
        } else {
            /* In archive or crash recovery. */
            if (t_thrd.xlog_cxt.readFile < 0) {
                uint32 sources;

                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                sources = XLOG_FROM_PG_XLOG;
                if (t_thrd.xlog_cxt.InArchiveRecovery) {
                    sources |= XLOG_FROM_ARCHIVE;
                }

                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, LOG, sources);

                if (t_thrd.xlog_cxt.readFile < 0) {
                    return -1;
                }
            }
        }
    }

    /*
     * At this point, we have the right segment open and if we're streaming we
     * know the requested record is in it.
     */
    Assert(t_thrd.xlog_cxt.readFile != -1);

    /*
     * If the current segment is being streamed from master, calculate how
     * much of the current page we have received already. We know the
     * requested record has been received, but this is for the benefit of
     * future calls, to allow quick exit at the top of this function.
     */
    t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;

    /* Read the requested page */
    t_thrd.xlog_cxt.readOff = targetPageOff;

try_again:
    if (lseek(t_thrd.xlog_cxt.readFile, (off_t)t_thrd.xlog_cxt.readOff, SEEK_SET) < 0) {
        ereport(emode_for_corrupt_record(LOG, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not seek in log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    pgstat_report_waitevent(WAIT_EVENT_WAL_READ);
    ret = read(t_thrd.xlog_cxt.readFile, xlogreader->readBuf, XLOG_BLCKSZ);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (ret != XLOG_BLCKSZ) {
        ereport(emode_for_corrupt_record(LOG, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not read from log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    Assert(targetSegNo == t_thrd.xlog_cxt.readSegNo);
    Assert(targetPageOff == t_thrd.xlog_cxt.readOff);
    Assert((uint32)reqLen <= t_thrd.xlog_cxt.readLen);

    *readTLI = t_thrd.xlog_cxt.curFileTLI;

    return t_thrd.xlog_cxt.readLen;

next_record_is_invalid:
    t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;

    /* In standby-mode, keep trying */
    if (t_thrd.xlog_cxt.StandbyMode) {
        goto retry;
    } else {
        return -1;
    }

triggered:
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;
    t_thrd.xlog_cxt.recoveryTriggered = true;

    return -1;
}

int ParallelXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
    TimeLineID *readTLI)
{
    int readLen = -1;
    pg_atomic_write_u64(&extreme_rto::g_dispatcher->rtoXlogBufState.targetRecPtr, targetRecPtr);
    xlogreader->readBuf = extreme_rto::g_dispatcher->rtoXlogBufState.readBuf;

    for (;;) {
        uint32 readSource = pg_atomic_read_u32(&(extreme_rto::g_recordbuffer->readSource));
        if (readSource & XLOG_FROM_STREAM) {
            readLen = ParallelXLogReadWorkBufRead(xlogreader, targetPagePtr, reqLen, targetRecPtr, readTLI);
        } else {
            readLen = ParallelXLogPageReadFile(xlogreader, targetPagePtr, reqLen, targetRecPtr, readTLI);
        }

        if (readLen > 0 || t_thrd.xlog_cxt.recoveryTriggered || !t_thrd.xlog_cxt.StandbyMode) {
            return readLen;
        }

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(10);
    }

    return readLen;
}

static ReplConnTarget GetRepConntarget(void)
{
    if (t_thrd.xlog_cxt.is_cascade_standby) {
        return REPCONNTARGET_STANDBY;
    } else {
        return REPCONNTARGET_PRIMARY;
    }
}

static void HandleCascadeStandbyPromote(XLogRecPtr* recptr)
{
    if (!t_thrd.xlog_cxt.is_cascade_standby ||
        t_thrd.xlog_cxt.server_mode != STANDBY_MODE ||
        !IS_DN_MULTI_STANDYS_MODE()) {
        return;
    }

    ShutdownWalRcv();

    /* switchover */
    volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    t_thrd.xlog_cxt.receivedUpto = 0;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->receivedUpto = 0;
    SpinLockRelease(&walrcv->mutex);
    t_thrd.xlog_cxt.is_cascade_standby = false;
    t_thrd.xlog_cxt.switchover_triggered = false;

    if (IsExtremeRtoRunning()) {
        t_thrd.xlog_cxt.readfrombuffer = false;
        /* restart from recvbuffer */
        pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState),
                            extreme_rto::WORKER_STATE_STOP);
    }

    /* failover */
    ResetFailoverTriggered();
    t_thrd.xlog_cxt.failover_triggered = false;

    SendPostmasterSignal(PMSIGNAL_UPDATE_NORMAL);

    /* request postmaster to start walreceiver again. */
    RequestXLogStreaming(recptr, t_thrd.xlog_cxt.PrimaryConnInfo,
                         GetRepConntarget(),
                         u_sess->attr.attr_storage.PrimarySlotName);
}

void CheckMaxPageFlushLSN(XLogRecPtr reqLsn)
{
    /* if recovery is done and force finish is enabled, update max_page_flush_lsn so */
    /* that force finish really starts to work from now on. */
    if (force_finish_enabled() && t_thrd.xlog_cxt.max_page_flush_lsn == MAX_XLOG_REC_PTR) {
        t_thrd.xlog_cxt.max_page_flush_lsn = reqLsn;
        ereport(LOG,
            (errmsg("update max_page_flush_lsn to %X/%X for standby before request streaming",
            (uint32) (t_thrd.xlog_cxt.max_page_flush_lsn >> 32),
            (uint32) (t_thrd.xlog_cxt.max_page_flush_lsn))));
        update_max_page_flush_lsn(reqLsn, t_thrd.proc_cxt.MyProcPid, true);
        set_global_max_page_flush_lsn(reqLsn);
    }
}

/*
 * Read the XLOG page containing RecPtr into readBuf (if not read already).
 * Returns number of bytes read, if the page is read successfully, or -1
 * in case of errors.  When errors occur, they are ereport'ed, but only
 * if they have not been previously reported.
 *
 * This is responsible for restoring files from archive as needed, as well
 * as for waiting for the requested WAL record to arrive in standby mode.
 *
 * 'emode' specifies the log level used for reporting "file not found" or
 * "end of WAL" situations in archive recovery, or in standby mode when a
 * trigger file is found. If set to WARNING or below, XLogPageRead() returns
 * false in those situations, on higher log levels the ereport() won't
 * return.
 *
 * In standby mode, if after a successful return of XLogPageRead() the
 * caller finds the record it's interested in to be broken, it should
 * ereport the error with the level determined by
 * emode_for_corrupt_record(), and then set "failedSources |= readSource"
 * and call XLogPageRead() again with the same arguments. This lets
 * XLogPageRead() to try fetching the record from another source, or to
 * sleep and retry.
 *
 * In standby mode, this only returns false if promotion has been triggered.
 * Otherwise it keeps sleeping and retrying indefinitely.
 */
int XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                 char *readBuf, TimeLineID *readTLI)
{
    /* Load reader private data */
    XLogPageReadPrivate *readprivate = (XLogPageReadPrivate *)xlogreader->private_data;
    int emode = readprivate->emode;
    bool randAccess = readprivate->randAccess;
    bool fetching_ckpt = readprivate->fetching_ckpt;
    uint32 targetPageOff;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr RecPtr = targetPagePtr;
    bool processtrxn = false;
    XLogSegNo replayedSegNo;
    uint32 ret;
#ifdef USE_ASSERT_CHECKING
    XLogSegNo targetSegNo;

    XLByteToSeg(targetPagePtr, targetSegNo);
#endif
    targetPageOff = targetPagePtr % XLogSegSize;

    load_server_mode();

    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if (t_thrd.xlog_cxt.readFile >= 0 && !XLByteInSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo)) {
        /*
         * Request a restartpoint if we've replayed too much xlog since the
         * last one.
         */
        if (t_thrd.xlog_cxt.StandbyModeRequested &&
            (t_thrd.xlog_cxt.bgwriterLaunched || t_thrd.xlog_cxt.pagewriter_launched) && !dummyStandbyMode) {
            if (get_real_recovery_parallelism() > 1) {
                XLByteToSeg(GetXLogReplayRecPtr(NULL), replayedSegNo);
            } else {
                replayedSegNo = t_thrd.xlog_cxt.readSegNo;
            }
            if (XLogCheckpointNeeded(replayedSegNo)) {
                (void)GetRedoRecPtr();
                if (XLogCheckpointNeeded(replayedSegNo)) {
                    RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
                }
            }
        }

        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        t_thrd.xlog_cxt.readSource = 0;
    }

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

retry:
    /* See if we need to retrieve more data */
    if (t_thrd.xlog_cxt.readFile < 0 ||
        (t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM && XLByteLT(t_thrd.xlog_cxt.receivedUpto, RecPtr))) {
        if (t_thrd.xlog_cxt.StandbyMode && t_thrd.xlog_cxt.startup_processing && !dummyStandbyMode) {
            /*
             * In standby mode, wait for the requested record to become
             * available, either via restore_command succeeding to restore the
             * segment, or via walreceiver having streamed the record.
             */
            for (;;) {
                /*
                 * Need to check here also for the case where consistency level is
                 * already reached without replaying any record i.e. just after reading
                 * of checkpoint data it has reached to minRecoveryPoint. Also
                 * whenever we are going to loop in the data receive from master
                 * node its bettwe we check if consistency level has reached. So
                 * instead of keeping in all places before ReadRecord, we can keep
                 * here in centralised location.
                 */
                ProcTxnWorkLoad(false);

                CheckRecoveryConsistency();
                if (WalRcvInProgress()) {
                    XLogRecPtr expectedRecPtr = RecPtr;
                    bool havedata = false;

                    // Check to see if the trigger file exists. If so, update the gaussdb state file.
                    if (CheckForStandbyTrigger()) {
                        SendPostmasterSignal(PMSIGNAL_UPDATE_NORMAL);
                    }

                    /*
                     * If we find an invalid record in the WAL streamed from
                     * master, something is seriously wrong. There's little
                     * chance that the problem will just go away, but PANIC is
                     * not good for availability either, especially in hot
                     * standby mode. Disconnect, and retry from
                     * archive/pg_xlog again. The WAL in the archive should be
                     * identical to what was streamed, so it's unlikely that
                     * it helps, but one can hope...
                     */
                    if (t_thrd.xlog_cxt.failedSources & XLOG_FROM_STREAM) {
                        ProcTxnWorkLoad(true);
                        if (IsExtremeRtoRunning()) {
                            t_thrd.xlog_cxt.readfrombuffer = false;
                            pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState),
                                                extreme_rto::WORKER_STATE_STOP);
                        }
                        ShutdownWalRcv();
                        continue;
                    }

                    if (!g_instance.attr.attr_storage.enable_mix_replication &&
                        !IS_DN_MULTI_STANDYS_MODE() && !getObsReplicationSlot()) {
                        /* Startup Data Streaming if none */
                        StartupDataStreaming();
                    }

                    /*
                     * Walreceiver is active, so see if new data has arrived.
                     *
                     * We only advance XLogReceiptTime when we obtain fresh
                     * WAL from walreceiver and observe that we had already
                     * processed everything before the most recent "chunk"
                     * that it flushed to disk.  In steady state where we are
                     * keeping up with the incoming data, XLogReceiptTime will
                     * be updated on each cycle.  When we are behind,
                     * XLogReceiptTime will not advance, so the grace time
                     * alloted to conflicting queries will decrease.
                     */
                    if (RecPtr % XLogSegSize == 0) {
                        XLByteAdvance(expectedRecPtr, SizeOfXLogLongPHD);
                    } else if (RecPtr % XLOG_BLCKSZ == 0) {
                        XLByteAdvance(expectedRecPtr, SizeOfXLogShortPHD);
                    }

                    if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                        havedata = true;
                    } else {
                        XLogRecPtr latestChunkStart;

                        t_thrd.xlog_cxt.receivedUpto = GetWalRcvWriteRecPtr(&latestChunkStart);
                        if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                            havedata = true;
                            if (!XLByteLT(RecPtr, latestChunkStart)) {
                                t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
                                SetCurrentChunkStartTime(t_thrd.xlog_cxt.XLogReceiptTime);
                            }
                        } else {
                            havedata = false;
                        }
                    }
                    if (havedata) {
                        /*
                         * Great, streamed far enough. Open the file if it's
                         * not open already.  Use XLOG_FROM_STREAM so that
                         * source info is set correctly and XLogReceiptTime
                         * isn't changed.
                         */
                        if (t_thrd.xlog_cxt.readFile < 0) {
                            t_thrd.xlog_cxt.readFile = XLogFileRead(t_thrd.xlog_cxt.readSegNo, PANIC,
                                                                    t_thrd.xlog_cxt.recoveryTargetTLI, XLOG_FROM_STREAM,
                                                                    false);
                            Assert(t_thrd.xlog_cxt.readFile >= 0);
                        } else {
                            /* just make sure source info is correct... */
                            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
                            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
                        }
                        if (IsExtremeRtoRunning() && (!g_instance.attr.attr_storage.enable_mix_replication)) {
                            t_thrd.xlog_cxt.readfrombuffer = true;
                        }
                        break;
                    }

                    t_thrd.xlog_cxt.RedoDone = IsRedoDonePromoting();
                    pg_memory_barrier();

                    /*
                     * The judging rules are the following:
                     * (1)For the One-Primary-Multi-Standbys deployment the dummy_status is set
                     * to be true which means no any dummy standby synchronization info need to be noticed;
                     * (2)For the Primary-Standby-DummyStandby deployment the dummy standby synchronization
                     * info need to be judged depending on enable_mix_replication.
                     */
                    bool dummy_status = RecoveryFromDummyStandby()
                                            ? (GetWalRcvDummyStandbySyncPercent() == SYNC_DUMMY_STANDBY_END &&
                                               (g_instance.attr.attr_storage.enable_mix_replication
                                                    ? true
                                                    : GetDataRcvDummyStandbySyncPercent() == SYNC_DUMMY_STANDBY_END))
                                            : true;

                    /* make sure all xlog from dummystandby fulshed to disk */
                    if (WalRcvIsDone() && CheckForFailoverTrigger() && dummy_status) {
                        t_thrd.xlog_cxt.receivedUpto = GetWalRcvWriteRecPtr(NULL);
                        if (XLByteLT(RecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                            /* wait xlog redo done */
                            continue;
                        }

                        ProcTxnWorkLoad(true);
                        /* use volatile pointer to prevent code rearrangement */
                        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                        SpinLockAcquire(&walrcv->mutex);
                        walrcv->dummyStandbyConnectFailed = false;
                        SpinLockRelease(&walrcv->mutex);

                        ereport(LOG, (errmsg("RecPtr(%X/%X),receivedUpto(%X/%X)", (uint32)(RecPtr >> 32),
                                             (uint32)RecPtr, (uint32)(t_thrd.xlog_cxt.receivedUpto >> 32),
                                             (uint32)t_thrd.xlog_cxt.receivedUpto)));

                        if (t_thrd.xlog_cxt.is_cascade_standby) {
                            ereport(LOG,
                                (errmsg("Cascade Standby has synchronized all the xlog and replication data, "
                                        "promote to standby")));

                        } else {
                            ereport(LOG,
                                (errmsg("Secondary Standby has synchronized all the xlog and replication data, standby "
                                        "promote to primary")));
                        }

                        ShutdownWalRcv();
                        ShutdownDataRcv();

                        if (t_thrd.xlog_cxt.is_cascade_standby) {
                            HandleCascadeStandbyPromote(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr);
                            continue;
                        } else {
                            goto triggered;
                        }
                    }

                    /*
                     * Data not here yet, so check for trigger then sleep for
                     * five seconds like in the WAL file polling case below.
                     */
                    if (WalRcvIsDone() && (CheckForSwitchoverTrigger() || CheckForFailoverTrigger())) {
                        if (t_thrd.xlog_cxt.is_cascade_standby && t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                            HandleCascadeStandbyPromote(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr);
                            continue;
                        }
                        goto retry;
                    }
                    if (!processtrxn) {
                        ProcTxnWorkLoad(true);
                        processtrxn = true;
                        goto retry;
                    }
                    /*
                     * Wait for more WAL to arrive, or timeout to be reached
                     */
                    WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch, WL_LATCH_SET | WL_TIMEOUT, 5000L);
                    processtrxn = false;
                    ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
                } else {
                    if (t_thrd.xlog_cxt.readfrombuffer) {
                        XLogRecPtr expectedRecPtr = RecPtr;
                        if (RecPtr % XLogSegSize == 0) {
                            XLByteAdvance(expectedRecPtr, SizeOfXLogLongPHD);
                        } else if (RecPtr % XLOG_BLCKSZ == 0) {
                            XLByteAdvance(expectedRecPtr, SizeOfXLogShortPHD);
                        }
                        t_thrd.xlog_cxt.receivedUpto = GetWalRcvWriteRecPtr(NULL);
                        if (!XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                            t_thrd.xlog_cxt.readfrombuffer = false;
                            if (t_thrd.xlog_cxt.readFile >= 0) {
                                close(t_thrd.xlog_cxt.readFile);
                                t_thrd.xlog_cxt.readFile = -1;
                            }
                            goto retry;
                        } else {
                            break;
                        }
                    }

                    uint32 sources;

                    /*
                     * Until walreceiver manages to reconnect, poll the
                     * archive.
                     */
                    if (t_thrd.xlog_cxt.readFile >= 0) {
                        close(t_thrd.xlog_cxt.readFile);
                        t_thrd.xlog_cxt.readFile = -1;
                    }
                    /* Reset curFileTLI if random fetch. */
                    if (randAccess) {
                        t_thrd.xlog_cxt.curFileTLI = 0;
                    }

                    /*
                     * Try to restore the file from archive, or read an
                     * existing file from pg_xlog.
                     */
                    sources = XLOG_FROM_ARCHIVE | XLOG_FROM_PG_XLOG;
                    ereport(DEBUG5, (errmsg("failedSources: %u", t_thrd.xlog_cxt.failedSources)));
                    if (!(sources & ~t_thrd.xlog_cxt.failedSources)) {
                        /*
                         * We've exhausted all options for retrieving the
                         * file. Retry.
                         */
                        t_thrd.xlog_cxt.failedSources = 0;

                        /*
                         * Before we sleep, re-scan for possible new timelines
                         * if we were requested to recover to the latest
                         * timeline.
                         */
                        if (t_thrd.xlog_cxt.recoveryTargetIsLatest) {
                            if (rescanLatestTimeLine()) {
                                continue;
                            }
                        }

                        if (t_thrd.startup_cxt.shutdown_requested) {
                            ereport(LOG, (errmsg("startup shutdown")));
                            proc_exit(0);
                        }

                        if (IsExtremeRtoSmartShutdown()) {
                            ExtremeRtoRedoManagerSendEndToStartup();
                        }

                        if (!xlogctl->IsRecoveryDone) {
                            g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time = GetCurrentTimestamp();
                            g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr = t_thrd.xlog_cxt.ReadRecPtr;
                            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                                          errmsg("XLogPageRead IsRecoveryDone is set true,"
                                                 "ReadRecPtr:%X/%X, EndRecPtr:%X/%X",
                                                 (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), 
                                                 (uint32)(t_thrd.xlog_cxt.ReadRecPtr),
                                                 (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32),
                                                 (uint32)(t_thrd.xlog_cxt.EndRecPtr))));
                            parallel_recovery::redo_dump_all_stats();
                        }

                        /*
                         * signal postmaster to update local redo end
                         * point to gaussdb state file.
                         */
                        ProcTxnWorkLoad(true);
                        if (!xlogctl->IsRecoveryDone) {
                            SendPostmasterSignal(PMSIGNAL_LOCAL_RECOVERY_DONE);
                        }

                        SpinLockAcquire(&xlogctl->info_lck);
                        xlogctl->IsRecoveryDone = true;
                        SpinLockRelease(&xlogctl->info_lck);
                        knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL | REDO_FINISH_STATUS_CM);
                        /*
                         * If it hasn't been long since last attempt, sleep 1s to
                         * avoid busy-waiting.
                         */
                        pg_usleep(150000L);
                        /*
                         * If primary_conninfo is set, launch walreceiver to
                         * try to stream the missing WAL, before retrying to
                         * restore from archive/pg_xlog.
                         *
                         * If fetching_ckpt is TRUE, RecPtr points to the
                         * initial checkpoint location. In that case, we use
                         * RedoStartLSN as the streaming start position
                         * instead of RecPtr, so that when we later jump
                         * backwards to start redo at RedoStartLSN, we will
                         * have the logs streamed already.
                         */
                        load_server_mode();

                        /* Get xlog and data from DummyStandby */
                        if (CheckForFailoverTrigger()) {
                            if (t_thrd.xlog_cxt.is_cascade_standby) {
                                HandleCascadeStandbyPromote(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr);
                                continue;
                            }

                            if (!RecoveryFromDummyStandby()) {
                                goto triggered;
                            }

                            ereport(LOG, (errmsg("Standby connect to DummyStandby begin ......")));
                            if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                                /* use volatile pointer to prevent code rearrangement */
                                volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

                                /* request xlog from secondary standby */
                                ereport(LOG, (errmsg("request xlog stream at %X/%X.",
                                                     fetching_ckpt ? (uint32)(t_thrd.xlog_cxt.RedoStartLSN >> 32)
                                                                    : (uint32)(targetRecPtr >> 32),
                                                     fetching_ckpt ? (uint32)t_thrd.xlog_cxt.RedoStartLSN
                                                                    : (uint32)targetRecPtr)));
                                ShutdownWalRcv();
                                t_thrd.xlog_cxt.receivedUpto = 0;
                                SpinLockAcquire(&walrcv->mutex);
                                walrcv->receivedUpto = 0;
                                SpinLockRelease(&walrcv->mutex);

                                RequestXLogStreaming(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr,
                                                     t_thrd.xlog_cxt.PrimaryConnInfo, REPCONNTARGET_DUMMYSTANDBY,
                                                     u_sess->attr.attr_storage.PrimarySlotName);
                                if (!g_instance.attr.attr_storage.enable_mix_replication &&
                                    !IS_DN_MULTI_STANDYS_MODE()) {
                                    StartupDataStreaming();
                                }
                                continue;
                            }
                        }else if (IS_DISASTER_RECOVER_MODE) {
                            ProcTxnWorkLoad(false);
                            /* use volatile pointer to prevent code rearrangement */
                            volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

                            ereport(LOG, (errmsg("request xlog stream from obs at %X/%X.",
                                                 fetching_ckpt ? (uint32)(t_thrd.xlog_cxt.RedoStartLSN >> 32)
                                                                : (uint32)(targetRecPtr >> 32),
                                                 fetching_ckpt ? (uint32)t_thrd.xlog_cxt.RedoStartLSN
                                                                : (uint32)targetRecPtr)));
                            ShutdownWalRcv();
                            t_thrd.xlog_cxt.receivedUpto = 0;
                            SpinLockAcquire(&walrcv->mutex);
                            walrcv->receivedUpto = 0;
                            SpinLockRelease(&walrcv->mutex);
                            if (IsExtremeRtoRunning()) {
                                /* restart from recvbuffer */
                                Assert(false);
                            }
                            RequestXLogStreaming(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr,
                                                0, REPCONNTARGET_OBS, 0);
                            continue;
                        } else if (t_thrd.xlog_cxt.PrimaryConnInfo || t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                            ProcTxnWorkLoad(false);
                            /* use volatile pointer to prevent code rearrangement */
                            volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                            CheckMaxPageFlushLSN(targetRecPtr);
                            ereport(LOG, (errmsg("request xlog stream at %X/%X.",
                                                 fetching_ckpt ? (uint32)(t_thrd.xlog_cxt.RedoStartLSN >> 32)
                                                                : (uint32)(targetRecPtr >> 32),
                                                 fetching_ckpt ? (uint32)t_thrd.xlog_cxt.RedoStartLSN
                                                                : (uint32)targetRecPtr)));
                            ShutdownWalRcv();
                            t_thrd.xlog_cxt.receivedUpto = 0;
                            SpinLockAcquire(&walrcv->mutex);
                            walrcv->receivedUpto = 0;
                            SpinLockRelease(&walrcv->mutex);

                            RequestXLogStreaming(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr,
                                                 t_thrd.xlog_cxt.PrimaryConnInfo, REPCONNTARGET_PRIMARY,
                                                 u_sess->attr.attr_storage.PrimarySlotName);
                            if (!g_instance.attr.attr_storage.enable_mix_replication && !IS_DN_MULTI_STANDYS_MODE()) {
                                StartupDataStreaming();
                            }
                            continue;
                        }

                    }
                    /* Don't try to read from a source that just failed */
                    sources &= ~t_thrd.xlog_cxt.failedSources;
                    t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, DEBUG2, sources);
                    if (t_thrd.xlog_cxt.readFile >= 0) {
                        break;
                    }

                    ereport(DEBUG5, (errmsg("do not find any more files.sources=%u failedSources=%u", sources,
                                            t_thrd.xlog_cxt.failedSources)));
                    /*
                     * Nope, not found in archive and/or pg_xlog.
                     */
                    t_thrd.xlog_cxt.failedSources |= sources;

                    /*
                     * Check to see if the trigger file exists. Note that we
                     * do this only after failure, so when you create the
                     * trigger file, we still finish replaying as much as we
                     * can from archive and pg_xlog before failover.
                     */
                    if (CheckForPrimaryTrigger()) {
                        goto triggered;
                    }

                    if ((CheckForFailoverTrigger() && !RecoveryFromDummyStandby()) || CheckForSwitchoverTrigger()) {
                        if (t_thrd.xlog_cxt.is_cascade_standby && t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                            HandleCascadeStandbyPromote(fetching_ckpt ? &t_thrd.xlog_cxt.RedoStartLSN : &targetRecPtr);
                            continue;
                        }
                        ereport(LOG,
                                (errmsg("read record failed when promoting, current lsn (%X/%X), received lsn(%X/%X),"
                                        "sources[%u], failedSources[%u], readSource[%u], readFile[%d], readId[%u],"
                                        "readSeg[%u], readOff[%u], readLen[%u]",
                                        (uint32)(RecPtr >> 32), (uint32)RecPtr,
                                        (uint32)(t_thrd.xlog_cxt.receivedUpto >> 32),
                                        (uint32)t_thrd.xlog_cxt.receivedUpto, sources, t_thrd.xlog_cxt.failedSources,
                                        t_thrd.xlog_cxt.readSource, t_thrd.xlog_cxt.readFile,
                                        (uint32)(t_thrd.xlog_cxt.readSegNo >> 32), (uint32)t_thrd.xlog_cxt.readSegNo,
                                        t_thrd.xlog_cxt.readOff, t_thrd.xlog_cxt.readLen)));
                        goto triggered;
                    }
                }

                /*
                 * This possibly-long loop needs to handle interrupts of
                 * startup process.
                 */
                RedoInterruptCallBack();
            }
        } else {
            /* In archive or crash recovery. */
            if (t_thrd.xlog_cxt.readFile < 0) {
                uint32 sources;

                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                sources = XLOG_FROM_PG_XLOG;
                if (t_thrd.xlog_cxt.InArchiveRecovery) {
                    sources |= XLOG_FROM_ARCHIVE;
                }

                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, emode, sources);
                if (dummyStandbyMode) {
                    ereport(LOG,
                            (errmsg("readseg:%lu, readFile:%d", t_thrd.xlog_cxt.readSegNo, t_thrd.xlog_cxt.readFile)));
                }

                if (t_thrd.xlog_cxt.readFile < 0) {
                    return -1;
                }
            }
        }
    }

    /*
     * At this point, we have the right segment open and if we're streaming we
     * know the requested record is in it.
     */
    if (!t_thrd.xlog_cxt.readfrombuffer)
        Assert(t_thrd.xlog_cxt.readFile != -1);

    /*
     * If the current segment is being streamed from master, calculate how
     * much of the current page we have received already. We know the
     * requested record has been received, but this is for the benefit of
     * future calls, to allow quick exit at the top of this function.
     */
    if (t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM || (IsExtremeRtoRunning() && t_thrd.xlog_cxt.readfrombuffer)) {
        if ((targetPagePtr / XLOG_BLCKSZ) != (t_thrd.xlog_cxt.receivedUpto / XLOG_BLCKSZ)) {
            t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
        } else {
            t_thrd.xlog_cxt.readLen = t_thrd.xlog_cxt.receivedUpto % XLogSegSize - targetPageOff;
        }
    } else {
        t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
    }

    /* Read the requested page */
    t_thrd.xlog_cxt.readOff = targetPageOff;

try_again:
    if (lseek(t_thrd.xlog_cxt.readFile, (off_t)t_thrd.xlog_cxt.readOff, SEEK_SET) < 0) {
        ereport(emode_for_corrupt_record(emode, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not seek in log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    pgstat_report_waitevent(WAIT_EVENT_WAL_READ);
    ret = read(t_thrd.xlog_cxt.readFile, readBuf, XLOG_BLCKSZ);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (ret != XLOG_BLCKSZ) {
        ereport(emode_for_corrupt_record(emode, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not read from log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    Assert(targetSegNo == t_thrd.xlog_cxt.readSegNo);
    Assert(targetPageOff == t_thrd.xlog_cxt.readOff);
    Assert((uint32)reqLen <= t_thrd.xlog_cxt.readLen);

    *readTLI = t_thrd.xlog_cxt.curFileTLI;

    return t_thrd.xlog_cxt.readLen;

next_record_is_invalid:
    t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;

    /* In standby-mode, keep trying */
    if (t_thrd.xlog_cxt.StandbyMode) {
        goto retry;
    } else {
        return -1;
    }

triggered:
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;
    t_thrd.xlog_cxt.recoveryTriggered = true;

    return -1;
}

/*
 * Determine what log level should be used to report a corrupt WAL record
 * in the current WAL page, previously read by XLogPageRead().
 *
 * 'emode' is the error mode that would be used to report a file-not-found
 * or legitimate end-of-WAL situation.	 Generally, we use it as-is, but if
 * we're retrying the exact same record that we've tried previously, only
 * complain the first time to keep the noise down.	However, we only do when
 * reading from pg_xlog, because we don't expect any invalid records in archive
 * or in records streamed from master. Files in the archive should be complete,
 * and we should never hit the end of WAL because we stop and wait for more WAL
 * to arrive before replaying it.
 *
 * NOTE: This function remembers the RecPtr value it was last called with,
 * to suppress repeated messages about the same record. Only call this when
 * you are about to ereport(), or you might cause a later message to be
 * erroneously suppressed.
 */
static int emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)
{
    if (t_thrd.xlog_cxt.readSource == XLOG_FROM_PG_XLOG && emode == LOG) {
        if (XLByteEQ(RecPtr, t_thrd.xlog_cxt.lastComplaint)) {
            emode = DEBUG1;
        } else {
            t_thrd.xlog_cxt.lastComplaint = RecPtr;
        }
    }
    return emode;
}

/*
 * Check to see whether the user-specified trigger file exists and whether a
 * failover request has arrived.  If either condition holds, request postmaster
 * to shut down walreceiver, wait for it to exit, and return true.
 */
static bool CheckForFailoverTrigger(void)
{
    struct stat stat_buf;
    StringInfo slotname = NULL;
    bool ret = false;

    if (t_thrd.xlog_cxt.server_mode != STANDBY_MODE) {
        ResetFailoverTriggered();
        t_thrd.xlog_cxt.failover_triggered = false;
        ret = false;
        goto exit;
    }

    if (t_thrd.xlog_cxt.failover_triggered) {
        ret = true;
        goto exit;
    }

    slotname = get_rcv_slot_name();
    Assert(slotname != NULL);

    if (IsFailoverTriggered()) {
        ereport(LOG, (errmsg("received failover notification")));
        ShutdownWalRcv();
        ShutdownDataRcv();
        ResetFailoverTriggered();
        t_thrd.xlog_cxt.failover_triggered = true;

        /*
         * If database has a mass of bcm page, clear bcm page maybe very slowly,
         * so we do not clear it.
         * getBcmFileList(true);
         */
        ResetSlotLSNEndRecovery(slotname);

        SendPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING);
        ret = true;
        goto exit;
    }

    if (t_thrd.xlog_cxt.TriggerFile == NULL) {
        ret = false;
        goto exit;
    }

    if (stat(t_thrd.xlog_cxt.TriggerFile, &stat_buf) == 0) {
        ereport(LOG, (errmsg("trigger file found: %s", t_thrd.xlog_cxt.TriggerFile)));
        ShutdownWalRcv();
        ShutdownDataRcv();
        unlink(t_thrd.xlog_cxt.TriggerFile);
        t_thrd.xlog_cxt.failover_triggered = true;

        /*
         * If database has a mass of bcm page, clear bcm page maybe very slowly,
         * so we do not clear it.
         * getBcmFileList(true);
         */
        ResetSlotLSNEndRecovery(slotname);

        ret = true;
    }
exit:
    if (slotname != NULL) {
        if (slotname->data) {
            pfree_ext(slotname->data);
        }
        pfree_ext(slotname);
    }
    return ret;
}

bool CheckForForceFinishRedoTrigger(TermFileData *term_file)
{
    uint32 permit_finish_redo = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.permitFinishRedo));
    uint32 hot_standby = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.hotStdby));
    if (hot_standby == ATOMIC_TRUE && permit_finish_redo == ATOMIC_TRUE && force_finish_enabled()) {
        *term_file = GetTermFileDataAndClear();
        return term_file->finish_redo;
    } else {
        return false;
    }
}

/*
 * Check to see whether the user-specified trigger file exists and whether a
 * switchover request has arrived.  If either condition holds, request postmaster
 * to shut down walreceiver, wait for it to exit, and return true.
 */
static bool CheckForSwitchoverTrigger(void)
{
    if (t_thrd.xlog_cxt.switchover_triggered) {
        return true;
    }

    if (IsSwitchoverTriggered()) {
        StringInfo slotname = NULL;

        ereport(LOG, (errmsg("received switchover notification")));

        slotname = get_rcv_slot_name();
        Assert(slotname != NULL);

        ShutdownWalRcv();
        ShutdownDataRcv();
        ResetSwitchoverTriggered();
        t_thrd.xlog_cxt.switchover_triggered = true;

        /*
         * If database has a mass of bcm page, clear bcm page maybe very slowly,
         * so we do not clear it.
         * getBcmFileList(true);
         */
        ResetSlotLSNEndRecovery(slotname);

        if (slotname->data != NULL) {
            pfree_ext(slotname->data);
        }
        pfree_ext(slotname);

        SendPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING);
        return true;
    }

    return false;
}

static bool CheckForPrimaryTrigger(void)
{
    if (AmStartupProcess()) {
        if (IsPrimaryTriggered()) {
            ereport(LOG, (errmsg("received primary request")));

            ResetPrimaryTriggered();
            return true;
        }
        return false;
    } else {
        /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
        if (tgigger == extreme_rto::TRIGGER_PRIMARY) {
            (void)pg_atomic_compare_exchange_u32(&(extreme_rto::g_startupTriggerState), &tgigger, 
                extreme_rto::TRIGGER_NORMAL);
            return true;
        }
    }
    
    return false;
}

static bool CheckForStandbyTrigger(void)
{
    if (AmStartupProcess()) {
        if (IsStandbyTriggered()) {
            ereport(LOG, (errmsg("received standby request")));

            ResetStandbyTriggered();
            return true;
        }
        return false;
    } else {
        /* check for primary */
        uint32 tgigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
        if (tgigger == extreme_rto::TRIGGER_STADNBY) {
            (void)pg_atomic_compare_exchange_u32(&(extreme_rto::g_startupTriggerState), &tgigger, 
                extreme_rto::TRIGGER_NORMAL);
            return true;
        }
    }
    return false;
}

bool CheckFinishRedoSignal(void)
{
    bool is_finish_redo = false;
    int cnt = 0;
    int fp = -1;
    if ((fp = open("finish_redo_file", O_RDONLY, S_IRUSR)) >= 0) {
        cnt = read(fp, &is_finish_redo, sizeof(bool));
        if (close(fp)) {
            ereport(LOG, (errcode_for_file_access(), errmsg("cannot close finish redo file.\n")));
        }
        if (cnt != sizeof(bool)) {
            ereport(LOG, (0, errmsg("cannot read finish redo file.\n")));
            unlink("finish_redo_file");
            return false;
        }

        ereport(LOG, (errmsg("CheckFinishRedoSignal read is_finish_redo successfully, forced_finishredo: %d\n",
                             is_finish_redo)));
    } else {
        return false;
    }
    unlink("finish_redo_file");
    return is_finish_redo;
}

extreme_rto::Enum_TriggeredState CheckForSatartupStatus(void)
{
    if (t_thrd.startup_cxt.primary_triggered) {
        ereport(LOG, (errmsg("received primary request")));
        ResetPrimaryTriggered();
        return extreme_rto::TRIGGER_PRIMARY;
    }
    if (t_thrd.startup_cxt.standby_triggered) {
        ereport(LOG, (errmsg("received standby request")));
        ResetStandbyTriggered();
        return extreme_rto::TRIGGER_STADNBY;
    }
    if (t_thrd.startup_cxt.failover_triggered) {
        return extreme_rto::TRIGGER_FAILOVER;
    }
    if (t_thrd.startup_cxt.switchover_triggered) {
        return extreme_rto::TRIGGER_FAILOVER;
    }
    return extreme_rto::TRIGGER_NORMAL;
}

/*
 * Check to see if a promote request has arrived. Should be
 * called by postmaster after receiving SIGUSR1.
 */
bool CheckPromoteSignal(void)
{
    uint32 Term = g_InvalidTermDN;
    int cnt = 0;
    int fp = -1;
    if ((fp = open("term_file", O_RDONLY, S_IRUSR)) >= 0) {
        cnt = read(fp, &Term, sizeof(uint32));
        if (close(fp)) {
            ereport(LOG, (errcode_for_file_access(), errmsg("cannot close term file when checking promote signal.\n")));
        }

        if (cnt != sizeof(uint32)) {
            ereport(LOG, (errcode_for_file_access(), errmsg("cannot read term file.\n")));
            return false;
        }
    }

    if (Term > g_instance.comm_cxt.localinfo_cxt.term_from_file &&
        t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) {
        g_instance.comm_cxt.localinfo_cxt.term_from_file = Term;
    }

    struct stat stat_buf;

    if (stat(FAILOVER_SIGNAL_FILE, &stat_buf) == 0) {
        /*
         * Since we are in a signal handler, it's not safe to ereport. We
         * silently ignore any error from unlink.
         */
        unlink(FAILOVER_SIGNAL_FILE);
        if (Term > g_instance.comm_cxt.localinfo_cxt.term_from_file) {
            g_instance.comm_cxt.localinfo_cxt.term_from_file = Term;
            g_instance.comm_cxt.localinfo_cxt.set_term = false;
            ereport(LOG, (errmsg("CheckPromoteSignal read term file, term %d\n", Term)));
        }
        return true;
    }

    if (CheckPostmasterSignal(PMSIGNAL_PROMOTE_STANDBY)) {
        /*
         * Since we are in a signal handler, it's not safe to ereport. We
         * silently ignore any error from unlink.
         */
        if (Term > g_instance.comm_cxt.localinfo_cxt.term_from_file) {
            g_instance.comm_cxt.localinfo_cxt.term_from_file = Term;
            g_instance.comm_cxt.localinfo_cxt.set_term = false;
            ereport(LOG, (errmsg("CheckPromoteSignal read term file, term %d\n", Term)));
        }

        /* Reset the seqno for matview */
        MatviewShmemSetInvalid();

        return true;
    }

    return false;
}

TermFileData GetTermFileDataAndClear(void)
{
    TermFileData term_file;
    uint32 readRst;
    term_file.term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                         g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    readRst = pg_atomic_read_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo));
    term_file.finish_redo = (readRst == 1);
    if (term_file.finish_redo && knl_g_get_local_redo_finish_status()) {
        ereport(WARNING, (0, errmsg("GetTermFileData: skip force finish redo when local redo finish")));
        term_file.finish_redo = false;
        pg_atomic_write_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo), 0);
    }
    return term_file;
}

/*
 * Check whether the signal is primary signal
 */
bool CheckPrimarySignal(void)
{
    struct stat stat_buf;
    uint32 term = g_InvalidTermDN;
    int cnt = 0;
    int fp = -1;

    if ((fp = open("term_file", O_RDONLY, S_IRUSR)) >= 0) {
        cnt = read(fp, &term, sizeof(uint32));
        if (close(fp)) {
            ereport(LOG, (errcode_for_file_access(), errmsg("cannot close term file when checking primary signal.\n")));
        }

        if (cnt != sizeof(uint32)) {
            ereport(LOG, (errcode_for_file_access(), errmsg("cannot read term file for notify primary.\n")));
            return false;
        }
    }

    if (stat(PRIMARY_SIGNAL_FILE, &stat_buf) == 0) {
        /*
         * Since we are in a signal handler, it's not safe to ereport. We
         * silently ignore any error from unlink.
         */
        (void)unlink(PRIMARY_SIGNAL_FILE);
        if (term > g_instance.comm_cxt.localinfo_cxt.term_from_file) {
            g_instance.comm_cxt.localinfo_cxt.term_from_file = term;
            ereport(LOG, (errmsg("CheckPrimarySignal term %u\n", term)));
        }
        return true;
    }
    return false;
}

/*
 * Check whether the signal is standby signal
 */
bool CheckStandbySignal(void)
{
    struct stat stat_buf;

    if (stat(STANDBY_SIGNAL_FILE, &stat_buf) == 0) {
        /*
         * Since we are in a signal handler, it's not safe to elog. We
         * silently ignore any error from unlink.
         */
        (void)unlink(STANDBY_SIGNAL_FILE);
        return true;
    }
    return false;
}

/*
 * Check to see if a switchover request has arrived and
 * read demote mode from switchover signal file.
 * Should be called by postmaster after receiving SIGUSR1.
 */
int CheckSwitchoverSignal(void)
{
    int mode = 0;
    int switchfile = -1;
    int returnCode;

    /*
     * Since we are in a signal handler, it's not safe to elog. We
     * silently ignore any error from fopen/fread/fclose/unlink.
     */
    switchfile = open(SWITCHOVER_SIGNAL_FILE, O_RDONLY, S_IRUSR);
    if (switchfile < 0) {
        return mode;
    }

    returnCode = read(switchfile, &mode, sizeof(int));
    if (returnCode != sizeof(int)) {
        mode = 0;
    }

    if (mode < SmartDemote || mode > FastDemote) {
        mode = 0;
    }

    if (close(switchfile)) {
        ereport(LOG, (errcode_for_file_access(), errmsg("cannot close switchfile.\n")));
    }
    (void)unlink(SWITCHOVER_SIGNAL_FILE);

    return mode;
}

/*
 * Wake up startup process to replay newly arrived WAL, or to notice that
 * failover has been requested.
 */
void WakeupRecovery(void)
{
    SetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
}

void WakeupDataRecovery(void)
{
    SetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->dataRecoveryLatch);
}

/*
 * Update the WalWriterSleeping flag.
 */
void SetWalWriterSleeping(bool sleeping)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->WalWriterSleeping = sleeping;
    SpinLockRelease(&xlogctl->info_lck);
}

/*
 * CloseXlogFilesAtThreadExit
 *		Close opened xlog files at thread exit time
 */
void CloseXlogFilesAtThreadExit(void)
{
    if (t_thrd.xlog_cxt.openLogFile >= 0) {
        XLogFileClose();
    }
    closeXLogRead();
}

void StartupDataStreaming(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    /* Only Single mode && multi standby mode doesn't need data receiver */
    if (IS_SINGLE_NODE && !IS_DN_DUMMY_STANDYS_MODE())
        return;

    if (xlogctl->IsRecoveryDone && !DataRcvInProgress()) {
        load_server_mode();

        if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE && !CheckForSwitchoverTrigger()) {
            if (CheckForFailoverTrigger()) {
                /* if we got all the data in secondary, no need reconnect again */
                if (GetDataRcvDummyStandbySyncPercent() != SYNC_DUMMY_STANDBY_END) {
                    RequestDataStreaming(NULL, REPCONNTARGET_DUMMYSTANDBY);
                }
            } else {
                RequestDataStreaming(NULL, REPCONNTARGET_PRIMARY);
            }
        }
    }
}

/*
 * load the current ongoing mode from the HaShmData
 */
void load_server_mode(void)
{
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;

    SpinLockAcquire(&hashmdata->mutex);
    t_thrd.xlog_cxt.server_mode = hashmdata->current_mode;
    t_thrd.xlog_cxt.is_cascade_standby = hashmdata->is_cascade_standby;
    SpinLockRelease(&hashmdata->mutex);
}

/*
 * Get the current ongoing mode from the HaShmData
 */
bool IsServerModeStandby()
{
    load_server_mode();
    return (t_thrd.xlog_cxt.server_mode == STANDBY_MODE);
}

/*
 * Calculate difference between two lsns.
 */
uint64 XLogDiff(XLogRecPtr end, XLogRecPtr start)
{
    int64 len = 0;

    if (XLByteLT(end, start)) {
        return 0;
    }

    len = end - start;

    return len;
}

/*
 * get  and set walsender EndRecPtr for dummy standby.
 */
static void SetDummyStandbyEndRecPtr(XLogReaderState *xlogreader)
{
    XLogRecord *record = NULL;

    do {
        record = ReadRecord(xlogreader, InvalidXLogRecPtr, LOG, false);
    } while (record != NULL);

    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    if (XLByteLT(walrcv->receivedUpto, t_thrd.xlog_cxt.EndRecPtr))
        walrcv->receivedUpto = t_thrd.xlog_cxt.EndRecPtr;
    SpinLockRelease(&walrcv->mutex);
}

/*
 * get the crc of the xlog record
 */
pg_crc32 GetXlogRecordCrc(XLogRecPtr RecPtr, bool &crcvalid)
{
    XLogReaderState *xlogreader = NULL;
    XLogPageReadPrivate readprivate;
    pg_crc32 reccrc = 0;
    TimeLineID tli = 0;
    XLogRecord *rec = NULL;
    char *errorMsg = NULL;
    errno_t rc = EOK;

    if (!t_thrd.xlog_cxt.expectedTLIs && t_thrd.shemem_ptr_cxt.XLogCtl && t_thrd.shemem_ptr_cxt.ControlFile) {
        tli = t_thrd.shemem_ptr_cxt.XLogCtl->ThisTimeLineID;
        t_thrd.xlog_cxt.expectedTLIs = list_make1_int((int)tli);

        tli = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID;
        /* Build list with newest item first */
        if (!list_member_int(t_thrd.xlog_cxt.expectedTLIs, (int)tli)) {
            t_thrd.xlog_cxt.expectedTLIs = lcons_int((int)tli, t_thrd.xlog_cxt.expectedTLIs);
        }
    }

    if (!XRecOffIsValid(RecPtr)) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                        errmsg("invalid record offset at %X/%X.", (uint32)(RecPtr >> 32), (uint32)RecPtr)));
    }

    /* Set up XLOG reader facility */
    rc = memset_s(&readprivate, sizeof(XLogPageReadPrivate), 0, sizeof(XLogPageReadPrivate));
    securec_check(rc, "\0", "\0");
    xlogreader = XLogReaderAllocate(&XLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor")));
    }
    xlogreader->system_identifier = GetSystemIdentifier();

    if (dummyStandbyMode) {
        rec = ReadRecord(xlogreader, RecPtr, LOG, false);
    } else {
        rec = XLogReadRecord(xlogreader, RecPtr, &errorMsg, true);
    }

    crcvalid = true;

    if (rec != NULL) {
        reccrc = rec->xl_crc;

        if (dummyStandbyMode) {
            SetDummyStandbyEndRecPtr(xlogreader);
        }
    } else {
        crcvalid = false;
    }

    /* Shut down readFile facility, free space. */
    ShutdownReadFileFacility();

    /* Shut down the xlog reader facility. */
    XLogReaderFree(xlogreader);
    xlogreader = NULL;

    return reccrc;
}

static void ShutdownReadFileFacility(void)
{
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
    }
    if (t_thrd.xlog_cxt.expectedTLIs) {
        list_free(t_thrd.xlog_cxt.expectedTLIs);
        t_thrd.xlog_cxt.expectedTLIs = NULL;
    }
}

bool CheckFpwBeforeFirstCkpt(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    return xlogctl->FpwBeforeFirstCkpt;
}

void DisableFpwBeforeFirstCkpt(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->FpwBeforeFirstCkpt = false;
    SpinLockRelease(&xlogctl->info_lck);
}

/*
 * calculate the LsnXlogFlushData file size
 */
Size LsnXlogFlushChkShmemSize(void)
{
    Size size = sizeof(LsnXlogFlushData);
    size = MAXALIGN(size);
    return size;
}

uint32 get_controlfile_timeline(void)
{
    return t_thrd.shemem_ptr_cxt.ControlFile->timeline;
}

/*
 * initital the LsnXlogFlushData share memory
 */
void LsnXlogFlushChkShmInit(void)
{
    bool found = false;

    t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile = (LsnXlogFlushData *)ShmemInitStruct("LsnXlogFlushChk File",
                                                                                      sizeof(LsnXlogFlushData), &found);

    if (t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile == NULL) {
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("not enough shared memory for pg_lsnxlogflushchk share memory")));
    }

    if (!found) {
        t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile->localLsnFlushPoint = 0;
        t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile->peerXlogFlushPoint = 0;

        /* calculate the crc value */
        INIT_CRC32(t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile->crc);
        COMP_CRC32(t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile->crc,
                   (char *)t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile, offsetof(LsnXlogFlushData, crc));
        FIN_CRC32(t_thrd.shemem_ptr_cxt.g_LsnXlogFlushChkFile->crc);
    }

    return;
}

void heap_xlog_bcm_new_page(xl_heap_logical_newpage *xlrec, RelFileNode node, char *cuData)
{
    CFileNode cFileNode(node);
    cFileNode.m_forkNum = xlrec->forknum;
    cFileNode.m_attid = xlrec->attid;
    CUStorage *cuStorage = New(CurrentMemoryContext) CUStorage(cFileNode);
    cuStorage->SaveCU(cuData, xlrec->offset, xlrec->blockSize, false);
    DELETE_EX(cuStorage);
}

static bool heap_xlog_logical_new_page_with_data(XLogReaderState *record)
{
    bool result = false;
    xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)XLogRecGetData(record);
    if (xlrec->type == COLUMN_STORE && xlrec->hasdata) {
        char *cuData = XLogRecGetData(record) + SizeOfHeapLogicalNewPage;
        RelFileNode tmp_node;
        RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));

        Assert(XLogRecGetBucketId(record) == InvalidBktId);

        heap_xlog_bcm_new_page(xlrec, tmp_node, cuData);
        result = true;
    }
    return result;
}

/* reserve function */
void heap_xlog_logical_new_page(XLogReaderState *record)
{
    /*
     * one primary muti standby and enable_mix_replication is off need reocord cu xlog.
     * NO ereport(ERROR) from here till newpage op is logged
     */
    if (IS_DN_MULTI_STANDYS_MODE()) {
        heap_xlog_logical_new_page_with_data(record);
        return;
    }

    if (!g_instance.attr.attr_storage.enable_mix_replication && !IS_DN_MULTI_STANDYS_MODE()) {
        /* timeseries table use xlog to sync data */
        if (g_instance.attr.attr_common.enable_tsdb) {
            bool result = heap_xlog_logical_new_page_with_data(record);
            if (!result) {
                StartupDataStreaming();
            }
        } else {
            StartupDataStreaming();
        }
    }

    return;
}

/*
 * Returns true if 'expectedTLEs' contains a timeline with id 'tli'
 */
static bool timeLineInHistory(TimeLineID tli, List *expectedTLEs)
{
    ListCell *cell = NULL;

    foreach (cell, expectedTLEs) {
        if ((TimeLineID)lfirst_int(cell) == tli) {
            return true;
        }
    }

    return false;
}

void SetCBMTrackedLSN(XLogRecPtr trackedLSN)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->cbmTrackedLSN = trackedLSN;
    SpinLockRelease(&xlogctl->info_lck);

    ereport(LOG, (errmsg("set CBM tracked LSN point: %08X/%08X ", (uint32)(trackedLSN >> 32), (uint32)trackedLSN)));
}

XLogRecPtr GetCBMTrackedLSN(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr trackedLSN;

    SpinLockAcquire(&xlogctl->info_lck);
    trackedLSN = xlogctl->cbmTrackedLSN;
    SpinLockRelease(&xlogctl->info_lck);
    ereport(DEBUG5, (errmsg("get CBM tracked LSN point: %08X/%08X ", (uint32)(trackedLSN >> 32), (uint32)trackedLSN)));
    return trackedLSN;
}

void SetCBMRotateLsn(XLogRecPtr rotate_lsn)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr curr_rotate_lsn;
    curr_rotate_lsn = GetCBMRotateLsn();
    while(XLByteLT(curr_rotate_lsn, rotate_lsn) 
        && pg_atomic_compare_exchange_u64(&xlogctl->cbmMaxRequestRotateLsn, &curr_rotate_lsn, rotate_lsn) == false);
    ereport(LOG, (errmsg("set CBM SetCBMRotateLsn point: %08X/%08X ", (uint32)(rotate_lsn >> 32), (uint32)rotate_lsn)));
}

XLogRecPtr GetCBMRotateLsn(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    volatile XLogRecPtr rotate_lsn;
    rotate_lsn = pg_atomic_read_u64(&xlogctl->cbmMaxRequestRotateLsn);
    return rotate_lsn;
}

void SetCBMFileStartLsn(XLogRecPtr currCbmNameStartLsn)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->currCbmFileStartLsn = currCbmNameStartLsn;
    SpinLockRelease(&xlogctl->info_lck);
    ereport(DEBUG1, (errmsg("set CBM SetCBMFileStartLsn %08X/%08X ", 
        (uint32)(currCbmNameStartLsn >> 32), (uint32)currCbmNameStartLsn)));
}

XLogRecPtr GetCBMFileStartLsn()
{
    XLogRecPtr currCbmNameStartLsn;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    SpinLockAcquire(&xlogctl->info_lck);
    currCbmNameStartLsn = xlogctl->currCbmFileStartLsn;
    SpinLockRelease(&xlogctl->info_lck);
    return currCbmNameStartLsn;
}

void SetDelayXlogRecycle(bool toDelay, bool isRedo)
{
    XLogRecPtr recptr;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->delayXlogRecycle = toDelay;
    SpinLockRelease(&xlogctl->info_lck);
    if (isRedo == false && t_thrd.proc->workingVersionNum >= 92233) {
        START_CRIT_SECTION();
        XLogBeginInsert();
        XLogRegisterData((char *)(&toDelay), sizeof(toDelay));
        recptr = XLogInsert(RM_XLOG_ID, XLOG_DELAY_XLOG_RECYCLE);
        XLogWaitFlush(recptr);
        END_CRIT_SECTION();
    }
    ereport(DEBUG5, (errmsg("set delay xlog recycle to %s", toDelay ? "true" : "false")));
}

bool GetDelayXlogRecycle(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    bool toDelay = false;

    SpinLockAcquire(&xlogctl->info_lck);
    toDelay = xlogctl->delayXlogRecycle;
    SpinLockRelease(&xlogctl->info_lck);

    ereport(DEBUG5, (errmsg("get delay xlog recycle of %s", toDelay ? "true" : "false")));

    return toDelay;
}

void SetDDLDelayStartPtr(XLogRecPtr ddlDelayStartPtr)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;

    SpinLockAcquire(&xlogctl->info_lck);
    xlogctl->ddlDelayStartPtr = ddlDelayStartPtr;
    SpinLockRelease(&xlogctl->info_lck);

    ereport(LOG, (errmsg("set DDL delay start LSN point: %08X/%08X ", (uint32)(ddlDelayStartPtr >> 32),
                         (uint32)ddlDelayStartPtr)));
}

XLogRecPtr GetDDLDelayStartPtr(void)
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr start_lsn;
    XLogRecPtr old_start_lsn;

    start_lsn = ReplicationSlotsComputeConfirmedLSN();

    if (t_thrd.proc != NULL && t_thrd.proc->workingVersionNum < BACKUP_SLOT_VERSION_NUM) {
        SpinLockAcquire(&xlogctl->info_lck);
        old_start_lsn = xlogctl->ddlDelayStartPtr;
        SpinLockRelease(&xlogctl->info_lck);

        if (XLByteLT(old_start_lsn, start_lsn)) {
            start_lsn = old_start_lsn;
        }
    }
    ereport(DEBUG5, (errmsg("get DDL delay start LSN: %08X/%08X ", (uint32)(start_lsn >> 32), (uint32)start_lsn)));

    return start_lsn;
}

bool IsRoachRestore(void)
{
    return (t_thrd.xlog_cxt.InRecovery && t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_BARRIER &&
            t_thrd.xlog_cxt.recoveryTargetBarrierId != NULL &&
            strncmp(t_thrd.xlog_cxt.recoveryTargetBarrierId, ROACH_BACKUP_PREFIX, strlen(ROACH_BACKUP_PREFIX)) == 0);
}

const uint UPDATE_REC_XLOG_NUM = 4;
#if defined(__x86_64__) || defined(__aarch64__)
bool atomic_update_dirty_page_queue_rec_lsn(XLogRecPtr current_insert_lsn, bool need_immediately_update)
{
    XLogRecPtr cur_rec_lsn = InvalidXLogRecPtr;
    uint128_u compare;
    uint128_u exchange;
    uint128_u current;

    compare = atomic_compare_and_swap_u128((uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn);
    Assert(sizeof(g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn) == SIZE_OF_UINT64);
    Assert(sizeof(g_instance.ckpt_cxt_ctl->dirty_page_queue_tail) == SIZE_OF_UINT64);

loop:
    cur_rec_lsn = compare.u64[0];
    /* if we already left behind dirty array queue reclsn, do nothing */
    if (!XLByteLE(current_insert_lsn, cur_rec_lsn) &&
        (need_immediately_update || current_insert_lsn - cur_rec_lsn > XLOG_SEG_SIZE * UPDATE_REC_XLOG_NUM)) {

        exchange.u64[0] = current_insert_lsn;
        exchange.u64[1] = compare.u64[1];

        current = atomic_compare_and_swap_u128(
            (uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn, compare, exchange);
        if (!UINT128_IS_EQUAL(compare, current)) {
            UINT128_COPY(compare, current);
            goto loop;
        }
        return true;
    }
    return false;
}
#endif

/**
 * @Description: Push dirty buffer to dirty page queue, need ensure the recLSN incrementally. when is
 *              not in recovery mode, XLog insert or do full checkpoint will update the dirty page
 *              queue recLSN. when is in recovery mode, redo checkpoint XLog, update the dirty page
 *              queue recLSN to checkpoint's redo lsn.
 */
void update_dirty_page_queue_rec_lsn(XLogRecPtr current_insert_lsn, bool need_immediately_update)
{
    bool is_update = false;
    uint32 freespace;

    if (!ENABLE_INCRE_CKPT) {
        return;
    }

    /* Compute new incremental checkpoint redo record ptr = location of next XLOG record. */
    freespace = INSERT_FREESPACE(current_insert_lsn);
    if (freespace == 0) {
        if (current_insert_lsn % XLogSegSize == 0) {
            current_insert_lsn += SizeOfXLogLongPHD;
        } else {
            current_insert_lsn += SizeOfXLogShortPHD;
        }
    }

#if defined(__x86_64__) || defined(__aarch64__)
    is_update = atomic_update_dirty_page_queue_rec_lsn(current_insert_lsn, need_immediately_update);
#else
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);

    if (!XLByteLE(current_insert_lsn, g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn) &&
        (need_immediately_update || current_insert_lsn - g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn >
        XLOG_SEG_SIZE * UPDATE_REC_XLOG_NUM)) {
        g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn = current_insert_lsn;
        is_update = true;
    }
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);

#endif
    if (is_update && u_sess->attr.attr_storage.log_pagewriter && RecoveryInProgress()) {
        ereport(LOG, (errmodule(MOD_INCRE_CKPT),
                      errmsg("update dirty page queue recovery lsn is %08X/%08X",
                             (uint32)(current_insert_lsn >> XLOG_LSN_SWAP), (uint32)current_insert_lsn)));
    }
    return;
}

uint64 get_dirty_page_queue_rec_lsn()
{
    uint64 dirty_page_queue_rec_lsn = 0;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u compare;
    compare = atomic_compare_and_swap_u128((uint128_u*)&g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn);
    dirty_page_queue_rec_lsn = compare.u64[0];
#else
    SpinLockAcquire(&g_instance.ckpt_cxt_ctl->queue_lock);
    dirty_page_queue_rec_lsn = g_instance.ckpt_cxt_ctl->dirty_page_queue_reclsn;
    SpinLockRelease(&g_instance.ckpt_cxt_ctl->queue_lock);
#endif
    return dirty_page_queue_rec_lsn;
}

/**
 * @Description: get the recLSN of the first page in the dirty page queue.
 * @return: dirty page queue first valid recLSN.
 */
XLogRecPtr ckpt_get_min_rec_lsn(void)
{
    uint64 queue_loc;
    XLogRecPtr min_rec_lsn = InvalidXLogRecPtr;

    /*
     * If head recLSN is Invalid, then add head, get next buffer recLSN, if head equal tail,
     * return InvalidXLogRecPtr.
     */
    if (get_dirty_page_num() == 0) {
        return InvalidXLogRecPtr;
    }
    queue_loc = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
    while (XLogRecPtrIsInvalid(min_rec_lsn) && (queue_loc < get_dirty_page_queue_tail())) {
        Buffer buffer;
        BufferDesc *buf_desc = NULL;
        XLogRecPtr page_rec_lsn = InvalidXLogRecPtr;
        uint64 temp_loc = queue_loc % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot *slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];

        /* slot location is pre-occupied, but the buffer not set finish, need wait and retry. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            pg_usleep(1);
            queue_loc = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);
            continue;
        }
        pg_memory_barrier();
        queue_loc++;

        buffer = slot->buffer;
        /* slot state is vaild, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (BufferIsInvalid(buffer)) {
            continue;
        }
        buf_desc = GetBufferDescriptor(buffer - 1);
        page_rec_lsn = pg_atomic_read_u64(&buf_desc->rec_lsn);
        if (!BufferIsInvalid(slot->buffer)) {
            min_rec_lsn = page_rec_lsn;
        }
    }
    return min_rec_lsn;
}

void WaitCheckpointSync(void)
{
    if (t_thrd.shemem_ptr_cxt.ControlFile != NULL &&
        u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        XLogRecPtr chcekpoint = t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
        LWLockRelease(ControlFileLock);
        SyncRepWaitForLSN(chcekpoint);
    }
}

void GetRecoveryLatch()
{
    OwnLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
}

void ReLeaseRecoveryLatch()
{
    DisownLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
}

XLogRecPtr mpfl_get_max_lsn_from_buf(char *buf)
{
    uint32 i = 0;
    XLogRecPtr *lsn = NULL;
    XLogRecPtr tmp_lsn = 0;
    Assert(buf != NULL);
    for (i = 0; i < MPFL_PAGE_NUM; ++i) {
        lsn = (XLogRecPtr *)(buf + i * BLCKSZ);
        pg_atomic_write_u64(&(g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[i]), *lsn);
        if (XLByteLT(tmp_lsn, *lsn)) {
            tmp_lsn = *lsn;
        }
    }
    return tmp_lsn;
}

XLogRecPtr mpfl_read_max_flush_lsn()
{
    int fd = -1;

    char *file_head = g_instance.comm_cxt.predo_cxt.ali_buf;
    XLogRecPtr max_flush_lsn;
    if (!file_exists(MAX_PAGE_FLUSH_LSN_FILE)) {
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_REDO), errmsg("mpfl file does not exist")));
        return MAX_XLOG_REC_PTR;
    }
    /* O_DSYNC for less IO */
    fd = open(MAX_PAGE_FLUSH_LSN_FILE, MPFL_FILE_FLAG, MPFL_FILE_PERM);
    if (fd == -1) {
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW),
                          errmsg("mpfl could not open file \"%s\": %m", MAX_PAGE_FLUSH_LSN_FILE)));
        return MAX_XLOG_REC_PTR;
    }

    /* alignment for O_DIRECT */
    Assert(file_head != NULL);
    pgstat_report_waitevent(WAIT_EVENT_MPFL_READ);
    if (!mpfl_pread_file(fd, file_head, MPFL_FILE_SIZE, 0)) {
        max_flush_lsn = MAX_XLOG_REC_PTR;
    } else {
        max_flush_lsn = mpfl_get_max_lsn_from_buf(file_head);
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    close(fd);
    if (max_flush_lsn == 0) {
        max_flush_lsn = MAX_XLOG_REC_PTR;
    }
    return max_flush_lsn;
}

void mpfl_ulink_file()
{
    int ret = 0;
    ret = unlink(MAX_PAGE_FLUSH_LSN_FILE);
    if (ret < 0 && errno != ENOENT) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[mpfl_ulink_file]: unlink %s failed! ret:%u", MAX_PAGE_FLUSH_LSN_FILE, ret)));
    } else {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("[mpfl_ulink_file]: unlink %s sucessfully! ret:%u", MAX_PAGE_FLUSH_LSN_FILE, ret)));
    }
}

bool mpfl_pread_file(int fd, void *buf, int32 size, int64 offset)
{
    int32 curr_size, total_size;
    total_size = 0;
    do {
        curr_size = pread64(fd, ((char *)buf + total_size), (size - total_size), offset);
        if (curr_size == -1) {
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_REDO),
                              errmsg("mpfl_pread_file:Read file error, size:%d, offset:%ld, total_size:%d", size,
                                     offset, total_size)));
            return false;
        }

        total_size += curr_size;
        offset += curr_size;
    } while (curr_size > 0);

    if (total_size != size) {
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_REDO),
                          errmsg("mpfl_pread_file: file size mismatch: expected %d, read %d, offset:%ld", size,
                                 total_size, offset)));
        return false;
    }
    return true;
}

void mpfl_pwrite_file(int fd, const void *buf, int size, int64 offset)
{
    int write_size = 0;
    uint32 try_times = 0;
    const uint64 mpfl_try_times = 8;
    const uint64 mpfl_sleep_us = 256L;

    if ((offset + size) > MPFL_FILE_SIZE) {
        ereport(PANIC, (errmodule(MOD_REDO), errmsg("mpfl_pwrite_file failed, MPFL_FILE_SIZE %d, "
                                                    "offset %ld, write_size %d",
                                                    MPFL_FILE_SIZE, offset, size)));
    }

    while (try_times < mpfl_try_times) {
        write_size = pwrite64(fd, buf, size, offset);
        if (write_size == 0) {
            try_times++;
            pg_usleep(mpfl_sleep_us);
            continue;
        } else if (write_size < 0) {
            ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_REDO), errmsg("mpfl_pwrite_file: Write file error")));
        } else {
            break;
        }
    }
    if (write_size != size) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_REDO),
                 errmsg("mpfl_pwrite_file: Write file size mismatch: expected %d, written %d", size, write_size)));
    }
}

void mpfl_new_file()
{
    errno_t errorno = EOK;
    char *file_head = g_instance.comm_cxt.predo_cxt.ali_buf;
    int fd = -1;
    if (file_exists(MAX_PAGE_FLUSH_LSN_FILE)) {
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_REDO), "max_flush_lsn file exists, deleting it"));
        if (unlink(MAX_PAGE_FLUSH_LSN_FILE) != 0) {
            ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_REDO), errmsg("Could not remove max_flush_lsn file")));
        }
    }
    /* Open file with O_SYNC, to make sure the data and file system control info on file after block writing. */
    fd = open(MAX_PAGE_FLUSH_LSN_FILE, (MPFL_FILE_FLAG | O_CREAT), MPFL_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_REDO),
                        errmsg("Could not create file \"%s\"", MAX_PAGE_FLUSH_LSN_FILE)));
    }
    Assert(file_head != NULL);
    errorno = memset_s(file_head, MPFL_FILE_SIZE, 0, MPFL_FILE_SIZE);
    securec_check(errorno, "", "");
    pgstat_report_waitevent(WAIT_EVENT_MPFL_INIT);
    mpfl_pwrite_file(fd, file_head, MPFL_FILE_SIZE, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);
    close(fd);
}

void update_max_page_flush_lsn(XLogRecPtr biggest_lsn, ThreadId thdId, bool is_force)
{
    uint32 hash_value = hashquickany(0xFFFFFFFF, (unsigned char *)(&thdId), sizeof(ThreadId));
    uint32 lock_id = hash_value % NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS;
    LWLock *p_lock = NULL;
    int fd = -1;
    char *page = NULL;
    int64 offset;
    XLogRecPtr *new_lsn;
    XlogUpMinRecovPointSwitchChk(biggest_lsn);
    if (XLByteLE(biggest_lsn, pg_atomic_read_u64(&(g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[lock_id]))) &&
        !is_force) {
        return;
    }
    /* we must keep consistency between g_instance.comm_cxt.predo_cxt.max_page_flush_lsn */
    /* and info.max_page_flush_lsn. */
    p_lock = &(t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstMPFLLock + (lock_id)].lock);
    LWLockAcquire(p_lock, LW_EXCLUSIVE);
    if (is_force) {
        pg_atomic_write_u64(&(g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[lock_id]), biggest_lsn);
    } else {
        if (XLByteLT(pg_atomic_read_u64(&g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[lock_id]), biggest_lsn)) {
            pg_atomic_write_u64(&(g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[lock_id]), biggest_lsn);
        } else {
            LWLockRelease(p_lock);
            return;
        }
    }
    /* Open file with O_SYNC, to make sure the data and file system control info on file after block writing. */
    fd = open(MAX_PAGE_FLUSH_LSN_FILE, (MPFL_FILE_FLAG), MPFL_FILE_PERM);
    if (fd == -1) {
        LWLockRelease(p_lock);
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_REDO),
                        errmsg("Could not open file \"%s\"", MAX_PAGE_FLUSH_LSN_FILE)));
    }
    offset = lock_id * BLCKSZ;
    page = g_instance.comm_cxt.predo_cxt.ali_buf + offset;
    new_lsn = (XLogRecPtr *)page;
    *new_lsn = biggest_lsn;
    pgstat_report_waitevent(WAIT_EVENT_MPFL_WRITE);
    mpfl_pwrite_file(fd, page, BLCKSZ, offset);
    pgstat_report_waitevent(WAIT_EVENT_END);
    close(fd);
    LWLockRelease(p_lock);
}

void set_global_max_page_flush_lsn(XLogRecPtr lsn)
{
    LWLock *p_lock = NULL;
    for (uint32 i = 0; i < NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS; ++i) {
        p_lock = &(t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstMPFLLock + (i)].lock);
        LWLockAcquire(p_lock, LW_EXCLUSIVE);
        pg_atomic_write_u64(&(g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[i]), lsn);
        LWLockRelease(p_lock);
    }
}

XLogRecPtr get_global_max_page_flush_lsn()
{
    XLogRecPtr maxLsn = 0;
    for (uint32 i = 0; i < NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS; ++i) {
        XLogRecPtr tmpLsn = pg_atomic_read_u64(&g_instance.comm_cxt.predo_cxt.max_page_flush_lsn[i]);
        if (XLByteLT(maxLsn, tmpLsn)) {
            maxLsn = tmpLsn;
        }
    }
    return maxLsn;
}

bool IsRecoveryDone()
{
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    return xlogctl->IsRecoveryDone;
}


void ExtremRtoUpdateMinCheckpoint()
{
    if (t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone && t_thrd.xlog_cxt.minRecoveryPoint == InvalidXLogRecPtr) {
        t_thrd.xlog_cxt.minRecoveryPoint = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
    }
}

/* IsArchiverOnStandby */
extern bool IsValidArchiverStandby(WalSnd* walsnd)
{
    if (walsnd == NULL) {
        return false;
    }
    if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole) &&
        walsnd->is_start_archive) {
        return true;
    } else {
        return false;
    }
}