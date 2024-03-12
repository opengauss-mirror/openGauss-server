/* -------------------------------------------------------------------------
 *
 * heapam.h
 *	  openGauss heap access method definitions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HEAPAM_H
#define HEAPAM_H

#include "access/sdir.h"
#include "access/skey.h"
#include "access/xlogrecord.h"
#include "access/multixact.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "storage/lock/lock.h"
#include "storage/pagecompress.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/snapshot.h"
#include "replication/bcm.h"

/* "options" flag bits for heap_insert */
#define HEAP_INSERT_SKIP_WAL 0x0001
#define HEAP_INSERT_SKIP_FSM 0x0002
#define HEAP_INSERT_FROZEN 0x0004
#define HEAP_INSERT_SPECULATIVE 0x0008
#define HEAP_INSERT_SKIP_ERROR 0x0010
#define HEAP_INSERT_SPLIT_PARTITION 0x0020

/* ----------------------------------------------------------------
 *               Scan State Information
 * ----------------------------------------------------------------
 */
typedef struct SeqScanAccessor {
    BlockNumber sa_last_prefbf;  /* last prefetch block number */
    BlockNumber sa_pref_trigbf;  /* last triggle block number */
    uint32 sa_prefetch_quantity; /* preftch quantity*/
    uint32 sa_prefetch_trigger;  /* the prefetch-trigger distance bewteen last prefetched buffer and currently accessed buffer */
} SeqScanAccessor;

typedef struct RangeScanInRedis{
    uint8 isRangeScanInRedis;
    uint8 sliceTotal;
    uint8 sliceIndex;
} RangeScanInRedis;

/*
 * Generic descriptor for table scans. This is the base-class for table scans,
 * which needs to be embedded in the scans of individual AMs.
 */
typedef struct TableScanDescData
{
    /* scan parameters */        
    // !! rs_rd MUST BE FIRST MEMBER !!
    Relation        rs_rd;        /* heap relation descriptor */
    Snapshot        rs_snapshot;  /* snapshot to see */
    int             rs_nkeys;     /* number of scan keys */
    ScanKey         rs_key;       /* array of scan key descriptors */
    bool rs_pageatatime;  /* verify visibility page-at-a-time? */

    /*
     * Information about type and behaviour of the scan, a bitmask of members
     * of the ScanOptions enum (see tableam.h).
     */
    uint32          rs_flags;

    /* state set up at initscan time */
    BlockNumber rs_nblocks;           /* number of blocks to scan */
    BlockNumber rs_startblock;        /* block # to start at */
    BufferAccessStrategy rs_strategy; /* access strategy for reads */
    bool rs_syncscan;                 /* report location to syncscan logic? */

    /* fields used for ustore partial seq scan */
    AttrNumber lastVar = -1; /* last variable (table column) needed for seq scan (non-inclusive), -1 = all */
    bool* boolArr = NULL; /* false elements indicate columns that must be copied during seq scan */

    /* scan current state */
    bool rs_inited;        /* false = scan not init'd yet */
    BlockNumber rs_cblock; /* current block # in scan, if any */
    Buffer rs_cbuf;        /* current buffer in scan, if any */

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    int rs_cindex;                                   /* current tuple's index in vistuples */
    int rs_ntuples;                                  /* number of visible tuples on page */
    OffsetNumber rs_vistuples[MaxHeapTuplesPerPage]; /* their offsets */
    SeqScanAccessor* rs_ss_accessor;                 /* adio use it to init prefetch quantity and trigger */

    /* state set up at initscan time */
    RangeScanInRedis  rs_rangeScanInRedis;       /* if it is a range scan in redistribution */
    TupleTableSlot *slot; /* For begin scan of CopyTo */

    /* variables for batch mode scan */
    int rs_ctupRows;
    int rs_maxScanRows;

    /* variables for ndp pushdown scan */
    bool ndp_pushdown_optimized;
    void *ndp_ctx;
} TableScanDescData;

/* struct definition appears in relscan.h */
typedef struct TableScanDescData *TableScanDesc;
typedef struct HeapScanDescData* HeapScanDesc;
typedef struct IndexScanDescData* IndexScanDesc;

/*
 * Base class for fetches from a table via an index. This is the base-class
 * for such scans, which needs to be embedded in the respective struct for
 * individual AMs.
 */
typedef struct IndexFetchTableData
{
    Relation rel;
} IndexFetchTableData;

/*
 * Descriptor for fetches from heap via an index.
 */
typedef struct IndexFetchHeapData
{
    IndexFetchTableData xs_base; /* AM independent part of the descriptor */

    /* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
} IndexFetchHeapData;

struct ScanState;

typedef struct BulkInsertStateData* BulkInsertState;

typedef enum {
    /* SELECT FOR KEY SHARE */
    LockTupleKeyShare,
    /* SELECT FOR SHARE */
    LockTupleShared,
    /* SELECT FOR NO KEY UPDATE, and UPDATEs that don't modify key columns */
    LockTupleNoKeyExclusive,
    /* SELECT FOR UPDATE, UPDATEs that modify key columns, and DELETE */
    LockTupleExclusive 
} LockTupleMode;

#define MaxLockTupleMode        LockTupleExclusive

static const struct {
    LOCKMODE hwlock;
    MultiXactStatus lockstatus;
    MultiXactStatus updstatus;
} TupleLockExtraInfo[MaxLockTupleMode + 1] = {
    {
        /* LockTupleKeyShare */
        AccessShareLock,
        MultiXactStatusForKeyShare,
        (MultiXactStatus)-1 /* KeyShare does not allow updating tuples */
    },
    {
        RowShareLock, /* LockTupleShared */
        MultiXactStatusForShare,
        (MultiXactStatus)-1
    },
    {
        ExclusiveLock, /* LockTupleNoKeyExclusive */
        MultiXactStatusForNoKeyUpdate,
        MultiXactStatusNoKeyUpdate
    },
    {
        AccessExclusiveLock, /* LockTupleExclusive */
        MultiXactStatusForUpdate,
        MultiXactStatusUpdate
    }
};

/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LOCK_TUPLE_TUP_LOCK(rel, tup, mode) LockTuple((rel), (tup), TupleLockExtraInfo[mode].hwlock, true)
#define UNLOCK_TUPLE_TUP_LOCK(rel, tup, mode) UnlockTuple((rel), (tup), TupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleTuplock(_rel, _tup, _mode) \
    ConditionalLockTuple((_rel), (_tup), TupleLockExtraInfo[_mode].hwlock)

/*
 * This table maps tuple lock strength values for each particular
 * MultiXactStatus value.
 */
static const LockTupleMode MULTIXACT_STATUS_LOCK[MultiXactStatusUpdate + 1] = {
    LockTupleShared,          /* ForShare */
    LockTupleKeyShare,       /* ForKeyShare */
    LockTupleNoKeyExclusive, /* ForNoKeyUpdate */
    LockTupleExclusive,      /* ForUpdate */
    LockTupleNoKeyExclusive, /* NoKeyUpdate */
    LockTupleExclusive       /* Update */
};

/* Get the LockTupleMode for a given MultiXactStatus */
#define TUPLOCK_FROM_MXSTATUS(status) (MULTIXACT_STATUS_LOCK[(status)])
/* Get the LOCKMODE for a given MultiXactStatus */
#define LOCKMODE_FROM_MXSTATUS(status) (TupleLockExtraInfo[TUPLOCK_FROM_MXSTATUS((status))].hwlock)

/* the last arguments info for heap_multi_insert() */
typedef struct {
    /* compression info: dictionary buffer and its size */
    const char* dictData;
    int dictSize;

    /* forbid to use page replication */
    bool disablePageReplication;
} HeapMultiInsertExtraArgs;

/*
 * When tuple_update, tuple_delete, or tuple_lock fail because the target 
 * tuple is already outdated, they fill in this struct to provide information 
 * to the caller about what happened.
 *
 * ctid is the target's ctid link: it is the same as the target's TID if the
 * target was deleted, or the location of the replacement tuple if the target
 * was updated.
 *
 * xmax is the outdating transaction's XID.  If the caller wants to visit the
 * replacement tuple, it must check that this matches before believing the
 * replacement is really a match.
 *
 * cmax is the outdating command's CID, but only when the failure code is
 * HeapTupleSelfUpdated (i.e., something in the current transaction outdated the
 * tuple); otherwise cmax is zero.  (We make this restriction because
 * HeapTupleHeaderGetCmax doesn't work for tuples outdated in other
 * transactions.)
 */
typedef struct TM_FailureData
{
    ItemPointerData ctid;
    TransactionId xmax;
    TransactionId xmin;
    CommandId cmax;
    bool in_place_updated_or_locked;
} TM_FailureData;

#define enable_heap_bcm_data_replication() \
    (u_sess->attr.attr_storage.enable_data_replicate && !g_instance.attr.attr_storage.enable_mix_replication)

/* ----------------
 *		function prototypes for heap access method
 *
 * heap_create, heap_create_with_catalog, and heap_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */

/* in heap/heapam.c */
extern Relation relation_open(Oid relationId, LOCKMODE lockmode, int2 bucketId=-1);
extern Partition partitionOpenWithRetry(Relation relation, Oid partitionId, LOCKMODE lockmode,  const char * stmt);
extern Partition partitionOpen(Relation relation, Oid partitionId, LOCKMODE lockmode, int2 bucketId=-1);
extern void partitionClose(Relation relation, Partition partition, LOCKMODE lockmode);
extern Partition tryPartitionOpen(Relation relation, Oid partitionId, LOCKMODE lockmode);
extern Partition PartitionOpenWithPartitionno(Relation relation, Oid partition_id, int partitionno, LOCKMODE lockmode);
extern Relation try_relation_open(Oid relationId, LOCKMODE lockmode);
extern Relation relation_openrv(const RangeVar* relation, LOCKMODE lockmode);
extern Relation relation_openrv_extended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok,
    bool isSupportSynonym = false, StringInfo detailInfo = NULL);
extern void relation_close(Relation relation, LOCKMODE lockmode);
#define bucketCloseRelation(bucket)  releaseDummyRelation(&(bucket))
extern Relation bucketGetRelation(Relation rel, Partition part, int2 bucketId);
extern Relation heap_open(Oid relationId, LOCKMODE lockmode, int2 bucketid=-1);
extern Relation heap_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation HeapOpenrvExtended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok,
    bool isSupportSynonym = false, StringInfo detailInfo = NULL);
extern Partition bucketGetPartition(Partition part, int2 bucketid);
extern void bucketClosePartition(Partition bucket);
extern void HeapShrinkRelation(Relation relation);

#define heap_close(r,l)  relation_close(r,l)

/* struct definition appears in relscan.h */
typedef struct ParallelHeapScanDescData *ParallelHeapScanDesc;

/*
 * HeapScanIsValid
 *		True iff the heap scan is valid.
 */
#define HeapScanIsValid(scan) PointerIsValid(scan)

extern TableScanDesc heap_beginscan(
    Relation relation, Snapshot snapshot, int nkeys, ScanKey key, RangeScanInRedis rangeScanInRedis = {false,0,0});
extern TableScanDesc heap_beginscan_strat(
    Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync);
extern TableScanDesc heap_beginscan_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key);
extern TableScanDesc heap_beginscan_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    bool allow_strat, bool allow_sync, RangeScanInRedis rangeScanInRedis);

extern void heapgetpage(TableScanDesc scan, BlockNumber page, bool* has_cur_xact_write = NULL);

extern void heap_invalid_invisible_tuple(HeapTuple tuple);

extern void heap_invalid_invisible_tuple(HeapTuple tuple);

extern void heap_rescan(TableScanDesc sscan, ScanKey key);
extern void heap_endscan(TableScanDesc scan);

extern HeapTuple heap_getnext(TableScanDesc scan, ScanDirection direction, bool* has_cur_xact_write = NULL);
extern bool HeapamGetNextBatchMode(TableScanDesc scan, ScanDirection direction);

extern void heap_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir);
extern void HeapParallelscanInitialize(ParallelHeapScanDesc target, Relation relation);
extern TableScanDesc HeapBeginscanParallel(Relation, ParallelHeapScanDesc);

extern HeapTuple heapGetNextForVerify(TableScanDesc scan, ScanDirection direction, bool& isValidRelationPage);
extern bool heap_fetch(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer *userbuf, bool keepBuf,
    Relation statsRelation, bool* has_cur_xact_write = NULL);
extern bool TableIndexFetchTupleCheck(Relation rel, ItemPointer tid, Snapshot snapshot, bool *allDead);

extern bool heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
    HeapTuple heapTuple, HeapTupleHeaderData* uncompressTup, bool* all_dead, bool first_call,
    bool* has_cur_xact_write = NULL);
extern bool heap_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot, bool* all_dead);

extern void heap_get_latest_tid(Relation relation, Snapshot snapshot, ItemPointer tid);

extern void setLastTid(const ItemPointer tid);
extern void heap_get_max_tid(Relation rel, ItemPointer ctid);

extern BulkInsertState GetBulkInsertState(void);
extern void FreeBulkInsertState(BulkInsertState);

extern Oid heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate,
    bool istoast = false);
extern void heap_abort_speculative(Relation relation, HeapTuple tuple);
extern bool heap_page_prepare_for_xid(
    Relation relation, Buffer buffer, TransactionId xid, bool multi, bool pageReplication = false);
extern bool heap_change_xidbase_after_freeze(Relation relation, Buffer buffer);

extern bool rewrite_page_prepare_for_xid(Page page, TransactionId xid, bool multi);
extern int heap_multi_insert(Relation relation, Relation parent, HeapTuple* tuples, int ntuples, CommandId cid, int options,
    BulkInsertState bistate, HeapMultiInsertExtraArgs* args);
extern TM_Result heap_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck, 
    bool wait, TM_FailureData *tmfd, bool allow_delete_self = false);
extern TM_Result heap_update(Relation relation, Relation parentRelation, ItemPointer otid, HeapTuple newtup,
    CommandId cid, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
    bool allow_delete_self = false);
extern TM_Result heap_lock_tuple(Relation relation, HeapTuple tuple, Buffer* buffer, 
    CommandId cid, LockTupleMode mode, LockWaitPolicy waitPolicy, bool follow_updates, TM_FailureData *tmfd,
    bool allow_lock_self = false, int waitSec = 0);
void FixInfomaskFromInfobits(uint8 infobits, uint16 *infomask, uint16 *infomask2);

extern void heap_inplace_update(Relation relation, HeapTuple tuple, bool waitFlush = false);
extern bool heap_freeze_tuple(HeapTuple tuple, TransactionId cutoff_xid, TransactionId cutoff_multi,
    bool *changedMultiXid = NULL);
extern bool heap_tuple_needs_freeze(HeapTuple tuple, TransactionId cutoff_xid, MultiXactId cutoff_multi, Buffer buf);
extern TransactionId MultiXactIdGetUpdateXid(TransactionId xmax, uint16 t_infomask, uint16 t_infomask2);

extern Oid simple_heap_insert(Relation relation, HeapTuple tup);
extern void simple_heap_delete(Relation relation, ItemPointer tid, int options = 0, bool allow_update_self = false);
extern void simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup, bool allow_update_self = false);
extern MultiXactStatus GetMXactStatusForLock(LockTupleMode mode, bool isUpdate);

extern void heap_markpos(TableScanDesc scan);
extern void heap_restrpos(TableScanDesc scan);

extern void heap_sync(Relation relation, LOCKMODE lockmode = RowExclusiveLock);

extern void partition_sync(Relation rel, Oid partitionId, LOCKMODE toastLockmode);

static inline void ReportPartitionOpenError(Relation relation, Oid partition_id)
{
#ifndef FRONTEND
    ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
        errmsg("partition %u does not exist on relation \"%s\"", partition_id, RelationGetRelationName(relation)),
        errdetail("this partition may have already been dropped"),
        errcause("If there is a DDL operation, the cause is incorrect operation. Otherwise, it is a system error."),
        erraction("Retry this operation after the DDL operation finished or Contact engineer for support.")));
#endif
}

static inline void ReportNoExistPartition(PartStatus partStatus, Oid partOid)
{
    if (partStatus == PART_METADATA_NOEXIST && module_logging_is_on(MOD_GPI)) {
        ereport(LOG, (errmodule(MOD_GPI), errmsg("Partition %u does not exist in GPI", partOid)));
    }
}

extern void heap_redo(XLogReaderState* rptr);
extern void heap_desc(StringInfo buf, XLogReaderState* record);
extern const char* heap_type_name(uint8 subtype);
extern void heap2_redo(XLogReaderState* rptr);
extern void heap2_desc(StringInfo buf, XLogReaderState* record);
extern const char* heap2_type_name(uint8 subtype);
extern void heap3_redo(XLogReaderState* rptr);
extern void heap3_desc(StringInfo buf, XLogReaderState* record);
extern const char* heap3_type_name(uint8 subtype);
extern void heap_bcm_redo(xl_heap_bcm* xlrec, RelFileNode node, XLogRecPtr lsn);

extern XLogRecPtr log_heap_cleanup_info(const RelFileNode* rnode, TransactionId latestRemovedXid);
extern XLogRecPtr log_heap_clean(Relation reln, Buffer buffer, OffsetNumber* redirected, int nredirected,
    OffsetNumber* nowdead, int ndead, OffsetNumber* nowunused, int nunused, TransactionId latestRemovedXid,
    bool repair_fragmentation);
extern XLogRecPtr log_heap_freeze(Relation reln, Buffer buffer, TransactionId cutoff_xid, MultiXactId cutoff_multi,
    OffsetNumber* offsets, int offcnt);
extern XLogRecPtr log_heap_invalid(Relation reln, Buffer buffer, TransactionId cutoff_xid, OffsetNumber* offsets,
    int offcnt);
extern XLogRecPtr log_heap_visible(RelFileNode rnode, BlockNumber block, Buffer heap_buffer, Buffer vm_buffer,
    TransactionId cutoff_xid, bool free_dict);
extern XLogRecPtr log_cu_bcm(const RelFileNode* rnode, int col, uint64 block, int status, int count);
extern XLogRecPtr log_heap_bcm(const RelFileNode* rnode, int col, uint64 block, int status);
extern XLogRecPtr log_newpage(RelFileNode* rnode, ForkNumber forkNum, BlockNumber blk, Page page, bool page_std, 
    TdeInfo* tdeinfo = NULL);
extern XLogRecPtr log_logical_newpage(
    RelFileNode* rnode, ForkNumber forkNum, BlockNumber blk, Page page, Buffer buffer);
extern XLogRecPtr log_logical_newcu(
    RelFileNode* rnode, ForkNumber forkNum, int attid, Size offset, int size, char* cuData);
extern XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std, TdeInfo* tde_info = NULL);
/* in heap/pruneheap.c */
extern void heap_page_prune_opt(Relation relation, Buffer buffer);
extern int heap_page_prune(Relation relation, Buffer buffer, TransactionId OldestXmin, bool report_stats,
    TransactionId* latestRemovedXid, bool repairFragmentation);
extern void heap_page_prune_execute(Page page, OffsetNumber* redirected, int nredirected, OffsetNumber* nowdead,
    int ndead, OffsetNumber* nowunused, int nunused, bool repairFragmentation);
extern void heap_get_root_tuples(Page page, OffsetNumber* root_offsets);

extern IndexFetchTableData * heapam_index_fetch_begin(Relation rel);
extern void heapam_index_fetch_reset(IndexFetchTableData *scan);
extern void heapam_index_fetch_end(IndexFetchTableData *scan);
extern HeapTuple heapam_index_fetch_tuple(IndexScanDesc scan, bool *all_dead, bool* has_cur_xact_write = NULL);

/* in heap/syncscan.c */
extern void ss_report_location(Relation rel, BlockNumber location);
extern BlockNumber ss_get_location(Relation rel, BlockNumber relnblocks);
extern void SyncScanShmemInit(void);
extern Size SyncScanShmemSize(void);

extern void PushHeapPageToDataQueue(Buffer buffer);

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid.
 *	Beware of multiple evaluations of snapshot argument.
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
extern bool HeapTupleSatisfiesVisibility(HeapTuple stup, Snapshot snapshot, Buffer buffer,
    bool* has_cur_xact_write = NULL);
extern void HeapTupleCheckVisible(Snapshot snapshot, HeapTuple tuple, Buffer buffer);

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum {
    HEAPTUPLE_DEAD,               /* tuple is dead and deletable */
    HEAPTUPLE_LIVE,               /* tuple is live (committed, no deleter) */
    HEAPTUPLE_RECENTLY_DEAD,      /* tuple is dead, but not deletable yet */
    HEAPTUPLE_INSERT_IN_PROGRESS, /* inserting xact is still in progress */
    HEAPTUPLE_DELETE_IN_PROGRESS  /* deleting xact is still in progress */
} HTSV_Result;

typedef enum {
    CheckLockTupleMode,
    CheckMultiXactLockMode,
    CheckXactLockTableMode,
    CheckSubXactLockTableMode
} CheckWaitLockMode;


/* Special "satisfies" routines with different APIs */
extern TM_Result HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid, Buffer buffer, bool self_visible = false);
extern HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin, Buffer buffer, bool isAnalyzing = false);
extern bool HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin);
extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer, uint16 infomask, TransactionId xid);
extern bool HeapTupleIsOnlyLocked(HeapTuple tuple);

/*
 * To avoid leaking to much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not tqual.c.
 */
extern bool ResolveCminCmaxDuringDecoding(
    struct HTAB* tuplecid_data, Snapshot snapshot, HeapTuple htup, Buffer buffer, CommandId* cmin, CommandId* cmax);
extern TableScanDesc heap_beginscan_internal(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    uint32 flags, ParallelHeapScanDesc parallel_scan, RangeScanInRedis rangeScanInRedis = {false, 0, 0});
#ifdef USE_SPQ
extern Relation try_table_open(Oid relationId, LOCKMODE lockmode);
#endif
#endif /* HEAPAM_H */
