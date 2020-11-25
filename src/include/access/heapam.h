/* -------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
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
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "storage/pagecompress.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/snapshot.h"
#include "replication/bcm.h"

/* "options" flag bits for heap_insert */
#define HEAP_INSERT_SKIP_WAL 0x0001
#define HEAP_INSERT_SKIP_FSM 0x0002
#define HEAP_INSERT_FROZEN 0x0004
#define HEAP_INSERT_SPECULATIVE 0x0008

typedef struct BulkInsertStateData* BulkInsertState;

typedef enum { LockTupleShared, LockTupleExclusive } LockTupleMode;

/* the last arguments info for heap_multi_insert() */
typedef struct {
    /* compression info: dictionary buffer and its size */
    const char* dictData;
    int dictSize;

    /* forbid to use page replication */
    bool disablePageReplication;
} HeapMultiInsertExtraArgs;

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
extern Relation try_relation_open(Oid relationId, LOCKMODE lockmode);
extern Relation relation_openrv(const RangeVar* relation, LOCKMODE lockmode);
extern Relation relation_openrv_extended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok,
    bool isSupportSynonym = false, StringInfo detailInfo = NULL);
extern void relation_close(Relation relation, LOCKMODE lockmode);
#define bucketCloseRelation(bucket)  releaseDummyRelation(&(bucket))
extern Relation bucketGetRelation(Relation rel, Partition part, int2 bucketId);
extern Relation heap_open(Oid relationId, LOCKMODE lockmode, int2 bucketid=-1);
extern Relation heap_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation heap_openrv_extended(const RangeVar* relation, LOCKMODE lockmode, bool missing_ok,
    bool isSupportSynonym = false, StringInfo detailInfo = NULL);
extern Partition bucketGetPartition(Partition part, int2 bucketid);
extern void bucketClosePartition(Partition bucket);

#define heap_close(r,l)  relation_close(r,l)

/* struct definition appears in relscan.h */
typedef struct HeapScanDescData* HeapScanDesc;
typedef struct ParallelHeapScanDescData *ParallelHeapScanDesc;

/*
 * HeapScanIsValid
 *		True iff the heap scan is valid.
 */
#define HeapScanIsValid(scan) PointerIsValid(scan)

extern HeapScanDesc heap_beginscan(
    Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool isRangeScanInRedis = false);
extern HeapScanDesc heap_beginscan_strat(
    Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync);
extern HeapScanDesc heap_beginscan_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key);
extern HeapScanDesc heap_beginscan_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    bool allow_strat, bool allow_sync, bool isRangeScanInRedis);

extern void heapgetpage(HeapScanDesc scan, BlockNumber page);

extern void heap_rescan(HeapScanDesc scan, ScanKey key);
extern void heap_endscan(HeapScanDesc scan);
extern HeapTuple heap_getnext(HeapScanDesc scan, ScanDirection direction);

extern Size heap_parallelscan_estimate(Snapshot snapshot);
extern void heap_parallelscan_initialize(ParallelHeapScanDesc target, Size pscan_len, Relation relation,
    Snapshot snapshot);
extern void heap_parallelscan_reinitialize(ParallelHeapScanDesc parallel_scan);
extern HeapScanDesc heap_beginscan_parallel(Relation relation, ParallelHeapScanDesc parallel_scan);

extern void heap_init_parallel_seqscan(HeapScanDesc scan, int32 dop, ScanDirection dir);

extern HeapTuple heapGetNextForVerify(HeapScanDesc scan, ScanDirection direction, bool& isValidRelationPage);
extern bool heap_fetch(
    Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer* userbuf, bool keep_buf, Relation stats_relation);
extern bool heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
    HeapTuple heapTuple, HeapTupleHeaderData* uncompressTup, bool* all_dead, bool first_call);
extern bool heap_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot, bool* all_dead);

extern void heap_get_latest_tid(Relation relation, Snapshot snapshot, ItemPointer tid);

extern void setLastTid(const ItemPointer tid);
extern void heap_get_max_tid(Relation rel, ItemPointer ctid);

extern BulkInsertState GetBulkInsertState(void);
extern void FreeBulkInsertState(BulkInsertState);

extern Oid heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate);
extern void heap_abort_speculative(Relation relation, HeapTuple tuple);
extern bool heap_page_prepare_for_xid(
    Relation relation, Buffer buffer, TransactionId xid, bool multi, bool pageReplication = false);
extern bool heap_change_xidbase_after_freeze(Relation relation, Buffer buffer);

extern bool rewrite_page_prepare_for_xid(Page page, TransactionId xid, bool multi);
extern int heap_multi_insert(Relation relation, Relation parent, HeapTuple* tuples, int ntuples, CommandId cid, int options,
    BulkInsertState bistate, HeapMultiInsertExtraArgs* args);
extern HTSU_Result heap_delete(Relation relation, ItemPointer tid, ItemPointer ctid, TransactionId* update_xmax,
    CommandId cid, Snapshot crosscheck, bool wait, bool allow_delete_self = false);
extern HTSU_Result heap_update(Relation relation, Relation parentRelation, ItemPointer otid, HeapTuple newtup,
    ItemPointer ctid, TransactionId* update_xmax, CommandId cid, Snapshot crosscheck,
    bool wait, bool allow_update_self = false);
extern HTSU_Result heap_lock_tuple(Relation relation, HeapTuple tuple, Buffer* buffer, ItemPointer ctid,
    TransactionId* update_xmax, CommandId cid, LockTupleMode mode, bool nowait, bool allow_lock_self = false);

extern void heap_inplace_update(Relation relation, HeapTuple tuple);
extern bool heap_freeze_tuple(HeapTuple tuple, TransactionId cutoff_xid);
extern bool heap_tuple_needs_freeze(HeapTuple tuple, TransactionId cutoff_xid, Buffer buf);

extern Oid simple_heap_insert(Relation relation, HeapTuple tup);
extern void simple_heap_delete(Relation relation, ItemPointer tid, int options = 0);
extern void simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup);

extern void heap_markpos(HeapScanDesc scan);
extern void heap_restrpos(HeapScanDesc scan);

extern void heap_sync(Relation relation, LOCKMODE lockmode = RowExclusiveLock);

extern void partition_sync(Relation rel, Oid partitionId, LOCKMODE toastLockmode);

extern void heap_redo(XLogReaderState* rptr);
extern void heap_desc(StringInfo buf, XLogReaderState* record);
extern void heap2_redo(XLogReaderState* rptr);
extern void heap_bcm_redo(xl_heap_bcm* xlrec, RelFileNode node, XLogRecPtr lsn);
extern void heap2_desc(StringInfo buf, XLogReaderState* record);
extern void heap3_redo(XLogReaderState* rptr);
extern void heap3_desc(StringInfo buf, XLogReaderState* record);

extern bool heap_page_upgrade(Relation relation, Buffer buffer);
extern void heap_page_upgrade_nocheck(Relation relation, Buffer buffer);

extern XLogRecPtr log_heap_cleanup_info(const RelFileNode* rnode, TransactionId latestRemovedXid);
extern XLogRecPtr log_heap_clean(Relation reln, Buffer buffer, OffsetNumber* redirected, int nredirected,
    OffsetNumber* nowdead, int ndead, OffsetNumber* nowunused, int nunused, TransactionId latestRemovedXid,
    bool repair_fragmentation);
extern XLogRecPtr log_heap_freeze(
    Relation reln, Buffer buffer, TransactionId cutoff_xid, OffsetNumber* offsets, int offcnt);
extern XLogRecPtr log_heap_visible(RelFileNode rnode, BlockNumber block, Buffer heap_buffer, Buffer vm_buffer,
    TransactionId cutoff_xid, bool free_dict);
extern XLogRecPtr log_cu_bcm(const RelFileNode* rnode, int col, uint64 block, int status, int count);
extern XLogRecPtr log_heap_bcm(const RelFileNode* rnode, int col, uint64 block, int status);
extern XLogRecPtr log_newpage(RelFileNode* rnode, ForkNumber forkNum, BlockNumber blk, Page page, bool page_std);
extern XLogRecPtr log_logical_newpage(
    RelFileNode* rnode, ForkNumber forkNum, BlockNumber blk, Page page, Buffer buffer);
extern XLogRecPtr log_logical_newcu(
    RelFileNode* rnode, ForkNumber forkNum, int attid, Size offset, int size, char* cuData);
extern XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std);
/* in heap/pruneheap.c */
extern void heap_page_prune_opt(Relation relation, Buffer buffer);
extern int heap_page_prune(Relation relation, Buffer buffer, TransactionId OldestXmin, bool report_stats,
    TransactionId* latestRemovedXid, bool repairFragmentation);
extern void heap_page_prune_execute(Page page, OffsetNumber* redirected, int nredirected, OffsetNumber* nowdead,
    int ndead, OffsetNumber* nowunused, int nunused, bool repairFragmentation);
extern void heap_get_root_tuples(Page page, OffsetNumber* root_offsets);

/* in heap/syncscan.c */
extern void ss_report_location(Relation rel, BlockNumber location);
extern BlockNumber ss_get_location(Relation rel, BlockNumber relnblocks);
extern void SyncScanShmemInit(void);
extern Size SyncScanShmemSize(void);

extern void PushHeapPageToDataQueue(Buffer buffer);

#endif /* HEAPAM_H */
