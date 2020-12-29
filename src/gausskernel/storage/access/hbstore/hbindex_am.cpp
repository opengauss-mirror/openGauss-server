/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * hbindex_am.cpp
 *	  hash bucket index access method.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/hbstore/hbindex_am.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "access/hbucket_am.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
#include "workload/workload.h"
#include "catalog/pg_hashbucket_fn.h"
#include "optimizer/bucketpruning.h"

static IndexScanDesc hbkt_idx_beginscan(Relation heapRelation,
    Relation indexRelation,
    Snapshot snapshot,
    int nkeys, int norderbys,
    ScanState* scanState)
{
    Assert(scanState != NULL);
    HBktIdxScanDesc hpScan = NULL;
    List *bucketlist = NIL;
    BucketInfo *bucketInfo = ((Scan *)(scanState->ps.plan))->bucketInfo;
    /*
     * for global plan cache, there  isn't bucketInfo in plan,
     * so we cal bucketInfo for further pruning here.
     */
    if ((bucketInfo == NULL || bucketInfo->buckets == NIL) && ENABLE_GPC) {
        bucketInfo = CalBucketInfo(scanState);
    }

    /* Step 1: load bucket */
    bucketlist = hbkt_load_buckets(heapRelation, bucketInfo);
    if (bucketlist == NIL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hpScan = (HBktIdxScanDesc)palloc0(sizeof(HBktIdxScanDescData));
    hpScan->rs_rd = heapRelation;
    hpScan->idx_rd = indexRelation;
    hpScan->scanState = scanState;
    hpScan->hBktList = bucketlist;

    /* Step 3: open first partitioned relation */
    hpScan->curr_slot = 0;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    hpScan->currBktHeapRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
    hpScan->currBktIdxRel = bucketGetRelation(hpScan->idx_rd, NULL, bucketid);

    /* Step 4: open a partitioned IndexScanDesc */
    hpScan->currBktIdxScan = index_beginscan(hpScan->currBktHeapRel, hpScan->currBktIdxRel, snapshot, nkeys, norderbys);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return (IndexScanDesc)hpScan;
}

static inline void free_hbucket_idxscan(IndexScanDesc idxscan, Relation bktHeapRel, Relation bktIdxRel)
{
    index_endscan(idxscan);
    bucketCloseRelation(bktHeapRel);
    bucketCloseRelation(bktIdxRel);
}

static void hbkt_idx_endscan(IndexScanDesc scan)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;
    Assert(hpScan);
    Assert(hpScan->currBktIdxRel);
    free_hbucket_idxscan(hpScan->currBktIdxScan, hpScan->currBktHeapRel, hpScan->currBktIdxRel);

    list_free_ext(hpScan->hBktList);

    pfree(hpScan);
}


void hbkt_idx_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;

    Assert(hpScan);

    Snapshot snapshot = hpScan->currBktIdxScan->xs_snapshot;
    free_hbucket_idxscan(hpScan->currBktIdxScan, hpScan->currBktHeapRel, hpScan->currBktIdxRel);

    hpScan->curr_slot = 0;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    hpScan->currBktHeapRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
    hpScan->currBktIdxRel = bucketGetRelation(hpScan->idx_rd, NULL, bucketid);

    hpScan->currBktIdxScan = (IndexScanDesc)index_beginscan(hpScan->currBktHeapRel, hpScan->currBktIdxRel, snapshot, nkeys, norderbys);
    hpScan->currBktIdxScan->xs_want_itup = true;
    index_rescan(hpScan->currBktIdxScan, keys, nkeys, orderbys, norderbys);
}

/*
 * If the scan of current index partition is finished, we continue to switch and scan
 * the next non-empty partition
 */
static ItemPointer switch_and_scan_next_idx_hbkt(HBktIdxScanDesc hpScan, ScanDirection direction)
{
    Relation nextPartRel = NULL;
    Relation nextIdxRel = NULL;
    IndexScanDesc nextPartScan = NULL;
    IndexScanDesc currPartScan = hpScan->currBktIdxScan;
    ItemPointer tidptr = NULL;

    while (true) {
        /* Step 1. To check whether all partition have been scanned. */
        if (hpScan->curr_slot + 1 >= list_length(hpScan->hBktList)) {
            return NULL;
        }

        /* Step 2. Get the next partition and its relation */
        hpScan->curr_slot++;
        int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
        nextPartRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
        nextIdxRel = bucketGetRelation(hpScan->idx_rd, NULL, bucketid);

        /* Step 3. Build a HeapScan to fetch tuples */
        nextPartScan = (IndexScanDesc)index_beginscan(nextPartRel, nextIdxRel, currPartScan->xs_snapshot, currPartScan->numberOfKeys,
                                       currPartScan->numberOfOrderBys);
        if (nextPartScan == NULL) {
            /* release opened bucket relation and index */
            bucketCloseRelation(nextPartRel);
            bucketCloseRelation(nextIdxRel);
            continue;
        }

        /* Step 4. pass the scankeys to the nextPartScan */
        nextPartScan->xs_want_itup = true;
        index_rescan(nextPartScan, currPartScan->keyData, currPartScan->numberOfKeys, currPartScan->orderByData,
                     currPartScan->numberOfOrderBys);

        /* Step 5. Fetch a tuple from the next partition, if no tuple is
         * in this partition, then release the handles and continue to scan
         * the next partition */
        tidptr = index_getnext_tid(nextPartScan, direction);
        if (tidptr == NULL) {
            free_hbucket_idxscan(nextPartScan, nextPartRel, nextIdxRel);
            continue;
        }
        /* Step 6. If the next partition has tuples, then we switch the old scan
         * to next */
        free_hbucket_idxscan(hpScan->currBktIdxScan, hpScan->currBktHeapRel, hpScan->currBktIdxRel);

        hpScan->currBktHeapRel = nextPartRel;
        hpScan->currBktIdxRel = nextIdxRel;
        hpScan->currBktIdxScan = nextPartScan;
        return tidptr;
    }

    return NULL;
}

static ItemPointer hbkt_idx_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;


    ItemPointer tidptr = index_getnext_tid(hpScan->currBktIdxScan, direction);
    if (tidptr != NULL) {
        return tidptr;
    }

    return switch_and_scan_next_idx_hbkt(hpScan, direction);
}

HeapTuple hbkt_idx_getnext(IndexScanDesc scan, ScanDirection direction)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;
    HeapTuple heapTuple;
    ItemPointer tid;
    IndexScanDescData *currIdxScan = hpScan->currBktIdxScan;

    for (;;) {
        /* IO collector and IO scheduler */
        if (u_sess->attr.attr_resource.use_workload_manager) {
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
        }

        if (currIdxScan->xs_continue_hot) {
            /*
             * We are resuming scan of a HOT chain after having returned an
             * earlier member.	Must still hold pin on current heap page.
             */
            Assert(BufferIsValid(currIdxScan->xs_cbuf));
            Assert(ItemPointerGetBlockNumber(&currIdxScan->xs_ctup.t_self) ==
                   BufferGetBlockNumber(currIdxScan->xs_cbuf));
        } else {
            /* Time to fetch the next TID from the index */
            tid = hbkt_idx_getnext_tid(scan, direction);
            /* If we're out of index entries, we're done */
            if (tid == NULL) {
                break;
            }
            currIdxScan = hpScan->currBktIdxScan;
        }

        /*
         * Fetch the next (or only) visible heap tuple for this index entry.
         * If we don't find anything, loop around and grab the next TID from
         * the index.
         */

        heapTuple = index_fetch_heap(currIdxScan);
        if (heapTuple != NULL) {
            Assert(!HEAP_TUPLE_IS_COMPRESSED(heapTuple->t_data));
            return heapTuple;
        }
    }

    return NULL; /* failure exit */
}


static IndexScanDesc hbkt_idx_beginscan_bitmap(Relation indexRelation, Snapshot snapshot, int nkeys, ScanState *scanState)
{
    HBktIdxScanDesc hpScan = NULL;
    Relation idxRelation = NULL;
    List *bucketlist = NIL;
    BucketInfo *bucketInfo = ((Scan *)(scanState->ps.plan))->bucketInfo;
    /*
     * for global plan cache, there  isn't bucketInfo in plan,
     * so we cal bucketInfo for further pruning here.
     */
    if ((bucketInfo == NULL || bucketInfo->buckets == NIL) && ENABLE_GPC) {
        bucketInfo = CalBucketInfo(scanState);
    }

    Assert(scanState != NULL);

    /* Step 1: load partition */
    bucketlist = hbkt_load_buckets(indexRelation, bucketInfo);
    if (bucketlist == NIL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hpScan = (HBktIdxScanDesc)palloc0(sizeof(HBktIdxScanDescData));
    hpScan->idx_rd = indexRelation;
    hpScan->scanState = scanState;
    hpScan->hBktList = bucketlist;

    /* Step 3: open first partitioned relation */
    hpScan->curr_slot = 0;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    idxRelation = bucketGetRelation(indexRelation, NULL, bucketid);
    hpScan->currBktIdxRel = idxRelation;

    /* Step 4: open a partitioned IndexScanDesc */
    hpScan->currBktIdxScan = index_beginscan_bitmap(idxRelation, snapshot, nkeys);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return (IndexScanDesc)hpScan;
}

bool hbkt_idx_bitmapscan_switch_bucket(IndexScanDesc scan, int targetSlot)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;
    Relation idxRel = NULL;
    IndexScanDesc partScan = NULL;
    BitmapIndexScanState *indexstate = (BitmapIndexScanState *)hpScan->scanState;

    /* Step 1. To check whether all partition have been scanned. */
    if (targetSlot >= list_length(hpScan->hBktList)) {
        return false;
    }
    hpScan->curr_slot = targetSlot;

    /* Step 2. get part from current slot */
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    idxRel = bucketGetRelation(hpScan->idx_rd, NULL, bucketid);

    /* Step 3. Build a indexBitmapScan */
    partScan = (IndexScanDesc)index_beginscan_bitmap(idxRel, hpScan->currBktIdxScan->xs_snapshot, indexstate->biss_NumScanKeys);

    index_rescan(partScan, hpScan->currBktIdxScan->keyData, hpScan->currBktIdxScan->numberOfKeys, NULL, 0);

    /* Step 4. end current indexBitmapScan */
    index_endscan(hpScan->currBktIdxScan);
    bucketCloseRelation(hpScan->currBktIdxRel);

    hpScan->currBktIdxRel = idxRel;
    hpScan->currBktIdxScan = partScan;
    return true;
}

int64 hbkt_idx_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
    HBktIdxScanDesc hpScan = (HBktIdxScanDesc)scan;
    return index_getbitmap(hpScan->currBktIdxScan, bitmap);
}

/* ------------------------------------------------------------------------
 * common scan handler
 *     Common SCAN HANDLER for hbkt or non-hbkt table scan operations
 *     Reconstruct these functions into the hook API in the future.
 * ------------------------------------------------------------------------
 */

IndexScanDesc scan_handler_idx_beginscan(Relation heap_relation, Relation index_relation, Snapshot snapshot, int nkeys, int norderbys, ScanState* scan_state)
{
    if (unlikely(RELATION_OWN_BUCKET(heap_relation))) {
        return hbkt_idx_beginscan(heap_relation, index_relation, snapshot, nkeys, norderbys, scan_state);
    } else {
        return index_beginscan(heap_relation, index_relation, snapshot, nkeys, norderbys, scan_state);
    }
}

IndexScanDesc scan_handler_idx_beginscan_bitmap(Relation indexRelation, Snapshot snapshot, int nkeys, ScanState* scan_state)
{
    if (unlikely(RELATION_OWN_BUCKET(indexRelation))) {
        return hbkt_idx_beginscan_bitmap(indexRelation, snapshot, nkeys, scan_state);
    } else {
        return index_beginscan_bitmap(indexRelation, snapshot, nkeys, scan_state);
    }
}

void scan_handler_idx_rescan(IndexScanDesc scan, ScanKey key, int nkeys, ScanKey orderbys, int norderbys)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        hbkt_idx_rescan(scan, key, nkeys, orderbys, norderbys);
    } else {
        index_rescan(scan, key, nkeys, orderbys, norderbys);
    }
}

void scan_handler_idx_rescan_local(IndexScanDesc scan, ScanKey key, int nkeys, ScanKey orderbys, int norderbys)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        index_rescan(((HBktIdxScanDesc)scan)->currBktIdxScan, key, nkeys, orderbys, norderbys);
    } else {
        index_rescan(scan, key, nkeys, orderbys, norderbys);
    }
}

void scan_handler_idx_endscan(IndexScanDesc scan)
{    
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        hbkt_idx_endscan(scan);
    } else {
        index_endscan(scan);
    }
}

void scan_handler_idx_markpos(IndexScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        index_markpos(((HBktIdxScanDesc)scan)->currBktIdxScan);
    } else {
        index_markpos(scan);
    }
}

void scan_handler_idx_restrpos(IndexScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        index_restrpos(((HBktIdxScanDesc)scan)->currBktIdxScan);
    } else {
        index_restrpos(scan);
    }
}

HeapTuple scan_handler_idx_fetch_heap(IndexScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        return index_fetch_heap(((HBktIdxScanDesc)scan)->currBktIdxScan);
    } else {
        return index_fetch_heap(scan);
    }
}

HeapTuple scan_handler_idx_getnext(IndexScanDesc scan, ScanDirection direction)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        return hbkt_idx_getnext(scan, direction);
    } else {
        return index_getnext(scan, direction);
    }
}

ItemPointer scan_handler_idx_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        return hbkt_idx_getnext_tid(scan, direction);
    } else {
        return index_getnext_tid(scan, direction);
    }
}

int64 scan_handler_idx_getbitmap(IndexScanDesc scan, TIDBitmap* bitmap)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->indexRelation))) {
        return hbkt_idx_getbitmap(scan, bitmap);
    } else {
        return index_getbitmap(scan, bitmap);
    }
}
