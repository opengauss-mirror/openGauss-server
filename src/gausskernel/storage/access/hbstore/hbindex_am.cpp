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
 * ---------------------------------------------------------------------------------------
 *
 * hbindex_am.cpp
 *  hash bucket index access method.
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/hbstore/hbindex_am.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/hbucket_am.h"
#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
#include "utils/tqual.h"
#include "workload/workload.h"
#include "catalog/pg_hashbucket_fn.h"

void hbkt_idx_rescan(AbsIdxScanDesc scan, ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys);
void hbkt_idx_endscan(AbsIdxScanDesc scan);
void hbkt_idx_markpos(AbsIdxScanDesc scan);
void hbkt_idx_restrpos(AbsIdxScanDesc scan);
ItemPointer hbkt_idx_getnext_tid(AbsIdxScanDesc scan, ScanDirection direction);
HeapTuple hbkt_idx_fetch_heap(AbsIdxScanDesc scan);
HeapTuple hbkt_idx_getnext(AbsIdxScanDesc scan, ScanDirection direction);
int64 hbkt_idx_getbitmap(AbsIdxScanDesc scan, TIDBitmap *bitmap);

const IndexAm g_HBucketIdxAm = {
    .idx_rescan = hbkt_idx_rescan,
    .idx_rescan_local = hbkt_idx_rescan_local,
    .idx_rescan_bitmap = hbkt_idx_rescan_bitmap,
    .idx_endscan = hbkt_idx_endscan,
    .idx_markpos = hbkt_idx_markpos,
    .idx_restrpos = hbkt_idx_restrpos,
    .idx_getnext_tid = hbkt_idx_getnext_tid,
    .idx_fetch_heap = hbkt_idx_fetch_heap,
    .idx_getnext = hbkt_idx_getnext,
	.idx_getbitmap = hbkt_idx_getbitmap
};

HBktIdxScanDesc hbkt_idx_beginscan(Relation heap_relation,
    Relation index_relation,
    Snapshot snapshot,
    int nkeys, int norderbys,
    ScanState* scan_state)
{
    Assert(scan_state != NULL);
    HBktIdxScanDesc hp_scan = NULL;
    List* bucket_list = NIL;
    BucketInfo* bucket_info = ((Scan*)(scan_state->ps.plan))->bucketInfo;

    /* Step 1: load bucket */
    bucket_list = hbkt_load_buckets(heap_relation, bucket_info);
    if (bucket_list == NIL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hp_scan = (HBktIdxScanDesc)palloc0(sizeof(HBktIdxScanDescData));
    hp_scan->sd.type = T_ScanDesc_HBucketIndex;
    hp_scan->sd.idxAm = &g_HBucketIdxAm;
    hp_scan->rs_rd = heap_relation;
    hp_scan->idx_rd = index_relation;
    hp_scan->scanState = scan_state;
    hp_scan->hBktList = bucket_list;

    /* Step 3: open first partitioned relation */
    hp_scan->curr_slot = 0;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);	
    hp_scan->currBktHeapRel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);
    hp_scan->currBktIdxRel  = bucketGetRelation(hp_scan->idx_rd, NULL, bucketid);

    /* Step 4: open a partitioned AbsIdxScanDesc */
    hp_scan->currBktIdxScan = index_beginscan(hp_scan->currBktHeapRel, 
                                             hp_scan->currBktIdxRel,
                                             snapshot, nkeys, norderbys);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return hp_scan;
}

static inline void free_hbucket_idxscan(IndexScanDesc idx_scan, Relation bkt_heap_rel, Relation bkt_idx_rel)
{
    index_endscan(idx_scan);
    bucketCloseRelation(bkt_heap_rel);
    bucketCloseRelation(bkt_idx_rel);
}

void hbkt_idx_endscan(AbsIdxScanDesc scan)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    Assert(hp_scan->currBktIdxRel);
    free_hbucket_idxscan(hp_scan->currBktIdxScan, hp_scan->currBktHeapRel, hp_scan->currBktIdxRel);

    list_free_ext(hp_scan->hBktList);

    pfree(hp_scan);
}

void hbkt_idx_rescan_local(AbsIdxScanDesc scan, ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);
    index_rescan(hp_scan->currBktIdxScan, keys, nkeys, orderbys, norderbys);
}

void hbkt_idx_rescan(AbsIdxScanDesc scan, ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;

    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    Snapshot snapshot = hp_scan->currBktIdxScan->xs_snapshot;
    free_hbucket_idxscan(hp_scan->currBktIdxScan, hp_scan->currBktHeapRel, hp_scan->currBktIdxRel);

    hp_scan->curr_slot = 0;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);	
    hp_scan->currBktHeapRel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);
    hp_scan->currBktIdxRel  = bucketGetRelation(hp_scan->idx_rd, NULL, bucketid);

    hp_scan->currBktIdxScan = index_beginscan(hp_scan->currBktHeapRel, 
	                                         hp_scan->currBktIdxRel,
	                                         snapshot, 
	                                         nkeys, 
	                                         norderbys);
    hp_scan->currBktIdxScan->xs_want_itup = true;
    index_rescan(hp_scan->currBktIdxScan, keys, nkeys, orderbys, norderbys);
}

void hbkt_idx_rescan_bitmap(AbsIdxScanDesc scan, ScanKey keys, int nkeys,
    ScanKey orderbys, int norderbys)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    Snapshot snapshot = hp_scan->currBktIdxScan->xs_snapshot;
    free_hbucket_idxscan(hp_scan->currBktIdxScan, hp_scan->currBktHeapRel, hp_scan->currBktIdxRel);

    hp_scan->curr_slot = 0;
    int bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);
    hp_scan->currBktIdxRel = bucketGetRelation(hp_scan->idx_rd, NULL, bucketid);

    hp_scan->currBktIdxScan = index_beginscan_bitmap(hp_scan->currBktIdxRel,
        snapshot, nkeys);

    hp_scan->currBktIdxScan->xs_want_itup = true;
    index_rescan(hp_scan->currBktIdxScan, keys, nkeys, orderbys, norderbys);
}

void hbkt_idx_markpos(AbsIdxScanDesc scan)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    index_markpos(hp_scan->currBktIdxScan);
}

void hbkt_idx_restrpos(AbsIdxScanDesc scan)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    index_restrpos(hp_scan->currBktIdxScan);
}

/*
 * If the scan of current index partition is finished, we continue to switch and scan
 * the next non-empty partition
 */
static ItemPointer switch_and_scan_next_idx_hbkt(HBktIdxScanDesc hp_scan, ScanDirection direction)
{
    Relation next_part_rel = NULL;
    Relation next_idx_rel = NULL;
    IndexScanDesc next_part_scan = NULL;
    IndexScanDesc curr_part_scan = hp_scan->currBktIdxScan;
    ItemPointer tid = NULL;

    for (;;) {
        /* Step 1. To check whether all partition have been scanned. */
        if (hp_scan->curr_slot + 1 >= list_length(hp_scan->hBktList)) {
            return NULL;
        }

        /* Step 2. Get the next partition and its relation */
        hp_scan->curr_slot++;
        int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);		
        next_part_rel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);
        next_idx_rel  = bucketGetRelation(hp_scan->idx_rd, NULL, bucketid);

        /* Step 3. Build a HeapScan to fetch tuples */
        next_part_scan = index_beginscan(next_part_rel, next_idx_rel,
            curr_part_scan->xs_snapshot, curr_part_scan->numberOfKeys,
            curr_part_scan->numberOfOrderBys);
        if (next_part_scan == NULL) {
            /* release opened bucket relation and index */
            bucketCloseRelation(next_part_rel);
            bucketCloseRelation(next_idx_rel);
            continue;
        }

        /* Step 4. pass the scankeys to the nextPartScan */
        next_part_scan->xs_want_itup = true;
        index_rescan(next_part_scan, curr_part_scan->keyData, curr_part_scan->numberOfKeys,
            curr_part_scan->orderByData, curr_part_scan->numberOfOrderBys);

        /* Step 5. Fetch a tuple from the next partition, if no tuple is
		 * in this partition, then release the handles and continue to scan
		 * the next partition */
        tid = index_getnext_tid(next_part_scan, direction);
        if (tid == NULL) {
            free_hbucket_idxscan(next_part_scan, next_part_rel, next_idx_rel);
            continue;
        }
        /* Step 6. If the next partition has tuples, then we switch the old scan
		 * to next */
        free_hbucket_idxscan(hp_scan->currBktIdxScan, hp_scan->currBktHeapRel, hp_scan->currBktIdxRel);

        hp_scan->currBktHeapRel = next_part_rel;
        hp_scan->currBktIdxRel  = next_idx_rel;
        hp_scan->currBktIdxScan = next_part_scan;
        return tid;
    }

    return NULL;
}

ItemPointer hbkt_idx_getnext_tid(AbsIdxScanDesc scan, ScanDirection direction)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucketIndex);

    ItemPointer tidptr = index_getnext_tid(hp_scan->currBktIdxScan, direction);
    if (tidptr != NULL) {
        return tidptr;
    }

    return switch_and_scan_next_idx_hbkt(hp_scan, direction);
}

HeapTuple hbkt_idx_getnext(AbsIdxScanDesc scan, ScanDirection direction)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucketIndex);

    HeapTuple heap_tuple;
    ItemPointer tid;
    IndexScanDescData* curr_idx_scan = hp_scan->currBktIdxScan;

    for (;;) {
        /* IO collector and IO scheduler */
        if (u_sess->attr.attr_resource.use_workload_manager) {
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
        }

        if (curr_idx_scan->xs_continue_hot) {
            /*
			 * We are resuming scan of a HOT chain after having returned an
			 * earlier member.	Must still hold pin on current heap page.
			 */
            Assert(BufferIsValid(curr_idx_scan->xs_cbuf));
            Assert(ItemPointerGetBlockNumber(&curr_idx_scan->xs_ctup.t_self) == 
                   BufferGetBlockNumber(curr_idx_scan->xs_cbuf));
        } else {
            /* Time to fetch the next TID from the index */
            tid = hbkt_idx_getnext_tid(scan, direction);
            /* If we're out of index entries, we're done */
            if (tid == NULL) {
                break;
            }
            curr_idx_scan = hp_scan->currBktIdxScan;
        }

        /*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */

        heap_tuple = index_fetch_heap(curr_idx_scan);
        if (heap_tuple != NULL) {
            Assert(!HEAP_TUPLE_IS_COMPRESSED(heap_tuple->t_data));
            return heap_tuple;
        }
    }

    return NULL; /* failure exit */
}

HeapTuple hbkt_idx_fetch_heap(AbsIdxScanDesc scan)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucketIndex);

    return index_fetch_heap(hp_scan->currBktIdxScan);
}

HBktIdxScanDesc hbkt_idx_beginscan_bitmap(Relation index_relation,
    Snapshot snapshot,
    int nkeys,
    ScanState* scan_state)
{
    HBktIdxScanDesc hp_scan = NULL;
    Relation idx_relation = NULL;
    List* bucket_list = NIL;
    BucketInfo* bucket_info = ((Scan*)(scan_state->ps.plan))->bucketInfo;

    Assert(scan_state != NULL);

    /* Step 1: load partition */
    bucket_list = hbkt_load_buckets(index_relation, bucket_info);
    if (bucket_list == NIL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hp_scan = (HBktIdxScanDesc)palloc0(sizeof(HBktIdxScanDescData));
    hp_scan->sd.type = T_ScanDesc_HBucketIndex;
    hp_scan->sd.idxAm = &g_HBucketIdxAm;
    hp_scan->idx_rd = index_relation;
    hp_scan->scanState = scan_state;
    hp_scan->hBktList = bucket_list;

    /* Step 3: open first partitioned relation */
    hp_scan->curr_slot = 0;	
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);
    idx_relation = bucketGetRelation(index_relation, NULL, bucketid);
    hp_scan->currBktIdxRel = idx_relation;

    /* Step 4: open a partitioned AbsIdxScanDesc */
    hp_scan->currBktIdxScan = index_beginscan_bitmap(idx_relation, snapshot, nkeys);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return hp_scan;
}

bool hbkt_idx_bitmapscan_switch_bucket(AbsIdxScanDesc scan, int target_slot)
{
    HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
    Assert(scan->type == T_ScanDesc_HBucketIndex);

    Relation idx_rel = NULL;
    IndexScanDesc part_scan = NULL;
    BitmapIndexScanState* index_state = (BitmapIndexScanState*)hp_scan->scanState;

    /* Step 1. To check whether all partition have been scanned. */
    if (target_slot >= list_length(hp_scan->hBktList)) {
        return false;
    }
    hp_scan->curr_slot = target_slot;

    /* Step 2. get part from current slot */
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);
    idx_rel = bucketGetRelation(hp_scan->idx_rd, NULL, bucketid);

    /* Step 3. Build a indexBitmapScan */
    part_scan = index_beginscan_bitmap(idx_rel,
        hp_scan->currBktIdxScan->xs_snapshot, index_state->biss_NumScanKeys);

    index_rescan(part_scan, hp_scan->currBktIdxScan->keyData, hp_scan->currBktIdxScan->numberOfKeys, NULL, 0);

    /* Step 4. end current indexBitmapScan */
    index_endscan(hp_scan->currBktIdxScan);
    bucketCloseRelation(hp_scan->currBktIdxRel);

    hp_scan->currBktIdxRel = idx_rel;
    hp_scan->currBktIdxScan = part_scan;
    return true;
}

int64 hbkt_idx_getbitmap(AbsIdxScanDesc scan, TIDBitmap *bitmap)
{
	HBktIdxScanDesc hp_scan = (HBktIdxScanDesc)scan;
	Assert(scan->type == T_ScanDesc_HBucketIndex);

	return index_getbitmap(hp_scan->currBktIdxScan, bitmap);
}

