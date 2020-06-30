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
 * hbucket_am.cpp
 *
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/hbstore/hbucket_am.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "utils/builtins.h"
#include "access/hbucket_am.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "knl/knl_session.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/memutils.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "catalog/pg_hashbucket_fn.h"
#include "nodes/makefuncs.h"


const TableAm g_HBucketTblAm = {
    .table_endscan = hbkt_tbl_endscan,
    .table_rescan = hbkt_tbl_rescan,
    .table_getnext = hbkt_tbl_getnext,
    .table_markpos = hbkt_tbl_markpos,
    .table_restrpos = hbkt_tbl_restrpos,
    .table_getpage = hbkt_tbl_getpage,
    .table_init_parallel_seqscan = hbkt_tbl_init_parallel_seqscan
};

List* hbkt_load_buckets(Relation relation, BucketInfo* bkt_info)
{
    ListCell *bkt_id_cell = NULL;
    List    *bucket_list = NIL;
	oidvector *blist = searchHashBucketByOid(relation->rd_bucketoid);

    if (bkt_info == NULL || bkt_info->buckets == NIL) {
		// load all buckets
        for (int i = 0; i < blist->dim1; i++) {
            bucket_list = lappend_int(bucket_list, blist->values[i]);
        }
    } else { 
		// load selected buckets
        foreach (bkt_id_cell, bkt_info->buckets) {
            int bkt_id = lfirst_int(bkt_id_cell);
            if (bkt_id < 0 || bkt_id >= BUCKETDATALEN) {
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("buckets id %d of table is outsize range [%d,%d]",
                        bkt_id, 0, BUCKETDATALEN - 1)));
            }
            if (lookupHBucketid(blist, 0, bkt_id) == -1) {
                continue;
            }
            bucket_list = lappend_int(bucket_list, bkt_id);
        }
    }
    return bucket_list;
}

static inline void free_hbucket_scan(HeapScanDesc bkt_scan, Relation bkt_rel)
{
    Assert(bkt_rel != NULL);
    heap_endscan(bkt_scan);
    bucketCloseRelation(bkt_rel);
}

static HBktTblScanDesc hbkt_tbl_create_scan(Relation relation, ScanState* state)
{
    HBktTblScanDesc hp_scan = NULL;
    List* bucket_list = NIL;
    BucketInfo* bkt_info = NULL;

    if (state != NULL) {
        bkt_info = ((SeqScan*)(state->ps.plan))->bucketInfo;
    }

    /* Step 1: load bucket */
    bucket_list = hbkt_load_buckets(relation, bkt_info);
    if (bucket_list == NIL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hp_scan = (HBktTblScanDesc)palloc0(sizeof(HBktTblScanDescData));
    hp_scan->sd.type = T_ScanDesc_HBucket;
    hp_scan->sd.tblAm = &g_HBucketTblAm;
    hp_scan->rs_rd = relation;
    hp_scan->scanState = (ScanState*)state;
    hp_scan->hBktList = bucket_list;

    /* Step 3: open first bucketed relation */
    hp_scan->curr_slot = 0;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);		
    hp_scan->currBktRel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);
    return hp_scan;
}

HBktTblScanDesc hbkt_tbl_beginscan(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* state /* = NULL*/)
{
    HBktTblScanDesc hp_scan = hbkt_tbl_create_scan(relation, state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hp_scan == NULL) {
        return NULL;
    }

    /* Step 4: open a bucketed AbsTblScanDesc */
    bool isRangeScanInRedis = reset_scan_qual(hp_scan->currBktRel, state);
    hp_scan->currBktScan = heap_beginscan(hp_scan->currBktRel, snapshot,
        nkeys, key, isRangeScanInRedis);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return hp_scan;
}

HBktTblScanDesc hbkt_tbl_beginscan_bm(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* scan_state)
{
    HBktTblScanDesc hp_scan = hbkt_tbl_create_scan(relation, scan_state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hp_scan == NULL) {
        return NULL;
    }

    /* Step 4: open a bucketed AbsTblScanDesc */
    hp_scan->currBktScan = heap_beginscan_bm(hp_scan->currBktRel, snapshot, nkeys, key);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return hp_scan;
}

HBktTblScanDesc hbkt_tbl_beginscan_sampling(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, bool allow_strat, bool allow_sync, ScanState* scan_state)
{
    HBktTblScanDesc hp_scan = hbkt_tbl_create_scan(relation, scan_state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hp_scan == NULL) {
        return NULL;
    }

    /* Step 4: open a hash-bucket HeapScanDesc */
    bool isRangeScanInRedis = reset_scan_qual(hp_scan->currBktRel, scan_state);
    hp_scan->currBktScan = heap_beginscan_sampling(hp_scan->currBktRel, snapshot,
        nkeys, key, allow_strat, allow_sync, isRangeScanInRedis);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return hp_scan;
}

HBktTblScanDesc hbkt_tbl_begin_tidscan(Relation relation, ScanState* state)
{
    HBktTblScanDesc hp_scan = hbkt_tbl_create_scan(relation, state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hp_scan == NULL) {
        state->ps.stubType = PST_Scan;
    }
    return hp_scan;
}

void hbkt_tbl_end_tidscan(AbsTblScanDesc scan)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
    Assert(hp_scan);

    bucketCloseRelation(hp_scan->currBktRel);

    list_free_ext(hp_scan->hBktList);
    Assert(hp_scan->hBktList == NIL);

    pfree(scan);
}

void hbkt_tbl_endscan(AbsTblScanDesc scan)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
    Assert(hp_scan);
    Assert(scan->type == T_ScanDesc_HBucket);

    free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);

    list_free_ext(hp_scan->hBktList);
    Assert(hp_scan->hBktList == NIL);

    pfree(scan);
}

void hbkt_tbl_rescan(AbsTblScanDesc scan, ScanKey key)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
    Assert(scan->type == T_ScanDesc_HBucket);

    Snapshot snapshot = hp_scan->currBktScan->rs_snapshot;
    int nkeys = hp_scan->currBktScan->rs_nkeys;

    free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);

    hp_scan->curr_slot = 0;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);		
    hp_scan->currBktRel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);

    bool isRangeScanInRedis = reset_scan_qual(hp_scan->currBktRel, hp_scan->scanState);
    hp_scan->currBktScan = heap_beginscan(hp_scan->currBktRel, snapshot, nkeys, key, isRangeScanInRedis);
    heap_rescan(hp_scan->currBktScan, key);
}

static inline void try_init_bucket_parallel(HeapScanDesc next_bkt_scan, ScanState* sstate)
{
    if (sstate != NULL && *(NodeTag*)sstate == T_SeqScanState) {
        heap_init_parallel_seqscan(next_bkt_scan,
            sstate->ps.plan->dop, sstate->partScanDirection);
        next_bkt_scan->rs_ss_accessor = sstate->ss_scanaccessor;
    }
}

/*
 * If the scan of current bucket is finished, we continue to switch and scan
 * the next non-empty bucket
 */
static HeapTuple switch_and_scan_next_tbl_hbkt(HBktTblScanDesc hp_scan, ScanDirection direction)
{
    Relation next_bkt_rel = NULL;
    HeapScanDesc next_bkt_scan = NULL;
    HeapScanDesc curr_bkt_scan = hp_scan->currBktScan;
    HeapTuple htup = NULL;

    for (;;) {
        /* Step 1. To check whether all bucket have been scanned. */
        if (hp_scan->curr_slot + 1 >= list_length(hp_scan->hBktList)) {
            return NULL;
        }

        /* Step 2. Get the next bucket and its relation */
        hp_scan->curr_slot++;
        int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);
        next_bkt_rel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);

        /* Step 3. Build a HeapScan to fetch tuples */
        (void)reset_scan_qual(next_bkt_rel, hp_scan->scanState);
        next_bkt_scan = heap_beginscan(next_bkt_rel, curr_bkt_scan->rs_snapshot,
            curr_bkt_scan->rs_nkeys, curr_bkt_scan->rs_key,
            curr_bkt_scan->rs_isRangeScanInRedis);

        try_init_bucket_parallel(next_bkt_scan, hp_scan->scanState);

        /* Step 4. Fetch a tuple from the next bucket, if no tuple is 
		 * in this bucket, then release the handles and continue to scan 
		 * the next bucket */
        htup = heap_getnext(next_bkt_scan, direction);
        if (htup == NULL) {
            free_hbucket_scan(next_bkt_scan, next_bkt_rel);
            continue;
        }

        /* Step 5. If the next bucket has tuples, then we switch the old scan 
		 * to next */
        free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);

        hp_scan->currBktRel = next_bkt_rel;
        hp_scan->currBktScan = next_bkt_scan;
        return htup;
    }

    return NULL;
}

HeapTuple hbkt_tbl_getnext(AbsTblScanDesc scan, ScanDirection direction)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucket);

    HeapTuple htup = heap_getnext(hp_scan->currBktScan, direction);
    if (htup != NULL) {
        return htup;
    }

    return switch_and_scan_next_tbl_hbkt(hp_scan, direction);
}

/**
 * Switch the HbktScan to next bucket for sampling scan
 * @return if no bucket to switch return false; otherwise return true;
 */
bool hbkt_sampling_scan_nextbucket(HBktTblScanDesc hp_scan)
{
    Relation next_bkt_rel = NULL;
    HeapScanDesc next_bkt_scan = NULL;
    HeapScanDesc curr_bkt_scan = hp_scan->currBktScan;

    /* Step 1. To check whether all buckets have been scanned. */
    if (hp_scan->curr_slot + 1 >= list_length(hp_scan->hBktList)) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hp_scan->curr_slot++;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);	
    next_bkt_rel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);

    /* Step 3. Build a HeapScan for new bucket */
    next_bkt_scan = heap_beginscan_sampling(next_bkt_rel, curr_bkt_scan->rs_snapshot,
        curr_bkt_scan->rs_nkeys, curr_bkt_scan->rs_key,
        curr_bkt_scan->rs_allow_strat, curr_bkt_scan->rs_allow_sync,
        curr_bkt_scan->rs_isRangeScanInRedis);

    /* Step 4. Set the parallel scan parameter */
    ScanState* sstate = hp_scan->scanState;
    try_init_bucket_parallel(next_bkt_scan, sstate);

    /* Step 5. If the next bucket has tuples, then we switch the old scan 
		* to next */
    free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);

    hp_scan->currBktRel = next_bkt_rel;
    hp_scan->currBktScan = next_bkt_scan;

    return true;
}

/**
 * Switch the HbktScan to next bucket for BitmapHeap scan
 * @return if no bucket to switch return false; otherwise return true;
 */
bool hbkt_bitmapheap_scan_nextbucket(HBktTblScanDesc hp_scan)
{
    Relation next_bkt_rel = NULL;
    HeapScanDesc next_bkt_scan = NULL;
    HeapScanDesc curr_bkt_scan = hp_scan->currBktScan;

    if (hp_scan->curr_slot + 1 >= list_length(hp_scan->hBktList)) {
        return false;
    }

    /* switch to next hash bucket */
    hp_scan->curr_slot++;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);		
    next_bkt_rel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);

    next_bkt_scan = heap_beginscan_bm(next_bkt_rel, curr_bkt_scan->rs_snapshot,
		                            curr_bkt_scan->rs_nkeys, curr_bkt_scan->rs_key);
    free_hbucket_scan(curr_bkt_scan, hp_scan->currBktRel);
    hp_scan->currBktRel = next_bkt_rel;
    hp_scan->currBktScan = next_bkt_scan;

    return true;
}

void hbkt_tbl_markpos(AbsTblScanDesc scan)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucket);

    /* Note: no locking manipulations needed */
    heap_markpos(hp_scan->currBktScan);
}

void hbkt_tbl_restrpos(AbsTblScanDesc scan)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
    Assert(scan->type == T_ScanDesc_HBucket);

    heap_restrpos(hp_scan->currBktScan);
}

void hbkt_tbl_getpage(AbsTblScanDesc scan, BlockNumber page)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
    Assert(scan->type == T_ScanDesc_HBucket);

    heapgetpage(hp_scan->currBktScan, page);
}

void hbkt_tbl_init_parallel_seqscan(AbsTblScanDesc scan, int32 dop, ScanDirection dir)
{
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;

    Assert(scan->type == T_ScanDesc_HBucket);

    heap_init_parallel_seqscan(hp_scan->currBktScan, dop, dir);
}

/*
 * Switch the HbktScan to next bucket for Tid Scan
 * @return if no bucket to switch return false, otherwise return true.
 */
bool hbkt_tbl_tid_nextbucket(HBktTblScanDesc hp_scan)
{
    Relation next_bkt_rel = NULL;

    /* Step 1. To check whether all buckets have been scanned. */
    if (hp_scan->curr_slot + 1 >= list_length(hp_scan->hBktList)) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hp_scan->curr_slot++;
    int2 bucketid = list_nth_int(hp_scan->hBktList, hp_scan->curr_slot);
    next_bkt_rel = bucketGetRelation(hp_scan->rs_rd,  NULL, bucketid);

    /* Step 3. If the next bucket has tuples, then we switch the old scan 
	  * to next */
    bucketCloseRelation(hp_scan->currBktRel);

    hp_scan->currBktRel = next_bkt_rel;

    return true;
}

RedisMergeItemOrderArray *hbkt_get_merge_list_from_str(const char* merge_list)
{
    char *merge_list_str = NULL;
    ListCell *cell = NULL;
    if (merge_list == NULL || strlen(merge_list) == 0) {
        return NULL;
    }
    merge_list_str = pstrdup(merge_list);
    RedisMergeItemOrderArray    *result = NULL;
    List	   *elemlist = NIL;
    if (!SplitIdentifierString(merge_list_str, ';', &elemlist)) {
            pfree(merge_list_str);
            merge_list_str = NULL;
            list_free_ext(elemlist);
            return NULL;
    }
    int i = 0;
    result = (RedisMergeItemOrderArray *)palloc0(sizeof(RedisMergeItemOrderArray));
    result->length = list_length(elemlist);
    result->array = (RedisMergeItem *)palloc(result->length * sizeof(RedisMergeItem));
    foreach(cell, elemlist) {
        char *single_item = (char *)lfirst(cell);
        List	   *datalist = NIL;
        if (!SplitIdentifierString(single_item, ':', &datalist)) {
            pfree(merge_list_str);
            merge_list_str = NULL;
            list_free_ext(elemlist);
            list_free_ext(datalist);
            return NULL;
        }
        if ((list_length(datalist)) != 3) {
            pfree(merge_list_str);
            merge_list_str = NULL;
            list_free_ext(elemlist);
            list_free_ext(datalist);
            return NULL;
        }
        result->array[i].bktid = int2(atoi((char *)lfirst(datalist->head)));
        result->array[i].start = strtol((char *)lsecond(datalist), NULL, 10);
        result->array[i].end = strtol((char *)lthird(datalist), NULL, 10);
        i++;
        list_free_ext(datalist);
    }
    pfree(merge_list_str);
    merge_list_str = NULL;
    list_free_ext(elemlist);
    return result;
}

RedisMergeItemOrderArray *hbkt_get_merge_list_from_relopt(Relation relation)
{
    char *tmp = NULL;
    tmp = RelationGetRelMergeList(relation);
    if (tmp == NULL || strlen(tmp)) {
        return NULL;
    }
    return hbkt_get_merge_list_from_str(tmp);
}

List *hbkt_set_merge_list_to_relopt(List *rel_options, RedisMergeItemOrderArray* merge_items) {
    if (merge_items == NULL) {
        return rel_options;
    }
    size_t size = 0;
    char *merge_str = NULL;
#define SINGLE_LEN 50 + 3
    char single_str[SINGLE_LEN + 1] = {0};
    int rc;
    /* malloc the max possiable length of int+uint64+uint64+split */
    size = merge_items->length * (SINGLE_LEN) + 1;
    merge_str = (char *)palloc0(size);
    for (int i = 0; i < merge_items->length; i++) {
		rc = snprintf_s(single_str, SINGLE_LEN + 1, SINGLE_LEN, "%d:%lld:%lld;",
		    merge_items->array[i].bktid, 
		    merge_items->array[i].start, 
		    merge_items->array[i].end);
		securec_check_ss(rc, "", "");
        rc = strcat_s(merge_str, size, single_str);
        securec_check_ss(rc, "", "");
    }
    /* remove the last ';' */
    merge_str[strlen(merge_str) - 1] = 0;
    rel_options = list_delete_name(rel_options, "merge_list");
    rel_options = lappend(rel_options, makeDefElem(pstrdup("merge_list"), 
        (Node*)makeString(pstrdup(merge_str))));
    
    pfree(merge_str);
    return rel_options;
}

void freeRedisMergeItemOrderArray(RedisMergeItemOrderArray *merge_items) {
    if (merge_items == NULL) {
        return;
    }
    pfree(merge_items->array);
    merge_items->array = NULL;
    pfree(merge_items);
    merge_items = NULL;
    return ;
}

RedisMergeItem *search_redis_merge_item(const RedisMergeItemOrderArray *merge_items, const int2 bucketid) {
    int    low = 0;
	int    high = merge_items->length - 1;

	/* binary search */
	while (low <= high) {
		int  mid = (high + low) / 2;
		RedisMergeItem tmp = merge_items->array[mid];
		if (tmp.bktid == bucketid) {
			return &merge_items->array[mid];
		} else if (tmp.bktid < bucketid) {
			low = mid + 1;
		} else {
			high = mid - 1;
		}
	}
    return NULL;
}

