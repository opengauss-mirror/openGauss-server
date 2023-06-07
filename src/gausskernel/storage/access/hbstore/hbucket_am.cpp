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
 * hbucket_am.cpp
 *	  hash bucket access method.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/hbstore/hbucket_am.cpp
 *
 * -------------------------------------------------------------------------
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
#include "access/heapam.h"
#include "utils/syscache.h"
#include "catalog/pg_hashbucket_fn.h"
#include "nodes/makefuncs.h"
#include "pgxc/groupmgr.h"
#include "catalog/pgxc_class.h"
#include "optimizer/bucketpruning.h"
#include "executor/node/nodeSeqscan.h"

#ifdef ENABLE_MULTIPLE_NODES
TableScanDesc GetTableScanDesc(TableScanDesc scan, Relation rel)
{
    if (scan != NULL && rel != NULL && RELATION_CREATE_BUCKET(scan->rs_rd)) {
        return (TableScanDesc)((HBktTblScanDesc)scan)->currBktScan;
    } else {
        return scan;
    }
}

IndexScanDesc GetIndexScanDesc(IndexScanDesc scan)
{
    if (scan != NULL && RELATION_OWN_BUCKET(scan->indexRelation)) {
        return ((HBktIdxScanDesc)scan)->currBktIdxScan;
    } else {
        return (IndexScanDesc)scan;
    }
}
#endif

oidvector *hbkt_load_buckets(Relation relation, BucketInfo *bktInfo)
{
    ListCell  *bktIdCell = NULL;
    oidvector *bucketList = NULL;
    oidvector *blist = searchHashBucketByOid(relation->rd_bucketoid);

    if (bktInfo == NULL || bktInfo->buckets == NIL) {
        // load all buckets
        bucketList = buildoidvector(blist->values, blist->dim1);
    } else {
        // load selected buckets
        Oid  bucketTmp[BUCKETDATALEN];
        bool needSort  = false;
        int  bucketCnt = 0;
        int  prevBktId = -1;

        foreach (bktIdCell, bktInfo->buckets) {
            int bktId = lfirst_int(bktIdCell);
            if (bktId < 0 || bktId >= BUCKETDATALEN) {
                ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("buckets id %d of table is outsize range [%d,%d]", bktId, 0, BUCKETDATALEN - 1)));
            }
            if (lookupHBucketid(blist, 0, bktId) != -1) {
                Assert(bktId != prevBktId);
                if (bktId < prevBktId) {
                    needSort = true;
                }
                bucketTmp[bucketCnt++] = bktId;
                prevBktId = bktId;
            }
        }
        /* Make sure bucketlist is in ascending order */
        if (needSort) {
            qsort(bucketTmp, bucketCnt, sizeof(Oid), bid_cmp);
        }
        if (bucketCnt != 0) {
            bucketList = buildoidvector(bucketTmp, bucketCnt);
        }
    }
    return bucketList;
}

static void free_hbucket_scan(TableScanDesc bktScan, Relation bktRel)
{
    tableam_scan_end(bktScan);
    if (bktRel != NULL) {
        bucketCloseRelation(bktRel);
    }
}

static TableScanDesc hbkt_tbl_create_scan(Relation relation, ScanState *state)
{
    HBktTblScanDesc hpScan = NULL;
    oidvector *bucketlist = NULL;
    BucketInfo *bktInfo = NULL;

    if (state != NULL) {
        bktInfo = ((SeqScan *)(state->ps.plan))->bucketInfo;
        /*
         * for global plan cache, there  isn't bucketInfo in plan,
         * so we cal bucketInfo for further pruning here.
         */
        if ((bktInfo == NULL || bktInfo->buckets == NIL) && ENABLE_GPC) {
            bktInfo = CalBucketInfo(state);
        }
    }

    /* Step 1: load bucket */
    bucketlist = hbkt_load_buckets(relation, bktInfo);
    if (bucketlist == NULL) {
        return NULL;
    }

    /* Step 2: allocate and initialize scan descriptor */
    hpScan = (HBktTblScanDesc)palloc0(sizeof(HBktTblScanDescData));
    hpScan->rs_rd = relation;
    hpScan->scanState = (ScanState *)state;
    hpScan->hBktList = bucketlist;

    if (RelationIsPartitioned(hpScan->rs_rd)) {
        /*
         * It is a partition-hashbucket table, just set the
         * parent relation as the target initially.
         * The reason why we are here is only because we are
         * going to do cross-partition and cross-bucket
         * indexscan in underlying layer.
         */
        hpScan->currBktRel = hpScan->rs_rd;
    } else {
        /* Step 3: open first bucketed relation */
        hpScan->curr_slot = 0;
        int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
        hpScan->currBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
    }

    return (TableScanDesc)hpScan;
}

static TableScanDesc hbkt_tbl_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
                                   ScanState *state, bool isRangeScanInRedis)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)hbkt_tbl_create_scan(relation, state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hpScan == NULL) {
        return NULL;
    }

    /* Step 4: open a bucketed AbsTblScanDesc */
    RangeScanInRedis rangeScanInRedis = reset_scan_qual(hpScan->currBktRel, state, isRangeScanInRedis);
    hpScan->currBktScan = tableam_scan_begin(hpScan->currBktRel, snapshot, nkeys, key, rangeScanInRedis);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return (TableScanDesc)hpScan;
}

static TableScanDesc hbkt_tbl_beginscan_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
                                      ScanState *scanState)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)hbkt_tbl_create_scan(relation, scanState);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hpScan == NULL) {
        return NULL;
    }

    /* Step 4: open a bucketed AbsTblScanDesc */
    hpScan->currBktScan = tableam_scan_begin_bm(hpScan->currBktRel, snapshot, nkeys, key);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return (TableScanDesc)hpScan;
}

static TableScanDesc hbkt_tbl_beginscan_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
                                            bool allow_strat, bool allow_sync, ScanState *scanState)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)hbkt_tbl_create_scan(relation, scanState);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hpScan == NULL) {
        return NULL;
    }

    /* Step 4: open a hash-bucket HeapScanDesc */
    RangeScanInRedis rangeScanInRedis = reset_scan_qual(hpScan->currBktRel, scanState);
    hpScan->currBktScan = tableam_scan_begin_sampling(hpScan->currBktRel, snapshot, nkeys, key, allow_strat, allow_sync, rangeScanInRedis);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    return (TableScanDesc)hpScan;
}

TableScanDesc hbkt_tbl_begin_tidscan(Relation relation, ScanState *state)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)hbkt_tbl_create_scan(relation, state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hpScan == NULL) {
        state->ps.stubType = PST_Scan;
    }
    return (TableScanDesc)hpScan;
}

static void hbkt_tbl_end_tidscan(TableScanDesc scan)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;

    pfree_ext(hpScan->hBktList);

    pfree(hpScan);
}

static void hbkt_tbl_rescan(TableScanDesc scan, ScanKey key, bool is_bitmap_rescan)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;

    Snapshot snapshot = hpScan->currBktScan->rs_snapshot;
    int nkeys = hpScan->currBktScan->rs_nkeys;
    Relation bktHeapRel = (hpScan->rs_rd == hpScan->currBktRel) ? NULL : hpScan->currBktRel;
    free_hbucket_scan(hpScan->currBktScan, bktHeapRel);

    if (RelationIsPartitioned(hpScan->rs_rd)) {
        /*
         * It is a partition-hashbucket table, just set the
         * parent relation as the target initially.
         * The reason why we are here is only because we are
         * going to do cross-partition and cross-bucket
         * indexscan in underlying layer.
         */
        hpScan->currBktRel = hpScan->rs_rd;
    } else {
        /* Step 3: open first bucketed relation */
        hpScan->curr_slot = 0;
        int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
        hpScan->currBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
    }

    RangeScanInRedis rangeScanInRedis = reset_scan_qual(hpScan->currBktRel, hpScan->scanState);
    if (!is_bitmap_rescan) {
        hpScan->currBktScan = tableam_scan_begin(hpScan->currBktRel, snapshot, nkeys, key, rangeScanInRedis);
    } else {
        hpScan->currBktScan = tableam_scan_begin_bm(hpScan->currBktRel, snapshot, nkeys, key);
    }
    tableam_scan_rescan(hpScan->currBktScan, key);
}

static void try_init_bucket_parallel(TableScanDesc nextBktScan, ScanState *sstate)
{
    if (sstate != NULL && *(NodeTag *)sstate == T_SeqScanState) {
        scan_handler_tbl_init_parallel_seqscan(nextBktScan, sstate->ps.plan->dop, sstate->partScanDirection);
        nextBktScan->rs_ss_accessor = sstate->ss_scanaccessor;
    }
}

/*
 * If the scan of current bucket is finished, we continue to switch and scan
 * the next non-empty bucket
 */
static HeapTuple switch_and_scan_next_tbl_hbkt(TableScanDesc scan, ScanDirection direction)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;
    Relation nextBktRel = NULL;
    TableScanDesc nextBktScan = NULL;
    TableScanDesc currBktScan = hpScan->currBktScan;
    HeapTuple htup = NULL;

    while (true) {
        /* Step 1. To check whether all bucket have been scanned. */
        if (hpScan->curr_slot + 1 >=  hpScan->hBktList->dim1) {
            return NULL;
        }

        /* Step 2. Get the next bucket and its relation */
        hpScan->curr_slot++;
        int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
        nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

        /* Step 3. Build a HeapScan to fetch tuples */
        (void)reset_scan_qual(nextBktRel, hpScan->scanState);
        nextBktScan = tableam_scan_begin(nextBktRel, currBktScan->rs_snapshot, currBktScan->rs_nkeys, currBktScan->rs_key,
                                     currBktScan->rs_rangeScanInRedis);

        try_init_bucket_parallel(nextBktScan, hpScan->scanState);

        /* Step 4. Fetch a tuple from the next bucket, if no tuple is
         * in this bucket, then release the handles and continue to scan
         * the next bucket */
        htup = (HeapTuple) tableam_scan_getnexttuple(nextBktScan, direction);
        if (htup == NULL) {
            free_hbucket_scan(nextBktScan, nextBktRel);
            continue;
        }

        /* Step 5. If the next bucket has tuples, then we switch the old scan
         * to next */
        free_hbucket_scan(hpScan->currBktScan, hpScan->currBktRel);

        hpScan->currBktRel = nextBktRel;
        hpScan->currBktScan = nextBktScan;
        return htup;
    }

    return NULL;
}


/**
 * Switch the HbktScan to next bucket for sampling scan
 * @return if no bucket to switch return false; otherwise return true;
 */
bool hbkt_sampling_scan_nextbucket(TableScanDesc scan)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;
    Relation nextBktRel = NULL;
    TableScanDesc nextBktScan = NULL;
    TableScanDesc currBktScan = hpScan->currBktScan;

    /* Step 1. To check whether all buckets have been scanned. */
    if (hpScan->curr_slot + 1 >= hpScan->hBktList->dim1) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hpScan->curr_slot++;
    int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
    nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    /* Step 3. Build a HeapScan for new bucket */
    nextBktScan = tableam_scan_begin_sampling(nextBktRel, currBktScan->rs_snapshot, currBktScan->rs_nkeys, currBktScan->rs_key,
        ((currBktScan->rs_flags & SO_ALLOW_STRAT) != 0), ((currBktScan->rs_flags & SO_ALLOW_SYNC) != 0), currBktScan->rs_rangeScanInRedis);

    /* Step 4. Set the parallel scan parameter */
    ScanState *sstate = hpScan->scanState;
    try_init_bucket_parallel(nextBktScan, sstate);

    /* Step 5. If the next bucket has tuples, then we switch the old scan
     * to next */
    free_hbucket_scan(hpScan->currBktScan, hpScan->currBktRel);

    hpScan->currBktRel = nextBktRel;
    hpScan->currBktScan = nextBktScan;

    return true;
}

/**
 * Switch the HbktScan to next bucket for BitmapHeap scan
 * @return if no bucket to switch return false; otherwise return true;
 */
bool hbkt_bitmapheap_scan_nextbucket(HBktTblScanDesc hpScan)
{
    Relation nextBktRel = NULL;
    TableScanDesc nextBktScan = NULL;
    TableScanDesc currBktScan = hpScan->currBktScan;

    if (hpScan->curr_slot + 1 >= hpScan->hBktList->dim1) {
        return false;
    }

    /* switch to next hash bucket */
    hpScan->curr_slot++;
    int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
    nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    nextBktScan = tableam_scan_begin_bm(nextBktRel, currBktScan->rs_snapshot, currBktScan->rs_nkeys, currBktScan->rs_key);
    free_hbucket_scan(currBktScan, hpScan->currBktRel);
    hpScan->currBktRel = nextBktRel;
    hpScan->currBktScan = nextBktScan;

    return true;
}

bool cbi_bitmapheap_scan_nextbucket(HBktTblScanDesc hpscan, GPIScanDesc gpiscan, CBIScanDesc cbiscan)
{
    Assert(hpscan != NULL);
    Assert(cbiscan != NULL);

    Relation targetheap;
    TableScanDesc currbktscan, nextbktscan;

    /* vailidation check */
    if (lookupHBucketid(hpscan->hBktList, 0, cbiscan->bucketid) == -1) {
        cbiscan->bucketid = InvalidBktId;
        return false;
    }

    currbktscan = hpscan->currBktScan;
    if (gpiscan != NULL && gpiscan->partition != NULL) { /* partition is not NULL indicates this is a global index */
        Assert(gpiscan->parentRelation != NULL);
        targetheap = bucketGetRelation(gpiscan->parentRelation, gpiscan->partition, cbiscan->bucketid);
    } else {
        targetheap = bucketGetRelation(hpscan->rs_rd, NULL, cbiscan->bucketid);
    }
    nextbktscan = tableam_scan_begin_bm(targetheap, currbktscan->rs_snapshot, currbktscan->rs_nkeys,
        currbktscan->rs_key);
    if (hpscan->currBktRel != hpscan->rs_rd && RelationIsBucket(hpscan->currBktRel)) {
        free_hbucket_scan(currbktscan, hpscan->currBktRel);
    }
    hpscan->currBktRel = targetheap;
    hpscan->currBktScan = nextbktscan;

    return true;
}

Relation cbi_bitmapheap_scan_getbucket(const HBktTblScanDesc hpscan, const GPIScanDesc gpiscan,
    const CBIScanDesc cbiscan, int2 bucketid)
{
    Relation targetheap = NULL;

    Assert(hpscan != NULL);
    Assert(cbiscan != NULL);

    /* vailidation check */
    if (lookupHBucketid(hpscan->hBktList, 0, bucketid) == -1) {
        return NULL;
    } 

    if (cbi_scan_need_change_bucket(cbiscan, bucketid)) {
        if (gpiscan != NULL && gpiscan->partition != NULL) {
            /* for cross-partition and cross-bucket */
            Assert(gpiscan->parentRelation != NULL);
            targetheap = bucketGetRelation(gpiscan->parentRelation, gpiscan->partition, bucketid);
        } else {
            /* for cross-bucket */
            targetheap = bucketGetRelation(hpscan->rs_rd, NULL, bucketid);
        }
    } else {
        targetheap = hpscan->currBktRel;
    }

    return targetheap;
}

/*
 * Switch the HbktScan to next bucket for Tid Scan
 * @return if no bucket to switch return false, otherwise return true.
 */
bool hbkt_tbl_tid_nextbucket(HBktTblScanDesc hpScan)
{
    Relation nextBktRel = NULL;

    /* Step 1. To check whether all buckets have been scanned. */
    if (hpScan->curr_slot + 1 >= hpScan->hBktList->dim1) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hpScan->curr_slot++;
    int2 bucketid = hpScan->hBktList->values[hpScan->curr_slot];
    nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    /* Step 3. If the next bucket has tuples, then we switch the old scan
     * to next */
    bucketCloseRelation(hpScan->currBktRel);

    hpScan->currBktRel = nextBktRel;

    return true;
}

static bool hbkt_parse_one_item(char* startPtr, char** endPtr,
    RedisMergeItem* curItem)
{
    bool parser_finish = false;

#define BASE 10

    /* 1. parse bucketid */
    Assert(startPtr);
    errno = 0;
    curItem->bktid = (int2)strtoll(startPtr, endPtr, BASE);
    if ((errno == ERANGE && (curItem->bktid == LLONG_MAX || curItem->bktid == LLONG_MIN)) || startPtr == *endPtr ||
        **endPtr == '\0') {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to parse bucketid from merge_list: %m")));
    }

    if (curItem->bktid < InvalidBktId || curItem->bktid > BUCKETDATALEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("bucketid parsed from merge_list is inivalid: %d", curItem->bktid)));
    }

    /* 2. parse start ctid */
    startPtr = *endPtr + 1; /* add 1 delimiter */
    Assert(startPtr);
    curItem->start = strtoll(startPtr, endPtr, BASE);
    if ((errno == ERANGE && ((int64)curItem->start == LLONG_MAX || (int64)curItem->start == LLONG_MIN)) ||
        startPtr == *endPtr || **endPtr == '\0') {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to parse start ctid from merge_list: %m")));
    }

    /* 3. parse end ctid */
    startPtr = *endPtr + 1; /* add 1 delimiter */
    Assert(startPtr);
    curItem->end = strtoll(startPtr, endPtr, BASE);
    if ((errno == ERANGE && ((int64)curItem->end == LLONG_MAX || (int64)curItem->end == LLONG_MIN)) ||
        startPtr == *endPtr) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to parse end ctid from merge_list: %m")));
    }

    if (**endPtr == '\0') {
        parser_finish = true;
    }

    return parser_finish;
}

/*
 * Parse all merge items from merge_list.
 */
RedisMergeItem *hbkt_get_merge_item_internal(char* merge_list,
    int merge_list_length, RedisMergeItemOrderArray *result, int2 bucketid)
{
    char* startPtr = merge_list;
    char* endPtr;
    int curItemIdx = 0;
    char* last_item_str = NULL;
    RedisMergeItem* targetItem = NULL;
    bool found = false;

    Assert(merge_list);
    Assert(merge_list_length > 0);
    Assert(result);
    Assert(bucketid < SegmentBktId);

    if (strncmp(merge_list, NOT_EXIST_MERGE_LIST, strlen(NOT_EXIST_MERGE_LIST)) == 0) {
        return NULL;
    }
    /* only get one item */
    if (bucketid > InvalidBktId) {
        targetItem = (RedisMergeItem*)palloc0(sizeof(RedisMergeItem));
    }

    while (true) {
        /*
         * Parse merge items one by one. The format of merge_list
         * is "bucketid:start:end;bucketid:start:end;....". For example,
         * "0:0:10;1:0:100;2:100:1000".
         */
        RedisMergeItem *curItem;
        Assert(curItemIdx < result->length);

        if (bucketid > InvalidBktId) { /* for get one item with target bucketid */
            curItem = targetItem;
        } else {
            curItem = &result->itemarray[curItemIdx];
        }

        /* parse one item from merge_list */
        bool parse_finish = hbkt_parse_one_item(startPtr, &endPtr, curItem);
        curItemIdx++;

        /* find the target bucketid */
        if (bucketid > InvalidBktId && curItem->bktid == bucketid) {
            found = true;
            break;
        }

        /* already parsed all itms */
        if (parse_finish || curItemIdx == result->length) {
            break;
        }

        /* sanity check */
        if (endPtr - merge_list > merge_list_length) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("failed to parse merge list, out of boundary")));
        }

        /* prepare for parsing next item */
        startPtr = endPtr + 1; /* add 1 delimiter */

        /*
         * merge_list may be not end with '\0', we have to
         * palloc another space to copy the origin string
         * of last item to make sure end with '\0'.
         */
        if (curItemIdx == result->length - 1) {
            int remain_len = merge_list_length - (startPtr - merge_list);
            last_item_str = (char*)palloc0(remain_len + 1);

            errno_t rc = memcpy_s(last_item_str, remain_len, startPtr, remain_len);
            securec_check(rc, "\0", "\0");
            startPtr = last_item_str;
        }
    }

    pfree_ext(last_item_str);

    /* return null and free memory if not match bucketid */
    if (bucketid > InvalidBktId && !found) {
        pfree_ext(targetItem);
    }

    return targetItem;
}


RedisMergeItemOrderArray *hbkt_get_all_merge_item(char* merge_list,
    int merge_list_length)
{
    RedisMergeItemOrderArray *result = NULL;
    int length = 0;
    int i;

    if (merge_list == NULL || merge_list_length == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("merge_list could not be NULL")));
        return NULL;
    }

    /* get length of item array */
    for (i = 0; i < merge_list_length; i++) {
        if (merge_list[i] == ';' || i == (merge_list_length - 1)) {
            length++;
        }
    }
    result = (RedisMergeItemOrderArray *)palloc(sizeof(RedisMergeItemOrderArray));
    result->length = length;
    result->itemarray = (RedisMergeItem *)palloc(result->length * sizeof(RedisMergeItem));
    (void)hbkt_get_merge_item_internal(merge_list, merge_list_length, result, InvalidBktId);
    return result;
}

RedisMergeItem *hbkt_get_one_merge_item(char* merge_list,
    int merge_list_length, int2 bucketid)
{
    int i;
    int length = 0;
    RedisMergeItemOrderArray array;

    if (merge_list == NULL || merge_list_length == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("merge_list could not be NULL")));
        return NULL;
    }

    /* get length of item array */
    for (i = 0; i < merge_list_length; i++) {
        if (merge_list[i] == ';' || i == (merge_list_length - 1)) {
            length++;
        }
    }

    array.length = length;
    /* array.itemarray will not be used in get one item mode */
    array.itemarray = NULL; 
    return hbkt_get_merge_item_internal(merge_list, merge_list_length,
                                        &array, bucketid);
}

void hbkt_set_merge_list_to_pgxc_class_option(Oid pcrelid, RedisMergeItemOrderArray *merge_items, bool first_set)
{
    size_t size = 0;
    char *merge_str = NULL;
    bool find = false;
#define SINGLE_LEN 50 + 3
    char single_str[SINGLE_LEN + 1] = {0};
    int rc;
    bool not_exist = false;
    if (merge_items == NULL) {
        not_exist = true;
        merge_str = NOT_EXIST_MERGE_LIST;
        goto update;
    }
    /* malloc the max possiable length of int+uint64+uint64+split */
    size = merge_items->length * (SINGLE_LEN) + 1;
    merge_str = (char *)palloc0(size);
    for (int i = 0; i < merge_items->length; i++) {
        rc = snprintf_s(single_str, SINGLE_LEN + 1, SINGLE_LEN, "%d:%lld:%lld;", merge_items->itemarray[i].bktid,
                        merge_items->itemarray[i].start, merge_items->itemarray[i].end);
        securec_check_ss(rc, "\0", "\0");
        rc = strcat_s(merge_str, size, single_str);
        securec_check_ss(rc, "\0", "\0");
    }
    /* remove the last ';' */
    merge_str[strlen(merge_str) - 1] = 0;
update:
    (void)searchMergeListByRelid(pcrelid, &find, false, false);
    if (find == true) {
        if (first_set == true) {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                            errmsg("find merge_str in pgxc_class and is first set %u", pcrelid)));
        }
        PgxcClassAlterForReloption(pcrelid, merge_str);
    } else {
        if (first_set == false) {
#ifdef USE_ASSERT_CHECKING
            /* skip this check for hbucketcheck */
            if (u_sess->attr.attr_storage.enable_hashbucket == true) {
                PgxcClassCreateForReloption(pcrelid, merge_str);
            } else {
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                                errmsg("not find merge_str in pgxc_class and not first set %u", pcrelid)));
            }
            if (not_exist == false) {
                pfree_ext(merge_str);
            }
            return;
#endif
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                            errmsg("not find merge_str in pgxc_class and not first set %u", pcrelid)));
        } else {
            PgxcClassCreateForReloption(pcrelid, merge_str);
        }
    }
    if (not_exist == false) {
        pfree_ext(merge_str);
    }
    return;
}

void freeRedisMergeItemOrderArray(RedisMergeItemOrderArray *merge_items)
{
    if (merge_items == NULL) {
        return;
    }
    if (merge_items->itemarray != NULL) {
        pfree(merge_items->itemarray);
        merge_items->itemarray = NULL;
    }
    pfree(merge_items);
    merge_items = NULL;
    return;
}

RedisMergeItem *search_redis_merge_item(const RedisMergeItemOrderArray *merge_items, const int2 bucketid)
{
    if (merge_items == NULL) {
        return NULL;
    }
    int low = 0;
    int high = merge_items->length - 1;

    /* binary search */
    while (low <= high) {
        int mid = (high + low) / 2;
        RedisMergeItem tmp = merge_items->itemarray[mid];
        if (tmp.bktid == bucketid) {
            return &merge_items->itemarray[mid];
        } else if (tmp.bktid < bucketid) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------------
 * common scan handler
 *     Common SCAN HANDLER for hbkt or non-hbkt table scan operations
 *     Reconstruct these functions into the hook API in the future.
 * ------------------------------------------------------------------------
 */
TableAmNdpRoutine_hook_type ndp_tableam = NULL;

/* Create HBktTblScanDesc with scan state. For hash-bucket table, it scans the
 * specified buckets in ScanState */
TableScanDesc scan_handler_tbl_beginscan(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* sstate, bool isRangeScanInRedis)
{
    if (RELATION_CREATE_BUCKET(relation)) {
        return (TableScanDesc)hbkt_tbl_beginscan(relation, snapshot, nkeys, key, sstate, isRangeScanInRedis);
    }
    RangeScanInRedis rangeScanInRedis = reset_scan_qual(relation, sstate, isRangeScanInRedis);

    if (sstate != NULL && sstate->ps.plan->ndp_pushdown_optimized) {
        return ndp_tableam->scan_begin(relation, snapshot, nkeys, key, sstate, rangeScanInRedis);
    }
    return tableam_scan_begin(relation, snapshot, nkeys, key, rangeScanInRedis);
}

TableScanDesc scan_handler_tbl_begin_tidscan(Relation relation, ScanState* state)
{
    if (unlikely(RELATION_OWN_BUCKET(relation))) {
        return hbkt_tbl_begin_tidscan(relation, state);
    } else {
        return NULL;
    }
}

void scan_handler_tbl_end_tidscan(TableScanDesc scan)
{
    if (unlikely(scan != NULL)) {
        Assert (RELATION_OWN_BUCKET(scan->rs_rd));
        hbkt_tbl_end_tidscan(scan);
    } else {
        return;
    }
}

void scan_handler_tbl_markpos(TableScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->rs_rd))) {
        tableam_scan_markpos(((HBktTblScanDesc)scan)->currBktScan);
    } else {
        tableam_scan_markpos(scan);
    }
}

void scan_handler_tbl_restrpos(TableScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->rs_rd))) {
        tableam_scan_restrpos(((HBktTblScanDesc)scan)->currBktScan);
    } else {
        tableam_scan_restrpos(scan);
    }
}

void scan_handler_tbl_init_parallel_seqscan(TableScanDesc scan, int32 dop, ScanDirection dir)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->rs_rd))) {
        tableam_scan_init_parallel_seqscan(((HBktTblScanDesc)scan)->currBktScan, dop, dir);
    } else if (scan->ndp_pushdown_optimized) {
        ndp_tableam->scan_init_parallel_seqscan(scan, dop, dir);
    } else {
        tableam_scan_init_parallel_seqscan(scan, dop, dir);
    }
}


TableScanDesc scan_handler_tbl_beginscan_bm(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* sstate)
{
    if (unlikely(RELATION_OWN_BUCKET(relation))) {
        return hbkt_tbl_beginscan_bm(relation, snapshot, nkeys, key, sstate);
    } else {
        return tableam_scan_begin_bm(relation, snapshot, nkeys, key);
    }
}

TableScanDesc scan_handler_tbl_beginscan_sampling(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, bool allow_strat, bool allow_sync, ScanState* sstate)
{
    if (RelationIsCUFormat(relation)) {
        return tableam_scan_begin_sampling(relation, snapshot,
            nkeys, key, allow_strat, allow_sync, sstate->rangeScanInRedis);
    }

    if (RELATION_CREATE_BUCKET(relation)) {
        return hbkt_tbl_beginscan_sampling(relation, snapshot, 
            nkeys, key, allow_strat, allow_sync, sstate);
    }

    RangeScanInRedis rangeScanInRedis = reset_scan_qual(relation, sstate);
    return tableam_scan_begin_sampling(relation, snapshot, nkeys, key, 
        allow_strat, allow_sync, rangeScanInRedis);
}

Tuple scan_handler_tbl_getnext(TableScanDesc scan, ScanDirection direction, Relation rel,
    bool* has_cur_xact_write)
{
    Assert(scan != NULL);
    if (unlikely(RELATION_CREATE_BUCKET(scan->rs_rd))) {
        Tuple htup = tableam_scan_getnexttuple(((HBktTblScanDesc)scan)->currBktScan, direction);
        if (htup != NULL) {
            return htup;
        }
        return (Tuple) switch_and_scan_next_tbl_hbkt(scan, direction);
    } else {
        return tableam_scan_getnexttuple(scan, direction, has_cur_xact_write);
    }
}

void scan_handler_tbl_endscan(TableScanDesc scan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->rs_rd))) {
        HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
        if (hp_scan->currBktRel != NULL && RelationIsBucket(hp_scan->currBktRel)) {
            free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);
        }
        pfree_ext(hp_scan->hBktList);
        pfree(hp_scan);
    } else if (scan->ndp_pushdown_optimized) {
        ndp_tableam->scan_end(scan);
    } else {
        tableam_scan_end(scan);
    }
}

void scan_handler_tbl_rescan(TableScanDesc scan, struct ScanKeyData* key, Relation rel, bool is_bitmap_rescan)
{
    if (unlikely(RELATION_OWN_BUCKET(scan->rs_rd))) {
        hbkt_tbl_rescan(scan, key, is_bitmap_rescan);
    } else if (scan->ndp_pushdown_optimized) {
        ndp_tableam->scan_rescan(scan, key);
    } else {
        tableam_scan_rescan(scan, key);
    }
}
