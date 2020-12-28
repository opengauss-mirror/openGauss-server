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

static char *get_child_string_from_merge_list(char *merge_list, int merge_list_length, int bucketid);

List *hbkt_load_buckets(Relation relation, BucketInfo *bktInfo)
{
    ListCell *bktIdCell = NULL;
    List *bucketList = NULL;
    oidvector *blist = searchHashBucketByOid(relation->rd_bucketoid);

#ifdef ENABLE_MULTIPLE_NODES
    CheckBucketMapLenValid();
#endif

    if (bktInfo == NULL || bktInfo->buckets == NIL) {
        // load all buckets
        for (int i = 0; i < blist->dim1; i++) {
            bucketList = lappend_int(bucketList, blist->values[i]);
        }
    } else {
        // load selected buckets
        foreach (bktIdCell, bktInfo->buckets) {
            int bktId = lfirst_int(bktIdCell);
            if (bktId < 0 || bktId >= BUCKETDATALEN) {
                ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("buckets id %d of table is outsize range [%d,%d]", bktId, 0, BUCKETDATALEN - 1)));
            }
            if (lookupHBucketid(blist, 0, bktId) == -1) {
                continue;
            }
            bucketList = lappend_int(bucketList, bktId);
        }
    }
    return bucketList;
}

static void free_hbucket_scan(TableScanDesc bktScan, Relation bktRel)
{
    Assert(bktRel != NULL);
    tableam_scan_end(bktScan);
    bucketCloseRelation(bktRel);
}

static TableScanDesc hbkt_tbl_create_scan(Relation relation, ScanState *state)
{
    HBktTblScanDesc hpScan = NULL;
    List *bucketlist = NULL;
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

    /* Step 3: open first bucketed relation */
    hpScan->curr_slot = 0;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    hpScan->currBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);
    return (TableScanDesc)hpScan;
}

static TableScanDesc hbkt_tbl_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
                                   ScanState *state /* = NULL */)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)hbkt_tbl_create_scan(relation, state);

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck2(CurrentMemoryContext);
#endif

    if (hpScan == NULL) {
        return NULL;
    }

    /* Step 4: open a bucketed AbsTblScanDesc */
    RangeScanInRedis rangeScanInRedis = reset_scan_qual(hpScan->currBktRel, state);
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

static void hbkt_tbl_rescan(TableScanDesc scan, ScanKey key)
{
    HBktTblScanDesc hpScan = (HBktTblScanDesc)scan;

    Snapshot snapshot = hpScan->currBktScan->rs_snapshot;
    int nkeys = hpScan->currBktScan->rs_nkeys;

    free_hbucket_scan(hpScan->currBktScan, hpScan->currBktRel);

    hpScan->curr_slot = 0;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    hpScan->currBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    RangeScanInRedis rangeScanInRedis = reset_scan_qual(hpScan->currBktRel, hpScan->scanState);
    hpScan->currBktScan = tableam_scan_begin(hpScan->currBktRel, snapshot, nkeys, key, rangeScanInRedis);
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
        if (hpScan->curr_slot + 1 >= list_length(hpScan->hBktList)) {
            return NULL;
        }

        /* Step 2. Get the next bucket and its relation */
        hpScan->curr_slot++;
        int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
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
    if (hpScan->curr_slot + 1 >= list_length(hpScan->hBktList)) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hpScan->curr_slot++;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
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

    if (hpScan->curr_slot + 1 >= list_length(hpScan->hBktList)) {
        return false;
    }

    /* switch to next hash bucket */
    hpScan->curr_slot++;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    nextBktScan = tableam_scan_begin_bm(nextBktRel, currBktScan->rs_snapshot, currBktScan->rs_nkeys, currBktScan->rs_key);
    free_hbucket_scan(currBktScan, hpScan->currBktRel);
    hpScan->currBktRel = nextBktRel;
    hpScan->currBktScan = nextBktScan;

    return true;
}


/*
 * Switch the HbktScan to next bucket for Tid Scan
 * @return if no bucket to switch return false, otherwise return true.
 */
bool hbkt_tbl_tid_nextbucket(HBktTblScanDesc hpScan)
{
    Relation nextBktRel = NULL;

    /* Step 1. To check whether all buckets have been scanned. */
    if (hpScan->curr_slot + 1 >= list_length(hpScan->hBktList)) {
        return false;
    }

    /* Step 2. Get the next bucket and its relation */
    hpScan->curr_slot++;
    int2 bucketid = list_nth_int(hpScan->hBktList, hpScan->curr_slot);
    nextBktRel = bucketGetRelation(hpScan->rs_rd, NULL, bucketid);

    /* Step 3. If the next bucket has tuples, then we switch the old scan
     * to next */
    bucketCloseRelation(hpScan->currBktRel);

    hpScan->currBktRel = nextBktRel;

    return true;
}

/**
 * fill the item by given str, str structure is "bucketid:startctid:endctid"
 * if bucketid is InvalidBktId,decode cell directly, else compare single_item.bucketid and input bucketid
 */
static bool hbkt_decode_merge_item_from_str(char *single_item, int single_item_length, RedisMergeItem *item,
                                            int2 bucketid)
{
#define MAXINT64LENGTH 20
    List *datalist = NIL;
    char tmp[MAXINT64LENGTH] = {0};
    int idx = 0;
    int j = 0;
    for (int i = 0; i < single_item_length; i++) {
        if (single_item[i] == ':') {
            tmp[j] = 0;
            if (idx == 0) {
                item->bktid = int2(atoi(tmp));
            }
            if (idx == 1) {
                item->start = strtoull(tmp, NULL, 10);
            }
            j = 0;
            idx++;
            continue;
        }
        if (i == (single_item_length - 1)) {
            tmp[j++] = single_item[i];
            tmp[j] = 0;
            if (idx == 2) {
                item->end = strtoull(tmp, NULL, 10);
            }
            j = 0;
            idx++;
        }

        tmp[j] = single_item[i];
        j++;
        /* max length of int64 */
        if (j >= MAXINT64LENGTH) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("error format single item string %s", single_item)));
        }
    }
    if (idx != 3) {
        list_free_ext(datalist);
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("error format single item string %s", single_item)));
    }
    return true;
}

RedisMergeItemOrderArray *hbkt_get_merge_list_from_str(char *merge_list, int merge_list_length)
{
    RedisMergeItemOrderArray *result = NULL;
    int start_pos = 0;
    char *start_pointer = merge_list;
    int idx = 0;
    bool error_decode = false;

    int i = 0;
    int length = 0;
    if (merge_list == NULL || merge_list_length == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("empty merge_list string")));
        return NULL;
    }
    for (i = 0; i < merge_list_length; i++) {
        if (merge_list[i] == ';' || i == (merge_list_length - 1)) {
            length++;
        }
    }
    result = (RedisMergeItemOrderArray *)palloc0(sizeof(RedisMergeItemOrderArray));
    result->length = length;
    result->itemarray = (RedisMergeItem *)palloc(result->length * sizeof(RedisMergeItem));
    for (i = 0; i < merge_list_length; i++) {
        if (merge_list[i] == ';' || i == (merge_list_length - 1)) {
            int single_item_length = merge_list[i] == ';' ? (i - start_pos) : (i - start_pos + 1);
            error_decode = hbkt_decode_merge_item_from_str(start_pointer, single_item_length, &result->itemarray[idx],
                                                           InvalidBktId);
            if (error_decode == false) {
                freeRedisMergeItemOrderArray(result);
                result = NULL;
                goto ret;
            }
            idx++;
            /* add 1 to skip ';' */
            start_pointer = merge_list + i + 1;
            start_pos = i + 1;
        }
    }
ret:
    return result;
}

RedisMergeItem *hbkt_get_merge_item_from_str(char *merge_list, int merge_list_length, int2 bucketid)
{
    char *merge_list_str = NULL;
    RedisMergeItem *item = NULL;
    bool found = false;

    if (merge_list == NULL || strlen(merge_list) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("empty merge_list string")));
        return NULL;
    }
    merge_list_str = get_child_string_from_merge_list(merge_list, merge_list_length, bucketid);
    if (merge_list_str == NULL) {
        goto ret;
    }
    item = (RedisMergeItem *)palloc(sizeof(RedisMergeItem));
    found = hbkt_decode_merge_item_from_str(merge_list_str, strlen(merge_list_str), item, bucketid);
    /* return null and free memory if not match bucketid */
    if (found == false) {
        pfree_ext(item);
    }
    pfree_ext(merge_list_str);
ret:
    return item;
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
    (void)searchMergeListByRelid(pcrelid, &find, false);
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

/* Create HBktTblScanDesc with scan state. For hash-bucket table, it scans the
 * specified buckets in ScanState */
TableScanDesc scan_handler_tbl_beginscan(Relation relation, Snapshot snapshot,
    int nkeys, ScanKey key, ScanState* sstate)
{
    if (RELATION_CREATE_BUCKET(relation)) {
        return (TableScanDesc)hbkt_tbl_beginscan(relation, snapshot, nkeys, key, sstate);
    }
    
    RangeScanInRedis rangeScanInRedis = reset_scan_qual(relation, sstate);
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

Tuple scan_handler_tbl_getnext(TableScanDesc scan, ScanDirection direction, Relation rel)
{
    if (unlikely(RELATION_CREATE_BUCKET(scan->rs_rd))) {
        Tuple htup = tableam_scan_getnexttuple(((HBktTblScanDesc)scan)->currBktScan, direction);
        if (htup != NULL) {
            return htup;
        }
        return (Tuple) switch_and_scan_next_tbl_hbkt(scan, direction);
    } else {
        return tableam_scan_getnexttuple(scan, direction);
    }
}

void scan_handler_tbl_endscan(TableScanDesc scan, Relation rel)
{
    if (unlikely(RELATION_CREATE_BUCKET(scan->rs_rd))) {
        HBktTblScanDesc hp_scan = (HBktTblScanDesc)scan;
        free_hbucket_scan(hp_scan->currBktScan, hp_scan->currBktRel);
        list_free_ext(hp_scan->hBktList);
        pfree(scan);
    } else {
        tableam_scan_end(scan);
    }
}

void scan_handler_tbl_rescan(TableScanDesc scan, struct ScanKeyData* key, Relation rel)
{
    if (unlikely(RELATION_CREATE_BUCKET(scan->rs_rd))) {
        hbkt_tbl_rescan(scan, key);
    } else {
        tableam_scan_rescan(scan, key);
    }
}

/*
 * get child string from merge_list of assignation bucketid
 */
static char *get_child_string_from_merge_list(char *merge_list, int merge_list_length, int bucketid)
{
    char *result = NULL;
    errno_t errorno = EOK;
    char tmp[6] = {0};
    char *strtemp = merge_list;
    int used_length = 0;
    while (strtemp != NULL) {
        int i = 0;
        for (i = 0; i < (merge_list_length - used_length) && strtemp[i] != ':'; i++) {
            tmp[i] = strtemp[i];
        }
        tmp[i] = 0;
        if (bucketid == atoi(tmp)) {
            for (i = 0; i < (merge_list_length - used_length) && strtemp[i] != ';'; i++) {
            }
            result = (char *)palloc(i + 1);
            errorno = memcpy_s(result, i, strtemp, i);
            securec_check(errorno, "\0", "\0");
            result[i] = 0;
            break;
        }
        if (bucketid < atoi(tmp)) {
            break;
        }
        strtemp = strchr(strtemp, ';');
        if (strtemp != NULL) {
            /* skip the char ';' */
            strtemp = strtemp + 1;
            used_length = int(strtemp - merge_list);
            if (used_length == merge_list_length) {
                break;
            }
        }
    }
    return result;
}
