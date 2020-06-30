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
 * hbucket_am.h
 *	  hash bucket table access method definitions.
 *
 *
 * IDENTIFICATION
 *        src/include/access/hbucket_am.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef HASHPART_AM_H
#define HASHPART_AM_H

#include "access/relscan.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "optimizer/bucketinfo.h"

/*
* redis need merge item
*/
typedef struct RedisMergeItem {
    int2 bktid;
    uint64 start;
    uint64 end;
}RedisMergeItem;

typedef struct RedisMergeItemOrderArray {
    RedisMergeItem *array;
    int length;
}RedisMergeItemOrderArray;


HBktTblScanDesc hbkt_tbl_beginscan(Relation relation, Snapshot snapshot,
                                   int nkeys, ScanKey key, ScanState *state = NULL);

HBktTblScanDesc hbkt_tbl_beginscan_sampling(Relation relation, Snapshot snapshot,
                                            int nkeys, ScanKey key, bool allow_strat, bool allow_sync, ScanState *scanState);

HBktTblScanDesc hbkt_tbl_beginscan_bm(Relation relation, Snapshot snapshot,
                                      int nkeys, ScanKey key, ScanState *scanState);

HBktTblScanDesc hbkt_tbl_begin_tidscan(Relation relation, ScanState *state);

void hbkt_tbl_end_tidscan(AbsTblScanDesc scan);

void hbkt_tbl_endscan(AbsTblScanDesc scan);

void hbkt_tbl_rescan(AbsTblScanDesc scan, ScanKey key);

HeapTuple hbkt_tbl_getnext(AbsTblScanDesc scan, ScanDirection direction);

bool hbkt_sampling_scan_nextbucket(HBktTblScanDesc hpScan);

bool hbkt_bitmapheap_scan_nextbucket(HBktTblScanDesc hpScan);

void hbkt_tbl_markpos(AbsTblScanDesc scan);

void hbkt_tbl_restrpos(AbsTblScanDesc scan);

void hbkt_tbl_getpage(AbsTblScanDesc scan, BlockNumber page);

void hbkt_tbl_init_parallel_seqscan(AbsTblScanDesc scan, int32 dop, ScanDirection dir);

List *hbkt_load_buckets(Relation relation, BucketInfo *bktInfo);

bool hbkt_tbl_tid_nextbucket(HBktTblScanDesc hpScan);

RedisMergeItemOrderArray *hbkt_get_merge_list_from_relopt(Relation relation);
RedisMergeItemOrderArray *hbkt_get_merge_list_from_str(const char* merge_list);

List *hbkt_set_merge_list_to_relopt(List *rel_options, RedisMergeItemOrderArray* merge_items);

extern RedisMergeItem *search_redis_merge_item(const RedisMergeItemOrderArray *merge_items, const int2 bucketid);
void freeRedisMergeItemOrderArray(RedisMergeItemOrderArray *merge_items);

#endif /* HASHPART_AM_H */
