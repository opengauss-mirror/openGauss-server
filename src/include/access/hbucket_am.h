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
#include "access/tableam.h"

/*
* redis need merge item
*/
typedef struct RedisMergeItem {
    int2 bktid;
    uint64 start;
    uint64 end;
}RedisMergeItem;

typedef struct RedisMergeItemOrderArray {
    RedisMergeItem *itemarray;
    int length;
}RedisMergeItemOrderArray;

extern RedisMergeItem *hbkt_get_one_merge_item(char *merge_list, int merge_list_length, int2 bucketid);
#ifdef ENABLE_MULTIPLE_NODES
extern TableScanDesc GetTableScanDesc(TableScanDesc scan, Relation rel);
extern IndexScanDesc GetIndexScanDesc(IndexScanDesc scan);
#else
#define GetTableScanDesc(scan, rel)   (scan)
#define GetIndexScanDesc(scan)        (scan)
#endif
extern oidvector* hbkt_load_buckets(Relation relation, BucketInfo* bkt_info);

extern RedisMergeItemOrderArray *hbkt_get_all_merge_item(char* merge_list, int merge_list_length);

void hbkt_set_merge_list_to_pgxc_class_option(Oid pcrelid, RedisMergeItemOrderArray* merge_items, bool first_set);

extern RedisMergeItem *search_redis_merge_item(const RedisMergeItemOrderArray *merge_items, const int2 bucketid);
extern void freeRedisMergeItemOrderArray(RedisMergeItemOrderArray *merge_items);

extern bool hbkt_sampling_scan_nextbucket(TableScanDesc hpScan);
extern bool hbkt_bitmapheap_scan_nextbucket(HBktTblScanDesc hpScan);
extern bool hbkt_tbl_tid_nextbucket(HBktTblScanDesc hpScan);
extern bool cbi_bitmapheap_scan_nextbucket(HBktTblScanDesc hpscan, GPIScanDesc gpiscan, CBIScanDesc cbiscan);
extern Relation cbi_bitmapheap_scan_getbucket(const HBktTblScanDesc hpscan, const GPIScanDesc gpiscan,
    const CBIScanDesc cbiscan, int2 bucketid);

extern TableScanDesc scan_handler_tbl_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
        ScanState* sstate = NULL, bool isRangeScanInRedis = false);
extern TableScanDesc scan_handler_tbl_begin_tidscan(Relation relation, ScanState* state);
extern void scan_handler_tbl_end_tidscan(TableScanDesc scan);
extern void scan_handler_tbl_markpos(TableScanDesc scan);
extern void scan_handler_tbl_restrpos(TableScanDesc scan);
extern void scan_handler_tbl_init_parallel_seqscan(TableScanDesc scan, int32 dop, ScanDirection dir);
extern TableScanDesc scan_handler_tbl_beginscan_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, ScanState* sstate);
extern TableScanDesc scan_handler_tbl_beginscan_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync, ScanState* sstate);
extern Tuple scan_handler_tbl_getnext(TableScanDesc scan, ScanDirection direction, Relation rel);
extern void scan_handler_tbl_endscan(TableScanDesc scan);
extern void scan_handler_tbl_rescan(TableScanDesc scan, struct ScanKeyData* key, Relation rel,
    bool is_bitmap_rescan = false);
#define NOT_EXIST_MERGE_LIST "not_exist_merge_list"

#endif /* HASHPART_AM_H */
