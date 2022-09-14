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
 * hbindex_am.h
 *	  hash bucket index access method definitions.

 *
 *
 * IDENTIFICATION
 *        src/include/access/hbindex_am.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef HBINDEX_AM_H
#define HBINDEX_AM_H

#include "access/relscan.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "optimizer/bucketinfo.h"
#include "utils/rel_gs.h"


static inline bool hbkt_idx_need_switch_bkt(IndexScanDesc scan, int targetSlot)
{
    if (RELATION_OWN_BUCKET(scan->indexRelation)) {
        HBktIdxScanDesc hbScan = (HBktIdxScanDesc)scan;
        return targetSlot < hbScan->hBktList->dim1 && targetSlot != hbScan->curr_slot;
    } else {
        return false;
    }
}

extern bool hbkt_idx_bitmapscan_switch_bucket(IndexScanDesc scan, int targetSlot);
extern bool cbi_scan_need_fix_hbkt_rel(IndexScanDesc scan, int2 bucketid = InvalidBktId);
extern bool cbi_scan_fix_hbkt_rel(HBktIdxScanDesc hpScan);
extern IndexScanDesc scan_handler_idx_beginscan(Relation heap_relation, Relation index_relation, Snapshot snapshot, 
    int nkeys, int norderbys, ScanState* scan_state = NULL);
extern IndexScanDesc scan_handler_idx_beginscan_bitmap(Relation indexRelation, Snapshot snapshot, int nkeys, ScanState* scan_state);
extern void scan_handler_idx_rescan(IndexScanDesc scan, ScanKey key, int nkeys, ScanKey orderbys, int norderbys);
extern void scan_handler_idx_rescan_local(IndexScanDesc scan, ScanKey key, int nkeys, ScanKey orderbys, int norderbys);
extern void scan_handler_idx_endscan(IndexScanDesc scan);
extern void scan_handler_idx_markpos(IndexScanDesc scan);
extern void scan_handler_idx_restrpos(IndexScanDesc scan);
extern HeapTuple scan_handler_idx_fetch_heap(IndexScanDesc scan);
extern HeapTuple scan_handler_idx_getnext(IndexScanDesc scan, ScanDirection direction, Oid expect_partoid = InvalidOid,
    int2 expect_bktid = InvalidBktId, bool* has_cur_xact_write = NULL);
extern ItemPointer scan_handler_idx_getnext_tid(IndexScanDesc scan, ScanDirection direction, bool *bktchg = NULL);
extern int64 scan_handler_idx_getbitmap(IndexScanDesc scan, TIDBitmap* bitmap);

#endif /* HBINDEX_AM_H */
