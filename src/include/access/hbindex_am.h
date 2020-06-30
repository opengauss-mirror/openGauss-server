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

HBktIdxScanDesc hbkt_idx_beginscan(Relation heapRelation,
                    Relation indexRelation,
                    Snapshot snapshot,
                    int nkeys, int norderbys,
                    ScanState *scanState);

void hbkt_idx_rescan_local(AbsIdxScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);

void hbkt_idx_rescan_bitmap(AbsIdxScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
HBktIdxScanDesc hbkt_idx_beginscan_bitmap(Relation indexRelation,
                    Snapshot snapshot,
                    int nkeys,
                    ScanState* scanState);

bool hbkt_idx_bitmapscan_switch_bucket(AbsIdxScanDesc scan, int targetSlot);

static inline bool hbkt_idx_need_switch_bkt(AbsIdxScanDesc scan, int targetSlot)
{
    HBktIdxScanDesc hbScan = (HBktIdxScanDesc)scan;

    return scan->type == T_ScanDesc_HBucketIndex 
        && targetSlot < list_length(hbScan->hBktList)
        && targetSlot != hbScan->curr_slot;
}

#endif /* HBINDEX_AM_H */
