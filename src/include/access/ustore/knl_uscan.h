/* -------------------------------------------------------------------------
 *
 * knl_uscan.h
 * scan logic for inplace engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uscan.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_USCAN_H
#define KNL_USCAN_H

#include "access/relscan.h"
#include "access/skey.h"
#include "access/sdir.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_upage.h"
#include "executor/tuptable.h"

typedef struct UHeapScanDescData {
    TableScanDescData rs_base;
    bool rs_bitmapscan;  /* true if this is really a bitmap scan */
    bool rs_samplescan;  /* true if this is really a sample scan */
    bool rs_allow_strat; /* allow or disallow use of access strategy */
    bool rs_allow_sync;  /* allow or disallow use of syncscan */

    TupleDesc rs_tupdesc; /* heap tuple descriptor for rs_ctup */

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    // XXXTAM
    ItemPointerData rs_mctid; /* marked scan position, if any */

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    int rs_mindex;                                   /* marked tuple's saved index */

    UHeapTuple rs_visutuples[MaxPossibleUHeapTuplesPerPage]; /* visible tuples */
    UHeapTuple rs_cutup;                             /* current tuple in scan, if any */
    UHeapTuple* rs_ctupBatch;	/* current tuples in scan */
    ParallelHeapScanDesc rs_parallel; /* parallel scan information */
} UHeapScanDescData;

typedef struct UHeapScanDescData *UHeapScanDesc;

const int UHEAP_SCAN_FALLBACK = -1;
const uint32 BULKSCAN_BLOCKS_PER_BUFFER = 4;

UHeapTuple UHeapGetTupleFromPage(UHeapScanDesc scan, ScanDirection dir, bool* has_cur_xact_write = NULL);
UHeapTuple UHeapGetNextForVerify(TableScanDesc sscan, ScanDirection direction, bool& isValidRelationPage);
TableScanDesc UHeapBeginScan(Relation relation, Snapshot snapshot, int nkeys, ParallelHeapScanDesc parallel_scan);

void UHeapMarkPos(TableScanDesc uscan);
void UHeapRestRpos(TableScanDesc sscan);
void UHeapEndScan(TableScanDesc uscan);
void UHeapRescan(TableScanDesc uscan, ScanKey key);
UHeapTuple UHeapGetNextSlotGuts(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot);
UHeapTuple UHeapIndexBuildGetNextTuple(UHeapScanDesc scan, TupleTableSlot *slot);
UHeapTuple UHeapSearchBuffer(ItemPointer tid, Relation relation, Buffer buffer,
                             Snapshot snapshot, bool *all_dead, UHeapTuple freebuf = NULL,
                             bool* has_cur_xact_write = NULL);
bool UHeapScanBitmapNextTuple(TableScanDesc sscan, TBMIterateResult *tbmres, TupleTableSlot *slot);
bool UHeapScanBitmapNextBlock(TableScanDesc sscan, const TBMIterateResult *tbmres,
                                     bool* has_cur_xact_write = NULL);
bool UHeapGetPage(TableScanDesc sscan, BlockNumber page, bool* has_cur_xact_write = NULL);

UHeapTuple UHeapGetNext(TableScanDesc sscan, ScanDirection dir, bool* has_cur_xact_write = NULL);
extern bool UHeapGetTupPageBatchmode(UHeapScanDesc scan, ScanDirection dir);
#endif
