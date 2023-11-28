/* -------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeBitmapHeapscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEBITMAPHEAPSCAN_H
#define NODEBITMAPHEAPSCAN_H

#include "nodes/execnodes.h"

extern BitmapHeapScanState* ExecInitBitmapHeapScan(BitmapHeapScan* node, EState* estate, int eflags);
extern void ExecEndBitmapHeapScan(BitmapHeapScanState* node);
extern void ExecReScanBitmapHeapScan(BitmapHeapScanState* node);
extern void BitmapHeapPrefetchNext(BitmapHeapScanState* node, TableScanDesc scan, const TIDBitmap* tbm,
                                   TBMIterator** prefetch_iterator);
extern void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate);
extern TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node);
extern bool TableScanBitmapNextTuple(TableScanDesc scan, TBMIterateResult *tbmres, TupleTableSlot *slot);
extern bool TableScanBitmapNextBlock(TableScanDesc scan, TBMIterateResult *tbmres, bool* has_cur_xact_write);
extern int TableScanBitmapNextTargetRel(TableScanDesc scan, BitmapHeapScanState *node);
extern TupleTableSlot* ExecBitmapHeapScan(PlanState* state);
extern void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate);

#endif /* NODEBITMAPHEAPSCAN_H */
