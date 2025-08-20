/*-------------------------------------------------------------------------
 *
 * nodeMemoize.h
 *
 *
 *
 * Portions Copyright (c) 2021-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMemoize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMEMOIZE_H
#define NODEMEMOIZE_H

#include "nodes/execnodes.h"
#include "lib/ilist.h"

extern uint32 datum_image_hash(Datum value, bool typByVal, int typLen);

typedef struct ParallelContext {
} ParallelContext;
struct ParallelWorkerContext;
extern MemoizeState *ExecInitMemoize(Memoize *node, EState *estate, int eflags);
extern void ExecEndMemoize(MemoizeState *node);
extern void ExecReScanMemoize(MemoizeState *node);
extern double ExecEstimateCacheEntryOverheadBytes(double ntuples);
extern void ExecMemoizeEstimate(MemoizeState *node);
extern void ExecMemoizeRetrieveInstrumentation(MemoizeState *node);

#endif                            /* NODEMEMOIZE_H */
