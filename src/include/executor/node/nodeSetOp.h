/* -------------------------------------------------------------------------
 *
 * nodeSetOp.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSetOp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODESETOP_H
#define NODESETOP_H

#include "nodes/execnodes.h"
#include "vecexecutor/vechashtable.h"
#include "executor/node/nodeAgg.h"

// hash strategy
#define MEMORY_HASHSETOP 0
#define DIST_HASHSETOP 1

#define HASHSETOP_PREPARE 0
#define HASHSETOP_FETCH 1

typedef struct SetopWriteFileControl : public AggWriteFileControl {
} SetopWriteFileControl;

extern SetOpState* ExecInitSetOp(SetOp* node, EState* estate, int eflags);
extern TupleTableSlot* ExecSetOp(SetOpState* node);
extern void ExecEndSetOp(SetOpState* node);
extern void ExecReScanSetOp(SetOpState* node);
extern void ExecEarlyFreeHashedSetop(SetOpState* node);
extern void ExecReSetSetOp(SetOpState* node);

#endif /* NODESETOP_H */
