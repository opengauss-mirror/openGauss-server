/* -------------------------------------------------------------------------
 *
 * nodeAppend.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAppend.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEAPPEND_H
#define NODEAPPEND_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

#define INVALID_SUBPLAN_INDEX -1

extern AppendState* ExecInitAppend(Append* node, EState* estate, int eflags);
extern TupleTableSlot* ExecAppend(AppendState* node);
extern void ExecEndAppend(AppendState* node);
extern void ExecReScanAppend(AppendState* node);
extern void ExecAppendEstimate(AppendState *node, ParallelContext *pcxt);
extern void ExecAppendInitializeDSM(AppendState *node, ParallelContext *pcxt, int nodeid);
extern void ExecAppendReInitializeDSM(AppendState *node, ParallelContext *pcxt);
extern void ExecAppendInitializeWorker(AppendState *node, void* context);
extern bool choose_next_subplan_locally(AppendState *node);
#endif /* NODEAPPEND_H */
