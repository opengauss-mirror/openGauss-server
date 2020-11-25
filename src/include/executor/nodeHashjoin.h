/* -------------------------------------------------------------------------
 *
 * nodeHashjoin.h
 *	  prototypes for nodeHashjoin.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHashjoin.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEHASHJOIN_H
#define NODEHASHJOIN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"
#include "storage/buffile.h"

extern HashJoinState* ExecInitHashJoin(HashJoin* node, EState* estate, int eflags);
extern TupleTableSlot* ExecHashJoin(HashJoinState* node);
extern void ExecEndHashJoin(HashJoinState* node);
extern void ExecReScanHashJoin(HashJoinState* node);
extern void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile** fileptr);
extern void ExecEarlyFreeHashJoin(HashJoinState* node);
extern void ExecReSetHashJoin(HashJoinState* node);

extern void ExecShutdownHashJoin(HashJoinState* node);
extern void ExecHashJoinInitializeDSM(HashJoinState* state, ParallelContext* pcxt, int nodeid);
extern void ExecHashJoinReInitializeDSM(HashJoinState* state, ParallelContext* pcxt);
extern void ExecHashJoinInitializeWorker(HashJoinState* state, void* pwcxt);

#endif /* NODEHASHJOIN_H */
