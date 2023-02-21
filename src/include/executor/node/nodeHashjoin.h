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

#include "nodes/execnodes.h"
#include "storage/buf/buffile.h"
#include "optimizer/planmem_walker.h"

extern HashJoinState* ExecInitHashJoin(HashJoin* node, EState* estate, int eflags);
extern void ExecEndHashJoin(HashJoinState* node);
extern void ExecReScanHashJoin(HashJoinState* node);
extern void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile** fileptr);
extern void ExecEarlyFreeHashJoin(HashJoinState* node);
extern void ExecReSetHashJoin(HashJoinState* node);
extern bool FindParam(Node* node_plan, void* context);
extern bool CheckParamWalker(PlanState* plan_stat);

#endif /* NODEHASHJOIN_H */
