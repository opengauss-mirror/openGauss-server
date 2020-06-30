/* ---------------------------------------------------------------------------------------
 * 
 * nodeStub.h
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/executor/nodeStub.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef NODESTUB_H
#define NODESTUB_H

#include "nodes/execnodes.h"

/* nodegroup */
extern PlanState *ExecInitNodeStubNorm(Plan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecProcNodeStub(PlanState *node);
extern void ExecEndNodeStub(PlanState *node);

#endif /* NODESTUB_H */
