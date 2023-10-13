/* -------------------------------------------------------------------------
 *
 * nodeAssertOp.h
 *
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAssertOp.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NODEASSERTOP_H
#define NODEASSERTOP_H

#ifdef USE_SPQ
#include "nodes/execnodes.h"

extern void ExecAssertOpExplainEnd(PlanState *planstate, struct StringInfoData *buf);
extern TupleTableSlot* ExecAssertOp(PlanState *node);
extern AssertOpState* ExecInitAssertOp(AssertOp *node, EState *estate, int eflags);
extern void ExecEndAssertOp(AssertOpState *node);
extern void ExecReScanAssertOp(AssertOpState *node);
#endif /* USE_SPQ */

#endif   /* NODEASSERTOP_H */
