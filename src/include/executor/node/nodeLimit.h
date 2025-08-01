/* -------------------------------------------------------------------------
 *
 * nodeLimit.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeLimit.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODELIMIT_H
#define NODELIMIT_H

#include "nodes/execnodes.h"

extern LimitState* ExecInitLimit(Limit* node, EState* estate, int eflags);
extern void ExecEndLimit(LimitState* node);
extern void ExecReScanLimit(LimitState* node);
extern void recompute_limits(LimitState* node);
typedef void (*RecomputeLimitsHookType) (float8 val);
#endif /* NODELIMIT_H */
