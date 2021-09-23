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

#include "nodes/execnodes.h"

extern AppendState* ExecInitAppend(Append* node, EState* estate, int eflags);
extern TupleTableSlot* ExecAppend(AppendState* node);
extern void ExecEndAppend(AppendState* node);
extern void ExecReScanAppend(AppendState* node);
extern bool exec_append_initialize_next(AppendState* appendstate);

#endif /* NODEAPPEND_H */
