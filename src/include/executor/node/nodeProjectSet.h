/* -------------------------------------------------------------------------
 *
 * nodeProjectSet.h
 *       support for the openGauss executor module
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/executor/nodeProjectSet.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEPROJECTSET_H
#define NODEPROJECTSET_H

#include "nodes/execnodes.h"

extern ProjectSetState *ExecInitProjectSet(ProjectSet *node, EState *estate, int eflags);
extern void ExecEndProjectSet(ProjectSetState *node);
extern void ExecReScanProjectSet(ProjectSetState *node);

#endif   /* NODEPROJECTSET_H */
