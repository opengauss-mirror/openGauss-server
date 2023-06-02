/* -------------------------------------------------------------------------
 *
 * nodeSortGroup.h
 *	  support for the openGauss executor module
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, openGauss Contributors
 *
 * src/include/executor/nodeSortGroup.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NODESORTGROUP_H
#define NODESORTGROUP_H


#include "nodes/execnodes.h"

extern SortGroupState *ExecInitSortGroup(SortGroup *node, EState *estate, int eflags);
extern void ExecEndSortGroup(SortGroupState *node);
extern void ExecReScanSortGroup(SortGroupState *node);

#endif /*NODESORTGROUP_H*/