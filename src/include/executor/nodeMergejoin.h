/* -------------------------------------------------------------------------
 *
 * nodeMergejoin.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMergejoin.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEMERGEJOIN_H
#define NODEMERGEJOIN_H

#include "nodes/execnodes.h"

extern MergeJoinState* ExecInitMergeJoin(MergeJoin* node, EState* estate, int eflags);
extern TupleTableSlot* ExecMergeJoin(MergeJoinState* node);
extern void ExecEndMergeJoin(MergeJoinState* node);
extern void ExecReScanMergeJoin(MergeJoinState* node);

/*
 * States of the ExecMergeJoin state machine
 */
#define EXEC_MJ_INITIALIZE_OUTER 1
#define EXEC_MJ_INITIALIZE_INNER 2
#define EXEC_MJ_JOINTUPLES 3
#define EXEC_MJ_NEXTOUTER 4
#define EXEC_MJ_TESTOUTER 5
#define EXEC_MJ_NEXTINNER 6
#define EXEC_MJ_SKIP_TEST 7
#define EXEC_MJ_SKIPOUTER_ADVANCE 8
#define EXEC_MJ_SKIPINNER_ADVANCE 9
#define EXEC_MJ_ENDOUTER 10
#define EXEC_MJ_ENDINNER 11

extern bool check_constant_qual(List* qual, bool* is_const_false);

#endif /* NODEMERGEJOIN_H */
