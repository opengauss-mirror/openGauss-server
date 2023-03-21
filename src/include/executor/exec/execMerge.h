/* ---------------------------------------------------------------------------------------
 * 
 * execMerge.h
 * 
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/execMerge.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef EXECMERGE_H
#define EXECMERGE_H

#include "nodes/execnodes.h"

/* flags for mt_merge_subcommands */
#define MERGE_INSERT 0x01
#define MERGE_UPDATE 0x02

struct VecModifyTableState;

extern void ExecMerge(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ResultRelInfo* resultRelInfo, char* partExprKeyStr = NULL);

extern void ExecInitMerge(ModifyTableState* mtstate, EState* estate, ResultRelInfo* resultRelInfo);

extern TupleTableSlot* ExtractScanTuple(ModifyTableState* mtstate, TupleTableSlot* slot, TupleDesc tupDesc);

extern TupleTableSlot* ExecMergeProjQual(ModifyTableState* mtstate, List* mergeMatchedActionStates,
    ExprContext* econtext, TupleTableSlot* originSlot, TupleTableSlot* result_slot, EState* estate);

#endif /* NODEMERGE_H */
