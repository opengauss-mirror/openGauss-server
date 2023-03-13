/* -------------------------------------------------------------------------
 *
 * nodeCtescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/executor/nodeCtescan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODECTESCAN_H
#define NODECTESCAN_H

#include "nodes/execnodes.h"

extern CteScanState* ExecInitCteScan(CteScan* node, EState* estate, int eflags);
extern void ExecEndCteScan(CteScanState* node);
extern void ExecReScanCteScan(CteScanState* node);

/*
 * stuffs for START WITH ... CONNECT BY support
 */
#define SYS_CONNECT_BY_PATH_FUNCOID    9350
#define CONNECT_BY_ROOT_FUNCOID        9351

#define IsHierarchicalQueryFuncOid(funcOid) \
    ((funcOid == SYS_CONNECT_BY_PATH_FUNCOID || funcOid == CONNECT_BY_ROOT_FUNCOID))

typedef struct StartWithFuncEvalState
{
    ExprContext *sw_econtext;
    ExprState   *sw_exprstate;
} StartWithFuncEvalState;

extern StartWithOpState* ExecInitStartWithOp(StartWithOp* node, EState* estate, int eflags);
extern void ExecEndStartWithOp(StartWithOpState *node);
extern void ExecReScanStartWithOp(StartWithOpState *node);

extern TupleTableSlot *ConvertRuScanOutputSlot(RecursiveUnionState *rustate,
                                               TupleTableSlot *scanSlot,
                                               bool inrecursing);
extern TupleTableSlot *ConvertStartWithOpOutputSlot(StartWithOpState *node,
                                                   TupleTableSlot *srcSlot,
                                                   TupleTableSlot *dstSlot);
extern TupleTableSlot *ConvertRuScanOutputSlot(RecursiveUnionState *rustate,
                                               TupleTableSlot *scanSlot,
                                               bool inrecursing);
extern bool CheckCycleExeception(StartWithOpState *node, TupleTableSlot *slot);
extern int SibglingsKeyCmp(Datum x, Datum y, SortSupport ssup);
extern int SibglingsKeyCmpFast(Datum x, Datum y, SortSupport ssup);

extern void markSWLevelBegin(StartWithOpState *node);
extern void markSWLevelEnd(StartWithOpState *node, int64 rowCount);
extern TupleTableSlot* GetStartWithSlot(RecursiveUnionState* node, TupleTableSlot* slot, bool isRecursive);
extern bool ExecStartWithRowLevelQual(RecursiveUnionState* node, TupleTableSlot* dstSlot);
#endif /* NODECTESCAN_H */
