/* -------------------------------------------------------------------------
 *
 * nodeModifyTable.h
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeModifyTable.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEMODIFYTABLE_H
#define NODEMODIFYTABLE_H

#include "nodes/execnodes.h"
#include "storage/lock/lock.h"

typedef TupleTableSlot* (*ExecInsertMtd)(ModifyTableState* state,TupleTableSlot*,
                        TupleTableSlot*,  EState*, bool, int, List**);
typedef TupleTableSlot* (*ExecUpdateMtd)(ItemPointer, Oid, Oid, HeapTupleHeader, TupleTableSlot*,
                        TupleTableSlot*, EPQState*, ModifyTableState*, bool, bool);

extern ModifyTableState* ExecInitModifyTable(ModifyTable* node, EState* estate, int eflags);
extern TupleTableSlot* ExecModifyTable(ModifyTableState* node);
extern void ExecEndModifyTable(ModifyTableState* node);
extern void ExecReScanModifyTable(ModifyTableState* node);
extern void RecordDeletedTuple(Oid relid, int2 bucketid, const ItemPointer tupleid, const Relation deldelta_rel);

/* May move all resizing declaration to appropriate postion sometime. */
extern bool RelationInClusterResizing(const Relation rel);
extern bool RelationInClusterResizingReadOnly(const Relation rel);
extern bool RelationInClusterResizingEndCatchup(const Relation rel);
extern bool CheckRangeVarInRedistribution(const RangeVar* range_var);
extern bool RelationIsDeleteDeltaTable(char* delete_delta_name);
extern Relation GetAndOpenDeleteDeltaRel(const Relation rel, LOCKMODE lockmode, bool isMultiCatchup);
extern Relation GetAndOpenNewTableRel(const Relation rel, LOCKMODE lockmode);
extern bool redis_func_dnstable(Oid funcid);
extern List* eval_ctid_funcs(Relation rel, List* original_quals, RangeScanInRedis *rangeScanInRedis);
extern char* nodeTagToString(NodeTag type);
extern bool ClusterResizingInProgress();
extern void RelationGetNewTableName(Relation rel, char* newtable_name);
extern bool RelationInClusterResizingWriteErrorMode(const Relation rel);

extern TupleTableSlot* ExecDelete(ItemPointer tupleid, Oid deletePartitionOid, int2 bucketid, HeapTupleHeader oldtuple,
    TupleTableSlot* planSlot, EPQState* epqstate, ModifyTableState* node, bool canSetTag);

extern TupleTableSlot* ExecUpdate(ItemPointer tupleid, Oid oldPartitionOid, int2 bucketid, HeapTupleHeader oldtuple,
    TupleTableSlot* slot, TupleTableSlot* planSlot, EPQState* epqstate, ModifyTableState* node, bool canSetTag,
    bool partKeyUpdate);

template <bool useHeapMultiInsert>
extern TupleTableSlot* ExecInsertT(ModifyTableState* state, TupleTableSlot* slot, TupleTableSlot* planSlot,
    EState* estate, bool canSetTag, int options, List** partitionList);
template <bool useHeapMultiInsert>
extern TupleTableSlot *
ExecHBucketInsertT(ModifyTableState* state, TupleTableSlot *slot,
    TupleTableSlot *planSlot,
    EState *estate,
    bool canSetTag,
    int options,
    List** partition_list);

extern void ExecCheckPlanOutput(Relation resultRel, List* targetList);

extern void ExecComputeStoredGenerated(ResultRelInfo *resultRelInfo, EState *estate, TupleTableSlot *slot,
    Tuple oldtuple, CmdType cmdtype);

#endif /* NODEMODIFYTABLE_H */
