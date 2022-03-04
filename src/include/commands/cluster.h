/* -------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for openGauss cluster command stuff
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/parsenodes.h"
#include "storage/lock/lock.h"
#include "utils/relcache.h"

extern void cluster(ClusterStmt* stmt, bool isTopLevel);
extern void cluster_rel(Oid tableOid, Oid partitionOid, Oid indexOid, bool recheck, bool verbose, int freeze_min_age,
    int freeze_table_age, void* mem_info, bool onerel);
extern void check_index_is_clusterable(
    Relation OldHeap, Oid indexOid, bool recheck, LOCKMODE lockmode, Oid* amid = NULL);
extern void mark_index_clustered(Relation rel, Oid indexOid);

extern Oid make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, int lockMode = AccessExclusiveLock);

extern Oid makePartitionNewHeap(Relation partitionedTableRel, TupleDesc partTabHeapDesc, Datum partTabRelOptions,
    Oid oldPartOid, Oid partToastOid, Oid NewTableSpace, bool isCStore = false, Oid subpartFilenode = InvalidOid);
extern double copy_heap_data_internal(Relation OldHeap, Relation OldIndex, Relation NewHeap, TransactionId OldestXmin,
    TransactionId FreezeXid, bool verbose, bool use_sort, AdaptMem* memUsage);
extern double CopyUHeapDataInternal(Relation oldHeap, Relation oldIndex, Relation newHeap, TransactionId oldestXmin,
    TransactionId freezeXid, bool verbose, bool useSort, const AdaptMem* memUsage);
extern void getPartitionRelxids(Relation ordTableRel, TransactionId* frozenXid, MultiXactId* multiXid = NULL);
extern void getRelationRelxids(Relation ordTableRel, TransactionId* frozenXid, MultiXactId* multiXid = NULL);
extern void  setRelationRelfrozenxid(Oid relid, TransactionId frozenXid);
extern void  setPartitionRelfrozenxid(Oid partid, TransactionId frozenXid);
extern void finishPartitionHeapSwap(Oid partitionOid, Oid tempTableOid, bool swapToastByContent,
    TransactionId frozenXid, MultiXactId multiXid, bool tempTableIsPartition = false);

extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog, bool swap_toast_by_content,
    bool check_constraints, TransactionId frozenXid, MultiXactId frozenMulti, AdaptMem* memInfo = NULL);

extern void vacuumFullPart(Oid partOid, VacuumStmt* vacstmt, int freeze_min_age, int freeze_table_age);
extern void GpiVacuumFullMainPartiton(Oid parentOid);
extern void CBIVacuumFullMainPartiton(Oid parentOid);
extern void updateRelationName(Oid relOid, bool isPartition, const char* relNewName);
extern List* GetIndexPartitionListByOrder(Relation indexRelation, LOCKMODE lockmode);
extern bool IsNeedToTransfer(Relation rel1, Relation rel2);
extern void swapRelationIndicesRelfileNode(Relation rel1, Relation rel2, bool swapBucket, bool swapIndex);
extern int IndexGetindisusable(Oid indexOid);

#endif /* CLUSTER_H */
