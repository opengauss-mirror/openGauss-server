/* -------------------------------------------------------------------------
 *
 * lmgr.h
 *	  openGauss lock manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lmgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LMGR_H
#define LMGR_H

#include "lib/stringinfo.h"
#include "storage/item/itemptr.h"
#include "storage/lock/lock.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/partcache.h"
#include "utils/partitionmap_gs.h"

extern void RelationInitLockInfo(Relation relation);

/* Lock a relation */
extern void LockRelationOid(Oid relid, LOCKMODE lockmode);
extern bool ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode);
extern void UnlockRelationId(LockRelId* relid, LOCKMODE lockmode);
extern void UnlockRelationOid(Oid relid, LOCKMODE lockmode);

extern void LockRelFileNode(const RelFileNode& rnode, LOCKMODE lockmode);
extern void UnlockRelFileNode(const RelFileNode& rnode, LOCKMODE lockmode);

extern void LockRelation(Relation relation, LOCKMODE lockmode);
extern bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode);
extern void UnlockRelation(Relation relation, LOCKMODE lockmode);
extern bool LockHasWaitersRelation(Relation relation, LOCKMODE lockmode);
extern bool LockHasWaitersPartition(Relation relation, LOCKMODE lockmode);

extern void LockRelationIdForSession(LockRelId* relid, LOCKMODE lockmode);
extern void UnlockRelationIdForSession(LockRelId* relid, LOCKMODE lockmode);

/* Lock a relation for extension */
extern void LockRelationForExtension(Relation relation, LOCKMODE lockmode);
extern void UnlockRelationForExtension(Relation relation, LOCKMODE lockmode);
extern bool ConditionalLockRelationForExtension(Relation relation, LOCKMODE lockmode);
extern int RelationExtensionLockWaiterCount(Relation relation);

extern void LockRelFileNodeForExtension(const RelFileNode& rnode, LOCKMODE lockmode);
extern void UnlockRelFileNodeForExtension(const RelFileNode& rnode, LOCKMODE lockmode);

/* Lock a page (currently only used within indexes) */
extern void LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);
extern bool ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);
extern void UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);

/* Lock a tuple (see heap_lock_tuple before assuming you understand this) */
extern void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode, bool allow_con_update = false, int waitSec = 0);
extern void LockTupleUid(Relation relation, uint64 uid, LOCKMODE lockmode, bool allow_con_update, bool lockTuple);
extern bool ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode);
extern void UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode);

/* Lock an XID (used to wait for a transaction to finish) */
extern void XactLockTableInsert(TransactionId xid);
extern void XactLockTableDelete(TransactionId xid);
extern void XactLockTableWait(TransactionId xid, bool allow_con_update = false, int waitSec = 0);
extern bool ConditionalXactLockTableWait(TransactionId xid, const Snapshot snapshot = NULL, bool waitparent = true,
                                         bool bCareNextxid = false);

/* Lock a SubXID */
extern void SubXactLockTableInsert(SubTransactionId subxid);
extern void SubXactLockTableWait(TransactionId xid, SubTransactionId subxid, bool allow_con_update = false, int waitSec = 0);
extern bool ConditionalSubXactLockTableWait(TransactionId xid, SubTransactionId subxid);

/* Lock a general object (other than a relation) of the current database */
extern void LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);
extern bool ConditionalLockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);
extern void UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);

/* Lock a shared-across-databases object (other than a relation) */
extern void LockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);
extern void UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);

extern void LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);
extern void UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode);
extern void PartitionInitLockInfo(Partition partition);

/* Describe a locktag for error messages */
extern void LockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type);
extern bool ConditionalLockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type);
extern void UnlockPartition(Oid relid, uint32 seq, LOCKMODE lockmode, int partition_lock_type);

extern void LockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode);
extern bool ConditionalLockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode);
extern void UnlockPartitionOid(Oid relid, uint32 seq, LOCKMODE lockmode);

extern void LockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode);
extern bool ConditionalLockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode);
extern void UnlockPartitionSeq(Oid relid, uint32 seq, LOCKMODE lockmode);
extern void UnlockPartitionSeqIfHeld(Oid relid, uint32 seq, LOCKMODE lockmode);

extern bool ConditionalLockPartitionWithRetry(Relation relation, Oid partitionId, LOCKMODE lockmode);

/* for vacuum partition */
void LockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode);
bool ConditionalLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode);
void UnLockPartitionVacuum(Relation prel, Oid partId, LOCKMODE lockmode);
void LockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode);
void UnLockPartitionVacuumForSession(PartitionIdentifier* partIdtf, Oid partrelid, Oid partid, LOCKMODE lockmode);

extern void DescribeLockTag(StringInfo buf, const LOCKTAG* tag);

extern bool ConditionalLockCStoreFreeSpace(Relation relation);
extern void UnlockCStoreFreeSpace(Relation relation);
extern const char* GetLockNameFromTagType(uint16 locktag_type);

extern void LockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode);
extern void UnlockProcedureIdForSession(Oid procId, Oid dbId, LOCKMODE lockmode);
extern void LockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode);
extern void UnlockPackageIdForSession(Oid packageId, Oid dbId, LOCKMODE lockmode);

extern void LockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode);
extern void UnlockProcedureIdForXact(Oid procId, Oid dbId, LOCKMODE lockmode);
extern void LockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode);
extern void UnlockPackageIdForXact(Oid packageId, Oid dbId, LOCKMODE lockmode);

#endif /* LMGR_H */
