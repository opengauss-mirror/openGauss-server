/* -------------------------------------------------------------------------
 *
 * storage.h
 *	  prototypes for functions in backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef STORAGE_H
#define STORAGE_H
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "storage/lmgr.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

#define DFS_STOR_FLAG  -1

extern void RelationCreateStorage(RelFileNode rnode, char relpersistence, Oid ownerid, Oid bucketOid = InvalidOid,
    Relation rel = NULL);
extern void RelationDropStorage(Relation rel, bool isDfsTruncate = false);
extern void RelationPreserveStorage(RelFileNode rnode, bool atCommit);
extern void RelationTruncate(Relation rel, BlockNumber nblocks, TransactionId latest_removed_xid = InvalidTransactionId);
extern void PartitionTruncate(Relation parent, Partition part, BlockNumber nblocks, TransactionId latest_removed_xid = InvalidTransactionId);
extern void PartitionDropStorage(Relation rel, Partition part);
extern void BucketCreateStorage(RelFileNode rnode, Oid bucketOid, Oid ownerid);
extern void InsertStorageIntoPendingList(_in_ const RelFileNode* rnode, _in_ AttrNumber attrnum, _in_ BackendId backend,
    _in_ Oid ownerid, _in_ bool atCommit, _in_ bool isDfsTruncate = false, Relation rel = NULL);

#ifdef ENABLE_MULTIPLE_NODES
namespace Tsdb {
extern void DropPartStorage(
    Oid partition_id, RelFileNode* partition_rnode, BackendId backend, Oid ownerid, List* target_cudesc_relids);
extern void InsertPartStorageIntoPendingList(_in_ RelFileNode* partition_rnode, _in_ AttrNumber part_id,
        _in_ BackendId backend, _in_ Oid ownerid, _in_ bool atCommit);
}
#endif   /* ENABLE_MULTIPLE_NODES */

// column-storage relation api
extern void CStoreRelCreateStorage(RelFileNode* rnode, AttrNumber attrnum, char relpersistence, Oid ownerid);
extern void CStoreRelDropColumn(Relation rel, AttrNumber attrnum, Oid ownerid);
extern void DfsStoreRelCreateStorage(RelFileNode* rnode, AttrNumber attrnum, char relpersistence);

/*
 * These functions used to be in storage/smgr/smgr.c, which explains the
 * naming
 */
extern void smgrDoPendingDeletes(bool isCommit);
extern int smgrGetPendingDeletes(bool forCommit, ColFileNode **ptr, bool skipTemp, int *numTempRel);
extern ColFileNodeRel* ConvertToOldColFileNode(ColFileNode *colFileNodes, int size, bool freeNodes = true);
extern void AtSubCommit_smgr(void);
extern void AtSubAbort_smgr();
extern void PostPrepare_smgr(void);

extern void ColMainFileNodesCreate(void);
extern void ColMainFileNodesDestroy(void);
extern void ColMainFileNodesAppend(RelFileNode* bcmFileNode, BackendId backend);
extern void ColumnRelationDoDeleteFiles(RelFileNode* bcmFileNode, ForkNumber forknum, BackendId backend, Oid ownerid);
extern void RowRelationDoDeleteFiles(RelFileNode rnode, BackendId backend, Oid ownerid, Oid relOid = InvalidOid,
                                     bool isCommit = false);

extern uint64 GetSMgrRelSize(RelFileNode* relfilenode, BackendId backend, ForkNumber forkNum);

/*
 * dfs storage api
 */
extern bool IsSmgrTruncate(const XLogReaderState *record);
extern bool IsSmgrCreate(const XLogReaderState* record);

extern void smgrApplyXLogTruncateRelation(XLogReaderState *record);
extern void XLogBlockSmgrRedoTruncate(RelFileNode rnode, BlockNumber blkno);

#endif   /* STORAGE_H */

