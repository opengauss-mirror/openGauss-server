/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * partcache.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/partcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARTCACHE_H
#define PARTCACHE_H
#include "access/tupdesc.h"
#include "nodes/bitmapset.h"
#include "utils/relcache.h"

#define PARTITIONTYPE(partition) (partition->pd_part->parttype)

#define PartitionIsPartitionedTable(partition) (PART_OBJ_TYPE_PARTED_TABLE == (partition)->pd_part->parttype)

#define PartitionIsToastTable(partition) (PART_OBJ_TYPE_TOAST_TABLE == (partition)->pd_part->parttype)

#define PartitionIsTablePartition(partition) (PART_OBJ_TYPE_TABLE_PARTITION == (partition)->pd_part->parttype)

#define PartitionIsBucket(partition) ((partition)->pd_node.bucketNode > InvalidBktId)

#define PartitionIsIndexPartition(partition) (PART_OBJ_TYPE_INDEX_PARTITION == (partition)->pd_part->parttype)

/*
 * Routines to open (lookup) and close a partcache entry
 */
extern Partition PartitionIdGetPartition(Oid partitionId, bool isbucket);
extern void PartitionClose(Partition partition);
extern char* PartitionOidGetName(Oid partOid);
extern Oid PartitionOidGetTablespace(Oid partOid);
/*
 * Routines for flushing/rebuilding relcache entries in various scenarios
 */
extern void PartitionForgetPartition(Oid partid);
/*
 * Routines for backend startup
 */
extern void PartitionCacheInitialize(void);
extern void PartitionCacheInitializePhase2(void);
extern void PartitionCacheInitializePhase3(void);
/*
 * Routine to create a partcache entry for an about-to-be-created relation
 */
Partition PartitionBuildLocalPartition(const char* relname, Oid partid, Oid partfilenode, Oid parttablespace);
/*
 * Routines for backend startup
 */
extern void PartitionCacheInitialize(void);

/*
 * Routines for flushing/rebuilding relcache entries in various scenarios
 */
extern void PartitionCacheInvalidateEntry(Oid partitionId);
extern void PartitionCacheInvalidate(void);
extern void PartitionCloseSmgrByOid(Oid partitionId);
extern void AtEOXact_PartitionCache(bool isCommit);
extern void AtEOSubXact_PartitionCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);
extern Relation partitionGetRelation(Relation rel, Partition part);

void releaseDummyRelation(Relation* relation);

extern void PartitionSetNewRelfilenode(Relation parent, Partition part, TransactionId freezeXid);

/*
 * Routines for global partition index open partition
 */
extern PartStatus PartitionGetMetadataStatus(Oid partOid, bool vacuumFlag);
extern Datum SetWaitCleanGpiRelOptions(Datum oldOptions, bool enable);
extern void PartitionedSetWaitCleanGpi(const char* parentName, Oid parentPartOid, bool enable, bool inplace);
extern void PartitionSetWaitCleanGpi(Oid partOid, bool enable, bool inplace);
extern bool PartitionInvisibleMetadataKeep(Datum datumRelOptions);
extern void PartitionedSetEnabledClean(Oid parentOid);
extern void PartitionSetEnabledClean(
    Oid parentOid, const Bitmapset* cleanedParts, const Bitmapset* invisibleParts, bool updatePartitioned);
extern void PartitionSetAllEnabledClean(Oid parentOid);
extern void PartitionGetAllInvisibleParts(Oid parentOid, Bitmapset** invisibleParts);
extern bool PartitionMetadataDisabledClean(Relation pgPartition);

#endif /* RELCACHE_H */

