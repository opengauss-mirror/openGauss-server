/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pg_partition_fn.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_partition_fn.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_PARTITION_FN_H
#define PG_PARTITION_FN_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include "access/htup.h"
#include "storage/lock/lock.h"
#include "access/heapam.h"
#include "storage/lmgr.h"

#define MAX_PARTITIONKEY_NUM 4
#define MAX_PARTITION_NUM 1048575    /* update LEN_PARTITION_PREFIX as well ! */
#define INTERVAL_PARTITION_NAME_PREFIX "sys_p"
#define INTERVAL_PARTITION_NAME_PREFIX_FMT "sys_p%u"
#define INTERVAL_PARTITION_NAME_SUFFIX_LEN 5  /* max length of partitio num */

/* 
 * In start/end syntax, partition name is of form: 
 *     PARTITION_PREFIX_NUM, 
 * so length of the name prefix must be restricted to:
 *     NAMEDATALEN (64) - 1  -  LENGTH ( "MAX_PARTITION_NUM" )  -  1
 * NOTICE: please update LEN_PARTITION_PREFIX if you modify macro MAX_PARTITION_NUM 
 */
#define LEN_PARTITION_PREFIX  55

/*
 * A suppositional sequence number for partition.
 */
#define ADD_PARTITION_ACTION (MAX_PARTITION_NUM + 1)

/*
 * We add ADD_PARTITION_ACTION sequence lock to prevent parallel complaints.
 */
extern void LockRelationForAddIntervalPartition(Relation rel);
extern void LockRelationForAccessIntervalPartitionTab(Relation rel);
extern void UnlockRelationForAccessIntervalPartTabIfHeld(Relation rel);
extern void UnlockRelationForAddIntervalPartition(Relation rel);

typedef void (*PartitionNameGetPartidCallback) (Oid partitioned_relation, const char *partition_name, Oid partId,
    Oid oldPartId, char partition_type, void *callback_arg, LOCKMODE callbackobj_lockMode);
extern void insertPartitionEntry(Relation pg_partition_desc, Partition new_part_desc, Oid new_part_id, 
                                 int2vector *pkey, const oidvector *inttablespace, Datum interval,
                                 Datum maxValues, Datum transitionPoint, Datum reloptions, char parttype);
extern bool isPartitionedObject(Oid relid, char relkind, bool missing_ok);
extern bool isPartitionObject(Oid partid, char partkind, bool missing_ok);
extern Oid getPartitionIndexOid(Oid indexid, Oid partitionid);
extern Oid getPartitionIndexTblspcOid(Oid indexid, Oid partitionid);
extern char* getPartitionIndexName(Oid indexid, Oid partitionid);
extern Oid indexPartGetHeapPart(Oid indexPart, bool missing_ok);
extern Oid searchPartitionIndexOid(Oid partitionedIndexid, List *pindex);
extern List *getPartitionObjectIdList(Oid relid, char relkind);
extern Oid partitionNameGetPartitionOid (Oid partitionedTableOid, 
                                         const char *partitionName,
                                         char objectType,
                                         LOCKMODE lockMode,
                                         bool missingOk, 
                                         bool nowWait,
                                         PartitionNameGetPartidCallback callback, 
                                         void *callback_arg,
                                         LOCKMODE callbackobj_lockMode,
                                         Oid *partOidForSubPart = NULL);
extern Oid partitionValuesGetPartitionOid(Relation rel, List *partKeyValueList, LOCKMODE lockMode, bool topClosed,
                                          bool missingOk, bool noWait);
extern Oid subpartitionValuesGetSubpartitionOid(Relation rel, List *partKeyValueList, List *subpartKeyValueList,
    LOCKMODE lockMode, bool topClosed, bool missingOk, bool noWait, Oid *partOidForSubPart);
extern List *searchPartitionIndexesByblid(Oid blid);
extern List *searchPgPartitionByParentId(char parttype, Oid parentId);
extern List* searchPgSubPartitionByParentId(char parttype, List *parentOids);
extern void freePartList(List *l);
extern void freeSubPartList(List* plist);
extern HeapTuple searchPgPartitionByParentIdCopy(char parttype, Oid parentId);
extern Oid GetBaseRelOidOfParition(Relation relation);

extern List* relationGetPartitionOidList(Relation rel);
extern List* RelationGetSubPartitionOidList(Relation rel, LOCKMODE lockmode = AccessShareLock);
extern List* RelationGetSubPartitionOidListList(Relation rel);
extern List* relationGetPartitionList(Relation relation, LOCKMODE lockmode);
extern List* RelationGetPartitionNameList(Relation relation);
extern void RelationGetSubpartitionInfo(Relation relation, char *subparttype, List **subpartKeyPosList,
    int2vector **subpartitionKey);
extern List* indexGetPartitionOidList(Relation indexRelation);
extern List* indexGetPartitionList(Relation indexRelation, LOCKMODE lockmode);
extern Relation SubPartitionGetRelation(Relation heap, Partition indexpart, LOCKMODE lockmode);
extern Partition SubPartitionOidGetPartition(Relation rel, Oid subPartOid, LOCKMODE lockmode);
extern Relation SubPartitionOidGetParentRelation(Relation rel, Oid subPartOid, LOCKMODE lockmode);
extern List* RelationGetSubPartitionList(Relation relation, LOCKMODE lockmode);
extern void  releasePartitionList(Relation relation, List** partList, LOCKMODE lockmode, bool validCheck = true);
extern void  releaseSubPartitionList(Relation relation, List** partList, LOCKMODE lockmode);
extern void releasePartitionOidList(List** partList);
extern void ReleaseSubPartitionOidList(List** partList);

#endif

