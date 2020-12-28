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

#define MAX_PARTITIONKEY_NUM 4
#define MAX_PARTITION_NUM 32767    /* update LEN_PARTITION_PREFIX as well ! */
#define MAX_LH_PARTITION_NUM 64    /* update LEN_LIST/HASH_PARTITION_PREFIX as well ! */
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
#define LEN_PARTITION_PREFIX  57

/*
 * A suppositional sequence number for partition.
 */
#define ADD_PARTITION_ACTION (MAX_PARTITION_NUM + 1)

/*
 * We acquire a AccessExclusiveLock on ADD_PARTITION_ACTION before we decide
 * to add an interval partition to prevent parallel complaints.
 */
#define lockRelationForAddIntervalPartition(relation) \
    LockPartition(RelationGetRelid(relation), ADD_PARTITION_ACTION, \
    AccessExclusiveLock, PARTITION_SEQUENCE_LOCK);

#define unLockRelationForAddIntervalPartition(relation) \
    UnlockPartition(RelationGetRelid(relation), ADD_PARTITION_ACTION, \
    AccessExclusiveLock, PARTITION_SEQUENCE_LOCK);

typedef void (*PartitionNameGetPartidCallback) (Oid partitioned_relation, const char *partition_name, Oid partId,
    Oid oldPartId, char partition_type, void *callback_arg, LOCKMODE callbackobj_lockMode);
extern void insertPartitionEntry(Relation pg_partition_desc, Partition new_part_desc, Oid new_part_id, 
                                 int2vector *pkey, const oidvector *inttablespace, Datum interval,
                                 Datum maxValues, Datum transitionPoint, Datum reloptions, char parttype);
extern bool isPartitionedObject(Oid relid, char relkind, bool missing_ok);
extern bool isPartitionObject(Oid partid, char partkind, bool missing_ok);
extern Oid getPartitionIndexOid(Oid indexid, Oid partitionid);
extern Oid getPartitionIndexTblspcOid(Oid indexid, Oid partitionid);
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
                                         LOCKMODE callbackobj_lockMode);
extern Oid partitionValuesGetPartitionOid(Relation rel, List *partKeyValueList, LOCKMODE lockMode, bool topClosed,
                                          bool missingOk, bool noWait);
extern List *searchPartitionIndexesByblid(Oid blid);
extern List *searchPgPartitionByParentId(char parttype, Oid parentId);
extern void freePartList(List *l);
extern HeapTuple searchPgPartitionByParentIdCopy(char parttype, Oid parentId);

#endif

