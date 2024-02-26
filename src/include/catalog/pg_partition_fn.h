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

#define MAX_PARTITION_NUM 1048575    /* update LEN_PARTITION_PREFIX as well ! */
#define INTERVAL_PARTITION_NAME_PREFIX "sys_p"
#define INTERVAL_PARTITION_NAME_PREFIX_FMT "sys_p%u"
#define INTERVAL_PARTITION_NAME_SUFFIX_LEN 5  /* max length of partitio num */

/* 系统子分区名格式 */
#define SUBPARTITION_NAME_PREFIX "sys_subp"
#define SUBPARTITION_NAME_PREFIX_FMT "sys_subp%u"

/* 
 * In start/end syntax, partition name is of form: 
 *     PARTITION_PREFIX_NUM, 
 * so length of the name prefix must be restricted to:
 *     NAMEDATALEN (64) - 1  -  LENGTH ( "MAX_PARTITION_NUM" )  -  1
 * NOTICE: please update LEN_PARTITION_PREFIX if you modify macro MAX_PARTITION_NUM 
 */
#define LEN_PARTITION_PREFIX  55

/*-----------------------------------------------------------*/
/* Partition Concurrency Design Part1: PARTITION OBJECT LOCK */
/*
 * A suppositional sequence number for partition lock.
 * We apply PARTITION OBJECT_LOCK to protect partition parallel work.
 * In this case, the relid will be locked by LockPartitionObject(relid, PARTITION_OBJECT_LOCK_SDEQUENCE, lockmode).
 * We take these three cases in condition:
 * 1. If it's a DML operation, the OBJECT_LOCK(ShareLock) will be added in parserOpenTable, and will be released
 *    in InitPlan. Note that the snapshot is obtained before InitPlan.
 * 2. If it's a DDL operation, the OBJECT_LOCK(ExclusiveLock) will be added before CommitTransaction, which will
 *    be released during CommitTransaction.
 * 3. If it's a ADD_INTERVAL_PARTITION operation, the operation will be transformed from DML to DDL. Besides the two
 *    actions above, we apply an additinal OBJECT_LOCK, INTERVAL_PARTITION_LOCK_SDEQUENCE on all INTERVAL DDL operation
 *    (including ADD_INTERVAL_PARTITION) to protect the whole process.
 */

typedef enum PartitionObjectLock {
    PARTITION_OBJECT_LOCK_SDEQUENCE = MAX_PARTITION_NUM + 1,
    /* A suppositional sequence number for all partition operations. This lock is designed to avoid committing DDL until
     * we get the DML executor snapshot before InitPlan.
     * This is to forbid instantaneous inconsistency. Just Consider the following condition: a SQL scans the destination
     * partitioned table and uses both Local Partitioned Index and Global Partitioned Index, the LPI scan partition info
     * is generated in static pruning in optimizer, while the GPI is in executor. Without PARTITION OBJECT_LOCK, if a
     * DDL operation commits, which creates a new partition and inserts into data, such as ADD_INTERVAL_PARTITION, the
     * inconsistency may accur. Since the new partition data is invisible for static pruning of LPI scan, and is visible
     * for GPI scan. */
    INTERVAL_PARTITION_LOCK_SDEQUENCE
    /* A suppositional sequence number for DDL operations of all interval partitioned table. This is to forbid
     * concurrency of ADD_INTERVAL_PARTITION. Note that an ADD_INTERVAL_PARTITION action only holds RowExclusiveLock
     * on the table, we need an additinal OBJECT_LOCK to protect the whole process. */
} PartitionObjectLock;

typedef enum PartitionObjectLockType {
    PARTITION_SHARE_LOCK = AccessShareLock,
    PARTITION_EXCLUSIVE_LOCK = AccessExclusiveLock
} PartitionObjectLockType;

extern void LockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type);
extern void UnlockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type);
extern bool ConditionalLockPartitionObject(Oid relOid, PartitionObjectLock object, PartitionObjectLockType type);
#ifndef ENABLE_MULTIPLE_NODES
extern void AddPartitionDMLInfo(Oid relOid);
extern void AddPartitionDDLInfo(Oid relOid);
extern void LockPartitionDDLOperation();
#endif

/*----------------------------------------------------------------*/
/* Partition Concurrency Design Part2: PARTITIONNO/SUBPARTITIONNO */
/*
 * PARTITIONNO/SUBPARTITIONNO: An unique identifier of each partition in a partitioned/subpartitioned table.
 * The partitionno/subpartitionno is recorded in catalog PG_PARTITION. See pg_partition.h for more detail.
 * 1. When create a partitioned table containing n partitions, the partitionno of each partition is set to 1~n, and the
 *    partitioned tuple records the maxvalue, we use a negative value, -n to distinguish from the partition tuple.
 *    Similarly, if a partition contains m subpartitions, the subpartitionno of each subpartition is set to 1~m, and the
 *    partition tuple records the maxvalue -m.
 * 2. When DDL is done on the partitioned table, think of four base conditions:
 *    a). ADD_PARTITION_ACTION: Firstly we read the record of partitioned tuple, and get the maxvalue of partitionno,
 *        we mask it as -x(the maxvalue is records in negative type). Then the partitionno of the new partition is set
 *        to x+1, and the maxvalue is updated into -(x+1).
 *    b). DROP_PARTITION_ACTION: Do nothing.
 *    c). INPLACE_UPDATE_ACTION: Firstly we read the record of the src partition tuple, we mask it as k. Then the
 *        partitionno of the new partition is set to k. Do nothing on the partitioned tuple.
 *    d). OTHER_PARTITION_ACTION: Do nothing.
 *    In fact, a partition DDL can be a collection of these four action above. For example, a Merge-Partition operation
 *    has a series of DROP_PARTITION_ACTION and an ADD_PARTITION_ACTION, a Truncate-Partition operation with
 *    UPDATE_GLOBAL_INDEX has an OTHER_PARTITION_ACTION and a INPLACE_UPDATE_ACTION.
 * 3. When DML is done on the partitioned table, think of three stages: pruning -> obtain-partoid -> partition-open.
 *    In the period of pruning -> obtain-partoid, the partseq may be dislocationed, in the period of obtain-partoid ->
 *    partition-open, the partoid may be inplace-updated to a new oid with an UPDATE_GLOBAL_INDEX DDL action. So the
 *    partitionno is used to solve these problem.
 *    a). When pruning, we save partitionno list when obtain the partseq list. See the definition of PruningResult.
 *    b). When obtain-partoid, we use partseq to get the partoid, and check the src partitionno with dest one, if not
 *        match, we use partitionno to research. See the definition of getPartitionOidFromSequence.
 *    c). When partition-open, if we have the dest partitionno, firstly we will do tryPartitionOpen, if not found, just
 *        use partitionno to research the new partoid, and retry. See the definition of PartitionOpenWithPartitionno. */

/* if the current partitionno reached to MAX_PARTITION_NO, run RelationResetPartitionno to reset it. */
#define MAX_PARTITION_NO (INT32_MAX - MAX_PARTITION_NUM)
#define INVALID_PARTITION_NO 0
#define PARTITIONNO_IS_VALID(partitionno) ((partitionno) > 0)

extern HeapTuple ScanPgPartition(Oid targetPartId, bool indexOK, Snapshot snapshot);
extern Oid RelOidGetPartitionTupleid(Oid relOid);
extern int GetCurrentPartitionNo(Oid partOid);
extern int GetCurrentSubPartitionNo(Oid partOid);
extern void UpdateCurrentPartitionNo(Oid partOid, int partitionno, bool inplace);
extern void UpdateCurrentSubPartitionNo(Oid partOid, int subpartitionno);
extern Oid GetPartOidWithPartitionno(Oid parentid, int partitionno, char parttype);
extern Oid InvisiblePartidGetNewPartid(Oid partoid);
extern void SetPartitionnoForPartitionState(PartitionState *partTableState);
extern void RelationResetPartitionno(Oid relOid, LOCKMODE relationlock);
extern int GetPartitionnoFromSequence(PartitionMap *partmap, int partseq);

#define PARTITION_LOG(format, ...)                                                                              \
    do {                                                                                                        \
        if (module_logging_is_on(MOD_PARTITION)) {                                                              \
            ereport(DEBUG2, (errmodule(MOD_PARTITION), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
        }                                                                                                       \
    } while (0)

extern const uint32 PARTITION_ENHANCE_VERSION_NUM;
#define PARTITIONNO_IN_UPGRADE (t_thrd.proc->workingVersionNum < PARTITION_ENHANCE_VERSION_NUM)
#define PARTITIONNO_VALID_ASSERT(partitionno)                                             \
    Assert(PARTITIONNO_IN_UPGRADE || (!PARTITIONNO_IN_UPGRADE && (partitionno) > 0))

typedef void (*PartitionNameGetPartidCallback) (Oid partitioned_relation, const char *partition_name, Oid partId,
    Oid oldPartId, char partition_type, void *callback_arg, LOCKMODE callbackobj_lockMode);

/* some partition expr key info */
struct PartitionExprKeyInfo {
    bool partkeyexprIsNull;
    bool partkeyIsFunc;
    char* partExprKeyStr;

    PartitionExprKeyInfo()
    {
        partkeyexprIsNull = true;
        partkeyIsFunc = false;
        partExprKeyStr = NULL;
    }
};

/* some pg_partition tuple info */
struct PartitionTupleInfo {
    int2vector* pkey;
    oidvector* intablespace;
    Datum interval;
    Datum boundaries;
    Datum transitionPoint;
    Datum reloptions;
    int partitionno;
    int subpartitionno;
    PartitionExprKeyInfo partexprkeyinfo;

    PartitionTupleInfo()
    {
        pkey = NULL;
        intablespace = NULL;
        interval = (Datum)0;
        boundaries = (Datum)0;
        transitionPoint = (Datum)0;
        reloptions = (Datum)0;
        partitionno = INVALID_PARTITION_NO;
        subpartitionno = INVALID_PARTITION_NO;
        partexprkeyinfo = PartitionExprKeyInfo();
    }
};

extern void insertPartitionEntry(Relation pg_partition_desc, Partition new_part_desc, Oid new_part_id,
    PartitionTupleInfo *partTupleInfo);
extern bool isPartitionedObject(Oid relid, char relkind, bool missing_ok);
extern bool isSubPartitionedObject(Oid relid, char relkind, bool missing_ok);
extern bool isPartitionObject(Oid partid, char partkind, bool missing_ok);
extern Oid getPartitionIndexOid(Oid indexid, Oid partitionid);
extern Oid getPartitionIndexTblspcOid(Oid indexid, Oid partitionid);
extern char* getPartitionIndexName(Oid indexid, Oid partitionid);
extern Oid indexPartGetHeapPart(Oid indexPart, bool missing_ok);
extern Oid searchPartitionIndexOid(Oid partitionedIndexid, List *pindex);
extern List *getPartitionObjectIdList(Oid relid, char relkind);
extern List* getSubPartitionObjectIdList(Oid relid);
extern Oid PartitionNameGetPartitionOid (Oid partitionedTableOid, 
                                         const char *partitionName,
                                         char objectType,
                                         LOCKMODE lockMode,
                                         bool missingOk, 
                                         bool noWait,
                                         PartitionNameGetPartidCallback callback, 
                                         void *callback_arg,
                                         LOCKMODE callbackobj_lockMode);
extern Oid SubPartitionNameGetSubPartitionOid(Oid partitionedRelationOid,
                                              const char* subpartitionName,
                                              LOCKMODE partlock,
                                              LOCKMODE subpartlock,
                                              bool missingOk,
                                              bool noWait,
                                              PartitionNameGetPartidCallback callback,
                                              void* callback_arg,
                                              LOCKMODE callbackobj_lockMode,
                                              Oid *partOidForSubPart);
extern Oid PartitionValuesGetPartitionOid(Relation rel, List *partKeyValueList, LOCKMODE lockMode, bool topClosed,
                                          bool missingOk, bool noWait);
extern Oid SubPartitionValuesGetSubPartitionOid(Relation rel, List *partKeyValueList, List *subpartKeyValueList,
    LOCKMODE partlock, LOCKMODE subpartlock, bool topClosed, bool missingOk, bool noWait, Oid *partOidForSubPart);
extern List *searchPartitionIndexesByblid(Oid blid);
extern List *searchPgPartitionByParentId(char parttype, Oid parentId, ScanDirection direction = ForwardScanDirection);
extern List *searchPgSubPartitionByParentId(char parttype, List *parentOids,
    ScanDirection direction = ForwardScanDirection);
extern void freePartList(List *l);
extern HeapTuple searchPgPartitionByParentIdCopy(char parttype, Oid parentId);
extern Oid GetBaseRelOidOfParition(Relation relation);

extern List* relationGetPartitionOidList(Relation rel);
extern List* RelationGetSubPartitionOidList(Relation rel, LOCKMODE lockmode = AccessShareLock, bool estimate = false);
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
extern bool PartExprKeyIsNull(Relation rel, char** partExprKeyStr = NULL);

#endif

