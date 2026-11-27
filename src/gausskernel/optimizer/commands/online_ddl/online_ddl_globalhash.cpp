/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * online_ddl_globalhash.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/online_ddl/online_ddl_globalhash.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "catalog/namespace.h"
#include "utils/elog.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/lmgr.h"
#include "storage/smgr/relfilenode_hash.h"
#include "utils/fmgrtab.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

#include "commands/online_ddl_deltalog.h"

#include "commands/online_ddl_ctid_map.h"
#include "commands/online_ddl_util.h"
#include "commands/online_ddl_globalhash.h"

#define DDL_GLOBAL_HASH_TABLE_SIZE 1024
#define PARTITION_APPEND_MAP_SIZE 32
#define ONLINE_DDL_MEMROY_CONTEX (g_instance.online_ddl_cxt.context)

static uint32 DDLGlobalHashKeyOpt(const void* key, Size keysize)
{
    DDLGlobalHashKey ddlGlobalHashKey = *(DDLGlobalHashKey*)key;
    Assert(keysize == sizeof(DDLGlobalHashKey));
    return DatumGetUInt32(hash_any((const unsigned char*)&ddlGlobalHashKey, (int)keysize));
}

static int DDLGlobalHashKeyMatch(const void* key1, const void* key2, Size keysize)
{
    const DDLGlobalHashKey* node1 = (const DDLGlobalHashKey*)key1;
    const DDLGlobalHashKey* node2 = (const DDLGlobalHashKey*)key2;
    Assert(keysize == sizeof(DDLGlobalHashKey));

    /* we just care whether the result is 0 or not */
    return !((node1->spcNode == node2->spcNode) && (node1->dbNode == node2->dbNode) && (node1->relId == node2->relId));
}

// init ddl global hash table
void initDDLGlobalHash(MemoryContext memctx)
{
    if (g_instance.online_ddl_cxt.globalInfoHash != NULL) {
        return;
    }
    HASHCTL hashCtrl;
    errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "\0", "\0");

    hashCtrl.keysize = sizeof(DDLGlobalHashKey);
    hashCtrl.entrysize = sizeof(DDLGlobalHashEntry);
    hashCtrl.hash = DDLGlobalHashKeyOpt;
    hashCtrl.match = DDLGlobalHashKeyMatch;
    hashCtrl.hcxt = memctx;
    LWLockAcquire(OnlineDDLHashLock, LW_EXCLUSIVE);
    g_instance.online_ddl_cxt.globalInfoHash = hash_create("ddl global hash table", DDL_GLOBAL_HASH_TABLE_SIZE,
                                                           &hashCtrl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    LWLockRelease(OnlineDDLHashLock);
}

DDLGlobalHashEntry* onlineDDLInitHashEntry(Relation relation, bool appendMode, OnlineDDLType onlineDDLType)
{
    DDLGlobalHashKey hashKey = GetDDLGlobalHashKey(relation->rd_node, relation->rd_id);

    TransactionId xid = GetCurrentTransactionId();
    uint64 sessionId = t_thrd.proc->sessionid;
    OnlineDDLRelOperators* operators = New(ONLINE_DDL_MEMROY_CONTEX)
        OnlineDDLRelOperators(ONLINE_DDL_MEMROY_CONTEX, relation, appendMode, onlineDDLType);
    operators->setStartXid(xid);
    operators->setSessionId(sessionId);

    DDLGlobalHashEntry* hashEntry =
        (DDLGlobalHashEntry*)MemoryContextAlloc(ONLINE_DDL_MEMROY_CONTEX, sizeof(DDLGlobalHashEntry));
    hashEntry->hashKey = hashKey;
    hashEntry->relId = relation->rd_id;
    hashEntry->operators = operators;

    return hashEntry;
}

void OnlineDDLRegisterGlobalHashEntry(DDLGlobalHashKey hashKey, OnlineDDLRelOperators* operators)
{
    /* register global hash entry */
    DDLGlobalHashEntry* entry = NULL;
    LWLockAcquire(OnlineDDLHashLock, LW_EXCLUSIVE);
    entry = (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &hashKey, HASH_ENTER, NULL);
    LWLockRelease(OnlineDDLHashLock);
    if (entry == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Failed to register global hash entry.")));
    }
    operators->setStatus(ONLINE_DDL_STATUS_PREPARE);
    entry->hashKey = hashKey;
    entry->operators = operators;

    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL), errmsg("Online DDL global hash entry registered for relation %u.%u.%u",
                                               hashKey.spcNode, hashKey.dbNode, hashKey.relId)));
}

void OnlineDDLRegisterGlobalHashEntry(DDLGlobalHashEntry* entry)
{
    /* check if exist in global hash */
    DDLGlobalHashKey key = entry->hashKey;
    bool found = false;
    LWLockAcquire(OnlineDDLHashLock, LW_EXCLUSIVE);
    DDLGlobalHashEntry* result =
        (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &key, HASH_ENTER, &found);
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL), errmsg("[Online-DDL] OnlineDDLRegisterGlobalHashEntry: hashKey {%u, %u, %u}",
                                               key.spcNode, key.dbNode, key.relId)));

    /*
     * If the entry is found, we need to check the status.
     * Maybe happend when the previous online ddl is finished, but the entry
     */
    if (found) {
        if (result->operators->getStatus() == ONLINE_DDL_END) {
            /* if the status is end, we need to delete the entry */
            result =
                (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &key, HASH_REMOVE, NULL);
            /* free the memory */
            delete result->operators;
            result =
                (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &key, HASH_ENTER, &found);
            ereport(ONLINE_DDL_LOG_LEVEL,
                    (errmodule(MOD_ONLINE_DDL),
                     errmsg("[Online-DDL] OnlineDDLRegisterGlobalHashEntry reinit: hashKey {%u, %u, %u}"
                            " reason: not found",
                            key.spcNode, key.dbNode, key.relId)));
            Assert(!found);
        } else {
            /* If the status is not end, we need to update the entry. */
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("[Online-ddl] Online DDL operation is already in progress for relation %u.%u.%u",
                                   key.spcNode, key.dbNode, key.relId)));
        }
    }
    result->hashKey = entry->hashKey;
    result->relId = entry->relId;
    result->operators = entry->operators;
    LWLockRelease(OnlineDDLHashLock);
}

bool OnlineDDLReleaseHashEntry(DDLGlobalHashKey hashKey, TransactionId xid)
{
    if (g_instance.online_ddl_cxt.globalInfoHash == NULL) {
        return false;
    }

    DDLGlobalHashEntry* entry = NULL;
    bool found = false;

    LWLockAcquire(OnlineDDLHashLock, LW_EXCLUSIVE);
    entry = (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &hashKey, HASH_FIND, &found);
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL), errmsg("[Online-DDL] OnlineDDLReleaseHashEntry: hashKey {%u, %u, %u} ,"
                                               "find out the entry, found: %d",
                                               hashKey.spcNode, hashKey.dbNode, hashKey.relId, found)));
    if (!found || entry == NULL || entry->operators->getStartXid() != xid) {
        LWLockRelease(OnlineDDLHashLock);
        return false;
    }

    OnlineDDLRelOperators* operators = entry->operators;
    if (operators->getStatus() == ONLINE_DDL_END || operators->getStatus() == ONLINE_DDL_STATUS_NONE) {
        entry = (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &hashKey, HASH_REMOVE, NULL);
        ereport(ONLINE_DDL_LOG_LEVEL,
                (errmodule(MOD_ONLINE_DDL),
                 errmsg("[Online-DDL] OnlineDDLReleaseHashEntry: hashKey {%u, %u, %u}, %d is disabled.",
                        hashKey.spcNode, hashKey.dbNode, hashKey.relId, operators->getStatus())));
        /* free the memory */
        delete operators;
        entry->operators = NULL;
        LWLockRelease(OnlineDDLHashLock);
        return true;
    } else {
        LWLockRelease(OnlineDDLHashLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Online DDL operation is already in progress for relation %u.%u.%u.", hashKey.spcNode,
                               hashKey.dbNode, hashKey.relId)));
    }
    LWLockRelease(OnlineDDLHashLock);
    return false;
}

OnlineDDLRelOperators::OnlineDDLRelOperators(MemoryContext context, Relation relation, bool appendMode,
                                             OnlineDDLType onlineDDLType)
    : relId(relation->rd_id),
      relfilenode(relation->rd_node),
      status(ONLINE_DDL_START),
      onlineDDLType(onlineDDLType),
      startXid(InvalidTransactionId),
      sessionId(0),
      tempSchemaName(NULL),
      deltaRelname(NULL),
      deltaRelation(NULL),
      deltaRelid(InvalidOid),
      deltaRelnamespace(InvalidOid),
      ctidMapRelation(NULL),
      baselineSnapshot(NULL),
      appender(NULL),
      appendMode(appendMode),
      endCtidInternal({0, 0}),
      targetBlockNumber(0),
      isForPartition(RELATION_IS_PARTITIONED(relation)),
      partitionAppendMap(NULL),
      currentPartitionOid(0),
      partRelInfo({0, 0, 0, false}),
      context(context),
      startTime(GetCurrentTimestamp())
{}

OnlineDDLRelOperators::~OnlineDDLRelOperators()
{
    if (tempSchemaName != NULL) {
        DestroyStringInfo(tempSchemaName);
    }
    if (deltaRelation != NULL) {
        relation_close(deltaRelation, AccessExclusiveLock);
        deltaRelation = NULL;
    }
    if (deltaRelname != NULL) {
        DestroyStringInfo(deltaRelname);
    }
    if (partitionAppendMap != NULL) {
        destroyPartitionAppendMap();
    }
}

void OnlineDDLRelOperators::openDeltaRelation(LOCKMODE lockmode)
{
    if (this->deltaRelname == NULL || this->deltaRelname->len == 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation name is not set.")));
    }

    Oid schemaOid = get_namespace_oid(this->tempSchemaName->data, false);
    if (!OidIsValid(schemaOid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                        errmsg("Schema \"%s\" does not exist.", this->tempSchemaName->data)));
    }

    Oid deltaRelOid = get_relname_relid(this->deltaRelname->data, schemaOid);
    if (!OidIsValid(deltaRelOid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("Delta relation \"%s\" does not exist.", this->deltaRelname->data)));
    }

    this->deltaRelation = relation_open(deltaRelOid, lockmode);
    if (this->deltaRelation == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Failed to open delta relation \"%s\".", this->deltaRelname->data)));
    }
}

void OnlineDDLRelOperators::initCtidMapRelation()
{
    StringInfo tempSchemaName = this->tempSchemaName;
    Assert(tempSchemaName != NULL && tempSchemaName->len > 0);
    Oid ctidMapOid = OnlineDDLCreateCtidMap(tempSchemaName);
    this->ctidMapRelation = relation_open(ctidMapOid, AccessShareLock);
}

void OnlineDDLRelOperators::closeDeltaRelation(LOCKMODE lockmode)
{
    if (this->deltaRelation == NULL) {
        return;
    }

    /* close delta relation */
    if (this->deltaRelation->rd_refcnt > 0) {
        relation_close(this->deltaRelation, lockmode);
    }
    this->deltaRelation = NULL;
}

void OnlineDDLRelOperators::closeCtidMapRelation(LOCKMODE lockmode)
{
    if (this->ctidMapRelation == NULL) {
        return;
    }

    /* close ctid map relation */
    if (this->ctidMapRelation->rd_refcnt > 0) {
        relation_close(this->ctidMapRelation, lockmode);
    }
    this->ctidMapRelation = NULL;
}

bool OnlineDDLRelOperators::recordTupleInsert(Relation relation, ItemPointer tid)
{
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL),
             errmsg("Record tuple insert for relation %u, tid: (%u, %u), no need to record.", relation->rd_id,
                    ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid))));
    return true;
}

bool OnlineDDLRelOperators::recordTupleEmpty()
{
    ItemPointerData ctid;
    ItemPointerSetBlockNumber(&ctid, 0);
    ItemPointerSetOffsetNumber(&ctid, 1);
    Relation deltaRel = relation_open(this->deltaRelid, RowExclusiveLock);
    OnlineDDLEmptyDeltaLog(deltaRel, &ctid);
    relation_close(deltaRel, RowExclusiveLock);
    return true;
}

bool OnlineDDLRelOperators::recordTupleMultiInsert(Relation relation, Tuple* tuples, int ntuples)
{
    bool result = true;
    Relation deltaRel = relation_open(this->deltaRelid, RowExclusiveLock);
    for (int i = 0; i < ntuples; i++) {
        result = recordTupleInsert(deltaRel, &((HeapTuple)tuples[i])->t_self) && result;
    }
    relation_close(deltaRel, RowExclusiveLock);
    if (result == false) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("[Online-DDL] Failed to record multi insert tuples for relation %u.", relation->rd_id)));
        return false;
    }
    return true;
}

bool OnlineDDLRelOperators::recordTupleDelete(Relation relation, ItemPointer tid, Oid partOid)
{
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL),
             errmsg("Record tuple delete for relation %u, partition %u, tid: (%u, %u)", relation->rd_id, partOid,
                    ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid))));
    if (onlineDDLType > ONLINE_DDL_CHECK) {
        Relation deltaRel = relation_open(this->deltaRelid, RowExclusiveLock);
        OnlineDDLDeleteDeltaLog(deltaRel, tid, partOid);
        relation_close(deltaRel, RowExclusiveLock);
    }
    return true;
}

bool OnlineDDLRelOperators::recordTupleDelete(Relation relation, ItemPointer tid)
{
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL), errmsg("Record tuple delete for relation %u, tid: (%u, %u)", relation->rd_id,
                                               ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid))));
    if (onlineDDLType > ONLINE_DDL_CHECK) {
        Relation deltaRel = relation_open(this->deltaRelid, RowExclusiveLock);
        OnlineDDLDeleteDeltaLog(deltaRel, tid);
        relation_close(deltaRel, RowExclusiveLock);
    }
    return true;
}

bool OnlineDDLRelOperators::recordTupleUpdate(Relation relation, ItemPointer oldTid, ItemPointer newTid)
{
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmodule(MOD_ONLINE_DDL),
             errmsg("Record tuple update for relation %u, oldTid: (%u, %u), newTid: (%u, %u)", relation->rd_id,
                    ItemPointerGetBlockNumber(oldTid), ItemPointerGetOffsetNumber(oldTid),
                    ItemPointerGetBlockNumber(newTid), ItemPointerGetOffsetNumber(newTid))));
    if (onlineDDLType > ONLINE_DDL_CHECK) {
        Relation deltaRel = relation_open(this->deltaRelid, RowExclusiveLock);
        OnlineDDLUpdateDeltaLog(deltaRel, oldTid, newTid);
        relation_close(deltaRel, RowExclusiveLock);
    }
    return true;
}

bool OnlineDDLRelOperators::enableTargetRelationAppendMode(ItemPointerData endCtid)
{
    this->appendMode = true;
    this->endCtidInternal = endCtid;
    this->targetBlockNumber = BlockIdGetBlockNumber(&(endCtid.ip_blkid));
    return true;
}

void OnlineDDLRelOperators::createPartitionAppendMap()
{
    Assert(this->onlineDDLType != ONLINE_DDL_VACUUM);
    if (this->partitionAppendMap != NULL) {
        return;
    }
    MemoryContext oldcxt = MemoryContextSwitchTo(this->context);
    HASHCTL hashCtrl;
    errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "\0", "\0");
    hashCtrl.keysize = sizeof(Oid);
    hashCtrl.entrysize = sizeof(PartitionAppendEntry);
    hashCtrl.hcxt = this->context;
    StringInfo hashName = makeStringInfo();
    appendStringInfo(hashName, "Online DDL Partition Append Map %u", this->startXid);
    this->partitionAppendMap =
        hash_create(hashName->data, PARTITION_APPEND_MAP_SIZE, &hashCtrl, HASH_ELEM | HASH_CONTEXT);
    DestroyStringInfo(hashName);
    MemoryContextSwitchTo(oldcxt);
}

void OnlineDDLRelOperators::destroyPartitionAppendMap()
{
    hash_destroy(this->partitionAppendMap);
    this->partitionAppendMap = NULL;
}

bool OnlineDDLRelOperators::enableTargetRelationAppendMode(Oid partOid, ItemPointerData endCtid)
{
    this->appendMode = true;
    createPartitionAppendMap();
    bool found = false;
    PartitionAppendEntry* entry =
        (PartitionAppendEntry*)hash_search(this->partitionAppendMap, &partOid, HASH_ENTER, &found);

    entry->endCtid = endCtid;
    return true;
}

ItemPointerData OnlineDDLRelOperators::getEndCtidForPartition(Oid partOid)
{
    if (this->partitionAppendMap == nullptr) {
        return this->endCtidInternal;
    }

    bool found = false;
    PartitionAppendEntry* entry =
        (PartitionAppendEntry*)hash_search(this->partitionAppendMap, &partOid, HASH_FIND, &found);

    if (found) {
        return entry->endCtid;
    } else {
        ItemPointerData invalid;
        ItemPointerSetInvalid(&invalid);
        return invalid;
    }
}

bool OnlineDDLRelOperators::clearTargetRelationAppendMode()
{
    if (!appendMode) {
        return false;
    }
    this->appendMode = false;
    errno_t rc = memset_s(&this->endCtidInternal, sizeof(ItemPointerData), 0, sizeof(ItemPointerData));
    securec_check(rc, "\0", "\0");
    this->targetBlockNumber = InvalidBlockNumber;
    return true;
}

bool OnlineDDLRelOperators::insertCtidMap(ItemPointer oldTid, Oid relId, ItemPointer newTid)
{
    if (onlineDDLType <= ONLINE_DDL_CHECK) {
        return false;
    }
    if (this->partitionAppendMap != NULL) {
        return OnlineDDLInsertCtidMap(oldTid, relId, newTid, this->ctidMapRelation);
    } else {
        return OnlineDDLInsertCtidMap(oldTid, newTid, this->ctidMapRelation);
    }
}

void OnlineDDLRelOperators::OnlineDDLAppendIncrementalData(Relation oldRelation, Relation newRelation,
                                                           AlteredTableInfo* alterTableInfo, OnlineDDLScenario scenario)
{
    List* indexOidList = NIL;
    Relation ctidMapIndex = NULL;

    /* Get ctid map index */
    indexOidList = RelationGetIndexList(this->ctidMapRelation);
    if (list_length(indexOidList) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("[Online-DDL] ctid map relation %u must have only one index.", this->ctidMapRelation->rd_id)));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);
    ctidMapIndex = index_open(indexOid, AccessShareLock);
    list_free_ext(indexOidList);

    if (onlineDDLType > ONLINE_DDL_CHECK) {
        this->appender = OnlineDDLInitAppender(oldRelation, newRelation, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->endCtidInternal, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data copy completed, start to append incremental data.")));
        OnlineDDLAppend(this->appender, scenario);
        CleanupAppender(this->appender);
        this->appender = NULL;
    } else {
        this->appender = OnlineDDLInitAppender(oldRelation, NULL, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->endCtidInternal, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data check completed, start to check incremental data.")));
        OnlineDDLOnlyCheck(this->appender);
        CleanupAppender(this->appender);
        this->appender = NULL;
    }
    if (ctidMapIndex != NULL) {
        index_close(ctidMapIndex, AccessShareLock);
    }
}

/* for vacuum */
void OnlineDDLRelOperators::OnlineDDLAppendIncrementalData(Relation oldRelation, Relation newRelation,
                                                           TransactionId oldestXmin, TransactionId freezeXid,
                                                           OnlineDDLScenario scenario)
{
    List* indexOidList = NIL;
    Relation ctidMapIndex = NULL;

    /* Get ctid map index */
    indexOidList = RelationGetIndexList(this->ctidMapRelation);
    if (list_length(indexOidList) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("[Online-DDL] ctid map relation %u must have only one index.", this->ctidMapRelation->rd_id)));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);
    ctidMapIndex = index_open(indexOid, AccessShareLock);
    list_free_ext(indexOidList);

    Assert(onlineDDLType == ONLINE_DDL_VACUUM || onlineDDLType == ONLINE_DDL_CLUSTER);
    this->appender = OnlineDDLInitAppender(oldRelation, newRelation, this->deltaRelation, this->ctidMapRelation,
                                           ctidMapIndex, this->endCtidInternal, NULL, onlineDDLType);
    ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                     errmsg("Online DDL baseline data copy completed, start to append incremental data.")));
    OnlineDDLAppenderInitVacuumState(this->appender, freezeXid, oldestXmin);
    if (this->partRelInfo.relId != InvalidOid) {
        OnlineDDLInitPartRelInfo(this->appender, this->partRelInfo.relId, this->partRelInfo.subParentId,
                                 this->partRelInfo.partOid, this->partRelInfo.isSubPartition);
    }
    OnlineDDLAppend(this->appender, scenario);
    CleanupAppender(this->appender);
    this->appender = NULL;
    if (ctidMapIndex != NULL) {
        index_close(ctidMapIndex, AccessShareLock);
    }
}

/* for partition table */
void OnlineDDLRelOperators::OnlineDDLAppendIncrementalData(List* oldPartRelList, List* newOidList,
                                                           AlteredTableInfo* alterTableInfo, OnlineDDLScenario scenario)
{
    List* indexOidList = NIL;
    Relation ctidMapIndex = NULL;

    /* Get ctid map index */
    indexOidList = RelationGetIndexList(this->ctidMapRelation);
    if (list_length(indexOidList) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("[Online-DDL] ctid map relation %u must have only one index.", this->ctidMapRelation->rd_id)));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);
    ctidMapIndex = index_open(indexOid, AccessShareLock);
    list_free_ext(indexOidList);

    if (onlineDDLType > ONLINE_DDL_CHECK) {
        this->appender = OnlineDDLInitAppender(oldPartRelList, newOidList, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->partitionAppendMap, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data copy completed, start to append incremental data.")));
        OnlineDDLAppend(this->appender, scenario);
        CleanupAppender(this->appender);
        this->appender = NULL;
    } else {
        this->appender = OnlineDDLInitAppender(oldPartRelList, NIL, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->partitionAppendMap, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data check completed, start to check incremental data.")));
        OnlineDDLOnlyCheck(this->appender);
        CleanupAppender(this->appender);
        this->appender = NULL;
    }
    if (ctidMapIndex != NULL) {
        index_close(ctidMapIndex, AccessShareLock);
    }
}

/* for partition vacuum */
void OnlineDDLRelOperators::OnlineDDLAppendIncrementalData(List* oldPartRelList, List* newOidList,
                                                           TransactionId oldestXmin, TransactionId freezeXid,
                                                           OnlineDDLScenario scenario)
{
    List* indexOidList = NIL;
    Relation ctidMapIndex = NULL;

    /* Get ctid map index */
    indexOidList = RelationGetIndexList(this->ctidMapRelation);
    if (list_length(indexOidList) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("[Online-DDL] ctid map relation %u must have only one index.", this->ctidMapRelation->rd_id)));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);
    ctidMapIndex = index_open(indexOid, AccessShareLock);
    list_free_ext(indexOidList);

    if (onlineDDLType > ONLINE_DDL_CHECK) {
        this->appender = OnlineDDLInitAppender(oldPartRelList, newOidList, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->partitionAppendMap, NULL, onlineDDLType);

        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data copy completed, start to append incremental data.")));
        OnlineDDLAppenderInitVacuumState(this->appender, freezeXid, oldestXmin);
        OnlineDDLAppend(this->appender, scenario);
        CleanupAppender(this->appender);
        this->appender = NULL;
    }
    if (ctidMapIndex != NULL) {
        index_close(ctidMapIndex, AccessShareLock);
    }
}

void OnlineDDLRelOperators::OnlineDDLAppendIncrementalData(List* oldPartRelList, Relation newRelation,
                                                           AlteredTableInfo* alterTableInfo, OnlineDDLScenario scenario)
{
    List* indexOidList = NIL;
    Relation ctidMapIndex = NULL;

    /* Get ctid map index */
    indexOidList = RelationGetIndexList(this->ctidMapRelation);
    if (list_length(indexOidList) != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("[Online-DDL] ctid map relation %u must have only one index.", this->ctidMapRelation->rd_id)));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);
    ctidMapIndex = index_open(indexOid, AccessShareLock);
    list_free_ext(indexOidList);

    if (onlineDDLType > ONLINE_DDL_CHECK) {
        this->appender = OnlineDDLInitAppender(oldPartRelList, newRelation, this->deltaRelation, this->ctidMapRelation,
                                               ctidMapIndex, this->partitionAppendMap, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data copy completed, start to append incremental data.")));
        OnlineDDLAppend(this->appender, scenario);
    } else {
        this->appender =
            OnlineDDLInitAppender(oldPartRelList, (Relation)NULL, this->deltaRelation, this->ctidMapRelation,
                                  ctidMapIndex, this->partitionAppendMap, alterTableInfo, onlineDDLType);
        ereport(NOTICE, (errmodule(MOD_ONLINE_DDL),
                         errmsg("Online DDL baseline data check completed, start to check incremental data.")));
        OnlineDDLOnlyCheck(this->appender);
    }
    if (ctidMapIndex != NULL) {
        index_close(ctidMapIndex, AccessShareLock);
    }
}

bool CheckOnlineDDLStatusRunning(DDLGlobalHashKey hashKey, TransactionId xid)
{
    if (g_instance.online_ddl_cxt.globalInfoHash == NULL) {
        return false;
    }

    DDLGlobalHashEntry* entry = NULL;
    bool found = false;

    entry = (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &hashKey, HASH_FIND, &found);
    if (!found || entry == NULL) {
        ereport(ONLINE_DDL_LOG_LEVEL,
                (errmodule(MOD_ONLINE_DDL),
                 errmsg("[Online-DDL] CheckOnlineDDLStatusRunning: hashKey {%u, %u, %u} is disabled,"
                        " reason: not found",
                        hashKey.spcNode, hashKey.dbNode, hashKey.relId)));
        return false;
    }
    if (entry->operators->getStatus() == ONLINE_DDL_STATUS_NONE || entry->operators->getStatus() == ONLINE_DDL_END ||
        xid != entry->operators->getStartXid()) {
        ereport(ONLINE_DDL_LOG_LEVEL,
                (errmodule(MOD_ONLINE_DDL),
                 errmsg("[Online-DDL] CheckOnlineDDLStatusRunning: hashKey {%u, %u, %u} is disabled,"
                        " reason: status is %d, or xid not match, current xid %lu, entry xid %lu",
                        hashKey.spcNode, hashKey.dbNode, hashKey.relId, entry->operators->getStatus(), xid,
                        entry->operators->getStartXid())));
        return false;
    }

    return true;
}

DDLGlobalHashEntry* OnlineDDLGetHashEntry(DDLGlobalHashKey hashKey)
{
    if (g_instance.online_ddl_cxt.globalInfoHash == NULL) {
        return NULL;
    }
    DDLGlobalHashEntry* entry = NULL;
    bool found = false;

    entry = (DDLGlobalHashEntry*)hash_search(g_instance.online_ddl_cxt.globalInfoHash, &hashKey, HASH_FIND, &found);
    if (!found) {
        entry = NULL;
    }
    return entry;
}

void OnlineDDLRelationSetup(Relation relation)
{
    if (RelationGetAppendMode(relation) != ONLINE_DDL_APPEND_MODE) {
        return;
    }
    DDLGlobalHashEntry* hashEntry = OnlineDDLGetHashEntry(GetDDLGlobalHashKey(relation->rd_node, relation->rd_id));
    if (hashEntry != NULL && (hashEntry->operators->getStatus() != ONLINE_DDL_STATUS_NONE &&
                              hashEntry->operators->getStatus() != ONLINE_DDL_END)) {
        relation->rd_online_ddl_operators = (void*)hashEntry->operators;
    }
}

void OnlineDDLRelationSetup(Relation relation, Relation parentRelation, Partition partition)
{
    if (RelationGetAppendMode(parentRelation) != ONLINE_DDL_APPEND_MODE) {
        return;
    }
    /* if parentRelation is parent partition of subpartition table, then use the relid of subpartitioned relation */
    int relId =
        RelationIsPartitionOfSubPartitionTable(parentRelation) ? parentRelation->parentId : parentRelation->rd_id;
    DDLGlobalHashEntry* hashEntry =
        OnlineDDLGetHashEntry(GetDDLGlobalHashKey(parentRelation->rd_node, relId));
    /* Only target partition can be setup */
    if (hashEntry != NULL && (hashEntry->operators->getStatus() != ONLINE_DDL_STATUS_NONE &&
                              hashEntry->operators->getStatus() != ONLINE_DDL_END)) {
        if (hashEntry->operators->getCurrentPartitionOid() != NULL &&
            hashEntry->operators->getCurrentPartitionOid() != partition->pd_id) {
            return;
        }
        relation->rd_online_ddl_operators = (void*)hashEntry->operators;
    }
}