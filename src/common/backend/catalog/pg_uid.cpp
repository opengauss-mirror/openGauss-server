/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2021-2023. All rights reserved.
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
 * -------------------------------------------------------------------------
 *
 * pg_uid.cpp
 *
 * IDENTIFICATION
 *     src/common/backend/catalog/pg_uid.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/namespace.h"
#include "catalog/pg_uid.h"
#include "catalog/pg_uid_fn.h"
#include "catalog/storage.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_class.h"
#include "pgxc/redistrib.h"
#include "storage/lock/lock.h"
#include "tcop/utility.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "access/hash.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "access/reloptions.h"
#include "access/hbucket_am.h"
#include "executor/node/nodeModifyTable.h"
#include "nodes/makefuncs.h"

static UidHashValue* FindUidHashCache(Oid dbOid, Oid relOid);

void DeleteDatabaseUidEntry(Oid dbOid)
{
    HASH_SEQ_STATUS status;
    UidHashValue* entry;
    LWLockAcquire(HashUidLock, LW_EXCLUSIVE);
    hash_seq_init(&status, t_thrd.storage_cxt.uidHashCache);
    while ((entry = (UidHashValue *) hash_seq_search(&status)) != NULL) {
        if (entry->key.dbOid != dbOid) {
            continue;
        }
        if (hash_search(t_thrd.storage_cxt.uidHashCache, &entry->key, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errmsg("uid hash cache corrupted")));
        }
    }
    LWLockRelease(HashUidLock);
}

/* Delete uid entry in gs_uid, only drop table call this function. */
void DeleteUidEntry(Oid relid)
{
    HeapTuple tuple  = SearchSysCache1(UIDRELID, relid);
    if (!HeapTupleIsValid(tuple)) {
        return;
    }

    /* remove hash table fisrt */
    UidHashKey key = {.dbOid = u_sess->proc_cxt.MyDatabaseId, .relOid = relid};
    LWLockAcquire(HashUidLock, LW_EXCLUSIVE);
    (void)hash_search(t_thrd.storage_cxt.uidHashCache, (void*)&key, HASH_REMOVE, NULL);
    LWLockRelease(HashUidLock);

    Relation gs_uid = heap_open(UidRelationId, RowExclusiveLock);
    simple_heap_delete(gs_uid, &tuple->t_self);
    ReleaseSysCache(tuple);
    heap_close(gs_uid, RowExclusiveLock);
    return;
}

void InsertUidEntry(Oid relid)
{
    Datum values[Natts_gs_uid];
    bool  nulls[Natts_gs_uid];
    errno_t errorno = EOK;

    HeapTuple tuple  = SearchSysCache1(UIDRELID, relid);
    /* already inserted one */
    if (HeapTupleIsValid(tuple)) {
        ReleaseSysCache(tuple);
        return;
    }

    /* This is a tad tedious, but way cleaner than what we used to do... */
    errorno = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check_c(errorno, "\0", "\0");

    errorno = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(errorno, "\0", "\0");

    Relation gs_uid = heap_open(UidRelationId, RowExclusiveLock);
    values[Anum_gs_uid_relid - 1] = ObjectIdGetDatum(relid);
    values[Anum_gs_uid_backup - 1] = UInt64GetDatum(0);
    HeapTuple newTuple = heap_form_tuple(RelationGetDescr(gs_uid), values, nulls);
    (void)simple_heap_insert(gs_uid, newTuple);
    CatalogUpdateIndexes(gs_uid, newTuple);

    heap_freetuple_ext(newTuple);
    heap_close(gs_uid, RowExclusiveLock);
    return;
}
static void UpdateUidEntryInternal(Oid relOid, uint64 &backupUid, bool init)
{
    Relation gs_uid = heap_open(UidRelationId, RowExclusiveLock);
    HeapTuple tuple = SearchSysCacheCopy1(UIDRELID, relOid);
    Assert(HeapTupleIsValid(tuple));
    Form_gs_uid relUid = (Form_gs_uid)GETSTRUCT(tuple);
    if (init) {
        backupUid = relUid->uid_backup + UID_RESTORE_DURATION;
    }
    if ((uint64)relUid->uid_backup < backupUid) { /* check again */
        relUid->uid_backup = backupUid;
        heap_inplace_update(gs_uid, tuple, true);
    }
    heap_freetuple_ext(tuple);
    heap_close(gs_uid, RowExclusiveLock);
}

static bool UidEntryExist(Oid relOid)
{
    HeapTuple tuple = SearchSysCache1(UIDRELID, relOid);
    if (!HeapTupleIsValid(tuple)) {
        return false;
    } else {
        ReleaseSysCache(tuple);
        return true;
    }
}

static bool UpdateUidEntry(Relation relation, uint64 backupUid, UidHashValue* value)
{
    uint32 expected = BACKUP_NO_START;
    bool backup = pg_atomic_compare_exchange_u32(&value->backUpState,
        &expected, BACKUP_IN_PROGRESS);
    if (!backup) {
        return false; /* other one is doing the same thing */
    }
    Assert(value);
    /* recheck again, quick bypass */
    if (value->backupUidRange >= backupUid) { /* safe to compare if hold uidBackup flag */
        pg_atomic_write_u32(&value->backUpState, BACKUP_NO_START);
        return false;
    }
    PG_TRY();
    {
        UpdateUidEntryInternal(RelationGetRelid(relation), backupUid, false);
    }
    PG_CATCH();
    {
        ereport(LOG, (errmsg("Update uid hash cache failed.")));
        pg_atomic_write_u32(&value->backUpState, BACKUP_NO_START);
        PG_RE_THROW();
    }
    PG_END_TRY();
    value->backupUidRange = backupUid; /* safe to write if hold uidBackup flag */
    pg_atomic_write_u32(&value->backUpState, BACKUP_NO_START);
    return true;
}

void InitUidCache(void)
{
    int rc = 0;
    HASHCTL ctl;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");

    ctl.keysize = sizeof(UidHashKey);
    ctl.entrysize = sizeof(UidHashValue);
    ctl.hash = tag_hash;
    t_thrd.storage_cxt.uidHashCache = HeapMemInitHash(
        "Shared Uid hash by request", 64, 256, &ctl, HASH_ELEM | HASH_FUNCTION);
    if (!t_thrd.storage_cxt.uidHashCache)
        ereport(FATAL, (errmsg("could not initialize shared uid hash table")));
}

static UidHashValue* FindUidHashCache(Oid dbOid, Oid relOid)
{
    UidHashKey key = {.dbOid = dbOid, .relOid = relOid};
    UidHashValue* value = NULL;
    LWLockAcquire(HashUidLock, LW_SHARED);
    value = (UidHashValue*)hash_search(t_thrd.storage_cxt.uidHashCache,
        (void*)&key, HASH_FIND, NULL);
    LWLockRelease(HashUidLock);
    return value;
}

void BuildUidHashCache(Oid dbOid, Oid relOid)
{
    if (FindUidHashCache(dbOid, relOid) || !UidEntryExist(relOid) || RecoveryInProgress()) {
        return;
    }
    UidHashKey key = {.dbOid = dbOid, .relOid = relOid};
    UidHashValue* value = NULL;
    bool found = false;
    LWLockAcquire(HashUidLock, LW_EXCLUSIVE);
    value = (UidHashValue*)hash_search(t_thrd.storage_cxt.uidHashCache,
        (void*)&key, HASH_ENTER, &found);
    if (!found) {
        PG_TRY();
        {
            uint64 backupUid = 0;
            UpdateUidEntryInternal(relOid, backupUid, true);
            Assert(backupUid - UID_RESTORE_DURATION >= 0);
            value->currentUid = (backupUid - UID_RESTORE_DURATION) + 1;
            value->backupUidRange = backupUid;
            value->backUpState = BACKUP_NO_START;
        }
        PG_CATCH();
        {
            ereport(LOG, (errmsg("Build uid hash cache failed.")));
            hash_search(t_thrd.storage_cxt.uidHashCache, (void*)&key, HASH_REMOVE, NULL);
            LWLockRelease(HashUidLock);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    LWLockRelease(HashUidLock);
}

#define FetchCurState(curBackupUid, curUid, value)                 \
    do {                                                           \
        curBackupUid = pg_atomic_read_u64(&value->backupUidRange); \
        curUid = pg_atomic_read_u64(&value->currentUid);           \
    } while (0)

#define TRIGGER_FACTOR (4)
uint64 GetNewUidForTuple(Relation relation)
{
    Assert(RELATION_HAS_UIDS(relation));
    Oid dbOid = relation->rd_node.dbNode;
    Oid relOid = RelationGetRelid(relation);
    UidHashValue* value = (UidHashValue*)FindUidHashCache(dbOid, relOid);
    if (value == NULL) {
        BuildUidHashCache(dbOid, relOid);
        value = (UidHashValue*)FindUidHashCache(dbOid, relOid);
    }
    Assert(value);
    uint64 res;
    uint64 curBackupUid;
    uint64 curUid;
    FetchCurState(curBackupUid, curUid, value);
    if (curUid + (uint64)UID_RESTORE_DURATION / TRIGGER_FACTOR > curBackupUid) {
        (void)UpdateUidEntry(relation, curBackupUid + UID_RESTORE_DURATION, value);
    }
    res = pg_atomic_fetch_add_u64(&value->currentUid, 1);
    FetchCurState(curBackupUid, curUid, value);
    while (res > curBackupUid) {
        (void)UpdateUidEntry(relation, curBackupUid + UID_RESTORE_DURATION, value);
        pg_usleep(10000L); /* 10ms delay */
        FetchCurState(curBackupUid, curUid, value);
    }
    return res;
}

