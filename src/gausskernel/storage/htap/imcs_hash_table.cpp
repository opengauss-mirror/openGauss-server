/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * imcs_hash_table.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imcs_hash_table.cpp
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/cucache_mgr.h"
#include "utils/aiomem.h"
#include "executor/instrument.h"
#include "utils/resowner.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "replication/syncrep.h"
#include "storage/cu.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/ss_imcucache_mgr.h"
#include "access/htap/imcs_hash_table.h"

IMCSHashTable* IMCSHashTable::m_imcs_hash_tbl = NULL;

/*
 * @Description: get Singleton Instance of IMCS hash table.
 * @Return: IMCS hash table instance.
 */
IMCSHashTable* IMCSHashTable::GetInstance(void)
{
    Assert(m_imcs_hash_tbl != NULL);
    return m_imcs_hash_tbl;
}

/*
 * @Description: create or recreate the Singleton Instance of CU cache
 */
void IMCSHashTable::NewSingletonInstance(void)
{
    if (IsUnderPostmaster)
        return;

    m_imcs_hash_tbl = New(CurrentMemoryContext) IMCSHashTable;

    m_imcs_hash_tbl->m_imcs_context = AllocSetContextCreate(
        g_instance.instance_context,
        "imcstore context",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_imcs_hash_tbl->m_imcs_context);
    InitImcsHash();
    m_imcs_hash_tbl->m_imcs_lock = LWLockAssign(LWTRANCHE_IMCS_HASH_LOCK);
    pg_atomic_init_u64(&(m_imcs_hash_tbl->m_xlog_latest_lsn), 0);
    MemoryContextSwitchTo(oldcontext);
}

void IMCSHashTable::InitImcsHash()
{
    HASHCTL info;
    int hash_flags = HASH_CONTEXT | HASH_EXTERN_CONTEXT | HASH_ELEM | HASH_FUNCTION | HASH_PARTITION;
    errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "\0", "\0");

    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(IMCSDesc);
    info.hash = tag_hash;
    info.hcxt = m_imcs_hash_tbl->m_imcs_context;
    info.num_partitions = NUM_CACHE_BUFFER_PARTITIONS / 2;
    m_imcs_hash_tbl->m_imcs_hash = hash_create(
        "IMCSDesc Lookup Table", IMCSTORE_HASH_TAB_CAPACITY, &info, hash_flags);

    rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "\0", "\0");
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(IMCSRelnodeMapEntry);
    info.hash = tag_hash;
    info.hcxt = m_imcs_hash_tbl->m_imcs_context;
    info.num_partitions = NUM_CACHE_BUFFER_PARTITIONS / 2;
    m_imcs_hash_tbl->m_relfilenode_hash = hash_create(
        "IMCSDesc Relfilenode Map Table", IMCSTORE_HASH_TAB_CAPACITY, &info, hash_flags);
}

void IMCSHashTable::CreateImcsDesc(Relation rel, int2vector* imcsAttsNum, int imcsNatts, bool useShareMemroy)
{
    bool found = false;
    Oid relOid = RelationGetRelid(rel);
    Oid relfileNode = RelationGetRelFileNode(rel);
    /* No need to be checked for partitions, because it has been checked for the parent rel. */
    if (!OidIsValid(rel->parentId)) {
        CheckAndSetDBName();
    }
    LWLockAcquire(m_imcs_lock, LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_imcs_context);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_ENTER, &found);
    if (found) {
        ereport(ERROR, (errmodule(MOD_HTAP),
            (errmsg("existed imcstore for rel(%d).", RelationGetRelid(rel)))));
    } else {
        imcsDesc->Init(rel, imcsAttsNum, imcsNatts, useShareMemroy);
        pg_atomic_add_fetch_u32(&g_instance.imcstore_cxt.imcs_tbl_cnt, 1);
        IMCSRelnodeMapEntry* entry = (IMCSRelnodeMapEntry*)hash_search(
            m_relfilenode_hash, &relfileNode, HASH_ENTER, &found);
        entry->relOid = relOid;
    }
    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(m_imcs_lock);
}

IMCSDesc* IMCSHashTable::GetImcsDesc(Oid relOid)
{
    if (!HAVE_HTAP_TABLES || CHECK_IMCSTORE_CACHE_DOWN) {
        return NULL;
    }
    LWLockAcquire(m_imcs_lock, LW_SHARED);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, NULL);
    LWLockRelease(m_imcs_lock);
    return imcsDesc;
}

IMCSDesc* IMCSHashTable::GetImcsDescByRelNode(Oid relNode)
{
    bool found = false;
    IMCSDesc* imcsDesc = NULL;
    IMCSRelnodeMapEntry* entry = NULL;
    if (!HAVE_HTAP_TABLES || CHECK_IMCSTORE_CACHE_DOWN) {
        return NULL;
    }
    LWLockAcquire(m_imcs_lock, LW_SHARED);
    entry = (IMCSRelnodeMapEntry*)hash_search(m_relfilenode_hash, &relNode, HASH_FIND, &found);
    if (found) {
        imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &entry->relOid, HASH_FIND, NULL);
    }
    LWLockRelease(m_imcs_lock);
    return imcsDesc;
}

void IMCSHashTable::UpdateImcsStatus(Oid relOid, int imcsStatus)
{
    bool found = false;
    LWLockAcquire(m_imcs_lock, LW_EXCLUSIVE);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, &found);
    if (!found) {
        LWLockRelease(m_imcs_lock);
        return;
    }
    imcsDesc->imcsStatus = imcsStatus;
    LWLockRelease(m_imcs_lock);
}

void IMCSHashTable::DeleteImcsDesc(Oid relOid, RelFileNode* relNode)
{
    bool found = false;
    if (!OidIsValid(relOid)) {
        return;
    }
    LWLockAcquire(m_imcs_lock, LW_EXCLUSIVE);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, &found);
    if (!found) {
        LWLockRelease(m_imcs_lock);
        return;
    }

    PG_TRY();
    {
        if (imcsDesc->imcuDescContext != NULL) {
            /* drop rowgroup\cu\cudesc, no need to drop RowGroups for primary node */
            LWLockAcquire(imcsDesc->imcsDescLock, LW_EXCLUSIVE);
            Assert(relNode);
            uint64 curUsedShareMem = imcsDesc->populateInShareMem ? imcsDesc->shareMemPool->m_usedMemSize : 0;
            imcsDesc->DropRowGroups(relNode);
            if (imcsDesc->populateInShareMem && imcsDesc->shareMemPool != NULL) {
                SS_IMCU_CACHE->AdjustUsedShmAfterUnPopulate(curUsedShareMem);
                imcsDesc->shareMemPool->Destroy();
                imcsDesc->shareMemPool = NULL;
            }
            if (g_instance.attr.attr_memory.enable_borrow_memory && imcsDesc->borrowMemPool != NULL) {
                imcsDesc->borrowMemPool->Destroy();
                imcsDesc->borrowMemPool = NULL;
            }
            LWLockRelease(imcsDesc->imcsDescLock);
            MemoryContextDelete(imcsDesc->imcuDescContext);
        }
        if (!imcsDesc->isPartition) {
            ResetDBNameIfNeed();
        }
        (void)hash_search(m_relfilenode_hash, &imcsDesc->relfilenode, HASH_REMOVE, NULL);
        (void)hash_search(m_imcs_hash, &relOid, HASH_REMOVE, NULL);
        LWLockRelease(m_imcs_lock);
        pg_atomic_sub_fetch_u32(&g_instance.imcstore_cxt.imcs_tbl_cnt, 1);

    }
    PG_CATCH();
    {
        LWLockRelease(m_imcs_lock);
    }
    PG_END_TRY();
}

void IMCSHashTable::ClearImcsMem(Oid relOid, RelFileNode* relNode)
{
    bool found = false;
    LWLockAcquire(m_imcs_lock, LW_SHARED);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, &found);
    if (!found) {
        LWLockRelease(m_imcs_lock);
        return;
    }

    PG_TRY();
    {
        if (imcsDesc->imcsStatus == IMCS_POPULATE_ERROR && imcsDesc->imcuDescContext != NULL) {
            LWLockAcquire(imcsDesc->imcsDescLock, LW_EXCLUSIVE);
            imcsDesc->DropRowGroups(relNode);
            LWLockRelease(imcsDesc->imcsDescLock);
            MemoryContextDelete(imcsDesc->imcuDescContext);
            imcsDesc->imcuDescContext = NULL;
        }
        LWLockRelease(m_imcs_lock);
    }
    PG_CATCH();
    {
        LWLockRelease(m_imcs_lock);
    }
    PG_END_TRY();
}

void IMCSHashTable::UpdatePrimaryImcsStatus(Oid relOid, int imcsStatus)
{
    HASH_SEQ_STATUS hashSeq;
    IMCSDesc *imcsDesc = NULL;
    SyncRepStandbyData *syncStandbys;
    int numStandbys = SyncRepGetSyncStandbys(&syncStandbys);

    LWLockAcquire(m_imcs_lock, LW_EXCLUSIVE);
    hash_seq_init(&hashSeq, m_imcs_hash);
    while ((imcsDesc = (IMCSDesc*)hash_seq_search(&hashSeq)) != NULL) {
        if (numStandbys == 0 || (imcsDesc->relOid == relOid ||
            (imcsDesc->isPartition && imcsDesc->parentOid == relOid))) {
            imcsDesc->imcsStatus = imcsStatus;
        }
    }
    /* update sub partition imcs status */
    if (numStandbys > 0) {
        hash_seq_init(&hashSeq, m_imcs_hash);
        while ((imcsDesc = (IMCSDesc*)hash_seq_search(&hashSeq)) != NULL) {
            if (imcsDesc->isPartition && imcsDesc->imcsStatus != imcsStatus) {
                IMCSDesc* parentImcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &(imcsDesc->parentOid), HASH_FIND, NULL);
                imcsDesc->imcsStatus = (parentImcsDesc->imcsStatus == imcsStatus) ? imcsStatus : imcsDesc->imcsStatus;
            }
        }
    }
    LWLockRelease(m_imcs_lock);
}

bool IMCSHashTable::HasInitialImcsTable()
{
    Relation rel = NULL;
    HASH_SEQ_STATUS hashSeq;
    IMCSDesc *imcsDesc = NULL;

    LWLockAcquire(m_imcs_lock, LW_SHARED);
    hash_seq_init(&hashSeq, m_imcs_hash);
    while ((imcsDesc = (IMCSDesc*)hash_seq_search(&hashSeq)) != NULL) {
        if (imcsDesc->imcsStatus == IMCS_POPULATE_INITIAL) {
            LWLockRelease(m_imcs_lock);
            return true;
        }
    }
    LWLockRelease(m_imcs_lock);
    return false;
}

void IMCSHashTable::FreeAllShareMemPool()
{
    bool found = false;
    Oid* relOid = NULL;
    int ret = 0;
    HASH_SEQ_STATUS hashSeq;
    IMCSDesc *imcsDesc = NULL;

    hash_seq_init(&hashSeq, m_imcs_hash);
    while ((imcsDesc = (IMCSDesc*)hash_seq_search(&hashSeq)) != NULL) {
        if (imcsDesc->shareMemPool != NULL) {
            ret = imcsDesc->shareMemPool->DestoryShmChunk();
            if (ret != 0) {
                ereport(WARNING, (errmsg("Failed to unmap share memory, error code: %d", ret)));
                continue;
            }
            imcsDesc->borrowMemPool = NULL;
        }
    }
}

void IMCSHashTable::FreeAllBorrowMemPool()
{
    HASH_SEQ_STATUS hashSeq;
    IMCSDesc *imcsDesc = NULL;

    hash_seq_init(&hashSeq, m_imcs_hash);
    while ((imcsDesc = (IMCSDesc*)hash_seq_search(&hashSeq)) != NULL) {
        if (imcsDesc->borrowMemPool != NULL && !imcsDesc->isPartition) {
            imcsDesc->borrowMemPool->Destroy();
            imcsDesc->borrowMemPool = NULL;
        }
    }
}
