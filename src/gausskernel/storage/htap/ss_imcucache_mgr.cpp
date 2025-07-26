/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ss_imcucache_mgr.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/ss_imcucache_mgr.cpp
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
#include "storage/cu.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/borrow_mem_pool.h"
#include "access/htap/share_mem_pool.h"
#include "access/htap/imcs_hash_table.h"
#include "access/htap/ss_imcucache_mgr.h"

SSIMCUDataCacheMgr* SSIMCUDataCacheMgr::m_data_cache = NULL;

int64 SSIMCUCacheMgrCalcSize()
{
    return g_instance.attr.attr_memory.ss_max_imcs_cache * 1024L;
}

/*
 * @Description: IMCUDataCacheMgrNumLocks
 * Returns the number of LW locks required by the IMCUDataCacheMgr instance.
 * This function is called by NumLWLocks() to calculate the required memory
 * for all the LW Locks.  Since this must be done prior to initializing the
 * instance of the IMCUDataCacheMgr class, the function cannot be defined
 * as a method of the class.
 * @Return: lock number
 */
int64 SSIMCUDataCacheMgrNumLocks()
{
    int64 cache_size = SSIMCUCacheMgrCalcSize();
    return cache_size / (BLCKSZ * MAX_IMCS_PAGES_ONE_CU) * MaxHeapAttributeNumber * IMCSTORE_DOUBLE;
}

/*
 * @Description: get Singleton Instance of CU cache
 * @Return: CU cache instance
 */
SSIMCUDataCacheMgr* SSIMCUDataCacheMgr::GetInstance(void)
{
    Assert(m_data_cache != NULL);
    return m_data_cache;
}

/*
 * @Description: create or recreate the Singleton Instance of CU cache
 */
void SSIMCUDataCacheMgr::NewSingletonInstance(void)
{
    int64 cache_size = 0;
    /* only Postmaster has the privilege to create or recreate CU cache instance */
    if (IsUnderPostmaster || !ENABLE_DSS)
        return;

    int condition = sizeof(DataSlotTagKey) != MAX_CACHE_TAG_LEN ? 1 : 0;
    BUILD_BUG_ON_CONDITION(condition);

    if (m_data_cache == NULL) {
        /* create this instance at the first time */
        m_data_cache = New(CurrentMemoryContext) SSIMCUDataCacheMgr;
        m_data_cache->m_cache_mgr = New(CurrentMemoryContext) CacheMgr;
    } else {
        /* destroy all resources of its members */
        m_data_cache->m_cache_mgr->Destroy();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(IMCS_HASH_TABLE->m_imcs_context);
    cache_size = SSIMCUCacheMgrCalcSize();
    m_data_cache->m_cstoreMaxSize = cache_size;
    int64 imcs_block_size = BLCKSZ * MAX_IMCS_PAGES_ONE_CU;
    /* init or reset this instance */
    m_data_cache->m_cache_mgr->storageType = SS_IMCSTORAGE;
    m_data_cache->m_cache_mgr->Init(cache_size, imcs_block_size, MGR_CACHE_TYPE_DATA, sizeof(CU));
    ereport(LOG, (errmodule(MOD_CACHE), errmsg("set ss imcstore data cache size(%ld)", cache_size)));

    m_data_cache->spqNodeNum = 0;
    m_data_cache->curSpqIdx = 0;
    m_data_cache->nodesDefinition = nullptr;
    m_data_cache->SetSPQNodeNumAndIdx();
    MemoryContextSwitchTo(oldcontext);
}

void SSIMCUDataCacheMgr::SetSPQNodeNumAndIdx()
{
    errno_t rc = 0;
    char *rawname = nullptr;
    List *nodelist = nullptr;
    NodeDefinition *node;
    int nodeIdx = 0;
    const char *curNodeName = GetConfigOption("pgxc_node_name", false, false);
    const char *liststring = g_instance.attr.attr_sql.ss_htap_cluster_map;
    if (liststring == nullptr || liststring[0] == '\0') {
        ereport(WARNING, (errmodule(MOD_HTAP),
            errmsg("ss_htap_cluster_map not set, can not enable dss imcstore.")));
        return;
    }
    rawname = pstrdup(liststring);
    (void)SplitIdentifierString(rawname, ',', &nodelist, false);
    spqNodeNum = list_length(nodelist);
    nodesDefinition = (NodeDefinition *)palloc0(sizeof(NodeDefinition) * (spqNodeNum));
    foreach_cell(lnode, nodelist) {
        node = &nodesDefinition[nodeIdx];
        List *itemlist;
        char *name, *ip, *port;
        char *nodestring = pstrdup((char *)lfirst(lnode));
        (void)SplitIdentifierString(nodestring, '|', &itemlist, false);
        name = (char *)list_nth(itemlist, 0);
        ip = (char *)list_nth(itemlist, 1);
        port = (char *)list_nth(itemlist, 2);
        if (strcmp(curNodeName, name) == 0) {
            curSpqIdx = nodeIdx;
        }
        rc = strncpy_s(node->nodename.data, NAMEDATALEN, name, NAMEDATALEN);
        securec_check_c(rc, "\0", "\0");
        rc = strncpy_s(node->nodehost.data, NAMEDATALEN, ip, NAMEDATALEN);
        securec_check_c(rc, "\0", "\0");
        rc = strncpy_s(node->nodehost1.data, NAMEDATALEN, ip, NAMEDATALEN);
        securec_check_c(rc, "\0", "\0");
        node->nodeport = (int)strtol(port, NULL, 10);
        nodeIdx++;
        pfree(nodestring);
        list_free(itemlist);
    }

    pfree(rawname);
}

bool SSIMCUDataCacheMgr::CheckRGOwnedByCurNode(uint32 rgid)
{
    return (rgid % SS_IMCU_CACHE->spqNodeNum) == SS_IMCU_CACHE->curSpqIdx;
}

void SSIMCUDataCacheMgr::BaseCacheCU(CU* srcCU, CU* slotCU)
{
    slotCU->m_compressedBuf = NULL;
    slotCU->m_tmpinfo = NULL;
    slotCU->m_compressedLoadBuf = NULL;
    slotCU->m_head_padding_size = srcCU->m_head_padding_size;
    slotCU->m_offsetSize = srcCU->m_offsetSize;
    slotCU->m_srcBufSize = srcCU->m_srcBufSize;
    slotCU->m_srcDataSize = srcCU->m_srcDataSize;
    slotCU->m_compressedBufSize = srcCU->m_compressedBufSize;
    slotCU->m_cuSize = srcCU->m_cuSize;
    slotCU->m_cuSizeExcludePadding = srcCU->m_cuSizeExcludePadding;
    slotCU->m_crc = srcCU->m_crc;
    slotCU->m_magic = srcCU->m_magic;
    slotCU->m_eachValSize = srcCU->m_eachValSize;
    slotCU->m_typeMode = srcCU->m_typeMode;
    slotCU->m_bpNullRawSize = srcCU->m_bpNullRawSize;
    slotCU->m_bpNullCompressedSize = srcCU->m_bpNullCompressedSize;
    slotCU->m_infoMode = srcCU->m_infoMode;
    slotCU->m_atttypid = srcCU->m_atttypid;
    slotCU->m_adio_error = srcCU->m_adio_error;
    slotCU->m_cache_compressed = srcCU->m_cache_compressed;
    slotCU->m_inCUCache = true;
    slotCU->m_numericIntLike = srcCU->m_numericIntLike;
}

bool SSIMCUDataCacheMgr::CacheShmCU(CU* srcCU, CU* slotCU, CUDesc* cuDescPtr, IMCSDesc* imcsDesc)
{
    errno_t rc = EOK;
    IMCUDataCacheMgr::BaseCacheCU(srcCU, slotCU);

    if (srcCU->m_srcBufSize != 0) {
        if (!SS_IMCU_CACHE->PreAllocateShmForRel(srcCU->m_srcBufSize)) {
            ereport(ERROR, (errmsg("can't alloc share memory, share memory is exhausted.")));;
        }

        char *buf = (char *)(imcsDesc->shareMemPool->AllocateCUMem(
            srcCU->m_srcBufSize, cuDescPtr->slot_id, &slotCU->shmCUOffset, &slotCU->shmChunkNumber));
        if (buf == nullptr) {
            return false;
        }
        rc = memcpy_s(buf, srcCU->m_srcBufSize, srcCU->m_srcBuf, srcCU->m_srcBufSize);
        securec_check(rc, "\0", "\0");
        slotCU->m_srcBuf = buf;
        slotCU->m_srcData = buf + srcCU->m_bpNullRawSize;
        if (srcCU->m_bpNullRawSize != 0) {
            slotCU->m_nulls = (unsigned char*)slotCU->m_srcBuf;
        }
    }

    if (!slotCU->HasNullValue()) {
        slotCU->FormValuesOffset<false>(cuDescPtr->row_count);
    } else {
        slotCU->FormValuesOffset<true>(cuDescPtr->row_count);
    }
    return true;
}

void SSIMCUDataCacheMgr::SaveCU(IMCSDesc* imcsDesc, RelFileNodeOld* rnode, int colId, CU* cuPtr, CUDesc* cuDescPtr)
{
    Assert(colId >= 0 && colId <= MaxHeapAttributeNumber);
    Assert(imcsDesc != NULL && rnode != NULL && cuPtr != NULL && cuDescPtr != NULL && ENABLE_DSS);
    /* null cu no need to save */
    if (cuDescPtr->IsNullCU()) {
        cuPtr->FreeSrcBuf();
        return;
    }
    DataSlotTag slotTag = InitCUSlotTag(rnode, colId, cuDescPtr->cu_id, cuDescPtr->cu_pointer);
    bool hasFound = false;
    CacheSlotId_t slotId = ReserveDataBlock(&slotTag, cuDescPtr->cu_size, hasFound);
    CU* slotCU = GetCUBuf(slotId);
    cuDescPtr->slot_id = slotId;
    if (CacheShmCU(cuPtr, slotCU, cuDescPtr, imcsDesc)) {
        cuPtr->Destroy();
    }
    slotCU->imcsDesc = imcsDesc;
    UnPinDataBlock(slotId);
    DataBlockCompleteIO(slotId);
    pg_atomic_add_fetch_u64(&imcsDesc->cuSizeInMem, (uint64)cuDescPtr->cu_size);
    pg_atomic_add_fetch_u64(&imcsDesc->cuNumsInMem, 1);
}

/*
 * @Description: get data cache manage current memory cache used size
 * @Return: cache used size
 */
int64 SSIMCUDataCacheMgr::GetCurrentMemSize()
{
    return pg_atomic_read_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used);
}

/*
 * @Description:  get global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void SSIMCUDataCacheMgr::GetCacheBlockInProgress(CacheSlotId_t *ioCacheBlock, CacheSlotId_t *uncompressCacheBlock)
{
    Assert(ioCacheBlock || uncompressCacheBlock);
    if (ioCacheBlock) {
        *ioCacheBlock = t_thrd.storage_cxt.SSIMCSCacheBlockInProgressIO;
    }
    if (uncompressCacheBlock) {
        *uncompressCacheBlock = t_thrd.storage_cxt.SSIMCSCacheBlockInProgressUncompress;
    }
}

/*
 * @Description:  set global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void SSIMCUDataCacheMgr::SetCacheBlockInProgress(CacheSlotId_t ioCacheBlock, CacheSlotId_t uncompressCacheBlock)
{
    if (IsValidCacheSlotID(ioCacheBlock)) {
        t_thrd.storage_cxt.SSIMCSCacheBlockInProgressIO = ioCacheBlock;
    }
    if (IsValidCacheSlotID(uncompressCacheBlock)) {
        t_thrd.storage_cxt.SSIMCSCacheBlockInProgressUncompress = uncompressCacheBlock;
    }
}

/*
 * @Description:  reset global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void SSIMCUDataCacheMgr::ResetCacheBlockInProgress(bool resetUncompress)
{
    t_thrd.storage_cxt.SSIMCSCacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    if (resetUncompress) {
        t_thrd.storage_cxt.SSIMCSCacheBlockInProgressUncompress = CACHE_BLOCK_INVALID_IDX;
    }
}

void SSIMCUDataCacheMgr::PinDataBlock(CacheSlotId_t slotId)
{
    m_cache_mgr->PinCacheBlock(slotId);
}

void SSIMCUDataCacheMgr::SaveSSRemoteCU(Relation rel, int imcsColId, CU *cuPtr, CUDesc *cuDescPtr, IMCSDesc *imcsDesc)
{
    if (cuDescPtr->IsNullCU()) {
        return;
    }

    DataSlotTag slotTag = InitCUSlotTag(
        (RelFileNodeOld*)&rel->rd_node, imcsColId, cuDescPtr->cu_id, cuDescPtr->cu_pointer);
    bool hasFound = false;
    CacheSlotId_t slotId = ReserveDataBlock(&slotTag, cuDescPtr->cu_size, hasFound);
    CU* slotCU = GetCUBuf(slotId);
    BaseCacheCU(cuPtr, slotCU);
    slotCU->shmChunkNumber = cuPtr->shmChunkNumber;
    slotCU->shmCUOffset = cuPtr->shmCUOffset;
    slotCU->imcsDesc = imcsDesc;

    char* buf = (char*)imcsDesc->shareMemPool->GetCUBuf(cuPtr->shmChunkNumber, cuPtr->shmCUOffset);
    slotCU->m_srcBuf = buf;
    slotCU->m_srcData = buf + slotCU->m_bpNullRawSize;
    if (slotCU->m_bpNullRawSize != 0) {
        slotCU->m_nulls = (unsigned char*)slotCU->m_srcBuf;
    }

    if (!slotCU->HasNullValue()) {
        slotCU->FormValuesOffset<false>(cuDescPtr->row_count);
    } else {
        slotCU->FormValuesOffset<true>(cuDescPtr->row_count);
    }
    cuDescPtr->slot_id = slotId;
    UnPinDataBlock(slotId);
    DataBlockCompleteIO(slotId);
    pg_atomic_add_fetch_u64(&imcsDesc->cuSizeInMem, (uint64)cuDescPtr->cu_size);
    pg_atomic_add_fetch_u64(&imcsDesc->cuNumsInMem, 1);
    cuPtr->Reset();
}

bool SSIMCUDataCacheMgr::PreAllocateShmForRel(uint64 dataSize)
{
    bool isAllocated = false;
    uint64 usedMem = 0;
    pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
    usedMem = pg_atomic_read_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used);
    if (usedMem + dataSize < m_cstoreMaxSize) {
        pg_atomic_add_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, dataSize);
        isAllocated = true;
    }
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
    return isAllocated;
}

void SSIMCUDataCacheMgr::FixDifferenceAfterVacuum(uint64 begin, uint64 end, uint64 prealloc)
{
    pg_atomic_sub_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, prealloc);
    pg_atomic_sub_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, begin);
    pg_atomic_add_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, end);
}

void SSIMCUDataCacheMgr::AdjustUsedShmAfterPopulate(Oid relOid)
{
    IMCSDesc* imcsDesc = NULL;
    uint64 preUsedMem = 0;
    uint64 actualUsedMem = 0;

    imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(relOid);
    pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
    preUsedMem = 0;
    actualUsedMem = imcsDesc->shareMemPool->m_usedMemSize;
    if (preUsedMem > actualUsedMem) {
        pg_atomic_sub_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, preUsedMem - actualUsedMem);
    } else {
        pg_atomic_add_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, actualUsedMem - preUsedMem);
    }
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
}

void SSIMCUDataCacheMgr::AdjustUsedShmAfterUnPopulate(uint64 usedShmMemSize)
{
    pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
    pg_atomic_sub_fetch_u64(&g_instance.imcstore_cxt.imcs_shm_cur_used, usedShmMemSize);
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
}
