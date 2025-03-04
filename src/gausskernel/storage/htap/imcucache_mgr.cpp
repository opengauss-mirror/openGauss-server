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
 * imcucache_mgr.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/imcucache_mgr.cpp
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
#include "replication/syncrep.h"
#include "access/htap/borrow_mem_pool.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/imcucache_mgr.h"

#define BUILD_BUG_ON_CONDITION(condition) ((void)sizeof(char[1 - 2 * (condition)]))

static void CreateIMCUDir(const char* path);
static void CreateIMCUDirAndClearCUFiles();
static void RemoveAllCUFiles(const char* path, bool removeTop);
static bool IsSpecialDir(const char* path);
static void GetFilePath(const char* path, const char* fileName, char* filePath);

IMCUDataCacheMgr* IMCUDataCacheMgr::m_data_cache = NULL;

const char* IMCU_GLOBAL_DIR = "global/imcu";
const char* IMCU_DEFAULT_DIR = "base/imcu";

int64 IMCUCacheMgrCalcSize()
{
    return g_instance.attr.attr_memory.max_imcs_cache * 1024L;
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
int64 IMCUDataCacheMgrNumLocks()
{
    int64 cache_size = IMCUCacheMgrCalcSize();
    return cache_size / (BLCKSZ * MAX_IMCS_PAGES_ONE_CU) * MaxHeapAttributeNumber * IMCSTORE_DOUBLE;
}

void ParseInfosByCacheTag(CacheTag* cacheTag, RelFileNode* rnode, int* attid, int32* cuId)
{
    CUSlotTag cuSlotTag;
    errno_t rc = EOK;
    rc = memcpy_s(&cuSlotTag, MAX_CACHE_TAG_LEN, cacheTag->key, sizeof(CUSlotTag));
    securec_check(rc, "\0", "\0");
    rnode->spcNode = cuSlotTag.m_rnode.spcNode;
    rnode->dbNode = cuSlotTag.m_rnode.dbNode;
    rnode->relNode = cuSlotTag.m_rnode.relNode;
    *attid = cuSlotTag.m_colId;
    *cuId =  cuSlotTag.m_CUId;
}

static void CreateIMCUDir(const char* path)
{
    int ret = mkdir(path, S_IRWXU);
    if (ret < 0 && errno == EEXIST) {
        return;
    }

    if (ret < 0) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("%s: could not create imcu directory \"%s\"\n", "IMCUDataCacheMgr", path)));
    }
}

static void CreateIMCUDirAndClearCUFiles()
{
    CreateIMCUDir(IMCU_DEFAULT_DIR);
    CreateIMCUDir(IMCU_GLOBAL_DIR);
    RemoveAllCUFiles(IMCU_DEFAULT_DIR, false);
    RemoveAllCUFiles(IMCU_GLOBAL_DIR, false);
}

static void GetFilePath(const char* path, const char* fileName, char* filePath)
{
    int rc = 0;

    rc = strncpy_s(filePath, MAXPGPATH - strlen(filePath), path, strlen(path));
    securec_check_c(rc, "", "");

    if (filePath[strlen(path) - 1] != '/') {
        rc = strcat_s(filePath, MAXPGPATH, "/");
        securec_check_c(rc, "", "");
    }
    rc = strcat_s(filePath, MAXPGPATH, fileName);
    securec_check_c(rc, "", "");
}

static bool IsSpecialDir(const char* path)
{
    return strcmp(path, ".") == 0 || strcmp(path, "..") == 0;
}

static void RemoveAllCUFiles(const char* path, bool removeTop)
{
    DIR* dir = NULL;
    dirent* dir_info = NULL;
    char file_path[MAXPGPATH];
    struct stat statbuf;

    if (lstat(path, &statbuf) < 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("%s: could not stat cu file \"%s\" in RemoveAllCUFiles.\n", "IMCUDataCacheMgr", path)));
    }

    if (S_ISDIR(statbuf.st_mode)) {
        if ((dir = opendir(path)) == NULL) {
            return;
        }
        while ((dir_info = readdir(dir)) != NULL) {
            GetFilePath(path, dir_info->d_name, file_path);
            if (IsSpecialDir(dir_info->d_name)) {
                continue;
            }
            RemoveAllCUFiles(file_path, removeTop);
            if (removeTop) {
                rmdir(file_path);
            }
        }
        closedir(dir);
    } else {
        if (unlink(path) != 0)
                ereport(ERROR, (errcode_for_file_access(),
                    errmsg("%s: could not remove cu file \"%s\" in RemoveAllCUFiles.\n", "IMCUDataCacheMgr", path)));
        return;
    }
    if (removeTop) {
        rmdir(path);
    }
}

/*
 * @Description: get Singleton Instance of CU cache
 * @Return: CU cache instance
 */
IMCUDataCacheMgr* IMCUDataCacheMgr::GetInstance(void)
{
    Assert(m_data_cache != NULL);
    return m_data_cache;
}

/*
 * @Description: create or recreate the Singleton Instance of CU cache
 */
void IMCUDataCacheMgr::NewSingletonInstance(void)
{
    int64 cache_size = 0;
    /* only Postmaster has the privilege to create or recreate CU cache instance */
    if (IsUnderPostmaster)
        return;

    int condition = sizeof(DataSlotTagKey) != MAX_CACHE_TAG_LEN ? 1 : 0;
    BUILD_BUG_ON_CONDITION(condition);

    CreateIMCUDirAndClearCUFiles();

    if (m_data_cache == NULL) {
        /* create this instance at the first time */
        m_data_cache = New(CurrentMemoryContext) IMCUDataCacheMgr;
        m_data_cache->m_cache_mgr = New(CurrentMemoryContext) CacheMgr;
    } else {
        /* destroy all resources of its members */
        m_data_cache->m_cache_mgr->Destroy();
    }
    cache_size = IMCUCacheMgrCalcSize();
    m_data_cache->m_cstoreMaxSize = cache_size;

    m_data_cache->m_imcs_context = AllocSetContextCreate(g_instance.instance_context,
                                                         "imcs context",
                                                         ALLOCSET_SMALL_MINSIZE,
                                                         ALLOCSET_SMALL_INITSIZE,
                                                         ALLOCSET_DEFAULT_MAXSIZE,
                                                         SHARED_CONTEXT);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_data_cache->m_imcs_context);

    int64 imcs_block_size = BLCKSZ * MAX_IMCS_PAGES_ONE_CU;
    /* init or reset this instance */
    m_data_cache->m_cache_mgr->isImcs = true;
    m_data_cache->m_cache_mgr->Init(cache_size, imcs_block_size, MGR_CACHE_TYPE_DATA, sizeof(CU));
    ereport(LOG, (errmodule(MOD_CACHE), errmsg("set data cache  size(%ld)", cache_size)));

    HASHCTL info;
    int hash_flags = HASH_CONTEXT | HASH_EXTERN_CONTEXT | HASH_ELEM | HASH_FUNCTION | HASH_PARTITION;
    errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "\0", "\0");

    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(IMCSDesc);
    info.hash = tag_hash;
    info.hcxt = m_data_cache->m_imcs_context;
    info.num_partitions = NUM_CACHE_BUFFER_PARTITIONS / IMCSTORE_DOUBLE;
    m_data_cache->m_imcs_hash = hash_create("IMCSDesc Lookup Table", IMCSTORE_HASH_TAB_CAPACITY, &info, hash_flags);

    m_data_cache->m_xlog_latest_lsn = InvalidXLogRecPtr;
    m_data_cache->m_imcs_lock = LWLockAssign(LWTRANCHE_IMCS_HASH_LOCK);
    m_data_cache->m_is_promote = false;
    MemoryContextSwitchTo(oldcontext);
}

void IMCUDataCacheMgr::BaseCacheCU(CU* srcCU, CU* slotCU)
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
 
void IMCUDataCacheMgr::CacheCU(CU* srcCU, CU* slotCU)
{
    slotCU->m_srcBuf = srcCU->m_srcBuf;
    slotCU->m_nulls = srcCU->m_nulls;
    slotCU->m_srcData = srcCU->m_srcData;
    slotCU->m_offset = srcCU->m_offset;
    BaseCacheCU(srcCU, slotCU);
}
 
bool IMCUDataCacheMgr::CacheBorrowMemCU(CU* srcCU, CU* slotCU, CUDesc* cuDescPtr)
{
    errno_t rc = EOK;
    BaseCacheCU(srcCU, slotCU);

    if (srcCU->m_srcBuf != NULL) {
        char *buf = (char *)(u_sess->imcstore_ctx.pinnedBorrowMemPool->Allocate(srcCU->m_srcBufSize));
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
        if (!slotCU->HasNullValue()) {
            slotCU->FormValuesOffset<false>(cuDescPtr->row_count);
        } else {
            slotCU->FormValuesOffset<true>(cuDescPtr->row_count);
        }
    }
    return true;
}

void IMCUDataCacheMgr::SaveCU(IMCSDesc* imcsDesc, RelFileNodeOld* rnode, int colId, CU* cuPtr, CUDesc* cuDescPtr)
{
    Assert(colId >= 0 && colId <= MaxHeapAttributeNumber);
    Assert(imcsDesc != NULL && rnode != NULL && cuPtr != NULL && cuDescPtr != NULL);
    /* null cu no need to save */
    if (cuDescPtr->IsNullCU()) {
        cuPtr->FreeSrcBuf();
        return;
    }
    DataSlotTag slotTag = InitCUSlotTag(rnode, colId, cuDescPtr->cu_id, cuDescPtr->cu_pointer);

RETRY_RESERVE_DATABLOCK:
    bool hasFound = false;
    CacheSlotId_t slotId = ReserveDataBlock(&slotTag, cuDescPtr->cu_size, hasFound);
    CU* slotCU = GetCUBuf(slotId);
    if (IsBorrowSlotId(slotId)) {
        if (CacheBorrowMemCU(cuPtr, slotCU, cuDescPtr)) {
            cuPtr->Destroy();
        } else {
            UnPinDataBlock(slotId);
            DataBlockCompleteIO(slotId);
            InvalidateCU(rnode, colId, cuDescPtr->cu_id, cuDescPtr->cu_pointer);
            goto RETRY_RESERVE_DATABLOCK;
        }
    } else {
        CacheCU(cuPtr, slotCU);
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
int64 IMCUDataCacheMgr::GetCurrentMemSize()
{
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) {
        return 0;
    }
    return m_cache_mgr->GetCurrentMemSize();
}


/*
 * @Description:  get global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void IMCUDataCacheMgr::GetCacheBlockInProgress(CacheSlotId_t *ioCacheBlock, CacheSlotId_t *uncompressCacheBlock)
{
    Assert(ioCacheBlock || uncompressCacheBlock);
    if (ioCacheBlock) {
        *ioCacheBlock = t_thrd.storage_cxt.IMCSCacheBlockInProgressIO;
    }
    if (uncompressCacheBlock) {
        *uncompressCacheBlock = t_thrd.storage_cxt.IMCSCacheBlockInProgressUncompress;
    }
}

/*
 * @Description:  set global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void IMCUDataCacheMgr::SetCacheBlockInProgress(CacheSlotId_t ioCacheBlock, CacheSlotId_t uncompressCacheBlock)
{
    if (IsValidCacheSlotID(ioCacheBlock)) {
        t_thrd.storage_cxt.IMCSCacheBlockInProgressIO = ioCacheBlock;
    }
    if (IsValidCacheSlotID(uncompressCacheBlock)) {
        t_thrd.storage_cxt.IMCSCacheBlockInProgressUncompress = uncompressCacheBlock;
    }
}

/*
 * @Description:  reset global block slot in progress
 * @IN ioCacheBlock: global cache block slot
 * @IN uncompressCacheBlock: global uncompress block slot
 */
void IMCUDataCacheMgr::ResetCacheBlockInProgress(bool resetUncompress)
{
    t_thrd.storage_cxt.IMCSCacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    if (resetUncompress) {
        t_thrd.storage_cxt.IMCSCacheBlockInProgressUncompress = CACHE_BLOCK_INVALID_IDX;
    }
}

void IMCUDataCacheMgr::CreateImcsDesc(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    bool found = false;
    Oid relOid = RelationGetRelid(rel);
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
        imcsDesc->Init(rel, imcsAttsNum, imcsNatts);
        pg_atomic_add_fetch_u32(&g_instance.imcstore_cxt.imcs_tbl_cnt, 1);
    }
    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(m_imcs_lock);
}

IMCSDesc* IMCUDataCacheMgr::GetImcsDesc(Oid relOid)
{
    if (!HAVE_HTAP_TABLES || CHECK_IMCSTORE_CACHE_DOWN) {
        return NULL;
    }

    LWLockAcquire(m_imcs_lock, LW_SHARED);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, NULL);
    LWLockRelease(m_imcs_lock);
    return imcsDesc;
}

void IMCUDataCacheMgr::UpdateImcsStatus(Oid relOid, int imcsStatus)
{
    LWLockAcquire(m_imcs_lock, LW_EXCLUSIVE);
    IMCSDesc* imcsDesc = (IMCSDesc*)hash_search(m_imcs_hash, &relOid, HASH_FIND, NULL);
    imcsDesc->imcsStatus = imcsStatus;
    LWLockRelease(m_imcs_lock);
}

void IMCUDataCacheMgr::DeleteImcsDesc(Oid relOid, RelFileNode* relNode)
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
            if (g_instance.attr.attr_memory.enable_borrow_memory && imcsDesc->borrowMemPool != NULL) {
                imcsDesc->borrowMemPool->Destroy();
                imcsDesc->borrowMemPool = NULL;
            }
            imcsDesc->DropRowGroups(relNode);
            LWLockRelease(imcsDesc->imcsDescLock);
            MemoryContextDelete(imcsDesc->imcuDescContext);
        }
        if (!imcsDesc->isPartition) {
            ResetDBNameIfNeed();
        }
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

void IMCUDataCacheMgr::ClearImcsMem(Oid relOid, RelFileNode* relNode)
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

void IMCUDataCacheMgr::UpdatePrimaryImcsStatus(Oid relOid, int imcsStatus)
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

bool IMCUDataCacheMgr::HasInitialImcsTable()
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

void IMCUDataCacheMgr::ResetInstance(bool isPromote)
{
    ereport(WARNING, (errmsg("IMCStore data cache manager reset.")));
    if (g_instance.attr.attr_memory.enable_borrow_memory) {
        m_data_cache->FreeAllBorrowMemPool();
    }

    if (m_data_cache != NULL) {
        HeapMemResetHash(m_data_cache->m_imcs_hash, "IMCSDesc Lookup Table");
        m_data_cache->m_cache_mgr->FreeImcstoreCache();
    }

    m_data_cache->m_is_promote = isPromote;
    CreateIMCUDirAndClearCUFiles();
    ereport(WARNING, (errmsg("IMCStore data cache manager reset successfully.")));
}

bool IMCUDataCacheMgr::IsBorrowSlotId(CacheSlotId_t slotId)
{
    return m_cache_mgr->IsBorrowSlotId(slotId);
}

int64 IMCUDataCacheMgr::GetCurrBorrowMemSize()
{
    return m_cache_mgr->GetCurrBorrowMemSize();
}

void IMCUDataCacheMgr::FreeAllBorrowMemPool()
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