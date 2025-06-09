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
 *
 * imcucache_mgr.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/imcucache_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCUCACHEMGR_H
#define IMCUCACHEMGR_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/relfilenode.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/hsearch.h"
#include "storage/lock/lwlock.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "storage/cu.h"
#include "storage/cache_mgr.h"
#include "storage/custorage.h"
#include "storage/cucache_mgr.h"

#define BUILD_BUG_ON_CONDITION(condition) ((void)sizeof(char[1 - 2 * (condition)]))
#define IMCSTORE_DOUBLE 2
#define WAIT_ROWGROUP_UNREFERENCE 100
#define IMCU_CACHE (IMCUDataCacheMgr::GetInstance())

extern const char* IMCU_GLOBAL_DIR;
extern const char* IMCU_DEFAULT_DIR;

int64 IMCUCacheMgrCalcSize();

int64 IMCUDataCacheMgrNumLocks();

extern void ParseInfosByCacheTag(CacheTag* cacheTag, RelFileNode* rnode, int* attid, int32* cuId);

/*
 * This class is to manage Data Cache.
 * For integer IMCU, we should store compressed data.
 * For string IMCU, because the speed of decompressing data is low,
 * so store source data.
 */
class IMCUDataCacheMgr : public DataCacheMgr {
public: // static
    static IMCUDataCacheMgr* GetInstance(void);
    static void NewSingletonInstance(void);
    static void ResetInstance(bool isPromote = false);
    static void BaseCacheCU(CU* srcCU, CU* slotCU);
    static void CacheCU(CU* srcCU, CU* slotCU);
    static bool CacheBorrowMemCU(CU* srcCU, CU* slotCU, CUDesc* cuDescPtr);

public:
    void SaveCU(IMCSDesc* imcsDesc, RelFileNodeOld* rnode, int colId, CU* cuPtr, CUDesc* cuDescPtr);

    int64 GetCurrentMemSize() override;
    void GetCacheBlockInProgress(CacheSlotId_t *ioCacheBlock, CacheSlotId_t *uncompressCacheBlock) override;
    void SetCacheBlockInProgress(CacheSlotId_t ioCacheBlock, CacheSlotId_t uncompressCacheBlock) override;
    void ResetCacheBlockInProgress(bool resetUncompress) override;

    bool IsBorrowSlotId(CacheSlotId_t slotId);
    int64 GetCurrBorrowMemSize();

    bool m_is_promote;

#ifndef ENABLE_UT
private:
#endif  // ENABLE_UT
    IMCUDataCacheMgr()
    {}
    ~IMCUDataCacheMgr()
    {}

    static IMCUDataCacheMgr* m_data_cache;
};

#endif /* IMCUCACHEMGR_H */