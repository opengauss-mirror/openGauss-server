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
 *
 * ss_imcucache_mgr.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/ss_imcucache_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SS_IMCUCACHEMGR_H
#define SS_IMCUCACHEMGR_H

#include "access/htap/imcucache_mgr.h"

#define SS_IMCU_CACHE (SSIMCUDataCacheMgr::GetInstance())

int64 SSIMCUCacheMgrCalcSize();

int64 SSIMCUDataCacheMgrNumLocks();

/*
* This class is to manage dss imcstore Data Cache.
*/
class SSIMCUDataCacheMgr : public DataCacheMgr {
public:
    static SSIMCUDataCacheMgr* GetInstance(void);
    static void NewSingletonInstance(void);
    static void BaseCacheCU(CU* srcCU, CU* slotCU);
    static bool CacheShmCU(CU* srcCU, CU* slotCU, CUDesc* cuDescPtr, IMCSDesc* imcsDesc);

public:
    void SetSPQNodeNumAndIdx();
    bool CheckRGOwnedByCurNode(uint32 rgid);
    void SaveCU(IMCSDesc* imcsDesc, RelFileNodeOld* rnode, int colId, CU* cuPtr, CUDesc* cuDescPtr);
    int64 GetCurrentMemSize() override;
    void GetCacheBlockInProgress(CacheSlotId_t *ioCacheBlock, CacheSlotId_t *uncompressCacheBlock) override;
    void SetCacheBlockInProgress(CacheSlotId_t ioCacheBlock, CacheSlotId_t uncompressCacheBlock) override;
    void ResetCacheBlockInProgress(bool resetUncompress) override;

    void PinDataBlock(CacheSlotId_t slotId);
    void SaveSSRemoteCU(Relation rel, int imcsColId, CU *cuPtr, CUDesc *cuDescPtr, IMCSDesc *imcsDesc);
    bool PreAllocateShmForRel(uint64 dataSize);
    void FixDifferenceAfterVacuum(uint64 begin, uint64 end, uint64 prealloc);
    void AdjustUsedShmAfterPopulate(Oid relOid);
    void AdjustUsedShmAfterUnPopulate(uint64 usedShmMemSize);

    /* for DSS imcstore and spq scan */
    int spqNodeNum;
    int curSpqIdx;
    NodeDefinition *nodesDefinition;

#ifndef ENABLE_UT
private:
#endif  // ENABLE_UT
    SSIMCUDataCacheMgr()
    {}
    ~SSIMCUDataCacheMgr()
    {}

    static SSIMCUDataCacheMgr* m_data_cache;
};
#endif /* SS_IMCUCACHEMGR_H */