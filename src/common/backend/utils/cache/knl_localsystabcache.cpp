/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 */

#include "utils/knl_globalsysdbcache.h"
#include "utils/knl_localsystabcache.h"
#include "utils/knl_localsystupcache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "storage/ipc.h"
#include "utils/catcache.h"

void LocalSysTabCache::CreateObject()
{
    local_systupcaches = (LocalSysTupCache **)palloc0(sizeof(LocalSysTupCache *) * SysCacheSize);
    for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
        local_systupcaches[cache_id] = New(CurrentMemoryContext) LocalSysTupCache(cache_id);
    }
}

void LocalSysTabCache::CreateCatBuckets()
{
    for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
        local_systupcaches[cache_id]->CreateCatBucket();
    }
}