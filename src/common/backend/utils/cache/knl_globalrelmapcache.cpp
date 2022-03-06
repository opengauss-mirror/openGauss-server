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

#include "executor/executor.h"
#include "knl/knl_session.h"
#include "knl/knl_thread.h"
#include "utils/knl_globalrelmapcache.h"
#include "utils/relmapper.h"
#include "postgres.h"
#include "securec.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/fd.h"

GlobalRelMapCache::GlobalRelMapCache(Oid dbOid, bool shared)
{
    m_dbOid = dbOid;
    m_isInited = false;
    m_isShared = shared;
}

GlobalRelMapCache::~GlobalRelMapCache()
{
    m_isInited = false;
}

/* RelMapCache initialization phase 1 */
void GlobalRelMapCache::Init()
{
    errno_t rc = memset_s(&m_relmap, sizeof(RelMapFile), 0, sizeof(RelMapFile));
    securec_check(rc, "\0", "\0");

    PthreadRwLockInit(&m_lock, NULL);
}

/* RelMapCache initialization phase 2 */
void GlobalRelMapCache::InitPhase2()
{
    if (m_isInited) {
        return;
    }

    LWLockAcquire(RelationMappingLock, LW_SHARED);

    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_lock);
    if (!m_isInited) {
        load_relmap_file(m_isShared, &m_relmap);
        pg_memory_barrier();

        /* Mark relmapcache as initalized once relmap is loaded */
        m_isInited = true;
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_lock);

    LWLockRelease(RelationMappingLock);
}

void GlobalRelMapCache::UpdateBy(RelMapFile *rel_map)
{
    /* here we have write lock of RelationMappingLock */
    if (!m_isInited) {
        return;
    }

    /*
     * dont lock relmap by RelationMappingLock, the only call happened after
     * lwlockaquire(relmap), ?? need refine
     */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_lock);

    /* we dont care what magic the rel_map is */
    errno_t rc = memcpy_s(&m_relmap, sizeof(RelMapFile), rel_map, sizeof(RelMapFile));
    securec_check(rc, "\0", "\0");

    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_lock);
}

void GlobalRelMapCache::CopyInto(RelMapFile *dest_relmap)
{
    Assert(m_isInited);

    /* obtain a lock and do copy */
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_lock);

    /* crc and pad needn't copy.
     * should make sure the dest_relmap are all zero */
    errno_t rc = memcpy_s(dest_relmap, sizeof(RelMapFile), &m_relmap,
        offsetof(RelMapFile, mappings) + m_relmap.num_mappings * sizeof(RelMapping));
    securec_check(rc, "\0", "\0");

    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_lock);
}