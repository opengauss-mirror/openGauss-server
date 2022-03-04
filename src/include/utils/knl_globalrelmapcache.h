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
 *
 * knl_globalrelmapcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalrelmapcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALRELMAPCACHE_H
#define KNL_GLOBALRELMAPCACHE_H

#include "postgres.h"
#include <pthread.h>
#include "utils/relmapper.h"

class GlobalRelMapCache : public BaseObject {
public:
    GlobalRelMapCache(Oid dbOid, bool shared);
    ~GlobalRelMapCache();

    /* phase 1 and phase 2 init function */
    void Init();
    void InitPhase2();

    /* relmap file handling function */
    void CopyInto(RelMapFile *rel_map);
    void UpdateBy(RelMapFile *rel_map);

private:
    Oid m_dbOid;
    volatile bool m_isInited;
    bool m_isShared;
    pthread_rwlock_t m_lock;
    RelMapFile m_relmap;
};

#endif