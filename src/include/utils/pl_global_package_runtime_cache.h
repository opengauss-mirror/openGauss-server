/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2002-2007, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * global PLSQL's package runtime cache
 *
 * IDENTIFICATION
 *        src/include/utils/pl_global_package_runtime_cache.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef GLOBAL_PKG_RUNTIME_H
#define GLOBAL_PKG_RUNTIME_H

#include "utils/plpgsql.h"
#include "nodes/pg_list.h"

typedef struct GPRCHashCtl
{
    int lockId;
    HTAB *hashTbl;
    MemoryContext context;
} GPRCHashCtl;

typedef struct SessionPackageRuntime 
{
    List *runtimes;
    MemoryContext context;
    List* portalContext;
    List* portalData;
    List* funcValInfo;
    bool is_insert_gs_source;
} SessionPackageRuntime;

typedef struct GPRCValue
{
    uint64  sessionId;
    SessionPackageRuntime* sessPkgRuntime;
} GPRCValue;

class PLGlobalPackageRuntimeCache : public BaseObject
{
public:
    bool Add(uint64 sessionId, SessionPackageRuntime* runtime);
    SessionPackageRuntime* Fetch(uint64 sessionId);
    bool Remove(uint64 sessionId);

    static PLGlobalPackageRuntimeCache* Instance()
    {
        static PLGlobalPackageRuntimeCache runtimeCache;
        if (!inited) {
            runtimeCache.Init();
            inited = true;
        }
        return &runtimeCache;
    }

private:

    PLGlobalPackageRuntimeCache() {
    };
    
    ~PLGlobalPackageRuntimeCache() {
    };

    void Init();
    GPRCHashCtl *hashArray;
    static volatile bool inited;
};

List* CopyPortalDatas(SessionPackageRuntime *runtime);
List* CopyPortalContexts(List *portalContexts);
List* CopyFuncInfoDatas(SessionPackageRuntime *runtime);


#endif
