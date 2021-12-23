/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pl_global_package_runtime_cache.cpp
 *    global PLSQL's package runtime cache
 *
 * IDENTIFICATION
 * src/common/pl/plpgsql/src/pl_global_package_runtime_cache.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "nodes/pg_list.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/plpgsql.h"
#include "utils/pl_global_package_runtime_cache.h"

static int getPartition(uint32 hashCode)
{
    return hashCode % NUM_GPRC_PARTITIONS;
}

static uint32 gprcHash(const void* key, Size keysize)
{
    return DatumGetUInt32(DirectFunctionCall1(hashint8, Int64GetDatumFast(*((uint64*)key))));
}

static int gprcMatch(const void *left, const void *right, Size keysize)
{
    uint64 *leftItem = (uint64*)left;
    uint64 *rightItem = (uint64*)right;
    return *leftItem == *rightItem;
}

PLGlobalPackageRuntimeCache::PLGlobalPackageRuntimeCache()
{
    init();
}

PLGlobalPackageRuntimeCache::~PLGlobalPackageRuntimeCache()
{
}

void PLGlobalPackageRuntimeCache::init()
{
    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(uint64);
    ctl.entrysize = sizeof(GPRCValue);
    ctl.hash = gprcHash;
    ctl.match = gprcMatch;
    const int hashSize = 1024;

    int flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_EXTERN_CONTEXT | HASH_NOEXCEPT;
    hashArray = (GPRCHashCtl*)
        MemoryContextAllocZero(GLOBAL_PLANCACHE_MEMCONTEXT, sizeof(GPRCHashCtl) * NUM_GPRC_PARTITIONS);
    for (uint32 i = 0; i < NUM_GPRC_PARTITIONS; i++) {
        hashArray[i].lockId = FirstGPRCMappingLock + i;
        hashArray[i].context = AllocSetContextCreate(
            GLOBAL_PLANCACHE_MEMCONTEXT,
            "GPRC_Bucket_Context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
        ctl.hcxt = hashArray[i].context;
        hashArray[i].hashTbl = hash_create("Global_Package_Runtime_hashtable", hashSize, &ctl, flags);
    }
}

static List* copyPackageRuntimeStates(SessionPackageRuntime *runtime)
{
    ListCell* cell = NULL;
    List* result = NULL;
    foreach(cell, runtime->runtimes) {
        PackageRuntimeState* runtimeState = (PackageRuntimeState*)lfirst(cell);
        PackageRuntimeState* newRuntimeState = (PackageRuntimeState*)palloc0(sizeof(PackageRuntimeState));
        newRuntimeState->datums = (PLpgSQL_datum**)palloc0(sizeof(PLpgSQL_datum*) * runtimeState->size);
        newRuntimeState->packageId = runtimeState->packageId;
        newRuntimeState->size = runtimeState->size;
        for (int i = 0; i < runtimeState->size; i++) {
            PLpgSQL_datum *datum = runtimeState->datums[i];
            if (datum == NULL) {
                continue;
            }
            PLpgSQL_datum *newDatum = deepCopyPlpgsqlDatum(datum);
            newRuntimeState->datums[i] = newDatum;
        }
        result = lappend(result, newRuntimeState);
    }
    return result;
}

static SessionPackageRuntime* copySessionPackageRuntime(SessionPackageRuntime *runtime)
{
    MemoryContext pkgRuntimeCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SessionPackageRuntime",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(pkgRuntimeCtx);
    SessionPackageRuntime *sessPkgRuntime = (SessionPackageRuntime*)palloc0(sizeof(SessionPackageRuntime));
    sessPkgRuntime->runtimes = copyPackageRuntimeStates(runtime);
    sessPkgRuntime->context = pkgRuntimeCtx;
    MemoryContextSwitchTo(oldCtx);
    return sessPkgRuntime;
}

bool PLGlobalPackageRuntimeCache::add(uint64 sessionId, SessionPackageRuntime* runtime)
{
    if (runtime == NULL) {
        ereport(WARNING,
            (errmsg("SessionPackageRuntime can't be null.")));
        return false;
    }

    ereport(DEBUG3, (errmodule(MOD_PLSQL), errcode(ERRCODE_LOG),
            errmsg("PLGlobalPackageRuntimeCache LOG: current session id: %lu , add session id: %lu",
                IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid, sessionId)));

    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl hashTblCtl = hashArray[partitionIndex];

    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(hashTblCtl.context);

    bool found = false;
    GPRCValue *entry = (GPRCValue*)hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_FIND, &found);
    if (found) {
        MemoryContextDelete(entry->sessPkgRuntime->context);
        entry->sessPkgRuntime = NULL;
        hash_search_with_hash_value(
            hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_REMOVE, &found);
    }

    entry = (GPRCValue*)hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_ENTER, &found);
    if (entry == NULL) {
        ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("palloc hash element GPRCValue failed"),
            errdetail("failed to add hash element for PLGlobalPackageRuntimeCache"),
            errcause("out of memory"),
            erraction("set more memory")));
    }
    entry->sessPkgRuntime = copySessionPackageRuntime(runtime);

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
    return true;
}

SessionPackageRuntime* PLGlobalPackageRuntimeCache::fetch(uint64 sessionId)
{
    ereport(DEBUG3, (errmodule(MOD_PLSQL), errcode(ERRCODE_LOG),
            errmsg("PLGlobalPackageRuntimeCache LOG: current session id: %lu , fetch session id: %lu",
                IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid, sessionId)));

    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl hashTblCtl = hashArray[partitionIndex];
    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);

    bool found = false;
    GPRCValue *entry = (GPRCValue*)hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_FIND, &found);
    if (!found) {
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        return NULL;
    }

    SessionPackageRuntime *sessPkgRuntime = copySessionPackageRuntime(entry->sessPkgRuntime);
    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
    return sessPkgRuntime;
}

bool PLGlobalPackageRuntimeCache::remove(uint64 sessionId)
{
    ereport(DEBUG3, (errmodule(MOD_PLSQL), errcode(ERRCODE_LOG),
            errmsg("PLGlobalPackageRuntimeCache LOG: current session id: %lu , remove session id: %lu",
                IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid, sessionId)));
    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl hashTblCtl = hashArray[partitionIndex];
    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);

    bool found = false;
    hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_REMOVE, &found);
    if (!found) {
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        return false;
    }

    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
    return true;
}
