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

#include "utils/plpgsql_domain.h"
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

static uint32 GprcHash(const void* key, Size keysize)
{
    return DatumGetUInt32(DirectFunctionCall1(hashint8, Int64GetDatumFast(*((uint64*)key))));
}

static int GprcMatch(const void *left, const void *right, Size keysize)
{
    uint64 *leftItem = (uint64*)left;
    uint64 *rightItem = (uint64*)right;
    if (*leftItem == *rightItem) {
        return 0;
    } else {
        return 1;
    }
}

volatile bool PLGlobalPackageRuntimeCache::inited = false;

void PLGlobalPackageRuntimeCache::Init()
{
    inited = true;
    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(uint64);
    ctl.entrysize = sizeof(GPRCValue);
    ctl.hash = GprcHash;
    ctl.match = GprcMatch;
    const int hashSize = 1024;

    int flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_EXTERN_CONTEXT | HASH_NOEXCEPT;
    hashArray = (GPRCHashCtl*)
        MemoryContextAllocZero(GLOBAL_PRC_MEMCONTEXT, sizeof(GPRCHashCtl) * NUM_GPRC_PARTITIONS);
    for (uint32 i = 0; i < NUM_GPRC_PARTITIONS; i++) {
        hashArray[i].lockId = FirstGPRCMappingLock + i;
        hashArray[i].context = AllocSetContextCreate(
            GLOBAL_PRC_MEMCONTEXT,
            "GPRC_Bucket_Context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
        ctl.hcxt = hashArray[i].context;
        hashArray[i].hashTbl = hash_create("Global_Package_Runtime_hashtable", hashSize, &ctl, flags);
    }
}

static List* CopyPackageRuntimeStates(SessionPackageRuntime *runtime)
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

List* CopyPortalDatas(SessionPackageRuntime *runtime)
{
    ListCell* cell = NULL;
    List* result = NIL;
    foreach(cell, runtime->portalData) {
        AutoSessionPortalData* portalData = (AutoSessionPortalData*)lfirst(cell);
        AutoSessionPortalData* newportalData = (AutoSessionPortalData*)palloc0(sizeof(AutoSessionPortalData));
        newportalData->outParamIndex = portalData->outParamIndex;
        newportalData->strategy = portalData->strategy;
        newportalData->cursorOptions = portalData->cursorOptions;
        newportalData->commandTag = portalData->commandTag;
        newportalData->atEnd = portalData->atEnd;
        newportalData->atStart = portalData->atStart;
        newportalData->portalPos = portalData->portalPos;
        newportalData->holdStore = portalData->holdStore;
        newportalData->holdContext = portalData->holdContext;
        newportalData->tupDesc = portalData->tupDesc;
        newportalData->is_open = portalData->is_open;
        newportalData->found = portalData->found;
        newportalData->not_found = portalData->not_found;
        newportalData->row_count = portalData->row_count;
        newportalData->null_open = portalData->null_open;
        newportalData->null_fetch = portalData->null_fetch;
        result = lappend(result, newportalData);
    }
    return result;
}

List* CopyFuncInfoDatas(SessionPackageRuntime *runtime)
{
    ListCell* cell = NULL;
    List* result = NIL;
    foreach(cell, runtime->funcValInfo) {
        AutoSessionFuncValInfo* funcInfo = (AutoSessionFuncValInfo*)lfirst(cell);
        AutoSessionFuncValInfo *newfuncInfo = (AutoSessionFuncValInfo*)palloc0(sizeof(AutoSessionFuncValInfo));
        newfuncInfo->found = funcInfo->found;
        newfuncInfo->sql_cursor_found = funcInfo->sql_cursor_found;
        newfuncInfo->sql_notfound = funcInfo->sql_notfound;
        newfuncInfo->sql_isopen = funcInfo->sql_isopen;
        newfuncInfo->sql_rowcount = funcInfo->sql_rowcount;
        newfuncInfo->sqlcode = funcInfo->sqlcode;
        newfuncInfo->sqlcode_isnull = funcInfo->sqlcode_isnull;
        result = lappend(result, newfuncInfo);
    }
    return result;
}

List* CopyPortalContexts(List *portalContexts)
{
    ListCell* cell = NULL;
    List* result = NIL;
    foreach(cell, portalContexts) {
        AutoSessionPortalContextData* portalContext = (AutoSessionPortalContextData*)lfirst(cell);
        AutoSessionPortalContextData* newPortalContext =
            (AutoSessionPortalContextData*)palloc0(sizeof(AutoSessionPortalContextData));
        newPortalContext->status = portalContext->status;
        newPortalContext->portalHoldContext = portalContext->portalHoldContext;
        result = lappend(result, newPortalContext);
    }
    return result;
}

static SessionPackageRuntime* CopySessionPackageRuntime(SessionPackageRuntime *runtime, bool isShare)
{
    MemoryContext pkgRuntimeCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SessionPackageRuntime",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE,
                                                        isShare? SHARED_CONTEXT : STANDARD_CONTEXT);
    MemoryContext oldCtx = MemoryContextSwitchTo(pkgRuntimeCtx);
    SessionPackageRuntime *sessPkgRuntime = (SessionPackageRuntime*)palloc0(sizeof(SessionPackageRuntime));
    sessPkgRuntime->runtimes = CopyPackageRuntimeStates(runtime);
    sessPkgRuntime->context = pkgRuntimeCtx;
    sessPkgRuntime->portalContext = CopyPortalContexts(runtime->portalContext);
    sessPkgRuntime->portalData = CopyPortalDatas(runtime);
    sessPkgRuntime->funcValInfo = CopyFuncInfoDatas(runtime);
    sessPkgRuntime->is_insert_gs_source = runtime->is_insert_gs_source;
    MemoryContextSwitchTo(oldCtx);
    return sessPkgRuntime;
}

bool PLGlobalPackageRuntimeCache::Add(uint64 sessionId, SessionPackageRuntime* runtime)
{
    if (runtime == NULL) {
        ereport(WARNING,
            (errmsg("SessionPackageRuntime can't be null.")));
        return false;
    }

    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl &hashTblCtl = hashArray[partitionIndex];

    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(hashTblCtl.context);

    volatile bool found = false;
    GPRCValue *entry = NULL;
    PG_TRY();
    {
        entry = (GPRCValue*)hash_search_with_hash_value(
            hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_ENTER, (bool*)&found);
        if (found) {
            /* should not happen */
            if (sessionId != entry->sessionId) {
                ereport(ERROR, (errmodule(MOD_GPRC), errcode(ERRCODE_LOG),
                    errmsg("session id not match when remove PLGlobalPackageRuntimeCache"),
                    errdetail("romove PLGlobalPackageRuntimeCache failed, current session id does not match")));
            }

            if (entry->sessPkgRuntime != NULL) {
                ereport(DEBUG3, (errmodule(MOD_GPRC), errcode(ERRCODE_LOG),
                    errmsg("PLGlobalPackageRuntimeCache: remove context(%p) when add, context(%s), parent(%s), shared(%d)",
                        entry->sessPkgRuntime->context, entry->sessPkgRuntime->context->name,
                        entry->sessPkgRuntime->context->parent->name, entry->sessPkgRuntime->context->is_shared)));
                MemoryContextDelete(entry->sessPkgRuntime->context);
            }
            entry->sessPkgRuntime = NULL;
        } else if (entry == NULL) {
            ereport(ERROR, (errmodule(MOD_GPRC), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("palloc hash element GPRCValue failed"),
                errdetail("failed to add hash element for PLGlobalPackageRuntimeCache"),
                errcause("out of memory"),
                erraction("set more memory")));
        }
        entry->sessPkgRuntime = CopySessionPackageRuntime(runtime, true);
        entry->sessionId = sessionId;
    }
    PG_CATCH();
    {
        /* if found and sessionId != entry->sessionId should not remove */
        if ((found == false && entry != NULL) ||
                (found && entry != NULL && sessionId == entry->sessionId)) {
            hash_search_with_hash_value(
                hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_REMOVE, (bool*)&found);
        }
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
    return true;
}

SessionPackageRuntime* PLGlobalPackageRuntimeCache::Fetch(uint64 sessionId)
{
    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl &hashTblCtl = hashArray[partitionIndex];
    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(hashTblCtl.context);

    volatile bool found = false;
    GPRCValue *entry = (GPRCValue*)hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_FIND, (bool*)&found);
    if (!found) {
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        return NULL;
    }

    MemoryContextSwitchTo(oldcontext);

    SessionPackageRuntime *sessPkgRuntime = NULL;
    PG_TRY();
    {
        sessPkgRuntime = CopySessionPackageRuntime(entry->sessPkgRuntime, false);
    }
    PG_CATCH();
    {
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        PG_RE_THROW();
    }
    PG_END_TRY();

    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
    return sessPkgRuntime;
}

bool PLGlobalPackageRuntimeCache::Remove(uint64 sessionId)
{
    uint32 hashCode = DirectFunctionCall1(hashint8, Int64GetDatumFast(sessionId));
    int partitionIndex = getPartition(hashCode);
    GPRCHashCtl &hashTblCtl = hashArray[partitionIndex];
    (void)LWLockAcquire(GetMainLWLockByIndex(hashTblCtl.lockId), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(hashTblCtl.context);

    volatile bool found = false;
    GPRCValue *entry = (GPRCValue*)hash_search_with_hash_value(
        hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_FIND, (bool*)&found);
    if (found) {
        if (sessionId != entry->sessionId) {
            /* should not happen */
            MemoryContextSwitchTo(oldcontext);
            LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
            ereport(ERROR, (errmodule(MOD_GPRC), errcode(ERRCODE_LOG),
                errmsg("session id not match when remove PLGlobalPackageRuntimeCache"),
                errdetail("romove PLGlobalPackageRuntimeCache failed, current session id does not match")));
        }
        MemoryContextDelete(entry->sessPkgRuntime->context);
        entry->sessPkgRuntime = NULL;
        hash_search_with_hash_value(
            hashTblCtl.hashTbl, (const void*)&sessionId, hashCode, HASH_REMOVE, (bool*)&found);
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));
        return true;
    }

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(hashTblCtl.lockId));

    return false;
}
