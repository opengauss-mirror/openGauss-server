/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* mem_snapshot.cpp
*  Memory snapshot related functions.
*
*
* IDENTIFICATION
*        src/common/backend/utils/mmgr/mem_snapshot.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "utils/mem_snapshot.h"
#include "memory_func.h"

/*
 *------------------------------------------------------------------------
 * memory snapshot main body
 *------------------------------------------------------------------------
 */
/*
 * recursive shared memory context
 */
void RecursiveSharedMemoryContext(const MemoryContext context, StringInfoDataHuge* buf, bool isShared)
{
    bool sharedLock = false;

    if (context == NULL) {
        return;
    }

    PG_TRY();
    {
        check_stack_depth();

        if (isShared) {
            MemoryContextLock(context);
            sharedLock = true;
        }

        if (context->type == T_SharedAllocSetContext) {
#ifndef ENABLE_MEMORY_CHECK
            appendStringInfoHuge(buf, "%s:%lu,%lu\n", context->name, ((AllocSet)context)->freeSpace,
                ((AllocSet)context)->totalSpace);
#else
            appendStringInfoHuge(buf, "%s:%lu,%lu\n", context->name, ((AsanSet)context)->freeSpace,
                ((AsanSet)context)->totalSpace);
#endif
        }

        /* recursive MemoryContext's child */
        for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
            if (child->is_shared) {
                RecursiveSharedMemoryContext(child, buf, child->is_shared);
            }
        }
    }
    PG_CATCH();
    {
        if (isShared && sharedLock) {
            MemoryContextUnlock(context);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (isShared) {
        MemoryContextUnlock(context);
    }

    return;
}


/*
 * recursive session or thread memory context
 */
void RecursiveUnSharedMemoryContext(const MemoryContext context, StringInfoDataHuge* buf)
{
    if (context == NULL) {
        return;
    }

#ifndef ENABLE_MEMORY_CHECK
    if (context->type == T_AllocSetContext) {
        appendStringInfoHuge(buf, "%s:%lu,%lu\n", context->name, ((AllocSet)context)->freeSpace,
            ((AllocSet)context)->totalSpace);
    }
#else
    if (context->type == T_AsanSetContext) {
        appendStringInfoHuge(buf, "%s:%lu,%lu\n", context->name, ((AsanSet)context)->freeSpace,
            ((AsanSet)context)->totalSpace);
    }
#endif

    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    /* recursive MemoryContext's child */
    for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
        RecursiveUnSharedMemoryContext(child, buf);
    }

    return;
}

/*
 * Traverse all threads, get thread memory context totalsize and freesize
 */
static void GetThreadMemoryContextDetail(StringInfoDataHuge* buf, uint32* procIdx)
{
    uint32 maxThreadNum = g_instance.proc_base->allProcCount -
        (uint32)(g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS);
    volatile PGPROC* proc = NULL;
    uint32 idx = 0;

    PG_TRY();
    {
        HOLD_INTERRUPTS();

        for (idx = 0; idx < maxThreadNum; idx++) {
            proc = g_instance.proc_base_all_procs[idx];
            *procIdx = idx;

            /* lock this proc's delete MemoryContext action */
            (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
            if (proc->topmcxt != NULL) {
                RecursiveUnSharedMemoryContext(proc->topmcxt, buf);
            }
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }

        /* get memory context detail from postmaster thread */
        if (IsNormalProcessingMode()) {
            RecursiveUnSharedMemoryContext(PmTopMemoryContext, buf);
        }

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*procIdx < maxThreadNum) {
            proc = g_instance.proc_base_all_procs[*procIdx];
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Get shared memory context totalsize and freesize
 */
static StringInfoHuge DumpSharedMemoryContext()
{
    StringInfoDataHuge* memInfo = (StringInfoDataHuge*)palloc0(sizeof(StringInfoDataHuge));
    initStringInfoHuge(memInfo);
    RecursiveSharedMemoryContext(g_instance.instance_context, memInfo, true);
    return memInfo;
}

/*
 * Get session memory context totalsize and freesize
 */
static StringInfoHuge DumpSessionMemoryContext()
{
    if (!ENABLE_THREAD_POOL) {
        return NULL;
    }

    knl_sess_control* sess = NULL;
    StringInfoDataHuge* memInfo = (StringInfoDataHuge*)palloc0(sizeof(StringInfoDataHuge));
    initStringInfoHuge(memInfo);
    g_threadPoolControler->GetSessionCtrl()->getSessionMemoryContextSpace(memInfo, &sess);
    return memInfo;
}

/*
 * Get thread memory context totalsize and freesize
 */
static StringInfoHuge DumpThreadMemoryContext()
{
    uint32 procIdx = 0;
    StringInfoDataHuge* memInfo = (StringInfoDataHuge*)palloc0(sizeof(StringInfoDataHuge));
    initStringInfoHuge(memInfo);
    GetThreadMemoryContextDetail(memInfo, &procIdx);
    return memInfo;
}

#ifdef MEMORY_CONTEXT_TRACK
/*
 * Get shared memory context memory alloc details
 */
static AllocChunk DumpSharedMemoryAllocInfo(const char* ctxName, int *memctxInfoLen)
{
    StringInfoDataHuge memInfo;
    initStringInfoHuge(&memInfo);
    gs_recursive_shared_memory_context(g_instance.instance_context, ctxName, &memInfo, true);
    AllocChunk memctxInfoRes = gs_collate_memctx_info(&memInfo, memctxInfoLen);
    return memctxInfoRes;
}

/*
 * Get session memory context memory alloc details
 */
static AllocChunk DumpSessionMemoryAllocInfo(const char* ctxName, int *memctxInfoLen)
{
    if (!ENABLE_THREAD_POOL) {
        return NULL;
    }

    knl_sess_control* sess = NULL;
    StringInfoDataHuge memInfo;
    initStringInfoHuge(&memInfo);
    g_threadPoolControler->GetSessionCtrl()->getSessionMemoryContextInfo(ctxName, &memInfo, &sess);
    AllocChunk memctxInfoRes = gs_collate_memctx_info(&memInfo, memctxInfoLen);
    return memctxInfoRes;
}

/*
 * Traverse all threads, get thread memory context alloc detail
 */
static void GetThreadMemoryAllocInfo(StringInfoDataHuge* buf, const char* ctxName, uint32* procIdx)
{
    uint32 maxThreadCounts = g_instance.proc_base->allProcCount -
        (uint32)(g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS);
    volatile PGPROC* proc = NULL;
    uint32 idx = 0;

    PG_TRY();
    {
        HOLD_INTERRUPTS();

        for (idx = 0; idx < maxThreadCounts; idx++) {
            proc = g_instance.proc_base_all_procs[idx];
            *procIdx = idx;

            /* lock this proc's delete MemoryContext action */
            (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
            if (proc->topmcxt != NULL) {
                gs_recursive_unshared_memory_context(proc->topmcxt, ctxName, buf);
            }
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }

        /* get memory context detail from postmaster thread */
        if (IsNormalProcessingMode()) {
            gs_recursive_unshared_memory_context(PmTopMemoryContext, ctxName, buf);
        }

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*procIdx < maxThreadCounts) {
            proc = g_instance.proc_base_all_procs[*procIdx];
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Get thread memory context memory alloc details
 */
static AllocChunk DumpThreadMemoryAllocInfo(const char* ctxName, int *memctxInfoLen)
{
    uint32 procIdx = 0;
    StringInfoDataHuge memInfo;
    initStringInfoHuge(&memInfo);
    GetThreadMemoryAllocInfo(&memInfo, ctxName, &procIdx);
    AllocChunk memctxInfoRes = gs_collate_memctx_info(&memInfo, memctxInfoLen);
    return memctxInfoRes;
}

/*
 * encap every memory context alloc detail (file,line,size)
 */
static void EncapMemoryAllocInfoToFile(cJSON *memoryContext, const char *contextName, DumpMemoryType type)
{
    AllocChunk allocInfo = NULL;
    int allocInfoLen = 0;
    switch (type) {
        case MEMORY_CONTEXT_SHARED:
            allocInfo = DumpSharedMemoryAllocInfo(contextName, &allocInfoLen);
            break;
        case MEMORY_CONTEXT_SESSION:
            allocInfo = DumpSessionMemoryAllocInfo(contextName, &allocInfoLen);
            break;
        case MEMORY_CONTEXT_THREAD:
            allocInfo = DumpThreadMemoryAllocInfo(contextName, &allocInfoLen);
            break;
        default:
            break;
    }

    if (allocInfo != NULL && allocInfoLen != 0) {
        for (int i = 0; i< allocInfoLen; i++) {
            cJSON* memoryDetail =  cJSON_CreateObject();
            cJSON_AddStringToObject(memoryDetail, "file", allocInfo[i].file);
            cJSON_AddNumberToObject(memoryDetail, "line", (double)allocInfo[i].line);
            cJSON_AddNumberToObject(memoryDetail, "size", (double)allocInfo[i].size);
            cJSON_AddItemToObject(memoryContext, "MemoryDetail", memoryDetail);
        }
    }
}

#endif

/*
 * contextname as comparison condition
 */
static int DumpMemoryCmpName(const void* cmpA, const void* cmpB)
{
    if (cmpA == NULL && cmpB == NULL) {
        return 0;
    } else if (cmpA == NULL) {
        return 1;
    } else if (cmpB == NULL) {
        return -1;
    }

    MemoryDumpData* dataA = (MemoryDumpData*)cmpA;
    MemoryDumpData* dataB = (MemoryDumpData*)cmpB;
    if (dataA->contextName == NULL && dataB->contextName == NULL) {
        return 0;
    } else if (dataA->contextName == NULL) {
        return 1;
    } else if (dataB->contextName == NULL) {
        return -1;
    }

    int cmpName = strcmp(dataA->contextName, dataB->contextName);
    return cmpName;
}

/*
 * Totalsize as comparison condition
 */
static int DumpMemoryCmpTotalSize(const void* cmpA, const void* cmpB)
{
    if (cmpA == NULL && cmpB == NULL) {
        return 0;
    } else if (cmpA == NULL) {
        return 1;
    } else if (cmpB == NULL) {
        return -1;
    }

    MemoryDumpData* chunkA = (MemoryDumpData*)cmpA;
    MemoryDumpData* chunkB = (MemoryDumpData*)cmpB;

    if (chunkA->totalSize > chunkB->totalSize) {
        return -1;
    } else if (chunkA->totalSize == chunkB->totalSize) {
        return 0;
    } else {
        return 1;
    }
}


/*
 * First sort by context, merge the memory size of the context with the same name, and then sort by
 * totalsize from large to small, taking the memory context of top 20
 */
static void SortMemoryContextInfo(MemoryDumpData* memctxInfoRes, int64 memctxInfoCnt, int64* resLen)
{
    int64 i = 0;
    int64 j = 1;

    qsort(memctxInfoRes, memctxInfoCnt, sizeof(MemoryDumpData), DumpMemoryCmpName);

    while (j < memctxInfoCnt) {
        MemoryDumpData* chunkI = &memctxInfoRes[i];
        MemoryDumpData* chunkJ = &memctxInfoRes[j];
        if (strcmp(chunkI->contextName, chunkJ->contextName) == 0) {
            chunkI->totalSize += chunkJ->totalSize;
            chunkI->freeSize += chunkJ->freeSize;
            ++j;
            continue;
        }

        ++i;
        chunkI = &memctxInfoRes[i];
        char* tmp = chunkI->contextName;
        chunkI->contextName = chunkJ->contextName;
        chunkJ->contextName = tmp;
        chunkI->totalSize = chunkJ->totalSize;
        chunkI->freeSize = chunkJ->freeSize;
        ++j;
    }
    *resLen = i + 1;

    qsort(memctxInfoRes, *resLen, sizeof(MemoryDumpData), DumpMemoryCmpTotalSize);

    /* only dump top 20 memory context */
    *resLen = TOP_MEMORY_CONTEXT_NUM;
}

/*
 * collect the totalsize and freesize for context
 */
static MemoryDumpData* CollateMemoryContextInfo(StringInfoHuge memInfo, int64* resLen)
{
    if (memInfo == NULL) {
        *resLen = 0;
        return NULL;
    }

    int64 i = 0;
    int64 memctxInfoCnt = 0;

    /* find alloc chunk info count */
    for (i = 0; i < memInfo->len; ++i) {
        if (memInfo->data[i] == ':') {
            ++memctxInfoCnt;
        }
    }

    if (memctxInfoCnt == 0) {
        *resLen = 0;
        return NULL;
    }

    /* Traverse memory application information */
    MemoryDumpData *memctxInfoRes =
        (MemoryDumpData*)palloc_huge(CurrentMemoryContext, sizeof(MemoryDumpData) * memctxInfoCnt);
    char* ctxName = memInfo->data;
    char* tmpCtxName = memInfo->data;
    char* freeSize = NULL;
    char* totalSize = NULL;
    int64 realMemctxCnt = 0;
    for (i = 0; i < memctxInfoCnt; ++i) {
        ctxName = tmpCtxName;
        freeSize = strchr(ctxName, ':');
        if (freeSize == NULL) {
            continue;
        }
        *freeSize = '\0';
        ++freeSize;

        totalSize = strchr(freeSize, ',');
        if (totalSize == NULL) {
            continue;
        }
        *totalSize = '\0';
        ++totalSize;

        tmpCtxName = strchr(totalSize, '\n');
        if (tmpCtxName == NULL) {
            continue;
        }
        *tmpCtxName = '\0';
        ++tmpCtxName;

        MemoryDumpData* dumpRes = &memctxInfoRes[realMemctxCnt];
        dumpRes->contextName = (char*)pstrdup(ctxName);
        dumpRes->freeSize = atol(freeSize);
        dumpRes->totalSize = atol(totalSize);
        realMemctxCnt++;
    }

    SortMemoryContextInfo(memctxInfoRes, realMemctxCnt, resLen);

    return memctxInfoRes;
}

/*
 * Generate a timestamp named file name
 */
static char* MemoryInfoFileGetFileName(pg_time_t timestamp, const char* suffix,
    const char* logdir, const char* filename_pattern)
{
    char* filename = NULL;
    int len = 0;
    int ret = 0;

    filename = (char*)palloc(MAXPGPATH);

    ret = snprintf_s(filename, MAXPGPATH, MAXPGPATH - 1, "%s/", logdir);
    securec_check_ss(ret, "", "");

    len = strlen(filename);

    /* treat Log_filename as a strftime pattern */
    (void)pg_strftime(filename + len, MAXPGPATH - len, filename_pattern, pg_localtime(&timestamp, log_timezone));

    if (suffix != NULL) {
        len = strlen(filename);
        if (len > 4 && (strcmp(filename + (len - 4), ".log") == 0))
            len -= 4;
        strlcpy(filename + len, suffix, MAXPGPATH - len);
    }

    return filename;
}

/*
 * create meminfo log file, named by timestamp
 */
static FILE* CreateMemoryInfoFile()
{
    char dump_dir[MAX_PATH_LEN] = {0};
    errno_t ss_rc;
    bool is_absolute = false;

    if (g_instance.stat_cxt.memory_log_directory == NULL) {
        elog(LOG, "mem_log directory does not exist.");
        return NULL;
    }

    // get the path of dump file
    is_absolute = is_absolute_path(g_instance.stat_cxt.memory_log_directory);
    if (is_absolute) {
        ss_rc = snprintf_s(
            dump_dir, sizeof(dump_dir), sizeof(dump_dir) - 1, "%s",
            g_instance.stat_cxt.memory_log_directory);
        securec_check_ss(ss_rc, "\0", "\0");
    } else {
        elog(LOG, "%s maybe not a directory.", g_instance.stat_cxt.memory_log_directory);
        return NULL;
    }

    // 2. check directory is valid
    struct stat info;
    if (stat(dump_dir, &info) == 0) {
        if (!S_ISDIR(info.st_mode)) {
            // S_ISDIR() doesn't exist on my windows
            elog(LOG, "%s maybe not a directory.", dump_dir);
            return NULL;
        }
    } else {
        elog(LOG, "mem_log directory does not exist.");
        return NULL;
    }

    // 3. create file to be dump
    char *dump_file = MemoryInfoFileGetFileName(time(NULL), ".log", dump_dir, "mem_log-%Y-%m-%d_%H%M%S.log");
    FILE* dump_fp = fopen(dump_file, "w");
    if (dump_fp == NULL) {
        elog(LOG, "dump_memory: Failed to create file: %s, cause: %s", dump_file, strerror(errno));
        return NULL;
    }

    return dump_fp;
}

static void EncapGlobalMemoryInfo(cJSON *root)
{
    unsigned long totalVm = 0, res = 0, shared = 0, text = 0, lib, data, dt;
    const char* statmPath = "/proc/self/statm";
    FILE* f = fopen(statmPath, "r");
    int pageSize = getpagesize();  // get the size(bytes) for a page
    if (pageSize <= 0) {
        ereport(WARNING, (errcode(ERRCODE_WARNING),
                errmsg("error for call 'getpagesize()', the values for "
                       "process_used_memory and other_used_memory are error!")));
        pageSize = 1;
    }

    if (f != NULL) {
        if (7 == fscanf_s(f, "%lu %lu %lu %lu %lu %lu %lu\n", &totalVm, &res, &shared, &text, &lib, &data, &dt)) {
            /* page translated to MB */
            totalVm = BYTES_TO_MB((unsigned long)(totalVm * pageSize));
            res = BYTES_TO_MB((unsigned long)(res * pageSize));
            shared = BYTES_TO_MB((unsigned long)(shared * pageSize));
            text = BYTES_TO_MB((unsigned long)(text * pageSize));
        }
        fclose(f);
    }

    int maxDynamicMemory = (int)(maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
    int dynamicUsedMemory = (int)(processMemInChunks << (chunkSizeInBits - BITS_IN_MB));
    int dynamicPeakMemory = (int)(peakChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
    int dynamicUsedShrctx = (int)(shareTrackedMemChunks << (chunkSizeInBits - BITS_IN_MB));
    int dynamicPeakShrctx = (int)(peakChunksSharedContext << (chunkSizeInBits - BITS_IN_MB));
    int maxBackendMemory = (int)(backendReservedMemInChunk << (chunkSizeInBits - BITS_IN_MB));
    int backendUsedMemory = (int)(backendUsedMemInChunk << (chunkSizeInBits - BITS_IN_MB));
    int cuSize = (int)(CUCache->GetCurrentMemSize() >> BITS_IN_MB);
    int otherUsedMemory = (int)(res - shared - text) - dynamicUsedMemory - cuSize;
    if (otherUsedMemory < 0) {
        otherUsedMemory = 0;
    }

    cJSON_AddNumberToObject(root, "Max_dynamic_memory", (double)maxDynamicMemory);
    cJSON_AddNumberToObject(root, "Dynamic_used_memory", (double)dynamicUsedMemory);
    cJSON_AddNumberToObject(root, "Dynamic_peak_memory", (double)dynamicPeakMemory);
    cJSON_AddNumberToObject(root, "Dynamic_used_shrctx", (double)dynamicUsedShrctx);
    cJSON_AddNumberToObject(root, "Dynamic_peak_shrctx", (double)dynamicPeakShrctx);
    cJSON_AddNumberToObject(root, "Max_backend_memory", (double)maxBackendMemory);
    cJSON_AddNumberToObject(root, "Backend_used_memory", (double)backendUsedMemory);
    cJSON_AddNumberToObject(root, "other_used_memory", (double)otherUsedMemory);
}

/*
 * encap top 20 memory context alloc size
 */
static void EncapMemoryContextInfoToFile(cJSON *memCtxInfo, const MemoryDumpData* dumpData,
    int64 dumpDataLen, DumpMemoryType type)
{
    cJSON* memoryType =  cJSON_CreateObject();

    /* encap memory context title */
    switch (type) {
        case MEMORY_CONTEXT_SHARED:
            cJSON_AddStringToObject(memoryType, "Context Type", "Shared Memory Context");
            break;
        case MEMORY_CONTEXT_SESSION:
            cJSON_AddStringToObject(memoryType, "Context Type", "Session Memory Context");
            break;
        case MEMORY_CONTEXT_THREAD:
            cJSON_AddStringToObject(memoryType, "Context Type", "Thread Memory Context");
            break;
        default:
            break;
    }

    for (int i = 0; i< dumpDataLen; i++) {
        cJSON* memoryContext =  cJSON_CreateObject();
        cJSON_AddStringToObject(memoryContext, "context", dumpData[i].contextName);
        cJSON_AddNumberToObject(memoryContext, "freeSize", (double)dumpData[i].freeSize);
        cJSON_AddNumberToObject(memoryContext, "totalSize", (double)dumpData[i].totalSize);
#ifdef MEMORY_CONTEXT_TRACK
        if (u_sess->attr.attr_memory.memory_trace_level == MEMORY_TRACE_LEVEL2) {
            EncapMemoryAllocInfoToFile(memoryContext, dumpData[i].contextName, type);
        }
#endif
        cJSON_AddItemToObject(memoryType, "Memory Context", memoryContext);
    }

    cJSON_AddItemToObject(memCtxInfo, "Memory Context Detail", memoryType);
}

/*
 * dump all memory context info
 */
static void DumpMemoryContextInfo()
{
    StringInfoHuge memBuf = NULL;
    MemoryDumpData* dumpData = NULL;
    int64 dumpDataLen = 0;
    cJSON* root =  cJSON_CreateObject();

    /* encap global memory info */
    cJSON* globalMemoryInfo =  cJSON_CreateObject();
    EncapGlobalMemoryInfo(globalMemoryInfo);
    cJSON_AddItemToObject(root, "Global Memory Statistics", globalMemoryInfo);

    cJSON* MemoryContextInfo =  cJSON_CreateObject();
    /* encap shared memory context info */
    memBuf = DumpSharedMemoryContext();
    if (memBuf != NULL) {
        dumpData = CollateMemoryContextInfo(memBuf, &dumpDataLen);
        if (dumpData != NULL && dumpDataLen != 0) {
            EncapMemoryContextInfoToFile(MemoryContextInfo, dumpData, dumpDataLen, MEMORY_CONTEXT_SHARED);
        }
    }

    /* encap session memory context info */
    memBuf = DumpSessionMemoryContext();
    if (memBuf != NULL) {
        dumpData = CollateMemoryContextInfo(memBuf, &dumpDataLen);
        if (dumpData != NULL && dumpDataLen != 0) {
            EncapMemoryContextInfoToFile(MemoryContextInfo, dumpData, dumpDataLen, MEMORY_CONTEXT_SESSION);
        }
    }

    /* encap thread memory context info */
    memBuf = DumpThreadMemoryContext();
    if (memBuf != NULL) {
        dumpData = CollateMemoryContextInfo(memBuf, &dumpDataLen);
        if (dumpData != NULL && dumpDataLen != 0) {
            EncapMemoryContextInfoToFile(MemoryContextInfo, dumpData, dumpDataLen, MEMORY_CONTEXT_THREAD);
        }
    }

    cJSON_AddItemToObject(root, "Memory Context Info", MemoryContextInfo);

    FILE* fp = CreateMemoryInfoFile();
    if (fp == NULL) {
        ereport(ERROR, (errmsg("create memory info file failed.")));
    }

    char *cjsonStr = cJSON_Print(root);
    if (cjsonStr == NULL) {
        ereport(ERROR, (errmsg("Json to string failed.")));
    }
    uint64 bytes = fwrite(cjsonStr, 1, strlen(cjsonStr), fp);
    if (bytes != (uint64)strlen(cjsonStr)) {
        elog(LOG, "Could not write memory usage information. Attempted to write %lu", strlen(cjsonStr));
    }
    (void)fclose(fp);

    pfree_ext(cjsonStr);
    cJSON_Delete(root);
}


/*
 *------------------------------------------------------------------------
 * Overload escape main body
 *------------------------------------------------------------------------
 */

/*
 * check dynamic memory is reach max_dynamic_memory resilience_memory_reject_percent
 */
static bool CheckMemoryReachResetLimit()
{
    if (!t_thrd.utils_cxt.gs_mp_inited) {
        return false;
    }

    if (u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_HIGH_KIND] == 0) {
        return false;
    }

    if ((processMemInChunks > maxChunksPerProcess
        * u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_HIGH_KIND] / FULL_PERCENT)) {
        return true;
    }

    return false;
}

/*
 * check session is reach resilience_threadpool_reject_cond
 */
static bool CheckThreadPoolReachResetLimit()
{
    if (!ENABLE_THREAD_POOL || !g_threadPoolControler) {
        return false;
    }

    if (u_sess->attr.attr_common.threadpool_reset_percent_list[PERCENT_HIGH_KIND] == 0) {
        return false;
    }

    int currentConn = g_instance.conn_cxt.CurConnCount + g_instance.conn_cxt.CurCMAConnCount;
    int threadpoolNum = g_threadPoolControler->GetThreadActualNum();
    if (threadpoolNum <= 0) {
        return false;
    }

    int64 currentConnPercent = currentConn * FULL_PERCENT / threadpoolNum;
    if (currentConnPercent > u_sess->attr.attr_common.threadpool_reset_percent_list[PERCENT_HIGH_KIND]) {
        return true;
    }

    return false;
}

/*
 * check dynamic memory is reach max_dynamic_memory 90%
 */
static bool CheckMemoryReachSnapshotLimit()
{
    if (!t_thrd.utils_cxt.gs_mp_inited) {
        return false;
    }

    if (u_sess->attr.attr_memory.memory_trace_level == MEMORY_TRACE_NONE) {
        return false;
    }

    if (processMemInChunks > maxChunksPerProcess * MEMORY_TRACE_PERCENT / FULL_PERCENT) {
        return true;
    }

    return false;
}

static bool CheckSessionCanTerminate(const volatile PGPROC* proc)
{
    if (proc->usedMemory == NULL) {
        return false;
    }

    if (superuser_arg(proc->roleId) || systemDBA_arg(proc->roleId)) {
        return false;
    }

    if (isMonitoradmin(proc->roleId)) {
        return false;
    }

    return true;
}
static SessMemoryUsage* getSessionMemoryUsage(int* num)
{
    uint32 max_thread_count =
     g_instance.proc_base->allProcCount - (uint32)g_instance.attr.attr_storage.max_prepared_xacts;
    volatile PGPROC* proc = NULL;
    uint32 idx = 0;

    HOLD_INTERRUPTS();

    SessMemoryUsage* result = (SessMemoryUsage*)palloc(max_thread_count * sizeof(SessMemoryUsage));
    int index = 0;

    for (idx = 0; idx < max_thread_count; idx++) {
        proc = g_instance.proc_base_all_procs[idx];

        /* lock this proc's delete MemoryContext action */
        (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
        if (CheckSessionCanTerminate(proc)) {
            result[index].sessid = proc->pid;
            result[index].usedSize = *proc->usedMemory;
            /* BackendStatusArray may not assigned while new connnection init */
            if (t_thrd.shemem_ptr_cxt.BackendStatusArray != NULL && proc->backendId != InvalidBackendId) {
                result[index].state = (int)t_thrd.shemem_ptr_cxt.BackendStatusArray[proc->backendId - 1].st_state;
            } else {
                result[index].state = STATE_IDLE;
            }
            index++;
        }
        (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
    }

    RESUME_INTERRUPTS();

    *num = index;
    return result;
}

static int memoryUsageCompare(const void* p1, const void* p2)
{
    SessMemoryUsage* m1 = (SessMemoryUsage*)p1;
    SessMemoryUsage* m2 = (SessMemoryUsage*)p2;
    /* release idle session first */
    if (m1->state != m2->state) {
        return m1->state < m2->state;
    }
    return m1->usedSize > m2->usedSize;
}

/*
 * clear all connections
 */
static void TerminateALLConnection()
{
    int sessNum = 0;
    SessMemoryUsage* sessUsage = NULL;
    if (ENABLE_THREAD_POOL) {
        sessUsage = g_threadPoolControler->GetSessionCtrl()->getSessionMemoryUsage(&sessNum);
    } else {
        sessUsage = getSessionMemoryUsage(&sessNum);
    }

    qsort(sessUsage, sessNum, sizeof(SessMemoryUsage), memoryUsageCompare);

    int i = 0;
    uint64 currentMemory = (uint64)processMemInChunks << chunkSizeInBits;
    uint64 targetMemory = ((uint64)maxChunksPerProcess << chunkSizeInBits) *
            (uint64)u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_LOW_KIND] / FULL_PERCENT;
    for (i = 0; i < sessNum; i++) {
        if (unlikely(currentMemory < targetMemory)) {
            break;
        }

        if (ENABLE_THREAD_POOL) {
            ThreadPoolSessControl *sess_ctrl = g_threadPoolControler->GetSessionCtrl();
            int ctrlIdx = sess_ctrl->FindCtrlIdxBySessId(sessUsage[i].sessid);
            sess_ctrl->SendSignal(ctrlIdx, SIGTERM);
        } else {
            kill_backend(sessUsage[i].sessid, false);
        }

        currentMemory -= sessUsage[i].usedSize;
    }

    pfree(sessUsage);
}

/*
 * Memory full clear all connections
 */
static void CleanConnectionByMemory()
{
    int waitCount = 0;
    PG_TRY();
    {
        g_instance.comm_cxt.rejectRequest = true;
        TerminateALLConnection();
        while (true) {
            if (processMemInChunks < maxChunksPerProcess *
                u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_LOW_KIND] / FULL_PERCENT) {
                break;
            }
            if (waitCount > MAX_WAIT_COUNT) {
                break;
            }
            waitCount++;
            pg_usleep(USECS_PER_SEC);
        }
        g_instance.comm_cxt.rejectRequest = false;
    }
    PG_CATCH();
    {
        g_instance.comm_cxt.rejectRequest = false;
    }
    PG_END_TRY();
}

/*
 * Thread pool full clear all connections
 */
static void CleanConnectionByThreadpool()
{
    int waitCount = 0;
    PG_TRY();
    {
        g_instance.comm_cxt.rejectRequest = true;
        TerminateALLConnection();
        while (true) {
            int currentConn = g_instance.conn_cxt.CurConnCount + g_instance.conn_cxt.CurCMAConnCount;
            int threadpoolNum = g_threadPoolControler->GetThreadActualNum();
            if (threadpoolNum > 0) {
                int64 currentConnPercent = currentConn * FULL_PERCENT / threadpoolNum;
                if (currentConnPercent < u_sess->attr.attr_common.threadpool_reset_percent_list[PERCENT_LOW_KIND]) {
                    break;
                }
            }
            if (waitCount > MAX_WAIT_COUNT) {
                break;
            }
            waitCount++;
            pg_usleep(USECS_PER_SEC);
        }
        g_instance.comm_cxt.rejectRequest = false;
    }
    PG_CATCH();
    {
        g_instance.comm_cxt.rejectRequest = false;
    }
    PG_END_TRY();
}

/*
 * Overload Escape entry
 */
void ExecOverloadEscape()
{
    MemoryContext context = AllocSetContextCreate(CurrentMemoryContext,
        "Overload Escape Context", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    MemoryContext oldContext = MemoryContextSwitchTo(context);

    DISABLE_MEMORY_PROTECT();

    if (CheckMemoryReachSnapshotLimit()) {
        if (!t_thrd.wlm_cxt.has_do_memory_snapshot) {
            elog(LOG, "Start record current memory info.");
            DumpMemoryContextInfo();
            elog(LOG, "End record current memory info.");
            t_thrd.wlm_cxt.has_do_memory_snapshot = true;
        }
    } else {
        t_thrd.wlm_cxt.has_do_memory_snapshot = false;
    }

    /* Non upgrade mode */
    if (u_sess->attr.attr_common.upgrade_mode == 0) {
        if (CheckMemoryReachResetLimit()) {
            elog(LOG, "Start clean connection by memory not enough.");
            CleanConnectionByMemory();
            elog(LOG, "End clean connection by memory not enough.");
        } else if (CheckThreadPoolReachResetLimit()) {
            elog(LOG, "Start clean connection by threadpool not enough.");
            CleanConnectionByThreadpool();
            elog(LOG, "End clean connection by threadpool not enough.");
        }
    }

    ENABLE_MEMORY_PROTECT();

    (void)MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(context);
}

/*
 *------------------------------------------------------------------------
 * Control parameter checking and assignment functions
 *------------------------------------------------------------------------
 */
static List* SplitPercentIntoList(const char* levels)
{
    List *result = NIL;
    char *str = pstrdup(levels);
    char *first_ch = str;
    int len = (int)strlen(str) + 1;

    for (int i = 0; i < len; i++) {
        if (str[i] == ',' || str[i] == '\0') {
            /* replace ',' with '\0'. */
            str[i] = '\0';

            /* copy this into result. */
            result = lappend(result, pstrdup(first_ch));

            /* move to the head of next string. */
            first_ch = str + i + 1;
        }
    }
    pfree(str);

    return result;
}

bool CheckStrIsIntNumber(const char* str)
{
    Size len = strlen(str);
    if (len > MAX_INT_NUM_LEN) {
        return false;
    }

    for (Size i = 0; i < len; i++) {
        if (!(str[i] >= '0' && str[i] <= '9')) {
            return false;
        }
    }
    return true;
}

void AssignThreadpoolResetPercent(const char* newval, void* extra)
{
    int *level = (int*) extra;

    u_sess->attr.attr_common.threadpool_reset_percent_list[PERCENT_LOW_KIND] = level[0];
    u_sess->attr.attr_common.threadpool_reset_percent_list[PERCENT_HIGH_KIND] = level[1];
}

bool CheckThreadpoolResetPercent(char** newval, void** extra, GucSource source)
{
    /* parse level */
    List *l = SplitPercentIntoList(*newval);

    if (list_length(l) != RESET_PERCENT_KIND) {
        GUC_check_errdetail("attr num:%d is error,resilience_threadpool_reject_cond attr is 2", l->length);
        list_free_deep(l);
        return false;
    }

    if (!CheckStrIsIntNumber((char*)linitial(l)) || !CheckStrIsIntNumber((char*)lsecond(l))) {
        GUC_check_errdetail("invalid input syntax");
        list_free_deep(l);
        return false;
    }

    int64 tpoolLowLevel = atol((char*)linitial(l));
    int64 tpoolHighLevel = atol((char*)lsecond(l));
    list_free_deep(l);

    if (!(tpoolLowLevel == 0 && tpoolHighLevel == 0)) {
        if (tpoolLowLevel < 0 || tpoolHighLevel < 0 || tpoolLowLevel >= tpoolHighLevel ||
            tpoolHighLevel > INT_MAX) {
            GUC_check_errdetail("invalid input syntax");
            return false;
        }
    }

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), RESET_PERCENT_KIND * sizeof(int));
    ((int*)(*extra))[0] = (int)tpoolLowLevel;
    ((int*)(*extra))[1] = (int)tpoolHighLevel;

    return true;
}

void AssignMemoryResetPercent(const char* newval, void* extra)
{
    int *level = (int*) extra;

    u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_LOW_KIND] = level[0];
    u_sess->attr.attr_memory.memory_reset_percent_list[PERCENT_HIGH_KIND] = level[1];
}

bool CheckMemoryResetPercent(char** newval, void** extra, GucSource source)
{
    /* parse level */
    List *l = SplitPercentIntoList(*newval);

    if (list_length(l) != RESET_PERCENT_KIND) {
        GUC_check_errdetail("attr num:%d is error,resilience_memory_reject_percent attr is 2", l->length);
        list_free_deep(l);
        return false;
    }

    if (!CheckStrIsIntNumber((char*)linitial(l)) || !CheckStrIsIntNumber((char*)lsecond(l))) {
        GUC_check_errdetail("invalid input syntax");
        list_free_deep(l);
        return false;
    }

    int64 memLowLevel = atol((char*)linitial(l));
    int64 memHighLevel = atol((char*)lsecond(l));
    list_free_deep(l);

    if (!(memLowLevel == 0 && memHighLevel == 0)) {
        if (memLowLevel < 0 || memHighLevel < 0 || memLowLevel >= memHighLevel ||
            memHighLevel > FULL_PERCENT) {
            GUC_check_errdetail("invalid input syntax");
            return false;
        }
    }

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), RESET_PERCENT_KIND * sizeof(int));
    ((int*)(*extra))[0] = (int)memLowLevel;
    ((int*)(*extra))[1] = (int)memHighLevel;

    return true;
}

/*
 *------------------------------------------------------------------------
 * init memory log directory
 *------------------------------------------------------------------------
 */
/*
 * init memory log directory
 */
void InitMemoryLogDirectory()
{
    MemoryContext oldContext = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    /* create memory log directory */
    init_instr_log_directory(true, MEMORY_LOG_TAG);
    (void)MemoryContextSwitchTo(oldContext);
}

