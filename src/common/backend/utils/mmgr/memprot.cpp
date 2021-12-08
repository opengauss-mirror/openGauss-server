/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * memprot.cpp
 *
 * The implementation of memory protection. We implement a mechanism to enforce
 * a quota on the total memory consumption at query level.
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/memprot.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "utils/aset.h"
#include "utils/atomic.h"
#include "utils/memprot.h"
#include "executor/exec/execStream.h"
#include "tcop/tcopprot.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "libcomm/libcomm.h"
#include "replication/dataprotocol.h"
#include "replication/walprotocol.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"

/* Track memory usage by all shared memory context */
volatile int32 shareTrackedMemChunks = 0;
int64 shareTrackedBytes = 0;

/* Track memory usage in storage for it calls malloc directly */
int64 storageTrackedBytes = 0;

/* peak size of shared memory context */
int32 peakChunksSharedContext = 0;

/* Chunk size in bits. By default a chunk is 1MB. */
unsigned int chunkSizeInBits = BITS_IN_MB;

/* Physical Memory quota of Machine In Memory */
int physicalMemQuotaInChunks = 0;

/* Smoothing value to eliminate errors */
int eliminateErrorsMemoryBytes = 1 * 1024 * 512;

int32 maxChunksPerProcess = 0;   // be set by GUC variable --max_dynamic_memory
volatile int32 processMemInChunks = 200;  // track the memory used by process --dynamic_used_memory
int32 peakChunksPerProcess = 0;  // the peak memory of process --dynamic_peak_memory
int32 comm_original_memory = 0;  // original comm memory
int32 maxSharedMemory = 0;       // original shared memory
int32 backendReservedMemInChunk = 0;  // reserved memory for backend threads
volatile int32 backendUsedMemInChunk = 0;      // the memory usage for backend threads

/* Track memory usage by all dynamic memory context */
volatile int32 dynmicTrackedMemChunks = 200;

/* THREAD LOCAL variable for print memory alarm string periodically */
THR_LOCAL TimestampTz last_print_timestamp = 0;

/*
 * This is the virtual function table for Memory Functions
 */
MemoryProtectFuncDef GenericFunctions = {MemoryProtectFunctions::gs_memprot_malloc<MEM_THRD>,
    MemoryProtectFunctions::gs_memprot_free<MEM_THRD>,
    MemoryProtectFunctions::gs_memprot_realloc<MEM_THRD>,
    MemoryProtectFunctions::gs_posix_memalign<MEM_THRD>};

MemoryProtectFuncDef SessionFunctions = {MemoryProtectFunctions::gs_memprot_malloc<MEM_SESS>,
    MemoryProtectFunctions::gs_memprot_free<MEM_SESS>,
    MemoryProtectFunctions::gs_memprot_realloc<MEM_SESS>,
    MemoryProtectFunctions::gs_posix_memalign<MEM_SESS>};

MemoryProtectFuncDef SharedFunctions = {MemoryProtectFunctions::gs_memprot_malloc<MEM_SHRD>,
    MemoryProtectFunctions::gs_memprot_free<MEM_SHRD>,
    MemoryProtectFunctions::gs_memprot_realloc<MEM_SHRD>,
    MemoryProtectFunctions::gs_posix_memalign<MEM_SHRD>};

extern uint64 g_searchserver_memory;
extern bool is_searchserver_api_load();
extern void* get_searchlet_resource_info(int* used_mem, int* peak_mem);

void gs_output_memory_info(void);

#ifdef MEMORY_CONTEXT_CHECKING
/*
 * inline function for memory enjection
 */
bool gs_memory_enjection(void)
{
    if (MEMORY_FAULT_PERCENT && !t_thrd.xact_cxt.bInAbortTransaction && !t_thrd.int_cxt.CritSectionCount &&
        !(AmPostmasterProcess()) && IsNormalProcessingMode() && t_thrd.utils_cxt.memNeedProtect &&
        (gs_random() % MAX_MEMORY_FAULT_PERCENT <= MEMORY_FAULT_PERCENT)) {
        return true;
    }

    return false;
}
#endif


/*
 * check if the node is on heavy memory status now?
 * is strict is true, we'll do some pre-judgement.
 */
FORCE_INLINE bool gs_sysmemory_busy(int64 used, bool strict)
{
    if (!GS_MP_INITED)
        return false;

    int64 percent = (processMemInChunks * 100) / maxChunksPerProcess;
    int usedInChunk = used >> chunkSizeInBits;

    if ((g_instance.wlm_cxt->stat_manager.comp_count > 1 || strict) && percent >= LOW_PROCMEM_MARK &&
        percent <= HIGH_PROCMEM_MARK) {
        if (usedInChunk >= LOW_WORKMEM_CHUNK &&
            usedInChunk > (double)maxChunksPerProcess / 2000 *
                              ((HIGH_PROCMEM_MARK - percent) * (HIGH_PROCMEM_MARK - percent) + 200))
            return true;
    }

    if (percent > HIGH_PROCMEM_MARK && usedInChunk > Min(maxChunksPerProcess / 100, LOW_WORKMEM_CHUNK))
        return true;

    return false;
}

/* check if system can service the new memory request */
bool gs_sysmemory_avail(int64 requestedBytes)
{
    if (!GS_MP_INITED)
        return true;  // don't care

    int64 newsize = t_thrd.utils_cxt.trackedBytes + requestedBytes;

    // How many chunks we need so far
    int32 newszChunk = (uint64)newsize >> chunkSizeInBits;

    if (newszChunk > t_thrd.utils_cxt.trackedMemChunks &&
        (processMemInChunks + newszChunk - t_thrd.utils_cxt.trackedMemChunks) > maxChunksPerProcess)
        return false;

    return true;
}

/* find the information of abnormal memory context */
void gs_find_abnormal_memctx(MemoryContext context)
{
#ifndef ENABLE_MEMORY_CHECK
    AllocSet aset = (AllocSet)context;
#else
    AsanSet aset = (AsanSet)context;
#endif

    if (context->type == T_SharedAllocSetContext || context->type == T_MemalignSharedAllocSetContext) {
        if (aset->totalSpace > SELF_SHARED_MEMCTX_LIMITATION) { // 100MB
            write_stderr("----debug_query_id=%lu, WARNING: the shared memory context '%s' is using %d MB size larger "
                         "than %d MB.\n",
                u_sess->debug_query_id,
                context->name,
                (int)(aset->totalSpace >> BITS_IN_MB),
                SELF_SHARED_MEMCTX_LIMITATION >> BITS_IN_MB);
        }
    } else {
        if (aset->totalSpace > (Size)(aset->maxSpaceSize + SELF_GENRIC_MEMCTX_LIMITATION)) { // 10MB + 10MB
            write_stderr("----debug_query_id=%lu, WARNING: the common memory context '%s' is using %d MB size larger "
                         "than %d MB.\n",
                u_sess->debug_query_id,
                context->name,
                (int)(aset->totalSpace >> BITS_IN_MB),
                (int)(aset->maxSpaceSize >> BITS_IN_MB));
        }
    }
}

/* recursive to verify the size of memory context and find the abnormal */
void gs_recursive_verify_memctx(MemoryContext context, bool is_shared)
{
    MemoryContext child;

    PG_TRY();
    {
        CHECK_FOR_INTERRUPTS();

        if (is_shared) {
            MemoryContextLock(context);
        }

        gs_find_abnormal_memctx(context);

        /*recursive MemoryContext's child*/
        for (child = context->firstchild; child != NULL; child = child->nextchild) {
            if (child->is_shared == is_shared) {
                gs_recursive_verify_memctx(child, is_shared);
            }
        }
    }
    PG_CATCH();
    {
        if (is_shared) {
            MemoryContextUnlock(context);
        }

        PG_RE_THROW();
    }
    PG_END_TRY();

    if (is_shared) {
        MemoryContextUnlock(context);
    }
}

/* reset the beyond chunk for communication threads when memory allocation failed */
void gs_memprot_reset_beyondchunk(void)
{
    t_thrd.utils_cxt.beyondChunk = 0;
}

/* display the query string info */
void gs_display_query_string(uint64 sessionid)
{
    /* print the query string who costs the maximum memory */
    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL &&
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid != sessionid) {
        /* get backend status with thread id */
        PgBackendStatus* beentry = pgstat_get_backend_single_entry(sessionid);
        if (beentry != NULL && *(beentry->st_activity) != '\0') {
            write_stderr(
                "----debug_query_id=%lu, It is not the current session and beentry info : "
                "datid<%u>, app_name<%s>, query_id<%lu>, tid<%lu>, lwtid<%d>, parent_sessionid<%lu>, thread_level<%d>, "
                "query_string<%s>.\n",
                u_sess->debug_query_id,
                beentry->st_databaseid,
                beentry->st_appname ? beentry->st_appname : "unnamed thread",
                beentry->st_queryid,
                beentry->st_procpid,
                beentry->st_tid,
                beentry->st_parent_sessionid,
                beentry->st_thread_level,
                beentry->st_activity);
        }
    }
}

/*
 * get the info from sessionMemoryEntry
 */
void gs_display_query_by_sessionentry(uint64* saveThreadid, uint64* maxThreadid)
{
    /* out of memory caused by session
     * find all user information, get its complicated queries
     */
    SessionLevelMemory *entry = NULL;
    SessionLevelMemory *tmpentry = NULL;
    SessionLevelMemory localEntry;
    int entryIndex = 0;
    int compCnt = 0;
    int beyCnt = 0;
    int beySize = 0;
    int allEstMemory = 0;
    int curCostMemory = 0;
    int allCostMemory = 0;
    int maxCurCostMemory = 0;
    int maxEstCostMemory = 0;
    int saveThreadCost = 0;
    int saveThreadEst = 0;

    for (entryIndex = 0; entryIndex < SessionMemoryArraySize; entryIndex++) {
        entry = &(t_thrd.shemem_ptr_cxt.sessionMemoryArray[entryIndex]);
        int ss_rc = memcpy_s(&localEntry, sizeof(localEntry), entry, sizeof(SessionLevelMemory));
        securec_check(ss_rc, "\0", "\0");

        if (entry->isValid == false || entry->iscomplex == 0)
            continue;

        tmpentry = &localEntry;

        allEstMemory += tmpentry->estimate_memory;
        curCostMemory = (int)((unsigned int)(tmpentry->queryMemInChunks - tmpentry->initMemInChunks)
                              << (chunkSizeInBits - BITS_IN_MB));

        allCostMemory += curCostMemory;

        compCnt++;

        /* get the query info with max beyond size */
        if (curCostMemory > tmpentry->estimate_memory) {
            if (tmpentry->estimate_memory)
                beyCnt++;

            if ((curCostMemory - tmpentry->estimate_memory) > beySize) {
                beySize = (curCostMemory - tmpentry->estimate_memory);
                *saveThreadid = tmpentry->sessionid;
                saveThreadCost = curCostMemory;
                saveThreadEst = tmpentry->estimate_memory;
            }
        }

        /* get the max cost query */
        if (curCostMemory > maxCurCostMemory) {
            maxCurCostMemory = curCostMemory;
            *maxThreadid = tmpentry->sessionid;
            maxEstCostMemory = tmpentry->estimate_memory;
        }
    }

    if (allEstMemory || curCostMemory)
        write_stderr("----debug_query_id=%lu, Total estimated Memory is %d MB, total current cost Memory is %d MB, "
                     "the difference is %d MB."
                     "The count of complicated queries is %d and the count of uncontrolled queries is %d.\n",
            u_sess->debug_query_id,
            allEstMemory,
            allCostMemory,
            allCostMemory - allEstMemory,
            compCnt,
            beyCnt);

    if (*saveThreadid == *maxThreadid) {
        write_stderr("----debug_query_id=%lu, The abnormal query thread id %lu."
                     "It current used memory is %d MB and estimated memory is %d MB."
                     "It also is the query which costs the maximum memory.\n",
            u_sess->debug_query_id,
            *saveThreadid,
            saveThreadCost,
            saveThreadEst);
        gs_display_query_string(*maxThreadid);
    } else {
        if (*saveThreadid) {
            write_stderr("----debug_query_id=%lu, The abnormal query thread id %lu."
                         "It current used memory is %d MB and estimated memory is %d MB.\n",
                u_sess->debug_query_id,
                *saveThreadid,
                saveThreadCost,
                saveThreadEst);
            gs_display_query_string(*saveThreadid);
        }
        if (*maxThreadid) {
            write_stderr("----debug_query_id=%lu, The query which costs the maximum memory is %lu."
                         "It current used memory is %d MB and estimated memory is %d MB.\n",
                u_sess->debug_query_id,
                *maxThreadid,
                maxCurCostMemory,
                maxEstCostMemory);
            gs_display_query_string(*maxThreadid);
        }
    }
}

/* display the information of uncontrolled query in some user */
void gs_display_uncontrolled_query(int* procIdx)
{
    /* libcomm permanent thread don't need to display query info */
    if (t_thrd.comm_cxt.LibcommThreadType != LIBCOMM_NONE || t_thrd.shemem_ptr_cxt.sessionMemoryArray == NULL)
        return;

    uint64 saveSessionid = 0;
    uint64 maxSessionid = 0;

    gs_display_query_by_sessionentry(&saveSessionid, &maxSessionid);

    /* search the thread and print its memory usage */
    if (saveSessionid || maxSessionid) {
        volatile PGPROC* proc = NULL;
        int idx = 0;
        PG_TRY();
        {
            /* Print the top used memory */
            HOLD_INTERRUPTS();

            for (idx = 0; idx < (int)g_instance.proc_base->allProcCount; idx++) {
                proc = g_instance.proc_base_all_procs[idx];
                *procIdx = idx;

                if (proc->sessMemorySessionid == saveSessionid || proc->sessMemorySessionid == maxSessionid) {
                    /*lock this proc's delete MemoryContext action*/
                    (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
                    if (NULL != proc->topmcxt)
                        gs_recursive_verify_memctx(proc->topmcxt, false);
                    (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
                }
            }

            RESUME_INTERRUPTS();
        }
        PG_CATCH();
        {
            if (*procIdx < (int)g_instance.proc_base->allProcCount) {
                proc = g_instance.proc_base_all_procs[*procIdx];
                (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
            }
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
}

/* print the detail information when memory allocation is failed */
void gs_output_memory_info(void)
{
    int max_dynamic_memory = (int)(maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
    int dynamic_used_memory = processMemInChunks << (chunkSizeInBits - BITS_IN_MB);
    int dynamic_peak_memory = (int)(peakChunksPerProcess << (chunkSizeInBits - BITS_IN_MB));
    int dynamic_used_shrctx = (int)(shareTrackedMemChunks << (chunkSizeInBits - BITS_IN_MB));
    int dynamic_peak_shrctx = (int)(peakChunksSharedContext << (chunkSizeInBits - BITS_IN_MB));
    int max_sctpcomm_memory = comm_original_memory;
    int sctpcomm_used_memory = (int)(gs_get_comm_used_memory() >> BITS_IN_MB);
    int sctpcomm_peak_memory = (int)(gs_get_comm_peak_memory() >> BITS_IN_MB);
    int comm_global_memctx = (gs_get_comm_context_memory() >> BITS_IN_MB);
    int gpu_max_dynamic_memory = 0;
    int gpu_dynamic_used_memory = 0;
    int gpu_dynamic_peak_memory = 0;
    int large_storage_memory = (int)(storageTrackedBytes >> BITS_IN_MB);
    int procIdx = 0;

#ifdef ENABLE_MULTIPLE_NODES
    /* get the memory used by gpu */
    if (is_searchserver_api_load()) {
        void* mem_info = get_searchlet_resource_info(&gpu_dynamic_used_memory, &gpu_dynamic_peak_memory);
        if (mem_info != NULL) {
            gpu_max_dynamic_memory = g_searchserver_memory;
            pfree(mem_info);
        }
    }
#endif

    /* output the memory usage */
    write_stderr("----debug_query_id=%lu, Memory information of whole process in MB:"
                 "max_dynamic_memory: %d, dynamic_used_memory: %d, dynamic_peak_memory: %d, "
                 "dynamic_used_shrctx: %d, dynamic_peak_shrctx: %d, "
                 "max_sctpcomm_memory: %d, sctpcomm_used_memory: %d, sctpcomm_peak_memory: %d, comm_global_memctx: %d, "
                 "gpu_max_dynamic_memory: %d, gpu_dynamic_used_memory: %d, gpu_dynamic_peak_memory: %d, "
                 "large_storage_memory: %d.\n",
        u_sess->debug_query_id,
        max_dynamic_memory,
        dynamic_used_memory,
        dynamic_peak_memory,
        dynamic_used_shrctx,
        dynamic_peak_shrctx,
        max_sctpcomm_memory,
        sctpcomm_used_memory,
        sctpcomm_peak_memory,
        comm_global_memctx,
        gpu_max_dynamic_memory,
        gpu_dynamic_used_memory,
        gpu_dynamic_peak_memory,
        large_storage_memory);

    int real_shrctx_size = dynamic_used_shrctx - sctpcomm_used_memory;

    /* if the used shared context is beyond the limitation, print all used memory context */
    if (real_shrctx_size > MAX_SHARED_MEMCTX_LIMITATION) { // 2G
        write_stderr("----debug_query_id=%lu, WARNING: the memory used in shared memory context is %d beyond %d, "
                     "it may cause the out of memory.\n",
            u_sess->debug_query_id,
            real_shrctx_size,
            MAX_SHARED_MEMCTX_LIMITATION);
        gs_recursive_verify_memctx(g_instance.instance_context, true);

        if (max_dynamic_memory != 0 && (real_shrctx_size > MAX_SHARED_MEMCTX_SIZE) &&  // 10G
            (real_shrctx_size * 100 / max_dynamic_memory) > 60) {
            write_stderr(
                "----debug_query_id=%lu, FATAL: share memory context is out of control!\n", u_sess->debug_query_id);
        }
    }

    /* if the used memory in communication is beyond the limitation, print one warning */
    if (sctpcomm_used_memory > MAX_COMM_USED_SIZE) {
        write_stderr("----debug_query_id=%lu, WARNING: the memory used in communication layer is beyond %d, "
                     "it may cause the out of memory.\n",
            u_sess->debug_query_id,
            MAX_COMM_USED_SIZE);

        if (max_dynamic_memory != 0 && (sctpcomm_used_memory > max_sctpcomm_memory) &&  // 4G
            ((sctpcomm_used_memory * 100 / max_dynamic_memory) > 60)) {
            write_stderr(
                "----debug_query_id=%lu, FATAL: communication memory is out of control!\n", u_sess->debug_query_id);
        }
    }

    /* display the information of uncontrolled query */
    gs_display_uncontrolled_query(&procIdx);
}

/* the failure interface when allocating memory */
template <bool flag>
void gs_memprot_failed(int64 sz, MemType type)
{
    /* request 50 chunks (> 50M) for processing error */
    t_thrd.utils_cxt.beyondChunk += 50;

    if (flag) {
        if (type == MEM_THRD)
            write_stderr(
                "----debug_query_id=%lu, memory allocation failed due to reaching the database memory limitation."
                " Current thread is consuming about %u MB, allocating %ld bytes.\n",
                u_sess->debug_query_id,
                (uint32)t_thrd.utils_cxt.trackedMemChunks << (chunkSizeInBits - BITS_IN_MB),
                sz);
        else
            write_stderr(
                "----debug_query_id=%lu, memory allocation failed due to reaching the database memory limitation."
                " Current session is consuming about %u MB, allocating %ld bytes.\n",
                u_sess->debug_query_id,
                (uint32)u_sess->stat_cxt.trackedMemChunks << (chunkSizeInBits - BITS_IN_MB),
                sz);
    } else {
        if (type == MEM_THRD)
            write_stderr(
                "----debug_query_id=%lu, FATAL: memory allocation failed due to reaching the OS memory limitation."
                " Current thread is consuming about %u MB, allocating %ld bytes.\n",
                u_sess->debug_query_id,
                (uint32)t_thrd.utils_cxt.trackedMemChunks << (chunkSizeInBits - BITS_IN_MB),
                sz);
        else
            write_stderr(
                "----debug_query_id=%lu, FATAL: memory allocation failed due to reaching the OS memory limitation."
                " Current session is consuming about %u MB, allocating %ld bytes.\n",
                u_sess->debug_query_id,
                (uint32)u_sess->stat_cxt.trackedMemChunks << (chunkSizeInBits - BITS_IN_MB),
                sz);
        write_stderr("----debug_query_id=%lu, Please check the sysctl configuration and GUC variable "
                     "g_instance.attr.attr_memory.max_process_memory.\n",
            u_sess->debug_query_id);
    }
}

static bool MemoryIsNotEnough(int32 currentMem, int32 maxMem, bool needProtect)
{
    if (currentMem < maxMem) {
        return false;
    }

    /* this memory limit check already be mask off */
    if ((u_sess && u_sess->attr.attr_memory.disable_memory_protect) ||
        !t_thrd.utils_cxt.memNeedProtect || !needProtect) {
        return false;
    }

    /* this malloc could not be prevented */
    if ((t_thrd.int_cxt.CritSectionCount != 0) ||
        (AmPostmasterProcess()) || t_thrd.xact_cxt.bInAbortTransaction) {
        return false;
    }

    if (currentMem >= (maxMem + t_thrd.utils_cxt.beyondChunk)) {
        write_stderr("ERROR: memory alloc failed. Current role is: %d, maxMem is: %d, currMem is: %d.\n",
            t_thrd.role, maxMem, currentMem);
        return true;
    }

    return false;
}

/*
 * Reserve num of chunks for current thread. The reservation has to be
 * valid against query level memory quota.
 */
template <MemType type>
static bool memTracker_ReserveMemChunks(int32 numChunksToReserve, bool needProtect)
{
    int32 total = 0;
    volatile int32 *currSize = NULL;
    int32 *maxSize = NULL;

    Assert(0 < numChunksToReserve);

    /* query level memory verification */
    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry && type != MEM_SHRD) {
        /* 1. increase memory in chunk at query level */
        total = gs_atomic_add_32(&(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks), numChunksToReserve);

        /* 2. update the peak memory of the query */
        if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery < total)
            gs_lock_test_and_set(&(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery), total);
    }

    /* increase chunk quota at global gaussdb process level */
    if (t_thrd.utils_cxt.backend_reserved) {
        currSize = &backendUsedMemInChunk;
        maxSize = &backendReservedMemInChunk;
    } else {
        currSize = &processMemInChunks;
        maxSize = &maxChunksPerProcess;
    }
    total = pg_atomic_add_fetch_u32((volatile uint32*)currSize, numChunksToReserve);

    /* Query memory quota is exhausted. Reset the counter then return false. */
    if (MemoryIsNotEnough(total, *maxSize, needProtect)) {
        (void)pg_atomic_sub_fetch_u32((volatile uint32*)currSize, numChunksToReserve);
        return false;
    }

    /* no longer to use the beyond chunk, so reset it */
    if (total < *maxSize) {
        t_thrd.utils_cxt.beyondChunk = 0;
    }

    if (peakChunksPerProcess < processMemInChunks + backendUsedMemInChunk) {
        peakChunksPerProcess = processMemInChunks + backendUsedMemInChunk;

        /*Print memory alarm information every 1 minute.*/
        int threshold = PROCMEM_HIGHWATER_THRESHOLD >> (chunkSizeInBits - BITS_IN_MB);
        if ((backendUsedMemInChunk + processMemInChunks) >
            (backendReservedMemInChunk + maxChunksPerProcess + threshold)) {
            TimestampTz current = GetCurrentTimestamp();
            if (u_sess != NULL && TimestampDifferenceExceeds(last_print_timestamp, current, 60000)) {
                uint32 processMemMB =
                    (uint32)(backendUsedMemInChunk + processMemInChunks) << (chunkSizeInBits - BITS_IN_MB);
                uint32 reserveMemMB = (uint32)numChunksToReserve << (chunkSizeInBits - BITS_IN_MB);
                write_stderr("WARNING: process memory allocation %u MB, pid %lu, "
                             "thread self memory %ld bytes, new %u bytes allocated, statement(%s).\n",
                    processMemMB, t_thrd.proc_cxt.MyProcPid, t_thrd.utils_cxt.trackedBytes, reserveMemMB,
                    (t_thrd.postgres_cxt.debug_query_string != NULL) ? t_thrd.postgres_cxt.debug_query_string : "NULL");

                last_print_timestamp = current;
            }
        }
    }

    return true;
}

/*
 * Reserve requestedBytes from the memory tracking system.
 *
 * We use BITS_IN_MB sized chunk as the unit to reserve the
 * memory for newly requested bytes for performance reason.
 * If the newly requested bytes can fit into the previous
 * reserved chunk, it does not reserve a new chunk.
 */
template <MemType type>
bool memTracker_ReserveMem(int64 requestedBytes, bool needProtect)
{
    bool status = true;
    int64 tb = 0;
    int32 tc = 0;
    int32 needChunk = 0;

    if (type == MEM_SHRD) {
        tc = (uint64)shareTrackedBytes >> chunkSizeInBits;
        tb = gs_atomic_add_64(&shareTrackedBytes, requestedBytes);
        gs_lock_test_and_set(&shareTrackedMemChunks, tc); /* reset the value */
    } else if (type == MEM_THRD) {
        tc = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.utils_cxt.trackedBytes += requestedBytes;
        tb = t_thrd.utils_cxt.trackedBytes;
    } else {
        tc = u_sess->stat_cxt.trackedMemChunks;
        u_sess->stat_cxt.trackedBytes += requestedBytes;
        tb = u_sess->stat_cxt.trackedBytes;
    }

    // How many chunks we need so far
    if (type != MEM_SHRD) {
        tb = tb + eliminateErrorsMemoryBytes;
    }
    int32 newszChunk = (uint64)tb >> chunkSizeInBits;

    if (newszChunk > tc) {
        needChunk = newszChunk - tc;

        status = memTracker_ReserveMemChunks<type>(needChunk, needProtect);
    }

    if (status == false) {
        if (type == MEM_SHRD)
            gs_atomic_add_64(&shareTrackedBytes, -requestedBytes);
        else if (type == MEM_THRD)
            t_thrd.utils_cxt.trackedBytes -= requestedBytes;
        else
            u_sess->stat_cxt.trackedBytes -= requestedBytes;
    } else {
        if (type == MEM_SHRD) {
            (void)pg_atomic_add_fetch_u32((volatile uint32*)&shareTrackedMemChunks, needChunk);
            if (shareTrackedMemChunks > peakChunksSharedContext)
                peakChunksSharedContext = shareTrackedMemChunks;
        } else {
            if (type == MEM_THRD)
                t_thrd.utils_cxt.trackedMemChunks = newszChunk;
            else
                u_sess->stat_cxt.trackedMemChunks = newszChunk;
            if (needChunk != 0) {
                (void)pg_atomic_add_fetch_u32((volatile uint32*)&dynmicTrackedMemChunks, needChunk);
            }

            t_thrd.utils_cxt.basedBytesInQueryLifeCycle += requestedBytes;
            if(t_thrd.utils_cxt.basedBytesInQueryLifeCycle > t_thrd.utils_cxt.peakedBytesInQueryLifeCycle)
                t_thrd.utils_cxt.peakedBytesInQueryLifeCycle = t_thrd.utils_cxt.basedBytesInQueryLifeCycle;
        }
    }

    return status;
}

/*
 * Releases "reduction" number of chunks to the query.
 */
template <MemType type>
static void memTracker_ReleaseMemChunks(int reduction)
{
    int total = 0;
    Assert(0 <= reduction);

    /* reduce chunk quota at global gaussdb process level */
    if (t_thrd.utils_cxt.backend_reserved) {
        total = pg_atomic_sub_fetch_u32((volatile uint32*)&backendUsedMemInChunk, reduction);
    } else {
        total = pg_atomic_sub_fetch_u32((volatile uint32*)&processMemInChunks, reduction);
    }

    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry && type != MEM_SHRD) {
        total = gs_atomic_add_32(&(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks), -reduction);
    }
}

/*
 * Releases requested size bytes memory.
 *
 * For performance reason this method accumulates free requests until it has
 * enough bytes to free a whole chunk.
 */
template <MemType type>
void memTracker_ReleaseMem(int64 toBeFreedRequested)
{
    int32 tc = 0;
    int64 tb = 0;

    /*
     * We need this adjustment as gaussdb may request to free more memory than it reserved, apparently
     * because a bug somewhere that tries to release memory for allocations made before the memory
     * tracking system was initialized.
     */
    if (type == MEM_SHRD)
        tb = shareTrackedBytes;
    else if (type == MEM_THRD)
        tb = t_thrd.utils_cxt.trackedBytes;
    else
        tb = u_sess->stat_cxt.trackedBytes;

    int64 toBeFreed = Min(tb, toBeFreedRequested);
    if (0 == toBeFreed) {
        return;
    }

    if (type == MEM_SHRD) {
        tc = (uint64)shareTrackedBytes >> chunkSizeInBits;
        tb = gs_atomic_add_64(&shareTrackedBytes, -toBeFreed);
        gs_lock_test_and_set(&shareTrackedMemChunks, tc); /* reset the value */
    } else if (type == MEM_THRD) {
        tc = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.utils_cxt.trackedBytes -= toBeFreed;
        t_thrd.utils_cxt.basedBytesInQueryLifeCycle -= toBeFreed;
        tb = t_thrd.utils_cxt.trackedBytes;
    } else {
        tc = u_sess->stat_cxt.trackedMemChunks;
        u_sess->stat_cxt.trackedBytes -= toBeFreed;
        tb = u_sess->stat_cxt.trackedBytes;
        t_thrd.utils_cxt.basedBytesInQueryLifeCycle -= toBeFreed;
    }

    if (type != MEM_SHRD) {
        tb = tb + eliminateErrorsMemoryBytes;
    }
    int newszChunk = (uint64)tb >> chunkSizeInBits;

    if (newszChunk < tc) {
        int reduction = tc - newszChunk;

        memTracker_ReleaseMemChunks<type>(reduction);

        if (type == MEM_SHRD)
            (void)pg_atomic_sub_fetch_u32((volatile uint32*)&shareTrackedMemChunks, reduction);
        else {
            if (type == MEM_THRD)
                t_thrd.utils_cxt.trackedMemChunks = newszChunk;
            else
                u_sess->stat_cxt.trackedMemChunks = newszChunk;
            (void)pg_atomic_sub_fetch_u32((volatile uint32*)&dynmicTrackedMemChunks, reduction);
        }

        /* reset the total value if not matching */
        int total = shareTrackedMemChunks + dynmicTrackedMemChunks;
        gs_lock_test_and_set(&processMemInChunks, total);
    }
}

// Return current memory usage in MB for query
//
int getSessionMemoryUsageMB()
{
    int used = 0;

    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry) {
        used = (unsigned int)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks -
                              t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks)
               << (chunkSizeInBits - BITS_IN_MB);
    }

    return used;
}

/*
 * Memory allocation for sz bytes. If memory quota is enabled, it uses gs_malloc_internal to
 * reserve the chunk and allocate memory.
 */
template <MemType mem_type>
void* MemoryProtectFunctions::gs_memprot_malloc(Size sz, bool needProtect)
{
    if (!t_thrd.utils_cxt.gs_mp_inited)
        return malloc(sz);

    void* ptr = NULL;
    bool status = memTracker_ReserveMem<mem_type>(sz, needProtect);

    if (status == true) {
        ptr = malloc(sz);
        if (ptr == NULL) {
            memTracker_ReleaseMem<mem_type>(sz);
            gs_memprot_failed<false>(sz, mem_type);

            return NULL;
        }

        return ptr;
    }

    gs_memprot_failed<true>(sz, mem_type);

    return NULL;
}

template <MemType mem_type>
void MemoryProtectFunctions::gs_memprot_free(void* ptr, Size sz)
{
    free(ptr);
    ptr = NULL;

    if (t_thrd.utils_cxt.gs_mp_inited)
        memTracker_ReleaseMem<mem_type>(sz);
}

/* Reallocates memory, respecting memory quota, if enabled */
template <MemType mem_type>
void* MemoryProtectFunctions::gs_memprot_realloc(void* ptr, Size sz, Size newsz, bool needProtect)
{
    Assert(GS_MP_INITED);  // Must be used when memory protect feature is avaiable

    void* ret = NULL;

    if ((newsz > 0) && memTracker_ReserveMem<mem_type>(newsz, needProtect)) {
        memTracker_ReleaseMem<mem_type>(sz);
        ret = realloc(ptr, newsz);
        if (ret == NULL) {
            memTracker_ReleaseMem<mem_type>(newsz);

            gs_memprot_failed<false>(newsz, mem_type);

            return NULL;
        }

        return ret;
    }

    gs_memprot_failed<true>(newsz, mem_type);
    return NULL;
}

/* posix_memalign interface */
template <MemType mem_type>
int MemoryProtectFunctions::gs_posix_memalign(void** memptr, Size alignment, Size sz, bool needProtect)
{
    if (!t_thrd.utils_cxt.gs_mp_inited)
        return posix_memalign(memptr, alignment, sz);

    int ret = 0;
    bool status = memTracker_ReserveMem<mem_type>(sz, needProtect);

    if (status == true) {
        ret = posix_memalign(memptr, alignment, sz);
        if (ret) {
            memTracker_ReleaseMem<mem_type>(sz);
            gs_memprot_failed<false>(sz, mem_type);

            return ret;
        }

        return ret;
    }

    gs_memprot_failed<true>(sz, mem_type);

    return ENOMEM; /* insufficient memory */
}

/**
 * reseve memory for mmap of compressed table
 * @tparam mem_type MEM_SHRD is supported only
 * @param sz reserved size(bytes)
 * @param needProtect
 * @return success or not
 */
template <MemType type>
bool MemoryProtectFunctions::gs_memprot_reserve(Size sz, bool needProtect)
{
    if (type != MEM_SHRD) {
        return false;
    }
    return memTracker_ReserveMem<type>(sz, needProtect);
}

/**
 * release the momery allocated by gs_memprot_reserve
 * @tparam type MEM_SHRD is supported only
 * @param sz free size(bytes)
 */
template <MemType type>
void MemoryProtectFunctions::gs_memprot_release(Size sz)
{
    if (type != MEM_SHRD) {
        return;
    }
    memTracker_ReleaseMem<type>(sz);
}

/* thread level initialization */
void gs_memprot_thread_init(void)
{
    /* The Process level protection has been triggered */
    if (maxChunksPerProcess) {
        t_thrd.utils_cxt.gs_mp_inited = true;
    }
}

void gs_memprot_reserved_backend(int avail_mem)
{
    int reserved_mem = 0;

    /* wal threads contains WALWRITER, WALRECEIVER, WALRECWRITE, DATARECIVER, DATARECWRITER */
    const int wal_thread_count = 5;
    int reserved_thread_count = g_instance.attr.attr_network.ReservedBackends +
                                NUM_CMAGENT_PROCS + wal_thread_count +
                                NUM_DCF_CALLBACK_PROCS +
                                g_instance.attr.attr_storage.max_wal_senders;
    /* reserve 10MB per-thread for sysadmin user */
    reserved_mem += reserved_thread_count * 10;
    ereport(LOG, (errmsg("reserved memory for backend threads is: %d MB", reserved_mem)));

    /* reserve memory for WAL sender and WAL receiver buffer */
    int64 wal_mem = 0;
    int64 data_sender_size = 1 + sizeof(DataPageMessageHeader) + WS_MAX_SEND_SIZE;
    wal_mem += data_sender_size * g_instance.attr.attr_storage.max_wal_senders;
    int64 wal_sender_size = 1 + sizeof(WalDataMessageHeader) + (int)WS_MAX_SEND_SIZE;
    wal_mem += wal_sender_size * g_instance.attr.attr_storage.max_wal_senders;
    int64 wal_receiver_size = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    wal_mem += offsetof(WalRcvCtlBlock, walReceiverBuffer) + wal_receiver_size;
    ereport(LOG, (errmsg("reserved memory for WAL buffers is: %ld MB", wal_mem >> BITS_IN_MB)));

    reserved_mem += (uint64)wal_mem >> BITS_IN_MB;

    backendReservedMemInChunk = reserved_mem;
    maxChunksPerProcess = ((unsigned int)avail_mem >> BITS_IN_KB) - reserved_mem;
    ereport(LOG, (errmsg("Set max backend reserve memory is: %d MB, max dynamic memory is: %d MB",
        backendReservedMemInChunk, maxChunksPerProcess)));
}

/* process level initialization only called in postmaster main thread */
void gs_memprot_init(Size size)
{
    if (g_instance.attr.attr_memory.enable_memory_limit && maxChunksPerProcess == 0) {
        maxSharedMemory = size >> BITS_IN_MB;

        Assert(g_instance.attr.attr_sql.udf_memory_limit >= UDF_DEFAULT_MEMORY);
        /* remove cstore buffer */
        int avail_mem = g_instance.attr.attr_memory.max_process_memory - g_instance.attr.attr_storage.cstore_buffers -
                        (g_instance.attr.attr_sql.udf_memory_limit - UDF_DEFAULT_MEMORY) - (size >> BITS_IN_KB);

        if (avail_mem < MIN_PROCESS_LIMIT) {
            ereport(WARNING,
                (errmsg(
                    "Failed to initialize the memory protect for "
                    "g_instance.attr.attr_storage.cstore_buffers (%d Mbytes) or shared memory (%lu Mbytes) is larger.",
                    g_instance.attr.attr_storage.cstore_buffers >> BITS_IN_KB,
                    size >> BITS_IN_MB)));
            backendReservedMemInChunk = 0;
            maxChunksPerProcess = 0;
            return;
        }

        gs_memprot_reserved_backend(avail_mem);

        /* Convert to MB unit */
        comm_original_memory = (g_instance.attr.attr_network.comm_usable_memory >> BITS_IN_KB);

        ereport(LOG,
            (errmsg("shared memory %lu Mbytes, memory context %d Mbytes, max process memory %d Mbytes",
                (size >> BITS_IN_MB),
                (maxChunksPerProcess + backendReservedMemInChunk),
                (g_instance.attr.attr_memory.max_process_memory >> BITS_IN_KB))));

        /* while dynamic memory is less than 2GB, set memory protect on may cause instance cannot run */
        if (maxChunksPerProcess < (MIN_PROCESS_LIMIT >> BITS_IN_KB)) {
            backendReservedMemInChunk = 0;
            maxChunksPerProcess = 0;
            t_thrd.utils_cxt.gs_mp_inited = false;
        } else {
            /* The following memoryContext from Postmaster can be protected */
            t_thrd.utils_cxt.gs_mp_inited = true;
        }
    }
}

/* Recompute how many memory remain except used by simsearch. */
void gs_memprot_process_gpu_memory(uint32 size)
{
    uint32 i = size >> (chunkSizeInBits - BITS_IN_MB);

    if (t_thrd.utils_cxt.gs_mp_inited == true) {
        uint32 remain_size = maxChunksPerProcess >> 1;
        if (remain_size < i) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("Failed to initilize the memory(%uM) of search server, "
                           "maybe it exceed the half of maxChunksPerProcess(%dM).",
                        i,
                        maxChunksPerProcess)));
        }

        maxChunksPerProcess -= i;
    }
}
