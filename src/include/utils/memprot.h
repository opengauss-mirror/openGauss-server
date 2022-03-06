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
 * ---------------------------------------------------------------------------------------
 * 
 * memprot.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/memprot.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MEMPROT_H
#define MEMPROT_H

#include "c.h"
#include "utils/aset.h"

#define BITS_IN_MB 20
#define BITS_IN_KB 10

#define MIN_PROCESS_LIMIT (2 * 1024 * 1024)  // 2GB
#define MEMPROT_INIT_SIZE 200                // 200MB for initialization memory

#define CHUNKS_TO_MB(chunks) ((chunks) << (chunkSizeInBits - BITS_IN_MB))
#define MB_TO_CHUNKS(mb) ((mb) >> (chunkSizeInBits - BITS_IN_MB))
#define CHUNKS_TO_BYTES(chunks) (((int64)chunks) << chunkSizeInBits)
#define BYTES_TO_CHUNKS(bytes) ((bytes) >> chunkSizeInBits)
#define BYTES_TO_MB(bytes) ((bytes) >> BITS_IN_MB)

#define HIGH_PROCMEM_MARK 80  // 80% of total node memory
#define LOW_PROCMEM_MARK 60   // 60% of total node memory

#define LOW_WORKMEM_CHUNK 256

#define MAX_MEMORY_FAULT_PERCENT (INT_MAX)

#define SELF_SHARED_MEMCTX_LIMITATION (100 * 1024 * 1024)  // 100MB
#define SELF_GENRIC_MEMCTX_LIMITATION (10 * 1024 * 1024)   // 10MB
#define MAX_SHARED_MEMCTX_LIMITATION (2 * 1024)            // 2GB
#define MAX_GENRIC_MEMCTX_LIMITATION (100 * 1024 * 1024)   // 100MB
#define MAX_SHARED_MEMCTX_SIZE (10 * 1024)                 // 10GB

#define MAX_COMM_USED_SIZE (4 * 1024)  // 4GB
#define SELF_QUERY_LIMITATION (300)    // 300MB

#define PROCMEM_HIGHWATER_THRESHOLD (5 * 1024)  // 5G

/* 200* 1024 kB */
/* Must be same as UDF_DEFAULT_MEMORY in agent_main.cpp and udf_memory_limit in cluster_guc.conf */
#define UDF_DEFAULT_MEMORY (200 * 1024)

extern unsigned int chunkSizeInBits;

extern int32 maxChunksPerProcess;
extern volatile int32 processMemInChunks;
extern int32 peakChunksPerProcess;
extern volatile int32 shareTrackedMemChunks;
extern int32 peakChunksSharedContext;
extern int32 maxChunksPerQuery;
extern int32 comm_original_memory;
extern int32 maxSharedMemory;
extern volatile int32 dynmicTrackedMemChunks;
extern int64 storageTrackedBytes;
extern int32 backendReservedMemInChunk;
extern volatile int32 backendUsedMemInChunk;

/* functions from memprot.cpp */
#ifdef MEMORY_CONTEXT_CHECKING
extern bool gs_memory_enjection(void);
#endif

extern bool gs_sysmemory_busy(int64 used, bool strict);

extern bool gs_sysmemory_avail(int64 requestedBytes);

extern void gs_memprot_thread_init(void);

extern void gs_memprot_init(Size size);

extern void gs_memprot_process_gpu_memory(uint32 size);

extern void gs_memprot_reset_beyondchunk(void);

#define GS_MEMPROT_MALLOC(sz, needProtect) MemoryProtectFunctions::gs_memprot_malloc<MEM_THRD>(sz, needProtect)
#define GS_MEMPROT_FREE(ptr, sz) MemoryProtectFunctions::gs_memprot_free<MEM_THRD>(ptr, sz)
#define GS_MEMPROT_REALLOC(ptr, sz, newsz, needProtect) MemoryProtectFunctions::gs_memprot_realloc<MEM_THRD>(ptr, sz, newsz, needProtect)
#define GS_MEMPROT_MEMALIGN(ptr, align, sz, needProtect) MemoryProtectFunctions::gs_posix_memalign<MEM_THRD>(ptr, align, sz, needProtect)
#define GS_MEMPROT_SHARED_MALLOC(sz) MemoryProtectFunctions::gs_memprot_malloc<MEM_SHRD>(sz)
#define GS_MEMPROT_SHARED_FREE(ptr, sz) MemoryProtectFunctions::gs_memprot_free<MEM_SHRD>(ptr, sz)

extern int getSessionMemoryUsageMB();

#endif
