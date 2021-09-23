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
 * mm_cfg.h
 *    Memory configuration interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_cfg.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_CFG_H
#define MM_CFG_H

#include "mm_def.h"
#include "string_buffer.h"
#include "utilities.h"

namespace MOT {
struct MemCfg {
    // HW/OS configuration
    uint32_t m_nodeCount;
    uint32_t m_cpuCount;  // total, not per node
    uint32_t m_maxThreadCount;
    uint32_t m_maxConnectionCount;
    uint32_t m_maxThreadsPerNode;  // derived

    // chunk directory
    uint32_t m_lazyLoadChunkDirectory;

    // memory limits
    uint64_t m_maxGlobalMemoryMb;
    uint64_t m_minGlobalMemoryMb;
    uint64_t m_maxLocalMemoryMb;
    uint64_t m_minLocalMemoryMb;
    uint64_t m_maxSessionMemoryKb;
    uint64_t m_minSessionMemoryKb;
    MemReserveMode m_reserveMemoryMode;
    MemStorePolicy m_storeMemoryPolicy;

    // chunk pool configuration
    MemAllocPolicy m_chunkAllocPolicy;
    uint32_t m_chunkPreallocWorkerCount;
    uint32_t m_highRedMarkPercent;

    // session large buffer store
    uint64_t m_sessionLargeBufferStoreSizeMb;
    uint64_t m_sessionLargeBufferStoreMaxObjectSizeMb;
    uint64_t m_sessionMaxHugeObjectSizeMb;
};

/**
 * @brief Load and initialize configuration.
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemCfgInit();

/**
 * @brief Cleanup all resources related to MM layer configuration.
 */
extern void MemCfgDestroy();

/** @var A global immutable instance of memory management configuration. */
extern MemCfg g_memGlobalCfg;

/**
 * @brief Prints global memory configuration into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 */
extern void MemCfgPrint(const char* name, LogLevel logLevel);

/**
 * @brief Dumps global memory configuration into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 */
extern void MemCfgToString(int indent, const char* name, StringBuffer* stringBuffer);

/**
 * @brief Retrieves the total system memory in megabytes.
 * @return The total system memory in megabytes, or zero if failed.
 */
extern uint64_t GetTotalSystemMemoryMb();

/**
 * @brief Retrieves the available system memory in mega-bytes.
 * @return The available system memory in megabytes, or zero if failed.
 */
extern uint64_t GetAvailableSystemMemoryMb();
}  // namespace MOT

/**
 * @brief Dumps global memory configuration into standard error stream.
 */
extern "C" void MemCfgDump();

#endif /* MM_CFG_H */
