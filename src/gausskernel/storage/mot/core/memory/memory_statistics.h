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
 * memory_statistics.h
 *    Memory statistics implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/memory_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MEMORY_STATISTICS_H
#define MEMORY_STATISTICS_H

#include "memory_statistic_variable.h"
#include "frequency_statistic_variable.h"
#include "iconfig_change_listener.h"
#include "numeric_statistic_variable.h"
#include "statistics_provider.h"
#include "stats/frequency_statistic_variable.h"
#include "typed_statistics_generator.h"

#include "mm_def.h"
#include "mm_buffer_class.h"

namespace MOT {
/** @brief Provide statistics in higher resolution. */
class DetailedMemoryThreadStatistics : public ThreadStatistics {
public:
    explicit DetailedMemoryThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    virtual ~DetailedMemoryThreadStatistics()
    {}

    /** @brief Updates the memory statistics for global-memory chunks on a specific node. */
    inline void AddGlobalChunksUsedNode(int node, int64_t bytes)
    {
        m_globalChunksUsedNode[node].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for local-memory chunks on a specific node. */
    inline void AddLocalChunksUsedNode(int node, int64_t bytes)
    {
        m_localChunksUsedNode[node].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for global-memory buffers on a specific node. */
    inline void AddGlobalBuffersUsedNode(int node, int64_t bytes)
    {
        m_globalBuffersUsedNode[node].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for global-memory buffers of specific size class on a specific node. */
    inline void AddGlobalBuffersUsed(int node, MemBufferClass bufferClass, int64_t bytes)
    {
        m_globalBuffersUsed[node][bufferClass].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for local-memory buffers on a specific node. */
    inline void AddLocalBuffersUsedNode(int node, int64_t bytes)
    {
        m_localBuffersUsedNode[node].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for local-memory buffers of specific size class on a specific node. */
    inline void AddLocalBuffersUsed(int node, MemBufferClass bufferClass, int64_t bytes)
    {
        m_localBuffersUsed[node][bufferClass].AddSample(bytes);
    }

    /** @brief Updates the memory statistics for global requested bytes. */
    inline void AddGlobalBytesRequested(int64_t bytes)
    {
        m_globalBytesRequested.AddSample(bytes);
    }

    /** @brief Updates the memory statistics for local-session requested bytes. */
    inline void AddSessionBytesRequested(int64_t bytes)
    {
        m_sessionBytesRequested.AddSample(bytes);
    }

private:
    // global chunks - total on each node
    MemoryStatisticVariable m_globalChunksUsedNode[MEM_MAX_NUMA_NODES];

    // local chunks - total on each node
    MemoryStatisticVariable m_localChunksUsedNode[MEM_MAX_NUMA_NODES];

    // global buffers - total on each node
    MemoryStatisticVariable m_globalBuffersUsedNode[MEM_MAX_NUMA_NODES];

    // global buffers - rate per node per buffer class
    MemoryStatisticVariable m_globalBuffersUsed[MEM_MAX_NUMA_NODES][MEM_BUFFER_CLASS_COUNT];

    // local buffers - total on each node
    MemoryStatisticVariable m_localBuffersUsedNode[MEM_MAX_NUMA_NODES];

    // local buffers - rate per node per buffer class
    MemoryStatisticVariable m_localBuffersUsed[MEM_MAX_NUMA_NODES][MEM_BUFFER_CLASS_COUNT];

    // global bytes requested
    MemoryStatisticVariable m_globalBytesRequested;

    // session bytes requested
    MemoryStatisticVariable m_sessionBytesRequested;
};

class DetailedMemoryGlobalStatistics : public GlobalStatistics {
public:
    explicit DetailedMemoryGlobalStatistics(GlobalStatistics::NamingScheme namingScheme);

    virtual ~DetailedMemoryGlobalStatistics()
    {}

    /** @brief Updates the statistics for total bytes allocated from kernel for local memory usage. */
    inline void AddNumaLocalAllocated(int node, int64_t bytes)
    {
        m_numaLocalAllocated[node].AddSample(bytes);
    }

    /** @brief Updates the statistics for total bytes reserved in global chunk pools. */
    inline void AddGlobalChunksReserved(int node, int64_t bytes)
    {
        m_globalChunksReserved[node].AddSample(bytes);
    }

    /** @brief Updates the statistics for total bytes reserved in local chunk pools. */
    inline void AddLocalChunksReserved(int node, int64_t bytes)
    {
        m_localChunksReserved[node].AddSample(bytes);
    }

private:
    MemoryStatisticVariable m_numaLocalAllocated[MEM_MAX_NUMA_NODES];
    MemoryStatisticVariable m_globalChunksReserved[MEM_MAX_NUMA_NODES];
    MemoryStatisticVariable m_localChunksReserved[MEM_MAX_NUMA_NODES];
};

class MemoryThreadStatistics : public ThreadStatistics {
public:
    explicit MemoryThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    virtual ~MemoryThreadStatistics()
    {}

    /** @brief Updates the memory statistics for total global-memory chunks on all nodes. */
    inline void AddGlobalChunksUsed(int64_t bytes)
    {
        m_globalChunksUsed.AddSample(bytes);
    }

    /** @brief Updates the memory statistics for total local-memory chunks on all nodes. */
    inline void AddLocalChunksUsed(int64_t bytes)
    {
        m_localChunksUsed.AddSample(bytes);
    }

    /** @brief Updates the memory statistics for total global-memory buffers used on all nodes. */
    inline void AddGlobalBuffersUsed(int64_t bytes)
    {
        m_globalBuffersUsed.AddSample(bytes);
    }

    /** @brief Updates the memory statistics for total local-memory buffers used on all nodes. */
    inline void AddLocalBuffersUsed(int64_t bytes)
    {
        m_localBuffersUsed.AddSample(bytes);
    }

    /** @brief Updates the statistics for total global allocation bytes used. */
    inline void AddGlobalBytesUsed(int64_t bytes)
    {
        m_globalBytesUsed.AddSample(bytes);
    }

    /** @brief Updates the statistics for total session-local allocation bytes used. */
    inline void AddSessionBytesUsed(int64_t bytes)
    {
        m_sessionBytesUsed.AddSample(bytes);
    }

    /** @brief Updates the statistics for time spent in malloc() (only when @ref MEM_ACTIVE is undefined). */
    inline void AddMallocTime(uint64_t nanos)
    {
        m_mallocTime.AddSample(nanos);
    }

    /** @brief Updates the statistics for time spent in free() (only when @ref MEM_ACTIVE is undefined). */
    inline void AddFreeTime(uint64_t nanos)
    {
        m_freeTime.AddSample(nanos);
    }

    /** @brief Updates the statistics for the amount of bytes retired into the garbage collector. */
    inline void AddGCRetiredBytes(uint64_t bytes)
    {
        m_gcRetiredBytes.AddSample(bytes);
    }

    /** @brief Updates the statistics for the amount of bytes reclaimed by the garbage collector. */
    inline void AddGCReclaimedBytes(uint64_t bytes)
    {
        m_gcReclaimedBytes.AddSample(bytes);
    }

    /** @brief Updates the statistics for the amount of bytes used by masstree index structures. */
    inline void AddMasstreeBytesUsed(uint64_t bytes)
    {
        m_masstreeBytesUsed.AddSample(bytes);
    }

private:
    // global/local chunks
    MemoryStatisticVariable m_globalChunksUsed;
    MemoryStatisticVariable m_localChunksUsed;

    // global buffers
    MemoryStatisticVariable m_globalBuffersUsed;
    MemoryStatisticVariable m_localBuffersUsed;

    // global objects
    MemoryStatisticVariable m_globalBytesUsed;

    // session-local objects
    MemoryStatisticVariable m_sessionBytesUsed;

    // total time spent in malloc/free
    NumericStatisticVariable m_mallocTime;
    NumericStatisticVariable m_freeTime;

    // GC statistics
    MemoryStatisticVariable m_gcRetiredBytes;
    MemoryStatisticVariable m_gcReclaimedBytes;

    // masstree statistics
    MemoryStatisticVariable m_masstreeBytesUsed;
};

class MemoryGlobalStatistics : public GlobalStatistics {
public:
    explicit MemoryGlobalStatistics(GlobalStatistics::NamingScheme namingScheme);

    virtual ~MemoryGlobalStatistics()
    {}

    /** @brief Updates the statistics for total NUMA-interleaved bytes allocated from kernel. */
    inline void AddNumaInterleavedAllocated(int64_t bytes)
    {
        return m_numaInterleavedAllocated.AddSample(bytes);
    }

    /** @brief Updates the statistics for total NUMA-local bytes allocated from kernel on some node. */
    inline void AddNumaLocalAllocated(int64_t bytes)
    {
        return m_numaLocalAllocated.AddSample(bytes);
    }

    /** @brief Updates the statistics for total bytes reserved in global chunk pools. */
    inline void AddGlobalChunksReserved(int64_t bytes)
    {
        return m_globalChunksReserved.AddSample(bytes);
    }

    /** @brief Updates the statistics for total bytes reserved in local chunk pools. */
    inline void AddLocalChunksReserved(int64_t bytes)
    {
        return m_localChunksReserved.AddSample(bytes);
    }

private:
    MemoryStatisticVariable m_numaInterleavedAllocated;
    MemoryStatisticVariable m_numaLocalAllocated;
    MemoryStatisticVariable m_globalChunksReserved;
    MemoryStatisticVariable m_localChunksReserved;
};

class DetailedMemoryStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
private:
    DetailedMemoryStatisticsProvider();
    virtual ~DetailedMemoryStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

public:
    /**
     * @brief Creates singleton instance. Must be called once during engine startup.
     * @return true if succeeded otherwise false.
     */
    static bool CreateInstance();

    /**
     * @brief Destroys singleton instance. Must be called once during engine shutdown.
     */
    static void DestroyInstance();

    /**
     * @brief Retrieves reference to singleton instance.
     * @return Database session statistics provider
     */
    static DetailedMemoryStatisticsProvider& GetInstance();

    /** @brief Updates the statistics for total bytes allocated from kernel for local memory usage. */
    inline void AddNumaLocalAllocated(int node, int64_t bytes)
    {
        DetailedMemoryGlobalStatistics* mgs = GetGlobalStatistics<DetailedMemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddNumaLocalAllocated(node, bytes);
        }
    }

    /** @brief Updates the statistics for total bytes reserved in global chunk pools. */
    inline void AddGlobalChunksReserved(int node, int64_t bytes)
    {
        DetailedMemoryGlobalStatistics* mgs = GetGlobalStatistics<DetailedMemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddGlobalChunksReserved(node, bytes);
        }
    }

    /** @brief Updates the statistics for total bytes reserved in local chunk pools. */
    inline void AddLocalChunksReserved(int node, int64_t bytes)
    {
        DetailedMemoryGlobalStatistics* mgs = GetGlobalStatistics<DetailedMemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddLocalChunksReserved(node, bytes);
        }
    }

    /** @brief Updates the memory statistics for global-memory chunks on a specific node. */
    inline void AddGlobalChunksUsed(int node, int64_t bytes)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            mts->AddGlobalChunksUsedNode(node, bytes);
        }
    }

    /** @brief Updates the memory statistics for local-memory chunks on a specific node. */
    inline void AddLocalChunksUsed(int node, int64_t bytes)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            mts->AddLocalChunksUsedNode(node, bytes);
        }
    }

    /** @brief Updates the memory statistics for global-memory buffers of specific size class on a specific node. */
    inline void AddGlobalBuffersUsed(int node, MemBufferClass bufferClass)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddGlobalBuffersUsedNode(node, bytes);
            mts->AddGlobalBuffersUsed(node, bufferClass, bytes);
        }
    }

    /** @brief Updates the memory statistics for global-memory buffers of specific size class on a specific node. */
    inline void AddGlobalBuffersFreed(int node, MemBufferClass bufferClass)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddGlobalBuffersUsedNode(node, -bytes);
            mts->AddGlobalBuffersUsed(node, bufferClass, -bytes);
        }
    }

    /** @brief Updates the memory statistics for local-memory buffers of specific size class on a specific node. */
    inline void AddLocalBuffersUsed(int node, MemBufferClass bufferClass)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddLocalBuffersUsedNode(node, bytes);
            mts->AddLocalBuffersUsed(node, bufferClass, bytes);
        }
    }

    /** @brief Updates the memory statistics for local-memory buffers of specific size class on a specific node. */
    inline void AddLocalBuffersFreed(int node, MemBufferClass bufferClass)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddLocalBuffersUsedNode(node, -bytes);
            mts->AddLocalBuffersUsed(node, bufferClass, -bytes);
        }
    }

    /** @brief Updates the memory statistics for global requested bytes. */
    inline void AddGlobalBytesRequested(int64_t bytes)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            mts->AddGlobalBytesRequested(bytes);
        }
    }

    /** @brief Updates the memory statistics for local-session requested bytes. */
    inline void AddSessionBytesRequested(int64_t bytes)
    {
        DetailedMemoryThreadStatistics* mts = GetCurrentThreadStatistics<DetailedMemoryThreadStatistics>();
        if (mts) {
            mts->AddSessionBytesRequested(bytes);
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    void OnConfigChange() override;

public:  // special case for performance reasons: allow direct access to singleton
    /** @var The single instance. */
    static DetailedMemoryStatisticsProvider* m_provider;

private:
    static TypedStatisticsGenerator<DetailedMemoryThreadStatistics, DetailedMemoryGlobalStatistics> m_generator;
};

class MemoryStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
private:
    MemoryStatisticsProvider();
    virtual ~MemoryStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

public:
    /**
     * @brief Creates singleton instance. Must be called once during engine startup.
     * @return true if succeeded otherwise false.
     */
    static bool CreateInstance();

    /**
     * @brief Destroys singleton instance. Must be called once during engine shutdown.
     */
    static void DestroyInstance();

    /**
     * @brief Retrieves reference to singleton instance.
     * @return Database session statistics provider
     */
    static MemoryStatisticsProvider& GetInstance();

    /** @brief Updates the statistics for total NUMA-interleaved bytes allocated from kernel. */
    inline void AddNumaInterleavedAllocated(int64_t bytes)
    {
        MemoryGlobalStatistics* mgs = GetGlobalStatistics<MemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddNumaInterleavedAllocated(bytes);
        }
    }

    /** @brief Updates the statistics for total NUMA-local bytes allocated from kernel on some node. */
    inline void AddNumaLocalAllocated(int64_t bytes)
    {
        MemoryGlobalStatistics* mgs = GetGlobalStatistics<MemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddNumaLocalAllocated(bytes);
        }
    }

    /** @brief Updates the statistics for total bytes reserved in global chunk pools. */
    inline void AddGlobalChunksReserved(int64_t bytes)
    {
        MemoryGlobalStatistics* mgs = GetGlobalStatistics<MemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddGlobalChunksReserved(bytes);
        }
    }

    /** @brief Updates the statistics for total bytes reserved in local chunk pools. */
    inline void AddLocalChunksReserved(int64_t bytes)
    {
        MemoryGlobalStatistics* mgs = GetGlobalStatistics<MemoryGlobalStatistics>();
        if (mgs) {
            mgs->AddLocalChunksReserved(bytes);
        }
    }

    /** @brief Updates the memory statistics for total global-memory chunks on all nodes. */
    inline void AddGlobalChunksUsed(int64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddGlobalChunksUsed(bytes);
        }
    }

    /** @brief Updates the memory statistics for total local-memory chunks on all nodes. */
    inline void AddLocalChunksUsed(int64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddLocalChunksUsed(bytes);
        }
    }

    /** @brief Updates the memory statistics for total global-memory buffers used on all nodes. */
    inline void AddGlobalBuffersUsed(MemBufferClass bufferClass)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddGlobalBuffersUsed(bytes);
        }
    }

    /** @brief Updates the memory statistics for total global-memory buffers used on all nodes. */
    inline void AddGlobalBuffersFreed(MemBufferClass bufferClass)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddGlobalBuffersUsed(-bytes);
        }
    }

    /** @brief Updates the memory statistics for total local-memory buffers used on all nodes. */
    inline void AddLocalBuffersUsed(MemBufferClass bufferClass)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddLocalBuffersUsed(bytes);
        }
    }

    /** @brief Updates the memory statistics for total local-memory buffers used on all nodes. */
    inline void AddLocalBuffersFreed(MemBufferClass bufferClass)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            int64_t bytes = K2B((int64_t)MemBufferClassToSizeKb(bufferClass));
            mts->AddLocalBuffersUsed(-bytes);
        }
    }

    /** @brief Updates the statistics for total global allocation bytes used. */
    inline void AddGlobalBytesUsed(int64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddGlobalBytesUsed(bytes);
        }
    }

    /** @brief Updates the statistics for total session-local allocation bytes used. */
    inline void AddSessionBytesUsed(int64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddSessionBytesUsed(bytes);
        }
    }

    /** @brief Updates the statistics for time spent in malloc() (only when @ref MEM_ACTIVE is undefined). */
    inline void AddMallocTime(uint64_t nanos)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddMallocTime(nanos);
        }
    }

    /** @brief Updates the statistics for time spent in free() (only when @ref MEM_ACTIVE is undefined). */
    inline void AddFreeTime(uint64_t nanos)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddFreeTime(nanos);
        }
    }

    /** @brief Updates the statistics for the amount of bytes used by masstree index structures. */
    inline void AddMasstreeBytesUsed(int64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddMasstreeBytesUsed(bytes);
        }
    }

    /** @brief Updates the statistics for the amount of bytes retired into the garbage collector. */
    inline void AddGCRetiredBytes(uint64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddGCRetiredBytes(bytes);
        }
    }

    /** @brief Updates the statistics for the amount of bytes reclaimed by the garbage collector. */
    inline void AddGCReclaimedBytes(uint64_t bytes)
    {
        MemoryThreadStatistics* mts = GetCurrentThreadStatistics<MemoryThreadStatistics>();
        if (mts) {
            mts->AddGCReclaimedBytes(bytes);
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    void OnConfigChange() override;

public:  // special case for performance reasons: allow direct access to singleton
    /** @var The single instance. */
    static MemoryStatisticsProvider* m_provider;

private:
    static TypedStatisticsGenerator<MemoryThreadStatistics, MemoryGlobalStatistics> m_generator;

protected:
    /**
     * @brief Print current memory status summary.
     */
    virtual void PrintStatisticsEx();
};
}  // namespace MOT

#endif /* MEMORY_STATISTICS_H */
