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
 * mot_configuration.h
 *    Holds global configuration for the MOT storage engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/mot_configuration.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_CONFIGURATION_H
#define MOT_CONFIGURATION_H

#include <set>
#include <climits>

#include "affinity.h"
#include "global.h"
#include "iconfig_change_listener.h"
#include "logger_type.h"
#include "utilities.h"
#include "redo_log_handler_type.h"
#include "index_defs.h"
#include "mm_def.h"
#include "mot_error.h"

namespace MOT {
/** @typedef Mapping from CPU identifier to NUMA node identifier. */
typedef std::map<int, int> CpuNodeMap;

typedef std::map<int, std::set<int>> CpuMap;

/**
 * @class MOTConfiguration
 * @brief Holds global configuration for the MOT storage engine.
 */
class MOTConfiguration : public IConfigChangeListener {
public:
    MOTConfiguration();
    ~MOTConfiguration();

    /** @brief Get reference to single instance of configuration class. */
    static MOTConfiguration& GetInstance()
    {
        return motGlobalConfiguration;
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange()
    {
        MOT_LOG_TRACE("Reloading configuration after change");
        LoadConfig();
    }

    /**
     * @brief Validates configuration. Call this function after loading configuration to validate
     * the loaded values are valid, not out of bounds, and not self-contradicting.
     * @return True if the configuration is valid, otherwise false.
     */
    inline bool IsValid() const
    {
        // make sure configuration is valid (i.e. has no internal conflicts or bound breaches)
        bool result = true;
        if ((GetRootError() != MOT_NO_ERROR) && (GetRootErrorSeverity() != MOT_SEVERITY_NORMAL)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to load configuration, configuration is invalid (seeing previous errors)");
            result = false;
        } else {
            // clear any previous errors
            ClearErrorStack();
        }
        return result;
    }

    /** @brief Configures the total memory used as reference for computing memory percentage values. */
    inline void SetTotalMemoryMb(uint32_t totalMemoryMb)
    {
        MOT_LOG_INFO("Configuring total memory for relative memory values to: %u MB", totalMemoryMb);
        m_totalMemoryMb = totalMemoryMb;
    }

    void SetPgNodes(uint16_t myId, uint16_t maxNodes)
    {
        m_dataNodeId = myId;
        m_maxDataNodes = maxNodes;
    }

    /**
     * @brief Helper function for setting a configuration item while parsing it.
     * @param name The configuration item name.
     * @param value The configuration item value.
     * @return True if configuration was set successfully, otherwise false.
     */
    bool SetFlag(const std::string& name, const std::string& value);

    /**********************************************************************/
    // Redo log configuration
    /**********************************************************************/
    /** Enable redo log mechanism. */
    bool m_enableRedoLog;

    /** The type of logger being used (not configurable). */
    LoggerType m_loggerType;

    /** Determines the redo log handler type (not configurable, but derived). */
    RedoLogHandlerType m_redoLogHandlerType;

    /**********************************************************************/
    // Commit configuration
    /**********************************************************************/
    /** @var Enables group commit (relevant only if envelope has synchronous_commit not set to off). */
    bool m_enableGroupCommit;

    /** @var Group commit size. */
    uint64_t m_groupCommitSize;

    /** @var Timeout in micro-seconds of timed group commit flush policies. */
    uint64_t m_groupCommitTimeoutUSec;

    /**********************************************************************/
    // Checkpoint configuration
    /**********************************************************************/
    /** @var Enable checkpoint mechanism. */
    bool m_enableCheckpoint;

    /** @var Enable incremental checkpoint. */
    bool m_enableIncrementalCheckpoint;

    /** @var Checkpoint working directory.  */
    std::string m_checkpointDir;

    /** @var checkpoint segments size in bytes. */
    uint32_t m_checkpointSegThreshold;

    /** @var number of worker threads to spawn to perform checkpoint. */
    uint32_t m_checkpointWorkers;

    /** @var Do checkpoints bit validations - use it for debugging only */
    bool m_validateCheckpoint;

    /**********************************************************************/
    // Recovery configuration
    /**********************************************************************/

    /** @var Specifies the number of workers used to recover from checkpoint. */
    uint32_t m_checkpointRecoveryWorkers;

    /**********************************************************************/
    // Transaction management variables (not configurable)
    /**********************************************************************/
    // OCC commit configuration
    bool m_abortBufferEnable;
    bool m_preAbort;
    TxnValidation m_validationLock;

    /**********************************************************************/
    // Machine configuration (not configurable, but loaded from system info)
    /**********************************************************************/
    /** @var Number of NUMA nodes of the machine. */
    uint16_t m_numaNodes = 1;

    /** @var Number of cores per CPU in the machine. */
    uint16_t m_coresPerCpu = 8;

    /** @var ID of pg node of the machine. */
    uint16_t m_dataNodeId = 0;

    /* @var Number of pg nodes on the machine. */
    uint16_t m_maxDataNodes = 1;

    /**********************************************************************/
    // Statistics configuration
    /**********************************************************************/
    /** @brief Enables statistics printing. */
    bool m_enableStats;

    /** Statistics printing period seconds. */
    uint32_t m_statPrintPeriodSeconds;

    /** Full statistics printing period in seconds. */
    uint32_t m_statPrintFullPeriodSeconds;

    /** @var Enable statistics gathering and printing during redo log recovery. */
    bool m_enableLogRecoveryStats;

    /** @var Specifies whether enable DB session statistics printing to log. */
    bool m_enableDbSessionStatistics;

    /** @var Specifies whether enable network statistics printing to log. */
    bool m_enableNetworkStatistics;

    /** @var Specifies whether enable log statistics printing to log. */
    bool m_enableLogStatistics;

    /** @var Specifies whether enable memory statistics printing to log. */
    bool m_enableMemoryStatistics;

    /** @var Specifies whether enable detailed memory statistics printing to log. */
    bool m_enableDetailedMemoryStatistics;

    /** @var Specifies whether enable process statistics printing to log. */
    bool m_enableProcessStatistics;

    /** @var Specifies whether enable system statistics printing to log. */
    bool m_enableSystemStatistics;

    /** @var Specifies whether enable JIT execution statistics printing to log. */
    bool m_enableJitStatistics;

    /**********************************************************************/
    // Error Log configuration
    /**********************************************************************/
    /** @var Log level being used in log messages. */
    LogLevel m_logLevel;

    /** @var libnuma errors log level */
    LogLevel m_numaErrorsLogLevel;

    /** @var libnuma warnings log level */
    LogLevel m_numaWarningsLogLevel;

    /** @var Configuration startup messages log level. */
    LogLevel m_cfgStartupLogLevel;

    /**********************************************************************/
    // Memory configuration
    /**********************************************************************/
    /** @var Maximum number of threads. */
    uint16_t m_maxThreads;

    /** @var Maximum number of connections. In thread-pooled environments this value may differ from @maxThreads. */
    uint32_t m_maxConnections;

    /** @var Affinity mapping of threads to processors for user sessions. */
    AffinityMode m_sessionAffinityMode;

    /** @var Affinity mapping of threads to processors for MOT tasks. */
    AffinityMode m_taskAffinityMode;

    /** @var Specifies whether to use lazy load scheme in the global chunk directory. */
    bool m_lazyLoadChunkDirectory;

    /** @var Maximum global memory limit in megabytes. */
    uint32_t m_globalMemoryMaxLimitMB;

    /** @var Minimum global memory limit in megabytes (implies pre-allocation and minimum reservation). */
    uint32_t m_globalMemoryMinLimitMB;

    /** @var Maximum local (per-node) memory limit in megabytes. */
    uint32_t m_localMemoryMaxLimitMB;

    /** @var Minimum local (per-node) memory limit in megabytes (implies pre-allocation and minimum reservation). */
    uint32_t m_localMemoryMinLimitMB;

    /** @var Maximum for single MOT session small memory allocations. */
    uint32_t m_sessionMemoryMaxLimitKB;

    /** @var Minimum (pre-allocated) for single MOT session small memory allocations. */
    uint32_t m_sessionMemoryMinLimitKB;

    /* @var Specifies whether to reserve physical memory on startup (or just virtual). */
    MemReserveMode m_reserveMemoryMode;

    /* @var Specifies whether unused memory should be reserved or returned to kernel. */
    MemStorePolicy m_storeMemoryPolicy;

    /** @var Specifies the chunk allocation policy for the global chunk pools. */
    MemAllocPolicy m_chunkAllocPolicy;

    /** @var The number of worker threads used to allocate memory chunks for initial memory reservation. */
    uint32_t m_chunkPreallocWorkerCount;

    /** @var The maximum number of chunks allocated by the chunk store, above which an emergency mode is signaled. */
    uint32_t m_highRedMarkPercent;

    /** @var The size in megabytes of the session large buffer store. */
    uint32_t m_sessionLargeBufferStoreSizeMB;

    /** @var The largest object size in megabytes in the session large buffer store. */
    uint32_t m_sessionLargeBufferStoreMaxObjectSizeMB;

    /** @var The largest single huge object size that can be allocated by any session directly from kernel. */
    uint32_t m_sessionMaxHugeObjectSizeMB;

    /**********************************************************************/
    // Garbage Collection configuration
    /**********************************************************************/
    /** @var Enable/disable garbage collection. */
    bool m_gcEnable;

    /** @var The threshold in bytes for reclamation to be triggered (per-thread). */
    uint32_t m_gcReclaimThresholdBytes;

    /** @var The amount of objects reclaimed in each cleanup round of a limbo group. */
    uint32_t m_gcReclaimBatchSize;

    /** @var The high threshold in bytes for reclamation to be triggered (per-thread). */
    uint32_t m_gcHighReclaimThresholdBytes;

    /**********************************************************************/
    // JIT configuration
    /**********************************************************************/
    /** @var Enable/disable JIT compilation and execution for planned queries. */
    bool m_enableCodegen;

    /** @var Specifies whether to force usage of TVM JIT compilation and execution. */
    bool m_forcePseudoCodegen;

    /** @var Specifies whether to print emitted LLVM/TVM IR code for JIT-compiled queries. */
    bool m_enableCodegenPrint;

    /** @var Limits the amount of JIT queries allowed per user session. */
    uint32_t m_codegenLimit;

    /**********************************************************************/
    // Storage configuration
    /**********************************************************************/
    /** @var Specifies whether defining an index over a null-able column is allowed. */
    bool m_allowIndexOnNullableColumn;

    /** @var Specifies the tree flavor for tree indexes. */
    IndexTreeFlavor m_indexTreeFlavor;

    /**********************************************************************/
    // General configuration
    /**********************************************************************/
    /** @var Configuration monitor period in seconds. */
    uint64_t m_configMonitorPeriodSeconds;

    /** @var Specifies whether to run consistency validation checks after benchmark. */
    bool m_runInternalConsistencyValidation;

    /**
     * @brief Retrieves the NUMA node for the given CPU.
     * @param cpu The logical identifier of the CPU.
     * @return The resulting NUMA node identifier.
     */
    int GetCpuNode(int cpu) const;

    uint16_t GetCoreByConnidFP(uint16_t cpu) const;

    inline bool IsHyperThread() const
    {
        return m_isSystemHyperThreaded;
    }

    int GetMappedCore(int logicId) const;

    void SetMaskToAllCoresinNumaSocket(cpu_set_t& mask, uint64_t threadId);

    void SetMaskToAllCoresinNumaSocket2(cpu_set_t& mask, int nodeId);

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    MOTConfiguration(const MOTConfiguration& orig) = delete;
    MOTConfiguration(const MOTConfiguration&& orig) = delete;
    MOTConfiguration& operator=(const MOTConfiguration& orig) = delete;
    MOTConfiguration& operator=(const MOTConfiguration&& orig) = delete;
    /** @endcond */

private:
    /** @var A singleton instance available for static initializers. */
    static MOTConfiguration motGlobalConfiguration;

    /** @var Map of CPUs to numa nodes */
    CpuNodeMap m_cpuNodeMapper;

    CpuMap m_osCpuMap;

    bool m_isSystemHyperThreaded = false;

    /** @var The total memory value to be used when loading memory percent values. By default uses system total. */
    uint32_t m_totalMemoryMb;

    // Default logger configuration
    /** @var Default enable file logger. */
    static constexpr bool DEFAULT_ENABLE_REDO_LOG = true;

    /** @var Default logger type. */
    static constexpr LoggerType DEFAULT_LOGGER_TYPE = LoggerType::EXTERNAL_LOGGER;

    /** @var Default redo log handler type. */
    static constexpr RedoLogHandlerType DEFAULT_REDO_LOG_HANDLER_TYPE = RedoLogHandlerType::SYNC_REDO_LOG_HANDLER;

    // default commit configuration
    /** @var Default enable group commit. */
    static constexpr bool DEFAULT_ENABLE_GROUP_COMMIT = false;

    /** @var Default group commit size. */
    static constexpr uint64_t DEFAULT_GROUP_COMMIT_SIZE = 16;

    /** @var Default group commit timeout. */
    static constexpr const char* DEFAULT_GROUP_COMMIT_TIMEOUT = "10 ms";

    /** @var Default group commit timeout in micro-seconds. */
    static constexpr uint64_t DEFAULT_GROUP_COMMIT_TIMEOUT_USEC = 10000;

    // default checkpoint configuration
    /** @var Default enable checkpoint. */
    static constexpr bool DEFAULT_ENABLE_CHECKPOINT = true;

    /** @var Default is incremental checkpoint. */
    static constexpr bool DEFAULT_ENABLE_INCREMENTAL_CHECKPOINT = false;

    /** @var Default checkpoint directory (empty, meaning data-node directory). */
    static constexpr const char* DEFAULT_CHECKPOINT_DIR = "";

    /**  @var Default checkpoint segments size */
    static constexpr const char* DEFAULT_CHECKPOINT_SEGSIZE = "16 MB";
    static constexpr uint32_t DEFAULT_CHECKPOINT_SEGSIZE_BYTES = (16 * 1024 * 1024);

    /** @var Default number of worker threads to spawn */
    static constexpr uint32_t DEFAULT_CHECKPOINT_WORKERS = 3;

    /** @var Default enable checkpoint validation. */
    static constexpr bool DEFAULT_VALIDATE_CHECKPOINT = false;

    // default recovery configuration
    /** @var Default number of workers used in recovery from checkpoint. */
    static constexpr uint32_t DEFAULT_CHECKPOINT_RECOVERY_WORKERS = 3;

    /** @var Default enable log recovery statistics. */
    static constexpr bool DEFAULT_ENABLE_LOG_RECOVERY_STATS = false;

    // default machine configuration
    /** @var Default number of NUMA nodes of the machine. */
    static constexpr uint16_t DEFAULT_NUMA_NODES = 1;

    /** @var Default number of cores per CPU in the machine. */
    static constexpr uint16_t DEFAULT_CORES_PER_CPU = 8;

    /** @var Default ID of pg node of the machine. */
    static constexpr uint16_t DEFAULT_DATA_NODE_ID = 0;

    /* @var Default upper bound number of pg nodes on the machine. */
    static constexpr uint16_t DEFAULT_MAX_DATA_NODES = 1;

    // default statistics configuration
    /** @var Default enable statistics printing. */
    static constexpr bool DEFAULT_ENABLE_STATS = false;

    /** @var Default statistics printing period in seconds. */
    static constexpr const char* DEFAULT_STATS_PRINT_PERIOD = "1 minutes";
    static constexpr uint32_t DEFAULT_STATS_PRINT_PERIOD_SECONDS = 60;

    /** @var Default full statistics printing period in seconds. */
    static constexpr const char* DEFAULT_FULL_STATS_PRINT_PERIOD = "5 minutes";
    static constexpr uint32_t DEFAULT_FULL_STATS_PRINT_PERIOD_SECONDS = 300;

    /** @var Default enable DB session statistics printing. */
    static constexpr bool DEFAULT_ENABLE_DB_SESSION_STAT_PRINT = false;

    /** @var Default enable network statistics printing. */
    static constexpr bool DEFAULT_ENABLE_NETWORK_STAT_PRINT = false;

    /** @var Default enable log statistics printing. */
    static constexpr bool DEFAULT_ENABLE_LOG_STAT_PRINT = false;

    /** @var Default enable memory statistics printing. */
    static constexpr bool DEFAULT_ENABLE_MEMORY_STAT_PRINT = false;

    /** @var Default enable detailed memory statistics printing. */
    static constexpr bool DEFAULT_ENABLE_DETAILED_MEMORY_STAT_PRINT = false;

    /** @var Default enable process statistics printing. */
    static constexpr bool DEFAULT_ENABLE_PROCESS_STAT_PRINT = false;

    /** @var Default enable system statistics printing. */
    static constexpr bool DEFAULT_ENABLE_SYSTEM_STAT_PRINT = false;

    /** @var Default enable JIT execution statistics printing. */
    static constexpr bool DEFAULT_ENABLE_JIT_STAT_PRINT = false;

    // default error log configuration
    /** @var Default log level limit. */
    static constexpr LogLevel DEFAULT_LOG_LEVEL = LogLevel::LL_INFO;

    /** @var Default log level for libnuma errors. */
    static constexpr LogLevel DEFAULT_NUMA_ERRORS_LOG_LEVEL = LogLevel::LL_ERROR;

    /** @var Default log level for libnuma warnings. */
    static constexpr LogLevel DEFAULT_NUMA_WARNINGS_LOG_LEVEL = LogLevel::LL_WARN;

    /** @var Default log level for configuration loading on startup messages. */
    static constexpr LogLevel DEFAULT_CFG_STARTUP_LOG_LEVEL = LogLevel::LL_TRACE;

    // default memory configuration
    /** @var Default maximum number of threads in the system. */
    static constexpr uint16_t DEFAULT_MAX_THREADS = 1024;

    /** @var Default maximum number of connections in the system. */
    static constexpr uint32_t DEFAULT_MAX_CONNECTIONS = 1024;

    /** @var Default thread affinity policy. */
    static constexpr AffinityMode DEFAULT_AFFINITY_MODE = AffinityMode::FILL_PHYSICAL_FIRST;

    /** @var The default value for using lazy load scheme in the global chunk directory. */
    static constexpr bool DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY = true;

    /** @var Default maximum limit for MOT global memory. */
    static constexpr const char* DEFAULT_MAX_MOT_GLOBAL_MEMORY = "80%";
    static constexpr uint32_t DEFAULT_MAX_MOT_GLOBAL_MEMORY_MB = 8 * 1024;

    /** @var Default minimum (pre-allocated) limit for MOT global memory. */
    static constexpr const char* DEFAULT_MIN_MOT_GLOBAL_MEMORY = "0 GB";
    static constexpr uint32_t DEFAULT_MIN_MOT_GLOBAL_MEMORY_MB = 0;

    /** @var Default maximum limit for MOT global memory (used to establish new ratio between local and global pools).
     */
    static constexpr const char* DEFAULT_MAX_MOT_LOCAL_MEMORY = "15%";
    static constexpr uint32_t DEFAULT_MAX_MOT_LOCAL_MEMORY_MB = 2 * 1024;

    /** @var Default minimum (pre-allocated) limit for MOT local memory. */
    static constexpr const char* DEFAULT_MIN_MOT_LOCAL_MEMORY = "0 GB";
    static constexpr uint32_t DEFAULT_MIN_MOT_LOCAL_MEMORY_MB = 0;

    /** @var Default maximum for single MOT session small memory allocations. */
    static constexpr const char* DEFAULT_MAX_MOT_SESSION_MEMORY = "0 MB";
    static constexpr uint32_t DEFAULT_MAX_MOT_SESSION_MEMORY_KB = 0;

    /** @var Default minimum (pre-allocated) for single MOT session small memory allocations. */
    static constexpr const char* DEFAULT_MIN_MOT_SESSION_MEMORY = "0 MB";
    static constexpr uint32_t DEFAULT_MIN_MOT_SESSION_MEMORY_KB = 0;

    /** @var Default physical or virtual memory reservation. */
    static constexpr MemReserveMode DEFAULT_RESERVE_MEMORY_MODE = MEM_RESERVE_VIRTUAL;

    /** @var Default physical or virtual memory reservation. */
    static constexpr MemStorePolicy DEFAULT_STORE_MEMORY_POLICY = MEM_STORE_COMPACT;

    /** @var Default chunk allocation policy for global chunk pools. */
    static constexpr MemAllocPolicy DEFAULT_CHUNK_ALLOC_POLICY = MEM_ALLOC_POLICY_AUTO;

    /** @var Default number of workers used to pre-allocate initial memory.  */
    static constexpr uint32_t DEFAULT_CHUNK_PREALLOC_WORKER_COUNT = 8;

    /** @var Default chunk store high red mark in percents of maximum. */
    static constexpr uint32_t DEFAULT_HIGH_RED_MARK_PERCENT = 90;

    /** @var The default size in megabytes of the session large buffer store. */
    static constexpr const char* DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE = " 0 MB";
    static constexpr uint32_t DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE_MB = 0;

    /** @var The default largest object size in megabytes in the session large buffer store. */
    static constexpr const char* DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE = "0 MB";
    static constexpr uint32_t DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB = 0;

    /** @var The default largest object size in megabytes that can be allocated form kernel for sessions. */
    static constexpr const char* DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE = "1 GB";
    static constexpr uint32_t DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE_MB = 1024;

    // default GC configuration
    /** @var Enable/disable garbage collection. */
    static constexpr bool DEFAULT_GC_ENABLE = true;

    /** @var The threshold in bytes for reclamation to be triggered (per-thread) */
    static constexpr const char* DEFAULT_GC_RECLAIM_THRESHOLD = "512 KB";
    static constexpr uint32_t DEFAULT_GC_RECLAIM_THRESHOLD_BYTES = 512 * KILO_BYTE;

    /** @var The amount of objects reclaimed in each cleanup round of a limbo group. */
    static constexpr uint32_t DEFAULT_GC_RECLAIM_BATCH_SIZE = 8000;

    /** @var The high threshold in bytes for reclamation to be triggered (per-thread) */
    static constexpr const char* DEFAULT_GC_HIGH_RECLAIM_THRESHOLD = "8 MB";
    static constexpr uint32_t DEFAULT_GC_HIGH_RECLAIM_THRESHOLD_BYTES = 8 * MEGA_BYTE;

    // default JIT configuration
    /** @var Default enable JIT compilation and execution. */
    static constexpr bool DEFAULT_ENABLE_MOT_CODEGEN = true;

    /* @var Default force usage of TVM although LLVM is supported on current platform. */
    static constexpr bool DEFAULT_FORCE_MOT_PSEUDO_CODEGEN = false;

    /** @var Default enable printing of emitted LLVM/TVM IR code of JIT-compiled queries. */
    static constexpr bool DEFAULT_ENABLE_MOT_CODEGEN_PRINT = false;

    /** @vart Default limit for the amount of JIT queries allowed per user session. */
    static constexpr uint32_t DEFAULT_MOT_CODEGEN_LIMIT = 100;

    // default storage configuration
    /** @var The default allow index on null-able column. */
    static constexpr bool DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN = true;

    /** @var The default tree flavor for tree indexes. */
    static constexpr IndexTreeFlavor DEFAULT_INDEX_TREE_FLAVOR = IndexTreeFlavor::INDEX_TREE_FLAVOR_MASSTREE;

    // default general configuration
    /** @var Default configuration monitor period in seconds. */
    static constexpr const char* DEFAULT_CFG_MONITOR_PERIOD = "5 seconds";
    static constexpr uint64_t DEFAULT_CFG_MONITOR_PERIOD_SECONDS = 5;

    /** @var The default value for consistency validation tests after benchmark running. */
    static constexpr bool DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION = false;

    /** @var The default total memory reference used for calculating memory percent value. */
    static constexpr uint32_t DEFAULT_TOTAL_MEMORY_MB = 10 * 1024;

    /** @brief Loads configuration from main configuration. */
    void LoadConfig();

    DECLARE_CLASS_LOGGER()

    // static helper methods
    static bool FindNumaNodes(int* maxNodes);
    static bool FindNumProcessors(uint16_t* maxCoresPerNode, CpuNodeMap* cpuNodeMapper, CpuMap* cpuOsMapper);
    static bool CheckHyperThreads();

    static void UpdateConfigItem(bool& oldValue, bool newValue, const char* name);
    static void UpdateConfigItem(std::string& oldValue, const char* newValue, const char* name);

    template <typename T>
    static void UpdateConfigItem(uint64_t& oldValue, T newValue, const char* name)
    {
        if (oldValue != newValue) {
            MOT_LOG_TRACE("Configuration of %s changed: %" PRIu64 " --> %" PRIu64, name, oldValue, (uint64_t)newValue);
            oldValue = newValue;
        }
    }

    template <typename T>
    static void UpdateConfigItem(uint32_t& oldValue, T newValue, const char* name)
    {
        if (newValue > UINT_MAX) {
            MOT_LOG_WARN("Configuration of %s overflowed: keeping default value %u", name, oldValue);
        } else if (oldValue != newValue) {
            MOT_LOG_TRACE("Configuration of %s changed: %u --> %u", name, oldValue, (uint32_t)newValue);
            oldValue = newValue;
        }
    }

    template <typename T>
    static void UpdateConfigItem(uint16_t& oldValue, T newValue, const char* name)
    {
        if (newValue > USHRT_MAX) {
            MOT_LOG_WARN("Configuration of %s overflowed: keeping default value %" PRIu16, name, oldValue);
        } else if (oldValue != newValue) {
            MOT_LOG_TRACE("Configuration of %s changed: %" PRIu16 " --> %" PRIu16, name, oldValue, (uint16_t)newValue);
            oldValue = newValue;
        }
    }

    template <typename T>
    static void UpdateUserConfigItem(T& oldValue, const T& newValue, const char* name)
    {
        if (oldValue != newValue) {
            mot_string strOldValue;
            mot_string strNewValue;
            MOT_LOG_TRACE("Configuration of %s changed: %s --> %s",
                name,
                TypeFormatter<T>::ToString(oldValue, strOldValue),
                TypeFormatter<T>::ToString(newValue, strNewValue));
            oldValue = newValue;
        }
    }

    void UpdateComponentLogLevel();

    static int ParseMemoryPercent(const char* memoryValue);
    uint64_t ParseMemoryValueBytes(const char* memoryValue, uint64_t defaultValue, const char* cfgPath);
    uint64_t ParseMemoryPercentTotal(const char* memoryValue, uint64_t defaultValue, const char* cfgPath);
    uint64_t ParseMemoryUnit(const char* memoryValue, uint64_t defaultValue, const char* cfgPath);

    static uint64_t ParseTimeValueMicros(const char* timeValue, uint64_t defaultValue, const char* cfgPath);
};

/**
 * @brief Retrieves the singleton instance of the system configuration object.
 * @return The single configuration object.
 */
inline MOTConfiguration& GetGlobalConfiguration()
{
    return MOTConfiguration::GetInstance();
}
}  // namespace MOT

#endif /* MOT_CONFIGURATION_H */
