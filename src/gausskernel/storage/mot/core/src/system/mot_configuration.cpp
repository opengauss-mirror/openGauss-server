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
 * mot_configuration.cpp
 *    Holds global configuration for the MOT storage engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/mot_configuration.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_configuration.h"
#include "utilities.h"
#include "config_manager.h"
#include "log_level_formatter.h"
#include "mm_cfg.h"
#include "sys_numa_api.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(MOTConfiguration, Configuration)

MOTConfiguration MOTConfiguration::motGlobalConfiguration;

// static member definitions
// redo-log configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_REDO_LOG;
constexpr LoggerType MOTConfiguration::DEFAULT_LOGGER_TYPE;
constexpr RedoLogHandlerType MOTConfiguration::DEFAULT_REDO_LOG_HANDLER_TYPE;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_GROUP_COMMIT;
constexpr uint64_t MOTConfiguration::DEFAULT_GROUP_COMMIT_SIZE;
constexpr const char* MOTConfiguration::DEFAULT_GROUP_COMMIT_TIMEOUT;
constexpr uint64_t MOTConfiguration::DEFAULT_GROUP_COMMIT_TIMEOUT_USEC;
// checkpoint configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_CHECKPOINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_INCREMENTAL_CHECKPOINT;
constexpr const char* MOTConfiguration::DEFAULT_CHECKPOINT_DIR;
constexpr const char* MOTConfiguration::DEFAULT_CHECKPOINT_SEGSIZE;
constexpr uint32_t MOTConfiguration::DEFAULT_CHECKPOINT_SEGSIZE_BYTES;
constexpr uint32_t MOTConfiguration::DEFAULT_CHECKPOINT_WORKERS;
constexpr bool MOTConfiguration::DEFAULT_VALIDATE_CHECKPOINT;
// recovery configuration members
constexpr uint32_t MOTConfiguration::DEFAULT_CHECKPOINT_RECOVERY_WORKERS;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_LOG_RECOVERY_STATS;
// machine configuration members
constexpr uint16_t MOTConfiguration::DEFAULT_NUMA_NODES;
constexpr uint16_t MOTConfiguration::DEFAULT_CORES_PER_CPU;
constexpr uint16_t MOTConfiguration::DEFAULT_DATA_NODE_ID;
constexpr uint16_t MOTConfiguration::DEFAULT_MAX_DATA_NODES;
// statistics configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_STATS;
constexpr const char* MOTConfiguration::DEFAULT_STATS_PRINT_PERIOD;
constexpr uint32_t MOTConfiguration::DEFAULT_STATS_PRINT_PERIOD_SECONDS;
constexpr const char* MOTConfiguration::DEFAULT_FULL_STATS_PRINT_PERIOD;
constexpr uint32_t MOTConfiguration::DEFAULT_FULL_STATS_PRINT_PERIOD_SECONDS;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_DB_SESSION_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_NETWORK_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_LOG_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MEMORY_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_DETAILED_MEMORY_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_PROCESS_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_SYSTEM_STAT_PRINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_JIT_STAT_PRINT;
// error log configuration members
constexpr LogLevel MOTConfiguration::DEFAULT_LOG_LEVEL;
constexpr LogLevel MOTConfiguration::DEFAULT_NUMA_ERRORS_LOG_LEVEL;
constexpr LogLevel MOTConfiguration::DEFAULT_NUMA_WARNINGS_LOG_LEVEL;
constexpr LogLevel MOTConfiguration::DEFAULT_CFG_STARTUP_LOG_LEVEL;
// memory configuration members
constexpr uint16_t MOTConfiguration::DEFAULT_MAX_THREADS;
constexpr uint32_t MOTConfiguration::DEFAULT_MAX_CONNECTIONS;
constexpr AffinityMode MOTConfiguration::DEFAULT_AFFINITY_MODE;
constexpr bool MOTConfiguration::DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_GLOBAL_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MAX_MOT_GLOBAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_LOCAL_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MAX_MOT_LOCAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_GLOBAL_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MIN_MOT_GLOBAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_LOCAL_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MIN_MOT_LOCAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_SESSION_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MAX_MOT_SESSION_MEMORY_KB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_SESSION_MEMORY;
constexpr uint32_t MOTConfiguration::DEFAULT_MIN_MOT_SESSION_MEMORY_KB;
constexpr MemReserveMode MOTConfiguration::DEFAULT_RESERVE_MEMORY_MODE;
constexpr MemStorePolicy MOTConfiguration::DEFAULT_STORE_MEMORY_POLICY;
constexpr MemAllocPolicy MOTConfiguration::DEFAULT_CHUNK_ALLOC_POLICY;
constexpr uint32_t MOTConfiguration::DEFAULT_CHUNK_PREALLOC_WORKER_COUNT;
constexpr uint32_t MOTConfiguration::DEFAULT_HIGH_RED_MARK_PERCENT;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE;
constexpr uint32_t MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE_MB;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE;
constexpr uint32_t MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE;
constexpr uint32_t MOTConfiguration::DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE_MB;
// GC configuration members
constexpr bool MOTConfiguration::DEFAULT_GC_ENABLE;
constexpr const char* MOTConfiguration::DEFAULT_GC_RECLAIM_THRESHOLD;
constexpr uint32_t MOTConfiguration::DEFAULT_GC_RECLAIM_THRESHOLD_BYTES;
constexpr uint32_t MOTConfiguration::DEFAULT_GC_RECLAIM_BATCH_SIZE;
constexpr const char* MOTConfiguration::DEFAULT_GC_HIGH_RECLAIM_THRESHOLD;
constexpr uint32_t MOTConfiguration::DEFAULT_GC_HIGH_RECLAIM_THRESHOLD_BYTES;
// JIT configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_CODEGEN;
constexpr bool MOTConfiguration::DEFAULT_FORCE_MOT_PSEUDO_CODEGEN;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_CODEGEN_PRINT;
constexpr uint32_t MOTConfiguration::DEFAULT_MOT_CODEGEN_LIMIT;
// storage configuration
constexpr bool MOTConfiguration::DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN;
constexpr IndexTreeFlavor MOTConfiguration::DEFAULT_INDEX_TREE_FLAVOR;
// general configuration members
constexpr const char* MOTConfiguration::DEFAULT_CFG_MONITOR_PERIOD;
constexpr uint64_t MOTConfiguration::DEFAULT_CFG_MONITOR_PERIOD_SECONDS;
constexpr bool MOTConfiguration::DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION;
constexpr uint32_t MOTConfiguration::DEFAULT_TOTAL_MEMORY_MB;

static constexpr unsigned int MAX_NUMA_NODES = 16u;
#define IS_HYPER_THREAD_CMD "lscpu | grep \"Thread(s) per core:\" |  awk '{print $4}'"

static bool ParseLoggerType(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, LoggerType* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = LoggerTypeFromString(newValue.c_str());
        if (*variableValue == LoggerType::INVALID_LOGGER) {
            result = false;
        }
    }
    return result;
}

static bool ParseValidation(const std::string& cfgName, const std::string& variableName, const std::string& newValue,
    TxnValidation* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        if (newValue == "no-wait") {
            *variableValue = TxnValidation::TXN_VALIDATION_NO_WAIT;
        } else if (newValue == "waiting") {
            *variableValue = TxnValidation::TXN_VALIDATION_WAITING;
        } else {
            *variableValue = TxnValidation::TXN_VALIDATION_ERROR;
        }
    }
    return result;
}

static bool ParseAffinity(const std::string& cfgName, const std::string& variableName, const std::string& newValue,
    AffinityMode* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = AffinityModeFromString(newValue.c_str());
        if (*variableValue == AffinityMode::AFFINITY_INVALID) {
            result = false;
        }
    }
    return result;
}

static bool ParseIndexTreeFlavor(const std::string& cfgName, const std::string& variableName,
    const std::string& newValue, IndexTreeFlavor* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = IndexTreeFlavorFromString(newValue.c_str());
        if (*variableValue == IndexTreeFlavor::INDEX_TREE_FLAVOR_INVALID) {
            result = false;
        }
    }
    return result;
}

static bool ParseRedoLogHandlerType(const std::string& cfgName, const std::string& variableName,
    const std::string& newValue, RedoLogHandlerType* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = RedoLogHandlerTypeFromString(newValue.c_str());
        if (*variableValue == RedoLogHandlerType::INVALID_REDO_LOG_HANDLER) {
            result = false;
        }
    }
    return result;
}

static bool ParseBool(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, bool* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = (newValue == "true");
        MOT_ASSERT(newValue == "true" || newValue == "false");
    }
    return result;
}

static bool ParseString(const std::string& cfgName, const std::string& variableName, const std::string& newValue,
    std::string* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = newValue;
        MOT_ASSERT(!newValue.empty());
    }
    return result;
}

static bool ParseUint64(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, uint64_t* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = std::stoul(newValue);
        MOT_ASSERT(!newValue.empty());
    }
    return result;
}

static bool ParseUint32(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, uint32_t* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = std::stoul(newValue);
        MOT_ASSERT(!newValue.empty());
    }
    return result;
}

static bool ParseUint16(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, uint16_t* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = (uint16_t)std::stoul(newValue);
        MOT_ASSERT(!newValue.empty());
    }
    return result;
}

static bool ParseLogLevel(
    const std::string& cfgName, const std::string& variableName, const std::string& newValue, LogLevel* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = LogLevelFromString(newValue.c_str());
    }
    return result;
}

static bool ParseMemoryReserveMode(const std::string& cfgName, const std::string& variableName,
    const std::string& newValue, MemReserveMode* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = MemReserveModeFromString(newValue.c_str());
    }
    return result;
}

static bool ParseMemoryStorePolicy(const std::string& cfgName, const std::string& variableName,
    const std::string& newValue, MemStorePolicy* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = MemStorePolicyFromString(newValue.c_str());
    }
    return result;
}

static bool ParseChunkAllocPolicy(const std::string& cfgName, const std::string& variableName,
    const std::string& newValue, MemAllocPolicy* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = MemAllocPolicyFromString(newValue.c_str());
    }
    return result;
}

bool MOTConfiguration::FindNumaNodes(int* maxNodes)
{
    int error = MotSysNumaAvailable();
    if (error == -1) {
        MOT_LOG_ERROR("Libnuma library not installed or not configured");
        return false;
    }

    *maxNodes = MotSysNumaConfiguredNodes();
    if (*maxNodes < 0) {
        MOT_LOG_ERROR("Invalid NUMA configuration max_nodes=%d", *maxNodes);
        return false;
    }

    if ((unsigned int)*maxNodes >= MAX_NUMA_NODES) {
        MOT_LOG_ERROR("sys_numa_num_configured_nodes() => %d, while MAX_NUMA_NODES=%u - cannot proceed",
            *maxNodes,
            MAX_NUMA_NODES);
        return false;
    }

    return true;
}

bool MOTConfiguration::FindNumProcessors(uint16_t* maxCoresPerNode, CpuNodeMap* cpuNodeMapper, CpuMap* cpuOsMapper)
{
    int nprocs = sysconf(_SC_NPROCESSORS_ONLN);
    if (nprocs <= 0) {
        MOT_LOG(LogLevel::LL_ERROR, "Invalid system configuration _SC_NPROCESSORS_ONLN=%d", nprocs);
        return false;
    }

    uint16_t cpusPerNode[MAX_NUMA_NODES];
    errno_t erc = memset_s(&(cpusPerNode[0]), sizeof(cpusPerNode), 0, sizeof(cpusPerNode));
    securec_check(erc, "\0", "\0");
    for (int i = 0; i < nprocs; i++) {
        int node = MotSysNumaGetNode(i);
        if (node < 0) {
            MOT_LOG_ERROR("Invalid NUMA configuration numa_node_of_cpu(%d) => %d", i, node);
            return false;
        }
        if ((unsigned int)node >= MAX_NUMA_NODES) {
            MOT_LOG_ERROR(
                "CPU %d is located in node %d, while MAX_NUMA_NODES=%u - cannot proceed", i, node, MAX_NUMA_NODES);
            return false;
        }
        cpusPerNode[node]++;
        (*cpuNodeMapper)[i] = node;
        (*cpuOsMapper)[node].insert(i);
    }

    // dynamically calculate number of CPU cores per NUMA node instead of hard-coded config value
    if (maxCoresPerNode != nullptr) {
        *maxCoresPerNode = 0;
        for (unsigned int j = 0; j < MAX_NUMA_NODES; j++) {
            if (cpusPerNode[j] > *maxCoresPerNode) {
                *maxCoresPerNode = cpusPerNode[j];
            }
        }
    }

    return true;
}

void MOTConfiguration::SetMaskToAllCoresinNumaSocket(cpu_set_t& mask, uint64_t threadId)
{
    int nodeId = GetCpuNode(threadId);
    auto nodeMap = m_osCpuMap[nodeId];
    for (auto it = nodeMap.begin(); it != nodeMap.end(); ++it) {
        if (MotSysNumaCpuAllowed(*it)) {
            CPU_SET(*it, &mask);
        }
    }
}

void MOTConfiguration::SetMaskToAllCoresinNumaSocket2(cpu_set_t& mask, int nodeId)
{
    auto nodeMap = m_osCpuMap[nodeId];
    for (auto it = nodeMap.begin(); it != nodeMap.end(); ++it) {
        if (MotSysNumaCpuAllowed(*it)) {
            CPU_SET(*it, &mask);
        }
    }
}

MOTConfiguration::MOTConfiguration()
    : m_enableRedoLog(DEFAULT_ENABLE_REDO_LOG),
      m_loggerType(DEFAULT_LOGGER_TYPE),
      m_redoLogHandlerType(DEFAULT_REDO_LOG_HANDLER_TYPE),
      m_enableGroupCommit(DEFAULT_ENABLE_GROUP_COMMIT),
      m_groupCommitSize(DEFAULT_GROUP_COMMIT_SIZE),
      m_groupCommitTimeoutUSec(DEFAULT_GROUP_COMMIT_TIMEOUT_USEC),
      m_enableCheckpoint(DEFAULT_ENABLE_CHECKPOINT),
      m_enableIncrementalCheckpoint(DEFAULT_ENABLE_INCREMENTAL_CHECKPOINT),
      m_checkpointDir(DEFAULT_CHECKPOINT_DIR),
      m_checkpointSegThreshold(DEFAULT_CHECKPOINT_SEGSIZE_BYTES),
      m_checkpointWorkers(DEFAULT_CHECKPOINT_WORKERS),
      m_validateCheckpoint(DEFAULT_VALIDATE_CHECKPOINT),
      m_checkpointRecoveryWorkers(DEFAULT_CHECKPOINT_RECOVERY_WORKERS),
      m_abortBufferEnable(true),
      m_preAbort(true),
      m_validationLock(TxnValidation::TXN_VALIDATION_NO_WAIT),
      m_numaNodes(DEFAULT_NUMA_NODES),
      m_coresPerCpu(DEFAULT_CORES_PER_CPU),
      m_dataNodeId(DEFAULT_DATA_NODE_ID),
      m_maxDataNodes(DEFAULT_MAX_DATA_NODES),
      m_enableStats(DEFAULT_ENABLE_STATS),
      m_statPrintPeriodSeconds(DEFAULT_STATS_PRINT_PERIOD_SECONDS),
      m_statPrintFullPeriodSeconds(DEFAULT_FULL_STATS_PRINT_PERIOD_SECONDS),
      m_enableLogRecoveryStats(DEFAULT_ENABLE_LOG_RECOVERY_STATS),
      m_enableDbSessionStatistics(DEFAULT_ENABLE_DB_SESSION_STAT_PRINT),
      m_enableNetworkStatistics(DEFAULT_ENABLE_NETWORK_STAT_PRINT),
      m_enableLogStatistics(DEFAULT_ENABLE_LOG_STAT_PRINT),
      m_enableMemoryStatistics(DEFAULT_ENABLE_MEMORY_STAT_PRINT),
      m_enableDetailedMemoryStatistics(DEFAULT_ENABLE_DETAILED_MEMORY_STAT_PRINT),
      m_enableProcessStatistics(DEFAULT_ENABLE_PROCESS_STAT_PRINT),
      m_enableSystemStatistics(DEFAULT_ENABLE_SYSTEM_STAT_PRINT),
      m_enableJitStatistics(DEFAULT_ENABLE_JIT_STAT_PRINT),
      m_logLevel(DEFAULT_LOG_LEVEL),
      m_numaErrorsLogLevel(DEFAULT_NUMA_ERRORS_LOG_LEVEL),
      m_numaWarningsLogLevel(DEFAULT_NUMA_WARNINGS_LOG_LEVEL),
      m_cfgStartupLogLevel(DEFAULT_CFG_STARTUP_LOG_LEVEL),
      m_maxThreads(DEFAULT_MAX_THREADS),
      m_maxConnections(DEFAULT_MAX_CONNECTIONS),
      m_sessionAffinityMode(DEFAULT_AFFINITY_MODE),
      m_taskAffinityMode(DEFAULT_AFFINITY_MODE),
      m_lazyLoadChunkDirectory(DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY),
      m_globalMemoryMaxLimitMB(DEFAULT_MAX_MOT_GLOBAL_MEMORY_MB),
      m_globalMemoryMinLimitMB(DEFAULT_MIN_MOT_GLOBAL_MEMORY_MB),
      m_localMemoryMaxLimitMB(DEFAULT_MAX_MOT_LOCAL_MEMORY_MB),
      m_localMemoryMinLimitMB(DEFAULT_MIN_MOT_LOCAL_MEMORY_MB),
      m_sessionMemoryMaxLimitKB(DEFAULT_MAX_MOT_SESSION_MEMORY_KB),
      m_sessionMemoryMinLimitKB(DEFAULT_MIN_MOT_SESSION_MEMORY_KB),
      m_reserveMemoryMode(DEFAULT_RESERVE_MEMORY_MODE),
      m_storeMemoryPolicy(DEFAULT_STORE_MEMORY_POLICY),
      m_chunkAllocPolicy(DEFAULT_CHUNK_ALLOC_POLICY),
      m_chunkPreallocWorkerCount(DEFAULT_CHUNK_PREALLOC_WORKER_COUNT),
      m_highRedMarkPercent(DEFAULT_HIGH_RED_MARK_PERCENT),
      m_sessionLargeBufferStoreSizeMB(DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE_MB),
      m_sessionLargeBufferStoreMaxObjectSizeMB(DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB),
      m_sessionMaxHugeObjectSizeMB(DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE_MB),
      m_gcEnable(DEFAULT_GC_ENABLE),
      m_gcReclaimThresholdBytes(DEFAULT_GC_RECLAIM_THRESHOLD_BYTES),
      m_gcReclaimBatchSize(DEFAULT_GC_RECLAIM_BATCH_SIZE),
      m_gcHighReclaimThresholdBytes(DEFAULT_GC_HIGH_RECLAIM_THRESHOLD_BYTES),
      m_enableCodegen(DEFAULT_ENABLE_MOT_CODEGEN),
      m_forcePseudoCodegen(DEFAULT_FORCE_MOT_PSEUDO_CODEGEN),
      m_enableCodegenPrint(DEFAULT_ENABLE_MOT_CODEGEN_PRINT),
      m_codegenLimit(DEFAULT_MOT_CODEGEN_LIMIT),
      m_allowIndexOnNullableColumn(DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN),
      m_indexTreeFlavor(DEFAULT_INDEX_TREE_FLAVOR),
      m_configMonitorPeriodSeconds(DEFAULT_CFG_MONITOR_PERIOD_SECONDS),
      m_runInternalConsistencyValidation(DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION),
      m_totalMemoryMb(DEFAULT_TOTAL_MEMORY_MB)
{
    // Since MOTConfiguration has a global instance it is initialized early (before main() or other code is called)
    // we must initialize the sys_numa API early enough. This look likes the right timing.
    MotSysNumaInit();
    int numa = DEFAULT_NUMA_NODES;
    if (FindNumaNodes(&numa)) {
        m_numaNodes = (uint16_t)numa;
    }

    uint16_t cores = DEFAULT_CORES_PER_CPU;
    if (FindNumProcessors(&cores, &m_cpuNodeMapper, &m_osCpuMap)) {
        m_coresPerCpu = cores;
    }

    m_isSystemHyperThreaded = CheckHyperThreads();
}

MOTConfiguration::~MOTConfiguration()
{
    // destroy late enough
    MotSysNumaDestroy();
}

bool MOTConfiguration::SetFlag(const std::string& name, const std::string& value)
{
    bool result = true;

    if (ParseBool(name, "enable_redo_log", value, &m_enableRedoLog)) {
    } else if (ParseLoggerType(name, "logger_type", value, &m_loggerType)) {
    } else if (ParseRedoLogHandlerType(name, "redo_log_handler_type", value, &m_redoLogHandlerType)) {
    } else if (ParseBool(name, "enable_group_commit", value, &m_enableGroupCommit)) {
    } else if (ParseUint64(name, "group_commit_size", value, &m_groupCommitSize)) {
    } else if (ParseUint64(name, "group_commit_timeout_usec", value, &m_groupCommitTimeoutUSec)) {
    } else if (ParseBool(name, "enable_checkpoint", value, &m_enableCheckpoint)) {
    } else if (ParseBool(name, "enable_incremental_checkpoint", value, &m_enableIncrementalCheckpoint)) {
    } else if (ParseString(name, "checkpoint_dir", value, &m_checkpointDir)) {
    } else if (ParseUint32(name, "checkpoint_segsize", value, &m_checkpointSegThreshold)) {
    } else if (ParseUint32(name, "checkpoint_workers", value, &m_checkpointWorkers)) {
    } else if (ParseBool(name, "validate_checkpoint", value, &m_validateCheckpoint)) {
    } else if (ParseUint32(name, "checkpoint_recovery_workers", value, &m_checkpointRecoveryWorkers)) {
    } else if (ParseBool(name, "abort_buffer_enable", value, &m_abortBufferEnable)) {
    } else if (ParseBool(name, "pre_abort", value, &m_preAbort)) {
    } else if (ParseValidation(name, "validation_lock", value, &m_validationLock)) {
    } else if (ParseBool(name, "enable_stats", value, &m_enableStats)) {
    } else if (ParseUint32(name, "stats_period_seconds", value, &m_statPrintPeriodSeconds)) {
    } else if (ParseUint32(name, "full_stats_period_seconds", value, &m_statPrintFullPeriodSeconds)) {
    } else if (ParseBool(name, "enable_log_recovery_stats", value, &m_enableLogRecoveryStats)) {
    } else if (ParseBool(name, "enable_db_session_stats", value, &m_enableDbSessionStatistics)) {
    } else if (ParseBool(name, "enable_network_stats", value, &m_enableNetworkStatistics)) {
    } else if (ParseBool(name, "enable_log_stats", value, &m_enableLogStatistics)) {
    } else if (ParseBool(name, "enable_memory_stats", value, &m_enableMemoryStatistics)) {
    } else if (ParseBool(name, "enable_detailed_memory_stats", value, &m_enableDetailedMemoryStatistics)) {
    } else if (ParseBool(name, "enable_process_stats", value, &m_enableProcessStatistics)) {
    } else if (ParseBool(name, "enable_system_stats", value, &m_enableSystemStatistics)) {
    } else if (ParseBool(name, "enable_jit_stats", value, &m_enableJitStatistics)) {
    } else if (ParseLogLevel(name, "log_level", value, &m_logLevel)) {
    } else if (ParseLogLevel(name, "numa_errors_log_level", value, &m_numaErrorsLogLevel)) {
    } else if (ParseLogLevel(name, "numa_warnings_log_level", value, &m_numaWarningsLogLevel)) {
    } else if (ParseLogLevel(name, "cfg_startup_log_level", value, &m_cfgStartupLogLevel)) {
    } else if (ParseUint16(name, "max_threads", value, &m_maxThreads)) {
    } else if (ParseUint32(name, "max_connections", value, &m_maxConnections)) {
    } else if (ParseAffinity(name, "affinity_mode", value, &m_sessionAffinityMode)) {
    } else if (ParseBool(name, "lazy_load_chunk_directory", value, &m_lazyLoadChunkDirectory)) {
    } else if (ParseUint32(name, "max_mot_global_memory_mb", value, &m_globalMemoryMaxLimitMB)) {
    } else if (ParseUint32(name, "min_mot_global_memory_mb", value, &m_globalMemoryMinLimitMB)) {
    } else if (ParseUint32(name, "max_mot_local_memory_mb", value, &m_localMemoryMaxLimitMB)) {
    } else if (ParseUint32(name, "min_mot_local_memory_mb", value, &m_localMemoryMinLimitMB)) {
    } else if (ParseUint32(name, "max_mot_session_memory_kb", value, &m_sessionMemoryMaxLimitKB)) {
    } else if (ParseUint32(name, "min_mot_session_memory_kb", value, &m_sessionMemoryMinLimitKB)) {
    } else if (ParseMemoryReserveMode(name, "reserve_memory_mode", value, &m_reserveMemoryMode)) {
    } else if (ParseMemoryStorePolicy(name, "store_memory_policy", value, &m_storeMemoryPolicy)) {
    } else if (ParseChunkAllocPolicy(name, "chunk_alloc_policy", value, &m_chunkAllocPolicy)) {
    } else if (ParseUint32(name, "chunk_prealloc_worker_count", value, &m_chunkPreallocWorkerCount)) {
    } else if (ParseUint32(name, "high_red_mark_percent", value, &m_highRedMarkPercent)) {
    } else if (ParseUint32(name, "session_large_buffer_store_size_mb", value, &m_sessionLargeBufferStoreSizeMB)) {
    } else if (ParseUint32(name,
                   "session_large_buffer_store_max_object_size_mb",
                   value,
                   &m_sessionLargeBufferStoreMaxObjectSizeMB)) {
    } else if (ParseUint32(name, "session_max_huge_object_size_mb", value, &m_sessionMaxHugeObjectSizeMB)) {
    } else if (ParseBool(name, "enable_mot_codegen", value, &m_enableCodegen)) {
    } else if (ParseBool(name, "force_mot_pseudo_codegen", value, &m_forcePseudoCodegen)) {
    } else if (ParseBool(name, "enable_mot_codegen_print", value, &m_enableCodegenPrint)) {
    } else if (ParseUint32(name, "mot_codegen_limit", value, &m_codegenLimit)) {
    } else if (ParseBool(name, "allow_index_on_nullable_column", value, &m_allowIndexOnNullableColumn)) {
    } else if (ParseIndexTreeFlavor(name, "index_tree_flavor", value, &m_indexTreeFlavor)) {
    } else if (ParseUint64(name, "config_monitor_period_seconds", value, &m_configMonitorPeriodSeconds)) {
    } else if (ParseBool(name, "run_internal_consistency_validation", value, &m_runInternalConsistencyValidation)) {
    } else {
        MOT_LOG_WARN("Unknown configuration variable %s (=%s)", name.c_str(), value.c_str());
        result = false;
    }

    if (result) {
        if (IS_AFFINITY_ACTIVE(m_sessionAffinityMode)) {
            m_taskAffinityMode = m_sessionAffinityMode;
        }
    }

    return result;
}

int MOTConfiguration::GetCpuNode(int cpu) const
{
    CpuNodeMap::const_iterator itr = m_cpuNodeMapper.find(cpu);
    return (itr != m_cpuNodeMapper.end()) ? itr->second : -1;
}

uint16_t MOTConfiguration::GetCoreByConnidFP(uint16_t cpu) const
{
    uint16_t numOfRealCores = (IsHyperThread() == true) ? m_coresPerCpu / 2 : m_coresPerCpu;  // 2 is for HyperThread
    // Lets pin physical first
    // cpu is already modulu of(numaNodes*num_cores)
    if (cpu < m_numaNodes * numOfRealCores) {
        cpu = cpu % (m_numaNodes * numOfRealCores);
        uint16_t coreIndex = cpu % numOfRealCores;
        uint16_t numaId = cpu / numOfRealCores;
        int counter = 0;
        auto coreSet = m_osCpuMap.find(numaId);
        for (auto it = coreSet->second.begin(); it != coreSet->second.end(); ++it) {
            if (counter == coreIndex) {
                return *it;
            }
            counter++;
        }
    } else {
        return cpu;
    }

    return cpu;
}

int MOTConfiguration::GetMappedCore(int logicId) const
{

    int counter = 0;
    for (auto it = m_osCpuMap.begin(); it != m_osCpuMap.end(); ++it) {
        for (auto it2 = (*it).second.begin(); it2 != (*it).second.end(); ++it2) {
            if (counter == logicId) {
                return *it2;
            }
            counter++;
        }
    }
    return -1;
}

#define UPDATE_CFG(var, cfgPath, defaultValue) \
    UpdateConfigItem(var, cfg->GetConfigValue(cfgPath, defaultValue), cfgPath)

#define UPDATE_USER_CFG(var, cfgPath, defaultValue) \
    UpdateUserConfigItem(var, cfg->GetUserConfigValue(cfgPath, defaultValue), cfgPath)

#define UPDATE_STRING_CFG(var, cfgPath, defaultValue) \
    UpdateConfigItem(var, cfg->GetStringConfigValue(cfgPath, defaultValue), cfgPath)

#define UPDATE_INT_CFG(var, cfgPath, defaultValue) \
    UpdateConfigItem(var, cfg->GetIntegerConfigValue(cfgPath, defaultValue), cfgPath)

#define UPDATE_MEM_CFG(var, cfgPath, defaultValue, scale)                                                   \
    do {                                                                                                    \
        uint64_t memoryValueBytes =                                                                         \
            ParseMemoryValueBytes(cfg->GetStringConfigValue(cfgPath, defaultValue), (uint64_t)-1, cfgPath); \
        UpdateConfigItem(var, (uint32_t)(memoryValueBytes / scale), cfgPath);                               \
    } while (0);

#define UPDATE_ABS_MEM_CFG(var, cfgPath, defaultValue, scale)                                         \
    do {                                                                                              \
        uint64_t memoryValueBytes =                                                                   \
            ParseMemoryUnit(cfg->GetStringConfigValue(cfgPath, defaultValue), (uint64_t)-1, cfgPath); \
        UpdateConfigItem(var, (uint32_t)(memoryValueBytes / scale), cfgPath);                         \
    } while (0);

#define UPDATE_TIME_CFG(var, cfgPath, defaultValue, scale)                                                 \
    do {                                                                                                   \
        uint64_t timeValueMicros =                                                                         \
            ParseTimeValueMicros(cfg->GetStringConfigValue(cfgPath, defaultValue), (uint64_t)-1, cfgPath); \
        UpdateConfigItem(var, (uint64_t)(timeValueMicros / scale), cfgPath);                               \
    } while (0);

void MOTConfiguration::LoadConfig()
{
    MOT_LOG_TRACE("Loading main configuration");
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();

    // logger configuration
    UPDATE_CFG(m_enableRedoLog, "enable_redo_log", DEFAULT_ENABLE_REDO_LOG);
    UPDATE_USER_CFG(m_loggerType, "logger_type", DEFAULT_LOGGER_TYPE);
    UPDATE_USER_CFG(m_redoLogHandlerType, "redo_log_handler_type", DEFAULT_REDO_LOG_HANDLER_TYPE);

    // commit configuration
    UPDATE_CFG(m_enableGroupCommit, "enable_group_commit", DEFAULT_ENABLE_GROUP_COMMIT);
    UPDATE_INT_CFG(m_groupCommitSize, "group_commit_size", DEFAULT_GROUP_COMMIT_SIZE);
    UPDATE_TIME_CFG(m_groupCommitTimeoutUSec, "group_commit_timeout", DEFAULT_GROUP_COMMIT_TIMEOUT, 1);

    // Checkpoint configuration
    UPDATE_CFG(m_enableCheckpoint, "enable_checkpoint", DEFAULT_ENABLE_CHECKPOINT);
    UPDATE_STRING_CFG(m_checkpointDir, "checkpoint_dir", DEFAULT_CHECKPOINT_DIR);
    UPDATE_MEM_CFG(m_checkpointSegThreshold, "checkpoint_segsize", DEFAULT_CHECKPOINT_SEGSIZE, 1);
    UPDATE_INT_CFG(m_checkpointWorkers, "checkpoint_workers", DEFAULT_CHECKPOINT_WORKERS);
    UPDATE_CFG(m_validateCheckpoint, "validate_checkpoint", DEFAULT_VALIDATE_CHECKPOINT);

    // Recovery configuration
    UPDATE_INT_CFG(m_checkpointRecoveryWorkers, "checkpoint_recovery_workers", DEFAULT_CHECKPOINT_RECOVERY_WORKERS);

    // Tx configuration - not configurable yet
    UPDATE_CFG(m_abortBufferEnable, "tx_abort_buffers_enable", true);
    UPDATE_CFG(m_preAbort, "tx_pre_abort", true);
    m_validationLock = TxnValidation::TXN_VALIDATION_NO_WAIT;

    // statistics configuration
    UPDATE_CFG(m_enableStats, "enable_stats", DEFAULT_ENABLE_STATS);
    UPDATE_TIME_CFG(m_statPrintPeriodSeconds, "print_stats_period", DEFAULT_STATS_PRINT_PERIOD, 1000000);
    UPDATE_TIME_CFG(m_statPrintFullPeriodSeconds, "print_full_stats_period", DEFAULT_FULL_STATS_PRINT_PERIOD, 1000000);
    UPDATE_CFG(m_enableLogRecoveryStats, "enable_log_recovery_stats", DEFAULT_ENABLE_LOG_RECOVERY_STATS);
    UPDATE_CFG(m_enableDbSessionStatistics, "enable_db_session_stats", DEFAULT_ENABLE_DB_SESSION_STAT_PRINT);
    UPDATE_CFG(m_enableNetworkStatistics, "enable_network_stats", DEFAULT_ENABLE_NETWORK_STAT_PRINT);
    UPDATE_CFG(m_enableLogStatistics, "enable_log_stats", DEFAULT_ENABLE_LOG_STAT_PRINT);
    UPDATE_CFG(m_enableMemoryStatistics, "enable_memory_stats", DEFAULT_ENABLE_MEMORY_STAT_PRINT);
    UPDATE_CFG(
        m_enableDetailedMemoryStatistics, "enable_detailed_memory_stats", DEFAULT_ENABLE_DETAILED_MEMORY_STAT_PRINT);
    UPDATE_CFG(m_enableProcessStatistics, "enable_process_stats", DEFAULT_ENABLE_PROCESS_STAT_PRINT);
    UPDATE_CFG(m_enableSystemStatistics, "enable_system_stats", DEFAULT_ENABLE_SYSTEM_STAT_PRINT);
    UPDATE_CFG(m_enableJitStatistics, "enable_jit_stats", DEFAULT_ENABLE_JIT_STAT_PRINT);

    // log configuration
    UPDATE_USER_CFG(m_logLevel, "log_level", DEFAULT_LOG_LEVEL);
    SetGlobalLogLevel(m_logLevel);
    UPDATE_USER_CFG(m_numaErrorsLogLevel, "numa_errors_log_level", DEFAULT_NUMA_ERRORS_LOG_LEVEL);
    UPDATE_USER_CFG(m_numaWarningsLogLevel, "numa_warnings_log_level", DEFAULT_NUMA_WARNINGS_LOG_LEVEL);
    UPDATE_USER_CFG(m_cfgStartupLogLevel, "cfg_startup_log_level", DEFAULT_CFG_STARTUP_LOG_LEVEL);

    // memory configuration
    UPDATE_INT_CFG(m_maxThreads, "max_threads", DEFAULT_MAX_THREADS);
    UPDATE_INT_CFG(m_maxConnections, "max_connections", DEFAULT_MAX_CONNECTIONS);
    UPDATE_USER_CFG(m_sessionAffinityMode, "affinity_mode", DEFAULT_AFFINITY_MODE);
    // we save copies because main affinity mode may be overridden when thread pool is used
    if (IS_AFFINITY_ACTIVE(m_sessionAffinityMode)) {  // update only if active
        m_taskAffinityMode = m_sessionAffinityMode;
    }
    UPDATE_CFG(m_lazyLoadChunkDirectory, "lazy_load_chunk_directory", DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY);
    UPDATE_MEM_CFG(m_globalMemoryMaxLimitMB, "max_mot_global_memory", DEFAULT_MAX_MOT_GLOBAL_MEMORY, MEGA_BYTE);
    UPDATE_MEM_CFG(m_globalMemoryMinLimitMB, "min_mot_global_memory", DEFAULT_MIN_MOT_GLOBAL_MEMORY, MEGA_BYTE);
    UPDATE_MEM_CFG(m_localMemoryMaxLimitMB, "max_mot_local_memory", DEFAULT_MAX_MOT_LOCAL_MEMORY, MEGA_BYTE);
    UPDATE_MEM_CFG(m_localMemoryMinLimitMB, "min_mot_local_memory", DEFAULT_MIN_MOT_LOCAL_MEMORY, MEGA_BYTE);
    UPDATE_ABS_MEM_CFG(m_sessionMemoryMaxLimitKB, "max_mot_session_memory", DEFAULT_MAX_MOT_SESSION_MEMORY, KILO_BYTE);
    UPDATE_ABS_MEM_CFG(m_sessionMemoryMinLimitKB, "min_mot_session_memory", DEFAULT_MIN_MOT_SESSION_MEMORY, KILO_BYTE);
    UPDATE_USER_CFG(m_reserveMemoryMode, "reserve_memory_mode", DEFAULT_RESERVE_MEMORY_MODE);
    UPDATE_USER_CFG(m_storeMemoryPolicy, "store_memory_policy", DEFAULT_STORE_MEMORY_POLICY);
    UPDATE_USER_CFG(m_chunkAllocPolicy, "chunk_alloc_policy", DEFAULT_CHUNK_ALLOC_POLICY);
    UPDATE_INT_CFG(m_chunkPreallocWorkerCount, "chunk_prealloc_worker_count", DEFAULT_CHUNK_PREALLOC_WORKER_COUNT);
    UPDATE_INT_CFG(m_highRedMarkPercent, "high_red_mark_percent", DEFAULT_HIGH_RED_MARK_PERCENT);
    UPDATE_ABS_MEM_CFG(m_sessionLargeBufferStoreSizeMB,
        "session_large_buffer_store_size",
        DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE,
        MEGA_BYTE);
    UPDATE_ABS_MEM_CFG(m_sessionLargeBufferStoreMaxObjectSizeMB,
        "session_large_buffer_store_max_object_size",
        DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE,
        MEGA_BYTE);
    UPDATE_ABS_MEM_CFG(
        m_sessionMaxHugeObjectSizeMB, "session_max_huge_object_size", DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE, MEGA_BYTE);

    // GC configuration
    UPDATE_CFG(m_gcEnable, "enable_gc", DEFAULT_GC_ENABLE);
    UPDATE_MEM_CFG(m_gcReclaimThresholdBytes, "reclaim_threshold", DEFAULT_GC_RECLAIM_THRESHOLD, 1);
    UPDATE_INT_CFG(m_gcReclaimBatchSize, "reclaim_batch_size", DEFAULT_GC_RECLAIM_BATCH_SIZE);
    UPDATE_MEM_CFG(m_gcHighReclaimThresholdBytes, "high_reclaim_threshold", DEFAULT_GC_HIGH_RECLAIM_THRESHOLD, 1);

    // JIT configuration
    UPDATE_CFG(m_enableCodegen, "enable_mot_codegen", DEFAULT_ENABLE_MOT_CODEGEN);
    UPDATE_CFG(m_forcePseudoCodegen, "force_mot_pseudo_codegen", DEFAULT_FORCE_MOT_PSEUDO_CODEGEN);
    UPDATE_CFG(m_enableCodegenPrint, "enable_mot_codegen_print", DEFAULT_ENABLE_MOT_CODEGEN_PRINT);
    UPDATE_INT_CFG(m_codegenLimit, "mot_codegen_limit", DEFAULT_MOT_CODEGEN_LIMIT);

    // storage configuration
    UPDATE_CFG(m_allowIndexOnNullableColumn, "allow_index_on_nullable_column", DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN);
    UPDATE_USER_CFG(m_indexTreeFlavor, "index_tree_flavor", DEFAULT_INDEX_TREE_FLAVOR);

    // general configuration
    UPDATE_TIME_CFG(m_configMonitorPeriodSeconds, "config_update_period", DEFAULT_CFG_MONITOR_PERIOD, 1000000);
    UPDATE_CFG(m_runInternalConsistencyValidation,
        "internal_consistency_validation",
        DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION);

    // load component log levels
    UpdateComponentLogLevel();

    MOT_LOG_TRACE("Main configuration loaded");
}

void MOTConfiguration::UpdateComponentLogLevel()
{
    LogLevel globalLogLevel = GetGlobalLogLevel();
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();
    const ConfigSection* cfgSection = cfg->GetConfigSection("Log");
    if (cfgSection != nullptr) {
        // all sub-sections are logger component configuration sections
        mot_string_list componentNames;
        if (!cfgSection->GetConfigSectionNames(componentNames)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to retrieve section names for section %s",
                cfgSection->GetFullPathName());
            return;
        }
        mot_string_list::const_iterator compItr = componentNames.cbegin();
        while (compItr != componentNames.cend()) {
            const mot_string& componentName = *compItr;
            MOT_LOG_DEBUG("Loading component %s log level", componentName.c_str());
            const ConfigSection* componentCfg = cfgSection->GetConfigSection(componentName.c_str());

            // configure component log level first then override log level specific loggers in the component
            LogLevel componentLevel = componentCfg->GetUserConfigValue<LogLevel>("log_level", globalLogLevel);
            if (componentLevel != globalLogLevel) {
                mot_string logLevelStr;
                MOT_LOG_INFO("Updating the log level of component %s to: %s",
                    componentName.c_str(),
                    TypeFormatter<LogLevel>::ToString(componentLevel, logLevelStr));
                SetLogComponentLogLevel(componentName.c_str(), componentLevel);
            }

            // all configuration values are logger configuration pairs (loggerName=log_level)
            mot_string_list loggerNames;
            if (!componentCfg->GetConfigValueNames(loggerNames)) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to retrieve logger names for section %s",
                    componentCfg->GetFullPathName());
                return;
            }
            mot_string_list::const_iterator loggerItr = loggerNames.cbegin();
            while (loggerItr != loggerNames.cend()) {
                const mot_string& loggerName = *loggerItr;
                if (loggerName.compare("log_level") != 0) {  // skip special value for entire component log level
                    MOT_LOG_DEBUG(
                        "Loading component/logger %s/%s log level", componentName.c_str(), loggerName.c_str());
                    LogLevel loggerLevel =
                        componentCfg->GetUserConfigValue<LogLevel>(loggerName.c_str(), LogLevel::LL_INFO);
                    if (loggerLevel != LogLevel::LL_INFO) {
                        mot_string logLevelStr;
                        MOT_LOG_INFO("Updating the log level of logger %s in component %s to: %s",
                            loggerName.c_str(),
                            componentName.c_str(),
                            TypeFormatter<LogLevel>::ToString(loggerLevel, logLevelStr));
                        SetLoggerLogLevel(componentName.c_str(), loggerName.c_str(), loggerLevel);
                    }
                }
                ++loggerItr;
            }

            ++compItr;
        }
    }
}

void MOTConfiguration::UpdateConfigItem(bool& oldValue, bool newValue, const char* name)
{
    if (oldValue != newValue) {
        MOT_LOG_TRACE(
            "Configuration of %s changed: %s --> %s", name, oldValue ? "TRUE" : "FALSE", newValue ? "TRUE" : "FALSE");
        oldValue = newValue;
    }
}

void MOTConfiguration::UpdateConfigItem(std::string& oldValue, const char* newValue, const char* name)
{
    if (oldValue != newValue) {
        MOT_LOG_TRACE("Configuration of %s changed: %s --> %s", name, oldValue.c_str(), newValue);
        oldValue = newValue;
    }
}

int MOTConfiguration::ParseMemoryPercent(const char* memoryValue)
{
    int result = -1;
    char* endptr = NULL;
    int percent = strtol(memoryValue, &endptr, 0);
    if (endptr == memoryValue) {
        MOT_LOG_WARN("Invalid memory value format: %s (expecting percentage digits)", memoryValue);
    } else if (*endptr != '%') {
        MOT_LOG_WARN("Invalid memory value format: %s (expecting percentage sign after percentage value)", memoryValue);
    } else if ((percent < 0) || (percent > 100)) {
        MOT_LOG_WARN("Invalid memory value format: %s (percentage value not in the range [0, 100])", memoryValue);
    } else {
        mot_string extra(endptr + 1);
        extra.trim();
        if (extra.length() != 0) {
            MOT_LOG_WARN("Invalid memory value format: %s (trailing characters after %%)", memoryValue);
        } else {
            MOT_LOG_TRACE("Parsed percentage: %d%%", percent);
            result = percent;
        }
    }
    return result;
}

uint64_t MOTConfiguration::ParseMemoryValueBytes(const char* memoryValue, uint64_t defaultValue, const char* cfgPath)
{
    // we expect one of the following formats:
    // <Number><Possible whitespace><Case Insensitive Unit: TB, GB, MB, KB, bytes>
    // <Percent><At least 1 whitespace><total/available>
    // we differentiate between the two formats by keywords: total/available
    uint64_t result = defaultValue;
    if (strchr(memoryValue, '%')) {
        result = ParseMemoryPercentTotal(memoryValue, defaultValue, cfgPath);
    } else {
        result = ParseMemoryUnit(memoryValue, defaultValue, cfgPath);
    }
    MOT_LOG_TRACE("Final memory value: %" PRIu64 " bytes", result);
    return result;
}

uint64_t MOTConfiguration::ParseMemoryPercentTotal(const char* memoryValue, uint64_t defaultValue, const char* cfgPath)
{
    uint64_t memoryValueBytes = defaultValue;
    // we expect to parse a number between 0-100, and then followed by "%"
    int percent = ParseMemoryPercent(memoryValue);
    if (percent >= 0) {
        memoryValueBytes = ((uint64_t)m_totalMemoryMb) * MEGA_BYTE * percent / 100;
        MOT_LOG_INFO(
            "Loaded %s: %d%% from total = %" PRIu64 " MB", cfgPath, percent, memoryValueBytes / 1024ul / 1024ul);
    } else {
        MOT_LOG_WARN("Invalid %s memory format: illegal percent specification", cfgPath);
    }
    return memoryValueBytes;
}

uint64_t MOTConfiguration::ParseMemoryUnit(const char* memoryValue, uint64_t defaultValue, const char* cfgPath)
{
    uint64_t memoryValueBytes = defaultValue;
    char* endptr = NULL;
    unsigned long long value = strtoull(memoryValue, &endptr, 0);
    if (endptr == memoryValue) {
        MOT_LOG_WARN("Invalid %s memory value format: %s (expecting value digits)", cfgPath, memoryValue);
    } else if (*endptr == 0) {
        MOT_LOG_WARN("Invalid %s memory value format: %s (expecting unit type after value)", cfgPath, memoryValue);
    } else {
        // get unit type and convert to mega-bytes
        mot_string suffix(endptr);
        suffix.trim();
        if (suffix.compare_no_case("TB") == 0) {
            MOT_LOG_TRACE("Loaded %s: %u TB", cfgPath, value);
            memoryValueBytes = ((uint64_t)value) * 1024ull * 1024ull * 1024ull * 1024ull;
        } else if (suffix.compare_no_case("GB") == 0) {
            MOT_LOG_TRACE("Loaded %s: %u GB", cfgPath, value);
            memoryValueBytes = ((uint64_t)value) * 1024ull * 1024ull * 1024ull;
        } else if (suffix.compare_no_case("MB") == 0) {
            MOT_LOG_TRACE("Loaded %s: %u MB", cfgPath, value);
            memoryValueBytes = ((uint64_t)value) * 1024ull * 1024ull;
        } else if (suffix.compare_no_case("KB") == 0) {
            MOT_LOG_TRACE("Loaded %s: %u KB", cfgPath, value);
            memoryValueBytes = ((uint64_t)value) * 1024;
        } else if (suffix.compare_no_case("bytes") == 0) {
            MOT_LOG_TRACE("Loaded %s: %u bytes", cfgPath, value);
            memoryValueBytes = value;
        } else {
            MOT_LOG_WARN("Invalid %s memory value format: %s (invalid unit specifier '%s' - should be one of TB, GB, "
                         "MB, KB or bytes)",
                cfgPath,
                memoryValue,
                suffix.c_str());
        }
    }
    return memoryValueBytes;
}

uint64_t MOTConfiguration::ParseTimeValueMicros(const char* timeValue, uint64_t defaultValue, const char* cfgPath)
{
    uint64_t timeValueMicors = defaultValue;
    char* endptr = NULL;
    unsigned long long value = strtoull(timeValue, &endptr, 0);
    if (endptr == timeValue) {
        MOT_LOG_WARN("Invalid %s time value format: %s (expecting value digits)", cfgPath, timeValue);
    } else if (*endptr == 0) {
        MOT_LOG_WARN("Invalid %s time value format: %s (expecting unit type after value)", cfgPath, timeValue);
    } else {
        // get unit type and convert to mega-bytes
        mot_string suffix(endptr);
        suffix.trim();
        if ((suffix.compare_no_case("d") == 0) || (suffix.compare_no_case("days") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u days", cfgPath, value);
            timeValueMicors = ((uint64_t)value) * 24ull * 60ull * 60ull * 1000ull * 1000ull;
        } else if ((suffix.compare_no_case("h") == 0) || (suffix.compare_no_case("hours") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u hours", cfgPath, value);
            timeValueMicors = ((uint64_t)value) * 60ull * 60ull * 1000ull * 1000ull;
        } else if ((suffix.compare_no_case("m") == 0) || (suffix.compare_no_case("mins") == 0) ||
                   (suffix.compare_no_case("minutes") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u minutes", cfgPath, value);
            timeValueMicors = ((uint64_t)value) * 60ull * 1000ull * 1000ull;
        } else if ((suffix.compare_no_case("s") == 0) || (suffix.compare_no_case("secs") == 0) ||
                   (suffix.compare_no_case("seconds") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u seconds", cfgPath, value);
            timeValueMicors = ((uint64_t)value) * 1000ull * 1000ull;
        } else if ((suffix.compare_no_case("ms") == 0) || (suffix.compare_no_case("millis") == 0) ||
                   (suffix.compare_no_case("milliseocnds") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u milli-seconds", cfgPath, value);
            timeValueMicors = ((uint64_t)value) * 1000ull;
        } else if ((suffix.compare_no_case("us") == 0) || (suffix.compare_no_case("micros") == 0) ||
                   (suffix.compare_no_case("microseocnds") == 0)) {
            MOT_LOG_TRACE("Loaded %s: %u micro-seconds", cfgPath, value);
            timeValueMicors = ((uint64_t)value);
        } else {
            MOT_LOG_WARN("Invalid %s time value format: %s (invalid unit specifier '%s' - should be one of d, h, m, s, "
                         "ms or us)",
                cfgPath,
                timeValue,
                suffix.c_str());
        }
    }
    return timeValueMicors;
}

bool MOTConfiguration::CheckHyperThreads()
{
    std::string result = ExecOsCommand(IS_HYPER_THREAD_CMD);
    char r = '0';

    if (result.length() >= 1) {
        r = result[0];
    }

    if (r != '1') {
        return true;
    } else {
        return false;
    }
}
}  // namespace MOT
