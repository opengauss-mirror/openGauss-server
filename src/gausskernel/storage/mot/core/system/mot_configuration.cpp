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
 *    src/gausskernel/storage/mot/core/system/mot_configuration.cpp
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
// Memory scaling constants
constexpr uint64_t MOTConfiguration::SCALE_BYTES;
constexpr uint64_t MOTConfiguration::SCALE_KILO_BYTES;
constexpr uint64_t MOTConfiguration::SCALE_MEGA_BYTES;
// Time scaling constants
constexpr uint64_t MOTConfiguration::SCALE_MICROS;
constexpr uint64_t MOTConfiguration::SCALE_MILLIS;
constexpr uint64_t MOTConfiguration::SCALE_SECONDS;
// redo-log configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_REDO_LOG;
constexpr LoggerType MOTConfiguration::DEFAULT_LOGGER_TYPE;
constexpr RedoLogHandlerType MOTConfiguration::DEFAULT_REDO_LOG_HANDLER_TYPE;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_GROUP_COMMIT;
constexpr uint64_t MOTConfiguration::DEFAULT_GROUP_COMMIT_SIZE;
constexpr uint64_t MOTConfiguration::MIN_GROUP_COMMIT_SIZE;
constexpr uint64_t MOTConfiguration::MAX_GROUP_COMMIT_SIZE;
constexpr const char* MOTConfiguration::DEFAULT_GROUP_COMMIT_TIMEOUT;
constexpr uint64_t MOTConfiguration::DEFAULT_GROUP_COMMIT_TIMEOUT_USEC;
constexpr uint64_t MOTConfiguration::MIN_GROUP_COMMIT_TIMEOUT_USEC;
constexpr uint64_t MOTConfiguration::MAX_GROUP_COMMIT_TIMEOUT_USEC;
// checkpoint configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_CHECKPOINT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_INCREMENTAL_CHECKPOINT;
constexpr const char* MOTConfiguration::DEFAULT_CHECKPOINT_DIR;
constexpr const char* MOTConfiguration::DEFAULT_CHECKPOINT_SEGSIZE;
constexpr uint64_t MOTConfiguration::DEFAULT_CHECKPOINT_SEGSIZE_BYTES;
constexpr uint64_t MOTConfiguration::MIN_CHECKPOINT_SEGSIZE_BYTES;
constexpr uint64_t MOTConfiguration::MAX_CHECKPOINT_SEGSIZE_BYTES;
constexpr uint32_t MOTConfiguration::DEFAULT_CHECKPOINT_WORKERS;
constexpr uint32_t MOTConfiguration::MIN_CHECKPOINT_WORKERS;
constexpr uint32_t MOTConfiguration::MAX_CHECKPOINT_WORKERS;
// recovery configuration members
constexpr uint32_t MOTConfiguration::DEFAULT_CHECKPOINT_RECOVERY_WORKERS;
constexpr uint32_t MOTConfiguration::MIN_CHECKPOINT_RECOVERY_WORKERS;
constexpr uint32_t MOTConfiguration::MAX_CHECKPOINT_RECOVERY_WORKERS;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_LOG_RECOVERY_STATS;
constexpr RecoveryMode MOTConfiguration::DEFAULT_RECOVERY_MODE;
constexpr uint32_t MOTConfiguration::DEFAULT_PARALLEL_RECOVERY_WORKERS;
constexpr uint32_t MOTConfiguration::MIN_PARALLEL_RECOVERY_WORKERS;
constexpr uint32_t MOTConfiguration::MAX_PARALLEL_RECOVERY_WORKERS;
constexpr uint32_t MOTConfiguration::DEFAULT_PARALLEL_RECOVERY_QUEUE_SIZE;
constexpr uint32_t MOTConfiguration::MIN_PARALLEL_RECOVERY_QUEUE_SIZE;
constexpr uint32_t MOTConfiguration::MAX_PARALLEL_RECOVERY_QUEUE_SIZE;
// machine configuration members
constexpr uint16_t MOTConfiguration::DEFAULT_NUMA_NODES;
constexpr uint16_t MOTConfiguration::DEFAULT_CORES_PER_CPU;
// statistics configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_STATS;
constexpr const char* MOTConfiguration::DEFAULT_STATS_PRINT_PERIOD;
constexpr uint64_t MOTConfiguration::DEFAULT_STATS_PRINT_PERIOD_SECONDS;
constexpr uint64_t MOTConfiguration::MIN_STATS_PRINT_PERIOD_SECONDS;
constexpr uint64_t MOTConfiguration::MAX_STATS_PRINT_PERIOD_SECONDS;
constexpr const char* MOTConfiguration::DEFAULT_FULL_STATS_PRINT_PERIOD;
constexpr uint64_t MOTConfiguration::DEFAULT_FULL_STATS_PRINT_PERIOD_SECONDS;
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
constexpr bool MOTConfiguration::DEFAULT_ENABLE_NUMA;
constexpr uint16_t MOTConfiguration::DEFAULT_MAX_THREADS;
constexpr uint16_t MOTConfiguration::MIN_MAX_THREADS;
constexpr uint16_t MOTConfiguration::MAX_MAX_THREADS;
constexpr uint32_t MOTConfiguration::DEFAULT_MAX_CONNECTIONS;
constexpr uint32_t MOTConfiguration::MIN_MAX_CONNECTIONS;
constexpr uint32_t MOTConfiguration::MAX_MAX_CONNECTIONS;
constexpr AffinityMode MOTConfiguration::DEFAULT_AFFINITY_MODE;
constexpr bool MOTConfiguration::DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_GLOBAL_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MAX_MOT_GLOBAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MIN_MAX_MOT_GLOBAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MAX_MAX_MOT_GLOBAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_LOCAL_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MAX_MOT_LOCAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MIN_MAX_MOT_LOCAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MAX_MAX_MOT_LOCAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_GLOBAL_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MIN_MOT_GLOBAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MIN_MIN_MOT_GLOBAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MAX_MIN_MOT_GLOBAL_MEMORY_MB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_LOCAL_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MIN_MOT_LOCAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MIN_MIN_MOT_LOCAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MAX_MIN_MOT_LOCAL_MEMORY_MB;
constexpr uint64_t MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB;
constexpr const char* MOTConfiguration::DEFAULT_MAX_MOT_SESSION_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MAX_MOT_SESSION_MEMORY_KB;
constexpr uint64_t MOTConfiguration::MIN_MAX_MOT_SESSION_MEMORY_KB;
constexpr uint64_t MOTConfiguration::MAX_MAX_MOT_SESSION_MEMORY_KB;
constexpr const char* MOTConfiguration::DEFAULT_MIN_MOT_SESSION_MEMORY;
constexpr uint64_t MOTConfiguration::DEFAULT_MIN_MOT_SESSION_MEMORY_KB;
constexpr uint64_t MOTConfiguration::MIN_MIN_MOT_SESSION_MEMORY_KB;
constexpr uint64_t MOTConfiguration::MAX_MIN_MOT_SESSION_MEMORY_KB;
constexpr MemReserveMode MOTConfiguration::DEFAULT_RESERVE_MEMORY_MODE;
constexpr MemStorePolicy MOTConfiguration::DEFAULT_STORE_MEMORY_POLICY;
constexpr MemAllocPolicy MOTConfiguration::DEFAULT_CHUNK_ALLOC_POLICY;
constexpr uint32_t MOTConfiguration::DEFAULT_CHUNK_PREALLOC_WORKER_COUNT;
constexpr uint32_t MOTConfiguration::MIN_CHUNK_PREALLOC_WORKER_COUNT;
constexpr uint32_t MOTConfiguration::MAX_CHUNK_PREALLOC_WORKER_COUNT;
constexpr uint32_t MOTConfiguration::DEFAULT_HIGH_RED_MARK_PERCENT;
constexpr uint32_t MOTConfiguration::MIN_HIGH_RED_MARK_PERCENT;
constexpr uint32_t MOTConfiguration::MAX_HIGH_RED_MARK_PERCENT;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE;
constexpr uint64_t MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE_MB;
constexpr uint64_t MOTConfiguration::MIN_SESSION_LARGE_BUFFER_STORE_SIZE_MB;
constexpr uint64_t MOTConfiguration::MAX_SESSION_LARGE_BUFFER_STORE_SIZE_MB;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE;
constexpr uint64_t MOTConfiguration::DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB;
constexpr uint64_t MOTConfiguration::MIN_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB;
constexpr uint64_t MOTConfiguration::MAX_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB;
constexpr const char* MOTConfiguration::DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE;
constexpr uint64_t MOTConfiguration::DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE_MB;
constexpr uint64_t MOTConfiguration::MIN_SESSION_MAX_HUGE_OBJECT_SIZE_MB;
constexpr uint64_t MOTConfiguration::MAX_SESSION_MAX_HUGE_OBJECT_SIZE_MB;
// GC configuration members
constexpr bool MOTConfiguration::DEFAULT_GC_ENABLE;
constexpr const char* MOTConfiguration::DEFAULT_GC_RECLAIM_THRESHOLD;
constexpr uint64_t MOTConfiguration::DEFAULT_GC_RECLAIM_THRESHOLD_BYTES;
constexpr uint64_t MOTConfiguration::MIN_GC_RECLAIM_THRESHOLD_BYTES;
constexpr uint64_t MOTConfiguration::MAX_GC_RECLAIM_THRESHOLD_BYTES;
constexpr uint32_t MOTConfiguration::DEFAULT_GC_RECLAIM_BATCH_SIZE;
constexpr uint32_t MOTConfiguration::MIN_GC_RECLAIM_BATCH_SIZE;
constexpr uint32_t MOTConfiguration::MAX_GC_RECLAIM_BATCH_SIZE;
constexpr const char* MOTConfiguration::DEFAULT_GC_HIGH_RECLAIM_THRESHOLD;
constexpr uint64_t MOTConfiguration::DEFAULT_GC_HIGH_RECLAIM_THRESHOLD_BYTES;
constexpr uint64_t MOTConfiguration::MIN_GC_HIGH_RECLAIM_THRESHOLD_BYTES;
constexpr uint64_t MOTConfiguration::MAX_GC_HIGH_RECLAIM_THRESHOLD_BYTES;
// JIT configuration members
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_CODEGEN;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_QUERY_CODEGEN;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_SP_CODEGEN;
constexpr const char* MOTConfiguration::DEFAULT_MOT_SP_CODEGEN_ALLOWED;
constexpr const char* MOTConfiguration::DEFAULT_MOT_SP_CODEGEN_PROHIBITED;
constexpr const char* MOTConfiguration::DEFAULT_MOT_PURE_SP_CODEGEN;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_CODEGEN_PRINT;
constexpr uint32_t MOTConfiguration::DEFAULT_MOT_CODEGEN_LIMIT;
constexpr uint32_t MOTConfiguration::MIN_MOT_CODEGEN_LIMIT;
constexpr uint32_t MOTConfiguration::MAX_MOT_CODEGEN_LIMIT;
constexpr bool MOTConfiguration::DEFAULT_ENABLE_MOT_CODEGEN_PROFILE;
// storage configuration
constexpr bool MOTConfiguration::DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN;
constexpr IndexTreeFlavor MOTConfiguration::DEFAULT_INDEX_TREE_FLAVOR;
// general configuration members
constexpr const char* MOTConfiguration::DEFAULT_CFG_MONITOR_PERIOD;
constexpr uint64_t MOTConfiguration::DEFAULT_CFG_MONITOR_PERIOD_SECONDS;
constexpr uint64_t MOTConfiguration::MIN_CFG_MONITOR_PERIOD_SECONDS;
constexpr uint64_t MOTConfiguration::MAX_CFG_MONITOR_PERIOD_SECONDS;
constexpr bool MOTConfiguration::DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION;
constexpr uint64_t MOTConfiguration::DEFAULT_TOTAL_MEMORY_MB;

static constexpr int MAX_NUMA_NODES = 16;

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

static bool ParseRecoveryMode(const std::string& cfgName, const std::string& variableName, const std::string& newValue,
    RecoveryMode* variableValue)
{
    bool result = (cfgName == variableName);
    if (result) {
        *variableValue = RecoveryModeFromString(newValue.c_str());
        if (*variableValue == RecoveryMode::RECOVERY_INVALID) {
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
        *variableValue = static_cast<uint16_t>(std::stoul(newValue));
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
    if (*maxNodes <= 0) {
        MOT_LOG_ERROR("Invalid NUMA configuration, some NUMA node has no memory");
        return false;
    }

    if (*maxNodes >= MAX_NUMA_NODES) {
        MOT_LOG_ERROR("sys_numa_num_configured_nodes() => %d, while MAX_NUMA_NODES=%d - cannot proceed",
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
        if (node >= MAX_NUMA_NODES) {
            MOT_LOG_ERROR(
                "CPU %d is located in node %d, while MAX_NUMA_NODES=%d - cannot proceed", i, node, MAX_NUMA_NODES);
            return false;
        }
        cpusPerNode[node]++;
        (*cpuNodeMapper)[i] = node;
        (void)(*cpuOsMapper)[node].insert(i);
    }

    // dynamically calculate number of CPU cores per NUMA node instead of hard-coded config value
    if (maxCoresPerNode != nullptr) {
        *maxCoresPerNode = 0;
        for (int j = 0; j < MAX_NUMA_NODES; j++) {
            if (cpusPerNode[j] > *maxCoresPerNode) {
                *maxCoresPerNode = cpusPerNode[j];
            }
        }
    }

    return true;
}

void MOTConfiguration::SetMaskToAllCoresinNumaSocketByCoreId(cpu_set_t& mask, int coreId)
{
    int nodeId = GetCpuNode(coreId);
    auto nodeMap = m_osCpuMap[nodeId];
    for (auto it = nodeMap.begin(); it != nodeMap.end(); (void)++it) {
        if (MotSysNumaCpuAllowed(*it)) {
            CPU_SET(*it, &mask);
        }
    }
}

void MOTConfiguration::SetMaskToAllCoresinNumaSocketByNodeId(cpu_set_t& mask, int nodeId)
{
    auto nodeMap = m_osCpuMap[nodeId];
    for (auto it = nodeMap.begin(); it != nodeMap.end(); (void)++it) {
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
      m_recoveryMode(DEFAULT_RECOVERY_MODE),
      m_parallelRecoveryWorkers(DEFAULT_PARALLEL_RECOVERY_WORKERS),
      m_parallelRecoveryQueueSize(DEFAULT_PARALLEL_RECOVERY_QUEUE_SIZE),
      m_checkpointRecoveryWorkers(DEFAULT_CHECKPOINT_RECOVERY_WORKERS),
      m_abortBufferEnable(true),
      m_preAbort(true),
      m_validationLock(TxnValidation::TXN_VALIDATION_NO_WAIT),
      m_numaNodes(DEFAULT_NUMA_NODES),
      m_coresPerCpu(DEFAULT_CORES_PER_CPU),
      m_numaAvailable(true),
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
      m_enableNuma(DEFAULT_ENABLE_NUMA),
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
      m_gcReclaimThresholdBytes(DEFAULT_GC_RECLAIM_THRESHOLD_BYTES),
      m_gcReclaimBatchSize(DEFAULT_GC_RECLAIM_BATCH_SIZE),
      m_gcHighReclaimThresholdBytes(DEFAULT_GC_HIGH_RECLAIM_THRESHOLD_BYTES),
      m_enableCodegen(DEFAULT_ENABLE_MOT_CODEGEN),
      m_enableQueryCodegen(DEFAULT_ENABLE_MOT_QUERY_CODEGEN),
      m_enableSPCodegen(DEFAULT_ENABLE_MOT_SP_CODEGEN),
      m_spCodegenAllowed(DEFAULT_MOT_SP_CODEGEN_ALLOWED),
      m_spCodegenProhibited(DEFAULT_MOT_SP_CODEGEN_PROHIBITED),
      m_pureSPCodegen(DEFAULT_MOT_PURE_SP_CODEGEN),
      m_enableCodegenPrint(DEFAULT_ENABLE_MOT_CODEGEN_PRINT),
      m_codegenLimit(DEFAULT_MOT_CODEGEN_LIMIT),
      m_enableCodegenProfile(DEFAULT_ENABLE_MOT_CODEGEN_PROFILE),
      m_allowIndexOnNullableColumn(DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN),
      m_indexTreeFlavor(DEFAULT_INDEX_TREE_FLAVOR),
      m_configMonitorPeriodSeconds(DEFAULT_CFG_MONITOR_PERIOD_SECONDS),
      m_runInternalConsistencyValidation(DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION),
      m_runInternalMvccConsistencyValidation(DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION),
      m_totalMemoryMb(DEFAULT_TOTAL_MEMORY_MB),
      m_suppressLog(0),
      m_loadExtraParams(false)
{}

void MOTConfiguration::Initialize()
{
    // Since MOTConfiguration has a global instance it is initialized early (before main() or other code is called)
    // we must initialize the sys_numa API early enough. This look likes the right timing.
    MotSysNumaInit();
    int numa = DEFAULT_NUMA_NODES;
    if (FindNumaNodes(&numa)) {
        m_numaNodes = static_cast<uint16_t>(numa);
    } else {
        MOT_LOG_WARN("Failed to infer the number of NUMA nodes on current machine, defaulting to %d", numa);
        m_numaAvailable = false;
    }

    uint16_t cores = DEFAULT_CORES_PER_CPU;
    if (FindNumProcessors(&cores, &m_cpuNodeMapper, &m_osCpuMap)) {
        m_coresPerCpu = cores;
    } else {
        MOT_LOG_WARN("Failed to infer the number of cores on the current machine, defaulting to %" PRIu16, cores);
        m_numaAvailable = false;
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
    } else if (ParseUint64(name, "checkpoint_segsize", value, &m_checkpointSegThreshold)) {
    } else if (ParseUint32(name, "checkpoint_workers", value, &m_checkpointWorkers)) {
    } else if (ParseUint32(name, "checkpoint_recovery_workers", value, &m_checkpointRecoveryWorkers)) {
    } else if (ParseRecoveryMode(name, "recovery_mode", value, &m_recoveryMode)) {
    } else if (ParseUint32(name, "parallel_recovery_workers", value, &m_parallelRecoveryWorkers)) {
    } else if (ParseUint32(name, "parallel_recovery_queue_size", value, &m_parallelRecoveryQueueSize)) {
    } else if (ParseBool(name, "abort_buffer_enable", value, &m_abortBufferEnable)) {
    } else if (ParseBool(name, "pre_abort", value, &m_preAbort)) {
    } else if (ParseValidation(name, "validation_lock", value, &m_validationLock)) {
    } else if (ParseBool(name, "enable_stats", value, &m_enableStats)) {
    } else if (ParseUint64(name, "stats_period_seconds", value, &m_statPrintPeriodSeconds)) {
    } else if (ParseUint64(name, "full_stats_period_seconds", value, &m_statPrintFullPeriodSeconds)) {
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
    } else if (ParseBool(name, "enable_numa", value, &m_enableNuma)) {
    } else if (ParseUint16(name, "max_threads", value, &m_maxThreads)) {
    } else if (ParseUint32(name, "max_connections", value, &m_maxConnections)) {
    } else if (ParseAffinity(name, "affinity_mode", value, &m_sessionAffinityMode)) {
    } else if (ParseBool(name, "lazy_load_chunk_directory", value, &m_lazyLoadChunkDirectory)) {
    } else if (ParseUint64(name, "max_mot_global_memory_mb", value, &m_globalMemoryMaxLimitMB)) {
    } else if (ParseUint64(name, "min_mot_global_memory_mb", value, &m_globalMemoryMinLimitMB)) {
    } else if (ParseUint64(name, "max_mot_local_memory_mb", value, &m_localMemoryMaxLimitMB)) {
    } else if (ParseUint64(name, "min_mot_local_memory_mb", value, &m_localMemoryMinLimitMB)) {
    } else if (ParseUint64(name, "max_mot_session_memory_kb", value, &m_sessionMemoryMaxLimitKB)) {
    } else if (ParseUint64(name, "min_mot_session_memory_kb", value, &m_sessionMemoryMinLimitKB)) {
    } else if (ParseMemoryReserveMode(name, "reserve_memory_mode", value, &m_reserveMemoryMode)) {
    } else if (ParseMemoryStorePolicy(name, "store_memory_policy", value, &m_storeMemoryPolicy)) {
    } else if (ParseChunkAllocPolicy(name, "chunk_alloc_policy", value, &m_chunkAllocPolicy)) {
    } else if (ParseUint32(name, "chunk_prealloc_worker_count", value, &m_chunkPreallocWorkerCount)) {
    } else if (ParseUint32(name, "high_red_mark_percent", value, &m_highRedMarkPercent)) {
    } else if (ParseUint64(name, "session_large_buffer_store_size_mb", value, &m_sessionLargeBufferStoreSizeMB)) {
    } else if (ParseUint64(name,
                   "session_large_buffer_store_max_object_size_mb",
                   value,
                   &m_sessionLargeBufferStoreMaxObjectSizeMB)) {
    } else if (ParseUint64(name, "session_max_huge_object_size_mb", value, &m_sessionMaxHugeObjectSizeMB)) {
    } else if (ParseBool(name, "enable_mot_codegen", value, &m_enableCodegen)) {
    } else if (ParseBool(name, "enable_mot_query_codegen", value, &m_enableQueryCodegen)) {
    } else if (ParseBool(name, "enable_mot_sp_codegen", value, &m_enableSPCodegen)) {
    } else if (ParseBool(name, "enable_mot_codegen_print", value, &m_enableCodegenPrint)) {
    } else if (ParseUint32(name, "mot_codegen_limit", value, &m_codegenLimit)) {
    } else if (ParseBool(name, "enable_mot_codegen_profile", value, &m_enableCodegenProfile)) {
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

int MOTConfiguration::GetCoreByConnidFP(int cpu) const
{
    int numOfRealCores = (IsHyperThread()) ? m_coresPerCpu / 2 : m_coresPerCpu;  // 2 is for HyperThread
    // Lets pin physical first
    // cpu is already modulo of (numaNodes * num_cores)
    if (cpu < ((int)m_numaNodes * numOfRealCores)) {
        cpu = cpu % ((int)m_numaNodes * numOfRealCores);
        int coreIndex = cpu % numOfRealCores;
        int numaId = cpu / numOfRealCores;
        int counter = 0;
        auto coreSet = m_osCpuMap.find(numaId);
        for (auto it = coreSet->second.begin(); it != coreSet->second.end(); (void)++it) {
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
    for (auto it = m_osCpuMap.begin(); it != m_osCpuMap.end(); (void)++it) {
        for (auto it2 = (*it).second.begin(); it2 != (*it).second.end(); (void)++it2) {
            if (counter == logicId) {
                return *it2;
            }
            counter++;
        }
    }
    return -1;
}

int MOTConfiguration::GetCoreFromNumaNodeByIndex(int numaId, int logicId) const
{
    auto coreSet = m_osCpuMap.find(numaId);
    if (coreSet != m_osCpuMap.end()) {
        int counter = 0;
        for (auto it = coreSet->second.begin(); it != coreSet->second.end(); (void)++it) {
            if (counter == logicId) {
                return *it;
            }
            counter++;
        }
    }

    return INVALID_CPU_ID;
}

#define UPDATE_BOOL_CFG(var, cfgPath, defaultValue) \
    UpdateBoolConfigItem(var, cfg->GetConfigValue(cfgPath, defaultValue, m_suppressLog == 0), cfgPath)

#define UPDATE_USER_CFG(var, cfgPath, defaultValue) \
    UpdateUserConfigItem(var, cfg->GetUserConfigValue(cfgPath, defaultValue, m_suppressLog == 0), cfgPath)

#define UPDATE_STRING_CFG(var, cfgPath, defaultValue) \
    UpdateStringConfigItem(var, cfg->GetStringConfigValue(cfgPath, defaultValue, m_suppressLog == 0), cfgPath)

#define UPDATE_INT_CFG(var, cfgPath, defaultValue, lowerBound, upperBound) \
    UpdateIntConfigItem(                                                   \
        var, cfg->GetIntegerConfigValue(cfgPath, defaultValue, m_suppressLog == 0), cfgPath, lowerBound, upperBound)

#define UPDATE_MEM_CFG(var, cfgPath, defaultValue, scale, lowerBound, upperBound) \
    UpdateMemConfigItem(var, cfgPath, defaultValue, scale, lowerBound, upperBound, true)

#define UPDATE_ABS_MEM_CFG(var, cfgPath, defaultValue, scale, lowerBound, upperBound) \
    UpdateMemConfigItem(var, cfgPath, defaultValue, scale, lowerBound, upperBound, false)

#define UPDATE_TIME_CFG(var, cfgPath, defaultValue, scale, lowerBound, upperBound) \
    UpdateTimeConfigItem(var, cfgPath, defaultValue, scale, lowerBound, upperBound)

void MOTConfiguration::LoadMemConfig()
{
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();

    // Memory configurations
    UPDATE_BOOL_CFG(m_enableNuma, "enable_numa", DEFAULT_ENABLE_NUMA);
    if (!m_numaAvailable && m_enableNuma) {
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Disabling NUMA forcibly as some NUMA node has no memory");
        }
        UpdateBoolConfigItem(m_enableNuma, false, "enable_numa");
    }

    // Even though we allow loading these unexposed parameter (max_threads and max_connections), in reality it is
    // always overridden by the external configuration loader GaussdbConfigLoader (so in effect whatever is defined
    // in mot.conf is discarded). See GaussdbConfigLoader::ConfigureMaxThreads() and
    // GaussdbConfigLoader::ConfigureMaxConnections() for more details.
    UPDATE_INT_CFG(m_maxThreads, "max_threads", DEFAULT_MAX_THREADS, MIN_MAX_THREADS, MAX_MAX_THREADS);
    UPDATE_INT_CFG(
        m_maxConnections, "max_connections", DEFAULT_MAX_CONNECTIONS, MIN_MAX_CONNECTIONS, MAX_MAX_CONNECTIONS);
    UPDATE_USER_CFG(m_sessionAffinityMode, "affinity_mode", DEFAULT_AFFINITY_MODE);
    // we save copies because main affinity mode may be overridden when thread pool is used
    if (IS_AFFINITY_ACTIVE(m_sessionAffinityMode)) {  // update only if active
        m_taskAffinityMode = m_sessionAffinityMode;
    }
    UPDATE_BOOL_CFG(m_lazyLoadChunkDirectory, "lazy_load_chunk_directory", DEFAULT_LAZY_LOAD_CHUNK_DIRECTORY);
    UPDATE_MEM_CFG(m_globalMemoryMaxLimitMB,
        "max_mot_global_memory",
        DEFAULT_MAX_MOT_GLOBAL_MEMORY,
        SCALE_MEGA_BYTES,
        MIN_MAX_MOT_GLOBAL_MEMORY_MB,
        MAX_MAX_MOT_GLOBAL_MEMORY_MB);
    UPDATE_MEM_CFG(m_globalMemoryMinLimitMB,
        "min_mot_global_memory",
        DEFAULT_MIN_MOT_GLOBAL_MEMORY,
        SCALE_MEGA_BYTES,
        MIN_MIN_MOT_GLOBAL_MEMORY_MB,
        MAX_MIN_MOT_GLOBAL_MEMORY_MB);
    // validate that min <= max
    if (m_globalMemoryMinLimitMB > m_globalMemoryMaxLimitMB) {
        // calculate min/max values, which may be specified in percentage from total
        ++m_suppressLog;
        uint64_t defaultMinMotGlobalMemoryMB =
            ParseMemoryValueBytes(
                DEFAULT_MIN_MOT_GLOBAL_MEMORY, DEFAULT_MIN_MOT_GLOBAL_MEMORY_MB, "min_mot_global_memory") /
            SCALE_MEGA_BYTES;
        uint64_t defaultMaxMotGlobalMemoryMB =
            ParseMemoryValueBytes(
                DEFAULT_MAX_MOT_GLOBAL_MEMORY, DEFAULT_MAX_MOT_GLOBAL_MEMORY_MB, "max_mot_global_memory") /
            SCALE_MEGA_BYTES;
        --m_suppressLog;
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Invalid global memory configuration: minimum (%" PRIu64
                         " MB) is greater than maximum (%" PRIu64 " MB), using defaults (%" PRIu64 " MB, %" PRIu64
                         " MB)",
                m_globalMemoryMinLimitMB,
                m_globalMemoryMaxLimitMB,
                defaultMinMotGlobalMemoryMB,
                defaultMaxMotGlobalMemoryMB);
        }
        UpdateIntConfigItem(m_globalMemoryMaxLimitMB,
            defaultMaxMotGlobalMemoryMB,
            "max_mot_global_memory",
            MIN_MAX_MOT_GLOBAL_MEMORY_MB,
            MAX_MAX_MOT_GLOBAL_MEMORY_MB);
        UpdateIntConfigItem(m_globalMemoryMinLimitMB,
            defaultMinMotGlobalMemoryMB,
            "min_mot_global_memory",
            MIN_MIN_MOT_GLOBAL_MEMORY_MB,
            MAX_MIN_MOT_GLOBAL_MEMORY_MB);
    }
    UPDATE_MEM_CFG(m_localMemoryMaxLimitMB,
        "max_mot_local_memory",
        DEFAULT_MAX_MOT_LOCAL_MEMORY,
        SCALE_MEGA_BYTES,
        MIN_MAX_MOT_LOCAL_MEMORY_MB,
        MAX_MAX_MOT_LOCAL_MEMORY_MB);
    UPDATE_MEM_CFG(m_localMemoryMinLimitMB,
        "min_mot_local_memory",
        DEFAULT_MIN_MOT_LOCAL_MEMORY,
        SCALE_MEGA_BYTES,
        MIN_MIN_MOT_LOCAL_MEMORY_MB,
        MAX_MIN_MOT_LOCAL_MEMORY_MB);
    // validate that min <= max
    if (m_localMemoryMinLimitMB > m_localMemoryMaxLimitMB) {
        // calculate min/max values, which may be specified in percentage from total
        ++m_suppressLog;
        uint64_t defaultMinMotLocalMemoryMB =
            ParseMemoryValueBytes(
                DEFAULT_MIN_MOT_LOCAL_MEMORY, DEFAULT_MIN_MOT_LOCAL_MEMORY_MB, "min_mot_local_memory") /
            SCALE_MEGA_BYTES;
        uint64_t defaultMaxMotLocalMemoryMB =
            ParseMemoryValueBytes(
                DEFAULT_MAX_MOT_LOCAL_MEMORY, DEFAULT_MAX_MOT_LOCAL_MEMORY_MB, "max_mot_local_memory") /
            SCALE_MEGA_BYTES;
        --m_suppressLog;
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Invalid local memory configuration: minimum (%" PRIu64
                         " MB) is greater than maximum (%" PRIu64 " MB), using defaults (%" PRIu64 " MB, %" PRIu64
                         " MB)",
                m_localMemoryMinLimitMB,
                m_localMemoryMaxLimitMB,
                defaultMinMotLocalMemoryMB,
                defaultMaxMotLocalMemoryMB);
        }
        UpdateIntConfigItem(m_localMemoryMaxLimitMB,
            defaultMaxMotLocalMemoryMB,
            "max_mot_local_memory",
            MIN_MAX_MOT_LOCAL_MEMORY_MB,
            MAX_MAX_MOT_LOCAL_MEMORY_MB);
        UpdateIntConfigItem(m_localMemoryMinLimitMB,
            defaultMinMotLocalMemoryMB,
            "min_mot_local_memory",
            MIN_MIN_MOT_LOCAL_MEMORY_MB,
            MAX_MIN_MOT_LOCAL_MEMORY_MB);
    }
    UPDATE_ABS_MEM_CFG(m_sessionMemoryMaxLimitKB,
        "max_mot_session_memory",
        DEFAULT_MAX_MOT_SESSION_MEMORY,
        SCALE_KILO_BYTES,
        MIN_MAX_MOT_SESSION_MEMORY_KB,
        MAX_MAX_MOT_SESSION_MEMORY_KB);
    UPDATE_ABS_MEM_CFG(m_sessionMemoryMinLimitKB,
        "min_mot_session_memory",
        DEFAULT_MIN_MOT_SESSION_MEMORY,
        SCALE_KILO_BYTES,
        MIN_MIN_MOT_SESSION_MEMORY_KB,
        MAX_MIN_MOT_SESSION_MEMORY_KB);
    // validate that min <= max
    if ((m_sessionMemoryMaxLimitKB > 0) && (m_sessionMemoryMinLimitKB > m_sessionMemoryMaxLimitKB)) {
        // calculate min/max values, which may be specified in percentage from total
        ++m_suppressLog;
        uint64_t defaultMinMotSessionMemoryKB =
            ParseMemoryValueBytes(
                DEFAULT_MIN_MOT_SESSION_MEMORY, DEFAULT_MIN_MOT_SESSION_MEMORY_KB, "min_mot_session_memory") /
            SCALE_KILO_BYTES;
        uint64_t defaultMaxMotSessionMemoryKB =
            ParseMemoryValueBytes(
                DEFAULT_MAX_MOT_SESSION_MEMORY, DEFAULT_MAX_MOT_SESSION_MEMORY_KB, "max_mot_session_memory") /
            SCALE_KILO_BYTES;
        --m_suppressLog;
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Invalid session memory configuration: minimum (%" PRIu64
                         " KB) is greater than maximum (%" PRIu64 " KB), using defaults (%" PRIu64 " KB, %" PRIu64
                         " KB)",
                m_sessionMemoryMinLimitKB,
                m_sessionMemoryMaxLimitKB,
                defaultMinMotSessionMemoryKB,
                defaultMaxMotSessionMemoryKB);
        }
        UpdateIntConfigItem(m_sessionMemoryMaxLimitKB,
            defaultMaxMotSessionMemoryKB,
            "max_mot_session_memory",
            MIN_MAX_MOT_SESSION_MEMORY_KB,
            MAX_MAX_MOT_SESSION_MEMORY_KB);
        UpdateIntConfigItem(m_sessionMemoryMinLimitKB,
            defaultMinMotSessionMemoryKB,
            "min_mot_session_memory",
            MIN_MIN_MOT_SESSION_MEMORY_KB,
            MAX_MIN_MOT_SESSION_MEMORY_KB);
    }
    UPDATE_USER_CFG(m_reserveMemoryMode, "reserve_memory_mode", DEFAULT_RESERVE_MEMORY_MODE);
    UPDATE_USER_CFG(m_storeMemoryPolicy, "store_memory_policy", DEFAULT_STORE_MEMORY_POLICY);
    UPDATE_USER_CFG(m_chunkAllocPolicy, "chunk_alloc_policy", DEFAULT_CHUNK_ALLOC_POLICY);
    UPDATE_INT_CFG(m_chunkPreallocWorkerCount,
        "chunk_prealloc_worker_count",
        DEFAULT_CHUNK_PREALLOC_WORKER_COUNT,
        MIN_CHUNK_PREALLOC_WORKER_COUNT,
        MAX_CHUNK_PREALLOC_WORKER_COUNT);
    if (m_loadExtraParams) {
        UPDATE_INT_CFG(m_highRedMarkPercent,
            "high_red_mark_percent",
            DEFAULT_HIGH_RED_MARK_PERCENT,
            MIN_HIGH_RED_MARK_PERCENT,
            MAX_HIGH_RED_MARK_PERCENT);
    }
    UPDATE_ABS_MEM_CFG(m_sessionLargeBufferStoreSizeMB,
        "session_large_buffer_store_size",
        DEFAULT_SESSION_LARGE_BUFFER_STORE_SIZE,
        SCALE_MEGA_BYTES,
        MIN_SESSION_LARGE_BUFFER_STORE_SIZE_MB,
        MAX_SESSION_LARGE_BUFFER_STORE_SIZE_MB);
    UPDATE_ABS_MEM_CFG(m_sessionLargeBufferStoreMaxObjectSizeMB,
        "session_large_buffer_store_max_object_size",
        DEFAULT_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE,
        SCALE_MEGA_BYTES,
        MIN_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB,
        MAX_SESSION_LARGE_BUFFER_STORE_MAX_OBJECT_SIZE_MB);
    UPDATE_ABS_MEM_CFG(m_sessionMaxHugeObjectSizeMB,
        "session_max_huge_object_size",
        DEFAULT_SESSION_MAX_HUGE_OBJECT_SIZE,
        SCALE_MEGA_BYTES,
        MIN_SESSION_MAX_HUGE_OBJECT_SIZE_MB,
        MAX_SESSION_MAX_HUGE_OBJECT_SIZE_MB);
}

void MOTConfiguration::LoadConfig()
{
    MOT_LOG_TRACE("Loading main configuration");
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();

    // load component log levels so we can enable traces in this logger
    UpdateComponentLogLevel();

    // logger configuration
    if (m_loadExtraParams) {
        UPDATE_BOOL_CFG(m_enableRedoLog, "enable_redo_log", DEFAULT_ENABLE_REDO_LOG);
        UPDATE_USER_CFG(m_loggerType, "logger_type", DEFAULT_LOGGER_TYPE);
    }

    // Even though we allow loading this unexposed parameter (redo_log_handler_type), in reality it is always
    // overridden by the external configuration loader GaussdbConfigLoader (so in effect whatever is defined in
    // mot.conf is discarded). See GaussdbConfigLoader::ConfigureRedoLogHandler() for more details.
    UPDATE_USER_CFG(m_redoLogHandlerType, "redo_log_handler_type", DEFAULT_REDO_LOG_HANDLER_TYPE);

    // commit configuration
    UPDATE_BOOL_CFG(m_enableGroupCommit, "enable_group_commit", DEFAULT_ENABLE_GROUP_COMMIT);
    UPDATE_INT_CFG(m_groupCommitSize,
        "group_commit_size",
        DEFAULT_GROUP_COMMIT_SIZE,
        MIN_GROUP_COMMIT_SIZE,
        MAX_GROUP_COMMIT_SIZE);
    UPDATE_TIME_CFG(m_groupCommitTimeoutUSec,
        "group_commit_timeout",
        DEFAULT_GROUP_COMMIT_TIMEOUT,
        SCALE_MICROS,
        MIN_GROUP_COMMIT_TIMEOUT_USEC,
        MAX_GROUP_COMMIT_TIMEOUT_USEC);

    // Checkpoint configuration
    if (m_loadExtraParams) {
        UPDATE_BOOL_CFG(m_enableCheckpoint, "enable_checkpoint", DEFAULT_ENABLE_CHECKPOINT);
    }

    if (!m_enableCheckpoint && !m_enableRedoLog) {
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Disabling redo_log forcibly as the checkpoint is disabled");
        }
        UpdateBoolConfigItem(m_enableRedoLog, false, "enable_redo_log");
    }

    UPDATE_STRING_CFG(m_checkpointDir, "checkpoint_dir", DEFAULT_CHECKPOINT_DIR);
    UPDATE_ABS_MEM_CFG(m_checkpointSegThreshold,
        "checkpoint_segsize",
        DEFAULT_CHECKPOINT_SEGSIZE,
        SCALE_BYTES,
        MIN_CHECKPOINT_SEGSIZE_BYTES,
        MAX_CHECKPOINT_SEGSIZE_BYTES);
    UPDATE_INT_CFG(m_checkpointWorkers,
        "checkpoint_workers",
        DEFAULT_CHECKPOINT_WORKERS,
        MIN_CHECKPOINT_WORKERS,
        MAX_CHECKPOINT_WORKERS);

    // Recovery configuration
    UPDATE_INT_CFG(m_checkpointRecoveryWorkers,
        "checkpoint_recovery_workers",
        DEFAULT_CHECKPOINT_RECOVERY_WORKERS,
        MIN_CHECKPOINT_RECOVERY_WORKERS,
        MAX_CHECKPOINT_RECOVERY_WORKERS);

    if (m_loadExtraParams) {
        UPDATE_USER_CFG(m_recoveryMode, "recovery_mode", DEFAULT_RECOVERY_MODE);
    }

    UPDATE_INT_CFG(m_parallelRecoveryWorkers,
        "parallel_recovery_workers",
        DEFAULT_PARALLEL_RECOVERY_WORKERS,
        MIN_PARALLEL_RECOVERY_WORKERS,
        MAX_PARALLEL_RECOVERY_WORKERS);

    UPDATE_INT_CFG(m_parallelRecoveryQueueSize,
        "parallel_recovery_queue_size",
        DEFAULT_PARALLEL_RECOVERY_QUEUE_SIZE,
        MIN_PARALLEL_RECOVERY_QUEUE_SIZE,
        MAX_PARALLEL_RECOVERY_QUEUE_SIZE);

    if (m_parallelRecoveryQueueSize < m_parallelRecoveryWorkers) {
        if (m_suppressLog == 0) {
            MOT_LOG_WARN("Invalid recovery configuration: parallel_recovery_queue_size (%" PRIu32 ") is lesser than "
                         "parallel_recovery_workers (%" PRIu32 "), changing parallel_recovery_queue_size to (%" PRIu32
                         ")",
                m_parallelRecoveryQueueSize,
                m_parallelRecoveryWorkers,
                m_parallelRecoveryWorkers);
        }
        m_parallelRecoveryQueueSize = m_parallelRecoveryWorkers;  // At least one transaction per processor
    }

    // Tx configuration - not configurable yet
    if (m_loadExtraParams) {
        UPDATE_BOOL_CFG(m_abortBufferEnable, "tx_abort_buffers_enable", true);
        UPDATE_BOOL_CFG(m_preAbort, "tx_pre_abort", true);
        m_validationLock = TxnValidation::TXN_VALIDATION_NO_WAIT;
    }

    // statistics configuration
    UPDATE_BOOL_CFG(m_enableStats, "enable_stats", DEFAULT_ENABLE_STATS);
    UPDATE_TIME_CFG(m_statPrintPeriodSeconds,
        "print_stats_period",
        DEFAULT_STATS_PRINT_PERIOD,
        SCALE_SECONDS,
        MIN_STATS_PRINT_PERIOD_SECONDS,
        MAX_STATS_PRINT_PERIOD_SECONDS);
    UPDATE_TIME_CFG(m_statPrintFullPeriodSeconds,
        "print_full_stats_period",
        DEFAULT_FULL_STATS_PRINT_PERIOD,
        SCALE_SECONDS,
        MIN_STATS_PRINT_PERIOD_SECONDS,
        MAX_STATS_PRINT_PERIOD_SECONDS);
    UPDATE_BOOL_CFG(m_enableLogRecoveryStats, "enable_log_recovery_stats", DEFAULT_ENABLE_LOG_RECOVERY_STATS);
    UPDATE_BOOL_CFG(m_enableDbSessionStatistics, "enable_db_session_stats", DEFAULT_ENABLE_DB_SESSION_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableNetworkStatistics, "enable_network_stats", DEFAULT_ENABLE_NETWORK_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableLogStatistics, "enable_log_stats", DEFAULT_ENABLE_LOG_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableMemoryStatistics, "enable_memory_stats", DEFAULT_ENABLE_MEMORY_STAT_PRINT);
    UPDATE_BOOL_CFG(
        m_enableDetailedMemoryStatistics, "enable_detailed_memory_stats", DEFAULT_ENABLE_DETAILED_MEMORY_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableProcessStatistics, "enable_process_stats", DEFAULT_ENABLE_PROCESS_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableSystemStatistics, "enable_system_stats", DEFAULT_ENABLE_SYSTEM_STAT_PRINT);
    UPDATE_BOOL_CFG(m_enableJitStatistics, "enable_jit_stats", DEFAULT_ENABLE_JIT_STAT_PRINT);

    // log configuration
    UPDATE_USER_CFG(m_logLevel, "log_level", DEFAULT_LOG_LEVEL);
    (void)SetGlobalLogLevel(m_logLevel);
    if (m_loadExtraParams) {
        UPDATE_USER_CFG(m_numaErrorsLogLevel, "numa_errors_log_level", DEFAULT_NUMA_ERRORS_LOG_LEVEL);
        UPDATE_USER_CFG(m_numaWarningsLogLevel, "numa_warnings_log_level", DEFAULT_NUMA_WARNINGS_LOG_LEVEL);
        UPDATE_USER_CFG(m_cfgStartupLogLevel, "cfg_startup_log_level", DEFAULT_CFG_STARTUP_LOG_LEVEL);
    }

    // memory configuration
    LoadMemConfig();

    // GC configuration
    UPDATE_ABS_MEM_CFG(m_gcReclaimThresholdBytes,
        "reclaim_threshold",
        DEFAULT_GC_RECLAIM_THRESHOLD,
        SCALE_BYTES,
        MIN_GC_RECLAIM_THRESHOLD_BYTES,
        MAX_GC_RECLAIM_THRESHOLD_BYTES);
    UPDATE_INT_CFG(m_gcReclaimBatchSize,
        "reclaim_batch_size",
        DEFAULT_GC_RECLAIM_BATCH_SIZE,
        MIN_GC_RECLAIM_BATCH_SIZE,
        MAX_GC_RECLAIM_BATCH_SIZE);
    UPDATE_ABS_MEM_CFG(m_gcHighReclaimThresholdBytes,
        "high_reclaim_threshold",
        DEFAULT_GC_HIGH_RECLAIM_THRESHOLD,
        SCALE_BYTES,
        MIN_GC_HIGH_RECLAIM_THRESHOLD_BYTES,
        MAX_GC_HIGH_RECLAIM_THRESHOLD_BYTES);

    // JIT configuration
    UPDATE_BOOL_CFG(m_enableCodegen, "enable_mot_codegen", DEFAULT_ENABLE_MOT_CODEGEN);
    UPDATE_BOOL_CFG(m_enableQueryCodegen, "enable_mot_query_codegen", DEFAULT_ENABLE_MOT_QUERY_CODEGEN);
    UPDATE_BOOL_CFG(m_enableSPCodegen, "enable_mot_sp_codegen", DEFAULT_ENABLE_MOT_SP_CODEGEN);

    if (m_loadExtraParams) {
        UPDATE_STRING_CFG(m_spCodegenAllowed, "mot_sp_codegen_allowed", DEFAULT_MOT_SP_CODEGEN_ALLOWED);
        UPDATE_STRING_CFG(m_spCodegenProhibited, "mot_sp_codegen_prohibited", DEFAULT_MOT_SP_CODEGEN_PROHIBITED);
        UPDATE_STRING_CFG(m_pureSPCodegen, "mot_pure_sp_codegen", DEFAULT_MOT_PURE_SP_CODEGEN);
    }

    UPDATE_BOOL_CFG(m_enableCodegenPrint, "enable_mot_codegen_print", DEFAULT_ENABLE_MOT_CODEGEN_PRINT);
    UPDATE_INT_CFG(
        m_codegenLimit, "mot_codegen_limit", DEFAULT_MOT_CODEGEN_LIMIT, MIN_MOT_CODEGEN_LIMIT, MAX_MOT_CODEGEN_LIMIT);
    UPDATE_BOOL_CFG(m_enableCodegenProfile, "enable_mot_codegen_profile", DEFAULT_ENABLE_MOT_CODEGEN_PROFILE);

    // storage configuration
    if (m_loadExtraParams) {
        UPDATE_BOOL_CFG(
            m_allowIndexOnNullableColumn, "allow_index_on_nullable_column", DEFAULT_ALLOW_INDEX_ON_NULLABLE_COLUMN);
        UPDATE_USER_CFG(m_indexTreeFlavor, "index_tree_flavor", DEFAULT_INDEX_TREE_FLAVOR);
    }

    // general configuration
    if (m_loadExtraParams) {
        UPDATE_TIME_CFG(m_configMonitorPeriodSeconds,
            "config_update_period",
            DEFAULT_CFG_MONITOR_PERIOD,
            SCALE_SECONDS,
            MIN_CFG_MONITOR_PERIOD_SECONDS,
            MAX_CFG_MONITOR_PERIOD_SECONDS);
        UPDATE_BOOL_CFG(m_runInternalConsistencyValidation,
            "internal_consistency_validation",
            DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION);
        UPDATE_BOOL_CFG(m_runInternalMvccConsistencyValidation,
            "internal_mvcc_consistency_validation",
            DEFAULT_RUN_INTERNAL_CONSISTENCY_VALIDATION);
    }

    MOT_LOG_TRACE("Main configuration loaded");
}

uint64_t MOTConfiguration::GetDefaultMemValueBytes(uint64_t& oldValue, const char* name, const char* defaultStrValue,
    uint64_t scale, uint64_t lowerBound, uint64_t upperBound, bool allowPercentage)
{
    ++m_suppressLog;
    uint64_t defaultValueBytes = allowPercentage ? ParseMemoryValueBytes(defaultStrValue, (uint64_t)-1, name)
                                                 : ParseMemoryUnit(defaultStrValue, (uint64_t)-1, name);
    UpdateIntConfigItem(oldValue, defaultValueBytes / scale, name, lowerBound, upperBound);
    --m_suppressLog;
    MOT_LOG_TRACE(
        "Converted memory default string value %s to byte value: %" PRIu64, defaultStrValue, defaultValueBytes);
    return defaultValueBytes;
}

void MOTConfiguration::UpdateMemConfigItem(uint64_t& oldValue, const char* name, const char* defaultStrValue,
    uint64_t scale, uint64_t lowerBound, uint64_t upperBound, bool allowPercentage)
{
    // we prepare first a default value from the string default value
    uint64_t defaultValueBytes =
        GetDefaultMemValueBytes(oldValue, name, defaultStrValue, scale, lowerBound, upperBound, allowPercentage);

    // now we carefully examine the configuration item type
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();
    const ConfigItem* cfgItem = cfg->GetConfigItem(name);
    if (cfgItem == nullptr) {
        // just keep default
        MOT_LOG_TRACE("Configuration item %s not found, keeping default %" PRIu64 ".", name, defaultValueBytes);
    } else if (cfgItem->GetClass() != ConfigItemClass::CONFIG_ITEM_VALUE) {
        if (m_suppressLog == 0) {
            MOT_LOG_ERROR(
                "Invalid configuration for %s: expected value item, got %s item. Keeping default value %" PRIu64 ".",
                name,
                ConfigItemClassToString(cfgItem->GetClass()),
                defaultValueBytes);
        }
        UpdateIntConfigItem(oldValue, defaultValueBytes / scale, name, lowerBound, upperBound);
    } else {
        // now we carefully examine the value type
        const ConfigValue* cfgValue = (const ConfigValue*)cfgItem;
        if (cfgValue->IsIntegral()) {
            // only a number was specified, so it is interpreted as bytes
            uint64_t memoryValueBytes =
                cfg->GetIntegerConfigValue<uint64_t>(name, defaultValueBytes, m_suppressLog == 0);
            UpdateIntConfigItem(oldValue, memoryValueBytes / scale, name, lowerBound, upperBound);
            MOT_LOG_TRACE("Loading integral memory value %s as bytes: %" PRIu64, name, memoryValueBytes);
        } else if (cfgValue->GetConfigValueType() == ConfigValueType::CONFIG_VALUE_STRING) {
            // value was parsed as string, meaning we have units or percentage specifier
            const char* strValue = cfg->GetStringConfigValue(name, defaultStrValue, m_suppressLog == 0);
            if ((strValue != nullptr) && (strValue[0] != 0)) {
                uint64_t memoryValueBytes = allowPercentage ? ParseMemoryValueBytes(strValue, defaultValueBytes, name)
                                                            : ParseMemoryUnit(strValue, defaultValueBytes, name);
                UpdateIntConfigItem(oldValue, memoryValueBytes / scale, name, lowerBound, upperBound);
            } else {
                if (m_suppressLog == 0) {
                    MOT_LOG_WARN("Empty value specified for configuration item %s, using default value: %" PRIu64
                                 " bytes",
                        name,
                        defaultValueBytes);
                }
            }
        } else {
            // unexpected value type
            if (m_suppressLog == 0) {
                MOT_LOG_WARN("Unexpected configuration item %s value type: %s. Keeping default value: %" PRIu64 ".",
                    name,
                    ConfigValueTypeToString(cfgValue->GetConfigValueType()),
                    defaultValueBytes);
            }
            UpdateIntConfigItem(oldValue, defaultValueBytes / scale, name, lowerBound, upperBound);
        }
    }
}

uint64_t MOTConfiguration::GetDefaultTimeValueUSecs(uint64_t& oldValue, const char* name, const char* defaultStrValue,
    uint64_t scale, uint64_t lowerBound, uint64_t upperBound)
{
    // we prepare first a default value from the string default value
    ++m_suppressLog;
    uint64_t defaultValueUSecs = ParseTimeValueMicros(defaultStrValue, (uint64_t)-1, name);
    UpdateIntConfigItem(oldValue, defaultValueUSecs / scale, name, lowerBound, upperBound);
    --m_suppressLog;
    return defaultValueUSecs;
}

void MOTConfiguration::UpdateTimeConfigItem(uint64_t& oldValue, const char* name, const char* defaultStrValue,
    uint64_t scale, uint64_t lowerBound, uint64_t upperBound)
{
    // we prepare first a default value from the string default value
    uint64_t defaultValueUSecs =
        GetDefaultTimeValueUSecs(oldValue, name, defaultStrValue, scale, lowerBound, upperBound);

    MOT_LOG_TRACE("Converted time default string value %s to usec value: %" PRIu64, defaultStrValue, defaultValueUSecs);

    // now we carefully examine the configuration item type
    const LayeredConfigTree* cfg = ConfigManager::GetInstance().GetLayeredConfigTree();
    const ConfigItem* cfgItem = cfg->GetConfigItem(name);
    if (cfgItem == nullptr) {
        // just keep default
        MOT_LOG_TRACE("Configuration item %s not found, keeping default %" PRIu64 ".", name, defaultValueUSecs);
    } else if (cfgItem->GetClass() != ConfigItemClass::CONFIG_ITEM_VALUE) {
        if (m_suppressLog == 0) {
            MOT_LOG_ERROR(
                "Invalid configuration for %s: expected value item, got %s item. Keeping default value %" PRIu64 ".",
                name,
                ConfigItemClassToString(cfgItem->GetClass()),
                defaultValueUSecs);
        }
        UpdateIntConfigItem(oldValue, defaultValueUSecs / scale, name, lowerBound, upperBound);
    } else {
        // now we carefully examine the value type
        const ConfigValue* cfgValue = (const ConfigValue*)cfgItem;
        if (cfgValue->IsIntegral()) {
            // only a number was specified, so it is interpreted as micro-seconds
            uint64_t timeValueUSecs = cfg->GetIntegerConfigValue<uint64_t>(name, defaultValueUSecs, m_suppressLog == 0);
            UpdateIntConfigItem(oldValue, timeValueUSecs / scale, name, lowerBound, upperBound);
            MOT_LOG_TRACE("Loading integral time value %s as micro-seconds: %" PRIu64, name, timeValueUSecs);
        } else if (cfgValue->GetConfigValueType() == ConfigValueType::CONFIG_VALUE_STRING) {
            // value was parsed as string, meaning we have units
            const char* strValue = cfg->GetStringConfigValue(name, defaultStrValue, m_suppressLog == 0);
            if ((strValue != nullptr) && (strValue[0] != 0)) {
                uint64_t timeValueUSecs = ParseTimeValueMicros(strValue, defaultValueUSecs, name);
                UpdateIntConfigItem(oldValue, timeValueUSecs / scale, name, lowerBound, upperBound);
            }
        } else {
            // unexpected value type
            if (m_suppressLog == 0) {
                MOT_LOG_WARN("Unexpected configuration item %s value type: %s. Keeping default value: %" PRIu64 ".",
                    name,
                    ConfigValueTypeToString(cfgValue->GetConfigValueType()),
                    defaultValueUSecs);
            }
            UpdateIntConfigItem(oldValue, defaultValueUSecs / scale, name, lowerBound, upperBound);
        }
    }
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
            MOT_ASSERT(componentCfg != nullptr);

            // configure component log level first then override log level specific loggers in the component
            LogLevel componentLevel = componentCfg->GetUserConfigValue<LogLevel>("log_level", globalLogLevel);
            if (componentLevel != globalLogLevel) {
                if (m_suppressLog == 0) {
                    mot_string logLevelStr;
                    MOT_LOG_INFO("Updating the log level of component %s to: %s",
                        componentName.c_str(),
                        TypeFormatter<LogLevel>::ToString(componentLevel, logLevelStr));
                }
                (void)SetLogComponentLogLevel(componentName.c_str(), componentLevel);
            }

            // all configuration values are logger configuration pairs (loggerName=log_level)
            mot_string_list loggerNames;
            if (!componentCfg->GetConfigSectionNames(loggerNames)) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to retrieve logger names for section %s",
                    componentCfg->GetFullPathName());
                return;
            }
            mot_string_list::const_iterator loggerItr = loggerNames.cbegin();
            while (loggerItr != loggerNames.cend()) {
                const mot_string& loggerName = *loggerItr;
                MOT_LOG_DEBUG("Loading component/logger %s/%s log level", componentName.c_str(), loggerName.c_str());
                const ConfigSection* loggerCfg = componentCfg->GetConfigSection(loggerName.c_str());
                LogLevel loggerLevel = loggerCfg->GetUserConfigValue<LogLevel>("log_level", componentLevel);
                if (loggerLevel != componentLevel) {
                    if (m_suppressLog == 0) {
                        mot_string logLevelStr;
                        MOT_LOG_INFO("Updating the log level of logger %s in component %s to: %s",
                            loggerName.c_str(),
                            componentName.c_str(),
                            TypeFormatter<LogLevel>::ToString(loggerLevel, logLevelStr));
                    }
                    (void)SetLoggerLogLevel(componentName.c_str(), loggerName.c_str(), loggerLevel);
                }
                ++loggerItr;
            }

            ++compItr;
        }
    }
}

void MOTConfiguration::UpdateBoolConfigItem(bool& oldValue, bool newValue, const char* name)
{
    if (oldValue != newValue) {
        MOT_LOG_TRACE(
            "Configuration of %s changed: %s --> %s", name, oldValue ? "TRUE" : "FALSE", newValue ? "TRUE" : "FALSE");
        oldValue = newValue;
    }
}

void MOTConfiguration::UpdateStringConfigItem(std::string& oldValue, const char* newValue, const char* name)
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
        if (!extra.is_valid()) {
            MOT_LOG_ERROR("Failed to allocate memory string: %s", endptr + 1);
        } else {
            extra.trim();
            if (extra.length() != 0) {
                MOT_LOG_WARN("Invalid memory value format: %s (trailing characters after %%)", memoryValue);
            } else {
                MOT_LOG_TRACE("Parsed percentage: %d%%", percent);
                result = percent;
            }
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
        memoryValueBytes = m_totalMemoryMb * MEGA_BYTE * percent / 100;
        if (m_suppressLog == 0) {
            MOT_LOG_INFO(
                "Loaded %s: %d%% from total = %" PRIu64 " MB", cfgPath, percent, (memoryValueBytes / 1024ul) / 1024ul);
        }
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
        MOT_LOG_TRACE("Missing %s memory value units: %s (kilobytes assumed)", cfgPath, memoryValue);
        memoryValueBytes = ((uint64_t)value) * 1024;
    } else {
        // get unit type and convert to mega-bytes
        mot_string suffix(endptr);
        if (!suffix.is_valid()) {
            MOT_LOG_ERROR("Failed to allocate memory string: %s", endptr);
        } else {
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
            } else {
                MOT_LOG_WARN("Invalid %s memory value format: %s (invalid unit specifier '%s' - should be one of TB, "
                             "GB, MB or KB)",
                    cfgPath,
                    memoryValue,
                    suffix.c_str());
            }
        }
    }
    return memoryValueBytes;
}

static inline bool IsTimeUnitDays(const mot_string& suffix)
{
    if ((suffix.compare_no_case("d") == 0) || (suffix.compare_no_case("days") == 0) ||
        (suffix.compare_no_case("day") == 0)) {
        return true;
    }
    return false;
}

static inline bool IsTimeUnitHours(const mot_string& suffix)
{
    if ((suffix.compare_no_case("h") == 0) || (suffix.compare_no_case("hours") == 0) ||
        (suffix.compare_no_case("hour") == 0)) {
        return true;
    }
    return false;
}

static inline bool IsTimeUnitMinutes(const mot_string& suffix)
{
    if ((suffix.compare_no_case("m") == 0) || (suffix.compare_no_case("mins") == 0) ||
        (suffix.compare_no_case("minutes") == 0) || (suffix.compare_no_case("min") == 0) ||
        (suffix.compare_no_case("minute") == 0)) {
        return true;
    }
    return false;
}

static inline bool IsTimeUnitSeconds(const mot_string& suffix)
{
    if ((suffix.compare_no_case("s") == 0) || (suffix.compare_no_case("secs") == 0) ||
        (suffix.compare_no_case("seconds") == 0) || (suffix.compare_no_case("sec") == 0) ||
        (suffix.compare_no_case("second") == 0)) {
        return true;
    }
    return false;
}

static inline bool IsTimeUnitMilliSeconds(const mot_string& suffix)
{
    if ((suffix.compare_no_case("ms") == 0) || (suffix.compare_no_case("millis") == 0) ||
        (suffix.compare_no_case("milliseconds") == 0) || (suffix.compare_no_case("milli") == 0) ||
        (suffix.compare_no_case("millisecond") == 0)) {
        return true;
    }
    return false;
}

static inline bool IsTimeUnitMicroSeconds(const mot_string& suffix)
{
    if ((suffix.compare_no_case("us") == 0) || (suffix.compare_no_case("micros") == 0) ||
        (suffix.compare_no_case("microseconds") == 0) || (suffix.compare_no_case("micro") == 0) ||
        (suffix.compare_no_case("microsecond") == 0)) {
        return true;
    }
    return false;
}

uint64_t MOTConfiguration::ParseTimeValueMicros(const char* timeValue, uint64_t defaultValue, const char* cfgPath)
{
    uint64_t timeValueMicros = defaultValue;
    char* endptr = NULL;
    unsigned long long value = strtoull(timeValue, &endptr, 0);
    if (endptr == timeValue) {
        MOT_LOG_WARN("Invalid %s time value format: %s (expecting value digits)", cfgPath, timeValue);
    } else if (*endptr == 0) {
        MOT_LOG_TRACE("Missing %s time value units: %s (milliseconds assumed)", cfgPath, timeValue);
        timeValueMicros = ((uint64_t)value) * 1000ull;
    } else {
        // get unit type and convert to micro-seconds
        mot_string suffix(endptr);
        if (!suffix.is_valid()) {
            MOT_LOG_ERROR("Failed to allocate time unit string: %s", endptr);
        } else {
            suffix.trim();
            if (IsTimeUnitDays(suffix)) {
                MOT_LOG_TRACE("Loaded %s: %u days", cfgPath, value);
                timeValueMicros = ((uint64_t)value) * 24ull * 60ull * 60ull * 1000ull * 1000ull;
            } else if (IsTimeUnitHours(suffix)) {
                MOT_LOG_TRACE("Loaded %s: %u hours", cfgPath, value);
                timeValueMicros = ((uint64_t)value) * 60ull * 60ull * 1000ull * 1000ull;
            } else if (IsTimeUnitMinutes(suffix)) {
                MOT_LOG_TRACE("Loaded %s: %u minutes", cfgPath, value);
                timeValueMicros = ((uint64_t)value) * 60ull * 1000ull * 1000ull;
            } else if (IsTimeUnitSeconds(suffix)) {
                MOT_LOG_TRACE("Loaded %s: %u seconds", cfgPath, value);
                timeValueMicros = ((uint64_t)value) * 1000ull * 1000ull;
            } else if (IsTimeUnitMilliSeconds(suffix)) {
                MOT_LOG_TRACE("Loaded %s: %u milli-seconds", cfgPath, value);
                timeValueMicros = ((uint64_t)value) * 1000ull;
            } else {
                MOT_LOG_WARN("Invalid %s time value format: %s (invalid unit specifier '%s' - should be one of d, h, "
                             "m, s or ms)",
                    cfgPath,
                    timeValue,
                    suffix.c_str());
            }
        }
    }
    return timeValueMicros;
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
