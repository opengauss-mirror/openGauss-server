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
 * gaussdb_config_loader.h
 *    GaussDB configuration loader implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/gaussdb_config_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GAUSSDB_CONFIG_LOADER_H
#define GAUSSDB_CONFIG_LOADER_H

#include "ext_config_loader.h"
#include "mot_internal.h"

// GaussDB configuration loader
class GaussdbConfigLoader : public MOT::ExtConfigLoader {
public:
    GaussdbConfigLoader() : MOT::ExtConfigLoader("GaussDB")
    {}

    virtual ~GaussdbConfigLoader()
    {}

protected:
    virtual bool OnLoadExtConfig()
    {
        // we should be careful here, configuration is not fully loaded yet and MOTConfiguration still contains default
        // values, so we trigger partial configuration reload so we can take what has been loaded up to this point from
        // the layered configuration tree in the configuration manager
        MOT_LOG_TRACE("Triggering initial partial configuration loading for external configuration overlaying");
        MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
        motCfg.LoadPartial();  // trigger partial load

        // load any relevant GUC values here
        bool result = ConfigureMaxConnections();
        if (result) {
            result = ConfigureMaxThreads();
        }
        if (result) {
            result = ConfigureAffinity();
        }
        if (result) {
            result = ConfigureRedoLogHandler();
        }
        if (result) {
            result = ConfigureMaxProcessMemory();
        }
        return result;
    }

private:
    bool ConfigureMaxConnections()
    {
        MOT_LOG_INFO("Loading max_connections from envelope into MOTEngine: %u",
            (unsigned)g_instance.attr.attr_network.MaxConnections);
        return AddExtUInt32ConfigItem("", "max_connections", (uint32_t)g_instance.attr.attr_network.MaxConnections);
    }

    void GetRequiredThreadCount(
        uint32_t& startupThreadCount, uint32_t& runtimeThreadCount, uint32_t& sessionThreadCount) const
    {
        // NOTE: parallel redo recovery also requires thread ids when invoking MOT engine (in case it is configured)
        MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
        startupThreadCount = motCfg.m_checkpointRecoveryWorkers + motCfg.m_chunkPreallocWorkerCount;
        runtimeThreadCount = motCfg.m_checkpointWorkers + 1;  // add one for statistics reporting thread

        // get the number of threads used to manage user sessions
        sessionThreadCount = 0;
        if (g_instance.attr.attr_common.enable_thread_pool) {  // thread pool enabled
            // use thread pool size in envelope configuration to determine the number of thread ids required
            sessionThreadCount = g_threadPoolControler->GetThreadNum();
        } else {  // thread-pool disabled
            // use max_connections in envelope configuration to determine the number of thread ids required
            sessionThreadCount = (uint32_t)g_instance.attr.attr_network.MaxConnections;
        }
        MOT_LOG_TRACE("Using %u threads for user sessions", sessionThreadCount);
    }

    bool ConfigureMaxThreads()
    {
        uint32_t startupThreadCount = 0;
        uint32_t runtimeThreadCount = 0;
        uint32_t sessionThreadCount = 0;

        GetRequiredThreadCount(startupThreadCount, runtimeThreadCount, sessionThreadCount);

        // compute total required number of threads, starting with run-time requirements
        uint32_t totalThreadCount = runtimeThreadCount + sessionThreadCount;
        MOT_LOG_TRACE("Total number of required threads: %u", totalThreadCount);

        // first check: there is enough threads for startup tasks
        bool warnIssued = false;
        if (totalThreadCount < startupThreadCount) {
            totalThreadCount = startupThreadCount;
            MOT_LOG_WARN("Adjusted maximum number of threads to %u to accommodate for startup tasks", totalThreadCount);
            warnIssued = true;
        }

        // second check: verify we did not breach total maximum
        if (totalThreadCount > MAX_THREAD_COUNT) {
            totalThreadCount = (unsigned)MAX_THREAD_COUNT;
            MOT_LOG_WARN("Adjusted maximum number of threads to %u due to maximum limit", totalThreadCount);
            warnIssued = true;
        }

        // print final value
        if (!warnIssued) {
            MOT_LOG_INFO("Adjusted maximum number of threads to %u", totalThreadCount);
        }

        return AddExtUInt32ConfigItem("", "max_threads", totalThreadCount);
    }

    bool ConfigureAffinity()
    {
        bool result = true;
        if (g_instance.attr.attr_common.enable_thread_pool) {
            MOT_LOG_INFO("Disabling affinity settings due to usage of thread pool in envelope");
            result = AddExtTypedConfigItem("", "affinity_mode", MOT::AffinityMode::AFFINITY_NONE);
        }
        return result;
    }

    bool ConfigureRedoLogHandler()
    {
        bool result = true;
        if (u_sess->attr.attr_storage.guc_synchronous_commit == SYNCHRONOUS_COMMIT_OFF) {
            /*
             * In MOT, for asynchronous redo log mode also we use SYNC_REDO_LOG_HANDLER.
             * Asynchronous behavior is handled by the envelope.
             */
            MOT_LOG_INFO("Configuring asynchronous redo-log handler due to synchronous_commit=off");
            result = AddExtTypedConfigItem<MOT::RedoLogHandlerType>(
                "", "redo_log_handler_type", MOT::RedoLogHandlerType::SYNC_REDO_LOG_HANDLER);
        } else if (MOT::GetGlobalConfiguration().m_enableGroupCommit) {
            MOT_LOG_INFO("Configuring segmented-group redo-log handler");
            result = AddExtTypedConfigItem<MOT::RedoLogHandlerType>(
                "", "redo_log_handler_type", MOT::RedoLogHandlerType::SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER);
        } else {
            MOT_LOG_INFO("Configuring synchronous redo-log handler");
            result = AddExtTypedConfigItem<MOT::RedoLogHandlerType>(
                "", "redo_log_handler_type", MOT::RedoLogHandlerType::SYNC_REDO_LOG_HANDLER);
        }
        return result;
    }

    bool ConfigureMaxProcessMemory()
    {
        bool result = true;

        // get max/min global memory and large allocation store for sessions from configuration
        MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
        uint64_t globalMemoryMb = motCfg.m_globalMemoryMaxLimitMB;
        uint64_t localMemoryMb = motCfg.m_localMemoryMaxLimitMB;
        uint64_t sessionLargeStoreMb = motCfg.m_sessionLargeBufferStoreSizeMB;

        // compare the sum with max_process_memory and system total
        uint64_t maxReserveMemoryMb = globalMemoryMb + localMemoryMb + sessionLargeStoreMb;

        // if the total memory is less than the required minimum, then issue a warning, fix it and return
        if (maxReserveMemoryMb < MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB) {
            MOT_LOG_WARN("MOT memory limits are too low, adjusting values");
            // we use current total as zero to force session large store zero value
            result =
                ConfigureMemoryLimits(MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
            return result;
        }

        // get system total
        uint64_t systemTotalMemoryMb = MOT::GetTotalSystemMemoryMb();
        if (systemTotalMemoryMb == 0) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Cannot retrieve total system memory");
            return false;
        }

        // get envelope limit
        uint64_t processTotalMemoryMb = g_instance.attr.attr_memory.max_process_memory / KILO_BYTE;

        // compute the real limit
        uint64_t upperLimitMb = min(systemTotalMemoryMb, processTotalMemoryMb);

        // get dynamic gap we need to preserve between MOT and envelope
        uint64_t dynamicGapMb = MIN_DYNAMIC_PROCESS_MEMORY / KILO_BYTE;

        MOT_LOG_TRACE("Checking for memory limits: globalMemoryMb=%" PRIu64 ", localMemoryMb=%" PRIu64
                      ", sessionLargeStoreMb=%" PRIu64 ", systemTotalMemoryMb=%" PRIu64
                      ", processTotalMemoryMb=%" PRIu64 ", upperLimitMb=%" PRIu64 ", dynamicGapMb=%" PRIu64
                      ", max_process_memory=%u",
            globalMemoryMb,
            localMemoryMb,
            sessionLargeStoreMb,
            systemTotalMemoryMb,
            processTotalMemoryMb,
            upperLimitMb,
            dynamicGapMb,
            g_instance.attr.attr_memory.max_process_memory);

        // we check that a 2GB gap is preserved
        if (upperLimitMb < maxReserveMemoryMb + dynamicGapMb) {
            // memory restriction conflict, issue warning and adjust values
            MOT_LOG_TRACE("MOT engine maximum memory definitions (global: %" PRIu64 " MB, local: %" PRIu64
                          " MB, session large "
                          "store: %" PRIu64 " MB, total: %" PRIu64
                          " MB) breach GaussDB maximum process memory restriction (%" PRIu64
                          " MB) and/or total system memory (%" PRIu64 " MB). "
                          "MOT values shall be adjusted accordingly to preserve required gap (%" PRIu64 " MB).",
                globalMemoryMb,
                localMemoryMb,
                sessionLargeStoreMb,
                maxReserveMemoryMb,
                processTotalMemoryMb,
                systemTotalMemoryMb,
                dynamicGapMb);

            // compute new total memory limit for MOT
            uint64_t newTotalMemoryMb = 0;
            if (upperLimitMb < dynamicGapMb) {
                // this can happen only if system memory is less than 2GB, we still allow minor breach
                MOT_LOG_WARN("Using minimal memory limits in MOT Engine due to system total memory restrictions");
                // we use current total as zero to force session large store zero value
                result = ConfigureMemoryLimits(
                    MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
            } else {
                newTotalMemoryMb = upperLimitMb - dynamicGapMb;
                if (newTotalMemoryMb < MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB) {
                    // in extreme cases we allow a minor breach of the dynamic gap
                    MOT_LOG_TRACE("Using minimal memory limits in MOT Engine due to GaussDB memory usage restrictions");
                    // we use current total as zero to force session large store zero value
                    result = ConfigureMemoryLimits(
                        MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
                } else {
                    MOT_LOG_TRACE("Adjusting memory limits in MOT Engine due to GaussDB memory usage restrictions");
                    result = ConfigureMemoryLimits(newTotalMemoryMb, maxReserveMemoryMb, globalMemoryMb, localMemoryMb);
                }
            }

            return result;
        }

        return result;
    }

    bool ConfigureMemoryLimits(uint64_t newTotalMemoryMb, uint64_t currentTotalMemoryMb, uint64_t currentGlobalMemoryMb,
        uint64_t currentLocalMemoryMb)
    {
        uint64_t newGlobalMemoryMb = 0;
        uint64_t newLocalMemoryMb = 0;
        uint64_t newSessionLargeStoreMemoryMb = 0;

        // compute new configuration values
        MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
        if (currentTotalMemoryMb > 0) {
            // we preserve the existing ratio between global and local memory, but reduce the total sum as required
            double ratio = ((double)newTotalMemoryMb) / ((double)currentTotalMemoryMb);
            newGlobalMemoryMb = (uint64_t)(currentGlobalMemoryMb * ratio);
            if (motCfg.m_sessionLargeBufferStoreSizeMB > 0) {
                newLocalMemoryMb = (uint64_t)(currentLocalMemoryMb * ratio);
                newSessionLargeStoreMemoryMb = newTotalMemoryMb - (newGlobalMemoryMb + newLocalMemoryMb);
            } else {
                // if the user configured zero for the session large store, then we want to keep it this way
                newSessionLargeStoreMemoryMb = 0;
                newLocalMemoryMb = newTotalMemoryMb - newGlobalMemoryMb;
            }

            MOT_LOG_TRACE("Attempting to use adjusted values for MOT memory limits: global = %" PRIu64
                          " MB, local = %" PRIu64 " MB, session large store = %" PRIu64 " MB, total = %" PRIu64 " MB",
                newGlobalMemoryMb,
                newLocalMemoryMb,
                newSessionLargeStoreMemoryMb,
                newGlobalMemoryMb + newLocalMemoryMb + newSessionLargeStoreMemoryMb);
        }

        // when current total memory is zero we use the minimum allowed, and also when minimum values are breached
        if ((newGlobalMemoryMb < MOT::MOTConfiguration::MIN_MAX_MOT_GLOBAL_MEMORY_MB) ||
            (newLocalMemoryMb < MOT::MOTConfiguration::MIN_MAX_MOT_LOCAL_MEMORY_MB)) {
            if (currentTotalMemoryMb > 0) {
                MOT_LOG_TRACE("Adjusted values breach minimum restrictions, falling back to minimum values");
            }
            newGlobalMemoryMb = MOT::MOTConfiguration::MIN_MAX_MOT_GLOBAL_MEMORY_MB;
            newLocalMemoryMb = MOT::MOTConfiguration::MIN_MAX_MOT_LOCAL_MEMORY_MB;
            newSessionLargeStoreMemoryMb = 0;
        }

        MOT_LOG_WARN("Adjusting MOT memory limits: global = %" PRIu64 " MB, local = %" PRIu64
                     " MB, session large store = %" PRIu64 " MB, total = %" PRIu64 " MB",
            newGlobalMemoryMb,
            newLocalMemoryMb,
            newSessionLargeStoreMemoryMb,
            newGlobalMemoryMb + newLocalMemoryMb + newSessionLargeStoreMemoryMb);

        // stream into MOT new definitions
        MOT::mot_string memCfg;
        bool result = memCfg.format("%" PRIu64 " MB", newGlobalMemoryMb);
        if (!result) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Load Configuration", "Failed to format global memory configuration string");
        } else {
            result = AddExtStringConfigItem("", "max_mot_global_memory", memCfg.c_str());
        }
        if (result) {
            result = memCfg.format("%" PRIu64 " MB", newLocalMemoryMb);
            if (!result) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Configuration", "Failed to format local memory configuration string");
            } else {
                result = AddExtStringConfigItem("", "max_mot_local_memory", memCfg.c_str());
            }
        }
        if (result && (motCfg.m_sessionLargeBufferStoreSizeMB != newSessionLargeStoreMemoryMb)) {
            result = memCfg.format("%" PRIu64 " MB", newSessionLargeStoreMemoryMb);
            if (!result) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Configuration", "Failed to format session memory configuration string");
            } else {
                result = AddExtStringConfigItem("", "session_large_buffer_store_size", memCfg.c_str());
            }
        }

        // Reset pre-allocation to zero, as it may be invalid now
        MOT_LOG_TRACE("Resetting memory pre-allocation to zero, since memory limits were adjusted");
        if (result) {
            result = AddExtStringConfigItem("", "min_mot_global_memory", "0 MB");
        }
        if (result) {
            result = AddExtStringConfigItem("", "min_mot_local_memory", "0 MB");
        }

        return result;
    }
};

#endif /* GAUSSDB_CONFIG_LOADER_H */
