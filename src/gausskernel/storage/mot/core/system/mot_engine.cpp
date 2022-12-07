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
 * mot_engine.cpp
 *    The single entry point to MOT storage engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/mot_engine.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "spin_lock.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "network_statistics.h"
#include "db_session_statistics.h"
#include "log_statistics.h"
#include "memory_statistics.h"
#include "process_statistics.h"
#include "system_statistics.h"
#include "thread_id.h"
#include "mm_api.h"
#include "mm_cfg.h"
#include "mm_global_api.h"
#include "mm_session_api.h"
#include "mm_raw_chunk_store.h"
#include "mm_numa.h"
#include "mot_error.h"
#include "connection_id.h"
#include "cycles.h"
#include "debug_utils.h"
#include "recovery_manager_factory.h"
#include "csn_manager.h"

// For mtSessionThreadInfo thread local
#include "kvthread.hh"

#include <unistd.h>
#include <cstdlib>
#include <sys/utsname.h>
#include <fstream>

namespace MOT {
IMPLEMENT_CLASS_LOGGER(MOTEngine, System);

MOTEngine* MOTEngine::m_engine = nullptr;

static CSNManager csnManager;

// Initialization helper macro (for system calls)
#define CHECK_SYS_INIT_STATUS(rc, syscall, format, ...)                                         \
    if (rc != 0) {                                                                              \
        MOT_REPORT_SYSTEM_PANIC_CODE(rc, syscall, "MOT Engine Startup", format, ##__VA_ARGS__); \
        break;                                                                                  \
    }

// Initialization helper macro (for sub-service initialization)
#define CHECK_INIT_STATUS(result, format, ...)                                             \
    if (!result) {                                                                         \
        MOT_REPORT_PANIC(MOT_ERROR_INTERNAL, "MOT Engine Startup", format, ##__VA_ARGS__); \
        break;                                                                             \
    }

MOTEngine::MOTEngine()
    : m_initialized(false),
      m_recovering(false),
      m_recoveringCleanup(false),
      m_sessionAffinity(0, 0, AffinityMode::AFFINITY_INVALID),
      m_taskAffinity(0, 0, AffinityMode::AFFINITY_INVALID),
      m_csnManager(nullptr),
      m_softMemoryLimitReached(0),
      m_padding(0),
      m_sessionManager(nullptr),
      m_tableManager(nullptr),
      m_surrogateKeyManager(nullptr),
      m_recoveryManager(nullptr),
      m_redoLogHandler(nullptr),
      m_checkpointManager(nullptr),
      m_ddlSigFunc(nullptr)
{}

MOTEngine::~MOTEngine()
{
    Destroy();
    m_csnManager = nullptr;
    m_ddlSigFunc = nullptr;
    m_sessionManager = nullptr;
    m_tableManager = nullptr;
    m_recoveryManager = nullptr;
    m_surrogateKeyManager = nullptr;
    m_redoLogHandler = nullptr;
    m_checkpointManager = nullptr;
}

MOTEngine* MOTEngine::CreateInstance(
    const char* configFilePath /* = nullptr */, int argc /* = 0 */, char* argv[] /* = nullptr */)
{
    if (m_engine == nullptr) {
        if (CreateInstanceNoInit(configFilePath, argc, argv) != nullptr) {
            bool result = m_engine->LoadConfig();
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "System Startup", "Failed to load Engine configuration");
            } else {
                result = m_engine->Initialize();
                if (!result) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "System Startup", "Engine initialization failed");
                }
            }

            if (!result) {
                DestroyInstance();
                MOT_ASSERT(m_engine == nullptr);
            }
        }
    }
    return m_engine;
}

void MOTEngine::DestroyInstance()
{
    if (m_engine != nullptr) {
        delete m_engine;
        m_engine = nullptr;
    }
}

MOTEngine* MOTEngine::CreateInstanceNoInit(
    const char* configFilePath /* = nullptr */, int argc /* = 0 */, char* argv[] /* = nullptr */)
{
    if (m_engine == nullptr) {
        MOT_LOG_TRACE("Startup: Loading MOT Engine instance");
        m_engine = new (std::nothrow) MOTEngine();
        if (m_engine == nullptr) {
            MOT_REPORT_PANIC(
                MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for MOTEngine instance, aborting");
        } else {
            m_engine->PrintCurrentWorkingDirectory();
            m_engine->PrintSystemInfo();
            if (!m_engine->InitializeConfiguration(configFilePath, argc, argv)) {
                MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
                    "MOT Engine Startup",
                    "Failed to load configuration from: %s",
                    configFilePath);
                delete m_engine;
                m_engine = nullptr;
            } else {
                m_engine->m_initStack.push(INIT_CFG_PHASE);
            }
        }
    }
    return m_engine;
}

bool MOTEngine::AddConfigLoader(ConfigLoader* configLoader)
{
    return ConfigManager::GetInstance().AddConfigLoader(configLoader);
}

bool MOTEngine::RemoveConfigLoader(ConfigLoader* configLoader)
{
    return ConfigManager::GetInstance().RemoveConfigLoader(configLoader->GetName());
}

bool MOTEngine::LoadConfig()
{
    bool result = ConfigManager::GetInstance().InitLoad();
    if (!result) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "System Startup", "Failed to load configuration for the first time");
    } else {
        if (!GetGlobalConfiguration().m_enableNuma) {
            MOT_LOG_WARN("NUMA-aware memory allocation is disabled");
        }

        // we must load also MM layer configuration so that max_process_memory initial check will see correct values
        int rc = MemCfgInit();
        if (rc != 0) {
            MOT_LOG_PANIC("Startup: Failed to load MM Layer configuration (see errors above), aborting.");
            result = false;
        }
    }
    if (result) {
        m_initStack.push(LOAD_CFG_PHASE);
    }
    return result;

    // now MOTEngine configuration is fully loaded and validated
    // so the envelope can check for conflicts between MOTEngine and envelope configuration
}

bool MOTEngine::Initialize()
{
    bool result = false;

    do {  // instead of goto
        m_initStack.push(INIT_CORE_SERVICES_PHASE);
        result = InitializeCoreServices();
        CHECK_INIT_STATUS(result, "Failed to Initialize core services");

        m_initStack.push(INIT_APP_SERVICES_PHASE);
        result = InitializeAppServices();
        CHECK_INIT_STATUS(result, "Failed to Initialize applicative services");

        m_initStack.push(START_BG_TASKS_PHASE);
        result = StartBackgroundTasks();
        CHECK_INIT_STATUS(result, "Failed to start background tasks");
    } while (0);

    if (result) {
        MOT_LOG_INFO("Startup: MOT Engine initialization finished successfully");
        m_initialized = true;
    } else {
        MOT_LOG_PANIC("Startup: MOT Engine initialization failed!");
        // caller is expected to call DestroyInstance() after failure
    }

    return result;
}

void MOTEngine::Destroy()
{
    MOT_LOG_INFO("Shutdown: Shutting down MOT Engine");
    while (!m_initStack.empty()) {
        switch (m_initStack.top()) {
            case START_BG_TASKS_PHASE:
                StopBackgroundTasks();
                break;

            case INIT_APP_SERVICES_PHASE:
                DestroyAppServices();
                break;

            case INIT_CORE_SERVICES_PHASE:
                DestroyCoreServices();
                break;

            case LOAD_CFG_PHASE:
                break;

            case INIT_CFG_PHASE:
                DestroyConfiguration();
                break;

            default:
                break;
        }
        m_initStack.pop();
    }
    ClearErrorStack();
    MOT_LOG_INFO("Shutdown: MOT Engine shutdown finished");
}

void MOTEngine::PrintCurrentWorkingDirectory() const
{
    char* cwd = (char*)malloc(sizeof(char) * PATH_MAX);
    if (cwd == nullptr) {
        MOT_LOG_TRACE("%s: Failed to allocate %d bytes", __FUNCTION__, PATH_MAX);
        return;
    }
    if (!getcwd(cwd, PATH_MAX)) {
        errno_t erc = strcpy_s(cwd, PATH_MAX, "N/A");
        securec_check(erc, "\0", "\0");
    }
    MOT_LOG_TRACE("Startup: Current working directory is %s", cwd);
    free(cwd);
}

void MOTEngine::PrintSystemInfo() const
{
#ifdef MOT_DEBUG
    MOT_LOG_WARN("Running in DEBUG mode");
#else
    MOT_LOG_TRACE("Running in RELEASE mode");
#endif
    const int bufSize = 128;
    char line[bufSize];
    struct utsname buf;
    if (uname(&buf) == 0) {
        MOT_LOG_TRACE("System: %s %s %s", buf.sysname, buf.release, buf.version);
    }

    // try common distribution files
    FILE* f = fopen("/etc/redhat-release", "r");
    if (f == nullptr) {
        f = fopen("/etc/SuSE-release", "r");
    }
    if (f == nullptr) {
        f = fopen("etc/euleros-release", "r");
    }
    if (f != nullptr) {
        while (fgets(line, bufSize, f) != nullptr) {
            if (strstr(line, "Linux") || strstr(line, "release")) {
                char* newlinePos = strchr(line, '\n');
                if (newlinePos != nullptr) {
                    *newlinePos = 0;
                }
                MOT_LOG_TRACE("Distribution: %s", line);
                break;
            }
        }
        (void)fclose(f);
    } else {
        // default to /etc/os-release
        f = fopen("/etc/os-release", "r");
        if (f != nullptr) {
            while (fgets(line, bufSize, f) != nullptr) {
                if (strstr(line, "PRETTY_NAME")) {
                    char* equalsPos = strchr(line, '=');
                    if (equalsPos != nullptr) {
                        // skip quote in beginning and end (2 characters)
                        size_t len = strlen(equalsPos + 2) - 1;
                        MOT_LOG_TRACE("Distribution: %.*s", len, equalsPos + 2)
                    }
                    break;
                }
            }
            (void)fclose(f);
        }
    }
}

bool MOTEngine::InitializeCoreServices()
{
    bool result = false;

    MOT_LOG_TRACE("Startup: Initializing core services");

    do {
        CpuCyclesLevelTime::Init();
        MOT_LOG_TRACE("Startup: System clock frequency detected");
        CheckPolicies();

        MOTConfiguration& cfg = GetGlobalConfiguration();
        m_sessionAffinity.Configure(cfg.m_numaNodes, cfg.m_coresPerCpu, cfg.m_sessionAffinityMode);
        m_taskAffinity.Configure(cfg.m_numaNodes, cfg.m_coresPerCpu, cfg.m_taskAffinityMode);
        MOT_LOG_INFO("Startup: Affinity initialized - numaNodes = %u, coresPerCpu = %u, sessionAffinityMode = %u, "
                     "taskAffinityMode = %u",
            cfg.m_numaNodes,
            cfg.m_coresPerCpu,
            cfg.m_sessionAffinityMode,
            cfg.m_taskAffinityMode);

        result = (InitThreadIdPool(cfg.m_maxThreads) == 0);
        CHECK_INIT_STATUS(result, "Failed to Initialize reusable thread identifier pool");
        m_initCoreStack.push(INIT_THREAD_ID_POOL_PHASE);

        result = (InitConnectionIdPool(cfg.m_maxConnections) == 0);
        CHECK_INIT_STATUS(result, "Failed to Initialize reusable connection identifier pool");
        m_initCoreStack.push(INIT_CONNECTION_ID_POOL_PHASE);

        // make sure loading thread gets a highest thread id on NUMA node 0, so next worker thread can use id 0
        result = (AllocThreadIdNumaHighest(0) != INVALID_THREAD_ID);
        CHECK_INIT_STATUS(result, "Failed to allocate loader thread identifier");
        m_initCoreStack.push(INIT_LOADER_THREAD_ID_PHASE);

        result = InitCurrentNumaNodeId();
        CHECK_INIT_STATUS(result, "Failed to allocate loader thread NUMA node identifier");
        m_initCoreStack.push(INIT_LOADER_NODE_ID_PHASE);

        result = InitializeStatistics();  // Initialize statistics objects
        CHECK_INIT_STATUS(result, "Failed to Initialize statistics collection");
        m_initCoreStack.push(INIT_STATISTICS_PHASE);

#ifdef MEM_ACTIVE
        result = (MemInit(cfg.m_reserveMemoryMode) == 0);
        CHECK_INIT_STATUS(result, "Failed to Initialize memory management sub-system");
        m_initCoreStack.push(INIT_MM_PHASE);
#endif

        result = InitializeSessionManager();
        CHECK_INIT_STATUS(result, "Failed to Initialize session manager");
        m_initCoreStack.push(INIT_SESSION_MANAGER_PHASE);

        result = InitializeTableManager();
        CHECK_INIT_STATUS(result, "Failed to Initialize table manager");
        m_initCoreStack.push(INIT_TABLE_MANAGER_PHASE);

        result = InitializeSurrogateKeyManager();
        CHECK_INIT_STATUS(result, "Failed to Initialize surrogate key manager");
        m_initCoreStack.push(INIT_SURROGATE_KEY_MANAGER_PHASE);

        result = m_gcContext.Init();
        CHECK_INIT_STATUS(result, "Failed to Initialize garbage collection sub-system");
        m_initCoreStack.push(INIT_GC_PHASE);

        result = InitializeDebugUtils();
        CHECK_INIT_STATUS(result, "Failed to Initialize debug utilities");
        m_initCoreStack.push(INIT_DEBUG_UTILS);

        SetCSNManager(&csnManager);
        m_initCoreStack.push(INIT_CSN_MANAGER);
    } while (0);

    if (result) {
        MOT_LOG_INFO("Startup: All core services Initialized successfully");
    } else {
        MOT_LOG_ERROR("Startup: Failed to initializing core services");
    }

    return result;
}

bool MOTEngine::InitializeAppServices()
{
    bool result = false;

    MOT_LOG_TRACE("Startup: Initializing applicative services");

    do {
        result = InitializeRedoLogHandler();
        CHECK_INIT_STATUS(result, "Failed to Initialize the redo-log handler");
        m_initAppStack.push(INIT_REDO_LOG_HANDLER_PHASE);

        result = InitializeRecoveryManager();
        CHECK_INIT_STATUS(result, "Failed to Initialize the recovery manager");
        m_initAppStack.push(INIT_RECOVERY_MANAGER_PHASE);

        // CheckpointManager uses RedoLogHandler, always make sure that
        // RedoLogHandler is initialize before CheckpointManager
        result = InitializeCheckpointManager();
        CHECK_INIT_STATUS(result, "Failed to Initialize the checkpoint manager");
        m_initAppStack.push(INIT_CHECKPOINT_MANAGER_PHASE);
    } while (0);

    if (result) {
        MOT_LOG_TRACE("Startup: All applicative services Initialized successfully");
    } else {
        MOT_LOG_ERROR("Startup: Failed to initializing applicative services");
    }

    return result;
}

bool MOTEngine::StartBackgroundTasks()
{
    bool result = true;

    MOT_LOG_TRACE("Startup: Starting background tasks");

    do {
        if (GetGlobalConfiguration().m_enableStats) {
            result = StatisticsManager::GetInstance().Start();
            CHECK_INIT_STATUS(result, "Failed to start the periodic statistics printing task");
            MOT_LOG_INFO("Startup: Statistics reporter started");
            m_startBgStack.push(START_STAT_PRINT_PHASE);
        }
    } while (0);

    if (result) {
        MOT_LOG_TRACE("Startup: All background tasks started successfully");
    } else {
        MOT_LOG_ERROR("Startup: Failed to start background tasks");
    }

    return result;
}

void MOTEngine::DestroyCoreServices()
{
    if (m_sessionManager != nullptr) {
        m_sessionManager->ReportActiveSessions();
    }

    MOT_LOG_TRACE("Shutdown: Destroying all core services");
    while (!m_initCoreStack.empty()) {
        switch (m_initCoreStack.top()) {
            case INIT_DEBUG_UTILS:
                DestroyDebugUtils();
                break;

            case INIT_GC_PHASE:
                break;

            case INIT_SURROGATE_KEY_MANAGER_PHASE:
                DestroySurrogateKeyManager();
                break;

            case INIT_TABLE_MANAGER_PHASE:
                DestroyTableManager();
                break;

            case INIT_SESSION_MANAGER_PHASE:
                DestroySessionManager();
                break;

            case INIT_MM_PHASE:
                MemDestroy();
                break;

            case INIT_STATISTICS_PHASE:
                DestroyStatistics();
                break;

            case INIT_LOADER_NODE_ID_PHASE:
                ClearCurrentNumaNodeId();
                break;

            case INIT_LOADER_THREAD_ID_PHASE:
                FreeThreadId();
                break;

            case INIT_CONNECTION_ID_POOL_PHASE:
                DestroyConnectionIdPool();
                break;

            case INIT_THREAD_ID_POOL_PHASE:
                DestroyThreadIdPool();
                break;

            default:
                break;
        }
        m_initCoreStack.pop();
    }

    MOT_LOG_INFO("Shutdown: All core services Destroyed");

    MOT_ASSERT(MOTCurrThreadId == INVALID_THREAD_ID);
    MOT_ASSERT(MOT_GET_CURRENT_SESSION_ID() == INVALID_SESSION_ID);
    MOT_ASSERT(MOTCurrentNumaNodeId == MEM_INVALID_NODE);
}

void MOTEngine::DestroyAppServices()
{
    MOT_LOG_TRACE("Shutdown: Destroying all applicative services");

    while (!m_initAppStack.empty()) {
        switch (m_initAppStack.top()) {
            case INIT_CHECKPOINT_MANAGER_PHASE:
                DestroyCheckpointManager();
                break;

            case INIT_RECOVERY_MANAGER_PHASE:
                DestroyRecoveryManager();
                break;

            case INIT_REDO_LOG_HANDLER_PHASE:
                DestroyRedoLogHandler();
                break;

            default:
                break;
        }
        m_initAppStack.pop();
    }

    MOT_LOG_TRACE("Shutdown: All applicative services Destroyed");

    m_tableManager->ClearAllTables();
    MOT_LOG_INFO("Shutdown: All tables cleared");
}

void MOTEngine::StopBackgroundTasks()
{
    MOT_LOG_TRACE("Shutdown: Stopping all background tasks");

    while (!m_startBgStack.empty()) {
        switch (m_startBgStack.top()) {
            case START_STAT_PRINT_PHASE:
                if (GetGlobalConfiguration().m_enableStats) {
                    StatisticsManager::GetInstance().Stop();
                }

            // fall through
            default:
                break;
        }
        m_startBgStack.pop();
    }

    MOT_LOG_INFO("Shutdown: All background tasks stopped");
}

bool MOTEngine::InitializeConfiguration(const char* configFilePath, int argc, char* argv[])
{
    bool result = true;

    // print the canonical path of the configuration file
    if (configFilePath != nullptr) {
        char* resolvedPath = (char*)malloc(sizeof(char) * PATH_MAX);
        if (resolvedPath == nullptr) {
            MOT_LOG_ERROR("Failed to allocate %u bytes for configuration loading", (unsigned)(sizeof(char) * PATH_MAX));
        } else {
            if (!realpath(configFilePath, resolvedPath)) {
                MOT_LOG_SYSTEM_WARN(realpath, "Failed to get resolved path from path %s", configFilePath);
                errno_t erc = strcpy_s(resolvedPath, PATH_MAX, "N/A");
                securec_check(erc, "\0", "\0");
            }
            MOT_LOG_INFO("Startup: Loading configuration from %s", resolvedPath);
            free(resolvedPath);
        }
    }

    // explicitly initialize global configuration singleton
    MOTConfiguration& motCfg = GetGlobalConfiguration();
    motCfg.Initialize();

    // create manager, and configure it with configuration file loader
    if (!ConfigManager::CreateInstance(argv, argc)) {
        MOT_LOG_ERROR("Failed to create configuration manager instance");
        result = false;
    } else {
        // register global configuration as first listener (other listeners rely on it)
        result = ConfigManager::GetInstance().AddConfigChangeListener(&motCfg);
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to register main configuration listener at the configuration manager");
            ConfigManager::DestroyInstance();
        } else if (configFilePath != nullptr) {
            // add configuration file loader
            result = ConfigManager::GetInstance().AddConfigFile(configFilePath);
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to add configuration file %s to configuration manager",
                    configFilePath);
                ConfigManager::DestroyInstance();
            }
        }
    }

    if (result) {
        MOT_LOG_TRACE("Startup: Configuration manager Initialized");
    }
    return result;
}

bool MOTEngine::InitializeStatistics()
{
    MOT_LOG_TRACE("Startup: Initializing statistics module");

    // create statistics manager before all other sub-systems
    bool result = false;

    // global statistics objects
    do {
        result = StatisticsManager::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize statistics manager");

        result = NetworkStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize network statistics provider");

        result = DbSessionStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize DB Session statistics provider");

        result = LogStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize redo log statistics provider");

        result = MemoryStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize memory statistics provider");

        result = DetailedMemoryStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize detailed memory statistics provider");

        result = ProcessStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize process statistics provider");

        result = SystemStatisticsProvider::CreateInstance();
        CHECK_INIT_STATUS(result, "Failed to Initialize system statistics provider");
    } while (0);

    if (!result) {
        // all singletons have safe cleanup
        DestroyStatistics();
        MOT_LOG_ERROR("Startup: Statistics module initialization failed");
    } else {
        MOT_LOG_TRACE("Startup: Statistics module Initialized successfully");
    }

    return result;
}

bool MOTEngine::InitializeSessionManager()
{
    MOT_LOG_TRACE("Startup: Initializing session manager");

    if (m_sessionManager != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize session manager");
        return false;
    }

    m_sessionManager = new (std::nothrow) SessionManager();
    if (m_sessionManager == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for session manager object");
        return false;
    }

    if (!m_sessionManager->Initialize(g_memGlobalCfg.m_nodeCount, g_memGlobalCfg.m_maxThreadCount)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOT Engine Startup", "Failed to Initialize the session manager");
        delete m_sessionManager;
        m_sessionManager = nullptr;
        return false;
    }

    MOT_LOG_TRACE("Startup: Session manager initialized successfully");
    return true;
}

bool MOTEngine::InitializeTableManager()
{
    MOT_LOG_TRACE("Startup: Initializing table manager");

    if (m_tableManager != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize table manager");
        return false;
    }

    m_tableManager = new (std::nothrow) TableManager();
    if (m_tableManager == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for table manager object");
        return false;
    }

    MOT_LOG_TRACE("Startup: Table manager initialized successfully");
    return true;
}

bool MOTEngine::InitializeSurrogateKeyManager()
{
    MOT_LOG_TRACE("Startup: Initializing surrogate key manager");

    if (m_surrogateKeyManager != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize surrogate key manager");
        return false;
    }

    m_surrogateKeyManager = new (std::nothrow) SurrogateKeyManager();
    if (m_surrogateKeyManager == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for surrogate key manager object");
        return false;
    }

    if (!m_surrogateKeyManager->Initialize(GetGlobalConfiguration().m_maxConnections)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOT Engine Startup", "Failed to Initialize the surrogate key manager");
        delete m_surrogateKeyManager;
        m_surrogateKeyManager = nullptr;
        return false;
    }

    MOT_LOG_TRACE("Startup: Surrogate key manager initialized successfully");
    return true;
}

bool MOTEngine::InitializeRecoveryManager()
{
    MOT_LOG_TRACE("Initializing the Recovery Manager");

    if (m_recoveryManager != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize recovery manager");
    }

    m_recoveryManager = RecoveryManagerFactory::CreateRecoveryManager();
    if (m_recoveryManager == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for recovery manager object");
        return false;
    }

    if (!m_recoveryManager->Initialize()) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOT Engine Startup", "Failed to Initialize the Recovery Manager");
        delete m_recoveryManager;
        m_recoveryManager = nullptr;
        return false;
    }

    MOT_LOG_INFO("Startup: Recovery manager initialized successfully");
    return true;
}

bool MOTEngine::InitializeCheckpointManager()
{
    MOT_LOG_TRACE("Initializing the Checkpoint Manager");

    if (m_checkpointManager != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize checkpoint manager");
    }

    m_checkpointManager = new (std::nothrow) CheckpointManager();
    if (m_checkpointManager == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for checkpoint manager object");
        return false;
    }

    if (!m_checkpointManager->Initialize()) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to initialize checkpoint manager");
        delete m_checkpointManager;
        m_checkpointManager = nullptr;
        return false;
    }

    MOT_LOG_INFO("Startup: Checkpoint manager initialized successfully");
    return true;
}

bool MOTEngine::InitializeRedoLogHandler()
{
    MOT_LOG_TRACE("Initializing the redo-log handler");

    if (m_redoLogHandler != nullptr) {
        // this is highly unexpected, and should especially be guarded in scenario of switch-over to standby (i.e. when
        // engine is re-initialized)
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_STATE, "MOT Engine Startup", "Double attempt to initialize redo-log handler");
    }

    if (GetGlobalConfiguration().m_enableRedoLog) {
        m_redoLogHandler = RedoLogHandlerFactory::CreateRedoLogHandler();
        if (m_redoLogHandler == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to allocate memory for redo-log handler");
            return false;
        }

        if (!m_redoLogHandler->Init()) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "MOT Engine Startup", "Failed to initialize redo-log handler");
            return false;
        }

        MOT_LOG_INFO("Startup: Redo-log handler initialized successfully");
    } else {
        MOT_LOG_INFO("Startup: Redo-log is disabled");
    }
    return true;
}

void MOTEngine::DestroyConfiguration()
{
    MOT_LOG_TRACE("Shutdown: Destroying configuration module");
    ConfigManager::DestroyInstance();
    MOT_LOG_INFO("Shutdown: Configuration module Destroyed");
}

void MOTEngine::DestroyStatistics()
{
    MOT_LOG_TRACE("Shutdown: Destroying statistics module");

    // cleanup global statistics objects
    SystemStatisticsProvider::DestroyInstance();
    ProcessStatisticsProvider::DestroyInstance();
    DetailedMemoryStatisticsProvider::DestroyInstance();
    MemoryStatisticsProvider::DestroyInstance();
    LogStatisticsProvider::DestroyInstance();
    DbSessionStatisticsProvider::DestroyInstance();
    NetworkStatisticsProvider::DestroyInstance();

    // cleanup statistics manager
    StatisticsManager::DestroyInstance();

    MOT_LOG_INFO("Shutdown: Statistics module Destroyed");
}

void MOTEngine::DestroySessionManager()
{
    MOT_LOG_TRACE("Shutdown: Destroying session manager");

    if (m_sessionManager != nullptr) {
        m_sessionManager->Destroy();
        delete m_sessionManager;
        m_sessionManager = nullptr;
    }

    MOT_LOG_INFO("Shutdown: Session manager destroyed");
}

void MOTEngine::DestroyTableManager()
{
    MOT_LOG_TRACE("Shutdown: Destroying table manager");

    if (m_tableManager != nullptr) {
        delete m_tableManager;
        m_tableManager = nullptr;
    }

    MOT_LOG_INFO("Shutdown: Table manager destroyed");
}

void MOTEngine::DestroySurrogateKeyManager()
{
    MOT_LOG_TRACE("Shutdown: Destroying key surrogate manager");

    if (m_surrogateKeyManager != nullptr) {
        m_surrogateKeyManager->Destroy();
        delete m_surrogateKeyManager;
        m_surrogateKeyManager = nullptr;
    }

    MOT_LOG_INFO("Shutdown: Key surrogate manager destroyed");
}

void MOTEngine::DestroyRecoveryManager()
{
    MOT_LOG_INFO("Destroying the Recovery Manager");
    if (m_recoveryManager != nullptr) {
        delete m_recoveryManager;
        m_recoveryManager = nullptr;
    }
}

void MOTEngine::DestroyCheckpointManager()
{
    MOT_LOG_INFO("Destroying the Checkpoint Manager");
    if (m_checkpointManager != nullptr) {
        delete m_checkpointManager;
        m_checkpointManager = nullptr;
    }
}

void MOTEngine::DestroyRedoLogHandler()
{
    MOT_LOG_INFO("Destroying the Redo Log Handler");
    if (m_redoLogHandler != nullptr) {
        delete m_redoLogHandler;
        m_redoLogHandler = nullptr;
    }
}

void MOTEngine::OnCurrentThreadEnding()
{
    MOT_LOG_TRACE("Cleaning up current thread");
    // we must guard against calls from unknown threads, or repeated calls from the same thread
    MOTThreadId threadId = MOTCurrThreadId;
    if ((threadId == INVALID_THREAD_ID) || (threadId >= MAX_THREAD_COUNT) ||
        (threadId >= GetGlobalConfiguration().m_maxThreads)) {
        MOT_LOG_WARN(
            "Ignoring attempt to cleanup current thread with invalid thread identifier: %u", (unsigned)threadId);
        return;
    }

    // masstree thread-info
    DestroyMasstreeThreadinfo();

    // thread GC session
    StatisticsManager::GetInstance().UnreserveThreadSlot();
    m_tableManager->ClearTablesThreadMemoryCache();
#if defined(MEM_ACTIVE)
    MemClearSessionThreadCaches();
#endif
    ClearCurrentNumaNodeId();
    FreeThreadId();
}

uint64_t MOTEngine::GetCurrentMemoryConsumptionBytes() const
{
    return MemGetCurrentGlobalMemoryBytes();
}

uint64_t MOTEngine::GetHardMemoryLimitBytes() const
{
    return g_memGlobalCfg.m_maxGlobalMemoryMb * MEGA_BYTE;
}

void MOTEngine::CheckPolicies()
{
    std::ifstream inFile;
    inFile.open("/proc/sys/kernel/numa_balancing");

    uint32_t flagValue;
    if ((inFile >> flagValue) && (flagValue != 0)) {
        MOT_LOG_WARN(
            "NUMA policy in the system is set to automatic. (/proc/sys/kernel/numa_balancing is %u)", flagValue);
    }
}
}  // namespace MOT
