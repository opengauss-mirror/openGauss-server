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
 * mot_engine.h
 *    The single entry point to MOT storage engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/mot_engine.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_ENGINE_H
#define MOT_ENGINE_H

#include <cstdint>
#include <map>
#include <string>
#include <queue>
#include <mutex>
#include <stack>
#include "table.h"
#include "affinity.h"
#include "icsn_manager.h"
#include "checkpoint_manager.h"
#include "utilities.h"
#include "redo_log_handler.h"
#include "mot_configuration.h"
#include "irecovery_manager.h"
#include "table_manager.h"
#include "session_manager.h"
#include "surrogate_key_manager.h"
#include "gc_context.h"
#include "mot_atomic_ops.h"

namespace MOT {
class ConfigLoader;
class RedoLogHandler;

/** @typedef CpSigFunc Callback for notifying envelope that engine finished checkpoint. */
typedef void (*CpSigFunc)(void);

/** @enum Transactional DDL execution phase constants. */
enum class TxnDDLPhase {
    /** @var Denotes transaction DDL execution. Emitted for each DDL operation within the transaction. */
    TXN_DDL_PHASE_EXEC,

    /** @var Denotes transaction DDL commit. Emitted only for the entire transaction. */
    TXN_DDL_PHASE_COMMIT,

    /**
     * @var Denotes transaction DDL rollback. Emitted on the relation level for create table and create index, and also
     * for the entire transaction.
     */
    TXN_DDL_PHASE_ROLLBACK,

    TXN_DDL_PHASE_POST_COMMIT_CLEANUP
};

/** @typedef DDLSigFunc Callback for being notified of DDL events in MOT tables. */
typedef void (*DDLSigFunc)(uint64_t relationId, DDLAccessType event, TxnDDLPhase txnDdlPhase);

/**
 * @class MOTEngine
 * @brief The single entry point to the MOT engine.
 * @detail The MOT engine manages three sets of objects: tables (with indexes), sessions
 * and transactions. Each object set is managed as a concurrent map. In addition The memory engine
 * provides access to configuration, logging and statistics.
 */
class MOTEngine {
private:
    /** @brief Private singleton constructor. */
    MOTEngine();

    /** @brief Private singleton destructor. */
    ~MOTEngine();

    /** @var The singleton engine instance. */
    static MOTEngine* m_engine;

public:
    // Initialization/Termination/Configuration API
    /**
     * @brief Creates the single MOT engine instance. Use this variant when you have no external configuration
     * loaders involved.
     * @param[opt] configFilePath The path to the configuration file of the MOT engine.
     * @param[opt] argc Command line argument count (excluding program name first argument).
     * @param[opt] argv Command line argument array (excluding program name first argument).
     * @return A reference to the single MOT engine instance, or null if initialization failed.
     */
    static MOTEngine* CreateInstance(const char* configFilePath = nullptr, int argc = 0, char* argv[] = nullptr);

    /**
     * @brief Destroys the single MOT engine instance.
     */
    static void DestroyInstance();

    /**
     * @brief Retrieves a reference to the single MOT engine instance.
     */
    static inline MOTEngine* GetInstance()
    {
        return m_engine;
    }

    /**
     * @brief Creates the single MOT engine instance and loads configuration, but without
     * executing any other initialization code. A further call to @ref Initialize() is expected.
     * @note Use this variant when you wish to add more configuration loaders before actual
     * initialization takes place. In this case initialization has several steps: @ref CreateInstanceNoInit(), @ref
     * AddConfigLoader(), @ref AddConfigLoader(), ..., @ref LoadConfig(), @ref Initialize().
     * @param[opt] configFilePath The path to the configuration file of the MOT engine.
     * @param[opt] argc Command line argument count (excluding program name first argument).
     * @param[opt] argv Command line argument array (excluding program name first argument).
     * @return A reference to the single MOT engine instance if configuration loading
     * succeeded, otherwise nullptr.
     */
    static MOTEngine* CreateInstanceNoInit(const char* configFilePath = nullptr, int argc = 0, char* argv[] = nullptr);

    /**
     * @brief Allow for adding external (envelope-based) configuration loader.
     * @param configLoader The external configuration loader to register.
     * @return True if succeeded, otherwise false.
     */
    bool AddConfigLoader(ConfigLoader* configLoader);

    /**
     * @brief Allow for removing external (envelope-based) configuration loader.
     * @param configLoader The external configuration loader to unregister.
     * @return True if succeeded, otherwise false.
     */
    bool RemoveConfigLoader(ConfigLoader* configLoader);

    /**
     * @brief Loads configuration from all registered configuration loaders. After this call is made,
     * the envelope can check for any conflicts with the envelope configuration. If there is no
     * conflict, then a call to @ref Initialize() can be made.
     * @return True if configuration was loaded successfully and is valid.
     */
    bool LoadConfig();

    /**
     * @brief Initializes the single MOT engine instance.
     * @return True if initialization succeeded, otherwise false, in which case the singleton instance
     * of the engine is deleted and nullified.
     * @note Call this method only if the engine was created with @ref createInstanceNoInit().
     */
    bool Initialize();

    /**
     * @brief Convenience interface to initialize auxiliary structures for managing affinity.
     */
    void InitializeAffinity()
    {
        MOTConfiguration& cfg = GetGlobalConfiguration();
        m_sessionAffinity.Configure(cfg.m_numaNodes, cfg.m_coresPerCpu, cfg.m_sessionAffinityMode);
        m_taskAffinity.Configure(cfg.m_numaNodes, cfg.m_coresPerCpu, cfg.m_taskAffinityMode);
    }

    // Misc API
    /** @brief Retrieves the session manager. */
    inline SessionManager* GetSessionManager()
    {
        return m_sessionManager;
    }

    /** @brief Retrieves the current GC session. */
    inline GcManager* GetCurrentGcSession()
    {
        return m_sessionManager->GetCurrentGcSession();
    }

    /** @brief Should be called by the envelope when a thread ends. Used for thread-level cleanup. */
    void OnCurrentThreadEnding();

    /** @brief Retrieves the table manager. */
    inline TableManager* GetTableManager()
    {
        return m_tableManager;
    }

    /** @brief Retrieves the surrogate key manager. */
    inline SurrogateKeyManager* GetSurrogateKeyManager()
    {
        return m_surrogateKeyManager;
    }

    /**
     * @brief Retrieves the affinity configuration for user sessions.
     * @return The affinity configuration for user sessions.
     */
    inline Affinity& GetSessionAffinity()
    {
        return m_sessionAffinity;
    }

    /**
     * @brief Retrieves the MOT task affinity configuration.
     * @return The MOT task affinity configuration.
     */
    inline Affinity& GetTaskAffinity()
    {
        return m_taskAffinity;
    }

    /** @brief Retrieves the commit sequence number manager. */
    inline void SetCSNManager(ICSNManager* manager)
    {
        m_csnManager = manager;
    }

    /** @brief Retrieves the commit sequence number manager. */
    inline ICSNManager& GetCSNManager()
    {
        return *m_csnManager;
    }

    inline uint64_t GetCurrentCSN()
    {
        return m_csnManager->GetCurrentCSN();
    }

    inline uint64_t GetNextCSN()
    {
        return m_csnManager->GetNextCSN();
    }

    inline uint64_t GetGcEpoch()
    {
        return m_csnManager->GetGcEpoch();
    }

    /** @brief Retrieves the transaction id manager. */
    inline TransactionIdManager& GetTxnIdManager()
    {
        return m_txnIdManager;
    }

    /** @brief Installs DDL event listener. */
    inline void SetDDLCallback(DDLSigFunc ddlSigFunc)
    {
        m_ddlSigFunc = ddlSigFunc;
    }

    /** @brief Notify of DDL events to external listeners. */
    inline void NotifyDDLEvent(uint64_t relationId, DDLAccessType event, TxnDDLPhase txnPhase) const
    {
        if (m_ddlSigFunc != nullptr) {
            m_ddlSigFunc(relationId, event, txnPhase);
        }
    }

    // Logging/Checkpoint API
    inline CheckpointManager* GetCheckpointManager()
    {
        return m_checkpointManager;
    }

    inline RedoLogHandler* GetRedoLogHandler()
    {
        return m_redoLogHandler;
    }

    inline bool CreateSnapshot()
    {
        bool result = true;
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            result = m_checkpointManager->CreateSnapShot();
        }
        return result;
    }

    inline bool SnapshotReady(uint64_t lsn)
    {
        bool result = true;
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            result = m_checkpointManager->SnapshotReady(lsn);
        }
        return result;
    }

    inline bool BeginCheckpoint()
    {
        bool result = true;
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            result = m_checkpointManager->BeginCheckpoint();
        }
        return result;
    }

    inline bool AbortCheckpoint()
    {
        bool result = true;
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            result = m_checkpointManager->Abort();
        }
        return result;
    }

    inline int GetCheckpointErrCode() const
    {
        int result = 0;
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            result = m_checkpointManager->GetErrorCode();
        }
        return result;
    }

    inline const char* GetCheckpointErrStr() const
    {
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            return m_checkpointManager->GetErrorString();
        }
        return nullptr;
    }

    /**
     * @brief Order the engine to write all of its redo log records to the log file.
     */
    inline bool WriteLog()
    {
        bool result = false;
        if (GetGlobalConfiguration().m_enableRedoLog) {
            m_redoLogHandler->Write();
            result = true;
        }
        return result;
    }

    // GC Context API
    inline GcManager* GetGcContext(uint32_t threadId)
    {
        return m_gcContext.GetGcContext(threadId);
    }

    inline void SetGcContext(uint32_t threadId, GcManager* gcManager)
    {
        return m_gcContext.SetGcContext(threadId, gcManager);
    }

    // Recovery API
    inline IRecoveryManager* GetRecoveryManager()
    {
        return m_recoveryManager;
    }

    inline bool CreateRecoverySessionContext()
    {
        MOT_ASSERT(m_recoveryManager);
        SessionContext* ctx = m_sessionManager->CreateSessionContext();
        MOT_ASSERT(ctx);
        if (ctx == nullptr) {
            return false;
        }

        /*
         * In recovery/replay, OCC Validation should always wait, since there should not be any conflict.
         * The checkpoint locks the row before writing to the checkpoint buffer. So, if the OCC validation
         * is set to true, commit transaction might fail with RC_ABORT during replay.
         */
        ctx->GetTxnManager()->SetValidationNoWait(false);

        return true;
    }

    inline void DestroyRecoverySessionContext()
    {
        SessionContext* ctx = MOT_GET_CURRENT_SESSION_CONTEXT();
        if (ctx != nullptr) {
            m_sessionManager->DestroySessionContext(ctx);
        }
        OnCurrentThreadEnding();
    }

    inline void SetRecoveryStatus(bool value)
    {
        MOT_ATOMIC_STORE(m_recovering, value);
    }

    inline void SetRecoveryCleanupStatus(bool value)
    {
        MOT_ATOMIC_STORE(m_recoveringCleanup, value);
    }

    inline bool StartRecovery()
    {
        SetRecoveryStatus(true);
        if (!CreateRecoverySessionContext()) {
            return false;
        }
        MOT_ASSERT(m_recoveryManager);
        if (!m_recoveryManager->RecoverDbStart()) {
            return false;
        }
        return true;
    }

    inline bool EndRecovery()
    {
        MOT_ASSERT(m_recoveryManager);
        SetRecoveryCleanupStatus(true);
        bool status = m_recoveryManager->RecoverDbEnd();
        DestroyRecoverySessionContext();
        SetRecoveryCleanupStatus(false);
        SetRecoveryStatus(false);
        return status;
    }

    bool IsRecovering() const
    {
        return m_recovering;
    }

    bool IsRecoveryPerformingCleanup() const
    {
        return MOT_ATOMIC_LOAD(m_recoveringCleanup);
    }

    // Memory Limit API
    /** @brief Queries if soft memory limit was reached. */
    inline bool IsSoftMemoryLimitReached() const
    {
        return (m_softMemoryLimitReached != 0);
    }

    /** @brief Notify soft memory limit was reached. */
    inline void SetSoftMemoryLimitReached()
    {
        MOT_ATOMIC_STORE(m_softMemoryLimitReached, 1);
    }

    /** @brief Reset soft memory limit flag. */
    inline void ResetSoftMemoryLimitReached()
    {
        MOT_ATOMIC_STORE(m_softMemoryLimitReached, 0);
    }

    /**
     * @brief Retrieves the current memory consumption (on all chunk pools).
     * @return The total memory consumption in bytes or zero if failed.
     */
    uint64_t GetCurrentMemoryConsumptionBytes() const;

    /** @brief Retrieves the configured maximum memory consumption (on all chunk pools). */
    uint64_t GetHardMemoryLimitBytes() const;

private:
    /** @brief Safe cleanup of all Initialized resources. */
    void Destroy();

    /** @brief Print startup information. */
    void PrintCurrentWorkingDirectory() const;

    /** @brief Print startup information. */
    void PrintSystemInfo() const;

    /**
     * @brief Initializes all core services in the engine.
     * @return True if initialization was successful.
     */
    bool InitializeCoreServices();

    /**
     * @brief Initializes all applicative services in the engine.
     * @return True if initialization was successful.
     */
    bool InitializeAppServices();

    /**
     * @brief Starts all background tasks.
     * @return True if initialization was successful.
     */
    bool StartBackgroundTasks();

    /**
     * @brief Destroys all core services in the engine.
     */
    void DestroyCoreServices();

    /**
     * @brief Destroys all applicative services in the engine.
     * @return True if initialization was successful.
     */
    void DestroyAppServices();

    /**
     * @brief Stops all background tasks.
     * @return True if initialization was successful.
     */
    void StopBackgroundTasks();

    /**
     * @brief Initializes global configuration.
     * @param configFilePath The path to the configuration file of the MOT engine.
     * @param argc Command line argument count.
     * @param argv Command line argument array.
     * @return True if configuration loading succeeded, or false if loading failed, or if the loaded
     * configuration is invalid.
     */
    bool InitializeConfiguration(const char* configFilePath, int argc, char* argv[]);

    /** @brief Initializes the statistics manager and all providers. */
    bool InitializeStatistics();

    /** @brief Initializes the session manager. */
    bool InitializeSessionManager();

    /** @brief Initializes the table manager. */
    bool InitializeTableManager();

    /** @brief Initializes the surrogate key manager. */
    bool InitializeSurrogateKeyManager();

    /** @brief Initializes the recovery manager. */
    bool InitializeRecoveryManager();

    /** @brief Initializes the checkpoint manager. */
    bool InitializeCheckpointManager();

    /** @brief Initializes the redo-log handler. */
    bool InitializeRedoLogHandler();

    /** @brief Destroys global configuration. */
    void DestroyConfiguration();

    /** @brief Destroys the statistics manager and all providers. */
    void DestroyStatistics();

    /** @brief Destroys the session manager. */
    void DestroySessionManager();

    /** @brief Destroys the table manager. */
    void DestroyTableManager();

    /** @brief Destroys the surrogate key manager. */
    void DestroySurrogateKeyManager();

    /** @brief Destroys the recovery manager. */
    void DestroyRecoveryManager();

    /** @brief Destroys the checkpoint manager. */
    void DestroyCheckpointManager();

    /** @brief Destroys the redo-log handler. */
    void DestroyRedoLogHandler();

    /** @var Specifies whether the engine was Initialized. */
    bool m_initialized;

    /** @var Specifies whether the engine is in recovery mode. */
    bool m_recovering;

    /** @var Specifies whether the engine is in recovery mode and performing final cleanups. */
    bool m_recoveringCleanup;

    /** @var Auxiliary structure to compute affinity for user sessions (when thread-pool is off). */
    Affinity m_sessionAffinity;

    /** @var Auxiliary structure to compute MOT task affinity. */
    Affinity m_taskAffinity;

    /** @var The commit sequence number handler (CSN). */
    ICSNManager* m_csnManager;

    /** @var The transaction id manager. */
    TransactionIdManager m_txnIdManager;

    /** @var Global flag for soft memory limit. */
    uint32_t m_softMemoryLimitReached;

    /** @var Keep next member variable offset aligned to 8-bytes. */
    uint32_t m_padding;

    /** @var The GC context. */
    GcContext m_gcContext;

    /** @var The session manager. */
    SessionManager* m_sessionManager;

    /** @var The table manager. */
    TableManager* m_tableManager;

    /** @var The surrogate key manager. */
    SurrogateKeyManager* m_surrogateKeyManager;

    /** @var The recovery manager. */
    IRecoveryManager* m_recoveryManager;

    /** @var The redo-log handler. */
    RedoLogHandler* m_redoLogHandler;

    /** @var The checkpoint manager. */
    CheckpointManager* m_checkpointManager;

    /** @var DDL event */
    DDLSigFunc m_ddlSigFunc;

    // record initialization failure point, so that Destroy can be called at any point of failure
    enum InitPhase {
        INIT_CFG_PHASE,
        LOAD_CFG_PHASE,
        INIT_CORE_SERVICES_PHASE,
        INIT_APP_SERVICES_PHASE,
        START_BG_TASKS_PHASE
    };
    stack<InitPhase> m_initStack;

    enum InitCorePhase {
        INIT_CORE_START,
        INIT_THREAD_ID_POOL_PHASE,
        INIT_CONNECTION_ID_POOL_PHASE,
        INIT_LOADER_THREAD_ID_PHASE,
        INIT_LOADER_NODE_ID_PHASE,
        INIT_STATISTICS_PHASE,
        INIT_MM_PHASE,
        INIT_SESSION_MANAGER_PHASE,
        INIT_TABLE_MANAGER_PHASE,
        INIT_SURROGATE_KEY_MANAGER_PHASE,
        INIT_GC_PHASE,
        INIT_DEBUG_UTILS,
        INIT_CSN_MANAGER,
        INIT_CORE_DONE
    };
    stack<InitCorePhase> m_initCoreStack;

    enum InitAppPhase {
        INIT_APP_START,
        INIT_REDO_LOG_HANDLER_PHASE,
        INIT_RECOVERY_MANAGER_PHASE,
        INIT_CHECKPOINT_MANAGER_PHASE,
        INIT_APP_DONE
    };
    stack<InitAppPhase> m_initAppStack;

    enum StartBgTaskPhase { START_STAT_PRINT_PHASE, START_BG_TASK_DONE };
    stack<StartBgTaskPhase> m_startBgStack;

    /**
     * @brief This function is a checker of OS policies to locate if something is potentially not configured properly.
     * System will proceed but print a message.
     */
    void CheckPolicies();

    // logger macro
    DECLARE_CLASS_LOGGER();
};

// global helpers
/** @brief Retrieves the affinity mode for user sessions. */
inline Affinity& GetSessionAffinity()
{
    return MOTEngine::GetInstance()->GetSessionAffinity();
}

/** @brief Retrieves the MOT task affinity mode. */
inline Affinity& GetTaskAffinity()
{
    return MOTEngine::GetInstance()->GetTaskAffinity();
}

/** @brief Retrieves the CSN manager. */
inline ICSNManager& GetCSNManager()
{
    return MOTEngine::GetInstance()->GetCSNManager();
}

/** @brief Retrieves the TXN Id manager. */
inline TransactionIdManager& GetTxnIdManager()
{
    return MOTEngine::GetInstance()->GetTxnIdManager();
}

/** @brief Retrieves the session manager. */
inline SessionManager* GetSessionManager()
{
    return MOTEngine::GetInstance()->GetSessionManager();
}

/** @brief Retrieves the table manager. */
inline TableManager* GetTableManager()
{
    return MOTEngine::GetInstance()->GetTableManager();
}

/** @brief Retrieves the surrogate key manager. */
inline SurrogateKeyManager* GetSurrogateKeyManager()
{
    return MOTEngine::GetInstance()->GetSurrogateKeyManager();
}

/** @brief Retrieves the recovery manager. */
inline IRecoveryManager* GetRecoveryManager()
{
    return MOTEngine::GetInstance()->GetRecoveryManager();
}

/** @brief Retrieves the checkpoint manager. */
inline CheckpointManager* GetCheckpointManager()
{
    return MOTEngine::GetInstance()->GetCheckpointManager();
}

/** @brief Retrieves the redo-log handler. */
inline RedoLogHandler* GetRedoLogHandler()
{
    return MOTEngine::GetInstance()->GetRedoLogHandler();
}
}  // namespace MOT

#endif /* MOT_ENGINE_H */
