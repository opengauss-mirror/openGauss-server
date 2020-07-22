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
 * mot_internal.cpp
 *    MOT Foreign Data Wrapper internal interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <ostream>
#include <istream>
#include <iomanip>

#include "postgres.h"

#include "access/dfs/dfs_query.h"
#include "access/sysattr.h"
#include "mot_internal.h"
#include "row.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "commands/dbcommands.h"
#include "knl/knl_session.h"

#include "log_statistics.h"
#include "spin_lock.h"

#include "txn.h"
#include "table.h"
#include "utilities.h"

#include "mot_engine.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "index_factory.h"
#include "column.h"
#include <pthread.h>
#include <cstring>

#include "mm_raw_chunk_store.h"
#include "ext_config_loader.h"
#include "config_manager.h"
#include "mot_error.h"
#include "utilities.h"
#include "jit_context.h"
#include "mm_cfg.h"
#include "jit_statistics.h"

#define MOT_MIN_MEMORY_USAGE_MB 128

#define IS_CHAR_TYPE(oid) (oid == VARCHAROID || oid == BPCHAROID || oid == TEXTOID || oid == CLOBOID || oid == BYTEAOID)
#define IS_INT_TYPE(oid)                                                                                           \
    (oid == BOOLOID || oid == CHAROID || oid == INT8OID || oid == INT2OID || oid == INT4OID || oid == FLOAT4OID || \
        oid == FLOAT8OID || oid == INT1OID || oid == DATEOID || oid == TIMEOID || oid == TIMESTAMPOID ||           \
        oid == TIMESTAMPTZOID)

#define FILL_KEY_NULL(toid, buf, len)                \
    {                                                \
        errno_t erc = memset_s(buf, len, 0x00, len); \
        securec_check(erc, "\0", "\0");              \
    }

#define FILL_KEY_MAX(toid, buf, len)                 \
    {                                                \
        errno_t erc = memset_s(buf, len, 0xff, len); \
        securec_check(erc, "\0", "\0");              \
    }

MOT::MOTEngine* MOTAdaptor::m_engine = nullptr;
static XLOGLogger xlogger;

static KEY_OPER keyOperStateMachine[KEY_OPER::READ_INVALID + 1][KEY_OPER::READ_INVALID];

// enable MOT Engine logging facilities
DECLARE_LOGGER(InternalExecutor, FDW)

/** @brief on_proc_exit() callback for cleaning up current thread - only when thread pool is ENABLED. */
static void MOTCleanupThread(int status, Datum ptr);

extern void MOTOnThreadShutdown();

// in a thread-pooled environment we need to ensure thread-locals are initialized properly
static inline void EnsureSafeThreadAccessInline()
{
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_DEBUG("Initializing safe thread access for current thread");
        MOT::AllocThreadId();
        // register for cleanup only once - not having a current thread id is the safe indicator we never registered
        // proc-exit callback for this thread
        if (g_instance.attr.attr_common.enable_thread_pool) {
            on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
            MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
        }
    }
    if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
        MOT::InitCurrentNumaNodeId();
    }
    MOT::InitMasstreeThreadinfo();
}

extern void EnsureSafeThreadAccess()
{
    EnsureSafeThreadAccessInline();
}

static void DestroySession(MOT::SessionContext* sessionContext)
{
    MOT_ASSERT(MOTAdaptor::m_engine);
    MOT_LOG_DEBUG("Destroying session context %p, connection_id %u", sessionContext, sessionContext->GetConnectionId());

    if (u_sess->mot_cxt.jit_session_context_pool) {
        JitExec::FreeSessionJitContextPool(u_sess->mot_cxt.jit_session_context_pool);
    }
    MOT::GetSessionManager()->DestroySessionContext(sessionContext);
}

// OA: Global map of PG session identification (required for session statistics)
// This approach is safer than saving information in the session context
static pthread_spinlock_t sessionDetailsLock;
typedef std::map<MOT::SessionId, pair<::ThreadId, pg_time_t>> SessionDetailsMap;
static SessionDetailsMap sessionDetailsMap;

static void InitSessionDetailsMap()
{
    pthread_spin_init(&sessionDetailsLock, 0);
}

static void DestroySessionDetailsMap()
{
    pthread_spin_destroy(&sessionDetailsLock);
}

static void RecordSessionDetails()
{
    MOT::SessionId sessionId = u_sess->mot_cxt.session_id;
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        sessionDetailsMap.emplace(sessionId, std::make_pair(t_thrd.proc->pid, t_thrd.proc->myStartTime));
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

static void ClearSessionDetails(MOT::SessionId sessionId)
{
    if (sessionId != INVALID_SESSION_ID) {
        pthread_spin_lock(&sessionDetailsLock);
        SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
        if (itr != sessionDetailsMap.end()) {
            sessionDetailsMap.erase(itr);
        }
        pthread_spin_unlock(&sessionDetailsLock);
    }
}

inline void ClearCurrentSessionDetails()
{
    ClearSessionDetails(u_sess->mot_cxt.session_id);
}

static void GetSessionDetails(MOT::SessionId sessionId, ::ThreadId* gaussSessionId, pg_time_t* sessionStartTime)
{
    // although we have the PGPROC in the user data of the session context, we prefer not to use
    // it due to safety (in some unknown constellation we might hold an invalid pointer)
    // it is much safer to save a copy of the two required fields
    pthread_spin_lock(&sessionDetailsLock);
    SessionDetailsMap::iterator itr = sessionDetailsMap.find(sessionId);
    if (itr != sessionDetailsMap.end()) {
        *gaussSessionId = itr->second.first;
        *sessionStartTime = itr->second.second;
    }
    pthread_spin_unlock(&sessionDetailsLock);
}

// provide safe session auto-cleanup in case of missing session closure
// This mechanism relies on the fact that when a session ends, eventually its thread is terminated
// ATTENTION: in thread-pooled envelopes this assumption no longer holds true, since the container thread keeps
// running after the session ends, and a session might run each time on a different thread, so we
// disable this feature, instead we use this mechanism to generate thread-ended event into the MM Engine
static pthread_key_t sessionCleanupKey;

static void SessionCleanup(void* key)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // in order to ensure session-id cleanup for session 0 we use positive values
    MOT::SessionId sessionId = (MOT::SessionId)(((uint64_t)key) - 1);
    if (sessionId != INVALID_SESSION_ID) {
        MOT_LOG_WARN("Encountered unclosed session %u (missing call to DestroyTxn()?)", (unsigned)sessionId);
        ClearSessionDetails(sessionId);
        if (MOTAdaptor::m_engine) {
            MOT::SessionContext* sessionContext = MOT::GetSessionManager()->GetSessionContext(sessionId);
            if (sessionContext != nullptr) {
                DestroySession(sessionContext);
            }
            // since a call to on_proc_exit(destroyTxn) was probably missing, we should also cleanup thread-locals
            // pay attention that if we got here it means the thread pool is disabled, so we must ensure thread-locals
            // are cleaned up right now. Due to these complexities, onCurrentThreadEnding() was designed to be proof
            // for repeated calls.
            MOTAdaptor::m_engine->OnCurrentThreadEnding();
        }
    }
}

static void InitSessionCleanup()
{
    pthread_key_create(&sessionCleanupKey, SessionCleanup);
}

static void DestroySessionCleanup()
{
    pthread_key_delete(sessionCleanupKey);
}

static void ScheduleSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, (const void*)(uint64_t)(u_sess->mot_cxt.session_id + 1));
}

static void CancelSessionCleanup()
{
    pthread_setspecific(sessionCleanupKey, nullptr);
}

// external configuration loader
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
        motCfg.OnConfigChange();  // trigger partial load

        // load any relevant GUC values here
        bool result = ConfigureMaxConnections();
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
            MOT_LOG_INFO("Configuring asynchronous redo-log handler due to synchronous_commit=off");
            result = AddExtTypedConfigItem<MOT::RedoLogHandlerType>(
                "", "redo_log_handler_type", MOT::RedoLogHandlerType::ASYNC_REDO_LOG_HANDLER);
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
        uint32_t globalMemoryMb = motCfg.m_globalMemoryMaxLimitMB;
        uint32_t localMemoryMb = motCfg.m_localMemoryMaxLimitMB;
        uint32_t sessionLargeStoreMb = motCfg.m_sessionLargeBufferStoreSizeMB;

        // compare the sum with max_process_memory and system total
        uint32_t maxReserveMemoryMb = globalMemoryMb + localMemoryMb + sessionLargeStoreMb;

        // if the total memory is less than the required minimum, then issue a warning, fix it and return
        if (maxReserveMemoryMb < MOT_MIN_MEMORY_USAGE_MB) {
            MOT_LOG_WARN("MOT memory limits are too low, adjusting values");
            // we use current total as zero to force session large store zero value
            result = ConfigureMemoryLimits(MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
            return result;
        }

        // get system total
        uint32_t systemTotalMemoryMb = MOT::GetTotalSystemMemoryMb();
        if (systemTotalMemoryMb == 0) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Cannot retrieve total system memory");
            return false;
        }

        // get envelope limit
        uint32_t processTotalMemoryMb = g_instance.attr.attr_memory.max_process_memory / KILO_BYTE;

        // compute the real limit
        uint32_t upperLimitMb = min(systemTotalMemoryMb, processTotalMemoryMb);

        // get dynamic gap we need to preserve between MOT and envelope
        uint32_t dynamicGapMb = MIN_DYNAMIC_PROCESS_MEMORY / KILO_BYTE;

        MOT_LOG_TRACE("Checking for memory limits: globalMemoryMb=%u, localMemoryMb=%u, sessionLargeStoreMb=%u, "
                      "systemTotalMemoryMb=%u, processTotalMemoryMb=%u, upperLimitMb=%u, dynamicGapMb=%u, "
                      "max_process_memory=%u",
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
            MOT_LOG_TRACE(
                "MOT engine maximum memory definitions (global: %u MB, local: %u MB, session large store: %u MB, "
                "total: "
                "%u MB) breach GaussDB maximum process memory restriction (%u MB) and/or total system memory (%u MB). "
                "MOT values shall be adjusted accordingly to preserve required gap (%u MB).",
                globalMemoryMb,
                localMemoryMb,
                sessionLargeStoreMb,
                maxReserveMemoryMb,
                processTotalMemoryMb,
                systemTotalMemoryMb,
                dynamicGapMb);

            // compute new total memory limit for MOT
            uint32_t newTotalMemoryMb = 0;
            if (upperLimitMb < dynamicGapMb) {
                // this can happen only if system memory is less than 2GB, we still allow minor breach
                MOT_LOG_WARN("Using minimal memory limits in MOT Engine due to system total memory restrictions");
                // we use current total as zero to force session large store zero value
                result = ConfigureMemoryLimits(MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
            } else {
                newTotalMemoryMb = upperLimitMb - dynamicGapMb;
                if (newTotalMemoryMb < MOT_MIN_MEMORY_USAGE_MB) {
                    // in extreme cases we allow a minor breach of the dynamic gap
                    MOT_LOG_TRACE("Using minimal memory limits in MOT Engine due to GaussDB memory usage restrictions");
                    // we use current total as zero to force session large store zero value
                    result = ConfigureMemoryLimits(MOT_MIN_MEMORY_USAGE_MB, 0, globalMemoryMb, localMemoryMb);
                } else {
                    MOT_LOG_TRACE("Adjusting memory limits in MOT Engine due to GaussDB memory usage restrictions");
                    result = ConfigureMemoryLimits(newTotalMemoryMb, maxReserveMemoryMb, globalMemoryMb, localMemoryMb);
                }
            }

            return result;
        }

        return result;
    }

    bool ConfigureMemoryLimits(uint32_t newTotalMemoryMb, uint32_t currentTotalMemoryMb, uint32_t currentGlobalMemoryMb,
        uint32_t currentLocalMemoryMb)
    {
        uint32_t newGlobalMemoryMb = 0;
        uint32_t newLocalMemoryMb = 0;
        uint32_t newSessionLargeStoreMemoryMb = 0;

        // compute new configuration values
        if (currentTotalMemoryMb > 0) {
            // we preserve the existing ratio between global and local memory, but reduce the total sum as required
            double ratio = ((double)newTotalMemoryMb) / ((double)currentTotalMemoryMb);
            newGlobalMemoryMb = (uint32_t)(currentGlobalMemoryMb * ratio);
            if (MOT::GetGlobalConfiguration().m_sessionLargeBufferStoreSizeMB > 0) {
                newLocalMemoryMb = (uint32_t)(currentLocalMemoryMb * ratio);
                newSessionLargeStoreMemoryMb = newTotalMemoryMb - newGlobalMemoryMb - newLocalMemoryMb;
            } else {
                // if the user configured zero for the session large store, then we want to keep it this way
                newSessionLargeStoreMemoryMb = 0;
                newLocalMemoryMb = newTotalMemoryMb - newGlobalMemoryMb;
            }
        } else {
            // when current total memory is zero we split the new total between global and local in ratio of 4:1
            newGlobalMemoryMb = (uint32_t)(newTotalMemoryMb * 0.8f);  // 80% to global memory
            newLocalMemoryMb = newTotalMemoryMb - newGlobalMemoryMb;  // 20% to local memory
            //  session large store remains zero!
        }

        MOT_LOG_WARN(
            "Adjusting MOT memory limits: global = %u MB, local = %u MB, session large store = %u MB, total = %u MB",
            newGlobalMemoryMb,
            newLocalMemoryMb,
            newSessionLargeStoreMemoryMb,
            newTotalMemoryMb);

        // stream into MOT new definitions
        MOT::mot_string memCfg;
        memCfg.format("%u MB", newGlobalMemoryMb);
        bool result = AddExtStringConfigItem("", "max_mot_global_memory", memCfg.c_str());
        if (result) {
            memCfg.format("%u MB", newLocalMemoryMb);
            result = AddExtStringConfigItem("", "max_mot_local_memory", memCfg.c_str());
        }
        if (result) {
            memCfg.format("%u MB", newSessionLargeStoreMemoryMb);
            result = AddExtStringConfigItem("", "session_large_buffer_store_size", memCfg.c_str());
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

static GaussdbConfigLoader* gaussdbConfigLoader = nullptr;

// Error code mapping array from MM to PG
static const MotErrToPGErrSt MM_ERRCODE_TO_PG[] = {
    // RC_OK
    {ERRCODE_SUCCESSFUL_COMPLETION, "Success", nullptr},
    // RC_ERROR
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_ABORT
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_UNSUPPORTED_COL_TYPE
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column type %s is not supported yet"},
    // RC_UNSUPPORTED_COL_TYPE_ARR
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column type Array of %s is not supported yet"},
    // RC_EXCEEDS_MAX_ROW_SIZE
    {ERRCODE_FEATURE_NOT_SUPPORTED,
        "Column definition of %s is not supported",
        "Column size %d exceeds max tuple size %u"},
    // RC_COL_NAME_EXCEEDS_MAX_SIZE
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column name %s exceeds max name size %u"},
    // RC_COL_SIZE_INVLALID
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column size %d exceeds max size %u"},
    // RC_TABLE_EXCEEDS_MAX_DECLARED_COLS
    {ERRCODE_FEATURE_NOT_SUPPORTED, "Can't create table", "Can't add column %s, number of declared columns is less"},
    // RC_INDEX_EXCEEDS_MAX_SIZE
    {ERRCODE_FDW_KEY_SIZE_EXCEEDS_MAX_ALLOWED,
        "Can't create index",
        "Total columns size is greater than maximum index size %u"},
    // RC_TABLE_EXCEEDS_MAX_INDEXES,
    {ERRCODE_FDW_TOO_MANY_INDEXES,
        "Can't create index",
        "Total number of indexes for table %s is greater than the maximum number if indexes allowed %u"},
    // RC_TXN_EXCEEDS_MAX_DDLS,
    {ERRCODE_FDW_TOO_MANY_DDL_CHANGES_IN_TRANSACTION_NOT_ALLOWED,
        "Cannot execute statement",
        "Maximum number of DDLs per transactions reached the maximum %u"},
    // RC_UNIQUE_VIOLATION
    {ERRCODE_UNIQUE_VIOLATION, "duplicate key value violates unique constraint \"%s\"", "Key %s already exists."},
    // RC_TABLE_NOT_FOUND
    {ERRCODE_UNDEFINED_TABLE, "Table \"%s\" doesn't exist", nullptr},
    // RC_INDEX_NOT_FOUND
    {ERRCODE_UNDEFINED_TABLE, "Index \"%s\" doesn't exist", nullptr},
    // RC_LOCAL_ROW_FOUND
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_NOT_FOUND
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_DELETED
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INSERT_ON_EXIST
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INDEX_RETRY_INSERT
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INDEX_DELETE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_NOT_VISIBLE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_MEMORY_ALLOCATION_ERROR
    {ERRCODE_OUT_OF_LOGICAL_MEMORY, "Memory is temporarily unavailable", nullptr},
    // RC_ILLEGAL_ROW_STATE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_NULL_VOILATION
    {ERRCODE_FDW_ERROR,
        "Null constraint violated",
        "NULL value cannot be inserted into non-null column %s at table %s"},
    // RC_PANIC
    {ERRCODE_FDW_ERROR, "Critical error", "Critical error: %s"},
    // RC_NA
    {ERRCODE_FDW_OPERATION_NOT_SUPPORTED, "A checkpoint is in progress - cannot truncate table.", nullptr},
    // RC_MAX_VALUE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr}};

static_assert(sizeof(MM_ERRCODE_TO_PG) / sizeof(MotErrToPGErrSt) == MOT::RC_MAX_VALUE + 1,
    "Not all MM engine error codes (RC) is mapped to PG error codes");

void report_pg_error(MOT::RC rc, MOT::TxnManager* txn, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5)
{
    const MotErrToPGErrSt* err = &MM_ERRCODE_TO_PG[rc];

    switch (rc) {
        case MOT::RC_OK:
            break;
        case MOT::RC_ERROR:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_ABORT:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_UNSUPPORTED_COL_TYPE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, NameListToString(col->typname->names))));
            break;
        }
        case MOT::RC_UNSUPPORTED_COL_TYPE_ARR: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, strVal(llast(col->typname->names)))));
            break;
        }
        case MOT::RC_COL_NAME_EXCEEDS_MAX_SIZE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, col->colname, (uint32_t)MOT::Column::MAX_COLUMN_NAME_LEN)));
            break;
        }
        case MOT::RC_COL_SIZE_INVLALID: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, (uint32_t)(uint64_t)arg2, (uint32_t)MAX_VARCHAR_LEN)));
            break;
        }
        case MOT::RC_EXCEEDS_MAX_ROW_SIZE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, (uint32_t)(uint64_t)arg2, (uint32_t)MAX_TUPLE_SIZE)));
            break;
        }
        case MOT::RC_TABLE_EXCEEDS_MAX_DECLARED_COLS: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, col->colname)));
            break;
        }
        case MOT::RC_INDEX_EXCEEDS_MAX_SIZE:
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, MAX_KEY_SIZE)));
            break;
        case MOT::RC_TABLE_EXCEEDS_MAX_INDEXES:
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, ((MOT::Table*)arg1)->GetTableName(), MAX_NUM_INDEXES)));
            break;
        case MOT::RC_TXN_EXCEEDS_MAX_DDLS:
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, MAX_DDL_ACCESS_SIZE)));
            break;
        case MOT::RC_UNIQUE_VIOLATION:
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, (char*)arg1),
                    errdetail(err->m_detail, (char*)arg2)));
            break;

        case MOT::RC_TABLE_NOT_FOUND:
        case MOT::RC_INDEX_NOT_FOUND:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg(err->m_msg, (char*)arg1)));
            break;

            // following errors are internal and should not get to an upper layer
        case MOT::RC_LOCAL_ROW_FOUND:
        case MOT::RC_LOCAL_ROW_NOT_FOUND:
        case MOT::RC_LOCAL_ROW_DELETED:
        case MOT::RC_INSERT_ON_EXIST:
        case MOT::RC_INDEX_RETRY_INSERT:
        case MOT::RC_INDEX_DELETE:
        case MOT::RC_LOCAL_ROW_NOT_VISIBLE:
        case MOT::RC_ILLEGAL_ROW_STATE:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_MEMORY_ALLOCATION_ERROR:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_NULL_VIOLATION: {
            ColumnDef* col = (ColumnDef*)arg1;
            MOT::Table* table = (MOT::Table*)arg2;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, col->colname, table->GetLongTableName().c_str())));
            break;
        }
        case MOT::RC_PANIC: {
            char* msg = (char*)arg1;
            ereport(FATAL,
                (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg), errdetail(err->m_detail, msg)));
            break;
        }
        case MOT::RC_NA:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_MAX_VALUE:
        default:
            ereport(ERROR, (errmodule(MOD_MM), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
    }
}

bool MOTAdaptor::m_initialized = false;
bool MOTAdaptor::m_callbacks_initialized = false;

static void WakeupWalWriter()
{
    if (g_instance.proc_base->walwriterLatch != nullptr) {
        SetLatch(g_instance.proc_base->walwriterLatch);
    }
}

void MOTAdaptor::Init()
{
    if (m_initialized) {
        return;
    }

    MOT::GetGlobalConfiguration().SetTotalMemoryMb(g_instance.attr.attr_memory.max_process_memory / KILO_BYTE);

    m_engine = MOT::MOTEngine::CreateInstanceNoInit(g_instance.attr.attr_common.MOTConfigFileName, 0, nullptr);
    if (m_engine == nullptr) {
        elog(FATAL, "Failed to create MOT engine");
    }

    gaussdbConfigLoader = new (std::nothrow) GaussdbConfigLoader();
    if (gaussdbConfigLoader == nullptr) {
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to allocate memory for GaussDB/MOTEngine configuration loader.");
    }
    MOT_LOG_TRACE("Adding external configuration loader for GaussDB");
    if (!m_engine->AddConfigLoader(gaussdbConfigLoader)) {
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to add GaussDB/MOTEngine configuration loader");
    }

    if (!m_engine->LoadConfig()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to load configuration for MOT engine.");
    }

    // check max process memory here - we do it anyway to protect ourselves from miscalculations
    uint32_t globalMemoryKb = MOT::g_memGlobalCfg.m_maxGlobalMemoryMb * KILO_BYTE;
    uint32_t localMemoryKb = MOT::g_memGlobalCfg.m_maxLocalMemoryMb * KILO_BYTE;
    uint32_t maxReserveMemoryKb = globalMemoryKb + localMemoryKb;

    if ((g_instance.attr.attr_memory.max_process_memory < (int32)maxReserveMemoryKb) ||
        ((g_instance.attr.attr_memory.max_process_memory - maxReserveMemoryKb) < MIN_DYNAMIC_PROCESS_MEMORY)) {
        // we allow one extreme case: GaussDB is configured to its limit, and zero memory is left for us
        if ((g_instance.attr.attr_memory.max_process_memory == MIN_DYNAMIC_PROCESS_MEMORY) &&
            (maxReserveMemoryKb <= MOT_MIN_MEMORY_USAGE_MB * KILO_BYTE)) {
            MOT_LOG_INFO("Allowing MOT to work in minimal memory mode");
        } else {
            m_engine->RemoveConfigLoader(gaussdbConfigLoader);
            delete gaussdbConfigLoader;
            gaussdbConfigLoader = nullptr;
            MOT::MOTEngine::DestroyInstance();
            elog(FATAL,
                "The value of pre-reserved memory for MOT engine is not reasonable: "
                "Request for a maximum of %u KB global memory, and %u KB session memory (total of %u KB) "
                "is invalid since max_process_memory is %u KB",
                globalMemoryKb,
                localMemoryKb,
                maxReserveMemoryKb,
                g_instance.attr.attr_memory.max_process_memory);
        }
    }

    if (!m_engine->Initialize()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize MOT engine.");
    }

    if (!JitExec::JitStatisticsProvider::CreateInstance()) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize JIT statistics.");
    }

    // make sure current thread is cleaned up properly when thread pool is enabled
    EnsureSafeThreadAccessInline();

    if (MOT::GetGlobalConfiguration().m_enableRedoLog &&
        MOT::GetGlobalConfiguration().m_loggerType == MOT::LoggerType::EXTERNAL_LOGGER) {
        m_engine->GetRedoLogHandler()->SetLogger(&xlogger);
        m_engine->GetRedoLogHandler()->SetWalWakeupFunc(WakeupWalWriter);
    }

    InitSessionDetailsMap();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        InitSessionCleanup();
    }
    InitDataNodeId();
    // fill key operation matrix
    for (uint8_t i = 0; i <= KEY_OPER::READ_INVALID; i++) {
        for (uint8_t j = 0; j < KEY_OPER::READ_INVALID; j++) {
            switch ((KEY_OPER)i) {
                case KEY_OPER::READ_KEY_EXACT:  // = : allows all operations
                    keyOperStateMachine[i][j] = (KEY_OPER)j;
                    break;
                case KEY_OPER::READ_KEY_OR_NEXT:  // >= : allows =, >, >=, like
                    keyOperStateMachine[i][j] =
                        (KEY_OPER)(((KEY_OPER)j) < KEY_OPER::READ_KEY_OR_PREV ? j : KEY_OPER::READ_INVALID);
                    break;
                case KEY_OPER::READ_KEY_AFTER:  // > : allows nothing
                    keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
                case KEY_OPER::READ_KEY_OR_PREV:  // <= : allows =, <, <=, like
                {
                    switch ((KEY_OPER)j) {
                        case KEY_OPER::READ_KEY_EXACT:
                        case KEY_OPER::READ_KEY_LIKE:
                        case KEY_OPER::READ_KEY_OR_PREV:
                        case KEY_OPER::READ_KEY_BEFORE:
                            keyOperStateMachine[i][j] = (KEY_OPER)j;
                            break;
                        default:
                            keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                            break;
                    }
                    break;
                }
                case KEY_OPER::READ_KEY_BEFORE:  // < : allows nothing
                    keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;

                case KEY_OPER::READ_KEY_LIKE:  // like: allows nothing
                    keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;

                case KEY_OPER::READ_INVALID:  // = : allows all operations
                    keyOperStateMachine[i][j] = (KEY_OPER)j;
                    break;

                default:
                    keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
            }
        }
    }
    m_initialized = true;
}

void MOTAdaptor::NotifyConfigChange()
{
    if (gaussdbConfigLoader != nullptr) {
        gaussdbConfigLoader->MarkChanged();
    }
}

void MOTAdaptor::InitDataNodeId()
{
    MOT::GetGlobalConfiguration().SetPgNodes(1, 1);
}

void MOTAdaptor::Fini()
{
    if (!m_initialized) {
        return;
    }

    JitExec::JitStatisticsProvider::DestroyInstance();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        DestroySessionCleanup();
    }
    DestroySessionDetailsMap();
    if (gaussdbConfigLoader != nullptr) {
        m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
    }

    EnsureSafeThreadAccessInline();
    MOT::MOTEngine::DestroyInstance();
    m_engine = nullptr;
    knl_thread_mot_init();  // reset all thread-locals, mandatory for standby switch-over
    m_initialized = false;
}

MOT::TxnManager* MOTAdaptor::InitTxnManager(MOT::ConnectionId connection_id /* = INVALID_CONNECTION_ID */)
{
    if (!u_sess->mot_cxt.txn_manager) {
        bool attachCleanFunc =
            (MOTCurrThreadId == INVALID_THREAD_ID ? true : !g_instance.attr.attr_common.enable_thread_pool);

        // First time we handle this connection
        if (m_engine == nullptr) {
            elog(ERROR, "initTxnManager: MOT engine is not initialized");
            return nullptr;
        }

        // create new session context
        MOT::SessionContext* session_ctx =
            MOT::GetSessionManager()->CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, connection_id);
        if (session_ctx == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Session Initialization", "Failed to create session context");
            ereport(FATAL, (errmsg("Session startup: failed to create session context.")));
            return nullptr;
        }
        MOT_ASSERT(u_sess->mot_cxt.session_context == session_ctx);
        MOT_ASSERT(u_sess->mot_cxt.session_id == session_ctx->GetSessionId());
        MOT_ASSERT(u_sess->mot_cxt.connection_id == session_ctx->GetConnectionId());

        // make sure we cleanup leftovers from other session
        u_sess->mot_cxt.jit_context_count = 0;

        // record session details for statistics report
        RecordSessionDetails();

        if (attachCleanFunc) {
            // schedule session cleanup when thread pool is not used
            if (!g_instance.attr.attr_common.enable_thread_pool) {
                on_proc_exit(DestroyTxn, PointerGetDatum(session_ctx));
                ScheduleSessionCleanup();
            } else {
                on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
                MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
            }
        }

        u_sess->mot_cxt.txn_manager = session_ctx->GetTxnManager();
        elog(DEBUG1, "Init TXN_MAN for thread %u", MOTCurrThreadId);
    }

    return u_sess->mot_cxt.txn_manager;
}

/** @brief Notification from thread pool that a session ended (only when thread pool is ENABLED). */
extern void MOTOnSessionClose()
{
    MOT_LOG_TRACE("Received session close notification (current session id: %u, current connection id: %u)",
        u_sess->mot_cxt.session_id,
        u_sess->mot_cxt.connection_id);
    if (u_sess->mot_cxt.session_id != INVALID_SESSION_ID) {
        ClearCurrentSessionDetails();
        if (!MOTAdaptor::m_engine) {
            MOT_LOG_ERROR("MOTOnSessionClose(): MOT engine is not initialized");
        } else {
            EnsureSafeThreadAccessInline();  // this is ok, it wil be cleaned up when thread exits
            MOT::SessionContext* sessionContext = u_sess->mot_cxt.session_context;
            if (sessionContext == nullptr) {
                MOT_LOG_WARN("Received session close notification, but no current session is found. Current session id "
                             "is %u. Request ignored.",
                    u_sess->mot_cxt.session_id);
            } else {
                DestroySession(sessionContext);
                MOT_ASSERT(u_sess->mot_cxt.session_id == INVALID_SESSION_ID);
            }
        }
    }
}

/** @brief Notification from thread pool that a pooled thread ended (only when thread pool is ENABLED). */
extern void MOTOnThreadShutdown()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    MOT_LOG_DEBUG("Received thread shutdown notification");
    if (!MOTAdaptor::m_engine) {
        MOT_LOG_ERROR("MOTOnThreadShutdown(): MOT engine is not initialized");
    } else {
        MOTAdaptor::m_engine->OnCurrentThreadEnding();
    }
    knl_thread_mot_init();  // reset all thread locals
}

/**
 * @brief on_proc_exit() callback to handle thread-cleanup - regardless of whether thread pool is enabled or not.
 * registration to on_proc_exit() is triggered by first call to EnsureSafeThreadAccessInline().
 */
static void MOTCleanupThread(int status, Datum ptr)
{
    MOT_ASSERT(g_instance.attr.attr_common.enable_thread_pool);

    // when thread pool is used we just cleanup current thread
    // this might be a duplicate because thread pool also calls MOTOnThreadShutdown() - this is still ok
    // because we guard against repeated calls in MOTEngine::onCurrentThreadEnding()
    MOTOnThreadShutdown();
}

void MOTAdaptor::DestroyTxn(int status, Datum ptr)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // cleanup session
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        CancelSessionCleanup();
    }
    ClearCurrentSessionDetails();
    MOT::SessionContext* session = (MOT::SessionContext*)DatumGetPointer(ptr);
    if (m_engine == nullptr) {
        elog(ERROR, "destroyTxn: MOT engine is not initialized");
    }

    if (session != MOT_GET_CURRENT_SESSION_CONTEXT()) {
        MOT_LOG_WARN("Ignoring request to delete session context: already deleted");
    } else if (session != nullptr) {
        elog(DEBUG1, "Destroy SessionContext, connection_id = %u \n", session->GetConnectionId());
        EnsureSafeThreadAccessInline();  // may be accessed from new thread pool worker
        MOT::GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
        if (gc != nullptr) {
            gc->GcEndTxn();
        }
        MOT::GetSessionManager()->DestroySessionContext(session);
    }

    // clean up thread
    MOTOnThreadShutdown();
}

MOT::RC MOTAdaptor::Commit(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit(tid);
    } else {
        return txn->LiteCommit(tid);
    }
}

MOT::RC MOTAdaptor::EndTransaction(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->EndTransaction();
    } else {
        // currently no call to end_transaction on coordinator
        return MOT::RC_OK;
    }
}

MOT::RC MOTAdaptor::Rollback(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->Rollback(tid);
    } else {
        return txn->LiteRollback(tid);
    }
}

MOT::RC MOTAdaptor::Prepare(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->Prepare(tid);
    } else {
        return txn->LitePrepare(tid);
    }
}

MOT::RC MOTAdaptor::CommitPrepared(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->CommitPrepared(tid);
    } else {
        return txn->LiteCommitPrepared(tid);
    }
}

MOT::RC MOTAdaptor::RollbackPrepared(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    if (!IS_PGXC_COORDINATOR) {
        return txn->RollbackPrepared(tid);
    } else {
        return txn->LiteRollbackPrepared(tid);
    }
}

MOT::RC MOTAdaptor::FailedCommitPrepared(::TransactionId tid)
{
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    return txn->FailedCommitPrepared(tid);
}

MOT::RC MOTAdaptor::InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    uint8_t* newRowData = nullptr;
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::Table* table = fdwState->m_table;
    MOT::Row* row = table->CreateNewRow();
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to create new row for table %s", table->GetLongTableName().c_str());
        return MOT::RC_MEMORY_ALLOCATION_ERROR;
    }
    newRowData = const_cast<uint8_t*>(row->GetData());
    PackRow(slot, table, fdwState->m_attrsUsed, newRowData);

    MOT::RC res = table->InsertRow(row, fdwState->m_currTxn);
    if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
    }
    return res;
}

MOT::RC MOTAdaptor::UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    MOT::RC rc;

    do {
        fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
        rc = fdwState->m_currTxn->UpdateLastRowState(MOT::AccessType::WR);
        if (rc != MOT::RC::RC_OK) {
            break;
        }
        uint8_t* rowData = const_cast<uint8_t*>(fdwState->m_currRow->GetData());
        PackUpdateRow(slot, fdwState->m_table, fdwState->m_attrsModified, rowData);
        MOT::BitmapSet modified_columns(fdwState->m_attrsModified, fdwState->m_table->GetFieldCount() - 1);

        rc = fdwState->m_currTxn->OverwriteRow(fdwState->m_currRow, modified_columns);
    } while (0);

    return rc;
}

MOT::RC MOTAdaptor::DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    EnsureSafeThreadAccessInline();
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::RC rc = fdwState->m_currTxn->DeleteLastRow();
    return rc;
}

// NOTE: colId starts from 1
bool MOTAdaptor::SetMatchingExpr(
    MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr, Expr* parent, bool set_local)
{
    bool res = false;
    MOT::TxnManager* txn = GetSafeTxn();
    uint16_t numIx = state->m_table->GetNumIndexes();

    for (uint16_t i = 0; i < numIx; i++) {
        // MOT::Index *ix = state->table->getIndex(i);
        MOT::Index* ix = txn->GetIndex(state->m_table, i);
        if (ix != nullptr && ix->IsFieldPresent(colId)) {
            if (marr->m_idx[i] == nullptr) {
                marr->m_idx[i] = (MatchIndex*)palloc0(sizeof(MatchIndex));
                marr->m_idx[i]->Init();
                marr->m_idx[i]->m_ix = ix;
            }

            res |= marr->m_idx[i]->SetIndexColumn(state, colId, op, expr, parent, set_local);
        }
    }

    return res;
}

inline int32_t MOTAdaptor::AddParam(List** params, Expr* expr)
{
    int32_t index = 0;
    ListCell* cell = nullptr;

    foreach (cell, *params) {
        ++index;
        if (equal(expr, (Node*)lfirst(cell)))
            break;
    }
    if (cell == nullptr) {
        /* add the parameter to the list */
        ++index;
        *params = lappend(*params, expr);
    }

    return index;
}

MatchIndex* MOTAdaptor::GetBestMatchIndex(MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal)
{
    MOT::TxnManager* txn = GetSafeTxn();
    MatchIndex* best = nullptr;
    double bestCost = INT_MAX;
    uint16_t numIx = festate->m_table->GetNumIndexes();
    uint16_t bestI = (uint16_t)-1;

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr && marr->m_idx[i]->IsUsable()) {
            double cost = marr->m_idx[i]->GetCost(numClauses);
            if (cost < bestCost) {
                if (bestI < MAX_NUM_INDEXES) {
                    if (marr->m_idx[i]->GetNumMatchedCols() < marr->m_idx[bestI]->GetNumMatchedCols())
                        continue;
                }
                bestCost = cost;
                bestI = i;
            }
        }
    }

    if (bestI < MAX_NUM_INDEXES) {
        best = marr->m_idx[bestI];
        for (int k = 0; k < 2; k++) {
            for (int j = 0; j < best->m_ix->GetNumFields(); j++) {
                if (best->m_colMatch[k][j]) {
                    if (best->m_opers[k][j] < KEY_OPER::READ_INVALID) {
                        best->m_params[k][j] = AddParam(&best->m_remoteConds, best->m_colMatch[k][j]);
                        if (!list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                            best->m_remoteCondsOrig = lappend(best->m_remoteCondsOrig, best->m_parentColMatch[k][j]);
                        }

                        if (j > 0 && best->m_opers[k][j - 1] != KEY_OPER::READ_KEY_EXACT &&
                            !list_member(festate->m_localConds, best->m_parentColMatch[k][j])) {
                            if (setLocal)
                                festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        }
                    } else if (!list_member(festate->m_localConds, best->m_parentColMatch[k][j]) &&
                               !list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                        if (setLocal)
                            festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        best->m_colMatch[k][j] = nullptr;
                        best->m_parentColMatch[k][j] = nullptr;
                    }
                }
            }
        }
    }

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr) {
            MatchIndex* mix = marr->m_idx[i];
            if (i != bestI) {
                if (setLocal) {
                    for (int k = 0; k < 2; k++) {
                        for (int j = 0; j < mix->m_ix->GetNumFields(); j++) {
                            if (mix->m_colMatch[k][j] &&
                                !list_member(festate->m_localConds, mix->m_parentColMatch[k][j]) &&
                                !(best != nullptr &&
                                    list_member(best->m_remoteCondsOrig, mix->m_parentColMatch[k][j]))) {
                                festate->m_localConds = lappend(festate->m_localConds, mix->m_parentColMatch[k][j]);
                            }
                        }
                    }
                }
                pfree(mix);
                marr->m_idx[i] = 0;
            }
        }
    }
    if (best != nullptr && best->m_ix != nullptr) {
        for (uint16_t i = 0; i < numIx; i++) {
            if (best->m_ix == txn->GetIndex(festate->m_table, i)) {
                best->m_ixPosition = i;
                break;
            }
        }
    }

    return best;
}

void MOTAdaptor::OpenCursor(Relation rel, MOTFdwStateSt* festate)
{
    bool matchKey = true;
    bool forwardDirection = true;
    bool found = false;

    EnsureSafeThreadAccessInline();

    // GetTableByExternalId cannot return nullptr at this stage, because it is protected by envelope's table lock.
    festate->m_table = festate->m_currTxn->GetTableByExternalId(rel->rd_id);

    do {
        // this scan all keys case
        // we need to open both cursors on start and end to prevent
        // infinite scan in case "insert into table A ... as select * from table A ...
        if (festate->m_bestIx == nullptr) {
            int fIx, bIx;
            uint8_t* buf = nullptr;
            // assumption that primary index cannot be changed, can take it from
            // table and not look on ddl_access
            MOT::Index* ix = festate->m_table->GetPrimaryIndex();
            uint16_t keyLength = ix->GetKeyLength();

            if (festate->m_order == SORTDIR_ENUM::SORTDIR_ASC) {
                fIx = 0;
                bIx = 1;
                festate->m_forwardDirectionScan = true;
            } else {
                fIx = 1;
                bIx = 0;
                festate->m_forwardDirectionScan = false;
            }

            festate->m_cursor[fIx] = festate->m_table->Begin(festate->m_currTxn->GetThdId());

            festate->m_stateKey[bIx].InitKey(keyLength);
            buf = festate->m_stateKey[bIx].GetKeyBuf();
            FILL_KEY_MAX(INT8OID, buf, keyLength);
            festate->m_cursor[bIx] =
                ix->Search(&festate->m_stateKey[bIx], false, false, festate->m_currTxn->GetThdId(), found);
            break;
        }

        for (int i = 0; i < 2; i++) {
            if (i == 1 && festate->m_bestIx->m_end < 0) {
                if (festate->m_forwardDirectionScan) {
                    uint8_t* buf = nullptr;
                    MOT::Index* ix = festate->m_bestIx->m_ix;
                    uint16_t keyLength = ix->GetKeyLength();

                    festate->m_stateKey[1].InitKey(keyLength);
                    buf = festate->m_stateKey[1].GetKeyBuf();
                    FILL_KEY_MAX(INT8OID, buf, keyLength);
                    festate->m_cursor[1] =
                        ix->Search(&festate->m_stateKey[1], false, false, festate->m_currTxn->GetThdId(), found);
                } else {
                    festate->m_cursor[1] = festate->m_bestIx->m_ix->Begin(festate->m_currTxn->GetThdId());
                }
                break;
            }

            KEY_OPER oper = (i == 0 ? festate->m_bestIx->m_ixOpers[0] : festate->m_bestIx->m_ixOpers[1]);

            forwardDirection = ((oper & ~KEY_OPER_PREFIX_BITMASK) < KEY_OPER::READ_KEY_OR_PREV);

            CreateKeyBuffer(rel, festate, i);

            if (i == 0) {
                festate->m_forwardDirectionScan = forwardDirection;
            }

            switch (oper) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_PREFIX_LIKE:
                case KEY_OPER::READ_PREFIX:
                case KEY_OPER::READ_PREFIX_OR_NEXT:
                    matchKey = true;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_AFTER:
                case KEY_OPER::READ_PREFIX_AFTER:
                    matchKey = false;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_PREFIX_OR_PREV:
                    matchKey = true;
                    forwardDirection = false;
                    break;

                case KEY_OPER::READ_KEY_BEFORE:
                case KEY_OPER::READ_PREFIX_BEFORE:
                    matchKey = false;
                    forwardDirection = false;
                    break;

                default:
                    elog(INFO, "Invalid key operation: %u", oper);
                    break;
            }

            festate->m_cursor[i] = festate->m_bestIx->m_ix->Search(
                &festate->m_stateKey[i], matchKey, forwardDirection, festate->m_currTxn->GetThdId(), found);

            if (!found && oper == KEY_OPER::READ_KEY_EXACT && festate->m_bestIx->m_ix->GetUnique()) {
                festate->m_cursor[i]->Invalidate();
                festate->m_cursor[i]->Destroy();
                delete festate->m_cursor[i];
                festate->m_cursor[i] = nullptr;
            }
        }
    } while (0);
}

static MOT::RC TableFieldType(const ColumnDef* colDef, MOT::MOT_CATALOG_FIELD_TYPES& type, int16* typeLen, bool& isBlob)
{
    MOT::RC res = MOT::RC_OK;
    Oid typoid;
    Type tup;
    Form_pg_type typeDesc;
    int32_t colLen;

    if (colDef->typname->arrayBounds != nullptr)
        return MOT::RC_UNSUPPORTED_COL_TYPE_ARR;

    tup = typenameType(nullptr, colDef->typname, &colLen);
    typeDesc = ((Form_pg_type)GETSTRUCT(tup));
    typoid = HeapTupleGetOid(tup);
    *typeLen = typeDesc->typlen;

    if (*typeLen < 0) {
        *typeLen = colLen;
        switch (typeDesc->typstorage) {
            case 'p':
                break;
            case 'x':
            case 'm':
                if (typoid == NUMERICOID) {
                    *typeLen = DECIMAL_MAX_SIZE;
                    break;
                }
                /* fall through */
            case 'e':
#ifdef USE_ASSERT_CHECKING
                if (typoid == TEXTOID)
                    *typeLen = colLen = MAX_VARCHAR_LEN;
#endif
                if (colLen > MAX_VARCHAR_LEN || colLen < 0) {
                    res = MOT::RC_COL_SIZE_INVLALID;
                } else {
                    isBlob = true;
                }
                break;
            default:
                break;
        }
    }

    switch (typoid) {
        case CHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR;
            break;
        case INT1OID:
        case BOOLOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINY;
            break;
        case INT2OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT;
            break;
        case INT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INT;
            break;
        case INT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
            break;
        case DATEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DATE;
            break;
        case TIMEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIME;
            break;
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMP;
            break;
        case INTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INTERVAL;
            break;
        case TINTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINTERVAL;
            break;
        case TIMETZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMETZ;
            break;
        case FLOAT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_FLOAT;
            break;
        case FLOAT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE;
            break;
        case NUMERICOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL;
            break;
        case VARCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case BPCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case TEXTOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case CLOBOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB;
            break;
        case BYTEAOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        default:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
            res = MOT::RC_UNSUPPORTED_COL_TYPE;
    }

    if (tup)
        ReleaseSysCache(tup);

    return res;
}

MOT::RC MOTAdaptor::CreateIndex(IndexStmt* index, ::TransactionId tid)
{
    MOT::RC res;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    txn->SetTransactionId(tid);
    MOT::Table* table = txn->GetTableByExternalId(index->relation->foreignOid);

    if (table == nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MM),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table not found for oid %u", index->relation->foreignOid)));
        return MOT::RC_ERROR;
    }

    if (table->GetNumIndexes() == MAX_NUM_INDEXES) {
        ereport(ERROR,
            (errmodule(MOD_MM),
                errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
        return MOT::RC_ERROR;
    }

    elog(LOG,
        "creating %s index %s (OID: %u), for table: %s",
        (index->primary ? "PRIMARY" : "SECONDARY"),
        index->idxname,
        index->indexOid,
        index->relation->relname);
    uint64_t keyLength = 0;
    MOT::Index* ix = nullptr;
    MOT::IndexOrder index_order = MOT::IndexOrder::INDEX_ORDER_SECONDARY;
    MOT::IndexingMethod indexing_method;
    MOT::IndexTreeFlavor flavor;

    if (strcmp(index->accessMethod, "btree") == 0) {
        // Use the default index tree flavor from configuration file
        indexing_method = MOT::IndexingMethod::INDEXING_METHOD_TREE;
        flavor = MOT::GetGlobalConfiguration().m_indexTreeFlavor;
    } else {
        ereport(ERROR, (errmodule(MOD_MM), errmsg("MOT supports indexes of type BTREE only (btree or btree_art)")));
        return MOT::RC_ERROR;
    }

    if (list_length(index->indexParams) > (int)MAX_KEY_COLUMNS) {
        ereport(ERROR,
            (errmodule(MOD_MM),
                errcode(ERRCODE_FDW_TOO_MANY_INDEX_COLUMNS),
                errmsg("Can't create index"),
                errdetail(
                    "Number of columns exceeds %d max allowed %u", list_length(index->indexParams), MAX_KEY_COLUMNS)));
        return MOT::RC_ERROR;
    }

    // check if we have primary and delete previous definition
    if (index->primary) {
        index_order = MOT::IndexOrder::INDEX_ORDER_PRIMARY;
    }

    ix = MOT::IndexFactory::CreateIndex(index_order, indexing_method, flavor);
    if (ix == nullptr) {
        report_pg_error(MOT::RC_ABORT, txn);
        return MOT::RC_ABORT;
    }
    ix->SetExtId(index->indexOid);
    ix->SetNumTableFields((uint32_t)table->GetFieldCount());
    int count = 0;

    ListCell* lc = nullptr;
    foreach (lc, index->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        uint64_t colid = table->GetFieldId((ielem->name != nullptr ? ielem->name : ielem->indexcolname));
        if (colid == (uint64_t)-1) {  // invalid column
            delete ix;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Specified column not found in table definition")));
            return MOT::RC_ERROR;
        }

        MOT::Column* col = table->GetField(colid);

        // Temp solution for NULLs, do not allow index creation on column that does not carry not null flag
        if (!MOT::GetGlobalConfiguration().m_allowIndexOnNullableColumn && !col->m_isNotNull) {
            delete ix;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_FDW_INDEX_ON_NULLABLE_COLUMN_NOT_ALLOWED),
                    errmsg("Can't create index on nullable columns"),
                    errdetail("Column %s is nullable", col->m_name)));
            return MOT::RC_ERROR;
        }

        // Temp solution, we have to support DECIMAL and NUMERIC indexes as well
        if (col->m_type == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            delete ix;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't create index on field"),
                    errdetail("INDEX on NUMERIC or DECIMAL fields not supported yet")));
            return MOT::RC_ERROR;
        }
        if (col->m_keySize > MAX_KEY_SIZE) {
            delete ix;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Column size is greater than maximum index size")));
            return MOT::RC_ERROR;
        }
        keyLength += col->m_keySize;

        ix->SetLenghtKeyFields(count, colid, col->m_keySize);
        count++;
    }

    ix->SetNumIndexFields(count);

    if ((res = ix->IndexInit(keyLength, index->unique, index->idxname, nullptr)) != MOT::RC_OK) {
        delete ix;
        report_pg_error(res, txn);
        return res;
    }

    res = txn->CreateIndex(table, ix, index->primary);
    if (res != MOT::RC_OK) {
        delete ix;
        if (res == MOT::RC_TABLE_EXCEEDS_MAX_INDEXES) {
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                    errmsg("Can not create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
            return MOT::RC_TABLE_EXCEEDS_MAX_INDEXES;
        } else {
            report_pg_error(txn->m_err, txn, index->idxname, txn->m_errMsgBuf);
            return MOT::RC_UNIQUE_VIOLATION;
        }
    }

    return MOT::RC_OK;
}

MOT::RC MOTAdaptor::CreateTable(CreateForeignTableStmt* table, ::TransactionId tid)
{
    bool hasBlob = false;
    MOT::Index* primaryIdx = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(tid);
    MOT::Table* currentTable = nullptr;
    MOT::RC res = MOT::RC_ERROR;
    std::string tname("");
    char* dbname = NULL;

    do {
        currentTable = new (std::nothrow) MOT::Table();
        if (currentTable == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MM), errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Allocation of table metadata failed")));
            break;
        }

        uint32_t columnCount = list_length(table->base.tableElts);

        // once the columns have been counted, we add one more for the nullable columns
        ++columnCount;

        // prepare table name
        dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbname == nullptr) {
            delete currentTable;
            currentTable = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", u_sess->proc_cxt.MyDatabaseId)));
            break;
        }
        tname.append(dbname);
        tname.append("_");
        if (table->base.relation->schemaname != nullptr) {
            tname.append(table->base.relation->schemaname);
        } else {
            tname.append("#");
        }

        tname.append("_");
        tname.append(table->base.relation->relname);

        if (!currentTable->Init(
            table->base.relation->relname, tname.c_str(), columnCount, table->base.relation->foreignOid)) {
            delete currentTable;
            currentTable = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR, txn);
            break;
        }

        // the null fields are copied verbatim because we have to give them back at some point
        res = currentTable->AddColumn(
            "null_bytes", BITMAPLEN(columnCount - 1), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_NULLBYTES);
        if (res != MOT::RC_OK) {
            delete currentTable;
            currentTable = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR, txn);
            break;
        }

        ListCell* cell;

        foreach (cell, table->base.tableElts) {
            int16 typeLen = 0;
            bool isBlob = false;
            MOT::MOT_CATALOG_FIELD_TYPES colType;
            ColumnDef* colDef = (ColumnDef*)lfirst(cell);

            if (colDef == nullptr || colDef->typname == nullptr) {
                delete currentTable;
                currentTable = nullptr;
                ereport(ERROR,
                    (errmodule(MOD_MM),
                        errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                        errmsg("Column definition is not complete"),
                        errdetail("target table is a foreign table")));
                break;
            }

            res = TableFieldType(colDef, colType, &typeLen, isBlob);
            if (res != MOT::RC_OK) {
                delete currentTable;
                currentTable = nullptr;
                report_pg_error(res, txn, colDef, (void*)(int64)typeLen);
                break;
            }
            hasBlob |= isBlob;

            if (colType == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
                if (list_length(colDef->typname->typmods) > 0) {
                    bool canMakeShort = true;
                    int precision = 0;
                    int scale = 0;
                    int count = 0;

                    ListCell* c = nullptr;
                    foreach (c, colDef->typname->typmods) {
                        Node* d = (Node*)lfirst(c);
                        if (!IsA(d, A_Const)) {
                            canMakeShort = false;
                            break;
                        }
                        A_Const* ac = (A_Const*)d;

                        if (ac->val.type != T_Integer) {
                            canMakeShort = false;
                            break;
                        }

                        if (count == 0) {
                            precision = ac->val.val.ival;
                        } else {
                            scale = ac->val.val.ival;
                        }

                        count++;
                    }

                    if (canMakeShort) {
                        int len = 0;

                        len += scale / DEC_DIGITS;
                        len += (scale % DEC_DIGITS > 0 ? 1 : 0);

                        precision -= scale;

                        len += precision / DEC_DIGITS;
                        len += (precision % DEC_DIGITS > 0 ? 1 : 0);

                        typeLen = sizeof(MOT::DecimalSt) + len * sizeof(NumericDigit);
                    }
                }
            }
            res = currentTable->AddColumn(colDef->colname, typeLen, colType, colDef->is_not_null);
            if (res != MOT::RC_OK) {
                delete currentTable;
                currentTable = nullptr;
                report_pg_error(res, txn, colDef, (void*)(int64)typeLen);
                break;
            }
        }

        if (res != MOT::RC_OK) {
            break;
        }

        currentTable->SetFixedLengthRow(!hasBlob);

        uint32_t tupleSize = currentTable->GetTupleSize();
        if (tupleSize > (unsigned int)MAX_TUPLE_SIZE) {
            delete currentTable;
            currentTable = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("MOT: Table %s tuple size %u exceeds MAX_TUPLE_SIZE=%u !!!",
                        table->base.relation->relname,
                        tupleSize,
                        (unsigned int)MAX_TUPLE_SIZE)));
        }

        if (!currentTable->InitRowPool()) {
            delete currentTable;
            currentTable = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR, txn);
            break;
        }

        elog(LOG,
            "creating table %s (OID: %u), num columns: %u, tuple: %u",
            currentTable->GetLongTableName().c_str(),
            table->base.relation->foreignOid,
            columnCount,
            tupleSize);
        // add default PK index
        MOT::RC rc = MOT::RC_OK;
        primaryIdx = MOT::IndexFactory::CreatePrimaryIndexEx(MOT::IndexingMethod::INDEXING_METHOD_TREE,
            DEFAULT_TREE_FLAVOR,
            8,
            currentTable->GetLongTableName(),
            rc,
            nullptr);
        if (rc != MOT::RC_OK) {
            delete currentTable;
            currentTable = nullptr;
            report_pg_error(rc, txn);
            break;
        }
        primaryIdx->SetNumTableFields(columnCount);
        primaryIdx->SetNumIndexFields(1);
        primaryIdx->SetLenghtKeyFields(0, -1, 8);
        primaryIdx->SetFakePrimary(true);

        // The primary index has been created and registered at the table, but not yet initialized
        currentTable->AddPrimaryIndex(std::move(primaryIdx));

        res = txn->CreateTable(currentTable);
    } while (0);

    if (res != MOT::RC_OK) {
        if (currentTable != nullptr) {
            MOT::GetTableManager()->DropTable(currentTable, txn->GetSessionContext());
        }
        if (primaryIdx != nullptr) {
            delete primaryIdx;
        }
    }

    return res;
}

MOT::RC MOTAdaptor::DropIndex(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    txn->SetTransactionId(tid);

    elog(LOG, "dropping index %s, ixoid: %u, taboid: %u", stmt->name, stmt->indexoid, stmt->reloid);

    // get table
    do {
        MOT::Index* index = txn->GetIndexByExternalId(stmt->reloid, stmt->indexoid);
        if (index == nullptr) {
            elog(LOG,
                "Drop index %s error, index oid %u of table oid %u not found.",
                stmt->name,
                stmt->indexoid,
                stmt->reloid);
            res = MOT::RC_INDEX_NOT_FOUND;
        } else {
            uint64_t table_relid = index->GetTable()->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid);
            res = txn->DropIndex(index);
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::DropTable(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    MOT::TxnManager* txn = GetSafeTxn();
    txn->SetTransactionId(tid);

    elog(LOG, "dropping table %s, oid: %u", stmt->name, stmt->reloid);
    do {
        tab = txn->GetTableByExternalId(stmt->reloid);
        if (tab == nullptr) {
            res = MOT::RC_TABLE_NOT_FOUND;
            elog(LOG, "Drop table %s error, table oid %u not found.", stmt->name, stmt->reloid);
        } else {
            uint64_t table_relid = tab->GetTableExId();
            JitExec::PurgeJitSourceCache(table_relid);
            res = txn->DropTable(tab);
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::TruncateTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;

    EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn();
    txn->SetTransactionId(tid);

    elog(LOG, "truncating table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = txn->GetTableByExternalId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Truncate table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->WrLock();
        res = txn->TruncateTable(tab);
        tab->Unlock();
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::VacuumTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    txn->SetTransactionId(tid);

    elog(LOG, "vacuuming table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = MOT::GetTableManager()->GetTableSafeByExId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Vacuum table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->Compact(txn);
        tab->Unlock();
    } while (0);
    return res;
}

uint64_t MOTAdaptor::GetTableIndexSize(uint64_t tabId, uint64_t ixId)
{
    uint64_t res = 0;
    EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn();
    MOT::Table* tab = nullptr;
    MOT::Index* ix = nullptr;

    do {
        tab = txn->GetTableByExternalId(tabId);
        if (tab == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MM),
                    errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                    errmsg("Get table size error, table oid %lu not found.", tabId)));
            break;
        }

        if (ixId > 0) {
            ix = txn->GetIndexByExternalId(tabId, ixId);
            if (ix == nullptr) {
                ereport(ERROR,
                    (errmodule(MOD_MM),
                        errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                        errmsg("Get index size error, index oid %lu for table oid %lu not found.", ixId, tabId)));
                break;
            }
            res = ix->GetIndexSize();
        } else
            res = tab->GetTableSize();
    } while (0);

    return res;
}

MotMemoryDetail* MOTAdaptor::GetMemSize(uint32_t* nodeCount, bool isGlobal)
{
    EnsureSafeThreadAccessInline();
    MotMemoryDetail* result = nullptr;
    *nodeCount = 0;

    /* We allocate an array of size (m_nodeCount + 1) to accommodate one aggregated entry of all global pools. */
    uint32_t statsArraySize = MOT::g_memGlobalCfg.m_nodeCount + 1;
    MOT::MemRawChunkPoolStats* chunkPoolStatsArray =
        (MOT::MemRawChunkPoolStats*)palloc(statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
    if (chunkPoolStatsArray != nullptr) {
        errno_t erc = memset_s(chunkPoolStatsArray,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats),
            0,
            statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
        securec_check(erc, "\0", "\0");

        uint32_t realStatsEntries;
        if (isGlobal) {
            realStatsEntries = MOT::MemRawChunkStoreGetGlobalStats(chunkPoolStatsArray, statsArraySize);
        } else {
            realStatsEntries = MOT::MemRawChunkStoreGetLocalStats(chunkPoolStatsArray, statsArraySize);
        }

        MOT_ASSERT(realStatsEntries <= statsArraySize);
        if (realStatsEntries > 0) {
            result = (MotMemoryDetail*)palloc(realStatsEntries * sizeof(MotMemoryDetail));
            if (result != nullptr) {
                for (uint32_t node = 0; node < realStatsEntries; ++node) {
                    result[node].numaNode = chunkPoolStatsArray[node].m_node;
                    result[node].reservedMemory = chunkPoolStatsArray[node].m_reservedBytes;
                    result[node].usedMemory = chunkPoolStatsArray[node].m_usedBytes;
                }
                *nodeCount = realStatsEntries;
            }
        }
        pfree(chunkPoolStatsArray);
    }

    return result;
}

MotSessionMemoryDetail* MOTAdaptor::GetSessionMemSize(uint32_t* sessionCount)
{
    EnsureSafeThreadAccessInline();
    MotSessionMemoryDetail* result = nullptr;
    *sessionCount = 0;

    uint32_t session_count = MOT::g_memGlobalCfg.m_maxThreadCount;
    MOT::MemSessionAllocatorStats* session_stats_array =
        (MOT::MemSessionAllocatorStats*)palloc(session_count * sizeof(MOT::MemSessionAllocatorStats));
    if (session_stats_array != nullptr) {
        uint32_t real_session_count = MOT::MemSessionGetAllStats(session_stats_array, session_count);
        if (real_session_count > 0) {
            result = (MotSessionMemoryDetail*)palloc(real_session_count * sizeof(MotSessionMemoryDetail));
            if (result != nullptr) {
                for (uint32_t session_index = 0; session_index < real_session_count; ++session_index) {
                    GetSessionDetails(session_stats_array[session_index].m_sessionId,
                        &result[session_index].threadid,
                        &result[session_index].threadStartTime);
                    result[session_index].totalSize = session_stats_array[session_index].m_reservedSize;
                    result[session_index].usedSize = session_stats_array[session_index].m_usedSize;
                    result[session_index].freeSize = result[session_index].totalSize - result[session_index].usedSize;
                }
                *sessionCount = real_session_count;
            }
        }
        pfree(session_stats_array);
    }

    return result;
}

void MOTAdaptor::CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start)
{
    uint8_t* buf = nullptr;
    uint8_t pattern = 0x00;
    EnsureSafeThreadAccessInline();
    int16_t num = festate->m_bestIx->m_ix->GetNumFields();
    const uint16_t* fieldLengths = festate->m_bestIx->m_ix->GetLengthKeyFields();
    const int16_t* orgCols = festate->m_bestIx->m_ix->GetColumnKeyFields();
    TupleDesc desc = rel->rd_att;
    uint16_t offset = 0;
    int32_t* exprs = nullptr;
    KEY_OPER* opers = nullptr;
    uint16_t keyLength;
    KEY_OPER oper;

    if (start == 0) {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_start];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_start];
        oper = festate->m_bestIx->m_ixOpers[0];
    } else {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_end];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_end];
        // end may be equal start but the operation maybe different, look at getCost
        oper = festate->m_bestIx->m_ixOpers[1];
    }

    keyLength = festate->m_bestIx->m_ix->GetKeyLength();
    festate->m_stateKey[start].InitKey(keyLength);
    buf = festate->m_stateKey[start].GetKeyBuf();

    switch (oper) {
        case KEY_OPER::READ_KEY_EXACT:
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_BEFORE:
        case KEY_OPER::READ_KEY_LIKE:
        case KEY_OPER::READ_PREFIX:
        case KEY_OPER::READ_PREFIX_LIKE:
        case KEY_OPER::READ_PREFIX_OR_NEXT:
        case KEY_OPER::READ_PREFIX_BEFORE:
            pattern = 0x00;
            break;

        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_PREFIX_AFTER:
        case KEY_OPER::READ_PREFIX_OR_PREV:
        case KEY_OPER::READ_KEY_AFTER:
            pattern = 0xff;
            break;

        default:
            elog(LOG, "Invalid key operation: %u", oper);
            break;
    }

    for (int i = 0; i < num; i++) {
        if (opers[i] < KEY_OPER::READ_INVALID) {
            bool is_null = false;
            ExprState* expr = (ExprState*)list_nth(festate->m_execExprs, exprs[i] - 1);
            Datum val = ExecEvalExpr((ExprState*)(expr), festate->m_econtext, &is_null, nullptr);
            if (is_null) {
                MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
                FILL_KEY_NULL(desc->attrs[orgCols[i] - 1]->atttypid, buf + offset, fieldLengths[i]);
            } else {
                MOT::Column* col = festate->m_table->GetField(orgCols[i]);
                uint8_t fill = 0x00;

                // in case of like fill rest of the key with appropriate to direction values
                if (opers[i] == KEY_OPER::READ_KEY_LIKE) {
                    switch (oper) {
                        case KEY_OPER::READ_KEY_LIKE:
                        case KEY_OPER::READ_KEY_OR_NEXT:
                        case KEY_OPER::READ_KEY_AFTER:
                        case KEY_OPER::READ_PREFIX:
                        case KEY_OPER::READ_PREFIX_LIKE:
                        case KEY_OPER::READ_PREFIX_OR_NEXT:
                        case KEY_OPER::READ_PREFIX_AFTER:
                            break;

                        case KEY_OPER::READ_PREFIX_BEFORE:
                        case KEY_OPER::READ_PREFIX_OR_PREV:
                        case KEY_OPER::READ_KEY_BEFORE:
                        case KEY_OPER::READ_KEY_OR_PREV:
                            fill = 0xff;
                            break;

                        case KEY_OPER::READ_KEY_EXACT:
                        default:
                            elog(LOG, "Invalid key operation: %u", oper);
                            break;
                    }
                }

                DatumToMOTKey(col,
                    expr->expr,
                    val,
                    desc->attrs[orgCols[i] - 1]->atttypid,
                    buf + offset,
                    fieldLengths[i],
                    opers[i],
                    fill);
            }
        } else {
            MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
            festate->m_stateKey[start].FillPattern(pattern, fieldLengths[i], offset);
        }

        offset += fieldLengths[i];
    }

    festate->m_bestIx->m_ix->AdjustKey(&festate->m_stateKey[start], pattern);
}

bool MOTAdaptor::IsScanEnd(MOTFdwStateSt* festate)
{
    bool res = false;
    EnsureSafeThreadAccessInline();

    // festate->cursor[1] (end iterator) might be NULL (in case it is not in use). If this is the case, return false
    // (which means we have not reached the end yet)
    if (festate->m_cursor[1] == nullptr) {
        return false;
    }

    if (!festate->m_cursor[1]->IsValid()) {
        return true;
    } else {
        const MOT::Key* startKey = nullptr;
        const MOT::Key* endKey = nullptr;
        MOT::Index* ix = (festate->m_bestIx != nullptr ? festate->m_bestIx->m_ix : festate->m_table->GetPrimaryIndex());

        startKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[0]->GetKey());
        endKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[1]->GetKey());
        if (startKey != nullptr && endKey != nullptr) {
            int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), ix->GetKeySizeNoSuffix());

            if (festate->m_forwardDirectionScan) {
                if (cmpRes > 0)
                    res = true;
            } else {
                if (cmpRes < 0)
                    res = true;
            }
        }
    }

    return res;
}

void MOTAdaptor::PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow)
{
    errno_t erc;
    EnsureSafeThreadAccessInline();
    HeapTuple srcData = slot->tts_tuple;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    bool hasnulls = HeapTupleHasNulls(srcData);
    uint64_t i = 0;
    uint64_t j = 1;
    uint64_t cols = table->GetFieldCount() - 1;  // column count includes null bits field

    // the null bytes are necessary and have to give them back
    if (!hasnulls) {
        erc = memset_s(destRow + table->GetFieldOffset(i), table->GetFieldSize(i), 0xff, table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    } else {
        erc = memcpy_s(destRow + table->GetFieldOffset(i),
            table->GetFieldSize(i),
            &srcData->t_data->t_bits[0],
            table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    }

    // we now copy the fields, for the time being the null ones will be copied as well
    for (; i < cols; i++, j++) {
        bool isnull = false;
        Datum value = slot_getattr(slot, j, &isnull);

        if (!isnull) {
            DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
        }
    }
}

void MOTAdaptor::PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint8_t* bits;
    uint64_t i = 0;
    uint64_t j = 1;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;
    bits = destRow + table->GetFieldOffset(i);

    for (; i < cols; i++, j++) {
        if (BITMAP_GET(attrs_used, i)) {
            bool isnull = false;
            Datum value = slot_getattr(slot, j, &isnull);

            if (!isnull) {
                DatumToMOT(table->GetField(j), value, tupdesc->attrs[i]->atttypid, destRow);
                BITMAP_SET(bits, i);
            } else {
                BITMAP_CLEAR(bits, i);
            }
        }
    }
}

void MOTAdaptor::UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow)
{
    EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;

    for (; i < cols; i++) {
        if (BITMAP_GET(attrs_used, i))
            MOTToDatum(table, tupdesc->attrs[i], srcRow, &(slot->tts_values[i]), &(slot->tts_isnull[i]));
        else {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = PointerGetDatum(nullptr);
        }
    }
}

// useful functions for data conversion: utils/fmgr/gmgr.cpp
void MOTAdaptor::MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null)
{
    EnsureSafeThreadAccessInline();
    if (!BITMAP_GET(data, (attr->attnum - 1))) {
        *is_null = true;
        *value = PointerGetDatum(nullptr);

        return;
    }

    size_t len = 0;
    MOT::Column* col = table->GetField(attr->attnum);

    *is_null = false;
    switch (attr->atttypid) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID:
        case BYTEAOID: {
            uintptr_t tmp;
            col->Unpack(data, &tmp, len);

            bytea* result = (bytea*)palloc(len + VARHDRSZ);
            errno_t erc = memcpy_s(VARDATA(result), len, (uint8_t*)tmp, len);
            securec_check(erc, "\0", "\0");
            SET_VARSIZE(result, len + VARHDRSZ);

            *value = PointerGetDatum(result);
            break;
        }
        case NUMERICOID: {
            MOT::DecimalSt* d;
            col->Unpack(data, (uintptr_t*)&d, len);

            *value = NumericGetDatum(MOTNumericToPG(d));
            break;
        }
        default:
            col->Unpack(data, value, len);
            break;
    }
}

void MOTAdaptor::DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data)
{
    EnsureSafeThreadAccessInline();
    switch (type) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea* txt = DatumGetByteaP(datum);
            size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
            char* src = VARDATA(txt);
            col->Pack(data, (uintptr_t)src, size - VARHDRSZ);

            if ((char*)datum != (char*)txt) {
                pfree(txt);
            }

            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            MOT::DecimalSt* d = (MOT::DecimalSt*)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR,
                    (errmodule(MOD_MM),
                        errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("Value exceeds maximum precision: %d", NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToMOT(n, *d);
            col->Pack(data, (uintptr_t)d, DECIMAL_SIZE(d));

            break;
        }
        default:
            col->Pack(data, datum, col->m_size);
            break;
    }
}

void MOTAdaptor::DatumToMOTKey(
    MOT::Column* col, Expr* expr, Datum datum, Oid type, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    EnsureSafeThreadAccessInline();
    switch (type) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            if (expr && IsA(expr, Const)) {  // OA: LLVM passes nullptr for expr parameter
                Const* c = (Const*)expr;

                if (c->constbyval) {
                    errno_t erc = memset_s(data, len, 0x00, len);
                    securec_check(erc, "\0", "\0");
                    break;
                }
            }
            bytea* txt = DatumGetByteaP(datum);
            size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
            char* src = VARDATA(txt);

            if (size > len)
                size = len;

            size -= VARHDRSZ;
            if (oper == KEY_OPER::READ_KEY_LIKE) {
                if (src[size - 1] == '%')
                    size -= 1;
                else
                    fill = 0x00;  // switch to equal
            }
            col->PackKey(data, (uintptr_t)src, size, fill);

            if ((char*)datum != (char*)txt) {
                pfree(txt);
            }

            break;
        }
        case FLOAT4OID: {
            if (expr && IsA(expr, Const)) {  // OA: LLVM passes nullptr for expr parameter
                Const* c = (Const*)expr;

                if (c->consttype == FLOAT8OID) {
                    MOT::DoubleConvT dc;
                    MOT::FloatConvT fc;
                    dc.m_r = (uint64_t)datum;
                    fc.m_v = (float)dc.m_v;
                    uint64_t u = (uint64_t)fc.m_r;
                    col->PackKey(data, u, col->m_size);
                } else {
                    col->PackKey(data, datum, col->m_size);
                }
            }
            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            MOT::DecimalSt* d = (MOT::DecimalSt*)buf;
            PGNumericToMOT(n, *d);
            col->PackKey(data, (uintptr_t)d, DECIMAL_SIZE(d));

            break;
        }
        default:
            col->PackKey(data, datum, col->m_size);
            break;
    }
}

bool MatchIndex::IsSameOper(KEY_OPER op1, KEY_OPER op2) const
{
    bool res = true;
    if (op1 == op2) {
        return res;
    }

    switch (op1) {
        case KEY_OPER::READ_KEY_EXACT: {
            res = true;
            break;
        }
        case KEY_OPER::READ_KEY_LIKE: {
            res = true;
            break;
        }
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_AFTER: {
            switch (op2) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_AFTER:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_KEY_BEFORE: {
            switch (op2) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_KEY_BEFORE:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        default:
            break;
    }

    return res;
}

void MatchIndex::ClearPreviousMatch(MOTFdwStateSt* state, bool set_local, int i, int j)
{
    if (m_parentColMatch[i][j] != nullptr) {
        if (set_local) {
            if (!list_member(state->m_localConds, m_parentColMatch[i][j])) {
                state->m_localConds = lappend(state->m_localConds, m_parentColMatch[i][j]);
            }
        }

        m_parentColMatch[i][j] = nullptr;
        m_numMatches[i]--;
    }
}

bool MatchIndex::SetIndexColumn(
    MOTFdwStateSt* state, int16_t colNum, KEY_OPER op, Expr* expr, Expr* parent, bool set_local)
{
    bool res = false;
    int i = 0;
    const int16_t* cols = m_ix->GetColumnKeyFields();
    int16_t numKeyCols = m_ix->GetNumFields();
    bool sameOper = false;

    for (; i < numKeyCols; i++) {
        if (cols[i] == colNum) {
            if (m_colMatch[0][i] == nullptr) {
                m_parentColMatch[0][i] = parent;
                m_colMatch[0][i] = expr;
                m_opers[0][i] = op;
                m_numMatches[0]++;
                res = true;
                break;
            } else {
                sameOper = IsSameOper(m_opers[0][i], op);
                // do not use current and previous expression for this column
                // in index selection
                // we can not differentiate between a > 2 and a > 1
                if (sameOper) {
                    if (op == KEY_OPER::READ_KEY_EXACT && m_opers[0][i] != op) {
                        ClearPreviousMatch(state, set_local, 0, i);
                        ClearPreviousMatch(state, set_local, 1, i);
                        m_parentColMatch[0][i] = parent;
                        m_colMatch[0][i] = expr;
                        m_opers[0][i] = op;
                        m_numMatches[0]++;
                        res = true;
                    } else if (m_opers[0][i] != KEY_OPER::READ_KEY_EXACT) {
                        ClearPreviousMatch(state, set_local, 0, i);
                    }
                    break;
                }
            }

            sameOper = false;
            if (m_colMatch[1][i] == nullptr) {
                m_parentColMatch[1][i] = parent;
                m_colMatch[1][i] = expr;
                m_opers[1][i] = op;
                m_numMatches[1]++;
                res = true;
            } else {
                sameOper = IsSameOper(m_opers[1][i], op);
                // do not use current and previous expression for this column
                // in index selection
                // we can not differentiate between a > 2 and a > 1
                if (sameOper) {
                    if (op == KEY_OPER::READ_KEY_EXACT && m_opers[0][i] != op) {
                        ClearPreviousMatch(state, set_local, 0, i);
                        m_parentColMatch[0][i] = parent;
                        m_colMatch[0][i] = expr;
                        m_opers[0][i] = op;
                        m_numMatches[0]++;
                        res = true;
                    } else if (m_opers[0][i] != KEY_OPER::READ_KEY_EXACT) {
                        ClearPreviousMatch(state, set_local, 0, i);
                    }
                    break;
                }
            }

            break;
        }
    }

    if (res && (i < numKeyCols)) {
        switch (state->m_table->GetFieldType(cols[i])) {
            case MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR:
            case MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR:
                res = false;
                break;
            default:
                break;
        }
    }
    return res;
}

inline bool MatchIndex::IsFullMatch() const
{
    return (m_numMatches[0] == m_ix->GetNumFields() || m_numMatches[1] == m_ix->GetNumFields());
}

inline double MatchIndex::GetCost(int numClauses)
{
    if (m_costs[0] == 0) {
        int partialIxMulti = 100;
        int notUsed[2] = {0, 0};
        int used[2] = {0, 0};

        m_costs[0] = 1.0;
        m_costs[1] = 1.0;

        for (int i = 0; i < m_ix->GetNumFields(); i++) {
            // we had same operations on the column, clean all settings
            if (m_parentColMatch[0][i] == nullptr && m_parentColMatch[1][i] != nullptr) {
                m_parentColMatch[0][i] = m_parentColMatch[1][i];
                m_colMatch[0][i] = m_colMatch[1][i];
                m_opers[0][i] = m_opers[1][i];

                m_parentColMatch[1][i] = nullptr;
                m_colMatch[1][i] = nullptr;
                m_opers[1][i] = KEY_OPER::READ_INVALID;

                m_numMatches[0]++;
                m_numMatches[1]--;
            } else if (m_parentColMatch[0][i] == nullptr) {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                m_colMatch[0][i] = nullptr;
            } else if (m_parentColMatch[1][i] == nullptr) {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                m_colMatch[1][i] = nullptr;
            }
        }

        for (int i = 0; i < m_ix->GetNumFields(); i++) {
            if (m_colMatch[0][i] == nullptr || notUsed[0] > 0) {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                notUsed[0]++;
            } else if (m_opers[0][i] < KEY_OPER::READ_INVALID) {
                KEY_OPER curr = keyOperStateMachine[m_ixOpers[0]][m_opers[0][i]];

                if (curr < KEY_OPER::READ_INVALID) {
                    m_ixOpers[0] = curr;
                    used[0]++;
                    if (m_colMatch[1][i] == nullptr &&
                        (m_opers[0][i] == KEY_OPER::READ_KEY_EXACT || m_opers[0][i] == KEY_OPER::READ_KEY_LIKE)) {
                        m_colMatch[1][i] = m_colMatch[0][i];
                        m_opers[1][i] = m_opers[0][i];
                        m_parentColMatch[1][i] = m_parentColMatch[0][i];
                    }
                } else {
                    m_opers[0][i] = KEY_OPER::READ_INVALID;
                    notUsed[0]++;
                }
            } else {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                notUsed[0]++;
            }

            if (m_colMatch[1][i] == nullptr || notUsed[1] > 0) {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                notUsed[1]++;
            } else if (m_opers[1][i] < KEY_OPER::READ_INVALID) {
                KEY_OPER curr = keyOperStateMachine[m_ixOpers[1]][m_opers[1][i]];

                if (curr < KEY_OPER::READ_INVALID) {
                    m_ixOpers[1] = curr;
                    used[1]++;
                } else {
                    m_opers[1][i] = KEY_OPER::READ_INVALID;
                    notUsed[1]++;
                }
            } else {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                notUsed[1]++;
            }
        }

        for (int i = 0; i < 2; i++) {
            int estimated_rows = 0;
            if (notUsed[i] > 0) {
                if (m_ixOpers[i] < KEY_OPER::READ_INVALID)
                    m_ixOpers[i] = (KEY_OPER)((uint8_t)m_ixOpers[i] | KEY_OPER_PREFIX_BITMASK);
                estimated_rows += (notUsed[i] * partialIxMulti);
            }

            // we assume that that all other operations will bring 10 times rows
            if (m_ixOpers[i] != KEY_OPER::READ_KEY_EXACT)
                estimated_rows += 10;

            // we assume that using partial key will bring 10 times more rows per not used column
            if (used[i] < numClauses) {
                estimated_rows += (numClauses - used[i]) * 10;
            }

            m_costs[i] += estimated_rows;
        }

        if (!m_ix->GetUnique()) {
            if (m_ixOpers[0] == KEY_OPER::READ_KEY_EXACT)
                m_ixOpers[0] = KEY_OPER::READ_PREFIX;
            else if (m_ixOpers[1] == KEY_OPER::READ_KEY_EXACT)
                m_ixOpers[1] = KEY_OPER::READ_PREFIX;
        }

        if (m_costs[0] <= m_costs[1]) {
            m_cost = m_costs[0];
            m_start = 0;

            if (m_colMatch[1][0]) {
                m_end = 1;
                if (m_ixOpers[1] == KEY_OPER::READ_PREFIX || m_ixOpers[1] == KEY_OPER::READ_PREFIX_LIKE) {
                    if (((m_ixOpers[0] & ~KEY_OPER_PREFIX_BITMASK) < KEY_OPER::READ_KEY_OR_PREV)) {
                        m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
                    } else {
                        m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_NEXT;
                    }
                }
            }
            if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_EXACT && m_ix->GetUnique()) {
                m_end = -1;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_EXACT) {
                m_end = m_start;
                m_ixOpers[1] = m_ixOpers[m_start];
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_PREFIX) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_LIKE) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_KEY_OR_PREV;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_PREFIX_LIKE) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
            }
        } else {
            m_cost = m_costs[1];
            m_start = 1;
            m_end = 0;

            KEY_OPER tmp = m_ixOpers[0];
            m_ixOpers[0] = m_ixOpers[1];
            m_ixOpers[1] = tmp;
        }
    }

    if (m_ixOpers[0] == KEY_OPER::READ_INVALID && m_ixOpers[1] == KEY_OPER::READ_INVALID) {
        return INT_MAX;
    }

    return m_cost;
}

bool MatchIndex::AdjustForOrdering(bool desc)
{
    if (m_end == -1 && m_ixOpers[0] == READ_KEY_EXACT) {  // READ_KEY_EXACT
        return true;
    }

    KEY_OPER curr = (KEY_OPER)(m_ixOpers[0] & ~KEY_OPER_PREFIX_BITMASK);
    bool has_both = (m_start != -1 && m_end != -1);
    bool curr_desc = !(curr < KEY_OPER::READ_KEY_OR_PREV);

    if (desc == curr_desc) {
        return true;
    } else if (!has_both) {
        return false;
    }

    KEY_OPER tmpo = m_ixOpers[0];
    m_ixOpers[0] = m_ixOpers[1];
    m_ixOpers[1] = tmpo;

    int32_t tmp = m_start;
    m_start = m_end;
    m_end = tmp;

    return true;
}

bool MatchIndex::CanApplyOrdering(const int* orderCols) const
{
    int16_t numKeyCols = m_ix->GetNumFields();

    // check if order columns are overlap index matched columns or are suffix for it
    for (int16_t i = 0; i < numKeyCols; i++) {
        // overlap: we can use index ordering
        if (m_colMatch[0][i] != nullptr && orderCols[i] == 1) {
            return true;
        }

        // suffix: the order columns are continuation of index columns, we can use index ordering
        if (m_colMatch[0][i] == nullptr && orderCols[i] == 1) {
            return true;
        }

        // we have gap between matched index columns and order columns
        if (m_colMatch[0][i] == nullptr && orderCols[i] == 0) {
            return false;
        }
    }

    return false;
}

void MatchIndex::Serialize(List** list) const
{
    List* ixlist = nullptr;

    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_ixPosition), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_start), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_end), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_cost), false, true));

    for (int i = 0; i < 2; i++) {
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_numMatches[i]), false, true));
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_costs[i]), false, true));
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_ixOpers[i]), false, true));

        for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
            ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_opers[i][j]), false, true));
            ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_params[i][j]), false, true));
        }
    }

    *list = list_concat(*list, ixlist);
}

void MatchIndex::Deserialize(ListCell* cell, uint64_t exTableID)
{
    MOT::TxnManager* txn = GetSafeTxn();

    m_ixPosition = (int32_t)((Const*)lfirst(cell))->constvalue;
    MOT::Table* table = txn->GetTableByExternalId(exTableID);
    if (table != nullptr) {
        m_ix = txn->GetIndex(table, m_ixPosition);
    }

    cell = lnext(cell);

    m_start = (int32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_end = (int32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_cost = (uint32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    for (int i = 0; i < 2; i++) {
        m_numMatches[i] = (int32_t)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        m_costs[i] = (uint32_t)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        m_ixOpers[i] = (KEY_OPER)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
            m_opers[i][j] = (KEY_OPER)((Const*)lfirst(cell))->constvalue;
            cell = lnext(cell);

            m_params[i][j] = (int32_t)((Const*)lfirst(cell))->constvalue;
            cell = lnext(cell);
        }
    }
}

void MatchIndex::Clean(MOTFdwStateSt* state)
{
    for (int k = 0; k < 2; k++) {
        for (int j = 0; j < m_ix->GetNumFields(); j++) {
            if (m_colMatch[k][j]) {
                if (!list_member(state->m_localConds, m_parentColMatch[k][j]) &&
                    !(m_remoteCondsOrig != nullptr && list_member(m_remoteCondsOrig, m_parentColMatch[k][j])))
                    state->m_localConds = lappend(state->m_localConds, m_parentColMatch[k][j]);
                m_colMatch[k][j] = nullptr;
                m_parentColMatch[k][j] = nullptr;
            }
        }
    }
}
