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
 * cl_state.h
 *
 * IDENTIFICATION
 *	  src\include\libpq\cl_state.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CL_STATE_H
#define CL_STATE_H

/* CLIENT LOGIC STATE */
#include "guc_vars.h"
#include <memory>
#include <unordered_map>
#include "client_logic_cache/cache_refresh_type.h"
#include "libpq-fe.h"
#include "postgres_fe.h"
#include "client_logic_processor/raw_values_list.h"
#include "client_logic_cache/proc_list.h"
#include "client_logic_cache/icached_rec.h"
#include "client_logic_cache/icached_column_manager.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_data_fetcher/data_fetcher_manager.h"
 #include "client_logic/client_logic_enums.h"

typedef struct pg_conn PGconn;

struct ICachedColumns;
typedef struct CopyStateData CopyStateData;
class CStringsMap;
typedef CStringsMap StringArgs;
class StatementData;
class ICachedColumnManager;

class HookResource;
typedef struct objectFqdn {
    char data[NAMEDATALEN * 4];
} ObjectFqdn;

typedef struct JNIEnv_ JNIEnv; /* forward declaration */
class _jobject;
typedef _jobject *jobject;     /* forward declaration */

#define HOOK_RESOURCES_COUNT 1
typedef struct clientlogic_parser_context {
    bool eaten_begin;
    bool eaten_declare;
} clientlogic_parser_context;

class PreparedStatementsList;

/*
    the PGClientLogic class maintains the per session state machine related to the Client Logic feature
*/
class PGClientLogic {
public:
    PGClientLogic(PGconn *conn, JNIEnv *java_env, jobject jdbc_handle);
    ~PGClientLogic();
    void clear_functions_list();
    void insert_function(const char *fname,  CachedProc *proc);
    const ICachedRec* get_cl_rec(const Oid typid, const char* pname) const;
    const int* get_rec_origial_ids(const Oid tpyid, const char* pname) const;
    size_t get_rec_origial_ids_length(const Oid typid, const char* pname) const;
    PGconn* m_conn;
    bool enable_client_encryption;
    bool disable_once;
    PreparedStatementsList *preparedStatements;
    PreparedStatementsList *pendingStatements;

    char lastStmtName[NAMEDATALEN];

    ObjectFqdn *droppedSchemas;
    size_t droppedSchemas_size;
    size_t droppedSchemas_allocated;
    ObjName *droppedGlobalSettings;
    ObjectFqdn *droppedColumnSettings;
    size_t droppedColumnSettings_size;
    size_t droppedColumnSettings_allocated;
    ExecStatusType m_lastResultStatus;
    bool isInvalidOperationOnColumn;
    bool isDuringRefreshCacheOnError;
    bool is_external_err;
    CacheRefreshType cacheRefreshType;
    HookResource* m_hookResources[HOOK_RESOURCES_COUNT];

    // GUC params
    GucParams gucParams;
    GucParams tmpGucParams;
    updateGucValues val_to_update;
    RawValuesList *rawValuesForReplace; /* helper list for replacing raw values in query string on text mode */
    RawValuesList raw_values_for_post_query; /* list of all raw values in the query for replacing in error response */
    ICachedColumnManager* m_cached_column_manager;
    char **called_functions_list;
    size_t called_functions_list_size;
    ProcList *proc_list;
    size_t client_cache_id;
    void *m_key_adpt;

    // sql parser context
    clientlogic_parser_context m_parser_context; 
    DataFetcherManager* m_data_fetcher_manager;
private:
    const ICachedRec* get_cl_proc(const char* pname) const;
};

#define CL_GET_KEY_ADPT(clientlogic) (KeyAdpt *)(void *)((clientlogic).m_key_adpt)
#endif /* CL_STATE_H */
