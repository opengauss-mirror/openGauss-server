/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cl_state.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\cl_state.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "libpq-int.h"
#include "cl_state.h"
#include "client_logic_cache/types_to_oid.h"
#include "client_logic_cache/cache_id.h"
#include "client_logic_hooks/hook_resource.h"
#include "client_logic_processor/raw_values_list.h"
#include "client_logic_processor/prepared_statements_list.h"
#include "client_logic_cache/cache_loader.h"
#include "client_logic_cache/cached_column_manager.h"
#include "client_logic_cache/proc_list.h"
#include "client_logic_cache/cached_proc.h"
#include "client_logic_cache/dataTypes.def"
#include "client_logic_data_fetcher/data_fetcher_manager.h"
#include "keymgr/security_key_adpt.h"

PGClientLogic::PGClientLogic(PGconn *conn, JNIEnv *java_env, jobject jdbc_handle)
    : m_conn(conn),
      enable_client_encryption(false),
      disable_once(false),
      preparedStatements(NULL),
      pendingStatements(NULL),
      droppedSchemas(NULL),
      droppedSchemas_size(0),
      droppedSchemas_allocated(0),
      droppedGlobalSettings(NULL),
      droppedColumnSettings(NULL),
      droppedColumnSettings_size(0),
      droppedColumnSettings_allocated(0),
      isInvalidOperationOnColumn(false),
      isDuringRefreshCacheOnError(false),
      is_external_err(false),
      cacheRefreshType(CacheRefreshType::CACHE_ALL),
      m_hookResources(),
      val_to_update(updateGucValues::GUC_NONE),
      called_functions_list(NULL),
      called_functions_list_size(0),
      proc_list(NULL)
{
    m_lastResultStatus = PGRES_EMPTY_QUERY;
    rawValuesForReplace = new RawValuesList();
    preparedStatements = new PreparedStatementsList;
    pendingStatements = new PreparedStatementsList;
    lastStmtName[0] = '\0';
    client_cache_id = get_client_cache_id();
    m_cached_column_manager = new CachedColumnManager;
    m_data_fetcher_manager = new DataFetcherManager(conn, java_env, jdbc_handle);
    m_parser_context.eaten_begin = false;
    m_parser_context.eaten_declare = false;
    m_key_adpt = key_adpt_new();
}

PGClientLogic::~PGClientLogic()
{
    if (rawValuesForReplace) {
        delete rawValuesForReplace;
        rawValuesForReplace = NULL;
    }

    for (size_t i = 0; i < HOOK_RESOURCES_COUNT; ++i) {
        if (m_hookResources[i] != NULL) {
            delete m_hookResources[i];
            m_hookResources[i] = NULL;
        }
    }

    if (preparedStatements) {
        delete preparedStatements;
        preparedStatements = NULL;
    }
    if (pendingStatements) {
        delete pendingStatements;
        pendingStatements = NULL;
    }

    put_client_cache_id_back(client_cache_id);
    free_obj_list(droppedGlobalSettings);

    libpq_free(droppedColumnSettings);
    libpq_free(droppedSchemas);

    if (m_data_fetcher_manager != NULL) {
        delete m_data_fetcher_manager;
        m_data_fetcher_manager = NULL;
    }
    m_cached_column_manager->clear();
    delete m_cached_column_manager;

    key_adpt_free((KeyAdpt *)m_key_adpt);
}

void PGClientLogic::clear_functions_list()
{
    for (size_t i = 0; i < called_functions_list_size; i++) {
        libpq_free(called_functions_list[i]);
    }
    libpq_free(called_functions_list);
    called_functions_list_size = 0;
    if (proc_list) {
        proc_list->clear();
        delete proc_list;
        proc_list = NULL;
    }
}

void PGClientLogic::insert_function(const char *fname, CachedProc *proc)
{
    if (called_functions_list_size == 0) {
        proc_list = new ProcList();
        called_functions_list = (char **)malloc(sizeof(char *));
    } else {
        called_functions_list = (char **)libpq_realloc(called_functions_list,
            called_functions_list_size * sizeof(char *), (called_functions_list_size + 1) * sizeof(char *));
    }
    if (called_functions_list == NULL) {
        return;
    }
    called_functions_list[called_functions_list_size] = strdup(fname);
    proc_list->add(proc);
    called_functions_list_size++;
}

const ICachedRec *PGClientLogic::get_cl_proc(const char *pname) const
{
    if (called_functions_list == NULL) {
        return NULL;
    }
    for (size_t i = 0; i < called_functions_list_size; i++) {
        if (strcmp(called_functions_list[i], pname) == 0) {
            return (ICachedRec *)proc_list->at(i);
        }
    }
    return NULL;
}

const ICachedRec *PGClientLogic::get_cl_rec(const Oid typid, const char *pname) const
{
    if (typid == RECORDOID) {
        return get_cl_proc(pname);
    } else {
        return (const ICachedRec *)m_cached_column_manager->get_cached_type(typid);
    }
}

const int *PGClientLogic::get_rec_origial_ids(const Oid typid, const char *pname) const
{
    const ICachedRec *rec = get_cl_rec(typid, pname);
    if (!rec) {
        return NULL;
    } else {
        return rec->get_original_ids();
    }
}

/**
 * gets the length of original oids for a type
 * @param typid the type oid as returned from the database
 * @param pname the type name as returned from the database
 * @return the number of original oids
 */
size_t PGClientLogic::get_rec_origial_ids_length(const Oid typid, const char *pname) const
{
    const ICachedRec *rec = get_cl_rec(typid, pname);
    if (rec == NULL) {
        return 0;
    }
    return rec->get_num_processed_args();
}
