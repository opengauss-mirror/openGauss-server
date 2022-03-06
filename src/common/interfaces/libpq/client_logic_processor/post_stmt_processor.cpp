/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * post_stmt_processor.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_processor\post_stmt_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "pg_config.h"

#include "stmt_processor.h"
#include "cl_state.h"
#include "client_logic_cache/cache_refresh_type.h"
#include "client_logic_hooks/hooks_manager.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "prepared_statement.h"
#include "prepared_statements_list.h"

#include <iostream>

void Processor::remove_dropped_column_settings(PGconn *conn, const bool is_success)
{
    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    if (conn->client_logic->droppedColumnSettings_size == 0) {
        return;
    }

    if (!is_success) {
        conn->client_logic->droppedColumnSettings_size = 0;
        return;
    }

    for (size_t i = 0; i < conn->client_logic->droppedColumnSettings_size; ++i) {
        const char *object_name = conn->client_logic->droppedColumnSettings[i].data;
        HooksManager::ColumnSettings::set_deletion_expected(*conn->client_logic, object_name, false);
    }
    conn->client_logic->droppedColumnSettings_size = 0;
}

void Processor::remove_dropped_global_settings(PGconn *conn, const bool is_success)
{
    ObjName *cur_cmk = conn->client_logic->droppedGlobalSettings;
    ObjName *to_free = NULL;

    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    if (cur_cmk == NULL) {
        return;
    }

    if (!is_success) {
        free_obj_list(conn->client_logic->droppedGlobalSettings);
        conn->client_logic->droppedGlobalSettings = NULL;
        return;
    }

    while (cur_cmk != NULL) {
        HooksManager::GlobalSettings::set_deletion_expected(*conn->client_logic, cur_cmk->obj_name, false);
        to_free = cur_cmk;
        cur_cmk = cur_cmk->next;
        libpq_free(to_free);
    }
    conn->client_logic->droppedGlobalSettings = NULL;
}

const bool Processor::remove_droppend_schemas(PGconn *conn, const bool is_success)
{
    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    if (conn->client_logic->droppedSchemas_size == 0) {
        return true;
    }

    if (!is_success) {
        conn->client_logic->droppedSchemas_size = 0;
        return true;
    }

    for (size_t i = 0; i < conn->client_logic->droppedSchemas_size; ++i) {
        const char *object_name = conn->client_logic->droppedSchemas[i].data;
        HooksManager::GlobalSettings::set_deletion_expected(*conn->client_logic, object_name, true);
        HooksManager::ColumnSettings::set_deletion_expected(*conn->client_logic, object_name, true);
        conn->client_logic->m_cached_column_manager->remove_schema(object_name);
    }
    conn->client_logic->droppedSchemas_size = 0;
    return true;
}
bool Processor::accept_pending_statements(PGconn *conn, bool is_success)
{
    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    if (!is_success) {
        conn->client_logic->pendingStatements->clear();
        return true;
    }

    conn->client_logic->preparedStatements->merge(conn->client_logic->pendingStatements);
    conn->client_logic->pendingStatements->clear();
    return true;
}

void handle_post_set_stmt(PGconn *conn, const bool is_success)
{
    if (!is_success) {
        return;
    }
    if (conn->client_logic->val_to_update & updateGucValues::GUC_ROLE) {
        conn->client_logic->m_cached_column_manager->set_user_schema(conn->client_logic->tmpGucParams.role.c_str());
        conn->client_logic->gucParams.role = conn->client_logic->tmpGucParams.role;
        conn->client_logic->val_to_update ^= updateGucValues::GUC_ROLE;
    }
    if (conn->client_logic->val_to_update & updateGucValues::SEARCH_PATH) {
        conn->client_logic->m_cached_column_manager->load_search_path(
            conn->client_logic->tmpGucParams.searchpathStr.c_str(),
            conn->client_logic->gucParams.role.c_str());
        conn->client_logic->val_to_update ^= updateGucValues::SEARCH_PATH;
    }
    if (conn->client_logic->val_to_update & updateGucValues::BACKSLASH_QUOTE) {
        conn->client_logic->gucParams.backslash_quote = conn->client_logic->tmpGucParams.backslash_quote;
        conn->client_logic->val_to_update ^= updateGucValues::BACKSLASH_QUOTE;
    }
    if (conn->client_logic->val_to_update & updateGucValues::CONFORMING) {
        conn->client_logic->gucParams.standard_conforming_strings =
            conn->client_logic->tmpGucParams.standard_conforming_strings;
        conn->client_logic->val_to_update ^= updateGucValues::CONFORMING;
    }
    if (conn->client_logic->val_to_update & updateGucValues::ESCAPE_STRING) {
        conn->client_logic->gucParams.escape_string_warning = conn->client_logic->tmpGucParams.escape_string_warning;
        conn->client_logic->val_to_update ^= updateGucValues::ESCAPE_STRING;
    }
}

bool Processor::run_post_query(PGconn *conn, bool force_error)
{
    if (!conn) {
        return false;
    }

    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    if (conn->client_logic->rawValuesForReplace) {
        conn->client_logic->rawValuesForReplace->clear();
    }

    if (!conn->client_logic->raw_values_for_post_query.empty()) {
        conn->client_logic->raw_values_for_post_query.clear();
    }
    char last_stmt_name[NAMEDATALEN];
    errno_t rc = EOK;
    rc = memset_s(last_stmt_name, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check_c(rc, "\0", "\0");
    /* swap local lastStmtName with lastStmtName object on connection */

    check_memcpy_s(
        memcpy_s(last_stmt_name, NAMEDATALEN, conn->client_logic->lastStmtName, NAMEDATALEN));
    check_memset_s(memset_s(conn->client_logic->lastStmtName, NAMEDATALEN,  0, NAMEDATALEN));

        int last_result = conn->client_logic->m_lastResultStatus;
    conn->client_logic->m_lastResultStatus = PGRES_EMPTY_QUERY;

    bool is_success = (last_result == PGRES_COMMAND_OK && !force_error);

    accept_pending_statements(conn, is_success);
    remove_droppend_schemas(conn, is_success);
    remove_dropped_global_settings(conn, is_success);
    remove_dropped_column_settings(conn, is_success);
    handle_post_set_stmt(conn, is_success);
    conn->client_logic->clear_functions_list();
    if (!is_success || (conn->queryclass != PGQUERY_SIMPLE && conn->queryclass != PGQUERY_EXTENDED)) {
        /* we only want to process successful queries that were actually executed */
        return true;
    }

    PreparedStatement *prepared_statement = conn->client_logic->preparedStatements->get_or_create(last_stmt_name);
    if (!prepared_statement) {
        return false;
    }

    if (!is_success || (conn->queryclass != PGQUERY_SIMPLE && conn->queryclass != PGQUERY_EXTENDED)) {
        /* we only want to process successful queries that were actually executed */
        return true;
    }

    if ((prepared_statement->cacheRefresh & CacheRefreshType::GLOBAL_SETTING) == CacheRefreshType::GLOBAL_SETTING &&
        prepared_statement->m_function_name[0] != '\0') {
        /*
         * run post_create hook requirements after the arguments have been validaity and the Global Setting is sure to
         * be created We conduct this operation here to support incremental executions of post_create. So we only call
         * post_create for the new Global Setting and not for existing Global Settings. We actually create a copy of the
         * hook here but we don't save it in memory - we toss it right away. The hook will be saved in memory when the
         * CacheLoader loads it from the catalog table
         */
        bool ret = HooksManager::GlobalSettings::post_create(*conn->client_logic, prepared_statement->m_function_name,
            prepared_statement->m_string_args);
        if (!ret) {
            return false;
        }
    }
    /*
     * we override the cacheRefreshType from the (prepared) statement object
     * otherwise, the cacheRefreshType is filled with the value from the getReadyForQuery
     */
    conn->client_logic->cacheRefreshType = prepared_statement->cacheRefresh;
    prepared_statement->cacheRefresh = CacheRefreshType::CACHE_NONE;
    conn->client_logic->m_cached_column_manager->load_cache(conn);
    return true;
}
