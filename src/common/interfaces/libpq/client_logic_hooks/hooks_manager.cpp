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
 * hooks_manager.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_hooks\hooks_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>
#include "hooks_manager.h"
#include "global_hook_executor.h"
#include "column_hook_executor.h"
#include "hooks_factory.h"
#include "client_logic_cache/cache_loader.h"
#include "postgres_ext.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cl_state.h"
#include <iostream>
#include "client_logic_cache/cached_column_setting.h"
#include "client_logic_cache/column_hook_executors_list.h"
#include "encryption_column_hook_executor.h"
#include "encryption_global_hook_executor.h"
#include "client_logic_processor/values_processor.h"
#include "client_logic_cache/cached_column_manager.h"

/* DDL */
GlobalHookExecutorSptr HooksManager::GlobalSettings::create_global_hook_executor(const char *function_name,
    PGClientLogic &clientlogic)
{
    return HooksFactory::GlobalSettings::get(function_name, clientlogic);
}

void HooksManager::GlobalSettings::delete_global_hook_executor(GlobalHookExecutor *global_hook_executor)
{
    HooksFactory::GlobalSettings::del(global_hook_executor);
}

bool HooksManager::GlobalSettings::pre_create(PGClientLogic &client_logic, const char *function_name,
    const StringArgs &args, const GlobalHookExecutor **existing_global_hook_executors,
    size_t existing_global_hook_executors_size)
{
    /*
     * 1. validate args
     * 2. update prepared statements state
     * this is not pointer in use, the creation here is for validation only
     */
    GlobalHookExecutorSptr global_hook_executor = create_global_hook_executor(function_name, client_logic);
    if (!global_hook_executor) {
        printfPQExpBuffer(&client_logic.m_conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): failed to create client master key for: %s\n"), function_name);
        return false;
    }
    bool ret =
        global_hook_executor->pre_create(args, existing_global_hook_executors, existing_global_hook_executors_size);
    delete_global_hook_executor(global_hook_executor);
    return ret;
}

bool HooksManager::GlobalSettings::post_create(PGClientLogic &client_logic, const char *function_name, StringArgs &args)
{
    GlobalHookExecutor *global_hook_executor = create_global_hook_executor(function_name, client_logic);
    if (!global_hook_executor) {
        return false;
    }
    bool ret = global_hook_executor->post_create(args);
    delete_global_hook_executor(global_hook_executor);
    return ret;
}

bool HooksManager::GlobalSettings::deprocess_column_setting(const unsigned char *processed_data, 
    size_t processed_data_size, const char *key_store, const char *key_path, const char *key_algo, unsigned char **data,
    size_t *data_size)
{
    PGClientLogic client_logic(NULL, NULL, NULL);
    GlobalHookExecutor *global_hook_executor = create_global_hook_executor("encryption", client_logic);
    if (!global_hook_executor) {
        return false;
    }
    bool ret = global_hook_executor->deprocess_column_setting(processed_data, processed_data_size, key_store, key_path,
        key_algo, data, data_size);
    delete_global_hook_executor(global_hook_executor);
    return ret;
}

ColumnHookExecutorSptr HooksManager::ColumnSettings::create_column_hook_executor(const char *function_name,
    const Oid oid, GlobalHookExecutorSptr global_hook_executor)
{
    return HooksFactory::ColumnSettings::get(function_name, oid, global_hook_executor);
}

void HooksManager::ColumnSettings::delete_column_hook_executor(ColumnHookExecutor *column_hook_executor)
{
    return HooksFactory::ColumnSettings::del(column_hook_executor);
}
bool HooksManager::ColumnSettings::pre_create(PGClientLogic &client_logic,
    const GlobalHookExecutorSptr global_hook_executor, const char *function, const StringArgs &args,
    StringArgs &new_args)
{
    ColumnHookExecutorSptr column_hook_executor =
        create_column_hook_executor(function, InvalidOid, global_hook_executor);
    if (!column_hook_executor) {
        printfPQExpBuffer(&client_logic.m_conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): failed to create client master key for: %s\n"), function);
        return false;
    }
    bool ret = column_hook_executor->pre_create(global_hook_executor->get_client_logic(), args, new_args);
    delete_column_hook_executor(column_hook_executor);
    return ret;
}


/* DML */
int HooksManager::get_estimated_processed_data_size(const ColumnHookExecutorsList *column_hook_executors_list,
    int data_size)
{
    int total = 0;
    size_t column_hook_executors_list_size = column_hook_executors_list->size();
    for (size_t i = 0; i < column_hook_executors_list_size; ++i) {
        ColumnHookExecutor *column_hook_executor = column_hook_executors_list->at(i);
        total += column_hook_executor->get_estimated_processed_data_size(data_size);
    }
    return total + 1; /* 1 is for the byte counting how many executors run */
}
int HooksManager::process_data(const ICachedColumn *cached_column,
    ColumnHookExecutorsList *column_hook_executors_list, const unsigned char *data, int data_size,
    unsigned char *processed_data)
{
    if (column_hook_executors_list == NULL) {
        return -1;
    }
    processed_data[0] = column_hook_executors_list->size();
    int total = 1;
    int ret = 0;

    size_t column_hook_executors_list_size = column_hook_executors_list->size();
    for (size_t i = 0; i < column_hook_executors_list_size; ++i) {
        ColumnHookExecutor *column_hook_executor = column_hook_executors_list->at(i);
        ret = column_hook_executor->process_data(cached_column, data, data_size, processed_data + 1);
        if (ret < 0) {
            return -1;
        } else {
            total += ret;
        }
    }
    return total;
}

DecryptDataRes HooksManager::deprocess_data(PGClientLogic &client_logic, const unsigned char *data_processed,
    int data_processed_size, unsigned char **data, int *data_plain_size)
{
    DecryptDataRes dec_dat_res = DEC_DATA_ERR;
    
    size_t column_settings_id_cnt(0);
    if (!data_processed || data_processed_size <= 0) {
        if (client_logic.m_conn->verbosity == PQERRORS_VERBOSE) {
            fprintf(stderr, "deprocess empty data\n");
        }
        return DEC_DATA_ERR;
    }

    /* column settings count (HEADER) */
    column_settings_id_cnt = (size_t)*data_processed++;
    if (column_settings_id_cnt < 1) {
        fprintf(stderr, "invalid number of column encryption key: %zu\n", column_settings_id_cnt);
        return DECRYPT_CEK_ERR;
    }
    --data_processed_size;

    /* iterate over all column settings */
    for (size_t i = 0; i < column_settings_id_cnt; ++i) {
        /* column setting ID */
        Oid column_setting_oid(0);
        errno_t rc = EOK;
        rc = memcpy_s(&column_setting_oid, sizeof(Oid), data_processed, sizeof(Oid));
        securec_check_c(rc, "\0", "\0");
        data_processed += sizeof(Oid);
        data_processed_size -= sizeof(Oid);

        /* column hook executor */
        ColumnHookExecutor *column_hook_executor =
            client_logic.m_cached_column_manager->get_column_hook_executor(column_setting_oid);
        if (!column_hook_executor) {
            return CLIENT_CACHE_ERR;
        }

        /* deprocess */
        dec_dat_res = column_hook_executor->deprocess_data(data_processed, data_processed_size,
            data, data_plain_size);
        if (dec_dat_res != DEC_DATA_SUCCEED) {
            return dec_dat_res;
        }

        /* next iteration will continue from the same point */
        data_processed = *data;
        data_processed_size = *data_plain_size;
    }

    return DEC_DATA_SUCCEED;
}

bool HooksManager::is_operator_allowed(const ICachedColumn *ce, const char * const op)
{
    ColumnHookExecutorsList *column_hook_executors_list = ce->get_column_hook_executors();
    size_t column_hook_executors_list_size = column_hook_executors_list->size();
    for (size_t i = 0; i < column_hook_executors_list_size; ++i) {
        ColumnHookExecutor *column_hook_executor = column_hook_executors_list->at(i);
        if (!column_hook_executor->is_operator_allowed(ce, op)) {
            return false;
        }
    }
    return true;
}

bool HooksManager::is_set_operation_allowed(const ICachedColumn *ce)
{
    if (ce == NULL) {
        return false;
    }
    ColumnHookExecutorsList *column_hook_executors_list = ce->get_column_hook_executors();
    if (column_hook_executors_list == NULL) {
        return false;
    }
    size_t column_hook_executors_list_size = column_hook_executors_list->size();
    for (size_t i = 0; i < column_hook_executors_list_size; ++i) {
        ColumnHookExecutor *column_hook_executor = column_hook_executors_list->at(i);
        if (column_hook_executor != NULL && !column_hook_executor->is_set_operation_allowed(ce)) {
            return false;
        }
    }
    return true;
}

bool HooksManager::GlobalSettings::set_deletion_expected(PGClientLogic& clientLogic, 
    const char *object_name, const bool is_schema)
{
    const CachedGlobalSetting **global_settings(NULL);
    size_t global_settings_size(0);
    if (is_schema) {
        global_settings =
            clientLogic.m_cached_column_manager->get_global_settings_by_schema_name(object_name, global_settings_size);
    } else {
        const CachedGlobalSetting *globalSetting = 
            clientLogic.m_cached_column_manager->get_global_setting_by_fqdn(object_name);
        global_settings = (const CachedGlobalSetting **)libpq_realloc(global_settings,
            sizeof(CachedGlobalSetting *) * global_settings_size,
            sizeof(CachedGlobalSetting *) * (global_settings_size + 1));
        if (global_settings == NULL) {
            return false;
        }
        global_settings[0] = globalSetting;
        global_settings_size = 1;
    }

    for (size_t i = 0; i < global_settings_size; ++i) {
        global_settings[i]->get_executor()->set_deletion_expected();
    }

    if (global_settings) {
        free(global_settings);
        global_settings = NULL;
    }
    return true;
}
bool HooksManager::ColumnSettings::set_deletion_expected(PGClientLogic& clientLogic, 
    const char *object_name, const bool is_schema)
{
    const CachedColumnSetting **column_settings(NULL);
    size_t column_settings_size(0);
    if (is_schema) {
        column_settings =
            clientLogic.m_cached_column_manager->get_column_setting_by_schema_name(object_name, column_settings_size);
    } else {
        const CachedColumnSetting *columnSetting = 
            clientLogic.m_cached_column_manager->get_column_setting_by_fqdn(object_name);
        column_settings = (const CachedColumnSetting **)malloc(sizeof(CachedColumnSetting *) * 1);
        if (column_settings == NULL) {
            fprintf(stderr, "out of memory when setting deleted objects\n");
            return false;
        }
        column_settings[0] = columnSetting;
        column_settings_size = 1;
    }

    for (size_t i = 0; i < column_settings_size; ++i) {
        column_settings[i]->get_executor()->set_deletion_expected();
    }
    libpq_free(column_settings);
    return true;
}
