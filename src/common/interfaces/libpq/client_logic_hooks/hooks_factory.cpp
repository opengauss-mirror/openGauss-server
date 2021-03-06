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
 * gscodegen.cpp
 *	  hooks_factory.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\hooks_factory.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "hooks_factory.h"
#include "encryption_column_hook_executor.h"
#include "encryption_global_hook_executor.h"
#include <algorithm>

// DEFINE NEW HOOKS HERE
static GlobalHookExecutor *EncryptionGlobalHookExecutorCreator(PGClientLogic &client_logic)
{
    GlobalHookExecutor *encryption_global_hook = new (std::nothrow) EncryptionGlobalHookExecutor(client_logic);
    if (encryption_global_hook == NULL) {
        fprintf(stderr, "failed to new GlobalHookExecutor object\n");
        exit(EXIT_FAILURE);
    }
    return encryption_global_hook;
}

static void EncryptionGlobalHookExecutorDeleter(GlobalHookExecutor *global_hook_executor)
{
    delete ((EncryptionColumnHookExecutor *)global_hook_executor);
}

static ColumnHookExecutor *EncryptionColumnHookExecutorCreator(GlobalHookExecutor *global_hook_executor, Oid oid)
{
    ColumnHookExecutor *encryption_column_hook =
        new (std::nothrow) EncryptionColumnHookExecutor(global_hook_executor, oid);
    if (encryption_column_hook == NULL) {
        fprintf(stderr, "failed to new ColumnHookExecutor object\n");
        exit(EXIT_FAILURE);
    }
    return encryption_column_hook;
}

static void EncryptionColumnHookExecutorDeleter(ColumnHookExecutor *column_hook_executor)
{
    delete ((EncryptionColumnHookExecutor *)column_hook_executor);
}

HooksFactory::GlobalSettings::GlobalHooksCreatorFuncsMapping
    HooksFactory::GlobalSettings::m_global_hooks_creator_mapping[] = {
        {"encryption", EncryptionGlobalHookExecutorCreator}
    };

HooksFactory::GlobalSettings::GlobalHooksDeleterFunctionsMapping
    HooksFactory::GlobalSettings::m_global_hooks_deleter_mapping[] = {
        {"encryption", EncryptionGlobalHookExecutorDeleter}
    };

HooksFactory::ColumnSettings::ColumnHooksCreatorFunctionsMapping 
    HooksFactory::ColumnSettings::m_column_hooks_creator_mapping[] = {
        {"encryption", EncryptionColumnHookExecutorCreator}
};

HooksFactory::ColumnSettings::ColumnHooksDeleterFunctionsMapping 
    HooksFactory::ColumnSettings::m_column_hooks_deleter_mapping[] = {
        {"encryption", EncryptionColumnHookExecutorDeleter}
};

GlobalHookExecutor *HooksFactory::GlobalSettings::get(const char *function_name, PGClientLogic &clientlogic)
{
    GlobalHookExecutor *global_hook_executor(NULL);
    GlobalHookExecutorCreator globalHookExecutorCreator(NULL);
    for (int i = 0; i < HOOKS_NUM; i++) {
        if (pg_strcasecmp(m_global_hooks_creator_mapping[i].name, function_name) == 0) {
            globalHookExecutorCreator = m_global_hooks_creator_mapping[i].executor;
        }
    }

    if (globalHookExecutorCreator) {
        global_hook_executor = globalHookExecutorCreator(clientlogic);
    }
    return global_hook_executor;
}

void HooksFactory::GlobalSettings::del(GlobalHookExecutor *global_hook_executor)
{
    GlobalHookExecutorDeleter globalHookExecutorDeleter(NULL);
    for (int i = 0; i < HOOKS_NUM; i++) {
        if (pg_strcasecmp(m_global_hooks_deleter_mapping[i].name, global_hook_executor->m_function_name) == 0) {
            globalHookExecutorDeleter = m_global_hooks_deleter_mapping[i].executor;
        }
    }

    if (globalHookExecutorDeleter) {
        globalHookExecutorDeleter(global_hook_executor);
    }
}

ColumnHookExecutor *HooksFactory::ColumnSettings::get(const char *function_name, Oid oid,
    GlobalHookExecutor *global_hook_executor)
{
    ColumnHookExecutor *column_hook_executor(NULL);
    ColumnHookExecutorCreator columnHookExecutorCreator(NULL);
    for (int i = 0; i < HOOKS_NUM; i++) {
        if (pg_strcasecmp(m_column_hooks_creator_mapping[i].name, function_name) == 0) {
            columnHookExecutorCreator = m_column_hooks_creator_mapping[i].executor;
        }
    }
    if (columnHookExecutorCreator) {
        column_hook_executor = columnHookExecutorCreator(global_hook_executor, oid);
    }
    return column_hook_executor;
}

void HooksFactory::ColumnSettings::del(ColumnHookExecutor *column_hook_executor)
{
    ColumnHookExecutorDeleter columnHookExecutorDeleter(NULL);
    for (int i = 0; i < HOOKS_NUM; i++) {
        if (pg_strcasecmp(m_column_hooks_deleter_mapping[i].name, column_hook_executor->m_function_name) == 0) {
            columnHookExecutorDeleter = m_column_hooks_deleter_mapping[i].executor;
        }
    }
    if (columnHookExecutorDeleter) {
        columnHookExecutorDeleter(column_hook_executor);
    }
}
