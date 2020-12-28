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
 * hooks_factory.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\hooks_factory.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef HOOKS_FACTORY_H
#define HOOKS_FACTORY_H

#include <string>
#define HOOKS_NUM 2
typedef unsigned int Oid;

class PGClientLogic;
class ColumnHookExecutor;

class GlobalHookExecutor;
class HooksFactory {
private:
    static const int m_FUNCTION_NAME_SIZE = 256;

public:
    class GlobalSettings {
    public:
        static GlobalHookExecutor *get(const char *function_name, PGClientLogic &);
        static void del(GlobalHookExecutor *global_hook_executor);

    private:
        /* CREATE */
        typedef GlobalHookExecutor *(*GlobalHookExecutorCreator)(PGClientLogic &client_logic);
        typedef struct GlobalHooksCreatorFuncsMapping {
            char name[m_FUNCTION_NAME_SIZE];
            GlobalHookExecutorCreator executor;
        } GlobalHooksCreatorFuncsMapping;
        static GlobalHooksCreatorFuncsMapping m_global_hooks_creator_mapping[HOOKS_NUM];

        /* DELETE */
        typedef void (*GlobalHookExecutorDeleter)(GlobalHookExecutor *);
        typedef struct GlobalHooksDeleterFunctionsMapping {
            char name[m_FUNCTION_NAME_SIZE];
            GlobalHookExecutorDeleter executor;
        } GlobalHooksDeleterFunctionsMapping;
        static GlobalHooksDeleterFunctionsMapping m_global_hooks_deleter_mapping[HOOKS_NUM];
    };

    class ColumnSettings {
    public:
        static ColumnHookExecutor *get(const char *function_name, Oid oid, GlobalHookExecutor *global_hook_executor);
        static void del(ColumnHookExecutor *column_hook_executor);

    private:
        typedef ColumnHookExecutor *(*ColumnHookExecutorCreator)(GlobalHookExecutor *, Oid);
        typedef struct ColumnHooksCreatorFunctionsMapping {
            char name[m_FUNCTION_NAME_SIZE];
            ColumnHookExecutorCreator executor;
        } ColumnHooksCreatorFunctionsMapping;
        static ColumnHooksCreatorFunctionsMapping m_column_hooks_creator_mapping[HOOKS_NUM];

        typedef void (*ColumnHookExecutorDeleter)(ColumnHookExecutor *);
        typedef struct ColumnHooksDeleterFunctionsMapping {
            char name[m_FUNCTION_NAME_SIZE];
            ColumnHookExecutorDeleter executor;
        } ColumnHooksDeleterFunctionsMapping;
        static ColumnHooksDeleterFunctionsMapping m_column_hooks_deleter_mapping[HOOKS_NUM];
    };
};
#endif /* HOOKS_FACTORY_H */
