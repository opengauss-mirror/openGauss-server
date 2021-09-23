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
 * hooks_manager.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\hooks_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef HOOKS_MANAGER_H
#define HOOKS_MANAGER_H

#include "pg_config.h"

#include <string>
#include <client_logic_cache/icached_column.h>
#include "client_logic_processor/values_processor.h"
typedef unsigned int Oid;

class PGClientLogic;
class ColumnHookExecutor;
typedef ColumnHookExecutor *ColumnHookExecutorSptr;

class GlobalHookExecutor;
typedef GlobalHookExecutor *GlobalHookExecutorSptr;
class CStringsMap;
typedef CStringsMap StringArgs;
class HooksManager {
public:
    /*
     * DDL REQUEST
     */
    class GlobalSettings {
    public:
        static GlobalHookExecutorSptr create_global_hook_executor(const char *function_name,
            PGClientLogic &clientlogic);
        static void delete_global_hook_executor(GlobalHookExecutor *global_hook_executor);

        /* during the DDL CREATE, this validate shall get all of the arguments as input */
        static bool pre_create(PGClientLogic &clientlogic, const char *function_name, const StringArgs &args,
            const GlobalHookExecutor **, size_t);
        static bool post_create(PGClientLogic &clientlogic, const char *function_name, StringArgs &args);
        static bool set_deletion_expected(PGClientLogic& clientLogic, const char *object_name, bool is_schema);
        static bool deprocess_column_setting(const unsigned char *processed_data, size_t processed_data_size, 
            const char *key_store, const char *key_path, const char *key_algo, unsigned char **data,
            size_t *data_size);
    };

    class ColumnSettings {
    public:
        static ColumnHookExecutorSptr create_column_hook_executor(const char *function_name, const Oid oid,
            GlobalHookExecutorSptr global_hook_executor);
        static void delete_column_hook_executor(ColumnHookExecutor *column_hook_executor);
        /*
         * during the DDL CREATE, this validate shall get all of the arguments as input
         * and pass these arguments to the function requested
         */
        static bool pre_create(PGClientLogic &client_logic, const GlobalHookExecutorSptr global_hook_executor,
            const char *function_name, const StringArgs &args, StringArgs &new_args);
        static bool set_deletion_expected(PGClientLogic& clientLogic, const char *object_name, const bool is_schema);
    };

    /*
     * DML REQUEST
     */

    /* process the data */
    static int get_estimated_processed_data_size(const ColumnHookExecutorsList *hookExecutors, int dataSize);
    static int process_data(const ICachedColumn *cachedColumn, ColumnHookExecutorsList *column_hook_executors_list,
        const unsigned char *data, int dataSize, unsigned char *processedData);

    /*
     * DML REPONSE
     */

    /* the dataProcessed should be encoded with some of the arguments in the begining of the data */
    static DecryptDataRes deprocess_data(PGClientLogic &clientlogic, const unsigned char *dataProcessed, 
        int dataProcessedsize, unsigned char **data, int *data_plain_size);

    /* UNION, IN, NOT IN, INTERSECT, EXCEPT */
    static bool is_set_operation_allowed(const ICachedColumn *ce);

    /* WHERE clause with "<,=,>" operations */
    static bool is_operator_allowed(const ICachedColumn *ce, const char * const op);
};

#endif
