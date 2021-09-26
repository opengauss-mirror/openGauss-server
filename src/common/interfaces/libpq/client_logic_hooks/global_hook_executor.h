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
 * global_hook_executor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\global_hook_executor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GLOBAL_HOOK_EXECUTOR_H
#define GLOBAL_HOOK_EXECUTOR_H

#include "postgres_fe.h"

#include "abstract_hook_executor.h"

class ColumnHookExecutor;
class PGClientLogic;
class CStringsMap;
typedef CStringsMap StringArgs;

/*
 * Every Global Setting object holds a GlobalHookExecutor instance.
 * The GlobalHookExecutor is an abstract class. The actual class to be initiated is based on the type chosen in
 * creation.
 */

class GlobalHookExecutor : public AbstractHookExecutor {
public:
    GlobalHookExecutor(PGClientLogic &client_logic, const char *function_name)
        : AbstractHookExecutor(function_name), m_clientLogic(client_logic)
    {}

    virtual ~GlobalHookExecutor() {}

    /* REQUIRED INTERFACES */

    /*
     * The Process function is called directly by the Column Hook's Process function. This is a mandatory part of the
     * flow and cannot be overriden. The Process function overrides / modified / approves the configuration as requested
     * by the Column Hook Executor. Every hook is required to implmement function.
     */
    virtual bool process(ColumnHookExecutor *column_hook_executor) = 0;

    /* OPTIONAL INTERFACES */

    /*
     * before the creation the Global Setting, the arguments should be validated.
     * the existing Global Settings already configured in the system are also passed as input so if required, they can
     * also be used for validity. For example: checking for uniqueness of arguments.
     */
    virtual bool pre_create(const StringArgs &args, const GlobalHookExecutor **existing_global_hook_executors,
        size_t existing_global_hook_executors_size);
    virtual bool post_create(const StringArgs& args);
    virtual bool deprocess_column_setting(const unsigned char *processed_data, size_t processed_data_size, 
    const char *key_store, const char *key_path, const char *key_algo, unsigned char **data, size_t *data_size);

    virtual bool set_deletion_expected();

    /*
     * BUILT-IN FUNCTIONS
     */
    void set_client_logic(PGClientLogic &);
    PGClientLogic &get_client_logic();

    PGClientLogic &m_clientLogic;
};

#endif
