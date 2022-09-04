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
 * encryption_pre_process.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\encryption_pre_process.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ENCRYPTION_PRE_PROCESS_H
#define ENCRYPTION_PRE_PROCESS_H

#include <string.h>
#include <memory>
#include <ostream>
#include "nodes/parsenodes_common.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_cache/cache_loader.h"
#include "stmt_processor.h"
#include "client_logic/cstrings_map.h"

class EncryptionPreProcess {
public:
    static bool run_pre_column_encryption_key(PGconn *conn, CreateClientLogicColumn *client_logic_column,
        char *global_key_name, const char **function_name, StringArgs &string_args, int *location,
        int *encrypted_value_location, size_t *encrypted_value_size, size_t *quote_num);
    static void set_new_query(char **query, size_t query_size, StringArgs string_args, int location,
        int encrypted_value_location, size_t encrypted_valueSize, size_t quote_num);
    static bool run_pre_client_master_key(PGconn *conn, CreateClientLogicGlobal *client_logic_global,
        char **function_name, StringArgs &string_args);
    static const int MAX_KEY_ADD_LEN = 1024;
};

#endif