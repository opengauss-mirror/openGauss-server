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
 * encryption_pre_process.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\encryption_pre_process.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "encryption_pre_process.h"
#include "client_logic_cache/icached_column_manager.h"
#include "processor_utils.h"
#include "pqexpbuffer.h"
#include "libpq-int.h"
/*
 * create cek
 * need to inital COLUMN ENCRYPTION KEY with CLIENT_MASTER_KEY,ALGORITHM,ENCRYPTED_VALUE,COLUMN_FUNCTION.
 * and their value must be unique.
 */
bool EncryptionPreProcess::run_pre_column_encryption_key(PGconn *conn, CreateClientLogicColumn *client_logic_column,
    char *global_key_name, const char **function_name, StringArgs &string_args, int *location,
    int *encrypted_value_location, size_t *encrypted_value_size, size_t *quote_num)
{
    ListCell *column_param_iter(NULL);
    bool if_duplicate_masterkey(false);
    bool if_duplicate_column_function(false);
    bool if_duplicate_cek_algorithm(false);
    bool if_duplicate_expected_value(false);

    foreach (column_param_iter, client_logic_column->column_setting_params) {
        ClientLogicColumnParam *column_param = static_cast<ClientLogicColumnParam *> lfirst(column_param_iter);
        if (column_param == NULL) {
            return false;
        }
        switch (column_param->key) {
            case ClientLogicColumnProperty::CLIENT_GLOBAL_SETTING: {
                if (if_duplicate_masterkey) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): duplicate master key args\n"));
                    return false;
                }
                if (!name_list_to_cstring(column_param->qualname, global_key_name, NAMEDATALEN * NAME_CNT)) {
                    fprintf(stderr, "ERROR(CLIENT): client master key name cannot be empty\n");
                    return false;
                }
                char global_fqdn[NAMEDATALEN * NAME_CNT];
                check_memset_s(memset_s(global_fqdn, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT));
                size_t global_fqdn_size =
                    conn->client_logic->m_cached_column_manager->get_object_fqdn(global_key_name, true, global_fqdn);
                if (global_fqdn_size == 0) {
                    conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
                    conn->client_logic->m_cached_column_manager->load_cache(conn);
                    global_fqdn_size = conn->client_logic->m_cached_column_manager->get_object_fqdn(global_key_name,
                        true, global_fqdn);
                    if (global_fqdn_size == 0) {
                        printfPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("ERROR(CLIENT): failed to get client master key %s from cache\n"),
                            global_key_name);
                        return false;
                    }
                }
                check_strncpy_s(strncpy_s(global_key_name, NAMEDATALEN * NAME_CNT, global_fqdn, global_fqdn_size));
                if_duplicate_masterkey = true;
                break;
            }
            case ClientLogicColumnProperty::COLUMN_COLUMN_FUNCTION: {
                if (if_duplicate_column_function) {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("ERROR(CLIENT): duplicate column function args\n"));
                    return false;
                }
                *function_name = column_param->value;
                if ((*function_name == NULL) || strlen(*function_name) == 0) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT) function name is missing\n"));
                    return false;
                }

                if_duplicate_column_function = true;
                break;
            }
            case ClientLogicColumnProperty::CEK_ALGORITHM: {
                if (if_duplicate_cek_algorithm) {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("ERROR(CLIENT): duplicate CEK algorithm args\n"));
                    return false;
                }
                string_args.set("ALGORITHM", column_param->value);

                *location = column_param->location;

                if_duplicate_cek_algorithm = true;
                break;
            }
            case ClientLogicColumnProperty::CEK_EXPECTED_VALUE: {
                if (if_duplicate_expected_value) {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("ERROR(CLIENT): duplicate expected value args\n"));
                    return false;
                }
                if (strlen(column_param->value) > MAX_VAL_LEN) {
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("ERROR(CLIENT): encryption key too long\n"));
                    return false;
                }
                string_args.set("ENCRYPTED_VALUE", column_param->value);
                *encrypted_value_location = column_param->location;
                *encrypted_value_size = strlen(column_param->value);
                char ch = '\'';
                for (size_t i = 0; i <= *encrypted_value_size; ++i) {
                    if (column_param->value[i] == ch) {
                        ++(*quote_num);
                    }
                }
                if_duplicate_expected_value = true;
                break;
            }
            default: {
                break;
            }
        }
    }
    return true;
}

/* append quotation marks to string */
void EncryptionPreProcess::set_new_query(char **query, size_t query_size, StringArgs string_args, int location,
    int encrypted_value_location, size_t encrypted_value_size, size_t quote_num)
{
    for (size_t i = 0; i < string_args.Size(); i++) {
        char string_to_add[MAX_KEY_ADD_LEN];
        errno_t rc = memset_s(string_to_add, MAX_KEY_ADD_LEN, 0, MAX_KEY_ADD_LEN);
        securec_check_c(rc, "\0", "\0");
        size_t total_in = 0;
        if (string_args.at(i) == NULL) {
            continue;
        }
        const char *key = string_args.at(i)->key;
        const char *value = string_args.at(i)->value;
        const size_t vallen = string_args.at(i)->valsize;
        if (!key || !value) {
            Assert(false);
            continue;
        }
        Assert(vallen < MAX_KEY_ADD_LEN);
        check_strncat_s(strncat_s(string_to_add, MAX_KEY_ADD_LEN, key, strlen(key)));
        total_in += strlen(key);
        check_strncat_s(strncat_s(string_to_add, MAX_KEY_ADD_LEN, "=\'", strlen("=\'")));
        total_in += strlen("=\'");
        check_strncat_s(strncat_s(string_to_add, MAX_KEY_ADD_LEN, value, vallen));
        total_in += vallen;
        check_strncat_s(strncat_s(string_to_add, MAX_KEY_ADD_LEN, "\'", strlen("\'")));
        total_in += strlen("\'");
        Assert(total_in < MAX_KEY_ADD_LEN);
        if (encrypted_value_location && encrypted_value_size) {
            *query = (char *)libpq_realloc(*query, query_size, query_size + vallen + 1);
            if (*query == NULL) {
                return;
            }
            check_memset_s(memset_s(*query + query_size, vallen + 1, 0, vallen + 1));
            char *replace_dest = *query + encrypted_value_location + strlen("\'");
            char *move_src =
                *query + encrypted_value_location + encrypted_value_size + quote_num + strlen("\'");
            char *move_dest = *query + encrypted_value_location + vallen + strlen("\'");
            check_memmove_s(memmove_s(move_dest,
                query_size - encrypted_value_location - encrypted_value_size - strlen("\'") + 1,
                move_src,
                query_size - encrypted_value_location - encrypted_value_size - strlen("\'")));
            query_size = query_size + vallen - encrypted_value_size;
            check_memcpy_s(memcpy_s(replace_dest, query_size - encrypted_value_location, value, vallen));
        } else {
            check_strcat_s(strcat_s(string_to_add, MAX_KEY_ADD_LEN, ","));
            size_t string_to_add_size = strlen(string_to_add);
            *query = (char *)libpq_realloc(*query, query_size, query_size + string_to_add_size + 1);
            if (*query == NULL) {
                return;
            }
            check_memmove_s(memmove_s(*query + location + string_to_add_size, query_size - location, *query + location,
                query_size - location));
            query_size += string_to_add_size;
            check_memcpy_s(memcpy_s(*query + location, query_size - location, string_to_add, string_to_add_size));
        }
        query[0][query_size] = '\0';
    }
    return;
}

/*
 * create cmk
 * need to inital CLIENT MASTER KEY with CMK_KEY_STORE,GLOBAL_FUNCTION,CMK_KEY_PATH,CMK_ALGORITHM.
 * and their value must be unique.
 */
bool EncryptionPreProcess::run_pre_client_master_key(PGconn *conn, CreateClientLogicGlobal *client_logic_global,
    char **function_name, StringArgs &string_args)
{
    ListCell *global_param_iter(NULL);
    /* whether has Initialized CMK_KEY_STORE,GLOBAL_FUNCTION,CMK_KEY_PATH,CMK_ALGORITHM or not */
    bool is_duplicate_keystore(false);
    bool is_duplicate_keypath(false);
    bool is_duplicate_algorithmtype(false);
    foreach (global_param_iter, client_logic_global->global_setting_params) {
        ClientLogicGlobalParam *global_param = static_cast<ClientLogicGlobalParam *> lfirst(global_param_iter);
        switch (global_param->key) {
            case ClientLogicGlobalProperty::CLIENT_GLOBAL_FUNCTION:
                *function_name = global_param->value;
                if ((*function_name == NULL) || strlen(*function_name) == 0) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT) function name is missing\n"));
                    return false;
                }
                break;
            case ClientLogicGlobalProperty::CMK_KEY_STORE:
                if (is_duplicate_keystore) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): duplicate keyStore args\n"));
                    return false;
                }
                string_args.set("KEY_STORE", global_param->value);
                is_duplicate_keystore = true;
                break;
            case ClientLogicGlobalProperty::CMK_KEY_PATH:
                if (is_duplicate_keypath) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): duplicate keyPath args\n"));
                    return false;
                }
                string_args.set("KEY_PATH", global_param->value);
                is_duplicate_keypath = true;
                break;
            case ClientLogicGlobalProperty::CMK_ALGORITHM:
                if (is_duplicate_algorithmtype) {
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): duplicate algorithm args\n"));
                    return false;
                }
                string_args.set("ALGORITHM", global_param->value);
                is_duplicate_algorithmtype = true;
                break;
            default:
                break;
        }
    }
    return true;
}
