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
 * cache_loader.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_cache\cache_loader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <iostream>
#include <stdlib.h>
#include "cache_loader.h"
#include "client_logic_common/client_logic_utils.h"
#include "cache_refresh_type.h"
#include "cached_column_setting.h"
#include "cached_column.h"
#include "proc_list.h"
#include "cached_proc.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_hooks/global_hook_executor.h"
#include "client_logic_hooks/column_hook_executor.h"
#include "client_logic_data_fetcher/data_fetcher_manager.h"
#include "client_logic_data_fetcher/data_fetcher.h"
#include "libpq-int.h"
#include "cached_type.h"
/* static configuration variables initialization */
CacheLoader::CacheLoader() : m_is_first_fetch(true), m_compat_type(ORA_FORMAT), m_current_database_name { 0 } {}

bool CacheLoader::has_global_setting() const
{
    return !m_global_settings_list.empty();
}

bool CacheLoader::has_column_setting() const
{
    return !m_column_settings_list.empty();
}

bool CacheLoader::has_cached_columns() const
{
    return !m_columns_list.empty();
}

bool CacheLoader::is_schema_exists(const char *schema_name) const
{
    return m_schemas_list.exists(schema_name);
}

bool CacheLoader::remove_schema(const char *schema_name)
{
    return m_schemas_list.remove(schema_name);
}

const GlobalHookExecutor **CacheLoader::get_global_hook_executors(size_t &global_hook_executors_size) const
{
    return m_global_settings_list.get_executors(global_hook_executors_size);
}

const CachedGlobalSetting *CacheLoader::get_global_setting_by_fqdn(const char *globalSettingFqdn) const
{
    return m_global_settings_list.get_by_fqdn(globalSettingFqdn);
}

const CachedGlobalSetting *CacheLoader::get_global_setting_by_oid(Oid global_setting_oid) const
{
    return m_global_settings_list.get_by_oid(global_setting_oid);
}

/*  Get ColumnHookExecutor by oid */
ColumnHookExecutor *CacheLoader::get_column_hook_executor(Oid column_setting_oid) const
{
    ColumnHookExecutor *column_hook_executor = NULL;
    const CachedColumnSetting *cached_column_setting = m_column_settings_list.get_by_oid(column_setting_oid);
    if (cached_column_setting != NULL) {
        column_hook_executor = cached_column_setting->get_executor();
    }
    return column_hook_executor;
}

const CachedColumnSetting *CacheLoader::get_column_setting_by_fqdn(const char *column_setting_fqdn) const
{
    return m_column_settings_list.get_by_fqdn(column_setting_fqdn);
}

const CachedColumnSetting *CacheLoader::get_column_setting_by_oid(Oid oid) const
{
    return m_column_settings_list.get_by_oid(oid);
}

const CachedGlobalSetting **CacheLoader::get_global_settings_by_schema_name(const char *schema_name,
    size_t &global_settings_list_size) const
{
    return m_global_settings_list.get_by_schema(schema_name, global_settings_list_size);
}

const CachedColumnSetting **CacheLoader::get_column_setting_by_schema_name(const char *schema_name,
    size_t &column_settings_list_size) const
{
    return m_column_settings_list.get_by_schema(schema_name, column_settings_list_size);
}

const CachedColumnSetting **CacheLoader::get_column_setting_by_global_setting_fqdn(const char *global_setting_fqdn,
    size_t &column_settings_list_size) const
{
    return m_column_settings_list.get_by_global_setting(global_setting_fqdn, column_settings_list_size);
}

/* Proc GETTERS */
const CachedProc *CacheLoader::get_proc_by_oid(Oid procOid) const
{
    return m_proc_list.get_by_oid(procOid);
}

CachedProc *CacheLoader::get_proc_by_details(const char *database_name, const char *schema_name,
    const char *proc_name) const
{
    if (!proc_name) {
        return NULL;
    }
    /*
        use database from query or the current database connected to
    */
    if (!database_name || strlen(database_name) == 0) {
        database_name = m_current_database_name.data;
    }
    if (!database_name || strlen(database_name) == 0) {
        return NULL;
    }

    if (schema_name && strlen(schema_name)) {
        return m_proc_list.get_by_details(database_name, schema_name, proc_name);
    }
    CachedProc *cached_proc = m_proc_list.get_by_details(database_name, "pg_catalog", proc_name);
    if (cached_proc != NULL) {
        return cached_proc;
    }
    /*
        look for the function in the SEARCH_PATH
    */
    size_t search_path_list_size = m_search_path_list.size();
    for (size_t i = 0; i < search_path_list_size; ++i) {
        schema_name = m_search_path_list.at(i);
        CachedProc *cached_proc = m_proc_list.get_by_details(database_name, schema_name, proc_name);
        if (cached_proc != NULL) {
            return cached_proc;
        }
    }
    return NULL;
}

/*
 * this function evalutes the full FQDN name (schema.object) based on,
 * 1. the full fqdn of the object was supplied in input
 * 2. the object's existance is evaluted in a loop couplied with the SEARCH_PATH array
 */
size_t CacheLoader::get_object_fqdn(const char *object_name, const bool is_global_setting, char *object_fqdn) const
{
    /* count how many dots we have in the object name and by which know if we have the fqdn or not */
    size_t dots_count(0);
    size_t object_name_size = strlen(object_name);
    for (size_t i = 0; i < object_name_size; ++i) {
        if (object_name[i] == '.') {
            ++dots_count;
        }
    }
    if (dots_count > 2) { /* 2 means it like schema.table.col */
        Assert(false);
        return 0;
    }

    object_fqdn[0] = '\0';
    if (dots_count > 0) {
        /* check if we don't have the full fqdn but the schema.object syntax instead */
        if (dots_count == 1) {
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, m_current_database_name.data,
                strlen(m_current_database_name.data)));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, ".", 1));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, object_name, strlen(object_name)));
        } else if (dots_count == 2) { /* 2 means it like schema.table.col */
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, object_name, strlen(object_name)));
        }

        /* now we have the full fqdn (database.schema.object) */
        if (is_global_setting) {
            if (get_global_setting_by_fqdn(object_fqdn) == NULL) {
                return 0;
            }
        } else {
            if (m_column_settings_list.get_by_fqdn(object_fqdn) == NULL) {
                return 0;
            }
        }
    } else {
        /*
         * short version of the object name was supplied (not FQDN and without the schema name)
         * looping over search paths to find an exact match
         */
        bool is_found(false);
        size_t search_path_list_size = m_search_path_list.size();
        for (size_t i = 0; i < search_path_list_size; ++i) {
            const char *search_path = m_search_path_list.at(i);
            /*
             * check if the schema in the possible SEARCH_PATH has any client-logic objects in it.
             * if it doesn't, then it wouldn't appear in the schemas_list
             */
            if (!m_schemas_list.exists(search_path)) {
                continue;
            }
            object_fqdn[0] = '\0';
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, m_current_database_name.data,
                strlen(m_current_database_name.data)));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, ".", 1));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, search_path, strlen(search_path)));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, ".", 1));
            check_strncat_s(strncat_s(object_fqdn, m_FQDN_MAX_SIZE, object_name, strlen(object_name)));
            if (is_global_setting) {
                if (get_global_setting_by_fqdn(object_fqdn) != NULL) {
                    is_found = true;
                    break;
                }
            } else {
                if (m_column_settings_list.get_by_fqdn(object_fqdn) != NULL) {
                    is_found = true;
                    break;
                }
            }
        }
        if (!is_found) {
            return 0;
        }
    }

    return strlen(object_fqdn);
}
/* load m_search_path_list which is the schema list that will be searched when getting CacheColums */
void CacheLoader::load_search_path(const char *search_path, const char *user_name)
{
    m_search_path_list.clear();
 
    const char *paths;
    const char *path;
    size_t pathlen;
    char tmp[NAMEDATALEN] = {0};

    if (search_path == NULL) {
        return;
    }

    /*
     * spilit paths into several path with ','
     * strip whitespace at both head and tail of each path
     * we are consistent with the back-end who calls SplitIdentifierString()
     *
     * e.g.
     *		spilit "a,b,, , c,d ,  e   "
     * 		into   {"a", "b", "c", "d", "e"}
     */
    paths = search_path;
    path = paths;
    pathlen = 0;
    for (; paths[0] != '\0'; paths++) {
        if (paths[0] != ',') {
            pathlen++;
            if (paths[1] != '\0') {
                continue;
            }
        }

        for (; path[0] == ' '; pathlen--, path++) {}; /* skip whitespace at the head */
        if (pathlen != 0) {
            for (; path[pathlen - 1] == ' '; pathlen--) {}; /* skip whitespace at the tail */
            if (pathlen != 0) {
                check_strncpy_s(strncpy_s(tmp, NAMEDATALEN, path, pathlen));
                if (pg_strcasecmp(tmp, "\"$user\"") == 0) {
                    m_search_path_list.add_user_schema(user_name);
                } else {
                    m_search_path_list.add(tmp);
                }
            }
        }

        check_memset_s(memset_s(tmp, NAMEDATALEN, 0, NAMEDATALEN));
        path = paths + 1;
        pathlen = 0;
    }
}

/* check the response if ok */
static void handle_conforming(const char *sval, PGconn *conn)
{
    if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 || pg_strcasecmp(sval, "yes") == 0 ||
        pg_strcasecmp(sval, "1") == 0) {
        conn->client_logic->gucParams.standard_conforming_strings = true;
    } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 || pg_strcasecmp(sval, "no") == 0 ||
        pg_strcasecmp(sval, "0") == 0) {
        conn->client_logic->gucParams.standard_conforming_strings = false;
    }
}
/* check the BACKSLASH if ok */
static void handle_back_slash_quote(const char *sval, PGconn *conn)
{
    if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 || pg_strcasecmp(sval, "yes") == 0 ||
        pg_strcasecmp(sval, "1") == 0) {
        conn->client_logic->gucParams.backslash_quote = BACKSLASH_QUOTE_ON;
    } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 || pg_strcasecmp(sval, "no") == 0 ||
        pg_strcasecmp(sval, "0") == 0) {
        conn->client_logic->gucParams.backslash_quote = BACKSLASH_QUOTE_OFF;
    } else if (pg_strcasecmp(sval, "safe_encoding") == 0) {
        conn->client_logic->gucParams.backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;
    }
}
/* check the BACKSLASH if ok */
static void handle_escape_string(const char *sval, PGconn *conn)
{
    if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 || pg_strcasecmp(sval, "yes") == 0 ||
        pg_strcasecmp(sval, "1") == 0) {
        conn->client_logic->gucParams.escape_string_warning = true;
    } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 || pg_strcasecmp(sval, "no") == 0 ||
        pg_strcasecmp(sval, "0") == 0) {
        conn->client_logic->gucParams.escape_string_warning = false;
    }
}

/*
 * when create a new temp table a, a is created in a temporary schema b, so b is not in the search_list.
 * when searching a, CacheColumns can't be found with search_list.
 * this function is to add b into search_list
 */
bool CacheLoader::add_temp_schema(PGconn *conn)
{
    /* get temp_schema from select */
    const char *query_temp =
        "SELECT distinct(pg_class_new.nspname) AS temp_schema FROM gs_encrypted_columns JOIN ( SELECT pg_class.oid, "
        "pg_class.relname, nc.nspname FROM   pg_class  JOIN pg_namespace nc ON (pg_class.relnamespace = nc.oid ) "
        "where pg_class.relpersistence= 't')  pg_class_new  ON ( gs_encrypted_columns.rel_id = pg_class_new.oid);";
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query_temp)) {
        printfPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("ERROR(CLIENT): failed to get search path for clientlogic cache\n"));
        return false;
    }
    /* copy temp schema into m_search_list */
    int schema_num = data_fetcher.get_column_index("temp_schema");

    while (data_fetcher.next()) {
        const char *data = data_fetcher[schema_num];
        m_search_path_list.add(data);
    }
    return true;
}

/* get the current_user from server */
bool CacheLoader::get_current_user(PGconn * const conn) const 
{
    const char *query_current_user = "SELECT current_user;";

    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query_current_user)) {
        fprintf(stderr, "failed to get current_user from server\n");
        return false;
    }

    int user_index = data_fetcher.get_column_index("current_user");
    while (data_fetcher.next()) {
        const char *current_user = data_fetcher[user_index];
        conn->client_logic->gucParams.role = std::string(current_user);
    }

    return true;
}

/* get the search path, sql_compatibility, backslash_quote, standard_conforming_strings, escape_string_warning from the
 * server */
bool CacheLoader::fill_pgsettings(PGconn *conn)
{
    /* getting search path from server */
    const char *query = "select name, setting from pg_settings WHERE name  IN ('search_path', 'sql_compatibility',  "
        "'current_schema', 'backslash_quote', 'standard_conforming_strings', 'escape_string_warning');";

    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        printfPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("ERROR(CLIENT): failed to get search path for clientlogic cache\n"));
        return false;
    }
    int name_num = data_fetcher.get_column_index("name");
    int setting_num = data_fetcher.get_column_index("setting");

    while (data_fetcher.next()) {
        const char *setting_name = data_fetcher[name_num];
        /* search_path */
        if (strcmp(setting_name, "search_path") == 0) {
            const char *search_path = data_fetcher[setting_num];
            load_search_path(search_path, conn->client_logic->gucParams.role.c_str());
        } else if (strcmp(setting_name, "sql_compatibility") == 0) {
            /* sql_compatibility */
            const char *sql_compatibility = data_fetcher[setting_num];
            if (pg_strcasecmp(sql_compatibility, "ORA") == 0) {
                m_compat_type = DatabaseType::ORA_FORMAT;
            } else {
                m_compat_type = DatabaseType::TD_FORMAT;
            }
        } else if (strcmp(setting_name, "backslash_quote") == 0) {
            /* backslash_quote */
            const char *data = data_fetcher[setting_num];
            handle_back_slash_quote(data, conn);
        } else if (strcmp(setting_name, "standard_conforming_strings") == 0) {
            /* standard_conforming_strings */
            const char *data = data_fetcher[setting_num];
            handle_conforming(data, conn);
        } else if (strcmp(setting_name, "escape_string_warning") == 0) {
            /* escape_string_warning */
            const char *data = data_fetcher[setting_num];
            handle_escape_string(data, conn);
        }
    }
    return true;
}

/* get global Setting from the server */
bool CacheLoader::fill_global_settings_map(PGconn *conn)
{
    m_global_settings_list.clear();
    const char *global_settings_query =
        "SELECT gs_client_global_keys.oid, gs_client_global_keys.global_key_name, pg_namespace.nspname, "
        "gs_client_global_keys_args.function_name, gs_client_global_keys_args.key, gs_client_global_keys_args.value, "
        "EXTRACT(EPOCH from gs_client_global_keys.create_date) as change_epoch "
        "FROM gs_client_global_keys JOIN pg_namespace ON (pg_namespace.Oid = gs_client_global_keys.key_namespace) "
        "JOIN gs_client_global_keys_args ON (gs_client_global_keys.Oid = gs_client_global_keys_args.global_key_id) "
        "ORDER BY gs_client_global_keys.Oid, gs_client_global_keys_args.function_name, gs_client_global_keys_args.key;";
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(global_settings_query)) {
        fprintf(stderr, "failed to get load global settings map\n");
        return false;
    }
    int global_setting_oid_num = data_fetcher.get_column_index("oid");
    int global_key_name_num = data_fetcher.get_column_index("global_key_name");
    int nspname_num = data_fetcher.get_column_index("nspname");
    int function_name_num = data_fetcher.get_column_index("function_name");
    int arg_key_num = data_fetcher.get_column_index("key");
    int arg_value_num = data_fetcher.get_column_index("value");
    int change_epoch_num = data_fetcher.get_column_index("change_epoch");
    CachedGlobalSetting *cached_global_setting(NULL);
    Oid object_oid_prev(0);
    while (data_fetcher.next()) {
        update_last_change_epoch(data_fetcher[change_epoch_num]);
        Oid object_oid = (Oid)atoi(data_fetcher[global_setting_oid_num]);

        /* OBJECT NAME */
        const char *object_name = data_fetcher[global_key_name_num];
        if (!object_name || strlen(object_name) == 0) {
            fprintf(stderr, "failed to get namespace of client master key\n");
            return false;
        }

        /* OBJECT NAMESPACE */
        const char *object_name_space = data_fetcher[nspname_num];
        if (!object_name_space || strlen(object_name_space) == 0) {
            fprintf(stderr, "failed to get namespace of client master key\n");
            return false;
        }

        /* SCHEMA */
        m_schemas_list.add(object_name_space);

        bool is_new_object(false);
        if (!object_oid_prev || object_oid != object_oid_prev) {
            cached_global_setting =
                new(std::nothrow) CachedGlobalSetting(object_oid, get_database_name(), object_name_space, object_name);
            if (cached_global_setting == NULL) {
                fprintf(stderr, "failed to allocate memory for client master key\n");
                return false;
            }
            is_new_object = true;
        }

        /* FUNCTION */
        if (is_new_object) {
            const char *function_name = data_fetcher[function_name_num];
            if (!function_name || strlen(function_name) == 0) {
                fprintf(stderr, "function name is missing\n");
                delete_key(cached_global_setting);
                return false;
            }

            GlobalHookExecutor *global_hook_executor = 
                HooksManager::GlobalSettings::create_global_hook_executor(function_name,  *conn->client_logic);
            if (global_hook_executor == NULL) {
                fprintf(stderr, "failed to create client master key\n");
                delete_key(cached_global_setting);
                return false;
            }
            cached_global_setting->set_global_hook_executor(global_hook_executor);
        }
        if (!cached_global_setting->get_executor()) {
            fprintf(stderr, "failed to get global hook executor\n");
            delete_key(cached_global_setting);
            return false;
        }

        /* ARGUMENT */
        const char *arg_key = data_fetcher[arg_key_num];
        const char *arg_value = data_fetcher[arg_value_num];
        if (!arg_key || strlen(arg_key) == 0 || !arg_value || strlen(arg_value) == 0) {
            fprintf(stderr, "failed to get argument for client master key\n");
            delete_key(cached_global_setting);
            return false;
        }
        cached_global_setting->get_executor()->add_argument(arg_key, arg_value);
        /* it's a new object */
        if (is_new_object) {
            m_global_settings_list.add(cached_global_setting);
        }
        object_oid_prev = object_oid;
    }
    return true;
}

/* get column Setting from the server */
bool CacheLoader::fill_column_settings_info_cache(PGconn *conn)
{
    m_column_settings_list.clear();
    const char *query = "SELECT  pg_namespace.nspname, gs_column_keys.column_key_distributed_id, "
        "gs_column_keys.column_key_name, "
        "gs_column_keys.global_key_id, "
        "gs_column_keys_args.function_name, gs_column_keys_args.key, "
        "gs_column_keys_args.value, "
        "EXTRACT(EPOCH from gs_column_keys.create_date) as change_epoch "
        "FROM gs_column_keys  JOIN pg_namespace ON (pg_namespace.Oid = "
        "gs_column_keys.key_namespace) JOIN gs_column_keys_args "
        "ON (gs_column_keys.oid=gs_column_keys_args.column_key_id) "
        "ORDER BY gs_column_keys.Oid, gs_column_keys_args.function_name, "
        "gs_column_keys_args.key;";
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
            "ERROR(CLIENT): fill_column_settings_info_cache - query to initialize did not return data\n"));
        return false;
    }
    /* getting the index of each column in the response */
    int column_setting_oid_num = data_fetcher.get_column_index("column_key_distributed_id");
    int column_key_name_num = data_fetcher.get_column_index("column_key_name");
    int global_key_id_num = data_fetcher.get_column_index("global_key_id");
    int arg_function_name_num = data_fetcher.get_column_index("function_name");
    int arg_key_num = data_fetcher.get_column_index("key");
    int arg_value_num = data_fetcher.get_column_index("value");
    int nspname_num = data_fetcher.get_column_index("nspname");
    int change_epoch_num = data_fetcher.get_column_index("change_epoch");

    CachedColumnSetting *column_setting(NULL);
    Oid object_oid_prev(0);
    m_column_settings_list.clear();
    while (data_fetcher.next()) {
        update_last_change_epoch(data_fetcher[change_epoch_num]);
        /* OBJECT ID */
        Oid object_oid = atoi(data_fetcher[column_setting_oid_num]);

        /* OBJECT NAME */
        const char *object_name = data_fetcher[column_key_name_num];

        /* OBJECT NAMESPACE */
        const char *object_name_space = data_fetcher[nspname_num];
        if (!object_name_space || strlen(object_name_space) == 0) {
            fprintf(stderr, "failed to get namespace of column encryption key\n");
            return false;
        }

        /* SCHEMA */
        m_schemas_list.add(object_name_space);

        /* CONSTRUCT NEW OBJECT */
        bool is_new_object(false);
        if (!object_oid_prev || object_oid != object_oid_prev) {
            column_setting =
                new(std::nothrow) CachedColumnSetting(object_oid, get_database_name(), object_name_space, object_name);
            if (column_setting == NULL) {
                fprintf(stderr, "failed to allocate memory for column encryption key\n");
                return false;
            }
            is_new_object = true;
        }

        /* FUNCTION */
        if (is_new_object) {
            const char *function_name = data_fetcher[arg_function_name_num];
            if (!function_name || strlen(function_name) == 0) {
                delete_key(column_setting);
                continue;
            }

            /* get client master key object */
            Oid global_setting_oid = atoi(data_fetcher[global_key_id_num]);
            const CachedGlobalSetting *cached_global_setting = get_global_setting_by_oid(global_setting_oid);
            if (!cached_global_setting) {
                delete_key(column_setting);
                continue;
            }
            check_strcat_s(strcat_s(column_setting->m_cached_global_setting, NAMEDATALEN * 4,
                cached_global_setting->get_fqdn())); /* 4 is the size of byte */

            /* create column hook executor */
            ColumnHookExecutor *column_hook_executor =
                HooksManager::ColumnSettings::create_column_hook_executor(function_name, 
                    object_oid, cached_global_setting->get_executor());
            if (column_hook_executor == NULL) {
                delete_key(column_setting);
                continue;
            }
            column_setting->set_executor(column_hook_executor);
        }
        if (!column_setting->get_executor()) {
            fprintf(stderr, "failed to get column hook executor\n");
            delete_key(column_setting);
            return false;
        }

        /* ARGUMENT */
        const char *arg_key = data_fetcher[arg_key_num];
        const char *arg_value = data_fetcher[arg_value_num];
        if (!arg_key || strlen(arg_key) == 0 || !arg_value || strlen(arg_value) == 0) {
            fprintf(stderr, "failed to get argument for column encryption key\n");
            delete_key(column_setting);
            return false;
        }
        column_setting->get_executor()->add_argument(arg_key, arg_value);

        if (is_new_object) {
            m_column_settings_list.add(column_setting);
        }
        object_oid_prev = object_oid;
    }
    return true;
}

/*
 * load cache for columns.
 * when drop schema, table. we need to refresh the CachedColumns in cache.
 */
bool CacheLoader::fill_cached_columns(PGconn *conn)
{
    if (m_column_settings_list.empty()) {
        return true;
    }
    const char *query = "SELECT gs_encrypted_columns.oid AS my_oid, gs_encrypted_columns.rel_id AS table_oid, "
        "current_database() AS table_catalog, pg_class_new.nspname AS "
        "table_schema,	pg_class_new.relname AS table_name, gs_encrypted_columns.column_name, a.attnum - "
        "( select count (*) from pg_attribute where attrelid = gs_encrypted_columns.rel_id  and attisdropped and "
        "attnum < a.attnum ) AS ordinal_position, a.atttypid, gs_encrypted_columns.data_type_original_oid, "
        "gs_encrypted_columns.data_type_original_mod, gs_column_keys.column_key_distributed_id AS column_setting_oid, "
		"EXTRACT(EPOCH from gs_encrypted_columns.create_date) as change_epoch "
		"FROM gs_encrypted_columns RIGHT JOIN gs_column_keys ON ( "
        "gs_encrypted_columns.column_key_id = gs_column_keys.oid ) JOIN (SELECT "
        "pg_class.oid, pg_class.relname, nc.nspname FROM "
        "pg_class  JOIN pg_namespace nc ON (pg_class.relnamespace = nc.oid ))                                          "
        "pg_class_new ON ( gs_encrypted_columns.rel_id = pg_class_new.oid) LEFT JOIN "
        "pg_attribute a "
        "ON (pg_class_new.oid = a.attrelid and gs_encrypted_columns.column_name = attname) "
        "order by table_oid, ordinal_position;";
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        fprintf(stderr, "fill_cached_columns query failed\n");
        return false;
    }
    /*  getting the index of each column in the response */
    int my_oid_num = data_fetcher.get_column_index("my_oid");
    int column_setting_oid_num = data_fetcher.get_column_index("column_setting_oid");
    int table_oid_num = data_fetcher.get_column_index("table_oid");
    int table_catalog_num = data_fetcher.get_column_index("table_catalog");
    int table_schema_num = data_fetcher.get_column_index("table_schema");
    int table_name_num = data_fetcher.get_column_index("table_name");
    int column_name_num = data_fetcher.get_column_index("column_name");
    int orig_type_oid_num = data_fetcher.get_column_index("data_type_original_oid");
    int orig_type_mod_num = data_fetcher.get_column_index("data_type_original_mod");
    int ordinal_position_num = data_fetcher.get_column_index("ordinal_position");
    int atttypid_num = data_fetcher.get_column_index("atttypid");
    int change_epoch_num = data_fetcher.get_column_index("change_epoch");
    m_columns_list.clear();
    while (data_fetcher.next()) {
        update_last_change_epoch(data_fetcher[change_epoch_num]);
        Oid my_oid = (Oid)atoll(data_fetcher[my_oid_num]);
        Oid table_oid = (Oid)atoll(data_fetcher[table_oid_num]);
        const char *database_name = data_fetcher[table_catalog_num];
        const char *schema_name = data_fetcher[table_schema_num];
        const char *table_name = data_fetcher[table_name_num];
        const char *column_name = data_fetcher[column_name_num];
        int column_position = atoi(data_fetcher[ordinal_position_num]);
        Oid data_type_oid = (Oid)atoi(data_fetcher[orig_type_oid_num]);
        int data_type_mod = atoi(data_fetcher[orig_type_mod_num]);

        CachedColumn *cached_column = new(std::nothrow) CachedColumn(my_oid, table_oid, database_name, schema_name,
            table_name, column_name, column_position, data_type_oid, data_type_mod);
        if (cached_column == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        cached_column->init();
        cached_column->set_data_type(atoi(data_fetcher[atttypid_num]));
        m_schemas_list.add(data_fetcher[table_schema_num]);
        /* column meta data */
        Oid column_setting_oid = data_fetcher[column_setting_oid_num] ? atoll(data_fetcher[column_setting_oid_num]) : 0;

        const CachedColumnSetting *cached_column_setting = m_column_settings_list.get_by_oid(column_setting_oid);
        if (cached_column_setting) {
            cached_column->add_executor(cached_column_setting->get_executor());
        } else {
            fprintf(stderr, "failed to retrieve column encryption key executor: %d\n", (int)column_setting_oid);
            continue;
        }

        /*  add to maps */
        m_columns_list.add(cached_column);
    }

    return true;
}

const bool CacheLoader::fill_cached_types(PGconn *conn)
{
    const char *query = "select pg_type.oid, current_database(), nspname ,typname from pg_type join pg_namespace on "
        "(typnamespace=pg_namespace.Oid) where typrelid in (Select rel_id from gs_encrypted_columns);";

    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        fprintf(stderr, "fill_cached_types - query to initialize failed or did not return data\n");
        return false;
    }
    int typid_num = data_fetcher.get_column_index("oid");
    int db_num = data_fetcher.get_column_index("current_database");
    int schema_num = data_fetcher.get_column_index("nspname");
    int typname_num = data_fetcher.get_column_index("typname");
    m_cached_types_list.clear();
    while (data_fetcher.next()) {
        Oid oid_value = (Oid)atoi(data_fetcher[typid_num]);
        CachedType *curr_type = new(std::nothrow) CachedType(oid_value, data_fetcher[typname_num],
            data_fetcher[schema_num], data_fetcher[db_num], conn->client_logic->m_cached_column_manager); 
        if (curr_type == NULL) {
            fprintf(stderr, "failed to new CachedType object\n");
            return false;
        }
        m_cached_types_list.add(curr_type);
    }
    return true;
}

bool CacheLoader::fill_cached_procs(PGconn *conn)
{
    const char *query = "select func_id, proargcachedcol, proallargtypes_orig,proname,"
        "pronargs,proargtypes,proallargtypes,proargnames,proargmodes,nspname,current_database() as dbname, "
        "EXTRACT(epoch from gs_proc.last_change) as change_epoch "
        "from gs_encrypted_proc gs_proc join pg_proc on gs_proc.func_id = pg_proc.oid "
        " join pg_namespace ON (pg_namespace.oid = pronamespace);";
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        fprintf(stderr, "fill_cached_procs - query to initialize failed or did not return data\n");
        return false;
    }
    // getting the index of each column in the response
    int func_oid_num = data_fetcher.get_column_index("func_id");
    int cached_col_num = data_fetcher.get_column_index("proargcachedcol");
    int alltypes_orig_num = data_fetcher.get_column_index("proallargtypes_orig");
    int proname_num = data_fetcher.get_column_index("proname");
    int pronargs_num = data_fetcher.get_column_index("pronargs");
    int proargtypes_num = data_fetcher.get_column_index("proargtypes");
    int proallargtypes_num = data_fetcher.get_column_index("proallargtypes");
    int proargmodes_num = data_fetcher.get_column_index("proargmodes");
    int proargnames_num = data_fetcher.get_column_index("proargnames");
    int dbname_num = data_fetcher.get_column_index("dbname");
    int schema_num = data_fetcher.get_column_index("nspname");
    int change_epoch_num = data_fetcher.get_column_index("change_epoch");
    m_proc_list.clear();
    while (data_fetcher.next()) {
        update_last_change_epoch(data_fetcher[change_epoch_num]);
        CachedProc *curr_proc = new(std::nothrow) CachedProc(conn);
        if (curr_proc == NULL) {
            fprintf(stderr, "failed to new CachedProc object\n");
            return false;
        }
        curr_proc->m_func_id = strtoul(data_fetcher[func_oid_num], NULL, NUMBER_BASE);
        curr_proc->m_proname = strdup(data_fetcher[proname_num]);
        curr_proc->m_pronargs = atoi(data_fetcher[pronargs_num]);
        curr_proc->m_dbname = strdup(data_fetcher[dbname_num]);
        curr_proc->m_schema_name = strdup(data_fetcher[schema_num]);

        if (data_fetcher[cached_col_num] != NULL && strlen(data_fetcher[cached_col_num]) > 0) {
            parse_oid_array(conn, data_fetcher[cached_col_num], &curr_proc->m_proargcachedcol);
        }
        if (data_fetcher[proargtypes_num] != NULL && strlen(data_fetcher[proargtypes_num]) > 0) {
            parse_oid_array(conn, data_fetcher[proargtypes_num], &curr_proc->m_proargtypes);
        }
        if (data_fetcher[alltypes_orig_num] != NULL && strlen(data_fetcher[alltypes_orig_num]) > 0) {
            curr_proc->m_nallargtypes =
                    parse_oid_array(conn, data_fetcher[alltypes_orig_num], &curr_proc->m_proallargtypes_orig);
        }
        if (data_fetcher[proallargtypes_num] != NULL && strlen(data_fetcher[proallargtypes_num]) > 0) {
            parse_oid_array(conn, data_fetcher[proallargtypes_num], &curr_proc->m_proallargtypes);
        }
        if (data_fetcher[proargnames_num] != NULL && strlen(data_fetcher[proargnames_num]) > 0) {
            curr_proc->m_nargnames =
                    parse_string_array(conn, data_fetcher[proargnames_num], &curr_proc->m_proargnames);
        }
        if (data_fetcher[proargmodes_num] != NULL && strlen(data_fetcher[proargmodes_num]) > 0) {
            parse_char_array(conn, data_fetcher[proargmodes_num], &curr_proc->m_proargmodes);
        }
        curr_proc->set_original_ids();

        // add to maps
        m_proc_list.add(curr_proc);
    }
    return true;
}

/**
 * Reloads the client logic cache only if the local timestamp
 * is earlier than the maximum timestmp on the server
 * @param conn database connection
 */
void CacheLoader::reload_cache_if_needed(PGconn *conn)
{
    double server_max_time_stamp = get_server_max_time_stamp(conn);
    if (get_local_max_time_stamp() < server_max_time_stamp) {
        conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
        fetch_catalog_tables(conn);
    }
}

/**
 * @return The timestmp of the latest configuration fetched
 */
double CacheLoader::get_local_max_time_stamp() const
{
    return m_change_epoch;
}

/**
 * @param conn connection to the database
 * @return the latest timestamp of client logic configuration on teh server
 */
double CacheLoader::get_server_max_time_stamp(const PGconn* const conn) const
{
    const char *query = "SELECT extract (epoch from create_date) FROM ("
			"SELECT max(M.create_date) as create_date "
			"FROM ("
			"SELECT max(create_date) as create_date FROM gs_encrypted_columns "
	        "UNION "
	        "SELECT max(create_date) as create_date FROM gs_column_keys "
	        "UNION "
	        "SELECT max(create_date) as create_date FROM gs_client_global_keys "
            "UNION "
	        "SELECT max(last_change) as create_date FROM gs_encrypted_proc "
	       ") AS M);";
    double result = -1; // initial value, means no global timestamp or error
    DataFetcher data_fetcher = conn->client_logic->m_data_fetcher_manager->get_data_fetcher();
    if (!data_fetcher.load(query)) {
        fprintf(stderr, "failed to server_max_time_stamp\n");
        return result;
    }
    while (data_fetcher.next()) {
        const char *data = data_fetcher[0];
        if (data != NULL && strlen (data) > 0) {
            result = atof(data);
        }
    }
    return result;
}

/**
 * Maintain m_change_epoch member with the latest client logic configuration on the server
 * @param time_since_epoc configuration timestamp
 */
void CacheLoader::update_last_change_epoch(const char *time_since_epoc)
{
    if (time_since_epoc == NULL || strlen(time_since_epoc) == 0) {
        return;
    }
    double value = atof(time_since_epoc);
    if (value > m_change_epoch) {
        m_change_epoch = value;
    }
}

bool CacheLoader::fetch_variables(PGconn *conn, CacheRefreshType cache_refresh_type)
{
    /*  load cache for "search path" data */
    if (!fill_pgsettings(conn)) {
        return false;
    }
    return true;
}

bool CacheLoader::load_client_logic_cache(CacheRefreshType cache_refresh_type, PGconn *conn)
{
    /* load temp schema */
    if ((cache_refresh_type & CacheRefreshType::SEARCH_PATH) == CacheRefreshType::SEARCH_PATH) {
        if (!fill_pgsettings(conn) || !add_temp_schema(conn)) {
            return false;
        }
    }

    /*  load cache for Global Settings */
    bool is_changed = false;
    if ((cache_refresh_type & CacheRefreshType::GLOBAL_SETTING) == CacheRefreshType::GLOBAL_SETTING) {
        if (!fill_global_settings_map(conn)) {
            return false;
        }
        is_changed = true;
    }

    /*  load cache for Column Settings */
    if (is_changed || (cache_refresh_type & CacheRefreshType::COLUMN_SETTING) == CacheRefreshType::COLUMN_SETTING) {
        if (!fill_column_settings_info_cache(conn)) {
            return false;
        }
        is_changed = true;
    } else {
        is_changed = false;
    }

    /* load cache for columns */
    if (is_changed || (cache_refresh_type & CacheRefreshType::COLUMNS) == CacheRefreshType::COLUMNS) {
        if (!fill_cached_columns(conn) || !fill_cached_types(conn)) {
            return false;
        }
    }

    // load cache for procedures */
    if (is_changed || (cache_refresh_type & CacheRefreshType::PROCEDURES) == CacheRefreshType::PROCEDURES) {
        if (!fill_cached_procs(conn)) {
            return false;
        }
    }
    return true;
}

/* according to the CacheRefreshType to update status */
bool CacheLoader::fetch_catalog_tables(PGconn *conn)
{
    Assert(conn->dbName != NULL);
    if (conn->dbName) {
        check_strncpy_s(strncpy_s(m_current_database_name.data, sizeof(m_current_database_name.data), conn->dbName,
            strlen(conn->dbName)));
    }

    /*
     * the first time we get here, the cacheRefreshType was filled by the ReadyForQuery
     * The value returned by the ReadyForQuery is NONE only if the CL catalog tables are empty
     * so if this is the first time we fetch the cache and the catalog tables are empty
     * so just return
     */
    CacheRefreshType cache_refresh_type = conn->client_logic->cacheRefreshType;
    if (cache_refresh_type == CacheRefreshType::CACHE_NONE && m_is_first_fetch) {
        return true;
    }

    /* reset the variable so we don't fetch twice by mistake */
    conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_NONE;

    if (m_is_first_fetch) {
        /* search_path depend on the parameter $user, update it when first time */
        if (!get_current_user(conn)) {
            return false;
        }

        /*
         * first time the cache is loaded. The following functions are loading to the cache only one time.
         * These variables are updated dynamically by parsing the SET statements so we only need to query the catalog
         * tables once.
         */
        if (!fetch_variables(conn, cache_refresh_type)) {
            return false;
        }
        m_is_first_fetch = false;
    }

    if (!load_client_logic_cache(cache_refresh_type, conn)) {
        return false;
    }

    return true;
}

DatabaseType CacheLoader::get_sql_compatibility() const
{
    return m_compat_type;
}

const char *CacheLoader::get_database_name() const
{
    return m_current_database_name.data;
}

const CachedColumn *CacheLoader::get_cached_column(Oid table_oid, int column_position) const
{
    return m_columns_list.get_by_oid(table_oid, column_position);
}

const CachedColumn *CacheLoader::get_cached_column(Oid oid) const
{
    return m_columns_list.get_by_oid(oid);
}
const CachedType *CacheLoader::get_type_by_oid(const Oid oid) const
{
    return m_cached_types_list.get_by_oid(oid);
}
/* get CacheColumn by database_name.schema_name.table_name.column_name */
const CachedColumn *CacheLoader::get_cached_column(const char *database_name, const char *schema_name,
    const char *table_name, const char *column_name) const
{
    if (!table_name || !column_name) {
        return NULL;
    }

    /* use database from query or the current database connected to */
    if (!database_name) {
        database_name = m_current_database_name.data;
    }
    if (!database_name || strlen(database_name) == 0) {
        return NULL;
    }

    if (schema_name) {
        return m_columns_list.get_by_details(database_name, schema_name, table_name, column_name);
    }

    /* look for the table in the SEARCH_PATH */
    size_t search_path_list_size = m_search_path_list.size();
    for (size_t i = 0; i < search_path_list_size; ++i) {
        schema_name = m_search_path_list.at(i);
        const CachedColumn *cached_column =
            m_columns_list.get_by_details(database_name, schema_name, table_name, column_name);
        if (cached_column != NULL) {
            return cached_column;
        }
    }
    return NULL;
}

/* get CacheColumns by database_name.schema_name.table_name and columns_list_size */
const ICachedColumn **CacheLoader::get_cached_columns(const char *database_name, const char *schema_name,
    const char *table_name, size_t &columns_list_size) const
{
    if (!table_name) {
        return NULL;
    }
    /* use database from query or the current database connected to */
    if (!database_name) {
        database_name = m_current_database_name.data;
    }
    if (!database_name || strlen(database_name) == 0) {
        return NULL;
    }

    if (schema_name) {
        return (const ICachedColumn **)m_columns_list.get_by_details(database_name, schema_name, table_name,
            columns_list_size);
    }

    /* look for the table in the SEARCH_PATH */
    size_t search_path_list_size = m_search_path_list.size();
    for (size_t i = 0; i < search_path_list_size; ++i) {
        schema_name = m_search_path_list.at(i);
        const CachedColumn **cached_columns =
            m_columns_list.get_by_details(database_name, schema_name, table_name, columns_list_size);
        if (cached_columns != NULL) {
            return (const ICachedColumn **)cached_columns;
        }
    }
    return NULL;
}

CacheLoader::~CacheLoader()
{
    clear();
}

void CacheLoader::clear()
{
    m_global_settings_list.clear();
    m_column_settings_list.clear();
    m_columns_list.clear();
    m_search_path_list.clear();
}
