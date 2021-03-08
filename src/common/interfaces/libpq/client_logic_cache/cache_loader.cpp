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
 * cache_loader.cpp
 *	  
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cache_loader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <iostream>
#include "cache_loader.h"
#include "client_logic_common/client_logic_utils.h"
#include "cache_refresh_type.h"
#include "cached_column_setting.h"
#include "cached_column.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_hooks/global_hook_executor.h"
#include "client_logic_hooks/column_hook_executor.h"
#include "libpq-int.h"

/* static configuration variables initialization */
CacheLoader::CacheLoader() : m_is_first_fetch(true), m_compat_type(ORA_FORMAT), m_current_database_name{0} {}

CacheLoader &CacheLoader::get_instance()
{
    static CacheLoader instance;
    return instance;
}

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
    if (dots_count > 2) {   /* 2 means it like schema.table.col */
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
        } else if (dots_count == 2) {  /* 2 means it like schema.table.col */
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
    /*
     * search if there is more than 1 schema in search_path
     * if there is, comma will separate between schemas names
     */
    size_t search_path_size = strlen(search_path);
    size_t schema_start(0);
    size_t schema_end(0);
    for (size_t i = 0; i < search_path_size; ++i) {
        bool is_delimiter(false);

        /* check if end of string */
        if (i + 1 == search_path_size) {
            schema_end = i;
        } else if (search_path[i + 1] == ',') {
            /*
             * check for delimiter
             */
            is_delimiter = true;
            schema_end = i;
        } else {
            continue;
        }

        NameData schema;
        check_strncpy_s(
            strncpy_s(schema.data, sizeof(schema.data), search_path + schema_start, schema_end - schema_start + 1));
        if (pg_strcasecmp(schema.data, "\"$user\"") == 0) {
            m_search_path_list.add_user_schema(user_name);
        } else {
            m_search_path_list.add(schema.data);
        }

        /*
         * if there's a delimiter so there probably is another schema after the delimiter but let's make sure
         */
        if (is_delimiter && (i + 2 < search_path_size)) { /* 2 is the size of , + 1 */
            schema_start = i + 2;  /* 2 is the size of , + 1 */
        }
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
 * this functuion is to add b into search_list
 */
bool CacheLoader::add_temp_schema(PGconn *conn)
{
    /* get temp_schema from select */
    const char *query_temp =
        "SELECT distinct(pg_class_new.nspname) AS temp_schema FROM gs_encrypted_columns JOIN ( SELECT pg_class.oid, "
        "pg_class.relname, nc.nspname FROM   pg_class  JOIN pg_namespace nc ON (pg_class.relnamespace = nc.oid ) "
        "where pg_class.relpersistence= 't')  pg_class_new  ON ( gs_encrypted_columns.rel_id = pg_class_new.oid);";
    PGresult *res_temp_schema = NULL;
    conn->client_logic->disable_once = true;
    res_temp_schema = PQexec(conn, query_temp);
    /* check respones */
    if (res_temp_schema == NULL) {
        fprintf(stderr, "failed to get search path from server\n");
        return false;
    }
    if (res_temp_schema->resultStatus != PGRES_TUPLES_OK) {
        PQclear(res_temp_schema);
        printfPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("ERROR(CLIENT): failed to get search path for clientlogic cache\n"));
        fprintf(stderr, "failed to get search path from server\n");
        return false;
    }
    /* copy temp schema into m_search_list */
    int schema_num = PQfnumber(res_temp_schema, "temp_schema");

    for (int n = 0; n < PQntuples(res_temp_schema); n++) {
        const char *data = PQgetvalue(res_temp_schema, n, schema_num);
        m_search_path_list.add(data);
    }
    PQclear(res_temp_schema);
    return true;
}

/* get the search path, sql_compatibility, backslash_quote, standard_conforming_strings, escape_string_warning from the
 * server */
bool CacheLoader::fill_pgsettings(PGconn *conn)
{
    /* getting search path from server */
    const char *query = "select name, setting from pg_settings WHERE name  IN ('search_path', 'sql_compatibility',  "
        "'current_schema', 'backslash_quote', 'standard_conforming_strings', 'escape_string_warning');";

    PGresult *res = NULL;
    conn->client_logic->disable_once = true;
    res = PQexec(conn, query);
    if (!res) {
        fprintf(stderr, "failed to get search path from server\n");
        return false;
    }
    if (res->resultStatus != PGRES_TUPLES_OK) {
        PQclear(res);
        printfPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("ERROR(CLIENT): failed to get search path for clientlogic cache\n"));
        fprintf(stderr, "failed to get search path from server\n");
        return false;
    }
    int name_num = PQfnumber(res, "name");
    int setting_num = PQfnumber(res, "setting");

    for (int n = 0; n < PQntuples(res); n++) {
        const char *setting_name = PQgetvalue(res, n, name_num);
        /* search_path */
        if (strcmp(PQgetvalue(res, n, name_num), "search_path") == 0) {
            const char *search_path = PQgetvalue(res, n, setting_num);
            load_search_path(search_path, conn->client_logic->gucParams.role.c_str());
        } else if (strcmp(setting_name, "sql_compatibility") == 0) {
            /* sql_compatibility */
            const char *sql_compatibility = PQgetvalue(res, n, setting_num);
            if (pg_strcasecmp(sql_compatibility, "ORA") == 0) {
                m_compat_type = DatabaseType::ORA_FORMAT;
            } else {
                m_compat_type = DatabaseType::TD_FORMAT;
            }
        } else if (strcmp(setting_name, "backslash_quote") == 0) {
            /* backslash_quote */
            const char *data = PQgetvalue(res, n, setting_num);
            handle_back_slash_quote(data, conn);
        } else if (strcmp(setting_name, "standard_conforming_strings") == 0) {
            /* standard_conforming_strings */
            const char *data = PQgetvalue(res, n, setting_num);
            handle_conforming(data, conn);
        } else if (strcmp(setting_name, "escape_string_warning") == 0) {
            /* escape_string_warning */
            const char *data = PQgetvalue(res, n, setting_num);
            handle_escape_string(data, conn);
        }
    }
    PQclear(res);
    return true;
}

/* get global Setting from the server */
bool CacheLoader::fill_global_settings_map(PGconn *conn)
{
    m_global_settings_list.clear();
    PGresult *res = NULL;
    const char *global_settings_query =
        "SELECT gs_client_global_keys.Oid, gs_client_global_keys.global_key_name, pg_namespace.nspname, "
        "gs_client_global_keys_args.function_name, gs_client_global_keys_args.key, gs_client_global_keys_args.value "
        "FROM gs_client_global_keys JOIN pg_namespace ON (pg_namespace.Oid = gs_client_global_keys.key_namespace) "
        "JOIN gs_client_global_keys_args ON (gs_client_global_keys.Oid = gs_client_global_keys_args.global_key_id) "
        "ORDER BY gs_client_global_keys.Oid, gs_client_global_keys_args.function_name, gs_client_global_keys_args.key";
    conn->client_logic->disable_once = true;
    res = PQexec(conn, global_settings_query);
    if (res == NULL) {
        fprintf(stderr, "failed to get client master key from server\n");
        return false;
    }
    if (res->resultStatus != PGRES_TUPLES_OK) {
        PQclear(res);
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
            "ERROR(CLIENT): fill_global_settings_map - query to initialize client master keys did not return data\n"));
        return false;
    }

    int global_setting_oid_num = PQfnumber(res, "Oid");
    int global_key_name_num = PQfnumber(res, "global_key_name");
    int nspname_num = PQfnumber(res, "nspname");
    int function_name_num = PQfnumber(res, "function_name");
    int arg_key_num = PQfnumber(res, "key");
    int arg_value_num = PQfnumber(res, "value");

    CachedGlobalSetting *cached_global_setting(NULL);
    Oid object_oid_prev(0);
    for (int n = 0; n < PQntuples(res); n++) {
        Oid object_oid = (Oid)atoi(PQgetvalue(res, n, global_setting_oid_num));

        /* OBJECT NAME */
        const char *object_name = PQgetvalue(res, n, global_key_name_num);
        if (!object_name || strlen(object_name) == 0) {
            PQclear(res);
            fprintf(stderr, "failed to get namespace of client master key\n");
            return false;
        }

        /* OBJECT NAMESPACE */
        const char *object_name_space = PQgetvalue(res, n, nspname_num);
        if (!object_name_space || strlen(object_name_space) == 0) {
            PQclear(res);
            fprintf(stderr, "failed to get namespace of client master key\n");
            return false;
        }

        /* SCHEMA */
        m_schemas_list.add(object_name_space);

        bool is_new_object(false);
        if (!object_oid_prev || object_oid != object_oid_prev) {
            cached_global_setting =
                new (std::nothrow) CachedGlobalSetting(object_oid, get_database_name(), object_name_space, object_name);
            if (cached_global_setting == NULL) {
                PQclear(res);
                fprintf(stderr, "failed to allocate memory for client master key\n");
                return false;
            }
            is_new_object = true;
        }

        /* FUNCTION */
        if (is_new_object) {
            const char *function_name = PQgetvalue(res, n, function_name_num);
            if (!function_name || strlen(function_name) == 0) {
                PQclear(res);
                fprintf(stderr, "function name is missing\n");
                delete_key(cached_global_setting);
                return false;
            }
            cached_global_setting->set_global_hook_executor(
                HooksManager::GlobalSettings::create_global_hook_executor(function_name, *conn->client_logic));
        }
        if (!cached_global_setting->get_executor()) {
            PQclear(res);
            fprintf(stderr, "failed to get global hook executor\n");
            delete_key(cached_global_setting);
            return false;
        }

        /* ARGUMENT */
        const char *arg_key = PQgetvalue(res, n, arg_key_num);
        const char *arg_value = PQgetvalue(res, n, arg_value_num);
        if (!arg_key || strlen(arg_key) == 0 || !arg_value || strlen(arg_value) == 0) {
            PQclear(res);
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
    PQclear(res);
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
        "gs_column_keys_args.value "
        "FROM gs_column_keys  JOIN pg_namespace ON (pg_namespace.Oid = "
        "gs_column_keys.key_namespace) JOIN gs_column_keys_args "
        "ON (gs_column_keys.oid=gs_column_keys_args.column_key_id) "
        "ORDER BY gs_column_keys.Oid, gs_column_keys_args.function_name, "
        "gs_column_keys_args.key";
    conn->client_logic->disable_once = true;
    PGresult *res = PQexec(conn, query);
    if (!res) {
        fprintf(stderr, "fill_column_settings_info_cache - failed to get run query\n");
        return false;
    }
    if (res->resultStatus != PGRES_TUPLES_OK) {
        PQclear(res);
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext(
            "ERROR(CLIENT): fill_column_settings_info_cache - query to initialize did not return data\n"));
        fprintf(stderr, "fill_column_settings_info_cache - query to initialize did not return data\n");
        return false;
    }
    /* getting the index of each column in the response */
    int column_setting_oid_num = PQfnumber(res, "column_key_distributed_id");
    int column_key_name_num = PQfnumber(res, "column_key_name");
    int global_key_id_num = PQfnumber(res, "global_key_id");
    int arg_function_name_num = PQfnumber(res, "function_name");
    int arg_key_num = PQfnumber(res, "key");
    int arg_value_num = PQfnumber(res, "value");
    int nspname_num = PQfnumber(res, "nspname");

    CachedColumnSetting *column_setting(NULL);
    Oid object_oid_prev(0);
    m_column_settings_list.clear();
    for (int n = 0; n < PQntuples(res); n++) {
        /* OBJECT ID */
        Oid object_oid = atoi(PQgetvalue(res, n, column_setting_oid_num));

        /* OBJECT NAME */
        const char *object_name = PQgetvalue(res, n, column_key_name_num);

        /* OBJECT NAMESPACE */
        const char *object_name_space = PQgetvalue(res, n, nspname_num);
        if (!object_name_space || strlen(object_name_space) == 0) {
            PQclear(res);
            fprintf(stderr, "failed to get namespace of column encryption key\n");
            return false;
        }

        /* SCHEMA */
        m_schemas_list.add(object_name_space);

        /* CONSTRUCT NEW OBJECT */
        bool is_new_object(false);
        if (!object_oid_prev || object_oid != object_oid_prev) {
            column_setting =
                new (std::nothrow) CachedColumnSetting(object_oid, get_database_name(), object_name_space, object_name);
            if (column_setting == NULL) {
                PQclear(res);
                fprintf(stderr, "failed to allocate memory for column encryption key\n");
                return false;
            }
            is_new_object = true;
        }

        /* FUNCTION */
        if (is_new_object) {
            const char *function_name = PQgetvalue(res, n, arg_function_name_num);
            if (!function_name || strlen(function_name) == 0) {
                fprintf(stderr, "failed to get related client master key when loading column encryption key\n");
                delete_key(column_setting);
                continue;
            }

            /* get client master key object */
            Oid global_setting_oid = atoi(PQgetvalue(res, n, global_key_id_num));
            const CachedGlobalSetting *cached_global_setting = get_global_setting_by_oid(global_setting_oid);
            if (!cached_global_setting) {
                fprintf(stderr, "failed to get related client master key when loading column encryption key\n");
                delete_key(column_setting);
                continue;
            }
            check_strcat_s(strcat_s(column_setting->m_cached_global_setting, NAMEDATALEN * 4,
                cached_global_setting->get_fqdn())); /* 4 is the size of byte */

            /* create column hook executor */
            column_setting->set_executor(HooksManager::ColumnSettings::create_column_hook_executor(function_name,
                object_oid, cached_global_setting->get_executor()));
        }
        if (!column_setting->get_executor()) {
            PQclear(res);
            fprintf(stderr, "failed to get column hook executor\n");
            delete_key(column_setting);
            return false;
        }

        /* ARGUMENT */
        const char *arg_key = PQgetvalue(res, n, arg_key_num);
        const char *arg_value = PQgetvalue(res, n, arg_value_num);
        if (!arg_key || strlen(arg_key) == 0 || !arg_value || strlen(arg_value) == 0) {
            PQclear(res);
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
    PQclear(res);
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
    PGresult *res = NULL;
    const char *query = "SELECT gs_encrypted_columns.rel_id AS table_oid,current_database() AS table_catalog,	"
                        "pg_class_new.nspname AS "
        "table_schema,	pg_class_new.relname AS table_name,       gs_encrypted_columns.column_name,	a.attnum AS "
        "ordinal_position, a.atttypid, gs_encrypted_columns.data_type_original_oid,       "
        "gs_encrypted_columns.data_type_original_mod,       gs_column_keys.column_key_distributed_id AS "
        "column_setting_oid FROM   gs_encrypted_columns       RIGHT JOIN gs_column_keys               ON ( "
        "gs_encrypted_columns.column_key_id =                               gs_column_keys.oid )      JOIN (SELECT "
        "pg_class.oid,                         pg_class.relname,		         nc.nspname                  FROM   "
        "pg_class  JOIN pg_namespace nc ON (pg_class.relnamespace = nc.oid ))                                          "
        "          pg_class_new              ON ( gs_encrypted_columns.rel_id = pg_class_new.oid)	LEFT JOIN "
        "pg_attribute a "
        "ON (pg_class_new.oid = a.attrelid and gs_encrypted_columns.column_name  = attname);";
    conn->client_logic->disable_once = true;
    res = PQexec(conn, query);
    if (res == NULL) {
        return false;
    }
    if (res->resultStatus != PGRES_TUPLES_OK) {
        PQclear(res);
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): fill_cached_columns - query to initialize did not return data\n"));
        fprintf(stderr, "fill_cached_columns - query to initialize did not return data\n");
        return false;
    }

    /*  getting the index of each column in the response */
    int column_setting_oid_num = PQfnumber(res, "column_setting_oid");
    int table_oid_num = PQfnumber(res, "table_oid");
    int table_catalog_num = PQfnumber(res, "table_catalog");
    int table_schema_num = PQfnumber(res, "table_schema");
    int table_name_num = PQfnumber(res, "table_name");
    int column_name_num = PQfnumber(res, "column_name");
    int orig_type_oid_num = PQfnumber(res, "data_type_original_oid");
    int orig_type_mod_num = PQfnumber(res, "data_type_original_mod");
    int ordinal_position_num = PQfnumber(res, "ordinal_position");
    int atttypid_num = PQfnumber(res, "atttypid");
    m_columns_list.clear();
    for (int n = 0; n < PQntuples(res); n++) {
        Oid table_oid = (Oid)atoll(PQgetvalue(res, n, table_oid_num));
        const char *database_name = PQgetvalue(res, n, table_catalog_num);
        const char *schema_name = PQgetvalue(res, n, table_schema_num);
        const char *table_name = PQgetvalue(res, n, table_name_num);
        const char *column_name = PQgetvalue(res, n, column_name_num);
        int column_position = atoi(PQgetvalue(res, n, ordinal_position_num));
        Oid data_type_oid = (Oid)atoi(PQgetvalue(res, n, orig_type_oid_num));
        int data_type_mod = atoi(PQgetvalue(res, n, orig_type_mod_num));

        CachedColumn *cached_column = new (std::nothrow) CachedColumn(table_oid, database_name, schema_name, table_name,
            column_name, column_position, data_type_oid, data_type_mod);
        if (cached_column == NULL) {
            PQclear(res);
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        cached_column->init();
        cached_column->set_data_type(atoi(PQgetvalue(res, n, atttypid_num)));
        m_schemas_list.add(PQgetvalue(res, n, table_schema_num));
        /* column meta data */
        Oid column_setting_oid =
            PQgetvalue(res, n, column_setting_oid_num) ? atoll(PQgetvalue(res, n, column_setting_oid_num)) : 0;

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

    PQclear(res);
    return true;
}

bool CacheLoader::fetch_variables(PGconn *conn, CacheRefreshType cache_refresh_type)
{
    /*  load cache for "search path" data */
    if (!fill_pgsettings(conn)) {
        return false;
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
    if (cache_refresh_type == CacheRefreshType::NONE && m_is_first_fetch) {
        return true;
    }

    /* reset the variable so we don't fetch twice by mistake */
    conn->client_logic->cacheRefreshType = CacheRefreshType::NONE;

    if (m_is_first_fetch) {
        /*
         * first time the cache is loaded. The following functions are loading to the cache only one time.
         * These variables are updated dynamically by parsing the SET statements so we only need to query the catalog
         * tables once.
         */
        if (!fetch_variables(conn, cache_refresh_type)) {
            return false;
        }
    }
    m_is_first_fetch = false;

    /* load temp schema */
    if ((cache_refresh_type & CacheRefreshType::SEARCH_PATH) == CacheRefreshType::SEARCH_PATH) {
        if (!add_temp_schema(conn)) {
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
        if (!fill_cached_columns(conn)) {
            return false;
        }
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
