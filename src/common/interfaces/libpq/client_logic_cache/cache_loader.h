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
 * cache_loader.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cache_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHE_LOADER_H
#define CACHE_LOADER_H

#include "cached_global_setting.h"
#include "cache_refresh_type.h"
#include "nodes/parsenodes_common.h"
#include "client_logic_common/table_full_name.h"
#include "schemas_list.h"
#include "column_settings_list.h"
#include "global_settings_list.h"
#include "columns_list.h"
#include "proc_list.h"
#include "cached_type_list.h"
#include "search_path_list.h"

#define delete_key(__p)      \
    do {                     \
        if ((__p) != NULL) { \
            delete((__p));     \
            (__p) = NULL;    \
        }                    \
    } while (0)

typedef struct pg_conn PGconn;
typedef unsigned int Oid;

/* Column Settings */
class CachedColumnSetting;

/* Cached Columns */
class ICachedColumn;

/* Column Hook Executor */
class ColumnHookExecutor;

class CacheLoader {
    /* *
     * @class CacheLoader
     * @brief class with response to query the server for filling the cache
     */
public:
    /* *
     * @brief fetching columns data from server to client's cache
     * @param conn current connection
     * @param cache_refresh_type flags defining what to fetche from server
     * @return boolean for success or failure
     */
    CacheLoader();
    ~CacheLoader();
    bool fetch_catalog_tables(PGconn *conn);
    void load_search_path(const char *search_path, const char *user_name);

    /* *
     * @brief checking if there is global settings in cache
     * @return true if any global settings is in the cache, false if not
     * useful function to test if we want to process query - in case no client master key in cache there is no need to
     * parse the query
     */
    bool has_global_setting() const;
    bool has_column_setting() const;
    bool has_cached_columns() const;

    /* *
     * @return server SQL compatibility as saved in client's cache
     */
    DatabaseType get_sql_compatibility() const;

    const CachedColumn *get_cached_column(Oid table_oid, int column_position) const;
    const CachedColumn *get_cached_column(const char *database_name, const char *schema_name, const char *table_name,
        const char *column_name) const;
    const CachedColumn* get_cached_column(Oid oid) const;
    const ICachedColumn **get_cached_columns(const char *database_name, const char *schema_name, const char *table_name,
        size_t &columns_list_size) const;

    /* *
     * @param sql_compat new sql compatibiltiy if was changed on server
     */
    inline void set_sql_compatibility(DatabaseType sql_compat);

    /* Global Setting GETTERS */
    const CachedGlobalSetting *get_global_setting_by_fqdn(const char *global_setting_fqdn) const;
    const CachedGlobalSetting *get_global_setting_by_oid(Oid global_setting_oid) const;
    const CachedGlobalSetting **get_global_settings_by_schema_name(const char *schema_name,
        size_t &global_settings_list_size) const;

    /* Column Setting GETTERS */
    const CachedColumnSetting *get_column_setting_by_fqdn(const char *column_setting_fqdn) const;
    const CachedColumnSetting *get_column_setting_by_oid(Oid column_setting_oid) const;
    const CachedColumnSetting **get_column_setting_by_schema_name(const char *schema_name,
        size_t &column_settings_list_size) const;
    const CachedColumnSetting **get_column_setting_by_global_setting_fqdn(const char *global_settinng_fqdn,
        size_t &column_settings_list_size) const;
    ColumnHookExecutor *get_column_hook_executor(Oid column_setting_oid) const;

    /* Proc GETTERS */
    const CachedProc* get_proc_by_oid(Oid procOid) const;
    CachedProc* get_proc_by_details(const char* database_name, const char* schema_name, const char* proc_name) const;
    /* type getter */
    const CachedType* get_type_by_oid(Oid typid) const;
    /* *
     * @brief get all column settings that were created by same client master key
     * @param globalSettingName FQDN of client master key
     * @return  all column settings depened on this global settings
     */
    size_t get_object_fqdn(const char *object_name, const bool is_global_setting, char *object_fqdn) const;

    const GlobalHookExecutor **get_global_hook_executors(size_t &global_hook_executors_size) const;
    void free_global_hook_executors(GlobalHookExecutor **global_hook_executors) const;

    bool is_schema_exists(const char *schema_name) const;
    bool remove_schema(const char *schema_name);
    const char *get_database_name() const;
    bool add_temp_schema(PGconn *conn);
    void set_user_schema(const char *user_name)
    {
        m_search_path_list.set_user_schema(user_name);
    }
    void clear();    
    void reload_cache_if_needed(PGconn *conn);

private:
    bool clear_global_settings();    

private:
    ColumnSettingsList m_column_settings_list;
    GlobalSettingsList m_global_settings_list;
    SchemasList m_schemas_list; /* < list of all schmeas that contain client logic values */
    ColumnsList m_columns_list;
    ProcList m_proc_list;
    CachedTypeList m_cached_types_list;
    SearchPathList m_search_path_list;

    bool fill_pgsettings(PGconn *conn);
    bool fill_global_settings_map(PGconn *conn);
    bool fill_cached_columns(PGconn *conn);
    bool fill_cached_procs(PGconn *conn);
    const bool fill_cached_types(PGconn *conn);
    bool fill_column_settings_info_cache(PGconn *conn);
    bool fetch_variables(PGconn *conn, CacheRefreshType cache_refresh_type);
    bool load_client_logic_cache(CacheRefreshType cache_refresh_type, PGconn *conn);
    bool get_current_user(PGconn * const conn) const;

    bool m_is_first_fetch;      /* < indicator if fetched at least once in the past any of the variables or the catalog
                                    tables from the server */
    DatabaseType m_compat_type; /* server SQL compatibility */
    NameData m_current_database_name;
    double m_change_epoch = 0; /* time stamp of the latest client logic configuration fetched from the server */
    void update_last_change_epoch(const char *time_since_epoc);
    double get_local_max_time_stamp() const;
    double get_server_max_time_stamp(const PGconn* const conn) const;

private:
    static const int m_FQDN_MAX_SIZE = NAMEDATALEN * 4;
    static const int NUMBER_BASE = 10;
};

#endif /* CACHE_LOADER_H */
