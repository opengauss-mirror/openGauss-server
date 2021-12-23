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
 * cached_column_manager.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_column_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_COLUMN_MANAGER_H
#define CACHED_COLUMN_MANAGER_H
#include "frontend_parser/datatypes.h"
#include "nodes/parsenodes_common.h"
#include "icached_column_manager.h"
#include "icached_columns.h"
#include "cached_proc.h"

class CacheLoader;
class CachedColumnSetting;
class ExprPartsList;

/**
 * @brief testing if column is relevant
 */
class CachedColumnManager : public ICachedColumnManager {
public:
    CachedColumnManager();
    ~CachedColumnManager();

    bool load_cache(PGconn *conn) override;
    void clear() override;
    void reload_cache_if_needed(PGconn *conn) override;
    const GlobalHookExecutor** get_global_hook_executors(size_t& global_hook_executors_size) const override;
    const CachedGlobalSetting* get_global_setting_by_fqdn(const char* globalSettingFqdn) const override;
    size_t get_object_fqdn(const char* object_name, bool is_global_setting, char* object_fqdn) const override;
    const CachedColumnSetting** get_column_setting_by_global_setting_fqdn(const char* global_setting_fqdn,
        size_t& column_settings_list_size) const override;
    bool remove_schema(const char* schema_name) override;
    void set_user_schema(const char* user_name) override;
    void load_search_path(const char* search_path, const char* username) override;
    ColumnHookExecutor* get_column_hook_executor(Oid columnSettingOid) const override;
    const CachedGlobalSetting** get_global_settings_by_schema_name(const char* schema_name,
        size_t& global_settings_list_size) const override;
    const CachedColumnSetting** get_column_setting_by_schema_name(const char* schemaName,
        size_t& column_settings_list_size) const override;
    const CachedColumnSetting* get_column_setting_by_fqdn(const char* columnSettingFqdn) const override;

	/**
	 * @brief getting columns information from cache by querying using its full name params
	 * @param insert_stmt Node in the parsetree contains InsertStmt
     * @param cached_columns return the cachecolumns used
	 * @return all columns in the queried table
	 */
    bool get_cached_columns(const InsertStmt *insert_stmt, ICachedColumns *cached_columns) const override;
    /**
     * @brief getting column information from cache by querying using its full name params
     * @param relation node representation of table
     * @return all columns in the queried table
     */
    bool get_cached_columns(const RangeVar *relation, ICachedColumns *cached_columns) const override;
	/**
	 * getting column information from cache by querying using its full name params
	 * @param update_stmt Node in the parsetree contains UpdateStmt
	 * @return all columns in the queried table in the set part of updateStmt
	 */
    bool get_cached_columns(const UpdateStmt *update_stmt, ICachedColumns *cached_columns) const override;
    bool get_cached_columns(const RangeVar *relation, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns) const override;
    /**
     * getting columns information from cache by querying using its full name params
     * @param delete_stmt Node in the parsetree contains DeleteStmt
     * @return all columns in the queried table
     */
    bool get_cached_columns(const DeleteStmt *delete_stmt, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns) const override;

    /**
     * @brief getting columns information from cache by querying using its full name params
     * @param copy_stmt Node in the parsetree contains CopyStmt
     * @return all columns in the queried table
     */
    bool get_cached_columns(const CopyStmt *copy_stmt, ICachedColumns *cached_columns) const override;

    /**
     * @brief getting  columns information from cache by querying using its full name params
     * @param select_stmt Node in the parsetree contains SelectStmt
     * @param where_expr_vec list of all expressions in where part of the query
     * @param is_operator_forbidden output if there is forbidden operator
     * @param cached_columns list that will contain all cached columns mentioned in select statment
     * @param filtered_cacehd_columns list that contain only columns that neeed to be processed
     * @return true in case of success, else false
     */
    bool get_cached_columns(const SelectStmt *select_stmt, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns,
        ICachedColumns *filtered_cached_columns) const override;
    /* *
     * @brief getting column information from cache by querying using its full name params
     * @param db_name  catalog name where the table contains this column is located
     * @param schema_name  schmea name where the table contains this column is located
     * @param table_name  table contains this ctolumn is located
     * @return shared pointer to all this table's columns information
     */
    bool get_cached_columns(const char *db_name, const char *schema_name, const char *table_name,
        ICachedColumns *cached_columns) const override;

    /**
     * @brief getting column information from cache by querying using table Oid and ordinal number
     * @param tid   OID of the table contains this column
     * @param cid  column ordinal # in table with OID=tid
     * @return shared pointer to column information
     */
    const ICachedColumn *get_cached_column(unsigned int tid, unsigned int cid) const override;

    /**
    * @brief getting column information from cache by querying cached column Oid
    * @param oid   OID of the cached column
    * @return shared pointer to column information
    */
    const ICachedColumn* get_cached_column(const Oid oid) const override;

	/**
     * @brief getting column information from cache by querying using its full name params
     * @param db_name  catalog name where the table contains this column is located
     * @param schema_name  schmea name where the table contains this column is located
     * @param table_name  table contains this column is located
     * @param column_name  column name
     * @return shared pointer to column information
     */
    const ICachedColumn *get_cached_column(const char *db_name, const char *schema_name, const char *table_name,
        const char *column_name) const override;
    const bool has_cached_columns(const char *db_name, const char *schema_name, const char *table_name) override;
    const bool is_schema_contains_objects(const char *schmea_name) override;
    ICachedColumn *create_cached_column(const char *name) override;
    const Oid get_cached_column_key_id(const char *column_key_name) override;
    const CachedColumnSetting *get_cached_column_setting_metadata(const Oid o) override;
    bool filter_cached_columns(const ICachedColumns *cached_columns, const ExprPartsList *where_exprs_list,
        bool &is_operator_Forbidden, ICachedColumns *new_cached_columns) const override;

    /*
     * @brief getting function information from cache by querying using its full name params
     * @param dbName  catalog name where the table contains this column is located
     * @param schemaName  schmea name where the table contains this column is located
     * @param function name
     * @return  pointer to column information
     */
    CachedProc* get_cached_proc(const char* db_name, const char* schema_name, const char* function_name) const override;
    const CachedProc* get_cached_proc(Oid funcOid) const override;
    const CachedType* get_cached_type(Oid typid) const override;

    /**
     * Constructor
     */
    /**
     * @brief checking if local cache contains any Global Settings
     * If nothing in cache, there is no need to run pre/post queries operations on other queries
     * @return true if there is at lease 1 in cache, false otherwise.
     */
    bool has_global_setting() override;

    /**
     * @brief checking if local cache contains any information about  colums.
     * If no column in cache, there is no need to run pre/post queries operations on other queries
     * @return true if there is at lease 1 column in cache, false otherwise.
     */
    const bool is_cache_empty() const override;

    DatabaseType get_sql_compatibility() const override;

private:
    bool expand_op(const List *operators, const ICachedColumn *ce) const;    
    const char *get_current_database() const;

private:
    CacheLoader* m_cache_loader;
};

#endif
