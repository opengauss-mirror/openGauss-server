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
 * icached_column_manager.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\icached_column_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ICACHED_COLUMN_MANAGER_H
#define ICACHED_COLUMN_MANAGER_H

#include "frontend_parser/datatypes.h"
#include "nodes/parsenodes_common.h"
#include "icached_column_manager.h"
#include "icached_columns.h"

typedef struct pg_conn PGconn;

class ExprPartsList;
enum class CacheRefreshType;
class ICachedColumn;
class CachedColumnSetting;

/* *
 * @brief testing if column is relevant
 */
class ICachedColumnManager {
public:
    static ICachedColumnManager &get_instance();

    virtual ~ICachedColumnManager() = default;
    virtual bool load_cache(PGconn *conn) = 0;

    /* *
     * @brief getting columns information from cache by querying using its full name params
     * @param insert_stmt Node in the parsetree contains InsertStmt
     * @return all columns in the queried table
     */
    virtual bool get_cached_columns(const InsertStmt *insert_stmt, ICachedColumns *cached_columns) const = 0;
    /* *
     * @brief getting column information from cache by querying using its full name params
     * @param relation node representation of table
     * @return all columns in the queried table
     */
    virtual bool get_cached_columns(const RangeVar *relation, ICachedColumns *cached_columns) const = 0;
    /* *
     * getting column information from cache by querying using its full name params
     * @param update_stmt Node in the parsetree contains UpdateStmt
     * @return all columns in the queried table in the set part of updateStmt
     */
    virtual bool get_cached_columns(const UpdateStmt *update_stmt, ICachedColumns *cached_columns) const = 0;
    virtual bool get_cached_columns(const RangeVar *relation, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns) const = 0;

    /* *
     * getting columns information from cache by querying using its full name params
     * @param delete_stmt Node in the parsetree contains DeleteStmt
     * @return all columns in the queried table
     */
    virtual bool get_cached_columns(const DeleteStmt *delete_stmt, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns) const = 0;

    /* *
     * @brief getting columns information from cache by querying using its full name params
     * @param copy_stmt Node in the parsetree contains CopyStmt
     * @return all columns in the queried table
     */
    virtual bool get_cached_columns(const CopyStmt *copy_stmt, ICachedColumns *cached_columns) const = 0;

    /* *
     * @brief getting  columns information from cache by querying using its full name params
     * @param select_stmt Node in the parsetree contains SelectStmt
     * @param where_exprs_list list of all expressions in where part of the query
     * @param is_operator_forbidden output if there is forbidden operator
     * @param parentSetOperation set operation of parent query might force us return all columns
     * @return all true in case of success, else false
     */
    virtual bool get_cached_columns(const SelectStmt *select_stmt, const ExprPartsList *where_exprs_list,
        bool &is_operator_forbidden, ICachedColumns *cached_columns, ICachedColumns *filtered_cached_columns) const = 0;

    /* *
     * @brief getting column information from cache by querying using its full name params
     * @param db_name  catalog name where the table contains this column is located
     * @param schema_name  schmea name where the table contains this column is located
     * @param table_name  table contains this ctolumn is located
     * @return shared pointer to all this table's columns information
     */
    virtual bool get_cached_columns(const char *db_name, const char *schema_name, const char *table_name,
        ICachedColumns *cached_columns) const = 0;
    /* *
     * @brief getting column information from cache by querying using its full name params
     * @param db_name  catalog name where the table contains this column is located
     * @param schema_name  schmea name where the table contains this column is located
     * @param table_name  table contains this column is located
     * @param column_name  column name
     * @return shared pointer to column information
     */
    virtual const ICachedColumn *get_cached_column(const char *db_name, const char *schema_name, const char *table_name,
        const char *column_name) const = 0;
    /* *
     * @brief getting column information from cache by querying using table Oid and ordinal number
     * @param tid   OID of the table contains this column
     * @param cid  column ordinal # in table with OID=tid
     * @return shared pointer to column information
     */
    virtual const ICachedColumn *get_cached_column(unsigned int tid, unsigned int cid) const = 0;

    virtual const bool has_cached_columns(const char *db_name, const char *schema_name, const char *table_name) = 0;
    virtual ICachedColumn *create_cached_column(const char *name) = 0;
    virtual const bool is_schema_contains_objects(const char *schmeaname) = 0;
    virtual const Oid get_cached_column_key_id(const char *column_key_name) = 0;
    virtual const CachedColumnSetting *get_cached_column_setting_metadata(const Oid oid) = 0;
    virtual bool filter_cached_columns(const ICachedColumns *cached_columns, const ExprPartsList *where_exprs_list,
        bool &is_operator_Forbidden, ICachedColumns *new_cached_columns) const = 0;

    /* *
     * @brief checking if local cache contains any Global Settings
     * If nothing in cache, there is no need to run pre/post queries operations on other queries
     * @return true if there is at lease 1 in cache, false otherwise.
     */
    virtual bool has_global_setting() = 0;

    /* *
     * @brief checking if local cache contains any information about  colums.
     * If no column in cache, there is no need to run pre/post queries operations on other queries
     * @return true if there is at lease 1 column in cache, false otherwise.
     */
    virtual const bool is_cache_empty() const = 0;

    virtual DatabaseType get_sql_compatibility() const = 0;
};

#endif
