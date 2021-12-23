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
 * cached_column_manager.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_column_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include <algorithm>
#include "cached_column_setting.h"
#include "cached_column_manager.h"
#include "cache_loader.h"
#include "cached_column.h"
#include "cached_columns.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_expressions/column_ref_data.h"
#include "client_logic_expressions/expr_processor.h"
#include "client_logic_expressions/expr_parts_list.h"
#include "client_logic_hooks/hooks_manager.h"
#include "libpq-int.h"
#include "cached_type.h"
#define CHK_PARAM(p) Assert(p && p->relation);

CachedColumnManager::CachedColumnManager()
{
    m_cache_loader = new CacheLoader;
}

CachedColumnManager::~CachedColumnManager()
{
    m_cache_loader->clear();
    delete m_cache_loader;
}

void CachedColumnManager::clear()
{
    m_cache_loader->clear();
}

bool CachedColumnManager::has_global_setting()
{
    return m_cache_loader->has_global_setting();
}

const GlobalHookExecutor** CachedColumnManager::get_global_hook_executors(size_t& global_hook_executors_size) const
{
    return m_cache_loader->get_global_hook_executors(global_hook_executors_size);
}

size_t CachedColumnManager::get_object_fqdn(const char* object_name, bool is_global_setting, char* object_fqdn) const
{
    return m_cache_loader->get_object_fqdn(object_name, is_global_setting, object_fqdn);
}

const CachedGlobalSetting* CachedColumnManager::get_global_setting_by_fqdn(const char* globalSettingFqdn) const
{
    return m_cache_loader->get_global_setting_by_fqdn(globalSettingFqdn);
}

const CachedColumnSetting** CachedColumnManager::get_column_setting_by_global_setting_fqdn(
    const char* global_setting_fqdn, size_t& column_settings_list_size) const
{
    return m_cache_loader->get_column_setting_by_global_setting_fqdn(global_setting_fqdn, column_settings_list_size);
}

bool CachedColumnManager::remove_schema(const char* schema_name)
{
    return m_cache_loader->remove_schema(schema_name);
}

void CachedColumnManager::set_user_schema(const char* user_name) 
{ 
    m_cache_loader->set_user_schema(user_name);
}

void CachedColumnManager::load_search_path(const char* search_path, const char* username)
{
    m_cache_loader->load_search_path(search_path, username);
}

const CachedGlobalSetting** CachedColumnManager::get_global_settings_by_schema_name(const char* schema_name, 
    size_t& global_settings_list_size) const 
{
    return m_cache_loader->get_global_settings_by_schema_name(schema_name, global_settings_list_size);
}

/* retrieve column by table oid and column oid */
const ICachedColumn *CachedColumnManager::get_cached_column(unsigned int table_oid, unsigned int column_index) const
{
    return m_cache_loader->get_cached_column(table_oid, column_index);
}

/* retrieve column by cached col oid */
const ICachedColumn* CachedColumnManager::get_cached_column(const Oid oid) const
{
    return m_cache_loader->get_cached_column(oid);
}

/* get  CacheColumns with database,schema ,tablename and columnname by cacheloader */
const ICachedColumn *CachedColumnManager::get_cached_column(const char *db_name, const char *schema_name,
    const char *table_name, const char *column_name) const
{
    return m_cache_loader->get_cached_column(db_name, schema_name, table_name, column_name);
}

ColumnHookExecutor* CachedColumnManager::get_column_hook_executor(Oid columnSettingOid) const
{
    return m_cache_loader->get_column_hook_executor(columnSettingOid);
}

const CachedColumnSetting** CachedColumnManager::get_column_setting_by_schema_name(const char* schemaName,
    size_t& column_settings_list_size) const
{
    return m_cache_loader->get_column_setting_by_schema_name(schemaName, column_settings_list_size);
}

const CachedColumnSetting* CachedColumnManager::get_column_setting_by_fqdn(const char* columnSettingFqdn) const
{
    return m_cache_loader->get_column_setting_by_fqdn(columnSettingFqdn);
}


bool CachedColumnManager::get_cached_columns(const char *db_name, const char *schema_name, const char *table_name,
    ICachedColumns *cached_columns_list) const
{
    if (!cached_columns_list) {
        Assert(false);
        return false;
    }
    cached_columns_list->set_order(true);

    size_t columns_list_size(0);
    const ICachedColumn **cached_columns =
        m_cache_loader->get_cached_columns(db_name, schema_name, table_name, columns_list_size);
    for (size_t i = 0; i < columns_list_size; ++i) {
        cached_columns_list->push(cached_columns[i]);
    }
    libpq_free(cached_columns);
    return true;
}

/* judge if database.schema.tablename.columnname has CacheColumns */
const bool CachedColumnManager::has_cached_columns(const char *db_name, const char *schema_name, const char *table_name)
{
    CachedColumns cached_columns;
    if (!get_cached_columns(db_name, schema_name, table_name, &cached_columns)) {
        return false;
    }
    return !cached_columns.is_empty();
}

/* judge if schema in CacheLoader */
const bool CachedColumnManager::is_schema_contains_objects(const char *schema_name)
{
    return m_cache_loader->is_schema_exists(schema_name);
}

/*
 * get CacheColumns according to the insertstmt.relation
 * [insert into table] table is the insertstmt.relation
 */
bool CachedColumnManager::get_cached_columns(const InsertStmt *insert_stmt, ICachedColumns *cached_columns) const
{
    if (!cached_columns) {
        Assert(false);
        return false;
    }
    CHK_PARAM(insert_stmt);
    if (insert_stmt->cols == nullptr) {
        /*
         * insert statement includes all of the columns in the table
         * so return all of them from the cache
         * columns order is assumed to be correct in the cache
         */
        return CachedColumnManager::get_cached_columns(insert_stmt->relation->catalogname,
            insert_stmt->relation->schemaname, insert_stmt->relation->relname, cached_columns);
    } else {
        /* specific columns requested in the insert statement */
        ListCell *col_iter = nullptr;
        cached_columns->set_order(false);
        foreach(col_iter, insert_stmt->cols) {
            if (col_iter == nullptr) {
                fprintf(stderr, "not yet supported\n");
                continue;
            }
            ResTarget *column = (ResTarget *)lfirst(col_iter);
            if (column == nullptr) {
                fprintf(stderr, "not yet supported\n");
                continue;
            }
            const ICachedColumn *cached_column = get_cached_column(insert_stmt->relation->catalogname,
                insert_stmt->relation->schemaname, insert_stmt->relation->relname, column->name);
            cached_columns->push(cached_column);
        }
        return true;
    }
}

/*
 * get CacheColumns according to the CopyStmt
 * like insertstmt
 */
bool CachedColumnManager::get_cached_columns(const CopyStmt *copy_stmt, ICachedColumns *cached_columns) const
{
    if (!cached_columns) {
        Assert(false);
        return false;
    }
    CHK_PARAM(copy_stmt);
    if (copy_stmt->attlist == nullptr) {
        /*
         * insert statement includes all of the columns in the table
         * so return all of them from the cache
         * columns order is assumed to be corect in the cache
         */
        return get_cached_columns(copy_stmt->relation->catalogname,
            copy_stmt->relation->schemaname, copy_stmt->relation->relname, cached_columns);
    } else {
        /* specific columns requested in the insert statement */
        ListCell *col_iter = nullptr;
        cached_columns->set_order(false);
        foreach(col_iter, copy_stmt->attlist) {
            if (col_iter == nullptr) {
                fprintf(stderr, "not yet supported\n");
                continue;
            }
            ResTarget *column = (ResTarget *)lfirst(col_iter);
            if (column == nullptr) {
                fprintf(stderr, "not yet supported\n");
                continue;
            }
            const ICachedColumn *cached_column = get_cached_column(copy_stmt->relation->catalogname,
                copy_stmt->relation->schemaname, copy_stmt->relation->relname, column->name);
            cached_columns->push(cached_column);
        }
        return true;
    }
}

/*
 * judge if CacheLoader has configuration about schema and so on
 * according to the cnfiguration, CacheLoader find the target CacheColumns.
 */
const bool CachedColumnManager::is_cache_empty() const
{
    if (!m_cache_loader->has_global_setting()) {
        return true;
    }

    if (!m_cache_loader->has_column_setting()) {
        return true;
    }

    if (!m_cache_loader->has_cached_columns()) {
        return true;
    }

    return false;
}

/*
 * get columnsetting by schema.database.table
 * get columnsetting's id
 */
const Oid CachedColumnManager::get_cached_column_key_id(const char *column_key_name)
{
    const CachedColumnSetting *column_setting =
        m_cache_loader->get_column_setting_by_fqdn(column_key_name);
    if (column_setting) {
        return column_setting->get_oid();
    }
    return InvalidOid;
}

const CachedColumnSetting *CachedColumnManager::get_cached_column_setting_metadata(const Oid oid)
{
    return m_cache_loader->get_column_setting_by_oid(oid);
}

/*
 * get columnsetting by schema.database.table
 * build a new CacheColumn with columnSettingName
 * it's useful when create a new Column
 */
ICachedColumn *CachedColumnManager::create_cached_column(const char *column_key_name)
{
    /*
     * check if the ColumnSetting requested for the column exists
     * if it doesn't exist then the CREATE TABLE statement will fail
     */
    CachedColumn *cached_column(NULL);
    const CachedColumnSetting *column_setting =
        m_cache_loader->get_column_setting_by_fqdn(column_key_name);
    if (column_setting) {
        cached_column = new (std::nothrow) CachedColumn();
        if (cached_column == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            exit(EXIT_FAILURE);
        }
        cached_column->add_executor(column_setting->get_executor());
    } else {
        fprintf(stderr, "failed to find in cache: %s\n", column_key_name);
    }
    return cached_column;
}

/* get columnsetting by schema.database.table */
bool CachedColumnManager::get_cached_columns(const RangeVar *relation, ICachedColumns *cached_columns) const
{
    return get_cached_columns(relation->catalogname, relation->schemaname, relation->relname, cached_columns);
}

/*
 * get columnsetting by updatestmt->relation
 * [update table] table is update->relaiton
 */
bool CachedColumnManager::get_cached_columns(const UpdateStmt *update_stmt, ICachedColumns *cached_columns) const
{
    CHK_PARAM(update_stmt);
    if (!update_stmt) {
        Assert(false);
        return false;
    }
    cached_columns->set_order(false);

    /* specific columns requested in the set part of update statement */
    ListCell *col_iter = nullptr;
    foreach(col_iter, update_stmt->targetList) {
        /* following cases should not occur */
        if (col_iter == nullptr) {
            fprintf(stderr, "not yet supported\n");
            continue;
        }
        ResTarget *column = (ResTarget *)lfirst(col_iter);
        if (column == nullptr) {
            fprintf(stderr, "not yet supported\n");
            continue;
        }
        const ICachedColumn *cached_column = get_cached_column(update_stmt->relation->catalogname,
            update_stmt->relation->schemaname, update_stmt->relation->relname, column->name);
        cached_columns->push(cached_column);
    }
    return true;
}

/*
 * judge if the CacheColumn support some operation
 * like randomized encryption doesn't support set operation
 */
bool CachedColumnManager::expand_op(const List *operators, const ICachedColumn *ce) const
{
    const char * const op = strVal(linitial(operators));
    return HooksManager::is_operator_allowed(ce, op);
}

/*
 * select target CacheColumns according to the where expression among all CacheColumns
 * and judge target CacheColumns if support the operation or not
 */
bool CachedColumnManager::filter_cached_columns(const ICachedColumns *cached_columns,
    const ExprPartsList *where_exprs_list, bool &is_operator_forbidden, ICachedColumns *new_cached_columns) const
{
    new_cached_columns->set_order(false);
    /* iterate over the columns that are present in the query */
    size_t where_exprs_list_size = where_exprs_list->size();
    for (size_t i = 0; i < where_exprs_list_size; ++i) {
        bool is_found(false);
        const ExprParts *expr_part = where_exprs_list->at(i);

        if (expr_part->column_ref != NULL) { 
            /* get column definiation from [where a.col1 = ...] */
            ColumnRefData column_ref_data;
            if (!exprProcessor::expand_column_ref(expr_part->column_ref, column_ref_data)) {
                return false;
            }

            /*
            * iterate the CacehColumns to find the target CacheColumn for a.col1
            * jugde the target CacheColumn support the operation or not
            */
            size_t cached_columns_size = cached_columns->size();
            for (size_t i = 0; i < cached_columns_size; ++i) {
                const ICachedColumn *cached_column = cached_columns->at(i);
                if (cached_column &&
                    (pg_strcasecmp(cached_column->get_col_name(), column_ref_data.m_column_name.data) == 0)) {
                    is_operator_forbidden = !expand_op(expr_part->operators, cached_column);
                    if (is_operator_forbidden) {
                        return new_cached_columns; /* stopping early */
                    }
                    new_cached_columns->push(cached_column);
                    is_found = true;
                    break;
                }
            }
        }
        if (!is_found) {
            new_cached_columns->push(NULL);
        }
    }
    return true;
}

/*
 * get all CacheColumns by rangvar
 * filter target CacheColumns from all CacheColumns by where
 */
bool CachedColumnManager::get_cached_columns(const RangeVar *relation, const ExprPartsList *where_exprs_list,
    bool &is_operator_forbidden, ICachedColumns *cached_columns) const
{
    if (!relation) {
        Assert(false);
        return false;
    }

    CachedColumns tmp_cached_columns;
    if (!get_cached_columns(relation, &tmp_cached_columns)) {
        return false;
    }
    return filter_cached_columns(&tmp_cached_columns, where_exprs_list, is_operator_forbidden, cached_columns);
}

/*
 * get all CacheColumns by delete_stmt->relation
 * filter target CacheColumns from all CacheColumns by where
 */
bool CachedColumnManager::get_cached_columns(const DeleteStmt *delete_stmt, const ExprPartsList *where_exprs_list,
    bool &is_operator_forbidden, ICachedColumns *cached_columns) const
{
    CHK_PARAM(delete_stmt);
    if (!delete_stmt || !delete_stmt->relation) {
        Assert(false);
        return false;
    }

    /* get all CacheColumns by delete_stmt->relation */
    CachedColumns tmp_cached_columns;
    CachedColumns alias_cached_columns (false, true);
    CachedColumns filter_cached_columns_temp (false);
    if (!get_cached_columns(delete_stmt->relation->catalogname, delete_stmt->relation->schemaname,
        delete_stmt->relation->relname, &tmp_cached_columns)) {
        return false;
    }
    /*
     * delete from  table1 as a1 where a1.col1 = ...
     */
    for (size_t i = 0; i < tmp_cached_columns.size(); i++) {
        ICachedColumn *alias_cached = new (std::nothrow) CachedColumn(tmp_cached_columns.at(i));
        if (alias_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            exit(EXIT_FAILURE);
        }
        if (delete_stmt->relation->alias != NULL) {
            char *alias_name = delete_stmt->relation->alias->aliasname;
            alias_cached->set_table_name(alias_name);
        }
        cached_columns->push(alias_cached);
    }
    return true;
}

/*
 * get all CacheColumns by selectStmt->fromClause which is a, so it needs to be iterated
 * filter target CacheColumns from all CacheColumns by where
 */
bool CachedColumnManager::get_cached_columns(const SelectStmt *select_stmt, const ExprPartsList *where_exprs_list,
    bool &is_operator_forbidden, ICachedColumns *cached_columns, ICachedColumns *filtered_cached_columns) const
{
    if (!select_stmt) {
        Assert(false);
        return false;
    }

    if (!select_stmt->fromClause) {
        return true;
    }

    ListCell *from_iter = NULL;
    /* itertaing over from list to get list of tables */
    foreach(from_iter, select_stmt->fromClause) {
        Node *node = (Node *)lfirst(from_iter);
        if (IsA(node, RangeVar)) {
            RangeVar *rangeVar = (RangeVar *)node;

            /* get columns relevant to this table */
            CachedColumns tmp_cached_columns;
            if (!get_cached_columns(rangeVar->catalogname, rangeVar->schemaname, rangeVar->relname,
                &tmp_cached_columns)) {
                return false;
            }
            /* keep columns that should be processed in this query */
            CachedColumns tmp_filter_cached_columns;
            if (!filter_cached_columns(&tmp_cached_columns, where_exprs_list, is_operator_forbidden,
                &tmp_filter_cached_columns)) {
                return false;
            }

            /*
             * if parent query is set operation, we need to get all columns of this table
             * to be checked later if set operation is legal on those columns
             */
            cached_columns->append(&tmp_cached_columns);
            /* add filtered columns to the list */
            filtered_cached_columns->append(&tmp_filter_cached_columns);
        }
    }
    return true;
}

DatabaseType CachedColumnManager::get_sql_compatibility() const
{
    return m_cache_loader->get_sql_compatibility();
}

const char *CachedColumnManager::get_current_database() const
{
    return m_cache_loader->get_database_name();
}

bool CachedColumnManager::load_cache(PGconn *conn)
{
    return m_cache_loader->fetch_catalog_tables(conn);
}

/**
 * Reloads the client logic cache only if the local timestamp
 * is earlier than the maximum timestmp on the server
 * @param conn database connection
 */
void CachedColumnManager::reload_cache_if_needed(PGconn *conn)
{
    m_cache_loader->reload_cache_if_needed(conn);
}

CachedProc* CachedColumnManager::get_cached_proc(const char* dbName, const char* schemaName,
    const char* functionName) const
{
    return m_cache_loader->get_proc_by_details(dbName, schemaName, functionName);
}

const CachedProc* CachedColumnManager::get_cached_proc(Oid funcOid) const
{
    return m_cache_loader->get_proc_by_oid(funcOid);
}

const CachedType* CachedColumnManager::get_cached_type(Oid typid) const
{
    return m_cache_loader->get_type_by_oid(typid);
}
