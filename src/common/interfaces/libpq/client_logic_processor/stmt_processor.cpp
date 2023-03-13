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
 * stmt_processor.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_processor\stmt_processor.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "pg_config.h"

#include "stmt_processor.h"
#include "client_logic_cache/icached_column_manager.h"
#include "client_logic_cache/icached_column.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_cache/cached_column.h"
#include "client_logic_cache/icached_columns.h"
#include "client_logic_cache/cache_loader.h"
#include "client_logic_cache/cached_column_setting.h"
#include "client_logic_cache/column_hook_executors_list.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_expressions/column_ref_data.h"
#include "client_logic/cstrings_map.h"
#include "raw_value.h"
#include "raw_values_cont.h"
#include "prepared_statement.h"
#include "prepared_statements_list.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "where_clause_processor.h"
#include "create_stmt_processor.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_fmt/gs_fmt.h"
#include "client_logic_fmt/gs_copy.h"
#include "values_processor.h"
#include "processor_utils.h"
#include "raw_values_list.h"
#include <algorithm>
#include "frontend_parser/Parser.h"
#include "nodes/feparser_memutils.h"
#include "client_logic_expressions/expr_processor.h"
#include "encryption_pre_process.h"
#include "client_logic_expressions/expr_parts_list.h"
#include "client_logic_expressions/pg_functions_support.h"
#include "catalog/pg_class.h"
#include "func_processor.h"

#include <iostream>

bool Processor::run_pre_update_statement_set(const UpdateStmt * const update_stmt, StatementData *statement_data)
{
    /* get column configuration from cache (for every column in query) */
    CachedColumns cached_columns;
    bool ret = statement_data->GetCacheManager()->get_cached_columns(update_stmt, &cached_columns);
    if (!ret || cached_columns.is_empty()) {
        return true;
    }

    /* get actual values from query */
    RawValuesList raw_values_list;
    if (!RawValues::get_raw_values_from_update_statement(update_stmt, statement_data, &raw_values_list)) {
        fprintf(stderr, "error getting raw valued from statment\n");
        return false;
    }

    if (!cached_columns.is_divisor(raw_values_list.size())) {
        fprintf(stderr, "columns and values do not match.\n");
        return false;
    }

    /* iterate on all of the values, and process if required by the configuration */
    return ValuesProcessor::process_values(statement_data, &cached_columns, 1, &raw_values_list); /* 1 : one row */
}

bool Processor::run_pre_update_statement_where(const UpdateStmt * const update_stmt, StatementData *statement_data)
{
    ExprPartsList where_expr_parts_list;
    if (!exprProcessor::expand_expr(update_stmt->whereClause, statement_data, &where_expr_parts_list)) {
        return false;
    }

    /* get all columns relevant for this query's where clause */
    bool is_op_forbidden(false);
    CachedColumns cached_columns;
    bool ret = statement_data->GetCacheManager()->get_cached_columns(update_stmt->relation, &where_expr_parts_list,
        is_op_forbidden, &cached_columns);
    if (is_op_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    if (!ret || cached_columns.is_empty()) {
        return true; // nothing to do
    }

    /* rewrite WHERE statement clause in the query */
    if (!WhereClauseProcessor::process(&cached_columns, &where_expr_parts_list, statement_data)) {
        return false;
    }
    return true;
}


bool Processor::run_pre_update_statement(const UpdateStmt * const update_stmt, 
    StatementData *statement_data, ICachedColumns* cached_columns)
{
    /* check if feature is used */
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    /* validate input */
    if (update_stmt == nullptr || update_stmt->relation == nullptr || update_stmt->targetList == nullptr) {
        fprintf(stderr, "wrong arguments list for update statment\n");
        return true;
    }

    if (!run_pre_update_statement_set(update_stmt, statement_data)) {
        return false;
    }
    if (!run_pre_update_statement_where(update_stmt, statement_data)) {
        return false;
    }
    /* update c1 set col1 =  ... returning [] */
    CachedColumns cached_column_temp;
    statement_data->GetCacheManager()->get_cached_columns(update_stmt->relation, &cached_column_temp);
    if (!run_pre_returning_list_statement(update_stmt->returningList, &cached_column_temp, cached_columns,
        statement_data)) {
        return false;
    }
    return true;
}

/* update/insert/delete returning []  the returning CacheColumn is useful for (with cte as... ) */
bool Processor::run_pre_returning_list_statement(const List *return_list, ICachedColumns *cached_columns_from,
    ICachedColumns *cached_columns, StatementData *statement_data)
{
    if (cached_columns != NULL && return_list != NULL && !cached_columns_from->is_empty()) {
        CStringsMap returning_list;
        if (!get_column_names_from_target_list(return_list, &returning_list, statement_data)) {
            return false;
        }
        for (size_t i = 0; i < cached_columns_from->size(); i++) {
            const ICachedColumn *cached_column = cached_columns_from->at(i);
            if (cached_column != NULL && find_in_name_map(returning_list, cached_column->get_col_name())) {
                CachedColumn *return_cached = new (std::nothrow) CachedColumn(cached_columns_from->at(i));
                if (return_cached == NULL) {
                    fprintf(stderr, "failed to new CachedColumn object\n");
                    return false;
                }
                cached_columns->push(return_cached);
            }
        }
    }
    return true;
}


bool Processor::run_pre_insert_statement(const InsertStmt * const insert_stmt, 
    StatementData *statement_data, ICachedColumns* cached_columns)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    RETURN_IF(insert_stmt == NULL, false);

    /* get column configuration from cache (for every column in query) */
    CachedColumns cached_columns_temp;
    bool ret = statement_data->GetCacheManager()->get_cached_columns(insert_stmt, &cached_columns_temp);
    if (!ret) {
        return true;
    }
    CachedColumns cached_column_insert(false, true);
    if (insert_stmt->withClause) {
        ret = run_pre_with_list_statement(insert_stmt->withClause->ctes, statement_data, &cached_column_insert);
        RETURN_IF(!ret, false);
    }
    SelectStmt *select_stmt = (SelectStmt *)(insert_stmt->selectStmt);
    if (select_stmt != nullptr && select_stmt->fromClause != nullptr && select_stmt->targetList != nullptr) {
        SetOperation set_operation(SETOP_NONE);
        bool all(false);
        CachedColumns cached_columns_select(false, true);
        ret = run_pre_select_statement(select_stmt, set_operation, all, statement_data, 
            &cached_columns_select, &cached_column_insert);
        RETURN_IF(!ret, false);
        
        if (cached_columns_select.size() != cached_columns_temp.not_null_size()) {
            if (cached_columns_select.size() < cached_columns_temp.size()) {
                printfPQExpBuffer(&statement_data->conn->errorMessage,
                    "ERROR(CLIENT): unencrypted data should not be inserted into encrypted column\n");
            } else {
                printfPQExpBuffer(&statement_data->conn->errorMessage,
                    "ERROR(CLIENT): encrypted data should not be inserted into unencrypted column\n");
            }
            return false;
        }

        size_t i = 0;
        size_t j = 0;
        ColumnHookExecutor *select_exe = NULL;
        ColumnHookExecutor *insert_exe = NULL;

        /* 
         * if a column is unencrypted, we push nothing in the 'cached_columns_select'
         * but, we need to push a 'NULL' in the 'cached_columns_temp' to reserve the position of this column
         */
        for (; i < cached_columns_select.size(); i++, j++) {
            select_exe = cached_columns_select.at(i)->get_column_hook_executors()->at(0);

            for (; j < cached_columns_temp.size(); j++) {
                if (cached_columns_temp.at(j) != NULL) {
                    break;
                }
            }

            insert_exe = cached_columns_temp.at(j)->get_column_hook_executors()->at(0);
            if (insert_exe != select_exe) {
                printfPQExpBuffer(&statement_data->conn->errorMessage,
                    "ERROR(CLIENT): encrypted data should not be inserted into encrypted column with different keys\n");
                return false;
            }
        }
    }

    if (cached_columns_temp.is_empty()) {
        return true;
    }
    /* get actual values from query */
    RawValuesList raw_values_list;
    if (!RawValues::get_raw_values_from_insert_statement(insert_stmt, statement_data, &raw_values_list)) {
        fprintf(stderr, "error getting raw valued from statment\n");
        return false;
    }
    /* in "INSERT DEFAULT VALUES" there are no values therefore we have nothing to do. */
    if (raw_values_list.empty()) {
        return true;
    }

    /* check that if columns were retrieved in a list from the query, then the number of raw values equals (modulo) */
    if (insert_stmt->cols && !cached_columns_temp.is_divisor(raw_values_list.size())) {
        return true;
    }
    /* insert c1 values () returning [] */
    ret = run_pre_returning_list_statement(insert_stmt->returningList, &cached_columns_temp, cached_columns,
        statement_data);
    RETURN_IF(!ret, false);

    return ValuesProcessor::process_values(statement_data, &cached_columns_temp, list_length(select_stmt->valuesLists),
        &raw_values_list);
}

bool Processor::deal_order_by_statement(const SelectStmt * const select_stmt, ICachedColumns *select_cached_columns,
        StatementData *statement_data)
{
    if (select_stmt == NULL || select_cached_columns == NULL || !select_cached_columns->is_to_process()) {
        return true;
    }

    List *sort_list = select_stmt->sortClause;
    ListCell *list_node = NULL;
    if (sort_list != NIL) {
        foreach (list_node, sort_list) {
            Node *n = (Node *)lfirst(list_node);
            if (IsA(n, SortBy)) {
                SortBy *sort = (SortBy *)n;
                Node *sort_node = sort->node;
                if (IsA(sort_node, ColumnRef)) {
                    ColumnRef *cref = (ColumnRef *)sort_node;
                    ColumnRefData column_ref_data;
                    if (!exprProcessor::expand_column_ref(cref, column_ref_data)) {
                        return false;
                    }
                    for (size_t i = 0; i < select_cached_columns->size(); i++) {
                        if (select_cached_columns->at(i) && select_cached_columns->at(i)->get_col_name()) {
                            if (strcmp(select_cached_columns->at(i)->get_col_name(),
                                NameStr(column_ref_data.m_column_name)) == 0) {
                                printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext(
                                    "ERROR(CLIENT): could not support order by operator for column encryption\n"));
                                return false;
                            }
                        }
                    }
                }
            }
        }
    }
    return true;
}

bool Processor::run_pre_cached_global_setting(PGconn *conn, const char *stmt_name,
    CreateClientLogicGlobal *client_logic_global)
{
    char *function_name(NULL);
    StringArgs string_args;

    bool result =
        EncryptionPreProcess::run_pre_client_master_key(conn, client_logic_global, &function_name, string_args);
    if (!result) {
        return false;
    }

    PreparedStatement *prepared_statement = conn->client_logic->pendingStatements->get_or_create(stmt_name);
    if (!prepared_statement) {
        return false;
    }

    check_strncpy_s(strncpy_s(prepared_statement->m_function_name, sizeof(prepared_statement->m_function_name),
        function_name, strlen(function_name)));
    prepared_statement->m_string_args = string_args;

    /*
     *  hooks arguments validatity checks
     *  We are actually creating the hook here using the factory for this operation. But it will not be saved.
     *  Only in the CacheLoader do we care the hook and save in the memory
     */
    size_t existing_global_hook_executors_size(0);
    const GlobalHookExecutor **existing_global_hook_executors =
        conn->client_logic->m_cached_column_manager->get_global_hook_executors(existing_global_hook_executors_size);
    bool ret = HooksManager::GlobalSettings::pre_create(*conn->client_logic, function_name, string_args,
        existing_global_hook_executors, existing_global_hook_executors_size);
    if (existing_global_hook_executors) {
        free(existing_global_hook_executors);
    }

    return ret;
}

bool Processor::run_pre_column_setting_statement(PGconn *conn, const char *stmt_name,
    CreateClientLogicColumn *client_logic_column, const char *query, PGClientLogicParams &params)
{
    if (!conn->client_logic->m_cached_column_manager->has_global_setting()) {
        conn->client_logic->cacheRefreshType |= CacheRefreshType::GLOBAL_SETTING;
        conn->client_logic->m_cached_column_manager->load_cache(conn); 
        if (!conn->client_logic->m_cached_column_manager->has_global_setting()) {
            printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): no global setting found in local cache\n");
            return false;
        }
    }

    int location = 0;
    char global_key_name[NAMEDATALEN * NAME_CNT];
    errno_t rc = EOK;
    rc = memset_s(global_key_name, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
    securec_check_c(rc, "\0", "\0");
    const char *function_name(NULL);
    StringArgs string_args;

    int encrypted_value_location = 0;
    size_t encrypted_value_size = 0;
    size_t quote_num = 0;
    bool result = EncryptionPreProcess::run_pre_column_encryption_key(conn, client_logic_column, global_key_name,
        &function_name, string_args, &location, &encrypted_value_location, &encrypted_value_size, &quote_num);
    if (!result) {
        return false;
    }

    StringArgs new_strings_args;

    const CachedGlobalSetting *global_setting =
        conn->client_logic->m_cached_column_manager->get_global_setting_by_fqdn(global_key_name);
    if (global_setting == NULL) {
        conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
        conn->client_logic->m_cached_column_manager->load_cache(conn);
        global_setting = conn->client_logic->m_cached_column_manager->get_global_setting_by_fqdn(global_key_name);
        if (global_setting == NULL) {
            if (strlen(global_key_name) == 0) {
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("ERROR(CLIENT): failed to get client master key from cache\n"));
            } else {
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("ERROR(CLIENT): failed to get client master key %s from cache\n"), global_key_name);
            }
            return false;
        }
    }
    if (!HooksManager::ColumnSettings::pre_create(*conn->client_logic, global_setting->get_executor(), function_name,
        string_args, new_strings_args)) {
        return false;
    }

    // update query
    params.new_query_size = strlen(query);
    params.new_query = (char *)calloc(params.new_query_size + 1, sizeof(char));
    if (params.new_query == NULL) {
        return false;
    }
    check_strncpy_s(strncpy_s(params.new_query, params.new_query_size + 1, query, params.new_query_size));
    params.new_query[params.new_query_size] = '\0';

    EncryptionPreProcess::set_new_query(&params.new_query, params.new_query_size, new_strings_args, location,
        encrypted_value_location, encrypted_value_size, quote_num);

    params.adjusted_query = params.new_query;
    params.adjusted_query_size = params.new_query_size;
    return true;
}

bool Processor::find_in_name_map(const CStringsMap &col_alias_map, const char *name)
{
    if (col_alias_map.Size() == 0) {
        /* should happen only if the commmand was SELECT* */
        return true;
    }
    if (col_alias_map.find(name)) {
        return true;
    }
    return false;
}
bool Processor::is_set_operation_allowed_on_datatype(const ICachedColumns *cached_columns,
    const CStringsMap &col_alias_map)
{
    /* operation is allowed if no specific column */
    size_t cached_columns_size = cached_columns->size();
    for (size_t i = 0; i < cached_columns_size; ++i) {
        const ICachedColumn *cached_column = cached_columns->at(i);
        if (find_in_name_map(col_alias_map, cached_column->get_col_name())) {
            if (!HooksManager::is_set_operation_allowed(cached_column)) {
                return false;
            }
        }
    }
    return true;
}

bool Processor::get_column_names_from_target_list(const List* target_list, CStringsMap* col_alias_map,
    StatementData* statement_data)
{
    ListCell *tl = NULL;
    if (target_list) {
        foreach (tl, target_list) {
            Node *n = (Node *)lfirst(tl);
            if (IsA(n, ResTarget)) {
                ResTarget *rt = (ResTarget *)n;
                char alias_name[NAMEDATALEN];
                errno_t rc = EOK;
                rc = memset_s(alias_name, NAMEDATALEN, 0, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                if (rt->name) {
                    check_strncat_s(strncat_s(alias_name, NAMEDATALEN, rt->name, strlen(rt->name)));
                }
                if IsA (rt->val, ColumnRef) {
                    ColumnRef *cr = (ColumnRef *)rt->val;
                    Node *field1 = (Node *)linitial(cr->fields);
                    if IsA (field1, A_Star)
                        continue;

                    ColumnRefData column_ref_data;
                    if (!exprProcessor::expand_column_ref(cr, column_ref_data)) {
                        return false;
                    }
                    if (strcmp(alias_name, "") != 0) {
                        col_alias_map->set(alias_name, column_ref_data.m_alias_fqdn,
                            strlen(column_ref_data.m_alias_fqdn));
                    } else {
                        /* hack for not having empty map as it needed for is_set_operation_allowed_on_datatype */
                        col_alias_map->set(column_ref_data.m_alias_fqdn, column_ref_data.m_alias_fqdn,
                            strlen(column_ref_data.m_alias_fqdn));
                    }
                } else if IsA (rt->val, FuncCall) {
                    FuncCall* fc = (FuncCall*)rt->val;
                    func_name_data func_name;
                    exprProcessor::expand_function_name(fc, func_name);
                    CachedProc* cached_proc =
                        statement_data->GetCacheManager()->get_cached_proc(NameStr(func_name.m_catalogName),
                        NameStr(func_name.m_schemaName), NameStr(func_name.m_functionName));
                    if (cached_proc) {
                        const char* fname =
                            alias_name && strlen(alias_name) > 0 ? alias_name : func_name.m_functionName.data;
                        statement_data->conn->client_logic->insert_function(fname, cached_proc);
                    }
                }
            }
        }
    }
    return true;
}

bool Processor::is_set_operation_allowed(const SelectStmt * const select_stmt, const ICachedColumns *cached_columns,
    const SetOperation &parent_select_operation, const bool &parent_all, StatementData *statement_data)
{
    /* check whether operations on sets are allowed on this data type (UNION,INTERSECT,ETC) */
    CStringsMap col_alias_map;
    if (!get_column_names_from_target_list(select_stmt->targetList, &col_alias_map, statement_data)) {
        return false;
    }

    /* check if SET OPERATION is allowed on all of the data types */
    bool is_union_all = (parent_select_operation == SETOP_UNION && parent_all);
    const bool isSetOperation = (parent_select_operation != SETOP_NONE && !is_union_all);
    if (isSetOperation && !is_set_operation_allowed_on_datatype(cached_columns, col_alias_map)) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): set operations are not allowed on randomized encrypted column\n");
        return false;
    }
    return true;
}
/* from a, (select * from b), c join d */
bool Processor::run_pre_from_list_statement(const List * const from_list, StatementData *statement_data,
    ICachedColumns *cached_columns, const ICachedColumns* cached_columns_parents)
{
    ListCell *fl = NULL;
    foreach (fl, from_list) {
        Node *n = (Node *)lfirst(fl);
        if (!run_pre_from_item_statement(n, statement_data, cached_columns, cached_columns_parents)) {
            return false;
        }
    }
    return true;
}

bool Processor::run_pre_range_statement(const RangeVar * const range_var, StatementData *statement_data,
    ICachedColumns *cached_columns, const ICachedColumns* cached_columns_parents)
{
    CachedColumns cached_column_range(false);
    statement_data->conn->client_logic->m_cached_column_manager->get_cached_columns(range_var, &cached_column_range);
    for (size_t i = 0; i < cached_column_range.size(); i++) {
        CachedColumn *range_cached = new (std::nothrow) CachedColumn(cached_column_range.at(i));
        if (range_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        cached_columns->push(range_cached);
    }
    if (cached_columns_parents != NULL) {
        for (size_t i = 0; i < cached_columns_parents->size(); i++) {
            if (range_var->relname != NULL &&
                strcmp(cached_columns_parents->at(i)->get_table_name(), range_var->relname) == 0) {
                CachedColumn *range_cached = new (std::nothrow) CachedColumn(cached_columns_parents->at(i));
                if (range_cached == NULL) {
                    fprintf(stderr, "failed to new CachedColumn object\n");
                    return false;
                }
                cached_columns->push(range_cached);
            }
        }
    }
    return true;
}

/* from a as alias , (select * from b), c join d */
bool Processor::run_pre_from_item_statement(const Node * const from_item, StatementData *statement_data,
    ICachedColumns *cached_columns, const ICachedColumns* cached_columns_parents)
{
    CachedColumns cached_column_temp(false, true);
    bool if_alias = false;
    char *alias_name(NULL);
    if (IsA(from_item, RangeSubselect)) {
        SelectStmt *select_stmt_node = (SelectStmt *)((RangeSubselect *)from_item)->subquery;
        SetOperation set_operation(SETOP_NONE);
        bool all(false);
        if (!run_pre_select_statement(select_stmt_node, set_operation, all, statement_data, &cached_column_temp)) {
            return false;
        }
        if (((RangeSubselect *)from_item)->alias != NULL) {
            if_alias = true;
            alias_name = ((RangeSubselect *)from_item)->alias->aliasname;
        }
    } else if (IsA(from_item, JoinExpr)) {
        if (!run_pre_join_statement((JoinExpr *)from_item, statement_data, &cached_column_temp)) {
            return false;
        }
    } else if (IsA(from_item, RangeVar)) {
        RangeVar *range_var = (RangeVar *)from_item;
        if (range_var == NULL) {
            return false;
        }
        if (range_var->alias != NULL) {
            if_alias = true;
            alias_name = range_var->alias->aliasname;
        }
        if (!run_pre_range_statement(range_var, statement_data, &cached_column_temp, cached_columns_parents)) {
            return false;
        }
    } else if (IsA(from_item, RangeFunction)) {
        RangeFunction* range_func = (RangeFunction*)from_item;
        ExprPartsList func_expr_parts_list;
        if (range_func->funccallnode) {
            if (handle_func_call((const FuncCall*)range_func->funccallnode, &func_expr_parts_list, statement_data)) {
                func_processor::process(&func_expr_parts_list, statement_data);
            }
        }
    }

    for (size_t i = 0; i < cached_column_temp.size(); i++) {
        CachedColumn *alias_cached = new (std::nothrow) CachedColumn(cached_column_temp.at(i));
        if (alias_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        if (if_alias) {
            alias_cached->set_table_name(alias_name);
        }
        cached_columns->push(alias_cached);
    }
    return true;
}

/* with cte1 as (), cte2 as () */
bool Processor::run_pre_with_list_statement(const List * const with_list, StatementData *statement_data,
    ICachedColumns *cached_columns)
{
    ListCell *fl = NULL;
    foreach (fl, with_list) {
        Node *n = (Node *)lfirst(fl);
        if (!run_pre_with_item_statement(n, statement_data, cached_columns)) {
            return false;
        }
    }
    return true;
}

/* with cte [name list]as (insert/update/delete/select) */
bool Processor::run_pre_with_item_statement(const Node * const with_item, StatementData *statement_data,
    ICachedColumns *cached_columns)
{
    bool re = true;
    CommonTableExpr *with_cte = (CommonTableExpr *)with_item;
    Node *n = with_cte->ctequery;
    CachedColumns cached_columns_sub(false, true);
    switch (nodeTag(n)) {
        case T_InsertStmt:
            re = run_pre_insert_statement((InsertStmt *)n, statement_data, &cached_columns_sub);
            break;
        case T_DeleteStmt:
            re = run_pre_delete_statement((DeleteStmt *)n, statement_data, &cached_columns_sub);
            break;
        case T_UpdateStmt:
            re = run_pre_update_statement((UpdateStmt *)n, statement_data, &cached_columns_sub);
            break;
        case T_SelectStmt: {
            SetOperation set_operation(SETOP_NONE);
            bool all(false);
            CachedColumns cached_columns_sub(false, true);
            re = run_pre_select_statement((SelectStmt *)n, set_operation, all, statement_data, &cached_columns_sub);
        }
        default:
            break;
    }
    size_t col_index = 0;
    for (; col_index < cached_columns_sub.size(); col_index++) {
        CachedColumn *alias_cached = new (std::nothrow) CachedColumn(cached_columns_sub.at(col_index));
        if (alias_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        alias_cached->set_table_name(with_cte->ctename);
        cached_columns->push(alias_cached);
    }

    return re;
}

/* from a join b */
bool Processor::run_pre_join_statement(const JoinExpr * const join_stmt, StatementData *statement_data,
    ICachedColumns *cached_columns)
{
    CachedColumns cached_larg(false);
    if (!run_pre_from_item_statement(join_stmt->larg, statement_data, &cached_larg)) {
        return false;
    }
    CachedColumns cached_rarg(false);
    if (!run_pre_from_item_statement(join_stmt->rarg, statement_data, &cached_rarg)) {
        return false;
    }
    cached_columns->append(&cached_larg);
    cached_columns->append(&cached_rarg);
    if (join_stmt->quals) {
        ExprPartsList equal_clause;
        CachedColumns cached_euqal(false);
        if (!exprProcessor::expand_expr(join_stmt->quals, statement_data, &equal_clause)) {
            return false;
        }

        bool is_operator_forbidden(false);
        statement_data->conn->client_logic->m_cached_column_manager->filter_cached_columns(cached_columns, 
            &equal_clause, is_operator_forbidden, &cached_euqal);
        if (is_operator_forbidden) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                "ERROR(CLIENT): euqal operator is not allowed on datatype of this column\n");
            return false;
        }

        if (!WhereClauseProcessor::process(cached_columns, &equal_clause, statement_data)) {
            return false;
        }
    } else if (join_stmt->usingClause) {
        if (!join_with_same_key(join_stmt->usingClause, &cached_larg, &cached_rarg, statement_data)) {
            return false;
        }
    } else if (join_stmt->isNatural) {
        /* from a natural join b we need to judge each column which have the same name in a and b shares the same key */
        for (size_t i = 0; i < cached_larg.size(); i++) {
            for (size_t j = 0; j < cached_rarg.size(); j++) {
                const ICachedColumn *cached_left = cached_larg.at(i);
                const ICachedColumn *cached_right = cached_rarg.at(j);
                if ((cached_left) && (cached_right) &&
                    strcmp(cached_left->get_col_name(), cached_right->get_col_name()) == 0) {
                    ColumnHookExecutor *larg_exe = cached_left->get_column_hook_executors()->at(0);
                    ColumnHookExecutor *rarg_exe = cached_right->get_column_hook_executors()->at(0);
                    if (larg_exe != rarg_exe) {
                        printfPQExpBuffer(&statement_data->conn->errorMessage,
                            "ERROR(CLIENT): Natural join is not allowed on columns with different keys\n");
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

bool Processor::join_with_same_key(List *usingClause, ICachedColumns *cached_larg, ICachedColumns *cached_rarg,
    StatementData *statement_data)
{
    /* from a join b using (id) we need to judge id in a and id in b share the same key */
    ListCell *tl = NULL;
    foreach (tl, usingClause) {
        char *column = strVal(lfirst(tl));
        for (size_t i = 0; i < cached_larg->size(); i++) {
            const ICachedColumn *cached_left = cached_larg->at(i);
            if ((cached_left) && strcmp(cached_left->get_col_name(), column) == 0) {
                for (size_t j = 0; j < cached_rarg->size(); j++) {
                    const ICachedColumn *cached_right = cached_rarg->at(j);
                    if ((cached_right) && strcmp(cached_right->get_col_name(), column) == 0) {
                        ColumnHookExecutor *larg_exe = cached_left->get_column_hook_executors()->at(0);
                        ColumnHookExecutor *rarg_exe = cached_right->get_column_hook_executors()->at(0);
                        if (larg_exe != rarg_exe) {
                            printfPQExpBuffer(&statement_data->conn->errorMessage,
                                "ERROR(CLIENT): equal operator is not allowed on columns with different keys\n");
                            return false;
                        }
                    }
                }
            }
        }
    }
    return true;
}

bool Processor::process_res_target(ResTarget* resTarget, StatementData* statementData, ExprPartsList* funcExprPartsList)
{
    if (resTarget) {
        if (nodeTag(resTarget->val) == T_FuncCall) {
            return handle_func_call((FuncCall*)resTarget->val, funcExprPartsList, statementData);
        }
    }
    return true;
}

bool Processor::run_pre_select_target_list(List* targetList, StatementData* statementData,
    ExprPartsList* funcExprPartsList)
{
    ListCell* listNode = NULL;
    if (targetList) {
        foreach (listNode, targetList) {
            Node* n = (Node*)lfirst(listNode);
            if (nodeTag(n) == T_ResTarget) {
                if (!process_res_target((ResTarget*)n, statementData, funcExprPartsList)) {
                    return false;
                }
            }
        }
    }
    return true;
}

bool Processor::run_pre_select_statement(const SelectStmt * const select_stmt, StatementData *statement_data,
    bool *unencrypted)
{
    SetOperation set_operation(SETOP_NONE);
    bool all(false);
    bool res(false);
    CachedColumns cached_columns(false, true);
    CachedColumns cached_columns_parents(false, true);
    res =  run_pre_select_statement(select_stmt, set_operation, all, statement_data,
        &cached_columns, &cached_columns_parents);
    if (unencrypted != NULL && cached_columns.size() == 0) {
        *unencrypted = true; /* if unencrypted is true, it means the table is not full encrypted */
    }
    return res;
}

bool Processor::run_pre_select_statement(const SelectStmt * const select_stmt, const SetOperation &parent_set_operation,
    const bool &parent_all, StatementData *statement_data, ICachedColumns *cached_columns, 
    ICachedColumns *cached_columns_parents)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    bool ret = false;
    /* recurse over SELECT's for SET operations such as UNION/INTERSECT/ETC. */
    if (select_stmt->op != SETOP_NONE) {
        ret = process_select_set_operation(select_stmt, statement_data, cached_columns);
        RETURN_IF(!ret, false);
    }

    /* Handle function calls */
    ExprPartsList func_expr_parts_list;
    if (!run_pre_select_target_list(select_stmt->targetList, statement_data, &func_expr_parts_list)) {
        return false;
    }
    /* procees fuction calls */
    if (!func_processor::process(&func_expr_parts_list, statement_data)) {
        return false;
    }

    /* handle single WHERE and HAVING statement */
    ExprPartsList where_expr_parts_list;
    ret = exprProcessor::expand_expr(select_stmt->whereClause, statement_data, &where_expr_parts_list);
    RETURN_IF(!ret, false);

    ExprPartsList having_expr_vec;
    ret = exprProcessor::expand_expr(select_stmt->havingClause, statement_data, &having_expr_vec);
    RETURN_IF(!ret, false);
    /*
     *  filtered_cached_where get all columns relevant for this query's where clause
     *  filtered_cached_having get all columns relevant for this query's haing clause
     *  cached_columns_from get all columns relevant for this query's from clause
     *  cachedColumns get all columns relevant for this query's target list
     */
    bool is_operator_forbidden(false);
    CachedColumns filtered_cached_where(false);
    CachedColumns filtered_cached_having(false);
    CachedColumns cached_columns_from(false, true);
    bool has_enc_col = false;
    /* get target list */
    CStringsMap target_list;
    ret = get_column_names_from_target_list(select_stmt->targetList, &target_list, statement_data);
    RETURN_IF(!ret, false);

    /* from (select *) */
    if (select_stmt->withClause) {
        if (!run_pre_with_list_statement(select_stmt->withClause->ctes, statement_data, cached_columns_parents)) {
            return false;
        }
    }
    ret = run_pre_from_list_statement(select_stmt->fromClause, statement_data, &cached_columns_from,
        cached_columns_parents);
    RETURN_IF(!ret, false);
    
    statement_data->conn->client_logic->m_cached_column_manager->filter_cached_columns(&cached_columns_from, 
        &where_expr_parts_list, is_operator_forbidden, &filtered_cached_where);
    if (is_operator_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    statement_data->conn->client_logic->m_cached_column_manager->filter_cached_columns(&cached_columns_from, 
        &having_expr_vec, is_operator_forbidden, &filtered_cached_where);
    if (is_operator_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    for (size_t i = 0; i < cached_columns_from.size(); i++) {
        if (find_in_name_map(target_list, cached_columns_from.at(i)->get_col_name())) {
            has_enc_col = true;
            CachedColumn *target = new (std::nothrow) CachedColumn(cached_columns_from.at(i));
            if (target == NULL) {
                fprintf(stderr, "failed to new CachedColumn object\n");
                return false;
            }
            cached_columns->push(target);
        }
    }

    if (select_stmt->intoClause != NULL && has_enc_col) {
        PreparedStatement *cur_stmt =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (cur_stmt == NULL) {
            return false;
        }
        cur_stmt->cacheRefresh |= CacheRefreshType::COLUMN_SETTING;
    }

    if (cached_columns_from.is_empty()) {
        return true; /* nothing to do */
    }

    if (is_operator_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }

    /* check if operation is already on all columns (basesd on their data types) only with target list */
    ret = is_set_operation_allowed(select_stmt, cached_columns, parent_set_operation, parent_all, statement_data);
    RETURN_IF(!ret, false);

    ret = deal_order_by_statement(select_stmt, cached_columns, statement_data);
    RETURN_IF(!ret, false);

    if (!select_stmt->whereClause && !select_stmt->havingClause) {
        return true;
    }

    /* rewrite WHERE statement clause and having clause the query */
    ret = WhereClauseProcessor::process(&cached_columns_from, &where_expr_parts_list, statement_data, &target_list);
    RETURN_IF(!ret, false);

    ret = WhereClauseProcessor::process(&cached_columns_from, &having_expr_vec, statement_data, &target_list);
    RETURN_IF(!ret, false);

    return true;
}

bool Processor::process_select_set_operation(const SelectStmt * const select_stmt, 
    StatementData *statement_data, ICachedColumns *cached_columns)
{
    /* recursion */
    CachedColumns cached_larg(false, true);
    if (!run_pre_select_statement(select_stmt->larg, select_stmt->op, select_stmt->all, statement_data,
        &cached_larg)) {
        return false;
    }

    CachedColumns cached_rarg(false, true);
    if (!run_pre_select_statement(select_stmt->rarg, select_stmt->op, select_stmt->all, statement_data,
        &cached_rarg)) {
        return false;
    }
    /*
        * func : 1. need to judge each cachedcolumn's key in cached_larg  exists in  cached_rarg.
        *  2. need to judge |cached_larg| equals |cached_rarg|.
        */
    if (cached_larg.size() != cached_rarg.size()) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): set operator is not allowed on columns with different keys\n");
        return false;
    }
    for (size_t i = 0; i < cached_larg.size(); ++i) {
        ColumnHookExecutor *larg_exe = cached_larg.at(i)->get_column_hook_executors()->at(0);
        ColumnHookExecutor *rarg_exe = cached_rarg.at(i)->get_column_hook_executors()->at(0);
        Oid lorigdatatype_oid = cached_larg.at(i)->get_origdatatype_oid();
        Oid rorigdatatype_oid = cached_rarg.at(i)->get_origdatatype_oid();
        if (larg_exe != rarg_exe) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                "ERROR(CLIENT): set operator is not allowed on columns with different keys\n");
            return false;
        } else if (lorigdatatype_oid != rorigdatatype_oid) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                "ERROR(CLIENT): set operator is not allowed on columns with different type\n");
            return false;
        }
    }
    for (size_t i = 0; i < cached_larg.size(); i++) {
        CachedColumn *set_cached = new (std::nothrow) CachedColumn(cached_larg.at(i));
        if (set_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        cached_columns->push(set_cached);
    }
    return true;
}

bool Processor::run_pre_delete_statement(const DeleteStmt *delete_stmt, StatementData *statement_data,
    ICachedColumns *cached_columns)
{
    /* checking if featue is in use */
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true; // nothing to do
    }
    /* handle single WHERE statement */
    ExprPartsList where_expr_parts_list;
    if (!exprProcessor::expand_expr(delete_stmt->whereClause, statement_data, &where_expr_parts_list)) {
        return false;
    }

    /* get all columns relevant for this query's where clause */
    bool is_operator_forbidden(false);
    CachedColumns cached_column_temp(false, true);
    CachedColumns filter_cached_columns_temp(false);
    CachedColumns filter_cached_columns(false, true);
    bool ret = statement_data->GetCacheManager()->get_cached_columns(delete_stmt, &where_expr_parts_list,
        is_operator_forbidden, &cached_column_temp);
    if (is_operator_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    if (!ret || cached_column_temp.is_empty()) {
        return true; /* nothing to do */
    }

    /* return list is useful for with cte as () */
    if (!run_pre_returning_list_statement(delete_stmt->returningList, &cached_column_temp, cached_columns,
        statement_data)) {
        return false;
    }

    ret = statement_data->GetCacheManager()->filter_cached_columns(&cached_column_temp, &where_expr_parts_list,
        is_operator_forbidden, &filter_cached_columns_temp);

    if (is_operator_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    
    if (!ret) {
        return false;
    }
    for (size_t i = 0; i < filter_cached_columns_temp.size(); i++) {
        if (filter_cached_columns_temp.at(i) == NULL) {
            filter_cached_columns.push(NULL);
        } else {
            CachedColumn *filter_cached = new (std::nothrow) CachedColumn(filter_cached_columns_temp.at(i));
            if (filter_cached == NULL) {
                fprintf(stderr, "failed to new CachedColumn object\n");
                return false;
            }
            filter_cached_columns.push(filter_cached);
        }
    }

    /* rewrite WHERE statement clause in the query */
    if (!WhereClauseProcessor::process(&filter_cached_columns, &where_expr_parts_list, statement_data)) {
        return false;
    };
    
    return true;
}

bool Processor::run_pre_execute_statement(const ExecuteStmt * const execute_stmt, StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty())
        return true;

    PreparedStatement *prepares_statement =
        statement_data->conn->client_logic->preparedStatements->get_or_create(execute_stmt->name);
    if (!prepares_statement) {
        return false;
    }

    if (!prepares_statement->cached_params || prepares_statement->cached_params->is_empty()) {
    /* no processed columns */
        return true; 
    }
    RawValuesList raw_values_list;
    bool ret = RawValues::get_raw_values_from_execute_statement(execute_stmt, statement_data, &raw_values_list);
    if (!ret) {
        fprintf(stderr, "error getting raw valued from statment\n");
        return false;
    }

    return ValuesProcessor::process_values(statement_data, prepares_statement->cached_params, 1,
        &raw_values_list);
}

bool Processor::run_pre_declare_cursor_statement(const DeclareCursorStmt * const declareCursorStmt,
    StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    return run_pre_statement(declareCursorStmt->query, statement_data);
}

bool Processor::run_pre_copy_statement(const CopyStmt * const copy_stmt, StatementData *statement_data)
{
    /* checking if feature is in use */
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true; /* nothing to do */
    }

    PreparedStatement *prepared_statement = NULL;
    if (copy_stmt->query) {
        /*
         *  we have a "COPY (stmt) TO STDOUT" query
         *  make sure that the query is a SELECT statement and that the SELECT statement is valid
         */
        if (nodeTag(copy_stmt->query) != T_SelectStmt)
            return false;
        bool unecrypted = false;
        bool res = run_pre_select_statement((SelectStmt *)copy_stmt->query, statement_data, &unecrypted);
        if (!res) {
            return false;
        }
        if (unecrypted) {
            return true;
        }
        if (copy_stmt->filename || copy_stmt->encrypted) { // we have a file the data will not pass through the client
            fprintf(stderr, "ERROR(CLIENT): column encryption does't support copy from server file to table\n");
            return false;
        }
    } else if (copy_stmt->relation) {
        CachedColumns cached_column_range(false);
        (void)statement_data->conn->client_logic->m_cached_column_manager->get_cached_columns(copy_stmt->relation,
            &cached_column_range);
        if (cached_column_range.size() == 0) {
            return true;
        }
        if (copy_stmt->filename || copy_stmt->encrypted) { // we have a file the data will not pass through the client
            fprintf(stderr, "ERROR(CLIENT): column encryption does't support copy from server file to table\n");
            return false;
        }
        /*
         *  "COPY <relation> FROM STDIN" requires us to build a cached columns list for the csv that will be inserted
         *  "COPY <relation> TO STDOUT" does not need any more processing
         */
        if (copy_stmt->is_from) {
            /* "COPY <relation> FROM STDIN" */
            prepared_statement =
                statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
            if (!prepared_statement) {
                fprintf(stderr, "failed to get PreparedStatement object\n");
                return false;
            }

            if (!prepared_statement->cached_copy_columns) {
                prepared_statement->cached_copy_columns = new (std::nothrow) CachedColumns;
                if (prepared_statement->cached_copy_columns == NULL) {
                    fprintf(stderr, "failed to new CachedColumns object\n");
                    return false;
                }
            }
            statement_data->GetCacheManager()->get_cached_columns(copy_stmt, prepared_statement->cached_copy_columns);
            prepared_statement->partial_csv_column_size = 0;
        }
    } else {
        /* shouldn't get here */
        return false;
    }

    if (!prepared_statement) {
        prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    }

    if (!prepared_statement) {
        return false;
    }
    delete_copy_state(prepared_statement->copy_state);
    prepared_statement->copy_state = pre_copy(copy_stmt, statement_data->query);
    return true;
}

bool Processor::run_pre_alter_table_statement(const AlterTableStmt *stmt, StatementData *statement_data)
{
    const List *cmds = reinterpret_cast<const AlterTableStmt * const>(stmt)->cmds;
    CachedColumns cached_columns;
    if (!statement_data->GetCacheManager()->get_cached_columns(stmt->relation, &cached_columns)) {
        return false;
    }
    ListCell *iter = NULL;
    foreach (iter, cmds) {
        const AlterTableCmd *cmd = (AlterTableCmd *)lfirst(iter);
        switch (cmd->subtype) {
            case AT_AddColumn: {
                /* ALTER TABLE ADD COLUMN */
                ColumnDef *def = (ColumnDef *)cmd->def;
                if (def->clientLogicColumnRef) {
                    ExprPartsList expr_vec;
                    if (!(def->clientLogicColumnRef->column_key_name)) {
                        fprintf(stderr, "ERROR(CLIENT): column encryption key cannot be empty\n");
                        return false;
                    }
                    char column_key_name[NAMEDATALEN * NAME_CNT];
                    errno_t rc = EOK;
                    rc = memset_s(column_key_name, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
                    securec_check_c(rc, "\0", "\0");
                    if (!name_list_to_cstring(def->clientLogicColumnRef->column_key_name, column_key_name,
                        sizeof(column_key_name))) {
                        fprintf(stderr, "ERROR(CLIENT): column encryption key name cannot be empty\n");
                        return false;
                    }

                    char object_fqdn[NAMEDATALEN * NAME_CNT];
                    rc = memset_s(object_fqdn, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
                    securec_check_c(rc, "\0", "\0");
                    size_t object_fqdn_size =
                        statement_data->GetCacheManager()->get_object_fqdn(column_key_name, false, object_fqdn);
                    if (object_fqdn_size == 0) {
                        statement_data->conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
                        statement_data->GetCacheManager()->load_cache(statement_data->conn);
                        object_fqdn_size = 
                            statement_data->GetCacheManager()->get_object_fqdn(column_key_name, false, object_fqdn);
                        if (object_fqdn_size == 0) {
                            fprintf(stderr,
                                "ERROR(CLIENT): error while trying to retrieve column encryption key from cache\n");
                            return false;
                        }
                    }
                    /* add DATATYPE_CL to "client logic" column */
                    CachedColumns cached_columns(false, true);
                    CachedColumns cached_columns_for_replace;
                    bool error = false;
                    RawValue *raw_value = createStmtProcessor::trans_column_definition(def, &expr_vec, &cached_columns,
                        &cached_columns_for_replace, statement_data, object_fqdn, error);
                    RETURN_IF(error, false);
                    if (raw_value) {
                        PreparedStatement *prepared_statement =
                            statement_data->conn->client_logic->pendingStatements->get_or_create(
                                statement_data->stmtName);
                        if (prepared_statement) {
                            prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
                        }
                        statement_data->conn->client_logic->rawValuesForReplace->add(raw_value);
                    }
                    if (expr_vec.empty()) {
                        continue;
                    }

                    /* process default value in column definition */
                    RawValuesList raw_values_list;
                    bool res =
                        RawValues::get_raw_values_from_consts_vec(&expr_vec, statement_data, 0, &raw_values_list);
                    RETURN_IF(!res, false);
                    res = ValuesProcessor::process_values(statement_data, &cached_columns_for_replace, 1,
                        &raw_values_list);
                    RETURN_IF(!res, false);
                }
                continue;
            }
            case AT_DropColumn:{
                const ICachedColumn *cached_column = statement_data->GetCacheManager()->get_cached_column(
                    stmt->relation->catalogname, stmt->relation->schemaname, stmt->relation->relname, cmd->name);
                if (!cached_column) {
                    continue;
                }
                PreparedStatement *prepared_statement =
                    statement_data->conn->client_logic->pendingStatements->get_or_create(
                        statement_data->stmtName);
                if (prepared_statement) {
                    prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
                }
                break;
            }
            case AT_ColumnDefault: {
                ExprPartsList expr_vec;
                const ICachedColumn *cached_column = statement_data->GetCacheManager()->get_cached_column(
                    stmt->relation->catalogname, stmt->relation->schemaname, stmt->relation->relname, cmd->name);
                if (!cached_column) {
                    continue;
                }
                CachedColumns cached_columns;
                cached_columns.push(cached_column);
                bool expr_res = exprProcessor::expand_expr(cmd->def, statement_data, &expr_vec);
                RETURN_IF(!expr_res, false);
                if (expr_vec.empty()) {
                    continue;
                }
                RawValuesList raw_values_list;
                expr_res = RawValues::get_raw_values_from_consts_vec(&expr_vec, statement_data, 0, &raw_values_list);
                RETURN_IF(!expr_res, false);
                return ValuesProcessor::process_values(statement_data, &cached_columns, 1, &raw_values_list);
            }
            case AT_AddConstraint: {
                if (!alter_add_constraint(cmd, &cached_columns, statement_data)) {
                    return false;
                }
                break;
            }
            default:
                continue;
        }
    }
    return true;
}


bool Processor::alter_add_constraint(const AlterTableCmd *cmd, ICachedColumns *cached_columns, 
    StatementData *statement_data)
{
    if (IsA(cmd->def, Constraint)) {
        Constraint * constraint = (Constraint *)cmd->def;
        if (constraint->keys != NULL) {
            ListCell *ixcell = NULL;
            foreach (ixcell, constraint->keys) {
                char *ikname = strVal(lfirst(ixcell));
                for (size_t i = 0; i < cached_columns->size(); i++) {
                    if (cached_columns->at(i)->get_col_name() != NULL && 
                        strcmp(cached_columns->at(i)->get_col_name(), ikname) == 0 &&
                        !createStmtProcessor::check_constraint(constraint, cached_columns->at(i)->get_data_type(),
                            ikname, cached_columns, statement_data)) {
                        return false;
                    }
                }
            }
        } else if (constraint->raw_expr != NULL) {
            if (!createStmtProcessor::transform_expr(constraint->raw_expr, "", cached_columns, statement_data)) {
                return false;
            }
        }
    } else {
        fprintf(stderr, "ERROR(CLIENT): unrecognized node type: %d\n", (int)nodeTag(cmd->def));
    }
    return true;
}

bool Processor::run_pre_drop_table_statement(const DropStmt *stmt, StatementData *statement_data)
{
    ListCell *cell = NULL;
    bool need_refresh = false;
    foreach (cell, stmt->objects) {
        List *names = (List *)lfirst(cell);
        char *relname = NULL;
        char *schemaname = NULL;
        char *catalogname = NULL;

        switch (list_length(names)) {
            case 1:
                relname = strVal(linitial(names));
                break;
            case 2:
                schemaname = strVal(linitial(names));
                relname = strVal(lsecond(names));
                break;
            case 3:
                catalogname = strVal(linitial(names));
                schemaname = strVal(lsecond(names));
                relname = strVal(lthird(names));
                break;
            default:
                char full_table_name[NAMEDATALEN * NAME_CNT];
                errno_t rc = EOK;
                rc = memset_s(full_table_name, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
                securec_check_c(rc, "\0", "\0");
                name_list_to_cstring(names, full_table_name, sizeof(full_table_name));
                fprintf(stderr, "ERROR(CLIENT): improper relation name (too many dotted names): %s\n", full_table_name);
                break;
        }
        if (statement_data->GetCacheManager()->has_cached_columns(catalogname, schemaname, relname)) {
            need_refresh = true;
        }
    }
    if (need_refresh) {
        PreparedStatement *prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            return false;
        }
        prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
    }
    return true;
}

bool Processor::run_pre_drop_schema_statement(const DropStmt *stmt, StatementData *statement_data)
{
    /*
     *  check if the schema is not empty
     *  and only if it is not empty and the schema is about to be removed (DROP CASCADE)
     *  then invoke the cache to reload
     */
    bool is_schema_contains_objects = false;
    ListCell *cell = NULL;
    foreach (cell, stmt->objects) {
        List *objname = (List *)lfirst(cell);
        char schema_name[NAMEDATALEN * 2];
        errno_t rc = EOK;
        rc = memset_s(schema_name, NAMEDATALEN * 2, 0, NAMEDATALEN * 2);
        securec_check_c(rc, "\0", "\0");
        if (!name_list_to_cstring(objname, schema_name, sizeof(schema_name))) {
            return false;
        }

        /* extend array if required */
        if (statement_data->conn->client_logic->droppedSchemas_size + 1 >
            statement_data->conn->client_logic->droppedSchemas_allocated) {
            statement_data->conn->client_logic->droppedSchemas =
                (ObjectFqdn *)libpq_realloc(statement_data->conn->client_logic->droppedSchemas,
                                            sizeof(*statement_data->conn->client_logic->droppedSchemas) *
                                            statement_data->conn->client_logic->droppedSchemas_size,
                                            sizeof(*statement_data->conn->client_logic->droppedSchemas) *
                                            (statement_data->conn->client_logic->droppedSchemas_size + 1));
            if (statement_data->conn->client_logic->droppedSchemas == NULL) {
                return false;
            }
            statement_data->conn->client_logic->droppedSchemas_allocated =
                statement_data->conn->client_logic->droppedSchemas_size + 1;
        }

        /* copy object name */
        check_strncpy_s(strncpy_s(
            statement_data->conn->client_logic->droppedSchemas[statement_data->conn->client_logic->droppedSchemas_size]
                .data,
            sizeof(statement_data->conn->client_logic
                       ->droppedSchemas[statement_data->conn->client_logic->droppedSchemas_size]),
            schema_name, strlen(schema_name)));
        ++statement_data->conn->client_logic->droppedSchemas_size;
        if (statement_data->GetCacheManager()->is_schema_contains_objects(schema_name)) {
            is_schema_contains_objects = true;
            break;
        }
    }

    /* check if objects are going to be dropped */
    if (stmt->behavior != DROP_CASCADE || !is_schema_contains_objects) {
        return true;
    }

    PreparedStatement *prepared_statement =
        statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    if (!prepared_statement) {
        return false;
    }
    prepared_statement->cacheRefresh |= CacheRefreshType::GLOBAL_SETTING;
    prepared_statement->cacheRefresh |= CacheRefreshType::COLUMN_SETTING;
    prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
    return true;
}

bool Processor::run_pre_drop_statement(const DropStmt *stmt, StatementData *statement_data)
{
    /*
     *  update the (prepared) statement's object's cacheRefresh value according to the DROP statement in progress
     *  this value will be used in the PostQuery to override the value of the cacheRefresh in the session and then used
     *  in the load_cache function
     */
    if (stmt->removeType == OBJECT_GLOBAL_SETTING) {
        ListCell *cell = NULL;
        char object_name[NAMEDATALEN * NAME_CNT];
        errno_t rc = EOK;
        ObjName *to_drop_cmk_list = NULL;

        rc = memset_s(object_name, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
        securec_check_c(rc, "\0", "\0");
        foreach (cell, stmt->objects) {
            /* get object name */
            List *names = (List *)lfirst(cell);
            if (!name_list_to_cstring(names, object_name, NAMEDATALEN * NAME_CNT)) {
                return false;
            }
            char object_fqdn[NAMEDATALEN * NAME_CNT];
            rc = memset_s(object_fqdn, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
            securec_check_c(rc, "\0", "\0");
            size_t object_fqdn_size = 
                statement_data->GetCacheManager()->get_object_fqdn(object_name, true, object_fqdn);
            /* skip if the object doesn't exist */
            if (object_fqdn_size == 0) {
                continue;
            }
            check_strncpy_s(strncpy_s(object_name, NAMEDATALEN * NAME_CNT, object_fqdn, strlen(object_fqdn)));

            to_drop_cmk_list = obj_list_append(to_drop_cmk_list, object_name);
            if (to_drop_cmk_list == NULL) {
                return false;
            }
            
            if (stmt->behavior == DROP_CASCADE) {
                /* add dependant column settings */
                size_t column_settings_list_size(0);
                const CachedColumnSetting **depened_column_settings =
                    statement_data->GetCacheManager()->get_column_setting_by_global_setting_fqdn(object_name,
                        column_settings_list_size);

                /*
                   extend array if required
                */
                if (statement_data->conn->client_logic->droppedColumnSettings_size + column_settings_list_size >
                    statement_data->conn->client_logic->droppedColumnSettings_allocated) {
                    statement_data->conn->client_logic->droppedColumnSettings =
                        (ObjectFqdn *)libpq_realloc(statement_data->conn->client_logic->droppedColumnSettings,
                        sizeof(*statement_data->conn->client_logic->droppedColumnSettings) *
                        statement_data->conn->client_logic->droppedColumnSettings_size,
                        sizeof(*statement_data->conn->client_logic->droppedColumnSettings) *
                        (statement_data->conn->client_logic->droppedColumnSettings_size + column_settings_list_size));
                    if (statement_data->conn->client_logic->droppedColumnSettings == NULL) {
                        libpq_free(depened_column_settings);
                        free_obj_list(to_drop_cmk_list);
                        return false;
                    }
                    statement_data->conn->client_logic->droppedColumnSettings_allocated =
                        statement_data->conn->client_logic->droppedColumnSettings_size + column_settings_list_size;
                }

                for (size_t i = 0; i < column_settings_list_size; ++i) {
                    check_strncpy_s(strncpy_s(
                        statement_data->conn->client_logic
                            ->droppedColumnSettings[statement_data->conn->client_logic->droppedColumnSettings_size + i]
                            .data,
                        sizeof(
                        statement_data->conn->client_logic
                                ->droppedColumnSettings[statement_data->conn->client_logic->droppedColumnSettings_size +
                        i].data),
                        depened_column_settings[i]->get_fqdn(), strlen(depened_column_settings[i]->get_fqdn())));
                }
                libpq_free(depened_column_settings);
                statement_data->conn->client_logic->droppedColumnSettings_size += column_settings_list_size;
            }
        }

        PreparedStatement *prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            free_obj_list(to_drop_cmk_list);
            return false;
        }
        statement_data->conn->client_logic->droppedGlobalSettings = to_drop_cmk_list;

        prepared_statement->cacheRefresh |= CacheRefreshType::GLOBAL_SETTING;
        if (stmt->behavior == DROP_CASCADE) {
            prepared_statement->cacheRefresh |= CacheRefreshType::COLUMN_SETTING;
            prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
        }
    } else if (stmt->removeType == OBJECT_COLUMN_SETTING) {
        PreparedStatement *prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            return false;
        }
        prepared_statement->cacheRefresh |= CacheRefreshType::COLUMN_SETTING;
        if (stmt->behavior == DROP_CASCADE) {
            prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
        }
        ListCell *cell = NULL;
        char object_name[NAMEDATALEN * NAME_CNT];

        foreach (cell, stmt->objects) {
            /* get object name */
            List *names = (List *)lfirst(cell);
            if (!name_list_to_cstring(names, object_name, NAMEDATALEN * NAME_CNT)) {
                return false;
            }
            char object_fqdn[NAMEDATALEN * NAME_CNT];
            errno_t rc = EOK;
            rc = memset_s(object_fqdn, NAMEDATALEN * NAME_CNT, 0, NAMEDATALEN * NAME_CNT);
            securec_check_c(rc, "\0", "\0");
            size_t object_fqdn_size = 
                statement_data->GetCacheManager()->get_object_fqdn(object_name, false, object_fqdn);
            /* skip if the object doesn't exist */
            if (object_fqdn_size == 0) {
                continue;
            }

            check_strncpy_s(strncpy_s(object_name, NAMEDATALEN * NAME_CNT, object_fqdn, object_fqdn_size));

            /*
               extend array if required
            */
            if (statement_data->conn->client_logic->droppedColumnSettings_size + 1 >
                statement_data->conn->client_logic->droppedColumnSettings_allocated) {
                statement_data->conn->client_logic->droppedColumnSettings =
                    (ObjectFqdn *)libpq_realloc(statement_data->conn->client_logic->droppedColumnSettings,
                                                sizeof(*statement_data->conn->client_logic->droppedColumnSettings) *
                                                statement_data->conn->client_logic->droppedColumnSettings_size,
                                                sizeof(*statement_data->conn->client_logic->droppedColumnSettings) *
                                                (statement_data->conn->client_logic->droppedColumnSettings_size + 1));
                if (statement_data->conn->client_logic->droppedColumnSettings == NULL) {
                    return false;
                }
                statement_data->conn->client_logic->droppedColumnSettings_allocated =
                    statement_data->conn->client_logic->droppedColumnSettings_size + 1;
            }

            /*
               add dependant column settings
               copy object name
           */
            check_strncpy_s(
                strncpy_s(statement_data->conn->client_logic
                            ->droppedColumnSettings[statement_data->conn->client_logic->droppedColumnSettings_size]
                            .data,
                    sizeof(statement_data->conn->client_logic
                           ->droppedColumnSettings[statement_data->conn->client_logic->droppedColumnSettings_size]),
                    object_name, strlen(object_name)));
            ++statement_data->conn->client_logic->droppedColumnSettings_size;
        }
    } else if (stmt->removeType == OBJECT_TABLE || stmt->removeType == OBJECT_VIEW) {
        run_pre_drop_table_statement(stmt, statement_data);
    } else if (stmt->removeType == OBJECT_SCHEMA) {
        run_pre_drop_schema_statement(stmt, statement_data);
    } else if (stmt->removeType == OBJECT_FUNCTION) {
        PreparedStatement* prepared_statement = 
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            return false;
        }
        prepared_statement->cacheRefresh |= CacheRefreshType::PROCEDURES;
    }
    return true;
}

bool Processor::run_pre_exec_direct_statement(const ExecDirectStmt *stmt, StatementData *statement_data)
{
    size_t add_length = stmt->location + strlen("\'");
    if (add_length <= 0) {
        return false;
    }
    statement_data->params.new_query_size = strlen(statement_data->query);
    statement_data->params.new_query = (char *)calloc(statement_data->params.new_query_size + 1, sizeof(char));
    if (statement_data->params.new_query == NULL) {
        return false;
    }
    check_strncpy_s(strncpy_s(statement_data->params.new_query, statement_data->params.new_query_size + 1,
        statement_data->query, statement_data->params.new_query_size));
    statement_data->params.new_query[statement_data->params.new_query_size] = '\0';
    StatementData direct_statement_data(statement_data->conn, "", stmt->query, 0, 0, 0, 0, 0);
    PGconn *conn = direct_statement_data.conn;
    direct_statement_data.stmtName = direct_statement_data.stmtName ? direct_statement_data.stmtName : "";
    if (direct_statement_data.query == nullptr) {
        return false;
    }
    /* func : call create() function instead but we need to make sure to always delete the prepared statements when they
     * are no longer in use */
    conn->client_logic->pendingStatements->get_or_create(direct_statement_data.stmtName);
    ListCell *stmt_iter = NULL;
    List *stmts = Parser::Parse(direct_statement_data.conn->client_logic, direct_statement_data.query);
    foreach (stmt_iter, stmts) {
        Node *direct_stmt = (Node *)lfirst(stmt_iter);
        bool client_logic_ret = run_pre_statement(direct_stmt, &direct_statement_data);
        if (!client_logic_ret) {
            return false;
        }
    }
    statement_data->conn->client_logic->rawValuesForReplace =
        direct_statement_data.conn->client_logic->rawValuesForReplace;

    size_t size = statement_data->conn->client_logic->rawValuesForReplace->size();
    statement_data->conn->client_logic->rawValuesForReplace->sort_by_location();
    int i = (int)(size - 1);
    for (; i >= 0; --i) {
        RawValue *raw_value = statement_data->conn->client_logic->rawValuesForReplace->at(i);
        if (raw_value == NULL || raw_value->m_processed_data == NULL) {
            continue;
        }
        raw_value->m_location += add_length;
        unsigned char *processed_data = 
            (unsigned char *)calloc(raw_value->m_processed_data_size + strlen("\'\'"), sizeof(unsigned char));
        if (processed_data == NULL) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): could not calloc buffer for processed_data\n"));
            return false;
        }

        check_memcpy_s(memcpy_s(processed_data, raw_value->m_processed_data_size + strlen("\'\'"), "'", 1));
        check_memcpy_s(memcpy_s(processed_data + 1, raw_value->m_processed_data_size + 1, raw_value->m_processed_data,
            raw_value->m_processed_data_size));
        check_memcpy_s(memcpy_s(processed_data + raw_value->m_processed_data_size + 1, 1, "'", 1));
        if (raw_value->m_processed_data != NULL) {
            libpq_free(raw_value->m_processed_data);
        }
        raw_value->m_processed_data = processed_data;
        raw_value->m_processed_data_size += strlen("\'\'");
    }
    return true;
}

bool Processor::run_pre_rlspolicy_using(const Node *stmt, StatementData *statement_data)
{
    Node *using_qual = NULL;
    RangeVar *relation = NULL;
    if (nodeTag(stmt) == T_CreateRlsPolicyStmt) {
        using_qual = ((CreateRlsPolicyStmt *)stmt)->usingQual;
        relation = ((CreateRlsPolicyStmt *)stmt)->relation;
    } else if (nodeTag(stmt) == T_AlterRlsPolicyStmt) {
        using_qual = ((AlterRlsPolicyStmt *)stmt)->usingQual;
        relation = ((AlterRlsPolicyStmt *)stmt)->relation;
    }

    ExprPartsList using_expr_parts_list;
    if (!exprProcessor::expand_expr(using_qual, statement_data, &using_expr_parts_list)) {
        return false;
    }

    /*
        get all columns relevant for this query's using clause
    */
    bool is_operation_forbidden(false);
    CachedColumns cached_columns;
    bool ret = statement_data->GetCacheManager()->get_cached_columns(relation, &using_expr_parts_list,
        is_operation_forbidden, &cached_columns);
    if (is_operation_forbidden) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            "ERROR(CLIENT): operator is not allowed on datatype of this column\n");
        return false;
    }
    if (!ret || cached_columns.is_empty()) {
        return true; // nothing to do
    }

    /* rewrite using statement clause in the query */
    statement_data->params.new_param_values =
        (unsigned char **)calloc(statement_data->nParams, sizeof(unsigned char *));
    if (statement_data->params.new_param_values == NULL) {
        return false;
    }
    statement_data->params.copy_sizes = (size_t *)calloc(statement_data->nParams, sizeof(size_t));
    if (statement_data->params.copy_sizes == NULL) {
        return false;
    }
    ret = WhereClauseProcessor::process(&cached_columns, &using_expr_parts_list, statement_data);
    if (!ret) {
        return false;
    }
    return true;
}

bool Processor::run_pre_create_function_stmt(const CreateFunctionStmt *stmt, StatementData *statement_data)
{
    if (stmt == NULL || statement_data == NULL) {
        return false;
    }

    if (stmt->parameters == NULL) {
        return true;
    }

    foreach_cell (lc, stmt->parameters) {
        FunctionParameter* fp  = (FunctionParameter*) lfirst(lc);
        const char* p_name =  strVal(llast(fp->argType->names));
        if (strcmp(p_name, "byteawithoutordercol") == 0 || strcmp(p_name, "byteawithoutorderwithequalcol") == 0 ||
            strcmp(p_name, "_byteawithoutordercol") == 0 || strcmp(p_name, "_byteawithoutorderwithequalcol") == 0) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): could not support functions when full encryption is on.\n"));
            return false;
        }
    }

    return true;
}

bool Processor::run_pre_create_rlspolicy_stmt(const CreateRlsPolicyStmt *stmt, StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    /* validate input */
    if (stmt == nullptr || stmt->relation == nullptr) {
        return false;
    } else if (stmt->usingQual == nullptr) {
        return true;
    }
    if (!run_pre_rlspolicy_using((Node *)stmt, statement_data)) {
        return true;
    }

    return true;
}

bool Processor::run_pre_alter_rlspolicy_stmt(const AlterRlsPolicyStmt *stmt, StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }
    /*
        validate input
    */
    if (stmt == nullptr || stmt->relation == nullptr) {
        return false;
    } else if (stmt->usingQual == nullptr) {
        return true;
    }
    if (!run_pre_rlspolicy_using((Node *)stmt, statement_data)) {
        return true;
    }

    return true;
}

static void handle_conforming(const VariableSetStmt *set_stmt, StatementData *statement_data)
{
    ListCell *args_iter = NULL;
    foreach (args_iter, set_stmt->args) {
        Node *node = (Node *)lfirst(args_iter);
        if (IsA(node, A_Const)) {
            A_Const *con = (A_Const *)node;
            if (IsA((Node *)&con->val, String)) {
                const char *sval = strVal(&con->val);
                statement_data->conn->client_logic->val_to_update |= updateGucValues::CONFORMING;
                if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 ||
                    pg_strcasecmp(sval, "yes") == 0 || pg_strcasecmp(sval, "1") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.standard_conforming_strings = true;
                } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 ||
                    pg_strcasecmp(sval, "no") == 0 || pg_strcasecmp(sval, "0") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.standard_conforming_strings = false;
                }
            }
        }
    }
}

static void handle_searchpath_set(const VariableSetStmt *set_stmt, StatementData *statement_data)
{
    ListCell *args_iter = NULL;
    bool is_first_search_path = true;
    foreach (args_iter, set_stmt->args) {
        Node *node = (Node *)lfirst(args_iter);
        if (IsA(node, A_Const)) {
            statement_data->conn->client_logic->val_to_update |= updateGucValues::SEARCH_PATH;
            A_Const *con = (A_Const *)node;
            if (is_first_search_path && (IsA((Node *)&con->val, String))) {
                statement_data->conn->client_logic->tmpGucParams.searchpathStr.assign(strVal(&con->val));
                is_first_search_path = false;
            } else if (IsA((Node *)&con->val, String)) {
                statement_data->conn->client_logic->tmpGucParams.searchpathStr.append(",");
                statement_data->conn->client_logic->tmpGucParams.searchpathStr.append(strVal(&con->val));
            }
        }
    }
}

static void handle_back_slash_quote(const VariableSetStmt *set_stmt, StatementData *statement_data)
{
    ListCell *args_iter = NULL;
    foreach (args_iter, set_stmt->args) {
        Node *node = (Node *)lfirst(args_iter);
        if (IsA(node, A_Const)) {
            A_Const *con = (A_Const *)node;
            if (IsA((Node *)&con->val, String)) {
                const char *sval = strVal(&con->val);
                statement_data->conn->client_logic->val_to_update |= updateGucValues::BACKSLASH_QUOTE;
                if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 ||
                    pg_strcasecmp(sval, "yes") == 0 || pg_strcasecmp(sval, "1") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.backslash_quote = BACKSLASH_QUOTE_ON;
                } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 ||
                    pg_strcasecmp(sval, "no") == 0 || pg_strcasecmp(sval, "0") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.backslash_quote = BACKSLASH_QUOTE_OFF;
                } else if (pg_strcasecmp(sval, "safe_encoding") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;
                }
            }
        }
    }
}

static void handle_escape_string(const VariableSetStmt *set_stmt, StatementData *statement_data)
{
    ListCell *args_iter = NULL;
    foreach (args_iter, set_stmt->args) {
        Node *node = (Node *)lfirst(args_iter);
        if (IsA(node, A_Const)) {
            A_Const *con = (A_Const *)node;
            if (IsA((Node *)&con->val, String)) {
                const char *sval = strVal(&con->val);
                statement_data->conn->client_logic->val_to_update |= updateGucValues::ESCAPE_STRING;
                if (pg_strcasecmp(sval, "true") == 0 || pg_strcasecmp(sval, "on") == 0 ||
                    pg_strcasecmp(sval, "yes") == 0 || pg_strcasecmp(sval, "1") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.escape_string_warning = true;
                } else if (pg_strcasecmp(sval, "false") == 0 || pg_strcasecmp(sval, "off") == 0 ||
                    pg_strcasecmp(sval, "no") == 0 || pg_strcasecmp(sval, "0") == 0) {
                    statement_data->conn->client_logic->tmpGucParams.escape_string_warning = false;
                }
            }
        }
    }
}

bool Processor::run_pre_set_statement(const VariableSetStmt *set_stmt, StatementData *statement_data)
{
    PreparedStatement *current_statement =
        statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    if (set_stmt->kind == VAR_RESET_ALL) {
        if (current_statement == NULL) {
            return false;
        }
        current_statement->cacheRefresh |= CacheRefreshType::SEARCH_PATH;
    } else if (set_stmt->kind == VAR_SET_ROLEPWD) {
        statement_data->conn->client_logic->val_to_update |= updateGucValues::GUC_ROLE;
        current_statement->cacheRefresh |= CacheRefreshType::CACHE_ALL;
        statement_data->conn->client_logic->tmpGucParams.role = strVal(&((A_Const *)(linitial(set_stmt->args)))->val);
    } else if (set_stmt->name &&
        (pg_strcasecmp(set_stmt->name, "search_path") == 0 || pg_strcasecmp(set_stmt->name, "current_schema") == 0)) {
        handle_searchpath_set(set_stmt, statement_data);
    } else if (set_stmt->name && pg_strcasecmp(set_stmt->name, "backslash_quote") == 0) {
        handle_back_slash_quote(set_stmt, statement_data);
    } else if (set_stmt->name && pg_strcasecmp(set_stmt->name, "standard_conforming_strings") == 0) {
        handle_conforming(set_stmt, statement_data);
    } else if (set_stmt->name && pg_strcasecmp(set_stmt->name, "escape_string_warning") == 0) {
        handle_escape_string(set_stmt, statement_data);
    } else if (set_stmt->name &&
        (pg_strcasecmp(set_stmt->name, "role") == 0 || pg_strcasecmp(set_stmt->name, "session_authorization") == 0)) {
        if (current_statement == NULL) {
            return false;
        }
        current_statement->cacheRefresh |= CacheRefreshType::CACHE_ALL;
    }
    return true;
}

bool Processor::run_pre_statement(const Node * const stmt, StatementData *statement_data)
{
    if (!stmt) {
        /* null valued in parser means fe do not care about this value */
        return true;
    }
    
    PreparedStatement *current_statement =
        statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    if (!current_statement) {
        return false;
    }
    switch (nodeTag(stmt)) {
        case T_InsertStmt:
            return run_pre_insert_statement((InsertStmt *)stmt, statement_data);
        case T_DeleteStmt:
            return run_pre_delete_statement((DeleteStmt *)stmt, statement_data);
        case T_UpdateStmt:
            return run_pre_update_statement((UpdateStmt *)stmt, statement_data);
        case T_SelectStmt: {
            bool unencrypted = false;
            return run_pre_select_statement((SelectStmt *)stmt, statement_data, &unencrypted);
        }
        case T_PrepareStmt:
            return run_pre_prepare_statement((PrepareStmt *)stmt, statement_data);
        case T_ExecuteStmt:
            return run_pre_execute_statement((ExecuteStmt *)stmt, statement_data);
        case T_DeclareCursorStmt:
            return run_pre_declare_cursor_statement((const DeclareCursorStmt * const)stmt, statement_data);
        case T_CopyStmt:
            return run_pre_copy_statement((CopyStmt *)stmt, statement_data);
        case T_AlterTableStmt:
            return run_pre_alter_table_statement((AlterTableStmt *)stmt, statement_data);
        case T_AlterRoleStmt:
            if (((AlterRoleStmt *)stmt)->options != NIL) {
                current_statement->cacheRefresh |= CacheRefreshType::CACHE_ALL;
            }
            break;
        case T_CreateStmt: {
            CreateStmt *create_stmt = (CreateStmt *)stmt;
            if (create_stmt->relation->relpersistence == RELPERSISTENCE_TEMP) {
                current_statement->cacheRefresh |= CacheRefreshType::SEARCH_PATH;
            }
            return createStmtProcessor::run_pre_create_statement(create_stmt, statement_data);
        }
        case T_CreateClientLogicGlobal:
            current_statement->cacheRefresh |= CacheRefreshType::GLOBAL_SETTING;
            return run_pre_cached_global_setting(statement_data->conn, statement_data->stmtName,
                (CreateClientLogicGlobal *)stmt);
        case T_CreateClientLogicColumn:
            current_statement->cacheRefresh |= CacheRefreshType::COLUMN_SETTING;
            return run_pre_column_setting_statement(statement_data->conn, statement_data->stmtName,
                (CreateClientLogicColumn *)stmt, statement_data->query, statement_data->params);
        case T_VariableSetStmt: {
            const VariableSetStmt *set_stmt = (const VariableSetStmt *)stmt;
            return run_pre_set_statement(set_stmt, statement_data);
        }
        case T_ViewStmt: {
            bool unencrypted = false;
            current_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
            /*
                rewrite query in the CREATE VIEW clause if query has relevant columns
            */
            return run_pre_select_statement((SelectStmt *)((ViewStmt *)stmt)->query, statement_data, &unencrypted);
        }
        case T_DropStmt:
            return run_pre_drop_statement((DropStmt *)stmt, statement_data);
            break;
        case T_TransactionStmt:
            if (((TransactionStmt *)stmt)->kind == TRANS_STMT_ROLLBACK) {
                current_statement->cacheRefresh |= CacheRefreshType::CACHE_ALL;
            }
            break;
        case T_ExecDirectStmt:
            return run_pre_exec_direct_statement((ExecDirectStmt *)stmt, statement_data);
        case T_CreateRlsPolicyStmt:
            return run_pre_create_rlspolicy_stmt((CreateRlsPolicyStmt *)stmt, statement_data);
        case T_AlterRlsPolicyStmt:
            return run_pre_alter_rlspolicy_stmt((AlterRlsPolicyStmt *)stmt, statement_data);
        case T_CreateFunctionStmt: {
            if (!run_pre_create_function_stmt((const CreateFunctionStmt *)stmt, statement_data)) {
                return false;
            }
            List* options = ((CreateFunctionStmt*)stmt)->options;
            bool res = func_processor::run_pre_create_function_stmt(options, statement_data);
            if (res) {
                current_statement->cacheRefresh |= CacheRefreshType::PROCEDURES;
            }
            return res;
        }
        case T_DoStmt:
            return func_processor::run_pre_create_function_stmt(((DoStmt*)stmt)->args, statement_data, true);
        case T_MergeStmt:
            return run_pre_merge_stmt((MergeStmt *)stmt, statement_data);
        case T_RenameStmt:
            current_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
            break;
        case T_DropRoleStmt:
            if (((DropRoleStmt *)stmt)->behavior == DROP_CASCADE) {
                current_statement->cacheRefresh |= CacheRefreshType::GLOBAL_SETTING;
            }
            break;
        case T_CreateTableAsStmt: {
            bool unencrypted = false;
            bool res = run_pre_select_statement((SelectStmt *)((CreateTableAsStmt *)stmt)->query,
                statement_data, &unencrypted);
            if (res && !unencrypted) {
                current_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
            }
            return res;
        }
        default:
            break;
    }
    return true;
}

bool Processor::run_pre_merge_stmt(const MergeStmt *stmt, StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }
    RETURN_IF(stmt == NULL, false);
    CachedColumns cached_columns(false, true);
    RangeVar *target_relation = (RangeVar *)stmt->relation;
    RangeVar *source_relation = (RangeVar *)stmt->source_relation;
    CachedColumns target_cached_columns;
    bool ret = true;
    ret = statement_data->GetCacheManager()->get_cached_columns(target_relation->catalogname, 
        target_relation->schemaname, target_relation->relname, &target_cached_columns);
    RETURN_IF(!ret, false);
    CachedColumns source_cached_columns;
    ret = statement_data->GetCacheManager()->get_cached_columns(source_relation->catalogname, 
        source_relation->schemaname, source_relation->relname, &source_cached_columns);
    RETURN_IF(!ret, false);
    for (size_t col_index = 0; col_index < target_cached_columns.size(); col_index++) {
        CachedColumn *target_alias_cached = new (std::nothrow) CachedColumn(target_cached_columns.at(col_index));
        if (target_alias_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        target_alias_cached->set_table_name(target_relation->alias->aliasname);
        cached_columns.push(target_alias_cached);
    }
    for (size_t col_index = 0; col_index < source_cached_columns.size(); col_index++) {
        CachedColumn *source_alias_cached = new (std::nothrow) CachedColumn(source_cached_columns.at(col_index));
        if (source_alias_cached == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        source_alias_cached->set_table_name(source_relation->alias->aliasname);
        cached_columns.push(source_alias_cached);
    }

    ExprPartsList join_condition_expr;
    /* no-support merge into operator when column using different keys to encrypt */
    ret = exprProcessor::expand_condition_expr(stmt->join_condition, &join_condition_expr);
    RETURN_IF(!ret, false);
    CachedColumns v_res;
    bool is_source_found = false;
    bool is_target_found = false;
    size_t join_exprs_list_size = join_condition_expr.size();
    for (size_t i = 0; i < join_exprs_list_size; i++) {
        const ExprParts *expr_part = join_condition_expr.at(i);
        RETURN_IF(expr_part == NULL, false);
        ColumnRefData column_ref_data;
        ret = exprProcessor::expand_column_ref(expr_part->column_ref, column_ref_data);
        RETURN_IF(!ret, false);

        size_t source_cached_columns_size = source_cached_columns.size();
        if (!is_target_found) {
            for (size_t i = 0; i < source_cached_columns_size; i++) {
                const ICachedColumn *source_tmp = source_cached_columns.at(i);
                if (source_tmp != NULL && strcmp(source_tmp->get_col_name(), 
                    (const char *)(NameStr(column_ref_data.m_column_name))) == 0) { /* found that column is encryped */
                    v_res.push(source_tmp);
                    is_source_found = true;
                    is_target_found = true;
                    break;
                }
            }
        }
        if (!is_source_found) {
            size_t target_cached_columns_size = target_cached_columns.size();
            for (size_t i = 0; i < target_cached_columns_size; i++) {
                const ICachedColumn *target_tmp = source_cached_columns.at(i);
                if (target_tmp != NULL && strcmp(target_tmp->get_col_name(), 
                    (const char *)(NameStr(column_ref_data.m_column_name))) == 0) { /* found that column is encryped */
                    v_res.push(target_tmp);
                    is_target_found = true;
                    is_source_found = true;
                    break;
                }
            }
        }
    }

    ColumnHookExecutor *rarg_exe = NULL;
    ColumnHookExecutor *larg_exe = NULL;
    for (size_t i = 0; i < v_res.size(); ++i) {
        const ICachedColumn *tmp = v_res.at(i);
        if (i == 0) {
            larg_exe = tmp->get_column_hook_executors()->at(0);
            continue;
        } else if (i == 1) {
            rarg_exe = tmp->get_column_hook_executors()->at(0);
        }
    }
    if (larg_exe != rarg_exe) {
        printfPQExpBuffer(&statement_data->conn->errorMessage, 
            "ERROR(CLIENT): operator is not allowed on encrypted columns with different encryption keys\n");
        return false;
    }

    ListCell *l = NULL;
    foreach (l, stmt->mergeWhenClauses) {
        MergeWhenClause *mergeWhenClause = (MergeWhenClause*)lfirst(l);
        ExprPartsList where_expr_list;
        ret = exprProcessor::expand_expr(mergeWhenClause->condition, statement_data, &where_expr_list);
        RETURN_IF(!ret, false);
        ret = WhereClauseProcessor::process(&cached_columns, &where_expr_list, statement_data);
        RETURN_IF(!ret, false);
    }
    return true;
}

bool Processor::run_pre_query(StatementData *statement_data, bool is_inner_query, bool *failed_to_parse)
{
    PGconn *conn = statement_data->conn;
    Assert(conn->client_logic && conn->client_logic->enable_client_encryption);
    Assert(!conn->client_logic->rawValuesForReplace->m_raw_values);
    if (statement_data->query == nullptr) {
        return false;
    }
    statement_data->params.adjusted_query = statement_data->query;
    statement_data->copy_params();

    /*
     * func : call create() function instead but we need to make sure to always delete the prepared statements when they
     * are no longer in use
     */
    conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName); 
    /* just create a default one */
    check_memcpy_s(memcpy_s(conn->client_logic->lastStmtName, NAMEDATALEN, statement_data->stmtName,
        strlen(statement_data->stmtName) + 1));
    ListCell *stmt_iter = NULL;
    List *stmts = Parser::Parse(statement_data->conn->client_logic, statement_data->query);
    foreach (stmt_iter, stmts) {
        Node *stmt = (Node *)lfirst(stmt_iter);
        if (!run_pre_statement(stmt, statement_data)) {
            /* in inner query in function parse, it is normal when it return false, so it need not clear memery */
            if (!is_inner_query) {
                run_post_query(conn, true);
            }
            return false;
        }
    }
    statement_data->replace_raw_values();
    if (!is_inner_query) {
        free_memory();
    } else { 
        statement_data->conn->client_logic->rawValuesForReplace->clear();
    }
    /* some callers may want to know if the batch query passed was parsed successfully by the bison parser */
    if (failed_to_parse != NULL) { 
        if (stmts != NULL) { 
            *failed_to_parse = false;
        } else {
            *failed_to_parse = true;
        }
    }
    return true;
}

bool Processor::run_pre_exec(StatementData *statement_data)
{
    Assert(statement_data->conn->client_logic && statement_data->conn->client_logic->enable_client_encryption);
    Assert(!statement_data->conn->client_logic->rawValuesForReplace->m_raw_values);
    statement_data->copy_params();
    check_memcpy_s(memcpy_s(statement_data->conn->client_logic->lastStmtName, NAMEDATALEN, statement_data->stmtName,
        strlen(statement_data->stmtName) + 1));
    PreparedStatement *prepares_statement =
        statement_data->conn->client_logic->preparedStatements->get_or_create(statement_data->stmtName);
    if (!prepares_statement) {
        return prepares_statement;
    }

    if (!prepares_statement->cached_params || prepares_statement->cached_params->is_empty()) {
        return true; /* no columns to process */
    }

    RawValuesList raw_values_list;
    if (!raw_values_list.gen_values_from_statement(statement_data)) {
        return false;
    }

    if (!ValuesProcessor::process_values(statement_data, prepares_statement->cached_params, 1,
        &raw_values_list)) {
        return true;
    }

    statement_data->replace_raw_values();
    return true;
}
