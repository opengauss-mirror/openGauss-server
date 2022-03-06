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
 * stmt_processor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\stmt_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STMT_PROCESSOR_H
#define STMT_PROCESSOR_H

#include <string>
#include "parser/scanner.h"
#include "client_logic_cache/icached_columns.h"
typedef struct pg_conn PGconn;

class CStringsMap;
typedef struct StatementData StatementData;
typedef struct PGClientLogicParams PGClientLogicParams;
class ExprPartsList;
/* *
 * @class Processor
 *
 *
 */
class Processor {
public:
    static bool run_pre_query(StatementData *statement_data, bool is_inner_query = false, bool *failed_to_parse = NULL);
    static bool run_pre_exec(StatementData *statement_data);
    /* *
     * 	CREATE commands require running functions later to update local cache
     */
    static bool run_post_query(PGconn *conn, bool force_error = false);
    static bool accept_pending_statements(PGconn *conn, bool isSuccess = true);
    static bool deal_order_by_statement(const SelectStmt * const select_stmt, ICachedColumns *select_cached_columns,
    		StatementData *statement_data);
    static bool run_pre_statement(const Node * const stmt, StatementData *statement_data);
    static const int MAX_KEY_ADD_LEN = 1024;

private:
    /* *
     * \defgroup PreStatement Pre statment functions
     * @{
     * Functions that run before or after statements that might affect Client Logic
     * 	-the following SQL statements requite treatment before being executed on server:
     * 	  - SELECT
     * 	  - INSERT
     * 	  - UPDATE
     * 	  - DELETE
     * 	  - PREPARE
     * 	  - EXECUTE
     * 	  - DECLARE
     * 	  - COPY
     * 	  - CREATE TABLE that one or more of the columns are cached columns for Client Logic
     * 	  - CREATE CLIENT LOGIC GLOBAL SETTING
     * 	  - CREATE CLIENT LOGIC COLUMN SETTING
     *
     * 	  all those functions are declared in stmt_processor.h but RunPreCreateStatment
     * 	  which is declared and defined in different file
     */
    /*
     * with cte as([select,insert,update,delete]) select from cte where .so we need to get CacheColumn from cte.
     */
    static bool run_pre_select_statement(const SelectStmt * const select_stmt, StatementData *statement_data);
    static bool run_pre_select_statement(const SelectStmt * const select_stmt, const SetOperation &parent_set_operation,
        const bool &parent_all, StatementData *statement_data, ICachedColumns *cacehd_columns = nullptr,
        ICachedColumns *cached_columns_parents = nullptr);
    static bool process_select_set_operation(const SelectStmt * const select_stmt, 
        StatementData *statement_data, ICachedColumns *cached_columns);
    static bool run_pre_insert_statement(const InsertStmt * const insert_stmt, StatementData *statement_data,
        ICachedColumns *cached_columns = nullptr);
    static bool run_pre_update_statement(const UpdateStmt *update_stmt, StatementData *statement_data,
        ICachedColumns *cached_columns = nullptr);
    static bool run_pre_update_statement_set(const UpdateStmt * const update_stmt, StatementData *statement_data);
    static bool run_pre_update_statement_where(const UpdateStmt * const update_stmt, StatementData *statement_data);
    static bool run_pre_delete_statement(const DeleteStmt * const delete_stmt, StatementData *statement_data,
        ICachedColumns *cached_columns = nullptr);
    static bool run_pre_prepare_statement(const PrepareStmt * const prepare_stmt, StatementData *statement_data);
    static bool run_pre_execute_statement(const ExecuteStmt * const execute_stmt, StatementData *statement_data);
    static bool run_pre_declare_cursor_statement(const DeclareCursorStmt * const declare_cursor_stmt,
        StatementData *statement_data);
    static bool run_pre_copy_statement(const CopyStmt * const stmt, StatementData *statement_data);
    static bool run_pre_alter_table_statement(const AlterTableStmt * const stmt, StatementData *statement_data);
    static bool alter_add_constraint(const AlterTableCmd *cmd, 
        ICachedColumns *cached_columns, StatementData *statement_data);
    static bool run_pre_column_setting_statement(PGconn *conn, const char *stmt_name,
        CreateClientLogicColumn *client_logic_column, const char *query, PGClientLogicParams &params);
    static bool run_pre_cached_global_setting(PGconn *conn, const char *stmt_name,
        CreateClientLogicGlobal *global_setting);
    static bool run_pre_drop_statement(const DropStmt *stmt, StatementData *statement_data);
    static bool run_pre_drop_table_statement(const DropStmt *stmt, StatementData *statement_data);
    static bool run_pre_drop_schema_statement(const DropStmt *stmt, StatementData *statement_data);
    static bool run_pre_set_statement(const VariableSetStmt *stmt, StatementData *statement_data);
    static bool run_pre_exec_direct_statement(const ExecDirectStmt *stmt, StatementData *statement_data);
    static bool run_pre_create_function_stmt(const CreateFunctionStmt *stmt, StatementData *statement_data);
    static bool run_pre_create_rlspolicy_stmt(const CreateRlsPolicyStmt *stmt, StatementData *statement_data);
    static bool run_pre_alter_rlspolicy_stmt(const AlterRlsPolicyStmt *stmt, StatementData *statement_data);
    static bool run_pre_rlspolicy_using(const Node *stmt, StatementData *statement_data);
    static const bool remove_droppend_schemas(PGconn *conn, const bool is_success);
    static void remove_dropped_global_settings(PGconn *conn, const bool is_success);
    static void remove_dropped_column_settings(PGconn *conn, const bool is_success);
    static bool is_set_operation_allowed(const SelectStmt * const select_stmt, const ICachedColumns *cached_columns,
        const SetOperation &parent_select_operation, const bool &select_cached_columns, StatementData *statement_data);
    static bool is_set_operation_allowed_on_datatype(const ICachedColumns *cached_columns,
        const CStringsMap &col_alias_map);
    static bool find_in_name_map(const CStringsMap &col_alias_map, const char *name);
    /*
     * from a1,(select *) as b, a2 join c
     */
    static bool run_pre_from_list_statement(const List * const from_list, StatementData *statement_data,
        ICachedColumns *cached_columns, const ICachedColumns *cached_columns_parents);
    static bool run_pre_from_item_statement(const Node * const from_item, StatementData *statement_data,
        ICachedColumns *cached_columns, const ICachedColumns *cached_columns_parents = nullptr);
    /*
     * a2 join c
     */
    static bool run_pre_join_statement(const JoinExpr * const join_stmt, StatementData *statement_data,
        ICachedColumns *cached_columns);
    static bool join_with_same_key(List *usingClause, ICachedColumns *cached_larg, ICachedColumns *cached_rarg,
        StatementData *statement_data);
    /* from RangeVar */
    static bool run_pre_range_statement(const RangeVar * const range_var, StatementData *statement_data,
    ICachedColumns *cached_columns, const ICachedColumns* cached_columns_parents);
    /*
     * with cte1(), cte2()
     */
    static bool run_pre_with_list_statement(const List * const with_list, StatementData *statement_data,
        ICachedColumns *cached_columns);
    static bool run_pre_with_item_statement(const Node * const with_item, StatementData *statement_data,
        ICachedColumns *cached_columns);
    /*
     * insert/delete/update returning list [target list]
     */
    static bool run_pre_returning_list_statement(const List *returning_list, ICachedColumns *cached_columns_from,
        ICachedColumns *cachedColumns, StatementData *statement_data);
    static bool run_pre_merge_stmt(const MergeStmt *stmt, StatementData *statement_data);

    static bool get_column_names_from_target_list(const List *target_list, CStringsMap *col_alias_map,
        StatementData *statement_data);
    static bool run_pre_select_target_list(List *targetList, StatementData *statementData,
        ExprPartsList *funcExprPartsList);
    static bool process_res_target(ResTarget *resTarget, StatementData *statementData,
        ExprPartsList *funcExprPartsList);
};

#endif
