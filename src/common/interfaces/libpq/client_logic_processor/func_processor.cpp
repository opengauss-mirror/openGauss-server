/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * func_processor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\func_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "func_processor.h"
#include "stmt_processor.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_expressions/expr_parts.h"
#include "client_logic_expressions/expr_parts_list.h"
#include "prepared_statements_list.h"
#include "raw_values_cont.h"
#include "values_processor.h"
#include "func_hardcoded_values.h"

/**
    process the query in CREATE FUNCTION, CREATE PROCEDURE or DO
    @param[in] list of DefElem nodes - they are derieved either from the CreateFunctionStmt object or the DoStmt object
    @param[in] StatementData - the current client logic state machine for the query
    @return boolean, returns true or false for severe, unexpected errors
*/

bool func_processor::run_pre_create_function_stmt(const List* options, StatementData *statement_data, bool is_do_stmt)
{
    FuncHardcodedValues::process(options, statement_data, is_do_stmt);
    return true; // ignore error
}

bool func_processor::process(const ExprPartsList* target_expr_vec, StatementData* statement_data)
{
    if (!statement_data->conn || !statement_data->query) {
        fprintf(stderr, "invalid input! statementData->query: %s\n", statement_data->query);
        return false;
    }
    PreparedStatement* prepared_statement =
        statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    if (prepared_statement == NULL) {
        return false;
    }
    prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
    if (target_expr_vec->size() == 0) {
        return true;
    }
    CachedColumns v_res(false);

    RawValuesList raw_values_list;
    if (!RawValues::get_raw_values_from_consts_vec(target_expr_vec, statement_data, 0, &raw_values_list)) {
        fprintf(stderr, "failed to get raw values\n");
        return false;
    }
    bool isAnyColumns = false;
    for (size_t i = 0; i < target_expr_vec->size(); i++) {
        const ExprParts* target_expr_parts = target_expr_vec->at(i);
        if (target_expr_parts == NULL) {
            return false;
        } else if (target_expr_parts->cl_column_oid == InvalidOid) {
            v_res.push(NULL);
        } else {
            const ICachedColumn* icol =
                statement_data->GetCacheManager()->get_cached_column(target_expr_parts->cl_column_oid);
            v_res.push(icol);
            if (icol) {
                isAnyColumns = true;
            }
        }
    }
    if (!isAnyColumns) {
        return true;
    }
    /* one row */
    return ValuesProcessor::process_values(statement_data, &v_res, 1, &raw_values_list);
}
