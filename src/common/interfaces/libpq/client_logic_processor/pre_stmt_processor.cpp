/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * pre_stmt_processor.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_processor\pre_stmt_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "pg_config.h"

#include "stmt_processor.h"
#include "cl_state.h"
#include "client_logic_cache/cache_refresh_type.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_common/statement_data.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "prepared_statement.h"
#include "prepared_statements_list.h"

#include <iostream>

#define BYTEAWITHOUTORDERWITHEQUALCOL_NAME "byteawithoutorderwithequalcol"
#define BYTEAWITHOUTORDERCOL_NAME "byteawithoutordercol"

bool Processor::run_pre_prepare_statement(const PrepareStmt *prepare_stmt, StatementData *statement_data)
{
    if (statement_data->GetCacheManager()->is_cache_empty()) {
        return true;
    }

    /*
     * func : call create() function instead but
     * we need to make sure to always delete the prepared statements when they
     * are no longer in use
     */
    (void)statement_data->conn->client_logic->pendingStatements->get_or_create(prepare_stmt->name);
    statement_data->stmtName = prepare_stmt->name;
    bool pre_ret = run_pre_statement(prepare_stmt->query, statement_data);
    if (pre_ret) {
        pre_ret = process_prepare_arg_types(prepare_stmt, statement_data);
    }
    return pre_ret;
}

/*
 * @Description: replace original arg types for cl parameters in prepare statement declaration
 * with corresponding client logic type to pass server side verification
 * @IN PrepareStmt: prepare statement
 * @IN StatementData: query data for client logic
 */
bool Processor::process_prepare_arg_types(const PrepareStmt *prepare_stmt, StatementData *statement_data)
{
    PreparedStatement *current_statement =
        statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
    bool is_invalid_args = current_statement == NULL || current_statement->cached_params == NULL ||
        current_statement->cached_params->size() == 0;
    if (is_invalid_args) {
        return true;
    }
    List *arg_types_list = prepare_stmt->argtypes;
    if (arg_types_list == 0) {
        return true;
    }
    ListCell *curr_arg = list_head(arg_types_list);
    /* estimate new query size change */
    size_t new_size = 0;
    bool need_replacement = false;
    for (unsigned int i = 0; i < current_statement->cached_params->size() && curr_arg != NULL;
        i++, curr_arg = lnext(curr_arg)) {
        const ICachedColumn *cached_column = current_statement->cached_params->at(i);
        /* check if column has policy for processing */
        const char *datatype = NULL;
        if (cached_column != NULL) {
            if (cached_column->get_data_type() == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
                datatype = BYTEAWITHOUTORDERWITHEQUALCOL_NAME;
            } else if (cached_column->get_data_type() == BYTEAWITHOUTORDERCOLOID) {
                datatype = BYTEAWITHOUTORDERCOL_NAME;
            } else {
                continue;
            }
            if (datatype != NULL) {
                need_replacement = true;
                new_size += strlen(datatype) -
                    (((TypeName *)lfirst(curr_arg))->end_location - ((TypeName *)lfirst(curr_arg))->location);
            }
        }
    }
    if (!need_replacement) {
        return true;
    }
    new_size += strlen(statement_data->params.adjusted_query);
    char *new_query = (char *)malloc(new_size + 1);
    if (new_query == NULL) {
        return false;
    }
    /* construct new quuery */
    arg_types_list = prepare_stmt->argtypes;
    curr_arg = list_head(arg_types_list);
    int new_offset = 0, old_offset = 0;
    for (unsigned int i = 0; i < current_statement->cached_params->size() && curr_arg != NULL;
        i++, curr_arg = lnext(curr_arg)) {
        const char *datatype = NULL;
        const ICachedColumn *cached_column = current_statement->cached_params->at(i);
        /* check if column has policy for processing */
        if (cached_column != NULL) {
            if (cached_column->get_data_type() == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
                datatype = BYTEAWITHOUTORDERWITHEQUALCOL_NAME;
            } else if (cached_column->get_data_type() == BYTEAWITHOUTORDERCOLOID) {
                datatype = BYTEAWITHOUTORDERCOL_NAME;
            } else {
                continue;
            }
            if (datatype != NULL) {
                check_strncpy_s(strncpy_s(new_query + new_offset, new_size + 1 - new_offset,
                    statement_data->params.adjusted_query + old_offset,
                    ((TypeName *)lfirst(curr_arg))->location - old_offset));
                new_offset += ((TypeName *)lfirst(curr_arg))->location - old_offset;
                old_offset = ((TypeName *)lfirst(curr_arg))->end_location;
                check_strcpy_s(strcpy_s(new_query + new_offset, new_size + 1 - new_offset, datatype));
                new_offset += strlen(datatype);
            }
        }
    }
    /* copy the rest */
    check_strcpy_s(strcpy_s(new_query + new_offset, new_size + 1 - new_offset,
        statement_data->params.adjusted_query + old_offset));
    libpq_free(statement_data->params.new_query);
    statement_data->params.new_query = new_query;
    statement_data->params.new_query_size = new_size;
    statement_data->params.adjusted_query = statement_data->params.new_query;
    return true;
}