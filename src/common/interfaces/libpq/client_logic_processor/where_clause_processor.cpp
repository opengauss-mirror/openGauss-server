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
 * where_clause_processor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\where_clause_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>
#include "where_clause_processor.h"
#include "raw_values_cont.h"
#include "values_processor.h"
#include "client_logic_cache/icached_column.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_expressions/column_ref_data.h"
#include "client_logic_expressions/expr_processor.h"
#include "client_logic_expressions/expr_parts_list.h"
#include "client_logic_cache/column_hook_executors_list.h"
#include "client_logic/cstrings_map.h"
void check_column_equal(ColumnRefData &column_ref_data, const ICachedColumn *cached_column, bool &found,
    const ICachedColumn **cached_column_key)
{
    bool if_euqal = column_ref_data.compare_with_cachecolumn(cached_column);
    if (if_euqal) {
        found = true;
        *cached_column_key = cached_column;
    }
}

bool col_keys_are_same(const ICachedColumns *cached_columns, const ExprPartsList *where_expr_vec,
    StatementData *statement_data, size_t i)
{
    bool expr_valid = where_expr_vec->at(i)->column_refl != NULL && where_expr_vec->at(i)->column_refr != NULL;
    if (expr_valid) {
        ColumnRefData column_refl_data;
        ColumnRefData column_refr_data;
        bool invalid_field =
            !exprProcessor::expand_column_ref(where_expr_vec->at(i)->column_refl, column_refl_data) ||
            !exprProcessor::expand_column_ref(where_expr_vec->at(i)->column_refr, column_refr_data);
        if (invalid_field) {
            Assert(false);
            return false;
        } 
        const ICachedColumn *cached_columnl(NULL);
        const ICachedColumn *cached_columnr(NULL);
        bool find_left = false;
        bool find_right = false;
        for (size_t i = 0; i < cached_columns->size(); i++) {
            const ICachedColumn *cached_column = cached_columns->at(i);
            check_column_equal(column_refl_data, cached_column, find_left, &cached_columnl);
            check_column_equal(column_refr_data, cached_column, find_right, &cached_columnr);
            if (find_left && find_right) {
                break;
            }
        }
        bool column_found = find_left && find_right && cached_columnl != NULL && cached_columnr != NULL;
        bool different_key = (find_left || find_right) && cached_columnl != NULL && cached_columnr != NULL;
        if (column_found) {
            ColumnHookExecutor *larg_exe = cached_columnl->get_column_hook_executors()->at(0);
            ColumnHookExecutor *rarg_exe = cached_columnr->get_column_hook_executors()->at(0);
            if (larg_exe != rarg_exe) {
                printfPQExpBuffer(&statement_data->conn->errorMessage,
                    "ERROR(CLIENT): equal operator is not allowed on columns with different key\n");
                return false;
            }
        } else if (different_key) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                "ERROR(CLIENT): equal operator is not allowed on columns with different key\n");
            return false;
        }
    }
    return true;
}

bool WhereClauseProcessor::process(const ICachedColumns *cached_columns, const ExprPartsList *where_expr_vec,
    StatementData *statement_data, const CStringsMap* target_list)
{
    /*
        rewriting the where clause is only required if there are columns to be processed
    */
    bool cache_empty = !cached_columns || cached_columns->is_empty();
    if (cache_empty) {
        return true;
    }

    /*
        validating input
    */
    bool state_empty = !statement_data->conn || !statement_data->query;
    if (state_empty) {
        fprintf(stderr, "invalid input! statement_data->query: %s\n", statement_data->query);
        return false;
    }

    CachedColumns v_res(false);

    RawValuesList raw_values_list;
    if (!RawValues::get_raw_values_from_consts_vec(where_expr_vec, statement_data, 0, &raw_values_list)) { 
        fprintf(stderr, "failed to get raw values\n");
        return false;
    }
    bool is_any_columns = false;
    for (size_t i = 0; i < where_expr_vec->size(); i++) {
        bool is_found = false;
        if (where_expr_vec->at(i)->column_ref != NULL) {
            /* a.col1 = b.col2 we need to judge col1 and col2 share the same key */
            if(!col_keys_are_same(cached_columns, where_expr_vec, statement_data, i)) {
                return false;
            }
            ColumnRefData column_ref_data;
            if (!exprProcessor::expand_column_ref(where_expr_vec->at(i)->column_ref, column_ref_data)) { 
                Assert(false);
                return false;
            }
            size_t cached_columns_size = cached_columns->size();
            for (size_t i = 0; i < cached_columns_size; ++i) {
                const ICachedColumn* cached_column = cached_columns->at(i);
                const char* colname_inquery = 
                    (target_list && target_list->Size() > 0) ? target_list->find(column_ref_data.m_alias_fqdn) : NULL;
                if (column_ref_data.compare_with_cachecolumn(cached_column, colname_inquery)) {
                    /* found that column is encryped */
                    v_res.push(cached_column);
                    is_any_columns = true;
                    is_found = true;
                    break;
                }
            }
        } else if (where_expr_vec->at(i)->cl_column_oid != InvalidOid) {
            const ICachedColumn *icol =
                statement_data->GetCacheManager()->get_cached_column(where_expr_vec->at(i)->cl_column_oid);
            if (icol) {
                v_res.push(icol);
                is_any_columns = true;
                is_found = true;
            }
        }

        /* if the column is not processed, we need to push null to vector for not processing its value */
        if (!is_found) {
            v_res.push(NULL);
        }
    }

    if (!is_any_columns) { 
        return true;
    }

    return ValuesProcessor::process_values(statement_data, &v_res,  1, &raw_values_list);
}
