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
 * statement_data.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_common\statement_data.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "statement_data.h"
#include <algorithm>
#include <iostream>

StatementData::~StatementData()
{
    params.new_query_size = 0;
    libpq_free(params.new_query);
}

ICachedColumnManager* StatementData::GetCacheManager() const
{
    return conn->client_logic->m_cached_column_manager;
}

const size_t StatementData::get_total_change() const
{
    size_t total_change = 0;
    size_t size = conn->client_logic->rawValuesForReplace->size();
    for (size_t i = 0; i < size; ++i) {
        RawValue *raw_value = conn->client_logic->rawValuesForReplace->at(i);
        if (!raw_value) {
            continue;
        }
        total_change += raw_value->m_processed_data_size - raw_value->m_data_size;
    }
    return total_change;
}
void StatementData::replace_raw_values()
{
    if (conn->client_logic->rawValuesForReplace->empty()) {
        return;
    }
    size_t size = conn->client_logic->rawValuesForReplace->size();
    const size_t total_change = get_total_change();
    if (!total_change) {
        return;
    }
    conn->client_logic->rawValuesForReplace->sort_by_location();

    /*
     * we are iterating over the array from top to bottom
     * the location of the value to replace is always correct in the loop because we start from the end
     */
    int i = (int)(size - 1);
    for (; i >= 0; --i) {
        RawValue *raw_value = conn->client_logic->rawValuesForReplace->at(i);
        /*
         * if the raw value is null, there is no need to replace it.
         * if is param, there is no need to replace it in the query, as the query is already saved in the server
         * the param is being saved in the list only for replacing value in post query in case of error
         */
        if (!raw_value || raw_value->m_is_param || !raw_value->m_processed_data) {
            continue;
        }
        if (raw_value->m_location > params.new_query_size) {
            fprintf(stderr, "wrong new location value: %zu, max allowed is: %zu\n", raw_value->m_location,
                params.new_query_size);
            continue;
        }
        size_t new_size = 0;
        const unsigned char *new_str = NULL;
        size_t original_size = 0;
        if (raw_value->m_data_size > 0) {
            original_size = raw_value->m_data_size;
            if (raw_value->m_empty_repeat) {
                new_size = strlen("''");
                new_str = (const unsigned char *)"''";
            } else {
                new_size = raw_value->m_processed_data_size;
                new_str = raw_value->m_processed_data;
            }
        } else {
            original_size = 0;
            new_size = raw_value->m_processed_data_size;
            new_str = raw_value->m_processed_data;
        }

        /* expand buffer */
        if (new_size > original_size) {
            params.new_query = (char *)libpq_realloc(params.new_query, params.new_query_size,
                params.new_query_size + (new_size - original_size) + 1);
            if (params.new_query == NULL) {
                return;
            }
        }
        /* move to the right */
        size_t dest_max = params.new_query_size - raw_value->m_location - original_size;
        if (dest_max > 0) {
            check_memmove_s(memmove_s(params.new_query + raw_value->m_location + new_size,
                dest_max,
                params.new_query + raw_value->m_location + original_size,
                params.new_query_size - raw_value->m_location - original_size));
        }
        /* shrink buffer */
        if (new_size < original_size) {
            params.new_query = (char *)libpq_realloc(params.new_query, params.new_query_size,
                params.new_query_size + (new_size - original_size) + 1);
            if (params.new_query == NULL) {
                return;
            }
        }

        /* copy data */
        params.new_query_size += (new_size - original_size);
        if (new_str != NULL) {
            check_memcpy_s(memcpy_s(params.new_query + raw_value->m_location,
                params.new_query_size - raw_value->m_location, new_str, new_size));
        }
        params.new_query[params.new_query_size] = '\0';
    }

    params.adjusted_query = params.new_query;
    params.adjusted_query_size = params.new_query_size;
    conn->client_logic->raw_values_for_post_query.merge_from(conn->client_logic->rawValuesForReplace);
}

void StatementData::copy_params()
{
    stmtName = stmtName ? stmtName : "";
    if (paramTypes) {
        libpq_free(params.adjusted_paramTypes);
        params.adjusted_paramTypes = (Oid *)calloc(nParams, sizeof(Oid));
        if (params.adjusted_paramTypes == NULL) {
            return;
        }
        check_memcpy_s(memcpy_s(params.adjusted_paramTypes, nParams * sizeof(Oid), paramTypes, nParams * sizeof(Oid)));
    }
    if (paramValues) {
        libpq_free(params.adjusted_param_values);
        params.adjusted_param_values = (const char **)calloc(nParams, sizeof(const char *));
        if (params.adjusted_param_values == NULL) {
            return;
        }
        for (size_t i = 0; i < nParams; ++i) {
            params.adjusted_param_values[i] = paramValues[i];
        }
    }
    if (paramLengths) {
        libpq_free(params.adjusted_param_lengths);
        params.adjusted_param_lengths = (int *)calloc(nParams, sizeof(int));
        if (params.adjusted_param_lengths == NULL) {
            return;
        }
        check_memcpy_s(
            memcpy_s(params.adjusted_param_lengths, nParams * sizeof(int), paramLengths, nParams * sizeof(int)));
    }
}
