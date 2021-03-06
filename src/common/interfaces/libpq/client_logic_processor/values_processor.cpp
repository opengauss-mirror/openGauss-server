/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * values_processor.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_processor\values_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <climits>
#include <iostream>
#include "values_processor.h"
#include "raw_value.h"
#include "raw_values_list.h"
#include "prepared_statement.h"
#include "prepared_statements_list.h"
#include "client_logic_cache/icached_column_manager.h"
#include "client_logic_cache/icached_column.h"
#include "client_logic_cache/cached_column.h"
#include "client_logic_common/pg_client_logic_params.h"
#include "client_logic_common/statement_data.h"
#include <algorithm>
#include "client_logic_fmt/gs_fmt.h"
#include "client_logic_hooks/hooks_manager.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_cache/dataTypes.def"
#include "client_logic_cache/types_to_oid.h"

static void process_prepare_state(const RawValue *raw_value, StatementData *statement_data)
{
    if (raw_value->m_is_param) {
        /*
         * prepared statements
         * keep the \0 in the end of the string when in textual mode.
         */
        int copy_size =
            (raw_value->m_processed_data_size + (raw_value->m_data_value_format ? 0 : 1)) * sizeof(unsigned char);
        if (!statement_data->params.new_param_values) {
            Assert(!statement_data->params.copy_sizes);
            statement_data->params.new_param_values =
                (unsigned char **)calloc(sizeof(unsigned char *), statement_data->nParams);
            statement_data->params.copy_sizes = (size_t *)calloc(sizeof(size_t), statement_data->nParams);
            statement_data->params.nParams = statement_data->nParams;
        }
        statement_data->params.new_param_values[raw_value->m_location] = (unsigned char *)malloc(copy_size);
        if (statement_data->params.new_param_values[raw_value->m_location] == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when processing state\n");
            return;
        }
        statement_data->params.copy_sizes[raw_value->m_location] = copy_size;
        check_memcpy_s(memcpy_s(statement_data->params.new_param_values[raw_value->m_location], copy_size,
            raw_value->m_processed_data, copy_size));
        if (raw_value->m_location < statement_data->nParams) {
            if (!statement_data->params.adjusted_param_values) {
                statement_data->params.adjusted_param_values =
                    (const char **)malloc(statement_data->nParams * sizeof(const char *));
                if (statement_data->params.adjusted_param_values ==  NULL) {
                    fprintf(stderr, "ERROR(CLIENT): out of memory when processing state\n");
                    return;
                }
                statement_data->params.nParams = statement_data->nParams;
            }
            Assert(!statement_data->params.new_param_values[raw_value->m_location]);
            statement_data->params.adjusted_param_values[raw_value->m_location] =
                (const char *)statement_data->params.new_param_values[raw_value->m_location];
            if (!statement_data->params.adjusted_param_lengths) {
                statement_data->params.adjusted_param_lengths = (int *)malloc(statement_data->nParams * sizeof(int));
                if (statement_data->params.adjusted_param_lengths == NULL) {
                    fprintf(stderr, "ERROR(CLIENT): out of memory when processing state\n");
                    return;
                }
                statement_data->params.nParams = statement_data->nParams;
            }
            statement_data->params.adjusted_param_lengths[raw_value->m_location] = raw_value->m_processed_data_size;
        }
    }
}

static bool process_inside_value(const StatementData *statement_data, RawValue *raw_value,
    const ICachedColumn *cached_column)
{
    char err_msg[MAX_ERRMSG_LENGTH];
    errno_t rc = EOK;
    rc = memset_s(err_msg, MAX_ERRMSG_LENGTH, 0, MAX_ERRMSG_LENGTH);
    securec_check_c(rc, "\0", "\0");
    if (!raw_value->process(cached_column, err_msg)) {
        if (strlen(err_msg) == 0) {
            printfPQExpBuffer(&statement_data->conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): failed to process data of processed column\n"));
        } else {
            printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext("ERROR(CLIENT): %s\n"), err_msg);
        }
        return false;
    }
    return true;
}

static bool save_prepared_statement(const StatementData *statement_data, const RawValue *raw_value,
    const ICachedColumn *cached_column)
{
    if (statement_data->stmtName && raw_value->m_is_param) {
        PreparedStatement *prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            return false;
        }
        if (!prepared_statement->cached_params) {
            prepared_statement->cached_params = new (std::nothrow) CachedColumns(false, false, true);
            if (prepared_statement->cached_params == NULL) {
                fprintf(stderr, "failed to new CachedColumns object\n");
                return false;
            }
        }

        ICachedColumn *prepared_cached_column = new (std::nothrow) CachedColumn(cached_column);
        if (prepared_cached_column == NULL) {
            fprintf(stderr, "failed to new CachedColumn object\n");
            return false;
        }
        prepared_cached_column->set_use_in_prepare(true);
        prepared_statement->cached_params->set(raw_value->m_location, prepared_cached_column);
    }
    return true;
}

bool ValuesProcessor::process_values(StatementData *statement_data, const ICachedColumns *cached_columns,
    const size_t rows_count, RawValuesList *raw_values)
{
    bool check_para_valid =
        !statement_data || !cached_columns || !raw_values || raw_values->empty() || (rows_count == 0);
    if (check_para_valid) {
        Assert(false);
        return false;
    }

    if (!cached_columns->is_to_process()) {
        return true;
    }

    bool is_any_relevant(false);

    /* replace original values with processed values */
    size_t values_per_row_count = raw_values->size() / rows_count;
    for (size_t m = 0; m < rows_count; ++m) {
        size_t cached_columns_size = cached_columns->size();
        for (size_t i = 0; i < cached_columns_size; ++i) {
            const ICachedColumn *cached_column = cached_columns->at(i);
            /* check if column has policy for processing */
            if (!cached_column) {
                continue;
            }

            /*
             * if we have the column name in the query then use its position in the query.
             * otherwise, use the order from the table.
             */
            RawValue *raw_value = NULL;
            size_t raw_value_index = 0;
            if (cached_columns->is_in_scheme_order()) {
                /* this can happen when "INSERT INTO TABLE VALUES" contains less values than the number of columns in
                 * the original table */
                if (cached_column->get_col_idx() > values_per_row_count) {
                    continue;
                }

                size_t column_index = cached_column->get_col_idx() - 1;
                raw_value_index = column_index + (m * values_per_row_count);
            } else {
                raw_value_index = i + (m * values_per_row_count);
            }

            /* precaution */
            if (raw_value_index >= raw_values->size()) {
                Assert(false);
                continue;
            }

            raw_value = raw_values->at(raw_value_index);
            /* rawValue in CREATE TABLE could be without a default value */
            if (!raw_value) {
                continue;
            }

            /* save the prepared statement's parameters (if the rawValue is defined as a parameter such as $1, $2) */
            if (!save_prepared_statement(statement_data, raw_value, cached_column)) {
                return false;
            }
            /* rawValue in INSERT could be NULL */
            if (!raw_value->m_data_value) {
                continue;
            }

            /*
             * at this point, we know for certain that the value will be replaced
             * if raw value is param, the query might be empty
             */
            bool raw_is_param = !is_any_relevant && !raw_value->m_is_param;
            if (raw_is_param) {
                is_any_relevant = true;

                /* rewrite query with new query */
                statement_data->params.new_query_size = strlen(statement_data->query);
                libpq_free(statement_data->params.new_query);
                statement_data->params.new_query = (char *)malloc(statement_data->params.new_query_size + 1);
                if (statement_data->params.new_query == NULL) {
                    fprintf(stderr, "ERROR(CLIENT): out of memory when processing data\n");
                    return false;
                }
                check_strncpy_s(strncpy_s(statement_data->params.new_query, statement_data->params.new_query_size + 1,
                    statement_data->query, statement_data->params.new_query_size));
                statement_data->params.new_query[statement_data->params.new_query_size] = '\0';
            }

            /* process the data inside the rawValue */
            if (!process_inside_value(statement_data, raw_value, cached_column)) {
                return false;
            }
            process_prepare_state(raw_value, statement_data);

            /*
             * 1. realign locations inside the rawValue after data was processed and probably enlarged
             * 2. add the rawValue to the list of rawValues intended for replacement in the original query to be sent to
             * the client
             */
            if (!raw_value->m_is_param) {
                int size_diff = (int)raw_value->m_processed_data_size - (int)raw_value->m_data_size;
                statement_data->offset += size_diff;
                for (size_t j = 1 + (raw_value_index + (m * values_per_row_count)); j < raw_values->size(); ++j) {
                    if (raw_values->at(j) && !raw_values->at(j)->m_is_param) {
                        raw_values->at(j)->m_new_location += size_diff;
                    }
                }
                statement_data->conn->client_logic->rawValuesForReplace->add(raw_value);
                raw_values->erase(raw_value_index, false);
            }
        }
    }

    if (is_any_relevant) {
        statement_data->params.adjusted_query = statement_data->params.new_query;
    }
    return true;
}

DecryptDataRes ValuesProcessor::deprocess_value(PGconn *conn, const unsigned char *processed_data,
    size_t processed_data_size, int original_typeid, int format, unsigned char **plain_text, size_t &plain_text_size,
    bool is_default)
{
    /* unescape data from its BYTEA format */
    size_t unescaped_processed_data_size = 0;
    unsigned char *unescaped_processed_data = NULL;
    DecryptDataRes dec_dat_res = DEC_DATA_ERR;
    int plain_text_size_tmp = 0;
    errno_t rc = EOK;

    if (format) { /* binary */
        unescaped_processed_data = (unsigned char *)processed_data;
        unescaped_processed_data_size = processed_data_size;
    } else {
        /* if the string is not NULL terminated,it cannot be send to PQunescapeBytea */
        unsigned char *processed_data_tmp =
            (unsigned char *)malloc((1 + processed_data_size) * sizeof(unsigned char));
        if (processed_data_tmp == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when decrypting data\n");
            return CLIENT_HEAP_ERR;
        }
        rc = memcpy_s(processed_data_tmp, processed_data_size + 1, processed_data, processed_data_size);
        securec_check_c(rc, "\0", "\0");
        processed_data_tmp[processed_data_size] = 0;

        const char *final = strchr((char *)processed_data_tmp, ':');
        /*
         * in case of default values, the data arrives as '\x/COLUMN_SETTING_OID/CYPHER/::byteawithoutorderwithequalcol'
         * we have to ignore those chars at the end
         */
        if (final != NULL) {
            unsigned char text_to_deprocess[176]; /* see above changing the query in case of CE */
            rc = memset_s(text_to_deprocess, sizeof(text_to_deprocess), 0, sizeof(text_to_deprocess));
            securec_check_c(rc, "\0", "\0");
            if (final - (char *)processed_data_tmp > 2) { /* 2 is the shortest length , such as "::" */
                check_strncpy_s(strncpy_s((char *)text_to_deprocess, sizeof(text_to_deprocess), 
                    (char *)processed_data_tmp + 1, final - (char *)processed_data_tmp - 2));
                text_to_deprocess[final - (char *)processed_data_tmp - 2] = 0;
            }

            unescaped_processed_data = PQunescapeBytea(text_to_deprocess, &unescaped_processed_data_size);
        } else {
            unescaped_processed_data = PQunescapeBytea(processed_data_tmp, &unescaped_processed_data_size);
        }
        if (processed_data_tmp != NULL) {
            libpq_free(processed_data_tmp);
        }
        if (unescaped_processed_data == NULL) {
            fprintf(stderr, "ERROR(CLIENT): failed to unescape processed data\n");
            return DEC_DATA_ERR;
        }
    }

    if (!unescaped_processed_data_size) {
        plain_text_size = 0;
        return DEC_DATA_SUCCEED;
    } else if (unescaped_processed_data_size < sizeof(Oid)) {
        /*
         * if the size is smaller the size of Oid, so setting oid is not there
         * and this is an error
         */
        fprintf(stderr, "ERROR(CLIENT): wrong value for processed column\n");
        libpq_free(unescaped_processed_data);
        return DEC_DATA_ERR;
    }
    dec_dat_res = HooksManager::deprocess_data(*conn->client_logic, unescaped_processed_data,
        unescaped_processed_data_size, plain_text, &plain_text_size_tmp);
    if (dec_dat_res != DEC_DATA_SUCCEED) {
        return dec_dat_res;
    }

    bool plain_invalid = plain_text_size_tmp == 0 && !(*plain_text);
    if (plain_invalid) {
        /* the only accetped way to reach here is no proper cmk permissions */
        *plain_text = (unsigned char *)malloc(processed_data_size * sizeof(unsigned char));
        if (*plain_text == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when decrypting data\n");
            libpq_free(unescaped_processed_data);
            return CLIENT_HEAP_ERR;
        }
        errno_t rc = memcpy_s(*plain_text, processed_data_size, processed_data, processed_data_size);
        securec_check_c(rc, "\0", "\0");
        plain_text_size = processed_data_size;
        libpq_free(unescaped_processed_data);
        return DEC_DATA_SUCCEED;
    }

    libpq_free(unescaped_processed_data);
    if (plain_text_size_tmp < 0 || !(*plain_text)) {
        return DEC_DATA_SUCCEED;
    }

    plain_text_size = plain_text_size_tmp;
    if (!format) { /* text */
        process_text_format(plain_text, plain_text_size, is_default, original_typeid);
    } else {
        size_t result_size = 0;
        errno_t rc = EOK;
        char err_msg[MAX_ERRMSG_LENGTH];
        rc = memset_s(err_msg, MAX_ERRMSG_LENGTH, 0, MAX_ERRMSG_LENGTH);
        securec_check_c(rc, "\0", "\0");
        unsigned char *result =
            Format::restore_binary(*plain_text, plain_text_size, original_typeid, 0, -1, &result_size, err_msg);
        libpq_free(*plain_text);
        *plain_text = (unsigned char *)malloc(result_size + 1);
        if (*plain_text == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when decrypting data\n");
            libpq_free(unescaped_processed_data);
            return CLIENT_HEAP_ERR;
        }
        rc = memcpy_s(*plain_text, result_size + 1, result, result_size);
        securec_check_c(rc, "\0", "\0");
        (*plain_text)[result_size] = 0;
        plain_text_size = result_size;
        libpq_free(result);
    }

    return DEC_DATA_SUCCEED;
}

const bool ValuesProcessor::textual_rep(const Oid oid)
{
    return (oid != BOOLOID && oid != INT8OID && oid != INT2OID && oid != INT1OID && oid != INT4OID && oid != OIDOID &&
        oid != NUMERICOID);
}

void ValuesProcessor::process_text_format(unsigned char **plain_text, size_t &plain_text_size, bool is_default,
    int original_typeid)
{
    size_t result_size = 0;
    char *res = Format::binary_to_text(*plain_text, plain_text_size, original_typeid, 0, -1, &result_size);
    Assert(res != NULL);
    if (is_default) {
        size_t tmp_plain_text_allocated = strlen((char *)*plain_text) + 256;
        char *tmp_plain_text = (char *)malloc(tmp_plain_text_allocated);
        if (tmp_plain_text == NULL) {
            libpq_free(res);
            return;
        }
        if (textual_rep(original_typeid)) {
            check_strncpy_s(strncpy_s(tmp_plain_text, tmp_plain_text_allocated, "\'", 1));
            check_strncat_s(strncat_s(tmp_plain_text, tmp_plain_text_allocated,
                (char *)*plain_text, strlen((char *)*plain_text)));
            check_strncat_s(strncat_s(tmp_plain_text, tmp_plain_text_allocated, "\'", 1));
        } else {
            check_strncpy_s(strncpy_s(tmp_plain_text, tmp_plain_text_allocated, res, result_size));
        }
        check_strncat_s(strncat_s(tmp_plain_text, tmp_plain_text_allocated, "::", 2));
        const char *type = TypesMap::typesTextToOidMap.find_by_oid(original_typeid);
        if (type == NULL) {
            libpq_free(tmp_plain_text);
            libpq_free(res);
            return;
        }
        check_strncat_s(
            strncat_s(tmp_plain_text, tmp_plain_text_allocated, type, strlen(type)));
        plain_text_size = strlen(tmp_plain_text);
        if (*plain_text) {
            free(*plain_text);
            *plain_text = NULL;
        }
        *plain_text = (unsigned char *)malloc(strlen(tmp_plain_text) + 1);
        if (*plain_text == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when processing text format\n");
            libpq_free(tmp_plain_text);
            libpq_free(res);
            return;
        }
        check_memcpy_s(memcpy_s((char *)*plain_text, strlen(tmp_plain_text) + 1, (char *)tmp_plain_text,
            strlen((char *)tmp_plain_text)));
        (*plain_text)[plain_text_size] = '\0';
        libpq_free(tmp_plain_text);
    } else {
        libpq_free(*plain_text);
        *plain_text = (unsigned char *)malloc(result_size + 1);
        if (*plain_text == NULL) {
            fprintf(stderr, "ERROR(CLIENT): out of memory when processing text format\n");
            libpq_free(res);
            return;
        }
        errno_t rc = EOK;
        rc = memcpy_s(*plain_text, result_size + 1, res, result_size);
        securec_check_c(rc, "\0", "\0");
        (*plain_text)[result_size] = 0;
        plain_text_size = result_size;
    }
    libpq_free(res);
}
