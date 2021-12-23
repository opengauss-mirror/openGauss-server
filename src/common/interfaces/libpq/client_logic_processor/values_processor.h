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
 * values_processor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\values_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef VALUES_PROCESSOR_H
#define VALUES_PROCESSOR_H

#include <string>
#include "client_logic_cache/icached_columns.h"

typedef struct pg_conn PGconn;

typedef struct PGClientLogicParams PGClientLogicParams;
class RawValue;
typedef unsigned int Oid;
typedef struct StatementData StatementData;
class RawValuesList;

enum DecryptDataRes {
    DEC_DATA_SUCCEED = 0,
    DEC_DATA_ERR,
    INVALID_DATA_LEN,
    DECRYPT_CEK_ERR,
    DERIVE_CEK_ERR,
    CLIENT_HEAP_ERR,
    CLIENT_CACHE_ERR,
};

/* the text's status when process text format */
enum ProcessStatus {
    ONLY_VALUE = 0,     /* only the text value, like text */
    ADD_QUOTATION_MARK, /* add '' to text value, like 'text' */
    ADD_TYPE,           /* add '' and type to text value, like 'text'::varchar */
};

class ValuesProcessor {
public:
    static bool process_values(StatementData *statement_data, const ICachedColumns *cached_columns,
        const size_t rows_count, RawValuesList *raw_values_list);
    static DecryptDataRes deprocess_value(PGconn *conn, const unsigned char *processed_data, size_t processed_data_size,
        int original_typeid, int format, unsigned char **plain_text, size_t &plain_text_size,
        ProcessStatus process_status = ONLY_VALUE);
private:
    static void process_text_format(unsigned char **plain_text, size_t &plain_text_size,
        ProcessStatus process_status, int original_typeid);
    static DecryptDataRes process_null_value(unsigned char **plain_text, size_t &plain_text_size,
        int original_typeid, ProcessStatus process_status);
    static const bool textual_rep(const Oid oid);
    static bool process_rewrite_query_for_params(StatementData* const statement_data, bool &is_any_relevant, bool raw_value_param);
    static bool process_check_param_valid(StatementData* const statement_data, const ICachedColumns *cached_columns, const size_t rows_count, const RawValuesList *raw_values);
    static void process_set_adjustparam_types(StatementData* const statement_data,
                                                    const ICachedColumn *cached_column,
                                                    size_t m,
                                                    size_t i);
    static void process_update_values_location(RawValuesList* const raw_values, int size_diff, size_t j);
};

#endif
