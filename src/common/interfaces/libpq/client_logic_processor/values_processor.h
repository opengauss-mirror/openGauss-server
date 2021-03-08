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
};

class ValuesProcessor {
public:
    static bool process_values(StatementData *statement_data, const ICachedColumns *cached_columns,
        const size_t rows_count, RawValuesList *raw_values_list);
    static DecryptDataRes deprocess_value(PGconn *conn, const unsigned char *processed_data, size_t processed_data_size,
        int original_typeid, int format, unsigned char **plain_text, size_t &plain_text_size, bool is_default);
    
private:
    static void process_text_format(unsigned char **plain_text, size_t &plain_text_size, bool is_default, int original_typeid);
    static const bool textual_rep(const Oid oid);
};

#endif
