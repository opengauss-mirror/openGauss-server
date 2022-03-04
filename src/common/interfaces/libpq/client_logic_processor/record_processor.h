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
 * record_processor.h
 *
 * IDENTIFICATION
 * src\common\interfaces\libpq\client_logic_processor\record_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECORD_PROCESSOR_H
#define RECORD_PROCESSOR_H
#include <cstddef>
#include "libpq-int.h"
typedef struct pg_conn PGconn;

class RecordProcessor {
public:
    static bool DeProcessRecord(PGconn* conn, const char* processedData, size_t processedDataSize,
        const int* original_typesid,  const size_t original_typesid_size,  int format,
        unsigned char** plainText, size_t& plainTextSize, bool* is_decrypted);

private:
    static const size_t m_NULL_TERMINATION_SIZE = 1;
    static const size_t m_BYTEA_PREFIX_SIZE = 2;

};
#endif
