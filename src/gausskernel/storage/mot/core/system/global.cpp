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
 * global.cpp
 *    MOT constants and other definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/global.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"

namespace MOT {
extern const char* RcToString(RC rc)
{
    switch (rc) {
        case RC_OK:
            return "No Error";
        case RC_ERROR:
            return "Error";
        case RC_ABORT:
            return "Transaction aborted";
        case RC_UNSUPPORTED_COL_TYPE:
            return "Unsupported column type";
        case RC_EXCEEDS_MAX_ROW_SIZE:
            return "Maximum row size exceeded";
        case RC_COL_NAME_EXCEEDS_MAX_SIZE:
            return "Maximum column length exceeded";
        case RC_COL_SIZE_INVALID:
            return "Invalid column size";
        case RC_TABLE_EXCEEDS_MAX_DECLARED_COLS:
            return "Reached maximum number of columns in table";
        case RC_INDEX_EXCEEDS_MAX_SIZE:
            return "Index key size exceeds maximum key size";
        case RC_UNIQUE_VIOLATION:
            return "Unique constraint violated";
        case RC_TABLE_NOT_FOUND:
            return "Table not found";
        case RC_INDEX_NOT_FOUND:
            return "Index not found";
        case RC_LOCAL_ROW_FOUND:
            return "Local row found";
        case RC_LOCAL_ROW_NOT_FOUND:
            return "Local row not found";
        case RC_LOCAL_ROW_DELETED:
            return "Local row deleted";
        case RC_INSERT_ON_EXIST:
            return "Insert on exist";
        case RC_INDEX_RETRY_INSERT:
            return "Insert to index failed: row concurrency";
        case RC_INDEX_DELETE:
            return "Insert to index failed: row deleted";
        case RC_GC_INFO_REMOVE:
            return "GC info can be removed";
        case RC_SERIALIZATION_FAILURE:
            return "could not serialize access due to concurrent update";
        case RC_LOCAL_ROW_NOT_VISIBLE:
            return "Local row not visible";
        case RC_MEMORY_ALLOCATION_ERROR:
            return "Out of memory";
        case RC_ILLEGAL_ROW_STATE:
            return "Illegal row state";
        case RC_NULL_VIOLATION:
            return "Null-constraint violation";
        case RC_JIT_SP_EXCEPTION:
            return "JIT stored procedure exception thrown";
        case RC_TXN_ABORTED:
            return "Current transaction is aborted";
        case RC_CONCURRENT_MODIFICATION:
            return "Concurrent modification";
        case RC_STATEMENT_CANCELED:
            return "Statement canceled due to user request";

        default:
            return "N/A";
    }
}
}  // namespace MOT
