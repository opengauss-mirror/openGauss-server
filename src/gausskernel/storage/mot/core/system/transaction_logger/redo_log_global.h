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
 * redo_log_global.h
 *    Redo log operation codes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_global.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_GLOBAL_H
#define REDO_LOG_GLOBAL_H

#include <cstdint>

namespace MOT {
/**
 * @enum Redo log operation codes.
 */
enum OperationCode : uint16_t {
    /** @var Insert empty row with default field initialization and a given primary key. */
    CREATE_ROW = 1,

    /** @var Update attribute of fixed size. */
    UPDATE_ROW = 2,

    /** @var Update attribute of variable size. */
    UPDATE_ROW_VARIABLE = 3,

    /** @var Full overwrite an existing row. */
    OVERWRITE_ROW = 4,

    /** @var Remove a row by primary key. */
    REMOVE_ROW = 5,

    /** @var Prepare a transaction. */
    PREPARE_TX = 6,

    /** @var Commit a transaction. */
    COMMIT_TX = 7,

    /** @var Commit Prepared transaction. */
    COMMIT_PREPARED_TX = 8,

    /** @var Rollback a transaction. */
    ROLLBACK_TX = 9,

    /** @var Rollback Prepared transaction. */
    ROLLBACK_PREPARED_TX = 10,

    /** @var partial redo. */
    PARTIAL_REDO_TX = 11,

    /** @var table creation. */
    CREATE_TABLE = 12,

    /** @var table drop. */
    DROP_TABLE = 13,

    /** @var index creation. */
    CREATE_INDEX = 14,

    /** @var index drop. */
    DROP_INDEX = 15,

    /** @var truncate table. */
    TRUNCATE_TABLE = 16,

    /** @var This must be the last entry. */
    INVALID_OPERATION_CODE
};

inline const char* OperationCodeToString(OperationCode op)
{
    switch (op) {
        case OperationCode::CREATE_ROW:
            return "CREATE_ROW";
        case OperationCode::UPDATE_ROW:
            return "UPDATE_ROW";
        case OperationCode::UPDATE_ROW_VARIABLE:
            return "UPDATE_ROW_VARIABLE";
        case OperationCode::OVERWRITE_ROW:
            return "OVERWRITE_ROW";
        case OperationCode::REMOVE_ROW:
            return "REMOVE_ROW";
        case OperationCode::PREPARE_TX:
            return "PREPARE_TX";
        case OperationCode::COMMIT_TX:
            return "COMMIT_TX";
        case OperationCode::COMMIT_PREPARED_TX:
            return "COMMIT_PREPARED_TX";
        case OperationCode::ROLLBACK_TX:
            return "ROLLBACK_TX";
        case OperationCode::ROLLBACK_PREPARED_TX:
            return "ROLLBACK_PREPARED_TX";
        case OperationCode::PARTIAL_REDO_TX:
            return "PARTIAL_REDO_TX";
        case OperationCode::CREATE_TABLE:
            return "CREATE_TABLE";
        case OperationCode::CREATE_INDEX:
            return "CREATE_INDEX";
        case OperationCode::DROP_TABLE:
            return "DROP_TABLE";
        case OperationCode::DROP_INDEX:
            return "DROP_INDEX";
        case OperationCode::INVALID_OPERATION_CODE:
        default:
            return "UNKNOWN_OP_CODE";
    }
}

inline bool IsAbortOp(OperationCode op)
{
    return (op == OperationCode::ROLLBACK_TX || op == OperationCode::ROLLBACK_PREPARED_TX);
}

inline bool IsCommitOp(OperationCode op)
{
    return (op == OperationCode::COMMIT_TX || op == OperationCode::COMMIT_PREPARED_TX);
}
}  // namespace MOT

#endif /* REDO_LOG_GLOBAL_H */
