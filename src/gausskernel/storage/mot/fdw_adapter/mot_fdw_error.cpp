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
 * mot_fdw_error.cpp
 *    MOT Foreign Data Wrapper error reporting interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_error.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_fdw_error.h"
#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "utils/elog.h"

#include "column.h"
#include "table.h"

// Error code mapping array from MOT to PG
static const MotErrToPGErrSt MM_ERRCODE_TO_PG[] = {
    // RC_OK
    {ERRCODE_SUCCESSFUL_COMPLETION, "Success", nullptr},
    // RC_ERROR
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_ABORT
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_SERIALIZATION_FAILURE
    {ERRCODE_T_R_SERIALIZATION_FAILURE, "Could not serialize access due to concurrent update", nullptr},
    // RC_UNSUPPORTED_COL_TYPE
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column type %s is not supported yet"},
    // RC_UNSUPPORTED_COL_TYPE_ARR
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column type Array of %s is not supported yet"},
    // RC_EXCEEDS_MAX_ROW_SIZE
    {ERRCODE_FEATURE_NOT_SUPPORTED,
        "Column definition of %s is not supported",
        "Column size %d exceeds max tuple size %u"},
    // RC_COL_NAME_EXCEEDS_MAX_SIZE
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column name %s exceeds max name size %u"},
    // RC_COL_SIZE_INVALID
    {ERRCODE_INVALID_COLUMN_DEFINITION,
        "Column definition of %s is not supported",
        "Column size %d exceeds max size %u"},
    // RC_TABLE_EXCEEDS_MAX_DECLARED_COLS
    {ERRCODE_FEATURE_NOT_SUPPORTED, "Can't create table", "Can't add column %s, number of declared columns is less"},
    // RC_INDEX_EXCEEDS_MAX_SIZE
    {ERRCODE_FDW_KEY_SIZE_EXCEEDS_MAX_ALLOWED,
        "Can't create index",
        "Total columns size is greater than maximum index size %u"},
    // RC_TABLE_EXCEEDS_MAX_INDEXES,
    {ERRCODE_FDW_TOO_MANY_INDEXES,
        "Can't create index",
        "Total number of indexes for table %s is greater than the maximum number if indexes allowed %u"},
    // RC_TXN_EXCEEDS_MAX_DDLS,
    {ERRCODE_FDW_TOO_MANY_DDL_CHANGES_IN_TRANSACTION_NOT_ALLOWED,
        "Cannot execute statement",
        "Maximum number of DDLs per transactions reached the maximum %u"},
    // RC_UNIQUE_VIOLATION
    {ERRCODE_UNIQUE_VIOLATION, "duplicate key value violates unique constraint \"%s\"", "Key %s already exists."},
    // RC_TABLE_NOT_FOUND
    {ERRCODE_UNDEFINED_TABLE, "Table \"%s\" doesn't exist", nullptr},
    // RC_INDEX_NOT_FOUND
    {ERRCODE_UNDEFINED_TABLE, "Index \"%s\" doesn't exist", nullptr},
    // RC_LOCAL_ROW_FOUND
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_NOT_FOUND
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_DELETED
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INSERT_ON_EXIST
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INDEX_RETRY_INSERT
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_INDEX_DELETE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_LOCAL_ROW_NOT_VISIBLE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_MEMORY_ALLOCATION_ERROR
    {ERRCODE_OUT_OF_LOGICAL_MEMORY, "Memory is temporarily unavailable", nullptr},
    // RC_ILLEGAL_ROW_STATE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr},
    // RC_NULL_VOILATION
    {ERRCODE_FDW_ERROR,
        "Null constraint violated",
        "NULL value cannot be inserted into non-null column %s at table %s"},
    // RC_PANIC
    {ERRCODE_FDW_ERROR, "Critical error", "Critical error: %s"},
    // RC_NA
    {ERRCODE_FDW_OPERATION_NOT_SUPPORTED, "A checkpoint is in progress - cannot truncate table.", nullptr},
    // RC_MAX_VALUE
    {ERRCODE_FDW_ERROR, "Unknown error has occurred", nullptr}
};

static_assert(sizeof(MM_ERRCODE_TO_PG) / sizeof(MotErrToPGErrSt) == MOT::RC_MAX_VALUE + 1,
    "Not all MOT engine error codes (RC) is mapped to PG error codes");

void report_pg_error(MOT::RC rc, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5)
{
    const MotErrToPGErrSt* err = &MM_ERRCODE_TO_PG[rc];

    switch (rc) {
        case MOT::RC_OK:
            break;
        case MOT::RC_ERROR:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_ABORT:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_SERIALIZATION_FAILURE:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_UNSUPPORTED_COL_TYPE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, NameListToString(col->typname->names))));
            break;
        }
        case MOT::RC_UNSUPPORTED_COL_TYPE_ARR: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, strVal(llast(col->typname->names)))));
            break;
        }
        case MOT::RC_COL_NAME_EXCEEDS_MAX_SIZE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, col->colname, (uint32_t)MOT::Column::MAX_COLUMN_NAME_LEN)));
            break;
        }
        case MOT::RC_COL_SIZE_INVALID: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, (uint32_t)(uint64_t)arg2, (uint32_t)MAX_VARCHAR_LEN)));
            break;
        }
        case MOT::RC_EXCEEDS_MAX_ROW_SIZE: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, col->colname),
                    errdetail(err->m_detail, (uint32_t)(uint64_t)arg2, (uint32_t)MAX_TUPLE_SIZE)));
            break;
        }
        case MOT::RC_TABLE_EXCEEDS_MAX_DECLARED_COLS: {
            ColumnDef* col = (ColumnDef*)arg1;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, col->colname)));
            break;
        }
        case MOT::RC_INDEX_EXCEEDS_MAX_SIZE:
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, MAX_KEY_SIZE)));
            break;
        case MOT::RC_TABLE_EXCEEDS_MAX_INDEXES:
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, ((MOT::Table*)arg1)->GetTableName(), MAX_NUM_INDEXES)));
            break;
        case MOT::RC_TXN_EXCEEDS_MAX_DDLS:
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, MAX_DDL_ACCESS_SIZE)));
            break;
        case MOT::RC_UNIQUE_VIOLATION:
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg(err->m_msg, (char*)arg1),
                    errdetail(err->m_detail, (char*)arg2)));
            break;

        case MOT::RC_TABLE_NOT_FOUND:
        case MOT::RC_INDEX_NOT_FOUND:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg(err->m_msg, (char*)arg1)));
            break;

            // following errors are internal and should not get to an upper layer
        case MOT::RC_LOCAL_ROW_FOUND:
        case MOT::RC_LOCAL_ROW_NOT_FOUND:
        case MOT::RC_LOCAL_ROW_DELETED:
        case MOT::RC_INSERT_ON_EXIST:
        case MOT::RC_INDEX_RETRY_INSERT:
        case MOT::RC_INDEX_DELETE:
        case MOT::RC_LOCAL_ROW_NOT_VISIBLE:
        case MOT::RC_ILLEGAL_ROW_STATE:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_MEMORY_ALLOCATION_ERROR:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_NULL_VIOLATION: {
            ColumnDef* col = (ColumnDef*)arg1;
            MOT::Table* table = (MOT::Table*)arg2;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(err->m_pgErr),
                    errmsg("%s", err->m_msg),
                    errdetail(err->m_detail, col->colname, table->GetLongTableName().c_str())));
            break;
        }
        case MOT::RC_PANIC: {
            char* msg = (char*)arg1;
            ereport(PANIC,
                (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg), errdetail(err->m_detail, msg)));
            break;
        }
        case MOT::RC_NA:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
        case MOT::RC_MAX_VALUE:
        default:
            ereport(ERROR, (errmodule(MOD_MOT), errcode(err->m_pgErr), errmsg("%s", err->m_msg)));
            break;
    }
}
