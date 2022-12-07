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
 * mot_internal.h
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_internal.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_INTERNAL_H
#define MOT_INTERNAL_H

#include <map>
#include <string>
#include "catalog_column_types.h"
#include "foreign/fdwapi.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "pgstat.h"
#include "global.h"
#include "mot_fdw_error.h"
#include "mot_fdw_xlog.h"
#include "mot_fdw_snapshot_manager.h"
#include "mot_engine.h"
#include "bitmapset.h"
#include "storage/mot/jit_exec.h"
#include "mot_match_index.h"

using std::map;
using std::string;

#define MIN_DYNAMIC_PROCESS_MEMORY (2 * 1024 * 1024)

#define MOT_INSERT_FAILED_MSG "Insert failed"
#define MOT_UPDATE_FAILED_MSG "Update failed"
#define MOT_DELETE_FAILED_MSG "Delete failed"

#define MOT_UNIQUE_VIOLATION_MSG "duplicate key value violates unique constraint \"%s\""
#define MOT_UNIQUE_VIOLATION_DETAIL "Key %s already exists."
#define MOT_TABLE_NOTFOUND "Table \"%s\" doesn't exist"
#define MOT_UPDATE_INDEXED_FIELD_NOT_SUPPORTED "Update indexed field \"%s\" in table \"%s\" is not supported"

#define NULL_DETAIL ((char*)nullptr)
#define abortParentTransaction(msg, detail) \
    ereport(ERROR,                          \
        (errmodule(MOD_MOT), errcode(ERRCODE_FDW_ERROR), errmsg(msg), (detail != nullptr ? errdetail(detail) : 0)));

#define abortParentTransactionParams(error, msg, msg_p, detail, detail_p) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, msg_p), errdetail(detail, detail_p)));

#define abortParentTransactionParamsNoDetail(error, msg, ...) \
    ereport(ERROR, (errmodule(MOD_MOT), errcode(error), errmsg(msg, __VA_ARGS__)));

#define raiseAbortTxnError()                                                             \
    {                                                                                    \
        ereport(ERROR,                                                                   \
            (errmodule(MOD_MOT),                                                         \
                errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),                              \
                errmsg("current transaction is aborted, "                                \
                       "commands ignored until end of transaction block, firstChar[%c]", \
                    u_sess->proc_cxt.firstChar),                                         \
                errdetail("Please perform rollback")));                                  \
    }

#define isMemoryLimitReached()                                                                                         \
    {                                                                                                                  \
        if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {                                                        \
            MOT_LOG_ERROR("Maximum logical memory capacity %lu bytes of allowed %lu bytes reached",                    \
                (uint64_t)MOTAdaptor::m_engine->GetCurrentMemoryConsumptionBytes(),                                    \
                (uint64_t)MOTAdaptor::m_engine->GetHardMemoryLimitBytes());                                            \
            ereport(ERROR,                                                                                             \
                (errmodule(MOD_MOT),                                                                                   \
                    errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),                                                            \
                    errmsg("You have reached a maximum logical capacity"),                                             \
                    errdetail("Only destructive operations are allowed, please perform database cleanup to free some " \
                              "memory.")));                                                                            \
        }                                                                                                              \
    }

#define IS_CHAR_TYPE(oid) \
    ((oid) == VARCHAROID || (oid) == BPCHAROID || (oid) == TEXTOID || (oid) == CLOBOID || (oid) == BYTEAOID)
#define IS_INT_TYPE(oid)                                                                                 \
    ((oid) == BOOLOID || (oid) == CHAROID || (oid) == INT8OID || (oid) == INT2OID || (oid) == INT4OID || \
        (oid) == FLOAT4OID || (oid) == FLOAT8OID || (oid) == INT1OID)
#define IS_TIME_TYPE(oid) ((oid) == DATEOID || (oid) == TIMEOID || (oid) == TIMESTAMPOID || (oid) == TIMESTAMPTZOID)

namespace MOT {
class Table;
class Index;
class IndexIterator;
class Column;
class MOTEngine;
}  // namespace MOT

enum class FDW_LIST_TYPE : uint8_t { FDW_LIST_STATE = 1, FDW_LIST_BITMAP = 2 };

typedef struct Order_St {
    SortDir m_order;
    int m_lastMatch;
    int m_cols[MAX_KEY_COLUMNS];

    void init()
    {
        m_order = SortDir::SORTDIR_NONE;
        m_lastMatch = -1;
        for (uint32_t i = 0; i < MAX_KEY_COLUMNS; i++)
            m_cols[i] = 0;
    }
} OrderSt;

#define MOT_REC_TID_NAME "ctid"

/**
 * @struct MOTRecConvert
 * @brief Union to impersonate PG CTID field
 */
typedef struct MOTRecConvert {
    union {
        uint64_t m_ptr;
        ItemPointerData m_self; /* SelfItemPointer */
    } m_u;
} MOTRecConvertSt;

#define SORT_STRATEGY(x) (((x) == BTGreaterStrategyNumber) ? SortDir::SORTDIR_DESC : SortDir::SORTDIR_ASC)

/**
 * @struct MOTFdwState_St
 * @brief Structure used to pass information between planning and execution phase
 * of the query. Also, used for translation between PG and MOT data representation
 */
struct MOTFdwState_St {
    /** @var indicates PG transaction id */
    ::TransactionId m_txnId;

    /** @var indicates if the instance was initiated during SCAN operation */
    bool m_allocInScan;

    /** @var Query operation SELECT/INSERT... */
    CmdType m_cmdOper;

    /** @var Sorting direction ASC/DESC */
    SortDir m_order;

    /** @var Select query uses FOR UPDATE syntax */
    bool m_hasForUpdate;

    /** @var Indicates if during update indexed column is updated */
    MOT::UpdateIndexColumnType m_hasIndexedColUpdate;

    /** @var PG global id for table */
    Oid m_foreignTableId;

    /** @var number of columns in the table */
    AttrNumber m_numAttrs;

    /** @var CTID column index in the resulting column array */
    AttrNumber m_ctidNum;

    /** @var number of conditions used FDW engine */
    uint16_t m_numExpr;

    /** @var bitmap which contains column indexes used in the query */
    uint8_t* m_attrsUsed;

    /**
     * @var m_attrsModified which contains column indexes used in the update.
     * This will be merged into m_attrsUsed in BeginModify
     */
    uint8_t* m_attrsModified;

    /** @var expression values*/
    List* m_remoteConds;

    /** @var original expressions of a form "a < b" */
    List* m_remoteCondsOrig;

    /** @var expressions to be passed to a PG filter */
    List* m_localConds;

    /** @var Initialized expressions for creating key buffers */
    List* m_execExprs;

    /** @var context for extracting expression value */
    ExprContext* m_econtext;

    /** @var initial full scan cost of the table */
    double m_startupCost;

    /** @var scan cost after using query conditions */
    double m_totalCost;

    /** @var best index to perform query based on const conditions only */
    MatchIndex* m_bestIx;

    /** @var best index to perform query based on parametrized conditions */
    MatchIndex* m_paramBestIx;

    /** @var placeholder for above members */
    MatchIndex m_bestIxBuf;

    // ENGINE
    /** @var pointer to a MOT table structure during execution */
    MOT::Table* m_table;

    /** @var start and end cursors for scan operations */
    MOT::IndexIterator* m_cursor[2] = {nullptr, nullptr};

    /** @var current transaction manager */
    MOT::TxnManager* m_currTxn;

    /** @var number of rows fetched by scan */
    uint32_t m_rowsFound = 0;

    /** @var indicates if cursors were initialized */
    bool m_cursorOpened = false;

    /** @var key buffers for cursors initialization */
    MOT::MaxKey m_stateKey[2];

    /** @var indicates direction of scan */
    bool m_forwardDirectionScan;

    /** @var MOT internal command */
    MOT::AccessType m_internalCmdOper;
};

/**
 * @class MOTAdaptor
 * @brief Interface for PG and MOT communication
 */
class MOTAdaptor {
public:
    /**
     * @brief Initialize MOT engine singleton.
     */
    static void Init();

    /**
     * @brief Destroy MOT engine singleton.
     */
    static void Destroy();

    /**
     * @brief Indicates that MOT configuration file has been changed.
     */
    static void NotifyConfigChange();

    /**
     * @brief Set internal MOT query command.
     * @param festate MOT Query execution state.
     */
    static inline void GetCmdOper(MOTFdwStateSt* festate)
    {
        switch (festate->m_cmdOper) {
            case CMD_SELECT:
                if (festate->m_hasForUpdate) {
                    festate->m_internalCmdOper = MOT::AccessType::RD_FOR_UPDATE;
                } else {
                    festate->m_internalCmdOper = MOT::AccessType::RD;
                }
                break;
            case CMD_DELETE:
                festate->m_internalCmdOper = MOT::AccessType::DEL;
                break;
            case CMD_UPDATE:
                festate->m_internalCmdOper = MOT::AccessType::WR;
                break;
            case CMD_INSERT:
                festate->m_internalCmdOper = MOT::AccessType::INS;
                break;
            case CMD_UNKNOWN:
            case CMD_MERGE:
            case CMD_UTILITY:
            case CMD_NOTHING:
            default:
                festate->m_internalCmdOper = MOT::AccessType::INV;
                break;
        }
    }

    /**
     * @brief Initialize MOT transaction manager and session.
     * @param callerSrc Name of the function initiated the call
     * @param connection_id MOT connection id
     */
    static MOT::TxnManager* InitTxnManager(
        const char* callerSrc, MOT::ConnectionId connection_id = INVALID_CONNECTION_ID);

    /**
     * @brief Destroy MOT transaction manager and session.
     * @param status PG connection status
     * @param ptr Pointer to MOT internal data (session)
     */
    static void DestroyTxn(int status, Datum ptr);

    /**
     * @brief Create MOT table from PG create table statement.
     * @param table PG table statement
     * @param tid transaction id
     */
    static MOT::RC CreateTable(CreateForeignTableStmt* table, TransactionId tid);

    /**
     * @brief Create MOT index from PG create index statement.
     * @param index PG index statement
     * @param tid transaction id
     */
    static MOT::RC CreateIndex(IndexStmt* index, TransactionId tid);

    /**
     * @brief Drop MOT index from PG drop index statement.
     * @param stmt PG drop index statement
     * @param tid transaction id
     */
    static MOT::RC DropIndex(DropForeignStmt* stmt, TransactionId tid);

    /**
     * @brief Drop MOT table from PG drop table statement.
     * @param stmt PG drop table statement
     * @param tid transaction id
     */
    static MOT::RC DropTable(DropForeignStmt* stmt, TransactionId tid);

    /**
     * @brief Add MOT column from PG add column statement.
     * @param cmd PG add column statement
     * @param tid transaction id
     */
    static MOT::RC AlterTableAddColumn(AlterForeingTableCmd* cmd, TransactionId tid);

    /**
     * @brief Drop MOT column from PG drop column statement.
     * @param cmd PG drop column statement
     * @param tid transaction id
     */
    static MOT::RC AlterTableDropColumn(AlterForeingTableCmd* cmd, TransactionId tid);

    /**
     * @brief Rename MOT column from PG rename column statement.
     * @param cmd PG rename column statement
     * @param tid transaction id
     */
    static MOT::RC AlterTableRenameColumn(RenameForeingTableCmd* cmd, TransactionId tid);

    /**
     * @brief Truncate MOT table.
     * @param rel PG table structure
     * @param tid transaction id
     */
    static MOT::RC TruncateTable(Relation rel, TransactionId tid);

    /**
     * @brief Vacuum MOT table.
     * @param rel PG table structure
     * @param tid transaction id
     */
    static void VacuumTable(Relation rel, TransactionId tid);

    /**
     * @brief Returns index memory size.
     * @param tabId PG OID for table
     * @param ixId PG OID for index
     */
    static uint64_t GetTableIndexSize(uint64_t tabId, uint64_t ixId);

    /**
     * @brief Returns MOT memory size.
     * @param nodeCount OUT holds number of numa sockets
     * @param isGlobal indicator to count global or local (session) memory
     */
    static MotMemoryDetail* GetMemSize(uint32_t* nodeCount, bool isGlobal);

    /**
     * @brief Returns session memory stats.
     * @param sessionCount OUT holds number of sessions
     */
    static MotSessionMemoryDetail* GetSessionMemSize(uint32_t* sessionCount);

    /**
     * @brief Validates transaction before actual commit.
     */
    static MOT::RC ValidateCommit();

    /**
     * @brief Performs actual commit.
     * @param csn transaction id
     */
    static void RecordCommit(uint64_t csn);

    /**
     * @brief Performs validate and actual commit.
     * @param csn transaction id
     */
    static MOT::RC Commit(uint64_t csn);

    /**
     * @brief Returns true in case transaction is read only.
     */
    static bool IsTxnWriteSetEmpty();

    /**
     * @brief Cleanup transaction manager after transaction ends.
     */
    static void EndTransaction();

    /**
     * @brief Performs rollback of transaction.
     */
    static void Rollback();

    /**
     * @brief Performs prepare of transaction in case of two-phase commit.
     */
    static MOT::RC Prepare();

    /**
     * @brief Performs actual commit of prepared transaction.
     * @param csn transaction id
     */
    static void CommitPrepared(uint64_t csn);

    /**
     * @brief Performs rollback of prepared transaction.
     */
    static void RollbackPrepared();

    /**
     * @brief Performs insert into MOT engine.
     * @param fdwState query state
     * @param slot PG row
     */
    static MOT::RC InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

    /**
     * @brief Performs update of a fetched row.
     * @param fdwState query state
     * @param slot PG row containing changes
     * @param currRow MOT row to be updated
     */
    static MOT::RC UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow);

    /**
     * @brief Performs delete of a row.
     * @param fdwState query state
     * @param slot PG row to be deleted
     */
    static MOT::RC DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

    /**
     * @brief Performs conversion of DECIMAL PG type to MOT type.
     * @param n PG value
     * @param[out] d MOT value
     */
    inline static void PGNumericToMOT(const Numeric n, MOT::DecimalSt& d)
    {
        int sign = NUMERIC_SIGN(n);

        d.m_hdr.m_flags = 0;
        d.m_hdr.m_flags |= (sign == NUMERIC_POS
                                ? DECIMAL_POSITIVE
                                : (sign == NUMERIC_NEG ? DECIMAL_NEGATIVE : ((sign == NUMERIC_NAN) ? DECIMAL_NAN : 0)));
        d.m_hdr.m_ndigits = NUMERIC_NDIGITS(n);
        d.m_hdr.m_scale = NUMERIC_DSCALE(n);
        d.m_hdr.m_weight = NUMERIC_WEIGHT(n);
        d.m_round = 0;
        if (d.m_hdr.m_ndigits > 0) {
            errno_t erc = memcpy_s(d.m_digits,
                DECIMAL_MAX_SIZE - sizeof(MOT::DecimalSt),
                (void*)NUMERIC_DIGITS(n),
                d.m_hdr.m_ndigits * sizeof(NumericDigit));
            securec_check(erc, "\0", "\0");
        }
    }

    /**
     * @brief Performs conversion of DECIMAL MOT type to PG type.
     * @param d MOT value
     */
    inline static Numeric MOTNumericToPG(MOT::DecimalSt* d)
    {
        NumericVar v;

        v.ndigits = d->m_hdr.m_ndigits;
        v.dscale = d->m_hdr.m_scale;
        v.weight = (int)(int16_t)(d->m_hdr.m_weight);
        v.sign = (d->m_hdr.m_flags & DECIMAL_POSITIVE
                      ? NUMERIC_POS
                      : (d->m_hdr.m_flags & DECIMAL_NEGATIVE ? NUMERIC_NEG
                                                             : ((d->m_hdr.m_flags & DECIMAL_NAN) ? DECIMAL_NAN : 0)));
        v.buf = (NumericDigit*)&d->m_round;
        v.digits = (NumericDigit*)d->m_digits;

        return makeNumeric(&v);
    }

    /**
     * @brief Performs conversion of value from PG type to MOT type.
     * @param col MOT column
     * @param datum PG value
     * @parma type PG type id
     * @parma data MOT data pointer
     */
    static void DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data);

    /**
     * @brief Performs conversion of value from PG type to MOT key type.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param colType PG column type
     * @param data MOT key placeholder
     * @param len MOT placeholder length
     * @param oper MOT key operation type
     * @param fill MOT fill value
     */
    static void DatumToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill = 0x00);

    /**
     * @brief Performs conversion of value from MOT to PG.
     * @param table MOT table
     * @param attr PG column metadata
     * @param data MOT pointer to a row data
     * @param value PG placeholder for a value
     * @param is_null indicates if requested column value is null
     */
    static void MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null);

    /**
     * @brief Performs conversion of PG row to MOT row.
     * @param slot PG row
     * @param table MOT table
     * @param attrs_used bitmap indicating which columns should be converted
     * @parma destRow MOT data holder for a row
     */
    static void PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow);

    /**
     * @brief Performs conversion of PG row to MOT row.
     * @param slot PG row
     * @param table MOT table
     * @param attrs_used bitmap indicating which columns should be converted
     * @parma destRow MOT data holder for a row
     */
    static void PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow);

    /**
     * @brief Performs conversion of MOT row to PG row.
     * @param slot PG row
     * @param table MOT table
     * @param attrs_used bitmap indicating which columns should be converted
     * @parma srcRow MOT data holder for a row
     */
    static void UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow);

    /**
     * @brief Performs open of scan cursors for a query.
     * @param rel PG table
     * @param festate MOT query state
     */
    static void OpenCursor(Relation rel, MOTFdwStateSt* festate);

    /**
     * @brief Verifies if scan has reached the end.
     * @param festate MOT query state
     */
    static bool IsScanEnd(MOTFdwStateSt* festate);

    /**
     * @brief Creates key buffer for scan operation.
     * @param rel PG table
     * @param festate MOT query state
     * @param start indicates for which cursor key buffer is created (start,end)
     */
    static void CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start);

    /**
     * @brief Returns true in case column participates in index.
     * @param colId column index in metadata
     * @param table MOT table
     */
    static bool IsColumnIndexed(int16_t colId, MOT::Table* table);

    /**
     * @brief Associates query expression with index column.
     * @param state query state
     * @param marr array of matched indexes for query execution
     * @param colId column index in the table
     * @param expr PG expression containing value
     * @param parent parent expression for a value
     * @param set_local indicates if the expression should be processed by PG
     */
    static bool SetMatchingExpr(MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr,
        Expr* parent, bool set_local);

    /**
     * @brief Peeks the best index for scan operation among matched indexes.
     * @param festate query state
     * @param marr array of matched indexes for query execution
     * @param numClauses number of conditions used in query
     * @param setLocal indicates if the expression should be processed by PG
     */
    static MatchIndex* GetBestMatchIndex(
        MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal = true);

    /**
     * @brief Add parameter expression to the list.
     * @param params list of expressions
     * @param expr expression to be added
     */
    inline static int32_t AddParam(List** params, Expr* expr)
    {
        int32_t index = 0;
        ListCell* cell = nullptr;

        foreach (cell, *params) {
            ++index;
            if (equal(expr, (Node*)lfirst(cell))) {
                break;
            }
        }
        if (cell == nullptr) {
            /* add the parameter to the list */
            ++index;
            *params = lappend(*params, expr);
        }

        return index;
    }

    /** @var MOT engine singleton */
    static MOT::MOTEngine* m_engine;

    /** @var indicates if MOT engine initialized */
    static bool m_initialized;

    /** @var indicates if MOT callbacks has been installed */
    static bool m_callbacks_initialized;

private:
    /**
     * @brief Adds all the columns.
     * @param table Table object being created.
     * @param tableElts Column definitions list.
     * @param[out] hasBlob Whether any column is a blob.
     * NOTE: On failure, table object will be deleted and ereport will be done.
     */
    static void AddTableColumns(MOT::Table* table, List* tableElts, bool& hasBlob);

    /**
     * @brief Performs conversion of varchar value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param colType PG column type
     * @param data MOT key placeholder
     * @param len MOT placeholder length
     * @param oper MOT key operation type
     * @param fill MOT fill value
     */
    static void VarcharToMOTKey(MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill);

    /**
     * @brief Performs conversion of float value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param data MOT key placeholder
     */
    static void FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);

    /**
     * @brief Performs conversion of decimal value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param data MOT key placeholder
     */
    static void NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);

    /**
     * @brief Performs conversion of timestamp value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param data MOT key placeholder
     */
    static void TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);

    /**
     * @brief Performs conversion of timestamp with time zone value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param data MOT key placeholder
     */
    static void TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);

    /**
     * @brief Performs conversion of date value from PG to MOT key.
     * @param col MOT column
     * @param datumType PG data type
     * @param datum value
     * @param data MOT key placeholder
     */
    static void DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data);
};

/**
 * @brief Creates or retrieves MOT transaction manager.
 * @param collerSrc function name which has called it
 * @param txn_id transaction id
 */
inline MOT::TxnManager* GetSafeTxn(const char* callerSrc, ::TransactionId txn_id = 0)
{
    if (!u_sess->mot_cxt.txn_manager) {
        // result of InitTxnManager() is put in u_sess->mot_cxt.txn_manager, so no need to check return value
        (void)MOTAdaptor::InitTxnManager(callerSrc);
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            if (txn_id != 0) {
                u_sess->mot_cxt.txn_manager->SetTransactionId(txn_id);
            }
        } else {
            report_pg_error(MOT_GET_ROOT_ERROR_RC());
        }
    }
    return u_sess->mot_cxt.txn_manager;
}

/**
 * @brief Initializes MOT thread local members.
 */
extern bool EnsureSafeThreadAccess(bool throwError = true);

/**
 * @brief Serialize bitmap into a PG node list.
 * @param result list of nodes
 * @param bitmap data containing bitmap
 * @param len length of bitmap data
 * @param hasIndexUpd indicates if query contains update on indexed column
 */
inline List* BitmapSerialize(List* result, uint8_t* bitmap, int16_t len, MOT::UpdateIndexColumnType hasIndexUpd)
{
    // set list type to FDW_LIST_TYPE::FDW_LIST_BITMAP
    result = lappend(
        result, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(FDW_LIST_TYPE::FDW_LIST_BITMAP), false, true));
    result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(hasIndexUpd), false, true));
    for (int i = 0; i < len; i++) {
        result = lappend(result, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(bitmap[i]), false, true));
    }

    return result;
}

/**
 * @brief De-Serialize list of nodes into a bitmap.
 * @param bitmap data containing bitmap
 * @param len length of bitmap data
 * @param[out] hasIndexUpd indicates if query contains update on indexed column
 * @param[in/out] cell start point of nodes that contain bitmap information
 */
inline void BitmapDeSerialize(uint8_t* bitmap, int16_t len, MOT::UpdateIndexColumnType& hasIndexUpd, ListCell** cell)
{
    if (cell != nullptr && *cell != nullptr) {
        int type = ((Const*)lfirst(*cell))->constvalue;
        if (type == static_cast<int>(FDW_LIST_TYPE::FDW_LIST_BITMAP)) {
            *cell = lnext(*cell);
            hasIndexUpd = (MOT::UpdateIndexColumnType)((Const*)lfirst(*cell))->constvalue;
            *cell = lnext(*cell);
            for (int i = 0; i < len; i++) {
                bitmap[i] = (uint8_t)((Const*)lfirst(*cell))->constvalue;
                *cell = lnext(*cell);
            }
        }
    }
}

/**
 * @brief Destroys scan cursors.
 * @param state MOT query state
 */
inline void CleanCursors(MOTFdwStateSt* state)
{
    for (int i = 0; i < 2; i++) {
        if (state->m_cursor[i]) {
            if (state->m_currTxn != nullptr) {
                std::unordered_map<uint64_t, uint64_t>::iterator it =
                    state->m_currTxn->m_queryState.find((uint64_t)state->m_cursor[i]);
                if (it != state->m_currTxn->m_queryState.end()) {
                    (void)state->m_currTxn->m_queryState.erase((uint64_t)state->m_cursor[i]);
                    state->m_cursor[i]->Invalidate();
                    state->m_cursor[i]->Destroy();
                    delete state->m_cursor[i];
                }
            }
            state->m_cursor[i] = NULL;
        }
    }
}

/**
 * @brief Destroys scan cursors.
 * @param txn MOT transaction manager
 */
inline void CleanQueryStatesOnError(MOT::TxnManager* txn)
{
    if (txn != nullptr) {
        for (auto& itr : txn->m_queryState) {
            MOT::IndexIterator* cursor = (MOT::IndexIterator*)itr.second;
            if (cursor != nullptr) {
                cursor->Invalidate();
                cursor->Destroy();
                delete cursor;
            }
        }
        txn->m_queryState.clear();
    }
}

/**
 * @brief Returns true in case the transaction has been aborted.
 * @param txn MOT transaction manager
 */
inline bool IsTxnInAbortState(MOT::TxnManager* txn)
{
    return txn->IsTxnAborted();
}

#endif  // MOT_INTERNAL_H
