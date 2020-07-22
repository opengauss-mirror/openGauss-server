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
 *    MOT Foreign Data Wrapper internal interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/src/mot_internal.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_INT_H
#define MOT_INT_H

#include <map>
#include <string>
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "global.h"
#include "catalog_column_types.h"
#include "mot_fdw_xlog.h"
#include "foreign/fdwapi.h"
#include "system/mot_engine.h"
#include "bitmapset.h"
#include "storage/mot/jit_exec.h"

using std::map;
using std::string;

#define MIN_DYNAMIC_PROCESS_MEMORY 2 * 1024 * 1024

#define MOT_INSERT_FAILED_MSG "Insert failed"
#define MOT_UPDATE_FAILED_MSG "Update failed"
#define MOT_DELETE_FAILED_MSG "Delete failed"

#define MOT_UNIQUE_VIOLATION_MSG "duplicate key value violates unique constraint \"%s\""
#define MOT_UNIQUE_VIOLATION_DETAIL "Key %s already exists."
#define MOT_TABLE_NOTFOUND "Table \"%s\" doesn't exist"
#define MOT_UPDATE_INDEXED_FIELD_NOT_SUPPORTED "Update indexed field \"%s\" in table \"%s\" is not supported"

#define NULL_DETAIL ((char*)nullptr)
#define abortParentTransaction(msg, detail)                                                                         \
    ereport(ERROR,                                                                                                  \
        (errmodule(MOD_MM), errcode(ERRCODE_FDW_ERROR), errmsg(msg), (detail != nullptr ? errdetail(detail) : 0))); \
    u_sess->mot_cxt.txn_manager->SetTxnState(MOT::TxnState::TXN_REQUEST_FOR_ROLLBACK);

#define abortParentTransactionParams(error, msg, msg_p, detail, detail_p)                                 \
    ereport(ERROR, (errmodule(MOD_MM), errcode(error), errmsg(msg, msg_p), errdetail(detail, detail_p))); \
    u_sess->mot_cxt.txn_manager->SetTxnState(MOT::TxnState::TXN_REQUEST_FOR_ROLLBACK);

#define abortParentTransactionParamsNoDetail(error, msg, ...)                      \
    ereport(ERROR, (errmodule(MOD_MM), errcode(error), errmsg(msg, __VA_ARGS__))); \
    u_sess->mot_cxt.txn_manager->SetTxnState(MOT::TxnState::TXN_REQUEST_FOR_ROLLBACK);

#define isMemoryLimitReached()                                                                                         \
    {                                                                                                                  \
        if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {                                                        \
            MOT_LOG_ERROR("Maximum logical memory capacity %lu bytes of allowed %lu bytes reached",                    \
                (uint64_t)MOTAdaptor::m_engine->GetCurrentMemoryConsumptionBytes(),                                    \
                (uint64_t)MOTAdaptor::m_engine->GetHardMemoryLimitBytes());                                            \
            ereport(ERROR,                                                                                             \
                (errmodule(MOD_MM),                                                                                    \
                    errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),                                                            \
                    errmsg("You have reached a maximum logical capacity"),                                             \
                    errdetail("Only destructive operations are allowed, please perform database cleanup to free some " \
                              "memory.")));                                                                            \
        }                                                                                                              \
    }

// forward declaration
struct CreateStmt;

namespace MOT {
class Table;
class Index;
class IndexIterator;
class memory_manager_numa;
class Column;
class MOTEngine;
}  // namespace MOT

typedef struct MOTFdwState_St MOTFdwStateSt;

typedef struct MotErrToPGErr_St {
    int m_pgErr;
    const char* m_msg;
    const char* m_detail;
} MotErrToPGErrSt;

void report_pg_error(MOT::RC rc, MOT::TxnManager* txn, void* arg1 = nullptr, void* arg2 = nullptr, void* arg3 = nullptr,
    void* arg4 = nullptr, void* arg5 = nullptr);

#define KEY_OPER_PREFIX_BITMASK 0x10
#define MAX_VARCHAR_LEN 1024

typedef enum : uint8_t {
    READ_KEY_EXACT = 0,    // equal
    READ_KEY_LIKE = 1,     // like
    READ_KEY_OR_NEXT = 2,  // ge
    READ_KEY_AFTER = 3,    // gt
    READ_KEY_OR_PREV = 4,  // le
    READ_KEY_BEFORE = 5,   // lt
    READ_INVALID = 6,

    // partial key eq
    READ_PREFIX = KEY_OPER_PREFIX_BITMASK | READ_KEY_EXACT,
    // partial key ge
    READ_PREFIX_OR_NEXT = KEY_OPER_PREFIX_BITMASK | READ_KEY_OR_NEXT,
    // partial key gt
    READ_PREFIX_AFTER = KEY_OPER_PREFIX_BITMASK | READ_KEY_AFTER,
    // partial key le
    READ_PREFIX_OR_PREV = KEY_OPER_PREFIX_BITMASK | READ_KEY_OR_PREV,
    // partial key lt
    READ_PREFIX_BEFORE = KEY_OPER_PREFIX_BITMASK | READ_KEY_BEFORE,
    // partial key like
    READ_PREFIX_LIKE = KEY_OPER_PREFIX_BITMASK | READ_KEY_LIKE,
} KEY_OPER;

typedef enum : uint8_t { SORTDIR_NONE = 0, SORTDIR_ASC = 1, SORTDIR_DESC = 2 } SORTDIR_ENUM;

typedef struct Order_St {
    SORTDIR_ENUM m_order;
    int m_lastMatch;
    int m_cols[MAX_KEY_COLUMNS];

    void init()
    {
        m_order = SORTDIR_NONE;
        m_lastMatch = -1;
        for (uint32_t i = 0; i < MAX_KEY_COLUMNS; i++)
            m_cols[i] = 0;
    }
} OrderSt;

#define MOT_REC_TID_NAME "ctid"

typedef struct MOTRecConvert {
    union {
        uint64_t m_ptr;
        ItemPointerData m_self; /* SelfItemPointer */
    } m_u;
} MOTRecConvertSt;

#define SORT_STRATEGY(x) ((x == BTGreaterStrategyNumber) ? SORTDIR_DESC : SORTDIR_ASC)

class MatchIndex {
public:
    MatchIndex()
    {
        Init();
    }

    ~MatchIndex()
    {}

    List* m_remoteConds = nullptr;
    List* m_remoteCondsOrig = nullptr;
    MOT::Index* m_ix = nullptr;
    int32_t m_ixPosition = 0;
    Expr* m_colMatch[2][MAX_KEY_COLUMNS];
    Expr* m_parentColMatch[2][MAX_KEY_COLUMNS];
    KEY_OPER m_opers[2][MAX_KEY_COLUMNS];
    int32_t m_params[2][MAX_KEY_COLUMNS];
    int32_t m_numMatches[2] = {0, 0};
    double m_costs[2] = {0, 0};
    KEY_OPER m_ixOpers[2] = {READ_INVALID, READ_INVALID};

    // this is for iteration start condition
    int32_t m_start = -1;
    int32_t m_end = -1;
    double m_cost = 0;

    void Init()
    {
        for (int i = 0; i < 2; i++) {
            for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
                m_colMatch[i][j] = nullptr;
                m_parentColMatch[i][j] = nullptr;
                m_opers[i][j] = READ_INVALID;
                m_params[i][j] = -1;
            }
            m_ixOpers[i] = READ_INVALID;
            m_costs[i] = 0;
            m_numMatches[i] = 0;
        }
        m_start = m_end = -1;
    }

    bool IsSameOper(KEY_OPER op1, KEY_OPER op2) const;
    void ClearPreviousMatch(MOTFdwStateSt* state, bool set_local, int i, int j);
    bool SetIndexColumn(MOTFdwStateSt* state, int16_t colNum, KEY_OPER op, Expr* expr, Expr* parent, bool set_local);

    inline bool IsUsable() const
    {
        return (m_colMatch[0][0] != nullptr);
    }
    inline bool IsFullMatch() const;

    inline int32_t GetNumMatchedCols() const
    {
        return m_numMatches[0];
    }
    inline double GetCost(int numClauses);
    bool CanApplyOrdering(const int* orderCols) const;
    bool AdjustForOrdering(bool desc);
    void Serialize(List** list) const;
    void Deserialize(ListCell* cell, uint64_t exTableID);
    void Clean(MOTFdwStateSt* state);
};

class MatchIndexArr {
public:
    MatchIndexArr()
    {
        for (uint i = 0; i < MAX_NUM_INDEXES; i++)
            m_idx[i] = nullptr;
    }

    ~MatchIndexArr()
    {}

    void Clear(bool release = false)
    {
        for (uint i = 0; i < MAX_NUM_INDEXES; i++) {
            if (m_idx[i]) {
                if (release)
                    pfree(m_idx[i]);
                m_idx[i] = nullptr;
            }
        }
    }
    MatchIndex* m_idx[MAX_NUM_INDEXES];
};

struct MOTFdwState_St {
    ::TransactionId m_txnId;
    bool m_allocInScan;
    CmdType m_cmdOper;
    SORTDIR_ENUM m_order;
    bool m_hasForUpdate;
    Oid m_foreignTableId;
    AttrNumber m_numAttrs;
    AttrNumber m_ctidNum;
    uint16_t m_numExpr;
    uint8_t* m_attrsUsed;
    uint8_t* m_attrsModified;  // this will be merged into attrs_used in BeginModify
    List* m_remoteConds;
    List* m_remoteCondsOrig;
    List* m_localConds;
    List* m_execExprs;
    ExprContext* m_econtext;
    double m_startupCost;
    double m_totalCost;

    MatchIndex* m_bestIx;
    MatchIndex* m_paramBestIx;
    MatchIndex m_bestIxBuf;

    // ENGINE
    MOT::Table* m_table;
    MOT::IndexIterator* m_cursor[2] = {nullptr, nullptr};
    MOT::TxnManager* m_currTxn;
    MOT::Row* m_currRow = nullptr;
    void* m_currItem = nullptr;
    uint32_t m_rowsFound = 0;
    bool m_cursorOpened = false;
    MOT::MaxKey m_stateKey[2];
    bool m_forwardDirectionScan;
    MOT::AccessType m_internalCmdOper;
};

class MOTAdaptor {
public:
    static void Init();
    static void Fini();
    static void NotifyConfigChange();
    static void InitDataNodeId();

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
    static MOT::TxnManager* InitTxnManager(MOT::ConnectionId connection_id = INVALID_CONNECTION_ID);
    static void DestroyTxn(int status, Datum ptr);
    static void DeleteTablePtr(MOT::Table* t);

    static MOT::RC CreateTable(CreateForeignTableStmt* table, TransactionId tid);
    static MOT::RC CreateIndex(IndexStmt* index, TransactionId tid);
    static MOT::RC DropIndex(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC DropTable(DropForeignStmt* stmt, TransactionId tid);
    static MOT::RC TruncateTable(Relation rel, TransactionId tid);
    static MOT::RC VacuumTable(Relation rel, TransactionId tid);
    static uint64_t GetTableIndexSize(uint64_t tabId, uint64_t ixId);
    static MotMemoryDetail* GetMemSize(uint32_t* nodeCount, bool isGlobal);
    static MotSessionMemoryDetail* GetSessionMemSize(uint32_t* sessionCount);
    static MOT::RC Commit(TransactionId tid);
    static MOT::RC EndTransaction(TransactionId tid);
    static MOT::RC Rollback(TransactionId tid);
    static MOT::RC Prepare(TransactionId tid);
    static MOT::RC CommitPrepared(TransactionId tid);
    static MOT::RC RollbackPrepared(TransactionId tid);
    static MOT::RC FailedCommitPrepared(TransactionId tid);
    static MOT::RC InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);
    static MOT::RC UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);
    static MOT::RC DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot);

    /* Convertors */
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

    // data conversion
    static void DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data);
    static void DatumToMOTKey(MOT::Column* col, Expr* expr, Datum datum, Oid type, uint8_t* data, size_t len,
        KEY_OPER oper, uint8_t fill = 0x00);
    static void MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null);

    static void PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow);
    static void PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow);
    static void UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow);

    // scan helpers
    static void OpenCursor(Relation rel, MOTFdwStateSt* festate);
    static bool IsScanEnd(MOTFdwStateSt* festate);
    static void CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start);

    // planning helpers
    static bool SetMatchingExpr(MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr,
        Expr* parent, bool set_local);
    static MatchIndex* GetBestMatchIndex(
        MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal = true);
    inline static int32_t AddParam(List** params, Expr* expr);

    static MOT::MOTEngine* m_engine;
    static bool m_initialized;
    static bool m_callbacks_initialized;
};

inline MOT::TxnManager* GetSafeTxn(::TransactionId txn_id = 0)
{
    if (!u_sess->mot_cxt.txn_manager) {
        MOTAdaptor::InitTxnManager();
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            if (txn_id != 0) {
                u_sess->mot_cxt.txn_manager->SetTransactionId(txn_id);
            }
        } else {
            report_pg_error(MOT_GET_ROOT_ERROR_RC(), nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
        }
    }
    return u_sess->mot_cxt.txn_manager;
}

extern void EnsureSafeThreadAccess();

#endif  // MOT_INT_H
