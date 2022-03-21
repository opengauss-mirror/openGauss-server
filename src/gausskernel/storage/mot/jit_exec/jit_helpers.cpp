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
 * jit_helpers.cpp
 *    Helper functions for JIT execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_helpers.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "access/xact.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "jit_helpers.h"
#include "jit_common.h"
#include "mot_internal.h"
#include "utilities.h"
#include <unordered_set>

typedef std::unordered_set<int64_t> DistinctIntSetType;
typedef std::unordered_set<double> DistinctDoubleSetType;

DECLARE_LOGGER(LlvmHelpers, JitExec)

/** @brief Helper to prepare a numeric zero. */
static Datum makeNumericZero()
{
    return DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
}

/** @brief Convert numeric Datum to double precision value. */
static double numericToDouble(Datum numeric_value)
{
    return (double)DatumGetFloat8(DirectFunctionCall1(numeric_float8, numeric_value));
}

/** @brief Allocates and initializes a distinct set of integers. */
static void* prepareDistinctIntSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctIntSetType));
    return new (buf) DistinctIntSetType();
}

/** @brief Allocates and initializes a distinct set of double-precision values. */
static void* prepareDistinctDoubleSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctDoubleSetType));
    return new (buf) DistinctDoubleSetType();
}

/** @brief Inserts an integer to a distinct set of integers. */
static int insertDistinctIntItem(void* distinct_set, int64_t item)
{
    int result = 0;
    DistinctIntSetType* int_set = (DistinctIntSetType*)distinct_set;
    if (int_set->insert(item).second) {
        result = 1;
    }
    return result;
}

/** @brief Inserts an integer to a distinct set of double-precision values. */
static int insertDistinctDoubleItem(void* distinct_set, double item)
{
    int result = 0;
    DistinctDoubleSetType* double_set = (DistinctDoubleSetType*)distinct_set;
    if (double_set->insert(item).second) {
        result = 1;
    }
    return result;
}

/** @brief Destroys and frees a distinct set of integers. */
static void destroyDistinctIntSet(void* distinct_set)
{
    DistinctIntSetType* int_set = (DistinctIntSetType*)distinct_set;
    int_set->~DistinctIntSetType();
    MOT::MemSessionFree(distinct_set);
}

/** @brief Destroys and frees a distinct set of double-precision values. */
static void destroyDistinctDoubleSet(void* distinct_set)
{
    DistinctDoubleSetType* double_set = (DistinctDoubleSetType*)distinct_set;
    double_set->~DistinctDoubleSetType();
    MOT::MemSessionFree(distinct_set);
}

/*---------------------------  DEBUG Print Helpers ---------------------------*/
#ifdef MOT_JIT_DEBUG
/** @brief Prints to log a numeric value. */
static void dbg_print_numeric(const char* msg, Numeric num)
{
    NumericDigit* digits = NUMERIC_DIGITS(num);
    int ndigits;
    int i;

    ndigits = NUMERIC_NDIGITS(num);

    MOT_LOG_BEGIN(MOT::LogLevel::LL_DEBUG, "%s: NUMERIC w=%d d=%d ", msg, NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));
    switch (NUMERIC_SIGN(num)) {
        case NUMERIC_POS:
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "POS");
            break;

        case NUMERIC_NEG:
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "NEG");
            break;

        case NUMERIC_NAN:
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "NaN");
            break;

        default:
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "SIGN=0x%x", NUMERIC_SIGN(num));
            break;
    }

    for (i = 0; i < ndigits; i++) {
        MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, " %0*d", DEC_DIGITS, digits[i]);
    }
    MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, " (%f)", numericToDouble(NumericGetDatum(num)));
    MOT_LOG_END(MOT::LogLevel::LL_DEBUG);
}

/** @brief Prints to log a varchar value. */
static void dbg_print_varchar(const char* msg, VarChar* vc)
{
    size_t size = VARSIZE(vc);
    char* src = VARDATA(vc);
    MOT_LOG_DEBUG("%s: size=%u, data=%.*s", msg, (unsigned)size, (int)size, src);
    size = VARSIZE_ANY_EXHDR(vc);
    src = VARDATA_ANY(vc);
    MOT_LOG_DEBUG("%s: [PG] size=%u, data=%.*s", msg, (unsigned)size, (int)size, src);
    // NOTE: last printout looks better, make sure this is what gets into the row
}

/** @var Prints to log a geneirc datum value. */
static void dbg_print_datum(const char* msg, Oid ptype, Datum datum, bool isnull)
{
    if (isnull) {
        MOT_LOG_DEBUG("[type %u] NULL", ptype);
    } else if (ptype == NUMERICOID) {
        dbg_print_numeric(msg, DatumGetNumeric(datum));
    } else if (ptype == VARCHAROID) {
        dbg_print_varchar(msg, DatumGetVarCharPP(datum));
    } else {
        MOT_LOG_BEGIN(MOT::LogLevel::LL_DEBUG, "%s: ", msg);
        switch (ptype) {
            case BOOLOID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[bool] %u", (unsigned)DatumGetBool(datum));
                break;

            case CHAROID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[char] %u", (unsigned)DatumGetChar(datum));
                break;

            case INT1OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[int1] %u", (unsigned)DatumGetUInt8(datum));
                break;

            case INT2OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[int2] %u", (unsigned)DatumGetUInt16(datum));
                break;

            case INT4OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[int4] %u", (unsigned)DatumGetUInt32(datum));
                break;

            case INT8OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[int8] %" PRIu64, (uint64_t)DatumGetUInt64(datum));
                break;

            case TIMESTAMPOID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[timestamp] %" PRIu64, (uint64_t)DatumGetTimestamp(datum));
                break;

            case FLOAT4OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[float4] %f", (double)DatumGetFloat4(datum));
                break;

            case FLOAT8OID:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[float8] %f", (double)DatumGetFloat8(datum));
                break;

            default:
                MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "[type %u] %" PRIu64, ptype, (uint64_t)datum);
                break;
        }

        MOT_LOG_END(MOT::LogLevel::LL_DEBUG);
    }
}
#endif

// helper debug printing macros
#ifdef MOT_JIT_DEBUG
#define DBG_PRINT_NUMERIC(msg, numeric)                 \
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) { \
        dbg_print_numeric(msg, numeric);                \
    }
#else
#define DBG_PRINT_NUMERIC(msg, numeric)
#endif

#ifdef MOT_JIT_DEBUG
#define DBG_PRINT_VARCHAR(msg, vc)                      \
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) { \
        dbg_print_varchar(msg, vc);                     \
    }
#else
#define DBG_PRINT_VARCHAR(msg, vc)
#endif

#ifdef MOT_JIT_DEBUG
#define DBG_PRINT_DATUM(msg, ptype, datum, isnull)      \
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) { \
        dbg_print_datum(msg, ptype, datum, isnull);     \
    }
#else
#define DBG_PRINT_DATUM(msg, ptype, datum, isnull)
#endif

static Oid column_type_to_pg(MOT::MOT_CATALOG_FIELD_TYPES column_type)
{
    Oid pg_type = -1;
    switch (column_type) {
        case MOT::MOT_TYPE_DECIMAL:
            pg_type = NUMERICOID;
            break;

        case MOT::MOT_TYPE_VARCHAR:
            pg_type = VARCHAROID;
            break;

        case MOT::MOT_TYPE_CHAR:
            pg_type = CHAROID;
            break;

        case MOT::MOT_TYPE_TINY:
            pg_type = INT1OID;
            break;

        case MOT::MOT_TYPE_SHORT:
            pg_type = INT2OID;
            break;

        case MOT::MOT_TYPE_INT:
            pg_type = INT4OID;
            break;

        case MOT::MOT_TYPE_LONG:
            pg_type = INT8OID;
            break;

        case MOT::MOT_TYPE_FLOAT:
            pg_type = FLOAT4OID;
            break;

        case MOT::MOT_TYPE_DOUBLE:
            pg_type = FLOAT8OID;
            break;

        case MOT::MOT_TYPE_DATE:
            pg_type = DATEOID;
            break;

        case MOT::MOT_TYPE_TIME:
            pg_type = TIMEOID;
            break;

        case MOT::MOT_TYPE_TIMESTAMP:
            pg_type = TIMESTAMPOID;
            break;

        case MOT::MOT_TYPE_TIMESTAMPTZ:
            pg_type = TIMESTAMPTZOID;
            break;

        case MOT::MOT_TYPE_INTERVAL:
            pg_type = INTERVALOID;
            break;

        case MOT::MOT_TYPE_TIMETZ:
            pg_type = TIMETZOID;
            break;

        case MOT::MOT_TYPE_BLOB:
            pg_type = BLOBOID;
            break;

        default:
            break;
    }

    return pg_type;
}

/*---------------------------  LLVM Access Helpers ---------------------------*/
extern "C" {
void debugLog(const char* function, const char* msg)
{
    MOT_LOG_DEBUG("%s: %s", function, msg);
}

/*---------------------------  Engine Access Helpers ---------------------------*/
int isSoftMemoryLimitReached()
{
    int result = 0;
    if (MOT::MOTEngine::GetInstance()->IsSoftMemoryLimitReached()) {
        MOT_LOG_TRACE("Memory limit reached, aborting transaction");
        result = 1;
    }
    return result;
}

MOT::MOTEngine* getMOTEngine()
{
    return MOT::MOTEngine::GetInstance();
}

MOT::Index* getPrimaryIndex(MOT::Table* table)
{
    MOT::Index* index = table->GetPrimaryIndex();
    MOT_LOG_DEBUG("Retrieved primary index %p from table %s @%p", index, table->GetTableName().c_str(), table);
    return index;
}

MOT::Index* getTableIndex(MOT::Table* table, int index_id)
{
    MOT::Index* index = table->GetSecondaryIndex(index_id);
    MOT_LOG_DEBUG("Retrieved index %p [%s]from table %s @%p by id %d",
        index,
        index->GetName().c_str(),
        table->GetTableName().c_str(),
        table,
        index_id);
    return index;
}

void InitKey(MOT::Key* key, MOT::Index* index)
{
    MOT_LOG_DEBUG("Initializing key %p by source index %s (%p)", key, index->GetName().c_str(), index);
    key->InitKey(index->GetKeyLength());
    MOT_LOG_DEBUG("key %p initialized to %u bytes", key, key->GetKeyLength());
}

MOT::Column* getColumnAt(MOT::Table* table, int table_colid)
{
    MOT_LOG_DEBUG("Retrieving table column %d from table %p", table_colid, table);
    MOT::Column* column = table->GetField(table_colid);
    MOT_LOG_DEBUG("Retrieved table column %p id %d from table %s at %p",
        column,
        table_colid,
        table->GetTableName().c_str(),
        table);
    return column;
}

void setExprArgIsNull(int arg_pos, int isnull)
{
    MOT_LOG_DEBUG("Setting expression argument %d isnull to: %d", arg_pos, isnull);
    u_sess->mot_cxt.jit_context->m_argIsNull[arg_pos] = isnull ? 1 : 0;
}

int getExprArgIsNull(int arg_pos)
{
    int result = u_sess->mot_cxt.jit_context->m_argIsNull[arg_pos];
    MOT_LOG_DEBUG("Retrieved expression argument %d isnull: %d", arg_pos, result);
    return result;
}

Datum GetConstAt(int constId, int argPos)
{
    MOT_LOG_DEBUG("Retrieving constant datum by id %d", constId);
    Datum result = PointerGetDatum(nullptr);
    JitExec::JitContext* ctx = u_sess->mot_cxt.jit_context;
    if (constId < (int)ctx->m_constDatums.m_datumCount) {
        JitExec::JitDatum* datum = &ctx->m_constDatums.m_datums[constId];
        result = datum->m_datum;
        setExprArgIsNull(argPos, datum->m_isNull);
        DBG_PRINT_DATUM("Retrieved constant datum", datum->m_type, datum->m_datum, datum->m_isNull);
    } else {
        MOT_LOG_ERROR("Invalid constant identifier: %d", constId);
    }
    return result;
}

Datum getDatumParam(ParamListInfo params, int paramid, int arg_pos)
{
    MOT_LOG_DEBUG("Retrieving datum param at index %d", paramid);
    DBG_PRINT_DATUM(
        "Param value", params->params[paramid].ptype, params->params[paramid].value, params->params[paramid].isnull);
    setExprArgIsNull(arg_pos, params->params[paramid].isnull);
    return params->params[paramid].value;
}

Datum readDatumColumn(MOT::Table* table, MOT::Row* row, int colid, int arg_pos)
{
    MOT::Column* column = table->GetField(colid);
    MOT_LOG_DEBUG("Reading Datum value from row %p column %p", row, column);
    Datum result = PointerGetDatum(NULL);  // return proper NULL datum if column value is null

    FormData_pg_attribute attr;
    attr.attnum = colid;
    attr.atttypid = column_type_to_pg(column->m_type);

    bool isnull = false;
    MOTAdaptor::MOTToDatum(table, &attr, (uint8_t*)row->GetData(), &result, &isnull);
    DBG_PRINT_DATUM("Column value", attr.atttypid, result, isnull);
    setExprArgIsNull(arg_pos, (int)isnull);

    return result;
}

void writeDatumColumn(MOT::Row* row, MOT::Column* column, Datum value)
{
    MOT_LOG_DEBUG("Writing to row %p column %p datum value", row, column);
    DBG_PRINT_DATUM("Datum value", column_type_to_pg(column->m_type), value, 0);
    MOTAdaptor::DatumToMOT(column, value, column_type_to_pg(column->m_type), (uint8_t*)row->GetData());
}

void buildDatumKey(
    MOT::Column* column, MOT::Key* key, Datum value, int index_colid, int offset, int size, int value_type)
{
    int isnull = getExprArgIsNull(0);
    MOT_LOG_DEBUG("buildKey: writing datum value for index column %d at key buf offset %d (col=%p, key=%p, datum=%p, "
                  "value-type=%d, key-size=%u, col-size=%u, size=%d, is-null: %d)",
        index_colid,
        offset,
        column,
        key,
        value,
        value_type,
        (unsigned)key->GetKeyLength(),
        (unsigned)column->m_size,
        size,
        isnull);
    DBG_PRINT_DATUM("Key Datum", value_type, value, 0);
    if (isnull) {
        MOT_LOG_DEBUG("Setting null key datum at offset %d (%d bytes)", offset, size);
        errno_t erc = memset_s(key->GetKeyBuf() + offset, key->GetKeyLength() - offset, 0x00, size);
        securec_check(erc, "\0", "\0");
    } else {
        MOTAdaptor::DatumToMOTKey(column,
            (Oid)value_type,
            value,
            column->m_envelopeType,
            key->GetKeyBuf() + offset,
            column->m_size,
            KEY_OPER::READ_KEY_EXACT,
            0x00);
    }
}

/*---------------------------  Invoke PG Operators ---------------------------*/
// cast operators retain first null parameter as null result
// other operator will crash if null is provided
#define APPLY_UNARY_OPERATOR(funcid, name)                \
    Datum invoke_##name(Datum arg, int arg_pos)           \
    {                                                     \
        MOT_LOG_DEBUG("Invoking unary operator: " #name); \
        return DirectFunctionCall1(name, arg);            \
    }

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                \
    Datum invoke_##name(Datum arg, int arg_pos)                \
    {                                                          \
        MOT_LOG_DEBUG("Invoking unary cast operator: " #name); \
        int isnull = getExprArgIsNull(arg_pos);                \
        return isnull ? arg : DirectFunctionCall1(name, arg);  \
    }

#define APPLY_BINARY_OPERATOR(funcid, name)                \
    Datum invoke_##name(Datum lhs, Datum rhs, int arg_pos) \
    {                                                      \
        MOT_LOG_DEBUG("Invoking binary operator: " #name); \
        return DirectFunctionCall2(name, lhs, rhs);        \
    }

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                       \
    Datum invoke_##name(Datum lhs, Datum rhs, int arg_pos)             \
    {                                                                  \
        MOT_LOG_DEBUG("Invoking binary cast operator: " #name);        \
        int lhs_isnull = getExprArgIsNull(arg_pos);                    \
        return lhs_isnull ? lhs : DirectFunctionCall2(name, lhs, rhs); \
    }

#define APPLY_TERNARY_OPERATOR(funcid, name)                             \
    Datum invoke_##name(Datum arg1, Datum arg2, Datum arg3, int arg_pos) \
    {                                                                    \
        MOT_LOG_DEBUG("Invoking ternary operator: " #name);              \
        return DirectFunctionCall3(name, arg1, arg2, arg3);              \
    }

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                \
    Datum invoke_##name(Datum arg1, Datum arg2, Datum arg3, int arg_pos)         \
    {                                                                            \
        MOT_LOG_DEBUG("Invoking ternary operator: " #name);                      \
        int arg1_isnull = getExprArgIsNull(arg_pos);                             \
        return arg1_isnull ? arg1 : DirectFunctionCall3(name, arg1, arg2, arg3); \
    }

APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

MOT::Row* searchRow(MOT::Table* table, MOT::Key* key, int access_mode_value)
{
    MOT_LOG_DEBUG("Searching row at table %p by key %p", table, key);
    MOT::Row* row = NULL;
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT::AccessType access_mode = (MOT::AccessType)access_mode_value;
    row = curr_txn->RowLookupByKey(table, access_mode, key);
    if (row == nullptr) {
        MOT_LOG_DEBUG("Row not found");
    }
    MOT_LOG_DEBUG("Returning row: %p", row);
    return row;
}

/*---------------------------  BitmapSet Helpers ---------------------------*/
void setBit(MOT::BitmapSet* bmp, int col)
{
    MOT_LOG_DEBUG("Setting column %d at bitset %p", col, bmp);
    bmp->SetBit(col);
    MOT_LOG_DEBUG("Column %d at bitset %p set successfully", col, bmp);
}

void resetBitmapSet(MOT::BitmapSet* bmp)
{
    MOT_LOG_DEBUG("Resetting bitset %p", bmp);
    bmp->Clear();
}

/*---------------------------  General Helpers ---------------------------*/
int writeRow(MOT::Row* row, MOT::BitmapSet* bmp)
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("Writing row %p to DB", row);
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("Current txn is: %p", curr_txn);
    rc = curr_txn->UpdateLastRowState(MOT::AccessType::WR);
    MOT_LOG_DEBUG("Row write result: %d", (int)rc);
    if (rc == MOT::RC_OK) {
        MOT_LOG_DEBUG("Overwriting row %p with bitset %p", row, bmp);
        rc = curr_txn->OverwriteRow(row, *bmp);
        MOT_LOG_DEBUG("Overwrite row result: %d", (int)rc);
    }
    return (int)rc;
}

int getTableFieldCount(MOT::Table* table)
{
    MOT_LOG_DEBUG("Retrieving table %p field count", table);
    int result = (int)table->GetFieldCount();
    MOT_LOG_DEBUG("Table %p field count is %u", table, result);
    return result;
}

MOT::Row* createNewRow(MOT::Table* table)
{
    MOT_LOG_DEBUG("Creating new row from table %p", table);
    MOT::Row* row = table->CreateNewRow();
    MOT_LOG_DEBUG("Created new row %p", row);
    return row;
}

int insertRow(MOT::Table* table, MOT::Row* row)
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("Inserting row %p to DB", row);
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("Current txn is: %p", curr_txn);
    rc = table->InsertRow(row, curr_txn);
    if (rc != MOT::RC_OK) {
        MOT_LOG_DEBUG("Insert row failed with rc: %d", (int)rc);
    }
    return (int)rc;
}

int deleteRow()
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("Deleting row from DB");
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("Current txn is: %p", curr_txn);
    rc = curr_txn->DeleteLastRow();
    if (rc != MOT::RC_OK) {
        MOT_LOG_DEBUG("Delete row failed with rc: %d", (int)rc);
    }
    return (int)rc;
}

void setRowNullBits(MOT::Table* table, MOT::Row* row)
{
    // we assume that every column not participating in INSERT is a NULL, so we nullify all row null bits
    // later, each inserted column will update its null bit status
    uint8_t* rowData = const_cast<uint8_t*>(row->GetData());
    uint8_t* bits = rowData + table->GetFieldOffset((uint64_t)0);
    MOT_LOG_DEBUG("Setting row %p null bits at address %p, %u bytes", row, bits, (unsigned)table->GetFieldSize(0));
    errno_t erc = memset_s(bits, table->GetFieldSize(0), 0x00, table->GetFieldSize(0));
    securec_check(erc, "\0", "\0");
}

int setConstNullBit(MOT::Table* table, MOT::Row* row, int table_colid, int isnull)
{
    MOT_LOG_DEBUG("Setting const null bit at table %p row %p column id %d: isnull=%d", table, row, table_colid, isnull);

    if (isnull && table->GetField(table_colid)->m_isNotNull) {
        MOT_LOG_DEBUG("Cannot set null to not-nullable column %d", table_colid);
        if (u_sess->mot_cxt.jit_context != NULL) {
            u_sess->mot_cxt.jit_context->m_nullColumnId = table_colid;
        }
        return (int)MOT::RC_NULL_VIOLATION;
    }

    uint8_t* rowData = (uint8_t*)row->GetData();
    uint8_t* bits = rowData + table->GetFieldOffset((uint64_t)0);
    if (isnull) {
        BITMAP_CLEAR(bits, (table_colid - 1));  // zero-based index column id
    } else {
        BITMAP_SET(bits, (table_colid - 1));  // zero-based index column id
    }

    return (int)MOT::RC_OK;
}

int setExprResultNullBit(MOT::Table* table, MOT::Row* row, int table_colid)
{
    int isnull = getExprArgIsNull(0);  // final result is always put in arg zero
    MOT_LOG_DEBUG("Setting expression result isnull at column %d to: %d", table_colid, isnull);
    return setConstNullBit(table, row, table_colid, isnull);
}

void execClearTuple(TupleTableSlot* slot)
{
    MOT_LOG_DEBUG("Clearing tuple %p", slot);
    ::ExecClearTuple(slot);
}

void execStoreVirtualTuple(TupleTableSlot* slot)
{
    MOT_LOG_DEBUG("Storing virtual tuple %p", slot);
    ::ExecStoreVirtualTuple(slot);
}

void selectColumn(MOT::Table* table, MOT::Row* row, TupleTableSlot* slot, int table_colid, int tuple_colid)
{
    MOT_LOG_DEBUG("Selecting into tuple column %d from table %s, row %p column id %d [%s]",
        tuple_colid,
        table->GetTableName().c_str(),
        row,
        table_colid,
        table->GetFieldName(table_colid));
    uint8_t* rowData = const_cast<uint8_t*>(row->GetData());
    slot->tts_tupleDescriptor->attrs[tuple_colid]->attnum = table_colid;
    MOTAdaptor::MOTToDatum(table,
        slot->tts_tupleDescriptor->attrs[tuple_colid],
        rowData,
        &(slot->tts_values[tuple_colid]),
        &(slot->tts_isnull[tuple_colid]));
    DBG_PRINT_DATUM("Column Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid]->atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
}

void setTpProcessed(uint64_t* tp_processed, uint64_t rows)
{
    MOT_LOG_DEBUG("Setting tp_processed at %p to %" PRIu64 "", tp_processed, rows);
    *tp_processed = rows;
}

void setScanEnded(int* scan_ended, int result)
{
    MOT_LOG_DEBUG("Setting scan_ended at %p to %d", scan_ended, result);
    *scan_ended = result;
}

/*---------------------------  Range Update Helpers ---------------------------*/
void copyKey(MOT::Index* index, MOT::Key* src_key, MOT::Key* dest_key)
{
    MOT_LOG_DEBUG("Copy key: index = %s (%p), src_key = %p (%u bytes), dest_key = %p (%u bytes)",
        index->GetName().c_str(),
        index,
        src_key,
        src_key->GetKeyLength(),
        dest_key,
        dest_key->GetKeyLength());
    dest_key->CpKey(*src_key);
    MOT_LOG_DEBUG("Copied key %p into key %p", src_key, dest_key);
}

void FillKeyPattern(MOT::Key* key, unsigned char pattern, int offset, int size)
{
    MOT_LOG_DEBUG("Filling key %p pattern %u at offset %d, size %d: %s",
        key,
        (unsigned)pattern,
        offset,
        size,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    key->FillPattern(pattern, size, offset);
    MOT_LOG_DEBUG("Filled key %p pattern %u at offset %d, size %d: %s",
        key,
        (unsigned)pattern,
        offset,
        size,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
}

void adjustKey(MOT::Key* key, MOT::Index* index, unsigned char pattern)
{
    MOT_LOG_DEBUG("Adjusting key %p with pattern %u: %s",
        key,
        (unsigned)pattern,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    index->AdjustKey(key, pattern);
    MOT_LOG_DEBUG("Adjusted key %p with pattern %u: %s",
        key,
        (unsigned)pattern,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
}

MOT::IndexIterator* searchIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound)
{
    MOT::IndexIterator* itr = NULL;
    bool matchKey = include_bound;  // include bound in query
    bool forwardDirection = forward_scan ? true : false;
    bool found = false;

    MOT_LOG_DEBUG("Creating begin iterator for index %p from key %p (include_bound=%s): %s",
        index,
        key,
        include_bound ? "true" : "false",
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("searchIterator: Current txn is: %p", curr_txn);

    itr = index->Search(key, matchKey, forwardDirection, curr_txn->GetThdId(), found);
    if (!found) {
        MOT_LOG_DEBUG("searchIterator: Exact match not found, still continuing, itr=%p", itr);
    } else {
        MOT_LOG_DEBUG("searchIterator: Exact match found with iterator %p", itr);
    }

    return itr;
}

MOT::IndexIterator* beginIterator(MOT::Index* index)
{
    return index->Begin(MOTCurrThreadId);
}

MOT::IndexIterator* createEndIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound)
{
    MOT::IndexIterator* itr = NULL;
    bool matchKey = include_bound;  // include bound in query
    bool forwardDirection =
        forward_scan ? false : true;  // in forward scan search key or previous, in backwards scan search key or next
    bool found = false;

    MOT_LOG_DEBUG(
        "Creating end iterator (forward_scan=%d, forwardDirection=%s, include_bound=%s)) for index %p from key %p: %s",
        forward_scan,
        forwardDirection ? "true" : "false",
        include_bound ? "true" : "false",
        index,
        key,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("Current txn is: %p", curr_txn);

    itr = index->Search(key, matchKey, forwardDirection, curr_txn->GetThdId(), found);
    if (!found) {
        MOT_LOG_DEBUG("createEndIterator: Exact match not found, still continuing, itr=%p", itr);
    } else {
        MOT_LOG_DEBUG("createEndIterator: Exact match found with iterator %p", itr);
    }
    return itr;
}

int isScanEnd(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int forward_scan)
{
    MOT_LOG_DEBUG("Checking if scan ended");

    int res = 0;

    if (itr != nullptr && !itr->IsValid()) {
        MOT_LOG_DEBUG("isScanEnd(): begin iterator is not valid");
        res = 1;
    } else if (end_itr != nullptr && !end_itr->IsValid()) {
        MOT_LOG_DEBUG("isScanEnd(): end iterator is not valid");
        res = 1;
    } else {
        const MOT::Key* startKey = nullptr;
        const MOT::Key* endKey = nullptr;

        if (itr != nullptr) {
            startKey = reinterpret_cast<const MOT::Key*>(const_cast<void*>(itr->GetKey()));
            MOT_LOG_DEBUG("Start key: %s", MOT::HexStr(startKey->GetKeyBuf(), startKey->GetKeyLength()).c_str());
        }
        if (end_itr != nullptr) {
            endKey = reinterpret_cast<const MOT::Key*>(const_cast<void*>(end_itr->GetKey()));
            MOT_LOG_DEBUG("End key:   %s", MOT::HexStr(endKey->GetKeyBuf(), endKey->GetKeyLength()).c_str());
        }

        if (startKey != nullptr && endKey != nullptr) {
            int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), index->GetKeySizeNoSuffix());
            MOT_LOG_DEBUG("isScanEnd(): cmpRes = %d", cmpRes);
            if (forward_scan) {
                if (cmpRes > 0) {  // end key is included in scan (so == is not reported as end of scan)
                    MOT_LOG_DEBUG("isScanEnd(): end of forward scan detected");
                    res = 1;
                }
            } else {
                if (cmpRes < 0) {
                    MOT_LOG_DEBUG("isScanEnd(): end of backward scan detected");
                    res = 1;
                }
            }
        } else {
            MOT_LOG_DEBUG("isScanEnd(): either start key or end key is invalid");
        }
    }

    return res;
}

MOT::Row* getRowFromIterator(
    MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int access_mode, int forward_scan)
{
    MOT::Row* row = NULL;
    MOT::RC rc = MOT::RC_OK;

    MOT_LOG_DEBUG("getRowFromIterator(): Retrieving row from iterator %p", itr);
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    do {
        // get row from iterator using primary sentinel
        MOT::Sentinel* sentinel = itr->GetPrimarySentinel();
        row = curr_txn->RowLookup((MOT::AccessType)access_mode, sentinel, rc);
        if (row == NULL) {
            MOT_LOG_DEBUG("getRowFromIterator(): Encountered NULL row during scan, advancing iterator");
            itr->Next();
            continue;
        }

        // verify the scan did not pass the end iterator
        if (isScanEnd(index, itr, end_itr, forward_scan)) {
            MOT_LOG_DEBUG("getRowFromIterator(): Detected end of scan");
            row = NULL;
            itr->Invalidate();
            break;
        }

        // prepare already for next round
        itr->Next();
        break;
    } while (itr->IsValid());

    MOT_LOG_DEBUG("getRowFromIterator(): Retrieved row %p from iterator %p", row, itr);
    return row;
}

void destroyIterator(MOT::IndexIterator* itr)
{
    MOT_LOG_DEBUG("Destroying iterator %p", itr);
    itr->Invalidate();
    itr->Destroy();
    delete itr;
}

/*---------------------------  Stateful Execution Helpers ---------------------------*/
void setStateIterator(MOT::IndexIterator* itr, int begin_itr, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state iterator %p (begin_itr=%d, inner_scan=%d)", itr, begin_itr, inner_scan);
    if (u_sess->mot_cxt.jit_context) {
        if (inner_scan) {
            if (begin_itr) {
                u_sess->mot_cxt.jit_context->m_innerBeginIterator = itr;
            } else {
                u_sess->mot_cxt.jit_context->m_innerEndIterator = itr;
            }
        } else {
            if (begin_itr) {
                u_sess->mot_cxt.jit_context->m_beginIterator = itr;
            } else {
                u_sess->mot_cxt.jit_context->m_endIterator = itr;
            }
        }
    }
}

MOT::IndexIterator* getStateIterator(int begin_itr, int inner_scan)
{
    MOT::IndexIterator* result = NULL;
    if (u_sess->mot_cxt.jit_context) {
        if (inner_scan) {
            if (begin_itr) {
                result = u_sess->mot_cxt.jit_context->m_innerBeginIterator;
            } else {
                result = u_sess->mot_cxt.jit_context->m_innerEndIterator;
            }
        } else {
            if (begin_itr) {
                result = u_sess->mot_cxt.jit_context->m_beginIterator;
            } else {
                result = u_sess->mot_cxt.jit_context->m_endIterator;
            }
        }
    }
    MOT_LOG_DEBUG("Retrieved state iterator %p (begin_itr=%d, inner_scan=%d)", result, begin_itr, inner_scan);
    return result;
}

int isStateIteratorNull(int begin_itr, int inner_scan)
{
    int is_null = 0;
    if (u_sess->mot_cxt.jit_context) {
        if (inner_scan) {
            if (begin_itr) {
                is_null = u_sess->mot_cxt.jit_context->m_innerBeginIterator ? 0 : 1;
            } else {
                is_null = u_sess->mot_cxt.jit_context->m_innerEndIterator ? 0 : 1;
            }
        } else {
            if (begin_itr) {
                is_null = u_sess->mot_cxt.jit_context->m_beginIterator ? 0 : 1;
            } else {
                is_null = u_sess->mot_cxt.jit_context->m_endIterator ? 0 : 1;
            }
        }
    }
    MOT_LOG_DEBUG(
        "Checked if state iterator is null (begin_itr=%d, inner_scan=%d): result=%d", begin_itr, inner_scan, is_null);
    return is_null;
}

int isStateScanEnd(int forward_scan, int inner_scan)
{
    int result = -1;
    if (inner_scan) {
        result = isScanEnd(u_sess->mot_cxt.jit_context->m_innerIndex,
            u_sess->mot_cxt.jit_context->m_innerBeginIterator,
            u_sess->mot_cxt.jit_context->m_innerEndIterator,
            forward_scan);
    } else {
        result = isScanEnd(u_sess->mot_cxt.jit_context->m_index,
            u_sess->mot_cxt.jit_context->m_beginIterator,
            u_sess->mot_cxt.jit_context->m_endIterator,
            forward_scan);
    }
    MOT_LOG_DEBUG(
        "Checked if state scan ended (forward_scan=%d, inner_scan=%d): result=%d", forward_scan, inner_scan, result);
    return result;
}

MOT::Row* getRowFromStateIterator(int access_mode, int forward_scan, int inner_scan)
{
    MOT::Row* result = NULL;
    if (inner_scan) {
        result = getRowFromIterator(u_sess->mot_cxt.jit_context->m_innerIndex,
            u_sess->mot_cxt.jit_context->m_innerBeginIterator,
            u_sess->mot_cxt.jit_context->m_innerEndIterator,
            access_mode,
            forward_scan);
    } else {
        result = getRowFromIterator(u_sess->mot_cxt.jit_context->m_index,
            u_sess->mot_cxt.jit_context->m_beginIterator,
            u_sess->mot_cxt.jit_context->m_endIterator,
            access_mode,
            forward_scan);
    }
    MOT_LOG_DEBUG("Retrieved row %p from state iterator (access_mode=%d, forward_scan=%d, inner_scan=%d): result=%d",
        result,
        access_mode,
        forward_scan,
        inner_scan);
    return result;
}

void destroyStateIterators(int inner_scan)
{
    MOT_LOG_DEBUG("Destroying state iterators (inner_scan=%d)", inner_scan);
    if (inner_scan) {
        if (u_sess->mot_cxt.jit_context->m_innerBeginIterator) {
            destroyIterator(u_sess->mot_cxt.jit_context->m_innerBeginIterator);
            u_sess->mot_cxt.jit_context->m_innerBeginIterator = NULL;
        }
        if (u_sess->mot_cxt.jit_context->m_innerEndIterator) {
            destroyIterator(u_sess->mot_cxt.jit_context->m_innerEndIterator);
            u_sess->mot_cxt.jit_context->m_innerEndIterator = NULL;
        }
    } else {
        if (u_sess->mot_cxt.jit_context->m_beginIterator) {
            destroyIterator(u_sess->mot_cxt.jit_context->m_beginIterator);
            u_sess->mot_cxt.jit_context->m_beginIterator = NULL;
        }
        if (u_sess->mot_cxt.jit_context->m_endIterator) {
            destroyIterator(u_sess->mot_cxt.jit_context->m_endIterator);
            u_sess->mot_cxt.jit_context->m_endIterator = NULL;
        }
    }
}

void setStateScanEndFlag(int scan_ended, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state scan end flag to %d (inner_scan=%d)", scan_ended, inner_scan);
    if (inner_scan) {
        u_sess->mot_cxt.jit_context->m_innerScanEnded = scan_ended;
    } else {
        u_sess->mot_cxt.jit_context->m_scanEnded = scan_ended;
    }
}

int getStateScanEndFlag(int inner_scan)
{
    int result = -1;
    if (inner_scan) {
        result = (int)u_sess->mot_cxt.jit_context->m_innerScanEnded;
    } else {
        result = (int)u_sess->mot_cxt.jit_context->m_scanEnded;
    }
    MOT_LOG_DEBUG("Retrieved state scan end flag %d (inner_scan=%d)", result, inner_scan);
    return result;
}

void resetStateRow(int inner_scan)
{
    MOT_LOG_DEBUG("Resetting state row to NULL (inner_scan=%d)", inner_scan);
    if (inner_scan) {
        u_sess->mot_cxt.jit_context->m_innerRow = NULL;
    } else {
        u_sess->mot_cxt.jit_context->m_row = NULL;
    }
}

void setStateRow(MOT::Row* row, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state row to %p (inner_scan=%d)", row, inner_scan);
    if (inner_scan) {
        u_sess->mot_cxt.jit_context->m_innerRow = row;
    } else {
        u_sess->mot_cxt.jit_context->m_row = row;
    }
}

MOT::Row* getStateRow(int inner_scan)
{
    MOT::Row* result = NULL;
    if (inner_scan) {
        result = u_sess->mot_cxt.jit_context->m_innerRow;
    } else {
        result = u_sess->mot_cxt.jit_context->m_row;
    }
    MOT_LOG_DEBUG("Retrieved state row %p (inner_scan=%d)", result, inner_scan);
    return result;
}

void copyOuterStateRow()
{
    MOT_LOG_DEBUG("Copying outer state row %p into safe copy %p (for JOIN query)",
        u_sess->mot_cxt.jit_context->m_row,
        u_sess->mot_cxt.jit_context->m_outerRowCopy);
    u_sess->mot_cxt.jit_context->m_outerRowCopy->Copy(u_sess->mot_cxt.jit_context->m_row);
}

MOT::Row* getOuterStateRowCopy()
{
    MOT_LOG_DEBUG("Retrieved outer state row copy %p (for JOIN query)", u_sess->mot_cxt.jit_context->m_outerRowCopy);
    return u_sess->mot_cxt.jit_context->m_outerRowCopy;
}

int isStateRowNull(int inner_scan)
{
    int result = -1;
    if (inner_scan) {
        result = (u_sess->mot_cxt.jit_context->m_innerRow == NULL) ? 1 : 0;
    } else {
        result = (u_sess->mot_cxt.jit_context->m_row == NULL) ? 1 : 0;
    }
    MOT_LOG_DEBUG("Checked if state row is null (inner_scan=%d): result=%d", inner_scan, result);
    return result;
}

void resetStateLimitCounter()
{
    MOT_LOG_DEBUG("Resetting state limit counter to 0");
    u_sess->mot_cxt.jit_context->m_limitCounter = 0;
}

void incrementStateLimitCounter()
{
    ++u_sess->mot_cxt.jit_context->m_limitCounter;
    MOT_LOG_DEBUG("Incremented state limit counter to %d", u_sess->mot_cxt.jit_context->m_limitCounter);
}

int getStateLimitCounter()
{
    return u_sess->mot_cxt.jit_context->m_limitCounter;
    MOT_LOG_DEBUG("Retrieved state limit counter with value %d", u_sess->mot_cxt.jit_context->m_limitCounter);
}

void prepareAvgArray(int element_type, int element_count)
{
    MOT_LOG_DEBUG("Preparing AVG() array with %d elements of type %d", element_count, element_type);
    Datum* elements = (Datum*)palloc(sizeof(Datum) * element_count);
    for (int i = 0; i < element_count; ++i) {
        if (element_type == NUMERICOID) {
            elements[i] = makeNumericZero();
        } else if (element_type == INT8OID) {
            elements[i] = Int64GetDatum(0);
        } else if (element_type == FLOAT8OID) {
            elements[i] = Float8GetDatum(0.0f);
        }
    }

    int elmlen = 0;
    if (element_type == NUMERICOID) {
        elmlen = -1;  // Numeric is a varlena object (see definition of NumericData at utils/numeric.h, and numeric type
                      // in catalog/pg_type.h)
    } else if (element_type == INT8OID || element_type == FLOAT8OID) {
        elmlen = 8;
    }

    ArrayType* avg_array = construct_array(elements, element_count, element_type, elmlen, true, 0);
    u_sess->mot_cxt.jit_context->m_avgArray = PointerGetDatum(avg_array);
}

Datum loadAvgArray()
{
    Datum result = u_sess->mot_cxt.jit_context->m_avgArray;
    MOT_LOG_DEBUG("Loaded AVG() array %" PRIu64, (uint64_t)result);
    return result;
}

void saveAvgArray(Datum avg_array)
{
    MOT_LOG_DEBUG("Saving AVG() array %" PRIu64, (uint64_t)avg_array);
    u_sess->mot_cxt.jit_context->m_avgArray = avg_array;
}

Datum computeAvgFromArray(int element_type)
{
    Datum avg = PointerGetDatum(NULL);
    if (element_type == NUMERICOID) {
        avg = DirectFunctionCall1(numeric_avg, u_sess->mot_cxt.jit_context->m_avgArray);
    } else if (element_type == INT8OID) {
        avg = DirectFunctionCall1(int8_avg, u_sess->mot_cxt.jit_context->m_avgArray);
    } else if (element_type == FLOAT8OID) {
        avg = DirectFunctionCall1(float8_avg, u_sess->mot_cxt.jit_context->m_avgArray);
    }
    MOT_LOG_DEBUG("Computed AVG() from array %" PRIu64 ": %f",
        (uint64_t)u_sess->mot_cxt.jit_context->m_avgArray,
        numericToDouble(avg));
    return avg;
}

void resetAggValue(int element_type)
{
    MOT_LOG_DEBUG("Resetting aggregated value to zero of type %d", element_type);
    if (element_type == NUMERICOID) {
        u_sess->mot_cxt.jit_context->m_aggValue = makeNumericZero();
    } else if (element_type == INT8OID) {
        u_sess->mot_cxt.jit_context->m_aggValue = Int64GetDatum(0);
    } else if (element_type == FLOAT4OID) {
        u_sess->mot_cxt.jit_context->m_aggValue = Float4GetDatum(((float)0.0));
    } else if (element_type == FLOAT8OID) {
        u_sess->mot_cxt.jit_context->m_aggValue = Float8GetDatum(((double)0.0));
    }
}

void resetCountAgg()
{
    MOT_LOG_DEBUG("Resetting aggregated count value to 0");
    u_sess->mot_cxt.jit_context->m_aggValue = Int64GetDatum(0);
}

Datum getAggValue()
{
    Datum result = u_sess->mot_cxt.jit_context->m_aggValue;
    MOT_LOG_DEBUG("Retrieved aggregated value: %" PRIu64, result);
    return result;
}

void setAggValue(Datum value)
{
    MOT_LOG_DEBUG("Setting aggregated value to: %" PRIu64, value);
    u_sess->mot_cxt.jit_context->m_aggValue = value;
}

void resetAggMaxMinNull()
{
    MOT_LOG_DEBUG("Resetting aggregated max/min null flag to true");
    u_sess->mot_cxt.jit_context->m_maxMinAggNull = 1;
}

void setAggMaxMinNotNull()
{
    MOT_LOG_DEBUG("Setting aggregated max/min null flag to false");
    u_sess->mot_cxt.jit_context->m_maxMinAggNull = 0;
}

int getAggMaxMinIsNull()
{
    int result = (int)u_sess->mot_cxt.jit_context->m_maxMinAggNull;
    MOT_LOG_DEBUG("Retrieved aggregated max/min null flag: %d", result);
    return result;
}

void prepareDistinctSet(int element_type)
{
    switch (element_type) {
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
            MOT_LOG_DEBUG("Preparing distinct integer set for type %d", element_type);
            u_sess->mot_cxt.jit_context->m_distinctSet = prepareDistinctIntSet();
            break;

        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            MOT_LOG_DEBUG("Preparing distinct double-precision value set for type %d", element_type);
            u_sess->mot_cxt.jit_context->m_distinctSet = prepareDistinctDoubleSet();
            break;
        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }
}

int insertDistinctItem(int element_type, Datum item)
{
    int result = 0;
    switch (element_type) {
        case INT8OID:
            MOT_LOG_DEBUG("Inserting distinct int8 value %" PRIu64, DatumGetInt64(item));
            result = insertDistinctIntItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetInt64(item));
            break;

        case INT4OID:
            MOT_LOG_DEBUG("Inserting distinct int4 value %" PRIu64, DatumGetInt32(item));
            result = insertDistinctIntItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetInt32(item));
            break;

        case INT2OID:
            MOT_LOG_DEBUG("Inserting distinct int2 value %" PRIu64, DatumGetInt16(item));
            result = insertDistinctIntItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetInt16(item));
            break;

        case INT1OID:
            MOT_LOG_DEBUG("Inserting distinct int1 value %" PRIu64, DatumGetInt8(item));
            result = insertDistinctIntItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetInt8(item));
            break;

        case FLOAT4OID:
            MOT_LOG_DEBUG("Inserting distinct float4 value %f", (float)DatumGetFloat4(item));
            result = insertDistinctDoubleItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetFloat4(item));
            break;

        case FLOAT8OID:
            MOT_LOG_DEBUG("Inserting distinct float8 value %f", (double)DatumGetFloat8(item));
            result = insertDistinctDoubleItem(u_sess->mot_cxt.jit_context->m_distinctSet, DatumGetFloat8(item));
            break;

        case NUMERICOID:
            MOT_LOG_DEBUG("Inserting distinct numeric value %f", numericToDouble(item));
            result = insertDistinctDoubleItem(u_sess->mot_cxt.jit_context->m_distinctSet, numericToDouble(item));
            break;
        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }
    return result;
}

void destroyDistinctSet(int element_type)
{
    switch (element_type) {
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
            MOT_LOG_DEBUG("Destroying distinct integer set for type %d", element_type);
            destroyDistinctIntSet(u_sess->mot_cxt.jit_context->m_distinctSet);
            break;

        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            MOT_LOG_DEBUG("Destroying distinct double-precision value set for type %d", element_type);
            destroyDistinctDoubleSet(u_sess->mot_cxt.jit_context->m_distinctSet);
            break;
        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }
}

void resetTupleDatum(TupleTableSlot* slot, int tuple_colid, int zero_type)
{
    // write zero numeric on the fly (we rely on current memory context to clean up)
    MOT_LOG_DEBUG("Resetting tuple %p column %d datum to typed zero by type %d", slot, tuple_colid, zero_type)
    switch (zero_type) {
        case NUMERICOID:
            writeTupleDatum(slot, tuple_colid, makeNumericZero());
            break;

        case FLOAT8OID:
            writeTupleDatum(slot, tuple_colid, Float8GetDatum(0));
            break;

        case FLOAT4OID:
            writeTupleDatum(slot, tuple_colid, Float4GetDatum(0));
            break;

        case INT8OID:
            writeTupleDatum(slot, tuple_colid, Int64GetDatum(0));
            break;

        case INT4OID:
            writeTupleDatum(slot, tuple_colid, Int32GetDatum(0));
            break;

        case INT2OID:
            writeTupleDatum(slot, tuple_colid, Int16GetDatum(0));
            break;

        default:
            MOT_LOG_PANIC("Unexpected zero type while resetting tuple datum: %d", zero_type)
            break;
    }
}

Datum readTupleDatum(TupleTableSlot* slot, int tuple_colid, int arg_pos)
{
    MOT_LOG_DEBUG("Reading datum from tuple column %d ", tuple_colid);
    Datum result = slot->tts_values[tuple_colid];
    DBG_PRINT_DATUM("Pre-sum Tuple Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid]->atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
    bool isnull = (result == PointerGetDatum(NULL));
    setExprArgIsNull(arg_pos, (int)isnull);
    return result;
}

void writeTupleDatum(TupleTableSlot* slot, int tuple_colid, Datum datum)
{
    MOT_LOG_DEBUG("Writing datum to tuple column %d ", tuple_colid);
    bool isnull = (datum == PointerGetDatum(NULL));
    slot->tts_values[tuple_colid] = datum;
    slot->tts_isnull[tuple_colid] = isnull;
    DBG_PRINT_DATUM("Post-sum Tuple Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid]->atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
}

Datum SelectSubQueryResult(int subQueryIndex)
{
    JitExec::JitContext* jitContext = u_sess->mot_cxt.jit_context;
    return readTupleDatum(jitContext->m_subQueryData[subQueryIndex].m_slot, 0, 0);
}

void CopyAggregateToSubQueryResult(int subQueryIndex)
{
    MOT_LOG_DEBUG("Copying aggregate datum to sub-query %d slot", subQueryIndex);
    JitExec::JitContext* jitContext = u_sess->mot_cxt.jit_context;
    writeTupleDatum(jitContext->m_subQueryData[subQueryIndex].m_slot, 0, jitContext->m_aggValue);
}

TupleTableSlot* GetSubQuerySlot(int subQueryIndex)
{
    return u_sess->mot_cxt.jit_context->m_subQueryData[subQueryIndex].m_slot;
}

MOT::Table* GetSubQueryTable(int subQueryIndex)
{
    return u_sess->mot_cxt.jit_context->m_subQueryData[subQueryIndex].m_table;
}

MOT::Index* GetSubQueryIndex(int subQueryIndex)
{
    return u_sess->mot_cxt.jit_context->m_subQueryData[subQueryIndex].m_index;
}

MOT::Key* GetSubQuerySearchKey(int subQueryIndex)
{
    return u_sess->mot_cxt.jit_context->m_subQueryData[subQueryIndex].m_searchKey;
}

MOT::Key* GetSubQueryEndIteratorKey(int subQueryIndex)
{
    return u_sess->mot_cxt.jit_context->m_subQueryData[subQueryIndex].m_endIteratorKey;
}
}  // extern "C"
