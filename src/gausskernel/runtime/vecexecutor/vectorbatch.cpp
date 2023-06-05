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
 * ---------------------------------------------------------------------------------------
 *
 * vectorbatch.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vectorbatch.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecvar.h"
#include "vecexecutor/vectorbatch.inl"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "access/sysattr.h"
#include "lz4.h"
#include "executor/instrument.h"
#include "optimizer/streamplan.h"

ScalarVector::ScalarVector() : m_rows(0), m_const(false), m_flag(NULL), m_buf(NULL), m_vals(NULL)
{
    m_addVar = NULL;
}

ScalarVector::~ScalarVector()
{
    m_flag = NULL;
    m_buf = NULL;
    m_vals = NULL;
}

void ScalarVector::init(MemoryContext cxt, ScalarDesc desc)
{
    m_desc = desc;
    MemoryContext oldCxt = MemoryContextSwitchTo(cxt);

    m_flag = (uint8*)palloc0(sizeof(uint8) * BatchMaxSize);
    m_vals = (ScalarValue*)palloc(sizeof(ScalarValue) * BatchMaxSize);

    MemoryContextSwitchTo(oldCxt);

    m_buf = New(cxt) VarBuf(cxt);

    BindingFp();
}

void ScalarVector::init(MemoryContext cxt, ScalarVector *vec, const int batchSize)
{
    m_desc = vec->m_desc;
    m_rows = vec->m_rows;
    m_buf = vec->m_buf;

    MemoryContext oldCxt = MemoryContextSwitchTo(cxt);
    m_flag = (uint8*)palloc0(sizeof(uint8) * batchSize);
    m_vals = (ScalarValue*)palloc(sizeof(ScalarValue) * batchSize);
    MemoryContextSwitchTo(oldCxt);

    BindingFp();
}

void ScalarVector::Serialize(StringInfo buf, int idx)
{
    pq_sendint8(buf, m_flag[idx]);

    if (NOT_NULL(m_flag[idx])) {
        Datum val = m_vals[idx];
        if (m_desc.encoded) {
            Size dataLen = 0;
            if (m_desc.typeId == NAMEOID) {
                dataLen = datumGetSize(val, false, -2);
            } else {
                dataLen = VARSIZE_ANY(val);
            }
            /* varLen must be valid */
            Assert(dataLen > 0);
            Assert(AllocSizeIsValid(dataLen));
            appendBinaryStringInfo(buf, DatumGetPointer(val), dataLen);
        } else
            pq_sendbytes(buf, (char*)(&val), sizeof(ScalarValue));
    }
}

void ScalarVector::Serialize(StringInfo buf)
{
    pq_sendbytes(buf, (char*)this, sizeof(ScalarVector));
    pq_sendbytes(buf, (char*)m_flag, sizeof(uint8) * m_rows);

    if (m_desc.encoded) {
        Datum val = (Datum)0;
        Size dataLen = 0;
        for (int i = 0; i < m_rows; i++) {
            if (IsNull(i))
                continue;

            val = Decode(m_vals[i]);
            if (m_desc.typeId == NAMEOID) {
                dataLen = datumGetSize(val, false, -2);
            } else {
                dataLen = VARSIZE_ANY(val);
            }
            /* varLen must be valid */
            Assert(dataLen > 0);
            Assert(AllocSizeIsValid(dataLen));
            appendBinaryStringInfo(buf, DatumGetPointer(val), dataLen);
        }
    } else
        pq_sendbytes(buf, (char*)m_vals, sizeof(ScalarValue) * m_rows);
}

char* ScalarVector::Deserialize(char* msg, size_t len)
{
    VarBuf* buf = m_buf;
    uint8* flag = m_flag;
    ScalarValue* vals = m_vals;
    errno_t rc = EOK;

    rc = memcpy_s(this, sizeof(ScalarVector), msg, sizeof(ScalarVector));
    securec_check(rc, "", "");

    // restore the pointer
    m_buf = buf;
    m_flag = flag;
    m_vals = vals;

    msg += sizeof(ScalarVector);
    rc = memcpy_s(m_flag, sizeof(uint8) * BatchMaxSize, msg, sizeof(uint8) * m_rows);
    securec_check(rc, "\0", "\0");
    msg += sizeof(uint8) * m_rows;

    if (m_desc.encoded) {
        int var_len = 0;
        for (int i = 0; i < m_rows; i++) {
            if (IsNull(i))
                continue;
            if (m_desc.typeId == NAMEOID) {
                var_len = datumGetSize(PointerGetDatum(msg), false, -2);
            } else {
                var_len = VARSIZE_ANY(msg);
            }
            /* var_len must be valid */
            Assert(var_len > 0);
            Assert(AllocSizeIsValid(var_len));
            AddHeaderVar(PointerGetDatum(msg), i);
            msg += var_len;
        }
    } else {
        rc = memcpy_s(m_vals, sizeof(ScalarValue) * BatchMaxSize, msg, sizeof(ScalarValue) * m_rows);
        securec_check(rc, "\0", "\0");
        msg += sizeof(ScalarValue) * m_rows;
    }

    return msg;
}

void VectorBatch::init(MemoryContext cxt, ScalarDesc* desc, int ncols)
{
    m_cols = ncols;

    MemoryContext old_cxt = MemoryContextSwitchTo(cxt);
    m_sel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
    (void)MemoryContextSwitchTo(old_cxt);

    for (int i = 0; i < BatchMaxSize; i++) {
        m_sel[i] = true;
    }

    m_arr = New(cxt) ScalarVector[m_cols];

    for (int i = 0; i < m_cols; i++)
        m_arr[i].init(cxt, desc[i]);
}

void VectorBatch::init(MemoryContext cxt, TupleDesc desc)
{
    ScalarDesc scalar_desc;

    FormData_pg_attribute* attrs = desc->attrs;

    m_cols = desc->natts;

    MemoryContext old_cxt = MemoryContextSwitchTo(cxt);
    m_sel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
    (void)MemoryContextSwitchTo(old_cxt);

    for (int i = 0; i < BatchMaxSize; i++) {
        m_sel[i] = true;
    }

    m_arr = New(cxt) ScalarVector[m_cols];
    for (int i = 0; i < m_cols; i++) {
        scalar_desc.encoded = COL_IS_ENCODE(attrs[i].atttypid);
        scalar_desc.typeId = attrs[i].atttypid;
        /* for vector result batch, treat tid as int8 */
        if (scalar_desc.typeId == TIDOID) {
            scalar_desc.typeId = INT8OID;
        }
        scalar_desc.typeMod = attrs[i].atttypmod;
        m_arr[i].init(cxt, scalar_desc);
    }
}

void VectorBatch::init(MemoryContext cxt, VectorBatch* batch)
{
    m_cols = batch->m_cols;

    MemoryContext old_cxt = MemoryContextSwitchTo(cxt);
    m_sel = (bool*)palloc(sizeof(bool) * BatchMaxSize);
    (void)MemoryContextSwitchTo(old_cxt);

    for (int i = 0; i < BatchMaxSize; i++) {
        m_sel[i] = true;
    }

    m_arr = New(cxt) ScalarVector[m_cols];
    for (int i = 0; i < m_cols; i++) {
        m_arr[i].init(cxt, batch->m_arr[i].m_desc);
    }
}

bool VectorBatch::IsValid()
{
    if (m_cols <= 0)
        return false;

    if (m_rows > BatchMaxSize)
        return false;

    for (int i = 0; i < m_cols; i++) {
        if (m_arr[i].m_rows != m_rows && m_arr[i].m_rows != BatchMaxSize)
            return false;
    }

    return true;
}

void VectorBatch::FixRowCount()
{
    int values = m_rows;

    for (int i = 0; i < m_cols; i++) {
        m_arr[i].m_rows = values;
    }
}

void VectorBatch::FixRowCount(int rows)
{
    m_rows = rows;

    for (int i = 0; i < m_cols; i++) {
        m_arr[i].m_rows = rows;
    }
}

VectorBatch::VectorBatch(MemoryContext cxt, ScalarDesc* desc, int ncols)
    : m_rows(0), m_cols(0), m_checkSel(false), m_sel(NULL), m_arr(NULL), m_sysColumns(NULL), m_pCompressBuf(NULL)
{
    init(cxt, desc, ncols);
}

VectorBatch::VectorBatch(MemoryContext cxt, TupleDesc desc)
    : m_rows(0), m_cols(0), m_checkSel(false), m_sel(NULL), m_arr(NULL), m_sysColumns(NULL), m_pCompressBuf(NULL)
{
    init(cxt, desc);
}

VectorBatch::VectorBatch(MemoryContext cxt, VectorBatch* batch)
    : m_rows(0), m_cols(0), m_checkSel(false), m_sel(NULL), m_arr(NULL), m_sysColumns(NULL), m_pCompressBuf(NULL)
{
    init(cxt, batch);
}

VectorBatch::~VectorBatch()
{
    m_sel = NULL;
    m_arr = NULL;
    m_sysColumns = NULL;
    m_pCompressBuf = NULL;
}

void VectorBatch::Serialize(StringInfo buf, int idx)
{
    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    for (int i = 0; i < m_cols; i++) {
        m_arr[i].Serialize(buf, idx);
    }
    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::Deserialize(char* msg)
{
    int n_row = m_rows;
    errno_t rc = 0;

    NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    for (int i = 0; i < m_cols; i++) {
        ScalarVector* column = &m_arr[i];
        column->m_flag[n_row] = ((uint8*)msg)[0];
        msg += 1;

        if (NOT_NULL(column->m_flag[n_row])) {
            if (column->m_desc.encoded) {
                int var_len = VARSIZE_ANY(msg);
                /* var_len must be valid */
                Assert(var_len > 0);
                Assert(AllocSizeIsValid(var_len));
                column->AddHeaderVar(PointerGetDatum(msg), n_row);
                msg += var_len;
            } else {
                rc = memcpy_s(&column->m_vals[n_row], sizeof(ScalarValue), msg, sizeof(ScalarValue));
                securec_check(rc, "\0", "\0");
                msg += sizeof(ScalarValue);
            }
        }
        column->m_rows++;
    }
    m_rows++;
    Assert(IsValid());

    NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::SerializeWithoutCompress(StringInfo buf)
{
    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);

    FixRowCount();
    Assert(IsValid());

    pq_sendint(buf, m_rows, 4);
    pq_sendint(buf, m_cols, 4);

    for (int i = 0; i < m_cols; i++) {
        m_arr[i].Serialize(buf);
    }

    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::DeserializeWithoutDecompress(char* msg, size_t msg_len)
{
    char* omsg = msg;
    int cols = 0;
    errno_t rc = EOK;

    NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);

    Reset();

    rc = memcpy_s(&m_rows, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    m_rows = ntohl(m_rows);
    msg += 4;

    rc = memcpy_s(&cols, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    cols = ntohl(cols);
    msg += 4;

    // We assume the batch pre-allocated, so columns shall match
    //
    Assert(cols == m_cols);

    m_checkSel = false;
    for (int i = 0; i < m_cols; i++) {
        msg = m_arr[i].Deserialize(msg, msg_len - (msg - omsg));
    }

    Assert(IsValid());

    NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::SerializeWithLZ4Compress(StringInfo buf)
{
    int* p_len = NULL;
    char* p_dst = NULL;

    StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);

    FixRowCount();
    Assert(IsValid());

    if (m_pCompressBuf)
        resetStringInfo(m_pCompressBuf);
    else
        m_pCompressBuf = makeStringInfo();

    pq_sendint32(buf, m_rows);
    pq_sendint32(buf, m_cols);

    // Serialize per vector data.
    //
    for (int i = 0; i < m_cols; ++i) {
        m_arr[i].Serialize(m_pCompressBuf);
    }

    // Send original length.
    //
    pq_sendint32(buf, m_pCompressBuf->len);

    enlargeStringInfo(buf, LZ4_COMPRESSBOUND(m_pCompressBuf->len) + 4);
    p_len = (int*)(buf->data + buf->len);
    p_dst = buf->data + buf->len + 4;

    // buf
    //			|  -  -  -  -  | 4 bytes,  m_rows
    //			|  -  -  -  -  | 4 bytes,  m_cols
    //			|  -  -  -  -  | 4 bytes,  m_pCompressBuf->len
    //	p_len ->	|  -  -  -  -  | 4 bytes,  length of compressed data
    //	p_dst ->	|  -  -  -  - ...... |  compressed data
    // Write length before compressed data.
    //
    *p_len =
        LZ4_compress_default(m_pCompressBuf->data, p_dst, m_pCompressBuf->len, LZ4_compressBound(m_pCompressBuf->len));
    validate_LZ4_compress_result(*p_len, MOD_VEC_EXECUTOR, "vector batch serialize");
    buf->len += (*p_len + 4);

    // send as network byte order
    //
    *p_len = htonl(*p_len);

    StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::DeserializeWithLZ4Decompress(char* msg, size_t msg_len)
{
    int c_rows = 0;
    int c_columns = 0;
#ifdef USE_ASSERT_CHECKING
    char* omsg = msg;
#endif
    int o_len = 0;
    int c_len = 0;
    char* p_msg = NULL;
    char* p_o_msg = NULL;
    errno_t rc = EOK;
    int return_len = 0;

    NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    Reset(true);

    rc = memcpy_s(&c_rows, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    m_rows = ntohl(c_rows);
    msg += 4;

    rc = memcpy_s(&c_columns, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    c_columns = ntohl(c_columns);
    msg += 4;

    rc = memcpy_s(&o_len, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    o_len = ntohl(o_len);
    msg += 4;

    rc = memcpy_s(&c_len, sizeof(int), msg, 4);
    securec_check(rc, "\0", "\0");
    c_len = ntohl(c_len);
    msg += 4;

    if (m_pCompressBuf)
        resetStringInfo(m_pCompressBuf);
    else
        m_pCompressBuf = makeStringInfo();

    enlargeStringInfo(m_pCompressBuf, o_len);
    p_o_msg = p_msg = m_pCompressBuf->data;
    return_len = LZ4_decompress_safe(msg, p_msg, c_len, o_len);
    Assert(return_len == o_len);

    if (return_len != o_len) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("LZ4 decompress failed when deserializing message, return %d, "
                       "compressed length %d, original length %d",
                return_len,
                c_len,
                o_len)));
    }

    msg += c_len;

    for (int i = 0; i < c_columns; i++) {
        p_msg = m_arr[i].Deserialize(p_msg, o_len - (p_msg - p_o_msg));
    }

    Assert(msg - omsg <= (int)msg_len);
    Assert(IsValid());

    NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

void VectorBatch::Reset(bool reset_flag)
{
    errno_t rc;
    m_rows = 0;
    for (int i = 0; i < m_cols; i++) {
        m_arr[i].m_rows = 0;
        if (m_arr[i].m_buf != NULL)
            m_arr[i].m_buf->Reset();

        if (reset_flag) {
            rc = memset_s(m_arr[i].m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
            securec_check(rc, "\0", "\0");
        }
    }
}

void VectorBatch::ResetSelection(bool value)
{
    bool* p_selection = NULL;
    int i = 0;

    p_selection = m_sel;

    for (i = 0; i < BatchMaxSize; i++)
        p_selection[i] = value;
}

/*
 * @Description	: Optimize Pack batch, move specific column data that we want, since there
 *				  are unnecessarily operations that all column data will be moved.
 * @in sel		: flag which row data need to move.
 * @in copy_vars 	: flag which column data need to move.
 */
void VectorBatch::OptimizePack(const bool* sel, List* copy_vars)
{
    if (m_sysColumns == NULL)
        OptimizePackT<true, false>(sel, copy_vars);
    else
        OptimizePackT<true, true>(sel, copy_vars);
}

/*
 * @Description	: Optimize Pack batch, move specific column data that we want, since there
 *				  are unnecessarily operations that all column data will be moved in late read
                  situation. In the late read situation, the columns which are needed to move
                  include: late_vars columns and ctid columns
 * @in sel		: flag which row data need to move.
 * @in late_vars 	: flag which column data need to move in late read.
 * @in ctid_col_idx	: flag which ctid column data.
 */
void VectorBatch::OptimizePackForLateRead(const bool* sel, List* late_vars, int ctid_col_idx)
{
    Assert(ctid_col_idx >= 0 && ctid_col_idx < this->m_cols);
    if (m_sysColumns == NULL)
        OptimizePackTForLateRead<true, false>(sel, late_vars, ctid_col_idx);
    else
        OptimizePackTForLateRead<true, true>(sel, late_vars, ctid_col_idx);
}

void VectorBatch::Pack(const bool* sel)
{
    if (m_sysColumns == NULL)
        PackT<true, false>(sel);
    else
        PackT<true, true>(sel);
}

void VectorBatch::CreateSysColContainer(MemoryContext cxt, List* sys_var_list)
{
    ListCell* c = NULL;
    int col_index = 0;
    int sys_index;
    int num_sys_var = list_length(sys_var_list);
    m_sysColumns = (SysColContainer*)palloc(sizeof(SysColContainer));
    m_sysColumns->sysColumns = num_sys_var;
    m_sysColumns->m_ppColumns = New(cxt) ScalarVector[num_sys_var];

    ScalarDesc desc;
    desc.encoded = false;

    for (int i = 0; i < num_sys_var; i++) {
        m_sysColumns->m_ppColumns[i].init(cxt, desc);
    }

    foreach (c, sys_var_list) {
        sys_index = lfirst_int(c);
        Assert(sys_index < 0 && sys_index >= XC_NodeIdAttributeNumber);
        m_sysColumns->sysColumpMap[-sys_index] = col_index;

        switch (sys_index) {
            case SelfItemPointerAttributeNumber:
                m_sysColumns->m_ppColumns[m_sysColumns->sysColumpMap[-sys_index]].m_desc.typeId = INT8OID;
                break;
            case XC_NodeIdAttributeNumber:
                m_sysColumns->m_ppColumns[m_sysColumns->sysColumpMap[-sys_index]].m_desc.typeId = INT8OID;
                break;
            case TableOidAttributeNumber:
                m_sysColumns->m_ppColumns[m_sysColumns->sysColumpMap[-sys_index]].m_desc.typeId = OIDOID;
                break;
            case MinTransactionIdAttributeNumber:
                m_sysColumns->m_ppColumns[m_sysColumns->sysColumpMap[-sys_index]].m_desc.typeId = INT8OID;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Column store don't support this system column")));
        }

        col_index++;
    }
}

ScalarVector* VectorBatch::GetSysVector(int sys_col_idx)
{
    Assert(m_sysColumns != NULL);
    return &m_sysColumns->m_ppColumns[m_sysColumns->sysColumpMap[-sys_col_idx]];
}

int VectorBatch::GetSysColumnNum()
{
    return m_sysColumns ? m_sysColumns->sysColumns : 0;
}

void VectorBatch::CopyNth(VectorBatch* batch, int nth)
{
    for (int i = 0; i < m_cols; i++) {
        m_arr[i].copyNth(&batch->m_arr[i], nth);
    }

    m_rows++;
}

Datum ScalarVector::AddVar(Datum data, int aindex)
{
    return (this->*m_addVar)(data, aindex);
}

Datum ScalarVector::AddBPCharWithoutHeader(const char* data, int max_len, int len, int aindex)
{
    Assert(max_len >= len);
    BpChar* bpchar = (BpChar*)m_buf->Allocate(max_len + VARHDRSZ);
    SET_VARSIZE(bpchar, max_len + VARHDRSZ);
    char* dest = VARDATA(bpchar);
    errno_t errorno = EOK;
    errorno = memcpy_s(dest, max_len, data, len);
    securec_check(errorno, "\0", "\0");

    /* blank pad the string if necessary */
    if (max_len > len) {
        errorno = memset_s(dest + len, max_len - len, ' ', max_len - len);
        securec_check(errorno, "\0", "\0");
    }

    m_vals[aindex] = PointerGetDatum(bpchar);
    return m_vals[aindex];
}

Datum ScalarVector::AddVarCharWithoutHeader(const char* data, int len, int aindex)
{
    text* varchar = (text*)m_buf->Allocate(len + VARHDRSZ);
    SET_VARSIZE(varchar, len + VARHDRSZ);
    char* dest = VARDATA(varchar);
    if (len > 0) {
        errno_t errorno = EOK;
        errorno = memcpy_s(dest, len, data, len);
        securec_check(errorno, "\0", "\0");
    }
    m_vals[aindex] = PointerGetDatum((VarChar*)varchar);
    return m_vals[aindex];
}

Datum ScalarVector::AddShortNumericWithoutHeader(int64 value, uint8 scale, int aindex)
{
    Numeric numeric_ptr = (Numeric)m_buf->Allocate(NUMERIC_64SZ);
    SET_VARSIZE(numeric_ptr, NUMERIC_64SZ);
    numeric_ptr->choice.n_header = NUMERIC_64 + scale;
    *((int64*)(numeric_ptr->choice.n_bi.n_data)) = value;
    m_vals[aindex] = PointerGetDatum(numeric_ptr);
    return m_vals[aindex];
}

Datum ScalarVector::AddBigNumericWithoutHeader(int128 value, uint8 scale, int aindex)
{
    Numeric result = (Numeric)m_buf->Allocate(NUMERIC_128SZ);
    SET_VARSIZE(result, NUMERIC_128SZ);
    result->choice.n_header = NUMERIC_128 + scale;
    *((int128*)(result->choice.n_bi.n_data)) = value;
    m_vals[aindex] = PointerGetDatum(result);
    return m_vals[aindex];
}

// Copy from source with length bytes to vector buffer
//
char* ScalarVector::AddVars(const char* src, int length)
{
    return m_buf->Append(src, length);
}

Datum ScalarVector::AddVarWithHeader(Datum data)
{
    int typlen = 0;
    if (m_desc.typeId == NAMEOID) {
        typlen = datumGetSize(data, false, -2);
    } else {
        typlen = VARSIZE_ANY(data);
    }
    Datum val = PointerGetDatum(m_buf->Append(DatumGetPointer(data), typlen));
    return val;
}

Datum ScalarVector::DatumCstringToScalar(Datum data, Size len)
{
    char* src_ptr = NULL;
    int var_len;
    src_ptr = DatumGetPointer(data);
    char* result = NULL;

    if ((len + VARHDRSZ_SHORT) < VARATT_SHORT_MAX) {
        var_len = len + VARHDRSZ_SHORT + 1;
        result = (char*)palloc(var_len);
        SET_VARSIZE_SHORT(result, var_len);
    } else {
        var_len = len + VARHDRSZ + 1;
        result = (char*)palloc(var_len);
        SET_VARSIZE(result, var_len);
    }
    if (len > 0) {
        errno_t errorno = EOK;
        errorno = memcpy_s(VARDATA_ANY(result), len + 1, src_ptr, len + 1);
        securec_check(errorno, "\0", "\0");
    }

    Datum val = PointerGetDatum(result);
    return val;
}

Datum ScalarVector::DatumFixLenToScalar(Datum data, Size len)
{
    char* src_ptr = NULL;
    int var_len;
    src_ptr = DatumGetPointer(data);
    char* result = NULL;
    int errorno = 0;

    var_len = len + VARHDRSZ_SHORT;
    result = (char*)palloc(var_len);
    SET_VARSIZE_SHORT(result, var_len);

    errorno = memcpy_s(VARDATA_ANY(result), len, src_ptr, len);
    securec_check(errorno, "\0", "\0");

    Datum val = PointerGetDatum(result);
    return val;
}

void ScalarVector::copy(ScalarVector* vector)
{
    errno_t rc;
    Assert(vector != NULL);
    m_rows = vector->m_rows;
    m_desc = vector->m_desc;
    rc = memcpy_s(m_flag, BatchMaxSize, vector->m_flag, BatchMaxSize);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(m_vals, BatchMaxSize * sizeof(ScalarValue), vector->m_vals, BatchMaxSize * sizeof(ScalarValue));
    securec_check(rc, "\0", "\0");
}

/*
 * @Description: Shallow copy batch  with specific rows
 * @in vector - current column to be copyed.
 * @in start_idx - start index at current vector
 * @in end_idx - end index at  current vector
 * @return - void
 */
void ScalarVector::copy(ScalarVector* vector, int start_idx, int end_idx)
{
    errno_t rc;
    int copy_rows;
    Assert(vector != NULL);

    copy_rows = end_idx - start_idx;
    m_desc = vector->m_desc;
    rc = memcpy_s(&m_flag[m_rows], BatchMaxSize - m_rows, &vector->m_flag[start_idx], copy_rows);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(
        &m_vals[m_rows], copy_rows * sizeof(ScalarValue), &vector->m_vals[start_idx], copy_rows * sizeof(ScalarValue));
    securec_check(rc, "\0", "\0");

    m_rows += copy_rows;
}

/*
 * @Description: Deep copy batch with specific rows
 * @in vector - current column to be copyed.
 * @in start_idx - start index at current vector
 * @in end_idx - end index at  current vector
 * @return - void
 */
void ScalarVector::copyDeep(ScalarVector* vector, int start_idx, int end_idx)
{
    errno_t rc;
    Assert(vector != NULL);
    int j = m_rows; /* rows index */
    int copy_rows;  /* the number of rows to be copyed */
    m_desc = vector->m_desc;
    ScalarValue* vals = vector->m_vals;

    copy_rows = end_idx - start_idx;
    rc = memcpy_s(&m_flag[m_rows], BatchMaxSize - m_rows, &vector->m_flag[start_idx], copy_rows);
    securec_check(rc, "\0", "\0");
    if (m_desc.encoded) {
        for (int i = start_idx; i < end_idx; i++) {
            if (NOT_NULL(m_flag[j])) {
                m_vals[j] = AddVarWithHeader(Decode(vals[i]));
            }
            j++;
        }
    } else {
        rc = memcpy_s(&m_vals[m_rows],
            (BatchMaxSize - m_rows) * sizeof(ScalarValue),
            &vector->m_vals[start_idx],
            copy_rows * sizeof(ScalarValue));
        securec_check(rc, "\0", "\0");
    }

    m_rows += copy_rows;
}

void ScalarVector::copyFlag(ScalarVector* vector, int start_idx, int end_idx)
{
    errno_t rc;
    Assert(vector != NULL);
    int copy_rows;  /* the number of rows to be copyed */

    copy_rows = end_idx - start_idx;
    rc = memcpy_s(&m_flag[m_rows], BatchMaxSize - m_rows, &vector->m_flag[start_idx], copy_rows);
    securec_check(rc, "\0", "\0");
}

void ScalarVector::copyNth(ScalarVector* vector, int nth)
{
    Assert(vector != NULL);
    m_desc = vector->m_desc;
    ScalarValue* vals = vector->m_vals;

    m_flag[m_rows] = vector->m_flag[nth];

    if (NOT_NULL(m_flag[m_rows])) {
        if (m_desc.encoded)

            m_vals[m_rows] = AddVarWithHeader(Decode(vals[nth]));
        else
            m_vals[m_rows] = vector->m_vals[nth];
    }

    m_rows++;
}

void ScalarVector::copy(ScalarVector* vector, const bool* selection)
{
    int i;

    Assert(vector != NULL);
    Assert(selection != NULL);

    /*
     * If m_rows is 0, copy vector->m_rows to m_rows.
     * Else, m_rows is the minimum value of m_rows and vector->m_rows.
     * For example: m_rows = 500, vector->m_rows = 1000, final m_rows is 500.
     * m_rows = 1000, vector->m_rows = 500, final m_rows is 500.
     */
    if (m_rows == 0) {
        m_rows = vector->m_rows;
    } else {
        m_rows = Min(m_rows, vector->m_rows);
    }

    for (i = 0; i < m_rows; i++) {
        if (selection[i]) {
            m_flag[i] = vector->m_flag[i];
            m_vals[i] = vector->m_vals[i];
        }
    }
}

ScalarValue ScalarVector::DatumToScalar(Datum datum_val, Oid datum_type, bool is_null)
{
    ScalarValue val = 0;
    Size datum_len; /* length of the datum */

    DBG_ASSERT(datum_type != InvalidOid);

    if (!is_null) {
        if (COL_IS_ENCODE(datum_type)) {
            switch (datum_type) {
                case MACADDROID:
                    val = DatumFixLenToScalar(datum_val, 6);
                    break;
                case TIMETZOID:
                case TINTERVALOID:
                    val = DatumFixLenToScalar(datum_val, 12);
                    break;
                case INTERVALOID:
                case UUIDOID:
                    val = DatumFixLenToScalar(datum_val, 16);
                    break;
                case NAMEOID:
                    val = DatumFixLenToScalar(datum_val, 64);
                    break;
                case UNKNOWNOID:
                case CSTRINGOID:
                    datum_len = strlen((char*)datum_val);
                    val = DatumCstringToScalar(datum_val, datum_len);
                    break;
                /* numeric -> int64|int128|original numeric */
                case NUMERICOID:
                    /*
                     * Turn numeric data to big integer.
                     * numeric -> int64|int128|original numeric
                     */
                    val = try_convert_numeric_normal_to_fast(datum_val);
                    break;
                default:
                    val = datum_val;
                    break;
            }
        } else
            val = datum_val;
    }

    return val;
}

void ScalarVector::BindingFp()
{
    if (m_desc.encoded) {
        switch (m_desc.typeId) {
            case MACADDROID:
                m_addVar = &ScalarVector::AddFixLenVar<6>;
                break;
            case TIMETZOID:
            case TINTERVALOID:
                m_addVar = &ScalarVector::AddFixLenVar<12>;
                break;
            case INTERVALOID:
            case UUIDOID:
                m_addVar = &ScalarVector::AddFixLenVar<16>;
                break;
            case NAMEOID:
                m_addVar = &ScalarVector::AddFixLenVar<64>;
                break;
            case UNKNOWNOID:
            case CSTRINGOID:
                m_addVar = &ScalarVector::AddCStringVar;
                break;
            default:
                m_addVar = &ScalarVector::AddHeaderVar;
                break;
        }
    } else  // in case with no desc specify
        m_addVar = &ScalarVector::AddHeaderVar;
}

Datum ScalarVector::AddHeaderVar(Datum data, int aindex)
{
    m_vals[aindex] = AddVarWithHeader(data);
    return m_vals[aindex];
}

Datum ScalarVector::AddCStringVar(Datum data, int aindex)
{
    char* src_ptr = NULL;
    int var_len;
    src_ptr = DatumGetPointer(data);
    Size len = strlen(DatumGetPointer(data)) + 1;
    char* result = NULL;

    if ((len + VARHDRSZ_SHORT) < VARATT_SHORT_MAX) {
        var_len = len + VARHDRSZ_SHORT + 1;
        result = (char*)m_buf->Allocate(var_len);
        SET_VARSIZE_SHORT(result, var_len);
    } else {
        var_len = len + VARHDRSZ + 1;
        result = (char*)m_buf->Allocate(var_len);
        SET_VARSIZE(result, var_len);
    }
    errno_t errorno = EOK;
    if (len > 0) {
        errorno = memcpy_s(VARDATA_ANY(result), len + 1, src_ptr, len + 1);
        securec_check(errorno, "\0", "\0");
    }

    Datum val = PointerGetDatum(result);
    m_vals[aindex] = val;
    return val;
}

template <Size len>
Datum ScalarVector::AddFixLenVar(Datum data, int aindex)
{
    char* src_ptr = NULL;
    int var_len;
    src_ptr = DatumGetPointer(data);
    char* result = NULL;

    var_len = len + VARHDRSZ_SHORT;
    result = (char*)m_buf->Allocate(var_len);
    SET_VARSIZE_SHORT(result, var_len);
    errno_t errorno = memcpy_s(VARDATA_ANY(result), len, src_ptr, len);
    securec_check(errorno, "\0", "\0");

    Datum val = PointerGetDatum(result);
    m_vals[aindex] = val;
    return val;
}

/*
 * @Description : get actual data from scalar value
 * @in data	:  scalar data
 * @return		: the result after extract
 */
Datum ExtractVarType(Datum* data)
{
    return (*data);
}

/*
 * @Description : get ctid data from data
 * @in data	:   scalar value
 * @return		: the ctid data
 */
Datum ExtractAddrType(Datum* data)
{
    return PointerGetDatum(data);
}

/*
 * @Description : get the fix length data from varlena data
 * @in data	:  varlena data
 * @return		: the fix length data
 */
Datum ExtractFixedType(Datum* data)
{
    return PointerGetDatum((char*)(*data) + VARHDRSZ_SHORT);
}

/*
 * @Description : get cstring data from varlena data
 * @in data	:  varlena data
 * @return		:  cstring data
 */
Datum ExtractCstringType(Datum* data)
{
    Datum tmp;
    tmp = *data;
    if (VARATT_IS_1B(tmp))
        return PointerGetDatum((char*)tmp + VARHDRSZ_SHORT);
    else
        return PointerGetDatum((char*)tmp + VARHDRSZ);
}

VarBuf::VarBuf(MemoryContext context)
    : m_head(NULL), m_current(NULL), m_context(context), m_bufNum(0), m_bufInitLen(VAR_BUF_SIZE)
{}

VarBuf::~VarBuf()
{
    m_head = NULL;
    m_current = NULL;
    m_context = NULL;
}

void VarBuf::Init()
{
    m_head = CreateBuf(VAR_BUF_SIZE);
    m_current = m_head;
}

void VarBuf::Init(int bufLen)
{
    m_head = CreateBuf(bufLen);
    m_bufInitLen = Max(bufLen, VAR_BUF_SIZE);
    m_current = m_head;
}

void VarBuf::Reset()
{
    varBuf* buf = m_head;
    while (buf != NULL) {
        buf->len = 0;
        buf = buf->next;
    }
    m_current = m_head;
}

char* VarBuf::Allocate(int data_len)
{
    DBG_ASSERT(data_len > 0);
    if (m_current == NULL)
        Init();

    int align_data_len = MAXALIGN(data_len);
    if (m_current->len + align_data_len > m_current->size) {
        varBuf* buf = m_current->next;
        varBuf* last_buf = m_current;
        while (buf != NULL) {
            if (buf->len + align_data_len <= buf->size)
                break;

            last_buf = buf;
            buf = buf->next;
        }

        if (buf == NULL) {
            buf = CreateBuf(align_data_len);
            last_buf->next = buf;
        }

        m_current = buf;
    }

    char* addr = m_current->buf + m_current->len;
    m_current->len += align_data_len;

    return addr;
}

char* VarBuf::Append(const char* data, int data_len)
{
    char* addr = Allocate(data_len);
    errno_t rc;
    rc = memcpy_s(addr, data_len, data, data_len);
    securec_check(rc, "\0", "\0");

    return addr;
}

varBuf* VarBuf::CreateBuf(int data_len)
{
    varBuf* buf = NULL;

    AutoContextSwitch memGuard(m_context);
    buf = (varBuf*)palloc(sizeof(varBuf));
    buf->len = 0;
    buf->size = (data_len < m_bufInitLen) ? m_bufInitLen : data_len;
    buf->next = NULL;
    buf->buf = (char*)palloc(buf->size);
    m_bufNum++;

    return buf;
}

void VarBuf::DeInit(bool needfree)
{
    varBuf* buf = m_head;
    varBuf* last_buf = NULL;

    if (needfree) {
        while (buf != NULL) {
            buf->len = 0;
            last_buf = buf;
            buf = buf->next;
            pfree_ext(last_buf->buf);
            pfree_ext(last_buf);
        }
    }
    m_head = NULL;
    m_current = NULL;
    m_bufNum = 0;
    m_bufInitLen = VAR_BUF_SIZE;
}
