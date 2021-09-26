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
 * cu.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cu.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cstore_psort.h"
#include "utils/rel_gs.h"
#include "access/reloptions.h"
#include "access/tupmacs.h"
#include "knl/knl_instance.h"
#include "nodes/primnodes.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/cu.h"
#include "utils/gs_bitmap.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/pg_crc.h"
#include "port/pg_crc32c.h"
#include "utils/builtins.h"
#include "storage/time_series_compress.h"


int CUAlignUtils::GetCuAlignSizeColumnId(int columnId)
{
    Assert(columnId > 0);
    int align_size = ALIGNOF_CUSIZE;
    if (columnId >= TS_COLUMN_ID_BASE) {
        align_size = ALIGNOF_TIMESERIES_CUSIZE;
    }
    return align_size;
}

uint32 CUAlignUtils::AlignCuSize(int len, int align_size)
{
    return TYPEALIGN(((uint32)align_size), (len));
}

static inline int uint64_to_str(char* str, uint64 val)
{
    char stack[MAX_LEN_CHAR_TO_BIGINT_BUF] = {0};
    int idx = 0;
    int i = 0;
    Assert(str != NULL);
    Assert(val > 0);

    while (val > 0) {
        stack[idx] = '0' + (val % 10);
        val /= 10;
        idx++;
    }

    while (idx > 0) {
        str[i] = stack[idx - 1];
        i++;
        idx--;
    }
    return i;
}

FORCE_INLINE
void CUDesc::Reset()
{
    cu_id = InValidCUID;
    errno_t rc = 0;
    rc = memset_s(cu_min, MIN_MAX_LEN, 0, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
    rc = memset_s(cu_max, MIN_MAX_LEN, 0, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
    row_count = 0;
    cu_size = 0;
    cu_mode = 0;
    cu_pointer = 0;
    magic = 0;
    xmin = 0;
}

FORCE_INLINE
void CUDesc::SetNullCU()
{
    Assert(((uint32)cu_mode & CU_MODE_LOWMASK) == 0);
    cu_mode = (int)((uint32)cu_mode | CU_FULL_NULL);
}

FORCE_INLINE
void CUDesc::SetNormalCU()
{
    Assert(((uint32)cu_mode & CU_MODE_LOWMASK) == 0);
    cu_mode = (int)((uint32)cu_mode | CU_NORMAL);
}

FORCE_INLINE
void CUDesc::SetSameValCU()
{
    Assert(((uint32)cu_mode & CU_MODE_LOWMASK) == 0);
    cu_mode = (int)((uint32)cu_mode | CU_SAME_VAL);
}

FORCE_INLINE
void CUDesc::SetNoMinMaxCU()
{
    Assert(((uint32)cu_mode & CU_MODE_LOWMASK) == 0);
    cu_mode = (int)((uint32)cu_mode | CU_NO_MINMAX_CU);
}

FORCE_INLINE
void CUDesc::SetCUHasNull()
{
    Assert(((uint32)cu_mode & CU_MODE_LOWMASK) == 0);
    cu_mode = (int)((uint32)cu_mode | CU_HAS_NULL);
}

FORCE_INLINE
bool CUDesc::IsNullCU() const
{
    return ((((uint32)cu_mode & CU_MODE_LOWMASK) == CU_FULL_NULL) ? true : false);
}

FORCE_INLINE
bool CUDesc::IsNormalCU() const
{
    return ((((uint32)cu_mode & CU_MODE_LOWMASK) == CU_NORMAL) ? true : false);
}

FORCE_INLINE
bool CUDesc::IsSameValCU() const
{
    return ((((uint32)cu_mode & CU_MODE_LOWMASK) == CU_SAME_VAL) ? true : false);
}

FORCE_INLINE
bool CUDesc::IsNoMinMaxCU() const
{
    return ((((uint32)cu_mode & CU_MODE_LOWMASK) == CU_NO_MINMAX_CU) ? true : false);
}

FORCE_INLINE
bool CUDesc::CUHasNull() const
{
    return ((((uint32)cu_mode & CU_MODE_LOWMASK) == CU_HAS_NULL) ? true : false);
}

CUDesc::CUDesc()
{
    Reset();
}

CUDesc::~CUDesc()
{}

FORCE_INLINE
void CU::Reset()
{
    m_crc = 0;
    m_srcData = NULL;
    m_infoMode = 0;
    m_compressedBuf = NULL;
    m_compressedBufSize = 0;
    m_compressedLoadBuf = NULL;
    m_head_padding_size = 0;
    m_srcBuf = NULL;
    m_srcBufSize = 0;
    m_srcDataSize = 0;
    m_nulls = NULL;
    m_cuSize = 0;
    m_bpNullRawSize = 0;
    m_bpNullCompressedSize = 0;
    m_offset = NULL;
    m_offsetSize = 0;
    m_cuSizeExcludePadding = 0;

    m_tmpinfo = NULL;
    m_magic = 0;
    m_cache_compressed = false;
    m_adio_error = false;
    m_inCUCache = false;
    m_eachValSize = 0;
    m_typeMode = 0;
    m_atttypid = 0;
    m_numericIntLike = false;
}

void CU::SetTypeLen(int typeLen)
{
    m_eachValSize = typeLen;
}

void CU::SetTypeMode(int typeMode)
{
    m_typeMode = typeMode;
}

void CU::SetAttTypeId(uint32 atttypid)
{
    m_atttypid = atttypid;
}

void CU::SetAttInfo(int typeLen, int typeMode, uint32 atttypid)
{
    SetTypeLen(typeLen);
    SetTypeMode(typeMode);
    SetAttTypeId(atttypid);
}

CU::CU(int typeLen, int typeMode, uint32 atttypid)
{
    Reset();
    m_eachValSize = typeLen;
    m_typeMode = typeMode;
    m_atttypid = atttypid;
}

CU::CU()
{
    Reset();
}

CU::~CU()
{
    FreeSrcBuf();

    m_nulls = NULL;
    m_tmpinfo = NULL;
    m_compressedBuf = NULL;
    m_compressedLoadBuf = NULL;
    m_srcData = NULL;
}

void CU::Destroy()
{
    FreeMem<false>();
}

void CU::InitMem(uint32 initialSize, int rowCount, bool hasNull)
{
    if (hasNull) {
        m_infoMode |= CU_HasNULL;
        m_bpNullRawSize = bitmap_size(rowCount);
    }

    if (initialSize < UINT32_MAX - m_bpNullRawSize - 8) {
        m_srcBufSize = initialSize + m_bpNullRawSize;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("inititalSize is too large: initialSize(%u), m_bpNullRawSize(%u)",
                               initialSize, m_bpNullRawSize)));
    }
    Assert(m_srcBuf == NULL);
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
    m_srcBuf = (char*)CStoreMemAlloc::Palloc(m_srcBufSize + 8, !m_inCUCache);

    errno_t rc = memset_s(m_srcBuf, m_srcBufSize, 0, m_srcBufSize);
    securec_check(rc, "\0", "\0");

    // NULL value bitmap starting position
    if (hasNull) {
        m_nulls = (unsigned char*)m_srcBuf;
    }

    // Source data starting position
    m_srcData = m_srcBuf + m_bpNullRawSize;
}

void CU::ReallocMem(Size size)
{
    /// *m_srcBufSize* couldn't be 0, otherwise
    /// we cannot know whether *CU_HasNULL* is set right,
    /// or *m_nulls* is not NULL.
    Assert(m_srcBufSize > 0);

    // How many bytes of NULL value bitmap
    Assert(m_srcData >= m_srcBuf);
    uint32 nullBytes = m_srcData - m_srcBuf;
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
    m_srcBuf = (char*)CStoreMemAlloc::Repalloc(m_srcBuf, size + nullBytes + 8, m_srcBufSize, !m_inCUCache);
    m_srcBufSize = size + nullBytes;

    // NULL value bitmap starting position
    if ((m_infoMode & CU_HasNULL) != 0) {
        m_nulls = (unsigned char*)m_srcBuf;
    }

    // Source data starting position
    m_srcData = m_srcBuf + nullBytes;
}

FORCE_INLINE
bool CU::IsNull(uint32 row) const
{
    return (m_nulls && (m_nulls[row / 8] & (1 << (row % 8))));
}

// compute how many NULL values before the row record, including this row record
int CU::CountNullValuesBefore(int rows) const
{
    int nullNum = 0;
    uint32 nBytes = rows / 8;

    if (HasNullValue()) {
        uint32 i;

        for (i = 0; i < nBytes; ++i)
            nullNum += NumberOfBit1Set[m_nulls[i]];

        for (i = 0; i < (uint32)(rows % 8); ++i) {
            if (m_nulls[nBytes] & (1U << i))
                ++nullNum;
        }
    }
    return nullNum;
}

bool CU::CheckCrc()
{
    uint32 tmpCrc = *(uint32*)m_compressedBuf;
    uint16 tmpMode = *(uint16*)(m_compressedBuf + sizeof(m_crc) + sizeof(m_magic));

    // generate checksum
    m_crc = GenerateCrc(tmpMode);

    // EQ_CRC32C as same as EQ_CRC32
    bool isSame = EQ_CRC32C(m_crc, tmpCrc);
    if (unlikely(!isSame)) {
        ereport(WARNING,
                (ERRCODE_DATA_CORRUPTED,
                 errmsg("CU verification failed, calculated checksum %u but expected %u", m_crc, tmpCrc)));

        if (u_sess->attr.attr_common.ignore_checksum_failure)
            return true;
    }

    return isSame;
}

bool CU::CheckMagic(uint32 magic) const
{
    bool is_same = true;
    uint32 tmp_magic = m_magic;

    if (m_compressedBuf)
        tmp_magic = *(uint32*)(m_compressedBuf + sizeof(m_crc));

    is_same = (tmp_magic == magic);

    if (unlikely(!is_same)) {
        ereport(WARNING,
                (ERRCODE_DATA_CORRUPTED,
                 errmsg("CU magic verification failed, magic %u but expected %u", tmp_magic, magic)));
    }

    return is_same;
}

bool CU::IsVerified(uint32 magic)
{
    bool checksum_success = false;
    bool magic_success = false;

    // check checksum
    checksum_success = CheckCrc();
    if (!checksum_success)
        return false;

    // check magic
    magic_success = CheckMagic(magic);
    if (!magic_success)
        return false;

    return true;
}

// append a value into cu.
// repeat is used for cu with all the same value.
void CU::AppendCuData(_in_ Datum value, _in_ int repeat, _in_ Form_pg_attribute attr, __inout CU* cu)
{
    const int valSize = (int)datumGetSize(value, attr->attbyval, attr->attlen);

    if (attr->attlen > 0 && attr->attlen <= 8) {
        for (int cnt = 0; cnt < repeat; ++cnt) {
            cu->AppendValue(value, valSize);
        }
    } else {
        for (int cnt = 0; cnt < repeat; ++cnt) {
            cu->AppendValue(DatumGetPointer(value), valSize);
        }
    }
}

FORCE_INLINE
void CU::AppendNullValue(int row)
{
    Assert(m_nulls != NULL);
    m_nulls[row / 8] |= (1 << (uint32)(row % 8));
}

void CU::AppendValue(Datum val, int size)
{
    if (unlikely(m_srcData + m_srcDataSize + size > m_srcBuf + m_srcBufSize)) {
        // it's better to expand the memory step by step with 1MB.
        Assert(size >= 0);
        ReallocMem((Size)m_srcBufSize + (uint32)size + 1024 * 1024);
    }
    store_att_byval(m_srcData + m_srcDataSize, val, size);
    m_srcDataSize += size;
}

void CU::AppendValue(const char* val, int size)
{
    errno_t rc;
    if (unlikely(m_srcData + m_srcDataSize + size > m_srcBuf + m_srcBufSize)) {
        // it's better to expand the memory step by step with 1MB.
        Assert(size >= 0);
        ReallocMem((Size)m_srcBufSize + (uint32)size + 1024 * 1024);
    }

    rc = memcpy_s(m_srcData + m_srcDataSize, size, val, size);
    securec_check(rc, "\0", "\0");
    m_srcDataSize += size;
}

// it's the caller that defines the action of CRC computation.
// it's decided by the input argument *force*.
uint32 CU::GenerateCrc(uint16 info_mode) const
{
    bool isCRC32C = (CU_CRC32C == (info_mode & CU_CRC32C));

    ASSERT_CUSIZE(m_cuSize);

    uint32 tmpCrc = 0;

    if (likely(isCRC32C)) {
        // using CRC32C
        INIT_CRC32C(tmpCrc);
        COMP_CRC32C(tmpCrc, m_compressedBuf + sizeof(tmpCrc), m_cuSize - sizeof(tmpCrc));
        FIN_CRC32C(tmpCrc);

#ifdef USE_ASSERT_CHECKING
#if defined(USE_SSE42_CRC32C_WITH_RUNTIME_CHECK)
        // DEBUG mode will recheck sse42 result is same as sb8
        if (pg_comp_crc32c == pg_comp_crc32c_sse42) {
            uint32 sb8_crc32c = 0;
            INIT_CRC32C(sb8_crc32c);
            sb8_crc32c =
                pg_comp_crc32c_sb8(sb8_crc32c, m_compressedBuf + sizeof(sb8_crc32c), m_cuSize - sizeof(sb8_crc32c));
            FIN_CRC32C(sb8_crc32c);

            if (!EQ_CRC32C(tmpCrc, sb8_crc32c)) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("the CRC32C checksum are different between SSE42 (0x%x) and SB8 (0x%x).",
                                tmpCrc,
                                sb8_crc32c)));
            }
        }
#endif
#endif
    } else {
        // using PG's CRC32
        INIT_CRC32(tmpCrc);
        COMP_CRC32(tmpCrc, m_compressedBuf + sizeof(tmpCrc), m_cuSize - sizeof(tmpCrc));
        FIN_CRC32(tmpCrc);
    }
    return tmpCrc;
}

/*
 * @Description: compress one CU
 * @IN compress_modes: compressing modes
 * @IN valCount: values count
 * @See also:
 */
void CU::Compress(int valCount, int16 compress_modes, int align_size)
{
    errno_t rc;

    // Step 1: initialize allocate the size of compress_buffer
    // source data size + nulls bitmap size + header size
    // We guarantee that compress data size will not exceed it
    m_compressedBufSize = CUAlignUtils::AlignCuSize(m_srcDataSize + m_bpNullRawSize + sizeof(CU), align_size);
    m_compressedBuf = (char*)CStoreMemAlloc::Palloc(m_compressedBufSize, !m_inCUCache);

    int16 headerLen = GetCUHeaderSize();
    char* buf = m_compressedBuf + headerLen;

    // Step 2: fill Compress NULL bitmap
    buf = CompressNullBitmapIfNeed(buf);

    // Step 3: Compress data
    bool compressed = false;
    if (COMPRESS_NO != heaprel_get_compression_from_modes(compress_modes))
        compressed = CompressData(buf, valCount, compress_modes, align_size);

    // case 1: user defines that input data shouldn't be compressed.
    // case 2: even though user has defined to compress data, but the compressed data' size
    //        is bigger than the uncompressed data's.
    //   so use the raw data other than the compressed data.
    if (compressed == false) {
        rc = memcpy_s(buf, m_srcDataSize, m_srcData, m_srcDataSize);
        securec_check(rc, "\0", "\0");
        m_cuSizeExcludePadding = headerLen + m_bpNullCompressedSize + m_srcDataSize;
        m_cuSize = CUAlignUtils::AlignCuSize(m_cuSizeExcludePadding, align_size);
        PADDING_CU(buf + m_srcDataSize, m_cuSize - m_cuSizeExcludePadding);
    }

    // encrypt cu after compress
    CUDataEncrypt(buf);

    // Step 4: Fill compress_buffer header
    FillCompressBufHeader();

    m_cache_compressed = true;

    // Step 5; free source buffer
    FreeSrcBuf();
}

// 	  CompressBufHeader
// Note that if you change this function, you should sync to modify the following methods:
//    CU::FillCompressBufHeader()
//    CU::UnCompressHeader()
int16 CU::GetCUHeaderSize(void) const
{
    return sizeof(m_crc) +   // CRC
            sizeof(m_magic) +    // magic
            sizeof(m_infoMode) + // Info data
            // Nulls Bitmap if it's compressed
            (HasNullValue() ? sizeof(m_bpNullCompressedSize) : 0) + sizeof(m_srcDataSize) + // uncompressed data size
            sizeof(int);                                                                    // compressed data size
}

void CU::FillCompressBufHeader(void)
{
    errno_t rc;

    // m_crc will be set at the end of compress
    char* buf = m_compressedBuf;
    int pos = sizeof(m_crc);

    rc = memcpy_s(buf + pos, sizeof(m_magic), &m_magic, sizeof(m_magic));
    securec_check(rc, "\0", "\0");
    pos += sizeof(m_magic);
    m_infoMode |= CU_CRC32C;

    rc = memcpy_s(buf + pos, sizeof(m_infoMode), &m_infoMode, sizeof(m_infoMode));
    securec_check(rc, "\0", "\0");
    pos += sizeof(m_infoMode);

    if (HasNullValue()) {
        rc = memcpy_s(
                 buf + pos, sizeof(m_bpNullCompressedSize), &m_bpNullCompressedSize, sizeof(m_bpNullCompressedSize));
        securec_check(rc, "\0", "\0");
        pos += sizeof(m_bpNullCompressedSize);
    }

    rc = memcpy_s(buf + pos, sizeof(m_srcDataSize), &m_srcDataSize, sizeof(m_srcDataSize));
    securec_check(rc, "\0", "\0");
    pos += sizeof(m_srcDataSize);

    int cmprDataSize = m_cuSizeExcludePadding - GetCUHeaderSize() - m_bpNullCompressedSize;
    rc = memcpy_s(buf + pos, sizeof(cmprDataSize), &cmprDataSize, sizeof(cmprDataSize));
    securec_check(rc, "\0", "\0");
    pos += sizeof(cmprDataSize);
    Assert(pos == GetCUHeaderSize());

    // finally, compute CRC value
    m_crc = GenerateCrc(m_infoMode);
    *(uint32*)m_compressedBuf = m_crc;
}

// FUTURE CASE: null bitmap data should be compressed and decompressed
// Note that: both CompressNullBitmapIfNeed() && UnCompressNullBitmapIfNeed() should be modified together.
char* CU::CompressNullBitmapIfNeed(_in_ char* buf)
{
    errno_t rc;
    if (HasNullValue()) {
        Assert(m_bpNullRawSize > 0);
        // FUTURE CASE: compress NULL bitmap data later
        rc = memcpy_s(buf, m_bpNullRawSize, m_nulls, m_bpNullRawSize);
        securec_check(rc, "\0", "\0");
        m_bpNullCompressedSize = m_bpNullRawSize;
    }

    return (buf + m_bpNullCompressedSize);
}

/*
 * @Description: compress one CU data.
 * @IN compress_modes: compressing modes
 * @IN nVals: values' number
 * @OUT outBuf: output buffer
 * @Return:
 * @See also:
 */
bool CU::CompressData(_out_ char* outBuf, _in_ int nVals, _in_ int16 compress_modes, int align_size)
{
    int compressOutSize = 0;
    bool beDelta2Compressed = false;
    bool beXORCompressed = false;

    /* get compression value from compressing modes */
    int8 compression = heaprel_get_compression_from_modes(compress_modes);

    CompressionArg2 output = {0};
    output.buf = outBuf;
    output.sz = (m_compressedBuf + m_compressedBufSize) - outBuf;

    CompressionArg1 input = {0};
    input.sz = m_srcDataSize;
    input.buf = m_srcData;
    input.mode = compress_modes;

    /* set compression filter for this CU data */
    compression_options* ref_filter = (compression_options*)m_tmpinfo->m_options;

    if (g_instance.attr.attr_common.enable_tsdb && (ATT_IS_TIMESTAMP(m_atttypid) || ATT_IS_FLOAT(m_atttypid))) {
        SequenceCodec sequenceCoder(m_eachValSize, m_atttypid);
        compressOutSize = sequenceCoder.compress(input, output);
        if (ATT_IS_TIMESTAMP(m_atttypid)) {
            beDelta2Compressed = true;
        } else if (ATT_IS_FLOAT(m_atttypid)) {
            beXORCompressed = true;
        }
    }
    if (compressOutSize < 0 || (!beDelta2Compressed && !beXORCompressed)) {
        output = {0};
        output.buf = outBuf;
        output.sz = (m_compressedBuf + m_compressedBufSize) - outBuf;

        if (m_infoMode & CU_IntLikeCompressed) {
            if (ATT_IS_CHAR_TYPE(m_atttypid)) {
                IntegerCoder intCoder(8);

                /* set min/max value */
                if (m_tmpinfo->m_valid_minmax) {
                    intCoder.SetMinMaxVal(m_tmpinfo->m_min_value, m_tmpinfo->m_max_value);
                }
                /* input a hint about RLE encoding */
                intCoder.m_adopt_rle = ref_filter->m_adopt_rle;
                compressOutSize = intCoder.Compress(input, output);
            } else if (ATT_IS_NUMERIC_TYPE(m_atttypid)) {
                if (compression > COMPRESS_LOW) {
                    /// numeric data type compression.
                    /// lz4/zlib is used directly.
                    input.buildGlobalDict = false;
                    input.useGlobalDict = false;
                    input.globalDict = NULL;
                    input.useDict = false;
                    input.numVals = HasNullValue() ? (nVals - CountNullValuesBefore(nVals)) : nVals;

                    StringCoder strCoder;
                    compressOutSize = strCoder.Compress(input, output);
                }
            } else {
                // for future, another type
            }
        } else if (m_eachValSize > 0 && m_eachValSize <= 8) {
            IntegerCoder intCoder(m_eachValSize);
            /* set min/max value */
            if (m_tmpinfo->m_valid_minmax) {
                intCoder.SetMinMaxVal(m_tmpinfo->m_min_value, m_tmpinfo->m_max_value);
            }
            /* input a hint about RLE encoding */
            intCoder.m_adopt_rle = ref_filter->m_adopt_rle;
            compressOutSize = intCoder.Compress(input, output);
        } else {
            // FUTURE CASE: complete global dictionary
            Assert(-1 == m_eachValSize || m_eachValSize > 8);
            input.buildGlobalDict = false;
            input.useGlobalDict = false;
            input.globalDict = NULL;

            // for fixed-length datatype whose size is bigger than 8,
            // lz4/zlib method is applied to directly, excluding dictionary method.
            // for var-length datatype whose size is -1, dictionary method can be applied
            // to. so try it first.
            input.useDict = (m_eachValSize > 8) ? false : (COMPRESS_LOW != compression);

            // the number of values is excluding the number of NULL values.
            input.numVals = HasNullValue() ? (nVals - CountNullValuesBefore(nVals)) : nVals;

            // StringCoder.Compress
            StringCoder strCoder;
            /* input hints about both RLE and DICTIONARY encoding */
            strCoder.m_adopt_rle = ref_filter->m_adopt_rle;
            strCoder.m_adopt_dict = ref_filter->m_adopt_dict;
            compressOutSize = strCoder.Compress(input, output);
        }
    }

    if (compressOutSize > 0) {
        // compress successfully, compute CU size and set the compression info.
        Assert((uint32)compressOutSize < m_srcDataSize);
        Assert((0 == (output.modes & CU_INFOMASK2)) && (0 != (output.modes & CU_INFOMASK1)));
        m_infoMode |= (output.modes & CU_INFOMASK1);

        m_cuSizeExcludePadding = (outBuf - m_compressedBuf) + compressOutSize;
        m_cuSize = CUAlignUtils::AlignCuSize(m_cuSizeExcludePadding, align_size);
        Assert(m_cuSize <= m_compressedBufSize);
        PADDING_CU(m_compressedBuf + m_cuSizeExcludePadding, m_cuSize - m_cuSizeExcludePadding);

        if (!ref_filter->m_sampling_fihished) {
            /* sample and set adopted compression methods */
            ref_filter->set_common_flags(output.modes);
        }

        return true;
    }

    return false;
}

void CU::UnCompress(_in_ int rowCount, _in_ uint32 magic, int align_size)
{
    Assert(m_compressedBuf && m_compressedBufSize > 0);
    Assert(m_cuSize > 0);
    ASSERT_CUSIZE(m_cuSize);

    if (unlikely(!(m_compressedBuf && m_compressedBufSize > 0) || !(m_cuSize > 0))) {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("Find invalid compressed buffer or invalid CU while uncompressing.")));
    }

    // Step 1: UnCompress CU header
    char* buf = UnCompressHeader(magic, align_size);

    Assert((HasNullValue() && (m_bpNullCompressedSize <= bitmap_size(rowCount))) ||
           (!HasNullValue() && (m_bpNullCompressedSize == 0)));

    // Step 2: Allocate memory for uncompress
    //   because *m_srcDataSize* cannot be greater than 2G, so it's safe
    //   use uint32 here to plus ZLIB_EXTRA_SIZE.
    InitMem(m_srcDataSize + ZLIB_EXTRA_SIZE, rowCount, HasNullValue());

    // Step 3: UnComprss NULL bitmap if need
    buf = UnCompressNullBitmapIfNeed(buf, rowCount);

    // decrypt before UnCompress
    CUDataDecrypt(buf);

    // Step 4: UnCompress data
    UnCompressData(buf, rowCount);

    // Step 5: generate offset array to prepare for accessing randomly if need
    if (!HasNullValue()) {
        FormValuesOffset<false>(rowCount);
    } else {
        FormValuesOffset<true>(rowCount);
    }

    /*
     * This memory barrier prevents unordered write,
     * which may set complete flag before finishing uncompressing CU.
     */
#ifdef __aarch64__
    pg_memory_barrier();
#endif

    m_cache_compressed = false;
}

char* CU::UnCompressHeader(_in_ uint32 magic, int align_size)
{
    int pos = 0;

    m_crc = *(uint32*)(m_compressedBuf + pos);
    pos += sizeof(m_crc);

    m_magic = *(uint32*)(m_compressedBuf + pos);
    pos += sizeof(m_magic);

    Assert(magic == m_magic);

    if (magic != m_magic)
        ereport(PANIC,
                (errmsg(
                     "magic is not matched, maybe data has corrupted, cudesc magic(%u) stored magic(%u)", magic, m_magic)));

    m_infoMode = *(int16*)(m_compressedBuf + pos);
    pos += sizeof(m_infoMode);

    if (HasNullValue()) {
        m_bpNullCompressedSize = *(int16*)(m_compressedBuf + pos);
        pos += sizeof(m_bpNullCompressedSize);
    }

    m_srcDataSize = *(int32*)(m_compressedBuf + pos);
    pos += sizeof(m_srcDataSize);

    int cmprDataSize = *(int*)(m_compressedBuf + pos);
    pos += sizeof(cmprDataSize);

    Assert(pos == GetCUHeaderSize());
    m_cuSizeExcludePadding = (pos + m_bpNullCompressedSize + cmprDataSize);
    uint32 align_cu_size = (uint32)CUAlignUtils::AlignCuSize(m_cuSizeExcludePadding, align_size);
    if (m_cuSize != align_cu_size) {
        // this may caused by upgrade
        if (m_cuSize == ALLIGN_CUSIZE32(m_cuSizeExcludePadding) ||
            m_cuSize == ALIGNOF_CUSIZE512(m_cuSizeExcludePadding)) {
            return m_compressedBuf + pos;
        }
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("CU size error, %u in CU descriptor but %u in CU header",
                        m_cuSize,
                        align_cu_size)));
    }

    return m_compressedBuf + pos;
}

char* CU::UnCompressNullBitmapIfNeed(const char* buf, int rowCount)
{
    errno_t rc;
    if (HasNullValue()) {
        Assert(m_bpNullCompressedSize <= bitmap_size(rowCount));
        if (m_bpNullCompressedSize == m_bpNullRawSize) {
            rc = memcpy_s(m_nulls, m_bpNullRawSize, buf, m_bpNullRawSize);
            securec_check(rc, "\0", "\0");
        } else {
            // FUTURE CASE: uncompress nulls bitmap data
        }
    }

    return (char*)(buf + m_bpNullCompressedSize);
}

/*
 * @Description: is using dscale compress for numeric
 * @Return: true for dscale compress
 */
inline bool CU::IsNumericDscaleCompress() const
{
    return (m_infoMode & CU_DSCALE_NUMERIC) != 0;
}

/// numeric data type decompression.
/// Notice: *nNotNulls* is the number of not-null values.
template <bool DscaleFlag>
void CU::UncompressNumeric(char* inBuf, int nNotNulls, int typmode)
{
    Assert(m_infoMode & CU_IntLikeCompressed);
    Assert(ATT_IS_NUMERIC_TYPE(m_atttypid));

    char* tmpOutBuf = NULL;
    char* tmpInBuf = inBuf;
    int tmpInBufSize = this->m_cuSizeExcludePadding - this->GetCUHeaderSize() - this->m_bpNullCompressedSize;
    int tmpOutBufSize = 0;

    if ((this->m_infoMode & CU_INFOMASK1) & (~CU_IntLikeCompressed)) {
        /// decompress directly using lz4/zlib method.
        if (this->m_srcDataSize < INT_MAX - 8) {
            tmpOutBufSize = this->m_srcDataSize;
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                            errmsg("this->m_srcDataSize is too large: %u", this->m_srcDataSize)));
        }

        // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
        tmpOutBuf = (char*)palloc(tmpOutBufSize + 8);

        CompressionArg2 in = {tmpInBuf, tmpInBufSize, (uint16)this->m_infoMode};
        CompressionArg1 out = {tmpOutBuf, tmpOutBufSize, 0, NULL, 0, false, false, false};
        int err_code = 0;

        StringCoder strDecoder;
        err_code = strDecoder.Decompress(in, out);
        if (err_code <= 0) {
            /// maybe i's not necessary but it'ts a good habit.
            /// this memory will be freed when aborting transaction.
            pfree(tmpOutBuf);
            tmpOutBuf = NULL;
            if (-2 == err_code) {
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("memory is not enough during decompressing CU for integer type %d", (m_eachValSize))));
            } else {
                ereport(PANIC,
                        (errmsg("data corrupts during decompressing CU for integer type %d", (m_eachValSize))));
            }
        }
        Assert(err_code == out.sz);

        /// set the input arguments for next decompression step.
        tmpInBuf = tmpOutBuf;
        tmpInBufSize = out.sz;
    }

    /// the following function will reset these struct objects.
    numerics_statistics_out decmprOut1;
    numerics_cmprs_out decmprOut2;
    numerics_decmprs_addr_ref addrRef;
    char* ptr = NULL;
    uint16 flags = 0;

    /* filter is not used during decompression, so just set it null. */
    decmprOut2.filter = NULL;

    /// first parse the header info.
    ptr = NumericDecompressParseHeader(tmpInBuf, &flags, &decmprOut1, &decmprOut2);

    /// fetch all the adjust scale codes.
    decmprOut1.n_notnull = nNotNulls;
    if (DscaleFlag) {
        ptr = NumericDecompressParseDscales(ptr, &decmprOut1, flags);
        this->m_numericIntLike = (0 == decmprOut1.failed_cnt) ? true : false;
    } else {
        ptr = NumericDecompressParseAscales(ptr, &decmprOut1, flags);
    }

    /// decompress and get all the int64 && int32 && failed numeric values.
    (void)NumericDecompressParseEachPart(ptr, &decmprOut1, &decmprOut2, &addrRef);

    /// Important:
    /// we have to consider the size of *m_srcData*. Notice,
    /// *m_srcDataSize* here is not the size of raw numerics,
    /// but compressed integer values, because we compress them
    /// before entering the normal compress processor.
    int realSrcDataSize = (this->m_srcBuf + this->m_srcBufSize) - this->m_srcData;
    Assert((uint32)realSrcDataSize >= this->m_srcDataSize);
    int decompress_size = decmprOut1.original_size;
    if (likely(realSrcDataSize < decompress_size)) {
        /// we just remalloc for enough spaces, but not to
        /// change the value of *m_srcDataSize*. I have checked
        /// all the callers, and it's ok now.
        Assert(decompress_size > 0);
        this->ReallocMem((uint32)(decompress_size) + (uint32)(this->m_srcData - this->m_srcBuf));
    }
    /// restore all the numeric data.
    NumericDecompressRestoreValues<DscaleFlag>(this->m_srcData, typmode, &decmprOut1, &decmprOut2, &addrRef);

    /// finally we should release all the memory.
    NumericDecompressReleaseResources(&decmprOut1, &decmprOut2, &addrRef);

    if (tmpOutBuf != NULL) {
        pfree_ext(tmpOutBuf);
    }
}

void CU::UnCompressData(char* buf, int rowCount)
{
    if ((m_infoMode & CU_IntLikeCompressed) && ATT_IS_NUMERIC_TYPE(m_atttypid)) {
        /// compute the number of not-null values.
        /// and then decompress numeric type CU.
        if (IsNumericDscaleCompress())
            UncompressNumeric<true>(buf, rowCount - CountNullValuesBefore(rowCount), m_typeMode);
        else
            UncompressNumeric<false>(buf, rowCount - CountNullValuesBefore(rowCount), m_typeMode);
        return;
    }

    if ((m_infoMode & CU_INFOMASK1) != 0) {
        CompressionArg2 in;
        in.buf = buf;
        in.sz = m_cuSizeExcludePadding - GetCUHeaderSize() - m_bpNullCompressedSize;
        in.modes = m_infoMode;

        CompressionArg1 out = {0};
        out.buf = m_srcData;
        out.sz = m_srcDataSize;
        int eachValSize = m_eachValSize;
        int err_code;

        if (CU_Delta2Compressed == (m_infoMode & CU_COMPRESS_MASK1)) {
            ereport(DEBUG1,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("CU::UnCompressdaData use delta2 uncompress, m_infoMode (0x%hx)", m_infoMode)));
            SequenceCodec sequenceCoder(sizeof(int64), m_atttypid);
            err_code = sequenceCoder.decompress(in, out);
        } else if (CU_XORCompressed == (m_infoMode & CU_COMPRESS_MASK1)) {
            // XOR Decompress
            ereport(DEBUG1,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("CU::UnCompressdaData use XOR uncompress, m_infoMode (0x%hx)", m_infoMode)));
            SequenceCodec sequenceCoder(eachValSize, m_atttypid);
            err_code = sequenceCoder.decompress(in, out);
        } else {
            /*
             * this CU_IntLikeCompressed branch handle varlen datatype,
             * and the above branch has handled numeric datatype.
             */
            if ((m_infoMode & CU_IntLikeCompressed) != 0) {
                /*
                 * IF judgement above shouldn't include " &&  ATT_IS_CHAR_TYPE(m_atttypid) ".
                 * here is an example to break the condition:
                 *
                 *    CREATE TABLE fail_alter ( a int, b bpchar ) WITH ( ORIENTATION = COLUMN ) ;
                 *    INSERT INTO fail_alter VALUES ( 1, 1 ), (1, 20) , (1, 2), (1, null);
                 *    VACUUM FULL fail_alter ;
                 *    ALTER TABLE fail_alter ALTER COLUMN b SET DATA TYPE text ; <---- will not rewrite column data
                 *    SELECT * FROM fail_alter ;  <---- COREDUMP happens here
                 *
                 * please see function ATColumnChangeRequiresRewrite() for that root cause.
                 */
                Assert(!ATT_IS_NUMERIC_TYPE(m_atttypid));
                eachValSize = 8;
            }
            if (eachValSize > 0 && eachValSize <= 8) {
                // Integer Type Decompress
                IntegerCoder intDecoder(eachValSize);
                err_code = intDecoder.Decompress(in, out);
            } else {
                // String Type Decompress
                StringCoder strDecoder;
                err_code = strDecoder.Decompress(in, out);
            }
        }

        if (err_code <= 0) {
            if (-2 == err_code)
                ereport(ERROR,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                         errmsg("memory is not enough during decompressing CU for integer type %d", (m_eachValSize))));
            else
                ereport(PANIC, (errmsg("data corrupts during decompressing CU for integer type %d", (m_eachValSize))));
        }
        Assert(err_code == out.sz);
    } else {
        Assert(m_srcDataSize == (m_cuSizeExcludePadding - GetCUHeaderSize() - m_bpNullCompressedSize));
        errno_t rc = memcpy_s(m_srcData, m_srcDataSize, buf, m_srcDataSize);
        securec_check(rc, "\0", "\0");
        Assert((buf + m_srcDataSize) == (m_compressedBuf + m_cuSizeExcludePadding));
    }

    if ((m_infoMode & CU_IntLikeCompressed)) {
        /*
         * here m_atttypid is the target data type, which may be not the same with
         * the source data type when inserting or bulk loading. ALTER COLUMN SET
         * DATA TYPE is one of the changed paths.
         * for data type BPCHAR, take careful these two cases:
         * 1) BPCHAR(n),
         *    the tailing characters may be spaces, and its typemode is not -1.
         *    number like encoding will be applied to this case.
         * 2) BPCHAR,
         *    its typemode is -1, and number like encoding will not be applied to this case.
         *    so treat it as the normal string data type.
         */
        Assert(!ATT_IS_NUMERIC_TYPE(m_atttypid));
        if ((m_atttypid == BPCHAROID) && (m_typeMode > VARHDRSZ)) {
            this->DeFormNumberStringCU<true>();
        } else { /* deform the other varlen type */
            this->DeFormNumberStringCU<false>();
        }
    }
    return;
}

template <bool bpcharType>
void CU::DeFormNumberStringCU()
{
    errno_t errorno = EOK;
    uint32 pos = 0;

    // current CU format:
    //   8B: data count
    //   data array: each data is 8B
    //   8B: total length of original varlenas, excluding var-head.
    uint32 dataCount = *(uint64*)this->m_srcData;
    uint32 origTotalSize = *(uint64*)(this->m_srcData + sizeof(uint64) * (dataCount + 1));

    // buffer format:
    //   NULL bitmap | string head(4 bit) | string values | .... | '\0'
    uint32 tmpSrcBufSize = origTotalSize + dataCount * VARHDRSZ + this->m_bpNullRawSize;
    char* tmpSrcBuf = (char*)CStoreMemAlloc::Palloc(sizeof(char) * (tmpSrcBufSize + 1), !this->m_inCUCache);

    if (bpcharType) {
        errorno = memset_s(tmpSrcBuf, tmpSrcBufSize, ' ', tmpSrcBufSize);
        securec_check_c(errorno, "\0", "\0");
    }
    tmpSrcBuf[tmpSrcBufSize] = '\0';

    // copy bitmap and jump if need
    if (this->m_bpNullRawSize != 0) {
        errorno = memcpy_s(tmpSrcBuf, tmpSrcBufSize, this->m_srcBuf, this->m_bpNullRawSize);
        securec_check_c(errorno, "\0", "\0");
        pos += this->m_bpNullRawSize;
    }

    int32 realStrLen = 0;
    int32 const bpcharLen = this->m_typeMode - VARHDRSZ;
    uint64 integer = 0;
    char* tmpSrcData = (this->m_srcData + sizeof(uint64));
    while (dataCount > 0) {
        integer = *(uint64*)tmpSrcData;
        // this is header of char value
        pos += VARHDRSZ;
        Assert(pos < tmpSrcBufSize);
        realStrLen = uint64_to_str(tmpSrcBuf + pos, integer);
        Assert(realStrLen > 0);

        if (bpcharType) {
            // deform char type, need jump blank
            Assert(this->m_typeMode > VARHDRSZ);
            Assert(realStrLen <= bpcharLen);
            SET_VARSIZE(tmpSrcBuf + pos - VARHDRSZ, this->m_typeMode);
            pos += bpcharLen;
        } else {
            SET_VARSIZE(tmpSrcBuf + pos - VARHDRSZ, realStrLen + VARHDRSZ);
            pos += realStrLen;
        }

        Assert(pos <= tmpSrcBufSize);
        tmpSrcData += sizeof(uint64);
        --dataCount;
    }

    // assign all the new memory addresses and their size.
    CStoreMemAlloc::Pfree(this->m_srcBuf, !this->m_inCUCache);
    this->m_srcBuf = tmpSrcBuf;
    this->m_srcBufSize = tmpSrcBufSize + 1;
    this->m_srcData = this->m_srcBuf + this->m_bpNullRawSize;
    this->m_srcDataSize = this->m_srcBuf + pos - this->m_srcData;
    if (this->m_bpNullRawSize != 0) {
        this->m_nulls = (unsigned char*)this->m_srcBuf;
    }
}

template <bool hasNull>
void CU::FormValuesOffset(int rows)
{
    // If fixed length and no NULL value, it will be skipped
    if (this->m_eachValSize > 0 && !hasNull) {
        return;
    }

    Assert(NULL == this->m_offset);
    this->m_offset = (int32*)CStoreMemAlloc::Palloc(sizeof(int32) * (rows + 1), !this->m_inCUCache);
    this->m_offsetSize = sizeof(int32) * (rows + 1);
    this->m_offset[0] = 0;
    int32 tmp = 0;
    int32 tmpPrev = 0;

    /*
     * If this CU is IntLikeCompressed, numeric type, not null value and all data
     * in the numeric CU can be transformed to Int64, we can calculate the offset in the fast way.
     */
    if ((m_infoMode & CU_IntLikeCompressed)
        && ATT_IS_NUMERIC_TYPE(m_atttypid) && !hasNull && m_numericIntLike) {
        for (int i = 1; i < rows + 1; ++i) {
            this->m_offset[i] = i * NUMERIC_64SZ;
        }
        return;
    }

    if (-1 == this->m_eachValSize) {
        for (int i = 1; i < rows + 1; ++i) {
            if (hasNull && this->IsNull(i - 1)) {
                this->m_offset[i] = this->m_offset[i - 1];
            } else {
                tmp = tmpPrev + VARSIZE_ANY(this->m_srcData + tmpPrev);
                this->m_offset[i] = tmp;
                tmpPrev = tmp;

                /*
                 * Check the valid offset.
                 * here m_srcBufSize is used but not m_srcDataSize becaused of numeric datatype.
                 * raw numeric may be converted to integer value first (m_srcDataSize) then compressed.
                 * the size of raw numeric may be greater than m_srcDataSize.
                 */
                if (unlikely((uint32)tmp > this->m_srcBufSize)) {
                    ereport(defence_errlevel(),
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("The offset is incorrect, idx = %d, offset = %d.", i, tmp)));
                }
            }
        }
    } else if (-2 == this->m_eachValSize) {
        for (int i = 1; i < rows + 1; ++i) {
            if (hasNull && this->IsNull(i - 1)) {
                this->m_offset[i] = this->m_offset[i - 1];
            } else {
                this->m_offset[i] = this->m_offset[i - 1] + strlen(this->m_srcData + this->m_offset[i - 1]);
            }

            /* Check the valid offset. */
            if (unlikely((uint32)this->m_offset[i] > this->m_srcBufSize)) {
                ereport(defence_errlevel(),
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("The offset is incorrect, idx = %d, offset = %d.", i, tmp)));
            }
        }
    } else if (this->m_eachValSize > 0) {
        for (int i = 1; i < rows + 1; ++i) {
            if (hasNull && this->IsNull(i - 1)) {
                this->m_offset[i] = this->m_offset[i - 1];
            } else {
                this->m_offset[i] = this->m_offset[i - 1] + this->m_eachValSize;
            }
        }
    }
}

/*
 * @Description: fill the vector by the tids.
 * @in tids: the tid vector.
 * @out vec: the output ScalarVector.
 * @template attlen: the length of this column.
 * @template hasNull: whether the cu has null value.
 */
template <int attlen, bool hasNull>
int CU::ToVectorLateRead(_in_ ScalarVector* tids, _out_ ScalarVector* vec)
{
    ScalarValue* tidVals = tids->m_vals;
    ItemPointer tidPtr = NULL;
    uint32 tmpCuId = InValidCUID;
    uint32 tmpOffset = 0;
    int pos = 0;

    for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
        tidPtr = (ItemPointer)(tidVals + rowCnt);
        tmpCuId = ItemPointerGetBlockNumber(tidPtr);
        tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        if (hasNull && this->IsNull(tmpOffset)) {
            vec->SetNull(pos);
            pos++;
            continue;
        }

        switch (attlen) {
            case sizeof(char):
            case sizeof(int16):
            case sizeof(int32):
            case sizeof(Datum): {
                ScalarValue* dest = vec->m_vals + pos;
                *dest = this->GetValue<attlen, hasNull>(tmpOffset);
                break;
            }
            case 12:
            case 16:
            case -1:
            case -2: {
                ScalarValue value = this->GetValue<attlen, hasNull>(tmpOffset);
                vec->AddVar(PointerGetDatum(value), pos);
                break;
            }
            default:
                Assert(0);
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unsupported datatype branch")));
                break;
        }
        pos++;
    }

    return pos;
}

/*
 * @Description: fill the vector by the tids in special template that attlen = 8 and not null value.
 * @in tids: the tid vector.
 * @out vec: the output ScalarVector.
 */
template <>
int CU::ToVectorLateRead<8, false>(_in_ ScalarVector* tids, _out_ ScalarVector* vec)
{
    ScalarValue* tidVals = tids->m_vals;
    ItemPointer tidPtr = NULL;

    uint32 tmpOffset = 0;
    uint32 firstOffset = 0;
    uint32 nextOffset = 0;

    int pos = 0;
    size_t contiguous = 0;

    for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
        tidPtr = (ItemPointer)(tidVals + rowCnt);
        tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        contiguous = 0;
        nextOffset = tmpOffset;
        firstOffset = tmpOffset;

        while (tmpOffset == nextOffset) {
            ++contiguous;
            ++nextOffset;

            if (unlikely(++rowCnt == tids->m_rows)) {
                break;
            }

            /* fetch and check the next data contiguous within the same cu. */
            tidPtr = (ItemPointer)(tidVals + rowCnt);
            tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;
        }

        /* rowCnt is point to the first tid in the next contiguous data. */
        --rowCnt;
        errno_t rc = memcpy_s((char*)(vec->m_vals + pos),
                              (size_t)(contiguous << 3),
                              ((uint64*)this->m_srcData + firstOffset),
                              (size_t)(contiguous << 3));
        securec_check(rc, "\0", "\0");

        pos += contiguous;
    }

    return pos;
}

/*
 * @Description: fill the vector by the tids in special template that attlen = -1 and not null value.
 * @in tids: the tid vector.
 * @out vec: the output ScalarVector.
 */
template <>
int CU::ToVectorLateRead<-1, false>(_in_ ScalarVector* tids, _out_ ScalarVector* vec)
{
    ScalarValue* tidVals = tids->m_vals;
    ItemPointer tidPtr = NULL;

    uint32 tmpOffset = 0;
    uint32 firstOffset = 0;
    uint32 nextOffset = 0;

    int pos = 0;
    int contiguous = 0;

    for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
        tidPtr = (ItemPointer)(tidVals + rowCnt);
        tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        contiguous = 0;
        nextOffset = tmpOffset;
        firstOffset = tmpOffset;

        while (tmpOffset == nextOffset) {
            ++contiguous;
            ++nextOffset;

            if (unlikely(++rowCnt == tids->m_rows))
                break;

            /* fetch and check the next data contiguous within the same cu. */
            tidPtr = (ItemPointer)(tidVals + rowCnt);
            tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;
        }

        /* rowCnt is point to the first tid in the next contiguous data. */
        --rowCnt;

        int32* offset = this->m_offset + firstOffset;
        int32 obase = offset[0];
        int totalBytes = offset[contiguous] - obase;
        char* src = this->m_srcData + obase;

        /*
         * We can copy all elements in one batch together and fix the pointer
         * by adding the memory base. It is manaully unrolled the loop to give
         * a strong hint to compiler.
         */
        char* base = vec->AddVars(src, totalBytes) - obase;
        ScalarValue* dest = vec->m_vals + pos;
        for (uint32 k = 0; k < ((uint32)contiguous >> 2); ++k) {
            *dest++ = (ScalarValue)(base + *offset++);
            *dest++ = (ScalarValue)(base + *offset++);
            *dest++ = (ScalarValue)(base + *offset++);
            *dest++ = (ScalarValue)(base + *offset++);
        }
        for (uint32 k = 0; k < ((uint32)contiguous & 0x3); k++)
            *dest++ = (ScalarValue)(base + *offset++);
        pos += contiguous;
    }

    return pos;
}

template int CU::ToVectorLateRead<1, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<2, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<4, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<8, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<12, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<16, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<-1, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<-2, true>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<1, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<2, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<4, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<8, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<12, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<16, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<-1, false>(ScalarVector*, ScalarVector*);
template int CU::ToVectorLateRead<-2, false>(ScalarVector*, ScalarVector*);

template <int attlen, bool hasNull, bool hasDeadRow>
int CU::ToVectorT(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU, __inout int& curScanPos,
                  _out_ int& deadRows, _in_ uint8* cuDelMask)
{
    ScalarValue* dest = vec->m_vals;
    char* src = m_srcData + curScanPos;
    int pos = 0;

    for (int i = 0; i < leftRows && pos < BatchMaxSize; ++i) {
        uint32 row = (uint32)(i + rowCursorInCU);
        // Set Null if need
        if (hasNull && IsNull(row)) {
            if (!(hasDeadRow && ((cuDelMask[row >> 3] & (1 << (row % 8))) != 0))) {
                vec->SetNull(pos);
                ++pos;
            } else
                ++deadRows;

            continue;
        }

        // Set normal value
        if (attlen > 0) {
            // Skip dead row
            // Note that we must maintain src pointer and curScanPos
            if (hasDeadRow && ((cuDelMask[row >> 3] & (1 << (row % 8))) != 0)) {
                src += attlen;
                curScanPos += attlen;
                ++deadRows;
                continue;
            }

            // Now this is a live row
            switch (attlen) {
                case sizeof(char):
                    dest[pos] = *(uint8 *)src;
                    src += sizeof(char);
                    curScanPos += sizeof(char);
                    break;
                case sizeof(uint16):
                    dest[pos] = *(uint16*)src;
                    src += sizeof(uint16);
                    curScanPos += sizeof(uint16);
                    break;
                case sizeof(uint32):
                    dest[pos] = *(uint32*)src;
                    src += sizeof(uint32);
                    curScanPos += sizeof(uint32);
                    break;
                case sizeof(uint64):
                    dest[pos] = *(Datum*)src;
                    src += sizeof(Datum);
                    curScanPos += sizeof(Datum);
                    break;
                case 12:
                case 16:
                    vec->AddVar(PointerGetDatum(src), pos);
                    src += attlen;
                    curScanPos += attlen;
                    break;
                default:
                    Assert(false);
                    break;
            }
            ++pos;
        } else if (attlen == -1) {
            Datum v = PointerGetDatum(src);
            int len = VARSIZE_ANY(src);
            Assert(len > 0);

            // this is a live row
            if (!(hasDeadRow && ((cuDelMask[row >> 3] & (1 << (row % 8))) != 0))) {
                vec->AddVar(v, pos);
                ++pos;
            } else
                ++deadRows;
            src += len;
            curScanPos += len;
        } else if (attlen == -2) {
            Datum v = PointerGetDatum(src);
            int len = strlen(src) + 1;

            // this is a live row
            if (!(hasDeadRow && ((cuDelMask[row >> 3] & (1 << (row % 8))) != 0))) {
                vec->AddVar(v, pos);
                ++pos;
            } else
                ++deadRows;
            src += len;
            curScanPos += len;
        }
    }
    return pos;
}

// A specilized tempalte optimized implementation for variable length vector
// i.e., (attlen == -1), by copying all elements in one batch togeher.
template <>
int CU::ToVectorT<-1, false, false>(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU,
                                    __inout int& curScanPos, _out_ int& deadRows, _in_ uint8* cuDelMask)
{
    Assert((uint32)curScanPos < this->m_srcBufSize);
    Assert((uint32)rowCursorInCU < (this->m_offsetSize / sizeof(int32)));

    char* src = this->m_srcData + curScanPos;
    int32* offset = this->m_offset + rowCursorInCU;
    int32 obase = offset[0];

    // Total bytes to be copied is calculated from the offset table.
    uint32 totalRows = Min(leftRows, BatchMaxSize);
    int totalBytes = offset[totalRows] - obase;
    Assert(totalBytes > 0);

    // We can copy all elements in one batch together and fix the pointer
    // by adding the memory base. It is manaully unrolled the loop to give
    // a strong hint to compiler
    char* base = vec->AddVars(src, totalBytes) - obase;
    ScalarValue* dest = vec->m_vals;
    for (uint32 i = 0; i < (totalRows >> 2); ++i) {
        *dest++ = (ScalarValue)(base + *offset++);
        *dest++ = (ScalarValue)(base + *offset++);
        *dest++ = (ScalarValue)(base + *offset++);
        *dest++ = (ScalarValue)(base + *offset++);
    }
    for (uint32 i = 0; i < (totalRows & 0x3); i++)
        *dest++ = (ScalarValue)(base + *offset++);

    // Update scan cursor and set return values
    deadRows = 0;
    curScanPos += totalBytes;

    return totalRows;
}

// A specilized tempalte optimized implementation when *attlen* is 8.
template <>
int CU::ToVectorT<8, false, false>(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU,
                                   __inout int& curScanPos, _out_ int& deadRows, _in_ uint8* cuDelMask)
{
    Assert(sizeof(ScalarValue) == 8);

    // copy data from src[] to dest[].
    ScalarValue* src = (ScalarValue*)(this->m_srcData + curScanPos);
    ScalarValue* dest = vec->m_vals;
    int totalRows = Min(leftRows, BatchMaxSize);
    int totalBytes = (uint32)totalRows << 3;  // totalRows*8

    // directly copy all the elements in one batch together.
    errno_t rc = memcpy_s(dest, (8 * BatchMaxSize), src, totalBytes);
    securec_check(rc, "", "");

    // update output arguments
    deadRows = 0;
    curScanPos += totalBytes;

    return totalRows;
}

template <int attlen, bool hasDeadRow>
int CU::ToVector(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU, __inout int& curScanPos,
                 _out_ int& deadRows, _in_ uint8* cuDelMask)
{
    int num = 0;

    if (!this->HasNullValue())
        num = this->ToVectorT<attlen, false, hasDeadRow>(vec, leftRows, rowCursorInCU, curScanPos, deadRows, cuDelMask);
    else
        num = this->ToVectorT<attlen, true, hasDeadRow>(vec, leftRows, rowCursorInCU, curScanPos, deadRows, cuDelMask);
    return num;
}

// enumerate possible instances
template int CU::ToVector<1, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<2, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<4, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<8, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<12, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<16, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<-1, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<-2, true>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<1, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<2, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<4, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<8, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<12, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<16, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<-1, false>(ScalarVector*, int, int, int&, int&, uint8*);
template int CU::ToVector<-2, false>(ScalarVector*, int, int, int&, int&, uint8*);

FORCE_INLINE
bool CU::HasNullValue() const
{
    return (m_infoMode & CU_HasNULL) ? true : false;
}

/*
 * @Description:  free compress buf without unregister, called by cucache mgr
 * @See also:
 */
void CU::FreeCompressBuf()
{
    if (m_compressedLoadBuf) { // read
        free(m_compressedLoadBuf);
    } else { // write
        if (m_compressedBuf)
            free(m_compressedBuf);
    }
    m_compressedBuf = NULL;
    m_compressedBufSize = 0;
    m_compressedLoadBuf = NULL;
    m_head_padding_size = 0;
}

void CU::FreeSrcBuf()
{
    if (m_srcBuf) {
        CStoreMemAlloc::Pfree(m_srcBuf, !m_inCUCache);
    }
    m_srcBuf = NULL;
    m_srcBufSize = 0;

    if (m_offset) {
        CStoreMemAlloc::Pfree(m_offset, !m_inCUCache);
    }
    m_offset = NULL;
    m_offsetSize = 0;
}

FORCE_INLINE
int CU::GetCUSize() const
{
    ASSERT_CUSIZE(m_cuSize);
    return m_cuSize;
}

FORCE_INLINE
void CU::SetCUSize(int cuSize)
{
    m_cuSize = cuSize;
    ASSERT_CUSIZE(m_cuSize);
}

FORCE_INLINE
int CU::GetCompressBufSize() const
{
    return m_compressedBufSize;
}

FORCE_INLINE
int CU::GetUncompressBufSize() const
{
    return m_srcBufSize + m_offsetSize;
}

FORCE_INLINE
void CU::SetMagic(uint32 magic)
{
    m_magic = magic;
}

FORCE_INLINE
uint32 CU::GetMagic() const
{
    return m_magic;
}

/*
 * CU data encrypt. Will support in the future.
 * Since CU have no buffer and write disk directly,so no concurrent thread issue.
 */
void CU::CUDataEncrypt(char* buf)
{
    return;
}

/*
 * CU data decrypt. Will support in the future.
 * Since CU have no buffer and write disk directly,so no concurrent thread issue.
 */
void CU::CUDataDecrypt(char* buf)
{
    return;
}

/* 
 * @description: memmove cu.m_srcData to cu.m_srcBuf + null_size
 *               then copy null buffer to cu.m_srcBuf, ts function
 * @param {CU}
 * @param {bitmap*} : bitmap of null data
 * @param {null_size*} : bitmap size
 * @return {void} 
 */
void CU::copy_nullbuf_to_cu(const char* bitmap, uint16 null_size)
{
    errno_t rc = memmove_s(m_srcBuf + null_size, m_srcBufSize - null_size, m_srcData, m_srcDataSize);
    securec_check(rc, "\0", "\0");
    m_srcData = m_srcBuf + null_size;
    m_nulls = (unsigned char*)m_srcBuf;
    rc = memcpy_s(m_nulls, null_size, bitmap, null_size);
    securec_check(rc, "\0", "\0");
    m_bpNullRawSize = null_size;
}

/* 
 * @description:  we estimate the memory size, not so exactly, ts function
 * @param {reserved_cu_byte} 
 * @return {uint32} total_field_srcbuf_size
 */
uint32 CU::init_field_mem(const int reserved_cu_byte)
{
    const uint32 field_srcbuf_init_size = 8 * 1024 * 1024; // 8M, just one estimated size without exact caculation
    Size init_size = (Size)GetCUHeaderSize() + sizeof(Datum) + field_srcbuf_init_size;
    init_size = MAXALIGN(init_size);
    uint32 null_size = (uint32)bitmap_size(MAX_BATCH_ROWS);
    m_inCUCache = true;
    m_srcBufSize = init_size + null_size;
    m_srcBuf = reinterpret_cast<char*>(CStoreMemAlloc::Palloc(m_srcBufSize + reserved_cu_byte, !m_inCUCache));
    SetMagic((uint32)(GetCurrentTransactionIdIfAny()));
    m_compressedBufSize =
        CUAlignUtils::AlignCuSize(m_srcBufSize + null_size + sizeof(CU), ALIGNOF_TIMESERIES_CUSIZE);
    m_compressedBuf = reinterpret_cast<char*>(CStoreMemAlloc::Palloc(m_compressedBufSize, !m_inCUCache));
    return m_srcBufSize;
}

/* 
 * @description:  Init time rows used memory in CU
 * @param {CU}
 * @return {} total_time_srcbuf_size
 */
uint32 CU::init_time_mem(const int reserved_cu_byte)
{
    Size init_size = (Size)GetCUHeaderSize() + sizeof(Datum) + sizeof(TimestampTz) * MAX_BATCH_ROWS;
    init_size = MAXALIGN(init_size);
    Size null_size = bitmap_size(MAX_BATCH_ROWS);
    m_inCUCache = true;

    m_srcBufSize = init_size + null_size;
    m_srcBuf = reinterpret_cast<char*>(CStoreMemAlloc::Palloc(m_srcBufSize + reserved_cu_byte, !m_inCUCache));
    SetMagic((uint32)(GetCurrentTransactionIdIfAny()));
    m_compressedBufSize =
        CUAlignUtils::AlignCuSize(m_srcBufSize + null_size + sizeof(CU), ALIGNOF_TIMESERIES_CUSIZE);
    m_compressedBuf = reinterpret_cast<char*>(CStoreMemAlloc::Palloc(m_compressedBufSize, !m_inCUCache));
    return m_srcBufSize;
}

void CU::check_cu_consistence(const CUDesc* cudesc) const
{
    if (m_srcBuf == NULL ||
        (m_eachValSize < 0 && m_offset == NULL) ||
        (HasNullValue() && m_offset == NULL) ||
        (m_magic != cudesc->magic) ||
        (m_cuSize != (uint32)cudesc->cu_size)) {
            ereport(defence_errlevel(), (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("CU info dismatch CUDesc info.")));
        }
}

int CU::GetCurScanPos(int rowCursorInCU)
{
    Assert(rowCursorInCU >= 0);
    Assert(this->m_offset[rowCursorInCU] >= 0);
    return this->m_offset[rowCursorInCU];
}
