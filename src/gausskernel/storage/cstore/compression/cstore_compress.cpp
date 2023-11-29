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
 * cstore_compress.cpp
 *      user interface for compression methods
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/compression/cstore_compress.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/cu.h"
#include "utils/biginteger.h"
#include "utils/gs_bitmap.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

extern int8 heaprel_get_compresslevel_from_modes(int16 modes);
extern int8 heaprel_get_compression_from_modes(int16 modes);

/// we need to do unit testing for some *static* functions,
/// so we redefine *static* if *ENABLE_UT* is defined.
/// at the end of this file we will restore it.
#ifdef ENABLE_UT
    #define static
#endif

/* compress level table for different compression values */
static const int8 compresslevel_tables[COMPRESS_HIGH + 1][REL_MAX_COMPRESSLEVEL + 1] = {
    /* COMPRESS_NO */
    {0, 0, 0, 0},
    /* COMPRESS_LOW :: LZ4 */
    {   LZ4Wrapper::lz4_min_level,
        LZ4Wrapper::lz4hc_min_level,
        LZ4Wrapper::lz4hc_min_level + LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_min_level + LZ4Wrapper::lz4hc_level_step * 2
    },
    /* COMPRESS_MIDDLE :: LZ4 */
    {   LZ4Wrapper::lz4hc_recommend_level - LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_recommend_level,
        LZ4Wrapper::lz4hc_recommend_level + LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_recommend_level + LZ4Wrapper::lz4hc_level_step * 2
    },
    /* COMPRESS_HIGH :: ZLIB */
    {   ZlibEncoder::zlib_recommend_level,
        ZlibEncoder::zlib_recommend_level + ZlibEncoder::zlib_level_step,
        ZlibEncoder::zlib_recommend_level + ZlibEncoder::zlib_level_step * 2,
        ZlibEncoder::zlib_max_level
    }
};

// FUTURE CASE: add other types in ascending order
// all types in this table needn't to compute the min/max value.
// all types in this table are signed.
// Warning:
//   the Oid collect must a subset of FuncTabSetMinMax[] Oid collect,
//   and the corresponding min/max function mustn't be NULL.
static Oid SignedTypeOidTable[] = {
    CHAROID,  // oid equals to 18
    INT8OID,  // oid equals to 20
    INT2OID,  // oid equals to 21
    INT4OID,  // oid equals to 23
    DATEOID,  // oid equals to 1082
#ifdef HAVE_INT64_TIMESTAMP
    TIMEOID,  // oid equals to 1083
    TIMESTAMPOID,  // oid equals to 1114
    TIMESTAMPTZOID,  // oid equals to 1184
#endif
    INT1OID  // oid equals to 5545, tinyint
};

static const int SignedTypeOidTableSize = sizeof(SignedTypeOidTable) / sizeof(Oid);

bool NeedToRecomputeMinMax(Oid typeOid)
{
    int left = 0;
    int mid = -1;
    int right = SignedTypeOidTableSize - 1;

    while (left <= right) {
        mid = left + ((right - left) / 2);

        if (SignedTypeOidTable[mid] > typeOid) {
            right = mid - 1;
        } else if (SignedTypeOidTable[mid] < typeOid) {
            left = mid + 1;
        } else {
            return false;
        }
    }

    return true;
}

int64 ConvertToInt64Data(_in_ const char* inBuf, _in_ const short eachValSize)
{
    Assert(eachValSize > 0 && eachValSize <= (short)sizeof(int64));
    switch (eachValSize) {
        case sizeof(int8):
            return *(int8*)inBuf;
        // no need to break
        case sizeof(int16):
            return *(int16*)inBuf;
        // no need to break
        case sizeof(int32):
            return *(int32*)inBuf;
        // no need to break
        case sizeof(int64):
            return *(int64*)inBuf;
        // no need to break
        default:
            Assert(false);
            return -1;  // keep complier silient
    }
}

void Int64DataConvertTo(_in_ int64 inVal, _in_ short eachValSize, __inout char* outBuf)
{
    Assert(eachValSize > 0 && eachValSize <= (short)sizeof(int64));
    switch (eachValSize) {
        case sizeof(int8):
            *(int8*)outBuf = (int8)inVal;
            break;
        case sizeof(int16):
            *(int16*)outBuf = (int16)inVal;
            break;
        case sizeof(int32):
            *(int32*)outBuf = (int32)inVal;
            break;
        case sizeof(int64):
            *(int64*)outBuf = (int64)inVal;
            break;
        default:
            Assert(false);
            break;
    }
}

// function template
//     compute the min/max value for different integer type
template <typename intType>
static void ComputeMinMaxVal(_in_ intType* vals, _in_ int nvals, _out_ int64* minVal, _out_ int64* maxVal)
{
    Assert(nvals > 0);
    intType tempMin = vals[0];
    intType tempMax = vals[0];

    for (int i = 1; i < nvals; ++i) {
        if (vals[i] < tempMin) {
            tempMin = vals[i];
        } else if (vals[i] > tempMax) {
            tempMax = vals[i];
        }
    }

    *minVal = tempMin;
    *maxVal = tempMax;
}

// compute the min/max value for different integer type
// int8/int16/int32/int64 supported.
static void ComputeIntMinMaxVal(
    _in_ char* inBuf, _in_ int inSize, _in_ short eachValSize, _out_ int64* minVal, _out_ int64* maxVal)
{
    Assert(eachValSize > 0 && eachValSize <= (short)sizeof(int64));
    switch (eachValSize) {
        case sizeof(int8): {
            Assert(0 == (inSize % sizeof(int8)));
            ComputeMinMaxVal<int8>((int8*)inBuf, (inSize / sizeof(int8)), minVal, maxVal);
            break;
        }
        case sizeof(int16): {
            Assert(0 == (inSize % sizeof(int16)));
            ComputeMinMaxVal<int16>((int16*)inBuf, (inSize / sizeof(int16)), minVal, maxVal);
            break;
        }
        case sizeof(int32): {
            Assert(0 == (inSize % sizeof(int32)));
            ComputeMinMaxVal<int32>((int32*)inBuf, (inSize / sizeof(int32)), minVal, maxVal);
            break;
        }
        case sizeof(int64): {
            Assert(0 == (inSize % sizeof(int64)));
            ComputeMinMaxVal<int64>((int64*)inBuf, (inSize / sizeof(int64)), minVal, maxVal);
            break;
        }
        default:
            Assert(false);
            break;
    }
}

#ifdef USE_ASSERT_CHECKING

// function template
//     compute the min/max value for different integer type
template <typename intType>
static void CheckMinMaxValue(intType* vals, int nvals, int64 minVal, int64 maxVal)
{
    Assert(nvals > 0);
    bool minFound = false;
    bool maxFound = false;
    for (int i = 0; i < nvals; ++i) {
        Assert(minVal <= vals[i] && vals[i] <= maxVal);
        if (!minFound && minVal == vals[i]) {
            minFound = true;
        }
        if (!maxFound && maxVal == vals[i]) {
            maxFound = true;
        }
    }
    Assert(minFound && maxFound);
}

static void CheckMinMaxValue(char* rawDataBuf, int rawDataSize, short eachValSize, int64 minVal, int64 maxVal)
{
    Assert(eachValSize > 0 && eachValSize <= (short)sizeof(int64));
    switch (eachValSize) {
        case sizeof(int8): {
            Assert((rawDataSize % sizeof(int8)) == 0);
            CheckMinMaxValue<int8>((int8*)rawDataBuf, (rawDataSize / sizeof(int8)), minVal, maxVal);
            break;
        }
        case sizeof(int16): {
            Assert((rawDataSize % sizeof(int16)) == 0);
            CheckMinMaxValue<int16>((int16*)rawDataBuf, (rawDataSize / sizeof(int16)), minVal, maxVal);
            break;
        }
        case sizeof(int32): {
            Assert((rawDataSize % sizeof(int32)) == 0);
            CheckMinMaxValue<int32>((int32*)rawDataBuf, (rawDataSize / sizeof(int32)), minVal, maxVal);
            break;
        }
        case sizeof(int64): {
            Assert((rawDataSize % sizeof(int64)) == 0);
            CheckMinMaxValue<int64>((int64*)rawDataBuf, (rawDataSize / sizeof(int64)), minVal, maxVal);
            break;
        }
        default:
            Assert(false);
            break;
    }
}

static void IntegerCheckCompressedData(
    char* rawData, int rawDataSize, char* cmprBuf, int cmprBufSize, uint16 modes, short eachValSize)
{
    if (modes == 0)
        return;

    CompressionArg2 in = {cmprBuf, cmprBufSize, modes};
    CompressionArg1 out = {0};
    out.buf = (char*)palloc(rawDataSize);
    out.sz = rawDataSize;

    IntegerCoder intDecoder(eachValSize);
    int uncmprSize = intDecoder.Decompress(in, out);
    Assert(uncmprSize == rawDataSize);
    Assert(memcmp(out.buf, rawData, rawDataSize) == 0);
    pfree(out.buf);
}

#endif

static FORCE_INLINE void prepareSwapBuf(char *&buf1, char *&buf2, int &sz1, int &sz2, char *newbuf2, int newsz2,
                                        bool &prepared)
{
    Assert(prepared == false);
    buf1 = buf2;
    buf2 = newbuf2;
    sz1 = sz2;
    sz2 = newsz2;
    prepared = true;
}

static FORCE_INLINE void swapBuf(char *&buf1, char *&buf2, int &sz1, int &sz2)
{
    char *tmp = buf1;
    buf1 = buf2;
    buf2 = tmp;

    int tmpsz = sz1;
    sz1 = sz2;
    sz2 = tmpsz;
}

IntegerCoder::IntegerCoder(short valSize)
    : m_adopt_rle(true), m_minVal(0), m_maxVal(0), m_isValid(false), m_eachValSize(valSize)
{}

void IntegerCoder::SetMinMaxVal(int64 min, int64 max)
{
    m_minVal = min;
    m_maxVal = max;
    m_isValid = true;
}

void IntegerCoder::InsertMinMaxVal(char* buf, int* usedSize)
{
    // reserve space at the head to insert the min/max value
    errno_t ret = memmove_s(buf + (m_eachValSize * 2), *usedSize, buf, *usedSize);
    securec_check(ret, "", "");

    // insert the min/max value ahead
    Assert(m_isValid);
    Int64DataConvertTo(m_minVal, m_eachValSize, buf);
    Int64DataConvertTo(m_maxVal, m_eachValSize, buf + m_eachValSize);
    *usedSize += (m_eachValSize * 2);
}

// 0 returned means that no data has been compressed.
// othersize value>0 returned and compressed data and its size are within out.
template <bool adopt_rle>
int IntegerCoder::CompressInner(const CompressionArg1& in, CompressionArg2& out)
{
    int8 compression = heaprel_get_compression_from_modes(in.mode);
    int8 compresslevel = heaprel_get_compresslevel_from_modes(in.mode);

    Assert(in.globalDict == NULL && in.useDict == false);
    Assert(compression >= COMPRESS_LOW && compression <= COMPRESS_HIGH);
    Assert((in.sz % this->m_eachValSize) == 0);

    Size boundSize = 0;
    short outValSize = 0;

    char* currInBuf = in.buf;
    int currInBufSize = in.sz;

    BufferHelper tempOutBuf = {NULL, 0, Unknown};
    int cmprSize = 0;
    errno_t rc = EOK;

    BufferHelperMalloc(&tempOutBuf, in.sz);

    // if min/max isn't set by SetMinMaxVal(), compute before anything to do
    if (!this->m_isValid) {
        ComputeIntMinMaxVal(in.buf, in.sz, this->m_eachValSize, &this->m_minVal, &this->m_maxVal);
        this->m_isValid = true;
    }
#ifdef USE_ASSERT_CHECKING
    CheckMinMaxValue(in.buf, in.sz, m_eachValSize, m_minVal, m_maxVal);
#endif

    // Step 1: try to do DELTA compression
    DeltaCoder delta(this->m_minVal, this->m_maxVal, true);
    outValSize = delta.GetOutValSize();
    Assert(outValSize <= this->m_eachValSize);
    if (outValSize < this->m_eachValSize) { // check whether delta compression can be applied to
        // the outbuf is big enough to store compressed data, so getBound() needn't be called.
        // and the resulting size must be smaller than the input data size.
        Assert(tempOutBuf.bufSize > delta.GetBound((in.sz / this->m_eachValSize)));
        cmprSize = delta.Compress(currInBuf, tempOutBuf.buf, currInBufSize, tempOutBuf.bufSize, this->m_eachValSize);
        // Compressed size plus min/max value should be less than source buffer size
        // If condition is ok, we can apply delta compression.
        // InsertMinMaxVal() can be called safely later.
        if (cmprSize + (this->m_eachValSize * 2) < currInBufSize) {
            Assert(cmprSize < currInBufSize && (Size)cmprSize < tempOutBuf.bufSize);
            rc = memcpy_s(out.buf, cmprSize, tempOutBuf.buf, cmprSize);
            securec_check(rc, "", "");
            out.sz = cmprSize;
            out.modes |= CU_DeltaCompressed;

            currInBuf = out.buf;
            currInBufSize = cmprSize;
        } else { // we can't apply delta compression
            outValSize = this->m_eachValSize;
        }
    }
    Assert(outValSize <= this->m_eachValSize);

    // Step2: try to do RleCoder compression
    if (adopt_rle) {
        RleCoder rle(outValSize);
        boundSize = rle.CompressGetBound(currInBufSize);
        if (boundSize > tempOutBuf.bufSize) {
            BufferHelperRemalloc(&tempOutBuf, boundSize);
        }
        cmprSize = rle.Compress(currInBuf, tempOutBuf.buf, currInBufSize, tempOutBuf.bufSize);
        Assert(cmprSize >= 0);
        if (cmprSize > 0 && cmprSize < currInBufSize) {
            rc = memcpy_s(out.buf, cmprSize, tempOutBuf.buf, cmprSize);
            securec_check(rc, "", "");
            out.sz = cmprSize;
            out.modes |= CU_RLECompressed;

            currInBuf = out.buf;
            currInBufSize = cmprSize;
        }
    }

    // Step3: try to apply LZ4 or Zlib according to CompressLevel
    // Apply different compression method for compressionLevel
    // COMPRESS_LOW:    delta compression | RleCoder
    // COMPRESS_MIDDLE: delta compression | RleCoder | LZ4
    // COMPRESS_HIGH:   delta compression | RleCoder | Zlib
    // We can skip LZ4/Zlib compression when level is COMPRESS_MIDDLE or COMPRESS_HIGH
    if (compression == COMPRESS_LOW) {
        BufferHelperFree(&tempOutBuf);

        // when delta compression is applied to, insert the min/max at the last step.
        // we think that it will reduce the compression ratio if all min/max data and
        // delta results are passed in and compacked.
        if (out.modes & CU_DeltaCompressed) {
            this->InsertMinMaxVal(out.buf, &out.sz);
        }

#ifdef USE_ASSERT_CHECKING
        IntegerCheckCompressedData(in.buf, in.sz, out.buf, out.sz, out.modes, this->m_eachValSize);
#endif
        return ((out.modes != 0) ? out.sz : 0);
    }

    if (compression == COMPRESS_MIDDLE) {
        LZ4Wrapper lz4;
        lz4.SetCompressionLevel(compresslevel_tables[compression][compresslevel]);
        boundSize = lz4.CompressGetBound(currInBufSize);
        if (boundSize > tempOutBuf.bufSize) {
            BufferHelperRemalloc(&tempOutBuf, boundSize);
        }
        cmprSize = lz4.Compress(currInBuf, tempOutBuf.buf, currInBufSize);
    } else if (compression == COMPRESS_HIGH) {
        ZlibEncoder zlib;
        zlib.Prepare(compresslevel_tables[compression][compresslevel]);

        boundSize = zlib.CompressGetBound(currInBufSize);
        if (boundSize > tempOutBuf.bufSize) {
            BufferHelperRemalloc(&tempOutBuf, boundSize);
        }

        bool done = false;
        zlib.Reset((unsigned char*)currInBuf, currInBufSize);
        cmprSize = zlib.Compress((unsigned char*)tempOutBuf.buf, (int)tempOutBuf.bufSize, done);
        if (!done) {
            cmprSize = 0;
        }
    }

    // if cmprSize is 0, we have to read data from input buffer.
    // if cmprSize > 0, check whether compression make benefits.
    Assert(cmprSize >= 0);
    if (cmprSize > 0 && cmprSize < currInBufSize) {
        Assert((Size)cmprSize <= tempOutBuf.bufSize);
        rc = memcpy_s(out.buf, cmprSize, tempOutBuf.buf, cmprSize);
        securec_check(rc, "", "");
        out.sz = cmprSize;
        out.modes |= ((compression == COMPRESS_MIDDLE) ? CU_LzCompressed : CU_ZlibCompressed);
    }

    BufferHelperFree(&tempOutBuf);

    // when delta compression is applied to, insert the min/max at the last step.
    // we think that it will reduce the compression ratio if all min/max data and
    // delta results are passed in and compacked.
    if (out.modes & CU_DeltaCompressed) {
        this->InsertMinMaxVal(out.buf, &out.sz);
    }
#ifdef USE_ASSERT_CHECKING
    IntegerCheckCompressedData(in.buf, in.sz, out.buf, out.sz, out.modes, this->m_eachValSize);
#endif
    return ((out.modes != 0) ? out.sz : 0);
}

/* enumerate possible instances */
template int IntegerCoder::CompressInner<true>(const CompressionArg1&, CompressionArg2&);
template int IntegerCoder::CompressInner<false>(const CompressionArg1&, CompressionArg2&);

/*
 * @Description: compress integer values
 * @IN in: input arguments
 * @OUT out: output arguments
 * @Return: 0 returned means that no data has been compressed.
 *    othersize returned value > 0, compressed data and its size within out.
 * @See also:
 */
int IntegerCoder::Compress(const CompressionArg1& in, CompressionArg2& out)
{
    if (m_adopt_rle) {
        return CompressInner<true>(in, out);
    } else {
        return CompressInner<false>(in, out);
    }
}

int IntegerCoder::Decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out)
{
    char* nextInBuf = NULL;
    int nextInSize = 0;
    uint16 modes = in.modes;
    short inValSize = 0;

    // if delta compression is applied to, the first <m_eachValSize> bytes means
    // the min value, and the second <m_eachValSize> bytes means the max value.
    if (0 != (modes & CU_DeltaCompressed)) {
        m_minVal = ConvertToInt64Data(in.buf, m_eachValSize);
        m_maxVal = ConvertToInt64Data(in.buf + m_eachValSize, m_eachValSize);
        nextInBuf = in.buf + m_eachValSize * 2;
        nextInSize = in.sz - m_eachValSize * 2;

        inValSize = DeltaGetBytesNum(m_minVal, m_maxVal);
    } else {
        nextInBuf = in.buf;
        nextInSize = in.sz;
    }

    // at most case, tmpBuf is always needed. so palloc() is called earlier.
    // maybe tmpBuf should be allocated later when it's mecessary, but it make the logic complex.
    BufferHelper tmpBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&tmpBuf, out.sz);

    char* nextOutBuf = out.buf;
    int nextOutSize = 0;

    // for multi-kind methods during decompressing, two output buffer are used.
    // this time, take buffer1 as the input, and buffer2 as the output.
    // the next time, buffer2 will be taken as the input and buffer1 taken as the output.
    // but first, the two buffers must be set rightly.
    bool preparedOk = false;

    if ((modes & CU_LzCompressed) != 0) {
        // either LZ4 or ZLIB compression is applied to INTEGER data.
        Assert(0 == (modes & CU_ZlibCompressed));

        LZ4Wrapper lzDecoder;
        Assert(lzDecoder.DecompressGetBound(nextInBuf) > 0 && lzDecoder.DecompressGetBound(nextInBuf) <= out.sz);
        nextOutSize = lzDecoder.Decompress(nextInBuf, nextOutBuf, nextInSize);
        Assert((nextOutSize > 0) && (nextOutSize <= out.sz));

        // prepare input buffer and output buffer for the next compression method
        prepareSwapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize, tmpBuf.buf, out.sz, preparedOk);
    } else if ((modes & CU_ZlibCompressed) != 0) {
        // either LZ4 or ZLIB compression is applied to INTEGER data.
        Assert((modes & CU_LzCompressed) == 0);

        ZlibDecoder zlibDecoder;
        nextOutSize = zlibDecoder.Prepare((unsigned char*)nextInBuf, nextInSize);
        if (nextOutSize == 0) {
            bool done = false;
            nextOutSize = zlibDecoder.Decompress((unsigned char*)nextOutBuf, out.sz, done, true);
            Assert(done && (nextOutSize > 0) && (nextOutSize <= out.sz));

            // prepare input buffer and output buffer for the next compression method
            prepareSwapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize, tmpBuf.buf, out.sz, preparedOk);
        } else {
            BufferHelperFree(&tmpBuf);
            return -2;
        }
    }

    if ((modes & CU_RLECompressed) != 0) {
        // case 1: both delta and rle methods are applied to, the value size is inValSize,
        //         which is the size of DELTA value.
        // case 2: only rle but delta methods is applied to, the value size is m_eachValSize,
        //         which is the size of raw value.
        short oneValSize = (modes & CU_DeltaCompressed) ? inValSize : m_eachValSize;

        RleCoder rle(oneValSize);
        nextOutSize = rle.Decompress(nextInBuf, nextOutBuf, nextInSize, out.sz);
        Assert(nextOutSize > nextInSize && nextOutSize <= out.sz);

        // prepare input buffer and output buffer for the next compression method
        if (preparedOk) {
            swapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize);
        } else {
            prepareSwapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize, tmpBuf.buf, out.sz, preparedOk);
        }
    }

    if ((modes & CU_DeltaCompressed) != 0) {
        DeltaCoder delta(m_minVal, m_eachValSize, false);
        nextOutSize = delta.Decompress(nextInBuf, nextOutBuf, nextInSize, out.sz, inValSize);
        Assert(nextOutSize > nextInSize && nextOutSize <= out.sz);

        // even this is the last decompression, this logic is the same to the above,
        // which make the returning logic simple and uniform.
        if (preparedOk) {
            swapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize);
        } else {
            prepareSwapBuf(nextInBuf, nextOutBuf, nextInSize, nextOutSize, tmpBuf.buf, out.sz, preparedOk);
        }
    }

    // FUTURE CASE: if two output buffers are provided, memcpy() will be reduced once
    Assert(nextInSize <= out.sz);
    if (nextInBuf != out.buf) {
        errno_t rc = memcpy_s(out.buf, nextInSize, nextInBuf, nextInSize);
        securec_check(rc, "", "");
    }

    BufferHelperFree(&tmpBuf);
    return nextInSize;
}

// before call this method, set m_intResults and m_nIntResults correctly.
// if dictionary compress fails, m_nIntResults must be set to 0.
int StringCoder::CompressNumbers(
    _in_ int max, _in_ int compressing_modes, __inout char* outBuf, _in_ int outBufSize, _out_ uint16& mode)
{
    // make sure output buffer' size is equal to or bigger than the input bufffer' size
    Assert(NULL != m_dicCodes && m_dicCodesNum > 0);
    Assert(outBufSize >= (int)(sizeof(DicCodeType) * m_dicCodesNum));

    CompressionArg1 input = {0};
    input.buf = (char*)m_dicCodes;
    input.sz = sizeof(DicCodeType) * m_dicCodesNum;
    input.mode = compressing_modes;

    CompressionArg2 output = {0};
    output.buf = outBuf;
    output.sz = outBufSize;

    // after dictionary compression, the results are during [0, max], so
    // the min-val is always 0 and the size of each value is uint16.
    IntegerCoder intCoder(sizeof(DicCodeType));
    intCoder.SetMinMaxVal(0, max);
    /* input a hint about RLE encoding */
    intCoder.m_adopt_rle = m_adopt_rle;
    int cmprSize = intCoder.Compress(input, output);
    if (cmprSize > 0) {
        // compress successfull, and set the compression mode.
        Assert(cmprSize <= outBufSize && cmprSize < input.sz);
        mode |= output.modes;
        return cmprSize;
    }

    // compress failed, copy the raw number data
    errno_t rc = memcpy_s(outBuf, input.sz, input.buf, input.sz);
    securec_check(rc, "", "");
    return input.sz;
}

void StringCoder::DecompressNumbers(
    _in_ char* inBuf, _in_ int inBufSize, _in_ uint16 mode, _out_ char* outBuf, _in_ int outBufSize)
{
    CompressionArg2 numberIn;
    numberIn.buf = inBuf;
    numberIn.sz = inBufSize;
    numberIn.modes = mode;

    CompressionArg1 numberOut;
    numberOut.buf = outBuf;
    numberOut.sz = outBufSize;

    IntegerCoder intDecoder(sizeof(DicCodeType));
    int uncmprSize = intDecoder.Decompress(numberIn, numberOut);
    Assert(uncmprSize > 0 && uncmprSize <= outBufSize);
    Assert((uncmprSize % sizeof(DicCodeType)) == 0);

    m_dicCodes = (DicCodeType*)palloc(uncmprSize);
    m_dicCodesNum = uncmprSize / sizeof(DicCodeType);
    errno_t rc = memcpy_s(m_dicCodes, uncmprSize, outBuf, uncmprSize);
    securec_check(rc, "", "");
}

int StringCoder::Compress(CompressionArg1& in, CompressionArg2& out)
{
    if (m_adopt_dict) {
        return CompressInner<true>(in, out);
    } else {
        return CompressInner<false>(in, out);
    }
}

template <bool adopt_dict>
int StringCoder::CompressInner(CompressionArg1& in, CompressionArg2& out)
{
    Assert(heaprel_get_compression_from_modes(in.mode) >= COMPRESS_LOW);
    Assert(heaprel_get_compression_from_modes(in.mode) <= COMPRESS_HIGH);
    Assert((in.buildGlobalDict && in.globalDict != NULL) || // case 1: to build global dictionary
            (in.useGlobalDict && in.globalDict != NULL) ||      // case 2: to compress by global dictionary
            (in.useDict && !in.useGlobalDict) ||                // case 3: to compress by local dictionary
            (!in.useDict && in.globalDict == NULL));            // case 4: no suitable for dictionary method

    int mode = 0;
    int cmprSize = 0;

    /* now only local dictionary encode is available */
    if (in.useDict && adopt_dict) {
        int dicCodeSize = 0;
        int boundSize = 0;
        DictHeader* dictHeader = NULL;

        // build local dictionary and compress
        DicCoder* dict = New(CurrentMemoryContext) DicCoder(in.buf, in.sz, in.numVals, 0);
        boundSize = dict->CompressGetBound();
        Assert((boundSize % sizeof(DicCodeType)) == 0);
        this->m_dicCodes = (DicCodeType*)palloc(boundSize);
        this->m_dicCodesNum = boundSize / sizeof(DicCodeType);

        dicCodeSize = dict->Compress((char*)this->m_dicCodes);
        dictHeader = dict->GetHeader();
        // Only dictionary method make benefits to raw data, number compression continues.
        // that will exclude the special condition, in which dictionary' size plus compressed
        // number's size is also smaller than raw data' size.
        if ((dicCodeSize > 0) && (dictHeader->m_totalSize > 0) &&
            (dicCodeSize + (int)dictHeader->m_totalSize < in.sz)) {
            // Results Part I: dictionary data
            Assert(dicCodeSize == (int)(sizeof(DicCodeType) * this->m_dicCodesNum));
            Assert(dictHeader && ((int)dictHeader->m_totalSize < out.sz));
            errno_t rc = memcpy_s(out.buf, dictHeader->m_totalSize, (char*)dictHeader, dictHeader->m_totalSize);
            securec_check(rc, "", "");

            out.modes |= CU_DicEncode; /* global dict mode: (CU_DicEncode | CU_GlobalDic) */

            // Results Part II: Number Data
            //   after dictionary compression, the minval is alwarys 0,
            //   and the maxval is dict-items count subtracted 1.
            //   the same level to dict compression is used to compress numbers.
            cmprSize = (int)dictHeader->m_totalSize + this->CompressNumbers((dictHeader->m_itemsCount - 1), in.mode,
                                                                            (out.buf + dictHeader->m_totalSize), (out.sz - dictHeader->m_totalSize), out.modes);
        }

        /* destroy unused object */
        delete dict;
        pfree(this->m_dicCodes);
        this->m_dicCodes = NULL;

        // dictionary compress successful
        if (cmprSize > 0) {
            return cmprSize;
        }
    }

    /* one of the followings,
     * 1. dictionary compress fails, degrade to lz4/zlib compression method
     * 2. caller give the hint which don't adopt dictionary compression.
     */
    cmprSize = this->CompressWithoutDict(in.buf, in.sz, in.mode, out.buf, out.sz, mode);
    if (cmprSize > 0 && cmprSize < in.sz) {
        out.modes |= mode;
        return cmprSize;
    }
    return 0;
}

/* enumerate possible instances */
template int StringCoder::CompressInner<true>(CompressionArg1&, CompressionArg2&);
template int StringCoder::CompressInner<false>(CompressionArg1&, CompressionArg2&);

// compress directly using zlib/lz4 methods
int StringCoder::CompressWithoutDict(_in_ char* inBuf, _in_ int inBufSize, _in_ int compressing_modes,
                                     _out_ char* outBuf, _in_ int outBufSize, _out_ int& mode)
{
    int8 compression = heaprel_get_compression_from_modes(compressing_modes);
    int8 compresslevel = heaprel_get_compresslevel_from_modes(compressing_modes);
    int boundSize = 0;
    int outSize = 0;
    int tempMode = 0;
    BufferHelper tempOutBuf = {NULL, 0, Unknown};

    if (compression == COMPRESS_HIGH) {
        ZlibEncoder zlib;
        zlib.Prepare(compresslevel_tables[compression][compresslevel]);

        boundSize = zlib.CompressGetBound(inBufSize);
        if (boundSize <= outBufSize) {
            tempOutBuf.buf = outBuf;
            tempOutBuf.bufSize = outBufSize;
        } else {
            BufferHelperMalloc(&tempOutBuf, boundSize);
        }

        bool done = false;
        zlib.Reset((unsigned char*)inBuf, inBufSize);
        outSize = zlib.Compress((unsigned char*)tempOutBuf.buf, (int)tempOutBuf.bufSize, done);
        if (done) {
            tempMode = CU_ZlibCompressed;
        } else {
            outSize = 0;
        }
    } else if (compression == COMPRESS_MIDDLE || compression == COMPRESS_LOW) {
        LZ4Wrapper lz4;
        lz4.SetCompressionLevel(compresslevel_tables[compression][compresslevel]);

        boundSize = lz4.CompressGetBound(inBufSize);
        if (boundSize <= outBufSize) {
            tempOutBuf.buf = outBuf;
            tempOutBuf.bufSize = outBufSize;
        } else {
            BufferHelperMalloc(&tempOutBuf, boundSize);
        }
        outSize = lz4.Compress(inBuf, tempOutBuf.buf, inBufSize);
        tempMode = CU_LzCompressed;
    }

    // compress successfully, compressed data' size is returned.
    // rewrite the compressed data into output buffer if necessary.
    if (outSize > 0 && outSize < inBufSize && outSize <= outBufSize) {
        errno_t rc;
        if (outBuf != tempOutBuf.buf) {
            rc = memcpy_s(outBuf, outSize, tempOutBuf.buf, outSize);
            securec_check(rc, "", "");
        }
        mode |= tempMode;
    }
    if (tempOutBuf.buf != outBuf) {
        BufferHelperFree(&tempOutBuf);
    }

    return outSize;
}

int StringCoder::DecompressWithoutDict(
    _in_ char* inBuf, _in_ int inBufSize, _in_ uint16 mode, _out_ char* outBuf, _out_ int outBufSize)
{
    int outSize = 0;
    if (mode & CU_ZlibCompressed) {
        Assert((mode & CU_LzCompressed) == 0);

        ZlibDecoder zlibDecoder;
        outSize = zlibDecoder.Prepare((unsigned char*)inBuf, inBufSize);
        if (outSize == Z_OK) {
            bool done = false;
            outSize = zlibDecoder.Decompress((unsigned char*)outBuf, outBufSize, done, true);
            Assert(done && (outSize > 0) && (outSize <= outBufSize));
        } else {
            // just only the memory is not enough when outSize isn't Z_OK
            return -2;
        }
    } else if (mode & CU_LzCompressed) {
        Assert((mode & CU_ZlibCompressed) == 0);

        LZ4Wrapper lzDecoder;
        Assert(lzDecoder.DecompressGetBound(inBuf) > 0 && lzDecoder.DecompressGetBound(inBuf) <= outBufSize);
        outSize = lzDecoder.Decompress(inBuf, outBuf, inBufSize);
        Assert((outSize > 0) && (outSize <= outBufSize));
    }

    return outSize;
}

int StringCoder::Decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out)
{
    // case 1: dictionary method is not applied to, so use lz4/zlib directly to decompress
    if ((in.modes & CU_DicEncode) == 0) {
        return DecompressWithoutDict(in.buf, in.sz, in.modes, out.buf, out.sz);
    }

    // case 2: dictionary method is first applied to, so read and parse the dictionary
    DicCoder* dict = New(CurrentMemoryContext) DicCoder(in.buf);
    DictHeader* dictHeader = dict->GetHeader();
    DecompressNumbers(in.buf + dictHeader->m_totalSize, in.sz - dictHeader->m_totalSize, in.modes, out.buf, out.sz);
    int outSize = dict->Decompress((char*)m_dicCodes, m_dicCodesNum * sizeof(DicCodeType), out.buf, out.sz);
    delete dict;

    if (m_dicCodes) {
        pfree(m_dicCodes);
        m_dicCodes = NULL;
    }

    return outSize;
}

///
/// DeltaPlusRLEv2 Implements
///
#ifdef USE_ASSERT_CHECKING

/// check the compressed data.
static void DeltaPlusRLEv2_Check(char* rawData, char* cmprData, int cmprSize, int64 minVal, int nValues, uint16 modes,
                                 short orgValSize, short dltDataSize)
{
    /// don't care max-value.
    DeltaPlusRLEv2 deltaPlusRle(minVal, orgValSize, nValues, orgValSize);

    if (modes & CU_DeltaCompressed)
        deltaPlusRle.SetDltValSize(dltDataSize);

    /// until here rawData and cmprData are not null.
    int rawSize = int(orgValSize * nValues);
    char* realData = (char*)palloc(rawSize);
    int realSize = deltaPlusRle.Decompress((char**)&realData, rawSize, cmprData, cmprSize, modes);

    Assert(realSize == rawSize);
    Assert(memcmp(realData, rawData, realSize) == 0);

    pfree(realData);
}
#endif

/// get the bound size needed.
/// the returned value is available for both delta and rle methods.
int DeltaPlusRLEv2::GetBound()
{
    if (DeltaCanBeApplied(&m_deltaBytes, m_minVal, m_maxVal, m_valueBytes)) {
        Assert(RleGetBound2(m_nValues, m_deltaBytes) > DeltaGetBound(m_nValues, m_deltaBytes));
        return (int)RleGetBound2(m_nValues, m_deltaBytes);
    }
    return (int)RleGetBound2(m_nValues, m_valueBytes);
}

/// if Delta method is used, we don't store min/max values
/// in compressed block. it's your responsibility to remember
/// these information.
/// if RLE methos is used, the space *out* points to will be
/// freed, and then set a new valid memory address.
int DeltaPlusRLEv2::Compress(char** out, int outSize, uint16* outModes, char* in, int inSize)
{
    if (m_adopt_rle) {
        return CompressInner<true>(out, outSize, outModes, in, inSize);
    } else {
        return CompressInner<false>(out, outSize, outModes, in, inSize);
    }
}

template <bool adopt_rle>
int DeltaPlusRLEv2::CompressInner(char** out, int outSize, uint16* outModes, char* in, int inSize)
{
    int deltaCmprSize = 0;
    int rleCmprSize = 0;

    /// clean *outModes* value.
    *outModes = 0;

    if (this->m_deltaBytes < this->m_valueBytes) {
        /// OK, Delta method is used to.
        DeltaCoder delta(this->m_minVal, this->m_maxVal, true);
        deltaCmprSize = delta.Compress(in, *out, inSize, outSize, this->m_valueBytes);
        *outModes |= CU_DeltaCompressed;
        Assert(deltaCmprSize > 0);

        if (adopt_rle) {
            /// until here, we have to use a temp memory.
            RleCoder rle(this->m_deltaBytes, RleCoder::RleMinRepeats_v2[this->m_deltaBytes]);
            int rleBoundSize = rle.CompressGetBound(deltaCmprSize);
            char* rleBoundBuf = (char*)palloc(rleBoundSize);
            rleCmprSize = rle.Compress(*out, rleBoundBuf, deltaCmprSize, rleBoundSize);
            if (rleCmprSize > 0 && rleCmprSize < deltaCmprSize) {
                /// first free the unused *out* space.
                pfree(*out);
                /// set the new space to return.
                *out = rleBoundBuf;
                rleBoundBuf = NULL;
                *outModes |= CU_RLECompressed;

#ifdef USE_ASSERT_CHECKING
                DeltaPlusRLEv2_Check(in, *out, rleCmprSize,
                                     this->m_minVal, this->m_nValues, *outModes,
                                     this->m_valueBytes, this->m_deltaBytes);
#endif
                return rleCmprSize;
            } else {
                /// RLE compress fails.
                pfree(rleBoundBuf);
                rleBoundBuf = NULL;
            }
        }

#ifdef USE_ASSERT_CHECKING
        DeltaPlusRLEv2_Check(in, *out, deltaCmprSize,
                             this->m_minVal, this->m_nValues, *outModes,
                             this->m_valueBytes, this->m_deltaBytes);
#endif
        return deltaCmprSize;
    }

    if (adopt_rle) {
        /* OK, we will try to apply RLE method only. */
        RleCoder rle(this->m_valueBytes, RleCoder::RleMinRepeats_v2[this->m_valueBytes]);
        rleCmprSize = rle.Compress(in, *out, inSize, outSize);
        if (rleCmprSize > 0 && rleCmprSize < inSize) {
            /* compressed data are in *out buffer, so we don't do anything extra. */
            *outModes |= CU_RLECompressed;

#ifdef USE_ASSERT_CHECKING
            DeltaPlusRLEv2_Check(in, *out, rleCmprSize,
                                 this->m_minVal, this->m_nValues, *outModes,
                                 this->m_valueBytes, this->m_deltaBytes);
#endif
            return rleCmprSize;
        }
    }

    /* compress fails; */
    return 0;
}

/* enumerate possible instances */
template int DeltaPlusRLEv2::CompressInner<true>(char**, int, uint16*, char*, int);
template int DeltaPlusRLEv2::CompressInner<false>(char**, int, uint16*, char*, int);

int DeltaPlusRLEv2::Decompress(char** out, int outSize, char* in, int inSize, uint16 modes)
{
    char* dltIn = in;
    char* dltOut = *out;
    int dltInSize = inSize;
    int dltOutSize = outSize;

    int rleOrigSize = 0;
    int dltOrigSize = 0;

    Assert(modes != 0);  /// don't handle the uncompressed data.
    if (modes & CU_DeltaCompressed) {
        Assert(m_deltaBytes < m_valueBytes);
    } else {
        Assert(m_deltaBytes == m_valueBytes);
    }

    if (modes & CU_RLECompressed) {
        /// step 1: rle decompress
        RleCoder rle(m_deltaBytes, RleCoder::RleMinRepeats_v2[m_deltaBytes]);
        rleOrigSize = rle.Decompress(in, *out, inSize, outSize);
        Assert(rleOrigSize == m_nValues * m_deltaBytes);
        Assert(rleOrigSize <= outSize);

        if ((modes & CU_DeltaCompressed) == 0) {
            /// only RLE is used, so return early.
            return rleOrigSize;
        }

        /// step 2: set the arguments for Delta decompression.
        dltIn = *out;
        dltInSize = rleOrigSize;
        dltOutSize = m_valueBytes * m_nValues;
        dltOut = (char*)palloc(dltOutSize);
    }

    Assert((modes & CU_DeltaCompressed) != 0);

    /// step 1: delta decompress
    DeltaCoder dlt(m_minVal, m_valueBytes, false);
    dltOrigSize = dlt.Decompress(dltIn, dltOut, dltInSize, dltOutSize, m_deltaBytes);
    Assert(dltOrigSize > 0 && dltOrigSize <= dltOutSize);
    Assert(dltOrigSize == m_valueBytes * m_nValues);

    /// step 2: exchang memory pointer with each other in
    ///   order to reduce the number of memcpy() calls.
    if (dltOut != *out) {
        pfree(*out);
        *out = dltOut;
    }
    return dltOrigSize;
}

/// compute the numbers of int64 values, int32 values,
/// and the failed numerics.
static void NumericDecompressCount(numerics_statistics_out* inStat)
{
    char* ascales = inStat->ascale_codes;
    int total = inStat->n_notnull;
    int nInt64 = 0;
    int nInt32 = 0;

    for (int i = 0; i < total; ++i) {
        char scale = *ascales++;
        if (scale > MAX_INT32_ASCALE_ENCODING) {
            Assert(scale <= MAX_INT64_ASCALE_ENCODING);
            ++nInt64;
        } else if (scale > FAILED_ENCODING) {
            ++nInt32;
        } else {
            Assert(scale == FAILED_ENCODING);
        }
    }
    inStat->int64_n_values = nInt64;
    inStat->int32_n_values = nInt32;
    inStat->failed_cnt = total - nInt64 - nInt32;
}

/*
 * @Description: compute the numbers of int64 values, int32 values and the failed numerics.
 * @IN/OUT inStat: numeric statisticss
 */
static void NumericDecompressDscaleCount(numerics_statistics_out* inStat)
{
    char* ascales = inStat->ascale_codes;
    int total = inStat->n_notnull;
    int nInt64 = 0;
    int nInt32 = 0;
    Assert(total <= DefaultFullCUSize);

    for (int i = 0; i < total; ++i) {
        char scale = *ascales++;
        if (scale == INT32_DSCALE_ENCODING) {
            ++nInt32;
        } else if (scale == INT64_DSCALE_ENCODING) {
            ++nInt64;
        } else {
            Assert(scale == NUMERIC_DSCALE_ENCODING);
        }
    }
    inStat->int64_n_values = nInt64;
    inStat->int32_n_values = nInt32;
    inStat->failed_cnt = total - nInt64 - nInt32;
}

/// Given the two values, (*valueIfSet* is for bit=1 and
/// *valueIfUnset* is for bit=0), and the bitmap, then parse
/// this map and fill the values into *values* array whose size
/// is *nValues*.
template <typename T>
static int ParseBitmap(char* buf, T* values, int nValues, T valueIfSet, T valueIfUnset)
{
    const uint8 bitmask[] = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
    const uint8* pmask = bitmask;
    const uint8* bytes = (const uint8*)buf;

    /// whole/complete bytes used for this bitmap
    const uint32 bpWholeBytes = (uint32)nValues >> 3;
    /// available bits in the last part-byte for this bitmap
    /// it may be 0 or others less than 8.
    const uint32 bpRemainBits = (uint32)nValues & 0x07;

    int nUnsetValues = 0;
    uint8 eachByte = 0;

    Assert((uint32)nValues == (bpRemainBits + (bpWholeBytes << 3)));

    for (uint32 i = 0; i < bpWholeBytes; ++i) {
        eachByte = *bytes++;
        values[0] = (eachByte & bitmask[0]) ? valueIfSet : valueIfUnset;
        values[1] = (eachByte & bitmask[1]) ? valueIfSet : valueIfUnset;
        values[2] = (eachByte & bitmask[2]) ? valueIfSet : valueIfUnset;
        values[3] = (eachByte & bitmask[3]) ? valueIfSet : valueIfUnset;
        values[4] = (eachByte & bitmask[4]) ? valueIfSet : valueIfUnset;
        values[5] = (eachByte & bitmask[5]) ? valueIfSet : valueIfUnset;
        values[6] = (eachByte & bitmask[6]) ? valueIfSet : valueIfUnset;
        values[7] = (eachByte & bitmask[7]) ? valueIfSet : valueIfUnset;
        values += 8;
        nUnsetValues += NumberOfBit1Set[eachByte];
    }

    Assert(bpRemainBits <= 7);
    eachByte = *bytes;
    switch (bpRemainBits) {
        case 7:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 6:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 5:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 4:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 3:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 2:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
        /* fall-through */
        case 1:
            *values++ = (eachByte & (*pmask++)) ? valueIfSet : valueIfUnset;
            /// count the number of FALSE value.
            nUnsetValues += NumberOfBit1Set[eachByte];
            break;
        case 0:
        default:
            /// nothing to do
            break;
    }
    return nValues - nUnsetValues;
}

#ifdef ENABLE_UT
/// using for numeric ascale format , only for UT
/// use 3 bits to remember each ascale code. and at last
/// return the used size.
static int CompactAscaleCodesBy3Bits(uint8* outBuf, uint8* inValues, int nInValues)
{
    uint8* pack = outBuf;                /// each packed byte
    const int groups = nInValues >> 3;   /// treat 8 values as a group
    const int remain = nInValues & 0x7;  /// left out

    for (int i = 0; i < groups; ++i) {
        /// 3 bits + 3 bits + 2 bits
        *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
        /// 1 bit + 3bits + 3 bits + 1 bit
        *pack++ = ((inValues[2] & 0x1) << 7) | (inValues[3] << 4) | (inValues[4] << 1) | ((inValues[5] & 0x4) >> 2);
        /// 2 bits + 3 bits + 3 bits
        *pack++ = ((inValues[5] & 0x3) << 6) | (inValues[6] << 3) | inValues[7];
        inValues += 8;
    }

    switch (remain) {
        case 7:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
            *pack++ = ((inValues[2] & 0x1) << 7) | (inValues[3] << 4) | (inValues[4] << 1) | ((inValues[5] & 0x4) >> 2);
            *pack++ = ((inValues[5] & 0x3) << 6) | (inValues[6] << 3);
            break;
        case 6:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
            *pack++ = ((inValues[2] & 0x1) << 7) | (inValues[3] << 4) | (inValues[4] << 1) | ((inValues[5] & 0x4) >> 2);
            *pack++ = ((inValues[5] & 0x3) << 6);
            break;
        case 5:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
            *pack++ = ((inValues[2] & 0x1) << 7) | (inValues[3] << 4) | (inValues[4] << 1);
            break;
        case 4:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
            *pack++ = ((inValues[2] & 0x1) << 7) | (inValues[3] << 4);
            break;
        case 3:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2) | (inValues[2] >> 1);
            *pack++ = ((inValues[2] & 0x1) << 7);
            break;
        case 2:
            *pack++ = (inValues[0] << 5) | (inValues[1] << 2);
            break;
        case 1:
            *pack++ = (inValues[0] << 5);
            break;
        case 0:
        default:
            /// nothing to do
            break;
    }
    return pack - outBuf;
}
#endif  // ENABLE_UT

static inline void ParseAscaleCodes3Bytes(uint8*& in, uint8*& out)
{
    if (in != nullptr && out != nullptr) {
        uint8 pack1 = *in++;
        uint8 pack2 = *in++;
        uint8 pack3 = *in++;

        *out++ = (pack1 >> 5);
        *out++ = (pack1 >> 2) & 0x07;
        *out++ = ((pack1 & 0x3) << 1) | ((pack2 & 0x80) >> 7);
        *out++ = (pack2 >> 4) & 0x07;
        *out++ = (pack2 >> 1) & 0x07;
        *out++ = ((pack2 & 0x1) << 2) | ((pack3 & 0xc0) >> 6);
        *out++ = (pack3 >> 3) & 0x07;
    }
}

/// decode the 3-bits map, and at last return map size.
static int ParseAscaleCodesBy3Bits(uint8* inBuf, uint8* outValues, int nOutValues)
{
    uint8* in = inBuf;
    uint8* out = outValues;
    const uint32 groups = (uint32)nOutValues >> 3;
    const uint32 remain = (uint32)nOutValues & 0x7;
    uint8 pack1 = 0;
    uint8 pack2 = 0;
    uint8 pack3 = 0;

    for (uint32 i = 0; i < groups; ++i) {
        ParseAscaleCodes3Bytes(in, out);
        *out++ = (pack3 & 0x07);
    }

    switch (remain) {
        case 7:
            /// there is also 3 bytes needed.
            ParseAscaleCodes3Bytes(in, out);
            break;
        case 6:
            /// there is also 3 bytes needed.
            pack1 = *in++;
            pack2 = *in++;
            pack3 = *in++;

            *out++ = (pack1 >> 5);
            *out++ = (pack1 >> 2) & 0x07;
            *out++ = ((pack1 & 0x3) << 1) | ((pack2 & 0x80) >> 7);
            *out++ = (pack2 >> 4) & 0x07;
            *out++ = (pack2 >> 1) & 0x07;
            *out++ = ((pack2 & 0x1) << 2) | ((pack3 & 0xc0) >> 6);
            break;
        case 5:
            /// there is also 2 bytes needed.
            pack1 = *in++;
            pack2 = *in++;
            *out++ = (pack1 >> 5);
            *out++ = (pack1 >> 2) & 0x07;
            *out++ = ((pack1 & 0x3) << 1) | ((pack2 & 0x80) >> 7);
            *out++ = (pack2 >> 4) & 0x07;
            *out++ = (pack2 >> 1) & 0x07;
            break;
        case 4:
            /// there is also 2 bytes needed.
            pack1 = *in++;
            pack2 = *in++;
            *out++ = (pack1 >> 5);
            *out++ = (pack1 >> 2) & 0x07;
            *out++ = ((pack1 & 0x3) << 1) | ((pack2 & 0x80) >> 7);
            *out++ = (pack2 >> 4) & 0x07;
            break;
        case 3:
            /// there is also 2 bytes needed.
            pack1 = *in++;
            pack2 = *in++;
            *out++ = (pack1 >> 5);
            *out++ = (pack1 >> 2) & 0x07;
            *out++ = ((pack1 & 0x3) << 1) | ((pack2 & 0x80) >> 7);
            break;
        case 2:
            /// there is also 1 byte needed.
            pack1 = *in++;
            *out++ = (pack1 >> 5);
            *out++ = (pack1 >> 2) & 0x07;
            break;
        case 1:
            /// there is also 1 byte needed.
            pack1 = *in++;
            *out++ = (pack1 >> 5);
            break;
        case 0:
        default:  /// nothing to do
            break;
    }
    return in - inBuf;
}

#ifdef ENABLE_UT
// using for numeric ascale format , only for UT
// compact two codes into a byte which are from adjusted scales,
// and fill the output buffer.
static void CompactAscaleCodesBy4Bits(uint8* outBuf, uint8* inValues, int nInValues)
{
    // treat every 8 values as a group
    const int groups = nInValues >> 3;
    // remains exluding the last if odd is true
    const int remain = nInValues & 0x06;
    // handle the last value in special
    const bool lastIsOdd = ((nInValues & 0x01) == 0x01);

    for (int i = 0; i < groups; ++i, inValues += 8) {
        *outBuf++ = (inValues[0] << 4) | inValues[1];
        *outBuf++ = (inValues[2] << 4) | inValues[3];
        *outBuf++ = (inValues[4] << 4) | inValues[5];
        *outBuf++ = (inValues[6] << 4) | inValues[7];
    }

    Assert(remain == 0 || remain == 2 || remain == 4 || remain == 6);
    switch (remain) {
        case 6:
            *outBuf++ = (inValues[0] << 4) | inValues[1];
            *outBuf++ = (inValues[2] << 4) | inValues[3];
            *outBuf++ = (inValues[4] << 4) | inValues[5];
            inValues += 6;
            break;
        case 4:
            *outBuf++ = (inValues[0] << 4) | inValues[1];
            *outBuf++ = (inValues[2] << 4) | inValues[3];
            inValues += 4;
            break;
        case 2:
            *outBuf++ = (inValues[0] << 4) | inValues[1];
            inValues += 2;
            break;
        case 0:
        default:
            break;
    }

    if (lastIsOdd) {
        *outBuf = (*inValues) << 4;
    }
}
#endif  // ENABLE_UT

static void ParseAscaleCodesBy4Bits(numerics_statistics_out* out, uint8* inBuf, uint8* outValues, int nOutValues)
{
    /// treat every 8 values as a group
    const uint32 groups = (uint32)nOutValues >> 3;
    /// remains exluding the last if odd is true
    const uint32 remain = (uint32)nOutValues & 0x06;
    /// handle the last value in special
    const bool lastIsOdd = (((uint32)nOutValues & 0x01) == 0x01);

#define DecodeAndAdvance()                     \
    do {                                       \
        *outValues++ = ((*inBuf & 0xF0) >> 4); \
        *outValues++ = ((*inBuf++) & 0x0F);    \
    } while (0)

    for (uint32 i = 0; i < groups; ++i) {
        DecodeAndAdvance();
        DecodeAndAdvance();
        DecodeAndAdvance();
        DecodeAndAdvance();
    }

    Assert(remain == 0 || remain == 2 || remain == 4 || remain == 6);
    switch (remain) {
        case 6:
            DecodeAndAdvance();
        /* fall-through */
        case 4:
            DecodeAndAdvance();
        /* fall-through */
        case 2:
            DecodeAndAdvance();
        /* fall-through */
        case 0:
        default:
            /// nothing to do
            break;
    }

    if (lastIsOdd) {
        *outValues = ((*inBuf & 0xF0) >> 4);
    }
}

/*
 * @Description: parse dscale from compacted  data
 * @IN inBuf: compacted bytes of dscale
 * @OUT nOutValues: number of compacted dscale
 * @IN outValues: number of dscale
 * @Return: size of dest buffer used
 */
static int ParseDscaleCodesBy2Bits(uint8* inBuf, uint8* outValues, int nOutValues)
{
    uint8* startPos = inBuf;
    // treat every 4 values as a group
    const uint32 groups = (uint32)nOutValues >> 2;
    const uint32 remain = (uint32)nOutValues & 0x03;

    for (uint32 i = 0; i < groups; ++i) {
        *outValues++ = (*inBuf >> 6) & 0x03;
        *outValues++ = (*inBuf >> 4) & 0x03;
        *outValues++ = (*inBuf >> 2) & 0x03;
        *outValues++ = *inBuf & 0x03;
        ++inBuf;
    }

    switch (remain) {
        case 3:
            *outValues++ = (*inBuf >> 6) & 0x03;
            *outValues++ = (*inBuf >> 4) & 0x03;
            *outValues++ = (*inBuf >> 2) & 0x03;
            ++inBuf;
            break;
        case 2:
            *outValues++ = (*inBuf >> 6) & 0x03;
            *outValues++ = (*inBuf >> 4) & 0x03;
            ++inBuf;
            break;
        case 1:
            *outValues++ = (*inBuf >> 6) & 0x03;
            ++inBuf;
            break;
        default:
            // nothing to do
            break;
    }

    return inBuf - startPos;
}

template <bool hasNull, bool allAreInteger>
static void NumericStatistics(numerics_statistics_out* numStatic, Datum* batchValues, char* batchNulls, int batchRows,
                              SetNullNumericFunc SetNullFunc, void* memobj)
{
    bool* success = numStatic->success;
    char* ascaleCodes = numStatic->ascale_codes;
    int64* int64Values = numStatic->int64_values;
    int32* int32Values = numStatic->int32_values;

    int64 int64MinVal = 0;
    int64 int64MaxVal = 0;
    int32 int32MinVal = 0;
    int32 int32MaxVal = 0;
    int nSuccess = 0;
    int failedSize = 0;
    int nInt64Values = 0;
    int nInt32Values = 0;
    int nNotNulls = 0;
    int origTotalSize = 0;
    bool int32AscalesAreSame = true;
    bool int64AscalesAreSame = true;
    char int32PrevAsacle = 0;
    char int64PrevAsacle = 0;

    char* srcValue = NULL;

    for (int i = 0; i < batchRows; ++i) {
        if (hasNull && bitmap_been_set(batchNulls, i)) {
            SetNullFunc(i, memobj);
            Assert(!success[i]);  // treat as failed.
            continue;
        }

        srcValue = (char*)DatumGetPointer(batchValues[i]);
        if (allAreInteger || success[i]) {
            int64 thisValue = int64Values[nSuccess];
            char thisScale = ascaleCodes[nNotNulls];
            ++nSuccess;

            // decompress  size is numeric with bigint64
            origTotalSize += NUMERIC_64SZ;

            if (thisValue >= INT32_MIN_VALUE && thisValue <= INT32_MAX_VALUE) {
                if (nInt32Values > 0) {
                    // update int32 min/max values
                    if (thisValue < int32MinVal) {
                        int32MinVal = thisValue;
                    } else if (thisValue > int32MaxVal) {
                        int32MaxVal = thisValue;
                    }

                    /* dscale is always the same */
                    Assert((thisScale == int32PrevAsacle));
                } else {
                    // init int32 min/max values
                    int32MinVal = int32MaxVal = thisValue;
                    int32PrevAsacle = thisScale;
                }

                // remember int32 value and its status codes.
                int32Values[nInt32Values++] = thisValue;
                ascaleCodes[nNotNulls] = INT32_DSCALE_ENCODING;  // 2 bit encoding
            } else {
                if (nInt64Values > 0) {
                    // update int64 min/max values
                    if (thisValue < int64MinVal) {
                        int64MinVal = thisValue;
                    } else if (thisValue > int64MaxVal) {
                        int64MaxVal = thisValue;
                    }

                    /* dscale is always the same */
                    Assert(thisScale == int64PrevAsacle);
                } else {
                    // init int64 min/max values
                    int64MinVal = int64MaxVal = thisValue;
                    int64PrevAsacle = thisScale;
                }

                // remember int64 value and its status codes.
                int64Values[nInt64Values++] = thisValue;
                ascaleCodes[nNotNulls] = INT64_DSCALE_ENCODING;  // 2 bit encoding
            }
        } else {
            Assert(false == allAreInteger);
            failedSize += VARSIZE_ANY(srcValue);
            ascaleCodes[nNotNulls] = NUMERIC_DSCALE_ENCODING;  // 2 bit encoding
        }
        ++nNotNulls;
    }

    // fill the output info
    numStatic->failed_size = failedSize;
    numStatic->n_notnull = nNotNulls;
    numStatic->failed_cnt = nNotNulls - nInt32Values - nInt64Values;
    numStatic->original_size = origTotalSize + failedSize;

    numStatic->int64_minval = int64MinVal;
    numStatic->int64_maxval = int64MaxVal;
    numStatic->int64_n_values = nInt64Values;
    numStatic->int64_values_are_same = (nInt64Values > 0 && int64MinVal == int64MaxVal);
    numStatic->int64_ascales_are_same = int64AscalesAreSame;
    numStatic->int64_same_ascale = int64PrevAsacle;

    numStatic->int32_minval = int32MinVal;
    numStatic->int32_maxval = int32MaxVal;
    numStatic->int32_n_values = nInt32Values;
    numStatic->int32_values_are_same = (nInt32Values > 0 && int32MinVal == int32MaxVal);
    numStatic->int32_ascales_are_same = int32AscalesAreSame;
    numStatic->int32_same_ascale = int32PrevAsacle;
}

template <bool hasNull>
static void NumericCompressEachValuesPart(
    numerics_cmprs_out* out, numerics_statistics_out* in, Datum* batchValues, char* batchNulls, int batchRows)
{
    // try to compress int64 values with DELTA && RLE.
    if (in->int64_n_values > 0 && in->int64_minval != in->int64_maxval) {
        DeltaPlusRLEv2 deltaPlusRle(in->int64_minval, in->int64_maxval, in->int64_n_values, sizeof(int64));

        CompressionArg2* pOutArg = &out->int64_out_args;
        pOutArg->sz = deltaPlusRle.GetBound();
        // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
        pOutArg->buf = (char*)palloc(pOutArg->sz + 8);
        pOutArg->modes = 0;

        /* input a hint whether RLE should be adopted */
        deltaPlusRle.m_adopt_rle = out->filter->m_adopt_numeric2int_int64_rle;

        out->int64_cmpr_size = deltaPlusRle.Compress(
                                   &pOutArg->buf, pOutArg->sz, &pOutArg->modes, (char*)in->int64_values, sizeof(int64) * in->int64_n_values);
    }

    // try to compress int32 values with DELTA && RLE.
    if (in->int32_n_values > 0 && in->int32_minval != in->int32_maxval) {
        DeltaPlusRLEv2 deltaPlusRle(in->int32_minval, in->int32_maxval, in->int32_n_values, sizeof(int32));

        CompressionArg2* pOutArg = &out->int32_out_args;
        pOutArg->sz = deltaPlusRle.GetBound();
        // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
        pOutArg->buf = (char*)palloc(pOutArg->sz + 8);
        pOutArg->modes = 0;

        /* input a hint whether RLE should be adopted */
        deltaPlusRle.m_adopt_rle = out->filter->m_adopt_numeric2int_int32_rle;

        out->int32_cmpr_size = deltaPlusRle.Compress(
                                   &pOutArg->buf, pOutArg->sz, &pOutArg->modes, (char*)in->int32_values, sizeof(int32) * in->int32_n_values);
    }

    // try to compress the other numerics
    if (in->failed_cnt > 0) {
        char* numeric_ptr = NULL;
        char* ptr = NULL;
        bool* success = NULL;
        int numeric_len = 0;
        int rc = 0;

        out->other_srcbuf = (char*)palloc(in->failed_size);
        ptr = out->other_srcbuf;
        success = in->success;
        for (int i = 0; i < batchRows; ++i) {
            if (*success++ || (hasNull && bitmap_been_set(batchNulls, i))) {
                continue;
            }

            numeric_ptr = DatumGetPointer(batchValues[i]);
            numeric_len = VARSIZE_ANY(numeric_ptr);
            rc = memcpy_s(ptr, numeric_len, numeric_ptr, numeric_len);
            securec_check(rc, "", "");
            ptr += numeric_len;
        }
        Assert(in->failed_size == (int)(ptr - out->other_srcbuf));

        LZ4Wrapper lz4;
        int other_outbuf_sz = 0;
        lz4.SetCompressionLevel(compresslevel_tables[COMPRESS_LOW][0]);
        other_outbuf_sz = lz4.CompressGetBound((int)in->failed_size);
        out->other_outbuf = (char*)palloc(other_outbuf_sz);
        out->other_cmpr_size = lz4.Compress(out->other_srcbuf, out->other_outbuf, in->failed_size);
        out->other_apply_lz4 = (out->other_cmpr_size > 0 && out->other_cmpr_size < (int)in->failed_size);
    }
}

// compute the total size needed for this compressed numeric data.
static inline int NumericCompressGetTotalSize(numerics_statistics_out* phase1_out, numerics_cmprs_out* phase2_out, int align_size)
{
    int total_size = NUMERIC_FLAG_SIZE                             // head flags info
                     + align_size                              // extra for padding
                     + sizeof(int)                                 // original size info
                     + sizeof(int64) * 2                           // the same int64 value or min/max values
                     + sizeof(int32)                               // int64 compressed size
                     + sizeof(char)                                // the same int64 scale
                     + sizeof(int32) * 2                           // the same int32 value or min/max values
                     + sizeof(int32)                               // int32 compressed size
                     + sizeof(char)                                // the same int32 scale
                     + sizeof(int32)                               // failed size
                     + ((uint32)(phase1_out->n_notnull + 1) >> 1)  // status collection
                    ;

    // int64 values
    if (phase2_out->int64_out_args.modes != 0) {
        total_size += phase2_out->int64_cmpr_size;
    } else {
        total_size += (sizeof(int64) * phase1_out->int64_n_values);
    }

    // int32 values
    if (phase2_out->int32_out_args.modes != 0) {
        total_size += phase2_out->int32_cmpr_size;
    } else {
        total_size += (sizeof(int32) * phase1_out->int32_n_values);
    }

    // the other numeric uncompressed.
    if (phase1_out->failed_cnt > 0) {
        if (phase2_out->other_apply_lz4) {
            total_size += phase2_out->other_cmpr_size;
        } else {
            total_size += phase1_out->failed_size;
        }
    }

    return total_size;
}

char* NumericDecompressParseHeader(
    char* ptr, uint16* out_flags, numerics_statistics_out* out1, numerics_cmprs_out* out2)
{
    uint16 flags = *(uint16*)ptr;
    ptr += sizeof(uint16);

    int rc = memset_s(out1, sizeof(numerics_statistics_out), 0, sizeof(numerics_statistics_out));
    securec_check_c(rc, "", "");
    rc = memset_s(out2, sizeof(numerics_cmprs_out), 0, sizeof(numerics_cmprs_out));
    securec_check_c(rc, "", "");

    /// first restore the size of original values.
    out1->original_size = *(int*)ptr;
    ptr += sizeof(int);

    /// parse the part of int64 values
    if (flags & NUMERIC_FLAG_EXIST_INT64_VAL) {
        if (flags & NUMERIC_FLAG_INT64_SAME_VAL) {
            out1->int64_values_are_same = true;
            out1->int64_minval = out1->int64_maxval = *(int64*)ptr;
            ptr += sizeof(int64);
        } else if (((flags & NUMERIC_FLAG_INT64_WITH_DELTA) != 0) || ((flags & NUMERIC_FLAG_INT64_WITH_RLE) != 0)) {
            if (flags & NUMERIC_FLAG_INT64_WITH_DELTA) {
                out2->int64_out_args.modes |= CU_DeltaCompressed;
                out1->int64_minval = *(int64*)ptr;
                ptr += sizeof(int64);
                out1->int64_maxval = *(uint8*)ptr;
                ptr += sizeof(uint8);
            }
            if (flags & NUMERIC_FLAG_INT64_WITH_RLE) {
                out2->int64_out_args.modes |= CU_RLECompressed;
            }

            out2->int64_cmpr_size = *(int32*)ptr;
            ptr += sizeof(int32);
        }

        if (flags & NUMERIC_FLAG_INT64_SAME_ASCALE) {
            out1->int64_ascales_are_same = true;
            out1->int64_same_ascale = *ptr++;
        }
    }

    /// parse the part of int32 values
    if (flags & NUMERIC_FLAG_EXIST_INT32_VAL) {
        if (flags & NUMERIC_FLAG_INT32_SAME_VAL) {
            out1->int32_values_are_same = true;
            out1->int32_minval = out1->int32_maxval = *(int32*)ptr;
            ptr += sizeof(int32);
        } else if (((flags & NUMERIC_FLAG_INT32_WITH_DELTA) != 0) || ((flags & NUMERIC_FLAG_INT32_WITH_RLE) != 0)) {
            if (flags & NUMERIC_FLAG_INT32_WITH_DELTA) {
                out2->int32_out_args.modes |= CU_DeltaCompressed;
                out1->int32_minval = *(int32*)ptr;
                ptr += sizeof(int32);
                out1->int32_maxval = *(uint8*)ptr;
                ptr += sizeof(uint8);
            }
            if (flags & NUMERIC_FLAG_INT32_WITH_RLE) {
                out2->int32_out_args.modes |= CU_RLECompressed;
            }

            out2->int32_cmpr_size = *(int32*)ptr;
            ptr += sizeof(int32);
        }

        if (flags & NUMERIC_FLAG_INT32_SAME_ASCALE) {
            out1->int32_ascales_are_same = true;
            out1->int32_same_ascale = *ptr++;
        }
    }

    /// parse the part of remain numeric values
    if (flags & NUMERIC_FLAG_EXIST_OTHERS) {
        if (flags & NUMERIC_FLAG_OTHER_WITH_LZ4) {
            out2->other_apply_lz4 = true;
        } else {
            out1->failed_size = *(int32*)ptr;
            ptr += sizeof(int32);
        }
    }

    *out_flags = flags;
    return ptr;
}

#ifdef ENABLE_UT
/// using for numeric ascale format , only for UT
static char* CompressAscalesUsingRLE(char* inBuf, int inSize, uint16* pFlags)
{
    /// now only 1~8 bytes values can be handled with RLE,
    /// so we use 1 byte as the value size.
    RleCoder rle(1, RleCoder::RleMinRepeats_v2[1]);
    int outBound = rle.CompressGetBound(inSize);
    char* outBuf = (char*)palloc(outBound);
    int rleCmprSize = rle.Compress(inBuf, outBuf, inSize, outBound);

    int realOutSize = inSize;
    if (rleCmprSize > 0 && (rleCmprSize + (int)sizeof(int)) < inSize) {
        /// set RLE flag, and remember the real size.
        *pFlags |= NUMERIC_FLAG_SCALE_WITH_RLE;
        *(int*)inBuf = rleCmprSize;
        inBuf += sizeof(int);

        /// ok, copy the compressed ascale codes.
        int rc = memcpy_s(inBuf, inSize, outBuf, rleCmprSize);
        securec_check(rc, "\0", "\0");
        realOutSize = rleCmprSize;
    }
    pfree(outBuf);

    // the returned value pointing to the tail of real data.
    return inBuf + realOutSize;
}

/*
 * @Description: fill ascale info to head area.
 *     we will reduce the used memory size of adjusted scales.
 *     so all the maps of 1 bit, 3 bits and 4 bits are considered.
 *     if 4 bits unit is used, it's better to apply RLE method to the map.
 *     so *pFlags* is needed, and may be set and changed.
 * @IN out1: statistics result
 * @IN out2: compression result
 * @OUT pFlags: remember what modes to use
 * @IN/OUT ptr: buffer to write ascale values
 * @Return: new address to write data
 * @See also:
 */
static char* NumericCompressFillAscales(
    char* ptr, numerics_statistics_out* out1, numerics_cmprs_out* out2, uint16* pFlags)
{
    /// we cannot tolerate this case where either int64 or int32 part exists.
    Assert(out1->n_notnull != out1->failed_cnt);
    Assert(out1->int32_n_values > 0 || out1->int64_n_values > 0);
    Assert(out1->n_notnull > 0);

    if (out1->failed_cnt == 0) {
        if (out1->int64_n_values == 0) {
            if (out1->int32_ascales_are_same) {
                /// each value is equal to out1->int32_same_ascale
                /// so we needn't store anything.
                return ptr;
            }

            /// there are 5 different scales.
            /// so wee need 3 bits to store each scale.
            return ptr + CompactAscaleCodesBy3Bits((uint8*)ptr, (uint8*)out1->ascale_codes, out1->n_notnull);
        }

        if (out1->int32_n_values == 0) {
            if (out1->int64_ascales_are_same) {
                /// each value is equal to out1->int64_same_ascale.
                /// so we needn't remember anything.
                return ptr;
            }
        }

        /// there are at least 9 different scales to store, and
        /// so we need 4 bits to remember each scale. fall down!
        if (out1->int64_ascales_are_same && out1->int32_ascales_are_same) {
            FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, out1->int64_same_ascale);
            return ptr + (long)BITMAPLEN(out1->n_notnull);
        }
    } else {
        if (out1->int32_n_values == 0 && out1->int64_ascales_are_same) {
            FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, out1->int64_same_ascale);
            return ptr + (long)BITMAPLEN(out1->n_notnull);
        }

        if (out1->int64_n_values == 0) {
            if (out1->int32_ascales_are_same) {
                FillBitmap<char>(ptr, out1->ascale_codes, out1->n_notnull, out1->int32_same_ascale);
                return ptr + (long)BITMAPLEN(out1->n_notnull);
            }

            /// there are 6 different scales to remember, including 5 for int32
            /// and 1 for uncompressed values.
            return ptr + CompactAscaleCodesBy3Bits((uint8*)ptr, (uint8*)out1->ascale_codes, out1->n_notnull);
        }
    }

    /// compact two 4bits to one byte.
    CompactAscaleCodesBy4Bits((uint8*)ptr, (uint8*)out1->ascale_codes, out1->n_notnull);

    int in_size = ((out1->n_notnull + 1) >> 1);
    if (out2->filter->m_adopt_numeric2int_ascale_rle) {
        return CompressAscalesUsingRLE(ptr, in_size, pFlags);
    } else {
        return (ptr + in_size);
    }
}
#endif  // ENABLE_UT

char* NumericDecompressParseAscales(char* inBuf, numerics_statistics_out* decmprStatOut, uint16 flags)
{
    /// we cannot tolerate this case where either int64 or int32 part exists.
    Assert((flags & (NUMERIC_FLAG_EXIST_INT64_VAL | NUMERIC_FLAG_EXIST_INT32_VAL)) != 0);
    /// until here, how many records should be known.
    Assert(decmprStatOut->n_notnull > 0);

    int usedBytes = 0;

    if ((flags & NUMERIC_FLAG_EXIST_OTHERS) == 0) {
        decmprStatOut->failed_cnt = 0;

        if ((flags & NUMERIC_FLAG_EXIST_INT64_VAL) == 0) {
            if (decmprStatOut->int32_ascales_are_same) {
                /// each value is equal to decmprStatOut->int32_same_ascale
                decmprStatOut->int32_n_values = decmprStatOut->n_notnull;
                decmprStatOut->int64_n_values = 0;
                return inBuf;
            }

            /// until here we have to malloc space for ascale codes array.
            decmprStatOut->ascale_codes = (char*)palloc(decmprStatOut->n_notnull);
            usedBytes =
                ParseAscaleCodesBy3Bits((uint8*)inBuf, (uint8*)decmprStatOut->ascale_codes, decmprStatOut->n_notnull);
            NumericDecompressCount(decmprStatOut);
            return inBuf + usedBytes;
        }

        if (unlikely(decmprStatOut->int64_ascales_are_same && ((flags & NUMERIC_FLAG_EXIST_INT32_VAL) == 0))) {
            /// each value is equal to decmprStatOut->int64_same_ascale
            decmprStatOut->int64_n_values = decmprStatOut->n_notnull;
            decmprStatOut->int32_n_values = 0;
            return inBuf;
        }

        /// until here we have to malloc space for ascale codes array.
        decmprStatOut->ascale_codes = (char*)palloc(decmprStatOut->n_notnull);

        if (decmprStatOut->int64_ascales_are_same && decmprStatOut->int32_ascales_are_same) {
            decmprStatOut->int64_n_values = ParseBitmap<char>(inBuf,
                                                              decmprStatOut->ascale_codes,
                                                              decmprStatOut->n_notnull,
                                                              decmprStatOut->int32_same_ascale,
                                                              decmprStatOut->int64_same_ascale);
            decmprStatOut->int32_n_values = decmprStatOut->n_notnull - decmprStatOut->int64_n_values;
            return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
        }
    } else {
        /// until here we have to malloc space for ascale codes array.
        decmprStatOut->ascale_codes = (char*)palloc(decmprStatOut->n_notnull);

        if (decmprStatOut->int64_ascales_are_same && ((flags & NUMERIC_FLAG_EXIST_INT32_VAL) == 0)) {
            decmprStatOut->int64_n_values = ParseBitmap<char>(inBuf,
                                                              decmprStatOut->ascale_codes,
                                                              decmprStatOut->n_notnull,
                                                              FAILED_ENCODING,
                                                              decmprStatOut->int64_same_ascale);
            decmprStatOut->int32_n_values = 0;
            decmprStatOut->failed_cnt = decmprStatOut->n_notnull - decmprStatOut->int64_n_values;
            return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
        }

        if ((flags & NUMERIC_FLAG_EXIST_INT64_VAL) == 0) {
            if (decmprStatOut->int32_ascales_are_same) {
                decmprStatOut->int32_n_values = ParseBitmap<char>(inBuf,
                                                                  decmprStatOut->ascale_codes,
                                                                  decmprStatOut->n_notnull,
                                                                  FAILED_ENCODING,
                                                                  decmprStatOut->int32_same_ascale);
                decmprStatOut->int64_n_values = 0;
                decmprStatOut->failed_cnt = decmprStatOut->n_notnull - decmprStatOut->int32_n_values;
                return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
            }

            usedBytes =
                ParseAscaleCodesBy3Bits((uint8*)inBuf, (uint8*)decmprStatOut->ascale_codes, decmprStatOut->n_notnull);
            NumericDecompressCount(decmprStatOut);
            return inBuf + usedBytes;
        }
    }

    int ascaleSize = ((uint32)(decmprStatOut->n_notnull + 1) >> 1);
    if (flags & NUMERIC_FLAG_SCALE_WITH_RLE) {
        /// the first 4B has remember the real size.
        int inSize = *(int*)inBuf;
        char* outBuf = (char*)palloc(ascaleSize);
        inBuf += sizeof(int);

        /// RLE decompress first.
        RleCoder rle(1, RleCoder::RleMinRepeats_v2[1]);
        int rleOrigSize = rle.Decompress(inBuf, outBuf, inSize, ascaleSize);
        if (unlikely(rleOrigSize != ascaleSize)) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("RLE decompress failed, expected bytes %d, real size %d", ascaleSize, rleOrigSize)));
        }

        /// decode and count the integer values
        ParseAscaleCodesBy4Bits(
            decmprStatOut, (uint8*)outBuf, (uint8*)decmprStatOut->ascale_codes, decmprStatOut->n_notnull);
        NumericDecompressCount(decmprStatOut);

        pfree(outBuf);
        outBuf = NULL;
        return inBuf + inSize;
    } else {
        ParseAscaleCodesBy4Bits(
            decmprStatOut, (uint8*)inBuf, (uint8*)decmprStatOut->ascale_codes, decmprStatOut->n_notnull);
        NumericDecompressCount(decmprStatOut);
        return inBuf + ascaleSize;
    }
}



/*
 * @Description:  decompress  dscale
 * @IN inBuf: buffer of compressed dscale
 * @IN/OUT decmprStatOut: numeric statistics
 * @IN flags: compression flags
 * @Return:
 */
char* NumericDecompressParseDscales(char* inBuf, numerics_statistics_out* decmprStatOut, uint16 flags)
{
    // we cannot tolerate this case where either int64 or int32 part exists.
    Assert((flags & (NUMERIC_FLAG_EXIST_INT64_VAL | NUMERIC_FLAG_EXIST_INT32_VAL)) != 0);
    // until here, how many records should be known.
    Assert(decmprStatOut->n_notnull > 0);
    // if exist value then must have same scale
    Assert((flags & NUMERIC_FLAG_EXIST_INT32_VAL) || decmprStatOut->int32_ascales_are_same == 0);
    Assert((flags & NUMERIC_FLAG_EXIST_INT64_VAL) || decmprStatOut->int64_ascales_are_same == 0);

    // Case 1: All values are integer
    if ((flags & NUMERIC_FLAG_EXIST_OTHERS) == 0) {
        decmprStatOut->failed_cnt = 0;

        if ((flags & NUMERIC_FLAG_EXIST_INT64_VAL) == 0) {
            // only int32 with same scale
            // each value is equal to decmprStatOut->int32_same_ascale
            decmprStatOut->int32_n_values = decmprStatOut->n_notnull;
            decmprStatOut->int64_n_values = 0;
            return inBuf;
        } else if ((flags & NUMERIC_FLAG_EXIST_INT32_VAL) == 0) {
            // only int64 with same scale
            // each value is equal to decmprStatOut->int64_same_ascale
            decmprStatOut->int64_n_values = decmprStatOut->n_notnull;
            decmprStatOut->int32_n_values = 0;
            return inBuf;
        } else {
            // int32 with same scale and int64 with same scale
            Assert(decmprStatOut->int64_ascales_are_same && decmprStatOut->int32_ascales_are_same);
            // until here we have to malloc space for ascale codes array.
            decmprStatOut->ascale_codes = (char*)palloc(decmprStatOut->n_notnull);

            decmprStatOut->int64_n_values = ParseBitmap<char>(inBuf,
                                                              decmprStatOut->ascale_codes,
                                                              decmprStatOut->n_notnull,
                                                              INT32_DSCALE_ENCODING,
                                                              INT64_DSCALE_ENCODING);
            decmprStatOut->int32_n_values = decmprStatOut->n_notnull - decmprStatOut->int64_n_values;
            return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
        }
    } else { // Case 2: Mixed with integer and numeric
        // until here we have to malloc space for ascale codes array.
        decmprStatOut->ascale_codes = (char*)palloc(decmprStatOut->n_notnull);

        if ((flags & NUMERIC_FLAG_EXIST_INT32_VAL) == 0) {
            // int64 with same scale and numeric
            decmprStatOut->int64_n_values = ParseBitmap<char>(inBuf,
                                                              decmprStatOut->ascale_codes,
                                                              decmprStatOut->n_notnull,
                                                              NUMERIC_DSCALE_ENCODING,
                                                              INT64_DSCALE_ENCODING);
            decmprStatOut->int32_n_values = 0;
            decmprStatOut->failed_cnt = decmprStatOut->n_notnull - decmprStatOut->int64_n_values;
            return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
        } else if ((flags & NUMERIC_FLAG_EXIST_INT64_VAL) == 0) {
            // int32 with same scale and numeric
            decmprStatOut->int32_n_values = ParseBitmap<char>(inBuf,
                                                              decmprStatOut->ascale_codes,
                                                              decmprStatOut->n_notnull,
                                                              NUMERIC_DSCALE_ENCODING,
                                                              INT32_DSCALE_ENCODING);
            decmprStatOut->int64_n_values = 0;
            decmprStatOut->failed_cnt = decmprStatOut->n_notnull - decmprStatOut->int32_n_values;
            return inBuf + (long)BITMAPLEN(decmprStatOut->n_notnull);
        } else {
            // int32 with same scale, int64 with same scale and numeric
            int usedBytes =
                ParseDscaleCodesBy2Bits((uint8*)inBuf, (uint8*)decmprStatOut->ascale_codes, decmprStatOut->n_notnull);

            NumericDecompressDscaleCount(decmprStatOut);
            return inBuf + usedBytes;
        }
    }
}

char* NumericDecompressParseEachPart(
    char* ptr, numerics_statistics_out* out1, numerics_cmprs_out* out2, numerics_decmprs_addr_ref* out3)
{
    int totalSize = 0;
    int tmpSize = 0;
    uint16 modes = 0;

    /// step1: int64 values
    if (out1->int64_n_values > 0 && !out1->int64_values_are_same) {
        modes = out2->int64_out_args.modes;

        if (modes != 0) {
            DeltaPlusRLEv2 deltaPlusRle(out1->int64_minval, out1->int64_maxval, out1->int64_n_values, sizeof(int64));

            if (modes & CU_DeltaCompressed) {
                /// *int64_maxval* remember the datum size in RLE block.
                /// its size is between 0 and 8. so it's safe to convert
                /// it to short data type.
                deltaPlusRle.SetDltValSize((short)out1->int64_maxval);
            }

            /// ok, have to new a array for holding int64 values.
            tmpSize = int(sizeof(int64) * out1->int64_n_values);
            // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
            out1->int64_values = (int64*)palloc(tmpSize + 8);

            totalSize =
                deltaPlusRle.Decompress((char**)&out1->int64_values, tmpSize, ptr, out2->int64_cmpr_size, modes);

            Assert(totalSize == int(sizeof(int64) * out1->int64_n_values));
            ptr += out2->int64_cmpr_size;
        } else {
            /// needn't to copy the data, we can access directly later.
            out3->int64_addr_ref = ptr;
            ptr += (sizeof(int64) * out1->int64_n_values);
        }
    }
    /// step2: int32 values
    if (out1->int32_n_values > 0 && !out1->int32_values_are_same) {
        modes = out2->int32_out_args.modes;

        if (modes > 0) {
            DeltaPlusRLEv2 deltaPlusRle(out1->int32_minval, out1->int32_maxval, out1->int32_n_values, sizeof(int32));

            if (modes & CU_DeltaCompressed) {
                /// *int32_maxval* remember the datum size in RLE block.
                /// its size is between 0 and 8. so it's safe to convert
                /// it to short data type.
                deltaPlusRle.SetDltValSize((short)out1->int32_maxval);
            }

            /// ok, have to new a array for holding int32 values.
            tmpSize = int(sizeof(int32) * out1->int32_n_values);
            // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
            out1->int32_values = (int32*)palloc(tmpSize + 8);

            totalSize =
                deltaPlusRle.Decompress((char**)&out1->int32_values, tmpSize, ptr, out2->int32_cmpr_size, modes);

            Assert(totalSize == int(sizeof(int32) * out1->int32_n_values));
            ptr += out2->int32_cmpr_size;
        } else {
            /// needn't to copy the data, we can access directly later.
            out3->int32_addr_ref = ptr;
            ptr += (sizeof(int32) * out1->int32_n_values);
        }
    }
    /// step3: the other numeric values
    if (out1->failed_cnt > 0) {
        if (out2->other_apply_lz4) {
            LZ4Wrapper lzDecoder;
            int cmprSize = lzDecoder.DecompressGetCmprSize(ptr);
            out1->failed_size = lzDecoder.DecompressGetBound(ptr);
            out2->other_srcbuf = (char*)palloc(out1->failed_size);
            totalSize = lzDecoder.Decompress(ptr, out2->other_srcbuf, cmprSize);
            Assert(totalSize == out1->failed_size);
        } else {
            /// needn't to copy the data, we can access directly later.
            out3->other_addr_ref = ptr;
            ptr += out1->failed_size;
        }
    }
    return ptr;
}

/// control the ratio how many numeric values fails to
/// convert to int64 values.
static const int numericDefaultFailedRatio = 20;
static int NumericMaxFailedRatio = numericDefaultFailedRatio;

void NumericConfigFailedRatio(int failed_ratio)
{
    if (failed_ratio <= 0) {
        NumericMaxFailedRatio = numericDefaultFailedRatio;
    } else if (failed_ratio >= 100) {
        NumericMaxFailedRatio = 100;
    } else {
        NumericMaxFailedRatio = failed_ratio;
    }
}

bool NumericCompressBatchValues(
    BatchNumeric* batch, numerics_statistics_out* out1, numerics_cmprs_out* out2, int* compressed_datasize, int align_size)
{
    Datum* batchValues = batch->values;
    char* batchNulls = batch->nulls;
    int batchRows = batch->rows;
    int nSuccess = 0;
    int nullCount = 0;

    /* Step 1: Convert numeric to integer if need */
    nSuccess = batch_convert_short_numeric_to_int64(batchValues, batchNulls, batchRows, batch->hasNull,
                                                    out1->int64_values, out1->ascale_codes, out1->success, &nullCount);
    /* Null values should be excluded when calculating the conversion success rate */
    if (nSuccess == 0 || nSuccess < (batchRows - nullCount) * (100 - NumericMaxFailedRatio) * 0.01) {
#ifdef ENABLE_UT
        // execute ahead for ut testing
        if (batch->hasNull) {
            NumericStatistics<true, false>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        } else {
            NumericStatistics<false, false>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        }
#endif

        return false;
    }

    /* Step 2: Compress numeric as integer. Maybe mixed with integer and numeric */
    if (batch->hasNull) {
        if (nSuccess == batchRows) {
            NumericStatistics<true, true>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        } else {
            NumericStatistics<true, false>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        }
        NumericCompressEachValuesPart<true>(out2, out1, batchValues, batchNulls, batchRows);
    } else {
        if (nSuccess == batchRows) {
            NumericStatistics<false, true>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        } else {
            NumericStatistics<false, false>(out1, batchValues, batchNulls, batchRows, batch->func, batch->mobj);
        }
        NumericCompressEachValuesPart<false>(out2, out1, batchValues, batchNulls, batchRows);
    }

    // compute the total size of this cu
    *compressed_datasize = NumericCompressGetTotalSize(out1, out2, align_size);
    return true;
}

/// release resources after numeric compression
void NumericCompressReleaseResource(numerics_statistics_out* statOut, numerics_cmprs_out* cmprOut)
{
    pfree_ext(statOut->success);
    pfree_ext(statOut->ascale_codes);
    pfree_ext(statOut->int64_values);
    pfree_ext(statOut->int32_values);
    pfree_ext(cmprOut->other_srcbuf);
    pfree_ext(cmprOut->other_outbuf);
    pfree_ext(cmprOut->int32_out_args.buf);
    pfree_ext(cmprOut->int64_out_args.buf);
}

/// get the first bit1 position of an int32 value.
/// the function is the same to Log2(n)
static short Int32ValueLog(uint16 value)
{
    short n = 0;

    if (value > 0xFF) {
        n += 8;
        value >>= n;
    }
    if (value > 0x0F) {
        n += 4;
        value >>= 4;
    }
    if (value > 0x03) {
        n += 2;
        value >>= 2;
    }

    return (value > 0x01) ? (n + 1) : n;
}

/// all the values are the same and it's defined by *value* and *ascale*.
/// we have to copy this value *repeats* times to *outBuf*. In order to recude
/// the call number of memcpy(), we write this function. Its max call number
/// is up to *repeats*, and is
/// 1. Log2(repeats)+1 if repeats is not 2^^k, or
/// 2. Log2(repeats)   if repeats is     2^^k
static void NumericDecompressRepeatOneValue(
    char* outBuf, int64 value, char ascale, int typeMode, uint16 repeats, bool DscaleFlag)
{
    /// *repeats* is at least 1.
    Assert(repeats > 0);
    int singleLen = 0;

    /// it's important that we must copy once at the first time,
    /// so that we can try our best to recude the number
    /// of memcpy calls as possible by using *loops* and *remain*.
    if (DscaleFlag) {
        /* Dscale is aligned to excution engine, so we can simplely convert */
        MAKE_NUMERIC64(outBuf, value, ascale);
        singleLen = NUMERIC_64SZ;
    } else {
        singleLen = convert_int64_to_short_numeric(outBuf, value, ascale, typeMode);
    }

    uint32 loops = Int32ValueLog(repeats);
    int remain = repeats - (1U << loops);
    Assert(remain >= 0 && (uint)remain < (1U << loops));

    char* copyBuff = outBuf + singleLen;
    uint32 copySize = singleLen;
    int rc = 0;
    for (uint32 i = 0; i < loops; ++i) {
        /// in each loop, from *copyBuff* we always copy the exsiting data
        /// once whose size is *copySize*.
        rc = memcpy_s(copyBuff, copySize, outBuf, copySize);
        securec_check(rc, "", "");
        copyBuff += copySize;
        copySize <<= 1;
    }
    if (remain > 0) {
        copySize = remain * singleLen;
        rc = memcpy_s(copyBuff, copySize, outBuf, copySize);
        securec_check(rc, "", "");
    }
}

/// all the values are of int64 or int32 type, and they have the same
/// adjust scale. so use this template function to resotre numeric values.
template <typename T, bool DscaleFlag>
static void RestoreNumericsWithSameAscaleT(char* dest, T* values, int nValues, char sameAsacle, int typeMode)
{
    int tmpNumLen = 0;

    for (int i = 0; i < nValues; ++i) {
        if (DscaleFlag) {
            MAKE_NUMERIC64(dest, (*values++), sameAsacle);
            tmpNumLen = NUMERIC_64SZ;
        } else {
            tmpNumLen = convert_int64_to_short_numeric(dest, (*values++), sameAsacle, typeMode);
        }

        /// advance the pointer to the output buffer.
        dest += tmpNumLen;
    }
}

/*
 * @Description:  Mixed values includes interger and numeric, So we must special convert
 *                           in order to be aligned with excution engine
 * @OUT outPtr: output buffer
 * @IN int32Val: array of int32 values
 * @IN int64Val: array of int64 values
 * @IN flag: array of flags which indicate int32 or int64 or numeric
 * @IN same64Scale: dscale value for int32
 * @IN same32Scale: dscale value for int64
 * @IN nNotNulls: number of values
 * @IN otherValuesPtr: array of numeric values
 */
template <bool int64SameValue, bool int32SameValue>
static void ConvertMixedToBINumeric(char* outPtr, int32* int32Val, int64* int64Val, const char* flag, int int32Dscale,
                                    int int64Dscale, int nNotNulls, char* otherValuesPtr)
{
    int out_len = 0;
    char* tmpNumeric = NULL;
    char* out = outPtr;
    char* otherValues = otherValuesPtr;
    char dscale = 0;
    int64 val = 0;
    errno_t rc = EOK;

    for (int i = 0; i < nNotNulls; ++i) {
        if (*flag == INT32_DSCALE_ENCODING) {
            // int32
            dscale = int32Dscale;
            Assert(dscale >= 0 && dscale <= MAXINT64DIGIT);

            if (!int32SameValue) {
                val = (int64) * int32Val++;
            } else {
                val = *int32Val;
            }
            MAKE_NUMERIC64(out, val, dscale);
            out_len = NUMERIC_64SZ;
        } else if (*flag == INT64_DSCALE_ENCODING) {
            // int64
            dscale = int64Dscale;
            Assert(dscale >= 0 && dscale <= MAXINT64DIGIT);

            if (!int64SameValue) {
                val = *int64Val++;
            } else {
                val = *int64Val;
            }
            MAKE_NUMERIC64(out, val, dscale);
            out_len = NUMERIC_64SZ;
        } else {
            // numeric
            Assert(*flag == NUMERIC_DSCALE_ENCODING);

            tmpNumeric = DatumGetPointer(otherValues);
            out_len = VARSIZE_ANY(tmpNumeric);
            rc = memcpy_s(out, out_len, tmpNumeric, out_len);
            securec_check(rc, "", "");
            otherValues += out_len;
        }

        // advance the pointer to the output buffer.
        ++flag;
        out += out_len;
    }
}

/// Given all the int64[], int32[], numeric[] values, and adjust scale codes,
/// original Numeric data will be restored and stored in *outBuf*.
template extern void NumericDecompressRestoreValues<true>(char* outBuf, int typeMode,
                                                          numerics_statistics_out* decmprStatOut, numerics_cmprs_out* decmprOut, numerics_decmprs_addr_ref* addrRefs);

template extern void NumericDecompressRestoreValues<false>(char* outBuf, int typeMode,
                                                           numerics_statistics_out* decmprStatOut, numerics_cmprs_out* decmprOut, numerics_decmprs_addr_ref* addrRefs);

template <bool DscaleFlag>
void NumericDecompressRestoreValues(char* outBuf, int typeMode, numerics_statistics_out* decmprStatOut,
                                    numerics_cmprs_out* decmprOut, numerics_decmprs_addr_ref* addrRefs)
{
    char* ascaleCodes = decmprStatOut->ascale_codes;

    /// int64 values may be from either *int64_values* or *int64_addr_ref*.
    /// it dependents on the *int64_out_args.modes*.
    /// the same to int32 values and raw numeric part.
    int64* int64Values =
        (decmprOut->int64_out_args.modes != 0) ? decmprStatOut->int64_values : (int64*)addrRefs->int64_addr_ref;
    int32* int32Values =
        (decmprOut->int32_out_args.modes != 0) ? decmprStatOut->int32_values : (int32*)addrRefs->int32_addr_ref;
    char* otherValues = decmprOut->other_apply_lz4 ? decmprOut->other_srcbuf : addrRefs->other_addr_ref;

    int64 tmpInt64Val = decmprStatOut->int64_minval;
    int32 tmpInt32Val = decmprStatOut->int32_minval;
    char tmpInt64Ascale = decmprStatOut->int64_same_ascale;
    char tmpInt32Ascale = decmprStatOut->int32_same_ascale;

    if (!DscaleFlag) {
        tmpInt64Ascale -= DIFF_INT64_ASCALE_ENCODING;
        tmpInt32Ascale -= DIFF_INT32_ASCALE_ENCODING;
    }

    const bool int64SameAscale = decmprStatOut->int64_ascales_are_same;
    const bool int64SameValue = decmprStatOut->int64_values_are_same;
    const bool int32SameAscale = decmprStatOut->int32_ascales_are_same;
    const bool int32SameValue = decmprStatOut->int32_values_are_same;
    const int nNotNulls = decmprStatOut->n_notnull;

    /* Step 1: handle integer numeric which are stored as integer */
    if (decmprStatOut->failed_cnt == 0) {
        if (unlikely(decmprStatOut->int64_n_values == 0 && int32SameAscale)) {
            Assert(nNotNulls == decmprStatOut->int32_n_values);

            if (int32SameValue) {
                /// the same scale and the single int32 value,
                /// so that use the spicail function.
                NumericDecompressRepeatOneValue(
                    outBuf, decmprStatOut->int32_minval, tmpInt32Ascale, typeMode, nNotNulls, DscaleFlag);

                return;
            }

            /// the same ascale but different int32 values,
            /// so that use the template function.
            RestoreNumericsWithSameAscaleT<int32, DscaleFlag>(outBuf, int32Values, nNotNulls, tmpInt32Ascale, typeMode);

            return;
        }

        if (unlikely(decmprStatOut->int32_n_values == 0 && int64SameAscale)) {
            Assert(nNotNulls == decmprStatOut->int64_n_values);

            if (int64SameValue) {
                /// the same one int64 value case, and use the spicail function.
                NumericDecompressRepeatOneValue(
                    outBuf, decmprStatOut->int64_minval, tmpInt64Ascale, typeMode, nNotNulls, DscaleFlag);

                return;
            }

            /// the same ascale but different int64 values,
            /// so that use the template function.
            RestoreNumericsWithSameAscaleT<int64, DscaleFlag>(outBuf, int64Values, nNotNulls, tmpInt64Ascale, typeMode);

            return;
        }
    } else {
        Assert(decmprStatOut->int32_n_values > 0 || decmprStatOut->int64_n_values > 0);
    }

    char* out = outBuf;
    int out_len = 0;
    char* tmpNumeric = NULL;
    int rc = 0;

    /* Step 2: handle mixed numeric case which are stored as integer or numeric */
    if (DscaleFlag) {
        if (int64SameValue) {
            if (int32SameValue) {
                ConvertMixedToBINumeric<true, true>(out, &tmpInt32Val, &tmpInt64Val, ascaleCodes,
                                                    tmpInt32Ascale, tmpInt64Ascale, nNotNulls, otherValues);
            } else {
                ConvertMixedToBINumeric<true, false>(out, int32Values, &tmpInt64Val, ascaleCodes,
                                                     tmpInt32Ascale, tmpInt64Ascale, nNotNulls, otherValues);
            }
        } else {
            if (int32SameValue) {
                ConvertMixedToBINumeric<false, true>(out, &tmpInt32Val, int64Values, ascaleCodes,
                                                     tmpInt32Ascale, tmpInt64Ascale, nNotNulls, otherValues);
            } else {
                ConvertMixedToBINumeric<false, false>(out, int32Values, int64Values, ascaleCodes,
                                                      tmpInt32Ascale, tmpInt64Ascale, nNotNulls, otherValues);
            }
        }
        return;
    }

    /*
     * Just for old numeric compression. We don't remove these code because we must
     * support old numeric compression format.
     */
    Assert(ascaleCodes != NULL);
    for (int i = 0; i < nNotNulls; ++i) {
        // for int64 value, its ascale codes is [6, 14]
        if (*ascaleCodes > MAX_INT32_ASCALE_ENCODING) {
            if (!int64SameAscale) {
                tmpInt64Ascale = *ascaleCodes - DIFF_INT64_ASCALE_ENCODING;
            }
            if (!int64SameValue) {
                tmpInt64Val = (*int64Values++);
            }
            Assert(tmpInt64Ascale >= INT64_MIN_ASCALE && tmpInt64Ascale <= INT64_MAX_ASCALE);

            out_len = convert_int64_to_short_numeric(out, tmpInt64Val, tmpInt64Ascale, typeMode);
        } else if (*ascaleCodes > FAILED_ENCODING) { // for int32 value, its ascale codes is [1, 5]
            if (!int32SameAscale) {
                tmpInt32Ascale = *ascaleCodes - DIFF_INT32_ASCALE_ENCODING;
            }
            if (!int32SameValue) {
                tmpInt32Val = (*int32Values++);
            }
            Assert(tmpInt32Ascale >= INT32_MIN_ASCALE && tmpInt32Ascale <= INT32_MAX_ASCALE);

            out_len = convert_int64_to_short_numeric(out, tmpInt32Val, tmpInt32Ascale, typeMode);
        } else { // for failed value, its ascale code is 0.
            Assert(*ascaleCodes == FAILED_ENCODING);
            tmpNumeric = DatumGetPointer(otherValues);
            out_len = VARSIZE_ANY(tmpNumeric);
            rc = memcpy_s(out, out_len, tmpNumeric, out_len);
            securec_check(rc, "", "");
            otherValues += out_len;
        }

        // advance the pointer to the output buffer.
        ++ascaleCodes;
        out += out_len;
    }
}

void NumericDecompressReleaseResources(
    numerics_statistics_out* decmprStatOut, numerics_cmprs_out* decmprOut, numerics_decmprs_addr_ref* addrRef)
{
    Assert(decmprStatOut->success == NULL);

    pfree_ext(decmprStatOut->int32_values);
    pfree_ext(decmprStatOut->int64_values);
    pfree_ext(decmprStatOut->ascale_codes);

    Assert(decmprOut->other_outbuf == NULL);
    Assert(decmprOut->int64_out_args.buf == NULL);
    Assert(decmprOut->int32_out_args.buf == NULL);

    pfree_ext(decmprOut->other_srcbuf);
    addrRef->int32_addr_ref = NULL;
    addrRef->int64_addr_ref = NULL;
    addrRef->other_addr_ref = NULL;
}

/*
 * @Description: reset all flags
 * @See also:
 */
void compression_options::reset(void)
{
    m_sampling_finished = false; /* need a sampling */
    m_adopt_numeric2int_ascale_rle = true;
    m_adopt_numeric2int_int32_rle = true;
    m_adopt_numeric2int_int64_rle = true;
    m_adopt_dict = true;
    m_adopt_rle = true;
}

/*
 * @Description: set compressin filter for numeric datatype according to given modes
 * @IN modes: numeric compression modes
 * @See also:
 */
void compression_options::set_numeric_flags(uint16 modes)
{
    m_adopt_numeric2int_int32_rle = ((modes & NUMERIC_FLAG_INT32_WITH_RLE) != 0);
    m_adopt_numeric2int_int64_rle = ((modes & NUMERIC_FLAG_INT64_WITH_RLE) != 0);
    m_adopt_numeric2int_ascale_rle = ((modes & NUMERIC_FLAG_SCALE_WITH_RLE) != 0);
}

/*
 * @Description: set compressin filter according to given modes
 * @IN modes: compression modes
 * @See also:
 */
void compression_options::set_common_flags(uint32 modes)
{
    m_adopt_dict = ((modes & CU_DicEncode) != 0);
    m_adopt_rle = ((modes & CU_RLECompressed) != 0);
}

#ifdef ENABLE_UT
    #undef static
#endif
