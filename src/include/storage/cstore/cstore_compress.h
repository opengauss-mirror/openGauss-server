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
 * cstore_compress.h
 *        routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cstore/cstore_compress.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GSCSTORE_COMPRESS_H
#define GSCSTORE_COMPRESS_H

#include "storage/compress_kits.h"

// CU_INFOMASK1 >>>>
//
#define CU_DeltaCompressed 0x0001
#define CU_DicEncode 0x0002
// CU_CompressExtend is used for extended compression.
// For compression added afterwards, it can be defined as CU_CompressExtend + 0x0001~0x0008
#define CU_CompressExtend 0x0004    // Used for extended compression
#define CU_Delta2Compressed 0x0005  // CU_Delta2Compressed equals CU_CompressExtend plus 0x0001
#define CU_XORCompressed 0x0006     // CU_XORCompressed equals CU_CompressExtend plus 0x0002
#define CU_RLECompressed 0x0008
#define CU_LzCompressed 0x0010
#define CU_ZlibCompressed 0x0020
#define CU_BitpackCompressed 0x0040
#define CU_IntLikeCompressed 0x0080

extern bool NeedToRecomputeMinMax(Oid typeOid);
extern int64 ConvertToInt64Data(_in_ const char* inBuf, _in_ const short eachValSize);
extern void Int64DataConvertTo(_in_ int64 inVal, _in_ short eachValSize, _out_ char* outBuf);

#define GLOBAL_DICT_SIZE 4096

/* compression filter.
 * step 1: sample. use the first CU data to sample, and detect
 *         what compression methods to adopt;
 * step 2: apply these filter to subsequent compressing.
 */
struct compression_options {
    /* sampling flag */
    bool m_sampling_fihished;

    /* the followings are adopt-compression-method flags */

    /* flags for numeric to integer */
    bool m_adopt_numeric2int_ascale_rle;
    bool m_adopt_numeric2int_int32_rle;
    bool m_adopt_numeric2int_int64_rle;

    /* common flags */
    bool m_adopt_dict; /* Dictionary encoding */
    bool m_adopt_rle;  /* RLE encoding */

    void reset(void);
    void set_numeric_flags(uint16 modes);
    void set_common_flags(uint32 modes);
};

// input arguments for compression &&
// output arguments for decompression
//
typedef struct {
    // shared var by both IntegerCoder && StringCoder
    //
    char* buf;
    int sz;
    /* compressing modes include compression and compress level. */
    int16 mode;
    /* here int16 padding */

    // only just for StringCoder, whether to use dictionary
    //
    void* globalDict;
    int numVals;
    bool useDict;
    bool useGlobalDict;
    bool buildGlobalDict;
} CompressionArg1;

// output arguments for compression &&
// input arguments for decompression
//
typedef struct {
    char* buf;
    int sz;
    uint16 modes;
} CompressionArg2;

class IntegerCoder : public BaseObject {
public:
    virtual ~IntegerCoder()
    {}

    IntegerCoder(short eachValSize);
    void SetMinMaxVal(int64 min, int64 max);
    int Compress(_in_ const CompressionArg1& in, _out_ CompressionArg2& out);
    int Decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out);

    /* optimizing flags */
    bool m_adopt_rle;

private:
    void InsertMinMaxVal(char* buf, int* usedSize);

    /* inner implement for compress API */
    template <bool adopt_rle>
    int CompressInner(const CompressionArg1& in, CompressionArg2& out);

private:
    // m_isValid = true, min/max is passed in by SetMinMaxVal().
    // m_isValid = false, min/max should be computed innerly.
    //
    int64 m_minVal;
    int64 m_maxVal;
    bool m_isValid;
    short m_eachValSize;
};

class StringCoder : public BaseObject {
public:
    virtual ~StringCoder()
    {}

    StringCoder() : m_adopt_rle(true), m_adopt_dict(true), m_dicCodes(NULL), m_dicCodesNum(0)
    {}

    int Compress(_in_ CompressionArg1& in, _in_ CompressionArg2& out);
    int Decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out);

    /* optimizing flags */
    bool m_adopt_rle;
    bool m_adopt_dict;

private:
    /* inner implement for compress api */
    template <bool adopt_dict>
    int CompressInner(CompressionArg1& in, CompressionArg2& out);

    // compress/decompress directly using lz4/zlib but without global/local dictionary
    //
    int CompressWithoutDict(_in_ char* inBuf, _in_ int inBufSize, _in_ int compressing_modes, _out_ char* outBuf,
        _in_ int outBufSize, _out_ int& mode);
    int DecompressWithoutDict(
        _in_ char* inBuf, _in_ int inBufSize, _in_ uint16 mode, _out_ char* outBuf, _out_ int outBufSize);

    int CompressNumbers(
        _in_ int max, _in_ int compressing_modes, __inout char* outBuf, _in_ int outBufSize, _out_ uint16& mode);
    void DecompressNumbers(
        _in_ char* inBuf, _in_ int inBufSize, _in_ uint16 mode, _out_ char* outBuf, _in_ int outBufSize);

private:
    DicCodeType* m_dicCodes;
    DicCodeType m_dicCodesNum;
};

/// light-weight implementation for Delta-RLE compression.
class DeltaPlusRLEv2 : public BaseObject {
public:
    DeltaPlusRLEv2(int64 minVal, int64 maxVal, int32 nVals, short valueBytes)
    {
        m_minVal = minVal;
        m_nValues = nVals;
        m_valueBytes = valueBytes;

        /// m_maxVal is not used during decompression.
        /// so that you can skip it.
        m_maxVal = maxVal;

        /// m_deltaBytes will be updated later.
        m_deltaBytes = valueBytes;

        m_adopt_rle = true;
    }
    virtual ~DeltaPlusRLEv2()
    { /* don't create or destroy any resource. */
    }

    /// API about compressing.
    int GetBound();
    int Compress(char** outBuf, int outBufSize, uint16* outModes, char* inBuf, int inBufSize);

    /// API about decompressing.
    void SetDltValSize(short dltBytes)
    {
        /// if delta is used, m_deltaBytes < m_valueBytes.
        /// otherwise, they should be equal to.
        Assert(dltBytes < m_valueBytes);
        m_deltaBytes = dltBytes;
    }
    int Decompress(char** outBuf, int outBufSize, char* inBuf, int inBufSize, uint16 modes);

    /* optimizing flags */
    bool m_adopt_rle;

private:
    int64 m_minVal;
    int64 m_maxVal;
    int32 m_nValues;
    short m_valueBytes;
    short m_deltaBytes;

    /* inner implement for compress API */
    template <bool adopt_rle>
    int CompressInner(char** out, int outSize, uint16* outModes, char* in, int inSize);
};

///
/// compress the batch of numeric
///

// int32 integer:
//    ascales in [-2, 2] <--> encoding val [1, 5]
// int64 integer:
//    ascales in [-4, 4] <--> encoding val [6, 14]
// we represent status codes with 4 bits, and 0 means that it
// fails to compress. so 15 is unused.
//
#define FAILED_ENCODING 0
#define MAX_INT32_ASCALE_ENCODING 5
#define MAX_INT64_ASCALE_ENCODING 14
#define DIFF_INT32_ASCALE_ENCODING 3
#define DIFF_INT64_ASCALE_ENCODING 10

// dsacle
// 2 bits flag for dscale encoding
#define NUMERIC_DSCALE_ENCODING 0
#define INT32_DSCALE_ENCODING 1
#define INT64_DSCALE_ENCODING 2

/// bit flags for NUMERIC compressed head info
#define NUMERIC_FLAG_EXIST_OTHERS 0x0001
#define NUMERIC_FLAG_OTHER_WITH_LZ4 0x0002
#define NUMERIC_FLAG_EXIST_INT32_VAL 0x0004
#define NUMERIC_FLAG_INT32_SAME_VAL 0x0008
#define NUMERIC_FLAG_INT32_SAME_ASCALE 0x0010
#define NUMERIC_FLAG_INT32_WITH_DELTA 0x0020
#define NUMERIC_FLAG_INT32_WITH_RLE 0x0040
#define NUMERIC_FLAG_EXIST_INT64_VAL 0x0080
#define NUMERIC_FLAG_INT64_SAME_VAL 0x0100
#define NUMERIC_FLAG_INT64_SAME_ASCALE 0x0200
#define NUMERIC_FLAG_INT64_WITH_DELTA 0x0400
#define NUMERIC_FLAG_INT64_WITH_RLE 0x0800
#define NUMERIC_FLAG_SCALE_WITH_RLE 0x1000
/// notice: add new bit flag above
#define NUMERIC_FLAG_SIZE (2)  // use byte as unit

/// function type definition.
typedef void (*SetNullNumericFunc)(int which, void* memobj);

/// struct type definition
typedef struct {
    bool* success;  /// treat null value as failed.
    char* ascale_codes;
    int64* int64_values;
    int32* int32_values;

    int64 int64_minval;
    int64 int64_maxval;
    int32 int32_minval;
    int32 int32_maxval;
    int int64_n_values;
    int int32_n_values;
    int n_notnull;
    int failed_cnt;
    int failed_size;

    /// it's needed and used to malloc exact memory during decompressing.
    int original_size;

    bool int64_ascales_are_same;
    bool int64_values_are_same;
    /// valid only when int64_ascales_are_same is true.
    char int64_same_ascale;
    bool int32_ascales_are_same;
    bool int32_values_are_same;
    /// valid only when int32_ascales_are_same is true.
    char int32_same_ascale;
} numerics_statistics_out;

typedef struct {
    CompressionArg2 int64_out_args;
    CompressionArg2 int32_out_args;
    int int64_cmpr_size;
    int int32_cmpr_size;

    char* other_srcbuf;
    char* other_outbuf;
    int other_cmpr_size;
    bool other_apply_lz4;

    /* compression filter */
    compression_options* filter;
} numerics_cmprs_out;

/// maybe these values cannot use any compression
/// method. so during decompression we can directly
/// access the memory of compressed data, needn't
/// to copy again, and reduce the number of memcpy()
/// calls.
typedef struct {
    char* int64_addr_ref;
    char* int32_addr_ref;
    char* other_addr_ref;
} numerics_decmprs_addr_ref;

typedef struct {
    Datum* values;
    char* nulls; /* nulls bitmap */
    int rows;

    /// *func* and *mobj* must be set if *hasNull* is true.
    bool hasNull;
    SetNullNumericFunc func;
    /// the second argument of *func*.
    void* mobj;
} BatchNumeric;

/// configure the failed ratio.
/// valid value is 1~100.
extern void NumericConfigFailedRatio(int failed_ratio);

/// given one batch of numeric values, try to compress them.
/// you must set both *setnull* if there are nulls.
/// return the not-exact size of compressed data if compress successfully.
/// otherwise return false, and *compressed_datasize* is undefined.
///
extern bool NumericCompressBatchValues(
    BatchNumeric* batch, numerics_statistics_out* out1, numerics_cmprs_out* out2, int* compressed_datasize, int align_size);

/// copy the data after compress successfully.
extern char* NumericCopyCompressedBatchValues(char* ptr, numerics_statistics_out* out1, numerics_cmprs_out* out2);
extern void NumericCompressReleaseResource(numerics_statistics_out* statOut, numerics_cmprs_out* cmprOut);

/// decompress APIs
extern char* NumericDecompressParseHeader(
    char* ptr, uint16* out_flags, numerics_statistics_out* out1, numerics_cmprs_out* out2);
extern char* NumericDecompressParseAscales(char* ptr, numerics_statistics_out* out1, uint16 flags);
extern char* NumericDecompressParseEachPart(
    char* ptr, numerics_statistics_out* out1, numerics_cmprs_out* out2, numerics_decmprs_addr_ref* out3);
extern char* NumericDecompressParseDscales(char* ptr, numerics_statistics_out* out1, uint16 flags);

template <bool DscaleFlag>
extern void NumericDecompressRestoreValues(char* outBuf, int typeMode, numerics_statistics_out* stat_out,
    numerics_cmprs_out* cmpr_out, numerics_decmprs_addr_ref* in3);
extern void NumericDecompressReleaseResources(
    numerics_statistics_out* decmprStatOut, numerics_cmprs_out* decmprOut, numerics_decmprs_addr_ref* addrRef);

// fill bitmap according to values[n_values]
// bit should be set to 0 if it's equal to *valueIfUnset*,
// otherwise bit should be set to 1.
template <typename T>
extern void FillBitmap(char* buf, const T* values, int nValues, T valueIfUnset)
{
    unsigned char* byteBuf = (unsigned char*)(buf - 1);
    uint32 bitMask = HIGHBIT;

    for (int cnt = 0; cnt < nValues; ++cnt) {
        if (bitMask != HIGHBIT) {
            bitMask <<= 1;
        } else {
            byteBuf++;
            *byteBuf = 0x00;
            bitMask = 1;
        }
        if (valueIfUnset == values[cnt]) {
            continue;
        }
        *byteBuf |= bitMask;
    }
}
#endif
