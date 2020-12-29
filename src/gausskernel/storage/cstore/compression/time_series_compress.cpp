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
 *  time_series_compress.cpp
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/compression/time_series_compress.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "storage/time_series_compress.h"
#include "access/reloptions.h"
#include "storage/compress_kits.h"
#include "nodes/primnodes.h"
#include "catalog/pg_type.h"

const int LZ4_STEP = 2;

/* Array illustrates Delta2 value and its corresponding key */
/* D取值范围	表示D范围的Key值
 * 0		            0000
 * [-2^8+1,0)	        0001
 * (0,2^8-1]	        1001
 * [-2^16+1,0)	        0010
 * (0,2^16-1]	        1010
 * [-2^24+1,0)	        0011
 * (0,2^24-1]	        1011
 * [-2^32+1,0)	        0100
 * (0,2^32-1]	        1100
 * [-2^40+1,0)	        0101
 * (0,2^40-1]	        1101
 * [-2^48+1,0)	        0110
 * (0,2^48-1]	        1110
 * [-2^56+1,0)	        0111
 * (0,2^56-1]	        1111
 * [-2^63+1, 2^63-1]	1000 */
static const Delta2KeyMap g_keyMap[DELTA2_KEY_MAX + 1] = {{0, 0, 0x1, 0x0},
    {0, 0xFF, 0x1, 0x1},
    {0xFF, 0xFFFF, 0x2, 0x2},
    {0xFFFF, 0xFFFFFF, 0x3, 0x3},
    {0xFFFFFF, 0xFFFFFFFF, 0x4, 0x4},
    {0xFFFFFFFF, 0xFFFFFFFFFF, 0x5, 0x5},
    {0xFFFFFFFFFF, 0xFFFFFFFFFFFF, 0x6, 0x6},
    {0xFFFFFFFFFFFF, 0xFFFFFFFFFFFFFF, 0x7, 0x7},
    {0xFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF, 0x8, 0x8}
};

static const int8 g_compresslevel_tables[COMPRESS_HIGH + 1][MAX_COMPRESSLEVEL + 1] = {
    /* COMPRESS_NO */
    {0, 0, 0, 0},
    /* COMPRESS_LOW :: LZ4 */
    {   LZ4Wrapper::lz4_min_level,
        LZ4Wrapper::lz4hc_min_level,
        LZ4Wrapper::lz4hc_min_level + LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_min_level + LZ4Wrapper::lz4hc_level_step * LZ4_STEP
    },
    /* COMPRESS_MIDDLE :: LZ4 */
    {   LZ4Wrapper::lz4hc_recommend_level - LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_recommend_level,
        LZ4Wrapper::lz4hc_recommend_level + LZ4Wrapper::lz4hc_level_step,
        LZ4Wrapper::lz4hc_recommend_level + LZ4Wrapper::lz4hc_level_step * LZ4_STEP
    },
    /* COMPRESS_HIGH :: ZLIB */
    {   ZlibEncoder::zlib_recommend_level,
        ZlibEncoder::zlib_recommend_level + ZlibEncoder::zlib_level_step,
        ZlibEncoder::zlib_recommend_level + ZlibEncoder::zlib_level_step * LZ4_STEP,
        ZlibEncoder::zlib_max_level
    }
};

static FORCE_INLINE void prepare_swap_buf(
    char*& buf1, char*& buf2, int& sz1, int& sz2, char* newbuf2, int newsz2, bool& prepared)
{
    Assert(false == prepared);
    buf1 = buf2;
    buf2 = newbuf2;
    sz1 = sz2;
    sz2 = newsz2;
    prepared = true;
}

static FORCE_INLINE void swap_buf(char*& buf1, char*& buf2, int& sz1, int& sz2)
{
    char* tmp = buf1;
    buf1 = buf2;
    buf2 = tmp;

    int tmpsz = sz1;
    sz1 = sz2;
    sz2 = tmpsz;
}

static FORCE_INLINE unsigned short find_key(int64 value)
{
    unsigned short key_sign = (value <= 0) ? 0x0 : DELTA2_KEY_MAX;
    for (unsigned short index = DELTA2_KEY_MAX / 2;
         index < sizeof(g_keyMap) / sizeof(Delta2KeyMap) - 1;) {
        if (((value <= (int64)g_keyMap[index].max) && (value > (int64)g_keyMap[index].min)) ||
            ((-value <= (int64)g_keyMap[index].max) && (-value > (int64)g_keyMap[index].min))) {
            return (index + key_sign);
        } else if (value > (int64)g_keyMap[index].max || -value > (int64)g_keyMap[index].max) {
            index++;
        } else {
            index--;
        }
    }
    return DELTA2_KEY_MAX;
}

/*************************************************************************
 *                         Delta-of-Delta Compression                     *
 *************************************************************************/
Delta2Codec::Delta2Codec(int64 extra, bool beCompress)
{
    value_size = 0;
    if (!beCompress) {
        value_size = extra;
    }
    key_idx = 0;
}

/**
 * @time_series_compress.cpp
 * @Description: Write delta2 value
 *  key      D-Range
 *  0x0000   [0,0]
 *  0x1000   [-2^63+1, 2^63-1]
 *  0x0XXX   [-2^56+1,0)
 *  0x1XXX   (0, 2^56-1]
 * @in value -  delta2 value
 * @in buf_len - total size of outbuf
 * @out outbuf - compression buffer.
 * @out pos - postion.
 * @return -  true or false
 */
void Delta2Codec::write_value(int64 value, unsigned int buf_len, char* outbuf, unsigned int* pos)
{
    int64 write_data = value;

    Assert(outbuf != NULL && pos != NULL && *pos <= buf_len);
    if (value == 0) {
        *(char*)(outbuf + *pos) = *(char*)&value;
        *pos = *pos + DELTA2_KEY_LEN;
        return;
    }

    unsigned short key = find_key(value);
    if (key == DELTA2_KEY_MAX) {
        key_idx = DELTA2_KEY_MAX;
    } else {
        key_idx = key & DELTA2_KEY_SIGN_MASK;
        int sign = ((key & DELTA2_BYTE_LEN_MASK) == 0) ? -1 : 1;
        uint64 tmp = sign * value;
        write_data = g_keyMap[key_idx].max & tmp;
    }

    *(char*)(outbuf + *pos) = key;
    *pos += DELTA2_KEY_LEN;
    Assert(*pos + g_keyMap[key_idx].byte_len <= buf_len);
    return write_data_by_size(outbuf, pos, write_data, g_keyMap[key_idx].byte_len);
}

int64 Delta2Codec::read_value(_in_ char* inbuf, unsigned int* pos, unsigned int buf_len) const
{
    unsigned short key;
    unsigned short delta2_bytes;
    int sign;
    int64 tmp;

    Assert(inbuf != NULL && pos != NULL);
    Assert(*pos + DELTA2_KEY_LEN <= buf_len);

    tmp = read_data_by_size(inbuf, pos, sizeof(char));
    key = (unsigned short)((unsigned int)tmp & DELTA2_KEY_MASK);
    if (key == 0) {
        return key;
    }

    if (key == DELTA2_KEY_MAX) {
        return read_data_by_size(inbuf, pos, sizeof(int64));
    }

    /* Read delta2 value by key value */
    sign = ((key & DELTA2_BYTE_LEN_MASK) == 0) ? -1 : 1;
    delta2_bytes = key & DELTA2_KEY_SIGN_MASK;
    Assert(*pos + delta2_bytes <= buf_len);
    tmp = read_data_by_size(inbuf, pos, delta2_bytes);

    return sign * tmp;
}

/**
 * @time_series_compress.cpp
 * @Description: Do delta2 compression
 * @in inbuf -  input buffer
 * @in insize - total size of inbuf
 * @in outsize - total size of inbuf
 * @out outbuf - compression buffer.
 * @return -  total size of compressed buffer
 */
int64 Delta2Codec::compress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
                            _in_ unsigned short data_size)
{
    int64 read_data;
    int64 write_data;
    int64 pre_data = 0;
    int64 pre_delta = 0;
    int count = 0;
    unsigned int in_pos = 0;
    unsigned int out_pos = 0;

    Assert(insize > 0 && inbuf != NULL && outbuf != NULL);

    do {
        read_data = read_data_by_size(inbuf, &in_pos, data_size);
        if (count == 0) {
            write_data = read_data;
            Assert(out_pos + data_size <= outsize);
            write_data_by_size(outbuf, &out_pos, write_data, data_size);
        } else if (count == 1) {
            /* Warning: write_data may ouversize the biggest value of write_data type */
            write_data = read_data - pre_data;
            pre_delta = write_data;
            write_value(write_data, outsize, outbuf, &out_pos);
        } else {
            write_data = (read_data - pre_data) - pre_delta;
            pre_delta += write_data;
            write_value(write_data, outsize, outbuf, &out_pos);
        }
        pre_data = read_data;
        count++;
    } while (likely((in_pos + data_size) <= insize));

    return out_pos;
}

/**
 * @time_series_compress.cpp
 * @Description: Do delta2 De-compression
 * @in inbuf -  input buffer
 * @in insize - total size of inbuf
 * @in outsize - total size of inbuf
 * @in data_size - data size
 * @out outbuf - compression buffer.
 * @return -  total size of de-compressed buffer
 */
int64 Delta2Codec::decompress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
                              _in_ unsigned short data_size) const
{
    int count = 0;
    unsigned int in_pos = 0;
    unsigned int out_pos = 0;
    int64 read_data;
    int64 write_data;
    int64 pre_delta;
    short next_read_Len = data_size;

    Assert(insize > 0 && inbuf != NULL && outbuf != NULL);

    do {
        if (count == 0) {
            read_data = read_data_by_size(inbuf, &in_pos, data_size);
            write_data = read_data;
        } else if (count == 1) {
            pre_delta = read_value(inbuf, &in_pos, insize);
            write_data += pre_delta;
        } else {
            int64 delta2_val = read_value(inbuf, &in_pos, insize);
            write_data += (pre_delta + delta2_val);
            pre_delta += delta2_val;
        }
        Assert(out_pos + data_size <= outsize);
        write_data_by_size(outbuf, &out_pos, write_data, data_size);
        count++;
        next_read_Len = DELTA2_KEY_LEN;
    } while (likely((unsigned int)(in_pos + next_read_Len) <= insize));

    return out_pos;
}

/*************************************************************************
 *                         Sequence Codec                                 *
 *************************************************************************/
SequenceCodec::SequenceCodec(short valSize, uint32 dataType)
{
    value_size = valSize;
    data_type = dataType;
}

int SequenceCodec::lz4_compress(
    const int8 compression, const int8 compress_level, Size bound_size, BufferHelper* tmpOutBuf,
    _out_ CompressionArg2& out)
{
    LZ4Wrapper lz4;
    lz4.SetCompressionLevel(g_compresslevel_tables[compression][compress_level]);
    bound_size = lz4.CompressGetBound(out.sz);
    if (bound_size > tmpOutBuf->bufSize) {
        BufferHelperRemalloc(tmpOutBuf, bound_size);
    }
    return lz4.Compress(out.buf, tmpOutBuf->buf, out.sz);
}

int SequenceCodec::zlib_compress(
    int8 compression, int8 compress_level, Size bound_size, BufferHelper* tmpOutBuf, _out_ CompressionArg2& out)
{
    ZlibEncoder zlib;
    int compress_size;
    zlib.Prepare(g_compresslevel_tables[compression][compress_level]);
    bound_size = zlib.CompressGetBound(out.sz);
    if (bound_size > tmpOutBuf->bufSize) {
        BufferHelperRemalloc(tmpOutBuf, bound_size);
    }
    bool done = false;
    zlib.Reset((unsigned char*)out.buf, out.sz);
    compress_size = zlib.Compress((unsigned char*)tmpOutBuf->buf, (int)tmpOutBuf->bufSize, done);
    return (done == false ? 0 : compress_size);
}

/**
 * @time_series_compress.cpp
 * @Description: Do Sequence compression
 * @in in -  input compression arg
 * @out out - output compression arg
 * @return -  total size of compressed buffer
 */
int SequenceCodec::compress(_in_ const CompressionArg1& in, _out_ CompressionArg2& out)
{
    Size bound_size = 0;
    int8 compression = heaprel_get_compression_from_modes(in.mode);
    int8 compress_level = heaprel_get_compresslevel_from_modes(in.mode);
    Assert(compression >= COMPRESS_LOW);
    Assert(compression <= COMPRESS_HIGH);
    Assert(value_size > 0);
    Assert((in.sz % this->value_size) == 0);

    ereport(DEBUG1,
            (ERRCODE_DATA_CORRUPTED,
             errmsg(
                 "Seq codec: size= %d, compression = %d, level = %d", this->value_size, compression, compress_level)));
    BufferHelper tmpOutBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&tmpOutBuf, (in.sz / this->value_size) * (this->value_size + 1));

    int compress_size = 0;
    if (ATT_IS_TIMESTAMP(data_type)) {
        Delta2Codec delta2(this->value_size, true);
        compress_size = delta2.compress(in.buf, tmpOutBuf.buf, in.sz, tmpOutBuf.bufSize, this->value_size);
    } else if (ATT_IS_FLOAT(data_type)) {
        XORCodec xorCompress;
        compress_size = xorCompress.compress(in.buf, tmpOutBuf.buf, in.sz, tmpOutBuf.bufSize, this->value_size);
    }

    if (compress_size >= in.sz) {
        BufferHelperFree(&tmpOutBuf);
        ereport(DEBUG1, (ERRCODE_DATA_CORRUPTED, errmsg("SeqCodex err, out_sz= %d, in.sz = %d", compress_size, in.sz)));
        return -1;
    }
    error_t rc = memcpy_s(out.buf, out.sz, tmpOutBuf.buf, compress_size);
    securec_check(rc, "", "");
    out.sz = compress_size;
    if (ATT_IS_TIMESTAMP(data_type)) {
        out.modes |= CU_Delta2Compressed;
    } else {
        out.modes |= CU_XORCompressed;
    }

    if (COMPRESS_LOW == compression) {
        BufferHelperFree(&tmpOutBuf);
        return ((0 != out.modes) ? out.sz : 0);
    } else if (COMPRESS_MIDDLE == compression) {
        compress_size = lz4_compress(compression, compress_level, bound_size, &tmpOutBuf, out);
    } else if (COMPRESS_HIGH == compression) {
        compress_size = zlib_compress(compression, compress_level, bound_size, &tmpOutBuf, out);
    }
    if (compress_size > 0 && compress_size < out.sz) {
        rc = memset_s(out.buf, out.sz, 0, out.sz);
        securec_check(rc, "", "");
        rc = memcpy_s(out.buf, out.sz, tmpOutBuf.buf, compress_size);
        securec_check(rc, "", "");
        out.sz = compress_size;
        out.modes |= ((COMPRESS_MIDDLE == compression) ? CU_LzCompressed : CU_ZlibCompressed);
    }

    BufferHelperFree(&tmpOutBuf);
    return ((0 != out.modes) ? out.sz : 0);
}

/**
 * @time_series_compress.cpp
 * @Description: Do Sequence de-compression
 * @in in -  input compression arg
 * @out out - output compression arg
 * @return -  total size of de-compressed buffer
 */
int SequenceCodec::decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out)
{
    BufferHelper tmpBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&tmpBuf, out.sz);

    char* next_out_buf = out.buf;
    char* next_in_buf = in.buf;
    int next_out_size = 0;
    int next_in_size = in.sz;
    bool is_prepared = false;

    if (0 != (in.modes & CU_LzCompressed)) {
        LZ4Wrapper lzDecoder;
        Assert(lzDecoder.DecompressGetBound(next_in_buf) > 0 && lzDecoder.DecompressGetBound(next_in_buf) <= out.sz);
        next_out_size = lzDecoder.Decompress(next_in_buf, next_out_buf, next_in_size);
        Assert((next_out_size > 0) && (next_out_size <= out.sz));
        prepare_swap_buf(next_in_buf, next_out_buf, next_in_size, next_out_size, tmpBuf.buf, out.sz, is_prepared);
    } else if (0 != (in.modes & CU_ZlibCompressed)) {
        ZlibDecoder zlibDecoder;
        next_out_size = zlibDecoder.Prepare((unsigned char*)next_in_buf, next_in_size);
        if (0 == next_out_size) {
            bool done = false;
            next_out_size = zlibDecoder.Decompress((unsigned char*)next_out_buf, out.sz, done, true);
            Assert(done && (next_out_size > 0) && (next_out_size <= out.sz));

            prepare_swap_buf(next_in_buf, next_out_buf, next_in_size, next_out_size, tmpBuf.buf, out.sz, is_prepared);
        } else {
            BufferHelperFree(&tmpBuf);
            return -2;
        }
    }

    /* Delta-delta de-compress */
    if (CU_Delta2Compressed == (in.modes & CU_Delta2Compressed)) {
        Delta2Codec delta2(value_size, false);
        next_out_size = delta2.decompress(next_in_buf, next_out_buf, next_in_size, out.sz, value_size);
        /* XOR de-compress */
    } else if (CU_XORCompressed == (in.modes & CU_XORCompressed)) {
        XORCodec xorCompress;
        next_out_size = xorCompress.decompress(next_in_buf, next_out_buf, next_in_size, out.sz, value_size);
    }

    Assert(next_out_size > next_in_size && next_out_size <= out.sz);
    if (is_prepared) {
        swap_buf(next_in_buf, next_out_buf, next_in_size, next_out_size);
    } else {
        prepare_swap_buf(next_in_buf, next_out_buf, next_in_size, next_out_size, tmpBuf.buf, out.sz, is_prepared);
    }

    Assert(next_in_size <= out.sz);
    errno_t rc = memcpy_s(out.buf, out.sz, next_in_buf, next_in_size);
    securec_check(rc, "", "");

    BufferHelperFree(&tmpBuf);
    return next_in_size;
}

/*************************************************************************
 *                         XOR Compression                                *
 *************************************************************************/
XORCodec::XORCodec()
{
    numBits_ = 0;
    previousValue_ = 0;
    previousValueLeadingZeros_ = 0;
    previousValueTrailingZeros_ = 0;
    bitsStore_ = 0;
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR compression
 * @in inbuf -  input buffer
 * @in insize - total size of inbuf
 * @in outsize - total size of inbuf
 * @in data_size - each data byte size
 * @out outbuf - compression buffer.
 * @return -  total size of compressed buffer
 */
int64 XORCodec::compress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
                         _in_ unsigned short data_size)
{
    int64 read_data;
    int64 write_data;
    unsigned short data_step = data_size;
    unsigned int in_pos = 0;
    unsigned int out_pos = 0;

    Assert(insize > 0 && inbuf != NULL && outbuf != NULL);

    do {
        read_data = read_data_by_size(inbuf, &in_pos, data_size);
        write_data = read_data;
        appendValue(write_data, outbuf, out_pos);
    } while (likely((in_pos + data_step) <= insize));
    flushLastBitString(outbuf, out_pos);

    return out_pos;
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR appendValue
 * @in value -  input value
 * @out out_pos - outbuffer write position
 * @out outbuf - compression buffer.
 * @return -  NULL
 */
void XORCodec::appendValue(int64 value, char* outbuf, unsigned int& out_pos)
{
    uint64_t* p = (uint64_t*)&value;
    uint64_t xorWithPrevius = previousValue_ ^ *p;

    if (xorWithPrevius == 0) {
        addValueToBitString(0, 1, numBits_, outbuf, out_pos);
        return;
    }

    addValueToBitString(1, 1, numBits_, outbuf, out_pos);

    uint32_t leadingZeros = __builtin_clzll(xorWithPrevius);
    uint32_t trailingZeros = __builtin_ctzll(xorWithPrevius);

    if (leadingZeros > kMaxLeadingZerosLength) {
        leadingZeros = kMaxLeadingZerosLength;
    }

    int const floatSize = 64;
    int blockSize = floatSize - leadingZeros - trailingZeros;
    uint32_t expectedSize = kLeadingZerosLengthBits + kBlockSizeLengthBits + blockSize;
    uint32_t previousBlockInformationSize = floatSize - previousValueTrailingZeros_ - previousValueLeadingZeros_;

    if (leadingZeros >= previousValueLeadingZeros_ && trailingZeros >= previousValueTrailingZeros_ &&
        previousBlockInformationSize < expectedSize) {
        /* Control bit for using previous block information. */
        addValueToBitString(1, 1, numBits_, outbuf, out_pos);

        uint64_t blockValue = xorWithPrevius >> previousValueTrailingZeros_;
        addValueToBitString(blockValue, previousBlockInformationSize, numBits_, outbuf, out_pos);
    } else {
        /* Control bit for not using previous block information. */
        addValueToBitString(0, 1, numBits_, outbuf, out_pos);

        addValueToBitString(leadingZeros, kLeadingZerosLengthBits, numBits_, outbuf, out_pos);

        /* To fit in 6 bits. There will never be a zero size block */
        addValueToBitString(blockSize - kBlockSizeAdjustment, kBlockSizeLengthBits, numBits_, outbuf, out_pos);

        uint64_t blockValue = xorWithPrevius >> trailingZeros;
        addValueToBitString(blockValue, blockSize, numBits_, outbuf, out_pos);

        previousValueTrailingZeros_ = trailingZeros;
        previousValueLeadingZeros_ = leadingZeros;
    }
    previousValue_ = *p;
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR addValueToBitString
 * @in value -  input XOR parse value
 * @in bitsInValue - input value bits
 * @in numBits - value write bit count
 * @out out_pos - outbuffer write position
 * @out outbuf - compression buffer.
 * @return -  NULL
 */
void XORCodec::addValueToBitString(
    uint64_t value, uint64_t bitsInValue, uint32_t& numBits, char* outbuf, unsigned int& out_pos)
{
    uint32_t const byteSize = 8;
    uint32_t bitsAvailable = (numBits & 0x7) ? (byteSize - (numBits & 0x7)) : 0;

    if (bitsInValue <= bitsAvailable) {
        numBits += bitsInValue;
        /* Everything fits inside the last byte */
        bitsStore_ += (value << (bitsAvailable - bitsInValue));
        flushBitString(numBits, outbuf, out_pos);
        return;
    }

    uint32_t bitsLeft = bitsInValue;
    if (bitsAvailable > 0) {
        numBits += bitsAvailable;
        /* Fill up the last byte */
        bitsStore_ += (value >> (bitsInValue - bitsAvailable));
        flushBitString(numBits, outbuf, out_pos);
        bitsLeft -= bitsAvailable;
    }

    while (bitsLeft >= byteSize) {
        numBits += byteSize;
        /* Enough bits for a dedicated byte */
        char ch = (value >> (bitsLeft - byteSize)) & 0xFF;
        bitsStore_ += ch;
        flushBitString(numBits, outbuf, out_pos);
        bitsLeft -= byteSize;
    }

    if (bitsLeft != 0) {
        numBits += bitsLeft;
        /* Start a new byte with the rest of the bits */
        char ch = (char)((value & ((1U << bitsLeft) - 1)) << (uint32_t)(byteSize - bitsLeft));
        bitsStore_ += ch;
    }
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR flushBitString
 * @in numBits - value write bit count
 * @out out_pos - outbuffer write position
 * @out outbuf - compression buffer.
 * @return -  NULL
 */
void XORCodec::flushBitString(const uint32_t& numBits, char* outbuf, unsigned int& out_pos)
{
    uint32_t const byteSize = 8;
    unsigned short data_size_byte = 1;
    int64 byte_data = (int64)(bitsStore_);
    if ((numBits % byteSize) == 0) {
        write_data_by_size(outbuf, &out_pos, byte_data, data_size_byte);
        bitsStore_ = 0;
    }
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR flushLastBitString
 * @out out_pos - outbuffer write position
 * @out outbuf - compression buffer.
 * @return -  NULL
 */
void XORCodec::flushLastBitString(char* outbuf, unsigned int& out_pos)
{
    uint32_t const byteSize = 8;
    unsigned short data_size_byte = 1;
    int64 byte_data = (int64)(bitsStore_);
    if ((numBits_ % byteSize) != 0) {
        write_data_by_size(outbuf, &out_pos, byte_data, data_size_byte);
        bitsStore_ = 0;
    }
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR decompression
 * @in inbuf -  input buffer
 * @in insize - total size of inbuf
 * @in outsize - total size of inbuf
 * @in data_size - each data byte size
 * @out outbuf - decompression buffer.
 * @return -  total size of decompressed buffer
 */
int64 XORCodec::decompress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
                           _in_ unsigned short data_size)
{
    uint64_t previousValue = 0;
    uint64_t previousLeadingZeros = 0;
    uint64_t previousTrailingZeros = 0;
    uint64_t bitPos = 0;

    int64 read_data = 0;
    int64 write_data = 0;
    unsigned short data_step = data_size;
    unsigned int in_pos = 0;
    unsigned int out_pos = 0;
    unsigned short data_size_byte = 1;
    Assert(data_size != 0);
    Assert(insize > 0 && inbuf != NULL && outbuf != NULL);

    unsigned int loop_count = (outsize / data_size);
    read_data = read_data_by_size(inbuf, &in_pos, data_size_byte);
    bitsStore_ = (char)read_data;

    do {
        write_data = readNextValue(bitPos, previousValue, previousLeadingZeros, previousTrailingZeros, inbuf, in_pos);
        write_data_by_size(outbuf, &out_pos, write_data, data_step);
        loop_count--;
    } while (likely(loop_count));

    return out_pos;
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR readNextValue
 * @in previousValue -  input previous value
 * @in previousLeadingZeros - previous leading zero size
 * @in previousTrailingZeros - previous trailing zero size
 * @in bitPos - value write bit count
 * @out in_pos - outbuffer write position
 * @out inbuf - decompression buffer.
 * @return -  decompress value
 */
int64 XORCodec::readNextValue(uint64_t& bitPos, uint64_t& previousValue, uint64_t& previousLeadingZeros,
                              uint64_t& previousTrailingZeros, char* inbuf, unsigned int& in_pos)
{
    uint64_t nonZeroValue = readValueFromBitString(bitPos, 1, inbuf, in_pos);
    if (!nonZeroValue) {
        int64 return_value = (int64)previousValue;
        return return_value;
    }

    uint64_t usePreviousBlockInformation = readValueFromBitString(bitPos, 1, inbuf, in_pos);
    uint64_t xorValue;
    int floatSize = 64;
    if (usePreviousBlockInformation) {
        xorValue =
            readValueFromBitString(bitPos, floatSize - previousLeadingZeros - previousTrailingZeros, inbuf, in_pos);
        xorValue <<= previousTrailingZeros;
    } else {
        uint64_t leadingZeros = readValueFromBitString(bitPos, kLeadingZerosLengthBits, inbuf, in_pos);
        uint64_t blockSize = readValueFromBitString(bitPos, kBlockSizeLengthBits, inbuf, in_pos) + kBlockSizeAdjustment;
        previousTrailingZeros = floatSize - blockSize - leadingZeros;
        xorValue = readValueFromBitString(bitPos, blockSize, inbuf, in_pos);
        xorValue <<= previousTrailingZeros;
        previousLeadingZeros = leadingZeros;
    }

    uint64_t value = xorValue ^ previousValue;
    previousValue = value;
    int64 return_value = (int64)value;
    return return_value;
}

/**
 * @time_series_compress.cpp
 * @Description: Do XOR flushBitString
 * @in bitPos - value write bit count
 * @in bitsToRead - value to read bits
 * @out in_pos - outbuffer write position
 * @out inbuf - decompression buffer.
 * @return -  bits to read value
 */
uint64_t XORCodec::readValueFromBitString(uint64_t& bitPos, uint32_t bitsToRead, char* inbuf, unsigned int& in_pos)
{
    uint64_t value = 0;
    int const byteSize = 8;
    uint8_t const byteMoveSize = 7;
    for (uint32_t i = 0; i < bitsToRead; i++) {
        value <<= 1;
        uint8_t un_bitsStore = (uint8_t)bitsStore_;
        uint64_t bit = (un_bitsStore >> (byteMoveSize - (bitPos & 0x7))) & 1;
        value += bit;
        bitPos++;
        if ((bitPos % byteSize) == 0) {
            bitsStore_ = (char)read_data_by_size(inbuf, &in_pos, sizeof(char));
        }
    }
    return value;
}
