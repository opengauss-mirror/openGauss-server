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
 * time_series_compress.h
 *        Data structure for delta-delta compression and XOR compression.
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/time_series_compress.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SERIES_COMPRESS_H
#define SERIES_COMPRESS_H

#include "c.h"
#include "storage/cstore/cstore_compress.h"

const int DELTA2_RANGE = 8;
const int MAX_COMPRESSLEVEL = 3;

const unsigned short DELTA2_KEY_MAX = 0x8;

const int CU_COMPRESS_MASK1 = 0x000F;

#define ATT_IS_DELTA2_SUPPORT(atttypid)                                                                    \
    (atttypid == DATEOID || atttypid == TIMESTAMPOID || atttypid == INT4RANGEOID || atttypid == INT2OID || \
        atttypid == INT4OID || atttypid == INT8OID)

#define ATT_IS_TIMESTAMP(atttypid) (atttypid == TIMESTAMPOID || atttypid == TIMESTAMPTZOID)

#define ATT_IS_FLOAT(atttypid) (atttypid == FLOAT4OID || atttypid == FLOAT8OID)

typedef struct Delta2KeyMap {
    long unsigned int min;
    long unsigned int max;
    unsigned short key_suffix;
    unsigned short byte_len;
} Delta2KeyMap;

class Delta2Codec : public BaseObject {
public:
    Delta2Codec(int64 extra, bool beCompress);
    virtual ~Delta2Codec()
    {}

    int64 compress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
        _in_ unsigned short data_size);

    int64 decompress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
        _in_ unsigned short data_size) const;

private:
    void write_value(int64 value, unsigned int buf_len, char* outbuf, unsigned int* pos);
    int64 read_value(char* inbuf, unsigned int* pos, unsigned int buf_len) const;
    short value_size;
    short key_idx;
    static const short DELTA2_KEY_MASK = 0xFF;
    static const short DELTA2_KEY_SIGN_MASK = 0x7;
    static const short DELTA2_BYTE_LEN_MASK = 0x8;
    static const unsigned int DELTA2_KEY_LEN = 1; /* delta key len--1 byte */
};

class SequenceCodec : public BaseObject {
public:
    SequenceCodec(short valSize, uint32 dataType);
    virtual ~SequenceCodec()
    {}

    int lz4_compress(
        int8 compression, int8 compress_level, Size bound_size, BufferHelper* tmpOutBuf, _out_ CompressionArg2& out);
    int zlib_compress(
        int8 compression, int8 compress_level, Size bound_size, BufferHelper* tmpOutBuf, _out_ CompressionArg2& out);
    int compress(_in_ const CompressionArg1& in, _out_ CompressionArg2& out);
    int decompress(_in_ const CompressionArg2& in, _out_ CompressionArg1& out);

private:
    short value_size;
    uint32 data_type;
};

class XORCodec : public BaseObject {
public:
    XORCodec();
    virtual ~XORCodec()
    {}

    int64 compress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
        _in_ unsigned short data_size);

    int64 decompress(_in_ char* inbuf, _out_ char* outbuf, _in_ unsigned int insize, _in_ unsigned int outsize,
        _in_ unsigned short data_size);

private:
    // write compress value to write buffer
    void appendValue(int64 value, char* outbuf, unsigned int& out_pos);
    void addValueToBitString(
        uint64_t value, uint64_t bitsInValue, uint32_t& numBits, char* outbuf, unsigned int& out_pos);
    void flushBitString(const uint32_t& numBits, char* outbuf, unsigned int& out_pos);
    void flushLastBitString(char* outbuf, unsigned int& out_pos);

    // read compress value to read buffer
    int64 readNextValue(uint64_t& bitPos, uint64_t& previousValue, uint64_t& previousLeadingZeros,
        uint64_t& previousTrailingZeros, char* inbuf, unsigned int& in_pos);
    uint64_t readValueFromBitString(uint64_t& bitPos, uint32_t bitsToRead, char* inbuf, unsigned int& in_pos);

    static constexpr uint32_t kLeadingZerosLengthBits = 5;
    static constexpr uint32_t kBlockSizeLengthBits = 6;
    static constexpr uint32_t kMaxLeadingZerosLength = (1 << kLeadingZerosLengthBits) - 1;
    static constexpr uint32_t kBlockSizeAdjustment = 1;

    uint64_t previousValue_;
    uint32_t numBits_;
    uint8_t previousValueLeadingZeros_;
    uint8_t previousValueTrailingZeros_;
    char bitsStore_;
};

#endif
