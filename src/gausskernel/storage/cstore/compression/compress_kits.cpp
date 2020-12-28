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
 * compress_kits.cpp
 *      kits/tools/implement for compression methods
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/compression/compress_kits.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/hash.h"
#include "storage/compress_kits.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "nodes/memnodes.h"
#include "lz4.h"
#include "lz4hc.h"

/* The macro to validate if the return value is available */
#define MEMPROT_ALLOC_VALID(buf, size)                                                                               \
    {                                                                                                                \
        if (NULL == (buf))                                                                                           \
            ereport(ERROR,                                                                                           \
                    (errcode(ERRCODE_OUT_OF_MEMORY),                                                                     \
                     errmsg("memory is temporarily unavailable"),                                                     \
                     errdetail("Failed on request of size %lu bytes under storage engine.", (unsigned long)(size)))); \
    }

/*
 * @Description: buffer malloc
 * @Param[IN/OUT] p: buffer to malloc
 * @Param[IN] s: buffer size needed
 * @See also:
 */
void BufferHelperMalloc(BufferHelper* p, Size in_size)
{
    Assert(Unknown == p->bufType);
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.

    if (in_size + 8 <= 0) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg(" palloc size to big, size(%ld).", (long)in_size)));
    }
    Size s = in_size + 8;
    if (s < MaxAllocSize) {
        /* malloc from memory context */
        p->bufType = FromMemCnxt;
        p->buf = (char*)palloc(s);
    } else {
        /* malloc from protected memory */
        p->bufType = FromMemProt;
        p->buf = (char*)GS_MEMPROT_MALLOC(s, true);
        MEMPROT_ALLOC_VALID(p->buf, s);
        gs_atomic_add_64(&storageTrackedBytes, s);
    }
    p->bufSize = s;
}

/*
 * @Description: remalloc buffer
 * @Param[IN/OUT] p: the existing buffer
 * @Param[IN] s: new buffer size
 * @See also:
 */
void BufferHelperRemalloc(BufferHelper* p, Size in_size)
{
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
    if (in_size + 8 <= 0) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg(" palloc size to big, size(%ld).", (long)in_size)));
    }
    Size s = in_size + 8;
    if (FromMemCnxt == p->bufType) {
        if (s < MaxAllocSize) {
            p->buf = (char*)repalloc(p->buf, s);
            p->bufSize = s;
        } else {
            /* fetch big memory, and copy original data. */
            char* buf = (char*)GS_MEMPROT_MALLOC(s, true);
            MEMPROT_ALLOC_VALID(buf, s);
            gs_atomic_add_64(&storageTrackedBytes, s);

            errno_t ecode = memcpy_s(buf, s, p->buf, p->bufSize);
            securec_check(ecode, "", "");
            pfree(p->buf);

            p->buf = buf;
            p->bufSize = s;
            p->bufType = FromMemProt;
        }
    } else {
        Assert(FromMemProt == p->bufType);
        if (t_thrd.utils_cxt.gs_mp_inited) {
            /* realloc directly from protected memory */
            p->buf = (char*)GS_MEMPROT_REALLOC(p->buf, p->bufSize, s, true);
            MEMPROT_ALLOC_VALID(p->buf, s - p->bufSize);
            gs_atomic_add_64(&storageTrackedBytes, s - p->bufSize);
            p->bufSize = s;
        } else {
            /* malloc a new bigger memory from protected memory */
            char* buf = (char*)GS_MEMPROT_MALLOC(s, true);
            MEMPROT_ALLOC_VALID(buf, s);
            gs_atomic_add_64(&storageTrackedBytes, s);

            /* copy data into new buffer */
            errno_t ecode = memcpy_s(buf, s, p->buf, p->bufSize);
            securec_check(ecode, "", "");

            /* free old buffer */
            GS_MEMPROT_FREE(p->buf, p->bufSize);
            gs_atomic_add_64(&storageTrackedBytes, -1 * p->bufSize);

            /* remember the new buffer and its size */
            p->buf = buf;
            p->bufSize = s;
            /* ignore to set p->bufType */
        }
    }
}

/*
 * @Description: buffer free
 * @Param[IN] p: buffer pointer to free
 * @See also:
 */
void BufferHelperFree(BufferHelper* p)
{
    Assert(Unknown != p->bufType);
    if (FromMemCnxt == p->bufType) {
        pfree(p->buf);
    } else if (FromMemProt == p->bufType) {
        GS_MEMPROT_FREE(p->buf, p->bufSize);
        gs_atomic_add_64(&storageTrackedBytes, -1 * p->bufSize);
    }
    p->buf = NULL;
    p->bufSize = 0;
}

#ifdef USE_ASSERT_CHECKING
static void RleCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize,
                                   _in_ short eachValSize, _in_ unsigned int minRepeats);

static void DeltaCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize,
                                     _in_ short eachRawValSize, _in_ short eachCmprValSize, _in_ int64 minVal, _in_ int64 maxVal);

static void Lz4CheckCompressedData(
    _in_ const char* rawData, _in_ int rawDataSize, _in_ const char* cmprBuf, _in_ int cmprBufSize);

static void ZlibCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize);

static void DictCheckCompressedData(
    _in_ char* rawData, _in_ int rawDataSize, _in_ DictHeader* dicData, _in_ DicCodeType* dicCodes, _in_ int nDicCodes);

#endif

template <short datasize>
static FORCE_INLINE int64 readData(char* inbuf, unsigned int* inpos)
{
    char* psrc = inbuf + *inpos;
    int64 val = *(int64*)psrc;

#ifdef WORDS_BIGENDIAN

    switch (datasize) {
        case sizeof(char):
            val = val >> 56;
            break;

        case sizeof(int16):
            val = val >> 48;
            break;

        case sizeof(int32):
            val = val >> 32;
            break;

        case sizeof(int64):
            val = val;
            break;

        case 3:
            val = val >> 40;
            break;

        case 5:
            val = val >> 24;
            break;

        case 6:
            val = val >> 16;
            break;

        case 7:
            val = val >> 8;
            break;

        default:
            Assert(false);
            break;
    }

#else /* !WORDS_BIGENDIAN */

    switch (datasize) {
        case sizeof(char):
            val = val & 0xFF;
            break;

        case sizeof(int16):
            val = val & 0xFFFF;
            break;

        case sizeof(int32):
            val = val & 0xFFFFFFFF;
            break;

        case sizeof(int64):
            val = val;
            break;

        case 3:
            val = val & 0xFFFFFF;
            break;

        case 5:
            val = val & 0xFFFFFFFFFF;
            break;

        case 6:
            val = val & 0xFFFFFFFFFFFF;
            break;

        case 7:
            val = val & 0xFFFFFFFFFFFFFF;
            break;

        default:
            Assert(false);
            break;
    }

#endif /* WORDS_BIGENDIAN */

    *inpos += datasize;
    return val;
}

template <short datasize>
static FORCE_INLINE void writeData(char* out, unsigned int* outpos, int64 data)
{
    char* pdst = out + *outpos;
    char* data_pos = (char*)&data;

    switch (datasize) {
        case sizeof(char):
            *(char*)pdst = (char)(data);
            break;

        case sizeof(int16):
            *(int16*)pdst = (int16)(data);
            break;

        case sizeof(int32):
            *(int32*)pdst = (int32)(data);
            break;

        case sizeof(int64):
            *(int64*)pdst = (int64)(data);
            break;

#ifdef WORDS_BIGENDIAN

        case 3:
            data_pos = data_pos + 5;
            *(int16*)pdst = *(int16*)data_pos;
            pdst = pdst + 2;
            data_pos = data_pos + 2;
            *(char*)pdst = *data_pos;
            break;

        case 5:
            data_pos = data_pos + 3;
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(char*)pdst = *data_pos;
            break;

        case 6:
            data_pos = data_pos + 2;
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(int16*)pdst = *(int16*)data_pos;
            break;

        case 7:
            data_pos = data_pos + 1;
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(int16*)pdst = *(int16*)data_pos;
            pdst = pdst + 2;
            data_pos = data_pos + 2;
            *(char*)pdst = *data_pos;
            break;

        default:
            Assert(false);
    }

#else /* !WORDS_BIGENDIAN */

        case 3:
            *(int16*)pdst = *(int16*)data_pos;
            pdst = pdst + 2;
            data_pos = data_pos + 2;
            *(char*)pdst = *data_pos;
            break;

        case 5:
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(char*)pdst = *data_pos;
            break;

        case 6:
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(int16*)pdst = *(int16*)data_pos;
            break;

        case 7:
            *(int32*)pdst = *(int32*)data_pos;
            pdst = pdst + 4;
            data_pos = data_pos + 4;
            *(int16*)pdst = *(int16*)data_pos;
            pdst = pdst + 2;
            data_pos = data_pos + 2;
            *(char*)pdst = *data_pos;
            break;

        default:
            Assert(false);
            break;
    }

#endif /* WORDS_BIGENDIAN */

    *outpos += datasize;
}

/*************************************************************************
 *                             RLE Compression
 *
 * There are several different ways to do RLE. Instead of coding runs
 * for both repeating and non-repeating sections, a special marker is
 * used to indicate the start of a repeating section. Non-repeating
 * sections can thus have any length without being interrupted by control
 * bytes, except for the rare case when the special marker appears in
 * the non-repeating section (which is coded with at most two bytes).
 *
 * Repeating runs can be as long as 32767 times. Runs shorter than 128 times
 * require two metadata for coding (MARKER + COUNT), whereas runs longer than
 * or equal to 128 times require three metadata for coding (MARKER +
 * COUNT_HIGH|0x80 + COUNT_LOW). This is normally a double-win in compression
 * and it's very seldom a loss of compression ratio compared to using a fixed
 * metadata coding (which allows coding a run of 256 times ).
 *
 * With this scheme, the worst case compression result is
 *            insize +  (insize / m_eachValSize) * (1/2) + 1
 *
 *************************************************************************/
// different m_eachValSize, different marker used, just only repeating 0xFE.
// m_eachValSize is the index of this array.
const int64 RleCoder::RleMarker[] = {(int64)0,
                                     (int64)0x00000000000000FE,
                                     (int64)0x000000000000FEFE,
                                     (int64)0x0000000000FEFEFE,
                                     (int64)0x00000000FEFEFEFE,
                                     (int64)0x000000FEFEFEFEFE,
                                     (int64)0x0000FEFEFEFEFEFE,
                                     (int64)0x00FEFEFEFEFEFEFE,
                                     (int64)0xFEFEFEFEFEFEFEFE
                                    };

// make sure that RLE method can save some space than plain storage for X repeats values.
// X means the repeating times.
// then it should be: X > 2 + 1.0 / m_eachValSize       ( 1 <= m_eachValSize <= 8)
// so min repeats is computed, which is 4.
const unsigned int RleCoder::RleMinRepeats_v1 = 4;

const unsigned int RleCoder::RleMinRepeats_v2[] = {0, 4, 3, 3, 3, 3, 3, 3, 3};

// the highest bit is used to indecate that repeats number is stored with
// two bytes (bit = 1) or one byte (bit = 0)
const unsigned int RleCoder::RleMaxRepeats = 0x7fff;

// Recommend: outsize is computed by calling CompressGetBound(), and then
//   passed into Compress().
int RleCoder::Compress(_in_ char* inbuf, __inout char* outbuf, _in_ const int insize, _in_ const int outsize)
{
    int ret = 0;
    switch (m_eachValSize) {
        case sizeof(char):
            ret = InnerCompress<sizeof(char)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int16):
            ret = InnerCompress<sizeof(int16)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int32):
            ret = InnerCompress<sizeof(int32)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int64):
            ret = InnerCompress<sizeof(int64)>(inbuf, outbuf, insize, outsize);
            break;

        case 3:
            ret = InnerCompress<3>(inbuf, outbuf, insize, outsize);
            break;

        case 5:
            ret = InnerCompress<5>(inbuf, outbuf, insize, outsize);
            break;

        case 6:
            ret = InnerCompress<6>(inbuf, outbuf, insize, outsize);
            break;

        case 7:
            ret = InnerCompress<7>(inbuf, outbuf, insize, outsize);
            break;

        default:
            Assert(false);
            return -1; /* make complier slience */
    }
    return ret;
}

template <short eachValSize>
int RleCoder::InnerCompress(char* inbuf, char* outbuf, const int insize, const int outsize)
{
    Assert(insize > 0);
    int64 data1 = 0;
    int64 data2 = 0;
    unsigned int inpos = 0;
    unsigned int outpos = 0;
    unsigned int count = 0;

    data1 = readData<eachValSize>(inbuf, &inpos);
    count = 1;
    if (likely(insize >= (2 * eachValSize))) {
        data2 = readData<eachValSize>(inbuf, &inpos);
        count = 2;

        // Main compression loop
        do {
            if (data1 == data2) {
                // scan a sequence of identical block data, until another exception happens
                while ((data1 == data2) && ((inpos + eachValSize) <= (unsigned int)insize) &&
                       (count < RleCoder::RleMaxRepeats)) {
                    data2 = readData<eachValSize>(inbuf, &inpos);
                    ++count;
                }

                if (data1 == data2) {
                    if (count >= this->m_minRepeats)
                        this->WriteRuns<eachValSize>(outbuf, &outpos, data1, count);
                    else
                        this->WriteNonRuns<eachValSize>(outbuf, &outpos, data1, count);

                    // repeating data have been written, and then read the new data1;
                    count = 0;
                    if ((inpos + eachValSize) <= (unsigned int)insize) {
                        data1 = readData<eachValSize>(inbuf, &inpos);
                        count = 1;
                    }
                } else {
                    --count;
                    Assert(count > 0);
                    if (count >= this->m_minRepeats)
                        this->WriteRuns<eachValSize>(outbuf, &outpos, data1, count);
                    else
                        this->WriteNonRuns<eachValSize>(outbuf, &outpos, data1, count);
                    data1 = data2;
                    count = 1;
                }
            } else {
                Assert(count == 2);
                this->WriteNonRuns<eachValSize>(outbuf, &outpos, data1);
                data1 = data2;
                count = 1;
            }
            Assert(outpos <= (unsigned int)outsize);

            /* read the new data2 to continue to compress; */
            if ((inpos + eachValSize) <= (unsigned int)insize) {
                data2 = readData<eachValSize>(inbuf, &inpos);
                Assert(count == 1);
                count = 2;
            }

            /* make sure there is no input data when count is either 1 or 0; */
            Assert(count == 2 || count == 1 || count == 0);
            Assert((count >= 2) || ((inpos + eachValSize) > (unsigned int)insize));
        } while (count >= 2);
    }

    /*
     * make sure all input-data are handled and compressed.
     * And at most one data is left out.
     */
    Assert(inpos == (unsigned int)insize);
    Assert(count == 0 || count == 1);
    if (count == 1)
        this->WriteNonRuns<eachValSize>(outbuf, &outpos, data1);

    Assert(outpos <= (unsigned int)outsize);
#ifdef USE_ASSERT_CHECKING
    RleCheckCompressedData(inbuf, insize, outbuf, outpos, eachValSize, this->m_minRepeats);
#endif
    return outpos;
}

// Notice: compression ratio should be remembered somewhere. so the outsize can be
//   set enough to decompress. and we can ignore checking outbuf's size
int RleCoder::Decompress(_in_ char* inbuf, _out_ char* outbuf, _in_ const int insize, _in_ const int outsize)
{
    int ret = 0;
    switch (m_eachValSize) {
        case sizeof(char):
            ret = InnerDecompress<sizeof(char)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int16):
            ret = InnerDecompress<sizeof(int16)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int32):
            ret = InnerDecompress<sizeof(int32)>(inbuf, outbuf, insize, outsize);
            break;

        case sizeof(int64):
            ret = InnerDecompress<sizeof(int64)>(inbuf, outbuf, insize, outsize);
            break;

        case 3:
            ret = InnerDecompress<3>(inbuf, outbuf, insize, outsize);
            break;

        case 5:
            ret = InnerDecompress<5>(inbuf, outbuf, insize, outsize);
            break;

        case 6:
            ret = InnerDecompress<6>(inbuf, outbuf, insize, outsize);
            break;

        case 7:
            ret = InnerDecompress<7>(inbuf, outbuf, insize, outsize);
            break;

        default:
            Assert(false);
            return -1; /* make complier slience */
    }
    return ret;
}

template <short eachValSize>
int RleCoder::InnerDecompress(char* inbuf, char* outbuf, const int insize, const int outsize)
{
    Assert(insize >= 1);
    unsigned int inpos = 0;
    unsigned int outpos = 0;
    int outcnt = 0;
    char* inptr = inbuf;

    // Main decompression loop
    do {
        int64 symbol = readData<eachValSize>(inptr, &inpos);

        // maybe We had a marker data, check it first
        if (RleCoder::RleMarker[eachValSize] == symbol) {
            uint8 markerCount = *(uint8*)(inptr + inpos);

            if (markerCount >= this->m_minRepeats) {
                // [RleMinRepeats, RleMaxRepeats] indicates the compressed Runs data.
                uint16 symbolCount = 0;

                // the first bit represents whether tow bytes are
                // used to store the length info.
                if (markerCount & 0x80) {
                    symbolCount = ((uint16)(markerCount << 8) + *(uint8*)(inptr + inpos + 1)) & 0x7fff;
                    inpos += sizeof(uint16);
                } else {
                    symbolCount = markerCount;
                    ++inpos;
                }
                Assert(symbolCount >= this->m_minRepeats && symbolCount <= RleCoder::RleMaxRepeats);

                symbol = readData<eachValSize>(inptr, &inpos);
                for (uint16 i = 0; i < symbolCount; ++i) {
                    writeData<eachValSize>(outbuf, &outpos, symbol);
                    Assert(outpos <= (unsigned int)outsize);
                }
                outcnt += symbolCount;
            } else {
                // [1, RleMinRepeats - 1] indicates that symbol is the same to marker itself,
                // and repeat time is markerCount.
                ++inpos;
                Assert(markerCount > 0);
                for (uint8 i = 0; i < markerCount; ++i) {
                    writeData<eachValSize>(outbuf, &outpos, symbol);
                    Assert(outpos <= (unsigned int)outsize);
                }
                outcnt += markerCount;
            }
        } else {
            // No marker, copy the plain data
            writeData<eachValSize>(outbuf, &outpos, symbol);
            Assert(outpos <= (unsigned int)outsize);
            ++outcnt;
        }
    } while (likely((inpos + eachValSize) <= (unsigned int)insize));

    Assert(inpos == (unsigned int)insize);
    Assert(outpos <= (unsigned int)outsize);
    return (eachValSize * outcnt);
}

// the default repeat is 1 when NonRuns does appear.
// otherwise, although Runs already happens, but the repeats is smaller than RLEMinRepeats,
// so these Runs will be degraded to NonRuns and written plainly.
template <short eachValSize>
void RleCoder::WriteNonRuns(char* outbuf, unsigned int* outpos, int64 symbol, unsigned int repeat)
{
    unsigned int idx = *outpos;
    unsigned int i;

    Assert(repeat < this->m_minRepeats);
    if (likely(RleCoder::RleMarker[eachValSize] != symbol)) {
        // in normal case Non-Runs will be written plainly into out-buffer.
        for (i = 0; i < repeat; ++i)
            writeData<eachValSize>(outbuf, &idx, symbol);
    } else {
        // Special case: this symbol is the same to marker. the format is
        //      MARKER + REPEAT
        // where REPEAT is in [1, RleMinRepeats - 1] and only holds one byte.
        // In normal runs, the storage format is:
        //      MARKER + REPEAT + SYMBOL
        // where REPEAT is equal to or greater than RleMinRepeats.
        // it's safe to convert repeat to CHAR type.
        writeData<eachValSize>(outbuf, &idx, symbol);
        outbuf[idx++] = (char)repeat;
    }

    Assert(this->NonRunsNeedSpace((RleCoder::RleMarker[eachValSize] == symbol), repeat) == (int16)(idx - *outpos));
    *outpos = idx;
}

/*
 * RUNs format as followings:
 *	  marker: it holds <m_datasize> bytes;
 *	  repeats: it holds 1B when repeat < 128; otherwise 2B when repeat < 0x7fff;
 *	  data: it holds <m_datasize> bytes;
 */
template <short eachValSize>
void RleCoder::WriteRuns(char* outbuf, unsigned int* outpos, int64 symbol, unsigned int repeat)
{
    unsigned int idx = *outpos;

    Assert(repeat >= this->m_minRepeats && repeat <= RleCoder::RleMaxRepeats);
    writeData<eachValSize>(outbuf, &idx, RleCoder::RleMarker[eachValSize]);
    if (repeat >= 128) {
        /*
         * it's not safe to assign by uint16 datatype because of BE/LE problem.
         * outbuf[idx] holds the higher byte, and outbuf[idx+1] holds the lower byte.
         */
        *(uint8*)(outbuf + idx) = (uint8)((repeat | 0x8000) >> 8);
        *(uint8*)(outbuf + idx + 1) = (uint8)(repeat & 0xff);
        idx += sizeof(uint16);
    } else {
        /* it's safe to convert unsigned int to uint8, becase repeat is in [0, 127] */
        *(uint8*)(outbuf + idx) = (uint8)repeat;
        ++idx;
    }
    writeData<eachValSize>(outbuf, &idx, symbol);

    Assert(this->RunsNeedSpace(repeat) == (int16)(idx - *outpos));
    *outpos = idx;
}

#ifdef USE_ASSERT_CHECKING

int16 RleCoder::NonRunsNeedSpace(bool equalToMarker, int16 repeats)
{
    Assert(repeats > 0 && repeats < (int16)m_minRepeats);
    return equalToMarker ? (m_markersize + 1) : (m_eachValSize * repeats);
}

int16 RleCoder::RunsNeedSpace(int16 repeats)
{
    Assert(repeats >= (int16)m_minRepeats && repeats <= (int16)RleCoder::RleMaxRepeats);
    return (this->m_markersize + (repeats < 128 ? 1 : 2) + this->m_eachValSize);
}

#endif

/*************************************************************************
 *                         Delta Compression                              *
 *************************************************************************/
DeltaCoder::DeltaCoder(int64 mindata, int64 extra, bool willCompress) : m_mindata(mindata), m_outValSize(0)
{
    if (willCompress) {
#ifdef USE_ASSERT_CHECKING
        m_maxdata = extra;
#endif
        m_outValSize = DeltaGetBytesNum(mindata, extra);
    } else
        m_outValSize = (short)extra;
}

template <bool PlusDelta, short inValSize>
int DeltaCoder::DoDeltaOperation(_in_ char* inbuf, _out_ char* outbuf, _in_ int insize, _in_ short outValSize)
{
    int ret = 0;
    switch (outValSize) {
        case sizeof(char):
            ret = doDeltaOperation<PlusDelta, inValSize, sizeof(char)>(inbuf, outbuf, insize);
            break;

        case sizeof(int16):
            ret = doDeltaOperation<PlusDelta, inValSize, sizeof(int16)>(inbuf, outbuf, insize);
            break;

        case sizeof(int32):
            ret = doDeltaOperation<PlusDelta, inValSize, sizeof(int32)>(inbuf, outbuf, insize);
            break;

        case sizeof(int64):
            ret = doDeltaOperation<PlusDelta, inValSize, sizeof(int64)>(inbuf, outbuf, insize);
            break;

        case 3:
            ret = doDeltaOperation<PlusDelta, inValSize, 3>(inbuf, outbuf, insize);
            break;

        case 5:
            ret = doDeltaOperation<PlusDelta, inValSize, 5>(inbuf, outbuf, insize);
            break;

        case 6:
            ret = doDeltaOperation<PlusDelta, inValSize, 6>(inbuf, outbuf, insize);
            break;

        case 7:
            ret = doDeltaOperation<PlusDelta, inValSize, 7>(inbuf, outbuf, insize);
            break;

        default:
            Assert(false);
            break;
    }
    return ret;
}

// doDeltaOperation
//	inbuf: the input data buffer
//  insize: the size of inbuf
//  outbuf: the output buffer
//  inDataSize: the each data value size
template <bool PlusDelta, short inValSize, short outValSize>
int DeltaCoder::doDeltaOperation(_in_ char* inbuf, _out_ char* outbuf, _in_ int insize)
{
    unsigned int inpos = 0;
    unsigned int outpos = 0;

    // Warning !!!
    // it's a problem that wdata is of type int64, because when
    //   1) PlusDelta is false,
    //   2) rdata is the biggest value of type int64,
    //   3) m_mindata is the smallest value of type int64,
    // then the difference will be the biggest value of type uint64, which
    // is out of range int64 datatype.
    // luckly our delta value is byte bound, and when decompressing inValSize will
    // not be 8 bytes. so that it's safe here wdata is defined as int64 type.
    int64 rdata = 0;
    int64 wdata = 0;
    uint32 outcnt = 0;

    do {
        rdata = readData<inValSize>(inbuf, &inpos);
        if (PlusDelta)
            wdata = rdata + m_mindata;
        else
            wdata = rdata - m_mindata;
        writeData<outValSize>(outbuf, &outpos, wdata);
        ++outcnt;
    } while (likely(((int)inpos + inValSize) <= insize));

    return (outValSize * outcnt);
}

// Compress
//	inbuf: the input data buffer
//  insize: the size of inbuf
//  outbuf: the output buffer
//  outsize: the sizeof outbuf
//  inDataSize: the each data value size
int64 DeltaCoder::Compress(
    _in_ char* inbuf, _out_ char* outbuf, _in_ int insize, _in_ int outsize, _in_ short inDataSize)
{
    int64 ret = 0;
    Assert(insize > 0);
    Assert(insize == ((insize / inDataSize) * inDataSize));
    if (unlikely((int64)outsize < (int64)GetBound((insize / inDataSize))))
        return 0;

    switch (inDataSize) {
        case sizeof(char):
            // because our delta value is byte bound, CHAR datatype shouldn't be compressed
            Assert(false);
            break;

        case sizeof(int16):
            ret = DoDeltaOperation<false, sizeof(int16)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case sizeof(int32):
            ret = DoDeltaOperation<false, sizeof(int32)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case sizeof(int64):
            ret = DoDeltaOperation<false, sizeof(int64)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 3:
            ret = DoDeltaOperation<false, 3>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 5:
            ret = DoDeltaOperation<false, 5>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 6:
            ret = DoDeltaOperation<false, 6>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 7:
            ret = DoDeltaOperation<false, 7>(inbuf, outbuf, insize, m_outValSize);
            break;

        default:
            Assert(false);
            break;
    }

#ifdef USE_ASSERT_CHECKING
    DeltaCheckCompressedData(inbuf, insize, outbuf, ret, inDataSize, m_outValSize, m_mindata, m_maxdata);
#endif

    return ret;
}

int64 DeltaCoder::Decompress(
    _in_ char* inbuf, _out_ char* outbuf, _in_ int insize, _in_ int outsize, _in_ short inDataSize)
{
    int64 ret = 0;
    Assert(insize > 0);
    Assert(insize == ((insize / inDataSize) * inDataSize));

    switch (inDataSize) {
        case sizeof(char):
            ret = DoDeltaOperation<true, sizeof(char)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case sizeof(int16):
            ret = DoDeltaOperation<true, sizeof(int16)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case sizeof(int32):
            ret = DoDeltaOperation<true, sizeof(int32)>(inbuf, outbuf, insize, m_outValSize);
            break;

        case sizeof(int64):
            // because our delta value is byte bound, INT64 value shouldn't be decompressed.
            Assert(false);
            break;

        case 3:
            ret = DoDeltaOperation<true, 3>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 5:
            ret = DoDeltaOperation<true, 5>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 6:
            ret = DoDeltaOperation<true, 6>(inbuf, outbuf, insize, m_outValSize);
            break;

        case 7:
            ret = DoDeltaOperation<true, 7>(inbuf, outbuf, insize, m_outValSize);
            break;

        default:
            Assert(false);
            break;
    }
    return ret;
}

/*************************************************************************
 *                         Dictionary Compression                         *
 *************************************************************************/
// we think dictionary compress ratio shouldn't be smaller than 4.0.
// it's adding 1 that makes sure the result is bigger than 0.
#define DictGetMaxNumber(_v) ((int)((_v)*1.0 / 4) + 1)

#define InvalidPosition 0xFFFFFFFF
#define FreeHashSlot 0xFFFF

DicCoder::DicCoder(char* inBuf, int inSize, int numVals, int maxItemCnt)
{
    m_dataLen = 0;
    m_inBuf = inBuf;
    m_inSize = inSize;
    m_numVals = numVals;

    InitDictData(DictGetMaxNumber(m_inSize), ((maxItemCnt > 0) ? maxItemCnt : DictGetMaxNumber(m_numVals)));
}

DicCoder::DicCoder(int dataLen, int maxItemCnt)
{
    m_numVals = 0;
    m_dataLen = dataLen;

    InitDictData(maxItemCnt * m_dataLen, maxItemCnt);
}

void DicCoder::InitDictData(int itemsMaxSize, int itemsMaxCount)
{
    m_dictData.m_maxItemCount = itemsMaxCount;
    m_dictData.m_itemsMaxSize = itemsMaxSize;
    m_dictData.m_curItemCount = 0;
    m_dictData.m_itemUsedSize = 0;

    // Dictionary Data in memory : Struct DictHeader + Dict Items Data + Hash Slots
    m_dictData.m_header = (DictHeader*)palloc(ComputeDictMaxSize(itemsMaxSize, itemsMaxCount));
    m_dictData.m_data = (char*)m_dictData.m_header + sizeof(DictHeader);
    m_dictData.m_slots = (DicCodeType*)(m_dictData.m_data + m_dictData.m_itemsMaxSize);
    errno_t rc = memset_s(m_dictData.m_data, m_dictData.m_itemsMaxSize, 0, m_dictData.m_itemsMaxSize);
    securec_check(rc, "", "");

    // init hash slots
    int hashCount = GetHashSlotsCount();
    for (int i = 0; i < hashCount; ++i)
        m_dictData.m_slots[i] = FreeHashSlot;

    // m_dicItemPos[i] is start position of the i-value, and m_dicItemPos[i+1] is end position
    // of the i-value. so the extra (itemsMaxCount+1) is used to record end position of the
    // itemsMaxCount-value.
    const int n = 1 + itemsMaxCount;
    m_dictData.m_itemOffset = (uint32*)palloc(sizeof(uint32) * n);
    m_dictData.m_itemOffset[0] = 0;
    for (int i = 1; i < n; ++i)
        m_dictData.m_itemOffset[i] = InvalidPosition;
}

// each result compressed with dictionary method is a DicCodeType data.
// the bound size just is sizeof(DicCodeType) multiplied by nVals.
int DicCoder::CompressGetBound(void) const
{
    return (m_numVals * sizeof(DicCodeType));
}

int DicCoder::Compress(_out_ char* outBuf)
{
    DicCodeType* outptr = (DicCodeType*)outBuf;
    char* inptr = m_inBuf;
    int inpos = 0;
    int cnt = 0;

    while (inpos < m_inSize) {
        Datum oneVal = PointerGetDatum(inptr + inpos);
        if (EncodeOneValue(oneVal, outptr[cnt])) {
            ++cnt;
            inpos += VARSIZE_ANY(oneVal);
            continue;
        }
        break;
    }
    Assert((cnt <= m_numVals) && (inpos <= m_inSize));

    // compete dictionary coding, return the results' size
    if ((inpos == m_inSize) && (m_dictData.m_itemUsedSize + sizeof(DictHeader) < (unsigned int)m_inSize)) {
        // dictionary compress successfully, so fill the meta data for dictionary data
        Assert(cnt == m_numVals);
        m_dictData.m_header->m_totalSize = sizeof(DictHeader) + m_dictData.m_itemUsedSize;
        m_dictData.m_header->m_itemsCount = m_dictData.m_curItemCount;

#ifdef USE_ASSERT_CHECKING
        DictCheckCompressedData(m_inBuf, m_inSize, m_dictData.m_header, outptr, m_numVals);
#endif

        return CompressGetBound();
    }
    return 0;
}

DicCoder::~DicCoder()
{
    if (m_dictData.m_header != NULL) {
        pfree(m_dictData.m_header);
        m_dictData.m_header = NULL;
    }

    if (m_dictData.m_itemOffset != NULL) {
        pfree(m_dictData.m_itemOffset);
        m_dictData.m_itemOffset = NULL;
    }
    m_inBuf = NULL;
}

bool DicCoder::EncodeOneValue(_in_ Datum datum, _out_ DicCodeType& result)
{
    Size datumLen = VARSIZE_ANY(datum);
    char* datumChars = DatumGetPointer(datum);
    uint32 hashValue = hash_any((unsigned char*)datumChars, datumLen);
    uint32 slotCount = GetHashSlotsCount();
    uint32 slotIndex = hashValue % slotCount;
    DicCodeType* hashSlots = m_dictData.m_slots;
    errno_t rc = EOK;

    for (int64 conflict = 0; conflict < slotCount; ++conflict) {
        DicCodeType dictItemIdx = hashSlots[slotIndex];

        if (FreeHashSlot == hashSlots[slotIndex]) {
            if (IsEnoughToAppend(datumLen)) {
                // add new item into my dictionary
                dictItemIdx = m_dictData.m_curItemCount++;
                hashSlots[slotIndex] = dictItemIdx;
                rc = memcpy_s((m_dictData.m_data + m_dictData.m_itemUsedSize), datumLen, datumChars, datumLen);
                securec_check(rc, "", "");
                m_dictData.m_itemUsedSize += datumLen;
                result = dictItemIdx;

                // dictItemIdx increases by 1. we record each value's position to speed up finding
                // the contents of the i-value and comparing with the input value.
                Assert(InvalidPosition != m_dictData.m_itemOffset[dictItemIdx]);
                Assert(InvalidPosition == m_dictData.m_itemOffset[dictItemIdx + 1]);
                m_dictData.m_itemOffset[dictItemIdx + 1] = m_dictData.m_itemOffset[dictItemIdx] + datumLen;
                return true;
            }

            // not enough space, false returned
            return false;
        }

        Assert(dictItemIdx < m_dictData.m_curItemCount);
        Assert(InvalidPosition != m_dictData.m_itemOffset[dictItemIdx]);
        char* dicDatumChars = m_dictData.m_data + m_dictData.m_itemOffset[dictItemIdx];
        Size dicDatumLen = VARSIZE_ANY(PointerGetDatum(dicDatumChars));
        if (dicDatumLen == datumLen && 0 == memcmp(dicDatumChars, datumChars, datumLen)) {
            // find the same existing item
            result = dictItemIdx;
            return true;
        }

        // hash conflict, and search the next hash slot
        slotIndex = (slotIndex + 1) % slotCount;
    }

    // keep complier slient
    Assert(false);
    return false;
}

// Initialize dictionary item, m_dicItemPos according to dicInDisk
DicCoder::DicCoder(char* dictInDisk)
{
    m_dataLen = 0;
    m_inBuf = NULL;
    m_inSize = 0;
    m_numVals = 0;
    DictHeader* dictHeader = m_dictData.m_header = (DictHeader*)dictInDisk;
    m_dictData.m_data = (char*)dictHeader + sizeof(DictHeader);
    m_dictData.m_itemOffset = (uint32*)palloc(sizeof(uint32) * (dictHeader->m_itemsCount + 1));

    // fill m_dicItemPos[] to record the positions of all items, to speed
    // up the decompressing effect.
    int dictDataSize = dictHeader->m_totalSize - sizeof(DictHeader);
    int itemIdex = 0;
    m_dictData.m_itemOffset[itemIdex] = 0;
    while ((int)m_dictData.m_itemOffset[itemIdex] < dictDataSize) {
        Datum datum = PointerGetDatum(m_dictData.m_data + m_dictData.m_itemOffset[itemIdex]);
        Size datumLen = VARSIZE_ANY(datum);
        m_dictData.m_itemOffset[itemIdex + 1] = m_dictData.m_itemOffset[itemIdex] + datumLen;
        ++itemIdex;
    }
    Assert(itemIdex == (int)dictHeader->m_itemsCount);
    Assert(dictDataSize == (int)m_dictData.m_itemOffset[itemIdex]);
}

// FUTURE CASE: decompress data into shared cache, maybe memcpy() is needed.
int DicCoder::Decompress(char* inBuf, int inBufSize, char* outBuf, int outBufSize)
{
    DicCodeType* itemIndex = (DicCodeType*)inBuf;
    DicCodeType itemCount = inBufSize / sizeof(DicCodeType);
    int outPos = 0;
    errno_t rc = EOK;

    for (DicCodeType i = 0; i < itemCount; ++i) {
        char* pItem = m_dictData.m_data + m_dictData.m_itemOffset[itemIndex[i]];
        Size itemLen = VARSIZE_ANY(pItem);
        rc = memcpy_s(outBuf + outPos, itemLen, pItem, itemLen);
        securec_check(rc, "", "");
        outPos += itemLen;
    }
    Assert(outPos <= outBufSize);

    // because m_dictData.m_header is just a reference to input buffer,
    // so here reset it and avoid to free by ~DicDataWrapper() method.
    m_dictData.m_header = NULL;
    m_dictData.m_data = NULL;
    return outPos;
}

void DicCoder::DecodeOneValue(_in_ DicCodeType itemIndx, _out_ Datum* result) const
{
    char* pItem = m_dictData.m_data + m_dictData.m_itemOffset[itemIndx];
    *result = PointerGetDatum(pItem);
}

/*************************************************************************
 *                             LZ4Wrapper                                *
 *************************************************************************/
void LZ4Wrapper::SetCompressionLevel(int8 level)
{
    Assert(level >= LZ4Wrapper::lz4_min_level && level <= LZ4Wrapper::lz4hc_max_level);
    m_compressionLevel = level;
}

int LZ4Wrapper::CompressGetBound(int insize) const
{
    return SizeOfLz4Header + LZ4_compressBound(insize);
}

/*
 * @Description: compress input data 'source', whose size is 'sourceSize',
 *      using LZ4 and write the results to 'dest'. The parameter compression
 *      level is represented by class parameter m_compressionLevel. Must call
 *      CompressGetBound() to ensure that 'dest' buffer is large enough.
 * @OUT dest: output data buffer
 * @IN source: input data buffer
 * @IN sourceSize: input data size
 * @Return: return 0 if compress fails; otherwise return compressed data size.
 * @See also:
 */
int LZ4Wrapper::Compress(const char* source, char* dest, int sourceSize) const
{
    int outsize = 0;
    Lz4Header* header = (Lz4Header*)dest;

    if (lz4_min_level == m_compressionLevel) {
        /* call the fastest LZ4 API without compression level */
        outsize = LZ4_compress_default(source, header->data, sourceSize, LZ4_compressBound(sourceSize));
    } else {
        /* call HZ4 HC-API with compression level */
        outsize = LZ4_compress_HC(source, header->data, sourceSize, LZ4_compressBound(sourceSize), m_compressionLevel);
    }

    if (Benefited(outsize, sourceSize)) {
        header->compressLen = outsize;
        header->rawLen = sourceSize;
#ifdef USE_ASSERT_CHECKING
        Lz4CheckCompressedData(source, sourceSize, dest, (int)(SizeOfLz4Header + outsize));
#endif
        return (SizeOfLz4Header + outsize);
    }
    return 0;
}

int LZ4Wrapper::DecompressGetBound(const char* source) const
{
    Lz4Header* header = (Lz4Header*)source;
    Assert(Benefited(header->compressLen, header->rawLen));
    return header->rawLen;
}

int LZ4Wrapper::DecompressGetCmprSize(const char* source) const
{
    Lz4Header* header = (Lz4Header*)source;
    Assert(Benefited(header->compressLen, header->rawLen));
    return header->compressLen + SizeOfLz4Header;
}

// Make dest buffer's size is enough to hold uncompressed data.
// To make it, call getDecompressBound() to get the raw data size
// before calling decompress().
int LZ4Wrapper::Decompress(const char* source, char* dest, int sourceSize) const
{
    Lz4Header* header = (Lz4Header*)source;
    Assert(sourceSize == ((int)SizeOfLz4Header + header->compressLen));
    int destSize = DecompressGetBound(source);
    return LZ4_decompress_safe(header->data, dest, header->compressLen, destSize);
}

/*************************************************************************
 *                           ZLIB Compression                             *
 *************************************************************************/
ZlibEncoder::ZlibEncoder() : m_flush(Z_NO_FLUSH)
{
    errno_t rc = memset_s(&m_strm, sizeof(m_strm), 0, sizeof(m_strm));
    securec_check(rc, "", "");
    m_errno = Z_OK;
}

ZlibEncoder::~ZlibEncoder()
{
    (void)deflateEnd(&m_strm);
}

// prepare and init *m_strm* for compression.
// Notice:
//   because constructor method cannot handle exception,
//   so call deflateInit() and handle its returning value.
void ZlibEncoder::Prepare(int level)
{
    m_errno = deflateInit(&m_strm, level);

    switch (m_errno) {
        case Z_OK:
            return;

        case Z_MEM_ERROR:
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory when preparing zlib encoder.")));
            break;

        case Z_STREAM_ERROR:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("level %d is invalid when preparing zlib encoder.", level)));
            break;

        case Z_VERSION_ERROR:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("version is incompatible when preparing zlib encoder.")));
            break;

        default:
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("error %d occurs when preparing zlib encoder.", m_errno)));
            break;
    }
}

int ZlibEncoder::CompressGetBound(int insize)
{
    return (int)deflateBound(&m_strm, (uLong)insize);
}

void ZlibEncoder::Reset(unsigned char* inbuf, const int insize)
{
    m_flush = Z_NO_FLUSH;
    m_strm.avail_in = (const unsigned int)insize;
    m_strm.next_in = inbuf;
}

int ZlibEncoder::Compress(unsigned char* outbuf, const int outsize, bool& done)
{
#ifdef USE_ASSERT_CHECKING
    unsigned char* tempInbuf = m_strm.next_in;
    size_t tempInSize = m_strm.avail_in;
#endif

    done = false;
    m_strm.avail_out = (const unsigned int)outsize;
    m_strm.next_out = outbuf;
    m_errno = deflate(&m_strm, m_flush);
    if (m_errno < 0)
        return m_errno;

    if (m_flush == Z_NO_FLUSH) {
        // if outbuf is too small to catch compressed data, raw data will
        // be left and avail_in is greater than 0. otherwise, avail_in is
        // equal to 0.
        if (m_strm.avail_in > 0)
            return (outsize - m_strm.avail_out);

        // if compression has been done, make m_flush be Z_FINISH and call
        // deflate() again. so that all the compressed data must be written
        // into outbuf.
        m_flush = Z_FINISH;
        m_errno = deflate(&m_strm, m_flush);
    }

    // Compress successfully only if m_errno is Z_STREAM_END.
    // even if m_strm.avail_in is 0 and m_errno is Z_OK, it doesn't make sure
    // that all pending results are written into outbuf.
    done = (m_errno == Z_STREAM_END);
    if (done) {
        Assert(m_strm.avail_in == 0);
        m_errno = Z_OK;
        int sz = (outsize - m_strm.avail_out);
#ifdef USE_ASSERT_CHECKING
        ZlibCheckCompressedData((char*)tempInbuf, (int)tempInSize, (char*)outbuf, sz);
#endif
        (void)deflateReset(&m_strm);
        return sz;
    }

    // compression doesn't finish completely. user should call this method
    // again by passing new output-buffer.
    Assert(m_errno == Z_OK);
    return (outsize - m_strm.avail_out);
}

ZlibDecoder::ZlibDecoder() : m_flush(Z_NO_FLUSH)
{
    m_strm.zalloc = Z_NULL;
    m_strm.zfree = Z_NULL;
    m_strm.opaque = Z_NULL;
    m_strm.avail_in = 0;
    m_strm.next_in = Z_NULL;
    m_errno = inflateInit(&m_strm);
}

ZlibDecoder::~ZlibDecoder()
{
    m_errno = inflateEnd(&m_strm);
    Assert(m_errno == Z_OK);
}

int ZlibDecoder::Prepare(unsigned char* inbuf, const int insize)
{
    // because constructor method cannot handle exception, so check it here.
    // maybe it's not a good idea.
    // FUTURE CASE: do something to fixed me
    if (m_errno != Z_OK) {
        return m_errno;
    }

    m_flush = Z_NO_FLUSH;
    m_strm.avail_in = (const unsigned int)insize;
    m_strm.next_in = inbuf;
    return Z_OK;
}

int ZlibDecoder::Decompress(unsigned char* outbuf, const int outsize, bool& done, bool bufIsEnough)
{
    if (bufIsEnough)
        m_flush = Z_FINISH;

    m_strm.next_out = outbuf;
    m_strm.avail_out = (const unsigned int)outsize;
    m_errno = inflate(&m_strm, m_flush);

    switch (m_errno) {
        case Z_NEED_DICT: {
            m_errno = Z_DATA_ERROR;
        }
        /* fall-through */
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            done = false;
            return m_errno;
        default:
            break;
    }

    // if enough outbuf is provided with, then bufIsEnough should be set true. decompression will
    // be done by calling inflate() only once and m_strm.avail_in will be 0.
    // zlib requires an extra padding byte at the end of the stream in order to successfully complete
    // decompression. so maybe it ends up with a Z_BUF_ERROR at the end of a stream when zlib has
    // completed inflation and there's no more input. Thats not a problem. so we don't care this case
    // if m_strm.avail_in is 0.
    done = (m_strm.avail_in == 0) && (m_errno == Z_STREAM_END || m_errno == Z_BUF_ERROR);
    Assert(!bufIsEnough || done);
    if (done) {
        m_errno = Z_OK;
        return (outsize - m_strm.avail_out);
    }

    // decompression doesn't finish completely. it says, some part of compressed data has been handled,
    // and the piece of uncompressed data are stored into outbuf. user shoud call this method again by
    // passing in new output buffer.
    Assert(m_errno == Z_OK);
    return (outsize - m_strm.avail_out);
}

#ifdef USE_ASSERT_CHECKING

// decompress and check immediately after compressing at running time
static void RleCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize,
                                   _in_ short eachValSize, _in_ unsigned int minRepeats)
{
    BufferHelper uncmprBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&uncmprBuf, rawDataSize);

    RleCoder rle(eachValSize, minRepeats);
    int uncmprSize = rle.Decompress(cmprBuf, uncmprBuf.buf, cmprBufSize, rawDataSize);
    Assert(uncmprSize == rawDataSize);
    Assert(memcmp(uncmprBuf.buf, rawData, rawDataSize) == 0);

    BufferHelperFree(&uncmprBuf);
}

static void DeltaCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize,
                                     _in_ short eachRawValSize, _in_ short eachCmprValSize, _in_ int64 minVal, _in_ int64 maxVal)
{
    short inValSize = DeltaGetBytesNum(minVal, maxVal);
    Assert(inValSize == eachCmprValSize);

    BufferHelper uncmprBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&uncmprBuf, rawDataSize);

    DeltaCoder delta(minVal, eachRawValSize, false);
    int uncmprSize = delta.Decompress(cmprBuf, uncmprBuf.buf, cmprBufSize, rawDataSize, eachCmprValSize);
    Assert(uncmprSize == rawDataSize);
    Assert(memcmp(uncmprBuf.buf, rawData, rawDataSize) == 0);

    BufferHelperFree(&uncmprBuf);
}

static void Lz4CheckCompressedData(
    _in_ const char* rawData, _in_ int rawDataSize, _in_ const char* cmprBuf, _in_ int cmprBufSize)
{
    LZ4Wrapper lz4;
    int uncmprSize = lz4.DecompressGetBound(cmprBuf);
    Assert(uncmprSize > 0 && uncmprSize == rawDataSize);

    BufferHelper uncmprBuf = {NULL, 0, Unknown};
    BufferHelperMalloc(&uncmprBuf, uncmprSize);

    int realDataSize = lz4.Decompress(cmprBuf, uncmprBuf.buf, cmprBufSize);
    Assert(realDataSize > 0 && realDataSize == rawDataSize);
    Assert(memcmp(uncmprBuf.buf, rawData, rawDataSize) == 0);

    BufferHelperFree(&uncmprBuf);
}

static void ZlibCheckCompressedData(_in_ char* rawData, _in_ int rawDataSize, _in_ char* cmprBuf, _in_ int cmprBufSize)
{
    BufferHelper uncmprBuf = {NULL, 0, Unknown};
    bool done = false;
    BufferHelperMalloc(&uncmprBuf, (Size)rawDataSize + ZLIB_EXTRA_SIZE);

    ZlibDecoder zlib;
    int retCode = zlib.Prepare((unsigned char*)cmprBuf, cmprBufSize);
    Assert(retCode == 0);
    int uncmprSize = zlib.Decompress((unsigned char*)uncmprBuf.buf, uncmprBuf.bufSize, done, true);
    Assert(uncmprSize == rawDataSize);
    Assert(memcmp(uncmprBuf.buf, rawData, rawDataSize) == 0);

    BufferHelperFree(&uncmprBuf);
}

static void DictCheckCompressedData(
    _in_ char* rawData, _in_ int rawDataSize, _in_ DictHeader* dicData, _in_ DicCodeType* dicCodes, _in_ int nDicCodes)
{
    DicCoder* dict = New(CurrentMemoryContext) DicCoder((char*)dicData);
    DictHeader* dictHeader = dict->GetHeader();
    Assert(dictHeader->m_totalSize > 0);
    Assert(dictHeader->m_itemsCount > 0);
    for (int i = 0; i < nDicCodes; ++i) {
        // dicCodes[i] is between [0, dictHeader->m_itemsCount - 1]
        Assert(dicCodes[i] >= 0 && dicCodes[i] <= (dictHeader->m_itemsCount - 1));
    }

    char* uncmprBuf = (char*)palloc(rawDataSize);
    int uncmprSize = dict->Decompress((char*)dicCodes, sizeof(DicCodeType) * nDicCodes, uncmprBuf, rawDataSize);
    Assert(uncmprSize == rawDataSize);
    Assert(memcmp(uncmprBuf, rawData, rawDataSize) == 0);
    pfree(uncmprBuf);
    uncmprBuf = NULL;
    delete dict;
}

#endif

int64 read_data_by_size(_in_ char* inbuf, unsigned int* pos, short size)
{
    int64 ret = 0;

    switch (size) {
        case sizeof(char):
            ret = readData<sizeof(char)>(inbuf, pos);
            break;
        case sizeof(int16):
            ret = readData<sizeof(int16)>(inbuf, pos);
            break;
        case sizeof(int32):
            ret = readData<sizeof(int32)>(inbuf, pos);
            break;
        case sizeof(int64):
            ret = readData<sizeof(int64)>(inbuf, pos);
            break;
        case 3:
            ret = readData<3>(inbuf, pos);
            break;
        case 5:
            ret = readData<5>(inbuf, pos);
            break;
        case 6:
            ret = readData<6>(inbuf, pos);
            break;
        case 7:
            ret = readData<7>(inbuf, pos);
            break;
        default:
            Assert(false);
            break;
    }
    return ret;
}

void write_data_by_size(_in_ char* inbuf, unsigned int* pos, int64 data, const short size)
{
    switch (size) {
        case sizeof(char):
            writeData<sizeof(char)>(inbuf, pos, data);
            break;
        case sizeof(int16):
            writeData<sizeof(int16)>(inbuf, pos, data);
            break;
        case sizeof(int32):
            writeData<sizeof(int32)>(inbuf, pos, data);
            break;
        case sizeof(int64):
            writeData<sizeof(int64)>(inbuf, pos, data);
            break;
        case 3:
            writeData<3>(inbuf, pos, data);
            break;
        case 5:
            writeData<5>(inbuf, pos, data);
            break;
        case 6:
            writeData<6>(inbuf, pos, data);
            break;
        case 7:
            writeData<7>(inbuf, pos, data);
            break;
        default:
            Assert(false);
            break;
    }

    return;
}
