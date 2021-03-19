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
 * compress_kits.h
 *        kits/tools/implement for compression methods
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/compress_kits.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COMPRESS_KITS_H
#define COMPRESS_KITS_H

#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "zlib.h"

enum BufMallocType {
    Unknown,      // buffer is not valid
    FromMemCnxt,  // buffer is from PG memory context
    FromMemProt,  // buffer is so big and from protect manager
};

typedef struct {
    char* buf;
    // remember the current buffer size.
    Size bufSize;
    BufMallocType bufType;
} BufferHelper;

extern void BufferHelperMalloc(BufferHelper* p, Size s);
extern void BufferHelperRemalloc(BufferHelper* p, Size s);
extern void BufferHelperFree(BufferHelper* p);
void write_data_by_size(_in_ char* inbuf, unsigned int* pos, int64 data, const short size);
int64 read_data_by_size(_in_ char* inbuf, unsigned int* pos, short size);

// when decompress some extra bytes are appended to the outbuf.
//
#define ZLIB_EXTRA_SIZE 30

/// Given the min-value and max-value, return how many bytes needed
/// to remember their difference value. it's byte bound.
extern inline short DeltaGetBytesNum(_in_ int64 mindata, _in_ int64 maxdata)
{
    Assert(mindata <= maxdata);
    uint64 diff = maxdata - mindata;

    if ((0 == diff) || (1 == diff))
        return 1;  // byte bound

    const uint64 masks[8] = {0x00000000000000FF,
        0x000000000000FF00,
        0x0000000000FF0000,
        0x00000000FF000000,
        0x000000FF00000000,
        0x0000FF0000000000,
        0x00FF000000000000,
        0xFF00000000000000};
    short i = 7;
    while (0 == (diff & masks[i])) {
        --i;
        Assert(i >= 0);
    }
    return (i + 1);  // byte bound
}

/// judge whether delta can be applied to.
/// if so, store the needed bytes of difference value and return true.
/// otherwise, do nothing and return false.
extern inline bool DeltaCanBeApplied(short* deltaBytes, int64 minVal, int64 maxVal, short valueBytes)
{
    short dltBytes = DeltaGetBytesNum(minVal, maxVal);
    if (dltBytes < valueBytes) {
        *deltaBytes = dltBytes;
        return true;
    }
    return false;
}

/// get memory bound for Delta compression.
extern inline int DeltaGetBound(int nValues, short newSize)
{
    return (newSize * nValues);
}

/// get memory bound for Delta compression.
extern inline int64 RleGetBound1(int inSize, short eachSize)
{
    Assert((eachSize >= (short)sizeof(int8)) && (eachSize <= (short)sizeof(int64)));
    Assert(inSize % eachSize == 0);
    return (1 + inSize + (((uint32)(inSize / eachSize)) >> 1));
}

/// get memory bound for Delta compression.
/// the same to RleGetBound1() but different means for
/// the first input argument.
extern inline int64 RleGetBound2(int nVals, short eachSize)
{
    Assert((eachSize >= (short)sizeof(int8)) && (eachSize <= (short)sizeof(int64)));
    Assert(nVals > 0);
    return (1 + (eachSize * nVals) + ((uint32)nVals >> 1));
}

// RleCoder compress && decompress
//
class RleCoder : public BaseObject {
public:
    /// RLE_v1 constructor.
    RleCoder(short oneValSize)
    {
        m_eachValSize = m_markersize = oneValSize;
        m_minRepeats = RleMinRepeats_v1;
    }
    /// RLE_v2 constructor.
    /// *minRepeats* is from *RleMinRepeats_v2*
    RleCoder(short oneValSize, unsigned int minRepeats)
    {
        m_eachValSize = m_markersize = oneValSize;
        m_minRepeats = minRepeats;
    }
    virtual ~RleCoder()
    {}

    /*
     * Given each value' size and input buffer' size, the upmost memory under the worst condition can be computed.
     * so before RLE compression, get the upmost memory by calling CompressGetBound().
     * And caller should remember the size of raw data, pass that value during RLE decompressing.
     *
     * the common steps of compressing are:
     *     step1: RleCoder rel(each value size);
     *     step2: int64 outBufSize = rel.CompressGetBound(your input buffer size);
     *     step3: char* outBufPrt = (char*) malloc(outBufSize);
     *     step4: int results = rel.Compress(in, outBufPrt, insize, outBufSize);
     *
     * if results=0, that means raw data cannot be RLE compressed, and you should read
     * raw data directly instead of outBufPtr.
     * caller must check results returned, and this rule is also applied to Decompress.
     */
    int Compress(_in_ char* inbuf, __inout char* outbuf, _in_ const int insize, _in_ const int outsize);
    int Decompress(_in_ char* inbuf, __inout char* outbuf, _in_ const int insize, _in_ const int outsize);

    /*
     * after RLE compression values has the following three storage formats:
     *  1) plain values
     *  2) RLE MARKER + REPEATS + SYMBOL   (REPEATS >= RleMinRepeats)
     *  3) RLE MARKER + REPEATS(1B)        (REPEATS <  RleMinRepeats)
     *  the first format doesn't expand extra memory. and the second format will save some memory. so
     *  that only the third format can expand and hold extra space. the worst case is like:
     *     ......
     *     MARKER1 SYMBOL(NON-MARKER)
     *     MARKER1 SYMBOL(NON-MARKER)
     *     ......
     *     ......
     *  so we know the upmost memory is computed by:
     *
     *     insize +  (insize / m_eachValSize) * (1/2) + 1
     */
    FORCE_INLINE
    int64 CompressGetBound(const int insize)
    {
        return RleGetBound1(insize, m_eachValSize);
    }

private:
    template <short eachValSize>
    void WriteNonRuns(char* outbuf, unsigned int* outpos, int64 symbol, unsigned int repeat = 1);

    template <short eachValSize>
    void WriteRuns(char* outbuf, unsigned int* outpos, int64 symbol, unsigned int repeat);

#ifdef USE_ASSERT_CHECKING
    int16 NonRunsNeedSpace(bool equalToMarker, int16 repeats = 1);
    int16 RunsNeedSpace(int16 repeats);
#endif

    template <short eachValSize>
    int InnerCompress(char* inbuf, char* outbuf, const int insize, const int outsize);

    template <short eachValSize>
    int InnerDecompress(char* inbuf, char* outbuf, const int insize, const int outsize);

private:
    /// RLE_v1 members start here.
    short m_eachValSize;
    short m_markersize;
    /// RLE_v2 members start here.
    unsigned int m_minRepeats;

private:
    /// RLE_v1 always uses the constant min-repeats,
    /// which is defined by *RleMinRepeats_v1*.
    static const int64 RleMarker[sizeof(int64) + 1];
    static const unsigned int RleMinRepeats_v1;
    static const unsigned int RleMaxRepeats;

public:
    /// RLE_v2 uses different min-repeats for different
    /// value size. it's just an optimization to
    /// *RleMinRepeats_v1* in RLE_v1.
    /// Don't worry about upgrading. they appear in different cases.
    /// we can get better compression ratio from RLE_v2 than RLE_v1.
    static const unsigned int RleMinRepeats_v2[sizeof(int64) + 1];
};

class DeltaCoder : public BaseObject {
public:
    // for Compress, extra means the max data.
    // for Decompress, extra meas the size of decompressed data, or raw data size.
    //
    DeltaCoder(int64 mindata, int64 extra, bool willCompress);
    virtual ~DeltaCoder()
    {}

    // this method is shared by Compress and Decompress. it should be called ahead so that
    // the memory needed is enough.
    //
    FORCE_INLINE uint64 GetBound(uint64 dataNum)
    {
        Assert(m_outValSize > 0);
        return (m_outValSize * dataNum);
    }

    // for Compress, min/max data are passed by constructor method, and the raw datasize
    // is provided with <inDataSize> of method Compress(). at the most case, compressed
    // datasize is different from raw datasize, which is fetched by calling GetOutValSize()
    //
    int64 Compress(char* inbuf, char* outbuf, int insize, int outsize, short inDataSize);
    FORCE_INLINE short GetOutValSize(void)
    {
        return m_outValSize;
    }

    // for Decompress, mindata && uncompressed datasize are passed by constructor method.
    // this method only just need compressed datasize, that's <inDataSize>.
    //
    int64 Decompress(char* inbuf, char* outbuf, int insize, int outsize, short inDataSize);

private:
    template <bool PlusDelta, short inValSize>
    int DoDeltaOperation(_in_ char* inbuf, _out_ char* outbuf, _in_ int insize, _in_ short outValSize);

    template <bool PlusDelta, short inValSize, short outValSize>
    int doDeltaOperation(_in_ char* inbuf, _out_ char* outbuf, _in_ int insize);

    int64 m_mindata;
#ifdef USE_ASSERT_CHECKING
    int64 m_maxdata;
#endif
    short m_outValSize;
};

typedef uint16 DicCodeType;

/* Dictionary Data In Disk
 *
 * m_totalSize:  including this header and items data
 * m_itemsCount: holds how many dictionary items after this header
 * array of string data(items data) follows this header.
 */
typedef struct DictHeader {
    uint32 m_totalSize;
    uint32 m_itemsCount;
} DictHeader;

/*
 * Dictionary Data In Memory
 *
 * m_itemsMaxSize: the max size holding all items data.
 * m_itemUsedSize: used size by items data, it must be <= m_itemsMaxSize.
 * m_curItemCount: the count of current used slots.
 * m_maxItemCount:
 *     for global dictionary, it is GLOBAL_DIC_SIZE.
 *     for local dictionary, it will be doubled once it is full;
 * m_itemOffset: remembers offsets of each raw value in dict
 * data[1]: includes two parts,
 *     (part 1)
 *     (part 2)  indexes of the string data array, which size is m_maxItemCount*2, and the type is
 *               uint32 DicDataHashSlot[1]. 0xFFFF means available.
 */
typedef struct DictDataInMemory {
    uint32 m_itemsMaxSize;
    uint32 m_itemUsedSize;
    DicCodeType m_maxItemCount;
    DicCodeType m_curItemCount;
    uint32* m_itemOffset;
    DicCodeType* m_slots;
    char* m_data;
    DictHeader* m_header;
} DicData;

// dictionary compress && decompress
//
class DicCoder : public BaseObject {
public:
    virtual ~DicCoder();
    DictHeader* GetHeader(void)
    {
        return m_dictData.m_header;
    }

    // compression methods
    //
    DicCoder(char* inBuf, int inSize, int numVals, int maxItemCnt);
    DicCoder(int dataLen, int maxItemCnt);

    int CompressGetBound(void) const;
    int Compress(_out_ char* outBuf);

    bool EncodeOneValue(_in_ Datum datum, _out_ DicCodeType& result);

    // decompression methods
    //
    DicCoder(char* dictInDisk);
    int Decompress(char* inBuf, int inBufSize, char* outBuf, int outBufSize);
    void DecodeOneValue(_in_ DicCodeType itemIndx, _out_ Datum* result) const;

private:
    int GetHashSlotsCount(void)
    {
        return (m_dictData.m_maxItemCount * 2);
    }
    int ComputeDictMaxSize(int itemsMaxSize, int itemsMaxCount)
    {
        return (sizeof(DictHeader) + itemsMaxSize + sizeof(DicCodeType) * (itemsMaxCount * 2));
    }
    bool IsEnoughToAppend(int appendSize)
    {
        // both space size and item number must be enough to hold extra one
        //
        return (((m_dictData.m_itemUsedSize + appendSize) <= m_dictData.m_itemsMaxSize) &&
                (m_dictData.m_curItemCount < m_dictData.m_maxItemCount));
    }

    void InitDictData(int itemsMaxSize, int itemsMaxCount);

private:
    int m_dataLen;
    char* m_inBuf;
    int m_inSize;
    int m_numVals;
    DicData m_dictData;
};

// LZ4 && LZ4 HC compress and decompress
//
class LZ4Wrapper : public BaseObject {
public:
    /*
     * compression level is [0, 16]
     * the more compression level, the better compression ratio, the lowest decompression;
     * the least compression level, the worst compression ratio, the fastest decompression;
     */
    static const int8 lz4_min_level = 0;
    static const int8 lz4hc_min_level = 1;
    static const int8 lz4hc_recommend_level = 9;
    static const int8 lz4hc_level_step = 2;
    static const int8 lz4hc_max_level = 16;

    typedef struct Lz4Header {
        int rawLen;
        int compressLen;
        char data[FLEXIBLE_ARRAY_MEMBER];
    } Lz4Header;

public:
    LZ4Wrapper() : m_compressionLevel(LZ4Wrapper::lz4hc_min_level)
    {}
    virtual ~LZ4Wrapper()
    {}

    void SetCompressionLevel(int8 level);
    int CompressGetBound(int insize) const;
    int Compress(const char* source, char* dest, int sourceSize) const;

    int DecompressGetBound(const char* source) const;
    int DecompressGetCmprSize(const char* source) const;
    int Decompress(const char* source, char* dest, int sourceSize) const;

private:
#define SizeOfLz4Header offsetof(LZ4Wrapper::Lz4Header, data)

    bool Benefited(const int& outsize, const int& srcsize) const
    {
        return (outsize > 0 && (((int)SizeOfLz4Header + outsize) < srcsize));
    }

private:
    int8 m_compressionLevel;
};

// ZLIB compress && decompress
//
class ZlibEncoder : public BaseObject {
public:
    /* the other compression level macro:
     * Z_NO_COMPRESSION: no compress
     * Z_DEFAULT_COMPRESSION: -1
     * Z_BEST_SPEED:
     */
    static const int8 zlib_recommend_level = 6;
    static const int8 zlib_max_level = Z_BEST_COMPRESSION; /* level 9 */
    static const int8 zlib_level_step = 1;

public:
    virtual ~ZlibEncoder();
    ZlibEncoder();

    /*
     * Solution 1:
     *     step 1: call Reset() to set input-buffer and size;
     *     step 2: char* outbuf[fixedSize]; outsize = fixedSize;
     *     step 2: done = false
     *             while ( !done && Compress(outbuf, outsize, done) >= 0 ) {
     *                 store the compressed outbuf data;
     *             }
     *             handle the exception cases;
     *
     * Solution 2:
     *    step 1: call Reset() to set input-buffer and size;
     *    step 2: outsize = CompressGetBound(); so that the outsize is enough to Compress;
     *            char* outbuf = palloc(outsize);
     *    step 3: Compress(outbuf, outsize, done);  Assert(true == done);
     */
    void Prepare(int level = Z_DEFAULT_COMPRESSION);
    void Reset(unsigned char* inbuf, const int insize);
    int CompressGetBound(int insize);
    int Compress(unsigned char* outbuf, const int outsize, bool& done);

private:
    z_stream m_strm;
    int m_errno;
    int m_flush;
};

class ZlibDecoder : public BaseObject {
public:
    virtual ~ZlibDecoder();
    ZlibDecoder();

    // step 1: call Prepare() first to set input buffer.
    // step 2: call Decompress() again and again, until done is true, or returned value < 0,
    //      that means some errors happen.
    //
    int Prepare(unsigned char* inbuf, const int insize);
    int Decompress(unsigned char* outbuf, const int outsize, bool& done, bool bufIsEnough = false);

private:
    z_stream m_strm;
    int m_errno;
    int m_flush;
};

#endif
