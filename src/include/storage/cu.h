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
 * cu.h
 *    routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cu.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CU_H
#define CU_H

#include "vecexecutor/vectorbatch.h"
#include "cstore.h"
#include "storage/cstore/cstore_mem_alloc.h"
#include "utils/datum.h"
#include "storage/lock/lwlock.h"


#define ATT_IS_CHAR_TYPE(atttypid) (atttypid == BPCHAROID || atttypid == VARCHAROID || atttypid == NVARCHAR2OID)
#define ATT_IS_NUMERIC_TYPE(atttypid) (atttypid == NUMERICOID)

// max uint64 value length 19, set 24 for 8 bit  align
#define MAX_LEN_CHAR_TO_BIGINT_BUF (24)
#define MAX_LEN_CHAR_TO_BIGINT (19)

// CU size always is alligned to ALIGNOF_CU
//

// ADIO requires minimum CU size of 8192 and 512 block alignment
#define ALIGNOF_CUSIZE (8192)
#define ALIGNOF_TIMESERIES_CUSIZE (2)

enum {TS_COLUMN_ID_BASE = 2000};

#define ALLIGN_CUSIZE2(_LEN) TYPEALIGN(2, (_LEN))
#define ALIGNOF_CUSIZE512(_LEN) TYPEALIGN(512, (_LEN))
#define ALLIGN_CUSIZE32(_LEN) TYPEALIGN(32, (_LEN))
#define ALLIGN_CUSIZE(_LEN) TYPEALIGN(ALIGNOF_CUSIZE, (_LEN))

#define ASSERT_CUSIZE(_LEN) \
    Assert((_LEN) == ALLIGN_CUSIZE(_LEN) || (_LEN) == ALLIGN_CUSIZE32(_LEN) \
    || (_LEN) == ALIGNOF_CUSIZE512(_LEN) || (_LEN) == ALLIGN_CUSIZE2(_LEN))

class CUAlignUtils {
public:
    static uint32 AlignCuSize(int len, int align_size);
    static int GetCuAlignSizeColumnId(int columnId);
};

#define PADDING_CU(_PTR, _LEN)             \
    do {                                   \
        char* p = (char*)(_PTR);           \
        int len = (_LEN);                  \
        for (int i = 0; i < len; ++i, ++p) \
            *p = 0;                        \
    } while (0)

#define MIN_MAX_LEN 32

// CU_INFOMASK1 has all the compression mode info.
// CU_INFOMASK2 has the other data attribute info.
//
#define CU_INFOMASK1 0x00FF
#define CU_INFOMASK2 0xFF00

// CU_INFOMASK1 is in file storage/cstore/cstore_compress.h
// CU_INFOMASK2 is working for the following
//
#define CU_DSCALE_NUMERIC 0x0100  // flag for numeric dscale compress
#define CU_HasNULL 0x0400
// indicate that this CRC is just one magic data, and
// CRC computation can be ignored during query.
#define CU_IgnoreCRC 0x0800
// using CRC32C for checksum
#define CU_CRC32C 0x1000
// CU is encrypt
#define CU_ENCRYPT 0x2000

// the CU_mode of CUDesc
//
#define CU_NORMAL 0x01
#define CU_FULL_NULL 0x02
#define CU_SAME_VAL 0x03
#define CU_NO_MINMAX_CU 0x04
#define CU_HAS_NULL 0x08

// The mask of CU_NORMAL, CU_FULL_NULL,
// CU_SAME_VAL, CU_NO_MINMAX_CU
//
#define CU_MODE_LOWMASK 0x0f

// how many bits are set 1 in an unsigned byte
//
const uint8 NumberOfBit1Set[256] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4,
    3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2,
    2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
    3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4,
    4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
    4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5,
    5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

struct CUDesc : public BaseObject {
    TransactionId xmin;
    /*
     * serial-number of CU
     */
    uint32 cu_id;

    // The min value of CU
    // If type is fixed-length, cu_min stores the value.
    // If type is variable-length, cu_min store len and value
    // Format:  len (1byte) value (len <= MIN_MAX_LEN)
    //
    char cu_min[MIN_MAX_LEN];

    // The max value of CU
    // If type is fixed-length, cu_max store the value.
    // If type is variable-length, cu_max store len and value
    // Format:  len (1byte) value (len <= MIN_MAX_LEN)
    //
    char cu_max[MIN_MAX_LEN];

    /*
     * The row number of CU
     */
    int row_count;

    /*
     * The CU data size
     */
    int cu_size;

    /*
     * The CU information mask
     */
    int cu_mode;

    /*
     * The pointer of CU in CU Storage
     */
    CUPointer cu_pointer;

    /*
     * magic number is used to check CU Data valid
     */
    uint32 magic;

public:
    CUDesc();
    ~CUDesc();

    void SetNullCU();
    bool IsNullCU() const;
    void SetNormalCU();
    void SetSameValCU();
    bool IsNormalCU() const;
    bool IsSameValCU() const;
    void SetNoMinMaxCU();
    bool IsNoMinMaxCU() const;
    void SetCUHasNull();
    bool CUHasNull() const;

    void Reset();
    void Destroy()
    {}
};

/* temp info about CU compression
 * because CU data cache exists, we should control used memory and
 * reduce as much as possible. So all temp data during compressing
 * will be placed together.
 */
struct cu_tmp_compress_info {
    /* CU compression options, which type is compression_options */
    void* m_options;

    /* min/max value for integer compression.
     * m_valid_minmax indicates whether the two are valid.
     */
    int64 m_min_value;
    int64 m_max_value;
    bool m_valid_minmax;
};

/* CU struct:
 *                                   before compressing
 *
 * +------------------------+  <--  m_srcBuf               -
 * |                        |                              |
 * |       Header Info      |                         m_srcBufSize
 * |                        |                              |
 * +------------------------+  <--  m_nulls      -         |
 * |                        |                    |         |
 * |       Null Bitmap      |            m_bpNullRawSize   |
 * |                        |                    |         |
 * +------------------------+  <--  m_srcData    -         |
 * |                        |                    |         |
 * |     Compressed Data    |               m_srcDataSize  |
 * |                        |                    |         |
 * +------------------------+                    -         |
 * |      Padding Data      |                              |
 * +------------------------+                              -
 */
class CU : public BaseObject {
public:
    /* Source buffer: nulls bitmap + source data. */
    char* m_srcBuf;

    /* The pointer of Null value in m_srcBuf. */
    unsigned char* m_nulls;

    /* The pointer of source data in m_srcBuf. */
    char* m_srcData;

    /* Compressed buffer: compressed header + compressed data */
    char* m_compressedBuf;

    /* support with accessing datum randomly after loading CU data */
    int32* m_offset;

    /* temp info about CU compression */
    cu_tmp_compress_info* m_tmpinfo;

    /* adio load cu,  compressbuf + padding */
    char* m_compressedLoadBuf;
    int m_head_padding_size;

    /* the number of m_offset items */
    int32 m_offsetSize;

    /* source buffer size. */
    uint32 m_srcBufSize;

    /* source data size */
    uint32 m_srcDataSize;

    /* Compressed buffer size */
    uint32 m_compressedBufSize;

    /* CU size, including padding data */
    uint32 m_cuSize;

    /* compressed CU size, excluding padding data */
    uint32 m_cuSizeExcludePadding;

    /* CRC check code */
    uint32 m_crc;

    /* magic number is used to check CU Data valid */
    uint32 m_magic;

    /* some information for compressing integer type
     * m_eachValSize: the size of each value. -1 or -2 means varlena type.
     * m_typeMode: type mode from attribute' typmode. for numeric it has
     *   precision and scale info.
     */
    int m_eachValSize;
    int m_typeMode;

    /*
     * Nulls Bitmap Size about compressed && uncompressed
     *    m_bpNullCompressedSize is 0 if the CU has no Nulls.
     * otherwise, it's stored in CU header. see FillCompressBufHeader().
     *    m_bpNullRawSize can be computed if row count is given.
     *    it's initialized by InitMem().
     */
    uint16 m_bpNullRawSize;
    uint16 m_bpNullCompressedSize;

    /*
     * Some information.
     * whether has NULL value, compressed mode and so on.
     */
    uint16 m_infoMode;

    /* column type id , used for distinguish char and varchar */
    uint32 m_atttypid;

    bool m_adio_error;       /* error occur in ADIO mode */
    bool m_cache_compressed; /* describe whether CU compressed in CU cache or not,
                              * ADIO load cu into memory which compressed,
                              * when scan use the CU, it should compress first.
                              */
    bool m_inCUCache;        /* whether in CU cache */

    bool m_numericIntLike; /* whether all data in the numeric CU can be transformed to Int64 */

public:
    CU();
    CU(int typeLen, int typeMode, uint32 atttypid);
    ~CU();
    void Destroy();

    /*
     *  Check CRC code
     */
    bool CheckCrc();

    /*
     *	Generate CRC code
     */
    uint32 GenerateCrc(uint16 info_mode) const;

    /*
     *  Append value
     */
    void AppendValue(Datum val, int size);

    /*
     *  Append value
     */
    void AppendValue(const char* val, int size);

    /*
     *  Append Null value
     */
    void AppendNullValue(int row);

    static void AppendCuData(_in_ Datum value, _in_ int repeat, _in_ Form_pg_attribute attr, __inout CU* cu);

    // Compress data
    //
    int16 GetCUHeaderSize(void) const;
    void Compress(int valCount, int16 compress_modes, int align_size);
    void FillCompressBufHeader(void);
    char* CompressNullBitmapIfNeed(_in_ char* buf);
    bool CompressData(_out_ char* outBuf, _in_ int nVals, _in_ int16 compressOption, int align_size);

    // Uncompress data
    //
    char* UnCompressHeader(_in_ uint32 magic, int align_size);
    void UnCompress(_in_ int rowCount, _in_ uint32 magic, int align_size);
    char* UnCompressNullBitmapIfNeed(const char* buf, int rowCount);
    void UnCompressData(_in_ char* buf, _in_ int rowCount);
    template <bool DscaleFlag>
    void UncompressNumeric(char* inBuf, int nNotNulls, int typmode);

    // access datum randomly in CU
    //
    template <bool hasNull>
    void FormValuesOffset(int rows);

    template <int attlen, bool hasNull>
    ScalarValue GetValue(int rowIdx);

    /*
     *  CU to Vector
     */
    template <int attlen, bool hasDeadRow>
    int ToVector(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU, __inout int& curScanPos,
        _out_ int& deadRows, _in_ uint8* cuDelMask);

    /*
     *  CU to Vector
     */
    template <int attlen, bool hasNull, bool hasDeadRow>
    int ToVectorT(_out_ ScalarVector* vec, _in_ int leftRows, _in_ int rowCursorInCU, __inout int& curScanPos,
        _out_ int& deadRows, _in_ uint8* cuDelMask);

    template <int attlen, bool hasNull>
    int ToVectorLateRead(_in_ ScalarVector* tids, _out_ ScalarVector* vec);

    // GET method is used to set the CUDesc info after compressing CU.
    // SET method is used to set the CU info during decompressing CU data.
    //
    int GetCUSize() const;
    void SetCUSize(int cuSize);

    int GetCompressBufSize() const;

    int GetUncompressBufSize() const;

    bool CheckMagic(uint32 magic) const;
    void SetMagic(uint32 magic);
    uint32 GetMagic() const;

    bool IsVerified(uint32 magic);

    /*
     * Is NULL value
     */
    bool IsNull(uint32 row) const;

    /*
     * The number of NULL before rows.
     */
    int CountNullValuesBefore(int rows) const;

    void FreeCompressBuf();
    void FreeSrcBuf();

    void Reset();
    void SetTypeLen(int typeLen);
    void SetTypeMode(int typeMode);
    void SetAttTypeId(uint32 atttypid);
    void SetAttInfo(int typeLen, int typeMode, uint32 atttypid);
    bool HasNullValue() const;

    void InitMem(uint32 initialSize, int rowCount, bool hasNull);
    void ReallocMem(Size size);

    template <bool freeByCUCacheMgr>
    void FreeMem();

    /* timeseries function */
    void copy_nullbuf_to_cu(const char* bitmap, uint16 null_size);
    uint32 init_field_mem(const int reserved_cu_byte);
    uint32 init_time_mem(const int reserved_cu_byte);
    void check_cu_consistence(const CUDesc* cudesc) const;
    int GetCurScanPos(int rowCursorInCU);

private:
    template <bool char_type>
    void DeFormNumberStringCU();

    bool IsNumericDscaleCompress() const;

    // encrypt cu data
    void CUDataEncrypt(char* buf);
    // decrypt cu data
    void CUDataDecrypt(char* buf);
};

template <int attlen, bool hasNull>
ScalarValue CU::GetValue(int rowIdx)
{
    // Notice: this function don't handle the case where it's a
    //    NULL value. caller must be sure this prerequisite.
    Assert(!(HasNullValue() && IsNull(rowIdx)));
    Assert(!hasNull || (hasNull && m_offset && m_offsetSize > 0));

    ScalarValue destVal;
    switch (attlen) {
        case sizeof(uint8): {
            if (!hasNull)
                destVal = *((uint8*)m_srcData + rowIdx);
            else
                destVal = *(uint8*)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case sizeof(uint16): {
            if (!hasNull)
                destVal = *((uint16*)m_srcData + rowIdx);
            else
                destVal = *(uint16*)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case sizeof(uint32): {
            if (!hasNull)
                destVal = *((uint32*)m_srcData + rowIdx);
            else
                destVal = *(uint32*)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case sizeof(uint64): {
            if (!hasNull)
                destVal = *((uint64*)m_srcData + rowIdx);
            else
                destVal = *(uint64*)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case 12: {
            if (!hasNull)
                destVal = (ScalarValue)((uint8*)m_srcData + (12 * rowIdx));
            else
                destVal = (ScalarValue)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case 16: {
            if (!hasNull)
                destVal = (ScalarValue)((uint8*)m_srcData + (16 * rowIdx));
            else
                destVal = (ScalarValue)(m_srcData + m_offset[rowIdx]);
            break;
        }
        case -1:
        case -2:
            destVal = (ScalarValue)((uint8*)m_srcData + m_offset[rowIdx]);
            break;
        default:
            ereport(ERROR, (errmsg("unsupported datatype branch")));
            break;
    }
    return destVal;
}

template <bool freeByCUCacheMgr>
void CU::FreeMem()
{
    if (this->m_srcBuf) {
        if (!freeByCUCacheMgr) {
            CStoreMemAlloc::Pfree(this->m_srcBuf, !this->m_inCUCache);
        } else {
            free(this->m_srcBuf);
        }
        this->m_srcBuf = NULL;
        this->m_srcBufSize = 0;
    }
    if (this->m_compressedLoadBuf) {
        if (!freeByCUCacheMgr) {
            CStoreMemAlloc::Pfree(this->m_compressedLoadBuf, !this->m_inCUCache);
        } else {
            free(this->m_compressedLoadBuf);
        }
        this->m_compressedBuf = NULL;
        this->m_compressedBufSize = 0;
        this->m_compressedLoadBuf = NULL;
        this->m_head_padding_size = 0;
    } else {
        if (this->m_compressedBuf) {
            if (!freeByCUCacheMgr) {
                CStoreMemAlloc::Pfree(this->m_compressedBuf, !this->m_inCUCache);
            } else {
                free(this->m_compressedBuf);
            }
            this->m_compressedBuf = NULL;
            this->m_compressedBufSize = 0;
            this->m_compressedLoadBuf = NULL;
            this->m_head_padding_size = 0;
        }
    }
    if (this->m_offset) {
        if (!freeByCUCacheMgr) {
            CStoreMemAlloc::Pfree(this->m_offset, !this->m_inCUCache);
        } else {
            free(this->m_offset);
        }
        this->m_offset = NULL;
        this->m_offsetSize = 0;
    }
}

#endif
