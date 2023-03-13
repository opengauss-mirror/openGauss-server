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
 * pagecompress.cpp
 *	  page compression manager routines
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/page/pagecompress.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/pagecompress.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/dynahash.h"
#include "access/hash.h"
#include "access/tuptoaster.h"
#include "access/tableam.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

static int int16Compare(Datum arg1, Datum arg2);
static int int32Compare(Datum arg1, Datum arg2);
static int uint32Compare(Datum arg1, Datum arg2);
static int int64Compare(Datum arg1, Datum arg2);

static int64 int16Minus(Datum arg1, Datum arg2);
static int64 int32Minus(Datum arg1, Datum arg2);
static int64 uint32Minus(Datum arg1, Datum arg2);
static int64 int64Minus(Datum arg1, Datum arg2);

static Datum int16Plus(Datum arg1, int64 arg2);
static Datum int32Plus(Datum arg1, int64 arg2);
static Datum uint32Plus(Datum arg1, int64 arg2);
static Datum int64Plus(Datum arg1, int64 arg2);

static const NumberTypeOpt* binarySearch(Oid typeOid);

/* const array for number type. */
static const NumberTypeOpt g_number_types[] = {
    { 20, int64Compare, int64Minus, int64Plus },    /* int8 */
    { 21, int16Compare, int16Minus, int16Plus },    /* int2 */
    { 23, int32Compare, int32Minus, int32Plus },    /* int4 */
    { 26, uint32Compare, uint32Minus, uint32Plus }, /* oid */
    { 1082, int32Compare, int32Minus, int32Plus },  /* date */
    { 1083, int64Compare, int64Minus, int64Plus },  /* time */
    { 1114, int64Compare, int64Minus, int64Plus },  /* timestamp */
    { 1184, int64Compare, int64Minus, int64Plus },  /* timestamptz */
    { 12089, int64Compare, int64Minus, int64Plus }, /* time_stamp */
};
static const int g_number_type_count = sizeof(g_number_types) / sizeof(NumberTypeOpt);

/* Map about max values in byte for delta compression */
static const int64 g_max_value_in_bytes[] = {0x0000000000000000,
                                             0x00000000000000FF,
                                             0x000000000000FFFF,
                                             0x0000000000FFFFFF,
                                             0x00000000FFFFFFFF,
                                             0x000000FFFFFFFFFF,
                                             0x0000FFFFFFFFFFFF,
                                             0x00FFFFFFFFFFFFFF
                                            };
static const int64 g_number_max_value_in_bytes = (int64)(sizeof(g_max_value_in_bytes) / sizeof(int64));

#define getTupleSpace(_tuple) (sizeof(ItemIdData) + MAXALIGN((_tuple)->t_len))

/* How many bits must be used at least for storing one delta-based data */
static uint32 getBitsOfDelta(uint64 delta)
{
    /* guard against too-large input, which would put us into infinite loop */
    if (delta > ULLONG_MAX / 2) {
        return 64;
    }

    uint32 i = 0;
    for (uint64 limit = 1; limit < delta; limit <<= 1) {
        i++;
    }

    return i;
}

/* Given the number of bits, compute the size of bytes. */
#define getSizeOfBits(_bits) (((_bits) + 7) >> 3)

/* MAX_PREFIX_LEN limits the length of prefix string */
#define MAX_PREFIX_LEN 256

/* judge whether a char is digital */
#define isDigital(_ch) (((_ch) >= '0') && ((_ch) <= '9'))

/* judge whether it's a taost tuple */
#define isToastTuple(_tup) (HeapTupleHasExternal(_tup) || (_tup)->t_len > TOAST_TUPLE_THRESHOLD)

/* judge whether this attribute is dropped */
#define isAttrDropped(_attr) (InvalidOid == ((_attr).atttypid))

/* one data repeats at least such times so that it will be added into dict. */
#define DICTITEM_MIN_REPEATS 3
#define DICTITEM_NUM_FACTOR 10

/* Limit for Dictionary Compression.
 * 1. the max number of dict item is <MAX_DICITEMID>, whose id starts from 0;
 * 2. after compress, one attribute is stored by <DICT_ITEMID_SIZE> byte;
 */
#define MAX_DICITEMID 256
#define DICT_ITEMID_SIZE 1
#define DICT_MISS_THRESHOLD 2

static int matchDictItem(const void* left, const void* right, Size keysize)
{
    const DictItemData* leftItem = (DictItemData*)left;
    const DictItemData* rightItem = (DictItemData*)right;
    Assert(leftItem != NULL && rightItem != NULL);

    /* we just care whether the result is 0 or not. */
    if (leftItem->itemSize != rightItem->itemSize) {
        return 1;
    }

    return memcmp(leftItem->itemData, rightItem->itemData, leftItem->itemSize);
}

static uint32 hashDictItem(const void* key, Size keysize)
{
    const DictItemData* item = (const DictItemData*)key;
    return DatumGetUInt32(hash_any((const unsigned char*)item->itemData, item->itemSize));
}

/* create hash table for building dictionary */
static HTAB* hashCreate(const char* name, MemoryContext memCxt, int nelem, int keysize, int entrysize)
{
    HASHCTL hashCtrl;
    errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hash = (HashValueFunc)hashDictItem;
    hashCtrl.match = (HashCompareFunc)matchDictItem;
    hashCtrl.keysize = (Size)keysize;
    hashCtrl.entrysize = entrysize;
    hashCtrl.hcxt = memCxt;

    int flags = (HASH_FUNCTION | HASH_COMPARE | HASH_ELEM | HASH_CONTEXT);

    return hash_create(name, nelem, &hashCtrl, flags);
}

void DictCmprMeta::BuildHashTbl(MemoryContext memCxt)
{
    m_hash = hashCreate("DictInfo Hash", memCxt, dictItemNum + 16, sizeof(DictItemData), sizeof(ItemEntry));

    Assert(dictItemNum <= MAX_DICITEMID);
    for (int i = 0; i < dictItemNum; ++i) {
        ItemEntry* dicItemEntry = (ItemEntry*)hash_search(m_hash, (const void*)(dictItems + i), HASH_ENTER, NULL);
        Assert(dicItemEntry);
        dicItemEntry->id = (unsigned char)i;
    }
}

unsigned char DictCmprMeta::GetDictItemId(DictItemData* key, bool& found)
{
    ItemEntry* dicItemEntry = (ItemEntry*)hash_search(m_hash, (const void*)key, HASH_FIND, &found);
    if (found)
        return dicItemEntry->id;
    return 0;
}

void DictCmprMeta::Destroy()
{
    if (m_hash) {
        hash_destroy(m_hash);
        m_hash = NULL;
    }
}

/* write all the dict metadata into given buffer */
void DictCmprMeta::GetDictItems(char* buf) const
{
    int offset = 0;
    errno_t rc = EOK;
    for (int i = 0; i < dictItemNum; ++i) {
        rc = memcpy_s(buf + offset, (size_t)(BLCKSZ - offset), dictItems[i].itemData, (size_t)(dictItems[i].itemSize));
        securec_check(rc, "", "");
        offset += dictItems[i].itemSize;
    }
}

template <int attlen>
Datum PageCompress::NumstrCompress(Datum src, int& outSize)
{
    uint32 evenLen;
    int destLen = 0;
    uint32 srcLen = 0;
    char* srcPtr = NULL;
    unsigned char* outBuf = NULL;
    unsigned char* destPtr = NULL;

    Assert(attlen == -1 || attlen == -2);
    if (attlen == -1) {
        char* p = DatumGetPointer(src);
        Assert(p != NULL);
        srcPtr = VARDATA_ANY(p);
        srcLen = VARSIZE_ANY_EXHDR(p);
    } else {
        srcPtr = DatumGetPointer(src);
        srcLen = (uint32)strlen(srcPtr);
    }

    /* for empty string, no need to compress */
    if (srcLen == 0) {
        outSize = 0;
        return 0;
    }

    Assert(srcPtr != NULL && srcLen > 0);

    /*
     * If the srcLen is odd, we need set the highest 4 bits for 1 for last byte
     * in order to distinguish the last byte is one original byte
     */
    unsigned char* srcData = (unsigned char*)srcPtr;
    if (srcLen % 2 != 0) {
        if (!isDigital(srcData[srcLen - 1])) {
            outSize = 0;
            return 0;
        }

        evenLen = srcLen - 1;
        destLen = (int)((evenLen >> 1) + 1);
    } else {
        evenLen = srcLen;
        destLen = (int)(evenLen >> 1);
    }
    Assert(destLen >= 0);

    if (attlen == -1) {
        outBuf = (unsigned char*)palloc((Size)(destLen + VARHDRSZ));
        if (destLen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX) {
            outSize = (int)(destLen + VARHDRSZ_SHORT);
            SET_VARSIZE_SHORT(outBuf, outSize);
            destPtr = outBuf + VARHDRSZ_SHORT;
        } else {
            outSize = destLen + VARHDRSZ;
            SET_VARSIZE(outBuf, outSize);
            destPtr = outBuf + VARHDRSZ;
        }
    } else {
        /* Last byte has been set '\0' */
        outSize = destLen + 1;
        destPtr = outBuf = (unsigned char*)palloc0((Size)outSize);
    }

    /* Two bytes combine one byte */
    int pos = 0;
    for (uint32 i = 0; i < evenLen; i = i + 2) {
        if (isDigital(srcData[i]) && isDigital(srcData[i + 1])) {
            destPtr[pos] = (srcData[i] - '0') | (((unsigned int)(srcData[i + 1] - '0')) << 4);
            /*
             * If attlen is -2, we can't use 0 represent '0''0', because '\0' is 0
             * So we guarantee all code > 0
             */
            destPtr[pos] += 1;
            ++pos;
        } else {
            pfree(outBuf);
            outSize = 0;
            return (Datum)0;
        }
    }

    /* Deal with last byte if we need */
    if (srcLen > evenLen) {
        destPtr[pos++] = (srcData[srcLen - 1] - '0') | 0xF0;
    }
    Assert(pos == destLen);

    return PointerGetDatum(outBuf);
}

Datum PageCompress::NumstrUncompress(
    Datum src, int attlen, int& cmprValSize, char* uncmprValBuf, int* dataLenWithPadding, char attalign)
{
    Assert(attlen == -1 || attlen == -2);

    uint32 srcLen = 0;
    char* srcPtr = NULL;
    if (attlen == -1) {
        char* p = DatumGetPointer(src);
        Assert(p != NULL);
        srcPtr = VARDATA_ANY(p);
        srcLen = VARSIZE_ANY_EXHDR(p);
        cmprValSize = (int)(VARSIZE_ANY(src));
    } else {
        srcPtr = DatumGetPointer(src);
        srcLen = (uint32)strlen(srcPtr);
        cmprValSize = (int)(srcLen + 1);
    }
    Assert(srcPtr != NULL && srcLen > 0 && cmprValSize > 0);

    unsigned char* outBuf = NULL;
    unsigned char* destPtr = NULL;
    unsigned char* srcData = (unsigned char*)srcPtr;
    int destLen;
    int padding_size = 0;
    int datum_size = 0;
    unsigned char lastByte = srcData[srcLen - 1];

    if ((lastByte & 0xF0) == 0xF0)
        destLen = (int)(((srcLen - 1) << 1) + 1);
    else
        destLen = (int)(srcLen << 1);
    Assert(destLen > 0);

    if (attlen == -1) {
        /*
         * if the datum header datum_size is 4B,
         * 1. aligned is required.
           2. the first byte of padding must be set 0 if padding exists.
         * NB: nothing to do if the datum header is 1B;
         */
        if (uncmprValBuf) {
            outBuf = (unsigned char*)uncmprValBuf;

            Assert(dataLenWithPadding != NULL);
            *outBuf = 0;
            outBuf = (unsigned char*)att_align_nominal(outBuf, attalign);
            padding_size = int(outBuf - (unsigned char*)uncmprValBuf);
        } else {
            outBuf = (unsigned char*)palloc((Size)(destLen + VARHDRSZ));
        }

        datum_size = destLen + VARHDRSZ;
        SET_VARSIZE(outBuf, datum_size);
        destPtr = outBuf + VARHDRSZ;
    } else {
        /* Last byte has been set '\0' */
        datum_size = destLen + 1;
        if (uncmprValBuf)
            destPtr = outBuf = (unsigned char*)uncmprValBuf;
        else
            destPtr = outBuf = (unsigned char*)palloc((Size)datum_size);

        errno_t rc = memset_s(destPtr, (size_t)datum_size, 0, (size_t)datum_size);
        securec_check(rc, "", "");
    }

    /* One byte split into two bytes */
    int pos = 0;
    for (uint32 i = 0; i < srcLen - 1; ++i) {
        destPtr[pos++] = ((srcData[i] - 1) & 0x0F) + '0';
        destPtr[pos++] = (((srcData[i] - 1) & 0xF0) >> 4) + '0';
    }

    /* Deal with last byte if we need */
    if ((lastByte & 0xF0) == 0xF0)
        destPtr[pos++] = (lastByte & 0x0F) + '0';
    else {
        destPtr[pos++] = ((lastByte - 1) & 0x0F) + '0';
        destPtr[pos++] = (((lastByte - 1) & 0xF0) >> 4) + '0';
    }

    if (dataLenWithPadding) {
        Assert(uncmprValBuf != NULL);
        Assert(padding_size >= 0 && datum_size > 0);
        *dataLenWithPadding = padding_size + datum_size;
    }

    return PointerGetDatum(outBuf);
}

PageCompress::PageCompress(Relation rel, MemoryContext memCtx)
{
    m_rel = rel;
    m_inTups = NULL;
    m_inTupsNum = 0;
    m_current = 0;
    m_outputTupsNum = 0;
    m_mappedTupsNum = 0;
    m_uncmprTupIdx = 0;
    m_cmprTupsNum = 0;
    m_toastTupsNum = 0;
    m_cmprHeaderSize = 0;
    m_outputTups = NULL;
    m_cmprTups = NULL;
    m_toastTups = NULL;
    m_mappedTups = NULL;
    m_nextBatch = false;
    m_last = false;

    /* these member will be resident in parent memory context. */
    MemoryContext oldMemCnxt = MemoryContextSwitchTo(memCtx);

    m_parentMemCxt = memCtx;
    m_cmprHeaderData = (char*)palloc(BLCKSZ);
    m_pageMemCnxt = AllocSetContextCreate(
                        memCtx, "PageCompression", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    int nattrs = RelationGetNumberOfAttributes(m_rel);
    m_cmprMode = (char*)palloc0((Size)(nattrs * sizeof(char)));
    m_cmprMeta = (void**)palloc0((Size)(nattrs * sizeof(void*)));
    m_numTypeOpt = (const NumberTypeOpt**)palloc((Size)(nattrs * sizeof(NumberTypeOpt*)));
    m_dictMiss = (uint32*)palloc0((Size)(nattrs * sizeof(uint32)));
    m_dictItemId = (unsigned char*)palloc((Size)(nattrs * sizeof(unsigned char)));
    m_deformVals = (Datum*)palloc((Size)(nattrs * sizeof(Datum)));
    m_deformNulls = (bool*)palloc((Size)(nattrs * sizeof(bool)));

    (void)MemoryContextSwitchTo(oldMemCnxt);

    FormData_pg_attribute* attrs = m_rel->rd_att->attrs;
    for (int col = 0; col < nattrs; ++col) {
        m_numTypeOpt[col] = binarySearch(attrs[col].atttypid);
    }

#ifdef TRACE_COMPRESS
    nLoops = 0;
#endif
}

PageCompress::~PageCompress()
{
    ResetPerPage();
    pfree_ext(m_cmprHeaderData);
    pfree_ext(m_cmprMode);
    pfree_ext(m_cmprMeta);
    pfree_ext(m_numTypeOpt);
    pfree_ext(m_dictItemId);
    pfree_ext(m_dictMiss);
    pfree_ext(m_deformVals);
    pfree_ext(m_deformNulls);
    pfree_ext(m_cmprTups);
    pfree_ext(m_toastTups);
    pfree_ext(m_mappedTups);
    m_inTups = NULL;
    m_rel = NULL;
    m_parentMemCxt = NULL;

    MemoryContextDelete(m_pageMemCnxt);
    m_pageMemCnxt = NULL;
}

ColArray* PageCompress::DeformTups(HeapTuple* tups, int nTups)
{
    int attrNum = RelationGetNumberOfAttributes(m_rel);
    TupleDesc desc = RelationGetDescr(m_rel);

    ColArray* colArray = (ColArray*)palloc((Size)(sizeof(ColArray) * attrNum));

    for (int i = 0; i < attrNum; ++i) {
        colArray[i].nulls = (bool*)palloc0((Size)(sizeof(bool) * nTups));
        colArray[i].val = (Datum*)palloc0((Size)(sizeof(Datum) * nTups));
        colArray[i].num = nTups;
    }

    /* storage format is as followings:
     *
     *   {col_1, row_1} {col_1, row_2} {col_1, row_3} {col_1, row_4} ...
     *   {col_2, row_1} {col_2, row_2} {col_2, row_3} {col_2, row_4} ...
     *   {col_3, row_1} {col_3, row_2} {col_3, row_3} {col_3, row_4} ...
     *   ....
     *   ....
     */
    for (int row = 0; row < nTups; ++row) {
        heap_deform_tuple(tups[row], desc, m_deformVals, m_deformNulls);

        for (int col = 0; col < attrNum; ++col) {
            colArray[col].val[row] = m_deformVals[col];
            colArray[col].nulls[row] = m_deformNulls[col];
        }
    }

    return colArray;
}

/*
 * ---------------------------------------------------------------
 * 	          Delta Compression/Decompression Methods
 * ---------------------------------------------------------------
 */
DeltaCmprMeta* PageCompress::DeltaGetCmprMeta(ColArray* colArray, const NumberTypeOpt* opt) const
{
    Datum minVal, maxVal;
    Datum* values = colArray->val;
    bool* isnulls = colArray->nulls;

    /* init min/max value. */
    int i = 0;
    while (i < colArray->num && isnulls[i])
        ++i;
    /* if all the values are NULLs, return NULL */
    if (i == colArray->num)
        return NULL;

    /* loop and get the min/max value in colArray. */
    maxVal = minVal = values[i];
    for (i++; i < colArray->num; i++) {
        if (isnulls[i])
            continue;

        if (opt->compare(minVal, values[i]) > 0)
            minVal = values[i];
        else if (opt->compare(maxVal, values[i]) < 0)
            maxVal = values[i];
    }

    int64 diff = opt->minus(maxVal, minVal);
    int bytes = (int)getSizeOfBits(getBitsOfDelta((uint64)diff));
    if (bytes >= g_number_max_value_in_bytes) {
        return NULL;
    }

    DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)palloc(sizeof(DeltaCmprMeta));
    deltaInfo->MinVal = minVal;
    deltaInfo->MaxVal = maxVal;
    deltaInfo->bytes = bytes;

    return deltaInfo;
}

/*
 * Given both delta meta and a value, compute and return its delta data.
 * returning true means that delta compress successes, and deltaBuf is valid whose size would be
 * computed by metaInfo->bytes.
 * metaInfo, opt and val are the input parameters; and deltaBuf is the output
 * returning false means that delta compress fails.
 */
bool PageCompress::DeltaCompress(
    DeltaCmprMeta* metaInfo, const NumberTypeOpt* opt, Datum val, unsigned char* deltaBuf) const
{
    int64 diff = opt->minus(val, metaInfo->MinVal);

    /*
     * if this diff is negative, don't compress;
     * if this diff is out of range, don't compress;
     */
    Assert(metaInfo->bytes < g_number_max_value_in_bytes);
    if ((diff < 0) || (diff > g_max_value_in_bytes[metaInfo->bytes])) {
        return false;
    }

    /*
     * until here, we know that:
     *   1. diff is greater than 0;
     *   2. the highest byte is 0x00, yes, at most 7 bytes is used and valid.
     *   3. diff can be changed to uint64 safely.
     */
    Assert((diff >= 0) && ((diff >> 56) == 0x00));

    uint64 udiff = (uint64)diff;
    for (int i = metaInfo->bytes - 1; i >= 0; i--) {
        deltaBuf[i] = (udiff & 0xFF);
        udiff = udiff >> 8;
    }

    return true;
}

/* [IN]: metaInfo && opt && deltaBuf[]
 * delta decompress.
 */
Datum PageCompress::DeltaUncompress(DeltaCmprMeta* metaInfo, const NumberTypeOpt* opt, unsigned char* deltaBuf)
{
    uint64 diff = 0;
    unsigned char* delta = (unsigned char*)deltaBuf;

    diff |= delta[0];
    for (int i = 1; i < metaInfo->bytes; i++) {
        diff = (diff << 8) | delta[i];
    }

    Assert((diff >> 56) == 0x00);
    return opt->plus(metaInfo->MinVal, (int64)diff);
}

Datum PageCompress::DictUncompress(DictCmprMeta* metaInfo, const char* compressBuf, int attlen)
{
    Datum res = 0;
    unsigned char id = *((unsigned char*)compressBuf);
    DictItemData* val = metaInfo->dictItems + id;

    /* refer to fetch_att() */
    if (attlen > 0) {
        switch (attlen) {
            case sizeof(char): {
                Assert(0);
                break;
            }
            case sizeof(int16): {
                res = Int16GetDatum(*(int16*)val->itemData);
                break;
            }
            case sizeof(int32): {
                res = Int32GetDatum(*(int32*)val->itemData);
                break;
            }
            case sizeof(Datum): {
                res = *(Datum*)val->itemData;
                break;
            }
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported byval length: %d", (int)(attlen))));
        }
    } else
        res = PointerGetDatum(val->itemData);
    return res;
}

/*
 * ---------------------------------------------------------------
 * 	          Prefix Compression/Decompression Methods
 * ---------------------------------------------------------------
 */
template <int attlen>
int PageCompress::PrefixLen(Datum str1, Datum str2)
{
    char* strData1 = NULL;
    char* strData2 = NULL;
    int maxPrefixLen = 0;
    int prefixLen = 0;

    /* Note that numeric column should not do this. */
    if (attlen == -1) {
        char* ptr1 = DatumGetPointer(str1);
        char* ptr2 = DatumGetPointer(str2);
        strData1 = VARDATA_ANY(ptr1);
        strData2 = VARDATA_ANY(ptr2);
        maxPrefixLen = Min(VARSIZE_ANY_EXHDR(ptr1), VARSIZE_ANY_EXHDR(ptr2));
    } else if (attlen == -2) {
        strData1 = DatumGetCString(str1);
        strData2 = DatumGetCString(str2);
        size_t str_len1 = strlen(strData1);
        size_t str_len2 = strlen(strData2);
        maxPrefixLen = (int)Min(str_len1, str_len2);
    }

    while (prefixLen < maxPrefixLen && strData1[prefixLen] == strData2[prefixLen])
        ++prefixLen;

    return prefixLen;
}

/*
 * Get common prefix from colArray
 * This function outputs prefixLen and prefix string
 */
template <int attlen>
char* PageCompress::PrefixGetCmprMeta(ColArray* colArray, int& prefixLen)
{
    int prev = 0;
    int next = 0;
    int arrayLen = colArray->num;
    char* prefixStr = NULL;

    /* Initialize the prefixLen by comparing the first and second tuple */
    while (prev < arrayLen && colArray->nulls[prev])
        ++prev;
    next = prev + 1;

    while (next < arrayLen && colArray->nulls[next])
        ++next;

    if (next < arrayLen) {
        prefixLen = PrefixLen<attlen>(colArray->val[prev], colArray->val[next]);
        if (prefixLen == 0) {
            /*
             * if the first two have no common prefix, we don't continue to advance.
             * maybe this trick is unreasonalbe.
             */
            return NULL;
        }

        Assert(prev < arrayLen);
        if (attlen == -1)
            prefixStr = VARDATA_ANY(DatumGetPointer(colArray->val[prev]));
        else
            prefixStr = DatumGetCString(colArray->val[prev]);

        /* Loop for geting the final prefixLen */
        prev = next;
        for (++next; next < arrayLen; ++next) {
            while (next < arrayLen && colArray->nulls[next]) {
                ++next;
            }
            if (next == arrayLen) {
                break;
            }

            int len = PrefixLen<attlen>(colArray->val[prev], colArray->val[next]);
            if (prefixLen > len && len > 0) {
                prefixLen = len;
            }
            prev = next;
        }
    }
    return prefixStr;
}

template <int attlen>
bool PageCompress::PrefixCompress(PrefixCmprMeta* prefix, Datum val, Datum& cmprsVal, int& cmprsValSize)
{
    char* attStart = NULL;
    int attSize;

    if (-1 == attlen) {
        char* ptr = DatumGetPointer(val);
        Assert(ptr != NULL);
        attStart = VARDATA_ANY(ptr);
        attSize = VARSIZE_ANY_EXHDR(ptr);
    } else {
        attStart = DatumGetCString(val);
        attSize = (int)(strlen(attStart) + 1);
    }
    Assert(attStart != NULL && attSize >= 0);
    Assert(prefix != NULL && prefix->len > 0 && prefix->prefixStr != NULL);

    if (prefix->len > attSize || memcmp(prefix->prefixStr, attStart, (size_t)prefix->len)) {
        return false;
    }

    if (-1 == attlen) {
        int suffixLen = attSize - prefix->len;
        Assert(suffixLen >= 0);
        char* suffix = (char*)palloc((Size)(suffixLen + VARHDRSZ));
        errno_t errorno = EOK;

        /* use one byte var-header as possible as much */
        if (suffixLen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX) {
            SET_VARSIZE_SHORT(suffix, suffixLen + VARHDRSZ_SHORT);
            if (suffixLen > 0) {
                errorno = memcpy_s((suffix + VARHDRSZ_SHORT), (size_t)suffixLen, (attStart + prefix->len), (size_t)suffixLen);
                securec_check(errorno, "\0", "\0");
            }
            cmprsValSize = (int)(suffixLen + VARHDRSZ_SHORT);
        } else {
            SET_VARSIZE(suffix, suffixLen + VARHDRSZ);
            errorno = memcpy_s((suffix + VARHDRSZ), (size_t)suffixLen, (attStart + prefix->len), (size_t)suffixLen);
            securec_check(errorno, "\0", "\0");
            cmprsValSize = suffixLen + VARHDRSZ;
        }
        cmprsVal = PointerGetDatum(suffix);
    } else {
        cmprsVal = CStringGetDatum(attStart + prefix->len);
        cmprsValSize = (int)(strlen(attStart + prefix->len) + 1);
    }
    Assert(cmprsValSize > 0);

    return true;
}

Datum PageCompress::PrefixUncompress(int attlen, PrefixCmprMeta* prefix, char* start, int* attrCmprsValSize,
                                     char* uncmprValBuf, int* dataLenWithPadding, char attalign)
{
    int attsize;
    char* attdata = NULL;
    char* attr = NULL;
    int datum_size;
    int padding_size = 0;
    errno_t rc = EOK;

    Assert(-1 == attlen || -2 == attlen);
    Assert(prefix != NULL && prefix->len > 0 && prefix->prefixStr != NULL);
    Assert(start != NULL && attrCmprsValSize != NULL);

    if (-1 == attlen) {
        char* ptr = DatumGetPointer(start);
        attdata = VARDATA_ANY(ptr);
        attsize = VARSIZE_ANY_EXHDR(ptr);
        *attrCmprsValSize = VARSIZE_ANY(ptr);
        Assert(attdata != NULL && attsize >= 0 && *attrCmprsValSize > 0);

        int attRawLen = prefix->len + attsize;

        /*
         * if the datum header datum_size is 4B,
         * 1. aligned is required.
         * 2. the first byte of padding must be set 0 if padding exists.
         * NB: nothing to do if the datum header is 1B;
         */
        if (uncmprValBuf) {
            attr = uncmprValBuf;

            Assert(dataLenWithPadding != NULL);
            *attr = 0;
            attr = (char*)att_align_nominal(attr, attalign);
            padding_size = (int)(attr - uncmprValBuf);
        } else {
            attr = (char*)palloc((Size)(attRawLen + VARHDRSZ));
        }

        datum_size = attRawLen + VARHDRSZ;
        SET_VARSIZE(attr, datum_size);
        int offset = VARHDRSZ;
        rc = memcpy_s((attr + offset), (size_t)(datum_size - offset), prefix->prefixStr, (size_t)prefix->len);
        securec_check(rc, "", "");
        offset += prefix->len;
        if (attsize > 0) {
            rc = memcpy_s((attr + offset), (size_t)(datum_size - offset), attdata, (size_t)attsize);
            securec_check(rc, "", "");
        }
    } else {
        /* compressed attribute's info */
        attdata = DatumGetCString(start);
        attsize = (int)strlen(attdata);
        *attrCmprsValSize = attsize + 1;
        Assert(attdata != NULL && attsize >= 0 && *attrCmprsValSize > 0);

        datum_size = prefix->len + attsize + 1;
        if (uncmprValBuf)
            attr = uncmprValBuf;
        else
            attr = (char*)palloc((Size)datum_size);
        rc = memcpy_s(attr, (size_t)datum_size, prefix->prefixStr, (size_t)prefix->len);
        securec_check(rc, "", "");
        if (attsize > 0) {
            rc = memcpy_s((attr + prefix->len), datum_size - prefix->len, attdata, attsize);
            securec_check(rc, "", "");
        }
        attr[datum_size - 1] = '\0';
    }

    if (dataLenWithPadding) {
        Assert(uncmprValBuf != NULL);
        Assert(padding_size >= 0 && datum_size > 0);
        *dataLenWithPadding = padding_size + datum_size;
    }

    return PointerGetDatum(attr);
}

/* choose compression method between delta and dict.
 * we prefer delta method to dict method.
 */
void PageCompress::ChooseMethodForFixedLenType(ColArray* colArray, int col)
{
    int hintMode = m_rel->rd_att->attrs[col].attcmprmode;

    /* first, try to use the user's hint */
    if (SetCmprMethod(colArray, col, hintMode))
        return;

    /* then, try to check whether delta value encoding is proper */
    if (CMPR_DELTA != hintMode && SetCmprMethod(colArray, col, CMPR_DELTA))
        return;

    /* then, try to check dictionary compress method */
    if (CMPR_DICT != hintMode && SetCmprMethod(colArray, col, CMPR_DICT))
        return;

    (void)SetCmprMethod(colArray, col, CMPR_NONE);
}

template <int attFlag>
void PageCompress::FillHashKey(DictItemData& key, const int& attlen, const Datum& attval)
{
    if (attFlag > 0) {
        key.itemSize = (int16)attlen;
        key.itemData = (char*)palloc((Size)attlen);
        store_att_byval(key.itemData, attval, attlen);
    } else if (-1 == attFlag) {
        key.itemData = DatumGetPointer(attval);
        key.itemSize = (int16)VARSIZE_ANY(key.itemData);
    } else if (-2 == attFlag) {
        key.itemData = DatumGetPointer(attval);
        key.itemSize = (int16)(strlen(key.itemData) + 1);
    }
}

/* Choose dictinary items and put it into DictCmprMeta */
template <int attFlag>
DictCmprMeta* PageCompress::DictGetCmprMeta(ColArray* colArray, int col, int attlen)
{
    int i;

    /*
     * If the times of failing to choose dictionary compression exceeds
     * the threshold, we shouldn't waste CPU to try again.
     */
    if (m_dictMiss[col] > DICT_MISS_THRESHOLD)
        return NULL;

    DictCmprMeta* dicInfo = (DictCmprMeta*)palloc0(sizeof(DictCmprMeta));
    int pos = 0;
    bool found = false;
    HTAB* dictHash =
        hashCreate("DictHash", m_pageMemCnxt, colArray->num + 16, sizeof(DictItemData), sizeof(DictItemEntry));

    for (i = 0; i < colArray->num; ++i) {
        if (colArray->nulls[i]) {
            continue;
        }

        DictItemData key;
        if (attFlag > 0) {
            FillHashKey<1>(key, attlen, colArray->val[i]);
        } else if (attFlag == -1) {
            FillHashKey<-1>(key, attlen, colArray->val[i]);
        } else if (attFlag == -2) {
            FillHashKey<-2>(key, attlen, colArray->val[i]);
        }

        DictItemEntry* dicItemEntry = (DictItemEntry*)hash_search(dictHash, (const void*)&key, HASH_ENTER, &found);
        Assert(dicItemEntry != NULL);
        if (!found) {
            dicItemEntry->hitCount = 1;
            dicItemEntry->beChoose = false;
            continue;
        }
        ++dicItemEntry->hitCount;

        /*
         * If the repeated times exceeds DICTITEM_NUM_FACTOR, then we should
         * choose it as dictionary item
         * Description: use a priority queue to keep Top N dictionary item by hitCount
         */
        if (!dicItemEntry->beChoose &&
            (dicItemEntry->hitCount >= Max(colArray->num / DICTITEM_NUM_FACTOR, DICTITEM_MIN_REPEATS))) {
            dicItemEntry->beChoose = true;

            dicInfo->dictItems[pos++] = key;
            dicInfo->dictItemNum = pos;
            dicInfo->totalBytes += key.itemSize;
            Assert(dicInfo->dictItemNum <= (colArray->num >> 1));

            if (dicInfo->dictItemNum >= MAX_DICITEMID)
                break;
        }
    }

    hash_destroy(dictHash);
    if (pos > 0)
        return dicInfo;

    ++m_dictMiss[col];
    pfree(dicInfo);
    return NULL;
}

template <int attlen>
bool PageCompress::NumstrEvaluate(ColArray* colArray)
{
    int n = colArray->num;
    int numStr = 0;
    bool flag = true;
    Datum* val = colArray->val;
    bool* nulls = colArray->nulls;

    for (int i = 0; i < n; ++i) {
        if (nulls[i])
            continue;

        int len = 0;
        char* dataPtr = NULL;

        if (attlen == -1) {
            char* p = DatumGetPointer(val[i]);
            dataPtr = VARDATA_ANY(p);
            len = VARSIZE_ANY_EXHDR(p);
        } else if (attlen == -2) {
            dataPtr = DatumGetPointer(val[i]);
            len = (int)strlen(dataPtr);
        }

        /* do not count empty string */
        if (len == 0) {
            continue;
        }

        for (int j = 0; j < len; ++j) {
            if (!isDigital(dataPtr[j])) {
                flag = false;
                break;
            }
        }

        if (flag == false)
            break;

        /*
         * a little trick: we needn't check all the values, and if the
         * the first some values are suitable for compression, retrun
         * and try to compress them later.
         */
        if (++numStr > 2) {
            flag = true;
            break;
        }
    }

    /* no need to compress if all sample values are null or empty string */
    return (0 == numStr) ? false : flag;
}

/* choose compression method between prefix and dict.
 * we prefer prefix method to dict method.
 */
void PageCompress::ChooseMethodForVaryLenType(ColArray* colArray, int col)
{
    int hintMode = m_rel->rd_att->attrs[col].attcmprmode;

    /* first, try to use the user's hint */
    if (SetCmprMethod(colArray, col, hintMode))
        return;

    /* then, try to check dictionary compress method */
    if (CMPR_DICT != hintMode && SetCmprMethod(colArray, col, CMPR_DICT))
        return;

    /*
     * Try to get prefix.
     * Note that numeric column should not do this
     */
    if (CMPR_PREFIX != hintMode && SetCmprMethod(colArray, col, CMPR_PREFIX))
        return;

    /* then, try to number-string compress method */
    if (CMPR_NUMSTR != hintMode && SetCmprMethod(colArray, col, CMPR_NUMSTR))
        return;

    (void)SetCmprMethod(colArray, col, CMPR_NONE);
}

/*
 * if user has specify the compress method by CREATE TABLE sql clause,
 * we take it as a hint, and first try to get compression metadata using it.
 * but maybe it fails, and we have to try the other methods then.
 */
bool PageCompress::SetCmprMethod(ColArray* colArray, int col, int mode)
{
    FormData_pg_attribute* attrs = m_rel->rd_att->attrs;
    int attlen = attrs[col].attlen;

    switch (mode) {
        case CMPR_DELTA: {
            Assert(m_numTypeOpt[col] != NULL && attlen > 1);
            DeltaCmprMeta* deltaInfo = DeltaGetCmprMeta(colArray, m_numTypeOpt[col]);

            if (deltaInfo == NULL)
                return false;

            /*
             * this if condition tells us that it's always true that
             *		 deltaInfo->bytes < 8
             */
            Assert(deltaInfo->bytes <= g_number_max_value_in_bytes);
            if (deltaInfo->bytes >= attlen) {
                pfree(deltaInfo);
                return false;
            }

            m_cmprMode[col] = CMPR_DELTA;
            m_cmprMeta[col] = deltaInfo;
            return true;
        }

        case CMPR_DICT: {
            DictCmprMeta* dicInfo = NULL;
            if (-2 == attlen)
                dicInfo = DictGetCmprMeta<-2>(colArray, col, attlen);
            else if (-1 == attlen)
                dicInfo = DictGetCmprMeta<-1>(colArray, col, attlen);
            else
                dicInfo = DictGetCmprMeta<1>(colArray, col, attlen);

            if (dicInfo == NULL)
                return false;

            dicInfo->BuildHashTbl(m_pageMemCnxt);
            m_cmprMode[col] = CMPR_DICT;
            m_cmprMeta[col] = dicInfo;
            return true;
        }

        case CMPR_PREFIX: {
            int prefixLen = 0;
            char* prefixStr = NULL;

            Assert(-1 == attlen || -2 == attlen);
            if (-2 == attlen)
                prefixStr = PrefixGetCmprMeta<-2>(colArray, prefixLen);
            else if (-1 == attlen)
                prefixStr = PrefixGetCmprMeta<-1>(colArray, prefixLen);

            /* If this array has common prefix, we fill prefix compression meta information */
            if (prefixLen > 0 && prefixLen < MAX_PREFIX_LEN) {
                PrefixCmprMeta* prefixInfo = (PrefixCmprMeta*)palloc(sizeof(PrefixCmprMeta));
                prefixInfo->len = prefixLen;
                prefixInfo->prefixStr = (char*)palloc((Size)prefixLen);
                errno_t rc = memcpy_s(prefixInfo->prefixStr, (size_t)prefixLen, prefixStr, (size_t)prefixLen);
                securec_check(rc, "", "");

                m_cmprMeta[col] = prefixInfo;
                m_cmprMode[col] = CMPR_PREFIX;
                return true;
            }
            return false;
        }

        case CMPR_NUMSTR: {
            Assert(-1 == attlen || -2 == attlen);
            bool NumerStringFlag = (-1 == attlen) ? NumstrEvaluate<-1>(colArray) : NumstrEvaluate<-2>(colArray);
            if (NumerStringFlag) {
                m_cmprMode[col] = CMPR_NUMSTR;
                m_cmprMeta[col] = NULL;
                return true;
            }
            return false;
        }

        case CMPR_UNDEF:
            return false;

        case CMPR_NONE:
        default:
            m_cmprMode[col] = CMPR_NONE;
            m_cmprMeta[col] = NULL;
            return true;
    }
}

void PageCompress::ChooseCmprMethod(ColArray* colArrays)
{
    int attrNum = RelationGetNumberOfAttributes(m_rel);
    FormData_pg_attribute* attrs = m_rel->rd_att->attrs;

    for (int col = 0; col < attrNum; ++col) {
        /* continue if this attribute shouldn't be compressed specified by user; */
        if (CMPR_NONE == attrs[col].attcmprmode)
            continue;

        /* continue if this attribute is dropped by user */
        if (isAttrDropped(attrs[col])) {
            (void)SetCmprMethod(colArrays + col, col, CMPR_NONE);
            continue;
        }
        if (m_numTypeOpt[col] != NULL) {
            Assert(attrs[col].attlen > 1);
            ChooseMethodForFixedLenType(colArrays + col, col);
        } else if (attrs[col].attlen > 0) {
            /* These types are not compressed. */
            (void)SetCmprMethod(colArrays + col, col, CMPR_NONE);
        } else {
            Assert(attrs[col].attlen == -1 || attrs[col].attlen == -2);
            ChooseMethodForVaryLenType(colArrays + col, col);
        }
    }
}

void PageCompress::SetCmprHeaderData(void)
{
    FormData_pg_attribute* atts = RelationGetDescr(m_rel)->attrs;
    int nattrs = RelationGetNumberOfAttributes(m_rel);
    int offset = 0;
    int rawCols = 0;
    errno_t rc = EOK;

    for (int col = 0; col < nattrs; ++col) {
        m_cmprHeaderData[offset++] = m_cmprMode[col]; /* compression mode */

        switch (m_cmprMode[col]) {
            case CMPR_DELTA: {
                DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)m_cmprMeta[col];
                int attrLen = atts[col].attlen;
                Assert(deltaInfo->bytes < g_number_max_value_in_bytes);
                Assert(attrLen > 0);

                /* how many bytes is used for delta value */
                m_cmprHeaderData[offset++] = (char)deltaInfo->bytes;
                /* the min/base value */
                store_att_byval(m_cmprHeaderData + offset, deltaInfo->MinVal, attrLen);
                offset += attrLen;
                break;
            }

            case CMPR_DICT: {
                DictCmprMeta* dicInfo = (DictCmprMeta*)m_cmprMeta[col];
                Assert(dicInfo->dictItemNum < 0xFF);
                /* how many dictionary items */
                m_cmprHeaderData[offset++] = (char)dicInfo->dictItemNum;

                dicInfo->GetDictItems(m_cmprHeaderData + offset);
                offset += dicInfo->totalBytes;
                break;
            }

            case CMPR_PREFIX: {
                Assert(-2 == atts[col].attlen || -1 == atts[col].attlen);
                PrefixCmprMeta* prefixInfo = (PrefixCmprMeta*)m_cmprMeta[col];
                Assert(prefixInfo->len > 0 && prefixInfo->len < MAX_PREFIX_LEN);
                /* how many bytes is used for prefix string */
                m_cmprHeaderData[offset++] = (char)prefixInfo->len;
                /* data of prefix string */
                rc = memcpy_s(m_cmprHeaderData + offset, (size_t)(BLCKSZ - offset), prefixInfo->prefixStr, (size_t)(prefixInfo->len));
                securec_check(rc, "", "");
                offset += prefixInfo->len;
                break;
            }

            case CMPR_NUMSTR:
                break;

            case CMPR_NONE:
                ++rawCols;
                break;
            default:
                break;
        }
    }

    /*
     * the same to special space in page, start address of compression header
     * is aligned using MAXALIGN().
     */
    m_cmprHeaderSize = (nattrs == rawCols) ? 0 : (int)MAXALIGN((uint32)offset);
    Assert(m_cmprHeaderSize < BLCKSZ);
}

/* fetch and parse the compression meta data, then return meta infomation object.
 * metaStart [IN]: start address of compression metadata within page for each attribute;
 * attrRawLen [IN]: fixed length of this attribute, it's from pg_attr->attlen;
 * metaSize [OUT]: size of compression metadata within page for each attribute;
 * mode [OUT]: compression method;
 */
void* PageCompress::FetchAttrCmprMeta(char* metaStart, const int attrRawLen, int* metaSize, char* mode)
{
    int offset = 1;
    *mode = metaStart[0];
    void* retInfo = NULL;

    switch (*mode) {
        case CMPR_DELTA: {
            DeltaCmprMeta* deltaInfo = &t_thrd.storage_cxt.cmprMetaInfo->deltaInfo;

            /* how many bytes is used for delta value */
            deltaInfo->bytes = metaStart[offset];
            ++offset;

            Assert(attrRawLen > 0);
            /* read and fetch the min/base value */
            deltaInfo->MinVal = fetch_att(metaStart + offset, true, attrRawLen);
            offset += attrRawLen;
            *metaSize = offset;
            retInfo = (void*)deltaInfo;
            break;
        }
        case CMPR_DICT: {
            DictCmprMeta* dicInfo = &t_thrd.storage_cxt.cmprMetaInfo->dicInfo;
            dicInfo->dictItemNum = metaStart[offset++];

            if (attrRawLen > 0) {
                for (int i = 0; i < dicInfo->dictItemNum; ++i) {
                    dicInfo->dictItems[i].itemData = metaStart + offset;
                    dicInfo->dictItems[i].itemSize = (int16)attrRawLen;
                    offset += attrRawLen;
                }
            } else if (attrRawLen == -1) {
                for (int i = 0; i < dicInfo->dictItemNum; ++i) {
                    dicInfo->dictItems[i].itemData = metaStart + offset;
                    dicInfo->dictItems[i].itemSize = (int16)VARSIZE_ANY(dicInfo->dictItems[i].itemData);
                    offset += dicInfo->dictItems[i].itemSize;
                }
            } else if (attrRawLen == -2) {
                for (int i = 0; i < dicInfo->dictItemNum; ++i) {
                    dicInfo->dictItems[i].itemData = metaStart + offset;
                    dicInfo->dictItems[i].itemSize = (int16)strlen(dicInfo->dictItems[i].itemData) + 1;
                    offset += dicInfo->dictItems[i].itemSize;
                }
            }
            *metaSize = offset;
            retInfo = (void*)dicInfo;
            break;
        }

        case CMPR_PREFIX: {
            Assert(-2 == attrRawLen || -1 == attrRawLen);
            PrefixCmprMeta* prefixInfo = &t_thrd.storage_cxt.cmprMetaInfo->prefixInfo;
            prefixInfo->len = (metaStart[offset] & 0x00FF);
            ++offset;
            prefixInfo->prefixStr = metaStart + offset;
            offset += prefixInfo->len;
            *metaSize = offset;
            retInfo = (void*)prefixInfo;
            break;
        }

        case CMPR_NUMSTR:
        case CMPR_NONE: {
            *metaSize = offset;
            retInfo = (void*)NULL;
            break;
        }

        default: {
            Assert(false);
            retInfo = (void*)NULL;
            break;
        }
    }

    return retInfo;
}

/* according to compression mode and meta information, return the value size
 * of this attribute in the tuple.
 */
int PageCompress::GetAttrCmprValSize(char mode, int attlen, void* metaInfo, const char* attrStart)
{
    switch (mode) {
        case CMPR_DELTA: {
            DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)metaInfo;
            return deltaInfo->bytes;
        }

        case CMPR_DICT: {
            DictCmprMeta* dicInfo = (DictCmprMeta*)metaInfo;
            return (dicInfo->dictItemNum == 1) ? 0 : DICT_ITEMID_SIZE;
        }

        case CMPR_PREFIX:
        case CMPR_NUMSTR: {
            return (attlen == -1) ? VARSIZE_ANY(attrStart) : (strlen(attrStart) + 1);
        }

        case CMPR_NONE: {
            Assert(false);
            break;
        }
        default:
            break;
    }

    Assert(false);
    return 0;
}

/*
 * external buffer must be offered for some compress method, in order to
 * reduce the times of multi memcpy() calls.
 */
bool PageCompress::NeedExternalBuf(char mode)
{
    return ((mode == CMPR_PREFIX) || (mode == CMPR_NUMSTR));
}

int PageCompress::UncompressOneAttr(
    char mode, char attalign, int attlen, void* metaInfo, char* attrStart, int* attrCmprsValSize, char* uncmprValBuf)
{
    int dataLenWithPadding = 0;

    Assert((attlen == -1) || (attlen == -2 && attalign == 'c'));
    Assert(NeedExternalBuf(mode));

    switch (mode) {
        case CMPR_PREFIX: {
            PrefixCmprMeta* prefixInfo = (PrefixCmprMeta*)metaInfo;
            (void)PrefixUncompress(
                attlen, prefixInfo, attrStart, attrCmprsValSize, uncmprValBuf, &dataLenWithPadding, attalign);
            break;
        }

        case CMPR_NUMSTR: {
            (void)NumstrUncompress(
                PointerGetDatum(attrStart), attlen, *attrCmprsValSize, uncmprValBuf, &dataLenWithPadding, attalign);
            break;
        }

        default:
            Assert(false);
            break;
    }

    return dataLenWithPadding;
}

/* accordint to compression mode and meta information, set the length of compressed value,
 * decompress this attribute in the tuple, and return both of them.
 * mode [IN]: compression mode;
 * metaInfo [IN]: compression meta information;
 * typeOid [IN]: type oid of the attribute;
 * attlen [IN]: attr len of this attribute;
 * attrStart [IN]: start of attr data;
 * attrCmprsValSize [OUT]: length of compressed value;
 */
Datum PageCompress::UncompressOneAttr(
    char mode, void* metaInfo, Oid typeOid, int attlen, char* attrStart, int* attrCmprsValSize)
{
    switch (mode) {
        case CMPR_DELTA: {
            DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)metaInfo;
            if (deltaInfo->bytes == 0) {
                *attrCmprsValSize = 0;
                return deltaInfo->MinVal;
            }

            *attrCmprsValSize = deltaInfo->bytes;

            /*
             * typeOid is InvalidOid when this attribute is dropped. But its
             * typlen and typalign are also always kept. in this case, its
             * value cann't be accessed and computed because of lost atttype.
             * so that we just only skip it, and continue to access next valid attr.
             *
             *  please refer to RemoveAttributeById().
             */
            if (InvalidOid == typeOid) {
                return (Datum)0;
            }

            const NumberTypeOpt* opt = binarySearch(typeOid);
            return DeltaUncompress(deltaInfo, opt, (unsigned char*)attrStart);
        }

        case CMPR_DICT: {
            DictCmprMeta* dicInfo = (DictCmprMeta*)metaInfo;

            if (dicInfo->dictItemNum == 1) {
                *attrCmprsValSize = 0;
                char id = 0;
                return DictUncompress(dicInfo, &id, attlen);
            }
            *attrCmprsValSize = DICT_ITEMID_SIZE;
            return DictUncompress(dicInfo, attrStart, attlen);
        }

        case CMPR_PREFIX: {
            PrefixCmprMeta* prefixInfo = (PrefixCmprMeta*)metaInfo;
            return PrefixUncompress(attlen, prefixInfo, attrStart, attrCmprsValSize);
        }

        case CMPR_NUMSTR: {
            return NumstrUncompress(PointerGetDatum(attrStart), attlen, *attrCmprsValSize);
        }

        case CMPR_NONE: {
            Assert(false);
            return (Datum)0;
        }
        default:
            break;
    }

    Assert(false);
    return (Datum)0;
}

/* Get dictionary item ID */
bool PageCompress::DictCompress(int col, Datum val, unsigned char& dicItemId)
{
    bool found = false;
    int attlen = m_rel->rd_att->attrs[col].attlen;
    DictCmprMeta* dicInfo = (DictCmprMeta*)m_cmprMeta[col];
    DictItemData key;

    if (attlen > 0)
        FillHashKey<1>(key, attlen, val);
    else if (-1 == attlen)
        FillHashKey<-1>(key, attlen, val);
    else if (-2 == attlen)
        FillHashKey<-2>(key, attlen, val);

    dicItemId = dicInfo->GetDictItemId(&key, found);

    if (attlen > 0)
        pfree(key.itemData);

    return found;
}

/* compress the col's attribute of one tuple, ouput data is written into formTuple */
void PageCompress::CompressOneAttr(Datum val, bool null, FormCmprTupleData* formTuple, int col)
{
    formTuple->compressed[col] = false;

    if (null) {
        formTuple->isnulls[col] = true;
        formTuple->values[col] = (Datum)0;
        return;
    }

    FormData_pg_attribute* attrs = m_rel->rd_att->attrs;
    formTuple->values[col] = val;
    formTuple->isnulls[col] = false;

    switch (m_cmprMode[col]) {
        case CMPR_DELTA: {
            DeltaCmprMeta* metaInfo = (DeltaCmprMeta*)m_cmprMeta[col];
            const NumberTypeOpt* opt = m_numTypeOpt[col];
            unsigned char* deltaBuf = (unsigned char*)palloc((Size)metaInfo->bytes);
            if (DeltaCompress(metaInfo, opt, val, deltaBuf)) {
                formTuple->compressed[col] = true;
                if (metaInfo->bytes == 0) {
                    /*
                     * this means that all values of this column within page are equal to.
                     * we will store only the one into compression header, and null in every
                     * column for tuples.
                     * Notice that: keep this flag formTuple->isnulls[col] is false.
                     */
                    formTuple->valsize[col] = 0;
                    formTuple->values[col] = (Datum)0;
                    pfree(deltaBuf);
                } else {
                    formTuple->valsize[col] = metaInfo->bytes;
                    formTuple->values[col] = PointerGetDatum(deltaBuf);
                }
            } else
                pfree(deltaBuf);
            break;
        }

        case CMPR_DICT: {
            if (DictCompress(col, val, m_dictItemId[col])) {
                formTuple->compressed[col] = true;
                DictCmprMeta* dicInfo = (DictCmprMeta*)m_cmprMeta[col];
                if (dicInfo->dictItemNum == 1) {
                    formTuple->valsize[col] = 0;
                    formTuple->values[col] = (Datum)0;
                } else {
                    formTuple->valsize[col] = DICT_ITEMID_SIZE;
                    formTuple->values[col] = PointerGetDatum(m_dictItemId + col);
                }
            }
            break;
        }

        case CMPR_PREFIX: {
            int attrLen = attrs[col].attlen;
            Assert(attrLen == -1 || attrLen == -2);
            PrefixCmprMeta* prefixInfo = (PrefixCmprMeta*)m_cmprMeta[col];
            Datum cmprsVal;
            int cmprsValSize;
            bool successful = false;

            /*
             * we think that prefix method doesn't work well on those data,
             * which are compressed by lz method internally. so skipp it.
             */
            if (VARATT_IS_4B_C(DatumGetPointer(val))) {
                break;
            }

            if (-1 == attrLen)
                successful = PrefixCompress<-1>(prefixInfo, val, cmprsVal, cmprsValSize);
            else
                successful = PrefixCompress<-2>(prefixInfo, val, cmprsVal, cmprsValSize);

            if (successful) {
                formTuple->compressed[col] = true;
                formTuple->valsize[col] = cmprsValSize;
                formTuple->values[col] = cmprsVal;
            }
            break;
        }

        case CMPR_NUMSTR: {
            int attrLen = attrs[col].attlen;
            Assert(-1 == attrLen || -2 == attrLen);

            int outSize = 0;
            Datum outValue;

            if (-1 == attrLen) {
                outValue = NumstrCompress<-1>(val, outSize);
                Assert(outSize == 0 || (Size)outSize == VARSIZE_ANY(DatumGetPointer(outValue)));
            } else {
                outValue = NumstrCompress<-2>(val, outSize);
                Assert(outSize == 0 || (Size)outSize == strlen(DatumGetPointer(outValue)) + 1);
            }
            if (outSize > 0) {
                formTuple->compressed[col] = true;
                formTuple->valsize[col] = outSize;
                formTuple->values[col] = outValue;
            }
            break;
        }

        case CMPR_NONE: {
            break;
        }
        default:
            break;
    }
}

bool PageCompress::CompressOnePage(void)
{
    const int blksize = RelationGetTargetPageUsage(m_rel, HEAP_DEFAULT_FILLFACTOR);
    int attrNum = RelationGetNumberOfAttributes(m_rel);
    int size, row;
    int prevToastNum = m_toastTupsNum;
    int thisToasts;

    MemoryContext oldMemCnxt;
    ColArray* colArray = NULL;
    TupleDesc desc = RelationGetDescr(m_rel);
    FormCmprTupleData formTupleData;
    Datum* values = NULL;
    bool* isnulls = NULL;
    bool fillOnePage = false;
    bool hascmpr = false;
    bool remainBeforeCmpr = true;
    errno_t rc = EOK;

    /* reset local mem-context after handling one page tuples. */
    ResetPerPage();

    oldMemCnxt = MemoryContextSwitchTo(m_pageMemCnxt);

    /* at the end of the current batch, only toast tuples need to be written into pages. */
    if ((m_current == m_inTupsNum) || m_nextBatch) {
        Assert(m_mappedTupsNum == 0);
        ResetCmprMeta();
        /* jump to branch handle_toasts only if m_current is equal to m_inTupsNum */
        goto handle_toasts;
    }

    /*
     * Step 1: how many rows are compressed and form one page
     * We need evaluate proper row numbers.
     */
    size = SizeOfHeapPageHeaderData;
    for (row = m_current; row < m_inTupsNum; ++row) {
        /* collect all the toast tuples, and write once at the end of each batch */
        if (isToastTuple(m_inTups[row]))
            m_toastTups[m_toastTupsNum++] = m_inTups[row];
        else {
            if (size + (int)getTupleSpace(m_inTups[row]) > blksize)
                break;

            /* collect all the untoast tuples, and enter comperss flow */
            m_mappedTups[m_mappedTupsNum++] = m_inTups[row];
            size += (int)getTupleSpace(m_inTups[row]);
        }
    }

    /*
     * raw tuples cann't fill one page, then we must check m_last flag to
     * write these ones. and toast tuples would be taken care.
     */
    if (row == m_inTupsNum) {
        ResetCmprMeta();
        remainBeforeCmpr = true;
        goto handle_remains;
    }

    /* Step 2: deform tuples into ColArray */
    colArray = DeformTups(m_mappedTups, m_mappedTupsNum);

    /*
     * Step 3: Sampling data of each column and choose proper compression method
     * We need generate compression meta information for step 3.
     */
    ChooseCmprMethod(colArray);
    SetCmprHeaderData();
    if (m_cmprHeaderSize == 0) {
        /* all the tuples within the current batch are not suitable for compress. */
        OutputRawTupsBeforeCmpr(m_toastTupsNum - prevToastNum);
        (void)MemoryContextSwitchTo(oldMemCnxt);
        return true;
    }

#ifdef USE_ASSERT_CHECKING
    CheckCmprHeaderData();
#endif

    /* Step 4: Compress the current batch rows. */
    formTupleData.compressed = (bool*)palloc((Size)(sizeof(bool) * attrNum));
    formTupleData.valsize = (int*)palloc((Size)(sizeof(int) * attrNum));
    formTupleData.values = (Datum*)palloc((Size)(sizeof(Datum) * attrNum));
    formTupleData.isnulls = (bool*)palloc((Size)(sizeof(bool) * attrNum));
    size = (int)MAXALIGN(SizeOfHeapPageHeaderData) + (int)MAXALIGN((uint32)m_cmprHeaderSize);

    for (row = 0; row < m_mappedTupsNum; ++row) {
        hascmpr = false;

        for (int col = 0; col < attrNum; ++col) {
            CompressOneAttr(colArray[col].val[row], colArray[col].nulls[row], &formTupleData, col);
            hascmpr = (hascmpr || formTupleData.compressed[col]);
#ifdef USE_ASSERT_CHECKING
            CheckCmprAttr(colArray[col].val[row], colArray[col].nulls[row], col, &formTupleData);
#endif
        }

        /*
         * form compressed tuple and then dispatch until part of tuples fill one page,
         * or all compressed tuples fill part of one page.
         */
        if (hascmpr) {
            fillOnePage = Dispatch(m_mappedTups[row], (HeapTuple)tableam_tops_form_cmprs_tuple(desc, &formTupleData, TableAmHeap), size, blksize);
        } else {
            fillOnePage = Dispatch(m_mappedTups[row], size, blksize);
        }

        if (fillOnePage)
            break;
    }

    /*
     * Step 5: Judge whether compressed data is enough to fill one page
     * If not enough, we compress the next tuples. But it is different from
     * Step 4, if one column is compressed by delta value encoding, we can't
     * change delta value.
     */
    if (fillOnePage) {
        Assert(row < m_mappedTupsNum);
        ResetCmprMeta();
        OutputRawTupsAfterCmpr(m_toastTupsNum - prevToastNum);
        (void)MemoryContextSwitchTo(oldMemCnxt);
        return true;
    }
    Assert(row == m_mappedTupsNum);

    values = (Datum*)palloc((Size)(sizeof(Datum) * attrNum));
    isnulls = (bool*)palloc((Size)(sizeof(bool) * attrNum));

    /* continue to read and compress following tuples, until one page is filled */
    for (row = m_current + m_mappedTupsNum + (m_toastTupsNum - prevToastNum); row < m_inTupsNum; ++row) {
        HeapTuple raw = m_inTups[row];
        if (isToastTuple(raw)) {
            m_toastTups[m_toastTupsNum++] = raw;
            continue;
        }

        heap_deform_tuple(raw, desc, values, isnulls);
        hascmpr = false;

        for (int col = 0; col < attrNum; ++col) {
            CompressOneAttr(values[col], isnulls[col], &formTupleData, col);
            hascmpr = (hascmpr || formTupleData.compressed[col]);
#ifdef USE_ASSERT_CHECKING
            CheckCmprAttr(values[col], isnulls[col], col, &formTupleData);
#endif
        }

        if (hascmpr) {
            fillOnePage = Dispatch(raw, (HeapTuple)tableam_tops_form_cmprs_tuple(desc, &formTupleData, TableAmHeap), size, blksize);
        } else {
            fillOnePage = Dispatch(raw, size, blksize);
        }

        if (fillOnePage)
            break;
    }

    if (fillOnePage || m_last) {
        Assert(fillOnePage || (row == m_inTupsNum));
        OutputCmprTups(m_toastTupsNum - prevToastNum);
        (void)MemoryContextSwitchTo(oldMemCnxt);
        return true;
    }

    /*
     * after compress, remain tuples cann't fill one page.
     * we think that cotinue to read and compress more tuples from next batch
     * maybe is better. otherwise this page will holds one tuple at least.
     */
    Assert(row == m_inTupsNum && m_last == false);
    m_cmprHeaderSize = 0;
    remainBeforeCmpr = false;

handle_remains:

    thisToasts = m_toastTupsNum - prevToastNum;
    if (m_mappedTupsNum == 0) {
        /* case 1: only toast tuples remain. */
        Assert(thisToasts == (m_inTupsNum - m_current));
        m_current = m_inTupsNum;
    } else if (m_last) {
        /*
         * case 2: for the last batch,
         * first write all the raw tuples left into pages;
         * then handle the remain toast tuples;
         */
        Assert(remainBeforeCmpr);
        OutputRawTupsBeforeCmpr(thisToasts);
        Assert(m_current == m_inTupsNum);
        (void)MemoryContextSwitchTo(oldMemCnxt);
        return true;
    } else {
        /*
         * case 3: if not the last batch
         * first write all toasts of the current batch into pages;
         * then move the remain into next batch;
         */
        m_nextBatch = true;

        /*
         * case 3.1: toats and raw tuples must be collected sperately;
         */
        if (thisToasts > 0) {
            /*
             * step 1: collect these toasts in the front, make sure they are free
             *       after they are written into pages by heap_multi_insert().
             */
            rc = memcpy_s(m_inTups + m_current,
                          (size_t)(sizeof(HeapTuple) * thisToasts),
                          m_toastTups + prevToastNum,
                          (size_t)(sizeof(HeapTuple) * thisToasts));
            securec_check(rc, "", "");
            m_current += thisToasts;
        }

        /* step 2: collect raw tuples in the end, make sure they are moved ahead next buffer. */
        if (remainBeforeCmpr) {
            /* case 3.2.1: raw tuples are all in m_mappedTups[m_mappedTupsNum] */
            rc = memcpy_s(m_inTups + m_current,
                          (size_t)(sizeof(HeapTuple) * m_mappedTupsNum),
                          m_mappedTups,
                          (size_t)(sizeof(HeapTuple) * m_mappedTupsNum));
            securec_check(rc, "", "");
        } else {
            /*
             * case 3.2.2: some are int m_mappedTups[m_cmprTupsNum], and the others
             *   are in m_cmprTups[m_uncmprTupIdx, m_inTupsNum)
             */
            int offset = m_current;
            if (m_cmprTupsNum > 0) {
                rc = memcpy_s(m_inTups + offset,
                              (size_t)(sizeof(HeapTuple) * m_cmprTupsNum),
                              m_mappedTups,
                              (size_t)(sizeof(HeapTuple) * m_cmprTupsNum));
                securec_check(rc, "", "");
                offset += m_cmprTupsNum;
                m_cmprTupsNum = 0;
            }

            int uncmpr = m_inTupsNum - m_uncmprTupIdx;
            if (uncmpr > 0) {
                rc = memcpy_s(m_inTups + offset, (size_t)(sizeof(HeapTuple) * uncmpr), m_cmprTups + m_uncmprTupIdx,
                              (size_t)(sizeof(HeapTuple) * uncmpr));
                securec_check(rc, "", "");
            }
        }
    }

handle_toasts:

    if (m_toastTupsNum > 0) {
        OutputToasts();
        (void)MemoryContextSwitchTo(oldMemCnxt);
        return true;
    }

    /* all tuples of this batch have been handled. */
    pfree(m_toastTups);
    pfree(m_mappedTups);
    pfree(m_cmprTups);
    m_toastTups = NULL;
    m_mappedTups = NULL;
    m_cmprTups = NULL;

    (void)MemoryContextSwitchTo(oldMemCnxt);
    return false;
}

void PageCompress::OutputRawTupsBeforeCmpr(int nToasts)
{
    /*
     * write all the tuples left out in the last batch.
     * make m_current equal to m_inTupsNum so that toast tuples
     * will be written into pages at the last time.
     */
    m_outputTups = m_mappedTups;
    m_outputTupsNum = m_mappedTupsNum;

#ifdef USE_ASSERT_CHECKING
    CheckOutputTups(NULL, 0, m_mappedTups, m_mappedTupsNum, nToasts);
#endif
#ifdef TRACE_COMPRESS
    ++nLoops;
    TraceCmprPage(0, NULL, NULL, 0, m_mappedTups, m_mappedTupsNum, nToasts);
#endif

    m_current += (m_outputTupsNum + nToasts);
    m_cmprHeaderSize = 0;
    m_cmprTupsNum = 0;
}

void PageCompress::OutputRawTupsAfterCmpr(int nToasts)
{
    /*
     * after compress, raw tuples exist in both m_mappedTups[] and m_cmprTups[].
     * and the second range is [m_uncmprTupIdx, m_inTupsNum).
     * we have to collect all raw tuples into m_mappedTups[].
     */
    m_outputTups = m_mappedTups;
    int uncmprNum = m_inTupsNum - m_uncmprTupIdx;
    if (uncmprNum > 0) {
        errno_t rc = memcpy_s(m_mappedTups + m_cmprTupsNum,
                              (size_t)(sizeof(HeapTuple) * uncmprNum),
                              m_cmprTups + m_uncmprTupIdx,
                              (size_t)(sizeof(HeapTuple) * uncmprNum));
        securec_check(rc, "", "");
    }
    Assert((m_cmprTupsNum + uncmprNum) <= m_mappedTupsNum);
    m_outputTupsNum = m_mappedTupsNum;

#ifdef USE_ASSERT_CHECKING
    CheckOutputTups(
        m_mappedTups, m_cmprTupsNum, (m_mappedTups + m_cmprTupsNum), (m_mappedTupsNum - m_cmprTupsNum), nToasts);
#endif
#ifdef TRACE_COMPRESS
    ++nLoops;
    TraceCmprPage(1, NULL, NULL, 0, m_outputTups, m_outputTupsNum, nToasts);
#endif

    m_current += (m_outputTupsNum + nToasts);
    m_cmprHeaderSize = 0;
    m_cmprTupsNum = 0;
}

void PageCompress::OutputCmprTups(int nToasts)
{
    /*
     * after compress, output tuples exist in only m_cmprTups[], both compressed and some raws.
     * the previous range is [0, m_cmprTupsNum) and the other is [m_uncmprTupIdx, m_inTupsNum).
     * we have to append the second part to the first part.
     */
    Assert(m_cmprTupsNum <= m_uncmprTupIdx);
    m_outputTups = m_cmprTups;
    int uncmprNum = m_inTupsNum - m_uncmprTupIdx;
    if (0 == uncmprNum)
        m_outputTupsNum = m_cmprTupsNum;
    else if (m_cmprTupsNum == m_uncmprTupIdx)
        m_outputTupsNum = m_inTupsNum;
    else {
        errno_t rc = memmove_s(m_cmprTups + m_cmprTupsNum,
                               (size_t)(sizeof(HeapTuple) * uncmprNum),
                               m_cmprTups + m_uncmprTupIdx,
                               (size_t)(sizeof(HeapTuple) * uncmprNum));
        securec_check(rc, "", "");
        m_outputTupsNum = m_cmprTupsNum + uncmprNum;
    }

#ifdef USE_ASSERT_CHECKING
    Assert(m_outputTupsNum >= m_mappedTupsNum);
    CheckOutputTups(m_mappedTups, m_cmprTupsNum, (m_cmprTups + m_cmprTupsNum), uncmprNum, nToasts);
#endif
#ifdef TRACE_COMPRESS
    ++nLoops;
    TraceCmprPage(2, m_cmprTups, m_mappedTups, m_cmprTupsNum, (m_cmprTups + m_cmprTupsNum), uncmprNum, nToasts);
#endif

    m_current += (m_outputTupsNum + nToasts);
}

void PageCompress::OutputToasts(void)
{
    /*
     * toast tuples within each batch are collected togather, and written once
     * into pages before next SetBatchTuples() is called.
     */
    m_outputTups = m_toastTups;
    m_outputTupsNum = m_toastTupsNum;
#ifdef TRACE_COMPRESS
    ++nLoops;
    ereport(INFO, (errmsg("Loop %d, Tag: Toast Tuples, num %d", nLoops, m_toastTupsNum)));
#endif
    m_toastTupsNum = 0;
    Assert((m_last == false) || (m_current == m_inTupsNum));
    Assert(m_cmprHeaderSize == 0);
    Assert(m_cmprTupsNum == 0);
}

void PageCompress::SetBatchTuples(HeapTuple* tups, int ntups, bool isLast)
{
    m_inTups = tups;
    m_inTupsNum = ntups;
    m_current = 0;
    m_last = isLast;

    /*
     * we know the number of input tuples, so alloc the arrays for handling toast
     * tuples. after this batch has been handled, free these memory.
     */
    MemoryContext oldMemCnxt = MemoryContextSwitchTo(m_parentMemCxt);

    m_toastTups = (HeapTuple*)palloc((Size)(sizeof(HeapTuple) * ntups));
    m_mappedTups = (HeapTuple*)palloc((Size)(sizeof(HeapTuple) * ntups));
    m_cmprTups = (HeapTuple*)palloc((Size)(sizeof(HeapTuple) * ntups));
    m_toastTupsNum = 0;
    m_nextBatch = false;

    (void)MemoryContextSwitchTo(oldMemCnxt);
}

void PageCompress::ResetPerPage(void)
{
    int attNum = m_rel->rd_att->natts;

    /* Free everything by destroying dictionary */
    for (int att = 0; att < attNum; ++att) {
        if (m_cmprMode[att] == CMPR_DICT && m_cmprMeta[att]) {
            ((DictCmprMeta*)m_cmprMeta[att])->Destroy();
            m_cmprMeta[att] = NULL;
        }
    }

    MemoryContextReset(m_pageMemCnxt);
    /* m_cmprHeaderData would be fixed during the entire compress, keep it. */
    m_cmprHeaderSize = 0;
    m_outputTups = NULL;
    m_outputTupsNum = 0;

    m_mappedTupsNum = 0;
    m_cmprTupsNum = 0;
    m_uncmprTupIdx = m_inTupsNum;
}

/*
 * reset all compress meta info when tuples are not compressed and output.
 *   1. before OutputRawTupsAfterCmpr() is called;
 *   2. before OutputRawTupsBeforeCmpr() is called, optional;
 *   3. before branch handle_remains is jumpped to;
 *   4. before branch handle_toasts is jumpped to;
 */
void PageCompress::ResetCmprMeta(void)
{
    int attNum = m_rel->rd_att->natts;
    for (int i = 0; i < attNum; ++i) {
        m_cmprMode[i] = CMPR_NONE;
        m_cmprMeta[i] = NULL;
    }
    m_cmprHeaderSize = 0;
}

/*
 * it's different from BackWrite(), which transforms compressed tuples' info
 * backward into raw tuples. but this method will transforms raw tuples' info
 * forward into compressed tuples.
 * NB: now this method applies to VACUUM FULL && CLUSTER
 */
void PageCompress::ForwardWrite(void)
{
    if (m_cmprTupsNum == 0)
        return;
    errno_t rc = EOK;

    Assert(m_cmprHeaderSize > 0);
    for (int i = 0; i < m_cmprTupsNum; i++) {
        HeapTupleHeader dest = m_cmprTups[i]->t_data;
        HeapTupleHeader src = m_mappedTups[i]->t_data;

        Assert(HEAP_TUPLE_IS_COMPRESSED(dest));
        Assert(dest->t_hoff == src->t_hoff);
        Assert(HeapTupleHeaderGetNatts(dest, m_rel->rd_att) == HeapTupleHeaderGetNatts(src, m_rel->rd_att));
        Assert((0x000F & dest->t_infomask) == (0x000F & src->t_infomask));
        Assert(
            (0 == (dest->t_infomask & HEAP_HASNULL)) ||
            0 == memcmp(
                (char*)dest->t_bits, (char*)src->t_bits, BITMAPLEN(HeapTupleHeaderGetNatts(src, m_rel->rd_att))));

        rc = memcpy_s((char*)dest, src->t_hoff, (char*)src, src->t_hoff);
        securec_check(rc, "", "");
        HEAP_TUPLE_SET_COMPRESSED(dest);
    }
}

/*
 * after heap_multi_insert() is done, we should write infomask back
 * to the raw tuples. these information will be used by trigger and
 * index build.
 * this api exists just to reduce public methods as much as possible.
 */
void PageCompress::BackWrite(void)
{
    /* compressed tuples exist and are valid only if m_cmprHeaderSize > 0 */
    if (m_cmprTupsNum == 0)
        return;

    errno_t rc = EOK;

    Assert(m_cmprHeaderSize > 0);
    for (int i = 0; i < m_cmprTupsNum; i++) {
        HeapTupleHeader dest = m_mappedTups[i]->t_data;
        HeapTupleHeader src = m_cmprTups[i]->t_data;

        Assert(dest->t_hoff == src->t_hoff);
        Assert((0x07FF & dest->t_infomask2) == (0x07FF & src->t_infomask2));
        Assert((0x000F & dest->t_infomask) == (0x000F & src->t_infomask));
        Assert(HeapTupleHasNulls(m_mappedTups[i]) == HeapTupleHasNulls(m_cmprTups[i]));

        rc = memcpy_s((char*)dest, src->t_hoff, (char*)src, src->t_hoff);
        securec_check(rc, "", "");
        HEAP_TUPLE_CLEAR_COMPRESSED(dest);
        m_mappedTups[i]->t_self = m_cmprTups[i]->t_self;
        m_mappedTups[i]->t_tableOid = m_cmprTups[i]->t_tableOid;
#ifdef PGXC
        m_mappedTups[i]->t_xc_node_id = m_cmprTups[i]->t_xc_node_id;
#endif
    }
}

bool PageCompress::Dispatch(HeapTuple raw, HeapTuple cmpr, int& nowSize, const int& blksize)
{
    Assert(HEAP_TUPLE_IS_COMPRESSED(cmpr->t_data));
    int needSize = (int)getTupleSpace(cmpr);
    if (nowSize + needSize > blksize)
        return true;

    Assert(m_cmprTupsNum < m_uncmprTupIdx);
    HeapTupleCopyBase(cmpr, raw);
    m_cmprTups[m_cmprTupsNum] = cmpr;
    m_mappedTups[m_cmprTupsNum] = raw;
    ++m_cmprTupsNum;
#ifdef USE_ASSERT_CHECKING
    CheckCmprTuple(raw, cmpr);
#endif
    nowSize += needSize;
    return false;
}

bool PageCompress::Dispatch(HeapTuple raw, int& nowSize, const int& blksize)
{
    Assert(!HEAP_TUPLE_IS_COMPRESSED(raw->t_data));
    int needSize = (int)getTupleSpace(raw);
    if (nowSize + needSize > blksize)
        return true;

    Assert(m_uncmprTupIdx > m_cmprTupsNum);
    --m_uncmprTupIdx;
    m_cmprTups[m_uncmprTupIdx] = raw;
    nowSize += needSize;
    return false;
}

#ifdef USE_ASSERT_CHECKING

void PageCompress::CheckCmprDatum(Datum arg1, Datum arg2, Form_pg_attribute attr)
{
    int attlen = attr->attlen;
    errno_t rc = EOK;

    if (attr->attbyval) {
        Assert(attlen > 0 && attlen <= SIZEOF_DATUM);
        Assert(arg1 == arg2);
        return;
    }

    if (attlen > 0) {
        Assert(0 == memcmp(DatumGetPointer(arg1), DatumGetPointer(arg2), attlen));
        return;
    }

    /*
     * we pack compressed data as many as possible, even its storage type is PLAIN;
     * so raw data must be first packed before comparing;
     */
    char packedBuf1[VARATT_SHORT_MAX] = {0};
    char packedBuf2[VARATT_SHORT_MAX] = {0};
    Pointer pOldValue1 = DatumGetPointer(arg1);
    Pointer pOldValue2 = DatumGetPointer(arg2);

    if (-1 == attr->attlen) {
        if (!VARATT_IS_EXTERNAL(pOldValue1) && !VARATT_IS_SHORT(pOldValue1) &&
            VARATT_CONVERTED_SHORT_SIZE(pOldValue1) <= VARATT_SHORT_MAX) {
            Size len = VARATT_CONVERTED_SHORT_SIZE(pOldValue1);
            Assert(len <= VARATT_SHORT_MAX);
            SET_VARSIZE_SHORT(&packedBuf1[0], len);
            rc = memcpy_s(&packedBuf1[1], VARATT_SHORT_MAX - VARHDRSZ_SHORT, VARDATA(pOldValue1), len - 1);
            securec_check(rc, "", "");
            arg1 = PointerGetDatum(packedBuf1);
        }

        if (!VARATT_IS_EXTERNAL(pOldValue2) && !VARATT_IS_SHORT(pOldValue2) &&
            VARATT_CONVERTED_SHORT_SIZE(pOldValue2) <= VARATT_SHORT_MAX) {
            Size len = VARATT_CONVERTED_SHORT_SIZE(pOldValue2);
            Assert(len <= VARATT_SHORT_MAX);
            SET_VARSIZE_SHORT(&packedBuf2[0], len);
            rc = memcpy_s(&packedBuf2[1], VARATT_SHORT_MAX - VARHDRSZ_SHORT, VARDATA(pOldValue2), len - 1);
            securec_check(rc, "", "");
            arg2 = PointerGetDatum(packedBuf2);
        }
    }

    char* ptr1 = DatumGetPointer(arg1);
    char* ptr2 = DatumGetPointer(arg2);
    int size1 = 0;
    int size2 = 0;

    if (-1 == attlen) {
        size1 = VARSIZE_ANY(ptr1);
        size2 = VARSIZE_ANY(ptr2);
    } else if (-2 == attlen) {
        size1 = strlen(ptr1) + 1;
        size2 = strlen(ptr2) + 1;
    }

    Assert(size1 == size2);
    Assert(0 == memcmp(ptr2, ptr1, size1));
    return;
}

void PageCompress::CheckCmprAttr(Datum rawVal, bool rawNull, int col, FormCmprTupleData* formTuple)
{
    Assert(rawNull == formTuple->isnulls[col]);
    if (rawNull) {
        Assert((Datum)0 == formTuple->values[col]);
        return;
    }

    if (formTuple->compressed[col] == false) {
        Assert(rawVal == formTuple->values[col]);
        return;
    }

    Assert(m_cmprMode[col] != CMPR_UNDEF);
    FormData_pg_attribute* atts = RelationGetDescr(m_rel)->attrs;
    Assert(1 != atts[col].attlen);
    int cmprSize;
    Datum value = UncompressOneAttr(m_cmprMode[col],
                                    m_cmprMeta[col],
                                    atts[col].atttypid,
                                    atts[col].attlen,
                                    DatumGetPointer(formTuple->values[col]),
                                    &cmprSize);

    Assert(cmprSize == formTuple->valsize[col]);
    CheckCmprDatum(rawVal, value, &atts[col]);
}

void PageCompress::CheckCmprTuple(HeapTuple rawTup, HeapTuple cmprTup)
{
    TupleDesc desc = RelationGetDescr(m_rel);
    FormData_pg_attribute* atts = desc->attrs;
    int nattrs = desc->natts;

    Datum* values = (Datum*)palloc(sizeof(Datum) * nattrs);
    Datum* values2 = (Datum*)palloc(sizeof(Datum) * nattrs);
    bool* isnulls = (bool*)palloc(sizeof(bool) * nattrs);
    bool* isnulls2 = (bool*)palloc(sizeof(bool) * nattrs);

    heap_deform_cmprs_tuple(cmprTup, desc, values, isnulls, m_cmprHeaderData);
    heap_deform_tuple(rawTup, desc, values2, isnulls2);

    for (int col = 0; col < nattrs; ++col) {
        Assert(isnulls[col] == isnulls2[col]);
        if (isnulls[col]) {
            Assert((Datum)0 == values[col]);
            Assert((Datum)0 == values2[col]);
            continue;
        }

        CheckCmprDatum(values[col], values2[col], &atts[col]);
    }

    pfree(values);
    pfree(isnulls);
    pfree(values2);
    pfree(isnulls2);
}

void PageCompress::CheckCmprHeaderData(void)
{
    if (m_cmprHeaderSize == 0) {
        return;
    }

    FormData_pg_attribute* atts = RelationGetDescr(m_rel)->attrs;
    int nattrs = RelationGetNumberOfAttributes(m_rel);
    int cmprsOff = 0; /* pointer to the start of compression meta */
    void* metaInfo = NULL;
    char mode = 0;

    for (int col = 0; col < nattrs; ++col) {
        int metaSize = 0;
        metaInfo = PageCompress::FetchAttrCmprMeta(m_cmprHeaderData + cmprsOff, atts[col].attlen, &metaSize, &mode);
        cmprsOff += metaSize;

        Assert(mode != CMPR_UNDEF && m_cmprMode[col] == mode);
        switch (mode) {
            case CMPR_DELTA: {
                DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)metaInfo;
                DeltaCmprMeta* target = (DeltaCmprMeta*)m_cmprMeta[col];
                Assert(deltaInfo->bytes == target->bytes);
                Assert(deltaInfo->MinVal == target->MinVal);

                break;
            }

            case CMPR_DICT: {
                DictCmprMeta* dictMeta = (DictCmprMeta*)metaInfo;
                DictCmprMeta* target = (DictCmprMeta*)m_cmprMeta[col];
                Assert(dictMeta->dictItemNum == target->dictItemNum);

                for (int i = 0; i < dictMeta->dictItemNum; ++i) {
                    Assert(dictMeta->dictItems[i].itemSize == target->dictItems[i].itemSize);
                    Assert(0 == memcmp(dictMeta->dictItems[i].itemData, target->dictItems[i].itemData,
                                       target->dictItems[i].itemSize));
                }

                break;
            }

            case CMPR_PREFIX: {
                PrefixCmprMeta* prefixMeta = (PrefixCmprMeta*)metaInfo;
                PrefixCmprMeta* target = (PrefixCmprMeta*)m_cmprMeta[col];
                Assert(prefixMeta->len == target->len);
                Assert(0 == memcmp(prefixMeta->prefixStr, target->prefixStr, target->len));

                break;
            }

            case CMPR_NUMSTR:
            case CMPR_NONE:
                break;
            default:
                break;
        }
    }

    Assert(m_cmprHeaderSize == (int)MAXALIGN(cmprsOff));
    Assert(m_cmprHeaderSize < BLCKSZ);
}

/*
 * check the number and lossless for input tuples.
 *   1. dispatch to toast arrays;
 *   2. dispatch to uncompressed arrays;
 *   3. compress and then mapped to mapped arrays;
 * Important: call before m_current is modified and changed.
 */
void PageCompress::CheckOutputTups(HeapTuple* mapped, int nMapped, HeapTuple* uncmpr, int nUncmpr, int nToasts)
{
    int start = m_current;
    int end = m_current + m_outputTupsNum + nToasts;
    int realToasts = 0;
    int realCmpr = 0;
    int realUncmpr = 0;

    Assert((m_outputTupsNum == nToasts) || (m_outputTupsNum == (nMapped + nUncmpr)));

    for (; start < end; ++start) {
        HeapTuple check = m_inTups[start];
        bool found = false;

        if (isToastTuple(check)) {
            for (int i = m_toastTupsNum - nToasts; i < m_toastTupsNum; ++i) {
                if (check != m_toastTups[i])
                    continue;
                found = true;
                ++realToasts;
                break;
            }

            Assert(found == true);
            continue;
        }

        for (int i = 0; i < nMapped; ++i) {
            if (check != mapped[i])
                continue;
            found = true;
            ++realCmpr;
            break;
        }

        if (found == true)
            continue;

        for (int i = 0; i < nUncmpr; ++i) {
            if (check != uncmpr[i])
                continue;
            found = true;
            ++realUncmpr;
            break;
        }
        Assert(found == true);
    }

    Assert(nToasts == realToasts);
    Assert(nMapped == realCmpr);
    Assert(nUncmpr == realUncmpr);
}

#endif

#ifdef TRACE_COMPRESS

void PageCompress::TraceCmprPage(
    int flag, HeapTuple* cmpr, HeapTuple* mapped, int nCmpr, HeapTuple* uncmpr, int nUncmpr, int nToasts)
{
    int start = m_current;
    int end = m_current + m_outputTupsNum + nToasts;
    int nattrs = RelationGetNumberOfAttributes(m_rel);
    Form_pg_attribute* atts = RelationGetDescr(m_rel)->attrs;

    char* tag = NULL;
    if (flag == 0)
        tag = "Tag: Raw Tuples Before Compress";
    else if (flag == 1)
        tag = "Tag: Raw Tuples After Compress";
    else if (flag == 2)
        tag = "Tag: Compressed Tuples Of one page";
    else
        tag = "Tag: Toast Tuples";
    ereport(INFO, (errmsg("Loop %d, range[%d - %d], %s", nLoops, start, (end - 1), tag)));

    DeltaCmprMeta* deltaMeta = NULL;
    PrefixCmprMeta* prefixMeta = NULL;
    DictCmprMeta* dictMeta = NULL;

    for (int col = 0; col < nattrs; ++col) {
        switch (m_cmprMode[col]) {
            case CMPR_NONE:
                ereport(INFO, (errmsg("COL %d: None Compress", col)));
                break;

            case CMPR_DELTA:
                deltaMeta = (DeltaCmprMeta*)m_cmprMeta[col];
                ereport(INFO,
                        (errmsg("COL %d: Delta Compress, attrlen %d, delta bytes %d",
                                col,
                                atts[col]->attlen,
                                deltaMeta->bytes)));
                break;

            case CMPR_NUMSTR:
                ereport(INFO, (errmsg("COL %d: Number String Compress", col)));
                break;

            case CMPR_PREFIX:
                prefixMeta = (PrefixCmprMeta*)m_cmprMeta[col];
                ereport(INFO, (errmsg("COL %d: Prefix Compress, len %d", col, prefixMeta->len)));
                break;

            case CMPR_DICT:
                dictMeta = (DictCmprMeta*)m_cmprMeta[col];
                char str[0xFF * (sizeof(int16) + 1) + 1] = {0};
                char* p = str;
                for (int i = 0; i < dictMeta->dictItemNum; ++i) {
                    (void)sprintf_s(p, sizeof(str), "%2d ", dictMeta->dictItems[i].itemSize);
                    p = p + 3;
                }
                ereport(INFO, (errmsg("COL %d: Dict Compress, num %d, len {%s}", col, dictMeta->dictItemNum, str)));
                break;
            default:
                break;
        }
    }

    for (; start < end; ++start) {
        HeapTuple check = m_inTups[start];

        if (isToastTuple(check)) {
            for (int i = m_toastTupsNum - nToasts; i < m_toastTupsNum; ++i) {
                if (check != m_toastTups[i])
                    continue;
                ereport(INFO, (errmsg("	[%d] TOAST, raw len %d", start, check->t_len)));
                break;
            }
            continue;
        }

        for (int i = 0; i < nCmpr; ++i) {
            if (check != mapped[i])
                continue;
            ereport(INFO, (errmsg("	[%d] CMPR, raw len %d, cmpr len %d", start, check->t_len, cmpr[i]->t_len)));
            continue;
        }

        for (int i = 0; i < nUncmpr; ++i) {
            if (check != uncmpr[i])
                continue;
            ereport(INFO, (errmsg("	[%d] UNCMPR, raw len %d", start, check->t_len)));
            break;
        }
    }

    ereport(INFO, (errmsg("Summary: cmpr %d, uncmpr %d, toasts %d", nCmpr, nUncmpr, nToasts)));
}

#endif

/* binary search.
 * negative will be returned if search fails. Otherwise return the position of oid.
 */
static const NumberTypeOpt* binarySearch(Oid typeOid)
{
    int left = 0;
    int right = g_number_type_count - 1;

    while (left <= right) {
        int middle = left + ((right - left) / 2);

        if (g_number_types[middle].typeOid > typeOid) {
            right = middle - 1;
        } else if (g_number_types[middle].typeOid < typeOid) {
            left = middle + 1;
        } else
            return (g_number_types + middle);
    }

    return NULL;
}

/* int16 Compare && Minus && Plus functions. */
static int int16Compare(Datum arg1, Datum arg2)
{
    int16 val1 = DatumGetInt16(arg1);
    int16 val2 = DatumGetInt16(arg2);
    if (val1 > val2)
        return (1);
    if (val1 < val2)
        return (-1);
    return (0);
}

static int64 int16Minus(Datum arg1, Datum arg2)
{
    int16 val1 = DatumGetInt16(arg1);
    int16 val2 = DatumGetInt16(arg2);

    PG_RETURN_INT64((int64)(val1 - val2));
}

/* we don't care overflow problem about the plus, it's because:
 *   arg1 must be the result returned by int16Minus();
 *   arg2 is from <arg2> within int16Minus();
 */
static Datum int16Plus(Datum arg1, int64 arg2)
{
    int16 val1 = DatumGetInt16(arg1);
    int16 val2 = (int16)arg2;

    PG_RETURN_INT16((int16)(val1 + val2));
}

/* int32 Compare && Minus && Plus functions. */
static int int32Compare(Datum arg1, Datum arg2)
{
    int32 val1 = DatumGetInt32(arg1);
    int32 val2 = DatumGetInt32(arg2);
    if (val1 > val2)
        return 1;
    if (val1 < val2)
        return -1;
    return 0;
}

static int64 int32Minus(Datum arg1, Datum arg2)
{
    int32 val1 = DatumGetInt32(arg1);
    int32 val2 = DatumGetInt32(arg2);

    PG_RETURN_INT64((int64)(val1 - val2));
}

/* we don't care overflow problem about the plus, it's because:
 *   arg1 must be the result returned by int32Minus();
 *   arg2 is from <arg2> within int32Minus();
 */
static Datum int32Plus(Datum arg1, int64 arg2)
{
    int32 val1 = DatumGetInt32(arg1);
    int32 val2 = (int32)arg2;

    PG_RETURN_INT32((int32)(val1 + val2));
}

/* uint32 Compare && Minus && Plus functions. */
static int uint32Compare(Datum arg1, Datum arg2)
{
    uint32 val1 = DatumGetUInt32(arg1);
    uint32 val2 = DatumGetUInt32(arg2);
    if (val1 > val2)
        return 1;
    if (val1 < val2)
        return -1;
    return 0;
}

static int64 uint32Minus(Datum arg1, Datum arg2)
{
    uint32 val1 = DatumGetUInt32(arg1);
    uint32 val2 = DatumGetUInt32(arg2);

    PG_RETURN_INT64((int64)(val1 - val2));
}

/* we don't care overflow problem about the plus, it's because:
 *   arg1 must be the result returned by uint32Minus();
 *   arg2 is from <arg2> within uint32Minus();
 */
static Datum uint32Plus(Datum arg1, int64 arg2)
{
    uint32 val1 = DatumGetUInt32(arg1);
    uint32 val2 = (uint32)arg2;

    PG_RETURN_UINT32((uint32)(val1 + val2));
}

/* int64 Compare && Minus && Plus functions. */
static int int64Compare(Datum arg1, Datum arg2)
{
    int64 val1 = DatumGetInt64(arg1);
    int64 val2 = DatumGetInt64(arg2);
    if (val1 > val2)
        return 1;
    if (val1 < val2)
        return -1;
    return 0;
}

static int64 int64Minus(Datum arg1, Datum arg2)
{
    int64 val1 = DatumGetInt64(arg1);
    int64 val2 = DatumGetInt64(arg2);

    PG_RETURN_INT64((int64)(val1 - val2));
}

/* we don't care overflow problem about the plus, it's because:
 *   arg1 must be the result returned by int64Minus();
 *   arg2 is from <arg2> within int64Minus();
 */
static Datum int64Plus(Datum arg1, int64 arg2)
{
    int64 val1 = DatumGetInt64(arg1);

    PG_RETURN_INT64((int64)(val1 + arg2));
}
