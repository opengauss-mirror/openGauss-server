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
 * pagecompress.h
 *        page compression manager routines
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/pagecompress.h
 *
 * mixed compression methods will be used, including delta/prefix/dict compression.
 * delta compression would apply to data types: number; datetime; timestamp;  number string;
 * prefix compression would apply to data types: string;
 * dictionary compression can apply to all the data types.
 * one column only use the single one compression method.
 * compressed tuples will be placed into one page with compression meta data.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PAGE_COMPRESS_H
#define PAGE_COMPRESS_H

#include "c.h"
#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf/bufpage.h"
#include "utils/hsearch.h"

/* Compression Mode
 *
 * CMPR_NUMSTR is a special kind of CMPR_DELTA.
 * chars ('0'-'9') can be handled by delta-compression based on '0'.
 * so that each number would be storaged in 4bits, and compression ratio is 2:1.
 * !!! Note that You must also change ATT_CMPR_UNDEFINED and others in parsenode.h
 * Description: we should refine code to remove these redundancy macro
 */
#define CMPR_DELTA 1
#define CMPR_DICT 2
#define CMPR_PREFIX 3
#define CMPR_NUMSTR 4
#define CMPR_NONE 0
#define CMPR_UNDEF 0x7F

typedef struct DictItemData {
    char* itemData;
    int16 itemSize;
} DictItemData;

struct ColArray {
    Datum* val;
    bool* nulls;
    int num;
};

struct DeltaCmprMeta {
    Datum MinVal;
    Datum MaxVal;
    int bytes;
};

class DictCmprMeta {
public:
    typedef struct {
        DictItemData item;
        unsigned char id;
    } ItemEntry;

    void BuildHashTbl(MemoryContext memCxt);
    unsigned char GetDictItemId(DictItemData* key, bool& found);
    void Destroy();
    void GetDictItems(char* buf) const;

    int dictItemNum;

    /* MAX dictionary item number <= MAX_DICITEMID */
    DictItemData dictItems[256];
    int totalBytes;

    HTAB* m_hash;
};

struct PrefixCmprMeta {
    char* prefixStr;
    int len;
};

typedef union CmprMetaUnion {
    DeltaCmprMeta deltaInfo;
    DictCmprMeta dicInfo;
    PrefixCmprMeta prefixInfo;
} CmprMetaUnion;

typedef int (*CompareFuncPtr)(Datum, Datum);
typedef int64 (*MinusFuncPtr)(Datum, Datum);
typedef Datum (*PlusFuncPtr)(Datum, int64);

typedef struct {
    Oid typeOid;
    CompareFuncPtr compare;
    MinusFuncPtr minus;
    PlusFuncPtr plus;
} NumberTypeOpt;

typedef struct {
    DictItemData key;
    int16 hitCount;
    bool beChoose;
} DictItemEntry;

/*
 * PageCompress
 * Compress one page using multiple compression method.
 * 1. Dictonary compression.
 * 2. Delta value encoding.
 * 3. Prefix compression.
 */
class PageCompress : public BaseObject {
public:
    PageCompress(Relation rel, MemoryContext memCtx);
    virtual ~PageCompress();

    /* compress api */
    void SetBatchTuples(HeapTuple* tups, int ntups, bool last);
    bool CompressOnePage(void);

    HeapTuple* GetOutputTups(void)
    {
        return m_outputTups;
    }
    int GetOutputCount(void)
    {
        return m_outputTupsNum;
    }
    const char* GetCmprHeaderData(void)
    {
        return m_cmprHeaderData;
    }
    int GetCmprHeaderSize(void)
    {
        return m_cmprHeaderSize;
    }
    int Remains(void)
    {
        return (m_inTupsNum - m_current);
    }
    const MemoryContext SelfMemCnxt(void)
    {
        return m_pageMemCnxt;
    }
    void BackWrite(void);
    void ForwardWrite(void);

    /* decompress api */
    static void* FetchAttrCmprMeta(char* metaStart, const int attrRawLen, int* metaSize, char* mode);
    static int GetAttrCmprValSize(char mode, int attlen, void* metaInfo, const char* attrStart);
    static bool NeedExternalBuf(char mode);
    static int UncompressOneAttr(char mode, char attalign, int attlen, void* metaInfo, char* attrStart,
        int* attrCmprsValSize, char* uncmprValBuf);
    static Datum UncompressOneAttr(
        char mode, void* metaInfo, Oid typeOid, int attlen, char* attrStart, int* attrCmprsValSize);

private:
    /* compress methods */
    void CompressOneAttr(Datum val, bool null, FormCmprTupleData* formTuple, int col);
    ColArray* DeformTups(HeapTuple* tups, int ntups);

    /* Choose compression method for each column */
    void ChooseMethodForFixedLenType(ColArray* colArray, int col);
    void ChooseMethodForVaryLenType(ColArray* colArray, int col);
    void ChooseCmprMethod(ColArray* colArray);
    bool SetCmprMethod(ColArray* colArray, int col, int mode);

    /* delta compress methods */
    DeltaCmprMeta* DeltaGetCmprMeta(ColArray* colArray, const NumberTypeOpt* opt) const;
    bool DeltaCompress(DeltaCmprMeta* deltaInfo, const NumberTypeOpt* opt, Datum val, unsigned char* deltaBuf) const;
    static Datum DeltaUncompress(DeltaCmprMeta* delta, const NumberTypeOpt* opt, unsigned char* start);

    /* prefix compress methods */
    template <int attlen>
    int PrefixLen(Datum str1, Datum str2);
    template <int attlen>
    char* PrefixGetCmprMeta(ColArray* colArray, int& prefixLen);
    template <int attlen>
    bool PrefixCompress(PrefixCmprMeta* prefix, Datum val, Datum& cmprsVal, int& cmprsValSize);
    static Datum PrefixUncompress(int attlen, PrefixCmprMeta* prefix, char* start, int* valSize,
        char* uncmprValBuf = NULL, int* dataLenWithPadding = NULL, char attalign = 'c');

    /* dictionary compress methods */
    template <int attFlag>
    void FillHashKey(DictItemData& key, const int& attlen, const Datum& attval);
    template <int attFlag>
    DictCmprMeta* DictGetCmprMeta(ColArray* colArray, int col, int attlen);
    bool DictCompress(int col, Datum val, unsigned char& dictItemId);
    static Datum DictUncompress(DictCmprMeta* metaInfo, const char* compressBuf, int attlen);

    /* number string compress mothods */
    template <int attlen>
    bool NumstrEvaluate(ColArray* colArray);
    template <int attlen>
    static Datum NumstrCompress(Datum src, int& outSize);
    static Datum NumstrUncompress(Datum src, int attlen, int& cmprValSize, char* uncmprValBuf = NULL,
        int* dataLenWithPadding = NULL, char attalign = 'c');

    /* write compression meta data and set its size*/
    void SetCmprHeaderData(void);

    /* dispatch tuples during compression */
    bool Dispatch(HeapTuple raw, HeapTuple cmpr, int& nowSize, const int& blksize);
    bool Dispatch(HeapTuple raw, int& nowSize, const int& blksize);
    void OutputRawTupsBeforeCmpr(int nToasts);
    void OutputRawTupsAfterCmpr(int nToasts);
    void OutputCmprTups(int nToasts);
    void OutputToasts(void);

    /* memory context */
    void ResetPerPage(void);
    void ResetCmprMeta(void);

#ifdef USE_ASSERT_CHECKING
    void CheckCmprDatum(Datum arg1, Datum arg2, Form_pg_attribute attr);
    void CheckCmprAttr(Datum rawVal, bool rawNull, int col, FormCmprTupleData* formTuple);
    void CheckCmprTuple(HeapTuple rawTup, HeapTuple cmprTup);
    void CheckCmprHeaderData(void);
    void CheckOutputTups(HeapTuple* cmpr, int nCmpr, HeapTuple* uncmpr, int nUncmpr, int nToasts);
#endif

#ifdef TRACE_COMPRESS
    void TraceCmprPage(
        int flag, HeapTuple* cmpr, HeapTuple* mapped, int nCmpr, HeapTuple* uncmpr, int nUncmpr, int nToasts);
    int nLoops;
#endif

    /* Output compressed tuples */
    char* m_cmprHeaderData;
    HeapTuple* m_outputTups;
    int m_outputTupsNum;
    int m_cmprHeaderSize;

    /*
     * input original tuples
     * if not the last batch, maybe wait for continuing to read more tuples.
     * but for the last, all tuples must be handled and written into pages.
     */
    Relation m_rel;
    HeapTuple* m_inTups;
    int m_inTupsNum;
    int m_current;
    bool m_last;

    Datum* m_deformVals;
    bool* m_deformNulls;

    /* compression mode && meta info && number type func */
    char* m_cmprMode;
    void** m_cmprMeta;

    /*
     * Each element represents for miss times when try to
     * choose dictionary compression method for each column
     */
    uint32* m_dictMiss;
    unsigned char* m_dictItemId;

    const NumberTypeOpt** m_numTypeOpt;

    /* local memory context */
    MemoryContext m_pageMemCnxt;
    MemoryContext m_parentMemCxt;

    /* handle toast tuples */
    HeapTuple* m_toastTups;
    HeapTuple* m_mappedTups;
    HeapTuple* m_cmprTups;
    int m_toastTupsNum;
    int m_mappedTupsNum;
    int m_cmprTupsNum;
    int m_uncmprTupIdx;
    bool m_nextBatch;
};

/* --------------------------------
 *		Page Compression Macros
 * --------------------------------
 */

#define getPageDict(page) (AssertMacro(PageIsCompressed(page)), (PageGetSpecialPointer(page)))

#endif /* PAGE_COMPRESS_H */
