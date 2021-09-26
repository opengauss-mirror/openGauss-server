/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * hll.h
 *    Realize the basic functions of HLL algorithm, which are called by hll_function and hll_mpp.
 *
 * The original HyperLogLog algorithm was proposed by Flajolet P[1], which were modified by Google engineers[2].
 * Redis HLL[3] set sparse and dense mode to save hll object, in order to save memory; while Postgres HLL is much less
 * complex. We get a balance from them and privide our new HLL design. Moreover, the cardinality estimation approach
 * used in our HLL is derived from Otmar Ertl[4].
 * [1] Flajolet P, Fusy É, Gandouet O, et al. Hyperloglog: the analysis of a near-optimal cardinality estimation
 * algorithm[C]. 2007.
 * [2] Heule S, Nunkesser M, Hall A. HyperLogLog in practice: algorithmic engineering of a state of the art cardinality
 * estimation algorithm[C]//Proceedings of the 16th International Conference on Extending Database Technology.
 * 2013: 683-692.
 * [3] Salvatore Sanlippo. Redis new data structure: The HyperLogLog. http://antirez.com/news/75, 2014.
 * [4] Ertl O. New cardinality estimation algorithms for hyperloglog sketches[J]. arXiv preprint arXiv:1702.01284, 2017.
 *
 * The design of HyperLogLog is as follows:
 * 1. Use MurmurHash3 to get a 64-bit hash value
 * 2. We use the first m bits as an index, and calculate how many continuous zero ocuured in left bits, which is set as
 *    value. That means for each hash value, we get a (key, value), which is also named as register in some papers.
 * 3. We use three type to save (key, value) pairs, which are defined in enum HllType: HLL_EXPLICIT/HLL_SPARSE/HLL_FULL
 *    a. In HLL_EXPLICIT mode, each 8-byte saves a hashvalue. We make sure the hashvalue saved in HLL_EXPLICIT mode is
 *       in order, that is helpful in hll_add and hll_union function.
 *    b. In HLL_SPARSE mode, we set 4 type to saves values. Since each value ranges from 0 to (64-m), so 6bits is enough
 *       to save a value, but it's better to use 8 bits (1 byte) for locating and calculation. So we use 8 bits to saves
 *       it, the first 2 bits are treated as flag, which is defined in enum RegisterType, the left 6 bits save values.
 *       SPARSE_LONGZERO: 00(count/64-1). Units is 64, used to save zero registers. For example, 00000111 means there
 *                        are (7+1)*64 zero registers.
 *       SPARSE_ZERO: 01(count-1). Units is 1, used to save zero registers. For example, 01000111 means there are (7+1)
 *                    zero registers.
 *       DENSE_VALUE: 10(value-1), used to save one not-zero register. For example, 10000111 means there are one
 *                    register, which value is 7+1.
 *       SPARSE_VALUE: 11(value-1)(4bit)(count-1)(2bit), used to save several registers. For example, 11000111 means
 *                     there are 4 registers, each value is 2.
 *    c. In HLL_FULL mode, we create an array of length (2^m), and save each value in index key.
 * 4. We use variable-length uint8 array to save hllData, when create a new hll, we set hllData nullptr. We use a
 *    parameter named LenTotal to record the size of hllData, and lenUsed to record how many bytes are used to save
 *    (key, value) pairs. If lenLeft=(LenTotal-lenUsed) is not enough, expand hllData.
 * 5. Cardinality estimation:
 *    case HLL_EMPTY: 0.
 *    case HLL_EXPLICIT: lenUsed/8. Since each 8-byte saves a hashvalue.
 *    other case: a new approach drived in Otmar Ertl's paper.
 *
 * IDENTIFICATION
 * src/include/utils/hll.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HLL_H
#define HLL_H

#include "fmgr.h"
#include <float.h>

#ifndef DBL_EPSILON
#define DBL_EPSILON 2.2204460492503131e-16
#endif

/* we use 3 types to save hllObject: HLL_EXPLICIT, HLL_SPARSE, HLL_FULL */
enum HllType {
    HLL_UNINIT = 0, /* uninit hllObject */
    HLL_EMPTY,      /* an epmty hllObject */
    HLL_EXPLICIT,   /* explicit mode */
    HLL_SPARSE,     /* sparse mode */
    HLL_FULL,       /* full mode */
    HLL_UNDEFINED   /* invalid type */
};
const char hllTypename[6][15] = {"HLL_UNINIT", "HLL_EMPTY", "HLL_EXPLICIT", "HLL_SPARSE", "HLL_FULL", "HLL_UNDEFINED"};

/* in sparse mode, we use 4 types to set register byte, the former 2-bit set type, the later 6-bit set value */
/* the difference between SPARSE_LONGZERO and SPARSE_ZERO is that the unit of the former is 1 << HLL_LONGZERO_UNIT */
enum RegisterType {
    SPARSE_LONGZERO = 0, /* 00count(6bit), 64*(count+1) indicates the length of zero counts, max is 4096 */
    SPARSE_ZERO,         /* 01count(6bit), count+1 indicates the length of zero counts, max is 64 */
    DENSE_VALUE,         /* 11value(6bit), value+1 indicates the register value */
    SPARSE_VALUE,        /* 10value(4bit)count(2bit), for efficiency type SPARSE_VALUE is not used */
};

#define BYTEVAL_LENGTH(byteval) (VARSIZE(byteval) - VARHDRSZ)
#define BITS_TO_VALUE(bits) ((uint64_t)1 << (bits))
#define BITS_MAX_VALUE(bits) (((uint64_t)1 << (bits)) - 1)

/* default value, can be changed by function HllSetPara */
#define HLL_LOG2_REGISTER 14      /* default register bit is 14, that means we use the former 14-bit to locate */
                                  /* the register, the later 50-bit to calculate continuous_zero */
#define HLL_LOG2_EXPLICIT_SIZE 10 /* default explicit mode size */
#define HLL_LOG2_SPARSE_SIZE 12   /* default sparse mode size */
#define HLL_DUPLICATE_CHECK 0     /* we use int not boolean, in order to check the invalidation of input */
/* default value max */
#define HLL_LOG2_REGISTER_MIN 10
#define HLL_LOG2_EXPLICIT_SIZE_MIN 0
#define HLL_LOG2_SPARSE_SIZE_MIN 0
#define HLL_DUPLICATE_CHECK_MIN 0
/* default value min */
#define HLL_LOG2_REGISTER_MAX 16
#define HLL_LOG2_EXPLICIT_SIZE_MAX 12
#define HLL_LOG2_SPARSE_SIZE_MAX 14
#define HLL_DUPLICATE_CHECK_MAX 1

/* const value */
#define INT8_LEN 8
#define INT64_LEN 64

#define HLL_ALPHA_INF 0.721347520444481703680 /* constant for 0.5/ln(2) */
#define HLL_EXPAND_SIZE 256                   /* if current size is not enough, expand it each time */
#define HLL_REGISTER_BITS 6      /* continuous_zero max is 64 - HLL_LOG2_REGISTER < 2^6, so 6bit is enough */
#define HLL_EXPLICIT_UINT 8      /* the explicit mode unit is 8, while other mode is 1 */
#define HLL_LONGZERO_UNIT 64     /* the unit of SPARSE_LONGZERO type */
#define HLL_LOG2_LONGZERO_UNIT 6 /* 2^6=64 */
#define HLL_LONGZERO_FULL 0x3F   /* 00111111 */
#define HLL_ZERO_FULL 0x7F       /* 01111111 */

/* get byte/type in sparse mode */
#define BYTE_TYPE(byte) (RegisterType)((byte) >> HLL_REGISTER_BITS)
#define TYPE_BYTE(type) ((RegisterType)(type) << HLL_REGISTER_BITS)
/* set byte in sparse mode */
#define SET_LONGZERO_VALUE(value) ((((value) >> HLL_LOG2_LONGZERO_UNIT) - 1) | TYPE_BYTE(SPARSE_LONGZERO))
#define SET_ZERO_VALUE(value) (((value)-1) | TYPE_BYTE(SPARSE_ZERO))
#define SET_DENSE_VALUE(value) (((value)-1) | TYPE_BYTE(DENSE_VALUE))
/* in sparse mode, the value saved in a hll byte can never be zero, so if the value is a, we save a - 1 in the byte */
#define BYTE_GET_VALUE(byte) (((byte)&0x3F) + 1)

#define HLL_REGISTER_MAX_VALUE BITS_MAX_VALUE(HLL_REGISTER_BITS) /* 00111111 */

uint64_t HllHash(const void *key, const int len, const int seed);

/* hllpara is set by guc */
struct HllPara {
    uint8_t log2Registers;    /* number of registers, default 14, range 10~16 */
    uint8_t log2Explicitsize; /* size of explicit mode, default 8, range 0~10 */
    uint8_t log2Sparsesize;   /* size of sparse mode, default 12, range 0~14 */
    uint8_t duplicateCheck;   /* whether check duplicate, default true */
};
/* some function for struct HllPara */
void HllCheckPararange(const int32_t log2Registers, const int32_t log2Explicitsize, const int32_t log2Sparsesize,
    const int32_t duplicateCheck); /* must execute HllCheckPararange for HllSetPara */
void HllCheckParaequal(const HllPara hllpara1, const HllPara hllpara2);
void HllCopyPara(HllPara *hllpara1, const HllPara *hllpara2);
void HllParaSetDefault(HllPara *hllpara);
int32_t HllParaPack(const HllPara hllpara);           /* pack HllPara to typmod */
void HllParaUnpack(int32_t typmod, HllPara *hllpara); /* unpack HllPara from typmod */

/*
 * HllObjectData saves all the hll data
 * we use uint8_t* to save hllData, lenUsed is the length of used hlldata, lenLeft=(LenTotal-lenUsed) is the length of
 * left space. if lenLeft is not enouth, expand HLL_EXPAND_SIZE
 */
#define HLL_FLAG_LEN 3   /* length of 'HLL' */
#define HLL_HEAD_SIZE 27 /* size of HllObjectData except *hllData, don't think of alignment */
#define HLL_PARA_BIT 4   /* we only use 4 bit to save every hll para if compress */
#define HLL_LEN_BIT 17   /* we only use 17 bit to save lenTotal and lenUsed if compress */
struct HllObjectData {
    char hllHead[HLL_FLAG_LEN]; /* "HLL" */
    HllType hlltype; /* range 0~5, so if pack it, we set one byte to save it */
    HllPara hllpara;

    uint32_t lenTotal; /* total size of hlldata */
    uint32_t lenUsed; /* number of occupied register, min is 1, max is 2^nRegisterBit */
    uint8_t *hllData;

    double NDVEstimator; /* we use DVEstimator to save NDV, if hllObject changed, DVEstimator will set to -1 */
    uint64_t lastHash; /* used to save the last hash value, it's helpful if from an in-order dataset */
};
typedef HllObjectData *HllObject;
void HllObjectPack(const HllObject hllobject, uint8_t *hllbyte, const size_t hllsize); /* pack HllObject to hllbyte */
void HllObjectUnpack(HllObject hllobject, const uint8_t *hllbyte, const size_t hllsize,
    MemoryContext cxt); /* unpack HllObject from hllbyte */

class Hll : public BaseObject {
public:

    Hll(MemoryContext context);
    virtual ~Hll();
    void HllInit();
    void HllFree();

    void HllEmpty();
    void HllSetPara(const HllPara hllpara);
    void HllAdd(const uint64_t hashval);
    void HllUnion(Hll aHll);
    double HllCardinality();
    void HllObjectPack(uint8_t *hllbyte, const size_t hllsize);
    void HllObjectUnpack(const uint8_t *hllbyte, const size_t hllsize);

    /* since all parameters are private, we need some function to get the value */
    char *HllPrint(); /* print hll info */
    HllType HllGetType();
    uint8_t HllRegisters();
    uint8_t HllExplicitsize();
    uint8_t HllSparsesize();
    uint8_t HllDuplicateCheck();
    uint32_t HllDatalen();
    HllPara *HllGetPara();

private:
    HllObject hllObject;
    MemoryContext cxt;

    /* some basic function for hll */
    uint8_t HllGetByte(const uint32_t index);                   /* get hlldata[index] */
    void HllSetByte(const uint32_t index, const uint8_t value); /* set hlldata[index] = value */
    void HllDataExpand();                                       /* expand hlldata's size */
    void HllCopy(const Hll aHll);                               /* copy aHll to Hll */
    void HllUpdateValue(const uint32_t index, const uint8_t *insertValue, const uint32_t insertLen,
        const uint32_t originLen); /* inplace hllData[index]~hllData[index+originLen-1] with insertValue */
    void HllHashToValue(const uint64_t hashval, uint32_t *index, uint8_t *value); /* get (index, value) from hashval */

    /* set register value, used in function HllAdd */
    void HllSetRegisterValue(const uint64_t hashval); /* update register's value */
    /* the next functions is subfunction */
    void HllSetEmptyValue(const uint64_t hashval);
    void HllSetExplicitValue(const uint64_t hashval);
    void HllSetSparseValue(const uint64_t hashval);
    void HllSetFullValue(const uint64_t hashval);

    /* transform function */
    void HllTransformFull();    /* transform into full mode */
    void HllTransformSparse();  /* transform into sparse mode */
    void HllEmptyToSparse();    /* transform from empty mode into sparse mode */
    void HllExplicitToSparse(); /* transform from explicit mode into sparse mode */
    uint32_t HllSetSparseZero(uint8_t *hlldata, const uint32_t zerocount); /* set zero value to hlldata */

    /* explicit function */
    uint64_t HllGetExplictitValue(const uint32_t index);
    void HllExplicitCheck(); /* check whether explicit mode is ok */

    /* sparse function */
    void HllGetRegisterUnit(const uint32_t registerIndex, uint32_t *byteIndex, uint32_t *unitIndex);
    void HllUpdateSparseValue(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue);
    void HllUpdateSparseType1(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue);
    void HllUpdateSparseType2(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue);
    void HllUpdateSparseType3(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue);
    void HllSetSparseByte(uint8_t *destByte, const uint32_t count, const uint8_t value);

    /* hll union function */
    void HllCombineFull(Hll aHll);
    void HllComibineExplicit(Hll aHll);

    /* hll ndv function, used in function HllCardinality */
    void HllCalculateNDV();
    void HllGetHistogram(uint32_t *histogram);
    double HllGetTau(double x);
    double HllGetSigma(double x);
};

#endif
