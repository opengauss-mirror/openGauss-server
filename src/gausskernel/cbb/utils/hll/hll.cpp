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
 * hll.cpp
 *    Realize the basic functions of HLL algorithm, which are called by hll_function and hll_mpp.
 *
 * IDENTIFICATION
 * src/gausskernel/cbb/utils/hll/hll.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <postgres.h>
#include <math.h>

#include "miscadmin.h"
#include "utils/bloom_filter.h"
#include "utils/hll.h"

static filter::Murmur3 murmur3;
/* hash function, must make sure the input_para is valid */
uint64_t HllHash(const void *key, const int len, const int seed)
{
    Assert(key != NULL);
    if (seed < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("negative seed value is invalid")));
    }

    return (uint64_t)murmur3.hash64((const char *)key, len, seed);
}

/* check the object HllPara's range is ok */
void HllCheckPararange(const int32_t log2Registers, const int32_t log2Explicitsize, const int32_t log2Sparsesize,
    const int32_t duplicateCheck)
{
    if (log2Registers < HLL_LOG2_REGISTER_MIN || log2Registers > HLL_LOG2_REGISTER_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("log2m = %d is out of range, it should be in range %d to %d, or set -1 as default",
                log2Registers, HLL_LOG2_REGISTER_MIN, HLL_LOG2_REGISTER_MAX)));
    }
    if (log2Explicitsize < HLL_LOG2_EXPLICIT_SIZE_MIN || log2Explicitsize > HLL_LOG2_EXPLICIT_SIZE_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("log2explicit = %d is out of range, it should be in range %d to %d, or set -1 as default",
                log2Explicitsize, HLL_LOG2_EXPLICIT_SIZE_MIN, HLL_LOG2_EXPLICIT_SIZE_MAX)));
    }
    if (log2Sparsesize < HLL_LOG2_SPARSE_SIZE_MIN || log2Sparsesize > HLL_LOG2_SPARSE_SIZE_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("log2sparse = %d is out of range, it should be in range %d to %d, or set -1 as default",
                log2Sparsesize, HLL_LOG2_SPARSE_SIZE_MIN, HLL_LOG2_SPARSE_SIZE_MAX)));
    }
    if (duplicateCheck < HLL_DUPLICATE_CHECK_MIN || duplicateCheck > HLL_DUPLICATE_CHECK_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("duplicatecheck = %d is out of range, it should be in range %d to %d, or set -1 as default",
                duplicateCheck, HLL_DUPLICATE_CHECK_MIN, HLL_DUPLICATE_CHECK_MAX)));
    }
}

/* check the object HllPara is equal, for some operation, such as HllUnion, HllPara must be same */
void HllCheckParaequal(const HllPara hllpara1, const HllPara hllpara2)
{
    if (hllpara1.log2Registers != hllpara2.log2Registers) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("log2m does not match: source is %d and dest is %d",
            hllpara1.log2Registers, hllpara2.log2Registers)));
    }
    if (hllpara1.log2Explicitsize != hllpara2.log2Explicitsize) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("log2explicit does not match: source is %d and dest is %d",
                hllpara1.log2Explicitsize, hllpara2.log2Explicitsize)));
    }
    if (hllpara1.log2Sparsesize != hllpara2.log2Sparsesize) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("log2sparse does not match: source is %d and dest is %d",
                hllpara1.log2Sparsesize, hllpara2.log2Sparsesize)));
    }
    if (hllpara1.duplicateCheck != hllpara2.duplicateCheck) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("duplicatecheck does not match: source is %d and dest is %d",
                hllpara1.duplicateCheck, hllpara2.duplicateCheck)));
    }
}

/* copy the object HllPara */
void HllCopyPara(HllPara *hllpara1, const HllPara *hllpara2)
{
    *hllpara1 = *hllpara2;
}

/* set the object HllPara as default value */
void HllParaSetDefault(HllPara *hllpara)
{
    Assert(hllpara != NULL);

    hllpara->log2Registers = HLL_LOG2_REGISTER;
    hllpara->log2Explicitsize = HLL_LOG2_EXPLICIT_SIZE;
    hllpara->log2Sparsesize = HLL_LOG2_SPARSE_SIZE;
    hllpara->duplicateCheck = HLL_DUPLICATE_CHECK;
}

/*
 * pack hllpara to 32bit, we don't check the input para here, must be careful
 * should run HllCheckPararange before
 */
int32_t HllParaPack(const HllPara hllpara)
{
    return *(int32_t *)&hllpara;
}

/* unpack hllpara from 32bit */
void HllParaUnpack(int32_t typmod, HllPara *hllpara)
{
    *hllpara = *(HllPara *)&typmod;
}

/*
 * pack a hllobject to uint8 stream
 * hllsize is the length of hllbyte, hllsize = HLL_HEAD_SIZE + hllobject->lenUsed + 1
 */
void HllObjectPack(const HllObject hllobject, uint8_t *hllbyte, const size_t hllsize)
{
    Assert(hllobject != NULL);
    Assert(hllbyte != NULL);
    if (hllsize <= (size_t)(HLL_HEAD_SIZE + hllobject->lenUsed)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("hllsize = %zu is smaller than hllobjectsize=%d", hllsize, HLL_HEAD_SIZE + hllobject->lenUsed)));
    }
    if (hllobject->hlltype < HLL_EMPTY || hllobject->hlltype > HLL_FULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hll type, type=%d", hllobject->hlltype)));
    }

    errno_t rc = memset_s(hllbyte, hllsize, '\0', hllsize);
    securec_check(rc, "\0", "\0");

    int i = 0;
    /* hllHead = "HLL" */
    rc = memcpy_s(hllbyte + i, sizeof(uint8_t) * HLL_FLAG_LEN, hllobject->hllHead, sizeof(uint8_t) * HLL_FLAG_LEN);
    securec_check(rc, "\0", "\0");
    i += HLL_FLAG_LEN;

    /* compress hllinfo into compressinfo, must keep consistent with HllObjectUnpack */
    uint64_t compressinfo = 0;
    compressinfo |= (uint64_t)hllobject->hlltype; /* range 0~4, 4bit */
    compressinfo <<= HLL_PARA_BIT;
    compressinfo |= (hllobject->hllpara.log2Registers - HLL_LOG2_REGISTER_MIN); /* range 10~16, 4bit */
    compressinfo <<= HLL_PARA_BIT;
    compressinfo |= hllobject->hllpara.log2Explicitsize; /* range 0~12, 4bit */
    compressinfo <<= HLL_PARA_BIT;
    compressinfo |= hllobject->hllpara.log2Sparsesize; /* range 0~14, 4bit */
    compressinfo <<= HLL_PARA_BIT;
    compressinfo |= hllobject->hllpara.duplicateCheck; /* range 0~1, 4bit */
    compressinfo <<= HLL_LEN_BIT;
    compressinfo |= hllobject->lenTotal; /* range 0~2^16, 17bit */
    compressinfo <<= HLL_LEN_BIT;
    compressinfo |= hllobject->lenUsed; /* range 0~2^16, 17bit */

    rc = memcpy_s(hllbyte + i, sizeof(uint64_t), &compressinfo, sizeof(uint64_t));
    securec_check(rc, "\0", "\0");
    i += sizeof(uint64_t);
    rc = memcpy_s(hllbyte + i, sizeof(double), &hllobject->NDVEstimator, sizeof(double));
    securec_check(rc, "\0", "\0");
    i += sizeof(double);
    rc = memcpy_s(hllbyte + i, sizeof(uint64_t), &hllobject->lastHash, sizeof(uint64_t));
    securec_check(rc, "\0", "\0");
    i += sizeof(uint64_t);

    if (hllobject->lenUsed > 0) {
        rc = memcpy_s(hllbyte + HLL_HEAD_SIZE, hllobject->lenUsed, hllobject->hllData, hllobject->lenUsed);
        securec_check(rc, "\0", "\0");
    }
}

/* unpack a hllobject from uint8 stream, hllsize is the length of hllbyte */
void HllObjectUnpack(HllObject hllobject, const uint8_t *hllbyte, const size_t hllsize, MemoryContext cxt)
{
    Assert(hllobject != NULL);
    Assert(hllbyte != NULL);

    if (hllsize < HLL_HEAD_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("not a hll type, size=%zu is not enough", hllsize)));
    }

    int i = 0;
    errno_t rc;
    /* check hll head, hllHead = "HLL" */
    rc = memcpy_s(hllobject->hllHead, HLL_FLAG_LEN, hllbyte + i, HLL_FLAG_LEN);
    securec_check(rc, "\0", "\0");
    i += HLL_FLAG_LEN;
    if (strncmp(hllobject->hllHead, "HLL", HLL_FLAG_LEN) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("not a hll type, head is not match")));
    }

    uint64_t compressinfo = 0;
    rc = memcpy_s(&compressinfo, sizeof(uint64_t), hllbyte + i, sizeof(uint64_t));
    securec_check(rc, "\0", "\0");
    i += sizeof(uint64_t);

    /* uncompress hllinfo from compressinfo, must keep consistent with HllObjectPack */
    hllobject->lenUsed = compressinfo & BITS_MAX_VALUE(HLL_LEN_BIT);
    compressinfo >>= HLL_LEN_BIT;
    hllobject->lenTotal = compressinfo & BITS_MAX_VALUE(HLL_LEN_BIT);
    compressinfo >>= HLL_LEN_BIT;
    hllobject->hllpara.duplicateCheck = compressinfo & BITS_MAX_VALUE(HLL_PARA_BIT);
    compressinfo >>= HLL_PARA_BIT;
    hllobject->hllpara.log2Sparsesize = compressinfo & BITS_MAX_VALUE(HLL_PARA_BIT);
    compressinfo >>= HLL_PARA_BIT;
    hllobject->hllpara.log2Explicitsize = compressinfo & BITS_MAX_VALUE(HLL_PARA_BIT);
    compressinfo >>= HLL_PARA_BIT;
    hllobject->hllpara.log2Registers = compressinfo & BITS_MAX_VALUE(HLL_PARA_BIT);
    hllobject->hllpara.log2Registers += HLL_LOG2_REGISTER_MIN;
    compressinfo >>= HLL_PARA_BIT;
    hllobject->hlltype = (HllType)(compressinfo & BITS_MAX_VALUE(HLL_PARA_BIT));

    /* check hll type */
    if (hllobject->hlltype > HLL_FULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hll type, type=%d", hllobject->hlltype)));
    }

    rc = memcpy_s(&hllobject->NDVEstimator, sizeof(double), hllbyte + i, sizeof(double));
    securec_check(rc, "\0", "\0");
    i += sizeof(double);
    rc = memcpy_s(&hllobject->lastHash, sizeof(uint64_t), hllbyte + i, sizeof(uint64_t));
    securec_check(rc, "\0", "\0");
    i += sizeof(uint64_t);

    /* check hllsize is enough */
    if (hllsize <= (size_t)(HLL_HEAD_SIZE + hllobject->lenUsed)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("hllsize = %zu is smaller than hllobjectsize=%d", hllsize, HLL_HEAD_SIZE + hllobject->lenUsed)));
    }

    if (hllobject->lenUsed > 0) {
        MemoryContext old = MemoryContextSwitchTo(cxt);
        hllobject->hllData = (uint8_t *)palloc0((hllobject->lenTotal) * sizeof(uint8_t));
        (void)MemoryContextSwitchTo(old);

        rc = memcpy_s(hllobject->hllData, hllobject->lenUsed, hllbyte + HLL_HEAD_SIZE, hllobject->lenUsed);
        securec_check(rc, "\0", "\0");
    }
}

Hll::Hll(MemoryContext context) : cxt(context) {}

Hll::~Hll() {}

/* init a hll, execute it after create a new class hll */
void Hll::HllInit()
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    hllObject = (HllObjectData *)palloc0(sizeof(HllObjectData));
    (void)MemoryContextSwitchTo(old);

    /* hllHead = "HLL" */
    errno_t rc = memcpy_s(hllObject->hllHead, HLL_FLAG_LEN, "HLL", HLL_FLAG_LEN);
    securec_check(rc, "", "");

    hllObject->hlltype = HLL_UNINIT;
    HllParaSetDefault(&hllObject->hllpara);
    hllObject->NDVEstimator = -1;
    hllObject->lastHash = 0;
    hllObject->hllData = NULL;
    hllObject->lenTotal = 0;
    hllObject->lenUsed = 0;
}

/* free a hll, execute it before delere the class hll */
void Hll::HllFree()
{
    pfree_ext(hllObject->hllData);
    pfree_ext(hllObject);
}

/* set a hll to empty */
void Hll::HllEmpty()
{
    hllObject->hlltype = HLL_EMPTY;
    hllObject->NDVEstimator = 0;
    hllObject->lastHash = 0;

    pfree_ext(hllObject->hllData);
    hllObject->lenTotal = 0;
    hllObject->lenUsed = 0;
}

/* set hll->llObject.hllpara = hllpara */
void Hll::HllSetPara(const HllPara hllpara)
{
    if (hllObject->hlltype > HLL_EMPTY) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("only can set parameters on an uninit/empty hll object")));
    }

    HllCopyPara(&hllObject->hllpara, &hllpara);
}

/* Add hashval to hllObject */
void Hll::HllAdd(const uint64_t hashval)
{
    if (hllObject->hllpara.duplicateCheck && hashval == hllObject->lastHash) {
        return;
    }

    HllSetRegisterValue(hashval);
    hllObject->lastHash = hashval;
}

/* combine two hll to one */
void Hll::HllUnion(Hll aHll)
{
    /* if aHll is HLL_UNINIT or HLL_EMPTY, return hll itself; if hll itself is HLL_UNINIT or HLL_EMPTY, return aHll */
    if (aHll.hllObject->hlltype <= HLL_EMPTY) {
        return;
    }
    if (hllObject->hlltype <= HLL_EMPTY) {
        return HllCopy(aHll);
    }

    HllCheckParaequal(hllObject->hllpara, aHll.hllObject->hllpara);

    /* if both explicit, combine into explicit, else into full */
    if (aHll.hllObject->hlltype == HLL_EXPLICIT && hllObject->hlltype == HLL_EXPLICIT) {
        HllComibineExplicit(aHll);
    } else {
        /* before combine, we transform them into fullmode */
        HllTransformFull();
        aHll.HllTransformFull();
        HllCombineFull(aHll);
    }
}

/* calculate ndv of hll */
double Hll::HllCardinality()
{
    /* check if need refresh NDVEstimator */
    if (hllObject->NDVEstimator == -1) {
        HllCalculateNDV();
    }

    return hllObject->NDVEstimator;
}

void Hll::HllObjectPack(uint8_t *hllbyte, const size_t hllsize)
{
    ::HllObjectPack(hllObject, hllbyte, hllsize);
}

void Hll::HllObjectUnpack(const uint8_t *hllbyte, const size_t hllsize)
{
    ::HllObjectUnpack(hllObject, hllbyte, hllsize, cxt);
}

/* print hll infomation to a string */
char *Hll::HllPrint()
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    size_t len = 1024;
    char *hllinfo = (char *)palloc0(len);
    (void)MemoryContextSwitchTo(old);

    errno_t rc = memset_s(hllinfo, len, '\0', len);
    securec_check(rc, "\0", "\0");

    switch (hllObject->hlltype) {
        case HLL_UNINIT:
        case HLL_EMPTY:
        case HLL_EXPLICIT:
        case HLL_SPARSE:
        case HLL_FULL: {
            rc = snprintf_s(hllinfo, len, len - 1,
                "type=%d(%s), log2m=%d, log2explicit=%d, log2sparse=%d, duplicatecheck=%d",
                hllObject->hlltype, hllTypename[hllObject->hlltype], hllObject->hllpara.log2Registers,
                hllObject->hllpara.log2Explicitsize, hllObject->hllpara.log2Sparsesize,
                hllObject->hllpara.duplicateCheck);
            securec_check_ss(rc, "\0", "\0");
            break;
        }
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hll type")));
    }

    return hllinfo;
}

/* performance function, get a hll parameter */
HllType Hll::HllGetType()
{
    return hllObject->hlltype;
}

/* performance function, get a hll parameter */
uint8_t Hll::HllRegisters()
{
    return hllObject->hllpara.log2Registers;
}

/* performance function, get a hll parameter */
uint8_t Hll::HllExplicitsize()
{
    return hllObject->hllpara.log2Explicitsize;
}

/* performance function, get a hll parameter */
uint8_t Hll::HllSparsesize()
{
    return hllObject->hllpara.log2Sparsesize;
}

/* performance function, get a hll parameter */
uint8_t Hll::HllDuplicateCheck()
{
    return hllObject->hllpara.duplicateCheck;
}

/* performance function, get a hll parameter */
uint32_t Hll::HllDatalen()
{
    return hllObject->lenUsed;
}

/* performance function, get a hll parameter */
HllPara *Hll::HllGetPara()
{
    return &hllObject->hllpara;
}

/* get hlldata[index] */
uint8_t Hll::HllGetByte(const uint32_t index)
{
    if (index >= hllObject->lenUsed) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("hll index is out of range, current_size=%d", hllObject->lenUsed)));
    }

    return hllObject->hllData[index];
}

/* set hlldata[index] = value */
void Hll::HllSetByte(const uint32_t index, const uint8_t value)
{
    if (index >= hllObject->lenUsed) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("hll index is out of range, current_size=%d", hllObject->lenUsed)));
    }

    hllObject->hllData[index] = value;
}

/* expand hlldata's size */
void Hll::HllDataExpand()
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    if (hllObject->lenTotal == 0) {
        Assert(hllObject->hllData == NULL);
        hllObject->hllData = (uint8_t *)palloc0(HLL_EXPAND_SIZE * sizeof(uint8_t));
    } else {
        Assert(hllObject->hllData != NULL);
        uint32_t newSize = hllObject->lenTotal + HLL_EXPAND_SIZE;
        hllObject->hllData = (uint8_t *)repalloc(hllObject->hllData, newSize * sizeof(uint8_t));
    }
    (void)MemoryContextSwitchTo(old);

    hllObject->lenTotal += HLL_EXPAND_SIZE;
    return;
}

/* copy aHll to Hll */
void Hll::HllCopy(const Hll aHll)
{
    /* first copy hlldata */
    switch (aHll.hllObject->hlltype) {
        case HLL_EMPTY: {
            HllCopyPara(&hllObject->hllpara, &aHll.hllObject->hllpara);
            HllEmpty();
            return;
        }
        case HLL_EXPLICIT:
        case HLL_SPARSE:
        case HLL_FULL: {
            if (hllObject->lenTotal < aHll.hllObject->lenTotal) {
                /* if current size is not enough, remalloc it */
                pfree_ext(hllObject->hllData);
                MemoryContext old = MemoryContextSwitchTo(cxt);
                hllObject->hllData = (uint8_t *)palloc0(aHll.hllObject->lenTotal * sizeof(uint8_t));
                (void)MemoryContextSwitchTo(old);

                hllObject->lenTotal = aHll.hllObject->lenTotal;
                hllObject->lenUsed = aHll.hllObject->lenUsed;
            } else {
                hllObject->lenUsed = aHll.hllObject->lenUsed;
            }
            /* copy hll info */
            errno_t rc = memcpy_s(hllObject->hllData, hllObject->lenUsed, aHll.hllObject->hllData, hllObject->lenUsed);
            securec_check(rc, "", "");
            break;
        }
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hll type")));
    }

    HllCopyPara(&hllObject->hllpara, &aHll.hllObject->hllpara);
    hllObject->hlltype = aHll.hllObject->hlltype;
    hllObject->NDVEstimator = aHll.hllObject->NDVEstimator;
    hllObject->lastHash = aHll.hllObject->lastHash;
}

/*
 * update HllObject value of index
 * we simply replace original value of hllData by insert value, but the length may not be equal
 * we will set hllData[index] = insertValue[0], hllData[index + 1] = insertValue[1], ...
 * then hllData[index + insertLen] = hllData[index + originLen], ...
 */
void Hll::HllUpdateValue(const uint32_t index, const uint8_t *insertValue, const uint32_t insertLen,
    const uint32_t originLen)
{
    /* can never be out of range, because we need remove hllData[index]~ hllData[index+originLen-1] */
    if (((originLen == 0) && (index > hllObject->lenUsed)) ||
        ((originLen > 0) && (index + originLen > hllObject->lenUsed))) {
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("hll index is out of range, current_size=%d", hllObject->lenUsed)));
    }

    /* if memory is not enough, expand it */
    while (insertLen - originLen > (hllObject->lenTotal - hllObject->lenUsed)) {
        HllDataExpand();
    }

    errno_t rc;
    /* first we move the hllData to set hllData[index->index + insertLen] empty */
    uint32_t moveLen = hllObject->lenUsed - index - originLen;
    if ((insertLen != originLen) && (moveLen > 0)) {
        rc =
            memmove_s(hllObject->hllData + index + insertLen, moveLen, hllObject->hllData + index + originLen, moveLen);
        securec_check(rc, "", "");
    }
    /* then we copy the insertValue to hllData[index->index + insertLen] */
    rc = memcpy_s(hllObject->hllData + index, insertLen, insertValue, insertLen);
    securec_check(rc, "", "");
    /* last we update the para */
    hllObject->lenUsed += insertLen - originLen;
}

/* get (index, value) from hashval */
void Hll::HllHashToValue(const uint64_t hashval, uint32_t *index, uint8_t *value)
{
    uint8_t calzeroLen = INT64_LEN - hllObject->hllpara.log2Registers;

    /* we get head bits as index, tail bits calculates zero count. In this case, if hashval is ascending, index is
     * ascending too */
    *index = (uint32_t)(hashval >> calzeroLen);
    *value = (uint8_t)(__builtin_ctzll(hashval) + 1);
    if (*value > calzeroLen) {
        *value = 0;
    }
}

/* Set hllObject register index to value */
void Hll::HllSetRegisterValue(const uint64_t hashval)
{
    switch (hllObject->hlltype) {
        case HLL_UNINIT:
            HllEmpty();
        case HLL_EMPTY:
            HllSetEmptyValue(hashval);
            break;
        case HLL_EXPLICIT:
            HllSetExplicitValue(hashval);
            break;
        case HLL_SPARSE:
            HllSetSparseValue(hashval);
            break;
        case HLL_FULL:
            HllSetFullValue(hashval);
            break;
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hll type")));
    }
}

/* Set hllObject register index to value in empty mode */
void Hll::HllSetEmptyValue(const uint64_t hashval)
{
    Assert(hllObject->hlltype == HLL_EMPTY);

    if (hllObject->hllpara.log2Explicitsize == 0 && hllObject->hllpara.log2Sparsesize == 0) {
        HllTransformFull();
        HllSetFullValue(hashval);
    } else if (hllObject->hllpara.log2Explicitsize == 0) {
        HllTransformSparse();
        HllSetSparseValue(hashval);
    } else {
        hllObject->hlltype = HLL_EXPLICIT;
        HllSetExplicitValue(hashval);
    }
}

/* Set hllObject register index to value in explicit mode */
void Hll::HllSetExplicitValue(const uint64_t hashval)
{
    Assert(hllObject->hlltype == HLL_EXPLICIT);

    uint32_t explicitLen = hllObject->lenUsed / HLL_EXPLICIT_UINT;
    uint64_t currentHash;

    if ((hllObject->lenTotal - hllObject->lenUsed) < HLL_EXPLICIT_UINT) {
        HllDataExpand();
    }

    /* 1. find the index to update hlldata, similar to function upper_bound */
    uint32_t start = 0;
    uint32_t last = explicitLen;
    while (start < last) {
        uint32_t mid = (start + last) / 2;
        currentHash = HllGetExplictitValue(mid * HLL_EXPLICIT_UINT);
        if (currentHash <= hashval) {
            start = mid + 1;
        } else {
            last = mid;
        }
    }
    uint32_t bindex = start; /* originvalue[bindex] > hashval, originvalue[bindex-1] <= hashval */

    /* 2. set insert value */
    uint8_t insertValue[HLL_EXPLICIT_UINT];
    errno_t rc = memcpy_s(insertValue, sizeof(uint64_t), &hashval, sizeof(uint64_t));
    securec_check(rc, "\0", "\0");

    /* 3. update hlldata, if value is equal, simply return */
    if ((bindex > 0) && (HllGetExplictitValue((bindex - 1) * HLL_EXPLICIT_UINT) == hashval)) {
        return;
    }

    HllUpdateValue(HLL_EXPLICIT_UINT * bindex, insertValue, HLL_EXPLICIT_UINT, 0);
    /* in explicit mode, simple update ndv */
    hllObject->NDVEstimator++;

    /* 4. check if need transform mode */
    HllExplicitCheck();
}

/* Set hllObject register index to value in sparse mode */
void Hll::HllSetSparseValue(const uint64_t hashval)
{
    Assert(hllObject->hlltype == HLL_SPARSE);

    uint32_t index = 0;
    uint8_t value = 0;
    HllHashToValue(hashval, &index, &value);
    if (value == 0) {
        return;
    }

    /* 1. First, we get registerIndex and unitIndex from hllObject that we need to update */
    uint32_t byteIndex = 0;
    uint32_t unitIndex = 0;
    HllGetRegisterUnit(index, &byteIndex, &unitIndex);
    /* 2. Second, update registerValue in sparse mode, also reset NDVEstimator to -1 */
    HllUpdateSparseValue(byteIndex, unitIndex, value);
    /* 3. Third, check if it need to transform into full mode */
    if (hllObject->lenUsed > BITS_TO_VALUE(hllObject->hllpara.log2Sparsesize)) {
        HllTransformFull();
    }
}

/* Set hllObject register index to value in full mode */
void Hll::HllSetFullValue(const uint64_t hashval)
{
    Assert(hllObject->hlltype == HLL_FULL);

    uint32_t index = 0;
    uint8_t value = 0;
    HllHashToValue(hashval, &index, &value);
    if (value == 0) {
        return;
    }

    if (value > HllGetByte(index)) {
        HllSetByte(index, value);
        hllObject->NDVEstimator = -1;
    }
}

/* transform hll into full mode */
void Hll::HllTransformFull()
{
    /* first we copy hllData into newdata */
    MemoryContext old = MemoryContextSwitchTo(cxt);
    uint8_t *newdata = (uint8_t *)palloc0(BITS_TO_VALUE(hllObject->hllpara.log2Registers) * sizeof(uint8_t));
    (void)MemoryContextSwitchTo(old);

    switch (hllObject->hlltype) {
        case HLL_EMPTY:
            /* set all zero, do nothing */
            break;
        case HLL_EXPLICIT: {
            for (uint32_t i = 0; i < hllObject->lenUsed; i += HLL_EXPLICIT_UINT) {
                uint64_t hashval = HllGetExplictitValue(i);

                uint32_t index = 0;
                uint8_t value = 0;
                HllHashToValue(hashval, &index, &value);
                newdata[index] = value;
            }
            break;
        }
        case HLL_SPARSE: {
            uint32_t index = 0; /* index max is 2^16 */
            for (uint32_t i = 0; i < hllObject->lenUsed; i++) {
                uint8_t byteValue = HllGetByte(i);
                switch (BYTE_TYPE(byteValue)) {
                    case SPARSE_LONGZERO:
                        index += BYTE_GET_VALUE(byteValue) * HLL_LONGZERO_UNIT;
                        break;
                    case SPARSE_ZERO:
                        index += BYTE_GET_VALUE(byteValue);
                        break;
                    case DENSE_VALUE:
                        newdata[index++] = BYTE_GET_VALUE(byteValue);
                        break;
                    default: /* can never reach here, keep compiler safety */
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll register type")));
                }
            }
            /* check if ok */
            Assert(index == BITS_TO_VALUE(hllObject->hllpara.log2Registers));
            break;
        }
        case HLL_FULL: /* do nothing */
            pfree_ext(newdata);
            return;
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll type")));
    }

    /* then we replace hllData with newdata */
    pfree_ext(hllObject->hllData);
    hllObject->hllData = newdata;

    hllObject->hlltype = HLL_FULL;
    hllObject->lenUsed = BITS_TO_VALUE(hllObject->hllpara.log2Registers);
    hllObject->lenTotal = hllObject->lenUsed;
}

/* transform hll into sparse mode */
void Hll::HllTransformSparse()
{
    switch (hllObject->hlltype) {
        case HLL_EMPTY:
            HllEmptyToSparse();
            break;
        case HLL_EXPLICIT:
            HllExplicitToSparse();
            break;
        case HLL_SPARSE:
            break;
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll type")));
    }
}

/* check whether explicit mode is ok */
void Hll::HllExplicitCheck()
{
    Assert(hllObject->hlltype == HLL_EXPLICIT);

    if (hllObject->lenUsed > BITS_TO_VALUE(hllObject->hllpara.log2Explicitsize)) {
        if (hllObject->hllpara.log2Sparsesize > hllObject->hllpara.log2Explicitsize) {
            HllTransformSparse();
            if (hllObject->lenUsed > BITS_TO_VALUE(hllObject->hllpara.log2Sparsesize)) {
                HllTransformFull();
            }
        } else {
            HllTransformFull();
        }
    }
}

/* transform from empty mode into sparse mode */
void Hll::HllEmptyToSparse()
{
    Assert(hllObject->hlltype == HLL_EMPTY);
    Assert(hllObject->hllData == NULL);

    int log2byte = hllObject->hllpara.log2Registers - HLL_REGISTER_BITS - HLL_LOG2_LONGZERO_UNIT;
    const int nbyte = (log2byte > 0) ? BITS_TO_VALUE(log2byte) : 1;

    MemoryContext old = MemoryContextSwitchTo(cxt);
    if (nbyte > 1) { /* means all HLL_LONGZERO_FULL */
        hllObject->hllData = (uint8_t *)palloc0(nbyte * sizeof(uint8_t));
        errno_t rc = memset_s(hllObject->hllData, nbyte, HLL_LONGZERO_FULL, nbyte);
        securec_check(rc, "", "");
        hllObject->lenTotal = nbyte;
    } else { /* means 1-byte SPARSE_LONGZERO */
        hllObject->hllData = (uint8_t *)palloc0(sizeof(uint8_t));
        *hllObject->hllData = SET_LONGZERO_VALUE(BITS_TO_VALUE(hllObject->hllpara.log2Registers));
        hllObject->lenTotal = 1;
    }
    (void)MemoryContextSwitchTo(old);

    hllObject->lenUsed = hllObject->lenTotal;
    hllObject->hlltype = HLL_SPARSE;
}

/* transform from explicit mode into sparse mode */
void Hll::HllExplicitToSparse()
{
    Assert(hllObject->hlltype == HLL_EXPLICIT);

    uint32_t zerocount = 0;   /* zero_register between explicit byte */
    int lastindex = -1;       /* last explicit index */
    uint32_t sparseindex = 0; /* current sparse index */

    /* when transform into sparse mode, we think of the worst condition:
     * nbyte-1 bytes HLL_LONGZERO_FULL, 1-byte SPARSE_LONGZERO,
     * 1-byte SPARSE_ZERO, 1-byte DENSE_VALUE */
    int log2byte = hllObject->hllpara.log2Registers - HLL_REGISTER_BITS - HLL_LOG2_LONGZERO_UNIT;
    const int nbyte = (log2byte > 0) ? BITS_TO_VALUE(log2byte) : 1;
    uint32_t sparseMinsize = nbyte + 2; /* sparse left size should be greater than it */
    uint32_t sparsesize = sparseMinsize; /* init sparse size */

    MemoryContext old = MemoryContextSwitchTo(cxt);
    uint8_t *newdata = (uint8_t *)palloc0(sparsesize * sizeof(uint8_t));
    uint64_t *explicitdata = (uint64_t *)hllObject->hllData;
    uint32_t explicitlen = hllObject->lenUsed / HLL_EXPLICIT_UINT;
    for (uint32_t i = 0; i < explicitlen; i++) {
        uint32_t index = 0;
        uint8_t value = 0;
        HllHashToValue(explicitdata[i], &index, &value);

        if (value == 0) {
            continue;
        }

        /* the hashvalue is in order, and we get the head bits as index, so must be in order too */
        Assert((int)index >= lastindex);
        uint8_t newvalue = SET_DENSE_VALUE(value);
        if ((int)index == lastindex) { /* if index is same, simply update value */
            if (newdata[sparseindex - 1] < newvalue) {
                newdata[sparseindex - 1] = newvalue;
            }
        } else {
            zerocount = index - lastindex - 1;
            /* set the head zero value */
            sparseindex += HllSetSparseZero(newdata + sparseindex, zerocount);
            /* set DENSE_VALUE */
            newdata[sparseindex++] = newvalue;
            /* if set one value, update index */
            lastindex = index;

            /* check if repalloc is needed */
            if (sparseindex + sparseMinsize > sparsesize) {
                sparsesize += HLL_EXPAND_SIZE;
                newdata = (uint8_t *)repalloc(newdata, sparsesize * sizeof(uint8_t));
            }
        }
    }
    (void)MemoryContextSwitchTo(old);

    /* set the tail zero value, similar to the head */
    zerocount = BITS_TO_VALUE(hllObject->hllpara.log2Registers) - lastindex - 1;
    sparseindex += HllSetSparseZero(newdata + sparseindex, zerocount);

    /* update hllObject */
    pfree_ext(hllObject->hllData);
    hllObject->hllData = newdata;
    hllObject->lenTotal = sparsesize;
    hllObject->lenUsed = sparseindex;
    hllObject->hlltype = HLL_SPARSE;
}

/* set zero value to hlldata */
uint32_t Hll::HllSetSparseZero(uint8_t *hlldata, uint32_t zerocount)
{
    uint32_t index = 0;
    if (zerocount >= HLL_LONGZERO_UNIT) {
        /* first set full SPARSE_LONGZERO */
        int fullnum = zerocount >> (HLL_REGISTER_BITS + HLL_LOG2_LONGZERO_UNIT);
        if (fullnum > 0) {
            errno_t rc = memset_s(hlldata, fullnum, HLL_LONGZERO_FULL, fullnum);
            securec_check(rc, "", "");
            index += fullnum;
        }
        /* then set SPARSE_LONGZERO */
        zerocount &= 0xfff;
        if (zerocount >= HLL_LONGZERO_UNIT) {
            hlldata[index++] = SET_LONGZERO_VALUE(zerocount);
            zerocount &= BITS_MAX_VALUE(HLL_LOG2_LONGZERO_UNIT);
        }
    }
    /* third set SPARSE_ZERO */
    if (zerocount > 0) {
        hlldata[index++] = SET_ZERO_VALUE(zerocount);
    }

    return index;
}

/* we don't check index in this function, must be careful */
uint64_t Hll::HllGetExplictitValue(const uint32_t index)
{
    uint64_t *explicitdata = (uint64_t *)hllObject->hllData;
    return explicitdata[index / HLL_EXPLICIT_UINT];
}

/*
 * for sparse mode, if we want to locate the index of an register, it's on hllData[byteIndex],
 * since a byte can save several register, we set the unitIndex.
 * for example, if hllData = 00111111 00111111 00111111 00111111,
 * which saves (255+1)*64*4=16384 registers, and if we set registerIndex = 256 * 64 + 100,
 * then the byteIndex = 1, unitIndex = 100.
 * must be careful for type SPARSE_LONGZERO, whose units is 64.
 */
void Hll::HllGetRegisterUnit(const uint32_t registerIndex, uint32_t *byteIndex, uint32_t *unitIndex)
{
    if (registerIndex == 0) {
        *byteIndex = 0;
        *unitIndex = 0;
        return;
    }

    uint32_t currentRegisterSum = 0; /* how many registers so far */
    uint32_t currentRegister = 0; /* how many registers in current byte */
    uint32_t i; /* current byteIndex */
    for (i = 0; i < hllObject->lenUsed; i++) {
        const uint8_t byteValue = HllGetByte(i);
        switch (BYTE_TYPE(byteValue)) {
            case SPARSE_LONGZERO:
                currentRegister = BYTE_GET_VALUE(byteValue) << HLL_LOG2_LONGZERO_UNIT;
                break;
            case SPARSE_ZERO:
                currentRegister = BYTE_GET_VALUE(byteValue);
                break;
            case DENSE_VALUE:
                currentRegister = 1;
                break;
            default: /* can never reach here, keep compiler safety */
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll register type")));
        }
        currentRegisterSum += currentRegister;
        if (currentRegisterSum > registerIndex) {
            break;
        }
    }
    if (i == hllObject->lenUsed) { /* if never break, means error, it will never happen in normal condition */
        if (registerIndex >= BITS_TO_VALUE(hllObject->hllpara.log2Registers)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hll index is out of range")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("hllData missed in sparse mode")));
        }
    }

    *byteIndex = i;
    *unitIndex = registerIndex - (currentRegisterSum - currentRegister);
}

/*
 * update hllObject in sparsemode
 * return false indicates no update
 * must nmake sure that longzero type is in front of zero type,
 * so the continuous 2 bytes can never be SPARSE_ZERO SPARSE_LONGZERO
 */
void Hll::HllUpdateSparseValue(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue)
{
    if (newValue == 0) {
        return;
    }

    uint8_t byteValue = HllGetByte(byteIndex);

    switch (BYTE_TYPE(byteValue)) {
        case SPARSE_LONGZERO:
            return HllUpdateSparseType1(byteIndex, unitIndex, newValue);
        case SPARSE_ZERO:
            return HllUpdateSparseType2(byteIndex, unitIndex, newValue);
        case DENSE_VALUE:
            return HllUpdateSparseType3(byteIndex, unitIndex, newValue);
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll register type")));
    }
}

/* update hll value of type SPARSE_LONGZERO */
void Hll::HllUpdateSparseType1(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue)
{
    uint8_t byteValue = HllGetByte(byteIndex);
    Assert(BYTE_TYPE(byteValue) == SPARSE_LONGZERO);

    if (newValue == 0) {
        return;
    }

    /* left and right save the corresponding zero-count
     * for example, if we update 00000011(means there are 4*64 zero_register), and unitIndex = 64*2+1
     * so there are 2*64+1 zero_register left, 1*64+62 right */
    uint32_t left = unitIndex;
    uint32_t right = BYTE_GET_VALUE(byteValue) * HLL_LONGZERO_UNIT - left - 1;

    /* update the hllObject, for SPARSE_LONGZERO, we create 5_byte_size object,
     * for worst condition, SPARSE_LONGZERO -> SPARSE_LONGZERO+SPARSE_ZERO+DENSE_VALUE+SPARSE_LONGZERO+SPARSE_ZERO */
    uint8_t newByte[5];
    int index = 0;
    int originLen = 1; /* if right side is SPARSE_ZERO, originLen=2 */
    if (left >= HLL_LONGZERO_UNIT) {
        HllSetSparseByte(&newByte[index++], left, 0);
        left &= BITS_MAX_VALUE(HLL_LOG2_LONGZERO_UNIT);
    }
    if (left > 0) {
        HllSetSparseByte(&newByte[index++], left, 0);
    }
    HllSetSparseByte(&newByte[index++], 1, newValue);
    /* must be careful if right side is SPARSE_ZERO */
    if (byteIndex < hllObject->lenUsed - 1) {
        uint8_t laterByte = HllGetByte(byteIndex + 1);
        if (BYTE_TYPE(laterByte) == SPARSE_ZERO) {
            originLen++;
            right += BYTE_GET_VALUE(laterByte);
        }
    }
    if (right >= HLL_LONGZERO_UNIT) {
        HllSetSparseByte(&newByte[index++], right, 0);
        right &= BITS_MAX_VALUE(HLL_LOG2_LONGZERO_UNIT);
    }
    if (right > 0) {
        HllSetSparseByte(&newByte[index++], right, 0);
    }
    HllUpdateValue(byteIndex, newByte, index, originLen);

    hllObject->NDVEstimator = -1;
}

/* update hll value of type SPARSE_ZERO */
void Hll::HllUpdateSparseType2(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue)
{
    uint8_t byteValue = HllGetByte(byteIndex);
    Assert(BYTE_TYPE(byteValue) == SPARSE_ZERO);

    if (newValue == 0) {
        return;
    }

    /* left and right save the corresponding zero-count
     * for example, if we update 01000011(means there are 4 zero_register), and unitIndex = 2
     * so there are 2 zero_register left, 1 right */
    uint32_t left = unitIndex;
    uint32_t right = BYTE_GET_VALUE(byteValue) - left - 1;

    /* update the hllObject, for SPARSE_LONGZERO, we create 3_byte_size object,
     * for worst condition, SPARSE_ZERO -> SPARSE_ZERO+DENSE_VALUE+SPARSE_ZERO */
    uint8_t newByte[3] = {0};

    int index = 0;
    if (left > 0) {
        HllSetSparseByte(&newByte[index++], left, 0);
    }
    HllSetSparseByte(&newByte[index++], 1, newValue);
    if (right > 0) {
        HllSetSparseByte(&newByte[index++], right, 0);
    }
    HllUpdateValue(byteIndex, newByte, index, 1);

    hllObject->NDVEstimator = -1;
}

/* update hll value of type DENSE_VALUE */
void Hll::HllUpdateSparseType3(const uint32_t byteIndex, const uint32_t unitIndex, const uint8_t newValue)
{
    uint8_t byteValue = HllGetByte(byteIndex);
    Assert(BYTE_TYPE(byteValue) == DENSE_VALUE);
    Assert(unitIndex == 0);

    if (newValue <= BYTE_GET_VALUE(byteValue)) {
        return;
    }
    HllSetByte(byteIndex, SET_DENSE_VALUE(newValue));

    hllObject->NDVEstimator = -1;
}

/* set a byte */
void Hll::HllSetSparseByte(uint8_t *destByte, const uint32_t count, const uint8_t value)
{
    Assert(count != 0);

    if (value == 0) {
        if (count >= HLL_LONGZERO_UNIT) {
            *destByte = SET_LONGZERO_VALUE(count);
        } else {
            *destByte = SET_ZERO_VALUE(count);
        }
    } else if (count == 1) {
        *destByte = SET_DENSE_VALUE(value);
    } else { /* it will never happen in normal condition */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("can't set sparse byte cause hll data is out of range")));
    }
}

/* combine two full object */
void Hll::HllCombineFull(Hll aHll)
{
    Assert(hllObject->hlltype == HLL_FULL);
    Assert(aHll.hllObject->hlltype == HLL_FULL);

    for (uint32_t i = 0; i < hllObject->lenUsed; i++) {
        if (hllObject->hllData[i] < aHll.hllObject->hllData[i]) {
            hllObject->hllData[i] = aHll.hllObject->hllData[i];
        }
    }
    /* reset ndv and lasthash */
    hllObject->NDVEstimator = -1;
    hllObject->lastHash = 0;
}

/* combine two explicit object, similar to merge two ordered arrays */
void Hll::HllComibineExplicit(Hll aHll)
{
    Assert(hllObject->hlltype == HLL_EXPLICIT);
    Assert(aHll.hllObject->hlltype == HLL_EXPLICIT);

    MemoryContext old = MemoryContextSwitchTo(cxt);
    uint8_t *newdata = (uint8_t *)palloc0((hllObject->lenUsed + aHll.hllObject->lenUsed) * sizeof(uint8_t));
    (void)MemoryContextSwitchTo(old);

    uint32_t p1 = 0; /* point of hllData */
    uint32_t p2 = 0; /* point of aHll.hllData */
    uint32_t p = 0;  /* point of newdata */
    errno_t rc;

    /* combine two ordered arrays */
    uint32_t len1 = hllObject->lenUsed;
    uint32_t len2 = aHll.hllObject->lenUsed;
    while ((p1 < len1) && (p2 < len2)) {
        uint64_t hashval1 = HllGetExplictitValue(p1);
        uint64_t hashval2 = aHll.HllGetExplictitValue(p2);
        if (hashval1 < hashval2) {
            rc = memcpy_s(newdata + p, HLL_EXPLICIT_UINT, hllObject->hllData + p1, HLL_EXPLICIT_UINT);
            p1 += HLL_EXPLICIT_UINT;
        } else if (hashval1 > hashval2) {
            rc = memcpy_s(newdata + p, HLL_EXPLICIT_UINT, aHll.hllObject->hllData + p2, HLL_EXPLICIT_UINT);
            p2 += HLL_EXPLICIT_UINT;
        } else {
            rc = memcpy_s(newdata + p, HLL_EXPLICIT_UINT, hllObject->hllData + p1, HLL_EXPLICIT_UINT);
            p1 += HLL_EXPLICIT_UINT;
            p2 += HLL_EXPLICIT_UINT;
        }
        p += HLL_EXPLICIT_UINT;
        securec_check(rc, "", "");
    }
    uint32_t leftLen = len1 + len2 - p1 - p2; /* equal to len1-p1 or len2-p2 */
    if (p1 < len1) {                          /* that means p2=len2 */
        rc = memcpy_s(newdata + p, leftLen, hllObject->hllData + p1, leftLen);
        securec_check(rc, "", "");
    } else if (p2 < len2) { /* that means p1=len1 */
        rc = memcpy_s(newdata + p, leftLen, aHll.hllObject->hllData + p2, leftLen);
        securec_check(rc, "", "");
    }
    p += leftLen;

    pfree_ext(hllObject->hllData);
    hllObject->hllData = newdata;
    hllObject->lenTotal = len1 + len2;
    hllObject->lenUsed = p;
    /* reset lasthash */
    hllObject->lastHash = 0;
    /* update ndv */
    hllObject->NDVEstimator = hllObject->lenUsed / HLL_EXPLICIT_UINT;

    /* check if need transform mode */
    HllExplicitCheck();
}

/* get hll histogram, histogram's size is 64, and init to 0 */
void Hll::HllGetHistogram(uint32_t *histogram)
{
    uint32_t i;
    uint8_t value;
    switch (hllObject->hlltype) {
        case HLL_EMPTY:
            return;
        case HLL_EXPLICIT: {
            for (i = 0; i < hllObject->lenUsed; i += HLL_EXPLICIT_UINT) {
                uint64_t hashval = HllGetExplictitValue(i);
                uint32_t index = 0;
                uint8_t value = 0;
                HllHashToValue(hashval, &index, &value);

                histogram[value]++;
            }
            histogram[0] = BITS_TO_VALUE(hllObject->hllpara.log2Registers) - hllObject->lenUsed / HLL_EXPLICIT_UINT;
            return;
        }
        case HLL_SPARSE: {
            for (i = 0; i < hllObject->lenUsed; i++) {
                value = HllGetByte(i);
                switch (BYTE_TYPE(value)) {
                    case SPARSE_LONGZERO:
                        histogram[0] += BYTE_GET_VALUE(value) * HLL_LONGZERO_UNIT;
                        break;
                    case SPARSE_ZERO:
                        histogram[0] += BYTE_GET_VALUE(value);
                        break;
                    case DENSE_VALUE:
                        histogram[BYTE_GET_VALUE(value)]++;
                        break;
                    default: /* can never reach here, keep compiler safety */
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll register type")));
                }
            }
            return;
        }
        case HLL_FULL: {
            for (i = 0; i < hllObject->lenUsed; i++) {
                histogram[HllGetByte(i)]++;
            }
            return;
        }
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("undefined hll type")));
    }
}

/* see in "New cardinality estimation algorithms for HyperLogLog sketches" page16 */
double Hll::HllGetTau(double x)
{
    if (x == 0 || x == 1) {
        return 0;
    }
    double y = 1;
    double z = 1 - x;
    double tau = 0;
    while (abs(tau - z) > DBL_EPSILON) {
        CHECK_FOR_INTERRUPTS();
        x = sqrt(x);
        tau = z;
        y *= 0.5;
        z -= (1 - x) * (1 - x) * y;
    }
    return tau / 3;
}

/* see in "New cardinality estimation algorithms for HyperLogLog sketches" page16 */
double Hll::HllGetSigma(double x)
{
    /* if x==1, return infinite, but it can never happen */
    if (x == 1) {
        return INFINITY;
    }
    double y = 1;
    double z = x;
    double sigma = 0;
    while (abs(sigma - z) > DBL_EPSILON) {
        CHECK_FOR_INTERRUPTS();
        x *= x;
        sigma = z;
        z += x * y;
        y *= 2;
    }
    return sigma;
}

void Hll::HllCalculateNDV()
{
    int i;
    /* In normal case, hlltype here will never be HLL_EMPTY or HLL_EXPLICIT. But if indeed came here, we need do
     * something to repair it */
    switch (hllObject->hlltype) {
        case HLL_EMPTY:
            /* in empty mode, we have set ndv to zero before, if gone here, we need reset it to zero */
            hllObject->NDVEstimator = 0;
            return;
        case HLL_EXPLICIT:
            /* in explicit mode, we have updated ndv before, if gone here, we need calculate ndv again, but it will be
             * an estimated value, not exact value */
        case HLL_SPARSE:
        case HLL_FULL: {
            uint32_t histogram[64] = { 0 };
            HllGetHistogram(histogram);
            uint32_t finalIndex = INT64_LEN - hllObject->hllpara.log2Registers;
            double registers = BITS_TO_VALUE(hllObject->hllpara.log2Registers); /* set double for calculation */

            /* see in "New cardinality estimation algorithms for HyperLogLog sketches" page16 */
            double z = 0;
            z += registers * HllGetTau(1 - histogram[finalIndex + 1] / registers);
            for (i = finalIndex; i > 0; i--) {
                z += histogram[i];
                z *= 0.5;
            }
            z += registers * HllGetSigma(histogram[0] / registers);

            hllObject->NDVEstimator = HLL_ALPHA_INF * registers * registers / z;
            return;
        }
        default: /* can never reach here, keep compiler safety */
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("undefined hll type")));
    }
}
