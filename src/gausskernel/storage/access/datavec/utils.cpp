/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * utils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "access/datavec/utils.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/bitvec.h"
#include "access/datavec/vector.h"

Size VectorItemSize(int dimensions)
{
    return VECTOR_SIZE(dimensions);
}

Size HalfvecItemSize(int dimensions)
{
    return HALFVEC_SIZE(dimensions);
}

Size BitItemSize(int dimensions)
{
    return VARBITTOTALLEN(dimensions);
}

void VectorUpdateCenter(Pointer v, int dimensions, const float *x)
{
    Vector *vec = (Vector *)v;

    SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++) {
        vec->x[k] = x[k];
    }
}

void HalfvecUpdateCenter(Pointer v, int dimensions, const float *x)
{
    HalfVector *vec = (HalfVector *)v;

    SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++) {
        vec->x[k] = Float4ToHalfUnchecked(x[k]);
    }
}

void BitUpdateCenter(Pointer v, int dimensions, const float *x)
{
    VarBit *vec = (VarBit *)v;
    unsigned char *nx = VARBITS(vec);

    SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
    VARBITLEN(vec) = dimensions;

    for (uint32 k = 0; k < VARBITBYTES(vec); k++) {
        nx[k] = 0;
    }

    for (int k = 0; k < dimensions; k++) {
        nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));
    }
}

void VectorSumCenter(Pointer v, float *x)
{
    Vector *vec = (Vector *)v;

    for (int k = 0; k < vec->dim; k++) {
        x[k] += vec->x[k];
    } 
}

void HalfvecSumCenter(Pointer v, float *x)
{
    HalfVector *vec = (HalfVector *)v;

    for (int k = 0; k < vec->dim; k++) {
        x[k] += HalfToFloat4(vec->x[k]);
    }
}

void BitSumCenter(Pointer v, float *x)
{
    VarBit *vec = (VarBit *)v;

    for (int k = 0; k < VARBITLEN(vec); k++) {
        x[k] += (float)(((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);
    }
}

/*
 * Allocate a vector array
 */
VectorArray VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
    VectorArray res = (VectorArray)palloc(sizeof(VectorArrayData));

    /* Ensure items are aligned to prevent UB */
    itemsize = MAXALIGN(itemsize);

    res->length = 0;
    res->maxlen = maxlen;
    res->dim = dimensions;
    res->itemsize = itemsize;
    res->items = (char *)palloc0_huge(CurrentMemoryContext, maxlen * itemsize);
    return res;
}

/*
 * Free a vector array
 */
void VectorArrayFree(VectorArray arr)
{
    if (arr->items != NULL) {
        pfree(arr->items);
    }
    pfree(arr);
}