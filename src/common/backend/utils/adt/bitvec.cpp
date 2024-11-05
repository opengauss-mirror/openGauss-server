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
 * bitvec.cpp
 *
 * IDENTIFICATION
 *        src/common/backend/utils/adt/bitvec.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/datavec/bitvec.h"
#include "utils/varbit.h"

uint64 (*BitHammingDistance)(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
double (*BitJaccardDistance)(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa, uint64 bb);
static THR_LOCAL bool BitvecNeedInitialization = true;

/*
 * Allocate and initialize a new bit vector
 */
VarBit *InitBitVector(int dim)
{
    VarBit *result;
    int size;

    size = VARBITTOTALLEN(dim);
    result = (VarBit *)palloc0(size);
    SET_VARSIZE(result, size);
    VARBITLEN(result) = dim;

    return result;
}

/*
 * Ensure same dimensions
 */
static inline void CheckDims(VarBit *a, VarBit *b)
{
    if (VARBITLEN(a) != VARBITLEN(b))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("different bit lengths %u and %u", VARBITLEN(a), VARBITLEN(b))));
}

/*
 * Get the Hamming distance between two bit vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hamming_distance);
Datum hamming_distance(PG_FUNCTION_ARGS)
{
    VarBit *a = PG_GETARG_VARBIT_P(0);
    VarBit *b = PG_GETARG_VARBIT_P(1);

    if (BitvecNeedInitialization) {
        BitvecInit();
        BitvecNeedInitialization = false;
    }

    CheckDims(a, b);

    PG_RETURN_FLOAT8((double)BitHammingDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b), 0));
}

/*
 * Get the Jaccard distance between two bit vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(jaccard_distance);
Datum jaccard_distance(PG_FUNCTION_ARGS)
{
    VarBit *a = PG_GETARG_VARBIT_P(0);
    VarBit *b = PG_GETARG_VARBIT_P(1);

    if (BitvecNeedInitialization) {
        BitvecInit();
        BitvecNeedInitialization = false;
    }

    CheckDims(a, b);

    PG_RETURN_FLOAT8(BitJaccardDistance(VARBITBYTES(a), VARBITS(a), VARBITS(b), 0, 0, 0));
}
