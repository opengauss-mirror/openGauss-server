/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * scalarquantizer.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/scalarquantizer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <float.h>
#include "access/datavec/scalarquantizer.h"


ScalarQuantizer *InitScalarQuantizer(int dim)
{
    Size size = dim * sizeof(float);
    ScalarQuantizer *sq = (ScalarQuantizer *)palloc(sizeof(ScalarQuantizer));
    sq->dim = dim;
    sq->decodeVec = NULL;
    sq->trained = (float *)palloc(size * 2);
    float a = FLT_MAX;
    float b = -FLT_MAX;
    int i;

    for (i = 0; i < dim; i++) {
        sq->trained[i] = a;
    }
    for (i = dim; i < 2 * dim; i++) {
        sq->trained[i] = b;
    }
    return sq;
}

void FreeScalarQuantizer(ScalarQuantizer *sq)
{
    if (sq->trained != NULL) {
        pfree(sq->trained);
    }
    if (sq->decodeVec != NULL) {
        pfree(sq->decodeVec);
    }
    pfree(sq);
}