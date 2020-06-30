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
 * vecgrpuniq.cpp
 *    Prototypes for vectorized unique and group
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecgrpuniq.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "vecexecutor/vecgrpuniq.h"

Datum BuildScanBatch(PG_FUNCTION_ARGS)
{
    GUCell* cell = (GUCell*)PG_GETARG_DATUM(0);
    Encap* cap = (Encap*)PG_GETARG_DATUM(1);
    int i = 0;
    VectorBatch* scan_batch = cap->batch;
    int nrows = scan_batch->m_rows;
    ScalarVector* p_vector = NULL;
    int outerCols = cap->outerCols;
    ScalarVector* arr = scan_batch->m_arr;
    GUCellVal* val = cell->val;

    for (i = 0; i < outerCols; i++) {
        p_vector = &arr[i];

        p_vector->m_vals[nrows] = val[i].val;
        p_vector->m_flag[nrows] = val[i].flag;
        p_vector->m_rows++;
    }

    scan_batch->m_rows++;

    PG_FREE_IF_COPY(cell, 0);
    PG_FREE_IF_COPY(cap, 1);

    PG_RETURN_DATUM(0);
}
