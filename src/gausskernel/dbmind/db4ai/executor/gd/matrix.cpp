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
 *---------------------------------------------------------------------------------------
 *
 *  matrix.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/matrix.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/matrix.h"

#define MATRIX_LIMITED_OUTPUT 30

void matrix_print(const Matrix *matrix, StringInfo buf, bool full)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    const gd_float *pf = matrix->data;
    appendStringInfoChar(buf, '[');
    for (int r = 0; r < matrix->rows; r++) {
        if (!full && matrix->rows > MATRIX_LIMITED_OUTPUT && r > (MATRIX_LIMITED_OUTPUT / 2) &&
            r < matrix->rows - (MATRIX_LIMITED_OUTPUT / 2)) {
            if (matrix->columns > 1)
                appendStringInfoString(buf, ",\n...");
            else
                appendStringInfoString(buf, ", ...");

            r = matrix->rows - MATRIX_LIMITED_OUTPUT / 2;
            continue;
        }

        if (matrix->columns > 1) {
            if (r > 0)
                appendStringInfoString(buf, ",\n");

            appendStringInfoChar(buf, '[');
        } else {
            if (r > 0)
                appendStringInfoString(buf, ", ");
        }
        for (int c = 0; c < matrix->columns; c++) {
            if (c > 0)
                appendStringInfoString(buf, ", ");

            appendStringInfo(buf, "%.16g", *pf++);
        }
        if (matrix->columns > 1)
            appendStringInfoChar(buf, ']');
    }
    appendStringInfoChar(buf, ']');
}

void elog_matrix(int elevel, const char *msg, const Matrix *matrix)
{
    StringInfoData buf;
    initStringInfo(&buf);
    matrix_print(matrix, &buf, false);
    ereport(elevel, (errmodule(MOD_DB4AI), errmsg("%s = %s", msg, buf.data)));
    pfree(buf.data);
}
