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
 * scalarquantizer.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/scalarquantizer.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SCALARQUANTIZER_H
#define SCALARQUANTIZER_H

#include "postgres.h"
#include "access/genam.h"
#include "nodes/execnodes.h"
#include "access/datavec/vector.h"

#define SQ_RANGE 255.0f

typedef struct ScalarQuantizer {
    int dim;
    float *trained; /* min+diff, the num of trained is 2*dim */
    Vector *decodeVec;
} ScalarQuantizer;

ScalarQuantizer *InitScalarQuantizer(int dim);
void FreeScalarQuantizer(ScalarQuantizer *sq);

#endif