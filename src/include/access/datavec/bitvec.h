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
 * bitvec.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/bitvec.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BITVEC_H
#define BITVEC_H

#include "postgres.h"
#include "utils/varbit.h"

extern uint64 (*BitHammingDistance)(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 distance);
extern double (*BitJaccardDistance)(uint32 bytes, unsigned char *ax, unsigned char *bx, uint64 ab, uint64 aa,
                                    uint64 bb);

void BitvecInit(void);

VarBit *InitBitVector(int dim);

Datum hamming_distance(PG_FUNCTION_ARGS);
Datum jaccard_distance(PG_FUNCTION_ARGS);

#endif
