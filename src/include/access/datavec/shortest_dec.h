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
 * shortest_dec.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/shortest_dec.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SHORTEST_DEC_H
#define SHORTEST_DEC_H

#define FLOAT_SHORTEST_DECIMAL_LEN 16

int FloatToShortestDecimalBufn(float f, char *result);
int FloatToShortestDecimalBuf(float f, char *result);

#endif /* SHORTEST_DEC_H */
