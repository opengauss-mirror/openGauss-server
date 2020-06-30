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
 * vecunique.h
 *     Prototypes for vectorized unique
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecunique.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECUNIQUE_H
#define VECUNIQUE_H

#include "vecexecutor/vecnodes.h"

extern VecUniqueState* ExecInitVecUnique(VecUnique* node, EState* estate, int eflags);
extern VectorBatch* ExecVecUnique(VecUniqueState* node);
extern void ExecEndVecUnique(VecUniqueState* node);
extern void ExecReScanVecUnique(VecUniqueState* node);

#endif /* VECUNIQUE_H */
