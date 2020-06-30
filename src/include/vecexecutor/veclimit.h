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
 * veclimit.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/veclimit.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODELIMIT_H
#define VECNODELIMIT_H

#include "vecexecutor/vecnodes.h"

extern VectorBatch* ExecVecLimit(VecLimitState* node);
extern VecLimitState* ExecInitVecLimit(VecLimit* node, EState* estate, int eflags);
extern void ExecEndVecLimit(VecLimitState* node);
extern void ExecReScanVecLimit(VecLimitState* node);

#endif /*VECNODELIMIT_H*/
