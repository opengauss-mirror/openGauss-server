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
 * vecnoderesult.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnoderesult.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODERESULT_H
#define VECNODERESULT_H

#include "vecexecutor/vecnodes.h"

extern VecResultState* ExecInitVecResult(VecResult* node, EState* estate, int eflags);
extern VectorBatch* ExecVecResult(VecResultState* node);
extern void ExecEndVecResult(VecResultState* node);
extern void ExecReScanVecResult(VecResultState* node);

#endif /* NODERESULT_H */
