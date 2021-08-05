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
 *  nodeGD.h
 *
 * IDENTIFICATION
 *        src/include/executor/nodeGD.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NODE_GD_H
#define NODE_GD_H

#include "nodes/execnodes.h"

extern GradientDescentState* ExecInitGradientDescent(GradientDescent* node, EState* estate, int eflags);
extern TupleTableSlot* ExecGradientDescent(GradientDescentState* state);
extern void ExecEndGradientDescent(GradientDescentState* state);

typedef void (*GradientDescentHook_iteration)(GradientDescentState* state);
extern GradientDescentHook_iteration gdhook_iteration;

List* makeGradientDescentExpr(AlgorithmML algorithm, List* list, int field);

#endif /* NODE_GD_H */
