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
 * vecnoderowtovector.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnoderowtovector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODEROWTOVECTOR_H
#define VECNODEROWTOVECTOR_H

#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vectorbatch.h"

extern struct varlena *DetoastDatumBatch(struct varlena* datum, ScalarVector* arr);
extern RowToVecState* ExecInitRowToVec(RowToVec* node, EState* estate, int eflags);
extern VectorBatch* ExecRowToVec(RowToVecState* node);
extern void ExecEndRowToVec(RowToVecState* node);
extern void ExecReScanRowToVec(RowToVecState* node);
extern bool VectorizeOneTuple(VectorBatch* pBatch, TupleTableSlot* slot, MemoryContext transformContext);
#endif /* NODEROWTOVEC_H */