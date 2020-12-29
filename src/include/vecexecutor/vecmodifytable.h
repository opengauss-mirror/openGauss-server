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
 * vecmodifytable.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecmodifytable.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECMODIFYTABLE_H
#define VECMODIFYTABLE_H

#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"

extern VecModifyTableState* ExecInitVecModifyTable(VecModifyTable* node, EState* estate, int eflags);

extern VectorBatch* ExecVecModifyTable(VecModifyTableState* node);

extern void ExecEndVecModifyTable(VecModifyTableState* node);

extern void ExecReScanVecModifyTable(VecModifyTableState* node);

template <class T>
extern VectorBatch* ExecVecUpdate(
    VecModifyTableState* state, T* update_op, VectorBatch* batch, EState* estate, bool canSetTag, int options);

template <class T>
extern VectorBatch* ExecVecInsert(VecModifyTableState* state, T* insert_op, VectorBatch* batch, VectorBatch* planBatch,
    EState* estate, bool canSetTag, int options);

extern bool checkInsertScanPartitionSame(VecModifyTableState* mtstate);

struct InsertArg;
extern void* CreateOperatorObject(CmdType operation, bool isPartitioned, Relation resultRelationDesc,
    ResultRelInfo* resultRelInfo, EState* estate, TupleDesc sortTupDesc, InsertArg* args, Relation* dataDestRel,
    VecModifyTableState* node);

#endif /* VECMODIFYTABLE_H */
