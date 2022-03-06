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
 * nodePartIterator.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/nodePartIterator.h
 *
 * NOTE
 *        Partition Iterator is a new execution operator for partition wise join.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef NODEPARTITERATOR_H
#define NODEPARTITERATOR_H

#include "nodes/execnodes.h"

extern void SetPartitionIteratorParamter(PartIteratorState* node, List* subPartLengthList);
extern PartIteratorState* ExecInitPartIterator(PartIterator* node, EState* estate, int eflags);
extern TupleTableSlot* ExecPartIterator(PartIteratorState* node);
extern void ExecEndPartIterator(PartIteratorState* node);
extern void ExecReScanPartIterator(PartIteratorState* node);

#endif /* NODEPARTITERATOR_H */
