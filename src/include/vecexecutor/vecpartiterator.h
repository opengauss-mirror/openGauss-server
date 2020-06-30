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
 * vecpartiterator.h
 *     Partition Iterator is a new execution operator for partition wise join.
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecpartiterator.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECPARTITERATOR_H
#define VECPARTITERATOR_H

#include "vecexecutor/vecnodes.h"

extern VecPartIteratorState* ExecInitVecPartIterator(VecPartIterator* node, EState* estate, int eflags);
extern VectorBatch* ExecVecPartIterator(VecPartIteratorState* node);
extern void ExecEndVecPartIterator(VecPartIteratorState* node);
extern void ExecReScanVecPartIterator(VecPartIteratorState* node);

#endif /* VECPARTITERATOR_H */
