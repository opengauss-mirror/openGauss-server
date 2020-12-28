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
 * vecnodesort.h
 * 
 * 
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California 
 *
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodesort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NODEVECSORT_H
#define NODEVECSORT_H

#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"
#include "workload/workload.h"

extern VecSortState* ExecInitVecSort(Sort* node, EState* estate, int eflags);
extern VectorBatch* ExecVecSort(VecSortState* node);
extern void ExecEndVecSort(VecSortState* node);
extern void ExecVecSortMarkPos(VecSortState* node);
extern void ExecVecSortRestrPos(VecSortState* node);
extern void ExecReScanVecSort(VecSortState* node);
extern long ExecGetMemCostVecSort(VecSort*);
extern void ExecEarlyFreeVecSort(VecSortState* node);

#endif /* NODEVECSORT_H */
