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
 * vecsubqueryscan.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecsubqueryscan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODESUBQUERYSCAN_H
#define VECNODESUBQUERYSCAN_H

#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vectorbatch.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"

// Vectorized implementation
//
extern VecSubqueryScanState* ExecInitVecSubqueryScan(VecSubqueryScan* node, EState* estate, int eflags);
extern VectorBatch* ExecVecSubqueryScan(VecSubqueryScanState* node);
extern void ExecEndVecSubqueryScan(VecSubqueryScanState* node);
extern void ExecReScanVecSubqueryScan(VecSubqueryScanState* node);

#endif /* NODESUBQUERYSCAN_H */
