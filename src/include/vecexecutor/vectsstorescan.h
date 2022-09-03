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
 * vectsstorescan.h
 *         tsstore scan
 *
 * IDENTIFICATION
 *        src/include/vecexecutor/vectsstorescan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODETSSTORESCAN_H
#define VECNODETSSTORESCAN_H

#include "utils/relcache.h"

struct TsStoreScan;
struct EState;
struct TsStoreScanState;
struct VectorBatch;

extern TsStoreScanState* ExecInitTsStoreScan(TsStoreScan* node, Relation parentHeapRel, EState* estate, int eflags,
    bool indexFlag = false, bool codegenInUplevel = false);
extern VectorBatch* ExecTsStoreScan(TsStoreScanState* node);
extern void ExecEndTsStoreScan(TsStoreScanState* node, bool indexFlag);
extern void ExecReScanTsStoreScan(TsStoreScanState* node);
extern VectorBatch* ts_apply_projection_and_filter(TsStoreScanState* node, 
                                                   VectorBatch* pScanBatch, 
                                                   ExprDoneCond* isDone);
void reset_sys_vector(const List* sys_attr_list, VectorBatch* vector);                                                   

#endif /* VECNODETSSTORESCAN_H */
