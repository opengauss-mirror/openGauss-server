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
 * vecnodecstoreindexscan.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodecstoreindexscan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODEINDEXSCAN_H
#define VECNODEINDEXSCAN_H

#include "vecexecutor/vecnodes.h"
#include "nodes/plannodes.h"

// Column index scan interfaces
//
extern CStoreIndexScanState* ExecInitCstoreIndexScan(CStoreIndexScan* node, EState* estate, int eflags);
extern VectorBatch* ExecCstoreIndexScan(CStoreIndexScanState* node);
extern void ExecEndCstoreIndexScan(CStoreIndexScanState* node);
extern void ExecCstoreIndexMarkPos(CStoreIndexScanState* node);
extern void ExecCstoreIndexRestrPos(CStoreIndexScanState* node);
extern void ExecReScanCStoreIndexScan(CStoreIndexScanState* node);

#endif /* VECNODEINDEXSCAN_H */
