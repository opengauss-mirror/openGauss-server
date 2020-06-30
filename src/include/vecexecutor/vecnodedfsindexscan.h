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
 * vecnodedfsindexscan.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodedfsindexscan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODEDFSINDEXSCAN_H
#define VECNODEDFSINDEXSCAN_H

#include "vecexecutor/vecnodes.h"
#include "nodes/plannodes.h"

// Column index scan interfaces
//
extern DfsIndexScanState* ExecInitDfsIndexScan(DfsIndexScan* node, EState* estate, int eflags);
extern VectorBatch* ExecDfsIndexScan(DfsIndexScanState* node);
extern void ExecEndDfsIndexScan(DfsIndexScanState* node);
extern void ExecReScanDfsIndexScan(DfsIndexScanState* node);

#endif /* VECNODEDFSINDEXSCAN_H */
