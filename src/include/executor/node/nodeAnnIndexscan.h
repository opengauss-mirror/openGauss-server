/* -------------------------------------------------------------------------
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 * src/include/executor/nodeAnnIndexscan.h
 */
#ifndef NODEANNINDEXSCAN_H
#define NODEANNINDEXSCAN_H

#include "executor/exec/execStream.h"
#include "nodes/execnodes.h"

extern AnnIndexScanState* ExecInitAnnIndexScan(AnnIndexScan* node, EState* estate, int eflags);
extern void ExecEndAnnIndexScan(AnnIndexScanState* node);
extern void ExecAnnIndexMarkPos(AnnIndexScanState* node);
extern void ExecAnnIndexRestrPos(AnnIndexScanState* node);
extern void ExecReScanAnnIndexScan(AnnIndexScanState* node);

extern void ExecInitPartitionForAnnIndexScan(AnnIndexScanState* indexstate, EState* estate);

#endif /* NODEANNINDEXSCAN_H */
