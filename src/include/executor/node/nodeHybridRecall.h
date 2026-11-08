/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 *
 * src/include/executor/node/nodeHybridRecall.h
 *
 * [P0-3] HybridRecall - two-branch recall fusion parent (hybrid recall
 * design doc §8). ExecProcNode is wired centrally in ExecInitNode; this
 * header only declares the lifecycle entry points.
 * -------------------------------------------------------------------------
 */
#ifndef NODEHYBRIDRECALL_H
#define NODEHYBRIDRECALL_H

#include "nodes/execnodes.h"

struct Stream;

extern HybridRecallState* ExecInitHybridRecall(HybridRecall* node, EState* estate, int eflags);
extern void ExecEndHybridRecall(HybridRecallState* node);
extern void ExecReScanHybridRecall(HybridRecallState* node);
extern void ExecEnableHybridRecallWorkerRankOnly(const Stream* stream, PlanState* branch);

#endif /* NODEHYBRIDRECALL_H */
